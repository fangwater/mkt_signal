//! Merge corrected CME TWAP columns from recomputed donor roots.
//!
//! The donor roots are produced by the normal TAS-only baseline and HFQ
//! exporters after their TWAP implementation changes. This binary verifies the
//! published row identity and writes only `twap` plus `implied_twap` into fresh
//! staging roots; it never regenerates books, sizes, OHLC, or labels.

use anyhow::{anyhow, bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use mimalloc::MiMalloc;
use polars::prelude::{
    DataFrame, NamedFrom, ParquetCompression, ParquetReader, ParquetWriter, SerReader, Series,
};
use rayon::prelude::*;
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const DEFAULT_RAW: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min";
const DEFAULT_HFQ: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min_hfq";
const TWAP_COLUMNS: [&str; 2] = ["twap", "implied_twap"];

#[derive(Parser, Debug)]
#[command(about = "Patch only CME baseline TWAP columns from validated donor roots")]
struct Args {
    #[arg(long, default_value = DEFAULT_RAW)]
    raw_input_root: PathBuf,
    #[arg(long)]
    raw_donor_root: PathBuf,
    #[arg(long)]
    raw_output_root: PathBuf,
    #[arg(long, default_value = DEFAULT_HFQ)]
    hfq_input_root: PathBuf,
    #[arg(long)]
    hfq_donor_root: PathBuf,
    #[arg(long)]
    hfq_output_root: PathBuf,
    #[arg(long, default_value = "2020-01-01")]
    start: NaiveDate,
    #[arg(long, default_value = "2026-06-01")]
    end: NaiveDate,
    #[arg(long, value_delimiter = ',', default_value = "ES,NQ,RTY,YM,GC,CL")]
    products: Vec<String>,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone, Copy)]
struct ProductSpec {
    product: &'static str,
    exchange: &'static str,
}

const PRODUCTS: &[ProductSpec] = &[
    ProductSpec {
        product: "ES",
        exchange: "CME",
    },
    ProductSpec {
        product: "NQ",
        exchange: "CME",
    },
    ProductSpec {
        product: "RTY",
        exchange: "CME",
    },
    ProductSpec {
        product: "YM",
        exchange: "CBOT",
    },
    ProductSpec {
        product: "GC",
        exchange: "COMEX",
    },
    ProductSpec {
        product: "CL",
        exchange: "NYMEX",
    },
];

#[derive(Clone)]
struct Job {
    product: String,
    exchange: String,
    day: NaiveDate,
    input: PathBuf,
    donor: PathBuf,
    output: PathBuf,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("cme_patch_twap_1min failed: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Arc::new(Args::parse());
    validate_args(&args)?;
    let products = selected_products(&args.products)?;
    let raw_jobs = list_jobs(
        &args.raw_input_root,
        &args.raw_donor_root,
        &args.raw_output_root,
        &products,
        args.start,
        args.end,
        args.overwrite,
    )?;
    let hfq_jobs = list_jobs(
        &args.hfq_input_root,
        &args.hfq_donor_root,
        &args.hfq_output_root,
        &products,
        args.start,
        args.end,
        args.overwrite,
    )?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .stack_size(16 * 1024 * 1024)
        .build()?;
    let writer_lock = Arc::new(Mutex::new(()));
    let raw = pool.install(|| patch_jobs(&raw_jobs, &writer_lock, args.dry_run))?;
    let hfq = pool.install(|| patch_jobs(&hfq_jobs, &writer_lock, args.dry_run))?;
    eprintln!(
        "cme_patch_twap_1min complete raw_files={} raw_changed={} hfq_files={} hfq_changed={} dry_run={}",
        raw.0, raw.1, hfq.0, hfq.1, args.dry_run
    );
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    if args.end <= args.start || args.workers == 0 {
        bail!("invalid date range or worker count");
    }
    for (label, input, donor, output) in [
        (
            "raw",
            &args.raw_input_root,
            &args.raw_donor_root,
            &args.raw_output_root,
        ),
        (
            "hfq",
            &args.hfq_input_root,
            &args.hfq_donor_root,
            &args.hfq_output_root,
        ),
    ] {
        if input == donor || input == output || donor == output || output.starts_with(input) {
            bail!("{label} roots must be independent staging directories");
        }
        if [input, donor, output]
            .iter()
            .any(|path| path.to_string_lossy().contains("rocksdb"))
        {
            bail!("{label} roots must be parquet roots, never RocksDB");
        }
    }
    Ok(())
}

fn selected_products(requested: &[String]) -> Result<Vec<ProductSpec>> {
    let mut seen = BTreeSet::new();
    let mut selected = Vec::new();
    for product in requested {
        let product = product.trim().to_ascii_uppercase();
        if !seen.insert(product.clone()) {
            continue;
        }
        let spec = PRODUCTS
            .iter()
            .copied()
            .find(|spec| spec.product == product)
            .ok_or_else(|| anyhow!("unsupported CME product {product:?}"))?;
        selected.push(spec);
    }
    if selected.is_empty() {
        bail!("no products selected");
    }
    Ok(selected)
}

fn list_jobs(
    input_root: &Path,
    donor_root: &Path,
    output_root: &Path,
    products: &[ProductSpec],
    start: NaiveDate,
    end: NaiveDate,
    overwrite: bool,
) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for spec in products {
        let dir = input_root.join(spec.exchange).join(spec.product);
        if !dir.is_dir() {
            bail!("missing input directory {}", dir.display());
        }
        for entry in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
            let input = entry?.path();
            if input.extension().and_then(|value| value.to_str()) != Some("parquet") {
                continue;
            }
            let stem = input
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("");
            let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") else {
                continue;
            };
            if day < start || day >= end {
                continue;
            }
            let output = output_root
                .join(spec.exchange)
                .join(spec.product)
                .join(format!("{stem}.parquet"));
            if output.exists() && !overwrite {
                bail!("refusing to overwrite {}", output.display());
            }
            let donor = donor_root
                .join(spec.exchange)
                .join(spec.product)
                .join(format!("{stem}.parquet"));
            if !donor.is_file() {
                bail!("missing donor {}", donor.display());
            }
            jobs.push(Job {
                product: spec.product.to_string(),
                exchange: spec.exchange.to_string(),
                day,
                input,
                donor,
                output,
            });
        }
    }
    jobs.sort_by(|left, right| {
        left.exchange
            .cmp(&right.exchange)
            .then(left.product.cmp(&right.product))
            .then(left.day.cmp(&right.day))
    });
    if jobs.is_empty() {
        bail!("no input files selected");
    }
    Ok(jobs)
}

fn read_frame(path: &Path) -> Result<DataFrame> {
    ParquetReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", path.display()))
}

fn ensure_identity(input: &DataFrame, donor: &DataFrame) -> Result<()> {
    if input.height() != donor.height() {
        bail!(
            "row count mismatch: {} != {}",
            input.height(),
            donor.height()
        );
    }
    for name in ["contract_id", "ric"] {
        let left = input.column(name)?.str()?;
        let right = donor.column(name)?.str()?;
        for index in 0..input.height() {
            if left.get(index) != right.get(index) {
                bail!("identity mismatch column={name} row={index}");
            }
        }
    }
    let left = input.column("ts")?.i64()?;
    let right = donor.column("ts")?.i64()?;
    for index in 0..input.height() {
        if left.get(index) != right.get(index) {
            bail!("identity mismatch column=ts row={index}");
        }
    }
    Ok(())
}

fn patch_frame(mut input: DataFrame, donor: &DataFrame) -> Result<(DataFrame, u64)> {
    ensure_identity(&input, donor)?;
    let mut changed = 0u64;
    for name in TWAP_COLUMNS {
        let old = input.column(name)?.f64()?;
        let new = donor.column(name)?.f64()?;
        for index in 0..input.height() {
            if old.get(index).map(f64::to_bits) != new.get(index).map(f64::to_bits) {
                changed += 1;
            }
        }
        input.replace(
            name,
            Series::new(name.into(), new.into_iter().collect::<Vec<_>>()),
        )?;
    }
    Ok((input, changed))
}

fn write_atomic(path: &Path, mut frame: DataFrame) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("parquet.tmp");
    let result = (|| -> Result<()> {
        ParquetWriter::new(File::create(&temporary)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut frame)?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn patch_jobs(jobs: &[Job], lock: &Mutex<()>, dry_run: bool) -> Result<(u64, u64)> {
    let results = jobs
        .par_iter()
        .map(|job| -> Result<u64> {
            let input = read_frame(&job.input)?;
            let donor = read_frame(&job.donor)?;
            let (patched, changed) = patch_frame(input, &donor)?;
            if !dry_run {
                let _guard = lock
                    .lock()
                    .map_err(|_| anyhow!("parquet writer lock poisoned"))?;
                write_atomic(&job.output, patched)?;
            }
            eprintln!(
                "twap_patch {} {} {} rows={} changed={changed}",
                job.exchange,
                job.product,
                job.day,
                donor.height()
            );
            Ok(changed)
        })
        .collect::<Vec<_>>();
    let mut changed = 0u64;
    for result in results {
        changed += result?;
    }
    Ok((jobs.len() as u64, changed))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(twap: Option<f64>, implied_twap: Option<f64>, close: f64) -> DataFrame {
        DataFrame::new(vec![
            Series::new("contract_id".into(), ["CME:ES:2024-03"]),
            Series::new("ric".into(), ["ESH24"]),
            Series::new("ts".into(), [0_i64]),
            Series::new("close".into(), [Some(close)]),
            Series::new("twap".into(), [twap]),
            Series::new("implied_twap".into(), [implied_twap]),
        ])
        .unwrap()
    }

    #[test]
    fn patch_frame_replaces_only_twap_family() {
        let input = frame(Some(100.0), Some(101.0), 99.0);
        let donor = frame(Some(102.0), Some(103.0), 98.0);
        let (patched, changed) = patch_frame(input, &donor).unwrap();
        assert_eq!(changed, 2);
        assert_eq!(
            patched.column("close").unwrap().f64().unwrap().get(0),
            Some(99.0)
        );
        assert_eq!(
            patched.column("twap").unwrap().f64().unwrap().get(0),
            Some(102.0)
        );
        assert_eq!(
            patched
                .column("implied_twap")
                .unwrap()
                .f64()
                .unwrap()
                .get(0),
            Some(103.0)
        );
    }
}
