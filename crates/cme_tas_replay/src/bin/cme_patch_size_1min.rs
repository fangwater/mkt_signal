//! Merge audited CME order-size overlay columns into a published baseline root.
//!
//! The overlay donor may have older values for unrelated fields, notably TWAP.
//! This tool verifies row identity and replaces only the twelve order-size
//! columns in fresh staging output.

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

const DEFAULT_INPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min";
const SIZE_COLUMNS: [&str; 12] = [
    "large_order",
    "medium_order",
    "small_order",
    "large_buy",
    "large_sell",
    "medium_buy",
    "medium_sell",
    "small_buy",
    "small_sell",
    "net_buy_large",
    "net_buy_medium",
    "net_buy_small",
];

#[derive(Parser, Debug)]
#[command(about = "Patch only audited CME baseline order-size columns")]
struct Args {
    #[arg(long, default_value = DEFAULT_INPUT)]
    input_root: PathBuf,
    #[arg(long)]
    overlay_root: PathBuf,
    #[arg(long)]
    output_root: PathBuf,
    #[arg(long, default_value = "2017-01-01")]
    start: NaiveDate,
    #[arg(long, default_value = "2020-01-01")]
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
    overlay: PathBuf,
    output: PathBuf,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("cme_patch_size_1min failed: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Arc::new(Args::parse());
    validate_args(&args)?;
    let products = selected_products(&args.products)?;
    let jobs = list_jobs(
        &args.input_root,
        &args.overlay_root,
        &args.output_root,
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
    let result = pool.install(|| patch_jobs(&jobs, &writer_lock, args.dry_run))?;
    eprintln!(
        "cme_patch_size_1min complete files={} changed={} dry_run={}",
        result.0, result.1, args.dry_run
    );
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    if args.end <= args.start || args.workers == 0 {
        bail!("invalid date range or worker count");
    }
    if args.input_root == args.overlay_root
        || args.input_root == args.output_root
        || args.overlay_root == args.output_root
        || args.output_root.starts_with(&args.input_root)
    {
        bail!("input, overlay, and output roots must be independent staging directories");
    }
    if [&args.input_root, &args.overlay_root, &args.output_root]
        .iter()
        .any(|path| path.to_string_lossy().contains("rocksdb"))
    {
        bail!("all roots must be parquet roots, never RocksDB");
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
    overlay_root: &Path,
    output_root: &Path,
    products: &[ProductSpec],
    start: NaiveDate,
    end: NaiveDate,
    overwrite: bool,
) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for spec in products {
        let overlay_dir = overlay_root.join(spec.exchange).join(spec.product);
        if !overlay_dir.is_dir() {
            bail!("missing overlay directory {}", overlay_dir.display());
        }
        for entry in
            fs::read_dir(&overlay_dir).with_context(|| format!("read {}", overlay_dir.display()))?
        {
            let overlay = entry?.path();
            if overlay.extension().and_then(|value| value.to_str()) != Some("parquet") {
                continue;
            }
            let stem = overlay
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("");
            let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") else {
                continue;
            };
            if day < start || day >= end {
                continue;
            }
            let input = input_root
                .join(spec.exchange)
                .join(spec.product)
                .join(format!("{stem}.parquet"));
            if !input.is_file() {
                bail!("missing input {}", input.display());
            }
            let output = output_root
                .join(spec.exchange)
                .join(spec.product)
                .join(format!("{stem}.parquet"));
            if output.exists() && !overwrite {
                bail!("refusing to overwrite {}", output.display());
            }
            jobs.push(Job {
                product: spec.product.to_string(),
                exchange: spec.exchange.to_string(),
                day,
                input,
                overlay,
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
        bail!("no overlay files selected");
    }
    Ok(jobs)
}

fn read_frame(path: &Path) -> Result<DataFrame> {
    ParquetReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", path.display()))
}

fn ensure_identity(input: &DataFrame, overlay: &DataFrame) -> Result<()> {
    if input.height() != overlay.height() {
        bail!(
            "row count mismatch: {} != {}",
            input.height(),
            overlay.height()
        );
    }
    for name in ["contract_id", "ric"] {
        let left = input.column(name)?.str()?;
        let right = overlay.column(name)?.str()?;
        for index in 0..input.height() {
            if left.get(index) != right.get(index) {
                bail!("identity mismatch column={name} row={index}");
            }
        }
    }
    let left = input.column("ts")?.i64()?;
    let right = overlay.column("ts")?.i64()?;
    for index in 0..input.height() {
        if left.get(index) != right.get(index) {
            bail!("identity mismatch column=ts row={index}");
        }
    }
    Ok(())
}

fn patch_frame(mut input: DataFrame, overlay: &DataFrame) -> Result<(DataFrame, u64)> {
    ensure_identity(&input, overlay)?;
    let mut changed = 0u64;
    for name in SIZE_COLUMNS {
        let old = input.column(name)?.f64()?;
        let new = overlay.column(name)?.f64()?;
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
            let overlay = read_frame(&job.overlay)?;
            let (patched, changed) = patch_frame(input, &overlay)?;
            if !dry_run {
                let _guard = lock
                    .lock()
                    .map_err(|_| anyhow!("parquet writer lock poisoned"))?;
                write_atomic(&job.output, patched)?;
            }
            eprintln!(
                "size_patch {} {} {} rows={} changed={changed}",
                job.exchange,
                job.product,
                job.day,
                overlay.height()
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

    fn frame(size_value: f64, close: f64, twap: f64) -> DataFrame {
        let mut frame = DataFrame::new(vec![
            Series::new("contract_id".into(), ["CME:ES:2024-03"]),
            Series::new("ric".into(), ["ESH24"]),
            Series::new("ts".into(), [0_i64]),
            Series::new("close".into(), [close]),
            Series::new("twap".into(), [twap]),
        ])
        .unwrap();
        for name in SIZE_COLUMNS {
            frame
                .with_column(Series::new(name.into(), [size_value]))
                .unwrap();
        }
        frame
    }

    #[test]
    fn patch_frame_replaces_only_size_columns() {
        let input = frame(0.0, 99.0, 100.0);
        let overlay = frame(12.0, 98.0, 101.0);
        let (patched, changed) = patch_frame(input, &overlay).unwrap();
        assert_eq!(changed, SIZE_COLUMNS.len() as u64);
        assert_eq!(
            patched.column("close").unwrap().f64().unwrap().get(0),
            Some(99.0)
        );
        assert_eq!(
            patched.column("twap").unwrap().f64().unwrap().get(0),
            Some(100.0)
        );
        for name in SIZE_COLUMNS {
            assert_eq!(
                patched.column(name).unwrap().f64().unwrap().get(0),
                Some(12.0)
            );
        }
    }
}
