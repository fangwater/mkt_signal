//! Stitch dominant CME baseline minutes and apply strict additive back adjustment.

use anyhow::{anyhow, bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use mimalloc::MiMalloc;
use polars::prelude::{
    BooleanChunked, DataFrame, NamedFrom, NewChunkedArray, ParquetCompression, ParquetReader,
    ParquetWriter, SerReader, Series,
};
use rayon::prelude::*;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const DEFAULT_INPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min";
const DEFAULT_ROLL: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/cme_volume_roll_1d";
const DEFAULT_OUTPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min_hfq";

const BASE_PRICE_COLUMNS: &[&str] = &[
    "open",
    "high",
    "low",
    "close",
    "vwap",
    "buy_vwap",
    "sell_vwap",
    "twap",
    "mid_price",
    "implied_vwap",
    "implied_twap",
];

#[derive(Parser, Debug)]
#[command(name = "cme_baseline_hfq_1min")]
struct Args {
    #[arg(long, default_value = DEFAULT_INPUT)]
    input_root: PathBuf,
    #[arg(long, default_value = DEFAULT_ROLL)]
    roll_root: PathBuf,
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    output_root: PathBuf,
    #[arg(long, default_value = "2024-01-01")]
    start: NaiveDate,
    #[arg(long, default_value = "2026-06-01")]
    end: NaiveDate,
    #[arg(long, value_delimiter = ',', default_value = "ES,NQ,RTY,YM,GC,CL")]
    products: Vec<String>,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long)]
    overwrite: bool,
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

fn product_spec(product: &str) -> Result<ProductSpec> {
    PRODUCTS
        .iter()
        .copied()
        .find(|spec| spec.product == product)
        .ok_or_else(|| anyhow!("unsupported HFQ product {product:?}"))
}

#[derive(Clone, Debug)]
struct Adjustment {
    effective_day: NaiveDate,
    value: Option<f64>,
    skipped: bool,
}

#[derive(Clone)]
struct ProductInputs {
    dominants: BTreeMap<NaiveDate, String>,
    adjustments: Vec<Adjustment>,
}

#[derive(Clone)]
struct Job {
    product: String,
    day: NaiveDate,
    ric: String,
    gap: f64,
    input: PathBuf,
    output: PathBuf,
}

#[derive(Serialize)]
struct CoverageRow {
    exchange: String,
    product: String,
    trading_day: String,
    status: String,
    ric: String,
    gap: Option<f64>,
    input_file: String,
    output_file: String,
}

fn header_index(headers: &csv::StringRecord, name: &str) -> Result<usize> {
    headers
        .iter()
        .position(|value| value == name)
        .with_context(|| format!("CSV is missing column {name}"))
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "t" => Ok(true),
        "false" | "0" | "f" | "" => Ok(false),
        other => bail!("invalid boolean {other:?}"),
    }
}

fn load_product_inputs(root: &Path, spec: ProductSpec) -> Result<ProductInputs> {
    let dir = root.join(spec.exchange).join(spec.product);
    let dominant_path = dir.join("dominant.csv");
    let adjustment_path = dir.join("adjustment.csv");

    let mut dominant_reader = csv::Reader::from_path(&dominant_path)
        .with_context(|| format!("open {}", dominant_path.display()))?;
    let headers = dominant_reader.headers()?.clone();
    let day_index = header_index(&headers, "trading_day")?;
    let ric_index = header_index(&headers, "ric")?;
    let mut dominants = BTreeMap::new();
    for record in dominant_reader.records() {
        let record = record?;
        let day = NaiveDate::parse_from_str(record.get(day_index).unwrap_or(""), "%Y-%m-%d")?;
        let ric = record.get(ric_index).unwrap_or("").trim();
        if !ric.is_empty() {
            dominants.insert(day, ric.to_string());
        }
    }

    let mut adjustment_reader = csv::Reader::from_path(&adjustment_path)
        .with_context(|| format!("open {}", adjustment_path.display()))?;
    let headers = adjustment_reader.headers()?.clone();
    let effective_index = header_index(&headers, "effective_trading_day")?;
    let value_index = header_index(&headers, "adjustment_value")?;
    let skipped_index = header_index(&headers, "skipped")?;
    let mut adjustments = Vec::new();
    for record in adjustment_reader.records() {
        let record = record?;
        let text = record.get(value_index).unwrap_or("").trim();
        let value = if text.is_empty() {
            None
        } else {
            let value = text.parse::<f64>()?;
            if !value.is_finite() {
                bail!("non-finite adjustment in {}", adjustment_path.display());
            }
            Some(value)
        };
        adjustments.push(Adjustment {
            effective_day: NaiveDate::parse_from_str(
                record.get(effective_index).unwrap_or(""),
                "%Y-%m-%d",
            )?,
            value,
            skipped: parse_bool(record.get(skipped_index).unwrap_or(""))?,
        });
    }
    adjustments.sort_by_key(|row| row.effective_day);
    Ok(ProductInputs {
        dominants,
        adjustments,
    })
}

fn gap_before(adjustments: &[Adjustment], day: NaiveDate) -> Result<Option<f64>> {
    let mut gap = 0.0;
    for adjustment in adjustments {
        if day >= adjustment.effective_day {
            continue;
        }
        if adjustment.skipped {
            return Ok(None);
        }
        if let Some(value) = adjustment.value {
            gap += value;
        }
    }
    if !gap.is_finite() {
        bail!("HFQ gap is not finite for {day}");
    }
    Ok(Some(gap))
}

fn list_jobs(args: &Args) -> Result<(Vec<Job>, Vec<CoverageRow>)> {
    if args.end <= args.start || args.workers == 0 {
        bail!("invalid date range or worker count");
    }
    if args.output_root.to_string_lossy().contains("drop_special")
        || args.output_root == args.input_root
    {
        bail!("refusing unsafe HFQ output {}", args.output_root.display());
    }
    let mut jobs = Vec::new();
    let mut coverage = Vec::new();
    for requested in &args.products {
        let product = requested.trim().to_ascii_uppercase();
        let spec = product_spec(&product)?;
        let metadata = load_product_inputs(&args.roll_root, spec)?;
        let dir = args.input_root.join(spec.exchange).join(spec.product);
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
            if day < args.start || day >= args.end {
                continue;
            }
            let output = args
                .output_root
                .join(spec.exchange)
                .join(spec.product)
                .join(format!("{stem}.parquet"));
            let mut row = CoverageRow {
                exchange: spec.exchange.to_string(),
                product: spec.product.to_string(),
                trading_day: day.to_string(),
                status: String::new(),
                ric: metadata.dominants.get(&day).cloned().unwrap_or_default(),
                gap: None,
                input_file: input.display().to_string(),
                output_file: output.display().to_string(),
            };
            let Some(ric) = metadata.dominants.get(&day) else {
                row.status = "no_dominant".to_string();
                coverage.push(row);
                continue;
            };
            let Some(gap) = gap_before(&metadata.adjustments, day)? else {
                row.status = "incomplete_adjustment_path".to_string();
                coverage.push(row);
                continue;
            };
            row.gap = Some(gap);
            if output.exists() && !args.overwrite {
                row.status = "existing".to_string();
                coverage.push(row);
                continue;
            }
            row.status = "ready".to_string();
            coverage.push(row);
            jobs.push(Job {
                product: spec.product.to_string(),
                day,
                ric: ric.clone(),
                gap,
                input,
                output,
            });
        }
    }
    jobs.sort_by(|left, right| {
        left.day
            .cmp(&right.day)
            .then(left.product.cmp(&right.product))
    });
    coverage.sort_by(|left, right| {
        left.trading_day
            .cmp(&right.trading_day)
            .then(left.product.cmp(&right.product))
    });
    Ok((jobs, coverage))
}

fn shift_column(df: &mut DataFrame, name: &str, gap: f64) -> Result<()> {
    let values = df
        .column(name)
        .with_context(|| format!("missing HFQ price column {name}"))?
        .f64()?
        .into_iter()
        .map(|value| value.map(|price| price + gap))
        .collect::<Vec<_>>();
    df.replace(name, Series::new(name.into(), values))?;
    Ok(())
}

fn valid_adjusted_l1(bid: f64, ask: f64) -> bool {
    bid.is_finite() && ask.is_finite() && ask >= bid
}

fn valid_adjusted_l1_pair(bid: Option<f64>, ask: Option<f64>) -> bool {
    matches!((bid, ask), (None, None))
        || matches!((bid, ask), (Some(bid), Some(ask)) if valid_adjusted_l1(bid, ask))
}

fn apply_hfq(mut df: DataFrame, job: &Job) -> Result<DataFrame> {
    let ric = df.column("ric")?.str()?;
    let mask = BooleanChunked::from_iter_values(
        "mask".into(),
        ric.into_iter().map(|value| value == Some(job.ric.as_str())),
    );
    df = df.filter(&mask)?;
    if df.height() == 0 {
        bail!(
            "{} {} dominant RIC {} is absent",
            job.product,
            job.day,
            job.ric
        );
    }
    df.replace(
        "contract_id",
        Series::new("contract_id".into(), vec![job.product.clone(); df.height()]),
    )?;
    for &name in BASE_PRICE_COLUMNS {
        shift_column(&mut df, name, job.gap)?;
    }
    for side in ["bid", "ask"] {
        for level in 0..10 {
            shift_column(&mut df, &format!("{side}{level}p"), job.gap)?;
        }
    }
    let timestamps = df.column("ts")?.i64()?;
    let bids = df.column("bid0p")?.f64()?;
    let asks = df.column("ask0p")?.f64()?;
    let mut previous = None;
    for index in 0..df.height() {
        let ts = timestamps.get(index).context("null HFQ ts")?;
        if previous.is_some_and(|value| ts <= value) {
            bail!("{} {} timestamps are not increasing", job.product, job.day);
        }
        previous = Some(ts);
        if !valid_adjusted_l1_pair(bids.get(index), asks.get(index)) {
            bail!(
                "{} {} invalid adjusted L1 row {index}",
                job.product,
                job.day
            );
        }
    }
    Ok(df)
}

fn write_parquet_atomic(path: &Path, mut df: DataFrame) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let result = (|| -> Result<()> {
        ParquetWriter::new(File::create(&tmp)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)?;
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn process_job(job: &Job, parquet_lock: &Mutex<()>) -> Result<u64> {
    let file = File::open(&job.input).with_context(|| format!("open {}", job.input.display()))?;
    let df = ParquetReader::new(file)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", job.input.display()))?;
    let df = apply_hfq(df, job)?;
    let rows = df.height() as u64;
    let _guard = parquet_lock
        .lock()
        .map_err(|_| anyhow!("parquet lock poisoned"))?;
    let result = std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("cme-hfq-parquet".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn_scoped(scope, || write_parquet_atomic(&job.output, df))
            .map_err(anyhow::Error::from)?
            .join()
            .map_err(|_| anyhow!("HFQ writer panicked for {}", job.output.display()))
    })?;
    result?;
    eprintln!(
        "hfq_done product={} day={} ric={} gap={} rows={}",
        job.product, job.day, job.ric, job.gap, rows
    );
    Ok(rows)
}

fn write_coverage(path: &Path, rows: &[CoverageRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer = csv::Writer::from_path(path)?;
    for row in rows {
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}

fn run() -> Result<()> {
    let args = Arc::new(Args::parse());
    let (jobs, coverage) = list_jobs(&args)?;
    let coverage_path = args.output_root.join("_audit").join("coverage.csv");
    write_coverage(&coverage_path, &coverage)?;
    let mut status_counts = HashMap::new();
    for row in &coverage {
        *status_counts.entry(row.status.as_str()).or_insert(0usize) += 1;
    }
    eprintln!(
        "hfq_start jobs={} coverage={} statuses={:?} output={}",
        jobs.len(),
        coverage.len(),
        status_counts,
        args.output_root.display()
    );
    if jobs.is_empty() {
        return Ok(());
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .stack_size(16 * 1024 * 1024)
        .build()?;
    let parquet_lock = Arc::new(Mutex::new(()));
    let results = pool.install(|| {
        jobs.par_iter()
            .map(|job| process_job(job, &parquet_lock))
            .collect::<Vec<_>>()
    });
    let mut rows = 0u64;
    for result in results {
        rows += result?;
    }
    eprintln!("hfq_complete files={} rows={rows}", jobs.len());
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("cme_baseline_hfq_1min failed: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_gap_stops_before_skipped_roll() {
        let rows = vec![
            Adjustment {
                effective_day: NaiveDate::from_ymd_opt(2024, 12, 18).unwrap(),
                value: Some(-0.4),
                skipped: false,
            },
            Adjustment {
                effective_day: NaiveDate::from_ymd_opt(2025, 2, 18).unwrap(),
                value: None,
                skipped: true,
            },
            Adjustment {
                effective_day: NaiveDate::from_ymd_opt(2025, 3, 19).unwrap(),
                value: Some(-0.2),
                skipped: false,
            },
        ];
        assert_eq!(
            gap_before(&rows, NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()).unwrap(),
            None
        );
        assert_eq!(
            gap_before(&rows, NaiveDate::from_ymd_opt(2025, 2, 18).unwrap()).unwrap(),
            Some(-0.2)
        );
    }

    #[test]
    fn additive_adjusted_l1_may_cross_zero() {
        assert!(valid_adjusted_l1(-11.01, -10.99));
        assert!(valid_adjusted_l1(0.0, 0.01));
        assert!(!valid_adjusted_l1(-10.0, -10.01));
        assert!(!valid_adjusted_l1(f64::NAN, 1.0));
    }

    #[test]
    fn quote_free_baseline_rows_are_valid_hfq_input() {
        assert!(valid_adjusted_l1_pair(None, None));
        assert!(!valid_adjusted_l1_pair(Some(1.0), None));
    }
}
