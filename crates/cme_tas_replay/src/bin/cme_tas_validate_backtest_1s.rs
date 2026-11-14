//! Validate published CME dense 1s parquet files without loading the full year at once.

use anyhow::{bail, Context, Result};
use clap::Parser;
use polars::prelude::{DataFrame, ParquetReader, SerReader};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const DEFAULT_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/backtest_1s";
const COLUMNS: &[&str] = &[
    "contract_id",
    "ric",
    "ts",
    "bid0p",
    "bid0v",
    "ask0p",
    "ask0v",
    "buy_high",
    "sell_low",
    "close",
    "midp",
];
const PRODUCTS: &[(&str, &str)] = &[
    ("CME", "ES"),
    ("CME", "NQ"),
    ("CME", "RTY"),
    ("CBOT", "YM"),
    ("COMEX", "GC"),
    ("NYMEX", "CL"),
];

#[derive(Parser, Debug)]
#[command(name = "cme_tas_validate_backtest_1s")]
struct Args {
    #[arg(long, default_value = DEFAULT_ROOT)]
    root: PathBuf,
    #[arg(long, default_value_t = 2024)]
    year: i32,
    #[arg(long, default_value_t = 6)]
    workers: usize,
    #[arg(long)]
    expected_rows: Option<u64>,
}

#[derive(Clone)]
struct Job {
    product: &'static str,
    path: PathBuf,
    intervals: Vec<[i64; 2]>,
}

#[derive(Deserialize)]
struct DayAudit {
    intervals: Vec<[i64; 2]>,
}

struct FileResult {
    product: &'static str,
    rows: u64,
}

fn required_columns(df: &DataFrame, path: &Path) -> Result<()> {
    let actual = df
        .get_column_names()
        .into_iter()
        .map(|name| name.as_str())
        .collect::<Vec<_>>();
    if actual != COLUMNS {
        bail!(
            "{} has columns {:?}, expected {:?}",
            path.display(),
            actual,
            COLUMNS
        );
    }
    for name in [
        "contract_id",
        "ric",
        "ts",
        "bid0p",
        "bid0v",
        "ask0p",
        "ask0v",
        "close",
        "midp",
    ] {
        let nulls = df.column(name)?.null_count();
        if nulls != 0 {
            bail!("{} column {name} has {nulls} nulls", path.display());
        }
    }
    Ok(())
}

fn validate_file(job: &Job) -> Result<FileResult> {
    let file = File::open(&job.path).with_context(|| format!("open {}", job.path.display()))?;
    let df = ParquetReader::new(file)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", job.path.display()))?;
    required_columns(&df, &job.path)?;

    let contract = df.column("contract_id")?.str()?;
    let ric = df.column("ric")?.str()?;
    let ts = df.column("ts")?.i64()?;
    let bid = df.column("bid0p")?.f64()?;
    let bid_size = df.column("bid0v")?.f64()?;
    let ask = df.column("ask0p")?.f64()?;
    let ask_size = df.column("ask0v")?.f64()?;
    let buy_high = df.column("buy_high")?.f64()?;
    let sell_low = df.column("sell_low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let midp = df.column("midp")?.f64()?;

    let mut last_by_ric: HashMap<&str, (i64, usize)> = HashMap::new();
    let mut contract_by_ric: HashMap<&str, &str> = HashMap::new();
    let mut previous: Option<(i64, &str)> = None;
    for index in 0..df.height() {
        let contract_id = contract
            .get(index)
            .with_context(|| format!("null contract_id row {index} {}", job.path.display()))?;
        let ric_value = ric
            .get(index)
            .with_context(|| format!("null ric row {index} {}", job.path.display()))?;
        let second = ts
            .get(index)
            .with_context(|| format!("null ts row {index} {}", job.path.display()))?;
        if contract_id.is_empty() || ric_value.is_empty() {
            bail!("empty key row {index} {}", job.path.display());
        }
        let interval_index = job
            .intervals
            .iter()
            .position(|interval| second >= interval[0] && second <= interval[1])
            .with_context(|| {
                format!(
                    "row {index} ts {second} is outside audit intervals in {}",
                    job.path.display()
                )
            })?;
        if let Some((previous_ts, previous_contract)) = previous {
            if second < previous_ts || (second == previous_ts && contract_id <= previous_contract) {
                bail!(
                    "unsorted or duplicate key row {index} {}: ({second}, {contract_id}) after ({previous_ts}, {previous_contract})",
                    job.path.display()
                );
            }
        }
        previous = Some((second, contract_id));
        if let Some((last, last_interval)) = last_by_ric.insert(ric_value, (second, interval_index))
        {
            if interval_index == last_interval && second != last + 1 {
                bail!(
                    "non-continuous ric {ric_value} row {index} {}: {last} -> {second}",
                    job.path.display()
                );
            }
            if interval_index != last_interval {
                let previous_end = job.intervals[last_interval][1];
                if interval_index <= last_interval
                    || (last != previous_end - 1 && last != previous_end)
                {
                    bail!(
                        "invalid interval transition ric {ric_value} row {index} {}: interval {last_interval} ts {last} -> interval {interval_index} ts {second}",
                        job.path.display()
                    );
                }
            }
        }
        if let Some(existing) = contract_by_ric.insert(ric_value, contract_id) {
            if existing != contract_id {
                bail!(
                    "ric {ric_value} maps to both {existing} and {contract_id} in {}",
                    job.path.display()
                );
            }
        }

        let bid_value = bid.get(index).expect("required bid0p checked");
        let bid_size_value = bid_size.get(index).expect("required bid0v checked");
        let ask_value = ask.get(index).expect("required ask0p checked");
        let ask_size_value = ask_size.get(index).expect("required ask0v checked");
        let close_value = close.get(index).expect("required close checked");
        let midp_value = midp.get(index).expect("required midp checked");
        if !(bid_value.is_finite()
            && bid_value > 0.0
            && bid_size_value.is_finite()
            && bid_size_value >= 0.0
            && ask_value.is_finite()
            && ask_value >= bid_value
            && ask_size_value.is_finite()
            && ask_size_value >= 0.0
            && close_value.is_finite()
            && close_value > 0.0
            && midp_value == (bid_value + ask_value) / 2.0)
        {
            bail!("invalid market values row {index} {}", job.path.display());
        }
        for (name, value) in [
            ("buy_high", buy_high.get(index)),
            ("sell_low", sell_low.get(index)),
        ] {
            if value.is_some_and(|value| !value.is_finite() || value <= 0.0) {
                bail!("invalid {name} row {index} {}", job.path.display());
            }
        }
    }
    Ok(FileResult {
        product: job.product,
        rows: df.height() as u64,
    })
}

fn collect_jobs(args: &Args) -> Result<Vec<Job>> {
    let prefix = args.year.to_string();
    let mut jobs = Vec::new();
    for &(exchange, product) in PRODUCTS {
        let dir = args.root.join(exchange).join(product);
        for entry in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with(&prefix) && name.ends_with(".parquet") {
                let audit_path = args
                    .root
                    .join("_audit/session_conformance")
                    .join(product)
                    .join(name.replace(".parquet", ".json"));
                let audit: DayAudit = serde_json::from_slice(
                    &fs::read(&audit_path)
                        .with_context(|| format!("read {}", audit_path.display()))?,
                )
                .with_context(|| format!("parse {}", audit_path.display()))?;
                jobs.push(Job {
                    product,
                    path,
                    intervals: audit.intervals,
                });
            }
        }
    }
    jobs.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(jobs)
}

fn run(args: &Args) -> Result<()> {
    if args.workers == 0 {
        bail!("workers must be positive");
    }
    let jobs = collect_jobs(args)?;
    let completed = AtomicUsize::new(0);
    let started = Instant::now();
    let pool = ThreadPoolBuilder::new()
        .num_threads(args.workers.min(jobs.len()).max(1))
        .thread_name(|id| format!("cme-backtest-validate-{id}"))
        .build()?;
    let results = pool.install(|| {
        jobs.par_iter()
            .map(|job| {
                let result = validate_file(job);
                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if done % 50 == 0 || done == jobs.len() {
                    eprintln!(
                        "cme_tas_validate_backtest_1s progress={done}/{} elapsed_s={:.1}",
                        jobs.len(),
                        started.elapsed().as_secs_f64()
                    );
                }
                result
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let mut by_product: HashMap<&str, (usize, u64)> = HashMap::new();
    let mut total_rows = 0u64;
    for result in results {
        total_rows += result.rows;
        let entry = by_product.entry(result.product).or_default();
        entry.0 += 1;
        entry.1 += result.rows;
    }
    if args
        .expected_rows
        .is_some_and(|expected| expected != total_rows)
    {
        bail!(
            "validated {total_rows} rows, expected {}",
            args.expected_rows.unwrap()
        );
    }
    for &(_, product) in PRODUCTS {
        let (files, rows) = by_product.get(product).copied().unwrap_or_default();
        println!("product={product} files={files} rows={rows}");
    }
    println!(
        "cme_tas_validate_backtest_1s files={} rows={total_rows}",
        jobs.len()
    );
    Ok(())
}

fn main() {
    if let Err(error) = run(&Args::parse()) {
        eprintln!("cme_tas_validate_backtest_1s failed: {error:#}");
        std::process::exit(1);
    }
}
