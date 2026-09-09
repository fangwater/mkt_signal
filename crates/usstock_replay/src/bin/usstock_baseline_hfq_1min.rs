//! Materialize US-stock 1-minute HFQ baseline parquet from the audited action chain.
//!
//! The action CSV contains one point per New York ex-date open. For a raw row
//! at `t`, its factor is the first point with `effective_ts_utc > t`; therefore
//! the ex-date open itself is unadjusted while all earlier rows include it.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use clap::Parser;
use polars::prelude::{
    DataFrame, NamedFrom, ParquetCompression, ParquetReader, ParquetWriter, SerReader, Series,
};
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_INPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/usstock_data/baseline_data_1m";
const DEFAULT_ACTIONS: &str =
    "/mnt/hdd-raid5-72t/liang_torch/usstock_data/adjustment_factor/us_equity_adjustment_factor.csv";
const DEFAULT_OUTPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/usstock_data/baseline_data_1min_hfq";
const DEFAULT_START: &str = "2021-07-01";
const DEFAULT_END: &str = "2026-06-30";

const PRICE_COLUMNS: &[&str] = &[
    "bid0p", "ask0p", "open", "high", "low", "close", "midp", "buy_high", "sell_low",
];
const VOLUME_COLUMNS: &[&str] = &[
    "bid0v",
    "ask0v",
    "volume",
    "buy_volume",
    "sell_volume",
    "unknown_volume",
];
const AMOUNT_COLUMNS: &[&str] = &["amount", "buy_amount", "sell_amount", "unknown_amount"];

#[derive(Parser, Debug)]
#[command(name = "usstock_baseline_hfq_1min")]
#[command(about = "Apply audited Yahoo/LSEG action factors to US-stock minute parquet")]
struct Args {
    #[arg(long, default_value = DEFAULT_INPUT)]
    input_root: PathBuf,
    #[arg(long, default_value = DEFAULT_ACTIONS)]
    actions_csv: PathBuf,
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    output_root: PathBuf,
    /// Inclusive session date in YYYY-MM-DD.
    #[arg(long, default_value = DEFAULT_START)]
    start: String,
    /// Inclusive session date in YYYY-MM-DD. Must not exceed the audited action window.
    #[arg(long, default_value = DEFAULT_END)]
    end: String,
    #[arg(long, value_delimiter = ',')]
    rics: Vec<String>,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Deserialize)]
struct ActionCsvRow {
    ric: String,
    effective_ts_utc: String,
    cumulative_price_factor: f64,
    cumulative_volume_factor: f64,
    window_start: String,
    window_end: String,
}

#[derive(Clone, Copy, Debug)]
struct ActionPoint {
    effective_ts_ns: i64,
    price_factor: f64,
    volume_factor: f64,
}

#[derive(Clone, Debug)]
struct ActionChain {
    window_start: NaiveDate,
    window_end: NaiveDate,
    points: Vec<ActionPoint>,
}

impl ActionChain {
    fn factors_at(&self, ts_seconds: i64) -> (f64, f64) {
        let ts_ns = ts_seconds.saturating_mul(1_000_000_000);
        let index = self
            .points
            .partition_point(|point| point.effective_ts_ns <= ts_ns);
        self.points
            .get(index)
            .map(|point| (point.price_factor, point.volume_factor))
            .unwrap_or((1.0, 1.0))
    }
}

#[derive(Clone, Debug)]
struct Job {
    venue: String,
    ric: String,
    day: NaiveDate,
    input: PathBuf,
    output: PathBuf,
    chain: Option<ActionChain>,
}

fn main() -> Result<()> {
    // Outer Rayon workers own file-level parallelism. Avoid a nested Polars pool
    // per worker, which can exhaust the thread stack on large batch runs.
    if std::env::var_os("POLARS_MAX_THREADS").is_none() {
        std::env::set_var("POLARS_MAX_THREADS", "1");
    }
    let args = Args::parse();
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    if args.output_root == args.input_root {
        bail!("HFQ output_root must differ from input_root");
    }
    let start = parse_day(&args.start)?;
    let end = parse_day(&args.end)?;
    if end < start {
        bail!("--end precedes --start");
    }
    let chains = load_action_chains(&args.actions_csv)?;
    let requested: BTreeSet<String> = args
        .rics
        .iter()
        .map(|ric| ric.trim().to_string())
        .filter(|ric| !ric.is_empty())
        .collect();
    let jobs = list_jobs(&args, start, end, &chains, &requested)?;
    if args.dry_run {
        println!(
            "usstock_baseline_hfq_1min dry-run files={} rics={}",
            jobs.len(),
            jobs.iter()
                .map(|job| &job.ric)
                .collect::<BTreeSet<_>>()
                .len()
        );
        return Ok(());
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        // Polars' nested parquet decode/write path exceeds Rayon's default worker stack.
        .stack_size(32 * 1024 * 1024)
        .build()
        .context("build HFQ worker pool")?;
    let files = AtomicU64::new(0);
    let rows = AtomicU64::new(0);
    pool.install(|| {
        jobs.par_iter().try_for_each(|job| -> Result<()> {
            let written = process_job(job)?;
            files.fetch_add(1, Ordering::Relaxed);
            rows.fetch_add(written, Ordering::Relaxed);
            Ok(())
        })
    })?;
    println!(
        "usstock_baseline_hfq_1min complete files={} rows={}",
        files.load(Ordering::Relaxed),
        rows.load(Ordering::Relaxed)
    );
    Ok(())
}

fn parse_day(text: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").with_context(|| format!("invalid date {text}"))
}

fn parse_effective_ts_ns(text: &str) -> Result<i64> {
    DateTime::parse_from_rfc3339(text)
        .with_context(|| format!("invalid effective_ts_utc {text:?}"))?
        .with_timezone(&Utc)
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("effective_ts_utc outside nanosecond range {text:?}"))
}

fn load_action_chains(path: &Path) -> Result<BTreeMap<String, ActionChain>> {
    let mut rows_by_ric: BTreeMap<String, Vec<ActionCsvRow>> = BTreeMap::new();
    let mut reader =
        csv::Reader::from_path(path).with_context(|| format!("read {}", path.display()))?;
    for row in reader.deserialize::<ActionCsvRow>() {
        let row = row.with_context(|| format!("parse {}", path.display()))?;
        if row.ric.trim().is_empty() {
            bail!("empty ric in {}", path.display());
        }
        rows_by_ric.entry(row.ric.clone()).or_default().push(row);
    }
    let mut chains = BTreeMap::new();
    for (ric, mut rows) in rows_by_ric {
        rows.sort_by(|left, right| left.effective_ts_utc.cmp(&right.effective_ts_utc));
        let window_start = parse_day(&rows[0].window_start)?;
        let window_end = parse_day(&rows[0].window_end)?;
        let mut points = Vec::with_capacity(rows.len());
        for row in rows {
            if parse_day(&row.window_start)? != window_start
                || parse_day(&row.window_end)? != window_end
            {
                bail!("inconsistent action coverage window for {ric}");
            }
            if !row.cumulative_price_factor.is_finite()
                || !row.cumulative_volume_factor.is_finite()
                || row.cumulative_price_factor <= 0.0
                || row.cumulative_volume_factor <= 0.0
            {
                bail!("invalid cumulative action factor for {ric}");
            }
            let point = ActionPoint {
                effective_ts_ns: parse_effective_ts_ns(&row.effective_ts_utc)?,
                price_factor: row.cumulative_price_factor,
                volume_factor: row.cumulative_volume_factor,
            };
            if points.last().is_some_and(|previous: &ActionPoint| {
                previous.effective_ts_ns >= point.effective_ts_ns
            }) {
                bail!("duplicate or non-increasing effective action time for {ric}");
            }
            points.push(point);
        }
        chains.insert(
            ric,
            ActionChain {
                window_start,
                window_end,
                points,
            },
        );
    }
    if chains.is_empty() {
        bail!("no action rows in {}", path.display());
    }
    Ok(chains)
}

fn sorted_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut directories = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let path = entry?.path();
        if path.is_dir()
            && !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with('_'))
        {
            directories.push(path);
        }
    }
    directories.sort();
    Ok(directories)
}

fn list_jobs(
    args: &Args,
    start: NaiveDate,
    end: NaiveDate,
    chains: &BTreeMap<String, ActionChain>,
    requested: &BTreeSet<String>,
) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for venue_dir in sorted_dirs(&args.input_root)? {
        let venue = venue_dir
            .file_name()
            .and_then(|name| name.to_str())
            .context("non-UTF8 venue directory")?
            .to_string();
        for ric_dir in sorted_dirs(&venue_dir)? {
            let ric = ric_dir
                .file_name()
                .and_then(|name| name.to_str())
                .context("non-UTF8 RIC directory")?
                .to_string();
            if !requested.is_empty() && !requested.contains(&ric) {
                continue;
            }
            let chain = chains.get(&ric).cloned();
            for entry in
                fs::read_dir(&ric_dir).with_context(|| format!("read {}", ric_dir.display()))?
            {
                let input = entry?.path();
                if input.extension().and_then(|extension| extension.to_str()) != Some("parquet") {
                    continue;
                }
                let stem = input
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .unwrap_or("");
                let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") else {
                    continue;
                };
                if day < start || day > end {
                    continue;
                }
                if let Some(chain) = &chain {
                    if day < chain.window_start || day > chain.window_end {
                        bail!(
                            "input day {day} is outside audited action window {}..={} for {ric}",
                            chain.window_start,
                            chain.window_end
                        );
                    }
                }
                let output = args
                    .output_root
                    .join(&venue)
                    .join(&ric)
                    .join(format!("{stem}.parquet"));
                if output.exists() && !args.overwrite {
                    continue;
                }
                jobs.push(Job {
                    venue: venue.clone(),
                    ric: ric.clone(),
                    day,
                    input,
                    output,
                    chain: chain.clone(),
                });
            }
        }
    }
    jobs.sort_by(|left, right| left.ric.cmp(&right.ric).then(left.day.cmp(&right.day)));
    if jobs.is_empty() {
        bail!(
            "no eligible input parquet files under {}",
            args.input_root.display()
        );
    }
    Ok(jobs)
}

fn multiply_column(df: &mut DataFrame, name: &str, factors: &[f64]) -> Result<()> {
    let values = df
        .column(name)
        .with_context(|| format!("missing required HFQ column {name}"))?
        .f64()
        .with_context(|| format!("HFQ column {name} must be Float64"))?
        .into_iter()
        .zip(factors)
        .map(|(value, factor)| {
            value.map(|number| {
                if number.is_finite() {
                    number * factor
                } else {
                    number
                }
            })
        })
        .collect::<Vec<_>>();
    df.replace(name, Series::new(name.into(), values))?;
    Ok(())
}

fn validate_identity(df: &DataFrame, job: &Job) -> Result<Vec<i64>> {
    let rics = df.column("ric")?.str()?.into_iter().collect::<Vec<_>>();
    let venues = df.column("venue")?.str()?.into_iter().collect::<Vec<_>>();
    let timestamps = df.column("ts")?.i64()?.into_iter().collect::<Vec<_>>();
    let mut output = Vec::with_capacity(df.height());
    let mut previous = None;
    for index in 0..df.height() {
        if rics[index] != Some(job.ric.as_str()) || venues[index] != Some(job.venue.as_str()) {
            bail!("identity mismatch row {index} in {}", job.input.display());
        }
        let ts = timestamps[index].context("null timestamp in baseline input")?;
        if ts.rem_euclid(60) != 0 || previous.is_some_and(|previous| ts <= previous) {
            bail!(
                "invalid minute timestamp row {index} in {}",
                job.input.display()
            );
        }
        previous = Some(ts);
        output.push(ts);
    }
    Ok(output)
}

fn apply_hfq(mut df: DataFrame, job: &Job) -> Result<DataFrame> {
    let timestamps = validate_identity(&df, job)?;
    let (price_factors, volume_factors): (Vec<_>, Vec<_>) = timestamps
        .iter()
        .map(|timestamp| {
            job.chain
                .as_ref()
                .map(|chain| chain.factors_at(*timestamp))
                .unwrap_or((1.0, 1.0))
        })
        .unzip();
    for name in PRICE_COLUMNS {
        multiply_column(&mut df, name, &price_factors)?;
    }
    for level in 0..10 {
        for side in ["l2_bid", "l2_ask"] {
            multiply_column(&mut df, &format!("{side}{level}p"), &price_factors)?;
            multiply_column(&mut df, &format!("{side}{level}v"), &volume_factors)?;
        }
    }
    for name in VOLUME_COLUMNS {
        multiply_column(&mut df, name, &volume_factors)?;
    }
    let amount_factors = price_factors
        .iter()
        .zip(&volume_factors)
        .map(|(price, volume)| price * volume)
        .collect::<Vec<_>>();
    for name in AMOUNT_COLUMNS {
        multiply_column(&mut df, name, &amount_factors)?;
    }
    Ok(df)
}

fn write_atomic(path: &Path, mut df: DataFrame) -> Result<()> {
    let parent = path
        .parent()
        .context("HFQ output has no parent directory")?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let temporary = path.with_extension("parquet.tmp");
    let result = (|| -> Result<()> {
        ParquetWriter::new(File::create(&temporary)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn process_job(job: &Job) -> Result<u64> {
    let frame = ParquetReader::new(File::open(&job.input)?)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", job.input.display()))?;
    let rows = frame.height() as u64;
    let adjusted = apply_hfq(frame, job)?;
    write_atomic(&job.output, adjusted)
        .with_context(|| format!("write {}", job.output.display()))?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_factor_starts_strictly_before_the_ex_date_open() {
        let chain = ActionChain {
            window_start: NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
            window_end: NaiveDate::from_ymd_opt(2021, 12, 31).unwrap(),
            points: vec![ActionPoint {
                effective_ts_ns: 1_000_000_000,
                price_factor: 0.5,
                volume_factor: 2.0,
            }],
        };
        assert_eq!(chain.factors_at(0), (0.5, 2.0));
        assert_eq!(chain.factors_at(1), (1.0, 1.0));
    }
}
