//! Read-only Tardis trades -> hourly KLL -> monthly order-size thresholds.

use anyhow::{bail, ensure, Context, Result};
use chrono::{Datelike, NaiveDate, Utc};
use clap::Parser;
use flate2::read::MultiGzDecoder;
use rayon::prelude::*;
use rolling_common::kll_quantile::{FrozenKllSketch, StreamingKllSketch};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, UNIX_EPOCH};

const HOUR_US: i64 = 3_600_000_000;
const HEADER: [&str; 8] = [
    "exchange",
    "symbol",
    "timestamp",
    "local_timestamp",
    "id",
    "side",
    "price",
    "amount",
];

#[derive(Debug, Parser, Serialize)]
#[command(
    about = "Compute monthly P50/P90 trade-notional thresholds from read-only Tardis gzip CSVs"
)]
struct Args {
    /// TardisSource root containing binance_usd_SYMBOL / binance_spot_SYMBOL.
    #[arg(long)]
    root: PathBuf,
    /// Sample month, YYYY-MM; defaults to the previous complete UTC calendar month.
    #[arg(long)]
    month: Option<String>,
    /// New output directory; an existing directory is never overwritten.
    #[arg(long)]
    output: PathBuf,
    /// Comma-separated symbols, or use --symbols-file.
    #[arg(
        long,
        value_delimiter = ',',
        conflicts_with = "symbols_file",
        required_unless_present = "symbols_file"
    )]
    symbols: Vec<String>,
    /// Whitespace/comma-separated USDT symbols.
    #[arg(long, conflicts_with = "symbols")]
    symbols_file: Option<PathBuf>,
    #[arg(long, value_delimiter = ',', default_value = "binance-futures,binance")]
    exchanges: Vec<String>,
    #[arg(long, default_value_t = 8)]
    workers: usize,
    #[arg(long, default_value_t = 0.5)]
    medium_quantile: f32,
    #[arg(long, default_value_t = 0.9)]
    large_quantile: f32,
}

struct Month {
    name: String,
    apply_month: String,
    days: Vec<NaiveDate>,
    start_us: i64,
    end_us: i64,
}

impl Month {
    fn parse(value: &str) -> Result<Self> {
        ensure!(value.len() == 7, "month must use YYYY-MM");
        let start = NaiveDate::parse_from_str(&format!("{value}-01"), "%Y-%m-%d")?;
        let (year, month) = if start.month() == 12 {
            (start.year() + 1, 1)
        } else {
            (start.year(), start.month() + 1)
        };
        let end = NaiveDate::from_ymd_opt(year, month, 1).context("invalid month end")?;
        let days = start.iter_days().take_while(|day| *day < end).collect();
        Ok(Self {
            name: value.to_string(),
            apply_month: end.format("%Y-%m").to_string(),
            days,
            start_us: start
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_micros(),
            end_us: end
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_micros(),
        })
    }
}

#[derive(Serialize)]
struct Job {
    exchange: String,
    symbol: String,
    paths: Vec<PathBuf>,
}

#[derive(Default, Serialize)]
struct Stats {
    source_rows: u64,
    sample_count: u64,
    invalid_values: u64,
    outside_month: u64,
    late_trades: u64,
    nonempty_hours: usize,
    first_timestamp_us: Option<i64>,
    last_timestamp_us: Option<i64>,
}

#[derive(Serialize)]
struct SourceFile {
    path: PathBuf,
    bytes: u64,
    modified_unix_ns: u128,
    sha256: String,
    source_rows: u64,
    sample_count: u64,
}

#[derive(Serialize)]
struct SketchOutput {
    sample_count: usize,
    level_capacity: usize,
    levels: Vec<Vec<f64>>,
}

#[derive(Serialize)]
struct Calculation {
    medium_notional_threshold: f64,
    large_notional_threshold: f64,
    stats: Stats,
    files: Vec<SourceFile>,
    sketch: SketchOutput,
    elapsed_seconds: f64,
}

#[derive(Serialize)]
struct JobOutput {
    exchange: String,
    symbol: String,
    source_month: String,
    apply_month: String,
    status: String,
    error: Option<String>,
    calculation: Option<Calculation>,
}

struct HashingReader<R> {
    inner: R,
    hash: Sha256,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hash.update(&buf[..n]);
        Ok(n)
    }
}

fn calculate(job: &Job, month: &Month, args: &Args) -> Result<Calculation> {
    let started = Instant::now();
    let missing: Vec<_> = job.paths.iter().filter(|path| !path.is_file()).collect();
    ensure!(missing.is_empty(), "missing source files: {missing:?}");
    let mut stats = Stats::default();
    let mut files = Vec::new();
    let mut current_hour = None;
    let mut current = StreamingKllSketch::new();
    let mut hourly = Vec::new();
    for path in &job.paths {
        let metadata = fs::metadata(path)?;
        let modified = metadata.modified()?;
        let hashing = HashingReader {
            inner: BufReader::with_capacity(1 << 20, File::open(path)?),
            hash: Sha256::new(),
        };
        let mut reader = csv::Reader::from_reader(MultiGzDecoder::new(hashing));
        ensure!(
            reader.headers()?.iter().eq(HEADER),
            "unexpected CSV header: {}",
            path.display()
        );
        let before_rows = stats.source_rows;
        let before_samples = stats.sample_count;
        let mut record = csv::StringRecord::new();
        while reader
            .read_record(&mut record)
            .with_context(|| format!("read {}", path.display()))?
        {
            stats.source_rows += 1;
            let parsed = parse_record(&record, job)
                .with_context(|| format!("{}:{}", path.display(), reader.position().line() - 1))?;
            let Some((timestamp, notional)) = parsed else {
                stats.invalid_values += 1;
                continue;
            };
            if timestamp < month.start_us || timestamp >= month.end_us {
                stats.outside_month += 1;
                continue;
            }
            let hour = timestamp.div_euclid(HOUR_US);
            if current_hour.is_some_and(|previous| hour < previous) {
                stats.late_trades += 1;
                continue;
            }
            if current_hour != Some(hour) {
                if !current.is_empty() {
                    hourly.push(current.freeze());
                    current.reset();
                }
                current_hour = Some(hour);
            }
            current.insert(notional);
            stats.sample_count += 1;
            stats.first_timestamp_us = Some(
                stats
                    .first_timestamp_us
                    .map_or(timestamp, |v| v.min(timestamp)),
            );
            stats.last_timestamp_us = Some(
                stats
                    .last_timestamp_us
                    .map_or(timestamp, |v| v.max(timestamp)),
            );
        }
        let hashing = reader.into_inner().into_inner();
        let after = fs::metadata(path)?;
        ensure!(
            after.len() == metadata.len() && after.modified()? == modified,
            "source changed during read: {}",
            path.display()
        );
        files.push(SourceFile {
            path: path.clone(),
            bytes: metadata.len(),
            modified_unix_ns: modified.duration_since(UNIX_EPOCH)?.as_nanos(),
            sha256: format!("{:x}", hashing.hash.finalize()),
            source_rows: stats.source_rows - before_rows,
            sample_count: stats.sample_count - before_samples,
        });
    }
    if !current.is_empty() {
        hourly.push(current.freeze());
    }
    stats.nonempty_hours = hourly.len();
    let expected_hours = ((month.end_us - month.start_us) / HOUR_US) as usize;
    ensure!(hourly.len() == expected_hours,
        "incomplete hourly coverage: expected {expected_hours}, found {}; samples={}, invalid={}, late={}, outside={}",
        hourly.len(), stats.sample_count, stats.invalid_values, stats.late_trades, stats.outside_month);
    let sketch = FrozenKllSketch::merge_all(hourly.iter())?.context("no valid samples")?;
    ensure!(
        sketch.sample_count as u64 == stats.sample_count,
        "sample-count mismatch"
    );
    let quantiles = sketch.quantiles(&[args.medium_quantile, args.large_quantile]);
    let medium = quantiles[0].context("missing medium quantile")?;
    let large = quantiles[1].context("missing large quantile")?;
    ensure!(
        medium.is_finite() && large.is_finite() && medium > 0.0 && large >= medium,
        "invalid thresholds"
    );
    Ok(Calculation {
        medium_notional_threshold: medium,
        large_notional_threshold: large,
        stats,
        files,
        sketch: SketchOutput {
            sample_count: sketch.sample_count,
            level_capacity: sketch.level_capacity,
            levels: sketch.levels,
        },
        elapsed_seconds: started.elapsed().as_secs_f64(),
    })
}

fn parse_record(row: &csv::StringRecord, job: &Job) -> Result<Option<(i64, f64)>> {
    ensure!(row.len() == HEADER.len(), "wrong column count");
    ensure!(
        row[0] == job.exchange && row[1] == job.symbol,
        "exchange/symbol mismatch"
    );
    let timestamp: i64 = row[2].parse().context("invalid timestamp")?;
    let id: i64 = row[4].parse().context("invalid trade id")?;
    ensure!(
        timestamp > 0 && id > 0,
        "timestamp and trade id must be positive"
    );
    ensure!(
        row[5].eq_ignore_ascii_case("buy") || row[5].eq_ignore_ascii_case("sell"),
        "invalid side"
    );
    let price: f64 = row[6].parse().context("invalid price")?;
    let amount: f64 = row[7].parse().context("invalid amount")?;
    let notional = price * amount;
    if [price, amount, notional]
        .iter()
        .any(|v| !v.is_finite() || *v <= 0.0)
    {
        return Ok(None);
    }
    Ok(Some((timestamp, notional)))
}

fn symbols(args: &Args) -> Result<Vec<String>> {
    let raw = match &args.symbols_file {
        Some(path) => fs::read_to_string(path)?,
        None => args.symbols.join(","),
    };
    let symbols: Vec<_> = raw
        .split(|c: char| c.is_whitespace() || c == ',')
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_uppercase)
        .collect();
    ensure!(!symbols.is_empty(), "empty symbol list");
    let mut seen = BTreeSet::new();
    for symbol in &symbols {
        ensure!(
            symbol.len() > 4
                && symbol.ends_with("USDT")
                && symbol.bytes().all(|c| c.is_ascii_alphanumeric()),
            "expected an alphanumeric USDT symbol, got {symbol}"
        );
        ensure!(seen.insert(symbol), "duplicate symbol: {symbol}");
    }
    Ok(symbols)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = File::options()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut data = serde_json::to_vec_pretty(value)?;
    data.push(b'\n');
    atomic_write(path, &data)
}

fn source_fingerprint() -> String {
    let mut hash = Sha256::new();
    hash.update(include_bytes!("main.rs"));
    hash.update(include_bytes!("../Cargo.toml"));
    hash.update(include_bytes!("../../rolling_common/src/kll_quantile.rs"));
    hash.update(include_bytes!("../../../Cargo.lock"));
    format!("{:x}", hash.finalize())
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.workers > 0, "workers must be positive");
    ensure!(
        args.medium_quantile.is_finite()
            && args.large_quantile.is_finite()
            && 0.0 <= args.medium_quantile
            && args.medium_quantile <= args.large_quantile
            && args.large_quantile <= 1.0,
        "quantiles must satisfy 0 <= medium <= large <= 1"
    );
    let previous = Utc::now()
        .date_naive()
        .with_day(1)
        .unwrap()
        .pred_opt()
        .unwrap();
    let month = Month::parse(
        &args
            .month
            .clone()
            .unwrap_or_else(|| previous.format("%Y-%m").to_string()),
    )?;
    let symbols = symbols(&args)?;
    let root = fs::canonicalize(&args.root).context("resolve input root")?;
    let mut jobs = Vec::new();
    let mut seen = BTreeSet::new();
    for exchange in &args.exchanges {
        ensure!(seen.insert(exchange), "duplicate exchange: {exchange}");
        let prefix = match exchange.as_str() {
            "binance-futures" => "binance_usd",
            "binance" => "binance_spot",
            _ => bail!("unsupported exchange: {exchange}"),
        };
        for symbol in &symbols {
            let dir = root
                .join(format!("{prefix}_{}", symbol.to_ascii_lowercase()))
                .join("trades");
            let paths = month
                .days
                .iter()
                .map(|day| dir.join(format!("{exchange}_trades_{day}_{symbol}.csv.gz")))
                .collect();
            jobs.push(Job {
                exchange: exchange.clone(),
                symbol: symbol.clone(),
                paths,
            });
        }
    }
    ensure!(!jobs.is_empty(), "no exchanges selected");
    // Outputs must live outside the raw source tree, including through symlinks.
    let parent = args
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let canonical_parent = fs::canonicalize(parent).context("output parent must already exist")?;
    ensure!(
        !canonical_parent.starts_with(&root),
        "output must be outside the input root"
    );
    fs::create_dir(&args.output).context("output directory must be new")?;
    fs::create_dir(args.output.join("audit"))?;
    let started_at = Utc::now().to_rfc3339();
    let started = Instant::now();
    let manifest = serde_json::json!({
        "args": args, "source_month": month.name, "apply_month": month.apply_month,
        "start_us_inclusive": month.start_us, "end_us_exclusive": month.end_us,
        "tool_source_sha256": source_fingerprint(), "started_at_utc": started_at,
        "algorithm": "hourly KLL (capacity 512), chronological merge_all, weighted linear quantiles with f32 q",
        "unit": "USDT", "jobs": jobs,
    });
    write_json(&args.output.join("manifest.json"), &manifest)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build()?;
    let results: Vec<JobOutput> = pool.install(|| {
        jobs.par_iter()
            .map(|job| -> Result<JobOutput> {
                let (status, error, calculation) = match calculate(job, &month, &args) {
                    Ok(value) => ("complete", None, Some(value)),
                    Err(error) => ("failed", Some(format!("{error:#}")), None),
                };
                let output = JobOutput {
                    exchange: job.exchange.clone(),
                    symbol: job.symbol.clone(),
                    source_month: month.name.clone(),
                    apply_month: month.apply_month.clone(),
                    status: status.to_string(),
                    error,
                    calculation,
                };
                write_json(
                    &args
                        .output
                        .join("audit")
                        .join(format!("{}_{}.json", job.exchange, job.symbol)),
                    &output,
                )?;
                eprintln!(
                    "{} {} {} samples={} elapsed={:.1}s{}",
                    job.exchange,
                    job.symbol,
                    status,
                    output
                        .calculation
                        .as_ref()
                        .map_or(0, |c| c.stats.sample_count),
                    started.elapsed().as_secs_f64(),
                    output
                        .error
                        .as_ref()
                        .map_or(String::new(), |error| format!(" error={error}"))
                );
                Ok(output)
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let failures = results.iter().filter(|r| r.status != "complete").count();
    let mut csv = csv::Writer::from_writer(Vec::new());
    csv.write_record([
        "exchange",
        "symbol",
        "source_month",
        "apply_month",
        "medium_quantile",
        "large_quantile",
        "medium_notional_threshold",
        "large_notional_threshold",
        "sample_count",
        "nonempty_hours",
    ])?;
    for row in &results {
        if let Some(c) = &row.calculation {
            csv.serialize((
                &row.exchange,
                &row.symbol,
                &row.source_month,
                &row.apply_month,
                args.medium_quantile,
                args.large_quantile,
                c.medium_notional_threshold,
                c.large_notional_threshold,
                c.stats.sample_count,
                c.stats.nonempty_hours,
            ))?;
        }
    }
    atomic_write(&args.output.join("thresholds.csv"), &csv.into_inner()?)?;
    write_json(
        &args.output.join("results.json"),
        &serde_json::json!({
            "status": if failures == 0 { "complete" } else { "partial" },
            "requested": jobs.len(), "completed": jobs.len() - failures, "failed": failures,
            "source_month": month.name, "apply_month": month.apply_month,
            "medium_quantile": args.medium_quantile, "large_quantile": args.large_quantile,
            "elapsed_seconds": started.elapsed().as_secs_f64(), "finished_at_utc": Utc::now().to_rfc3339(),
            "results": results,
        }),
    )?;
    ensure!(
        failures == 0,
        "{failures} jobs failed; inspect results.json (CSV contains successful jobs only)"
    );
    eprintln!(
        "completed {} jobs in {:.1}s: {}",
        jobs.len(),
        started.elapsed().as_secs_f64(),
        args.output.display()
    );
    Ok(())
}
