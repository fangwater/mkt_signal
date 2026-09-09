//! Compute uniform trade-only LSEG factors from US-stock HFQ minute parquet.
//!
//! The source package has real LL2 for only part of the RIC universe. This
//! replay deliberately never reads book columns, so every RIC shares one
//! trade-only feature contract. Unavailable size buckets and depth formulas
//! are represented by IEEE NaN, never invented as zero-valued observations.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
#[cfg(test)]
use mkt_signal::factor_pub::lseg_features::LSEG_TRADE_FIELD_COUNT;
use mkt_signal::factor_pub::lseg_features::{
    LsegFactorPlan, LsegTradeBar, LsegTradeOnlyFeatureState, LSEG_ALL_FACTORS,
};
use polars::prelude::{
    DataFrame, Float64Chunked, Int32Chunked, Int64Chunked, NamedFrom, ParquetCompression,
    ParquetReader, ParquetWriter, SerReader, Series, StringChunked,
};
use rayon::prelude::*;
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_INPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/usstock_data/baseline_data_1min_hfq";
const DEFAULT_OUTPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/usstock_data/baseline_factor_1min_hfq";
const DEFAULT_START: &str = "2021-07-01";
const DEFAULT_END: &str = "2026-06-30";

#[derive(Parser, Debug)]
#[command(name = "usstock_features_1min_hfq")]
#[command(about = "Compute trade-only LSEG factors from US-stock HFQ minute parquet")]
struct Args {
    #[arg(long, default_value = DEFAULT_INPUT)]
    input_root: PathBuf,
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    output_root: PathBuf,
    /// Inclusive session date in YYYY-MM-DD.
    #[arg(long, default_value = DEFAULT_START)]
    start: String,
    /// Inclusive session date in YYYY-MM-DD.
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

#[derive(Clone, Debug)]
struct DayFile {
    day: NaiveDate,
    path: PathBuf,
}

#[derive(Clone, Debug)]
struct RicJob {
    venue: String,
    ric: String,
    days: Vec<DayFile>,
}

#[derive(Debug)]
struct OutputRow {
    ts: i64,
    factors: Vec<f64>,
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
    if args.input_root == args.output_root {
        bail!("factor output_root must differ from input_root");
    }
    let start = parse_day(&args.start)?;
    let end = parse_day(&args.end)?;
    if end < start {
        bail!("--end precedes --start");
    }
    let requested: BTreeSet<String> = args
        .rics
        .iter()
        .map(|ric| ric.trim().to_string())
        .filter(|ric| !ric.is_empty())
        .collect();
    let jobs = list_jobs(&args.input_root, start, end, &requested)?;
    let planned_files = jobs.iter().map(|job| job.days.len()).sum::<usize>();
    if args.dry_run {
        println!(
            "usstock_features_1min_hfq dry-run rics={} files={planned_files}",
            jobs.len()
        );
        return Ok(());
    }
    let plan = LsegFactorPlan::from_factor_names(vec![LSEG_ALL_FACTORS.to_string()])?;
    let factor_names = plan
        .factor_names()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        // Polars' nested parquet decode/write path exceeds Rayon's default worker stack.
        .stack_size(32 * 1024 * 1024)
        .build()
        .context("build US-stock feature worker pool")?;
    let files = AtomicU64::new(0);
    let rows = AtomicU64::new(0);
    pool.install(|| {
        jobs.par_iter().try_for_each(|job| -> Result<()> {
            let (written_files, written_rows) = replay_ric(&args, &plan, &factor_names, job)?;
            files.fetch_add(written_files, Ordering::Relaxed);
            rows.fetch_add(written_rows, Ordering::Relaxed);
            Ok(())
        })
    })?;
    println!(
        "usstock_features_1min_hfq complete files={} rows={} factors={}",
        files.load(Ordering::Relaxed),
        rows.load(Ordering::Relaxed),
        factor_names.len()
    );
    Ok(())
}

fn parse_day(text: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").with_context(|| format!("invalid date {text}"))
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
    root: &Path,
    start: NaiveDate,
    end: NaiveDate,
    requested: &BTreeSet<String>,
) -> Result<Vec<RicJob>> {
    let mut jobs = Vec::new();
    for venue_dir in sorted_dirs(root)? {
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
            let mut days = Vec::new();
            for entry in
                fs::read_dir(&ric_dir).with_context(|| format!("read {}", ric_dir.display()))?
            {
                let path = entry?.path();
                if path.extension().and_then(|extension| extension.to_str()) != Some("parquet") {
                    continue;
                }
                let stem = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .unwrap_or("");
                let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") else {
                    continue;
                };
                if day >= start && day <= end {
                    days.push(DayFile { day, path });
                }
            }
            days.sort_by_key(|file| file.day);
            if !days.is_empty() {
                jobs.push(RicJob {
                    venue: venue.clone(),
                    ric,
                    days,
                });
            }
        }
    }
    jobs.sort_by(|left, right| left.ric.cmp(&right.ric));
    if jobs.is_empty() {
        bail!("no eligible HFQ parquet files under {}", root.display());
    }
    Ok(jobs)
}

fn replay_ric(
    args: &Args,
    plan: &LsegFactorPlan,
    factor_names: &[String],
    job: &RicJob,
) -> Result<(u64, u64)> {
    let state_symbol = state_symbol(&job.venue, &job.ric)?;
    let mut state = LsegTradeOnlyFeatureState::default();
    let mut previous_ts = None;
    let mut pending_factors = None;
    let mut files = 0u64;
    let mut rows = 0u64;
    for day in &job.days {
        let output = args
            .output_root
            .join(&job.venue)
            .join(&job.ric)
            .join(format!("{}.parquet", day.day.format("%Y%m%d")));
        let output_rows = process_day(
            &day.path,
            &job.venue,
            &job.ric,
            &state_symbol,
            &mut state,
            &mut previous_ts,
            &mut pending_factors,
            plan,
        )?;
        if output.exists() && !args.overwrite {
            continue;
        }
        write_day(&output, &job.venue, &job.ric, factor_names, &output_rows)?;
        files += 1;
        rows += output_rows.len() as u64;
    }
    Ok((files, rows))
}

#[allow(clippy::too_many_arguments)]
fn process_day(
    path: &Path,
    venue: &str,
    ric: &str,
    state_symbol: &str,
    state: &mut LsegTradeOnlyFeatureState,
    previous_ts: &mut Option<i64>,
    pending_factors: &mut Option<Vec<f64>>,
    plan: &LsegFactorPlan,
) -> Result<Vec<OutputRow>> {
    let frame = ParquetReader::new(File::open(path)?)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", path.display()))?;
    let rics = string_column(&frame, "ric")?;
    let venues = string_column(&frame, "venue")?;
    let timestamps = i64_column(&frame, "ts")?;
    let open = f64_column(&frame, "open")?;
    let high = f64_column(&frame, "high")?;
    let low = f64_column(&frame, "low")?;
    let close = f64_column(&frame, "close")?;
    let volume = f64_column(&frame, "volume")?;
    let amount = f64_column(&frame, "amount")?;
    let count = i32_column(&frame, "count")?;
    let buy_count = i32_column(&frame, "buy_count")?;
    let sell_count = i32_column(&frame, "sell_count")?;
    let buy_amount = f64_column(&frame, "buy_amount")?;
    let sell_amount = f64_column(&frame, "sell_amount")?;
    let buy_volume = f64_column(&frame, "buy_volume")?;
    let sell_volume = f64_column(&frame, "sell_volume")?;

    let mut output = Vec::with_capacity(frame.height());
    for index in 0..frame.height() {
        if rics.get(index) != Some(ric) || venues.get(index) != Some(venue) {
            bail!("identity mismatch row {index} in {}", path.display());
        }
        let ts = timestamps
            .get(index)
            .with_context(|| format!("null ts row {index} in {}", path.display()))?;
        if ts.rem_euclid(60) != 0 {
            bail!("non-minute ts {ts} in {}", path.display());
        }
        let segment_break = match *previous_ts {
            Some(previous) if ts <= previous => {
                bail!("non-increasing ts {previous} -> {ts} in {}", path.display());
            }
            Some(previous) => ts - previous != 60,
            None => false,
        };
        let trade = trade_bar(
            f64_at(open, index),
            f64_at(high, index),
            f64_at(low, index),
            f64_at(close, index),
            f64_at(volume, index),
            f64_at(amount, index),
            i32_at(count, index),
            i32_at(buy_count, index),
            i32_at(sell_count, index),
            f64_at(buy_amount, index),
            f64_at(sell_amount, index),
            f64_at(buy_volume, index),
            f64_at(sell_volume, index),
        );
        let factors = take_shifted_factors(pending_factors, segment_break, plan.len());
        state
            .push(ts * 1_000, state_symbol, trade, segment_break)
            .with_context(|| format!("advance trade-only state {ric} ts={ts}"))?;
        *pending_factors = Some(
            state
                .factor_values(plan)?
                .into_iter()
                .map(|value| value.unwrap_or(f64::NAN))
                .collect(),
        );
        *previous_ts = Some(ts);
        output.push(OutputRow { ts, factors });
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn trade_bar(
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    amount: f64,
    count: f64,
    buy_count: f64,
    sell_count: f64,
    buy_amount: f64,
    sell_amount: f64,
    buy_volume: f64,
    sell_volume: f64,
) -> LsegTradeBar {
    let average_amount = ratio(amount, count);
    let vwap = ratio(amount, volume);
    let buy_vwap = ratio(buy_amount, buy_volume);
    let sell_vwap = ratio(sell_amount, sell_volume);
    let net_buy_amount = difference(buy_amount, sell_amount);
    let net_buy_volume = difference(buy_volume, sell_volume);
    let net_buy_pct = ratio(net_buy_amount, amount);
    // The base has no KLL bucket replay. NaN preserves the distinction between
    // unavailable bucket attribution and a measured zero-sized bucket.
    let unavailable = f64::NAN;
    LsegTradeBar::from_slice(&[
        open,
        high,
        low,
        close,
        volume,
        amount,
        average_amount,
        count,
        buy_count,
        sell_count,
        buy_amount,
        sell_amount,
        buy_volume,
        sell_volume,
        unavailable,
        unavailable,
        unavailable,
        unavailable,
        unavailable,
        unavailable,
        unavailable,
        unavailable,
        unavailable,
        vwap,
        buy_vwap,
        sell_vwap,
        net_buy_amount,
        net_buy_volume,
        net_buy_pct,
        unavailable,
        unavailable,
        unavailable,
    ])
    .expect("US-stock trade bar field count is fixed")
}

fn ratio(numerator: f64, denominator: f64) -> f64 {
    if numerator.is_finite() && denominator.is_finite() && denominator > 0.0 {
        numerator / denominator
    } else {
        f64::NAN
    }
}

fn difference(left: f64, right: f64) -> f64 {
    if left.is_finite() && right.is_finite() {
        left - right
    } else {
        f64::NAN
    }
}

fn take_shifted_factors(
    pending_factors: &mut Option<Vec<f64>>,
    segment_break: bool,
    factor_count: usize,
) -> Vec<f64> {
    if segment_break {
        *pending_factors = None;
    }
    pending_factors
        .take()
        .unwrap_or_else(|| vec![f64::NAN; factor_count])
}

fn state_symbol(venue: &str, ric: &str) -> Result<String> {
    let value = format!("{venue}:{}", ric.replace('.', "-"));
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'-'))
    {
        bail!("unsupported RIC for factor state {ric:?}");
    }
    Ok(value)
}

fn write_day(
    path: &Path,
    venue: &str,
    ric: &str,
    factor_names: &[String],
    rows: &[OutputRow],
) -> Result<()> {
    if rows
        .iter()
        .any(|row| row.factors.len() != factor_names.len())
    {
        bail!("factor row width does not match factor schema");
    }
    let mut ts = Vec::with_capacity(rows.len());
    let mut factor_columns = factor_names
        .iter()
        .map(|_| Vec::with_capacity(rows.len()))
        .collect::<Vec<Vec<f64>>>();
    for row in rows {
        ts.push(row.ts);
        for (index, value) in row.factors.iter().copied().enumerate() {
            factor_columns[index].push(value);
        }
    }
    let n = rows.len();
    let mut columns = vec![
        Series::new("venue".into(), vec![venue.to_string(); n]),
        Series::new("ric".into(), vec![ric.to_string(); n]),
        Series::new("ts".into(), ts),
        Series::new(
            "source_depth_ts_utc_ns".into(),
            vec![Option::<i64>::None; n],
        ),
    ];
    for (name, values) in factor_names.iter().zip(factor_columns) {
        columns.push(Series::new(name.as_str().into(), values));
    }
    let mut frame = DataFrame::new(columns).context("create factor dataframe")?;
    let parent = path
        .parent()
        .context("factor output has no parent directory")?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
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

fn string_column<'a>(frame: &'a DataFrame, name: &str) -> Result<&'a StringChunked> {
    frame
        .column(name)
        .with_context(|| format!("missing {name}"))?
        .str()
        .with_context(|| format!("{name} must be Utf8"))
}

fn i64_column<'a>(frame: &'a DataFrame, name: &str) -> Result<&'a Int64Chunked> {
    frame
        .column(name)
        .with_context(|| format!("missing {name}"))?
        .i64()
        .with_context(|| format!("{name} must be Int64"))
}

fn i32_column<'a>(frame: &'a DataFrame, name: &str) -> Result<&'a Int32Chunked> {
    frame
        .column(name)
        .with_context(|| format!("missing {name}"))?
        .i32()
        .with_context(|| format!("{name} must be Int32"))
}

fn f64_column<'a>(frame: &'a DataFrame, name: &str) -> Result<&'a Float64Chunked> {
    frame
        .column(name)
        .with_context(|| format!("missing {name}"))?
        .f64()
        .with_context(|| format!("{name} must be Float64"))
}

fn f64_at(column: &Float64Chunked, index: usize) -> f64 {
    column.get(index).unwrap_or(f64::NAN)
}

fn i32_at(column: &Int32Chunked, index: usize) -> f64 {
    column.get(index).map(f64::from).unwrap_or(f64::NAN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_bar_keeps_unavailable_size_buckets_as_nan() {
        let bar = trade_bar(
            100.0, 101.0, 99.0, 100.5, 10.0, 1_000.0, 4.0, 2.0, 1.0, 700.0, 200.0, 7.0, 2.0,
        );
        assert_eq!(bar.values.len(), LSEG_TRADE_FIELD_COUNT);
        assert_eq!(bar.values[23], 100.0);
        assert_eq!(bar.values[26], 500.0);
        assert!(bar.values[14].is_nan());
        assert!(bar.values[31].is_nan());
    }

    #[test]
    fn segment_break_discards_pending_factor_vector() {
        let mut pending = Some(vec![1.0, 2.0]);
        assert!(take_shifted_factors(&mut pending, true, 2)
            .into_iter()
            .all(f64::is_nan));
    }
}
