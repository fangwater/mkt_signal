//! Replay `cn_features` from stitched `baseline_data_1min_hfq` parquet.
//!
//! Output: `{output_root}/{exchange}/{product}/{YYYYMMDD}.parquet`
//! VWAP in the input is already quoted. `volume_multiple_verified` is true so
//! the engine does not hide the three VWAP fields. Empty minutes without a
//! two-sided book are skipped. Segment breaks follow the 1min tea / 30-minute
//! gap rule and TradDay changes.

use anyhow::{bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::Asia::Shanghai;
use clap::Parser;
use log::info;
use mkt_signal::factor_pub::cn_features::{
    FuturesDepth5, FuturesFactorPlan, FuturesFusionInput, FuturesFusionState, FuturesTradeBar,
    FUTURES_TRADE_FIELD_COUNT, FUTURES_TRADE_FIELD_NAMES, QUALITY_SEGMENT_BREAK,
};
use polars::prelude::{
    DataFrame, Float64Chunked, NamedFrom, ParquetReader, ParquetWriter, SerReader, Series,
    StringChunked,
};
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const SESSION_BREAK_SEC: i64 = 1_800;
const TEA_MIN_GAP_SEC: i64 = 480;
const DEFAULT_IN: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min_hfq";
const DEFAULT_OUT: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_factor_1min_hfq";

#[derive(Parser, Debug)]
#[command(name = "cn_features_1min_hfq")]
#[command(about = "Compute cn_features from baseline_data_1min_hfq parquet")]
struct Args {
    #[arg(long, default_value = DEFAULT_IN)]
    in_root: PathBuf,
    #[arg(long, default_value = DEFAULT_OUT)]
    out_root: PathBuf,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
    #[arg(long, default_value_t = 8)]
    workers: usize,
    #[arg(long)]
    product: Option<String>,
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    dry_run: bool,
}

struct DayFile {
    day: NaiveDate,
    path: PathBuf,
}

struct InputRow {
    ts: i64,
    trading_day: u32,
    quality_flags: u32,
    trade: [f64; FUTURES_TRADE_FIELD_COUNT],
    depth: FuturesDepth5,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    if args.out_root.to_string_lossy().contains("cme_tas_rocksdb")
        || args.in_root.to_string_lossy().contains("cme_tas_rocksdb")
    {
        bail!("refusing to touch a CME RocksDB path");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    let start = parse_day(&args.start)?;
    let end = parse_day(&args.end)?;
    if start > end {
        bail!("start after end");
    }
    let plan = FuturesFactorPlan::from_factor_names(vec!["cn_features_all".into()])?;
    let factor_names: Vec<String> = plan.factor_names().map(ToOwned::to_owned).collect();
    let filter = args.product.as_ref().map(|text| {
        text.split(',')
            .map(|part| part.trim().to_ascii_uppercase())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
    });
    let jobs = list_jobs(&args.in_root, filter.as_deref())?;
    info!(
        "cn_features_1min_hfq products={} start={} end={} in={} out={}",
        jobs.len(),
        start,
        end,
        args.in_root.display(),
        args.out_root.display()
    );
    let files = AtomicU64::new(0);
    let rows = AtomicU64::new(0);
    let skipped = AtomicU64::new(0);
    jobs.into_par_iter()
        .with_max_len(1)
        .try_for_each(|(exchange, product)| -> Result<()> {
            let stats =
                replay_product(&args, &plan, &factor_names, &exchange, &product, start, end)?;
            files.fetch_add(stats.0, Ordering::Relaxed);
            rows.fetch_add(stats.1, Ordering::Relaxed);
            skipped.fetch_add(stats.2, Ordering::Relaxed);
            Ok(())
        })?;
    eprintln!(
        "cn_features_1min_hfq ok files={} rows={} skipped_no_book={}",
        files.load(Ordering::Relaxed),
        rows.load(Ordering::Relaxed),
        skipped.load(Ordering::Relaxed)
    );
    Ok(())
}

fn list_jobs(root: &Path, filter: Option<&[String]>) -> Result<Vec<(String, String)>> {
    let mut jobs = Vec::new();
    if !root.is_dir() {
        return Ok(jobs);
    }
    for exchange in sorted_dirs(root)? {
        let Some(exchange_name) = exchange.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if exchange_name.starts_with('_') {
            continue;
        }
        for product in sorted_dirs(&exchange)? {
            let Some(product_name) = product.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if let Some(filter) = filter {
                if !filter.iter().any(|item| item == product_name) {
                    continue;
                }
            }
            jobs.push((exchange_name.to_string(), product_name.to_string()));
        }
    }
    jobs.sort();
    Ok(jobs)
}

fn sorted_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn list_days(root: &Path, exchange: &str, product: &str) -> Result<Vec<DayFile>> {
    let dir = root.join(exchange).join(product);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        if stem.len() != 8 {
            continue;
        }
        let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") else {
            continue;
        };
        out.push(DayFile { day, path });
    }
    out.sort_by_key(|item| item.day);
    Ok(out)
}

fn replay_product(
    args: &Args,
    plan: &FuturesFactorPlan,
    factor_names: &[String],
    exchange: &str,
    product: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<(u64, u64, u64)> {
    let days = list_days(&args.in_root, exchange, product)?;
    let mut series: Vec<InputRow> = Vec::new();
    let mut skipped = 0u64;
    for day in &days {
        if day.day < start || day.day > end {
            continue;
        }
        let (rows, skip) = read_day(&day.path, product)?;
        skipped += skip;
        series.extend(rows);
    }
    if series.is_empty() {
        return Ok((0, 0, skipped));
    }
    series.sort_by_key(|row| row.ts);
    let mut state = FuturesFusionState::default();
    let mut by_day: BTreeMap<NaiveDate, Vec<(i64, Vec<Option<f64>>)>> = BTreeMap::new();
    let mut prev_ts: Option<i64> = None;
    for mut row in series {
        if let Some(prev) = prev_ts {
            if is_session_break(prev, row.ts) {
                row.quality_flags |= QUALITY_SEGMENT_BREAK;
            }
        }
        prev_ts = Some(row.ts);
        let ts = row.ts;
        let trading_day = row.trading_day;
        state.push(FuturesFusionInput {
            ts_ms: ts * 1000,
            symbol: product.to_string(),
            trading_day,
            trade: FuturesTradeBar::from_slice(&row.trade)?,
            depth: Some(row.depth),
            quality_flags: row.quality_flags,
            volume_multiple: 1.0,
            volume_multiple_verified: true,
        })?;
        let values = state.factor_values(plan)?;
        let day = trad_day_from_ts(ts);
        if day < start || day > end {
            continue;
        }
        by_day.entry(day).or_default().push((ts, values));
    }
    let mut files = 0u64;
    let mut rows_out = 0u64;
    if !args.dry_run {
        for (day, rows) in by_day {
            let dest = args
                .out_root
                .join(exchange)
                .join(product)
                .join(format!("{}.parquet", day.format("%Y%m%d")));
            if dest.exists() && !args.overwrite {
                continue;
            }
            write_day(&dest, product, factor_names, &rows)?;
            files += 1;
            rows_out += rows.len() as u64;
        }
    }
    eprintln!("{exchange} {product} files={files} rows={rows_out} skipped_no_book={skipped}");
    Ok((files, rows_out, skipped))
}

fn read_day(path: &Path, product: &str) -> Result<(Vec<InputRow>, u64)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let df = ParquetReader::new(file)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", path.display()))?;
    let n = df.height();
    let contract = string_column(&df, "contract_id")?;
    let ts = i64_column(&df, "ts")?;
    let mut trade_cols = Vec::with_capacity(FUTURES_TRADE_FIELD_COUNT);
    for name in FUTURES_TRADE_FIELD_NAMES {
        trade_cols.push(f64_column(&df, name)?);
    }
    let mut bid_p = Vec::new();
    let mut bid_v = Vec::new();
    let mut ask_p = Vec::new();
    let mut ask_v = Vec::new();
    for i in 0..5 {
        bid_p.push(f64_column(&df, &format!("bid{i}p"))?);
        bid_v.push(f64_column(&df, &format!("bid{i}v"))?);
        ask_p.push(f64_column(&df, &format!("ask{i}p"))?);
        ask_v.push(f64_column(&df, &format!("ask{i}v"))?);
    }
    let mut out = Vec::new();
    let mut skipped = 0u64;
    for i in 0..n {
        let id = contract
            .get(i)
            .filter(|s| !s.is_empty())
            .with_context(|| format!("empty contract_id row {i} {}", path.display()))?;
        if id != product {
            continue;
        }
        let ts_sec = ts
            .get(i)
            .with_context(|| format!("null ts row {i} {}", path.display()))?;
        let bid0 = bid_p[0].get(i).unwrap_or(f64::NAN);
        let ask0 = ask_p[0].get(i).unwrap_or(f64::NAN);
        if !(bid0.is_finite() && bid0 > 0.0 && ask0.is_finite() && ask0 >= bid0) {
            skipped += 1;
            continue;
        }
        let mut trade = [f64::NAN; FUTURES_TRADE_FIELD_COUNT];
        for (index, col) in trade_cols.iter().enumerate() {
            trade[index] = col.get(i).unwrap_or(f64::NAN);
        }
        let mut bid_prices = [f64::NAN; 5];
        let mut bid_amounts = [f64::NAN; 5];
        let mut ask_prices = [f64::NAN; 5];
        let mut ask_amounts = [f64::NAN; 5];
        for lvl in 0..5 {
            bid_prices[lvl] = bid_p[lvl].get(i).unwrap_or(f64::NAN);
            bid_amounts[lvl] = bid_v[lvl].get(i).unwrap_or(f64::NAN);
            ask_prices[lvl] = ask_p[lvl].get(i).unwrap_or(f64::NAN);
            ask_amounts[lvl] = ask_v[lvl].get(i).unwrap_or(f64::NAN);
        }
        let depth =
            FuturesDepth5::from_slices(&bid_prices, &bid_amounts, &ask_prices, &ask_amounts)?;
        let trading_day = trad_day_u32(ts_sec);
        out.push(InputRow {
            ts: ts_sec,
            trading_day,
            quality_flags: 0,
            trade,
            depth,
        });
    }
    Ok((out, skipped))
}

fn write_day(
    path: &Path,
    product: &str,
    factor_names: &[String],
    rows: &[(i64, Vec<Option<f64>>)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut contract_id = Vec::with_capacity(rows.len());
    let mut ts = Vec::with_capacity(rows.len());
    let mut cols: Vec<Vec<Option<f64>>> = factor_names
        .iter()
        .map(|_| Vec::with_capacity(rows.len()))
        .collect();
    for (stamp, values) in rows {
        contract_id.push(product.to_string());
        ts.push(*stamp);
        for (index, value) in values.iter().enumerate() {
            cols[index].push(*value);
        }
    }
    let mut series = vec![
        Series::new("contract_id".into(), contract_id),
        Series::new("ts".into(), ts),
    ];
    for (name, values) in factor_names.iter().zip(cols) {
        series.push(Series::new(name.as_str().into(), values));
    }
    let mut dataframe = DataFrame::new(series).context("build factor dataframe")?;
    let temporary = path.with_extension("parquet.tmp");
    let file =
        File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?;
    ParquetWriter::new(file)
        .finish(&mut dataframe)
        .with_context(|| format!("write {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("rename {} -> {}", temporary.display(), path.display()))?;
    Ok(())
}

fn string_column<'a>(dataframe: &'a DataFrame, name: &str) -> Result<&'a StringChunked> {
    dataframe
        .column(name)
        .with_context(|| format!("missing {name}"))?
        .str()
        .with_context(|| format!("{name} must be Utf8"))
}

fn i64_column<'a>(
    dataframe: &'a DataFrame,
    name: &str,
) -> Result<&'a polars::prelude::Int64Chunked> {
    dataframe
        .column(name)
        .with_context(|| format!("missing {name}"))?
        .i64()
        .with_context(|| format!("{name} must be Int64"))
}

fn f64_column<'a>(dataframe: &'a DataFrame, name: &str) -> Result<&'a Float64Chunked> {
    dataframe
        .column(name)
        .with_context(|| format!("missing {name}"))?
        .f64()
        .with_context(|| format!("{name} must be Float64"))
}

fn parse_day(text: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").with_context(|| format!("bad date {text}"))
}

fn shanghai(ts_sec: i64) -> chrono::DateTime<chrono_tz::Tz> {
    Utc.timestamp_opt(ts_sec, 0)
        .single()
        .expect("unix second")
        .with_timezone(&Shanghai)
}

fn on_or_next_weekday(day: NaiveDate) -> NaiveDate {
    match day.weekday() {
        Weekday::Sat => day + Duration::days(2),
        Weekday::Sun => day + Duration::days(1),
        _ => day,
    }
}

fn trad_day_from_ts(ts_sec: i64) -> NaiveDate {
    let local = shanghai(ts_sec);
    let day = local.date_naive();
    let hour = local.hour();
    if hour >= 20 {
        on_or_next_weekday(day + Duration::days(1))
    } else if hour < 8 {
        on_or_next_weekday(day)
    } else {
        day
    }
}

fn trad_day_u32(ts_sec: i64) -> u32 {
    let day = trad_day_from_ts(ts_sec);
    (day.year() as u32) * 10_000 + day.month() * 100 + day.day()
}

fn is_tea_break(prev: i64, next: i64) -> bool {
    if next - prev < TEA_MIN_GAP_SEC {
        return false;
    }
    let a = shanghai(prev);
    let b = shanghai(next);
    a.hour() == 10
        && (10..20).contains(&a.minute())
        && b.hour() == 10
        && (25..36).contains(&b.minute())
}

fn is_session_break(prev: i64, next: i64) -> bool {
    next - prev >= SESSION_BREAK_SEC || is_tea_break(prev, next)
}
