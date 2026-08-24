//! Rust session-aware rewrite for exported CME TAS 1-second backtest parquet.
//!
//! Reads existing sparse or partially densified day parquet under
//! `{exchange}/{product}/{YYYYMMDD}.parquet`. Within each declared CME session,
//! output starts at the first valid causal L1 and then emits every second to
//! the scheduled close. Maintenance and closed intervals remain absent.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, TimeZone};
use chrono_tz::America::Chicago;
use clap::Parser;
use crossbeam_channel::unbounded;
use log::info;
use polars::prelude::{
    DataFrame, Float64Chunked, Int64Chunked, NamedFrom, ParquetReader, ParquetWriter, SerReader,
    Series, StringChunked,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

const DEFAULT_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/backtest_1s";
const DEFAULT_SESSION_CSV: &str =
    "/home/u171/fanghaizhou/cme_globex_daily_trading_intervals_utc_2024_to_2026-08-22_audited_v2.csv";
const DEFAULT_WORKERS: usize = 8;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_export_backtest_1s")]
struct Args {
    #[arg(long, default_value = DEFAULT_ROOT)]
    root: PathBuf,
    #[arg(long, default_value = DEFAULT_SESSION_CSV)]
    session_csv: PathBuf,
    #[arg(long, default_value_t = DEFAULT_WORKERS)]
    workers: usize,
    #[arg(long, default_value = "")]
    products: String,
}

#[derive(Debug)]
struct SessionCalendar {
    by_group: BTreeMap<String, Vec<(i64, i64)>>,
}

#[derive(Debug, Deserialize)]
struct SessionCsvRow {
    schedule_group: String,
    is_trading: String,
    open_utc: Option<String>,
    close_utc: Option<String>,
}

impl SessionCalendar {
    fn load(path: &Path) -> Result<Self> {
        let mut reader = csv::Reader::from_path(path)
            .with_context(|| format!("open CME session CSV {}", path.display()))?;
        let mut by_group: BTreeMap<String, Vec<(i64, i64)>> = BTreeMap::new();
        for record in reader.deserialize() {
            let row: SessionCsvRow =
                record.with_context(|| format!("parse CME session CSV {}", path.display()))?;
            if row.is_trading != "1" {
                continue;
            }
            let open = row
                .open_utc
                .as_deref()
                .ok_or_else(|| anyhow!("trading session row has no open_utc"))?;
            let close = row
                .close_utc
                .as_deref()
                .ok_or_else(|| anyhow!("trading session row has no close_utc"))?;
            let start = DateTime::parse_from_rfc3339(open)
                .with_context(|| format!("parse open_utc {open:?}"))?
                .timestamp();
            let end = DateTime::parse_from_rfc3339(close)
                .with_context(|| format!("parse close_utc {close:?}"))?
                .timestamp();
            if end <= start {
                bail!(
                    "invalid session interval [{start}, {end}) in {}",
                    path.display()
                );
            }
            by_group
                .entry(row.schedule_group)
                .or_default()
                .push((start, end));
        }
        for intervals in by_group.values_mut() {
            intervals.sort_unstable();
        }
        if by_group.is_empty() {
            bail!(
                "CME session CSV {} has no trading intervals",
                path.display()
            );
        }
        Ok(Self { by_group })
    }

    fn intervals_for(&self, product: &str, trading_day: NaiveDate) -> Result<Vec<(i64, i64)>> {
        let group = schedule_group(product)?;
        let end_local = Chicago
            .from_local_datetime(&trading_day.and_hms_opt(17, 0, 0).expect("valid 17:00"))
            .single()
            .ok_or_else(|| anyhow!("ambiguous Chicago 17:00 for {trading_day}"))?;
        let start_local = end_local - ChronoDuration::days(1);
        let window_start = start_local.timestamp();
        let window_end = end_local.timestamp();
        let clipped = self
            .by_group
            .get(group)
            .ok_or_else(|| anyhow!("CME session CSV has no group {group}"))?
            .iter()
            .copied()
            .filter_map(|(start, end)| {
                if start < window_end && end > window_start {
                    Some((start.max(window_start), end.min(window_end)))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let mut merged: Vec<(i64, i64)> = Vec::new();
        for (start, end) in clipped {
            if let Some((_, previous_end)) = merged.last_mut() {
                if start <= *previous_end {
                    *previous_end = (*previous_end).max(end);
                    continue;
                }
            }
            merged.push((start, end));
        }
        Ok(merged)
    }
}

fn schedule_group(product: &str) -> Result<&'static str> {
    let group = match product {
        "C" | "W" | "KW" | "S" | "SM" | "BO" => "grains_oilseeds",
        "FF" | "TU" | "FV" | "TY" | "TN" | "US" | "U" | "S1R" | "SRA" => "interest_rates",
        "YM" | "ES" | "NQ" | "RTY" | "MEM" => "equity_indices",
        "AD" | "BP" | "BR" | "CD" | "JY" | "KRW" | "MP" | "NE" | "NOKA" | "PLZ" | "SEK" | "SF"
        | "URO" => "fx",
        "BTC" | "ETH" => "cryptocurrency",
        "FC" | "LC" | "LH" => "livestock",
        "CL" | "WTCL" | "HO" | "RB" | "NG" | "JKM" => "energy",
        "GC" | "SI" | "HG" | "ALI" | "HRC" | "PL" | "PA" => "metals",
        other => return Err(anyhow!("no CME session group for product {other}")),
    };
    Ok(group)
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Identity {
    contract_id: String,
    ric: String,
}

#[derive(Clone, Debug)]
struct SparseRow {
    ts: i64,
    bid0p: Option<f64>,
    bid0v: Option<f64>,
    ask0p: Option<f64>,
    ask0v: Option<f64>,
    buy_high: Option<f64>,
    sell_low: Option<f64>,
    close: Option<f64>,
    midp: Option<f64>,
}

impl SparseRow {
    fn valid_book(&self) -> bool {
        matches!(
            (self.bid0p, self.bid0v, self.ask0p, self.ask0v),
            (Some(bid), Some(bidv), Some(ask), Some(askv))
                if bid.is_finite()
                    && ask.is_finite()
                    && bidv.is_finite()
                    && askv.is_finite()
                    && bid > 0.0
                    && ask >= bid
                    && bidv >= 0.0
                    && askv >= 0.0
        )
    }

    fn has_event(&self) -> bool {
        self.buy_high.is_some_and(f64::is_finite) || self.sell_low.is_some_and(f64::is_finite)
    }
}

#[derive(Default)]
struct CarryState {
    bid0p: Option<f64>,
    bid0v: Option<f64>,
    ask0p: Option<f64>,
    ask0v: Option<f64>,
    close: Option<f64>,
    midp: Option<f64>,
}

impl CarryState {
    fn update(&mut self, row: &SparseRow) {
        if row.valid_book() {
            self.bid0p = row.bid0p;
            self.bid0v = row.bid0v;
            self.ask0p = row.ask0p;
            self.ask0v = row.ask0v;
        }
        if row.close.is_some_and(|value| value.is_finite()) {
            self.close = row.close;
        }
        if row.midp.is_some_and(|value| value.is_finite()) {
            self.midp = row.midp;
        }
    }

    fn row(&self, ts: i64) -> SparseRow {
        SparseRow {
            ts,
            bid0p: self.bid0p,
            bid0v: self.bid0v,
            ask0p: self.ask0p,
            ask0v: self.ask0v,
            buy_high: None,
            sell_low: None,
            close: self.close,
            midp: self.midp,
        }
    }
}

#[derive(Clone, Debug)]
struct DenseRow {
    identity: usize,
    ts: i64,
    bid0p: Option<f64>,
    bid0v: Option<f64>,
    ask0p: Option<f64>,
    ask0v: Option<f64>,
    buy_high: Option<f64>,
    sell_low: Option<f64>,
    close: Option<f64>,
    midp: Option<f64>,
}

impl DenseRow {
    fn from_sparse(identity: usize, row: SparseRow) -> Self {
        Self {
            identity,
            ts: row.ts,
            bid0p: row.bid0p,
            bid0v: row.bid0v,
            ask0p: row.ask0p,
            ask0v: row.ask0v,
            buy_high: row.buy_high,
            sell_low: row.sell_low,
            close: row.close,
            midp: row.midp,
        }
    }
}

fn densify_ric(
    rows: Vec<SparseRow>,
    identity: usize,
    intervals: &[(i64, i64)],
) -> Result<Vec<DenseRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut by_ts = BTreeMap::new();
    for row in rows {
        by_ts.insert(row.ts, row);
    }
    let source: Vec<SparseRow> = by_ts.into_values().collect();
    let mut output = Vec::new();
    for &(start, end) in intervals {
        if end <= start {
            bail!("invalid session interval [{start}, {end})");
        }
        let Some(first) = source
            .iter()
            .find(|row| row.ts >= start && row.ts < end && row.valid_book())
        else {
            continue;
        };
        let mut state = CarryState::default();
        let mut source_pos = source.partition_point(|row| row.ts < first.ts);
        let mut ts = first.ts;
        while ts < end {
            let current = if source_pos < source.len() && source[source_pos].ts == ts {
                let row = source[source_pos].clone();
                source_pos += 1;
                state.update(&row);
                Some(row)
            } else {
                None
            };
            let mut row = state.row(ts);
            if let Some(source_row) = current {
                row.buy_high = source_row.buy_high.filter(|value| value.is_finite());
                row.sell_low = source_row.sell_low.filter(|value| value.is_finite());
            }
            output.push(DenseRow::from_sparse(identity, row));
            ts += 1;
        }
        if let Some(endpoint) = source.iter().find(|row| row.ts == end && row.has_event()) {
            let mut endpoint = endpoint.clone();
            state.update(&endpoint);
            endpoint.bid0p = state.bid0p;
            endpoint.bid0v = state.bid0v;
            endpoint.ask0p = state.ask0p;
            endpoint.ask0v = state.ask0v;
            endpoint.close = state.close;
            endpoint.midp = state.midp;
            output.push(DenseRow::from_sparse(identity, endpoint));
        }
    }
    Ok(output)
}

fn already_dense(rows: &[SparseRow], intervals: &[(i64, i64)]) -> bool {
    let mut by_ts = BTreeMap::new();
    for row in rows {
        if by_ts.insert(row.ts, row).is_some() {
            return false;
        }
    }
    if by_ts.iter().any(|(ts, row)| {
        !intervals
            .iter()
            .any(|(start, end)| (*ts >= *start && *ts < *end) || (*ts == *end && row.has_event()))
    }) {
        return false;
    }
    for &(start, end) in intervals {
        let segment = by_ts
            .range(start..end)
            .map(|(_, row)| *row)
            .collect::<Vec<_>>();
        let Some(first_index) = segment.iter().position(|row| row.valid_book()) else {
            if !segment.is_empty() {
                return false;
            }
            continue;
        };
        let first_ts = segment[first_index].ts;
        if first_ts >= end || segment.len() != (end - first_ts) as usize {
            return false;
        }
        for (offset, row) in segment.iter().enumerate() {
            if row.ts != first_ts + offset as i64 || !row.valid_book() {
                return false;
            }
        }
    }
    true
}

fn str_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a StringChunked> {
    df.column(name)
        .with_context(|| format!("missing {name}"))?
        .str()
        .with_context(|| format!("{name} is not String"))
}

fn i64_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Int64Chunked> {
    df.column(name)
        .with_context(|| format!("missing {name}"))?
        .i64()
        .with_context(|| format!("{name} is not Int64"))
}

fn f64_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Float64Chunked> {
    df.column(name)
        .with_context(|| format!("missing {name}"))?
        .f64()
        .with_context(|| format!("{name} is not Float64"))
}

fn required_str<'a>(col: &'a StringChunked, index: usize, name: &str) -> Result<&'a str> {
    col.get(index)
        .ok_or_else(|| anyhow!("{name} is null at row {index}"))
}

fn required_i64(col: &Int64Chunked, index: usize, name: &str) -> Result<i64> {
    col.get(index)
        .ok_or_else(|| anyhow!("{name} is null at row {index}"))
}

fn read_rows(path: &Path) -> Result<BTreeMap<Identity, Vec<SparseRow>>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let df = ParquetReader::new(file)
        .finish()
        .with_context(|| format!("read {}", path.display()))?;
    let contract_id = str_col(&df, "contract_id")?;
    let ric = str_col(&df, "ric")?;
    let ts = i64_col(&df, "ts")?;
    let bid0p = f64_col(&df, "bid0p")?;
    let bid0v = f64_col(&df, "bid0v")?;
    let ask0p = f64_col(&df, "ask0p")?;
    let ask0v = f64_col(&df, "ask0v")?;
    let buy_high = f64_col(&df, "buy_high")?;
    let sell_low = f64_col(&df, "sell_low")?;
    let close = f64_col(&df, "close")?;
    let midp = f64_col(&df, "midp")?;
    let mut rows: BTreeMap<Identity, Vec<SparseRow>> = BTreeMap::new();
    for index in 0..df.height() {
        let identity = Identity {
            contract_id: required_str(contract_id, index, "contract_id")?.to_string(),
            ric: required_str(ric, index, "ric")?.to_string(),
        };
        rows.entry(identity).or_default().push(SparseRow {
            ts: required_i64(ts, index, "ts")?,
            bid0p: bid0p.get(index),
            bid0v: bid0v.get(index),
            ask0p: ask0p.get(index),
            ask0v: ask0v.get(index),
            buy_high: buy_high.get(index),
            sell_low: sell_low.get(index),
            close: close.get(index),
            midp: midp.get(index),
        });
    }
    Ok(rows)
}

fn rows_to_dataframe(rows: &[DenseRow], identities: &[Identity]) -> Result<DataFrame> {
    let mut contract_id: Vec<&str> = Vec::with_capacity(rows.len());
    let mut ric: Vec<&str> = Vec::with_capacity(rows.len());
    let mut ts = Vec::with_capacity(rows.len());
    let mut bid0p = Vec::with_capacity(rows.len());
    let mut bid0v = Vec::with_capacity(rows.len());
    let mut ask0p = Vec::with_capacity(rows.len());
    let mut ask0v = Vec::with_capacity(rows.len());
    let mut buy_high = Vec::with_capacity(rows.len());
    let mut sell_low = Vec::with_capacity(rows.len());
    let mut close = Vec::with_capacity(rows.len());
    let mut midp = Vec::with_capacity(rows.len());
    for row in rows {
        let identity = identities
            .get(row.identity)
            .ok_or_else(|| anyhow!("dense identity {} is out of range", row.identity))?;
        contract_id.push(&identity.contract_id);
        ric.push(&identity.ric);
        ts.push(row.ts);
        bid0p.push(row.bid0p);
        bid0v.push(row.bid0v);
        ask0p.push(row.ask0p);
        ask0v.push(row.ask0v);
        buy_high.push(row.buy_high);
        sell_low.push(row.sell_low);
        close.push(row.close);
        midp.push(row.midp);
    }
    DataFrame::new(vec![
        Series::new("contract_id".into(), contract_id),
        Series::new("ric".into(), ric),
        Series::new("ts".into(), ts),
        Series::new("bid0p".into(), bid0p),
        Series::new("bid0v".into(), bid0v),
        Series::new("ask0p".into(), ask0p),
        Series::new("ask0v".into(), ask0v),
        Series::new("buy_high".into(), buy_high),
        Series::new("sell_low".into(), sell_low),
        Series::new("close".into(), close),
        Series::new("midp".into(), midp),
    ])
    .context("build backtest dataframe")
}

#[derive(Clone)]
struct Job {
    path: PathBuf,
    product: String,
    trading_day: NaiveDate,
}

#[derive(Default)]
struct Stats {
    files: u64,
    sparse_rows: u64,
    dense_rows: u64,
}

fn rewrite_day(path: &Path, intervals: &[(i64, i64)]) -> Result<(u64, u64)> {
    let sparse = read_rows(path)?;
    let sparse_rows = sparse.values().map(Vec::len).sum::<usize>() as u64;
    if sparse.values().all(|rows| already_dense(rows, intervals)) {
        return Ok((sparse_rows, sparse_rows));
    }
    let identities = sparse.keys().cloned().collect::<Vec<_>>();
    let mut dense = Vec::new();
    for (identity, (_key, rows)) in sparse.into_iter().enumerate() {
        dense.extend(densify_ric(rows, identity, intervals)?);
    }
    dense.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then(left.identity.cmp(&right.identity))
    });
    let mut df = rows_to_dataframe(&dense, &identities)?;
    let tmp = path.with_extension("parquet.tmp");
    let file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    ParquetWriter::new(file)
        .finish(&mut df)
        .with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("rename {}", path.display()))?;
    Ok((sparse_rows, dense.len() as u64))
}

fn list_jobs(root: &Path, products: &[String]) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for exchange in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let exchange = exchange?;
        if !exchange.file_type()?.is_dir() {
            continue;
        }
        for product in fs::read_dir(exchange.path())? {
            let product = product?;
            if !product.file_type()?.is_dir() {
                continue;
            }
            let product_name = product.file_name().to_string_lossy().to_string();
            if !products.is_empty() && !products.iter().any(|item| item == &product_name) {
                continue;
            }
            for file in fs::read_dir(product.path())? {
                let file = file?;
                if !file.file_type()?.is_file()
                    || file.path().extension().is_none_or(|ext| ext != "parquet")
                {
                    continue;
                }
                let path = file.path();
                let stem = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| anyhow!("invalid parquet filename {}", path.display()))?;
                let trading_day = NaiveDate::parse_from_str(stem, "%Y%m%d")
                    .with_context(|| format!("parse trading day in {}", path.display()))?;
                jobs.push(Job {
                    path,
                    product: product_name.clone(),
                    trading_day,
                });
            }
        }
    }
    jobs.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(jobs)
}

fn run(args: Args) -> Result<()> {
    if args.workers == 0 {
        bail!("workers must be >= 1");
    }
    let products = args
        .products
        .split(',')
        .filter_map(|item| {
            let item = item.trim();
            (!item.is_empty()).then_some(item.to_string())
        })
        .collect::<Vec<_>>();
    let calendar = Arc::new(SessionCalendar::load(&args.session_csv)?);
    let jobs = list_jobs(&args.root, &products)?;
    if jobs.is_empty() {
        bail!("no parquet files under {}", args.root.display());
    }
    let workers = args.workers.min(jobs.len()).max(1);
    info!(
        "cme_tas_export_backtest_1s root={} files={} workers={} products={}",
        args.root.display(),
        jobs.len(),
        workers,
        if products.is_empty() {
            "all"
        } else {
            "selected"
        }
    );
    let (tx, rx) = unbounded::<Job>();
    for job in jobs {
        tx.send(job).context("enqueue backtest parquet job")?;
    }
    drop(tx);
    let completed = Arc::new(AtomicU64::new(0));
    let mut joins = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let rx = rx.clone();
        let calendar = Arc::clone(&calendar);
        let completed = Arc::clone(&completed);
        joins.push(
            thread::Builder::new()
                .name(format!("cme-tas-backtest-export-{worker_id}"))
                .spawn(move || -> Result<Stats> {
                    let mut stats = Stats::default();
                    while let Ok(job) = rx.recv() {
                        let intervals = calendar.intervals_for(&job.product, job.trading_day)?;
                        let (sparse_rows, dense_rows) = rewrite_day(&job.path, &intervals)?;
                        stats.files += 1;
                        stats.sparse_rows += sparse_rows;
                        stats.dense_rows += dense_rows;
                        let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                        if done % 100 == 0 {
                            info!(
                                "backtest parquet files_done={} sparse_rows={} dense_rows={}",
                                done, stats.sparse_rows, stats.dense_rows
                            );
                        }
                    }
                    Ok(stats)
                })
                .context("spawn backtest parquet worker")?,
        );
    }
    let mut total = Stats::default();
    for join in joins {
        let stats = join
            .join()
            .map_err(|_| anyhow!("backtest parquet worker panicked"))??;
        total.files += stats.files;
        total.sparse_rows += stats.sparse_rows;
        total.dense_rows += stats.dense_rows;
    }
    info!(
        "cme_tas_export_backtest_1s complete files={} sparse_rows={} dense_rows={}",
        total.files, total.sparse_rows, total.dense_rows
    );
    Ok(())
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .init();
    if let Err(err) = run(Args::parse()) {
        eprintln!("{err:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ts: i64, bid: Option<f64>, ask: Option<f64>, event: Option<f64>) -> SparseRow {
        SparseRow {
            ts,
            bid0p: bid,
            bid0v: bid.map(|_| 10.0),
            ask0p: ask,
            ask0v: ask.map(|_| 9.0),
            buy_high: event,
            sell_low: None,
            close: Some(100.0),
            midp: Some(100.5),
        }
    }

    #[test]
    fn starts_at_valid_book_and_runs_to_session_close() {
        let output = densify_ric(
            vec![
                row(100, None, None, Some(99.0)),
                row(102, Some(100.0), Some(101.0), None),
            ],
            0,
            &[(100, 105)],
        )
        .unwrap();
        assert_eq!(
            output.iter().map(|row| row.ts).collect::<Vec<_>>(),
            vec![102, 103, 104]
        );
        assert!(output.iter().all(|row| row.bid0p == Some(100.0)));
        assert!(output.iter().skip(1).all(|row| row.buy_high.is_none()));
    }

    #[test]
    fn retains_real_event_exactly_at_session_close() {
        let output = densify_ric(
            vec![
                row(100, Some(100.0), Some(101.0), None),
                row(105, Some(101.0), Some(102.0), Some(102.0)),
            ],
            0,
            &[(100, 105)],
        )
        .unwrap();
        assert_eq!(
            output.iter().map(|row| row.ts).collect::<Vec<_>>(),
            vec![100, 101, 102, 103, 104, 105]
        );
        assert_eq!(output.last().and_then(|row| row.buy_high), Some(102.0));
    }

    #[test]
    fn complete_grid_is_skipped_but_a_missing_second_is_not() {
        let complete = vec![
            row(100, Some(100.0), Some(101.0), None),
            row(101, Some(100.0), Some(101.0), None),
            row(102, Some(100.0), Some(101.0), None),
        ];
        assert!(already_dense(&complete, &[(100, 103)]));
        let incomplete = vec![complete[0].clone(), complete[2].clone()];
        assert!(!already_dense(&incomplete, &[(100, 103)]));
        let duplicate = vec![
            complete[0].clone(),
            complete[1].clone(),
            complete[1].clone(),
            complete[2].clone(),
        ];
        assert!(!already_dense(&duplicate, &[(100, 103)]));
    }
}
