//! 1-minute drop_special trades from TAS RocksDB `cme_trade` + `cme_special`.
//!
//! Python correctness baseline: `lseg/cme_tas_drop_special_1min.py`.
//! Special only fills `special_count` / `special_volume`. ylabel is not written.
//! Do not write into `baseline_data_1m`. One shared secondary (`Arc<DB>`); do
//! not take the primary write lock.
//!
//! Pipeline: dispatch RIC jobs → parallel compute (scan + 1min synth) →
//! reduce by product root → write `{exchange}/{product}/{YYYYMMDD}.parquet`.
//! Compute unit is the expiry RIC. Write unit is the product day file.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, TimeZone};
use chrono_tz::America::Chicago;
use clap::Parser;
use cme_tas_replay::drop_special_1min::{
    fill_full_session_minutes, route_for_row, synthesize_drop_special_1min,
    write_drop_special_parquet, DropSpecialMinute, MinuteFillState,
};
use cme_tas_replay::{
    decode_cme_special, decode_cme_trade, decode_ric, encode_key, encode_ric, key_ts_utc_ns,
    parse_date_time_ns, research_root_exchange, research_root_of, SlimTrade, CF_CME_SPECIAL,
    CF_CME_TRADE, KEY_LEN, RIC_LEN,
};
use crossbeam_channel::unbounded;
use log::{error, info};
use rocksdb::{Direction, IteratorMode, Options, ReadOptions, DB};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

struct FlushWriter<W: Write>(W);

impl<W: Write> Write for FlushWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.0.write(buf)?;
        self.0.flush()?;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

const DEFAULT_READAHEAD_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_WORKERS: usize = 32;
const DEFAULT_WRITE_WORKERS: usize = 4;

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

    fn intervals_for(&self, product: &str, trading_day: u32) -> Result<Vec<(i64, i64)>> {
        let group = schedule_group(product)?;
        let date = trading_day_date(trading_day)?;
        let end_local = Chicago
            .from_local_datetime(&date.and_hms_opt(17, 0, 0).expect("valid 17:00"))
            .single()
            .ok_or_else(|| anyhow!("ambiguous Chicago 17:00 for {trading_day}"))?;
        let start_local = end_local - ChronoDuration::days(1);
        let window_start = start_local.timestamp();
        let window_end = end_local.timestamp();
        let intervals = self
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
        Ok(intervals)
    }
}

fn trading_day_date(trading_day: u32) -> Result<NaiveDate> {
    let year = trading_day / 10_000;
    let month = (trading_day / 100) % 100;
    let day = trading_day % 100;
    NaiveDate::from_ymd_opt(year as i32, month, day)
        .ok_or_else(|| anyhow!("invalid trading day {trading_day}"))
}

fn trading_day_key(date: NaiveDate) -> u32 {
    date.year() as u32 * 10_000 + date.month() * 100 + date.day()
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

#[derive(Parser, Debug)]
#[command(name = "cme_tas_drop_special_1min")]
struct Args {
    #[arg(long, default_value = "config/cme_tas_drop_special_1min.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DropSpecialConfig {
    rocksdb_dir: PathBuf,
    #[serde(default = "default_secondary_dir")]
    secondary_dir: PathBuf,
    #[serde(default = "default_out_root")]
    out_root: PathBuf,
    #[serde(default = "default_session_csv")]
    session_csv: PathBuf,
    #[serde(default)]
    rics: Vec<String>,
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
    #[serde(default = "default_readahead_bytes")]
    readahead_bytes: usize,
    #[serde(default = "default_workers")]
    workers: usize,
    #[serde(default = "default_write_workers")]
    write_workers: usize,
    #[serde(default)]
    overwrite: bool,
}

fn default_secondary_dir() -> PathBuf {
    PathBuf::from(
        "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb.drop_special.secondary",
    )
}

fn default_out_root() -> PathBuf {
    PathBuf::from("/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1m_drop_special")
}

fn default_session_csv() -> PathBuf {
    PathBuf::from(
        "/home/u171/fanghaizhou/cme_globex_daily_trading_intervals_utc_2024_to_2026-08-22_audited_v2.csv",
    )
}

fn default_readahead_bytes() -> usize {
    DEFAULT_READAHEAD_BYTES
}

fn default_workers() -> usize {
    DEFAULT_WORKERS
}

fn default_write_workers() -> usize {
    DEFAULT_WRITE_WORKERS
}

fn open_rocksdb_secondary(primary: &Path, secondary: &Path) -> Result<DB> {
    if !primary.exists() {
        bail!("rocksdb {} does not exist", primary.display());
    }
    if let Some(parent) = secondary.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create secondary parent {}", parent.display()))?;
    }
    let mut db_opts = Options::default();
    db_opts.create_if_missing(false);
    db_opts.create_missing_column_families(false);
    db_opts.set_max_open_files(-1);
    let names = DB::list_cf(&db_opts, primary)
        .with_context(|| format!("list column families {}", primary.display()))?;
    if !names.iter().any(|name| name == CF_CME_TRADE) {
        bail!(
            "rocksdb {} has no {CF_CME_TRADE} column family",
            primary.display()
        );
    }
    let db = DB::open_cf_as_secondary(&db_opts, primary, secondary, &names).with_context(|| {
        format!(
            "open rocksdb secondary {} from {}",
            secondary.display(),
            primary.display()
        )
    })?;
    db.try_catch_up_with_primary()
        .context("catch up rocksdb secondary")?;
    Ok(db)
}

fn trade_read_opts(readahead_bytes: usize) -> ReadOptions {
    let mut opts = ReadOptions::default();
    opts.set_readahead_size(readahead_bytes);
    opts.fill_cache(false);
    opts
}

fn parse_bound(raw: Option<&str>) -> Result<Option<u64>> {
    match raw {
        None => Ok(None),
        Some(value) => Ok(Some(parse_date_time_ns(value)?)),
    }
}

fn next_item(iter: &mut rocksdb::DBIterator<'_>) -> Result<Option<(Box<[u8]>, Box<[u8]>)>> {
    match iter.next() {
        None => Ok(None),
        Some(item) => Ok(Some(item.context("rocksdb iterator")?)),
    }
}

fn next_ric_seek_key(ric: &str) -> Result<Option<[u8; KEY_LEN]>> {
    let mut prefix = encode_ric(ric)?;
    for i in (0..RIC_LEN).rev() {
        if prefix[i] != 0xFF {
            prefix[i] = prefix[i].saturating_add(1);
            for byte in prefix.iter_mut().skip(i + 1) {
                *byte = 0;
            }
            let mut key = [0u8; KEY_LEN];
            key[..RIC_LEN].copy_from_slice(&prefix);
            return Ok(Some(key));
        }
    }
    Ok(None)
}

fn collect_rics_from_cf(
    db: &DB,
    cf_name: &str,
    readahead_bytes: usize,
) -> Result<BTreeSet<String>> {
    let Some(cf) = db.cf_handle(cf_name) else {
        return Ok(BTreeSet::new());
    };
    let mut out = BTreeSet::new();
    let mut start = [0u8; KEY_LEN];
    loop {
        let mut iter = db.iterator_cf_opt(
            cf,
            trade_read_opts(readahead_bytes),
            IteratorMode::From(&start, Direction::Forward),
        );
        let Some((key, _)) = next_item(&mut iter)? else {
            break;
        };
        if key.len() != KEY_LEN {
            bail!("{cf_name} key length {} is not {KEY_LEN}", key.len());
        }
        let ric = decode_ric(&key[..RIC_LEN])?;
        out.insert(ric.clone());
        match next_ric_seek_key(&ric)? {
            Some(next) => start = next,
            None => break,
        }
    }
    Ok(out)
}

fn collect_rics(db: &DB, readahead_bytes: usize, filter: &[String]) -> Result<Vec<String>> {
    let mut rics = collect_rics_from_cf(db, CF_CME_TRADE, readahead_bytes)?;
    rics.extend(collect_rics_from_cf(db, CF_CME_SPECIAL, readahead_bytes)?);
    if !filter.is_empty() {
        let allow: BTreeSet<&str> = filter.iter().map(String::as_str).collect();
        rics.retain(|ric| allow.contains(ric.as_str()));
    }
    Ok(rics.into_iter().collect())
}

#[derive(Clone)]
struct RicJob {
    ric: String,
    exchange: String,
    product: String,
}

struct ProductKey {
    exchange: String,
    product: String,
}

struct RicResult {
    job: RicJob,
    rows: Vec<DropSpecialMinute>,
}

struct ProductBatch {
    key: ProductKey,
    rows: Vec<DropSpecialMinute>,
}

fn dispatch_ric_jobs(rics: &[String]) -> Result<(Vec<RicJob>, BTreeMap<(String, String), u64>)> {
    let mut jobs = Vec::with_capacity(rics.len());
    let mut remaining: BTreeMap<(String, String), u64> = BTreeMap::new();
    for ric in rics {
        let Some(root) = research_root_of(ric)? else {
            bail!("RIC {ric} is not a research-root expiry");
        };
        let exchange = research_root_exchange(root)
            .ok_or_else(|| anyhow!("research root {root} has no exchange"))?;
        let key = (exchange.to_string(), root.to_string());
        remaining
            .entry(key)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        jobs.push(RicJob {
            ric: ric.clone(),
            exchange: exchange.to_string(),
            product: root.to_string(),
        });
    }
    Ok((jobs, remaining))
}

fn scan_cf(
    db: &DB,
    cf_name: &str,
    ric: &str,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
    readahead_bytes: usize,
    decode: fn(&[u8]) -> Result<SlimTrade>,
) -> Result<Vec<SlimTrade>> {
    let Some(cf) = db.cf_handle(cf_name) else {
        if cf_name == CF_CME_SPECIAL {
            return Ok(Vec::new());
        }
        bail!("column family {cf_name} missing");
    };
    let start_key = encode_key(ric, start_ns.unwrap_or(0), 0, 0)?;
    let iter = db.iterator_cf_opt(
        cf,
        trade_read_opts(readahead_bytes),
        IteratorMode::From(&start_key, Direction::Forward),
    );
    let mut rows = Vec::new();
    for item in iter {
        let (key, value) = item.with_context(|| format!("scan {cf_name}"))?;
        if key.len() != KEY_LEN {
            bail!("{cf_name} key length {} is not {KEY_LEN}", key.len());
        }
        let key_ric = decode_ric(&key[..RIC_LEN])?;
        if key_ric != ric {
            break;
        }
        let ts = key_ts_utc_ns(&key)?;
        if end_ns.is_some_and(|end| ts >= end) {
            break;
        }
        if start_ns.is_some_and(|start| ts < start) {
            continue;
        }
        let rec = decode(&value)?;
        if rec.ric != ric {
            bail!("{cf_name} value ric {} does not match key {ric}", rec.ric);
        }
        if rec.ts_utc_ns != ts {
            bail!(
                "{cf_name} value ts {} does not match key {ts} for {ric}",
                rec.ts_utc_ns
            );
        }
        rows.push(rec);
    }
    Ok(rows)
}

fn scan_one_ric(
    db: &DB,
    ric: &str,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
    readahead_bytes: usize,
    session_calendar: &SessionCalendar,
) -> Result<Vec<DropSpecialMinute>> {
    let trades = scan_cf(
        db,
        CF_CME_TRADE,
        ric,
        start_ns,
        end_ns,
        readahead_bytes,
        decode_cme_trade,
    )?;
    let specials = scan_cf(
        db,
        CF_CME_SPECIAL,
        ric,
        start_ns,
        end_ns,
        readahead_bytes,
        decode_cme_special,
    )?;
    let raw = synthesize_drop_special_1min(&trades, &specials, None)?;
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let (exchange, product, _) = route_for_row(&raw[0])?;
    let contract_id = raw[0].contract_id.clone();
    let first_ric = raw[0].ric.clone();
    if first_ric != ric {
        bail!("scan for {ric} synthesized row for {first_ric}");
    }
    let mut by_day: BTreeMap<u32, Vec<DropSpecialMinute>> = BTreeMap::new();
    for row in raw {
        let (row_exchange, row_product, day) = route_for_row(&row)?;
        if row_exchange != exchange || row_product != product || row.contract_id != contract_id {
            bail!("RIC {ric} changed route or contract_id while scanning");
        }
        by_day.entry(day).or_default().push(row);
    }
    let first_day = *by_day.first_key_value().expect("raw rows are nonempty").0;
    let last_day = *by_day.last_key_value().expect("raw rows are nonempty").0;
    let mut current_date = trading_day_date(first_day)?;
    let last_date = trading_day_date(last_day)?;
    let mut filled = Vec::new();
    let mut state = MinuteFillState::default();
    loop {
        let day = trading_day_key(current_date);
        let intervals = session_calendar.intervals_for(&product, day)?;
        let day_rows = by_day.remove(&day).unwrap_or_default();
        let day_filled =
            fill_full_session_minutes(day_rows, &contract_id, ric, &intervals, &mut state)?;
        filled.extend(day_filled);
        if current_date == last_date {
            break;
        }
        current_date = current_date
            .succ_opt()
            .ok_or_else(|| anyhow!("trading-day overflow after {day}"))?;
    }
    filled.sort_by(|left, right| left.ts.cmp(&right.ts));
    Ok(filled)
}

fn is_day_in_overwrite_window(
    trading_day: u32,
    start_day: Option<u32>,
    end_day: Option<u32>,
) -> bool {
    start_day.is_none_or(|start| trading_day >= start)
        && end_day.is_none_or(|end| trading_day <= end)
}

fn unlink_overwrite_files(
    out_root: &Path,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
) -> Result<u64> {
    let mut removed = 0u64;
    if !out_root.exists() {
        return Ok(0);
    }
    let start_day = start_ns
        .map(cme_tas_replay::tradeday_yyyymmdd)
        .transpose()?;
    let end_day = end_ns.map(cme_tas_replay::tradeday_yyyymmdd).transpose()?;
    for exchange in fs::read_dir(out_root)? {
        let exchange = exchange?;
        if !exchange.file_type()?.is_dir() {
            continue;
        }
        for product in fs::read_dir(exchange.path())? {
            let product = product?;
            if !product.file_type()?.is_dir() {
                continue;
            }
            for file in fs::read_dir(product.path())? {
                let file = file?;
                if !file.file_type()?.is_file() {
                    continue;
                }
                let path = file.path();
                if path.extension().is_none_or(|ext| ext != "parquet") {
                    continue;
                }
                let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                    continue;
                };
                let Ok(trading_day) = stem.parse::<u32>() else {
                    continue;
                };
                if trading_day_date(trading_day).is_err() {
                    continue;
                }
                if is_day_in_overwrite_window(trading_day, start_day, end_day) {
                    fs::remove_file(file.path())?;
                    removed += 1;
                }
            }
        }
    }
    Ok(removed)
}

fn write_days(
    out_root: &Path,
    key: &ProductKey,
    rows: Vec<DropSpecialMinute>,
    overwrite: bool,
) -> Result<(u64, u64)> {
    let mut by_file: BTreeMap<u32, Vec<DropSpecialMinute>> = BTreeMap::new();
    for row in rows {
        let (exchange, product, day) = route_for_row(&row)?;
        if exchange != key.exchange || product != key.product {
            bail!(
                "RIC {} routed to {}/{} but reducer owns {}/{}",
                row.ric,
                exchange,
                product,
                key.exchange,
                key.product
            );
        }
        by_file.entry(day).or_default().push(row);
    }
    let mut files = 0u64;
    let mut written = 0u64;
    for (day, mut chunk) in by_file {
        chunk.sort_by(|a, b| a.ts.cmp(&b.ts).then(a.ric.cmp(&b.ric)));
        let dest = out_root
            .join(&key.exchange)
            .join(&key.product)
            .join(format!("{day}.parquet"));
        if dest.exists() && !overwrite {
            continue;
        }
        write_drop_special_parquet(&dest, &chunk)?;
        files += 1;
        written += chunk.len() as u64;
    }
    Ok((files, written))
}

struct ComputeStats {
    rics: u64,
    rows: u64,
}

struct WriteStats {
    products: u64,
    files: u64,
    rows: u64,
}

fn run(config: &DropSpecialConfig) -> Result<()> {
    if config.out_root.as_os_str() == "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1m" {
        bail!("refusing to write into baseline_data_1m");
    }
    if config.workers == 0 {
        bail!("workers must be >= 1");
    }
    if config.write_workers == 0 {
        bail!("write_workers must be >= 1");
    }
    let start_ns = parse_bound(config.start.as_deref())?;
    let end_ns = parse_bound(config.end.as_deref())?;
    let session_calendar = Arc::new(SessionCalendar::load(&config.session_csv)?);
    info!(
        "using CME session calendar {}",
        config.session_csv.display()
    );
    info!(
        "opening rocksdb secondary {} from {}",
        config.secondary_dir.display(),
        config.rocksdb_dir.display()
    );
    let db = Arc::new(open_rocksdb_secondary(
        &config.rocksdb_dir,
        &config.secondary_dir,
    )?);
    info!("listing RICs from {CF_CME_TRADE} / {CF_CME_SPECIAL}");
    let rics = collect_rics(&db, config.readahead_bytes, &config.rics)?;
    let (jobs, remaining_counts) = dispatch_ric_jobs(&rics)?;
    let product_count = remaining_counts.len();
    let workers = config.workers.min(jobs.len().max(1)).max(1);
    let write_workers = config.write_workers.min(product_count.max(1)).max(1);
    info!(
        "cme_tas_drop_special_1min rocksdb={} out={} rics={} products={} compute_workers={} write_workers={}",
        config.rocksdb_dir.display(),
        config.out_root.display(),
        jobs.len(),
        product_count,
        workers,
        write_workers
    );
    if config.overwrite {
        let removed = unlink_overwrite_files(&config.out_root, start_ns, end_ns)?;
        info!(
            "overwrite removed {removed} parquet files under {} for trade-day range {:?}..{:?}",
            config.out_root.display(),
            start_ns
                .map(cme_tas_replay::tradeday_yyyymmdd)
                .transpose()?,
            end_ns.map(cme_tas_replay::tradeday_yyyymmdd).transpose()?
        );
    }
    fs::create_dir_all(&config.out_root)?;

    let (job_tx, job_rx) = unbounded::<RicJob>();
    for job in jobs {
        job_tx.send(job).context("enqueue drop_special RIC")?;
    }
    drop(job_tx);

    let abort = Arc::new(AtomicBool::new(false));
    let rics_done = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let (result_tx, result_rx) = unbounded::<RicResult>();
    let (write_tx, write_rx) = unbounded::<ProductBatch>();

    let mut write_joins = Vec::with_capacity(write_workers);
    for worker_id in 0..write_workers {
        let write_rx = write_rx.clone();
        let out_root = config.out_root.clone();
        let overwrite = config.overwrite;
        let handle = thread::Builder::new()
            .name(format!("cme-tas-ds-write-{worker_id}"))
            .spawn(move || -> Result<WriteStats> {
                let mut stats = WriteStats {
                    products: 0,
                    files: 0,
                    rows: 0,
                };
                while let Ok(batch) = write_rx.recv() {
                    let exchange = batch.key.exchange.clone();
                    let product = batch.key.product.clone();
                    let (files, rows) =
                        write_days(&out_root, &batch.key, batch.rows, overwrite)?;
                    stats.products += 1;
                    stats.files += files;
                    stats.rows += rows;
                    info!(
                        "drop_special wrote {exchange}/{product} files={files} rows={rows} elapsed_s={:.1}",
                        started.elapsed().as_secs_f64()
                    );
                }
                Ok(stats)
            })
            .with_context(|| format!("spawn drop_special write worker {worker_id}"))?;
        write_joins.push(handle);
    }
    drop(write_rx);

    let remaining = Arc::new(Mutex::new(remaining_counts));
    let pending: Arc<Mutex<BTreeMap<(String, String), ProductBatch>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let reducer = {
        let remaining = Arc::clone(&remaining);
        let pending = Arc::clone(&pending);
        thread::Builder::new()
            .name("cme-tas-ds-reduce".to_string())
            .spawn(move || -> Result<u64> {
                let mut released = 0u64;
                while let Ok(result) = result_rx.recv() {
                    let key = (result.job.exchange.clone(), result.job.product.clone());
                    let ready = {
                        let mut pending_guard = pending.lock().expect("drop_special pending lock");
                        let mut remaining_guard =
                            remaining.lock().expect("drop_special remaining lock");
                        let batch =
                            pending_guard
                                .entry(key.clone())
                                .or_insert_with(|| ProductBatch {
                                    key: ProductKey {
                                        exchange: result.job.exchange,
                                        product: result.job.product,
                                    },
                                    rows: Vec::new(),
                                });
                        batch.rows.extend(result.rows);
                        let left = remaining_guard.get_mut(&key).ok_or_else(|| {
                            anyhow!("reducer saw unknown product {}/{}", key.0, key.1)
                        })?;
                        if *left == 0 {
                            bail!("reducer underflow for {}/{}", key.0, key.1);
                        }
                        *left -= 1;
                        (*left == 0).then(|| {
                            remaining_guard.remove(&key);
                            pending_guard.remove(&key).expect("pending product batch")
                        })
                    };
                    if let Some(batch) = ready {
                        write_tx
                            .send(batch)
                            .context("enqueue drop_special product write")?;
                        released += 1;
                    }
                }
                Ok(released)
            })
            .context("spawn drop_special reducer")?
    };

    let mut compute_joins = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let db = Arc::clone(&db);
        let job_rx = job_rx.clone();
        let result_tx = result_tx.clone();
        let abort = Arc::clone(&abort);
        let rics_done = Arc::clone(&rics_done);
        let session_calendar = Arc::clone(&session_calendar);
        let readahead = config.readahead_bytes;
        let handle = thread::Builder::new()
            .name(format!("cme-tas-ds-compute-{worker_id}"))
            .spawn(move || -> Result<ComputeStats> {
                let mut stats = ComputeStats { rics: 0, rows: 0 };
                while let Ok(job) = job_rx.recv() {
                    if abort.load(AtomicOrdering::Relaxed) {
                        break;
                    }
                    match scan_one_ric(
                        &db,
                        &job.ric,
                        start_ns,
                        end_ns,
                        readahead,
                        &session_calendar,
                    ) {
                        Ok(rows) => {
                            stats.rics += 1;
                            stats.rows += rows.len() as u64;
                            result_tx.send(RicResult { job, rows }).map_err(|_| {
                                anyhow!("drop_special compute result channel closed")
                            })?;
                            let done = rics_done.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                            if done % 20 == 0 {
                                info!(
                                    "drop_special rics_done={done} elapsed_s={:.1}",
                                    started.elapsed().as_secs_f64()
                                );
                            }
                        }
                        Err(err) => {
                            abort.store(true, AtomicOrdering::Relaxed);
                            let err = err.context(format!(
                                "compute worker {worker_id} failed on {}",
                                job.ric
                            ));
                            error!("cme_tas_drop_special_1min {err:#}");
                            return Err(err);
                        }
                    }
                }
                Ok(stats)
            })
            .with_context(|| format!("spawn drop_special compute worker {worker_id}"))?;
        compute_joins.push(handle);
    }
    drop(result_tx);
    drop(job_rx);

    let mut computed_rics = 0u64;
    let mut computed_rows = 0u64;
    let mut compute_err = None;
    for join in compute_joins {
        match join
            .join()
            .map_err(|_| anyhow!("drop_special compute worker panicked"))
        {
            Ok(Ok(stats)) => {
                computed_rics += stats.rics;
                computed_rows += stats.rows;
            }
            Ok(Err(err)) => {
                abort.store(true, AtomicOrdering::Relaxed);
                if compute_err.is_none() {
                    compute_err = Some(err);
                }
            }
            Err(err) => {
                abort.store(true, AtomicOrdering::Relaxed);
                if compute_err.is_none() {
                    compute_err = Some(err);
                }
            }
        }
    }
    let released = reducer
        .join()
        .map_err(|_| anyhow!("drop_special reducer panicked"))??;

    let mut files = 0u64;
    let mut rows = 0u64;
    let mut products_done = 0u64;
    let mut write_err = None;
    for join in write_joins {
        match join
            .join()
            .map_err(|_| anyhow!("drop_special write worker panicked"))
        {
            Ok(Ok(stats)) => {
                files += stats.files;
                rows += stats.rows;
                products_done += stats.products;
            }
            Ok(Err(err)) => {
                if write_err.is_none() {
                    write_err = Some(err);
                }
            }
            Err(err) => {
                if write_err.is_none() {
                    write_err = Some(err);
                }
            }
        }
    }
    if let Some(err) = compute_err {
        return Err(err);
    }
    if let Some(err) = write_err {
        return Err(err);
    }
    info!(
        "cme_tas_drop_special_1min products={products_done}/{released} rics={computed_rics} compute_rows={computed_rows} files={files} rows={rows} elapsed_s={:.1}",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .target(env_logger::Target::Pipe(Box::new(FlushWriter(
            std::io::stderr(),
        ))))
        .init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config).unwrap_or_else(|err| {
        panic!("read drop_special config {}: {err}", args.config.display());
    });
    let config: DropSpecialConfig = toml::from_str(&content).unwrap_or_else(|err| {
        panic!("parse drop_special config {}: {err}", args.config.display());
    });
    if let Err(err) = run(&config) {
        eprintln!("{err:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::is_day_in_overwrite_window;

    #[test]
    fn overwrite_without_bounds_removes_every_output_day() {
        assert!(is_day_in_overwrite_window(20260403, None, None));
    }

    #[test]
    fn overwrite_bounds_include_only_selected_trade_days() {
        assert!(!is_day_in_overwrite_window(
            20260402,
            Some(20260403),
            Some(20260406)
        ));
        assert!(is_day_in_overwrite_window(
            20260403,
            Some(20260403),
            Some(20260406)
        ));
        assert!(is_day_in_overwrite_window(
            20260406,
            Some(20260403),
            Some(20260406)
        ));
        assert!(!is_day_in_overwrite_window(
            20260407,
            Some(20260403),
            Some(20260406)
        ));
    }
}
