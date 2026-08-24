//! Hourly trade-notional KLL from TAS RocksDB `cme_trade`.
//!
//! Same ClickHouse table shape as crypto `tardis_ipc_replay` `kll_only`.
//! Printable trades only. Special is not sampled. One sequential iterator
//! with a large readahead; do not open a second writer on the primary DB.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use clap::Parser;
use cme_tas_replay::hourly_kll::{
    timestamp_us_from_utc_ns, trade_notional, HourlyNotionalKll, HourlyNotionalKllSnapshot,
    CME_TAS_KLL_TABLE, CME_TAS_KLL_VENUE,
};
use cme_tas_replay::{
    decode_cme_trade, decode_ric, encode_key, key_ts_utc_ns, parse_date_time_ns, CF_CME_TRADE,
    KEY_LEN, RIC_LEN,
};
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use log::{info, warn};
use mkt_parsers::msg::trade_notional_kll_msg::TradeNotionalKllMsg;
use rocksdb::{Direction, IteratorMode, Options, ReadOptions, DB};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_READAHEAD_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_BATCH_ROWS: usize = 10_000;
const DEFAULT_FLUSH_MS: u64 = 1_000;
const DEFAULT_QUEUE_CAPACITY: usize = 100_000;
const DEFAULT_CLICKHOUSE_URL: &str = "http://127.0.0.1:8123";
const DEFAULT_DATABASE: &str = "baseline";
const PROGRESS_TRADES: u64 = 1_000_000;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_kll")]
struct Args {
    #[arg(long, default_value = "config/cme_tas_kll.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KllConfig {
    rocksdb_dir: PathBuf,
    #[serde(default = "default_secondary_dir")]
    secondary_dir: PathBuf,
    #[serde(default)]
    rics: Vec<String>,
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
    #[serde(default = "default_readahead_bytes")]
    readahead_bytes: usize,
    #[serde(default)]
    overwrite_existing: bool,
    #[serde(default)]
    clickhouse: ClickHouseConfig,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ClickHouseConfig {
    url: String,
    database: String,
    table: String,
    batch_rows: usize,
    flush_ms: u64,
    queue_capacity: usize,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_CLICKHOUSE_URL.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            table: CME_TAS_KLL_TABLE.to_string(),
            batch_rows: DEFAULT_BATCH_ROWS,
            flush_ms: DEFAULT_FLUSH_MS,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

fn default_secondary_dir() -> PathBuf {
    PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb.kll.secondary")
}

fn default_readahead_bytes() -> usize {
    DEFAULT_READAHEAD_BYTES
}

struct ClickHouseWriterConfig {
    url: String,
    database: String,
    table: String,
    batch_rows: usize,
    flush_interval: Duration,
}

struct ClickHouseWriterStats {
    inserted_rows: u64,
    inserted_batches: u64,
}

struct StageClickHouseWriter {
    sender: Sender<Bytes>,
    join_handle: thread::JoinHandle<Result<ClickHouseWriterStats>>,
}

impl StageClickHouseWriter {
    fn start(config: ClickHouseWriterConfig, queue_capacity: usize) -> Result<Self> {
        if config.batch_rows == 0 {
            bail!("clickhouse.batch_rows must be > 0");
        }
        if queue_capacity == 0 {
            bail!("clickhouse.queue_capacity must be > 0");
        }
        let (sender, receiver) = bounded(queue_capacity);
        let join_handle = thread::Builder::new()
            .name(format!("{}-clickhouse-writer", config.table))
            .spawn(move || run_stage_clickhouse_writer(receiver, config))
            .context("spawn KLL ClickHouse writer")?;
        Ok(Self {
            sender,
            join_handle,
        })
    }

    fn sender(&self) -> Sender<Bytes> {
        self.sender.clone()
    }

    fn finish(self) -> Result<ClickHouseWriterStats> {
        drop(self.sender);
        self.join_handle
            .join()
            .map_err(|_| anyhow!("KLL ClickHouse writer panicked"))?
    }
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

fn encode_hourly_kll(snapshot: &HourlyNotionalKllSnapshot, symbol: &str) -> Result<Bytes> {
    let payload = TradeNotionalKllMsg {
        symbol: symbol.to_string(),
        venue: CME_TAS_KLL_VENUE,
        hour_start_ms: snapshot.hour_start_ms,
        sketch: snapshot.sketch.clone(),
    }
    .to_bytes()
    .context("encode hourly notional KLL payload")?;
    let mut row = Vec::with_capacity(32 + symbol.len() + payload.len());
    row.extend_from_slice(&snapshot.hour_start_ms.to_le_bytes());
    append_var_uint(&mut row, symbol.len() as u64);
    row.extend_from_slice(symbol.as_bytes());
    row.extend_from_slice(&(snapshot.sketch.sample_count as u64).to_le_bytes());
    row.extend_from_slice(&(snapshot.sketch.level_capacity as u32).to_le_bytes());
    append_var_uint(&mut row, payload.len() as u64);
    row.extend_from_slice(&payload);
    Ok(Bytes::from(row))
}

fn append_var_uint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn validate_identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("invalid ClickHouse identifier: {value}");
    }
    Ok(())
}

fn clickhouse_http_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(1)
        .timeout(Duration::from_secs(300))
        .build()
        .context("build ClickHouse HTTP client")
}

fn clickhouse_execute(client: &reqwest::blocking::Client, url: &str, query: &str) -> Result<()> {
    let response = client
        .post(url.trim_end_matches('/'))
        .query(&[("query", query)])
        .body(Vec::new())
        .send()
        .with_context(|| format!("ClickHouse request failed: {query}"))?;
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status();
    let body = response.text().unwrap_or_default();
    bail!("ClickHouse request failed: status={status} body={body}");
}

fn clickhouse_execute_sync_mutation(
    client: &reqwest::blocking::Client,
    url: &str,
    query: &str,
) -> Result<()> {
    let response = client
        .post(url.trim_end_matches('/'))
        .query(&[("query", query), ("mutations_sync", "2")])
        .body(Vec::new())
        .send()
        .with_context(|| format!("ClickHouse mutation failed: {query}"))?;
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status();
    let body = response.text().unwrap_or_default();
    bail!("ClickHouse mutation failed: status={status} body={body}");
}

fn ensure_hourly_kll_table(url: &str, database: &str, table: &str) -> Result<()> {
    validate_identifier(database)?;
    validate_identifier(table)?;
    let client = clickhouse_http_client()?;
    clickhouse_execute(
        &client,
        url,
        &format!("CREATE DATABASE IF NOT EXISTS {database}"),
    )?;
    clickhouse_execute(&client, url, &format!(
        "CREATE TABLE IF NOT EXISTS {database}.{table} (hour_start DateTime64(3, 'UTC') CODEC(Delta, ZSTD), symbol String, sample_count UInt64, level_capacity UInt32, payload String CODEC(ZSTD)) ENGINE = MergeTree PARTITION BY toYYYYMM(hour_start) ORDER BY (symbol, hour_start)"
    ))
}

fn clickhouse_symbol_list(symbols: &[String]) -> Result<String> {
    if symbols.is_empty() {
        bail!("ClickHouse symbol list is empty");
    }
    let mut out = String::new();
    for (i, symbol) in symbols.iter().enumerate() {
        if symbol.is_empty() || symbol.contains('\'') {
            bail!("invalid ClickHouse symbol {symbol:?}");
        }
        if i > 0 {
            out.push(',');
        }
        out.push('\'');
        out.push_str(symbol);
        out.push('\'');
    }
    Ok(out)
}

fn delete_hourly_kll_range(
    url: &str,
    database: &str,
    table: &str,
    symbols: Option<&[String]>,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> Result<()> {
    validate_identifier(database)?;
    validate_identifier(table)?;
    let client = clickhouse_http_client()?;
    let mut clauses = Vec::new();
    if let Some(symbols) = symbols {
        clauses.push(format!("symbol IN ({})", clickhouse_symbol_list(symbols)?));
    }
    if let Some(start_ms) = start_ms {
        clauses.push(format!(
            "hour_start >= fromUnixTimestamp64Milli({start_ms})"
        ));
    }
    if let Some(end_ms) = end_ms {
        clauses.push(format!("hour_start < fromUnixTimestamp64Milli({end_ms})"));
    }
    let where_sql = if clauses.is_empty() {
        "1".to_string()
    } else {
        clauses.join(" AND ")
    };
    clickhouse_execute_sync_mutation(
        &client,
        url,
        &format!("ALTER TABLE {database}.{table} DELETE WHERE {where_sql}"),
    )
}

fn ns_to_hour_ms(ns: u64) -> Result<i64> {
    let ms = i64::try_from(ns / 1_000_000).map_err(|_| anyhow!("ns {ns} exceeds i64 ms"))?;
    Ok(cme_tas_replay::hourly_kll::align_ms(
        ms,
        cme_tas_replay::hourly_kll::HOUR_MS,
    ))
}

fn run_stage_clickhouse_writer(
    receiver: Receiver<Bytes>,
    config: ClickHouseWriterConfig,
) -> Result<ClickHouseWriterStats> {
    let client = clickhouse_http_client()?;
    let mut batch = Vec::with_capacity(config.batch_rows);
    let mut stats = ClickHouseWriterStats {
        inserted_rows: 0,
        inserted_batches: 0,
    };
    loop {
        match receiver.recv_timeout(config.flush_interval) {
            Ok(payload) => {
                batch.push(payload);
                if batch.len() >= config.batch_rows {
                    flush_stage_clickhouse_batch(&client, &config, &mut batch, &mut stats)?;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                flush_stage_clickhouse_batch(&client, &config, &mut batch, &mut stats)?;
            }
            Err(RecvTimeoutError::Disconnected) => {
                flush_stage_clickhouse_batch(&client, &config, &mut batch, &mut stats)?;
                return Ok(stats);
            }
        }
    }
}

fn flush_stage_clickhouse_batch(
    client: &reqwest::blocking::Client,
    config: &ClickHouseWriterConfig,
    batch: &mut Vec<Bytes>,
    stats: &mut ClickHouseWriterStats,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut body = Vec::with_capacity(batch.iter().map(Bytes::len).sum());
    for row in batch.iter() {
        body.extend_from_slice(row);
    }
    let query = format!(
        "INSERT INTO {}.{} FORMAT RowBinary",
        config.database, config.table
    );
    let response = client
        .post(config.url.trim_end_matches('/'))
        .query(&[("query", query.as_str())])
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .send()
        .with_context(|| {
            format!(
                "ClickHouse insert into {}.{}",
                config.database, config.table
            )
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let response_body = response.text().unwrap_or_default();
        bail!(
            "ClickHouse insert failed: table={}.{} status={} body={}",
            config.database,
            config.table,
            status,
            response_body
        );
    }
    stats.inserted_rows = stats.inserted_rows.saturating_add(batch.len() as u64);
    stats.inserted_batches = stats.inserted_batches.saturating_add(1);
    batch.clear();
    Ok(())
}

struct RicKll {
    ric: String,
    kll: HourlyNotionalKll,
    trades: u64,
    hours: u64,
}

impl RicKll {
    fn new(ric: String) -> Self {
        Self {
            ric,
            kll: HourlyNotionalKll::new(),
            trades: 0,
            hours: 0,
        }
    }

    fn on_trade(&mut self, ts_utc_ns: u64, price: i64, volume: u32) -> Result<Option<Bytes>> {
        let timestamp_us = timestamp_us_from_utc_ns(ts_utc_ns)?;
        let notional = trade_notional(price, volume)?;
        self.trades = self.trades.saturating_add(1);
        let Some(snapshot) = self.kll.on_notional(timestamp_us, notional) else {
            return Ok(None);
        };
        self.hours = self.hours.saturating_add(1);
        Ok(Some(encode_hourly_kll(&snapshot, &self.ric)?))
    }

    fn flush(&mut self) -> Result<Option<Bytes>> {
        let Some(snapshot) = self.kll.flush() else {
            return Ok(None);
        };
        self.hours = self.hours.saturating_add(1);
        Ok(Some(encode_hourly_kll(&snapshot, &self.ric)?))
    }
}

fn send_row(sender: &Sender<Bytes>, row: Bytes) -> Result<()> {
    sender
        .send(row)
        .map_err(|_| anyhow!("hourly KLL writer stopped"))
}

fn finish_ric(state: &mut RicKll, sender: &Sender<Bytes>) -> Result<()> {
    if let Some(row) = state.flush()? {
        send_row(sender, row)?;
    }
    if state.kll.late_trades() > 0 {
        warn!(
            "Hourly KLL late trades: ric={} late={}",
            state.ric,
            state.kll.late_trades()
        );
    }
    info!(
        "Hourly KLL ric complete: ric={} trades={} hours={}",
        state.ric, state.trades, state.hours
    );
    Ok(())
}

fn scan_trades(
    db: &DB,
    readahead_bytes: usize,
    rics: &[String],
    start_ns: Option<u64>,
    end_ns: Option<u64>,
    sender: &Sender<Bytes>,
) -> Result<(u64, u64, u64)> {
    let Some(cf) = db.cf_handle(CF_CME_TRADE) else {
        bail!("column family {CF_CME_TRADE} missing");
    };
    let allow: Option<BTreeSet<&str>> = if rics.is_empty() {
        None
    } else {
        Some(rics.iter().map(|s| s.as_str()).collect())
    };
    let last_allowed = allow
        .as_ref()
        .and_then(|set| set.iter().next_back().copied());
    let start_key = match allow.as_ref().and_then(|set| set.iter().next().copied()) {
        Some(ric) => encode_key(ric, start_ns.unwrap_or(0), 0, 0)?,
        None => [0u8; KEY_LEN],
    };
    let iter = db.iterator_cf_opt(
        cf,
        trade_read_opts(readahead_bytes),
        IteratorMode::From(&start_key, Direction::Forward),
    );
    let mut current: Option<RicKll> = None;
    let mut source_trades = 0u64;
    let mut rics_done = 0u64;
    let mut last_progress = Instant::now();
    let started = Instant::now();
    for item in iter {
        let (key, value) = item.context("scan cme_trade")?;
        if key.len() != KEY_LEN {
            bail!("cme_trade key length {} is not {KEY_LEN}", key.len());
        }
        let ric = decode_ric(&key[..RIC_LEN])?;
        if let Some(allow) = &allow {
            if !allow.contains(ric.as_str()) {
                if last_allowed.is_some_and(|last| ric.as_str() > last) {
                    break;
                }
                continue;
            }
        }
        let ts = key_ts_utc_ns(&key)?;
        if start_ns.is_some_and(|start| ts < start) || end_ns.is_some_and(|end| ts >= end) {
            continue;
        }
        let rec = decode_cme_trade(&value)?;
        if rec.ric != ric {
            bail!("cme_trade value ric {} does not match key {ric}", rec.ric);
        }
        if rec.ts_utc_ns != ts {
            bail!(
                "cme_trade value ts {} does not match key {ts} for {ric}",
                rec.ts_utc_ns
            );
        }
        if current.as_ref().is_some_and(|state| state.ric != ric) {
            if let Some(mut prev) = current.take() {
                finish_ric(&mut prev, sender)?;
                rics_done = rics_done.saturating_add(1);
            }
        }
        if current.is_none() {
            current = Some(RicKll::new(ric.clone()));
        }
        let state = current.as_mut().expect("current ric");
        if let Some(row) = state.on_trade(rec.ts_utc_ns, rec.price, rec.volume)? {
            send_row(sender, row)?;
        }
        source_trades = source_trades.saturating_add(1);
        if source_trades % PROGRESS_TRADES == 0
            || last_progress.elapsed() >= Duration::from_secs(30)
        {
            info!(
                "Hourly KLL progress: ric={} trades={} rics_done={} elapsed={:.2?}",
                ric,
                source_trades,
                rics_done,
                started.elapsed()
            );
            last_progress = Instant::now();
        }
    }
    if let Some(mut prev) = current.take() {
        finish_ric(&mut prev, sender)?;
        rics_done = rics_done.saturating_add(1);
    }
    Ok((source_trades, rics_done, started.elapsed().as_secs()))
}

fn replay(config: &KllConfig) -> Result<()> {
    if config.readahead_bytes == 0 {
        bail!("readahead_bytes must be > 0");
    }
    let start_ns = parse_bound(config.start.as_deref())?;
    let end_ns = parse_bound(config.end.as_deref())?;
    if let (Some(start), Some(end)) = (start_ns, end_ns) {
        if start >= end {
            bail!("start must be before end");
        }
    }
    let table = if config.clickhouse.table.is_empty() {
        CME_TAS_KLL_TABLE.to_string()
    } else {
        config.clickhouse.table.clone()
    };
    ensure_hourly_kll_table(&config.clickhouse.url, &config.clickhouse.database, &table)?;
    if config.overwrite_existing {
        let start_ms = start_ns.map(ns_to_hour_ms).transpose()?;
        let end_ms = end_ns.map(ns_to_hour_ms).transpose()?;
        let symbols = if config.rics.is_empty() {
            None
        } else {
            Some(config.rics.as_slice())
        };
        info!(
            "Replacing existing hourly KLL range: symbols={} start_ms={:?} end_ms={:?}",
            if config.rics.is_empty() {
                "ALL".to_string()
            } else {
                config.rics.join(",")
            },
            start_ms,
            end_ms
        );
        delete_hourly_kll_range(
            &config.clickhouse.url,
            &config.clickhouse.database,
            &table,
            symbols,
            start_ms,
            end_ms,
        )?;
    }
    let db = open_rocksdb_secondary(&config.rocksdb_dir, &config.secondary_dir)?;
    let writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: table.clone(),
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let sender = writer.sender();
    info!(
        "Starting CME TAS hourly KLL: table={} readahead_bytes={} rics={} start={:?} end={:?}",
        table,
        config.readahead_bytes,
        if config.rics.is_empty() {
            "ALL".to_string()
        } else {
            config.rics.len().to_string()
        },
        config.start,
        config.end
    );
    let (trades, rics_done, elapsed_s) = scan_trades(
        &db,
        config.readahead_bytes,
        &config.rics,
        start_ns,
        end_ns,
        &sender,
    )?;
    drop(sender);
    let stats = writer.finish()?;
    info!(
        "CME TAS hourly KLL complete: trades={} rics={} hours={} batches={} elapsed_s={}",
        trades, rics_done, stats.inserted_rows, stats.inserted_batches, elapsed_s
    );
    Ok(())
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config)
        .with_context(|| format!("read KLL config {}", args.config.display()))?;
    let config: KllConfig = toml::from_str(&content)
        .with_context(|| format!("parse KLL config {}", args.config.display()))?;
    replay(&config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::hourly_kll::HOUR_MS;

    #[test]
    fn rowbinary_round_trip_matches_crypto_layout() {
        let mut kll = HourlyNotionalKll::new();
        assert!(kll.on_notional(1_000, 12.5).is_none());
        let snapshot = kll.flush().expect("hour");
        assert_eq!(snapshot.hour_start_ms, 0);
        let row = encode_hourly_kll(&snapshot, "ADF26").unwrap();
        let mut cur = row.as_ref();
        let hour = i64::from_le_bytes(cur[..8].try_into().unwrap());
        assert_eq!(hour, 0);
        cur = &cur[8..];
        assert_eq!(cur[0] as usize, "ADF26".len());
        cur = &cur[1..];
        assert_eq!(&cur[..5], b"ADF26");
        cur = &cur[5..];
        let samples = u64::from_le_bytes(cur[..8].try_into().unwrap());
        assert_eq!(samples, 1);
        let _ = HOUR_MS;
    }
}
