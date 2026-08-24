//! Sparse 1s backtest + ylabel from TAS RocksDB `cme_trade` + `cme_quote`.
//!
//! Existing hourly KLL is left alone. Special is not used. Idle seconds with
//! an unchanged book are not written. Each expiry RIC is sequential (book
//! state). Distinct RICs are sharded across workers. Do not open a second
//! writer on the primary DB.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use clap::Parser;
use cme_tas_replay::hourly_kll::timestamp_us_from_utc_ns;
use cme_tas_replay::sparse_1s::{
    encode_market_1s_clickhouse_row, encode_ylabel_clickhouse_row,
    market_1s_clickhouse_columns_sql, top_of_book_from_quote, trade_price_amount,
    trade_price_amount_reject_reason, ylabel_clickhouse_columns_sql, ylabel_table_name,
    Sparse1sAggregator, SparseYlabelAggregator, BACKTEST_TABLE, YLABEL_HORIZON_MS,
};
use cme_tas_replay::{
    decode_cme_quote, decode_cme_trade, decode_ric, encode_key, encode_ric, key_ts_utc_ns,
    parse_date_time_ns, CF_CME_QUOTE, CF_CME_TRADE, KEY_LEN, RIC_LEN,
};
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use log::{info, warn};
use rocksdb::{Direction, IteratorMode, Options, ReadOptions, DB};
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_READAHEAD_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_BATCH_ROWS: usize = 50_000;
const DEFAULT_FLUSH_MS: u64 = 1_000;
const DEFAULT_QUEUE_CAPACITY: usize = 200_000;
const DEFAULT_WORKERS: usize = 32;
const DEFAULT_CLICKHOUSE_URL: &str = "http://127.0.0.1:8123";
const DEFAULT_DATABASE: &str = "baseline";
const PROGRESS_EVENTS: u64 = 5_000_000;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_backtest")]
struct Args {
    #[arg(long, default_value = "config/cme_tas_backtest.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BacktestConfig {
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
    #[serde(default = "default_workers")]
    workers: usize,
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
    batch_rows: usize,
    flush_ms: u64,
    queue_capacity: usize,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_CLICKHOUSE_URL.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            batch_rows: DEFAULT_BATCH_ROWS,
            flush_ms: DEFAULT_FLUSH_MS,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

fn default_secondary_dir() -> PathBuf {
    PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb.backtest.secondary")
}

fn default_readahead_bytes() -> usize {
    DEFAULT_READAHEAD_BYTES
}

fn default_workers() -> usize {
    DEFAULT_WORKERS
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
            .context("spawn backtest ClickHouse writer")?;
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
            .map_err(|_| anyhow!("backtest ClickHouse writer panicked"))?
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
    for need in [CF_CME_TRADE, CF_CME_QUOTE] {
        if !names.iter().any(|name| name == need) {
            bail!("rocksdb {} has no {need} column family", primary.display());
        }
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

fn ensure_tables(url: &str, database: &str, tables: &[String], columns_sql: &str) -> Result<()> {
    validate_identifier(database)?;
    let client = clickhouse_http_client()?;
    clickhouse_execute(
        &client,
        url,
        &format!("CREATE DATABASE IF NOT EXISTS {database}"),
    )?;
    for table in tables {
        validate_identifier(table)?;
        clickhouse_execute(
            &client,
            url,
            &format!(
                "CREATE TABLE IF NOT EXISTS {database}.{table} ({columns_sql}) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (symbol, ts)"
            ),
        )?;
    }
    Ok(())
}

fn delete_range(
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
        clauses.push(format!("ts >= fromUnixTimestamp64Milli({start_ms})"));
    }
    if let Some(end_ms) = end_ms {
        clauses.push(format!("ts < fromUnixTimestamp64Milli({end_ms})"));
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

fn ns_to_ms(ns: u64) -> Result<i64> {
    i64::try_from(ns / 1_000_000).map_err(|_| anyhow!("ns {ns} exceeds i64 ms"))
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

enum EventKind {
    Quote,
    Trade,
}

fn parse_key(key: &[u8]) -> Result<(String, u64)> {
    if key.len() != KEY_LEN {
        bail!("key length {} is not {KEY_LEN}", key.len());
    }
    Ok((decode_ric(&key[..RIC_LEN])?, key_ts_utc_ns(key)?))
}

fn next_item(
    iter: &mut rocksdb::DBIteratorWithThreadMode<'_, DB>,
) -> Result<Option<(Box<[u8]>, Box<[u8]>)>> {
    match iter.next() {
        Some(item) => Ok(Some(item.context("scan rocksdb")?)),
        None => Ok(None),
    }
}

fn send_row(sender: &Sender<Bytes>, row: Bytes) -> Result<()> {
    sender
        .send(row)
        .map_err(|_| anyhow!("ClickHouse writer stopped"))
}

struct RicState {
    ric: String,
    bars: Sparse1sAggregator,
    ylabels: Vec<SparseYlabelAggregator>,
    trades: u64,
    skipped_trades: u64,
    quotes: u64,
    bar_rows: u64,
}

impl RicState {
    fn new(ric: String) -> Result<Self> {
        let ylabels = YLABEL_HORIZON_MS
            .iter()
            .map(|h| SparseYlabelAggregator::new(*h))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            ric,
            bars: Sparse1sAggregator::new(),
            ylabels,
            trades: 0,
            skipped_trades: 0,
            quotes: 0,
            bar_rows: 0,
        })
    }
}

fn apply_quote(
    state: &mut RicState,
    ts_utc_ns: u64,
    value: &[u8],
    bar_sender: &Sender<Bytes>,
    ylabel_senders: &[Sender<Bytes>],
) -> Result<()> {
    let rec = decode_cme_quote(value)?;
    if rec.ric != state.ric {
        bail!("cme_quote ric {} does not match {}", rec.ric, state.ric);
    }
    if rec.ts_utc_ns != ts_utc_ns {
        bail!(
            "cme_quote ts {} does not match key {ts_utc_ns} for {}",
            rec.ts_utc_ns,
            state.ric
        );
    }
    let Some(book) = top_of_book_from_quote(rec.bid, rec.bid_size, rec.ask, rec.ask_size) else {
        return Ok(());
    };
    let timestamp_us = timestamp_us_from_utc_ns(ts_utc_ns)?;
    for bar in state.bars.on_quote(timestamp_us, book)? {
        state.bar_rows = state.bar_rows.saturating_add(1);
        send_row(
            bar_sender,
            Bytes::from(encode_market_1s_clickhouse_row(&state.ric, &bar)),
        )?;
    }
    let midp = book.midp();
    for (agg, sender) in state.ylabels.iter_mut().zip(ylabel_senders.iter()) {
        for y in agg.on_midp(timestamp_us, midp)? {
            send_row(
                sender,
                Bytes::from(encode_ylabel_clickhouse_row(&state.ric, &y)),
            )?;
        }
    }
    state.quotes = state.quotes.saturating_add(1);
    Ok(())
}

fn apply_trade(
    state: &mut RicState,
    ts_utc_ns: u64,
    value: &[u8],
    bar_sender: &Sender<Bytes>,
    ylabel_senders: &[Sender<Bytes>],
) -> Result<()> {
    let rec = decode_cme_trade(value)?;
    if rec.ric != state.ric {
        bail!("cme_trade ric {} does not match {}", rec.ric, state.ric);
    }
    if rec.ts_utc_ns != ts_utc_ns {
        bail!(
            "cme_trade ts {} does not match key {ts_utc_ns} for {}",
            rec.ts_utc_ns,
            state.ric
        );
    }
    let Some((price, amount)) = trade_price_amount(rec.price, rec.volume) else {
        let reason = trade_price_amount_reject_reason(rec.price, rec.volume);
        warn!(
            "skip invalid trade: ric={} ts_utc_ns={} price_e9={} volume={} aggressor={} reason={}",
            state.ric, ts_utc_ns, rec.price, rec.volume, rec.aggressor, reason
        );
        state.skipped_trades = state.skipped_trades.saturating_add(1);
        return Ok(());
    };
    if !(rec.aggressor == 0 || rec.aggressor == 1 || rec.aggressor == 2) {
        warn!(
            "skip invalid trade: ric={} ts_utc_ns={} price_e9={} volume={} aggressor={} reason=bad_aggressor",
            state.ric, ts_utc_ns, rec.price, rec.volume, rec.aggressor
        );
        state.skipped_trades = state.skipped_trades.saturating_add(1);
        return Ok(());
    }
    let timestamp_us = timestamp_us_from_utc_ns(ts_utc_ns)?;
    for bar in state
        .bars
        .on_trade(timestamp_us, rec.aggressor, price, amount)?
    {
        state.bar_rows = state.bar_rows.saturating_add(1);
        send_row(
            bar_sender,
            Bytes::from(encode_market_1s_clickhouse_row(&state.ric, &bar)),
        )?;
    }
    for (agg, sender) in state.ylabels.iter_mut().zip(ylabel_senders.iter()) {
        for y in agg.on_trade(timestamp_us, price, amount)? {
            send_row(
                sender,
                Bytes::from(encode_ylabel_clickhouse_row(&state.ric, &y)),
            )?;
        }
    }
    state.trades = state.trades.saturating_add(1);
    Ok(())
}

fn finish_ric(
    mut state: RicState,
    bar_sender: &Sender<Bytes>,
    ylabel_senders: &[Sender<Bytes>],
) -> Result<()> {
    for bar in state.bars.finish()? {
        state.bar_rows = state.bar_rows.saturating_add(1);
        send_row(
            bar_sender,
            Bytes::from(encode_market_1s_clickhouse_row(&state.ric, &bar)),
        )?;
    }
    for (agg, sender) in state.ylabels.iter_mut().zip(ylabel_senders.iter()) {
        for y in agg.finish()? {
            send_row(
                sender,
                Bytes::from(encode_ylabel_clickhouse_row(&state.ric, &y)),
            )?;
        }
    }
    info!(
        "Backtest ric complete: ric={} trades={} skipped_trades={} quotes={} bars={}",
        state.ric, state.trades, state.skipped_trades, state.quotes, state.bar_rows
    );
    Ok(())
}

fn next_ric_seek_key(ric: &str) -> Result<Option<[u8; KEY_LEN]>> {
    let mut prefix = encode_ric(ric)?;
    for i in (0..RIC_LEN).rev() {
        if prefix[i] != 0xff {
            prefix[i] += 1;
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
    let cf = db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow!("column family {cf_name} missing"))?;
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
    let mut rics = collect_rics_from_cf(db, CF_CME_QUOTE, readahead_bytes)?;
    rics.extend(collect_rics_from_cf(db, CF_CME_TRADE, readahead_bytes)?);
    if !filter.is_empty() {
        let allow: BTreeSet<&str> = filter.iter().map(String::as_str).collect();
        rics.retain(|ric| allow.contains(ric.as_str()));
    }
    Ok(rics.into_iter().collect())
}

fn shard_rics(rics: &[String], workers: usize) -> Vec<Vec<String>> {
    let mut shards = vec![Vec::new(); workers];
    for (i, ric) in rics.iter().enumerate() {
        shards[i % workers].push(ric.clone());
    }
    shards
}

fn scan_one_ric(
    db: &DB,
    ric: &str,
    readahead_bytes: usize,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
    bar_sender: &Sender<Bytes>,
    ylabel_senders: &[Sender<Bytes>],
    events_total: &AtomicU64,
    rics_done: &AtomicU64,
    started: Instant,
) -> Result<u64> {
    let start_key = encode_key(ric, start_ns.unwrap_or(0), 0, 0)?;
    let quote_cf = db
        .cf_handle(CF_CME_QUOTE)
        .ok_or_else(|| anyhow!("column family {CF_CME_QUOTE} missing"))?;
    let trade_cf = db
        .cf_handle(CF_CME_TRADE)
        .ok_or_else(|| anyhow!("column family {CF_CME_TRADE} missing"))?;
    let mut quote_iter = db.iterator_cf_opt(
        quote_cf,
        trade_read_opts(readahead_bytes),
        IteratorMode::From(&start_key, Direction::Forward),
    );
    let mut trade_iter = db.iterator_cf_opt(
        trade_cf,
        trade_read_opts(readahead_bytes),
        IteratorMode::From(&start_key, Direction::Forward),
    );
    let mut quote_item = next_item(&mut quote_iter)?;
    let mut trade_item = next_item(&mut trade_iter)?;
    let mut state = RicState::new(ric.to_string())?;
    let mut events = 0u64;
    let mut last_progress = Instant::now();

    loop {
        let q = match &quote_item {
            Some((key, _)) => Some(parse_key(key)?),
            None => None,
        };
        let t = match &trade_item {
            Some((key, _)) => Some(parse_key(key)?),
            None => None,
        };
        let next = match (q, t) {
            (None, None) => break,
            (Some((item_ric, ts)), None) => (EventKind::Quote, item_ric, ts),
            (None, Some((item_ric, ts))) => (EventKind::Trade, item_ric, ts),
            (Some((q_ric, q_ts)), Some((t_ric, t_ts))) => match q_ric.cmp(&t_ric) {
                Ordering::Less => (EventKind::Quote, q_ric, q_ts),
                Ordering::Greater => (EventKind::Trade, t_ric, t_ts),
                Ordering::Equal => {
                    if q_ts <= t_ts {
                        (EventKind::Quote, q_ric, q_ts)
                    } else {
                        (EventKind::Trade, t_ric, t_ts)
                    }
                }
            },
        };
        let (kind, item_ric, ts) = next;
        if item_ric != ric {
            break;
        }
        if start_ns.is_some_and(|start| ts < start) || end_ns.is_some_and(|end| ts >= end) {
            match kind {
                EventKind::Quote => quote_item = next_item(&mut quote_iter)?,
                EventKind::Trade => trade_item = next_item(&mut trade_iter)?,
            }
            continue;
        }
        match kind {
            EventKind::Quote => {
                let value = quote_item
                    .as_ref()
                    .map(|(_, v)| v.to_vec())
                    .context("quote value")?;
                apply_quote(&mut state, ts, &value, bar_sender, ylabel_senders)?;
                quote_item = next_item(&mut quote_iter)?;
            }
            EventKind::Trade => {
                let value = trade_item
                    .as_ref()
                    .map(|(_, v)| v.to_vec())
                    .context("trade value")?;
                apply_trade(&mut state, ts, &value, bar_sender, ylabel_senders)?;
                trade_item = next_item(&mut trade_iter)?;
            }
        }
        events = events.saturating_add(1);
        let total = events_total.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        if total % PROGRESS_EVENTS == 0 || last_progress.elapsed() >= Duration::from_secs(30) {
            info!(
                "Backtest progress: ric={} worker_events={} events={} rics_done={} elapsed={:.2?}",
                ric,
                events,
                total,
                rics_done.load(AtomicOrdering::Relaxed),
                started.elapsed()
            );
            last_progress = Instant::now();
        }
    }
    finish_ric(state, bar_sender, ylabel_senders)?;
    rics_done.fetch_add(1, AtomicOrdering::Relaxed);
    Ok(events)
}

fn scan(
    db: Arc<DB>,
    workers: usize,
    readahead_bytes: usize,
    rics: &[String],
    start_ns: Option<u64>,
    end_ns: Option<u64>,
    bar_sender: &Sender<Bytes>,
    ylabel_senders: &[Sender<Bytes>],
) -> Result<(u64, u64, u64)> {
    let listed = collect_rics(&db, readahead_bytes, rics)?;
    if listed.is_empty() {
        bail!("no RICs to scan");
    }
    let workers = workers.min(listed.len()).max(1);
    info!(
        "Backtest RIC list: rics={} workers={} (quote and trade prefixes, {} seeks each)",
        listed.len(),
        workers,
        listed.len()
    );
    let shards = shard_rics(&listed, workers);
    let events_total = Arc::new(AtomicU64::new(0));
    let rics_done = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut joins = Vec::with_capacity(workers);
    for (worker_id, shard) in shards.into_iter().enumerate() {
        if shard.is_empty() {
            continue;
        }
        let db = Arc::clone(&db);
        let bar_sender = bar_sender.clone();
        let ylabel_senders = ylabel_senders.to_vec();
        let events_total = Arc::clone(&events_total);
        let rics_done = Arc::clone(&rics_done);
        let handle = thread::Builder::new()
            .name(format!("cme-tas-bt-{worker_id}"))
            .spawn(move || -> Result<u64> {
                let mut local = 0u64;
                for ric in shard {
                    local = local.saturating_add(scan_one_ric(
                        &db,
                        &ric,
                        readahead_bytes,
                        start_ns,
                        end_ns,
                        &bar_sender,
                        &ylabel_senders,
                        &events_total,
                        &rics_done,
                        started,
                    )?);
                }
                Ok(local)
            })
            .with_context(|| format!("spawn backtest worker {worker_id}"))?;
        joins.push(handle);
    }
    for handle in joins {
        handle
            .join()
            .map_err(|_| anyhow!("backtest worker panicked"))??;
    }
    Ok((
        events_total.load(AtomicOrdering::Relaxed),
        rics_done.load(AtomicOrdering::Relaxed),
        started.elapsed().as_secs(),
    ))
}

fn replay(config: &BacktestConfig) -> Result<()> {
    if config.readahead_bytes == 0 {
        bail!("readahead_bytes must be > 0");
    }
    if config.workers == 0 {
        bail!("workers must be >= 1");
    }
    let start_ns = parse_bound(config.start.as_deref())?;
    let end_ns = parse_bound(config.end.as_deref())?;
    if let (Some(start), Some(end)) = (start_ns, end_ns) {
        if start >= end {
            bail!("start must be before end");
        }
    }
    let ylabel_tables = YLABEL_HORIZON_MS
        .iter()
        .map(|h| ylabel_table_name(*h))
        .collect::<Result<Vec<_>>>()?;
    ensure_tables(
        &config.clickhouse.url,
        &config.clickhouse.database,
        &[BACKTEST_TABLE.to_string()],
        &market_1s_clickhouse_columns_sql(),
    )?;
    ensure_tables(
        &config.clickhouse.url,
        &config.clickhouse.database,
        &ylabel_tables,
        &ylabel_clickhouse_columns_sql(),
    )?;
    if config.overwrite_existing {
        let start_ms = start_ns.map(ns_to_ms).transpose()?;
        let end_ms = end_ns.map(ns_to_ms).transpose()?;
        let symbols = if config.rics.is_empty() {
            None
        } else {
            Some(config.rics.as_slice())
        };
        info!(
            "Replacing existing sparse 1s/ylabel range: symbols={} start_ms={:?} end_ms={:?}",
            if config.rics.is_empty() {
                "ALL".to_string()
            } else {
                config.rics.join(",")
            },
            start_ms,
            end_ms
        );
        delete_range(
            &config.clickhouse.url,
            &config.clickhouse.database,
            BACKTEST_TABLE,
            symbols,
            start_ms,
            end_ms,
        )?;
        for table in &ylabel_tables {
            delete_range(
                &config.clickhouse.url,
                &config.clickhouse.database,
                table,
                symbols,
                start_ms,
                end_ms,
            )?;
        }
    }
    let db = Arc::new(open_rocksdb_secondary(
        &config.rocksdb_dir,
        &config.secondary_dir,
    )?);
    let bar_writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: BACKTEST_TABLE.to_string(),
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let ylabel_writers = ylabel_tables
        .iter()
        .map(|table| {
            StageClickHouseWriter::start(
                ClickHouseWriterConfig {
                    url: config.clickhouse.url.clone(),
                    database: config.clickhouse.database.clone(),
                    table: table.clone(),
                    batch_rows: config.clickhouse.batch_rows,
                    flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
                },
                config.clickhouse.queue_capacity,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let bar_sender = bar_writer.sender();
    let ylabel_senders: Vec<Sender<Bytes>> = ylabel_writers
        .iter()
        .map(StageClickHouseWriter::sender)
        .collect();
    info!(
        "Starting CME TAS sparse 1s + ylabel: workers={} readahead_bytes={} rics={} start={:?} end={:?}",
        config.workers,
        config.readahead_bytes,
        if config.rics.is_empty() {
            "ALL".to_string()
        } else {
            config.rics.len().to_string()
        },
        config.start,
        config.end
    );
    let (events, rics_done, elapsed_s) = scan(
        db,
        config.workers,
        config.readahead_bytes,
        &config.rics,
        start_ns,
        end_ns,
        &bar_sender,
        &ylabel_senders,
    )?;
    drop(bar_sender);
    drop(ylabel_senders);
    let bar_stats = bar_writer.finish()?;
    let ylabel_stats = ylabel_writers
        .into_iter()
        .map(StageClickHouseWriter::finish)
        .collect::<Result<Vec<_>>>()?;
    info!(
        "CME TAS sparse 1s complete: events={} rics={} bars={} batches={} elapsed_s={}",
        events, rics_done, bar_stats.inserted_rows, bar_stats.inserted_batches, elapsed_s
    );
    for (table, stats) in ylabel_tables.iter().zip(ylabel_stats.iter()) {
        info!(
            "ylabel complete: table={} rows={} batches={}",
            table, stats.inserted_rows, stats.inserted_batches
        );
    }
    Ok(())
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config)
        .with_context(|| format!("read backtest config {}", args.config.display()))?;
    let config: BacktestConfig = toml::from_str(&content)
        .with_context(|| format!("parse backtest config {}", args.config.display()))?;
    replay(&config)
}
