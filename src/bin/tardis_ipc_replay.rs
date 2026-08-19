//! Replay Tardis trades and incremental L2 files through the live market-data IPC contract.

#![allow(dead_code)] // Legacy market-data IPC helpers remain until the separate publisher replaces them.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use clap::{Parser, ValueEnum};
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use csv::StringRecord;
use flate2::read::GzDecoder;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use mkt_parsers::msg::mkt_msg::{IncMsg, Level, TradeMsg};
use mkt_parsers::msg::trade_flow_feature_msg::{
    TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
};
use mkt_parsers::msg::trade_notional_kll_msg::{TradeNotionalKllMsg, TRADE_NOTIONAL_KLL_MAX_BYTES};
use mkt_signal::common::amount_threshold::AmountThreshold;
use mkt_signal::depth_pub::orderbook::OrderBook;
use mkt_signal::factor_pub::trade_flow_feature_pub::local_baseline::{
    BaselineBar, HourlyNotionalKll, HourlyNotionalKllSnapshot, LocalBaselineAggregator,
};
use mkt_signal::factor_pub::trade_market_1s::{
    encode_market_1s_clickhouse_row, market_1s_clickhouse_columns_sql,
    market_1s_clickhouse_select_sql, market_1s_clickhouse_value_columns, market_1s_filename,
    market_1s_table_name, parse_market_1s_clickhouse_tsv, utc_day_start_sec, validate_market_1s_day,
    write_market_1s_hdf, TopOfBookTracker, TradeMarket1sAggregator, TradeMarket1sBar,
    DEFAULT_FILE_SIDS, DEFAULT_MARKET_1S_OUTPUT_DIR, SECONDS_PER_DAY,
};
use mkt_signal::factor_pub::trade_notional_kll::{
    load_merged_hourly_kll, order_size_thresholds, previous_month, utc_month_bounds,
    validate_order_size_quantiles, HOUR_MS,
};
use order_common::TradingVenue;
use rayon::prelude::*;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

const TRADE_PAYLOAD_BYTES: usize = 128;
const INCREMENTAL_PAYLOAD_BYTES: usize = 2048;
const MAX_LEVELS_PER_INCREMENTAL_CHUNK: usize = 100;
const MAX_LEVELS_PER_BOOK_EVENT: usize = 10_000;
const HISTORY_SIZE: usize = 100;
const MAX_SUBSCRIBERS: usize = 10;
const BASELINE_DATABASE: &str = "baseline";
const BASELINE_VALUE_COUNT: usize = TRADE_FLOW_FEATURE_DIM + 80;
const DEFAULT_CLICKHOUSE_URL: &str = "http://127.0.0.1:18123";
const DEFAULT_CLICKHOUSE_BATCH_ROWS: usize = 10_000;
const DEFAULT_CLICKHOUSE_FLUSH_MS: u64 = 1_000;
const DEFAULT_CLICKHOUSE_QUEUE_CAPACITY: usize = 100_000;
const PROGRESS_LOG_EVENTS: u64 = 1_000_000;
const PROGRESS_BAR_WIDTH: usize = 24;

#[derive(Parser, Debug)]
#[command(name = "tardis_ipc_replay")]
#[command(about = "Replay Tardis trades and incremental L2 as dat_pbs IPC market data")]
struct Args {
    /// Replay TOML configuration.
    #[arg(long, default_value = "config/tardis_replay.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
struct ReplayConfig {
    /// Legacy root containing Tardis trades/ and incremental_book_L2/ directories.
    /// Used for every symbol unless symbol_data_dirs overrides it.
    #[serde(default)]
    data_dir: Option<PathBuf>,
    /// Optional per-symbol Tardis roots for layouts with one directory per symbol.
    #[serde(default)]
    symbol_data_dirs: BTreeMap<String, PathBuf>,
    /// Venue used in dat_pbs/<SYMBOL>/<venue>/trade and incremental service names.
    venue: String,
    /// Symbols are standardized to uppercase before discovering files and opening IPC services.
    symbols: Vec<String>,
    #[serde(default)]
    start_date: Option<String>,
    #[serde(default)]
    end_date: Option<String>,
    #[serde(default = "default_replay_workers")]
    replay_workers: usize,
    #[serde(default = "default_publish_ipc")]
    publish_ipc: bool,
    #[serde(default)]
    #[serde(alias = "publish_hourly_notional_kll")]
    write_hourly_notional_kll: bool,
    /// Read trades and write hourly notional KLL rows without baseline or L2 work.
    #[serde(default)]
    kll_only: bool,
    /// Rebuild only 5s/60s trade baseline rows using previous-month KLL thresholds.
    #[serde(default)]
    order_size_only: bool,
    /// Rebuild only 5s/60s depth baseline rows; preserve trade and KLL tables.
    #[serde(default)]
    depth_only: bool,
    /// Read trades and incremental L2 and write 1s bars to ClickHouse.
    /// Daily HDF is only an optional export of those rows.
    #[serde(default)]
    market_1s_only: bool,
    /// Delete selected symbol/date rows before replaying so reruns are idempotent.
    #[serde(default)]
    overwrite_existing: bool,
    #[serde(default)]
    order_size: OrderSizeConfig,
    #[serde(default)]
    market_1s: Market1sConfig,
    #[serde(default)]
    clickhouse: ClickHouseConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct OrderSizeConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_medium_quantile")]
    medium_quantile: f32,
    #[serde(default = "default_large_quantile")]
    large_quantile: f32,
}

#[derive(Debug, Clone, Deserialize)]
struct Market1sConfig {
    /// Write 1s CTA market bars to ClickHouse in the default full replay,
    /// alongside 5s/60s baseline and hourly KLL. `market_1s_only` still skips
    /// those outputs. Daily HDF is only an export of the ClickHouse rows.
    #[serde(default = "default_market_1s_enabled")]
    enabled: bool,
    #[serde(default = "default_market_1s_output_dir")]
    output_dir: PathBuf,
    #[serde(default = "default_market_1s_file_sids")]
    file_sids: String,
    #[serde(default = "default_market_1s_python")]
    python: PathBuf,
    #[serde(default = "default_market_1s_write_clickhouse")]
    write_clickhouse: bool,
    #[serde(default = "default_market_1s_write_hdf")]
    write_hdf: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct MonthlyAmountThreshold {
    start_us: i64,
    end_us: i64,
    threshold: AmountThreshold,
}

#[derive(Debug, Deserialize)]
struct ClickHouseConfig {
    #[serde(default = "default_clickhouse_url")]
    url: String,
    #[serde(default = "default_clickhouse_database")]
    database: String,
    #[serde(default = "default_clickhouse_batch_rows")]
    batch_rows: usize,
    #[serde(default = "default_clickhouse_flush_ms")]
    flush_ms: u64,
    #[serde(default = "default_clickhouse_queue_capacity")]
    queue_capacity: usize,
}

#[derive(Debug, Clone)]
struct DepthReplayTask {
    symbol: String,
    path: PathBuf,
    start_ms: i64,
    end_ms: i64,
}

#[derive(Debug, Clone)]
struct DepthReplaySymbol {
    symbol: String,
    days: Vec<DepthReplayTask>,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: default_clickhouse_url(),
            database: default_clickhouse_database(),
            batch_rows: default_clickhouse_batch_rows(),
            flush_ms: default_clickhouse_flush_ms(),
            queue_capacity: default_clickhouse_queue_capacity(),
        }
    }
}

impl Default for OrderSizeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            medium_quantile: default_medium_quantile(),
            large_quantile: default_large_quantile(),
        }
    }
}

impl Default for Market1sConfig {
    fn default() -> Self {
        Self {
            enabled: default_market_1s_enabled(),
            output_dir: default_market_1s_output_dir(),
            file_sids: default_market_1s_file_sids(),
            python: default_market_1s_python(),
            write_clickhouse: default_market_1s_write_clickhouse(),
            write_hdf: default_market_1s_write_hdf(),
        }
    }
}

fn default_replay_workers() -> usize {
    1
}

fn default_publish_ipc() -> bool {
    false
}

fn default_medium_quantile() -> f32 {
    0.5
}

fn default_large_quantile() -> f32 {
    0.9
}

fn default_clickhouse_url() -> String {
    DEFAULT_CLICKHOUSE_URL.to_string()
}

fn default_clickhouse_database() -> String {
    BASELINE_DATABASE.to_string()
}

fn default_clickhouse_batch_rows() -> usize {
    DEFAULT_CLICKHOUSE_BATCH_ROWS
}

fn default_clickhouse_flush_ms() -> u64 {
    DEFAULT_CLICKHOUSE_FLUSH_MS
}

fn default_clickhouse_queue_capacity() -> usize {
    DEFAULT_CLICKHOUSE_QUEUE_CAPACITY
}

fn default_market_1s_enabled() -> bool {
    false
}

fn default_market_1s_output_dir() -> PathBuf {
    PathBuf::from(DEFAULT_MARKET_1S_OUTPUT_DIR)
}

fn default_market_1s_file_sids() -> String {
    DEFAULT_FILE_SIDS.to_string()
}

fn default_market_1s_python() -> PathBuf {
    PathBuf::from("python3")
}

fn default_market_1s_write_clickhouse() -> bool {
    true
}

fn default_market_1s_write_hdf() -> bool {
    true
}

#[derive(Debug, Clone)]
struct TradeRow {
    timestamp_us: i64,
    id: i64,
    symbol: String,
    side: char,
    price: f64,
    amount: f64,
}

#[derive(Debug, Clone)]
struct BookLevelRow {
    timestamp_us: i64,
    is_snapshot: bool,
    is_bid: bool,
    price: f64,
    amount: f64,
}

#[derive(Debug, Clone)]
struct BookEvent {
    timestamp_us: i64,
    is_snapshot: bool,
    symbol: String,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

#[derive(Debug, Clone)]
enum ReplayEvent {
    Trade(TradeRow),
    Book(BookEvent),
}

impl ReplayEvent {
    fn timestamp_us(&self) -> i64 {
        match self {
            Self::Trade(row) => row.timestamp_us,
            Self::Book(event) => event.timestamp_us,
        }
    }
}

struct CsvGzipReader {
    paths: Vec<PathBuf>,
    next_path: usize,
    current_path: Option<PathBuf>,
    reader: Option<csv::Reader<GzDecoder<BufReader<File>>>>,
}

impl CsvGzipReader {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self {
            paths,
            next_path: 0,
            current_path: None,
            reader: None,
        }
    }

    fn next_record(&mut self) -> Result<Option<StringRecord>> {
        loop {
            if self.reader.is_none() {
                let Some(path) = self.paths.get(self.next_path).cloned() else {
                    return Ok(None);
                };
                self.next_path += 1;
                let file = File::open(&path)
                    .with_context(|| format!("failed to open Tardis file {}", path.display()))?;
                self.reader = Some(csv::Reader::from_reader(GzDecoder::new(BufReader::new(
                    file,
                ))));
                self.current_path = Some(path);
            }

            let mut record = StringRecord::new();
            let reader = self.reader.as_mut().expect("reader initialized above");
            match reader.read_record(&mut record) {
                Ok(true) => return Ok(Some(record)),
                Ok(false) => {
                    self.reader = None;
                    self.current_path = None;
                }
                Err(err) => {
                    let path = self
                        .current_path
                        .as_ref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_else(|| "<unknown>".to_string());
                    return Err(err).with_context(|| format!("failed to read Tardis CSV {path}"));
                }
            }
        }
    }

    fn file_progress(&self) -> (usize, usize) {
        let active_file = usize::from(self.reader.is_some());
        (self.next_path.saturating_sub(active_file), self.paths.len())
    }
}

struct TradeEventReader {
    records: CsvGzipReader,
}

impl TradeEventReader {
    fn next_event(&mut self) -> Result<Option<TradeRow>> {
        self.records
            .next_record()?
            .map(|record| parse_trade_record(&record))
            .transpose()
    }
}

struct BookEventReader {
    records: CsvGzipReader,
    pending: Option<BookLevelRow>,
    symbol: String,
}

impl BookEventReader {
    fn next_event(&mut self) -> Result<Option<BookEvent>> {
        let first = match self.pending.take() {
            Some(row) => row,
            None => match self.records.next_record()? {
                Some(record) => parse_book_record(&record)?,
                None => return Ok(None),
            },
        };

        let timestamp_us = first.timestamp_us;
        let is_snapshot = first.is_snapshot;
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        append_book_level(&first, &mut bids, &mut asks);

        loop {
            let Some(record) = self.records.next_record()? else {
                break;
            };
            let row = parse_book_record(&record)?;
            if row.timestamp_us != timestamp_us {
                self.pending = Some(row);
                break;
            }
            if bids.len() + asks.len() >= MAX_LEVELS_PER_BOOK_EVENT {
                // A pathological same-timestamp batch must not monopolize replay.
                // The next chunk keeps the same timestamp and is applied in order.
                self.pending = Some(row);
                break;
            }
            append_book_level(&row, &mut bids, &mut asks);
        }

        Ok(Some(BookEvent {
            timestamp_us,
            is_snapshot,
            symbol: self.symbol.clone(),
            bids,
            asks,
        }))
    }
}

fn append_book_level(row: &BookLevelRow, bids: &mut Vec<Level>, asks: &mut Vec<Level>) {
    let level = Level::from_values(row.price, row.amount);
    if row.is_bid {
        bids.push(level);
    } else {
        asks.push(level);
    }
}

struct ReplayInput {
    trades: Option<TradeEventReader>,
    books: Option<BookEventReader>,
    next_trade: Option<TradeRow>,
    next_book: Option<BookEvent>,
}

impl ReplayInput {
    fn new(
        data_dir: &Path,
        venue_slug: &str,
        symbol: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> Result<Self> {
        let trades = Some(TradeEventReader {
            records: CsvGzipReader::new(discover_files(
                data_dir, "trades", venue_slug, symbol, start_date, end_date,
            )?),
        });
        let books = Some(BookEventReader {
            records: CsvGzipReader::new(discover_files(
                data_dir,
                "incremental_book_L2",
                venue_slug,
                symbol,
                start_date,
                end_date,
            )?),
            symbol: symbol.to_string(),
            pending: None,
        });

        let mut input = Self {
            trades,
            books,
            next_trade: None,
            next_book: None,
        };
        input.refill()?;
        Ok(input)
    }

    fn refill(&mut self) -> Result<()> {
        if self.next_trade.is_none() {
            if let Some(reader) = self.trades.as_mut() {
                self.next_trade = reader.next_event()?;
            }
        }
        if self.next_book.is_none() {
            if let Some(reader) = self.books.as_mut() {
                self.next_book = reader.next_event()?;
            }
        }
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<ReplayEvent>> {
        self.refill()?;
        let event = match (&self.next_trade, &self.next_book) {
            (None, None) => None,
            (Some(_), None) => self.next_trade.take().map(ReplayEvent::Trade),
            (None, Some(_)) => self.next_book.take().map(ReplayEvent::Book),
            (Some(trade), Some(book)) => match trade.timestamp_us.cmp(&book.timestamp_us) {
                // Apply book changes before a same-timestamp trade, matching the useful downstream order.
                Ordering::Less => self.next_trade.take().map(ReplayEvent::Trade),
                Ordering::Equal | Ordering::Greater => self.next_book.take().map(ReplayEvent::Book),
            },
        };
        Ok(event)
    }

    fn file_progress(&self) -> (usize, usize) {
        let (trade_done, trade_total) = self
            .trades
            .as_ref()
            .map(|reader| reader.records.file_progress())
            .unwrap_or_default();
        let (book_done, book_total) = self
            .books
            .as_ref()
            .map(|reader| reader.records.file_progress())
            .unwrap_or_default();
        (trade_done + book_done, trade_total + book_total)
    }
}

/// A contiguous L2 reconstruction. `is_snapshot` is deliberately ignored:
/// every source row is applied as a price-level update in event-time order.
struct DepthAggregator {
    orderbook: OrderBook,
    next_update_id: i64,
    next_bar_ms: i64,
    end_ms: i64,
}

impl DepthAggregator {
    fn new(start_ms: i64, end_ms: i64) -> Self {
        Self {
            orderbook: OrderBook::new(),
            next_update_id: 1,
            next_bar_ms: start_ms,
            end_ms,
        }
    }

    fn on_book(&mut self, event: &BookEvent) -> Result<Vec<(i64, [f64; 80])>> {
        let target_ms = align_5s(event.timestamp_us.div_euclid(1_000));
        let completed = self.emit_before(target_ms)?;
        let bids: Vec<(f64, f64)> = event
            .bids
            .iter()
            .map(|level| (level.price, level.amount))
            .collect();
        let asks: Vec<(f64, f64)> = event
            .asks
            .iter()
            .map(|level| (level.price, level.amount))
            .collect();
        self.orderbook
            .apply_update(&bids, &asks, self.next_update_id, event.timestamp_us);
        self.next_update_id = self.next_update_id.saturating_add(1);
        if !self.orderbook.is_valid() {
            self.orderbook.prune_crossed_by_best_update_id();
        }
        Ok(completed)
    }

    fn finish(&mut self) -> Result<Vec<(i64, [f64; 80])>> {
        self.emit_before(self.end_ms)
    }

    fn emit_before(&mut self, target_ms: i64) -> Result<Vec<(i64, [f64; 80])>> {
        let mut out = Vec::new();
        let limit = target_ms.min(self.end_ms);
        while self.next_bar_ms < limit {
            out.push((self.next_bar_ms, self.depth_values(self.next_bar_ms)?));
            self.next_bar_ms += 5_000;
        }
        Ok(out)
    }

    fn depth_values(&self, ts_ms: i64) -> Result<[f64; 80]> {
        let (bids, asks) = self.orderbook.get_depth(20);
        let valid_best = |levels: &[(f64, f64)]| {
            levels.first().is_some_and(|(price, amount)| {
                price.is_finite() && *price > 0.0 && amount.is_finite()
            })
        };
        if !valid_best(&bids) || !valid_best(&asks) {
            bail!("cannot emit depth bar from empty or invalid order book at ts_ms={ts_ms}");
        }
        let mut values = [0.0; 80];
        for (index, (price, amount)) in bids.iter().enumerate() {
            values[index * 2] = *price;
            values[index * 2 + 1] = *amount;
        }
        for (index, (price, amount)) in asks.iter().enumerate() {
            let offset = 40 + index * 2;
            values[offset] = *price;
            values[offset + 1] = *amount;
        }
        Ok(values)
    }
}

fn align_5s(timestamp_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(5_000)
}

struct IpcReplayPublisher {
    trade: Option<Publisher<ipc::Service, [u8; TRADE_PAYLOAD_BYTES], ()>>,
    incremental: Option<Publisher<ipc::Service, [u8; INCREMENTAL_PAYLOAD_BYTES], ()>>,
}

struct HourlyNotionalKllPublisher {
    publisher: Publisher<ipc::Service, [u8; TRADE_NOTIONAL_KLL_MAX_BYTES], ()>,
}

#[derive(Debug, Clone)]
struct ClickHouseWriterConfig {
    url: String,
    database: String,
    table: String,
    batch_rows: usize,
    flush_interval: Duration,
}

#[derive(Debug, Default)]
struct ClickHouseWriterStats {
    inserted_rows: u64,
    inserted_batches: u64,
}

struct BaselineClickHouseWriter {
    sender: Sender<Bytes>,
    join_handle: thread::JoinHandle<Result<ClickHouseWriterStats>>,
}

/// Append-only RowBinary writer used by the independent trade/depth staging tables.
struct StageClickHouseWriter {
    sender: Sender<Bytes>,
    join_handle: thread::JoinHandle<Result<ClickHouseWriterStats>>,
}

impl StageClickHouseWriter {
    fn start(config: ClickHouseWriterConfig, queue_capacity: usize) -> Result<Self> {
        let (sender, receiver) = bounded(queue_capacity);
        let join_handle = thread::Builder::new()
            .name(format!("{}-clickhouse-writer", config.table))
            .spawn(move || run_stage_clickhouse_writer(receiver, config))
            .context("spawn staging ClickHouse writer")?;
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
            .map_err(|_| anyhow!("staging ClickHouse writer panicked"))?
    }
}

impl BaselineClickHouseWriter {
    fn start(config: ClickHouseWriterConfig, queue_capacity: usize) -> Result<Self> {
        if config.batch_rows == 0 {
            bail!("clickhouse_batch_rows must be > 0");
        }
        if queue_capacity == 0 {
            bail!("clickhouse_queue_capacity must be > 0");
        }
        ensure_baseline_tables(&config.url, &config.database, &config.table)?;
        let (sender, receiver) = bounded(queue_capacity);
        let join_handle = thread::Builder::new()
            .name("baseline-clickhouse-writer".to_string())
            .spawn(move || run_clickhouse_writer(receiver, config))
            .context("spawn baseline ClickHouse writer")?;
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
            .map_err(|_| anyhow!("baseline ClickHouse writer panicked"))?
    }
}

impl HourlyNotionalKllPublisher {
    fn new(symbol: &str, venue_slug: &str) -> Result<Self> {
        let node_name = format!(
            "tardis_notional_kll_pub_{}_{}",
            symbol.to_ascii_lowercase(),
            venue_slug.replace('-', "_")
        );
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;
        let service_name = format!("factor_pub/{symbol}/{venue_slug}/trade_notional_kll_hourly");
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_NOTIONAL_KLL_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(MAX_SUBSCRIBERS)
            .history_size(HISTORY_SIZE)
            .subscriber_max_buffer_size(16)
            .open_or_create()?;
        let publisher = service
            .publisher_builder()
            .create()
            .with_context(|| format!("failed to create hourly KLL publisher {service_name}"))?;
        info!("Tardis hourly KLL publisher ready: {}", service_name);
        Ok(Self { publisher })
    }

    fn publish(
        &self,
        symbol: &str,
        venue: TradingVenue,
        snapshot: HourlyNotionalKllSnapshot,
    ) -> Result<()> {
        let data = TradeNotionalKllMsg {
            symbol: symbol.to_string(),
            venue: venue.to_u8(),
            hour_start_ms: snapshot.hour_start_ms,
            sketch: snapshot.sketch,
        }
        .to_bytes()?;
        publish_padded(&self.publisher, &data, "trade_notional_kll_hourly")
    }
}

impl IpcReplayPublisher {
    fn new(symbol: &str, venue_slug: &str) -> Result<Self> {
        let node_name = format!(
            "tardis_ipc_replay_{}_{}",
            symbol.to_ascii_lowercase(),
            venue_slug.replace('-', "_")
        );
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let trade_service_name = replay_ipc_service_name(symbol, venue_slug, "trade");
        let trade = Some(
            node.service_builder(&ServiceName::new(&trade_service_name)?)
                .publish_subscribe::<[u8; TRADE_PAYLOAD_BYTES]>()
                .max_publishers(1)
                .max_subscribers(MAX_SUBSCRIBERS)
                .history_size(HISTORY_SIZE)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?
                .publisher_builder()
                .create()
                .with_context(|| {
                    format!("failed to create trade publisher {trade_service_name}")
                })?,
        );
        let incremental_service_name = replay_ipc_service_name(symbol, venue_slug, "incremental");
        let incremental = Some(
            node.service_builder(&ServiceName::new(&incremental_service_name)?)
                .publish_subscribe::<[u8; INCREMENTAL_PAYLOAD_BYTES]>()
                .max_publishers(1)
                .max_subscribers(MAX_SUBSCRIBERS)
                .history_size(HISTORY_SIZE)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?
                .publisher_builder()
                .create()
                .with_context(|| {
                    format!("failed to create incremental publisher {incremental_service_name}")
                })?,
        );

        info!(
            "Tardis IPC publisher ready: trade={} incremental={}",
            trade.is_some(),
            incremental.is_some()
        );
        Ok(Self { trade, incremental })
    }

    fn publish_trade(&self, row: &TradeRow) -> Result<()> {
        let data = TradeMsg::create(
            row.symbol.clone(),
            row.id,
            row.timestamp_us,
            row.side,
            row.price,
            row.amount,
        )
        .to_bytes();
        let publisher = self
            .trade
            .as_ref()
            .context("trade publisher is disabled by --source")?;
        publish_padded(publisher, &data, "trade")
    }

    fn publish_book(&self, event: &BookEvent, update_id: i64) -> Result<usize> {
        let chunks = build_incremental_chunks(event, update_id)?;
        let publisher = self
            .incremental
            .as_ref()
            .context("incremental publisher is disabled by --source")?;
        for chunk in &chunks {
            publish_padded(publisher, &chunk.to_bytes(), "incremental")?;
        }
        Ok(chunks.len())
    }
}

fn publish_padded<const N: usize>(
    publisher: &Publisher<ipc::Service, [u8; N], ()>,
    data: &[u8],
    channel: &str,
) -> Result<()> {
    if data.len() > N {
        bail!(
            "{channel} payload is {} bytes, exceeds IPC limit {N}",
            data.len()
        );
    }
    let mut payload = [0u8; N];
    payload[..data.len()].copy_from_slice(data);
    let sample = publisher
        .loan_uninit()
        .with_context(|| format!("failed to loan {channel} IPC sample"))?;
    sample
        .write_payload(payload)
        .send()
        .with_context(|| format!("failed to send {channel} IPC sample"))?;
    Ok(())
}

fn build_incremental_chunks(event: &BookEvent, update_id: i64) -> Result<Vec<IncMsg>> {
    let mut chunks = Vec::new();
    let mut bid_offset = 0;
    let mut ask_offset = 0;

    while bid_offset < event.bids.len() || ask_offset < event.asks.len() {
        let bid_count = (event.bids.len() - bid_offset).min(MAX_LEVELS_PER_INCREMENTAL_CHUNK);
        let remaining = MAX_LEVELS_PER_INCREMENTAL_CHUNK - bid_count;
        let ask_count = (event.asks.len() - ask_offset).min(remaining);
        let chunk_index = u8::try_from(chunks.len())
            .context("too many incremental chunks in one Tardis event")?;
        let mut message = IncMsg::create(
            event.symbol.clone(),
            update_id,
            update_id,
            event.timestamp_us,
            event.is_snapshot,
            bid_count as u32,
            ask_count as u32,
        );
        message.set_chunk_index(chunk_index);
        message.set_is_last(false);
        for (index, level) in event.bids[bid_offset..bid_offset + bid_count]
            .iter()
            .enumerate()
        {
            message.set_bid_level(index, *level);
        }
        for (index, level) in event.asks[ask_offset..ask_offset + ask_count]
            .iter()
            .enumerate()
        {
            message.set_ask_level(index, *level);
        }
        bid_offset += bid_count;
        ask_offset += ask_count;
        chunks.push(message);
    }

    if chunks.is_empty() {
        bail!("empty Tardis book event at {}", event.timestamp_us);
    }
    let last = chunks.last_mut().expect("non-empty checked above");
    last.set_is_last(true);
    Ok(chunks)
}

fn discover_files(
    data_dir: &Path,
    dataset: &str,
    venue_slug: &str,
    symbol: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<Vec<PathBuf>> {
    let dir = data_dir.join(dataset);
    let prefixed = format!("{venue_slug}_{dataset}_");
    let suffixed = format!("_{symbol}.csv.gz");
    let mut paths = fs::read_dir(&dir)
        .with_context(|| format!("failed to read Tardis dataset directory {}", dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return false;
            };
            let date = if name.starts_with(&prefixed) && name.ends_with(&suffixed) {
                name.strip_prefix(&prefixed)
                    .and_then(|value| value.strip_suffix(&suffixed))
            } else if name.len() == 17 && name.ends_with(".csv.gz") {
                name.strip_suffix(".csv.gz")
            } else {
                None
            };
            date.is_some_and(|date| {
                date.len() == 10
                    && start_date.is_none_or(|start| date >= start)
                    && end_date.is_none_or(|end| date <= end)
            })
        })
        .collect::<Vec<_>>();
    paths.sort();
    if paths.is_empty() {
        bail!(
            "no {dataset} files for venue={} symbol={} in {} within requested dates",
            venue_slug,
            symbol,
            data_dir.display()
        );
    }
    Ok(paths)
}

fn trade_file_date(path: &Path) -> Result<NaiveDate> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| format!("trade file name {}", path.display()))?;
    let date = if name.len() == 17 && name.ends_with(".csv.gz") {
        name.strip_suffix(".csv.gz")
            .expect("checked suffix above")
            .to_string()
    } else {
        name.rsplit('_')
            .nth(1)
            .with_context(|| format!("extract date from {}", path.display()))?
            .to_string()
    };
    NaiveDate::parse_from_str(&date, "%Y-%m-%d")
        .with_context(|| format!("parse trade date {date} from {}", path.display()))
}

fn parse_trade_record(record: &StringRecord) -> Result<TradeRow> {
    let timestamp_us = parse_i64(record, 2, "timestamp")?;
    let id = parse_i64(record, 4, "id")?;
    let side = parse_side(field(record, 5, "side")?)?;
    let price = parse_price_or_amount(record, 6, "price", false)?;
    let amount = parse_price_or_amount(record, 7, "amount", false)?;
    Ok(TradeRow {
        timestamp_us,
        id,
        symbol: field(record, 1, "symbol")?.to_ascii_uppercase(),
        side,
        price,
        amount,
    })
}

fn parse_book_record(record: &StringRecord) -> Result<BookLevelRow> {
    let timestamp_us = parse_i64(record, 2, "timestamp")?;
    let is_snapshot = field(record, 4, "is_snapshot")?
        .parse::<bool>()
        .context("invalid is_snapshot")?;
    let is_bid = match field(record, 5, "side")? {
        "bid" | "Bid" | "BID" => true,
        "ask" | "Ask" | "ASK" => false,
        value => bail!("unsupported Tardis book side '{value}'"),
    };
    Ok(BookLevelRow {
        timestamp_us,
        is_snapshot,
        is_bid,
        price: parse_price_or_amount(record, 6, "price", false)?,
        // A zero amount is a valid incremental deletion.
        amount: parse_price_or_amount(record, 7, "amount", true)?,
    })
}

fn field<'a>(record: &'a StringRecord, index: usize, name: &str) -> Result<&'a str> {
    record
        .get(index)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("missing Tardis CSV field {name}"))
}

fn parse_i64(record: &StringRecord, index: usize, name: &str) -> Result<i64> {
    let value = field(record, index, name)?
        .parse::<i64>()
        .with_context(|| format!("invalid Tardis {name}"))?;
    if value <= 0 {
        bail!("Tardis {name} must be positive, got {value}");
    }
    Ok(value)
}

fn parse_price_or_amount(
    record: &StringRecord,
    index: usize,
    name: &str,
    allow_zero: bool,
) -> Result<f64> {
    let value = field(record, index, name)?
        .parse::<f64>()
        .with_context(|| format!("invalid Tardis {name}"))?;
    if !value.is_finite() || value < 0.0 || (!allow_zero && value == 0.0) {
        let expected = if allow_zero {
            "non-negative"
        } else {
            "positive"
        };
        bail!("Tardis {name} must be finite and {expected}");
    }
    Ok(value)
}

fn parse_side(value: &str) -> Result<char> {
    match value {
        "buy" | "Buy" | "BUY" => Ok('B'),
        "sell" | "Sell" | "SELL" => Ok('S'),
        _ => bail!("unsupported Tardis trade side '{value}'"),
    }
}

fn validate_dates(config: &ReplayConfig) -> Result<()> {
    for date in [&config.start_date, &config.end_date].into_iter().flatten() {
        if date.len() != 10
            || !date.as_bytes().get(4).is_some_and(|byte| *byte == b'-')
            || !date.as_bytes().get(7).is_some_and(|byte| *byte == b'-')
            || !date
                .chars()
                .enumerate()
                .all(|(index, ch)| matches!(index, 4 | 7) || ch.is_ascii_digit())
        {
            bail!("date must use YYYY-MM-DD, got '{date}'");
        }
    }
    if let (Some(start), Some(end)) = (&config.start_date, &config.end_date) {
        if start > end {
            bail!("start_date must not be after end_date");
        }
    }
    Ok(())
}

fn order_size_target_months(config: &ReplayConfig) -> Result<Vec<String>> {
    if !config.order_size.enabled {
        return Ok(Vec::new());
    }
    validate_order_size_quantiles(
        config.order_size.medium_quantile,
        config.order_size.large_quantile,
    )?;
    let start = config
        .start_date
        .as_deref()
        .context("order_size.enabled requires start_date")?;
    let end = config
        .end_date
        .as_deref()
        .context("order_size.enabled requires end_date")?;
    let start = NaiveDate::parse_from_str(start, "%Y-%m-%d")
        .context("parse order-size replay start_date")?;
    let end =
        NaiveDate::parse_from_str(end, "%Y-%m-%d").context("parse order-size replay end_date")?;
    if start > end {
        bail!("order-size replay start_date must not be after end_date");
    }

    let mut year = start.year();
    let mut month = start.month();
    let end_month = (end.year(), end.month());
    let mut months = Vec::new();
    loop {
        months.push(format!("{year:04}-{month:02}"));
        if (year, month) == end_month {
            break;
        }
        if month == 12 {
            year = year
                .checked_add(1)
                .context("order-size target month exceeds supported year range")?;
            month = 1;
        } else {
            month += 1;
        }
    }
    Ok(months)
}

fn load_replay_order_size_thresholds(
    config: &ReplayConfig,
    venue: TradingVenue,
    venue_slug: &str,
    symbols: &[String],
) -> Result<Option<BTreeMap<String, Vec<MonthlyAmountThreshold>>>> {
    let target_months = order_size_target_months(config)?;
    if target_months.is_empty() {
        return Ok(None);
    }
    let table = trade_notional_kll_table_name(venue_slug);
    let thresholds = symbols
        .par_iter()
        .map(
            |symbol| -> Result<(String, Vec<MonthlyAmountThreshold>)> {
                let mut symbol_thresholds = Vec::with_capacity(target_months.len());
                for target_month in &target_months {
                    let source_month = previous_month(target_month)?;
                    let (source_start_ms, source_end_ms) = utc_month_bounds(&source_month)?;
                    let expected_source_hours = usize::try_from(
                        (source_end_ms - source_start_ms).div_euclid(HOUR_MS),
                    )
                    .context("source-month hour count exceeds usize")?;
                    let merged = load_merged_hourly_kll(
                        &config.clickhouse.url,
                        &config.clickhouse.database,
                        &table,
                        symbol,
                        source_start_ms,
                        source_end_ms,
                    )
                    .with_context(|| {
                        format!(
                            "load source-month order-size KLL: symbol={symbol} source_month={source_month}"
                        )
                    })?;
                    if merged.source_hourly_rows != expected_source_hours {
                        bail!(
                            "incomplete source-month KLL for symbol={symbol} source_month={source_month}: expected_hours={} actual_hours={}",
                            expected_source_hours,
                            merged.source_hourly_rows
                        );
                    }
                    if merged.venue != venue.to_u8() {
                        bail!(
                            "hourly KLL venue mismatch for symbol={symbol}: expected={} actual={}",
                            venue.to_u8(),
                            merged.venue
                        );
                    }
                    let (medium_notional_threshold, large_notional_threshold) =
                        order_size_thresholds(
                            &merged.sketch,
                            config.order_size.medium_quantile,
                            config.order_size.large_quantile,
                        )?;
                    let threshold = AmountThreshold {
                        medium_notional_threshold,
                        large_notional_threshold,
                    };
                    if !threshold.is_online() {
                        bail!(
                            "invalid order-size threshold for symbol={symbol}: medium={} large={}",
                            medium_notional_threshold,
                            large_notional_threshold
                        );
                    }
                    let (target_start_ms, target_end_ms) = utc_month_bounds(target_month)?;
                    symbol_thresholds.push(MonthlyAmountThreshold {
                        start_us: target_start_ms
                            .checked_mul(1_000)
                            .context("target month start exceeds microseconds")?,
                        end_us: target_end_ms
                            .checked_mul(1_000)
                            .context("target month end exceeds microseconds")?,
                        threshold,
                    });
                    info!(
                        "Loaded replay order-size thresholds: symbol={} target_month={} source_month={} source_hours={} samples={} q_medium={} medium={} q_large={} large={}",
                        symbol,
                        target_month,
                        source_month,
                        merged.source_hourly_rows,
                        merged.sketch.sample_count,
                        config.order_size.medium_quantile,
                        medium_notional_threshold,
                        config.order_size.large_quantile,
                        large_notional_threshold,
                    );
                }
                Ok((symbol.clone(), symbol_thresholds))
            },
        )
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .collect();
    Ok(Some(thresholds))
}

fn threshold_for_timestamp(
    thresholds: &[MonthlyAmountThreshold],
    index: &mut usize,
    timestamp_us: i64,
) -> Result<AmountThreshold> {
    while *index < thresholds.len() && timestamp_us >= thresholds[*index].end_us {
        *index += 1;
    }
    let period = thresholds
        .get(*index)
        .with_context(|| format!("no order-size threshold covers timestamp_us={timestamp_us}"))?;
    if timestamp_us < period.start_us {
        bail!("no order-size threshold covers timestamp_us={timestamp_us}");
    }
    Ok(period.threshold)
}

fn replay_time_bounds(config: &ReplayConfig) -> Result<(i64, i64)> {
    let start = config
        .start_date
        .as_deref()
        .context("overwrite_existing requires start_date")?;
    let end = config
        .end_date
        .as_deref()
        .context("overwrite_existing requires end_date")?;
    let start = NaiveDate::parse_from_str(start, "%Y-%m-%d").context("parse replay start_date")?;
    let end = NaiveDate::parse_from_str(end, "%Y-%m-%d").context("parse replay end_date")?;
    let start = DateTime::<Utc>::from_naive_utc_and_offset(
        start.and_hms_opt(0, 0, 0).expect("valid midnight"),
        Utc,
    )
    .timestamp_millis();
    let end = DateTime::<Utc>::from_naive_utc_and_offset(
        end.succ_opt()
            .context("replay end_date exceeds supported range")?
            .and_hms_opt(0, 0, 0)
            .expect("valid midnight"),
        Utc,
    )
    .timestamp_millis();
    Ok((start, end))
}

fn replay(config: &ReplayConfig) -> Result<()> {
    validate_dates(config)?;
    validate_replay_mode(config)?;
    order_size_target_months(config)?;
    if config.replay_workers == 0 {
        bail!("replay_workers must be > 0");
    }
    let venue = TradingVenue::from_str(&config.venue, true)
        .map_err(|err| anyhow!("unsupported replay venue '{}': {err}", config.venue))?;
    let venue_slug = venue.data_pub_slug();
    let symbols = replay_symbols(config)?;
    if config.publish_ipc {
        bail!("tardis_ipc_replay no longer publishes trade or incremental IPC; use the separate publisher")
    }
    replay_staging(config, venue, venue_slug, &symbols)
}

fn validate_replay_mode(config: &ReplayConfig) -> Result<()> {
    let exclusive_modes = [
        config.kll_only,
        config.order_size_only,
        config.depth_only,
        config.market_1s_only,
    ]
    .into_iter()
    .filter(|enabled| *enabled)
    .count();
    if exclusive_modes > 1 {
        bail!("kll_only, order_size_only, depth_only, and market_1s_only are mutually exclusive");
    }
    if config.kll_only && !config.write_hourly_notional_kll {
        bail!("kll_only requires write_hourly_notional_kll=true");
    }
    if config.kll_only && config.order_size.enabled {
        bail!("kll_only cannot be combined with order_size.enabled=true");
    }
    if config.kll_only && config.order_size_only {
        bail!("kll_only cannot be combined with order_size_only=true");
    }
    if config.order_size_only && !config.order_size.enabled {
        bail!("order_size_only requires order_size.enabled=true");
    }
    if config.order_size_only && config.write_hourly_notional_kll {
        bail!("order_size_only requires write_hourly_notional_kll=false");
    }
    if config.order_size_only && !config.overwrite_existing {
        bail!("order_size_only requires overwrite_existing=true");
    }
    if config.depth_only && config.write_hourly_notional_kll {
        bail!("depth_only requires write_hourly_notional_kll=false");
    }
    if config.depth_only && config.order_size.enabled {
        bail!("depth_only requires order_size.enabled=false");
    }
    if config.depth_only && !config.overwrite_existing {
        bail!("depth_only requires overwrite_existing=true");
    }
    if config.market_1s_only && config.write_hourly_notional_kll {
        bail!("market_1s_only requires write_hourly_notional_kll=false");
    }
    if config.market_1s_only && config.order_size.enabled {
        bail!("market_1s_only requires order_size.enabled=false");
    }
    if config.market_1s_only && config.publish_ipc {
        bail!("market_1s_only cannot publish IPC");
    }
    if market_1s_requested(config) && !config.market_1s.write_clickhouse {
        bail!("market_1s requires write_clickhouse=true; daily HDF is only an export");
    }
    if market_1s_requested(config)
        && config.market_1s.write_hdf
        && config.market_1s.file_sids != DEFAULT_FILE_SIDS
    {
        bail!("market_1s.file_sids must be {DEFAULT_FILE_SIDS} to match tardis_1s_*_sids_1_6_*.h5");
    }
    if market_1s_requested(config) && config.start_date.is_none() {
        bail!("market_1s requires start_date");
    }
    if market_1s_requested(config) && config.end_date.is_none() {
        bail!("market_1s requires end_date");
    }
    Ok(())
}

fn market_1s_requested(config: &ReplayConfig) -> bool {
    config.market_1s_only || config.market_1s.enabled
}

fn replay_hourly_kll_only(
    config: &ReplayConfig,
    venue: TradingVenue,
    venue_slug: &str,
    symbols: &[String],
    kll_table: &str,
) -> Result<()> {
    ensure_hourly_kll_table(
        &config.clickhouse.url,
        &config.clickhouse.database,
        kll_table,
    )?;
    if config.overwrite_existing {
        let (start_ms, end_ms) = replay_time_bounds(config)?;
        info!(
            "Replacing existing hourly KLL range: symbols={} start_ms={} end_ms={}",
            symbols.join(","),
            start_ms,
            end_ms,
        );
        delete_hourly_kll_range(
            &config.clickhouse.url,
            &config.clickhouse.database,
            kll_table,
            symbols,
            start_ms,
            end_ms,
        )?;
    }

    let writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: kll_table.to_string(),
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let sender = writer.sender();
    let workers = config.replay_workers.min(symbols.len()).max(1);
    info!(
        "Starting hourly KLL-only replay: venue={} symbols={} workers={}",
        venue_slug,
        symbols.len(),
        workers
    );
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .context("build hourly KLL replay pool")?;
    let result = pool.install(|| {
        symbols.par_iter().try_for_each(|symbol| {
            replay_hourly_kll_symbol(config, venue, venue_slug, symbol, &sender)
        })
    });
    drop(sender);
    let stats = writer.finish()?;
    result?;
    info!(
        "Hourly KLL-only replay complete: rows={} batches={}",
        stats.inserted_rows, stats.inserted_batches
    );
    Ok(())
}

fn replay_hourly_kll_symbol(
    config: &ReplayConfig,
    venue: TradingVenue,
    venue_slug: &str,
    symbol: &str,
    writer: &Sender<Bytes>,
) -> Result<()> {
    let data_dir = replay_data_dir(config, symbol)?;
    let mut reader = TradeEventReader {
        records: CsvGzipReader::new(discover_files(
            data_dir,
            "trades",
            venue_slug,
            symbol,
            config.start_date.as_deref(),
            config.end_date.as_deref(),
        )?),
    };
    let mut kll = HourlyNotionalKll::new();
    let mut source_events = 0u64;
    let mut rows = 0u64;
    let mut last_progress_files = 0usize;
    let started = Instant::now();
    let (_, total_files) = reader.records.file_progress();
    log_stage_progress(
        "kll",
        symbol,
        0,
        total_files,
        source_events,
        rows,
        started.elapsed(),
    );
    while let Some(row) = reader.next_event()? {
        if let Some(snapshot) = kll.on_trade(row.timestamp_us, row.price, row.amount) {
            writer
                .send(encode_hourly_kll(&snapshot, symbol, venue.to_u8())?)
                .map_err(|_| anyhow!("hourly KLL writer stopped"))?;
            rows = rows.saturating_add(1);
        }
        source_events = source_events.saturating_add(1);
        let (files_done, _) = reader.records.file_progress();
        if files_done != last_progress_files || source_events % PROGRESS_LOG_EVENTS == 0 {
            log_stage_progress(
                "kll",
                symbol,
                files_done,
                total_files,
                source_events,
                rows,
                started.elapsed(),
            );
            last_progress_files = files_done;
        }
    }
    if let Some(snapshot) = kll.flush() {
        writer
            .send(encode_hourly_kll(&snapshot, symbol, venue.to_u8())?)
            .map_err(|_| anyhow!("hourly KLL writer stopped"))?;
        rows = rows.saturating_add(1);
    }
    let (files_done, _) = reader.records.file_progress();
    log_stage_progress(
        "kll",
        symbol,
        files_done,
        total_files,
        source_events,
        rows,
        started.elapsed(),
    );
    info!(
        "Hourly KLL replay complete: symbol={} source_events={} rows={} late_trades={} elapsed={:.2?}",
        symbol,
        source_events,
        rows,
        kll.late_trades(),
        started.elapsed()
    );
    Ok(())
}

fn replay_order_size_trade_only(
    config: &ReplayConfig,
    venue: TradingVenue,
    venue_slug: &str,
    symbols: &[String],
    trade_5s_table: &str,
    trade_60s_table: &str,
    kll_table: &str,
) -> Result<()> {
    ensure_trade_staging_tables(
        &config.clickhouse.url,
        &config.clickhouse.database,
        trade_5s_table,
        trade_60s_table,
        kll_table,
    )?;
    let size_thresholds = load_replay_order_size_thresholds(config, venue, venue_slug, symbols)?
        .context("order_size_only requires loaded order-size thresholds")?;
    let (start_ms, end_ms) = replay_time_bounds(config)?;
    delete_baseline_ranges(
        &config.clickhouse.url,
        &config.clickhouse.database,
        &[trade_5s_table, trade_60s_table],
        symbols,
        start_ms,
        end_ms,
    )?;

    let trade_5s_writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: trade_5s_table.to_string(),
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let trade_60s_writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: trade_60s_table.to_string(),
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let trade_5s_sender = trade_5s_writer.sender();
    let trade_60s_sender = trade_60s_writer.sender();
    let workers = config.replay_workers.min(symbols.len()).max(1);
    info!(
        "Starting order-size-only trade baseline replay: venue={} symbols={} workers={} dates={:?}..{:?}",
        venue_slug,
        symbols.len(),
        workers,
        config.start_date,
        config.end_date,
    );
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .context("build order-size-only replay pool")?;
    let result = pool.install(|| {
        symbols.par_iter().try_for_each(|symbol| {
            let thresholds = size_thresholds
                .get(symbol)
                .with_context(|| format!("missing order-size thresholds for symbol={symbol}"))?;
            replay_trade_stage_symbol(
                config,
                venue,
                venue_slug,
                symbol,
                &trade_5s_sender,
                &trade_60s_sender,
                None,
                Some(thresholds.as_slice()),
            )
        })
    });
    drop(trade_5s_sender);
    drop(trade_60s_sender);
    let trade_5s_stats = trade_5s_writer.finish()?;
    let trade_60s_stats = trade_60s_writer.finish()?;
    result?;
    info!(
        "Order-size-only trade baseline replay complete: trade_5s_rows={} trade_60s_rows={} trade_5s_batches={} trade_60s_batches={}",
        trade_5s_stats.inserted_rows,
        trade_60s_stats.inserted_rows,
        trade_5s_stats.inserted_batches,
        trade_60s_stats.inserted_batches,
    );
    Ok(())
}

fn replay_staging(
    config: &ReplayConfig,
    venue: TradingVenue,
    venue_slug: &str,
    symbols: &[String],
) -> Result<()> {
    let trade_5s_table = baseline_trade_table_name(venue_slug, 5_000);
    let depth_5s_table = baseline_depth_table_name(venue_slug, 5_000);
    let trade_60s_table = baseline_trade_table_name(venue_slug, 60_000);
    let depth_60s_table = baseline_depth_table_name(venue_slug, 60_000);
    let kll_table = trade_notional_kll_table_name(venue_slug);
    if config.kll_only {
        return replay_hourly_kll_only(config, venue, venue_slug, symbols, &kll_table);
    }
    if config.order_size_only {
        return replay_order_size_trade_only(
            config,
            venue,
            venue_slug,
            symbols,
            &trade_5s_table,
            &trade_60s_table,
            &kll_table,
        );
    }
    if config.depth_only {
        return replay_depth_only(
            config,
            venue_slug,
            symbols,
            &trade_5s_table,
            &depth_5s_table,
            &trade_60s_table,
            &depth_60s_table,
            &kll_table,
        );
    }
    if config.market_1s_only {
        return replay_market_1s_only(config, venue_slug, symbols);
    }
    ensure_staging_tables(
        &config.clickhouse.url,
        &config.clickhouse.database,
        &trade_5s_table,
        &depth_5s_table,
        &trade_60s_table,
        &depth_60s_table,
        &kll_table,
    )?;
    let market_1s_table = market_1s_table_name(venue_slug);
    if config.market_1s.enabled {
        ensure_market_1s_table(
            &config.clickhouse.url,
            &config.clickhouse.database,
            &market_1s_table,
        )?;
    }
    let size_thresholds = load_replay_order_size_thresholds(config, venue, venue_slug, symbols)?;
    if config.overwrite_existing {
        let (start_ms, end_ms) = replay_time_bounds(config)?;
        info!(
            "Replacing existing Tardis staging range: symbols={} start_ms={} end_ms={}",
            symbols.join(","),
            start_ms,
            end_ms,
        );
        let mut staging_tables = vec![
            trade_5s_table.as_str(),
            depth_5s_table.as_str(),
            trade_60s_table.as_str(),
            depth_60s_table.as_str(),
        ];
        if config.market_1s.enabled {
            staging_tables.push(market_1s_table.as_str());
        }
        delete_staging_range(
            &config.clickhouse.url,
            &config.clickhouse.database,
            &staging_tables,
            config
                .write_hourly_notional_kll
                .then_some(kll_table.as_str()),
            symbols,
            start_ms,
            end_ms,
        )?;
    }
    let trade_5s_writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: trade_5s_table,
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let depth_5s_writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: depth_5s_table,
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let trade_60s_writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: trade_60s_table,
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let depth_60s_writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: depth_60s_table,
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let kll_config = config
        .write_hourly_notional_kll
        .then(|| ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: kll_table,
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        });
    let market_1s_writer = config
        .market_1s
        .enabled
        .then(|| {
            StageClickHouseWriter::start(
                ClickHouseWriterConfig {
                    url: config.clickhouse.url.clone(),
                    database: config.clickhouse.database.clone(),
                    table: market_1s_table.clone(),
                    batch_rows: config.clickhouse.batch_rows,
                    flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
                },
                config.clickhouse.queue_capacity,
            )
        })
        .transpose()?;
    let trade_5s_sender = trade_5s_writer.sender();
    let depth_5s_sender = depth_5s_writer.sender();
    let trade_60s_sender = trade_60s_writer.sender();
    let depth_60s_sender = depth_60s_writer.sender();
    let market_1s_sender = market_1s_writer.as_ref().map(StageClickHouseWriter::sender);
    let depth_symbols = build_depth_tasks(config, venue_slug, symbols)?;
    let workers = config.replay_workers.min(symbols.len()).max(1);
    info!(
        "Starting split Tardis staging replay: venue={} symbols={} trade_workers={} depth_workers={} market_1s={}",
        venue_slug,
        symbols.len(),
        workers,
        workers,
        config.market_1s.enabled,
    );
    let trade_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .context("build trade replay pool")?;
    let depth_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .context("build depth replay pool")?;
    let market_1s_pool = config
        .market_1s
        .enabled
        .then(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .context("build 1s market replay pool")
        })
        .transpose()?;
    let result = std::thread::scope(|scope| {
        let trade_handle = scope.spawn(|| {
            trade_pool.install(|| {
                symbols.par_iter().try_for_each(|symbol| {
                    let size_threshold = size_thresholds
                        .as_ref()
                        .and_then(|thresholds| thresholds.get(symbol))
                        .map(Vec::as_slice);
                    replay_trade_stage_symbol(
                        config,
                        venue,
                        venue_slug,
                        symbol,
                        &trade_5s_sender,
                        &trade_60s_sender,
                        kll_config.as_ref(),
                        size_threshold,
                    )
                })
            })
        });
        let market_1s_handle = market_1s_pool.as_ref().map(|pool| {
            scope.spawn(|| {
                pool.install(|| {
                    symbols.par_iter().try_for_each(|symbol| {
                        replay_market_1s_symbol(
                            config,
                            venue_slug,
                            symbol,
                            market_1s_sender.as_ref(),
                        )
                    })
                })
            })
        });
        let depth_result = depth_pool.install(|| {
            depth_symbols.par_iter().try_for_each(|symbol| {
                replay_depth_stage_symbol(symbol, &depth_5s_sender, &depth_60s_sender)
            })
        });
        let trade_result = trade_handle
            .join()
            .map_err(|_| anyhow!("trade replay worker panicked"))?;
        let market_1s_result = market_1s_handle
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| anyhow!("1s market replay worker panicked"))
            })
            .transpose()?;
        trade_result
            .and(depth_result)
            .and(market_1s_result.unwrap_or(Ok(())))
    });
    drop(trade_5s_sender);
    drop(depth_5s_sender);
    drop(trade_60s_sender);
    drop(depth_60s_sender);
    drop(market_1s_sender);
    let trade_5s_stats = trade_5s_writer.finish()?;
    let depth_5s_stats = depth_5s_writer.finish()?;
    let trade_60s_stats = trade_60s_writer.finish()?;
    let depth_60s_stats = depth_60s_writer.finish()?;
    let market_1s_stats = market_1s_writer
        .map(StageClickHouseWriter::finish)
        .transpose()?;
    result?;
    info!(
        "Tardis staging replay complete: trade_5s_rows={} depth_5s_rows={} trade_60s_rows={} depth_60s_rows={} trade_5s_batches={} depth_5s_batches={} trade_60s_batches={} depth_60s_batches={}",
        trade_5s_stats.inserted_rows,
        depth_5s_stats.inserted_rows,
        trade_60s_stats.inserted_rows,
        depth_60s_stats.inserted_rows,
        trade_5s_stats.inserted_batches,
        depth_5s_stats.inserted_batches,
        trade_60s_stats.inserted_batches,
        depth_60s_stats.inserted_batches,
    );
    if let Some(stats) = market_1s_stats {
        info!(
            "1s market ClickHouse complete: table={} rows={} batches={}",
            market_1s_table, stats.inserted_rows, stats.inserted_batches
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn replay_depth_only(
    config: &ReplayConfig,
    venue_slug: &str,
    symbols: &[String],
    trade_5s_table: &str,
    depth_5s_table: &str,
    trade_60s_table: &str,
    depth_60s_table: &str,
    kll_table: &str,
) -> Result<()> {
    ensure_staging_tables(
        &config.clickhouse.url,
        &config.clickhouse.database,
        trade_5s_table,
        depth_5s_table,
        trade_60s_table,
        depth_60s_table,
        kll_table,
    )?;
    let depth_symbols = build_depth_tasks(config, venue_slug, symbols)?;
    validate_depth_task_coverage(config, &depth_symbols)?;
    let (start_ms, end_ms) = replay_time_bounds(config)?;
    info!(
        "Replacing existing depth-only staging range: symbols={} start_ms={} end_ms={}",
        symbols.join(","),
        start_ms,
        end_ms,
    );
    delete_staging_range(
        &config.clickhouse.url,
        &config.clickhouse.database,
        &[depth_5s_table, depth_60s_table],
        None,
        symbols,
        start_ms,
        end_ms,
    )?;

    let depth_5s_writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: depth_5s_table.to_string(),
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let depth_60s_writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: depth_60s_table.to_string(),
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let depth_5s_sender = depth_5s_writer.sender();
    let depth_60s_sender = depth_60s_writer.sender();
    let total_days: usize = depth_symbols.iter().map(|symbol| symbol.days.len()).sum();
    let workers = config.replay_workers.min(depth_symbols.len()).max(1);
    info!(
        "Starting depth-only Tardis staging replay: venue={} symbols={} day_tasks={} workers={} dates={:?}..{:?}",
        venue_slug,
        depth_symbols.len(),
        total_days,
        workers,
        config.start_date,
        config.end_date,
    );
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .context("build depth-only replay pool")?;
    let result: Result<()> = pool.install(|| {
        depth_symbols.par_iter().try_for_each(|symbol| {
            replay_depth_stage_symbol(symbol, &depth_5s_sender, &depth_60s_sender)
        })
    });
    drop(depth_5s_sender);
    drop(depth_60s_sender);
    let depth_5s_stats = depth_5s_writer.finish()?;
    let depth_60s_stats = depth_60s_writer.finish()?;
    result?;
    info!(
        "Depth-only Tardis staging replay complete: depth_5s_rows={} depth_60s_rows={} depth_5s_batches={} depth_60s_batches={}",
        depth_5s_stats.inserted_rows,
        depth_60s_stats.inserted_rows,
        depth_5s_stats.inserted_batches,
        depth_60s_stats.inserted_batches,
    );
    Ok(())
}

fn validate_depth_task_coverage(
    config: &ReplayConfig,
    symbols: &[DepthReplaySymbol],
) -> Result<()> {
    let (start_ms, end_ms) = replay_time_bounds(config)?;
    let expected_days = usize::try_from((end_ms - start_ms).div_euclid(86_400_000))
        .context("depth-only day count exceeds usize")?;
    if expected_days == 0 {
        bail!("depth-only replay requires at least one day");
    }
    for symbol in symbols {
        if symbol.days.len() != expected_days {
            bail!(
                "incomplete depth files for symbol={}: expected_days={} actual_days={}",
                symbol.symbol,
                expected_days,
                symbol.days.len()
            );
        }
        for (index, task) in symbol.days.iter().enumerate() {
            let expected_start = start_ms + index as i64 * 86_400_000;
            if task.start_ms != expected_start || task.end_ms != expected_start + 86_400_000 {
                bail!(
                    "non-contiguous depth file coverage for symbol={} index={} expected_start_ms={} actual_start_ms={} path={}",
                    symbol.symbol,
                    index,
                    expected_start,
                    task.start_ms,
                    task.path.display()
                );
            }
        }
    }
    Ok(())
}

fn replay_market_1s_only(
    config: &ReplayConfig,
    venue_slug: &str,
    symbols: &[String],
) -> Result<()> {
    let table = market_1s_table_name(venue_slug);
    ensure_market_1s_table(&config.clickhouse.url, &config.clickhouse.database, &table)?;
    if config.overwrite_existing {
        let (start_ms, end_ms) = replay_time_bounds(config)?;
        info!(
            "Replacing existing 1s market ClickHouse range: symbols={} start_ms={} end_ms={}",
            symbols.join(","),
            start_ms,
            end_ms,
        );
        delete_staging_range(
            &config.clickhouse.url,
            &config.clickhouse.database,
            &[table.as_str()],
            None,
            symbols,
            start_ms,
            end_ms,
        )?;
    }

    let clickhouse_writer = StageClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: table.clone(),
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?;
    let clickhouse_sender = clickhouse_writer.sender();
    let workers = config.replay_workers.min(symbols.len()).max(1);
    info!(
        "Starting 1s market replay: venue={} symbols={} workers={} clickhouse_table={} export_hdf={}",
        venue_slug,
        symbols.len(),
        workers,
        table,
        config.market_1s.write_hdf,
    );
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .context("build trade-only 1s replay pool")?;
    let result = pool.install(|| {
        symbols.par_iter().try_for_each(|symbol| {
            replay_market_1s_symbol(config, venue_slug, symbol, Some(&clickhouse_sender))
        })
    });
    drop(clickhouse_sender);
    let clickhouse_stats = clickhouse_writer.finish()?;
    result?;
    info!(
        "1s market ClickHouse complete: table={} rows={} batches={}",
        table, clickhouse_stats.inserted_rows, clickhouse_stats.inserted_batches
    );
    Ok(())
}

fn replay_market_1s_symbol(
    config: &ReplayConfig,
    venue_slug: &str,
    symbol: &str,
    clickhouse_sender: Option<&Sender<Bytes>>,
) -> Result<()> {
    let sender = clickhouse_sender.context("1s market ClickHouse writer is required")?;
    let table = market_1s_table_name(venue_slug);
    let data_dir = replay_data_dir(config, symbol)?;
    let trade_files = discover_files(
        data_dir,
        "trades",
        venue_slug,
        symbol,
        config.start_date.as_deref(),
        config.end_date.as_deref(),
    )?;
    let depth_files = discover_files(
        data_dir,
        "incremental_book_L2",
        venue_slug,
        symbol,
        config.start_date.as_deref(),
        config.end_date.as_deref(),
    )?;
    let mut by_day: BTreeMap<NaiveDate, (Option<PathBuf>, Option<PathBuf>)> = BTreeMap::new();
    for path in trade_files {
        let day = trade_file_date(&path)?;
        by_day.entry(day).or_default().0 = Some(path);
    }
    for path in depth_files {
        let day = trade_file_date(&path)?;
        by_day.entry(day).or_default().1 = Some(path);
    }
    let mut tracker = TopOfBookTracker::default();
    let mut seed_close = None;
    let mut written_days = 0u64;
    let mut exported_days = 0u64;
    let started = Instant::now();
    let total_days = by_day.len();
    for (index, (day, (trade_path, depth_path))) in by_day.into_iter().enumerate() {
        let trade_path = trade_path
            .with_context(|| format!("missing trades file for symbol={symbol} day={day}"))?;
        let depth_path = depth_path.with_context(|| {
            format!("missing incremental_book_L2 file for symbol={symbol} day={day}")
        })?;
        let output = config.market_1s.output_dir.join(market_1s_filename(
            symbol,
            &config.market_1s.file_sids,
            day,
        ));
        let day_start_sec = utc_day_start_sec(day)?;
        let start_ms = day_start_sec * 1_000;
        let end_ms = start_ms + SECONDS_PER_DAY * 1_000;
        let already_in_clickhouse = !config.overwrite_existing
            && market_1s_clickhouse_day_complete(
                &config.clickhouse.url,
                &config.clickhouse.database,
                &table,
                symbol,
                start_ms,
                end_ms,
            )?;
        if already_in_clickhouse {
            apply_depth_file_to_tracker(symbol, &depth_path, &mut tracker)?;
            seed_close = last_trade_price_from_file(&trade_path)?.or(seed_close);
            if config.market_1s.write_hdf && !output.is_file() {
                let bars = load_market_1s_day_from_clickhouse(
                    &config.clickhouse.url,
                    &config.clickhouse.database,
                    &table,
                    symbol,
                    start_ms,
                    end_ms,
                )?;
                validate_market_1s_day(symbol, day, &bars)?;
                write_market_1s_hdf(&output, symbol, &bars, &config.market_1s.python)
                    .with_context(|| format!("export 1s market HDF {}", output.display()))?;
                exported_days += 1;
                info!(
                    "Exported 1s market HDF from ClickHouse: symbol={} day={} path={}",
                    symbol,
                    day,
                    output.display()
                );
            } else {
                info!(
                    "Skipping existing 1s market ClickHouse day: symbol={} day={} table={}",
                    symbol, day, table
                );
            }
            continue;
        }
        let bars = aggregate_market_1s_day(
            symbol,
            day,
            &trade_path,
            &depth_path,
            &mut tracker,
            seed_close,
        )?;
        validate_market_1s_day(symbol, day, &bars)?;
        for bar in &bars {
            sender
                .send(Bytes::from(encode_market_1s_clickhouse_row(symbol, bar)))
                .map_err(|_| anyhow!("1s market ClickHouse writer stopped"))?;
        }
        if config.market_1s.write_hdf {
            write_market_1s_hdf(&output, symbol, &bars, &config.market_1s.python)
                .with_context(|| format!("export 1s market HDF {}", output.display()))?;
            exported_days += 1;
        }
        seed_close = bars.last().map(|bar| bar.close).or(seed_close);
        written_days += 1;
        info!(
            "Wrote 1s market ClickHouse day: symbol={} day={} export_hdf={} files={}/{} elapsed={:.2?}",
            symbol,
            day,
            config.market_1s.write_hdf,
            index + 1,
            total_days,
            started.elapsed()
        );
    }
    info!(
        "1s market symbol complete: symbol={} days={} clickhouse_days={} hdf_export_days={} elapsed={:.2?}",
        symbol,
        total_days,
        written_days,
        exported_days,
        started.elapsed()
    );
    Ok(())
}

fn aggregate_market_1s_day(
    symbol: &str,
    day: NaiveDate,
    trade_path: &Path,
    depth_path: &Path,
    tracker: &mut TopOfBookTracker,
    seed_close: Option<f64>,
) -> Result<Vec<TradeMarket1sBar>> {
    let mut trades = TradeEventReader {
        records: CsvGzipReader::new(vec![trade_path.to_path_buf()]),
    };
    let mut books = BookEventReader {
        records: CsvGzipReader::new(vec![depth_path.to_path_buf()]),
        pending: None,
        symbol: symbol.to_string(),
    };
    let mut next_trade = trades.next_event()?;
    let mut next_book = books.next_event()?;
    let mut aggregator = TradeMarket1sAggregator::new(day, tracker.snapshot(), seed_close)?;
    loop {
        let apply_book = match (&next_trade, &next_book) {
            (None, None) => break,
            (_, Some(book)) => next_trade
                .as_ref()
                .is_none_or(|trade| book.timestamp_us <= trade.timestamp_us),
            (Some(_), None) => false,
        };
        if apply_book {
            let event = next_book.take().expect("book present");
            tracker.apply_book_levels(event.timestamp_us, &event.bids, &event.asks);
            if let Some(book) = tracker.snapshot() {
                aggregator
                    .on_book(event.timestamp_us, book)
                    .with_context(|| {
                        format!(
                            "aggregate 1s depth symbol={symbol} day={day} path={}",
                            depth_path.display()
                        )
                    })?;
            }
            next_book = books.next_event()?;
            continue;
        }
        let row = next_trade.take().expect("trade present");
        aggregator
            .on_trade(row.timestamp_us, row.side == 'B', row.price, row.amount)
            .with_context(|| {
                format!(
                    "aggregate 1s trade symbol={symbol} day={day} path={}",
                    trade_path.display()
                )
            })?;
        next_trade = trades.next_event()?;
    }
    aggregator.finish().with_context(|| {
        format!(
            "finish 1s market symbol={symbol} day={day} trade={} depth={}",
            trade_path.display(),
            depth_path.display()
        )
    })
}

fn last_trade_price_from_file(path: &Path) -> Result<Option<f64>> {
    let mut reader = TradeEventReader {
        records: CsvGzipReader::new(vec![path.to_path_buf()]),
    };
    let mut last = None;
    while let Some(row) = reader.next_event()? {
        last = Some(row.price);
    }
    Ok(last)
}

fn apply_depth_file_to_tracker(
    symbol: &str,
    path: &Path,
    tracker: &mut TopOfBookTracker,
) -> Result<()> {
    let mut reader = BookEventReader {
        records: CsvGzipReader::new(vec![path.to_path_buf()]),
        pending: None,
        symbol: symbol.to_string(),
    };
    while let Some(event) = reader.next_event()? {
        tracker.apply_book_levels(event.timestamp_us, &event.bids, &event.asks);
    }
    Ok(())
}

fn ensure_market_1s_table(url: &str, database: &str, table: &str) -> Result<()> {
    validate_identifier(database)?;
    validate_identifier(table)?;
    let client = clickhouse_http_client()?;
    clickhouse_execute(
        &client,
        url,
        &format!("CREATE DATABASE IF NOT EXISTS {database}"),
    )?;
    clickhouse_execute(
        &client,
        url,
        &format!(
            "CREATE TABLE IF NOT EXISTS {database}.{table} ({}) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (symbol, ts)",
            market_1s_clickhouse_columns_sql()
        ),
    )?;
    for column in market_1s_clickhouse_value_columns() {
        clickhouse_execute(
            &client,
            url,
            &format!("ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS {column} Float64"),
        )?;
    }
    Ok(())
}

fn market_1s_clickhouse_day_complete(
    url: &str,
    database: &str,
    table: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<bool> {
    validate_identifier(database)?;
    validate_identifier(table)?;
    validate_identifier(symbol)?;
    let client = clickhouse_http_client()?;
    let body = clickhouse_select(
        &client,
        url,
        &format!(
            "SELECT count() FROM {database}.{table} WHERE symbol = '{symbol}' AND ts >= fromUnixTimestamp64Milli({start_ms}) AND ts < fromUnixTimestamp64Milli({end_ms})"
        ),
    )?;
    let count = body
        .trim()
        .parse::<i64>()
        .with_context(|| format!("parse 1s ClickHouse day count: {body}"))?;
    Ok(count == SECONDS_PER_DAY)
}

fn load_market_1s_day_from_clickhouse(
    url: &str,
    database: &str,
    table: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<TradeMarket1sBar>> {
    validate_identifier(database)?;
    validate_identifier(table)?;
    validate_identifier(symbol)?;
    let client = clickhouse_http_client()?;
    let body = clickhouse_select(
        &client,
        url,
        &format!(
            "SELECT {} FROM {database}.{table} WHERE symbol = '{symbol}' AND ts >= fromUnixTimestamp64Milli({start_ms}) AND ts < fromUnixTimestamp64Milli({end_ms}) ORDER BY ts",
            market_1s_clickhouse_select_sql()
        ),
    )?;
    let mut bars = Vec::with_capacity(SECONDS_PER_DAY as usize);
    for line in body.lines() {
        if line.is_empty() {
            continue;
        }
        bars.push(parse_market_1s_clickhouse_tsv(line)?);
    }
    Ok(bars)
}

fn replay_trade_stage_symbol(
    config: &ReplayConfig,
    venue: TradingVenue,
    venue_slug: &str,
    symbol: &str,
    trade_5s_sender: &Sender<Bytes>,
    trade_60s_sender: &Sender<Bytes>,
    kll_config: Option<&ClickHouseWriterConfig>,
    size_thresholds: Option<&[MonthlyAmountThreshold]>,
) -> Result<()> {
    let data_dir = replay_data_dir(config, symbol)?;
    let (_, replay_end_ms) = replay_time_bounds(config)?;
    let mut reader = TradeEventReader {
        records: CsvGzipReader::new(discover_files(
            data_dir,
            "trades",
            venue_slug,
            symbol,
            config.start_date.as_deref(),
            config.end_date.as_deref(),
        )?),
    };
    let mut aggregator = LocalBaselineAggregator::new();
    let mut hourly_kll = config
        .write_hourly_notional_kll
        .then(HourlyNotionalKll::new);
    let mut kll_rows = Vec::new();
    let mut rows_5s = 0u64;
    let mut rows_60s = 0u64;
    let mut source_events = 0u64;
    let mut threshold_index = 0usize;
    let mut last_progress_files = 0usize;
    let started = Instant::now();
    let (_, total_files) = reader.records.file_progress();
    log_stage_progress(
        "trade",
        symbol,
        0,
        total_files,
        source_events,
        rows_5s,
        started.elapsed(),
    );
    while let Some(row) = reader.next_event()? {
        let closed = match size_thresholds {
            Some(thresholds) => {
                let threshold =
                    threshold_for_timestamp(thresholds, &mut threshold_index, row.timestamp_us)?;
                aggregator.on_trade_with_threshold(
                    row.timestamp_us,
                    row.side == 'B',
                    row.price,
                    row.amount,
                    threshold,
                )
            }
            None => aggregator.on_trade(row.timestamp_us, row.side == 'B', row.price, row.amount),
        };
        for bar in closed {
            trade_5s_sender
                .send(encode_stage_row(
                    bar.start_ms,
                    symbol,
                    &bar.trade_feature_values(),
                ))
                .map_err(|_| anyhow!("trade staging writer stopped"))?;
            rows_5s += 1;
        }
        for bar in aggregator.drain_sixty_second_bars() {
            trade_60s_sender
                .send(encode_stage_row(
                    bar.start_ms,
                    symbol,
                    &bar.trade_feature_values(),
                ))
                .map_err(|_| anyhow!("60s trade staging writer stopped"))?;
            rows_60s += 1;
        }
        if let Some(kll) = hourly_kll.as_mut() {
            if let Some(snapshot) = kll.on_trade(row.timestamp_us, row.price, row.amount) {
                kll_rows.push(encode_hourly_kll(&snapshot, symbol, venue.to_u8())?);
            }
        }
        source_events += 1;
        let (files_done, _) = reader.records.file_progress();
        if files_done != last_progress_files || source_events % PROGRESS_LOG_EVENTS == 0 {
            log_stage_progress(
                "trade",
                symbol,
                files_done,
                total_files,
                source_events,
                rows_5s,
                started.elapsed(),
            );
            last_progress_files = files_done;
        }
    }
    for bar in aggregator.flush_until_ms(replay_end_ms) {
        trade_5s_sender
            .send(encode_stage_row(
                bar.start_ms,
                symbol,
                &bar.trade_feature_values(),
            ))
            .map_err(|_| anyhow!("trade staging writer stopped"))?;
        rows_5s += 1;
    }
    for bar in aggregator.drain_sixty_second_bars() {
        trade_60s_sender
            .send(encode_stage_row(
                bar.start_ms,
                symbol,
                &bar.trade_feature_values(),
            ))
            .map_err(|_| anyhow!("60s trade staging writer stopped"))?;
        rows_60s += 1;
    }
    if let Some(kll) = hourly_kll.as_mut() {
        if let Some(snapshot) = kll.flush() {
            kll_rows.push(encode_hourly_kll(&snapshot, symbol, venue.to_u8())?);
        }
    }
    if let Some(kll_config) = kll_config {
        let client = clickhouse_http_client()?;
        let mut stats = ClickHouseWriterStats::default();
        flush_stage_clickhouse_batch(&client, kll_config, &mut kll_rows, &mut stats)?;
    }
    let (files_done, _) = reader.records.file_progress();
    log_stage_progress(
        "trade",
        symbol,
        files_done,
        total_files,
        source_events,
        rows_5s,
        started.elapsed(),
    );
    info!(
        "Tardis trade staging complete: symbol={} source_events={} rows_5s={} rows_60s={} elapsed={:.2?}",
        symbol,
        source_events,
        rows_5s,
        rows_60s,
        started.elapsed(),
    );
    Ok(())
}

fn encode_hourly_kll(
    snapshot: &HourlyNotionalKllSnapshot,
    symbol: &str,
    venue: u8,
) -> Result<Bytes> {
    let payload = TradeNotionalKllMsg {
        symbol: symbol.to_string(),
        venue,
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

fn replay_depth_stage_symbol(
    symbol: &DepthReplaySymbol,
    depth_5s_sender: &Sender<Bytes>,
    depth_60s_sender: &Sender<Bytes>,
) -> Result<()> {
    let first_day = symbol
        .days
        .first()
        .with_context(|| format!("no depth files for symbol={}", symbol.symbol))?;
    let last_day = symbol.days.last().expect("first day checked above");
    let mut aggregator = DepthAggregator::new(first_day.start_ms, last_day.end_ms);
    let started = Instant::now();
    let total_days = symbol.days.len();
    let mut source_events = 0u64;
    let mut rows_5s = 0u64;
    let mut rows_60s = 0u64;
    log_stage_progress(
        "depth",
        &symbol.symbol,
        0,
        total_days,
        source_events,
        rows_5s,
        started.elapsed(),
    );
    for (day_index, task) in symbol.days.iter().enumerate() {
        let day_stats =
            replay_depth_stage_day(task, &mut aggregator, depth_5s_sender, depth_60s_sender)?;
        source_events += day_stats.source_events;
        rows_5s += day_stats.rows_5s;
        rows_60s += day_stats.rows_60s;
        log_stage_progress(
            "depth",
            &symbol.symbol,
            day_index + 1,
            total_days,
            source_events,
            rows_5s,
            started.elapsed(),
        );
    }
    let final_stats = enqueue_depth_bars(
        aggregator
            .finish()
            .with_context(|| format!("finish depth reconstruction for symbol={}", symbol.symbol))?,
        &symbol.symbol,
        depth_5s_sender,
        depth_60s_sender,
    )?;
    rows_5s += final_stats.rows_5s;
    rows_60s += final_stats.rows_60s;
    info!(
        "Tardis depth staging complete: symbol={} source_events={} rows_5s={} rows_60s={} elapsed={:.2?}",
        symbol.symbol,
        source_events,
        rows_5s,
        rows_60s,
        started.elapsed(),
    );
    Ok(())
}

#[derive(Default)]
struct DepthStageStats {
    source_events: u64,
    rows_5s: u64,
    rows_60s: u64,
}

fn replay_depth_stage_day(
    task: &DepthReplayTask,
    aggregator: &mut DepthAggregator,
    depth_5s_sender: &Sender<Bytes>,
    depth_60s_sender: &Sender<Bytes>,
) -> Result<DepthStageStats> {
    let mut reader = BookEventReader {
        records: CsvGzipReader::new(vec![task.path.clone()]),
        pending: None,
        symbol: task.symbol.clone(),
    };
    let mut stats = DepthStageStats::default();
    while let Some(event) = reader.next_event()? {
        let bars = aggregator.on_book(&event).with_context(|| {
            format!(
                "reconstruct depth for symbol={} path={}",
                task.symbol,
                task.path.display()
            )
        })?;
        let emitted = enqueue_depth_bars(bars, &task.symbol, depth_5s_sender, depth_60s_sender)?;
        stats.rows_5s += emitted.rows_5s;
        stats.rows_60s += emitted.rows_60s;
        stats.source_events += 1;
    }
    Ok(stats)
}

fn enqueue_depth_bars(
    bars: Vec<(i64, [f64; 80])>,
    symbol: &str,
    depth_5s_sender: &Sender<Bytes>,
    depth_60s_sender: &Sender<Bytes>,
) -> Result<DepthStageStats> {
    let mut stats = DepthStageStats::default();
    for (ts, values) in bars {
        depth_5s_sender
            .send(encode_stage_row(ts, symbol, &values))
            .map_err(|_| anyhow!("depth staging writer stopped"))?;
        stats.rows_5s += 1;
        if ts.rem_euclid(60_000) == 0 {
            depth_60s_sender
                .send(encode_stage_row(ts, symbol, &values))
                .map_err(|_| anyhow!("60s depth staging writer stopped"))?;
            stats.rows_60s += 1;
        }
    }
    Ok(stats)
}

fn build_depth_tasks(
    config: &ReplayConfig,
    venue_slug: &str,
    symbols: &[String],
) -> Result<Vec<DepthReplaySymbol>> {
    let mut symbols_tasks = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let mut days = Vec::new();
        let data_dir = replay_data_dir(config, symbol)?;
        for path in discover_files(
            data_dir,
            "incremental_book_L2",
            venue_slug,
            symbol,
            config.start_date.as_deref(),
            config.end_date.as_deref(),
        )? {
            let date = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.rsplit('_').nth(1))
                .ok_or_else(|| anyhow!("extract date from {}", path.display()))?;
            let day = NaiveDate::parse_from_str(date, "%Y-%m-%d")
                .with_context(|| format!("parse depth date {date}"))?;
            let start_ms = DateTime::<Utc>::from_naive_utc_and_offset(
                day.and_hms_opt(0, 0, 0).expect("valid midnight"),
                Utc,
            )
            .timestamp_millis();
            days.push(DepthReplayTask {
                symbol: symbol.clone(),
                path,
                start_ms,
                end_ms: start_ms + 86_400_000,
            });
        }
        symbols_tasks.push(DepthReplaySymbol {
            symbol: symbol.clone(),
            days,
        });
    }
    Ok(symbols_tasks)
}

fn encode_stage_row(ts_ms: i64, symbol: &str, values: &[f64]) -> Bytes {
    let mut row = Vec::with_capacity(8 + 10 + symbol.len() + values.len() * 8);
    row.extend_from_slice(&ts_ms.to_le_bytes());
    append_var_uint(&mut row, symbol.len() as u64);
    row.extend_from_slice(symbol.as_bytes());
    for value in values {
        row.extend_from_slice(&value.to_le_bytes());
    }
    Bytes::from(row)
}

fn replay_symbols(config: &ReplayConfig) -> Result<Vec<String>> {
    let symbols = normalize_replay_symbols(&config.symbols)?;
    for symbol in &symbols {
        replay_data_dir(config, symbol)?;
    }
    Ok(symbols)
}

fn normalize_replay_symbols(raw_symbols: &[String]) -> Result<Vec<String>> {
    let mut symbols = Vec::with_capacity(raw_symbols.len());
    for raw_symbol in raw_symbols {
        let symbol = raw_symbol.trim().to_ascii_uppercase();
        if symbol.is_empty() {
            bail!("symbol must not be empty");
        }
        if !symbols.contains(&symbol) {
            symbols.push(symbol);
        }
    }
    if symbols.is_empty() {
        bail!("at least one symbol is required");
    }
    Ok(symbols)
}

fn replay_ipc_service_name(symbol: &str, venue_slug: &str, channel: &str) -> String {
    format!("dat_pbs/{symbol}/{venue_slug}/{channel}")
}

fn replay_data_dir<'a>(config: &'a ReplayConfig, symbol: &str) -> Result<&'a Path> {
    if let Some((_, path)) = config
        .symbol_data_dirs
        .iter()
        .find(|(configured_symbol, _)| configured_symbol.trim().eq_ignore_ascii_case(symbol))
    {
        return Ok(path);
    }
    config.data_dir.as_deref().ok_or_else(|| {
        anyhow!(
            "missing data directory for symbol={symbol}; set symbol_data_dirs.{symbol} or data_dir"
        )
    })
}

fn replay_symbol(
    config: &ReplayConfig,
    venue: TradingVenue,
    venue_slug: &str,
    symbol: &str,
    clickhouse_sender: Option<&Sender<Bytes>>,
) -> Result<()> {
    let data_dir = replay_data_dir(config, symbol)?;
    let mut input = ReplayInput::new(
        data_dir,
        venue_slug,
        symbol,
        config.start_date.as_deref(),
        config.end_date.as_deref(),
    )?;
    let publisher = config
        .publish_ipc
        .then(|| IpcReplayPublisher::new(symbol, venue_slug))
        .transpose()?;
    let hourly_kll_publisher = config
        .write_hourly_notional_kll
        .then(|| HourlyNotionalKllPublisher::new(symbol, venue_slug))
        .transpose()?;
    let mut baseline = Some(LocalBaselineAggregator::new());
    let mut hourly_kll = config
        .write_hourly_notional_kll
        .then(HourlyNotionalKll::new);
    let (initial_files_done, total_files) = input.file_progress();

    info!(
        "Starting Tardis replay: root={} venue={} symbol={} ipc={} dates={:?}..{:?} input_files={}",
        data_dir.display(),
        venue_slug,
        symbol,
        config.publish_ipc,
        config.start_date,
        config.end_date,
        total_files
    );
    log_replay_progress(symbol, initial_files_done, total_files, 0);

    let mut previous_timestamp_us = None;
    let mut source_events = 0u64;
    let mut trade_messages = 0u64;
    let mut incremental_messages = 0u64;
    let mut hourly_kll_messages = 0u64;
    let mut next_update_id = 1i64;
    let mut last_progress_files = initial_files_done;
    while let Some(event) = input.next_event()? {
        let timestamp_us = event.timestamp_us();
        if let Some(previous) = previous_timestamp_us {
            if timestamp_us < previous {
                warn!(
                    "Tardis source timestamp moved backward: previous={} current={}",
                    previous, timestamp_us
                );
            }
        }
        previous_timestamp_us = Some(timestamp_us);

        match event {
            ReplayEvent::Trade(row) => {
                if let Some(publisher) = publisher.as_ref() {
                    publisher.publish_trade(&row)?;
                    trade_messages += 1;
                }
                if let Some(baseline) = baseline.as_mut() {
                    let closed =
                        baseline.on_trade(row.timestamp_us, row.side == 'B', row.price, row.amount);
                    if let Some(writer) = clickhouse_sender {
                        enqueue_baseline_bars(&closed, &row.symbol, venue.to_u8(), writer)?;
                    }
                }
                if let Some(kll) = hourly_kll.as_mut() {
                    if let Some(snapshot) = kll.on_trade(row.timestamp_us, row.price, row.amount) {
                        hourly_kll_publisher
                            .as_ref()
                            .expect("KLL publisher is enabled with KLL aggregation")
                            .publish(&row.symbol, venue, snapshot)?;
                        hourly_kll_messages += 1;
                    }
                }
            }
            ReplayEvent::Book(event) => {
                if let Some(baseline) = baseline.as_mut() {
                    let closed = baseline.on_book(
                        event.timestamp_us,
                        event.is_snapshot,
                        &event.bids,
                        &event.asks,
                    );
                    if let Some(writer) = clickhouse_sender {
                        enqueue_baseline_bars(&closed, &event.symbol, venue.to_u8(), writer)?;
                    }
                }
                if let Some(publisher) = publisher.as_ref() {
                    incremental_messages += publisher.publish_book(&event, next_update_id)? as u64;
                }
                next_update_id = next_update_id
                    .checked_add(1)
                    .context("synthetic update id overflow")?;
            }
        }
        source_events += 1;
        let (files_done, _) = input.file_progress();
        if files_done != last_progress_files || source_events % PROGRESS_LOG_EVENTS == 0 {
            log_replay_progress(symbol, files_done, total_files, source_events);
            last_progress_files = files_done;
        }
    }
    let (files_done, _) = input.file_progress();
    log_replay_progress(symbol, files_done, total_files, source_events);

    if let Some(baseline) = baseline.as_mut() {
        let closed = baseline.flush();
        if let Some(writer) = clickhouse_sender {
            enqueue_baseline_bars(&closed, symbol, venue.to_u8(), writer)?;
        }
        for stats in baseline.stats() {
            info!(
                "Tardis local baseline: bar_ms={} closed={} traded={} depth20={} padded_depth20={} late_trades={}",
                stats.bar_ms,
                stats.closed_bars,
                stats.traded_bars,
                stats.depth20_bars,
                stats.padded_depth20_bars,
                stats.late_trades
            );
        }
    }
    if let Some(kll) = hourly_kll.as_mut() {
        if let Some(snapshot) = kll.flush() {
            hourly_kll_publisher
                .as_ref()
                .expect("KLL publisher is enabled with KLL aggregation")
                .publish(symbol, venue, snapshot)?;
            hourly_kll_messages += 1;
        }
        info!(
            "Tardis hourly notional KLL: published={} late_trades={}",
            hourly_kll_messages,
            kll.late_trades()
        );
    }

    info!(
        "Tardis IPC replay complete: source_events={} trade_messages={} incremental_messages={} hourly_kll_messages={}",
        source_events, trade_messages, incremental_messages, hourly_kll_messages
    );
    Ok(())
}

fn log_replay_progress(symbol: &str, files_done: usize, total_files: usize, source_events: u64) {
    let bar = progress_bar(files_done, total_files);
    info!(
        "Tardis replay progress: symbol={} [{}] files={}/{} events={}",
        symbol, bar, files_done, total_files, source_events
    );
}

fn log_stage_progress(
    stream: &str,
    symbol: &str,
    files_done: usize,
    total_files: usize,
    source_events: u64,
    rows: u64,
    elapsed: Duration,
) {
    let bar = progress_bar(files_done, total_files);
    info!(
        "Tardis staging progress: stream={} symbol={} [{}] files={}/{} source_events={} rows={} elapsed={:.2?}",
        stream,
        symbol,
        bar,
        files_done,
        total_files,
        source_events,
        rows,
        elapsed,
    );
}

fn progress_bar(files_done: usize, total_files: usize) -> String {
    let filled = if total_files == 0 {
        0
    } else {
        (files_done * PROGRESS_BAR_WIDTH / total_files).min(PROGRESS_BAR_WIDTH)
    };
    format!(
        "{}{}",
        "#".repeat(filled),
        "-".repeat(PROGRESS_BAR_WIDTH - filled)
    )
}

fn enqueue_baseline_bars(
    bars: &[BaselineBar],
    symbol: &str,
    venue: u8,
    writer: &Sender<Bytes>,
) -> Result<()> {
    for bar in bars {
        let payload = bar.to_trade_flow_feature_payload(symbol, venue)?;
        writer
            .send(payload)
            .map_err(|_| anyhow!("baseline ClickHouse writer stopped"))?;
    }
    Ok(())
}

fn baseline_table_name(venue_slug: &str, bar_ms: i64) -> String {
    let venue = venue_slug.replace('-', "_");
    format!("baseline_{venue}_{}s", bar_ms / 1_000)
}

fn baseline_trade_table_name(venue_slug: &str, bar_ms: i64) -> String {
    format!("{}_trade", baseline_table_name(venue_slug, bar_ms))
}

fn baseline_depth_table_name(venue_slug: &str, bar_ms: i64) -> String {
    format!("{}_depth", baseline_table_name(venue_slug, bar_ms))
}

fn trade_notional_kll_table_name(venue_slug: &str) -> String {
    format!("trade_notional_kll_{}_hourly", venue_slug.replace('-', "_"))
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

fn ensure_staging_tables(
    url: &str,
    database: &str,
    trade_5s_table: &str,
    depth_5s_table: &str,
    trade_60s_table: &str,
    depth_60s_table: &str,
    kll_table: &str,
) -> Result<()> {
    validate_identifier(depth_5s_table)?;
    validate_identifier(depth_60s_table)?;
    ensure_trade_staging_tables(url, database, trade_5s_table, trade_60s_table, kll_table)?;
    let client = clickhouse_http_client()?;
    clickhouse_execute(&client, url, &format!(
        "CREATE TABLE IF NOT EXISTS {database}.{depth_5s_table} ({}) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (symbol, ts)",
        baseline_depth_columns_sql()
    ))?;
    clickhouse_execute(&client, url, &format!(
        "CREATE TABLE IF NOT EXISTS {database}.{depth_60s_table} ({}) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (symbol, ts)",
        baseline_depth_columns_sql()
    ))
}

fn ensure_trade_staging_tables(
    url: &str,
    database: &str,
    trade_5s_table: &str,
    trade_60s_table: &str,
    kll_table: &str,
) -> Result<()> {
    validate_identifier(trade_5s_table)?;
    validate_identifier(trade_60s_table)?;
    ensure_hourly_kll_table(url, database, kll_table)?;
    let client = clickhouse_http_client()?;
    clickhouse_execute(&client, url, &format!(
        "CREATE TABLE IF NOT EXISTS {database}.{trade_5s_table} ({}) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (symbol, ts)",
        baseline_trade_columns_sql()
    ))?;
    clickhouse_execute(&client, url, &format!(
        "CREATE TABLE IF NOT EXISTS {database}.{trade_60s_table} ({}) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (symbol, ts)",
        baseline_trade_columns_sql()
    ))
}

fn delete_hourly_kll_range(
    url: &str,
    database: &str,
    table: &str,
    symbols: &[String],
    start_ms: i64,
    end_ms: i64,
) -> Result<()> {
    validate_identifier(database)?;
    validate_identifier(table)?;
    let symbol_list = clickhouse_symbol_list(symbols)?;
    let client = clickhouse_http_client()?;
    clickhouse_execute_sync_mutation(
        &client,
        url,
        &format!(
            "ALTER TABLE {database}.{table} DELETE WHERE symbol IN ({symbol_list}) AND hour_start >= fromUnixTimestamp64Milli({start_ms}) AND hour_start < fromUnixTimestamp64Milli({end_ms})"
        ),
    )
}

fn delete_staging_range(
    url: &str,
    database: &str,
    tables: &[&str],
    kll_table: Option<&str>,
    symbols: &[String],
    start_ms: i64,
    end_ms: i64,
) -> Result<()> {
    delete_baseline_ranges(url, database, tables, symbols, start_ms, end_ms)?;
    let Some(kll_table) = kll_table else {
        return Ok(());
    };
    validate_identifier(kll_table)?;
    delete_hourly_kll_range(url, database, kll_table, symbols, start_ms, end_ms)
}

fn delete_baseline_ranges(
    url: &str,
    database: &str,
    tables: &[&str],
    symbols: &[String],
    start_ms: i64,
    end_ms: i64,
) -> Result<()> {
    validate_identifier(database)?;
    let symbol_list = clickhouse_symbol_list(symbols)?;
    let client = clickhouse_http_client()?;
    for table in tables {
        validate_identifier(table)?;
        clickhouse_execute_sync_mutation(
            &client,
            url,
            &format!(
                "ALTER TABLE {database}.{table} DELETE WHERE symbol IN ({symbol_list}) AND ts >= fromUnixTimestamp64Milli({start_ms}) AND ts < fromUnixTimestamp64Milli({end_ms})"
            ),
        )?;
    }
    Ok(())
}

fn clickhouse_symbol_list(symbols: &[String]) -> Result<String> {
    if symbols.is_empty() {
        bail!("at least one symbol is required for range replacement");
    }
    for symbol in symbols {
        validate_identifier(symbol)?;
    }
    Ok(symbols
        .iter()
        .map(|symbol| format!("'{symbol}'"))
        .collect::<Vec<_>>()
        .join(", "))
}

fn baseline_trade_columns_sql() -> String {
    let mut columns = vec![
        "ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD)".to_string(),
        "symbol String".to_string(),
    ];
    columns.extend(
        TRADE_FLOW_FEATURE_FIELD_NAMES
            .iter()
            .map(|name| format!("{name} Float64")),
    );
    columns.join(", ")
}

fn baseline_depth_columns_sql() -> String {
    let mut columns = vec![
        "ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD)".to_string(),
        "symbol String".to_string(),
    ];
    for side in ["bid", "ask"] {
        for level in 0..20 {
            columns.push(format!("{side}_{level:02}_price Float64"));
            columns.push(format!("{side}_{level:02}_amount Float64"));
        }
    }
    columns.join(", ")
}

fn ensure_baseline_tables(url: &str, database: &str, table: &str) -> Result<()> {
    validate_identifier(database)?;
    validate_identifier(table)?;
    let client = clickhouse_http_client()?;
    clickhouse_execute(
        &client,
        url,
        &format!("CREATE DATABASE IF NOT EXISTS {database}"),
    )?;
    let query = format!(
        "CREATE TABLE IF NOT EXISTS {database}.{table} ({}) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (symbol, ts)",
        baseline_table_columns_sql()
    );
    clickhouse_execute(&client, url, &query)
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

fn baseline_table_columns_sql() -> String {
    let mut columns = vec![
        "ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD)".to_string(),
        "symbol String".to_string(),
    ];
    columns.extend(
        TRADE_FLOW_FEATURE_FIELD_NAMES
            .iter()
            .map(|name| format!("{name} Float64")),
    );
    for side in ["bid", "ask"] {
        for level in 0..20 {
            columns.push(format!("{side}_{level:02}_price Float64"));
            columns.push(format!("{side}_{level:02}_amount Float64"));
        }
    }
    columns.join(", ")
}

fn clickhouse_http_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(1)
        .timeout(Duration::from_secs(30))
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

fn clickhouse_select(
    client: &reqwest::blocking::Client,
    url: &str,
    query: &str,
) -> Result<String> {
    let response = client
        .post(url.trim_end_matches('/'))
        .query(&[("query", query)])
        .body(Vec::new())
        .send()
        .with_context(|| format!("ClickHouse select failed: {query}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        bail!("ClickHouse select failed: status={status} body={body}");
    }
    response
        .text()
        .context("read ClickHouse select response")
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

fn run_clickhouse_writer(
    receiver: Receiver<Bytes>,
    config: ClickHouseWriterConfig,
) -> Result<ClickHouseWriterStats> {
    let client = clickhouse_http_client()?;
    let mut batch = Vec::with_capacity(config.batch_rows);
    let mut stats = ClickHouseWriterStats::default();
    loop {
        match receiver.recv_timeout(config.flush_interval) {
            Ok(payload) => {
                batch.push(payload);
                if batch.len() >= config.batch_rows {
                    flush_clickhouse_batch(&client, &config, &mut batch, &mut stats)?;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                flush_clickhouse_batch(&client, &config, &mut batch, &mut stats)?;
            }
            Err(RecvTimeoutError::Disconnected) => {
                flush_clickhouse_batch(&client, &config, &mut batch, &mut stats)?;
                return Ok(stats);
            }
        }
    }
}

fn run_stage_clickhouse_writer(
    receiver: Receiver<Bytes>,
    config: ClickHouseWriterConfig,
) -> Result<ClickHouseWriterStats> {
    let client = clickhouse_http_client()?;
    let mut batch = Vec::with_capacity(config.batch_rows);
    let mut stats = ClickHouseWriterStats::default();
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

fn flush_clickhouse_batch(
    client: &reqwest::blocking::Client,
    config: &ClickHouseWriterConfig,
    batch: &mut Vec<Bytes>,
    stats: &mut ClickHouseWriterStats,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut body = Vec::with_capacity(batch.len() * 1_000);
    for payload in batch.iter() {
        let message = TradeFlowFeatureMsg::from_bytes(payload)
            .context("decode standard baseline TradeFlowFeatureMsg")?;
        append_clickhouse_row_binary(&mut body, &message)?;
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

fn append_clickhouse_row_binary(output: &mut Vec<u8>, message: &TradeFlowFeatureMsg) -> Result<()> {
    if message.values.len() != BASELINE_VALUE_COUNT {
        bail!(
            "baseline message value count={} expected={}",
            message.values.len(),
            BASELINE_VALUE_COUNT
        );
    }
    output.extend_from_slice(&message.ts.to_le_bytes());
    append_var_uint(output, message.symbol.len() as u64);
    output.extend_from_slice(message.symbol.as_bytes());
    for value in &message.values {
        output.extend_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

fn append_var_uint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config)
        .with_context(|| format!("read replay config {}", args.config.display()))?;
    let config: ReplayConfig = toml::from_str(&content)
        .with_context(|| format!("parse replay config {}", args.config.display()))?;
    replay(&config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tardis_trade_into_ipc_side() {
        let record = StringRecord::from(vec![
            "binance-futures",
            "BTCUSDT",
            "1576281603397000",
            "1576281603537469",
            "20654725",
            "sell",
            "7252.74",
            "1.367",
        ]);
        let row = parse_trade_record(&record).unwrap();
        assert_eq!(row.side, 'S');
        assert_eq!(row.timestamp_us, 1_576_281_603_397_000);
        assert_eq!(row.id, 20_654_725);
    }

    #[test]
    fn allows_zero_amount_for_incremental_deletion() {
        let record = StringRecord::from(vec![
            "binance-futures",
            "BTCUSDT",
            "1575849602606000",
            "1575849602993699",
            "false",
            "bid",
            "7508.19",
            "0",
        ]);
        let row = parse_book_record(&record).unwrap();
        assert!(row.is_bid);
        assert_eq!(row.amount, 0.0);
    }

    #[test]
    fn daily_depth_reconstruction_ignores_snapshot_marker() {
        let mut agg = DepthAggregator::new(0, 10_000);
        let first = BookEvent {
            timestamp_us: 100,
            is_snapshot: false,
            symbol: "BTCUSDT".to_string(),
            bids: vec![Level::from_values(100.0, 1.0)],
            asks: vec![Level::from_values(101.0, 2.0)],
        };
        assert!(agg.on_book(&first).expect("first update").is_empty());
        let second = BookEvent {
            timestamp_us: 5_000_100,
            is_snapshot: true,
            symbol: "BTCUSDT".to_string(),
            bids: vec![],
            asks: vec![],
        };
        let bars = agg.on_book(&second).expect("second update");
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].0, 0);
        assert_eq!(bars[0].1[0], 100.0);
        assert_eq!(bars[0].1[1], 1.0);
        assert_eq!(bars[0].1[40], 101.0);
        assert_eq!(bars[0].1[41], 2.0);
    }

    #[test]
    fn depth_reconstruction_carries_book_across_utc_day_boundary() {
        let mut agg = DepthAggregator::new(86_395_000, 86_405_000);
        let before_midnight = BookEvent {
            timestamp_us: 86_395_000_100,
            is_snapshot: false,
            symbol: "BTCUSDT".to_string(),
            bids: vec![Level::from_values(100.0, 1.0)],
            asks: vec![Level::from_values(101.0, 2.0)],
        };
        assert!(agg
            .on_book(&before_midnight)
            .expect("seed pre-midnight book")
            .is_empty());

        let after_midnight = BookEvent {
            timestamp_us: 86_400_000_100,
            is_snapshot: false,
            symbol: "BTCUSDT".to_string(),
            bids: vec![Level::from_values(100.5, 3.0)],
            asks: vec![],
        };
        let previous_day = agg.on_book(&after_midnight).expect("cross midnight");
        assert_eq!(previous_day[0].1[0], 100.0);
        assert_eq!(previous_day[0].1[40], 101.0);

        let current_day = agg.finish().expect("finish current day");
        assert_eq!(current_day[0].0, 86_400_000);
        assert_eq!(current_day[0].1[0], 100.5);
        assert_eq!(current_day[0].1[40], 101.0);
    }

    #[test]
    fn depth_reconstruction_rejects_bar_before_book_initialization() {
        let mut agg = DepthAggregator::new(0, 10_000);
        let late_first_update = BookEvent {
            timestamp_us: 5_000_100,
            is_snapshot: false,
            symbol: "BTCUSDT".to_string(),
            bids: vec![Level::from_values(100.0, 1.0)],
            asks: vec![Level::from_values(101.0, 2.0)],
        };
        let error = agg
            .on_book(&late_first_update)
            .expect_err("empty book must not produce an all-zero bar");
        assert!(error.to_string().contains("ts_ms=0"));
    }

    #[test]
    fn chunks_large_book_and_marks_only_final_chunk_as_last() {
        let event = BookEvent {
            timestamp_us: 123,
            is_snapshot: true,
            symbol: "BTCUSDT".to_string(),
            bids: (0..150)
                .map(|i| Level::from_values(100.0 - i as f64, 1.0))
                .collect(),
            asks: (0..75)
                .map(|i| Level::from_values(101.0 + i as f64, 1.0))
                .collect(),
        };
        let chunks = build_incremental_chunks(&event, 42).unwrap();
        assert_eq!(chunks.len(), 3);
        assert!(!chunks[0].is_last());
        assert!(!chunks[1].is_last());
        assert!(chunks[2].is_last());
        assert_eq!(
            chunks.iter().map(|chunk| chunk.levels.len()).sum::<usize>(),
            225
        );
        assert!(chunks
            .iter()
            .all(|chunk| chunk.levels.len() <= MAX_LEVELS_PER_INCREMENTAL_CHUNK));
        assert!(chunks.iter().all(|chunk| chunk.final_update_id == 42));
    }

    #[test]
    fn baseline_schema_matches_standard_message_width() {
        let columns = baseline_table_columns_sql();
        assert_eq!(columns.matches(" Float64").count(), BASELINE_VALUE_COUNT);
        assert!(columns.starts_with("ts DateTime64(3, 'UTC')"));
        assert!(columns.contains(", symbol String,"));
        assert_eq!(
            baseline_table_name("binance-futures", 5_000),
            "baseline_binance_futures_5s"
        );
    }

    #[test]
    fn row_binary_preserves_standard_message_order() {
        let values: Vec<f64> = (0..BASELINE_VALUE_COUNT)
            .map(|index| index as f64)
            .collect();
        let message = TradeFlowFeatureMsg::from_indexed_values(
            "BTCUSDT".to_string(),
            TradingVenue::BinanceFutures.to_u8(),
            1_700_000_000_005,
            &values,
        )
        .expect("message");
        let mut row = Vec::new();
        append_clickhouse_row_binary(&mut row, &message).expect("row binary");

        assert_eq!(i64::from_le_bytes(row[..8].try_into().unwrap()), message.ts);
        assert_eq!(row[8], message.symbol.len() as u8);
        assert_eq!(&row[9..16], b"BTCUSDT");
        assert_eq!(f64::from_le_bytes(row[16..24].try_into().unwrap()), 0.0);
        let last_offset = 16 + (BASELINE_VALUE_COUNT - 1) * 8;
        assert_eq!(
            f64::from_le_bytes(row[last_offset..last_offset + 8].try_into().unwrap()),
            (BASELINE_VALUE_COUNT - 1) as f64
        );
    }

    #[test]
    fn normalizes_and_deduplicates_parallel_symbols() {
        let symbols = normalize_replay_symbols(&[
            "btcusdt".to_string(),
            " ETHUSDT ".to_string(),
            "BTCUSDT".to_string(),
        ])
        .expect("symbols");
        assert_eq!(symbols, ["BTCUSDT", "ETHUSDT"]);
    }

    #[test]
    fn builds_safe_clickhouse_symbol_list() {
        assert_eq!(
            clickhouse_symbol_list(&["BTCUSDT".to_string(), "ETHUSDT".to_string()])
                .expect("symbol list"),
            "'BTCUSDT', 'ETHUSDT'"
        );
        assert!(clickhouse_symbol_list(&[]).is_err());
    }

    #[test]
    fn scopes_replay_ipc_services_by_symbol_then_venue() {
        assert_eq!(
            replay_ipc_service_name("BTCUSDT", "binance-futures", "trade"),
            "dat_pbs/BTCUSDT/binance-futures/trade"
        );
        assert_eq!(
            replay_ipc_service_name("ETHUSDT", "binance-futures", "incremental"),
            "dat_pbs/ETHUSDT/binance-futures/incremental"
        );
    }

    #[test]
    fn order_size_replay_builds_target_month_sequence() {
        let mut config: ReplayConfig =
            toml::from_str(include_str!("../../config/tardis_replay.toml")).unwrap();
        config.order_size.enabled = true;
        config.start_date = Some("2025-12-05".to_string());
        config.end_date = Some("2026-03-20".to_string());
        assert_eq!(
            order_size_target_months(&config).unwrap(),
            ["2025-12", "2026-01", "2026-02", "2026-03"]
        );
    }

    #[test]
    fn order_size_replay_validates_quantile_order() {
        let mut config: ReplayConfig =
            toml::from_str(include_str!("../../config/tardis_replay.toml")).unwrap();
        config.order_size.enabled = true;
        config.order_size.medium_quantile = 0.95;
        config.order_size.large_quantile = 0.9;
        assert!(order_size_target_months(&config).is_err());
    }

    #[test]
    fn switches_order_size_threshold_at_utc_month_boundary() {
        let (jan_start_ms, feb_start_ms) = utc_month_bounds("2026-01").unwrap();
        let (_, mar_start_ms) = utc_month_bounds("2026-02").unwrap();
        let thresholds = [
            MonthlyAmountThreshold {
                start_us: jan_start_ms * 1_000,
                end_us: feb_start_ms * 1_000,
                threshold: AmountThreshold {
                    medium_notional_threshold: 10.0,
                    large_notional_threshold: 20.0,
                },
            },
            MonthlyAmountThreshold {
                start_us: feb_start_ms * 1_000,
                end_us: mar_start_ms * 1_000,
                threshold: AmountThreshold {
                    medium_notional_threshold: 30.0,
                    large_notional_threshold: 40.0,
                },
            },
        ];
        let mut index = 0;
        assert_eq!(
            threshold_for_timestamp(&thresholds, &mut index, feb_start_ms * 1_000 - 1)
                .unwrap()
                .medium_notional_threshold,
            10.0
        );
        assert_eq!(
            threshold_for_timestamp(&thresholds, &mut index, feb_start_ms * 1_000)
                .unwrap()
                .medium_notional_threshold,
            30.0
        );
        assert!(threshold_for_timestamp(&thresholds, &mut index, mar_start_ms * 1_000).is_err());
    }

    #[test]
    fn kll_only_requires_kll_output_and_disables_order_size() {
        let mut config: ReplayConfig =
            toml::from_str(include_str!("../../config/tardis_replay.toml")).unwrap();
        config.kll_only = true;
        config.write_hourly_notional_kll = false;
        assert!(validate_replay_mode(&config).is_err());

        config.write_hourly_notional_kll = true;
        config.order_size.enabled = true;
        assert!(validate_replay_mode(&config).is_err());

        config.order_size.enabled = false;
        assert!(validate_replay_mode(&config).is_ok());
    }

    #[test]
    fn order_size_only_preserves_kll_and_requires_overwrite() {
        let mut config: ReplayConfig =
            toml::from_str(include_str!("../../config/tardis_replay.toml")).unwrap();
        config.order_size_only = true;
        config.order_size.enabled = true;
        config.write_hourly_notional_kll = false;
        config.overwrite_existing = true;
        assert!(validate_replay_mode(&config).is_ok());

        config.write_hourly_notional_kll = true;
        assert!(validate_replay_mode(&config).is_err());
        config.write_hourly_notional_kll = false;
        config.overwrite_existing = false;
        assert!(validate_replay_mode(&config).is_err());
    }

    #[test]
    fn depth_only_preserves_trade_and_kll_and_requires_overwrite() {
        let mut config: ReplayConfig =
            toml::from_str(include_str!("../../config/tardis_replay.toml")).unwrap();
        config.depth_only = true;
        config.write_hourly_notional_kll = false;
        config.order_size.enabled = false;
        config.overwrite_existing = true;
        assert!(validate_replay_mode(&config).is_ok());

        config.order_size.enabled = true;
        assert!(validate_replay_mode(&config).is_err());
        config.order_size.enabled = false;
        config.overwrite_existing = false;
        assert!(validate_replay_mode(&config).is_err());
        config.overwrite_existing = true;
        config.order_size_only = true;
        assert!(validate_replay_mode(&config).is_err());
    }

    #[test]
    fn market_1s_only_disables_kll_order_size_and_other_modes() {
        let mut config: ReplayConfig =
            toml::from_str(include_str!("../../config/tardis_replay.toml")).unwrap();
        config.market_1s_only = true;
        config.write_hourly_notional_kll = false;
        config.order_size.enabled = false;
        config.publish_ipc = false;
        assert!(validate_replay_mode(&config).is_ok());

        config.write_hourly_notional_kll = true;
        assert!(validate_replay_mode(&config).is_err());
        config.write_hourly_notional_kll = false;
        config.order_size.enabled = true;
        assert!(validate_replay_mode(&config).is_err());
        config.order_size.enabled = false;
        config.kll_only = true;
        assert!(validate_replay_mode(&config).is_err());
        config.kll_only = false;
        config.market_1s.write_clickhouse = false;
        assert!(validate_replay_mode(&config).is_err());
    }

    #[test]
    fn default_replay_can_write_market_1s_with_baseline_and_kll() {
        let mut config: ReplayConfig =
            toml::from_str(include_str!("../../config/tardis_replay.toml")).unwrap();
        assert!(config.market_1s.enabled);
        assert!(config.write_hourly_notional_kll);
        assert!(!config.market_1s_only);
        assert!(validate_replay_mode(&config).is_ok());

        config.order_size.enabled = true;
        assert!(validate_replay_mode(&config).is_ok());
    }

    #[test]
    fn accepts_date_named_trade_files() {
        let root =
            std::env::temp_dir().join(format!("tardis-market-1s-discover-{}", std::process::id()));
        let trades = root.join("trades");
        fs::create_dir_all(&trades).unwrap();
        let path = trades.join("2026-01-02.csv.gz");
        fs::write(&path, []).unwrap();
        let found = discover_files(
            &root,
            "trades",
            "binance-futures",
            "BNBUSDT",
            Some("2026-01-01"),
            Some("2026-01-02"),
        )
        .unwrap();
        assert_eq!(found, [path.clone()]);
        assert_eq!(
            trade_file_date(&path).unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn replay_config_template_parses() {
        let config: ReplayConfig = toml::from_str(include_str!("../../config/tardis_replay.toml"))
            .expect("replay config template");
        assert_eq!(config.venue, "binance-futures");
        assert_eq!(
            config.symbols,
            ["XRPUSDT", "DOGEUSDT", "SOLUSDT", "ETHUSDT", "BTCUSDT", "BNBUSDT"]
        );
        assert_eq!(config.start_date.as_deref(), Some("2024-12-01"));
        assert_eq!(config.end_date.as_deref(), Some("2024-12-31"));
        assert!(!config.publish_ipc);
        assert!(!config.kll_only);
        assert!(!config.order_size_only);
        assert!(!config.depth_only);
        assert!(!config.market_1s_only);
        assert!(config.overwrite_existing);
        assert!(config.market_1s.enabled);
        assert_eq!(
            config.market_1s.output_dir,
            PathBuf::from(
                "/mnt/hdd-raid5-72t/liang_torch/crypto_data/cta_backtest_data/binanceswap/tardis_agg_1s_daily"
            )
        );
        assert_eq!(config.market_1s.file_sids, "1_6");
        assert!(config.market_1s.write_clickhouse);
        assert!(config.market_1s.write_hdf);
        assert!(!config.order_size.enabled);
        assert_eq!(config.order_size.medium_quantile, 0.5);
        assert_eq!(config.order_size.large_quantile, 0.9);
        assert_eq!(
            replay_data_dir(&config, "SOLUSDT").expect("SOL data directory"),
            Path::new("/mnt/30.133_xintang/Data/Crypto/TardisSource/binance_usd_solusdt")
        );
        assert_eq!(config.replay_workers, 6);
        assert_eq!(config.clickhouse.database, "baseline");
    }
}
