//! Replay Tardis trades and incremental L2 files through the live market-data IPC contract.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
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
use mkt_signal::factor_pub::trade_flow_feature_pub::local_baseline::{
    BaselineBar, HourlyNotionalKll, HourlyNotionalKllSnapshot, LocalBaselineAggregator,
};
use order_common::TradingVenue;
use rayon::prelude::*;
use serde::Deserialize;
use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

const TRADE_PAYLOAD_BYTES: usize = 128;
const INCREMENTAL_PAYLOAD_BYTES: usize = 2048;
const MAX_LEVELS_PER_INCREMENTAL_CHUNK: usize = 100;
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
    /// Root containing Tardis trades/ and incremental_book_L2/ directories.
    data_dir: PathBuf,
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
    publish_hourly_notional_kll: bool,
    #[serde(default)]
    clickhouse: ClickHouseConfig,
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

fn default_replay_workers() -> usize {
    1
}

fn default_publish_ipc() -> bool {
    true
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
            if row.timestamp_us != timestamp_us || row.is_snapshot != is_snapshot {
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
    let prefix = format!("{venue_slug}_{dataset}_");
    let suffix = format!("_{symbol}.csv.gz");
    let mut paths = fs::read_dir(&dir)
        .with_context(|| format!("failed to read Tardis dataset directory {}", dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return false;
            };
            if !name.starts_with(&prefix) || !name.ends_with(&suffix) {
                return false;
            }
            let date = name
                .strip_prefix(&prefix)
                .and_then(|value| value.strip_suffix(&suffix));
            date.is_some_and(|date| {
                start_date.is_none_or(|start| date >= start)
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

fn replay(config: &ReplayConfig) -> Result<()> {
    validate_dates(config)?;
    if config.replay_workers == 0 {
        bail!("replay_workers must be > 0");
    }
    let venue = TradingVenue::from_str(&config.venue, true)
        .map_err(|err| anyhow!("unsupported replay venue '{}': {err}", config.venue))?;
    let venue_slug = venue.data_pub_slug();
    let symbols = replay_symbols(config)?;
    let replay_pool = (symbols.len() > 1)
        .then(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(config.replay_workers.min(symbols.len()))
                .build()
                .context("build Tardis replay Rayon pool")
        })
        .transpose()?;
    let mut clickhouse_writer = Some(BaselineClickHouseWriter::start(
        ClickHouseWriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: baseline_table_name(venue_slug, 5_000),
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        },
        config.clickhouse.queue_capacity,
    )?);
    let clickhouse_sender = clickhouse_writer
        .as_ref()
        .map(BaselineClickHouseWriter::sender);

    let replay_result = if symbols.len() == 1 {
        replay_symbol(
            config,
            venue,
            venue_slug,
            &symbols[0],
            clickhouse_sender.as_ref(),
        )
    } else {
        let workers = config.replay_workers.min(symbols.len());
        info!(
            "Starting parallel Tardis IPC replay: venue={} symbols={} workers={}",
            venue_slug,
            symbols.len(),
            workers
        );
        replay_pool
            .as_ref()
            .expect("parallel replay pool created for multiple symbols")
            .install(|| {
                symbols.par_iter().try_for_each(|symbol| {
                    replay_symbol(
                        config,
                        venue,
                        venue_slug,
                        symbol,
                        clickhouse_sender.as_ref(),
                    )
                })
            })
    };

    let writer_result = clickhouse_writer
        .take()
        .map(BaselineClickHouseWriter::finish);
    replay_result?;
    if let Some(stats) = writer_result.transpose()? {
        info!(
            "Tardis baseline ClickHouse writer: inserted_rows={} inserted_batches={}",
            stats.inserted_rows, stats.inserted_batches
        );
    }
    Ok(())
}

fn replay_symbols(config: &ReplayConfig) -> Result<Vec<String>> {
    normalize_replay_symbols(&config.symbols)
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

fn replay_symbol(
    config: &ReplayConfig,
    venue: TradingVenue,
    venue_slug: &str,
    symbol: &str,
    clickhouse_sender: Option<&Sender<Bytes>>,
) -> Result<()> {
    let mut input = ReplayInput::new(
        &config.data_dir,
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
        .publish_hourly_notional_kll
        .then(|| HourlyNotionalKllPublisher::new(symbol, venue_slug))
        .transpose()?;
    let mut baseline = Some(LocalBaselineAggregator::new());
    let mut hourly_kll = config
        .publish_hourly_notional_kll
        .then(HourlyNotionalKll::new);
    let (initial_files_done, total_files) = input.file_progress();

    info!(
        "Starting Tardis replay: root={} venue={} symbol={} ipc={} dates={:?}..{:?} input_files={}",
        config.data_dir.display(),
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
    let filled = if total_files == 0 {
        0
    } else {
        (files_done * PROGRESS_BAR_WIDTH / total_files).min(PROGRESS_BAR_WIDTH)
    };
    let bar = format!(
        "{}{}",
        "#".repeat(filled),
        "-".repeat(PROGRESS_BAR_WIDTH - filled)
    );
    info!(
        "Tardis replay progress: symbol={} [{}] files={}/{} events={}",
        symbol, bar, files_done, total_files, source_events
    );
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
    fn replay_config_template_parses() {
        let config: ReplayConfig = toml::from_str(include_str!("../../config/tardis_replay.toml"))
            .expect("replay config template");
        assert_eq!(config.venue, "binance-futures");
        assert_eq!(config.symbols, ["BTCUSDT", "ETHUSDT"]);
        assert_eq!(config.replay_workers, 2);
        assert_eq!(config.clickhouse.database, "baseline");
    }
}
