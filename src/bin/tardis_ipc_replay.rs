//! Replay Tardis trades and incremental L2 files through the live market-data IPC contract.

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use csv::StringRecord;
use flate2::read::GzDecoder;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use mkt_parsers::msg::mkt_msg::{IncMsg, Level, TradeMsg};
use mkt_parsers::msg::trade_notional_kll_msg::{TradeNotionalKllMsg, TRADE_NOTIONAL_KLL_MAX_BYTES};
use mkt_signal::factor_pub::trade_flow_feature_pub::local_baseline::{
    HourlyNotionalKll, HourlyNotionalKllSnapshot, LocalBaselineAggregator,
};
use order_common::TradingVenue;
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ReplaySource {
    All,
    Trades,
    Incremental,
}

#[derive(Parser, Debug)]
#[command(name = "tardis_ipc_replay")]
#[command(about = "Replay Tardis trades and incremental L2 as dat_pbs IPC market data")]
struct Args {
    /// Tardis symbol root containing trades/ and incremental_book_L2/.
    #[arg(long)]
    data_dir: PathBuf,

    /// Venue that determines dat_pbs/<venue>/trade and incremental service names.
    #[arg(long, value_enum, default_value_t = TradingVenue::BinanceFutures)]
    venue: TradingVenue,

    /// Exchange symbol to replay.
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

    /// First UTC date to include, in YYYY-MM-DD format.
    #[arg(long)]
    start_date: Option<String>,

    /// Last UTC date to include, in YYYY-MM-DD format.
    #[arg(long)]
    end_date: Option<String>,

    /// Which historical stream to publish.
    #[arg(long, value_enum, default_value_t = ReplaySource::All)]
    source: ReplaySource,

    /// Historical time multiplier. Zero publishes as fast as IPC accepts messages.
    #[arg(long, default_value_t = 0.0)]
    speed: f64,

    /// Stop after this many source events. Zero replays all selected files.
    #[arg(long, default_value_t = 0)]
    max_events: u64,

    /// Aggregate local 5s baseline bars and derive 10s/60s bars from them.
    #[arg(long)]
    aggregate_baseline: bool,

    /// Publish one frozen notional KLL snapshot per UTC hour to a dedicated IPC service.
    #[arg(long)]
    publish_hourly_notional_kll: bool,
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
        source: ReplaySource,
    ) -> Result<Self> {
        let wants_trades = matches!(source, ReplaySource::All | ReplaySource::Trades);
        let wants_books = matches!(source, ReplaySource::All | ReplaySource::Incremental);
        let trades = wants_trades
            .then(|| {
                discover_files(data_dir, "trades", venue_slug, symbol, start_date, end_date).map(
                    |paths| TradeEventReader {
                        records: CsvGzipReader::new(paths),
                    },
                )
            })
            .transpose()?;
        let books = wants_books
            .then(|| {
                discover_files(
                    data_dir,
                    "incremental_book_L2",
                    venue_slug,
                    symbol,
                    start_date,
                    end_date,
                )
                .map(|paths| BookEventReader {
                    records: CsvGzipReader::new(paths),
                    symbol: symbol.to_string(),
                    pending: None,
                })
            })
            .transpose()?;

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
}

struct IpcReplayPublisher {
    trade: Option<Publisher<ipc::Service, [u8; TRADE_PAYLOAD_BYTES], ()>>,
    incremental: Option<Publisher<ipc::Service, [u8; INCREMENTAL_PAYLOAD_BYTES], ()>>,
}

struct HourlyNotionalKllPublisher {
    publisher: Publisher<ipc::Service, [u8; TRADE_NOTIONAL_KLL_MAX_BYTES], ()>,
}

impl HourlyNotionalKllPublisher {
    fn new(venue_slug: &str) -> Result<Self> {
        let node_name = format!("tardis_notional_kll_pub_{}", venue_slug.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;
        let service_name = format!("factor_pub/{venue_slug}/trade_notional_kll_hourly");
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
    fn new(venue_slug: &str, source: ReplaySource) -> Result<Self> {
        let node_name = format!("tardis_ipc_replay_{}", venue_slug.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let trade = matches!(source, ReplaySource::All | ReplaySource::Trades)
            .then(|| {
                let service_name = format!("dat_pbs/{venue_slug}/trade");
                node.service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; TRADE_PAYLOAD_BYTES]>()
                    .max_publishers(1)
                    .max_subscribers(MAX_SUBSCRIBERS)
                    .history_size(HISTORY_SIZE)
                    .subscriber_max_buffer_size(8192)
                    .open_or_create()?
                    .publisher_builder()
                    .create()
                    .with_context(|| format!("failed to create trade publisher {service_name}"))
            })
            .transpose()?;
        let incremental = matches!(source, ReplaySource::All | ReplaySource::Incremental)
            .then(|| {
                let service_name = format!("dat_pbs/{venue_slug}/incremental");
                node.service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; INCREMENTAL_PAYLOAD_BYTES]>()
                    .max_publishers(1)
                    .max_subscribers(MAX_SUBSCRIBERS)
                    .history_size(HISTORY_SIZE)
                    .subscriber_max_buffer_size(8192)
                    .open_or_create()?
                    .publisher_builder()
                    .create()
                    .with_context(|| {
                        format!("failed to create incremental publisher {service_name}")
                    })
            })
            .transpose()?;

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

fn validate_dates(args: &Args) -> Result<()> {
    if !args.speed.is_finite() || args.speed < 0.0 {
        bail!("speed must be finite and >= 0");
    }
    for date in [&args.start_date, &args.end_date].into_iter().flatten() {
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
    if let (Some(start), Some(end)) = (&args.start_date, &args.end_date) {
        if start > end {
            bail!("start_date must not be after end_date");
        }
    }
    Ok(())
}

fn replay(args: &Args) -> Result<()> {
    validate_dates(args)?;
    if args.publish_hourly_notional_kll
        && !matches!(args.source, ReplaySource::All | ReplaySource::Trades)
    {
        bail!("--publish-hourly-notional-kll requires a replay source that includes trades");
    }
    let symbol = args.symbol.trim().to_ascii_uppercase();
    if symbol.is_empty() {
        bail!("symbol must not be empty");
    }
    let venue_slug = args.venue.data_pub_slug();
    let mut input = ReplayInput::new(
        &args.data_dir,
        venue_slug,
        &symbol,
        args.start_date.as_deref(),
        args.end_date.as_deref(),
        args.source,
    )?;
    let publisher = IpcReplayPublisher::new(venue_slug, args.source)?;
    let hourly_kll_publisher = args
        .publish_hourly_notional_kll
        .then(|| HourlyNotionalKllPublisher::new(venue_slug))
        .transpose()?;
    let mut baseline = args.aggregate_baseline.then(LocalBaselineAggregator::new);
    let mut hourly_kll = args
        .publish_hourly_notional_kll
        .then(HourlyNotionalKll::new);

    info!(
        "Starting Tardis IPC replay: root={} venue={} symbol={} source={:?} dates={:?}..{:?} speed={}x",
        args.data_dir.display(), venue_slug, symbol, args.source, args.start_date, args.end_date, args.speed
    );

    let mut previous_timestamp_us = None;
    let mut source_events = 0u64;
    let mut trade_messages = 0u64;
    let mut incremental_messages = 0u64;
    let mut hourly_kll_messages = 0u64;
    let mut next_update_id = 1i64;
    while let Some(event) = input.next_event()? {
        if args.max_events != 0 && source_events >= args.max_events {
            break;
        }
        let timestamp_us = event.timestamp_us();
        if let Some(previous) = previous_timestamp_us {
            if timestamp_us < previous {
                warn!(
                    "Tardis source timestamp moved backward: previous={} current={}",
                    previous, timestamp_us
                );
            } else if args.speed > 0.0 {
                let delay_us = (timestamp_us - previous) as f64 / args.speed;
                if delay_us >= 1.0 {
                    thread::sleep(Duration::from_micros(delay_us.min(u64::MAX as f64) as u64));
                }
            }
        }
        previous_timestamp_us = Some(timestamp_us);

        match event {
            ReplayEvent::Trade(row) => {
                publisher.publish_trade(&row)?;
                trade_messages += 1;
                if let Some(baseline) = baseline.as_mut() {
                    baseline.on_trade(row.timestamp_us, row.side == 'B', row.price, row.amount);
                }
                if let Some(kll) = hourly_kll.as_mut() {
                    if let Some(snapshot) = kll.on_trade(row.timestamp_us, row.price, row.amount) {
                        hourly_kll_publisher
                            .as_ref()
                            .expect("KLL publisher is enabled with KLL aggregation")
                            .publish(&row.symbol, args.venue, snapshot)?;
                        hourly_kll_messages += 1;
                    }
                }
            }
            ReplayEvent::Book(event) => {
                incremental_messages += publisher.publish_book(&event, next_update_id)? as u64;
                next_update_id = next_update_id
                    .checked_add(1)
                    .context("synthetic update id overflow")?;
            }
        }
        source_events += 1;
    }

    if let Some(baseline) = baseline.as_mut() {
        baseline.flush();
        for stats in baseline.stats() {
            info!(
                "Tardis local baseline: bar_ms={} closed={} traded={} ffill={} late_trades={}",
                stats.bar_ms,
                stats.closed_bars,
                stats.traded_bars,
                stats.ffill_bars,
                stats.late_trades
            );
        }
    }
    if let Some(kll) = hourly_kll.as_mut() {
        if let Some(snapshot) = kll.flush() {
            hourly_kll_publisher
                .as_ref()
                .expect("KLL publisher is enabled with KLL aggregation")
                .publish(&symbol, args.venue, snapshot)?;
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

fn main() -> Result<()> {
    env_logger::init();
    replay(&Args::parse())
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
}
