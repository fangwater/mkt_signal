//! Shared streaming Tardis CSV readers used by baseline replay and CTA history synthesis.
use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use csv::StringRecord;
use flate2::read::GzDecoder;
use log::warn;
use mkt_parsers::msg::mkt_msg::Level;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::BufReader,
    path::{Path, PathBuf},
};
pub const MAX_LEVELS_PER_BOOK_EVENT: usize = 10_000;
#[derive(Debug, Clone)]
pub struct TradeRow {
    pub timestamp_us: i64,
    pub id: i64,
    pub symbol: String,
    pub side: char,
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Clone)]
pub struct BookLevelRow {
    pub timestamp_us: i64,
    pub is_snapshot: bool,
    pub is_bid: bool,
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Clone)]
pub struct BookEvent {
    pub timestamp_us: i64,
    pub is_snapshot: bool,
    pub symbol: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, Clone)]
pub enum ReplayEvent {
    Trade(TradeRow),
    Book(BookEvent),
}

impl ReplayEvent {
    pub fn timestamp_us(&self) -> i64 {
        match self {
            Self::Trade(row) => row.timestamp_us,
            Self::Book(event) => event.timestamp_us,
        }
    }
}

pub struct CsvGzipReader {
    pub paths: Vec<PathBuf>,
    pub next_path: usize,
    pub current_path: Option<PathBuf>,
    pub reader: Option<csv::Reader<GzDecoder<BufReader<File>>>>,
}

impl CsvGzipReader {
    pub fn new(paths: Vec<PathBuf>) -> Self {
        Self {
            paths,
            next_path: 0,
            current_path: None,
            reader: None,
        }
    }

    pub fn location(&self) -> String {
        let path = self
            .current_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<unknown>".to_string());
        let line = self
            .reader
            .as_ref()
            .map(|reader| reader.position().line())
            .unwrap_or(0);
        format!("{path}:{line}")
    }

    pub fn next_record(&mut self) -> Result<Option<StringRecord>> {
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
                    return Err(err)
                        .with_context(|| format!("failed to read Tardis CSV {}", self.location()));
                }
            }
        }
    }

    pub fn file_progress(&self) -> (usize, usize) {
        let active_file = usize::from(self.reader.is_some());
        (self.next_path.saturating_sub(active_file), self.paths.len())
    }
}

pub struct TradeEventReader {
    pub records: CsvGzipReader,
}

impl TradeEventReader {
    pub fn next_event(&mut self) -> Result<Option<TradeRow>> {
        loop {
            let Some(record) = self.records.next_record()? else {
                return Ok(None);
            };
            match parse_trade_record(&record) {
                Ok(Some(row)) => return Ok(Some(row)),
                Ok(None) => {
                    warn!(
                        "Skipping invalid Tardis trade at {}: price={:?} amount={:?}",
                        self.records.location(),
                        record.get(6),
                        record.get(7)
                    );
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("parse Tardis trade {}", self.records.location())
                    });
                }
            }
        }
    }
}

pub struct BookEventReader {
    pub records: CsvGzipReader,
    pub pending: Option<BookLevelRow>,
    pub symbol: String,
}

impl BookEventReader {
    pub fn next_event(&mut self) -> Result<Option<BookEvent>> {
        let first = match self.pending.take() {
            Some(row) => row,
            None => match self.records.next_record()? {
                Some(record) => parse_book_record(&record)
                    .with_context(|| format!("parse Tardis book {}", self.records.location()))?,
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
            let row = parse_book_record(&record)
                .with_context(|| format!("parse Tardis book {}", self.records.location()))?;
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

pub fn append_book_level(row: &BookLevelRow, bids: &mut Vec<Level>, asks: &mut Vec<Level>) {
    let level = Level::from_values(row.price, row.amount);
    if row.is_bid {
        bids.push(level);
    } else {
        asks.push(level);
    }
}

pub struct ReplayInput {
    pub trades: Option<TradeEventReader>,
    pub books: Option<BookEventReader>,
    pub next_trade: Option<TradeRow>,
    pub next_book: Option<BookEvent>,
}

impl ReplayInput {
    pub fn from_files(
        trade_paths: Vec<PathBuf>,
        book_paths: Vec<PathBuf>,
        symbol: &str,
    ) -> Result<Self> {
        let mut input = Self {
            trades: (!trade_paths.is_empty()).then(|| TradeEventReader {
                records: CsvGzipReader::new(trade_paths),
            }),
            books: (!book_paths.is_empty()).then(|| BookEventReader {
                records: CsvGzipReader::new(book_paths),
                pending: None,
                symbol: symbol.to_string(),
            }),
            next_trade: None,
            next_book: None,
        };
        input.refill()?;
        Ok(input)
    }
    pub fn new(
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

    pub fn refill(&mut self) -> Result<()> {
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

    pub fn next_event(&mut self) -> Result<Option<ReplayEvent>> {
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

    pub fn file_progress(&self) -> (usize, usize) {
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

pub fn discover_files(
    data_dir: &Path,
    dataset: &str,
    venue_slug: &str,
    symbol: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<Vec<PathBuf>> {
    let dir = data_dir.join(dataset);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
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
    Ok(paths)
}

pub fn discovered_file_dates(paths: &[PathBuf]) -> Result<BTreeSet<NaiveDate>> {
    paths.iter().map(|path| trade_file_date(path)).collect()
}

/// Skip missing calendar days before the first available file. After that day,
/// every date through `end` must be present; a hole is an error, not a skip.
pub fn skip_leading_then_require_contiguous(
    available: &BTreeSet<NaiveDate>,
    start: NaiveDate,
    end: NaiveDate,
    label: &str,
) -> Result<Option<NaiveDate>> {
    if start > end {
        bail!("start_date must not be after end_date");
    }
    let mut first = None;
    let mut day = start;
    loop {
        if available.contains(&day) {
            first = Some(day);
            break;
        }
        if day == end {
            break;
        }
        day = day
            .succ_opt()
            .context("replay date exceeds supported range")?;
    }
    let Some(first) = first else {
        return Ok(None);
    };
    if first > start {
        let skipped_end = first
            .pred_opt()
            .context("first available day has no predecessor")?;
        warn!(
            "Skipping missing leading dates for {label}: {start} to {skipped_end} inclusive; first available day is {first}"
        );
    }
    day = first;
    loop {
        if !available.contains(&day) {
            bail!(
                "{label}: missing {day} after data started on {first}; only leading dates before the first available day may be skipped"
            );
        }
        if day == end {
            break;
        }
        day = day
            .succ_opt()
            .context("replay date exceeds supported range")?;
    }
    Ok(Some(first))
}

pub fn require_discovered_coverage(
    paths: &[PathBuf],
    start: NaiveDate,
    end: NaiveDate,
    label: &str,
) -> Result<Option<NaiveDate>> {
    let available = discovered_file_dates(paths)?;
    skip_leading_then_require_contiguous(&available, start, end, label)
}

pub fn inclusive_dates(start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>> {
    if start > end {
        bail!("start_date must not be after end_date");
    }
    let mut days = Vec::new();
    let mut day = start;
    loop {
        days.push(day);
        if day == end {
            break;
        }
        day = day
            .succ_opt()
            .context("replay date exceeds supported range")?;
    }
    Ok(days)
}

pub fn files_by_day(paths: &[PathBuf]) -> Result<BTreeMap<NaiveDate, PathBuf>> {
    let mut by_day = BTreeMap::new();
    for path in paths {
        by_day.insert(trade_file_date(path)?, path.clone());
    }
    Ok(by_day)
}

pub fn require_paired_day_coverage(
    trade_files: &[PathBuf],
    depth_files: &[PathBuf],
    start: NaiveDate,
    end: NaiveDate,
    label: &str,
) -> Result<
    Option<(
        NaiveDate,
        BTreeMap<NaiveDate, PathBuf>,
        BTreeMap<NaiveDate, PathBuf>,
    )>,
> {
    let trades = files_by_day(trade_files)?;
    let depth = files_by_day(depth_files)?;
    let first_trade =
        skip_leading_then_require_contiguous(&trades.keys().copied().collect(), start, end, label)?;
    let first_depth =
        skip_leading_then_require_contiguous(&depth.keys().copied().collect(), start, end, label)?;
    match (first_trade, first_depth) {
        (Some(first_trade), Some(first_depth)) => {
            let first = first_trade.max(first_depth);
            skip_leading_then_require_contiguous(
                &trades
                    .keys()
                    .filter(|day| depth.contains_key(day))
                    .copied()
                    .collect(),
                first,
                end,
                label,
            )?;
            Ok(Some((first, trades, depth)))
        }
        _ => Ok(None),
    }
}

pub fn trade_file_date(path: &Path) -> Result<NaiveDate> {
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

pub fn parse_trade_record(record: &StringRecord) -> Result<Option<TradeRow>> {
    let timestamp_us = parse_i64(record, 2, "timestamp")?;
    let id = parse_i64(record, 4, "id")?;
    let side = parse_side(field(record, 5, "side")?)?;
    let Some(price) = parse_trade_price_or_amount(record, 6, "price")? else {
        return Ok(None);
    };
    let Some(amount) = parse_trade_price_or_amount(record, 7, "amount")? else {
        return Ok(None);
    };
    Ok(Some(TradeRow {
        timestamp_us,
        id,
        symbol: field(record, 1, "symbol")?.to_ascii_uppercase(),
        side,
        price,
        amount,
    }))
}

pub fn parse_book_record(record: &StringRecord) -> Result<BookLevelRow> {
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

pub fn field<'a>(record: &'a StringRecord, index: usize, name: &str) -> Result<&'a str> {
    record
        .get(index)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("missing Tardis CSV field {name}"))
}

pub fn parse_i64(record: &StringRecord, index: usize, name: &str) -> Result<i64> {
    let value = field(record, index, name)?
        .parse::<i64>()
        .with_context(|| format!("invalid Tardis {name}"))?;
    if value <= 0 {
        bail!("Tardis {name} must be positive, got {value}");
    }
    Ok(value)
}

pub fn parse_price_or_amount(
    record: &StringRecord,
    index: usize,
    name: &str,
    allow_zero: bool,
) -> Result<f64> {
    let raw = field(record, index, name)?;
    let value = raw
        .parse::<f64>()
        .with_context(|| format!("invalid Tardis {name} '{raw}'"))?;
    if !value.is_finite() || value < 0.0 || (!allow_zero && value == 0.0) {
        let expected = if allow_zero {
            "non-negative"
        } else {
            "positive"
        };
        bail!("Tardis {name} must be finite and {expected}, got '{raw}'");
    }
    Ok(value)
}

/// Trades with non-finite or non-positive price/amount are dropped, not fatal.
/// Book deletions still use [`parse_price_or_amount`] with `allow_zero=true`.
pub fn parse_trade_price_or_amount(
    record: &StringRecord,
    index: usize,
    name: &str,
) -> Result<Option<f64>> {
    let raw = field(record, index, name)?;
    let value = raw
        .parse::<f64>()
        .with_context(|| format!("invalid Tardis {name} '{raw}'"))?;
    if !value.is_finite() || value <= 0.0 {
        return Ok(None);
    }
    Ok(Some(value))
}

pub fn parse_side(value: &str) -> Result<char> {
    match value {
        "buy" | "Buy" | "BUY" => Ok('B'),
        "sell" | "Sell" | "SELL" => Ok('S'),
        _ => bail!("unsupported Tardis trade side '{value}'"),
    }
}
