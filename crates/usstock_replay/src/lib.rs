//! Replay the LSEG US equities Time and Sales export into a dedicated RocksDB.
//!
//! This intentionally does not reuse CME contract routing or integer-volume
//! codecs.  Equity TAS includes fractional-share prints and quote-side clears
//! encoded as `0,0`; both are material source semantics.

pub mod bin_msg;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use csv::StringRecord;
use flate2::read::MultiGzDecoder;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

const RIC_LEN: usize = 32;
const KEY_LEN: usize = 8 + 2 + 8;
const MAGIC: [u8; 2] = *b"US";
const RAW_ROW_MAGIC: [u8; 2] = *b"UR";
const SCHEMA_MAGIC: [u8; 2] = *b"UH";
const KIND_TRADE: u8 = 1;
const KIND_QUOTE: u8 = 2;
const KIND_CORRECTION: u8 = 3;
const KIND_STATUS: u8 = 4;
const PERIOD_PREFIX: &str = "period:";
const STATUS_WRITING: &[u8] = b"writing";
const STATUS_DONE: &[u8] = b"done";
const BATCH_ROWS: usize = 50_000;

pub const CF_REPLAY_META: &str = "replay_meta";
pub const CF_SOURCE_SCHEMA: &str = "source_schema";
pub const CF_SOURCE_UNROUTED: &str = "source_unrouted";
pub const US_STOCK_CF_PREFIX: &str = "us_stock:";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayConfig {
    pub data_root: PathBuf,
    pub period: String,
    #[serde(default)]
    pub periods: Vec<String>,
    pub rocksdb_dir: PathBuf,
    #[serde(default)]
    pub rics: Vec<String>,
    #[serde(default = "default_workers")]
    pub workers: usize,
    #[serde(default = "default_progress_every")]
    pub progress_every: u64,
    #[serde(default)]
    pub max_source_rows: Option<u64>,
    #[serde(default = "default_log_path")]
    pub log_path: PathBuf,
    #[serde(default = "default_unparsed_path")]
    pub unparsed_path: PathBuf,
}

fn default_workers() -> usize {
    1
}

fn default_progress_every() -> u64 {
    1_000_000
}

fn default_log_path() -> PathBuf {
    PathBuf::from("/tmp/usstock_replay.log")
}

fn default_unparsed_path() -> PathBuf {
    PathBuf::from("/tmp/usstock_replay_unparsed.log")
}

pub fn load_config(path: &Path) -> Result<ReplayConfig> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parse {}", path.display()))
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Census {
    pub source_rows: u64,
    pub written_rows: u64,
    pub written_unrouted: u64,
    pub written_trades: u64,
    pub written_quotes: u64,
    pub written_corrections: u64,
    pub written_statuses: u64,
    pub skipped_ric_filter: u64,
    pub skipped_unparsed: u64,
    pub counted: BTreeMap<String, u64>,
}

impl Census {
    fn count(&mut self, name: &str) {
        *self.counted.entry(name.to_string()).or_default() += 1;
    }

    fn merge_from(&mut self, other: Census) {
        self.source_rows += other.source_rows;
        self.written_rows += other.written_rows;
        self.written_unrouted += other.written_unrouted;
        self.written_trades += other.written_trades;
        self.written_quotes += other.written_quotes;
        self.written_corrections += other.written_corrections;
        self.written_statuses += other.written_statuses;
        self.skipped_ric_filter += other.skipped_ric_filter;
        self.skipped_unparsed += other.skipped_unparsed;
        for (name, count) in other.counted {
            *self.counted.entry(name).or_default() += count;
        }
    }
}

impl fmt::Display for Census {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "usstock_replay finished source_rows={} written_rows={} written_unrouted={} written_trades={} written_quotes={} written_corrections={} written_statuses={} skipped_ric_filter={} skipped_unparsed={}",
            self.source_rows,
            self.written_rows,
            self.written_unrouted,
            self.written_trades,
            self.written_quotes,
            self.written_corrections,
            self.written_statuses,
            self.skipped_ric_filter,
            self.skipped_unparsed,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StockTrade {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub source_sequence: u64,
    pub venue: String,
    pub price_e9: i64,
    pub volume_e9: i64,
    pub bid_e9: i64,
    pub bid_size_e9: i64,
    pub ask_e9: i64,
    pub ask_size_e9: i64,
    pub bid_venue: String,
    pub ask_venue: String,
    pub qualifiers: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StockQuote {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub source_sequence: u64,
    pub bid_e9: i64,
    pub bid_size_e9: i64,
    pub ask_e9: i64,
    pub ask_size_e9: i64,
    pub bid_venue: String,
    pub ask_venue: String,
    pub qualifiers: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StockStatus {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub source_sequence: u64,
    pub qualifiers: String,
    pub trading_status: String,
    pub halt_reason: String,
}

fn validate_period(period: &str) -> Result<()> {
    if period.is_empty()
        || period.len() > 64
        || !period.is_ascii()
        || !period
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        bail!("invalid TAS period {period:?}");
    }
    Ok(())
}

fn resolved_periods(config: &ReplayConfig) -> Result<Vec<String>> {
    let periods = if config.periods.is_empty() {
        vec![config.period.clone()]
    } else {
        config.periods.clone()
    };
    if periods.is_empty() {
        bail!("need at least one TAS period");
    }
    let mut distinct = BTreeSet::new();
    for period in &periods {
        validate_period(period)?;
        if !distinct.insert(period) {
            bail!("duplicate TAS period {period}");
        }
    }
    Ok(periods)
}

fn encode_ric(ric: &str) -> Result<[u8; RIC_LEN]> {
    if ric.is_empty() || !ric.is_ascii() || ric.len() > RIC_LEN {
        bail!("RIC {ric:?} must be nonempty ASCII and at most {RIC_LEN} bytes");
    }
    let mut out = [0_u8; RIC_LEN];
    out[..ric.len()].copy_from_slice(ric.as_bytes());
    Ok(out)
}

fn parse_date_time_ns(raw: &str) -> Result<u64> {
    if raw.is_empty() {
        bail!("empty required Date-Time");
    }
    let dt = DateTime::parse_from_rfc3339(raw)
        .map_err(|err| anyhow!("invalid Date-Time {raw:?}: {err}"))?;
    let nanos = dt
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("Date-Time {raw:?} is outside nanosecond range"))?;
    u64::try_from(nanos).map_err(|_| anyhow!("Date-Time {raw:?} predates Unix epoch"))
}

fn append_text(out: &mut Vec<u8>, value: &str) -> Result<()> {
    let len =
        u16::try_from(value.len()).map_err(|_| anyhow!("text field exceeds {} bytes", u16::MAX))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take<'a>(buf: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow!("record offset overflow"))?;
    let out = buf
        .get(*offset..end)
        .ok_or_else(|| anyhow!("truncated record"))?;
    *offset = end;
    Ok(out)
}

fn take_u64(buf: &[u8], offset: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(take(buf, offset, 8)?.try_into()?))
}

fn take_i64(buf: &[u8], offset: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(take(buf, offset, 8)?.try_into()?))
}

fn take_text(buf: &[u8], offset: &mut usize) -> Result<String> {
    let len = u16::from_le_bytes(take(buf, offset, 2)?.try_into()?) as usize;
    Ok(std::str::from_utf8(take(buf, offset, len)?)?.to_string())
}

fn encode_value_prefix(out: &mut Vec<u8>, kind: u8) {
    out.extend_from_slice(&MAGIC);
    out.push(kind);
}

fn decode_value_prefix(buf: &[u8], expected_kind: u8) -> Result<usize> {
    if buf.len() < 3 || buf[0..2] != MAGIC {
        bail!("invalid usstock replay record prefix");
    }
    if buf[2] != expected_kind {
        bail!(
            "record kind {} does not match expected {expected_kind}",
            buf[2]
        );
    }
    Ok(3)
}

fn ensure_consumed(buf: &[u8], offset: usize) -> Result<()> {
    if offset != buf.len() {
        bail!("record has {} trailing bytes", buf.len() - offset);
    }
    Ok(())
}

pub fn encode_trade(row: &StockTrade) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(160 + row.qualifiers.len());
    encode_value_prefix(&mut out, KIND_TRADE);
    out.extend_from_slice(&row.exch_hms_ns.to_le_bytes());
    out.extend_from_slice(&row.source_sequence.to_le_bytes());
    for value in [
        row.price_e9,
        row.volume_e9,
        row.bid_e9,
        row.bid_size_e9,
        row.ask_e9,
        row.ask_size_e9,
    ] {
        out.extend_from_slice(&value.to_le_bytes());
    }
    for text in [&row.venue, &row.bid_venue, &row.ask_venue, &row.qualifiers] {
        append_text(&mut out, text)?;
    }
    Ok(out)
}

pub fn decode_trade(ric: &str, ts_utc_ns: u64, buf: &[u8]) -> Result<StockTrade> {
    let mut offset = decode_value_prefix(buf, KIND_TRADE)?;
    let exch_hms_ns = take_u64(buf, &mut offset)?;
    let source_sequence = take_u64(buf, &mut offset)?;
    let price_e9 = take_i64(buf, &mut offset)?;
    let volume_e9 = take_i64(buf, &mut offset)?;
    let bid_e9 = take_i64(buf, &mut offset)?;
    let bid_size_e9 = take_i64(buf, &mut offset)?;
    let ask_e9 = take_i64(buf, &mut offset)?;
    let ask_size_e9 = take_i64(buf, &mut offset)?;
    let venue = take_text(buf, &mut offset)?;
    let bid_venue = take_text(buf, &mut offset)?;
    let ask_venue = take_text(buf, &mut offset)?;
    let qualifiers = take_text(buf, &mut offset)?;
    ensure_consumed(buf, offset)?;
    Ok(StockTrade {
        ric: ric.to_string(),
        ts_utc_ns,
        exch_hms_ns,
        source_sequence,
        venue,
        price_e9,
        volume_e9,
        bid_e9,
        bid_size_e9,
        ask_e9,
        ask_size_e9,
        bid_venue,
        ask_venue,
        qualifiers,
    })
}

fn encode_quote_with_kind(row: &StockQuote, kind: u8) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(128 + row.qualifiers.len());
    encode_value_prefix(&mut out, kind);
    out.extend_from_slice(&row.exch_hms_ns.to_le_bytes());
    out.extend_from_slice(&row.source_sequence.to_le_bytes());
    for value in [row.bid_e9, row.bid_size_e9, row.ask_e9, row.ask_size_e9] {
        out.extend_from_slice(&value.to_le_bytes());
    }
    for text in [&row.bid_venue, &row.ask_venue, &row.qualifiers] {
        append_text(&mut out, text)?;
    }
    Ok(out)
}

pub fn encode_quote(row: &StockQuote) -> Result<Vec<u8>> {
    encode_quote_with_kind(row, KIND_QUOTE)
}

pub fn decode_quote(ric: &str, ts_utc_ns: u64, buf: &[u8]) -> Result<StockQuote> {
    let mut offset = decode_value_prefix(buf, KIND_QUOTE)?;
    let exch_hms_ns = take_u64(buf, &mut offset)?;
    let source_sequence = take_u64(buf, &mut offset)?;
    let bid_e9 = take_i64(buf, &mut offset)?;
    let bid_size_e9 = take_i64(buf, &mut offset)?;
    let ask_e9 = take_i64(buf, &mut offset)?;
    let ask_size_e9 = take_i64(buf, &mut offset)?;
    let bid_venue = take_text(buf, &mut offset)?;
    let ask_venue = take_text(buf, &mut offset)?;
    let qualifiers = take_text(buf, &mut offset)?;
    ensure_consumed(buf, offset)?;
    Ok(StockQuote {
        ric: ric.to_string(),
        ts_utc_ns,
        exch_hms_ns,
        source_sequence,
        bid_e9,
        bid_size_e9,
        ask_e9,
        ask_size_e9,
        bid_venue,
        ask_venue,
        qualifiers,
    })
}

pub fn encode_correction(row: &StockTrade) -> Result<Vec<u8>> {
    let mut value = encode_trade(row)?;
    value[2] = KIND_CORRECTION;
    Ok(value)
}

pub fn decode_correction(ric: &str, ts_utc_ns: u64, buf: &[u8]) -> Result<StockTrade> {
    let mut copy = buf.to_vec();
    if copy.len() < 3 || copy[2] != KIND_CORRECTION {
        bail!("not a usstock correction record");
    }
    copy[2] = KIND_TRADE;
    decode_trade(ric, ts_utc_ns, &copy)
}

pub fn encode_status(row: &StockStatus) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(
        96 + row.qualifiers.len() + row.trading_status.len() + row.halt_reason.len(),
    );
    encode_value_prefix(&mut out, KIND_STATUS);
    out.extend_from_slice(&row.exch_hms_ns.to_le_bytes());
    out.extend_from_slice(&row.source_sequence.to_le_bytes());
    for text in [&row.qualifiers, &row.trading_status, &row.halt_reason] {
        append_text(&mut out, text)?;
    }
    Ok(out)
}

pub fn decode_status(ric: &str, ts_utc_ns: u64, buf: &[u8]) -> Result<StockStatus> {
    let mut offset = decode_value_prefix(buf, KIND_STATUS)?;
    let exch_hms_ns = take_u64(buf, &mut offset)?;
    let source_sequence = take_u64(buf, &mut offset)?;
    let qualifiers = take_text(buf, &mut offset)?;
    let trading_status = take_text(buf, &mut offset)?;
    let halt_reason = take_text(buf, &mut offset)?;
    ensure_consumed(buf, offset)?;
    Ok(StockStatus {
        ric: ric.to_string(),
        ts_utc_ns,
        exch_hms_ns,
        source_sequence,
        qualifiers,
        trading_status,
        halt_reason,
    })
}

fn encode_key(ts_utc_ns: u64, part: u16, source_row: u64) -> [u8; KEY_LEN] {
    let mut key = [0_u8; KEY_LEN];
    key[..8].copy_from_slice(&ts_utc_ns.to_be_bytes());
    key[8..10].copy_from_slice(&part.to_be_bytes());
    key[10..].copy_from_slice(&source_row.to_be_bytes());
    key
}

pub fn decode_key(key: &[u8]) -> Result<(u64, u16, u64)> {
    if key.len() != KEY_LEN {
        bail!("stock key must be {KEY_LEN} bytes, got {}", key.len());
    }
    Ok((
        u64::from_be_bytes(key[..8].try_into()?),
        u16::from_be_bytes(key[8..10].try_into()?),
        u64::from_be_bytes(key[10..].try_into()?),
    ))
}

/// Encodes every source cell in header order.  A set bitmap bit is followed by
/// that cell's raw UTF-8 bytes, length-prefixed with u32 LE.  Empty cells have
/// an unset bit.  This representation deliberately keeps identity, market,
/// status, reference, and analytic cells together so filtering rules cannot
/// discard source information during replay.
pub fn encode_raw_source_row(record: &StringRecord, expected_columns: usize) -> Result<Vec<u8>> {
    let column_count = record.len().max(expected_columns);
    let column_count_u32 = u32::try_from(column_count)
        .map_err(|_| anyhow!("source row has too many columns: {column_count}"))?;
    let bitmap_len = (column_count + 7) / 8;
    let bitmap_len_u32 = u32::try_from(bitmap_len)
        .map_err(|_| anyhow!("source row bitmap is too large: {bitmap_len}"))?;
    let mut bitmap = vec![0_u8; bitmap_len];
    let mut payloads = Vec::new();
    for index in 0..column_count {
        let value = record.get(index).unwrap_or("");
        if value.is_empty() {
            continue;
        }
        bitmap[index / 8] |= 1 << (index % 8);
        let len = u32::try_from(value.len())
            .map_err(|_| anyhow!("source cell {index} exceeds u32 length"))?;
        payloads.push((len, value));
    }
    let mut out = Vec::with_capacity(
        10 + bitmap_len + payloads.iter().map(|(_, v)| v.len() + 4).sum::<usize>(),
    );
    out.extend_from_slice(&RAW_ROW_MAGIC);
    out.extend_from_slice(&column_count_u32.to_le_bytes());
    out.extend_from_slice(&bitmap_len_u32.to_le_bytes());
    out.extend_from_slice(&bitmap);
    for (len, value) in payloads {
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    Ok(out)
}

pub fn decode_raw_source_row(value: &[u8]) -> Result<Vec<String>> {
    if value.len() < 10 || value[..2] != RAW_ROW_MAGIC {
        bail!("invalid raw source-row record");
    }
    let column_count = u32::from_le_bytes(value[2..6].try_into()?) as usize;
    let bitmap_len = u32::from_le_bytes(value[6..10].try_into()?) as usize;
    let expected_bitmap_len = (column_count + 7) / 8;
    if bitmap_len != expected_bitmap_len {
        bail!("raw source-row bitmap length {bitmap_len} does not match {column_count} columns");
    }
    let mut offset = 10;
    let bitmap = take(value, &mut offset, bitmap_len)?;
    let mut cells = Vec::with_capacity(column_count);
    for index in 0..column_count {
        if bitmap[index / 8] & (1 << (index % 8)) == 0 {
            cells.push(String::new());
            continue;
        }
        let len = u32::from_le_bytes(take(value, &mut offset, 4)?.try_into()?) as usize;
        cells.push(std::str::from_utf8(take(value, &mut offset, len)?)?.to_string());
    }
    ensure_consumed(value, offset)?;
    Ok(cells)
}

pub fn decode_source_schema(value: &[u8]) -> Result<Vec<String>> {
    if value.len() < 6 || value[..2] != SCHEMA_MAGIC {
        bail!("invalid source-schema record");
    }
    let count = u32::from_le_bytes(value[2..6].try_into()?) as usize;
    let mut offset = 6;
    let mut names = Vec::with_capacity(count);
    for _ in 0..count {
        let len = u32::from_le_bytes(take(value, &mut offset, 4)?.try_into()?) as usize;
        names.push(std::str::from_utf8(take(value, &mut offset, len)?)?.to_string());
    }
    ensure_consumed(value, offset)?;
    Ok(names)
}

#[derive(Debug, Clone)]
struct ColIdx {
    ric: usize,
    date_time: usize,
    event_type: usize,
}

#[derive(Debug, Clone)]
struct HeaderMap {
    names: Vec<String>,
    idx: ColIdx,
}

impl HeaderMap {
    fn from_headers(headers: &StringRecord) -> Result<Self> {
        let mut names = Vec::with_capacity(headers.len());
        let mut by_name = BTreeMap::new();
        for (index, raw) in headers.iter().enumerate() {
            let name = raw.trim();
            if name.is_empty() {
                bail!("TAS header contains an empty column name at index {index}");
            }
            if by_name.insert(name.to_string(), index).is_some() {
                bail!("TAS header repeats column {name:?}");
            }
            names.push(name.to_string());
        }
        let required = |name: &str| -> Result<usize> {
            by_name
                .get(name)
                .copied()
                .ok_or_else(|| anyhow!("stock TAS header missing required column {name}"))
        };
        Ok(Self {
            idx: ColIdx {
                ric: required("#RIC")?,
                date_time: required("Date-Time")?,
                event_type: required("Type")?,
            },
            names,
        })
    }

    fn summary(&self, record: &StringRecord) -> String {
        let mut parts = Vec::new();
        for (index, name) in self.names.iter().enumerate() {
            let value = record.get(index).map(str::trim).unwrap_or("");
            if !value.is_empty() {
                parts.push(format!("{name}={value}"));
            }
            if parts.len() == 12 {
                break;
            }
        }
        if parts.is_empty() {
            "<empty>".to_string()
        } else {
            parts.join(" ")
        }
    }
}

#[derive(Debug, Clone)]
struct Job {
    period: String,
    part_no: u16,
    path: PathBuf,
    header_path: PathBuf,
    has_header: bool,
}

fn period_dir(config: &ReplayConfig, period: &str) -> PathBuf {
    config.data_root.join(format!(
        "shanghai_evolution_equities_time_and_sales_ric_list_0_tas_{period}"
    ))
}

fn part_number(path: &Path) -> Result<u16> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("part path {} is not UTF-8", path.display()))?;
    let number = name
        .strip_prefix("merged-Data-part-")
        .and_then(|name| name.strip_suffix(".csv.gz"))
        .ok_or_else(|| anyhow!("unrecognized stock TAS part filename {name}"))?;
    number
        .parse::<u16>()
        .map_err(|err| anyhow!("invalid stock TAS part number {number:?}: {err}"))
}

fn discover_jobs(config: &ReplayConfig, periods: &[String]) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for period in periods {
        let dir = period_dir(config, period);
        let single = dir.join("merged-Data.csv.gz");
        let mut parts = Vec::new();
        for entry in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with("merged-Data-part-") && name.ends_with(".csv.gz") {
                parts.push((part_number(&path)?, path));
            }
        }
        if single.is_file() {
            if !parts.is_empty() {
                bail!(
                    "{} contains both merged-Data.csv.gz and multipart TAS files",
                    dir.display()
                );
            }
            jobs.push(Job {
                period: period.clone(),
                part_no: 0,
                header_path: single.clone(),
                path: single,
                has_header: true,
            });
            continue;
        }
        if parts.is_empty() {
            bail!("{} has no complete stock TAS gzip input", dir.display());
        }
        parts.sort_by_key(|(number, _)| *number);
        if parts.first().map(|(number, _)| *number) != Some(0) {
            bail!("{} multipart TAS input is missing part 0", dir.display());
        }
        let header_path = parts[0].1.clone();
        for (part_no, path) in parts {
            jobs.push(Job {
                period: period.clone(),
                part_no,
                has_header: part_no == 0,
                path,
                header_path: header_path.clone(),
            });
        }
    }
    Ok(jobs)
}

fn read_header_record(path: &Path) -> Result<StringRecord> {
    let file = File::open(path).with_context(|| format!("open header {}", path.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(1024 * 1024, file));
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(decoder);
    let headers = reader
        .records()
        .next()
        .ok_or_else(|| anyhow!("{} is empty", path.display()))?
        .with_context(|| format!("read header {}", path.display()))?;
    if headers.get(0).map(str::trim) != Some("#RIC") {
        bail!(
            "{} does not begin with the stock TAS #RIC header",
            path.display()
        );
    }
    Ok(headers)
}

fn read_header(path: &Path) -> Result<HeaderMap> {
    HeaderMap::from_headers(&read_header_record(path)?)
}

fn db_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_max_open_files(4096);
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.increase_parallelism(16);
    options.set_max_background_jobs(16);
    options.set_write_buffer_size(256 * 1024 * 1024);
    options.set_max_write_buffer_number(8);
    options
}

fn cf_options() -> Options {
    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_write_buffer_size(256 * 1024 * 1024);
    options.set_max_write_buffer_number(8);
    options
}

fn stock_cf_name(ric: &str) -> Result<String> {
    encode_ric(ric)?;
    Ok(format!("{US_STOCK_CF_PREFIX}{ric}"))
}

fn descriptors(mut names: Vec<String>) -> Vec<ColumnFamilyDescriptor> {
    if !names.iter().any(|name| name == "default") {
        names.push("default".to_string());
    }
    if !names.iter().any(|name| name == CF_REPLAY_META) {
        names.push(CF_REPLAY_META.to_string());
    }
    if !names.iter().any(|name| name == CF_SOURCE_SCHEMA) {
        names.push(CF_SOURCE_SCHEMA.to_string());
    }
    if !names.iter().any(|name| name == CF_SOURCE_UNROUTED) {
        names.push(CF_SOURCE_UNROUTED.to_string());
    }
    names.sort();
    names.dedup();
    names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options()))
        .collect()
}

fn open_rocksdb(path: &Path) -> Result<DB> {
    if path.exists() && !path.is_dir() {
        bail!("rocksdb_dir {} is not a directory", path.display());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let names = if path.is_dir() {
        DB::list_cf(&Options::default(), path)
            .with_context(|| format!("list RocksDB column families {}", path.display()))?
    } else {
        vec![
            "default".to_string(),
            CF_REPLAY_META.to_string(),
            CF_SOURCE_SCHEMA.to_string(),
            CF_SOURCE_UNROUTED.to_string(),
        ]
    };
    for name in &names {
        if name != "default"
            && name != CF_REPLAY_META
            && name != CF_SOURCE_SCHEMA
            && name != CF_SOURCE_UNROUTED
            && !name.starts_with(US_STOCK_CF_PREFIX)
        {
            bail!(
                "RocksDB {} has unsupported legacy column family {name:?}; create a new stock RocksDB",
                path.display()
            );
        }
    }
    DB::open_cf_descriptors(&db_options(), path, descriptors(names))
        .with_context(|| format!("open RocksDB {}", path.display()))
}

fn cf<'a>(db: &'a DB, name: &str) -> Result<&'a ColumnFamily> {
    db.cf_handle(name)
        .ok_or_else(|| anyhow!("RocksDB is missing column family {name}"))
}

fn report_rics(config: &ReplayConfig, periods: &[String]) -> Result<BTreeSet<String>> {
    let mut rics = config.rics.iter().cloned().collect::<BTreeSet<_>>();
    let mut missing_reports = Vec::new();
    for period in periods {
        let path = period_dir(config, period).join("merged-Report.csv.gz");
        if !path.is_file() {
            missing_reports.push(path);
            continue;
        }
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let decoder = MultiGzDecoder::new(BufReader::with_capacity(1024 * 1024, file));
        let mut reader = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(decoder);
        let headers = reader
            .headers()
            .with_context(|| format!("read report header {}", path.display()))?;
        let ric_index = headers
            .iter()
            .position(|name| name.trim() == "#RIC")
            .ok_or_else(|| anyhow!("{} report has no #RIC column", path.display()))?;
        for row in reader.records() {
            let row = row.with_context(|| format!("read report row {}", path.display()))?;
            let ric = row.get(ric_index).map(str::trim).unwrap_or("");
            if !ric.is_empty() {
                encode_ric(ric)?;
                rics.insert(ric.to_string());
            }
        }
    }
    if !missing_reports.is_empty() && config.rics.is_empty() {
        let paths = missing_reports
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "US-stock TAS report is required to precreate per-RIC column families; missing {paths}. Set rics explicitly only for a bounded input"
        );
    }
    if rics.is_empty() {
        bail!("no stock RICs found in reports or config rics");
    }
    Ok(rics)
}

fn create_stock_column_families(db: &mut DB, rics: &BTreeSet<String>) -> Result<()> {
    for ric in rics {
        let name = stock_cf_name(ric)?;
        if db.cf_handle(&name).is_none() {
            db.create_cf(&name, &cf_options())
                .with_context(|| format!("create US-stock column family {name}"))?;
        }
    }
    Ok(())
}

fn schema_meta_key(period: &str) -> Vec<u8> {
    format!("header:{period}").into_bytes()
}

fn store_period_schemas(db: &DB, jobs: &[Job]) -> Result<()> {
    let schema_cf = cf(db, CF_SOURCE_SCHEMA)?;
    let mut headers = BTreeMap::<String, PathBuf>::new();
    for job in jobs {
        match headers.get(&job.period) {
            None => {
                headers.insert(job.period.clone(), job.header_path.clone());
            }
            Some(existing) if existing == &job.header_path => {}
            Some(existing) => bail!(
                "period {} has conflicting header paths {} and {}",
                job.period,
                existing.display(),
                job.header_path.display()
            ),
        }
    }
    for (period, path) in headers {
        let encoded =
            bin_msg::UsStockSchemaMsg::from_headers(&read_header_record(&path)?)?.into_bytes();
        db.put_cf(schema_cf, schema_meta_key(&period), encoded)
            .with_context(|| format!("write source schema for period {period}"))?;
    }
    Ok(())
}

fn encode_unrouted_key(period: &str, part: u16, source_row: u64) -> Result<Vec<u8>> {
    validate_period(period)?;
    let mut key = Vec::with_capacity(period.len() + 1 + 2 + 8);
    key.extend_from_slice(period.as_bytes());
    key.push(0);
    key.extend_from_slice(&part.to_be_bytes());
    key.extend_from_slice(&source_row.to_be_bytes());
    Ok(key)
}

fn period_meta_key(period: &str) -> Vec<u8> {
    format!("{PERIOD_PREFIX}{period}").into_bytes()
}

fn claim_period(db: &DB, period: &str) -> Result<()> {
    let meta = cf(db, CF_REPLAY_META)?;
    let key = period_meta_key(period);
    match db.get_cf(meta, &key)? {
        None => {
            let mut options = WriteOptions::default();
            options.set_sync(true);
            db.put_cf_opt(meta, key, STATUS_WRITING, &options)
                .with_context(|| format!("claim period {period}"))?;
            Ok(())
        }
        Some(status) if status.as_slice() == STATUS_DONE => {
            bail!("period {period} is already done; refuse to overwrite")
        }
        Some(status) if status.as_slice() == STATUS_WRITING => bail!(
            "period {period} is marked writing; inspect or replace the incomplete RocksDB before retrying"
        ),
        Some(status) => bail!(
            "period {period} has unknown replay watermark {:?}",
            String::from_utf8_lossy(&status)
        ),
    }
}

fn finish_period(db: &DB, period: &str) -> Result<()> {
    let meta = cf(db, CF_REPLAY_META)?;
    let key = period_meta_key(period);
    if db.get_cf(meta, &key)?.as_deref() != Some(STATUS_WRITING) {
        bail!("period {period} is not writing before finish");
    }
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.put_cf_opt(meta, key, STATUS_DONE, &options)
        .with_context(|| format!("finish period {period}"))
}

struct AuditSink {
    log: Mutex<File>,
    unparsed: Mutex<File>,
}

impl AuditSink {
    fn open(log_path: &Path, unparsed_path: &Path) -> Result<Self> {
        for path in [log_path, unparsed_path] {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create audit directory {}", parent.display()))?;
            }
        }
        Ok(Self {
            log: Mutex::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_path)
                    .with_context(|| format!("open log {}", log_path.display()))?,
            ),
            unparsed: Mutex::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(unparsed_path)
                    .with_context(|| format!("open unparsed log {}", unparsed_path.display()))?,
            ),
        })
    }

    fn log(&self, message: impl fmt::Display) {
        if let Ok(mut file) = self.log.lock() {
            let _ = writeln!(
                file,
                "[{}] {message}",
                Utc::now().format("%Y-%m-%dT%H:%M:%SZ")
            );
        }
    }

    fn unparsed(&self, job: &Job, source_row: u64, summary: &str, err: &anyhow::Error) {
        let line = format!(
            "[{}] period={} part_no={} source_row={} part={} reason={} :: {}\n",
            Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            job.period,
            job.part_no,
            source_row,
            job.path.display(),
            err,
            summary,
        );
        if let Ok(mut file) = self.unparsed.lock() {
            let _ = file.write_all(line.as_bytes());
        }
    }
}

fn flush_batch(db: &DB, batch: &mut WriteBatch) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut options = WriteOptions::default();
    options.set_sync(false);
    db.write_opt(std::mem::take(batch), &options)
        .context("write RocksDB batch")
}

fn replay_job(
    config: &ReplayConfig,
    db: &DB,
    job: &Job,
    ric_filter: Option<&BTreeSet<String>>,
    abort: &AtomicBool,
    audit: &AuditSink,
) -> Result<Census> {
    if abort.load(Ordering::Relaxed) {
        bail!("aborted before opening {}", job.path.display());
    }
    let file = File::open(&job.path).with_context(|| format!("open {}", job.path.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(16 * 1024 * 1024, file));
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(decoder);
    let map = if job.has_header {
        let headers = reader
            .records()
            .next()
            .ok_or_else(|| anyhow!("{} is empty", job.path.display()))?
            .with_context(|| format!("read header {}", job.path.display()))?;
        if headers.get(0).map(str::trim) != Some("#RIC") {
            bail!("{} part 0 is missing the #RIC header", job.path.display());
        }
        HeaderMap::from_headers(&headers)?
    } else {
        read_header(&job.header_path)?
    };

    let mut batch = WriteBatch::default();
    let mut pending = 0_usize;
    let mut census = Census::default();
    let started = Instant::now();
    let mut remaining = config.max_source_rows;

    for result in reader.records() {
        if remaining.is_some_and(|limit| limit == 0) {
            break;
        }
        if abort.load(Ordering::Relaxed) && census.source_rows % 100_000 == 0 {
            bail!("aborted while reading {}", job.path.display());
        }
        let record = result.with_context(|| format!("read row from {}", job.path.display()))?;
        census.source_rows += 1;
        if let Some(remaining) = remaining.as_mut() {
            *remaining = remaining.saturating_sub(1);
        }
        let source_row = census.source_rows;
        let parsed = (|| -> Result<()> {
            let ric = record.get(map.idx.ric).map(str::trim).unwrap_or("");
            if ric_filter.is_some_and(|filter| !filter.contains(ric)) {
                census.skipped_ric_filter += 1;
                return Ok(());
            }
            let message = bin_msg::UsStockSourceRowMsg::from_csv(&record, map.names.len())?;
            let event_type = record.get(map.idx.event_type).map(str::trim).unwrap_or("");
            match event_type {
                "Trade" => census.written_trades += 1,
                "Quote" => census.written_quotes += 1,
                "Correction" => census.written_corrections += 1,
                "Mkt. Condition" => census.written_statuses += 1,
                other if other.is_empty() => census.count("type_empty"),
                other => census.count(&format!("type:{other}")),
            }
            let route = (|| -> Result<(String, u64)> {
                if ric.is_empty() {
                    bail!("empty #RIC");
                }
                let ts_utc_ns =
                    parse_date_time_ns(record.get(map.idx.date_time).map(str::trim).unwrap_or(""))?;
                Ok((stock_cf_name(ric)?, ts_utc_ns))
            })();
            match route {
                Ok((stock_cf_name, ts_utc_ns)) => match cf(db, &stock_cf_name) {
                    Ok(stock_cf) => {
                        batch.put_cf(
                            stock_cf,
                            encode_key(ts_utc_ns, job.part_no, source_row),
                            message.into_bytes(),
                        );
                        census.written_rows += 1;
                    }
                    Err(err) => {
                        let unrouted_cf = cf(db, CF_SOURCE_UNROUTED)?;
                        batch.put_cf(
                            unrouted_cf,
                            encode_unrouted_key(&job.period, job.part_no, source_row)?,
                            message.into_bytes(),
                        );
                        census.written_rows += 1;
                        census.written_unrouted += 1;
                        census.count("unrouted");
                        audit.unparsed(job, source_row, &map.summary(&record), &err);
                    }
                },
                Err(err) => {
                    let unrouted_cf = cf(db, CF_SOURCE_UNROUTED)?;
                    batch.put_cf(
                        unrouted_cf,
                        encode_unrouted_key(&job.period, job.part_no, source_row)?,
                        message.into_bytes(),
                    );
                    census.written_rows += 1;
                    census.written_unrouted += 1;
                    census.count("unrouted");
                    audit.unparsed(job, source_row, &map.summary(&record), &err);
                }
            }
            pending += 1;
            if pending >= BATCH_ROWS {
                flush_batch(db, &mut batch)?;
                pending = 0;
            }
            Ok(())
        })();
        if let Err(err) = parsed {
            census.skipped_unparsed += 1;
            census.count("unparsed");
            audit.unparsed(job, source_row, &map.summary(&record), &err);
        }
        if config.progress_every > 0 && census.source_rows % config.progress_every == 0 {
            let elapsed = started.elapsed().as_secs_f64().max(0.001);
            audit.log(format!(
                "progress period={} part_no={} source_rows={} written_rows={} unrouted={} trades={} quotes={} corrections={} statuses={} unparsed={} rows_per_s={:.0}",
                job.period,
                job.part_no,
                census.source_rows,
                census.written_rows,
                census.written_unrouted,
                census.written_trades,
                census.written_quotes,
                census.written_corrections,
                census.written_statuses,
                census.skipped_unparsed,
                census.source_rows as f64 / elapsed,
            ));
        }
    }
    flush_batch(db, &mut batch)?;
    Ok(census)
}

/// Replay every selected source file.  Full runs claim/finish each period in
/// `replay_meta`; a diagnostic `max_source_rows` run writes no done watermark.
pub fn replay(config: &ReplayConfig) -> Result<Census> {
    if config.workers == 0 {
        bail!("workers must be positive");
    }
    let periods = resolved_periods(config)?;
    let jobs = discover_jobs(config, &periods)?;
    let capped = config.max_source_rows.is_some();
    let audit = Arc::new(AuditSink::open(&config.log_path, &config.unparsed_path)?);
    audit.log(format!(
        "start periods={periods:?} workers={} jobs={} rocksdb={} capped={capped}",
        config.workers,
        jobs.len(),
        config.rocksdb_dir.display(),
    ));
    let stock_rics = report_rics(config, &periods)?;
    let mut mutable_db = open_rocksdb(&config.rocksdb_dir)?;
    create_stock_column_families(&mut mutable_db, &stock_rics)?;
    let db = Arc::new(mutable_db);
    store_period_schemas(&db, &jobs)?;
    if !capped {
        for period in &periods {
            claim_period(&db, period)?;
        }
    }

    let filter = if config.rics.is_empty() {
        None
    } else {
        Some(config.rics.iter().cloned().collect::<BTreeSet<_>>())
    };
    let queue = Arc::new(Mutex::new(VecDeque::from(jobs)));
    let abort = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for worker_id in 0..config.workers.min(
        queue
            .lock()
            .map_err(|_| anyhow!("job queue poisoned"))?
            .len(),
    ) {
        let config = config.clone();
        let db = Arc::clone(&db);
        let queue = Arc::clone(&queue);
        let abort = Arc::clone(&abort);
        let audit = Arc::clone(&audit);
        let filter = filter.clone();
        workers.push(thread::spawn(move || -> Result<Census> {
            let mut local = Census::default();
            loop {
                let job = queue
                    .lock()
                    .map_err(|_| anyhow!("job queue poisoned"))?
                    .pop_front();
                let Some(job) = job else {
                    return Ok(local);
                };
                audit.log(format!(
                    "worker={worker_id} claim period={} part_no={} path={}",
                    job.period,
                    job.part_no,
                    job.path.display(),
                ));
                match replay_job(&config, &db, &job, filter.as_ref(), &abort, &audit) {
                    Ok(census) => local.merge_from(census),
                    Err(err) => {
                        abort.store(true, Ordering::Relaxed);
                        return Err(err.context(format!(
                            "worker {worker_id} failed on period {} part {}",
                            job.period, job.part_no
                        )));
                    }
                }
            }
        }));
    }

    let mut census = Census::default();
    let mut first_error = None;
    for worker in workers {
        match worker.join() {
            Ok(Ok(local)) => census.merge_from(local),
            Ok(Err(err)) => {
                abort.store(true, Ordering::Relaxed);
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Err(_) => {
                abort.store(true, Ordering::Relaxed);
                if first_error.is_none() {
                    first_error = Some(anyhow!("usstock replay worker panicked"));
                }
            }
        }
    }
    if let Some(err) = first_error {
        audit.log(format!("failed: {err:#}"));
        return Err(err);
    }
    db.flush().context("flush stock RocksDB")?;
    if !capped {
        for period in &periods {
            finish_period(&db, period)?;
        }
    }
    audit.log(&census);
    Ok(census)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use rocksdb::IteratorMode;
    use std::io::Write;
    use tempfile::TempDir;

    const HEADER: &str = "#RIC,Date-Time,Type,Ex/Cntrb.ID,Price,Volume,Buyer ID,Bid Price,Bid Size,Seller ID,Ask Price,Ask Size,Qualifiers,Seq. No.,Exch Time,Trading Status,Halt Reason\n";

    fn write_gzip(path: &Path, text: &str) {
        let file = File::create(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(text.as_bytes()).unwrap();
        encoder.finish().unwrap();
    }

    fn fixture_config(temp: &TempDir, period: &str) -> ReplayConfig {
        ReplayConfig {
            data_root: temp.path().join("normalised"),
            period: period.to_string(),
            periods: vec![],
            rocksdb_dir: temp.path().join("rocksdb"),
            rics: vec!["AAPL.O".to_string(), "SPY.P".to_string()],
            workers: 1,
            progress_every: 0,
            max_source_rows: None,
            log_path: temp.path().join("replay.log"),
            unparsed_path: temp.path().join("unparsed.log"),
        }
    }

    fn period_path(config: &ReplayConfig, period: &str) -> PathBuf {
        period_dir(config, period)
    }

    #[test]
    fn replay_single_file_preserves_every_source_row() {
        let temp = TempDir::new().unwrap();
        let period = "2026-07-01_2026-08-14";
        let config = fixture_config(&temp, period);
        let dir = period_path(&config, period);
        fs::create_dir_all(&dir).unwrap();
        let rows = [
            HEADER,
            "AAPL.O,2026-07-01T08:00:00.000000001Z,Quote,,,,NAS,289.17,40,DEX,289.38,80,book,1,08:00:00.000000001,,\n",
            "AAPL.O,2026-07-01T08:00:00.000000002Z,Quote,,,,,0,0,DEX,289.01,12,clear-bid,2,08:00:00.000000002,,\n",
            "AAPL.O,2026-07-01T08:00:00.000000003Z,Trade,PSE,289.4917,0.010362,NAS,289.17,40,DEX,289.50,5480,odd-lot,3,08:00:00.000000003,,\n",
            "AAPL.O,2026-07-01T08:00:00.000000004Z,Correction,PSE,289.4917,0.010362,NAS,289.17,40,DEX,289.50,5480,correct,4,08:00:00.000000004,,\n",
            "AAPL.O,2026-07-01T08:00:00.000000005Z,Mkt. Condition,,,,,,,,,,open,5,08:00:00.000000005,Open,\n",
            "AAPL.O,2026-07-01T08:00:00.000000006Z,Auction,PSE,289.50,1,,,,,,auction,6,08:00:00.000000006,,\n",
        ]
        .concat();
        write_gzip(&dir.join("merged-Data.csv.gz"), &rows);

        let census = replay(&config).unwrap();
        assert_eq!(census.source_rows, 6);
        assert_eq!(census.written_rows, 6);
        assert_eq!(census.written_trades, 1);
        assert_eq!(census.written_quotes, 2);
        assert_eq!(census.written_corrections, 1);
        assert_eq!(census.written_statuses, 1);
        assert_eq!(census.skipped_unparsed, 0);

        let db = open_rocksdb(&config.rocksdb_dir).unwrap();
        let schema_cf = cf(&db, CF_SOURCE_SCHEMA).unwrap();
        let schema = db
            .get_cf(schema_cf, schema_meta_key(period))
            .unwrap()
            .unwrap();
        assert_eq!(decode_source_schema(&schema).unwrap().len(), 17);
        let aapl_cf_name = stock_cf_name("AAPL.O").unwrap();
        let aapl_cf = cf(&db, &aapl_cf_name).unwrap();
        let events = db
            .iterator_cf(aapl_cf, IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(events.len(), 6);
        let decoded = events
            .iter()
            .map(|(_, value)| decode_raw_source_row(value).unwrap())
            .collect::<Vec<_>>();
        let trade = decoded.iter().find(|cells| cells[2] == "Trade").unwrap();
        assert_eq!(trade[5], "0.010362");
        assert_eq!(trade[12], "odd-lot");
        let cleared_bid = decoded
            .iter()
            .find(|cells| cells[12] == "clear-bid")
            .unwrap();
        assert_eq!(cleared_bid[7], "0");
        assert_eq!(cleared_bid[8], "0");
        assert_eq!(cleared_bid[10], "289.01");
        let status = decoded
            .iter()
            .find(|cells| cells[2] == "Mkt. Condition")
            .unwrap();
        assert_eq!(status[15], "Open");
        assert!(decoded.iter().any(|cells| cells[2] == "Auction"));

        drop(db);
        assert!(replay(&config)
            .unwrap_err()
            .to_string()
            .contains("already done"));
    }

    #[test]
    fn multipart_input_reuses_part_zero_header() {
        let temp = TempDir::new().unwrap();
        let period = "2025-07-01_2026-07-01";
        let config = fixture_config(&temp, period);
        let dir = period_path(&config, period);
        fs::create_dir_all(&dir).unwrap();
        write_gzip(
            &dir.join("merged-Data-part-000000.csv.gz"),
            format!(
                "{HEADER}SPY.P,2025-07-01T08:00:00Z,Trade,PSE,620.1,1,NAS,620,20,DEX,620.2,10,one,1,08:00:00,,\n"
            ).as_str(),
        );
        write_gzip(
            &dir.join("merged-Data-part-000001.csv.gz"),
            "SPY.P,2025-07-01T08:00:01Z,Trade,PSE,620.2,0.5,NAS,620,20,DEX,620.3,10,two,2,08:00:01,,\n",
        );
        let census = replay(&config).unwrap();
        assert_eq!(census.written_rows, 2);
    }

    #[test]
    fn raw_row_and_schema_codecs_round_trip_every_cell() {
        let headers = StringRecord::from(vec!["#RIC", "Date-Time", "Type", "PE Ratio"]);
        let row = StringRecord::from(vec!["AAPL.O", "2026-07-01T00:00:00.1Z", "Trade", "35.1429"]);
        assert_eq!(
            decode_raw_source_row(&encode_raw_source_row(&row, headers.len()).unwrap()).unwrap(),
            row.iter().map(str::to_string).collect::<Vec<_>>()
        );
        assert_eq!(
            bin_msg::UsStockSchemaMsg::from_headers(&headers)
                .unwrap()
                .headers()
                .unwrap(),
            headers.iter().map(str::to_string).collect::<Vec<_>>()
        );
    }
}
