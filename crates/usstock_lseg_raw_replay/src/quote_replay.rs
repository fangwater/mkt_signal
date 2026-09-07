use crate::event_codec::{
    decode_correction, decode_trade, encode_correction, encode_exact_slots, encode_trade,
    layout_for_message, layout_for_type, validate_exact_slots, CorrectionValue, TradeValue,
    WireLayout, MISSING_DATE, MISSING_U16, MISSING_U32, MISSING_U64, MSG_CANCEL, MSG_PREVIOUS_DAY,
    MSG_TRADE,
};
use crate::quote_codec::{
    decode_candidate, decode_key, encode_candidate, encode_key, encode_quote, encode_quote_state,
    later_candidate, QuoteCandidate, QuoteStateValue, QuoteValue, MISSING_CODE, MISSING_PRICE,
    MISSING_SIZE, MSG_QUOTE, MSG_QUOTE_STATE, SIDE_CLEAR, SIDE_UNCHANGED,
};
use crate::raw::{read_messages, RawField, RawMessage};
use crate::{Manifest, MANIFEST_FILE};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDate, NaiveTime, Timelike, Utc};
use crossbeam_channel::unbounded;
use flate2::read::MultiGzDecoder;
use fs2::FileExt;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MergeOperands, MultiThreaded,
    Options, WriteBatch, WriteOptions,
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use zstd::stream::read::Decoder as ZstdDecoder;

type ReplayDb = DBWithThreadMode<MultiThreaded>;

const NS_PER_SEC: u64 = 1_000_000_000;
const MS_PER_DAY: u64 = 86_400_000;
const WRITE_BATCH_OPS: usize = 50_000;
const CF_META: &str = "replay_meta";
const TEMP_PREFIX: &str = "tmp:";
const VENUE_PREFIX: &str = "v:";
const INSTRUMENT_PREFIX: &str = "i:";

const QUOTE_FIELDS: [(u32, &str); 24] = [
    (22, "BID"),
    (25, "ASK"),
    (30, "BIDSIZE"),
    (31, "ASKSIZE"),
    (11683, "BIDFINMMID"),
    (11684, "ASKFINMMID"),
    (3298, "BIDXID"),
    (3297, "ASKXID"),
    (6579, "BID_COND_N"),
    (6580, "ASK_COND_N"),
    (293, "BID_MMID1"),
    (296, "ASK_MMID1"),
    (1000, "GV1_TEXT"),
    (8937, "LIMIT_INDQ"),
    (3887, "SEQNUM_QT"),
    (118, "PRC_QL_CD"),
    (3264, "PRC_QL3"),
    (8406, "QTE_ORIGIN"),
    (1041, "GV1_FLAG"),
    (12783, "NBBO_IND"),
    (3855, "QUOTIM_MS"),
    (1025, "QUOTIM"),
    (14238, "ORDRECV_MS"),
    (14246, "ORDREC2_MS"),
];

const QUOTE_RIPPLE_FIELDS: [(u32, &str); 4] =
    [(23, "BID_1"), (24, "BID_2"), (26, "ASK_1"), (27, "ASK_2")];

const RANGE_FIELDS: [(u32, &str); 14] = [
    (90, "YRHIGH"),
    (91, "YRLOW"),
    (110, "YCHIGH_IND"),
    (111, "YCLOW_IND"),
    (350, "YRHIGHDAT"),
    (351, "YRLOWDAT"),
    (1075, "YRHI_IND"),
    (1076, "YRLO_IND"),
    (3265, "52WK_HIGH"),
    (3266, "52WK_LOW"),
    (3448, "52W_HDAT"),
    (3449, "52W_HIND"),
    (3450, "52W_LDAT"),
    (3451, "52W_LIND"),
];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteReplayConfig {
    pub period: String,
    pub staging_dir: Option<PathBuf>,
    #[serde(default)]
    pub inputs: Vec<PathBuf>,
    pub rocksdb_dir: PathBuf,
    #[serde(default = "default_progress_every")]
    pub progress_every: u64,
    #[serde(default)]
    pub keep_temporary_column_families: bool,
    #[serde(default = "default_replay_workers")]
    pub workers: usize,
}

fn default_progress_every() -> u64 {
    1_000_000
}

fn default_replay_workers() -> usize {
    16
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct QuoteReplayCensus {
    pub source_messages: u64,
    pub source_quotes: u64,
    pub source_quote_ripples: u64,
    pub source_empty_closing_runs: u64,
    pub source_range_updates: u64,
    pub source_trades: u64,
    pub source_corrections: u64,
    pub source_states: u64,
    pub source_refreshes: u64,
    pub temporary_snapshots: u64,
    pub quote_seconds: u64,
    pub venue_quote_values: u64,
    pub quote_state_values: u64,
    pub event_values: u64,
    pub encoded_by_type: BTreeMap<String, u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct QuoteVerifyCensus {
    pub status: String,
    pub venue_column_families: u64,
    pub instrument_column_families: u64,
    pub venue_quote_values: u64,
    pub quote_state_values: u64,
    pub event_values: u64,
    pub values_by_type: BTreeMap<String, u64>,
}

impl QuoteReplayCensus {
    fn merge_from(&mut self, other: Self) {
        self.source_messages += other.source_messages;
        self.source_quotes += other.source_quotes;
        self.source_quote_ripples += other.source_quote_ripples;
        self.source_empty_closing_runs += other.source_empty_closing_runs;
        self.source_range_updates += other.source_range_updates;
        self.source_trades += other.source_trades;
        self.source_corrections += other.source_corrections;
        self.source_states += other.source_states;
        self.source_refreshes += other.source_refreshes;
        self.temporary_snapshots += other.temporary_snapshots;
        self.quote_seconds += other.quote_seconds;
        self.venue_quote_values += other.venue_quote_values;
        self.quote_state_values += other.quote_state_values;
        self.event_values += other.event_values;
        for (name, count) in other.encoded_by_type {
            *self.encoded_by_type.entry(name).or_default() += count;
        }
    }
}

impl std::fmt::Display for QuoteVerifyCensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RAW verify status={} venue_column_families={} instrument_column_families={} venue_quote_values={} quote_state_values={} event_values={} values_by_type={:?}",
            self.status,
            self.venue_column_families,
            self.instrument_column_families,
            self.venue_quote_values,
            self.quote_state_values,
            self.event_values,
            self.values_by_type,
        )
    }
}

impl std::fmt::Display for QuoteReplayCensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RAW replay source_messages={} source_quotes={} source_quote_ripples={} source_empty_closing_runs={} source_range_updates={} source_trades={} source_corrections={} source_states={} source_refreshes={} temporary_snapshots={} quote_seconds={} venue_quote_values={} quote_state_values={} event_values={} encoded_by_type={:?}",
            self.source_messages,
            self.source_quotes,
            self.source_quote_ripples,
            self.source_empty_closing_runs,
            self.source_range_updates,
            self.source_trades,
            self.source_corrections,
            self.source_states,
            self.source_refreshes,
            self.temporary_snapshots,
            self.quote_seconds,
            self.venue_quote_values,
            self.quote_state_values,
            self.event_values,
            self.encoded_by_type,
        )
    }
}

pub fn load_quote_replay_config(path: &Path) -> Result<QuoteReplayConfig> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let config: QuoteReplayConfig =
        toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
    if config.period.is_empty() {
        bail!("RAW replay period must not be empty");
    }
    if config.staging_dir.is_some() == !config.inputs.is_empty() {
        bail!("set exactly one of staging_dir or inputs");
    }
    if config.workers == 0 {
        bail!("RAW replay workers must be >= 1");
    }
    Ok(config)
}

pub type RawReplayConfig = QuoteReplayConfig;
pub type RawReplayCensus = QuoteReplayCensus;
pub type RawVerifyCensus = QuoteVerifyCensus;

pub fn load_raw_replay_config(path: &Path) -> Result<RawReplayConfig> {
    load_quote_replay_config(path)
}

fn validate_cf_component(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        bail!("invalid {label} column-family component {value:?}");
    }
    Ok(())
}

pub fn venue_cf_name(ric: &str, venue: &str) -> Result<String> {
    validate_cf_component(ric, "RIC")?;
    validate_cf_component(venue, "venue")?;
    Ok(format!("{VENUE_PREFIX}{ric}:{venue}"))
}

pub fn instrument_cf_name(ric: &str) -> Result<String> {
    validate_cf_component(ric, "RIC")?;
    Ok(format!("{INSTRUMENT_PREFIX}{ric}"))
}

fn temporary_cf_name(ric: &str) -> Result<String> {
    validate_cf_component(ric, "RIC")?;
    Ok(format!("{TEMP_PREFIX}{ric}"))
}

fn quote_candidate_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut best = existing.map(ToOwned::to_owned);
    for operand in operands {
        best = Some(match best {
            Some(previous) => later_candidate(&previous, operand),
            None => operand.to_vec(),
        });
    }
    best
}

fn cf_options() -> Options {
    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_write_buffer_size(4 * 1024 * 1024);
    options.set_max_write_buffer_number(2);
    options.set_min_write_buffer_number_to_merge(1);
    options.set_merge_operator_associative("raw_quote_last", quote_candidate_merge);
    options
}

fn open_new_db(path: &Path) -> Result<ReplayDb> {
    if path.exists() {
        if !path.is_dir() || path.read_dir()?.next().is_some() {
            bail!(
                "RAW replay output {} already exists and is not empty",
                path.display()
            );
        }
    } else if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_max_open_files(8192);
    options.set_db_write_buffer_size(2 * 1024 * 1024 * 1024);
    options.increase_parallelism(16);
    options.set_max_background_jobs(16);
    options.set_merge_operator_associative("raw_quote_last", quote_candidate_merge);
    let descriptors = ["default", CF_META]
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options()));
    ReplayDb::open_cf_descriptors(&options, path, descriptors)
        .with_context(|| format!("open RocksDB {}", path.display()))
}

fn open_existing_db(path: &Path) -> Result<(ReplayDb, Vec<String>)> {
    if !path.is_dir() {
        bail!("quote RocksDB {} is not a directory", path.display());
    }
    let names = ReplayDb::list_cf(&Options::default(), path)
        .with_context(|| format!("list column families in {}", path.display()))?;
    for name in &names {
        if name != "default"
            && name != CF_META
            && !name.starts_with(VENUE_PREFIX)
            && !name.starts_with(INSTRUMENT_PREFIX)
        {
            bail!("unsupported or unfinished column family {name:?}");
        }
    }
    let descriptors = names
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options()));
    let mut options = Options::default();
    options.set_merge_operator_associative("raw_quote_last", quote_candidate_merge);
    let db = ReplayDb::open_cf_descriptors(&options, path, descriptors)
        .with_context(|| format!("open RAW RocksDB {}", path.display()))?;
    Ok((db, names))
}

fn ensure_cf<'a>(
    db: &'a ReplayDb,
    cf_lock: &Mutex<()>,
    name: &str,
) -> Result<Arc<BoundColumnFamily<'a>>> {
    if db.cf_handle(name).is_none() {
        let _guard = cf_lock
            .lock()
            .map_err(|_| anyhow!("RAW column-family creation lock is poisoned"))?;
        if db.cf_handle(name).is_none() {
            db.create_cf(name, &cf_options())
                .with_context(|| format!("create column family {name}"))?;
        }
    }
    db.cf_handle(name)
        .ok_or_else(|| anyhow!("column family {name} missing after create"))
}

fn flush_batch(db: &ReplayDb, batch: &mut WriteBatch) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut options = WriteOptions::default();
    options.set_sync(false);
    db.write_opt(std::mem::take(batch), &options)
        .context("write RAW quote RocksDB batch")?;
    Ok(())
}

fn parse_date_time_ns(raw: &str) -> Result<u64> {
    let parsed = DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("parse RAW Date-Time {raw:?}"))?;
    let nanos = parsed
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("RAW Date-Time {raw:?} is outside nanosecond range"))?;
    u64::try_from(nanos).map_err(|_| anyhow!("RAW Date-Time {raw:?} predates Unix epoch"))
}

pub fn parse_price_e9(raw: &str) -> Result<i64> {
    if raw.is_empty() {
        return Ok(MISSING_PRICE);
    }
    let (negative, unsigned) = match raw.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, raw),
    };
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 9
    {
        bail!("invalid e9 price {raw:?}");
    }
    let integer: i128 = integer.parse()?;
    let mut fraction_text = fraction.to_string();
    while fraction_text.len() < 9 {
        fraction_text.push('0');
    }
    let fraction: i128 = if fraction_text.is_empty() {
        0
    } else {
        fraction_text.parse()?
    };
    let mut scaled = integer
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| anyhow!("e9 price overflow {raw:?}"))?;
    if negative {
        scaled = -scaled;
    }
    i64::try_from(scaled).map_err(|_| anyhow!("e9 price overflow {raw:?}"))
}

fn parse_size(raw: &str) -> Result<u32> {
    if raw.is_empty() {
        return Ok(MISSING_SIZE);
    }
    raw.parse::<u32>()
        .with_context(|| format!("parse quote size {raw:?}"))
}

fn field<'a>(message: &'a RawMessage, name: &str) -> Result<&'a RawField> {
    message
        .fields
        .iter()
        .find(|field| field.name == name)
        .ok_or_else(|| anyhow!("Quote {} {} missing {name}", message.ric, message.date_time))
}

fn validate_quote_signature(message: &RawMessage) -> Result<()> {
    let mut expected = QUOTE_FIELDS.to_vec();
    if message.fields.len() == 25 {
        expected.insert(22, (3386, "QUOTE_DATE"));
    }
    let actual = message
        .fields
        .iter()
        .map(|field| (field.fid, field.name.as_str()))
        .collect::<Vec<_>>();
    if actual != expected {
        bail!(
            "unsupported Quote FID signature for {} {}: {:?}",
            message.ric,
            message.date_time,
            actual
        );
    }
    Ok(())
}

fn is_quote_ripple(message: &RawMessage) -> Result<bool> {
    if message.message_class != "UPDATE" || message.update_type != "QUOTE" {
        return Ok(false);
    }
    if message.fields.is_empty()
        || message.fields.iter().any(|field| {
            !QUOTE_RIPPLE_FIELDS
                .iter()
                .any(|expected| expected == &(field.fid, field.name.as_str()))
        })
    {
        return Ok(false);
    }
    let mut seen = BTreeSet::new();
    for field in &message.fields {
        if !seen.insert((field.fid, field.name.as_str())) {
            bail!(
                "duplicate Quote ripple FID {} {} at {} {}",
                field.fid,
                field.name,
                message.ric,
                message.date_time
            );
        }
        if !field.enum_value.trim().is_empty() {
            bail!(
                "Quote ripple FID {} {} unexpectedly has enum value {:?} at {} {}",
                field.fid,
                field.name,
                field.enum_value,
                message.ric,
                message.date_time
            );
        }
        parse_price_e9(&field.value).with_context(|| {
            format!(
                "validate Quote ripple FID {} {} at {} {}",
                field.fid, field.name, message.ric, message.date_time
            )
        })?;
    }
    Ok(true)
}

fn is_empty_closing_run(message: &RawMessage) -> Result<bool> {
    if message.message_class != "UPDATE"
        || message.update_type != "CLOSING_RUN"
        || message.fields.is_empty()
        || message
            .fields
            .iter()
            .any(|field| !field.value.is_empty() || !field.enum_value.is_empty())
    {
        return Ok(false);
    }
    layout_for_message(message)?;
    Ok(true)
}

fn is_range_update_only(message: &RawMessage) -> bool {
    message.message_class == "UPDATE"
        && message.update_type == "UNSPECIFIED"
        && !message.fields.is_empty()
        && message.fields.iter().all(|field| {
            RANGE_FIELDS
                .iter()
                .any(|expected| expected == &(field.fid, field.name.as_str()))
        })
}

fn parse_venue(field: &RawField) -> Result<String> {
    let code = field.enum_value.trim();
    if !code.is_empty() {
        validate_cf_component(code, "venue")?;
        return Ok(code.to_string());
    }
    match field.value.trim() {
        "" | "0" => Ok(String::new()),
        other => bail!(
            "venue FID {} has numeric value {other:?} without enum code",
            field.name
        ),
    }
}

fn parse_quality(message: &RawMessage) -> Result<u16> {
    let left = field(message, "PRC_QL_CD")?.value.trim();
    let right = field(message, "PRC_QL3")?.value.trim();
    if !left.is_empty() && !right.is_empty() && left != right {
        bail!("Quote quality codes disagree: {left:?} vs {right:?}");
    }
    let value = if left.is_empty() { right } else { left };
    if value.is_empty() {
        Ok(MISSING_CODE)
    } else {
        value
            .parse::<u16>()
            .with_context(|| format!("parse quote quality code {value:?}"))
    }
}

fn quote_bucket_ns(message: &RawMessage, source_ts_utc_ns: u64) -> Result<u64> {
    let date = match message
        .fields
        .iter()
        .find(|field| field.name == "QUOTE_DATE")
    {
        Some(field) if !field.value.is_empty() => {
            NaiveDate::parse_from_str(&field.value, "%Y-%m-%d")?
        }
        _ => DateTime::<Utc>::from_timestamp_nanos(i64::try_from(source_ts_utc_ns)?).date_naive(),
    };
    let quote_ms = field(message, "QUOTIM_MS")?.value.trim();
    let millis = if quote_ms.is_empty() {
        let raw = field(message, "QUOTIM")?.value.trim();
        if raw.is_empty() {
            return Ok((source_ts_utc_ns / NS_PER_SEC) * NS_PER_SEC);
        }
        let time = chrono::NaiveTime::parse_from_str(raw, "%H:%M:%S%.f")?;
        u64::from(time.num_seconds_from_midnight()) * 1_000
            + u64::from(time.nanosecond() / 1_000_000)
    } else {
        quote_ms
            .parse::<u64>()
            .with_context(|| format!("parse QUOTIM_MS {quote_ms:?}"))?
    };
    if millis >= MS_PER_DAY {
        bail!("QUOTIM_MS {millis} is outside one day");
    }
    let midnight = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow!("invalid Quote date {date}"))?
        .and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("Quote date {date} is outside nanosecond range"))?;
    let midnight = u64::try_from(midnight)?;
    Ok(midnight + (millis / 1_000) * NS_PER_SEC)
}

pub fn parse_quote(message: &RawMessage, part: u16, shard: u16) -> Result<(u64, QuoteCandidate)> {
    if message.message_class != "UPDATE" || message.update_type != "QUOTE" {
        bail!("parse_quote called for non-UPDATE/QUOTE message");
    }
    validate_quote_signature(message)?;
    if message.source_row > u64::from(u32::MAX) {
        bail!("source row {} does not fit 32 bits", message.source_row);
    }
    let source_ts_utc_ns = parse_date_time_ns(&message.date_time)?;
    let source_order = (u64::from(part) << 48) | (u64::from(shard) << 32) | message.source_row;
    let bucket = quote_bucket_ns(message, source_ts_utc_ns)?;
    Ok((
        bucket,
        QuoteCandidate {
            source_ts_utc_ns,
            source_order,
            bid: parse_price_e9(&field(message, "BID")?.value)?,
            bid_size: parse_size(&field(message, "BIDSIZE")?.value)?,
            ask: parse_price_e9(&field(message, "ASK")?.value)?,
            ask_size: parse_size(&field(message, "ASKSIZE")?.value)?,
            bid_venue: parse_venue(field(message, "BIDXID")?)?,
            ask_venue: parse_venue(field(message, "ASKXID")?)?,
            quality_code: parse_quality(message)?,
        },
    ))
}

fn source_order(message: &RawMessage, part: u16, shard: u16) -> Result<u64> {
    if message.source_row > u64::from(u32::MAX) {
        bail!("source row {} does not fit 32 bits", message.source_row);
    }
    Ok((u64::from(part) << 48) | (u64::from(shard) << 32) | message.source_row)
}

fn optional_field<'a>(message: &'a RawMessage, name: &str) -> Option<&'a RawField> {
    message.fields.iter().find(|field| field.name == name)
}

fn parse_u64_or_missing(raw: &str, label: &str) -> Result<u64> {
    if raw.is_empty() {
        Ok(MISSING_U64)
    } else {
        raw.parse::<u64>()
            .with_context(|| format!("parse {label} {raw:?}"))
    }
}

fn parse_u32_or_missing(raw: &str, label: &str) -> Result<u32> {
    if raw.is_empty() {
        Ok(MISSING_U32)
    } else {
        raw.parse::<u32>()
            .with_context(|| format!("parse {label} {raw:?}"))
    }
}

fn parse_u16_or_missing(raw: &str, label: &str) -> Result<u16> {
    if raw.is_empty() {
        Ok(MISSING_U16)
    } else {
        raw.parse::<u16>()
            .with_context(|| format!("parse {label} {raw:?}"))
    }
}

fn parse_event_ms(raw: &str, label: &str) -> Result<u32> {
    if raw.is_empty() {
        return Ok(MISSING_U32);
    }
    if raw.bytes().all(|byte| byte.is_ascii_digit()) {
        let value = raw
            .parse::<u32>()
            .with_context(|| format!("parse {label} milliseconds {raw:?}"))?;
        if u64::from(value) >= MS_PER_DAY {
            bail!("{label} {value} is outside one day");
        }
        return Ok(value);
    }
    let time = NaiveTime::parse_from_str(raw, "%H:%M:%S%.f")
        .with_context(|| format!("parse {label} time {raw:?}"))?;
    Ok(time.num_seconds_from_midnight() * 1_000 + time.nanosecond() / 1_000_000)
}

fn parse_date_days(raw: &str, label: &str) -> Result<i32> {
    if raw.is_empty() {
        return Ok(MISSING_DATE);
    }
    let date = NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .with_context(|| format!("parse {label} date {raw:?}"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    i32::try_from(date.signed_duration_since(epoch).num_days())
        .with_context(|| format!("{label} date is outside i32 days"))
}

fn condition4(raw: &str, label: &str) -> Result<[u8; 4]> {
    if !raw.is_ascii() || raw.as_bytes().contains(&0) || raw.len() > 4 {
        bail!("{label} must be at most four ASCII bytes: {raw:?}");
    }
    let mut out = [0_u8; 4];
    out[..raw.len()].copy_from_slice(raw.as_bytes());
    Ok(out)
}

fn event_venue(message: &RawMessage, name: &str) -> Result<(String, u32)> {
    let Some(field) = optional_field(message, name) else {
        return Ok((String::new(), MISSING_U32));
    };
    let id = parse_u32_or_missing(field.value.trim(), name)?;
    let venue = field.enum_value.trim();
    if !venue.is_empty() {
        validate_cf_component(venue, "venue")?;
    }
    Ok((venue.to_string(), id))
}

struct EncodedEvent {
    msg_type: u8,
    venue: String,
    value: Vec<u8>,
    name: String,
}

fn parse_trade_event(
    message: &RawMessage,
    source_ts_utc_ns: u64,
    source_order: u64,
) -> Result<EncodedEvent> {
    let normal = optional_field(message, "TRDPRC_1").is_some();
    let (price_name, size_name, time_name, id_name, sequence_name, condition_name, venue_name) =
        if normal {
            (
                "TRDPRC_1",
                "TRDVOL_1",
                "TRDTIM_MS",
                "TRADE_ID",
                "SEQNUM",
                "LSTSALCOND",
                "TRADE_EXID",
            )
        } else {
            (
                "IRGPRC",
                "IRGVOL",
                "IRGTIM_MS",
                "IRG_TRDID",
                "IRG_SEQNO",
                "IRGSALCOND",
                "IRG_EXID",
            )
        };
    let price = optional_field(message, price_name)
        .ok_or_else(|| anyhow!("TradeMsg is missing {price_name}"))?;
    let size = optional_field(message, size_name)
        .ok_or_else(|| anyhow!("TradeMsg is missing {size_name}"))?;
    let event_ms = optional_field(message, time_name)
        .map(|field| parse_event_ms(field.value.trim(), time_name))
        .transpose()?
        .unwrap_or(MISSING_U32);
    let trade_id = optional_field(message, id_name)
        .map(|field| parse_u64_or_missing(field.value.trim(), id_name))
        .transpose()?
        .unwrap_or(MISSING_U64);
    let sequence = optional_field(message, sequence_name)
        .map(|field| parse_u64_or_missing(field.value.trim(), sequence_name))
        .transpose()?
        .unwrap_or(MISSING_U64);
    let condition = optional_field(message, condition_name)
        .map(|field| condition4(&field.value, condition_name))
        .transpose()?
        .unwrap_or([0; 4]);
    let (venue, exchange_id) = event_venue(message, venue_name)?;
    let mut flags = 0_u16;
    if optional_field(message, "ODD_PRC").is_some() {
        flags |= 1 << 0;
    }
    if optional_field(message, "BLK_PRC1").is_some() {
        flags |= 1 << 1;
    }
    if optional_field(message, "CRSTRD_PRC").is_some() {
        flags |= 1 << 2;
    }
    if optional_field(message, "RETRAN_IND").is_some_and(|field| field.value.trim() == "1") {
        flags |= 1 << 3;
    }
    if optional_field(message, "THRESH_IND").is_some_and(|field| field.value.trim() == "1") {
        flags |= 1 << 4;
    }
    let quality_code = optional_field(message, "PRC_QL2")
        .map(|field| parse_u16_or_missing(field.value.trim(), "PRC_QL2"))
        .transpose()?
        .unwrap_or(MISSING_U16);
    let row = TradeValue {
        source_ts_utc_ns,
        source_order,
        event_ms,
        exchange_id,
        price: parse_price_e9(price.value.trim())?,
        size: parse_u64_or_missing(size.value.trim(), size_name)?,
        trade_id,
        sequence,
        condition,
        flags,
        quality_code,
    };
    Ok(EncodedEvent {
        msg_type: MSG_TRADE,
        venue,
        value: encode_trade(&row).to_vec(),
        name: "TradeMsg".to_string(),
    })
}

fn parse_correction_event(
    message: &RawMessage,
    source_ts_utc_ns: u64,
    source_order: u64,
    layout: &WireLayout,
) -> Result<EncodedEvent> {
    let cancel = layout.msg_type == MSG_CANCEL;
    let prefix = if cancel { "CAN" } else { "PD" };
    let price_name = if cancel { "CAN_PRC" } else { "PDTRDPRC" };
    let size_name = if cancel { "CAN_VOL" } else { "PREDAYVOL" };
    let time_name = if cancel { "CTRDTIM_MS" } else { "PDTRDTM_MS" };
    let id_name = if cancel { "CAN_TRD_ID" } else { "PD_TRDID" };
    let sequence_name = if cancel { "INS_SEQNO" } else { "PD_SEQNO" };
    let condition_name = if cancel { "CAN_COND_N" } else { "PD_SALCOND" };
    let condition_code_name = if cancel { "CAN_COND" } else { "" };
    let date_name = if cancel { "CAN_DATE" } else { "PDTRDDATE" };
    let (venue, exchange_id) = if cancel {
        event_venue(message, "CAN_EXID")?
    } else {
        (String::new(), MISSING_U32)
    };
    let row = CorrectionValue {
        source_ts_utc_ns,
        source_order,
        event_ms: optional_field(message, time_name)
            .map(|field| parse_event_ms(field.value.trim(), time_name))
            .transpose()?
            .unwrap_or(MISSING_U32),
        exchange_id,
        price: parse_price_e9(field(message, price_name)?.value.trim())?,
        size: parse_u64_or_missing(field(message, size_name)?.value.trim(), size_name)?,
        trade_id: optional_field(message, id_name)
            .map(|field| parse_u64_or_missing(field.value.trim(), id_name))
            .transpose()?
            .unwrap_or(MISSING_U64),
        sequence: optional_field(message, sequence_name)
            .map(|field| parse_u64_or_missing(field.value.trim(), sequence_name))
            .transpose()?
            .unwrap_or(MISSING_U64),
        condition: optional_field(message, condition_name)
            .map(|field| condition4(&field.value, condition_name))
            .transpose()?
            .unwrap_or([0; 4]),
        condition_code: if condition_code_name.is_empty() {
            MISSING_U16
        } else {
            optional_field(message, condition_code_name)
                .map(|field| parse_u16_or_missing(field.value.trim(), condition_code_name))
                .transpose()?
                .unwrap_or(MISSING_U16)
        },
        flags: u16::from(
            optional_field(message, &format!("{prefix}_TDTH_X"))
                .is_some_and(|field| !field.value.trim().is_empty()),
        ),
        trade_date_days: optional_field(message, date_name)
            .map(|field| parse_date_days(field.value.trim(), date_name))
            .transpose()?
            .unwrap_or(MISSING_DATE),
    };
    Ok(EncodedEvent {
        msg_type: layout.msg_type,
        venue,
        value: encode_correction(&row).to_vec(),
        name: layout.name.clone(),
    })
}

fn encode_nonquote_events(
    message: &RawMessage,
    part: u16,
    shard: u16,
) -> Result<Vec<EncodedEvent>> {
    let layout = layout_for_message(message)?;
    let source_ts_utc_ns = parse_date_time_ns(&message.date_time)?;
    let order = source_order(message, part, shard)?;
    if message.update_type == "CORRECTION" {
        let mut events = Vec::new();
        if optional_field(message, "CAN_PRC").is_some() {
            events.push(parse_correction_event(
                message,
                source_ts_utc_ns,
                order,
                layout_for_type(MSG_CANCEL)?,
            )?);
        }
        if optional_field(message, "PDTRDPRC").is_some() {
            events.push(parse_correction_event(
                message,
                source_ts_utc_ns,
                order,
                layout_for_type(MSG_PREVIOUS_DAY)?,
            )?);
        }
        if layout.exact_slots {
            events.push(EncodedEvent {
                msg_type: layout.msg_type,
                venue: String::new(),
                value: encode_exact_slots(message, source_ts_utc_ns, order, layout)?,
                name: layout.name.clone(),
            });
        }
        if events.is_empty() {
            bail!("{} has no Correction encoder", layout.name);
        }
        return Ok(events);
    }
    match layout.msg_type {
        MSG_TRADE => Ok(vec![parse_trade_event(message, source_ts_utc_ns, order)?]),
        _ if layout.exact_slots => Ok(vec![EncodedEvent {
            msg_type: layout.msg_type,
            venue: String::new(),
            value: encode_exact_slots(message, source_ts_utc_ns, order, layout)?,
            name: layout.name.clone(),
        }]),
        _ => bail!("{} has no RAW event encoder", layout.name),
    }
}

fn source_location(path: &Path) -> Result<(u16, u16)> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("non-UTF8 RAW shard name"))?;
    let part = name
        .strip_prefix("merged-Data-part-")
        .and_then(|rest| rest.get(..6))
        .ok_or_else(|| anyhow!("cannot parse source part from {name}"))?;
    let part = part
        .parse()
        .with_context(|| format!("parse part in {name}"))?;
    let shard = match name.split_once("-shard-") {
        Some((_, rest)) => rest
            .get(..6)
            .ok_or_else(|| anyhow!("cannot parse source shard from {name}"))?
            .parse()
            .with_context(|| format!("parse shard in {name}"))?,
        None => 0,
    };
    Ok((part, shard))
}

fn discover_inputs(config: &QuoteReplayConfig) -> Result<Vec<(u16, u16, PathBuf)>> {
    if !config.inputs.is_empty() {
        let mut inputs = config
            .inputs
            .iter()
            .map(|path| {
                let (part, shard) = source_location(path)?;
                Ok((part, shard, path.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        inputs.sort();
        return Ok(inputs);
    }
    let directory = config.staging_dir.as_ref().expect("validated staging_dir");
    let manifest_path = directory.join(MANIFEST_FILE);
    let manifest: Manifest = serde_json::from_reader(
        File::open(&manifest_path)
            .with_context(|| format!("open complete manifest {}", manifest_path.display()))?,
    )?;
    manifest.validate(&config.period, true)?;
    manifest
        .shards
        .iter()
        .map(|shard| {
            Ok((
                shard.original_part,
                u16::try_from(shard.shard_index).context("RAW shard index exceeds u16")?,
                directory.join(&shard.file),
            ))
        })
        .collect()
}

fn read_one_input<F>(path: &Path, on_message: F) -> Result<u64>
where
    F: FnMut(RawMessage) -> Result<()>,
{
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::with_capacity(16 * 1024 * 1024, file);
    match path.extension().and_then(|value| value.to_str()) {
        Some("zst") => read_messages(ZstdDecoder::new(reader)?, on_message),
        Some("gz") => read_messages(MultiGzDecoder::new(reader), on_message),
        Some("csv") => read_messages(reader, on_message),
        other => bail!("unsupported RAW input extension {other:?}"),
    }
}

fn side_is_clear(price: i64, size: u32) -> Result<bool> {
    match (
        price == MISSING_PRICE,
        size == MISSING_SIZE,
        price == 0,
        size == 0,
    ) {
        (true, true, _, _) | (false, false, true, true) => Ok(true),
        (false, false, _, _) => Ok(false),
        _ => bail!("Quote side has price/size presence mismatch"),
    }
}

fn put_venue_quote(
    db: &ReplayDb,
    cf_lock: &Mutex<()>,
    batch: &mut WriteBatch,
    ric: &str,
    venue: &str,
    key: &[u8],
    value: QuoteValue,
) -> Result<()> {
    let cf = ensure_cf(db, cf_lock, &venue_cf_name(ric, venue)?)?;
    batch.put_cf(&cf, key, encode_quote(&value));
    Ok(())
}

fn finalize_one_ric(
    db: &ReplayDb,
    cf_lock: &Mutex<()>,
    temporary_name: &str,
    census: &mut QuoteReplayCensus,
) -> Result<()> {
    let ric = temporary_name
        .strip_prefix(TEMP_PREFIX)
        .ok_or_else(|| anyhow!("invalid temporary column family {temporary_name}"))?;
    let temp_cf = db
        .cf_handle(temporary_name)
        .ok_or_else(|| anyhow!("missing temporary column family {temporary_name}"))?;
    let instrument_name = instrument_cf_name(ric)?;
    ensure_cf(db, cf_lock, &instrument_name)?;

    let mut venues = BTreeSet::new();
    for row in db.iterator_cf(&temp_cf, rocksdb::IteratorMode::Start) {
        let (_, value) = row?;
        let candidate = decode_candidate(&value)?;
        if !candidate.bid_venue.is_empty() {
            venues.insert(candidate.bid_venue);
        }
        if !candidate.ask_venue.is_empty() {
            venues.insert(candidate.ask_venue);
        }
    }
    for venue in venues {
        ensure_cf(db, cf_lock, &venue_cf_name(ric, &venue)?)?;
    }

    let instrument_cf = db
        .cf_handle(&instrument_name)
        .expect("created instrument CF");
    let mut batch = WriteBatch::default();
    let mut last_quality = MISSING_CODE;
    for row in db.iterator_cf(&temp_cf, rocksdb::IteratorMode::Start) {
        let (key, value) = row?;
        let (msg_type, bucket, source_row) = decode_key(&key)?;
        if msg_type != MSG_QUOTE || source_row != 0 {
            bail!("invalid temporary Quote key in {temporary_name}");
        }
        let candidate = decode_candidate(&value)?;
        let bid_clear = side_is_clear(candidate.bid, candidate.bid_size)?;
        let ask_clear = side_is_clear(candidate.ask, candidate.ask_size)?;
        if !bid_clear && candidate.bid_venue.is_empty() {
            bail!("valid bid has no venue for {ric} at {bucket}");
        }
        if !ask_clear && candidate.ask_venue.is_empty() {
            bail!("valid ask has no venue for {ric} at {bucket}");
        }

        let quote_key = encode_key(MSG_QUOTE, bucket, 0);
        if !bid_clear && !ask_clear && candidate.bid_venue == candidate.ask_venue {
            put_venue_quote(
                db,
                cf_lock,
                &mut batch,
                ric,
                &candidate.bid_venue,
                &quote_key,
                QuoteValue {
                    source_ts_utc_ns: candidate.source_ts_utc_ns,
                    source_order: candidate.source_order,
                    bid: candidate.bid,
                    bid_size: candidate.bid_size,
                    ask: candidate.ask,
                    ask_size: candidate.ask_size,
                },
            )?;
            census.venue_quote_values += 1;
        } else {
            if !bid_clear {
                put_venue_quote(
                    db,
                    cf_lock,
                    &mut batch,
                    ric,
                    &candidate.bid_venue,
                    &quote_key,
                    QuoteValue {
                        source_ts_utc_ns: candidate.source_ts_utc_ns,
                        source_order: candidate.source_order,
                        bid: candidate.bid,
                        bid_size: candidate.bid_size,
                        ask: MISSING_PRICE,
                        ask_size: MISSING_SIZE,
                    },
                )?;
                census.venue_quote_values += 1;
            }
            if !ask_clear {
                put_venue_quote(
                    db,
                    cf_lock,
                    &mut batch,
                    ric,
                    &candidate.ask_venue,
                    &quote_key,
                    QuoteValue {
                        source_ts_utc_ns: candidate.source_ts_utc_ns,
                        source_order: candidate.source_order,
                        bid: MISSING_PRICE,
                        bid_size: MISSING_SIZE,
                        ask: candidate.ask,
                        ask_size: candidate.ask_size,
                    },
                )?;
                census.venue_quote_values += 1;
            }
        }

        let quality_changed = candidate.quality_code != last_quality;
        if bid_clear || ask_clear || quality_changed {
            let state_key = encode_key(MSG_QUOTE_STATE, bucket, 0);
            let state = QuoteStateValue {
                source_ts_utc_ns: candidate.source_ts_utc_ns,
                source_order: candidate.source_order,
                bid_action: if bid_clear {
                    SIDE_CLEAR
                } else {
                    SIDE_UNCHANGED
                },
                ask_action: if ask_clear {
                    SIDE_CLEAR
                } else {
                    SIDE_UNCHANGED
                },
                quality_code: candidate.quality_code,
            };
            batch.put_cf(&instrument_cf, state_key, encode_quote_state(&state)?);
            census.quote_state_values += 1;
        }
        last_quality = candidate.quality_code;
        census.quote_seconds += 1;
        if batch.len() >= WRITE_BATCH_OPS {
            flush_batch(db, &mut batch)?;
        }
    }
    flush_batch(db, &mut batch)?;
    Ok(())
}

fn write_metadata(
    db: &ReplayDb,
    config: &QuoteReplayConfig,
    census: &QuoteReplayCensus,
) -> Result<()> {
    let cf = db.cf_handle(CF_META).context("missing replay_meta CF")?;
    let status = if config.staging_dir.is_some() {
        "complete"
    } else {
        "diagnostic-complete"
    };
    let rows = [
        ("schema", "lseg-usstock-raw-fixed".to_string()),
        ("scope", "all-audited-raw-messages".to_string()),
        ("status", status.to_string()),
        ("period", config.period.clone()),
        (
            "quote_value_bytes",
            crate::quote_codec::QUOTE_VALUE_LEN.to_string(),
        ),
        (
            "quote_state_value_bytes",
            crate::quote_codec::QUOTE_STATE_VALUE_LEN.to_string(),
        ),
        ("source_messages", census.source_messages.to_string()),
        ("source_quotes", census.source_quotes.to_string()),
        (
            "source_quote_ripples",
            census.source_quote_ripples.to_string(),
        ),
        (
            "source_empty_closing_runs",
            census.source_empty_closing_runs.to_string(),
        ),
        (
            "source_range_updates",
            census.source_range_updates.to_string(),
        ),
        ("source_trades", census.source_trades.to_string()),
        ("source_corrections", census.source_corrections.to_string()),
        ("source_states", census.source_states.to_string()),
        ("source_refreshes", census.source_refreshes.to_string()),
        ("quote_seconds", census.quote_seconds.to_string()),
        ("venue_quote_values", census.venue_quote_values.to_string()),
        ("quote_state_values", census.quote_state_values.to_string()),
        ("event_values", census.event_values.to_string()),
        ("deferred_messages", "0".to_string()),
    ];
    for (key, value) in rows {
        db.put_cf(&cf, key.as_bytes(), value.as_bytes())?;
    }
    Ok(())
}

fn metadata_u64(db: &ReplayDb, key: &str) -> Result<u64> {
    let cf = db.cf_handle(CF_META).context("missing replay_meta CF")?;
    let value = db
        .get_cf(&cf, key.as_bytes())?
        .ok_or_else(|| anyhow!("replay_meta is missing {key}"))?;
    std::str::from_utf8(&value)?
        .parse()
        .with_context(|| format!("parse replay_meta/{key}"))
}

pub fn verify_quote_rocksdb(path: &Path) -> Result<QuoteVerifyCensus> {
    let (db, names) = open_existing_db(path)?;
    let meta = db.cf_handle(CF_META).context("missing replay_meta CF")?;
    for (key, expected) in [
        ("schema", "lseg-usstock-raw-fixed"),
        ("scope", "all-audited-raw-messages"),
    ] {
        let actual = db
            .get_cf(&meta, key.as_bytes())?
            .ok_or_else(|| anyhow!("replay_meta is missing {key}"))?;
        if actual.as_slice() != expected.as_bytes() {
            bail!(
                "replay_meta/{key} is {:?}, expected {expected:?}",
                String::from_utf8_lossy(&actual)
            );
        }
    }
    let status = db
        .get_cf(&meta, b"status")?
        .ok_or_else(|| anyhow!("replay_meta is missing status"))?;
    if !matches!(status.as_slice(), b"complete" | b"diagnostic-complete") {
        bail!(
            "replay_meta/status is unsupported: {:?}",
            String::from_utf8_lossy(&status)
        );
    }

    let mut census = QuoteVerifyCensus {
        status: String::from_utf8(status.to_vec())?,
        ..QuoteVerifyCensus::default()
    };
    for name in names {
        if name.starts_with(VENUE_PREFIX) || name.starts_with(INSTRUMENT_PREFIX) {
            if name.starts_with(VENUE_PREFIX) {
                census.venue_column_families += 1;
            } else {
                census.instrument_column_families += 1;
            }
            let cf = db.cf_handle(&name).context("missing listed data CF")?;
            for row in db.iterator_cf(&cf, rocksdb::IteratorMode::Start) {
                let (key, value) = row?;
                let (msg_type, key_ts, source_order) = decode_key(&key)?;
                match msg_type {
                    MSG_QUOTE => {
                        if !name.starts_with(VENUE_PREFIX) || source_order != 0 {
                            bail!("invalid QuoteMsg key/column family in {name}");
                        }
                        let quote = crate::quote_codec::decode_quote(&value)?;
                        let bid_missing =
                            quote.bid == MISSING_PRICE && quote.bid_size == MISSING_SIZE;
                        let ask_missing =
                            quote.ask == MISSING_PRICE && quote.ask_size == MISSING_SIZE;
                        if bid_missing && ask_missing {
                            bail!("empty QuoteMsg in {name}");
                        }
                        if (quote.bid == MISSING_PRICE) != (quote.bid_size == MISSING_SIZE)
                            || (quote.ask == MISSING_PRICE) != (quote.ask_size == MISSING_SIZE)
                        {
                            bail!("partial QuoteMsg side in {name}");
                        }
                        census.venue_quote_values += 1;
                    }
                    MSG_QUOTE_STATE => {
                        if !name.starts_with(INSTRUMENT_PREFIX) || source_order != 0 {
                            bail!("invalid QuoteStateMsg key/column family in {name}");
                        }
                        crate::quote_codec::decode_quote_state(&value)?;
                        census.quote_state_values += 1;
                    }
                    MSG_TRADE => {
                        let decoded = decode_trade(&value)?;
                        if (decoded.source_ts_utc_ns, decoded.source_order)
                            != (key_ts, source_order)
                        {
                            bail!("TradeMsg key/value source mismatch in {name}");
                        }
                        census.event_values += 1;
                    }
                    MSG_CANCEL | MSG_PREVIOUS_DAY => {
                        let decoded = decode_correction(&value)?;
                        if (decoded.source_ts_utc_ns, decoded.source_order)
                            != (key_ts, source_order)
                        {
                            bail!("CorrectionMsg key/value source mismatch in {name}");
                        }
                        census.event_values += 1;
                    }
                    other => {
                        let layout = layout_for_type(other)?;
                        validate_exact_slots(&value, layout)?;
                        let value_ts = u64::from_le_bytes(value[0..8].try_into()?);
                        let value_order = u64::from_le_bytes(value[8..16].try_into()?);
                        if (value_ts, value_order) != (key_ts, source_order) {
                            bail!("{} key/value source mismatch in {name}", layout.name);
                        }
                        if !name.starts_with(INSTRUMENT_PREFIX) {
                            bail!("instrument-level {} found in {name}", layout.name);
                        }
                        census.event_values += 1;
                    }
                }
                if !matches!(msg_type, MSG_QUOTE | MSG_QUOTE_STATE) {
                    let layout = layout_for_type(msg_type)?;
                    *census
                        .values_by_type
                        .entry(layout.name.clone())
                        .or_default() += 1;
                }
            }
        } else if name != "default" && name != CF_META {
            bail!("unsupported column family {name:?}");
        }
    }
    let expected_quotes = metadata_u64(&db, "venue_quote_values")?;
    let expected_states = metadata_u64(&db, "quote_state_values")?;
    let expected_events = metadata_u64(&db, "event_values")?;
    if metadata_u64(&db, "deferred_messages")? != 0 {
        bail!("complete RAW RocksDB has deferred messages");
    }
    let classified_messages = [
        "source_quotes",
        "source_quote_ripples",
        "source_empty_closing_runs",
        "source_range_updates",
        "source_trades",
        "source_corrections",
        "source_states",
        "source_refreshes",
    ]
    .into_iter()
    .try_fold(0_u64, |total, key| {
        total
            .checked_add(metadata_u64(&db, key)?)
            .ok_or_else(|| anyhow!("RAW classified message count overflow"))
    })?;
    if classified_messages != metadata_u64(&db, "source_messages")? {
        bail!("RAW source message classification counts do not add up");
    }
    if census.venue_quote_values != expected_quotes
        || census.quote_state_values != expected_states
        || census.event_values != expected_events
    {
        bail!(
            "RocksDB counts {:?} do not match metadata quote_values={expected_quotes} state_values={expected_states} event_values={expected_events}",
            census
        );
    }
    Ok(census)
}

pub fn verify_raw_rocksdb(path: &Path) -> Result<RawVerifyCensus> {
    verify_quote_rocksdb(path)
}

pub fn raw_building_path(final_path: &Path) -> PathBuf {
    final_path.with_file_name(format!(
        "{}.building",
        final_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("usstock-raw-rocksdb")
    ))
}

pub fn raw_lock_path(final_path: &Path) -> PathBuf {
    final_path.with_file_name(format!(
        "{}.lock",
        final_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("usstock-raw-rocksdb")
    ))
}

pub struct RawTargetLock {
    _file: File,
}

pub fn acquire_raw_target_lock(final_path: &Path) -> Result<RawTargetLock> {
    let lock_path = raw_lock_path(final_path);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create RAW replay lock parent {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open RAW replay lock {}", lock_path.display()))?;
    file.try_lock_exclusive().with_context(|| {
        format!(
            "another RAW replay owns target lock {}; refusing to touch its output",
            lock_path.display()
        )
    })?;
    Ok(RawTargetLock { _file: file })
}

fn replay_one_input(
    db: &ReplayDb,
    cf_lock: &Mutex<()>,
    part: u16,
    shard: u16,
    path: &Path,
    abort: &AtomicBool,
    progress_every: u64,
) -> Result<(QuoteReplayCensus, BTreeSet<String>)> {
    let mut census = QuoteReplayCensus::default();
    let mut temporary_names = BTreeSet::new();
    let mut batch = WriteBatch::default();
    let mut open = BTreeMap::<String, (u64, QuoteCandidate)>::new();
    read_one_input(path, |message| {
        if abort.load(Ordering::Relaxed) {
            bail!("RAW replay worker aborted after another worker failed");
        }
        census.source_messages += 1;
        if is_quote_ripple(&message)? {
            census.source_quote_ripples += 1;
            return Ok(());
        }
        if is_empty_closing_run(&message)? {
            census.source_empty_closing_runs += 1;
            return Ok(());
        }
        if is_range_update_only(&message) {
            census.source_range_updates += 1;
            return Ok(());
        }
        if message.message_class != "UPDATE" || message.update_type != "QUOTE" {
            match (message.message_class.as_str(), message.update_type.as_str()) {
                ("UPDATE", "TRADE") => census.source_trades += 1,
                ("UPDATE", "CORRECTION") => census.source_corrections += 1,
                ("REFRESH", "") => census.source_refreshes += 1,
                ("UPDATE", "UNSPECIFIED" | "CLOSING_RUN" | "VERIFY") => census.source_states += 1,
                other => bail!(
                    "unsupported RAW outer message class/update type {:?} at {} {}",
                    other,
                    message.ric,
                    message.date_time
                ),
            }
            let events = encode_nonquote_events(&message, part, shard).with_context(|| {
                format!(
                    "encode RAW {}/{} message at {} {}",
                    message.message_class,
                    if message.update_type.is_empty() {
                        "<EMPTY>"
                    } else {
                        &message.update_type
                    },
                    message.ric,
                    message.date_time
                )
            })?;
            for event in events {
                let source_ts = u64::from_le_bytes(event.value[0..8].try_into()?);
                let order = u64::from_le_bytes(event.value[8..16].try_into()?);
                let cf_name = if event.venue.is_empty() {
                    instrument_cf_name(&message.ric)?
                } else {
                    venue_cf_name(&message.ric, &event.venue)?
                };
                let cf = ensure_cf(db, cf_lock, &cf_name)?;
                batch.put_cf(
                    &cf,
                    encode_key(event.msg_type, source_ts, order),
                    event.value,
                );
                census.event_values += 1;
                *census.encoded_by_type.entry(event.name).or_default() += 1;
            }
            if batch.len() >= WRITE_BATCH_OPS {
                flush_batch(db, &mut batch)?;
            }
            return Ok(());
        }
        census.source_quotes += 1;
        let (bucket, candidate) = parse_quote(&message, part, shard)?;
        let ric = message.ric;
        if let Some((previous_bucket, previous)) = open.get(&ric) {
            if *previous_bucket == bucket {
                if candidate.source_order >= previous.source_order {
                    open.insert(ric, (bucket, candidate));
                }
                return Ok(());
            }
            let temp_name = temporary_cf_name(&ric)?;
            let cf = ensure_cf(db, cf_lock, &temp_name)?;
            batch.merge_cf(
                &cf,
                encode_key(MSG_QUOTE, *previous_bucket, 0),
                encode_candidate(previous)?,
            );
            temporary_names.insert(temp_name);
            census.temporary_snapshots += 1;
            if batch.len() >= WRITE_BATCH_OPS {
                flush_batch(db, &mut batch)?;
            }
        }
        open.insert(ric, (bucket, candidate));
        Ok(())
    })
    .with_context(|| format!("replay RAW messages from {}", path.display()))?;
    for (ric, (bucket, candidate)) in open {
        let temp_name = temporary_cf_name(&ric)?;
        let cf = ensure_cf(db, cf_lock, &temp_name)?;
        batch.merge_cf(
            &cf,
            encode_key(MSG_QUOTE, bucket, 0),
            encode_candidate(&candidate)?,
        );
        temporary_names.insert(temp_name);
        census.temporary_snapshots += 1;
    }
    flush_batch(db, &mut batch)?;
    if progress_every > 0 && census.source_messages >= progress_every {
        log::info!(
            "RAW replay completed part={part} shard={shard} {} {census}",
            path.display()
        );
    }
    Ok((census, temporary_names))
}

pub fn replay_quotes(config: &QuoteReplayConfig) -> Result<QuoteReplayCensus> {
    let inputs = discover_inputs(config)?;
    if inputs.is_empty() {
        bail!("RAW replay has no inputs");
    }
    let final_path = &config.rocksdb_dir;
    let building_path = raw_building_path(final_path);
    if final_path.exists() || building_path.exists() {
        bail!(
            "RAW RocksDB output already exists: {} or {}",
            final_path.display(),
            building_path.display()
        );
    }
    let db = Arc::new(open_new_db(&building_path)?);
    let cf_lock = Arc::new(Mutex::new(()));
    let abort = Arc::new(AtomicBool::new(false));
    let mut census = QuoteReplayCensus::default();
    let mut temporary_names = BTreeSet::new();
    let worker_count = config.workers.min(inputs.len()).max(1);
    log::info!(
        "RAW replay starting workers={worker_count} shards={} rocksdb={}",
        inputs.len(),
        building_path.display()
    );
    let (job_tx, job_rx) = unbounded();
    for input in inputs {
        job_tx
            .send(input)
            .map_err(|_| anyhow!("RAW replay job queue closed before workers started"))?;
    }
    drop(job_tx);
    let mut handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let db = Arc::clone(&db);
        let cf_lock = Arc::clone(&cf_lock);
        let abort = Arc::clone(&abort);
        let job_rx = job_rx.clone();
        let progress_every = config.progress_every;
        handles.push(
            thread::Builder::new()
                .name(format!("usstock-raw-shard-{worker_id}"))
                .spawn(move || -> Result<(QuoteReplayCensus, BTreeSet<String>)> {
                    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let mut local_census = QuoteReplayCensus::default();
                        let mut local_names = BTreeSet::new();
                        while let Ok((part, shard, path)) = job_rx.recv() {
                            if abort.load(Ordering::Relaxed) {
                                break;
                            }
                            log::info!(
                                "RAW replay worker={worker_id} claimed part={part} shard={shard} {}",
                                path.display()
                            );
                            match replay_one_input(
                                &db,
                                &cf_lock,
                                part,
                                shard,
                                &path,
                                &abort,
                                progress_every,
                            ) {
                                Ok((part_census, names)) => {
                                    local_census.merge_from(part_census);
                                    local_names.extend(names);
                                }
                                Err(error) => {
                                    abort.store(true, Ordering::Relaxed);
                                    return Err(error.context(format!(
                                        "RAW replay worker {worker_id} failed on part={part} shard={shard} {}",
                                        path.display()
                                    )));
                                }
                            }
                        }
                        Ok((local_census, local_names))
                    }));
                    match run {
                        Ok(result) => result,
                        Err(_) => {
                            abort.store(true, Ordering::Relaxed);
                            bail!("RAW replay worker {worker_id} panicked")
                        }
                    }
                })
                .with_context(|| format!("spawn RAW replay worker {worker_id}"))?,
        );
    }
    drop(job_rx);
    let mut results = Vec::with_capacity(handles.len());
    for (worker_id, handle) in handles.into_iter().enumerate() {
        results.push(match handle.join() {
            Ok(result) => result,
            Err(_) => {
                abort.store(true, Ordering::Relaxed);
                Err(anyhow!(
                    "RAW replay worker {worker_id} panicked outside guard"
                ))
            }
        });
    }
    let mut first_abort = None;
    for result in results {
        match result {
            Ok((local, names)) => {
                census.merge_from(local);
                temporary_names.extend(names);
            }
            Err(error) => {
                if format!("{error:#}").contains("aborted after another worker failed") {
                    first_abort.get_or_insert(error);
                } else {
                    return Err(error);
                }
            }
        }
    }
    if let Some(error) = first_abort {
        return Err(error);
    }

    for name in &temporary_names {
        finalize_one_ric(&db, &cf_lock, name, &mut census)?;
    }
    write_metadata(&db, config, &census)?;
    db.flush().context("flush RAW quote RocksDB")?;
    if !config.keep_temporary_column_families {
        for name in &temporary_names {
            db.drop_cf(name)
                .with_context(|| format!("drop finalized temporary column family {name}"))?;
        }
    }
    drop(db);
    fs::rename(&building_path, final_path).with_context(|| {
        format!(
            "publish RAW quote RocksDB {} -> {}",
            building_path.display(),
            final_path.display()
        )
    })?;
    Ok(census)
}

pub fn replay_raw(config: &RawReplayConfig) -> Result<RawReplayCensus> {
    replay_quotes(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quote_codec::{decode_quote, decode_quote_state};
    use tempfile::TempDir;

    #[test]
    fn prices_are_exact_and_missing_is_distinct_from_zero() {
        assert_eq!(parse_price_e9("").unwrap(), MISSING_PRICE);
        assert_eq!(parse_price_e9("0").unwrap(), 0);
        assert_eq!(parse_price_e9("123.456789001").unwrap(), 123_456_789_001);
        assert_eq!(parse_price_e9("-1.25").unwrap(), -1_250_000_000);
        assert!(parse_price_e9("1.0000000001").is_err());
    }

    #[test]
    fn column_family_names_include_ric_and_venue() {
        assert_eq!(venue_cf_name("AAPL.O", "NAS").unwrap(), "v:AAPL.O:NAS");
        assert_eq!(instrument_cf_name("AAPL.O").unwrap(), "i:AAPL.O");
    }

    #[test]
    fn target_lock_has_single_owner() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("locked-output");
        let first = acquire_raw_target_lock(&output).unwrap();
        let error = acquire_raw_target_lock(&output).err().unwrap();
        assert!(error.to_string().contains("another RAW replay owns"));
        drop(first);
        acquire_raw_target_lock(&output).unwrap();
    }

    #[test]
    fn replays_last_quote_of_second_then_routes_by_venue() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("quote-db");
        let input = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/merged-Data-part-000000-shard-000000.csv");
        let config = QuoteReplayConfig {
            period: "fixture".to_string(),
            staging_dir: None,
            inputs: vec![input],
            rocksdb_dir: output.clone(),
            progress_every: 0,
            keep_temporary_column_families: false,
            workers: 1,
        };
        let census = replay_quotes(&config).unwrap();
        assert_eq!(census.source_messages, 8);
        assert_eq!(census.source_quotes, 4);
        assert_eq!(census.source_quote_ripples, 1);
        assert_eq!(census.source_empty_closing_runs, 1);
        assert_eq!(census.source_range_updates, 1);
        assert_eq!(census.temporary_snapshots, 3);
        assert_eq!(census.quote_seconds, 3);
        assert_eq!(census.venue_quote_values, 2);
        assert_eq!(census.quote_state_values, 2);
        assert_eq!(census.source_trades, 1);
        assert_eq!(census.event_values, 1);
        assert_eq!(census.encoded_by_type.get("TradeMsg"), Some(&1));

        let names = ReplayDb::list_cf(&Options::default(), &output).unwrap();
        assert!(names.iter().any(|name| name == "v:AAPL.O:IEX"));
        assert!(names.iter().any(|name| name == "v:AAPL.O:NAS"));
        assert!(names.iter().any(|name| name == "i:AAPL.O"));
        assert!(!names.iter().any(|name| name == "v:AAPL.O:BAT"));
        assert!(!names.iter().any(|name| name.starts_with(TEMP_PREFIX)));

        assert_eq!(
            verify_quote_rocksdb(&output).unwrap(),
            QuoteVerifyCensus {
                status: "diagnostic-complete".to_string(),
                venue_column_families: 2,
                instrument_column_families: 1,
                venue_quote_values: 2,
                quote_state_values: 2,
                event_values: 1,
                values_by_type: BTreeMap::from([("TradeMsg".to_string(), 1)]),
            }
        );
        let (db, _) = open_existing_db(&output).unwrap();
        let iex = db.cf_handle("v:AAPL.O:IEX").unwrap();
        let iex_rows = db
            .iterator_cf(&iex, rocksdb::IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(iex_rows.len(), 1);
        let (_, bucket, source_row) = decode_key(&iex_rows[0].0).unwrap();
        assert_eq!(bucket, 1_625_097_600_000_000_000);
        assert_eq!(source_row, 0);
        let quote = decode_quote(&iex_rows[0].1).unwrap();
        assert_eq!(quote.bid, 100_110_000_000);
        assert_eq!(quote.ask, 100_210_000_000);

        let instrument = db.cf_handle("i:AAPL.O").unwrap();
        let rows = db
            .iterator_cf(&instrument, rocksdb::IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(decode_key(&rows[0].0).unwrap().0, MSG_TRADE);
        assert_eq!(decode_trade(&rows[0].1).unwrap().price, 100_150_000_000);
        let states = rows
            .iter()
            .filter(|(key, _)| decode_key(key).unwrap().0 == MSG_QUOTE_STATE)
            .collect::<Vec<_>>();
        assert_eq!(states.len(), 2);
        let first = decode_quote_state(&states[0].1).unwrap();
        assert_eq!(first.bid_action, SIDE_UNCHANGED);
        assert_eq!(first.ask_action, SIDE_CLEAR);
        assert_eq!(first.quality_code, 77);
        let second = decode_quote_state(&states[1].1).unwrap();
        assert_eq!(second.bid_action, SIDE_CLEAR);
        assert_eq!(second.ask_action, SIDE_CLEAR);
        assert_eq!(second.quality_code, 94);
    }

    fn logical_dump(path: &Path) -> BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>> {
        let (db, mut names) = open_existing_db(path).unwrap();
        names.sort();
        names
            .into_iter()
            .map(|name| {
                let cf = db.cf_handle(&name).unwrap();
                let rows = db
                    .iterator_cf(&cf, rocksdb::IteratorMode::Start)
                    .map(|row| {
                        let (key, value) = row.unwrap();
                        (key.to_vec(), value.to_vec())
                    })
                    .collect();
                (name, rows)
            })
            .collect()
    }

    #[test]
    fn sixteen_workers_match_one_worker_logically() {
        let temp = TempDir::new().unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/merged-Data-part-000000-shard-000000.csv");
        let mut inputs = Vec::new();
        for shard in 0..17 {
            let path = temp
                .path()
                .join(format!("merged-Data-part-000000-shard-{shard:06}.csv"));
            fs::copy(&fixture, &path).unwrap();
            inputs.push(path);
        }
        let serial_path = temp.path().join("serial");
        let parallel_path = temp.path().join("parallel");
        let serial = replay_quotes(&QuoteReplayConfig {
            period: "parallel-fixture".to_string(),
            staging_dir: None,
            inputs: inputs.clone(),
            rocksdb_dir: serial_path.clone(),
            progress_every: 0,
            keep_temporary_column_families: false,
            workers: 1,
        })
        .unwrap();
        let parallel = replay_quotes(&QuoteReplayConfig {
            period: "parallel-fixture".to_string(),
            staging_dir: None,
            inputs,
            rocksdb_dir: parallel_path.clone(),
            progress_every: 0,
            keep_temporary_column_families: false,
            workers: 16,
        })
        .unwrap();
        assert_eq!(parallel, serial);
        assert_eq!(
            verify_raw_rocksdb(&parallel_path).unwrap(),
            verify_raw_rocksdb(&serial_path).unwrap()
        );
        assert_eq!(logical_dump(&parallel_path), logical_dump(&serial_path));
    }

    #[test]
    fn cross_shard_quote_uses_latest_source_order() {
        let temp = TempDir::new().unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/merged-Data-part-000000-shard-000000.csv");
        let first = temp.path().join("merged-Data-part-000000-shard-000000.csv");
        let second = temp.path().join("merged-Data-part-000000-shard-000001.csv");
        fs::copy(&fixture, &first).unwrap();
        let later_source = fs::read_to_string(&fixture)
            .unwrap()
            .replace(
                "2021-07-01T00:00:00.900000000Z",
                "2021-07-01T00:00:00.050000000Z",
            )
            .replace("100.11", "200.11");
        fs::write(&second, later_source).unwrap();
        let output = temp.path().join("cross-shard");
        replay_quotes(&QuoteReplayConfig {
            period: "cross-shard-fixture".to_string(),
            staging_dir: None,
            inputs: vec![first, second],
            rocksdb_dir: output.clone(),
            progress_every: 0,
            keep_temporary_column_families: false,
            workers: 2,
        })
        .unwrap();
        let (db, _) = open_existing_db(&output).unwrap();
        let cf = db.cf_handle("v:AAPL.O:IEX").unwrap();
        let rows = db
            .iterator_cf(&cf, rocksdb::IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 1);
        let quote = decode_quote(&rows[0].1).unwrap();
        assert_eq!(quote.bid, 200_110_000_000);
        assert_eq!(quote.source_order >> 32, 1);
    }
}
