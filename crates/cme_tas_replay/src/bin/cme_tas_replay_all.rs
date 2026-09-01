//! TAS replay into the year+product RocksDB.
//!
//! Quotes are stored as a 1s BBO snapshot. Half-side quotes overlay only within
//! the same second; a new second starts without inheriting either side.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::product::{
    encode_all_key, is_product_cf_name, parse_product, period_year, product_cf_name,
    quote_last_merge, quote_second_bucket_ns, FORBIDDEN_LEGACY_ROCKSDB,
};
use cme_tas_replay::shard::{period_dir_name, TasShardManifest};
use cme_tas_replay::{
    classify, decode_period_status, encode_cme_auction, encode_cme_correction,
    encode_cme_imbalance, encode_cme_implied_vol, encode_cme_price_limit, encode_cme_price_print,
    encode_cme_quote, encode_cme_settlement, encode_cme_special, encode_cme_status,
    encode_cme_tot_volume, encode_cme_trade, encode_period_status, encode_reference_change,
    encode_symbology_change, implied_vol_source, is_no_imbalance, overlay_imbalance,
    overlay_price_limit, overlay_quote_bbo, overlay_tot_volume, parse_aggressor, parse_change_type,
    parse_date_time_ns, parse_exch_hms_ns, parse_imbalance_side, parse_limit_price_e9,
    parse_price_e9, parse_reference_change_type, parse_seq_u64, parse_volume, period_meta_key,
    quote_has_complete_side, sanitize_quote_sides, tot_volume_from_cells, tradeday_yyyymmdd,
    validate_period, ColumnRules, EventKind, PeriodStatus, SlimAuction, SlimCorrection,
    SlimImbalance, SlimImpliedVol, SlimPriceLimit, SlimPricePrint, SlimQuote, SlimReferenceChange,
    SlimSettlement, SlimStatus, SlimSymbologyChange, SlimTotVolume, SlimTrade, CF_REPLAY_META,
    KIND_CME_AUCTION, KIND_CME_CORRECTION, KIND_CME_IMBALANCE, KIND_CME_IMPLIED_VOL,
    KIND_CME_PRICE_LIMIT, KIND_CME_PRICE_PRINT, KIND_CME_QUOTE, KIND_CME_SETTLEMENT,
    KIND_CME_SPECIAL, KIND_CME_STATUS, KIND_CME_TOT_VOLUME, KIND_CME_TRADE, KIND_REFERENCE_CHANGE,
    KIND_SYMBOLOGY_CHANGE,
};
use csv::{ByteRecord, StringRecord};
use csv_core::{ReadRecordResult, Reader as CoreCsvReader};
use flate2::read::MultiGzDecoder;
use log::{error, info, LevelFilter, Log, Metadata, Record};
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamilyDescriptor, DBWithThreadMode,
    MultiThreaded, Options, WriteBatch, WriteOptions,
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use zstd::stream::read::Decoder as ZstdDecoder;

type AllDb = DBWithThreadMode<MultiThreaded>;

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_replay_all")]
#[command(about = "Replay all TAS products into year+product RocksDB")]
struct Args {
    #[arg(long, default_value = "config/cme_tas_replay_all.toml")]
    config: PathBuf,
    #[arg(long)]
    max_source_rows: Option<u64>,
    /// Re-read only known repairable rows recorded in `unparsed_path`.
    #[arg(long)]
    repair_unparsed: bool,
    /// Delete one failed period's product column families before a full restart.
    #[arg(long)]
    reset_writing_period: bool,
    /// Recover the DB once and flush every column family so old WALs can retire.
    #[arg(long)]
    flush_all_column_families: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct ReplayConfig {
    data_root: PathBuf,
    #[serde(default)]
    shard_root: Option<PathBuf>,
    period: String,
    #[serde(default)]
    periods: Vec<String>,
    rocksdb_dir: PathBuf,
    #[serde(default)]
    rics: Vec<String>,
    #[serde(default)]
    part_index: usize,
    #[serde(default)]
    job_offset: usize,
    #[serde(default)]
    max_jobs: Option<usize>,
    #[serde(default = "default_workers")]
    workers: usize,
    /// Shared workers for parsing rows within each gzip part. Results retain source order.
    #[serde(default = "default_parse_workers")]
    parse_workers: usize,
    /// Number of physical CSV lines decompressed before one parallel parse pass.
    #[serde(default = "default_parse_chunk_rows")]
    parse_chunk_rows: usize,
    #[serde(default)]
    max_source_rows: Option<u64>,
    #[serde(default = "default_column_rules")]
    column_rules: PathBuf,
    #[serde(default = "default_progress_every")]
    progress_every: u64,
    #[serde(default = "default_log_path")]
    log_path: PathBuf,
    #[serde(default = "default_unparsed_path")]
    unparsed_path: PathBuf,
    #[serde(default = "default_unmatched_path")]
    unmatched_path: PathBuf,
    #[serde(default = "default_quote_fallback_path")]
    quote_fallback_path: PathBuf,
    #[serde(default)]
    max_tradedays: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayFormat {
    Gzip,
    ZstdShard,
}

#[derive(Debug, Clone)]
struct ReplayJob {
    period: String,
    original_part: u16,
    shard_index: Option<u32>,
    path: PathBuf,
    format: ReplayFormat,
    expected_header: Option<Arc<str>>,
}

impl ReplayJob {
    fn label(&self) -> String {
        match self.shard_index {
            Some(shard) => format!("part={} shard={shard}", self.original_part),
            None => format!("part={}", self.original_part),
        }
    }
}

fn default_progress_every() -> u64 {
    1_000_000
}

fn default_workers() -> usize {
    1
}

fn default_parse_workers() -> usize {
    1
}

fn default_parse_chunk_rows() -> usize {
    8_192
}

fn default_column_rules() -> PathBuf {
    PathBuf::from("../preprocess/lseg/tas_column_rules.json")
}

fn default_log_path() -> PathBuf {
    PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay_all.log")
}

fn default_unparsed_path() -> PathBuf {
    PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay_all_unparsed.log")
}

fn default_unmatched_path() -> PathBuf {
    PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay_all_unmatched.log")
}

fn default_quote_fallback_path() -> PathBuf {
    PathBuf::from(
        "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay_all_quote_exch_fallback.log",
    )
}

fn resolved_periods(config: &ReplayConfig) -> Result<Vec<String>> {
    let periods = if config.periods.is_empty() {
        vec![config.period.clone()]
    } else {
        config.periods.clone()
    };
    if periods.is_empty() {
        bail!("need period or periods");
    }
    for period in &periods {
        validate_period(period)?;
        let _ = period_year(period)?;
    }
    Ok(periods)
}

fn is_forbidden_legacy_rocksdb(path: &Path) -> bool {
    if path == Path::new(FORBIDDEN_LEGACY_ROCKSDB) {
        return true;
    }
    match (
        path.canonicalize(),
        Path::new(FORBIDDEN_LEGACY_ROCKSDB).canonicalize(),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn refuse_legacy_rocksdb(path: &Path) -> Result<()> {
    if is_forbidden_legacy_rocksdb(path) {
        bail!(
            "cme_tas_replay_all refuses the legacy RocksDB {}; use cme_tas_rocksdb_all_products",
            FORBIDDEN_LEGACY_ROCKSDB
        );
    }
    Ok(())
}

struct FileLogger {
    file: Mutex<File>,
}

impl Log for FileLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::Level::Info
            && (metadata.target().starts_with("cme_tas_replay_all")
                || metadata.target() == "cme_tas_replay_all")
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let line = format!(
            "[{} {}] {}\n",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            record.level(),
            record.args()
        );
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(line.as_bytes());
            let _ = file.flush();
        }
    }

    fn flush(&self) {
        if let Ok(mut file) = self.file.lock() {
            let _ = file.flush();
        }
    }
}

fn init_logger(path: &Path) {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).unwrap_or_else(|err| {
                panic!("create log dir {}: {err}", parent.display());
            });
        }
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|err| panic!("open log file {}: {err}", path.display()));
    let logger = FileLogger {
        file: Mutex::new(file),
    };
    let _ = log::set_boxed_logger(Box::new(logger));
    log::set_max_level(LevelFilter::Info);
}

struct ColIdx {
    ric: usize,
    date_time: usize,
    event_type: usize,
    price: usize,
    volume: usize,
    qualifiers: usize,
    bid: usize,
    bid_size: usize,
    ask: usize,
    ask_size: usize,
    exch_time: usize,
    up_lim: usize,
    lo_lim: usize,
    imp_vol: usize,
    bid_imp_vol: usize,
    ask_imp_vol: usize,
    total_volume: usize,
    acc_volume: usize,
    implied_yield: usize,
    imbalance_qty: usize,
    imbalance_side: usize,
    change_type: usize,
    old_value: usize,
    new_value: usize,
    source_date: usize,
    original_price: usize,
    original_volume: usize,
    original_seq: usize,
}

impl ColIdx {
    fn projected_sources(&self) -> [usize; 28] {
        [
            self.ric,
            self.date_time,
            self.event_type,
            self.price,
            self.volume,
            self.qualifiers,
            self.bid,
            self.bid_size,
            self.ask,
            self.ask_size,
            self.exch_time,
            self.up_lim,
            self.lo_lim,
            self.imp_vol,
            self.bid_imp_vol,
            self.ask_imp_vol,
            self.total_volume,
            self.acc_volume,
            self.implied_yield,
            self.imbalance_qty,
            self.imbalance_side,
            self.change_type,
            self.old_value,
            self.new_value,
            self.source_date,
            self.original_price,
            self.original_volume,
            self.original_seq,
        ]
    }
}

const PROJECTED_FIELD_CAPACITY: usize = 32;
const MISSING_FIELD_OFFSET: u32 = u32::MAX;
const WRITE_BATCH_OPS: usize = 8_192;

#[derive(Clone, Copy)]
struct FieldRange {
    start: u32,
    end: u32,
}

const MISSING_FIELD_RANGE: FieldRange = FieldRange {
    start: MISSING_FIELD_OFFSET,
    end: MISSING_FIELD_OFFSET,
};

enum DataRecord {
    Simple {
        ranges: [FieldRange; PROJECTED_FIELD_CAPACITY],
    },
    Quoted(StringRecord),
}

struct HeaderMap {
    names: Vec<String>,
    groups: Vec<String>,
    idx: ColIdx,
    forbidden_idxs: Vec<usize>,
    forbidden_mask: Vec<bool>,
    projected_slot_by_source: Vec<Option<u8>>,
}

impl HeaderMap {
    fn require_idx(by_name: &BTreeMap<String, usize>, name: &str) -> Result<usize> {
        by_name
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("TAS header missing required column {name}"))
    }

    fn from_headers(headers: &StringRecord, rules: &ColumnRules) -> Result<Self> {
        let mut names = Vec::with_capacity(headers.len());
        let mut groups = Vec::with_capacity(headers.len());
        let mut by_name = BTreeMap::new();
        for (index, name) in headers.iter().enumerate() {
            groups.push(rules.group_of(name)?.to_string());
            names.push(name.to_string());
            by_name.insert(name.to_string(), index);
        }
        for required in &rules.required_identity {
            if !by_name.contains_key(required) {
                bail!("TAS header missing required column {required}");
            }
        }
        let idx = ColIdx {
            ric: Self::require_idx(&by_name, "#RIC")?,
            date_time: Self::require_idx(&by_name, "Date-Time")?,
            event_type: Self::require_idx(&by_name, "Type")?,
            price: Self::require_idx(&by_name, "Price")?,
            volume: Self::require_idx(&by_name, "Volume")?,
            qualifiers: Self::require_idx(&by_name, "Qualifiers")?,
            bid: Self::require_idx(&by_name, "Bid Price")?,
            bid_size: Self::require_idx(&by_name, "Bid Size")?,
            ask: Self::require_idx(&by_name, "Ask Price")?,
            ask_size: Self::require_idx(&by_name, "Ask Size")?,
            exch_time: Self::require_idx(&by_name, "Exch Time")?,
            up_lim: Self::require_idx(&by_name, "UpLim Price")?,
            lo_lim: Self::require_idx(&by_name, "LoLim Price")?,
            imp_vol: Self::require_idx(&by_name, "Imp. Vol.")?,
            bid_imp_vol: Self::require_idx(&by_name, "Bid Imp. Vol")?,
            ask_imp_vol: Self::require_idx(&by_name, "Ask Imp. Vol")?,
            total_volume: Self::require_idx(&by_name, "Total Volume")?,
            acc_volume: Self::require_idx(&by_name, "Acc. Volume")?,
            implied_yield: Self::require_idx(&by_name, "Implied Yield")?,
            imbalance_qty: Self::require_idx(&by_name, "Imbalance Quantity")?,
            imbalance_side: Self::require_idx(&by_name, "Imbalance Side")?,
            change_type: Self::require_idx(&by_name, "Change Type")?,
            old_value: Self::require_idx(&by_name, "Old Value")?,
            new_value: Self::require_idx(&by_name, "New Value")?,
            source_date: Self::require_idx(&by_name, "Date")?,
            original_price: Self::require_idx(&by_name, "Original Price")?,
            original_volume: Self::require_idx(&by_name, "Original Volume")?,
            original_seq: Self::require_idx(&by_name, "Original Seq. No.")?,
        };
        let mut forbidden_idxs = Vec::new();
        for (index, name) in names.iter().enumerate() {
            let group = groups[index].as_str();
            if rules.is_forbidden_futures_group(group)
                && !rules.is_allowed_price_limit_column(name)
                && !rules.is_allowed_implied_yield_column(name)
                && !rules.is_allowed_implied_vol_column(name)
                && !rules.is_allowed_tot_volume_column(name)
                && !rules.is_allowed_imbalance_column(name)
            {
                forbidden_idxs.push(index);
            }
        }
        let mut forbidden_mask = vec![false; names.len()];
        for &index in &forbidden_idxs {
            forbidden_mask[index] = true;
        }
        let mut projected_sources = idx.projected_sources().to_vec();
        projected_sources.sort_unstable();
        projected_sources.dedup();
        if projected_sources.len() > PROJECTED_FIELD_CAPACITY {
            bail!(
                "TAS projection needs {} fields, capacity is {PROJECTED_FIELD_CAPACITY}",
                projected_sources.len()
            );
        }
        let mut projected_slot_by_source = vec![None; names.len()];
        for (slot, source) in projected_sources.iter().copied().enumerate() {
            projected_slot_by_source[source] = Some(slot as u8);
        }
        Ok(Self {
            names,
            groups,
            idx,
            forbidden_idxs,
            forbidden_mask,
            projected_slot_by_source,
        })
    }

    fn cell_at<'a>(&self, raw_line: &'a str, record: &'a DataRecord, idx: usize) -> &'a str {
        match record {
            DataRecord::Simple { ranges } => {
                let Some(slot) = self
                    .projected_slot_by_source
                    .get(idx)
                    .and_then(|slot| *slot)
                else {
                    return "";
                };
                let range = ranges[slot as usize];
                if range.start == MISSING_FIELD_OFFSET {
                    return "";
                }
                raw_line[range.start as usize..range.end as usize].trim()
            }
            DataRecord::Quoted(record) => record.get(idx).map(str::trim).unwrap_or(""),
        }
    }

    fn required_at<'a>(
        &self,
        raw_line: &'a str,
        record: &'a DataRecord,
        idx: usize,
        name: &str,
    ) -> Result<&'a str> {
        let value = self.cell_at(raw_line, record, idx);
        if value.is_empty() {
            bail!("unhandled empty required TAS field {name:?}");
        }
        Ok(value)
    }

    fn filled_cells_string(&self, record: &StringRecord) -> String {
        let mut out = String::new();
        for (index, name) in self.names.iter().enumerate() {
            let value = record.get(index).map(str::trim).unwrap_or("");
            if value.is_empty() {
                continue;
            }
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(name);
            out.push('=');
            out.push_str(value);
        }
        for extra in record.iter().skip(self.names.len()) {
            let value = extra.trim();
            if value.is_empty() {
                continue;
            }
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str("<extra>=");
            out.push_str(value);
        }
        if out.is_empty() {
            out.push_str("<empty>");
        }
        out
    }

    fn filled_cells(&self, raw_line: &str, record: &DataRecord) -> String {
        match record {
            DataRecord::Quoted(record) => self.filled_cells_string(record),
            DataRecord::Simple { .. } => record_from_line(raw_line)
                .map(|record| self.filled_cells_string(&record))
                .unwrap_or_else(|_| raw_line.to_string()),
        }
    }
}

const UNPARSED_ERROR_CAP: u64 = 20;
const UNPARSED_SIZE_ABORT: u64 = 2 * 1024 * 1024 * 1024;

struct UnparsedSink {
    file: Mutex<File>,
    dumped: AtomicU64,
    bytes: AtomicU64,
}

impl UnparsedSink {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create unparsed dir {}", parent.display()))?;
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open unparsed file {}", path.display()))?;
        let existing = file.metadata().map(|meta| meta.len()).unwrap_or(0);
        Ok(Self {
            file: Mutex::new(file),
            dumped: AtomicU64::new(0),
            bytes: AtomicU64::new(existing),
        })
    }

    fn dump(
        &self,
        period: &str,
        part: &Path,
        part_no: u16,
        source_row: u64,
        filled: &str,
        err: &anyhow::Error,
        abort: &AtomicBool,
    ) {
        let line = format!(
            "[{}] period={} part_no={} source_row={} part={} reason={} :: {}\n",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            period,
            part_no,
            source_row,
            part.display(),
            err,
            filled
        );
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(line.as_bytes());
        }
        let n = self.dumped.fetch_add(1, Ordering::Relaxed) + 1;
        let size = self.bytes.fetch_add(line.len() as u64, Ordering::Relaxed) + line.len() as u64;
        if n <= UNPARSED_ERROR_CAP {
            error!(
                "cme_tas_replay_all unparsed period={period} part_no={part_no} source_row={source_row} part={} {filled}",
                part.display()
            );
            if n == UNPARSED_ERROR_CAP {
                error!("cme_tas_replay_all further unparsed rows go only to the unparsed file");
            }
        }
        if size >= UNPARSED_SIZE_ABORT {
            if !abort.swap(true, Ordering::Relaxed) {
                error!(
                    "cme_tas_replay_all unparsed log reached {size} bytes; aborting at 2GiB for review"
                );
            }
        }
    }
}

struct UnmatchedSink {
    file: Mutex<File>,
    dumped: AtomicU64,
}

impl UnmatchedSink {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create unmatched dir {}", parent.display()))?;
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open unmatched file {}", path.display()))?;
        Ok(Self {
            file: Mutex::new(file),
            dumped: AtomicU64::new(0),
        })
    }

    fn dump(
        &self,
        period: &str,
        part: &Path,
        part_no: u16,
        source_row: u64,
        reason: &str,
        raw_line: &str,
    ) {
        let line = format!(
            "[{}] period={} part_no={} source_row={} part={} reason={} :: {}\n",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            period,
            part_no,
            source_row,
            part.display(),
            reason,
            raw_line
        );
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(line.as_bytes());
        }
        self.dumped.fetch_add(1, Ordering::Relaxed);
    }
}

fn parse_source_date_yyyymmdd(raw: &str) -> Result<u32> {
    if raw.is_empty() {
        return Ok(0);
    }
    let bytes = raw.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        bail!("unhandled Settlement Price Date {raw:?}");
    }
    let digits = [
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[5], bytes[6], bytes[8], bytes[9],
    ];
    if !digits.iter().all(u8::is_ascii_digit) {
        bail!("unhandled Settlement Price Date {raw:?}");
    }
    Ok(digits
        .into_iter()
        .fold(0u32, |value, digit| value * 10 + u32::from(digit - b'0')))
}

fn empty_correction_fields(
    price: &str,
    volume: &str,
    acc_volume: &str,
    original_price: &str,
    original_volume: &str,
    original_seq: &str,
    qualifiers: &str,
) -> bool {
    price.is_empty()
        && volume.is_empty()
        && acc_volume.is_empty()
        && original_price.is_empty()
        && original_volume.is_empty()
        && original_seq.is_empty()
        && qualifiers.is_empty()
}

#[derive(Default)]
struct Census {
    source_rows: u64,
    written_trades: u64,
    written_specials: u64,
    written_quotes: u64,
    written_renames: u64,
    written_limits: u64,
    written_settlements: u64,
    written_implied_vols: u64,
    written_tot_volumes: u64,
    written_price_prints: u64,
    written_imbalances: u64,
    written_auctions: u64,
    written_corrections: u64,
    written_statuses: u64,
    written_reference_changes: u64,
    counted: BTreeMap<&'static str, u64>,
    unmatched_product: u64,
    quote_exch_time_fallback: u64,
    quote_fallback_rics: BTreeMap<String, u64>,
    skipped_ric_filter: u64,
    skipped_after_window: u64,
    skipped_unparsed: u64,
    repaired_rows: u64,
}

impl Census {
    fn merge_from(&mut self, other: &Census) {
        self.source_rows += other.source_rows;
        self.written_trades += other.written_trades;
        self.written_specials += other.written_specials;
        self.written_quotes += other.written_quotes;
        self.written_renames += other.written_renames;
        self.written_limits += other.written_limits;
        self.written_settlements += other.written_settlements;
        self.written_implied_vols += other.written_implied_vols;
        self.written_tot_volumes += other.written_tot_volumes;
        self.written_price_prints += other.written_price_prints;
        self.written_imbalances += other.written_imbalances;
        self.written_auctions += other.written_auctions;
        self.written_corrections += other.written_corrections;
        self.written_statuses += other.written_statuses;
        self.written_reference_changes += other.written_reference_changes;
        self.unmatched_product += other.unmatched_product;
        self.quote_exch_time_fallback += other.quote_exch_time_fallback;
        self.skipped_ric_filter += other.skipped_ric_filter;
        self.skipped_after_window += other.skipped_after_window;
        self.skipped_unparsed += other.skipped_unparsed;
        self.repaired_rows += other.repaired_rows;
        for (name, count) in &other.counted {
            *self.counted.entry(name).or_insert(0) += count;
        }
        for (ric, count) in &other.quote_fallback_rics {
            *self.quote_fallback_rics.entry(ric.clone()).or_insert(0) += count;
        }
    }
}

fn write_quote_fallback_log(path: &Path, rics: &BTreeMap<String, u64>) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create quote fallback dir {}", parent.display()))?;
        }
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open quote fallback file {}", path.display()))?;
    let stamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ");
    writeln!(
        file,
        "[{stamp}] quote_exch_time_fallback unique_rics={} total_rows={}",
        rics.len(),
        rics.values().copied().sum::<u64>()
    )?;
    for (ric, count) in rics {
        let product = parse_product(ric).unwrap_or_else(|| ric.to_string());
        writeln!(file, "[{stamp}] ric={ric} product={product} rows={count}")?;
    }
    file.flush()?;
    Ok(())
}

fn discover_parts(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut parts = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read TAS period {}", dir.display()))? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.starts_with("merged-Data-part-") && name.ends_with(".csv.gz") {
            parts.push(path);
        }
    }
    parts.sort();
    if parts.is_empty() {
        bail!("no merged-Data-part-*.csv.gz under {}", dir.display());
    }
    Ok(parts)
}

fn leftover_staging_dir(final_dir: &Path) -> PathBuf {
    let name = final_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("rocksdb");
    final_dir.with_file_name(format!("{name}.building"))
}

fn dir_is_empty(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(true);
    }
    if !path.is_dir() {
        bail!(
            "rocksdb_dir {} exists and is not a directory",
            path.display()
        );
    }
    Ok(path
        .read_dir()
        .with_context(|| format!("read rocksdb_dir {}", path.display()))?
        .next()
        .is_none())
}

fn is_capped_run(config: &ReplayConfig) -> bool {
    config.max_source_rows.is_some() || config.max_tradedays > 0
}

fn product_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_write_buffer_size(16 * 1024 * 1024);
    opts.set_max_write_buffer_number(2);
    opts.set_min_write_buffer_number_to_merge(1);
    opts.set_level_zero_file_num_compaction_trigger(4);
    opts.set_level_zero_slowdown_writes_trigger(20);
    opts.set_level_zero_stop_writes_trigger(36);
    opts.set_merge_operator_associative("quote_last", quote_last_merge);
    opts
}

fn maintenance_cf_options(block_cache: &Cache) -> Options {
    let mut opts = product_cf_options();
    let mut table_options = BlockBasedOptions::default();
    table_options.set_block_cache(block_cache);
    table_options.set_cache_index_and_filter_blocks(true);
    table_options.set_pin_l0_filter_and_index_blocks_in_cache(false);
    table_options.set_pin_top_level_index_and_filter(false);
    opts.set_block_based_table_factory(&table_options);
    opts
}

fn open_rocksdb(path: &Path) -> Result<AllDb> {
    refuse_legacy_rocksdb(path)?;
    if path.exists() && !path.is_dir() {
        bail!(
            "rocksdb_dir {} exists and is not a directory",
            path.display()
        );
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create rocksdb parent {}", parent.display()))?;
    }
    let mut db_opts = Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    db_opts.set_max_open_files(65_536);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    db_opts.increase_parallelism(32);
    db_opts.set_max_background_jobs(32);
    db_opts.set_write_buffer_size(16 * 1024 * 1024);
    db_opts.set_max_write_buffer_number(2);
    db_opts.set_db_write_buffer_size(8 * 1024 * 1024 * 1024);
    db_opts.set_max_subcompactions(4);
    db_opts.set_merge_operator_associative("quote_last", quote_last_merge);
    let names = if path.is_dir() && path.read_dir()?.next().is_some() {
        AllDb::list_cf(&Options::default(), path)
            .with_context(|| format!("list column families {}", path.display()))?
    } else {
        vec!["default".to_string(), CF_REPLAY_META.to_string()]
    };
    for name in &names {
        if name != "default" && name != CF_REPLAY_META && !is_product_cf_name(name) {
            bail!(
                "RocksDB {} has unsupported column family {name:?}; use a new all-product directory",
                path.display()
            );
        }
    }
    let mut names = names;
    if !names.iter().any(|name| name == "default") {
        names.push("default".to_string());
    }
    if !names.iter().any(|name| name == CF_REPLAY_META) {
        names.push(CF_REPLAY_META.to_string());
    }
    let descriptors: Vec<ColumnFamilyDescriptor> = names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, product_cf_options()))
        .collect();
    AllDb::open_cf_descriptors(&db_opts, path, descriptors)
        .with_context(|| format!("open rocksdb {}", path.display()))
}

fn open_rocksdb_for_maintenance(path: &Path) -> Result<AllDb> {
    refuse_legacy_rocksdb(path)?;
    if !path.is_dir() {
        bail!("maintenance RocksDB {} is not a directory", path.display());
    }
    let names = AllDb::list_cf(&Options::default(), path)
        .with_context(|| format!("list maintenance column families {}", path.display()))?;
    for name in &names {
        if name != "default" && name != CF_REPLAY_META && !is_product_cf_name(name) {
            bail!(
                "RocksDB {} has unsupported column family {name:?}",
                path.display()
            );
        }
    }

    let mut db_opts = Options::default();
    db_opts.set_max_open_files(512);
    db_opts.set_max_file_opening_threads(8);
    db_opts.set_skip_stats_update_on_db_open(true);
    db_opts.increase_parallelism(32);
    db_opts.set_max_background_jobs(32);
    db_opts.set_db_write_buffer_size(8 * 1024 * 1024 * 1024);
    let block_cache = Cache::new_lru_cache(1024 * 1024 * 1024);
    let descriptors = names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, maintenance_cf_options(&block_cache)))
        .collect::<Vec<_>>();
    AllDb::open_cf_descriptors(&db_opts, path, descriptors)
        .with_context(|| format!("open maintenance RocksDB {}", path.display()))
}

fn flush_all_column_families(db: &AllDb, workers: usize) -> Result<usize> {
    let mut names = AllDb::list_cf(&Options::default(), db.path())
        .with_context(|| format!("list column families before flush {}", db.path().display()))?;
    names.sort();
    let completed = AtomicU64::new(0);
    let pool = ThreadPoolBuilder::new()
        .num_threads(workers.max(1).min(names.len().max(1)))
        .thread_name(|id| format!("cme-tas-cf-flush-{id}"))
        .build()
        .context("build all-column-family flush pool")?;
    pool.install(|| {
        names.par_iter().try_for_each(|name| -> Result<()> {
            let cf = db
                .cf_handle(name)
                .ok_or_else(|| anyhow!("column family {name:?} disappeared before flush"))?;
            db.flush_cf(&cf)
                .with_context(|| format!("flush column family {name}"))?;
            let count = completed.fetch_add(1, Ordering::Relaxed) + 1;
            if count % 100 == 0 || count as usize == names.len() {
                info!("flushed column families {count}/{}", names.len());
                eprintln!("flushed column families {count}/{}", names.len());
            }
            Ok(())
        })
    })?;
    db.flush_wal(true).context("sync WAL after all-CF flush")?;
    Ok(names.len())
}

fn maintenance_flush_all_column_families(config: &ReplayConfig) -> Result<()> {
    let started = Instant::now();
    eprintln!(
        "recovering maintenance RocksDB {} before all-CF flush",
        config.rocksdb_dir.display()
    );
    let db = open_rocksdb_for_maintenance(&config.rocksdb_dir)?;
    let meta = replay_meta_cf(&db)?;
    let mut done = 0_u64;
    for item in db.iterator_cf(&meta, rocksdb::IteratorMode::Start) {
        let (key, value) = item.context("scan replay watermarks before all-CF flush")?;
        if !key.starts_with(b"period:") {
            continue;
        }
        match decode_period_status(&value)? {
            PeriodStatus::Done => done += 1,
            PeriodStatus::Writing => bail!(
                "refuse all-CF flush while replay watermark {:?} is writing",
                String::from_utf8_lossy(&key)
            ),
        }
    }
    let flushed = flush_all_column_families(&db, 16)?;
    eprintln!(
        "all-CF flush complete column_families={flushed} done_periods={done} elapsed_s={:.1}",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn replay_meta_cf<'a>(db: &'a AllDb) -> Result<std::sync::Arc<BoundColumnFamily<'a>>> {
    db.cf_handle(CF_REPLAY_META)
        .ok_or_else(|| anyhow!("column family {CF_REPLAY_META} missing"))
}

fn claim_period(db: &AllDb, period: &str) -> Result<()> {
    validate_period(period)?;
    let cf = replay_meta_cf(db)?;
    let key = period_meta_key(period)?;
    match db
        .get_cf(&cf, &key)
        .with_context(|| format!("read period watermark {period}"))?
    {
        Some(bytes) => match decode_period_status(&bytes)? {
            PeriodStatus::Done => bail!(
                "period {period} is already done in this RocksDB; refuse to overwrite"
            ),
            PeriodStatus::Writing => bail!(
                "period {period} is marked writing; previous run did not finish. refuse to append into a half-written period"
            ),
        },
        None => {
            let mut opts = WriteOptions::default();
            opts.set_sync(true);
            db.put_cf_opt(
                &cf,
                &key,
                encode_period_status(PeriodStatus::Writing),
                &opts,
            )
            .with_context(|| format!("claim period {period}"))?;
            Ok(())
        }
    }
}

fn finish_period(db: &AllDb, period: &str) -> Result<()> {
    validate_period(period)?;
    let cf = replay_meta_cf(db)?;
    let key = period_meta_key(period)?;
    match db
        .get_cf(&cf, &key)
        .with_context(|| format!("read period watermark {period} before finish"))?
    {
        Some(bytes) => match decode_period_status(&bytes)? {
            PeriodStatus::Writing => {}
            PeriodStatus::Done => {
                bail!("period {period} is already done; refuse to mark done twice")
            }
        },
        None => bail!("period {period} missing writing watermark before finish"),
    }
    let mut opts = WriteOptions::default();
    opts.set_sync(true);
    db.put_cf_opt(&cf, &key, encode_period_status(PeriodStatus::Done), &opts)
        .with_context(|| format!("finish period {period}"))?;
    Ok(())
}

fn part_number(path: &Path) -> Result<u16> {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("part path {} has no file name", path.display()))?;
    let digits = name
        .strip_prefix("merged-Data-part-")
        .and_then(|rest| rest.strip_suffix(".csv.gz"))
        .ok_or_else(|| anyhow!("unrecognized TAS part name {name}"))?;
    digits
        .parse::<u16>()
        .map_err(|err| anyhow!("TAS part number {digits:?} in {name}: {err}"))
}

fn flush_batch(db: &AllDb, batch: &mut WriteBatch) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut opts = WriteOptions::default();
    opts.set_sync(false);
    db.write_opt(std::mem::take(batch), &opts)
        .context("write rocksdb batch")?;
    Ok(())
}

fn ensure_cf(db: &AllDb, lock: &Mutex<()>, name: &str) -> Result<()> {
    if db.cf_handle(name).is_some() {
        return Ok(());
    }
    let _guard = lock.lock().expect("column family create");
    if db.cf_handle(name).is_some() {
        return Ok(());
    }
    db.create_cf(name, &product_cf_options())
        .with_context(|| format!("create column family {name}"))?;
    Ok(())
}

fn product_cf<'a>(
    db: &'a AllDb,
    lock: &Mutex<()>,
    name: &str,
) -> Result<std::sync::Arc<BoundColumnFamily<'a>>> {
    ensure_cf(db, lock, name)?;
    db.cf_handle(name)
        .ok_or_else(|| anyhow!("column family {name} missing after create"))
}

fn record_from_line(line: &str) -> Result<StringRecord> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(line.as_bytes());
    match reader.records().next() {
        Some(Ok(record)) => Ok(record),
        Some(Err(err)) => Err(err.into()),
        None => bail!("empty TAS csv line"),
    }
}

struct CoreLineParser {
    reader: CoreCsvReader,
    output: Vec<u8>,
    ends: Vec<usize>,
}

impl CoreLineParser {
    fn new(field_capacity: usize) -> Self {
        Self {
            reader: CoreCsvReader::new(),
            output: Vec::new(),
            ends: vec![0; field_capacity.max(1)],
        }
    }

    fn parse(&mut self, line: &str) -> Result<StringRecord> {
        self.output.resize(line.len().max(1), 0);
        let mut input = line.as_bytes();
        let mut output_len = 0usize;
        let mut ends_len = 0usize;
        let mut finalizing = false;

        loop {
            let (status, input_read, output_written, ends_written) = self.reader.read_record(
                input,
                &mut self.output[output_len..],
                &mut self.ends[ends_len..],
            );
            input = &input[input_read..];
            output_len += output_written;
            ends_len += ends_written;

            match status {
                ReadRecordResult::InputEmpty => {
                    if !input.is_empty() {
                        bail!("csv-core left input after reporting InputEmpty");
                    }
                    if finalizing {
                        bail!("csv-core did not finish a physical TAS row");
                    }
                    finalizing = true;
                }
                ReadRecordResult::OutputFull => {
                    let next = self.output.len().saturating_mul(2).max(output_len + 1);
                    self.output.resize(next, 0);
                }
                ReadRecordResult::OutputEndsFull => {
                    let next = self.ends.len().saturating_mul(2).max(ends_len + 1);
                    self.ends.resize(next, 0);
                }
                ReadRecordResult::Record => {
                    if !input.is_empty() {
                        bail!("physical TAS line contained more than one CSV record");
                    }
                    let mut fields = Vec::with_capacity(ends_len);
                    let mut start = 0usize;
                    for &end in &self.ends[..ends_len] {
                        fields.push(&self.output[start..end]);
                        start = end;
                    }
                    let record = ByteRecord::from(fields);
                    return StringRecord::from_byte_record(record)
                        .map_err(|err| anyhow!("TAS row is not valid UTF-8: {err}"));
                }
                ReadRecordResult::End => bail!("empty TAS csv line"),
            }
        }
    }
}

fn parse_data_line(parser: &mut CoreLineParser, line: &str, map: &HeaderMap) -> Result<DataRecord> {
    if !line.as_bytes().contains(&b'"') {
        if line.len() > u32::MAX as usize {
            bail!("TAS physical row exceeds u32 byte offsets");
        }
        let mut ranges = [MISSING_FIELD_RANGE; PROJECTED_FIELD_CAPACITY];
        let mut start = 0usize;
        let mut first_forbidden = None;
        for (index, field) in line.split(',').enumerate() {
            let end = start + field.len();
            if index < map.names.len() {
                if map.forbidden_mask[index] && !field.trim().is_empty() {
                    first_forbidden.get_or_insert((index, start, end));
                }
                if let Some(slot) = map.projected_slot_by_source[index] {
                    ranges[slot as usize] = FieldRange {
                        start: start as u32,
                        end: end as u32,
                    };
                }
            } else if !field.trim().is_empty() {
                bail!(
                    "unhandled extra nonempty TAS cells after column {}",
                    map.names.len()
                );
            }
            start = end.saturating_add(1);
        }
        let ric_slot = map.projected_slot_by_source[map.idx.ric]
            .expect("#RIC must be part of the projected TAS fields")
            as usize;
        let ric_range = ranges[ric_slot];
        let ric = if ric_range.start == MISSING_FIELD_OFFSET {
            ""
        } else {
            line[ric_range.start as usize..ric_range.end as usize].trim()
        };
        if !ric.is_empty() && !ric.starts_with('.') {
            if let Some((index, field_start, field_end)) = first_forbidden {
                let value = line[field_start..field_end].trim();
                bail!(
                    "unhandled nonempty TAS column {:?}={value:?} on futures {ric} (group {})",
                    map.names[index],
                    map.groups[index]
                );
            }
        }
        return Ok(DataRecord::Simple { ranges });
    }
    parser.parse(line).map(DataRecord::Quoted)
}

struct ParsedSourceLine {
    raw_line: String,
    record: ParsedRecord,
}

enum PrecheckedRecord {
    UnmatchedProduct,
    Classified {
        cf_name: Arc<str>,
        kind: EventKind,
        quote_probe: Option<SlimQuote>,
        has_imbalance: bool,
        no_imbalance: bool,
        has_iv: bool,
        price_limit_ignored: bool,
        implied_yield_ignored: bool,
    },
}

enum ParsedRecord {
    Valid {
        record: DataRecord,
        prechecked: PrecheckedRecord,
    },
    InvalidCsv(anyhow::Error),
    InvalidRecord {
        record: DataRecord,
        error: anyhow::Error,
    },
}

fn precheck_record(
    raw_line: &str,
    record: &DataRecord,
    map: &HeaderMap,
    part: &Path,
    year: u16,
    route_cache: &mut Option<(String, Option<Arc<str>>)>,
) -> Result<PrecheckedRecord> {
    if let DataRecord::Quoted(record) = record {
        if record.len() > map.names.len() {
            for extra in record.iter().skip(map.names.len()) {
                if !extra.trim().is_empty() {
                    bail!(
                        "unhandled extra nonempty TAS cells after column {} in {}",
                        map.names.len(),
                        part.display()
                    );
                }
            }
        }
    }

    let ric = map.required_at(raw_line, record, map.idx.ric, "#RIC")?;
    if !ric.starts_with('.') && matches!(record, DataRecord::Quoted(_)) {
        for &idx in &map.forbidden_idxs {
            let value = map.cell_at(raw_line, record, idx);
            if !value.is_empty() {
                bail!(
                    "unhandled nonempty TAS column {:?}={value:?} on futures {ric} (group {})",
                    map.names[idx],
                    map.groups[idx]
                );
            }
        }
    }
    let cf_name = match route_cache {
        Some((cached_ric, cached_cf)) if cached_ric == ric => cached_cf.clone(),
        _ => {
            let routed = parse_product(ric)
                .map(|product| product_cf_name(year, &product).map(Arc::<str>::from))
                .transpose()?;
            *route_cache = Some((ric.to_string(), routed.clone()));
            routed
        }
    };
    let Some(cf_name) = cf_name else {
        return Ok(PrecheckedRecord::UnmatchedProduct);
    };
    let date_time = map.required_at(raw_line, record, map.idx.date_time, "Date-Time")?;
    let event_type = map.required_at(raw_line, record, map.idx.event_type, "Type")?;
    let price = map.cell_at(raw_line, record, map.idx.price);
    let volume = map.cell_at(raw_line, record, map.idx.volume);
    let qualifiers = map.cell_at(raw_line, record, map.idx.qualifiers);
    let up_lim = map.cell_at(raw_line, record, map.idx.up_lim);
    let lo_lim = map.cell_at(raw_line, record, map.idx.lo_lim);

    let mut kind = classify(ric, event_type, price, volume, qualifiers)
        .with_context(|| format!("classify {ric} {date_time} in {}", part.display()))?;
    if kind == EventKind::DropEmptyTrade {
        kind = overlay_price_limit(kind, up_lim, lo_lim)?;
    }
    kind = overlay_tot_volume(
        kind,
        ric,
        volume,
        map.cell_at(raw_line, record, map.idx.total_volume),
    )?;

    let imbalance_qty = map.cell_at(raw_line, record, map.idx.imbalance_qty);
    let imbalance_side = map.cell_at(raw_line, record, map.idx.imbalance_side);
    let no_imbalance = is_no_imbalance(imbalance_qty, imbalance_side)?;
    let has_imbalance = !no_imbalance && (!imbalance_qty.is_empty() || !imbalance_side.is_empty());
    let quote_probe = if event_type == "Quote" {
        let mut quote = SlimQuote {
            ric: String::new(),
            ts_utc_ns: 0,
            exch_hms_ns: 0,
            bid: parse_price_e9(map.cell_at(raw_line, record, map.idx.bid))?,
            bid_size: parse_volume(map.cell_at(raw_line, record, map.idx.bid_size))?,
            ask: parse_price_e9(map.cell_at(raw_line, record, map.idx.ask))?,
            ask_size: parse_volume(map.cell_at(raw_line, record, map.idx.ask_size))?,
        };
        sanitize_quote_sides(&mut quote);
        if kind == EventKind::CmeQuote && !quote_has_complete_side(&quote) {
            kind = EventKind::DropEmptyQuote;
        }
        Some(quote)
    } else {
        None
    };
    if !no_imbalance {
        kind = overlay_imbalance(kind, imbalance_qty, imbalance_side)?;
    }

    let price_limit_ignored =
        kind != EventKind::CmePriceLimit && (!up_lim.is_empty() || !lo_lim.is_empty());
    if price_limit_ignored && kind != EventKind::CmeQuote && kind != EventKind::DropEmptyQuote {
        let name = if !up_lim.is_empty() {
            "UpLim Price"
        } else {
            "LoLim Price"
        };
        let value = if !up_lim.is_empty() { up_lim } else { lo_lim };
        bail!(
            "unhandled nonempty TAS column {name:?}={value:?} on {} {ric} {date_time}",
            kind.as_str()
        );
    }

    let has_iv = !map.cell_at(raw_line, record, map.idx.imp_vol).is_empty()
        || !map
            .cell_at(raw_line, record, map.idx.bid_imp_vol)
            .is_empty()
        || !map
            .cell_at(raw_line, record, map.idx.ask_imp_vol)
            .is_empty();
    if kind == EventKind::CmeTotVolume && has_iv {
        bail!("TOT {ric} unexpectedly has implied vol at {date_time}");
    }
    let implied_yield = map.cell_at(raw_line, record, map.idx.implied_yield);
    let implied_yield_ignored = !implied_yield.is_empty();
    if implied_yield_ignored && kind != EventKind::CmeQuote && kind != EventKind::DropEmptyQuote {
        bail!(
            "unhandled nonempty TAS column \"Implied Yield\"={implied_yield:?} on {} {ric} {date_time}",
            kind.as_str()
        );
    }

    Ok(PrecheckedRecord::Classified {
        cf_name,
        kind,
        quote_probe,
        has_imbalance,
        no_imbalance,
        has_iv,
        price_limit_ignored,
        implied_yield_ignored,
    })
}

/// Read compressed output sequentially, but parse each bounded line chunk in
/// parallel. Indexed Rayon collection retains exact source order.
struct ParallelRecordReader<'a, R> {
    inner: R,
    pool: &'a ThreadPool,
    map: &'a HeaderMap,
    part: &'a Path,
    year: u16,
    chunk_rows: usize,
    pending: VecDeque<ParsedSourceLine>,
    eof: bool,
}

impl<'a, R: BufRead> ParallelRecordReader<'a, R> {
    fn new(
        inner: R,
        pool: &'a ThreadPool,
        map: &'a HeaderMap,
        part: &'a Path,
        year: u16,
        chunk_rows: usize,
    ) -> Result<Self> {
        if chunk_rows == 0 {
            bail!("parse_chunk_rows must be >= 1");
        }
        Ok(Self {
            inner,
            pool,
            map,
            part,
            year,
            chunk_rows,
            pending: VecDeque::new(),
            eof: false,
        })
    }

    fn next_line(&mut self) -> Result<Option<ParsedSourceLine>> {
        if let Some(parsed) = self.pending.pop_front() {
            return Ok(Some(parsed));
        }
        if self.eof {
            return Ok(None);
        }

        let mut raw_lines = Vec::with_capacity(self.chunk_rows);
        while raw_lines.len() < self.chunk_rows {
            let mut line = String::new();
            let read = self.inner.read_line(&mut line)?;
            if read == 0 {
                self.eof = true;
                break;
            }
            while matches!(line.as_bytes().last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            if !line.is_empty() {
                raw_lines.push(line);
            }
        }

        let field_capacity = self.map.names.len() + 16;
        let parsed: Vec<ParsedSourceLine> = self.pool.install(|| {
            raw_lines
                .into_par_iter()
                .map_init(
                    || (CoreLineParser::new(field_capacity), None),
                    |(parser, route_cache), raw_line| {
                        let record = match parse_data_line(parser, &raw_line, self.map) {
                            Ok(record) => match precheck_record(
                                &raw_line,
                                &record,
                                self.map,
                                self.part,
                                self.year,
                                route_cache,
                            ) {
                                Ok(prechecked) => ParsedRecord::Valid { record, prechecked },
                                Err(error) => ParsedRecord::InvalidRecord { record, error },
                            },
                            Err(error) => ParsedRecord::InvalidCsv(error),
                        };
                        ParsedSourceLine { raw_line, record }
                    },
                )
                .collect()
        });
        self.pending = parsed.into();
        Ok(self.pending.pop_front())
    }

    fn into_inner(self) -> R {
        self.inner
    }
}

struct OpenBbo {
    ric: String,
    cf_name: String,
    bucket_ns: u64,
    rec: SlimQuote,
}

fn next_kind_key(
    last_ric_ts: &mut Option<(String, u64, u32)>,
    last_key: &mut Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]>,
    kind: u8,
    ric: &str,
    ts_utc_ns: u64,
    part_no: u16,
    date_time: &str,
) -> Result<[u8; cme_tas_replay::product::ALL_KEY_LEN]> {
    let seq = match last_ric_ts {
        Some((prev_ric, prev_ts, prev_seq)) if prev_ric == ric && *prev_ts == ts_utc_ns => prev_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("seq overflow for {ric} at {date_time}"))?,
        _ => 0,
    };
    *last_ric_ts = Some((ric.to_string(), ts_utc_ns, seq));
    let key = encode_all_key(kind, ric, ts_utc_ns, part_no, seq)?;
    if last_key.is_some_and(|prev| key <= prev) {
        bail!("TAS key is not strictly increasing for {ric} at {date_time} in part {part_no}");
    }
    *last_key = Some(key);
    Ok(key)
}

fn persist_implied_vol(
    db: &AllDb,
    cf_lock: &Mutex<()>,
    batch: &mut WriteBatch,
    census: &mut Census,
    last_key: &mut Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]>,
    last_ric_ts: &mut Option<(String, u64, u32)>,
    ric: &str,
    cf_name: &str,
    ts_utc_ns: u64,
    part_no: u16,
    date_time: &str,
    exch_hms_ns: u64,
    last_iv: i64,
    bid_iv: i64,
    ask_iv: i64,
    source: u8,
) -> Result<()> {
    let rec = SlimImpliedVol {
        ric: ric.to_string(),
        ts_utc_ns,
        exch_hms_ns,
        last_iv,
        bid_iv,
        ask_iv,
        source,
    };
    let key = next_kind_key(
        last_ric_ts,
        last_key,
        KIND_CME_IMPLIED_VOL,
        ric,
        ts_utc_ns,
        part_no,
        date_time,
    )?;
    let cf = product_cf(db, cf_lock, cf_name)?;
    batch.put_cf(&cf, key, encode_cme_implied_vol(&rec)?);
    census.written_implied_vols += 1;
    if batch.len() >= WRITE_BATCH_OPS {
        flush_batch(db, batch)?;
    }
    Ok(())
}

fn persist_imbalance(
    db: &AllDb,
    cf_lock: &Mutex<()>,
    batch: &mut WriteBatch,
    census: &mut Census,
    last_key: &mut Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]>,
    last_ric_ts: &mut Option<(String, u64, u32)>,
    ric: &str,
    cf_name: &str,
    ts_utc_ns: u64,
    part_no: u16,
    date_time: &str,
    exch_hms_ns: u64,
    quantity: &str,
    side: &str,
) -> Result<()> {
    let rec = SlimImbalance {
        ric: ric.to_string(),
        ts_utc_ns,
        exch_hms_ns,
        quantity: parse_volume(quantity)?,
        side: parse_imbalance_side(side)?,
    };
    let key = next_kind_key(
        last_ric_ts,
        last_key,
        KIND_CME_IMBALANCE,
        ric,
        ts_utc_ns,
        part_no,
        date_time,
    )?;
    let cf = product_cf(db, cf_lock, cf_name)?;
    batch.put_cf(&cf, key, encode_cme_imbalance(&rec)?);
    census.written_imbalances += 1;
    if batch.len() >= WRITE_BATCH_OPS {
        flush_batch(db, batch)?;
    }
    Ok(())
}

fn flush_one_bbo(
    db: &AllDb,
    cf_lock: &Mutex<()>,
    batch: &mut WriteBatch,
    census: &mut Census,
    snap: &OpenBbo,
) -> Result<()> {
    let cf = product_cf(db, cf_lock, &snap.cf_name)?;
    let key = encode_all_key(KIND_CME_QUOTE, &snap.ric, snap.bucket_ns, 0, 0)?;
    batch.merge_cf(&cf, key, encode_cme_quote(&snap.rec)?);
    census.written_quotes += 1;
    if batch.len() >= WRITE_BATCH_OPS {
        flush_batch(db, batch)?;
    }
    Ok(())
}

fn flush_all_bbos(
    db: &AllDb,
    cf_lock: &Mutex<()>,
    batch: &mut WriteBatch,
    census: &mut Census,
    open: &mut BTreeMap<String, OpenBbo>,
) -> Result<()> {
    let snaps: Vec<OpenBbo> = std::mem::take(open).into_values().collect();
    for snap in snaps {
        flush_one_bbo(db, cf_lock, batch, census, &snap)?;
    }
    Ok(())
}

fn overlay_quote_within_bucket(
    previous: Option<&OpenBbo>,
    cf_name: &str,
    bucket_ns: u64,
    incoming: SlimQuote,
) -> SlimQuote {
    match previous {
        Some(previous) if previous.cf_name == cf_name && previous.bucket_ns == bucket_ns => {
            overlay_quote_bbo(&previous.rec, &incoming)
        }
        _ => incoming,
    }
}

fn apply_open_bbo(
    db: &AllDb,
    cf_lock: &Mutex<()>,
    batch: &mut WriteBatch,
    census: &mut Census,
    open: &mut BTreeMap<String, OpenBbo>,
    ric: &str,
    cf_name: &str,
    bucket_ns: u64,
    rec: SlimQuote,
) -> Result<()> {
    let bucket_changed = open
        .get(ric)
        .is_some_and(|prev| prev.cf_name != cf_name || prev.bucket_ns != bucket_ns);
    if bucket_changed {
        let snap = open
            .remove(ric)
            .ok_or_else(|| anyhow!("open BBO disappeared before bucket flush for {ric}"))?;
        flush_one_bbo(db, cf_lock, batch, census, &snap)?;
    }
    let rec = overlay_quote_within_bucket(open.get(ric), cf_name, bucket_ns, rec);
    open.insert(
        ric.to_string(),
        OpenBbo {
            ric: ric.to_string(),
            cf_name: cf_name.to_string(),
            bucket_ns,
            rec,
        },
    );
    Ok(())
}

enum ReplayDecoder {
    Gzip(MultiGzDecoder<BufReader<File>>),
    Zstd(ZstdDecoder<'static, BufReader<File>>),
}

impl Read for ReplayDecoder {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Gzip(decoder) => decoder.read(buffer),
            Self::Zstd(decoder) => decoder.read(buffer),
        }
    }
}

impl ReplayDecoder {
    fn compressed_position(self) -> Result<u64> {
        match self {
            Self::Gzip(decoder) => {
                let mut file = decoder.into_inner().into_inner();
                file.stream_position().context("tell gzip input")
            }
            Self::Zstd(decoder) => {
                let mut reader = decoder.finish();
                reader.stream_position().context("tell zstd input")
            }
        }
    }
}

fn replay_part(
    config: &ReplayConfig,
    parse_pool: &ThreadPool,
    db: &AllDb,
    cf_lock: &Mutex<()>,
    job: &ReplayJob,
    ric_filter: &Option<BTreeSet<String>>,
    abort: &AtomicBool,
    unparsed: &UnparsedSink,
    unmatched: &UnmatchedSink,
    selected_rows: Option<&BTreeSet<u64>>,
    repair_source_seq: bool,
) -> Result<Census> {
    let period = job.period.as_str();
    let part = job.path.as_path();
    let part_no = job.original_part;
    if abort.load(Ordering::Relaxed) {
        bail!("aborted before opening {}", part.display());
    }
    let rules = ColumnRules::load(&config.column_rules)?;
    let year = period_year(period)?;

    let compressed_len = fs::metadata(part)
        .with_context(|| format!("stat {}", part.display()))?
        .len();
    let file = File::open(part).with_context(|| format!("open {}", part.display()))?;
    let decoder = match job.format {
        ReplayFormat::Gzip => ReplayDecoder::Gzip(MultiGzDecoder::new(BufReader::with_capacity(
            16 * 1024 * 1024,
            file,
        ))),
        ReplayFormat::ZstdShard => ReplayDecoder::Zstd(
            ZstdDecoder::with_buffer(BufReader::with_capacity(16 * 1024 * 1024, file))
                .with_context(|| format!("open zstd decoder for {}", part.display()))?,
        ),
    };
    let mut lines = BufReader::with_capacity(16 * 1024 * 1024, decoder);
    let mut line = String::new();
    let embedded_header = job.format == ReplayFormat::ZstdShard || part_no == 0;
    let map = if embedded_header {
        line.clear();
        let n = lines
            .read_line(&mut line)
            .with_context(|| format!("read TAS header from {}", part.display()))?;
        if n == 0 {
            bail!("part 0 is empty");
        }
        let header_line = line.trim_end_matches(['\n', '\r']);
        if let Some(expected) = &job.expected_header {
            if header_line != expected.as_ref() {
                bail!(
                    "TAS shard header in {} does not match its manifest",
                    part.display()
                );
            }
        }
        let headers = record_from_line(header_line)?;
        if headers.get(0).map(str::trim) != Some("#RIC") {
            bail!(
                "part 0 first row is {:?}, expected TAS header starting with #RIC",
                headers.get(0)
            );
        }
        HeaderMap::from_headers(&headers, &rules)?
    } else {
        let header_part = period_dir_for(config, period).join("merged-Data-part-000000.csv.gz");
        let header_file = File::open(&header_part)
            .with_context(|| format!("open part 0 header {}", header_part.display()))?;
        let header_decoder =
            MultiGzDecoder::new(BufReader::with_capacity(1024 * 1024, header_file));
        let mut header_reader = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(header_decoder);
        let headers = header_reader
            .headers()
            .context("read TAS header from part 0 for later part")?
            .clone();
        HeaderMap::from_headers(&headers, &rules)?
    };

    let mut lines =
        ParallelRecordReader::new(lines, parse_pool, &map, part, year, config.parse_chunk_rows)?;
    let mut census = Census::default();
    let mut batch = WriteBatch::default();
    let mut last_trade_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_special_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_rename_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_limit_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_settle_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_iv_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_tot_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_print_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_imbalance_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_auction_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_correction_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_status_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_reference_key: Option<[u8; cme_tas_replay::product::ALL_KEY_LEN]> = None;
    let mut last_trade_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_special_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_rename_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_limit_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_settle_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_iv_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_tot_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_print_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_imbalance_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_auction_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_correction_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_status_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_reference_ric_ts: Option<(String, u64, u32)> = None;
    let mut open_bbos: BTreeMap<String, OpenBbo> = BTreeMap::new();
    let mut remaining = config.max_source_rows;
    let mut first_tradeday: Option<u32> = None;
    let mut last_tradeday: Option<u32> = None;
    let mut seen_tradedays: BTreeMap<u32, ()> = BTreeMap::new();
    let started = Instant::now();
    info!(
        "cme_tas_replay_all start period={} part={} part_no={} year={} extra_rics={:?} max_source_rows={:?} max_tradedays={} progress_every={}",
        period,
        part.display(),
        part_no,
        year,
        config.rics,
        config.max_source_rows,
        config.max_tradedays,
        config.progress_every
    );

    loop {
        if remaining.is_some_and(|left| left == 0) {
            break;
        }
        let Some(parsed_line) = lines
            .next_line()
            .with_context(|| format!("read TAS row from {}", part.display()))?
        else {
            break;
        };
        let ParsedSourceLine { raw_line, record } = parsed_line;
        census.source_rows += 1;
        if let Some(left) = remaining.as_mut() {
            *left = left.saturating_sub(1);
        }
        if census.source_rows % 100_000 == 0 && abort.load(Ordering::Relaxed) {
            bail!("aborted while reading {}", part.display());
        }
        let source_row = census.source_rows;
        if selected_rows.is_some_and(|rows| !rows.contains(&source_row)) {
            continue;
        }
        if selected_rows.is_some() {
            census.repaired_rows += 1;
        }
        let (record, prechecked) = match record {
            ParsedRecord::Valid { record, prechecked } => (record, prechecked),
            ParsedRecord::InvalidCsv(err) => {
                let cells = record_from_line(&raw_line)
                    .map(|record| map.filled_cells_string(&record))
                    .unwrap_or_else(|_| raw_line.to_string());
                unparsed.dump(period, part, part_no, source_row, &cells, &err, abort);
                census.skipped_unparsed += 1;
                *census.counted.entry("unparsed_skip").or_insert(0) += 1;
                if abort.load(Ordering::Relaxed) {
                    bail!("aborted because unparsed log exceeded 2GiB");
                }
                continue;
            }
            ParsedRecord::InvalidRecord { record, error } => {
                unparsed.dump(
                    period,
                    part,
                    part_no,
                    source_row,
                    &map.filled_cells(&raw_line, &record),
                    &error,
                    abort,
                );
                census.skipped_unparsed += 1;
                *census.counted.entry("unparsed_skip").or_insert(0) += 1;
                if abort.load(Ordering::Relaxed) {
                    bail!("aborted because unparsed log exceeded 2GiB");
                }
                continue;
            }
        };
        let PrecheckedRecord::Classified {
            cf_name,
            kind,
            mut quote_probe,
            has_imbalance,
            no_imbalance,
            has_iv,
            price_limit_ignored,
            implied_yield_ignored,
        } = prechecked
        else {
            unmatched.dump(
                period,
                part,
                part_no,
                source_row,
                "unmatched_product",
                &raw_line,
            );
            census.unmatched_product += 1;
            *census.counted.entry("unmatched_product").or_insert(0) += 1;
            continue;
        };
        let parsed = (|| -> Result<bool> {
            let ric = map.required_at(&raw_line, &record, map.idx.ric, "#RIC")?;
            let date_time = map.required_at(&raw_line, &record, map.idx.date_time, "Date-Time")?;
            let price = map.cell_at(&raw_line, &record, map.idx.price);
            let volume = map.cell_at(&raw_line, &record, map.idx.volume);
            let qualifiers = map.cell_at(&raw_line, &record, map.idx.qualifiers);
            let up_lim = map.cell_at(&raw_line, &record, map.idx.up_lim);
            let lo_lim = map.cell_at(&raw_line, &record, map.idx.lo_lim);
            let bid = map.cell_at(&raw_line, &record, map.idx.bid);
            let bid_size = map.cell_at(&raw_line, &record, map.idx.bid_size);
            let ask = map.cell_at(&raw_line, &record, map.idx.ask);
            let ask_size = map.cell_at(&raw_line, &record, map.idx.ask_size);
            let exch_time = map.cell_at(&raw_line, &record, map.idx.exch_time);

            let total_volume = map.cell_at(&raw_line, &record, map.idx.total_volume);
            let imbalance_qty = map.cell_at(&raw_line, &record, map.idx.imbalance_qty);
            let imbalance_side = map.cell_at(&raw_line, &record, map.idx.imbalance_side);
            if price_limit_ignored {
                *census.counted.entry("price_limit_ignored").or_insert(0) += 1;
            }
            let last_iv_raw = map.cell_at(&raw_line, &record, map.idx.imp_vol);
            let bid_iv_raw = map.cell_at(&raw_line, &record, map.idx.bid_imp_vol);
            let ask_iv_raw = map.cell_at(&raw_line, &record, map.idx.ask_imp_vol);
            if implied_yield_ignored {
                *census.counted.entry("implied_yield_ignored").or_insert(0) += 1;
            }
            *census.counted.entry(kind.as_str()).or_insert(0) += 1;
            if config.progress_every > 0 && census.source_rows % config.progress_every == 0 {
                let elapsed = started.elapsed().as_secs_f64().max(0.001);
                info!(
                    "cme_tas_replay_all progress period={} part_no={} source_rows={} written_trades={} written_specials={} written_quotes={} written_renames={} written_limits={} written_settlements={} written_implied_vols={} written_tot_volumes={} written_price_prints={} written_imbalances={} written_auctions={} written_corrections={} written_statuses={} written_reference_changes={} unmatched_product={} quote_exch_time_fallback={} skipped_unparsed={} last_ric={} last_ts={} kind={} rows_per_s={:.0} elapsed_s={:.1}",
                    period,
                    part_no,
                    census.source_rows,
                    census.written_trades,
                    census.written_specials,
                    census.written_quotes,
                    census.written_renames,
                    census.written_limits,
                    census.written_settlements,
                    census.written_implied_vols,
                    census.written_tot_volumes,
                    census.written_price_prints,
                    census.written_imbalances,
                    census.written_auctions,
                    census.written_corrections,
                    census.written_statuses,
                    census.written_reference_changes,
                    census.unmatched_product,
                    census.quote_exch_time_fallback,
                    census.skipped_unparsed,
                    ric,
                    date_time,
                    kind.as_str(),
                    census.source_rows as f64 / elapsed,
                    elapsed
                );
            }

            if ric_filter
                .as_ref()
                .is_some_and(|wanted| !wanted.contains(ric))
            {
                census.skipped_ric_filter += 1;
                return Ok(false);
            }
            if no_imbalance {
                *census.counted.entry("drop_no_imbalance").or_insert(0) += 1;
            }
            if kind == EventKind::CmeAuction && price.is_empty() {
                *census.counted.entry("drop_auction_no_price").or_insert(0) += 1;
                return Ok(false);
            }
            if kind == EventKind::CmeCorrection
                && empty_correction_fields(
                    price,
                    volume,
                    map.cell_at(&raw_line, &record, map.idx.acc_volume),
                    map.cell_at(&raw_line, &record, map.idx.original_price),
                    map.cell_at(&raw_line, &record, map.idx.original_volume),
                    map.cell_at(&raw_line, &record, map.idx.original_seq),
                    qualifiers,
                )
            {
                *census.counted.entry("drop_empty_correction").or_insert(0) += 1;
                return Ok(false);
            }

            let persist_kind = match kind {
                EventKind::IndexPrint if !price.is_empty() && !volume.is_empty() => {
                    EventKind::CmeTrade
                }
                EventKind::CmeTrade
                | EventKind::CmeSpecial
                | EventKind::CmeQuote
                | EventKind::SymbologyChange
                | EventKind::CmePriceLimit
                | EventKind::CmeSettlement
                | EventKind::CmeTotVolume
                | EventKind::CmePricePrint
                | EventKind::CmeImbalance
                | EventKind::CmeAuction
                | EventKind::CmeCorrection
                | EventKind::CmeStatus
                | EventKind::ReferenceChange
                | EventKind::DropSettleIv => kind,
                EventKind::DropEmptyQuote if has_iv => kind,
                _ => return Ok(false),
            };

            let ts_utc_ns = parse_date_time_ns(date_time)?;
            let tradeday = tradeday_yyyymmdd(ts_utc_ns)?;
            if first_tradeday.is_none() {
                first_tradeday = Some(tradeday);
                info!(
                    "cme_tas_replay_all first tradeday={tradeday} part_no={part_no} ric={ric} ts={date_time}"
                );
            }
            if !seen_tradedays.contains_key(&tradeday)
                && config.max_tradedays > 0
                && seen_tradedays.len() as u32 >= config.max_tradedays
            {
                info!(
                    "cme_tas_replay_all reached max_tradedays={} first={:?} this={} ric={ric} ts={date_time}; stop",
                    config.max_tradedays, first_tradeday, tradeday
                );
                census.skipped_after_window += 1;
                return Ok(true);
            }
            seen_tradedays.insert(tradeday, ());
            last_tradeday = Some(tradeday);

            let exch_hms_ns = parse_exch_hms_ns(exch_time)?;
            if persist_kind == EventKind::CmeQuote {
                let (bucket_ns, used_date_time) = quote_second_bucket_ns(ts_utc_ns, exch_hms_ns)?;
                if used_date_time {
                    census.quote_exch_time_fallback += 1;
                    *census
                        .quote_fallback_rics
                        .entry(ric.to_string())
                        .or_insert(0) += 1;
                    *census
                        .counted
                        .entry("quote_exch_time_fallback")
                        .or_insert(0) += 1;
                }
                let mut rec = quote_probe
                    .take()
                    .ok_or_else(|| anyhow!("Quote row missing parallel quote preparse"))?;
                rec.ric = ric.to_string();
                rec.ts_utc_ns = ts_utc_ns;
                rec.exch_hms_ns = exch_hms_ns;
                apply_open_bbo(
                    db,
                    cf_lock,
                    &mut batch,
                    &mut census,
                    &mut open_bbos,
                    ric,
                    &cf_name,
                    bucket_ns,
                    rec,
                )?;
                if has_iv {
                    persist_implied_vol(
                        db,
                        cf_lock,
                        &mut batch,
                        &mut census,
                        &mut last_iv_key,
                        &mut last_iv_ric_ts,
                        ric,
                        &cf_name,
                        ts_utc_ns,
                        part_no,
                        date_time,
                        exch_hms_ns,
                        parse_price_e9(last_iv_raw)?,
                        parse_price_e9(bid_iv_raw)?,
                        parse_price_e9(ask_iv_raw)?,
                        implied_vol_source(last_iv_raw, bid_iv_raw, ask_iv_raw, false)?,
                    )?;
                }
                if has_imbalance {
                    persist_imbalance(
                        db,
                        cf_lock,
                        &mut batch,
                        &mut census,
                        &mut last_imbalance_key,
                        &mut last_imbalance_ric_ts,
                        ric,
                        &cf_name,
                        ts_utc_ns,
                        part_no,
                        date_time,
                        exch_hms_ns,
                        imbalance_qty,
                        imbalance_side,
                    )?;
                }
                return Ok(false);
            }

            if persist_kind == EventKind::DropSettleIv || persist_kind == EventKind::DropEmptyQuote
            {
                if !has_iv {
                    return Ok(false);
                }
                persist_implied_vol(
                    db,
                    cf_lock,
                    &mut batch,
                    &mut census,
                    &mut last_iv_key,
                    &mut last_iv_ric_ts,
                    ric,
                    &cf_name,
                    ts_utc_ns,
                    part_no,
                    date_time,
                    exch_hms_ns,
                    parse_price_e9(last_iv_raw)?,
                    parse_price_e9(bid_iv_raw)?,
                    parse_price_e9(ask_iv_raw)?,
                    implied_vol_source(
                        last_iv_raw,
                        bid_iv_raw,
                        ask_iv_raw,
                        persist_kind == EventKind::DropSettleIv,
                    )?,
                )?;
                return Ok(false);
            }

            let last_ric_ts = match persist_kind {
                EventKind::CmeTrade => &mut last_trade_ric_ts,
                EventKind::CmeSpecial => &mut last_special_ric_ts,
                EventKind::SymbologyChange => &mut last_rename_ric_ts,
                EventKind::CmePriceLimit => &mut last_limit_ric_ts,
                EventKind::CmeSettlement => &mut last_settle_ric_ts,
                EventKind::CmeTotVolume => &mut last_tot_ric_ts,
                EventKind::CmePricePrint => &mut last_print_ric_ts,
                EventKind::CmeImbalance => &mut last_imbalance_ric_ts,
                EventKind::CmeAuction => &mut last_auction_ric_ts,
                EventKind::CmeCorrection => &mut last_correction_ric_ts,
                EventKind::CmeStatus => &mut last_status_ric_ts,
                EventKind::ReferenceChange => &mut last_reference_ric_ts,
                _ => unreachable!("quote and IV-only already handled"),
            };
            let kind_byte = match persist_kind {
                EventKind::CmeTrade => KIND_CME_TRADE,
                EventKind::CmeSpecial => KIND_CME_SPECIAL,
                EventKind::SymbologyChange => KIND_SYMBOLOGY_CHANGE,
                EventKind::CmePriceLimit => KIND_CME_PRICE_LIMIT,
                EventKind::CmeSettlement => KIND_CME_SETTLEMENT,
                EventKind::CmeTotVolume => KIND_CME_TOT_VOLUME,
                EventKind::CmePricePrint => KIND_CME_PRICE_PRINT,
                EventKind::CmeImbalance => KIND_CME_IMBALANCE,
                EventKind::CmeAuction => KIND_CME_AUCTION,
                EventKind::CmeCorrection => KIND_CME_CORRECTION,
                EventKind::CmeStatus => KIND_CME_STATUS,
                EventKind::ReferenceChange => KIND_REFERENCE_CHANGE,
                _ => unreachable!("quote and IV-only already handled"),
            };
            let last_key = match persist_kind {
                EventKind::CmeTrade => &mut last_trade_key,
                EventKind::CmeSpecial => &mut last_special_key,
                EventKind::SymbologyChange => &mut last_rename_key,
                EventKind::CmePriceLimit => &mut last_limit_key,
                EventKind::CmeSettlement => &mut last_settle_key,
                EventKind::CmeTotVolume => &mut last_tot_key,
                EventKind::CmePricePrint => &mut last_print_key,
                EventKind::CmeImbalance => &mut last_imbalance_key,
                EventKind::CmeAuction => &mut last_auction_key,
                EventKind::CmeCorrection => &mut last_correction_key,
                EventKind::CmeStatus => &mut last_status_key,
                EventKind::ReferenceChange => &mut last_reference_key,
                _ => unreachable!("quote and IV-only already handled"),
            };
            let key = if repair_source_seq {
                let repair_seq = u32::try_from(source_row).map_err(|_| {
                    anyhow!("repair source row {source_row} does not fit a TAS key sequence")
                })?;
                let key = encode_all_key(kind_byte, ric, ts_utc_ns, part_no, repair_seq)?;
                if last_key.is_some_and(|prev| key <= prev) {
                    bail!(
                        "repair TAS key is not strictly increasing for {ric} at {date_time} in part {part_no}"
                    );
                }
                *last_key = Some(key);
                key
            } else {
                next_kind_key(
                    last_ric_ts,
                    last_key,
                    kind_byte,
                    ric,
                    ts_utc_ns,
                    part_no,
                    date_time,
                )?
            };
            let cf = product_cf(db, cf_lock, &cf_name)?;
            match persist_kind {
                EventKind::CmeTrade => {
                    let rec = SlimTrade {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        exch_hms_ns,
                        price: parse_price_e9(price)?,
                        volume: parse_volume(volume)?,
                        bid: parse_price_e9(bid)?,
                        bid_size: parse_volume(bid_size)?,
                        ask: parse_price_e9(ask)?,
                        ask_size: parse_volume(ask_size)?,
                        aggressor: parse_aggressor(qualifiers)?,
                    };
                    batch.put_cf(&cf, key, encode_cme_trade(&rec)?);
                    census.written_trades += 1;
                }
                EventKind::CmeSpecial => {
                    let rec = SlimTrade {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        exch_hms_ns,
                        price: parse_price_e9(price)?,
                        volume: parse_volume(volume)?,
                        bid: parse_price_e9(bid)?,
                        bid_size: parse_volume(bid_size)?,
                        ask: parse_price_e9(ask)?,
                        ask_size: parse_volume(ask_size)?,
                        aggressor: parse_aggressor(qualifiers)?,
                    };
                    batch.put_cf(&cf, key, encode_cme_special(&rec)?);
                    census.written_specials += 1;
                }
                EventKind::SymbologyChange => {
                    let rec = SlimSymbologyChange {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        change_type: parse_change_type(map.cell_at(
                            &raw_line,
                            &record,
                            map.idx.change_type,
                        ))?,
                        old_value: map
                            .cell_at(&raw_line, &record, map.idx.old_value)
                            .to_string(),
                        new_value: map
                            .cell_at(&raw_line, &record, map.idx.new_value)
                            .to_string(),
                    };
                    batch.put_cf(&cf, key, encode_symbology_change(&rec)?);
                    census.written_renames += 1;
                }
                EventKind::CmePriceLimit => {
                    let rec = SlimPriceLimit {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        up_lim: parse_limit_price_e9(up_lim)?,
                        lo_lim: parse_limit_price_e9(lo_lim)?,
                    };
                    batch.put_cf(&cf, key, encode_cme_price_limit(&rec)?);
                    census.written_limits += 1;
                }
                EventKind::CmeSettlement => {
                    let rec = SlimSettlement {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        price: parse_price_e9(price)?,
                        source_date_yyyymmdd: parse_source_date_yyyymmdd(map.cell_at(
                            &raw_line,
                            &record,
                            map.idx.source_date,
                        ))?,
                    };
                    batch.put_cf(&cf, key, encode_cme_settlement(&rec)?);
                    census.written_settlements += 1;
                }
                EventKind::CmeTotVolume => {
                    let rec = SlimTotVolume {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        exch_hms_ns,
                        volume: tot_volume_from_cells(volume, total_volume)?,
                    };
                    batch.put_cf(&cf, key, encode_cme_tot_volume(&rec)?);
                    census.written_tot_volumes += 1;
                }
                EventKind::CmePricePrint => {
                    let rec = SlimPricePrint {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        exch_hms_ns,
                        price: parse_price_e9(price)?,
                    };
                    batch.put_cf(&cf, key, encode_cme_price_print(&rec)?);
                    census.written_price_prints += 1;
                }
                EventKind::CmeImbalance => {
                    let rec = SlimImbalance {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        exch_hms_ns,
                        quantity: parse_volume(imbalance_qty)?,
                        side: parse_imbalance_side(imbalance_side)?,
                    };
                    batch.put_cf(&cf, key, encode_cme_imbalance(&rec)?);
                    census.written_imbalances += 1;
                }
                EventKind::CmeAuction => {
                    let rec = SlimAuction {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        exch_hms_ns,
                        price: parse_price_e9(price)?,
                        volume: parse_volume(volume)?,
                    };
                    batch.put_cf(&cf, key, encode_cme_auction(&rec)?);
                    census.written_auctions += 1;
                }
                EventKind::CmeCorrection => {
                    let rec = SlimCorrection {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        exch_hms_ns,
                        price: parse_price_e9(price)?,
                        volume: parse_volume(volume)?,
                        acc_volume: parse_volume(map.cell_at(
                            &raw_line,
                            &record,
                            map.idx.acc_volume,
                        ))?,
                        original_price: parse_price_e9(map.cell_at(
                            &raw_line,
                            &record,
                            map.idx.original_price,
                        ))?,
                        original_volume: parse_volume(map.cell_at(
                            &raw_line,
                            &record,
                            map.idx.original_volume,
                        ))?,
                        original_seq: parse_seq_u64(map.cell_at(
                            &raw_line,
                            &record,
                            map.idx.original_seq,
                        ))?,
                        qualifiers: qualifiers.to_string(),
                    };
                    batch.put_cf(&cf, key, encode_cme_correction(&rec)?);
                    census.written_corrections += 1;
                }
                EventKind::CmeStatus => {
                    let rec = SlimStatus {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        exch_hms_ns,
                        qualifiers: qualifiers.to_string(),
                    };
                    batch.put_cf(&cf, key, encode_cme_status(&rec)?);
                    census.written_statuses += 1;
                }
                EventKind::ReferenceChange => {
                    let rec = SlimReferenceChange {
                        ric: ric.to_string(),
                        ts_utc_ns,
                        change_type: parse_reference_change_type(map.cell_at(
                            &raw_line,
                            &record,
                            map.idx.change_type,
                        ))?,
                        old_value: map
                            .cell_at(&raw_line, &record, map.idx.old_value)
                            .to_string(),
                        new_value: map
                            .cell_at(&raw_line, &record, map.idx.new_value)
                            .to_string(),
                    };
                    batch.put_cf(&cf, key, encode_reference_change(&rec)?);
                    census.written_reference_changes += 1;
                }
                _ => unreachable!("quote and IV-only already handled"),
            }
            if has_iv {
                persist_implied_vol(
                    db,
                    cf_lock,
                    &mut batch,
                    &mut census,
                    &mut last_iv_key,
                    &mut last_iv_ric_ts,
                    ric,
                    &cf_name,
                    ts_utc_ns,
                    part_no,
                    date_time,
                    exch_hms_ns,
                    parse_price_e9(last_iv_raw)?,
                    parse_price_e9(bid_iv_raw)?,
                    parse_price_e9(ask_iv_raw)?,
                    implied_vol_source(last_iv_raw, bid_iv_raw, ask_iv_raw, false)?,
                )?;
            }
            if batch.len() >= WRITE_BATCH_OPS {
                flush_batch(db, &mut batch)?;
            }
            Ok(false)
        })();
        match parsed {
            Ok(true) => break,
            Ok(false) => {}
            Err(err) => {
                unparsed.dump(
                    period,
                    part,
                    part_no,
                    source_row,
                    &map.filled_cells(&raw_line, &record),
                    &err,
                    abort,
                );
                census.skipped_unparsed += 1;
                *census.counted.entry("unparsed_skip").or_insert(0) += 1;
                if abort.load(Ordering::Relaxed) {
                    bail!("aborted because unparsed log exceeded 2GiB");
                }
            }
        }
    }
    flush_all_bbos(db, cf_lock, &mut batch, &mut census, &mut open_bbos)?;
    flush_batch(db, &mut batch)?;
    let lines = lines.into_inner();
    let decoder = lines.into_inner();
    let pos = decoder
        .compressed_position()
        .with_context(|| format!("tell {}", part.display()))?;
    if config.max_source_rows.is_none() && config.max_tradedays == 0 && pos < compressed_len {
        bail!(
            "compressed input ended at byte {pos} of {compressed_len} in {}",
            part.display()
        );
    }

    let elapsed_ms = started.elapsed().as_millis();
    info!(
        "cme_tas_replay_all finished period={} part={} part_no={} source_rows={} written_trades={} written_specials={} written_quotes={} written_renames={} written_limits={} written_settlements={} written_implied_vols={} written_tot_volumes={} written_price_prints={} written_imbalances={} written_auctions={} written_corrections={} written_statuses={} written_reference_changes={} unmatched_product={} quote_exch_time_fallback={} skipped_ric_filter={} skipped_unparsed={} skipped_after_window={} first_tradeday={:?} last_tradeday={:?} elapsed_ms={}",
        period,
        part.display(),
        part_no,
        census.source_rows,
        census.written_trades,
        census.written_specials,
        census.written_quotes,
        census.written_renames,
        census.written_limits,
        census.written_settlements,
        census.written_implied_vols,
        census.written_tot_volumes,
        census.written_price_prints,
        census.written_imbalances,
        census.written_auctions,
        census.written_corrections,
        census.written_statuses,
        census.written_reference_changes,
        census.unmatched_product,
        census.quote_exch_time_fallback,
        census.skipped_ric_filter,
        census.skipped_unparsed,
        census.skipped_after_window,
        first_tradeday,
        last_tradeday,
        elapsed_ms
    );
    for (name, count) in &census.counted {
        info!("cme_tas_replay_all class part_no={part_no} {name}={count}");
    }
    Ok(census)
}

fn period_dir_for(config: &ReplayConfig, period: &str) -> PathBuf {
    config.data_root.join(period_dir_name(period))
}

fn collect_gzip_jobs(config: &ReplayConfig, periods: &[String]) -> Result<Vec<ReplayJob>> {
    let mut jobs = Vec::new();
    for (idx, period) in periods.iter().enumerate() {
        let dir = period_dir_for(config, period);
        let all = discover_parts(&dir)?;
        if config.workers == 1 {
            if idx > 0 {
                continue;
            }
            let part = all.get(config.part_index).ok_or_else(|| {
                anyhow!(
                    "part_index {} out of range; found {} gzip parts in {period}",
                    config.part_index,
                    all.len()
                )
            })?;
            jobs.push(ReplayJob {
                period: period.clone(),
                original_part: part_number(part)?,
                shard_index: None,
                path: part.clone(),
                format: ReplayFormat::Gzip,
                expected_header: None,
            });
            continue;
        }
        for part in all {
            jobs.push(ReplayJob {
                period: period.clone(),
                original_part: part_number(&part)?,
                shard_index: None,
                path: part,
                format: ReplayFormat::Gzip,
                expected_header: None,
            });
        }
    }
    Ok(jobs)
}

fn collect_shard_jobs(config: &ReplayConfig, periods: &[String]) -> Result<Vec<ReplayJob>> {
    let root = config
        .shard_root
        .as_ref()
        .expect("collect_shard_jobs requires shard_root");
    let mut jobs = Vec::new();
    for (period_index, period) in periods.iter().enumerate() {
        let dir = root.join(period_dir_name(period));
        let manifest = TasShardManifest::load(&dir)?;
        manifest.validate(period, !is_capped_run(config))?;
        let expected_header = Arc::<str>::from(manifest.header.as_str());
        let selected_part = if config.workers == 1 {
            if period_index > 0 {
                continue;
            }
            Some(
                manifest
                    .sources
                    .get(config.part_index)
                    .ok_or_else(|| {
                        anyhow!(
                            "part_index {} out of range; found {} sharded source parts in {period}",
                            config.part_index,
                            manifest.sources.len()
                        )
                    })?
                    .original_part,
            )
        } else {
            None
        };
        for shard in &manifest.shards {
            if selected_part.is_some_and(|part| shard.original_part != part) {
                continue;
            }
            let path = dir.join(&shard.file);
            let actual_bytes = fs::metadata(&path)
                .with_context(|| format!("stat TAS shard {}", path.display()))?
                .len();
            if actual_bytes != shard.compressed_bytes {
                bail!(
                    "TAS shard {} is {actual_bytes} bytes, manifest expects {}",
                    path.display(),
                    shard.compressed_bytes
                );
            }
            jobs.push(ReplayJob {
                period: period.clone(),
                original_part: shard.original_part,
                shard_index: Some(shard.shard_index),
                path,
                format: ReplayFormat::ZstdShard,
                expected_header: Some(Arc::clone(&expected_header)),
            });
        }
    }
    Ok(jobs)
}

fn collect_jobs(config: &ReplayConfig, periods: &[String]) -> Result<Vec<ReplayJob>> {
    if config.workers == 0 {
        bail!("workers must be >= 1");
    }
    let jobs = if config.shard_root.is_some() {
        collect_shard_jobs(config, periods)?
    } else {
        collect_gzip_jobs(config, periods)?
    };
    if jobs.is_empty() {
        bail!("no TAS replay inputs for periods {periods:?}");
    }
    if config.job_offset >= jobs.len() {
        bail!(
            "job_offset {} out of range for {} TAS replay inputs",
            config.job_offset,
            jobs.len()
        );
    }
    let limited = jobs
        .into_iter()
        .skip(config.job_offset)
        .take(config.max_jobs.unwrap_or(usize::MAX))
        .collect::<Vec<_>>();
    if limited.is_empty() {
        bail!("max_jobs selected no TAS replay inputs");
    }
    Ok(limited)
}

fn repair_log_path(path: &Path, suffix: &str) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("replay.log");
    path.with_file_name(format!("{name}.{suffix}"))
}

/// Remove only one failed period from the all-product DB. Completed periods are
/// retained, and a second writing watermark makes the operation refuse rather
/// than guessing which writer owns the database.
fn reset_writing_period(config: &ReplayConfig) -> Result<()> {
    refuse_legacy_rocksdb(&config.rocksdb_dir)?;
    let periods = resolved_periods(config)?;
    if periods.len() != 1 {
        bail!("reset_writing_period requires exactly one configured period");
    }
    let period = &periods[0];
    let year = period_year(period)?;
    let period_key = period_meta_key(period)?;
    let names = AllDb::list_cf(&Options::default(), &config.rocksdb_dir).with_context(|| {
        format!(
            "list column families before reset {}",
            config.rocksdb_dir.display()
        )
    })?;
    let prefix = format!("p:{year}:");
    let drop_names = names
        .iter()
        .filter(|name| name.starts_with(&prefix))
        .cloned()
        .collect::<Vec<_>>();
    let db = open_rocksdb(&config.rocksdb_dir)?;
    let meta = replay_meta_cf(&db)?;
    let mut requested_seen = false;
    for item in db.iterator_cf(&meta, rocksdb::IteratorMode::Start) {
        let (key, value) = item.context("scan replay watermarks before reset")?;
        if !key.starts_with(b"period:") {
            continue;
        }
        match decode_period_status(&value)? {
            PeriodStatus::Done => {}
            PeriodStatus::Writing if key.as_ref() == period_key.as_slice() => {
                requested_seen = true;
            }
            PeriodStatus::Writing => bail!(
                "refuse reset: unrelated in-progress replay watermark {:?} exists",
                String::from_utf8_lossy(&key)
            ),
        }
    }
    if !requested_seen {
        bail!("refuse reset: {period} is not marked writing");
    }
    for name in &drop_names {
        db.drop_cf(name)
            .with_context(|| format!("drop failed period column family {name}"))?;
    }
    let mut batch = WriteBatch::default();
    batch.delete_cf(&meta, &period_key);
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options)
        .with_context(|| format!("delete failed replay watermark {period}"))?;
    db.flush_cf(&meta)
        .context("flush replay metadata after period reset")?;
    info!(
        "cme_tas_replay_all reset period={} dropped_product_cfs={} rocksdb={}",
        period,
        drop_names.len(),
        config.rocksdb_dir.display()
    );
    Ok(())
}

fn repairable_unparsed_reason(reason: &str) -> bool {
    (reason.starts_with("auction ")
        && (reason.ends_with(" missing Volume") || reason.ends_with(" missing Price")))
        || reason == "unhandled Imbalance Side \"N\""
        || (reason.starts_with("correction ")
            && reason.ends_with(" has no price, volume, original fields, or qualifiers"))
        || (reason.starts_with("status Qualifiers ")
            && reason.ends_with(" longer than 152 bytes; refuse to truncate"))
}

fn repair_targets(
    path: &Path,
    periods: &[String],
) -> Result<BTreeMap<(String, u16, PathBuf), BTreeSet<u64>>> {
    let requested = periods.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let file = File::open(path)
        .with_context(|| format!("open unparsed repair input {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut targets = BTreeMap::<(String, u16, PathBuf), BTreeSet<u64>>::new();
    for line in reader.lines() {
        let line =
            line.with_context(|| format!("read unparsed repair input {}", path.display()))?;
        let Some((metadata, _filled)) = line.split_once(" :: ") else {
            continue;
        };
        let Some(period) = metadata
            .split_once("period=")
            .and_then(|(_, rest)| rest.split_once(" part_no=").map(|(value, _)| value))
        else {
            continue;
        };
        if !requested.contains(period) {
            continue;
        }
        let Some(part_no) = metadata
            .split_once("part_no=")
            .and_then(|(_, rest)| rest.split_once(" source_row=").map(|(value, _)| value))
        else {
            continue;
        };
        let Some(source_row) = metadata
            .split_once("source_row=")
            .and_then(|(_, rest)| rest.split_once(" part=").map(|(value, _)| value))
        else {
            continue;
        };
        let Some(part) = metadata
            .split_once("part=")
            .and_then(|(_, rest)| rest.split_once(" reason=").map(|(value, _)| value))
        else {
            continue;
        };
        let Some(reason) = metadata.split_once("reason=").map(|(_, value)| value) else {
            continue;
        };
        if !repairable_unparsed_reason(reason) {
            continue;
        }
        let part_no = part_no
            .parse::<u16>()
            .with_context(|| format!("parse repair part number {part_no:?}"))?;
        let source_row = source_row
            .parse::<u64>()
            .with_context(|| format!("parse repair source row {source_row:?}"))?;
        targets
            .entry((period.to_string(), part_no, PathBuf::from(part)))
            .or_default()
            .insert(source_row);
    }
    Ok(targets)
}

fn repair_unparsed(config: &ReplayConfig) -> Result<()> {
    refuse_legacy_rocksdb(&config.rocksdb_dir)?;
    let periods = resolved_periods(config)?;
    if config.workers == 0 || config.parse_workers == 0 || config.parse_chunk_rows == 0 {
        bail!("repair requires workers, parse_workers, and parse_chunk_rows >= 1");
    }
    let targets = repair_targets(&config.unparsed_path, &periods)?;
    let expected_rows = targets.values().map(|rows| rows.len() as u64).sum::<u64>();
    if expected_rows == 0 {
        bail!(
            "no repairable rows for periods {periods:?} in {}",
            config.unparsed_path.display()
        );
    }
    let parse_pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(config.parse_workers)
            .thread_name(|id| format!("cme-tas-repair-parse-{id}"))
            .build()
            .context("build TAS repair parse pool")?,
    );
    let repair_errors = repair_log_path(&config.unparsed_path, "repair-errors.log");
    let repair_unmatched = repair_log_path(&config.unmatched_path, "repair-unmatched.log");
    let unparsed = Arc::new(UnparsedSink::open(&repair_errors)?);
    let unmatched = Arc::new(UnmatchedSink::open(&repair_unmatched)?);
    let db = Arc::new(open_rocksdb(&config.rocksdb_dir)?);
    let cf_lock = Arc::new(Mutex::new(()));
    let abort = Arc::new(AtomicBool::new(false));
    let mut census = Census::default();
    let started = Instant::now();
    let target_shards = targets.len();
    info!(
        "cme_tas_replay_all repair start periods={periods:?} target_rows={expected_rows} target_shards={} rocksdb={}",
        target_shards,
        config.rocksdb_dir.display()
    );
    let (job_tx, job_rx) = crossbeam_channel::unbounded();
    for ((period, part_no, path), rows) in targets {
        job_tx
            .send((
                ReplayJob {
                    period,
                    original_part: part_no,
                    shard_index: None,
                    path,
                    format: ReplayFormat::ZstdShard,
                    expected_header: None,
                },
                rows,
            ))
            .map_err(|_| anyhow!("TAS repair job queue closed before start"))?;
    }
    drop(job_tx);

    let worker_n = config.workers.min(target_shards).max(1);
    let mut handles = Vec::with_capacity(worker_n);
    for worker_id in 0..worker_n {
        let parse_pool = Arc::clone(&parse_pool);
        let db = Arc::clone(&db);
        let cf_lock = Arc::clone(&cf_lock);
        let abort = Arc::clone(&abort);
        let unparsed = Arc::clone(&unparsed);
        let unmatched = Arc::clone(&unmatched);
        let job_rx = job_rx.clone();
        let config = config.clone();
        handles.push(
            thread::Builder::new()
                .name(format!("cme-tas-repair-{worker_id}"))
                .spawn(move || -> Result<Census> {
                    let mut local = Census::default();
                    while let Ok((job, rows)) = job_rx.recv() {
                        info!(
                            "cme_tas_replay_all repair worker={worker_id} claimed period={} {} path={}",
                            job.period,
                            job.label(),
                            job.path.display()
                        );
                        match replay_part(
                            &config,
                            &parse_pool,
                            &db,
                            &cf_lock,
                            &job,
                            &None,
                            &abort,
                            &unparsed,
                            &unmatched,
                            Some(&rows),
                            true,
                        ) {
                            Ok(part_census) => local.merge_from(&part_census),
                            Err(err) => {
                                abort.store(true, Ordering::Relaxed);
                                return Err(err.context(format!(
                                    "repair worker {worker_id} failed on period {} {} {}",
                                    job.period,
                                    job.label(),
                                    job.path.display()
                                )));
                            }
                        }
                    }
                    Ok(local)
                })
                .context("spawn TAS repair worker")?,
        );
    }
    drop(job_rx);

    let mut first_err: Option<anyhow::Error> = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(part_census)) => census.merge_from(&part_census),
            Ok(Err(err)) => {
                abort.store(true, Ordering::Relaxed);
                let aborted = format!("{err:#}").contains("aborted");
                match &first_err {
                    None => first_err = Some(err),
                    Some(prev) if format!("{prev:#}").contains("aborted") && !aborted => {
                        first_err = Some(err);
                    }
                    _ => {}
                }
            }
            Err(_) => {
                abort.store(true, Ordering::Relaxed);
                if first_err.is_none() {
                    first_err = Some(anyhow!("TAS repair worker panicked"));
                }
            }
        }
    }
    if let Some(err) = first_err {
        return Err(err);
    }

    flush_all_column_families(&db, 16).context("flush repaired RocksDB records")?;
    if census.repaired_rows != expected_rows {
        bail!(
            "repair visited {} rows, expected {expected_rows}",
            census.repaired_rows
        );
    }
    if census.skipped_unparsed != 0 || census.unmatched_product != 0 {
        bail!(
            "repair left unparsed={} unmatched={}; see {} and {}",
            census.skipped_unparsed,
            census.unmatched_product,
            repair_errors.display(),
            repair_unmatched.display()
        );
    }
    info!(
        "cme_tas_replay_all repair finished target_rows={} written_auctions={} drop_no_imbalance={} elapsed_ms={}",
        census.repaired_rows,
        census.written_auctions,
        census.counted.get("drop_no_imbalance").copied().unwrap_or(0),
        started.elapsed().as_millis()
    );
    Ok(())
}

fn replay(config: &ReplayConfig) -> Result<()> {
    refuse_legacy_rocksdb(&config.rocksdb_dir)?;
    let periods = resolved_periods(config)?;
    let jobs = collect_jobs(config, &periods)?;
    if config.parse_workers == 0 {
        bail!("parse_workers must be >= 1");
    }
    if config.parse_chunk_rows == 0 {
        bail!("parse_chunk_rows must be >= 1");
    }
    let parse_pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(config.parse_workers)
            .thread_name(|id| format!("cme-tas-row-parse-{id}"))
            .build()
            .context("build TAS row parse pool")?,
    );
    let ric_filter: Option<BTreeSet<String>> = if config.rics.is_empty() {
        None
    } else {
        Some(config.rics.iter().cloned().collect())
    };
    let final_dir = config.rocksdb_dir.clone();
    let leftover = leftover_staging_dir(&final_dir);
    if leftover.exists() {
        bail!(
            "leftover staging {} exists; refuse to append until it is inspected and removed",
            leftover.display()
        );
    }
    let capped = is_capped_run(config);
    if capped && !dir_is_empty(&final_dir)? {
        bail!(
            "refuse partial replay (max_source_rows/max_tradedays) into nonempty {}; use an empty throwaway dir",
            final_dir.display()
        );
    }

    let started = Instant::now();
    info!(
        "cme_tas_replay_all start workers={} parse_workers={} parse_chunk_rows={} parts={} rocksdb={} periods={:?} capped={} max_tradedays={} unparsed={} unmatched={} quote_fallback={}",
        config.workers,
        config.parse_workers,
        config.parse_chunk_rows,
        jobs.len(),
        final_dir.display(),
        periods,
        capped,
        config.max_tradedays,
        config.unparsed_path.display(),
        config.unmatched_path.display(),
        config.quote_fallback_path.display()
    );

    let db = open_rocksdb(&final_dir)?;
    if !capped {
        for period in &periods {
            claim_period(&db, period)?;
        }
    }
    let db = Arc::new(db);
    let cf_lock = Arc::new(Mutex::new(()));
    let abort = Arc::new(AtomicBool::new(false));
    let unparsed = Arc::new(UnparsedSink::open(&config.unparsed_path)?);
    let unmatched = Arc::new(UnmatchedSink::open(&config.unmatched_path)?);
    let worker_n = config.workers.min(jobs.len()).max(1);
    let (job_tx, job_rx) = crossbeam_channel::unbounded();
    for job in jobs.iter().cloned() {
        job_tx
            .send(job)
            .map_err(|_| anyhow!("TAS replay job queue closed before start"))?;
    }
    drop(job_tx);

    let mut handles = Vec::with_capacity(worker_n);
    for worker_id in 0..worker_n {
        let db = Arc::clone(&db);
        let cf_lock = Arc::clone(&cf_lock);
        let abort = Arc::clone(&abort);
        let unparsed = Arc::clone(&unparsed);
        let unmatched = Arc::clone(&unmatched);
        let parse_pool = Arc::clone(&parse_pool);
        let job_rx = job_rx.clone();
        let config = config.clone();
        let ric_filter = ric_filter.clone();
        handles.push(
            thread::Builder::new()
                .name(format!("cme-tas-shard-replay-{worker_id}"))
                .spawn(move || -> Result<Census> {
                    let mut local = Census::default();
                    while let Ok(job) = job_rx.recv() {
                        info!(
                            "cme_tas_replay_all worker={worker_id} claimed period={} {} path={}",
                            job.period,
                            job.label(),
                            job.path.display()
                        );
                        match replay_part(
                            &config,
                            &parse_pool,
                            &db,
                            &cf_lock,
                            &job,
                            &ric_filter,
                            &abort,
                            &unparsed,
                            &unmatched,
                            None,
                            false,
                        ) {
                            Ok(part_census) => local.merge_from(&part_census),
                            Err(err) => {
                                abort.store(true, Ordering::Relaxed);
                                let err = err.context(format!(
                                    "worker {worker_id} failed on period {} {} {}",
                                    job.period,
                                    job.label(),
                                    job.path.display()
                                ));
                                error!("cme_tas_replay_all {err:#}");
                                return Err(err);
                            }
                        }
                    }
                    Ok(local)
                })
                .context("spawn TAS shard replay worker")?,
        );
    }
    drop(job_rx);

    let mut census = Census::default();
    let mut first_err: Option<anyhow::Error> = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(part_census)) => census.merge_from(&part_census),
            Ok(Err(err)) => {
                abort.store(true, Ordering::Relaxed);
                let aborted = format!("{err:#}").contains("aborted");
                match &first_err {
                    None => first_err = Some(err),
                    Some(prev) if format!("{prev:#}").contains("aborted") && !aborted => {
                        first_err = Some(err);
                    }
                    _ => {}
                }
            }
            Err(_) => {
                abort.store(true, Ordering::Relaxed);
                if first_err.is_none() {
                    first_err = Some(anyhow!("TAS shard replay worker panicked"));
                }
            }
        }
    }
    if let Some(err) = first_err {
        drop(db);
        return Err(err);
    }

    flush_all_column_families(&db, 16).context("flush all replay column families")?;
    if !capped {
        for period in &periods {
            finish_period(&db, period)?;
        }
        let meta = replay_meta_cf(&db)?;
        db.flush_cf(&meta)
            .context("flush completed replay watermarks")?;
        db.flush_wal(true)
            .context("sync WAL after completed replay watermarks")?;
    }
    drop(db);

    if !census.quote_fallback_rics.is_empty() {
        write_quote_fallback_log(&config.quote_fallback_path, &census.quote_fallback_rics)?;
        info!(
            "cme_tas_replay_all quote_exch_time_fallback unique_rics={} total_rows={} path={}",
            census.quote_fallback_rics.len(),
            census.quote_exch_time_fallback,
            config.quote_fallback_path.display()
        );
    }

    let elapsed_ms = started.elapsed().as_millis();
    info!(
        "cme_tas_replay_all finished workers={} parts={} periods={:?} source_rows={} written_trades={} written_specials={} written_quotes={} written_renames={} written_limits={} written_settlements={} written_implied_vols={} written_tot_volumes={} written_price_prints={} written_imbalances={} written_auctions={} written_corrections={} written_statuses={} written_reference_changes={} unmatched_product={} quote_exch_time_fallback={} skipped_ric_filter={} skipped_unparsed={} skipped_after_window={} elapsed_ms={}",
        config.workers,
        jobs.len(),
        periods,
        census.source_rows,
        census.written_trades,
        census.written_specials,
        census.written_quotes,
        census.written_renames,
        census.written_limits,
        census.written_settlements,
        census.written_implied_vols,
        census.written_tot_volumes,
        census.written_price_prints,
        census.written_imbalances,
        census.written_auctions,
        census.written_corrections,
        census.written_statuses,
        census.written_reference_changes,
        census.unmatched_product,
        census.quote_exch_time_fallback,
        census.skipped_ric_filter,
        census.skipped_unparsed,
        census.skipped_after_window,
        elapsed_ms
    );
    for (name, count) in &census.counted {
        info!("cme_tas_replay_all class {name}={count}");
    }
    println!(
        "cme_tas_replay_all finished workers={} parts={} periods={:?} source_rows={} written_trades={} written_specials={} written_quotes={} written_renames={} written_limits={} written_settlements={} written_implied_vols={} written_tot_volumes={} written_price_prints={} written_imbalances={} written_auctions={} written_corrections={} written_statuses={} written_reference_changes={} unmatched_product={} quote_exch_time_fallback={} skipped_ric_filter={} skipped_unparsed={} skipped_after_window={} elapsed_ms={}",
        config.workers,
        jobs.len(),
        periods,
        census.source_rows,
        census.written_trades,
        census.written_specials,
        census.written_quotes,
        census.written_renames,
        census.written_limits,
        census.written_settlements,
        census.written_implied_vols,
        census.written_tot_volumes,
        census.written_price_prints,
        census.written_imbalances,
        census.written_auctions,
        census.written_corrections,
        census.written_statuses,
        census.written_reference_changes,
        census.unmatched_product,
        census.quote_exch_time_fallback,
        census.skipped_ric_filter,
        census.skipped_unparsed,
        census.skipped_after_window,
        elapsed_ms
    );
    println!("classes");
    for (name, count) in &census.counted {
        println!("  {name}={count}");
    }
    Ok(())
}

fn install_panic_hook(log_path: PathBuf) {
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let name = thread.name().unwrap_or("unnamed");
        let loc = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown".to_string());
        let payload = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Box<dyn Any>".to_string()
        };
        let line = format!(
            "[{} ERROR] cme_tas_replay_all panic thread={name} at {loc}: {payload}\n",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ")
        );
        let _ = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .and_then(|mut file| {
                file.write_all(line.as_bytes())?;
                file.flush()
            });
        eprint!("{line}");
    }));
}

fn main() {
    let args = Args::parse();
    let content = fs::read_to_string(&args.config).unwrap_or_else(|err| {
        panic!("read replay config {}: {err}", args.config.display());
    });
    let mut config: ReplayConfig = toml::from_str(&content).unwrap_or_else(|err| {
        panic!("parse replay config {}: {err}", args.config.display());
    });
    if args.max_source_rows.is_some() {
        config.max_source_rows = args.max_source_rows;
    }
    init_logger(&config.log_path);
    install_panic_hook(config.log_path.clone());
    info!("cme_tas_replay_all log_path={}", config.log_path.display());
    info!(
        "cme_tas_replay_all unparsed_path={}",
        config.unparsed_path.display()
    );
    info!(
        "cme_tas_replay_all unmatched_path={}",
        config.unmatched_path.display()
    );
    info!(
        "cme_tas_replay_all quote_fallback_path={}",
        config.quote_fallback_path.display()
    );
    eprintln!(
        "cme_tas_replay_all logging to {}",
        config.log_path.display()
    );
    eprintln!(
        "cme_tas_replay_all unparsed rows to {}",
        config.unparsed_path.display()
    );
    eprintln!(
        "cme_tas_replay_all unmatched rows to {}",
        config.unmatched_path.display()
    );
    eprintln!(
        "cme_tas_replay_all quote Exch Time fallback contracts to {}",
        config.quote_fallback_path.display()
    );
    let selected_modes = usize::from(args.repair_unparsed)
        + usize::from(args.reset_writing_period)
        + usize::from(args.flush_all_column_families);
    let result = if selected_modes > 1 {
        Err(anyhow!(
            "--repair-unparsed, --reset-writing-period, and --flush-all-column-families are mutually exclusive"
        ))
    } else if args.repair_unparsed {
        repair_unparsed(&config)
    } else if args.reset_writing_period {
        reset_writing_period(&config)
    } else if args.flush_all_column_families {
        maintenance_flush_all_column_families(&config)
    } else {
        replay(&config)
    };
    if let Err(err) = result {
        error!("cme_tas_replay_all failed: {err:?}");
        eprintln!("cme_tas_replay_all failed: {err:?}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::product::{later_quote_value, product_cf_name, ALL_KEY_LEN};
    use cme_tas_replay::{decode_cme_correction, MISSING_PRICE};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use rocksdb::IteratorMode;
    use std::io::Cursor;
    use tempfile::TempDir;

    fn parallel_reader_test_map() -> HeaderMap {
        let idx = ColIdx {
            ric: 0,
            date_time: 0,
            event_type: 1,
            price: 2,
            volume: 2,
            qualifiers: 2,
            bid: 2,
            bid_size: 2,
            ask: 2,
            ask_size: 2,
            exch_time: 2,
            up_lim: 2,
            lo_lim: 2,
            imp_vol: 2,
            bid_imp_vol: 2,
            ask_imp_vol: 2,
            total_volume: 2,
            acc_volume: 2,
            implied_yield: 2,
            imbalance_qty: 2,
            imbalance_side: 2,
            change_type: 2,
            old_value: 2,
            new_value: 2,
            source_date: 2,
            original_price: 2,
            original_volume: 2,
            original_seq: 2,
        };
        HeaderMap {
            names: vec![
                "#RIC".to_string(),
                "Type".to_string(),
                "Empty".to_string(),
                "Forbidden".to_string(),
            ],
            groups: vec![
                "identity".to_string(),
                "identity".to_string(),
                "identity".to_string(),
                "yield".to_string(),
            ],
            idx,
            forbidden_idxs: vec![3],
            forbidden_mask: vec![false, false, false, true],
            projected_slot_by_source: vec![Some(0), Some(1), Some(2), None],
        }
    }

    #[test]
    fn reusable_core_parser_matches_csv_reader() {
        let lines = [
            "A,,",
            "B,plain,last",
            "C,\"comma,value\",tail",
            "D,\"embedded \"\"quote\"\"\",",
            "E,\" leading and trailing \"  ,x",
        ];
        let mut parser = CoreLineParser::new(2);
        let map = parallel_reader_test_map();
        for _ in 0..20 {
            for line in lines {
                let expected = record_from_line(line).unwrap();
                assert_eq!(parser.parse(line).unwrap(), expected);
                let projected = parse_data_line(&mut parser, line, &map).unwrap();
                for index in 0..3 {
                    assert_eq!(
                        map.cell_at(line, &projected, index),
                        expected.get(index).map(str::trim).unwrap_or("")
                    );
                }
            }
        }
    }

    #[test]
    fn reusable_core_parser_grows_field_end_buffer() {
        let line = (0..330)
            .map(|index| format!("field-{index}"))
            .collect::<Vec<_>>()
            .join(",");
        let mut parser = CoreLineParser::new(1);
        let actual = parser.parse(&line).unwrap();
        assert_eq!(actual, record_from_line(&line).unwrap());
        assert_eq!(actual.len(), 330);
    }

    #[test]
    fn parallel_record_reader_preserves_order_and_precheck_result() {
        let pool = ThreadPoolBuilder::new().num_threads(4).build().unwrap();
        let map = parallel_reader_test_map();
        let source = b"A,Trade,,\n.IDX,Trade,,allowed-for-index\nB,Trade,,forbidden\nC,Trade,,\n\x01,Trade,,\n";
        let mut reader = ParallelRecordReader::new(
            Cursor::new(source),
            &pool,
            &map,
            Path::new("part.csv.gz"),
            2010,
            2,
        )
        .unwrap();
        let mut rows = Vec::new();
        while let Some(row) = reader.next_line().unwrap() {
            rows.push(row);
        }

        let rics = rows
            .iter()
            .map(|row| match &row.record {
                ParsedRecord::Valid { record, .. } | ParsedRecord::InvalidRecord { record, .. } => {
                    map.cell_at(&row.raw_line, record, 0)
                }
                ParsedRecord::InvalidCsv(_) => "<invalid-csv>",
            })
            .collect::<Vec<_>>();
        assert_eq!(rics, vec!["A", ".IDX", "<invalid-csv>", "C", "\x01"]);
        assert_eq!(
            rows.iter()
                .map(|row| row.raw_line.as_str())
                .collect::<Vec<_>>(),
            vec![
                "A,Trade,,",
                ".IDX,Trade,,allowed-for-index",
                "B,Trade,,forbidden",
                "C,Trade,,",
                "\x01,Trade,,",
            ]
        );
        assert!(matches!(rows[0].record, ParsedRecord::Valid { .. }));
        assert!(matches!(rows[1].record, ParsedRecord::Valid { .. }));
        match &rows[2].record {
            ParsedRecord::InvalidCsv(error) => {
                assert!(error.to_string().contains("Forbidden"), "{error}");
            }
            _ => panic!("expected the third row to fail projected validation"),
        }
        assert!(matches!(rows[3].record, ParsedRecord::Valid { .. }));
        assert!(matches!(
            rows[4].record,
            ParsedRecord::Valid {
                prechecked: PrecheckedRecord::UnmatchedProduct,
                ..
            }
        ));
    }

    #[test]
    fn parallel_record_reader_rejects_zero_chunk_rows() {
        let pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let map = parallel_reader_test_map();
        let result = ParallelRecordReader::new(
            Cursor::new(Vec::<u8>::new()),
            &pool,
            &map,
            Path::new("part.csv.gz"),
            2010,
            0,
        );
        assert!(result.is_err());
    }

    #[test]
    fn claim_period_allows_a_second_period_and_refuses_repeat() {
        let dir = TempDir::new().unwrap();
        let db = open_rocksdb(dir.path()).unwrap();
        claim_period(&db, "2026-01-01_2026-06-01").unwrap();
        finish_period(&db, "2026-01-01_2026-06-01").unwrap();
        claim_period(&db, "2025-01-01_2026-01-01").unwrap();
        let err = claim_period(&db, "2026-01-01_2026-06-01").unwrap_err();
        assert!(err.to_string().contains("already done"), "{err}");
        let err = claim_period(&db, "2025-01-01_2026-01-01").unwrap_err();
        assert!(err.to_string().contains("marked writing"), "{err}");
    }

    #[test]
    fn reset_writing_period_drops_only_the_requested_year() {
        let dir = TempDir::new().unwrap();
        let config = ReplayConfig {
            data_root: dir.path().to_path_buf(),
            shard_root: None,
            period: "2024-01-01_2025-01-01".to_string(),
            periods: Vec::new(),
            rocksdb_dir: dir.path().join("rocksdb"),
            rics: Vec::new(),
            part_index: 0,
            job_offset: 0,
            max_jobs: None,
            workers: 1,
            parse_workers: 1,
            parse_chunk_rows: 1,
            max_source_rows: None,
            column_rules: dir.path().join("rules.json"),
            progress_every: 1,
            log_path: dir.path().join("replay.log"),
            unparsed_path: dir.path().join("unparsed.log"),
            unmatched_path: dir.path().join("unmatched.log"),
            quote_fallback_path: dir.path().join("fallback.log"),
            max_tradedays: 0,
        };
        {
            let db = open_rocksdb(&config.rocksdb_dir).unwrap();
            claim_period(&db, "2010-01-01_2011-01-01").unwrap();
            finish_period(&db, "2010-01-01_2011-01-01").unwrap();
            claim_period(&db, &config.period).unwrap();
            let lock = Mutex::new(());
            let old = product_cf(&db, &lock, "p:2010:AD").unwrap();
            db.put_cf(&old, b"old", b"keep").unwrap();
            let partial = product_cf(&db, &lock, "p:2024:AD").unwrap();
            db.put_cf(&partial, b"partial", b"drop").unwrap();
            db.flush().unwrap();
        }
        reset_writing_period(&config).unwrap();
        let db = open_rocksdb(&config.rocksdb_dir).unwrap();
        let old = db.cf_handle("p:2010:AD").unwrap();
        assert_eq!(
            db.get_cf(&old, b"old").unwrap().as_deref(),
            Some(b"keep" as &[u8])
        );
        assert!(db.cf_handle("p:2024:AD").is_none());
        let meta = replay_meta_cf(&db).unwrap();
        assert!(db
            .get_cf(&meta, period_meta_key(&config.period).unwrap())
            .unwrap()
            .is_none());
        assert_eq!(
            decode_period_status(
                &db.get_cf(&meta, period_meta_key("2010-01-01_2011-01-01").unwrap())
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            PeriodStatus::Done
        );
    }

    #[test]
    fn refuse_legacy_path() {
        let err = refuse_legacy_rocksdb(Path::new(FORBIDDEN_LEGACY_ROCKSDB)).unwrap_err();
        assert!(err.to_string().contains("legacy RocksDB"), "{err}");
        let dir = TempDir::new().unwrap();
        refuse_legacy_rocksdb(dir.path()).unwrap();
    }

    #[test]
    fn leftover_building_dir_is_refused_before_open() {
        let dir = TempDir::new().unwrap();
        let final_dir = dir.path().join("cme_tas_rocksdb_all_products");
        fs::create_dir_all(leftover_staging_dir(&final_dir)).unwrap();
        let leftover = leftover_staging_dir(&final_dir);
        assert!(leftover.exists());
        assert!(leftover
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .ends_with(".building"));
    }

    #[test]
    fn quote_key_is_bucket_with_zero_part_seq() {
        let key = encode_all_key(KIND_CME_QUOTE, "ADF26", 1_000_000_000, 0, 0).unwrap();
        assert_eq!(key.len(), ALL_KEY_LEN);
        assert_eq!(key[0], KIND_CME_QUOTE);
        assert_eq!(&key[ALL_KEY_LEN - 6..ALL_KEY_LEN - 4], &0u16.to_be_bytes());
        assert_eq!(&key[ALL_KEY_LEN - 4..], &0u32.to_be_bytes());
    }

    #[test]
    fn bbo_snapshot_keeps_later_date_time() {
        let older = encode_cme_quote(&SlimQuote {
            ric: "ADF26".to_string(),
            ts_utc_ns: 100,
            exch_hms_ns: 1,
            bid: 1,
            bid_size: 1,
            ask: 2,
            ask_size: 1,
        })
        .unwrap();
        let newer = encode_cme_quote(&SlimQuote {
            ric: "ADF26".to_string(),
            ts_utc_ns: 200,
            exch_hms_ns: 1,
            bid: 9,
            bid_size: 1,
            ask: 10,
            ask_size: 1,
        })
        .unwrap();
        assert_eq!(later_quote_value(&older, &newer), newer.as_slice());
        let later_bid_only = encode_cme_quote(&SlimQuote {
            ric: "ADF26".to_string(),
            ts_utc_ns: 300,
            exch_hms_ns: 1,
            bid: 5,
            bid_size: 4,
            ask: MISSING_PRICE,
            ask_size: cme_tas_replay::MISSING_VOLUME,
        })
        .unwrap();
        let overlaid =
            cme_tas_replay::decode_cme_quote(&later_quote_value(&newer, &later_bid_only)).unwrap();
        assert_eq!(overlaid.bid, 5);
        assert_eq!(overlaid.bid_size, 4);
        assert_eq!(overlaid.ask, 10);
        assert_eq!(overlaid.ask_size, 1);
        assert_eq!(overlaid.ts_utc_ns, 300);
    }

    #[test]
    fn bbo_overlays_only_within_the_same_second_bucket() {
        let previous = OpenBbo {
            ric: "ADF26".to_string(),
            cf_name: "p:2026:AD".to_string(),
            bucket_ns: 1_000_000_000,
            rec: SlimQuote {
                ric: "ADF26".to_string(),
                ts_utc_ns: 1_100_000_000,
                exch_hms_ns: 1_100_000_000,
                bid: 10,
                bid_size: 2,
                ask: 12,
                ask_size: 3,
            },
        };
        let bid_only = SlimQuote {
            ric: "ADF26".to_string(),
            ts_utc_ns: 1_200_000_000,
            exch_hms_ns: 1_200_000_000,
            bid: 11,
            bid_size: 4,
            ask: MISSING_PRICE,
            ask_size: cme_tas_replay::MISSING_VOLUME,
        };

        let same_bucket = overlay_quote_within_bucket(
            Some(&previous),
            "p:2026:AD",
            1_000_000_000,
            bid_only.clone(),
        );
        assert_eq!(same_bucket.bid, 11);
        assert_eq!(same_bucket.ask, 12);

        let next_bucket =
            overlay_quote_within_bucket(Some(&previous), "p:2026:AD", 2_000_000_000, bid_only);
        assert_eq!(next_bucket.bid, 11);
        assert_eq!(next_bucket.ask, MISSING_PRICE);
        assert_eq!(next_bucket.ask_size, cme_tas_replay::MISSING_VOLUME);
    }

    #[test]
    fn parse_source_date_accepts_iso_or_empty() {
        assert_eq!(parse_source_date_yyyymmdd("").unwrap(), 0);
        assert_eq!(parse_source_date_yyyymmdd("2011-01-03").unwrap(), 20110103);
        assert!(parse_source_date_yyyymmdd("20110103").is_err());
    }

    #[test]
    fn empty_correction_has_no_persisted_payload() {
        assert!(empty_correction_fields("", "", "", "", "", "", ""));
        assert!(!empty_correction_fields("", "", "", "", "", "7", ""));
        assert!(!empty_correction_fields("", "", "5", "", "", "", ""));
    }

    #[test]
    fn quote_fallback_log_lists_ric_and_product() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fallback.log");
        let mut rics = BTreeMap::new();
        rics.insert("HSIF1".to_string(), 12);
        rics.insert("HSIEAS".to_string(), 3);
        write_quote_fallback_log(&path, &rics).unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("ric=HSIF1 product=HSI rows=12"), "{text}");
        assert!(text.contains("ric=HSIEAS product=HSIEAS rows=3"), "{text}");
        assert!(text.contains("unique_rics=2 total_rows=15"), "{text}");
    }

    #[test]
    fn all_cf_flush_drains_non_default_memtables() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("all-cf-flush.rocksdb");
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let product_cf = "p:2026:YM";
        let db = AllDb::open_cf_descriptors(
            &options,
            &db_path,
            [
                ColumnFamilyDescriptor::new("default", Options::default()),
                ColumnFamilyDescriptor::new(product_cf, Options::default()),
            ],
        )
        .unwrap();
        let cf = db.cf_handle(product_cf).unwrap();
        db.put_cf(&cf, b"trade", b"payload").unwrap();
        assert_eq!(
            db.property_int_value_cf(&cf, rocksdb::properties::NUM_ENTRIES_ACTIVE_MEM_TABLE)
                .unwrap(),
            Some(1)
        );

        assert_eq!(flush_all_column_families(&db, 2).unwrap(), 2);
        assert_eq!(
            db.property_int_value_cf(&cf, rocksdb::properties::NUM_ENTRIES_ACTIVE_MEM_TABLE)
                .unwrap(),
            Some(0)
        );
        assert_eq!(db.get_cf(&cf, b"trade").unwrap().unwrap(), b"payload");
    }

    #[test]
    fn repair_targets_selects_only_known_2024_shapes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("unparsed.log");
        fs::write(
            &path,
            concat!(
                "[2026-08-28T03:06:35Z] period=2024-01-01_2025-01-01 part_no=2 source_row=45377 part=/tmp/auction.zst reason=auction FBTPM4 missing Volume :: #RIC=FBTPM4 Type=Auction Price=117.94\n",
                "[2026-08-28T03:06:40Z] period=2024-01-01_2025-01-01 part_no=2 source_row=137813 part=/tmp/imbalance.zst reason=unhandled Imbalance Side \"N\" :: #RIC=FCEZ4 Type=Quote Imbalance Quantity=0 Imbalance Side=N\n",
                "[2026-08-28T03:06:41Z] period=2024-01-01_2025-01-01 part_no=2 source_row=137814 part=/tmp/auction-size.zst reason=auction KSH4 missing Price :: #RIC=KSH4 Type=Auction Bid Size=100\n",
                "[2026-08-28T03:06:41Z] period=2024-01-01_2025-01-01 part_no=2 source_row=137815 part=/tmp/correction.zst reason=correction FFZ24 has no price, volume, original fields, or qualifiers :: #RIC=FFZ24 Type=Correction Seq. No.=1\n",
                "[2026-08-28T03:06:41Z] period=2024-01-01_2025-01-01 part_no=2 source_row=99 part=/tmp/ignored.zst reason=unhandled volume \"bad\" :: #RIC=BAD Type=Trade\n",
                "[2026-08-28T03:06:42Z] period=2025-01-01_2026-01-01 part_no=0 source_row=7 part=/tmp/other-period.zst reason=auction FDXZ5 missing Volume :: #RIC=FDXZ5 Type=Auction Price=1\n",
            ),
        )
        .unwrap();
        let periods = vec!["2024-01-01_2025-01-01".to_string()];
        let targets = repair_targets(&path, &periods).unwrap();
        assert_eq!(targets.len(), 4);
        assert_eq!(
            targets
                .get(&(periods[0].clone(), 2, PathBuf::from("/tmp/auction.zst")))
                .unwrap(),
            &BTreeSet::from([45_377])
        );
        assert_eq!(
            targets
                .get(&(periods[0].clone(), 2, PathBuf::from("/tmp/imbalance.zst")))
                .unwrap(),
            &BTreeSet::from([137_813])
        );
        assert_eq!(
            targets
                .get(&(
                    periods[0].clone(),
                    2,
                    PathBuf::from("/tmp/auction-size.zst")
                ))
                .unwrap(),
            &BTreeSet::from([137_814])
        );
        assert_eq!(
            targets
                .get(&(periods[0].clone(), 2, PathBuf::from("/tmp/correction.zst")))
                .unwrap(),
            &BTreeSet::from([137_815])
        );
    }

    #[test]
    fn replay_part_persists_acc_volume_only_correction() {
        let dir = TempDir::new().unwrap();
        let header = [
            "#RIC",
            "Date-Time",
            "Type",
            "Price",
            "Volume",
            "Qualifiers",
            "Bid Price",
            "Bid Size",
            "Ask Price",
            "Ask Size",
            "Exch Time",
            "UpLim Price",
            "LoLim Price",
            "Imp. Vol.",
            "Bid Imp. Vol",
            "Ask Imp. Vol",
            "Total Volume",
            "Acc. Volume",
            "Implied Yield",
            "Imbalance Quantity",
            "Imbalance Side",
            "Change Type",
            "Old Value",
            "New Value",
            "Date",
            "Original Price",
            "Original Volume",
            "Original Seq. No.",
        ];
        let mut row = vec![""; header.len()];
        row[0] = "YAPH1";
        row[1] = "2010-07-05T05:34:35.895804000Z";
        row[2] = "Correction";
        row[17] = "5";
        let input = dir.path().join("merged-Data-part-000000.csv.gz");
        let mut gzip = GzEncoder::new(File::create(&input).unwrap(), Compression::default());
        writeln!(gzip, "{}", header.join(",")).unwrap();
        writeln!(gzip, "{}", row.join(",")).unwrap();
        gzip.finish().unwrap();

        let config = ReplayConfig {
            data_root: dir.path().to_path_buf(),
            shard_root: None,
            period: "2010-01-01_2011-01-01".to_string(),
            periods: Vec::new(),
            rocksdb_dir: dir.path().join("rocksdb"),
            rics: Vec::new(),
            part_index: 0,
            job_offset: 0,
            max_jobs: None,
            workers: 1,
            parse_workers: 1,
            parse_chunk_rows: 1,
            max_source_rows: None,
            column_rules: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../preprocess/lseg/tas_column_rules.json"),
            progress_every: 1,
            log_path: dir.path().join("replay.log"),
            unparsed_path: dir.path().join("unparsed.log"),
            unmatched_path: dir.path().join("unmatched.log"),
            quote_fallback_path: dir.path().join("fallback.log"),
            max_tradedays: 0,
        };
        let job = ReplayJob {
            period: config.period.clone(),
            original_part: 0,
            shard_index: None,
            path: input,
            format: ReplayFormat::Gzip,
            expected_header: None,
        };
        let db = open_rocksdb(&config.rocksdb_dir).unwrap();
        let pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let cf_lock = Mutex::new(());
        let abort = AtomicBool::new(false);
        let unparsed = UnparsedSink::open(&config.unparsed_path).unwrap();
        let unmatched = UnmatchedSink::open(&config.unmatched_path).unwrap();
        let census = replay_part(
            &config, &pool, &db, &cf_lock, &job, &None, &abort, &unparsed, &unmatched, None, false,
        )
        .unwrap();

        assert_eq!(census.source_rows, 1);
        assert_eq!(census.written_corrections, 1);
        assert_eq!(census.skipped_unparsed, 0);
        assert_eq!(census.unmatched_product, 0);
        let cf_name = product_cf_name(2010, "YAP").unwrap();
        let cf = db.cf_handle(&cf_name).unwrap();
        let (_, value) = db
            .iterator_cf(&cf, IteratorMode::Start)
            .next()
            .unwrap()
            .unwrap();
        let correction = decode_cme_correction(&value).unwrap();
        assert_eq!(correction.ric, "YAPH1");
        assert_eq!(correction.acc_volume, 5);
        assert_eq!(fs::metadata(&config.unparsed_path).unwrap().len(), 0);
        assert_eq!(fs::metadata(&config.unmatched_path).unwrap().len(), 0);
    }
}
