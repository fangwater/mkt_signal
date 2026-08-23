//! Highly parallel, settlement-only scan of LSEG TAS gzip parts.
//!
//! This intentionally does not construct a 294-field CSV record for every
//! source row. It validates the part-0 header, then reads only `#RIC`,
//! `Date-Time`, `Type`, and (for settlement rows) `Price` plus source `Date`.
//! Gzip parts are the parallelism boundary; one process owns the RocksDB
//! writer lock and may have many worker threads.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::{
    decode_cme_settlement, decode_period_status, decode_ric, encode_cme_settlement, encode_key,
    encode_period_status, is_research_ric, key_ts_utc_ns, parse_date_time_ns, parse_price_e9,
    validate_period, PeriodStatus, SlimSettlement, CF_CME_PRICE_LIMIT, CF_CME_QUOTE,
    CF_CME_SETTLEMENT, CF_CME_SPECIAL, CF_CME_TRADE, CF_REPLAY_META, CF_SETTLEMENT_SCAN_META,
    CF_SYMBOLOGY_CHANGE, KEY_LEN, RIC_LEN,
};
use flate2::read::MultiGzDecoder;
use log::{error, info, LevelFilter, Log, Metadata, Record};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use serde::Deserialize;
use std::collections::{BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

const INPUT_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const WRITE_BATCH_ROWS: usize = 8_192;
const DEFAULT_WORKERS: usize = 32;
const DEFAULT_PROGRESS_EVERY: u64 = 10_000_000;
const DEFAULT_SAMPLE_ROWS: u64 = 5;
const DEFAULT_MAX_OPEN_FILES: i32 = 4_096;
const SETTLEMENT_TYPE: &[u8] = b"Settlement Price";
const SETTLEMENT_META_PREFIX: &str = "settlement_period:";
const SOURCE_DATE_INDEX: usize = 55;
const REQUIRED_HEAD: [&[u8]; 8] = [
    b"#RIC",
    b"Domain",
    b"Date-Time",
    b"GMT Offset",
    b"Type",
    b"Ex/Cntrb.ID",
    b"LOC",
    b"Price",
];

#[derive(Parser, Debug)]
#[command(name = "cme_tas_settlement_scan")]
#[command(about = "Scan only TAS Settlement Price rows into RocksDB")]
struct Args {
    #[arg(long, default_value = "config/cme_tas_settlement_scan.toml")]
    config: PathBuf,
    /// Diagnostic cap. This is only accepted with one worker and an empty DB.
    #[arg(long)]
    max_source_rows: Option<u64>,
    /// Override configured worker count, useful with a capped dry-run.
    #[arg(long)]
    workers: Option<usize>,
    /// Override the single-part index when workers is one.
    #[arg(long)]
    part_index: Option<usize>,
    /// Parse and report source messages without opening or writing RocksDB.
    #[arg(long)]
    dry_run: bool,
    /// Delete only an unfinished settlement scan before a complete restart.
    #[arg(long)]
    reset_incomplete: bool,
    /// Verify completed watermarks and every cme_settlement key/value pair.
    #[arg(long)]
    verify: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanConfig {
    data_root: PathBuf,
    period: String,
    #[serde(default)]
    periods: Vec<String>,
    rocksdb_dir: PathBuf,
    #[serde(default)]
    rics: Vec<String>,
    #[serde(default = "default_workers")]
    workers: usize,
    #[serde(default)]
    part_index: usize,
    #[serde(default)]
    max_source_rows: Option<u64>,
    #[serde(default = "default_progress_every")]
    progress_every: u64,
    #[serde(default = "default_sample_rows")]
    sample_rows: u64,
    /// Bound RocksDB table-reader metadata when opening the multi-terabyte DB.
    #[serde(default = "default_max_open_files")]
    max_open_files: i32,
    #[serde(default = "default_log_path")]
    log_path: PathBuf,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    reset_incomplete: bool,
}

fn default_workers() -> usize {
    DEFAULT_WORKERS
}

fn default_progress_every() -> u64 {
    DEFAULT_PROGRESS_EVERY
}

fn default_sample_rows() -> u64 {
    DEFAULT_SAMPLE_ROWS
}

fn default_max_open_files() -> i32 {
    DEFAULT_MAX_OPEN_FILES
}

fn default_log_path() -> PathBuf {
    PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_settlement_scan.log")
}

struct FileLogger {
    file: Mutex<File>,
}

impl Log for FileLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::Level::Info
            && (metadata.target().starts_with("cme_tas_settlement_scan")
                || metadata.target() == "cme_tas_settlement_scan")
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
                panic!("create settlement log dir {}: {err}", parent.display());
            });
        }
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|err| panic!("open settlement log {}: {err}", path.display()));
    let _ = log::set_boxed_logger(Box::new(FileLogger {
        file: Mutex::new(file),
    }));
    log::set_max_level(LevelFilter::Info);
}

#[derive(Default)]
struct Census {
    source_rows: u64,
    settlement_rows: u64,
    written: u64,
    skipped_unmapped: u64,
    skipped_ric_filter: u64,
}

impl Census {
    fn merge_from(&mut self, other: &Self) {
        self.source_rows += other.source_rows;
        self.settlement_rows += other.settlement_rows;
        self.written += other.written;
        self.skipped_unmapped += other.skipped_unmapped;
        self.skipped_ric_filter += other.skipped_ric_filter;
    }
}

struct SettlementFields<'a> {
    ric: &'a [u8],
    date_time: &'a [u8],
    price: &'a [u8],
    source_date: &'a [u8],
}

fn strip_line_ending(mut line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\n') {
        line = &line[..line.len() - 1];
    }
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    line
}

/// Read one field from the first eight simple TAS columns. The relevant TAS
/// cells are ASCII scalars; a quote escape there is a schema change, not a
/// value this zero-allocation scanner may silently reinterpret.
fn next_csv_field<'a>(line: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    if *offset >= line.len() {
        bail!("TAS CSV row ends before the required projected column");
    }
    if line[*offset] == b'"' {
        *offset += 1;
        let start = *offset;
        while *offset < line.len() && line[*offset] != b'"' {
            *offset += 1;
        }
        if *offset == line.len() {
            bail!("unterminated quoted TAS field in projected columns");
        }
        let end = *offset;
        *offset += 1;
        if line.get(*offset) == Some(&b'"') {
            bail!("escaped quote in projected TAS column is unsupported by settlement scanner");
        }
        match line.get(*offset) {
            Some(b',') => *offset += 1,
            None => {}
            Some(other) => bail!(
                "unexpected byte {:?} after quoted projected TAS field",
                *other as char
            ),
        }
        return Ok(&line[start..end]);
    }
    let start = *offset;
    while *offset < line.len() && line[*offset] != b',' {
        if line[*offset] == b'"' {
            bail!("quote inside unquoted projected TAS field");
        }
        *offset += 1;
    }
    let end = *offset;
    if line.get(*offset) == Some(&b',') {
        *offset += 1;
    }
    Ok(&line[start..end])
}

fn first_eight_fields(line: &[u8]) -> Result<[&[u8]; 8]> {
    let line = strip_line_ending(line);
    let mut offset = 0;
    Ok([
        next_csv_field(line, &mut offset)?,
        next_csv_field(line, &mut offset)?,
        next_csv_field(line, &mut offset)?,
        next_csv_field(line, &mut offset)?,
        next_csv_field(line, &mut offset)?,
        next_csv_field(line, &mut offset)?,
        next_csv_field(line, &mut offset)?,
        next_csv_field(line, &mut offset)?,
    ])
}

fn validate_part_zero_header(line: &[u8], part: &Path) -> Result<()> {
    let found = first_eight_fields(line)?;
    for (index, (actual, expected)) in found.iter().zip(REQUIRED_HEAD).enumerate() {
        if *actual != expected {
            bail!(
                "settlement scanner expected TAS header column {} to be {:?}, got {:?} in {}",
                index,
                String::from_utf8_lossy(expected),
                String::from_utf8_lossy(actual),
                part.display()
            );
        }
    }
    let mut offset = 0;
    let mut source_date_name = None;
    for index in 0..=SOURCE_DATE_INDEX {
        let field = next_csv_field(strip_line_ending(line), &mut offset)?;
        if index == SOURCE_DATE_INDEX {
            source_date_name = Some(field);
        }
    }
    if source_date_name != Some(&b"Date"[..]) {
        bail!(
            "settlement scanner expected TAS header column {SOURCE_DATE_INDEX} to be Date, got {:?} in {}",
            source_date_name.map(String::from_utf8_lossy),
            part.display()
        );
    }
    Ok(())
}

/// Return only a `Settlement Price` row. Non-settlement rows stop after the
/// fifth CSV cell, avoiding parsing the remaining 289 source columns.
fn settlement_fields(line: &[u8]) -> Result<Option<SettlementFields<'_>>> {
    let line = strip_line_ending(line);
    let mut offset = 0;
    let ric = next_csv_field(line, &mut offset)?;
    let _domain = next_csv_field(line, &mut offset)?;
    let date_time = next_csv_field(line, &mut offset)?;
    let _gmt_offset = next_csv_field(line, &mut offset)?;
    let event_type = next_csv_field(line, &mut offset)?;
    if event_type != SETTLEMENT_TYPE {
        return Ok(None);
    }
    let _exchange = next_csv_field(line, &mut offset)?;
    let _location = next_csv_field(line, &mut offset)?;
    let price = next_csv_field(line, &mut offset)?;
    for _ in 8..SOURCE_DATE_INDEX {
        let _ = next_csv_field(line, &mut offset)?;
    }
    let source_date = next_csv_field(line, &mut offset)?;
    if ric.is_empty() || date_time.is_empty() || price.is_empty() {
        bail!("Settlement Price row is missing #RIC, Date-Time, or Price");
    }
    Ok(Some(SettlementFields {
        ric,
        date_time,
        price,
        source_date,
    }))
}

fn projected_utf8<'a>(value: &'a [u8], name: &str) -> Result<&'a str> {
    std::str::from_utf8(value).map_err(|err| anyhow!("{name} is not UTF-8: {err}"))
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

fn discover_parts(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut parts = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read TAS period {}", dir.display()))? {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
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

fn part_number(path: &Path) -> Result<u16> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("part path {} has no file name", path.display()))?;
    let digits = name
        .strip_prefix("merged-Data-part-")
        .and_then(|rest| rest.strip_suffix(".csv.gz"))
        .ok_or_else(|| anyhow!("unrecognized TAS part name {name}"))?;
    digits
        .parse::<u16>()
        .map_err(|err| anyhow!("TAS part number {digits:?} in {name}: {err}"))
}

fn period_dir_for(config: &ScanConfig, period: &str) -> PathBuf {
    config.data_root.join(format!(
        "shanghai_evolution_futures_time_and_sales_ric_list_0_tas_{period}"
    ))
}

fn resolved_periods(config: &ScanConfig) -> Result<Vec<String>> {
    let periods = if config.periods.is_empty() {
        vec![config.period.clone()]
    } else {
        config.periods.clone()
    };
    let mut seen = BTreeSet::new();
    for period in &periods {
        validate_period(period)?;
        if !seen.insert(period) {
            bail!("duplicate settlement scan period {period}");
        }
    }
    Ok(periods)
}

fn collect_jobs(config: &ScanConfig, periods: &[String]) -> Result<Vec<(String, u16, PathBuf)>> {
    if config.workers == 0 {
        bail!("workers must be >= 1");
    }
    let mut jobs = Vec::new();
    for (period_index, period) in periods.iter().enumerate() {
        let parts = discover_parts(&period_dir_for(config, period))?;
        if config.workers == 1 {
            if period_index > 0 {
                continue;
            }
            let part = parts.get(config.part_index).ok_or_else(|| {
                anyhow!(
                    "part_index {} out of range; found {} gzip parts in {period}",
                    config.part_index,
                    parts.len()
                )
            })?;
            jobs.push((period.clone(), part_number(part)?, part.clone()));
        } else {
            for part in parts {
                jobs.push((period.clone(), part_number(&part)?, part));
            }
        }
    }
    if jobs.is_empty() {
        bail!("no TAS gzip parts for settlement periods {periods:?}");
    }
    Ok(jobs)
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

fn base_cf_options() -> Options {
    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_write_buffer_size(128 * 1024 * 1024);
    options.set_max_write_buffer_number(4);
    options.set_min_write_buffer_number_to_merge(1);
    options.set_level_zero_file_num_compaction_trigger(8);
    options.set_level_zero_slowdown_writes_trigger(64);
    options.set_level_zero_stop_writes_trigger(96);
    options
}

fn open_rocksdb(path: &Path, max_open_files: i32) -> Result<DB> {
    if max_open_files <= 0 {
        bail!("max_open_files must be positive, got {max_open_files}");
    }
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
    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);
    db_options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    db_options.set_max_open_files(max_open_files);
    db_options.increase_parallelism(32);
    db_options.set_max_background_jobs(32);
    let mut names: BTreeSet<String> = if path.exists() && !dir_is_empty(path)? {
        DB::list_cf(&db_options, path)
            .with_context(|| format!("list rocksdb column families {}", path.display()))?
            .into_iter()
            .collect()
    } else {
        BTreeSet::from(["default".to_string()])
    };
    for required in [
        CF_CME_TRADE,
        CF_CME_SPECIAL,
        CF_CME_QUOTE,
        CF_SYMBOLOGY_CHANGE,
        CF_CME_PRICE_LIMIT,
        CF_REPLAY_META,
        CF_CME_SETTLEMENT,
        CF_SETTLEMENT_SCAN_META,
    ] {
        names.insert(required.to_string());
    }
    let cf_options = base_cf_options();
    let descriptors: Vec<_> = names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options.clone()))
        .collect();
    DB::open_cf_descriptors(&db_options, path, descriptors)
        .with_context(|| format!("open rocksdb {}", path.display()))
}

fn settlement_meta_cf(db: &DB) -> Result<&ColumnFamily> {
    db.cf_handle(CF_SETTLEMENT_SCAN_META)
        .ok_or_else(|| anyhow!("column family {CF_SETTLEMENT_SCAN_META} missing"))
}

fn settlement_meta_key(period: &str) -> Result<Vec<u8>> {
    validate_period(period)?;
    Ok(format!("{SETTLEMENT_META_PREFIX}{period}").into_bytes())
}

fn claim_period(db: &DB, period: &str) -> Result<()> {
    let cf = settlement_meta_cf(db)?;
    let key = settlement_meta_key(period)?;
    match db
        .get_cf(cf, &key)
        .with_context(|| format!("read settlement watermark {period}"))?
    {
        Some(value) => match decode_period_status(&value)? {
            PeriodStatus::Done => bail!(
                "settlement period {period} is already done in this RocksDB; refuse to overwrite"
            ),
            PeriodStatus::Writing => {
                bail!("settlement period {period} is marked writing; previous scan did not finish")
            }
        },
        None => {
            let mut options = WriteOptions::default();
            options.set_sync(true);
            db.put_cf_opt(
                cf,
                key,
                encode_period_status(PeriodStatus::Writing),
                &options,
            )
            .with_context(|| format!("claim settlement period {period}"))?;
            Ok(())
        }
    }
}

fn finish_period(db: &DB, period: &str) -> Result<()> {
    let cf = settlement_meta_cf(db)?;
    let key = settlement_meta_key(period)?;
    match db
        .get_cf(cf, &key)
        .with_context(|| format!("read settlement watermark {period} before finish"))?
    {
        Some(value) if decode_period_status(&value)? == PeriodStatus::Writing => {}
        Some(_) => bail!("settlement period {period} is already done"),
        None => bail!("settlement period {period} has no writing watermark"),
    }
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.put_cf_opt(cf, key, encode_period_status(PeriodStatus::Done), &options)
        .with_context(|| format!("finish settlement period {period}"))
}

/// Clear a failed, settlement-only run before restarting it from source.
/// Completed periods and other in-progress settlement scans are deliberately
/// refused, so this cannot discard a completed or unrelated result set.
fn reset_incomplete(db: &DB, periods: &[String]) -> Result<()> {
    let meta = settlement_meta_cf(db)?;
    let requested: BTreeSet<Vec<u8>> = periods
        .iter()
        .map(|period| settlement_meta_key(period))
        .collect::<Result<_>>()?;
    let mut seen = BTreeSet::new();
    for item in db.iterator_cf(meta, IteratorMode::Start) {
        let (key, value) = item.context("scan settlement watermarks before reset")?;
        if !key.starts_with(SETTLEMENT_META_PREFIX.as_bytes()) {
            continue;
        }
        match decode_period_status(&value)? {
            PeriodStatus::Done => bail!(
                "refuse reset: completed settlement watermark {:?} exists",
                String::from_utf8_lossy(&key)
            ),
            PeriodStatus::Writing if !requested.contains(&key.to_vec()) => bail!(
                "refuse reset: unrelated in-progress settlement watermark {:?} exists",
                String::from_utf8_lossy(&key)
            ),
            PeriodStatus::Writing => {
                seen.insert(key.to_vec());
            }
        }
    }
    if seen != requested {
        bail!(
            "refuse reset: requested settlement watermarks do not exactly match existing writing watermarks"
        );
    }
    let settlement = db
        .cf_handle(CF_CME_SETTLEMENT)
        .ok_or_else(|| anyhow!("column family {CF_CME_SETTLEMENT} missing"))?;
    let mut batch = WriteBatch::default();
    batch.delete_range_cf(settlement, [0u8], [u8::MAX]);
    for key in requested {
        batch.delete_cf(meta, key);
    }
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options)
        .context("clear unfinished settlement scan")?;
    db.flush().context("flush unfinished settlement reset")?;
    Ok(())
}

fn flush_batch(db: &DB, batch: &mut WriteBatch) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut options = WriteOptions::default();
    options.set_sync(false);
    db.write_opt(std::mem::take(batch), &options)
        .context("write settlement RocksDB batch")
}

fn verify(config: &ScanConfig) -> Result<()> {
    if config.dry_run || config.reset_incomplete || config.max_source_rows.is_some() {
        bail!("verify cannot be combined with dry_run, reset_incomplete, or max_source_rows");
    }
    let periods = resolved_periods(config)?;
    let db = open_rocksdb(&config.rocksdb_dir, config.max_open_files)?;
    let meta = settlement_meta_cf(&db)?;
    for period in &periods {
        let key = settlement_meta_key(period)?;
        let value = db
            .get_cf(meta, &key)
            .with_context(|| format!("read settlement watermark {period}"))?
            .ok_or_else(|| anyhow!("settlement period {period} has no completion watermark"))?;
        if decode_period_status(&value)? != PeriodStatus::Done {
            bail!("settlement period {period} is not done");
        }
    }
    let settlement = db
        .cf_handle(CF_CME_SETTLEMENT)
        .ok_or_else(|| anyhow!("column family {CF_CME_SETTLEMENT} missing"))?;
    let mut count = 0u64;
    let mut previous: Option<Box<[u8]>> = None;
    let mut first: Option<SlimSettlement> = None;
    let mut last: Option<SlimSettlement> = None;
    for item in db.iterator_cf(settlement, IteratorMode::Start) {
        let (key, value) = item.context("scan cme_settlement")?;
        if key.len() != KEY_LEN {
            bail!("cme_settlement key length {} is not {KEY_LEN}", key.len());
        }
        if previous
            .as_ref()
            .is_some_and(|prior| prior.as_ref() >= key.as_ref())
        {
            bail!("cme_settlement key order is not strictly increasing");
        }
        let record = decode_cme_settlement(&value)?;
        let key_ric = decode_ric(&key[..RIC_LEN])?;
        let key_ts = key_ts_utc_ns(&key)?;
        if record.ric != key_ric || record.ts_utc_ns != key_ts {
            bail!(
                "cme_settlement key/value mismatch: key=({key_ric},{key_ts}) value=({}, {})",
                record.ric,
                record.ts_utc_ns
            );
        }
        if !is_research_ric(&record.ric)? {
            bail!("cme_settlement has unmapped RIC {}", record.ric);
        }
        if first.is_none() {
            first = Some(record.clone());
        }
        last = Some(record);
        previous = Some(key);
        count = count
            .checked_add(1)
            .ok_or_else(|| anyhow!("settlement count overflow"))?;
    }
    if count == 0 {
        bail!("cme_settlement is empty after completed scan");
    }
    println!(
        "cme_tas_settlement_scan verify passed periods={periods:?} records={count} first={:?} last={:?}",
        first, last
    );
    Ok(())
}

fn scan_part(
    config: &ScanConfig,
    db: Option<&DB>,
    period: &str,
    part_no: u16,
    part: &Path,
    ric_filter: Option<&BTreeSet<String>>,
    abort: &AtomicBool,
    sampled: &AtomicU64,
) -> Result<Census> {
    if abort.load(Ordering::Relaxed) {
        bail!("settlement scan aborted before opening {}", part.display());
    }
    let settlement_cf = match db {
        Some(db) => Some(
            db.cf_handle(CF_CME_SETTLEMENT)
                .ok_or_else(|| anyhow!("column family {CF_CME_SETTLEMENT} missing"))?,
        ),
        None => None,
    };
    let file = File::open(part).with_context(|| format!("open TAS gzip {}", part.display()))?;
    let compressed_len = file
        .metadata()
        .with_context(|| format!("stat TAS gzip {}", part.display()))?
        .len();
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(INPUT_BUFFER_BYTES, file));
    let mut reader = BufReader::with_capacity(INPUT_BUFFER_BYTES, decoder);
    let mut line = Vec::with_capacity(2_048);
    if part_no == 0 {
        reader
            .read_until(b'\n', &mut line)
            .with_context(|| format!("read TAS header {}", part.display()))?;
        if line.is_empty() {
            bail!("empty part-0 TAS gzip {}", part.display());
        }
        validate_part_zero_header(&line, part)?;
        line.clear();
    }

    let started = Instant::now();
    let mut census = Census::default();
    let mut batch = WriteBatch::default();
    let mut last_key: Option<[u8; KEY_LEN]> = None;
    let mut last_ric_ts: Option<(String, u64, u32)> = None;
    let mut next_progress = config.progress_every;
    loop {
        if abort.load(Ordering::Relaxed) {
            bail!("settlement scan aborted in {}", part.display());
        }
        if config
            .max_source_rows
            .is_some_and(|max_rows| census.source_rows >= max_rows)
        {
            break;
        }
        line.clear();
        let bytes = reader
            .read_until(b'\n', &mut line)
            .with_context(|| format!("read TAS source row {}", part.display()))?;
        if bytes == 0 {
            break;
        }
        census.source_rows += 1;
        if config.progress_every > 0 && census.source_rows >= next_progress {
            let elapsed = started.elapsed().as_secs_f64().max(0.001);
            info!(
                "cme_tas_settlement_scan progress period={period} part_no={part_no} source_rows={} settlement_rows={} written={} skipped_unmapped={} rows_per_s={:.0} elapsed_s={:.1}",
                census.source_rows,
                census.settlement_rows,
                census.written,
                census.skipped_unmapped,
                census.source_rows as f64 / elapsed,
                elapsed
            );
            next_progress = next_progress.saturating_add(config.progress_every);
        }
        let fields = settlement_fields(&line).with_context(|| {
            format!(
                "parse projected TAS columns period={period} part_no={part_no} source_row={}",
                census.source_rows
            )
        })?;
        let Some(fields) = fields else {
            continue;
        };
        census.settlement_rows += 1;
        let ric = projected_utf8(fields.ric, "Settlement Price #RIC")?;
        if !is_research_ric(ric)? {
            census.skipped_unmapped += 1;
            continue;
        }
        if ric_filter.is_some_and(|wanted| !wanted.contains(ric)) {
            census.skipped_ric_filter += 1;
            continue;
        }
        let date_time = projected_utf8(fields.date_time, "Settlement Price Date-Time")?;
        let price_raw = projected_utf8(fields.price, "Settlement Price Price")?;
        let source_date_raw = projected_utf8(fields.source_date, "Settlement Price Date")?;
        let ts_utc_ns = parse_date_time_ns(date_time)
            .with_context(|| format!("parse Settlement Price Date-Time for {ric} {date_time}"))?;
        let price = parse_price_e9(price_raw)
            .with_context(|| format!("parse Settlement Price Price for {ric} {date_time}"))?;
        let source_date_yyyymmdd = parse_source_date_yyyymmdd(source_date_raw)
            .with_context(|| format!("parse Settlement Price Date for {ric} {date_time}"))?;
        let sequence = match &last_ric_ts {
            Some((previous_ric, previous_ts, previous_seq))
                if previous_ric == ric && *previous_ts == ts_utc_ns =>
            {
                previous_seq.checked_add(1).ok_or_else(|| {
                    anyhow!("settlement sequence overflow for {ric} at {date_time}")
                })?
            }
            _ => 0,
        };
        last_ric_ts = Some((ric.to_string(), ts_utc_ns, sequence));
        let key = encode_key(ric, ts_utc_ns, part_no, sequence)?;
        if last_key.is_some_and(|previous| key <= previous) {
            bail!(
                "settlement key is not strictly increasing for {ric} at {date_time} in part {part_no}"
            );
        }
        last_key = Some(key);
        let sample_index = sampled.fetch_add(1, Ordering::Relaxed);
        if sample_index < config.sample_rows {
            info!(
                "cme_tas_settlement_scan sample period={period} part_no={part_no} ric={ric} date_time={date_time} price={price_raw} source_date={source_date_raw} scaled_price={price}"
            );
        }
        if let (Some(db), Some(cf)) = (db, settlement_cf) {
            let row = SlimSettlement {
                ric: ric.to_string(),
                ts_utc_ns,
                price,
                source_date_yyyymmdd,
            };
            batch.put_cf(cf, key, encode_cme_settlement(&row)?);
            census.written += 1;
            if batch.len() >= WRITE_BATCH_ROWS {
                flush_batch(db, &mut batch)?;
            }
        }
    }
    if let Some(db) = db {
        flush_batch(db, &mut batch)?;
    }
    if config.max_source_rows.is_none() {
        let decoder = reader.into_inner();
        let mut file = decoder.into_inner().into_inner();
        let position = file
            .stream_position()
            .with_context(|| format!("tell {}", part.display()))?;
        if position < compressed_len {
            bail!(
                "gzip ended at byte {position} of {compressed_len} in {}; concatenated member was not consumed",
                part.display()
            );
        }
    }
    info!(
        "cme_tas_settlement_scan finished period={period} part_no={part_no} source_rows={} settlement_rows={} written={} skipped_unmapped={} skipped_ric_filter={} elapsed_ms={}",
        census.source_rows,
        census.settlement_rows,
        census.written,
        census.skipped_unmapped,
        census.skipped_ric_filter,
        started.elapsed().as_millis()
    );
    Ok(census)
}

fn scan(config: &ScanConfig) -> Result<()> {
    let periods = resolved_periods(config)?;
    let jobs = collect_jobs(config, &periods)?;
    if config.max_source_rows.is_some() && config.workers != 1 {
        bail!("max_source_rows requires workers = 1; a per-worker cap is not an input window");
    }
    let capped = config.max_source_rows.is_some();
    if config.reset_incomplete && (config.dry_run || capped) {
        bail!("reset_incomplete requires a full non-dry settlement scan");
    }
    if capped && !config.dry_run && !dir_is_empty(&config.rocksdb_dir)? {
        bail!(
            "refuse capped settlement scan into nonempty {}; use an empty throwaway DB",
            config.rocksdb_dir.display()
        );
    }
    let db = if config.dry_run {
        None
    } else {
        Some(Arc::new(open_rocksdb(
            &config.rocksdb_dir,
            config.max_open_files,
        )?))
    };
    if let Some(db) = &db {
        if config.reset_incomplete {
            reset_incomplete(db, &periods)?;
            info!("cme_tas_settlement_scan cleared unfinished settlement scan before restart");
        }
        if !capped {
            for period in &periods {
                claim_period(db, period)?;
            }
        }
    }
    let ric_filter = if config.rics.is_empty() {
        None
    } else {
        Some(config.rics.iter().cloned().collect::<BTreeSet<_>>())
    };
    info!(
        "cme_tas_settlement_scan start workers={} parts={} periods={periods:?} rocksdb={} dry_run={} capped={capped} max_open_files={}",
        config.workers,
        jobs.len(),
        config.rocksdb_dir.display(),
        config.dry_run,
        config.max_open_files,
    );
    let started = Instant::now();
    let queue = Arc::new(Mutex::new(
        jobs.iter().cloned().rev().collect::<VecDeque<_>>(),
    ));
    let abort = Arc::new(AtomicBool::new(false));
    let sampled = Arc::new(AtomicU64::new(0));
    let workers = config.workers.min(jobs.len()).max(1);
    let mut handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let config = config.clone();
        let queue = Arc::clone(&queue);
        let abort = Arc::clone(&abort);
        let sampled = Arc::clone(&sampled);
        let db = db.clone();
        let ric_filter = ric_filter.clone();
        handles.push(thread::Builder::new().name(format!("cme-settlement-{worker_id}")).spawn(
            move || -> Result<Census> {
                let mut total = Census::default();
                loop {
                    let job = queue.lock().expect("settlement part queue").pop_back();
                    let Some((period, part_no, part)) = job else {
                        break;
                    };
                    info!(
                        "cme_tas_settlement_scan worker={worker_id} claimed period={period} part_no={part_no} path={}",
                        part.display()
                    );
                    match scan_part(
                        &config,
                        db.as_deref(),
                        &period,
                        part_no,
                        &part,
                        ric_filter.as_ref(),
                        &abort,
                        &sampled,
                    ) {
                        Ok(census) => total.merge_from(&census),
                        Err(err) => {
                            abort.store(true, Ordering::Relaxed);
                            return Err(err.context(format!(
                                "settlement worker {worker_id} failed period={period} part_no={part_no} {}",
                                part.display()
                            )));
                        }
                    }
                }
                Ok(total)
            },
        )?);
    }
    let mut total = Census::default();
    let mut first_error = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(census)) => total.merge_from(&census),
            Ok(Err(err)) => {
                abort.store(true, Ordering::Relaxed);
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Err(_) => {
                abort.store(true, Ordering::Relaxed);
                if first_error.is_none() {
                    first_error = Some(anyhow!("settlement worker thread panicked"));
                }
            }
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    if let Some(db) = &db {
        db.flush().context("flush settlement RocksDB")?;
        if !capped {
            for period in &periods {
                finish_period(db, period)?;
            }
        }
    }
    println!(
        "cme_tas_settlement_scan finished workers={workers} parts={} periods={periods:?} source_rows={} settlement_rows={} written={} skipped_unmapped={} skipped_ric_filter={} elapsed_ms={}",
        jobs.len(),
        total.source_rows,
        total.settlement_rows,
        total.written,
        total.skipped_unmapped,
        total.skipped_ric_filter,
        started.elapsed().as_millis()
    );
    Ok(())
}

fn main() {
    let args = Args::parse();
    let content = fs::read_to_string(&args.config)
        .unwrap_or_else(|err| panic!("read settlement config {}: {err}", args.config.display()));
    let mut config: ScanConfig = toml::from_str(&content)
        .unwrap_or_else(|err| panic!("parse settlement config {}: {err}", args.config.display()));
    if args.max_source_rows.is_some() {
        config.max_source_rows = args.max_source_rows;
    }
    if let Some(workers) = args.workers {
        config.workers = workers;
    }
    if let Some(part_index) = args.part_index {
        config.part_index = part_index;
    }
    if args.dry_run {
        config.dry_run = true;
    }
    if args.reset_incomplete {
        config.reset_incomplete = true;
    }
    init_logger(&config.log_path);
    eprintln!(
        "cme_tas_settlement_scan logging to {}",
        config.log_path.display()
    );
    let result = if args.verify {
        verify(&config)
    } else {
        scan(&config)
    };
    if let Err(err) = result {
        error!("cme_tas_settlement_scan failed: {err:#}");
        eprintln!("cme_tas_settlement_scan failed: {err:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::decode_cme_settlement;
    use tempfile::TempDir;

    #[test]
    fn projects_only_the_settlement_columns() {
        let mut cells = vec![""; SOURCE_DATE_INDEX + 1];
        cells[0] = "ADF26";
        cells[1] = "Market Price";
        cells[2] = "2026-01-01T22:00:00.000000000Z";
        cells[3] = "-6";
        cells[4] = "Settlement Price";
        cells[7] = "0.6673";
        cells[SOURCE_DATE_INDEX] = "2026-01-01";
        let line = format!("{}\n", cells.join(","));
        let fields = settlement_fields(line.as_bytes()).unwrap().unwrap();
        assert_eq!(fields.ric, b"ADF26");
        assert_eq!(fields.date_time, b"2026-01-01T22:00:00.000000000Z");
        assert_eq!(fields.price, b"0.6673");
        assert_eq!(fields.source_date, b"2026-01-01");
        assert_eq!(parse_source_date_yyyymmdd("2026-01-01").unwrap(), 20260101);
    }

    #[test]
    fn stops_after_type_for_non_settlement() {
        let line = b"ADF26,Market Price,2026-01-01T23:00:00Z,-6,Quote\n";
        assert!(settlement_fields(line).unwrap().is_none());
    }

    #[test]
    fn validates_the_stable_head_columns() {
        let mut cells = vec![""; SOURCE_DATE_INDEX + 1];
        for (index, name) in REQUIRED_HEAD.iter().enumerate() {
            cells[index] = std::str::from_utf8(name).unwrap();
        }
        cells[SOURCE_DATE_INDEX] = "Date";
        let header = cells.join(",");
        validate_part_zero_header(header.as_bytes(), Path::new("part-000000.csv.gz")).unwrap();
        cells[4] = "Event";
        let bad = cells.join(",");
        assert!(
            validate_part_zero_header(bad.as_bytes(), Path::new("part-000000.csv.gz")).is_err()
        );
    }

    #[test]
    fn rejects_missing_settlement_price() {
        let mut cells = vec![""; SOURCE_DATE_INDEX + 1];
        cells[0] = "ADF26";
        cells[2] = "2026-01-01T22:00:00Z";
        cells[4] = "Settlement Price";
        assert!(settlement_fields(cells.join(",").as_bytes()).is_err());
    }

    #[test]
    fn writes_settlement_and_uses_an_independent_watermark() {
        let dir = TempDir::new().unwrap();
        let db = open_rocksdb(dir.path(), DEFAULT_MAX_OPEN_FILES).unwrap();
        let period = "2026-01-01_2026-06-01";
        claim_period(&db, period).unwrap();
        let row = SlimSettlement {
            ric: "UROM25".to_string(),
            ts_utc_ns: parse_date_time_ns("2024-08-22T19:00:21.620994993Z").unwrap(),
            price: parse_price_e9("1.1247").unwrap(),
            source_date_yyyymmdd: 20240822,
        };
        let key = encode_key(&row.ric, row.ts_utc_ns, 9, 0).unwrap();
        let cf = db.cf_handle(CF_CME_SETTLEMENT).unwrap();
        db.put_cf(cf, key, encode_cme_settlement(&row).unwrap())
            .unwrap();
        let bytes = db.get_cf(cf, key).unwrap().unwrap();
        let back = decode_cme_settlement(&bytes).unwrap();
        assert_eq!(back.price, 1_124_700_000);
        assert_eq!(back.source_date_yyyymmdd, 20240822);
        finish_period(&db, period).unwrap();
        let meta = settlement_meta_cf(&db).unwrap();
        let value = db
            .get_cf(meta, settlement_meta_key(period).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(decode_period_status(&value).unwrap(), PeriodStatus::Done);
        assert!(db
            .get_cf(
                db.cf_handle(CF_REPLAY_META).unwrap(),
                b"period:2026-01-01_2026-06-01"
            )
            .unwrap()
            .is_none());
    }

    #[test]
    fn reset_refuses_done_but_clears_exact_writing_periods() {
        let dir = TempDir::new().unwrap();
        let db = open_rocksdb(dir.path(), DEFAULT_MAX_OPEN_FILES).unwrap();
        let periods = vec!["2024-01-01_2025-01-01".to_string()];
        claim_period(&db, &periods[0]).unwrap();
        let cf = db.cf_handle(CF_CME_SETTLEMENT).unwrap();
        db.put_cf(cf, b"settlement", b"partial").unwrap();
        reset_incomplete(&db, &periods).unwrap();
        assert!(db.get_cf(cf, b"settlement").unwrap().is_none());
        assert!(db
            .get_cf(
                settlement_meta_cf(&db).unwrap(),
                settlement_meta_key(&periods[0]).unwrap()
            )
            .unwrap()
            .is_none());
        claim_period(&db, &periods[0]).unwrap();
        finish_period(&db, &periods[0]).unwrap();
        assert!(reset_incomplete(&db, &periods).is_err());
    }
}
