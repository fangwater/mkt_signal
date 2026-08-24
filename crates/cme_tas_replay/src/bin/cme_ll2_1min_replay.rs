//! Stream Normalized LL2 gzip parts into minute-end Depth10 RocksDB records.
//!
//! This is deliberately independent from the TAS RocksDB and never writes a
//! raw-tick or one-second layer. Every stored value is the final complete
//! source snapshot in one RIC/minute, plus that minute's source-update count.

use anyhow::{anyhow, bail, Context, Result};
use chrono::DateTime;
use clap::Parser;
use cme_tas_replay::ll2_1min::{
    decode_ll2_minute, decode_ll2_minute_stage_key, encode_ll2_minute, encode_ll2_minute_stage_key,
    Ll2Minute, Ll2MinuteKey, CF_LL2_MINUTE, CF_LL2_MINUTE_META, CF_LL2_MINUTE_STAGE,
    LL2_DEPTH_LEVELS, LL2_MINUTE_KEY_LEN,
};
use cme_tas_replay::{
    research_root_exchange, research_root_of, tradeday_yyyymmdd, validate_period, PeriodStatus,
};
use crossbeam_channel::unbounded;
use flate2::read::MultiGzDecoder;
use log::{info, warn};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

const DEFAULT_WORKERS: usize = 12;
const WRITE_BATCH_ROWS: usize = 8_192;
const INPUT_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const PROGRESS_EVERY: u64 = 10_000_000;
const HEADER_COLUMNS: usize = 66;

#[derive(Parser, Debug)]
#[command(name = "cme_ll2_1min_replay")]
#[command(about = "Aggregate LSEG Normalized LL2 to final Depth10 snapshots per minute")]
struct Args {
    #[arg(long, default_value = "config/cme_ll2_1min.toml")]
    config: PathBuf,
    /// Parse source rows but do not create or write RocksDB.
    #[arg(long)]
    dry_run: bool,
    /// Limit a one-part diagnostic run. Requires workers=1 and an empty DB.
    #[arg(long)]
    max_source_rows: Option<u64>,
    /// Validate all completed minute records and watermarks without reading gzip.
    #[arg(long)]
    verify: bool,
    /// Remove the entire incomplete LL2 minute DB before replaying its configured periods.
    #[arg(long)]
    reset_incomplete: bool,
    /// Override configured periods with one LL2 period suffix.
    #[arg(long)]
    period: Option<String>,
    /// Restrict to one part after period selection. Intended for dry-run or capped smoke tests.
    #[arg(long)]
    part_index: Option<u16>,
    /// Override the configured RocksDB directory, for an isolated smoke run.
    #[arg(long)]
    rocksdb_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    data_root: PathBuf,
    period: String,
    #[serde(default)]
    periods: Vec<String>,
    rocksdb_dir: PathBuf,
    session_csv: PathBuf,
    #[serde(default)]
    rics: Vec<String>,
    #[serde(default = "default_workers")]
    workers: usize,
}

#[derive(Debug, Deserialize)]
struct SessionCsvRow {
    schedule_group: String,
    is_trading: String,
    open_utc: Option<String>,
    close_utc: Option<String>,
}

#[derive(Debug)]
struct SessionCalendar {
    by_group: BTreeMap<String, Vec<(i64, i64)>>,
}

impl SessionCalendar {
    fn load(path: &Path) -> Result<Self> {
        let mut reader = csv::Reader::from_path(path)
            .with_context(|| format!("open CME session CSV {}", path.display()))?;
        let mut by_group: BTreeMap<String, Vec<(i64, i64)>> = BTreeMap::new();
        for item in reader.deserialize() {
            let row: SessionCsvRow =
                item.with_context(|| format!("parse CME session CSV {}", path.display()))?;
            if row.is_trading != "1" {
                continue;
            }
            let open = row
                .open_utc
                .as_deref()
                .ok_or_else(|| anyhow!("trading session row has no open_utc"))?;
            let close = row
                .close_utc
                .as_deref()
                .ok_or_else(|| anyhow!("trading session row has no close_utc"))?;
            let start = DateTime::parse_from_rfc3339(open)
                .with_context(|| format!("parse session open {open:?}"))?
                .timestamp();
            let end = DateTime::parse_from_rfc3339(close)
                .with_context(|| format!("parse session close {close:?}"))?
                .timestamp();
            if end <= start {
                bail!("invalid CME session interval [{start}, {end})");
            }
            by_group
                .entry(row.schedule_group)
                .or_default()
                .push((start, end));
        }
        for intervals in by_group.values_mut() {
            intervals.sort_unstable();
        }
        if by_group.is_empty() {
            bail!(
                "CME session CSV {} has no trading intervals",
                path.display()
            );
        }
        Ok(Self { by_group })
    }

    fn contains(&self, product_root: &str, ts_utc_sec: i64) -> Result<bool> {
        let group = schedule_group(product_root)?;
        let intervals = self
            .by_group
            .get(group)
            .ok_or_else(|| anyhow!("session calendar has no group {group}"))?;
        let upper = intervals.partition_point(|(start, _)| *start <= ts_utc_sec);
        Ok(upper > 0 && ts_utc_sec < intervals[upper - 1].1)
    }
}

fn schedule_group(product: &str) -> Result<&'static str> {
    match product {
        "C" | "W" | "KW" | "S" | "SM" | "BO" => Ok("grains_oilseeds"),
        "FF" | "TU" | "FV" | "TY" | "TN" | "US" | "U" | "S1R" | "SRA" => Ok("interest_rates"),
        "YM" | "ES" | "NQ" | "RTY" | "MEM" => Ok("equity_indices"),
        "AD" | "BP" | "BR" | "CD" | "JY" | "KRW" | "MP" | "NE" | "NOKA" | "PLZ" | "SEK" | "SF"
        | "URO" => Ok("fx"),
        "BTC" | "ETH" => Ok("cryptocurrency"),
        "FC" | "LC" | "LH" => Ok("livestock"),
        "CL" | "WTCL" | "HO" | "RB" | "NG" | "JKM" => Ok("energy"),
        "GC" | "SI" | "HG" | "ALI" | "HRC" | "PL" | "PA" => Ok("metals"),
        _ => bail!("no CME session group for product root {product:?}"),
    }
}

#[derive(Debug, Clone)]
struct Job {
    period: String,
    part_no: u16,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct Route {
    ric: String,
    exchange: String,
    product_root: String,
}

#[derive(Default, Debug, Clone)]
struct PartStats {
    source_rows: u64,
    empty_rows: u64,
    routed_rows: u64,
    off_session_rows: u64,
    minute_rows: u64,
    skipped_ric_filter: u64,
}

impl PartStats {
    fn add(&mut self, other: &Self) {
        self.source_rows += other.source_rows;
        self.empty_rows += other.empty_rows;
        self.routed_rows += other.routed_rows;
        self.off_session_rows += other.off_session_rows;
        self.minute_rows += other.minute_rows;
        self.skipped_ric_filter += other.skipped_ric_filter;
    }
}

fn default_workers() -> usize {
    DEFAULT_WORKERS
}

fn resolved_periods(config: &Config, override_period: Option<&str>) -> Result<Vec<String>> {
    let periods = if let Some(period) = override_period {
        vec![period.to_string()]
    } else if config.periods.is_empty() {
        vec![config.period.clone()]
    } else {
        config.periods.clone()
    };
    let mut unique = BTreeSet::new();
    for period in &periods {
        validate_period(period)?;
        if !unique.insert(period.clone()) {
            bail!("duplicate LL2 period {period}");
        }
    }
    Ok(periods)
}

fn period_dir(config: &Config, period: &str) -> PathBuf {
    config.data_root.join(format!(
        "shanghai_evolution_futures_market_depth_ric_list_0_ll2_{period}"
    ))
}

fn parse_part_number(path: &Path) -> Result<u16> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("part path {} has no UTF-8 name", path.display()))?;
    let digits = name
        .strip_prefix("merged-Data-part-")
        .and_then(|value| value.strip_suffix(".csv.gz"))
        .ok_or_else(|| anyhow!("invalid LL2 part name {name}"))?;
    digits
        .parse::<u16>()
        .with_context(|| format!("parse LL2 part number {digits:?}"))
}

fn discover_parts(
    config: &Config,
    periods: &[String],
    part_index: Option<u16>,
) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for period in periods {
        let dir = period_dir(config, period);
        if !dir.is_dir() {
            bail!("LL2 period directory is missing: {}", dir.display());
        }
        let mut paths = fs::read_dir(&dir)
            .with_context(|| format!("read LL2 period directory {}", dir.display()))?
            .filter_map(|item| item.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("merged-Data-part-") && name.ends_with(".csv.gz")
                    })
            })
            .collect::<Vec<_>>();
        paths.sort();
        if paths.is_empty() {
            bail!("no LL2 gzip parts under {}", dir.display());
        }
        for path in paths {
            let part_no = parse_part_number(&path)?;
            if part_index.is_some_and(|wanted| wanted != part_no) {
                continue;
            }
            jobs.push(Job {
                period: period.clone(),
                part_no,
                path,
            });
        }
    }
    jobs.sort_by(|left, right| {
        left.period
            .cmp(&right.period)
            .then(left.part_no.cmp(&right.part_no))
    });
    if jobs.is_empty() {
        bail!("no LL2 parts match the configured period/part selection");
    }
    Ok(jobs)
}

fn meta_key(period: &str) -> Vec<u8> {
    format!("period:{period}").into_bytes()
}

fn db_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_max_open_files(4_096);
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.increase_parallelism(32);
    options.set_max_background_jobs(32);
    options.set_write_buffer_size(256 * 1024 * 1024);
    options.set_max_write_buffer_number(8);
    options.set_min_write_buffer_number_to_merge(2);
    options.set_level_zero_file_num_compaction_trigger(12);
    options.set_level_zero_slowdown_writes_trigger(64);
    options.set_level_zero_stop_writes_trigger(96);
    options
}

fn open_replay_db(path: &Path) -> Result<DB> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let options = db_options();
    let cf_options = db_options();
    let descriptors = vec![
        ColumnFamilyDescriptor::new("default", cf_options.clone()),
        ColumnFamilyDescriptor::new(CF_LL2_MINUTE, cf_options.clone()),
        ColumnFamilyDescriptor::new(CF_LL2_MINUTE_STAGE, cf_options.clone()),
        ColumnFamilyDescriptor::new(CF_LL2_MINUTE_META, cf_options),
    ];
    DB::open_cf_descriptors(&options, path, descriptors)
        .with_context(|| format!("open LL2 minute RocksDB {}", path.display()))
}

fn open_existing_db(path: &Path) -> Result<DB> {
    let names = DB::list_cf(&Options::default(), path)
        .with_context(|| format!("list column families {}", path.display()))?;
    let descriptors: Vec<_> = names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
        .collect();
    DB::open_cf_descriptors(&Options::default(), path, descriptors)
        .with_context(|| format!("open existing LL2 minute RocksDB {}", path.display()))
}

fn meta_cf(db: &DB) -> Result<&ColumnFamily> {
    db.cf_handle(CF_LL2_MINUTE_META)
        .ok_or_else(|| anyhow!("RocksDB has no {CF_LL2_MINUTE_META} column family"))
}

fn status(db: &DB, period: &str) -> Result<Option<PeriodStatus>> {
    match db.get_cf(meta_cf(db)?, meta_key(period))? {
        None => Ok(None),
        Some(value) if value.as_slice() == b"writing" => Ok(Some(PeriodStatus::Writing)),
        Some(value) if value.as_slice() == b"done" => Ok(Some(PeriodStatus::Done)),
        Some(value) => bail!(
            "invalid LL2 period status {:?}",
            String::from_utf8_lossy(&value)
        ),
    }
}

fn put_status(db: &DB, period: &str, status: PeriodStatus) -> Result<()> {
    let value = match status {
        PeriodStatus::Writing => b"writing".as_slice(),
        PeriodStatus::Done => b"done".as_slice(),
    };
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.put_cf_opt(meta_cf(db)?, meta_key(period), value, &options)
        .with_context(|| format!("write LL2 period status {period}"))
}

fn claim_periods(db: &DB, periods: &[String]) -> Result<()> {
    for period in periods {
        match status(db, period)? {
            None => put_status(db, period, PeriodStatus::Writing)?,
            Some(PeriodStatus::Done) => {
                bail!("LL2 period {period} is already done; refuse overwrite")
            }
            Some(PeriodStatus::Writing) => {
                bail!("LL2 period {period} is incomplete; rerun with --reset-incomplete")
            }
        }
    }
    Ok(())
}

fn clear_incomplete(config: &Config, periods: &[String]) -> Result<()> {
    if !config.rocksdb_dir.exists() {
        return Ok(());
    }
    let db = open_existing_db(&config.rocksdb_dir)?;
    for period in periods {
        if status(&db, period)? == Some(PeriodStatus::Done) {
            bail!("refusing --reset-incomplete because period {period} is done");
        }
    }
    drop(db);
    let expected = "cme_ll2_1min_rocksdb";
    if config
        .rocksdb_dir
        .file_name()
        .and_then(|name| name.to_str())
        != Some(expected)
    {
        bail!(
            "refusing to remove unexpected RocksDB path {}",
            config.rocksdb_dir.display()
        );
    }
    fs::remove_dir_all(&config.rocksdb_dir).with_context(|| {
        format!(
            "remove incomplete LL2 RocksDB {}",
            config.rocksdb_dir.display()
        )
    })?;
    Ok(())
}

fn next_csv_field<'a>(line: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    if *offset >= line.len() {
        bail!("LL2 row ends before expected column");
    }
    if line[*offset] == b'"' {
        *offset += 1;
        let start = *offset;
        while *offset < line.len() && line[*offset] != b'"' {
            *offset += 1;
        }
        if *offset == line.len() || line.get(*offset + 1) == Some(&b'"') {
            bail!("unsupported quoted LL2 CSV field");
        }
        let end = *offset;
        *offset += 1;
        if line.get(*offset) == Some(&b',') {
            *offset += 1;
        } else if *offset != line.len() {
            bail!("unexpected byte after quoted LL2 CSV field");
        }
        return Ok(&line[start..end]);
    }
    let start = *offset;
    while *offset < line.len() && line[*offset] != b',' {
        if line[*offset] == b'"' {
            bail!("quote in unquoted LL2 CSV field");
        }
        *offset += 1;
    }
    let end = *offset;
    if line.get(*offset) == Some(&b',') {
        *offset += 1;
    }
    Ok(&line[start..end])
}

/// Source CSV legally omits trailing empty LL2 book cells. Identity columns
/// remain strict; only the depth projection uses this tolerant reader.
fn next_ll2_book_field<'a>(line: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    if *offset >= line.len() {
        return Ok(&[]);
    }
    next_csv_field(line, offset)
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

fn validate_header(line: &[u8]) -> Result<()> {
    let mut offset = 0;
    let mut fields = Vec::with_capacity(HEADER_COLUMNS);
    for _ in 0..HEADER_COLUMNS {
        fields.push(next_csv_field(strip_line_ending(line), &mut offset)?);
    }
    if offset != strip_line_ending(line).len() {
        bail!("LL2 header has more than {HEADER_COLUMNS} columns");
    }
    if fields[0] != b"#RIC"
        || fields[1] != b"Domain"
        || fields[2] != b"Date-Time"
        || fields[4] != b"Type"
        || fields[65] != b"Exch Time"
    {
        bail!("unexpected LL2 header identity columns");
    }
    for level in 1..=LL2_DEPTH_LEVELS {
        let base = 5 + (level - 1) * 6;
        let expected = [
            format!("L{level}-BidPrice"),
            format!("L{level}-BidSize"),
            format!("L{level}-BuyNo"),
            format!("L{level}-AskPrice"),
            format!("L{level}-AskSize"),
            format!("L{level}-SellNo"),
        ];
        for (offset, name) in expected.iter().enumerate() {
            if fields[base + offset] != name.as_bytes() {
                bail!("unexpected LL2 header at column {}", base + offset);
            }
        }
    }
    Ok(())
}

fn utf8<'a>(raw: &'a [u8], field: &str) -> Result<&'a str> {
    std::str::from_utf8(raw).with_context(|| format!("LL2 {field} is not UTF-8"))
}

fn parse_count(raw: &[u8]) -> Result<u32> {
    if raw.is_empty() {
        return Ok(0);
    }
    let mut value = 0u32;
    for byte in raw {
        if !byte.is_ascii_digit() {
            bail!("invalid LL2 book count {:?}", String::from_utf8_lossy(raw));
        }
        value = value
            .checked_mul(10)
            .and_then(|current| current.checked_add(u32::from(*byte - b'0')))
            .ok_or_else(|| anyhow!("LL2 book count overflow"))?;
    }
    Ok(value)
}

fn parse_scaled(raw: &[u8], field: &str) -> Result<i64> {
    if raw.is_empty() {
        return Ok(cme_tas_replay::MISSING_PRICE);
    }
    let mut index = 0usize;
    let negative = raw.first() == Some(&b'-');
    if negative {
        index = 1;
    }
    let integer_start = index;
    let mut integer = 0i128;
    while index < raw.len() && raw[index] != b'.' {
        if !raw[index].is_ascii_digit() {
            bail!("invalid LL2 {field} {:?}", String::from_utf8_lossy(raw));
        }
        integer = integer
            .checked_mul(10)
            .and_then(|current| current.checked_add(i128::from(raw[index] - b'0')))
            .ok_or_else(|| anyhow!("LL2 {field} overflow"))?;
        index += 1;
    }
    if index == integer_start {
        bail!("invalid LL2 {field} {:?}", String::from_utf8_lossy(raw));
    }
    let mut fraction = 0i128;
    let mut fraction_len = 0usize;
    if index < raw.len() {
        index += 1;
        while index < raw.len() {
            if !raw[index].is_ascii_digit() || fraction_len == 9 {
                bail!("invalid LL2 {field} {:?}", String::from_utf8_lossy(raw));
            }
            fraction = fraction
                .checked_mul(10)
                .and_then(|current| current.checked_add(i128::from(raw[index] - b'0')))
                .ok_or_else(|| anyhow!("LL2 {field} overflow"))?;
            fraction_len += 1;
            index += 1;
        }
    }
    for _ in fraction_len..9 {
        fraction *= 10;
    }
    let mut scaled = integer
        .checked_mul(1_000_000_000)
        .and_then(|current| current.checked_add(fraction))
        .ok_or_else(|| anyhow!("LL2 {field} overflow"))?;
    if negative {
        scaled = -scaled;
    }
    i64::try_from(scaled).map_err(|_| anyhow!("LL2 {field} does not fit i64"))
}

fn parse_digits(raw: &[u8]) -> Result<i32> {
    if raw.is_empty() || !raw.iter().all(u8::is_ascii_digit) {
        bail!(
            "invalid LL2 Date-Time component {:?}",
            String::from_utf8_lossy(raw)
        );
    }
    raw.iter().try_fold(0i32, |value, byte| {
        value
            .checked_mul(10)
            .and_then(|current| current.checked_add(i32::from(*byte - b'0')))
            .ok_or_else(|| anyhow!("LL2 Date-Time component overflow"))
    })
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i64::from(era * 146_097 + doe - 719_468)
}

fn parse_date_time_ns_fast(raw: &[u8]) -> Result<u64> {
    if raw.len() < 20
        || raw[4] != b'-'
        || raw[7] != b'-'
        || raw[10] != b'T'
        || raw[13] != b':'
        || raw[16] != b':'
        || raw.last() != Some(&b'Z')
    {
        bail!(
            "unsupported LL2 Date-Time {:?}",
            String::from_utf8_lossy(raw)
        );
    }
    let year = parse_digits(&raw[0..4])?;
    let month = parse_digits(&raw[5..7])? as u32;
    let day = parse_digits(&raw[8..10])? as u32;
    let hour = parse_digits(&raw[11..13])? as u32;
    let minute = parse_digits(&raw[14..16])? as u32;
    let second = parse_digits(&raw[17..19])? as u32;
    if !(1..=12).contains(&month) || day == 0 || hour >= 24 || minute >= 60 || second >= 60 {
        bail!("invalid LL2 Date-Time {:?}", String::from_utf8_lossy(raw));
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
        31,
        28 + u32::from(leap),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if day > month_days[(month - 1) as usize] {
        bail!("invalid LL2 Date-Time {:?}", String::from_utf8_lossy(raw));
    }
    let fraction_raw = match raw.get(19) {
        Some(b'.') => &raw[20..raw.len() - 1],
        Some(b'Z') => &[][..],
        _ => bail!(
            "unsupported LL2 Date-Time {:?}",
            String::from_utf8_lossy(raw)
        ),
    };
    if fraction_raw.len() > 9 || !fraction_raw.iter().all(u8::is_ascii_digit) {
        bail!(
            "invalid LL2 Date-Time fraction {:?}",
            String::from_utf8_lossy(raw)
        );
    }
    let mut fraction = parse_digits(fraction_raw)? as u64;
    for _ in fraction_raw.len()..9 {
        fraction *= 10;
    }
    let days = days_from_civil(year, month, day);
    if days < 0 {
        bail!(
            "LL2 Date-Time before Unix epoch {:?}",
            String::from_utf8_lossy(raw)
        );
    }
    let seconds = u64::try_from(days)?
        .checked_mul(86_400)
        .and_then(|value| {
            value.checked_add(u64::from(hour) * 3_600 + u64::from(minute) * 60 + u64::from(second))
        })
        .ok_or_else(|| anyhow!("LL2 Date-Time overflow"))?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| anyhow!("LL2 Date-Time overflow"))
}

fn route_for_ric(ric: &str, cache: &mut Option<(String, Option<Route>)>) -> Result<Option<Route>> {
    if let Some((cached_ric, route)) = cache.as_ref() {
        if cached_ric == ric {
            return Ok(route.clone());
        }
    }
    let route = match research_root_of(ric)? {
        None => None,
        Some(root) => Some(Route {
            ric: ric.to_string(),
            exchange: research_root_exchange(root)
                .ok_or_else(|| anyhow!("research root {root} has no exchange"))?
                .to_string(),
            product_root: root.to_string(),
        }),
    };
    *cache = Some((ric.to_string(), route.clone()));
    Ok(route)
}

fn parse_line(
    line: &[u8],
    source_seq: u64,
    calendar: &SessionCalendar,
    ric_filter: &Option<BTreeSet<String>>,
    route_cache: &mut Option<(String, Option<Route>)>,
) -> Result<Option<(Ll2MinuteKey, Ll2Minute)>> {
    let mut offset = 0;
    let line = strip_line_ending(line);
    let ric = utf8(next_csv_field(line, &mut offset)?, "#RIC")?;
    let domain = next_csv_field(line, &mut offset)?;
    let date_time = next_csv_field(line, &mut offset)?;
    let _gmt_offset = next_csv_field(line, &mut offset)?;
    let event_type = next_csv_field(line, &mut offset)?;
    let Some(route) = route_for_ric(ric, route_cache)? else {
        return Ok(None);
    };
    if ric_filter
        .as_ref()
        .is_some_and(|allowed| !allowed.contains(ric))
    {
        return Ok(None);
    }
    if domain != b"Market Price" || event_type != b"Normalized LL2" {
        bail!(
            "unexpected LL2 Domain/Type for {ric}: {:?}/{:?}",
            String::from_utf8_lossy(domain),
            String::from_utf8_lossy(event_type)
        );
    }
    let ts_utc_ns = parse_date_time_ns_fast(date_time)?;
    let ts_utc_sec = i64::try_from(ts_utc_ns / 1_000_000_000)
        .map_err(|_| anyhow!("LL2 timestamp out of i64 range for {ric}"))?;
    if !calendar.contains(&route.product_root, ts_utc_sec)? {
        return Ok(None);
    }
    let mut value = Ll2Minute::empty(ts_utc_ns, source_seq);
    for level in 0..LL2_DEPTH_LEVELS {
        value.bid_prices[level] =
            parse_scaled(next_ll2_book_field(line, &mut offset)?, "bid price")?;
        value.bid_sizes[level] = parse_scaled(next_ll2_book_field(line, &mut offset)?, "bid size")?;
        value.bid_counts[level] = parse_count(next_ll2_book_field(line, &mut offset)?)?;
        value.ask_prices[level] =
            parse_scaled(next_ll2_book_field(line, &mut offset)?, "ask price")?;
        value.ask_sizes[level] = parse_scaled(next_ll2_book_field(line, &mut offset)?, "ask size")?;
        value.ask_counts[level] = parse_count(next_ll2_book_field(line, &mut offset)?)?;
    }
    let _exch_time = next_ll2_book_field(line, &mut offset)?;
    if offset != line.len() {
        bail!("LL2 row for {ric} has extra columns");
    }
    let minute_utc_sec = (ts_utc_sec as u64 / 60) * 60;
    let key = Ll2MinuteKey {
        exchange: route.exchange,
        product_root: route.product_root,
        trading_day: tradeday_yyyymmdd(ts_utc_ns)?,
        ric: route.ric,
        minute_utc_sec,
    };
    Ok(Some((key, value)))
}

fn flush_stage(db: &DB, batch: &mut WriteBatch) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut options = WriteOptions::default();
    options.set_sync(false);
    db.write_opt(std::mem::take(batch), &options)
        .context("write LL2 minute stage batch")
}

fn replay_part(
    db: Option<&DB>,
    calendar: &SessionCalendar,
    job: &Job,
    ric_filter: &Option<BTreeSet<String>>,
    abort: &AtomicBool,
    max_source_rows: Option<u64>,
) -> Result<PartStats> {
    let file = File::open(&job.path).with_context(|| format!("open {}", job.path.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(INPUT_BUFFER_BYTES, file));
    let mut reader = BufReader::with_capacity(INPUT_BUFFER_BYTES, decoder);
    let mut line = Vec::with_capacity(1024);
    if job.part_no == 0 {
        reader
            .read_until(b'\n', &mut line)
            .context("read LL2 header")?;
        if line.is_empty() {
            bail!("LL2 part 0 is empty: {}", job.path.display());
        }
        validate_header(&line)
            .with_context(|| format!("validate LL2 header {}", job.path.display()))?;
        line.clear();
    }
    let stage_cf = db
        .map(|database| {
            database
                .cf_handle(CF_LL2_MINUTE_STAGE)
                .ok_or_else(|| anyhow!("missing {CF_LL2_MINUTE_STAGE}"))
        })
        .transpose()?;
    let mut stats = PartStats::default();
    let mut batch = WriteBatch::default();
    let mut current: Option<(Ll2MinuteKey, Ll2Minute)> = None;
    let mut last_order: Option<(String, u64)> = None;
    let mut route_cache = None;
    loop {
        if max_source_rows.is_some_and(|limit| stats.source_rows >= limit) {
            break;
        }
        line.clear();
        let bytes = reader
            .read_until(b'\n', &mut line)
            .with_context(|| format!("read {}", job.path.display()))?;
        if bytes == 0 {
            break;
        }
        if strip_line_ending(&line).is_empty() {
            stats.empty_rows += 1;
            continue;
        }
        stats.source_rows += 1;
        if stats.source_rows % 100_000 == 0 && abort.load(Ordering::Relaxed) {
            bail!("aborted while reading {}", job.path.display());
        }
        let parsed = parse_line(
            &line,
            stats.source_rows,
            calendar,
            ric_filter,
            &mut route_cache,
        )
        .with_context(|| {
            format!(
                "parse LL2 period={} part={} source_row={} prefix={:?}",
                job.period,
                job.part_no,
                stats.source_rows,
                String::from_utf8_lossy(
                    &strip_line_ending(&line)[..strip_line_ending(&line).len().min(160)]
                )
            )
        })?;
        let Some((key, value)) = parsed else {
            continue;
        };
        stats.routed_rows += 1;
        let order = (key.ric.clone(), value.source_ts_utc_ns);
        if last_order
            .as_ref()
            .is_some_and(|previous| order < *previous)
        {
            bail!(
                "LL2 source order regressed in {} at {}",
                job.path.display(),
                key.ric
            );
        }
        last_order = Some(order);
        match &mut current {
            Some((current_key, current_value)) if current_key == &key => {
                let update_count = current_value
                    .update_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("LL2 minute update_count overflow"))?;
                *current_value = Ll2Minute {
                    update_count,
                    ..value
                };
            }
            Some(_) => {
                let (finished_key, finished_value) = current.take().expect("current exists");
                if let (Some(database), Some(cf)) = (db, stage_cf) {
                    batch.put_cf(
                        cf,
                        encode_ll2_minute_stage_key(&finished_key, job.part_no)?,
                        encode_ll2_minute(&finished_value),
                    );
                    if batch.len() >= WRITE_BATCH_ROWS {
                        flush_stage(database, &mut batch)?;
                    }
                }
                stats.minute_rows += 1;
                current = Some((key, value));
            }
            None => current = Some((key, value)),
        }
        if stats.source_rows % PROGRESS_EVERY == 0 {
            info!(
                "ll2_1min progress period={} part={} source_rows={} routed_rows={} minute_rows={}",
                job.period, job.part_no, stats.source_rows, stats.routed_rows, stats.minute_rows
            );
        }
    }
    if let Some((key, value)) = current {
        if let (Some(_database), Some(cf)) = (db, stage_cf) {
            batch.put_cf(
                cf,
                encode_ll2_minute_stage_key(&key, job.part_no)?,
                encode_ll2_minute(&value),
            );
        }
        stats.minute_rows += 1;
    }
    if let (Some(database), Some(_cf)) = (db, stage_cf) {
        flush_stage(database, &mut batch)?;
    }
    Ok(stats)
}

fn compact_stage(db: &mut DB) -> Result<u64> {
    let stage = db
        .cf_handle(CF_LL2_MINUTE_STAGE)
        .ok_or_else(|| anyhow!("missing {CF_LL2_MINUTE_STAGE}"))?;
    let final_cf = db
        .cf_handle(CF_LL2_MINUTE)
        .ok_or_else(|| anyhow!("missing {CF_LL2_MINUTE}"))?;
    if db
        .iterator_cf(final_cf, IteratorMode::Start)
        .next()
        .is_some()
    {
        bail!("final LL2 minute CF is not empty before compaction");
    }
    let mut batch = WriteBatch::default();
    let mut current_key: Option<[u8; LL2_MINUTE_KEY_LEN]> = None;
    let mut current_value: Option<Ll2Minute> = None;
    let mut winner_part = 0u16;
    let mut written = 0u64;
    for item in db.iterator_cf(stage, IteratorMode::Start) {
        let (key, value) = item.context("iterate LL2 minute stage")?;
        let (_decoded, part) = decode_ll2_minute_stage_key(&key)?;
        let candidate = decode_ll2_minute(&value)?;
        let prefix: [u8; LL2_MINUTE_KEY_LEN] = key[..LL2_MINUTE_KEY_LEN]
            .try_into()
            .expect("stage key prefix");
        if current_key.as_ref() == Some(&prefix) {
            let active = current_value.as_mut().expect("value exists with key");
            let update_count = active
                .update_count
                .checked_add(candidate.update_count)
                .ok_or_else(|| anyhow!("LL2 compaction update_count overflow"))?;
            if candidate.ordering_tuple(part) > active.ordering_tuple(winner_part) {
                *active = candidate;
                winner_part = part;
            }
            active.update_count = update_count;
            continue;
        }
        if let (Some(previous_key), Some(previous_value)) =
            (current_key.take(), current_value.take())
        {
            batch.put_cf(final_cf, previous_key, encode_ll2_minute(&previous_value));
            written += 1;
            if batch.len() >= WRITE_BATCH_ROWS {
                let mut options = WriteOptions::default();
                options.set_sync(false);
                db.write_opt(std::mem::take(&mut batch), &options)
                    .context("write LL2 minute final batch")?;
            }
        }
        current_key = Some(prefix);
        current_value = Some(candidate);
        winner_part = part;
    }
    if let (Some(key), Some(value)) = (current_key, current_value) {
        batch.put_cf(final_cf, key, encode_ll2_minute(&value));
        written += 1;
    }
    if !batch.is_empty() {
        let mut options = WriteOptions::default();
        options.set_sync(false);
        db.write_opt(batch, &options)
            .context("write LL2 minute final tail")?;
    }
    db.drop_cf(CF_LL2_MINUTE_STAGE)
        .context("drop LL2 minute staging column family")?;
    Ok(written)
}

fn run(config: &Config, args: &Args) -> Result<()> {
    if config.workers == 0 {
        bail!("workers must be positive");
    }
    let periods = resolved_periods(config, args.period.as_deref())?;
    let rocksdb_dir = args
        .rocksdb_dir
        .as_deref()
        .unwrap_or(config.rocksdb_dir.as_path());
    if args.reset_incomplete {
        if args.rocksdb_dir.is_some() {
            bail!("--reset-incomplete only accepts the configured RocksDB directory");
        }
        clear_incomplete(config, &periods)?;
    }
    let jobs = discover_parts(config, &periods, args.part_index)?;
    let max_rows = args.max_source_rows;
    if max_rows.is_some() && jobs.len() != 1 {
        bail!("--max-source-rows requires exactly one selected LL2 part");
    }
    let calendar = Arc::new(SessionCalendar::load(&config.session_csv)?);
    let ric_filter =
        (!config.rics.is_empty()).then(|| config.rics.iter().cloned().collect::<BTreeSet<_>>());
    let db = (!args.dry_run)
        .then(|| open_replay_db(rocksdb_dir).map(Arc::new))
        .transpose()?;
    if let Some(database) = &db {
        claim_periods(database, &periods)?;
    }
    let started = Instant::now();
    let abort = Arc::new(AtomicBool::new(false));
    let first_error = Arc::new(Mutex::new(None::<String>));
    let (job_tx, job_rx) = unbounded::<Job>();
    for job in jobs.iter().cloned() {
        job_tx.send(job).expect("job receiver exists");
    }
    drop(job_tx);
    let workers = config.workers.min(jobs.len()).max(1);
    let mut joins = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let job_rx = job_rx.clone();
        let calendar = Arc::clone(&calendar);
        let abort = Arc::clone(&abort);
        let first_error = Arc::clone(&first_error);
        let db = db.clone();
        let ric_filter = ric_filter.clone();
        joins.push(thread::Builder::new().name(format!("cme-ll2-minute-{worker_id}")).spawn(move || -> PartStats {
            let mut total = PartStats::default();
            while let Ok(job) = job_rx.recv() {
                if abort.load(Ordering::Relaxed) {
                    break;
                }
                match replay_part(db.as_deref(), &calendar, &job, &ric_filter, &abort, max_rows) {
                    Ok(stats) => {
                        info!("ll2_1min part done period={} part={} source_rows={} routed_rows={} minute_rows={}", job.period, job.part_no, stats.source_rows, stats.routed_rows, stats.minute_rows);
                        total.add(&stats);
                    }
                    Err(error) => {
                        let mut guard = first_error.lock().expect("LL2 first-error lock");
                        if guard.is_none() {
                            *guard = Some(format!("{error:#}"));
                        }
                        abort.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
            total
        })?);
    }
    let mut total = PartStats::default();
    for join in joins {
        total.add(&join.join().map_err(|_| anyhow!("LL2 worker panicked"))?);
    }
    if let Some(error) = first_error.lock().expect("LL2 first-error lock").take() {
        bail!("LL2 worker failed: {error}");
    }
    if abort.load(Ordering::Relaxed) {
        bail!("LL2 minute replay aborted");
    }
    if let Some(database) = db {
        let mut database = Arc::try_unwrap(database)
            .map_err(|_| anyhow!("LL2 workers still hold RocksDB after join"))?;
        let final_rows = compact_stage(&mut database)?;
        for period in &periods {
            put_status(&database, period, PeriodStatus::Done)?;
        }
        info!("ll2_1min complete source_rows={} routed_rows={} off_session_rows={} stage_minutes={} final_minutes={} elapsed_s={:.1}", total.source_rows, total.routed_rows, total.off_session_rows, total.minute_rows, final_rows, started.elapsed().as_secs_f64());
    } else {
        info!(
            "ll2_1min dry run source_rows={} routed_rows={} stage_minutes={} elapsed_s={:.1}",
            total.source_rows,
            total.routed_rows,
            total.minute_rows,
            started.elapsed().as_secs_f64()
        );
    }
    Ok(())
}

fn verify(config: &Config, args: &Args) -> Result<()> {
    let rocksdb_dir = args
        .rocksdb_dir
        .as_deref()
        .unwrap_or(config.rocksdb_dir.as_path());
    let names = DB::list_cf(&Options::default(), rocksdb_dir)
        .with_context(|| format!("list column families {}", rocksdb_dir.display()))?;
    if names.iter().any(|name| name == CF_LL2_MINUTE_STAGE) {
        bail!("LL2 minute DB still has staging CF; replay is incomplete");
    }
    let db = open_existing_db(rocksdb_dir)?;
    for period in resolved_periods(config, args.period.as_deref())? {
        if status(&db, &period)? != Some(PeriodStatus::Done) {
            bail!("LL2 period {period} is not done");
        }
    }
    let cf = db
        .cf_handle(CF_LL2_MINUTE)
        .ok_or_else(|| anyhow!("missing {CF_LL2_MINUTE}"))?;
    let mut previous: Option<Vec<u8>> = None;
    let mut rows = 0u64;
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, value) = item.context("verify LL2 minute record")?;
        if previous
            .as_ref()
            .is_some_and(|last| key.as_ref() <= last.as_slice())
        {
            bail!("LL2 minute keys are not strictly ordered");
        }
        let decoded_key = cme_tas_replay::ll2_1min::decode_ll2_minute_key(&key)?;
        let decoded_value = decode_ll2_minute(&value)?;
        if decoded_value.source_ts_utc_ns / 1_000_000_000 < decoded_key.minute_utc_sec
            || decoded_value.source_ts_utc_ns / 1_000_000_000 >= decoded_key.minute_utc_sec + 60
        {
            bail!(
                "LL2 minute source timestamp is outside key minute for {}",
                decoded_key.ric
            );
        }
        previous = Some(key.to_vec());
        rows += 1;
    }
    println!("verified ll2 minute RocksDB rows={rows}");
    Ok(())
}

fn main() {
    env_logger::init();
    let args = Args::parse();
    let text = match fs::read_to_string(&args.config) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("read {}: {error}", args.config.display());
            std::process::exit(1);
        }
    };
    let config: Config = match toml::from_str(&text) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("parse {}: {error}", args.config.display());
            std::process::exit(1);
        }
    };
    let result = if args.verify {
        verify(&config, &args)
    } else {
        run(&config, &args)
    };
    if let Err(error) = result {
        warn!("cme_ll2_1min_replay failed: {error:#}");
        eprintln!("cme_ll2_1min_replay failed: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::ll2_1min::{
        decode_ll2_minute, encode_ll2_minute, encode_ll2_minute_stage_key, Ll2Minute, Ll2MinuteKey,
    };

    fn calendar() -> SessionCalendar {
        let mut by_group = BTreeMap::new();
        by_group.insert("fx".to_string(), vec![(1_767_308_400, 1_767_400_000)]);
        SessionCalendar { by_group }
    }

    fn source_line(ask: &str) -> Vec<u8> {
        let mut fields = vec![
            "ADF26".to_string(),
            "Market Price".to_string(),
            "2026-01-01T23:00:00.019074617Z".to_string(),
            "-6".to_string(),
            "Normalized LL2".to_string(),
        ];
        for level in 0..LL2_DEPTH_LEVELS {
            fields.extend([
                format!("0.66{level}"),
                "1".to_string(),
                "2".to_string(),
                ask.to_string(),
                "3".to_string(),
                "4".to_string(),
            ]);
        }
        fields.push("23:00:00.000000000".to_string());
        assert_eq!(fields.len(), HEADER_COLUMNS);
        fields.join(",").into_bytes()
    }

    #[test]
    fn projected_row_keeps_ten_levels_and_chicago_trading_day() {
        let calendar = calendar();
        let mut cache = None;
        let (key, row) = parse_line(&source_line("0.66785"), 9, &calendar, &None, &mut cache)
            .unwrap()
            .unwrap();
        assert_eq!(key.exchange, "CME");
        assert_eq!(key.product_root, "AD");
        assert_eq!(key.trading_day, 20260102);
        assert_eq!(key.minute_utc_sec, 1_767_308_400);
        assert_eq!(row.ask_prices[0], 667_850_000);
        assert_eq!(row.ask_counts[9], 4);
    }

    #[test]
    fn trailing_empty_depth_cells_are_source_nulls_not_parse_errors() {
        let calendar = calendar();
        let mut cache = None;
        let line = b"ADF26,Market Price,2026-01-01T23:00:00.019074617Z,-6,Normalized LL2";
        let (_key, row) = parse_line(line, 1, &calendar, &None, &mut cache)
            .unwrap()
            .unwrap();
        assert_eq!(
            row.bid_prices,
            [cme_tas_replay::MISSING_PRICE; LL2_DEPTH_LEVELS]
        );
        assert_eq!(row.ask_counts, [0; LL2_DEPTH_LEVELS]);
    }

    #[test]
    fn compaction_uses_last_stage_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cme_ll2_1min_rocksdb");
        let mut db = open_replay_db(&path).unwrap();
        let stage = db.cf_handle(CF_LL2_MINUTE_STAGE).unwrap();
        let key = Ll2MinuteKey {
            exchange: "CME".to_string(),
            product_root: "ES".to_string(),
            trading_day: 20240109,
            ric: "ESH24".to_string(),
            minute_utc_sec: 1_704_754_800,
        };
        let mut first = Ll2Minute::empty(1_704_754_801_000_000_000, 1);
        first.bid_prices[0] = 4_798_000_000_000;
        let mut second = first.clone();
        second.source_ts_utc_ns += 1;
        second.source_seq = 2;
        second.update_count = 2;
        second.bid_prices[0] += 250_000_000;
        db.put_cf(
            stage,
            encode_ll2_minute_stage_key(&key, 0).unwrap(),
            encode_ll2_minute(&first),
        )
        .unwrap();
        db.put_cf(
            stage,
            encode_ll2_minute_stage_key(&key, 1).unwrap(),
            encode_ll2_minute(&second),
        )
        .unwrap();
        assert_eq!(compact_stage(&mut db).unwrap(), 1);
        let final_cf = db.cf_handle(CF_LL2_MINUTE).unwrap();
        let value = db
            .get_cf(
                final_cf,
                cme_tas_replay::ll2_1min::encode_ll2_minute_key(&key).unwrap(),
            )
            .unwrap()
            .unwrap();
        let final_row = decode_ll2_minute(&value).unwrap();
        assert_eq!(final_row.update_count, 3);
        assert_eq!(final_row.bid_prices[0], second.bid_prices[0]);
        assert!(db.cf_handle(CF_LL2_MINUTE_STAGE).is_none());
    }
}
