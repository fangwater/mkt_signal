//! Replay local CME TAS gzip parts into RocksDB.
//!
//! Source parts are concatenated gzip members; `MultiGzDecoder` is required.
//! One process, N part workers, one live DB. Listed periods share the worker
//! pool. Unhandled rows are dumped and skipped; they do not abort the run.
//! A period already marked done / writing is still refused. Writes trade,
//! special, quote, RIC rename, and price-limit (daily cage).

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::{
    classify, decode_period_status, encode_cme_price_limit, encode_cme_quote, encode_cme_special,
    overlay_price_limit,
    encode_cme_trade, encode_key, encode_period_status, encode_symbology_change, is_research_ric,
    parse_aggressor, parse_change_type, parse_date_time_ns, parse_exch_hms_ns, parse_price_e9,
    parse_volume, period_meta_key, quote_has_complete_side, tradeday_yyyymmdd, validate_period,
    ColumnRules, EventKind, PeriodStatus, SlimPriceLimit, SlimQuote, SlimSymbologyChange,
    SlimTrade, CF_CME_PRICE_LIMIT, CF_CME_QUOTE, CF_CME_SPECIAL, CF_CME_TRADE, CF_REPLAY_META,
    CF_SYMBOLOGY_CHANGE, KEY_LEN, RESEARCH_PRODUCT_ROOTS,
};
use csv::StringRecord;
use flate2::read::MultiGzDecoder;
use log::{error, info, LevelFilter, Log, Metadata, Record};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_replay")]
#[command(about = "Replay CME TAS gzip parts into RocksDB")]
struct Args {
    #[arg(long, default_value = "config/cme_tas_replay.toml")]
    config: PathBuf,
    #[arg(long)]
    max_source_rows: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayConfig {
    data_root: PathBuf,
    period: String,
    /// Extra TAS periods to run in the same process / RocksDB. Empty = `period` only.
    #[serde(default)]
    periods: Vec<String>,
    rocksdb_dir: PathBuf,
    #[serde(default)]
    rics: Vec<String>,
    /// Exclusive part to replay when `workers == 1`. Ignored when `workers > 1`.
    #[serde(default)]
    part_index: usize,
    /// Concurrent part workers. 1 = single part (`part_index`). >1 = all parts, this many at a time.
    #[serde(default = "default_workers")]
    workers: usize,
    #[serde(default)]
    max_source_rows: Option<u64>,
    #[serde(default = "default_column_rules")]
    column_rules: PathBuf,
    /// Progress every N source rows. 0 disables the heartbeat.
    #[serde(default = "default_progress_every")]
    progress_every: u64,
    #[serde(default = "default_log_path")]
    log_path: PathBuf,
    /// One line per unhandled row. The run continues.
    #[serde(default = "default_unparsed_path")]
    unparsed_path: PathBuf,
    /// Stop after this many CME sessions from the first futures row. 0 = no cap.
    #[serde(default)]
    max_tradedays: u32,
}

fn default_progress_every() -> u64 {
    1_000_000
}

fn default_workers() -> usize {
    1
}

fn default_column_rules() -> PathBuf {
    PathBuf::from("../preprocess/lseg/tas_column_rules.json")
}

fn default_log_path() -> PathBuf {
    PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay.log")
}

fn default_unparsed_path() -> PathBuf {
    PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay_unparsed.log")
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
    }
    Ok(periods)
}

struct FileLogger {
    file: Mutex<File>,
}

impl Log for FileLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::Level::Info
            && (metadata.target().starts_with("cme_tas_replay") || metadata.target() == "cme_tas_replay")
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
    implied_yield: usize,
    change_type: usize,
    old_value: usize,
    new_value: usize,
}

struct HeaderMap {
    names: Vec<String>,
    groups: Vec<String>,
    by_name: BTreeMap<String, usize>,
    idx: ColIdx,
    /// Forbidden-group columns that still fail the row when nonempty.
    /// Price-limit, settle-IV, and implied-yield columns are excluded here
    /// and checked with the same kind/qualifier rules as before.
    forbidden_idxs: Vec<usize>,
}

fn cell_at(record: &StringRecord, idx: usize) -> &str {
    record.get(idx).map(str::trim).unwrap_or("")
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
            implied_yield: Self::require_idx(&by_name, "Implied Yield")?,
            change_type: Self::require_idx(&by_name, "Change Type")?,
            old_value: Self::require_idx(&by_name, "Old Value")?,
            new_value: Self::require_idx(&by_name, "New Value")?,
        };
        let mut forbidden_idxs = Vec::new();
        for (index, name) in names.iter().enumerate() {
            let group = groups[index].as_str();
            if rules.is_forbidden_futures_group(group)
                && !rules.is_allowed_price_limit_column(name)
                && !rules.is_allowed_implied_yield_column(name)
                && !rules.is_allowed_settle_iv_column(name)
            {
                forbidden_idxs.push(index);
            }
        }
        Ok(Self {
            names,
            groups,
            by_name,
            idx,
            forbidden_idxs,
        })
    }

    fn cell<'a>(&self, record: &'a StringRecord, name: &str) -> &'a str {
        self.by_name
            .get(name)
            .and_then(|&idx| record.get(idx))
            .map(str::trim)
            .unwrap_or("")
    }

    fn required_at<'a>(&self, record: &'a StringRecord, idx: usize, name: &str) -> Result<&'a str> {
        let value = cell_at(record, idx);
        if value.is_empty() {
            bail!("unhandled empty required TAS field {name:?}");
        }
        Ok(value)
    }

    fn filled_cells(&self, record: &StringRecord) -> String {
        let mut out = String::new();
        for name in &self.names {
            let value = self.cell(record, name);
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
}

const UNPARSED_ERROR_CAP: u64 = 20;

struct UnparsedSink {
    file: Mutex<File>,
    dumped: AtomicU64,
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
        filled: &str,
        err: &anyhow::Error,
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
        if n <= UNPARSED_ERROR_CAP {
            error!(
                "cme_tas_replay unparsed period={period} part_no={part_no} source_row={source_row} part={} {filled}",
                part.display()
            );
            if n == UNPARSED_ERROR_CAP {
                error!(
                    "cme_tas_replay further unparsed rows go only to the unparsed file"
                );
            }
        }
    }
}

#[derive(Default)]
struct Census {
    source_rows: u64,
    written_trades: u64,
    written_specials: u64,
    written_quotes: u64,
    written_renames: u64,
    written_limits: u64,
    counted: BTreeMap<&'static str, u64>,
    skipped_unmapped: u64,
    skipped_ric_filter: u64,
    skipped_after_window: u64,
    skipped_unparsed: u64,
}

impl Census {
    fn merge_from(&mut self, other: &Census) {
        self.source_rows += other.source_rows;
        self.written_trades += other.written_trades;
        self.written_specials += other.written_specials;
        self.written_quotes += other.written_quotes;
        self.written_renames += other.written_renames;
        self.written_limits += other.written_limits;
        self.skipped_unmapped += other.skipped_unmapped;
        self.skipped_ric_filter += other.skipped_ric_filter;
        self.skipped_after_window += other.skipped_after_window;
        self.skipped_unparsed += other.skipped_unparsed;
        for (name, count) in &other.counted {
            *self.counted.entry(name).or_insert(0) += count;
        }
    }
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

fn open_rocksdb(path: &Path) -> Result<DB> {
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
    db_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    db_opts.increase_parallelism(32);
    db_opts.set_max_background_jobs(32);
    db_opts.set_write_buffer_size(256 * 1024 * 1024);
    db_opts.set_max_write_buffer_number(8);
    db_opts.set_min_write_buffer_number_to_merge(2);
    db_opts.set_level_zero_file_num_compaction_trigger(8);
    db_opts.set_level_zero_slowdown_writes_trigger(64);
    db_opts.set_level_zero_stop_writes_trigger(96);
    db_opts.set_max_subcompactions(4);
    let mut cf_opts = Options::default();
    cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    cf_opts.set_write_buffer_size(256 * 1024 * 1024);
    cf_opts.set_max_write_buffer_number(8);
    cf_opts.set_min_write_buffer_number_to_merge(2);
    cf_opts.set_level_zero_file_num_compaction_trigger(8);
    cf_opts.set_level_zero_slowdown_writes_trigger(64);
    cf_opts.set_level_zero_stop_writes_trigger(96);
    let mut quote_opts = cf_opts.clone();
    quote_opts.set_write_buffer_size(512 * 1024 * 1024);
    quote_opts.set_max_write_buffer_number(12);
    quote_opts.set_level_zero_file_num_compaction_trigger(16);
    quote_opts.set_level_zero_slowdown_writes_trigger(80);
    quote_opts.set_level_zero_stop_writes_trigger(128);
    let descriptors = vec![
        ColumnFamilyDescriptor::new("default", cf_opts.clone()),
        ColumnFamilyDescriptor::new(CF_CME_TRADE, cf_opts.clone()),
        ColumnFamilyDescriptor::new(CF_CME_SPECIAL, cf_opts.clone()),
        ColumnFamilyDescriptor::new(CF_CME_QUOTE, quote_opts),
        ColumnFamilyDescriptor::new(CF_SYMBOLOGY_CHANGE, cf_opts.clone()),
        ColumnFamilyDescriptor::new(CF_CME_PRICE_LIMIT, cf_opts.clone()),
        ColumnFamilyDescriptor::new(CF_REPLAY_META, cf_opts),
    ];
    DB::open_cf_descriptors(&db_opts, path, descriptors)
        .with_context(|| format!("open rocksdb {}", path.display()))
}

fn replay_meta_cf(db: &DB) -> Result<&ColumnFamily> {
    db.cf_handle(CF_REPLAY_META)
        .ok_or_else(|| anyhow!("column family {CF_REPLAY_META} missing"))
}

fn claim_period(db: &DB, period: &str) -> Result<()> {
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

fn finish_period(db: &DB, period: &str) -> Result<()> {
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

fn flush_batch(db: &DB, batch: &mut WriteBatch) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut opts = WriteOptions::default();
    opts.set_sync(false);
    db.write_opt(std::mem::take(batch), &opts)
        .context("write rocksdb batch")?;
    Ok(())
}

fn replay_part(
    config: &ReplayConfig,
    db: &DB,
    period: &str,
    part: &Path,
    part_no: u16,
    ric_filter: &Option<BTreeSet<String>>,
    abort: &AtomicBool,
    unparsed: &UnparsedSink,
) -> Result<Census> {
    if abort.load(Ordering::Relaxed) {
        bail!("aborted before opening {}", part.display());
    }
    let rules = ColumnRules::load(&config.column_rules)?;
    let cf_trade = db
        .cf_handle(CF_CME_TRADE)
        .ok_or_else(|| anyhow!("column family {CF_CME_TRADE} missing"))?;
    let cf_special = db
        .cf_handle(CF_CME_SPECIAL)
        .ok_or_else(|| anyhow!("column family {CF_CME_SPECIAL} missing"))?;
    let cf_quote = db
        .cf_handle(CF_CME_QUOTE)
        .ok_or_else(|| anyhow!("column family {CF_CME_QUOTE} missing"))?;
    let cf_rename = db
        .cf_handle(CF_SYMBOLOGY_CHANGE)
        .ok_or_else(|| anyhow!("column family {CF_SYMBOLOGY_CHANGE} missing"))?;
    let cf_limit = db
        .cf_handle(CF_CME_PRICE_LIMIT)
        .ok_or_else(|| anyhow!("column family {CF_CME_PRICE_LIMIT} missing"))?;

    let file = File::open(part).with_context(|| format!("open {}", part.display()))?;
    let compressed_len = file
        .metadata()
        .with_context(|| format!("stat {}", part.display()))?
        .len();
    // Tick History parts are concatenated gzip members. GzDecoder would stop
    // after the first member and look like a clean EOF.
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(16 * 1024 * 1024, file));
    // Only part 0 carries the 294-column header. Later parts continue the
    // same CSV stream: the first line is already a data row.
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(decoder);
    let map = if part_no == 0 {
        let headers = reader
            .records()
            .next()
            .ok_or_else(|| anyhow!("part 0 is empty"))?
            .context("read TAS header from part 0")?;
        if headers.get(0).map(str::trim) != Some("#RIC") {
            bail!(
                "part 0 first row is {:?}, expected TAS header starting with #RIC",
                headers.get(0)
            );
        }
        HeaderMap::from_headers(&headers, &rules)?
    } else {
        let header_part = part.with_file_name("merged-Data-part-000000.csv.gz");
        let header_file = File::open(&header_part).with_context(|| {
            format!("open part 0 header {}", header_part.display())
        })?;
        let header_decoder = MultiGzDecoder::new(BufReader::with_capacity(1024 * 1024, header_file));
        let mut header_reader = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(header_decoder);
        let headers = header_reader
            .headers()
            .context("read TAS header from part 0 for later part")?
            .clone();
        HeaderMap::from_headers(&headers, &rules)?
    };

    let mut census = Census::default();
    let mut batch = WriteBatch::default();
    let mut last_trade_key: Option<[u8; KEY_LEN]> = None;
    let mut last_special_key: Option<[u8; KEY_LEN]> = None;
    let mut last_quote_key: Option<[u8; KEY_LEN]> = None;
    let mut last_rename_key: Option<[u8; KEY_LEN]> = None;
    let mut last_limit_key: Option<[u8; KEY_LEN]> = None;
    let mut last_trade_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_special_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_quote_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_rename_ric_ts: Option<(String, u64, u32)> = None;
    let mut last_limit_ric_ts: Option<(String, u64, u32)> = None;
    let mut ric_cache: Option<(String, bool)> = None;
    let mut remaining = config.max_source_rows;
    let mut first_tradeday: Option<u32> = None;
    let mut last_tradeday: Option<u32> = None;
    let mut seen_tradedays: BTreeMap<u32, ()> = BTreeMap::new();
    let started = Instant::now();
    info!(
        "cme_tas_replay start period={} part={} part_no={} research_roots={} extra_rics={:?} max_source_rows={:?} max_tradedays={} progress_every={}",
        period,
        part.display(),
        part_no,
        RESEARCH_PRODUCT_ROOTS.len(),
        config.rics,
        config.max_source_rows,
        config.max_tradedays,
        config.progress_every
    );

    for record in reader.records() {
        if remaining.is_some_and(|left| left == 0) {
            break;
        }
        let record = record.with_context(|| format!("read TAS row from {}", part.display()))?;
        census.source_rows += 1;
        if let Some(left) = remaining.as_mut() {
            *left = left.saturating_sub(1);
        }
        if census.source_rows % 100_000 == 0 && abort.load(Ordering::Relaxed) {
            bail!("aborted while reading {}", part.display());
        }
        let source_row = census.source_rows;
        let parsed = (|| -> Result<bool> {
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

        let ric = map.required_at(&record, map.idx.ric, "#RIC")?;
        if ric.starts_with('.') {
            if config.progress_every > 0 && census.source_rows % config.progress_every == 0 {
                let elapsed = started.elapsed().as_secs_f64().max(0.001);
                info!(
                    "cme_tas_replay progress period={} part_no={} source_rows={} written_trades={} written_specials={} written_quotes={} written_renames={} written_limits={} last_ric={} kind=index_skip rows_per_s={:.0} elapsed_s={:.1}",
                    period,
                    part_no,
                    census.source_rows,
                    census.written_trades,
                    census.written_specials,
                    census.written_quotes,
                    census.written_renames,
                    census.written_limits,
                    ric,
                    census.source_rows as f64 / elapsed,
                    elapsed
                );
            }
            *census.counted.entry("index_skip").or_insert(0) += 1;
            return Ok(false);
        }
        let mapped = match &ric_cache {
            Some((prev, mapped)) if prev == ric => *mapped,
            _ => {
                let mapped = is_research_ric(ric)?;
                ric_cache = Some((ric.to_string(), mapped));
                mapped
            }
        };
        if !mapped {
            if config.progress_every > 0 && census.source_rows % config.progress_every == 0 {
                let elapsed = started.elapsed().as_secs_f64().max(0.001);
                info!(
                    "cme_tas_replay progress period={} part_no={} source_rows={} written_trades={} written_specials={} written_quotes={} written_renames={} written_limits={} skipped_unmapped={} last_ric={} kind=unmapped_skip rows_per_s={:.0} elapsed_s={:.1}",
                    period,
                    part_no,
                    census.source_rows,
                    census.written_trades,
                    census.written_specials,
                    census.written_quotes,
                    census.written_renames,
                    census.written_limits,
                    census.skipped_unmapped + 1,
                    ric,
                    census.source_rows as f64 / elapsed,
                    elapsed
                );
            }
            *census.counted.entry("unmapped_skip").or_insert(0) += 1;
            census.skipped_unmapped += 1;
            return Ok(false);
        }
        let date_time = map.required_at(&record, map.idx.date_time, "Date-Time")?;
        let event_type = map.required_at(&record, map.idx.event_type, "Type")?;
        let price = cell_at(&record, map.idx.price);
        let volume = cell_at(&record, map.idx.volume);
        let qualifiers = cell_at(&record, map.idx.qualifiers);
        let up_lim = cell_at(&record, map.idx.up_lim);
        let lo_lim = cell_at(&record, map.idx.lo_lim);
        let bid = cell_at(&record, map.idx.bid);
        let bid_size = cell_at(&record, map.idx.bid_size);
        let ask = cell_at(&record, map.idx.ask);
        let ask_size = cell_at(&record, map.idx.ask_size);
        let exch_time = cell_at(&record, map.idx.exch_time);

        for &idx in &map.forbidden_idxs {
            let value = cell_at(&record, idx);
            if !value.is_empty() {
                bail!(
                    "unhandled nonempty TAS column {:?}={value:?} on futures {ric} (group {})",
                    map.names[idx],
                    map.groups[idx]
                );
            }
        }

        let mut kind = classify(ric, event_type, price, volume, qualifiers)
            .with_context(|| format!("classify {ric} {date_time} in {}", part.display()))?;
        if kind == EventKind::DropEmptyTrade {
            kind = overlay_price_limit(kind, up_lim, lo_lim)?;
        }
        if kind == EventKind::CmeQuote {
            let probe = SlimQuote {
                ric: String::new(),
                ts_utc_ns: 0,
                exch_hms_ns: 0,
                bid: parse_price_e9(bid)?,
                bid_size: parse_volume(bid_size)?,
                ask: parse_price_e9(ask)?,
                ask_size: parse_volume(ask_size)?,
            };
            if !quote_has_complete_side(&probe)? {
                kind = EventKind::DropEmptyQuote;
            }
        }
        if kind != EventKind::CmePriceLimit && (!up_lim.is_empty() || !lo_lim.is_empty()) {
            if kind != EventKind::CmeQuote && kind != EventKind::DropEmptyQuote {
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
            *census.counted.entry("price_limit_ignored").or_insert(0) += 1;
        }
        if kind != EventKind::DropSettleIv {
            let value = cell_at(&record, map.idx.imp_vol);
            if !value.is_empty() {
                bail!(
                    "unhandled nonempty TAS column \"Imp. Vol.\"={value:?} on {} {ric} {date_time}",
                    kind.as_str()
                );
            }
        }
        let implied_yield = cell_at(&record, map.idx.implied_yield);
        if !implied_yield.is_empty() {
            if kind != EventKind::CmeQuote && kind != EventKind::DropEmptyQuote {
                bail!(
                    "unhandled nonempty TAS column \"Implied Yield\"={implied_yield:?} on {} {ric} {date_time}",
                    kind.as_str()
                );
            }
            *census.counted.entry("implied_yield_ignored").or_insert(0) += 1;
        }
        *census.counted.entry(kind.as_str()).or_insert(0) += 1;
        if config.progress_every > 0 && census.source_rows % config.progress_every == 0 {
            let elapsed = started.elapsed().as_secs_f64().max(0.001);
            info!(
                "cme_tas_replay progress period={} part_no={} source_rows={} written_trades={} written_specials={} written_quotes={} written_renames={} written_limits={} skipped_unmapped={} skipped_ric_filter={} skipped_unparsed={} last_ric={} last_ts={} kind={} rows_per_s={:.0} elapsed_s={:.1}",
                period,
                part_no,
                census.source_rows,
                census.written_trades,
                census.written_specials,
                census.written_quotes,
                census.written_renames,
                census.written_limits,
                census.skipped_unmapped,
                census.skipped_ric_filter,
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
        let persist = matches!(
            kind,
            EventKind::CmeTrade
                | EventKind::CmeSpecial
                | EventKind::CmeQuote
                | EventKind::SymbologyChange
                | EventKind::CmePriceLimit
        );
        if !persist {
            return Ok(false);
        }

        let ts_utc_ns = parse_date_time_ns(date_time)?;
        let tradeday = tradeday_yyyymmdd(ts_utc_ns)?;
        if first_tradeday.is_none() {
            first_tradeday = Some(tradeday);
            info!(
                "cme_tas_replay first futures tradeday={tradeday} part_no={part_no} ric={ric} ts={date_time}"
            );
        }
        if !seen_tradedays.contains_key(&tradeday)
            && config.max_tradedays > 0
            && seen_tradedays.len() as u32 >= config.max_tradedays
        {
            info!(
                "cme_tas_replay reached max_tradedays={} first={:?} this={} ric={ric} ts={date_time}; stop",
                config.max_tradedays, first_tradeday, tradeday
            );
            census.skipped_after_window += 1;
            return Ok(true);
        }
        seen_tradedays.insert(tradeday, ());
        last_tradeday = Some(tradeday);
        let last_ric_ts = match kind {
            EventKind::CmeTrade => &mut last_trade_ric_ts,
            EventKind::CmeSpecial => &mut last_special_ric_ts,
            EventKind::CmeQuote => &mut last_quote_ric_ts,
            EventKind::SymbologyChange => &mut last_rename_ric_ts,
            EventKind::CmePriceLimit => &mut last_limit_ric_ts,
            _ => unreachable!("non-persist kind already filtered"),
        };
        let seq = match last_ric_ts {
            Some((prev_ric, prev_ts, prev_seq)) if prev_ric == ric && *prev_ts == ts_utc_ns => {
                prev_seq
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("seq overflow for {ric} at {date_time}"))?
            }
            _ => 0,
        };
        *last_ric_ts = Some((ric.to_string(), ts_utc_ns, seq));
        let key = encode_key(ric, ts_utc_ns, part_no, seq)?;
        let last_key = match kind {
            EventKind::CmeTrade => &mut last_trade_key,
            EventKind::CmeSpecial => &mut last_special_key,
            EventKind::CmeQuote => &mut last_quote_key,
            EventKind::SymbologyChange => &mut last_rename_key,
            EventKind::CmePriceLimit => &mut last_limit_key,
            _ => unreachable!("non-persist kind already filtered"),
        };
        if last_key.is_some_and(|prev| key <= prev) {
            bail!(
                "TAS key is not strictly increasing for {ric} at {date_time} in part {part_no}"
            );
        }
        *last_key = Some(key);
        match kind {
            EventKind::CmeTrade => {
                let rec = SlimTrade {
                    ric: ric.to_string(),
                    ts_utc_ns,
                    exch_hms_ns: parse_exch_hms_ns(exch_time)?,
                    price: parse_price_e9(price)?,
                    volume: parse_volume(volume)?,
                    bid: parse_price_e9(bid)?,
                    bid_size: parse_volume(bid_size)?,
                    ask: parse_price_e9(ask)?,
                    ask_size: parse_volume(ask_size)?,
                    aggressor: parse_aggressor(qualifiers)?,
                };
                batch.put_cf(&cf_trade, key, encode_cme_trade(&rec)?);
                census.written_trades += 1;
            }
            EventKind::CmeSpecial => {
                let rec = SlimTrade {
                    ric: ric.to_string(),
                    ts_utc_ns,
                    exch_hms_ns: parse_exch_hms_ns(exch_time)?,
                    price: parse_price_e9(price)?,
                    volume: parse_volume(volume)?,
                    bid: parse_price_e9(bid)?,
                    bid_size: parse_volume(bid_size)?,
                    ask: parse_price_e9(ask)?,
                    ask_size: parse_volume(ask_size)?,
                    aggressor: parse_aggressor(qualifiers)?,
                };
                batch.put_cf(&cf_special, key, encode_cme_special(&rec)?);
                census.written_specials += 1;
            }
            EventKind::CmeQuote => {
                let rec = SlimQuote {
                    ric: ric.to_string(),
                    ts_utc_ns,
                    exch_hms_ns: parse_exch_hms_ns(exch_time)?,
                    bid: parse_price_e9(bid)?,
                    bid_size: parse_volume(bid_size)?,
                    ask: parse_price_e9(ask)?,
                    ask_size: parse_volume(ask_size)?,
                };
                batch.put_cf(&cf_quote, key, encode_cme_quote(&rec)?);
                census.written_quotes += 1;
            }
            EventKind::SymbologyChange => {
                let rec = SlimSymbologyChange {
                    ric: ric.to_string(),
                    ts_utc_ns,
                    change_type: parse_change_type(cell_at(&record, map.idx.change_type))?,
                    old_value: cell_at(&record, map.idx.old_value).to_string(),
                    new_value: cell_at(&record, map.idx.new_value).to_string(),
                };
                batch.put_cf(&cf_rename, key, encode_symbology_change(&rec)?);
                census.written_renames += 1;
            }
            EventKind::CmePriceLimit => {
                let rec = SlimPriceLimit {
                    ric: ric.to_string(),
                    ts_utc_ns,
                    up_lim: parse_price_e9(up_lim)?,
                    lo_lim: parse_price_e9(lo_lim)?,
                };
                batch.put_cf(&cf_limit, key, encode_cme_price_limit(&rec)?);
                census.written_limits += 1;
            }
            _ => unreachable!("non-persist kind already filtered"),
        }
        if batch.len() >= 8192 {
            flush_batch(db, &mut batch)?;
        }
        Ok(false)
        })();
        match parsed {
            Ok(true) => break,
            Ok(false) => {}
            Err(err) => {
                unparsed.dump(period, part, part_no, source_row, &map.filled_cells(&record), &err);
                census.skipped_unparsed += 1;
                *census.counted.entry("unparsed_skip").or_insert(0) += 1;
            }
        }
    }
    flush_batch(db, &mut batch)?;
    let decoder = reader.into_inner();
    let mut file = decoder.into_inner().into_inner();
    let pos = file
        .stream_position()
        .with_context(|| format!("tell {}", part.display()))?;
    if config.max_source_rows.is_none() && config.max_tradedays == 0 && pos < compressed_len {
        bail!(
            "gzip ended at byte {pos} of {compressed_len} in {}; concatenated member was not consumed",
            part.display()
        );
    }

    let elapsed_ms = started.elapsed().as_millis();
    info!(
        "cme_tas_replay finished period={} part={} part_no={} source_rows={} written_trades={} written_specials={} written_quotes={} written_renames={} written_limits={} skipped_unmapped={} skipped_ric_filter={} skipped_unparsed={} skipped_after_window={} first_tradeday={:?} last_tradeday={:?} elapsed_ms={}",
        period,
        part.display(),
        part_no,
        census.source_rows,
        census.written_trades,
        census.written_specials,
        census.written_quotes,
        census.written_renames,
        census.written_limits,
        census.skipped_unmapped,
        census.skipped_ric_filter,
        census.skipped_unparsed,
        census.skipped_after_window,
        first_tradeday,
        last_tradeday,
        elapsed_ms
    );
    for (name, count) in &census.counted {
        info!("cme_tas_replay class part_no={part_no} {name}={count}");
    }
    Ok(census)
}

fn period_dir_for(config: &ReplayConfig, period: &str) -> PathBuf {
    config.data_root.join(format!(
        "shanghai_evolution_futures_time_and_sales_ric_list_0_tas_{period}"
    ))
}

fn collect_jobs(config: &ReplayConfig, periods: &[String]) -> Result<Vec<(String, u16, PathBuf)>> {
    if config.workers == 0 {
        bail!("workers must be >= 1");
    }
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
            jobs.push((period.clone(), part_number(part)?, part.clone()));
            continue;
        }
        for part in all {
            jobs.push((period.clone(), part_number(&part)?, part));
        }
    }
    if jobs.is_empty() {
        bail!("no TAS gzip parts for periods {periods:?}");
    }
    Ok(jobs)
}

fn replay(config: &ReplayConfig) -> Result<()> {
    let periods = resolved_periods(config)?;
    let jobs = collect_jobs(config, &periods)?;
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
        "cme_tas_replay start workers={} parts={} rocksdb={} periods={:?} capped={} research_roots={} max_tradedays={} unparsed={}",
        config.workers,
        jobs.len(),
        final_dir.display(),
        periods,
        capped,
        RESEARCH_PRODUCT_ROOTS.len(),
        config.max_tradedays,
        config.unparsed_path.display()
    );

    let db = open_rocksdb(&final_dir)?;
    if !capped {
        for period in &periods {
            claim_period(&db, period)?;
        }
    }
    let db = Arc::new(db);
    let abort = Arc::new(AtomicBool::new(false));
    let unparsed = Arc::new(UnparsedSink::open(&config.unparsed_path)?);
    let queue = Arc::new(Mutex::new(jobs.iter().cloned().rev().collect::<Vec<_>>()));
    let worker_n = config.workers.min(jobs.len()).max(1);
    let mut handles = Vec::with_capacity(worker_n);
    for worker_id in 0..worker_n {
        let db = Arc::clone(&db);
        let abort = Arc::clone(&abort);
        let unparsed = Arc::clone(&unparsed);
        let queue = Arc::clone(&queue);
        let config = ReplayConfig {
            data_root: config.data_root.clone(),
            period: config.period.clone(),
            periods: config.periods.clone(),
            rocksdb_dir: config.rocksdb_dir.clone(),
            rics: config.rics.clone(),
            part_index: config.part_index,
            workers: config.workers,
            max_source_rows: config.max_source_rows,
            column_rules: config.column_rules.clone(),
            progress_every: config.progress_every,
            log_path: config.log_path.clone(),
            unparsed_path: config.unparsed_path.clone(),
            max_tradedays: config.max_tradedays,
        };
        let ric_filter = ric_filter.clone();
        handles.push(thread::spawn(move || -> Result<Census> {
            let mut local = Census::default();
            loop {
                let job = {
                    let mut guard = queue.lock().expect("part queue");
                    guard.pop()
                };
                let Some((period, part_no, part)) = job else {
                    break;
                };
                info!(
                    "cme_tas_replay worker={worker_id} claimed period={period} part_no={part_no} path={}",
                    part.display()
                );
                match replay_part(
                    &config,
                    &db,
                    &period,
                    &part,
                    part_no,
                    &ric_filter,
                    &abort,
                    &unparsed,
                ) {
                    Ok(part_census) => local.merge_from(&part_census),
                    Err(err) => {
                        abort.store(true, Ordering::Relaxed);
                        let err = err.context(format!(
                            "worker {worker_id} failed on period {period} part {part_no} {}",
                            part.display()
                        ));
                        error!("cme_tas_replay {err:#}");
                        return Err(err);
                    }
                }
            }
            Ok(local)
        }));
    }

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
                    first_err = Some(anyhow!("worker thread panicked"));
                }
            }
        }
    }
    if let Some(err) = first_err {
        drop(db);
        return Err(err);
    }

    db.flush().context("flush rocksdb")?;
    if !capped {
        for period in &periods {
            finish_period(&db, period)?;
        }
    }
    drop(db);

    let elapsed_ms = started.elapsed().as_millis();
    info!(
        "cme_tas_replay finished workers={} parts={} periods={:?} source_rows={} written_trades={} written_specials={} written_quotes={} written_renames={} written_limits={} skipped_unmapped={} skipped_ric_filter={} skipped_unparsed={} skipped_after_window={} elapsed_ms={}",
        config.workers,
        jobs.len(),
        periods,
        census.source_rows,
        census.written_trades,
        census.written_specials,
        census.written_quotes,
        census.written_renames,
        census.written_limits,
        census.skipped_unmapped,
        census.skipped_ric_filter,
        census.skipped_unparsed,
        census.skipped_after_window,
        elapsed_ms
    );
    for (name, count) in &census.counted {
        info!("cme_tas_replay class {name}={count}");
    }
    println!(
        "cme_tas_replay finished workers={} parts={} periods={:?} source_rows={} written_trades={} written_specials={} written_quotes={} written_renames={} written_limits={} skipped_unmapped={} skipped_ric_filter={} skipped_unparsed={} skipped_after_window={} elapsed_ms={}",
        config.workers,
        jobs.len(),
        periods,
        census.source_rows,
        census.written_trades,
        census.written_specials,
        census.written_quotes,
        census.written_renames,
        census.written_limits,
        census.skipped_unmapped,
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
            "[{} ERROR] cme_tas_replay panic thread={name} at {loc}: {payload}\n",
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
    info!("cme_tas_replay log_path={}", config.log_path.display());
    info!("cme_tas_replay unparsed_path={}", config.unparsed_path.display());
    eprintln!("cme_tas_replay logging to {}", config.log_path.display());
    eprintln!(
        "cme_tas_replay unparsed rows to {}",
        config.unparsed_path.display()
    );
    if let Err(err) = replay(&config) {
        error!("cme_tas_replay failed: {err:?}");
        eprintln!("cme_tas_replay failed: {err:?}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn claim_period_allows_a_second_period_and_refuses_repeat() {
        let dir = TempDir::new().unwrap();
        let db = open_rocksdb(dir.path()).unwrap();
        claim_period(&db, "2026-01-01_2026-06-01").unwrap();
        finish_period(&db, "2026-01-01_2026-06-01").unwrap();
        claim_period(&db, "2025-01-01_2026-01-01").unwrap();
        let err = claim_period(&db, "2026-01-01_2026-06-01").unwrap_err();
        assert!(
            err.to_string().contains("already done"),
            "{err}"
        );
        let err = claim_period(&db, "2025-01-01_2026-01-01").unwrap_err();
        assert!(
            err.to_string().contains("marked writing"),
            "{err}"
        );
    }

    #[test]
    fn leftover_building_dir_is_refused_before_open() {
        let dir = TempDir::new().unwrap();
        let final_dir = dir.path().join("cme_tas_rocksdb");
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
    fn filled_cells_prints_nonempty_source_columns() {
        let headers = StringRecord::from(vec![
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
            "Implied Yield",
            "Change Type",
            "Old Value",
            "New Value",
        ]);
        let mut columns = BTreeMap::new();
        for name in headers.iter() {
            let group = match name {
                "#RIC" | "Date-Time" | "Type" | "Qualifiers" | "Exch Time" | "Change Type"
                | "Old Value" | "New Value" => "identity",
                "Price" | "Volume" | "Bid Price" | "Bid Size" | "Ask Price" | "Ask Size" => {
                    "trade"
                }
                "UpLim Price" | "LoLim Price" => "limit",
                "Imp. Vol." => "iv",
                "Implied Yield" => "yield",
                _ => "other",
            };
            columns.insert(name.to_string(), group.to_string());
        }
        let rules = ColumnRules {
            types: vec![
                "Trade".into(),
                "Quote".into(),
                "Mkt. Condition".into(),
                "Correction".into(),
            ],
            required_identity: vec!["#RIC".into(), "Date-Time".into(), "Type".into()],
            columns,
        };
        let map = HeaderMap::from_headers(&headers, &rules).unwrap();
        let mut cells = vec![""; headers.len()];
        cells[0] = "ESH26";
        cells[1] = "2026-01-07T09:22:58.013583911Z";
        cells[2] = "Trade";
        cells[4] = "20";
        let record = StringRecord::from(cells);
        assert_eq!(
            map.filled_cells(&record),
            "#RIC=ESH26 Date-Time=2026-01-07T09:22:58.013583911Z Type=Trade Volume=20"
        );
        assert_eq!(map.idx.ric, 0);
        assert_eq!(map.idx.volume, 4);
        assert!(map.forbidden_idxs.contains(&map.idx.implied_yield) == false);
        assert!(map.forbidden_idxs.contains(&map.idx.imp_vol) == false);
    }

    #[test]
    fn header_map_indexes_required_columns_and_skips_allowed_forbidden_groups() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../preprocess/lseg/tas_column_rules.json");
        let rules = ColumnRules::load(&path).unwrap();
        let mut names: Vec<&str> = rules.columns.keys().map(String::as_str).collect();
        names.sort();
        // Real TAS header order is the catalog order in the source file, not
        // BTree key order. from_headers only needs every required name present.
        let headers = StringRecord::from(rules.columns.keys().cloned().collect::<Vec<_>>());
        let map = HeaderMap::from_headers(&headers, &rules).unwrap();
        assert_eq!(map.names.len(), 294);
        assert_eq!(cell_at(&headers, map.idx.ric).is_empty(), false);
        assert!(!map.forbidden_idxs.contains(&map.idx.up_lim));
        assert!(!map.forbidden_idxs.contains(&map.idx.lo_lim));
        assert!(!map.forbidden_idxs.contains(&map.idx.imp_vol));
        assert!(!map.forbidden_idxs.contains(&map.idx.implied_yield));
        assert!(!map.forbidden_idxs.is_empty());
    }

    #[test]
    fn resolved_periods_uses_list_when_present() {
        let config = ReplayConfig {
            data_root: PathBuf::from("/tmp"),
            period: "2026-01-01_2026-06-01".into(),
            periods: vec![
                "2024-01-01_2025-01-01".into(),
                "2025-01-01_2026-01-01".into(),
                "2026-01-01_2026-06-01".into(),
            ],
            rocksdb_dir: PathBuf::from("/tmp/db"),
            rics: vec![],
            part_index: 0,
            workers: 32,
            max_source_rows: None,
            column_rules: PathBuf::from("rules.json"),
            progress_every: 1_000_000,
            log_path: PathBuf::from("/tmp/log"),
            unparsed_path: PathBuf::from("/tmp/unparsed"),
            max_tradedays: 0,
        };
        assert_eq!(
            resolved_periods(&config).unwrap(),
            vec![
                "2024-01-01_2025-01-01",
                "2025-01-01_2026-01-01",
                "2026-01-01_2026-06-01",
            ]
        );
    }
}
