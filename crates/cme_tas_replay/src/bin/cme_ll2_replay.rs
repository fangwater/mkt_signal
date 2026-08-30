//! Full-universe LSEG Normalized LL2 replay.
//!
//! Each source row is already a complete L1--L10 snapshot. This replay keeps
//! only the last source snapshot in each `RIC + UTC minute`, writes no raw or
//! staging layer, and partitions RocksDB column families by source year and
//! parsed product (`p:{year}:{product}`).

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::ll2_1min::{
    decode_ll2_minute, decode_ll2_minute_key, encode_ll2_minute, encode_ll2_minute_key,
    encode_source_order, ll2_latest_merge, minute_key_for, Ll2Minute, LL2_MINUTE_KEY_LEN,
    NS_PER_MINUTE,
};
use cme_tas_replay::ll2_shard::{ll2_period_dir_name, Ll2ShardManifest};
use cme_tas_replay::ll2_source::{
    parse_normalized_ll2_line, strip_line_ending, validate_normalized_ll2_header,
};
use cme_tas_replay::product::{is_product_cf_name, parse_product, period_year, product_cf_name};
use cme_tas_replay::{
    decode_period_status, encode_period_status, encode_ric, period_meta_key, validate_period,
    PeriodStatus, CF_REPLAY_META,
};
use crossbeam_channel::unbounded;
use flate2::read::MultiGzDecoder;
use log::{info, warn};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded,
    Options, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use zstd::stream::read::Decoder as ZstdDecoder;

type Ll2Db = DBWithThreadMode<MultiThreaded>;

const DEFAULT_WORKERS: usize = 96;
const INPUT_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const WRITE_BATCH_ROWS: usize = 8_192;
const DEFAULT_PROGRESS_EVERY: u64 = 10_000_000;
const EXPECTED_DB_NAME: &str = "cme_ll2_1min_rocksdb_all_products";

#[derive(Parser, Debug)]
#[command(name = "cme_ll2_replay")]
#[command(about = "Replay every Normalized LL2 RIC as final Depth10 snapshot per UTC minute")]
struct Args {
    #[arg(long, default_value = "config/cme_ll2_replay.toml")]
    config: PathBuf,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    verify: bool,
    #[arg(long)]
    preflight: bool,
    #[arg(long)]
    reset_writing_period: bool,
    #[arg(long)]
    period: Option<String>,
    #[arg(long)]
    part_index: Option<u16>,
    #[arg(long)]
    shard_index: Option<u32>,
    #[arg(long)]
    max_source_rows: Option<u64>,
    #[arg(long)]
    rocksdb_dir: Option<PathBuf>,
    #[arg(long)]
    shard_root: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct Config {
    data_root: PathBuf,
    shard_root: PathBuf,
    period: String,
    #[serde(default)]
    periods: Vec<String>,
    rocksdb_dir: PathBuf,
    #[serde(default = "default_workers")]
    workers: usize,
    #[serde(default = "default_progress_every")]
    progress_every: u64,
}

#[derive(Debug, Clone)]
struct Job {
    period: String,
    year: u16,
    part_no: u16,
    shard_index: u32,
    path: PathBuf,
    expected_header: Arc<str>,
}

#[derive(Default, Debug, Clone)]
struct PartStats {
    source_rows: u64,
    minute_snapshots: u64,
    first_snapshot: Option<SnapshotBoundary>,
    last_snapshot: Option<SnapshotBoundary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotBoundary {
    cf_name: String,
    key: [u8; LL2_MINUTE_KEY_LEN],
}

#[derive(Debug, Clone)]
struct PartResult {
    job: Job,
    stats: PartStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReportCensus {
    report_rows: u64,
    active_rows: u64,
    zero_count_rows: u64,
    expected_source_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PeriodCensus {
    source_rows: u64,
    minute_snapshots: u64,
    report_rows: u64,
    active_rows: u64,
    zero_count_rows: u64,
}

impl PartStats {
    fn add(&mut self, other: &Self) {
        self.source_rows += other.source_rows;
        self.minute_snapshots += other.minute_snapshots;
    }

    fn record_snapshot(&mut self, cf_name: String, key: [u8; LL2_MINUTE_KEY_LEN]) {
        let boundary = SnapshotBoundary { cf_name, key };
        if self.first_snapshot.is_none() {
            self.first_snapshot = Some(boundary.clone());
        }
        self.last_snapshot = Some(boundary);
        self.minute_snapshots += 1;
    }
}

fn default_workers() -> usize {
    DEFAULT_WORKERS
}

fn default_progress_every() -> u64 {
    DEFAULT_PROGRESS_EVERY
}

fn resolved_periods(config: &Config, override_period: Option<&str>) -> Result<Vec<String>> {
    let periods = if let Some(period) = override_period {
        vec![period.to_string()]
    } else if config.periods.is_empty() {
        vec![config.period.clone()]
    } else {
        config.periods.clone()
    };
    let mut unique_periods = BTreeSet::new();
    let mut unique_years = BTreeSet::new();
    for period in &periods {
        validate_period(period)?;
        if !unique_periods.insert(period.clone()) {
            bail!("duplicate LL2 period {period}");
        }
        let year = period_year(period)?;
        if !unique_years.insert(year) {
            bail!("multiple LL2 periods share source year {year}; year+product CF ownership is ambiguous");
        }
    }
    Ok(periods)
}

fn period_dir(config: &Config, period: &str) -> PathBuf {
    config.data_root.join(ll2_period_dir_name(period))
}

fn report_census(config: &Config, period: &str) -> Result<ReportCensus> {
    let path = period_dir(config, period).join("merged-Report.csv.gz");
    let file = File::open(&path).with_context(|| format!("open LL2 report {}", path.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::new(file));
    let mut reader = csv::ReaderBuilder::new()
        .flexible(false)
        .from_reader(decoder);
    let headers = reader
        .headers()
        .with_context(|| format!("read LL2 report header {}", path.display()))?
        .clone();
    if headers.iter().collect::<Vec<_>>() != ["#RIC", "Domain", "Start", "End", "Status", "Count"] {
        bail!("unexpected LL2 report header in {}", path.display());
    }
    let mut census = ReportCensus {
        report_rows: 0,
        active_rows: 0,
        zero_count_rows: 0,
        expected_source_rows: 0,
    };
    for result in reader.records() {
        let record = result.with_context(|| format!("parse LL2 report {}", path.display()))?;
        let count = record
            .get(5)
            .ok_or_else(|| anyhow!("LL2 report row has no Count"))?
            .parse::<u64>()
            .with_context(|| format!("parse LL2 report Count for {:?}", record.get(0)))?;
        census.report_rows += 1;
        census.zero_count_rows += u64::from(count == 0);
        if count > 0 {
            let ric = record
                .get(0)
                .ok_or_else(|| anyhow!("LL2 report row has no #RIC"))?;
            if record.get(1) != Some("Market Price") {
                bail!("active LL2 report RIC {ric:?} is not Domain=Market Price");
            }
            encode_ric(ric).with_context(|| format!("active LL2 report RIC {ric:?}"))?;
            let product = parse_product(ric).ok_or_else(|| {
                anyhow!("cannot derive product from active LL2 report RIC {ric:?}")
            })?;
            product_cf_name(period_year(period)?, &product)?;
            census.active_rows += 1;
        }
        census.expected_source_rows = census
            .expected_source_rows
            .checked_add(count)
            .ok_or_else(|| anyhow!("LL2 report Count sum overflow"))?;
    }
    Ok(census)
}

fn shard_period_dir(config: &Config, period: &str) -> PathBuf {
    config.shard_root.join(ll2_period_dir_name(period))
}

fn load_shard_manifest(
    config: &Config,
    period: &str,
    require_complete: bool,
) -> Result<Ll2ShardManifest> {
    let dir = shard_period_dir(config, period);
    let manifest = Ll2ShardManifest::load(&dir)?;
    manifest.validate(period, require_complete)?;
    validate_normalized_ll2_header(manifest.header.as_bytes())?;
    Ok(manifest)
}

fn collect_shard_jobs(
    config: &Config,
    period: &str,
    part_index: Option<u16>,
    shard_index: Option<u32>,
    require_complete: bool,
) -> Result<Vec<Job>> {
    let dir = shard_period_dir(config, period);
    let manifest = load_shard_manifest(config, period, require_complete)?;
    let expected_header = Arc::<str>::from(manifest.header.as_str());
    let year = period_year(period)?;
    let mut jobs = Vec::new();
    for shard in &manifest.shards {
        if part_index.is_some_and(|wanted| wanted != shard.original_part)
            || shard_index.is_some_and(|wanted| wanted != shard.shard_index)
        {
            continue;
        }
        let path = dir.join(&shard.file);
        let actual_bytes = fs::metadata(&path)
            .with_context(|| format!("stat LL2 shard {}", path.display()))?
            .len();
        if actual_bytes != shard.compressed_bytes {
            bail!(
                "LL2 shard {} is {actual_bytes} bytes, manifest expects {}",
                path.display(),
                shard.compressed_bytes
            );
        }
        jobs.push(Job {
            period: period.to_string(),
            year,
            part_no: shard.original_part,
            shard_index: shard.shard_index,
            path,
            expected_header: Arc::clone(&expected_header),
        });
    }
    if jobs.is_empty() {
        bail!("no LL2 shards match period={period} part={part_index:?} shard={shard_index:?}");
    }
    Ok(jobs)
}

fn product_cf_options() -> Options {
    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_write_buffer_size(64 * 1024 * 1024);
    options.set_max_write_buffer_number(4);
    options.set_min_write_buffer_number_to_merge(1);
    options.set_level_zero_file_num_compaction_trigger(8);
    options.set_level_zero_slowdown_writes_trigger(32);
    options.set_level_zero_stop_writes_trigger(48);
    options.set_merge_operator_associative("ll2_latest_snapshot", ll2_latest_merge);
    options
}

fn open_db(path: &Path) -> Result<Ll2Db> {
    if path.exists() && !path.is_dir() {
        bail!(
            "rocksdb_dir {} exists and is not a directory",
            path.display()
        );
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);
    db_options.set_max_open_files(65_536);
    db_options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    db_options.increase_parallelism(32);
    db_options.set_max_background_jobs(32);
    db_options.set_db_write_buffer_size(8 * 1024 * 1024 * 1024);
    db_options.set_max_subcompactions(4);
    db_options.set_merge_operator_associative("ll2_latest_snapshot", ll2_latest_merge);
    let names = if path.is_dir() && path.read_dir()?.next().is_some() {
        Ll2Db::list_cf(&Options::default(), path)
            .with_context(|| format!("list column families {}", path.display()))?
    } else {
        vec!["default".to_string(), CF_REPLAY_META.to_string()]
    };
    for name in &names {
        if name != "default" && name != CF_REPLAY_META && !is_product_cf_name(name) {
            bail!(
                "unsupported LL2 column family {name:?} in {}",
                path.display()
            );
        }
    }
    let descriptors = names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, product_cf_options()))
        .collect::<Vec<_>>();
    Ll2Db::open_cf_descriptors(&db_options, path, descriptors)
        .with_context(|| format!("open LL2 RocksDB {}", path.display()))
}

fn meta_cf<'a>(db: &'a Ll2Db) -> Result<Arc<BoundColumnFamily<'a>>> {
    db.cf_handle(CF_REPLAY_META)
        .ok_or_else(|| anyhow!("column family {CF_REPLAY_META} missing"))
}

fn period_status(db: &Ll2Db, period: &str) -> Result<Option<PeriodStatus>> {
    let cf = meta_cf(db)?;
    match db.get_cf(&cf, period_meta_key(period)?)? {
        Some(value) => Ok(Some(decode_period_status(&value)?)),
        None => Ok(None),
    }
}

fn census_key(period: &str) -> Result<Vec<u8>> {
    validate_period(period)?;
    Ok(format!("census:{period}").into_bytes())
}

fn finish_period(db: &Ll2Db, period: &str, census: &PeriodCensus) -> Result<()> {
    if period_status(db, period)? != Some(PeriodStatus::Writing) {
        bail!("LL2 period {period} is not writing before finish");
    }
    let cf = meta_cf(db)?;
    let mut batch = WriteBatch::default();
    batch.put_cf(
        &cf,
        census_key(period)?,
        serde_json::to_vec(census).context("serialize LL2 period census")?,
    );
    batch.put_cf(
        &cf,
        period_meta_key(period)?,
        encode_period_status(PeriodStatus::Done),
    );
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options)
        .with_context(|| format!("finish LL2 period {period}"))
}

fn read_period_census(db: &Ll2Db, period: &str) -> Result<PeriodCensus> {
    let cf = meta_cf(db)?;
    let bytes = db
        .get_cf(&cf, census_key(period)?)?
        .ok_or_else(|| anyhow!("LL2 period {period} has no census"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("decode LL2 census {period}"))
}

fn claim_periods(db: &Ll2Db, periods: &[String]) -> Result<()> {
    for period in periods {
        match period_status(db, period)? {
            None => {}
            Some(PeriodStatus::Done) => {
                bail!("LL2 period {period} is already done; refuse overwrite")
            }
            Some(PeriodStatus::Writing) => bail!(
                "LL2 period {period} is marked writing; use --reset-writing-period before retry"
            ),
        }
    }
    let cf = meta_cf(db)?;
    let mut batch = WriteBatch::default();
    for period in periods {
        batch.put_cf(
            &cf,
            period_meta_key(period)?,
            encode_period_status(PeriodStatus::Writing),
        );
    }
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options).context("claim LL2 periods")
}

fn ensure_cf(db: &Ll2Db, lock: &Mutex<()>, name: &str) -> Result<()> {
    if db.cf_handle(name).is_some() {
        return Ok(());
    }
    let _guard = lock.lock().expect("LL2 column family create lock");
    if db.cf_handle(name).is_none() {
        db.create_cf(name, &product_cf_options())
            .with_context(|| format!("create LL2 column family {name}"))?;
    }
    Ok(())
}

fn product_cf<'a>(
    db: &'a Ll2Db,
    lock: &Mutex<()>,
    name: &str,
) -> Result<Arc<BoundColumnFamily<'a>>> {
    ensure_cf(db, lock, name)?;
    db.cf_handle(name)
        .ok_or_else(|| anyhow!("LL2 column family {name} missing after create"))
}

fn flush_batch(db: &Ll2Db, batch: &mut WriteBatch) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut options = WriteOptions::default();
    options.set_sync(false);
    db.write_opt(std::mem::take(batch), &options)
        .context("write LL2 minute snapshot batch")
}

fn write_snapshot(
    db: &Ll2Db,
    cf_lock: &Mutex<()>,
    batch: &mut WriteBatch,
    cf_name: &str,
    key: &[u8; LL2_MINUTE_KEY_LEN],
    value: &Ll2Minute,
    boundary: bool,
) -> Result<()> {
    let cf = product_cf(db, cf_lock, cf_name)?;
    if boundary {
        batch.merge_cf(&cf, key, encode_ll2_minute(value));
    } else {
        batch.put_cf(&cf, key, encode_ll2_minute(value));
    }
    if batch.len() >= WRITE_BATCH_ROWS {
        flush_batch(db, batch)?;
    }
    Ok(())
}

fn replay_part(
    db: Option<&Ll2Db>,
    cf_lock: &Mutex<()>,
    job: &Job,
    abort: &AtomicBool,
    max_source_rows: Option<u64>,
    progress_every: u64,
) -> Result<PartStats> {
    let file = File::open(&job.path).with_context(|| format!("open {}", job.path.display()))?;
    let decoder = ZstdDecoder::with_buffer(BufReader::with_capacity(INPUT_BUFFER_BYTES, file))
        .with_context(|| format!("open LL2 zstd shard {}", job.path.display()))?;
    let mut reader = BufReader::with_capacity(INPUT_BUFFER_BYTES, decoder);
    let mut line = Vec::with_capacity(1024);
    reader
        .read_until(b'\n', &mut line)
        .context("read LL2 shard header")?;
    if line.is_empty() {
        bail!("LL2 shard is empty: {}", job.path.display());
    }
    validate_normalized_ll2_header(&line)
        .with_context(|| format!("validate LL2 header {}", job.path.display()))?;
    if std::str::from_utf8(strip_line_ending(&line))? != job.expected_header.as_ref() {
        bail!(
            "LL2 shard header does not match manifest: {}",
            job.path.display()
        );
    }

    let mut stats = PartStats::default();
    let mut batch = WriteBatch::default();
    let mut current: Option<(String, [u8; LL2_MINUTE_KEY_LEN], Ll2Minute)> = None;
    let mut last_order: Option<(String, u64)> = None;
    loop {
        if max_source_rows.is_some_and(|limit| stats.source_rows >= limit) {
            break;
        }
        line.clear();
        if reader
            .read_until(b'\n', &mut line)
            .with_context(|| format!("read {}", job.path.display()))?
            == 0
        {
            break;
        }
        if strip_line_ending(&line).is_empty() {
            bail!("empty LL2 source row in {}", job.path.display());
        }
        stats.source_rows += 1;
        if stats.source_rows % 100_000 == 0 && abort.load(Ordering::Relaxed) {
            bail!("aborted while reading {}", job.path.display());
        }
        let source = parse_normalized_ll2_line(&line).with_context(|| {
            format!(
                "parse LL2 period={} part={} shard={} source_row={} prefix={:?}",
                job.period,
                job.part_no,
                job.shard_index,
                stats.source_rows,
                String::from_utf8_lossy(
                    &strip_line_ending(&line)[..strip_line_ending(&line).len().min(160)]
                )
            )
        })?;
        let order = (source.ric.clone(), source.source_ts_utc_ns);
        if last_order
            .as_ref()
            .is_some_and(|previous| order < *previous)
        {
            bail!(
                "LL2 source order regressed in {} at {}",
                job.path.display(),
                source.ric
            );
        }
        last_order = Some(order);
        let product = parse_product(&source.ric)
            .ok_or_else(|| anyhow!("cannot derive product from LL2 RIC {:?}", source.ric))?;
        let cf_name = product_cf_name(job.year, &product)?;
        let key = encode_ll2_minute_key(&minute_key_for(&source))?;
        let value = Ll2Minute::from_source(
            source,
            encode_source_order(job.part_no, job.shard_index, stats.source_rows)?,
        );
        match &mut current {
            Some((active_cf, active_key, active_value))
                if active_cf == &cf_name && active_key == &key =>
            {
                if value.ordering_tuple() > active_value.ordering_tuple() {
                    *active_value = value;
                }
            }
            Some(_) => {
                let (finished_cf, finished_key, finished_value) = current.take().unwrap();
                if let Some(database) = db {
                    let boundary = stats.minute_snapshots == 0;
                    write_snapshot(
                        database,
                        cf_lock,
                        &mut batch,
                        &finished_cf,
                        &finished_key,
                        &finished_value,
                        boundary,
                    )?;
                }
                stats.record_snapshot(finished_cf, finished_key);
                current = Some((cf_name, key, value));
            }
            None => current = Some((cf_name, key, value)),
        }
        if progress_every > 0 && stats.source_rows % progress_every == 0 {
            info!(
                "cme_ll2_replay progress period={} part={} shard={} source_rows={} minute_snapshots={}",
                job.period,
                job.part_no,
                job.shard_index,
                stats.source_rows,
                stats.minute_snapshots
            );
        }
    }
    if let Some((cf_name, key, value)) = current {
        if let Some(database) = db {
            write_snapshot(database, cf_lock, &mut batch, &cf_name, &key, &value, true)?;
        }
        stats.record_snapshot(cf_name, key);
    }
    if let Some(database) = db {
        flush_batch(database, &mut batch)?;
    }
    Ok(stats)
}

fn product_cfs_for_year(db: &Ll2Db, path: &Path, year: u16) -> Result<Vec<String>> {
    let prefix = format!("p:{year}:");
    let mut names = Ll2Db::list_cf(&Options::default(), path)
        .with_context(|| format!("list column families {}", path.display()))?
        .into_iter()
        .filter(|name| name.starts_with(&prefix))
        .collect::<Vec<_>>();
    names.sort();
    for name in &names {
        if db.cf_handle(name).is_none() {
            bail!("LL2 column family {name} disappeared");
        }
    }
    Ok(names)
}

fn compact_year(db: &Ll2Db, path: &Path, year: u16) -> Result<usize> {
    let names = product_cfs_for_year(db, path, year)?;
    for name in &names {
        let cf = db
            .cf_handle(name)
            .ok_or_else(|| anyhow!("missing {name}"))?;
        db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
    }
    Ok(names.len())
}

fn aggregate_period_results(period: &str, results: &[PartResult]) -> Result<PartStats> {
    let mut selected = results
        .iter()
        .filter(|result| result.job.period == period)
        .collect::<Vec<_>>();
    selected.sort_by_key(|result| (result.job.part_no, result.job.shard_index));
    if selected.is_empty() {
        bail!("LL2 period {period} produced no part results");
    }
    let mut total = PartStats::default();
    for result in &selected {
        total.add(&result.stats);
    }
    for adjacent in selected.windows(2) {
        if adjacent[0].stats.last_snapshot.is_some()
            && adjacent[0].stats.last_snapshot == adjacent[1].stats.first_snapshot
        {
            total.minute_snapshots = total
                .minute_snapshots
                .checked_sub(1)
                .ok_or_else(|| anyhow!("LL2 final snapshot count underflow"))?;
        }
    }
    total.first_snapshot = selected
        .iter()
        .find_map(|result| result.stats.first_snapshot.clone());
    total.last_snapshot = selected
        .iter()
        .rev()
        .find_map(|result| result.stats.last_snapshot.clone());
    Ok(total)
}

fn interleave_year_jobs(jobs_by_period: Vec<Vec<Job>>) -> Vec<Job> {
    let max_jobs = jobs_by_period.iter().map(Vec::len).max().unwrap_or(0);
    let total_jobs = jobs_by_period.iter().map(Vec::len).sum();
    let mut output = Vec::with_capacity(total_jobs);
    for index in 0..max_jobs {
        for jobs in &jobs_by_period {
            if let Some(job) = jobs.get(index) {
                output.push(job.clone());
            }
        }
    }
    output
}

fn run_periods(
    config: &Config,
    args: &Args,
    db: Option<&Arc<Ll2Db>>,
    cf_lock: &Arc<Mutex<()>>,
    periods: &[String],
) -> Result<PartStats> {
    let mut reports = BTreeMap::new();
    let mut jobs_by_period = Vec::with_capacity(periods.len());
    for period in periods {
        reports.insert(period.clone(), report_census(config, period)?);
        jobs_by_period.push(collect_shard_jobs(
            config,
            period,
            args.part_index,
            args.shard_index,
            !args.dry_run,
        )?);
    }
    let jobs = interleave_year_jobs(jobs_by_period);
    if args.max_source_rows.is_some() && jobs.len() != 1 {
        bail!("--max-source-rows requires exactly one selected LL2 shard");
    }
    if let Some(database) = db {
        claim_periods(database, periods)?;
    }
    let started = Instant::now();
    let abort = Arc::new(AtomicBool::new(false));
    let first_error = Arc::new(Mutex::new(None::<String>));
    let (job_tx, job_rx) = unbounded::<Job>();
    for job in jobs.iter().cloned() {
        job_tx.send(job).expect("LL2 job receiver exists");
    }
    drop(job_tx);
    let worker_count = config.workers.min(jobs.len()).max(1);
    let mut joins = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let db = db.cloned();
        let cf_lock = Arc::clone(cf_lock);
        let abort = Arc::clone(&abort);
        let first_error = Arc::clone(&first_error);
        let job_rx = job_rx.clone();
        let max_source_rows = args.max_source_rows;
        let progress_every = config.progress_every;
        joins.push(
            thread::Builder::new()
                .name(format!("cme-ll2-1min-{worker_id}"))
                .spawn(move || -> Vec<PartResult> {
                    let mut results = Vec::new();
                    while let Ok(job) = job_rx.recv() {
                        if abort.load(Ordering::Relaxed) {
                            break;
                        }
                        match replay_part(
                            db.as_deref(),
                            &cf_lock,
                            &job,
                            &abort,
                            max_source_rows,
                            progress_every,
                        ) {
                            Ok(stats) => {
                                info!(
                                    "cme_ll2_replay shard done period={} part={} shard={} source_rows={} minute_snapshots={}",
                                    job.period,
                                    job.part_no,
                                    job.shard_index,
                                    stats.source_rows,
                                    stats.minute_snapshots
                                );
                                results.push(PartResult { job, stats });
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
                    results
                })
                .context("spawn LL2 replay worker")?,
        );
    }
    drop(job_rx);
    let mut results = Vec::with_capacity(jobs.len());
    for join in joins {
        results.extend(
            join.join()
                .map_err(|_| anyhow!("LL2 replay worker panicked"))?,
        );
    }
    if let Some(error) = first_error.lock().expect("LL2 first-error lock").take() {
        bail!("LL2 worker failed: {error}");
    }
    if results.len() != jobs.len() {
        bail!(
            "LL2 replay completed {} part results for {} jobs",
            results.len(),
            jobs.len()
        );
    }
    let mut totals = BTreeMap::new();
    let mut grand_total = PartStats::default();
    for period in periods {
        let total = aggregate_period_results(period, &results)?;
        grand_total.add(&total);
        totals.insert(period.clone(), total);
    }
    if let Some(database) = db {
        database.flush().context("flush LL2 RocksDB")?;
    }
    for period in periods {
        let report = reports.get(period).expect("report exists for period");
        let total = totals.get(period).expect("total exists for period");
        if db.is_some() && total.source_rows != report.expected_source_rows {
            bail!(
                "LL2 period {period} read {} source rows but merged-Report expects {}",
                total.source_rows,
                report.expected_source_rows
            );
        }
        if let Some(database) = db {
            let cf_count = compact_year(
                database,
                args.rocksdb_dir.as_deref().unwrap_or(&config.rocksdb_dir),
                period_year(period)?,
            )?;
            finish_period(
                database,
                period,
                &PeriodCensus {
                    source_rows: total.source_rows,
                    minute_snapshots: total.minute_snapshots,
                    report_rows: report.report_rows,
                    active_rows: report.active_rows,
                    zero_count_rows: report.zero_count_rows,
                },
            )?;
            info!(
                "cme_ll2_replay period complete period={} source_rows={} minute_snapshots={} report_rows={} active_rows={} zero_count_rows={} product_cfs={} elapsed_s={:.1}",
                period,
                total.source_rows,
                total.minute_snapshots,
                report.report_rows,
                report.active_rows,
                report.zero_count_rows,
                cf_count,
                started.elapsed().as_secs_f64()
            );
        } else {
            info!(
                "cme_ll2_replay dry run period={} source_rows={} minute_snapshots={} report_expected_rows={} report_zero_count_rows={} elapsed_s={:.1}",
                period,
                total.source_rows,
                total.minute_snapshots,
                report.expected_source_rows,
                report.zero_count_rows,
                started.elapsed().as_secs_f64()
            );
        }
    }
    Ok(grand_total)
}

fn reset_writing_period(config: &Config, args: &Args, periods: &[String]) -> Result<()> {
    let path = args.rocksdb_dir.as_deref().unwrap_or(&config.rocksdb_dir);
    if path.file_name().and_then(|name| name.to_str()) != Some(EXPECTED_DB_NAME) {
        bail!(
            "refusing reset for unexpected RocksDB path {}",
            path.display()
        );
    }
    let db = open_db(path)?;
    for period in periods {
        if period_status(&db, period)? != Some(PeriodStatus::Writing) {
            bail!("LL2 period {period} is not marked writing");
        }
    }
    let mut dropped = 0usize;
    for period in periods {
        let names = product_cfs_for_year(&db, path, period_year(period)?)?;
        for name in names {
            db.drop_cf(&name)
                .with_context(|| format!("drop incomplete LL2 column family {name}"))?;
            dropped += 1;
        }
    }
    let meta = meta_cf(&db)?;
    let mut batch = WriteBatch::default();
    for period in periods {
        batch.delete_cf(&meta, period_meta_key(period)?);
        batch.delete_cf(&meta, census_key(period)?);
    }
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options)
        .context("clear LL2 writing watermarks")?;
    println!(
        "reset LL2 periods={}; dropped {dropped} product column families",
        periods.len()
    );
    Ok(())
}

fn verify(config: &Config, args: &Args, periods: &[String]) -> Result<()> {
    let path = args.rocksdb_dir.as_deref().unwrap_or(&config.rocksdb_dir);
    let db = open_db(path)?;
    let mut expected_by_year = BTreeMap::new();
    for period in periods {
        if period_status(&db, period)? != Some(PeriodStatus::Done) {
            bail!("LL2 period {period} is not done");
        }
        let census = read_period_census(&db, period)?;
        let report = report_census(config, period)?;
        if census.source_rows != report.expected_source_rows
            || census.report_rows != report.report_rows
            || census.active_rows != report.active_rows
            || census.zero_count_rows != report.zero_count_rows
        {
            bail!("LL2 period {period} census no longer matches merged-Report");
        }
        expected_by_year.insert(period_year(period)?, census.minute_snapshots);
    }
    let names = Ll2Db::list_cf(&Options::default(), path)?
        .into_iter()
        .filter(|name| name != "default" && name != CF_REPLAY_META)
        .collect::<Vec<_>>();
    let verified_rows = AtomicU64::new(0);
    let pool = ThreadPoolBuilder::new()
        .num_threads(32.min(names.len()).max(1))
        .thread_name(|index| format!("cme-ll2-verify-{index}"))
        .build()
        .context("build LL2 verify pool")?;
    let per_cf = pool.install(|| {
        names
            .par_iter()
            .map(|name| -> Result<(u16, u64)> {
                if !is_product_cf_name(name) {
                    bail!("unsupported LL2 column family {name:?}");
                }
                let year = name
                    .strip_prefix("p:")
                    .and_then(|value| value.get(..4))
                    .ok_or_else(|| anyhow!("invalid LL2 column family {name:?}"))?
                    .parse::<u16>()?;
                let cf = db
                    .cf_handle(name)
                    .ok_or_else(|| anyhow!("missing {name}"))?;
                let mut previous: Option<Vec<u8>> = None;
                let mut local_rows = 0u64;
                for item in db.iterator_cf(&cf, IteratorMode::Start) {
                    let (key, value) =
                        item.with_context(|| format!("iterate LL2 column family {name}"))?;
                    if previous
                        .as_ref()
                        .is_some_and(|last| key.as_ref() <= last.as_slice())
                    {
                        bail!("LL2 keys are not strictly ordered in {name}");
                    }
                    let decoded_key = decode_ll2_minute_key(&key)?;
                    let decoded_value = decode_ll2_minute(&value)?;
                    if decoded_value.source_ts_utc_ns / NS_PER_MINUTE * NS_PER_MINUTE
                        != decoded_key.minute_utc_ns
                    {
                        bail!(
                            "LL2 source timestamp is outside minute key for {}",
                            decoded_key.ric
                        );
                    }
                    let product = parse_product(&decoded_key.ric).ok_or_else(|| {
                        anyhow!("cannot derive product from key RIC {}", decoded_key.ric)
                    })?;
                    if product_cf_name(year, &product)? != *name {
                        bail!(
                            "LL2 key RIC {} is stored in wrong CF {name}",
                            decoded_key.ric
                        );
                    }
                    previous = Some(key.to_vec());
                    local_rows += 1;
                }
                let total = verified_rows.fetch_add(local_rows, Ordering::Relaxed) + local_rows;
                if total / 100_000_000 != total.saturating_sub(local_rows) / 100_000_000 {
                    info!("cme_ll2_replay verify progress rows={total}");
                }
                Ok((year, local_rows))
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let rows = verified_rows.load(Ordering::Relaxed);
    let mut rows_by_year = BTreeMap::<u16, u64>::new();
    for (year, count) in per_cf {
        *rows_by_year.entry(year).or_default() += count;
    }
    for (year, expected) in expected_by_year {
        let actual = rows_by_year.get(&year).copied().unwrap_or(0);
        if actual != expected {
            bail!("LL2 year {year} has {actual} final minutes, census expects {expected}");
        }
    }
    println!(
        "verified LL2 1min RocksDB product_cfs={} rows={rows}",
        names.len()
    );
    Ok(())
}

fn preflight(config: &Config, periods: &[String]) -> Result<()> {
    let mut expected_source_rows = 0u64;
    let mut report_rows = 0u64;
    let mut active_rows = 0u64;
    let mut zero_count_rows = 0u64;
    for period in periods {
        let census = report_census(config, period)?;
        let manifest = load_shard_manifest(config, period, true)?;
        let manifest_rows = manifest
            .sources
            .iter()
            .try_fold(0u64, |total, source| total.checked_add(source.data_rows))
            .ok_or_else(|| anyhow!("LL2 shard manifest row sum overflow"))?;
        if manifest_rows != census.expected_source_rows {
            bail!(
                "LL2 shard manifest for {period} has {manifest_rows} rows, Report expects {}",
                census.expected_source_rows
            );
        }
        let jobs = collect_shard_jobs(config, period, None, None, true)?;
        println!(
            "LL2 preflight period={period} sources={} shards={} expected_source_rows={} report_rows={} active_rows={} zero_count_rows={}",
            manifest.sources.len(),
            jobs.len(),
            census.expected_source_rows,
            census.report_rows,
            census.active_rows,
            census.zero_count_rows
        );
        expected_source_rows = expected_source_rows
            .checked_add(census.expected_source_rows)
            .ok_or_else(|| anyhow!("LL2 preflight source-row sum overflow"))?;
        report_rows += census.report_rows;
        active_rows += census.active_rows;
        zero_count_rows += census.zero_count_rows;
    }
    println!(
        "LL2 preflight complete periods={} expected_source_rows={} report_rows={} active_rows={} zero_count_rows={}",
        periods.len(), expected_source_rows, report_rows, active_rows, zero_count_rows
    );
    Ok(())
}

fn run(config: &Config, args: &Args, periods: &[String]) -> Result<()> {
    if config.workers == 0 {
        bail!("workers must be >= 1");
    }
    if args.dry_run && args.max_source_rows.is_none() {
        bail!("--dry-run requires --max-source-rows to keep diagnostics bounded");
    }
    if !args.dry_run
        && (args.max_source_rows.is_some()
            || args.part_index.is_some()
            || args.shard_index.is_some())
    {
        bail!("--max-source-rows, --part-index, and --shard-index require --dry-run");
    }
    let path = args.rocksdb_dir.as_deref().unwrap_or(&config.rocksdb_dir);
    let db = (!args.dry_run)
        .then(|| open_db(path).map(Arc::new))
        .transpose()?;
    let cf_lock = Arc::new(Mutex::new(()));
    let total = run_periods(config, args, db.as_ref(), &cf_lock, periods)?;
    info!(
        "cme_ll2_replay complete periods={} source_rows={} minute_snapshots={}",
        periods.len(),
        total.source_rows,
        total.minute_snapshots
    );
    Ok(())
}

fn main() {
    env_logger::init();
    let args = Args::parse();
    let result = (|| -> Result<()> {
        let text = fs::read_to_string(&args.config)
            .with_context(|| format!("read {}", args.config.display()))?;
        let mut config: Config =
            toml::from_str(&text).with_context(|| format!("parse {}", args.config.display()))?;
        if let Some(path) = &args.rocksdb_dir {
            config.rocksdb_dir = path.clone();
        }
        if let Some(path) = &args.shard_root {
            config.shard_root = path.clone();
        }
        let periods = resolved_periods(&config, args.period.as_deref())?;
        let mode_count = usize::from(args.verify)
            + usize::from(args.preflight)
            + usize::from(args.reset_writing_period);
        if mode_count > 1 {
            bail!("--verify, --preflight, and --reset-writing-period are mutually exclusive");
        }
        if args.verify {
            verify(&config, &args, &periods)
        } else if args.preflight {
            preflight(&config, &periods)
        } else if args.reset_writing_period {
            reset_writing_period(&config, &args, &periods)
        } else {
            run(&config, &args, &periods)
        }
    })();
    if let Err(error) = result {
        warn!("cme_ll2_replay failed: {error:#}");
        eprintln!("cme_ll2_replay failed: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::ll2_1min::{decode_ll2_minute, Ll2MinuteKey};
    use cme_tas_replay::ll2_shard::{
        ll2_shard_file_name, Ll2ShardEntry, Ll2ShardGroup, Ll2ShardSource,
        LL2_SHARD_MANIFEST_VERSION,
    };
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    use tempfile::tempdir;

    fn source_line(ts: &str, bid: &str) -> Vec<u8> {
        let mut fields = vec![
            "ADF26".to_string(),
            "Market Price".to_string(),
            ts.to_string(),
            "-6".to_string(),
            "Normalized LL2".to_string(),
        ];
        for level in 0..10 {
            fields.extend([
                if level == 0 {
                    bid.to_string()
                } else {
                    String::new()
                },
                if level == 0 {
                    "1".to_string()
                } else {
                    String::new()
                },
                if level == 0 {
                    "1".to_string()
                } else {
                    String::new()
                },
                if level == 0 {
                    "0.6672".to_string()
                } else {
                    String::new()
                },
                if level == 0 {
                    "2".to_string()
                } else {
                    String::new()
                },
                if level == 0 {
                    "1".to_string()
                } else {
                    String::new()
                },
            ]);
        }
        fields.push("23:00:00.000000000".into());
        fields.join(",").into_bytes()
    }

    fn source_header() -> String {
        let mut fields = vec![
            "#RIC".to_string(),
            "Domain".to_string(),
            "Date-Time".to_string(),
            "GMT Offset".to_string(),
            "Type".to_string(),
        ];
        for level in 1..=10 {
            fields.extend([
                format!("L{level}-BidPrice"),
                format!("L{level}-BidSize"),
                format!("L{level}-BuyNo"),
                format!("L{level}-AskPrice"),
                format!("L{level}-AskSize"),
                format!("L{level}-SellNo"),
            ]);
        }
        fields.push("Exch Time".into());
        fields.join(",")
    }

    fn write_gzip(path: &Path, lines: &[Vec<u8>]) {
        let file = File::create(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::fast());
        for line in lines {
            encoder.write_all(line).unwrap();
            encoder.write_all(b"\n").unwrap();
        }
        encoder.finish().unwrap();
    }

    fn write_zstd(path: &Path, header: &str, line: &[u8]) -> u64 {
        let file = File::create(path).unwrap();
        let mut encoder = zstd::stream::write::Encoder::new(file, 1).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.write_all(header.as_bytes()).unwrap();
        encoder.write_all(b"\n").unwrap();
        encoder.write_all(line).unwrap();
        encoder.write_all(b"\n").unwrap();
        encoder.finish().unwrap();
        fs::metadata(path).unwrap().len()
    }

    #[test]
    fn same_minute_keeps_last_snapshot_without_staging_cf() {
        let root = tempdir().unwrap();
        let db_path = root.path().join(EXPECTED_DB_NAME);
        let db = open_db(&db_path).unwrap();
        let lock = Mutex::new(());
        let mut batch = WriteBatch::default();
        let first =
            parse_normalized_ll2_line(&source_line("2026-01-01T23:00:00.019074617Z", "0.6671"))
                .unwrap();
        let second =
            parse_normalized_ll2_line(&source_line("2026-01-01T23:00:59.919074617Z", "0.66715"))
                .unwrap();
        let key = encode_ll2_minute_key(&minute_key_for(&first)).unwrap();
        let cf_name = product_cf_name(2026, "AD").unwrap();
        write_snapshot(
            &db,
            &lock,
            &mut batch,
            &cf_name,
            &key,
            &Ll2Minute::from_source(first, encode_source_order(0, 0, 1).unwrap()),
            true,
        )
        .unwrap();
        write_snapshot(
            &db,
            &lock,
            &mut batch,
            &cf_name,
            &key,
            &Ll2Minute::from_source(second, encode_source_order(0, 0, 2).unwrap()),
            true,
        )
        .unwrap();
        flush_batch(&db, &mut batch).unwrap();
        let cf = db.cf_handle(&cf_name).unwrap();
        let value = db.get_cf(&cf, key).unwrap().unwrap();
        assert_eq!(
            decode_ll2_minute(&value).unwrap().bid_prices[0],
            667_150_000
        );
        assert_eq!(
            decode_ll2_minute_key(
                &encode_ll2_minute_key(&Ll2MinuteKey {
                    ric: "ADF26".into(),
                    minute_utc_ns: 1_767_308_400_000_000_000,
                })
                .unwrap()
            )
            .unwrap()
            .ric,
            "ADF26"
        );
        assert!(Ll2Db::list_cf(&Options::default(), &db_path)
            .unwrap()
            .iter()
            .all(|name| !name.contains("stage")));
    }

    #[test]
    fn fixture_replay_finishes_census_and_verifies() {
        let root = tempdir().unwrap();
        let period = "2026-01-01_2026-06-01";
        let data_root = root.path().join("normalisedLL2");
        let period_root = data_root.join(format!(
            "shanghai_evolution_futures_market_depth_ric_list_0_ll2_{period}"
        ));
        fs::create_dir_all(&period_root).unwrap();
        write_gzip(
            &period_root.join("merged-Report.csv.gz"),
            &[
                b"#RIC,Domain,Start,End,Status,Count".to_vec(),
                b"ADF26,Market Price,2026-01-01T23:00:00.019074617Z,2026-01-01T23:00:59.919074617Z,Active,2".to_vec(),
                b"ADF27,Market Price,,,Inactive,0".to_vec(),
            ],
        );
        let shard_root = root.path().join("ll2_shards");
        let shard_period_root = shard_root.join(ll2_period_dir_name(period));
        fs::create_dir_all(&shard_period_root).unwrap();
        let first_line = source_line("2026-01-01T23:00:00.019074617Z", "0.6671");
        let second_line = source_line("2026-01-01T23:00:59.919074617Z", "0.66715");
        let first_file = ll2_shard_file_name(0, 0);
        let second_file = ll2_shard_file_name(1, 0);
        let first_bytes = write_zstd(
            &shard_period_root.join(&first_file),
            &source_header(),
            &first_line,
        );
        let second_bytes = write_zstd(
            &shard_period_root.join(&second_file),
            &source_header(),
            &second_line,
        );
        let first_boundary = Ll2ShardGroup {
            ric: "ADF26".into(),
            second_utc_ns: 1_767_308_400_000_000_000,
        };
        let second_boundary = Ll2ShardGroup {
            ric: "ADF26".into(),
            second_utc_ns: 1_767_308_459_000_000_000,
        };
        let manifest = Ll2ShardManifest {
            format_version: LL2_SHARD_MANIFEST_VERSION,
            period: period.into(),
            complete: true,
            header: source_header(),
            rows_per_shard: 1,
            zstd_level: 1,
            sources: vec![
                Ll2ShardSource {
                    file: "merged-Data-part-000000.csv.gz".into(),
                    original_part: 0,
                    compressed_bytes: 1,
                    data_rows: 1,
                    data_bytes: first_line.len() as u64 + 1,
                    shard_count: 1,
                    complete: true,
                },
                Ll2ShardSource {
                    file: "merged-Data-part-000001.csv.gz".into(),
                    original_part: 1,
                    compressed_bytes: 1,
                    data_rows: 1,
                    data_bytes: second_line.len() as u64 + 1,
                    shard_count: 1,
                    complete: true,
                },
            ],
            shards: vec![
                Ll2ShardEntry {
                    file: first_file,
                    original_part: 0,
                    shard_index: 0,
                    rows: 1,
                    data_bytes: first_line.len() as u64 + 1,
                    compressed_bytes: first_bytes,
                    first_group: first_boundary.clone(),
                    last_group: first_boundary,
                },
                Ll2ShardEntry {
                    file: second_file,
                    original_part: 1,
                    shard_index: 0,
                    rows: 1,
                    data_bytes: second_line.len() as u64 + 1,
                    compressed_bytes: second_bytes,
                    first_group: second_boundary.clone(),
                    last_group: second_boundary,
                },
            ],
        };
        manifest.write(&shard_period_root).unwrap();
        let rocksdb_dir = root.path().join(EXPECTED_DB_NAME);
        let config = Config {
            data_root,
            shard_root,
            period: period.into(),
            periods: vec![],
            rocksdb_dir: rocksdb_dir.clone(),
            workers: 1,
            progress_every: 0,
        };
        let args = Args {
            config: PathBuf::new(),
            dry_run: false,
            verify: false,
            preflight: false,
            reset_writing_period: false,
            period: Some(period.into()),
            part_index: None,
            shard_index: None,
            max_source_rows: None,
            rocksdb_dir: None,
            shard_root: None,
        };
        run(&config, &args, &[period.into()]).unwrap();
        verify(&config, &args, &[period.into()]).unwrap();

        let db = open_db(&rocksdb_dir).unwrap();
        assert_eq!(
            period_status(&db, period).unwrap(),
            Some(PeriodStatus::Done)
        );
        assert_eq!(
            read_period_census(&db, period).unwrap(),
            PeriodCensus {
                source_rows: 2,
                minute_snapshots: 1,
                report_rows: 2,
                active_rows: 1,
                zero_count_rows: 1,
            }
        );
        assert_eq!(
            Ll2Db::list_cf(&Options::default(), &rocksdb_dir)
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            ["default", "p:2026:AD", "replay_meta"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
    }

    #[test]
    fn jobs_are_interleaved_across_years() {
        let job = |period: &str, shard_index: u32| Job {
            period: period.into(),
            year: period[..4].parse().unwrap(),
            part_no: 0,
            shard_index,
            path: PathBuf::new(),
            expected_header: Arc::from("header"),
        };
        let jobs = interleave_year_jobs(vec![
            vec![
                job("2024-01-01_2025-01-01", 0),
                job("2024-01-01_2025-01-01", 1),
            ],
            vec![
                job("2025-01-01_2026-01-01", 0),
                job("2025-01-01_2026-01-01", 1),
            ],
        ]);
        assert_eq!(
            jobs.iter().map(|value| value.year).collect::<Vec<_>>(),
            [2024, 2025, 2024, 2025]
        );
    }
}
