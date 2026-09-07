//! One process, many TradDay workers, one RocksDB.

use anyhow::{bail, Context, Result};
use chrono::{Datelike, NaiveDate};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options,
    WriteBatch, WriteOptions,
};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::codec::{
    day_meta_key, encode_depth, encode_key, encode_oi, encode_queue, encode_trade,
    is_product_cf_name, product_cf_name, CF_REPLAY_META, KIND_DEPTH, KIND_OI, KIND_QUEUE,
    KIND_TRADE, STATUS_DONE, STATUS_WRITING,
};
use crate::events::{process_instrument, process_queues, QueueSnapshot, Snapshot};
use crate::session::Exchange;
use crate::source::{
    iter_calendar_days, load_queue_day, load_trad_day, parse_day, DEFAULT_LOOKBACK_DAYS,
    DEFAULT_OVERLAP_CUT,
};

pub type L2Db = DBWithThreadMode<MultiThreaded>;

pub const FORBIDDEN_ROCKSDB_MARK: &str = "cme_tas_rocksdb";
pub const DEFAULT_ROCKSDB_DIR: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/cn_l2_rocksdb";
const BATCH_PUTS: usize = 65_536;

#[derive(Clone, Debug)]
pub struct Job {
    pub exchange: Exchange,
    pub trad_day: NaiveDate,
}

#[derive(Clone, Debug, Default)]
pub struct DayStats {
    pub trades: u64,
    pub depths: u64,
    pub open_ints: u64,
    pub queues: u64,
    pub snapshots: u64,
}

fn product_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_write_buffer_size(16 * 1024 * 1024);
    opts.set_max_write_buffer_number(2);
    opts.set_min_write_buffer_number_to_merge(1);
    opts
}

pub fn refuse_cme_rocksdb(path: &Path) -> Result<()> {
    let text = path.to_string_lossy();
    if text.contains(FORBIDDEN_ROCKSDB_MARK) {
        bail!(
            "refusing to write CN L2 into a CME RocksDB path: {}",
            path.display()
        );
    }
    if let Ok(canonical) = path.canonicalize() {
        if canonical.to_string_lossy().contains(FORBIDDEN_ROCKSDB_MARK) {
            bail!(
                "refusing to write CN L2 into a CME RocksDB path: {}",
                canonical.display()
            );
        }
    }
    Ok(())
}

pub fn open_rocksdb(path: &Path) -> Result<L2Db> {
    refuse_cme_rocksdb(path)?;
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
    let names = if path.is_dir() && path.read_dir()?.next().is_some() {
        L2Db::list_cf(&Options::default(), path)
            .with_context(|| format!("list column families {}", path.display()))?
    } else {
        vec!["default".to_string(), CF_REPLAY_META.to_string()]
    };
    for name in &names {
        if name != "default" && name != CF_REPLAY_META && !is_product_cf_name(name) {
            bail!(
                "RocksDB {} has unsupported column family {name:?}; use a new CN L2 directory",
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
    L2Db::open_cf_descriptors(&db_opts, path, descriptors)
        .with_context(|| format!("open rocksdb {}", path.display()))
}

pub fn open_rocksdb_read_only(path: &Path) -> Result<L2Db> {
    refuse_cme_rocksdb(path)?;
    if !path.is_dir() {
        bail!("rocksdb_dir {} is not a directory", path.display());
    }
    let names = L2Db::list_cf(&Options::default(), path)
        .with_context(|| format!("list column families {}", path.display()))?;
    for name in &names {
        if name != "default" && name != CF_REPLAY_META && !is_product_cf_name(name) {
            bail!(
                "RocksDB {} has unsupported column family {name:?}; refusing to read",
                path.display()
            );
        }
    }
    let mut db_opts = Options::default();
    db_opts.set_max_open_files(65_536);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    L2Db::open_cf_for_read_only(&db_opts, path, names, false)
        .with_context(|| format!("open rocksdb read-only {}", path.display()))
}

fn replay_meta_cf(db: &L2Db) -> Result<std::sync::Arc<BoundColumnFamily<'_>>> {
    db.cf_handle(CF_REPLAY_META)
        .ok_or_else(|| anyhow::anyhow!("column family {CF_REPLAY_META} missing"))
}

pub fn read_day_status(db: &L2Db, exchange: Exchange, day: NaiveDate) -> Result<Option<Vec<u8>>> {
    let cf = replay_meta_cf(db)?;
    db.get_cf(&cf, day_meta_key(exchange.as_str(), day))
        .with_context(|| format!("read day watermark {} {day}", exchange.as_str()))
}

pub fn claim_day(db: &L2Db, exchange: Exchange, day: NaiveDate) -> Result<()> {
    match read_day_status(db, exchange, day)? {
        Some(bytes) if bytes == STATUS_DONE => {
            bail!(
                "{} {day} is already done in this RocksDB; refuse to overwrite",
                exchange.as_str()
            )
        }
        Some(bytes) if bytes == STATUS_WRITING => bail!(
            "{} {day} is marked writing; previous run did not finish",
            exchange.as_str()
        ),
        Some(bytes) => bail!(
            "{} {day} has unknown watermark {}",
            exchange.as_str(),
            String::from_utf8_lossy(&bytes)
        ),
        None => {
            let cf = replay_meta_cf(db)?;
            let mut opts = WriteOptions::default();
            opts.set_sync(true);
            db.put_cf_opt(
                &cf,
                day_meta_key(exchange.as_str(), day),
                STATUS_WRITING,
                &opts,
            )
            .with_context(|| format!("claim {} {day}", exchange.as_str()))?;
            Ok(())
        }
    }
}

pub fn finish_day(db: &L2Db, exchange: Exchange, day: NaiveDate) -> Result<()> {
    match read_day_status(db, exchange, day)? {
        Some(bytes) if bytes == STATUS_WRITING => {}
        Some(bytes) if bytes == STATUS_DONE => {
            bail!(
                "{} {day} is already done; refuse to mark done twice",
                exchange.as_str()
            )
        }
        Some(bytes) => bail!(
            "{} {day} has unknown watermark {}",
            exchange.as_str(),
            String::from_utf8_lossy(&bytes)
        ),
        None => bail!(
            "{} {day} missing writing watermark before finish",
            exchange.as_str()
        ),
    }
    let cf = replay_meta_cf(db)?;
    let mut opts = WriteOptions::default();
    opts.set_sync(true);
    db.put_cf_opt(
        &cf,
        day_meta_key(exchange.as_str(), day),
        STATUS_DONE,
        &opts,
    )
    .with_context(|| format!("finish {} {day}", exchange.as_str()))?;
    Ok(())
}

fn ensure_cf(db: &L2Db, lock: &Mutex<()>, name: &str) -> Result<()> {
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
    db: &'a L2Db,
    lock: &Mutex<()>,
    name: &str,
) -> Result<std::sync::Arc<BoundColumnFamily<'a>>> {
    ensure_cf(db, lock, name)?;
    db.cf_handle(name)
        .ok_or_else(|| anyhow::anyhow!("column family {name} missing after create"))
}

fn flush_batch(db: &L2Db, batch: &mut WriteBatch) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut opts = WriteOptions::default();
    opts.set_sync(false);
    db.write_opt(std::mem::take(batch), &opts)
        .context("write rocksdb batch")?;
    Ok(())
}

pub fn write_snapshots(
    db: &L2Db,
    cf_lock: &Mutex<()>,
    exchange: Exchange,
    snapshots: Vec<Snapshot>,
) -> Result<DayStats> {
    let mut stats = DayStats {
        snapshots: snapshots.len() as u64,
        ..DayStats::default()
    };
    let mut by_instrument: HashMap<String, Vec<Snapshot>> = HashMap::new();
    for snap in snapshots {
        by_instrument
            .entry(snap.instrument_id.clone())
            .or_default()
            .push(snap);
    }
    let mut batch = WriteBatch::default();
    let mut pending = 0usize;
    for mut group in by_instrument.into_values() {
        let Some(first) = group.first() else {
            continue;
        };
        let cf_name = product_cf_name(first.trad_day.year(), &first.product_id)?;
        let output = process_instrument(&mut group, exchange)?;
        let cf = product_cf(db, cf_lock, &cf_name)?;
        for (seq, trade) in output.trades {
            let key = encode_key(KIND_TRADE, &trade.instrument, trade.ts_utc_ns, seq)?;
            batch.put_cf(&cf, key, encode_trade(&trade)?);
            stats.trades += 1;
            pending += 1;
            if pending >= BATCH_PUTS {
                flush_batch(db, &mut batch)?;
                pending = 0;
            }
        }
        for depth in output.depths {
            let key = encode_key(KIND_DEPTH, &depth.instrument, depth.ts_utc_ns, 0)?;
            batch.put_cf(&cf, key, encode_depth(&depth)?);
            stats.depths += 1;
            pending += 1;
            if pending >= BATCH_PUTS {
                flush_batch(db, &mut batch)?;
                pending = 0;
            }
        }
        for (seq, oi) in output.open_ints {
            let key = encode_key(KIND_OI, &oi.instrument, oi.ts_utc_ns, seq)?;
            batch.put_cf(&cf, key, encode_oi(&oi)?);
            stats.open_ints += 1;
            pending += 1;
            if pending >= BATCH_PUTS {
                flush_batch(db, &mut batch)?;
                pending = 0;
            }
        }
    }
    flush_batch(db, &mut batch)?;
    Ok(stats)
}

pub fn write_queues(db: &L2Db, cf_lock: &Mutex<()>, queues: Vec<QueueSnapshot>) -> Result<u64> {
    let mut written = 0u64;
    let mut by_instrument: HashMap<String, Vec<QueueSnapshot>> = HashMap::new();
    for snap in queues {
        by_instrument
            .entry(snap.instrument_id.clone())
            .or_default()
            .push(snap);
    }
    let mut batch = WriteBatch::default();
    let mut pending = 0usize;
    for mut group in by_instrument.into_values() {
        let Some(first) = group.first() else {
            continue;
        };
        let cf_name = product_cf_name(first.trad_day.year(), &first.product_id)?;
        let output = process_queues(&mut group)?;
        let cf = product_cf(db, cf_lock, &cf_name)?;
        for (seq, queue) in output {
            let key = encode_key(KIND_QUEUE, &queue.instrument, queue.ts_utc_ns, seq)?;
            batch.put_cf(&cf, key, encode_queue(&queue)?);
            written += 1;
            pending += 1;
            if pending >= BATCH_PUTS {
                flush_batch(db, &mut batch)?;
                pending = 0;
            }
        }
    }
    flush_batch(db, &mut batch)?;
    Ok(written)
}

pub fn replay_day(
    db: &L2Db,
    cf_lock: &Mutex<()>,
    job: &Job,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
    lookback_days: u64,
) -> Result<DayStats> {
    let snapshots = load_trad_day(
        job.exchange,
        job.trad_day,
        l2_root,
        msg_root,
        overlap_cut,
        lookback_days,
    )?;
    let mut stats = write_snapshots(db, cf_lock, job.exchange, snapshots)?;
    let queues = load_queue_day(
        job.exchange,
        job.trad_day,
        l2_root,
        msg_root,
        overlap_cut,
        lookback_days,
    )?;
    stats.queues = write_queues(db, cf_lock, queues)?;
    Ok(stats)
}

#[derive(Clone, Debug)]
pub struct ReplayArgs {
    pub l2_root: Option<PathBuf>,
    pub msg_root: Option<PathBuf>,
    pub exchanges: Vec<Exchange>,
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub rocksdb_dir: PathBuf,
    pub workers: usize,
    pub overlap_cut: Option<NaiveDate>,
    pub lookback_days: u64,
}

impl ReplayArgs {
    pub fn overlap_cut_or_default(
        raw: Option<&str>,
        both_roots: bool,
    ) -> Result<Option<NaiveDate>> {
        if let Some(text) = raw {
            if text.trim().is_empty() {
                return Ok(None);
            }
            return Ok(Some(parse_day(text)?));
        }
        if both_roots {
            Ok(Some(parse_day(DEFAULT_OVERLAP_CUT)?))
        } else {
            Ok(None)
        }
    }
}

pub fn list_jobs(args: &ReplayArgs) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for exchange in &args.exchanges {
        for trad_day in iter_calendar_days(args.start, args.end)? {
            jobs.push(Job {
                exchange: *exchange,
                trad_day,
            });
        }
    }
    Ok(jobs)
}

pub fn run_replay(args: ReplayArgs) -> Result<(DayStats, usize)> {
    if args.l2_root.is_none() && args.msg_root.is_none() {
        bail!("--l2-root or --msg-root is required");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    if args.end < args.start {
        bail!("end precedes start");
    }
    let db = open_rocksdb(&args.rocksdb_dir)?;
    let all_jobs = list_jobs(&args)?;
    let mut jobs = Vec::new();
    for job in all_jobs {
        match read_day_status(&db, job.exchange, job.trad_day)? {
            Some(bytes) if bytes == STATUS_DONE => continue,
            Some(bytes) if bytes == STATUS_WRITING => bail!(
                "{} {} is marked writing; previous run did not finish",
                job.exchange.as_str(),
                job.trad_day
            ),
            Some(bytes) => bail!(
                "{} {} has unknown watermark {}",
                job.exchange.as_str(),
                job.trad_day,
                String::from_utf8_lossy(&bytes)
            ),
            None => jobs.push(job),
        }
    }
    let db = Arc::new(db);
    let cf_lock = Arc::new(Mutex::new(()));
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = crossbeam_channel::unbounded::<Job>();
    for job in jobs.iter().cloned() {
        tx.send(job).expect("enqueue TradDay job");
    }
    drop(tx);
    let workers = args.workers.min(jobs.len().max(1));
    let l2_root = args.l2_root.clone();
    let msg_root = args.msg_root.clone();
    let overlap_cut = args.overlap_cut;
    let lookback_days = if args.lookback_days == 0 {
        DEFAULT_LOOKBACK_DAYS
    } else {
        args.lookback_days
    };
    let mut handles = Vec::new();
    for worker_id in 0..workers {
        let rx = rx.clone();
        let db = Arc::clone(&db);
        let cf_lock = Arc::clone(&cf_lock);
        let stop = Arc::clone(&stop);
        let l2_root = l2_root.clone();
        let msg_root = msg_root.clone();
        handles.push(std::thread::spawn(move || -> Result<(DayStats, usize)> {
            let mut stats = DayStats::default();
            let mut days = 0usize;
            while let Ok(job) = rx.recv() {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(err) = claim_day(&db, job.exchange, job.trad_day) {
                    stop.store(true, Ordering::Relaxed);
                    return Err(err).with_context(|| {
                        format!(
                            "worker {worker_id} claim {} {}",
                            job.exchange.as_str(),
                            job.trad_day
                        )
                    });
                }
                let day = match replay_day(
                    &db,
                    &cf_lock,
                    &job,
                    l2_root.as_deref(),
                    msg_root.as_deref(),
                    overlap_cut,
                    lookback_days,
                ) {
                    Ok(day) => day,
                    Err(err) => {
                        stop.store(true, Ordering::Relaxed);
                        return Err(err).with_context(|| {
                            format!(
                                "worker {worker_id} {} {}",
                                job.exchange.as_str(),
                                job.trad_day
                            )
                        });
                    }
                };
                if let Err(err) = finish_day(&db, job.exchange, job.trad_day) {
                    stop.store(true, Ordering::Relaxed);
                    return Err(err).with_context(|| {
                        format!(
                            "worker {worker_id} finish {} {}",
                            job.exchange.as_str(),
                            job.trad_day
                        )
                    });
                }
                if day.snapshots > 0 || day.queues > 0 {
                    eprintln!(
                        "{} {} trades={} depths={} oi={} queues={}",
                        job.exchange.as_str(),
                        job.trad_day,
                        day.trades,
                        day.depths,
                        day.open_ints,
                        day.queues
                    );
                }
                stats.trades += day.trades;
                stats.depths += day.depths;
                stats.open_ints += day.open_ints;
                stats.queues += day.queues;
                stats.snapshots += day.snapshots;
                days += 1;
            }
            Ok((stats, days))
        }));
    }
    let mut stats = DayStats::default();
    let mut days = 0usize;
    let mut first_err = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok((part, n))) => {
                stats.trades += part.trades;
                stats.depths += part.depths;
                stats.open_ints += part.open_ints;
                stats.queues += part.queues;
                stats.snapshots += part.snapshots;
                days += n;
            }
            Ok(Err(err)) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
            Err(_) => {
                if first_err.is_none() {
                    first_err = Some(anyhow::anyhow!("worker thread panicked"));
                }
            }
        }
    }
    if let Some(err) = first_err {
        return Err(err);
    }
    db.flush().context("flush rocksdb")?;
    db.cancel_all_background_work(true);
    drop(db);
    Ok((stats, days))
}
