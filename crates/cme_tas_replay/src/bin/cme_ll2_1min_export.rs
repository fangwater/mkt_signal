//! Export compact LL2 minute RocksDB records into product/day parquet files.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::ll2_1min::{
    decode_ll2_minute, decode_ll2_minute_key, e9_to_f64, Ll2Minute, Ll2MinuteKey, CF_LL2_MINUTE,
    CF_LL2_MINUTE_STAGE, LL2_DEPTH_LEVELS,
};
use cme_tas_replay::{decade_base_from_utc_ns, parse_contract_id};
use crossbeam_channel::bounded;
use polars::prelude::{DataFrame, NamedFrom, ParquetWriter, Series};
use rocksdb::{IteratorMode, Options, DB};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

const DEFAULT_ROCKSDB_DIR: &str = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_ll2_1min_rocksdb";
const DEFAULT_SECONDARY_DIR: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_ll2_1min_rocksdb.export.secondary";
const DEFAULT_OUTPUT_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/level2_1min";
const DEFAULT_WRITE_WORKERS: usize = 4;

#[derive(Parser, Debug)]
#[command(name = "cme_ll2_1min_export")]
#[command(about = "Export LL2 minute RocksDB into {exchange}/{product}/{YYYYMMDD}.parquet")]
struct Args {
    #[arg(long, default_value = DEFAULT_ROCKSDB_DIR)]
    rocksdb_dir: PathBuf,
    #[arg(long, default_value = DEFAULT_SECONDARY_DIR)]
    secondary_dir: PathBuf,
    #[arg(long, default_value = DEFAULT_OUTPUT_ROOT)]
    out_root: PathBuf,
    #[arg(long, default_value_t = DEFAULT_WRITE_WORKERS)]
    write_workers: usize,
    #[arg(long)]
    overwrite: bool,
}

#[derive(Debug)]
struct OutputRow {
    contract_id: String,
    ric: String,
    ts: i64,
    source_ts_utc_ns: i64,
    update_count: i64,
    bid_prices: [Option<f64>; LL2_DEPTH_LEVELS],
    bid_sizes: [Option<f64>; LL2_DEPTH_LEVELS],
    bid_counts: [i32; LL2_DEPTH_LEVELS],
    ask_prices: [Option<f64>; LL2_DEPTH_LEVELS],
    ask_sizes: [Option<f64>; LL2_DEPTH_LEVELS],
    ask_counts: [i32; LL2_DEPTH_LEVELS],
}

#[derive(Debug)]
struct DayBatch {
    exchange: String,
    product_root: String,
    trading_day: u32,
    rows: Vec<OutputRow>,
}

fn open_secondary(primary: &Path, secondary: &Path) -> Result<DB> {
    if secondary.exists() {
        let mut entries = secondary
            .read_dir()
            .with_context(|| format!("read {}", secondary.display()))?;
        if entries.next().is_some() {
            bail!(
                "LL2 export secondary directory already exists and is nonempty: {}",
                secondary.display()
            );
        }
    }
    let names = DB::list_cf(&Options::default(), primary)
        .with_context(|| format!("list column families {}", primary.display()))?;
    if names.iter().any(|name| name == CF_LL2_MINUTE_STAGE) {
        bail!("LL2 minute RocksDB still contains staging CF; replay is incomplete");
    }
    if !names.iter().any(|name| name == CF_LL2_MINUTE) {
        bail!("LL2 minute RocksDB has no {CF_LL2_MINUTE} column family");
    }
    DB::open_cf_as_secondary(&Options::default(), primary, secondary, names).with_context(|| {
        format!(
            "open LL2 RocksDB secondary {} from {}",
            secondary.display(),
            primary.display()
        )
    })
}

fn output_row(key: Ll2MinuteKey, value: Ll2Minute) -> Result<OutputRow> {
    let source_ts_utc_ns = i64::try_from(value.source_ts_utc_ns)
        .map_err(|_| anyhow!("LL2 source timestamp overflows i64"))?;
    let contract = parse_contract_id(&key.ric, decade_base_from_utc_ns(value.source_ts_utc_ns)?)?
        .ok_or_else(|| anyhow!("LL2 key RIC {} is not a research contract", key.ric))?;
    if contract.0 != key.exchange || contract.1 != key.product_root {
        bail!("LL2 key route does not match RIC {}", key.ric);
    }
    Ok(OutputRow {
        contract_id: contract.2,
        ric: key.ric,
        ts: i64::try_from(key.minute_utc_sec)
            .map_err(|_| anyhow!("LL2 minute timestamp overflows i64"))?,
        source_ts_utc_ns,
        update_count: i64::from(value.update_count),
        bid_prices: std::array::from_fn(|index| e9_to_f64(value.bid_prices[index])),
        bid_sizes: std::array::from_fn(|index| e9_to_f64(value.bid_sizes[index])),
        bid_counts: std::array::from_fn(|index| {
            i32::try_from(value.bid_counts[index]).unwrap_or(i32::MAX)
        }),
        ask_prices: std::array::from_fn(|index| e9_to_f64(value.ask_prices[index])),
        ask_sizes: std::array::from_fn(|index| e9_to_f64(value.ask_sizes[index])),
        ask_counts: std::array::from_fn(|index| {
            i32::try_from(value.ask_counts[index]).unwrap_or(i32::MAX)
        }),
    })
}

fn rows_to_dataframe(rows: &[OutputRow]) -> Result<DataFrame> {
    let n = rows.len();
    let mut contract_id = Vec::with_capacity(n);
    let mut ric = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    let mut source_ts_utc_ns = Vec::with_capacity(n);
    let mut update_count = Vec::with_capacity(n);
    let mut bid_prices: Vec<Vec<Option<f64>>> = (0..LL2_DEPTH_LEVELS)
        .map(|_| Vec::with_capacity(n))
        .collect();
    let mut bid_sizes: Vec<Vec<Option<f64>>> = (0..LL2_DEPTH_LEVELS)
        .map(|_| Vec::with_capacity(n))
        .collect();
    let mut bid_counts: Vec<Vec<i32>> = (0..LL2_DEPTH_LEVELS)
        .map(|_| Vec::with_capacity(n))
        .collect();
    let mut ask_prices: Vec<Vec<Option<f64>>> = (0..LL2_DEPTH_LEVELS)
        .map(|_| Vec::with_capacity(n))
        .collect();
    let mut ask_sizes: Vec<Vec<Option<f64>>> = (0..LL2_DEPTH_LEVELS)
        .map(|_| Vec::with_capacity(n))
        .collect();
    let mut ask_counts: Vec<Vec<i32>> = (0..LL2_DEPTH_LEVELS)
        .map(|_| Vec::with_capacity(n))
        .collect();
    for row in rows {
        contract_id.push(row.contract_id.clone());
        ric.push(row.ric.clone());
        ts.push(row.ts);
        source_ts_utc_ns.push(row.source_ts_utc_ns);
        update_count.push(row.update_count);
        for level in 0..LL2_DEPTH_LEVELS {
            bid_prices[level].push(row.bid_prices[level]);
            bid_sizes[level].push(row.bid_sizes[level]);
            bid_counts[level].push(row.bid_counts[level]);
            ask_prices[level].push(row.ask_prices[level]);
            ask_sizes[level].push(row.ask_sizes[level]);
            ask_counts[level].push(row.ask_counts[level]);
        }
    }
    let mut columns = vec![
        Series::new("contract_id".into(), contract_id),
        Series::new("ric".into(), ric),
        Series::new("ts".into(), ts),
        Series::new("source_ts_utc_ns".into(), source_ts_utc_ns),
        Series::new("update_count".into(), update_count),
    ];
    for level in 0..LL2_DEPTH_LEVELS {
        columns.push(Series::new(
            format!("bid{level}p").into(),
            std::mem::take(&mut bid_prices[level]),
        ));
        columns.push(Series::new(
            format!("bid{level}v").into(),
            std::mem::take(&mut bid_sizes[level]),
        ));
        columns.push(Series::new(
            format!("bid{level}n").into(),
            std::mem::take(&mut bid_counts[level]),
        ));
        columns.push(Series::new(
            format!("ask{level}p").into(),
            std::mem::take(&mut ask_prices[level]),
        ));
        columns.push(Series::new(
            format!("ask{level}v").into(),
            std::mem::take(&mut ask_sizes[level]),
        ));
        columns.push(Series::new(
            format!("ask{level}n").into(),
            std::mem::take(&mut ask_counts[level]),
        ));
    }
    DataFrame::new(columns).context("build LL2 minute parquet dataframe")
}

fn write_day(out_root: &Path, batch: DayBatch, overwrite: bool) -> Result<u64> {
    let mut rows = batch.rows;
    rows.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then(left.contract_id.cmp(&right.contract_id))
            .then(left.ric.cmp(&right.ric))
    });
    let destination = out_root
        .join(&batch.exchange)
        .join(&batch.product_root)
        .join(format!("{:08}.parquet", batch.trading_day));
    if destination.exists() && !overwrite {
        bail!(
            "output exists and overwrite=false: {}",
            destination.display()
        );
    }
    let parent = destination.parent().expect("output has parent");
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let temporary = destination.with_extension("parquet.tmp");
    let mut dataframe = rows_to_dataframe(&rows)?;
    let file =
        File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?;
    ParquetWriter::new(file)
        .finish(&mut dataframe)
        .with_context(|| format!("write {}", temporary.display()))?;
    fs::rename(&temporary, &destination).with_context(|| {
        format!(
            "rename {} -> {}",
            temporary.display(),
            destination.display()
        )
    })?;
    Ok(rows.len() as u64)
}

fn run(args: &Args) -> Result<()> {
    if args.write_workers == 0 {
        bail!("write_workers must be positive");
    }
    if args.out_root == Path::new("/mnt/hdd-raid5-72t/liang_torch/lseg_data/level2_1s") {
        bail!("refusing to write level2_1s; this exporter only owns level2_1min");
    }
    let db = open_secondary(&args.rocksdb_dir, &args.secondary_dir)?;
    let cf = db
        .cf_handle(CF_LL2_MINUTE)
        .ok_or_else(|| anyhow!("missing {CF_LL2_MINUTE}"))?;
    let abort = Arc::new(AtomicBool::new(false));
    let files = Arc::new(AtomicU64::new(0));
    let rows_written = Arc::new(AtomicU64::new(0));
    let (sender, receiver) = bounded::<DayBatch>(args.write_workers * 2);
    let mut writers = Vec::with_capacity(args.write_workers);
    for worker in 0..args.write_workers {
        let receiver = receiver.clone();
        let out_root = args.out_root.clone();
        let abort = Arc::clone(&abort);
        let files = Arc::clone(&files);
        let rows_written = Arc::clone(&rows_written);
        let overwrite = args.overwrite;
        writers.push(
            thread::Builder::new()
                .name(format!("cme-ll2-export-{worker}"))
                .spawn(move || -> Result<()> {
                    while let Ok(batch) = receiver.recv() {
                        if abort.load(Ordering::Relaxed) {
                            break;
                        }
                        match write_day(&out_root, batch, overwrite) {
                            Ok(rows) => {
                                files.fetch_add(1, Ordering::Relaxed);
                                rows_written.fetch_add(rows, Ordering::Relaxed);
                            }
                            Err(error) => {
                                abort.store(true, Ordering::Relaxed);
                                return Err(error);
                            }
                        }
                    }
                    Ok(())
                })?,
        );
    }
    drop(receiver);
    let mut active: Option<DayBatch> = None;
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        if abort.load(Ordering::Relaxed) {
            break;
        }
        let (raw_key, raw_value) = item.context("iterate LL2 minute RocksDB")?;
        let key = decode_ll2_minute_key(&raw_key)?;
        let value = decode_ll2_minute(&raw_value)?;
        let same_day = active.as_ref().is_some_and(|batch| {
            batch.exchange == key.exchange
                && batch.product_root == key.product_root
                && batch.trading_day == key.trading_day
        });
        if !same_day {
            if let Some(batch) = active.take() {
                sender
                    .send(batch)
                    .context("send LL2 export day to writer")?;
            }
            active = Some(DayBatch {
                exchange: key.exchange.clone(),
                product_root: key.product_root.clone(),
                trading_day: key.trading_day,
                rows: Vec::new(),
            });
        }
        active
            .as_mut()
            .expect("active batch initialized")
            .rows
            .push(output_row(key, value)?);
    }
    if let Some(batch) = active {
        sender.send(batch).context("send final LL2 export day")?;
    }
    drop(sender);
    for writer in writers {
        writer
            .join()
            .map_err(|_| anyhow!("LL2 parquet writer panicked"))??;
    }
    if abort.load(Ordering::Relaxed) {
        bail!("LL2 parquet export aborted");
    }
    println!(
        "exported LL2 minute parquet files={} rows={}",
        files.load(Ordering::Relaxed),
        rows_written.load(Ordering::Relaxed)
    );
    Ok(())
}

fn main() {
    let args = Args::parse();
    if let Err(error) = run(&args) {
        eprintln!("cme_ll2_1min_export failed: {error:#}");
        std::process::exit(1);
    }
}
