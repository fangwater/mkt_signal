//! CME TAS 1-minute, 60-column supervision-label replay.
//!
//! Printable `cme_trade` records in RocksDB provide sparse minute TWAP/VWAP.
//! Daily exported `backtest_1s` parquet provides the last valid two-sided L1
//! midprice of each minute. Neither source is forward-filled here.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::drop_special_1min::{
    chicago_trade_date_yyyymmdd, synthesize_drop_special_1min, DropSpecialMinute,
};
use cme_tas_replay::ylabel_1m::{
    build_ylabel_rows, causal_prices_from_minutes, write_ylabel_parquet, YlabelRow,
};
use cme_tas_replay::{
    decode_cme_trade, decode_ric, encode_key, encode_ric, key_ts_utc_ns, parse_date_time_ns,
    research_root_exchange, research_root_of, SlimTrade, CF_CME_TRADE, KEY_LEN,
    RESEARCH_PRODUCT_ROOTS, RIC_LEN,
};
use log::info;
use polars::prelude::{
    DataFrame, Float64Chunked, Int64Chunked, ParquetReader, SerReader, StringChunked,
};
use rocksdb::{Direction, IteratorMode, Options, ReadOptions, DB};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

const DEFAULT_READAHEAD_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_WORKERS: usize = 32;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_ylabel_1m")]
struct Args {
    #[arg(long, default_value = "config/cme_tas_ylabel_1m.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    rocksdb_dir: PathBuf,
    #[serde(default = "default_secondary_dir")]
    secondary_dir: PathBuf,
    #[serde(default = "default_backtest_root")]
    backtest_root: PathBuf,
    #[serde(default = "default_out_root")]
    out_root: PathBuf,
    #[serde(default)]
    rics: Vec<String>,
    /// Restrict the emitted trading days. Input scans retain full history and
    /// future context so rolling/range values at the boundary remain correct.
    #[serde(default)]
    start: Option<String>,
    /// Exclusive UTC timestamp bound for emitted trading days.
    #[serde(default)]
    end: Option<String>,
    #[serde(default = "default_readahead_bytes")]
    readahead_bytes: usize,
    #[serde(default = "default_workers")]
    workers: usize,
    #[serde(default)]
    overwrite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProductKey {
    exchange: String,
    product: String,
}

#[derive(Debug, Default)]
struct BacktestRic {
    contract_id: Option<String>,
    // Raw minute left edge -> final valid second-start midprice in that minute.
    minute_midp: BTreeMap<i64, (i64, f64)>,
}

fn default_secondary_dir() -> PathBuf {
    PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb.ylabel_1m.secondary")
}

fn default_backtest_root() -> PathBuf {
    PathBuf::from("/mnt/hdd-raid5-72t/liang_torch/lseg_data/backtest_1s")
}

fn default_out_root() -> PathBuf {
    PathBuf::from(
        "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1m_drop_special_ylabel_1m",
    )
}

fn default_readahead_bytes() -> usize {
    DEFAULT_READAHEAD_BYTES
}

fn default_workers() -> usize {
    DEFAULT_WORKERS
}

fn open_rocksdb_secondary(primary: &Path, secondary: &Path) -> Result<DB> {
    if !primary.exists() {
        bail!("rocksdb {} does not exist", primary.display());
    }
    if let Some(parent) = secondary.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create secondary parent {}", parent.display()))?;
    }
    let mut options = Options::default();
    options.create_if_missing(false);
    options.create_missing_column_families(false);
    options.set_max_open_files(-1);
    let names = DB::list_cf(&options, primary)
        .with_context(|| format!("list column families {}", primary.display()))?;
    if !names.iter().any(|name| name == CF_CME_TRADE) {
        bail!(
            "rocksdb {} has no {CF_CME_TRADE} column family",
            primary.display()
        );
    }
    let db = DB::open_cf_as_secondary(&options, primary, secondary, &names).with_context(|| {
        format!(
            "open rocksdb secondary {} from {}",
            secondary.display(),
            primary.display()
        )
    })?;
    db.try_catch_up_with_primary()
        .context("catch up RocksDB secondary")?;
    Ok(db)
}

fn read_options(readahead_bytes: usize) -> ReadOptions {
    let mut options = ReadOptions::default();
    options.set_readahead_size(readahead_bytes);
    options.fill_cache(false);
    options
}

fn parse_bound(raw: Option<&str>) -> Result<Option<u64>> {
    raw.map(parse_date_time_ns).transpose()
}

fn next_item(iter: &mut rocksdb::DBIterator<'_>) -> Result<Option<(Box<[u8]>, Box<[u8]>)>> {
    match iter.next() {
        Some(item) => Ok(Some(item.context("iterate RocksDB")?)),
        None => Ok(None),
    }
}

fn next_ric_seek_key(ric: &str) -> Result<Option<[u8; KEY_LEN]>> {
    let mut prefix = encode_ric(ric)?;
    for index in (0..RIC_LEN).rev() {
        if prefix[index] != u8::MAX {
            prefix[index] += 1;
            for byte in prefix.iter_mut().skip(index + 1) {
                *byte = 0;
            }
            let mut key = [0u8; KEY_LEN];
            key[..RIC_LEN].copy_from_slice(&prefix);
            return Ok(Some(key));
        }
    }
    Ok(None)
}

fn collect_trade_rics(
    db: &DB,
    readahead_bytes: usize,
    filter: &[String],
) -> Result<BTreeSet<String>> {
    let cf = db
        .cf_handle(CF_CME_TRADE)
        .ok_or_else(|| anyhow!("missing {CF_CME_TRADE} column family"))?;
    let mut output = BTreeSet::new();
    let mut start = [0u8; KEY_LEN];
    loop {
        let mut iter = db.iterator_cf_opt(
            cf,
            read_options(readahead_bytes),
            IteratorMode::From(&start, Direction::Forward),
        );
        let Some((key, _)) = next_item(&mut iter)? else {
            break;
        };
        if key.len() != KEY_LEN {
            bail!("{CF_CME_TRADE} key length {} is not {KEY_LEN}", key.len());
        }
        let ric = decode_ric(&key[..RIC_LEN])?;
        if filter.is_empty() || filter.iter().any(|candidate| candidate == &ric) {
            if research_root_of(&ric)?.is_some() {
                output.insert(ric.clone());
            }
        }
        match next_ric_seek_key(&ric)? {
            Some(next) => start = next,
            None => break,
        }
    }
    Ok(output)
}

fn scan_trades(db: &DB, ric: &str, readahead_bytes: usize) -> Result<Vec<SlimTrade>> {
    let cf = db
        .cf_handle(CF_CME_TRADE)
        .ok_or_else(|| anyhow!("missing {CF_CME_TRADE} column family"))?;
    let start = encode_key(ric, 0, 0, 0)?;
    let mut output = Vec::new();
    for item in db.iterator_cf_opt(
        cf,
        read_options(readahead_bytes),
        IteratorMode::From(&start, Direction::Forward),
    ) {
        let (key, value) = item.context("scan cme_trade")?;
        if key.len() != KEY_LEN {
            bail!("{CF_CME_TRADE} key length {} is not {KEY_LEN}", key.len());
        }
        if decode_ric(&key[..RIC_LEN])? != ric {
            break;
        }
        let key_ts = key_ts_utc_ns(&key)?;
        let record = decode_cme_trade(&value)?;
        if record.ric != ric || record.ts_utc_ns != key_ts {
            bail!("{CF_CME_TRADE} key/value mismatch for {ric}");
        }
        output.push(record);
    }
    Ok(output)
}

fn str_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a StringChunked> {
    df.column(name)
        .with_context(|| format!("missing {name}"))?
        .str()
        .with_context(|| format!("{name} is not String"))
}

fn i64_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Int64Chunked> {
    df.column(name)
        .with_context(|| format!("missing {name}"))?
        .i64()
        .with_context(|| format!("{name} is not Int64"))
}

fn f64_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Float64Chunked> {
    df.column(name)
        .with_context(|| format!("missing {name}"))?
        .f64()
        .with_context(|| format!("{name} is not Float64"))
}

fn required_str<'a>(column: &'a StringChunked, index: usize, name: &str) -> Result<&'a str> {
    column
        .get(index)
        .ok_or_else(|| anyhow!("{name} is null at row {index}"))
}

fn required_i64(column: &Int64Chunked, index: usize, name: &str) -> Result<i64> {
    column
        .get(index)
        .ok_or_else(|| anyhow!("{name} is null at row {index}"))
}

fn product_key_for_ric(ric: &str) -> Result<ProductKey> {
    let root = research_root_of(ric)?.ok_or_else(|| anyhow!("unrouted RIC {ric}"))?;
    let exchange = research_root_exchange(root)
        .ok_or_else(|| anyhow!("research root {root} has no exchange"))?;
    Ok(ProductKey {
        exchange: exchange.to_string(),
        product: root.to_string(),
    })
}

fn add_backtest_file(
    path: &Path,
    expected: &ProductKey,
    allowed_rics: Option<&BTreeSet<String>>,
    output: &mut BTreeMap<String, BacktestRic>,
) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let df = ParquetReader::new(file)
        .finish()
        .with_context(|| format!("read {}", path.display()))?;
    let contract_id = str_col(&df, "contract_id")?;
    let ric = str_col(&df, "ric")?;
    let ts = i64_col(&df, "ts")?;
    let bid = f64_col(&df, "bid0p")?;
    let ask = f64_col(&df, "ask0p")?;
    for index in 0..df.height() {
        let value_ric = required_str(ric, index, "ric")?;
        if allowed_rics.is_some_and(|set| !set.contains(value_ric)) {
            continue;
        }
        if product_key_for_ric(value_ric)? != *expected {
            bail!(
                "{} has RIC {value_ric} outside {}/{}",
                path.display(),
                expected.exchange,
                expected.product
            );
        }
        let value_contract_id = required_str(contract_id, index, "contract_id")?;
        let value_ts = required_i64(ts, index, "ts")?;
        let entry = output.entry(value_ric.to_string()).or_default();
        match entry.contract_id.as_deref() {
            Some(previous) if previous != value_contract_id => bail!(
                "{} changes contract_id for {}: {previous} -> {value_contract_id}",
                path.display(),
                value_ric
            ),
            Some(_) => {}
            None => entry.contract_id = Some(value_contract_id.to_string()),
        }
        let (Some(bid), Some(ask)) = (bid.get(index), ask.get(index)) else {
            continue;
        };
        if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0 {
            continue;
        }
        let minute = value_ts - value_ts.rem_euclid(60);
        let midp = (bid + ask) / 2.0;
        match entry.minute_midp.get(&minute) {
            Some((previous_ts, _)) if *previous_ts > value_ts => {}
            _ => {
                entry.minute_midp.insert(minute, (value_ts, midp));
            }
        }
    }
    Ok(())
}

fn load_backtest_product(
    root: &Path,
    key: &ProductKey,
    allowed_rics: Option<&BTreeSet<String>>,
) -> Result<BTreeMap<String, BacktestRic>> {
    let dir = root.join(&key.exchange).join(&key.product);
    if !dir.exists() {
        return Ok(BTreeMap::new());
    }
    let mut paths = fs::read_dir(&dir)
        .with_context(|| format!("read {}", dir.display()))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            entry.file_type().ok().and_then(|kind| {
                (kind.is_file() && entry.path().extension().is_some_and(|ext| ext == "parquet"))
                    .then_some(entry.path())
            })
        })
        .collect::<Vec<_>>();
    paths.sort();
    let mut output = BTreeMap::new();
    for path in paths {
        add_backtest_file(&path, key, allowed_rics, &mut output)?;
    }
    Ok(output)
}

fn scan_trade_rows_parallel(
    db: Arc<DB>,
    rics: Vec<String>,
    readahead_bytes: usize,
    workers: usize,
) -> Result<BTreeMap<String, Vec<DropSpecialMinute>>> {
    if rics.is_empty() {
        return Ok(BTreeMap::new());
    }
    let workers = workers.min(rics.len()).max(1);
    let tasks = Arc::new(std::sync::Mutex::new(rics.into_iter()));
    let mut joins = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let db = Arc::clone(&db);
        let tasks = Arc::clone(&tasks);
        joins.push(
            thread::Builder::new()
                .name(format!("cme-tas-ylabel-trade-{worker_id}"))
                .spawn(move || -> Result<Vec<(String, Vec<DropSpecialMinute>)>> {
                    let mut output = Vec::new();
                    loop {
                        let next = tasks.lock().expect("ylabel task lock").next();
                        let Some(ric) = next else {
                            break;
                        };
                        let trades = scan_trades(&db, &ric, readahead_bytes)?;
                        let minutes = synthesize_drop_special_1min(&trades, &[], None)?;
                        output.push((ric, minutes));
                    }
                    Ok(output)
                })
                .with_context(|| format!("spawn ylabel trade worker {worker_id}"))?,
        );
    }
    let mut output = BTreeMap::new();
    for join in joins {
        for (ric, rows) in join
            .join()
            .map_err(|_| anyhow!("ylabel trade worker panicked"))??
        {
            if output.insert(ric.clone(), rows).is_some() {
                bail!("duplicate trade result for {ric}");
            }
        }
    }
    Ok(output)
}

fn all_product_keys() -> Vec<ProductKey> {
    RESEARCH_PRODUCT_ROOTS
        .iter()
        .filter_map(|root| {
            research_root_exchange(root).map(|exchange| ProductKey {
                exchange: exchange.to_string(),
                product: (*root).to_string(),
            })
        })
        .collect()
}

fn output_day_selected(day: u32, start_day: Option<u32>, end_day: Option<u32>) -> bool {
    start_day.is_none_or(|start| day >= start) && end_day.is_none_or(|end| day <= end)
}

fn selected_day_bounds(
    start_ns: Option<u64>,
    end_ns: Option<u64>,
) -> Result<(Option<u32>, Option<u32>)> {
    let start_day = start_ns
        .map(cme_tas_replay::tradeday_yyyymmdd)
        .transpose()?;
    let end_day = match end_ns {
        None => None,
        Some(0) => bail!("end must be after Unix epoch"),
        Some(end) => Some(cme_tas_replay::tradeday_yyyymmdd(end - 1)?),
    };
    if let (Some(start), Some(end)) = (start_day, end_day) {
        if start > end {
            bail!("start/end select no trading days");
        }
    }
    Ok((start_day, end_day))
}

fn unlink_overwrite_files(
    out_root: &Path,
    start_day: Option<u32>,
    end_day: Option<u32>,
) -> Result<u64> {
    if !out_root.exists() {
        return Ok(0);
    }
    let mut removed = 0u64;
    for exchange in fs::read_dir(out_root)? {
        let exchange = exchange?;
        if !exchange.file_type()?.is_dir() {
            continue;
        }
        for product in fs::read_dir(exchange.path())? {
            let product = product?;
            if !product.file_type()?.is_dir() {
                continue;
            }
            for file in fs::read_dir(product.path())? {
                let file = file?;
                if !file.file_type()?.is_file()
                    || file.path().extension().is_none_or(|ext| ext != "parquet")
                {
                    continue;
                }
                let Some(day) = file
                    .path()
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .and_then(|value| value.parse::<u32>().ok())
                else {
                    continue;
                };
                if output_day_selected(day, start_day, end_day) {
                    fs::remove_file(file.path())?;
                    removed += 1;
                }
            }
        }
    }
    Ok(removed)
}

fn write_product_days(
    out_root: &Path,
    key: &ProductKey,
    rows: Vec<YlabelRow>,
    overwrite: bool,
    start_day: Option<u32>,
    end_day: Option<u32>,
) -> Result<(u64, u64)> {
    let mut by_day: BTreeMap<u32, Vec<YlabelRow>> = BTreeMap::new();
    for row in rows {
        let day = chicago_trade_date_yyyymmdd(row.ts)?;
        if output_day_selected(day, start_day, end_day) {
            by_day.entry(day).or_default().push(row);
        }
    }
    let mut files = 0u64;
    let mut written = 0u64;
    for (day, mut rows) in by_day {
        rows.sort_by(|left, right| left.ts.cmp(&right.ts).then(left.ric.cmp(&right.ric)));
        let path = out_root
            .join(&key.exchange)
            .join(&key.product)
            .join(format!("{day}.parquet"));
        if path.exists() && !overwrite {
            continue;
        }
        write_ylabel_parquet(&path, &rows)?;
        files += 1;
        written += rows.len() as u64;
    }
    Ok((files, written))
}

fn run(config: &Config) -> Result<()> {
    if config.workers == 0 {
        bail!("workers must be >= 1");
    }
    if config.out_root == config.backtest_root {
        bail!("ylabel out_root must not overwrite backtest_1s");
    }
    if config
        .out_root
        .file_name()
        .is_some_and(|name| name == "baseline_data_1m")
    {
        bail!("ylabel out_root must not overwrite baseline_data_1m");
    }
    let start_ns = parse_bound(config.start.as_deref())?;
    let end_ns = parse_bound(config.end.as_deref())?;
    if let (Some(start), Some(end)) = (start_ns, end_ns) {
        if start >= end {
            bail!("start must be before end");
        }
    }
    let (start_day, end_day) = selected_day_bounds(start_ns, end_ns)?;
    info!(
        "opening CME ylabel RocksDB secondary {} from {}",
        config.secondary_dir.display(),
        config.rocksdb_dir.display()
    );
    let db = Arc::new(open_rocksdb_secondary(
        &config.rocksdb_dir,
        &config.secondary_dir,
    )?);
    let trade_rics = collect_trade_rics(&db, config.readahead_bytes, &config.rics)?;
    let allowed_rics = (!config.rics.is_empty())
        .then(|| config.rics.iter().cloned().collect::<BTreeSet<String>>());
    if config.overwrite {
        let removed = unlink_overwrite_files(&config.out_root, start_day, end_day)?;
        info!(
            "ylabel overwrite removed {removed} parquet files in trade-day range {:?}..{:?}",
            start_day, end_day
        );
    }
    fs::create_dir_all(&config.out_root)
        .with_context(|| format!("create {}", config.out_root.display()))?;

    let mut totals = (0u64, 0u64, 0u64);
    for key in all_product_keys() {
        let product_trade_rics = trade_rics
            .iter()
            .filter_map(|ric| {
                (product_key_for_ric(ric).ok().as_ref() == Some(&key)).then(|| ric.clone())
            })
            .collect::<Vec<_>>();
        let backtest = load_backtest_product(&config.backtest_root, &key, allowed_rics.as_ref())?;
        if product_trade_rics.is_empty() && backtest.is_empty() {
            continue;
        }
        let trade_rows = scan_trade_rows_parallel(
            Arc::clone(&db),
            product_trade_rics,
            config.readahead_bytes,
            config.workers,
        )?;
        let rics = trade_rows
            .keys()
            .chain(backtest.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut rows = Vec::new();
        for ric in rics {
            let trade = trade_rows.get(&ric).map(Vec::as_slice).unwrap_or(&[]);
            let backtest_ric = backtest.get(&ric);
            let contract_id = trade
                .first()
                .map(|row| row.contract_id.as_str())
                .or_else(|| backtest_ric.and_then(|item| item.contract_id.as_deref()))
                .ok_or_else(|| anyhow!("{ric} has neither trade nor backtest contract_id"))?;
            let midps = backtest_ric
                .map(|item| {
                    item.minute_midp
                        .iter()
                        .map(|(&minute, &(_, midp))| (minute, midp))
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default();
            let prices = causal_prices_from_minutes(trade, &midps)?;
            rows.extend(build_ylabel_rows(contract_id, &ric, &prices));
        }
        let (files, written) = write_product_days(
            &config.out_root,
            &key,
            rows,
            config.overwrite,
            start_day,
            end_day,
        )?;
        totals.0 += 1;
        totals.1 += files;
        totals.2 += written;
        info!(
            "ylabel wrote {}/{}/ files={} rows={}",
            key.exchange, key.product, files, written
        );
    }
    info!(
        "cme_tas_ylabel_1m complete products={} files={} rows={}",
        totals.0, totals.1, totals.2
    );
    Ok(())
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config)
        .unwrap_or_else(|err| panic!("read config {}: {err}", args.config.display()));
    let config: Config = toml::from_str(&content)
        .unwrap_or_else(|err| panic!("parse config {}: {err}", args.config.display()));
    if let Err(err) = run(&config) {
        eprintln!("{err:#}");
        std::process::exit(1);
    }
}
