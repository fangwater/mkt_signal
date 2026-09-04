//! Fill the 12 order-size columns in CME baseline_data_1min.
//!
//! Each Chicago TradDay month uses exact linear P50/P90 thresholds from the
//! previous natural month for the same product. Only printable `cme_trade`
//! records are sampled. Implied prints participate in the total size buckets
//! but not directional buy/sell buckets; Special records are deliberately
//! never read from RocksDB by this program.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{Datelike, Months, NaiveDate};
use clap::Parser;
use cme_tas_replay::baseline_1min::{SizeBuckets, SizeThresholds};
use cme_tas_replay::product::{encode_all_key, exch_event_time_ns, product_cf_name, ALL_KEY_LEN};
use cme_tas_replay::{
    decode_cme_trade, decode_period_status, price_e9_to_f64, PeriodStatus, AGGRESSOR_BUY,
    AGGRESSOR_IMPLIED, AGGRESSOR_SELL, CF_REPLAY_META, KIND_CME_TRADE, MISSING_EXCH_HMS_NS,
    MISSING_VOLUME, RIC_LEN,
};
use mimalloc::MiMalloc;
use polars::prelude::{
    DataFrame, NamedFrom, ParquetCompression, ParquetReader, ParquetWriter, SerReader, Series,
};
use rayon::prelude::*;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode,
    MultiThreaded, Options,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

type DB = DBWithThreadMode<MultiThreaded>;

const DEFAULT_TAS_DB: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products";
const DEFAULT_INPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min";
const DEFAULT_OUTPUT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min_size_staging";
const DEFAULT_SECONDARY: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products.size.secondary";
const DEFAULT_PSQL: &str = "/mnt/nvme-raid0-28t/apps/pgsql16/bin/psql";
const DEFAULT_PG_SOCKET: &str = "/mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run";
const PERIOD_META_PREFIX: &str = "period:";
const NS_PER_SEC: u64 = 1_000_000_000;
const HALF_DAY_NS: u64 = 43_200 * NS_PER_SEC;
const TAS_BLOCK_CACHE_BYTES: usize = 1024 * 1024 * 1024;

const SIZE_COLUMNS: &[&str] = &[
    "large_order",
    "medium_order",
    "small_order",
    "large_buy",
    "large_sell",
    "medium_buy",
    "medium_sell",
    "small_buy",
    "small_sell",
    "net_buy_large",
    "net_buy_medium",
    "net_buy_small",
];

#[derive(Parser, Debug)]
#[command(name = "cme_baseline_fill_size_buckets")]
struct Args {
    #[arg(long, default_value = DEFAULT_TAS_DB)]
    tas_rocksdb: PathBuf,
    #[arg(long, default_value = DEFAULT_SECONDARY)]
    tas_secondary: PathBuf,
    #[arg(long, default_value = DEFAULT_INPUT)]
    input_root: PathBuf,
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    output_root: PathBuf,
    #[arg(long, default_value = "2020-01-01")]
    start: NaiveDate,
    #[arg(long, default_value = "2026-06-01")]
    end: NaiveDate,
    #[arg(long, value_delimiter = ',', default_value = "ES,NQ,RTY,YM,GC,CL")]
    products: Vec<String>,
    #[arg(long, default_value_t = 2)]
    workers: usize,
    #[arg(long)]
    overwrite: bool,
    /// Resume a staging root. A product-month is skipped only after its
    /// completed month audit exists; a partial month is recomputed from input.
    #[arg(long)]
    resume: bool,
    #[arg(long)]
    direct_read_only: bool,
    #[arg(long)]
    max_months: Option<usize>,
    #[arg(long, default_value = DEFAULT_PSQL)]
    psql: PathBuf,
    #[arg(long, default_value = DEFAULT_PG_SOCKET)]
    pg_socket: PathBuf,
}

#[derive(Clone, Copy, Debug)]
struct ProductSpec {
    product: &'static str,
    exchange: &'static str,
}

const PRODUCTS: &[ProductSpec] = &[
    ProductSpec {
        product: "ES",
        exchange: "CME",
    },
    ProductSpec {
        product: "NQ",
        exchange: "CME",
    },
    ProductSpec {
        product: "RTY",
        exchange: "CME",
    },
    ProductSpec {
        product: "YM",
        exchange: "CBOT",
    },
    ProductSpec {
        product: "GC",
        exchange: "COMEX",
    },
    ProductSpec {
        product: "CL",
        exchange: "NYMEX",
    },
];

#[derive(Clone, Debug)]
struct DayJob {
    day: NaiveDate,
    input: PathBuf,
    output: PathBuf,
}

#[derive(Clone, Debug)]
struct MonthJob {
    product: String,
    exchange: String,
    year: i32,
    month: u32,
    days: Vec<DayJob>,
    publish: bool,
}

#[derive(Clone, Copy, Debug)]
struct TradeRef {
    day_index: u16,
    row_index: u32,
    amount: f64,
    aggressor: u8,
}

struct MonthScan {
    row_counts: Vec<usize>,
    trades: Vec<TradeRef>,
    special_records: u64,
}

#[derive(Serialize)]
struct MonthAudit {
    product: String,
    exchange: String,
    chicago_trading_month: String,
    threshold_source_month: Option<String>,
    threshold_method: &'static str,
    threshold_sample: &'static str,
    threshold_sample_trades: u64,
    implied_policy: &'static str,
    special_policy: &'static str,
    p50: Option<f64>,
    p90: Option<f64>,
    files: u64,
    rows: u64,
    printable_trades: u64,
    implied_trades: u64,
    special_records_excluded: u64,
    traded_minutes: u64,
}

fn product_spec(product: &str) -> Result<ProductSpec> {
    PRODUCTS
        .iter()
        .copied()
        .find(|spec| spec.product == product)
        .ok_or_else(|| anyhow!("unsupported CME baseline product {product:?}"))
}

fn validate_args(args: &Args) -> Result<Vec<ProductSpec>> {
    if args.start >= args.end || args.workers == 0 {
        bail!("invalid date range or worker count");
    }
    if args.start.day() != 1 || args.end.day() != 1 {
        bail!("--start and --end must be first-of-month boundaries");
    }
    if args.input_root == args.output_root {
        bail!("input and output roots must differ; write and validate staging first");
    }
    if args.output_root.starts_with(&args.input_root) {
        bail!("output root must not be inside the input root");
    }
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for product in &args.products {
        let product = product.trim().to_ascii_uppercase();
        if seen.insert(product.clone()) {
            output.push(product_spec(&product)?);
        }
    }
    if output.is_empty() {
        bail!("no products selected");
    }
    Ok(output)
}

fn list_month_jobs(args: &Args, products: &[ProductSpec]) -> Result<Vec<MonthJob>> {
    let warmup_start = args
        .start
        .checked_sub_months(Months::new(1))
        .context("cannot compute previous threshold month")?;
    let mut grouped: BTreeMap<(String, String, i32, u32), Vec<DayJob>> = BTreeMap::new();
    for spec in products {
        let input_dir = args.input_root.join(spec.exchange).join(spec.product);
        if !input_dir.is_dir() {
            bail!("missing baseline directory {}", input_dir.display());
        }
        for entry in fs::read_dir(&input_dir)? {
            let input = entry?.path();
            if input.extension().and_then(|value| value.to_str()) != Some("parquet") {
                continue;
            }
            let stem = input
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("");
            let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") else {
                continue;
            };
            if day < warmup_start || day >= args.end {
                continue;
            }
            let output = args
                .output_root
                .join(spec.exchange)
                .join(spec.product)
                .join(format!("{stem}.parquet"));
            grouped
                .entry((
                    spec.product.to_string(),
                    spec.exchange.to_string(),
                    day.year(),
                    day.month(),
                ))
                .or_default()
                .push(DayJob { day, input, output });
        }
    }
    let mut jobs = grouped
        .into_iter()
        .map(|((product, exchange, year, month), mut days)| {
            days.sort_by_key(|job| job.day);
            MonthJob {
                product,
                exchange,
                year,
                month,
                days,
                publish: true,
            }
        })
        .collect::<Vec<_>>();
    jobs.sort_by(|left, right| {
        left.year
            .cmp(&right.year)
            .then(left.month.cmp(&right.month))
            .then(left.product.cmp(&right.product))
    });
    let mut target_keys = jobs
        .iter()
        .filter(|job| job.days[0].day >= args.start)
        .map(|job| (job.product.clone(), job.year, job.month))
        .collect::<Vec<_>>();
    if let Some(limit) = args.max_months {
        target_keys.truncate(limit);
    }
    if target_keys.is_empty() {
        bail!("no baseline months selected");
    }
    let target_keys = target_keys.into_iter().collect::<BTreeSet<_>>();
    let warmup_keys = target_keys
        .iter()
        .map(|(product, year, month)| {
            let day = NaiveDate::from_ymd_opt(*year, *month, 1)
                .expect("valid grouped month")
                .checked_sub_months(Months::new(1))
                .expect("previous grouped month");
            (product.clone(), day.year(), day.month())
        })
        .collect::<BTreeSet<_>>();
    jobs.retain(|job| {
        let key = (job.product.clone(), job.year, job.month);
        target_keys.contains(&key) || warmup_keys.contains(&key)
    });
    for job in &mut jobs {
        let key = (job.product.clone(), job.year, job.month);
        job.publish = target_keys.contains(&key);
    }
    if args.resume {
        for job in &mut jobs {
            if !job.publish {
                continue;
            }
            let audit = args
                .output_root
                .join("_audit")
                .join("size_buckets")
                .join(&job.product)
                .join(format!("{:04}{:02}.json", job.year, job.month));
            if audit.exists() {
                job.publish = false;
            }
        }
    } else if !args.overwrite {
        for job in jobs.iter().filter(|job| job.publish) {
            if let Some(path) = job
                .days
                .iter()
                .find_map(|day| day.output.exists().then_some(&day.output))
            {
                bail!(
                    "staging output already exists: {}; pass --overwrite to resume",
                    path.display()
                );
            }
        }
    }
    if !jobs.iter().any(|job| job.publish) {
        bail!("no baseline months selected after resume filtering");
    }
    Ok(jobs)
}

fn load_multipliers(args: &Args, products: &[ProductSpec]) -> Result<BTreeMap<String, f64>> {
    let query = "SELECT product_root, exchange, volume_multiple, verified FROM public.cme_research_product_multipliers WHERE product_root IN ('ES','NQ','RTY','YM','GC','CL') ORDER BY product_root";
    let output = Command::new(&args.psql)
        .args([
            "-h",
            args.pg_socket.to_str().context("PG socket is not UTF-8")?,
            "-p",
            "5433",
            "-U",
            "u171",
            "-d",
            "market_metadata",
            "-At",
            "-F",
            "\t",
            "-c",
            query,
        ])
        .output()
        .with_context(|| format!("run {}", args.psql.display()))?;
    if !output.status.success() {
        bail!(
            "multiplier query failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let mut loaded = BTreeMap::new();
    for (index, line) in String::from_utf8(output.stdout)?.lines().enumerate() {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 4 {
            bail!("multiplier row {} has {} fields", index + 1, fields.len());
        }
        let product = fields[0].to_string();
        let expected = product_spec(&product)?;
        if fields[1] != expected.exchange || fields[3] != "t" {
            bail!("unverified or mismatched multiplier row {line:?}");
        }
        let multiplier = fields[2].parse::<f64>()?;
        if !(multiplier.is_finite() && multiplier > 0.0) {
            bail!("invalid multiplier {multiplier} for {product}");
        }
        loaded.insert(product, multiplier);
    }
    for spec in products {
        if !loaded.contains_key(spec.product) {
            bail!("missing verified multiplier for {}", spec.product);
        }
    }
    Ok(loaded)
}

fn tas_options(block_cache: &Cache) -> Options {
    let mut options = Options::default();
    options.set_merge_operator_associative("quote_last", cme_tas_replay::product::quote_last_merge);
    let mut table_options = BlockBasedOptions::default();
    table_options.set_block_cache(block_cache);
    table_options.set_cache_index_and_filter_blocks(true);
    table_options.set_pin_l0_filter_and_index_blocks_in_cache(false);
    table_options.set_pin_top_level_index_and_filter(false);
    options.set_block_based_table_factory(&table_options);
    options
}

fn open_db(primary: &Path, secondary: &Path, direct: bool) -> Result<DB> {
    let cf_names = DB::list_cf(&Options::default(), primary)
        .with_context(|| format!("list TAS CFs in {}", primary.display()))?;
    let mut db_options = Options::default();
    db_options.set_max_open_files(512);
    db_options.set_max_file_opening_threads(8);
    db_options.set_skip_stats_update_on_db_open(true);
    // The TAS database has tens of thousands of SSTs across many column
    // families. Keep their index/filter blocks in one bounded cache instead
    // of retaining metadata separately in every table reader during open.
    let block_cache = Cache::new_lru_cache(TAS_BLOCK_CACHE_BYTES);
    let descriptors = cf_names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, tas_options(&block_cache)))
        .collect::<Vec<_>>();
    if direct {
        return DB::open_cf_descriptors_read_only(&db_options, primary, descriptors, false)
            .with_context(|| format!("open {} read-only", primary.display()));
    }
    if let Some(parent) = secondary.parent() {
        fs::create_dir_all(parent)?;
    }
    let db = DB::open_cf_descriptors_as_secondary(&db_options, primary, secondary, descriptors)
        .with_context(|| {
            format!(
                "open secondary {} from {}",
                secondary.display(),
                primary.display()
            )
        })?;
    db.try_catch_up_with_primary()?;
    Ok(db)
}

fn period_for_year(year: i32) -> Result<String> {
    match year {
        2017..=2025 => Ok(format!("{year:04}-01-01_{:04}-01-01", year + 1)),
        2026 => Ok("2026-01-01_2026-06-01".to_string()),
        _ => bail!("unsupported baseline year {year}"),
    }
}

fn require_done(db: &DB, year: i32) -> Result<()> {
    let period = period_for_year(year)?;
    let cf = db
        .cf_handle(CF_REPLAY_META)
        .context("missing replay_meta")?;
    let key = format!("{PERIOD_META_PREFIX}{period}");
    let value = db
        .get_cf(&cf, key.as_bytes())?
        .with_context(|| format!("TAS period {period} has no watermark"))?;
    if decode_period_status(&value)? != PeriodStatus::Done {
        bail!("TAS period {period} is not done");
    }
    Ok(())
}

fn key_prefix(ric: &str) -> Result<Vec<u8>> {
    Ok(encode_all_key(KIND_CME_TRADE, ric, 0, 0, 0)?[..1 + RIC_LEN].to_vec())
}

fn key_ts_ns(key: &[u8]) -> Result<u64> {
    if key.len() != ALL_KEY_LEN {
        bail!("TAS key length {} != {ALL_KEY_LEN}", key.len());
    }
    Ok(u64::from_be_bytes(key[17..25].try_into().unwrap()))
}

fn event_time_ns(source_ns: u64, exch_hms_ns: u64) -> Result<u64> {
    if exch_hms_ns == MISSING_EXCH_HMS_NS {
        Ok(source_ns)
    } else {
        exch_event_time_ns(source_ns, exch_hms_ns)
    }
}

fn read_parquet(path: &Path) -> Result<DataFrame> {
    ParquetReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", path.display()))
}

fn close_enough(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-8 * left.abs().max(right.abs()).max(1.0)
}

fn value_at(df: &DataFrame, name: &str, row: usize, path: &Path) -> Result<f64> {
    df.column(name)?
        .f64()?
        .get(row)
        .with_context(|| format!("null {name} row {row} in {}", path.display()))
}

fn scan_day(
    db: &DB,
    cf: &impl rocksdb::AsColumnFamilyRef,
    job: &DayJob,
    day_index: usize,
    multiplier: f64,
    trades: &mut Vec<TradeRef>,
) -> Result<(usize, u64)> {
    let df = read_parquet(&job.input)?;
    if df.height() > u32::MAX as usize || day_index > u16::MAX as usize {
        bail!("day or row index exceeds overlay encoding");
    }
    let rics = df.column("ric")?.str()?;
    let timestamps = df.column("ts")?.i64()?;
    let mut rows_by_ric: BTreeMap<String, BTreeMap<i64, u32>> = BTreeMap::new();
    for row in 0..df.height() {
        let ric = rics
            .get(row)
            .with_context(|| format!("null ric row {row} in {}", job.input.display()))?;
        let ts = timestamps
            .get(row)
            .with_context(|| format!("null ts row {row} in {}", job.input.display()))?;
        if ts % 60 != 0 {
            bail!("non-minute ts {ts} in {}", job.input.display());
        }
        if rows_by_ric
            .entry(ric.to_string())
            .or_default()
            .insert(ts, row as u32)
            .is_some()
        {
            bail!(
                "duplicate (ric, ts)=({ric}, {ts}) in {}",
                job.input.display()
            );
        }
    }

    let mut observed_count = vec![0u32; df.height()];
    let mut observed_amount = vec![0.0f64; df.height()];
    let mut observed_buy = vec![0.0f64; df.height()];
    let mut observed_sell = vec![0.0f64; df.height()];
    let mut observed_implied = vec![0.0f64; df.height()];
    for (ric, minute_rows) in &rows_by_ric {
        let first = *minute_rows.keys().next().context("empty RIC minute map")?;
        let last = *minute_rows
            .keys()
            .next_back()
            .context("empty RIC minute map")?;
        let padded_start = (u64::try_from(first)? * NS_PER_SEC).saturating_sub(HALF_DAY_NS);
        let padded_end = (u64::try_from(last + 60)? * NS_PER_SEC).saturating_add(HALF_DAY_NS);
        let prefix = key_prefix(ric)?;
        let seek = encode_all_key(KIND_CME_TRADE, ric, padded_start, 0, 0)?;
        for item in db.iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward)) {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            if key_ts_ns(&key)? > padded_end {
                break;
            }
            let rec = decode_cme_trade(&value)?;
            let event_ns = event_time_ns(rec.ts_utc_ns, rec.exch_hms_ns)?;
            let minute = i64::try_from(event_ns / NS_PER_SEC / 60 * 60)?;
            let Some(&row_index) = minute_rows.get(&minute) else {
                continue;
            };
            let Some(price) = price_e9_to_f64(rec.price).filter(|price| *price > 0.0) else {
                continue;
            };
            if rec.volume == MISSING_VOLUME || rec.volume == 0 {
                bail!("trade {ric} has invalid volume {}", rec.volume);
            }
            if !matches!(
                rec.aggressor,
                AGGRESSOR_IMPLIED | AGGRESSOR_BUY | AGGRESSOR_SELL
            ) {
                bail!("trade {ric} has invalid aggressor {}", rec.aggressor);
            }
            let amount = price * f64::from(rec.volume) * multiplier;
            let row = row_index as usize;
            observed_count[row] += 1;
            observed_amount[row] += amount;
            match rec.aggressor {
                AGGRESSOR_BUY => observed_buy[row] += amount,
                AGGRESSOR_SELL => observed_sell[row] += amount,
                AGGRESSOR_IMPLIED => observed_implied[row] += amount,
                _ => unreachable!(),
            }
            trades.push(TradeRef {
                day_index: day_index as u16,
                row_index,
                amount,
                aggressor: rec.aggressor,
            });
        }
    }

    let mut special_records = 0u64;
    for row in 0..df.height() {
        let expected_count = value_at(&df, "count", row, &job.input)?;
        let expected_amount = value_at(&df, "amount", row, &job.input)?;
        let expected_buy = value_at(&df, "buy_amount", row, &job.input)?;
        let expected_sell = value_at(&df, "sell_amount", row, &job.input)?;
        let expected_implied = value_at(&df, "implied_amount", row, &job.input)?;
        let special_count = value_at(&df, "special_count", row, &job.input)?;
        if expected_count != f64::from(observed_count[row])
            || !close_enough(expected_amount, observed_amount[row])
            || !close_enough(expected_buy, observed_buy[row])
            || !close_enough(expected_sell, observed_sell[row])
            || !close_enough(expected_implied, observed_implied[row])
        {
            bail!(
                "TAS/baseline mismatch {} row {}: count {}/{} amount {}/{} buy {}/{} sell {}/{} implied {}/{}",
                job.input.display(),
                row,
                expected_count,
                observed_count[row],
                expected_amount,
                observed_amount[row],
                expected_buy,
                observed_buy[row],
                expected_sell,
                observed_sell[row],
                expected_implied,
                observed_implied[row]
            );
        }
        if special_count < 0.0 || special_count.fract() != 0.0 {
            bail!(
                "invalid special_count {special_count} in {}",
                job.input.display()
            );
        }
        special_records += special_count as u64;
    }
    Ok((df.height(), special_records))
}

fn percentile_in_place(records: &mut [TradeRef], percentile: f64) -> Result<f64> {
    if records.is_empty() || !(0.0..=1.0).contains(&percentile) {
        bail!(
            "invalid percentile sample size={} p={percentile}",
            records.len()
        );
    }
    if records.len() == 1 {
        return Ok(records[0].amount);
    }
    let position = percentile * (records.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    let lower_value = {
        let (_, value, _) = records
            .select_nth_unstable_by(lower, |left, right| left.amount.total_cmp(&right.amount));
        value.amount
    };
    if lower == upper {
        return Ok(lower_value);
    }
    let upper_value = {
        let (_, value, _) = records
            .select_nth_unstable_by(upper, |left, right| left.amount.total_cmp(&right.amount));
        value.amount
    };
    let weight = position - lower as f64;
    Ok(lower_value * (1.0 - weight) + upper_value * weight)
}

fn validate_size_columns(
    df: &DataFrame,
    buckets: &[SizeBuckets],
    path: &Path,
    thresholds_available: bool,
) -> Result<u64> {
    if df.height() != buckets.len() {
        bail!(
            "row count changed for {}: {} != {}",
            path.display(),
            df.height(),
            buckets.len()
        );
    }
    let mut traded_minutes = 0u64;
    for (row, bucket) in buckets.iter().copied().enumerate() {
        let (nl, nm, ns) = bucket.nets();
        let expected = [
            ("large_order", bucket.large_order),
            ("medium_order", bucket.medium_order),
            ("small_order", bucket.small_order),
            ("large_buy", bucket.large_buy),
            ("large_sell", bucket.large_sell),
            ("medium_buy", bucket.medium_buy),
            ("medium_sell", bucket.medium_sell),
            ("small_buy", bucket.small_buy),
            ("small_sell", bucket.small_sell),
            ("net_buy_large", nl),
            ("net_buy_medium", nm),
            ("net_buy_small", ns),
        ];
        for (name, value) in expected {
            if !close_enough(value_at(df, name, row, path)?, value) {
                bail!("persisted {name} mismatch row {row} in {}", path.display());
            }
        }
        let amount = value_at(df, "amount", row, path)?;
        let directed =
            value_at(df, "buy_amount", row, path)? + value_at(df, "sell_amount", row, path)?;
        if thresholds_available {
            if !close_enough(bucket.total(), amount)
                || !close_enough(bucket.directional_total(), directed)
            {
                bail!(
                    "size-bucket invariant failed row {row} in {}",
                    path.display()
                );
            }
        } else if bucket.total() != 0.0 || bucket.directional_total() != 0.0 {
            bail!(
                "size buckets must be zero without a prior-month threshold at row {row} in {}",
                path.display()
            );
        }
        traded_minutes += u64::from(amount > 0.0);
    }
    Ok(traded_minutes)
}

fn replace_size_columns(
    df: &mut DataFrame,
    buckets: &[SizeBuckets],
    path: &Path,
    thresholds_available: bool,
) -> Result<u64> {
    if df.height() != buckets.len() {
        bail!(
            "row count changed for {}: {} != {}",
            path.display(),
            df.height(),
            buckets.len()
        );
    }
    let (net_large, net_medium, net_small): (Vec<_>, Vec<_>, Vec<_>) =
        buckets.iter().copied().map(SizeBuckets::nets).fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut large, mut medium, mut small), (nl, nm, ns)| {
                large.push(nl);
                medium.push(nm);
                small.push(ns);
                (large, medium, small)
            },
        );
    let columns = [
        (
            "large_order",
            buckets.iter().map(|b| b.large_order).collect(),
        ),
        (
            "medium_order",
            buckets.iter().map(|b| b.medium_order).collect(),
        ),
        (
            "small_order",
            buckets.iter().map(|b| b.small_order).collect(),
        ),
        ("large_buy", buckets.iter().map(|b| b.large_buy).collect()),
        ("large_sell", buckets.iter().map(|b| b.large_sell).collect()),
        ("medium_buy", buckets.iter().map(|b| b.medium_buy).collect()),
        (
            "medium_sell",
            buckets.iter().map(|b| b.medium_sell).collect(),
        ),
        ("small_buy", buckets.iter().map(|b| b.small_buy).collect()),
        ("small_sell", buckets.iter().map(|b| b.small_sell).collect()),
        ("net_buy_large", net_large),
        ("net_buy_medium", net_medium),
        ("net_buy_small", net_small),
    ];
    for (name, values) in columns {
        df.replace(name, Series::new(name.into(), values))?;
    }
    validate_size_columns(df, buckets, path, thresholds_available)
}

fn write_parquet_atomic(path: &Path, mut df: DataFrame) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let result = (|| -> Result<()> {
        ParquetWriter::new(File::create(&tmp)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)?;
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn write_parquet_serialized(lock: &Mutex<()>, path: &Path, df: DataFrame) -> Result<()> {
    let _guard = lock.lock().map_err(|_| anyhow!("parquet lock poisoned"))?;
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("cme-size-parquet".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn_scoped(scope, || write_parquet_atomic(path, df))
            .map_err(anyhow::Error::from)?
            .join()
            .map_err(|_| anyhow!("parquet writer panicked for {}", path.display()))?
    })
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let result = (|| -> Result<()> {
        serde_json::to_writer_pretty(File::create(&tmp)?, value)?;
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn scan_month(db: &DB, job: &MonthJob, multiplier: f64) -> Result<MonthScan> {
    let cf_name = product_cf_name(job.year as u16, &job.product)?;
    let cf = db
        .cf_handle(&cf_name)
        .with_context(|| format!("missing TAS CF {cf_name}"))?;
    let mut trades = Vec::new();
    let mut row_counts = Vec::with_capacity(job.days.len());
    let mut special_records = 0u64;
    for (day_index, day) in job.days.iter().enumerate() {
        let (rows, specials) = scan_day(db, &cf, day, day_index, multiplier, &mut trades)
            .with_context(|| format!("scan {} {}", job.product, day.day))?;
        row_counts.push(rows);
        special_records += specials;
    }
    if trades.is_empty() {
        bail!(
            "{} {:04}-{:02} has no printable trades",
            job.product,
            job.year,
            job.month
        );
    }
    Ok(MonthScan {
        row_counts,
        trades,
        special_records,
    })
}

fn previous_month(year: i32, month: u32) -> (i32, u32) {
    if month == 1 {
        (year - 1, 12)
    } else {
        (year, month - 1)
    }
}

fn process_month(
    db: &DB,
    job: &MonthJob,
    multiplier: f64,
    threshold_source: Option<(i32, u32, &mut Vec<TradeRef>)>,
    output_root: &Path,
    parquet_lock: &Mutex<()>,
) -> Result<(MonthAudit, Vec<TradeRef>)> {
    let scan = scan_month(db, job, multiplier)?;
    let expected_source = previous_month(job.year, job.month);
    let (threshold_source_month, threshold_sample_trades, p50, p90, thresholds) =
        if let Some((source_year, source_month, trades)) = threshold_source {
            if (source_year, source_month) != expected_source {
                bail!(
                    "{} {:04}-{:02} threshold source is {:04}-{:02}, expected {:04}-{:02}",
                    job.product,
                    job.year,
                    job.month,
                    source_year,
                    source_month,
                    expected_source.0,
                    expected_source.1
                );
            }
            let sample_count = trades.len() as u64;
            let p50 = percentile_in_place(trades, 0.5)?;
            let p90 = percentile_in_place(trades, 0.9)?;
            (
                Some(format!("{source_year:04}-{source_month:02}")),
                sample_count,
                Some(p50),
                Some(p90),
                Some(SizeThresholds::new(p50, p90)?),
            )
        } else {
            (None, 0, None, None, None)
        };
    let mut buckets = scan
        .row_counts
        .iter()
        .map(|&rows| vec![SizeBuckets::default(); rows])
        .collect::<Vec<_>>();
    let mut implied_trades = 0u64;
    for trade in &scan.trades {
        if let Some(thresholds) = thresholds {
            buckets[trade.day_index as usize][trade.row_index as usize].add(
                trade.amount,
                trade.aggressor,
                thresholds,
            )?;
        }
        implied_trades += u64::from(trade.aggressor == AGGRESSOR_IMPLIED);
    }

    let mut rows = 0u64;
    let mut traded_minutes = 0u64;
    for (day, day_buckets) in job.days.iter().zip(&buckets) {
        let mut df = read_parquet(&day.input)?;
        for &name in SIZE_COLUMNS {
            if df.column(name).is_err() {
                bail!("missing size column {name} in {}", day.input.display());
            }
        }
        traded_minutes +=
            replace_size_columns(&mut df, day_buckets, &day.input, thresholds.is_some())?;
        rows += df.height() as u64;
        write_parquet_serialized(parquet_lock, &day.output, df)?;
        let persisted = read_parquet(&day.output)?;
        validate_size_columns(&persisted, day_buckets, &day.output, thresholds.is_some())?;
    }
    let audit = MonthAudit {
        product: job.product.clone(),
        exchange: job.exchange.clone(),
        chicago_trading_month: format!("{:04}-{:02}", job.year, job.month),
        threshold_source_month,
        threshold_method: "previous natural month linear P50/P90 (NumPy default)",
        threshold_sample: "previous Chicago TradDay month printable cme_trade notional only",
        threshold_sample_trades,
        implied_policy: "included in total bucket; excluded from buy/sell sides",
        special_policy: "excluded",
        p50,
        p90,
        files: job.days.len() as u64,
        rows,
        printable_trades: scan.trades.len() as u64,
        implied_trades,
        special_records_excluded: scan.special_records,
        traded_minutes,
    };
    let audit_path = output_root
        .join("_audit")
        .join("size_buckets")
        .join(&job.product)
        .join(format!("{:04}{:02}.json", job.year, job.month));
    write_json_atomic(&audit_path, &audit)?;
    eprintln!(
        "size_month_done product={} month={:04}-{:02} threshold_source={} files={} rows={} trades={} implied={} specials_excluded={} p50={} p90={}",
        job.product,
        job.year,
        job.month,
        audit.threshold_source_month.as_deref().unwrap_or("none"),
        audit.files,
        audit.rows,
        audit.printable_trades,
        audit.implied_trades,
        audit.special_records_excluded,
        audit
            .p50
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        audit
            .p90
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    Ok((audit, scan.trades))
}

fn process_product(
    db: &DB,
    jobs: &[MonthJob],
    multiplier: f64,
    output_root: &Path,
    parquet_lock: &Mutex<()>,
) -> Result<Vec<MonthAudit>> {
    let mut previous: Option<(i32, u32, Vec<TradeRef>)> = None;
    let mut audits = Vec::new();
    for job in jobs {
        if !job.publish {
            let scan = scan_month(db, job, multiplier)?;
            previous = Some((job.year, job.month, scan.trades));
            continue;
        }
        if let Some((source_year, source_month, _)) = &previous {
            let expected = previous_month(job.year, job.month);
            if (*source_year, *source_month) != expected {
                bail!(
                    "{} {:04}-{:02} is missing previous natural month {:04}-{:02}",
                    job.product,
                    job.year,
                    job.month,
                    expected.0,
                    expected.1
                );
            }
        }
        let threshold_source = previous
            .as_mut()
            .map(|(year, month, trades)| (*year, *month, trades));
        let (audit, current_trades) = process_month(
            db,
            job,
            multiplier,
            threshold_source,
            output_root,
            parquet_lock,
        )?;
        audits.push(audit);
        previous = Some((job.year, job.month, current_trades));
    }
    Ok(audits)
}

fn run() -> Result<()> {
    let args = Arc::new(Args::parse());
    let products = validate_args(&args)?;
    let jobs = list_month_jobs(&args, &products)?;
    let multipliers = Arc::new(load_multipliers(&args, &products)?);
    let years = jobs.iter().map(|job| job.year).collect::<BTreeSet<_>>();
    eprintln!("opening TAS {}", args.tas_rocksdb.display());
    let db = Arc::new(open_db(
        &args.tas_rocksdb,
        &args.tas_secondary,
        args.direct_read_only,
    )?);
    for year in years {
        require_done(&db, year)?;
    }
    eprintln!(
        "size_fill_start months={} files={} workers={} input={} output={}",
        jobs.iter().filter(|job| job.publish).count(),
        jobs.iter()
            .filter(|job| job.publish)
            .map(|job| job.days.len())
            .sum::<usize>(),
        args.workers,
        args.input_root.display(),
        args.output_root.display()
    );
    let mut grouped = BTreeMap::<String, Vec<MonthJob>>::new();
    for job in jobs {
        grouped.entry(job.product.clone()).or_default().push(job);
    }
    let product_jobs = grouped.into_values().collect::<Vec<_>>();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .stack_size(16 * 1024 * 1024)
        .build()?;
    let parquet_lock = Arc::new(Mutex::new(()));
    let results = pool.install(|| {
        product_jobs
            .par_iter()
            .map(|jobs| {
                let job = jobs.first().context("empty product job list")?;
                let multiplier = *multipliers
                    .get(&job.product)
                    .with_context(|| format!("missing multiplier for {}", job.product))?;
                process_product(&db, jobs, multiplier, &args.output_root, &parquet_lock)
            })
            .collect::<Vec<_>>()
    });
    let mut files = 0u64;
    let mut rows = 0u64;
    let mut trades = 0u64;
    let mut implied = 0u64;
    let mut specials = 0u64;
    for result in results {
        for audit in result? {
            files += audit.files;
            rows += audit.rows;
            trades += audit.printable_trades;
            implied += audit.implied_trades;
            specials += audit.special_records_excluded;
        }
    }
    eprintln!(
        "size_fill_complete months={} files={} rows={} trades={} implied={} specials_excluded={}",
        product_jobs
            .iter()
            .flat_map(|jobs| jobs.iter())
            .filter(|job| job.publish)
            .count(),
        files,
        rows,
        trades,
        implied,
        specials
    );
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("cme_baseline_fill_size_buckets failed: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::{encode_cme_trade, SlimTrade, MISSING_PRICE, PRICE_SCALE};
    use tempfile::tempdir;

    #[test]
    fn period_mapping_includes_historical_tas_replays() {
        assert_eq!(period_for_year(2017).unwrap(), "2017-01-01_2018-01-01");
        assert_eq!(period_for_year(2019).unwrap(), "2019-01-01_2020-01-01");
        assert!(period_for_year(2016).is_err());
    }

    #[test]
    fn previous_month_crosses_year_boundary() {
        assert_eq!(previous_month(2024, 1), (2023, 12));
        assert_eq!(previous_month(2024, 7), (2024, 6));
    }

    fn fixture_trade(ric: &str, second: u64, price: f64, volume: u32, aggressor: u8) -> SlimTrade {
        SlimTrade {
            ric: ric.to_string(),
            ts_utc_ns: second * NS_PER_SEC,
            exch_hms_ns: MISSING_EXCH_HMS_NS,
            price: (price * PRICE_SCALE as f64) as i64,
            volume,
            bid: MISSING_PRICE,
            bid_size: MISSING_VOLUME,
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            aggressor,
        }
    }

    fn fixture_frame() -> Result<DataFrame> {
        let mut columns = vec![
            Series::new("ric".into(), ["ESH24"]),
            Series::new("ts".into(), [0i64]),
            Series::new("count".into(), [3.0f64]),
            Series::new("amount".into(), [120.0f64]),
            Series::new("buy_amount".into(), [10.0f64]),
            Series::new("sell_amount".into(), [80.0f64]),
            Series::new("implied_amount".into(), [30.0f64]),
            Series::new("special_count".into(), [1.0f64]),
        ];
        for &name in SIZE_COLUMNS {
            columns.push(Series::new(name.into(), [0.0f64]));
        }
        Ok(DataFrame::new(columns)?)
    }

    #[test]
    fn selection_percentile_matches_linear_definition() {
        let mut records = [1.0, 2.0, 3.0, 4.0].map(|amount| TradeRef {
            day_index: 0,
            row_index: 0,
            amount,
            aggressor: AGGRESSOR_BUY,
        });
        assert_eq!(percentile_in_place(&mut records, 0.5).unwrap(), 2.5);
        assert!((percentile_in_place(&mut records, 0.9).unwrap() - 3.7).abs() < 1e-12);
    }

    #[test]
    fn roots_must_differ() {
        let root = PathBuf::from("/tmp/same");
        let args = Args {
            tas_rocksdb: PathBuf::from(DEFAULT_TAS_DB),
            tas_secondary: PathBuf::from(DEFAULT_SECONDARY),
            input_root: root.clone(),
            output_root: root,
            start: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            end: NaiveDate::from_ymd_opt(2020, 2, 1).unwrap(),
            products: vec!["ES".to_string()],
            workers: 1,
            overwrite: false,
            resume: false,
            direct_read_only: true,
            max_months: None,
            psql: PathBuf::from(DEFAULT_PSQL),
            pg_socket: PathBuf::from(DEFAULT_PG_SOCKET),
        };
        assert!(validate_args(&args).is_err());
    }

    #[test]
    fn month_jobs_include_previous_month_as_read_only_warmup() -> Result<()> {
        let temp = tempdir()?;
        let input_root = temp.path().join("input");
        let output_root = temp.path().join("output");
        let product_dir = input_root.join("CME/ES");
        fs::create_dir_all(&product_dir)?;
        for day in ["20231229", "20240102", "20240201"] {
            File::create(product_dir.join(format!("{day}.parquet")))?;
        }
        let args = Args {
            tas_rocksdb: PathBuf::from(DEFAULT_TAS_DB),
            tas_secondary: PathBuf::from(DEFAULT_SECONDARY),
            input_root,
            output_root,
            start: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap(),
            products: vec!["ES".to_string()],
            workers: 1,
            overwrite: false,
            resume: false,
            direct_read_only: true,
            max_months: None,
            psql: PathBuf::from(DEFAULT_PSQL),
            pg_socket: PathBuf::from(DEFAULT_PG_SOCKET),
        };
        let jobs = list_month_jobs(&args, &[product_spec("ES")?])?;
        assert_eq!(jobs.len(), 3);
        assert_eq!(
            jobs.iter()
                .map(|job| (job.year, job.month, job.publish))
                .collect::<Vec<_>>(),
            vec![(2023, 12, false), (2024, 1, true), (2024, 2, true)]
        );
        Ok(())
    }

    #[test]
    fn overlay_includes_implied_and_excludes_special_end_to_end() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("tas");
        let input = temp.path().join("input.parquet");
        let output_root = temp.path().join("output");
        let output = output_root.join("CME/ES/20240102.parquet");
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let cf_name = product_cf_name(2024, "ES")?;
        let db = DB::open_cf_descriptors(
            &options,
            &db_path,
            vec![
                ColumnFamilyDescriptor::new("default", Options::default()),
                ColumnFamilyDescriptor::new(&cf_name, Options::default()),
            ],
        )?;
        let cf = db.cf_handle(&cf_name).context("fixture CF")?;
        for (seq, trade) in [
            fixture_trade("ESH24", 10, 1.0, 10, AGGRESSOR_BUY),
            fixture_trade("ESH24", 20, 1.0, 30, AGGRESSOR_IMPLIED),
            fixture_trade("ESH24", 30, 1.0, 80, AGGRESSOR_SELL),
        ]
        .iter()
        .enumerate()
        {
            db.put_cf(
                &cf,
                encode_all_key(KIND_CME_TRADE, "ESH24", trade.ts_utc_ns, 0, seq as u32)?,
                encode_cme_trade(trade)?,
            )?;
        }
        write_parquet_atomic(&input, fixture_frame()?)?;
        let job = MonthJob {
            product: "ES".to_string(),
            exchange: "CME".to_string(),
            year: 2024,
            month: 1,
            days: vec![DayJob {
                day: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                input,
                output: output.clone(),
            }],
            publish: true,
        };
        let mut previous_trades = [5.0, 15.0, 25.0]
            .map(|amount| TradeRef {
                day_index: 0,
                row_index: 0,
                amount,
                aggressor: AGGRESSOR_BUY,
            })
            .to_vec();
        let (audit, _) = process_month(
            &db,
            &job,
            1.0,
            Some((2023, 12, &mut previous_trades)),
            &output_root,
            &Mutex::new(()),
        )?;
        assert_eq!(audit.printable_trades, 3);
        assert_eq!(audit.implied_trades, 1);
        assert_eq!(audit.special_records_excluded, 1);
        assert_eq!(audit.threshold_source_month.as_deref(), Some("2023-12"));
        assert_eq!(audit.threshold_sample_trades, 3);
        assert_eq!(audit.p50, Some(15.0));
        assert_eq!(audit.p90, Some(23.0));
        let result = read_parquet(&output)?;
        assert_eq!(value_at(&result, "small_order", 0, &output)?, 10.0);
        assert_eq!(value_at(&result, "medium_order", 0, &output)?, 0.0);
        assert_eq!(value_at(&result, "large_order", 0, &output)?, 110.0);
        assert_eq!(value_at(&result, "small_buy", 0, &output)?, 10.0);
        assert_eq!(value_at(&result, "medium_buy", 0, &output)?, 0.0);
        assert_eq!(value_at(&result, "medium_sell", 0, &output)?, 0.0);
        assert_eq!(value_at(&result, "large_sell", 0, &output)?, 80.0);
        Ok(())
    }

    #[test]
    fn first_month_without_warmup_writes_zero_size_columns() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("tas");
        let input = temp.path().join("input.parquet");
        let output_root = temp.path().join("output");
        let output = output_root.join("CME/ES/20170103.parquet");
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let cf_name = product_cf_name(2017, "ES")?;
        let db = DB::open_cf_descriptors(
            &options,
            &db_path,
            vec![
                ColumnFamilyDescriptor::new("default", Options::default()),
                ColumnFamilyDescriptor::new(&cf_name, Options::default()),
            ],
        )?;
        let cf = db.cf_handle(&cf_name).context("fixture CF")?;
        for (seq, trade) in [
            fixture_trade("ESH24", 10, 1.0, 10, AGGRESSOR_BUY),
            fixture_trade("ESH24", 20, 1.0, 30, AGGRESSOR_IMPLIED),
            fixture_trade("ESH24", 30, 1.0, 80, AGGRESSOR_SELL),
        ]
        .iter()
        .enumerate()
        {
            db.put_cf(
                &cf,
                encode_all_key(KIND_CME_TRADE, "ESH24", trade.ts_utc_ns, 0, seq as u32)?,
                encode_cme_trade(trade)?,
            )?;
        }
        write_parquet_atomic(&input, fixture_frame()?)?;
        let job = MonthJob {
            product: "ES".to_string(),
            exchange: "CME".to_string(),
            year: 2017,
            month: 1,
            days: vec![DayJob {
                day: NaiveDate::from_ymd_opt(2017, 1, 3).unwrap(),
                input,
                output: output.clone(),
            }],
            publish: true,
        };
        let (audit, _) = process_month(&db, &job, 1.0, None, &output_root, &Mutex::new(()))?;
        assert_eq!(audit.threshold_source_month, None);
        assert_eq!(audit.p50, None);
        assert_eq!(audit.p90, None);
        let result = read_parquet(&output)?;
        for &name in SIZE_COLUMNS {
            assert_eq!(value_at(&result, name, 0, &output)?, 0.0);
        }
        Ok(())
    }
}
