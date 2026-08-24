//! Build audited CME Group continuous one-minute Depth10 data and factors.
//!
//! This joins fixed-expiry TAS minute bars and direct Normalized LL2 minute
//! snapshots only after PostgreSQL has certified the dominant-map/adjustment
//! chain as continuous.  It never writes back to either fixed-expiry source.

use anyhow::{anyhow, bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use log::info;
use mkt_parsers::msg::trade_flow_feature_msg::TRADE_FLOW_FEATURE_DIM;
use mkt_signal::factor_pub::lseg_features::{
    LsegDepth10, LsegFactorPlan, LsegFeatureState, LsegFusionInput, LsegTradeBar,
};
use polars::prelude::{
    DataFrame, Float64Chunked, Int32Chunked, Int64Chunked, NamedFrom, ParquetReader, ParquetWriter,
    SerReader, Series, StringChunked,
};
use postgres::{Config as PostgresConfig, NoTls};
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEPTH_LEVELS: usize = 10;
const MINUTE_SECONDS: i64 = 60;
const DEFAULT_PG_HOST: &str = "/mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run";
const DEFAULT_PG_PORT: u16 = 5433;
const DEFAULT_PG_USER: &str = "u171";
const DEFAULT_PG_DATABASE: &str = "market_metadata";

const TRADE_VALUE_COLUMNS: [&str; 44] = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "count",
    "special_count",
    "special_volume",
    "avg_amount",
    "vwap",
    "twap",
    "buy_count",
    "sell_count",
    "buy_volume",
    "sell_volume",
    "buy_amount",
    "sell_amount",
    "buy_vwap",
    "sell_vwap",
    "buy_twap",
    "sell_twap",
    "buy_high",
    "sell_low",
    "net_buy_amount",
    "net_buy_volume",
    "net_buy_pct",
    "implied_count",
    "implied_volume",
    "implied_amount",
    "implied_vwap",
    "implied_twap",
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

const TRADE_PRICE_INDICES: [usize; 14] = [0, 1, 2, 3, 10, 11, 18, 19, 20, 21, 22, 23, 30, 31];

// TRADE_FLOW_FEATURE_FIELD_NAMES projected into TRADE_VALUE_COLUMNS.  Keep
// this explicit so the factor hot path never performs per-row name lookup.
const FACTOR_TRADE_INDICES: [usize; TRADE_FLOW_FEATURE_DIM] = [
    0, 1, 2, 3, 4, 5, 9, 6, 12, 13, 16, 17, 14, 15, 32, 33, 34, 35, 36, 37, 38, 39, 40, 10, 18, 19,
    24, 25, 26, 41, 42, 43,
];

#[derive(Parser, Debug)]
#[command(name = "lseg_cme_continuous_1min_replay")]
#[command(about = "Build PG-certified continuous CME Depth10 minute data and LSEG factors")]
struct Args {
    #[arg(long, default_value = "config/lseg_cme_continuous_1min_replay.toml")]
    config: PathBuf,
    #[arg(long)]
    validate_config_only: bool,
    /// Override configured products with one or more EXCHANGE/ROOT values.
    #[arg(long)]
    product: Vec<String>,
    #[arg(long)]
    start_date: Option<String>,
    #[arg(long)]
    end_date: Option<String>,
    #[arg(long)]
    dry_run: bool,
    /// Replace this pipeline's owned continuous and factor outputs.
    #[arg(long)]
    overwrite: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    trade_root: PathBuf,
    depth_root: PathBuf,
    continuous_output_root: PathBuf,
    factor_output_root: PathBuf,
    start_date: String,
    end_date: String,
    #[serde(default)]
    products: Vec<String>,
    factors: Vec<String>,
    #[serde(default = "default_workers")]
    workers: usize,
    #[serde(default)]
    overwrite: bool,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    postgres: PgConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PgConfig {
    #[serde(default = "default_pg_host")]
    host: String,
    #[serde(default = "default_pg_port")]
    port: u16,
    #[serde(default = "default_pg_user")]
    user: String,
    #[serde(default = "default_pg_database")]
    database: String,
    #[serde(default = "default_pg_timeout")]
    connect_timeout_secs: u64,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            host: default_pg_host(),
            port: default_pg_port(),
            user: default_pg_user(),
            database: default_pg_database(),
            connect_timeout_secs: default_pg_timeout(),
        }
    }
}

#[derive(Debug)]
struct ValidatedConfig {
    start_date: NaiveDate,
    end_date: NaiveDate,
    products: HashSet<ProductKey>,
    factor_plan: LsegFactorPlan,
    factor_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ProductKey {
    exchange: String,
    product_root: String,
}

impl ProductKey {
    fn continuous_id(&self) -> String {
        format!("{}:{}", self.exchange, self.product_root)
    }

    fn output_relative_path(&self, trading_day: NaiveDate) -> PathBuf {
        PathBuf::from(&self.exchange)
            .join(&self.product_root)
            .join(format!("{}.parquet", trading_day.format("%Y%m%d")))
    }
}

#[derive(Debug, Clone)]
struct DominantDay {
    contract_id: Option<String>,
    symbol: Option<String>,
    price_adjustment: f64,
}

#[derive(Debug, Clone)]
struct DayJob {
    trading_day: NaiveDate,
    trade_path: PathBuf,
    depth_path: Option<PathBuf>,
    dominant: Option<DominantDay>,
}

#[derive(Debug, Clone)]
struct ProductJob {
    key: ProductKey,
    days: Vec<DayJob>,
}

#[derive(Debug)]
struct TradeRow {
    contract_id: String,
    ric: String,
    ts: i64,
    values: [Option<f64>; TRADE_VALUE_COLUMNS.len()],
}

#[derive(Debug)]
struct DepthRow {
    ts: i64,
    source_ts_utc_ns: i64,
    update_count: i64,
    bid_prices: [Option<f64>; DEPTH_LEVELS],
    bid_sizes: [Option<f64>; DEPTH_LEVELS],
    bid_counts: [i32; DEPTH_LEVELS],
    ask_prices: [Option<f64>; DEPTH_LEVELS],
    ask_sizes: [Option<f64>; DEPTH_LEVELS],
    ask_counts: [i32; DEPTH_LEVELS],
}

#[derive(Debug)]
struct ContinuousRow {
    continuous_id: String,
    exchange: String,
    product_root: String,
    source_contract_id: String,
    source_ric: String,
    ts: i64,
    price_adjustment: f64,
    source_depth_ts_utc_ns: i64,
    source_depth_update_count: i64,
    trade_values: [Option<f64>; TRADE_VALUE_COLUMNS.len()],
    bid_prices: [Option<f64>; DEPTH_LEVELS],
    bid_sizes: [Option<f64>; DEPTH_LEVELS],
    bid_counts: [i32; DEPTH_LEVELS],
    ask_prices: [Option<f64>; DEPTH_LEVELS],
    ask_sizes: [Option<f64>; DEPTH_LEVELS],
    ask_counts: [i32; DEPTH_LEVELS],
}

#[derive(Debug)]
struct FactorRow {
    continuous_id: String,
    source_contract_id: String,
    source_ric: String,
    ts: i64,
    price_adjustment: f64,
    source_depth_ts_utc_ns: i64,
    factors: Vec<Option<f64>>,
}

#[derive(Default)]
struct ContinuousFeatureState {
    features: LsegFeatureState,
    last_ts: Option<i64>,
}

#[derive(Debug, Default, Clone)]
struct DayManifest {
    exchange: String,
    product_root: String,
    trading_day: NaiveDate,
    source_contract_id: Option<String>,
    source_ric: Option<String>,
    status: String,
    trade_rows: u64,
    written_rows: u64,
    missing_depth_rows: u64,
    invalid_depth_rows: u64,
    off_session_rows: u64,
}

#[derive(Debug, Default)]
struct ProductResult {
    continuous_rows: u64,
    factor_rows: u64,
    manifests: Vec<DayManifest>,
}

fn default_workers() -> usize {
    8
}

fn default_pg_host() -> String {
    DEFAULT_PG_HOST.to_string()
}

fn default_pg_port() -> u16 {
    DEFAULT_PG_PORT
}

fn default_pg_user() -> String {
    DEFAULT_PG_USER.to_string()
}

fn default_pg_database() -> String {
    DEFAULT_PG_DATABASE.to_string()
}

fn default_pg_timeout() -> u64 {
    10
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let text = fs::read_to_string(&args.config)
        .with_context(|| format!("read {}", args.config.display()))?;
    let mut config: Config =
        toml::from_str(&text).with_context(|| format!("parse {}", args.config.display()))?;
    if !args.product.is_empty() {
        config.products = args.product;
    }
    if let Some(start_date) = args.start_date {
        config.start_date = start_date;
    }
    if let Some(end_date) = args.end_date {
        config.end_date = end_date;
    }
    if args.dry_run {
        config.dry_run = true;
    }
    if args.overwrite {
        config.overwrite = true;
    }
    let validated = validate_config(&config)?;
    if args.validate_config_only {
        println!(
            "validated CME continuous 1min config: dates={}..{} requested_products={} factors={} workers={} dry_run={}",
            validated.start_date,
            validated.end_date,
            validated.products.len(),
            validated.factor_names.len(),
            config.workers,
            config.dry_run,
        );
        return Ok(());
    }
    replay(&config, &validated)
}

fn validate_config(config: &Config) -> Result<ValidatedConfig> {
    for (name, path) in [
        ("trade_root", &config.trade_root),
        ("depth_root", &config.depth_root),
    ] {
        if !path.is_dir() {
            bail!("{name} is not a directory: {}", path.display());
        }
    }
    let start_date = parse_date(&config.start_date, "start_date")?;
    let end_date = parse_date(&config.end_date, "end_date")?;
    if start_date > end_date {
        bail!("start_date must not be after end_date");
    }
    if config.workers == 0 {
        bail!("workers must be > 0");
    }
    if config.postgres.connect_timeout_secs == 0 {
        bail!("postgres.connect_timeout_secs must be > 0");
    }
    let products = config
        .products
        .iter()
        .map(|value| parse_product(value))
        .collect::<Result<HashSet<_>>>()?;
    let factor_plan = LsegFactorPlan::from_factor_names(config.factors.clone())?;
    let factor_names = factor_plan.factor_names().map(ToOwned::to_owned).collect();
    Ok(ValidatedConfig {
        start_date,
        end_date,
        products,
        factor_plan,
        factor_names,
    })
}

fn parse_date(value: &str, field: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("{field} must be YYYY-MM-DD, got {value:?}"))
}

fn parse_product(value: &str) -> Result<ProductKey> {
    let normalized = value.trim().to_ascii_uppercase();
    let mut parts = normalized.split('/');
    let exchange = parts.next().unwrap_or_default();
    let product_root = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || exchange.is_empty()
        || product_root.is_empty()
        || !valid_component(exchange)
        || !valid_component(product_root)
    {
        bail!("product must be EXCHANGE/ROOT with ASCII letters or digits: {value:?}");
    }
    Ok(ProductKey {
        exchange: exchange.to_string(),
        product_root: product_root.to_string(),
    })
}

fn valid_component(value: &str) -> bool {
    value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn replay(config: &Config, validated: &ValidatedConfig) -> Result<()> {
    let dominant_days = load_dominant_days(config, validated)?;
    if dominant_days.is_empty() {
        bail!("PostgreSQL returned no certified continuous CME roots");
    }
    let sessions = load_trading_intervals(config, validated)?;
    let jobs = discover_jobs(config, validated, &dominant_days)?;
    if jobs.is_empty() {
        bail!(
            "no fixed-expiry minute trade files in {} for {}..{} and the PG-certified roots",
            config.trade_root.display(),
            validated.start_date,
            validated.end_date
        );
    }
    preflight_outputs(config, &jobs)?;
    info!(
        "starting CME continuous 1min replay: products={} days={} factors={} workers={} trade_root={} depth_root={} continuous_output_root={} factor_output_root={} dry_run={}",
        jobs.len(),
        jobs.iter().map(|job| job.days.len()).sum::<usize>(),
        validated.factor_names.len(),
        config.workers,
        config.trade_root.display(),
        config.depth_root.display(),
        config.continuous_output_root.display(),
        config.factor_output_root.display(),
        config.dry_run,
    );
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(config.workers)
        .build()
        .context("build CME continuous replay worker pool")?;
    let results = pool.install(|| {
        jobs.par_iter()
            .map(|job| process_product(config, validated, &sessions, job))
            .collect::<Result<Vec<_>>>()
    })?;
    let mut total = ProductResult::default();
    for result in results {
        total.continuous_rows = total.continuous_rows.saturating_add(result.continuous_rows);
        total.factor_rows = total.factor_rows.saturating_add(result.factor_rows);
        total.manifests.extend(result.manifests);
    }
    total.manifests.sort_by(|left, right| {
        left.exchange
            .cmp(&right.exchange)
            .then(left.product_root.cmp(&right.product_root))
            .then(left.trading_day.cmp(&right.trading_day))
    });
    if !config.dry_run {
        write_manifest(
            &config.continuous_output_root,
            &total.manifests,
            config.overwrite,
        )?;
    }
    let written_days = total
        .manifests
        .iter()
        .filter(|row| row.status == "written")
        .count();
    info!(
        "CME continuous 1min replay complete: continuous_rows={} factor_rows={} written_days={} manifest_days={}",
        total.continuous_rows,
        total.factor_rows,
        written_days,
        total.manifests.len(),
    );
    Ok(())
}

fn pg_client(config: &Config) -> Result<postgres::Client> {
    let mut pg = PostgresConfig::new();
    pg.host(&config.postgres.host)
        .port(config.postgres.port)
        .user(&config.postgres.user)
        .dbname(&config.postgres.database)
        .options("-c default_transaction_read_only=on")
        .connect_timeout(Duration::from_secs(config.postgres.connect_timeout_secs));
    pg.connect(NoTls).with_context(|| {
        format!(
            "connect PostgreSQL host={} port={} database={} user={}",
            config.postgres.host,
            config.postgres.port,
            config.postgres.database,
            config.postgres.user
        )
    })
}

fn load_dominant_days(
    config: &Config,
    validated: &ValidatedConfig,
) -> Result<BTreeMap<ProductKey, BTreeMap<NaiveDate, DominantDay>>> {
    let query = "
        SELECT d.exchange, d.product_root, d.trading_day, d.contract_id, d.symbol,
               COALESCE((
                   SELECT a.cumulative_factor
                   FROM public.adjustment_factor_cme_tas a
                   WHERE a.product_root = d.product_root
                     AND NOT a.skipped
                     AND a.cumulative_complete
                     AND a.effective_trading_day > d.trading_day
                   ORDER BY a.effective_trading_day ASC
                   LIMIT 1
               ), 0.0) AS price_adjustment
        FROM public.dominant_cme_tas d
        JOIN public.cme_tas_continuity_status s
          ON s.exchange = d.exchange AND s.product_root = d.product_root
        WHERE s.continuous
          AND d.trading_day >= $1
          AND d.trading_day <= $2
        ORDER BY d.exchange, d.product_root, d.trading_day
    ";
    let mut client = pg_client(config)?;
    let rows = client
        .query(query, &[&validated.start_date, &validated.end_date])
        .context("query certified CME dominant/adjustment rows")?;
    let mut output: BTreeMap<ProductKey, BTreeMap<NaiveDate, DominantDay>> = BTreeMap::new();
    for row in rows {
        let key = ProductKey {
            exchange: row.get::<_, String>(0),
            product_root: row.get::<_, String>(1),
        };
        if !validated.products.is_empty() && !validated.products.contains(&key) {
            continue;
        }
        let day: NaiveDate = row.get(2);
        let adjustment: f64 = row.get(5);
        if !adjustment.is_finite() {
            bail!(
                "non-finite cumulative adjustment for {}/{} {day}",
                key.exchange,
                key.product_root
            );
        }
        let inserted = output.entry(key.clone()).or_default().insert(
            day,
            DominantDay {
                contract_id: row.get(3),
                symbol: row.get(4),
                price_adjustment: adjustment,
            },
        );
        if inserted.is_some() {
            bail!(
                "duplicate PG dominant day for {}/{} {day}",
                key.exchange,
                key.product_root
            );
        }
    }
    Ok(output)
}

fn load_trading_intervals(
    config: &Config,
    validated: &ValidatedConfig,
) -> Result<BTreeMap<ProductKey, Vec<(i64, i64)>>> {
    let query = "
        SELECT exchange, product_root,
               EXTRACT(EPOCH FROM open_utc)::bigint,
               EXTRACT(EPOCH FROM close_utc)::bigint
        FROM public.cme_globex_product_trading_intervals
        WHERE is_trading
          AND close_utc >= ($1::date - INTERVAL '2 days')
          AND open_utc < ($2::date + INTERVAL '3 days')
        ORDER BY exchange, product_root, open_utc, close_utc
    ";
    let mut client = pg_client(config)?;
    let rows = client
        .query(query, &[&validated.start_date, &validated.end_date])
        .context("query CME product trading intervals")?;
    let mut output: BTreeMap<ProductKey, Vec<(i64, i64)>> = BTreeMap::new();
    for row in rows {
        let key = ProductKey {
            exchange: row.get::<_, String>(0),
            product_root: row.get::<_, String>(1),
        };
        if !validated.products.is_empty() && !validated.products.contains(&key) {
            continue;
        }
        let open: i64 = row.get(2);
        let close: i64 = row.get(3);
        if close <= open {
            bail!(
                "invalid PG trading interval for {}/{} [{open}, {close})",
                key.exchange,
                key.product_root
            );
        }
        output.entry(key).or_default().push((open, close));
    }
    for (key, intervals) in &mut output {
        intervals.sort_unstable();
        if intervals.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            bail!(
                "overlapping PG trading intervals for {}/{}",
                key.exchange,
                key.product_root
            );
        }
    }
    Ok(output)
}

fn discover_jobs(
    config: &Config,
    validated: &ValidatedConfig,
    dominants: &BTreeMap<ProductKey, BTreeMap<NaiveDate, DominantDay>>,
) -> Result<Vec<ProductJob>> {
    let mut jobs = Vec::new();
    for exchange_path in sorted_dirs(&config.trade_root)? {
        let exchange = file_name(&exchange_path)?;
        for product_path in sorted_dirs(&exchange_path)? {
            let key = ProductKey {
                exchange: exchange.clone(),
                product_root: file_name(&product_path)?,
            };
            if !dominants.contains_key(&key) {
                continue;
            }
            let days = sorted_parquet_files(&product_path)?
                .into_iter()
                .filter_map(|trade_path| {
                    let trading_day = match path_trading_day(&trade_path) {
                        Ok(day) => day,
                        Err(error) => return Some(Err(error)),
                    };
                    if trading_day < validated.start_date || trading_day > validated.end_date {
                        return None;
                    }
                    let depth_path = config
                        .depth_root
                        .join(key.output_relative_path(trading_day));
                    Some(Ok(DayJob {
                        trading_day,
                        trade_path,
                        depth_path: depth_path.is_file().then_some(depth_path),
                        dominant: dominants
                            .get(&key)
                            .and_then(|days| days.get(&trading_day))
                            .cloned(),
                    }))
                })
                .collect::<Result<Vec<_>>>()?;
            if !days.is_empty() {
                jobs.push(ProductJob { key, days });
            }
        }
    }
    jobs.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(jobs)
}

fn preflight_outputs(config: &Config, jobs: &[ProductJob]) -> Result<()> {
    if config.dry_run || config.overwrite {
        return Ok(());
    }
    for product in jobs {
        for day in &product.days {
            let Some(dominant) = &day.dominant else {
                continue;
            };
            if dominant.contract_id.is_none() || day.depth_path.is_none() {
                continue;
            }
            let relative = product.key.output_relative_path(day.trading_day);
            for output in [
                config.continuous_output_root.join(&relative),
                config.factor_output_root.join(&relative),
            ] {
                if output.exists() {
                    bail!("output exists and overwrite=false: {}", output.display());
                }
            }
        }
    }
    let manifest = config.continuous_output_root.join("coverage_manifest.csv");
    if manifest.exists() {
        bail!("output exists and overwrite=false: {}", manifest.display());
    }
    Ok(())
}

fn process_product(
    config: &Config,
    validated: &ValidatedConfig,
    sessions: &BTreeMap<ProductKey, Vec<(i64, i64)>>,
    product: &ProductJob,
) -> Result<ProductResult> {
    let intervals = sessions.get(&product.key).ok_or_else(|| {
        anyhow!(
            "no PG trading intervals for certified root {}/{}",
            product.key.exchange,
            product.key.product_root
        )
    })?;
    let mut state = ContinuousFeatureState::default();
    let mut result = ProductResult::default();
    for day in &product.days {
        let mut manifest = DayManifest {
            exchange: product.key.exchange.clone(),
            product_root: product.key.product_root.clone(),
            trading_day: day.trading_day,
            ..DayManifest::default()
        };
        let Some(dominant) = &day.dominant else {
            manifest.status = "missing_pg_dominant".to_string();
            result.manifests.push(manifest);
            continue;
        };
        manifest.source_contract_id = dominant.contract_id.clone();
        manifest.source_ric = dominant.symbol.clone();
        let (Some(contract_id), Some(ric)) = (&dominant.contract_id, &dominant.symbol) else {
            manifest.status = "unassigned_pg_dominant".to_string();
            result.manifests.push(manifest);
            continue;
        };
        let Some(depth_path) = &day.depth_path else {
            manifest.status = "missing_level2_1min_day".to_string();
            result.manifests.push(manifest);
            continue;
        };
        let (continuous, factors, processed_manifest) = process_day(
            &product.key,
            intervals,
            contract_id,
            ric,
            dominant.price_adjustment,
            &day.trade_path,
            depth_path,
            &mut state,
            validated,
            manifest,
        )?;
        if continuous.is_empty() {
            result.manifests.push(processed_manifest);
            continue;
        }
        let relative = product.key.output_relative_path(day.trading_day);
        if !config.dry_run {
            write_continuous(
                &config.continuous_output_root.join(&relative),
                &continuous,
                config.overwrite,
            )?;
            write_factors(
                &config.factor_output_root.join(&relative),
                &factors,
                &validated.factor_names,
                config.overwrite,
            )?;
        }
        result.continuous_rows = result
            .continuous_rows
            .saturating_add(continuous.len() as u64);
        result.factor_rows = result.factor_rows.saturating_add(factors.len() as u64);
        result.manifests.push(processed_manifest);
    }
    info!(
        "continuous root complete: {}/{} continuous_rows={} factor_rows={}",
        product.key.exchange, product.key.product_root, result.continuous_rows, result.factor_rows
    );
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn process_day(
    key: &ProductKey,
    intervals: &[(i64, i64)],
    contract_id: &str,
    ric: &str,
    price_adjustment: f64,
    trade_path: &Path,
    depth_path: &Path,
    state: &mut ContinuousFeatureState,
    validated: &ValidatedConfig,
    mut manifest: DayManifest,
) -> Result<(Vec<ContinuousRow>, Vec<FactorRow>, DayManifest)> {
    let trades = read_trade_rows(trade_path, contract_id, ric)?;
    manifest.trade_rows = trades.len() as u64;
    if trades.is_empty() {
        manifest.status = "missing_dominant_trade_rows".to_string();
        return Ok((Vec::new(), Vec::new(), manifest));
    }
    let depths = read_depth_rows(depth_path, contract_id, ric)?;
    let mut continuous = Vec::with_capacity(trades.len());
    for trade in trades {
        if !in_trading_interval(intervals, trade.ts) {
            manifest.off_session_rows = manifest.off_session_rows.saturating_add(1);
            continue;
        }
        let Some(depth) = depths.get(&trade.ts) else {
            manifest.missing_depth_rows = manifest.missing_depth_rows.saturating_add(1);
            continue;
        };
        if depth.ts != trade.ts {
            bail!("depth map key does not match source depth timestamp");
        }
        if !valid_best_depth(depth) {
            manifest.invalid_depth_rows = manifest.invalid_depth_rows.saturating_add(1);
            continue;
        }
        let row = make_continuous_row(key, trade, depth, price_adjustment)?;
        if !valid_best_depth_values(&row) {
            bail!(
                "price adjustment produced invalid BBO for {}/{} contract={} ts={}",
                key.exchange,
                key.product_root,
                contract_id,
                row.ts
            );
        }
        continuous.push(row);
    }
    if continuous.is_empty() {
        manifest.status = "no_valid_same_minute_bbo".to_string();
        return Ok((continuous, Vec::new(), manifest));
    }
    continuous.sort_by_key(|row| row.ts);
    if continuous.windows(2).any(|pair| pair[0].ts == pair[1].ts) {
        bail!(
            "duplicate continuous minute for {}/{} {}",
            key.exchange,
            key.product_root,
            trade_path.display()
        );
    }
    let factors = compute_factors(&continuous, state, validated)?;
    if factors.len() != continuous.len() {
        bail!("continuous factor output length does not match continuous rows");
    }
    manifest.status = "written".to_string();
    manifest.written_rows = continuous.len() as u64;
    Ok((continuous, factors, manifest))
}

fn in_trading_interval(intervals: &[(i64, i64)], ts: i64) -> bool {
    let index = intervals.partition_point(|(open, _)| *open <= ts);
    index > 0 && ts < intervals[index - 1].1
}

fn read_trade_rows(path: &Path, contract_id: &str, ric: &str) -> Result<Vec<TradeRow>> {
    let mut columns = vec![
        "contract_id".to_string(),
        "ric".to_string(),
        "ts".to_string(),
    ];
    columns.extend(TRADE_VALUE_COLUMNS.iter().map(|name| (*name).to_string()));
    let df = read_parquet(path, columns)?;
    let contracts = string_column(&df, "contract_id")?;
    let rics = string_column(&df, "ric")?;
    let ts = i64_column(&df, "ts")?;
    let values = TRADE_VALUE_COLUMNS
        .iter()
        .map(|name| f64_column(&df, name))
        .collect::<Result<Vec<_>>>()?;
    let mut rows = Vec::new();
    for index in 0..df.height() {
        let current_contract = required_str(contracts, index, "contract_id", path)?;
        if current_contract != contract_id {
            continue;
        }
        let current_ric = required_str(rics, index, "ric", path)?;
        if current_ric != ric {
            bail!(
                "trade contract/ric mismatch in {}: contract_id={} expected_ric={} actual_ric={}",
                path.display(),
                contract_id,
                ric,
                current_ric
            );
        }
        let minute = required_i64(ts, index, "ts", path)?;
        if minute.rem_euclid(MINUTE_SECONDS) != 0 {
            bail!(
                "trade ts is not a minute boundary: {minute} in {}",
                path.display()
            );
        }
        rows.push(TradeRow {
            contract_id: current_contract.to_string(),
            ric: current_ric.to_string(),
            ts: minute,
            values: std::array::from_fn(|field| values[field].get(index)),
        });
    }
    rows.sort_by_key(|row| row.ts);
    if rows.windows(2).any(|pair| pair[0].ts == pair[1].ts) {
        bail!(
            "duplicate trade minute for contract_id={} in {}",
            contract_id,
            path.display()
        );
    }
    Ok(rows)
}

fn read_depth_rows(path: &Path, contract_id: &str, ric: &str) -> Result<BTreeMap<i64, DepthRow>> {
    let mut columns = vec![
        "contract_id".to_string(),
        "ric".to_string(),
        "ts".to_string(),
        "source_ts_utc_ns".to_string(),
        "update_count".to_string(),
    ];
    for side in ["bid", "ask"] {
        for level in 0..DEPTH_LEVELS {
            columns.push(format!("{side}{level}p"));
            columns.push(format!("{side}{level}v"));
            columns.push(format!("{side}{level}n"));
        }
    }
    let df = read_parquet(path, columns)?;
    let contracts = string_column(&df, "contract_id")?;
    let rics = string_column(&df, "ric")?;
    let ts = i64_column(&df, "ts")?;
    let source_ts = i64_column(&df, "source_ts_utc_ns")?;
    let update_count = i64_column(&df, "update_count")?;
    let bid_prices = depth_f64_columns(&df, "bid", "p")?;
    let bid_sizes = depth_f64_columns(&df, "bid", "v")?;
    let bid_counts = depth_i32_columns(&df, "bid", "n")?;
    let ask_prices = depth_f64_columns(&df, "ask", "p")?;
    let ask_sizes = depth_f64_columns(&df, "ask", "v")?;
    let ask_counts = depth_i32_columns(&df, "ask", "n")?;
    let mut rows = BTreeMap::new();
    for index in 0..df.height() {
        let current_contract = required_str(contracts, index, "contract_id", path)?;
        if current_contract != contract_id {
            continue;
        }
        let current_ric = required_str(rics, index, "ric", path)?;
        if current_ric != ric {
            bail!(
                "depth contract/ric mismatch in {}: contract_id={} expected_ric={} actual_ric={}",
                path.display(),
                contract_id,
                ric,
                current_ric
            );
        }
        let minute = required_i64(ts, index, "ts", path)?;
        let source = required_i64(source_ts, index, "source_ts_utc_ns", path)?;
        if source / 1_000_000_000 < minute || source / 1_000_000_000 >= minute + MINUTE_SECONDS {
            bail!(
                "depth source timestamp is outside its minute in {}",
                path.display()
            );
        }
        let updates = required_i64(update_count, index, "update_count", path)?;
        if updates <= 0 {
            bail!("depth update_count must be positive in {}", path.display());
        }
        let row = DepthRow {
            ts: minute,
            source_ts_utc_ns: source,
            update_count: updates,
            bid_prices: std::array::from_fn(|level| bid_prices[level].get(index)),
            bid_sizes: std::array::from_fn(|level| bid_sizes[level].get(index)),
            bid_counts: std::array::from_fn(|level| bid_counts[level].get(index).unwrap_or(0)),
            ask_prices: std::array::from_fn(|level| ask_prices[level].get(index)),
            ask_sizes: std::array::from_fn(|level| ask_sizes[level].get(index)),
            ask_counts: std::array::from_fn(|level| ask_counts[level].get(index).unwrap_or(0)),
        };
        if rows.insert(minute, row).is_some() {
            bail!(
                "duplicate depth minute for contract_id={} in {}",
                contract_id,
                path.display()
            );
        }
    }
    Ok(rows)
}

fn depth_f64_columns<'a>(
    df: &'a DataFrame,
    side: &str,
    suffix: &str,
) -> Result<Vec<&'a Float64Chunked>> {
    (0..DEPTH_LEVELS)
        .map(|level| f64_column(df, &format!("{side}{level}{suffix}")))
        .collect()
}

fn depth_i32_columns<'a>(
    df: &'a DataFrame,
    side: &str,
    suffix: &str,
) -> Result<Vec<&'a Int32Chunked>> {
    (0..DEPTH_LEVELS)
        .map(|level| i32_column(df, &format!("{side}{level}{suffix}")))
        .collect()
}

fn valid_best_depth(row: &DepthRow) -> bool {
    valid_best_arrays(
        row.bid_prices[0],
        row.bid_sizes[0],
        row.ask_prices[0],
        row.ask_sizes[0],
    )
}

fn valid_best_depth_values(row: &ContinuousRow) -> bool {
    valid_best_arrays(
        row.bid_prices[0],
        row.bid_sizes[0],
        row.ask_prices[0],
        row.ask_sizes[0],
    )
}

fn valid_best_arrays(
    bid_price: Option<f64>,
    bid_size: Option<f64>,
    ask_price: Option<f64>,
    ask_size: Option<f64>,
) -> bool {
    [bid_price, bid_size, ask_price, ask_size]
        .into_iter()
        .all(|value| value.is_some_and(|value| value.is_finite() && value >= 0.0))
        && bid_price.unwrap() <= ask_price.unwrap()
}

fn make_continuous_row(
    key: &ProductKey,
    trade: TradeRow,
    depth: &DepthRow,
    price_adjustment: f64,
) -> Result<ContinuousRow> {
    if !price_adjustment.is_finite() {
        bail!(
            "non-finite price adjustment for {}/{}",
            key.exchange,
            key.product_root
        );
    }
    let mut trade_values = trade.values;
    for index in TRADE_PRICE_INDICES {
        if let Some(value) = trade_values[index] {
            trade_values[index] = Some(value + price_adjustment);
        }
    }
    let bid_prices = std::array::from_fn(|level| {
        depth.bid_prices[level]
            .map(|value| value + price_adjustment)
            .filter(|value| value.is_finite() && *value >= 0.0)
    });
    let ask_prices = std::array::from_fn(|level| {
        depth.ask_prices[level]
            .map(|value| value + price_adjustment)
            .filter(|value| value.is_finite() && *value >= 0.0)
    });
    let bid_sizes = std::array::from_fn(|level| {
        depth.bid_sizes[level].filter(|value| value.is_finite() && *value >= 0.0)
    });
    let ask_sizes = std::array::from_fn(|level| {
        depth.ask_sizes[level].filter(|value| value.is_finite() && *value >= 0.0)
    });
    Ok(ContinuousRow {
        continuous_id: key.continuous_id(),
        exchange: key.exchange.clone(),
        product_root: key.product_root.clone(),
        source_contract_id: trade.contract_id,
        source_ric: trade.ric,
        ts: trade.ts,
        price_adjustment,
        source_depth_ts_utc_ns: depth.source_ts_utc_ns,
        source_depth_update_count: depth.update_count,
        trade_values,
        bid_prices,
        bid_sizes,
        bid_counts: depth.bid_counts,
        ask_prices,
        ask_sizes,
        ask_counts: depth.ask_counts,
    })
}

fn compute_factors(
    rows: &[ContinuousRow],
    state: &mut ContinuousFeatureState,
    validated: &ValidatedConfig,
) -> Result<Vec<FactorRow>> {
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        let segment_break = state
            .last_ts
            .is_some_and(|previous| row.ts != previous + MINUTE_SECONDS);
        let bid_prices: [f64; DEPTH_LEVELS] =
            std::array::from_fn(|level| row.bid_prices[level].unwrap_or(f64::NAN));
        let bid_sizes: [f64; DEPTH_LEVELS] =
            std::array::from_fn(|level| row.bid_sizes[level].unwrap_or(f64::NAN));
        let ask_prices: [f64; DEPTH_LEVELS] =
            std::array::from_fn(|level| row.ask_prices[level].unwrap_or(f64::NAN));
        let ask_sizes: [f64; DEPTH_LEVELS] =
            std::array::from_fn(|level| row.ask_sizes[level].unwrap_or(f64::NAN));
        let depth = LsegDepth10::from_slices(&bid_prices, &bid_sizes, &ask_prices, &ask_sizes)
            .context("build continuous native Depth10")?;
        let values: [f64; TRADE_FLOW_FEATURE_DIM] = std::array::from_fn(|field| {
            row.trade_values[FACTOR_TRADE_INDICES[field]].unwrap_or(f64::NAN)
        });
        let ts_ms = row
            .ts
            .checked_mul(1_000)
            .ok_or_else(|| anyhow!("continuous timestamp overflows milliseconds: {}", row.ts))?;
        state.features.push(LsegFusionInput {
            ts_ms,
            symbol: row.continuous_id.clone(),
            trade: LsegTradeBar::from_slice(&values).context("build continuous trade-flow bar")?,
            depth,
            segment_break,
        })?;
        state.last_ts = Some(row.ts);
        output.push(FactorRow {
            continuous_id: row.continuous_id.clone(),
            source_contract_id: row.source_contract_id.clone(),
            source_ric: row.source_ric.clone(),
            ts: row.ts,
            price_adjustment: row.price_adjustment,
            source_depth_ts_utc_ns: row.source_depth_ts_utc_ns,
            factors: state.features.factor_values(&validated.factor_plan)?,
        });
    }
    Ok(output)
}

fn write_continuous(path: &Path, rows: &[ContinuousRow], overwrite: bool) -> Result<()> {
    if path.exists() && !overwrite {
        bail!("output exists and overwrite=false: {}", path.display());
    }
    let mut df = continuous_dataframe(rows)?;
    write_dataframe(path, &mut df)
}

fn write_factors(
    path: &Path,
    rows: &[FactorRow],
    factor_names: &[String],
    overwrite: bool,
) -> Result<()> {
    if path.exists() && !overwrite {
        bail!("output exists and overwrite=false: {}", path.display());
    }
    let mut df = factor_dataframe(rows, factor_names)?;
    write_dataframe(path, &mut df)
}

fn write_dataframe(path: &Path, df: &mut DataFrame) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("output has no parent: {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let temporary = path.with_extension("parquet.tmp");
    let file =
        File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?;
    ParquetWriter::new(file)
        .finish(df)
        .with_context(|| format!("write {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("rename {} -> {}", temporary.display(), path.display()))?;
    Ok(())
}

fn continuous_dataframe(rows: &[ContinuousRow]) -> Result<DataFrame> {
    let n = rows.len();
    let mut continuous_id = Vec::with_capacity(n);
    let mut exchange = Vec::with_capacity(n);
    let mut product_root = Vec::with_capacity(n);
    let mut source_contract_id = Vec::with_capacity(n);
    let mut source_ric = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    let mut price_adjustment = Vec::with_capacity(n);
    let mut source_depth_ts = Vec::with_capacity(n);
    let mut source_depth_updates = Vec::with_capacity(n);
    let mut trade_columns: Vec<Vec<Option<f64>>> = (0..TRADE_VALUE_COLUMNS.len())
        .map(|_| Vec::with_capacity(n))
        .collect();
    let mut bid_prices: Vec<Vec<Option<f64>>> =
        (0..DEPTH_LEVELS).map(|_| Vec::with_capacity(n)).collect();
    let mut bid_sizes: Vec<Vec<Option<f64>>> =
        (0..DEPTH_LEVELS).map(|_| Vec::with_capacity(n)).collect();
    let mut bid_counts: Vec<Vec<i32>> = (0..DEPTH_LEVELS).map(|_| Vec::with_capacity(n)).collect();
    let mut ask_prices: Vec<Vec<Option<f64>>> =
        (0..DEPTH_LEVELS).map(|_| Vec::with_capacity(n)).collect();
    let mut ask_sizes: Vec<Vec<Option<f64>>> =
        (0..DEPTH_LEVELS).map(|_| Vec::with_capacity(n)).collect();
    let mut ask_counts: Vec<Vec<i32>> = (0..DEPTH_LEVELS).map(|_| Vec::with_capacity(n)).collect();
    for row in rows {
        continuous_id.push(row.continuous_id.clone());
        exchange.push(row.exchange.clone());
        product_root.push(row.product_root.clone());
        source_contract_id.push(row.source_contract_id.clone());
        source_ric.push(row.source_ric.clone());
        ts.push(row.ts);
        price_adjustment.push(row.price_adjustment);
        source_depth_ts.push(row.source_depth_ts_utc_ns);
        source_depth_updates.push(row.source_depth_update_count);
        for field in 0..TRADE_VALUE_COLUMNS.len() {
            trade_columns[field].push(row.trade_values[field]);
        }
        for level in 0..DEPTH_LEVELS {
            bid_prices[level].push(row.bid_prices[level]);
            bid_sizes[level].push(row.bid_sizes[level]);
            bid_counts[level].push(row.bid_counts[level]);
            ask_prices[level].push(row.ask_prices[level]);
            ask_sizes[level].push(row.ask_sizes[level]);
            ask_counts[level].push(row.ask_counts[level]);
        }
    }
    let mut columns = vec![
        Series::new("continuous_id".into(), continuous_id),
        Series::new("exchange".into(), exchange),
        Series::new("product_root".into(), product_root),
        Series::new("source_contract_id".into(), source_contract_id),
        Series::new("source_ric".into(), source_ric),
        Series::new("ts".into(), ts),
        Series::new("price_adjustment".into(), price_adjustment),
        Series::new("source_depth_ts_utc_ns".into(), source_depth_ts),
        Series::new("source_depth_update_count".into(), source_depth_updates),
    ];
    for (index, name) in TRADE_VALUE_COLUMNS.iter().enumerate() {
        columns.push(Series::new(
            (*name).into(),
            std::mem::take(&mut trade_columns[index]),
        ));
    }
    for level in 0..DEPTH_LEVELS {
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
    DataFrame::new(columns).context("build continuous minute dataframe")
}

fn factor_dataframe(rows: &[FactorRow], factor_names: &[String]) -> Result<DataFrame> {
    let n = rows.len();
    let mut continuous_id = Vec::with_capacity(n);
    let mut source_contract_id = Vec::with_capacity(n);
    let mut source_ric = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    let mut price_adjustment = Vec::with_capacity(n);
    let mut source_depth_ts = Vec::with_capacity(n);
    let mut values: Vec<Vec<Option<f64>>> = (0..factor_names.len())
        .map(|_| Vec::with_capacity(n))
        .collect();
    for row in rows {
        if row.factors.len() != factor_names.len() {
            bail!("factor row width does not match factor schema");
        }
        continuous_id.push(row.continuous_id.clone());
        source_contract_id.push(row.source_contract_id.clone());
        source_ric.push(row.source_ric.clone());
        ts.push(row.ts);
        price_adjustment.push(row.price_adjustment);
        source_depth_ts.push(row.source_depth_ts_utc_ns);
        for (index, value) in row.factors.iter().copied().enumerate() {
            values[index].push(value);
        }
    }
    let mut columns = vec![
        Series::new("continuous_id".into(), continuous_id),
        Series::new("source_contract_id".into(), source_contract_id),
        Series::new("source_ric".into(), source_ric),
        Series::new("ts".into(), ts),
        Series::new("price_adjustment".into(), price_adjustment),
        Series::new("source_depth_ts_utc_ns".into(), source_depth_ts),
    ];
    for (index, name) in factor_names.iter().enumerate() {
        columns.push(Series::new(
            name.clone().into(),
            std::mem::take(&mut values[index]),
        ));
    }
    DataFrame::new(columns).context("build continuous factor dataframe")
}

fn write_manifest(root: &Path, rows: &[DayManifest], overwrite: bool) -> Result<()> {
    fs::create_dir_all(root).with_context(|| format!("create {}", root.display()))?;
    let path = root.join("coverage_manifest.csv");
    if path.exists() && !overwrite {
        bail!("output exists and overwrite=false: {}", path.display());
    }
    let temporary = root.join("coverage_manifest.csv.tmp");
    let mut writer = csv::Writer::from_path(&temporary)
        .with_context(|| format!("create {}", temporary.display()))?;
    writer.write_record([
        "exchange",
        "product_root",
        "trading_day",
        "source_contract_id",
        "source_ric",
        "status",
        "trade_rows",
        "written_rows",
        "missing_depth_rows",
        "invalid_depth_rows",
        "off_session_rows",
    ])?;
    for row in rows {
        writer.write_record([
            row.exchange.as_str(),
            row.product_root.as_str(),
            &row.trading_day.format("%Y%m%d").to_string(),
            row.source_contract_id.as_deref().unwrap_or(""),
            row.source_ric.as_deref().unwrap_or(""),
            row.status.as_str(),
            &row.trade_rows.to_string(),
            &row.written_rows.to_string(),
            &row.missing_depth_rows.to_string(),
            &row.invalid_depth_rows.to_string(),
            &row.off_session_rows.to_string(),
        ])?;
    }
    writer.flush()?;
    fs::rename(&temporary, &path)
        .with_context(|| format!("rename {} -> {}", temporary.display(), path.display()))?;
    Ok(())
}

fn read_parquet(path: &Path, columns: Vec<String>) -> Result<DataFrame> {
    let file = File::open(path).with_context(|| format!("open parquet {}", path.display()))?;
    ParquetReader::new(file)
        .with_columns(Some(columns))
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read parquet {}", path.display()))
}

fn string_column<'a>(df: &'a DataFrame, name: &str) -> Result<&'a StringChunked> {
    df.column(name)
        .with_context(|| format!("missing parquet column {name}"))?
        .str()
        .with_context(|| format!("parquet column {name} must be Utf8"))
}

fn i64_column<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Int64Chunked> {
    df.column(name)
        .with_context(|| format!("missing parquet column {name}"))?
        .i64()
        .with_context(|| format!("parquet column {name} must be Int64"))
}

fn i32_column<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Int32Chunked> {
    df.column(name)
        .with_context(|| format!("missing parquet column {name}"))?
        .i32()
        .with_context(|| format!("parquet column {name} must be Int32"))
}

fn f64_column<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Float64Chunked> {
    df.column(name)
        .with_context(|| format!("missing parquet column {name}"))?
        .f64()
        .with_context(|| format!("parquet column {name} must be Float64"))
}

fn required_str<'a>(
    column: &'a StringChunked,
    index: usize,
    name: &str,
    path: &Path,
) -> Result<&'a str> {
    column
        .get(index)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing {name} at row {index} in {}", path.display()))
}

fn required_i64(column: &Int64Chunked, index: usize, name: &str, path: &Path) -> Result<i64> {
    column
        .get(index)
        .ok_or_else(|| anyhow!("missing {name} at row {index} in {}", path.display()))
}

fn sorted_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = fs::read_dir(root)
        .with_context(|| format!("read directory {}", root.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn sorted_parquet_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = fs::read_dir(root)
        .with_context(|| format!("read directory {}", root.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("parquet"))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(ToOwned::to_owned)
        .with_context(|| format!("path has no UTF-8 name: {}", path.display()))
}

fn path_trading_day(path: &Path) -> Result<NaiveDate> {
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .with_context(|| format!("path has no UTF-8 stem: {}", path.display()))?;
    NaiveDate::parse_from_str(stem, "%Y%m%d")
        .with_context(|| format!("parquet name must be YYYYMMDD.parquet: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth() -> DepthRow {
        DepthRow {
            ts: 1_704_754_800,
            source_ts_utc_ns: 1_704_754_859_000_000_000,
            update_count: 3,
            bid_prices: [Some(100.0); DEPTH_LEVELS],
            bid_sizes: [Some(1.0); DEPTH_LEVELS],
            bid_counts: [0; DEPTH_LEVELS],
            ask_prices: [Some(101.0); DEPTH_LEVELS],
            ask_sizes: [Some(2.0); DEPTH_LEVELS],
            ask_counts: [0; DEPTH_LEVELS],
        }
    }

    #[test]
    fn additive_adjustment_changes_only_price_fields_and_book_prices() {
        let key = ProductKey {
            exchange: "CME".to_string(),
            product_root: "ES".to_string(),
        };
        let mut values = [Some(10.0); TRADE_VALUE_COLUMNS.len()];
        values[4] = Some(7.0);
        values[5] = Some(70.0);
        let row = make_continuous_row(
            &key,
            TradeRow {
                contract_id: "CME:ES:2024-03".to_string(),
                ric: "ESH24".to_string(),
                ts: 1_704_754_800,
                values,
            },
            &depth(),
            5.0,
        )
        .unwrap();
        assert_eq!(row.trade_values[0], Some(15.0));
        assert_eq!(row.trade_values[10], Some(15.0));
        assert_eq!(row.trade_values[4], Some(7.0));
        assert_eq!(row.trade_values[5], Some(70.0));
        assert_eq!(row.bid_prices[0], Some(105.0));
        assert_eq!(row.ask_prices[0], Some(106.0));
    }

    #[test]
    fn valid_bbo_does_not_require_l2_to_l10() {
        let mut row = depth();
        assert!(valid_best_depth(&row));
        row.bid_sizes[5] = None;
        assert!(valid_best_depth(&row));
        row.ask_prices[0] = Some(99.0);
        assert!(!valid_best_depth(&row));
    }

    #[test]
    fn interval_membership_is_left_closed_right_open() {
        let intervals = [(100, 160), (200, 260)];
        assert!(in_trading_interval(&intervals, 100));
        assert!(in_trading_interval(&intervals, 159));
        assert!(!in_trading_interval(&intervals, 160));
        assert!(!in_trading_interval(&intervals, 199));
    }

    #[test]
    fn factor_projection_matches_the_trade_feature_contract() {
        use mkt_parsers::msg::trade_flow_feature_msg::TRADE_FLOW_FEATURE_FIELD_NAMES;

        for (factor_index, source_name) in TRADE_FLOW_FEATURE_FIELD_NAMES.iter().enumerate() {
            assert_eq!(
                TRADE_VALUE_COLUMNS[FACTOR_TRADE_INDICES[factor_index]],
                *source_name
            );
        }
    }
}
