//! Replay Tonglian domestic-futures L2 snapshots into trade/depth baseline staging tables.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Asia::Shanghai;
use clap::{Parser, ValueEnum};
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use csv::{ReaderBuilder, StringRecord};
use log::{info, warn};
use mkt_parsers::msg::trade_flow_feature_msg::{
    TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
};
use postgres::{Config as PostgresConfig, NoTls};
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};
use zip::ZipArchive;

const DEPTH_LEVELS: usize = 5;
const DEFAULT_CLICKHOUSE_URL: &str = "http://127.0.0.1:18123";
const DEFAULT_DATABASE: &str = "baseline";
const DEFAULT_BATCH_ROWS: usize = 10_000;
const DEFAULT_FLUSH_MS: u64 = 1_000;
const DEFAULT_QUEUE_CAPACITY: usize = 100_000;
const PROGRESS_ROWS: u64 = 1_000_000;
const MULTIPLIER_TABLE: &str = "public.domestic_future_product_multipliers";
const MULTIPLIER_QUERY: &str = "SELECT product, exchange, volume_multiple, verified, fetched_at, effective_from, effective_to, source FROM public.domestic_future_product_multipliers WHERE exchange = $1 ORDER BY product";
const DEFAULT_MULTIPLIER_PG_HOST: &str = "/mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run";
const DEFAULT_MULTIPLIER_PG_PORT: u16 = 5433;
const DEFAULT_MULTIPLIER_PG_USER: &str = "u171";
const DEFAULT_MULTIPLIER_PG_DATABASE: &str = "market_metadata";

const QUALITY_VOLUME_RESET: u32 = 1 << 0;
const QUALITY_TURNOVER_RESET: u32 = 1 << 1;
const QUALITY_VOLUME_GAP: u32 = 1 << 2;
const QUALITY_TURNOVER_GAP: u32 = 1 << 3;
const QUALITY_AMOUNT_WITHOUT_VOLUME: u32 = 1 << 4;
const QUALITY_VOLUME_WITHOUT_AMOUNT: u32 = 1 << 5;
const QUALITY_HIGH_RESET: u32 = 1 << 6;
const QUALITY_LOW_RESET: u32 = 1 << 7;
const QUALITY_VOLUME_MULTIPLE_ASSUMED: u32 = 1 << 8;
const QUALITY_SEGMENT_BREAK: u32 = 1 << 9;
const QUALITY_ORDER_SIZE_UNAVAILABLE: u32 = 1 << 10;
const QUALITY_LAST_PRICE_MISSING: u32 = 1 << 11;
const QUALITY_PREVENT_EMPTY_BAR_FILL: u32 = QUALITY_VOLUME_RESET
    | QUALITY_TURNOVER_RESET
    | QUALITY_VOLUME_GAP
    | QUALITY_TURNOVER_GAP
    | QUALITY_AMOUNT_WITHOUT_VOLUME
    | QUALITY_VOLUME_WITHOUT_AMOUNT
    | QUALITY_HIGH_RESET
    | QUALITY_LOW_RESET
    | QUALITY_LAST_PRICE_MISSING;

#[derive(Parser, Debug)]
#[command(name = "tonglian_baseline_replay")]
#[command(about = "Replay Tonglian five-level futures snapshots into ClickHouse baseline bars")]
struct Args {
    /// Exchange-specific TOML configuration.
    #[arg(long, default_value = "config/tonglian_baseline_xsge.toml")]
    config: PathBuf,
    /// Override the config's diagnostic source-row limit for each ZIP.
    #[arg(long)]
    max_source_rows_per_file: Option<u64>,
}

#[derive(Debug, Clone, Copy, Deserialize, ValueEnum, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
enum Exchange {
    Ccfx,
    Xdce,
    Xgfe,
    Xsge,
    Xsie,
    Xzce,
}

impl Exchange {
    fn code(self) -> &'static str {
        match self {
            Self::Ccfx => "ccfx",
            Self::Xdce => "xdce",
            Self::Xgfe => "xgfe",
            Self::Xsge => "xsge",
            Self::Xsie => "xsie",
            Self::Xzce => "xzce",
        }
    }

    fn file_name(self, date: &str) -> String {
        format!("future_{}l2_{date}.zip", self.code())
    }

    fn excludes_efp(self) -> bool {
        matches!(self, Self::Xsge | Self::Xsie)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayConfig {
    exchange: Exchange,
    data_root: PathBuf,
    start_date: String,
    end_date: String,
    #[serde(default)]
    symbols: Vec<String>,
    volume_multiplier_postgres: VolumeMultiplierPostgresConfig,
    /// A source gap longer than this starts a new forward-fill/direction segment.
    #[serde(default = "default_continuous_gap_ms")]
    max_continuous_gap_ms: i64,
    /// Local HH:MM minutes retained as cumulative baselines but excluded from bars.
    #[serde(default)]
    excluded_local_minutes: Vec<String>,
    #[serde(default = "default_workers")]
    replay_workers: usize,
    #[serde(default = "default_dry_run")]
    dry_run: bool,
    #[serde(default)]
    overwrite_existing: bool,
    /// Diagnostic limit. Omit for a full file replay.
    #[serde(default)]
    max_source_rows_per_file: Option<u64>,
    #[serde(default)]
    clickhouse: ClickHouseConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct VolumeMultiplierPostgresConfig {
    #[serde(default = "default_multiplier_pg_host")]
    host: String,
    #[serde(default = "default_multiplier_pg_port")]
    port: u16,
    #[serde(default = "default_multiplier_pg_user")]
    user: String,
    #[serde(default = "default_multiplier_pg_database")]
    database: String,
    #[serde(default = "default_multiplier_pg_connect_timeout_secs")]
    connect_timeout_secs: u64,
    /// Inclusive/exclusive replay coverage established for this undated snapshot.
    effective_from: String,
    effective_to: String,
}

#[derive(Debug, Clone, Copy)]
struct ResolvedVolumeMultiplier {
    value: f64,
    verified: bool,
}
#[derive(Debug, Clone)]
struct VolumeMultiplierRecord {
    resolved: ResolvedVolumeMultiplier,
    fetched_at: DateTime<Utc>,
    effective_from: Option<NaiveDate>,
    effective_to: Option<NaiveDate>,
    source: String,
}

#[derive(Debug)]
struct VolumeMultiplierCatalog {
    exchange: Exchange,
    products: HashMap<String, VolumeMultiplierRecord>,
    fetched_at_min: DateTime<Utc>,
    fetched_at_max: DateTime<Utc>,
    unverified_products: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ClickHouseConfig {
    #[serde(default = "default_clickhouse_url")]
    url: String,
    #[serde(default = "default_database")]
    database: String,
    #[serde(default = "default_batch_rows")]
    batch_rows: usize,
    #[serde(default = "default_flush_ms")]
    flush_ms: u64,
    #[serde(default = "default_queue_capacity")]
    queue_capacity: usize,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: default_clickhouse_url(),
            database: default_database(),
            batch_rows: default_batch_rows(),
            flush_ms: default_flush_ms(),
            queue_capacity: default_queue_capacity(),
        }
    }
}

fn default_continuous_gap_ms() -> i64 {
    30_000
}

fn default_workers() -> usize {
    1
}

fn default_dry_run() -> bool {
    true
}

fn default_multiplier_pg_host() -> String {
    DEFAULT_MULTIPLIER_PG_HOST.to_string()
}

fn default_multiplier_pg_port() -> u16 {
    DEFAULT_MULTIPLIER_PG_PORT
}

fn default_multiplier_pg_user() -> String {
    DEFAULT_MULTIPLIER_PG_USER.to_string()
}

fn default_multiplier_pg_database() -> String {
    DEFAULT_MULTIPLIER_PG_DATABASE.to_string()
}

fn default_multiplier_pg_connect_timeout_secs() -> u64 {
    5
}

impl VolumeMultiplierCatalog {
    fn load(exchange: Exchange, config: &VolumeMultiplierPostgresConfig) -> Result<Self> {
        let mut postgres = PostgresConfig::new();
        postgres
            .host(&config.host)
            .port(config.port)
            .user(&config.user)
            .dbname(&config.database)
            .options("-c default_transaction_read_only=on")
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs));
        let mut client = postgres.connect(NoTls).with_context(|| {
            format!(
                "connect to volume multiplier PostgreSQL host={} port={} database={} user={}",
                config.host, config.port, config.database, config.user
            )
        })?;
        let rows = client
            .query(MULTIPLIER_QUERY, &[&exchange.code()])
            .with_context(|| {
                format!(
                    "query {} for exchange={}",
                    MULTIPLIER_TABLE,
                    exchange.code()
                )
            })?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let product: String = row.try_get("product").context("read multiplier product")?;
            let row_exchange: String = row
                .try_get("exchange")
                .context("read multiplier exchange")?;
            if row_exchange != exchange.code() {
                bail!(
                    "multiplier row exchange mismatch: requested={} product={} row={}",
                    exchange.code(),
                    product,
                    row_exchange
                );
            }
            let value: i32 = row
                .try_get("volume_multiple")
                .with_context(|| format!("read volume_multiple for {product}"))?;
            records.push((
                product,
                VolumeMultiplierRecord {
                    resolved: ResolvedVolumeMultiplier {
                        value: f64::from(value),
                        verified: row
                            .try_get("verified")
                            .context("read multiplier verified")?,
                    },
                    fetched_at: row
                        .try_get("fetched_at")
                        .context("read multiplier fetched_at")?,
                    effective_from: row
                        .try_get("effective_from")
                        .context("read multiplier effective_from")?,
                    effective_to: row
                        .try_get("effective_to")
                        .context("read multiplier effective_to")?,
                    source: row.try_get("source").context("read multiplier source")?,
                },
            ));
        }
        Self::from_records(exchange, records)
    }

    fn from_records(
        exchange: Exchange,
        records: Vec<(String, VolumeMultiplierRecord)>,
    ) -> Result<Self> {
        let mut products = HashMap::with_capacity(records.len());
        let mut fetched_at_min: Option<DateTime<Utc>> = None;
        let mut fetched_at_max: Option<DateTime<Utc>> = None;
        let mut unverified_products = Vec::new();
        for (product, record) in records {
            if product.is_empty() || !product.bytes().all(|byte| byte.is_ascii_uppercase()) {
                bail!(
                    "invalid product in {}: exchange={} product={}",
                    MULTIPLIER_TABLE,
                    exchange.code(),
                    product
                );
            }
            if !positive_finite(record.resolved.value) {
                bail!(
                    "invalid volume_multiple in {}: exchange={} product={} value={}",
                    MULTIPLIER_TABLE,
                    exchange.code(),
                    product,
                    record.resolved.value
                );
            }
            if record
                .effective_from
                .zip(record.effective_to)
                .is_some_and(|(start, end)| start >= end)
            {
                bail!(
                    "invalid multiplier effective range: exchange={} product={} from={:?} to={:?}",
                    exchange.code(),
                    product,
                    record.effective_from,
                    record.effective_to
                );
            }
            if !record.resolved.verified {
                unverified_products.push(product.clone());
            }
            match fetched_at_min.as_ref() {
                Some(current) if current <= &record.fetched_at => {}
                _ => fetched_at_min = Some(record.fetched_at),
            }
            match fetched_at_max.as_ref() {
                Some(current) if current >= &record.fetched_at => {}
                _ => fetched_at_max = Some(record.fetched_at),
            }
            if products.insert(product.clone(), record).is_some() {
                bail!(
                    "duplicate multiplier product in {}: exchange={} product={}",
                    MULTIPLIER_TABLE,
                    exchange.code(),
                    product
                );
            }
        }
        if products.is_empty() {
            bail!(
                "{} returned no rows for exchange={}",
                MULTIPLIER_TABLE,
                exchange.code()
            );
        }
        unverified_products.sort();
        Ok(Self {
            exchange,
            products,
            fetched_at_min: fetched_at_min.context("missing multiplier fetched_at minimum")?,
            fetched_at_max: fetched_at_max.context("missing multiplier fetched_at maximum")?,
            unverified_products,
        })
    }

    fn resolve_or_panic(&self, instrument_id: &str, trading_day: u32) -> ResolvedVolumeMultiplier {
        let product = product_id_from_instrument(instrument_id).unwrap_or_else(|error| {
            panic!(
                "cannot resolve volume multiplier: exchange={} instrument={} trading_day={} error={error:#}",
                self.exchange.code(),
                instrument_id,
                trading_day
            )
        });
        let record = self.products.get(product).unwrap_or_else(|| {
            panic!(
                "missing volume multiplier in {}: exchange={} product={} instrument={} trading_day={}",
                MULTIPLIER_TABLE,
                self.exchange.code(),
                product,
                instrument_id,
                trading_day
            )
        });
        let trading_date = NaiveDate::parse_from_str(&trading_day.to_string(), "%Y%m%d")
            .unwrap_or_else(|error| panic!("invalid trading_day={trading_day}: {error}"));
        if record
            .effective_from
            .is_some_and(|effective_from| trading_date < effective_from)
            || record
                .effective_to
                .is_some_and(|effective_to| trading_date >= effective_to)
        {
            panic!(
                "volume multiplier is outside its row-level effective range: exchange={} product={} trading_day={} from={:?} to={:?}",
                self.exchange.code(),
                product,
                trading_day,
                record.effective_from,
                record.effective_to
            );
        }
        record.resolved
    }

    fn source_summary(&self) -> String {
        let mut sources: Vec<&str> = self
            .products
            .values()
            .map(|record| record.source.as_str())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        sources.sort_unstable();
        sources.join(" + ")
    }
}

fn default_clickhouse_url() -> String {
    DEFAULT_CLICKHOUSE_URL.to_string()
}

fn default_database() -> String {
    DEFAULT_DATABASE.to_string()
}

fn default_batch_rows() -> usize {
    DEFAULT_BATCH_ROWS
}

fn default_flush_ms() -> u64 {
    DEFAULT_FLUSH_MS
}

fn default_queue_capacity() -> usize {
    DEFAULT_QUEUE_CAPACITY
}

#[derive(Debug, Clone)]
struct ReplayTask {
    trading_day: u32,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct HeaderMap {
    action_day: usize,
    trading_day: usize,
    update_time: usize,
    instrument_id: usize,
    last_price: usize,
    high_price: usize,
    low_price: usize,
    volume: usize,
    turnover: usize,
    bid_price: [usize; DEPTH_LEVELS],
    bid_volume: [usize; DEPTH_LEVELS],
    ask_price: [usize; DEPTH_LEVELS],
    ask_volume: [usize; DEPTH_LEVELS],
}

impl HeaderMap {
    fn from_headers(headers: &StringRecord) -> Result<Self> {
        let index: HashMap<&str, usize> = headers
            .iter()
            .enumerate()
            .map(|(offset, name)| (name.trim().trim_start_matches('\u{feff}'), offset))
            .collect();
        let required = |name: &str| {
            index
                .get(name)
                .copied()
                .with_context(|| format!("Tonglian CSV missing required column {name}"))
        };
        Ok(Self {
            action_day: required("ActionDay")?,
            trading_day: required("TradDay")?,
            update_time: required("UpdateTime")?,
            instrument_id: required("InstruID")?,
            last_price: required("LastPrice")?,
            high_price: required("HighPrice")?,
            low_price: required("LowPrice")?,
            volume: required("Volume")?,
            turnover: required("Turnover")?,
            bid_price: required_level_indices(&required, "BidPrice")?,
            bid_volume: required_level_indices(&required, "BidVolume")?,
            ask_price: required_level_indices(&required, "AskPrice")?,
            ask_volume: required_level_indices(&required, "AskVolume")?,
        })
    }
}

fn required_level_indices(
    required: &impl Fn(&str) -> Result<usize>,
    prefix: &str,
) -> Result<[usize; DEPTH_LEVELS]> {
    let mut indices = [0usize; DEPTH_LEVELS];
    for level in 1..=DEPTH_LEVELS {
        indices[level - 1] = required(&format!("{prefix}{level}"))?;
    }
    Ok(indices)
}

#[derive(Debug, Clone)]
struct Snapshot {
    timestamp_ms: i64,
    local_minute: u16,
    trading_day: u32,
    symbol: String,
    last_price: Option<f64>,
    high_price: Option<f64>,
    low_price: Option<f64>,
    volume: Option<f64>,
    turnover: Option<f64>,
    bid_prices: [f64; DEPTH_LEVELS],
    bid_amounts: [f64; DEPTH_LEVELS],
    ask_prices: [f64; DEPTH_LEVELS],
    ask_amounts: [f64; DEPTH_LEVELS],
    valid_book: bool,
}

#[derive(Debug, Clone)]
struct DepthValues {
    timestamp_ms: i64,
    bid_prices: [f64; DEPTH_LEVELS],
    bid_amounts: [f64; DEPTH_LEVELS],
    ask_prices: [f64; DEPTH_LEVELS],
    ask_amounts: [f64; DEPTH_LEVELS],
}

#[derive(Debug)]
struct OutOfOrderSnapshot {
    symbol: String,
    trading_day: u32,
    previous_timestamp_ms: i64,
    current_timestamp_ms: i64,
}

impl fmt::Display for OutOfOrderSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "out-of-order snapshot for symbol={} trading_day={} previous_ts={} current_ts={}",
            self.symbol, self.trading_day, self.previous_timestamp_ms, self.current_timestamp_ms
        )
    }
}

impl std::error::Error for OutOfOrderSnapshot {}

impl From<&Snapshot> for DepthValues {
    fn from(snapshot: &Snapshot) -> Self {
        Self {
            timestamp_ms: snapshot.timestamp_ms,
            bid_prices: snapshot.bid_prices,
            bid_amounts: snapshot.bid_amounts,
            ask_prices: snapshot.ask_prices,
            ask_amounts: snapshot.ask_amounts,
        }
    }
}

impl Snapshot {
    fn parse(record: &StringRecord, columns: &HeaderMap) -> Result<Self> {
        let action_day = record_field(record, columns.action_day, "ActionDay")?;
        let update_time = record_field(record, columns.update_time, "UpdateTime")?;
        let timestamp_ms = parse_shanghai_timestamp(action_day, update_time)?;
        let local_time = NaiveTime::parse_from_str(update_time.trim(), "%H:%M:%S%.f")
            .with_context(|| format!("parse UpdateTime={update_time}"))?;
        let trading_day_text = record_field(record, columns.trading_day, "TradDay")?;
        let trading_day = parse_yyyymmdd_u32(trading_day_text, "TradDay")?;
        let symbol = record_field(record, columns.instrument_id, "InstruID")?
            .trim()
            .to_ascii_uppercase();
        if symbol.is_empty() {
            bail!("empty InstruID");
        }

        let mut bid_prices = [f64::NAN; DEPTH_LEVELS];
        let mut bid_amounts = [f64::NAN; DEPTH_LEVELS];
        let mut ask_prices = [f64::NAN; DEPTH_LEVELS];
        let mut ask_amounts = [f64::NAN; DEPTH_LEVELS];
        for level in 0..DEPTH_LEVELS {
            parse_depth_level(
                record,
                columns.bid_price[level],
                columns.bid_volume[level],
                &mut bid_prices[level],
                &mut bid_amounts[level],
            );
            parse_depth_level(
                record,
                columns.ask_price[level],
                columns.ask_volume[level],
                &mut ask_prices[level],
                &mut ask_amounts[level],
            );
        }
        let valid_book = positive_finite(bid_prices[0])
            && positive_finite(ask_prices[0])
            && bid_amounts[0].is_finite()
            && ask_amounts[0].is_finite()
            && bid_prices[0] <= ask_prices[0];

        Ok(Self {
            timestamp_ms,
            local_minute: (local_time.hour() * 60 + local_time.minute()) as u16,
            trading_day,
            symbol,
            last_price: parse_positive_optional(record.get(columns.last_price)),
            high_price: parse_positive_optional(record.get(columns.high_price)),
            low_price: parse_positive_optional(record.get(columns.low_price)),
            volume: parse_nonnegative_optional(record.get(columns.volume)),
            turnover: parse_nonnegative_optional(record.get(columns.turnover)),
            bid_prices,
            bid_amounts,
            ask_prices,
            ask_amounts,
            valid_book,
        })
    }
}

fn record_field<'a>(record: &'a StringRecord, index: usize, name: &str) -> Result<&'a str> {
    record
        .get(index)
        .with_context(|| format!("Tonglian CSV record missing {name} at column {index}"))
}

fn parse_depth_level(
    record: &StringRecord,
    price_index: usize,
    amount_index: usize,
    price_out: &mut f64,
    amount_out: &mut f64,
) {
    let price = parse_positive_optional(record.get(price_index));
    let amount = parse_nonnegative_optional(record.get(amount_index));
    if let (Some(price), Some(amount)) = (price, amount) {
        *price_out = price;
        *amount_out = amount;
    }
}

fn parse_positive_optional(value: Option<&str>) -> Option<f64> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| positive_finite(*value))
}

fn parse_nonnegative_optional(value: Option<&str>) -> Option<f64> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
}

fn positive_finite(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

fn parse_yyyymmdd_u32(value: &str, name: &str) -> Result<u32> {
    NaiveDate::parse_from_str(value.trim(), "%Y%m%d")
        .with_context(|| format!("parse {name}={value}"))?;
    value
        .trim()
        .parse::<u32>()
        .with_context(|| format!("parse numeric {name}={value}"))
}

fn parse_shanghai_timestamp(action_day: &str, update_time: &str) -> Result<i64> {
    let date = NaiveDate::parse_from_str(action_day.trim(), "%Y%m%d")
        .with_context(|| format!("parse ActionDay={action_day}"))?;
    let time = NaiveTime::parse_from_str(update_time.trim(), "%H:%M:%S%.f")
        .with_context(|| format!("parse UpdateTime={update_time}"))?;
    let local = NaiveDateTime::new(date, time);
    let timestamp = Shanghai
        .from_local_datetime(&local)
        .single()
        .with_context(|| format!("resolve Asia/Shanghai local timestamp {local}"))?;
    Ok(timestamp.timestamp_millis())
}

#[derive(Debug, Clone, Copy)]
enum DeltaObservation {
    Baseline,
    Valid(f64),
    Invalid(u32),
}

#[derive(Debug, Clone)]
struct CumulativeState {
    previous: Option<f64>,
    has_baseline: bool,
    gap_flag: u32,
    reset_flag: u32,
}

impl CumulativeState {
    fn new(gap_flag: u32, reset_flag: u32) -> Self {
        Self {
            previous: None,
            has_baseline: false,
            gap_flag,
            reset_flag,
        }
    }

    fn observe(&mut self, current: Option<f64>) -> DeltaObservation {
        let Some(current) = current else {
            if self.has_baseline {
                self.previous = None;
                return DeltaObservation::Invalid(self.gap_flag);
            }
            return DeltaObservation::Baseline;
        };
        let Some(previous) = self.previous.replace(current) else {
            let was_initialized = self.has_baseline;
            self.has_baseline = true;
            return if was_initialized {
                DeltaObservation::Invalid(self.gap_flag)
            } else {
                DeltaObservation::Baseline
            };
        };
        self.has_baseline = true;
        let delta = current - previous;
        if delta < 0.0 {
            DeltaObservation::Invalid(self.reset_flag)
        } else {
            DeltaObservation::Valid(delta)
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    Buy,
    Sell,
    Unknown,
}

#[derive(Debug, Clone, Default)]
struct IntervalContribution {
    volume: f64,
    amount: f64,
    count: f64,
    buy_count: f64,
    sell_count: f64,
    buy_volume: f64,
    sell_volume: f64,
    buy_amount: f64,
    sell_amount: f64,
    trade_price: Option<f64>,
    extra_high: Option<f64>,
    extra_low: Option<f64>,
    volume_valid: bool,
    turnover_valid: bool,
    flags: u32,
}

impl IntervalContribution {
    fn valid() -> Self {
        Self {
            volume_valid: true,
            turnover_valid: true,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
struct InstrumentState {
    volume: CumulativeState,
    turnover: CumulativeState,
    previous_high: Option<f64>,
    previous_low: Option<f64>,
    previous_bid: Option<f64>,
    previous_ask: Option<f64>,
    previous_trade_price: Option<f64>,
    last_tick_direction: Option<Direction>,
    previous_timestamp_ms: Option<i64>,
    previous_depth: Option<DepthValues>,
    force_segment_break: bool,
    bars_5s: BarState,
    bars_60s: BarState,
}

impl InstrumentState {
    fn new(volume_multiple: f64, volume_multiple_verified: bool) -> Self {
        Self {
            volume: CumulativeState::new(QUALITY_VOLUME_GAP, QUALITY_VOLUME_RESET),
            turnover: CumulativeState::new(QUALITY_TURNOVER_GAP, QUALITY_TURNOVER_RESET),
            previous_high: None,
            previous_low: None,
            previous_bid: None,
            previous_ask: None,
            previous_trade_price: None,
            last_tick_direction: None,
            previous_timestamp_ms: None,
            previous_depth: None,
            force_segment_break: false,
            bars_5s: BarState::new(5_000, volume_multiple, volume_multiple_verified),
            bars_60s: BarState::new(60_000, volume_multiple, volume_multiple_verified),
        }
    }

    fn on_snapshot(
        &mut self,
        snapshot: &Snapshot,
        max_continuous_gap_ms: i64,
        include_in_bars: bool,
    ) -> Result<Vec<CompletedBar>> {
        if self
            .previous_timestamp_ms
            .is_some_and(|previous| snapshot.timestamp_ms < previous)
        {
            return Err(OutOfOrderSnapshot {
                symbol: snapshot.symbol.clone(),
                trading_day: snapshot.trading_day,
                previous_timestamp_ms: self.previous_timestamp_ms.unwrap_or_default(),
                current_timestamp_ms: snapshot.timestamp_ms,
            }
            .into());
        }
        let segment_break = self.force_segment_break
            || self.previous_timestamp_ms.is_some_and(|previous| {
                snapshot.timestamp_ms.saturating_sub(previous) > max_continuous_gap_ms
            });
        self.force_segment_break = false;
        self.previous_timestamp_ms = Some(snapshot.timestamp_ms);
        if segment_break {
            self.previous_bid = None;
            self.previous_ask = None;
            self.previous_trade_price = None;
            self.last_tick_direction = None;
            self.previous_depth = None;
        }
        let depth_before_boundary = self.previous_depth.clone();

        let volume_observation = self.volume.observe(snapshot.volume);
        let turnover_observation = self.turnover.observe(snapshot.turnover);
        let volume_has_delta = matches!(volume_observation, DeltaObservation::Valid(_));
        let turnover_has_delta = matches!(turnover_observation, DeltaObservation::Valid(_));
        let mut contribution = IntervalContribution::valid();
        if segment_break {
            contribution.flags |= QUALITY_SEGMENT_BREAK;
        }
        match volume_observation {
            DeltaObservation::Baseline => {}
            DeltaObservation::Valid(delta) => contribution.volume = delta,
            DeltaObservation::Invalid(flag) => {
                contribution.volume_valid = false;
                contribution.flags |= flag;
            }
        }
        match turnover_observation {
            DeltaObservation::Baseline => {}
            DeltaObservation::Valid(delta) => contribution.amount = delta,
            DeltaObservation::Invalid(flag) => {
                contribution.turnover_valid = false;
                contribution.flags |= flag;
            }
        }

        if volume_has_delta
            && turnover_has_delta
            && contribution.volume > 0.0
            && contribution.amount == 0.0
        {
            contribution.turnover_valid = false;
            contribution.flags |= QUALITY_VOLUME_WITHOUT_AMOUNT;
        } else if volume_has_delta
            && turnover_has_delta
            && contribution.volume == 0.0
            && contribution.amount > 0.0
        {
            contribution.turnover_valid = false;
            contribution.flags |= QUALITY_AMOUNT_WITHOUT_VOLUME;
        } else if volume_has_delta && matches!(turnover_observation, DeltaObservation::Baseline) {
            contribution.turnover_valid = false;
            contribution.flags |= QUALITY_TURNOVER_GAP;
        } else if matches!(volume_observation, DeltaObservation::Baseline) && turnover_has_delta {
            contribution.volume_valid = false;
            contribution.flags |= QUALITY_VOLUME_GAP;
        }

        if contribution.volume > 0.0 {
            contribution.count = 1.0;
            let direction = if let Some(price) = snapshot.last_price {
                contribution.trade_price = Some(price);
                self.classify_trade(price)
            } else {
                contribution.flags |= QUALITY_LAST_PRICE_MISSING;
                Direction::Unknown
            };
            match direction {
                Direction::Buy => {
                    contribution.buy_count = 1.0;
                    contribution.buy_volume = contribution.volume;
                    contribution.buy_amount = contribution.amount;
                }
                Direction::Sell => {
                    contribution.sell_count = 1.0;
                    contribution.sell_volume = contribution.volume;
                    contribution.sell_amount = contribution.amount;
                }
                Direction::Unknown => {
                    contribution.buy_count = 0.5;
                    contribution.sell_count = 0.5;
                    contribution.buy_volume = contribution.volume * 0.5;
                    contribution.sell_volume = contribution.volume * 0.5;
                    contribution.buy_amount = contribution.amount * 0.5;
                    contribution.sell_amount = contribution.amount * 0.5;
                }
            }
        }

        update_cumulative_extrema(snapshot, &mut contribution, self);
        if snapshot.valid_book {
            self.previous_bid = Some(snapshot.bid_prices[0]);
            self.previous_ask = Some(snapshot.ask_prices[0]);
        }

        let mut completed = Vec::with_capacity(4);
        if !include_in_bars {
            if let Some(bar) = self.bars_5s.end_segment() {
                completed.push(bar);
            }
            if let Some(bar) = self.bars_60s.end_segment() {
                completed.push(bar);
            }
            self.previous_bid = None;
            self.previous_ask = None;
            self.previous_trade_price = None;
            self.last_tick_direction = None;
            self.previous_depth = None;
            self.force_segment_break = true;
            return Ok(completed);
        }
        let eligible_for_bar = snapshot.valid_book
            || contribution.volume > 0.0
            || contribution.extra_high.is_some()
            || contribution.extra_low.is_some()
            || !contribution.volume_valid
            || !contribution.turnover_valid;
        if !eligible_for_bar {
            return Ok(completed);
        }
        completed.extend(self.bars_5s.on_snapshot(
            snapshot,
            &contribution,
            segment_break,
            depth_before_boundary.as_ref(),
        ));
        completed.extend(self.bars_60s.on_snapshot(
            snapshot,
            &contribution,
            segment_break,
            depth_before_boundary.as_ref(),
        ));
        if snapshot.valid_book {
            self.previous_depth = Some(DepthValues::from(snapshot));
        }
        Ok(completed)
    }

    fn classify_trade(&mut self, price: f64) -> Direction {
        let direction = match (self.previous_bid, self.previous_ask) {
            (Some(bid), Some(ask)) if bid < ask && price >= ask => Direction::Buy,
            (Some(bid), Some(ask)) if bid < ask && price <= bid => Direction::Sell,
            _ => match self.previous_trade_price {
                Some(previous) if price > previous => Direction::Buy,
                Some(previous) if price < previous => Direction::Sell,
                Some(_) => self.last_tick_direction.unwrap_or(Direction::Unknown),
                None => Direction::Unknown,
            },
        };
        if let Some(previous) = self.previous_trade_price {
            if price > previous {
                self.last_tick_direction = Some(Direction::Buy);
            } else if price < previous {
                self.last_tick_direction = Some(Direction::Sell);
            }
        }
        self.previous_trade_price = Some(price);
        direction
    }

    fn flush(&mut self) -> Vec<CompletedBar> {
        let mut completed = Vec::with_capacity(2);
        if let Some(bar) = self.bars_5s.flush() {
            completed.push(bar);
        }
        if let Some(bar) = self.bars_60s.flush() {
            completed.push(bar);
        }
        completed
    }
}

fn update_cumulative_extrema(
    snapshot: &Snapshot,
    contribution: &mut IntervalContribution,
    state: &mut InstrumentState,
) {
    if let Some(current) = snapshot.high_price {
        match state.previous_high.replace(current) {
            Some(previous) if current > previous => contribution.extra_high = Some(current),
            Some(previous) if current < previous => contribution.flags |= QUALITY_HIGH_RESET,
            _ => {}
        }
    } else {
        state.previous_high = None;
    }
    if let Some(current) = snapshot.low_price {
        match state.previous_low.replace(current) {
            Some(previous) if current < previous => contribution.extra_low = Some(current),
            Some(previous) if current > previous => contribution.flags |= QUALITY_LOW_RESET,
            _ => {}
        }
    } else {
        state.previous_low = None;
    }
}

#[derive(Debug, Clone)]
struct WorkingBar {
    start_ms: i64,
    trading_day: u32,
    symbol: String,
    has_trade: bool,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    amount: f64,
    count: f64,
    buy_count: f64,
    sell_count: f64,
    buy_amount: f64,
    sell_amount: f64,
    buy_volume: f64,
    sell_volume: f64,
    volume_valid: bool,
    turnover_valid: bool,
    flags: u32,
    bid_prices: [f64; DEPTH_LEVELS],
    bid_amounts: [f64; DEPTH_LEVELS],
    ask_prices: [f64; DEPTH_LEVELS],
    ask_amounts: [f64; DEPTH_LEVELS],
    valid_book: bool,
}

impl WorkingBar {
    fn new(start_ms: i64, snapshot: &Snapshot, depth_at_start: Option<&DepthValues>) -> Self {
        let (bid_prices, bid_amounts, ask_prices, ask_amounts, valid_book) = depth_at_start
            .filter(|depth| depth.timestamp_ms < start_ms)
            .map(|depth| {
                (
                    depth.bid_prices,
                    depth.bid_amounts,
                    depth.ask_prices,
                    depth.ask_amounts,
                    true,
                )
            })
            .unwrap_or((
                [f64::NAN; DEPTH_LEVELS],
                [f64::NAN; DEPTH_LEVELS],
                [f64::NAN; DEPTH_LEVELS],
                [f64::NAN; DEPTH_LEVELS],
                false,
            ));
        Self {
            start_ms,
            trading_day: snapshot.trading_day,
            symbol: snapshot.symbol.clone(),
            has_trade: false,
            open: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            close: f64::NAN,
            volume: 0.0,
            amount: 0.0,
            count: 0.0,
            buy_count: 0.0,
            sell_count: 0.0,
            buy_amount: 0.0,
            sell_amount: 0.0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            volume_valid: true,
            turnover_valid: true,
            flags: 0,
            bid_prices,
            bid_amounts,
            ask_prices,
            ask_amounts,
            valid_book,
        }
    }

    fn update(&mut self, contribution: &IntervalContribution) {
        self.volume += contribution.volume;
        self.amount += contribution.amount;
        self.count += contribution.count;
        self.buy_count += contribution.buy_count;
        self.sell_count += contribution.sell_count;
        self.buy_amount += contribution.buy_amount;
        self.sell_amount += contribution.sell_amount;
        self.buy_volume += contribution.buy_volume;
        self.sell_volume += contribution.sell_volume;
        self.volume_valid &= contribution.volume_valid;
        self.turnover_valid &= contribution.turnover_valid;
        self.flags |= contribution.flags;

        if let Some(price) = contribution.trade_price {
            if !self.has_trade {
                self.has_trade = true;
                self.open = price;
                self.high = price;
                self.low = price;
            } else {
                self.high = self.high.max(price);
                self.low = self.low.min(price);
            }
            self.close = price;
        }
        if let Some(high) = contribution.extra_high {
            self.high = if self.high.is_finite() {
                self.high.max(high)
            } else {
                high
            };
        }
        if let Some(low) = contribution.extra_low {
            self.low = if self.low.is_finite() {
                self.low.min(low)
            } else {
                low
            };
        }
    }

    fn invalidate_depth(&mut self) {
        self.bid_prices = [f64::NAN; DEPTH_LEVELS];
        self.bid_amounts = [f64::NAN; DEPTH_LEVELS];
        self.ask_prices = [f64::NAN; DEPTH_LEVELS];
        self.ask_amounts = [f64::NAN; DEPTH_LEVELS];
        self.valid_book = false;
    }
}

#[derive(Debug, Clone)]
struct BarState {
    interval_ms: i64,
    volume_multiple: f64,
    volume_multiple_verified: bool,
    current: Option<WorkingBar>,
    last_close: Option<f64>,
    last_vwap: Option<f64>,
    last_buy_vwap: Option<f64>,
    last_sell_vwap: Option<f64>,
}

impl BarState {
    fn new(interval_ms: i64, volume_multiple: f64, volume_multiple_verified: bool) -> Self {
        Self {
            interval_ms,
            volume_multiple,
            volume_multiple_verified,
            current: None,
            last_close: None,
            last_vwap: None,
            last_buy_vwap: None,
            last_sell_vwap: None,
        }
    }

    fn on_snapshot(
        &mut self,
        snapshot: &Snapshot,
        contribution: &IntervalContribution,
        segment_break: bool,
        depth_at_start: Option<&DepthValues>,
    ) -> Vec<CompletedBar> {
        let target = align(snapshot.timestamp_ms, self.interval_ms);
        let mut completed = Vec::with_capacity(1);
        let should_close = self
            .current
            .as_ref()
            .is_some_and(|current| current.start_ms != target);
        if should_close {
            if let Some(bar) = self.finalize_current() {
                completed.push(bar);
            }
        }
        if segment_break {
            self.last_close = None;
            self.last_vwap = None;
            self.last_buy_vwap = None;
            self.last_sell_vwap = None;
            if !should_close {
                if let Some(current) = self.current.as_mut() {
                    current.invalidate_depth();
                }
            }
        }
        if self.current.is_none() {
            let mut current = WorkingBar::new(target, snapshot, depth_at_start);
            if !self.volume_multiple_verified {
                current.flags |= QUALITY_VOLUME_MULTIPLE_ASSUMED;
            }
            self.current = Some(current);
        }
        if let Some(current) = self.current.as_mut() {
            current.update(contribution);
        }
        completed
    }

    fn flush(&mut self) -> Option<CompletedBar> {
        self.finalize_current()
    }

    fn end_segment(&mut self) -> Option<CompletedBar> {
        let completed = self.finalize_current();
        self.last_close = None;
        self.last_vwap = None;
        self.last_buy_vwap = None;
        self.last_sell_vwap = None;
        completed
    }

    fn finalize_current(&mut self) -> Option<CompletedBar> {
        let mut bar = self.current.take()?;
        let mut values = [0.0f64; TRADE_FLOW_FEATURE_DIM];
        let ordinary_empty_bar = !bar.has_trade
            && bar.volume_valid
            && bar.turnover_valid
            && bar.volume == 0.0
            && bar.amount == 0.0
            && !bar.high.is_finite()
            && !bar.low.is_finite()
            && bar.flags & QUALITY_PREVENT_EMPTY_BAR_FILL == 0;
        if ordinary_empty_bar {
            let close = self.last_close.unwrap_or(f64::NAN);
            bar.open = close;
            bar.high = close;
            bar.low = close;
            bar.close = close;
        }

        values[0] = bar.open;
        values[1] = bar.high;
        values[2] = bar.low;
        values[3] = bar.close;
        values[4] = bar.volume;
        values[5] = bar.amount;
        values[6] = if bar.count > 0.0 {
            bar.amount / bar.count
        } else {
            0.0
        };
        values[7] = bar.count;
        values[8] = bar.buy_count;
        values[9] = bar.sell_count;
        values[10] = bar.buy_amount;
        values[11] = bar.sell_amount;
        values[12] = bar.buy_volume;
        values[13] = bar.sell_volume;
        if bar.has_trade {
            for value in &mut values[14..23] {
                *value = f64::NAN;
            }
            bar.flags |= QUALITY_ORDER_SIZE_UNAVAILABLE;
        }
        values[23] = if bar.volume > 0.0 {
            bar.amount / bar.volume / self.volume_multiple
        } else if ordinary_empty_bar {
            self.last_vwap.unwrap_or(f64::NAN)
        } else {
            f64::NAN
        };
        values[24] = if bar.buy_volume > 0.0 {
            bar.buy_amount / bar.buy_volume / self.volume_multiple
        } else if ordinary_empty_bar || (bar.volume > 0.0 && bar.volume_valid && bar.turnover_valid)
        {
            self.last_buy_vwap.unwrap_or(f64::NAN)
        } else {
            f64::NAN
        };
        values[25] = if bar.sell_volume > 0.0 {
            bar.sell_amount / bar.sell_volume / self.volume_multiple
        } else if ordinary_empty_bar || (bar.volume > 0.0 && bar.volume_valid && bar.turnover_valid)
        {
            self.last_sell_vwap.unwrap_or(f64::NAN)
        } else {
            f64::NAN
        };
        values[26] = bar.buy_amount - bar.sell_amount;
        values[27] = bar.buy_volume - bar.sell_volume;
        values[28] = if bar.amount > 0.0 {
            values[26] / bar.amount
        } else {
            0.0
        };
        if bar.has_trade {
            values[29] = f64::NAN;
            values[30] = f64::NAN;
            values[31] = f64::NAN;
        }

        if bar.flags & QUALITY_LAST_PRICE_MISSING != 0 {
            values[0] = f64::NAN;
            values[3] = f64::NAN;
        }

        if !bar.volume_valid {
            values[4] = f64::NAN;
            values[7] = f64::NAN;
            values[8] = f64::NAN;
            values[9] = f64::NAN;
            values[10] = f64::NAN;
            values[11] = f64::NAN;
            values[12] = f64::NAN;
            values[13] = f64::NAN;
            values[23] = f64::NAN;
            values[24] = f64::NAN;
            values[25] = f64::NAN;
            values[26] = f64::NAN;
            values[27] = f64::NAN;
            values[28] = f64::NAN;
            for value in &mut values[14..23] {
                *value = f64::NAN;
            }
        }
        if !bar.turnover_valid {
            values[5] = f64::NAN;
            values[6] = f64::NAN;
            values[10] = f64::NAN;
            values[11] = f64::NAN;
            values[23] = f64::NAN;
            values[24] = f64::NAN;
            values[25] = f64::NAN;
            values[26] = f64::NAN;
            values[28] = f64::NAN;
            for value in &mut values[14..23] {
                *value = f64::NAN;
            }
        }

        update_last(&mut self.last_close, values[3]);
        update_last(&mut self.last_vwap, values[23]);
        update_last(&mut self.last_buy_vwap, values[24]);
        update_last(&mut self.last_sell_vwap, values[25]);
        Some(CompletedBar {
            interval_ms: self.interval_ms,
            start_ms: bar.start_ms,
            trading_day: bar.trading_day,
            symbol: bar.symbol,
            trade_values: values,
            quality_flags: bar.flags,
            volume_multiple: self.volume_multiple,
            volume_multiple_verified: self.volume_multiple_verified,
            valid_book: bar.valid_book,
            bid_prices: bar.bid_prices,
            bid_amounts: bar.bid_amounts,
            ask_prices: bar.ask_prices,
            ask_amounts: bar.ask_amounts,
        })
    }
}

fn update_last(target: &mut Option<f64>, value: f64) {
    if positive_finite(value) {
        *target = Some(value);
    }
}

fn align(timestamp_ms: i64, interval_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(interval_ms)
}

#[derive(Debug, Clone)]
struct CompletedBar {
    interval_ms: i64,
    start_ms: i64,
    trading_day: u32,
    symbol: String,
    trade_values: [f64; TRADE_FLOW_FEATURE_DIM],
    quality_flags: u32,
    volume_multiple: f64,
    volume_multiple_verified: bool,
    valid_book: bool,
    bid_prices: [f64; DEPTH_LEVELS],
    bid_amounts: [f64; DEPTH_LEVELS],
    ask_prices: [f64; DEPTH_LEVELS],
    ask_amounts: [f64; DEPTH_LEVELS],
}

#[derive(Debug, Default, Clone, Copy)]
struct ReplayStats {
    source_rows: u64,
    parsed_rows: u64,
    skipped_rows: u64,
    excluded_efp_rows: u64,
    excluded_auction_rows: u64,
    out_of_order_rows: u64,
    trade_5s_rows: u64,
    depth_5s_rows: u64,
    trade_60s_rows: u64,
    depth_60s_rows: u64,
}

impl ReplayStats {
    fn add(&mut self, other: Self) {
        self.source_rows += other.source_rows;
        self.parsed_rows += other.parsed_rows;
        self.skipped_rows += other.skipped_rows;
        self.excluded_efp_rows += other.excluded_efp_rows;
        self.excluded_auction_rows += other.excluded_auction_rows;
        self.out_of_order_rows += other.out_of_order_rows;
        self.trade_5s_rows += other.trade_5s_rows;
        self.depth_5s_rows += other.depth_5s_rows;
        self.trade_60s_rows += other.trade_60s_rows;
        self.depth_60s_rows += other.depth_60s_rows;
    }
}

#[derive(Clone)]
struct StageSenders {
    trade_5s: Sender<Bytes>,
    depth_5s: Sender<Bytes>,
    trade_60s: Sender<Bytes>,
    depth_60s: Sender<Bytes>,
}

impl StageSenders {
    fn send(&self, bar: &CompletedBar, stats: &mut ReplayStats) -> Result<()> {
        let (trade_sender, depth_sender) = match bar.interval_ms {
            5_000 => (&self.trade_5s, &self.depth_5s),
            60_000 => (&self.trade_60s, &self.depth_60s),
            interval => bail!("unsupported completed interval {interval}"),
        };
        trade_sender
            .send(encode_trade_row(bar))
            .map_err(|_| anyhow!("trade ClickHouse writer stopped"))?;
        if bar.interval_ms == 5_000 {
            stats.trade_5s_rows += 1;
        } else {
            stats.trade_60s_rows += 1;
        }
        if bar.valid_book {
            depth_sender
                .send(encode_depth_row(bar))
                .map_err(|_| anyhow!("depth ClickHouse writer stopped"))?;
            if bar.interval_ms == 5_000 {
                stats.depth_5s_rows += 1;
            } else {
                stats.depth_60s_rows += 1;
            }
        }
        Ok(())
    }
}

fn count_dry_run_bar(bar: &CompletedBar, stats: &mut ReplayStats) {
    if bar.interval_ms == 5_000 {
        stats.trade_5s_rows += 1;
    } else {
        stats.trade_60s_rows += 1;
    }
    if bar.valid_book {
        if bar.interval_ms == 5_000 {
            stats.depth_5s_rows += 1;
        } else {
            stats.depth_60s_rows += 1;
        }
    }
}

fn emit_completed(
    completed: Vec<CompletedBar>,
    senders: Option<&StageSenders>,
    stats: &mut ReplayStats,
) -> Result<()> {
    for bar in completed {
        if let Some(senders) = senders {
            senders.send(&bar, stats)?;
        } else {
            count_dry_run_bar(&bar, stats);
        }
    }
    Ok(())
}

fn process_task(
    config: &ReplayConfig,
    task: &ReplayTask,
    volume_multipliers: &VolumeMultiplierCatalog,
    symbol_filter: &HashSet<String>,
    senders: Option<&StageSenders>,
) -> Result<ReplayStats> {
    let started = Instant::now();
    let file = File::open(&task.path)
        .with_context(|| format!("open Tonglian ZIP {}", task.path.display()))?;
    let mut archive = ZipArchive::new(BufReader::new(file))
        .with_context(|| format!("read Tonglian ZIP directory {}", task.path.display()))?;
    if archive.len() != 1 {
        bail!(
            "Tonglian future ZIP must contain exactly one CSV: path={} entries={}",
            task.path.display(),
            archive.len()
        );
    }
    let entry = archive
        .by_index(0)
        .with_context(|| format!("open Tonglian ZIP member {}", task.path.display()))?;
    if !entry.name().to_ascii_lowercase().ends_with(".csv") {
        bail!(
            "Tonglian ZIP member is not CSV: path={} member={}",
            task.path.display(),
            entry.name()
        );
    }
    let mut reader = ReaderBuilder::new().has_headers(true).from_reader(entry);
    let headers = reader
        .headers()
        .with_context(|| format!("read CSV header from {}", task.path.display()))?
        .clone();
    let columns = HeaderMap::from_headers(&headers)?;
    let mut states: HashMap<(String, u32), InstrumentState> = HashMap::new();
    let excluded_local_minutes = parse_excluded_local_minutes(&config.excluded_local_minutes)?;
    let mut stats = ReplayStats::default();

    for record in reader.records() {
        if config
            .max_source_rows_per_file
            .is_some_and(|limit| stats.source_rows >= limit)
        {
            break;
        }
        stats.source_rows += 1;
        let record = match record {
            Ok(record) => record,
            Err(err) => {
                if !config.dry_run {
                    return Err(err).with_context(|| {
                        format!(
                            "malformed Tonglian CSV row: path={} row={}",
                            task.path.display(),
                            stats.source_rows
                        )
                    });
                }
                stats.skipped_rows += 1;
                warn!(
                    "Skipping malformed Tonglian CSV row: path={} row={} error={}",
                    task.path.display(),
                    stats.source_rows,
                    err
                );
                continue;
            }
        };
        let mut snapshot = match Snapshot::parse(&record, &columns) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                if !config.dry_run {
                    return Err(err).with_context(|| {
                        format!(
                            "invalid Tonglian snapshot: path={} row={}",
                            task.path.display(),
                            stats.source_rows
                        )
                    });
                }
                stats.skipped_rows += 1;
                if stats.skipped_rows <= 10 {
                    warn!(
                        "Skipping invalid Tonglian snapshot: path={} row={} error={:#}",
                        task.path.display(),
                        stats.source_rows,
                        err
                    );
                }
                continue;
            }
        };
        if snapshot.trading_day != task.trading_day {
            if !config.dry_run {
                bail!(
                    "Tonglian TradDay mismatch: path={} row={} directory_day={} row_day={}",
                    task.path.display(),
                    stats.source_rows,
                    task.trading_day,
                    snapshot.trading_day
                );
            }
            stats.skipped_rows += 1;
            continue;
        }
        if config.exchange.excludes_efp() && snapshot.symbol.to_ascii_lowercase().contains("efp") {
            stats.excluded_efp_rows += 1;
            continue;
        }
        snapshot.symbol =
            normalize_instrument_id(config.exchange, &snapshot.symbol, snapshot.trading_day)?;
        if !symbol_filter.is_empty() && !symbol_filter.contains(&snapshot.symbol) {
            continue;
        }
        stats.parsed_rows += 1;
        let volume_multiplier =
            volume_multipliers.resolve_or_panic(&snapshot.symbol, snapshot.trading_day);
        let key = (snapshot.symbol.clone(), snapshot.trading_day);
        let state = states.entry(key).or_insert_with(|| {
            InstrumentState::new(volume_multiplier.value, volume_multiplier.verified)
        });
        let include_in_bars = !excluded_local_minutes.contains(&snapshot.local_minute);
        if !include_in_bars {
            stats.excluded_auction_rows += 1;
        }
        let completed =
            match state.on_snapshot(&snapshot, config.max_continuous_gap_ms, include_in_bars) {
                Ok(completed) => completed,
                Err(err)
                    if err.downcast_ref::<OutOfOrderSnapshot>().is_some() && config.dry_run =>
                {
                    stats.out_of_order_rows += 1;
                    if stats.out_of_order_rows <= 10 {
                        warn!("Skipping {err:#}");
                    }
                    continue;
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "process Tonglian snapshot: path={} row={}",
                            task.path.display(),
                            stats.source_rows
                        )
                    });
                }
            };
        emit_completed(completed, senders, &mut stats)?;
        if stats.source_rows % PROGRESS_ROWS == 0 {
            info!(
                "Tonglian replay progress: exchange={} trading_day={} source_rows={} parsed_rows={} bars_5s={} elapsed={:.2?}",
                config.exchange.code(),
                task.trading_day,
                stats.source_rows,
                stats.parsed_rows,
                stats.trade_5s_rows,
                started.elapsed()
            );
        }
    }
    for state in states.values_mut() {
        emit_completed(state.flush(), senders, &mut stats)?;
    }
    info!(
        "Tonglian replay file complete: exchange={} trading_day={} source_rows={} parsed_rows={} skipped={} efp={} auction={} out_of_order={} bars_5s={} bars_60s={} elapsed={:.2?}",
        config.exchange.code(),
        task.trading_day,
        stats.source_rows,
        stats.parsed_rows,
        stats.skipped_rows,
        stats.excluded_efp_rows,
        stats.excluded_auction_rows,
        stats.out_of_order_rows,
        stats.trade_5s_rows,
        stats.trade_60s_rows,
        started.elapsed()
    );
    Ok(stats)
}

#[derive(Debug, Clone)]
struct WriterConfig {
    url: String,
    database: String,
    table: String,
    columns: String,
    batch_rows: usize,
    flush_interval: Duration,
}

#[derive(Debug, Default)]
struct WriterStats {
    rows: u64,
    batches: u64,
}

struct ClickHouseWriter {
    sender: Sender<Bytes>,
    handle: thread::JoinHandle<Result<WriterStats>>,
}

impl ClickHouseWriter {
    fn start(config: WriterConfig, queue_capacity: usize) -> Result<Self> {
        let (sender, receiver) = bounded(queue_capacity);
        let name = format!("{}-writer", config.table);
        let handle = thread::Builder::new()
            .name(name)
            .spawn(move || run_writer(receiver, config))
            .context("spawn Tonglian ClickHouse writer")?;
        Ok(Self { sender, handle })
    }

    fn finish(self) -> Result<WriterStats> {
        drop(self.sender);
        self.handle
            .join()
            .map_err(|_| anyhow!("Tonglian ClickHouse writer panicked"))?
    }
}

struct Writers {
    trade_5s: ClickHouseWriter,
    depth_5s: ClickHouseWriter,
    trade_60s: ClickHouseWriter,
    depth_60s: ClickHouseWriter,
}

impl Writers {
    fn start(config: &ReplayConfig, tables: &TableNames) -> Result<Self> {
        let writer = |table: &str, columns: String| WriterConfig {
            url: config.clickhouse.url.clone(),
            database: config.clickhouse.database.clone(),
            table: table.to_string(),
            columns,
            batch_rows: config.clickhouse.batch_rows,
            flush_interval: Duration::from_millis(config.clickhouse.flush_ms),
        };
        Ok(Self {
            trade_5s: ClickHouseWriter::start(
                writer(&tables.trade_5s, trade_insert_columns_sql()),
                config.clickhouse.queue_capacity,
            )?,
            depth_5s: ClickHouseWriter::start(
                writer(&tables.depth_5s, depth_insert_columns_sql()),
                config.clickhouse.queue_capacity,
            )?,
            trade_60s: ClickHouseWriter::start(
                writer(&tables.trade_60s, trade_insert_columns_sql()),
                config.clickhouse.queue_capacity,
            )?,
            depth_60s: ClickHouseWriter::start(
                writer(&tables.depth_60s, depth_insert_columns_sql()),
                config.clickhouse.queue_capacity,
            )?,
        })
    }

    fn senders(&self) -> StageSenders {
        StageSenders {
            trade_5s: self.trade_5s.sender.clone(),
            depth_5s: self.depth_5s.sender.clone(),
            trade_60s: self.trade_60s.sender.clone(),
            depth_60s: self.depth_60s.sender.clone(),
        }
    }

    fn finish(self) -> Result<[WriterStats; 4]> {
        Ok([
            self.trade_5s.finish()?,
            self.depth_5s.finish()?,
            self.trade_60s.finish()?,
            self.depth_60s.finish()?,
        ])
    }
}

fn run_writer(receiver: Receiver<Bytes>, config: WriterConfig) -> Result<WriterStats> {
    let client = clickhouse_client()?;
    let mut batch = Vec::with_capacity(config.batch_rows);
    let mut stats = WriterStats::default();
    loop {
        match receiver.recv_timeout(config.flush_interval) {
            Ok(row) => {
                batch.push(row);
                if batch.len() >= config.batch_rows {
                    flush_batch(&client, &config, &mut batch, &mut stats)?;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                flush_batch(&client, &config, &mut batch, &mut stats)?;
            }
            Err(RecvTimeoutError::Disconnected) => {
                flush_batch(&client, &config, &mut batch, &mut stats)?;
                return Ok(stats);
            }
        }
    }
}

fn flush_batch(
    client: &reqwest::blocking::Client,
    config: &WriterConfig,
    batch: &mut Vec<Bytes>,
    stats: &mut WriterStats,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut body = Vec::with_capacity(batch.iter().map(Bytes::len).sum());
    for row in batch.iter() {
        body.extend_from_slice(row);
    }
    let query = format!(
        "INSERT INTO {}.{} ({}) FORMAT RowBinary",
        config.database, config.table, config.columns
    );
    let response = client
        .post(config.url.trim_end_matches('/'))
        .query(&[("query", query.as_str())])
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .send()
        .with_context(|| format!("insert into {}.{}", config.database, config.table))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        bail!(
            "ClickHouse insert failed: table={}.{} status={} body={}",
            config.database,
            config.table,
            status,
            body
        );
    }
    stats.rows += batch.len() as u64;
    stats.batches += 1;
    batch.clear();
    Ok(())
}

fn encode_trade_row(bar: &CompletedBar) -> Bytes {
    let mut row = Vec::with_capacity(64 + bar.symbol.len() + TRADE_FLOW_FEATURE_DIM * 8);
    append_row_prefix(&mut row, bar);
    for value in bar.trade_values {
        row.extend_from_slice(&value.to_le_bytes());
    }
    row.extend_from_slice(&bar.quality_flags.to_le_bytes());
    row.extend_from_slice(&bar.volume_multiple.to_le_bytes());
    row.push(u8::from(bar.volume_multiple_verified));
    Bytes::from(row)
}

fn encode_depth_row(bar: &CompletedBar) -> Bytes {
    let mut row = Vec::with_capacity(64 + bar.symbol.len() + DEPTH_LEVELS * 4 * 8);
    append_row_prefix(&mut row, bar);
    for values in [
        &bar.bid_prices,
        &bar.bid_amounts,
        &bar.ask_prices,
        &bar.ask_amounts,
    ] {
        append_var_uint(&mut row, DEPTH_LEVELS as u64);
        for value in values {
            row.extend_from_slice(&value.to_le_bytes());
        }
    }
    Bytes::from(row)
}

fn append_row_prefix(output: &mut Vec<u8>, bar: &CompletedBar) {
    output.extend_from_slice(&bar.start_ms.to_le_bytes());
    append_var_uint(output, bar.symbol.len() as u64);
    output.extend_from_slice(bar.symbol.as_bytes());
    output.extend_from_slice(&bar.trading_day.to_le_bytes());
}

fn append_var_uint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

#[derive(Debug)]
struct TableNames {
    trade_5s: String,
    depth_5s: String,
    trade_60s: String,
    depth_60s: String,
}

impl TableNames {
    fn new(exchange: Exchange) -> Self {
        let prefix = format!("baseline_{}_future", exchange.code());
        Self {
            trade_5s: format!("{prefix}_5s_trade"),
            depth_5s: format!("{prefix}_5s_depth"),
            trade_60s: format!("{prefix}_60s_trade"),
            depth_60s: format!("{prefix}_60s_depth"),
        }
    }

    fn all(&self) -> [&str; 4] {
        [
            &self.trade_5s,
            &self.depth_5s,
            &self.trade_60s,
            &self.depth_60s,
        ]
    }
}

fn trade_columns_sql() -> String {
    let mut columns = common_columns_sql();
    columns.extend(
        TRADE_FLOW_FEATURE_FIELD_NAMES
            .iter()
            .map(|name| format!("{name} Float64")),
    );
    columns.push("quality_flags UInt32".to_string());
    columns.push("volume_multiple Float64".to_string());
    columns.push("volume_multiple_verified UInt8".to_string());
    columns.join(", ")
}

fn trade_insert_columns_sql() -> String {
    ["ts", "symbol", "trading_day"]
        .into_iter()
        .chain(TRADE_FLOW_FEATURE_FIELD_NAMES.iter().copied())
        .chain([
            "quality_flags",
            "volume_multiple",
            "volume_multiple_verified",
        ])
        .collect::<Vec<_>>()
        .join(", ")
}

fn depth_columns_sql() -> String {
    let mut columns = common_columns_sql();
    columns.extend([
        "bid_prices Array(Float64) CODEC(ZSTD)".to_string(),
        "bid_amounts Array(Float64) CODEC(ZSTD)".to_string(),
        "ask_prices Array(Float64) CODEC(ZSTD)".to_string(),
        "ask_amounts Array(Float64) CODEC(ZSTD)".to_string(),
    ]);
    columns.join(", ")
}

fn depth_insert_columns_sql() -> String {
    [
        "ts",
        "symbol",
        "trading_day",
        "bid_prices",
        "bid_amounts",
        "ask_prices",
        "ask_amounts",
    ]
    .join(", ")
}

fn common_columns_sql() -> Vec<String> {
    vec![
        "ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD)".to_string(),
        "symbol String".to_string(),
        "trading_day UInt32".to_string(),
    ]
}

fn ensure_tables(config: &ReplayConfig, tables: &TableNames) -> Result<()> {
    validate_identifier(&config.clickhouse.database)?;
    for table in tables.all() {
        validate_identifier(table)?;
    }
    let client = clickhouse_client()?;
    clickhouse_execute(
        &client,
        &config.clickhouse.url,
        &format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            config.clickhouse.database
        ),
    )?;
    for (table, columns) in [
        (&tables.trade_5s, trade_columns_sql()),
        (&tables.trade_60s, trade_columns_sql()),
        (&tables.depth_5s, depth_columns_sql()),
        (&tables.depth_60s, depth_columns_sql()),
    ] {
        clickhouse_execute(
            &client,
            &config.clickhouse.url,
            &format!(
                "CREATE TABLE IF NOT EXISTS {}.{} ({}) ENGINE = MergeTree PARTITION BY trading_day ORDER BY (symbol, trading_day, ts)",
                config.clickhouse.database, table, columns
            ),
        )?;
    }
    Ok(())
}

fn delete_existing(
    config: &ReplayConfig,
    tables: &TableNames,
    symbol_filter: &HashSet<String>,
) -> Result<()> {
    let start = parse_config_date_u32(&config.start_date)?;
    let end = parse_config_date_u32(&config.end_date)?;
    let client = clickhouse_client()?;
    for table in tables.all() {
        let query = delete_existing_query(
            &config.clickhouse.database,
            table,
            start,
            end,
            symbol_filter,
        );
        clickhouse_execute(&client, &config.clickhouse.url, &query)?;
    }
    Ok(())
}

fn delete_existing_query(
    database: &str,
    table: &str,
    start: u32,
    end: u32,
    symbol_filter: &HashSet<String>,
) -> String {
    let symbol_clause = if symbol_filter.is_empty() {
        String::new()
    } else {
        let mut symbols: Vec<&str> = symbol_filter.iter().map(String::as_str).collect();
        symbols.sort_unstable();
        format!(
            " AND symbol IN ({})",
            symbols
                .into_iter()
                .map(|symbol| format!("'{symbol}'"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "ALTER TABLE {database}.{table} DELETE WHERE trading_day >= {start} AND trading_day <= {end}{symbol_clause} SETTINGS mutations_sync=2"
    )
}

fn clickhouse_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(300))
        .build()
        .context("build ClickHouse client")
}

fn clickhouse_execute(client: &reqwest::blocking::Client, url: &str, query: &str) -> Result<()> {
    let response = client
        .post(url.trim_end_matches('/'))
        .query(&[("query", query)])
        .body(Vec::new())
        .send()
        .with_context(|| format!("ClickHouse query failed: {query}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        bail!("ClickHouse query returned status={status} body={body}: {query}");
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("invalid ClickHouse identifier: {value}");
    }
    Ok(())
}

fn parse_config_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("parse replay date {value}"))
}

fn parse_config_date_u32(value: &str) -> Result<u32> {
    parse_config_date(value)?;
    value
        .replace('-', "")
        .parse::<u32>()
        .with_context(|| format!("parse numeric replay date {value}"))
}

fn discover_tasks(config: &ReplayConfig) -> Result<Vec<ReplayTask>> {
    let start = parse_config_date(&config.start_date)?;
    let end = parse_config_date(&config.end_date)?;
    if start > end {
        bail!("start_date must not be after end_date");
    }
    let exchange_root = config.data_root.join(config.exchange.code());
    let mut tasks = Vec::new();
    for entry in fs::read_dir(&exchange_root)
        .with_context(|| format!("read Tonglian exchange root {}", exchange_root.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let date_text = entry.file_name().to_string_lossy().to_string();
        let Ok(date) = NaiveDate::parse_from_str(&date_text, "%Y%m%d") else {
            continue;
        };
        if date < start || date > end {
            continue;
        }
        let path = entry.path().join(config.exchange.file_name(&date_text));
        if !path.is_file() {
            bail!("missing expected Tonglian future ZIP {}", path.display());
        }
        tasks.push(ReplayTask {
            trading_day: date_text.parse()?,
            path,
        });
    }
    tasks.sort_by_key(|task| task.trading_day);
    if tasks.is_empty() {
        bail!(
            "no Tonglian files found for exchange={} range={}..{} under {}",
            config.exchange.code(),
            config.start_date,
            config.end_date,
            exchange_root.display()
        );
    }
    Ok(tasks)
}

fn normalize_symbol_filter(raw_symbols: &[String]) -> Result<HashSet<String>> {
    let mut symbols = HashSet::new();
    for raw in raw_symbols {
        let symbol = raw.trim().to_ascii_uppercase();
        if symbol.is_empty()
            || !symbol
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            bail!("invalid domestic futures symbol {raw}");
        }
        symbols.insert(symbol);
    }
    Ok(symbols)
}

fn parse_excluded_local_minutes(values: &[String]) -> Result<HashSet<u16>> {
    let mut minutes = HashSet::with_capacity(values.len());
    for value in values {
        let time = NaiveTime::parse_from_str(value.trim(), "%H:%M")
            .with_context(|| format!("parse excluded_local_minutes value {value}"))?;
        minutes.insert((time.hour() * 60 + time.minute()) as u16);
    }
    Ok(minutes)
}

fn normalize_instrument_id(
    exchange: Exchange,
    instrument_id: &str,
    trading_day: u32,
) -> Result<String> {
    let normalized = instrument_id.trim().to_ascii_uppercase();
    if exchange != Exchange::Xzce {
        return Ok(normalized);
    }
    let digit_offset = normalized
        .bytes()
        .position(|byte| byte.is_ascii_digit())
        .unwrap_or(normalized.len());
    let (product, delivery) = normalized.split_at(digit_offset);
    if delivery.len() != 3 || !delivery.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(normalized);
    }
    let trading_date = NaiveDate::parse_from_str(&trading_day.to_string(), "%Y%m%d")
        .with_context(|| format!("parse trading_day={trading_day} for {instrument_id}"))?;
    let year_digit = delivery[0..1].parse::<i32>()?;
    let month = delivery[1..].parse::<u32>()?;
    if !(1..=12).contains(&month) {
        bail!("invalid XZCE delivery month: instrument={instrument_id}");
    }
    let trading_year = trading_date.year();
    let mut delivery_year = trading_year.div_euclid(10) * 10 + year_digit;
    if delivery_year < trading_year - 1 {
        delivery_year += 10;
    } else if delivery_year > trading_year + 8 {
        delivery_year -= 10;
    }
    Ok(format!("{product}{:02}{month:02}", delivery_year % 100))
}

fn product_id_from_instrument(instrument_id: &str) -> Result<&str> {
    let product_len = instrument_id
        .bytes()
        .take_while(u8::is_ascii_alphabetic)
        .count();
    if product_len == 0 {
        bail!("instrument has no alphabetic product prefix: {instrument_id}");
    }
    Ok(&instrument_id[..product_len])
}

fn validate_config(config: &ReplayConfig) -> Result<()> {
    if config.replay_workers == 0 {
        bail!("replay_workers must be > 0");
    }
    if config.max_continuous_gap_ms <= 0 {
        bail!("max_continuous_gap_ms must be > 0");
    }
    let start = parse_config_date(&config.start_date)?;
    let end = parse_config_date(&config.end_date)?;
    if start > end {
        bail!("start_date must not be after end_date");
    }
    let multiplier_config = &config.volume_multiplier_postgres;
    let metadata_start = parse_config_date(&multiplier_config.effective_from)?;
    let metadata_end = parse_config_date(&multiplier_config.effective_to)?;
    if metadata_start >= metadata_end {
        bail!("volume multiplier effective_from must be before effective_to");
    }
    if start < metadata_start || end >= metadata_end {
        bail!(
            "replay range {}..{} is outside volume multiplier coverage [{}, {})",
            config.start_date,
            config.end_date,
            multiplier_config.effective_from,
            multiplier_config.effective_to
        );
    }
    if multiplier_config.host != DEFAULT_MULTIPLIER_PG_HOST {
        bail!(
            "volume multiplier PostgreSQL host must be {}, got {}",
            DEFAULT_MULTIPLIER_PG_HOST,
            multiplier_config.host
        );
    }
    if multiplier_config.port != DEFAULT_MULTIPLIER_PG_PORT {
        bail!(
            "volume multiplier PostgreSQL port must be {}, got {}",
            DEFAULT_MULTIPLIER_PG_PORT,
            multiplier_config.port
        );
    }
    if multiplier_config.user != DEFAULT_MULTIPLIER_PG_USER {
        bail!(
            "volume multiplier PostgreSQL user must be {}, got {}",
            DEFAULT_MULTIPLIER_PG_USER,
            multiplier_config.user
        );
    }
    if multiplier_config.database != DEFAULT_MULTIPLIER_PG_DATABASE {
        bail!(
            "volume multiplier PostgreSQL database must be {}, got {}",
            DEFAULT_MULTIPLIER_PG_DATABASE,
            multiplier_config.database
        );
    }
    if multiplier_config.connect_timeout_secs == 0 {
        bail!("volume multiplier PostgreSQL connect_timeout_secs must be > 0");
    }
    if config.clickhouse.batch_rows == 0 || config.clickhouse.queue_capacity == 0 {
        bail!("ClickHouse batch_rows and queue_capacity must be > 0");
    }
    if !config.dry_run && config.max_source_rows_per_file.is_some() {
        bail!("max_source_rows_per_file is diagnostic-only and requires dry_run=true");
    }
    if config.overwrite_existing && config.dry_run {
        warn!("overwrite_existing is ignored because dry_run=true");
    }
    Ok(())
}

fn replay(config: &ReplayConfig) -> Result<()> {
    validate_config(config)?;
    let tasks = discover_tasks(config)?;
    let symbol_filter = normalize_symbol_filter(&config.symbols)?;
    let tables = TableNames::new(config.exchange);
    let volume_multipliers =
        VolumeMultiplierCatalog::load(config.exchange, &config.volume_multiplier_postgres)?;
    if !volume_multipliers.unverified_products.is_empty() {
        warn!(
            "Unverified PostgreSQL volume multipliers remain for exchange={}: {:?}",
            config.exchange.code(),
            volume_multipliers.unverified_products
        );
    }
    info!(
        "Starting Tonglian baseline replay: exchange={} tasks={} symbols={} dry_run={} max_rows_per_file={:?} segment_gap_ms={} multiplier_table={} multiplier_rows={} multiplier_provenance_sources={} multiplier_fetched_at=[{}, {}] multiplier_coverage=[{}, {}) tables={:?}",
        config.exchange.code(),
        tasks.len(),
        if symbol_filter.is_empty() { "ALL".to_string() } else { symbol_filter.len().to_string() },
        config.dry_run,
        config.max_source_rows_per_file,
        config.max_continuous_gap_ms,
        MULTIPLIER_TABLE,
        volume_multipliers.products.len(),
        volume_multipliers.source_summary(),
        volume_multipliers.fetched_at_min,
        volume_multipliers.fetched_at_max,
        config.volume_multiplier_postgres.effective_from,
        config.volume_multiplier_postgres.effective_to,
        tables,
    );

    let writers = if config.dry_run {
        None
    } else {
        ensure_tables(config, &tables)?;
        if config.overwrite_existing {
            delete_existing(config, &tables, &symbol_filter)?;
        }
        Some(Writers::start(config, &tables)?)
    };
    let senders = writers.as_ref().map(Writers::senders);
    let workers = config.replay_workers.min(tasks.len()).max(1);
    let started = Instant::now();
    let task_stats = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .context("build Tonglian replay worker pool")?
        .install(|| {
            tasks
                .par_iter()
                .map(|task| {
                    process_task(
                        config,
                        task,
                        &volume_multipliers,
                        &symbol_filter,
                        senders.as_ref(),
                    )
                })
                .collect::<Result<Vec<_>>>()
        })?;
    drop(senders);
    let writer_stats = writers.map(Writers::finish).transpose()?;
    let mut total = ReplayStats::default();
    for stats in task_stats {
        total.add(stats);
    }
    info!(
        "Tonglian baseline replay complete: exchange={} files={} source_rows={} parsed_rows={} skipped={} efp={} auction={} out_of_order={} trade_5s={} depth_5s={} trade_60s={} depth_60s={} dry_run={} writer_stats={:?} elapsed={:.2?}",
        config.exchange.code(),
        tasks.len(),
        total.source_rows,
        total.parsed_rows,
        total.skipped_rows,
        total.excluded_efp_rows,
        total.excluded_auction_rows,
        total.out_of_order_rows,
        total.trade_5s_rows,
        total.depth_5s_rows,
        total.trade_60s_rows,
        total.depth_60s_rows,
        config.dry_run,
        writer_stats.map(|stats| stats.map(|stat| (stat.rows, stat.batches))),
        started.elapsed(),
    );
    Ok(())
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config)
        .with_context(|| format!("read replay config {}", args.config.display()))?;
    let mut config: ReplayConfig = toml::from_str(&content)
        .with_context(|| format!("parse replay config {}", args.config.display()))?;
    if args.max_source_rows_per_file.is_some() {
        config.max_source_rows_per_file = args.max_source_rows_per_file;
    }
    replay(&config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn sample_headers() -> StringRecord {
        StringRecord::from(vec![
            "ActionDay",
            "TradDay",
            "UpdateTime",
            "InstruID",
            "LastPrice",
            "HighPrice",
            "LowPrice",
            "Volume",
            "Turnover",
            "BidPrice1",
            "BidVolume1",
            "BidPrice2",
            "BidVolume2",
            "BidPrice3",
            "BidVolume3",
            "BidPrice4",
            "BidVolume4",
            "BidPrice5",
            "BidVolume5",
            "AskPrice1",
            "AskVolume1",
            "AskPrice2",
            "AskVolume2",
            "AskPrice3",
            "AskVolume3",
            "AskPrice4",
            "AskVolume4",
            "AskPrice5",
            "AskVolume5",
        ])
    }

    fn snapshot(ts: i64, volume: f64, turnover: f64, last: f64) -> Snapshot {
        Snapshot {
            timestamp_ms: ts,
            local_minute: 9 * 60,
            trading_day: 20251103,
            symbol: "AP601".to_string(),
            last_price: Some(last),
            high_price: Some(last),
            low_price: Some(last),
            volume: Some(volume),
            turnover: Some(turnover),
            bid_prices: [100.0, 99.0, 98.0, 97.0, 96.0],
            bid_amounts: [1.0; 5],
            ask_prices: [101.0, 102.0, 103.0, 104.0, 105.0],
            ask_amounts: [2.0; 5],
            valid_book: true,
        }
    }

    fn multiplier_record(value: f64, verified: bool) -> VolumeMultiplierRecord {
        VolumeMultiplierRecord {
            resolved: ResolvedVolumeMultiplier { value, verified },
            fetched_at: DateTime::parse_from_rfc3339("2026-08-14T03:53:40Z")
                .unwrap()
                .with_timezone(&Utc),
            effective_from: None,
            effective_to: None,
            source: "fixture".to_string(),
        }
    }
    #[test]
    fn header_adapter_accepts_common_six_exchange_columns() {
        let map = HeaderMap::from_headers(&sample_headers()).expect("header map");
        assert_eq!(map.instrument_id, 3);
        assert_eq!(map.bid_price, [9, 11, 13, 15, 17]);
        assert_eq!(map.ask_volume, [20, 22, 24, 26, 28]);
    }

    #[test]
    fn parses_shanghai_time_as_real_utc_timestamp() {
        let timestamp = parse_shanghai_timestamp("20251103", "09:00:00.500").unwrap();
        assert_eq!(timestamp, 1_762_131_600_500);
    }

    #[test]
    fn cumulative_states_reset_independently() {
        let mut volume = CumulativeState::new(QUALITY_VOLUME_GAP, QUALITY_VOLUME_RESET);
        let mut turnover = CumulativeState::new(QUALITY_TURNOVER_GAP, QUALITY_TURNOVER_RESET);
        assert!(matches!(
            volume.observe(Some(10.0)),
            DeltaObservation::Baseline
        ));
        assert!(matches!(
            turnover.observe(Some(1_000.0)),
            DeltaObservation::Baseline
        ));
        assert!(matches!(
            volume.observe(Some(9.0)),
            DeltaObservation::Invalid(QUALITY_VOLUME_RESET)
        ));
        assert!(matches!(
            turnover.observe(Some(1_200.0)),
            DeltaObservation::Valid(200.0)
        ));
    }

    #[test]
    fn verified_volume_multiplier_converts_vwap_to_quote_units() {
        let mut state = InstrumentState::new(5.0, true);
        assert!(state
            .on_snapshot(&snapshot(0, 3.0, 427_500.0, 28_500.0), 30_000, true)
            .unwrap()
            .is_empty());
        assert!(state
            .on_snapshot(&snapshot(1_000, 6.0, 855_000.0, 28_500.0), 30_000, true)
            .unwrap()
            .is_empty());
        let bars = state.flush();
        let bar = bars.iter().find(|bar| bar.interval_ms == 5_000).unwrap();
        assert_eq!(bar.trade_values[4], 3.0);
        assert_eq!(bar.trade_values[5], 427_500.0);
        assert_eq!(bar.trade_values[23], 28_500.0);
        assert_eq!(bar.volume_multiple, 5.0);
        assert!(bar.volume_multiple_verified);
        assert_eq!(bar.quality_flags & QUALITY_VOLUME_MULTIPLE_ASSUMED, 0);
        assert_eq!(*encode_trade_row(bar).last().unwrap(), 1);
    }

    #[test]
    fn unverified_volume_multiplier_is_explicitly_flagged() {
        let mut state = InstrumentState::new(10.0, false);
        state
            .on_snapshot(&snapshot(0, 1.0, 1_000.0, 100.0), 30_000, true)
            .unwrap();
        let bar = state
            .flush()
            .into_iter()
            .find(|bar| bar.interval_ms == 5_000)
            .unwrap();
        assert!(!bar.volume_multiple_verified);
        assert_ne!(bar.quality_flags & QUALITY_VOLUME_MULTIPLE_ASSUMED, 0);
        assert_eq!(*encode_trade_row(&bar).last().unwrap(), 0);
    }

    #[test]
    fn turnover_failure_does_not_discard_volume_or_ohlc() {
        let mut state = InstrumentState::new(1.0, true);
        state
            .on_snapshot(&snapshot(0, 10.0, 1_000.0, 100.0), 30_000, true)
            .unwrap();
        state
            .on_snapshot(&snapshot(1_000, 12.0, 900.0, 101.0), 30_000, true)
            .unwrap();
        let bars = state.flush();
        let bar = bars.iter().find(|bar| bar.interval_ms == 5_000).unwrap();
        assert_eq!(bar.trade_values[4], 2.0);
        assert_eq!(bar.trade_values[0], 101.0);
        assert!(bar.trade_values[5].is_nan());
        assert!(bar.trade_values[23].is_nan());
        assert_ne!(bar.quality_flags & QUALITY_TURNOVER_RESET, 0);
    }

    #[test]
    fn anomalous_empty_bar_is_not_forward_filled() {
        let mut state = InstrumentState::new(1.0, true);
        state
            .on_snapshot(&snapshot(0, 10.0, 1_000.0, 100.0), 30_000, true)
            .unwrap();
        state
            .on_snapshot(&snapshot(1_000, 11.0, 1_100.0, 100.0), 30_000, true)
            .unwrap();
        state
            .on_snapshot(&snapshot(5_000, 11.0, 900.0, 100.0), 30_000, true)
            .unwrap();
        let bars = state.flush();
        let bar = bars
            .iter()
            .find(|bar| bar.interval_ms == 5_000 && bar.start_ms == 5_000)
            .unwrap();
        assert!(bar.trade_values[0].is_nan());
        assert!(bar.trade_values[3].is_nan());
        assert!(bar.trade_values[5].is_nan());
        assert!(bar.trade_values[23].is_nan());
        assert_ne!(bar.quality_flags & QUALITY_TURNOVER_RESET, 0);
    }

    #[test]
    fn compressed_depth_row_encodes_four_five_value_arrays() {
        let mut state = InstrumentState::new(1.0, true);
        state
            .on_snapshot(&snapshot(0, 0.0, 0.0, 100.0), 30_000, true)
            .unwrap();
        let completed = state
            .on_snapshot(&snapshot(5_000, 0.0, 0.0, 100.0), 30_000, true)
            .unwrap();
        let bar = completed
            .into_iter()
            .chain(state.flush())
            .find(|bar| bar.interval_ms == 5_000 && bar.valid_book)
            .unwrap();
        let encoded = encode_depth_row(&bar);
        let mut cursor = Cursor::new(encoded.as_ref());
        use std::io::Read;
        let mut ts = [0u8; 8];
        cursor.read_exact(&mut ts).unwrap();
        assert_eq!(i64::from_le_bytes(ts), 5_000);
        assert_eq!(read_test_var_uint(&mut cursor), 5);
        let mut symbol = [0u8; 5];
        cursor.read_exact(&mut symbol).unwrap();
        assert_eq!(&symbol, b"AP601");
        let mut day = [0u8; 4];
        cursor.read_exact(&mut day).unwrap();
        assert_eq!(u32::from_le_bytes(day), 20251103);
        for _ in 0..4 {
            assert_eq!(read_test_var_uint(&mut cursor), 5);
            let mut values = [0u8; 40];
            cursor.read_exact(&mut values).unwrap();
        }
        assert_eq!(cursor.position() as usize, encoded.len());
    }

    #[test]
    fn depth_at_bar_start_requires_a_strictly_earlier_snapshot() {
        let boundary_snapshot = snapshot(5_000, 0.0, 0.0, 100.0);
        let boundary_depth = DepthValues::from(&boundary_snapshot);
        let without_causal_depth =
            WorkingBar::new(5_000, &boundary_snapshot, Some(&boundary_depth));
        assert!(!without_causal_depth.valid_book);

        let mut earlier_depth = boundary_depth;
        earlier_depth.timestamp_ms = 4_999;
        let with_causal_depth = WorkingBar::new(5_000, &boundary_snapshot, Some(&earlier_depth));
        assert!(with_causal_depth.valid_book);
        assert_eq!(with_causal_depth.bid_prices[0], 100.0);
    }

    fn read_test_var_uint(reader: &mut impl std::io::Read) -> u64 {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let mut byte = [0u8; 1];
            reader.read_exact(&mut byte).unwrap();
            value |= u64::from(byte[0] & 0x7f) << shift;
            if byte[0] & 0x80 == 0 {
                return value;
            }
        }
        panic!("invalid test varuint")
    }

    #[test]
    fn schemas_have_thirty_two_trade_fields_and_four_arrays() {
        assert_eq!(trade_columns_sql().matches(" Float64").count(), 33);
        assert_eq!(depth_columns_sql().matches("Array(Float64)").count(), 4);
        let trade_insert_columns = trade_insert_columns_sql();
        let trade_columns: Vec<&str> = trade_insert_columns.split(", ").collect();
        assert_eq!(trade_columns.len(), 3 + TRADE_FLOW_FEATURE_DIM + 3);
        assert_eq!(&trade_columns[..3], &["ts", "symbol", "trading_day"]);
        assert_eq!(
            &trade_columns[3..3 + TRADE_FLOW_FEATURE_DIM],
            TRADE_FLOW_FEATURE_FIELD_NAMES
        );
        assert_eq!(
            &trade_columns[3 + TRADE_FLOW_FEATURE_DIM..],
            &[
                "quality_flags",
                "volume_multiple",
                "volume_multiple_verified"
            ]
        );
        assert_eq!(
            depth_insert_columns_sql(),
            "ts, symbol, trading_day, bid_prices, bid_amounts, ask_prices, ask_amounts"
        );
        let tables = TableNames::new(Exchange::Xzce);
        assert_eq!(tables.depth_5s, "baseline_xzce_future_5s_depth");
    }

    #[test]
    fn overwrite_query_is_date_and_symbol_scoped() {
        let symbols = HashSet::from(["AP2601".to_string(), "CF2601".to_string()]);
        assert_eq!(
            delete_existing_query(
                "baseline",
                "baseline_xzce_future_5s_trade",
                20251103,
                20251104,
                &symbols,
            ),
            "ALTER TABLE baseline.baseline_xzce_future_5s_trade DELETE WHERE trading_day >= 20251103 AND trading_day <= 20251104 AND symbol IN ('AP2601', 'CF2601') SETTINGS mutations_sync=2"
        );
    }

    #[test]
    fn formal_replay_rejects_diagnostic_row_limit() {
        let mut config: ReplayConfig =
            toml::from_str(include_str!("../../config/tonglian_baseline_xzce.toml")).unwrap();
        config.dry_run = false;
        config.max_source_rows_per_file = Some(100);
        let error = validate_config(&config).unwrap_err().to_string();
        assert!(error.contains("diagnostic-only"), "{error}");
    }

    #[test]
    fn out_of_order_error_is_typed() {
        let mut state = InstrumentState::new(1.0, true);
        state
            .on_snapshot(&snapshot(2_000, 0.0, 0.0, 100.0), 30_000, true)
            .unwrap();
        let error = state
            .on_snapshot(&snapshot(1_000, 0.0, 0.0, 100.0), 30_000, true)
            .unwrap_err();
        assert!(error.downcast_ref::<OutOfOrderSnapshot>().is_some());
    }

    #[test]
    fn all_exchange_config_templates_parse_as_safe_dry_runs() {
        for path in [
            "../../config/tonglian_baseline_ccfx.toml",
            "../../config/tonglian_baseline_xdce.toml",
            "../../config/tonglian_baseline_xgfe.toml",
            "../../config/tonglian_baseline_xsge.toml",
            "../../config/tonglian_baseline_xsie.toml",
            "../../config/tonglian_baseline_xzce.toml",
        ] {
            let content = match path {
                "../../config/tonglian_baseline_ccfx.toml" => {
                    include_str!("../../config/tonglian_baseline_ccfx.toml")
                }
                "../../config/tonglian_baseline_xdce.toml" => {
                    include_str!("../../config/tonglian_baseline_xdce.toml")
                }
                "../../config/tonglian_baseline_xgfe.toml" => {
                    include_str!("../../config/tonglian_baseline_xgfe.toml")
                }
                "../../config/tonglian_baseline_xsge.toml" => {
                    include_str!("../../config/tonglian_baseline_xsge.toml")
                }
                "../../config/tonglian_baseline_xsie.toml" => {
                    include_str!("../../config/tonglian_baseline_xsie.toml")
                }
                "../../config/tonglian_baseline_xzce.toml" => {
                    include_str!("../../config/tonglian_baseline_xzce.toml")
                }
                _ => unreachable!(),
            };
            let config: ReplayConfig = toml::from_str(content).expect(path);
            assert!(config.dry_run, "{path}");
            assert!(!content.contains("[volume_multiplier_metadata]"), "{path}");
            assert!(!content.contains("[volume_multipliers]"), "{path}");
            assert_eq!(
                config.volume_multiplier_postgres.host, DEFAULT_MULTIPLIER_PG_HOST,
                "{path}"
            );
            assert_eq!(
                config.volume_multiplier_postgres.port, DEFAULT_MULTIPLIER_PG_PORT,
                "{path}"
            );
            assert_eq!(
                config.volume_multiplier_postgres.database, DEFAULT_MULTIPLIER_PG_DATABASE,
                "{path}"
            );
            assert!(!config.excluded_local_minutes.is_empty(), "{path}");
            validate_config(&config).expect(path);
        }
    }

    #[test]
    fn legacy_inline_multiplier_map_is_rejected() {
        let content = format!(
            "{}\n[volume_multipliers]\nAP = 10\n",
            include_str!("../../config/tonglian_baseline_xzce.toml")
        );
        let error = toml::from_str::<ReplayConfig>(&content)
            .unwrap_err()
            .to_string();
        assert!(error.contains("volume_multipliers"), "{error}");
    }

    #[test]
    fn postgres_catalog_resolves_product_and_verification_status() {
        let catalog = VolumeMultiplierCatalog::from_records(
            Exchange::Xzce,
            vec![("AP".to_string(), multiplier_record(10.0, true))],
        )
        .unwrap();
        let resolved = catalog.resolve_or_panic("AP2601", 20251103);
        assert_eq!(resolved.value, 10.0);
        assert!(resolved.verified);
    }

    #[test]
    #[should_panic(expected = "missing volume multiplier")]
    fn missing_postgres_product_panics_without_a_unit_fallback() {
        let catalog = VolumeMultiplierCatalog::from_records(
            Exchange::Xzce,
            vec![("AP".to_string(), multiplier_record(10.0, true))],
        )
        .unwrap();
        let _ = catalog.resolve_or_panic("XX2601", 20251103);
    }

    #[test]
    fn multiplier_query_is_fixed_to_the_canonical_postgres_table() {
        assert_eq!(
            MULTIPLIER_QUERY,
            "SELECT product, exchange, volume_multiple, verified, fetched_at, effective_from, effective_to, source FROM public.domestic_future_product_multipliers WHERE exchange = $1 ORDER BY product"
        );
    }

    #[test]
    fn replay_range_must_stay_inside_multiplier_snapshot_coverage() {
        let mut config: ReplayConfig =
            toml::from_str(include_str!("../../config/tonglian_baseline_xzce.toml")).unwrap();
        config.start_date = "2025-11-04".to_string();
        config.end_date = "2025-11-04".to_string();
        let error = validate_config(&config).unwrap_err().to_string();
        assert!(
            error.contains("outside volume multiplier coverage"),
            "{error}"
        );
    }

    #[test]
    fn expands_xzce_three_digit_delivery_year_from_trading_day() {
        assert_eq!(
            normalize_instrument_id(Exchange::Xzce, "AP601", 20251103).unwrap(),
            "AP2601"
        );
        assert_eq!(
            normalize_instrument_id(Exchange::Xzce, "CF101", 20101101).unwrap(),
            "CF1101"
        );
        assert_eq!(
            normalize_instrument_id(Exchange::Xzce, "AP2601", 20251103).unwrap(),
            "AP2601"
        );
    }
}
