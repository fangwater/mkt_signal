//! Replay fusion baseline factors and optional volatility from ClickHouse bars.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use clap::{Parser, ValueEnum};
use log::info;
use mkt_parsers::msg::trade_flow_feature_msg::{
    TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
};
use mkt_signal::factor_pub::fusion_factor_pub::app::{BaselineReplayState, MAX_SYMBOL_HISTORY};
use mkt_signal::factor_pub::fusion_factor_pub::SymbolFactorPlan;
use order_common::TradingVenue;
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DEPTH_VALUE_COUNT: usize = 80;
const INPUT_VALUE_COUNT: usize = TRADE_FLOW_FEATURE_DIM + DEPTH_VALUE_COUNT;
const PROGRESS_ROWS: u64 = 100_000;
const RL_VOL_COLUMN: &str = "rl_return_volatility";
const DAY_MS: i64 = 86_400_000;

#[derive(Parser, Debug)]
#[command(name = "db_fusion_factor_replay")]
#[command(about = "Compute baseline factors and optional volatility from ClickHouse bars")]
struct Args {
    #[arg(long, default_value = "config/db_fusion_factor_replay.toml")]
    config: PathBuf,
    /// Fetch and validate the configured factor plans without touching ClickHouse.
    #[arg(long)]
    validate_plan_only: bool,
}

#[derive(Debug, Deserialize)]
struct Config {
    venue: String,
    symbols: Vec<String>,
    start_date: String,
    end_date: String,
    #[serde(default)]
    factors: Vec<String>,
    #[serde(default)]
    factor_plan: Option<FactorPlanConfig>,
    #[serde(default = "default_workers")]
    replay_workers: usize,
    /// Optional independent UTC-day chunk size. Every chunk rebuilds its
    /// bounded rolling state from the preceding MAX_SYMBOL_HISTORY rows.
    #[serde(default)]
    replay_chunk_days: Option<usize>,
    /// Bound each chunk's preceding-state query so historical gaps cannot
    /// pull stale rows into a new replay period.
    #[serde(default)]
    replay_warmup_days: Option<usize>,
    #[serde(default)]
    rl_vol: RlVolConfig,
    clickhouse: ClickHouseConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct FactorPlanConfig {
    base_url: String,
    #[serde(default = "default_factor_plan_config_type")]
    config_type: String,
    #[serde(default = "default_factor_plan_timeout_ms")]
    request_timeout_ms: u64,
    /// Compute the ordered union of all selected symbols' factors for every
    /// symbol, instead of leaving factors outside each symbol's plan as NaN.
    #[serde(default)]
    uniform_factor_shape: bool,
}

impl FactorPlanConfig {
    fn validate(&self) -> Result<()> {
        if self.base_url.trim().is_empty() {
            bail!("factor_plan.base_url must not be empty");
        }
        if self.config_type.trim().is_empty() {
            bail!("factor_plan.config_type must not be empty");
        }
        if self.request_timeout_ms == 0 {
            bail!("factor_plan.request_timeout_ms must be > 0");
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct FactorPlanResponse {
    venue: String,
    config_type: String,
    thresholds: HashMap<String, FactorPlanItem>,
}

#[derive(Debug, Deserialize)]
struct FactorPlanItem {
    #[serde(default)]
    factors: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RlVolConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_vol_bar_ms")]
    bar_ms: i64,
    #[serde(default = "default_vol_pct_change_period")]
    pct_change_period: usize,
    #[serde(default = "default_vol_rolling_window")]
    rolling_window: usize,
    #[serde(default = "default_vol_scale_factor")]
    scale_factor: f64,
}

impl Default for RlVolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bar_ms: default_vol_bar_ms(),
            pct_change_period: default_vol_pct_change_period(),
            rolling_window: default_vol_rolling_window(),
            scale_factor: default_vol_scale_factor(),
        }
    }
}

impl RlVolConfig {
    fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.bar_ms <= 0 {
            bail!("rl_vol.bar_ms must be > 0");
        }
        if self.pct_change_period == 0 {
            bail!("rl_vol.pct_change_period must be > 0");
        }
        if self.rolling_window == 0 {
            bail!("rl_vol.rolling_window must be > 0");
        }
        if !self.scale_factor.is_finite() || self.scale_factor <= 0.0 {
            bail!("rl_vol.scale_factor must be finite and > 0");
        }
        self.required_history()?;
        Ok(())
    }

    fn required_history(&self) -> Result<usize> {
        self.pct_change_period
            .checked_add(self.rolling_window)
            .context("rl_vol history length overflow")
    }
}

#[derive(Debug, Deserialize)]
struct ClickHouseConfig {
    url: String,
    input_database: String,
    input_trade_table: String,
    input_depth_table: String,
    output_database: String,
    output_table: String,
    #[serde(default = "default_batch_rows")]
    batch_rows: usize,
}

fn default_workers() -> usize {
    1
}

fn default_batch_rows() -> usize {
    10_000
}

fn default_factor_plan_config_type() -> String {
    "factor_plan".to_string()
}

fn default_factor_plan_timeout_ms() -> u64 {
    10_000
}

fn default_vol_bar_ms() -> i64 {
    5_000
}

fn default_vol_pct_change_period() -> usize {
    12
}

fn default_vol_rolling_window() -> usize {
    30
}

fn default_vol_scale_factor() -> f64 {
    1.3
}

struct InputRow {
    ts_ms: i64,
    symbol: String,
    values: Vec<f64>,
}

struct OutputRow {
    ts_ms: i64,
    symbol: String,
    replay_version: u64,
    values: Vec<f64>,
    rl_return_volatility: Option<f64>,
}

struct SymbolReplayPlan {
    factor_plan: SymbolFactorPlan,
    output_indices: Vec<usize>,
}

struct ReplayPlans {
    output_factors: Vec<String>,
    symbols: HashMap<String, SymbolReplayPlan>,
    source: String,
    uniform_factor_shape: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct ReplayTask {
    symbol: String,
    start_ms: i64,
    end_ms: i64,
}

struct OfflineVolState {
    bar_ms: i64,
    pct_change_period: usize,
    scale_factor: f64,
    rolling_window: usize,
    required_history: usize,
    closes: VecDeque<f64>,
    returns: VecDeque<f64>,
    last_bar_ms: Option<i64>,
    last_close: Option<f64>,
}

impl OfflineVolState {
    fn new(config: &RlVolConfig) -> Result<Self> {
        let required_history = config.required_history()?;
        Ok(Self {
            bar_ms: config.bar_ms,
            pct_change_period: config.pct_change_period,
            scale_factor: config.scale_factor,
            rolling_window: config.rolling_window,
            required_history,
            closes: VecDeque::with_capacity(config.pct_change_period + 1),
            returns: VecDeque::with_capacity(config.rolling_window),
            last_bar_ms: None,
            last_close: None,
        })
    }

    fn push(&mut self, ts_ms: i64, close: f64) -> Result<Option<f64>> {
        if ts_ms.rem_euclid(self.bar_ms) != 0 {
            bail!(
                "rl vol input timestamp is not aligned: ts_ms={} bar_ms={}",
                ts_ms,
                self.bar_ms
            );
        }
        if !close.is_finite() || close <= 0.0 {
            return Ok(None);
        }

        if let (Some(last_bar_ms), Some(last_close)) = (self.last_bar_ms, self.last_close) {
            let elapsed_ms = ts_ms
                .checked_sub(last_bar_ms)
                .context("rl vol timestamp delta overflow")?;
            if elapsed_ms <= 0 {
                bail!(
                    "rl vol input timestamps must be strictly increasing: previous={} current={}",
                    last_bar_ms,
                    ts_ms
                );
            }
            if elapsed_ms % self.bar_ms != 0 {
                bail!(
                    "rl vol input timestamp gap is not divisible by bar_ms: previous={} current={} bar_ms={}",
                    last_bar_ms,
                    ts_ms,
                    self.bar_ms
                );
            }

            let missing_bars = elapsed_ms / self.bar_ms - 1;
            let history_limit = i64::try_from(self.required_history).unwrap_or(i64::MAX);
            let fill_count = missing_bars.min(history_limit) as usize;
            for _ in 0..fill_count {
                self.push_close(last_close);
            }
        }

        let value = self.push_close(close);
        self.last_bar_ms = Some(ts_ms);
        self.last_close = Some(close);
        Ok(value)
    }

    fn push_close(&mut self, close: f64) -> Option<f64> {
        self.closes.push_back(close);
        while self.closes.len() > self.pct_change_period + 1 {
            self.closes.pop_front();
        }
        if self.closes.len() < self.pct_change_period + 1 {
            return None;
        }

        let previous = *self.closes.front().expect("vol close history checked");
        let value = close / previous - 1.0;
        if !value.is_finite() {
            return None;
        }
        self.returns.push_back(value);
        while self.returns.len() > self.rolling_window {
            self.returns.pop_front();
        }
        if self.returns.len() < self.rolling_window || self.returns.len() < 2 {
            return None;
        }

        let mean = self.returns.iter().copied().sum::<f64>() / self.returns.len() as f64;
        let variance = self
            .returns
            .iter()
            .map(|value| {
                let delta = *value - mean;
                delta * delta
            })
            .sum::<f64>()
            / (self.returns.len() as f64 - 1.0);
        let scaled = variance.sqrt() * self.scale_factor;
        scaled.is_finite().then_some(scaled)
    }
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config)
        .with_context(|| format!("read config {}", args.config.display()))?;
    let config: Config = toml::from_str(&content)
        .with_context(|| format!("parse config {}", args.config.display()))?;
    if args.validate_plan_only {
        validate_plan_only(&config)
    } else {
        replay(&config)
    }
}

fn validate_plan_only(config: &Config) -> Result<()> {
    let venue = TradingVenue::from_str(&config.venue, true)
        .map_err(|err| anyhow!("unsupported replay venue '{}': {err}", config.venue))?;
    let symbols = normalize_symbols(&config.symbols)?;
    let plans = resolve_replay_plans(config, venue, &symbols)?;
    println!(
        "validated factor plans: venue={} source={} symbols={} union_columns={} uniform_factor_shape={}",
        venue.data_pub_slug(),
        plans.source,
        plans.symbols.len(),
        plans.output_factors.len(),
        plans.uniform_factor_shape,
    );
    for symbol in symbols {
        let plan = plans
            .symbols
            .get(&symbol)
            .with_context(|| format!("missing resolved factor plan for {symbol}"))?;
        println!(
            "symbol={} factors={}",
            symbol,
            plan.factor_plan.factor_names().len()
        );
    }
    Ok(())
}

fn replay(config: &Config) -> Result<()> {
    let venue = TradingVenue::from_str(&config.venue, true)
        .map_err(|err| anyhow!("unsupported replay venue '{}': {err}", config.venue))?;
    let symbols = normalize_symbols(&config.symbols)?;
    let replay_plans = resolve_replay_plans(config, venue, &symbols)?;
    let factors = &replay_plans.output_factors;
    let (start_ms, end_ms) = date_bounds(&config.start_date, &config.end_date)?;
    config.rl_vol.validate()?;
    if config.replay_workers == 0 {
        bail!("replay_workers must be > 0");
    }
    let tasks = build_replay_tasks(&symbols, start_ms, end_ms, config.replay_chunk_days)?;
    if config.replay_chunk_days.is_some() && config.replay_warmup_days.is_none() {
        bail!("replay_warmup_days is required when replay_chunk_days is configured");
    }
    let warmup_lookback_ms = optional_days_ms("replay_warmup_days", config.replay_warmup_days)?;
    if config.clickhouse.batch_rows == 0 {
        bail!("clickhouse.batch_rows must be > 0");
    }
    validate_identifier(&config.clickhouse.input_database)?;
    validate_identifier(&config.clickhouse.input_trade_table)?;
    validate_identifier(&config.clickhouse.input_depth_table)?;
    validate_identifier(&config.clickhouse.output_database)?;
    validate_identifier(&config.clickhouse.output_table)?;

    let close_field_index = config
        .rl_vol
        .enabled
        .then(|| trade_flow_field_index("close"))
        .transpose()?;
    ensure_output_table(&config.clickhouse, factors, config.rl_vol.enabled)?;
    let replay_version = Utc::now().timestamp_millis().try_into().map_err(|_| {
        anyhow!("system clock is before the Unix epoch; cannot create replay version")
    })?;
    let workers = config.replay_workers.min(tasks.len());
    let started_at = Instant::now();
    info!(
        "Starting database fusion factor replay: venue={} symbols={} tasks={} chunk_days={:?} factor_columns={} factor_plan_source={} uniform_factor_shape={} rl_vol={} workers={} dates={}..{} input={}.{}+{} output={}.{} replay_version={}",
        venue.data_pub_slug(),
        symbols.len(),
        tasks.len(),
        config.replay_chunk_days,
        factors.len(),
        replay_plans.source,
        replay_plans.uniform_factor_shape,
        config.rl_vol.enabled,
        workers,
        config.start_date,
        config.end_date,
        config.clickhouse.input_database,
        config.clickhouse.input_trade_table,
        config.clickhouse.input_depth_table,
        config.clickhouse.output_database,
        config.clickhouse.output_table,
        replay_version,
    );
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .context("build database fusion replay Rayon pool")?
        .install(|| {
            tasks.par_iter().try_for_each(|task| {
                let symbol_plan = replay_plans
                    .symbols
                    .get(&task.symbol)
                    .with_context(|| format!("missing resolved factor plan for {}", task.symbol))?;
                replay_symbol(
                    config,
                    venue,
                    &task.symbol,
                    task.start_ms,
                    task.end_ms,
                    factors,
                    symbol_plan,
                    replay_version,
                    close_field_index,
                    warmup_lookback_ms,
                )
            })
        })?;
    info!(
        "Database fusion factor replay complete: symbols={} tasks={} factors={} elapsed={:.2?}",
        symbols.len(),
        tasks.len(),
        factors.len(),
        started_at.elapsed(),
    );
    Ok(())
}

fn build_replay_tasks(
    symbols: &[String],
    start_ms: i64,
    end_ms: i64,
    chunk_days: Option<usize>,
) -> Result<Vec<ReplayTask>> {
    if start_ms >= end_ms {
        bail!("replay task range must be non-empty");
    }
    let chunk_ms = match chunk_days {
        Some(0) => bail!("replay_chunk_days must be > 0 when configured"),
        Some(days) => i64::try_from(days)
            .context("replay_chunk_days exceeds i64")?
            .checked_mul(DAY_MS)
            .context("replay chunk duration overflow")?,
        None => end_ms - start_ms,
    };
    let mut tasks = Vec::new();
    for symbol in symbols {
        let mut task_start_ms = start_ms;
        while task_start_ms < end_ms {
            let task_end_ms = task_start_ms
                .checked_add(chunk_ms)
                .context("replay task timestamp overflow")?
                .min(end_ms);
            tasks.push(ReplayTask {
                symbol: symbol.clone(),
                start_ms: task_start_ms,
                end_ms: task_end_ms,
            });
            task_start_ms = task_end_ms;
        }
    }
    Ok(tasks)
}

fn optional_days_ms(name: &str, days: Option<usize>) -> Result<Option<i64>> {
    match days {
        Some(0) => bail!("{name} must be > 0 when configured"),
        Some(days) => Ok(Some(
            i64::try_from(days)
                .with_context(|| format!("{name} exceeds i64"))?
                .checked_mul(DAY_MS)
                .with_context(|| format!("{name} duration overflow"))?,
        )),
        None => Ok(None),
    }
}

fn normalize_symbols(raw: &[String]) -> Result<Vec<String>> {
    let mut symbols = Vec::with_capacity(raw.len());
    for raw_symbol in raw {
        let symbol = raw_symbol.trim().to_ascii_uppercase();
        if symbol.is_empty()
            || !symbol
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            bail!("invalid symbol: {raw_symbol}");
        }
        if !symbols.contains(&symbol) {
            symbols.push(symbol);
        }
    }
    if symbols.is_empty() {
        bail!("at least one symbol is required");
    }
    Ok(symbols)
}

fn normalize_factor_names(scope: &str, raw: &[String]) -> Result<Vec<String>> {
    let mut factors = Vec::with_capacity(raw.len());
    for raw_name in raw {
        let name = raw_name.trim().to_string();
        validate_identifier(&name)?;
        if factors.contains(&name) {
            bail!("duplicate factor in {scope}: {name}");
        }
        factors.push(name);
    }
    if factors.is_empty() {
        bail!("at least one factor is required for {scope}");
    }
    Ok(factors)
}

fn resolve_replay_plans(
    config: &Config,
    venue: TradingVenue,
    symbols: &[String],
) -> Result<ReplayPlans> {
    let (factor_names_by_symbol, source, uniform_factor_shape) = match config.factor_plan.as_ref() {
        Some(plan_config) => {
            if !config.factors.is_empty() {
                bail!("config must use either factors or factor_plan, not both");
            }
            plan_config.validate()?;
            let response = load_factor_plan(plan_config, venue)?;
            let mut normalized_thresholds = HashMap::with_capacity(response.thresholds.len());
            for (raw_symbol, item) in response.thresholds {
                let symbol = raw_symbol.trim().to_ascii_uppercase();
                if symbol.is_empty() {
                    continue;
                }
                if normalized_thresholds.insert(symbol.clone(), item).is_some() {
                    bail!("factor plan contains duplicate normalized symbol {symbol}");
                }
            }

            let mut selected = HashMap::with_capacity(symbols.len());
            for symbol in symbols {
                let item = normalized_thresholds
                    .get(symbol)
                    .with_context(|| format!("factor plan response is missing symbol {symbol}"))?;
                let factors = normalize_factor_names(symbol, &item.factors)?;
                selected.insert(symbol.clone(), factors);
            }
            (
                selected,
                format!(
                    "{}/api/thresholds?venue={}&config_type={}",
                    plan_config.base_url.trim_end_matches('/'),
                    venue.data_pub_slug(),
                    plan_config.config_type
                ),
                plan_config.uniform_factor_shape,
            )
        }
        None => {
            let factors = normalize_factor_names("config.factors", &config.factors)?;
            let selected = symbols
                .iter()
                .map(|symbol| (symbol.clone(), factors.clone()))
                .collect();
            (selected, "config.factors".to_string(), false)
        }
    };

    build_replay_plans(
        symbols,
        factor_names_by_symbol,
        source,
        uniform_factor_shape,
    )
}

fn load_factor_plan(config: &FactorPlanConfig, venue: TradingVenue) -> Result<FactorPlanResponse> {
    let url = format!("{}/api/thresholds", config.base_url.trim_end_matches('/'));
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .build()
        .context("build factor plan HTTP client")?;
    let response: FactorPlanResponse = client
        .get(&url)
        .query(&[
            ("venue", venue.data_pub_slug()),
            ("config_type", config.config_type.trim()),
        ])
        .send()
        .with_context(|| format!("GET factor plan failed: {url}"))?
        .error_for_status()
        .with_context(|| format!("factor plan returned error status: {url}"))?
        .json()
        .with_context(|| format!("decode factor plan response failed: {url}"))?;
    if response.venue.trim() != venue.data_pub_slug() {
        bail!(
            "factor plan venue mismatch: requested={} response={}",
            venue.data_pub_slug(),
            response.venue
        );
    }
    if response.config_type.trim() != config.config_type.trim() {
        bail!(
            "factor plan config_type mismatch: requested={} response={}",
            config.config_type,
            response.config_type
        );
    }
    Ok(response)
}

fn build_replay_plans(
    symbols: &[String],
    mut factor_names_by_symbol: HashMap<String, Vec<String>>,
    source: String,
    uniform_factor_shape: bool,
) -> Result<ReplayPlans> {
    let mut output_factors = Vec::new();
    let mut output_index = HashMap::<String, usize>::new();
    let mut resolved = HashMap::with_capacity(symbols.len());

    for symbol in symbols {
        let names = factor_names_by_symbol
            .remove(symbol)
            .with_context(|| format!("missing factor names for {symbol}"))?;
        let factor_plan = SymbolFactorPlan::from_factor_names(symbol, names)?;
        if factor_plan.is_empty() {
            bail!("factor plan is empty for {symbol}");
        }
        BaselineReplayState::validate_factor_plan(&factor_plan)
            .with_context(|| format!("validate factor plan for {symbol}"))?;
        let mut output_indices = Vec::with_capacity(factor_plan.factor_names().len());
        for name in factor_plan.factor_names() {
            let index = match output_index.get(name) {
                Some(index) => *index,
                None => {
                    let index = output_factors.len();
                    output_factors.push(name.to_string());
                    output_index.insert(name.to_string(), index);
                    index
                }
            };
            output_indices.push(index);
        }
        resolved.insert(
            symbol.clone(),
            SymbolReplayPlan {
                factor_plan,
                output_indices,
            },
        );
    }

    if uniform_factor_shape {
        let output_indices = (0..output_factors.len()).collect::<Vec<_>>();
        for symbol in symbols {
            let factor_plan = SymbolFactorPlan::from_factor_names(symbol, output_factors.clone())?;
            BaselineReplayState::validate_factor_plan(&factor_plan)
                .with_context(|| format!("validate uniform factor plan for {symbol}"))?;
            resolved.insert(
                symbol.clone(),
                SymbolReplayPlan {
                    factor_plan,
                    output_indices: output_indices.clone(),
                },
            );
        }
    }

    Ok(ReplayPlans {
        output_factors,
        symbols: resolved,
        source,
        uniform_factor_shape,
    })
}

fn date_bounds(start: &str, end: &str) -> Result<(i64, i64)> {
    let start = NaiveDate::parse_from_str(start, "%Y-%m-%d")
        .with_context(|| format!("parse start_date {start}"))?;
    let end = NaiveDate::parse_from_str(end, "%Y-%m-%d")
        .with_context(|| format!("parse end_date {end}"))?;
    if start > end {
        bail!("start_date must not be after end_date");
    }
    let start_dt = start
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow!("build start timestamp"))?;
    let end_dt = end
        .succ_opt()
        .ok_or_else(|| anyhow!("end_date overflow"))?
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow!("build end timestamp"))?;
    Ok((
        DateTime::<Utc>::from_naive_utc_and_offset(start_dt, Utc).timestamp_millis(),
        DateTime::<Utc>::from_naive_utc_and_offset(end_dt, Utc).timestamp_millis(),
    ))
}

fn replay_symbol(
    config: &Config,
    venue: TradingVenue,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
    factors: &[String],
    symbol_plan: &SymbolReplayPlan,
    replay_version: u64,
    close_field_index: Option<usize>,
    warmup_lookback_ms: Option<i64>,
) -> Result<()> {
    let started_at = Instant::now();
    let client = clickhouse_client()?;
    let warmup_start_ms = warmup_lookback_ms
        .map(|lookback_ms| {
            start_ms
                .checked_sub(lookback_ms)
                .context("warm-up timestamp underflow")
        })
        .transpose()?;
    let warmup_query = prior_rows_query(&config.clickhouse, symbol, warmup_start_ms, start_ms);
    let mut warmup_response = client
        .post(config.clickhouse.url.trim_end_matches('/'))
        .query(&[("query", warmup_query)])
        .body(Vec::new())
        .send()
        .with_context(|| format!("read warm-up baseline rows for {symbol}"))?
        .error_for_status()
        .with_context(|| format!("ClickHouse warm-up query failed for {symbol}"))?;
    let mut warmup_rows = Vec::with_capacity(MAX_SYMBOL_HISTORY);
    while let Some(row) = read_input_row(&mut warmup_response)? {
        warmup_rows.push(row);
    }

    let query = input_query(&config.clickhouse, symbol, start_ms, end_ms);
    let mut response = client
        .post(config.clickhouse.url.trim_end_matches('/'))
        .query(&[("query", query)])
        .body(Vec::new())
        .send()
        .with_context(|| format!("read baseline rows for {symbol}"))?
        .error_for_status()
        .with_context(|| format!("ClickHouse baseline query failed for {symbol}"))?;

    let mut state = BaselineReplayState::default();
    let mut vol_state = config
        .rl_vol
        .enabled
        .then(|| OfflineVolState::new(&config.rl_vol))
        .transpose()?;
    for row in warmup_rows.into_iter().rev() {
        push_input_row(&mut state, venue, symbol, &row)?;
        let _ = state.factor_values(&symbol_plan.factor_plan);
        if let (Some(vol_state), Some(close_field_index)) = (vol_state.as_mut(), close_field_index)
        {
            vol_state.push(row.ts_ms, input_value(&row, close_field_index, "close")?)?;
        }
    }
    let mut batch = Vec::with_capacity(config.clickhouse.batch_rows);
    let mut read_rows = 0u64;
    let mut written_rows = 0u64;
    while let Some(row) = read_input_row(&mut response)? {
        push_input_row(&mut state, venue, symbol, &row)?;
        let symbol_values = state.factor_values(&symbol_plan.factor_plan);
        if symbol_values.len() != symbol_plan.output_indices.len() {
            bail!(
                "factor output width mismatch for {}: values={} indices={}",
                symbol,
                symbol_values.len(),
                symbol_plan.output_indices.len()
            );
        }
        let mut values = vec![f64::NAN; factors.len()];
        for (value, output_index) in symbol_values
            .into_iter()
            .zip(symbol_plan.output_indices.iter().copied())
        {
            values[output_index] = value;
        }
        let rl_return_volatility = match (vol_state.as_mut(), close_field_index) {
            (Some(vol_state), Some(close_field_index)) => {
                vol_state.push(row.ts_ms, input_value(&row, close_field_index, "close")?)?
            }
            _ => None,
        };
        read_rows = read_rows.saturating_add(1);
        batch.push(OutputRow {
            ts_ms: row.ts_ms,
            symbol: symbol.to_string(),
            replay_version,
            values,
            rl_return_volatility,
        });
        if batch.len() >= config.clickhouse.batch_rows {
            written_rows = written_rows.saturating_add(flush_output_batch(
                &client,
                &config.clickhouse,
                factors,
                config.rl_vol.enabled,
                &mut batch,
            )?);
        }
        if read_rows % PROGRESS_ROWS == 0 {
            info!(
                "Database fusion replay progress: symbol={} start_ms={} end_ms={} rows_read={} rows_written={} elapsed={:.2?}",
                symbol, start_ms, end_ms, read_rows, written_rows, started_at.elapsed(),
            );
        }
    }
    if read_rows == 0 {
        bail!(
            "no baseline rows for symbol={} in task range {}..{}",
            symbol,
            start_ms,
            end_ms
        );
    }
    written_rows = written_rows.saturating_add(flush_output_batch(
        &client,
        &config.clickhouse,
        factors,
        config.rl_vol.enabled,
        &mut batch,
    )?);
    info!(
        "Database fusion replay complete: symbol={} start_ms={} end_ms={} rows_read={} rows_written={} elapsed={:.2?}",
        symbol, start_ms, end_ms,
        read_rows,
        written_rows,
        started_at.elapsed(),
    );
    Ok(())
}

fn trade_flow_field_index(name: &str) -> Result<usize> {
    TRADE_FLOW_FEATURE_FIELD_NAMES
        .iter()
        .position(|candidate| *candidate == name)
        .with_context(|| format!("missing trade-flow input field {name}"))
}

fn input_value(row: &InputRow, index: usize, name: &str) -> Result<f64> {
    row.values
        .get(index)
        .copied()
        .with_context(|| format!("baseline row missing {name} at index {index}"))
}

fn push_input_row(
    state: &mut BaselineReplayState,
    venue: TradingVenue,
    expected_symbol: &str,
    row: &InputRow,
) -> Result<()> {
    if row.symbol != expected_symbol {
        bail!(
            "source query returned unexpected symbol {} for {expected_symbol}",
            row.symbol
        );
    }
    let message = TradeFlowFeatureMsg::from_indexed_values(
        row.symbol.clone(),
        venue.to_u8(),
        row.ts_ms,
        &row.values,
    )
    .context("build trade-flow feature message from baseline row")?;
    state.push(message)
}

fn input_query(config: &ClickHouseConfig, symbol: &str, start_ms: i64, end_ms: i64) -> String {
    format!(
        "SELECT toUnixTimestamp64Milli(t.ts), t.symbol, {} FROM {}.{} AS t INNER JOIN {}.{} AS d USING (symbol, ts) WHERE t.symbol = '{}' AND t.ts >= fromUnixTimestamp64Milli({}) AND t.ts < fromUnixTimestamp64Milli({}) ORDER BY t.ts FORMAT RowBinary",
        input_columns_sql(),
        config.input_database,
        config.input_trade_table,
        config.input_database,
        config.input_depth_table,
        symbol,
        start_ms,
        end_ms,
    )
}

fn prior_rows_query(
    config: &ClickHouseConfig,
    symbol: &str,
    warmup_start_ms: Option<i64>,
    start_ms: i64,
) -> String {
    let lower_bound = warmup_start_ms
        .map(|warmup_start_ms| {
            format!(" AND t.ts >= fromUnixTimestamp64Milli({})", warmup_start_ms)
        })
        .unwrap_or_default();
    format!(
        "SELECT toUnixTimestamp64Milli(t.ts), t.symbol, {} FROM {}.{} AS t INNER JOIN {}.{} AS d USING (symbol, ts) WHERE t.symbol = '{}'{} AND t.ts < fromUnixTimestamp64Milli({}) ORDER BY t.ts DESC LIMIT {} FORMAT RowBinary",
        input_columns_sql(),
        config.input_database,
        config.input_trade_table,
        config.input_database,
        config.input_depth_table,
        symbol,
        lower_bound,
        start_ms,
        MAX_SYMBOL_HISTORY,
    )
}

fn input_columns_sql() -> String {
    let mut columns: Vec<String> = TRADE_FLOW_FEATURE_FIELD_NAMES
        .iter()
        .map(|name| format!("t.{name}"))
        .collect();
    for side in ["bid", "ask"] {
        for level in 0..20 {
            columns.push(format!("d.{side}_{level:02}_price"));
            columns.push(format!("d.{side}_{level:02}_amount"));
        }
    }
    columns.join(", ")
}

fn read_input_row(reader: &mut impl Read) -> Result<Option<InputRow>> {
    let Some(ts_ms) = read_i64_or_eof(reader)? else {
        return Ok(None);
    };
    let symbol = read_string(reader)?;
    let mut values = Vec::with_capacity(INPUT_VALUE_COUNT);
    for _ in 0..INPUT_VALUE_COUNT {
        values.push(read_f64(reader)?);
    }
    Ok(Some(InputRow {
        ts_ms,
        symbol,
        values,
    }))
}

fn read_i64_or_eof(reader: &mut impl Read) -> Result<Option<i64>> {
    let mut bytes = [0u8; 8];
    match reader.read(&mut bytes[..1]) {
        Ok(0) => Ok(None),
        Ok(_) => {
            reader.read_exact(&mut bytes[1..])?;
            Ok(Some(i64::from_le_bytes(bytes)))
        }
        Err(err) => Err(err.into()),
    }
}

fn read_f64(reader: &mut impl Read) -> Result<f64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(f64::from_le_bytes(bytes))
}

fn read_string(reader: &mut impl Read) -> Result<String> {
    let len = read_var_uint(reader)? as usize;
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes)?;
    String::from_utf8(bytes).context("decode RowBinary symbol")
}

fn read_var_uint(reader: &mut impl Read) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        value |= ((byte[0] & 0x7f) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("RowBinary varuint is too long")
}

fn ensure_output_table(
    config: &ClickHouseConfig,
    factors: &[String],
    include_rl_vol: bool,
) -> Result<()> {
    let client = clickhouse_client()?;
    clickhouse_execute(
        &client,
        &config.url,
        &format!("CREATE DATABASE IF NOT EXISTS {}", config.output_database),
    )?;
    let columns = output_columns_sql(factors, include_rl_vol);
    clickhouse_execute(
        &client,
        &config.url,
        &format!(
            "CREATE TABLE IF NOT EXISTS {}.{} ({columns}) ENGINE = ReplacingMergeTree(replay_version) ORDER BY (symbol, ts)",
            config.output_database, config.output_table
        ),
    )?;
    ensure_config_covers_output_columns(&client, config, factors, include_rl_vol)?;
    for factor in factors {
        clickhouse_execute(
            &client,
            &config.url,
            &format!(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} Float64",
                config.output_database, config.output_table, factor
            ),
        )?;
    }
    if include_rl_vol {
        clickhouse_execute(
            &client,
            &config.url,
            &format!(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} Nullable(Float64)",
                config.output_database, config.output_table, RL_VOL_COLUMN
            ),
        )?;
        ensure_output_column_type(&client, config, RL_VOL_COLUMN, "Nullable(Float64)")?;
    }
    Ok(())
}

fn ensure_output_column_type(
    client: &reqwest::blocking::Client,
    config: &ClickHouseConfig,
    column: &str,
    expected_type: &str,
) -> Result<()> {
    let query = format!(
        "SELECT type FROM system.columns WHERE database = '{}' AND table = '{}' AND name = '{}' FORMAT TabSeparatedRaw",
        config.output_database, config.output_table, column
    );
    let actual_type = client
        .post(config.url.trim_end_matches('/'))
        .query(&[("query", query)])
        .body(Vec::new())
        .send()
        .with_context(|| format!("read output type for column {column}"))?
        .error_for_status()
        .with_context(|| format!("output type query failed for column {column}"))?
        .text()
        .with_context(|| format!("read output type response for column {column}"))?;
    let actual_type = actual_type.trim();
    if actual_type != expected_type {
        bail!(
            "output column {}.{}.{} must be {}, got {}",
            config.output_database,
            config.output_table,
            column,
            expected_type,
            if actual_type.is_empty() {
                "<missing>"
            } else {
                actual_type
            }
        );
    }
    Ok(())
}

fn ensure_config_covers_output_columns(
    client: &reqwest::blocking::Client,
    config: &ClickHouseConfig,
    factors: &[String],
    include_rl_vol: bool,
) -> Result<()> {
    let query = format!(
        "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}' AND name NOT IN ('ts', 'symbol', 'replay_version') ORDER BY name FORMAT TabSeparatedRaw",
        config.output_database, config.output_table
    );
    let response = client
        .post(config.url.trim_end_matches('/'))
        .query(&[("query", query)])
        .body(Vec::new())
        .send()
        .context("read database fusion output columns")?
        .error_for_status()
        .context("database fusion output column query failed")?
        .text()
        .context("read database fusion output columns response")?;
    let missing: Vec<&str> = response
        .lines()
        .filter(|name| {
            !factors.iter().any(|factor| factor == name)
                && !(include_rl_vol && *name == RL_VOL_COLUMN)
        })
        .collect();
    if !missing.is_empty() {
        bail!(
            "output table contains columns absent from the replay config: {}; include all existing factors and enable rl_vol when present so their values are retained",
            missing.join(",")
        );
    }
    Ok(())
}

fn output_columns_sql(factors: &[String], include_rl_vol: bool) -> String {
    let mut columns = vec![
        "ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD)".to_string(),
        "symbol String".to_string(),
        "replay_version UInt64".to_string(),
    ];
    columns.extend(factors.iter().map(|name| format!("{name} Float64")));
    if include_rl_vol {
        columns.push(format!("{RL_VOL_COLUMN} Nullable(Float64)"));
    }
    columns.join(", ")
}

fn flush_output_batch(
    client: &reqwest::blocking::Client,
    config: &ClickHouseConfig,
    factors: &[String],
    include_rl_vol: bool,
    batch: &mut Vec<OutputRow>,
) -> Result<u64> {
    if batch.is_empty() {
        return Ok(0);
    }
    let vol_bytes_per_row = if include_rl_vol { 9 } else { 0 };
    let mut body = Vec::with_capacity(batch.len() * (40 + factors.len() * 8 + vol_bytes_per_row));
    for row in batch.iter() {
        body.extend_from_slice(&row.ts_ms.to_le_bytes());
        append_var_uint(&mut body, row.symbol.len() as u64);
        body.extend_from_slice(row.symbol.as_bytes());
        body.extend_from_slice(&row.replay_version.to_le_bytes());
        if row.values.len() != factors.len() {
            bail!("baseline output width mismatch: {}", row.values.len());
        }
        for value in &row.values {
            body.extend_from_slice(&value.to_le_bytes());
        }
        if include_rl_vol {
            append_nullable_f64(&mut body, row.rl_return_volatility);
        }
    }
    let mut output_columns = factors.to_vec();
    if include_rl_vol {
        output_columns.push(RL_VOL_COLUMN.to_string());
    }
    let query = format!(
        "INSERT INTO {}.{} (ts, symbol, replay_version, {}) FORMAT RowBinary",
        config.output_database,
        config.output_table,
        output_columns.join(", "),
    );
    client
        .post(config.url.trim_end_matches('/'))
        .query(&[("query", query)])
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .send()
        .context("insert database fusion factor batch")?
        .error_for_status()
        .context("database fusion factor insert failed")?;
    let rows = batch.len() as u64;
    batch.clear();
    Ok(rows)
}

fn append_nullable_f64(output: &mut Vec<u8>, value: Option<f64>) {
    match value {
        Some(value) => {
            output.push(0);
            output.extend_from_slice(&value.to_le_bytes());
        }
        None => output.push(1),
    }
}

fn clickhouse_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(300))
        .build()
        .context("build ClickHouse HTTP client")
}

fn clickhouse_execute(client: &reqwest::blocking::Client, url: &str, query: &str) -> Result<()> {
    client
        .post(url.trim_end_matches('/'))
        .query(&[("query", query)])
        .body(Vec::new())
        .send()
        .with_context(|| format!("ClickHouse query failed: {query}"))?
        .error_for_status()
        .with_context(|| format!("ClickHouse query returned error: {query}"))?;
    Ok(())
}

fn append_var_uint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
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

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_signal::factor_pub::rl_vol::compute_rl_return_volatility;
    use std::io::Cursor;

    fn enabled_vol_config(
        pct_change_period: usize,
        rolling_window: usize,
        scale_factor: f64,
    ) -> RlVolConfig {
        RlVolConfig {
            enabled: true,
            bar_ms: 5_000,
            pct_change_period,
            rolling_window,
            scale_factor,
        }
    }

    #[test]
    fn reads_rowbinary_baseline_row() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&123_i64.to_le_bytes());
        append_var_uint(&mut bytes, 7);
        bytes.extend_from_slice(b"BTCUSDT");
        for index in 0..INPUT_VALUE_COUNT {
            bytes.extend_from_slice(&(index as f64).to_le_bytes());
        }
        let row = read_input_row(&mut Cursor::new(bytes))
            .expect("read row")
            .expect("row");
        assert_eq!(row.ts_ms, 123);
        assert_eq!(row.symbol, "BTCUSDT");
        assert_eq!(row.values.len(), INPUT_VALUE_COUNT);
        assert_eq!(
            row.values[INPUT_VALUE_COUNT - 1],
            (INPUT_VALUE_COUNT - 1) as f64
        );
    }

    #[test]
    fn output_schema_matches_selected_baselines() {
        let columns = output_columns_sql(
            &["baseline_042".to_string(), "baseline_118".to_string()],
            true,
        );
        assert_eq!(columns.matches(" Float64").count(), 2);
        assert!(columns.contains("replay_version UInt64"));
        assert!(columns.contains("baseline_042 Float64"));
        assert!(columns.contains("baseline_118 Float64"));
        assert!(columns.contains("rl_return_volatility Nullable(Float64)"));
        assert!(!columns.contains("baseline_001 Float64"));

        let columns = output_columns_sql(&["baseline_118".to_string()], false);
        assert!(!columns.contains(RL_VOL_COLUMN));
    }

    #[test]
    fn rowbinary_encodes_nullable_vol() {
        let mut bytes = Vec::new();
        append_nullable_f64(&mut bytes, None);
        assert_eq!(bytes, [1]);

        bytes.clear();
        append_nullable_f64(&mut bytes, Some(1.25));
        assert_eq!(bytes[0], 0);
        assert_eq!(&bytes[1..], &1.25_f64.to_le_bytes());
    }

    #[test]
    fn offline_vol_matches_batch_formula() {
        let config = enabled_vol_config(12, 30, 1.3);
        let mut state = OfflineVolState::new(&config).expect("vol state");
        let mut closes = VecDeque::new();
        let required = config.required_history().expect("history");

        for index in 0..80 {
            let close = 100.0 + (index as f64 / 3.0).sin() * 4.0 + index as f64 * 0.01;
            closes.push_back(close);
            while closes.len() > required {
                closes.pop_front();
            }
            let expected = compute_rl_return_volatility(
                &closes,
                config.pct_change_period,
                config.rolling_window,
            )
            .expect("batch vol")
            .map(|value| value * config.scale_factor);
            let actual = state
                .push(index as i64 * config.bar_ms, close)
                .expect("offline vol");

            match (actual, expected) {
                (Some(actual), Some(expected)) => assert_eq!(
                    actual, expected,
                    "batch and streaming windows differ at index={index}"
                ),
                (None, None) => {}
                pair => panic!("vol readiness mismatch at index={index}: {pair:?}"),
            }
        }
    }

    #[test]
    fn offline_vol_uses_prior_rows_as_warmup() {
        let config = enabled_vol_config(2, 3, 1.3);
        let mut state = OfflineVolState::new(&config).expect("vol state");
        for index in 0..4 {
            assert!(state
                .push(index * config.bar_ms, 100.0 + index as f64)
                .expect("warmup row")
                .is_none());
        }

        assert!(state
            .push(4 * config.bar_ms, 104.0)
            .expect("first target row")
            .is_some());
    }

    #[test]
    fn offline_vol_forward_fills_missing_bars() {
        let config = enabled_vol_config(1, 2, 1.0);
        let mut state = OfflineVolState::new(&config).expect("vol state");
        assert!(state.push(0, 100.0).expect("first bar").is_none());

        let actual = state
            .push(15_000, 110.0)
            .expect("bar after gap")
            .expect("vol after forward fill");
        let closes = VecDeque::from([100.0, 100.0, 110.0]);
        let expected = compute_rl_return_volatility(&closes, 1, 2)
            .expect("batch vol")
            .expect("ready batch vol");
        assert!((actual - expected).abs() < 1e-12);
    }

    #[test]
    fn offline_vol_rejects_duplicate_or_unaligned_timestamps() {
        let config = enabled_vol_config(1, 2, 1.0);
        let mut state = OfflineVolState::new(&config).expect("vol state");
        state.push(0, 100.0).expect("first bar");
        assert!(state.push(0, 101.0).is_err());

        let mut state = OfflineVolState::new(&config).expect("vol state");
        assert!(state.push(1, 100.0).is_err());
    }

    #[test]
    fn accepts_supported_static_factor_names_and_rejects_duplicates() {
        let factors = normalize_factor_names(
            "test",
            &[
                "baseline_042".to_string(),
                "factor_018".to_string(),
                "TD_TI_015".to_string(),
                "avg_price".to_string(),
            ],
        )
        .expect("valid selected factors");
        assert_eq!(
            factors,
            ["baseline_042", "factor_018", "TD_TI_015", "avg_price"]
        );
        SymbolFactorPlan::from_factor_names("test", factors).expect("mapped factors");
        assert!(normalize_factor_names(
            "test",
            &["factor_001".to_string(), "factor_001".to_string()]
        )
        .is_err());
        assert!(
            SymbolFactorPlan::from_factor_names("test", vec!["unknown_factor".to_string()])
                .is_err()
        );
    }

    #[test]
    fn symbol_factor_plans_build_a_stable_union() {
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let plans = build_replay_plans(
            &symbols,
            HashMap::from([
                (
                    "BTCUSDT".to_string(),
                    vec!["factor_018".to_string(), "baseline_118".to_string()],
                ),
                (
                    "ETHUSDT".to_string(),
                    vec!["baseline_118".to_string(), "TD_TI_015".to_string()],
                ),
            ]),
            "test".to_string(),
            false,
        )
        .expect("replay plans");

        assert_eq!(
            plans.output_factors,
            ["factor_018", "baseline_118", "TD_TI_015"]
        );
        assert_eq!(plans.symbols["BTCUSDT"].output_indices, [0_usize, 1_usize]);
        assert_eq!(plans.symbols["ETHUSDT"].output_indices, [1_usize, 2_usize]);
        assert!(!plans.uniform_factor_shape);
    }

    #[test]
    fn uniform_factor_shape_computes_the_union_for_every_symbol() {
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let plans = build_replay_plans(
            &symbols,
            HashMap::from([
                (
                    "BTCUSDT".to_string(),
                    vec!["factor_018".to_string(), "baseline_118".to_string()],
                ),
                (
                    "ETHUSDT".to_string(),
                    vec!["baseline_118".to_string(), "TD_TI_015".to_string()],
                ),
            ]),
            "test".to_string(),
            true,
        )
        .expect("uniform replay plans");

        assert_eq!(
            plans.output_factors,
            ["factor_018", "baseline_118", "TD_TI_015"]
        );
        assert!(plans.uniform_factor_shape);
        for symbol in symbols {
            let plan = &plans.symbols[&symbol];
            assert_eq!(
                plan.factor_plan.factor_names().collect::<Vec<_>>(),
                ["factor_018", "baseline_118", "TD_TI_015"]
            );
            assert_eq!(plan.output_indices, [0_usize, 1_usize, 2_usize]);
        }
    }

    #[test]
    fn date_bounds_are_inclusive() {
        let (start, end) = date_bounds("2026-06-15", "2026-07-15").expect("dates");
        assert_eq!(end - start, 31 * 24 * 60 * 60 * 1_000);
    }

    #[test]
    fn replay_tasks_default_to_symbols_and_split_into_contiguous_days() {
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let start_ms = 1_000_000_000;
        let end_ms = start_ms + 3 * DAY_MS;

        let whole = build_replay_tasks(&symbols, start_ms, end_ms, None).expect("whole tasks");
        assert_eq!(
            whole,
            [
                ReplayTask {
                    symbol: "BTCUSDT".to_string(),
                    start_ms,
                    end_ms,
                },
                ReplayTask {
                    symbol: "ETHUSDT".to_string(),
                    start_ms,
                    end_ms,
                },
            ]
        );

        let daily =
            build_replay_tasks(&symbols[..1], start_ms, end_ms, Some(1)).expect("daily tasks");
        assert_eq!(daily.len(), 3);
        for (index, task) in daily.iter().enumerate() {
            assert_eq!(task.start_ms, start_ms + index as i64 * DAY_MS);
            assert_eq!(task.end_ms, task.start_ms + DAY_MS);
        }
        assert!(build_replay_tasks(&symbols, start_ms, end_ms, Some(0)).is_err());
        assert_eq!(optional_days_ms("warmup", Some(1)).unwrap(), Some(DAY_MS));
        assert!(optional_days_ms("warmup", Some(0)).is_err());
    }

    #[test]
    fn replay_config_template_parses() {
        let config: Config =
            toml::from_str(include_str!("../../config/db_fusion_factor_replay.toml"))
                .expect("replay config template");
        assert_eq!(config.venue, "binance-futures");
        assert_eq!(
            config.symbols,
            ["XRPUSDT", "DOGEUSDT", "SOLUSDT", "ETHUSDT", "BTCUSDT", "BNBUSDT"]
        );
        assert_eq!(config.start_date, "2025-12-29");
        assert_eq!(config.end_date, "2026-07-31");
        assert!(config.factors.is_empty());
        let factor_plan = config
            .factor_plan
            .as_ref()
            .expect("live factor plan config");
        assert_eq!(factor_plan.base_url, "http://52.69.209.108:6322");
        assert_eq!(factor_plan.config_type, "factor_plan");
        assert!(factor_plan.uniform_factor_shape);
        assert_eq!(config.replay_workers, 48);
        assert_eq!(config.replay_chunk_days, Some(1));
        assert_eq!(config.replay_warmup_days, Some(1));
        let warmup_query = prior_rows_query(&config.clickhouse, "XRPUSDT", Some(1_000), 2_000);
        assert!(warmup_query.contains("t.ts >= fromUnixTimestamp64Milli(1000)"));
        assert!(warmup_query.contains("t.ts < fromUnixTimestamp64Milli(2000)"));
        assert!(config.rl_vol.enabled);
        assert_eq!(config.rl_vol.bar_ms, 5_000);
        assert_eq!(config.rl_vol.pct_change_period, 12);
        assert_eq!(config.rl_vol.rolling_window, 30);
        assert_eq!(config.rl_vol.scale_factor, 1.3);
        assert_eq!(
            config.clickhouse.input_trade_table,
            "baseline_binance_futures_5s_trade"
        );
        assert_eq!(
            config.clickhouse.input_depth_table,
            "baseline_binance_futures_5s_depth"
        );
        assert_eq!(
            config.clickhouse.output_table,
            "fusion_factor_binance_futures_5s"
        );

        let config: Config = toml::from_str(include_str!(
            "../../config/db_fusion_factor_replay_60s.toml"
        ))
        .expect("60s replay config template");
        assert_eq!(config.replay_chunk_days, None);
        assert_eq!(config.replay_warmup_days, None);
        assert!(config.factor_plan.is_none());
        assert_eq!(config.factors, ["baseline_118"]);
        assert!(!config.rl_vol.enabled);
        assert_eq!(
            config.clickhouse.input_trade_table,
            "baseline_binance_futures_60s_trade"
        );
        assert_eq!(
            config.clickhouse.input_depth_table,
            "baseline_binance_futures_60s_depth"
        );
        assert_eq!(
            config.clickhouse.output_table,
            "fusion_factor_binance_futures_60s"
        );
    }
}
