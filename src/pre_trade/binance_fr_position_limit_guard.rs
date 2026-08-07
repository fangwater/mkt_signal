use account_common::BinanceAccountMode;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use hmac::{Hmac, Mac};
use log::{debug, info, warn};
use order_common::{Side, TradingVenue};
use reqwest::Client;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use runtime_common::redis_client::{RedisClient, RedisSettings};
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use serde_json::Value;
use sha2::Sha256;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use trade_signal::ArbMode;

use crate::pre_trade::params_load::PreTradeParamsLoader;

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 10;
const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 30 * 60;
const BLOCK_SUMMARY_INTERVAL_US: i64 = 60_000_000;

thread_local! {
    static BINANCE_FR_POSITION_LIMIT_GUARD: RefCell<Option<BinanceFrPositionLimitState>> =
        const { RefCell::new(None) };
}

#[derive(Debug, Clone)]
struct BinanceFrPositionLimit {
    max_notional_value: f64,
    leverage: Option<f64>,
    current_notional: Option<f64>,
    bracket_notional_cap: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct BinanceFrPositionLimitCap {
    pub symbol: String,
    pub max_notional_value: f64,
    pub buffer: f64,
    pub cap: f64,
    pub amount_u: f64,
    pub pending_limit_orders: i32,
    pub leverage: Option<f64>,
    pub current_notional: Option<f64>,
    pub bracket_notional_cap: Option<f64>,
}

#[derive(Debug, Default)]
struct BlockCounter {
    count: u64,
    last_reason: String,
}

#[derive(Debug)]
struct BlockStats {
    last_log_us: i64,
    total: u64,
    by_symbol: BTreeMap<String, BlockCounter>,
}

impl BlockStats {
    fn new(now_us: i64) -> Self {
        Self {
            last_log_us: now_us,
            total: 0,
            by_symbol: BTreeMap::new(),
        }
    }

    fn record(&mut self, symbol: String, reason: String) {
        self.total = self.total.saturating_add(1);
        let entry = self.by_symbol.entry(symbol).or_default();
        entry.count = entry.count.saturating_add(1);
        entry.last_reason = reason;
    }

    fn maybe_log(&mut self, now_us: i64) {
        if self.total == 0 {
            self.last_log_us = now_us;
            return;
        }
        if now_us.saturating_sub(self.last_log_us) < BLOCK_SUMMARY_INTERVAL_US {
            return;
        }

        let mut parts = Vec::new();
        for (symbol, counter) in self.by_symbol.iter().take(12) {
            parts.push(format!(
                "{} count={} reason={}",
                symbol, counter.count, counter.last_reason
            ));
        }
        let omitted = self.by_symbol.len().saturating_sub(parts.len());
        warn!(
            "Binance FR position-limit guard blocked ArbOpen summary: total={} details=[{}] omitted={}",
            self.total,
            parts.join("; "),
            omitted
        );
        self.total = 0;
        self.by_symbol.clear();
        self.last_log_us = now_us;
    }
}

#[derive(Debug)]
struct BinanceFrPositionLimitState {
    enabled: bool,
    limits: FastHashMap<String, BinanceFrPositionLimit>,
    refresh_ctx: Option<BinanceFrPositionLimitRefreshContext>,
    refresh_in_flight: bool,
    last_refresh_us: i64,
    stats: BlockStats,
}

impl BinanceFrPositionLimitState {
    fn disabled() -> Self {
        Self {
            enabled: false,
            limits: fast_hash_map(),
            refresh_ctx: None,
            refresh_in_flight: false,
            last_refresh_us: 0,
            stats: BlockStats::new(get_timestamp_us()),
        }
    }

    fn enabled(
        limits: FastHashMap<String, BinanceFrPositionLimit>,
        refresh_ctx: BinanceFrPositionLimitRefreshContext,
    ) -> Self {
        Self {
            enabled: true,
            limits,
            refresh_ctx: Some(refresh_ctx),
            refresh_in_flight: false,
            last_refresh_us: get_timestamp_us(),
            stats: BlockStats::new(get_timestamp_us()),
        }
    }
}

#[derive(Debug, Clone)]
struct BinanceFrPositionLimitConfig {
    env_name: String,
    arb_mode: ArbMode,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
}

#[derive(Debug, Clone)]
struct BinanceFrPositionLimitRefreshContext {
    redis: RedisSettings,
    config: BinanceFrPositionLimitConfig,
}

#[derive(Debug)]
struct BinanceFrPositionLimitRefreshResult {
    limits: FastHashMap<String, BinanceFrPositionLimit>,
    online_symbols: usize,
    missing_online: Vec<String>,
    symbol_config_rows: usize,
    position_rows: usize,
    bracket_rows: usize,
}

pub struct BinanceFrPositionLimitGuard;

impl BinanceFrPositionLimitGuard {
    pub async fn initialize(
        redis: &RedisSettings,
        env_name: Option<String>,
        arb_mode: ArbMode,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        binance_account_mode: Option<BinanceAccountMode>,
    ) -> Result<()> {
        if !is_binance_fr_path(arb_mode, open_venue, hedge_venue, binance_account_mode) {
            install_guard_state(BinanceFrPositionLimitState::disabled());
            info!(
                "Binance FR position-limit guard disabled: mode={} open={:?} hedge={:?} account_mode={:?}",
                arb_mode.as_str(),
                open_venue,
                hedge_venue,
                binance_account_mode
            );
            return Ok(());
        }

        let Some(env_name) = env_name
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
        else {
            bail!(
                "Binance FR position-limit guard requires runtime env name from cwd (mode={})",
                arb_mode.as_str()
            );
        };

        let config = BinanceFrPositionLimitConfig {
            env_name,
            arb_mode,
            open_venue,
            hedge_venue,
        };
        let refresh_ctx = BinanceFrPositionLimitRefreshContext {
            redis: redis.clone(),
            config: config.clone(),
        };
        let refresh_result = refresh_binance_fr_position_limits(&refresh_ctx).await?;
        let missing_count = refresh_result.missing_online.len();
        install_guard_state(BinanceFrPositionLimitState::enabled(
            refresh_result.limits,
            refresh_ctx,
        ));
        info!(
            "Binance FR position-limit guard startup: env={} limits={} online_symbols={} missing_online={} symbol_config_rows={} position_rows={} bracket_rows={}",
            config.env_name,
            current_limit_count(),
            refresh_result.online_symbols,
            missing_count,
            refresh_result.symbol_config_rows,
            refresh_result.position_rows,
            refresh_result.bracket_rows
        );
        if missing_count > 0 {
            warn!(
                "Binance FR position-limit guard online symbols missing from resolved snapshots: count={} samples=[{}]",
                missing_count,
                refresh_result
                    .missing_online
                    .iter()
                    .take(20)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
        Self::start_background_refresh_task();
        Ok(())
    }

    fn start_background_refresh_task() {
        let interval_secs = refresh_interval_secs();
        tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            interval.tick().await;
            loop {
                interval.tick().await;
                Self::request_refresh("background_interval");
            }
        });
        info!(
            "Binance FR position-limit guard background refresh task started (interval: {}s)",
            interval_secs
        );
    }

    fn request_refresh(source: &'static str) {
        let Some(refresh_ctx) = Self::try_begin_refresh() else {
            return;
        };
        tokio::task::spawn_local(async move {
            match refresh_binance_fr_position_limits(&refresh_ctx).await {
                Ok(result) => Self::apply_refresh_result(source, result),
                Err(err) => warn!(
                    "Binance FR position-limit guard refresh failed source={}: {err:#}",
                    source
                ),
            }
            BINANCE_FR_POSITION_LIMIT_GUARD.with(|guard| {
                if let Some(state) = guard.borrow_mut().as_mut() {
                    state.refresh_in_flight = false;
                }
            });
        });
    }

    fn try_begin_refresh() -> Option<BinanceFrPositionLimitRefreshContext> {
        BINANCE_FR_POSITION_LIMIT_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let state = guard_ref.as_mut()?;
            if !state.enabled || state.refresh_in_flight {
                return None;
            }
            let refresh_ctx = state.refresh_ctx.clone()?;
            state.refresh_in_flight = true;
            Some(refresh_ctx)
        })
    }

    fn apply_refresh_result(source: &'static str, result: BinanceFrPositionLimitRefreshResult) {
        let count = result.limits.len();
        let missing_count = result.missing_online.len();
        BINANCE_FR_POSITION_LIMIT_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let Some(state) = guard_ref.as_mut() else {
                return;
            };
            if !state.enabled {
                return;
            }
            state.limits = result.limits;
            state.last_refresh_us = get_timestamp_us();
        });
        info!(
            "Binance FR position-limit guard refresh applied: source={} limits={} online_symbols={} missing_online={} symbol_config_rows={} position_rows={} bracket_rows={}",
            source,
            count,
            result.online_symbols,
            missing_count,
            result.symbol_config_rows,
            result.position_rows,
            result.bracket_rows
        );
        if missing_count > 0 {
            warn!(
                "Binance FR position-limit guard refresh missing online symbols: count={} samples=[{}]",
                missing_count,
                result
                    .missing_online
                    .iter()
                    .take(20)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
    }

    pub fn should_block_arb_open(
        opening_symbol: &str,
        opening_venue: TradingVenue,
        hedging_symbol: &str,
        hedging_venue: TradingVenue,
    ) -> bool {
        if !is_binance_fr_venues(opening_venue, hedging_venue) {
            return false;
        }
        let symbol = normalize_guard_symbol(hedging_symbol)
            .or_else(|| normalize_guard_symbol(opening_symbol))
            .unwrap_or_default();
        if symbol.is_empty() {
            warn!("Binance FR position-limit guard blocks ArbOpen: empty symbol");
            return true;
        }

        BINANCE_FR_POSITION_LIMIT_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let Some(state) = guard_ref.as_mut() else {
                warn!(
                    "Binance FR position-limit guard blocks ArbOpen: guard not initialized symbol={}",
                    symbol
                );
                return true;
            };
            if !state.enabled {
                return false;
            }
            if state.limits.contains_key(&symbol) {
                return false;
            }
            let now_us = get_timestamp_us();
            state
                .stats
                .record(symbol.clone(), "missing_binance_position_limit_snapshot".to_string());
            state.stats.maybe_log(now_us);
            true
        })
    }

    pub fn cap_for_symbol(symbol: &str, side: Side) -> Result<BinanceFrPositionLimitCap, String> {
        let symbol = normalize_guard_symbol(symbol)
            .ok_or_else(|| format!("Binance FR position-limit symbol invalid: {symbol}"))?;
        BINANCE_FR_POSITION_LIMIT_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let Some(state) = guard_ref.as_mut() else {
                return Err(format!(
                    "Binance FR position-limit guard not initialized, reject symbol={}",
                    symbol
                ));
            };
            if !state.enabled {
                return Err("Binance FR position-limit guard disabled".to_string());
            }
            let Some(record) = state.limits.get(&symbol) else {
                let now_us = get_timestamp_us();
                state.stats.record(
                    symbol.clone(),
                    "missing_binance_position_limit_snapshot".to_string(),
                );
                state.stats.maybe_log(now_us);
                return Err(format!(
                    "Binance FR position-limit snapshot missing symbol={}, fail closed",
                    symbol
                ));
            };

            calculate_cap_for_record(&symbol, side, record)
        })
    }

    pub fn ensure_projected_notional(
        symbol: &str,
        side: Side,
        current_open_base_qty: f64,
        add_open_base_qty: f64,
        current_futures_base_qty: f64,
        add_futures_base_qty: f64,
        price: f64,
        raw_open_qty: f64,
        open_qty_multiplier: f64,
    ) -> Result<(), String> {
        if !(price.is_finite() && price > 0.0) {
            return Err(format!(
                "Binance FR position-limit check missing price symbol={} price={}",
                symbol, price
            ));
        }
        let cap = Self::cap_for_symbol(symbol, side)?;
        let open_leg =
            projected_leg_notional(current_open_base_qty, add_open_base_qty, price, cap.cap);
        let futures_leg = projected_leg_notional(
            current_futures_base_qty,
            add_futures_base_qty,
            price,
            cap.cap,
        );
        if open_leg.exceeds || futures_leg.exceeds {
            info!(
                "Binance FR position-limit reject detail: symbol={} side={} price={:.8} current_open_qty={:.8} add_open_qty={:.8} next_open_qty={:.8} open_current_usdt={:.4} open_order_usdt={:.4} open_next_usdt={:.4} current_futures_qty={:.8} add_futures_qty={:.8} next_futures_qty={:.8} futures_current_usdt={:.4} futures_order_usdt={:.4} futures_next_usdt={:.4} exceeded_open={} exceeded_futures={} max_notional_value={:.4} buffer={:.4} cap={:.4} pending_limit_orders={} amount_u={:.4} leverage={:?} current_notional={:?} bracket_notional_cap={:?} raw_open_qty={:.8} open_qty_multiplier={:.8}",
                cap.symbol,
                side.as_str(),
                price,
                current_open_base_qty,
                add_open_base_qty,
                open_leg.next_qty,
                open_leg.current_usdt,
                open_leg.order_usdt,
                open_leg.next_usdt,
                current_futures_base_qty,
                add_futures_base_qty,
                futures_leg.next_qty,
                futures_leg.current_usdt,
                futures_leg.order_usdt,
                futures_leg.next_usdt,
                open_leg.exceeds,
                futures_leg.exceeds,
                cap.max_notional_value,
                cap.buffer,
                cap.cap,
                cap.pending_limit_orders,
                cap.amount_u,
                cap.leverage,
                cap.current_notional,
                cap.bracket_notional_cap,
                raw_open_qty,
                open_qty_multiplier
            );
            return Err(format!(
                "Binance FR position-limit cap exceeded symbol={} open_next={:.4}USDT futures_next={:.4}USDT cap={:.4}USDT max_notional_value={:.4} buffer={:.4}",
                cap.symbol, open_leg.next_usdt, futures_leg.next_usdt, cap.cap, cap.max_notional_value, cap.buffer
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct ProjectedLegNotional {
    next_qty: f64,
    current_usdt: f64,
    order_usdt: f64,
    next_usdt: f64,
    exceeds: bool,
}

fn projected_leg_notional(
    current_base_qty: f64,
    add_base_qty: f64,
    price: f64,
    cap_usdt: f64,
) -> ProjectedLegNotional {
    let next_qty = current_base_qty + add_base_qty;
    let current_usdt = current_base_qty.abs() * price;
    let order_usdt = add_base_qty.abs() * price;
    let next_usdt = next_qty.abs() * price;
    let eps = 1e-6_f64;
    let increases = next_usdt > current_usdt + eps;
    ProjectedLegNotional {
        next_qty,
        current_usdt,
        order_usdt,
        next_usdt,
        exceeds: increases && next_usdt > cap_usdt + eps,
    }
}

fn install_guard_state(state: BinanceFrPositionLimitState) {
    BINANCE_FR_POSITION_LIMIT_GUARD.with(|guard| {
        *guard.borrow_mut() = Some(state);
    });
}

fn current_limit_count() -> usize {
    BINANCE_FR_POSITION_LIMIT_GUARD.with(|guard| {
        guard
            .borrow()
            .as_ref()
            .map(|state| state.limits.len())
            .unwrap_or(0)
    })
}

fn is_binance_fr_path(
    arb_mode: ArbMode,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    account_mode: Option<BinanceAccountMode>,
) -> bool {
    arb_mode == ArbMode::FundingArb
        && is_binance_fr_venues(open_venue, hedge_venue)
        && account_mode == Some(BinanceAccountMode::Unified)
}

fn is_binance_fr_venues(open_venue: TradingVenue, hedge_venue: TradingVenue) -> bool {
    open_venue == TradingVenue::BinanceMargin && hedge_venue == TradingVenue::BinanceFutures
}

fn calculate_cap_for_record(
    symbol: &str,
    side: Side,
    record: &BinanceFrPositionLimit,
) -> Result<BinanceFrPositionLimitCap, String> {
    let max_notional_value = effective_max_notional_value(record).ok_or_else(|| {
        format!(
            "Binance FR position-limit max notional invalid symbol={} max_notional_value={:.4} bracket_notional_cap={:?}",
            symbol, record.max_notional_value, record.bracket_notional_cap
        )
    })?;
    let params = PreTradeParamsLoader::instance();
    let pending_limit_orders = match side {
        Side::Buy => params.arb_max_pending_limit_buy_orders(),
        Side::Sell => params.arb_max_pending_limit_sell_orders(),
    }
    .max(0);
    let amount_u = params.arb_amount_u_for_symbol(symbol);
    if !(amount_u.is_finite() && amount_u > 0.0) {
        return Err(format!(
            "Binance FR position-limit amount_u invalid symbol={} amount_u={}",
            symbol, amount_u
        ));
    }
    let buffer = pending_limit_orders as f64 * amount_u;
    let cap = max_notional_value - buffer;
    if !(cap.is_finite() && cap > 0.0) {
        return Err(format!(
            "Binance FR position-limit cap invalid symbol={} max_notional_value={:.4} buffer={:.4} pending_limit_orders={} amount_u={:.4}",
            symbol, max_notional_value, buffer, pending_limit_orders, amount_u
        ));
    }
    Ok(BinanceFrPositionLimitCap {
        symbol: symbol.to_string(),
        max_notional_value,
        buffer,
        cap,
        amount_u,
        pending_limit_orders,
        leverage: record.leverage,
        current_notional: record.current_notional,
        bracket_notional_cap: record.bracket_notional_cap,
    })
}

fn effective_max_notional_value(record: &BinanceFrPositionLimit) -> Option<f64> {
    let mut cap = None;
    if record.max_notional_value.is_finite() && record.max_notional_value > 0.0 {
        cap = Some(record.max_notional_value);
    }
    if let Some(bracket_cap) = record.bracket_notional_cap {
        if bracket_cap.is_finite() && bracket_cap > 0.0 {
            cap = Some(cap.map_or(bracket_cap, |value: f64| value.min(bracket_cap)));
        }
    }
    cap
}

async fn refresh_binance_fr_position_limits(
    refresh_ctx: &BinanceFrPositionLimitRefreshContext,
) -> Result<BinanceFrPositionLimitRefreshResult> {
    let client = Client::builder()
        .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
        .build()
        .context("build Binance FR position-limit http client")?;
    let mut limits = fetch_binance_symbol_configs(&client).await?;
    let symbol_config_rows = limits.len();
    let positions = match fetch_binance_position_limits(&client).await {
        Ok(positions) => positions,
        Err(err) => {
            warn!("Binance FR position-limit guard failed to fetch positionRisk: {err:#}");
            fast_hash_map()
        }
    };
    let position_rows = positions.len();
    merge_binance_position_limits(&mut limits, positions);
    let bracket_caps = match fetch_binance_leverage_brackets(&client).await {
        Ok(caps) => caps,
        Err(err) => {
            warn!("Binance FR position-limit guard failed to fetch leverageBracket: {err:#}");
            fast_hash_map()
        }
    };
    let bracket_rows = bracket_caps.len();
    for (symbol, cap) in bracket_caps {
        if let Some(limit) = limits.get_mut(&symbol) {
            limit.bracket_notional_cap = Some(cap);
        }
    }

    let online_symbols = match load_online_symbols(&refresh_ctx.redis, &refresh_ctx.config).await {
        Ok(symbols) => symbols,
        Err(err) => {
            warn!("Binance FR position-limit guard failed to load Redis online symbols: {err:#}");
            BTreeSet::new()
        }
    };
    let missing_online = online_symbols
        .iter()
        .filter(|symbol| !limits.contains_key(*symbol))
        .cloned()
        .collect::<Vec<_>>();
    Ok(BinanceFrPositionLimitRefreshResult {
        limits,
        online_symbols: online_symbols.len(),
        missing_online,
        symbol_config_rows,
        position_rows,
        bracket_rows,
    })
}

async fn fetch_binance_symbol_configs(
    client: &Client,
) -> Result<FastHashMap<String, BinanceFrPositionLimit>> {
    let body = binance_signed_get(client, "/papi/v1/um/symbolConfig").await?;
    parse_binance_position_limits_response(&body, "/papi/v1/um/symbolConfig", "symbolConfig")
}

async fn fetch_binance_position_limits(
    client: &Client,
) -> Result<FastHashMap<String, BinanceFrPositionLimit>> {
    let body = binance_signed_get(client, "/papi/v1/um/positionRisk").await?;
    parse_binance_position_limits_response(&body, "/papi/v1/um/positionRisk", "positionRisk")
}

fn parse_binance_position_limits_response(
    body: &str,
    path: &str,
    source: &str,
) -> Result<FastHashMap<String, BinanceFrPositionLimit>> {
    let value: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "Binance {} response is not JSON: {}",
            path,
            truncate(&body, 500)
        )
    })?;
    let Some(items) = value.as_array() else {
        bail!(
            "Binance {} response is not an array: {}",
            path,
            truncate(&body, 500)
        );
    };

    let mut limits = fast_hash_map();
    let mut skipped = 0usize;
    for item in items {
        match parse_binance_position_limit(item) {
            Some((symbol, record)) => {
                limits.insert(symbol, record);
            }
            None => skipped = skipped.saturating_add(1),
        }
    }
    if skipped > 0 {
        warn!(
            "Binance FR position-limit guard skipped {} rows with missing/invalid symbol or maxNotionalValue: skipped={}",
            source,
            skipped
        );
    }
    debug!(
        "Binance FR position-limit guard fetched {}: count={} skipped={}",
        source,
        limits.len(),
        skipped
    );
    Ok(limits)
}

fn merge_binance_position_limits(
    limits: &mut FastHashMap<String, BinanceFrPositionLimit>,
    positions: FastHashMap<String, BinanceFrPositionLimit>,
) {
    for (symbol, position) in positions {
        if let Some(limit) = limits.get_mut(&symbol) {
            limit.current_notional = position.current_notional;
            if limit.leverage.is_none() {
                limit.leverage = position.leverage;
            }
        } else {
            limits.insert(symbol, position);
        }
    }
}

fn parse_binance_position_limit(value: &Value) -> Option<(String, BinanceFrPositionLimit)> {
    let raw_symbol = value.get("symbol")?.as_str()?.trim().to_ascii_uppercase();
    let symbol = normalize_guard_symbol(&raw_symbol)?;
    let max_notional_value = parse_number_field(value, "maxNotionalValue")?;
    if !(max_notional_value.is_finite() && max_notional_value > 0.0) {
        return None;
    }
    let record = BinanceFrPositionLimit {
        max_notional_value,
        leverage: parse_number_field(value, "leverage"),
        current_notional: parse_number_field(value, "notional"),
        bracket_notional_cap: None,
    };
    Some((symbol, record))
}

async fn fetch_binance_leverage_brackets(client: &Client) -> Result<FastHashMap<String, f64>> {
    let body = binance_signed_get(client, "/papi/v1/um/leverageBracket").await?;
    let value: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "Binance /papi/v1/um/leverageBracket response is not JSON: {}",
            truncate(&body, 500)
        )
    })?;
    let Some(items) = value.as_array() else {
        bail!(
            "Binance /papi/v1/um/leverageBracket response is not an array: {}",
            truncate(&body, 500)
        );
    };

    let mut caps = fast_hash_map();
    let mut skipped = 0usize;
    for item in items {
        match parse_binance_leverage_bracket_cap(item) {
            Some((symbol, cap)) => {
                caps.insert(symbol, cap);
            }
            None => skipped = skipped.saturating_add(1),
        }
    }
    if skipped > 0 {
        warn!(
            "Binance FR position-limit guard skipped leverageBracket rows with missing/invalid symbol or notionalCap: skipped={}",
            skipped
        );
    }
    debug!(
        "Binance FR position-limit guard fetched leverageBracket: count={} skipped={}",
        caps.len(),
        skipped
    );
    Ok(caps)
}

fn parse_binance_leverage_bracket_cap(value: &Value) -> Option<(String, f64)> {
    let raw_symbol = value.get("symbol")?.as_str()?.trim().to_ascii_uppercase();
    let symbol = normalize_guard_symbol(&raw_symbol)?;
    let brackets = value.get("brackets")?.as_array()?;
    let mut max_cap: Option<f64> = None;
    for bracket in brackets {
        let Some(cap) = parse_number_field(bracket, "notionalCap") else {
            continue;
        };
        if cap.is_finite() && cap > 0.0 {
            max_cap = Some(max_cap.map_or(cap, |current| current.max(cap)));
        }
    }
    max_cap.map(|cap| (symbol, cap))
}

fn parse_number_field(value: &Value, field: &str) -> Option<f64> {
    let raw = value.get(field)?;
    match raw {
        Value::Number(num) => num.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

async fn load_online_symbols(
    redis: &RedisSettings,
    config: &BinanceFrPositionLimitConfig,
) -> Result<BTreeSet<String>> {
    let keys = online_symbol_keys(config);
    if keys.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut client = RedisClient::connect(redis.clone()).await?;
    let mut symbols = BTreeSet::new();
    for key in keys {
        let Some(raw) = client.get_string(&key).await? else {
            info!("Binance FR position-limit guard Redis key missing: {}", key);
            continue;
        };
        let values = decode_redis_list(&raw, &key)?;
        info!(
            "Binance FR position-limit guard Redis key loaded: {} count={}",
            key,
            values.len()
        );
        for value in values {
            if let Some(symbol) = normalize_guard_symbol(&value) {
                symbols.insert(symbol);
            }
        }
    }
    Ok(symbols)
}

fn online_symbol_keys(config: &BinanceFrPositionLimitConfig) -> Vec<String> {
    if config.arb_mode != ArbMode::FundingArb {
        return Vec::new();
    }
    let venue_suffix = format!(
        "{}_{}",
        config.open_venue.data_pub_slug(),
        config.hedge_venue.data_pub_slug()
    );
    [
        "dump_symbols",
        "trade_symbols",
        "fwd_trade_symbols",
        "bwd_trade_symbols",
        "unimmr_close_symbols",
    ]
    .into_iter()
    .map(|name| format!("{}:fr_{}:{}", config.env_name, name, venue_suffix))
    .collect()
}

fn decode_redis_list(raw: &str, key: &str) -> Result<Vec<String>> {
    let parsed: Value =
        serde_json::from_str(raw).with_context(|| format!("parse Redis JSON list key={key}"))?;
    let Some(list) = parsed.as_array() else {
        bail!("Redis key is not a JSON list: {key}");
    };
    Ok(list
        .iter()
        .filter_map(|item| {
            let value = item
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| item.to_string());
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        })
        .collect())
}

fn normalize_guard_symbol(value: &str) -> Option<String> {
    let mut text = value.trim().to_ascii_uppercase();
    if text.is_empty() {
        return None;
    }
    if let Some((head, _)) = text.split_once('@') {
        text = head.trim().to_string();
    }
    let canonical = text.replace(['_', '/'], "-");
    let asset = if let Some(stripped) = canonical.strip_suffix("-USDT-SWAP") {
        stripped.to_string()
    } else if let Some(idx) = canonical.find("-USDT-") {
        canonical[..idx].to_string()
    } else if let Some(stripped) = canonical.strip_suffix("-USDT") {
        stripped.to_string()
    } else {
        let cleaned = clean_symbol_text(&canonical);
        if cleaned.ends_with("USDT") && cleaned.len() > "USDT".len() {
            cleaned[..cleaned.len() - "USDT".len()].to_string()
        } else {
            cleaned
        }
    };
    let asset = clean_symbol_text(&asset);
    if asset.is_empty() {
        None
    } else {
        let normalized = normalize_symbol_for_internal(&format!("{asset}USDT"));
        (!normalized.is_empty()).then_some(normalized)
    }
}

fn clean_symbol_text(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect()
}

async fn binance_signed_get(client: &Client, path: &str) -> Result<String> {
    let api_key = required_env("BINANCE_API_KEY")?;
    let api_secret = required_env("BINANCE_API_SECRET")?;
    let base = env_or("BINANCE_PAPI_URL", "https://papi.binance.com");

    let mut params = BTreeMap::new();
    params.insert("recvWindow".to_string(), "5000".to_string());
    params.insert(
        "timestamp".to_string(),
        Utc::now().timestamp_millis().to_string(),
    );
    let query = build_query(&params);
    let signature = hmac_sha256_hex(&api_secret, &query);
    let url = format!(
        "{}{}?{}&signature={}",
        base.trim_end_matches('/'),
        path,
        query,
        signature
    );
    let resp = client
        .get(url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if !(200..300).contains(&status) {
        bail!(
            "Binance signed GET failed path={} status={} body={}",
            path,
            status,
            truncate(&body, 500)
        );
    }
    Ok(body)
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name)
        .map(|value| value.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{name} not set"))
}

fn refresh_interval_secs() -> u64 {
    std::env::var("PRE_TRADE_BINANCE_FR_POSITION_LIMIT_REFRESH_SECS")
        .or_else(|_| std::env::var("PRE_TRADE_BINANCE_POSITION_LIMIT_REFRESH_SECS"))
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REFRESH_INTERVAL_SECS)
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn build_query(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn hmac_sha256_hex(secret: &str, payload: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any size");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn truncate(value: &str, max_len: usize) -> String {
    if value.len() <= max_len {
        value.to_string()
    } else {
        let mut end = 0usize;
        for (idx, _) in value.char_indices() {
            if idx > max_len {
                break;
            }
            end = idx;
        }
        if end == 0 {
            end = value
                .char_indices()
                .nth(1)
                .map(|(idx, _)| idx)
                .unwrap_or(value.len());
        }
        format!("{}...<{} bytes>", &value[..end], value.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(
        env_name: &str,
        arb_mode: ArbMode,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> BinanceFrPositionLimitConfig {
        BinanceFrPositionLimitConfig {
            env_name: env_name.to_string(),
            arb_mode,
            open_venue,
            hedge_venue,
        }
    }

    fn position_limit(
        max_notional_value: f64,
        bracket_notional_cap: Option<f64>,
    ) -> BinanceFrPositionLimit {
        BinanceFrPositionLimit {
            max_notional_value,
            leverage: Some(5.0),
            current_notional: Some(1000.0),
            bracket_notional_cap,
        }
    }

    #[test]
    fn binance_fr_guard_only_supports_unified_funding_arb() {
        assert!(is_binance_fr_path(
            ArbMode::FundingArb,
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
            Some(BinanceAccountMode::Unified)
        ));
        assert!(!is_binance_fr_path(
            ArbMode::IntraArb,
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
            Some(BinanceAccountMode::Unified)
        ));
        assert!(!is_binance_fr_path(
            ArbMode::FundingArb,
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
            Some(BinanceAccountMode::Standard)
        ));
    }

    #[test]
    fn builds_fr_online_symbol_keys() {
        let keys = online_symbol_keys(&config(
            "binance_fr_arb02",
            ArbMode::FundingArb,
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        ));
        assert_eq!(
            keys,
            vec![
                "binance_fr_arb02:fr_dump_symbols:binance-margin_binance-futures",
                "binance_fr_arb02:fr_trade_symbols:binance-margin_binance-futures",
                "binance_fr_arb02:fr_fwd_trade_symbols:binance-margin_binance-futures",
                "binance_fr_arb02:fr_bwd_trade_symbols:binance-margin_binance-futures",
                "binance_fr_arb02:fr_unimmr_close_symbols:binance-margin_binance-futures",
            ]
        );
    }

    #[test]
    fn intra_has_no_online_symbol_keys() {
        let keys = online_symbol_keys(&config(
            "binance-intra-arb01",
            ArbMode::IntraArb,
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        ));
        assert!(keys.is_empty());
    }

    #[test]
    fn normalizes_binance_symbols() {
        assert_eq!(normalize_guard_symbol("HNT"), Some("HNTUSDT".to_string()));
        assert_eq!(
            normalize_guard_symbol("hnt-usdt-swap@x"),
            Some("HNTUSDT".to_string())
        );
        assert_eq!(
            normalize_guard_symbol("HNT_USDT"),
            Some("HNTUSDT".to_string())
        );
    }

    #[test]
    fn parses_position_limit_row() {
        let row = serde_json::json!({
            "symbol": "BTCUSDT",
            "leverage": "5",
            "maxNotionalValue": "50000",
            "notional": "-1200.5"
        });
        let (symbol, limit) = parse_binance_position_limit(&row).expect("position limit");
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(limit.max_notional_value, 50000.0);
        assert_eq!(limit.leverage, Some(5.0));
        assert_eq!(limit.current_notional, Some(-1200.5));
    }

    #[test]
    fn keeps_symbol_config_without_position_risk_row() {
        let symbol_config = serde_json::json!({
            "symbol": "AGLDUSDT",
            "leverage": 4,
            "maxNotionalValue": "50000"
        });
        let (symbol, limit) =
            parse_binance_position_limit(&symbol_config).expect("symbol config limit");
        let mut limits = fast_hash_map();
        limits.insert(symbol.clone(), limit);

        merge_binance_position_limits(&mut limits, fast_hash_map());

        let resolved = limits.get(&symbol).expect("resolved limit");
        assert_eq!(resolved.max_notional_value, 50000.0);
        assert_eq!(resolved.leverage, Some(4.0));
        assert_eq!(resolved.current_notional, None);
    }

    #[test]
    fn position_risk_only_overlays_runtime_fields() {
        let mut limits = fast_hash_map();
        limits.insert(
            "HFTUSDT".to_string(),
            BinanceFrPositionLimit {
                max_notional_value: 50000.0,
                leverage: Some(5.0),
                current_notional: None,
                bracket_notional_cap: None,
            },
        );
        let mut positions = fast_hash_map();
        positions.insert(
            "HFTUSDT".to_string(),
            BinanceFrPositionLimit {
                max_notional_value: 25000.0,
                leverage: Some(2.0),
                current_notional: Some(-1200.5),
                bracket_notional_cap: None,
            },
        );

        merge_binance_position_limits(&mut limits, positions);

        let resolved = limits.get("HFTUSDT").expect("resolved limit");
        assert_eq!(resolved.max_notional_value, 50000.0);
        assert_eq!(resolved.leverage, Some(5.0));
        assert_eq!(resolved.current_notional, Some(-1200.5));
    }

    #[test]
    fn projected_leg_cap_blocks_only_expansion() {
        let expanding = projected_leg_notional(6.0, 1.0, 1000.0, 5_000.0);
        assert!(expanding.exceeds);
        assert_eq!(expanding.next_usdt, 7_000.0);

        let reducing = projected_leg_notional(6.0, -2.0, 1000.0, 5_000.0);
        assert!(!reducing.exceeds);
        assert_eq!(reducing.next_usdt, 4_000.0);
    }

    #[test]
    fn parses_max_leverage_bracket_cap() {
        let row = serde_json::json!({
            "symbol": "BTCUSDT",
            "brackets": [
                {"bracket": 1, "notionalCap": "50000"},
                {"bracket": 2, "notionalCap": "250000"}
            ]
        });
        let (symbol, cap) = parse_binance_leverage_bracket_cap(&row).expect("bracket cap");
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(cap, 250000.0);
    }

    #[test]
    fn effective_cap_uses_lower_position_and_bracket_caps() {
        assert_eq!(
            effective_max_notional_value(&position_limit(50000.0, Some(250000.0))),
            Some(50000.0)
        );
        assert_eq!(
            effective_max_notional_value(&position_limit(250000.0, Some(50000.0))),
            Some(50000.0)
        );
        assert_eq!(
            effective_max_notional_value(&position_limit(50000.0, None)),
            Some(50000.0)
        );
    }
}
