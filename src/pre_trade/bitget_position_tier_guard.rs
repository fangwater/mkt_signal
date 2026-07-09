use anyhow::{bail, Context, Result};
use log::{debug, info, warn};
use order_common::{Side, TradingVenue};
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use runtime_common::redis_client::{RedisClient, RedisSettings};
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use serde_json::Value;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use trade_signal::ArbMode;

use crate::pre_trade::params_load::PreTradeParamsLoader;

const DEFAULT_CACHE_KEY: &str = "bitget_position_tier_cache:USDT-FUTURES";
const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 30;
const DEFAULT_LEVERAGE: u8 = 5;
const BLOCK_SUMMARY_INTERVAL_US: i64 = 60_000_000;

thread_local! {
    static BITGET_POSITION_TIER_GUARD: RefCell<Option<BitgetPositionTierState>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone)]
struct BitgetPositionTierLimit {
    risk_limit: f64,
    leverage: u8,
    configured_leverage: u8,
    exact_leverage_match: bool,
    max_leverage: Option<f64>,
    updated_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct BitgetPositionTierCap {
    pub symbol: String,
    pub risk_limit: f64,
    pub buffer: f64,
    pub cap: f64,
    pub amount_u: f64,
    pub pending_limit_orders: i32,
    pub leverage: u8,
    pub configured_leverage: u8,
    pub exact_leverage_match: bool,
    pub max_leverage: Option<f64>,
    pub updated_at_ms: Option<i64>,
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
            "Bitget position-tier guard blocked ArbOpen summary: total={} details=[{}] omitted={}",
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
struct BitgetPositionTierState {
    enabled: bool,
    limits: FastHashMap<String, BitgetPositionTierLimit>,
    refresh_ctx: Option<BitgetPositionTierRefreshContext>,
    refresh_in_flight: bool,
    last_refresh_us: i64,
    stats: BlockStats,
}

impl BitgetPositionTierState {
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
        limits: FastHashMap<String, BitgetPositionTierLimit>,
        refresh_ctx: BitgetPositionTierRefreshContext,
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
struct BitgetPositionTierConfig {
    env_name: Option<String>,
    arb_mode: ArbMode,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    cache_key: String,
    leverage: u8,
}

#[derive(Debug, Clone)]
struct BitgetPositionTierRefreshContext {
    redis: RedisSettings,
    config: BitgetPositionTierConfig,
}

#[derive(Debug)]
struct BitgetPositionTierRefreshResult {
    limits: FastHashMap<String, BitgetPositionTierLimit>,
    active_symbols: usize,
    missing_symbols: Vec<String>,
    fallback_leverage_symbols: usize,
    cached_symbols: usize,
}

#[derive(Debug)]
struct ParsedBitgetPositionTierCache {
    limits: FastHashMap<String, BitgetPositionTierLimit>,
    active_symbols: usize,
    missing_symbols: Vec<String>,
    fallback_leverage_symbols: usize,
    cached_symbols: usize,
}

pub struct BitgetPositionTierGuard;

impl BitgetPositionTierGuard {
    pub async fn initialize(
        redis: &RedisSettings,
        env_name: Option<String>,
        arb_mode: ArbMode,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<()> {
        if !is_bitget_position_tier_path(arb_mode, open_venue, hedge_venue) {
            install_guard_state(BitgetPositionTierState::disabled());
            info!(
                "Bitget position-tier guard disabled: mode={} open={:?} hedge={:?}",
                arb_mode.as_str(),
                open_venue,
                hedge_venue
            );
            return Ok(());
        }

        let config = BitgetPositionTierConfig {
            env_name: env_name
                .map(|value| value.trim().to_ascii_lowercase())
                .filter(|value| !value.is_empty()),
            arb_mode,
            open_venue,
            hedge_venue,
            cache_key: cache_key(),
            leverage: configured_leverage(),
        };
        let refresh_ctx = BitgetPositionTierRefreshContext {
            redis: redis.clone(),
            config: config.clone(),
        };
        let refresh_result = refresh_bitget_position_tiers(&refresh_ctx).await?;
        let missing_count = refresh_result.missing_symbols.len();
        install_guard_state(BitgetPositionTierState::enabled(
            refresh_result.limits,
            refresh_ctx,
        ));
        info!(
            "Bitget position-tier guard startup: env={:?} mode={} open={:?} hedge={:?} cache_key={} leverage={} limits={} active_symbols={} cached_symbols={} missing={} leverage_fallbacks={}",
            config.env_name,
            config.arb_mode.as_str(),
            config.open_venue,
            config.hedge_venue,
            config.cache_key,
            config.leverage,
            current_limit_count(),
            refresh_result.active_symbols,
            refresh_result.cached_symbols,
            missing_count,
            refresh_result.fallback_leverage_symbols
        );
        if missing_count > 0 {
            warn!(
                "Bitget position-tier guard cache missing active symbols: count={} samples=[{}]",
                missing_count,
                refresh_result
                    .missing_symbols
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
            "Bitget position-tier guard background refresh task started (interval: {}s)",
            interval_secs
        );
    }

    fn request_refresh(source: &'static str) {
        let Some(refresh_ctx) = Self::try_begin_refresh() else {
            return;
        };
        tokio::task::spawn_local(async move {
            match refresh_bitget_position_tiers(&refresh_ctx).await {
                Ok(result) => Self::apply_refresh_result(source, result),
                Err(err) => warn!(
                    "Bitget position-tier guard refresh failed source={}: {err:#}",
                    source
                ),
            }
            BITGET_POSITION_TIER_GUARD.with(|guard| {
                if let Some(state) = guard.borrow_mut().as_mut() {
                    state.refresh_in_flight = false;
                }
            });
        });
    }

    fn try_begin_refresh() -> Option<BitgetPositionTierRefreshContext> {
        BITGET_POSITION_TIER_GUARD.with(|guard| {
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

    fn apply_refresh_result(source: &'static str, result: BitgetPositionTierRefreshResult) {
        let count = result.limits.len();
        let missing_count = result.missing_symbols.len();
        BITGET_POSITION_TIER_GUARD.with(|guard| {
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
            "Bitget position-tier guard refresh applied: source={} limits={} active_symbols={} cached_symbols={} missing={} leverage_fallbacks={}",
            source,
            count,
            result.active_symbols,
            result.cached_symbols,
            missing_count,
            result.fallback_leverage_symbols
        );
        if missing_count > 0 {
            warn!(
                "Bitget position-tier guard refresh missing active symbols: count={} samples=[{}]",
                missing_count,
                result
                    .missing_symbols
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
        if !is_bitget_position_tier_venues(opening_venue, hedging_venue) {
            return false;
        }
        let symbol =
            bitget_guard_symbol(opening_symbol, opening_venue, hedging_symbol, hedging_venue)
                .unwrap_or_default();
        if symbol.is_empty() {
            warn!("Bitget position-tier guard blocks ArbOpen: empty symbol");
            return true;
        }

        BITGET_POSITION_TIER_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let Some(state) = guard_ref.as_mut() else {
                warn!(
                    "Bitget position-tier guard blocks ArbOpen: guard not initialized symbol={}",
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
            state.stats.record(
                symbol.clone(),
                "missing_bitget_position_tier_cache".to_string(),
            );
            state.stats.maybe_log(now_us);
            true
        })
    }

    pub fn cap_for_symbol(symbol: &str, side: Side) -> Result<BitgetPositionTierCap, String> {
        let symbol = normalize_guard_symbol(symbol)
            .ok_or_else(|| format!("Bitget position-tier symbol invalid: {symbol}"))?;
        BITGET_POSITION_TIER_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let Some(state) = guard_ref.as_mut() else {
                return Err(format!(
                    "Bitget position-tier guard not initialized, reject symbol={}",
                    symbol
                ));
            };
            if !state.enabled {
                return Err("Bitget position-tier guard disabled".to_string());
            }
            let Some(record) = state.limits.get(&symbol) else {
                let now_us = get_timestamp_us();
                state.stats.record(
                    symbol.clone(),
                    "missing_bitget_position_tier_cache".to_string(),
                );
                state.stats.maybe_log(now_us);
                return Err(format!(
                    "Bitget position-tier cache missing symbol={}, fail closed",
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
                "Bitget position-tier check missing price symbol={} price={}",
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
                "Bitget position-tier reject detail: symbol={} side={} price={:.8} current_open_qty={:.8} add_open_qty={:.8} next_open_qty={:.8} open_current_usdt={:.4} open_order_usdt={:.4} open_next_usdt={:.4} current_futures_qty={:.8} add_futures_qty={:.8} next_futures_qty={:.8} futures_current_usdt={:.4} futures_order_usdt={:.4} futures_next_usdt={:.4} exceeded_open={} exceeded_futures={} risk_limit={:.4} buffer={:.4} cap={:.4} pending_limit_orders={} amount_u={:.4} leverage={} configured_leverage={} exact_leverage_match={} max_leverage={:?} updated_at_ms={:?} raw_open_qty={:.8} open_qty_multiplier={:.8}",
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
                cap.risk_limit,
                cap.buffer,
                cap.cap,
                cap.pending_limit_orders,
                cap.amount_u,
                cap.leverage,
                cap.configured_leverage,
                cap.exact_leverage_match,
                cap.max_leverage,
                cap.updated_at_ms,
                raw_open_qty,
                open_qty_multiplier
            );
            return Err(format!(
                "Bitget position-tier cap exceeded symbol={} open_next={:.4}USDT futures_next={:.4}USDT cap={:.4}USDT risk_limit={:.4} buffer={:.4} leverage={} configured_leverage={}",
                cap.symbol,
                open_leg.next_usdt,
                futures_leg.next_usdt,
                cap.cap,
                cap.risk_limit,
                cap.buffer,
                cap.leverage,
                cap.configured_leverage
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

fn install_guard_state(state: BitgetPositionTierState) {
    BITGET_POSITION_TIER_GUARD.with(|guard| {
        *guard.borrow_mut() = Some(state);
    });
}

fn current_limit_count() -> usize {
    BITGET_POSITION_TIER_GUARD.with(|guard| {
        guard
            .borrow()
            .as_ref()
            .map(|state| state.limits.len())
            .unwrap_or(0)
    })
}

fn is_bitget_position_tier_path(
    _arb_mode: ArbMode,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> bool {
    is_bitget_position_tier_venues(open_venue, hedge_venue)
}

fn is_bitget_position_tier_venues(open_venue: TradingVenue, hedge_venue: TradingVenue) -> bool {
    open_venue == TradingVenue::BitgetFutures || hedge_venue == TradingVenue::BitgetFutures
}

fn bitget_guard_symbol(
    opening_symbol: &str,
    opening_venue: TradingVenue,
    hedging_symbol: &str,
    hedging_venue: TradingVenue,
) -> Option<String> {
    if hedging_venue == TradingVenue::BitgetFutures {
        normalize_guard_symbol(hedging_symbol)
    } else if opening_venue == TradingVenue::BitgetFutures {
        normalize_guard_symbol(opening_symbol)
    } else {
        None
    }
}

fn calculate_cap_for_record(
    symbol: &str,
    side: Side,
    record: &BitgetPositionTierLimit,
) -> Result<BitgetPositionTierCap, String> {
    let params = PreTradeParamsLoader::instance();
    let pending_limit_orders = match side {
        Side::Buy => params.arb_max_pending_limit_buy_orders(),
        Side::Sell => params.arb_max_pending_limit_sell_orders(),
    }
    .max(0);
    let amount_u = params.arb_amount_u_for_symbol(symbol);
    if !(amount_u.is_finite() && amount_u > 0.0) {
        return Err(format!(
            "Bitget position-tier amount_u invalid symbol={} amount_u={}",
            symbol, amount_u
        ));
    }
    let buffer = pending_limit_orders as f64 * amount_u;
    let cap = record.risk_limit - buffer;
    if !(cap.is_finite() && cap > 0.0) {
        return Err(format!(
            "Bitget position-tier cap invalid symbol={} risk_limit={:.4} buffer={:.4} pending_limit_orders={} amount_u={:.4} leverage={} configured_leverage={}",
            symbol,
            record.risk_limit,
            buffer,
            pending_limit_orders,
            amount_u,
            record.leverage,
            record.configured_leverage
        ));
    }
    Ok(BitgetPositionTierCap {
        symbol: symbol.to_string(),
        risk_limit: record.risk_limit,
        buffer,
        cap,
        amount_u,
        pending_limit_orders,
        leverage: record.leverage,
        configured_leverage: record.configured_leverage,
        exact_leverage_match: record.exact_leverage_match,
        max_leverage: record.max_leverage,
        updated_at_ms: record.updated_at_ms,
    })
}

async fn refresh_bitget_position_tiers(
    refresh_ctx: &BitgetPositionTierRefreshContext,
) -> Result<BitgetPositionTierRefreshResult> {
    let mut client = RedisClient::connect(refresh_ctx.redis.clone()).await?;
    let Some(raw) = client.get_string(&refresh_ctx.config.cache_key).await? else {
        bail!(
            "Bitget position-tier cache key missing: {}",
            refresh_ctx.config.cache_key
        );
    };
    let parsed =
        parse_bitget_position_tier_cache(&raw, refresh_ctx.config.leverage).with_context(|| {
            format!(
                "parse Redis Bitget position-tier cache key={}",
                refresh_ctx.config.cache_key
            )
        })?;
    Ok(BitgetPositionTierRefreshResult {
        limits: parsed.limits,
        active_symbols: parsed.active_symbols,
        missing_symbols: parsed.missing_symbols,
        fallback_leverage_symbols: parsed.fallback_leverage_symbols,
        cached_symbols: parsed.cached_symbols,
    })
}

fn parse_bitget_position_tier_cache(
    raw: &str,
    configured_leverage: u8,
) -> Result<ParsedBitgetPositionTierCache> {
    let value: Value =
        serde_json::from_str(raw).context("Bitget position-tier cache is not JSON")?;
    let Some(root) = value.as_object() else {
        bail!("Bitget position-tier cache root is not an object");
    };
    let symbols_value = root
        .get("symbols")
        .ok_or_else(|| anyhow::anyhow!("Bitget position-tier cache missing symbols"))?;
    let Some(symbols_obj) = symbols_value.as_object() else {
        bail!("Bitget position-tier cache symbols is not an object");
    };

    let mut active_symbols = parse_active_symbols(root.get("active_symbols"));
    if active_symbols.is_empty() {
        active_symbols = symbols_obj
            .keys()
            .filter_map(|symbol| normalize_guard_symbol(symbol))
            .collect();
    }

    let mut limits = fast_hash_map();
    let mut missing_symbols = Vec::new();
    let mut fallback_leverage_symbols = 0usize;
    for symbol in &active_symbols {
        let Some(record_value) = symbols_obj
            .get(symbol)
            .or_else(|| symbols_obj.get(&bitget_exchange_symbol(symbol)))
        else {
            missing_symbols.push(symbol.clone());
            continue;
        };
        match parse_bitget_position_tier_record(symbol, record_value, configured_leverage) {
            Some(record) => {
                if !record.exact_leverage_match {
                    fallback_leverage_symbols = fallback_leverage_symbols.saturating_add(1);
                }
                limits.insert(symbol.clone(), record);
            }
            None => missing_symbols.push(symbol.clone()),
        }
    }

    Ok(ParsedBitgetPositionTierCache {
        limits,
        active_symbols: active_symbols.len(),
        missing_symbols,
        fallback_leverage_symbols,
        cached_symbols: symbols_obj.len(),
    })
}

fn parse_active_symbols(value: Option<&Value>) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let Some(items) = value.and_then(Value::as_array) else {
        return out;
    };
    for item in items {
        if let Some(raw) = item.as_str() {
            if let Some(symbol) = normalize_guard_symbol(raw) {
                out.insert(symbol);
            }
        }
    }
    out
}

fn parse_bitget_position_tier_record(
    symbol: &str,
    value: &Value,
    configured_leverage: u8,
) -> Option<BitgetPositionTierLimit> {
    let risk_limits = value.get("risk_limit_by_leverage")?.as_object()?;
    let mut by_leverage = BTreeMap::new();
    for (raw_leverage, raw_cap) in risk_limits {
        let leverage = raw_leverage.trim().parse::<u8>().ok()?;
        let cap = parse_number_value(raw_cap)?;
        if leverage > 0 && cap.is_finite() && cap > 0.0 {
            by_leverage.insert(leverage, cap);
        }
    }
    let (leverage, risk_limit, exact_leverage_match) =
        select_risk_limit_for_leverage(&by_leverage, configured_leverage)?;
    debug!(
        "Bitget position-tier cache record parsed: symbol={} configured_leverage={} selected_leverage={} risk_limit={} exact={}",
        symbol,
        configured_leverage,
        leverage,
        risk_limit,
        exact_leverage_match
    );
    Some(BitgetPositionTierLimit {
        risk_limit,
        leverage,
        configured_leverage,
        exact_leverage_match,
        max_leverage: value.get("max_leverage").and_then(parse_number_value),
        updated_at_ms: value.get("updated_at_ms").and_then(parse_i64_value),
    })
}

fn select_risk_limit_for_leverage(
    by_leverage: &BTreeMap<u8, f64>,
    configured_leverage: u8,
) -> Option<(u8, f64, bool)> {
    if let Some(cap) = by_leverage.get(&configured_leverage).copied() {
        return Some((configured_leverage, cap, true));
    }
    if let Some((leverage, cap)) = by_leverage
        .range(..=configured_leverage)
        .next_back()
        .map(|(leverage, cap)| (*leverage, *cap))
    {
        return Some((leverage, cap, false));
    }
    by_leverage
        .iter()
        .min_by(|(_, lhs), (_, rhs)| lhs.partial_cmp(rhs).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(leverage, cap)| (*leverage, *cap, false))
}

fn parse_number_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(num) => num.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_i64_value(value: &Value) -> Option<i64> {
    match value {
        Value::Number(num) => num.as_i64(),
        Value::String(text) => text.trim().parse::<i64>().ok(),
        _ => None,
    }
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

fn bitget_exchange_symbol(internal_symbol: &str) -> String {
    normalize_guard_symbol(internal_symbol).unwrap_or_default()
}

fn clean_symbol_text(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect()
}

fn configured_leverage() -> u8 {
    std::env::var("PRE_TRADE_BITGET_POSITION_TIER_LEVERAGE")
        .or_else(|_| std::env::var("BITGET_POSITION_TIER_LEVERAGE"))
        .ok()
        .and_then(|value| value.trim().parse::<u8>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_LEVERAGE)
}

fn cache_key() -> String {
    std::env::var("PRE_TRADE_BITGET_POSITION_TIER_CACHE_KEY")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_CACHE_KEY.to_string())
}

fn refresh_interval_secs() -> u64 {
    std::env::var("PRE_TRADE_BITGET_POSITION_TIER_REFRESH_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REFRESH_INTERVAL_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_risk_limit_prefers_exact_leverage() {
        let mut by_leverage = BTreeMap::new();
        by_leverage.insert(4, 200_000.0);
        by_leverage.insert(5, 100_000.0);

        assert_eq!(
            select_risk_limit_for_leverage(&by_leverage, 5),
            Some((5, 100_000.0, true))
        );
    }

    #[test]
    fn select_risk_limit_falls_back_to_highest_available_below_target() {
        let mut by_leverage = BTreeMap::new();
        by_leverage.insert(1, 500_000.0);
        by_leverage.insert(4, 200_000.0);

        assert_eq!(
            select_risk_limit_for_leverage(&by_leverage, 5),
            Some((4, 200_000.0, false))
        );
    }

    #[test]
    fn parses_cache_and_reports_missing_active_symbols() {
        let raw = r#"{
            "active_symbols":["BTCUSDT","eth-usdt-swap","MISSINGUSDT"],
            "symbols":{
                "BTCUSDT":{
                    "updated_at_ms":123,
                    "max_leverage":"150",
                    "risk_limit_by_leverage":{"4":"4000","5":"3000"}
                },
                "ETHUSDT":{
                    "updated_at_ms":"456",
                    "max_leverage":"4",
                    "risk_limit_by_leverage":{"1":"9000","4":"6000"}
                }
            }
        }"#;

        let parsed = parse_bitget_position_tier_cache(raw, 5).unwrap();
        assert_eq!(parsed.active_symbols, 3);
        assert_eq!(parsed.cached_symbols, 2);
        assert_eq!(parsed.missing_symbols, vec!["MISSINGUSDT".to_string()]);
        assert_eq!(parsed.fallback_leverage_symbols, 1);

        let btc = parsed.limits.get("BTCUSDT").unwrap();
        assert_eq!(btc.risk_limit, 3000.0);
        assert_eq!(btc.leverage, 5);
        assert!(btc.exact_leverage_match);

        let eth = parsed.limits.get("ETHUSDT").unwrap();
        assert_eq!(eth.risk_limit, 6000.0);
        assert_eq!(eth.leverage, 4);
        assert!(!eth.exact_leverage_match);
        assert_eq!(eth.updated_at_ms, Some(456));
    }
}
