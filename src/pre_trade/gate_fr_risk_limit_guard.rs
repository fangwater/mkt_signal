use account_common::gate_auth::GateCredentials;
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
use sha2::{Digest, Sha512};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use trade_signal::ArbMode;

use crate::pre_trade::params_load::PreTradeParamsLoader;

type HmacSha512 = Hmac<Sha512>;

const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 10;
const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 30 * 60;
const BLOCK_SUMMARY_INTERVAL_US: i64 = 60_000_000;

thread_local! {
    static GATE_FR_RISK_LIMIT_GUARD: RefCell<Option<GateFrRiskLimitState>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone)]
struct GateFrPositionRisk {
    contract: String,
    risk_limit: f64,
    value: f64,
    cross_leverage_limit: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct GateFrRiskLimitCap {
    pub symbol: String,
    pub contract: String,
    pub risk_limit: f64,
    pub buffer: f64,
    pub cap: f64,
    pub amount_u: f64,
    pub pending_limit_orders: i32,
    pub gate_value: f64,
    pub cross_leverage_limit: Option<f64>,
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
            "Gate risk-limit guard blocked ArbOpen summary: total={} details=[{}] omitted={}",
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
struct GateFrRiskLimitState {
    enabled: bool,
    positions: FastHashMap<String, GateFrPositionRisk>,
    refresh_ctx: Option<GateFrRiskLimitRefreshContext>,
    refresh_in_flight: bool,
    last_refresh_us: i64,
    stats: BlockStats,
}

impl GateFrRiskLimitState {
    fn disabled() -> Self {
        Self {
            enabled: false,
            positions: fast_hash_map(),
            refresh_ctx: None,
            refresh_in_flight: false,
            last_refresh_us: 0,
            stats: BlockStats::new(get_timestamp_us()),
        }
    }

    fn enabled(
        positions: FastHashMap<String, GateFrPositionRisk>,
        refresh_ctx: GateFrRiskLimitRefreshContext,
    ) -> Self {
        Self {
            enabled: true,
            positions,
            refresh_ctx: Some(refresh_ctx),
            refresh_in_flight: false,
            last_refresh_us: get_timestamp_us(),
            stats: BlockStats::new(get_timestamp_us()),
        }
    }
}

#[derive(Debug, Clone)]
struct GateFrRiskLimitConfig {
    env_name: Option<String>,
    arb_mode: ArbMode,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    settle: String,
}

#[derive(Debug, Clone)]
struct GateFrRiskLimitRefreshContext {
    redis: RedisSettings,
    config: GateFrRiskLimitConfig,
}

#[derive(Debug)]
struct GateFrRiskLimitRefreshResult {
    positions: FastHashMap<String, GateFrPositionRisk>,
    online_symbols: usize,
    missing_online: Vec<String>,
}

pub struct GateFrRiskLimitGuard;

impl GateFrRiskLimitGuard {
    pub async fn initialize(
        redis: &RedisSettings,
        env_name: Option<String>,
        arb_mode: ArbMode,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<()> {
        if !is_gate_fr_path(arb_mode, open_venue, hedge_venue) {
            install_guard_state(GateFrRiskLimitState::disabled());
            info!(
                "Gate risk-limit guard disabled: mode={} open={:?} hedge={:?}",
                arb_mode.as_str(),
                open_venue,
                hedge_venue
            );
            return Ok(());
        }

        let config = GateFrRiskLimitConfig {
            env_name: env_name
                .map(|value| value.trim().to_ascii_lowercase())
                .filter(|value| !value.is_empty()),
            arb_mode,
            open_venue,
            hedge_venue,
            settle: gate_settle(),
        };
        let refresh_ctx = GateFrRiskLimitRefreshContext {
            redis: redis.clone(),
            config: config.clone(),
        };
        let refresh_result = refresh_gate_fr_risk_limits(&refresh_ctx).await?;
        let missing_count = refresh_result.missing_online.len();
        install_guard_state(GateFrRiskLimitState::enabled(
            refresh_result.positions,
            refresh_ctx,
        ));
        info!(
            "Gate risk-limit guard startup: env={:?} settle={} positions={} online_symbols={} missing_online={}",
            config.env_name,
            config.settle,
            current_position_count(),
            refresh_result.online_symbols,
            missing_count
        );
        if missing_count > 0 {
            warn!(
                "Gate risk-limit guard online symbols missing from Gate positions snapshot: count={} samples=[{}]",
                missing_count,
                refresh_result.missing_online.iter().take(20).cloned().collect::<Vec<_>>().join(",")
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
            "Gate risk-limit guard background refresh task started (interval: {}s)",
            interval_secs
        );
    }

    fn request_refresh(source: &'static str) {
        let Some(refresh_ctx) = Self::try_begin_refresh() else {
            return;
        };
        tokio::task::spawn_local(async move {
            match refresh_gate_fr_risk_limits(&refresh_ctx).await {
                Ok(result) => Self::apply_refresh_result(source, result),
                Err(err) => warn!(
                    "Gate risk-limit guard refresh failed source={}: {err:#}",
                    source
                ),
            }
            GATE_FR_RISK_LIMIT_GUARD.with(|guard| {
                if let Some(state) = guard.borrow_mut().as_mut() {
                    state.refresh_in_flight = false;
                }
            });
        });
    }

    fn try_begin_refresh() -> Option<GateFrRiskLimitRefreshContext> {
        GATE_FR_RISK_LIMIT_GUARD.with(|guard| {
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

    fn apply_refresh_result(source: &'static str, result: GateFrRiskLimitRefreshResult) {
        let count = result.positions.len();
        let missing_count = result.missing_online.len();
        GATE_FR_RISK_LIMIT_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let Some(state) = guard_ref.as_mut() else {
                return;
            };
            if !state.enabled {
                return;
            }
            state.positions = result.positions;
            state.last_refresh_us = get_timestamp_us();
        });
        info!(
            "Gate risk-limit guard refresh applied: source={} positions={} online_symbols={} missing_online={}",
            source,
            count,
            result.online_symbols,
            missing_count
        );
        if missing_count > 0 {
            warn!(
                "Gate risk-limit guard refresh missing online symbols: count={} samples=[{}]",
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
        if !is_gate_fr_venues(opening_venue, hedging_venue) {
            return false;
        }
        let symbol = normalize_guard_symbol(hedging_symbol)
            .or_else(|| normalize_guard_symbol(opening_symbol))
            .unwrap_or_default();
        if symbol.is_empty() {
            warn!("Gate risk-limit guard blocks ArbOpen: empty symbol");
            return true;
        }

        GATE_FR_RISK_LIMIT_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let Some(state) = guard_ref.as_mut() else {
                warn!(
                    "Gate risk-limit guard blocks ArbOpen: guard not initialized symbol={}",
                    symbol
                );
                return true;
            };
            if !state.enabled {
                return false;
            }
            if state.positions.contains_key(&symbol) {
                return false;
            }
            let now_us = get_timestamp_us();
            state
                .stats
                .record(symbol.clone(), "missing_gate_position_snapshot".to_string());
            state.stats.maybe_log(now_us);
            true
        })
    }

    pub fn cap_for_symbol(symbol: &str, side: Side) -> Result<GateFrRiskLimitCap, String> {
        let symbol = normalize_guard_symbol(symbol)
            .ok_or_else(|| format!("Gate risk_limit symbol invalid: {symbol}"))?;
        GATE_FR_RISK_LIMIT_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let Some(state) = guard_ref.as_mut() else {
                return Err(format!(
                    "Gate risk_limit guard not initialized, reject symbol={}",
                    symbol
                ));
            };
            if !state.enabled {
                return Err("Gate risk_limit guard disabled".to_string());
            }
            let Some(record) = state.positions.get(&symbol) else {
                let now_us = get_timestamp_us();
                state
                    .stats
                    .record(symbol.clone(), "missing_gate_position_snapshot".to_string());
                state.stats.maybe_log(now_us);
                return Err(format!(
                    "Gate risk_limit snapshot missing symbol={}, fail closed",
                    symbol
                ));
            };

            calculate_cap_for_record(&symbol, side, record)
        })
    }

    pub fn ensure_projected_notional(
        symbol: &str,
        side: Side,
        current_futures_base_qty: f64,
        add_futures_base_qty: f64,
        price: f64,
        raw_open_qty: f64,
        open_qty_multiplier: f64,
    ) -> Result<(), String> {
        if !(price.is_finite() && price > 0.0) {
            return Err(format!(
                "Gate risk_limit check missing price symbol={} price={}",
                symbol, price
            ));
        }
        let cap = Self::cap_for_symbol(symbol, side)?;
        let next_qty = current_futures_base_qty + add_futures_base_qty;
        let current_usdt = current_futures_base_qty.abs() * price;
        let order_usdt = add_futures_base_qty.abs() * price;
        let next_usdt = next_qty.abs() * price;
        let eps = 1e-6_f64;
        if next_usdt <= current_usdt + eps {
            return Ok(());
        }
        if next_usdt > cap.cap + eps {
            info!(
                "Gate risk_limit reject detail: symbol={} contract={} side={} price={:.8} current_futures_qty={:.8} add_futures_qty={:.8} next_qty={:.8} current_usdt={:.4} order_usdt={:.4} next_usdt={:.4} risk_limit={:.4} buffer={:.4} cap={:.4} pending_limit_orders={} amount_u={:.4} gate_value={:.4} cross_leverage_limit={:?} raw_open_qty={:.8} open_qty_multiplier={:.8}",
                cap.symbol,
                cap.contract,
                side.as_str(),
                price,
                current_futures_base_qty,
                add_futures_base_qty,
                next_qty,
                current_usdt,
                order_usdt,
                next_usdt,
                cap.risk_limit,
                cap.buffer,
                cap.cap,
                cap.pending_limit_orders,
                cap.amount_u,
                cap.gate_value,
                cap.cross_leverage_limit,
                raw_open_qty,
                open_qty_multiplier
            );
            return Err(format!(
                "Gate risk_limit cap exceeded symbol={} next={:.4}USDT cap={:.4}USDT risk_limit={:.4} buffer={:.4}",
                cap.symbol, next_usdt, cap.cap, cap.risk_limit, cap.buffer
            ));
        }
        Ok(())
    }
}

fn install_guard_state(state: GateFrRiskLimitState) {
    GATE_FR_RISK_LIMIT_GUARD.with(|guard| {
        *guard.borrow_mut() = Some(state);
    });
}

fn current_position_count() -> usize {
    GATE_FR_RISK_LIMIT_GUARD.with(|guard| {
        guard
            .borrow()
            .as_ref()
            .map(|state| state.positions.len())
            .unwrap_or(0)
    })
}

fn is_gate_fr_path(arb_mode: ArbMode, open_venue: TradingVenue, hedge_venue: TradingVenue) -> bool {
    matches!(arb_mode, ArbMode::FundingArb | ArbMode::IntraArb)
        && is_gate_fr_venues(open_venue, hedge_venue)
}

fn is_gate_fr_venues(open_venue: TradingVenue, hedge_venue: TradingVenue) -> bool {
    open_venue == TradingVenue::GateMargin && hedge_venue == TradingVenue::GateFutures
}

fn calculate_cap_for_record(
    symbol: &str,
    side: Side,
    record: &GateFrPositionRisk,
) -> Result<GateFrRiskLimitCap, String> {
    let params = PreTradeParamsLoader::instance();
    let pending_limit_orders = match side {
        Side::Buy => params.arb_max_pending_limit_buy_orders(),
        Side::Sell => params.arb_max_pending_limit_sell_orders(),
    }
    .max(0);
    let amount_u = params.arb_amount_u_for_symbol(symbol);
    if !(amount_u.is_finite() && amount_u > 0.0) {
        return Err(format!(
            "Gate risk_limit amount_u invalid symbol={} amount_u={}",
            symbol, amount_u
        ));
    }
    let buffer = pending_limit_orders as f64 * amount_u;
    let cap = record.risk_limit - buffer;
    if !(cap.is_finite() && cap > 0.0) {
        return Err(format!(
            "Gate risk_limit cap invalid symbol={} risk_limit={:.4} buffer={:.4} pending_limit_orders={} amount_u={:.4}",
            symbol, record.risk_limit, buffer, pending_limit_orders, amount_u
        ));
    }
    Ok(GateFrRiskLimitCap {
        symbol: symbol.to_string(),
        contract: record.contract.clone(),
        risk_limit: record.risk_limit,
        buffer,
        cap,
        amount_u,
        pending_limit_orders,
        gate_value: record.value,
        cross_leverage_limit: record.cross_leverage_limit,
    })
}

async fn refresh_gate_fr_risk_limits(
    refresh_ctx: &GateFrRiskLimitRefreshContext,
) -> Result<GateFrRiskLimitRefreshResult> {
    let client = Client::builder()
        .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
        .build()
        .context("build Gate risk-limit http client")?;
    let positions = fetch_gate_positions(&client, &refresh_ctx.config).await?;
    let online_symbols = match load_online_symbols(&refresh_ctx.redis, &refresh_ctx.config).await {
        Ok(symbols) => symbols,
        Err(err) => {
            warn!("Gate risk-limit guard failed to load Redis online symbols: {err:#}");
            BTreeSet::new()
        }
    };
    let missing_online = online_symbols
        .iter()
        .filter(|symbol| !positions.contains_key(*symbol))
        .cloned()
        .collect::<Vec<_>>();
    Ok(GateFrRiskLimitRefreshResult {
        positions,
        online_symbols: online_symbols.len(),
        missing_online,
    })
}

async fn fetch_gate_positions(
    client: &Client,
    config: &GateFrRiskLimitConfig,
) -> Result<FastHashMap<String, GateFrPositionRisk>> {
    let path = format!("/futures/{}/positions", config.settle);
    let (status, body) = gate_request(client, "GET", &path, &BTreeMap::new()).await?;
    if status >= 300 {
        bail!(
            "Gate positions request failed status={} body={}",
            status,
            truncate(&body, 500)
        );
    }
    let value: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "Gate positions response is not JSON: {}",
            truncate(&body, 500)
        )
    })?;
    let Some(items) = value.as_array() else {
        bail!(
            "Gate positions response is not an array: {}",
            truncate(&body, 500)
        );
    };

    let mut positions = fast_hash_map();
    let mut skipped = 0usize;
    for item in items {
        match parse_gate_position(item) {
            Some((symbol, record)) => {
                positions.insert(symbol, record);
            }
            None => skipped = skipped.saturating_add(1),
        }
    }
    if skipped > 0 {
        warn!(
            "Gate risk-limit guard skipped positions with missing/invalid contract or risk_limit: skipped={}",
            skipped
        );
    }
    debug!(
        "Gate risk-limit guard fetched positions: count={} skipped={}",
        positions.len(),
        skipped
    );
    Ok(positions)
}

fn parse_gate_position(value: &Value) -> Option<(String, GateFrPositionRisk)> {
    let contract = value.get("contract")?.as_str()?.trim().to_ascii_uppercase();
    if contract.is_empty() {
        return None;
    }
    let risk_limit = parse_number_field(value, "risk_limit")?;
    if !(risk_limit.is_finite() && risk_limit > 0.0) {
        return None;
    }
    let symbol = normalize_guard_symbol(&contract)?;
    let record = GateFrPositionRisk {
        contract,
        risk_limit,
        value: parse_number_field(value, "value").unwrap_or(0.0),
        cross_leverage_limit: parse_number_field(value, "cross_leverage_limit"),
    };
    Some((symbol, record))
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
    config: &GateFrRiskLimitConfig,
) -> Result<BTreeSet<String>> {
    let keys = online_symbol_keys(config);
    if keys.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut client = RedisClient::connect(redis.clone()).await?;
    let mut symbols = BTreeSet::new();
    for key in keys {
        let Some(raw) = client.get_string(&key).await? else {
            info!("Gate risk-limit guard Redis key missing: {}", key);
            continue;
        };
        let values = decode_redis_list(&raw, &key)?;
        info!(
            "Gate risk-limit guard Redis key loaded: {} count={}",
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

fn online_symbol_keys(config: &GateFrRiskLimitConfig) -> Vec<String> {
    let venue_suffix = format!(
        "{}_{}",
        config.open_venue.data_pub_slug(),
        config.hedge_venue.data_pub_slug()
    );
    match config.arb_mode {
        ArbMode::FundingArb => {
            let Some(env_name) = config.env_name.as_deref() else {
                return Vec::new();
            };
            [
                "dump_symbols",
                "trade_symbols",
                "fwd_trade_symbols",
                "bwd_trade_symbols",
                "unimmr_close_symbols",
            ]
            .into_iter()
            .map(|name| format!("{}:fr_{}:{}", env_name, name, venue_suffix))
            .collect()
        }
        ArbMode::IntraArb => {
            let exchange_suffix = config.open_venue.trade_engine_exchange();
            let mut keys = vec![
                format!("intra_dump_symbols:{exchange_suffix}"),
                format!("intra_trade_symbols:{exchange_suffix}"),
                format!("intra_fwd_trade_symbols:{exchange_suffix}"),
                format!("intra_bwd_trade_symbols:{exchange_suffix}"),
                format!("intra_unimmr_close_symbols:{exchange_suffix}"),
            ];
            if let Some(env_name) = config.env_name.as_deref() {
                keys.push(format!(
                    "{}:intra_unimmr_close_symbols:{}",
                    env_name, venue_suffix
                ));
            }
            keys
        }
        ArbMode::CrossArb => Vec::new(),
    }
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
    let raw = value.trim();
    if raw.is_empty() {
        return None;
    }
    let mut text = raw.to_ascii_uppercase();
    if let Some((head, _)) = text.split_once('@') {
        text = head.trim().to_string();
    }
    if let Some(stripped) = text.strip_suffix("_USDT") {
        let base = clean_symbol_text(stripped);
        return (!base.is_empty()).then(|| format!("{base}USDT"));
    }
    let normalized = normalize_symbol_for_internal(&text);
    (!normalized.is_empty()).then_some(normalized)
}

fn clean_symbol_text(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect()
}

async fn gate_request(
    client: &Client,
    method: &str,
    path: &str,
    params: &BTreeMap<String, String>,
) -> Result<(u16, String)> {
    let credentials = GateCredentials::from_env()?;
    let base = env_or("GATE_API_BASE", "https://api.gateio.ws");
    let prefix = "/api/v4";
    let query = build_query(params);
    let request_path = format!("{prefix}{path}");
    let timestamp = Utc::now().timestamp();
    let signature = gate_signature(
        &credentials.secret_key,
        method,
        &request_path,
        &query,
        "",
        timestamp,
    );
    let mut url = format!("{}{}", base.trim_end_matches('/'), request_path);
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query);
    }

    let builder = match method {
        "GET" => client.get(url),
        "POST" => client.post(url),
        other => bail!("unsupported Gate REST method {other}"),
    };
    let resp = builder
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .header("X-Gate-Size-Decimal", "1")
        .header("KEY", credentials.api_key)
        .header("Timestamp", timestamp.to_string())
        .header("SIGN", signature)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    Ok((status, body))
}

fn gate_settle() -> String {
    std::env::var("GATE_FUTURES_SETTLE")
        .unwrap_or_else(|_| "usdt".to_string())
        .trim()
        .to_ascii_lowercase()
}

fn refresh_interval_secs() -> u64 {
    std::env::var("PRE_TRADE_GATE_RISK_LIMIT_REFRESH_SECS")
        .or_else(|_| std::env::var("PRE_TRADE_GATE_FR_RISK_LIMIT_REFRESH_SECS"))
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

fn gate_signature(
    secret: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
    timestamp: i64,
) -> String {
    let body_hash = hex::encode(Sha512::digest(body.as_bytes()));
    let payload = format!(
        "{}\n{}\n{}\n{}\n{}",
        method.to_uppercase(),
        path,
        query,
        body_hash,
        timestamp
    );
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes()).expect("HMAC accepts any size");
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

    fn record(risk_limit: f64) -> GateFrPositionRisk {
        GateFrPositionRisk {
            contract: "BTC_USDT".to_string(),
            risk_limit,
            value: 0.0,
            cross_leverage_limit: Some(5.0),
        }
    }

    fn config(
        env_name: Option<&str>,
        arb_mode: ArbMode,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> GateFrRiskLimitConfig {
        GateFrRiskLimitConfig {
            env_name: env_name.map(str::to_string),
            arb_mode,
            open_venue,
            hedge_venue,
            settle: "usdt".to_string(),
        }
    }

    #[test]
    fn gate_risk_limit_path_covers_fr_and_intra_gate() {
        assert!(is_gate_fr_path(
            ArbMode::FundingArb,
            TradingVenue::GateMargin,
            TradingVenue::GateFutures
        ));
        assert!(is_gate_fr_path(
            ArbMode::IntraArb,
            TradingVenue::GateMargin,
            TradingVenue::GateFutures
        ));
        assert!(!is_gate_fr_path(
            ArbMode::CrossArb,
            TradingVenue::GateMargin,
            TradingVenue::GateFutures
        ));
        assert!(!is_gate_fr_path(
            ArbMode::IntraArb,
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures
        ));
    }

    #[test]
    fn builds_fr_online_symbol_keys() {
        let keys = online_symbol_keys(&config(
            Some("gate_fr_arb01"),
            ArbMode::FundingArb,
            TradingVenue::GateMargin,
            TradingVenue::GateFutures,
        ));
        assert_eq!(
            keys,
            vec![
                "gate_fr_arb01:fr_dump_symbols:gate-margin_gate-futures",
                "gate_fr_arb01:fr_trade_symbols:gate-margin_gate-futures",
                "gate_fr_arb01:fr_fwd_trade_symbols:gate-margin_gate-futures",
                "gate_fr_arb01:fr_bwd_trade_symbols:gate-margin_gate-futures",
                "gate_fr_arb01:fr_unimmr_close_symbols:gate-margin_gate-futures",
            ]
        );
    }

    #[test]
    fn builds_intra_online_symbol_keys() {
        let keys = online_symbol_keys(&config(
            Some("gate-intra-arb01"),
            ArbMode::IntraArb,
            TradingVenue::GateMargin,
            TradingVenue::GateFutures,
        ));
        assert_eq!(
            keys,
            vec![
                "intra_dump_symbols:gate",
                "intra_trade_symbols:gate",
                "intra_fwd_trade_symbols:gate",
                "intra_bwd_trade_symbols:gate",
                "intra_unimmr_close_symbols:gate",
                "gate-intra-arb01:intra_unimmr_close_symbols:gate-margin_gate-futures",
            ]
        );
    }

    #[test]
    fn normalize_gate_contract_to_internal_symbol() {
        assert_eq!(
            normalize_guard_symbol("BTC_USDT").as_deref(),
            Some("BTCUSDT")
        );
        assert_eq!(
            normalize_guard_symbol("btc-usdt").as_deref(),
            Some("BTCUSDT")
        );
    }

    #[test]
    fn calculate_cap_uses_side_pending_limit_and_amount_u() {
        let params = PreTradeParamsLoader::instance();
        let mut risk_params = std::collections::HashMap::new();
        risk_params.insert(
            "arb_max_pending_limit_buy_orders".to_string(),
            "3".to_string(),
        );
        risk_params.insert(
            "arb_max_pending_limit_sell_orders".to_string(),
            "2".to_string(),
        );
        risk_params.insert(
            "arb_close_max_pending_limit_buy_orders".to_string(),
            "3".to_string(),
        );
        risk_params.insert(
            "arb_close_max_pending_limit_sell_orders".to_string(),
            "3".to_string(),
        );
        let mut amount_overrides = fast_hash_map();
        amount_overrides.insert("BTCUSDT".to_string(), 250.0);
        params.apply_loaded_params(
            risk_params,
            fast_hash_map(),
            Some(100.0),
            amount_overrides,
            false,
        );

        let buy_cap = calculate_cap_for_record("BTCUSDT", Side::Buy, &record(50_000.0)).unwrap();
        assert_eq!(buy_cap.buffer, 750.0);
        assert_eq!(buy_cap.cap, 49_250.0);

        let sell_cap = calculate_cap_for_record("BTCUSDT", Side::Sell, &record(50_000.0)).unwrap();
        assert_eq!(sell_cap.buffer, 500.0);
        assert_eq!(sell_cap.cap, 49_500.0);
    }
}
