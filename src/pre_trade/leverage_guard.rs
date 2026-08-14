use account_common::bybit_auth::BybitCredentials;
use account_common::gate_auth::GateCredentials;
use account_common::okex_auth::OkexCredentials;
use account_common::BinanceAccountMode;
use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use log::{info, warn};
use order_common::TradingVenue;
use reqwest::Client;
use runtime_common::redis_client::{RedisClient, RedisSettings};
use runtime_common::time_util::get_timestamp_us;
use serde_json::{json, Value};
use sha2::{Digest, Sha256, Sha512};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::IpAddr;
use std::time::Duration;
use trade_engine::bybit_query::build_bybit_rest_client_with_timeout;
use trade_signal::ArbMode;

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

const TARGET_LEVERAGES: [u8; 1] = [5];
const BLOCK_SUMMARY_INTERVAL_US: i64 = 60_000_000;
const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 10;
const DEFAULT_REQUEST_SLEEP_MS: u64 = 120;
const DEFAULT_LEVERAGE_GUARD_REFRESH_SECS: u64 = 60;
const ON_DEMAND_REFRESH_DEBOUNCE_US: i64 = 2_000_000;
const BYBIT_ACCOUNT_INFO_PATH: &str = "/v5/account/info";

thread_local! {
    static LEVERAGE_GUARD: RefCell<Option<LeverageGuardState>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LeverageTarget {
    venue: TradingVenue,
    symbol: String,
}

impl LeverageTarget {
    fn new(venue: TradingVenue, symbol: &str) -> Self {
        Self {
            venue,
            symbol: normalize_online_value_to_internal_symbol(symbol).unwrap_or_else(|| {
                symbol
                    .trim()
                    .to_ascii_uppercase()
                    .replace(['-', '_', '/'], "")
            }),
        }
    }

    fn label(&self) -> String {
        format!("{:?}:{}", self.venue, self.symbol)
    }
}

#[derive(Debug, Clone)]
enum TargetStatus {
    Confirmed(u8),
    Failed { last_error: String },
}

impl TargetStatus {
    fn is_confirmed(&self) -> bool {
        matches!(self, Self::Confirmed(_))
    }

    fn reason_label(&self) -> &'static str {
        match self {
            Self::Confirmed(_) => "confirmed",
            Self::Failed { .. } => "set_failed",
        }
    }
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
    by_key: BTreeMap<String, BlockCounter>,
}

impl BlockStats {
    fn new(now_us: i64) -> Self {
        Self {
            last_log_us: now_us,
            total: 0,
            by_key: BTreeMap::new(),
        }
    }

    fn record(&mut self, key: String, reason: String) {
        self.total = self.total.saturating_add(1);
        let entry = self.by_key.entry(key).or_default();
        entry.count = entry.count.saturating_add(1);
        entry.last_reason = reason;
    }

    fn maybe_log(&mut self, global_block: Option<&GlobalBlock>, now_us: i64) {
        if self.total == 0 {
            self.last_log_us = now_us;
            return;
        }
        if now_us.saturating_sub(self.last_log_us) < BLOCK_SUMMARY_INTERVAL_US {
            return;
        }

        let mut parts = Vec::new();
        for (key, counter) in self.by_key.iter().take(12) {
            parts.push(format!(
                "{} count={} reason={}",
                key, counter.count, counter.last_reason
            ));
        }
        let omitted = self.by_key.len().saturating_sub(parts.len());
        let global_text = global_block
            .map(|block| {
                format!(
                    "true trigger={} reason={}",
                    block.trigger.label(),
                    block.reason
                )
            })
            .unwrap_or_else(|| "false".to_string());
        warn!(
            "ArbOpen leverage guard blocked summary: total={} global_open_block={} details=[{}] omitted={}",
            self.total,
            global_text,
            parts.join("; "),
            omitted
        );

        self.total = 0;
        self.by_key.clear();
        self.last_log_us = now_us;
    }
}

#[derive(Debug, Clone)]
struct GlobalBlock {
    trigger: LeverageTarget,
    reason: String,
}

#[derive(Debug)]
struct LeverageGuardState {
    enabled: bool,
    targets: HashMap<LeverageTarget, TargetStatus>,
    global_block: Option<GlobalBlock>,
    refresh_ctx: Option<GuardRefreshContext>,
    refresh_in_flight: bool,
    last_refresh_request_us: i64,
    stats: BlockStats,
}

impl LeverageGuardState {
    fn disabled() -> Self {
        Self {
            enabled: false,
            targets: HashMap::new(),
            global_block: None,
            refresh_ctx: None,
            refresh_in_flight: false,
            last_refresh_request_us: 0,
            stats: BlockStats::new(get_timestamp_us()),
        }
    }

    fn enabled(
        targets: HashMap<LeverageTarget, TargetStatus>,
        refresh_ctx: GuardRefreshContext,
    ) -> Self {
        Self {
            enabled: true,
            targets,
            global_block: None,
            refresh_ctx: Some(refresh_ctx),
            refresh_in_flight: false,
            last_refresh_request_us: 0,
            stats: BlockStats::new(get_timestamp_us()),
        }
    }
}

#[derive(Debug, Clone)]
struct GuardStartupConfig {
    env_name: String,
    arb_mode: ArbMode,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    binance_account_mode: Option<BinanceAccountMode>,
    bybit_rest_local_ip: Option<IpAddr>,
    target_venues: Vec<TradingVenue>,
    request_sleep_ms: u64,
}

#[derive(Debug, Clone)]
struct GuardRefreshContext {
    redis: RedisSettings,
    config: GuardStartupConfig,
}

#[derive(Debug)]
struct GuardRefreshResult {
    statuses: HashMap<LeverageTarget, TargetStatus>,
    online_symbols: usize,
    desired_targets: usize,
    retried_targets: usize,
}

#[derive(Debug)]
struct GuardRefreshSnapshot {
    existing_targets: HashMap<LeverageTarget, TargetStatus>,
    global_trigger: Option<LeverageTarget>,
    global_block_label: Option<String>,
}

impl GuardRefreshSnapshot {
    fn from_state(state: &LeverageGuardState) -> Self {
        Self {
            existing_targets: state.targets.clone(),
            global_trigger: state
                .global_block
                .as_ref()
                .map(|block| block.trigger.clone()),
            global_block_label: state
                .global_block
                .as_ref()
                .map(|block| block.trigger.label()),
        }
    }
}

pub struct LeverageGuard;

impl LeverageGuard {
    pub async fn initialize(
        redis: &RedisSettings,
        env_name: Option<String>,
        arb_mode: ArbMode,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        binance_account_mode: Option<BinanceAccountMode>,
        bybit_rest_local_ip: Option<IpAddr>,
    ) -> Result<()> {
        if open_venue == hedge_venue {
            install_guard_state(LeverageGuardState::disabled());
            info!(
                "ArbOpen leverage guard disabled: MM-only pre_trade path open_venue=hedge_venue={:?}",
                open_venue
            );
            return Ok(());
        }

        let Some(env_name) = env_name.filter(|value| !value.trim().is_empty()) else {
            bail!(
                "ArbOpen leverage guard requires runtime env name from cwd (mode={})",
                arb_mode.as_str()
            );
        };

        let target_venues =
            resolve_target_futures_venues(arb_mode, open_venue, hedge_venue, bybit_rest_local_ip)
                .await?;
        if target_venues.is_empty() {
            install_guard_state(LeverageGuardState::disabled());
            info!(
                "ArbOpen leverage guard disabled: no applicable futures venue requiring exchange-side leverage set in open={:?} hedge={:?}",
                open_venue, hedge_venue
            );
            return Ok(());
        }

        let config = GuardStartupConfig {
            env_name: env_name.trim().to_ascii_lowercase(),
            arb_mode,
            open_venue,
            hedge_venue,
            binance_account_mode,
            bybit_rest_local_ip,
            target_venues,
            request_sleep_ms: request_sleep_ms(),
        };

        let refresh_ctx = GuardRefreshContext {
            redis: redis.clone(),
            config: config.clone(),
        };

        let refresh_result = refresh_guard_targets(&refresh_ctx).await?;
        info!(
            "ArbOpen leverage guard startup: env={} mode={} online_symbols={} targets={} target_venues={:?} levels={:?}",
            config.env_name,
            config.arb_mode.as_str(),
            refresh_result.online_symbols,
            refresh_result.desired_targets,
            config.target_venues,
            TARGET_LEVERAGES
        );

        log_startup_summary(&refresh_result.statuses);
        install_guard_state(LeverageGuardState::enabled(
            refresh_result.statuses,
            refresh_ctx,
        ));
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
            "ArbOpen leverage guard background refresh task started (interval: {}s)",
            interval_secs
        );
    }

    fn request_refresh(source: &'static str) {
        let now_us = get_timestamp_us();
        let Some(refresh_ctx) = Self::try_begin_refresh(source, now_us) else {
            return;
        };

        tokio::task::spawn_local(async move {
            if let Err(err) = Self::run_refresh(refresh_ctx, source).await {
                warn!(
                    "ArbOpen leverage guard refresh failed source={}: {err:#}",
                    source
                );
            }
            LEVERAGE_GUARD.with(|guard| {
                if let Some(state) = guard.borrow_mut().as_mut() {
                    state.refresh_in_flight = false;
                }
            });
        });
    }

    fn try_begin_refresh(source: &'static str, now_us: i64) -> Option<GuardRefreshContext> {
        LEVERAGE_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let state = guard_ref.as_mut()?;
            if !state.enabled {
                return None;
            }
            let refresh_ctx = state.refresh_ctx.clone()?;
            if state.refresh_in_flight {
                return None;
            }
            if source != "background_interval"
                && state.last_refresh_request_us > 0
                && now_us.saturating_sub(state.last_refresh_request_us)
                    < ON_DEMAND_REFRESH_DEBOUNCE_US
            {
                return None;
            }
            state.refresh_in_flight = true;
            state.last_refresh_request_us = now_us;
            Some(refresh_ctx)
        })
    }

    async fn run_refresh(refresh_ctx: GuardRefreshContext, source: &'static str) -> Result<()> {
        let snapshot = current_refresh_snapshot();
        let refresh_result = refresh_guard_targets_with_state(
            &refresh_ctx,
            snapshot.existing_targets,
            snapshot.global_trigger,
        )
        .await?;
        Self::apply_refresh_result(source, snapshot.global_block_label, refresh_result);
        Ok(())
    }

    fn apply_refresh_result(
        source: &'static str,
        had_global_block: Option<String>,
        refresh_result: GuardRefreshResult,
    ) {
        let confirmed = refresh_result
            .statuses
            .values()
            .filter(|status| status.is_confirmed())
            .count();
        let failed = refresh_result.statuses.len().saturating_sub(confirmed);
        let mut cleared_global = None::<String>;

        LEVERAGE_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let Some(state) = guard_ref.as_mut() else {
                return;
            };
            if !state.enabled {
                return;
            }
            state.targets = refresh_result.statuses.clone();
            if let Some(global) = state.global_block.clone() {
                if matches!(
                    state.targets.get(&global.trigger),
                    Some(status) if status.is_confirmed()
                ) || (global.trigger.venue == TradingVenue::BitgetFutures
                    && matches!(
                        state.targets.get(&global.trigger),
                        Some(TargetStatus::Failed { .. })
                    ))
                {
                    cleared_global = Some(global.trigger.label());
                    state.global_block = None;
                }
            }
        });

        if let Some(trigger) = cleared_global.as_deref() {
            info!(
                "ArbOpen leverage guard global block cleared: source={} trigger={}",
                source, trigger
            );
        }

        if source != "background_interval"
            || refresh_result.retried_targets > 0
            || had_global_block.is_some()
            || cleared_global.is_some()
        {
            info!(
                "ArbOpen leverage guard refresh applied: source={} online_symbols={} desired_targets={} retried_targets={} confirmed={} failed={}",
                source,
                refresh_result.online_symbols,
                refresh_result.desired_targets,
                refresh_result.retried_targets,
                confirmed,
                failed
            );
        }

        if failed > 0 && (source != "background_interval" || refresh_result.retried_targets > 0) {
            let mut failed_samples = Vec::new();
            for (target, status) in &refresh_result.statuses {
                if let TargetStatus::Failed { last_error } = status {
                    if failed_samples.len() >= 12 {
                        break;
                    }
                    failed_samples.push(format!(
                        "{} err={}",
                        target.label(),
                        truncate(last_error, 180)
                    ));
                }
            }
            warn!(
                "ArbOpen leverage guard unresolved targets: source={} failed={} samples=[{}]",
                source,
                failed,
                failed_samples.join("; ")
            );
        }
    }

    pub fn should_block_arb_open(
        opening_symbol: &str,
        opening_venue: TradingVenue,
        hedging_symbol: &str,
        hedging_venue: TradingVenue,
    ) -> bool {
        let targets =
            arb_open_targets(opening_symbol, opening_venue, hedging_symbol, hedging_venue);
        if targets.is_empty() {
            return false;
        }

        let mut request_refresh = false;
        let blocked = LEVERAGE_GUARD.with(|guard| {
            let mut guard_ref = guard.borrow_mut();
            let Some(state) = guard_ref.as_mut() else {
                return false;
            };
            if !state.enabled {
                return false;
            }

            let now_us = get_timestamp_us();
            if let Some(global) = state.global_block.clone() {
                state
                    .stats
                    .record("global_open_block".to_string(), global.reason.clone());
                state.stats.maybe_log(state.global_block.as_ref(), now_us);
                return true;
            }

            let mut blocked = false;
            for target in targets {
                match state.targets.get(&target) {
                    Some(status) if status.is_confirmed() => {}
                    Some(status) => {
                        if target.venue == TradingVenue::BitgetFutures {
                            continue;
                        }
                        let reason = match status {
                            TargetStatus::Confirmed(_) => "confirmed".to_string(),
                            TargetStatus::Failed { last_error } => {
                                format!("{}:{}", status.reason_label(), truncate(last_error, 180))
                            }
                        };
                        state.stats.record(target.label(), reason);
                        blocked = true;
                    }
                    None => {
                        state.global_block = Some(GlobalBlock {
                            trigger: target.clone(),
                            reason: "unknown_symbol_or_venue".to_string(),
                        });
                        state
                            .stats
                            .record(target.label(), "unknown_symbol_or_venue".to_string());
                        blocked = true;
                        request_refresh = true;
                    }
                }
            }
            state.stats.maybe_log(state.global_block.as_ref(), now_us);
            blocked
        });

        if request_refresh {
            Self::request_refresh("arb_open_blocked");
        }

        blocked
    }
}

fn install_guard_state(state: LeverageGuardState) {
    LEVERAGE_GUARD.with(|guard| {
        *guard.borrow_mut() = Some(state);
    });
}

fn refresh_interval_secs() -> u64 {
    std::env::var("PRE_TRADE_LEVERAGE_GUARD_REFRESH_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_LEVERAGE_GUARD_REFRESH_SECS)
}

async fn refresh_guard_targets(refresh_ctx: &GuardRefreshContext) -> Result<GuardRefreshResult> {
    let snapshot = current_refresh_snapshot();
    refresh_guard_targets_with_state(
        refresh_ctx,
        snapshot.existing_targets,
        snapshot.global_trigger,
    )
    .await
}

async fn refresh_guard_targets_with_state(
    refresh_ctx: &GuardRefreshContext,
    existing_targets: HashMap<LeverageTarget, TargetStatus>,
    global_trigger: Option<LeverageTarget>,
) -> Result<GuardRefreshResult> {
    let keys = online_symbol_keys(&refresh_ctx.config);
    let symbols = load_online_symbols(&refresh_ctx.redis, &keys)
        .await
        .with_context(|| "load online symbols for ArbOpen leverage guard refresh")?;
    let discovered_targets = build_targets(&symbols, &refresh_ctx.config.target_venues);

    let mut desired_targets_map: BTreeMap<String, LeverageTarget> = discovered_targets
        .into_iter()
        .map(|target| (target.label(), target))
        .collect();
    if let Some(trigger) = global_trigger {
        desired_targets_map.insert(trigger.label(), trigger);
    }
    let desired_targets: Vec<LeverageTarget> = desired_targets_map.into_values().collect();

    let client = Client::builder()
        .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
        .build()
        .context("build leverage guard http client")?;
    let bybit_client = refresh_ctx
        .config
        .bybit_rest_local_ip
        .map(|local_ip| {
            build_bybit_rest_client_with_timeout(
                Some(local_ip),
                Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS),
            )
            .context("build Bybit leverage guard http client")
        })
        .transpose()?;

    let mut statuses = existing_targets;

    let mut retried_targets = 0usize;
    let desired_targets_len = desired_targets.len();
    for (idx, target) in desired_targets.iter().enumerate() {
        if statuses.contains_key(target) {
            continue;
        }
        retried_targets += 1;
        let target_client = if target.venue == TradingVenue::BybitFutures {
            bybit_client.as_ref().unwrap_or(&client)
        } else {
            &client
        };
        let status = set_target_with_fallback(target_client, &refresh_ctx.config, target).await;
        statuses.insert(target.clone(), status);
        if idx + 1 < desired_targets_len && refresh_ctx.config.request_sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(refresh_ctx.config.request_sleep_ms)).await;
        }
    }

    Ok(GuardRefreshResult {
        statuses,
        online_symbols: symbols.len(),
        desired_targets: desired_targets_len,
        retried_targets,
    })
}

fn current_refresh_snapshot() -> GuardRefreshSnapshot {
    LEVERAGE_GUARD.with(|guard| {
        guard
            .borrow()
            .as_ref()
            .map(GuardRefreshSnapshot::from_state)
            .unwrap_or_else(|| GuardRefreshSnapshot {
                existing_targets: HashMap::new(),
                global_trigger: None,
                global_block_label: None,
            })
    })
}

async fn resolve_target_futures_venues(
    arb_mode: ArbMode,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    bybit_rest_local_ip: Option<IpAddr>,
) -> Result<Vec<TradingVenue>> {
    let mut venues = target_futures_venues(arb_mode, open_venue, hedge_venue);
    if venues.contains(&TradingVenue::BybitFutures) {
        match bybit_account_is_pm(bybit_rest_local_ip).await {
            Ok(true) => {
                info!(
                    "ArbOpen leverage guard skips BybitFutures set-leverage: account is Portfolio Margin"
                );
                venues.retain(|venue| *venue != TradingVenue::BybitFutures);
            }
            Ok(false) => {}
            Err(err) => warn!(
                "ArbOpen leverage guard failed to detect Bybit account margin mode; retaining BybitFutures leverage guard: {err:#}"
            ),
        }
    }
    Ok(venues)
}

fn target_futures_venues(
    _arb_mode: ArbMode,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Vec<TradingVenue> {
    let mut seen = HashSet::new();
    let mut venues = Vec::new();
    for venue in [open_venue, hedge_venue] {
        if venue.is_futures() && seen.insert(venue) {
            venues.push(venue);
        }
    }
    venues
}

async fn bybit_account_is_pm(local_ip: Option<IpAddr>) -> Result<bool> {
    let credentials = BybitCredentials::from_env()?;
    let client = build_bybit_rest_client_with_timeout(
        local_ip,
        Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS),
    )
    .context("build bybit account info http client")?;
    let base = env_or("BYBIT_API_BASE", "https://api.bybit.com");
    let timestamp = Utc::now().timestamp_millis().to_string();
    let recv_window = "5000";
    let signature = hmac_sha256_hex(
        &credentials.secret_key,
        &format!("{}{}{}", timestamp, credentials.api_key, recv_window),
    );
    let resp = client
        .get(format!(
            "{}{}",
            base.trim_end_matches('/'),
            BYBIT_ACCOUNT_INFO_PATH
        ))
        .header("X-BAPI-API-KEY", credentials.api_key)
        .header("X-BAPI-SIGN", signature)
        .header("X-BAPI-SIGN-TYPE", "2")
        .header("X-BAPI-TIMESTAMP", timestamp)
        .header("X-BAPI-RECV-WINDOW", recv_window)
        .send()
        .await
        .with_context(|| format!("GET {BYBIT_ACCOUNT_INFO_PATH} failed"))?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if status != 200 {
        bail!(
            "GET {BYBIT_ACCOUNT_INFO_PATH} http_status={} body={}",
            status,
            truncate(&body, 500)
        );
    }
    let value: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "GET {BYBIT_ACCOUNT_INFO_PATH} response not JSON: {}",
            truncate(&body, 500)
        )
    })?;
    let ret_code = value.get("retCode");
    let ok = matches!(ret_code, Some(Value::Number(num)) if num.as_i64() == Some(0))
        || matches!(ret_code, Some(Value::String(code)) if code == "0");
    if !ok {
        let ret_msg = value
            .get("retMsg")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        bail!("GET {BYBIT_ACCOUNT_INFO_PATH} retCode={ret_code:?} retMsg={ret_msg}");
    }

    let result = value.get("result").unwrap_or(&Value::Null);
    let unified_status = result
        .get("unifiedMarginStatus")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let margin_mode = result
        .get("marginMode")
        .and_then(Value::as_str)
        .unwrap_or("");
    let is_pm = unified_status == 6 || margin_mode.eq_ignore_ascii_case("PORTFOLIO_MARGIN");
    info!(
        "ArbOpen leverage guard Bybit account mode: unifiedMarginStatus={} marginMode={} portfolio_margin={}",
        unified_status, margin_mode, is_pm
    );
    Ok(is_pm)
}

fn online_symbol_keys(config: &GuardStartupConfig) -> Vec<String> {
    let venue_suffix = format!(
        "{}_{}",
        config.open_venue.data_pub_slug(),
        config.hedge_venue.data_pub_slug()
    );
    match config.arb_mode {
        ArbMode::FundingArb => [
            "dump_symbols",
            "trade_symbols",
            "fwd_trade_symbols",
            "bwd_trade_symbols",
            "unimmr_close_symbols",
        ]
        .into_iter()
        .map(|name| format!("{}:fr_{}:{}", config.env_name, name, venue_suffix))
        .collect(),
        ArbMode::IntraArb => {
            let exchange_suffix = config.open_venue.trade_engine_exchange();
            vec![
                format!("{}:intra_dump_symbols:{exchange_suffix}", config.env_name),
                format!("{}:intra_trade_symbols:{exchange_suffix}", config.env_name),
                format!(
                    "{}:intra_fwd_trade_symbols:{exchange_suffix}",
                    config.env_name
                ),
                format!(
                    "{}:intra_bwd_trade_symbols:{exchange_suffix}",
                    config.env_name
                ),
            ]
        }
        ArbMode::CrossArb => {
            let key_suffix = format!(
                "{}-{}",
                config.open_venue.trade_engine_exchange(),
                config.hedge_venue.trade_engine_exchange()
            );
            vec![
                format!("cross_dump_symbols:{key_suffix}"),
                format!("cross_fwd_trade_symbols:{key_suffix}"),
                format!("cross_bwd_trade_symbols:{key_suffix}"),
                format!(
                    "{}:cross_unimmr_close_symbols:{}",
                    config.env_name, venue_suffix
                ),
            ]
        }
    }
}

async fn load_online_symbols(redis: &RedisSettings, keys: &[String]) -> Result<BTreeSet<String>> {
    let mut client = RedisClient::connect(redis.clone()).await?;
    let mut symbols = BTreeSet::new();
    for key in keys {
        let Some(raw) = client.get_string(key).await? else {
            info!("ArbOpen leverage guard Redis key missing: {}", key);
            continue;
        };
        let values = decode_redis_list(&raw, key)?;
        info!(
            "ArbOpen leverage guard Redis key loaded: {} count={}",
            key,
            values.len()
        );
        for value in values {
            if let Some(symbol) = normalize_online_value_to_internal_symbol(&value) {
                symbols.insert(symbol);
            }
        }
    }
    Ok(symbols)
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

fn build_targets(
    symbols: &BTreeSet<String>,
    target_venues: &[TradingVenue],
) -> Vec<LeverageTarget> {
    let mut targets = Vec::new();
    for symbol in symbols {
        for venue in target_venues {
            targets.push(LeverageTarget::new(*venue, symbol));
        }
    }
    targets.sort_by_key(|target| target.label());
    targets
}

fn normalize_online_value_to_internal_symbol(value: &str) -> Option<String> {
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
        Some(format!("{asset}USDT"))
    }
}

fn clean_symbol_text(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect()
}

fn arb_open_targets(
    opening_symbol: &str,
    opening_venue: TradingVenue,
    hedging_symbol: &str,
    hedging_venue: TradingVenue,
) -> Vec<LeverageTarget> {
    let mut targets = Vec::new();
    if opening_venue.is_futures() {
        targets.push(LeverageTarget::new(opening_venue, opening_symbol));
    }
    if hedging_venue.is_futures() {
        let target = LeverageTarget::new(hedging_venue, hedging_symbol);
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    targets
}

async fn set_target_with_fallback(
    client: &Client,
    config: &GuardStartupConfig,
    target: &LeverageTarget,
) -> TargetStatus {
    let mut last_error = String::new();
    for leverage in TARGET_LEVERAGES {
        match set_target_leverage(client, config, target, leverage).await {
            Ok(()) => return TargetStatus::Confirmed(leverage),
            Err(err) => {
                last_error = format!("{err:#}");
            }
        }
    }
    TargetStatus::Failed { last_error }
}

async fn set_target_leverage(
    client: &Client,
    config: &GuardStartupConfig,
    target: &LeverageTarget,
    leverage: u8,
) -> Result<()> {
    let symbol = symbol_for_venue(&target.symbol, target.venue);
    match target.venue {
        TradingVenue::BinanceFutures => {
            set_binance_leverage(client, &symbol, leverage, config.binance_account_mode).await
        }
        TradingVenue::OkexFutures => set_okx_leverage(client, &symbol, leverage).await,
        TradingVenue::BybitFutures => set_bybit_leverage(client, &symbol, leverage).await,
        TradingVenue::BitgetFutures => set_bitget_leverage(client, &symbol, leverage).await,
        TradingVenue::GateFutures => set_gate_leverage(client, &symbol, leverage).await,
        other => bail!("unsupported futures venue for leverage guard: {:?}", other),
    }
}

fn symbol_for_venue(symbol: &str, venue: TradingVenue) -> String {
    let internal = normalize_online_value_to_internal_symbol(symbol)
        .unwrap_or_else(|| clean_symbol_text(&symbol.to_ascii_uppercase()));
    let base = internal
        .strip_suffix("USDT")
        .filter(|value| !value.is_empty())
        .unwrap_or(internal.as_str());
    match venue {
        TradingVenue::OkexFutures => format!("{base}-USDT-SWAP"),
        TradingVenue::GateFutures => format!("{base}_USDT"),
        _ => format!("{base}USDT"),
    }
}

async fn set_binance_leverage(
    client: &Client,
    symbol: &str,
    leverage: u8,
    account_mode: Option<BinanceAccountMode>,
) -> Result<()> {
    let api_key = required_env("BINANCE_API_KEY")?;
    let api_secret = required_env("BINANCE_API_SECRET")?;
    let (base, path) = match account_mode {
        Some(BinanceAccountMode::Standard) => (
            env_or("BINANCE_FAPI_URL", "https://fapi.binance.com"),
            "/fapi/v1/leverage",
        ),
        Some(BinanceAccountMode::Unified) => (
            env_or("BINANCE_PAPI_URL", "https://papi.binance.com"),
            "/papi/v1/um/leverage",
        ),
        None => bail!("BINANCE_ACCOUNT_MODE not initialized"),
    };
    let mut params = BTreeMap::new();
    params.insert("leverage".to_string(), leverage.to_string());
    params.insert("recvWindow".to_string(), "5000".to_string());
    params.insert("symbol".to_string(), symbol.to_string());
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
        .post(url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if !(200..300).contains(&status) {
        bail!(
            "binance set leverage failed symbol={} leverage={} status={} body={}",
            symbol,
            leverage,
            status,
            truncate(&body, 500)
        );
    }
    Ok(())
}

async fn set_okx_leverage(client: &Client, symbol: &str, leverage: u8) -> Result<()> {
    let credentials = OkexCredentials::from_env()?;
    let base = env_or("OKX_BASE_URL", "https://www.okx.com");
    let path = "/api/v5/account/set-leverage";
    let body = serde_json::to_string(&json!({
        "instId": symbol,
        "lever": leverage.to_string(),
        "mgnMode": "cross",
    }))?;
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let signature = hmac_sha256_base64(
        &credentials.secret_key,
        &format!("{timestamp}POST{path}{body}"),
    );
    let resp = client
        .post(format!("{}{}", base.trim_end_matches('/'), path))
        .header("OK-ACCESS-KEY", credentials.api_key)
        .header("OK-ACCESS-SIGN", signature)
        .header("OK-ACCESS-TIMESTAMP", timestamp)
        .header("OK-ACCESS-PASSPHRASE", credentials.passphrase)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    let value: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let ok = (200..300).contains(&status)
        && value
            .get("code")
            .and_then(Value::as_str)
            .map(|code| code == "0")
            .unwrap_or(false);
    if !ok {
        bail!(
            "okx set leverage failed symbol={} leverage={} status={} body={}",
            symbol,
            leverage,
            status,
            truncate(&body, 500)
        );
    }
    Ok(())
}

async fn set_bybit_leverage(client: &Client, symbol: &str, leverage: u8) -> Result<()> {
    let credentials = BybitCredentials::from_env()?;
    let base = env_or("BYBIT_API_BASE", "https://api.bybit.com");
    let path = "/v5/position/set-leverage";
    let body = serde_json::to_string(&json!({
        "category": "linear",
        "symbol": symbol,
        "buyLeverage": leverage.to_string(),
        "sellLeverage": leverage.to_string(),
    }))?;
    let timestamp = Utc::now().timestamp_millis().to_string();
    let recv_window = "5000";
    let signature = hmac_sha256_hex(
        &credentials.secret_key,
        &format!(
            "{}{}{}{}",
            timestamp, credentials.api_key, recv_window, body
        ),
    );
    let resp = client
        .post(format!("{}{}", base.trim_end_matches('/'), path))
        .header("X-BAPI-API-KEY", credentials.api_key)
        .header("X-BAPI-SIGN", signature)
        .header("X-BAPI-SIGN-TYPE", "2")
        .header("X-BAPI-TIMESTAMP", timestamp)
        .header("X-BAPI-RECV-WINDOW", recv_window)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    let value: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let ret_code = value.get("retCode");
    let ret_code_ok = matches!(
        ret_code,
        Some(Value::Number(num)) if num.as_i64() == Some(0) || num.as_i64() == Some(110043)
    ) || matches!(
        ret_code,
        Some(Value::String(code)) if code == "0" || code == "110043"
    );
    let ok = (200..300).contains(&status) && ret_code_ok;
    if !ok {
        bail!(
            "bybit set leverage failed symbol={} leverage={} status={} body={}",
            symbol,
            leverage,
            status,
            truncate(&body, 500)
        );
    }
    Ok(())
}

async fn set_bitget_leverage(client: &Client, symbol: &str, leverage: u8) -> Result<()> {
    let api_key = required_env("BITGET_API_KEY")?;
    let api_secret = required_env("BITGET_API_SECRET")?;
    let passphrase = std::env::var("BITGET_API_PASSPHRASE")
        .or_else(|_| std::env::var("BITGET_PASSPHRASE"))
        .map(|value| value.trim().to_string())
        .map_err(|_| anyhow!("BITGET_API_PASSPHRASE not set"))?;
    if passphrase.is_empty() {
        bail!("BITGET_API_PASSPHRASE is empty");
    }

    let base = env_or("BITGET_API_BASE", "https://api.bitget.com");
    let path = "/api/v3/account/set-leverage";
    let body = serde_json::to_string(&json!({
        "category": "USDT-FUTURES",
        "symbol": symbol,
        "leverage": leverage.to_string(),
    }))?;
    let timestamp = Utc::now().timestamp_millis().to_string();
    let signature = hmac_sha256_base64(&api_secret, &format!("{timestamp}POST{path}{body}"));
    let resp = client
        .post(format!("{}{}", base.trim_end_matches('/'), path))
        .header("ACCESS-KEY", api_key)
        .header("ACCESS-SIGN", signature)
        .header("ACCESS-TIMESTAMP", timestamp)
        .header("ACCESS-PASSPHRASE", passphrase)
        .header("Content-Type", "application/json")
        .header("locale", "en-US")
        .body(body)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    let value: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let code = value.get("code").and_then(Value::as_str).unwrap_or("");
    if !(200..300).contains(&status) || !matches!(code, "00000" | "0") {
        bail!(
            "bitget set leverage failed symbol={} leverage={} status={} body={}",
            symbol,
            leverage,
            status,
            truncate(&body, 500)
        );
    }
    Ok(())
}

async fn set_gate_leverage(client: &Client, contract: &str, leverage: u8) -> Result<()> {
    let settle = std::env::var("GATE_FUTURES_SETTLE")
        .unwrap_or_else(|_| "usdt".to_string())
        .trim()
        .to_ascii_lowercase();
    let path = format!("/futures/{settle}/positions/{contract}");
    let (status, before) = gate_request(client, "GET", &path, &BTreeMap::new()).await?;
    if status >= 300 && !gate_missing_position(&before) {
        bail!(
            "gate get position failed contract={} status={} body={}",
            contract,
            status,
            truncate(&before, 500)
        );
    }

    ensure_gate_single_position_mode(client, &settle).await?;

    let mut params = BTreeMap::new();
    params.insert("cross_leverage_limit".to_string(), leverage.to_string());
    params.insert("leverage".to_string(), "0".to_string());
    let (status, body) = gate_request(client, "POST", &format!("{path}/leverage"), &params).await?;
    if status >= 300 {
        bail!(
            "gate set leverage failed contract={} leverage={} status={} body={}",
            contract,
            leverage,
            status,
            truncate(&body, 500)
        );
    }
    Ok(())
}

async fn ensure_gate_single_position_mode(client: &Client, settle: &str) -> Result<()> {
    let path = format!("/futures/{settle}/accounts");
    let (status, body) = gate_request(client, "GET", &path, &BTreeMap::new()).await?;
    if status >= 300 {
        bail!(
            "gate get account mode failed settle={} status={} body={}",
            settle,
            status,
            truncate(&body, 500)
        );
    }
    let mode = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| gate_account_position_mode(&value))
        .unwrap_or_else(|| "unknown".to_string());
    if mode == "single" {
        return Ok(());
    }
    let mut params = BTreeMap::new();
    params.insert("position_mode".to_string(), "single".to_string());
    let (status, body) = gate_request(
        client,
        "POST",
        &format!("/futures/{settle}/set_position_mode"),
        &params,
    )
    .await?;
    if status >= 300 {
        bail!(
            "gate set position_mode=single failed settle={} from={} status={} body={}",
            settle,
            mode,
            status,
            truncate(&body, 500)
        );
    }
    Ok(())
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
        .header("KEY", credentials.api_key)
        .header("Timestamp", timestamp.to_string())
        .header("SIGN", signature)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    Ok((status, body))
}

fn gate_missing_position(body: &str) -> bool {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("label")
                .and_then(Value::as_str)
                .map(|label| label.eq_ignore_ascii_case("POSITION_NOT_FOUND"))
        })
        .unwrap_or(false)
}

fn gate_account_position_mode(value: &Value) -> Option<String> {
    if let Some(mode) = value
        .get("position_mode")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|mode| !mode.is_empty())
    {
        return Some(mode.to_ascii_lowercase());
    }
    value
        .get("in_dual_mode")
        .and_then(Value::as_bool)
        .map(|dual| {
            if dual {
                "dual".to_string()
            } else {
                "single".to_string()
            }
        })
}

fn log_startup_summary(statuses: &HashMap<LeverageTarget, TargetStatus>) {
    let confirmed = statuses
        .values()
        .filter(|status| status.is_confirmed())
        .count();
    let failed = statuses.len().saturating_sub(confirmed);
    let mut level_counts: BTreeMap<u8, usize> = BTreeMap::new();
    let mut failed_samples = Vec::new();
    for (target, status) in statuses {
        match status {
            TargetStatus::Confirmed(level) => {
                *level_counts.entry(*level).or_default() += 1;
            }
            TargetStatus::Failed { last_error } => {
                if failed_samples.len() < 12 {
                    failed_samples.push(format!(
                        "{} err={}",
                        target.label(),
                        truncate(last_error, 180)
                    ));
                }
            }
        }
    }
    info!(
        "ArbOpen leverage guard initialized: targets={} confirmed={} failed={} levels={:?}",
        statuses.len(),
        confirmed,
        failed,
        level_counts
    );
    if failed > 0 {
        warn!(
            "ArbOpen leverage guard failed targets: failed={} samples=[{}]",
            failed,
            failed_samples.join("; ")
        );
    }
}

fn request_sleep_ms() -> u64 {
    std::env::var("PRE_TRADE_LEVERAGE_GUARD_SLEEP_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_REQUEST_SLEEP_MS)
}

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name)
        .map_err(|_| anyhow!("{name} not set"))?
        .trim()
        .to_string();
    if value.is_empty() {
        bail!("{name} is empty");
    }
    Ok(value)
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

fn hmac_sha256_base64(secret: &str, payload: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any size");
    mac.update(payload.as_bytes());
    BASE64.encode(mac.finalize().into_bytes())
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

    fn cfg(
        env_name: &str,
        arb_mode: ArbMode,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> GuardStartupConfig {
        GuardStartupConfig {
            env_name: env_name.to_string(),
            arb_mode,
            open_venue,
            hedge_venue,
            binance_account_mode: None,
            bybit_rest_local_ip: None,
            target_venues: target_futures_venues(arb_mode, open_venue, hedge_venue),
            request_sleep_ms: 0,
        }
    }

    #[test]
    fn builds_fr_online_symbol_keys() {
        let keys = online_symbol_keys(&cfg(
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
    fn builds_intra_online_symbol_keys() {
        let keys = online_symbol_keys(&cfg(
            "gate-intra-arb01",
            ArbMode::IntraArb,
            TradingVenue::GateMargin,
            TradingVenue::GateFutures,
        ));
        assert_eq!(
            keys,
            vec![
                "gate-intra-arb01:intra_dump_symbols:gate",
                "gate-intra-arb01:intra_trade_symbols:gate",
                "gate-intra-arb01:intra_fwd_trade_symbols:gate",
                "gate-intra-arb01:intra_bwd_trade_symbols:gate",
            ]
        );
    }

    #[test]
    fn builds_cross_online_symbol_keys() {
        let keys = online_symbol_keys(&cfg(
            "bitget-gate-cross-arb01",
            ArbMode::CrossArb,
            TradingVenue::BitgetFutures,
            TradingVenue::GateFutures,
        ));
        assert_eq!(
            keys,
            vec![
                "cross_dump_symbols:bitget-gate",
                "cross_fwd_trade_symbols:bitget-gate",
                "cross_bwd_trade_symbols:bitget-gate",
                "bitget-gate-cross-arb01:cross_unimmr_close_symbols:bitget-futures_gate-futures",
            ]
        );
    }

    #[test]
    fn normalizes_online_symbols_to_internal_usdt() {
        assert_eq!(
            normalize_online_value_to_internal_symbol("HNT"),
            Some("HNTUSDT".to_string())
        );
        assert_eq!(
            normalize_online_value_to_internal_symbol("HNT_USDT"),
            Some("HNTUSDT".to_string())
        );
        assert_eq!(
            normalize_online_value_to_internal_symbol("HNT-USDT-SWAP@x"),
            Some("HNTUSDT".to_string())
        );
        assert_eq!(
            normalize_online_value_to_internal_symbol("hnt/usdt/swap"),
            Some("HNTUSDT".to_string())
        );
        assert_eq!(
            normalize_online_value_to_internal_symbol("HNT-USDT-260628"),
            Some("HNTUSDT".to_string())
        );
    }

    #[test]
    fn formats_symbol_for_futures_venues() {
        assert_eq!(
            symbol_for_venue("HNTUSDT", TradingVenue::BinanceFutures),
            "HNTUSDT"
        );
        assert_eq!(
            symbol_for_venue("HNTUSDT", TradingVenue::OkexFutures),
            "HNT-USDT-SWAP"
        );
        assert_eq!(
            symbol_for_venue("HNTUSDT", TradingVenue::GateFutures),
            "HNT_USDT"
        );
    }

    #[test]
    fn cross_targets_include_both_futures_venues() {
        let venues = target_futures_venues(
            ArbMode::CrossArb,
            TradingVenue::BitgetFutures,
            TradingVenue::GateFutures,
        );
        assert_eq!(
            venues,
            vec![TradingVenue::BitgetFutures, TradingVenue::GateFutures]
        );
    }

    #[test]
    fn arb_open_targets_only_include_futures_legs() {
        let targets = arb_open_targets(
            "HNTUSDT",
            TradingVenue::GateMargin,
            "HNT_USDT",
            TradingVenue::GateFutures,
        );
        assert_eq!(
            targets,
            vec![LeverageTarget::new(TradingVenue::GateFutures, "HNTUSDT")]
        );
    }

    #[test]
    fn refresh_request_marks_in_flight_without_blocking_wait() {
        install_guard_state(LeverageGuardState::enabled(
            std::collections::HashMap::new(),
            GuardRefreshContext {
                redis: RedisSettings::default(),
                config: cfg(
                    "gate-intra-arb01",
                    ArbMode::IntraArb,
                    TradingVenue::GateMargin,
                    TradingVenue::GateFutures,
                ),
            },
        ));

        let now_us = 1_000_000;
        assert!(LeverageGuard::try_begin_refresh("arb_open_blocked", now_us).is_some());
        LEVERAGE_GUARD.with(|guard| {
            let guard_ref = guard.borrow();
            let state = guard_ref.as_ref().expect("guard state");
            assert!(state.refresh_in_flight);
            assert_eq!(state.last_refresh_request_us, now_us);
        });
        assert!(LeverageGuard::try_begin_refresh("arb_open_blocked", now_us + 1).is_none());

        install_guard_state(LeverageGuardState::disabled());
    }
}
