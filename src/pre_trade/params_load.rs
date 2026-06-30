use anyhow::Result;
use log::{debug, info, warn};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use order_common::TradingVenue;
use runtime_common::fast_hash::{fast_hash_map, fast_hash_map_with_capacity, FastHashMap};
use runtime_common::redis_client::{BlockingRedisClient, RedisClient, RedisSettings};
use runtime_common::symbol_util::normalize_symbol_for_internal;

/// Redis Key 配置
const REDIS_KEY_RISK_PARAMS: &str = "pre_trade_risk_params";

/// 后台刷新间隔（固定 60 秒）
const REFRESH_INTERVAL_SECS: u64 = 60;
const EXCHANGE_WARNING_MODE_UPPER_UNIMMR: f64 = 1.5;
const DEFAULT_UNIMMR_TRIGGER_LINE: f64 = 2.0;
const DEFAULT_UNIMMR_RECOVER_LINE: f64 = 2.2;
const DEFAULT_MIN_NON_TRADING_POSITION_USDT: f64 = 25.0;
const DEFAULT_ARB_ORDER_AMOUNT_U: f64 = 100.0;

type MaxPosUOverrideTable = FastHashMap<String, f64>;
type MaxPosUOverrides = FastHashMap<TradingVenue, MaxPosUOverrideTable>;
type AmountUOverrideTable = FastHashMap<String, f64>;

fn is_mm_pre_trade_mode(open_venue: TradingVenue, hedge_venue: TradingVenue) -> bool {
    open_venue == hedge_venue
}

/// 从 Redis 加载的 Pre-Trade 风控参数（内部数据结构）
#[derive(Debug, Clone)]
struct PreTradeParamsData {
    max_pos_u: f64,
    max_pos_u_overrides: MaxPosUOverrides,
    arb_order_amount_u: f64,
    arb_amount_u_overrides: AmountUOverrideTable,
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    max_leverage: f64,
    min_non_trading_position_usdt: f64,
    exec_max_position_imbalance_ratio: f64,
    unimmr_trigger_line: f64,
    unimmr_recover_line: f64,
    max_pending_limit_orders: i32,
    max_pending_limit_buy_orders: i32,
    max_pending_limit_sell_orders: i32,
    open_order_rate_limit_per_min: i32,
    open_order_rate_limit_10s: i32,
    hedge_order_rate_limit_per_min: i32,
    hedge_order_rate_limit_10s: i32,
    arb_max_pending_limit_buy_orders: i32,
    arb_max_pending_limit_sell_orders: i32,
    arb_close_max_pending_limit_buy_orders: i32,
    arb_close_max_pending_limit_sell_orders: i32,
    arb_open_order_rate_limit_per_min: i32,
    arb_open_order_rate_limit_10s: i32,
    arb_hedge_order_rate_limit_per_min: i32,
    arb_hedge_order_rate_limit_10s: i32,
    exec_order_rate_limit_per_min: i32,
    exec_order_rate_limit_10s: i32,
}

impl Default for PreTradeParamsData {
    fn default() -> Self {
        Self {
            max_pos_u: 1000.0,
            max_pos_u_overrides: fast_hash_map(),
            arb_order_amount_u: DEFAULT_ARB_ORDER_AMOUNT_U,
            arb_amount_u_overrides: fast_hash_map(),
            max_symbol_exposure_ratio: 0.8,
            max_total_exposure_ratio: 1.0,
            max_leverage: 3.0,
            min_non_trading_position_usdt: DEFAULT_MIN_NON_TRADING_POSITION_USDT,
            exec_max_position_imbalance_ratio: 0.0,
            unimmr_trigger_line: DEFAULT_UNIMMR_TRIGGER_LINE,
            unimmr_recover_line: DEFAULT_UNIMMR_RECOVER_LINE,
            max_pending_limit_orders: 3,
            max_pending_limit_buy_orders: 0,
            max_pending_limit_sell_orders: 0,
            open_order_rate_limit_per_min: 0,
            open_order_rate_limit_10s: 0,
            hedge_order_rate_limit_per_min: 0,
            hedge_order_rate_limit_10s: 0,
            arb_max_pending_limit_buy_orders: 0,
            arb_max_pending_limit_sell_orders: 0,
            arb_close_max_pending_limit_buy_orders: 3,
            arb_close_max_pending_limit_sell_orders: 3,
            arb_open_order_rate_limit_per_min: 0,
            arb_open_order_rate_limit_10s: 0,
            arb_hedge_order_rate_limit_per_min: 0,
            arb_hedge_order_rate_limit_10s: 0,
            exec_order_rate_limit_per_min: 0,
            exec_order_rate_limit_10s: 0,
        }
    }
}

/// Pre-Trade 参数加载器单例（thread-local 单线程版本）
pub struct PreTradeParamsLoader;

thread_local! {
    static PARAMS_DATA: RefCell<PreTradeParamsData> = RefCell::new(PreTradeParamsData::default());
}

/// 应急写回 Redis 所需的运行时上下文（settings + 完整 hash key）。
/// 在 `load_from_redis` 首次成功加载后由 OnceLock 兜底设置一次，供 `set_max_leverage_async` 使用。
#[derive(Debug, Clone)]
struct RedisWritebackContext {
    settings: RedisSettings,
    risk_params_full_key: String,
}

static REDIS_WRITEBACK: OnceLock<RedisWritebackContext> = OnceLock::new();

fn risk_params_full_key(redis: &RedisSettings) -> String {
    match redis.prefix.as_deref() {
        Some(prefix) if !prefix.is_empty() => format!("{prefix}{REDIS_KEY_RISK_PARAMS}"),
        _ => REDIS_KEY_RISK_PARAMS.to_string(),
    }
}

fn raw_redis_settings_for_full_key(redis: &RedisSettings) -> RedisSettings {
    let mut raw_settings = redis.clone();
    raw_settings.prefix = None;
    raw_settings
}

fn mm_max_pos_u_override_key(env_name: Option<&str>, open_venue: TradingVenue) -> Option<String> {
    let env_name = env_name.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!(
        "{env_name}:{}:mm:max_pos_u",
        open_venue.data_pub_slug()
    ))
}

fn exec_max_pos_u_override_key(env_name: Option<&str>, venue: TradingVenue) -> Option<String> {
    let env_name = env_name.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!(
        "{env_name}:{}:exec:max_pos_u",
        venue.data_pub_slug()
    ))
}

/// Arb（intra/cross/fr）共用的 per-symbol max_pos_u 覆盖键。
/// 由 sync_*_max_pos_u.py 写入 Redis STRING(JSON {symbol: max_pos_u})。
fn arb_max_pos_u_override_key(
    env_name: Option<&str>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<String> {
    let env_name = env_name.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!(
        "{env_name}:{}:{}:max_pos_u_overrides",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    ))
}

fn arb_strategy_params_key(open_venue: TradingVenue, hedge_venue: TradingVenue) -> String {
    format!(
        "fr_strategy_params_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn arb_amount_u_override_key(
    env_name: Option<&str>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<String> {
    let env_name = env_name.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!(
        "{env_name}:{}:{}:amount_u_overrides",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    ))
}

fn is_internal_symbol_key(symbol: &str) -> bool {
    !symbol.is_empty()
        && !symbol.ends_with("SWAP")
        && symbol
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

fn max_pos_u_symbol_key(symbol: &str) -> Cow<'_, str> {
    if is_internal_symbol_key(symbol) {
        Cow::Borrowed(symbol)
    } else {
        Cow::Owned(normalize_symbol_for_internal(symbol))
    }
}

fn max_pos_u_override_count(overrides: &MaxPosUOverrides) -> usize {
    overrides.values().map(MaxPosUOverrideTable::len).sum()
}

fn merge_max_pos_u_overrides(dst: &mut MaxPosUOverrides, src: MaxPosUOverrides) {
    for (venue, table) in src {
        dst.entry(venue).or_insert_with(fast_hash_map).extend(table);
    }
}

fn parse_amount_u_overrides(raw: &str, redis_key: &str) -> AmountUOverrideTable {
    let parsed: HashMap<String, f64> = serde_json::from_str(raw).unwrap_or_else(|err| {
        panic!(
            "Redis string '{}' 不是合法 JSON(symbol->amount_u): {} ({})",
            redis_key, raw, err
        )
    });

    let mut overrides = fast_hash_map_with_capacity(parsed.len());
    for (symbol, amount_u) in parsed {
        let symbol_trimmed = symbol.trim();
        if symbol_trimmed.is_empty() {
            panic!("Redis string '{}' 包含空 symbol", redis_key);
        }
        if !(amount_u.is_finite() && amount_u > 0.0) {
            panic!(
                "Redis string '{}' symbol={} amount_u 非法: {}",
                redis_key, symbol_trimmed, amount_u
            );
        }
        overrides.insert(normalize_symbol_for_internal(symbol_trimmed), amount_u);
    }
    overrides
}

fn parse_arb_order_amount_u(hash_map: &HashMap<String, String>, redis_key: &str) -> Option<f64> {
    let raw = hash_map.get("order_amount")?;
    match raw.parse::<f64>() {
        Ok(value) if value.is_finite() && value > 0.0 => Some(value),
        _ => {
            warn!(
                "strategy params key='{}' order_amount={} 无效，继续使用上一轮或默认 amount_u",
                redis_key, raw
            );
            None
        }
    }
}

fn parse_max_pos_u_overrides(
    raw: &str,
    open_venue: TradingVenue,
    redis_key: &str,
) -> MaxPosUOverrides {
    let parsed: HashMap<String, f64> = serde_json::from_str(raw).unwrap_or_else(|err| {
        panic!(
            "Redis string '{}' 不是合法 JSON(symbol->max_pos_u): {} ({})",
            redis_key, raw, err
        )
    });

    let mut normalized = fast_hash_map();
    let mut venue_table = fast_hash_map_with_capacity(parsed.len());
    for (symbol, max_pos_u) in parsed {
        let symbol_trimmed = symbol.trim();
        if symbol_trimmed.is_empty() {
            panic!("Redis string '{}' 包含空 symbol", redis_key);
        }
        if !(max_pos_u.is_finite() && max_pos_u > 0.0) {
            panic!(
                "Redis string '{}' symbol={} max_pos_u 非法: {}",
                redis_key, symbol_trimmed, max_pos_u
            );
        }
        let symbol_key = normalize_symbol_for_internal(symbol_trimmed);
        venue_table.insert(symbol_key, max_pos_u);
    }
    normalized.insert(open_venue, venue_table);
    normalized
}

fn resolve_max_pos_u_for_symbol(
    default_max_pos_u: f64,
    overrides: &MaxPosUOverrides,
    open_venue: TradingVenue,
    symbol: &str,
) -> f64 {
    let Some(venue_overrides) = overrides.get(&open_venue) else {
        return default_max_pos_u;
    };
    let symbol_key = max_pos_u_symbol_key(symbol);
    venue_overrides
        .get(symbol_key.as_ref())
        .copied()
        .unwrap_or(default_max_pos_u)
}

fn resolve_amount_u_for_symbol(
    default_amount_u: f64,
    overrides: &AmountUOverrideTable,
    symbol: &str,
) -> f64 {
    let symbol_key = max_pos_u_symbol_key(symbol);
    overrides
        .get(symbol_key.as_ref())
        .copied()
        .unwrap_or(default_amount_u)
}

fn normalize_unimmr_control_lines(trigger_line: f64, recover_line: f64) -> Option<(f64, f64)> {
    if trigger_line.is_finite()
        && recover_line.is_finite()
        && trigger_line > EXCHANGE_WARNING_MODE_UPPER_UNIMMR
        && recover_line > trigger_line
    {
        Some((trigger_line, recover_line))
    } else {
        None
    }
}

impl PreTradeParamsLoader {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        PreTradeParamsLoader
    }

    /// 从 Redis 加载参数并更新单例
    pub async fn load_from_redis(
        &self,
        redis: &RedisSettings,
        env_name: Option<&str>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<()> {
        let risk_key = risk_params_full_key(redis);
        let raw_settings = raw_redis_settings_for_full_key(redis);
        let _ = REDIS_WRITEBACK.set(RedisWritebackContext {
            settings: raw_settings.clone(),
            risk_params_full_key: risk_key.clone(),
        });
        let mut client = RedisClient::connect(raw_settings).await?;
        let hash_map = client.hgetall_map(&risk_key).await?;
        if hash_map.is_empty() {
            anyhow::bail!(
                "risk params hash not found or empty: key='{}' (prefix={:?})",
                risk_key,
                redis.prefix.as_deref()
            );
        }

        debug!(
            "risk params loaded from redis key='{}' (prefix={:?})",
            risk_key,
            redis.prefix.as_deref()
        );

        let mut max_pos_u_overrides: MaxPosUOverrides = fast_hash_map();
        if let Some(override_key) = mm_max_pos_u_override_key(env_name, open_venue) {
            if let Some(raw) = client.get_string(&override_key).await? {
                let parsed = parse_max_pos_u_overrides(&raw, open_venue, &override_key);
                debug!(
                    "max_pos_u overrides loaded key='{}' symbols={}",
                    override_key,
                    max_pos_u_override_count(&parsed)
                );
                merge_max_pos_u_overrides(&mut max_pos_u_overrides, parsed);
            }
        }
        for venue in [open_venue, hedge_venue] {
            if let Some(override_key) = exec_max_pos_u_override_key(env_name, venue) {
                if let Some(raw) = client.get_string(&override_key).await? {
                    let parsed = parse_max_pos_u_overrides(&raw, venue, &override_key);
                    debug!(
                        "exec max_pos_u overrides loaded key='{}' symbols={}",
                        override_key,
                        max_pos_u_override_count(&parsed)
                    );
                    merge_max_pos_u_overrides(&mut max_pos_u_overrides, parsed);
                }
            }
        }
        if let Some(override_key) = arb_max_pos_u_override_key(env_name, open_venue, hedge_venue) {
            if let Some(raw) = client.get_string(&override_key).await? {
                let parsed = parse_max_pos_u_overrides(&raw, open_venue, &override_key);
                debug!(
                    "arb max_pos_u overrides loaded key='{}' symbols={}",
                    override_key,
                    max_pos_u_override_count(&parsed)
                );
                merge_max_pos_u_overrides(&mut max_pos_u_overrides, parsed);
            }
        }

        let strategy_key = arb_strategy_params_key(open_venue, hedge_venue);
        let strategy_params = client.hgetall_map(&strategy_key).await?;
        let arb_order_amount_u = parse_arb_order_amount_u(&strategy_params, &strategy_key);
        let mut amount_u_overrides = fast_hash_map();
        if let Some(override_key) = arb_amount_u_override_key(env_name, open_venue, hedge_venue) {
            if let Some(raw) = client.get_string(&override_key).await? {
                amount_u_overrides = parse_amount_u_overrides(&raw, &override_key);
                debug!(
                    "arb amount_u overrides loaded key='{}' symbols={}",
                    override_key,
                    amount_u_overrides.len()
                );
            }
        }

        self.apply_loaded_params(
            hash_map,
            max_pos_u_overrides,
            arb_order_amount_u,
            amount_u_overrides,
            is_mm_pre_trade_mode(open_venue, hedge_venue),
        );
        Ok(())
    }

    pub fn load_from_redis_blocking(
        &self,
        redis: &RedisSettings,
        env_name: Option<&str>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<()> {
        let risk_key = risk_params_full_key(redis);
        let raw_settings = raw_redis_settings_for_full_key(redis);
        let _ = REDIS_WRITEBACK.set(RedisWritebackContext {
            settings: raw_settings.clone(),
            risk_params_full_key: risk_key.clone(),
        });
        let mut client = BlockingRedisClient::connect(raw_settings)?;
        let hash_map = client.hgetall_map(&risk_key)?;
        if hash_map.is_empty() {
            anyhow::bail!(
                "risk params hash not found or empty: key='{}' (prefix={:?})",
                risk_key,
                redis.prefix.as_deref()
            );
        }

        debug!(
            "risk params loaded from redis key='{}' (prefix={:?})",
            risk_key,
            redis.prefix.as_deref()
        );

        let mut max_pos_u_overrides: MaxPosUOverrides = fast_hash_map();
        if let Some(override_key) = mm_max_pos_u_override_key(env_name, open_venue) {
            if let Some(raw) = client.get_string(&override_key)? {
                let parsed = parse_max_pos_u_overrides(&raw, open_venue, &override_key);
                debug!(
                    "max_pos_u overrides loaded key='{}' symbols={}",
                    override_key,
                    max_pos_u_override_count(&parsed)
                );
                merge_max_pos_u_overrides(&mut max_pos_u_overrides, parsed);
            }
        }
        for venue in [open_venue, hedge_venue] {
            if let Some(override_key) = exec_max_pos_u_override_key(env_name, venue) {
                if let Some(raw) = client.get_string(&override_key)? {
                    let parsed = parse_max_pos_u_overrides(&raw, venue, &override_key);
                    debug!(
                        "exec max_pos_u overrides loaded key='{}' symbols={}",
                        override_key,
                        max_pos_u_override_count(&parsed)
                    );
                    merge_max_pos_u_overrides(&mut max_pos_u_overrides, parsed);
                }
            }
        }
        if let Some(override_key) = arb_max_pos_u_override_key(env_name, open_venue, hedge_venue) {
            if let Some(raw) = client.get_string(&override_key)? {
                let parsed = parse_max_pos_u_overrides(&raw, open_venue, &override_key);
                debug!(
                    "arb max_pos_u overrides loaded key='{}' symbols={}",
                    override_key,
                    max_pos_u_override_count(&parsed)
                );
                merge_max_pos_u_overrides(&mut max_pos_u_overrides, parsed);
            }
        }

        let strategy_key = arb_strategy_params_key(open_venue, hedge_venue);
        let strategy_params = client.hgetall_map(&strategy_key)?;
        let arb_order_amount_u = parse_arb_order_amount_u(&strategy_params, &strategy_key);
        let mut amount_u_overrides = fast_hash_map();
        if let Some(override_key) = arb_amount_u_override_key(env_name, open_venue, hedge_venue) {
            if let Some(raw) = client.get_string(&override_key)? {
                amount_u_overrides = parse_amount_u_overrides(&raw, &override_key);
                debug!(
                    "arb amount_u overrides loaded key='{}' symbols={}",
                    override_key,
                    amount_u_overrides.len()
                );
            }
        }

        self.apply_loaded_params(
            hash_map,
            max_pos_u_overrides,
            arb_order_amount_u,
            amount_u_overrides,
            is_mm_pre_trade_mode(open_venue, hedge_venue),
        );
        Ok(())
    }

    pub(crate) fn apply_loaded_params(
        &self,
        hash_map: HashMap<String, String>,
        max_pos_u_overrides: MaxPosUOverrides,
        arb_order_amount_u: Option<f64>,
        arb_amount_u_overrides: AmountUOverrideTable,
        mm_pre_trade_mode: bool,
    ) {
        let parse_f64 =
            |k: &str| -> Option<f64> { hash_map.get(k).and_then(|v| v.parse::<f64>().ok()) };
        let parse_i64 =
            |k: &str| -> Option<i64> { hash_map.get(k).and_then(|v| v.parse::<i64>().ok()) };

        PARAMS_DATA.with(|data| {
            let mut data = data.borrow_mut();

            if let Some(v) = parse_f64("max_pos_u") {
                data.max_pos_u = v;
            }
            data.max_pos_u_overrides = max_pos_u_overrides.clone();
            if let Some(v) = arb_order_amount_u {
                data.arb_order_amount_u = v;
            }
            data.arb_amount_u_overrides = arb_amount_u_overrides.clone();

            if let Some(v) = parse_f64("max_symbol_exposure_ratio") {
                data.max_symbol_exposure_ratio = v;
            }

            if let Some(v) = parse_f64("max_total_exposure_ratio") {
                data.max_total_exposure_ratio = v;
            }

            if let Some(v) = parse_f64("max_leverage") {
                if v > 0.0 {
                    data.max_leverage = v;
                } else {
                    warn!("max_leverage={} 无效，需大于 0，忽略更新", v);
                }
            }

            if let Some(v) = parse_f64("min_non_trading_position_usdt") {
                if v.is_finite() && v >= 0.0 {
                    data.min_non_trading_position_usdt = v;
                } else {
                    warn!(
                        "min_non_trading_position_usdt={} 无效，需为非负有限值，忽略更新",
                        v
                    );
                }
            }

            if let Some(v) = parse_f64("exec_max_position_imbalance_ratio") {
                if v.is_finite() && (0.0..=1.0).contains(&v) {
                    data.exec_max_position_imbalance_ratio = v;
                } else {
                    warn!("exec_max_position_imbalance_ratio={} 无效，需在 [0, 1] 内，忽略更新", v);
                }
            }

            let raw_unimmr_trigger_line = parse_f64("unimmr_trigger_line")
                .unwrap_or(data.unimmr_trigger_line);
            let raw_unimmr_recover_line =
                parse_f64("unimmr_recover_line").unwrap_or(data.unimmr_recover_line);
            if let Some((trigger_line, recover_line)) =
                normalize_unimmr_control_lines(raw_unimmr_trigger_line, raw_unimmr_recover_line)
            {
                data.unimmr_trigger_line = trigger_line;
                data.unimmr_recover_line = recover_line;
            } else {
                warn!(
                    "unimmr control lines invalid trigger={} recover={}，要求 {:.2} < trigger < recover，回退默认 trigger={:.2} recover={:.2}",
                    raw_unimmr_trigger_line,
                    raw_unimmr_recover_line,
                    EXCHANGE_WARNING_MODE_UPPER_UNIMMR,
                    DEFAULT_UNIMMR_TRIGGER_LINE,
                    DEFAULT_UNIMMR_RECOVER_LINE
                );
                data.unimmr_trigger_line = DEFAULT_UNIMMR_TRIGGER_LINE;
                data.unimmr_recover_line = DEFAULT_UNIMMR_RECOVER_LINE;
            }

            if let Some(v) = parse_i64("max_pending_limit_orders") {
                data.max_pending_limit_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("max_pending_limit_buy_orders") {
                data.max_pending_limit_buy_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("max_pending_limit_sell_orders") {
                data.max_pending_limit_sell_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("open_order_rate_limit_per_min") {
                data.open_order_rate_limit_per_min = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("open_order_rate_limit_10s") {
                data.open_order_rate_limit_10s = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("hedge_order_rate_limit_per_min") {
                data.hedge_order_rate_limit_per_min = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("hedge_order_rate_limit_10s") {
                data.hedge_order_rate_limit_10s = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_max_pending_limit_buy_orders") {
                data.arb_max_pending_limit_buy_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_max_pending_limit_sell_orders") {
                data.arb_max_pending_limit_sell_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_close_max_pending_limit_buy_orders") {
                data.arb_close_max_pending_limit_buy_orders = v.max(0) as i32;
            } else if !mm_pre_trade_mode {
                panic!(
                    "pre_trade_risk_params missing or invalid required field: arb_close_max_pending_limit_buy_orders"
                );
            }

            if let Some(v) = parse_i64("arb_close_max_pending_limit_sell_orders") {
                data.arb_close_max_pending_limit_sell_orders = v.max(0) as i32;
            } else if !mm_pre_trade_mode {
                panic!(
                    "pre_trade_risk_params missing or invalid required field: arb_close_max_pending_limit_sell_orders"
                );
            }

            if let Some(v) = parse_i64("arb_open_order_rate_limit_per_min") {
                data.arb_open_order_rate_limit_per_min = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_open_order_rate_limit_10s") {
                data.arb_open_order_rate_limit_10s = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_hedge_order_rate_limit_per_min") {
                data.arb_hedge_order_rate_limit_per_min = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_hedge_order_rate_limit_10s") {
                data.arb_hedge_order_rate_limit_10s = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("exec_order_rate_limit_per_min") {
                data.exec_order_rate_limit_per_min = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("exec_order_rate_limit_10s") {
                data.exec_order_rate_limit_10s = v.max(0) as i32;
            }

            debug!(
                "风控参数已加载: max_pos_u={:.2} overrides={} arb_order_amount_u={:.2} amount_u_overrides={} sym_ratio={:.4} total_ratio={:.4} max_leverage={:.2} min_non_trading_position_usdt={:.2} exec_position_imbalance_ratio={:.4} unimmr_trigger={:.2} unimmr_recover={:.2} max_pending={} max_pending_buy={} max_pending_sell={} open_rate_1m={} open_rate_10s={} hedge_rate_1m={} hedge_rate_10s={} arb_max_pending_buy={} arb_max_pending_sell={} arb_close_max_pending_buy={} arb_close_max_pending_sell={} arb_open_rate_1m={} arb_open_rate_10s={} arb_hedge_rate_1m={} arb_hedge_rate_10s={} exec_rate_1m={} exec_rate_10s={}",
                data.max_pos_u,
                max_pos_u_override_count(&data.max_pos_u_overrides),
                data.arb_order_amount_u,
                data.arb_amount_u_overrides.len(),
                data.max_symbol_exposure_ratio,
                data.max_total_exposure_ratio,
                data.max_leverage,
                data.min_non_trading_position_usdt,
                data.exec_max_position_imbalance_ratio,
                data.unimmr_trigger_line,
                data.unimmr_recover_line,
                data.max_pending_limit_orders,
                data.max_pending_limit_buy_orders,
                data.max_pending_limit_sell_orders,
                data.open_order_rate_limit_per_min,
                data.open_order_rate_limit_10s,
                data.hedge_order_rate_limit_per_min,
                data.hedge_order_rate_limit_10s,
                data.arb_max_pending_limit_buy_orders,
                data.arb_max_pending_limit_sell_orders,
                data.arb_close_max_pending_limit_buy_orders,
                data.arb_close_max_pending_limit_sell_orders,
                data.arb_open_order_rate_limit_per_min,
                data.arb_open_order_rate_limit_10s,
                data.arb_hedge_order_rate_limit_per_min,
                data.arb_hedge_order_rate_limit_10s,
                data.exec_order_rate_limit_per_min,
                data.exec_order_rate_limit_10s
            );
        });
    }

    /// 启动后台刷新任务（固定 60 秒间隔）
    pub fn start_background_refresh(
        redis: RedisSettings,
        env_name: Option<String>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) {
        tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(REFRESH_INTERVAL_SECS));
            let loader = PreTradeParamsLoader::instance();

            loop {
                interval.tick().await;

                match loader
                    .load_from_redis(&redis, env_name.as_deref(), open_venue, hedge_venue)
                    .await
                {
                    Ok(_) => {
                        debug!("风控参数后台刷新成功");
                    }
                    Err(e) => {
                        warn!("风控参数后台刷新失败: {:#}", e);
                    }
                }
            }
        });

        info!("后台参数刷新任务已启动 (间隔: {}s)", REFRESH_INTERVAL_SECS);
    }

    /// 打印参数三线表
    pub fn print_params_table(&self) {
        let data = PARAMS_DATA.with(|d| d.borrow().clone());

        let separator = "=".repeat(60);
        let mid_separator = "-".repeat(60);

        println!("\n{}", separator);
        println!("{:<40} {:>18}", "Parameter", "Value");
        println!("{}", mid_separator);
        println!("{:<40} {:>18.2}", "max_pos_u", data.max_pos_u);
        println!(
            "{:<40} {:>18.2}",
            "arb_order_amount_u", data.arb_order_amount_u
        );
        println!(
            "{:<40} {:>18}",
            "arb_amount_u_overrides",
            data.arb_amount_u_overrides.len()
        );
        println!(
            "{:<40} {:>18.4}",
            "max_symbol_exposure_ratio", data.max_symbol_exposure_ratio
        );
        println!(
            "{:<40} {:>18.4}",
            "max_total_exposure_ratio", data.max_total_exposure_ratio
        );
        println!("{:<40} {:>18.2}", "max_leverage", data.max_leverage);
        println!(
            "{:<40} {:>18.2}",
            "min_non_trading_position_usdt", data.min_non_trading_position_usdt
        );
        println!(
            "{:<40} {:>18.4}",
            "exec_max_position_imbalance_ratio", data.exec_max_position_imbalance_ratio
        );
        println!(
            "{:<40} {:>18.2}",
            "unimmr_trigger_line", data.unimmr_trigger_line
        );
        println!(
            "{:<40} {:>18.2}",
            "unimmr_recover_line", data.unimmr_recover_line
        );
        println!(
            "{:<40} {:>18}",
            "max_pending_limit_orders", data.max_pending_limit_orders
        );
        println!(
            "{:<40} {:>18}",
            "max_pending_limit_buy_orders", data.max_pending_limit_buy_orders
        );
        println!(
            "{:<40} {:>18}",
            "max_pending_limit_sell_orders", data.max_pending_limit_sell_orders
        );
        println!(
            "{:<40} {:>18}",
            "open_order_rate_limit_per_min", data.open_order_rate_limit_per_min
        );
        println!(
            "{:<40} {:>18}",
            "open_order_rate_limit_10s", data.open_order_rate_limit_10s
        );
        println!(
            "{:<40} {:>18}",
            "hedge_order_rate_limit_per_min", data.hedge_order_rate_limit_per_min
        );
        println!(
            "{:<40} {:>18}",
            "hedge_order_rate_limit_10s", data.hedge_order_rate_limit_10s
        );
        println!(
            "{:<40} {:>18}",
            "arb_max_pending_limit_buy_orders", data.arb_max_pending_limit_buy_orders
        );
        println!(
            "{:<40} {:>18}",
            "arb_max_pending_limit_sell_orders", data.arb_max_pending_limit_sell_orders
        );
        println!(
            "{:<40} {:>18}",
            "arb_close_max_pending_limit_buy_orders", data.arb_close_max_pending_limit_buy_orders
        );
        println!(
            "{:<40} {:>18}",
            "arb_close_max_pending_limit_sell_orders", data.arb_close_max_pending_limit_sell_orders
        );
        println!(
            "{:<40} {:>18}",
            "arb_open_order_rate_limit_per_min", data.arb_open_order_rate_limit_per_min
        );
        println!(
            "{:<40} {:>18}",
            "arb_open_order_rate_limit_10s", data.arb_open_order_rate_limit_10s
        );
        println!(
            "{:<40} {:>18}",
            "arb_hedge_order_rate_limit_per_min", data.arb_hedge_order_rate_limit_per_min
        );
        println!(
            "{:<40} {:>18}",
            "arb_hedge_order_rate_limit_10s", data.arb_hedge_order_rate_limit_10s
        );
        println!(
            "{:<40} {:>18}",
            "exec_order_rate_limit_per_min", data.exec_order_rate_limit_per_min
        );
        println!(
            "{:<40} {:>18}",
            "exec_order_rate_limit_10s", data.exec_order_rate_limit_10s
        );
        println!("{}", separator);
    }

    /// 获取 max_pos_u
    pub fn max_pos_u(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().max_pos_u)
    }

    pub fn max_pos_u_for_symbol(&self, open_venue: TradingVenue, symbol: &str) -> f64 {
        PARAMS_DATA.with(|data| {
            let data = data.borrow();
            resolve_max_pos_u_for_symbol(
                data.max_pos_u,
                &data.max_pos_u_overrides,
                open_venue,
                symbol,
            )
        })
    }

    pub fn arb_order_amount_u(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().arb_order_amount_u)
    }

    pub fn arb_amount_u_for_symbol(&self, symbol: &str) -> f64 {
        PARAMS_DATA.with(|data| {
            let data = data.borrow();
            resolve_amount_u_for_symbol(
                data.arb_order_amount_u,
                &data.arb_amount_u_overrides,
                symbol,
            )
        })
    }

    /// 获取 max_symbol_exposure_ratio
    pub fn max_symbol_exposure_ratio(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().max_symbol_exposure_ratio)
    }

    /// 获取 max_total_exposure_ratio
    pub fn max_total_exposure_ratio(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().max_total_exposure_ratio)
    }

    /// 获取 max_leverage
    pub fn max_leverage(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().max_leverage)
    }

    pub fn exec_max_position_imbalance_ratio(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().exec_max_position_imbalance_ratio)
    }

    pub fn min_non_trading_position_usdt(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().min_non_trading_position_usdt)
    }

    /// 获取 UniMMR 算法平仓触发线
    pub fn unimmr_trigger_line(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().unimmr_trigger_line)
    }

    /// 获取 UniMMR 算法平仓恢复线
    pub fn unimmr_recover_line(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().unimmr_recover_line)
    }

    /// 应急下调 max_leverage：立即更新本进程 thread-local 缓存（不等 60s 热加载），
    /// 同时 spawn 一个 tokio 任务把新值写回 Redis 风控 hash。
    /// Redis 写失败仅 warn，不影响本进程运行（下次 60s 热加载会按 Redis 实际值刷新）。
    /// 必须在 tokio runtime 上下文里调用（pre_trade 主循环已是 LocalSet）。
    pub fn set_max_leverage_async(&self, new_value: f64) {
        if !new_value.is_finite() || new_value <= 0.0 {
            warn!(
                "set_max_leverage_async ignored non-positive value {}",
                new_value
            );
            return;
        }
        // 1. 立即生效到本进程
        PARAMS_DATA.with(|data| data.borrow_mut().max_leverage = new_value);

        // 2. 异步写回 Redis；OnceLock 未初始化（理论上 load_from_redis 之前不应被调）说明启动序失常
        let Some(ctx) = REDIS_WRITEBACK.get() else {
            warn!(
                "set_max_leverage_async: REDIS_WRITEBACK not initialized, skip redis writeback (new_value={:.1})",
                new_value
            );
            return;
        };
        let settings = ctx.settings.clone();
        let key = ctx.risk_params_full_key.clone();
        let value = format!("{:.1}", new_value);
        tokio::task::spawn_local(async move {
            match RedisClient::connect(settings).await {
                Ok(mut client) => {
                    if let Err(e) = client
                        .hset_multiple_str(&key, &[("max_leverage".to_string(), value.clone())])
                        .await
                    {
                        warn!(
                            "set_max_leverage_async redis HSET failed key={} max_leverage={} err={:#}",
                            key, value, e
                        );
                    } else {
                        info!(
                            "set_max_leverage_async redis HSET ok key={} max_leverage={}",
                            key, value
                        );
                    }
                }
                Err(e) => warn!("set_max_leverage_async redis connect failed: {:#}", e),
            }
        });
    }

    /// 获取 max_pending_limit_orders
    pub fn max_pending_limit_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().max_pending_limit_orders)
    }

    /// 获取 max_pending_limit_buy_orders
    pub fn max_pending_limit_buy_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().max_pending_limit_buy_orders)
    }

    /// 获取 max_pending_limit_sell_orders
    pub fn max_pending_limit_sell_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().max_pending_limit_sell_orders)
    }

    /// 获取 open_order_rate_limit_per_min
    pub fn open_order_rate_limit_per_min(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().open_order_rate_limit_per_min)
    }

    /// 获取 open_order_rate_limit_10s
    pub fn open_order_rate_limit_10s(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().open_order_rate_limit_10s)
    }

    /// 获取 hedge_order_rate_limit_per_min
    pub fn hedge_order_rate_limit_per_min(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().hedge_order_rate_limit_per_min)
    }

    /// 获取 hedge_order_rate_limit_10s
    pub fn hedge_order_rate_limit_10s(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().hedge_order_rate_limit_10s)
    }

    /// 获取 arb_max_pending_limit_buy_orders
    pub fn arb_max_pending_limit_buy_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_max_pending_limit_buy_orders)
    }

    /// 获取 arb_max_pending_limit_sell_orders
    pub fn arb_max_pending_limit_sell_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_max_pending_limit_sell_orders)
    }

    /// 获取 arb_close_max_pending_limit_buy_orders
    pub fn arb_close_max_pending_limit_buy_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_close_max_pending_limit_buy_orders)
    }

    /// 获取 arb_close_max_pending_limit_sell_orders
    pub fn arb_close_max_pending_limit_sell_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_close_max_pending_limit_sell_orders)
    }

    /// 获取 arb_open_order_rate_limit_per_min
    pub fn arb_open_order_rate_limit_per_min(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_open_order_rate_limit_per_min)
    }

    /// 获取 arb_open_order_rate_limit_10s
    pub fn arb_open_order_rate_limit_10s(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_open_order_rate_limit_10s)
    }

    /// 获取 arb_hedge_order_rate_limit_per_min
    pub fn arb_hedge_order_rate_limit_per_min(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_hedge_order_rate_limit_per_min)
    }

    /// 获取 arb_hedge_order_rate_limit_10s
    pub fn arb_hedge_order_rate_limit_10s(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_hedge_order_rate_limit_10s)
    }

    pub fn exec_order_rate_limit_per_min(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().exec_order_rate_limit_per_min)
    }

    pub fn exec_order_rate_limit_10s(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().exec_order_rate_limit_10s)
    }

    /// 获取所有参数的快照（用于比较是否变化）
    pub fn snapshot(&self) -> PreTradeParamsSnapshot {
        PARAMS_DATA.with(|data| {
            let data = data.borrow();
            PreTradeParamsSnapshot {
                max_pos_u: data.max_pos_u,
                max_symbol_exposure_ratio: data.max_symbol_exposure_ratio,
                max_total_exposure_ratio: data.max_total_exposure_ratio,
                max_leverage: data.max_leverage,
                min_non_trading_position_usdt: data.min_non_trading_position_usdt,
                exec_max_position_imbalance_ratio: data.exec_max_position_imbalance_ratio,
                unimmr_trigger_line: data.unimmr_trigger_line,
                unimmr_recover_line: data.unimmr_recover_line,
                max_pending_limit_orders: data.max_pending_limit_orders,
                max_pending_limit_buy_orders: data.max_pending_limit_buy_orders,
                max_pending_limit_sell_orders: data.max_pending_limit_sell_orders,
                open_order_rate_limit_per_min: data.open_order_rate_limit_per_min,
                open_order_rate_limit_10s: data.open_order_rate_limit_10s,
                hedge_order_rate_limit_per_min: data.hedge_order_rate_limit_per_min,
                hedge_order_rate_limit_10s: data.hedge_order_rate_limit_10s,
                arb_max_pending_limit_buy_orders: data.arb_max_pending_limit_buy_orders,
                arb_max_pending_limit_sell_orders: data.arb_max_pending_limit_sell_orders,
                arb_close_max_pending_limit_buy_orders: data.arb_close_max_pending_limit_buy_orders,
                arb_close_max_pending_limit_sell_orders: data
                    .arb_close_max_pending_limit_sell_orders,
                arb_open_order_rate_limit_per_min: data.arb_open_order_rate_limit_per_min,
                arb_open_order_rate_limit_10s: data.arb_open_order_rate_limit_10s,
                arb_hedge_order_rate_limit_per_min: data.arb_hedge_order_rate_limit_per_min,
                arb_hedge_order_rate_limit_10s: data.arb_hedge_order_rate_limit_10s,
                exec_order_rate_limit_per_min: data.exec_order_rate_limit_per_min,
                exec_order_rate_limit_10s: data.exec_order_rate_limit_10s,
            }
        })
    }
}

/// 参数快照，用于比较参数是否变化
#[derive(Debug, Clone, PartialEq)]
pub struct PreTradeParamsSnapshot {
    pub max_pos_u: f64,
    pub max_symbol_exposure_ratio: f64,
    pub max_total_exposure_ratio: f64,
    pub max_leverage: f64,
    pub min_non_trading_position_usdt: f64,
    pub exec_max_position_imbalance_ratio: f64,
    pub unimmr_trigger_line: f64,
    pub unimmr_recover_line: f64,
    pub max_pending_limit_orders: i32,
    pub max_pending_limit_buy_orders: i32,
    pub max_pending_limit_sell_orders: i32,
    pub open_order_rate_limit_per_min: i32,
    pub open_order_rate_limit_10s: i32,
    pub hedge_order_rate_limit_per_min: i32,
    pub hedge_order_rate_limit_10s: i32,
    pub arb_max_pending_limit_buy_orders: i32,
    pub arb_max_pending_limit_sell_orders: i32,
    pub arb_close_max_pending_limit_buy_orders: i32,
    pub arb_close_max_pending_limit_sell_orders: i32,
    pub arb_open_order_rate_limit_per_min: i32,
    pub arb_open_order_rate_limit_10s: i32,
    pub arb_hedge_order_rate_limit_per_min: i32,
    pub arb_hedge_order_rate_limit_10s: i32,
    pub exec_order_rate_limit_per_min: i32,
    pub exec_order_rate_limit_10s: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_params() {
        let loader = PreTradeParamsLoader::instance();
        assert_eq!(loader.max_pos_u(), 1000.0);
        assert_eq!(loader.arb_order_amount_u(), DEFAULT_ARB_ORDER_AMOUNT_U);
        assert_eq!(
            loader.arb_amount_u_for_symbol("BTCUSDT"),
            DEFAULT_ARB_ORDER_AMOUNT_U
        );
        assert_eq!(loader.max_symbol_exposure_ratio(), 0.8);
        assert_eq!(loader.max_total_exposure_ratio(), 1.0);
        assert_eq!(loader.max_leverage(), 3.0);
        assert_eq!(
            loader.min_non_trading_position_usdt(),
            DEFAULT_MIN_NON_TRADING_POSITION_USDT
        );
        assert_eq!(loader.exec_max_position_imbalance_ratio(), 0.0);
        assert_eq!(loader.unimmr_trigger_line(), DEFAULT_UNIMMR_TRIGGER_LINE);
        assert_eq!(loader.unimmr_recover_line(), DEFAULT_UNIMMR_RECOVER_LINE);
        assert_eq!(loader.max_pending_limit_orders(), 3);
        assert_eq!(loader.max_pending_limit_buy_orders(), 0);
        assert_eq!(loader.max_pending_limit_sell_orders(), 0);
        assert_eq!(loader.open_order_rate_limit_per_min(), 0);
        assert_eq!(loader.open_order_rate_limit_10s(), 0);
        assert_eq!(loader.hedge_order_rate_limit_per_min(), 0);
        assert_eq!(loader.hedge_order_rate_limit_10s(), 0);
        assert_eq!(loader.arb_max_pending_limit_buy_orders(), 0);
        assert_eq!(loader.arb_max_pending_limit_sell_orders(), 0);
        assert_eq!(loader.arb_close_max_pending_limit_buy_orders(), 3);
        assert_eq!(loader.arb_close_max_pending_limit_sell_orders(), 3);
        assert_eq!(loader.arb_open_order_rate_limit_per_min(), 0);
        assert_eq!(loader.arb_open_order_rate_limit_10s(), 0);
        assert_eq!(loader.arb_hedge_order_rate_limit_per_min(), 0);
        assert_eq!(loader.arb_hedge_order_rate_limit_10s(), 0);
        assert_eq!(loader.exec_order_rate_limit_per_min(), 0);
        assert_eq!(loader.exec_order_rate_limit_10s(), 0);
    }

    #[test]
    fn test_snapshot() {
        let loader = PreTradeParamsLoader::instance();
        let snapshot = loader.snapshot();
        assert_eq!(snapshot.max_pos_u, 1000.0);
        assert_eq!(snapshot.max_leverage, 3.0);
        assert_eq!(
            snapshot.min_non_trading_position_usdt,
            DEFAULT_MIN_NON_TRADING_POSITION_USDT
        );
        assert_eq!(snapshot.unimmr_trigger_line, DEFAULT_UNIMMR_TRIGGER_LINE);
        assert_eq!(snapshot.unimmr_recover_line, DEFAULT_UNIMMR_RECOVER_LINE);
        assert_eq!(snapshot.max_pending_limit_buy_orders, 0);
        assert_eq!(snapshot.max_pending_limit_sell_orders, 0);
        assert_eq!(snapshot.open_order_rate_limit_per_min, 0);
        assert_eq!(snapshot.open_order_rate_limit_10s, 0);
        assert_eq!(snapshot.hedge_order_rate_limit_per_min, 0);
        assert_eq!(snapshot.hedge_order_rate_limit_10s, 0);
        assert_eq!(snapshot.arb_max_pending_limit_buy_orders, 0);
        assert_eq!(snapshot.arb_max_pending_limit_sell_orders, 0);
        assert_eq!(snapshot.arb_close_max_pending_limit_buy_orders, 3);
        assert_eq!(snapshot.arb_close_max_pending_limit_sell_orders, 3);
        assert_eq!(snapshot.arb_open_order_rate_limit_per_min, 0);
        assert_eq!(snapshot.arb_open_order_rate_limit_10s, 0);
        assert_eq!(snapshot.arb_hedge_order_rate_limit_per_min, 0);
        assert_eq!(snapshot.arb_hedge_order_rate_limit_10s, 0);
        assert_eq!(snapshot.exec_order_rate_limit_per_min, 0);
        assert_eq!(snapshot.exec_order_rate_limit_10s, 0);
    }

    #[test]
    fn test_resolve_amount_u_for_symbol_uses_override() {
        let overrides = parse_amount_u_overrides(r#"{"BTC_USDT":250, "ETHUSDT":125}"#, "amount");
        assert_eq!(
            resolve_amount_u_for_symbol(100.0, &overrides, "BTCUSDT"),
            250.0
        );
        assert_eq!(
            resolve_amount_u_for_symbol(100.0, &overrides, "eth-usdt"),
            125.0
        );
        assert_eq!(
            resolve_amount_u_for_symbol(100.0, &overrides, "SOLUSDT"),
            100.0
        );
    }

    #[test]
    fn test_resolve_max_pos_u_for_symbol_uses_override() {
        let mut overrides = fast_hash_map();
        let mut binance = fast_hash_map();
        binance.insert("BTCUSDT".to_string(), 2500.0);
        overrides.insert(TradingVenue::BinanceFutures, binance);
        let val = resolve_max_pos_u_for_symbol(
            1000.0,
            &overrides,
            TradingVenue::BinanceFutures,
            "btc-usdt",
        );
        assert_eq!(val, 2500.0);
    }

    #[test]
    fn test_resolve_max_pos_u_for_symbol_falls_back_to_default() {
        let overrides = fast_hash_map();
        let val = resolve_max_pos_u_for_symbol(
            1000.0,
            &overrides,
            TradingVenue::BinanceFutures,
            "btc-usdt",
        );
        assert_eq!(val, 1000.0);
    }

    #[test]
    fn test_resolve_max_pos_u_for_symbol_is_venue_scoped() {
        let mut overrides = fast_hash_map();
        let mut binance = fast_hash_map();
        binance.insert("BTCUSDT".to_string(), 2500.0);
        overrides.insert(TradingVenue::BinanceFutures, binance);
        let mut okex = fast_hash_map();
        okex.insert("BTCUSDT".to_string(), 1500.0);
        overrides.insert(TradingVenue::OkexFutures, okex);

        let binance_val = resolve_max_pos_u_for_symbol(
            1000.0,
            &overrides,
            TradingVenue::BinanceFutures,
            "btc-usdt",
        );
        let okex_val =
            resolve_max_pos_u_for_symbol(1000.0, &overrides, TradingVenue::OkexFutures, "BTCUSDT");

        assert_eq!(binance_val, 2500.0);
        assert_eq!(okex_val, 1500.0);
    }

    #[test]
    fn mm_mode_allows_missing_arb_close_limits() {
        let loader = PreTradeParamsLoader::instance();
        let mut params = HashMap::new();
        params.insert("max_pos_u".to_string(), "1234".to_string());

        loader.apply_loaded_params(params, fast_hash_map(), None, fast_hash_map(), true);

        assert_eq!(loader.max_pos_u(), 1234.0);
        assert_eq!(loader.arb_close_max_pending_limit_buy_orders(), 3);
        assert_eq!(loader.arb_close_max_pending_limit_sell_orders(), 3);
    }

    #[test]
    #[should_panic(
        expected = "pre_trade_risk_params missing or invalid required field: arb_close_max_pending_limit_buy_orders"
    )]
    fn non_mm_mode_requires_arb_close_limits() {
        let loader = PreTradeParamsLoader::instance();
        loader.apply_loaded_params(
            HashMap::new(),
            fast_hash_map(),
            None,
            fast_hash_map(),
            false,
        );
    }

    #[test]
    fn test_merge_max_pos_u_overrides_preserves_same_venue_symbols() {
        let mut dst =
            parse_max_pos_u_overrides(r#"{"BTCUSDT":2500}"#, TradingVenue::BinanceFutures, "first");
        let src = parse_max_pos_u_overrides(
            r#"{"ETHUSDT":1200}"#,
            TradingVenue::BinanceFutures,
            "second",
        );

        merge_max_pos_u_overrides(&mut dst, src);

        let binance = dst.get(&TradingVenue::BinanceFutures).expect("venue");
        assert_eq!(binance.get("BTCUSDT"), Some(&2500.0));
        assert_eq!(binance.get("ETHUSDT"), Some(&1200.0));
    }

    #[test]
    fn test_parse_max_pos_u_overrides_normalizes_symbols() {
        let overrides = parse_max_pos_u_overrides(
            r#"{"btc-usdt":2500,"ETH_USDT":1200}"#,
            TradingVenue::BinanceFutures,
            "binance_mm_alpha:binance-futures:mm:max_pos_u",
        );
        let binance = overrides
            .get(&TradingVenue::BinanceFutures)
            .expect("venue overrides");
        assert_eq!(binance.get("BTCUSDT"), Some(&2500.0));
        assert_eq!(binance.get("ETHUSDT"), Some(&1200.0));
    }

    #[test]
    fn test_exec_max_pos_u_override_key_format() {
        let key = exec_max_pos_u_override_key(Some("binance-exec01"), TradingVenue::BinanceFutures);
        assert_eq!(
            key.as_deref(),
            Some("binance-exec01:binance-futures:exec:max_pos_u")
        );
    }

    #[test]
    fn test_arb_max_pos_u_override_key_format() {
        let key = arb_max_pos_u_override_key(
            Some("okex-intra-arb01"),
            TradingVenue::OkexMargin,
            TradingVenue::OkexFutures,
        );
        assert_eq!(
            key.as_deref(),
            Some("okex-intra-arb01:okex-margin:okex-futures:max_pos_u_overrides")
        );
    }

    #[test]
    fn test_normalize_unimmr_control_lines() {
        assert_eq!(normalize_unimmr_control_lines(2.0, 2.2), Some((2.0, 2.2)));
        assert_eq!(normalize_unimmr_control_lines(1.5, 2.2), None);
        assert_eq!(normalize_unimmr_control_lines(2.2, 2.0), None);
    }
}
