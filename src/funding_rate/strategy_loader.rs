//! 策略参数定义
//!
//! 定义策略参数结构及其 Redis 加载逻辑：
//! - ArbDecision: 套利参数（订单量、超时、偏移、冷却时间等）

use anyhow::Result;
use log::{info, warn};
use serde::Deserialize;
use std::collections::HashMap;

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::common::symbol_util::normalize_symbol_for_venue;

use super::arb_decision::ArbDecision;
use super::mm_decision::MmDecision;
use crate::signal::common::TradingVenue;

/// Redis Key 配置
const DEFAULT_NAMESPACE: &str = "fr";

fn normalize_namespace(namespace: &str) -> String {
    let ns = namespace
        .trim()
        .trim_end_matches(['_', '-', ':'])
        .to_ascii_lowercase();
    if ns.is_empty() {
        DEFAULT_NAMESPACE.to_string()
    } else {
        ns
    }
}

fn normalize_mm_env_name(env_name: Option<&str>) -> String {
    let normalized = env_name
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());
    normalized.expect("MM strategy params require env_name; set ENV_NAME or run under an env dir")
}

fn infer_mm_env_name_from_runtime() -> String {
    if let Ok(env_name) = std::env::var("ENV_NAME") {
        let trimmed = env_name.trim();
        if !trimmed.is_empty() {
            return normalize_mm_env_name(Some(trimmed));
        }
    }

    let cwd = std::env::current_dir()
        .expect("MM strategy params require env_name; set ENV_NAME or run under an env dir");
    let leaf = cwd
        .file_name()
        .expect("MM strategy params require env_name; set ENV_NAME or run under an env dir")
        .to_string_lossy();
    normalize_mm_env_name(Some(leaf.as_ref()))
}

fn parse_bool_param(redis_key: &str, field: &str, raw: &str) -> bool {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => true,
        "false" | "0" | "no" | "off" | "" => false,
        _ => {
            panic!(
                "Redis hash '{}' {} 非法（仅支持 true/false）: {}",
                redis_key, field, raw
            )
        }
    }
}

fn parse_percentile_exclusive(redis_key: &str, field: &str, raw: &str) -> f64 {
    let value = raw.trim().parse::<f64>().unwrap_or_else(|_| {
        panic!(
            "Redis hash '{}' {} 无法解析为数字: {}",
            redis_key, field, raw
        )
    });
    if !(value.is_finite() && value > 0.0 && value < 99.0) {
        panic!(
            "Redis hash '{}' {} 无效(需在(0,99)内): {}",
            redis_key, field, value
        );
    }
    value
}

fn parse_utc_hhmm_minute(raw: &str) -> Option<u16> {
    let bytes = raw.as_bytes();
    if bytes.len() != 5 || bytes[2] != b':' {
        return None;
    }
    if !bytes[0].is_ascii_digit()
        || !bytes[1].is_ascii_digit()
        || !bytes[3].is_ascii_digit()
        || !bytes[4].is_ascii_digit()
    {
        return None;
    }
    let hour = u16::from(bytes[0] - b'0') * 10 + u16::from(bytes[1] - b'0');
    let minute = u16::from(bytes[3] - b'0') * 10 + u16::from(bytes[4] - b'0');
    if hour >= 24 || minute >= 60 {
        return None;
    }
    Some(hour * 60 + minute)
}

fn validate_open_block_utc_time_range(raw: &str) -> String {
    let trimmed = raw.trim();
    let Some((begin_raw, end_raw)) = trimmed.split_once('-') else {
        panic!(
            "open_block_utc_time_range 必须使用 UTC HH:MM-HH:MM 格式，例如 23:00-01:00: {}",
            raw
        );
    };
    let Some(begin_minute) = parse_utc_hhmm_minute(begin_raw) else {
        panic!(
            "open_block_utc_time_range begin 非法，必须为 UTC HH:MM: {}",
            begin_raw
        );
    };
    let Some(end_minute) = parse_utc_hhmm_minute(end_raw) else {
        panic!(
            "open_block_utc_time_range end 非法，必须为 UTC HH:MM: {}",
            end_raw
        );
    };
    if begin_minute == end_minute {
        panic!("open_block_utc_time_range begin/end 不能相同，避免全天/空窗口歧义: {raw}");
    }
    trimmed.to_string()
}

pub fn mm_strategy_params_key(hedge_venue: TradingVenue) -> String {
    format!("mm_strategy_params_{}", hedge_venue.data_pub_slug())
}

pub fn mm_amount_u_override_key_for_env(
    env_name: Option<&str>,
    hedge_venue: TradingVenue,
) -> String {
    let env_name = normalize_mm_env_name(env_name);
    format!("{env_name}:{}:mm:amount_u", hedge_venue.data_pub_slug())
}

pub fn mm_hedge_price_offset_limits_override_key_for_env(
    env_name: Option<&str>,
    hedge_venue: TradingVenue,
) -> String {
    let env_name = normalize_mm_env_name(env_name);
    format!(
        "{env_name}:{}:mm:hedge_price_offset_limits",
        hedge_venue.data_pub_slug()
    )
}

pub fn mm_hedge_price_offset_limit_upper_override_key_for_env(
    env_name: Option<&str>,
    hedge_venue: TradingVenue,
) -> String {
    let env_name = normalize_mm_env_name(env_name);
    format!(
        "{env_name}:{}:mm:hedge_price_offset_limit_upper",
        hedge_venue.data_pub_slug()
    )
}

pub fn mm_hedge_price_offset_limit_lower_override_key_for_env(
    env_name: Option<&str>,
    hedge_venue: TradingVenue,
) -> String {
    let env_name = normalize_mm_env_name(env_name);
    format!(
        "{env_name}:{}:mm:hedge_price_offset_limit_lower",
        hedge_venue.data_pub_slug()
    )
}

/// MM 的 per-symbol open_offset_lower 覆盖键。
/// 由 mm_config_server 写入 Redis STRING(JSON {symbol: f64})，单位价格分数。
pub fn mm_open_offset_lower_override_key_for_env(
    env_name: Option<&str>,
    hedge_venue: TradingVenue,
) -> String {
    let env_name = normalize_mm_env_name(env_name);
    format!(
        "{env_name}:{}:mm:open_offset_lower",
        hedge_venue.data_pub_slug()
    )
}

/// Arb（intra/cross/fr）共用的 per-symbol amount_u 覆盖键。
/// 由 sync_*_amount_u.py 写入 Redis STRING(JSON {symbol: amount_u})。
/// 与 MM 不同，arb 用 (open, hedge) 二元组定位，覆盖键可选（env 缺失时返回 None，
/// 让加载流程优雅 fallback 到 default order_amount）。
pub fn arb_amount_u_override_key(
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

/// Arb（intra/cross/fr）共用的 per-symbol open_offset_lower 覆盖键。
/// 由 config server 写入 Redis STRING(JSON {symbol: f64})，单位价格分数。
pub fn arb_open_offset_lower_override_key(
    env_name: Option<&str>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<String> {
    let env_name = env_name.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!(
        "{env_name}:{}:{}:open_offset_lower_overrides",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    ))
}

/// Arb（intra/cross/fr）共用的 per-symbol hedge_price_offset_limit 合并 STRING 键：
/// JSON {symbol: {hedge_price_offset_limit_lower, hedge_price_offset_limit_upper}}。
/// 加载时优先读这个键，缺失再回退到拆分的 upper / lower 两个键。
pub fn arb_hedge_price_offset_limits_override_key(
    env_name: Option<&str>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<String> {
    let env_name = env_name.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!(
        "{env_name}:{}:{}:hedge_price_offset_limits",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    ))
}

/// 拆分版 hedge_price_offset_limit_upper：JSON {symbol: f64}。
pub fn arb_hedge_price_offset_limit_upper_override_key(
    env_name: Option<&str>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<String> {
    let env_name = env_name.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!(
        "{env_name}:{}:{}:hedge_price_offset_limit_upper",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    ))
}

/// 拆分版 hedge_price_offset_limit_lower：JSON {symbol: f64}。
pub fn arb_hedge_price_offset_limit_lower_override_key(
    env_name: Option<&str>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<String> {
    let env_name = env_name.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!(
        "{env_name}:{}:{}:hedge_price_offset_limit_lower",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    ))
}

/// 从运行时（ENV_NAME 环境变量或当前工作目录名）推断 arb 的 env_name。
/// 与 MM 版本不同：env 缺失时不 panic，返回 None。
fn infer_arb_env_name_from_runtime() -> Option<String> {
    if let Ok(env_name) = std::env::var("ENV_NAME") {
        let trimmed = env_name.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_ascii_lowercase());
        }
    }
    let cwd = std::env::current_dir().ok()?;
    let leaf = cwd
        .file_name()?
        .to_string_lossy()
        .trim()
        .to_ascii_lowercase();
    if leaf.is_empty() {
        return None;
    }
    Some(leaf)
}

fn normalize_mm_override_symbol(
    raw_symbol: &str,
    open_venue: TradingVenue,
    redis_key: &str,
) -> String {
    let symbol_trimmed = raw_symbol.trim();
    if symbol_trimmed.is_empty() {
        panic!("Redis string '{}' 包含空 symbol", redis_key);
    }
    let normalized_input = symbol_trimmed
        .chars()
        .filter_map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => Some(ch.to_ascii_uppercase()),
            '-' | '_' | '/' | ' ' => Some('-'),
            _ => None,
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if normalized_input.is_empty() {
        panic!("Redis string '{}' 包含空 symbol", redis_key);
    }
    normalize_symbol_for_venue(&normalized_input, open_venue)
}

fn parse_mm_positive_f64_overrides(
    raw: &str,
    open_venue: TradingVenue,
    redis_key: &str,
    field_name: &str,
) -> HashMap<String, f64> {
    let parsed: HashMap<String, f64> = serde_json::from_str(raw).unwrap_or_else(|err| {
        panic!(
            "Redis string '{}' 不是合法 JSON(symbol->{}): {} ({})",
            redis_key, field_name, raw, err
        )
    });

    let mut normalized = HashMap::new();
    for (symbol, value) in parsed {
        let symbol_trimmed = symbol.trim();
        if !(value.is_finite() && value > 0.0) {
            panic!(
                "Redis string '{}' symbol={} {} 非法: {}",
                redis_key, symbol_trimmed, field_name, value
            );
        }
        let symbol_key = normalize_mm_override_symbol(symbol_trimmed, open_venue, redis_key);
        normalized.insert(symbol_key, value);
    }
    normalized
}

fn parse_mm_amount_u_overrides(
    raw: &str,
    open_venue: TradingVenue,
    redis_key: &str,
) -> HashMap<String, f64> {
    parse_mm_positive_f64_overrides(raw, open_venue, redis_key, "amount_u")
}

fn parse_mm_open_offset_lower_overrides(
    raw: &str,
    open_venue: TradingVenue,
    redis_key: &str,
) -> HashMap<String, f64> {
    let parsed: HashMap<String, f64> = serde_json::from_str(raw).unwrap_or_else(|err| {
        panic!(
            "Redis string '{}' 不是合法 JSON(symbol->open_offset_lower): {} ({})",
            redis_key, raw, err
        )
    });

    let mut normalized = HashMap::new();
    for (symbol, value) in parsed {
        let symbol_trimmed = symbol.trim();
        if !(value.is_finite() && value >= 0.0) {
            panic!(
                "Redis string '{}' symbol={} open_offset_lower 非法: {}",
                redis_key, symbol_trimmed, value
            );
        }
        let symbol_key = normalize_mm_override_symbol(symbol_trimmed, open_venue, redis_key);
        normalized.insert(symbol_key, value);
    }
    normalized
}

/// `parse_mm_positive_f64_overrides` 的零下限版本：允许 0（"不 clamp"语义），
/// 仍要求 finite 且非负。
fn parse_arb_open_offset_lower_overrides(
    raw: &str,
    open_venue: TradingVenue,
    redis_key: &str,
) -> HashMap<String, f64> {
    let parsed: HashMap<String, f64> = serde_json::from_str(raw).unwrap_or_else(|err| {
        panic!(
            "Redis string '{}' 不是合法 JSON(symbol->open_offset_lower): {} ({})",
            redis_key, raw, err
        )
    });

    let mut normalized = HashMap::new();
    for (symbol, value) in parsed {
        let symbol_trimmed = symbol.trim();
        if !(value.is_finite() && value >= 0.0) {
            panic!(
                "Redis string '{}' symbol={} open_offset_lower 非法: {}",
                redis_key, symbol_trimmed, value
            );
        }
        let symbol_key = normalize_mm_override_symbol(symbol_trimmed, open_venue, redis_key);
        normalized.insert(symbol_key, value);
    }
    normalized
}

#[derive(Debug, Deserialize)]
struct MmHedgePriceOffsetLimitOverride {
    #[serde(alias = "lower")]
    hedge_price_offset_limit_lower: f64,
    #[serde(alias = "upper")]
    hedge_price_offset_limit_upper: f64,
}

fn parse_mm_hedge_price_offset_limit_overrides(
    raw: &str,
    open_venue: TradingVenue,
    redis_key: &str,
) -> (HashMap<String, f64>, HashMap<String, f64>) {
    let parsed: HashMap<String, MmHedgePriceOffsetLimitOverride> = serde_json::from_str(raw)
        .unwrap_or_else(|err| {
            panic!(
                "Redis string '{}' 不是合法 JSON(symbol->hedge_price_offset_limits): {} ({})",
                redis_key, raw, err
            )
        });

    let mut lower_overrides = HashMap::new();
    let mut upper_overrides = HashMap::new();
    for (symbol, limits) in parsed {
        let symbol_trimmed = symbol.trim();
        let lower = limits.hedge_price_offset_limit_lower;
        let upper = limits.hedge_price_offset_limit_upper;
        if !(lower.is_finite() && upper.is_finite() && lower > 0.0 && upper >= lower) {
            panic!(
                "Redis string '{}' symbol={} hedge_price_offset_limit 非法: lower={} upper={} (need 0<lower<=upper)",
                redis_key, symbol_trimmed, lower, upper
            );
        }
        let symbol_key = normalize_mm_override_symbol(symbol_trimmed, open_venue, redis_key);
        lower_overrides.insert(symbol_key.clone(), lower);
        upper_overrides.insert(symbol_key, upper);
    }
    (lower_overrides, upper_overrides)
}

fn strategy_params_key(
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> String {
    let ns = normalize_namespace(namespace);
    if ns == "mm" {
        return mm_strategy_params_key(hedge_venue);
    }
    let prefix = if ns == "fr" {
        "fr_strategy_params".to_string()
    } else {
        format!("{ns}_strategy_params")
    };
    format!(
        "{}_{}_{}",
        prefix,
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn order_amount_field_name(namespace: &str) -> &'static str {
    if normalize_namespace(namespace) == "mm" {
        "default_order_amount"
    } else {
        "order_amount"
    }
}

/// 策略参数结构（从 Redis Hash 反序列化）
#[derive(Debug, Clone, Deserialize)]
pub struct StrategyParams {
    /// 单笔下单量（USDT）
    #[serde(default = "default_order_amount")]
    pub order_amount: f32,

    /// MM 按 symbol 覆盖的下单量（USDT）
    #[serde(default)]
    pub mm_amount_u_overrides: HashMap<String, f64>,

    /// MM 按 symbol 覆盖的开仓内层 offset 下限（价格分数；缺省取全局默认 0.0005）。
    /// Redis STRING key: <env>:<hedge_venue>:mm:open_offset_lower
    #[serde(default)]
    pub mm_open_offset_lower_overrides: HashMap<String, f64>,

    /// MM 按 symbol 覆盖的对冲侧 price_offset_limit upper
    /// Redis STRING key: <env>:<hedge_venue>:mm:hedge_price_offset_limits
    #[serde(default)]
    pub mm_hedge_price_offset_limit_upper_overrides: HashMap<String, f64>,

    /// MM 按 symbol 覆盖的对冲侧 price_offset_limit lower
    /// Redis STRING key: <env>:<hedge_venue>:mm:hedge_price_offset_limits
    #[serde(default)]
    pub mm_hedge_price_offset_limit_lower_overrides: HashMap<String, f64>,

    /// Arb（intra/cross/fr）按 symbol 覆盖的下单量（USDT）
    /// Redis STRING key: <env>:<open>:<hedge>:amount_u_overrides
    #[serde(default)]
    pub arb_amount_u_overrides: HashMap<String, f64>,

    /// Arb 按 symbol 覆盖的对冲侧 price_offset_limit upper
    /// Redis STRING key: <env>:<open>:<hedge>:hedge_price_offset_limits（合并）
    /// 或 <env>:<open>:<hedge>:hedge_price_offset_limit_upper（拆分回退）
    #[serde(default)]
    pub arb_hedge_price_offset_limit_upper_overrides: HashMap<String, f64>,

    /// Arb 按 symbol 覆盖的对冲侧 price_offset_limit lower
    #[serde(default)]
    pub arb_hedge_price_offset_limit_lower_overrides: HashMap<String, f64>,

    /// Arb 按 symbol 覆盖的开仓内层 offset 下限（价格分数；0 = 不 clamp）。
    /// Redis STRING key: <env>:<open>:<hedge>:open_offset_lower_overrides
    #[serde(default)]
    pub arb_open_offset_lower_overrides: HashMap<String, f64>,

    /// arb 开仓 plan 波动带缩放倍率 [low, high]（JSON 数组字符串，实际偏移区间=vol*[low,high]）
    #[serde(default = "default_vol_band_scale")]
    pub vol_band_scale: String,

    /// MM open 买侧波动区间缩放系数 [low, high]
    #[serde(default = "default_open_buy_vol_scale")]
    pub open_buy_vol_scale: String,

    /// MM open 卖侧波动区间缩放系数 [low, high]
    #[serde(default = "default_open_sell_vol_scale")]
    pub open_sell_vol_scale: String,

    /// MM 报单触发间隔（毫秒）
    #[serde(default = "default_order_interval_ms")]
    pub order_interval_ms: u64,

    /// MM 每个 symbol 的最大随机时钟偏移（毫秒）；0 表示关闭
    #[serde(default = "default_clock_shift_ms", alias = "enable_clock_shift_ms")]
    pub clock_shift_ms: u64,

    /// MM 每轮报单数量
    #[serde(default = "default_open_orders_per_round")]
    pub open_orders_per_round: u32,

    #[serde(default = "default_hedge_orders_per_round")]
    pub hedge_orders_per_round: u32,

    /// 开仓订单超时（秒）
    #[serde(default = "default_open_order_timeout")]
    pub open_order_timeout: u64,

    #[serde(default = "default_next_query_delay_ms")]
    pub next_query_delay_ms: u64,

    #[serde(default = "default_hedge_vol_multiplier")]
    pub hedge_vol_multiplier: f64,

    #[serde(default = "default_hedge_offset_ratio")]
    pub hedge_offset_ratio: f64,

    #[serde(default = "default_hedge_price_offset_limit_upper")]
    pub hedge_price_offset_limit_upper: f64,

    #[serde(default = "default_hedge_price_offset_limit_lower")]
    pub hedge_price_offset_limit_lower: f64,

    #[serde(default = "default_hedge_window_scale_low")]
    pub hedge_window_scale_low: f64,

    #[serde(default = "default_hedge_window_scale_high")]
    pub hedge_window_scale_high: f64,

    /// 对冲订单超时（秒）
    #[serde(default = "default_hedge_timeout")]
    pub hedge_timeout: u64,

    /// 对冲 request_seq 激进阈值（>=该值不偏移但仍挂 maker）
    #[serde(default = "default_hedge_aggressive_seq_threshold")]
    pub hedge_aggressive_seq_threshold: u32,

    /// 对冲触发 taker 的最大价格变动比例（百分比，允许小数，范围 (0, 99]）
    #[serde(default = "default_max_hedge_price_pct_change")]
    pub max_hedge_price_pct_change: f64,

    /// 信号冷却时间（秒）
    #[serde(default = "default_signal_cooldown")]
    pub signal_cooldown: u64,

    /// MM 是否启用基于 return score quantile 的方向撤单
    #[serde(default = "default_enable_return_score_cancel")]
    pub enable_return_score_cancel: bool,

    /// return score quantile 大于该百分位时，撤 sell 方向 open 单
    #[serde(default = "default_return_score_buy_cancel_quantile")]
    pub return_score_buy_cancel_quantile: f64,

    /// return score quantile 小于该百分位时，撤 buy/long 方向 open 单
    #[serde(default = "default_return_score_sell_cancel_quantile")]
    pub return_score_sell_cancel_quantile: f64,

    /// MM 是否启用基于 tlen 的开仓撤单链路（trigger/query/cancel）
    #[serde(default = "default_enable_tlen_cancel")]
    pub enable_tlen_cancel: bool,

    /// MM 开仓撤单触发频率限制（毫秒）
    #[serde(default = "default_tlen_cancel_freq_ms")]
    pub tlen_cancel_freq_ms: u64,

    /// Spread cancel 信号限流（毫秒），仅作用于 intra/cross spread cancel
    #[serde(default = "default_spread_cancel_cooldown_ms")]
    pub spread_cancel_cooldown_ms: u64,

    /// MM hedge 是否允许 return score 调整 hedge offset（false=使用中性 score 计算）
    #[serde(default = "default_enable_return_score_adjust_hedge")]
    pub enable_return_score_adjust_hedge: bool,

    /// 是否启用 env / pnlu 开仓限制（false=只读取并写入 from_key，不阻拦开仓）
    #[serde(default = "default_enable_environment_model")]
    pub enable_environment_model: bool,

    /// 是否启用波动率限制下单
    #[serde(default = "default_enable_volatility_limit")]
    pub enable_volatility_limit: bool,

    /// 波动率限制分位数（由下游波动率阈值逻辑使用）
    #[serde(default = "default_open_volatility_limit")]
    pub open_volatility_limit: f64,

    /// 是否启用 tradecount 限制下单（仅 MM；count.rolling(30,min_periods=25).mean() > threshold 才允许 open）
    #[serde(default = "default_enable_tradecount_limit")]
    pub enable_tradecount_limit: bool,

    /// tradecount 限制分位数（MM 决策侧对 count.rolling(30,min_periods=25).mean() 做内联阈值采样）
    #[serde(default = "default_open_tradecount_limit")]
    pub open_tradecount_limit: f64,

    /// 是否启用 UTC 日内时间段开仓阻断（仅 MM open）
    #[serde(default = "default_enable_open_time_block")]
    pub enable_open_time_block: bool,

    /// UTC 日内开仓阻断窗口，格式 HH:MM-HH:MM，允许跨天但 begin/end 不能相同
    #[serde(default = "default_open_block_utc_time_range")]
    pub open_block_utc_time_range: String,

    /// 收益率模型输出通道（"-" 表示禁用）
    #[serde(default = "default_return_model_service")]
    pub return_model_service: String,

    /// 环境模型输出通道（"-" 表示禁用）
    #[serde(default = "default_environment_model_service")]
    pub environment_model_service: String,
}

// 默认值函数
fn default_order_amount() -> f32 {
    100.0
}
fn default_vol_band_scale() -> String {
    "[0.0,1.0]".to_string()
}
fn default_open_buy_vol_scale() -> String {
    "[0.0,1.0]".to_string()
}
fn default_open_sell_vol_scale() -> String {
    "[0.0,1.0]".to_string()
}
fn default_order_interval_ms() -> u64 {
    5_000
}
fn default_clock_shift_ms() -> u64 {
    0
}
fn default_open_orders_per_round() -> u32 {
    1
}
fn default_hedge_orders_per_round() -> u32 {
    8
}
fn default_open_order_timeout() -> u64 {
    120
}
fn default_next_query_delay_ms() -> u64 {
    30_000
}
fn default_hedge_vol_multiplier() -> f64 {
    2.0
}
fn default_hedge_offset_ratio() -> f64 {
    1.3
}
fn default_hedge_price_offset_limit_upper() -> f64 {
    0.005
}
fn default_hedge_price_offset_limit_lower() -> f64 {
    0.0003
}
fn default_mm_hedge_price_offset_limit_lower() -> f64 {
    0.0005
}
fn default_hedge_window_scale_low() -> f64 {
    0.8
}
fn default_hedge_window_scale_high() -> f64 {
    1.3
}
fn default_hedge_timeout() -> u64 {
    30
}
fn default_hedge_aggressive_seq_threshold() -> u32 {
    6
}
fn default_max_hedge_price_pct_change() -> f64 {
    5.0
}
fn default_signal_cooldown() -> u64 {
    5
}
fn default_enable_return_score_cancel() -> bool {
    false
}
fn default_return_score_buy_cancel_quantile() -> f64 {
    90.0
}
fn default_return_score_sell_cancel_quantile() -> f64 {
    10.0
}
fn default_enable_tlen_cancel() -> bool {
    false
}
fn default_tlen_cancel_freq_ms() -> u64 {
    3_000
}
fn default_spread_cancel_cooldown_ms() -> u64 {
    100
}
fn default_enable_return_score_adjust_hedge() -> bool {
    true
}
fn default_enable_environment_model() -> bool {
    true
}
fn default_enable_volatility_limit() -> bool {
    true
}
fn default_open_volatility_limit() -> f64 {
    70.0
}
fn default_enable_tradecount_limit() -> bool {
    false
}
fn default_open_tradecount_limit() -> f64 {
    70.0
}
fn default_enable_open_time_block() -> bool {
    false
}
fn default_open_block_utc_time_range() -> String {
    "00:00-00:01".to_string()
}
fn default_return_model_service() -> String {
    "return_model".to_string()
}
fn default_environment_model_service() -> String {
    "environment_model".to_string()
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            order_amount: default_order_amount(),
            mm_amount_u_overrides: HashMap::new(),
            mm_open_offset_lower_overrides: HashMap::new(),
            mm_hedge_price_offset_limit_upper_overrides: HashMap::new(),
            mm_hedge_price_offset_limit_lower_overrides: HashMap::new(),
            arb_amount_u_overrides: HashMap::new(),
            arb_hedge_price_offset_limit_upper_overrides: HashMap::new(),
            arb_hedge_price_offset_limit_lower_overrides: HashMap::new(),
            arb_open_offset_lower_overrides: HashMap::new(),
            vol_band_scale: default_vol_band_scale(),
            open_buy_vol_scale: default_open_buy_vol_scale(),
            open_sell_vol_scale: default_open_sell_vol_scale(),
            order_interval_ms: default_order_interval_ms(),
            clock_shift_ms: default_clock_shift_ms(),
            open_orders_per_round: default_open_orders_per_round(),
            hedge_orders_per_round: default_hedge_orders_per_round(),
            open_order_timeout: default_open_order_timeout(),
            next_query_delay_ms: default_next_query_delay_ms(),
            hedge_vol_multiplier: default_hedge_vol_multiplier(),
            hedge_offset_ratio: default_hedge_offset_ratio(),
            hedge_price_offset_limit_upper: default_hedge_price_offset_limit_upper(),
            hedge_price_offset_limit_lower: default_hedge_price_offset_limit_lower(),
            hedge_window_scale_low: default_hedge_window_scale_low(),
            hedge_window_scale_high: default_hedge_window_scale_high(),
            hedge_timeout: default_hedge_timeout(),
            hedge_aggressive_seq_threshold: default_hedge_aggressive_seq_threshold(),
            max_hedge_price_pct_change: default_max_hedge_price_pct_change(),
            signal_cooldown: default_signal_cooldown(),
            enable_return_score_cancel: default_enable_return_score_cancel(),
            return_score_buy_cancel_quantile: default_return_score_buy_cancel_quantile(),
            return_score_sell_cancel_quantile: default_return_score_sell_cancel_quantile(),
            enable_tlen_cancel: default_enable_tlen_cancel(),
            tlen_cancel_freq_ms: default_tlen_cancel_freq_ms(),
            spread_cancel_cooldown_ms: default_spread_cancel_cooldown_ms(),
            enable_return_score_adjust_hedge: default_enable_return_score_adjust_hedge(),
            enable_environment_model: default_enable_environment_model(),
            enable_volatility_limit: default_enable_volatility_limit(),
            open_volatility_limit: default_open_volatility_limit(),
            enable_tradecount_limit: default_enable_tradecount_limit(),
            open_tradecount_limit: default_open_tradecount_limit(),
            enable_open_time_block: default_enable_open_time_block(),
            open_block_utc_time_range: default_open_block_utc_time_range(),
            return_model_service: default_return_model_service(),
            environment_model_service: default_environment_model_service(),
        }
    }
}

impl StrategyParams {
    /// 从 Redis Hash 加载
    pub async fn load_from_redis(
        redis: &RedisSettings,
        namespace: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Self> {
        let mut client = RedisClient::connect(redis.clone()).await?;
        let ns = normalize_namespace(namespace);
        let mm_env_name = if ns == "mm" {
            Some(infer_mm_env_name_from_runtime())
        } else {
            None
        };
        let redis_key = if ns == "mm" {
            mm_strategy_params_key(hedge_venue)
        } else {
            strategy_params_key(&ns, open_venue, hedge_venue)
        };
        let hash_map = client.hgetall_map(&redis_key).await?;
        if hash_map.is_empty() {
            panic!("Redis hash '{}' 为空或不存在，无法加载策略参数", redis_key);
        }

        // 手动解析 Hash 字段
        let order_amount_field = order_amount_field_name(&ns);
        let order_amount = match hash_map.get(order_amount_field) {
            Some(raw) => raw.parse::<f32>().unwrap_or_else(|_| {
                panic!(
                    "Redis hash '{}' {} 无法解析为数字: {}",
                    redis_key, order_amount_field, raw
                )
            }),
            None if ns == "mm" => {
                panic!("Redis hash '{}' 缺少 {}", redis_key, order_amount_field)
            }
            None => default_order_amount(),
        };
        let mm_amount_u_overrides = if ns == "mm" {
            let override_key =
                mm_amount_u_override_key_for_env(mm_env_name.as_deref(), hedge_venue);
            match client.get_string(&override_key).await? {
                Some(raw) => {
                    let parsed = parse_mm_amount_u_overrides(&raw, open_venue, &override_key);
                    info!(
                        "MM amount_u overrides loaded key='{}' symbols={}",
                        override_key,
                        parsed.len()
                    );
                    parsed
                }
                None => {
                    info!(
                        "MM amount_u override missing; use default_order_amount from key='{}'",
                        redis_key
                    );
                    HashMap::new()
                }
            }
        } else {
            HashMap::new()
        };
        // MM 的 per-symbol open_offset_lower 覆盖：缺失走全局默认（0.0005，由 MmDecision 持有）。
        let mm_open_offset_lower_overrides = if ns == "mm" {
            let override_key =
                mm_open_offset_lower_override_key_for_env(mm_env_name.as_deref(), hedge_venue);
            match client.get_string(&override_key).await? {
                Some(raw) => {
                    let parsed =
                        parse_mm_open_offset_lower_overrides(&raw, open_venue, &override_key);
                    info!(
                        "MM open_offset_lower overrides loaded key='{}' symbols={}",
                        override_key,
                        parsed.len()
                    );
                    parsed
                }
                None => {
                    info!(
                        "MM open_offset_lower override missing key='{}'; use default 0.0005",
                        override_key
                    );
                    HashMap::new()
                }
            }
        } else {
            HashMap::new()
        };
        let (
            mm_hedge_price_offset_limit_lower_overrides,
            mm_hedge_price_offset_limit_upper_overrides,
        ) = if ns == "mm" {
            let limits_key = mm_hedge_price_offset_limits_override_key_for_env(
                mm_env_name.as_deref(),
                hedge_venue,
            );
            match client.get_string(&limits_key).await? {
                Some(raw) => {
                    let (lower_overrides, upper_overrides) =
                        parse_mm_hedge_price_offset_limit_overrides(&raw, open_venue, &limits_key);
                    info!(
                        "MM hedge_price_offset_limits overrides loaded key='{}' symbols={}",
                        limits_key,
                        lower_overrides.len()
                    );
                    (lower_overrides, upper_overrides)
                }
                None => {
                    let upper_key = mm_hedge_price_offset_limit_upper_override_key_for_env(
                        mm_env_name.as_deref(),
                        hedge_venue,
                    );
                    let upper_overrides = match client.get_string(&upper_key).await? {
                        Some(raw) => {
                            let parsed = parse_mm_positive_f64_overrides(
                                &raw,
                                open_venue,
                                &upper_key,
                                "hedge_price_offset_limit_upper",
                            );
                            info!(
                                "MM hedge_price_offset_limit_upper split fallback loaded key='{}' symbols={}",
                                upper_key,
                                parsed.len()
                            );
                            parsed
                        }
                        None => HashMap::new(),
                    };

                    let lower_key = mm_hedge_price_offset_limit_lower_override_key_for_env(
                        mm_env_name.as_deref(),
                        hedge_venue,
                    );
                    let lower_overrides = match client.get_string(&lower_key).await? {
                        Some(raw) => {
                            let parsed = parse_mm_positive_f64_overrides(
                                &raw,
                                open_venue,
                                &lower_key,
                                "hedge_price_offset_limit_lower",
                            );
                            info!(
                                "MM hedge_price_offset_limit_lower split fallback loaded key='{}' symbols={}",
                                lower_key,
                                parsed.len()
                            );
                            parsed
                        }
                        None => HashMap::new(),
                    };

                    if lower_overrides.is_empty() && upper_overrides.is_empty() {
                        info!(
                            "MM hedge_price_offset_limits override missing key='{}'; use global lower/upper from key='{}'",
                            limits_key, redis_key
                        );
                    }
                    (lower_overrides, upper_overrides)
                }
            }
        } else {
            (HashMap::new(), HashMap::new())
        };
        // Arb（intra/cross/fr）的 per-symbol amount_u 覆盖：可选，env 缺失或键不存在
        // 都视为没有覆盖，回退到 strategy_params 的 default order_amount。
        let arb_amount_u_overrides = if ns == "intra" || ns == "cross" || ns == "fr" {
            let arb_env_name = infer_arb_env_name_from_runtime();
            match arb_amount_u_override_key(arb_env_name.as_deref(), open_venue, hedge_venue) {
                Some(override_key) => match client.get_string(&override_key).await? {
                    Some(raw) => {
                        let parsed = parse_mm_amount_u_overrides(&raw, open_venue, &override_key);
                        info!(
                            "Arb amount_u overrides loaded ns={} key='{}' symbols={}",
                            ns,
                            override_key,
                            parsed.len()
                        );
                        parsed
                    }
                    None => {
                        info!(
                            "Arb amount_u override missing ns={} key='{}'; use default order_amount",
                            ns, override_key
                        );
                        HashMap::new()
                    }
                },
                None => {
                    info!(
                        "Arb amount_u override skipped ns={} (env_name unavailable); use default order_amount",
                        ns
                    );
                    HashMap::new()
                }
            }
        } else {
            HashMap::new()
        };
        // Arb（intra/cross/fr）的 per-symbol open_offset_lower 覆盖：可选。env 缺失或键
        // 不存在视为没有覆盖，回退到 0（不 clamp，保持本字段加入前的行为）。
        let arb_open_offset_lower_overrides = if ns == "intra" || ns == "cross" || ns == "fr" {
            let arb_env_name = infer_arb_env_name_from_runtime();
            match arb_open_offset_lower_override_key(
                arb_env_name.as_deref(),
                open_venue,
                hedge_venue,
            ) {
                Some(override_key) => match client.get_string(&override_key).await? {
                    Some(raw) => {
                        let parsed =
                            parse_arb_open_offset_lower_overrides(&raw, open_venue, &override_key);
                        info!(
                            "Arb open_offset_lower overrides loaded ns={} key='{}' symbols={}",
                            ns,
                            override_key,
                            parsed.len()
                        );
                        parsed
                    }
                    None => {
                        info!(
                            "Arb open_offset_lower override missing ns={} key='{}'; lower=0 (no clamp)",
                            ns, override_key
                        );
                        HashMap::new()
                    }
                },
                None => {
                    info!(
                        "Arb open_offset_lower override skipped ns={} (env_name unavailable); lower=0",
                        ns
                    );
                    HashMap::new()
                }
            }
        } else {
            HashMap::new()
        };
        // Arb 的 per-symbol hedge_price_offset_limit 覆盖：先试合并 STRING（mm 同款 schema），
        // 缺失再回退到拆分的 upper / lower 两个 STRING。任一存在都按 per-symbol 覆盖处理；
        // 三者全缺则使用 strategy_params 全局 lower / upper。
        let (
            arb_hedge_price_offset_limit_lower_overrides,
            arb_hedge_price_offset_limit_upper_overrides,
        ) = if ns == "intra" || ns == "cross" || ns == "fr" {
            let arb_env_name = infer_arb_env_name_from_runtime();
            match arb_hedge_price_offset_limits_override_key(
                arb_env_name.as_deref(),
                open_venue,
                hedge_venue,
            ) {
                Some(limits_key) => match client.get_string(&limits_key).await? {
                    Some(raw) => {
                        let (lower_overrides, upper_overrides) =
                            parse_mm_hedge_price_offset_limit_overrides(
                                &raw,
                                open_venue,
                                &limits_key,
                            );
                        info!(
                            "Arb hedge_price_offset_limits overrides loaded ns={} key='{}' symbols={}",
                            ns,
                            limits_key,
                            lower_overrides.len()
                        );
                        (lower_overrides, upper_overrides)
                    }
                    None => {
                        let upper_key = arb_hedge_price_offset_limit_upper_override_key(
                            arb_env_name.as_deref(),
                            open_venue,
                            hedge_venue,
                        );
                        let upper_overrides = match upper_key {
                            Some(ref key) => match client.get_string(key).await? {
                                Some(raw) => {
                                    let parsed = parse_mm_positive_f64_overrides(
                                        &raw,
                                        open_venue,
                                        key,
                                        "hedge_price_offset_limit_upper",
                                    );
                                    info!(
                                        "Arb hedge_price_offset_limit_upper split fallback loaded ns={} key='{}' symbols={}",
                                        ns,
                                        key,
                                        parsed.len()
                                    );
                                    parsed
                                }
                                None => HashMap::new(),
                            },
                            None => HashMap::new(),
                        };

                        let lower_key = arb_hedge_price_offset_limit_lower_override_key(
                            arb_env_name.as_deref(),
                            open_venue,
                            hedge_venue,
                        );
                        let lower_overrides = match lower_key {
                            Some(ref key) => match client.get_string(key).await? {
                                Some(raw) => {
                                    let parsed = parse_mm_positive_f64_overrides(
                                        &raw,
                                        open_venue,
                                        key,
                                        "hedge_price_offset_limit_lower",
                                    );
                                    info!(
                                        "Arb hedge_price_offset_limit_lower split fallback loaded ns={} key='{}' symbols={}",
                                        ns,
                                        key,
                                        parsed.len()
                                    );
                                    parsed
                                }
                                None => HashMap::new(),
                            },
                            None => HashMap::new(),
                        };

                        if lower_overrides.is_empty() && upper_overrides.is_empty() {
                            info!(
                                "Arb hedge_price_offset_limits override missing ns={} key='{}'; use global lower/upper from key='{}'",
                                ns, limits_key, redis_key
                            );
                        }
                        (lower_overrides, upper_overrides)
                    }
                },
                None => {
                    info!(
                        "Arb hedge_price_offset_limits override skipped ns={} (env_name unavailable); use global lower/upper",
                        ns
                    );
                    (HashMap::new(), HashMap::new())
                }
            }
        } else {
            (HashMap::new(), HashMap::new())
        };
        let vol_band_scale = match hash_map.get("vol_band_scale") {
            Some(raw) => raw.to_string(),
            None => default_vol_band_scale(),
        };
        let open_buy_vol_scale = match hash_map.get("open_buy_vol_scale") {
            Some(raw) => raw.to_string(),
            None => {
                if ns == "mm" {
                    panic!("Redis hash '{}' 缺少 open_buy_vol_scale", redis_key);
                }
                default_open_buy_vol_scale()
            }
        };
        let open_sell_vol_scale = match hash_map.get("open_sell_vol_scale") {
            Some(raw) => raw.to_string(),
            None => {
                if ns == "mm" {
                    panic!("Redis hash '{}' 缺少 open_sell_vol_scale", redis_key);
                }
                default_open_sell_vol_scale()
            }
        };

        let order_interval_ms = match hash_map.get("order_interval_ms") {
            Some(raw) => {
                let parsed = raw.parse::<i64>().unwrap_or_else(|_| {
                    panic!(
                        "Redis hash '{}' order_interval_ms 无法解析为整数: {}",
                        redis_key, raw
                    )
                });
                if parsed <= 0 {
                    panic!(
                        "Redis hash '{}' order_interval_ms 无效(需>0): {}",
                        redis_key, parsed
                    );
                }
                parsed as u64
            }
            None => default_order_interval_ms(),
        };
        let clock_shift_ms = match hash_map
            .get("enable_clock_shift_ms")
            .or_else(|| hash_map.get("clock_shift_ms"))
        {
            Some(raw) => {
                let parsed = raw.parse::<i64>().unwrap_or_else(|_| {
                    panic!(
                        "Redis hash '{}' enable_clock_shift_ms 无法解析为整数: {}",
                        redis_key, raw
                    )
                });
                if parsed < 0 {
                    panic!(
                        "Redis hash '{}' enable_clock_shift_ms 无效(需>=0): {}",
                        redis_key, parsed
                    );
                }
                parsed as u64
            }
            None => default_clock_shift_ms(),
        };

        let open_orders_per_round = match hash_map.get("open_orders_per_round") {
            Some(raw) => {
                let parsed = raw.parse::<i64>().unwrap_or_else(|_| {
                    panic!(
                        "Redis hash '{}' open_orders_per_round 无法解析为整数: {}",
                        redis_key, raw
                    )
                });
                if parsed <= 0 {
                    panic!(
                        "Redis hash '{}' open_orders_per_round 无效(需>0): {}",
                        redis_key, parsed
                    );
                }
                parsed as u32
            }
            None => default_open_orders_per_round(),
        };
        let hedge_orders_per_round = hash_map
            .get("hedge_orders_per_round")
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or_else(default_hedge_orders_per_round);

        let open_order_timeout = hash_map
            .get("open_order_timeout")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(default_open_order_timeout);
        let next_query_delay_ms = hash_map
            .get("next_query_delay_ms")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(default_next_query_delay_ms);
        if clock_shift_ms > 0
            && (clock_shift_ms >= order_interval_ms || clock_shift_ms >= next_query_delay_ms)
        {
            panic!(
                "Redis hash '{}' enable_clock_shift_ms 必须小于 order_interval_ms 和 next_query_delay_ms: enable_clock_shift_ms={} order_interval_ms={} next_query_delay_ms={}",
                redis_key, clock_shift_ms, order_interval_ms, next_query_delay_ms
            );
        }
        let hedge_vol_multiplier = hash_map
            .get("hedge_vol_multiplier")
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or_else(default_hedge_vol_multiplier);
        let hedge_offset_ratio = hash_map
            .get("hedge_offset_ratio")
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or_else(default_hedge_offset_ratio);
        let hedge_price_offset_limit_upper = hash_map
            .get("hedge_price_offset_limit_upper")
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite())
            .unwrap_or_else(default_hedge_price_offset_limit_upper);
        let hedge_price_offset_limit_lower = hash_map
            .get("hedge_price_offset_limit_lower")
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite())
            .unwrap_or_else(|| {
                if ns == "mm" {
                    default_mm_hedge_price_offset_limit_lower()
                } else {
                    default_hedge_price_offset_limit_lower()
                }
            });
        let hedge_window_scale_low = hash_map
            .get("hedge_window_scale_low")
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or_else(default_hedge_window_scale_low);
        let hedge_window_scale_high = hash_map
            .get("hedge_window_scale_high")
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= hedge_window_scale_low)
            .unwrap_or_else(|| default_hedge_window_scale_high().max(hedge_window_scale_low));

        let hedge_timeout = hash_map
            .get("hedge_timeout")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(default_hedge_timeout);

        let hedge_aggressive_seq_threshold = hash_map
            .get("hedge_aggressive_seq_threshold")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or_else(default_hedge_aggressive_seq_threshold);

        let require_max_hedge_pct = ns == "intra" || ns == "cross";
        let max_hedge_price_pct_change = match hash_map.get("max_hedge_price_pct_change") {
            Some(raw) => {
                let parsed = raw.parse::<f64>().unwrap_or_else(|_| {
                    panic!(
                        "Redis hash '{}' max_hedge_price_pct_change 无法解析: {}",
                        redis_key, raw
                    )
                });
                if !(parsed.is_finite() && parsed > 0.0 && parsed <= 99.0) {
                    panic!(
                        "Redis hash '{}' max_hedge_price_pct_change 无效(需在(0,99]): {}",
                        redis_key, parsed
                    );
                }
                parsed
            }
            None => {
                if require_max_hedge_pct {
                    panic!("Redis hash '{}' 缺少 max_hedge_price_pct_change", redis_key);
                }
                default_max_hedge_price_pct_change()
            }
        };

        let signal_cooldown = hash_map
            .get("signal_cooldown")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(default_signal_cooldown);
        let enable_return_score_cancel = hash_map
            .get("enable_return_score_cancel")
            .map(|raw| parse_bool_param(&redis_key, "enable_return_score_cancel", raw))
            .unwrap_or_else(default_enable_return_score_cancel);
        let return_score_buy_cancel_quantile = hash_map
            .get("return_score_buy_cancel_quantile")
            .map(|raw| {
                parse_percentile_exclusive(&redis_key, "return_score_buy_cancel_quantile", raw)
            })
            .unwrap_or_else(default_return_score_buy_cancel_quantile);
        let return_score_sell_cancel_quantile = hash_map
            .get("return_score_sell_cancel_quantile")
            .map(|raw| {
                parse_percentile_exclusive(&redis_key, "return_score_sell_cancel_quantile", raw)
            })
            .unwrap_or_else(default_return_score_sell_cancel_quantile);
        let enable_tlen_cancel = match hash_map.get("enable_tlen_cancel") {
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" | "" => false,
                _ => {
                    panic!(
                        "Redis hash '{}' enable_tlen_cancel 非法（仅支持 true/false）: {}",
                        redis_key, raw
                    )
                }
            },
            None => default_enable_tlen_cancel(),
        };
        let tlen_cancel_freq_ms = match hash_map.get("tlen_cancel_freq_ms") {
            Some(raw) => {
                let parsed = raw.parse::<i64>().unwrap_or_else(|_| {
                    panic!(
                        "Redis hash '{}' tlen_cancel_freq_ms 无法解析为整数: {}",
                        redis_key, raw
                    )
                });
                if parsed <= 0 {
                    panic!(
                        "Redis hash '{}' tlen_cancel_freq_ms 无效(需>0): {}",
                        redis_key, parsed
                    );
                }
                parsed as u64
            }
            None => default_tlen_cancel_freq_ms(),
        };
        let spread_cancel_cooldown_ms = match hash_map.get("spread_cancel_cooldown_ms") {
            Some(raw) => {
                let parsed = raw.parse::<i64>().unwrap_or_else(|_| {
                    panic!(
                        "Redis hash '{}' spread_cancel_cooldown_ms 无法解析为整数: {}",
                        redis_key, raw
                    )
                });
                if parsed < 0 {
                    panic!(
                        "Redis hash '{}' spread_cancel_cooldown_ms 无效(需>=0): {}",
                        redis_key, parsed
                    );
                }
                parsed as u64
            }
            None => default_spread_cancel_cooldown_ms(),
        };
        let enable_return_score_adjust_hedge = match hash_map
            .get("enable_return_score_adjust_hegde")
            .or_else(|| hash_map.get("enable_return_score_adjust_hedge"))
        {
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" | "" => false,
                _ => {
                    panic!(
                        "Redis hash '{}' enable_return_score_adjust_hegde 非法（仅支持 true/false）: {}",
                        redis_key, raw
                    )
                }
            },
            None => default_enable_return_score_adjust_hedge(),
        };
        let enable_environment_model = match hash_map.get("enable_environment_model") {
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" | "" => false,
                _ => {
                    panic!(
                        "Redis hash '{}' enable_environment_model 非法（仅支持 true/false）: {}",
                        redis_key, raw
                    )
                }
            },
            None => default_enable_environment_model(),
        };
        let enable_volatility_limit = match hash_map.get("enable_volatility_limit") {
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" | "" => false,
                _ => {
                    panic!(
                        "Redis hash '{}' enable_volatility_limit 非法（仅支持 true/false）: {}",
                        redis_key, raw
                    )
                }
            },
            None => default_enable_volatility_limit(),
        };
        let open_volatility_limit = hash_map
            .get("open_volatility_limit")
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 100.0)
            .unwrap_or_else(default_open_volatility_limit);
        let enable_tradecount_limit = match hash_map.get("enable_tradecount_limit") {
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" | "" => false,
                _ => {
                    panic!(
                        "Redis hash '{}' enable_tradecount_limit 非法（仅支持 true/false）: {}",
                        redis_key, raw
                    )
                }
            },
            None => default_enable_tradecount_limit(),
        };
        let open_tradecount_limit = hash_map
            .get("open_tradecount_limit")
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 100.0)
            .unwrap_or_else(default_open_tradecount_limit);
        let enable_open_time_block = hash_map
            .get("enable_open_time_block")
            .map(|raw| parse_bool_param(&redis_key, "enable_open_time_block", raw))
            .unwrap_or_else(default_enable_open_time_block);
        let open_block_utc_time_range = hash_map
            .get("open_block_utc_time_range")
            .map(|raw| validate_open_block_utc_time_range(raw))
            .unwrap_or_else(default_open_block_utc_time_range);

        let strict_return_model_required = ns == "mm";
        let strict_env_model_dash_only = false;
        let allow_missing_model_service = ns == "fr";
        let return_model_service = match hash_map.get("return_model_service") {
            Some(v) => {
                let trimmed = v.trim();
                if trimmed.is_empty() {
                    if strict_return_model_required {
                        panic!(
                            "Redis hash '{}' return_model_service 为空（ns={}），不允许为空或 '-'",
                            redis_key, ns
                        );
                    } else if allow_missing_model_service {
                        warn!(
                            "Redis hash '{}' return_model_service 为空（ns={}），按 '-' 处理",
                            redis_key, ns
                        );
                        "-".to_string()
                    } else {
                        panic!(
                            "Redis hash '{}' return_model_service 为空，需显式配置通道名或 '-'",
                            redis_key
                        );
                    }
                } else if strict_return_model_required && trimmed == "-" {
                    panic!(
                        "Redis hash '{}' return_model_service='-' 非法（ns={}），仅允许 environment_model_service='-'",
                        redis_key, ns
                    );
                } else {
                    trimmed.to_string()
                }
            }
            None => {
                if strict_return_model_required {
                    panic!(
                        "Redis hash '{}' 缺少 return_model_service（ns={}），不允许缺失或 '-'",
                        redis_key, ns
                    );
                } else if allow_missing_model_service {
                    warn!(
                        "Redis hash '{}' 缺少 return_model_service（ns={}），按 '-' 处理",
                        redis_key, ns
                    );
                    "-".to_string()
                } else {
                    panic!(
                        "Redis hash '{}' 缺少 return_model_service，需显式配置通道名或 '-'",
                        redis_key
                    );
                }
            }
        };

        let environment_model_service = match hash_map.get("environment_model_service") {
            Some(v) => {
                let trimmed = v.trim();
                if strict_env_model_dash_only {
                    if trimmed != "-" {
                        panic!(
                            "Redis hash '{}' environment_model_service='{}' 非法（ns={}），必须为 '-'",
                            redis_key, trimmed, ns
                        );
                    }
                    "-".to_string()
                } else if trimmed.is_empty() {
                    if allow_missing_model_service {
                        warn!(
                            "Redis hash '{}' environment_model_service 为空（ns={}），按 '-' 处理",
                            redis_key, ns
                        );
                        "-".to_string()
                    } else {
                        panic!(
                            "Redis hash '{}' environment_model_service 为空，需显式配置通道名或 '-'",
                            redis_key
                        );
                    }
                } else {
                    trimmed.to_string()
                }
            }
            None => {
                if strict_env_model_dash_only {
                    panic!(
                        "Redis hash '{}' 缺少 environment_model_service（ns={}），必须显式为 '-'",
                        redis_key, ns
                    );
                } else if allow_missing_model_service {
                    warn!(
                        "Redis hash '{}' 缺少 environment_model_service（ns={}），按 '-' 处理",
                        redis_key, ns
                    );
                    "-".to_string()
                } else {
                    panic!(
                        "Redis hash '{}' 缺少 environment_model_service，需显式配置通道名或 '-'",
                        redis_key
                    );
                }
            }
        };
        Ok(Self {
            order_amount,
            mm_amount_u_overrides,
            mm_open_offset_lower_overrides,
            mm_hedge_price_offset_limit_upper_overrides,
            mm_hedge_price_offset_limit_lower_overrides,
            arb_amount_u_overrides,
            arb_hedge_price_offset_limit_upper_overrides,
            arb_hedge_price_offset_limit_lower_overrides,
            arb_open_offset_lower_overrides,
            vol_band_scale,
            open_buy_vol_scale,
            open_sell_vol_scale,
            order_interval_ms,
            clock_shift_ms,
            open_orders_per_round,
            hedge_orders_per_round,
            open_order_timeout,
            next_query_delay_ms,
            hedge_vol_multiplier,
            hedge_offset_ratio,
            hedge_price_offset_limit_upper,
            hedge_price_offset_limit_lower,
            hedge_window_scale_low,
            hedge_window_scale_high,
            hedge_timeout,
            hedge_aggressive_seq_threshold,
            max_hedge_price_pct_change,
            signal_cooldown,
            enable_return_score_cancel,
            return_score_buy_cancel_quantile,
            return_score_sell_cancel_quantile,
            enable_tlen_cancel,
            tlen_cancel_freq_ms,
            spread_cancel_cooldown_ms,
            enable_return_score_adjust_hedge,
            enable_environment_model,
            enable_volatility_limit,
            open_volatility_limit,
            enable_tradecount_limit,
            open_tradecount_limit,
            enable_open_time_block,
            open_block_utc_time_range,
            return_model_service,
            environment_model_service,
        })
    }

    fn parse_required_vol_scale_range(&self, raw: &str, field_name: &str) -> [f64; 2] {
        let values = serde_json::from_str::<Vec<f64>>(raw).unwrap_or_else(|err| {
            panic!(
                "{} 必须是长度为2的 JSON 数组，例如 [0.2,0.8]；当前值='{}' err={}",
                field_name, raw, err
            )
        });
        if values.len() != 2 {
            panic!(
                "{} 必须是长度为2的 JSON 数组，例如 [0.2,0.8]；当前值='{}'",
                field_name, raw
            );
        }
        let low = values[0];
        let high = values[1];
        if !low.is_finite() || !high.is_finite() || low < 0.0 || high < low {
            panic!(
                "{} 必须满足 0<=low<=high；当前值='{}' 解析后=[{}, {}]",
                field_name, raw, low, high
            );
        }
        [low, high]
    }

    fn parse_model_output_services(&self) -> Vec<String> {
        let mut services = Vec::new();
        for raw in [&self.return_model_service, &self.environment_model_service] {
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed == "-" {
                continue;
            }
            services.push(trimmed.to_string());
        }
        services
    }

    /// 应用参数到所有单例
    pub(crate) fn apply(&self) {
        // 1. 更新 ArbDecision / MmDecision
        let return_trimmed = self.return_model_service.trim();
        let return_model_service = if return_trimmed.is_empty() || return_trimmed == "-" {
            None
        } else {
            Some(return_trimmed.to_string())
        };
        let env_trimmed = self.environment_model_service.trim();
        let environment_model_service = if env_trimmed.is_empty() || env_trimmed == "-" {
            None
        } else {
            Some(env_trimmed.to_string())
        };

        let arb_vol_band_scale =
            self.parse_required_vol_scale_range(&self.vol_band_scale, "vol_band_scale");
        let arb_state_applied = ArbDecision::with_state_mut(|arb| {
            arb.order_amount = self.order_amount;
            arb.amount_u_overrides = self.arb_amount_u_overrides.clone();
            arb.open_offset_lower_overrides = self.arb_open_offset_lower_overrides.clone();
            arb.vol_band_scale = arb_vol_band_scale;
            arb.open_orders_per_round = self.open_orders_per_round;
            arb.open_order_ttl_us = self
                .open_order_timeout
                .saturating_mul(1_000_000)
                .min(i64::MAX as u64) as i64;
            arb.hedge_timeout_mm_us = self
                .hedge_timeout
                .saturating_mul(1_000_000)
                .min(i64::MAX as u64) as i64;
            arb.hedge_vol_multiplier = self.hedge_vol_multiplier;
            arb.hedge_offset_ratio = self.hedge_offset_ratio;
            arb.hedge_price_offset_limit_lower = self.hedge_price_offset_limit_lower;
            arb.hedge_price_offset_limit_upper = self.hedge_price_offset_limit_upper;
            arb.update_hedge_price_offset_limit_overrides(
                self.arb_hedge_price_offset_limit_lower_overrides.clone(),
                self.arb_hedge_price_offset_limit_upper_overrides.clone(),
            );
            arb.hedge_window_scale_low = self.hedge_window_scale_low;
            arb.hedge_window_scale_high = self.hedge_window_scale_high;
            arb.enable_return_score_adjust_hedge = self.enable_return_score_adjust_hedge;
            arb.hedge_aggressive_seq_threshold = self.hedge_aggressive_seq_threshold;
            arb.enable_tlen_cancel = self.enable_tlen_cancel;
            arb.tlen_cancel_freq_ms = self.tlen_cancel_freq_ms;
            arb.signal_cooldown_us = self
                .signal_cooldown
                .saturating_mul(1_000_000)
                .min(i64::MAX as u64) as i64;
            arb.spread_cancel_cooldown_us = self
                .spread_cancel_cooldown_ms
                .saturating_mul(1_000)
                .min(i64::MAX as u64) as i64;

            arb.max_hedge_price_pct_change = self.max_hedge_price_pct_change;
            arb.enable_environment_model = self.enable_environment_model;
            arb.update_enable_volatility_limit(self.enable_volatility_limit);
            arb.update_open_volatility_limit(self.open_volatility_limit);
            arb.return_model_service = return_model_service.clone();
            arb.environment_model_service = environment_model_service.clone();
            arb.environment_model_true_threshold = 0.0;
        })
        .is_some();
        // 旧版用 || 短路把 model_output 服务订阅串在 arb state 写入之后；ArbDecision 已初始化时
        // 第一项总返回 true，订阅这一步永远跑不到，导致 hedge 模型订阅从未建立。这里拆开顺序执行。
        if arb_state_applied {
            ArbDecision::try_update_model_output_services(self.parse_model_output_services());
        }
        let mm_applied = MmDecision::try_with_mut(|_decision| {
            let open_buy_vol_scale =
                self.parse_required_vol_scale_range(&self.open_buy_vol_scale, "open_buy_vol_scale");
            let open_sell_vol_scale = self
                .parse_required_vol_scale_range(&self.open_sell_vol_scale, "open_sell_vol_scale");
            _decision.update_order_amount(self.order_amount);
            _decision.update_order_amount_overrides(self.mm_amount_u_overrides.clone());
            _decision
                .update_open_offset_lower_overrides(self.mm_open_offset_lower_overrides.clone());
            _decision.update_clock_timing_params(
                self.order_interval_ms,
                self.next_query_delay_ms,
                self.clock_shift_ms,
            );
            _decision.update_open_orders_per_round(self.open_orders_per_round);
            _decision.update_open_vol_scale_ranges(open_buy_vol_scale, open_sell_vol_scale);
            _decision.update_enable_tlen_cancel(self.enable_tlen_cancel);
            _decision.update_tlen_cancel_freq_ms(self.tlen_cancel_freq_ms);
            _decision.update_mm_hedge_params(
                self.hedge_orders_per_round,
                self.hedge_vol_multiplier,
                self.hedge_offset_ratio,
                self.hedge_price_offset_limit_lower,
                self.hedge_price_offset_limit_upper,
                self.hedge_window_scale_low,
                self.hedge_window_scale_high,
                self.max_hedge_price_pct_change,
                self.next_query_delay_ms,
                self.enable_return_score_adjust_hedge,
            );
            _decision.update_hedge_price_offset_limit_overrides(
                self.mm_hedge_price_offset_limit_lower_overrides.clone(),
                self.mm_hedge_price_offset_limit_upper_overrides.clone(),
            );
            _decision.update_open_order_timeout(self.open_order_timeout);
            _decision.update_return_score_cancel_params(
                self.enable_return_score_cancel,
                self.return_score_buy_cancel_quantile,
                self.return_score_sell_cancel_quantile,
            );
            _decision.update_enable_environment_model(self.enable_environment_model);
            _decision.update_enable_volatility_limit(self.enable_volatility_limit);
            _decision.update_open_volatility_limit(self.open_volatility_limit);
            _decision.update_enable_tradecount_limit(self.enable_tradecount_limit);
            _decision.update_open_tradecount_limit(self.open_tradecount_limit);
            _decision.update_open_time_block(
                self.enable_open_time_block,
                &self.open_block_utc_time_range,
            );
            _decision.update_model_service_roles(
                self.return_model_service.clone(),
                self.environment_model_service.clone(),
            );
        })
        .is_some();

        if !arb_state_applied && !mm_applied {
            warn!("策略参数已加载，但 decision 尚未初始化");
        }

        info!(
            "✅ overrides applied: arb_amount_u_symbols={} arb_hedge_offset_lower_symbols={} arb_hedge_offset_upper_symbols={} mm_amount_u_symbols={} mm_open_offset_lower_symbols={} mm_hedge_offset_lower_symbols={} mm_hedge_offset_upper_symbols={}",
            self.arb_amount_u_overrides.len(),
            self.arb_hedge_price_offset_limit_lower_overrides.len(),
            self.arb_hedge_price_offset_limit_upper_overrides.len(),
            self.mm_amount_u_overrides.len(),
            self.mm_open_offset_lower_overrides.len(),
            self.mm_hedge_price_offset_limit_lower_overrides.len(),
            self.mm_hedge_price_offset_limit_upper_overrides.len(),
        );

        info!(
            "✅ 策略参数已更新: amount={:.2}, arb_vol_band_scale={}, mm_open_buy_vol_scale={}, mm_open_sell_vol_scale={}, hedge_window_scale_low={:.4}, hedge_window_scale_high={:.4}, order_interval_ms={}, enable_clock_shift_ms={}, open_orders_per_round={}, cooldown={}s, enable_return_score_cancel={}, return_score_buy_cancel_quantile={}, return_score_sell_cancel_quantile={}, enable_tlen_cancel={}, tlen_cancel_freq_ms={}, spread_cancel_cooldown_ms={}, enable_return_score_adjust_hedge={}, enable_environment_model={}, enable_volatility_limit={}, open_volatility_limit={}, enable_tradecount_limit={}, open_tradecount_limit={}, enable_open_time_block={}, open_block_utc_time_range={}, return_model_service={}, environment_model_service={}",
            self.order_amount,
            self.vol_band_scale,
            self.open_buy_vol_scale,
            self.open_sell_vol_scale,
            self.hedge_window_scale_low,
            self.hedge_window_scale_high,
            self.order_interval_ms,
            self.clock_shift_ms,
            self.open_orders_per_round,
            self.signal_cooldown,
            self.enable_return_score_cancel,
            self.return_score_buy_cancel_quantile,
            self.return_score_sell_cancel_quantile,
            self.enable_tlen_cancel,
            self.tlen_cancel_freq_ms,
            self.spread_cancel_cooldown_ms,
            self.enable_return_score_adjust_hedge,
            self.enable_environment_model,
            self.enable_volatility_limit,
            self.open_volatility_limit,
            self.enable_tradecount_limit,
            self.open_tradecount_limit,
            self.enable_open_time_block,
            self.open_block_utc_time_range,
            self.return_model_service,
            self.environment_model_service
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enable_return_score_cancel_default_is_false() {
        let params = StrategyParams::default();
        assert!(!params.enable_return_score_cancel);
    }

    #[test]
    fn test_return_score_cancel_quantile_defaults() {
        let params = StrategyParams::default();
        assert!((params.return_score_buy_cancel_quantile - 90.0).abs() < 1e-12);
        assert!((params.return_score_sell_cancel_quantile - 10.0).abs() < 1e-12);
    }

    #[test]
    fn test_tlen_cancel_freq_ms_default_is_3000() {
        let params = StrategyParams::default();
        assert_eq!(params.tlen_cancel_freq_ms, 3_000);
    }

    #[test]
    fn test_enable_tlen_cancel_default_is_false() {
        let params = StrategyParams::default();
        assert!(!params.enable_tlen_cancel);
    }

    #[test]
    fn test_enable_return_score_adjust_hedge_default_is_true() {
        let params = StrategyParams::default();
        assert!(params.enable_return_score_adjust_hedge);
    }

    #[test]
    fn test_enable_environment_model_default_is_true() {
        let params = StrategyParams::default();
        assert!(params.enable_environment_model);
    }

    #[test]
    fn test_enable_volatility_limit_default_is_true() {
        let params = StrategyParams::default();
        assert!(params.enable_volatility_limit);
    }

    #[test]
    fn test_open_volatility_limit_default_is_70() {
        let params = StrategyParams::default();
        assert!((params.open_volatility_limit - 70.0).abs() < 1e-12);
    }

    #[test]
    fn test_enable_tradecount_limit_default_is_false() {
        let params = StrategyParams::default();
        assert!(!params.enable_tradecount_limit);
    }

    #[test]
    fn test_open_tradecount_limit_default_is_70() {
        let params = StrategyParams::default();
        assert!((params.open_tradecount_limit - 70.0).abs() < 1e-12);
    }

    #[test]
    fn test_open_time_block_defaults_are_disabled_with_valid_range() {
        let params = StrategyParams::default();
        assert!(!params.enable_open_time_block);
        assert_eq!(params.open_block_utc_time_range, "00:00-00:01");
        assert_eq!(
            validate_open_block_utc_time_range(&params.open_block_utc_time_range),
            "00:00-00:01"
        );
    }

    #[test]
    fn test_open_time_block_validation_allows_cross_day() {
        assert_eq!(
            validate_open_block_utc_time_range("23:00-01:00"),
            "23:00-01:00"
        );
    }

    #[test]
    #[should_panic(expected = "begin/end 不能相同")]
    fn test_open_time_block_validation_rejects_equal_times() {
        let _ = validate_open_block_utc_time_range("12:00-12:00");
    }

    #[test]
    #[should_panic(expected = "必须为 UTC HH:MM")]
    fn test_open_time_block_validation_rejects_malformed_times() {
        let _ = validate_open_block_utc_time_range("7:00-08:00");
    }

    #[test]
    fn test_mm_strategy_params_key_is_venue_level() {
        let key = mm_strategy_params_key(TradingVenue::BinanceFutures);
        assert_eq!(key, "mm_strategy_params_binance-futures");
    }

    #[test]
    fn test_mm_order_amount_field_name_is_default_order_amount() {
        assert_eq!(order_amount_field_name("mm"), "default_order_amount");
        assert_eq!(order_amount_field_name("fr"), "order_amount");
    }

    #[test]
    fn test_mm_amount_u_override_key_includes_env_and_venue() {
        let key =
            mm_amount_u_override_key_for_env(Some("binance_mm_beta"), TradingVenue::BinanceFutures);
        assert_eq!(key, "binance_mm_beta:binance-futures:mm:amount_u");
    }

    #[test]
    fn test_mm_open_offset_lower_override_key_includes_env_and_venue() {
        let okex_key = mm_open_offset_lower_override_key_for_env(
            Some("okex_mm_alpha"),
            TradingVenue::OkexFutures,
        );
        let bybit_key = mm_open_offset_lower_override_key_for_env(
            Some("bybit_mm_alpha"),
            TradingVenue::BybitFutures,
        );
        assert_eq!(okex_key, "okex_mm_alpha:okex-futures:mm:open_offset_lower");
        assert_eq!(
            bybit_key,
            "bybit_mm_alpha:bybit-futures:mm:open_offset_lower"
        );
    }

    #[test]
    fn test_mm_hedge_price_offset_limit_keys_include_env_and_venue() {
        let limits_key = mm_hedge_price_offset_limits_override_key_for_env(
            Some("binance_mm_beta"),
            TradingVenue::BinanceFutures,
        );
        let upper_key = mm_hedge_price_offset_limit_upper_override_key_for_env(
            Some("binance_mm_beta"),
            TradingVenue::BinanceFutures,
        );
        let lower_key = mm_hedge_price_offset_limit_lower_override_key_for_env(
            Some("binance_mm_beta"),
            TradingVenue::BinanceFutures,
        );
        assert_eq!(
            limits_key,
            "binance_mm_beta:binance-futures:mm:hedge_price_offset_limits"
        );
        assert_eq!(
            upper_key,
            "binance_mm_beta:binance-futures:mm:hedge_price_offset_limit_upper"
        );
        assert_eq!(
            lower_key,
            "binance_mm_beta:binance-futures:mm:hedge_price_offset_limit_lower"
        );
    }

    #[test]
    fn test_parse_mm_amount_u_overrides_normalizes_symbols() {
        let overrides = parse_mm_amount_u_overrides(
            r#"{"btc-usdt":150,"ETH_USDT":80}"#,
            TradingVenue::BinanceMargin,
            "binance_mm_beta:binance-futures:mm:amount_u",
        );
        assert_eq!(overrides.get("BTCUSDT"), Some(&150.0));
        assert_eq!(overrides.get("ETHUSDT"), Some(&80.0));
    }

    #[test]
    fn test_parse_mm_open_offset_lower_overrides_normalizes_symbols_and_allows_zero() {
        let overrides = parse_mm_open_offset_lower_overrides(
            r#"{"btc-usdt":0,"ETH_USDT":0.0008}"#,
            TradingVenue::OkexFutures,
            "okex_mm_alpha:okex-futures:mm:open_offset_lower",
        );
        assert_eq!(overrides.get("BTC-USDT-SWAP"), Some(&0.0));
        assert_eq!(overrides.get("ETH-USDT-SWAP"), Some(&0.0008));
    }

    #[test]
    #[should_panic(expected = "open_offset_lower 非法")]
    fn test_parse_mm_open_offset_lower_overrides_rejects_negative() {
        let _ = parse_mm_open_offset_lower_overrides(
            r#"{"BTCUSDT":-0.0001}"#,
            TradingVenue::BybitFutures,
            "bybit_mm_alpha:bybit-futures:mm:open_offset_lower",
        );
    }

    #[test]
    #[should_panic]
    fn test_parse_mm_open_offset_lower_overrides_rejects_nan_or_non_finite() {
        let _ = parse_mm_open_offset_lower_overrides(
            r#"{"BTCUSDT":1e999}"#,
            TradingVenue::BybitFutures,
            "bybit_mm_alpha:bybit-futures:mm:open_offset_lower",
        );
    }

    #[test]
    fn test_arb_hedge_price_offset_limit_keys_include_env_and_venue_pair() {
        let limits_key = arb_hedge_price_offset_limits_override_key(
            Some("okex-intra-arb01"),
            TradingVenue::OkexMargin,
            TradingVenue::OkexFutures,
        )
        .unwrap();
        let upper_key = arb_hedge_price_offset_limit_upper_override_key(
            Some("okex-intra-arb01"),
            TradingVenue::OkexMargin,
            TradingVenue::OkexFutures,
        )
        .unwrap();
        let lower_key = arb_hedge_price_offset_limit_lower_override_key(
            Some("okex-intra-arb01"),
            TradingVenue::OkexMargin,
            TradingVenue::OkexFutures,
        )
        .unwrap();
        assert_eq!(
            limits_key,
            "okex-intra-arb01:okex-margin:okex-futures:hedge_price_offset_limits"
        );
        assert_eq!(
            upper_key,
            "okex-intra-arb01:okex-margin:okex-futures:hedge_price_offset_limit_upper"
        );
        assert_eq!(
            lower_key,
            "okex-intra-arb01:okex-margin:okex-futures:hedge_price_offset_limit_lower"
        );
    }

    #[test]
    fn test_arb_hedge_price_offset_limit_keys_skip_when_env_missing() {
        assert!(arb_hedge_price_offset_limits_override_key(
            None,
            TradingVenue::OkexMargin,
            TradingVenue::OkexFutures,
        )
        .is_none());
        assert!(arb_hedge_price_offset_limit_upper_override_key(
            Some("   "),
            TradingVenue::OkexMargin,
            TradingVenue::OkexFutures,
        )
        .is_none());
    }

    #[test]
    fn test_parse_mm_hedge_price_offset_limit_overrides_normalizes_symbols() {
        let (lower_overrides, upper_overrides) = parse_mm_hedge_price_offset_limit_overrides(
            r#"{"btc-usdt":{"hedge_price_offset_limit_lower":0.0005,"hedge_price_offset_limit_upper":0.005},"ETH_USDT":{"hedge_price_offset_limit_lower":0.0004,"hedge_price_offset_limit_upper":0.004}}"#,
            TradingVenue::BinanceMargin,
            "binance_mm_beta:binance-futures:mm:hedge_price_offset_limits",
        );
        assert_eq!(lower_overrides.get("BTCUSDT"), Some(&0.0005));
        assert_eq!(upper_overrides.get("BTCUSDT"), Some(&0.005));
        assert_eq!(lower_overrides.get("ETHUSDT"), Some(&0.0004));
        assert_eq!(upper_overrides.get("ETHUSDT"), Some(&0.004));
    }

    #[test]
    #[should_panic(expected = "hedge_price_offset_limit 非法")]
    fn test_parse_mm_hedge_price_offset_limit_overrides_rejects_invalid_range() {
        let _ = parse_mm_hedge_price_offset_limit_overrides(
            r#"{"BTCUSDT":{"hedge_price_offset_limit_lower":0.0005,"hedge_price_offset_limit_upper":0.0004}}"#,
            TradingVenue::BinanceMargin,
            "binance_mm_beta:binance-futures:mm:hedge_price_offset_limits",
        );
    }

    #[test]
    #[should_panic(expected = "amount_u 非法")]
    fn test_parse_mm_amount_u_overrides_rejects_invalid_amount() {
        let _ = parse_mm_amount_u_overrides(
            r#"{"BTCUSDT":0}"#,
            TradingVenue::BinanceMargin,
            "binance_mm_beta:binance-futures:mm:amount_u",
        );
    }

    #[test]
    #[should_panic(expected = "包含空 symbol")]
    fn test_parse_mm_amount_u_overrides_rejects_empty_symbol() {
        let _ = parse_mm_amount_u_overrides(
            r#"{"   ":100}"#,
            TradingVenue::BinanceMargin,
            "binance_mm_beta:binance-futures:mm:amount_u",
        );
    }

    #[test]
    fn test_mm_strategy_params_key_without_env_name_requirement() {
        let key = mm_strategy_params_key(TradingVenue::BinanceFutures);
        assert_eq!(key, "mm_strategy_params_binance-futures");
    }
}
