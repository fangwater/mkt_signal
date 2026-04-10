//! 策略参数定义
//!
//! 定义策略参数结构及其 Redis 加载逻辑：
//! - ArbDecision: 套利参数（订单量、超时、偏移、冷却时间等）
//! - SpreadFactor: MM/MT 模式

use anyhow::Result;
use log::{info, warn};
use serde::Deserialize;
use std::collections::HashMap;

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::common::symbol_util::normalize_symbol_for_venue;

use super::arb_decision::ArbDecision;
use super::common::FactorMode;
use super::mm_decision::MmDecision;
use super::spread_factor::SpreadFactor;
use crate::signal::common::TradingVenue;

/// Redis Key 配置
const DEFAULT_NAMESPACE: &str = "fr";

fn normalize_namespace(namespace: &str) -> String {
    let ns = namespace
        .trim()
        .trim_end_matches(|c: char| c == '_' || c == '-' || c == ':')
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

pub fn mm_strategy_params_key_for_env(env_name: Option<&str>, hedge_venue: TradingVenue) -> String {
    let base_key = format!("mm_strategy_params_{}", hedge_venue.data_pub_slug());
    let env_name = normalize_mm_env_name(env_name);
    format!("{env_name}:{base_key}")
}

pub fn mm_amount_u_override_key_for_env(
    env_name: Option<&str>,
    hedge_venue: TradingVenue,
) -> String {
    let env_name = normalize_mm_env_name(env_name);
    format!("{env_name}:{}:mm:amount_u", hedge_venue.data_pub_slug())
}

fn parse_mm_amount_u_overrides(
    raw: &str,
    open_venue: TradingVenue,
    redis_key: &str,
) -> HashMap<String, f64> {
    let parsed: HashMap<String, f64> = serde_json::from_str(raw).unwrap_or_else(|err| {
        panic!(
            "Redis string '{}' 不是合法 JSON(symbol->amount_u): {} ({})",
            redis_key, raw, err
        )
    });

    let mut normalized = HashMap::new();
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
        let symbol_key = normalize_symbol_for_venue(&normalized_input, open_venue);
        normalized.insert(symbol_key, amount_u);
    }
    normalized
}

fn strategy_params_key(
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> String {
    let ns = normalize_namespace(namespace);
    if ns == "mm" {
        return mm_strategy_params_key_for_env(
            Some(infer_mm_env_name_from_runtime().as_str()),
            hedge_venue,
        );
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
    /// 做市模式：MM（Maker-Maker）或 MT（Maker-Taker）
    #[serde(default = "default_mode")]
    pub mode: String,

    /// 单笔下单量（USDT）
    #[serde(default = "default_order_amount")]
    pub order_amount: f32,

    /// MM 按 symbol 覆盖的下单量（USDT）
    #[serde(default)]
    pub mm_amount_u_overrides: HashMap<String, f64>,

    /// arb 开仓 plan 的波动边界缩放系数
    #[serde(default = "default_open_scale")]
    pub open_scale: f64,

    /// MM open 买侧波动区间缩放系数 [low, high]
    #[serde(default = "default_open_buy_vol_scale")]
    pub open_buy_vol_scale: String,

    /// MM open 卖侧波动区间缩放系数 [low, high]
    #[serde(default = "default_open_sell_vol_scale")]
    pub open_sell_vol_scale: String,

    /// MM 报单触发间隔（毫秒）
    #[serde(default = "default_order_interval_ms")]
    pub order_interval_ms: u64,

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

    /// 对冲订单超时（秒）
    #[serde(default = "default_hedge_timeout")]
    pub hedge_timeout: u64,

    /// 对冲价格偏移
    #[serde(default = "default_hedge_price_offset")]
    pub hedge_price_offset: f64,

    /// 对冲 request_seq 激进阈值（>=该值不偏移但仍挂 maker）
    #[serde(default = "default_hedge_aggressive_seq_threshold")]
    pub hedge_aggressive_seq_threshold: u32,

    /// 对冲触发 taker 的最大价格变动比例（百分比，允许小数，范围 (0, 99]）
    #[serde(default = "default_max_hedge_price_pct_change")]
    pub max_hedge_price_pct_change: f64,

    /// 信号冷却时间（秒）
    #[serde(default = "default_signal_cooldown")]
    pub signal_cooldown: u64,

    /// MM 是否启用方向预测（true=按 return score 过滤单边，false=双边同时报价）
    #[serde(default = "default_prediction_mode")]
    pub prediction_mode: bool,

    /// MM 是否启用开仓撤单判断（false=跳过 MMCancel）
    #[serde(default = "default_enable_open_cancel")]
    pub enable_open_cancel: bool,

    /// MM 是否启用基于 tlen 的开仓撤单链路（trigger/query/cancel）
    #[serde(default = "default_enable_tlen_cancel")]
    pub enable_tlen_cancel: bool,

    /// MM 开仓撤单触发频率限制（毫秒）
    #[serde(default = "default_tlen_cancel_freq_ms")]
    pub tlen_cancel_freq_ms: u64,

    /// Spread cancel 信号限流（毫秒），仅作用于 xarb spread cancel
    #[serde(default = "default_spread_cancel_cooldown_ms")]
    pub spread_cancel_cooldown_ms: u64,

    /// MM hedge 是否允许 return score 调整 hedge offset（false=使用中性 score 计算）
    #[serde(default = "default_enable_return_score_adjust_hedge")]
    pub enable_return_score_adjust_hedge: bool,

    /// 是否启用 env / pnlu 开仓限制（false=只读取并写入 from_key，不阻拦开仓）
    #[serde(default = "default_enable_environment_model")]
    pub enable_environment_model: bool,

    /// 是否启用波动率限制下单（预留开关，当前先只做配置透传）
    #[serde(default = "default_enable_volatility_limit")]
    pub enable_volatility_limit: bool,

    /// 波动率限制分位数（读取 rolling_metrics 的 open_vol_xx）
    #[serde(default = "default_open_volatility_limit")]
    pub open_volatility_limit: f64,

    /// 收益率模型输出通道（"-" 表示禁用）
    #[serde(default = "default_return_model_service")]
    pub return_model_service: String,

    /// 环境模型输出通道（"-" 表示禁用）
    #[serde(default = "default_environment_model_service")]
    pub environment_model_service: String,
}

// 默认值函数
fn default_mode() -> String {
    "MM".to_string()
}
fn default_order_amount() -> f32 {
    100.0
}
fn default_open_scale() -> f64 {
    1.0
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
fn default_hedge_timeout() -> u64 {
    30
}
fn default_hedge_price_offset() -> f64 {
    0.0003
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
fn default_prediction_mode() -> bool {
    false
}
fn default_enable_open_cancel() -> bool {
    false
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
fn default_return_model_service() -> String {
    "return_model".to_string()
}
fn default_environment_model_service() -> String {
    "environment_model".to_string()
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            order_amount: default_order_amount(),
            mm_amount_u_overrides: HashMap::new(),
            open_scale: default_open_scale(),
            open_buy_vol_scale: default_open_buy_vol_scale(),
            open_sell_vol_scale: default_open_sell_vol_scale(),
            order_interval_ms: default_order_interval_ms(),
            open_orders_per_round: default_open_orders_per_round(),
            hedge_orders_per_round: default_hedge_orders_per_round(),
            open_order_timeout: default_open_order_timeout(),
            next_query_delay_ms: default_next_query_delay_ms(),
            hedge_vol_multiplier: default_hedge_vol_multiplier(),
            hedge_offset_ratio: default_hedge_offset_ratio(),
            hedge_price_offset_limit_upper: default_hedge_price_offset_limit_upper(),
            hedge_price_offset_limit_lower: default_hedge_price_offset_limit_lower(),
            hedge_timeout: default_hedge_timeout(),
            hedge_price_offset: default_hedge_price_offset(),
            hedge_aggressive_seq_threshold: default_hedge_aggressive_seq_threshold(),
            max_hedge_price_pct_change: default_max_hedge_price_pct_change(),
            signal_cooldown: default_signal_cooldown(),
            prediction_mode: default_prediction_mode(),
            enable_open_cancel: default_enable_open_cancel(),
            enable_tlen_cancel: default_enable_tlen_cancel(),
            tlen_cancel_freq_ms: default_tlen_cancel_freq_ms(),
            spread_cancel_cooldown_ms: default_spread_cancel_cooldown_ms(),
            enable_return_score_adjust_hedge: default_enable_return_score_adjust_hedge(),
            enable_environment_model: default_enable_environment_model(),
            enable_volatility_limit: default_enable_volatility_limit(),
            open_volatility_limit: default_open_volatility_limit(),
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
            mm_strategy_params_key_for_env(mm_env_name.as_deref(), hedge_venue)
        } else {
            strategy_params_key(&ns, open_venue, hedge_venue)
        };
        let hash_map = client.hgetall_map(&redis_key).await?;
        if hash_map.is_empty() {
            panic!("Redis hash '{}' 为空或不存在，无法加载策略参数", redis_key);
        }

        // 手动解析 Hash 字段
        let mode = hash_map
            .get("mode")
            .map(|s| s.to_string())
            .unwrap_or_else(default_mode);

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
        let open_scale = hash_map
            .get("open_scale")
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or_else(default_open_scale);
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
            .unwrap_or_else(default_hedge_price_offset_limit_lower);

        let hedge_timeout = hash_map
            .get("hedge_timeout")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(default_hedge_timeout);

        let hedge_price_offset = hash_map
            .get("hedge_price_offset")
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or_else(default_hedge_price_offset);

        let hedge_aggressive_seq_threshold = hash_map
            .get("hedge_aggressive_seq_threshold")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or_else(default_hedge_aggressive_seq_threshold);

        let require_max_hedge_pct = ns == "xarb";
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
        let prediction_mode = match hash_map.get("prediction_mode") {
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" | "" => false,
                _ => {
                    panic!(
                        "Redis hash '{}' prediction_mode 非法（仅支持 true/false）: {}",
                        redis_key, raw
                    )
                }
            },
            None => default_prediction_mode(),
        };
        let enable_open_cancel = match hash_map.get("enable_open_cancel") {
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" | "" => false,
                _ => {
                    panic!(
                        "Redis hash '{}' enable_open_cancel 非法（仅支持 true/false）: {}",
                        redis_key, raw
                    )
                }
            },
            None => default_enable_open_cancel(),
        };
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
            mode,
            order_amount,
            mm_amount_u_overrides,
            open_scale,
            open_buy_vol_scale,
            open_sell_vol_scale,
            order_interval_ms,
            open_orders_per_round,
            hedge_orders_per_round,
            open_order_timeout,
            next_query_delay_ms,
            hedge_vol_multiplier,
            hedge_offset_ratio,
            hedge_price_offset_limit_upper,
            hedge_price_offset_limit_lower,
            hedge_timeout,
            hedge_price_offset,
            hedge_aggressive_seq_threshold,
            max_hedge_price_pct_change,
            signal_cooldown,
            prediction_mode,
            enable_open_cancel,
            enable_tlen_cancel,
            tlen_cancel_freq_ms,
            spread_cancel_cooldown_ms,
            enable_return_score_adjust_hedge,
            enable_environment_model,
            enable_volatility_limit,
            open_volatility_limit,
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

    /// 解析 mode
    fn parse_mode(&self) -> FactorMode {
        match self.mode.to_uppercase().as_str() {
            "MM" => FactorMode::MM,
            "MT" => FactorMode::MT,
            _ => {
                warn!("未知的 mode: {}, 使用默认 MM", self.mode);
                FactorMode::MM
            }
        }
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

        let applied = ArbDecision::with_state_mut(|arb| {
            arb.order_amount = self.order_amount;
            arb.open_scale = self.open_scale;
            arb.open_orders_per_round = self.open_orders_per_round;
            arb.open_order_ttl_us = self
                .open_order_timeout
                .saturating_mul(1_000_000)
                .min(i64::MAX as u64) as i64;
            arb.hedge_timeout_mm_us = self
                .hedge_timeout
                .saturating_mul(1_000_000)
                .min(i64::MAX as u64) as i64;
            arb.hedge_price_offset = self.hedge_price_offset;
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
            arb.enable_volatility_limit = self.enable_volatility_limit;
            arb.open_volatility_limit = self.open_volatility_limit;
            arb.return_model_service = return_model_service.clone();
            arb.environment_model_service = environment_model_service.clone();
            arb.environment_model_true_threshold = 0.0;
        })
        .is_some()
            || ArbDecision::try_update_spread_arb_model_output_services(
                self.parse_model_output_services(),
            )
            || MmDecision::try_with_mut(|_decision| {
                let open_buy_vol_scale = self
                    .parse_required_vol_scale_range(&self.open_buy_vol_scale, "open_buy_vol_scale");
                let open_sell_vol_scale = self.parse_required_vol_scale_range(
                    &self.open_sell_vol_scale,
                    "open_sell_vol_scale",
                );
                _decision.update_order_amount(self.order_amount);
                _decision.update_order_amount_overrides(self.mm_amount_u_overrides.clone());
                _decision.update_order_interval_ms(self.order_interval_ms);
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
                    self.max_hedge_price_pct_change,
                    self.next_query_delay_ms,
                    self.enable_return_score_adjust_hedge,
                );
                _decision.update_open_order_timeout(self.open_order_timeout);
                _decision.update_prediction_mode(self.prediction_mode);
                _decision.update_enable_open_cancel(self.enable_open_cancel);
                _decision.update_enable_environment_model(self.enable_environment_model);
                _decision.update_enable_volatility_limit(self.enable_volatility_limit);
                _decision.update_open_volatility_limit(self.open_volatility_limit);
                _decision.update_model_service_roles(
                    self.return_model_service.clone(),
                    self.environment_model_service.clone(),
                );
            })
            .is_some();

        // 2. 更新 SpreadFactor 模式
        let spread_factor = SpreadFactor::instance();
        spread_factor.set_mode(self.parse_mode());

        if !applied {
            warn!("策略参数已加载，但 decision 尚未初始化（将仅更新 SpreadFactor）");
        }

        info!(
            "✅ 策略参数已更新: mode={}, amount={:.2}, arb_open_scale={:.4}, mm_open_buy_vol_scale={}, mm_open_sell_vol_scale={}, order_interval_ms={}, open_orders_per_round={}, cooldown={}s, prediction_mode={}, enable_open_cancel={}, enable_tlen_cancel={}, tlen_cancel_freq_ms={}, spread_cancel_cooldown_ms={}, enable_return_score_adjust_hedge={}, enable_environment_model={}, enable_volatility_limit={}, open_volatility_limit={}, return_model_service={}, environment_model_service={}",
            self.mode,
            self.order_amount,
            self.open_scale,
            self.open_buy_vol_scale,
            self.open_sell_vol_scale,
            self.order_interval_ms,
            self.open_orders_per_round,
            self.signal_cooldown,
            self.prediction_mode,
            self.enable_open_cancel,
            self.enable_tlen_cancel,
            self.tlen_cancel_freq_ms,
            self.spread_cancel_cooldown_ms,
            self.enable_return_score_adjust_hedge,
            self.enable_environment_model,
            self.enable_volatility_limit,
            self.open_volatility_limit,
            self.return_model_service,
            self.environment_model_service
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_mode() {
        let params_mm = StrategyParams {
            mode: "MM".to_string(),
            ..Default::default()
        };
        assert_eq!(params_mm.parse_mode(), FactorMode::MM);

        let params_mt = StrategyParams {
            mode: "mt".to_string(),
            ..Default::default()
        };
        assert_eq!(params_mt.parse_mode(), FactorMode::MT);
    }

    #[test]
    fn test_enable_open_cancel_default_is_false() {
        let params = StrategyParams::default();
        assert!(!params.enable_open_cancel);
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
    fn test_mm_strategy_params_key_includes_env_name() {
        let key =
            mm_strategy_params_key_for_env(Some("binance_mm_beta"), TradingVenue::BinanceFutures);
        assert_eq!(key, "binance_mm_beta:mm_strategy_params_binance-futures");
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
    #[should_panic(expected = "MM strategy params require env_name")]
    fn test_mm_strategy_params_key_without_env_name_panics() {
        let _ = mm_strategy_params_key_for_env(None, TradingVenue::BinanceFutures);
    }
}
