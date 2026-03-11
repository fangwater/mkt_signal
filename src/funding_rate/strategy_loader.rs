//! 策略参数定义
//!
//! 定义策略参数结构及其 Redis 加载逻辑：
//! - FrDecision: 下单量、超时、偏移、冷却时间
//! - SpreadFactor: MM/MT 模式

use anyhow::Result;
use log::{info, warn};
use serde::Deserialize;

use crate::common::redis_client::{RedisClient, RedisSettings};

use super::common::FactorMode;
use super::fr_decision::FrDecision;
use super::mm_decision::MmDecision;
use super::spread_factor::SpreadFactor;
use super::xarb_decision::XarbDecision;
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

fn strategy_params_key(
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> String {
    let ns = normalize_namespace(namespace);
    if ns == "mm" {
        return format!("mm_strategy_params_{}", hedge_venue.data_pub_slug());
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

/// 策略参数结构（从 Redis Hash 反序列化）
#[derive(Debug, Clone, Deserialize)]
pub struct StrategyParams {
    /// 做市模式：MM（Maker-Maker）或 MT（Maker-Taker）
    #[serde(default = "default_mode")]
    pub mode: String,

    /// 单笔下单量（USDT）
    #[serde(default = "default_order_amount")]
    pub order_amount: f32,

    /// MM 报单触发间隔（毫秒）
    #[serde(default = "default_order_interval_ms")]
    pub order_interval_ms: u64,

    /// MM 每轮报单数量
    #[serde(default = "default_open_orders_per_round")]
    pub open_orders_per_round: u32,

    #[serde(default = "default_hedge_orders_per_round")]
    pub hedge_orders_per_round: u32,

    /// 开仓挂单档位（JSON 数组）
    /// 例如: "[0.0002, 0.0004, 0.0006, 0.0008, 0.001]"
    #[serde(default = "default_price_offsets")]
    pub price_offsets: String,

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

    /// 对冲触发 taker 的最大价格变动比例（百分比，1~99 表示 1%~99%）
    #[serde(default = "default_max_hedge_price_pct_change")]
    pub max_hedge_price_pct_change: f64,

    /// 信号冷却时间（秒）
    #[serde(default = "default_signal_cooldown")]
    pub signal_cooldown: u64,

    /// MM 是否启用方向预测（true=按 return score 过滤单边，false=双边同时报价）
    #[serde(default = "default_prediction_mode")]
    pub prediction_mode: bool,

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
fn default_order_interval_ms() -> u64 {
    5_000
}
fn default_open_orders_per_round() -> u32 {
    1
}
fn default_hedge_orders_per_round() -> u32 {
    8
}
fn default_price_offsets() -> String {
    "[0.0002, 0.0004, 0.0006, 0.0008, 0.001]".to_string()
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
            order_interval_ms: default_order_interval_ms(),
            open_orders_per_round: default_open_orders_per_round(),
            hedge_orders_per_round: default_hedge_orders_per_round(),
            price_offsets: default_price_offsets(),
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
            return_model_service: default_return_model_service(),
            environment_model_service: default_environment_model_service(),
        }
    }
}

impl StrategyParams {
    /// 从 Redis Hash 加载
    pub(crate) async fn load_from_redis(
        redis: &RedisSettings,
        namespace: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Self> {
        let mut client = RedisClient::connect(redis.clone()).await?;
        let ns = normalize_namespace(namespace);
        let redis_key = strategy_params_key(&ns, open_venue, hedge_venue);
        let hash_map = client.hgetall_map(&redis_key).await?;
        if hash_map.is_empty() {
            panic!("Redis hash '{}' 为空或不存在，无法加载策略参数", redis_key);
        }

        // 手动解析 Hash 字段
        let mode = hash_map
            .get("mode")
            .map(|s| s.to_string())
            .unwrap_or_else(default_mode);

        let order_amount = hash_map
            .get("order_amount")
            .and_then(|s| s.parse::<f32>().ok())
            .unwrap_or_else(default_order_amount);

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

        let price_offsets = hash_map
            .get("price_offsets")
            .map(|s| s.to_string())
            .unwrap_or_else(default_price_offsets);

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
                if !(parsed.is_finite() && parsed >= 1.0 && parsed <= 99.0) {
                    panic!(
                        "Redis hash '{}' max_hedge_price_pct_change 无效(需在1~99): {}",
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

        let strict_return_model_required = ns == "xarb" || ns == "mm";
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
            order_interval_ms,
            open_orders_per_round,
            hedge_orders_per_round,
            price_offsets,
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
            return_model_service,
            environment_model_service,
        })
    }

    /// 解析 price_offsets JSON 数组
    fn parse_price_offsets(&self) -> Vec<f64> {
        serde_json::from_str::<Vec<f64>>(&self.price_offsets).unwrap_or_else(|err| {
            warn!(
                "无法解析 price_offsets: {} (err: {}), 使用默认值",
                self.price_offsets, err
            );
            vec![0.0002, 0.0004, 0.0006, 0.0008, 0.001]
        })
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
        // 1. 更新 decision（FR or xarb）
        let applied = FrDecision::try_with_mut(|decision| {
            decision.update_order_amount(self.order_amount);
            decision.update_price_offsets(self.parse_price_offsets());
            decision.update_open_order_timeout(self.open_order_timeout);
            decision.update_hedge_timeout(self.hedge_timeout);
            decision.update_hedge_price_offset(self.hedge_price_offset);
            decision.update_hedge_aggressive_seq_threshold(self.hedge_aggressive_seq_threshold);
            decision.update_signal_cooldown(self.signal_cooldown);
        })
        .is_some()
            || XarbDecision::try_with_mut(|decision| {
                decision.update_order_amount(self.order_amount);
                decision.update_price_offsets(self.parse_price_offsets());
                decision.update_open_order_timeout(self.open_order_timeout);
                decision.update_hedge_timeout(self.hedge_timeout);
                decision.update_hedge_price_offset(self.hedge_price_offset);
                decision.update_hedge_aggressive_seq_threshold(self.hedge_aggressive_seq_threshold);
                decision.update_max_hedge_price_pct_change(self.max_hedge_price_pct_change);
                decision.update_signal_cooldown(self.signal_cooldown);
                decision.update_model_service_roles(
                    self.return_model_service.clone(),
                    self.environment_model_service.clone(),
                );
                decision.update_model_output_services(self.parse_model_output_services());
            })
            .is_some()
            || MmDecision::try_with_mut(|_decision| {
                _decision.update_order_amount(self.order_amount);
                _decision.update_order_interval_ms(self.order_interval_ms);
                _decision.update_open_orders_per_round(self.open_orders_per_round);
                _decision.update_mm_hedge_params(
                    self.hedge_orders_per_round,
                    self.hedge_vol_multiplier,
                    self.hedge_offset_ratio,
                    self.hedge_price_offset_limit_lower,
                    self.hedge_price_offset_limit_upper,
                    self.next_query_delay_ms,
                );
                _decision.update_open_order_timeout(self.open_order_timeout);
                _decision.update_prediction_mode(self.prediction_mode);
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
            "✅ 策略参数已更新: mode={}, amount={:.2}, order_interval_ms={}, open_orders_per_round={}, cooldown={}s, prediction_mode={}, return_model_service={}, environment_model_service={}",
            self.mode,
            self.order_amount,
            self.order_interval_ms,
            self.open_orders_per_round,
            self.signal_cooldown,
            self.prediction_mode,
            self.return_model_service,
            self.environment_model_service
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_price_offsets() {
        let params = StrategyParams {
            price_offsets: "[0.0001, 0.0002, 0.0003]".to_string(),
            ..Default::default()
        };
        let offsets = params.parse_price_offsets();
        assert_eq!(offsets, vec![0.0001, 0.0002, 0.0003]);
    }

    #[test]
    fn test_parse_invalid_price_offsets() {
        let params = StrategyParams {
            price_offsets: "invalid json".to_string(),
            ..Default::default()
        };
        let offsets = params.parse_price_offsets();
        // 应该返回默认值
        assert_eq!(offsets.len(), 5);
    }

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
}
