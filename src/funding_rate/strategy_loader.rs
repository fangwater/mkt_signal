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

    /// 开仓挂单档位（JSON 数组）
    /// 例如: "[0.0002, 0.0004, 0.0006, 0.0008, 0.001]"
    #[serde(default = "default_price_offsets")]
    pub price_offsets: String,

    /// 开仓订单超时（秒）
    #[serde(default = "default_open_order_timeout")]
    pub open_order_timeout: u64,

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
}

// 默认值函数
fn default_mode() -> String {
    "MM".to_string()
}
fn default_order_amount() -> f32 {
    100.0
}
fn default_price_offsets() -> String {
    "[0.0002, 0.0004, 0.0006, 0.0008, 0.001]".to_string()
}
fn default_open_order_timeout() -> u64 {
    120
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

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            order_amount: default_order_amount(),
            price_offsets: default_price_offsets(),
            open_order_timeout: default_open_order_timeout(),
            hedge_timeout: default_hedge_timeout(),
            hedge_price_offset: default_hedge_price_offset(),
            hedge_aggressive_seq_threshold: default_hedge_aggressive_seq_threshold(),
            max_hedge_price_pct_change: default_max_hedge_price_pct_change(),
            signal_cooldown: default_signal_cooldown(),
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

        let price_offsets = hash_map
            .get("price_offsets")
            .map(|s| s.to_string())
            .unwrap_or_else(default_price_offsets);

        let open_order_timeout = hash_map
            .get("open_order_timeout")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(default_open_order_timeout);

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
                    panic!(
                        "Redis hash '{}' 缺少 max_hedge_price_pct_change",
                        redis_key
                    );
                }
                default_max_hedge_price_pct_change()
            }
        };

        let signal_cooldown = hash_map
            .get("signal_cooldown")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(default_signal_cooldown);

        Ok(Self {
            mode,
            order_amount,
            price_offsets,
            open_order_timeout,
            hedge_timeout,
            hedge_price_offset,
            hedge_aggressive_seq_threshold,
            max_hedge_price_pct_change,
            signal_cooldown,
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
        })
            .is_some();

        // 2. 更新 SpreadFactor 模式
        let spread_factor = SpreadFactor::instance();
        spread_factor.set_mode(self.parse_mode());

        if !applied {
            warn!("策略参数已加载，但 decision 尚未初始化（将仅更新 SpreadFactor）");
        }

        info!(
            "✅ 策略参数已更新: mode={}, amount={:.2}, cooldown={}s",
            self.mode, self.order_amount, self.signal_cooldown
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
