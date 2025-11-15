//! 策略参数加载器（事件驱动版）
//!
//! 从 Redis 定时加载策略参数并更新到各个单例：
//! - FrDecision: 下单量、超时、偏移、冷却时间
//! - SpreadFactor: MM/MT 模式
//!
//! 使用 tokio::spawn_local 单线程异步

use anyhow::Result;
use log::{info, warn};
use serde::Deserialize;
use std::time::Duration;

use crate::common::redis_client::{RedisClient, RedisSettings};

use super::common::FactorMode;
use super::decision::FrDecision;
use super::spread_factor::SpreadFactor;
use super::symbol_list::SymbolList;
use crate::signal::common::TradingVenue;

/// Redis Key 配置
const REDIS_KEY_STRATEGY_PARAMS: &str = "fr_strategy_params";

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
            signal_cooldown: default_signal_cooldown(),
        }
    }
}

impl StrategyParams {
    /// 从 Redis Hash 加载
    async fn load_from_redis(redis: &RedisSettings) -> Result<Self> {
        let mut client = RedisClient::connect(redis.clone()).await?;
        let hash_map = client.hgetall_map(REDIS_KEY_STRATEGY_PARAMS).await?;

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
    fn apply(&self) {
        // 1. 更新 FrDecision
        FrDecision::with_mut(|decision| {
            decision.update_order_amount(self.order_amount);
            decision.update_price_offsets(self.parse_price_offsets());
            decision.update_open_order_timeout(self.open_order_timeout);
            decision.update_hedge_timeout(self.hedge_timeout);
            decision.update_hedge_price_offset(self.hedge_price_offset);
            decision.update_signal_cooldown(self.signal_cooldown);
        });

        // 2. 更新 SpreadFactor 模式
        let spread_factor = SpreadFactor::instance();
        spread_factor.set_mode(self.parse_mode());

        info!("✅ 策略参数已更新: mode={}, amount={:.2}, cooldown={}s",
            self.mode, self.order_amount, self.signal_cooldown);
    }
}

/// 启动参数加载器（spawn_local）
///
/// 每 60 秒从 Redis 重新加载参数并更新
///
/// # 参数
/// - `redis`: Redis 配置
pub fn spawn_params_loader(redis: RedisSettings) {
    tokio::task::spawn_local(async move {
        info!("ParamsLoader 启动，60秒定时重载");

        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            // 1. 加载策略参数
            match StrategyParams::load_from_redis(&redis).await {
                Ok(params) => {
                    info!("从 Redis 加载策略参数成功");
                    params.apply();
                }
                Err(err) => {
                    warn!("从 Redis 加载策略参数失败: {:?}", err);
                }
            }

            // 2. 更新 SymbolList（dup_symbols + trade_symbols）
            match RedisClient::connect(redis.clone()).await {
                Ok(mut client) => {
                    let symbol_list = SymbolList::instance();
                    if let Err(err) = symbol_list.reload_from_redis(
                        &mut client,
                        &[TradingVenue::BinanceUm, TradingVenue::BinanceMargin]
                    ).await {
                        warn!("从 Redis 更新 SymbolList 失败: {:?}", err);
                    }
                }
                Err(err) => {
                    warn!("连接 Redis 更新 SymbolList 失败: {:?}", err);
                }
            }
        }
    });
}

/// 立即加载一次参数（同步调用）
///
/// 用于初始化时立即加载，不等待 60 秒
pub async fn load_params_once(redis: &RedisSettings) -> Result<()> {
    info!("立即加载策略参数...");

    // 1. 加载策略参数
    let params = StrategyParams::load_from_redis(redis).await?;
    params.apply();
    info!("策略参数初始加载完成");

    // 2. 更新 SymbolList（dup_symbols + trade_symbols）
    let mut client = RedisClient::connect(redis.clone()).await?;
    let symbol_list = SymbolList::instance();
    symbol_list.reload_from_redis(
        &mut client,
        &[TradingVenue::BinanceUm, TradingVenue::BinanceMargin]
    ).await?;
    info!("SymbolList 初始加载完成");

    Ok(())
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
