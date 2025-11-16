//! 统一配置加载器
//!
//! 整合所有热加载逻辑：
//! - StrategyParams: 策略参数（订单量、超时、模式等）
//! - SymbolList: 建仓/平仓交易对列表
//! - FrThresholds: 资金费率阈值（未来可集成）
//! - SpreadThresholds: 价差阈值（未来可集成）
//!
//! 使用 tokio::spawn_local 单线程异步

use anyhow::Result;
use log::{info, warn};
use std::time::Duration;

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::signal::common::TradingVenue;

use super::decision::FrDecision;
use super::fr_threshold_loader::load_from_redis as load_fr_thresholds;
use super::spread_threshold_loader::load_from_redis as load_spread_thresholds;
use super::strategy_loader::StrategyParams;
use super::symbol_list::SymbolList;

/// 配置加载间隔（秒）
const RELOAD_INTERVAL_SECS: u64 = 60;

/// 启动统一配置加载器（spawn_local）
///
/// 每 60 秒从 Redis 重新加载所有配置并更新到单例
///
/// # 参数
/// - `redis`: Redis 配置
pub fn spawn_config_loader(redis: RedisSettings) {
    tokio::task::spawn_local(async move {
        info!("ConfigLoader 启动，{}秒定时重载", RELOAD_INTERVAL_SECS);

        let mut interval = tokio::time::interval(Duration::from_secs(RELOAD_INTERVAL_SECS));

        loop {
            interval.tick().await;

            // 重载所有配置
            if let Err(err) = reload_all_configs(&redis).await {
                warn!("配置重载失败: {:?}", err);
            }
        }
    });
}

/// 立即加载一次所有配置（同步调用）
///
/// 用于初始化时立即加载，不等待定时器触发
///
/// # 参数
/// - `redis`: Redis 配置
pub async fn load_all_once(redis: &RedisSettings) -> Result<()> {
    info!("立即加载所有配置...");
    reload_all_configs(redis).await?;
    info!("所有配置初始加载完成");
    Ok(())
}

/// 重载所有配置的内部函数
async fn reload_all_configs(redis: &RedisSettings) -> Result<()> {
    // 1. 加载策略参数 -> FrDecision + SpreadFactor
    reload_strategy_params(redis).await?;

    // 2. 更新 SymbolList（建仓/平仓列表）
    reload_symbol_list(redis).await?;

    // 3. 加载价差阈值 -> SpreadFactor
    reload_spread_thresholds(redis).await?;

    // 4. 加载资金费率阈值 -> FundingRateFactor
    reload_fr_thresholds(redis).await?;

    // 5. 加载交易对白名单 -> FrDecision
    reload_check_symbols(redis).await?;

    info!("✅ 配置重载完成");
    Ok(())
}

/// 重载策略参数
async fn reload_strategy_params(redis: &RedisSettings) -> Result<()> {
    match StrategyParams::load_from_redis(redis).await {
        Ok(params) => {
            params.apply();
            info!("策略参数重载成功");
        }
        Err(err) => {
            warn!("策略参数重载失败: {:?}", err);
        }
    }
    Ok(())
}

/// 重载符号列表
async fn reload_symbol_list(redis: &RedisSettings) -> Result<()> {
    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            let symbol_list = SymbolList::instance();
            symbol_list
                .reload_from_redis(
                    &mut client,
                    &[TradingVenue::BinanceUm, TradingVenue::BinanceMargin],
                )
                .await?;
            info!("SymbolList 重载成功");
        }
        Err(err) => {
            warn!("SymbolList 重载失败: {:?}", err);
        }
    }
    Ok(())
}

/// 重载价差阈值
async fn reload_spread_thresholds(redis: &RedisSettings) -> Result<()> {
    let redis_key = std::env::var("SPREAD_THRESHOLD_REDIS_KEY")
        .unwrap_or_else(|_| "binance_spread_thresholds".to_string());

    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            if let Ok(spread_map) = client.hgetall_map(&redis_key).await {
                if let Err(err) = load_spread_thresholds(spread_map) {
                    warn!("价差阈值加载失败: {:?}", err);
                } else {
                    info!("价差阈值重载成功");
                }
            }
        }
        Err(err) => {
            warn!("连接 Redis 加载价差阈值失败: {:?}", err);
        }
    }
    Ok(())
}

/// 重载资金费率阈值
async fn reload_fr_thresholds(redis: &RedisSettings) -> Result<()> {
    let redis_key = std::env::var("FUNDING_THRESHOLD_REDIS_KEY")
        .unwrap_or_else(|_| "funding_rate_thresholds".to_string());

    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            if let Ok(funding_map) = client.hgetall_map(&redis_key).await {
                if let Err(err) = load_fr_thresholds(funding_map) {
                    warn!("资金费率阈值加载失败: {:?}", err);
                } else {
                    info!("资金费率阈值重载成功");
                }
            }
        }
        Err(err) => {
            warn!("连接 Redis 加载资金费率阈值失败: {:?}", err);
        }
    }
    Ok(())
}

/// 重载交易对白名单
async fn reload_check_symbols(redis: &RedisSettings) -> Result<()> {
    let redis_key = std::env::var("SYMBOLS_REDIS_KEY")
        .unwrap_or_else(|_| "fr_trade_symbols:binance_um".to_string());

    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            if let Ok(Some(symbols_json)) = client.get_string(&redis_key).await {
                if let Ok(symbols) = serde_json::from_str::<Vec<String>>(&symbols_json) {
                    let pairs: Vec<_> = symbols
                        .iter()
                        .map(|s| {
                            (
                                TradingVenue::BinanceMargin,
                                s.to_uppercase(),
                                TradingVenue::BinanceUm,
                                s.to_uppercase(),
                            )
                        })
                        .collect();

                    FrDecision::with_mut(|decision| {
                        decision.update_check_symbols(pairs);
                    });

                    info!("交易对白名单重载成功: {} 个", symbols.len());
                }
            }
        }
        Err(err) => {
            warn!("连接 Redis 加载交易对白名单失败: {:?}", err);
        }
    }
    Ok(())
}
