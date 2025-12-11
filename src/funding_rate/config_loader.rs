//! 统一配置加载器
//!
//! 整合所有热加载逻辑：
//! - StrategyParams: 策略参数（订单量、超时、模式等）
//! - SymbolList: 建仓/平仓交易对列表
//! - FrThresholds: 资金费率阈值（未来可集成）
//! - SpreadThresholds: 价差阈值（未来可集成）
//!
//! 使用 tokio::spawn_local 单线程异步

use anyhow::{Context, Result};
use log::{info, warn};
use std::time::Duration;

use crate::common::exchange::Exchange;
use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::signal::common::TradingVenue;

use super::common::venue_pair_for_exchange;
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
/// - `exchange`: 交易所
pub fn spawn_config_loader(redis: RedisSettings, exchange: Exchange) {
    tokio::task::spawn_local(async move {
        info!("ConfigLoader 启动，{}秒定时重载", RELOAD_INTERVAL_SECS);

        let mut interval = tokio::time::interval(Duration::from_secs(RELOAD_INTERVAL_SECS));

        loop {
            interval.tick().await;

            // 重载所有配置
            if let Err(err) = reload_all_configs(&redis, exchange).await {
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
/// - `exchange`: 交易所
pub async fn load_all_once(redis: &RedisSettings, exchange: Exchange) -> Result<()> {
    info!("立即加载所有配置...");
    reload_all_configs(redis, exchange).await?;
    info!("所有配置初始加载完成");
    Ok(())
}

/// 重载所有配置的内部函数
async fn reload_all_configs(redis: &RedisSettings, exchange: Exchange) -> Result<()> {
    // 1. 加载策略参数 -> FrDecision + SpreadFactor
    reload_strategy_params(redis).await?;

    // 2. 更新 SymbolList（建仓/平仓列表）
    reload_symbol_list(redis, exchange).await?;

    // 3. 加载价差阈值 -> SpreadFactor
    reload_spread_thresholds(redis, exchange).await?;

    // 4. 加载资金费率阈值 -> FundingRateFactor
    reload_fr_thresholds(redis, exchange).await?;

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
async fn reload_symbol_list(redis: &RedisSettings, exchange: Exchange) -> Result<()> {
    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            let symbol_list = SymbolList::instance();
            symbol_list
                .reload_from_redis(&mut client, &[exchange])
                .await?;
            info!("SymbolList 重载成功 (exchange: {})", exchange);
        }
        Err(err) => {
            warn!("SymbolList 重载失败: {:?}", err);
        }
    }
    Ok(())
}

/// 重载价差阈值
async fn reload_spread_thresholds(redis: &RedisSettings, exchange: Exchange) -> Result<()> {
    let redis_key = spread_thresholds_key(exchange);

    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            let spread_map = client
                .hgetall_map(&redis_key)
                .await
                .with_context(|| format!("读取 Redis Hash 失败: {}", redis_key))?;
            if spread_map.is_empty() {
                panic!("Redis hash '{}' 为空或不存在，无法加载价差阈值", redis_key);
            }
            load_spread_thresholds(spread_map)
                .with_context(|| format!("解析价差阈值失败 (key: {})", redis_key))?;
            info!("价差阈值重载成功 (key: {})", redis_key);
        }
        Err(err) => {
            warn!("连接 Redis 加载价差阈值失败: {:?}", err);
        }
    }
    Ok(())
}

/// 重载资金费率阈值
async fn reload_fr_thresholds(redis: &RedisSettings, exchange: Exchange) -> Result<()> {
    let redis_key = funding_thresholds_key(exchange);

    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            let funding_map = client
                .hgetall_map(&redis_key)
                .await
                .with_context(|| format!("读取 Redis Hash 失败: {}", redis_key))?;
            if funding_map.is_empty() {
                panic!(
                    "Redis hash '{}' 为空或不存在，无法加载资金费率阈值",
                    redis_key
                );
            }
            load_fr_thresholds(funding_map)
                .with_context(|| format!("解析资金费率阈值失败 (key: {})", redis_key))?;
            info!("资金费率阈值重载成功 (key: {})", redis_key);
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
        .unwrap_or_else(|_| "fr_trade_symbols:binance".to_string());

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
                                TradingVenue::BinanceFutures,
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

fn funding_thresholds_key(exchange: Exchange) -> String {
    let (open_venue, hedge_venue) = venue_pair_for_exchange(exchange);
    format!(
        "funding_rate_thresholds_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn spread_thresholds_key(exchange: Exchange) -> String {
    let (open_venue, hedge_venue) = venue_pair_for_exchange(exchange);
    format!(
        "fr_spread_thresholds_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}
