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

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::signal::common::TradingVenue;

use super::fr_threshold_loader::load_from_redis as load_fr_thresholds;
use super::spread_threshold_loader::load_from_redis as load_spread_thresholds;
use super::strategy_loader::StrategyParams;
use super::symbol_list::SymbolList;

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

/// 配置加载间隔（秒）
const RELOAD_INTERVAL_SECS: u64 = 60;

/// 启动统一配置加载器（spawn_local）
///
/// 每 60 秒从 Redis 重新加载所有配置并更新到单例
///
/// # 参数
/// - `redis`: Redis 配置
/// - `open_venue`: 开仓 venue
/// - `hedge_venue`: 对冲 venue
pub fn spawn_config_loader(
    redis: RedisSettings,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) {
    spawn_config_loader_with_namespace(
        redis,
        DEFAULT_NAMESPACE.to_string(),
        String::new(),
        open_venue,
        hedge_venue,
    );
}

pub fn spawn_config_loader_with_namespace(
    redis: RedisSettings,
    symbol_namespace: String,
    symbol_key_suffix: String,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) {
    tokio::task::spawn_local(async move {
        let ns = normalize_namespace(&symbol_namespace);
        info!(
            "ConfigLoader 启动，{}秒定时重载 (ns={} suffix={})",
            RELOAD_INTERVAL_SECS, ns, symbol_key_suffix
        );

        let mut interval = tokio::time::interval(Duration::from_secs(RELOAD_INTERVAL_SECS));

        loop {
            interval.tick().await;

            // 重载所有配置
            if let Err(err) =
                reload_all_configs(&redis, &ns, &symbol_key_suffix, open_venue, hedge_venue).await
            {
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
/// - `open_venue`: 开仓 venue
/// - `hedge_venue`: 对冲 venue
pub async fn load_all_once(
    redis: &RedisSettings,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    load_all_once_with_namespace(redis, DEFAULT_NAMESPACE, "", open_venue, hedge_venue).await?;
    info!("所有配置初始加载完成");
    Ok(())
}

pub async fn load_all_once_with_namespace(
    redis: &RedisSettings,
    symbol_namespace: &str,
    symbol_key_suffix: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let ns = normalize_namespace(symbol_namespace);
    info!(
        "立即加载所有配置... (ns={} suffix={})",
        ns, symbol_key_suffix
    );
    reload_all_configs(redis, &ns, symbol_key_suffix, open_venue, hedge_venue).await?;
    Ok(())
}

/// 重载所有配置的内部函数
async fn reload_all_configs(
    redis: &RedisSettings,
    namespace: &str,
    symbol_key_suffix: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    // 1. 加载策略参数 -> FrDecision + SpreadFactor
    reload_strategy_params(redis, namespace, open_venue, hedge_venue).await?;

    // 2. 更新 SymbolList（建仓/平仓列表）
    reload_symbol_list(redis, namespace, symbol_key_suffix, open_venue, hedge_venue).await?;

    // 3. 加载价差阈值 -> SpreadFactor
    reload_spread_thresholds(redis, namespace, open_venue, hedge_venue).await?;

    // 4. 加载资金费率阈值 -> FundingRateFactor
    reload_fr_thresholds(redis, namespace, open_venue, hedge_venue).await?;

    info!("✅ 配置重载完成");
    Ok(())
}

/// 重载策略参数
async fn reload_strategy_params(
    redis: &RedisSettings,
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    match StrategyParams::load_from_redis(redis, namespace, open_venue, hedge_venue).await {
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
async fn reload_symbol_list(
    redis: &RedisSettings,
    namespace: &str,
    symbol_key_suffix: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            let symbol_list = SymbolList::instance();
            let suffix = if symbol_key_suffix.trim().is_empty() {
                format!(
                    "{}_{}",
                    open_venue.data_pub_slug(),
                    hedge_venue.data_pub_slug()
                )
            } else {
                symbol_key_suffix.to_string()
            };
            symbol_list
                .reload_from_redis_with_key_suffix(&mut client, &suffix, namespace)
                .await?;
            info!("SymbolList 重载成功 (ns={} suffix={})", namespace, suffix);
        }
        Err(err) => {
            warn!("SymbolList 重载失败: {:?}", err);
        }
    }
    Ok(())
}

/// 重载价差阈值
async fn reload_spread_thresholds(
    redis: &RedisSettings,
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let redis_key = spread_thresholds_key(namespace, open_venue, hedge_venue);

    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            let spread_map = client
                .hgetall_map(&redis_key)
                .await
                .with_context(|| format!("读取 Redis Hash 失败: {}", redis_key))?;
            if spread_map.is_empty() {
                panic!("Redis hash '{}' 为空或不存在，无法加载价差阈值", redis_key);
            }
            load_spread_thresholds(spread_map, open_venue, hedge_venue)
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
async fn reload_fr_thresholds(
    redis: &RedisSettings,
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let redis_key = funding_thresholds_key(namespace, open_venue, hedge_venue);
    let ns = normalize_namespace(namespace);

    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            let funding_map = client
                .hgetall_map(&redis_key)
                .await
                .with_context(|| format!("读取 Redis Hash 失败: {}", redis_key))?;
            if funding_map.is_empty() {
                if ns == "fr" {
                    panic!(
                        "Redis hash '{}' 为空或不存在，无法加载资金费率阈值",
                        redis_key
                    );
                }
                warn!(
                    "Redis hash '{}' 为空或不存在，跳过资金费率阈值加载 (ns={})",
                    redis_key, ns
                );
                return Ok(());
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

fn funding_thresholds_key(
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> String {
    let ns = normalize_namespace(namespace);
    let prefix = if ns == "fr" {
        "funding_rate_thresholds".to_string()
    } else {
        format!("{ns}_funding_rate_thresholds")
    };
    format!(
        "{}_{}_{}",
        prefix,
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn spread_thresholds_key(
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> String {
    let ns = normalize_namespace(namespace);
    let prefix = if ns == "fr" {
        "fr_spread_thresholds".to_string()
    } else {
        format!("{ns}_spread_thresholds")
    };
    format!(
        "{}_{}_{}",
        prefix,
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}
