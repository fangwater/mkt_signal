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
use log::{debug, info, warn};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::signal::common::TradingVenue;
use crate::symbol_match::normalize_symbol_for_whitelist;

use super::common::resolve_return_score_thresholds_from_redis_map;
use super::fr_threshold_loader::load_from_redis as load_fr_thresholds;
use super::mm_decision::MmDecision;
use super::spread_threshold_loader::load_from_redis as load_spread_thresholds;
use super::strategy_loader::StrategyParams;
use super::symbol_list::SymbolList;
use super::xarb_decision::XarbDecision;
use super::xarb_funding_threshold_loader::XarbFundingThresholdsResolved;

const DEFAULT_NAMESPACE: &str = "fr";
const RETURN_SCORE_REDIS_REFRESH_SECS: u64 = 180;
const OPEN_VOL_THRESHOLD_REDIS_REFRESH_SECS: u64 = 180;

#[derive(Debug, Clone)]
struct RedisHashCacheEntry {
    fields: HashMap<String, String>,
    fetched_at: Instant,
}

thread_local! {
    static MM_RETURN_SCORE_CACHE: RefCell<HashMap<String, RedisHashCacheEntry>> =
        RefCell::new(HashMap::new());
    static OPEN_VOL_THRESHOLD_CACHE: RefCell<HashMap<String, RedisHashCacheEntry>> =
        RefCell::new(HashMap::new());
}

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

    // 3. xarb 统一从 rolling_metrics + mapping 配置重建内存阈值；
    // 其他 namespace 仍沿用原有 spread/fr Redis hash 热加载逻辑。
    if normalize_namespace(namespace) == "xarb" {
        reload_xarb_thresholds_from_rolling(redis, open_venue, hedge_venue).await?;
    } else {
        reload_spread_thresholds(redis, namespace, open_venue, hedge_venue).await?;
        reload_fr_thresholds(redis, namespace, open_venue, hedge_venue).await?;
    }

    // 5. 加载 return-model-score 阈值 -> MmDecision/XarbDecision（仅 ns=mm/xarb）
    reload_return_score_thresholds(redis, namespace, hedge_venue).await?;
    reload_open_volatility_thresholds(redis, namespace, open_venue, hedge_venue).await?;

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
    let ns = normalize_namespace(namespace);
    let strict_required = ns == "fr" || ns == "xarb";

    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            let spread_map = match client.hgetall_map(&redis_key).await {
                Ok(map) => map,
                Err(err) => {
                    if strict_required {
                        panic!(
                            "读取 Redis Hash '{}' 失败，无法加载价差阈值: {:?}",
                            redis_key, err
                        );
                    }
                    warn!(
                        "读取 Redis Hash 失败: {} ({:?}), 跳过价差阈值加载 (ns={})",
                        redis_key, err, ns
                    );
                    return Ok(());
                }
            };
            if spread_map.is_empty() {
                if strict_required {
                    panic!("Redis hash '{}' 为空或不存在，无法加载价差阈值", redis_key);
                }
                warn!(
                    "Redis hash '{}' 为空或不存在，跳过价差阈值加载 (ns={})",
                    redis_key, ns
                );
                return Ok(());
            }
            load_spread_thresholds(spread_map, open_venue, hedge_venue)
                .with_context(|| format!("解析价差阈值失败 (key: {})", redis_key))?;
            info!("价差阈值重载成功 (key: {})", redis_key);
        }
        Err(err) => {
            if strict_required {
                panic!("连接 Redis 失败，无法加载价差阈值: {:?}", err);
            }
            warn!("连接 Redis 加载价差阈值失败: {:?} (ns={})", err, ns);
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
            let funding_map = match client.hgetall_map(&redis_key).await {
                Ok(map) => map,
                Err(err) => {
                    if ns == "fr" {
                        panic!(
                            "读取 Redis Hash '{}' 失败，无法加载资金费率阈值: {:?}",
                            redis_key, err
                        );
                    }
                    warn!("读取 Redis Hash 失败: {} ({:?})", redis_key, err);
                    return Ok(());
                }
            };
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
            if ns == "fr" {
                panic!("连接 Redis 失败，无法加载资金费率阈值: {:?}", err);
            }
            warn!("连接 Redis 加载资金费率阈值失败: {:?}", err);
        }
    }
    Ok(())
}

/// 重载 return-model-score 阈值（仅 namespace=mm/xarb）
async fn reload_return_score_thresholds(
    redis: &RedisSettings,
    namespace: &str,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let ns = normalize_namespace(namespace);
    if ns != "mm" && ns != "xarb" {
        return Ok(());
    }

    let redis_key = return_model_score_thresholds_key(hedge_venue);
    let refresh_interval = Duration::from_secs(RETURN_SCORE_REDIS_REFRESH_SECS);

    let mut cached_fields: Option<HashMap<String, String>> = None;
    let mut cache_fresh = false;
    MM_RETURN_SCORE_CACHE.with(|cell| {
        let cache = cell.borrow();
        if let Some(entry) = cache.get(&redis_key) {
            cached_fields = Some(entry.fields.clone());
            cache_fresh = entry.fetched_at.elapsed() < refresh_interval;
        }
    });

    let score_map = if cache_fresh {
        debug!(
            "return score 阈值命中缓存 (ns={} key={} refresh={}s)",
            ns, redis_key, RETURN_SCORE_REDIS_REFRESH_SECS
        );
        cached_fields.unwrap_or_default()
    } else {
        match RedisClient::connect(redis.clone()).await {
            Ok(mut client) => match client.hgetall_map(&redis_key).await {
                Ok(map) => {
                    MM_RETURN_SCORE_CACHE.with(|cell| {
                        cell.borrow_mut().insert(
                            redis_key.clone(),
                            RedisHashCacheEntry {
                                fields: map.clone(),
                                fetched_at: Instant::now(),
                            },
                        );
                    });
                    info!(
                        "return score 阈值从 Redis 刷新 (ns={} key={} fields={} interval={}s)",
                        ns,
                        redis_key,
                        map.len(),
                        RETURN_SCORE_REDIS_REFRESH_SECS
                    );
                    map
                }
                Err(err) => {
                    if let Some(cache) = cached_fields {
                        warn!(
                            "读取 Redis Hash 失败: {} ({:?}), 使用缓存阈值 ns={} fields={}",
                            redis_key,
                            err,
                            ns,
                            cache.len()
                        );
                        cache
                    } else {
                        warn!(
                            "读取 Redis Hash 失败: {} ({:?}), 且无可用缓存，跳过 return score 阈值加载 ns={}",
                            redis_key, err, ns
                        );
                        return Ok(());
                    }
                }
            },
            Err(err) => {
                if let Some(cache) = cached_fields {
                    warn!(
                        "连接 Redis 失败 ({:?}), 使用缓存阈值 ns={} key={} fields={}",
                        err,
                        ns,
                        redis_key,
                        cache.len()
                    );
                    cache
                } else {
                    warn!(
                        "连接 Redis 加载 return score 阈值失败: {:?} (ns={} key={}, 无缓存)",
                        err, ns, redis_key
                    );
                    return Ok(());
                }
            }
        }
    };

    let loaded_fields = score_map.len();
    let (thresholds, stats) =
        resolve_return_score_thresholds_from_redis_map(score_map, hedge_venue);
    let updated = match ns.as_str() {
        "mm" => MmDecision::try_with_mut(|decision| {
            decision.update_return_score_thresholds(thresholds);
        }),
        "xarb" => XarbDecision::try_with_mut(|decision| {
            decision.update_return_score_thresholds(thresholds);
        }),
        _ => None,
    };
    if updated.is_none() {
        warn!(
            "return score 阈值已读取，但对应 decision 尚未初始化 (ns={} key={})",
            ns, redis_key
        );
        return Ok(());
    }

    info!(
        "return score 阈值应用完成 (ns={} key={} fields={} symbols={} incomplete_symbols={} ignored_fields={} bad_value_fields={})",
        ns,
        redis_key,
        loaded_fields,
        stats.loaded_symbols,
        stats.incomplete_symbols,
        stats.ignored_fields,
        stats.bad_value_fields
    );
    Ok(())
}

fn format_quantile_field_ref(prefix: &str, percentile: f64) -> String {
    let suffix = if (percentile - percentile.round()).abs() < 1e-9 {
        format!("{}", percentile.round() as i64)
    } else {
        let mut text = percentile.to_string();
        while text.contains('.') && text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
        text
    };
    format!("{prefix}_{suffix}")
}

fn resolve_symbol_single_quantile_thresholds(
    rolling_payloads: &HashMap<String, serde_json::Value>,
    field_ref: &str,
) -> (HashMap<String, f64>, usize) {
    let mut resolved = HashMap::new();
    let mut missing_refs = 0usize;

    for (symbol, payload) in rolling_payloads {
        match extract_quantile_value(payload, field_ref) {
            Some(value) => {
                resolved.insert(symbol.clone(), value);
            }
            None => {
                missing_refs += 1;
            }
        }
    }

    (resolved, missing_refs)
}

async fn reload_open_volatility_thresholds(
    redis: &RedisSettings,
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let ns = normalize_namespace(namespace);
    if ns != "mm" && ns != "xarb" {
        return Ok(());
    }

    let params =
        match StrategyParams::load_from_redis(redis, namespace, open_venue, hedge_venue).await {
            Ok(params) => params,
            Err(err) => {
                warn!(
                    "读取 open volatility limit 策略参数失败 (ns={} open={} hedge={}): {:?}",
                    ns,
                    open_venue.data_pub_slug(),
                    hedge_venue.data_pub_slug(),
                    err
                );
                return Ok(());
            }
        };

    let rolling_key = default_rolling_thresholds_key(open_venue, hedge_venue);
    let refresh_interval = Duration::from_secs(OPEN_VOL_THRESHOLD_REDIS_REFRESH_SECS);

    let mut cached_fields: Option<HashMap<String, String>> = None;
    let mut cache_fresh = false;
    OPEN_VOL_THRESHOLD_CACHE.with(|cell| {
        let cache = cell.borrow();
        if let Some(entry) = cache.get(&rolling_key) {
            cached_fields = Some(entry.fields.clone());
            cache_fresh = entry.fetched_at.elapsed() < refresh_interval;
        }
    });

    let rolling_map = if cache_fresh {
        debug!(
            "open volatility thresholds 命中缓存 (ns={} key={} refresh={}s)",
            ns, rolling_key, OPEN_VOL_THRESHOLD_REDIS_REFRESH_SECS
        );
        cached_fields.unwrap_or_default()
    } else {
        match RedisClient::connect(redis.clone()).await {
            Ok(mut client) => match client.hgetall_map(&rolling_key).await {
                Ok(map) => {
                    OPEN_VOL_THRESHOLD_CACHE.with(|cell| {
                        cell.borrow_mut().insert(
                            rolling_key.clone(),
                            RedisHashCacheEntry {
                                fields: map.clone(),
                                fetched_at: Instant::now(),
                            },
                        );
                    });
                    info!(
                        "open volatility thresholds 从 Redis 刷新 (ns={} key={} fields={} interval={}s)",
                        ns,
                        rolling_key,
                        map.len(),
                        OPEN_VOL_THRESHOLD_REDIS_REFRESH_SECS
                    );
                    map
                }
                Err(err) => {
                    if let Some(cache) = cached_fields {
                        warn!(
                            "读取 rolling_metrics hash 失败: {} ({:?}), 使用缓存 open volatility thresholds ns={} fields={}",
                            rolling_key,
                            err,
                            ns,
                            cache.len()
                        );
                        cache
                    } else {
                        warn!(
                            "读取 rolling_metrics hash 失败: {} ({:?}), 且无可用缓存，跳过 open volatility thresholds ns={}",
                            rolling_key, err, ns
                        );
                        return Ok(());
                    }
                }
            },
            Err(err) => {
                if let Some(cache) = cached_fields {
                    warn!(
                        "连接 Redis 失败 ({:?}), 使用缓存 open volatility thresholds ns={} key={} fields={}",
                        err,
                        ns,
                        rolling_key,
                        cache.len()
                    );
                    cache
                } else {
                    warn!(
                        "连接 Redis 加载 open volatility thresholds 失败: {:?} (ns={} key={}, 无缓存)",
                        err, ns, rolling_key
                    );
                    return Ok(());
                }
            }
        }
    };

    if rolling_map.is_empty() {
        warn!(
            "rolling_metrics hash '{}' 为空，跳过 open volatility thresholds 加载 (ns={})",
            rolling_key, ns
        );
        return Ok(());
    }

    let active_symbols: HashSet<String> = SymbolList::instance()
        .get_online_symbols()
        .into_iter()
        .map(|symbol| normalize_xarb_symbol(&symbol))
        .filter(|symbol| !symbol.is_empty())
        .collect();
    let rolling_payloads = parse_xarb_rolling_payloads(rolling_map, &active_symbols);
    let field_ref = format_quantile_field_ref("open_vol", params.open_volatility_limit);
    let (thresholds, missing_refs) =
        resolve_symbol_single_quantile_thresholds(&rolling_payloads, &field_ref);

    let updated = if ns == "xarb" {
        XarbDecision::try_with_mut(|decision| {
            decision.update_open_volatility_thresholds(thresholds.clone());
        })
        .is_some()
    } else {
        MmDecision::try_with_mut(|decision| {
            decision.update_open_volatility_thresholds(thresholds.clone());
        })
        .is_some()
    };

    if !updated {
        warn!(
            "open volatility thresholds 已生成，但 decision 尚未初始化 (ns={} open={} hedge={})",
            ns,
            open_venue.data_pub_slug(),
            hedge_venue.data_pub_slug()
        );
    }

    info!(
        "open volatility thresholds 应用完成 ns={} rolling_key={} field_ref={} active_symbols={} rolling_symbols={} loaded_symbols={} missing_refs={} enabled={}",
        ns,
        rolling_key,
        field_ref,
        active_symbols.len(),
        rolling_payloads.len(),
        thresholds.len(),
        missing_refs,
        params.enable_volatility_limit
    );

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

fn return_model_score_thresholds_key(venue: TradingVenue) -> String {
    format!("return_model_score_thresholds_{}", venue.data_pub_slug())
}

#[derive(Debug, Clone, Default)]
struct StoredXarbMappingConfig {
    rolling_key: Option<String>,
    mapping: HashMap<String, String>,
}

fn default_xarb_spread_mapping() -> HashMap<String, String> {
    HashMap::from([
        ("forward_open_mm".to_string(), "spread_5".to_string()),
        ("forward_open_mt".to_string(), "bidask_10".to_string()),
        ("forward_cancel_mm".to_string(), "spread_10".to_string()),
        ("forward_cancel_mt".to_string(), "bidask_15".to_string()),
        ("backward_open_mm".to_string(), "spread_95".to_string()),
        ("backward_open_mt".to_string(), "askbid_90".to_string()),
        ("backward_cancel_mm".to_string(), "spread_90".to_string()),
        ("backward_cancel_mt".to_string(), "askbid_85".to_string()),
    ])
}

fn default_xarb_funding_mapping(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> HashMap<String, String> {
    if open_venue.is_futures() && hedge_venue.is_futures() {
        HashMap::from([
            ("forward_open_mm".to_string(), "spread_fr_80".to_string()),
            ("backward_open_mm".to_string(), "spread_fr_20".to_string()),
        ])
    } else {
        HashMap::from([
            (
                "forward_open_mm".to_string(),
                "hedge_premium_rate_50".to_string(),
            ),
            (
                "backward_open_mm".to_string(),
                "hedge_premium_rate_50".to_string(),
            ),
        ])
    }
}

fn xarb_spread_mapping_key(open_venue: TradingVenue, hedge_venue: TradingVenue) -> String {
    format!(
        "xarb_spread_thresholds_config_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn xarb_funding_mapping_key(open_venue: TradingVenue, hedge_venue: TradingVenue) -> String {
    format!(
        "xarb_funding_thresholds_config_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn default_rolling_thresholds_key(open_venue: TradingVenue, hedge_venue: TradingVenue) -> String {
    format!(
        "rolling_metrics_thresholds_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn normalize_threshold_mapping(mapping: HashMap<String, String>) -> HashMap<String, String> {
    mapping
        .into_iter()
        .filter_map(|(key, value)| {
            let k = key.trim().to_string();
            let v = value.trim().to_string();
            if k.is_empty() || v.is_empty() {
                None
            } else {
                Some((k, v))
            }
        })
        .collect()
}

fn parse_xarb_mapping_config(
    raw: Option<String>,
    default_mapping: HashMap<String, String>,
) -> StoredXarbMappingConfig {
    let Some(text) = raw else {
        return StoredXarbMappingConfig {
            rolling_key: None,
            mapping: default_mapping,
        };
    };

    let parsed: serde_json::Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(err) => {
            warn!("解析 xarb mapping config 失败，回退默认 mapping: {err}");
            return StoredXarbMappingConfig {
                rolling_key: None,
                mapping: default_mapping,
            };
        }
    };

    let rolling_key = parsed
        .get("rolling_key")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let mapping = parsed
        .get("mapping")
        .and_then(|value| value.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(key, value)| {
                    let raw = value.as_str()?.trim();
                    if raw.is_empty() {
                        None
                    } else {
                        Some((key.trim().to_string(), raw.to_string()))
                    }
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_else(|| default_mapping.clone());

    let mapping = normalize_threshold_mapping(mapping);
    StoredXarbMappingConfig {
        rolling_key,
        mapping: if mapping.is_empty() {
            default_mapping
        } else {
            mapping
        },
    }
}

fn normalize_xarb_symbol(symbol: &str) -> String {
    normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures).to_ascii_uppercase()
}

fn normalize_quantile(raw: &serde_json::Value) -> Option<f64> {
    let value = match raw {
        serde_json::Value::Number(number) => number.as_f64()?,
        serde_json::Value::String(text) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };

    let quantile = if value > 1.0 { value / 100.0 } else { value };
    if (0.0..=1.0).contains(&quantile) {
        Some(quantile)
    } else {
        None
    }
}

fn normalize_quantile_value(raw: &serde_json::Value) -> Option<f64> {
    let value = match raw {
        serde_json::Value::Number(number) => number.as_f64()?,
        serde_json::Value::String(text) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    if value.is_finite() {
        Some(value)
    } else {
        None
    }
}

fn extract_quantile_value(payload: &serde_json::Value, field_ref: &str) -> Option<f64> {
    let (factor, percentile_text) = field_ref.rsplit_once('_')?;
    let percentile = percentile_text.trim().parse::<f64>().ok()? / 100.0;
    let quantiles_key = format!("{}_quantiles", factor.trim());
    let quantiles = payload.get(&quantiles_key)?.as_array()?;

    for item in quantiles {
        let obj = item.as_object()?;
        let q_raw = obj.get("q").or_else(|| obj.get("quantile"))?;
        let q = normalize_quantile(q_raw)?;
        if (q - percentile).abs() >= 1e-9 {
            continue;
        }
        for value_key in ["v", "threshold", "value"] {
            if let Some(raw) = obj.get(value_key) {
                return normalize_quantile_value(raw);
            }
        }
    }

    None
}

fn parse_xarb_rolling_payloads(
    rolling_map: HashMap<String, String>,
    active_symbols: &HashSet<String>,
) -> HashMap<String, serde_json::Value> {
    let mut payloads = HashMap::new();

    for (field, raw_json) in rolling_map {
        let Ok(payload) = serde_json::from_str::<serde_json::Value>(&raw_json) else {
            continue;
        };
        let symbol = payload
            .get("base_symbol")
            .and_then(|value| value.as_str())
            .or_else(|| payload.get("symbol").and_then(|value| value.as_str()))
            .unwrap_or_else(|| field.split("::").last().unwrap_or(field.as_str()));
        let symbol_key = normalize_xarb_symbol(symbol);
        if symbol_key.is_empty() {
            continue;
        }
        if !active_symbols.is_empty() && !active_symbols.contains(&symbol_key) {
            continue;
        }
        payloads.insert(symbol_key, payload);
    }

    payloads
}

fn resolve_symbol_quantile_thresholds(
    rolling_payloads: &HashMap<String, serde_json::Value>,
    mapping: &HashMap<String, String>,
) -> (HashMap<String, HashMap<String, f64>>, usize, Vec<String>) {
    let mut resolved = HashMap::new();
    let mut missing_refs = 0usize;
    let mut skipped_symbols = Vec::new();

    for (symbol, payload) in rolling_payloads {
        let mut values = HashMap::new();
        let mut missing = false;
        for (dest_field, field_ref) in mapping {
            match extract_quantile_value(payload, field_ref) {
                Some(value) => {
                    values.insert(dest_field.clone(), value);
                }
                None => {
                    missing_refs += 1;
                    missing = true;
                    break;
                }
            }
        }

        if missing {
            skipped_symbols.push(symbol.clone());
            continue;
        }

        if !values.is_empty() {
            resolved.insert(symbol.clone(), values);
        }
    }

    (resolved, missing_refs, skipped_symbols)
}

fn apply_xarb_spread_thresholds(
    resolved: &HashMap<String, HashMap<String, f64>>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> usize {
    let spread_factor = super::spread_factor::SpreadFactor::instance();
    spread_factor.clear_thresholds();

    let mut applied = 0usize;
    for (symbol, values) in resolved {
        if let (Some(mm), Some(mt)) = (values.get("forward_open_mm"), values.get("forward_open_mt"))
        {
            spread_factor.set_forward_open_threshold(
                open_venue,
                symbol,
                hedge_venue,
                symbol,
                *mm,
                *mt,
            );
            applied += 1;
        }
        if let (Some(mm), Some(mt)) = (
            values.get("forward_cancel_mm"),
            values.get("forward_cancel_mt"),
        ) {
            spread_factor.set_forward_open_cancel_threshold(
                open_venue,
                symbol,
                hedge_venue,
                symbol,
                *mm,
                *mt,
            );
            applied += 1;
        }
        if let (Some(mm), Some(mt)) = (
            values.get("backward_open_mm"),
            values.get("backward_open_mt"),
        ) {
            spread_factor.set_backward_open_threshold(
                open_venue,
                symbol,
                hedge_venue,
                symbol,
                *mm,
                *mt,
            );
            applied += 1;
        }
        if let (Some(mm), Some(mt)) = (
            values.get("backward_cancel_mm"),
            values.get("backward_cancel_mt"),
        ) {
            spread_factor.set_backward_cancel_threshold(
                open_venue,
                symbol,
                hedge_venue,
                symbol,
                *mm,
                *mt,
            );
            applied += 1;
        }
    }

    applied
}

fn resolve_xarb_funding_thresholds(
    resolved: &HashMap<String, HashMap<String, f64>>,
) -> HashMap<String, XarbFundingThresholdsResolved> {
    resolved
        .iter()
        .filter_map(|(symbol, values)| {
            Some((
                symbol.clone(),
                XarbFundingThresholdsResolved {
                    forward_open: *values.get("forward_open_mm")?,
                    backward_open: *values.get("backward_open_mm")?,
                },
            ))
        })
        .collect()
}

async fn reload_xarb_thresholds_from_rolling(
    redis: &RedisSettings,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let spread_config_key = xarb_spread_mapping_key(open_venue, hedge_venue);
    let funding_config_key = xarb_funding_mapping_key(open_venue, hedge_venue);
    let default_rolling_key = default_rolling_thresholds_key(open_venue, hedge_venue);

    let active_symbols: HashSet<String> = SymbolList::instance()
        .get_online_symbols()
        .into_iter()
        .map(|symbol| normalize_xarb_symbol(&symbol))
        .filter(|symbol| !symbol.is_empty())
        .collect();

    let mut client = RedisClient::connect(redis.clone()).await?;
    let spread_config = parse_xarb_mapping_config(
        client.get_string(&spread_config_key).await?,
        default_xarb_spread_mapping(),
    );
    let funding_config = parse_xarb_mapping_config(
        client.get_string(&funding_config_key).await?,
        default_xarb_funding_mapping(open_venue, hedge_venue),
    );

    let rolling_key = spread_config
        .rolling_key
        .clone()
        .or_else(|| funding_config.rolling_key.clone())
        .unwrap_or(default_rolling_key);

    let rolling_map = client.hgetall_map(&rolling_key).await.with_context(|| {
        format!(
            "读取 xarb rolling metrics 失败 (key={} open={} hedge={})",
            rolling_key,
            open_venue.data_pub_slug(),
            hedge_venue.data_pub_slug()
        )
    })?;
    if rolling_map.is_empty() {
        anyhow::bail!("xarb rolling metrics hash '{}' 为空", rolling_key);
    }

    let rolling_payloads = parse_xarb_rolling_payloads(rolling_map, &active_symbols);
    let (resolved_spread, spread_missing_refs, spread_skipped) =
        resolve_symbol_quantile_thresholds(&rolling_payloads, &spread_config.mapping);
    let (resolved_funding, funding_missing_refs, funding_skipped) =
        resolve_symbol_quantile_thresholds(&rolling_payloads, &funding_config.mapping);

    let spread_applied = apply_xarb_spread_thresholds(&resolved_spread, open_venue, hedge_venue);
    let funding_thresholds = resolve_xarb_funding_thresholds(&resolved_funding);
    let funding_symbols = funding_thresholds.len();

    let updated = XarbDecision::try_with_mut(|decision| {
        decision.update_funding_open_thresholds(funding_thresholds);
    });
    if updated.is_none() {
        warn!(
            "xarb funding thresholds 已生成，但 XarbDecision 尚未初始化 (open={} hedge={})",
            open_venue.data_pub_slug(),
            hedge_venue.data_pub_slug()
        );
    }

    info!(
        "xarb rolling thresholds 应用完成 rolling_key={} active_symbols={} rolling_symbols={} spread_cfg_fields={} spread_applied={} spread_skipped={} spread_missing_refs={} funding_cfg_fields={} funding_symbols={} funding_skipped={} funding_missing_refs={}",
        rolling_key,
        active_symbols.len(),
        rolling_payloads.len(),
        spread_config.mapping.len(),
        spread_applied,
        spread_skipped.len(),
        spread_missing_refs,
        funding_config.mapping.len(),
        funding_symbols,
        funding_skipped.len(),
        funding_missing_refs
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        default_xarb_funding_mapping, extract_quantile_value, normalize_xarb_symbol,
        parse_xarb_mapping_config,
    };
    use crate::signal::common::TradingVenue;
    use std::collections::HashMap;

    #[test]
    fn xarb_extract_quantile_value_supports_factor_refs_with_underscores() {
        let payload = serde_json::json!({
            "spread_fr_quantiles": [
                {"q": 0.2, "v": -0.001},
                {"q": 0.8, "v": 0.003}
            ]
        });
        let value = extract_quantile_value(&payload, "spread_fr_80").expect("spread_fr_80");
        assert!((value - 0.003).abs() < 1e-12);
    }

    #[test]
    fn xarb_default_funding_mapping_depends_on_venues() {
        let fut_fut =
            default_xarb_funding_mapping(TradingVenue::BinanceFutures, TradingVenue::OkexFutures);
        assert_eq!(
            fut_fut.get("forward_open_mm").map(String::as_str),
            Some("spread_fr_80")
        );
        assert_eq!(
            fut_fut.get("backward_open_mm").map(String::as_str),
            Some("spread_fr_20")
        );

        let margin_fut =
            default_xarb_funding_mapping(TradingVenue::BinanceMargin, TradingVenue::BinanceFutures);
        assert_eq!(
            margin_fut.get("forward_open_mm").map(String::as_str),
            Some("hedge_premium_rate_50")
        );
        assert_eq!(
            margin_fut.get("backward_open_mm").map(String::as_str),
            Some("hedge_premium_rate_50")
        );
    }

    #[test]
    fn xarb_mapping_config_falls_back_to_default_when_empty() {
        let defaults = HashMap::from([("forward_open_mm".to_string(), "spread_5".to_string())]);
        let parsed = parse_xarb_mapping_config(
            Some(r#"{"schema_version":1,"mapping":{}}"#.to_string()),
            defaults.clone(),
        );
        assert_eq!(parsed.mapping, defaults);
    }

    #[test]
    fn xarb_symbol_normalization_matches_runtime() {
        assert_eq!(normalize_xarb_symbol("BTC-USDT-SWAP"), "BTCUSDT");
    }
}
