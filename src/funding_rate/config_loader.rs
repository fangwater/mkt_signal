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

use super::arb_decision::ArbDecision;
use super::fr_threshold_loader::load_from_redis as load_fr_thresholds;
use super::mm_decision::MmDecision;
use super::rolling_threshold_sync::{
    alias_single_side_payloads, apply_xarb_spread_thresholds, default_fr_spread_mapping,
    default_single_side_rolling_key, default_xarb_spread_mapping, factor_chain_to_funding_mapping,
    format_quantile_field_ref, funding_chain_config_key, merge_rolling_payloads,
    normalize_xarb_symbol, parse_funding_chain_config, parse_plain_mapping_config,
    parse_xarb_mapping_config, parse_xarb_rolling_payloads, resolve_funding_thresholds,
    resolve_symbol_quantile_thresholds, resolve_symbol_single_quantile_thresholds,
    xarb_spread_mapping_key,
};
use super::strategy_loader::StrategyParams;
use super::symbol_list::SymbolList;

const DEFAULT_NAMESPACE: &str = "fr";
const OPEN_VOL_THRESHOLD_REDIS_REFRESH_SECS: u64 = 180;

#[derive(Debug, Clone)]
struct RedisHashCacheEntry {
    fields: HashMap<String, String>,
    fetched_at: Instant,
}

thread_local! {
    static OPEN_VOL_THRESHOLD_CACHE: RefCell<HashMap<String, RedisHashCacheEntry>> =
        RefCell::new(HashMap::new());
}

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
    // 1. 加载策略参数 -> ArbDecision + SpreadFactor
    reload_strategy_params(redis, namespace, open_venue, hedge_venue).await?;

    // 2. 更新 SymbolList（建仓/平仓列表）
    reload_symbol_list(redis, namespace, symbol_key_suffix, open_venue, hedge_venue).await?;

    // 3. 动态阈值统一走 rolling + mapping，静态 funding 阈值单独直读。
    reload_dynamic_thresholds(redis, namespace, open_venue, hedge_venue).await?;
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
            let ns = normalize_namespace(namespace);
            let env_dir = if ns == "fr" {
                Some(fr_env_dir_or_panic())
            } else {
                None
            };
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
                .reload_from_redis_with_key_prefix(
                    &mut client,
                    &suffix,
                    namespace,
                    env_dir.as_deref(),
                )
                .await?;
            info!(
                "SymbolList 重载成功 (ns={} suffix={} env_prefix={})",
                namespace,
                suffix,
                env_dir.as_deref().unwrap_or("-")
            );
        }
        Err(err) => {
            warn!("SymbolList 重载失败: {:?}", err);
        }
    }
    Ok(())
}

async fn reload_dynamic_thresholds(
    redis: &RedisSettings,
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let ns = normalize_namespace(namespace);
    if ns != "fr" && ns != "intra" && ns != "cross" {
        if ns == "mm" {
            reload_open_volatility_thresholds(redis, namespace, open_venue, hedge_venue).await?;
        }
        return Ok(());
    }

    match ns.as_str() {
        "intra" | "cross" => {
            reload_spread_thresholds_from_rolling(redis, &ns, open_venue, hedge_venue).await?
        }
        "fr" => reload_fr_dynamic_thresholds_from_rolling(redis, open_venue, hedge_venue).await?,
        _ => {}
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
    let ns = normalize_namespace(namespace);
    if ns != "fr" {
        return Ok(());
    }

    let env_dir = fr_env_dir_or_panic();
    let redis_key = funding_thresholds_key(&env_dir, open_venue, hedge_venue);

    match RedisClient::connect(redis.clone()).await {
        Ok(mut client) => {
            let funding_map = match client.hgetall_map(&redis_key).await {
                Ok(map) => map,
                Err(err) => {
                    panic!(
                        "读取 Redis Hash '{}' 失败，无法加载资金费率阈值 (env_dir={}): {:?}",
                        redis_key, env_dir, err
                    );
                }
            };
            if funding_map.is_empty() {
                panic!(
                    "Redis hash '{}' 为空或不存在，无法加载资金费率阈值 (env_dir={})",
                    redis_key, env_dir
                );
            }
            load_fr_thresholds(funding_map)
                .with_context(|| format!("解析资金费率阈值失败 (key: {})", redis_key))?;
            info!(
                "资金费率阈值重载成功 (key: {}, env_dir: {})",
                redis_key, env_dir
            );
        }
        Err(err) => {
            panic!(
                "连接 Redis 失败，无法加载资金费率阈值 (key: {}, env_dir: {}): {:?}",
                redis_key, env_dir, err
            );
        }
    }
    Ok(())
}

async fn reload_open_volatility_thresholds(
    redis: &RedisSettings,
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let ns = normalize_namespace(namespace);
    if ns != "mm" && ns != "intra" && ns != "cross" && ns != "fr" {
        return Ok(());
    }

    // MM open-volatility gating has moved to inline sampling in MmDecision and no longer
    // depends on rolling_metrics Redis hashes. Skip the legacy rolling bootstrap path to avoid
    // noisy warnings about missing rolling_metrics_params_* keys.
    if ns == "mm" {
        debug!(
            "skip legacy open volatility rolling bootstrap for mm namespace (open={} hedge={})",
            open_venue.data_pub_slug(),
            hedge_venue.data_pub_slug()
        );
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

    let rolling_key = default_single_side_rolling_key(open_venue);
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
    let mut rolling_payloads = parse_xarb_rolling_payloads(rolling_map, &active_symbols);
    alias_single_side_payloads(&mut rolling_payloads, open_venue, "open");
    let field_ref = format_quantile_field_ref("open_vol", params.open_volatility_limit);
    let (_thresholds, missing_refs) =
        resolve_symbol_single_quantile_thresholds(&rolling_payloads, &field_ref);

    let updated = if ns == "mm" {
        MmDecision::is_initialized()
    } else {
        ArbDecision::with_state_mut(|_| {}).is_some()
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
        active_symbols.len().saturating_sub(missing_refs),
        missing_refs,
        params.enable_volatility_limit
    );

    Ok(())
}

fn fr_spread_mapping_key(open_venue: TradingVenue, hedge_venue: TradingVenue) -> String {
    format!(
        "fr_spread_threshold_mapping_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn funding_thresholds_key(
    env_dir: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> String {
    format!(
        "{env_dir}:funding_rate_thresholds_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

/// 从 CWD basename 推断 FR 部署 env 名（如 `binance_fr_arb01`）。
/// 用于构造 env-aware 的资金费率阈值 Redis key（与 `pre_trade.rs` 的 `infer_dir_prefix_from_cwd` 一致）。
fn fr_env_dir_or_panic() -> String {
    std::env::current_dir()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            panic!(
                "FR static thresholds require running under <exchange>_fr_<env> CWD; \
                 could not infer env_dir from current_dir()"
            )
        })
}

fn default_rolling_thresholds_key(open_venue: TradingVenue, hedge_venue: TradingVenue) -> String {
    format!(
        "rolling_metrics_thresholds_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

async fn reload_spread_thresholds_from_rolling(
    redis: &RedisSettings,
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let spread_config_key = xarb_spread_mapping_key(namespace, open_venue, hedge_venue);
    let funding_chain_key = funding_chain_config_key(namespace, open_venue, hedge_venue);
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
    let funding_config = parse_funding_chain_config(client.get_string(&funding_chain_key).await?);
    let funding_mapping = factor_chain_to_funding_mapping(&funding_config.factor_chain);

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
        anyhow::bail!("{} rolling metrics hash '{}' 为空", namespace, rolling_key);
    }

    let pair_payloads = parse_xarb_rolling_payloads(rolling_map, &active_symbols);
    let mut open_payloads = match client
        .hgetall_map(&default_single_side_rolling_key(open_venue))
        .await
    {
        Ok(map) => parse_xarb_rolling_payloads(map, &active_symbols),
        Err(err) => {
            warn!(
                "读取 open 单边 rolling metrics 失败 (key={} open={}): {:?}",
                default_single_side_rolling_key(open_venue),
                open_venue.data_pub_slug(),
                err
            );
            HashMap::new()
        }
    };
    alias_single_side_payloads(&mut open_payloads, open_venue, "open");

    let mut hedge_payloads = match client
        .hgetall_map(&default_single_side_rolling_key(hedge_venue))
        .await
    {
        Ok(map) => parse_xarb_rolling_payloads(map, &active_symbols),
        Err(err) => {
            warn!(
                "读取 hedge 单边 rolling metrics 失败 (key={} hedge={}): {:?}",
                default_single_side_rolling_key(hedge_venue),
                hedge_venue.data_pub_slug(),
                err
            );
            HashMap::new()
        }
    };
    alias_single_side_payloads(&mut hedge_payloads, hedge_venue, "hedge");

    let rolling_payloads =
        merge_rolling_payloads(vec![pair_payloads, open_payloads, hedge_payloads]);
    let (resolved_spread, spread_missing_refs, spread_skipped) =
        resolve_symbol_quantile_thresholds(&rolling_payloads, &spread_config.mapping);
    let (resolved_funding, funding_missing_refs, funding_skipped) =
        resolve_symbol_quantile_thresholds(&rolling_payloads, &funding_mapping);

    let spread_applied = apply_xarb_spread_thresholds(&resolved_spread, open_venue, hedge_venue);
    let funding_thresholds = resolve_funding_thresholds(&resolved_funding);
    let funding_symbols = funding_thresholds.len();

    let updated = ArbDecision::with_state_mut(|arb| {
        arb.enable_funding_open_filter = funding_config.enabled;
        arb.funding_open_thresholds = funding_thresholds;
        arb.funding_factor_chain = funding_config.factor_chain.clone();
    });
    if updated.is_none() {
        warn!(
            "{} funding thresholds 已生成，但 ArbDecision 尚未初始化 (open={} hedge={})",
            namespace,
            open_venue.data_pub_slug(),
            hedge_venue.data_pub_slug()
        );
    }

    info!(
        "{} rolling thresholds 应用完成 rolling_key={} active_symbols={} rolling_symbols={} spread_cfg_fields={} spread_applied={} spread_skipped={} spread_missing_refs={} funding_chain_len={} funding_chain_enabled={} funding_symbols={} funding_skipped={} funding_missing_refs={}",
        namespace,
        rolling_key,
        active_symbols.len(),
        rolling_payloads.len(),
        spread_config.mapping.len(),
        spread_applied,
        spread_skipped.len(),
        spread_missing_refs,
        funding_config.factor_chain.len(),
        funding_config.factor_chain.iter().filter(|e| e.enabled).count(),
        funding_symbols,
        funding_skipped.len(),
        funding_missing_refs,
    );

    Ok(())
}

async fn reload_fr_dynamic_thresholds_from_rolling(
    redis: &RedisSettings,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let spread_config_key = fr_spread_mapping_key(open_venue, hedge_venue);
    let funding_chain_key = funding_chain_config_key("fr", open_venue, hedge_venue);
    let default_rolling_key = default_rolling_thresholds_key(open_venue, hedge_venue);
    let active_symbols: HashSet<String> = SymbolList::instance()
        .get_online_symbols()
        .into_iter()
        .map(|symbol| normalize_xarb_symbol(&symbol))
        .filter(|symbol| !symbol.is_empty())
        .collect();

    let mut client = RedisClient::connect(redis.clone()).await?;
    let spread_config = parse_plain_mapping_config(
        client
            .hgetall_map(&spread_config_key)
            .await
            .unwrap_or_default(),
        default_fr_spread_mapping(),
    );
    let funding_config =
        parse_funding_chain_config(client.get_string(&funding_chain_key).await.unwrap_or(None));
    let funding_mapping = factor_chain_to_funding_mapping(&funding_config.factor_chain);
    let rolling_key = funding_config
        .rolling_key
        .clone()
        .unwrap_or(default_rolling_key);
    let rolling_map = client.hgetall_map(&rolling_key).await.with_context(|| {
        format!(
            "读取 fr rolling metrics 失败 (key={} open={} hedge={})",
            rolling_key,
            open_venue.data_pub_slug(),
            hedge_venue.data_pub_slug()
        )
    })?;
    if rolling_map.is_empty() {
        anyhow::bail!("fr rolling metrics hash '{}' 为空", rolling_key);
    }

    let pair_payloads = parse_xarb_rolling_payloads(rolling_map, &active_symbols);
    let mut open_payloads = match client
        .hgetall_map(&default_single_side_rolling_key(open_venue))
        .await
    {
        Ok(map) => parse_xarb_rolling_payloads(map, &active_symbols),
        Err(err) => {
            warn!(
                "读取 fr open 单边 rolling metrics 失败 (key={} open={}): {:?}",
                default_single_side_rolling_key(open_venue),
                open_venue.data_pub_slug(),
                err
            );
            HashMap::new()
        }
    };
    alias_single_side_payloads(&mut open_payloads, open_venue, "open");

    let mut hedge_payloads = match client
        .hgetall_map(&default_single_side_rolling_key(hedge_venue))
        .await
    {
        Ok(map) => parse_xarb_rolling_payloads(map, &active_symbols),
        Err(err) => {
            warn!(
                "读取 fr hedge 单边 rolling metrics 失败 (key={} hedge={}): {:?}",
                default_single_side_rolling_key(hedge_venue),
                hedge_venue.data_pub_slug(),
                err
            );
            HashMap::new()
        }
    };
    alias_single_side_payloads(&mut hedge_payloads, hedge_venue, "hedge");

    let rolling_payloads =
        merge_rolling_payloads(vec![pair_payloads, open_payloads, hedge_payloads]);
    let (resolved_spread, spread_missing_refs, spread_skipped) =
        resolve_symbol_quantile_thresholds(&rolling_payloads, &spread_config.mapping);
    let (resolved_funding, funding_missing_refs, funding_skipped) =
        resolve_symbol_quantile_thresholds(&rolling_payloads, &funding_mapping);

    let spread_applied = apply_xarb_spread_thresholds(&resolved_spread, open_venue, hedge_venue);
    let funding_thresholds = resolve_funding_thresholds(&resolved_funding);
    let funding_symbols = funding_thresholds.len();

    let updated = ArbDecision::with_state_mut(|arb| {
        arb.enable_funding_open_filter = funding_config.enabled;
        arb.funding_open_thresholds = funding_thresholds;
        arb.funding_factor_chain = funding_config.factor_chain.clone();
    });
    if updated.is_none() {
        warn!(
            "fr dynamic thresholds 已生成，但 ArbDecision 尚未初始化 (open={} hedge={})",
            open_venue.data_pub_slug(),
            hedge_venue.data_pub_slug()
        );
    }

    info!(
        "fr rolling thresholds 应用完成 rolling_key={} active_symbols={} rolling_symbols={} spread_cfg_fields={} spread_applied={} spread_skipped={} spread_missing_refs={} funding_chain_len={} funding_chain_enabled={} funding_symbols={} funding_skipped={} funding_missing_refs={}",
        rolling_key,
        active_symbols.len(),
        rolling_payloads.len(),
        spread_config.mapping.len(),
        spread_applied,
        spread_skipped.len(),
        spread_missing_refs,
        funding_config.factor_chain.len(),
        funding_config.factor_chain.iter().filter(|e| e.enabled).count(),
        funding_symbols,
        funding_skipped.len(),
        funding_missing_refs,
    );

    Ok(())
}
