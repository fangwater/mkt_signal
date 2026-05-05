use anyhow::Result;
use log::{debug, info, warn};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::signal::common::TradingVenue;

/// Redis Key 配置
const REDIS_KEY_RISK_PARAMS: &str = "pre_trade_risk_params";

/// 后台刷新间隔（固定 60 秒）
const REFRESH_INTERVAL_SECS: u64 = 60;

/// 从 Redis 加载的 Pre-Trade 风控参数（内部数据结构）
#[derive(Debug, Clone)]
struct PreTradeParamsData {
    max_pos_u: f64,
    max_pos_u_overrides: HashMap<String, f64>,
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    max_leverage: f64,
    max_pending_limit_orders: i32,
    max_pending_limit_buy_orders: i32,
    max_pending_limit_sell_orders: i32,
    open_order_rate_limit_per_min: i32,
    open_order_rate_limit_10s: i32,
    hedge_order_rate_limit_per_min: i32,
    hedge_order_rate_limit_10s: i32,
    arb_max_pending_limit_buy_orders: i32,
    arb_max_pending_limit_sell_orders: i32,
    arb_open_order_rate_limit_per_min: i32,
    arb_open_order_rate_limit_10s: i32,
    arb_hedge_order_rate_limit_per_min: i32,
    arb_hedge_order_rate_limit_10s: i32,
}

impl Default for PreTradeParamsData {
    fn default() -> Self {
        Self {
            max_pos_u: 1000.0,
            max_pos_u_overrides: HashMap::new(),
            max_symbol_exposure_ratio: 0.8,
            max_total_exposure_ratio: 1.0,
            max_leverage: 3.0,
            max_pending_limit_orders: 3,
            max_pending_limit_buy_orders: 0,
            max_pending_limit_sell_orders: 0,
            open_order_rate_limit_per_min: 0,
            open_order_rate_limit_10s: 0,
            hedge_order_rate_limit_per_min: 0,
            hedge_order_rate_limit_10s: 0,
            arb_max_pending_limit_buy_orders: 0,
            arb_max_pending_limit_sell_orders: 0,
            arb_open_order_rate_limit_per_min: 0,
            arb_open_order_rate_limit_10s: 0,
            arb_hedge_order_rate_limit_per_min: 0,
            arb_hedge_order_rate_limit_10s: 0,
        }
    }
}

/// Pre-Trade 参数加载器单例（thread-local 单线程版本）
pub struct PreTradeParamsLoader;

thread_local! {
    static PARAMS_DATA: RefCell<PreTradeParamsData> = RefCell::new(PreTradeParamsData::default());
}

/// 应急写回 Redis 所需的运行时上下文（settings + 完整 hash key）。
/// 在 `load_from_redis` 首次成功加载后由 OnceLock 兜底设置一次，供 `set_max_leverage_async` 使用。
#[derive(Debug, Clone)]
struct RedisWritebackContext {
    settings: RedisSettings,
    risk_params_full_key: String,
}

static REDIS_WRITEBACK: OnceLock<RedisWritebackContext> = OnceLock::new();

fn risk_params_full_key(redis: &RedisSettings) -> String {
    match redis.prefix.as_deref() {
        Some(prefix) if !prefix.is_empty() => format!("{prefix}{REDIS_KEY_RISK_PARAMS}"),
        _ => REDIS_KEY_RISK_PARAMS.to_string(),
    }
}

fn mm_max_pos_u_override_key(env_name: Option<&str>, open_venue: TradingVenue) -> Option<String> {
    let env_name = env_name.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!(
        "{env_name}:{}:mm:max_pos_u",
        open_venue.data_pub_slug()
    ))
}

/// Arb（intra/cross/fr）共用的 per-symbol max_pos_u 覆盖键。
/// 由 sync_*_max_pos_u.py 写入 Redis STRING(JSON {symbol: max_pos_u})。
fn arb_max_pos_u_override_key(
    env_name: Option<&str>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<String> {
    let env_name = env_name.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!(
        "{env_name}:{}:{}:max_pos_u_overrides",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    ))
}

fn parse_max_pos_u_overrides(
    raw: &str,
    open_venue: TradingVenue,
    redis_key: &str,
) -> HashMap<String, f64> {
    let parsed: HashMap<String, f64> = serde_json::from_str(raw).unwrap_or_else(|err| {
        panic!(
            "Redis string '{}' 不是合法 JSON(symbol->max_pos_u): {} ({})",
            redis_key, raw, err
        )
    });

    let mut normalized = HashMap::new();
    for (symbol, max_pos_u) in parsed {
        let symbol_trimmed = symbol.trim();
        if symbol_trimmed.is_empty() {
            panic!("Redis string '{}' 包含空 symbol", redis_key);
        }
        if !(max_pos_u.is_finite() && max_pos_u > 0.0) {
            panic!(
                "Redis string '{}' symbol={} max_pos_u 非法: {}",
                redis_key, symbol_trimmed, max_pos_u
            );
        }
        let symbol_key = normalize_symbol_for_venue(symbol_trimmed, open_venue);
        normalized.insert(symbol_key, max_pos_u);
    }
    normalized
}

fn resolve_max_pos_u_for_symbol(
    default_max_pos_u: f64,
    overrides: &HashMap<String, f64>,
    open_venue: TradingVenue,
    symbol: &str,
) -> f64 {
    let symbol_key = normalize_symbol_for_venue(symbol, open_venue);
    overrides
        .get(&symbol_key)
        .copied()
        .unwrap_or(default_max_pos_u)
}

impl PreTradeParamsLoader {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        PreTradeParamsLoader
    }

    /// 从 Redis 加载参数并更新单例
    pub async fn load_from_redis(
        &self,
        redis: &RedisSettings,
        env_name: Option<&str>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<()> {
        let risk_key = risk_params_full_key(redis);
        let mut raw_settings = redis.clone();
        raw_settings.prefix = None;
        // 首次加载成功后缓存 settings + 完整 key，供后续应急写回（idempotent，set 失败忽略）。
        let _ = REDIS_WRITEBACK.set(RedisWritebackContext {
            settings: raw_settings.clone(),
            risk_params_full_key: risk_key.clone(),
        });
        let mut client = RedisClient::connect(raw_settings).await?;
        let hash_map = client.hgetall_map(&risk_key).await?;
        if hash_map.is_empty() {
            anyhow::bail!(
                "risk params hash not found or empty: key='{}' (prefix={:?})",
                risk_key,
                redis.prefix.as_deref()
            );
        }

        debug!(
            "risk params loaded from redis key='{}' (prefix={:?})",
            risk_key,
            redis.prefix.as_deref()
        );

        // 同时尝试 MM 与 arb 两套 key；二者命名空间不同（MM 用 hedge=open=futures、key
        // `<env>:<venue>:mm:max_pos_u`；arb 用 `<env>:<open>:<hedge>:max_pos_u_overrides`），
        // 实际部署里只会有一套 key 存在，合并到同一个 overrides map 即可。
        let mut max_pos_u_overrides: HashMap<String, f64> = HashMap::new();
        if let Some(override_key) = mm_max_pos_u_override_key(env_name, open_venue) {
            if let Some(raw) = client.get_string(&override_key).await? {
                let parsed = parse_max_pos_u_overrides(&raw, open_venue, &override_key);
                debug!(
                    "max_pos_u overrides loaded key='{}' symbols={}",
                    override_key,
                    parsed.len()
                );
                max_pos_u_overrides.extend(parsed);
            }
        }
        if let Some(override_key) = arb_max_pos_u_override_key(env_name, open_venue, hedge_venue) {
            if let Some(raw) = client.get_string(&override_key).await? {
                let parsed = parse_max_pos_u_overrides(&raw, open_venue, &override_key);
                debug!(
                    "arb max_pos_u overrides loaded key='{}' symbols={}",
                    override_key,
                    parsed.len()
                );
                max_pos_u_overrides.extend(parsed);
            }
        }

        let parse_f64 =
            |k: &str| -> Option<f64> { hash_map.get(k).and_then(|v| v.parse::<f64>().ok()) };
        let parse_i64 =
            |k: &str| -> Option<i64> { hash_map.get(k).and_then(|v| v.parse::<i64>().ok()) };

        PARAMS_DATA.with(|data| {
            let mut data = data.borrow_mut();

            if let Some(v) = parse_f64("max_pos_u") {
                data.max_pos_u = v;
            }
            data.max_pos_u_overrides = max_pos_u_overrides.clone();

            if let Some(v) = parse_f64("max_symbol_exposure_ratio") {
                data.max_symbol_exposure_ratio = v;
            }

            if let Some(v) = parse_f64("max_total_exposure_ratio") {
                data.max_total_exposure_ratio = v;
            }

            if let Some(v) = parse_f64("max_leverage") {
                if v > 0.0 {
                    data.max_leverage = v;
                } else {
                    warn!("max_leverage={} 无效，需大于 0，忽略更新", v);
                }
            }

            if let Some(v) = parse_i64("max_pending_limit_orders") {
                data.max_pending_limit_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("max_pending_limit_buy_orders") {
                data.max_pending_limit_buy_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("max_pending_limit_sell_orders") {
                data.max_pending_limit_sell_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("open_order_rate_limit_per_min") {
                data.open_order_rate_limit_per_min = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("open_order_rate_limit_10s") {
                data.open_order_rate_limit_10s = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("hedge_order_rate_limit_per_min") {
                data.hedge_order_rate_limit_per_min = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("hedge_order_rate_limit_10s") {
                data.hedge_order_rate_limit_10s = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_max_pending_limit_buy_orders") {
                data.arb_max_pending_limit_buy_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_max_pending_limit_sell_orders") {
                data.arb_max_pending_limit_sell_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_open_order_rate_limit_per_min") {
                data.arb_open_order_rate_limit_per_min = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_open_order_rate_limit_10s") {
                data.arb_open_order_rate_limit_10s = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_hedge_order_rate_limit_per_min") {
                data.arb_hedge_order_rate_limit_per_min = v.max(0) as i32;
            }

            if let Some(v) = parse_i64("arb_hedge_order_rate_limit_10s") {
                data.arb_hedge_order_rate_limit_10s = v.max(0) as i32;
            }

            debug!(
                "风控参数已加载: max_pos_u={:.2} overrides={} sym_ratio={:.4} total_ratio={:.4} max_leverage={:.2} max_pending={} max_pending_buy={} max_pending_sell={} open_rate_1m={} open_rate_10s={} hedge_rate_1m={} hedge_rate_10s={} arb_max_pending_buy={} arb_max_pending_sell={} arb_open_rate_1m={} arb_open_rate_10s={} arb_hedge_rate_1m={} arb_hedge_rate_10s={}",
                data.max_pos_u,
                data.max_pos_u_overrides.len(),
                data.max_symbol_exposure_ratio,
                data.max_total_exposure_ratio,
                data.max_leverage,
                data.max_pending_limit_orders,
                data.max_pending_limit_buy_orders,
                data.max_pending_limit_sell_orders,
                data.open_order_rate_limit_per_min,
                data.open_order_rate_limit_10s,
                data.hedge_order_rate_limit_per_min,
                data.hedge_order_rate_limit_10s,
                data.arb_max_pending_limit_buy_orders,
                data.arb_max_pending_limit_sell_orders,
                data.arb_open_order_rate_limit_per_min,
                data.arb_open_order_rate_limit_10s,
                data.arb_hedge_order_rate_limit_per_min,
                data.arb_hedge_order_rate_limit_10s
            );
        });

        Ok(())
    }

    /// 启动后台刷新任务（固定 60 秒间隔）
    pub fn start_background_refresh(
        redis: RedisSettings,
        env_name: Option<String>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) {
        tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(REFRESH_INTERVAL_SECS));
            let loader = PreTradeParamsLoader::instance();

            loop {
                interval.tick().await;

                match loader
                    .load_from_redis(&redis, env_name.as_deref(), open_venue, hedge_venue)
                    .await
                {
                    Ok(_) => {
                        debug!("风控参数后台刷新成功");
                    }
                    Err(e) => {
                        warn!("风控参数后台刷新失败: {:#}", e);
                    }
                }
            }
        });

        info!("后台参数刷新任务已启动 (间隔: {}s)", REFRESH_INTERVAL_SECS);
    }

    /// 打印参数三线表
    pub fn print_params_table(&self) {
        let data = PARAMS_DATA.with(|d| d.borrow().clone());

        let separator = "=".repeat(60);
        let mid_separator = "-".repeat(60);

        println!("\n{}", separator);
        println!("{:<40} {:>18}", "Parameter", "Value");
        println!("{}", mid_separator);
        println!("{:<40} {:>18.2}", "max_pos_u", data.max_pos_u);
        println!(
            "{:<40} {:>18.4}",
            "max_symbol_exposure_ratio", data.max_symbol_exposure_ratio
        );
        println!(
            "{:<40} {:>18.4}",
            "max_total_exposure_ratio", data.max_total_exposure_ratio
        );
        println!("{:<40} {:>18.2}", "max_leverage", data.max_leverage);
        println!(
            "{:<40} {:>18}",
            "max_pending_limit_orders", data.max_pending_limit_orders
        );
        println!(
            "{:<40} {:>18}",
            "max_pending_limit_buy_orders", data.max_pending_limit_buy_orders
        );
        println!(
            "{:<40} {:>18}",
            "max_pending_limit_sell_orders", data.max_pending_limit_sell_orders
        );
        println!(
            "{:<40} {:>18}",
            "open_order_rate_limit_per_min", data.open_order_rate_limit_per_min
        );
        println!(
            "{:<40} {:>18}",
            "open_order_rate_limit_10s", data.open_order_rate_limit_10s
        );
        println!(
            "{:<40} {:>18}",
            "hedge_order_rate_limit_per_min", data.hedge_order_rate_limit_per_min
        );
        println!(
            "{:<40} {:>18}",
            "hedge_order_rate_limit_10s", data.hedge_order_rate_limit_10s
        );
        println!(
            "{:<40} {:>18}",
            "arb_max_pending_limit_buy_orders", data.arb_max_pending_limit_buy_orders
        );
        println!(
            "{:<40} {:>18}",
            "arb_max_pending_limit_sell_orders", data.arb_max_pending_limit_sell_orders
        );
        println!(
            "{:<40} {:>18}",
            "arb_open_order_rate_limit_per_min", data.arb_open_order_rate_limit_per_min
        );
        println!(
            "{:<40} {:>18}",
            "arb_open_order_rate_limit_10s", data.arb_open_order_rate_limit_10s
        );
        println!(
            "{:<40} {:>18}",
            "arb_hedge_order_rate_limit_per_min", data.arb_hedge_order_rate_limit_per_min
        );
        println!(
            "{:<40} {:>18}",
            "arb_hedge_order_rate_limit_10s", data.arb_hedge_order_rate_limit_10s
        );
        println!("{}", separator);
    }

    /// 获取 max_pos_u
    pub fn max_pos_u(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().max_pos_u)
    }

    pub fn max_pos_u_for_symbol(&self, open_venue: TradingVenue, symbol: &str) -> f64 {
        PARAMS_DATA.with(|data| {
            let data = data.borrow();
            resolve_max_pos_u_for_symbol(
                data.max_pos_u,
                &data.max_pos_u_overrides,
                open_venue,
                symbol,
            )
        })
    }

    /// 获取 max_symbol_exposure_ratio
    pub fn max_symbol_exposure_ratio(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().max_symbol_exposure_ratio)
    }

    /// 获取 max_total_exposure_ratio
    pub fn max_total_exposure_ratio(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().max_total_exposure_ratio)
    }

    /// 获取 max_leverage
    pub fn max_leverage(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().max_leverage)
    }

    /// 应急下调 max_leverage：立即更新本进程 thread-local 缓存（不等 60s 热加载），
    /// 同时 spawn 一个 tokio 任务把新值写回 Redis 风控 hash。
    /// Redis 写失败仅 warn，不影响本进程运行（下次 60s 热加载会按 Redis 实际值刷新）。
    /// 必须在 tokio runtime 上下文里调用（pre_trade 主循环已是 LocalSet）。
    pub fn set_max_leverage_async(&self, new_value: f64) {
        if !new_value.is_finite() || new_value <= 0.0 {
            warn!(
                "set_max_leverage_async ignored non-positive value {}",
                new_value
            );
            return;
        }
        // 1. 立即生效到本进程
        PARAMS_DATA.with(|data| data.borrow_mut().max_leverage = new_value);

        // 2. 异步写回 Redis；OnceLock 未初始化（理论上 load_from_redis 之前不应被调）说明启动序失常
        let Some(ctx) = REDIS_WRITEBACK.get() else {
            warn!(
                "set_max_leverage_async: REDIS_WRITEBACK not initialized, skip redis writeback (new_value={:.1})",
                new_value
            );
            return;
        };
        let settings = ctx.settings.clone();
        let key = ctx.risk_params_full_key.clone();
        let value = format!("{:.1}", new_value);
        tokio::task::spawn_local(async move {
            match RedisClient::connect(settings).await {
                Ok(mut client) => {
                    if let Err(e) = client
                        .hset_multiple_str(&key, &[("max_leverage".to_string(), value.clone())])
                        .await
                    {
                        warn!(
                            "set_max_leverage_async redis HSET failed key={} max_leverage={} err={:#}",
                            key, value, e
                        );
                    } else {
                        info!(
                            "set_max_leverage_async redis HSET ok key={} max_leverage={}",
                            key, value
                        );
                    }
                }
                Err(e) => warn!("set_max_leverage_async redis connect failed: {:#}", e),
            }
        });
    }

    /// 获取 max_pending_limit_orders
    pub fn max_pending_limit_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().max_pending_limit_orders)
    }

    /// 获取 max_pending_limit_buy_orders
    pub fn max_pending_limit_buy_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().max_pending_limit_buy_orders)
    }

    /// 获取 max_pending_limit_sell_orders
    pub fn max_pending_limit_sell_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().max_pending_limit_sell_orders)
    }

    /// 获取 open_order_rate_limit_per_min
    pub fn open_order_rate_limit_per_min(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().open_order_rate_limit_per_min)
    }

    /// 获取 open_order_rate_limit_10s
    pub fn open_order_rate_limit_10s(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().open_order_rate_limit_10s)
    }

    /// 获取 hedge_order_rate_limit_per_min
    pub fn hedge_order_rate_limit_per_min(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().hedge_order_rate_limit_per_min)
    }

    /// 获取 hedge_order_rate_limit_10s
    pub fn hedge_order_rate_limit_10s(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().hedge_order_rate_limit_10s)
    }

    /// 获取 arb_max_pending_limit_buy_orders
    pub fn arb_max_pending_limit_buy_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_max_pending_limit_buy_orders)
    }

    /// 获取 arb_max_pending_limit_sell_orders
    pub fn arb_max_pending_limit_sell_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_max_pending_limit_sell_orders)
    }

    /// 获取 arb_open_order_rate_limit_per_min
    pub fn arb_open_order_rate_limit_per_min(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_open_order_rate_limit_per_min)
    }

    /// 获取 arb_open_order_rate_limit_10s
    pub fn arb_open_order_rate_limit_10s(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_open_order_rate_limit_10s)
    }

    /// 获取 arb_hedge_order_rate_limit_per_min
    pub fn arb_hedge_order_rate_limit_per_min(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_hedge_order_rate_limit_per_min)
    }

    /// 获取 arb_hedge_order_rate_limit_10s
    pub fn arb_hedge_order_rate_limit_10s(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().arb_hedge_order_rate_limit_10s)
    }

    /// 获取所有参数的快照（用于比较是否变化）
    pub fn snapshot(&self) -> PreTradeParamsSnapshot {
        PARAMS_DATA.with(|data| {
            let data = data.borrow();
            PreTradeParamsSnapshot {
                max_pos_u: data.max_pos_u,
                max_symbol_exposure_ratio: data.max_symbol_exposure_ratio,
                max_total_exposure_ratio: data.max_total_exposure_ratio,
                max_leverage: data.max_leverage,
                max_pending_limit_orders: data.max_pending_limit_orders,
                max_pending_limit_buy_orders: data.max_pending_limit_buy_orders,
                max_pending_limit_sell_orders: data.max_pending_limit_sell_orders,
                open_order_rate_limit_per_min: data.open_order_rate_limit_per_min,
                open_order_rate_limit_10s: data.open_order_rate_limit_10s,
                hedge_order_rate_limit_per_min: data.hedge_order_rate_limit_per_min,
                hedge_order_rate_limit_10s: data.hedge_order_rate_limit_10s,
                arb_max_pending_limit_buy_orders: data.arb_max_pending_limit_buy_orders,
                arb_max_pending_limit_sell_orders: data.arb_max_pending_limit_sell_orders,
                arb_open_order_rate_limit_per_min: data.arb_open_order_rate_limit_per_min,
                arb_open_order_rate_limit_10s: data.arb_open_order_rate_limit_10s,
                arb_hedge_order_rate_limit_per_min: data.arb_hedge_order_rate_limit_per_min,
                arb_hedge_order_rate_limit_10s: data.arb_hedge_order_rate_limit_10s,
            }
        })
    }
}

/// 参数快照，用于比较参数是否变化
#[derive(Debug, Clone, PartialEq)]
pub struct PreTradeParamsSnapshot {
    pub max_pos_u: f64,
    pub max_symbol_exposure_ratio: f64,
    pub max_total_exposure_ratio: f64,
    pub max_leverage: f64,
    pub max_pending_limit_orders: i32,
    pub max_pending_limit_buy_orders: i32,
    pub max_pending_limit_sell_orders: i32,
    pub open_order_rate_limit_per_min: i32,
    pub open_order_rate_limit_10s: i32,
    pub hedge_order_rate_limit_per_min: i32,
    pub hedge_order_rate_limit_10s: i32,
    pub arb_max_pending_limit_buy_orders: i32,
    pub arb_max_pending_limit_sell_orders: i32,
    pub arb_open_order_rate_limit_per_min: i32,
    pub arb_open_order_rate_limit_10s: i32,
    pub arb_hedge_order_rate_limit_per_min: i32,
    pub arb_hedge_order_rate_limit_10s: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_params() {
        let loader = PreTradeParamsLoader::instance();
        assert_eq!(loader.max_pos_u(), 1000.0);
        assert_eq!(loader.max_symbol_exposure_ratio(), 0.8);
        assert_eq!(loader.max_total_exposure_ratio(), 1.0);
        assert_eq!(loader.max_leverage(), 3.0);
        assert_eq!(loader.max_pending_limit_orders(), 3);
        assert_eq!(loader.max_pending_limit_buy_orders(), 0);
        assert_eq!(loader.max_pending_limit_sell_orders(), 0);
        assert_eq!(loader.open_order_rate_limit_per_min(), 0);
        assert_eq!(loader.open_order_rate_limit_10s(), 0);
        assert_eq!(loader.hedge_order_rate_limit_per_min(), 0);
        assert_eq!(loader.hedge_order_rate_limit_10s(), 0);
        assert_eq!(loader.arb_max_pending_limit_buy_orders(), 0);
        assert_eq!(loader.arb_max_pending_limit_sell_orders(), 0);
        assert_eq!(loader.arb_open_order_rate_limit_per_min(), 0);
        assert_eq!(loader.arb_open_order_rate_limit_10s(), 0);
        assert_eq!(loader.arb_hedge_order_rate_limit_per_min(), 0);
        assert_eq!(loader.arb_hedge_order_rate_limit_10s(), 0);
    }

    #[test]
    fn test_snapshot() {
        let loader = PreTradeParamsLoader::instance();
        let snapshot = loader.snapshot();
        assert_eq!(snapshot.max_pos_u, 1000.0);
        assert_eq!(snapshot.max_leverage, 3.0);
        assert_eq!(snapshot.max_pending_limit_buy_orders, 0);
        assert_eq!(snapshot.max_pending_limit_sell_orders, 0);
        assert_eq!(snapshot.open_order_rate_limit_per_min, 0);
        assert_eq!(snapshot.open_order_rate_limit_10s, 0);
        assert_eq!(snapshot.hedge_order_rate_limit_per_min, 0);
        assert_eq!(snapshot.hedge_order_rate_limit_10s, 0);
        assert_eq!(snapshot.arb_max_pending_limit_buy_orders, 0);
        assert_eq!(snapshot.arb_max_pending_limit_sell_orders, 0);
        assert_eq!(snapshot.arb_open_order_rate_limit_per_min, 0);
        assert_eq!(snapshot.arb_open_order_rate_limit_10s, 0);
        assert_eq!(snapshot.arb_hedge_order_rate_limit_per_min, 0);
        assert_eq!(snapshot.arb_hedge_order_rate_limit_10s, 0);
    }

    #[test]
    fn test_resolve_max_pos_u_for_symbol_uses_override() {
        let mut overrides = HashMap::new();
        overrides.insert("BTCUSDT".to_string(), 2500.0);
        let val = resolve_max_pos_u_for_symbol(
            1000.0,
            &overrides,
            TradingVenue::BinanceFutures,
            "btc-usdt",
        );
        assert_eq!(val, 2500.0);
    }

    #[test]
    fn test_resolve_max_pos_u_for_symbol_falls_back_to_default() {
        let overrides = HashMap::new();
        let val = resolve_max_pos_u_for_symbol(
            1000.0,
            &overrides,
            TradingVenue::BinanceFutures,
            "btc-usdt",
        );
        assert_eq!(val, 1000.0);
    }

    #[test]
    fn test_parse_max_pos_u_overrides_normalizes_symbols() {
        let overrides = parse_max_pos_u_overrides(
            r#"{"btc-usdt":2500,"ETH_USDT":1200}"#,
            TradingVenue::BinanceFutures,
            "binance_mm_alpha:binance-futures:mm:max_pos_u",
        );
        assert_eq!(overrides.get("BTCUSDT"), Some(&2500.0));
        assert_eq!(overrides.get("ETHUSDT"), Some(&1200.0));
    }

    #[test]
    fn test_arb_max_pos_u_override_key_format() {
        let key = arb_max_pos_u_override_key(
            Some("okex-intra-arb01"),
            TradingVenue::OkexMargin,
            TradingVenue::OkexFutures,
        );
        assert_eq!(
            key.as_deref(),
            Some("okex-intra-arb01:okex-margin:okex-futures:max_pos_u_overrides")
        );
    }
}
