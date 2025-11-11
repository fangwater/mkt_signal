use anyhow::Result;
use std::cell::RefCell;

/// 从 Redis 加载的 pre-trade 参数（内部数据结构）
#[derive(Debug, Clone)]
struct PreTradeParamsData {
    max_pos_u: f64,
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    max_leverage: f64,
    max_pending_limit_orders: i32,
    refresh_secs: u64,
}

impl Default for PreTradeParamsData {
    fn default() -> Self {
        Self {
            max_pos_u: 0.0,
            max_symbol_exposure_ratio: 1.0,
            max_total_exposure_ratio: 1.0,
            max_leverage: 3.0,
            max_pending_limit_orders: 3,
            refresh_secs: 30,
        }
    }
}

/// Pre-trade 参数单例（thread-local 单线程版本）
pub struct PreTradeParams;

thread_local! {
    static PARAMS_DATA: RefCell<PreTradeParamsData> = RefCell::new(PreTradeParamsData::default());
}

impl PreTradeParams {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        PreTradeParams
    }

    /// 从 Redis 加载参数并更新单例
    pub async fn load_from_redis(&self, redis_url: Option<&str>) -> Result<()> {
        let url = redis_url.unwrap_or("redis://127.0.0.1:6379/0");
        let cli = redis::Client::open(url)?;
        let mut mgr = redis::aio::ConnectionManager::new(cli).await?;

        let params: std::collections::HashMap<String, String> =
            redis::AsyncCommands::hgetall(&mut mgr, "binance_forward_arb_params").await?;

        let parse_f64 = |k: &str| -> Option<f64> {
            params.get(k).and_then(|v| v.parse::<f64>().ok())
        };
        let parse_u64 = |k: &str| -> Option<u64> {
            params.get(k).and_then(|v| v.parse::<u64>().ok())
        };
        let parse_i64 = |k: &str| -> Option<i64> {
            params.get(k).and_then(|v| v.parse::<i64>().ok())
        };

        PARAMS_DATA.with(|data| {
            let mut data = data.borrow_mut();

            if let Some(v) = parse_f64("pre_trade_max_pos_u") {
                data.max_pos_u = v;
            }

            if let Some(v) = parse_f64("pre_trade_max_symbol_exposure_ratio") {
                data.max_symbol_exposure_ratio = v;
            }

            if let Some(v) = parse_f64("pre_trade_max_total_exposure_ratio") {
                data.max_total_exposure_ratio = v;
            }

            if let Some(v) = parse_f64("pre_trade_max_leverage") {
                if v > 0.0 {
                    data.max_leverage = v;
                } else {
                    log::warn!("pre_trade_max_leverage={} 无效，需大于 0，忽略更新", v);
                }
            }

            if let Some(v) = parse_i64("max_pending_limit_orders") {
                data.max_pending_limit_orders = v.max(0) as i32;
            }

            if let Some(v) = parse_u64("pre_trade_refresh_secs") {
                data.refresh_secs = v;
            }

            log::debug!(
                "pre_trade params loaded: max_pos_u={:.2} sym_ratio={:.4} total_ratio={:.4} max_leverage={:.2} max_pending_limit={} refresh={}s",
                data.max_pos_u,
                data.max_symbol_exposure_ratio,
                data.max_total_exposure_ratio,
                data.max_leverage,
                data.max_pending_limit_orders,
                data.refresh_secs
            );
        });

        Ok(())
    }

    /// 获取 max_pos_u
    pub fn max_pos_u(&self) -> f64 {
        PARAMS_DATA.with(|data| data.borrow().max_pos_u)
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

    /// 获取 max_pending_limit_orders
    pub fn max_pending_limit_orders(&self) -> i32 {
        PARAMS_DATA.with(|data| data.borrow().max_pending_limit_orders)
    }

    /// 获取 refresh_secs
    pub fn refresh_secs(&self) -> u64 {
        PARAMS_DATA.with(|data| data.borrow().refresh_secs)
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
                refresh_secs: data.refresh_secs,
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
    pub refresh_secs: u64,
}
