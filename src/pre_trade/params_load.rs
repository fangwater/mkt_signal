use anyhow::Result;
use log::{debug, info, warn};
use std::cell::RefCell;
use std::time::Duration;

use crate::common::redis_client::{RedisClient, RedisSettings};

/// Redis Key 配置
const REDIS_KEY_RISK_PARAMS: &str = "fr_pre_trade_params";

/// 后台刷新间隔（固定 60 秒）
const REFRESH_INTERVAL_SECS: u64 = 60;

/// 从 Redis 加载的 Pre-Trade 风控参数（内部数据结构）
#[derive(Debug, Clone)]
struct PreTradeParamsData {
    max_pos_u: f64,
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    max_leverage: f64,
    max_pending_limit_orders: i32,
}

impl Default for PreTradeParamsData {
    fn default() -> Self {
        Self {
            max_pos_u: 1000.0,
            max_symbol_exposure_ratio: 0.8,
            max_total_exposure_ratio: 1.0,
            max_leverage: 3.0,
            max_pending_limit_orders: 3,
        }
    }
}

/// Pre-Trade 参数加载器单例（thread-local 单线程版本）
pub struct PreTradeParamsLoader;

thread_local! {
    static PARAMS_DATA: RefCell<PreTradeParamsData> = RefCell::new(PreTradeParamsData::default());
}

impl PreTradeParamsLoader {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        PreTradeParamsLoader
    }

    /// 从 Redis 加载参数并更新单例
    pub async fn load_from_redis(&self, redis: &RedisSettings) -> Result<()> {
        let mut client = RedisClient::connect(redis.clone()).await?;
        let hash_map = client.hgetall_map(REDIS_KEY_RISK_PARAMS).await?;

        let parse_f64 =
            |k: &str| -> Option<f64> { hash_map.get(k).and_then(|v| v.parse::<f64>().ok()) };
        let parse_i64 =
            |k: &str| -> Option<i64> { hash_map.get(k).and_then(|v| v.parse::<i64>().ok()) };

        PARAMS_DATA.with(|data| {
            let mut data = data.borrow_mut();

            if let Some(v) = parse_f64("max_pos_u") {
                data.max_pos_u = v;
            }

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

            debug!(
                "风控参数已加载: max_pos_u={:.2} sym_ratio={:.4} total_ratio={:.4} max_leverage={:.2} max_pending={}",
                data.max_pos_u,
                data.max_symbol_exposure_ratio,
                data.max_total_exposure_ratio,
                data.max_leverage,
                data.max_pending_limit_orders
            );
        });

        Ok(())
    }

    /// 启动后台刷新任务（固定 60 秒间隔）
    pub fn start_background_refresh(redis: RedisSettings) {
        tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(REFRESH_INTERVAL_SECS));
            let loader = PreTradeParamsLoader::instance();

            loop {
                interval.tick().await;

                match loader.load_from_redis(&redis).await {
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
        println!("{}", separator);
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
    }

    #[test]
    fn test_snapshot() {
        let loader = PreTradeParamsLoader::instance();
        let snapshot = loader.snapshot();
        assert_eq!(snapshot.max_pos_u, 1000.0);
        assert_eq!(snapshot.max_leverage, 3.0);
    }
}
