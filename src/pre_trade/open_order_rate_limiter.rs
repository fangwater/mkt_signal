use log::debug;
use std::cell::RefCell;
use std::collections::HashMap;

const ORDER_RATE_WINDOW_10S_US: i64 = 10_000_000;
const ORDER_RATE_WINDOW_1M_US: i64 = 60_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderRateStats {
    pub count_10s: usize,
    pub count_1m: usize,
}

#[derive(Default)]
struct OrderRateState {
    open_orders: HashMap<i64, i64>,
    arb_open_orders: HashMap<i64, i64>,
    hedge_orders: HashMap<i64, i64>,
    arb_hedge_orders: HashMap<i64, i64>,
    exec_orders: HashMap<i64, i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderRateBucket {
    MmOpen,
    // Standalone ArbOpenStrategy open orders only. ArbCloseStrategy
    // runs its own paired lifecycle and intentionally does not use this bucket.
    ArbOpen,
    MmHedge,
    // ArbHedgeStrategy hedge orders. Tracked separately from MmHedge so arbitrage
    // can be throttled with its own thresholds.
    ArbHedge,
    Exec,
}

impl OrderRateBucket {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MmOpen => "mm_open",
            Self::ArbOpen => "arb_open",
            Self::MmHedge => "mm_hedge",
            Self::ArbHedge => "arb_hedge",
            Self::Exec => "exec",
        }
    }
}

thread_local! {
    static ORDER_RATE_STATE: RefCell<OrderRateState> = RefCell::new(OrderRateState::default());
}

pub struct OrderRateLimiter;

impl OrderRateLimiter {
    pub fn stats(bucket: OrderRateBucket, now_us: i64) -> OrderRateStats {
        Self::stats_at(bucket, now_us)
    }

    pub fn check_limit(
        bucket: OrderRateBucket,
        limit_per_min: i32,
        limit_10s: i32,
        now_us: i64,
    ) -> Result<OrderRateStats, String> {
        let stats = Self::stats_at(bucket, now_us);
        if limit_10s > 0 && stats.count_10s >= limit_10s as usize {
            return Err(format!(
                "{} 近10秒下单数={}，达到上限 {}",
                bucket.as_str(),
                stats.count_10s,
                limit_10s
            ));
        }
        if limit_per_min > 0 && stats.count_1m >= limit_per_min as usize {
            return Err(format!(
                "{} 近60秒下单数={}，达到上限 {}",
                bucket.as_str(),
                stats.count_1m,
                limit_per_min
            ));
        }
        Ok(stats)
    }

    pub fn record(bucket: OrderRateBucket, client_order_id: i64, now_us: i64) -> OrderRateStats {
        ORDER_RATE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            Self::bucket_map_mut(&mut state, bucket).insert(client_order_id, now_us);
        });
        let stats = Self::stats_at(bucket, now_us);
        debug!(
            "order rate recorded: bucket={} client_order_id={} count_10s={} count_1m={}",
            bucket.as_str(),
            client_order_id,
            stats.count_10s,
            stats.count_1m
        );
        stats
    }

    pub fn cleanup_expired(now_us: i64) -> usize {
        ORDER_RATE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            let mut removed_total = 0usize;
            for bucket in [
                OrderRateBucket::MmOpen,
                OrderRateBucket::ArbOpen,
                OrderRateBucket::MmHedge,
                OrderRateBucket::ArbHedge,
                OrderRateBucket::Exec,
            ] {
                let bucket_map = Self::bucket_map_mut(&mut state, bucket);
                let before = bucket_map.len();
                bucket_map.retain(|_, submit_ts_us| {
                    now_us.saturating_sub(*submit_ts_us) < ORDER_RATE_WINDOW_1M_US
                });
                removed_total += before.saturating_sub(bucket_map.len());
            }
            removed_total
        })
    }

    fn stats_at(bucket: OrderRateBucket, now_us: i64) -> OrderRateStats {
        ORDER_RATE_STATE.with(|state| {
            let state = state.borrow();
            let orders = Self::bucket_map(&state, bucket);
            let mut count_10s = 0usize;
            let mut count_1m = 0usize;
            for submit_ts_us in orders.values() {
                let age_us = now_us.saturating_sub(*submit_ts_us);
                if age_us < ORDER_RATE_WINDOW_1M_US {
                    count_1m += 1;
                }
                if age_us < ORDER_RATE_WINDOW_10S_US {
                    count_10s += 1;
                }
            }
            OrderRateStats {
                count_10s,
                count_1m,
            }
        })
    }

    #[cfg(test)]
    fn clear() {
        ORDER_RATE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.open_orders.clear();
            state.arb_open_orders.clear();
            state.hedge_orders.clear();
            state.arb_hedge_orders.clear();
            state.exec_orders.clear();
        });
    }

    fn bucket_map(state: &OrderRateState, bucket: OrderRateBucket) -> &HashMap<i64, i64> {
        match bucket {
            OrderRateBucket::MmOpen => &state.open_orders,
            OrderRateBucket::ArbOpen => &state.arb_open_orders,
            OrderRateBucket::MmHedge => &state.hedge_orders,
            OrderRateBucket::ArbHedge => &state.arb_hedge_orders,
            OrderRateBucket::Exec => &state.exec_orders,
        }
    }

    fn bucket_map_mut(
        state: &mut OrderRateState,
        bucket: OrderRateBucket,
    ) -> &mut HashMap<i64, i64> {
        match bucket {
            OrderRateBucket::MmOpen => &mut state.open_orders,
            OrderRateBucket::ArbOpen => &mut state.arb_open_orders,
            OrderRateBucket::MmHedge => &mut state.hedge_orders,
            OrderRateBucket::ArbHedge => &mut state.arb_hedge_orders,
            OrderRateBucket::Exec => &mut state.exec_orders,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_10s_and_1m_windows_separately() {
        OrderRateLimiter::clear();
        OrderRateLimiter::record(OrderRateBucket::MmOpen, 1, 1_000_000);
        OrderRateLimiter::record(OrderRateBucket::MmOpen, 2, 55_000_000);
        OrderRateLimiter::record(OrderRateBucket::MmOpen, 3, 58_000_000);

        let stats =
            OrderRateLimiter::check_limit(OrderRateBucket::MmOpen, 10, 10, 60_000_000).unwrap();
        assert_eq!(stats.count_1m, 3);
        assert_eq!(stats.count_10s, 2);

        OrderRateLimiter::clear();
    }

    #[test]
    fn rejects_when_10s_limit_hit() {
        OrderRateLimiter::clear();
        OrderRateLimiter::record(OrderRateBucket::MmOpen, 1, 51_000_000);
        OrderRateLimiter::record(OrderRateBucket::MmOpen, 2, 52_000_000);

        let err =
            OrderRateLimiter::check_limit(OrderRateBucket::MmOpen, 10, 2, 60_000_000).unwrap_err();
        assert!(err.contains("近10秒"));

        OrderRateLimiter::clear();
    }

    #[test]
    fn arb_hedge_bucket_is_independent_from_mm_hedge() {
        OrderRateLimiter::clear();
        OrderRateLimiter::record(OrderRateBucket::MmHedge, 1, 51_000_000);
        OrderRateLimiter::record(OrderRateBucket::ArbHedge, 2, 52_000_000);
        OrderRateLimiter::record(OrderRateBucket::Exec, 3, 53_000_000);

        let mm_stats =
            OrderRateLimiter::check_limit(OrderRateBucket::MmHedge, 10, 10, 60_000_000).unwrap();
        let arb_stats =
            OrderRateLimiter::check_limit(OrderRateBucket::ArbHedge, 10, 10, 60_000_000).unwrap();
        let exec_stats =
            OrderRateLimiter::check_limit(OrderRateBucket::Exec, 10, 10, 60_000_000).unwrap();
        assert_eq!(mm_stats.count_10s, 1);
        assert_eq!(arb_stats.count_10s, 1);
        assert_eq!(exec_stats.count_10s, 1);

        OrderRateLimiter::clear();
    }

    #[test]
    fn arb_open_bucket_is_independent_from_mm_open() {
        OrderRateLimiter::clear();
        OrderRateLimiter::record(OrderRateBucket::MmOpen, 1, 51_000_000);
        OrderRateLimiter::record(OrderRateBucket::ArbOpen, 2, 52_000_000);

        let mm_stats =
            OrderRateLimiter::check_limit(OrderRateBucket::MmOpen, 10, 10, 60_000_000).unwrap();
        let arb_stats =
            OrderRateLimiter::check_limit(OrderRateBucket::ArbOpen, 10, 10, 60_000_000).unwrap();
        assert_eq!(mm_stats.count_10s, 1);
        assert_eq!(arb_stats.count_10s, 1);

        OrderRateLimiter::clear();
    }

    #[test]
    fn cleanup_removes_orders_older_than_1m() {
        OrderRateLimiter::clear();
        OrderRateLimiter::record(OrderRateBucket::MmOpen, 1, 1_000_000);
        OrderRateLimiter::record(OrderRateBucket::MmOpen, 2, 30_000_000);
        OrderRateLimiter::record(OrderRateBucket::MmOpen, 3, 59_000_000);
        OrderRateLimiter::record(OrderRateBucket::MmHedge, 101, 59_000_000);

        let removed = OrderRateLimiter::cleanup_expired(62_000_000);
        assert_eq!(removed, 1);

        let stats =
            OrderRateLimiter::check_limit(OrderRateBucket::MmOpen, 10, 10, 62_000_000).unwrap();
        assert_eq!(stats.count_1m, 2);

        let hedge_stats =
            OrderRateLimiter::check_limit(OrderRateBucket::MmHedge, 10, 10, 62_000_000).unwrap();
        assert_eq!(hedge_stats.count_1m, 1);

        OrderRateLimiter::clear();
    }
}
