use log::debug;
use std::cell::RefCell;
use std::collections::VecDeque;

const ORDER_RATE_WINDOW_10S_US: i64 = 10_000_000;
const ORDER_RATE_WINDOW_1M_US: i64 = 60_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderRateStats {
    pub count_10s: usize,
    pub count_1m: usize,
}

#[derive(Default)]
struct RollingRateWindow {
    orders_10s: VecDeque<i64>,
    orders_1m: VecDeque<i64>,
    last_seen_us: i64,
}

impl RollingRateWindow {
    fn normalize_now(&mut self, now_us: i64) -> i64 {
        let now_us = now_us.max(self.last_seen_us);
        self.last_seen_us = now_us;
        now_us
    }

    fn prune(&mut self, now_us: i64) -> usize {
        let now_us = self.normalize_now(now_us);
        let before_1m = self.orders_1m.len();
        while self
            .orders_10s
            .front()
            .is_some_and(|ts| now_us.saturating_sub(*ts) >= ORDER_RATE_WINDOW_10S_US)
        {
            self.orders_10s.pop_front();
        }
        while self
            .orders_1m
            .front()
            .is_some_and(|ts| now_us.saturating_sub(*ts) >= ORDER_RATE_WINDOW_1M_US)
        {
            self.orders_1m.pop_front();
        }
        before_1m.saturating_sub(self.orders_1m.len())
    }

    fn stats(&mut self, now_us: i64) -> OrderRateStats {
        self.prune(now_us);
        OrderRateStats {
            count_10s: self.orders_10s.len(),
            count_1m: self.orders_1m.len(),
        }
    }

    fn record(&mut self, now_us: i64) -> OrderRateStats {
        let now_us = self.normalize_now(now_us);
        self.prune(now_us);
        self.orders_10s.push_back(now_us);
        self.orders_1m.push_back(now_us);
        OrderRateStats {
            count_10s: self.orders_10s.len(),
            count_1m: self.orders_1m.len(),
        }
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.orders_10s.clear();
        self.orders_1m.clear();
        self.last_seen_us = 0;
    }
}

#[derive(Default)]
struct OrderRateState {
    open_orders: RollingRateWindow,
    arb_open_orders: RollingRateWindow,
    hedge_orders: RollingRateWindow,
    arb_hedge_orders: RollingRateWindow,
    exec_orders: RollingRateWindow,
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
        let stats = ORDER_RATE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            Self::bucket_window_mut(&mut state, bucket).record(now_us)
        });
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
                removed_total += Self::bucket_window_mut(&mut state, bucket).prune(now_us);
            }
            removed_total
        })
    }

    fn stats_at(bucket: OrderRateBucket, now_us: i64) -> OrderRateStats {
        ORDER_RATE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            Self::bucket_window_mut(&mut state, bucket).stats(now_us)
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

    fn bucket_window_mut(
        state: &mut OrderRateState,
        bucket: OrderRateBucket,
    ) -> &mut RollingRateWindow {
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
