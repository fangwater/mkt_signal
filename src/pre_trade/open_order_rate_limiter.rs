use log::debug;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};

const ORDER_RATE_WINDOW_10S_US: i64 = 10_000_000;
const ORDER_RATE_WINDOW_1M_US: i64 = 60_000_000;
const OKEX_MODIFY_WINDOW_US: i64 = 2_000_000;
const OKEX_MODIFY_LIMIT: usize = 60;

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

    fn next_available_at_us(&mut self, now_us: i64, limit_per_min: usize, limit_10s: usize) -> i64 {
        let now_us = self.normalize_now(now_us);
        self.prune(now_us);
        let mut retry_at_us = now_us;
        if limit_10s > 0 && self.orders_10s.len() >= limit_10s {
            let index = self.orders_10s.len() - limit_10s;
            if let Some(ts) = self.orders_10s.get(index) {
                retry_at_us = retry_at_us.max(ts.saturating_add(ORDER_RATE_WINDOW_10S_US));
            }
        }
        if limit_per_min > 0 && self.orders_1m.len() >= limit_per_min {
            let index = self.orders_1m.len() - limit_per_min;
            if let Some(ts) = self.orders_1m.get(index) {
                retry_at_us = retry_at_us.max(ts.saturating_add(ORDER_RATE_WINDOW_1M_US));
            }
        }
        retry_at_us
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

    fn is_empty(&self) -> bool {
        self.orders_10s.is_empty() && self.orders_1m.is_empty()
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
    strategy_orders: HashMap<String, RollingRateWindow>,
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

    pub fn next_available_at_us(
        bucket: OrderRateBucket,
        limit_per_min: i32,
        limit_10s: i32,
        now_us: i64,
    ) -> i64 {
        ORDER_RATE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            Self::bucket_window_mut(&mut state, bucket).next_available_at_us(
                now_us,
                limit_per_min.max(0) as usize,
                limit_10s.max(0) as usize,
            )
        })
    }

    pub fn check_strategy_limit(
        strategy_name: &str,
        limit_per_min: u32,
        limit_10s: u32,
        now_us: i64,
    ) -> Result<OrderRateStats, String> {
        let stats = ORDER_RATE_STATE.with(|state| {
            state
                .borrow_mut()
                .strategy_orders
                .entry(strategy_name.to_string())
                .or_default()
                .stats(now_us)
        });
        if limit_10s > 0 && stats.count_10s >= limit_10s as usize {
            return Err(format!(
                "chase strategy={} 近10秒下单数={}，达到上限 {}",
                strategy_name, stats.count_10s, limit_10s
            ));
        }
        if limit_per_min > 0 && stats.count_1m >= limit_per_min as usize {
            return Err(format!(
                "chase strategy={} 近60秒下单数={}，达到上限 {}",
                strategy_name, stats.count_1m, limit_per_min
            ));
        }
        Ok(stats)
    }

    pub fn record_strategy(
        strategy_name: &str,
        client_order_id: i64,
        now_us: i64,
    ) -> OrderRateStats {
        let stats = ORDER_RATE_STATE.with(|state| {
            state
                .borrow_mut()
                .strategy_orders
                .entry(strategy_name.to_string())
                .or_default()
                .record(now_us)
        });
        debug!(
            "order rate recorded: bucket=chase_strategy strategy_name={} client_order_id={} count_10s={} count_1m={}",
            strategy_name, client_order_id, stats.count_10s, stats.count_1m
        );
        stats
    }

    pub fn strategy_next_available_at_us(
        strategy_name: &str,
        limit_per_min: u32,
        limit_10s: u32,
        now_us: i64,
    ) -> i64 {
        ORDER_RATE_STATE.with(|state| {
            state
                .borrow_mut()
                .strategy_orders
                .entry(strategy_name.to_string())
                .or_default()
                .next_available_at_us(now_us, limit_per_min as usize, limit_10s as usize)
        })
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
            for window in state.strategy_orders.values_mut() {
                removed_total += window.prune(now_us);
            }
            state.strategy_orders.retain(|_, window| !window.is_empty());
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
            state.strategy_orders.clear();
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

#[derive(Default)]
struct OkexModifyWindow {
    requests: VecDeque<i64>,
    last_seen_us: i64,
}

impl OkexModifyWindow {
    fn prune(&mut self, now_us: i64) -> i64 {
        let now_us = now_us.max(self.last_seen_us);
        self.last_seen_us = now_us;
        while self
            .requests
            .front()
            .is_some_and(|ts| now_us.saturating_sub(*ts) >= OKEX_MODIFY_WINDOW_US)
        {
            self.requests.pop_front();
        }
        now_us
    }
}

thread_local! {
    static OKEX_MODIFY_RATE_STATE: RefCell<HashMap<String, OkexModifyWindow>> =
        RefCell::new(HashMap::new());
}

/// Native OKX applies the amend-order limit per user and instrument. A
/// pre-trade process owns one user, so the instrument is the local key.
pub struct OkexModifyRateLimiter;

impl OkexModifyRateLimiter {
    pub fn check_limit(symbol: &str, now_us: i64) -> Result<usize, String> {
        OKEX_MODIFY_RATE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            let window = state.entry(symbol.to_string()).or_default();
            window.prune(now_us);
            let count = window.requests.len();
            if count >= OKEX_MODIFY_LIMIT {
                Err(format!(
                    "okex modify symbol={} 近2秒请求数={}，达到上限 {}",
                    symbol, count, OKEX_MODIFY_LIMIT
                ))
            } else {
                Ok(count)
            }
        })
    }

    pub fn record(symbol: &str, now_us: i64) -> usize {
        OKEX_MODIFY_RATE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            let window = state.entry(symbol.to_string()).or_default();
            let now_us = window.prune(now_us);
            window.requests.push_back(now_us);
            window.requests.len()
        })
    }

    pub fn next_available_at_us(symbol: &str, now_us: i64) -> i64 {
        OKEX_MODIFY_RATE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            let window = state.entry(symbol.to_string()).or_default();
            let now_us = window.prune(now_us);
            if window.requests.len() < OKEX_MODIFY_LIMIT {
                return now_us;
            }
            let index = window.requests.len() - OKEX_MODIFY_LIMIT;
            window
                .requests
                .get(index)
                .copied()
                .unwrap_or(now_us)
                .saturating_add(OKEX_MODIFY_WINDOW_US)
        })
    }

    #[cfg(test)]
    fn clear() {
        OKEX_MODIFY_RATE_STATE.with(|state| state.borrow_mut().clear());
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

    #[test]
    fn next_available_time_waits_for_the_relevant_window_to_expire() {
        OrderRateLimiter::clear();
        OrderRateLimiter::record(OrderRateBucket::Exec, 1, 100);
        OrderRateLimiter::record(OrderRateBucket::Exec, 2, 200);

        assert_eq!(
            OrderRateLimiter::next_available_at_us(OrderRateBucket::Exec, 0, 2, 300),
            100 + ORDER_RATE_WINDOW_10S_US
        );
        assert_eq!(
            OrderRateLimiter::next_available_at_us(OrderRateBucket::Exec, 2, 0, 300),
            100 + ORDER_RATE_WINDOW_1M_US
        );
        OrderRateLimiter::clear();
    }

    #[test]
    fn strategy_limits_aggregate_symbols_and_isolate_strategy_names() {
        OrderRateLimiter::clear();
        OrderRateLimiter::record_strategy("alpha", 1, 51_000_000);
        OrderRateLimiter::record_strategy("alpha", 2, 52_000_000);

        assert!(OrderRateLimiter::check_strategy_limit("alpha", 10, 2, 60_000_000).is_err());
        assert!(OrderRateLimiter::check_strategy_limit("beta", 10, 2, 60_000_000).is_ok());
        assert_eq!(
            OrderRateLimiter::strategy_next_available_at_us("alpha", 0, 2, 60_000_000),
            51_000_000 + ORDER_RATE_WINDOW_10S_US
        );
        OrderRateLimiter::clear();
    }

    #[test]
    fn disabled_strategy_windows_still_keep_rolling_history_for_hot_reload() {
        OrderRateLimiter::clear();
        OrderRateLimiter::record_strategy("alpha", 1, 1_000_000);
        assert!(OrderRateLimiter::check_strategy_limit("alpha", 0, 0, 2_000_000).is_ok());
        assert!(OrderRateLimiter::check_strategy_limit("alpha", 1, 0, 2_000_000).is_err());

        assert_eq!(OrderRateLimiter::cleanup_expired(62_000_000), 1);
        assert!(OrderRateLimiter::check_strategy_limit("alpha", 1, 0, 62_000_000).is_ok());
        OrderRateLimiter::clear();
    }

    #[test]
    fn okex_modify_limit_is_per_symbol_and_expires_after_two_seconds() {
        OkexModifyRateLimiter::clear();
        for index in 0..OKEX_MODIFY_LIMIT {
            assert!(OkexModifyRateLimiter::check_limit("BTCUSDT", 1_000).is_ok());
            assert_eq!(OkexModifyRateLimiter::record("BTCUSDT", 1_000), index + 1);
        }
        assert!(OkexModifyRateLimiter::check_limit("BTCUSDT", 1_500).is_err());
        assert!(OkexModifyRateLimiter::check_limit("ETHUSDT", 1_500).is_ok());
        assert_eq!(
            OkexModifyRateLimiter::next_available_at_us("BTCUSDT", 1_500),
            1_000 + OKEX_MODIFY_WINDOW_US
        );
        assert!(
            OkexModifyRateLimiter::check_limit("BTCUSDT", 1_000 + OKEX_MODIFY_WINDOW_US).is_ok()
        );
        OkexModifyRateLimiter::clear();
    }
}
