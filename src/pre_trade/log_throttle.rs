use crate::pre_trade::open_order_rate_limiter::OrderRateBucket;
use crate::pre_trade::order_manager::Side;
use log::{info, warn};
use order_common::TradingVenue;
use runtime_common::time_util::get_timestamp_us;
use std::cell::RefCell;
use std::collections::HashMap;

const PRE_TRADE_LIMIT_LOG_INTERVAL_US: i64 = 20_000_000;
const CLOSE_BELOW_MIN_LOG_INTERVAL_US: i64 = 60_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OrderRateLimitLogKey {
    source: &'static str,
    bucket: OrderRateBucket,
    symbol: String,
    window: &'static str,
}

#[derive(Debug, Clone)]
struct OrderRateLimitLogState {
    last_log_ts_us: i64,
    suppressed: usize,
    last_strategy_id: Option<i32>,
    last_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PendingLimitLogKey {
    source: &'static str,
    symbol: String,
    side_u8: u8,
    limit_scope: &'static str,
}

#[derive(Debug, Clone)]
struct PendingLimitLogState {
    last_log_ts_us: i64,
    suppressed: usize,
    last_strategy_id: Option<i32>,
    last_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CloseBelowMinLogKey {
    source: &'static str,
    venue: TradingVenue,
    symbol: String,
    side_u8: u8,
}

#[derive(Debug, Clone)]
struct CloseBelowMinLogState {
    last_log_ts_us: i64,
    suppressed: usize,
    last_strategy_id: Option<i32>,
    last_open_pos: f64,
    last_order_qty: f64,
    last_price_hint: Option<f64>,
    last_reason: String,
}

thread_local! {
    static ORDER_RATE_LIMIT_LOGS: RefCell<HashMap<OrderRateLimitLogKey, OrderRateLimitLogState>> =
        RefCell::new(HashMap::new());
    static PENDING_LIMIT_LOGS: RefCell<HashMap<PendingLimitLogKey, PendingLimitLogState>> =
        RefCell::new(HashMap::new());
    static CLOSE_BELOW_MIN_LOGS: RefCell<HashMap<CloseBelowMinLogKey, CloseBelowMinLogState>> =
        RefCell::new(HashMap::new());
}

fn classify_order_rate_limit_window(reason: &str) -> &'static str {
    if reason.contains("近10秒") {
        "10s"
    } else if reason.contains("近60秒") {
        "60s"
    } else {
        "unknown"
    }
}

fn classify_pending_limit_scope(reason: &str) -> &'static str {
    if reason.contains("方向上限") {
        "side"
    } else if reason.contains("总上限") {
        "total"
    } else {
        "unknown"
    }
}

pub fn log_order_rate_limit_summary(
    source: &'static str,
    strategy_id: Option<i32>,
    bucket: OrderRateBucket,
    symbol: &str,
    reason: &str,
) {
    let now_us = get_timestamp_us();
    let key = OrderRateLimitLogKey {
        source,
        bucket,
        symbol: symbol.to_string(),
        window: classify_order_rate_limit_window(reason),
    };
    ORDER_RATE_LIMIT_LOGS.with(|logs| {
        let mut logs = logs.borrow_mut();
        let state = logs.entry(key).or_insert_with(|| OrderRateLimitLogState {
            last_log_ts_us: 0,
            suppressed: 0,
            last_strategy_id: strategy_id,
            last_reason: reason.to_string(),
        });
        state.suppressed += 1;
        state.last_strategy_id = strategy_id;
        state.last_reason.clear();
        state.last_reason.push_str(reason);
        if state.last_log_ts_us == 0
            || now_us.saturating_sub(state.last_log_ts_us) >= PRE_TRADE_LIMIT_LOG_INTERVAL_US
        {
            warn!(
                "{}: symbol={} 报单频率风控触发 summary: suppressed={} last_strategy_id={:?} bucket={} reason={}",
                source,
                symbol,
                state.suppressed,
                state.last_strategy_id,
                bucket.as_str(),
                state.last_reason
            );
            state.last_log_ts_us = now_us;
            state.suppressed = 0;
        }
    });
}

pub fn log_pending_limit_summary(
    source: &'static str,
    strategy_id: Option<i32>,
    symbol: &str,
    side: Side,
    reason: &str,
) {
    let now_us = get_timestamp_us();
    let key = PendingLimitLogKey {
        source,
        symbol: symbol.to_string(),
        side_u8: side.to_u8(),
        limit_scope: classify_pending_limit_scope(reason),
    };
    PENDING_LIMIT_LOGS.with(|logs| {
        let mut logs = logs.borrow_mut();
        let state = logs.entry(key).or_insert_with(|| PendingLimitLogState {
            last_log_ts_us: 0,
            suppressed: 0,
            last_strategy_id: strategy_id,
            last_reason: reason.to_string(),
        });
        state.suppressed += 1;
        state.last_strategy_id = strategy_id;
        state.last_reason.clear();
        state.last_reason.push_str(reason);
        if state.last_log_ts_us == 0
            || now_us.saturating_sub(state.last_log_ts_us) >= PRE_TRADE_LIMIT_LOG_INTERVAL_US
        {
            info!(
                "{}: symbol={} side={} 限价挂单数量风控触发 summary: suppressed={} last_strategy_id={:?} reason={}",
                source,
                symbol,
                side.as_str(),
                state.suppressed,
                state.last_strategy_id,
                state.last_reason
            );
            state.last_log_ts_us = now_us;
            state.suppressed = 0;
        }
    });
}

pub fn log_close_below_min_trade_summary(
    source: &'static str,
    strategy_id: Option<i32>,
    venue: TradingVenue,
    symbol: &str,
    side: Side,
    open_pos: f64,
    order_qty: f64,
    price_hint: Option<f64>,
    reason: &str,
) {
    let now_us = get_timestamp_us();
    let key = CloseBelowMinLogKey {
        source,
        venue,
        symbol: symbol.to_string(),
        side_u8: side.to_u8(),
    };
    CLOSE_BELOW_MIN_LOGS.with(|logs| {
        let mut logs = logs.borrow_mut();
        let state = logs.entry(key).or_insert_with(|| CloseBelowMinLogState {
            last_log_ts_us: 0,
            suppressed: 0,
            last_strategy_id: strategy_id,
            last_open_pos: open_pos,
            last_order_qty: order_qty,
            last_price_hint: price_hint,
            last_reason: reason.to_string(),
        });
        state.suppressed += 1;
        state.last_strategy_id = strategy_id;
        state.last_open_pos = open_pos;
        state.last_order_qty = order_qty;
        state.last_price_hint = price_hint;
        state.last_reason.clear();
        state.last_reason.push_str(reason);
        if state.last_log_ts_us == 0
            || now_us.saturating_sub(state.last_log_ts_us) >= CLOSE_BELOW_MIN_LOG_INTERVAL_US
        {
            info!(
                "{}: symbol={} venue={:?} side={} close below min trade requirements summary: suppressed={} last_strategy_id={:?} open_pos={:.8} order_qty={:.8} price_hint={:?} reason={}",
                source,
                symbol,
                venue,
                side.as_str(),
                state.suppressed,
                state.last_strategy_id,
                state.last_open_pos,
                state.last_order_qty,
                state.last_price_hint,
                state.last_reason
            );
            state.last_log_ts_us = now_us;
            state.suppressed = 0;
        }
    });
}
