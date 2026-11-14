use crate::pre_trade::open_order_rate_limiter::OrderRateBucket;
use crate::pre_trade::order_manager::Side;
use log::{error, info, warn, Level};
use order_common::TradingVenue;
use runtime_common::time_util::get_timestamp_us;
use std::cell::RefCell;
use std::collections::HashMap;

const PRE_TRADE_LIMIT_LOG_INTERVAL_US: i64 = 20_000_000;
const OPEN_RISK_REJECT_LOG_INTERVAL_US: i64 = 20_000_000;
const STRATEGY_INACTIVE_LOG_INTERVAL_US: i64 = 20_000_000;
const CLOSE_BELOW_MIN_LOG_INTERVAL_US: i64 = 60_000_000;
const ARB_CLOSE_PRECHECK_LOG_INTERVAL_US: i64 = 20_000_000;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OpenRiskRejectLogKey {
    source: &'static str,
    symbol: String,
    risk_name: &'static str,
}

#[derive(Debug, Clone)]
struct OpenRiskRejectLogState {
    last_log_ts_us: i64,
    suppressed: usize,
    last_strategy_id: Option<i32>,
    last_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StrategyInactiveLogKey {
    source: &'static str,
    symbol: String,
    reason_class: &'static str,
}

#[derive(Debug, Clone)]
struct StrategyInactiveLogState {
    last_log_ts_us: i64,
    suppressed: usize,
    last_strategy_id: Option<i32>,
    last_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ArbClosePrecheckLogKey {
    outcome: &'static str,
    symbol: String,
    side_u8: u8,
}

#[derive(Debug, Clone)]
struct ArbClosePrecheckLogState {
    last_log_ts_us: i64,
    suppressed: usize,
    last_opening_venue: TradingVenue,
    last_hedging_venue: TradingVenue,
    last_hedging_symbol: String,
    last_opening_pos: f64,
    last_hedging_pos: f64,
    last_amount: f64,
    last_amount_count: i64,
    last_price: f64,
    last_notional: f64,
    last_generation_time_us: i64,
    last_receive_us: i64,
    last_receive_lag_us: i64,
}

thread_local! {
    static ORDER_RATE_LIMIT_LOGS: RefCell<HashMap<OrderRateLimitLogKey, OrderRateLimitLogState>> =
        RefCell::new(HashMap::new());
    static PENDING_LIMIT_LOGS: RefCell<HashMap<PendingLimitLogKey, PendingLimitLogState>> =
        RefCell::new(HashMap::new());
    static CLOSE_BELOW_MIN_LOGS: RefCell<HashMap<CloseBelowMinLogKey, CloseBelowMinLogState>> =
        RefCell::new(HashMap::new());
    static OPEN_RISK_REJECT_LOGS: RefCell<HashMap<OpenRiskRejectLogKey, OpenRiskRejectLogState>> =
        RefCell::new(HashMap::new());
    static STRATEGY_INACTIVE_LOGS: RefCell<HashMap<StrategyInactiveLogKey, StrategyInactiveLogState>> =
        RefCell::new(HashMap::new());
    static ARB_CLOSE_PRECHECK_LOGS: RefCell<HashMap<ArbClosePrecheckLogKey, ArbClosePrecheckLogState>> =
        RefCell::new(HashMap::new());
}

#[allow(clippy::too_many_arguments)]
pub fn log_arb_close_precheck_summary(
    outcome: &'static str,
    level: Level,
    opening_symbol: &str,
    opening_venue: TradingVenue,
    hedging_symbol: &str,
    hedging_venue: TradingVenue,
    side: Side,
    opening_pos: f64,
    hedging_pos: f64,
    amount: f64,
    amount_count: i64,
    price: f64,
    generation_time_us: i64,
    receive_us: i64,
) {
    let now_us = get_timestamp_us();
    let notional = amount * price;
    let receive_lag_us = if generation_time_us > 0 {
        receive_us.saturating_sub(generation_time_us)
    } else {
        -1
    };
    let key = ArbClosePrecheckLogKey {
        outcome,
        symbol: opening_symbol.to_string(),
        side_u8: side.to_u8(),
    };
    ARB_CLOSE_PRECHECK_LOGS.with(|logs| {
        let mut logs = logs.borrow_mut();
        let state = logs
            .entry(key)
            .or_insert_with(|| ArbClosePrecheckLogState {
                last_log_ts_us: 0,
                suppressed: 0,
                last_opening_venue: opening_venue,
                last_hedging_venue: hedging_venue,
                last_hedging_symbol: hedging_symbol.to_string(),
                last_opening_pos: opening_pos,
                last_hedging_pos: hedging_pos,
                last_amount: amount,
                last_amount_count: amount_count,
                last_price: price,
                last_notional: notional,
                last_generation_time_us: generation_time_us,
                last_receive_us: receive_us,
                last_receive_lag_us: receive_lag_us,
            });
        state.suppressed += 1;
        state.last_opening_venue = opening_venue;
        state.last_hedging_venue = hedging_venue;
        state.last_hedging_symbol.clear();
        state.last_hedging_symbol.push_str(hedging_symbol);
        state.last_opening_pos = opening_pos;
        state.last_hedging_pos = hedging_pos;
        state.last_amount = amount;
        state.last_amount_count = amount_count;
        state.last_price = price;
        state.last_notional = notional;
        state.last_generation_time_us = generation_time_us;
        state.last_receive_us = receive_us;
        state.last_receive_lag_us = receive_lag_us;
        if state.last_log_ts_us == 0
            || now_us.saturating_sub(state.last_log_ts_us) >= ARB_CLOSE_PRECHECK_LOG_INTERVAL_US
        {
            log::log!(
                level,
                "ArbClose precheck summary: outcome={} opening={} {:?} hedging={} {:?} side={} open_pos={:.8} hedge_pos={:.8} amount={:.8} amount_count={} price={:.8} notional={:.8} min_notional=25 generation_time_us={} receive_us={} receive_lag_us={} suppressed={}",
                outcome,
                opening_symbol,
                state.last_opening_venue,
                state.last_hedging_symbol,
                state.last_hedging_venue,
                side.as_str(),
                state.last_opening_pos,
                state.last_hedging_pos,
                state.last_amount,
                state.last_amount_count,
                state.last_price,
                state.last_notional,
                state.last_generation_time_us,
                state.last_receive_us,
                state.last_receive_lag_us,
                state.suppressed
            );
            state.last_log_ts_us = now_us;
            state.suppressed = 0;
        }
    });
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

fn classify_strategy_inactive_reason(reason: &str) -> &'static str {
    if reason.starts_with("leverage risk failed:") {
        "leverage"
    } else if reason.starts_with("max position risk failed:") {
        "max_position"
    } else if reason.starts_with("open order rate limit triggered:") {
        "order_rate"
    } else if reason.starts_with("pending limit order risk failed:") {
        "pending_limit"
    } else if reason.contains("余额不足") {
        "balance"
    } else {
        "other"
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

pub fn log_open_risk_reject_summary(
    source: &'static str,
    strategy_id: Option<i32>,
    symbol: &str,
    risk_name: &'static str,
    reason: &str,
) {
    let now_us = get_timestamp_us();
    let key = OpenRiskRejectLogKey {
        source,
        symbol: symbol.to_string(),
        risk_name,
    };
    OPEN_RISK_REJECT_LOGS.with(|logs| {
        let mut logs = logs.borrow_mut();
        let state = logs.entry(key).or_insert_with(|| OpenRiskRejectLogState {
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
            || now_us.saturating_sub(state.last_log_ts_us) >= OPEN_RISK_REJECT_LOG_INTERVAL_US
        {
            error!(
                "{}: symbol={} {}检查失败 summary: suppressed={} last_strategy_id={:?} reason={}",
                source,
                symbol,
                risk_name,
                state.suppressed,
                state.last_strategy_id,
                state.last_reason
            );
            state.last_log_ts_us = now_us;
            state.suppressed = 0;
        }
    });
}

pub fn log_strategy_inactive_summary(
    source: &'static str,
    strategy_id: Option<i32>,
    symbol: &str,
    reason: &str,
) {
    let now_us = get_timestamp_us();
    let reason_class = classify_strategy_inactive_reason(reason);
    let key = StrategyInactiveLogKey {
        source,
        symbol: symbol.to_string(),
        reason_class,
    };
    STRATEGY_INACTIVE_LOGS.with(|logs| {
        let mut logs = logs.borrow_mut();
        let state = logs.entry(key).or_insert_with(|| StrategyInactiveLogState {
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
            || now_us.saturating_sub(state.last_log_ts_us) >= STRATEGY_INACTIVE_LOG_INTERVAL_US
        {
            warn!(
                "{}: symbol={} 未激活 summary: suppressed={} last_strategy_id={:?} reason_class={} reason={}",
                source,
                symbol,
                state.suppressed,
                state.last_strategy_id,
                reason_class,
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
