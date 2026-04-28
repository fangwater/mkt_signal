use crate::common::tick_math::QuantizedValue;
pub use crate::pre_trade::order_manager::CUMULATIVE_FILL_ROLLBACK_EPS;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus};
use crate::signal::common::TimeInForce;
use crate::strategy::order_query_parser::parse_compact_order_query_resp as parse_compact_order_query_resp_common;
use crate::trade_engine::query_parsers::compact_order::CompactOrderQueryResp;

pub const ORDER_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingOrderQueryReason {
    OrderWatchdog,
    CancelWatchdog,
    CancelFailed,
    CancelRejected,
}

impl PendingOrderQueryReason {
    pub fn is_cancel_rejected(self) -> bool {
        matches!(self, Self::CancelRejected)
    }

    pub fn watchdog_hint(self) -> &'static str {
        match self {
            Self::CancelRejected => "CancelRejectedWatchdog触发",
            Self::CancelWatchdog | Self::CancelFailed => "CancelWatchdog触发",
            Self::OrderWatchdog => "OrderWatchdog触发",
        }
    }

    pub fn query_send_failed_trigger(self) -> &'static str {
        match self {
            Self::CancelRejected => "cancel_rejected_query_send_failed",
            Self::CancelWatchdog | Self::CancelFailed => "cancel_query_send_failed",
            Self::OrderWatchdog => "order_query_send_failed",
        }
    }
}

pub fn qv_decimal_or_fallback(value: f64) -> String {
    QuantizedValue::from_decimal(value)
        .map(|qv| qv.decimal_string())
        .unwrap_or_else(|| format!("{value:.8}"))
}

pub fn monotonic_cumulative_fill(prev_cum: f64, incoming_cum: f64) -> f64 {
    Order::protect_cumulative_fill_value(prev_cum, incoming_cum).effective_cum
}

pub fn parse_strategy_compact_order_query_resp(
    body: &bytes::Bytes,
) -> Option<CompactOrderQueryResp> {
    let parsed = parse_compact_order_query_resp_common(body)?;
    if parsed.order_id <= 0 {
        return None;
    }
    if OrderExecutionStatus::from_u8(parsed.status_u8).is_none() {
        return None;
    }
    if TimeInForce::from_u8(parsed.time_in_force_u8).is_none() {
        return None;
    }
    if parsed.update_time_ms < 0 {
        return None;
    }
    Some(parsed)
}

#[cfg(test)]
mod tests {
    use super::{monotonic_cumulative_fill, qv_decimal_or_fallback};

    #[test]
    fn monotonic_cumulative_fill_keeps_local_value_on_rollback() {
        assert!((monotonic_cumulative_fill(4.2, 0.0) - 4.2).abs() < 1e-12);
    }

    #[test]
    fn monotonic_cumulative_fill_accepts_forward_progress() {
        assert!((monotonic_cumulative_fill(4.2, 5.6) - 5.6).abs() < 1e-12);
    }

    #[test]
    fn qv_decimal_falls_back_for_invalid_decimal() {
        assert_eq!(qv_decimal_or_fallback(f64::NAN), "NaN");
    }
}
