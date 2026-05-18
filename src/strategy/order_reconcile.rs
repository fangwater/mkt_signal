use crate::common::tick_math::QuantizedValue;
pub use crate::pre_trade::order_manager::CUMULATIVE_FILL_ROLLBACK_EPS;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus};
use crate::signal::common::{TimeInForce, TradingVenue};
use crate::strategy::order_query_parser::parse_compact_order_query_resp as parse_compact_order_query_resp_common;
use crate::trade_engine::query_parsers::compact_order::CompactOrderQueryResp;

pub const ORDER_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;
pub const BINANCE_PM_ORDER_QUERY_WATCHDOG_DELAY_US: i64 = 6_000_000;

pub fn order_query_watchdog_delay_us_for_venue(
    venue: TradingVenue,
    binance_is_standard: bool,
) -> i64 {
    if matches!(
        venue,
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures
    ) && !binance_is_standard
    {
        BINANCE_PM_ORDER_QUERY_WATCHDOG_DELAY_US
    } else {
        ORDER_QUERY_WATCHDOG_DELAY_US
    }
}

pub fn order_query_watchdog_delay_us(order: &Order, binance_is_standard: bool) -> i64 {
    order_query_watchdog_delay_us_for_venue(order.venue, binance_is_standard)
}

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
    OrderExecutionStatus::from_u8(parsed.status_u8)?;
    TimeInForce::from_u8(parsed.time_in_force_u8)?;
    if parsed.update_time_ms < 0 {
        return None;
    }
    Some(parsed)
}

#[cfg(test)]
mod tests {
    use super::{
        monotonic_cumulative_fill, order_query_watchdog_delay_us_for_venue, qv_decimal_or_fallback,
        BINANCE_PM_ORDER_QUERY_WATCHDOG_DELAY_US, ORDER_QUERY_WATCHDOG_DELAY_US,
    };
    use crate::signal::common::TradingVenue;

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

    #[test]
    fn binance_pm_uses_long_order_query_watchdog_delay() {
        assert_eq!(
            order_query_watchdog_delay_us_for_venue(TradingVenue::BinanceFutures, false),
            BINANCE_PM_ORDER_QUERY_WATCHDOG_DELAY_US
        );
        assert_eq!(
            order_query_watchdog_delay_us_for_venue(TradingVenue::BinanceMargin, false),
            BINANCE_PM_ORDER_QUERY_WATCHDOG_DELAY_US
        );
        assert_eq!(
            order_query_watchdog_delay_us_for_venue(TradingVenue::BinanceFutures, true),
            ORDER_QUERY_WATCHDOG_DELAY_US
        );
        assert_eq!(
            order_query_watchdog_delay_us_for_venue(TradingVenue::GateFutures, false),
            ORDER_QUERY_WATCHDOG_DELAY_US
        );
    }
}
