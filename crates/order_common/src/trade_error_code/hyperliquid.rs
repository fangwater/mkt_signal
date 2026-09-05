//! Stable internal codes for Hyperliquid's text-only action errors.

pub const ACTION_REJECTED: i32 = -65_000;
pub const POST_ONLY_REJECTED: i32 = -65_001;
pub const ORDER_NOT_FOUND: i32 = -65_002;
pub const INSUFFICIENT_MARGIN: i32 = -65_003;
pub const ACTION_AMBIGUOUS: i32 = -65_004;
pub const INSUFFICIENT_SPOT_BALANCE: i32 = -65_005;
pub const PRICE_LIMIT_REJECTED: i32 = -65_006;
pub const POSITION_LIMIT_EXCEEDED: i32 = -65_007;
pub const INVALID_TICK: i32 = -65_008;
pub const MIN_NOTIONAL: i32 = -65_009;
pub const REDUCE_ONLY_REJECTED: i32 = -65_010;
pub const NO_LIQUIDITY: i32 = -65_011;
pub const INVALID_TRIGGER_PRICE: i32 = -65_012;

pub fn describe_trade_error_code(code: i32) -> Option<&'static str> {
    match code {
        ACTION_REJECTED => Some("Action rejected"),
        POST_ONLY_REJECTED => Some("Post Only rejected"),
        ORDER_NOT_FOUND => Some("Order does not exist"),
        INSUFFICIENT_MARGIN => Some("Insufficient margin"),
        ACTION_AMBIGUOUS => Some("Action outcome ambiguous; order status query required"),
        INSUFFICIENT_SPOT_BALANCE => Some("Insufficient spot balance"),
        PRICE_LIMIT_REJECTED => Some("Order price outside exchange risk limits"),
        POSITION_LIMIT_EXCEEDED => Some("Open interest or position limit exceeded"),
        INVALID_TICK => Some("Price tick mismatch"),
        MIN_NOTIONAL => Some("Below minimum order notional"),
        REDUCE_ONLY_REJECTED => Some("Reduce only order would increase position"),
        NO_LIQUIDITY => Some("No immediately executable liquidity"),
        INVALID_TRIGGER_PRICE => Some("Invalid trigger price"),
        _ => None,
    }
}

/// Only documented payload errors belong here, never transport ambiguity.
pub fn describe_non_retryable_order_error(code: i32) -> Option<&'static str> {
    match code {
        INVALID_TICK | MIN_NOTIONAL | REDUCE_ONLY_REJECTED | INVALID_TRIGGER_PRICE => {
            describe_trade_error_code(code)
        }
        _ => None,
    }
}
