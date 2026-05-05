/// Synthetic Gate trade/ws error codes used when the venue only returns a string label/message.
pub const ORDER_NOT_FOUND: i32 = -100_501;
pub const ORDER_POC: i32 = -100_502;
/// Insufficient-margin / balance class. Mapped from Gate string labels in
/// `trade_response_handle::normalize_trade_error` so downstream logic can use
/// the same `is_insufficient_margin()` predicate as other venues.
pub const BALANCE_NOT_ENOUGH: i32 = -100_503;
pub const MARGIN_NOT_ENOUGH: i32 = -100_504;
pub const POSITION_MARGIN_TOO_LOW: i32 = -100_505;
pub const LIQUIDITY_NOT_ENOUGH: i32 = -100_506;

/// Gate trade/rest/ws error codes to short descriptions.
pub fn describe_trade_error_code(code: i32) -> Option<&'static str> {
    match code {
        0 => Some("Success"),
        ORDER_NOT_FOUND => Some("Order not found"),
        ORDER_POC => Some("Post Only rejected"),
        BALANCE_NOT_ENOUGH => Some("Balance insufficient"),
        MARGIN_NOT_ENOUGH => Some("Margin insufficient"),
        POSITION_MARGIN_TOO_LOW => Some("Position margin too low"),
        LIQUIDITY_NOT_ENOUGH => Some("Liquidity insufficient"),
        _ => None,
    }
}
