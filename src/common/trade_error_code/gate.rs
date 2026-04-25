/// Synthetic Gate trade/ws error codes used when the venue only returns a string label/message.
pub const ORDER_NOT_FOUND: i32 = -100_501;
pub const ORDER_POC: i32 = -100_502;

/// Gate trade/rest/ws error codes to short descriptions.
pub fn describe_trade_error_code(code: i32) -> Option<&'static str> {
    match code {
        0 => Some("Success"),
        ORDER_NOT_FOUND => Some("Order not found"),
        ORDER_POC => Some("Post Only rejected"),
        _ => None,
    }
}
