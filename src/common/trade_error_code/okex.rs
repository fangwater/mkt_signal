/// OKX trade/rest/ws error codes to short descriptions.
pub fn describe_trade_error_code(code: i32) -> Option<&'static str> {
    match code {
        60013 => Some("Invalid args"),
        1 => Some("Request failed"),
        51006 => Some("Price outside limit"),
        51061 => Some("Insufficient loanable assets"),
        51400 => Some("Cancel failed: filled/canceled/not exist"),
        51410 => Some("Cancel failed: canceling/settling"),
        51412 => Some("Cancel timeout"),
        51416 => Some("Cancel not supported: order triggered"),
        51511 => Some("Post Only rejected"),
        _ => None,
    }
}
