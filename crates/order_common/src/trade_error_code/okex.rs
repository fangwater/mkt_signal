/// OKX trade/rest/ws error codes to short descriptions.
pub fn describe_trade_error_code(code: i32) -> Option<&'static str> {
    match code {
        60013 => Some("Invalid args"),
        1 => Some("Request failed"),
        50011 => Some("Rate limit exceeded"),
        50014 => Some("Required parameter is empty"),
        51000 => Some("Parameter error"),
        51006 => Some("Price outside limit"),
        51008 => Some("Insufficient margin in account"),
        51137 => Some("Price above upper limit"),
        51061 => Some("Insufficient loanable assets"),
        51400 => Some("Cancel failed: filled/canceled/not exist"),
        51410 => Some("Cancel failed: canceling/settling"),
        51412 => Some("Cancel timeout"),
        51416 => Some("Cancel not supported: order triggered"),
        51511 => Some("Post Only rejected"),
        _ => None,
    }
}
