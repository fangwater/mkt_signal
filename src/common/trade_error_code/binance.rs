/// Binance trade/rest/ws error codes to short descriptions.
pub fn describe_trade_error_code(code: i32) -> Option<&'static str> {
    match code {
        -2011 => Some("Cancel rejected"),
        -4060 => Some("Invalid position side"),
        -4061 => Some("Position side not match"),
        -5022 => Some("Post Only rejected"),
        _ => None,
    }
}
