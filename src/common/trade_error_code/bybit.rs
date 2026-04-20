/// Bybit trade/rest/ws error codes to short descriptions.
pub fn describe_trade_error_code(code: i32) -> Option<&'static str> {
    match code {
        0 => Some("Success"),
        10403 => Some("WS rate limit exceeded for IP"),
        10404 => Some("Unsupported op or category"),
        10429 => Some("System-level frequency protection triggered"),
        20006 => Some("Duplicated reqId"),
        10016 => Some("Internal error or service restarting"),
        10019 => Some("WS trade service restarting; new requests rejected"),
        _ => None,
    }
}
