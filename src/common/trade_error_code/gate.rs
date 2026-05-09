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
pub const AUTO_BORROW_TOO_MUCH: i32 = -100_507;

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
        AUTO_BORROW_TOO_MUCH => Some("Auto borrow too much"),
        _ => None,
    }
}

pub fn parse_error_label(label: &str) -> Option<i32> {
    let normalized = label.trim().replace([' ', '-'], "_").to_ascii_uppercase();
    let is_poc_fill_immediately = normalized.contains("POC")
        && normalized.contains("FILL")
        && normalized.contains("IMMEDIATELY");
    match normalized.as_str() {
        "ORDER_NOT_FOUND" => Some(ORDER_NOT_FOUND),
        "ORDER_POC" | "POC_FILL_IMMEDIATELY" => Some(ORDER_POC),
        "BALANCE_NOT_ENOUGH" => Some(BALANCE_NOT_ENOUGH),
        "MARGIN_NOT_ENOUGH" => Some(MARGIN_NOT_ENOUGH),
        "POSITION_MARGIN_TOO_LOW" => Some(POSITION_MARGIN_TOO_LOW),
        "LIQUIDITY_NOT_ENOUGH" => Some(LIQUIDITY_NOT_ENOUGH),
        "AUTO_BORROW_TOO_MUCH" => Some(AUTO_BORROW_TOO_MUCH),
        _ if is_poc_fill_immediately => Some(ORDER_POC),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_gate_error_labels() {
        assert_eq!(parse_error_label("ORDER_NOT_FOUND"), Some(ORDER_NOT_FOUND));
        assert_eq!(parse_error_label("ORDER_POC"), Some(ORDER_POC));
        assert_eq!(parse_error_label("POC_FILL_IMMEDIATELY"), Some(ORDER_POC));
        assert_eq!(
            parse_error_label("poc order would fill immediately"),
            Some(ORDER_POC)
        );
        assert_eq!(
            parse_error_label("AUTO_BORROW_TOO_MUCH"),
            Some(AUTO_BORROW_TOO_MUCH)
        );
        assert_eq!(
            parse_error_label("BALANCE_NOT_ENOUGH"),
            Some(BALANCE_NOT_ENOUGH)
        );
        assert_eq!(parse_error_label("UNKNOWN_LABEL"), None);
    }
}
