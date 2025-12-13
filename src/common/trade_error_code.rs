use crate::common::exchange::Exchange;

/// Map common trade/rest/ws error codes to a short, stable description.
///
/// Notes:
/// - This intentionally maps only a small set of frequently-seen codes.
/// - Some exchanges return additional dynamic details (e.g. OKX `sMsg`); those should be logged
///   separately and are not encoded here.
pub fn describe_trade_error_code(exchange: Exchange, code: i32) -> Option<&'static str> {
    match exchange {
        Exchange::Binance => match code {
            -2011 => Some("Unknown order sent"),
            -5022 => Some("Post Only rejected"),
            _ => None,
        },
        Exchange::Okex => match code {
            60013 => Some("Invalid args"),
            1 => Some("Request failed"),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_codes() {
        assert_eq!(
            describe_trade_error_code(Exchange::Binance, -2011),
            Some("Unknown order sent")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Binance, -5022),
            Some("Post Only rejected")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 60013),
            Some("Invalid args")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 1),
            Some("Request failed")
        );
        assert_eq!(describe_trade_error_code(Exchange::Okex, 999), None);
    }
}

