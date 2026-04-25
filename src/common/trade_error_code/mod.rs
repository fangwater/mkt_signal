use crate::common::exchange::Exchange;

pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod gate;
pub mod okex;

/// Map common trade/rest/ws error codes to a short, stable description.
///
/// Notes:
/// - This intentionally maps only a small set of frequently-seen codes.
/// - Some exchanges return additional dynamic details (e.g. OKX `sMsg`); those should be logged
///   separately and are not encoded here.
pub fn describe_trade_error_code(exchange: Exchange, code: i32) -> Option<&'static str> {
    match exchange {
        Exchange::Binance => binance::describe_trade_error_code(code),
        Exchange::Bitget => bitget::describe_trade_error_code(code),
        Exchange::Bybit => bybit::describe_trade_error_code(code),
        Exchange::Gate => gate::describe_trade_error_code(code),
        Exchange::Okex => okex::describe_trade_error_code(code),
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
            Some("Cancel rejected")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Binance, -4116),
            Some("Duplicated client order id")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Binance, -4118),
            Some("Reduce only margin check failed")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Binance, -4131),
            Some("Market order rejected: price outside percent-price filter")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Binance, -4060),
            Some("Invalid position side")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Binance, -4061),
            Some("Position side not match")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Binance, -5022),
            Some("Post Only rejected")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Binance, 51006),
            Some("Exceeds maximum borrowable amount")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Binance, 51169),
            Some("Token pledged collateral limit reached")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 60013),
            Some("Invalid args")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 0),
            Some("Success")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 40015),
            Some("System error, retry later")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 40715),
            Some("Order size exceeds max open size")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 40800),
            Some("Insufficient margin amount")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 40931),
            Some("Duplicated clientOid")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 43001),
            Some("Order does not exist")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 43002),
            Some("Order placement failed")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 43003),
            Some("Order placement failed")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 43012),
            Some("Account balance insufficient")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 45031),
            Some("Order already terminal")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 50060),
            Some("Duplicated clientOid")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 22006),
            Some("Order price above risk-control max buy price")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 22007),
            Some("Order price below risk-control min sell price")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 22010),
            Some("IP whitelist required")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 22034),
            Some("Below minimum order quantity")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 22038),
            Some("Quantity step mismatch")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bitget, 70228),
            Some("Too many requests")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bybit, 0),
            Some("Success")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Gate, 0),
            Some("Success")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Gate, gate::ORDER_NOT_FOUND),
            Some("Order not found")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Gate, gate::ORDER_POC),
            Some("Post Only rejected")
        );
        assert_eq!(describe_trade_error_code(Exchange::Bitget, 999), None);
        assert_eq!(
            describe_trade_error_code(Exchange::Bybit, 10403),
            Some("WS rate limit exceeded for IP")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bybit, 10404),
            Some("Unsupported op or category")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bybit, 10429),
            Some("System-level frequency protection triggered")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bybit, 20006),
            Some("Duplicated reqId")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bybit, 10016),
            Some("Internal error or service restarting")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Bybit, 10019),
            Some("WS trade service restarting; new requests rejected")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 1),
            Some("Request failed")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 50011),
            Some("Rate limit exceeded")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 50014),
            Some("Required parameter is empty")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 51006),
            Some("Price outside limit")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 51008),
            Some("Insufficient margin in account")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 51137),
            Some("Price above upper limit")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 51061),
            Some("Insufficient loanable assets")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 51400),
            Some("Cancel failed: filled/canceled/not exist")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 51410),
            Some("Cancel failed: canceling/settling")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 51412),
            Some("Cancel timeout")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 51416),
            Some("Cancel not supported: order triggered")
        );
        assert_eq!(
            describe_trade_error_code(Exchange::Okex, 51511),
            Some("Post Only rejected")
        );
        assert_eq!(describe_trade_error_code(Exchange::Bybit, 999), None);
        assert_eq!(describe_trade_error_code(Exchange::Okex, 999), None);
    }
}
