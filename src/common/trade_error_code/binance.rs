/// Binance trade/rest/ws error codes to short descriptions.
pub fn describe_trade_error_code(code: i32) -> Option<&'static str> {
    match code {
        -2010 => Some("New order rejected"),
        -2011 => Some("Cancel rejected"),
        -2012 => Some("Cancel all failed"),
        -2013 => Some("Order does not exist"),
        -2014 => Some("Bad API key format"),
        -2015 => Some("Invalid API key/IP/permissions"),
        -2016 => Some("No trading window for symbol"),
        -2017 => Some("API keys locked"),
        -2018 => Some("Balance insufficient"),
        -2019 => Some("Margin insufficient"),
        -2020 => Some("Unable to fill"),
        -4116 => Some("Duplicated client order id"),
        -4118 => Some("Reduce only margin check failed"),
        -4131 => Some("Market order rejected: price outside percent-price filter"),
        51006 => Some("Exceeds maximum borrowable amount"),
        51061 => Some("Insufficient loanable assets"),
        51169 => Some("Token pledged collateral limit reached"),
        -4060 => Some("Invalid position side"),
        -4061 => Some("Position side not match"),
        -5022 => Some("Post Only rejected"),
        _ => None,
    }
}
