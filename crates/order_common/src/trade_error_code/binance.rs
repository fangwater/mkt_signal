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
        4001 | -4001 => Some("Price less than zero"),
        4002 | -4002 => Some("Price greater than max price"),
        -4003 => Some("Quantity less than zero"),
        -4004 => Some("Quantity less than minimum quantity"),
        -4005 => Some("Quantity greater than max quantity"),
        -4006 => Some("Stop price less than zero"),
        -4007 => Some("Stop price greater than max price"),
        -4008 => Some("Tick size less than zero"),
        -4009 => Some("Max price less than min price"),
        -5022 => Some("Post Only rejected"),
        _ => None,
    }
}

pub fn describe_non_retryable_order_error(code: i32) -> Option<&'static str> {
    match code {
        4001 | -4001 => Some("PRICE_LESS_THAN_ZERO/价格小于0"),
        4002 | -4002 => Some("PRICE_GREATER_THAN_MAX_PRICE/价格超过最大值"),
        -4003 => Some("QTY_LESS_THAN_ZERO/数量小于0"),
        -4004 => Some("QTY_LESS_THAN_MIN_QTY/数量小于最小值"),
        -4005 => Some("QTY_GREATER_THAN_MAX_QTY/数量大于最大值"),
        -4006 => Some("STOP_PRICE_LESS_THAN_ZERO/触发价小于最小值"),
        -4007 => Some("STOP_PRICE_GREATER_THAN_MAX_PRICE/触发价大于最大值"),
        -4008 => Some("TICK_SIZE_LESS_THAN_ZERO/价格精度小于0"),
        -4009 => Some("MAX_PRICE_LESS_THAN_MIN_PRICE/最大价格小于最小价格"),
        // Binance PAPI 资产抵押上限：短期重试通常无效，转入 residual 等后续流程处理。
        51169 => Some("PLEDGED_COLLATERAL_LIMIT_REACHED/抵押上限已达"),
        _ => None,
    }
}
