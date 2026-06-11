/// Bybit trade/rest/ws error codes to short descriptions.
pub const CONTRACT_NOT_LIVE: i32 = 110074;
pub const COLLATERAL_NOT_ENABLED: i32 = 170037;
pub const PLATFORM_LOAN_AMOUNT_NOT_ENOUGH: i32 = 170207;
pub const ORDER_NOT_FOUND: i32 = 170213;

pub fn describe_trade_error_code(code: i32) -> Option<&'static str> {
    match code {
        0 => Some("Success"),
        10403 => Some("WS rate limit exceeded for IP"),
        10404 => Some("Unsupported op or category"),
        10429 => Some("System-level frequency protection triggered"),
        20006 => Some("Duplicated reqId"),
        10016 => Some("Internal error or service restarting"),
        10019 => Some("WS trade service restarting; new requests rejected"),
        CONTRACT_NOT_LIVE => Some("Contract is not live"),
        COLLATERAL_NOT_ENABLED => Some("Collateral not enabled"),
        PLATFORM_LOAN_AMOUNT_NOT_ENOUGH => Some("Platform loan amount not enough"),
        ORDER_NOT_FOUND => Some("Order does not exist"),
        _ => None,
    }
}

pub fn describe_non_retryable_order_error(code: i32) -> Option<&'static str> {
    match code {
        CONTRACT_NOT_LIVE => Some("CONTRACT_NOT_LIVE/合约未上线或不可交易"),
        _ => None,
    }
}
