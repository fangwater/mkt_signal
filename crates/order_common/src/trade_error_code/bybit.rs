/// Bybit trade/rest/ws error codes to short descriptions.
pub const INTERNAL_SYSTEM_ERROR: i32 = 10016;
pub const OPEN_INTEREST_POSITION_LIMIT_EXCEEDED: i32 = 110021;
pub const CONTRACT_NOT_LIVE: i32 = 110074;
pub const LIABILITY_OVERFLOW_SPOT_LEVERAGE: i32 = 170034;
pub const COLLATERAL_NOT_ENABLED: i32 = 170037;
pub const PLATFORM_LOAN_AMOUNT_NOT_ENOUGH: i32 = 170207;
pub const ORDER_NOT_FOUND: i32 = 170213;
pub const MARGIN_TRADING_UNSUPPORTED: i32 = 170344;

pub fn describe_trade_error_code(code: i32) -> Option<&'static str> {
    match code {
        0 => Some("Success"),
        10403 => Some("WS rate limit exceeded for IP"),
        10404 => Some("Unsupported op or category"),
        10429 => Some("System-level frequency protection triggered"),
        20006 => Some("Duplicated reqId"),
        INTERNAL_SYSTEM_ERROR => Some("Internal error or service restarting"),
        10019 => Some("WS trade service restarting; new requests rejected"),
        OPEN_INTEREST_POSITION_LIMIT_EXCEEDED => Some("Open interest or position limit exceeded"),
        CONTRACT_NOT_LIVE => Some("Contract is not live"),
        LIABILITY_OVERFLOW_SPOT_LEVERAGE => Some("Liability overflow in spot leverage trade"),
        COLLATERAL_NOT_ENABLED => Some("Collateral not enabled"),
        PLATFORM_LOAN_AMOUNT_NOT_ENOUGH => Some("Platform loan amount not enough"),
        ORDER_NOT_FOUND => Some("Order does not exist"),
        MARGIN_TRADING_UNSUPPORTED => Some("Symbol is not supported on margin trading"),
        _ => None,
    }
}

pub fn describe_non_retryable_order_error(code: i32) -> Option<&'static str> {
    match code {
        OPEN_INTEREST_POSITION_LIMIT_EXCEEDED => {
            Some("OPEN_INTEREST_POSITION_LIMIT_EXCEEDED/OI或持仓限制")
        }
        CONTRACT_NOT_LIVE => Some("CONTRACT_NOT_LIVE/合约未上线或不可交易"),
        LIABILITY_OVERFLOW_SPOT_LEVERAGE => {
            Some("LIABILITY_OVERFLOW_SPOT_LEVERAGE/现货杠杆负债溢出")
        }
        COLLATERAL_NOT_ENABLED => Some("COLLATERAL_NOT_ENABLED/抵押品未启用"),
        PLATFORM_LOAN_AMOUNT_NOT_ENOUGH => Some("PLATFORM_LOAN_AMOUNT_NOT_ENOUGH/平台可借数量不足"),
        MARGIN_TRADING_UNSUPPORTED => Some("MARGIN_TRADING_UNSUPPORTED/币对不支持杠杆交易"),
        _ => None,
    }
}
