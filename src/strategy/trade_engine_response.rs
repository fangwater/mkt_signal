/// TradeEngineResponse trait 提供 trade engine 返回结果的通用访问接口
use crate::common::exchange::Exchange;

pub trait TradeEngineResponse {
    fn status(&self) -> u16;
    fn req_type(&self) -> u32;
    fn exchange(&self) -> u32;
    fn client_order_id(&self) -> i64;
    fn error_code(&self) -> i32;
    fn ip_weight(&self) -> u32;
    fn order_count(&self) -> u32;

    fn is_success(&self) -> bool {
        // Some exchanges (e.g. OKX) return HTTP 200 even when the operation fails.
        // We treat a response as successful only when HTTP status is OK *and* error_code is 0.
        self.status() == 200 && self.error_code() == 0
    }

    fn exchange_enum(&self) -> Option<Exchange> {
        Exchange::from_u8((self.exchange() & 0xFF) as u8)
    }

    /// Whether the order is rejected because maker-only/post-only would cross.
    ///
    /// Notes:
    /// - Binance GTX: -5022
    /// - OKX post-only sCode: 51511
    fn is_post_only_rejected(&self) -> bool {
        match self.exchange_enum() {
            Some(Exchange::Binance) => self.error_code() == -5022,
            Some(Exchange::Okex) => self.error_code() == 51511,
            _ => false,
        }
    }

    /// OKX: price is outside the allowed price-limit range (sCode=51006).
    fn is_price_limit_rejected(&self) -> bool {
        matches!(self.exchange_enum(), Some(Exchange::Okex)) && self.error_code() == 51006
    }

    /// Cancel rejected / cancel failed because order is already terminal.
    ///
    /// - Binance: -2011 (CANCEL_REJECTED / Unknown order sent)
    /// - OKX: 51400 (filled/canceled/not exist)
    fn is_cancel_rejected(&self) -> bool {
        match self.exchange_enum() {
            Some(Exchange::Binance) => self.error_code() == -2011,
            Some(Exchange::Okex) => matches!(self.error_code(), 51400 | 51410 | 51412 | 51416),
            _ => false,
        }
    }

    /// Generic "aggressive price rejected" bucket used by hedge retry logic.
    fn is_aggressive_price_rejected(&self) -> bool {
        self.is_post_only_rejected() || self.is_price_limit_rejected()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_success_requires_http_ok_and_no_error_code() {
        let ok = TradeEngineResponseMessage::new(200, 1, 1, 123, 0, 0, 0);
        assert!(ok.is_success());

        let okx_err = TradeEngineResponseMessage::new(200, 1, 1, 123, 51006, 0, 0);
        assert!(!okx_err.is_success());

        let http_err = TradeEngineResponseMessage::new(400, 1, 1, 123, -2011, 0, 0);
        assert!(!http_err.is_success());
    }

    #[test]
    fn detects_binance_cancel_rejected() {
        let binance_ex = crate::common::exchange::Exchange::Binance as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, binance_ex, 123, -2011, 0, 0);
        assert!(resp.is_cancel_rejected());
    }

    #[test]
    fn detects_okx_cancel_rejected() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51400, 0, 0);
        assert!(resp.is_cancel_rejected());
    }

    #[test]
    fn detects_okx_canceling_or_settling() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51410, 0, 0);
        assert!(resp.is_cancel_rejected());
    }

    #[test]
    fn detects_okx_cancel_timeout() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51412, 0, 0);
        assert!(resp.is_cancel_rejected());
    }

    #[test]
    fn detects_okx_cancel_not_supported_triggered() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51416, 0, 0);
        assert!(resp.is_cancel_rejected());
    }
}

/// trade engine 返回的通用消息
#[derive(Debug, Clone)]
pub struct TradeEngineResponseMessage {
    status: u16,
    req_type: u32,
    exchange: u32,
    client_order_id: i64,
    error_code: i32,
    ip_weight: u32,
    order_count: u32,
}

impl TradeEngineResponseMessage {
    pub fn new(
        status: u16,
        req_type: u32,
        exchange: u32,
        client_order_id: i64,
        error_code: i32,
        ip_weight: u32,
        order_count: u32,
    ) -> Self {
        Self {
            status,
            req_type,
            exchange,
            client_order_id,
            error_code,
            ip_weight,
            order_count,
        }
    }
}

impl TradeEngineResponse for TradeEngineResponseMessage {
    fn status(&self) -> u16 {
        self.status
    }

    fn req_type(&self) -> u32 {
        self.req_type
    }

    fn exchange(&self) -> u32 {
        self.exchange
    }

    fn client_order_id(&self) -> i64 {
        self.client_order_id
    }

    fn error_code(&self) -> i32 {
        self.error_code
    }

    fn ip_weight(&self) -> u32 {
        self.ip_weight
    }

    fn order_count(&self) -> u32 {
        self.order_count
    }
}
