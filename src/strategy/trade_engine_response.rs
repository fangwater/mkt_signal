/// TradeEngineResponse trait 提供 trade engine 返回结果的通用访问接口
use crate::common::exchange::Exchange;
use crate::trade_engine::trade_request::TradeRequestType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeRequestKind {
    Open,
    Cancel,
    Other,
}

pub trait TradeEngineResponse {
    fn status(&self) -> u16;
    fn req_type(&self) -> u32;
    fn exchange(&self) -> u32;
    fn client_order_id(&self) -> i64;
    fn error_code(&self) -> i32;
    fn ip_weight(&self) -> u32;
    fn order_count(&self) -> u32;

    /// Whether the HTTP layer returned 200 OK.
    ///
    /// Note: some exchanges (e.g. OKX) may still return an application error under HTTP 200.
    fn is_http_ok(&self) -> bool {
        (200..300).contains(&(self.status() as u32))
    }

    /// Semantic success for a request:
    /// - HTTP 200
    /// - application `error_code == 0`
    fn is_request_success(&self) -> bool {
        self.is_http_ok() && self.error_code() == 0
    }

    fn exchange_enum(&self) -> Option<Exchange> {
        Exchange::from_u8((self.exchange() & 0xFF) as u8)
    }

    fn request_kind(&self) -> TradeRequestKind {
        match TradeRequestType::try_from(self.req_type()) {
            Ok(
                TradeRequestType::BinanceNewUMOrder
                | TradeRequestType::BinanceNewUMConditionalOrder
                | TradeRequestType::BinanceNewMarginOrder
                | TradeRequestType::OkexNewMarginOrder
                | TradeRequestType::OkexNewUMOrder
                | TradeRequestType::GateUnifiedNewOrder
                | TradeRequestType::GateFuturesNewOrder,
            ) => TradeRequestKind::Open,
            Ok(
                TradeRequestType::BinanceCancelUMOrder
                | TradeRequestType::BinanceCancelUMConditionalOrder
                | TradeRequestType::BinanceCancelMarginOrder
                | TradeRequestType::OkexCancelMarginOrder
                | TradeRequestType::OkexCancelUMOrder
                | TradeRequestType::GateUnifiedCancelOrder
                | TradeRequestType::GateFuturesCancelOrder,
            ) => TradeRequestKind::Cancel,
            _ => TradeRequestKind::Other,
        }
    }

    fn is_open_request(&self) -> bool {
        self.request_kind() == TradeRequestKind::Open
    }

    fn is_cancel_request(&self) -> bool {
        self.request_kind() == TradeRequestKind::Cancel
    }

    fn is_open_rejected(&self) -> bool {
        self.is_open_request() && !self.is_request_success()
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

    /// OKX: price is outside the allowed price-limit range (e.g. sCode=51006/51137).
    fn is_price_limit_rejected(&self) -> bool {
        matches!(self.exchange_enum(), Some(Exchange::Okex))
            && matches!(self.error_code(), 51006 | 51137)
    }

    /// OKX: insufficient margin / loanable assets.
    fn is_insufficient_margin(&self) -> bool {
        matches!(self.exchange_enum(), Some(Exchange::Okex))
            && matches!(self.error_code(), 51008 | 51061)
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

    /// Cancel has no further action value (re-cancel will not help); query-only is appropriate.
    ///
    /// - Binance: -2011 (already terminal / unknown order)
    /// - OKX:
    ///   - 51400: filled/canceled/not exist
    ///   - 51410: canceling/settling (cancel already in progress)
    ///   - 51416: cancel not supported (triggered order)
    ///
    /// Note: OKX 51412 is a timeout; query first, but re-cancel may still be useful if order remains live.
    fn is_cancel_not_cancellable(&self) -> bool {
        match self.exchange_enum() {
            Some(Exchange::Binance) => self.error_code() == -2011,
            Some(Exchange::Okex) => matches!(self.error_code(), 51400 | 51410 | 51416),
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
        assert!(ok.is_http_ok());
        assert!(ok.is_request_success());

        let ok_ws = TradeEngineResponseMessage::new(206, 1, 1, 123, 0, 0, 0);
        assert!(ok_ws.is_http_ok());
        assert!(ok_ws.is_request_success());

        let okx_err = TradeEngineResponseMessage::new(200, 1, 1, 123, 51006, 0, 0);
        assert!(okx_err.is_http_ok());
        assert!(!okx_err.is_request_success());

        let http_err = TradeEngineResponseMessage::new(400, 1, 1, 123, -2011, 0, 0);
        assert!(!http_err.is_http_ok());
        assert!(!http_err.is_request_success());
    }

    #[test]
    fn detects_binance_cancel_rejected() {
        let binance_ex = crate::common::exchange::Exchange::Binance as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, binance_ex, 123, -2011, 0, 0);
        assert!(resp.is_cancel_rejected());
        assert!(resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_okx_cancel_rejected() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51400, 0, 0);
        assert!(resp.is_cancel_rejected());
        assert!(resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_okx_canceling_or_settling() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51410, 0, 0);
        assert!(resp.is_cancel_rejected());
        assert!(resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_okx_cancel_timeout() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51412, 0, 0);
        assert!(resp.is_cancel_rejected());
        assert!(!resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_okx_cancel_not_supported_triggered() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51416, 0, 0);
        assert!(resp.is_cancel_rejected());
        assert!(resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_open_rejected_by_req_kind() {
        let okx_new = TradeEngineResponseMessage::new(
            200,
            TradeRequestType::OkexNewUMOrder as u32,
            crate::common::exchange::Exchange::Okex as u32,
            123,
            51511,
            0,
            0,
        );
        assert!(okx_new.is_open_request());
        assert!(okx_new.is_open_rejected());

        let okx_cancel = TradeEngineResponseMessage::new(
            200,
            TradeRequestType::OkexCancelUMOrder as u32,
            crate::common::exchange::Exchange::Okex as u32,
            123,
            51400,
            0,
            0,
        );
        assert!(okx_cancel.is_cancel_request());
        assert!(!okx_cancel.is_open_rejected());
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
