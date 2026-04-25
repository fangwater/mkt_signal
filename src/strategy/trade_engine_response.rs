/// TradeEngineResponse trait 提供 trade engine 返回结果的通用访问接口
use crate::common::exchange::Exchange;
use crate::common::trade_error_code::gate;
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
    fn order_id(&self) -> Option<i64> {
        None
    }
    fn order_status_u8(&self) -> Option<u8> {
        None
    }
    fn order_update_time(&self) -> Option<i64> {
        None
    }
    fn executed_qty(&self) -> Option<f64> {
        None
    }
    fn response_price(&self) -> Option<f64> {
        None
    }

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
                | TradeRequestType::BinanceWsNewUMOrder
                | TradeRequestType::BinanceWsNewMarginOrder
                | TradeRequestType::BinanceNewUMConditionalOrder
                | TradeRequestType::BinanceNewMarginOrder
                | TradeRequestType::OkexNewMarginOrder
                | TradeRequestType::OkexNewUMOrder
                | TradeRequestType::GateUnifiedNewOrder
                | TradeRequestType::GateFuturesNewOrder
                | TradeRequestType::BybitNewMarginOrder
                | TradeRequestType::BybitNewUMOrder
                | TradeRequestType::BitgetNewMarginOrder
                | TradeRequestType::BitgetNewUMOrder,
            ) => TradeRequestKind::Open,
            Ok(
                TradeRequestType::BinanceCancelUMOrder
                | TradeRequestType::BinanceWsCancelUMOrder
                | TradeRequestType::BinanceWsCancelMarginOrder
                | TradeRequestType::BinanceCancelUMConditionalOrder
                | TradeRequestType::BinanceCancelMarginOrder
                | TradeRequestType::OkexCancelMarginOrder
                | TradeRequestType::OkexCancelUMOrder
                | TradeRequestType::GateUnifiedCancelOrder
                | TradeRequestType::GateFuturesCancelOrder
                | TradeRequestType::BybitCancelMarginOrder
                | TradeRequestType::BybitCancelUMOrder
                | TradeRequestType::BitgetCancelMarginOrder
                | TradeRequestType::BitgetCancelUMOrder,
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
    /// - Bybit 170217/170218 是 LIMIT-MAKER/PostOnly 类下单拒绝；如果交易所已接受后
    ///   再异步取消，会走订单更新里的 Cancelled/rejectReason 路径。
    /// - Gate 直接拒单时可能只返回 label=ORDER_POC；如果订单已进入订单流，则
    ///   finish_as=poc 会被解析成 Canceled，走撤单后重报路径。
    /// - Bitget post_only 穿价通常会先挂单成功再被交易所取消，应该看订单更新里的
    ///   cancelReason=post_only_fill_cancel；这里不额外按下单失败 code 识别。
    fn is_post_only_rejected(&self) -> bool {
        match self.exchange_enum() {
            Some(Exchange::Binance) => self.error_code() == -5022,
            Some(Exchange::Okex) => self.error_code() == 51511,
            Some(Exchange::Gate) => self.error_code() == gate::ORDER_POC,
            Some(Exchange::Bybit) => matches!(self.error_code(), 170217 | 170218),
            Some(Exchange::Bitget) => false,
            _ => false,
        }
    }

    /// OKX: price is outside the allowed price-limit range (e.g. sCode=51006/51137).
    fn is_price_limit_rejected(&self) -> bool {
        match self.exchange_enum() {
            Some(Exchange::Okex) => matches!(self.error_code(), 51006 | 51137),
            Some(Exchange::Bybit) => matches!(self.error_code(), 110003 | 170132 | 170193),
            Some(Exchange::Bitget) => matches!(
                self.error_code(),
                40815 | 40816 | 22006 | 22007 | 22008 | 22009 | 22046 | 22047
            ),
            _ => false,
        }
    }

    /// Bitget generic order-placement failure. This is an open-fail bucket, not a cancel result.
    fn is_order_placement_failed(&self) -> bool {
        match self.exchange_enum() {
            Some(Exchange::Bitget) => {
                self.is_open_request() && matches!(self.error_code(), 43002 | 43003)
            }
            _ => false,
        }
    }

    /// OKX: insufficient margin / loanable assets.
    fn is_insufficient_margin(&self) -> bool {
        match self.exchange_enum() {
            Some(Exchange::Binance) => {
                matches!(self.error_code(), -2018 | -2019 | 51006 | 51061 | 51169)
            }
            Some(Exchange::Okex) => matches!(self.error_code(), 51008 | 51061),
            Some(Exchange::Bybit) => matches!(
                self.error_code(),
                110004 | 110006 | 110007 | 110012 | 110014 | 110044 | 110045 | 170131
            ),
            Some(Exchange::Bitget) => {
                matches!(self.error_code(), 40798 | 40800 | 43012 | 45002 | 45003)
            }
            _ => false,
        }
    }

    /// Cancel rejected / cancel failed because order is already terminal.
    ///
    /// - Binance: -2011 (CANCEL_REJECTED / Unknown order sent)
    /// - OKX: 51400 (filled/canceled/not exist)
    fn is_cancel_rejected(&self) -> bool {
        match self.exchange_enum() {
            Some(Exchange::Binance) => self.error_code() == -2011,
            Some(Exchange::Okex) => matches!(self.error_code(), 51400 | 51410 | 51412 | 51416),
            Some(Exchange::Gate) => self.error_code() == gate::ORDER_NOT_FOUND,
            Some(Exchange::Bybit) => matches!(
                self.error_code(),
                110001 | 110008 | 110010 | 170139 | 170142 | 170143 | 170145 | 170190 | 170191
            ),
            Some(Exchange::Bitget) => {
                matches!(
                    self.error_code(),
                    22001 | 43001 | 43004 | 45031 | 45055 | 45057
                )
            }
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
            Some(Exchange::Gate) => self.error_code() == gate::ORDER_NOT_FOUND,
            Some(Exchange::Bybit) => matches!(
                self.error_code(),
                110001 | 110008 | 110010 | 170139 | 170142 | 170143 | 170145 | 170190 | 170191
            ),
            Some(Exchange::Bitget) => {
                matches!(
                    self.error_code(),
                    22001 | 43001 | 43004 | 45031 | 45055 | 45057
                )
            }
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
        let ok = TradeEngineResponseMessage::new(200, 1, 1, 123, 0);
        assert!(ok.is_http_ok());
        assert!(ok.is_request_success());

        let ok_ws = TradeEngineResponseMessage::new(206, 1, 1, 123, 0);
        assert!(ok_ws.is_http_ok());
        assert!(ok_ws.is_request_success());

        let okx_err = TradeEngineResponseMessage::new(200, 1, 1, 123, 51006);
        assert!(okx_err.is_http_ok());
        assert!(!okx_err.is_request_success());

        let http_err = TradeEngineResponseMessage::new(400, 1, 1, 123, -2011);
        assert!(!http_err.is_http_ok());
        assert!(!http_err.is_request_success());
    }

    #[test]
    fn detects_binance_cancel_rejected() {
        let binance_ex = crate::common::exchange::Exchange::Binance as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, binance_ex, 123, -2011);
        assert!(resp.is_cancel_rejected());
        assert!(resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_okx_cancel_rejected() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51400);
        assert!(resp.is_cancel_rejected());
        assert!(resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_okx_canceling_or_settling() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51410);
        assert!(resp.is_cancel_rejected());
        assert!(resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_okx_cancel_timeout() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51412);
        assert!(resp.is_cancel_rejected());
        assert!(!resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_okx_cancel_not_supported_triggered() {
        let okx_ex = crate::common::exchange::Exchange::Okex as u32;
        let resp = TradeEngineResponseMessage::new(200, 1, okx_ex, 123, 51416);
        assert!(resp.is_cancel_rejected());
        assert!(resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_gate_order_not_found_cancel() {
        let gate_ex = crate::common::exchange::Exchange::Gate as u32;
        let resp = TradeEngineResponseMessage::new(400, 1, gate_ex, 123, gate::ORDER_NOT_FOUND);
        assert!(resp.is_cancel_rejected());
        assert!(resp.is_cancel_not_cancellable());
    }

    #[test]
    fn detects_binance_insufficient_margin_like_codes() {
        let binance_ex = crate::common::exchange::Exchange::Binance as u32;
        let balance_insufficient = TradeEngineResponseMessage::new(400, 1, binance_ex, 123, -2018);
        let margin_insufficient = TradeEngineResponseMessage::new(400, 1, binance_ex, 123, -2019);
        let max_borrowable_exceeded =
            TradeEngineResponseMessage::new(400, 1, binance_ex, 123, 51006);
        let loanable_unavailable = TradeEngineResponseMessage::new(400, 1, binance_ex, 123, 51061);
        let collateral_cap = TradeEngineResponseMessage::new(400, 1, binance_ex, 123, 51169);

        assert!(balance_insufficient.is_insufficient_margin());
        assert!(margin_insufficient.is_insufficient_margin());
        assert!(max_borrowable_exceeded.is_insufficient_margin());
        assert!(loanable_unavailable.is_insufficient_margin());
        assert!(collateral_cap.is_insufficient_margin());
    }

    #[test]
    fn detects_open_rejected_by_req_kind() {
        let okx_new = TradeEngineResponseMessage::new(
            200,
            TradeRequestType::OkexNewUMOrder as u32,
            crate::common::exchange::Exchange::Okex as u32,
            123,
            51511,
        );
        assert!(okx_new.is_open_request());
        assert!(okx_new.is_open_rejected());

        let okx_cancel = TradeEngineResponseMessage::new(
            200,
            TradeRequestType::OkexCancelUMOrder as u32,
            crate::common::exchange::Exchange::Okex as u32,
            123,
            51400,
        );
        assert!(okx_cancel.is_cancel_request());
        assert!(!okx_cancel.is_open_rejected());
    }

    #[test]
    fn detects_bitget_order_placement_failed_as_open_fail() {
        let bitget_ex = crate::common::exchange::Exchange::Bitget as u32;
        for code in [43002, 43003] {
            let resp = TradeEngineResponseMessage::new(
                400,
                TradeRequestType::BitgetNewUMOrder as u32,
                bitget_ex,
                123,
                code,
            );
            assert!(resp.is_open_request());
            assert!(resp.is_open_rejected());
            assert!(resp.is_order_placement_failed());

            let cancel_resp = TradeEngineResponseMessage::new(
                400,
                TradeRequestType::BitgetCancelUMOrder as u32,
                bitget_ex,
                123,
                code,
            );
            assert!(cancel_resp.is_cancel_request());
            assert!(!cancel_resp.is_order_placement_failed());
        }
    }

    #[test]
    fn detects_bybit_post_only_rejected_by_venue_and_code() {
        let bybit_ex = crate::common::exchange::Exchange::Bybit as u32;
        for code in [170217, 170218] {
            let resp = TradeEngineResponseMessage::new(
                400,
                TradeRequestType::BybitNewUMOrder as u32,
                bybit_ex,
                123,
                code,
            );
            assert!(resp.is_open_request());
            assert!(resp.is_open_rejected());
            assert!(resp.is_post_only_rejected());
            assert!(resp.is_aggressive_price_rejected());
        }

        let same_code_on_bitget = TradeEngineResponseMessage::new(
            400,
            TradeRequestType::BitgetNewUMOrder as u32,
            crate::common::exchange::Exchange::Bitget as u32,
            123,
            170217,
        );
        assert!(!same_code_on_bitget.is_post_only_rejected());
    }

    #[test]
    fn detects_gate_post_only_rejected_by_venue_and_synthetic_code() {
        let resp = TradeEngineResponseMessage::new(
            400,
            TradeRequestType::GateFuturesNewOrder as u32,
            crate::common::exchange::Exchange::Gate as u32,
            123,
            gate::ORDER_POC,
        );
        assert!(resp.is_open_request());
        assert!(resp.is_open_rejected());
        assert!(resp.is_post_only_rejected());
        assert!(resp.is_aggressive_price_rejected());

        let same_code_on_bitget = TradeEngineResponseMessage::new(
            400,
            TradeRequestType::BitgetNewUMOrder as u32,
            crate::common::exchange::Exchange::Bitget as u32,
            123,
            gate::ORDER_POC,
        );
        assert!(!same_code_on_bitget.is_post_only_rejected());
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
    order_id: i64,
    order_status_u8: u8,
    order_update_time: i64,
    executed_qty: f64,
    response_price: f64,
}

impl TradeEngineResponseMessage {
    pub fn new(
        status: u16,
        req_type: u32,
        exchange: u32,
        client_order_id: i64,
        error_code: i32,
    ) -> Self {
        Self {
            status,
            req_type,
            exchange,
            client_order_id,
            error_code,
            order_id: 0,
            order_status_u8: 0,
            order_update_time: 0,
            executed_qty: 0.0,
            response_price: 0.0,
        }
    }

    pub fn new_with_tail(
        status: u16,
        req_type: u32,
        exchange: u32,
        client_order_id: i64,
        error_code: i32,
        order_id: i64,
        order_status_u8: u8,
        order_update_time: i64,
        executed_qty: f64,
        response_price: f64,
    ) -> Self {
        Self {
            status,
            req_type,
            exchange,
            client_order_id,
            error_code,
            order_id,
            order_status_u8,
            order_update_time,
            executed_qty,
            response_price,
        }
    }

    pub fn order_id_raw(&self) -> i64 {
        self.order_id
    }

    pub fn order_status_u8_raw(&self) -> u8 {
        self.order_status_u8
    }

    pub fn order_update_time_raw(&self) -> i64 {
        self.order_update_time
    }

    pub fn executed_qty_raw(&self) -> f64 {
        self.executed_qty
    }

    pub fn response_price_raw(&self) -> f64 {
        self.response_price
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

    fn order_id(&self) -> Option<i64> {
        if self.order_id > 0 {
            Some(self.order_id)
        } else {
            None
        }
    }

    fn order_status_u8(&self) -> Option<u8> {
        if self.order_status_u8 > 0 {
            Some(self.order_status_u8)
        } else {
            None
        }
    }

    fn order_update_time(&self) -> Option<i64> {
        if self.order_update_time > 0 {
            Some(self.order_update_time)
        } else {
            None
        }
    }

    fn executed_qty(&self) -> Option<f64> {
        if self.executed_qty > 0.0 {
            Some(self.executed_qty)
        } else {
            None
        }
    }

    fn response_price(&self) -> Option<f64> {
        if self.response_price > 0.0 {
            Some(self.response_price)
        } else {
            None
        }
    }
}
