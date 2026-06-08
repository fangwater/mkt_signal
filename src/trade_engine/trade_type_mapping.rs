use super::trade_request::TradeRequestType;

pub struct TradeTypeMapping;

impl TradeTypeMapping {
    /// 判断请求类型是否走 WebSocket
    pub fn is_websocket(request_type: TradeRequestType) -> bool {
        match request_type {
            // Binance REST 请求
            TradeRequestType::BinanceNewUMOrder
            | TradeRequestType::BinanceNewUMConditionalOrder
            | TradeRequestType::BinanceNewMarginOrder
            | TradeRequestType::BinanceCancelUMOrder
            | TradeRequestType::BinanceCancelAllUMOrders
            | TradeRequestType::BinanceCancelUMConditionalOrder
            | TradeRequestType::BinanceCancelAllUMConditionalOrders
            | TradeRequestType::BinanceCancelMarginOrder
            | TradeRequestType::BinanceModifyUMOrder
            | TradeRequestType::BinanceUMSetLeverage
            | TradeRequestType::BinanceUniversalTransfer => false,
            TradeRequestType::BinanceWsNewUMOrder
            | TradeRequestType::BinanceWsCancelUMOrder
            | TradeRequestType::BinanceWsNewMarginOrder
            | TradeRequestType::BinanceWsCancelMarginOrder => true,

            // OKEx 所有请求走 WebSocket
            TradeRequestType::OkexNewMarginOrder
            | TradeRequestType::OkexNewUMOrder
            | TradeRequestType::OkexCancelMarginOrder
            | TradeRequestType::OkexCancelUMOrder => true,

            // Gate 统一账户 / 合约走 WebSocket
            TradeRequestType::GateUnifiedNewOrder
            | TradeRequestType::GateUnifiedCancelOrder
            | TradeRequestType::GateFuturesNewOrder
            | TradeRequestType::GateFuturesCancelOrder
            | TradeRequestType::BybitNewMarginOrder
            | TradeRequestType::BybitNewUMOrder
            | TradeRequestType::BybitCancelMarginOrder
            | TradeRequestType::BybitCancelUMOrder
            | TradeRequestType::BitgetNewMarginOrder
            | TradeRequestType::BitgetNewUMOrder
            | TradeRequestType::BitgetCancelMarginOrder
            | TradeRequestType::BitgetCancelUMOrder => true,
        }
    }

    /// 根据请求类型获取endpoint（仅用于 REST）
    pub fn get_endpoint(request_type: TradeRequestType) -> &'static str {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => "/papi/v1/um/order",
            TradeRequestType::BinanceNewUMConditionalOrder => "/papi/v1/um/conditional/order",
            TradeRequestType::BinanceNewMarginOrder => "/papi/v1/margin/order",
            TradeRequestType::BinanceCancelUMOrder => "/papi/v1/um/order",
            TradeRequestType::BinanceCancelAllUMOrders => "/papi/v1/um/allOpenOrders",
            TradeRequestType::BinanceCancelUMConditionalOrder => "/papi/v1/um/conditional/order",
            TradeRequestType::BinanceCancelAllUMConditionalOrders => {
                "/papi/v1/um/conditional/allOpenOrders"
            }
            TradeRequestType::BinanceCancelMarginOrder => "/papi/v1/margin/order",
            TradeRequestType::BinanceModifyUMOrder => "/papi/v1/um/order",
            TradeRequestType::BinanceUMSetLeverage => "/papi/v1/um/leverage",
            TradeRequestType::BinanceUniversalTransfer => "/sapi/v1/asset/transfer",
            TradeRequestType::BinanceWsNewUMOrder
            | TradeRequestType::BinanceWsCancelUMOrder
            | TradeRequestType::BinanceWsNewMarginOrder
            | TradeRequestType::BinanceWsCancelMarginOrder => {
                unreachable!("Binance ws requests run via websocket; REST mapping not used")
            }
            TradeRequestType::OkexNewMarginOrder
            | TradeRequestType::OkexNewUMOrder
            | TradeRequestType::OkexCancelMarginOrder
            | TradeRequestType::OkexCancelUMOrder => {
                unreachable!("Okex requests run via websocket; REST mapping not used")
            }
            TradeRequestType::GateUnifiedNewOrder
            | TradeRequestType::GateUnifiedCancelOrder
            | TradeRequestType::GateFuturesNewOrder
            | TradeRequestType::GateFuturesCancelOrder => {
                unreachable!("Gate requests run via websocket; REST mapping not used")
            }
            TradeRequestType::BybitNewMarginOrder
            | TradeRequestType::BybitNewUMOrder
            | TradeRequestType::BybitCancelMarginOrder
            | TradeRequestType::BybitCancelUMOrder => {
                unreachable!("Bybit requests run via websocket; REST mapping not used")
            }
            TradeRequestType::BitgetNewMarginOrder
            | TradeRequestType::BitgetNewUMOrder
            | TradeRequestType::BitgetCancelMarginOrder
            | TradeRequestType::BitgetCancelUMOrder => {
                unreachable!("Bitget requests run via websocket; REST mapping not used")
            }
        }
    }

    /// 根据请求类型获取HTTP方法（仅用于 REST）
    pub fn get_method(request_type: TradeRequestType) -> &'static str {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => "POST",
            TradeRequestType::BinanceNewUMConditionalOrder => "POST",
            TradeRequestType::BinanceNewMarginOrder => "POST",
            TradeRequestType::BinanceCancelUMOrder => "DELETE",
            TradeRequestType::BinanceCancelAllUMOrders => "DELETE",
            TradeRequestType::BinanceCancelUMConditionalOrder => "DELETE",
            TradeRequestType::BinanceCancelAllUMConditionalOrders => "DELETE",
            TradeRequestType::BinanceCancelMarginOrder => "DELETE",
            TradeRequestType::BinanceModifyUMOrder => "PUT",
            TradeRequestType::BinanceUMSetLeverage => "POST",
            TradeRequestType::BinanceUniversalTransfer => "POST",
            TradeRequestType::BinanceWsNewUMOrder
            | TradeRequestType::BinanceWsCancelUMOrder
            | TradeRequestType::BinanceWsNewMarginOrder
            | TradeRequestType::BinanceWsCancelMarginOrder => {
                unreachable!("Binance ws requests run via websocket; REST mapping not used")
            }
            TradeRequestType::OkexNewMarginOrder
            | TradeRequestType::OkexNewUMOrder
            | TradeRequestType::OkexCancelMarginOrder
            | TradeRequestType::OkexCancelUMOrder => {
                unreachable!("Okex requests run via websocket; REST mapping not used")
            }
            TradeRequestType::GateUnifiedNewOrder
            | TradeRequestType::GateUnifiedCancelOrder
            | TradeRequestType::GateFuturesNewOrder
            | TradeRequestType::GateFuturesCancelOrder => {
                unreachable!("Gate requests run via websocket; REST mapping not used")
            }
            TradeRequestType::BybitNewMarginOrder
            | TradeRequestType::BybitNewUMOrder
            | TradeRequestType::BybitCancelMarginOrder
            | TradeRequestType::BybitCancelUMOrder => {
                unreachable!("Bybit requests run via websocket; REST mapping not used")
            }
            TradeRequestType::BitgetNewMarginOrder
            | TradeRequestType::BitgetNewUMOrder
            | TradeRequestType::BitgetCancelMarginOrder
            | TradeRequestType::BitgetCancelUMOrder => {
                unreachable!("Bitget requests run via websocket; REST mapping not used")
            }
        }
    }

    /// 根据请求类型获取API权重（仅用于 REST）
    pub fn get_weight(request_type: TradeRequestType) -> u32 {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => 1,
            TradeRequestType::BinanceNewUMConditionalOrder => 1,
            TradeRequestType::BinanceNewMarginOrder => 1,
            TradeRequestType::BinanceCancelUMOrder => 1,
            TradeRequestType::BinanceCancelAllUMOrders => 1,
            TradeRequestType::BinanceCancelUMConditionalOrder => 1,
            TradeRequestType::BinanceCancelAllUMConditionalOrders => 1,
            TradeRequestType::BinanceCancelMarginOrder => 2,
            TradeRequestType::BinanceModifyUMOrder => 1,
            TradeRequestType::BinanceUMSetLeverage => 1,
            TradeRequestType::BinanceUniversalTransfer => 900,
            TradeRequestType::BinanceWsNewUMOrder
            | TradeRequestType::BinanceWsCancelUMOrder
            | TradeRequestType::BinanceWsNewMarginOrder
            | TradeRequestType::BinanceWsCancelMarginOrder => {
                unreachable!("Binance ws requests run via websocket; REST mapping not used")
            }
            TradeRequestType::OkexNewMarginOrder
            | TradeRequestType::OkexNewUMOrder
            | TradeRequestType::OkexCancelMarginOrder
            | TradeRequestType::OkexCancelUMOrder => {
                unreachable!("Okex requests run via websocket; REST mapping not used")
            }
            TradeRequestType::GateUnifiedNewOrder
            | TradeRequestType::GateUnifiedCancelOrder
            | TradeRequestType::GateFuturesNewOrder
            | TradeRequestType::GateFuturesCancelOrder => {
                unreachable!("Gate requests run via websocket; REST mapping not used")
            }
            TradeRequestType::BybitNewMarginOrder
            | TradeRequestType::BybitNewUMOrder
            | TradeRequestType::BybitCancelMarginOrder
            | TradeRequestType::BybitCancelUMOrder => {
                unreachable!("Bybit requests run via websocket; REST mapping not used")
            }
            TradeRequestType::BitgetNewMarginOrder
            | TradeRequestType::BitgetNewUMOrder
            | TradeRequestType::BitgetCancelMarginOrder
            | TradeRequestType::BitgetCancelUMOrder => {
                unreachable!("Bitget requests run via websocket; REST mapping not used")
            }
        }
    }

    /// 检查请求类型是否需要签名（仅用于 REST）
    pub fn requires_signature(request_type: TradeRequestType) -> bool {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => true,
            TradeRequestType::BinanceNewUMConditionalOrder => true,
            TradeRequestType::BinanceNewMarginOrder => true,
            TradeRequestType::BinanceCancelUMOrder => true,
            TradeRequestType::BinanceCancelAllUMOrders => true,
            TradeRequestType::BinanceCancelUMConditionalOrder => true,
            TradeRequestType::BinanceCancelAllUMConditionalOrders => true,
            TradeRequestType::BinanceCancelMarginOrder => true,
            TradeRequestType::BinanceModifyUMOrder => true,
            TradeRequestType::BinanceUMSetLeverage => true,
            TradeRequestType::BinanceUniversalTransfer => true,
            TradeRequestType::BinanceWsNewUMOrder
            | TradeRequestType::BinanceWsCancelUMOrder
            | TradeRequestType::BinanceWsNewMarginOrder
            | TradeRequestType::BinanceWsCancelMarginOrder => {
                unreachable!("Binance ws requests run via websocket; REST mapping not used")
            }
            TradeRequestType::OkexNewMarginOrder
            | TradeRequestType::OkexNewUMOrder
            | TradeRequestType::OkexCancelMarginOrder
            | TradeRequestType::OkexCancelUMOrder => {
                unreachable!("Okex requests run via websocket; REST mapping not used")
            }
            TradeRequestType::GateUnifiedNewOrder
            | TradeRequestType::GateUnifiedCancelOrder
            | TradeRequestType::GateFuturesNewOrder
            | TradeRequestType::GateFuturesCancelOrder => {
                unreachable!("Gate requests run via websocket; REST mapping not used")
            }
            TradeRequestType::BybitNewMarginOrder
            | TradeRequestType::BybitNewUMOrder
            | TradeRequestType::BybitCancelMarginOrder
            | TradeRequestType::BybitCancelUMOrder => {
                unreachable!("Bybit requests run via websocket; REST mapping not used")
            }
            TradeRequestType::BitgetNewMarginOrder
            | TradeRequestType::BitgetNewUMOrder
            | TradeRequestType::BitgetCancelMarginOrder
            | TradeRequestType::BitgetCancelUMOrder => {
                unreachable!("Bitget requests run via websocket; REST mapping not used")
            }
        }
    }

    /// 检查请求类型是否需要API Key（仅用于 REST）
    pub fn requires_api_key(request_type: TradeRequestType) -> bool {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => true,
            TradeRequestType::BinanceNewUMConditionalOrder => true,
            TradeRequestType::BinanceNewMarginOrder => true,
            TradeRequestType::BinanceCancelUMOrder => true,
            TradeRequestType::BinanceCancelAllUMOrders => true,
            TradeRequestType::BinanceCancelUMConditionalOrder => true,
            TradeRequestType::BinanceCancelAllUMConditionalOrders => true,
            TradeRequestType::BinanceCancelMarginOrder => true,
            TradeRequestType::BinanceModifyUMOrder => true,
            TradeRequestType::BinanceUMSetLeverage => true,
            TradeRequestType::BinanceUniversalTransfer => true,
            TradeRequestType::BinanceWsNewUMOrder
            | TradeRequestType::BinanceWsCancelUMOrder
            | TradeRequestType::BinanceWsNewMarginOrder
            | TradeRequestType::BinanceWsCancelMarginOrder => {
                unreachable!("Binance ws requests run via websocket; REST mapping not used")
            }
            TradeRequestType::OkexNewMarginOrder
            | TradeRequestType::OkexNewUMOrder
            | TradeRequestType::OkexCancelMarginOrder
            | TradeRequestType::OkexCancelUMOrder => {
                unreachable!("Okex requests run via websocket; REST mapping not used")
            }
            TradeRequestType::GateUnifiedNewOrder
            | TradeRequestType::GateUnifiedCancelOrder
            | TradeRequestType::GateFuturesNewOrder
            | TradeRequestType::GateFuturesCancelOrder => {
                unreachable!("Gate requests run via websocket; REST mapping not used")
            }
            TradeRequestType::BybitNewMarginOrder
            | TradeRequestType::BybitNewUMOrder
            | TradeRequestType::BybitCancelMarginOrder
            | TradeRequestType::BybitCancelUMOrder => {
                unreachable!("Bybit requests run via websocket; REST mapping not used")
            }
            TradeRequestType::BitgetNewMarginOrder
            | TradeRequestType::BitgetNewUMOrder
            | TradeRequestType::BitgetCancelMarginOrder
            | TradeRequestType::BitgetCancelUMOrder => {
                unreachable!("Bitget requests run via websocket; REST mapping not used")
            }
        }
    }

    pub fn counts_toward_order_limit(request_type: TradeRequestType) -> bool {
        !matches!(request_type, TradeRequestType::BinanceUniversalTransfer)
    }
}

#[cfg(test)]
mod tests {
    use super::TradeTypeMapping;
    use crate::trade_engine::trade_request::TradeRequestType;

    #[test]
    fn binance_universal_transfer_maps_to_sapi() {
        assert!(!TradeTypeMapping::is_websocket(
            TradeRequestType::BinanceUniversalTransfer
        ));
        assert_eq!(
            TradeTypeMapping::get_endpoint(TradeRequestType::BinanceUniversalTransfer),
            "/sapi/v1/asset/transfer"
        );
        assert_eq!(
            TradeTypeMapping::get_method(TradeRequestType::BinanceUniversalTransfer),
            "POST"
        );
        assert_eq!(
            TradeTypeMapping::get_weight(TradeRequestType::BinanceUniversalTransfer),
            900
        );
        assert!(TradeTypeMapping::requires_signature(
            TradeRequestType::BinanceUniversalTransfer
        ));
        assert!(TradeTypeMapping::requires_api_key(
            TradeRequestType::BinanceUniversalTransfer
        ));
        assert!(!TradeTypeMapping::counts_toward_order_limit(
            TradeRequestType::BinanceUniversalTransfer
        ));
    }
}
