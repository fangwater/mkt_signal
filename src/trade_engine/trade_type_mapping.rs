use super::trade_request::TradeRequestType;

pub struct TradeTypeMapping;

impl TradeTypeMapping {
    /// 判断请求类型是否走 WebSocket
    pub fn is_websocket(request_type: TradeRequestType) -> bool {
        match request_type {
            // Binance 所有请求走 REST
            TradeRequestType::BinanceNewUMOrder
            | TradeRequestType::BinanceNewUMConditionalOrder
            | TradeRequestType::BinanceNewMarginOrder
            | TradeRequestType::BinanceCancelUMOrder
            | TradeRequestType::BinanceCancelAllUMOrders
            | TradeRequestType::BinanceCancelUMConditionalOrder
            | TradeRequestType::BinanceCancelAllUMConditionalOrders
            | TradeRequestType::BinanceCancelMarginOrder
            | TradeRequestType::BinanceModifyUMOrder
            | TradeRequestType::BinanceUMSetLeverage => false,

            // OKEx 所有请求走 WebSocket
            TradeRequestType::OkexNewMarginOrder
            | TradeRequestType::OkexNewUMOrder
            | TradeRequestType::OkexCancelMarginOrder
            | TradeRequestType::OkexCancelUMOrder => true,

            // Gate 统一账户 / 合约走 WebSocket
            TradeRequestType::GateUnifiedNewOrder
            | TradeRequestType::GateUnifiedCancelOrder
            | TradeRequestType::GateFuturesNewOrder
            | TradeRequestType::GateFuturesCancelOrder => true,
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
        }
    }
}
