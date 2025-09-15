use super::trade_request::TradeRequestType;
use super::trade_response::TradeResponseType;

pub struct TradeTypeMapping;

impl TradeTypeMapping {
    /// 根据请求类型获取endpoint
    pub fn get_endpoint(request_type: TradeRequestType) -> &'static str {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => "/papi/v1/um/order",
            TradeRequestType::BinanceNewUMConditionalOrder => "/papi/v1/um/conditional/order",
            TradeRequestType::BinanceNewMarginOrder => "/papi/v1/margin/order",
            TradeRequestType::BinanceCancelUMOrder => "/papi/v1/um/order",
            TradeRequestType::BinanceCancelAllUMOrders => "/papi/v1/um/allOpenOrders",
            TradeRequestType::BinanceCancelUMConditionalOrder => "/papi/v1/um/conditional/order",
            TradeRequestType::BinanceCancelAllUMConditionalOrders => "/papi/v1/um/conditional/allOpenOrders",
            TradeRequestType::BinanceCancelMarginOrder => "/papi/v1/margin/order",
            TradeRequestType::BinanceModifyUMOrder => "/papi/v1/um/order",
            TradeRequestType::BinanceQueryUMOrder => "/papi/v1/um/order",
            TradeRequestType::BinanceQueryUMOpenOrder => "/papi/v1/um/openOrder",
        }
    }

    /// 根据请求类型获取HTTP方法
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
            TradeRequestType::BinanceQueryUMOrder => "GET",
            TradeRequestType::BinanceQueryUMOpenOrder => "GET",
        }
    }

    /// 根据请求类型获取API权重
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
            TradeRequestType::BinanceQueryUMOrder => 1,
            TradeRequestType::BinanceQueryUMOpenOrder => 1,
        }
    }

    /// 根据请求类型获取对应的响应类型
    pub fn get_response_type(request_type: TradeRequestType) -> TradeResponseType {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => TradeResponseType::BinanceNewUMOrder,
            TradeRequestType::BinanceNewUMConditionalOrder => TradeResponseType::BinanceNewUMConditionalOrder,
            TradeRequestType::BinanceNewMarginOrder => TradeResponseType::BinanceNewMarginOrder,
            TradeRequestType::BinanceCancelUMOrder => TradeResponseType::BinanceCancelUMOrder,
            TradeRequestType::BinanceCancelAllUMOrders => TradeResponseType::BinanceCancelAllUMOrders,
            TradeRequestType::BinanceCancelUMConditionalOrder => TradeResponseType::BinanceCancelUMConditionalOrder,
            TradeRequestType::BinanceCancelAllUMConditionalOrders => TradeResponseType::BinanceCancelAllUMConditionalOrders,
            TradeRequestType::BinanceCancelMarginOrder => TradeResponseType::BinanceCancelMarginOrder,
            TradeRequestType::BinanceModifyUMOrder => TradeResponseType::BinanceModifyUMOrder,
            TradeRequestType::BinanceQueryUMOrder => TradeResponseType::BinanceQueryUMOrder,
            TradeRequestType::BinanceQueryUMOpenOrder => TradeResponseType::BinanceQueryUMOpenOrder,
        }
    }

    /// 检查请求类型是否需要签名
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
            TradeRequestType::BinanceQueryUMOrder => true,
            TradeRequestType::BinanceQueryUMOpenOrder => true,
        }
    }

    /// 检查请求类型是否需要API Key
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
            TradeRequestType::BinanceQueryUMOrder => true,
            TradeRequestType::BinanceQueryUMOpenOrder => true,
        }
    }
}