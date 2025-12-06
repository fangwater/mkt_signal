use super::trade_request::TradeRequestType;

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
            TradeRequestType::BinanceCancelAllUMConditionalOrders => {
                "/papi/v1/um/conditional/allOpenOrders"
            }
            TradeRequestType::BinanceCancelMarginOrder => "/papi/v1/margin/order",
            TradeRequestType::BinanceModifyUMOrder => "/papi/v1/um/order",
            TradeRequestType::BinanceQueryUMOrder => "/papi/v1/um/order",
            TradeRequestType::BinanceQueryUMOpenOrder => "/papi/v1/um/openOrder",
            TradeRequestType::BinanceUMSetLeverage => "/papi/v1/um/leverage",
            TradeRequestType::OkexNewMarginOrder => "/api/v5/trade/order",
            TradeRequestType::OkexNewUMOrder => "/api/v5/trade/order",
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
            TradeRequestType::BinanceUMSetLeverage => "POST",
            TradeRequestType::OkexNewMarginOrder => "POST",
            TradeRequestType::OkexNewUMOrder => "POST",
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
            TradeRequestType::BinanceUMSetLeverage => 1,
            TradeRequestType::OkexNewMarginOrder => 1,
            TradeRequestType::OkexNewUMOrder => 1,
        }
    }

    // 已不再需要响应类型映射（发布原始 JSON 即可）

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
            TradeRequestType::BinanceUMSetLeverage => true,
            TradeRequestType::OkexNewMarginOrder => true,
            TradeRequestType::OkexNewUMOrder => true,
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
            TradeRequestType::BinanceUMSetLeverage => true,
            TradeRequestType::OkexNewMarginOrder => true,
            TradeRequestType::OkexNewUMOrder => true,
        }
    }
}
