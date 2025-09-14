use super::trade_rsp_msg::{TradeRequestType, TradeResponseType};

pub struct TradeTypeMapping;

impl TradeTypeMapping {
    /// 根据请求类型获取endpoint
    pub fn get_endpoint(request_type: TradeRequestType) -> &'static str {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => "/papi/v1/um/order",
            TradeRequestType::BinanceNewUMConditionalOrder => "/papi/v1/um/conditional/order",
            TradeRequestType::BinanceNewMarginOrder => "/papi/v1/margin/order",
            TradeRequestType::BinanceCancelUMOrder => "/papi/v1/um/order",
        }
    }

    /// 根据请求类型获取HTTP方法
    pub fn get_method(request_type: TradeRequestType) -> &'static str {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => "POST",
            TradeRequestType::BinanceNewUMConditionalOrder => "POST",
            TradeRequestType::BinanceNewMarginOrder => "POST",
            TradeRequestType::BinanceCancelUMOrder => "DELETE",
        }
    }

    /// 根据请求类型获取API权重
    pub fn get_weight(request_type: TradeRequestType) -> u32 {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => 1,
            TradeRequestType::BinanceNewUMConditionalOrder => 1,
            TradeRequestType::BinanceNewMarginOrder => 1,
            TradeRequestType::BinanceCancelUMOrder => 1,
        }
    }

    /// 根据请求类型获取对应的响应类型
    pub fn get_response_type(request_type: TradeRequestType) -> TradeResponseType {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => TradeResponseType::BinanceNewUMOrder,
            TradeRequestType::BinanceNewUMConditionalOrder => TradeResponseType::BinanceNewUMConditionalOrder,
            TradeRequestType::BinanceNewMarginOrder => TradeResponseType::BinanceNewMarginOrder,
            TradeRequestType::BinanceCancelUMOrder => TradeResponseType::BinanceCancelUMOrder,
        }
    }

    /// 检查请求类型是否需要签名
    pub fn requires_signature(request_type: TradeRequestType) -> bool {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => true,
            TradeRequestType::BinanceNewUMConditionalOrder => true,
            TradeRequestType::BinanceNewMarginOrder => true,
            TradeRequestType::BinanceCancelUMOrder => true,
        }
    }

    /// 检查请求类型是否需要API Key
    pub fn requires_api_key(request_type: TradeRequestType) -> bool {
        match request_type {
            TradeRequestType::BinanceNewUMOrder => true,
            TradeRequestType::BinanceNewUMConditionalOrder => true,
            TradeRequestType::BinanceNewMarginOrder => true,
            TradeRequestType::BinanceCancelUMOrder => true,
        }
    }
}