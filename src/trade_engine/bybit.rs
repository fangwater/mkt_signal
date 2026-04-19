use std::convert::TryFrom;

use bytes::{BufMut, Bytes, BytesMut};
use serde_json::{json, Value};

use crate::common::bybit_account_msg::BybitBasicOrderMsg;
use crate::common::tick_math::QuantizedValue;
use crate::pre_trade::order_manager::{OrderType, Side};

use super::trade_request::{TradeRequestHeader, TradeRequestType};

const DEFAULT_RECV_WINDOW_MS: i64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BybitCategory {
    Spot,
    Linear,
}

impl BybitCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            BybitCategory::Spot => "spot",
            BybitCategory::Linear => "linear",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BybitNewOrderParams {
    pub side: Side,
    pub order_type: OrderType,
    pub reduce_only: bool,
    pub is_leverage: bool,
    pub quantity_qv: QuantizedValue,
    pub price_qv: QuantizedValue,
    pub symbol: String,
}

impl BybitNewOrderParams {
    const MIN_BIN_LEN: usize = 1 + 1 + 1 + 1 + 8 + 4 + 8 + 8 + 4 + 8 + 1;

    pub fn to_bytes(&self) -> Option<Bytes> {
        let symbol_bytes = self.symbol.as_bytes();
        if symbol_bytes.len() > u8::MAX as usize {
            return None;
        }

        let (qty_tick_i64, qty_tick_exp) = self.quantity_qv.get_tick_parts();
        let (price_tick_i64, price_tick_exp) = self.price_qv.get_tick_parts();

        let mut buf = BytesMut::with_capacity(Self::MIN_BIN_LEN + symbol_bytes.len());
        buf.put_u8(self.side.to_u8());
        buf.put_u8(self.order_type.to_u8());
        buf.put_u8(self.reduce_only as u8);
        buf.put_u8(self.is_leverage as u8);
        buf.put_i64_le(qty_tick_i64);
        buf.put_i32_le(qty_tick_exp);
        buf.put_i64_le(self.quantity_qv.get_count());
        buf.put_i64_le(price_tick_i64);
        buf.put_i32_le(price_tick_exp);
        buf.put_i64_le(self.price_qv.get_count());
        buf.put_u8(symbol_bytes.len() as u8);
        buf.put_slice(symbol_bytes);
        Some(buf.freeze())
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        if raw.len() < Self::MIN_BIN_LEN {
            return None;
        }

        let side = Side::from_u8(raw[0])?;
        let order_type = OrderType::from_u8(raw[1])?;
        let reduce_only = raw[2] != 0;
        let is_leverage = raw[3] != 0;
        let qty_tick_i64 = i64::from_le_bytes(raw[4..12].try_into().ok()?);
        let qty_tick_exp = i32::from_le_bytes(raw[12..16].try_into().ok()?);
        let qty_count = i64::from_le_bytes(raw[16..24].try_into().ok()?);
        let price_tick_i64 = i64::from_le_bytes(raw[24..32].try_into().ok()?);
        let price_tick_exp = i32::from_le_bytes(raw[32..36].try_into().ok()?);
        let price_count = i64::from_le_bytes(raw[36..44].try_into().ok()?);
        let symbol_len = raw[44] as usize;

        if raw.len() < Self::MIN_BIN_LEN + symbol_len {
            return None;
        }

        let symbol = std::str::from_utf8(&raw[45..45 + symbol_len]).ok()?;

        Some(Self {
            side,
            order_type,
            reduce_only,
            is_leverage,
            quantity_qv: QuantizedValue::from_parts(qty_tick_i64, qty_tick_exp, qty_count),
            price_qv: QuantizedValue::from_parts(price_tick_i64, price_tick_exp, price_count),
            symbol: symbol.to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct BybitCancelOrderParams {
    pub symbol: String,
    pub order_link_id: i64,
}

impl BybitCancelOrderParams {
    const MIN_BIN_LEN: usize = 8 + 1;

    pub fn to_bytes(&self) -> Option<Bytes> {
        let symbol_bytes = self.symbol.as_bytes();
        if symbol_bytes.len() > u8::MAX as usize {
            return None;
        }
        let mut buf = BytesMut::with_capacity(Self::MIN_BIN_LEN + symbol_bytes.len());
        buf.put_i64_le(self.order_link_id);
        buf.put_u8(symbol_bytes.len() as u8);
        buf.put_slice(symbol_bytes);
        Some(buf.freeze())
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        if raw.len() < Self::MIN_BIN_LEN {
            return None;
        }
        let order_link_id = i64::from_le_bytes(raw[0..8].try_into().ok()?);
        let symbol_len = raw[8] as usize;
        if raw.len() < Self::MIN_BIN_LEN + symbol_len {
            return None;
        }
        let symbol = std::str::from_utf8(&raw[9..9 + symbol_len]).ok()?;
        Some(Self {
            symbol: symbol.to_string(),
            order_link_id,
        })
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BybitNewOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BybitCancelOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,
}

pub trait ToBybitWsJson {
    fn to_ws_json(&self, req_id: &str, timestamp_ms: i64) -> Option<Value>;
}

impl BybitNewOrderRequest {
    fn create_with_type(
        req_type: TradeRequestType,
        create_time: i64,
        client_order_id: i64,
        params: Bytes,
    ) -> Self {
        let header = TradeRequestHeader {
            msg_type: req_type as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };
        Self { header, params }
    }

    pub fn create_margin(
        create_time: i64,
        client_order_id: i64,
        params: BybitNewOrderParams,
    ) -> Option<Self> {
        Some(Self::create_with_type(
            TradeRequestType::BybitNewMarginOrder,
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn create_um(
        create_time: i64,
        client_order_id: i64,
        params: BybitNewOrderParams,
    ) -> Option<Self> {
        Some(Self::create_with_type(
            TradeRequestType::BybitNewUMOrder,
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 4 + 8 + 8 + self.params.len();
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.header.msg_type);
        buf.put_u32_le(self.header.params_length);
        buf.put_i64_le(self.header.create_time);
        buf.put_i64_le(self.header.client_order_id);
        buf.put(self.params.clone());
        buf.freeze()
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 24 {
            return None;
        }
        let msg_type = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        let req_type = TradeRequestType::try_from(msg_type).ok()?;
        if req_type != TradeRequestType::BybitNewMarginOrder
            && req_type != TradeRequestType::BybitNewUMOrder
        {
            return None;
        }
        let params_length = u32::from_le_bytes(buf[4..8].try_into().ok()?) as usize;
        let create_time = i64::from_le_bytes(buf[8..16].try_into().ok()?);
        let client_order_id = i64::from_le_bytes(buf[16..24].try_into().ok()?);
        if buf.len() < 24 + params_length {
            return None;
        }
        let params = Bytes::copy_from_slice(&buf[24..24 + params_length]);
        let header = TradeRequestHeader {
            msg_type,
            params_length: params_length as u32,
            create_time,
            client_order_id,
        };
        Some(Self { header, params })
    }

    pub fn params_struct(&self) -> Option<BybitNewOrderParams> {
        BybitNewOrderParams::from_bytes(&self.params)
    }
}

impl BybitCancelOrderRequest {
    fn create_with_type(
        req_type: TradeRequestType,
        create_time: i64,
        client_order_id: i64,
        params: Bytes,
    ) -> Self {
        let header = TradeRequestHeader {
            msg_type: req_type as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };
        Self { header, params }
    }

    pub fn create_margin(
        create_time: i64,
        client_order_id: i64,
        params: BybitCancelOrderParams,
    ) -> Option<Self> {
        Some(Self::create_with_type(
            TradeRequestType::BybitCancelMarginOrder,
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn create_um(
        create_time: i64,
        client_order_id: i64,
        params: BybitCancelOrderParams,
    ) -> Option<Self> {
        Some(Self::create_with_type(
            TradeRequestType::BybitCancelUMOrder,
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 4 + 8 + 8 + self.params.len();
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.header.msg_type);
        buf.put_u32_le(self.header.params_length);
        buf.put_i64_le(self.header.create_time);
        buf.put_i64_le(self.header.client_order_id);
        buf.put(self.params.clone());
        buf.freeze()
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 24 {
            return None;
        }
        let msg_type = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        let req_type = TradeRequestType::try_from(msg_type).ok()?;
        if req_type != TradeRequestType::BybitCancelMarginOrder
            && req_type != TradeRequestType::BybitCancelUMOrder
        {
            return None;
        }
        let params_length = u32::from_le_bytes(buf[4..8].try_into().ok()?) as usize;
        let create_time = i64::from_le_bytes(buf[8..16].try_into().ok()?);
        let client_order_id = i64::from_le_bytes(buf[16..24].try_into().ok()?);
        if buf.len() < 24 + params_length {
            return None;
        }
        let params = Bytes::copy_from_slice(&buf[24..24 + params_length]);
        let header = TradeRequestHeader {
            msg_type,
            params_length: params_length as u32,
            create_time,
            client_order_id,
        };
        Some(Self { header, params })
    }

    pub fn params_struct(&self) -> Option<BybitCancelOrderParams> {
        BybitCancelOrderParams::from_bytes(&self.params)
    }
}

impl ToBybitWsJson for BybitNewOrderRequest {
    fn to_ws_json(&self, req_id: &str, timestamp_ms: i64) -> Option<Value> {
        let req_type = TradeRequestType::try_from(self.header.msg_type).ok()?;
        let params = self.params_struct()?;
        let category = bybit_category_for_req(req_type)?;

        let mut arg = json!({
            "category": category.as_str(),
            "symbol": params.symbol,
            "side": params.side.as_str(),
            "orderType": bybit_order_type_str(params.order_type),
            "qty": params.quantity_qv.decimal_string(),
            "orderLinkId": self.header.client_order_id.to_string(),
        });

        if let Some(map) = arg.as_object_mut() {
            if params.order_type.is_limit() {
                map.insert(
                    "timeInForce".to_string(),
                    json!(bybit_time_in_force_str(req_type, params.order_type)),
                );
                map.insert("price".to_string(), json!(params.price_qv.decimal_string()));
            } else if req_type == TradeRequestType::BybitNewMarginOrder && params.side.is_buy() {
                map.insert("marketUnit".to_string(), json!("baseCoin"));
            }

            if req_type == TradeRequestType::BybitNewMarginOrder {
                map.insert(
                    "isLeverage".to_string(),
                    json!(if params.is_leverage { 1 } else { 0 }),
                );
                map.insert("orderFilter".to_string(), json!("Order"));
            } else {
                map.insert("reduceOnly".to_string(), json!(params.reduce_only));
            }
        }

        Some(json!({
            "reqId": req_id,
            "header": {
                "X-BAPI-TIMESTAMP": timestamp_ms.to_string(),
                "X-BAPI-RECV-WINDOW": DEFAULT_RECV_WINDOW_MS.to_string(),
            },
            "op": "order.create",
            "args": [arg],
        }))
    }
}

impl ToBybitWsJson for BybitCancelOrderRequest {
    fn to_ws_json(&self, req_id: &str, timestamp_ms: i64) -> Option<Value> {
        let req_type = TradeRequestType::try_from(self.header.msg_type).ok()?;
        let params = self.params_struct()?;
        let category = bybit_category_for_req(req_type)?;

        let mut arg = json!({
            "category": category.as_str(),
            "symbol": params.symbol,
            "orderLinkId": params.order_link_id.to_string(),
        });
        if req_type == TradeRequestType::BybitCancelMarginOrder {
            if let Some(map) = arg.as_object_mut() {
                map.insert("orderFilter".to_string(), json!("Order"));
            }
        }

        Some(json!({
            "reqId": req_id,
            "header": {
                "X-BAPI-TIMESTAMP": timestamp_ms.to_string(),
                "X-BAPI-RECV-WINDOW": DEFAULT_RECV_WINDOW_MS.to_string(),
            },
            "op": "order.cancel",
            "args": [arg],
        }))
    }
}

#[derive(Debug, Clone)]
pub struct BybitWsOrderResponse {
    pub req_id: String,
    pub ret_code: i32,
    pub ret_msg: String,
    pub op: String,
    pub order_id: String,
    pub order_link_id: String,
}

impl BybitWsOrderResponse {
    pub fn from_json_str(payload: &str) -> Option<Self> {
        let val: Value = serde_json::from_str(payload).ok()?;
        Self::from_json_value(&val)
    }

    pub fn from_json_value(val: &Value) -> Option<Self> {
        let obj = val.as_object()?;
        let op = obj.get("op")?.as_str()?.to_string();
        if op != "order.create" && op != "order.cancel" {
            return None;
        }

        let data = obj.get("data").and_then(|v| v.as_object());
        let order_id = data
            .and_then(|d| d.get("orderId"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let order_link_id = data
            .and_then(|d| d.get("orderLinkId"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        Some(Self {
            req_id: obj
                .get("reqId")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            ret_code: parse_i32(obj.get("retCode")).unwrap_or(0),
            ret_msg: obj
                .get("retMsg")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            op,
            order_id,
            order_link_id,
        })
    }

    pub fn transport_id(&self) -> Option<i64> {
        self.req_id.parse::<i64>().ok()
    }

    pub fn client_order_id(&self) -> Option<i64> {
        self.order_link_id.parse::<i64>().ok()
    }

    pub fn order_id_i64(&self) -> i64 {
        BybitBasicOrderMsg::stable_i64_from_str(&self.order_id)
    }
}

fn bybit_category_for_req(req_type: TradeRequestType) -> Option<BybitCategory> {
    match req_type {
        TradeRequestType::BybitNewMarginOrder | TradeRequestType::BybitCancelMarginOrder => {
            Some(BybitCategory::Spot)
        }
        TradeRequestType::BybitNewUMOrder | TradeRequestType::BybitCancelUMOrder => {
            Some(BybitCategory::Linear)
        }
        _ => None,
    }
}

fn bybit_order_type_str(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Limit | OrderType::StopLossLimit | OrderType::TakeProfitLimit => "Limit",
        OrderType::Market
        | OrderType::StopLoss
        | OrderType::TakeProfit
        | OrderType::StopMarket
        | OrderType::TakeProfitMarket => "Market",
    }
}

fn bybit_time_in_force_str(req_type: TradeRequestType, order_type: OrderType) -> &'static str {
    if !order_type.is_limit() {
        return "IOC";
    }
    match req_type {
        TradeRequestType::BybitNewMarginOrder | TradeRequestType::BybitNewUMOrder => "PostOnly",
        _ => "GTC",
    }
}

fn parse_i32(value: Option<&Value>) -> Option<i32> {
    value.and_then(|val| {
        if let Some(n) = val.as_i64() {
            i32::try_from(n).ok()
        } else if let Some(n) = val.as_u64() {
            i32::try_from(n).ok()
        } else if let Some(s) = val.as_str() {
            s.parse::<i32>().ok()
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bybit_margin_payload_uses_post_only_and_margin_flags() {
        let params = BybitNewOrderParams {
            side: Side::Buy,
            order_type: OrderType::Limit,
            reduce_only: false,
            is_leverage: true,
            quantity_qv: QuantizedValue::from_decimal(0.25).unwrap(),
            price_qv: QuantizedValue::from_decimal(123.45).unwrap(),
            symbol: "BTCUSDT".to_string(),
        };
        let req = BybitNewOrderRequest::create_margin(1, 42, params).unwrap();
        let payload = req.to_ws_json("123", 1_711_001_595_207).unwrap();
        let arg = payload["args"].as_array().unwrap().first().unwrap();

        assert_eq!(payload["op"], json!("order.create"));
        assert_eq!(arg["category"], json!("spot"));
        assert_eq!(arg["timeInForce"], json!("PostOnly"));
        assert_eq!(arg["isLeverage"], json!(1));
        assert_eq!(arg["orderFilter"], json!("Order"));
        assert_eq!(arg["orderLinkId"], json!("42"));
    }

    #[test]
    fn bybit_linear_market_buy_omits_price_and_keeps_reduce_only() {
        let params = BybitNewOrderParams {
            side: Side::Sell,
            order_type: OrderType::Market,
            reduce_only: true,
            is_leverage: false,
            quantity_qv: QuantizedValue::from_decimal(1.0).unwrap(),
            price_qv: QuantizedValue::zero(),
            symbol: "ETHUSDT".to_string(),
        };
        let req = BybitNewOrderRequest::create_um(1, 99, params).unwrap();
        let payload = req.to_ws_json("456", 1_711_001_595_207).unwrap();
        let arg = payload["args"].as_array().unwrap().first().unwrap();

        assert_eq!(arg["category"], json!("linear"));
        assert_eq!(arg["orderType"], json!("Market"));
        assert_eq!(arg["reduceOnly"], json!(true));
        assert!(arg.get("price").is_none());
    }

    #[test]
    fn bybit_cancel_payload_uses_order_link_id() {
        let params = BybitCancelOrderParams {
            symbol: "BTCUSDT".to_string(),
            order_link_id: 42,
        };
        let req = BybitCancelOrderRequest::create_um(1, 100, params).unwrap();
        let payload = req.to_ws_json("789", 1_711_001_595_207).unwrap();
        let arg = payload["args"].as_array().unwrap().first().unwrap();

        assert_eq!(payload["op"], json!("order.cancel"));
        assert_eq!(arg["category"], json!("linear"));
        assert_eq!(arg["orderLinkId"], json!("42"));
    }

    #[test]
    fn parses_bybit_ws_order_response() {
        let payload = r#"{"reqId":"123","retCode":0,"retMsg":"OK","op":"order.create","data":{"orderId":"abcdef","orderLinkId":"42"}}"#;
        let resp = BybitWsOrderResponse::from_json_str(payload).unwrap();
        assert_eq!(resp.transport_id(), Some(123));
        assert_eq!(resp.client_order_id(), Some(42));
        assert_eq!(resp.ret_code, 0);
        assert!(resp.order_id_i64() > 0);
    }
}
