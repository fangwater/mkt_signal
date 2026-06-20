use std::convert::TryFrom;
use std::fmt::Write as _;

use bytes::{BufMut, Bytes, BytesMut};
use serde_json::Value;

use mkt_parsers::msg::bybit_account_msg::BybitBasicOrderMsg;
use order_common::{OrderType, Side};
use signal_common::tick_math::QuantizedValue;

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

    fn encoded_len(&self) -> Option<usize> {
        let symbol_len = self.symbol.len();
        if symbol_len > u8::MAX as usize {
            return None;
        }
        Some(Self::MIN_BIN_LEN + symbol_len)
    }

    fn write_to_buf(&self, buf: &mut BytesMut) -> Option<()> {
        let symbol_bytes = self.symbol.as_bytes();
        if symbol_bytes.len() > u8::MAX as usize {
            return None;
        }

        let (qty_tick_i64, qty_tick_exp) = self.quantity_qv.get_tick_parts();
        let (price_tick_i64, price_tick_exp) = self.price_qv.get_tick_parts();

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
        Some(())
    }

    pub fn to_bytes(&self) -> Option<Bytes> {
        let mut buf = BytesMut::with_capacity(self.encoded_len()?);
        self.write_to_buf(&mut buf)?;
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

    fn decode_raw(raw: &[u8]) -> Option<BybitNewOrderParamsRef<'_>> {
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

        Some(BybitNewOrderParamsRef {
            side,
            order_type,
            reduce_only,
            is_leverage,
            quantity_qv: QuantizedValue::from_parts(qty_tick_i64, qty_tick_exp, qty_count),
            price_qv: QuantizedValue::from_parts(price_tick_i64, price_tick_exp, price_count),
            symbol,
        })
    }
}

struct BybitNewOrderParamsRef<'a> {
    side: Side,
    order_type: OrderType,
    reduce_only: bool,
    is_leverage: bool,
    quantity_qv: QuantizedValue,
    price_qv: QuantizedValue,
    symbol: &'a str,
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
        let (order_link_id, symbol) = Self::decode_raw(raw)?;
        Some(Self {
            symbol: symbol.to_string(),
            order_link_id,
        })
    }

    fn decode_raw(raw: &[u8]) -> Option<(i64, &str)> {
        if raw.len() < Self::MIN_BIN_LEN {
            return None;
        }
        let order_link_id = i64::from_le_bytes(raw[0..8].try_into().ok()?);
        let symbol_len = raw[8] as usize;
        if raw.len() < Self::MIN_BIN_LEN + symbol_len {
            return None;
        }
        let symbol = std::str::from_utf8(&raw[9..9 + symbol_len]).ok()?;
        Some((order_link_id, symbol))
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

    fn to_bytes_with_type(
        req_type: TradeRequestType,
        create_time: i64,
        client_order_id: i64,
        params: &BybitNewOrderParams,
    ) -> Option<Bytes> {
        let params_len = params.encoded_len()?;
        let total_size = 4 + 4 + 8 + 8 + params_len;
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(req_type as u32);
        buf.put_u32_le(params_len as u32);
        buf.put_i64_le(create_time);
        buf.put_i64_le(client_order_id);
        params.write_to_buf(&mut buf)?;
        Some(buf.freeze())
    }

    pub fn margin_order_bytes(
        create_time: i64,
        client_order_id: i64,
        params: &BybitNewOrderParams,
    ) -> Option<Bytes> {
        Self::to_bytes_with_type(
            TradeRequestType::BybitNewMarginOrder,
            create_time,
            client_order_id,
            params,
        )
    }

    pub fn um_order_bytes(
        create_time: i64,
        client_order_id: i64,
        params: &BybitNewOrderParams,
    ) -> Option<Bytes> {
        Self::to_bytes_with_type(
            TradeRequestType::BybitNewUMOrder,
            create_time,
            client_order_id,
            params,
        )
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

    pub fn to_ws_json_string(&self, req_id: &str, timestamp_ms: i64) -> Option<String> {
        let req_type = TradeRequestType::try_from(self.header.msg_type).ok()?;
        let category = bybit_category_for_req(req_type)?;
        let (order_link_id, symbol) = BybitCancelOrderParams::decode_raw(&self.params)?;
        let mut out =
            bybit_payload_prefix(req_id, timestamp_ms, "order.cancel", 160 + symbol.len());
        push_json_field(&mut out, "category", category.as_str(), true);
        push_json_field(&mut out, "symbol", symbol, false);
        push_i64_string_field(&mut out, "orderLinkId", order_link_id, false);
        if req_type == TradeRequestType::BybitCancelMarginOrder {
            push_json_field(&mut out, "orderFilter", "Order", false);
        }
        out.push_str("}]}");
        Some(out)
    }
}

impl BybitNewOrderRequest {
    pub fn to_ws_json_string(&self, req_id: &str, timestamp_ms: i64) -> Option<String> {
        let req_type = TradeRequestType::try_from(self.header.msg_type).ok()?;
        let params = BybitNewOrderParams::decode_raw(&self.params)?;
        let category = bybit_category_for_req(req_type)?;
        let qty = params.quantity_qv.decimal_string();
        let price = params.price_qv.decimal_string();
        let mut out = bybit_payload_prefix(
            req_id,
            timestamp_ms,
            "order.create",
            192 + params.symbol.len() + qty.len() + price.len(),
        );

        push_json_field(&mut out, "category", category.as_str(), true);
        push_json_field(&mut out, "symbol", params.symbol, false);
        push_json_field(&mut out, "side", params.side.as_str(), false);
        push_json_field(
            &mut out,
            "orderType",
            bybit_order_type_str(params.order_type),
            false,
        );
        push_json_field(&mut out, "qty", &qty, false);
        push_i64_string_field(&mut out, "orderLinkId", self.header.client_order_id, false);

        if params.order_type.is_limit() {
            push_json_field(
                &mut out,
                "timeInForce",
                bybit_time_in_force_str(req_type, params.order_type),
                false,
            );
            push_json_field(&mut out, "price", &price, false);
        } else if req_type == TradeRequestType::BybitNewMarginOrder && params.side.is_buy() {
            push_json_field(&mut out, "marketUnit", "baseCoin", false);
        }

        if req_type == TradeRequestType::BybitNewMarginOrder {
            push_i32_field(
                &mut out,
                "isLeverage",
                if params.is_leverage { 1 } else { 0 },
                false,
            );
            push_json_field(&mut out, "orderFilter", "Order", false);
        } else {
            push_bool_field(&mut out, "reduceOnly", params.reduce_only, false);
        }

        out.push_str("}]}");
        Some(out)
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
    pub time_now_ms: i64,
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
        let parse_ms = |v: &Value| -> Option<i64> {
            if let Some(n) = v.as_i64() {
                Some(n)
            } else if let Some(n) = v.as_u64() {
                i64::try_from(n).ok()
            } else if let Some(s) = v.as_str() {
                s.parse::<i64>().ok()
            } else {
                None
            }
        };
        // Bybit v5 trade API 顶层 `time`（ms）；旧/部分响应放在 `header.Timenow`。两者择一，缺失则记 0。
        let time_now_ms = obj
            .get("time")
            .and_then(parse_ms)
            .or_else(|| {
                obj.get("header")
                    .and_then(|v| v.as_object())
                    .and_then(|header| header.get("Timenow"))
                    .and_then(parse_ms)
            })
            .unwrap_or(0);

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
            time_now_ms,
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

    pub fn order_status_u8(&self) -> u8 {
        if self.ret_code != 0 {
            return 0;
        }
        match self.op.as_str() {
            "order.create" => 1,
            "order.cancel" => 4,
            _ => 0,
        }
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

fn bybit_payload_prefix(
    req_id: &str,
    timestamp_ms: i64,
    op: &str,
    extra_capacity: usize,
) -> String {
    let mut out = String::with_capacity(extra_capacity + req_id.len());
    out.push_str("{\"reqId\":");
    push_json_string(&mut out, req_id);
    out.push_str(",\"header\":{\"X-BAPI-TIMESTAMP\":\"");
    write!(out, "{}", timestamp_ms).expect("write bybit timestamp");
    out.push_str("\",\"X-BAPI-RECV-WINDOW\":\"");
    write!(out, "{}", DEFAULT_RECV_WINDOW_MS).expect("write bybit recv window");
    out.push_str("\"},\"op\":");
    push_json_string(&mut out, op);
    out.push_str(",\"args\":[{");
    out
}

fn push_json_field(out: &mut String, key: &str, value: &str, first: bool) {
    if !first {
        out.push(',');
    }
    push_json_string(out, key);
    out.push(':');
    push_json_string(out, value);
}

fn push_i64_string_field(out: &mut String, key: &str, value: i64, first: bool) {
    if !first {
        out.push(',');
    }
    push_json_string(out, key);
    out.push_str(":\"");
    write!(out, "{}", value).expect("write bybit i64 string field");
    out.push('"');
}

fn push_i32_field(out: &mut String, key: &str, value: i32, first: bool) {
    if !first {
        out.push(',');
    }
    push_json_string(out, key);
    out.push(':');
    write!(out, "{}", value).expect("write bybit i32 field");
}

fn push_bool_field(out: &mut String, key: &str, value: bool, first: bool) {
    if !first {
        out.push(',');
    }
    push_json_string(out, key);
    out.push(':');
    out.push_str(if value { "true" } else { "false" });
}

fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if c <= '\u{1f}' => {
                write!(out, "\\u{:04x}", c as u32).expect("write json escape");
            }
            c => out.push(c),
        }
    }
    out.push('"');
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
    use serde_json::json;

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
        let payload: Value =
            serde_json::from_str(&req.to_ws_json_string("123", 1_711_001_595_207).unwrap())
                .unwrap();
        let arg = payload["args"].as_array().unwrap().first().unwrap();

        assert_eq!(payload["reqId"], json!("123"));
        assert_eq!(payload["op"], json!("order.create"));
        assert_eq!(
            payload["header"]["X-BAPI-TIMESTAMP"],
            json!("1711001595207")
        );
        assert_eq!(payload["header"]["X-BAPI-RECV-WINDOW"], json!("5000"));
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
        let payload: Value =
            serde_json::from_str(&req.to_ws_json_string("456", 1_711_001_595_207).unwrap())
                .unwrap();
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
        let payload: Value =
            serde_json::from_str(&req.to_ws_json_string("789", 1_711_001_595_207).unwrap())
                .unwrap();
        let arg = payload["args"].as_array().unwrap().first().unwrap();

        assert_eq!(payload["op"], json!("order.cancel"));
        assert_eq!(arg["category"], json!("linear"));
        assert_eq!(arg["orderLinkId"], json!("42"));
    }

    #[test]
    fn bybit_cancel_payload_fast_path_matches_json_shape() {
        let params = BybitCancelOrderParams {
            symbol: "BTCUSDT".to_string(),
            order_link_id: 42,
        };
        let req = BybitCancelOrderRequest::create_um(1, 100, params).unwrap();
        let payload: Value =
            serde_json::from_str(&req.to_ws_json_string("789", 1_711_001_595_207).unwrap())
                .unwrap();
        let arg = payload["args"].as_array().unwrap().first().unwrap();

        assert_eq!(payload["reqId"], json!("789"));
        assert_eq!(payload["op"], json!("order.cancel"));
        assert_eq!(
            payload["header"]["X-BAPI-TIMESTAMP"],
            json!("1711001595207")
        );
        assert_eq!(payload["header"]["X-BAPI-RECV-WINDOW"], json!("5000"));
        assert_eq!(arg["category"], json!("linear"));
        assert_eq!(arg["symbol"], json!("BTCUSDT"));
        assert_eq!(arg["orderLinkId"], json!("42"));
        assert!(arg.get("orderFilter").is_none());
    }

    #[test]
    fn bybit_margin_cancel_fast_path_keeps_order_filter() {
        let params = BybitCancelOrderParams {
            symbol: "ETHUSDT".to_string(),
            order_link_id: 43,
        };
        let req = BybitCancelOrderRequest::create_margin(1, 101, params).unwrap();
        let payload: Value =
            serde_json::from_str(&req.to_ws_json_string("790", 1_711_001_595_208).unwrap())
                .unwrap();
        let arg = payload["args"].as_array().unwrap().first().unwrap();

        assert_eq!(arg["category"], json!("spot"));
        assert_eq!(arg["symbol"], json!("ETHUSDT"));
        assert_eq!(arg["orderLinkId"], json!("43"));
        assert_eq!(arg["orderFilter"], json!("Order"));
    }

    #[test]
    fn parses_bybit_ws_order_response() {
        let payload = r#"{"reqId":"123","retCode":0,"retMsg":"OK","op":"order.create","data":{"orderId":"abcdef","orderLinkId":"42"},"time":1711001595209}"#;
        let resp = BybitWsOrderResponse::from_json_str(payload).unwrap();
        assert_eq!(resp.transport_id(), Some(123));
        assert_eq!(resp.client_order_id(), Some(42));
        assert_eq!(resp.ret_code, 0);
        assert_eq!(resp.time_now_ms, 1_711_001_595_209);
        assert_eq!(resp.order_status_u8(), 1);
        assert!(resp.order_id_i64() > 0);
    }

    #[test]
    fn parses_bybit_ws_order_response_without_order_link_id() {
        let payload = r#"{"reqId":"123","retCode":0,"retMsg":"OK","op":"order.create","data":{"orderId":"abcdef","orderLinkId":""},"time":1711001595209}"#;
        let resp = BybitWsOrderResponse::from_json_str(payload).unwrap();
        assert_eq!(resp.transport_id(), Some(123));
        assert_eq!(resp.client_order_id(), None);
        assert_eq!(resp.ret_code, 0);
        assert_eq!(resp.order_status_u8(), 1);
    }

    #[test]
    fn parses_bybit_ws_order_response_with_header_timenow_fallback() {
        let payload = r#"{"reqId":"123","retCode":0,"retMsg":"OK","op":"order.create","data":{"orderId":"abcdef","orderLinkId":"42"},"header":{"Timenow":"1711001595209"}}"#;
        let resp = BybitWsOrderResponse::from_json_str(payload).unwrap();
        assert_eq!(resp.transport_id(), Some(123));
        assert_eq!(resp.client_order_id(), Some(42));
        assert_eq!(resp.time_now_ms, 1_711_001_595_209);
    }

    #[test]
    fn parses_bybit_ws_order_response_without_time_field() {
        let payload = r#"{"reqId":"123","retCode":0,"retMsg":"OK","op":"order.create","data":{"orderId":"abcdef","orderLinkId":"42"}}"#;
        let resp = BybitWsOrderResponse::from_json_str(payload).unwrap();
        assert_eq!(resp.transport_id(), Some(123));
        assert_eq!(resp.time_now_ms, 0);
    }

    #[test]
    fn parses_failed_bybit_ws_order_response_without_order_link_id() {
        let payload = r#"{"reqId":"123","retCode":170217,"retMsg":"Only reduceOnly order is allowed","op":"order.create","data":{"orderId":"","orderLinkId":""},"time":1711001595209}"#;
        let resp = BybitWsOrderResponse::from_json_str(payload).unwrap();
        assert_eq!(resp.transport_id(), Some(123));
        assert_eq!(resp.client_order_id(), None);
        assert_eq!(resp.ret_code, 170217);
        assert_eq!(resp.ret_msg, "Only reduceOnly order is allowed");
        assert_eq!(resp.order_status_u8(), 0);
    }
}
