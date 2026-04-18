use std::convert::TryFrom;

use bytes::{BufMut, Bytes, BytesMut};
use serde_json::{json, Value};

use crate::common::tick_math::QuantizedValue;
use crate::pre_trade::order_manager::Side;

use super::trade_request::{TradeRequestHeader, TradeRequestType};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OkexOrderType {
    Market = 0,
    Limit = 1,
    PostOnly = 2,
    Fok = 3,
    Ioc = 4,
    OptimalLimitIoc = 5,
    Mmp = 6,
    MmpAndPostOnly = 7,
    Elp = 8,
}

impl TryFrom<u8> for OkexOrderType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(OkexOrderType::Market),
            1 => Ok(OkexOrderType::Limit),
            2 => Ok(OkexOrderType::PostOnly),
            3 => Ok(OkexOrderType::Fok),
            4 => Ok(OkexOrderType::Ioc),
            5 => Ok(OkexOrderType::OptimalLimitIoc),
            6 => Ok(OkexOrderType::Mmp),
            7 => Ok(OkexOrderType::MmpAndPostOnly),
            8 => Ok(OkexOrderType::Elp),
            _ => Err(()),
        }
    }
}

impl OkexOrderType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OkexOrderType::Market => "market",
            OkexOrderType::Limit => "limit",
            OkexOrderType::PostOnly => "post_only",
            OkexOrderType::Fok => "fok",
            OkexOrderType::Ioc => "ioc",
            OkexOrderType::OptimalLimitIoc => "optimal_limit_ioc",
            OkexOrderType::Mmp => "mmp",
            OkexOrderType::MmpAndPostOnly => "mmp_and_post_only",
            OkexOrderType::Elp => "elp",
        }
    }
}

/// 紧凑的 okex 下单参数：
/// side | order_type | reduce_only
/// | qty_tick_i64 | qty_tick_exp | qty_count
/// | price_tick_i64 | price_tick_exp | price_count
/// | client_order_id | symbol_len | symbol_bytes
#[derive(Debug, Clone)]
pub struct OkexNewOrderParams {
    pub side: Side,
    pub order_type: OkexOrderType,
    pub reduce_only: bool,
    pub quantity_qv: QuantizedValue,
    pub price_qv: QuantizedValue,
    pub symbol: String,
    pub client_order_id: i64,
}

impl OkexNewOrderParams {
    const MIN_BIN_LEN: usize = 1 + 1 + 1 + 8 + 4 + 8 + 8 + 4 + 8 + 8 + 1;

    pub fn to_bytes(&self) -> Option<Bytes> {
        let symbol_bytes = self.symbol.as_bytes();
        if symbol_bytes.len() > u8::MAX as usize {
            return None;
        }

        let (qty_tick_i64, qty_tick_exp) = self.quantity_qv.get_tick_parts();
        let (price_tick_i64, price_tick_exp) = self.price_qv.get_tick_parts();

        let mut buf = BytesMut::with_capacity(Self::MIN_BIN_LEN + symbol_bytes.len());
        buf.put_u8(self.side.to_u8());
        buf.put_u8(self.order_type as u8);
        buf.put_u8(self.reduce_only as u8);
        buf.put_i64_le(qty_tick_i64);
        buf.put_i32_le(qty_tick_exp);
        buf.put_i64_le(self.quantity_qv.get_count());
        buf.put_i64_le(price_tick_i64);
        buf.put_i32_le(price_tick_exp);
        buf.put_i64_le(self.price_qv.get_count());
        buf.put_i64_le(self.client_order_id);
        buf.put_u8(symbol_bytes.len() as u8);
        buf.put_slice(symbol_bytes);
        Some(buf.freeze())
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        if raw.len() < Self::MIN_BIN_LEN {
            return None;
        }

        let side = Side::from_u8(raw[0])?;
        let order_type = OkexOrderType::try_from(raw[1]).ok()?;
        let reduce_only = raw[2] != 0;
        let qty_tick_i64 = i64::from_le_bytes(raw[3..11].try_into().ok()?);
        let qty_tick_exp = i32::from_le_bytes(raw[11..15].try_into().ok()?);
        let qty_count = i64::from_le_bytes(raw[15..23].try_into().ok()?);
        let price_tick_i64 = i64::from_le_bytes(raw[23..31].try_into().ok()?);
        let price_tick_exp = i32::from_le_bytes(raw[31..35].try_into().ok()?);
        let price_count = i64::from_le_bytes(raw[35..43].try_into().ok()?);
        let client_order_id = i64::from_le_bytes(raw[43..51].try_into().ok()?);
        let symbol_len = raw[51] as usize;

        if raw.len() < Self::MIN_BIN_LEN + symbol_len {
            return None;
        }

        let symbol = std::str::from_utf8(&raw[52..52 + symbol_len]).ok()?;

        Some(Self {
            side,
            order_type,
            reduce_only,
            quantity_qv: QuantizedValue::from_parts(qty_tick_i64, qty_tick_exp, qty_count),
            price_qv: QuantizedValue::from_parts(price_tick_i64, price_tick_exp, price_count),
            symbol: symbol.to_string(),
            client_order_id,
        })
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct OkexNewOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 紧凑的二进制参数
}

/// 紧凑的 okex 撤单参数：ord_id | cl_ord_id | inst_len | inst_bytes
#[derive(Debug, Clone)]
pub struct OkexCancelOrderParams {
    pub ord_id: i64,
    pub cl_ord_id: i64,
    pub inst_id: String,
}

impl OkexCancelOrderParams {
    const MIN_BIN_LEN: usize = 8 + 8 + 1; // ord_id + cl_ord_id + len

    pub fn to_bytes(&self) -> Option<Bytes> {
        let inst_bytes = self.inst_id.as_bytes();
        if inst_bytes.len() > u8::MAX as usize {
            return None;
        }
        let mut buf = BytesMut::with_capacity(Self::MIN_BIN_LEN + inst_bytes.len());
        buf.put_i64_le(self.ord_id);
        buf.put_i64_le(self.cl_ord_id);
        buf.put_u8(inst_bytes.len() as u8);
        buf.put_slice(inst_bytes);
        Some(buf.freeze())
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        if raw.len() < Self::MIN_BIN_LEN {
            return None;
        }
        let ord_id = i64::from_le_bytes(raw[0..8].try_into().ok()?);
        let cl_ord_id = i64::from_le_bytes(raw[8..16].try_into().ok()?);
        let inst_len = raw[16] as usize;
        if raw.len() < Self::MIN_BIN_LEN + inst_len {
            return None;
        }
        let inst_id = std::str::from_utf8(&raw[17..17 + inst_len]).ok()?;
        Some(Self {
            ord_id,
            cl_ord_id,
            inst_id: inst_id.to_string(),
        })
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct OkexCancelOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,
}

pub trait ToOkexWsJson {
    fn to_ws_json(&self, inst_id_code: i64) -> Option<Value>;
}

impl OkexNewOrderRequest {
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
        params: OkexNewOrderParams,
    ) -> Option<Self> {
        let mut params = params;
        params.client_order_id = client_order_id;
        let params = params.to_bytes()?;
        Some(Self::create_with_type(
            TradeRequestType::OkexNewMarginOrder,
            create_time,
            client_order_id,
            params,
        ))
    }

    pub fn create_um(
        create_time: i64,
        client_order_id: i64,
        params: OkexNewOrderParams,
    ) -> Option<Self> {
        let mut params = params;
        params.client_order_id = client_order_id;
        let params = params.to_bytes()?;
        Some(Self::create_with_type(
            TradeRequestType::OkexNewUMOrder,
            create_time,
            client_order_id,
            params,
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
        if req_type != TradeRequestType::OkexNewMarginOrder
            && req_type != TradeRequestType::OkexNewUMOrder
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

    pub fn params_struct(&self) -> Option<OkexNewOrderParams> {
        OkexNewOrderParams::from_bytes(&self.params)
    }
}

impl OkexCancelOrderRequest {
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
        params: OkexCancelOrderParams,
    ) -> Option<Self> {
        let params = params.to_bytes()?;
        Some(Self::create_with_type(
            TradeRequestType::OkexCancelMarginOrder,
            create_time,
            client_order_id,
            params,
        ))
    }

    pub fn create_um(
        create_time: i64,
        client_order_id: i64,
        params: OkexCancelOrderParams,
    ) -> Option<Self> {
        let params = params.to_bytes()?;
        Some(Self::create_with_type(
            TradeRequestType::OkexCancelUMOrder,
            create_time,
            client_order_id,
            params,
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
        if req_type != TradeRequestType::OkexCancelMarginOrder
            && req_type != TradeRequestType::OkexCancelUMOrder
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

    pub fn params_struct(&self) -> Option<OkexCancelOrderParams> {
        OkexCancelOrderParams::from_bytes(&self.params)
    }
}

impl ToOkexWsJson for OkexNewOrderRequest {
    fn to_ws_json(&self, inst_id_code: i64) -> Option<Value> {
        let req_type = TradeRequestType::try_from(self.header.msg_type).ok()?;
        let params = self.params_struct()?;

        // tdMode 映射：
        // - OkexMargin：当前工程语义为“保证金/PM”，使用 "cross"
        // - OkexFutures：使用 "cross"
        let td_mode = match req_type {
            TradeRequestType::OkexNewMarginOrder => "cross",
            TradeRequestType::OkexNewUMOrder => "cross",
            _ => return None,
        };

        let qty = params.quantity_qv.decimal_string();
        let price = params.price_qv.decimal_string();
        let cl_id = params.client_order_id.to_string();
        let ord_type_str = params.order_type.as_str();
        let args_obj = if params.order_type == OkexOrderType::Market {
            // 市价买单默认用 quote 计价，这里强制 tgtCcy=base_ccy 让 sz 表示基础币数量
            let mut obj = json!({
                "instIdCode": inst_id_code,
                "side": params.side.as_str_lower(),
                "ordType": ord_type_str,
                "sz": qty,
                "tdMode": td_mode,
                "clOrdId": cl_id,
            });
            if req_type == TradeRequestType::OkexNewMarginOrder && params.side.is_buy() {
                if let Some(map) = obj.as_object_mut() {
                    map.insert("tgtCcy".to_string(), json!("base_ccy"));
                }
            }
            if req_type == TradeRequestType::OkexNewUMOrder {
                if let Some(map) = obj.as_object_mut() {
                    map.insert("reduceOnly".to_string(), json!(params.reduce_only));
                }
            }
            obj
        } else {
            let mut obj = json!({
                "instIdCode": inst_id_code,
                "side": params.side.as_str_lower(),
                "ordType": ord_type_str,
                "sz": qty,
                "px": price,
                "tdMode": td_mode,
                "clOrdId": cl_id,
            });
            if req_type == TradeRequestType::OkexNewUMOrder {
                if let Some(map) = obj.as_object_mut() {
                    map.insert("reduceOnly".to_string(), json!(params.reduce_only));
                }
            }
            obj
        };
        Some(json!({
            "op": "order",
            "id": cl_id,
            "args": [args_obj]
        }))
    }
}

impl ToOkexWsJson for OkexCancelOrderRequest {
    fn to_ws_json(&self, inst_id_code: i64) -> Option<Value> {
        let req_type = TradeRequestType::try_from(self.header.msg_type).ok()?;
        if req_type != TradeRequestType::OkexCancelMarginOrder
            && req_type != TradeRequestType::OkexCancelUMOrder
        {
            return None;
        }
        let params = self.params_struct()?;
        let mut obj = json!({
            "instIdCode": inst_id_code,
        });
        if params.ord_id != 0 {
            if let Some(map) = obj.as_object_mut() {
                map.insert("ordId".to_string(), json!(params.ord_id.to_string()));
            }
        }
        let cancel_cl_id = if params.cl_ord_id != 0 {
            params.cl_ord_id
        } else {
            self.header.client_order_id
        };
        if cancel_cl_id != 0 {
            if let Some(map) = obj.as_object_mut() {
                map.insert("clOrdId".to_string(), json!(cancel_cl_id.to_string()));
            }
        }

        Some(json!({
            "op": "cancel-order",
            "id": self.header.client_order_id.to_string(),
            "args": [obj]
        }))
    }
}

#[derive(Debug, Clone)]
pub struct OkexWsOrderRespItem {
    pub cl_ord_id: i64,
    pub ord_id: i64,
    pub ts_ms: i64,
    pub status_code: i32,
    pub tag: String,
    pub status_msg: String,
}

#[derive(Debug, Clone)]
pub struct OkexWsOrderResponse {
    pub id: i64,
    pub code: i32,
    pub msg: String,
    pub in_time_us: i64,
    pub out_time_us: i64,
    pub op: String,
    pub data: Option<OkexWsOrderRespItem>, // OKX 返回 data 要么空数组，要么单元素
}

impl OkexWsOrderResponse {
    pub fn from_json_str(payload: &str) -> Option<Self> {
        let val: Value = serde_json::from_str(payload).ok()?;
        Self::from_json_value(&val)
    }

    fn from_json_value(val: &Value) -> Option<Self> {
        let obj = val.as_object()?;
        let op = obj
            .get("op")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if !op.eq_ignore_ascii_case("order") && !op.eq_ignore_ascii_case("cancel-order") {
            return None;
        }

        let id = parse_i64_field(obj.get("id"));
        let code = parse_i32_field(obj.get("code"));
        let msg = obj
            .get("msg")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let in_time_us = parse_i64_field(obj.get("inTime"));
        let out_time_us = parse_i64_field(obj.get("outTime"));

        let data = obj
            .get("data")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.get(0))
            .map(|item| {
                let cl_ord_id = parse_i64_field(item.get("clOrdId"));
                let ord_id = parse_i64_field(item.get("ordId"));
                let ts_ms = parse_i64_field(item.get("ts"));
                let status_code = parse_i32_field(item.get("sCode"));
                let tag = item
                    .get("tag")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let status_msg = item
                    .get("sMsg")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                OkexWsOrderRespItem {
                    cl_ord_id,
                    ord_id,
                    ts_ms,
                    status_code,
                    tag,
                    status_msg,
                }
            });

        Some(Self {
            id,
            code,
            msg,
            in_time_us,
            out_time_us,
            op,
            data,
        })
    }

    /// 优先从 data.clOrdId 获取 client_order_id，否则回退到顶层 id
    pub fn client_order_id(&self) -> Option<i64> {
        self.data
            .as_ref()
            .and_then(|d| {
                if d.cl_ord_id != 0 {
                    Some(d.cl_ord_id)
                } else {
                    None
                }
            })
            .or_else(|| if self.id != 0 { Some(self.id) } else { None })
    }

    /// 将 OKX WS 返回压缩为紧凑的二进制形式，字符串长度使用 u16 储存
    pub fn to_bytes(&self) -> Bytes {
        fn put_str(buf: &mut BytesMut, s: &str) {
            let truncated_len = s.len().min(u16::MAX as usize) as u16;
            buf.put_u16_le(truncated_len);
            buf.put_slice(&s.as_bytes()[..truncated_len as usize]);
        }

        let mut buf = BytesMut::new();
        buf.put_i64_le(self.id);
        buf.put_i32_le(self.code);
        buf.put_i64_le(self.in_time_us);
        buf.put_i64_le(self.out_time_us);
        put_str(&mut buf, &self.op);
        put_str(&mut buf, &self.msg);

        let has_data = self.data.is_some();
        buf.put_u8(has_data as u8);
        if let Some(item) = &self.data {
            buf.put_i64_le(item.cl_ord_id);
            buf.put_i64_le(item.ord_id);
            buf.put_i64_le(item.ts_ms);
            buf.put_i32_le(item.status_code);
            put_str(&mut buf, &item.tag);
            put_str(&mut buf, &item.status_msg);
        }

        buf.freeze()
    }
}

fn parse_i64_field(v: Option<&Value>) -> i64 {
    v.and_then(|val| {
        if let Some(n) = val.as_i64() {
            Some(n)
        } else if let Some(n) = val.as_u64() {
            Some(n as i64)
        } else if let Some(s) = val.as_str() {
            s.parse::<i64>().ok()
        } else {
            None
        }
    })
    .unwrap_or(0)
}

fn parse_i32_field(v: Option<&Value>) -> i32 {
    v.and_then(|val| {
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
    .unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn okex_new_order_ws_payload_uses_inst_id_code_only() {
        let params = OkexNewOrderParams {
            side: Side::Buy,
            order_type: OkexOrderType::Limit,
            reduce_only: false,
            quantity_qv: QuantizedValue::from_decimal(1.25).unwrap(),
            price_qv: QuantizedValue::from_decimal(123.45).unwrap(),
            symbol: "BTC-USDT-SWAP".to_string(),
            client_order_id: 42,
        };
        let req = OkexNewOrderRequest::create_um(1, 42, params).unwrap();
        let payload = req.to_ws_json(123456).unwrap();
        let arg = payload["args"].as_array().unwrap().first().unwrap();

        assert_eq!(arg["instIdCode"], json!(123456));
        assert!(arg.get("instId").is_none());
        assert_eq!(arg["clOrdId"], json!("42"));
        assert_eq!(arg["sz"], json!("1.25"));
        assert_eq!(arg["px"], json!("123.45"));
    }

    #[test]
    fn okex_cancel_order_ws_payload_uses_inst_id_code_only() {
        let params = OkexCancelOrderParams {
            ord_id: 88,
            cl_ord_id: 42,
            inst_id: "BTC-USDT-SWAP".to_string(),
        };
        let req = OkexCancelOrderRequest::create_um(1, 99, params).unwrap();
        let payload = req.to_ws_json(654321).unwrap();
        let arg = payload["args"].as_array().unwrap().first().unwrap();

        assert_eq!(arg["instIdCode"], json!(654321));
        assert!(arg.get("instId").is_none());
        assert_eq!(arg["ordId"], json!("88"));
        assert_eq!(arg["clOrdId"], json!("42"));
    }
}
