use std::convert::TryFrom;

use bytes::{BufMut, Bytes, BytesMut};
use serde_json::{json, Value};

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

/// 紧凑的 okex 下单参数：side | order_type | qty | price | client_order_id | symbol_len | symbol_bytes
#[derive(Debug, Clone)]
pub struct OkexNewOrderParams {
    pub side: Side,
    pub order_type: OkexOrderType,
    pub quantity: f64,
    pub price: f64,
    pub symbol: String,
    pub client_order_id: i64,
}

impl OkexNewOrderParams {
    // side | order_type | qty | price | client_order_id | symbol_len | symbol_bytes
    const MIN_BIN_LEN: usize = 1 + 1 + 8 + 8 + 8 + 1;

    pub fn to_bytes(&self) -> Option<Bytes> {
        let symbol_bytes = self.symbol.as_bytes();
        if symbol_bytes.len() > u8::MAX as usize {
            return None;
        }

        let mut buf = BytesMut::with_capacity(Self::MIN_BIN_LEN + symbol_bytes.len());
        buf.put_u8(self.side.to_u8());
        buf.put_u8(self.order_type as u8);
        buf.put_f64_le(self.quantity);
        buf.put_f64_le(self.price);
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
        let quantity = f64::from_le_bytes(raw[2..10].try_into().ok()?);
        let price = f64::from_le_bytes(raw[10..18].try_into().ok()?);
        let client_order_id = i64::from_le_bytes(raw[18..26].try_into().ok()?);
        let symbol_len = raw[26] as usize;

        if raw.len() < Self::MIN_BIN_LEN + symbol_len {
            return None;
        }

        let symbol = std::str::from_utf8(&raw[27..27 + symbol_len]).ok()?;

        Some(Self {
            side,
            order_type,
            quantity,
            price,
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

pub trait ToOkexWsJson {
    fn to_ws_json(&self) -> Option<Value>;
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

impl ToOkexWsJson for OkexNewOrderRequest {
    fn to_ws_json(&self) -> Option<Value> {
        let req_type = TradeRequestType::try_from(self.header.msg_type).ok()?;
        let params = self.params_struct()?;
        let td_mode = match req_type {
            TradeRequestType::OkexNewMarginOrder | TradeRequestType::OkexNewUMOrder => "cross",
            _ => return None,
        };
        let qty = format_decimal(params.quantity);
        let price = format_decimal(params.price);
        let cl_id = params.client_order_id.to_string();
        let ord_type_str = params.order_type.as_str();
        let args_obj = if params.order_type == OkexOrderType::Market {
            json!({
                "instId": params.symbol,
                "side": params.side.as_str_lower(),
                "ordType": ord_type_str,
                "sz": qty,
                "tdMode": td_mode,
                "clOrdId": cl_id,
            })
        } else {
            json!({
                "instId": params.symbol,
                "side": params.side.as_str_lower(),
                "ordType": ord_type_str,
                "sz": qty,
                "px": price,
                "tdMode": td_mode,
                "clOrdId": cl_id,
            })
        };
        Some(json!({
            "op": "order",
            "id": cl_id,
            "args": [args_obj]
        }))
    }
}

fn format_decimal(value: f64) -> String {
    let mut s = format!("{:.15}", value);
    if let Some(dot_pos) = s.find('.') {
        while s.len() > dot_pos + 1 && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}
