use bytes::{BufMut, Bytes, BytesMut};
use log::debug;
use std::convert::TryFrom;

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum TradeRequestType {
    BinanceNewUMOrder = 4001,                   // 币安UM合约下单请求
    BinanceNewUMConditionalOrder = 4002,        // 币安UM条件单下单请求
    BinanceNewMarginOrder = 4003,               // 币安现货杠杆下单请求
    BinanceCancelUMOrder = 4004,                // 币安UM合约撤单请求
    BinanceCancelAllUMOrders = 4005,            // 币安UM合约撤销全部订单请求
    BinanceCancelUMConditionalOrder = 4006,     // 币安UM条件单撤单请求
    BinanceCancelAllUMConditionalOrders = 4007, // 币安UM条件单撤销全部订单请求
    BinanceCancelMarginOrder = 4008,            // 币安杠杆账户撤单请求
    BinanceModifyUMOrder = 4009,                // 币安UM合约修改订单请求
    BinanceQueryUMOrder = 4010,                 // 币安UM合约查询订单请求
    BinanceQueryUMOpenOrder = 4011,             // 币安UM合约查询当前挂单请求
    BinanceUMSetLeverage = 4012,                // 币安UM设置杠杆
}

// 交易请求的公共头部
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct TradeRequestHeader {
    pub msg_type: u32,        // 请求类型
    pub params_length: u32,   // 额外参数的长度
    pub create_time: i64,     // 构造请求的时间戳
    pub client_order_id: i64, // 客户端订单ID
}

#[derive(Debug, Clone)]
pub struct TradeRequestMsg {
    pub req_type: TradeRequestType,
    pub create_time: i64,
    pub client_order_id: i64,
    pub params: Bytes,
}

impl TryFrom<u32> for TradeRequestType {
    type Error = ();
    fn try_from(v: u32) -> Result<Self, Self::Error> {
        match v {
            4001 => Ok(TradeRequestType::BinanceNewUMOrder),
            4002 => Ok(TradeRequestType::BinanceNewUMConditionalOrder),
            4003 => Ok(TradeRequestType::BinanceNewMarginOrder),
            4004 => Ok(TradeRequestType::BinanceCancelUMOrder),
            4005 => Ok(TradeRequestType::BinanceCancelAllUMOrders),
            4006 => Ok(TradeRequestType::BinanceCancelUMConditionalOrder),
            4007 => Ok(TradeRequestType::BinanceCancelAllUMConditionalOrders),
            4008 => Ok(TradeRequestType::BinanceCancelMarginOrder),
            4009 => Ok(TradeRequestType::BinanceModifyUMOrder),
            4010 => Ok(TradeRequestType::BinanceQueryUMOrder),
            4011 => Ok(TradeRequestType::BinanceQueryUMOpenOrder),
            4012 => Ok(TradeRequestType::BinanceUMSetLeverage),
            _ => Err(()),
        }
    }
}

impl TradeRequestMsg {
    /// Parse a binary TradeRequest buffer into a structured message.
    /// Layout (little-endian):
    ///   u32 msg_type, u32 params_length, i64 create_time, i64 client_order_id, [params_length] bytes
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < 4 + 4 + 8 + 8 {
            debug!("TradeRequestMsg::parse buffer too short: {}", buf.len());
            return None;
        }
        let msg_type = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        let params_len = u32::from_le_bytes(buf[4..8].try_into().ok()?) as usize;
        let create_time = i64::from_le_bytes(buf[8..16].try_into().ok()?);
        let client_order_id = i64::from_le_bytes(buf[16..24].try_into().ok()?);
        if buf.len() < 24 + params_len {
            debug!(
                "TradeRequestMsg::parse invalid params_len: total={}, params_len={}",
                buf.len(),
                params_len
            );
            return None;
        }
        let req_type = TradeRequestType::try_from(msg_type).ok()?;
        let params = Bytes::copy_from_slice(&buf[24..24 + params_len]);
        debug!(
            "TradeRequest parsed: type={}, params_len={}, client_order_id={}",
            msg_type, params_len, client_order_id
        );
        Some(Self {
            req_type,
            create_time,
            client_order_id,
            params,
        })
    }
}

// 币安UM设置杠杆请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceUMSetLeverageRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 包含 symbol=...&leverage=... （其余由引擎补齐并签名）
}

impl BinanceUMSetLeverageRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceUMSetLeverage as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安UM合约下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewUMOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceNewUMOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceNewUMOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安UM条件单下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewUMConditionalOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceNewUMConditionalOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceNewUMConditionalOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安现货杠杆下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewMarginOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceNewMarginOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceNewMarginOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安UM合约撤单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceCancelUMOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceCancelUMOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceCancelUMOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安UM合约撤销全部订单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceCancelAllUMOrdersRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceCancelAllUMOrdersRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceCancelAllUMOrders as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安UM条件单撤单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceCancelUMConditionalOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceCancelUMConditionalOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceCancelUMConditionalOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安UM条件单撤销全部订单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceCancelAllUMConditionalOrdersRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceCancelAllUMConditionalOrdersRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceCancelAllUMConditionalOrders as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安杠杆账户撤单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceCancelMarginOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceCancelMarginOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceCancelMarginOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安UM合约修改订单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceModifyUMOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceModifyUMOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceModifyUMOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安UM合约查询订单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceQueryUMOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceQueryUMOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceQueryUMOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}

// 币安UM合约查询当前挂单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceQueryUMOpenOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceQueryUMOpenOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceQueryUMOpenOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self { header, params }
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
}
