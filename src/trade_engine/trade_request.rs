use bytes::{BufMut, Bytes, BytesMut};
use log::debug;
use std::time::Instant;

pub use order_common::TradeRequestType;

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct TradeRequestHeader {
    pub msg_type: u32,
    pub params_length: u32,
    pub create_time: i64,
    pub client_order_id: i64,
}

#[derive(Debug, Clone)]
pub struct TradeRequestMsg {
    pub req_type: TradeRequestType,
    pub create_time: i64,
    pub client_order_id: i64,
    pub params: Bytes,
    pub ipc_recv: Option<Instant>,
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
            ipc_recv: None,
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

// 币安UM WebSocket 下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceWsNewUMOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceWsNewUMOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceWsNewUMOrder as u32,
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

// 币安现货(标准账户) WebSocket 下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceWsNewMarginOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceWsNewMarginOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceWsNewMarginOrder as u32,
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

// 币安万向划转请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceUniversalTransferRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 包含 type=...&asset=...&amount=... （其余由引擎补齐并签名）
}

impl BinanceUniversalTransferRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceUniversalTransfer as u32,
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

// 币安UM WebSocket 撤单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceWsCancelUMOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

// 币安现货(标准账户) WebSocket 撤单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceWsCancelMarginOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

impl BinanceWsCancelMarginOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceWsCancelMarginOrder as u32,
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

impl BinanceWsCancelUMOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceWsCancelUMOrder as u32,
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

// Gate 统一账户下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct GateUnifiedNewOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // JSON 参数（req_param）
}

impl GateUnifiedNewOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::GateUnifiedNewOrder as u32,
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

// Gate 统一账户撤单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct GateUnifiedCancelOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // JSON 参数（req_param）
}

impl GateUnifiedCancelOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::GateUnifiedCancelOrder as u32,
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

// Gate U 本位合约下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct GateFuturesNewOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // JSON 参数（req_param）
}

impl GateFuturesNewOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::GateFuturesNewOrder as u32,
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

// Gate U 本位合约撤单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct GateFuturesCancelOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // JSON 参数（req_param）
}

impl GateFuturesCancelOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::GateFuturesCancelOrder as u32,
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

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BitgetMarginNewOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,
}

impl BitgetMarginNewOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BitgetNewMarginOrder as u32,
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

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BitgetUmNewOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,
}

impl BitgetUmNewOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BitgetNewUMOrder as u32,
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

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BitgetMarginCancelOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,
}

impl BitgetMarginCancelOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BitgetCancelMarginOrder as u32,
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

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BitgetUmCancelOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,
}

impl BitgetUmCancelOrderRequest {
    pub fn create(create_time: i64, client_order_id: i64, params: Bytes) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BitgetCancelUMOrder as u32,
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
