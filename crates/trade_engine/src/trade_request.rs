use bytes::{BufMut, Bytes, BytesMut};
use log::debug;
use order_common::{OrderType, Side};
use serde_json::{json, Value};
use signal_common::tick_math::QuantizedValue;
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

fn write_qv(buf: &mut BytesMut, qv: QuantizedValue) {
    let (tick_i64, tick_exp) = qv.get_tick_parts();
    buf.put_i64_le(tick_i64);
    buf.put_i32_le(tick_exp);
    buf.put_i64_le(qv.get_count());
}

fn read_qv(raw: &[u8], offset: &mut usize) -> Option<QuantizedValue> {
    if raw.len() < *offset + 20 {
        return None;
    }
    let tick_i64 = i64::from_le_bytes(raw[*offset..*offset + 8].try_into().ok()?);
    *offset += 8;
    let tick_exp = i32::from_le_bytes(raw[*offset..*offset + 4].try_into().ok()?);
    *offset += 4;
    let count = i64::from_le_bytes(raw[*offset..*offset + 8].try_into().ok()?);
    *offset += 8;
    Some(QuantizedValue::from_parts(tick_i64, tick_exp, count))
}

fn write_string(buf: &mut BytesMut, value: &str) -> Option<()> {
    let bytes = value.as_bytes();
    if bytes.len() > u16::MAX as usize {
        return None;
    }
    buf.put_u16_le(bytes.len() as u16);
    buf.put_slice(bytes);
    Some(())
}

fn read_string(raw: &[u8], offset: &mut usize) -> Option<String> {
    if raw.len() < *offset + 2 {
        return None;
    }
    let len = u16::from_le_bytes(raw[*offset..*offset + 2].try_into().ok()?) as usize;
    *offset += 2;
    if raw.len() < *offset + len {
        return None;
    }
    let value = std::str::from_utf8(&raw[*offset..*offset + len])
        .ok()?
        .to_string();
    *offset += len;
    Some(value)
}

fn write_optional_string(buf: &mut BytesMut, value: Option<&str>) -> Option<()> {
    match value {
        Some(value) => {
            buf.put_u8(1);
            write_string(buf, value)
        }
        None => {
            buf.put_u8(0);
            Some(())
        }
    }
}

fn read_optional_string(raw: &[u8], offset: &mut usize) -> Option<Option<String>> {
    if raw.len() < *offset + 1 {
        return None;
    }
    let present = raw[*offset] != 0;
    *offset += 1;
    if present {
        read_string(raw, offset).map(Some)
    } else {
        Some(None)
    }
}

fn signed_qv_string(qv: QuantizedValue, negative: bool) -> String {
    let abs = qv.decimal_string();
    if negative && abs != "0" {
        format!("-{abs}")
    } else {
        abs
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BinanceNewOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BinanceNewOrderParams> {
        BinanceNewOrderParams::from_bytes(&self.params)
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BinanceNewOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BinanceNewOrderParams> {
        BinanceNewOrderParams::from_bytes(&self.params)
    }
}

// 币安现货(标准账户) WebSocket 下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceWsNewMarginOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // 额外的请求参数（JSON或其他格式）
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceNewOrderParams {
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity_qv: QuantizedValue,
    pub price_qv: QuantizedValue,
    pub reduce_only: bool,
    pub margin_buy: bool,
    pub ws_response_full: bool,
    pub ws_um_response_result: bool,
    pub ws_margin_limit_maker: bool,
}

impl BinanceNewOrderParams {
    const FIXED_LEN: usize = 1 + 1 + 20 + 20 + 1 + 1 + 1 + 1 + 1 + 2;

    pub fn to_bytes(&self) -> Option<Bytes> {
        let mut buf = BytesMut::with_capacity(Self::FIXED_LEN + self.symbol.len());
        buf.put_u8(self.side.to_u8());
        buf.put_u8(self.order_type.to_u8());
        write_qv(&mut buf, self.quantity_qv);
        write_qv(&mut buf, self.price_qv);
        buf.put_u8(self.reduce_only as u8);
        buf.put_u8(self.margin_buy as u8);
        buf.put_u8(self.ws_response_full as u8);
        buf.put_u8(self.ws_um_response_result as u8);
        buf.put_u8(self.ws_margin_limit_maker as u8);
        write_string(&mut buf, &self.symbol)?;
        Some(buf.freeze())
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        let mut offset = 0usize;
        if raw.len() < 2 {
            return None;
        }
        let side = Side::from_u8(raw[offset])?;
        offset += 1;
        let order_type = OrderType::from_u8(raw[offset])?;
        offset += 1;
        let quantity_qv = read_qv(raw, &mut offset)?;
        let price_qv = read_qv(raw, &mut offset)?;
        if raw.len() < offset + 5 {
            return None;
        }
        let reduce_only = raw[offset] != 0;
        offset += 1;
        let margin_buy = raw[offset] != 0;
        offset += 1;
        let ws_response_full = raw[offset] != 0;
        offset += 1;
        let ws_um_response_result = raw[offset] != 0;
        offset += 1;
        let ws_margin_limit_maker = raw[offset] != 0;
        offset += 1;
        let symbol = read_string(raw, &mut offset)?;
        Some(Self {
            symbol,
            side,
            order_type,
            quantity_qv,
            price_qv,
            reduce_only,
            margin_buy,
            ws_response_full,
            ws_um_response_result,
            ws_margin_limit_maker,
        })
    }

    pub fn to_query_string(&self, req_type: TradeRequestType, client_order_id: i64) -> String {
        let is_margin = matches!(
            req_type,
            TradeRequestType::BinanceNewMarginOrder | TradeRequestType::BinanceWsNewMarginOrder
        );
        let is_ws_margin = req_type == TradeRequestType::BinanceWsNewMarginOrder;
        let is_ws_um = req_type == TradeRequestType::BinanceWsNewUMOrder;
        let order_type = if is_ws_margin && self.ws_margin_limit_maker && self.order_type.is_limit()
        {
            "LIMIT_MAKER"
        } else {
            self.order_type.as_str()
        };

        let mut params = Vec::with_capacity(8);
        params.push(format!("symbol={}", self.symbol));
        params.push(format!("side={}", self.side.as_str()));
        params.push(format!("type={order_type}"));
        params.push(format!("quantity={}", self.quantity_qv.decimal_string()));
        if !is_margin {
            params.push(format!("reduceOnly={}", self.reduce_only));
        }
        params.push(format!("newClientOrderId={client_order_id}"));
        if self.ws_response_full {
            params.push("newOrderRespType=FULL".to_string());
        } else if is_ws_um && self.ws_um_response_result {
            params.push("newOrderRespType=RESULT".to_string());
        }
        if is_margin && self.margin_buy {
            params.push("sideEffectType=MARGIN_BUY".to_string());
        }
        if self.order_type.is_limit() {
            if is_margin {
                if !is_ws_margin {
                    params.push("timeInForce=GTC".to_string());
                }
            } else {
                params.push("timeInForce=GTX".to_string());
            }
            params.push(format!("price={}", self.price_qv.decimal_string()));
        }
        params.join("&")
    }
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BinanceNewOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BinanceNewOrderParams> {
        BinanceNewOrderParams::from_bytes(&self.params)
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BinanceNewOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BinanceNewOrderParams> {
        BinanceNewOrderParams::from_bytes(&self.params)
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BinanceCancelOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BinanceCancelOrderParams> {
        BinanceCancelOrderParams::from_bytes(&self.params)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceCancelOrderParams {
    pub symbol: String,
    pub orig_client_order_id: i64,
}

impl BinanceCancelOrderParams {
    const FIXED_LEN: usize = 8 + 2;

    pub fn to_bytes(&self) -> Option<Bytes> {
        let mut buf = BytesMut::with_capacity(Self::FIXED_LEN + self.symbol.len());
        buf.put_i64_le(self.orig_client_order_id);
        write_string(&mut buf, &self.symbol)?;
        Some(buf.freeze())
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        if raw.len() < 8 {
            return None;
        }
        let mut offset = 0usize;
        let orig_client_order_id = i64::from_le_bytes(raw[offset..offset + 8].try_into().ok()?);
        offset += 8;
        let symbol = read_string(raw, &mut offset)?;
        Some(Self {
            symbol,
            orig_client_order_id,
        })
    }

    pub fn to_query_string(&self) -> String {
        format!(
            "symbol={}&origClientOrderId={}",
            self.symbol, self.orig_client_order_id
        )
    }
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BinanceCancelOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BinanceCancelOrderParams> {
        BinanceCancelOrderParams::from_bytes(&self.params)
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BinanceCancelOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BinanceCancelOrderParams> {
        BinanceCancelOrderParams::from_bytes(&self.params)
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BinanceCancelOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BinanceCancelOrderParams> {
        BinanceCancelOrderParams::from_bytes(&self.params)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateNewOrderParams {
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity_qv: QuantizedValue,
    pub price_qv: QuantizedValue,
    pub reduce_only: bool,
    pub auto_borrow_repay: bool,
}

impl GateNewOrderParams {
    const FIXED_LEN: usize = 1 + 1 + 20 + 20 + 1 + 1 + 2;

    pub fn to_bytes(&self) -> Option<Bytes> {
        let mut buf = BytesMut::with_capacity(Self::FIXED_LEN + self.symbol.len());
        buf.put_u8(self.side.to_u8());
        buf.put_u8(self.order_type.to_u8());
        write_qv(&mut buf, self.quantity_qv);
        write_qv(&mut buf, self.price_qv);
        buf.put_u8(self.reduce_only as u8);
        buf.put_u8(self.auto_borrow_repay as u8);
        write_string(&mut buf, &self.symbol)?;
        Some(buf.freeze())
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        let mut offset = 0usize;
        if raw.len() < 2 {
            return None;
        }
        let side = Side::from_u8(raw[offset])?;
        offset += 1;
        let order_type = OrderType::from_u8(raw[offset])?;
        offset += 1;
        let quantity_qv = read_qv(raw, &mut offset)?;
        let price_qv = read_qv(raw, &mut offset)?;
        if raw.len() < offset + 2 {
            return None;
        }
        let reduce_only = raw[offset] != 0;
        offset += 1;
        let auto_borrow_repay = raw[offset] != 0;
        offset += 1;
        let symbol = read_string(raw, &mut offset)?;
        Some(Self {
            symbol,
            side,
            order_type,
            quantity_qv,
            price_qv,
            reduce_only,
            auto_borrow_repay,
        })
    }

    pub fn to_gate_unified_json(&self, client_order_id: i64) -> Value {
        let mut req_param = serde_json::Map::new();
        req_param.insert("text".to_string(), json!(format!("t-{client_order_id}")));
        req_param.insert("currency_pair".to_string(), json!(self.symbol));
        req_param.insert(
            "type".to_string(),
            json!(if self.order_type.is_limit() {
                "limit"
            } else {
                "market"
            }),
        );
        req_param.insert("account".to_string(), json!("unified"));
        req_param.insert("side".to_string(), json!(self.side.as_str_lower()));
        req_param.insert(
            "amount".to_string(),
            json!(self.quantity_qv.decimal_string()),
        );
        if self.auto_borrow_repay {
            req_param.insert("auto_borrow".to_string(), json!(true));
            req_param.insert("auto_repay".to_string(), json!(true));
        }
        if self.order_type.is_limit() {
            req_param.insert("price".to_string(), json!(self.price_qv.decimal_string()));
            req_param.insert("time_in_force".to_string(), json!("poc"));
        }
        Value::Object(req_param)
    }

    pub fn to_gate_futures_json(&self, client_order_id: i64) -> Value {
        let mut req_param = serde_json::Map::new();
        req_param.insert("text".to_string(), json!(format!("t-{client_order_id}")));
        req_param.insert("contract".to_string(), json!(self.symbol));
        req_param.insert("account".to_string(), json!("unified"));
        req_param.insert(
            "size".to_string(),
            json!(signed_qv_string(self.quantity_qv, self.side.is_sell())),
        );
        if self.order_type.is_limit() {
            req_param.insert("price".to_string(), json!(self.price_qv.decimal_string()));
            req_param.insert("tif".to_string(), json!("poc"));
        } else {
            req_param.insert("price".to_string(), json!("0"));
            req_param.insert("tif".to_string(), json!("ioc"));
        }
        if self.reduce_only {
            req_param.insert("reduce_only".to_string(), json!(true));
        }
        Value::Object(req_param)
    }
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: GateNewOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<GateNewOrderParams> {
        GateNewOrderParams::from_bytes(&self.params)
    }
}

// Gate 统一账户撤单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct GateUnifiedCancelOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes, // JSON 参数（req_param）
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateCancelOrderParams {
    pub symbol: String,
    pub order_id: String,
}

impl GateCancelOrderParams {
    const FIXED_LEN: usize = 2 + 2;

    pub fn to_bytes(&self) -> Option<Bytes> {
        let mut buf =
            BytesMut::with_capacity(Self::FIXED_LEN + self.symbol.len() + self.order_id.len());
        write_string(&mut buf, &self.symbol)?;
        write_string(&mut buf, &self.order_id)?;
        Some(buf.freeze())
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        let mut offset = 0usize;
        let symbol = read_string(raw, &mut offset)?;
        let order_id = read_string(raw, &mut offset)?;
        Some(Self { symbol, order_id })
    }

    pub fn to_gate_unified_json(&self) -> Value {
        json!({
            "order_id": self.order_id,
            "currency_pair": self.symbol,
            "account": "unified",
        })
    }

    pub fn to_gate_futures_json(&self) -> Value {
        json!({
            "order_id": self.order_id,
            "contract": self.symbol,
        })
    }
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: GateCancelOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<GateCancelOrderParams> {
        GateCancelOrderParams::from_bytes(&self.params)
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: GateNewOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<GateNewOrderParams> {
        GateNewOrderParams::from_bytes(&self.params)
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: GateCancelOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<GateCancelOrderParams> {
        GateCancelOrderParams::from_bytes(&self.params)
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BitgetMarginNewOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitgetNewOrderParams {
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity_qv: QuantizedValue,
    pub price_qv: QuantizedValue,
    pub reduce_only: bool,
}

impl BitgetNewOrderParams {
    const FIXED_LEN: usize = 1 + 1 + 20 + 20 + 1 + 2;

    pub fn to_bytes(&self) -> Option<Bytes> {
        let mut buf = BytesMut::with_capacity(Self::FIXED_LEN + self.symbol.len());
        buf.put_u8(self.side.to_u8());
        buf.put_u8(self.order_type.to_u8());
        write_qv(&mut buf, self.quantity_qv);
        write_qv(&mut buf, self.price_qv);
        buf.put_u8(self.reduce_only as u8);
        write_string(&mut buf, &self.symbol)?;
        Some(buf.freeze())
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        let mut offset = 0usize;
        if raw.len() < 2 {
            return None;
        }
        let side = Side::from_u8(raw[offset])?;
        offset += 1;
        let order_type = OrderType::from_u8(raw[offset])?;
        offset += 1;
        let quantity_qv = read_qv(raw, &mut offset)?;
        let price_qv = read_qv(raw, &mut offset)?;
        if raw.len() < offset + 1 {
            return None;
        }
        let reduce_only = raw[offset] != 0;
        offset += 1;
        let symbol = read_string(raw, &mut offset)?;
        Some(Self {
            symbol,
            side,
            order_type,
            quantity_qv,
            price_qv,
            reduce_only,
        })
    }

    pub fn to_bitget_ws_arg(&self, req_type: TradeRequestType, client_order_id: i64) -> Value {
        let category = match req_type {
            TradeRequestType::BitgetNewMarginOrder => "margin",
            TradeRequestType::BitgetNewUMOrder => "usdt-futures",
            _ => "margin",
        };
        let mut req_param = serde_json::Map::new();
        req_param.insert("category".to_string(), json!(category));
        req_param.insert(
            "symbol".to_string(),
            json!(self.symbol.to_ascii_uppercase()),
        );
        req_param.insert("side".to_string(), json!(self.side.as_str_lower()));
        req_param.insert(
            "orderType".to_string(),
            json!(if self.order_type.is_limit() {
                "limit"
            } else {
                "market"
            }),
        );
        if self.order_type.is_limit() {
            req_param.insert("timeInForce".to_string(), json!("post_only"));
            req_param.insert("price".to_string(), json!(self.price_qv.decimal_string()));
        }
        req_param.insert("qty".to_string(), json!(self.quantity_qv.decimal_string()));
        req_param.insert("clientOid".to_string(), json!(client_order_id.to_string()));
        if self.reduce_only {
            req_param.insert("reduceOnly".to_string(), json!("YES"));
        }
        Value::Object(req_param)
    }
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BitgetNewOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BitgetNewOrderParams> {
        BitgetNewOrderParams::from_bytes(&self.params)
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BitgetNewOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BitgetNewOrderParams> {
        BitgetNewOrderParams::from_bytes(&self.params)
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BitgetMarginCancelOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitgetCancelOrderParams {
    pub order_id: Option<String>,
    pub client_order_id: String,
}

impl BitgetCancelOrderParams {
    const FIXED_LEN: usize = 1 + 2 + 2;

    pub fn to_bytes(&self) -> Option<Bytes> {
        let mut buf = BytesMut::with_capacity(
            Self::FIXED_LEN
                + self.order_id.as_deref().map(str::len).unwrap_or(0)
                + self.client_order_id.len(),
        );
        write_optional_string(&mut buf, self.order_id.as_deref())?;
        write_string(&mut buf, &self.client_order_id)?;
        Some(buf.freeze())
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        let mut offset = 0usize;
        let order_id = read_optional_string(raw, &mut offset)?;
        let client_order_id = read_string(raw, &mut offset)?;
        Some(Self {
            order_id,
            client_order_id,
        })
    }

    pub fn to_bitget_ws_arg(&self, req_type: TradeRequestType) -> Value {
        let category = match req_type {
            TradeRequestType::BitgetCancelMarginOrder => "margin",
            TradeRequestType::BitgetCancelUMOrder => "usdt-futures",
            _ => "margin",
        };
        let mut req_param = serde_json::Map::new();
        req_param.insert("category".to_string(), json!(category));
        if let Some(order_id) = &self.order_id {
            req_param.insert("orderId".to_string(), json!(order_id));
        }
        req_param.insert("clientOid".to_string(), json!(self.client_order_id));
        Value::Object(req_param)
    }
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BitgetCancelOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BitgetCancelOrderParams> {
        BitgetCancelOrderParams::from_bytes(&self.params)
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

    pub fn create_typed(
        create_time: i64,
        client_order_id: i64,
        params: BitgetCancelOrderParams,
    ) -> Option<Self> {
        Some(Self::create(
            create_time,
            client_order_id,
            params.to_bytes()?,
        ))
    }

    pub fn params_struct(&self) -> Option<BitgetCancelOrderParams> {
        BitgetCancelOrderParams::from_bytes(&self.params)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BinanceCancelOrderParams, BinanceNewOrderParams, GateCancelOrderParams,
        GateFuturesCancelOrderRequest, GateFuturesNewOrderRequest, GateNewOrderParams,
        TradeRequestType,
    };
    use order_common::{OrderType, Side};
    use signal_common::tick_math::QuantizedValue;

    #[test]
    fn binance_typed_new_order_params_render_query_string() {
        let params = BinanceNewOrderParams {
            symbol: "BTCUSDT".to_string(),
            side: Side::Sell,
            order_type: OrderType::Limit,
            quantity_qv: QuantizedValue::from_parts(1, -3, 300),
            price_qv: QuantizedValue::from_parts(1, -2, 12345),
            reduce_only: true,
            margin_buy: false,
            ws_response_full: false,
            ws_um_response_result: true,
            ws_margin_limit_maker: false,
        };

        let query = params.to_query_string(TradeRequestType::BinanceWsNewUMOrder, 42);

        assert!(query.contains("symbol=BTCUSDT"));
        assert!(query.contains("side=SELL"));
        assert!(query.contains("type=LIMIT"));
        assert!(query.contains("quantity=0.300"));
        assert!(query.contains("price=123.45"));
        assert!(query.contains("reduceOnly=true"));
        assert!(query.contains("timeInForce=GTX"));
        assert!(query.contains("newOrderRespType=RESULT"));
        assert!(query.contains("newClientOrderId=42"));
    }

    #[test]
    fn binance_ws_um_market_order_can_request_result_response() {
        let params = BinanceNewOrderParams {
            symbol: "ETHUSDT".to_string(),
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity_qv: QuantizedValue::from_parts(1, -2, 125),
            price_qv: QuantizedValue::from_parts(1, 0, 0),
            reduce_only: false,
            margin_buy: false,
            ws_response_full: false,
            ws_um_response_result: true,
            ws_margin_limit_maker: false,
        };

        let query = params.to_query_string(TradeRequestType::BinanceWsNewUMOrder, 43);

        assert!(query.contains("symbol=ETHUSDT"));
        assert!(query.contains("type=MARKET"));
        assert!(query.contains("quantity=1.25"));
        assert!(query.contains("newOrderRespType=RESULT"));
        assert!(!query.contains("timeInForce="));
        assert!(!query.contains("price="));
    }

    #[test]
    fn binance_typed_cancel_params_render_query_string() {
        let params = BinanceCancelOrderParams {
            symbol: "BTCUSDT".to_string(),
            orig_client_order_id: 42,
        };

        let query = params.to_query_string();

        assert_eq!(query, "symbol=BTCUSDT&origClientOrderId=42");
    }

    #[test]
    fn gate_typed_new_order_request_roundtrips_and_renders_futures_json() {
        let params = GateNewOrderParams {
            symbol: "SOL_USDT".to_string(),
            side: Side::Sell,
            order_type: OrderType::Limit,
            quantity_qv: QuantizedValue::from_parts(1, -2, 300),
            price_qv: QuantizedValue::from_parts(1, -3, 88560),
            reduce_only: true,
            auto_borrow_repay: false,
        };
        let request = GateFuturesNewOrderRequest::create_typed(1, 45, params)
            .expect("gate typed request should build");
        let decoded = request
            .params_struct()
            .expect("gate typed params should decode");
        let json = decoded.to_gate_futures_json(45);

        assert_eq!(json["contract"], "SOL_USDT");
        assert_eq!(json["size"], "-3.00");
        assert_eq!(json["price"], "88.560");
        assert_eq!(json["tif"], "poc");
        assert_eq!(json["reduce_only"], true);
    }

    #[test]
    fn gate_typed_cancel_request_roundtrips_and_renders_futures_json() {
        let params = GateCancelOrderParams {
            symbol: "SOL_USDT".to_string(),
            order_id: "t-45".to_string(),
        };
        let request = GateFuturesCancelOrderRequest::create_typed(1, 45, params)
            .expect("gate cancel typed request should build");
        let decoded = request
            .params_struct()
            .expect("gate cancel typed params should decode");
        let json = decoded.to_gate_futures_json();

        assert_eq!(json["contract"], "SOL_USDT");
        assert_eq!(json["order_id"], "t-45");
    }
}
