use crate::common::exchange::Exchange;
use crate::trade_engine::trade_request::TradeRequestType;
use bytes::{BufMut, BytesMut};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::service::ipc;
use log::debug;
use tokio::sync::mpsc;

// REST 请求执行后的输出（内部使用）
#[derive(Debug, Clone)]
pub struct TradeExecOutcome {
    pub req_type: TradeRequestType,
    pub client_order_id: i64,
    pub status: u16,
    pub body: String,
    pub exchange: Exchange,
    pub ip_used_weight_1m: Option<u32>,
    pub order_count_1m: Option<u32>,
}

impl TradeExecOutcome {
    pub fn parse(raw: &[u8]) -> Option<Self> {
        if raw.len() < 40 {
            return None;
        }

        let req_type_val = u32::from_le_bytes(raw[0..4].try_into().ok()?);
        let client_order_id = i64::from_le_bytes(raw[12..20].try_into().ok()?);
        let exchange_val = u32::from_le_bytes(raw[20..24].try_into().ok()?);
        let status = u16::from_le_bytes(raw[24..26].try_into().ok()?);
        let ip_used = u32::from_le_bytes(raw[28..32].try_into().ok()?);
        let order_count = u32::from_le_bytes(raw[32..36].try_into().ok()?);
        let body_len = u32::from_le_bytes(raw[36..40].try_into().ok()?) as usize;

        if raw.len() < 40 + body_len {
            return None;
        }

        let body_slice = &raw[40..40 + body_len];
        let body = match String::from_utf8(body_slice.to_vec()) {
            Ok(s) => s,
            Err(_) => String::from_utf8_lossy(body_slice).to_string(),
        };

        let req_type = TradeRequestType::try_from(req_type_val).ok()?;
        let exchange = Exchange::from_u8((exchange_val & 0xFF) as u8)?;
        let ip_used_weight_1m = if ip_used == u32::MAX {
            None
        } else {
            Some(ip_used)
        };
        let order_count_1m = if order_count == u32::MAX {
            None
        } else {
            Some(order_count)
        };

        Some(Self {
            req_type,
            client_order_id,
            status,
            body,
            exchange,
            ip_used_weight_1m,
            order_count_1m,
        })
    }
}

// 将原始 HTTP body（JSON 文本）直接发布到响应通道（16384 字节）
#[repr(C, align(8))]
#[derive(Debug, Clone)]
struct GenericResponseHeader {
    req_type: u32,
    local_recv_time: i64,
    client_order_id: i64,
    exchange: u32,
    status: u16,
    reserved: u16,
    ip_used_weight_1m: u32,
    order_count_1m: u32,
    body_length: u32,
}

impl GenericResponseHeader {
    fn to_bytes(&self) -> bytes::Bytes {
        let mut buf = BytesMut::with_capacity(40);
        buf.put_u32_le(self.req_type);
        buf.put_i64_le(self.local_recv_time);
        buf.put_i64_le(self.client_order_id);
        buf.put_u32_le(self.exchange);
        buf.put_u16_le(self.status);
        buf.put_u16_le(self.reserved);
        buf.put_u32_le(self.ip_used_weight_1m);
        buf.put_u32_le(self.order_count_1m);
        buf.put_u32_le(self.body_length);
        buf.freeze()
    }
}

pub fn spawn_response_handle(
    publisher: Publisher<ipc::Service, [u8; 16384], ()>,
    mut resp_rx: mpsc::UnboundedReceiver<TradeExecOutcome>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while let Some(out) = resp_rx.recv().await {
            // 检查是否是 cancel 请求的错误响应
            // 如果是 cancel 操作且状态码不是 200，则跳过 dispatch
            let is_cancel_request = matches!(
                out.req_type,
                TradeRequestType::BinanceCancelUMOrder
                    | TradeRequestType::BinanceCancelAllUMOrders
                    | TradeRequestType::BinanceCancelUMConditionalOrder
                    | TradeRequestType::BinanceCancelAllUMConditionalOrders
                    | TradeRequestType::BinanceCancelMarginOrder
            );

            if is_cancel_request && out.status != 200 {
                debug!(
                    "skip dispatch for cancel error: type={:?}, status={}, client_order_id={}",
                    out.req_type, out.status, out.client_order_id
                );
                continue;
            }

            let body = out.body.as_bytes();
            let now = chrono::Utc::now().timestamp_millis();
            let hdr = GenericResponseHeader {
                req_type: out.req_type as u32,
                local_recv_time: now,
                client_order_id: out.client_order_id,
                exchange: out.exchange as u32,
                status: out.status,
                reserved: 0,
                ip_used_weight_1m: out.ip_used_weight_1m.unwrap_or(u32::MAX),
                order_count_1m: out.order_count_1m.unwrap_or(u32::MAX),
                body_length: body.len() as u32,
            };
            let hdr_bytes = hdr.to_bytes();
            let mut buf = [0u8; 16384];
            let mut written = 0usize;
            let h = hdr_bytes.len().min(buf.len());
            buf[..h].copy_from_slice(&hdr_bytes[..h]);
            written += h;
            let remain = buf.len().saturating_sub(written);
            let bcopy = body.len().min(remain);
            if bcopy > 0 {
                buf[written..written + bcopy].copy_from_slice(&body[..bcopy]);
            }
            debug!(
                "publish header+json: type={}, status={}, body_len={}, truncated={}",
                hdr.req_type,
                hdr.status,
                body.len(),
                body.len() > bcopy
            );
            // 打印完整 JSON（debug 日志）
            if let Ok(full) = std::str::from_utf8(body) {
                debug!("resp json: {}", full);
            } else {
                debug!("resp body ({} bytes, non-utf8)", body.len());
            }
            if let Ok(sample) = publisher.loan_uninit() {
                let sample = sample.write_payload(buf);
                let _ = sample.send();
            }
        }
    })
}
