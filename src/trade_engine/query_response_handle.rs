use crate::common::exchange::Exchange;
use crate::trade_engine::query_request::QueryRequestType;
use bytes::{BufMut, BytesMut};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::service::ipc;
use log::debug;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct QueryExecOutcome {
    pub req_type: QueryRequestType,
    pub client_query_id: i64,
    pub status: u16,
    pub body: String,
    pub exchange: Exchange,
    pub ip_used_weight_1m: Option<u32>,
    pub query_count_1m: Option<u32>,
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
struct GenericResponseHeader {
    req_type: u32,
    local_recv_time: i64,
    client_query_id: i64,
    exchange: u32,
    status: u16,
    reserved: u16,
    ip_used_weight_1m: u32,
    query_count_1m: u32,
    body_length: u32,
}

impl GenericResponseHeader {
    fn to_bytes(&self) -> bytes::Bytes {
        let mut buf = BytesMut::with_capacity(40);
        buf.put_u32_le(self.req_type);
        buf.put_i64_le(self.local_recv_time);
        buf.put_i64_le(self.client_query_id);
        buf.put_u32_le(self.exchange);
        buf.put_u16_le(self.status);
        buf.put_u16_le(self.reserved);
        buf.put_u32_le(self.ip_used_weight_1m);
        buf.put_u32_le(self.query_count_1m);
        buf.put_u32_le(self.body_length);
        buf.freeze()
    }
}

pub fn spawn_query_response_handle(
    publisher: Publisher<ipc::Service, [u8; 16384], ()>,
    mut resp_rx: mpsc::UnboundedReceiver<QueryExecOutcome>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while let Some(out) = resp_rx.recv().await {
            let body = out.body.as_bytes();
            let now = chrono::Utc::now().timestamp_millis();
            let hdr = GenericResponseHeader {
                req_type: out.req_type as u32,
                local_recv_time: now,
                client_query_id: out.client_query_id,
                exchange: out.exchange as u32,
                status: out.status,
                reserved: 0,
                ip_used_weight_1m: out.ip_used_weight_1m.unwrap_or(u32::MAX),
                query_count_1m: out.query_count_1m.unwrap_or(u32::MAX),
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
                "publish query resp: type={}, status={}, body_len={}, truncated={}",
                hdr.req_type,
                hdr.status,
                body.len(),
                body.len() > bcopy
            );
            if let Ok(sample) = publisher.loan_uninit() {
                let sample = sample.write_payload(buf);
                let _ = sample.send();
            }
        }
    })
}

