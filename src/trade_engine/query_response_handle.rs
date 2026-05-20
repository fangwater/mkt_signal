use crate::common::exchange::Exchange;
use crate::common::iceoryx_publisher::QUERY_RESP_PAYLOAD;
use crate::trade_engine::query_request::QueryRequestType;
use bytes::{BufMut, Bytes, BytesMut};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::service::ipc;
use log::{debug, warn};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct QueryExecOutcome {
    pub req_type: QueryRequestType,
    pub client_query_id: i64,
    pub status: u16,
    pub body: Bytes,
    pub exchange: Exchange,
    pub ip_used_weight_1m: Option<u32>,
    pub query_count_1m: Option<u32>,
}

const QUERY_RESP_HEADER_LEN: usize = 4 + 8; // req_type + client_query_id

pub fn publish_query_response(
    publisher: &Publisher<ipc::Service, [u8; QUERY_RESP_PAYLOAD], ()>,
    out: QueryExecOutcome,
) {
    let body = out.body.as_ref();
    let mut buf = [0u8; QUERY_RESP_PAYLOAD];
    let mut written = 0usize;

    // header: req_type + client_query_id (little-endian)
    {
        let mut hdr = BytesMut::with_capacity(QUERY_RESP_HEADER_LEN);
        hdr.put_u32_le(out.req_type as u32);
        hdr.put_i64_le(out.client_query_id);
        let hdr_bytes = hdr.freeze();
        buf[..QUERY_RESP_HEADER_LEN].copy_from_slice(&hdr_bytes[..]);
        written += QUERY_RESP_HEADER_LEN;
    }

    let remain = buf.len().saturating_sub(written);
    let mut dropped_body = false;
    let bcopy = body.len().min(remain);
    if body.len() > remain {
        dropped_body = true;
        warn!(
            "query resp body too large, dropped: type={} client_query_id={} body_len={} max_body_len={}",
            out.req_type as u32,
            out.client_query_id,
            body.len(),
            remain
        );
    } else if bcopy > 0 {
        buf[written..written + bcopy].copy_from_slice(&body[..bcopy]);
    }
    debug!(
        "publish query resp: type={} client_query_id={} body_len={} dropped_body={}",
        out.req_type as u32,
        out.client_query_id,
        body.len(),
        dropped_body
    );
    if let Ok(sample) = publisher.loan_uninit() {
        let sample = sample.write_payload(buf);
        let _ = sample.send();
    }
}

pub fn spawn_query_response_handle(
    publisher: Publisher<ipc::Service, [u8; QUERY_RESP_PAYLOAD], ()>,
    mut resp_rx: mpsc::UnboundedReceiver<QueryExecOutcome>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while let Some(out) = resp_rx.recv().await {
            publish_query_response(&publisher, out);
        }
    })
}
