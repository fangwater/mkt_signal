use crate::common::exchange::Exchange;
use crate::trade_engine::trade_request::TradeRequestType;
use bytes::{BufMut, BytesMut};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::service::ipc;
use log::{debug, warn};
use serde_json::Value;
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

// 固定长度 trade response header（40 bytes），不再携带 raw JSON body。
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
    error_code: i32,
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
        buf.put_i32_le(self.error_code);
        buf.freeze()
    }
}

fn extract_code(v: &Value) -> Option<i32> {
    let code = v.get("code")?;
    if let Some(n) = code.as_i64() {
        return i32::try_from(n).ok();
    }
    if let Some(s) = code.as_str() {
        return s.parse::<i32>().ok();
    }
    None
}

fn extract_s_code(v: &Value) -> Option<i32> {
    let data = v.get("data")?;
    let maybe_item = if let Some(arr) = data.as_array() {
        arr.first()
    } else if data.is_object() {
        Some(data)
    } else {
        None
    }?;
    let s_code = maybe_item.get("sCode")?;
    if let Some(n) = s_code.as_i64() {
        return i32::try_from(n).ok();
    }
    if let Some(s) = s_code.as_str() {
        return s.parse::<i32>().ok();
    }
    None
}

fn extract_msg(v: &Value) -> Option<String> {
    if let Some(s) = v
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|arr| arr.first())
        .and_then(|first| first.get("sMsg"))
        .and_then(|m| m.as_str())
    {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = v
        .get("data")
        .and_then(|d| d.get("errs"))
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
    {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = v
        .get("data")
        .and_then(|d| d.get("errs"))
        .and_then(|e| e.get("label"))
        .and_then(|m| m.as_str())
    {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = v.get("msg").and_then(|m| m.as_str()) {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = v.get("message").and_then(|m| m.as_str()) {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    None
}

fn parse_error_code_and_msg(body: &str) -> (i32, Option<String>) {
    // Default: unknown/no code.
    let mut candidate = body.to_string();

    // Some WS wrappers include nested raw/payload text.
    if let Ok(v) = serde_json::from_str::<Value>(body) {
        if let Some(raw) = v.get("raw").and_then(|r| r.as_str()) {
            candidate = raw.to_string();
        } else if let Some(payload) = v.get("payload").and_then(|p| p.as_str()) {
            candidate = payload.to_string();
        }
    }

    if let Ok(v) = serde_json::from_str::<Value>(&candidate) {
        let mut code = extract_code(&v).unwrap_or(0);
        if let Some(s_code) = extract_s_code(&v) {
            if s_code != 0 {
                code = s_code;
            }
        }
        let msg = extract_msg(&v);
        return (code, msg);
    }

    (0, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefers_okx_s_code_when_present() {
        let body = r#"{"code":"1","data":[{"sCode":"51006","sMsg":"Order price is not within the price limit"}],"msg":"All operations failed"}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        assert_eq!(code, 51006);
        assert_eq!(
            msg.as_deref(),
            Some("Order price is not within the price limit")
        );
    }

    #[test]
    fn falls_back_to_top_level_code() {
        let body = r#"{"code":-5022,"msg":"Post Only order would be filled"}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        assert_eq!(code, -5022);
        assert_eq!(msg.as_deref(), Some("Post Only order would be filled"));
    }
}

pub fn spawn_response_handle(
    publisher: Publisher<ipc::Service, [u8; 64], ()>,
    mut resp_rx: mpsc::UnboundedReceiver<TradeExecOutcome>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while let Some(out) = resp_rx.recv().await {
            let (error_code, msg) = parse_error_code_and_msg(&out.body);
            let is_2xx = (200..300).contains(&(out.status as u32));
            if !is_2xx || error_code != 0 {
                if let Some(m) = msg.as_deref() {
                    warn!(
                        "trade resp error: ex={:?} type={:?} cli_ord_id={} status={} code={} msg={}",
                        out.exchange, out.req_type, out.client_order_id, out.status, error_code, m
                    );
                } else {
                    warn!(
                        "trade resp error: ex={:?} type={:?} cli_ord_id={} status={} code={}",
                        out.exchange, out.req_type, out.client_order_id, out.status, error_code
                    );
                }
            }

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
                error_code,
            };
            let hdr_bytes = hdr.to_bytes();
            let mut buf = [0u8; 64];
            let h = hdr_bytes.len().min(buf.len());
            buf[..h].copy_from_slice(&hdr_bytes[..h]);
            debug!(
                "publish trade resp header: type={}, status={}, code={}",
                hdr.req_type, hdr.status, hdr.error_code
            );
            if let Ok(sample) = publisher.loan_uninit() {
                let sample = sample.write_payload(buf);
                let _ = sample.send();
            }
        }
    })
}
