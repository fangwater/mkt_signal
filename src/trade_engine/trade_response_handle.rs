use crate::common::exchange::Exchange;
use crate::common::trade_error_code::gate;
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
    pub order_id: i64,
    pub order_status_u8: u8,
    pub order_update_time: i64,
    pub executed_qty: f64,
    pub response_price: f64,
}

// 固定长度 trade response header（22 bytes），不再携带 raw JSON body。
#[repr(C, align(8))]
#[derive(Debug, Clone)]
struct GenericResponseHeader {
    req_type: u32,
    client_order_id: i64,
    exchange: u32,
    status: u16,
    error_code: i32,
}

impl GenericResponseHeader {
    fn to_bytes(&self) -> bytes::Bytes {
        let mut buf = BytesMut::with_capacity(22);
        buf.put_u32_le(self.req_type);
        buf.put_i64_le(self.client_order_id);
        buf.put_u32_le(self.exchange);
        buf.put_u16_le(self.status);
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
        .and_then(|e| e.get("label"))
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
    if let Some(s) = v.get("label").and_then(|m| m.as_str()) {
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

fn normalize_trade_error(
    exchange: Exchange,
    code: i32,
    msg: Option<String>,
) -> (i32, Option<String>) {
    if exchange == Exchange::Gate && code == 0 {
        if let Some(m) = msg.as_deref() {
            if m.eq_ignore_ascii_case("ORDER_NOT_FOUND") {
                return (gate::ORDER_NOT_FOUND, msg);
            }
            if m.eq_ignore_ascii_case("ORDER_POC") {
                return (gate::ORDER_POC, msg);
            }
            // Insufficient-margin / balance class. Mapped to synthetic codes so
            // downstream `is_insufficient_margin()` (used by ArbHedge 51008
            // emergency path) treats Gate the same as the other venues.
            if m.eq_ignore_ascii_case("BALANCE_NOT_ENOUGH") {
                return (gate::BALANCE_NOT_ENOUGH, msg);
            }
            if m.eq_ignore_ascii_case("MARGIN_NOT_ENOUGH") {
                return (gate::MARGIN_NOT_ENOUGH, msg);
            }
            if m.eq_ignore_ascii_case("POSITION_MARGIN_TOO_LOW") {
                return (gate::POSITION_MARGIN_TOO_LOW, msg);
            }
            if m.eq_ignore_ascii_case("LIQUIDITY_NOT_ENOUGH") {
                return (gate::LIQUIDITY_NOT_ENOUGH, msg);
            }
        }
    }

    (code, msg)
}

fn is_cancel_request(req_type: TradeRequestType) -> bool {
    matches!(
        req_type,
        TradeRequestType::BinanceCancelUMOrder
            | TradeRequestType::BinanceCancelMarginOrder
            | TradeRequestType::BinanceCancelUMConditionalOrder
            | TradeRequestType::BinanceWsCancelUMOrder
            | TradeRequestType::BinanceWsCancelMarginOrder
            | TradeRequestType::OkexCancelMarginOrder
            | TradeRequestType::OkexCancelUMOrder
            | TradeRequestType::GateUnifiedCancelOrder
            | TradeRequestType::GateFuturesCancelOrder
            | TradeRequestType::BybitCancelMarginOrder
            | TradeRequestType::BybitCancelUMOrder
            | TradeRequestType::BitgetCancelMarginOrder
            | TradeRequestType::BitgetCancelUMOrder
    )
}

fn is_cancel_not_cancellable(exchange: Exchange, error_code: i32) -> bool {
    match exchange {
        Exchange::Binance => error_code == -2011,
        Exchange::Okex => matches!(error_code, 51400 | 51410 | 51416),
        Exchange::Gate => error_code == gate::ORDER_NOT_FOUND,
        Exchange::Bybit => matches!(
            error_code,
            110001 | 110008 | 110010 | 170139 | 170142 | 170143 | 170145 | 170190 | 170191
        ),
        _ => false,
    }
}

fn should_downgrade_trade_resp_error(out: &TradeExecOutcome, error_code: i32) -> bool {
    is_cancel_request(out.req_type) && is_cancel_not_cancellable(out.exchange, error_code)
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

    #[test]
    fn normalizes_gate_order_not_found_from_message() {
        let body = r#"{"header":{"status":400},"data":{"errs":{"label":"ORDER_NOT_FOUND","message":"ORDER_NOT_FOUND"}}}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, msg) = normalize_trade_error(Exchange::Gate, code, msg);
        assert_eq!(code, gate::ORDER_NOT_FOUND);
        assert_eq!(msg.as_deref(), Some("ORDER_NOT_FOUND"));
    }

    #[test]
    fn normalizes_gate_order_poc_from_label() {
        let body = r#"{"header":{"status":400},"data":{"errs":{"label":"ORDER_POC","message":"poc order would be filled immediately"}}}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, msg) = normalize_trade_error(Exchange::Gate, code, msg);
        assert_eq!(code, gate::ORDER_POC);
        assert_eq!(msg.as_deref(), Some("ORDER_POC"));
    }

    fn sample_outcome(req_type: TradeRequestType, exchange: Exchange) -> TradeExecOutcome {
        TradeExecOutcome {
            req_type,
            client_order_id: 1,
            status: 206,
            body: String::new(),
            exchange,
            order_id: 0,
            order_status_u8: 0,
            order_update_time: 0,
            executed_qty: 0.0,
            response_price: 0.0,
        }
    }

    #[test]
    fn downgrades_okx_terminal_cancel_errors() {
        let out = sample_outcome(TradeRequestType::OkexCancelUMOrder, Exchange::Okex);
        assert!(should_downgrade_trade_resp_error(&out, 51400));
        assert!(should_downgrade_trade_resp_error(&out, 51410));
        assert!(should_downgrade_trade_resp_error(&out, 51416));
        assert!(!should_downgrade_trade_resp_error(&out, 51412));
    }

    #[test]
    fn does_not_downgrade_non_cancel_requests() {
        let out = sample_outcome(TradeRequestType::OkexNewUMOrder, Exchange::Okex);
        assert!(!should_downgrade_trade_resp_error(&out, 51400));
    }
}

pub fn spawn_response_handle(
    publisher: Publisher<ipc::Service, [u8; 64], ()>,
    mut resp_rx: mpsc::UnboundedReceiver<TradeExecOutcome>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while let Some(out) = resp_rx.recv().await {
            let (error_code, msg) = parse_error_code_and_msg(&out.body);
            let (error_code, msg) = normalize_trade_error(out.exchange, error_code, msg);
            let is_2xx = (200..300).contains(&(out.status as u32));
            if !is_2xx || error_code != 0 {
                let downgrade = should_downgrade_trade_resp_error(&out, error_code);
                if let Some(m) = msg.as_deref() {
                    if downgrade {
                        debug!(
                            "trade resp benign cancel terminal: ex={:?} type={:?} cli_ord_id={} status={} code={} msg={}",
                            out.exchange,
                            out.req_type,
                            out.client_order_id,
                            out.status,
                            error_code,
                            m
                        );
                    } else {
                        warn!(
                            "trade resp error: ex={:?} type={:?} cli_ord_id={} status={} code={} msg={}",
                            out.exchange,
                            out.req_type,
                            out.client_order_id,
                            out.status,
                            error_code,
                            m
                        );
                    }
                } else if downgrade {
                    debug!(
                        "trade resp benign cancel terminal: ex={:?} type={:?} cli_ord_id={} status={} code={}",
                        out.exchange,
                        out.req_type,
                        out.client_order_id,
                        out.status,
                        error_code
                    );
                } else {
                    warn!(
                        "trade resp error: ex={:?} type={:?} cli_ord_id={} status={} code={}",
                        out.exchange, out.req_type, out.client_order_id, out.status, error_code
                    );
                }
            }

            let hdr = GenericResponseHeader {
                req_type: out.req_type as u32,
                client_order_id: out.client_order_id,
                exchange: out.exchange as u32,
                status: out.status,
                error_code,
            };
            let hdr_bytes = hdr.to_bytes();
            let mut buf = [0u8; 64];
            let h = hdr_bytes.len().min(buf.len());
            buf[..h].copy_from_slice(&hdr_bytes[..h]);
            if buf.len() >= h + 33 {
                buf[h..h + 8].copy_from_slice(&out.order_id.to_le_bytes());
                buf[h + 8] = out.order_status_u8;
                buf[h + 9..h + 17].copy_from_slice(&out.order_update_time.to_le_bytes());
                buf[h + 17..h + 25].copy_from_slice(&out.executed_qty.to_le_bytes());
                buf[h + 25..h + 33].copy_from_slice(&out.response_price.to_le_bytes());
            }
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
