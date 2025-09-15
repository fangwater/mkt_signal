use crate::common::exchange::Exchange;
use crate::trade_engine::trade_request::TradeRequestType;
use crate::trade_engine::trade_type_mapping::TradeTypeMapping;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::service::ipc;
use tokio::sync::mpsc;

pub struct TradeExecOutcome {
    pub req_type: TradeRequestType,
    pub client_order_id: i64,
    pub status: u16,
    pub body: String,
    pub exchange: Exchange,
    pub ip_used_weight_1m: Option<u32>,
    pub order_count_1m: Option<u32>,
}

// 统一的响应包头（外层封装）
// msg_type: 根据请求类型映射得到的响应类型或错误类型
// body_format: 0=原始HTTP body, 1=已解析的typed body, 2=错误typed
#[repr(C, align(8))]
#[derive(Debug, Clone)]
struct GenericTradeResponseHeader {
    msg_type: u32,
    local_recv_time: i64,
    client_order_id: i64,
    exchange: u32,
    status: u16,
    body_format: u16,
    body_length: u32,
}

impl GenericTradeResponseHeader {
    fn to_bytes(&self) -> bytes::Bytes {
        use bytes::{BufMut, BytesMut};
        let mut buf = BytesMut::with_capacity(4 + 8 + 8 + 4 + 2 + 2 + 4);
        buf.put_u32_le(self.msg_type);
        buf.put_i64_le(self.local_recv_time);
        buf.put_i64_le(self.client_order_id);
        buf.put_u32_le(self.exchange);
        buf.put_u16_le(self.status);
        buf.put_u16_le(self.body_format);
        buf.put_u32_le(self.body_length);
        buf.freeze()
    }
}

/// 响应处理器：
/// - 从 resp_rx 读取 OrderResponseEvent（执行层的 HTTP 原始结果已被封装）
/// - 解析/规范化（目前直接转发 JSON，后续可在此转成二进制结构）
/// - 通过 Iceoryx publisher 发布
pub fn spawn_response_handle(
    publisher: Publisher<ipc::Service, [u8; 8192], ()>,
    mut resp_rx: mpsc::UnboundedReceiver<TradeExecOutcome>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while let Some(out) = resp_rx.recv().await {
            let now = chrono::Utc::now().timestamp_millis();
            let resp_type = TradeTypeMapping::get_response_type(out.req_type) as u32;

            // 优先构造错误typed
            let (body_format, body_bytes) = if !(200..300).contains(&out.status) {
                // 错误使用一个简化的错误typed体：直接复用 trade_response::ErrorResponseMsg 的二进制
                let err = crate::trade_engine::trade_response::ErrorResponseMsg::create(
                    now,
                    out.client_order_id,
                    out.exchange,
                    out.status as i32,
                    out.body.clone(),
                );
                (2u16, err.to_bytes())
            } else {
                // 按类型尝试解析成typed体，否则原始body
                match build_typed_body_bytes(out.req_type, &out.body) {
                    Some(b) => (1u16, b),
                    None => (0u16, bytes::Bytes::from(out.body)),
                }
            };

            let header = GenericTradeResponseHeader {
                msg_type: if body_format == 2 { crate::trade_engine::trade_response::TradeResponseType::ErrorResponse as u32 } else { resp_type },
                local_recv_time: now,
                client_order_id: out.client_order_id,
                exchange: out.exchange as u32,
                status: out.status,
                body_format,
                body_length: body_bytes.len() as u32,
            };

            let hdr = header.to_bytes();
            let mut buf = [0u8; 8192];
            let total = (hdr.len() + body_bytes.len()).min(8192);
            let split = hdr.len().min(total);
            buf[..split].copy_from_slice(&hdr[..split]);
            let remain = total - split;
            if remain > 0 {
                buf[split..split + remain].copy_from_slice(&body_bytes[..remain]);
            }

            if let Ok(sample) = publisher.loan_uninit() {
                let sample = sample.write_payload(buf);
                let _ = sample.send();
            }
        }
    })
}

fn build_typed_body_bytes(req_type: TradeRequestType, body: &str) -> Option<bytes::Bytes> {
    use serde_json::Value;
    match req_type {
        TradeRequestType::BinanceNewUMOrder => {
            let v: Value = serde_json::from_str(body).ok()?;
            let order_id = v.get("orderId").and_then(|x| x.as_i64()).unwrap_or(0);
            let update_time = v.get("updateTime").and_then(|x| x.as_i64()).unwrap_or(0);
            let good_till_date = v.get("goodTillDate").and_then(|x| x.as_i64()).unwrap_or(0);
            let symbol = v.get("symbol").and_then(|x| x.as_str()).unwrap_or("").to_string();

            let side_char = match v.get("side").and_then(|x| x.as_str()).unwrap_or("") {
                "BUY" => 'B',
                "SELL" => 'S',
                _ => 'U',
            };
            let position_side_char = match v.get("positionSide").and_then(|x| x.as_str()).unwrap_or("") {
                "LONG" => 'L',
                "SHORT" => 'S',
                "BOTH" => 'B',
                _ => 'U',
            };
            let reduce_only = v.get("reduceOnly").and_then(|x| x.as_bool()).unwrap_or(false);

            let parse_f64 = |k: &str| -> f64 {
                v.get(k)
                    .and_then(|x| x.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0)
            };
            let cum_qty = parse_f64("cumQty");
            let cum_quote = parse_f64("cumQuote");
            let executed_qty = parse_f64("executedQty");
            let avg_price = parse_f64("avgPrice");
            let orig_qty = parse_f64("origQty");
            let price = parse_f64("price");

            let order_type = v.get("type").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let time_in_force = v.get("timeInForce").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let status = v.get("status").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let self_trade_prevention_mode = v.get("selfTradePreventionMode").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let price_match = v.get("priceMatch").and_then(|x| x.as_str()).unwrap_or("").to_string();

            let now = chrono::Utc::now().timestamp_millis();
            let resp = crate::trade_engine::trade_response::BinanceNewUMOrderResponse::create(
                now,
                0,
                symbol,
                order_id,
                update_time,
                good_till_date,
                side_char,
                position_side_char,
                reduce_only,
                cum_qty,
                cum_quote,
                executed_qty,
                avg_price,
                orig_qty,
                price,
                order_type,
                time_in_force,
                status,
                self_trade_prevention_mode,
                price_match,
            );
            Some(resp.body.to_bytes())
        }
        // 仅示例：将“撤销全部条件单”映射为typed体（字段简单）
        TradeRequestType::BinanceCancelAllUMConditionalOrders => {
            let v: Value = serde_json::from_str(body).ok()?;
            let code = match v.get("code") {
                Some(Value::Number(n)) => n.to_string(),
                Some(Value::String(s)) => s.clone(),
                _ => String::new(),
            };
            let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("").to_string();
            let dummy_now = chrono::Utc::now().timestamp_millis();
            let resp = crate::trade_engine::trade_response::BinanceCancelAllUMConditionalOrdersResponse::create(
                dummy_now,
                0,
                code,
                msg,
            );
            // 只取其 body 二进制，外层我们用统一包头
            Some(resp.body.to_bytes())
        }
        _ => None,
    }
}
