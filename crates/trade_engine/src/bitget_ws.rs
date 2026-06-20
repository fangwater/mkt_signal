use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use std::fmt::Write as _;

use crate::trade_request::{
    BitgetCancelOrderParamsRef, BitgetNewOrderParamsRef, TradeRequestMsg, TradeRequestType,
};
use account_common::bitget_auth::BitgetCredentials;

pub fn build_login_payload(creds: &BitgetCredentials) -> Result<String> {
    serde_json::to_string(&creds.build_login_message())
        .with_context(|| "serialize bitget login payload")
}

pub fn build_order_payload(msg: &TradeRequestMsg, transport_id: i64) -> Result<String> {
    let req_type = msg.req_type;
    let topic = match req_type {
        TradeRequestType::BitgetNewMarginOrder | TradeRequestType::BitgetNewUMOrder => {
            "place-order"
        }
        TradeRequestType::BitgetCancelMarginOrder | TradeRequestType::BitgetCancelUMOrder => {
            "cancel-order"
        }
        _ => {
            return Err(anyhow!(
                "unsupported bitget ws request type: {:?}",
                msg.req_type
            ))
        }
    };
    let category = bitget_category(req_type)?;
    let mut out = String::with_capacity(256 + msg.params.len());
    write!(
        out,
        "{{\"id\":\"{}\",\"op\":\"trade\",\"category\":",
        transport_id
    )
    .expect("write bitget payload id");
    push_json_string(&mut out, category);
    out.push_str(",\"topic\":");
    push_json_string(&mut out, topic);
    out.push_str(",\"args\":[{");

    match req_type {
        TradeRequestType::BitgetNewMarginOrder | TradeRequestType::BitgetNewUMOrder => {
            let params = BitgetNewOrderParamsRef::from_bytes(&msg.params).ok_or_else(|| {
                anyhow!(
                    "Bitget WS new order requires typed params, req_type={:?}",
                    req_type
                )
            })?;
            push_bitget_new_order_arg(&mut out, &params, msg.client_order_id);
        }
        TradeRequestType::BitgetCancelMarginOrder | TradeRequestType::BitgetCancelUMOrder => {
            let params = BitgetCancelOrderParamsRef::from_bytes(&msg.params).ok_or_else(|| {
                anyhow!(
                    "Bitget WS cancel order requires typed params, req_type={:?}",
                    req_type
                )
            })?;
            push_bitget_cancel_order_arg(&mut out, &params);
        }
        _ => unreachable!("unsupported bitget request type checked above"),
    }

    out.push_str("}]}");
    Ok(out)
}

fn bitget_category(req_type: TradeRequestType) -> Result<&'static str> {
    match req_type {
        TradeRequestType::BitgetNewMarginOrder | TradeRequestType::BitgetCancelMarginOrder => {
            Ok("margin")
        }
        TradeRequestType::BitgetNewUMOrder | TradeRequestType::BitgetCancelUMOrder => {
            Ok("usdt-futures")
        }
        _ => return Err(anyhow!("unsupported bitget req_type")),
    }
}

fn push_bitget_new_order_arg(
    out: &mut String,
    params: &BitgetNewOrderParamsRef<'_>,
    client_order_id: i64,
) {
    let mut first = true;
    if params.symbol.bytes().all(|b| !b.is_ascii_lowercase()) {
        push_json_field(out, "symbol", params.symbol, &mut first);
    } else {
        let symbol = params.symbol.to_ascii_uppercase();
        push_json_field(out, "symbol", &symbol, &mut first);
    }
    push_json_field(out, "side", params.side.as_str_lower(), &mut first);
    push_json_field(
        out,
        "orderType",
        if params.order_type.is_limit() {
            "limit"
        } else {
            "market"
        },
        &mut first,
    );
    if params.order_type.is_limit() {
        push_json_field(out, "timeInForce", "post_only", &mut first);
        push_json_field(out, "price", &params.price_qv.decimal_string(), &mut first);
    }
    push_json_field(out, "qty", &params.quantity_qv.decimal_string(), &mut first);
    push_i64_string_field(out, "clientOid", client_order_id, &mut first);
    if params.reduce_only {
        push_json_field(out, "reduceOnly", "YES", &mut first);
    }
}

fn push_bitget_cancel_order_arg(out: &mut String, params: &BitgetCancelOrderParamsRef<'_>) {
    let mut first = true;
    if let Some(order_id) = params.order_id {
        push_json_field(out, "orderId", order_id, &mut first);
    }
    push_json_field(out, "clientOid", params.client_order_id, &mut first);
}

fn push_json_field(out: &mut String, key: &str, value: &str, first: &mut bool) {
    if !*first {
        out.push(',');
    }
    *first = false;
    push_json_string(out, key);
    out.push(':');
    push_json_string(out, value);
}

fn push_i64_string_field(out: &mut String, key: &str, value: i64, first: &mut bool) {
    if !*first {
        out.push(',');
    }
    *first = false;
    push_json_string(out, key);
    out.push_str(":\"");
    write!(out, "{}", value).expect("write bitget integer field");
    out.push('"');
}

fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if c <= '\u{1f}' => {
                write!(out, "\\u{:04x}", c as u32).expect("write json escape");
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[derive(Debug, Clone)]
pub struct BitgetWsOrderResponse {
    pub event: String,
    pub id: i64,
    pub category: String,
    pub topic: String,
    pub code: String,
    pub msg: String,
    pub order_id: String,
    pub client_oid: String,
    pub create_time_ms: i64,
    /// 顶层 `ts`（ms）。Bitget 仅暴露这一个服务端时间戳，统一模型里既当 T2 也当 T3。
    pub ts_ms: i64,
}

impl BitgetWsOrderResponse {
    pub fn from_json_str(payload: &str) -> Option<Self> {
        let val: Value = serde_json::from_str(payload).ok()?;
        let obj = val.as_object()?;
        let event = obj.get("event")?.as_str()?.to_string();
        let topic = obj
            .get("topic")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let is_trade_event = event.eq_ignore_ascii_case("trade");
        let is_error_event = event.eq_ignore_ascii_case("error");
        if !is_trade_event && !is_error_event {
            return None;
        }
        if !topic.is_empty() && topic != "place-order" && topic != "cancel-order" {
            return None;
        }
        let first = obj
            .get("args")
            .and_then(|v| v.as_array())
            .and_then(|args| args.first())
            .and_then(|v| v.as_object());
        let create_time_ms = first
            .and_then(|first| first.get("cTime"))
            .and_then(parse_i64_value)
            .unwrap_or(0);
        Some(Self {
            event,
            id: obj.get("id").and_then(parse_i64_value).unwrap_or(0),
            category: obj
                .get("category")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            topic,
            code: obj.get("code")?.as_str()?.to_string(),
            msg: obj.get("msg")?.as_str()?.to_string(),
            order_id: first
                .and_then(|first| first.get("orderId"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            client_oid: first
                .and_then(|first| first.get("clientOid"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            create_time_ms,
            ts_ms: obj.get("ts").and_then(parse_i64_value).unwrap_or(0),
        })
    }

    pub fn is_success(&self) -> bool {
        self.code == "0" && self.msg.eq_ignore_ascii_case("success")
    }

    pub fn is_cancel(&self) -> bool {
        self.topic == "cancel-order"
    }

    pub fn client_order_id(&self) -> Option<i64> {
        self.client_oid.trim().parse::<i64>().ok()
    }

    pub fn order_id_i64(&self) -> i64 {
        self.order_id.trim().parse::<i64>().unwrap_or(0)
    }
}

fn parse_i64_value(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return Some(n as i64);
    }
    if let Some(s) = v.as_str() {
        return s.trim().parse::<i64>().ok();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{build_order_payload, BitgetWsOrderResponse};
    use crate::trade_request::{
        BitgetCancelOrderParams, BitgetNewOrderParams, TradeRequestMsg, TradeRequestType,
    };
    use order_common::{OrderType, Side};
    use serde_json::{json, Value};
    use signal_common::tick_math::QuantizedValue;

    fn trade_msg(
        req_type: TradeRequestType,
        client_order_id: i64,
        params: &[u8],
    ) -> TradeRequestMsg {
        TradeRequestMsg::create(req_type, 0, client_order_id, params).expect("trade request msg")
    }

    #[test]
    fn parses_bitget_trade_order_response() {
        let payload = r#"{
            "event":"trade",
            "id":"1750034396082",
            "category":"spot",
            "topic":"place-order",
            "args":[{"symbol":"BTCUSDT","orderId":"123","clientOid":"456","cTime":"1750034397008"}],
            "code":"0",
            "msg":"success"
        }"#;
        let resp = BitgetWsOrderResponse::from_json_str(payload).expect("bitget resp");
        assert_eq!(resp.event, "trade");
        assert_eq!(resp.id, 1750034396082);
        assert_eq!(resp.order_id, "123");
        assert_eq!(resp.client_oid, "456");
        assert!(resp.is_success());
    }

    #[test]
    fn parses_bitget_error_event_as_order_failure() {
        let payload = r#"{
            "event":"error",
            "id":"1750034396082",
            "topic":"place-order",
            "code":"30005",
            "msg":"open failed"
        }"#;
        let resp = BitgetWsOrderResponse::from_json_str(payload).expect("bitget error");
        assert_eq!(resp.event, "error");
        assert_eq!(resp.id, 1750034396082);
        assert_eq!(resp.code, "30005");
        assert_eq!(resp.msg, "open failed");
        assert!(!resp.is_success());
    }

    #[test]
    fn builds_bitget_um_order_payload_from_typed_params() {
        let params = BitgetNewOrderParams {
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity_qv: QuantizedValue::from_parts(1, -3, 10),
            price_qv: QuantizedValue::from_parts(1, 0, 100000),
            reduce_only: false,
        };
        let params = params.to_bytes().expect("typed params");
        let msg = trade_msg(TradeRequestType::BitgetNewUMOrder, 123, &params);
        let payload = build_order_payload(&msg, 999).expect("payload");
        let val: Value = serde_json::from_str(&payload).expect("json");
        assert_eq!(val["category"], json!("usdt-futures"));
        assert_eq!(val["topic"], json!("place-order"));
        assert_eq!(val["args"][0]["symbol"], json!("BTCUSDT"));
        assert_eq!(val["args"][0]["qty"], json!("0.010"));
        assert_eq!(val["args"][0]["timeInForce"], json!("post_only"));
        assert!(val["args"][0].get("category").is_none());
        assert!(val["args"][0].get("size").is_none());
        assert!(val["args"][0].get("force").is_none());
    }

    #[test]
    fn rejects_bitget_um_order_raw_json_params() {
        let msg = trade_msg(
            TradeRequestType::BitgetNewUMOrder,
            123,
            br#"{"category":"usdt-futures","symbol":"BTCUSDT","side":"buy","orderType":"limit","force":"post_only","size":"0.01","price":"100000","clientOid":"123"}"#,
        );
        let err = build_order_payload(&msg, 999).expect_err("raw json must be rejected");
        assert!(err.to_string().contains("requires typed params"));
    }

    #[test]
    fn rejects_bitget_um_cancel_raw_json_params() {
        let msg = trade_msg(
            TradeRequestType::BitgetCancelUMOrder,
            123,
            br#"{"orderId":"abc","clientOid":"123"}"#,
        );
        let err = build_order_payload(&msg, 999).expect_err("raw json must be rejected");
        assert!(err.to_string().contains("requires typed params"));
    }

    #[test]
    fn builds_bitget_um_cancel_payload_from_typed_params() {
        let params = BitgetCancelOrderParams {
            order_id: Some("abc".to_string()),
            client_order_id: "123".to_string(),
        };
        let params = params.to_bytes().expect("typed cancel params");
        let msg = trade_msg(TradeRequestType::BitgetCancelUMOrder, 123, &params);
        let payload = build_order_payload(&msg, 999).expect("payload");
        let val: Value = serde_json::from_str(&payload).expect("json");
        assert_eq!(val["topic"], json!("cancel-order"));
        assert_eq!(val["category"], json!("usdt-futures"));
        assert_eq!(val["args"][0]["orderId"], json!("abc"));
        assert_eq!(val["args"][0]["clientOid"], json!("123"));
        assert!(val["args"][0].get("category").is_none());
    }
}
