use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

use crate::portfolio_margin::bitget_auth::BitgetCredentials;
use crate::trade_engine::trade_request::{TradeRequestMsg, TradeRequestType};

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
    let args: Value =
        serde_json::from_slice(&msg.params).with_context(|| "invalid bitget req_param json")?;
    let (category, args) = match args {
        Value::Object(obj) => {
            let (category, arg) = normalize_bitget_trade_arg(obj, req_type)?;
            (category, Value::Array(vec![Value::Object(arg)]))
        }
        Value::Array(arr) => {
            let mut normalized = Vec::with_capacity(arr.len());
            let mut category = None;
            for item in arr {
                let Value::Object(obj) = item else {
                    return Err(anyhow!("bitget req_param array items must be objects"));
                };
                let (current_category, arg) = normalize_bitget_trade_arg(obj, req_type)?;
                if let Some(prev) = &category {
                    if prev != &current_category {
                        return Err(anyhow!(
                            "bitget req_param array has mixed categories: {} vs {}",
                            prev,
                            current_category
                        ));
                    }
                } else {
                    category = Some(current_category);
                }
                normalized.push(Value::Object(arg));
            }
            let category =
                category.ok_or_else(|| anyhow!("bitget req_param array must not be empty"))?;
            (category, Value::Array(normalized))
        }
        _ => return Err(anyhow!("bitget req_param must be object or array")),
    };
    let payload = json!({
        "id": transport_id.to_string(),
        "op": "trade",
        "category": category,
        "topic": topic,
        "args": args,
    });
    serde_json::to_string(&payload).with_context(|| "serialize bitget ws payload")
}

fn normalize_bitget_trade_arg(
    mut obj: serde_json::Map<String, Value>,
    req_type: TradeRequestType,
) -> Result<(String, serde_json::Map<String, Value>)> {
    if !obj.contains_key("category") {
        let default_category = match req_type {
            TradeRequestType::BitgetNewMarginOrder | TradeRequestType::BitgetCancelMarginOrder => {
                "spot"
            }
            TradeRequestType::BitgetNewUMOrder | TradeRequestType::BitgetCancelUMOrder => {
                "usdt-futures"
            }
            _ => return Err(anyhow!("unsupported bitget req_type for category default")),
        };
        obj.insert("category".to_string(), json!(default_category));
    }

    match req_type {
        TradeRequestType::BitgetNewMarginOrder | TradeRequestType::BitgetNewUMOrder => {
            // Bitget WS trade API expects qty/timeInForce. Upstream still emits size/force.
            if !obj.contains_key("qty") {
                if let Some(size) = obj.remove("size") {
                    obj.insert("qty".to_string(), size);
                }
            }
            if !obj.contains_key("timeInForce") {
                if let Some(force) = obj.remove("force") {
                    obj.insert("timeInForce".to_string(), force);
                }
            }
        }
        TradeRequestType::BitgetCancelMarginOrder | TradeRequestType::BitgetCancelUMOrder => {
            if !obj.contains_key("orderId") && !obj.contains_key("clientOid") {
                return Err(anyhow!("bitget cancel-order requires orderId or clientOid"));
            }
        }
        _ => return Err(anyhow!("unsupported bitget req_type")),
    }

    let category = extract_bitget_category(&obj)?;
    obj.remove("category");
    Ok((category, obj))
}

fn extract_bitget_category(obj: &serde_json::Map<String, Value>) -> Result<String> {
    obj.get("category")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("bitget req_param missing category"))
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
    use crate::trade_engine::trade_request::{TradeRequestMsg, TradeRequestType};
    use bytes::Bytes;
    use serde_json::{json, Value};

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
    fn builds_bitget_um_order_payload_with_top_level_category() {
        let msg = TradeRequestMsg {
            req_type: TradeRequestType::BitgetNewUMOrder,
            create_time: 0,
            client_order_id: 123,
            params: Bytes::from(
                r#"{"category":"usdt-futures","symbol":"BTCUSDT","side":"buy","orderType":"limit","force":"post_only","size":"0.01","price":"100000","clientOid":"123"}"#,
            ),
        };
        let payload = build_order_payload(&msg, 999).expect("payload");
        let val: Value = serde_json::from_str(&payload).expect("json");
        assert_eq!(val["category"], json!("usdt-futures"));
        assert_eq!(val["topic"], json!("place-order"));
        assert_eq!(val["args"][0]["qty"], json!("0.01"));
        assert_eq!(val["args"][0]["timeInForce"], json!("post_only"));
        assert!(val["args"][0].get("category").is_none());
        assert!(val["args"][0].get("size").is_none());
        assert!(val["args"][0].get("force").is_none());
    }

    #[test]
    fn fills_missing_bitget_um_category_from_request_type() {
        let msg = TradeRequestMsg {
            req_type: TradeRequestType::BitgetNewUMOrder,
            create_time: 0,
            client_order_id: 123,
            params: Bytes::from(
                r#"{"symbol":"BTCUSDT","side":"buy","orderType":"limit","force":"post_only","size":"0.01","price":"100000","clientOid":"123"}"#,
            ),
        };
        let payload = build_order_payload(&msg, 999).expect("payload");
        let val: Value = serde_json::from_str(&payload).expect("json");
        assert_eq!(val["category"], json!("usdt-futures"));
        assert!(val["args"][0].get("category").is_none());
    }

    #[test]
    fn builds_bitget_um_cancel_payload_in_uta_format() {
        let msg = TradeRequestMsg {
            req_type: TradeRequestType::BitgetCancelUMOrder,
            create_time: 0,
            client_order_id: 123,
            params: Bytes::from(r#"{"orderId":"abc","clientOid":"123"}"#),
        };
        let payload = build_order_payload(&msg, 999).expect("payload");
        let val: Value = serde_json::from_str(&payload).expect("json");
        assert_eq!(val["op"], json!("trade"));
        assert_eq!(val["topic"], json!("cancel-order"));
        assert_eq!(val["category"], json!("usdt-futures"));
        assert_eq!(val["args"][0]["orderId"], json!("abc"));
        assert_eq!(val["args"][0]["clientOid"], json!("123"));
        assert!(val["args"][0].get("category").is_none());
    }
}
