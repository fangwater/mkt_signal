use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

use crate::portfolio_margin::bitget_auth::BitgetCredentials;
use crate::trade_engine::trade_request::{TradeRequestMsg, TradeRequestType};

pub fn build_login_payload(creds: &BitgetCredentials) -> Result<String> {
    serde_json::to_string(&creds.build_login_message()).with_context(|| "serialize bitget login payload")
}

pub fn build_order_payload(msg: &TradeRequestMsg, transport_id: i64) -> Result<String> {
    let topic = match msg.req_type {
        TradeRequestType::BitgetNewMarginOrder | TradeRequestType::BitgetNewUMOrder => "trade",
        _ => {
            return Err(anyhow!(
                "unsupported bitget ws request type: {:?}",
                msg.req_type
            ))
        }
    };
    let args: Value =
        serde_json::from_slice(&msg.params).with_context(|| "invalid bitget req_param json")?;
    let args = match args {
        Value::Object(_) => Value::Array(vec![args]),
        Value::Array(_) => args,
        _ => return Err(anyhow!("bitget req_param must be object or array")),
    };
    let payload = json!({
        "id": transport_id.to_string(),
        "op": "trade",
        "topic": if topic == "trade" { "place-order" } else { topic },
        "args": args,
    });
    serde_json::to_string(&payload).with_context(|| "serialize bitget ws payload")
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
        let topic = obj.get("topic")?.as_str()?.to_string();
        if event != "trade" || (topic != "place-order" && topic != "cancel-order") {
            return None;
        }
        let args = obj.get("args")?.as_array()?;
        let first = args.first()?.as_object()?;
        let create_time_ms = first
            .get("cTime")
            .and_then(parse_i64_value)
            .unwrap_or(0);
        Some(Self {
            event,
            id: obj.get("id").and_then(parse_i64_value).unwrap_or(0),
            category: obj.get("category")?.as_str()?.to_string(),
            topic,
            code: obj.get("code")?.as_str()?.to_string(),
            msg: obj.get("msg")?.as_str()?.to_string(),
            order_id: first.get("orderId")?.as_str()?.to_string(),
            client_oid: first.get("clientOid")?.as_str()?.to_string(),
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
