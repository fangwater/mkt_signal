use crate::portfolio_margin::gate_auth::GateCredentials;
use crate::trade_engine::query_request::{QueryRequestMsg, QueryRequestType};
use crate::trade_engine::trade_request::{TradeRequestMsg, TradeRequestType};
use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

const CHANNEL_SPOT_LOGIN: &str = "spot.login";
const CHANNEL_FUTURES_LOGIN: &str = "futures.login";
const CHANNEL_SPOT_ORDER_PLACE: &str = "spot.order_place";
const CHANNEL_SPOT_ORDER_CANCEL: &str = "spot.order_cancel";
const CHANNEL_SPOT_ORDER_STATUS: &str = "spot.order_status";
const CHANNEL_FUTURES_ORDER_PLACE: &str = "futures.order_place";
const CHANNEL_FUTURES_ORDER_CANCEL: &str = "futures.order_cancel";
const CHANNEL_FUTURES_ORDER_STATUS: &str = "futures.order_status";
const EVENT_API: &str = "api";

fn sign_ws_api(secret: &str, event: &str, channel: &str, req_param: &str, timestamp: i64) -> String {
    let sign_str = format!("{event}\n{channel}\n{req_param}\n{timestamp}");
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes()).expect("invalid secret");
    mac.update(sign_str.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateWsKind {
    SpotUnified,
    FuturesUsdt,
}

impl GateWsKind {
    pub fn login_channel(self) -> &'static str {
        match self {
            GateWsKind::SpotUnified => CHANNEL_SPOT_LOGIN,
            GateWsKind::FuturesUsdt => CHANNEL_FUTURES_LOGIN,
        }
    }

    pub fn default_request_type(self) -> TradeRequestType {
        match self {
            GateWsKind::SpotUnified => TradeRequestType::GateUnifiedNewOrder,
            GateWsKind::FuturesUsdt => TradeRequestType::GateFuturesNewOrder,
        }
    }
}

pub fn build_login_message(creds: &GateCredentials) -> (String, String) {
    build_login_message_with_kind(creds, GateWsKind::SpotUnified)
}

pub fn build_login_message_with_kind(
    creds: &GateCredentials,
    kind: GateWsKind,
) -> (String, String) {
    let ts_s = chrono::Utc::now().timestamp();
    let req_id = format!("login-{}", chrono::Utc::now().timestamp_millis());
    let login_channel = kind.login_channel();
    let signature = sign_ws_api(&creds.secret_key, EVENT_API, login_channel, "", ts_s);
    let payload = json!({
        "time": ts_s,
        "channel": login_channel,
        "event": EVENT_API,
        "payload": {
            "req_id": req_id,
            "api_key": creds.api_key,
            "signature": signature,
            "timestamp": ts_s.to_string(),
        }
    });
    let msg = serde_json::to_string(&payload).expect("gate login json");
    (msg, req_id)
}

fn ensure_gate_text_prefix(req_param: &mut Value, client_order_id: i64) {
    let Some(obj) = req_param.as_object_mut() else {
        return;
    };
    let default_text = format!("t-{}", client_order_id);
    let text_value = obj
        .get("text")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string());
    match text_value {
        Some(text) => {
            if !text.starts_with("t-") {
                obj.insert("text".to_string(), Value::String(format!("t-{}", text)));
            }
        }
        None => {
            obj.insert("text".to_string(), Value::String(default_text));
        }
    }
}

pub fn build_api_payload(msg: &TradeRequestMsg) -> Result<String> {
    let channel = match msg.req_type {
        TradeRequestType::GateUnifiedNewOrder => CHANNEL_SPOT_ORDER_PLACE,
        TradeRequestType::GateUnifiedCancelOrder => CHANNEL_SPOT_ORDER_CANCEL,
        TradeRequestType::GateFuturesNewOrder => CHANNEL_FUTURES_ORDER_PLACE,
        TradeRequestType::GateFuturesCancelOrder => CHANNEL_FUTURES_ORDER_CANCEL,
        _ => {
            return Err(anyhow!(
                "unsupported gate request type: {:?}",
                msg.req_type
            ))
        }
    };

    let mut req_param: Value = serde_json::from_slice(&msg.params)
        .with_context(|| "invalid gate req_param json")?;
    if matches!(
        msg.req_type,
        TradeRequestType::GateUnifiedNewOrder | TradeRequestType::GateFuturesNewOrder
    ) {
        ensure_gate_text_prefix(&mut req_param, msg.client_order_id);
    }

    let ts_s = chrono::Utc::now().timestamp();
    let payload = json!({
        "time": ts_s,
        "channel": channel,
        "event": EVENT_API,
        "payload": {
            "req_id": msg.client_order_id.to_string(),
            "req_param": req_param,
        }
    });
    serde_json::to_string(&payload).with_context(|| "serialize gate ws payload")
}

pub fn build_query_payload(msg: &QueryRequestMsg) -> Result<String> {
    let channel = match msg.req_type {
        QueryRequestType::GateUnifiedOrderQuery => CHANNEL_SPOT_ORDER_STATUS,
        QueryRequestType::GateFuturesOrderQuery => CHANNEL_FUTURES_ORDER_STATUS,
        _ => {
            return Err(anyhow!(
                "unsupported gate query type: {:?}",
                msg.req_type
            ))
        }
    };

    let req_param: Value = serde_json::from_slice(&msg.params)
        .with_context(|| "invalid gate query req_param json")?;

    let ts_s = chrono::Utc::now().timestamp();
    let payload = json!({
        "time": ts_s,
        "channel": channel,
        "event": EVENT_API,
        "payload": {
            "req_id": msg.client_query_id.to_string(),
            "req_param": req_param,
        }
    });
    serde_json::to_string(&payload).with_context(|| "serialize gate ws query payload")
}
