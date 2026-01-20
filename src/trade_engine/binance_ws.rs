use crate::trade_engine::config::{ApiKey, RestConstants};
use crate::trade_engine::query_request::{QueryRequestMsg, QueryRequestType};
use crate::trade_engine::trade_request::{TradeRequestMsg, TradeRequestType};
use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::BTreeMap;

type HmacSha256 = Hmac<Sha256>;

const METHOD_ORDER_PLACE: &str = "order.place";
const METHOD_ORDER_CANCEL: &str = "order.cancel";
const METHOD_ORDER_STATUS: &str = "order.status";

fn parse_i64_value(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return Some(n as i64);
    }
    if let Some(s) = v.as_str() {
        let s = s.trim();
        if let Ok(parsed) = s.parse::<i64>() {
            return Some(parsed);
        }
    }
    None
}

fn parse_u16_value(v: &Value) -> Option<u16> {
    if let Some(n) = v.as_u64() {
        return u16::try_from(n).ok();
    }
    if let Some(n) = v.as_i64() {
        return u16::try_from(n).ok();
    }
    if let Some(s) = v.as_str() {
        return s.parse::<u16>().ok();
    }
    None
}

fn parse_params(raw: &[u8]) -> Result<BTreeMap<String, String>> {
    let raw_str = std::str::from_utf8(raw).with_context(|| "binance ws params not utf8")?;
    Ok(url::form_urlencoded::parse(raw_str.as_bytes())
        .into_owned()
        .collect())
}

fn serialize_params(params: &BTreeMap<String, String>) -> String {
    let mut ser = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in params.iter() {
        ser.append_pair(k, v);
    }
    ser.finish()
}

fn sign_params(params: &BTreeMap<String, String>, secret: &str) -> Result<String> {
    let query = serialize_params(params);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow!("invalid binance secret"))?;
    mac.update(query.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn build_signed_params(raw: &[u8], creds: &ApiKey) -> Result<BTreeMap<String, String>> {
    let mut params = parse_params(raw)?;
    params.insert("apiKey".to_string(), creds.key.trim().to_string());
    params.insert(
        "timestamp".to_string(),
        chrono::Utc::now().timestamp_millis().to_string(),
    );
    params
        .entry("recvWindow".to_string())
        .or_insert_with(|| RestConstants::RECV_WINDOW_MS.to_string());
    params.remove("signature");
    let sig = sign_params(&params, creds.secret.trim())?;
    params.insert("signature".to_string(), sig);
    Ok(params)
}

pub fn build_order_payload(msg: &TradeRequestMsg, creds: &ApiKey) -> Result<String> {
    let method = match msg.req_type {
        TradeRequestType::BinanceWsNewUMOrder => METHOD_ORDER_PLACE,
        TradeRequestType::BinanceWsCancelUMOrder => METHOD_ORDER_CANCEL,
        _ => {
            return Err(anyhow!(
                "unsupported binance ws request type: {:?}",
                msg.req_type
            ))
        }
    };

    let params = build_signed_params(&msg.params, creds)?;
    let payload = json!({
        "id": msg.client_order_id,
        "method": method,
        "params": params,
    });
    serde_json::to_string(&payload).with_context(|| "serialize binance ws payload")
}

pub fn build_query_payload(msg: &QueryRequestMsg, creds: &ApiKey) -> Result<String> {
    if msg.req_type != QueryRequestType::BinanceWsUMQuery {
        return Err(anyhow!(
            "unsupported binance ws query type: {:?}",
            msg.req_type
        ));
    }

    let params = build_signed_params(&msg.params, creds)?;
    let payload = json!({
        "id": msg.client_query_id,
        "method": METHOD_ORDER_STATUS,
        "params": params,
    });
    serde_json::to_string(&payload).with_context(|| "serialize binance ws query payload")
}

#[derive(Debug, Clone)]
pub struct BinanceWsResponse {
    pub id: Option<i64>,
    pub status: Option<u16>,
    pub error_code: Option<i32>,
    pub error_msg: Option<String>,
    pub result: Option<Value>,
}

pub fn parse_ws_response(payload: &str) -> Option<BinanceWsResponse> {
    let val: Value = serde_json::from_str(payload).ok()?;
    let id = val.get("id").and_then(parse_i64_value);
    let status = val.get("status").and_then(parse_u16_value);
    let (error_code, error_msg) = if let Some(err) = val.get("error") {
        let code = err.get("code").and_then(parse_i64_value).map(|v| v as i32);
        let msg = err
            .get("msg")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        (code, msg)
    } else {
        (None, None)
    };
    let result = val.get("result").cloned();
    Some(BinanceWsResponse {
        id,
        status,
        error_code,
        error_msg,
        result,
    })
}
