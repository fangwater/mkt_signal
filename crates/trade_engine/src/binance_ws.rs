use crate::config::RestConstants;
use crate::query_request::{QueryRequestMsg, QueryRequestType};
use crate::trade_request::{
    BinanceCancelOrderParamsRef, BinanceNewOrderParamsRef, TradeRequestMsg, TradeRequestType,
};
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use openssl::pkey::{Id as PKeyId, PKey, Private};
use openssl::sign::Signer;
use serde_json::Value;
use sha2::Sha256;
use signal_common::tick_math::QuantizedDecimal;

type HmacSha256 = Hmac<Sha256>;

pub const BINANCE_ED25519_PRIVATE_KEY_PATH_ENV: &str = "BINANCE_ED25519_PRIVATE_KEY_PATH";
pub const BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE_ENV: &str =
    "BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE";

const METHOD_ORDER_PLACE: &str = "order.place";
const METHOD_ORDER_CANCEL: &str = "order.cancel";
const METHOD_ORDER_STATUS: &str = "order.status";
const METHOD_SESSION_LOGON: &str = "session.logon";
const BINANCE_RECV_WINDOW_MS: &str = "5000";

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

fn parse_u32_value(v: &Value) -> Option<u32> {
    if let Some(n) = v.as_u64() {
        return u32::try_from(n).ok();
    }
    if let Some(n) = v.as_i64() {
        return u32::try_from(n).ok();
    }
    if let Some(s) = v.as_str() {
        return s.parse::<u32>().ok();
    }
    None
}

fn parse_f64_value(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    if let Some(n) = v.as_i64() {
        return Some(n as f64);
    }
    if let Some(n) = v.as_u64() {
        return Some(n as f64);
    }
    if let Some(s) = v.as_str() {
        let s = s.trim();
        if let Ok(parsed) = s.parse::<f64>() {
            return Some(parsed);
        }
    }
    None
}

fn feed_ordered_payload(params: &[(&str, &str)], mut feed: impl FnMut(&[u8])) {
    for (idx, (key, value)) in params.iter().enumerate() {
        if idx > 0 {
            feed(b"&");
        }
        feed(key.as_bytes());
        feed(b"=");
        feed(value.as_bytes());
    }
}

fn ordered_payload_len(params: &[(&str, &str)]) -> usize {
    params
        .iter()
        .map(|(key, value)| key.len() + value.len() + 1)
        .sum::<usize>()
        + params.len().saturating_sub(1)
}

fn write_ordered_payload_to_slice(out: &mut [u8], params: &[(&str, &str)]) -> usize {
    let mut offset = 0usize;
    feed_ordered_payload(params, |chunk| {
        let end = offset + chunk.len();
        out[offset..end].copy_from_slice(chunk);
        offset = end;
    });
    offset
}

fn push_ordered_payload(out: &mut Vec<u8>, params: &[(&str, &str)]) {
    out.reserve(ordered_payload_len(params));
    feed_ordered_payload(params, |chunk| out.extend_from_slice(chunk));
}

fn sign_ordered_params_hmac_fast(params: &[(&str, &str)], secret: &str) -> Result<[u8; 64]> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow!("invalid binance secret"))?;
    feed_ordered_payload(params, |chunk| mac.update(chunk));
    let digest = mac.finalize().into_bytes();
    let mut out = [0u8; 64];
    hex::encode_to_slice(digest, &mut out).expect("sha256 hmac hex output is exactly 64 bytes");
    Ok(out)
}

#[derive(Debug)]
pub enum BinanceWsSigner {
    Ed25519 { key: PKey<Private>, source: String },
    HmacSha256 { secret: String },
}

impl BinanceWsSigner {
    pub fn from_env_or_secret(secret: String) -> Result<Self> {
        let ed25519_path = std::env::var(BINANCE_ED25519_PRIVATE_KEY_PATH_ENV)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());

        if let Some(path) = ed25519_path {
            return Self::from_ed25519_pem_path(&path);
        }

        if secret.trim().is_empty() {
            return Err(anyhow!(
                "Binance WS signing requires either BINANCE_API_SECRET or {}",
                BINANCE_ED25519_PRIVATE_KEY_PATH_ENV
            ));
        }
        Ok(Self::HmacSha256 { secret })
    }

    pub fn from_ed25519_pem_path(path: &str) -> Result<Self> {
        let passphrase = std::env::var(BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE_ENV)
            .ok()
            .unwrap_or_default();
        let pem = std::fs::read(path)
            .with_context(|| format!("read {}={}", BINANCE_ED25519_PRIVATE_KEY_PATH_ENV, path))?;
        let key = if passphrase.is_empty() {
            PKey::private_key_from_pem(&pem).with_context(|| {
                format!(
                    "parse Ed25519 private key PEM from {}",
                    BINANCE_ED25519_PRIVATE_KEY_PATH_ENV
                )
            })?
        } else {
            PKey::private_key_from_pem_passphrase(&pem, passphrase.as_bytes()).with_context(
                || {
                    format!(
                        "parse encrypted Ed25519 private key PEM from {}",
                        BINANCE_ED25519_PRIVATE_KEY_PATH_ENV
                    )
                },
            )?
        };
        if key.id() != PKeyId::ED25519 {
            return Err(anyhow!(
                "{}={} is not an Ed25519 private key",
                BINANCE_ED25519_PRIVATE_KEY_PATH_ENV,
                path
            ));
        }
        Ok(Self::Ed25519 {
            key,
            source: path.to_string(),
        })
    }

    pub fn algorithm(&self) -> &'static str {
        match self {
            Self::Ed25519 { .. } => "ed25519",
            Self::HmacSha256 { .. } => "hmac_sha256",
        }
    }

    pub fn ed25519_source(&self) -> Option<&str> {
        match self {
            Self::Ed25519 { source, .. } => Some(source.as_str()),
            Self::HmacSha256 { .. } => None,
        }
    }

    pub fn uses_session_logon(&self) -> bool {
        matches!(self, Self::Ed25519 { .. })
    }

    fn sign_ordered_params(&self, params: &[(&str, &str)]) -> Result<String> {
        match self {
            Self::HmacSha256 { secret } => {
                let signature = sign_ordered_params_hmac_fast(params, secret.trim())?;
                Ok(std::str::from_utf8(&signature)
                    .expect("hex signature is valid utf8")
                    .to_string())
            }
            Self::Ed25519 { key, .. } => {
                let payload_len = ordered_payload_len(params);
                let mut signer = Signer::new_without_digest(key)
                    .with_context(|| "create Binance WS Ed25519 signer")?;
                let mut signature = [0u8; 64];
                let len = if payload_len <= 1024 {
                    let mut payload = [0u8; 1024];
                    let written =
                        write_ordered_payload_to_slice(&mut payload[..payload_len], params);
                    debug_assert_eq!(written, payload_len);
                    signer
                        .sign_oneshot(&mut signature, &payload[..payload_len])
                        .with_context(|| "sign Binance WS payload with Ed25519")?
                } else {
                    let mut payload = Vec::with_capacity(payload_len);
                    push_ordered_payload(&mut payload, params);
                    signer
                        .sign_oneshot(&mut signature, &payload)
                        .with_context(|| "sign Binance WS payload with Ed25519")?
                };
                Ok(BASE64_STANDARD.encode(&signature[..len]))
            }
        }
    }
}

fn push_json_string(out: &mut String, value: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";

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
            ch if ch <= '\u{1f}' => {
                let code = ch as usize;
                out.push_str("\\u00");
                out.push(HEX[(code >> 4) & 0x0f] as char);
                out.push(HEX[code & 0x0f] as char);
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}

fn push_json_param(out: &mut String, first: &mut bool, key: &str, value: &str) {
    if *first {
        *first = false;
    } else {
        out.push(',');
    }
    push_json_string(out, key);
    out.push(':');
    push_json_string(out, value);
}

fn build_payload_json(
    transport_id: i64,
    method: &str,
    params: &[(&str, &str)],
    signature: Option<&str>,
) -> String {
    let id = transport_id.to_string();
    let params_bytes: usize = params.iter().map(|(k, v)| k.len() + v.len() + 6).sum();
    let signature_bytes = signature
        .map(|sig| "signature".len() + sig.len() + 6)
        .unwrap_or(0);
    let mut out =
        String::with_capacity(32 + id.len() + method.len() + params_bytes + signature_bytes);
    out.push_str("{\"id\":");
    out.push_str(&id);
    out.push_str(",\"method\":");
    push_json_string(&mut out, method);
    out.push_str(",\"params\":{");

    let mut first = true;
    for (key, value) in params.iter() {
        push_json_param(&mut out, &mut first, key, value);
    }
    if let Some(signature) = signature {
        push_json_param(&mut out, &mut first, "signature", signature);
    }
    out.push_str("}}");
    out
}

fn build_signed_payload_json(
    transport_id: i64,
    method: &str,
    params: &[(&str, &str)],
    signer: &BinanceWsSigner,
) -> Result<String> {
    let signature = signer.sign_ordered_params(params)?;
    Ok(build_payload_json(
        transport_id,
        method,
        params,
        Some(signature.as_str()),
    ))
}

fn build_authorized_payload_json(
    transport_id: i64,
    method: &str,
    params: &[(&str, &str)],
    signer: &BinanceWsSigner,
) -> Result<String> {
    if signer.uses_session_logon() {
        return Ok(build_payload_json(transport_id, method, params, None));
    }
    build_signed_payload_json(transport_id, method, params, signer)
}

pub fn build_session_logon_payload(
    transport_id: i64,
    api_key: &str,
    signer: &BinanceWsSigner,
) -> Result<String> {
    if !signer.uses_session_logon() {
        return Err(anyhow!("Binance session.logon requires an Ed25519 signer"));
    }
    let timestamp = current_timestamp_ms_string();
    let ordered = [
        ("apiKey", api_key),
        ("recvWindow", BINANCE_RECV_WINDOW_MS),
        ("timestamp", timestamp.as_str()),
    ];
    build_signed_payload_json(transport_id, METHOD_SESSION_LOGON, &ordered, signer)
}

fn current_timestamp_ms_string() -> String {
    chrono::Utc::now().timestamp_millis().to_string()
}

fn build_new_order_payload_fast(
    params: &BinanceNewOrderParamsRef<'_>,
    req_type: TradeRequestType,
    client_order_id: i64,
    transport_id: i64,
    api_key: &str,
    signer: &BinanceWsSigner,
) -> Result<String> {
    let is_margin = matches!(
        req_type,
        TradeRequestType::BinanceNewMarginOrder | TradeRequestType::BinanceWsNewMarginOrder
    );
    let is_ws_margin = req_type == TradeRequestType::BinanceWsNewMarginOrder;
    let is_ws_um = req_type == TradeRequestType::BinanceWsNewUMOrder;
    let order_type = if is_ws_margin && params.ws_margin_limit_maker && params.order_type.is_limit()
    {
        "LIMIT_MAKER"
    } else {
        params.order_type.as_str()
    };

    let client_order_id = client_order_id.to_string();
    let quantity = QuantizedDecimal::try_from_value(params.quantity_qv)
        .ok_or_else(|| anyhow!("binance order quantity decimal exceeds inline buffer"))?;
    let price = params
        .order_type
        .is_limit()
        .then(|| {
            QuantizedDecimal::try_from_value(params.price_qv)
                .ok_or_else(|| anyhow!("binance order price decimal exceeds inline buffer"))
        })
        .transpose()?;
    let timestamp = current_timestamp_ms_string();
    let reduce_only = if params.reduce_only { "true" } else { "false" };
    let new_order_resp_type = if params.ws_response_full {
        Some("FULL")
    } else if is_ws_um && params.ws_um_response_result {
        Some("RESULT")
    } else {
        None
    };
    let side_effect_type = if is_margin && params.margin_buy {
        Some("MARGIN_BUY")
    } else {
        None
    };
    let time_in_force = if params.order_type.is_limit() {
        if is_margin {
            (!is_ws_margin).then_some("GTC")
        } else {
            Some("GTX")
        }
    } else {
        None
    };

    let mut ordered = [("", ""); 13];
    let mut len = 0usize;
    if !signer.uses_session_logon() {
        ordered[len] = ("apiKey", api_key);
        len += 1;
    }
    ordered[len] = ("newClientOrderId", client_order_id.as_str());
    len += 1;
    if let Some(value) = new_order_resp_type {
        ordered[len] = ("newOrderRespType", value);
        len += 1;
    }
    if let Some(value) = price.as_ref() {
        ordered[len] = ("price", value.as_str());
        len += 1;
    }
    ordered[len] = ("quantity", quantity.as_str());
    len += 1;
    ordered[len] = ("recvWindow", BINANCE_RECV_WINDOW_MS);
    len += 1;
    if !is_margin {
        ordered[len] = ("reduceOnly", reduce_only);
        len += 1;
    }
    ordered[len] = ("side", params.side.as_str());
    len += 1;
    if let Some(value) = side_effect_type {
        ordered[len] = ("sideEffectType", value);
        len += 1;
    }
    ordered[len] = ("symbol", params.symbol);
    len += 1;
    if let Some(value) = time_in_force {
        ordered[len] = ("timeInForce", value);
        len += 1;
    }
    ordered[len] = ("timestamp", timestamp.as_str());
    len += 1;
    ordered[len] = ("type", order_type);
    len += 1;

    build_authorized_payload_json(transport_id, METHOD_ORDER_PLACE, &ordered[..len], signer)
}

fn build_cancel_order_payload_fast(
    params: &BinanceCancelOrderParamsRef<'_>,
    transport_id: i64,
    api_key: &str,
    signer: &BinanceWsSigner,
) -> Result<String> {
    let orig_client_order_id = params.orig_client_order_id.to_string();
    let timestamp = current_timestamp_ms_string();
    let mut ordered = [("", ""); 5];
    let mut len = 0usize;
    if !signer.uses_session_logon() {
        ordered[len] = ("apiKey", api_key);
        len += 1;
    }
    ordered[len] = ("origClientOrderId", orig_client_order_id.as_str());
    len += 1;
    ordered[len] = ("recvWindow", BINANCE_RECV_WINDOW_MS);
    len += 1;
    ordered[len] = ("symbol", params.symbol);
    len += 1;
    ordered[len] = ("timestamp", timestamp.as_str());
    len += 1;
    build_authorized_payload_json(transport_id, METHOD_ORDER_CANCEL, &ordered[..len], signer)
}

fn build_typed_order_payload_fast(
    msg: &TradeRequestMsg,
    transport_id: i64,
    api_key: &str,
    signer: &BinanceWsSigner,
) -> Result<String> {
    match msg.req_type {
        TradeRequestType::BinanceWsNewUMOrder | TradeRequestType::BinanceWsNewMarginOrder => {
            let params = BinanceNewOrderParamsRef::from_bytes(&msg.params).ok_or_else(|| {
                anyhow!(
                    "invalid typed binance ws new order params for {:?}",
                    msg.req_type
                )
            })?;
            build_new_order_payload_fast(
                &params,
                msg.req_type,
                msg.client_order_id,
                transport_id,
                api_key,
                signer,
            )
        }
        TradeRequestType::BinanceWsCancelUMOrder | TradeRequestType::BinanceWsCancelMarginOrder => {
            let params = BinanceCancelOrderParamsRef::from_bytes(&msg.params).ok_or_else(|| {
                anyhow!(
                    "invalid typed binance ws cancel order params for {:?}",
                    msg.req_type
                )
            })?;
            build_cancel_order_payload_fast(&params, transport_id, api_key, signer)
        }
        _ => Err(anyhow!(
            "unsupported binance ws request type: {:?}",
            msg.req_type
        )),
    }
}

#[derive(Default)]
struct OrderStatusParams {
    symbol: Option<String>,
    order_id: Option<String>,
    orig_client_order_id: Option<String>,
    recv_window: Option<String>,
}

fn parse_order_status_params(raw: &[u8]) -> Result<OrderStatusParams> {
    let raw_str = std::str::from_utf8(raw).with_context(|| "binance ws query params not utf8")?;
    let mut params = OrderStatusParams::default();

    for (key, value) in url::form_urlencoded::parse(raw_str.as_bytes()) {
        match key.as_ref() {
            "symbol" => params.symbol = Some(value.into_owned()),
            "orderId" => params.order_id = Some(value.into_owned()),
            "origClientOrderId" => params.orig_client_order_id = Some(value.into_owned()),
            "recvWindow" => params.recv_window = Some(value.into_owned()),
            _ => return Err(anyhow!("unsupported binance ws order.status param: {key}")),
        }
    }

    if params.symbol.is_none() {
        return Err(anyhow!("missing binance ws order.status symbol param"));
    }
    match (
        params.order_id.is_some(),
        params.orig_client_order_id.is_some(),
    ) {
        (true, false) | (false, true) => Ok(params),
        (false, false) => Err(anyhow!(
            "missing binance ws order.status orderId/origClientOrderId param"
        )),
        (true, true) => Err(anyhow!(
            "binance ws order.status params must not include both orderId and origClientOrderId"
        )),
    }
}

fn build_order_status_payload_fast(
    raw: &[u8],
    transport_id: i64,
    api_key: &str,
    signer: &BinanceWsSigner,
) -> Result<String> {
    let params = parse_order_status_params(raw)?;
    let recv_window = params
        .recv_window
        .unwrap_or_else(|| RestConstants::RECV_WINDOW_MS.to_string());
    let timestamp = current_timestamp_ms_string();
    let symbol = params.symbol.expect("validated symbol");

    let mut ordered = Vec::with_capacity(6);
    if !signer.uses_session_logon() {
        ordered.push(("apiKey", api_key));
    }
    if let Some(order_id) = params.order_id.as_deref() {
        ordered.push(("orderId", order_id));
    }
    if let Some(orig_client_order_id) = params.orig_client_order_id.as_deref() {
        ordered.push(("origClientOrderId", orig_client_order_id));
    }
    ordered.push(("recvWindow", recv_window.as_str()));
    ordered.push(("symbol", symbol.as_str()));
    ordered.push(("timestamp", timestamp.as_str()));

    build_authorized_payload_json(transport_id, METHOD_ORDER_STATUS, &ordered, signer)
}

pub fn build_order_payload(
    msg: &TradeRequestMsg,
    transport_id: i64,
    api_key: &str,
    signer: &BinanceWsSigner,
) -> Result<String> {
    match msg.req_type {
        TradeRequestType::BinanceWsNewUMOrder | TradeRequestType::BinanceWsNewMarginOrder => {
            build_typed_order_payload_fast(msg, transport_id, api_key, signer)
        }
        TradeRequestType::BinanceWsCancelUMOrder | TradeRequestType::BinanceWsCancelMarginOrder => {
            build_typed_order_payload_fast(msg, transport_id, api_key, signer)
        }
        _ => Err(anyhow!(
            "unsupported binance ws request type: {:?}",
            msg.req_type
        )),
    }
}

pub fn build_query_payload(
    msg: &QueryRequestMsg,
    transport_id: i64,
    api_key: &str,
    signer: &BinanceWsSigner,
) -> Result<String> {
    if msg.req_type != QueryRequestType::BinanceWsUMQuery
        && msg.req_type != QueryRequestType::BinanceWsMarginQuery
    {
        return Err(anyhow!(
            "unsupported binance ws query type: {:?}",
            msg.req_type
        ));
    }

    build_order_status_payload_fast(&msg.params, transport_id, api_key, signer)
}

#[derive(Debug, Clone)]
pub struct BinanceWsRateLimit {
    pub rate_limit_type: String,
    pub interval: String,
    pub interval_num: u32,
    pub limit: u32,
    pub count: u32,
}

#[derive(Debug, Clone)]
pub struct BinanceWsResponse {
    pub id: Option<i64>,
    pub status: Option<u16>,
    pub error_code: Option<i32>,
    pub error_msg: Option<String>,
    pub result: Option<Value>,
    pub rate_limits: Vec<BinanceWsRateLimit>,
}

fn parse_rate_limits(val: &Value) -> Vec<BinanceWsRateLimit> {
    let Some(items) = val.get("rateLimits").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            Some(BinanceWsRateLimit {
                rate_limit_type: item.get("rateLimitType")?.as_str()?.to_string(),
                interval: item.get("interval")?.as_str()?.to_string(),
                interval_num: item.get("intervalNum").and_then(parse_u32_value)?,
                limit: item.get("limit").and_then(parse_u32_value)?,
                count: item.get("count").and_then(parse_u32_value)?,
            })
        })
        .collect()
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
        rate_limits: parse_rate_limits(&val),
    })
}

fn parse_order_status_u8(s: &str) -> Option<u8> {
    match s.to_uppercase().as_str() {
        "NEW" => Some(1),
        "PARTIALLY_FILLED" => Some(2),
        "FILLED" => Some(3),
        "CANCELED" | "CANCELLED" => Some(4),
        "EXPIRED" => Some(5),
        "EXPIRED_IN_MATCH" => Some(6),
        _ => None,
    }
}

/// Extract compact order info from Binance WS response result.
/// Returns (order_id, order_status_u8, update_time, executed_qty, price). Missing fields are returned as 0.
pub fn extract_order_info(resp: &BinanceWsResponse) -> (i64, u8, i64, f64, f64) {
    let Some(result) = resp.result.as_ref() else {
        return (0, 0, 0, 0.0, 0.0);
    };
    let order_id = result.get("orderId").and_then(parse_i64_value).unwrap_or(0);
    let status_u8 = result
        .get("status")
        .and_then(|v| v.as_str())
        .and_then(parse_order_status_u8)
        .unwrap_or(0);
    let update_time = result
        .get("updateTime")
        .and_then(parse_i64_value)
        .or_else(|| result.get("transactTime").and_then(parse_i64_value))
        .unwrap_or(0);
    let executed_qty = result
        .get("executedQty")
        .and_then(parse_f64_value)
        .unwrap_or(0.0);
    let price = result.get("price").and_then(parse_f64_value).unwrap_or(0.0);
    (order_id, status_u8, update_time, executed_qty, price)
}

#[cfg(test)]
mod tests {
    use super::{
        build_order_payload, build_query_payload, build_session_logon_payload, parse_ws_response,
        sign_ordered_params_hmac_fast, BinanceWsSigner,
    };
    use crate::query_request::{QueryRequestMsg, QueryRequestType};
    use crate::trade_request::{
        BinanceCancelOrderParams, BinanceNewOrderParams, TradeRequestMsg, TradeRequestType,
    };
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use base64::Engine;
    use bytes::Bytes;
    use openssl::pkey::PKey;
    use order_common::{OrderType, Side};
    use serde_json::Value;
    use signal_common::tick_math::QuantizedValue;
    use std::collections::BTreeMap;

    fn signer() -> BinanceWsSigner {
        BinanceWsSigner::HmacSha256 {
            secret: "secret".to_string(),
        }
    }

    fn trade_msg(
        req_type: TradeRequestType,
        client_order_id: i64,
        params: &[u8],
    ) -> TradeRequestMsg {
        TradeRequestMsg::create(req_type, 0, client_order_id, params).expect("trade request msg")
    }

    fn assert_signature_matches_sorted_params(value: &Value) {
        let params = value["params"].as_object().expect("params object");
        let mut sorted = BTreeMap::new();
        for (key, value) in params {
            if key == "signature" {
                continue;
            }
            sorted.insert(
                key.clone(),
                value.as_str().expect("string param").to_string(),
            );
        }
        let ordered: Vec<(&str, &str)> = sorted
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        let expected = sign_ordered_params_hmac_fast(&ordered, "secret").expect("signature");
        let expected = std::str::from_utf8(&expected).expect("hex signature utf8");
        assert_eq!(
            value["params"]["signature"].as_str().expect("signature"),
            expected
        );
    }

    #[test]
    fn ed25519_signer_emits_base64_signature() {
        let key = PKey::generate_ed25519().expect("ed25519 key");
        let signer = BinanceWsSigner::Ed25519 {
            key,
            source: "generated-test-key".to_string(),
        };
        let signature = signer
            .sign_ordered_params(&[("apiKey", "api-key"), ("timestamp", "123")])
            .expect("ed25519 signature");
        let decoded = BASE64_STANDARD.decode(signature).expect("base64 signature");
        assert_eq!(decoded.len(), 64);
    }

    #[test]
    fn builds_ed25519_session_logon_payload() {
        let key = PKey::generate_ed25519().expect("ed25519 key");
        let signer = BinanceWsSigner::Ed25519 {
            key,
            source: "generated-test-key".to_string(),
        };

        let payload = build_session_logon_payload(77, "api-key", &signer).expect("payload");
        let value: Value = serde_json::from_str(&payload).expect("json");

        assert_eq!(value["id"], 77);
        assert_eq!(value["method"], "session.logon");
        assert_eq!(value["params"]["apiKey"], "api-key");
        assert_eq!(value["params"]["recvWindow"], "5000");
        assert!(value["params"]["timestamp"].as_str().is_some());
        let signature = value["params"]["signature"].as_str().expect("signature");
        assert_eq!(BASE64_STANDARD.decode(signature).expect("base64").len(), 64);
    }

    #[test]
    fn ed25519_session_requests_omit_api_key_and_signature() {
        let key = PKey::generate_ed25519().expect("ed25519 key");
        let signer = BinanceWsSigner::Ed25519 {
            key,
            source: "generated-test-key".to_string(),
        };
        let params = BinanceNewOrderParams {
            symbol: "BTCUSDT".to_string(),
            side: Side::Sell,
            order_type: OrderType::Limit,
            quantity_qv: QuantizedValue::from_parts(1, -3, 300),
            price_qv: QuantizedValue::from_parts(1, -2, 12345),
            reduce_only: true,
            margin_buy: false,
            ws_response_full: false,
            ws_um_response_result: true,
            ws_margin_limit_maker: false,
        };
        let params = params.to_bytes().expect("typed params");
        let msg = trade_msg(TradeRequestType::BinanceWsNewUMOrder, 42, &params);

        let payload = build_order_payload(&msg, 99, "api-key", &signer).expect("payload");
        let value: Value = serde_json::from_str(&payload).expect("json");
        let params = value["params"].as_object().expect("params");

        assert_eq!(value["method"], "order.place");
        assert!(!params.contains_key("apiKey"));
        assert!(!params.contains_key("signature"));
        assert_eq!(value["params"]["symbol"], "BTCUSDT");
        assert_eq!(value["params"]["newClientOrderId"], "42");
    }

    #[test]
    fn builds_binance_order_payload_without_value_intermediate() {
        let params = BinanceNewOrderParams {
            symbol: "BTCUSDT".to_string(),
            side: Side::Sell,
            order_type: OrderType::Limit,
            quantity_qv: QuantizedValue::from_parts(1, -3, 300),
            price_qv: QuantizedValue::from_parts(1, -2, 12345),
            reduce_only: true,
            margin_buy: false,
            ws_response_full: false,
            ws_um_response_result: true,
            ws_margin_limit_maker: false,
        };
        let params = params.to_bytes().expect("typed params");
        let msg = trade_msg(TradeRequestType::BinanceWsNewUMOrder, 42, &params);

        let payload = build_order_payload(&msg, 99, "api-key", &signer()).expect("payload");
        let value: Value = serde_json::from_str(&payload).expect("json");

        assert_eq!(value["id"], 99);
        assert_eq!(value["method"], "order.place");
        assert_eq!(value["params"]["apiKey"], "api-key");
        assert_eq!(value["params"]["symbol"], "BTCUSDT");
        assert_eq!(value["params"]["side"], "SELL");
        assert_eq!(value["params"]["type"], "LIMIT");
        assert_eq!(value["params"]["quantity"], "0.300");
        assert_eq!(value["params"]["price"], "123.45");
        assert_eq!(value["params"]["timeInForce"], "GTX");
        assert_eq!(value["params"]["newClientOrderId"], "42");
        assert_eq!(value["params"]["newOrderRespType"], "RESULT");
        assert!(value["params"]["timestamp"].as_str().is_some());
        assert!(value["params"]["signature"]
            .as_str()
            .is_some_and(|s| !s.is_empty()));
        assert_signature_matches_sorted_params(&value);
    }

    #[test]
    fn builds_binance_cancel_payload_without_value_intermediate() {
        let params = BinanceCancelOrderParams {
            symbol: "BTCUSDT".to_string(),
            orig_client_order_id: 42,
        };
        let params = params.to_bytes().expect("typed params");
        let msg = trade_msg(TradeRequestType::BinanceWsCancelUMOrder, 43, &params);

        let payload = build_order_payload(&msg, 101, "api-key", &signer()).expect("payload");
        let value: Value = serde_json::from_str(&payload).expect("json");

        assert_eq!(value["id"], 101);
        assert_eq!(value["method"], "order.cancel");
        assert_eq!(value["params"]["apiKey"], "api-key");
        assert_eq!(value["params"]["symbol"], "BTCUSDT");
        assert_eq!(value["params"]["origClientOrderId"], "42");
        assert!(value["params"]["timestamp"].as_str().is_some());
        assert!(value["params"]["signature"]
            .as_str()
            .is_some_and(|s| !s.is_empty()));
        assert_signature_matches_sorted_params(&value);
    }

    #[test]
    fn builds_binance_query_payload_without_value_intermediate() {
        let msg = QueryRequestMsg {
            req_type: QueryRequestType::BinanceWsUMQuery,
            create_time: 0,
            client_query_id: 7,
            params: Bytes::from_static(b"symbol=BTCUSDT&origClientOrderId=42"),
        };

        let payload = build_query_payload(&msg, 100, "api-key", &signer()).expect("payload");
        let value: Value = serde_json::from_str(&payload).expect("json");

        assert_eq!(value["id"], 100);
        assert_eq!(value["method"], "order.status");
        assert_eq!(value["params"]["apiKey"], "api-key");
        assert_eq!(value["params"]["symbol"], "BTCUSDT");
        assert_eq!(value["params"]["origClientOrderId"], "42");
        assert!(value["params"]["timestamp"].as_str().is_some());
        assert!(value["params"]["signature"]
            .as_str()
            .is_some_and(|s| !s.is_empty()));
        assert_signature_matches_sorted_params(&value);
    }

    #[test]
    fn parses_binance_ws_rate_limits() {
        let payload = r#"{
            "id": 101,
            "status": 400,
            "error": {"code": -2011, "msg": "Unknown order sent."},
            "rateLimits": [
                {
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 2400,
                    "count": 17
                },
                {
                    "rateLimitType": "ORDERS",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 1200,
                    "count": 3
                }
            ]
        }"#;

        let resp = parse_ws_response(payload).expect("binance ws response");

        assert_eq!(resp.id, Some(101));
        assert_eq!(resp.status, Some(400));
        assert_eq!(resp.error_code, Some(-2011));
        assert_eq!(resp.rate_limits.len(), 2);
        assert_eq!(resp.rate_limits[0].rate_limit_type, "REQUEST_WEIGHT");
        assert_eq!(resp.rate_limits[0].limit, 2400);
        assert_eq!(resp.rate_limits[0].count, 17);
        assert_eq!(resp.rate_limits[1].rate_limit_type, "ORDERS");
        assert_eq!(resp.rate_limits[1].count, 3);
    }

    #[test]
    fn rejects_non_typed_binance_ws_order_params() {
        let msg = trade_msg(
            TradeRequestType::BinanceWsNewUMOrder,
            42,
            b"symbol=BTCUSDT&side=BUY",
        );

        let err = build_order_payload(&msg, 99, "api-key", &signer())
            .expect_err("should reject raw params");
        assert!(err
            .to_string()
            .contains("invalid typed binance ws new order params"));
    }

    #[test]
    fn rejects_unknown_binance_ws_query_param() {
        let msg = QueryRequestMsg {
            req_type: QueryRequestType::BinanceWsUMQuery,
            create_time: 0,
            client_query_id: 7,
            params: Bytes::from_static(b"symbol=BTCUSDT&origClientOrderId=42&foo=bar"),
        };

        let err = build_query_payload(&msg, 100, "api-key", &signer())
            .expect_err("should reject raw params");
        assert!(err
            .to_string()
            .contains("unsupported binance ws order.status param"));
    }
}
