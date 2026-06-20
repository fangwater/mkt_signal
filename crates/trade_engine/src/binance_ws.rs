use crate::config::RestConstants;
use crate::query_request::{QueryRequestMsg, QueryRequestType};
use crate::trade_request::{
    BinanceCancelOrderParams, BinanceNewOrderParams, TradeRequestMsg, TradeRequestType,
};
use account_common::ApiKey;
use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use std::collections::BTreeMap;

type HmacSha256 = Hmac<Sha256>;

const METHOD_ORDER_PLACE: &str = "order.place";
const METHOD_ORDER_CANCEL: &str = "order.cancel";
const METHOD_ORDER_STATUS: &str = "order.status";

#[derive(serde::Serialize)]
struct BinanceWsPayload<'a> {
    id: i64,
    method: &'a str,
    params: &'a BTreeMap<String, String>,
}

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

fn sign_query(query: &str, secret: &str) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow!("invalid binance secret"))?;
    mac.update(query.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn sign_params(params: &BTreeMap<String, String>, secret: &str) -> Result<String> {
    let query = serialize_params(params);
    sign_query(&query, secret)
}

fn sign_ordered_params(params: &[(&str, &str)], secret: &str) -> Result<String> {
    let mut ser = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in params.iter() {
        ser.append_pair(k, v);
    }
    let query = ser.finish();
    sign_query(&query, secret)
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

fn build_signed_payload_json(
    transport_id: i64,
    method: &str,
    params: &[(&str, &str)],
    creds: &ApiKey,
) -> Result<String> {
    let signature = sign_ordered_params(params, creds.secret.trim())?;
    let id = transport_id.to_string();
    let params_bytes: usize = params.iter().map(|(k, v)| k.len() + v.len() + 6).sum();
    let mut out = String::with_capacity(
        32 + id.len() + method.len() + params_bytes + "signature".len() + signature.len(),
    );
    out.push_str("{\"id\":");
    out.push_str(&id);
    out.push_str(",\"method\":");
    push_json_string(&mut out, method);
    out.push_str(",\"params\":{");

    let mut first = true;
    for (key, value) in params.iter() {
        push_json_param(&mut out, &mut first, key, value);
    }
    push_json_param(&mut out, &mut first, "signature", &signature);
    out.push_str("}}");
    Ok(out)
}

fn current_timestamp_ms_string() -> String {
    chrono::Utc::now().timestamp_millis().to_string()
}

fn build_new_order_payload_fast(
    params: &BinanceNewOrderParams,
    req_type: TradeRequestType,
    client_order_id: i64,
    transport_id: i64,
    creds: &ApiKey,
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

    let api_key = creds.key.trim();
    let client_order_id = client_order_id.to_string();
    let quantity = params.quantity_qv.decimal_string();
    let price = params
        .order_type
        .is_limit()
        .then(|| params.price_qv.decimal_string());
    let recv_window = RestConstants::RECV_WINDOW_MS.to_string();
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

    let mut ordered = Vec::with_capacity(13);
    ordered.push(("apiKey", api_key));
    ordered.push(("newClientOrderId", client_order_id.as_str()));
    if let Some(value) = new_order_resp_type {
        ordered.push(("newOrderRespType", value));
    }
    if let Some(value) = price.as_deref() {
        ordered.push(("price", value));
    }
    ordered.push(("quantity", quantity.as_str()));
    ordered.push(("recvWindow", recv_window.as_str()));
    if !is_margin {
        ordered.push(("reduceOnly", reduce_only));
    }
    ordered.push(("side", params.side.as_str()));
    if let Some(value) = side_effect_type {
        ordered.push(("sideEffectType", value));
    }
    ordered.push(("symbol", params.symbol.as_str()));
    if let Some(value) = time_in_force {
        ordered.push(("timeInForce", value));
    }
    ordered.push(("timestamp", timestamp.as_str()));
    ordered.push(("type", order_type));

    build_signed_payload_json(transport_id, METHOD_ORDER_PLACE, &ordered, creds)
}

fn build_cancel_order_payload_fast(
    params: &BinanceCancelOrderParams,
    transport_id: i64,
    creds: &ApiKey,
) -> Result<String> {
    let api_key = creds.key.trim();
    let orig_client_order_id = params.orig_client_order_id.to_string();
    let recv_window = RestConstants::RECV_WINDOW_MS.to_string();
    let timestamp = current_timestamp_ms_string();
    let ordered = [
        ("apiKey", api_key),
        ("origClientOrderId", orig_client_order_id.as_str()),
        ("recvWindow", recv_window.as_str()),
        ("symbol", params.symbol.as_str()),
        ("timestamp", timestamp.as_str()),
    ];
    build_signed_payload_json(transport_id, METHOD_ORDER_CANCEL, &ordered, creds)
}

fn build_typed_order_payload_fast(
    msg: &TradeRequestMsg,
    transport_id: i64,
    creds: &ApiKey,
) -> Result<Option<String>> {
    match msg.req_type {
        TradeRequestType::BinanceWsNewUMOrder | TradeRequestType::BinanceWsNewMarginOrder => {
            let Some(params) = BinanceNewOrderParams::from_bytes(&msg.params) else {
                return Ok(None);
            };
            build_new_order_payload_fast(
                &params,
                msg.req_type,
                msg.client_order_id,
                transport_id,
                creds,
            )
            .map(Some)
        }
        TradeRequestType::BinanceWsCancelUMOrder | TradeRequestType::BinanceWsCancelMarginOrder => {
            let Some(params) = BinanceCancelOrderParams::from_bytes(&msg.params) else {
                return Ok(None);
            };
            build_cancel_order_payload_fast(&params, transport_id, creds).map(Some)
        }
        _ => Ok(None),
    }
}

#[derive(Default)]
struct OrderStatusParams {
    symbol: Option<String>,
    order_id: Option<String>,
    orig_client_order_id: Option<String>,
    recv_window: Option<String>,
}

fn parse_order_status_params(raw: &[u8]) -> Result<Option<OrderStatusParams>> {
    let raw_str = std::str::from_utf8(raw).with_context(|| "binance ws query params not utf8")?;
    let mut params = OrderStatusParams::default();
    let mut has_unknown = false;

    for (key, value) in url::form_urlencoded::parse(raw_str.as_bytes()) {
        match key.as_ref() {
            "symbol" => params.symbol = Some(value.into_owned()),
            "orderId" => params.order_id = Some(value.into_owned()),
            "origClientOrderId" => params.orig_client_order_id = Some(value.into_owned()),
            "recvWindow" => params.recv_window = Some(value.into_owned()),
            "apiKey" | "timestamp" | "signature" => {}
            _ => has_unknown = true,
        }
    }

    if has_unknown
        || params.symbol.is_none()
        || (params.order_id.is_none() && params.orig_client_order_id.is_none())
    {
        Ok(None)
    } else {
        Ok(Some(params))
    }
}

fn build_order_status_payload_fast(
    raw: &[u8],
    transport_id: i64,
    creds: &ApiKey,
) -> Result<Option<String>> {
    let Some(params) = parse_order_status_params(raw)? else {
        return Ok(None);
    };
    let api_key = creds.key.trim();
    let recv_window = params
        .recv_window
        .unwrap_or_else(|| RestConstants::RECV_WINDOW_MS.to_string());
    let timestamp = current_timestamp_ms_string();
    let symbol = params.symbol.expect("validated symbol");

    let mut ordered = Vec::with_capacity(6);
    ordered.push(("apiKey", api_key));
    if let Some(order_id) = params.order_id.as_deref() {
        ordered.push(("orderId", order_id));
    }
    if let Some(orig_client_order_id) = params.orig_client_order_id.as_deref() {
        ordered.push(("origClientOrderId", orig_client_order_id));
    }
    ordered.push(("recvWindow", recv_window.as_str()));
    ordered.push(("symbol", symbol.as_str()));
    ordered.push(("timestamp", timestamp.as_str()));

    build_signed_payload_json(transport_id, METHOD_ORDER_STATUS, &ordered, creds).map(Some)
}

fn binance_trade_query(msg: &TradeRequestMsg) -> Option<String> {
    match msg.req_type {
        TradeRequestType::BinanceNewUMOrder
        | TradeRequestType::BinanceNewMarginOrder
        | TradeRequestType::BinanceWsNewUMOrder
        | TradeRequestType::BinanceWsNewMarginOrder => {
            BinanceNewOrderParams::from_bytes(&msg.params)
                .map(|params| params.to_query_string(msg.req_type, msg.client_order_id))
        }
        TradeRequestType::BinanceCancelUMOrder
        | TradeRequestType::BinanceCancelMarginOrder
        | TradeRequestType::BinanceWsCancelUMOrder
        | TradeRequestType::BinanceWsCancelMarginOrder => {
            BinanceCancelOrderParams::from_bytes(&msg.params).map(|params| params.to_query_string())
        }
        _ => None,
    }
}

pub fn build_order_payload(
    msg: &TradeRequestMsg,
    transport_id: i64,
    creds: &ApiKey,
) -> Result<String> {
    let method = match msg.req_type {
        TradeRequestType::BinanceWsNewUMOrder | TradeRequestType::BinanceWsNewMarginOrder => {
            METHOD_ORDER_PLACE
        }
        TradeRequestType::BinanceWsCancelUMOrder | TradeRequestType::BinanceWsCancelMarginOrder => {
            METHOD_ORDER_CANCEL
        }
        _ => {
            return Err(anyhow!(
                "unsupported binance ws request type: {:?}",
                msg.req_type
            ))
        }
    };

    if let Some(payload) = build_typed_order_payload_fast(msg, transport_id, creds)? {
        return Ok(payload);
    }

    let query = binance_trade_query(msg);
    let params = if let Some(query) = query {
        build_signed_params(query.as_bytes(), creds)?
    } else {
        build_signed_params(&msg.params, creds)?
    };
    serde_json::to_string(&BinanceWsPayload {
        id: transport_id,
        method,
        params: &params,
    })
    .with_context(|| "serialize binance ws payload")
}

pub fn build_query_payload(
    msg: &QueryRequestMsg,
    transport_id: i64,
    creds: &ApiKey,
) -> Result<String> {
    if msg.req_type != QueryRequestType::BinanceWsUMQuery
        && msg.req_type != QueryRequestType::BinanceWsMarginQuery
    {
        return Err(anyhow!(
            "unsupported binance ws query type: {:?}",
            msg.req_type
        ));
    }

    if let Some(payload) = build_order_status_payload_fast(&msg.params, transport_id, creds)? {
        return Ok(payload);
    }

    let params = build_signed_params(&msg.params, creds)?;
    serde_json::to_string(&BinanceWsPayload {
        id: transport_id,
        method: METHOD_ORDER_STATUS,
        params: &params,
    })
    .with_context(|| "serialize binance ws query payload")
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
    use super::{build_order_payload, build_query_payload, sign_params};
    use crate::query_request::{QueryRequestMsg, QueryRequestType};
    use crate::trade_request::{
        BinanceCancelOrderParams, BinanceNewOrderParams, TradeRequestMsg, TradeRequestType,
    };
    use account_common::ApiKey;
    use bytes::Bytes;
    use order_common::{OrderType, Side};
    use serde_json::Value;
    use signal_common::tick_math::QuantizedValue;
    use std::collections::BTreeMap;

    fn creds() -> ApiKey {
        ApiKey {
            name: "test".to_string(),
            key: "api-key".to_string(),
            secret: "secret".to_string(),
        }
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
        let expected = sign_params(&sorted, creds().secret.trim()).expect("signature");
        assert_eq!(
            value["params"]["signature"].as_str().expect("signature"),
            expected
        );
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
        let msg = TradeRequestMsg {
            req_type: TradeRequestType::BinanceWsNewUMOrder,
            create_time: 0,
            client_order_id: 42,
            params: params.to_bytes().expect("typed params"),
            ipc_recv: None,
        };

        let payload = build_order_payload(&msg, 99, &creds()).expect("payload");
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
        let msg = TradeRequestMsg {
            req_type: TradeRequestType::BinanceWsCancelUMOrder,
            create_time: 0,
            client_order_id: 43,
            params: params.to_bytes().expect("typed params"),
            ipc_recv: None,
        };

        let payload = build_order_payload(&msg, 101, &creds()).expect("payload");
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

        let payload = build_query_payload(&msg, 100, &creds()).expect("payload");
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
}
