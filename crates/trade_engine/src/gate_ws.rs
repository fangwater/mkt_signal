use crate::query_request::{QueryRequestMsg, QueryRequestType};
use crate::trade_request::{
    GateCancelOrderParams, GateNewOrderParams, TradeRequestMsg, TradeRequestType,
};
use account_common::gate_auth::GateCredentials;
use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha512;
use std::fmt::Write as _;

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

fn sign_ws_api(
    secret: &str,
    event: &str,
    channel: &str,
    req_param: &str,
    timestamp: i64,
) -> String {
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

pub fn build_api_payload(msg: &TradeRequestMsg, transport_id: i64) -> Result<String> {
    let channel = match msg.req_type {
        TradeRequestType::GateUnifiedNewOrder => CHANNEL_SPOT_ORDER_PLACE,
        TradeRequestType::GateUnifiedCancelOrder => CHANNEL_SPOT_ORDER_CANCEL,
        TradeRequestType::GateFuturesNewOrder => CHANNEL_FUTURES_ORDER_PLACE,
        TradeRequestType::GateFuturesCancelOrder => CHANNEL_FUTURES_ORDER_CANCEL,
        _ => return Err(anyhow!("unsupported gate request type: {:?}", msg.req_type)),
    };

    let ts_s = chrono::Utc::now().timestamp();
    let mut out = String::with_capacity(256 + msg.params.len());
    write!(out, "{{\"time\":{},\"channel\":", ts_s).expect("write gate ws time");
    push_json_string(&mut out, channel);
    out.push_str(",\"event\":");
    push_json_string(&mut out, EVENT_API);
    out.push_str(",\"payload\":{\"req_id\":\"");
    write!(out, "{}", transport_id).expect("write gate transport id");
    out.push_str("\",\"req_param\":{");

    match msg.req_type {
        TradeRequestType::GateUnifiedNewOrder => {
            let params = GateNewOrderParams::from_bytes(&msg.params).ok_or_else(|| {
                anyhow!(
                    "Gate unified WS new order requires typed params, req_type={:?}",
                    msg.req_type
                )
            })?;
            push_gate_unified_new_req_param(&mut out, &params, msg.client_order_id);
        }
        TradeRequestType::GateFuturesNewOrder => {
            let params = GateNewOrderParams::from_bytes(&msg.params).ok_or_else(|| {
                anyhow!(
                    "Gate futures WS new order requires typed params, req_type={:?}",
                    msg.req_type
                )
            })?;
            push_gate_futures_new_req_param(&mut out, &params, msg.client_order_id);
        }
        TradeRequestType::GateUnifiedCancelOrder => {
            let params = GateCancelOrderParams::from_bytes(&msg.params).ok_or_else(|| {
                anyhow!(
                    "Gate unified WS cancel order requires typed params, req_type={:?}",
                    msg.req_type
                )
            })?;
            push_gate_unified_cancel_req_param(&mut out, &params);
        }
        TradeRequestType::GateFuturesCancelOrder => {
            let params = GateCancelOrderParams::from_bytes(&msg.params).ok_or_else(|| {
                anyhow!(
                    "Gate futures WS cancel order requires typed params, req_type={:?}",
                    msg.req_type
                )
            })?;
            push_gate_futures_cancel_req_param(&mut out, &params);
        }
        _ => unreachable!("unsupported gate request type checked above"),
    }

    out.push_str("}}}");
    Ok(out)
}

fn push_gate_unified_new_req_param(
    out: &mut String,
    params: &GateNewOrderParams,
    client_order_id: i64,
) {
    let mut first = true;
    push_i64_prefixed_text_field(out, "text", "t-", client_order_id, &mut first);
    push_json_field(out, "currency_pair", &params.symbol, &mut first);
    push_json_field(
        out,
        "type",
        if params.order_type.is_limit() {
            "limit"
        } else {
            "market"
        },
        &mut first,
    );
    push_json_field(out, "account", "unified", &mut first);
    push_json_field(out, "side", params.side.as_str_lower(), &mut first);
    push_json_field(
        out,
        "amount",
        &params.quantity_qv.decimal_string(),
        &mut first,
    );
    if params.auto_borrow_repay {
        push_bool_field(out, "auto_borrow", true, &mut first);
        push_bool_field(out, "auto_repay", true, &mut first);
    }
    if params.order_type.is_limit() {
        push_json_field(out, "price", &params.price_qv.decimal_string(), &mut first);
        push_json_field(out, "time_in_force", "poc", &mut first);
    }
}

fn push_gate_futures_new_req_param(
    out: &mut String,
    params: &GateNewOrderParams,
    client_order_id: i64,
) {
    let mut first = true;
    push_i64_prefixed_text_field(out, "text", "t-", client_order_id, &mut first);
    push_json_field(out, "contract", &params.symbol, &mut first);
    push_json_field(out, "account", "unified", &mut first);
    let mut size = params.quantity_qv.decimal_string();
    if params.side.is_sell() && size != "0" {
        size.insert(0, '-');
    }
    push_json_field(out, "size", &size, &mut first);
    if params.order_type.is_limit() {
        push_json_field(out, "price", &params.price_qv.decimal_string(), &mut first);
        push_json_field(out, "tif", "poc", &mut first);
    } else {
        push_json_field(out, "price", "0", &mut first);
        push_json_field(out, "tif", "ioc", &mut first);
    }
    if params.reduce_only {
        push_bool_field(out, "reduce_only", true, &mut first);
    }
}

fn push_gate_unified_cancel_req_param(out: &mut String, params: &GateCancelOrderParams) {
    let mut first = true;
    push_json_field(out, "order_id", &params.order_id, &mut first);
    push_json_field(out, "currency_pair", &params.symbol, &mut first);
    push_json_field(out, "account", "unified", &mut first);
}

fn push_gate_futures_cancel_req_param(out: &mut String, params: &GateCancelOrderParams) {
    let mut first = true;
    push_json_field(out, "order_id", &params.order_id, &mut first);
    push_json_field(out, "contract", &params.symbol, &mut first);
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

fn push_i64_prefixed_text_field(
    out: &mut String,
    key: &str,
    prefix: &str,
    value: i64,
    first: &mut bool,
) {
    if !*first {
        out.push(',');
    }
    *first = false;
    push_json_string(out, key);
    out.push_str(":\"");
    out.push_str(prefix);
    write!(out, "{}", value).expect("write gate integer field");
    out.push('"');
}

fn push_bool_field(out: &mut String, key: &str, value: bool, first: &mut bool) {
    if !*first {
        out.push(',');
    }
    *first = false;
    push_json_string(out, key);
    out.push(':');
    out.push_str(if value { "true" } else { "false" });
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

pub fn build_query_payload(msg: &QueryRequestMsg, transport_id: i64) -> Result<String> {
    let channel = match msg.req_type {
        QueryRequestType::GateUnifiedOrderQuery => CHANNEL_SPOT_ORDER_STATUS,
        QueryRequestType::GateFuturesOrderQuery => CHANNEL_FUTURES_ORDER_STATUS,
        _ => return Err(anyhow!("unsupported gate query type: {:?}", msg.req_type)),
    };

    let req_param: Value =
        serde_json::from_slice(&msg.params).with_context(|| "invalid gate query req_param json")?;

    let ts_s = chrono::Utc::now().timestamp();
    let payload = json!({
        "time": ts_s,
        "channel": channel,
        "event": EVENT_API,
        "payload": {
            "req_id": transport_id.to_string(),
            "req_param": req_param,
        }
    });
    serde_json::to_string(&payload).with_context(|| "serialize gate ws query payload")
}

#[cfg(test)]
mod tests {
    use super::build_api_payload;
    use crate::trade_request::{
        GateCancelOrderParams, GateNewOrderParams, TradeRequestMsg, TradeRequestType,
    };
    use bytes::Bytes;
    use order_common::{OrderType, Side};
    use serde_json::{json, Value};
    use signal_common::tick_math::QuantizedValue;

    #[test]
    fn builds_gate_unified_order_payload_from_typed_params() {
        let params = GateNewOrderParams {
            symbol: "BTC_USDT".to_string(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity_qv: QuantizedValue::from_parts(1, -3, 10),
            price_qv: QuantizedValue::from_parts(1, 0, 100000),
            reduce_only: false,
            auto_borrow_repay: true,
        };
        let msg = TradeRequestMsg {
            req_type: TradeRequestType::GateUnifiedNewOrder,
            create_time: 0,
            client_order_id: 123,
            params: params.to_bytes().expect("typed params"),
            ipc_recv: None,
        };

        let payload = build_api_payload(&msg, 999).expect("payload");
        let val: Value = serde_json::from_str(&payload).expect("json");
        let req_param = &val["payload"]["req_param"];
        assert_eq!(val["channel"], json!("spot.order_place"));
        assert_eq!(val["payload"]["req_id"], json!("999"));
        assert_eq!(req_param["text"], json!("t-123"));
        assert_eq!(req_param["currency_pair"], json!("BTC_USDT"));
        assert_eq!(req_param["account"], json!("unified"));
        assert_eq!(req_param["amount"], json!("0.010"));
        assert_eq!(req_param["auto_borrow"], json!(true));
        assert_eq!(req_param["time_in_force"], json!("poc"));
    }

    #[test]
    fn builds_gate_futures_order_payload_from_typed_params() {
        let params = GateNewOrderParams {
            symbol: "BTC_USDT".to_string(),
            side: Side::Sell,
            order_type: OrderType::Market,
            quantity_qv: QuantizedValue::from_parts(1, -3, 10),
            price_qv: QuantizedValue::from_parts(1, 0, 100000),
            reduce_only: true,
            auto_borrow_repay: false,
        };
        let msg = TradeRequestMsg {
            req_type: TradeRequestType::GateFuturesNewOrder,
            create_time: 0,
            client_order_id: 123,
            params: params.to_bytes().expect("typed params"),
            ipc_recv: None,
        };

        let payload = build_api_payload(&msg, 999).expect("payload");
        let val: Value = serde_json::from_str(&payload).expect("json");
        let req_param = &val["payload"]["req_param"];
        assert_eq!(val["channel"], json!("futures.order_place"));
        assert_eq!(req_param["text"], json!("t-123"));
        assert_eq!(req_param["contract"], json!("BTC_USDT"));
        assert_eq!(req_param["size"], json!("-0.010"));
        assert_eq!(req_param["price"], json!("0"));
        assert_eq!(req_param["tif"], json!("ioc"));
        assert_eq!(req_param["reduce_only"], json!(true));
    }

    #[test]
    fn builds_gate_unified_cancel_payload_from_typed_params() {
        let params = GateCancelOrderParams {
            symbol: "BTC_USDT".to_string(),
            order_id: "abc".to_string(),
        };
        let msg = TradeRequestMsg {
            req_type: TradeRequestType::GateUnifiedCancelOrder,
            create_time: 0,
            client_order_id: 123,
            params: params.to_bytes().expect("typed params"),
            ipc_recv: None,
        };

        let payload = build_api_payload(&msg, 999).expect("payload");
        let val: Value = serde_json::from_str(&payload).expect("json");
        let req_param = &val["payload"]["req_param"];
        assert_eq!(val["channel"], json!("spot.order_cancel"));
        assert_eq!(req_param["order_id"], json!("abc"));
        assert_eq!(req_param["currency_pair"], json!("BTC_USDT"));
        assert_eq!(req_param["account"], json!("unified"));
    }

    #[test]
    fn rejects_gate_raw_json_params() {
        let msg = TradeRequestMsg {
            req_type: TradeRequestType::GateUnifiedNewOrder,
            create_time: 0,
            client_order_id: 123,
            params: Bytes::from_static(
                br#"{"currency_pair":"BTC_USDT","side":"buy","amount":"0.01"}"#,
            ),
            ipc_recv: None,
        };

        let err = build_api_payload(&msg, 999).expect_err("raw json must be rejected");
        assert!(err.to_string().contains("requires typed params"));
    }
}
