use crate::okex::{OkexCancelOrderRequest, OkexNewOrderParams, OkexNewOrderRequest};
use crate::trade_request::{
    BinanceCancelOrderParams, BinanceNewOrderParams, TradeRequestHeader, TradeRequestMsg,
    TradeRequestType,
};
use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use log::warn;
use order_common::{OrderStatus, OrderType};
use runtime_common::exchange::Exchange;
use serde_json::{json, Value};
use sha2::Sha256;
use symbol_utils::symbol_util::{extract_assets_from_symbol, normalize_symbol_for_internal};

type HmacSha256 = Hmac<Sha256>;

pub const DEFAULT_WS_URL: &str = "wss://wss.liquiditytech.com/v1/private";

#[derive(Debug, Clone)]
pub struct LtpCredentials {
    pub api_key: String,
    pub secret_key: String,
}

impl LtpCredentials {
    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("LTP_API_KEY")
            .map_err(|_| anyhow!("LTP_API_KEY not set"))?
            .trim()
            .to_string();
        let secret_key = std::env::var("LTP_API_SECRET")
            .map_err(|_| anyhow!("LTP_API_SECRET not set"))?
            .trim()
            .to_string();
        if api_key.is_empty() {
            return Err(anyhow!("LTP_API_KEY is empty"));
        }
        if secret_key.is_empty() {
            return Err(anyhow!("LTP_API_SECRET is empty"));
        }
        Ok(Self {
            api_key,
            secret_key,
        })
    }

    pub fn build_login_payload(&self, only_trade: bool) -> Result<String> {
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let message = format!("{timestamp}GET/users/self/verify");
        let mut mac = HmacSha256::new_from_slice(self.secret_key.as_bytes())
            .map_err(|_| anyhow!("invalid LTP secret"))?;
        mac.update(message.as_bytes());
        let sign = hex::encode(mac.finalize().into_bytes());

        let mut args = json!({
            "apiKey": self.api_key,
            "timestamp": timestamp,
            "sign": sign,
        });
        if only_trade {
            if let Some(obj) = args.as_object_mut() {
                obj.insert("onlyTrade".to_string(), json!(true));
            }
        }

        serde_json::to_string(&json!({
            "action": "login",
            "args": args,
        }))
        .with_context(|| "serialize LTP login payload")
    }
}

#[derive(Debug, Clone)]
pub struct LtpWsResponse {
    pub id: Option<i64>,
    pub event: Option<String>,
    pub code: i32,
    pub msg: String,
    pub data: Value,
    pub channel: Option<String>,
    pub inst_id: Option<String>,
}

impl LtpWsResponse {
    pub fn from_json_str(payload: &str) -> Option<Self> {
        let val: Value = serde_json::from_str(payload).ok()?;
        let channel = val
            .get("channel")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let inst_id = val
            .get("instId")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let id = val.get("id").and_then(parse_i64_value);
        let event = val
            .get("event")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let code = val.get("code").and_then(parse_i32_value).unwrap_or(0);
        let msg = val
            .get("msg")
            .or_else(|| val.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let data = val.get("data").cloned().unwrap_or(Value::Null);
        Some(Self {
            id,
            event,
            code,
            msg,
            data,
            channel,
            inst_id,
        })
    }

    pub fn is_login(&self) -> bool {
        self.event.as_deref() == Some("login")
    }

    pub fn is_trade_ack(&self) -> bool {
        matches!(
            self.event.as_deref(),
            Some("place_order") | Some("cancel_order")
        )
    }

    pub fn is_success(&self) -> bool {
        self.code == 0 || self.code == 200000
    }

    pub fn is_order_push(&self) -> bool {
        self.channel.as_deref() == Some("Orders") && self.data.is_object()
    }

    pub fn order_id_i64(&self) -> i64 {
        self.data
            .get("orderId")
            .and_then(parse_i64_value)
            .unwrap_or(0)
    }

    pub fn client_order_id_i64(&self) -> Option<i64> {
        self.data.get("clientOrderId").and_then(parse_i64_value)
    }

    pub fn order_update_time_ms(&self) -> i64 {
        self.data
            .get("updateAt")
            .or_else(|| self.data.get("createAt"))
            .and_then(parse_i64_value)
            .unwrap_or(0)
    }

    pub fn executed_qty(&self) -> f64 {
        self.data
            .get("executedQty")
            .and_then(parse_f64_value)
            .unwrap_or(0.0)
    }

    pub fn response_price(&self) -> f64 {
        self.data
            .get("lastExecutedPrice")
            .or_else(|| self.data.get("executedAvgPrice"))
            .or_else(|| self.data.get("limitPrice"))
            .and_then(parse_f64_value)
            .unwrap_or(0.0)
    }

    pub fn order_status_u8(&self) -> u8 {
        let Some(raw) = self.data.get("orderState").and_then(|v| v.as_str()) else {
            return 0;
        };
        match raw.to_ascii_uppercase().as_str() {
            "OPEN" => OrderStatus::New.to_u8(),
            "FAIL" | "REJECT" => 0,
            other => OrderStatus::from_str(other)
                .map(OrderStatus::to_u8)
                .unwrap_or(0),
        }
    }

    pub fn error_code_for_trade_response(&self) -> i32 {
        if self.is_success() {
            0
        } else {
            self.code
        }
    }
}

pub fn is_text_pong(payload: &str) -> bool {
    payload.trim().eq_ignore_ascii_case("pong")
}

pub fn build_order_payload(
    logical_exchange: Exchange,
    msg: &TradeRequestMsg,
    transport_id: i64,
) -> Result<String> {
    let (action, args) = match msg.req_type {
        TradeRequestType::BinanceNewUMOrder
        | TradeRequestType::BinanceNewMarginOrder
        | TradeRequestType::BinanceWsNewUMOrder
        | TradeRequestType::BinanceWsNewMarginOrder => {
            let params = BinanceNewOrderParams::from_bytes(&msg.params)
                .ok_or_else(|| anyhow!("decode binance new order params failed"))?;
            (
                "place_order",
                build_ltp_new_order_args_from_binance(logical_exchange, msg.req_type, msg, params)?,
            )
        }
        TradeRequestType::BinanceCancelUMOrder
        | TradeRequestType::BinanceCancelMarginOrder
        | TradeRequestType::BinanceWsCancelUMOrder
        | TradeRequestType::BinanceWsCancelMarginOrder => {
            let params = BinanceCancelOrderParams::from_bytes(&msg.params)
                .ok_or_else(|| anyhow!("decode binance cancel order params failed"))?;
            (
                "cancel_order",
                build_ltp_cancel_args_from_binance(msg.client_order_id, params),
            )
        }
        TradeRequestType::OkexNewMarginOrder | TradeRequestType::OkexNewUMOrder => {
            let params = OkexNewOrderRequest {
                header: header_for_msg(msg),
                params: msg.params.clone(),
            }
            .params_struct()
            .ok_or_else(|| anyhow!("decode okex new order params failed"))?;
            (
                "place_order",
                build_ltp_new_order_args_from_okex(msg.req_type, params)?,
            )
        }
        TradeRequestType::OkexCancelMarginOrder | TradeRequestType::OkexCancelUMOrder => {
            let params = OkexCancelOrderRequest {
                header: header_for_msg(msg),
                params: msg.params.clone(),
            }
            .params_struct()
            .ok_or_else(|| anyhow!("decode okex cancel order params failed"))?;
            ("cancel_order", build_ltp_cancel_args_from_okex(msg, params))
        }
        _ => {
            return Err(anyhow!(
                "unsupported LTP ws request type: {:?}",
                msg.req_type
            ))
        }
    };

    serde_json::to_string(&json!({
        "id": transport_id.to_string(),
        "action": action,
        "args": args,
    }))
    .with_context(|| "serialize LTP ws payload")
}

fn header_for_msg(msg: &TradeRequestMsg) -> TradeRequestHeader {
    TradeRequestHeader {
        msg_type: msg.req_type as u32,
        params_length: msg.params.len() as u32,
        create_time: msg.create_time,
        client_order_id: msg.client_order_id,
    }
}

fn build_ltp_new_order_args_from_binance(
    logical_exchange: Exchange,
    req_type: TradeRequestType,
    msg: &TradeRequestMsg,
    params: BinanceNewOrderParams,
) -> Result<Value> {
    let business = match req_type {
        TradeRequestType::BinanceNewMarginOrder | TradeRequestType::BinanceWsNewMarginOrder => {
            "SPOT"
        }
        TradeRequestType::BinanceNewUMOrder | TradeRequestType::BinanceWsNewUMOrder => "PERP",
        _ => return Err(anyhow!("unsupported binance LTP request: {:?}", req_type)),
    };
    let exchange = match logical_exchange {
        Exchange::Binance => "BINANCE",
        Exchange::Okex => "OKX",
        other => return Err(anyhow!("LTP backend does not support exchange {}", other)),
    };
    let sym = ltp_sym(exchange, business, &params.symbol);
    let mut args = json!({
        "clientOrderId": msg.client_order_id.to_string(),
        "sym": sym,
        "side": params.side.as_str(),
        "orderType": ltp_order_type(params.order_type)?,
    });
    fill_ltp_order_common_args(
        &mut args,
        params.order_type,
        params.quantity_qv.decimal_string(),
        params.price_qv.decimal_string(),
        params.reduce_only,
        if params.order_type.is_limit() {
            if matches!(
                req_type,
                TradeRequestType::BinanceNewUMOrder | TradeRequestType::BinanceWsNewUMOrder
            ) {
                Some("GTX")
            } else {
                Some("GTC")
            }
        } else {
            None
        },
    );
    Ok(args)
}

fn build_ltp_new_order_args_from_okex(
    req_type: TradeRequestType,
    params: OkexNewOrderParams,
) -> Result<Value> {
    let business = match req_type {
        TradeRequestType::OkexNewMarginOrder => "SPOT",
        TradeRequestType::OkexNewUMOrder => "PERP",
        _ => return Err(anyhow!("unsupported okex LTP request: {:?}", req_type)),
    };
    let sym = ltp_sym("OKX", business, &params.symbol);
    let order_type = match params.order_type {
        crate::okex::OkexOrderType::Market => OrderType::Market,
        _ => OrderType::Limit,
    };
    let tif = match params.order_type {
        crate::okex::OkexOrderType::Ioc => Some("IOC"),
        crate::okex::OkexOrderType::PostOnly | crate::okex::OkexOrderType::MmpAndPostOnly => {
            Some("GTX")
        }
        crate::okex::OkexOrderType::Fok => Some("FOK"),
        crate::okex::OkexOrderType::Market => None,
        _ => Some("GTC"),
    };
    let mut args = json!({
        "clientOrderId": params.client_order_id.to_string(),
        "sym": sym,
        "side": params.side.as_str(),
        "orderType": ltp_order_type(order_type)?,
    });
    fill_ltp_order_common_args(
        &mut args,
        order_type,
        params.quantity_qv.decimal_string(),
        params.price_qv.decimal_string(),
        params.reduce_only,
        tif,
    );
    Ok(args)
}

fn fill_ltp_order_common_args(
    args: &mut Value,
    order_type: OrderType,
    quantity: String,
    price: String,
    reduce_only: bool,
    tif: Option<&str>,
) {
    let Some(obj) = args.as_object_mut() else {
        return;
    };
    if !quantity.is_empty() {
        obj.insert("orderQty".to_string(), json!(quantity));
    }
    if order_type.is_limit() {
        obj.insert("limitPrice".to_string(), json!(price));
        obj.insert("timeInForce".to_string(), json!(tif.unwrap_or("GTC")));
    }
    if reduce_only {
        obj.insert("reduceOnly".to_string(), json!("true"));
    }
}

fn build_ltp_cancel_args_from_binance(
    fallback_client_order_id: i64,
    params: BinanceCancelOrderParams,
) -> Value {
    let client_order_id = if params.orig_client_order_id > 0 {
        params.orig_client_order_id
    } else {
        fallback_client_order_id
    };
    json!({
        "clientOrderId": client_order_id.to_string(),
    })
}

fn build_ltp_cancel_args_from_okex(
    msg: &TradeRequestMsg,
    params: crate::okex::OkexCancelOrderParams,
) -> Value {
    if params.cl_ord_id > 0 {
        json!({
            "clientOrderId": params.cl_ord_id.to_string(),
        })
    } else if params.ord_id > 0 {
        json!({
            "orderId": params.ord_id.to_string(),
        })
    } else {
        json!({
            "clientOrderId": msg.client_order_id.to_string(),
        })
    }
}

fn ltp_order_type(order_type: OrderType) -> Result<&'static str> {
    if order_type.is_limit() {
        Ok("LIMIT")
    } else if order_type.is_market() {
        Ok("MARKET")
    } else {
        Err(anyhow!("LTP backend only supports limit/market orders"))
    }
}

fn ltp_sym(exchange: &str, business: &str, symbol: &str) -> String {
    let (base, quote) = extract_assets_from_symbol(&normalize_symbol_for_internal(symbol));
    format!("{exchange}_{business}_{base}_{quote}")
}

fn parse_i64_value(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return i64::try_from(n).ok();
    }
    v.as_str()?.trim().parse::<i64>().ok()
}

fn parse_i32_value(v: &Value) -> Option<i32> {
    if let Some(n) = v.as_i64() {
        return i32::try_from(n).ok();
    }
    if let Some(n) = v.as_u64() {
        return i32::try_from(n).ok();
    }
    v.as_str()?.trim().parse::<i32>().ok()
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
    v.as_str()?.trim().parse::<f64>().ok()
}

pub fn ltp_status_for_response(resp: &LtpWsResponse) -> u16 {
    let order_state_failed = resp
        .data
        .get("orderState")
        .and_then(|v| v.as_str())
        .map(|s| matches!(s.to_ascii_uppercase().as_str(), "FAIL" | "REJECT"))
        .unwrap_or(false);
    if !resp.is_success() || order_state_failed {
        400
    } else {
        206
    }
}

pub fn warn_if_unsupported_ltp_exchange(exchange: Exchange) -> Result<()> {
    if matches!(exchange, Exchange::Binance | Exchange::Okex) {
        Ok(())
    } else {
        warn!(
            "LTP backend requested for {}, but only binance/okex are supported by current mapping",
            exchange
        );
        Err(anyhow!(
            "LTP backend currently supports logical binance/okex only, got {}",
            exchange
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_common::tick_math::QuantizedValue;

    #[test]
    fn builds_login_signature_shape() {
        let creds = LtpCredentials {
            api_key: "key".to_string(),
            secret_key: "secret".to_string(),
        };
        let payload = creds.build_login_payload(true).unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["action"], "login");
        assert_eq!(value["args"]["apiKey"], "key");
        assert_eq!(value["args"]["onlyTrade"], true);
        assert!(value["args"]["sign"].as_str().unwrap().len() == 64);
    }

    #[test]
    fn builds_binance_perp_ltp_order() {
        let params = BinanceNewOrderParams {
            symbol: "BTCUSDT".to_string(),
            side: order_common::Side::Buy,
            order_type: OrderType::Limit,
            quantity_qv: QuantizedValue::from_decimal(0.01).unwrap(),
            price_qv: QuantizedValue::from_decimal(60000.0).unwrap(),
            reduce_only: true,
            margin_buy: false,
            ws_response_full: false,
            ws_um_response_result: true,
            ws_margin_limit_maker: false,
        };
        let msg = TradeRequestMsg {
            req_type: TradeRequestType::BinanceWsNewUMOrder,
            create_time: 1,
            client_order_id: 123,
            params: params.to_bytes().unwrap(),
            ipc_recv: None,
        };
        let payload = build_order_payload(Exchange::Binance, &msg, 9).unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["id"], "9");
        assert_eq!(value["action"], "place_order");
        assert_eq!(value["args"]["clientOrderId"], "123");
        assert_eq!(value["args"]["sym"], "BINANCE_PERP_BTC_USDT");
        assert_eq!(value["args"]["timeInForce"], "GTX");
        assert_eq!(value["args"]["reduceOnly"], "true");
    }

    #[test]
    fn parses_order_push_status() {
        let payload = r#"{"channel":"Orders","instId":"BINANCE_PERP_ETH_USDT","data":{"orderId":"1703213979730000","clientOrderId":"123","orderState":"FILLED","executedQty":"0.01","lastExecutedPrice":"2346","updateAt":"1703213979731"}}"#;
        let resp = LtpWsResponse::from_json_str(payload).unwrap();
        assert!(resp.is_order_push());
        assert_eq!(resp.client_order_id_i64(), Some(123));
        assert_eq!(resp.order_status_u8(), OrderStatus::Filled.to_u8());
        assert_eq!(resp.order_update_time_ms(), 1703213979731);
    }
}
