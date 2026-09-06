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
    pub has_code: bool,
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
        let has_code = val.get("code").and_then(parse_i32_value).is_some();
        let code = val
            .get("code")
            .and_then(parse_i32_value)
            .unwrap_or_default();
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
            has_code,
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

    pub fn requires_order_query(&self) -> bool {
        // These responses describe an existing order, not its current lifecycle.
        self.has_code && matches!(self.code, 401009 | 401117)
    }

    pub fn is_success(&self) -> bool {
        self.has_code
            && match self.event.as_deref() {
                Some("login") => self.code == 0,
                Some("place_order") | Some("cancel_order") => self.code == 200000,
                _ => false,
            }
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
        ["executedAvgPrice", "lastExecutedPrice", "limitPrice"]
            .iter()
            .filter_map(|field| self.data.get(*field).and_then(parse_f64_value))
            .find(|price| price.is_finite() && *price > 0.0)
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

/// A fully validated LTP private user-data push. Numeric protocol values remain
/// strings so downstream persistence does not lose precision or fee identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LtpUserData {
    Order(LtpOrderPush),
    Trade(LtpTradePush),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LtpOrderPush {
    pub portfolio_id: String,
    pub order_id: String,
    pub client_order_id: String,
    pub exchange_type: String,
    pub business_type: String,
    pub sym: String,
    pub limit_price: String,
    pub order_qty: String,
    pub quote_order_qty: String,
    pub side: String,
    pub exchange_order_type: String,
    pub time_in_force: String,
    pub executed_qty: String,
    pub executed_amount: String,
    pub executed_avg_price: String,
    pub last_executed_qty: String,
    pub last_executed_price: String,
    pub last_executed_amount: String,
    pub fee: String,
    pub fee_coin: String,
    pub rebate: String,
    pub rebate_coin: String,
    pub order_state: String,
    pub update_at_ms: i64,
    pub create_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LtpTradePush {
    pub transaction_id: String,
    pub portfolio_id: String,
    pub order_id: String,
    pub client_order_id: String,
    pub exchange_type: String,
    pub business_type: String,
    pub sym: String,
    pub side: String,
    pub quantity: String,
    pub price: String,
    pub trading_fee: String,
    pub trading_fee_coin: String,
    pub rpnl: String,
    pub create_at_ms: i64,
    pub exec_type: String,
}

pub fn parse_ltp_user_data(payload: &str) -> Result<Option<LtpUserData>> {
    let value: Value =
        serde_json::from_str(payload).with_context(|| "decode LTP user-data push")?;
    let Some(channel) = value.get("channel").and_then(Value::as_str) else {
        return Ok(None);
    };
    if !matches!(channel, "Orders" | "Trades") {
        return Ok(None);
    }
    let data = value
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("LTP {channel} push missing object data"))?;

    match channel {
        "Orders" => Ok(Some(LtpUserData::Order(LtpOrderPush {
            portfolio_id: required_protocol_string(data, "portfolioId")?,
            order_id: required_protocol_string(data, "orderId")?,
            client_order_id: required_protocol_string(data, "clientOrderId")?,
            exchange_type: required_protocol_string(data, "exchangeType")?,
            business_type: required_protocol_string(data, "businessType")?,
            sym: required_protocol_string(data, "sym")?,
            limit_price: required_optional_decimal_string(data, "limitPrice")?,
            order_qty: required_optional_decimal_string(data, "orderQty")?,
            quote_order_qty: required_decimal_string(data, "quoteOrderQty")?,
            side: required_protocol_string(data, "side")?,
            exchange_order_type: required_protocol_string(data, "exchangeOrderType")?,
            time_in_force: required_protocol_string(data, "timeInForce")?,
            executed_qty: required_decimal_string(data, "executedQty")?,
            executed_amount: required_decimal_string(data, "executedAmount")?,
            executed_avg_price: required_decimal_string(data, "executedAvgPrice")?,
            last_executed_qty: required_decimal_string(data, "lastExecutedQty")?,
            last_executed_price: required_decimal_string(data, "lastExecutedPrice")?,
            last_executed_amount: required_decimal_string(data, "lastExecutedAmount")?,
            fee: required_decimal_string(data, "fee")?,
            fee_coin: required_protocol_string(data, "feeCoin")?,
            rebate: required_decimal_string(data, "rebate")?,
            rebate_coin: required_protocol_string(data, "rebateCoin")?,
            order_state: required_protocol_string(data, "orderState")?,
            update_at_ms: required_millis_timestamp(data, "updateAt")?,
            create_at_ms: required_millis_timestamp(data, "createAt")?,
        }))),
        "Trades" => Ok(Some(LtpUserData::Trade(LtpTradePush {
            transaction_id: required_protocol_string(data, "transactionId")?,
            portfolio_id: required_protocol_string(data, "portfolioId")?,
            order_id: required_protocol_string(data, "orderId")?,
            client_order_id: required_protocol_string(data, "clientOrderId")?,
            exchange_type: required_protocol_string(data, "exchangeType")?,
            business_type: required_protocol_string(data, "businessType")?,
            sym: required_protocol_string(data, "sym")?,
            side: required_protocol_string(data, "side")?,
            quantity: required_decimal_string(data, "quantity")?,
            price: required_decimal_string(data, "price")?,
            trading_fee: required_decimal_string(data, "tradingFee")?,
            trading_fee_coin: required_protocol_string(data, "tradingFeeCoin")?,
            rpnl: required_decimal_string(data, "rpnl")?,
            create_at_ms: required_millis_timestamp(data, "createAt")?,
            exec_type: required_protocol_string(data, "execType")?,
        }))),
        _ => Ok(None),
    }
}

fn required_protocol_string(data: &serde_json::Map<String, Value>, field: &str) -> Result<String> {
    let value = data
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("LTP user-data field {field} must be a string"))?;
    Ok(value.to_string())
}

fn required_decimal_string(data: &serde_json::Map<String, Value>, field: &str) -> Result<String> {
    let value = required_protocol_string(data, field)?;
    let number = value
        .parse::<f64>()
        .with_context(|| format!("LTP user-data field {field} is not decimal"))?;
    if !number.is_finite() {
        return Err(anyhow!("LTP user-data field {field} is not finite"));
    }
    Ok(value)
}

fn required_optional_decimal_string(
    data: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<String> {
    let value = required_protocol_string(data, field)?;
    if value.is_empty() {
        return Ok(value);
    }
    required_decimal_string(data, field)
}

fn required_millis_timestamp(data: &serde_json::Map<String, Value>, field: &str) -> Result<i64> {
    let value = required_protocol_string(data, field)?;
    let timestamp = value
        .parse::<i64>()
        .with_context(|| format!("LTP user-data field {field} is not an integer timestamp"))?;
    if timestamp <= 0 {
        return Err(anyhow!("LTP user-data field {field} must be positive"));
    }
    Ok(timestamp)
}

pub fn is_text_pong(payload: &str) -> bool {
    payload.trim().eq_ignore_ascii_case("pong")
}

pub fn build_order_payload(
    logical_exchange: Exchange,
    msg: &TradeRequestMsg,
    transport_id: i64,
) -> Result<String> {
    let expected_exchange = match msg.req_type {
        TradeRequestType::BinanceNewUMOrder
        | TradeRequestType::BinanceNewMarginOrder
        | TradeRequestType::BinanceWsNewUMOrder
        | TradeRequestType::BinanceWsNewMarginOrder
        | TradeRequestType::BinanceCancelUMOrder
        | TradeRequestType::BinanceCancelMarginOrder
        | TradeRequestType::BinanceWsCancelUMOrder
        | TradeRequestType::BinanceWsCancelMarginOrder => Exchange::Binance,
        TradeRequestType::OkexNewUMOrder
        | TradeRequestType::OkexNewMarginOrder
        | TradeRequestType::OkexCancelUMOrder
        | TradeRequestType::OkexCancelMarginOrder => Exchange::Okex,
        _ => return Err(anyhow!("unsupported RapidX request type")),
    };
    if logical_exchange != expected_exchange {
        return Err(anyhow!("RapidX request does not match logical exchange"));
    }
    if msg.client_order_id <= 0 {
        return Err(anyhow!(
            "RapidX request requires a positive client order ID"
        ));
    }
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
                params: msg.params_bytes(),
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
                params: msg.params_bytes(),
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
        "ts": ltp_timestamp_us(),
        "args": args,
    }))
    .with_context(|| "serialize LTP ws payload")
}

fn ltp_timestamp_us() -> String {
    chrono::Utc::now().timestamp_micros().to_string()
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
    if order_state_failed || (!resp.is_order_push() && !resp.is_success()) {
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
    #[test]
    fn existing_order_acknowledgements_require_a_query() {
        for code in [401009, 401117] {
            let payload = serde_json::json!({"event":"place_order", "code":code});
            assert!(super::LtpWsResponse::from_json_str(&payload.to_string())
                .unwrap()
                .requires_order_query());
        }
        let response =
            super::LtpWsResponse::from_json_str(r#"{"event":"place_order","code":200000}"#)
                .unwrap();
        assert!(!response.requires_order_query());
    }
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
        let params = params.to_bytes().unwrap();
        let msg = TradeRequestMsg::create(TradeRequestType::BinanceWsNewUMOrder, 1, 123, &params)
            .expect("trade request msg");
        let payload = build_order_payload(Exchange::Binance, &msg, 9).unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["id"], "9");
        assert_eq!(value["action"], "place_order");
        let ts = value["ts"].as_str().expect("microsecond action timestamp");
        assert!(ts.parse::<i64>().unwrap() > 1_000_000_000_000_000);
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

    #[test]
    fn requires_action_specific_success_codes() {
        let login = LtpWsResponse::from_json_str(r#"{"event":"login","code":0}"#).unwrap();
        assert!(login.is_success());
        let login_action_code =
            LtpWsResponse::from_json_str(r#"{"event":"login","code":200000}"#).unwrap();
        assert!(!login_action_code.is_success());
        let action =
            LtpWsResponse::from_json_str(r#"{"event":"place_order","code":200000,"data":{}}"#)
                .unwrap();
        assert!(action.is_success());
        let missing_code =
            LtpWsResponse::from_json_str(r#"{"event":"place_order","data":{}}"#).unwrap();
        assert!(!missing_code.is_success());
    }

    #[test]
    fn parses_documented_order_and_trade_pushes_without_lossy_coercion() {
        let order = parse_ltp_user_data(
            r#"{"channel":"Orders","data":{"portfolioId":"1702884522340000","orderId":"1703213979730000","clientOrderId":"abc123","exchangeType":"BINANCE","businessType":"PERP","sym":"BINANCE_PERP_ETH_USDT","limitPrice":"2346.00000001","orderQty":"0.01","quoteOrderQty":"0","side":"BUY","exchangeOrderType":"LIMIT","timeInForce":"GTC","executedQty":"0","executedAmount":"0","executedAvgPrice":"0","lastExecutedQty":"0","lastExecutedPrice":"0","lastExecutedAmount":"0","fee":"10","feeCoin":"USDT","rebate":"0.1","rebateCoin":"USDT","orderState":"NEW","updateAt":"1703213979731","createAt":"1703213979731"}}"#,
        )
        .unwrap();
        let Some(LtpUserData::Order(order)) = order else {
            panic!("expected order push");
        };
        assert_eq!(order.order_id, "1703213979730000");
        assert_eq!(order.client_order_id, "abc123");
        assert_eq!(order.limit_price, "2346.00000001");
        assert_eq!(order.fee_coin, "USDT");
        assert_eq!(order.update_at_ms, 1_703_213_979_731);

        let trade = parse_ltp_user_data(
            r#"{"channel":"Trades","data":{"transactionId":"38132969466022978","portfolioId":"2066376093138754","orderId":"2104172237333826","exchangeType":"BINANCE","businessType":"PERP","sym":"BINANCE_PERP_APT_USDT","side":"BUY","quantity":"4","price":"2.24509925","tradingFee":"-0.00089804","tradingFeeCoin":"USDT","rpnl":"0","clientOrderId":"abc123","createAt":"1763977805203","execType":"MAKER"}}"#,
        )
        .unwrap();
        let Some(LtpUserData::Trade(trade)) = trade else {
            panic!("expected trade push");
        };
        assert_eq!(trade.transaction_id, "38132969466022978");
        assert_eq!(trade.trading_fee, "-0.00089804");
        assert_eq!(trade.trading_fee_coin, "USDT");
        assert_eq!(trade.create_at_ms, 1_763_977_805_203);
    }

    #[test]
    fn rejects_lossy_or_incomplete_user_data() {
        let err = parse_ltp_user_data(r#"{"channel":"Trades","data":{"transactionId":1}}"#)
            .expect_err("numeric protocol identity must not be coerced");
        assert!(err.to_string().contains("transactionId"));
    }
}
