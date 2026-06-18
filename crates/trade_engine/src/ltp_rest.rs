use crate::ltp_ws::LtpCredentials;
use crate::query_parsers::compact_order::{CompactOrderQueryResp, ORDER_QUERY_NOT_FOUND_MARKER};
use crate::query_request::{QueryRequestMsg, QueryRequestType};
use crate::query_response_handle::QueryExecOutcome;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use mkt_parsers::msg::basic_account_msg::{
    BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg, BasicUmUnrealizedMsg,
};
use order_common::{OrderExecutionStatus, TimeInForce};
use runtime_common::exchange::Exchange;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::BTreeMap;

type HmacSha256 = Hmac<Sha256>;

pub const DEFAULT_REST_URL: &str = "https://api.liquiditytech.com";
const ORDER_PATH: &str = "/api/v1/trading/order";
const ASSET_PATH: &str = "/api/v1/user/asset";
const POSITION_PATH: &str = "/api/v1/trading/position";

#[derive(Debug, Clone)]
pub struct LtpRestClient {
    base_url: String,
    creds: LtpCredentials,
    http: reqwest::Client,
}

impl LtpRestClient {
    pub fn from_env() -> Result<Self> {
        let base_url = std::env::var("LTP_REST_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_REST_URL.to_string());
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            creds: LtpCredentials::from_env()?,
            http: reqwest::Client::new(),
        })
    }

    pub async fn query_order(&self, msg: &QueryRequestMsg) -> QueryLtpOrderOutcome {
        let params = match build_ltp_query_params(msg) {
            Ok(params) => params,
            Err(err) => {
                return QueryLtpOrderOutcome::Http {
                    status: 400,
                    body: Bytes::from(err.to_string()),
                }
            }
        };

        match self.signed_get(ORDER_PATH, &params).await {
            Ok((status, body)) => parse_ltp_order_query_json(status, &body),
            Err(err) => QueryLtpOrderOutcome::Http {
                status: 0,
                body: Bytes::from(err.to_string()),
            },
        }
    }

    pub async fn query_snapshot(
        &self,
        msg: &QueryRequestMsg,
        logical_exchange: Exchange,
    ) -> Vec<QueryExecOutcome> {
        let ltp_exchange = match logical_exchange {
            Exchange::Binance => "BINANCE",
            Exchange::Okex => "OKX",
            _ => {
                return vec![query_outcome(
                    msg,
                    logical_exchange,
                    503,
                    Bytes::from(format!(
                        "LTP snapshot query unsupported for exchange {}",
                        logical_exchange
                    )),
                )]
            }
        };

        match msg.req_type {
            QueryRequestType::BinanceMarginQuery
            | QueryRequestType::BinanceUMQuery
            | QueryRequestType::BinanceWsMarginQuery
            | QueryRequestType::BinanceWsUMQuery
            | QueryRequestType::OkexMarginQuery
            | QueryRequestType::OkexUMQuery => {
                let (status, body) = self.query_order(msg).await.status_body();
                vec![query_outcome(msg, logical_exchange, status, body)]
            }
            QueryRequestType::BinancePmBalanceSnapshot
            | QueryRequestType::BinanceUmBalanceSnapshotStd
            | QueryRequestType::BinanceSpotAccountSnapshotStd
            | QueryRequestType::OkexAccountBalanceSnapshot => {
                match self.signed_get(ASSET_PATH, &BTreeMap::new()).await {
                    Ok((status, body)) => parse_ltp_assets_snapshot(
                        msg,
                        logical_exchange,
                        ltp_exchange,
                        status,
                        &body,
                    ),
                    Err(err) => vec![query_outcome(
                        msg,
                        logical_exchange,
                        0,
                        Bytes::from(err.to_string()),
                    )],
                }
            }
            QueryRequestType::BinancePmUsdtFreeSnapshot
            | QueryRequestType::OkexUsdtAvailableSnapshot => {
                match self.signed_get(ASSET_PATH, &BTreeMap::new()).await {
                    Ok((status, body)) => parse_ltp_usdt_available_snapshot(
                        msg,
                        logical_exchange,
                        ltp_exchange,
                        status,
                        &body,
                    ),
                    Err(err) => vec![query_outcome(
                        msg,
                        logical_exchange,
                        0,
                        Bytes::from(err.to_string()),
                    )],
                }
            }
            QueryRequestType::BinancePmUsdtMaxBorrowable | QueryRequestType::OkexUsdtMaxLoan => {
                vec![query_outcome(
                    msg,
                    logical_exchange,
                    503,
                    Bytes::from_static(
                        b"LTP backend does not provide native max borrow/max loan semantics",
                    ),
                )]
            }
            QueryRequestType::BinanceUmAccountSnapshot
            | QueryRequestType::BinanceUmAccountSnapshotStd
            | QueryRequestType::OkexPositionsSnapshot => {
                let mut params = BTreeMap::new();
                params.insert("exchange".to_string(), ltp_exchange.to_string());
                match self.signed_get(POSITION_PATH, &params).await {
                    Ok((status, body)) => {
                        parse_ltp_positions_snapshot(msg, logical_exchange, status, &body)
                    }
                    Err(err) => vec![query_outcome(
                        msg,
                        logical_exchange,
                        0,
                        Bytes::from(err.to_string()),
                    )],
                }
            }
            _ => {
                vec![query_outcome(
                    msg,
                    logical_exchange,
                    503,
                    Bytes::from_static(b"LTP backend query type unsupported"),
                )]
            }
        }
    }

    async fn signed_get(
        &self,
        path: &str,
        params: &BTreeMap<String, String>,
    ) -> Result<(u16, String)> {
        let nonce = chrono::Utc::now().timestamp().to_string();
        let signature = sign_params(&self.creds.secret_key, params, &nonce)?;
        let query = encode_query(params);
        let url = if query.is_empty() {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}{}?{}", self.base_url, path, query)
        };
        let resp = self
            .http
            .get(&url)
            .header("Content-Type", "application/json")
            .header("X-MBX-APIKEY", self.creds.api_key.as_str())
            .header("nonce", nonce)
            .header("signature", signature)
            .send()
            .await
            .with_context(|| format!("LTP signed GET {}", url))?;
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Ok((status, body))
    }
}

#[derive(Debug, Clone)]
pub enum QueryLtpOrderOutcome {
    Compact { status: u16, body: Bytes },
    NotFound,
    Http { status: u16, body: Bytes },
}

impl QueryLtpOrderOutcome {
    pub fn status_body(self) -> (u16, Bytes) {
        match self {
            Self::Compact { status, body } => (status, body),
            Self::NotFound => (404, Bytes::from_static(ORDER_QUERY_NOT_FOUND_MARKER)),
            Self::Http { status, body } => (status, body),
        }
    }
}

fn query_outcome(
    msg: &QueryRequestMsg,
    exchange: Exchange,
    status: u16,
    body: Bytes,
) -> QueryExecOutcome {
    QueryExecOutcome {
        req_type: msg.req_type,
        client_query_id: msg.client_query_id,
        status,
        body,
        exchange,
        ip_used_weight_1m: None,
        query_count_1m: None,
    }
}

fn parse_ltp_assets_snapshot(
    msg: &QueryRequestMsg,
    logical_exchange: Exchange,
    ltp_exchange: &str,
    http_status: u16,
    body: &str,
) -> Vec<QueryExecOutcome> {
    if http_status != 200 {
        return vec![query_outcome(
            msg,
            logical_exchange,
            http_status,
            Bytes::from(body.to_string()),
        )];
    }

    match parse_ltp_asset_basic_msgs(body, ltp_exchange) {
        Ok(payloads) => payloads
            .into_iter()
            .map(|body| query_outcome(msg, logical_exchange, http_status, body))
            .collect(),
        Err(err) => vec![query_outcome(
            msg,
            logical_exchange,
            400,
            Bytes::from(format!("parse LTP asset snapshot failed: {err:#}")),
        )],
    }
}

fn parse_ltp_positions_snapshot(
    msg: &QueryRequestMsg,
    logical_exchange: Exchange,
    http_status: u16,
    body: &str,
) -> Vec<QueryExecOutcome> {
    if http_status != 200 {
        return vec![query_outcome(
            msg,
            logical_exchange,
            http_status,
            Bytes::from(body.to_string()),
        )];
    }

    match parse_ltp_position_basic_msgs(body, logical_exchange) {
        Ok(payloads) => {
            if payloads.is_empty() {
                return vec![query_outcome(
                    msg,
                    logical_exchange,
                    http_status,
                    Bytes::new(),
                )];
            }
            payloads
                .into_iter()
                .map(|body| query_outcome(msg, logical_exchange, http_status, body))
                .collect()
        }
        Err(err) => vec![query_outcome(
            msg,
            logical_exchange,
            400,
            Bytes::from(format!("parse LTP position snapshot failed: {err:#}")),
        )],
    }
}

fn parse_ltp_usdt_available_snapshot(
    msg: &QueryRequestMsg,
    logical_exchange: Exchange,
    ltp_exchange: &str,
    http_status: u16,
    body: &str,
) -> Vec<QueryExecOutcome> {
    if http_status != 200 {
        return vec![query_outcome(
            msg,
            logical_exchange,
            http_status,
            Bytes::from(body.to_string()),
        )];
    }

    match parse_ltp_usdt_available_body(body, msg.req_type, ltp_exchange) {
        Ok(body) => vec![query_outcome(msg, logical_exchange, http_status, body)],
        Err(err) => vec![query_outcome(
            msg,
            logical_exchange,
            400,
            Bytes::from(format!("parse LTP USDT available failed: {err:#}")),
        )],
    }
}

#[derive(Debug, Deserialize)]
struct LtpAssetOuter {
    #[serde(default)]
    code: i32,
    #[serde(default)]
    message: String,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Value,
}

#[derive(Debug, Deserialize)]
struct LtpAssetRow {
    #[serde(default)]
    coin: String,
    #[serde(default, rename = "exchangeType")]
    exchange_type: String,
    #[serde(default)]
    balance: String,
    #[serde(default)]
    available: String,
    #[serde(default, rename = "maxTransferable")]
    max_transferable: String,
    #[serde(default)]
    equity: String,
    #[serde(default)]
    debt: String,
    #[serde(default)]
    borrow: String,
    #[serde(default)]
    overdraw: String,
    #[serde(default, rename = "updateAt")]
    update_at: String,
}

fn parse_ltp_asset_basic_msgs(body: &str, ltp_exchange: &str) -> Result<Vec<Bytes>> {
    let outer: LtpAssetOuter =
        serde_json::from_str(body).with_context(|| "decode LTP asset response")?;
    ensure_ltp_success(outer.code, &outer.message, &outer.msg)?;

    let rows = ltp_asset_rows_from_data(outer.data, ltp_exchange)?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut out = Vec::new();
    for row in rows {
        if row.coin.trim().is_empty() {
            continue;
        }
        let ts = parse_i64(&row.update_at);
        let ts = if ts > 0 { ts } else { now_ms };
        let coin = row.coin.to_ascii_uppercase();
        let debt = first_positive_f64(&[&row.debt, &row.borrow, &row.overdraw]);
        let wallet = first_present_f64(&[&row.balance]).unwrap_or_else(|| {
            first_present_f64(&[&row.equity])
                .map(|equity| equity + debt)
                .unwrap_or(0.0)
        });
        out.push(BasicBalanceMsg::create(ts, coin.clone(), wallet).to_bytes());
        out.push(BasicBorrowInterestMsg::create(ts, coin, debt, 0.0).to_bytes());
    }
    Ok(out)
}

fn parse_ltp_usdt_available_body(
    body: &str,
    req_type: QueryRequestType,
    ltp_exchange: &str,
) -> Result<Bytes> {
    let row = parse_ltp_usdt_asset_row(body, ltp_exchange)?;
    let available = first_present_f64(&[&row.available, &row.max_transferable])
        .or_else(|| first_present_f64(&[&row.equity]))
        .unwrap_or(0.0);
    let response = match req_type {
        QueryRequestType::BinancePmUsdtFreeSnapshot => {
            json!([{
                "asset": "USDT",
                "crossMarginFree": available.to_string(),
            }])
        }
        QueryRequestType::OkexUsdtAvailableSnapshot => {
            json!({
                "code": "0",
                "data": [{
                    "details": [{
                        "ccy": "USDT",
                        "availEq": available.to_string(),
                    }]
                }],
                "msg": "",
            })
        }
        other => {
            return Err(anyhow!(
                "unsupported LTP USDT available request: {:?}",
                other
            ))
        }
    };
    serde_json::to_vec(&response)
        .map(Bytes::from)
        .with_context(|| "encode LTP USDT available compatibility body")
}

fn parse_ltp_usdt_asset_row(body: &str, ltp_exchange: &str) -> Result<LtpAssetRow> {
    let outer: LtpAssetOuter =
        serde_json::from_str(body).with_context(|| "decode LTP asset response")?;
    ensure_ltp_success(outer.code, &outer.message, &outer.msg)?;
    ltp_asset_rows_from_data(outer.data, ltp_exchange)?
        .into_iter()
        .find(|row| row.coin.eq_ignore_ascii_case("USDT"))
        .ok_or_else(|| anyhow!("LTP asset response does not contain USDT for {ltp_exchange}"))
}

fn ltp_asset_rows_from_data(data: Value, ltp_exchange: &str) -> Result<Vec<LtpAssetRow>> {
    let mut rows = Vec::new();
    match data {
        Value::Object(map) => {
            if map.contains_key("coin") {
                push_asset_rows(Value::Object(map), ltp_exchange, &mut rows)?;
            } else {
                for (_, value) in map {
                    push_asset_rows(value, ltp_exchange, &mut rows)?;
                }
            }
        }
        Value::Array(items) => push_asset_rows(Value::Array(items), ltp_exchange, &mut rows)?,
        Value::Null => {}
        other => return Err(anyhow!("unexpected LTP asset data shape: {other:?}")),
    }
    Ok(rows)
}

fn push_asset_rows(value: Value, ltp_exchange: &str, rows: &mut Vec<LtpAssetRow>) -> Result<()> {
    match value {
        Value::Array(items) => {
            for item in items {
                let row: LtpAssetRow =
                    serde_json::from_value(item).with_context(|| "decode LTP asset row")?;
                if row.exchange_type.eq_ignore_ascii_case(ltp_exchange) {
                    rows.push(row);
                }
            }
        }
        Value::Object(_) => {
            let row: LtpAssetRow =
                serde_json::from_value(value).with_context(|| "decode LTP asset row")?;
            if row.exchange_type.eq_ignore_ascii_case(ltp_exchange) {
                rows.push(row);
            }
        }
        Value::Null => {}
        other => return Err(anyhow!("unexpected LTP asset row shape: {other:?}")),
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct LtpPositionOuter {
    #[serde(default)]
    code: i32,
    #[serde(default)]
    message: String,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Vec<LtpPositionRow>,
}

#[derive(Debug, Deserialize)]
struct LtpPositionRow {
    #[serde(default)]
    sym: String,
    #[serde(default, rename = "positionSide")]
    position_side: String,
    #[serde(default, rename = "positionQty")]
    position_qty: String,
    #[serde(default, rename = "unrealizedPNL", alias = "unrealizedPnl")]
    unrealized_pnl: String,
    #[serde(default, rename = "updateAt")]
    update_at: String,
}

fn parse_ltp_position_basic_msgs(body: &str, logical_exchange: Exchange) -> Result<Vec<Bytes>> {
    let outer: LtpPositionOuter =
        serde_json::from_str(body).with_context(|| "decode LTP position response")?;
    ensure_ltp_success(outer.code, &outer.message, &outer.msg)?;

    let mut out = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();
    for row in outer.data {
        let Some(inst_id) = ltp_sym_to_internal(&row.sym, logical_exchange) else {
            continue;
        };
        let raw_qty = parse_f64(&row.position_qty) as f32;
        let raw_ts = parse_i64(&row.update_at);
        let ts = if raw_ts > 0 { raw_ts } else { now_ms };
        let side = ltp_position_side_to_char(&row.position_side);
        let amount = match side {
            'L' | 'S' => raw_qty.abs(),
            _ => raw_qty,
        };
        if amount != 0.0 {
            out.push(BasicPositionMsg::create(ts, inst_id.clone(), side, amount).to_bytes());
        }
        if let Some(pnl) = first_present_f64(&[&row.unrealized_pnl]) {
            out.push(BasicUmUnrealizedMsg::create(ts, inst_id, side, pnl).to_bytes());
        }
    }
    Ok(out)
}

fn ensure_ltp_success(code: i32, message: &str, msg: &str) -> Result<()> {
    if code == 0 || code == 200000 {
        return Ok(());
    }
    let text = if message.is_empty() { msg } else { message };
    Err(anyhow!("LTP response code={} message={}", code, text))
}

fn first_present_f64(values: &[&str]) -> Option<f64> {
    values.iter().find_map(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            trimmed.parse::<f64>().ok()
        }
    })
}

fn first_positive_f64(values: &[&str]) -> f64 {
    values
        .iter()
        .filter_map(|value| first_present_f64(&[value]))
        .map(f64::abs)
        .find(|value| *value > 0.0)
        .unwrap_or(0.0)
}

fn ltp_position_side_to_char(side: &str) -> char {
    match side.to_ascii_uppercase().as_str() {
        "LONG" => 'L',
        "SHORT" => 'S',
        "NET" | "BOTH" | "NONE" => 'N',
        _ => 'N',
    }
}

fn ltp_sym_to_internal(sym: &str, logical_exchange: Exchange) -> Option<String> {
    let upper = sym.trim().to_ascii_uppercase();
    match logical_exchange {
        Exchange::Binance => upper
            .strip_prefix("BINANCE_PERP_")
            .or_else(|| upper.strip_prefix("BINANCE_SPOT_"))
            .map(|tail| tail.replace('_', "")),
        Exchange::Okex => {
            let tail = upper
                .strip_prefix("OKX_PERP_")
                .or_else(|| upper.strip_prefix("OKX_SWAP_"));
            tail.map(|tail| {
                let mut parts = tail.split('_');
                match (parts.next(), parts.next(), parts.next()) {
                    (Some(base), Some(quote), None) => format!("{base}-{quote}-SWAP"),
                    _ => tail.replace('_', "-"),
                }
            })
        }
        _ => None,
    }
}

fn sign_params(secret: &str, params: &BTreeMap<String, String>, nonce: &str) -> Result<String> {
    let mut message = String::new();
    for (idx, (key, value)) in params.iter().enumerate() {
        if idx > 0 {
            message.push('&');
        }
        message.push_str(key);
        message.push('=');
        message.push_str(value);
    }
    message.push('&');
    message.push_str(nonce);

    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| anyhow!("invalid LTP secret"))?;
    mac.update(message.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn encode_query(params: &BTreeMap<String, String>) -> String {
    let mut ser = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        ser.append_pair(key, value);
    }
    ser.finish()
}

pub fn build_ltp_query_params(msg: &QueryRequestMsg) -> Result<BTreeMap<String, String>> {
    if !matches!(
        msg.req_type,
        QueryRequestType::BinanceMarginQuery
            | QueryRequestType::BinanceUMQuery
            | QueryRequestType::BinanceWsMarginQuery
            | QueryRequestType::BinanceWsUMQuery
            | QueryRequestType::OkexMarginQuery
            | QueryRequestType::OkexUMQuery
    ) {
        return Err(anyhow!(
            "LTP order query does not support request type {:?}",
            msg.req_type
        ));
    }

    let raw = std::str::from_utf8(&msg.params).unwrap_or("");
    let parsed: BTreeMap<String, String> = url::form_urlencoded::parse(raw.as_bytes())
        .into_owned()
        .collect();
    let mut out = BTreeMap::new();
    if let Some(order_id) = parsed
        .get("orderId")
        .or_else(|| parsed.get("ordId"))
        .filter(|v| !v.trim().is_empty() && v.trim() != "0")
    {
        out.insert("orderId".to_string(), order_id.trim().to_string());
    } else if let Some(client_order_id) = parsed
        .get("origClientOrderId")
        .or_else(|| parsed.get("clOrdId"))
        .or_else(|| parsed.get("clientOrderId"))
        .filter(|v| !v.trim().is_empty() && v.trim() != "0")
    {
        out.insert(
            "clientOrderId".to_string(),
            client_order_id.trim().to_string(),
        );
    } else if msg.client_query_id > 0 {
        out.insert("clientOrderId".to_string(), msg.client_query_id.to_string());
    } else {
        return Err(anyhow!(
            "LTP order query requires orderId or clientOrderId params"
        ));
    }
    Ok(out)
}

#[derive(Debug, Deserialize)]
struct LtpOrderOuter {
    #[serde(default)]
    code: i32,
    #[serde(default)]
    message: String,
    #[serde(default)]
    msg: String,
    data: Option<LtpOrderData>,
}

#[derive(Debug, Deserialize)]
struct LtpOrderData {
    #[serde(default, rename = "orderId")]
    order_id: String,
    #[serde(default, rename = "orderState")]
    order_state: String,
    #[serde(default, rename = "timeInForce")]
    time_in_force: String,
    #[serde(default, rename = "executedQty")]
    executed_qty: String,
    #[serde(default, rename = "executedAvgPrice")]
    executed_avg_price: String,
    #[serde(default, rename = "lastExecutedPrice")]
    last_executed_price: String,
    #[serde(default, rename = "limitPrice")]
    limit_price: String,
    #[serde(default, rename = "updateAt")]
    update_at: String,
    #[serde(default, rename = "createAt")]
    create_at: String,
}

pub fn parse_ltp_order_query_json(http_status: u16, body: &str) -> QueryLtpOrderOutcome {
    if http_status == 404 {
        return QueryLtpOrderOutcome::NotFound;
    }
    let outer: LtpOrderOuter = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return QueryLtpOrderOutcome::Http {
                status: http_status,
                body: Bytes::from(body.to_string()),
            }
        }
    };
    if outer.code != 0 && outer.code != 200000 {
        let msg = if outer.message.is_empty() {
            outer.msg
        } else {
            outer.message
        };
        let lower = msg.to_ascii_lowercase();
        if lower.contains("not found")
            || lower.contains("does not exist")
            || lower.contains("not exist")
        {
            return QueryLtpOrderOutcome::NotFound;
        }
        return QueryLtpOrderOutcome::Http {
            status: if http_status == 200 { 400 } else { http_status },
            body: Bytes::from(body.to_string()),
        };
    }
    let Some(data) = outer.data else {
        return QueryLtpOrderOutcome::NotFound;
    };
    let compact = CompactOrderQueryResp {
        executed_qty: parse_f64(&data.executed_qty),
        order_id: parse_i64(&data.order_id),
        status_u8: ltp_order_state_to_exec_status(data.order_state.as_str()),
        update_time_ms: parse_i64(&data.update_at).max(parse_i64(&data.create_at)),
        time_in_force_u8: TimeInForce::from_str(data.time_in_force.as_str())
            .unwrap_or(TimeInForce::GTC)
            .to_u8(),
        response_price: response_price(&data),
    };
    QueryLtpOrderOutcome::Compact {
        status: http_status,
        body: compact.to_bytes(),
    }
}

fn parse_f64(v: &str) -> f64 {
    v.trim().parse::<f64>().unwrap_or(0.0)
}

fn parse_i64(v: &str) -> i64 {
    v.trim().parse::<i64>().unwrap_or(0)
}

fn response_price(data: &LtpOrderData) -> f64 {
    let avg = parse_f64(&data.executed_avg_price);
    if avg > 0.0 {
        return avg;
    }
    let last = parse_f64(&data.last_executed_price);
    if last > 0.0 {
        return last;
    }
    parse_f64(&data.limit_price)
}

fn ltp_order_state_to_exec_status(state: &str) -> u8 {
    match state.to_ascii_uppercase().as_str() {
        "FILLED" => OrderExecutionStatus::Filled.to_u8(),
        "CANCELLED" | "CANCELED" | "EXPIRED" => OrderExecutionStatus::Cancelled.to_u8(),
        "REJECT" | "REJECTED" | "FAIL" => OrderExecutionStatus::Rejected.to_u8(),
        "NEW" | "OPEN" | "PARTIALLY_FILLED" => OrderExecutionStatus::Create.to_u8(),
        _ => OrderExecutionStatus::Create.to_u8(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_request::GenericQueryRequest;
    use mkt_parsers::msg::basic_account_msg::{
        BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg, BasicUmUnrealizedMsg,
    };
    use runtime_common::time_util::get_timestamp_us;

    #[test]
    fn builds_ltp_query_params_from_binance_client_id() {
        let req = GenericQueryRequest::create(
            QueryRequestType::BinanceWsUMQuery,
            get_timestamp_us(),
            42,
            Bytes::from("symbol=BTCUSDT&origClientOrderId=123"),
        );
        let msg = QueryRequestMsg::parse(req.to_bytes().as_ref()).unwrap();
        let params = build_ltp_query_params(&msg).unwrap();
        assert_eq!(params.get("clientOrderId").map(String::as_str), Some("123"));
    }

    #[test]
    fn builds_ltp_query_params_from_okex_order_id() {
        let req = GenericQueryRequest::create(
            QueryRequestType::OkexUMQuery,
            get_timestamp_us(),
            42,
            Bytes::from("instId=BTC-USDT-SWAP&ordId=999"),
        );
        let msg = QueryRequestMsg::parse(req.to_bytes().as_ref()).unwrap();
        let params = build_ltp_query_params(&msg).unwrap();
        assert_eq!(params.get("orderId").map(String::as_str), Some("999"));
    }

    #[test]
    fn parses_ltp_order_query_success() {
        let body = r#"{"code":200000,"message":"Success","data":{"orderId":"2106591138643265","clientOrderId":"myorder001","orderState":"FILLED","sym":"BINANCE_SPOT_ETH_USDT","timeInForce":"GTC","executedQty":"0.5","executedAvgPrice":"3200","lastExecutedPrice":"3200","limitPrice":"3200","updateAt":"1744466410000"}}"#;
        let parsed = parse_ltp_order_query_json(200, body);
        match parsed {
            QueryLtpOrderOutcome::Compact { status, body } => {
                let compact = CompactOrderQueryResp::from_bytes_prefix(&body).unwrap();
                assert_eq!(status, 200);
                assert_eq!(compact.order_id, 2106591138643265);
                assert_eq!(compact.executed_qty, 0.5);
                assert_eq!(compact.status_u8, OrderExecutionStatus::Filled.to_u8());
                assert_eq!(compact.response_price, 3200.0);
            }
            other => panic!("unexpected parse result: {:?}", other),
        }
    }

    #[test]
    fn parses_ltp_reject_as_rejected() {
        let body = r#"{"code":200000,"message":"Success","data":{"orderId":"1","orderState":"REJECT","timeInForce":"GTC","executedQty":"0","executedAvgPrice":"0","limitPrice":"0.1","updateAt":"1769068022495"}}"#;
        let parsed = parse_ltp_order_query_json(200, body);
        match parsed {
            QueryLtpOrderOutcome::Compact { body, .. } => {
                let compact = CompactOrderQueryResp::from_bytes_prefix(&body).unwrap();
                assert_eq!(compact.status_u8, OrderExecutionStatus::Rejected.to_u8());
                assert_eq!(compact.response_price, 0.1);
            }
            other => panic!("unexpected parse result: {:?}", other),
        }
    }

    #[test]
    fn parses_ltp_assets_to_basic_balance_and_borrow() {
        let body = r#"{
            "code":200000,
            "message":"Success",
            "data":{
                "portfolio-a":[{
                    "coin":"USDT",
                    "exchangeType":"BINANCE",
                    "balance":"100.5",
                    "equity":"90.5",
                    "debt":"10",
                    "available":"80",
                    "updateAt":"1769068022495"
                },{
                    "coin":"BTC",
                    "exchangeType":"OKX",
                    "balance":"1",
                    "debt":"0",
                    "updateAt":"1769068022495"
                }]
            }
        }"#;

        let msgs = parse_ltp_asset_basic_msgs(body, "BINANCE").expect("parse assets");
        assert_eq!(msgs.len(), 2);

        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("balance");
        assert_eq!(bal.timestamp, 1769068022495);
        assert_eq!(bal.symbol, "USDT");
        assert!((bal.wallet - 100.5).abs() < 1e-12);

        let borrow = BasicBorrowInterestMsg::from_bytes(&msgs[1]).expect("borrow");
        assert_eq!(borrow.symbol, "USDT");
        assert!((borrow.borrowed - 10.0).abs() < 1e-12);
        assert_eq!(borrow.interest, 0.0);
    }

    #[test]
    fn parses_ltp_assets_falls_back_to_equity_plus_debt() {
        let body = r#"{
            "code":200000,
            "data":[{
                "coin":"ETH",
                "exchangeType":"OKX",
                "equity":"5",
                "borrow":"2",
                "updateAt":"1769068022495"
            }]
        }"#;

        let msgs = parse_ltp_asset_basic_msgs(body, "OKX").expect("parse assets");
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("balance");
        assert_eq!(bal.symbol, "ETH");
        assert!((bal.wallet - 7.0).abs() < 1e-12);
    }

    #[test]
    fn parses_ltp_binance_positions_to_basic_msgs() {
        let body = r#"{
            "code":200000,
            "message":"Success",
            "data":[{
                "sym":"BINANCE_PERP_ETH_USDT",
                "positionSide":"LONG",
                "positionQty":"0.25",
                "unrealizedPNL":"12.5",
                "updateAt":"1769068022495"
            }]
        }"#;

        let msgs = parse_ltp_position_basic_msgs(body, Exchange::Binance).expect("parse positions");
        assert_eq!(msgs.len(), 2);

        let pos = BasicPositionMsg::from_bytes(&msgs[0]).expect("position");
        assert_eq!(pos.inst_id, "ETHUSDT");
        assert_eq!(pos.position_side, 'L');
        assert!((pos.position_amount - 0.25).abs() < 1e-6);

        let pnl = BasicUmUnrealizedMsg::from_bytes(&msgs[1]).expect("pnl");
        assert_eq!(pnl.inst_id, "ETHUSDT");
        assert_eq!(pnl.position_side, 'L');
        assert!((pnl.unrealized_pnl - 12.5).abs() < 1e-12);
    }

    #[test]
    fn parses_ltp_okx_positions_to_swap_inst_id() {
        let body = r#"{
            "code":200000,
            "data":[{
                "sym":"OKX_PERP_BTC_USDT",
                "positionSide":"NET",
                "positionQty":"-2",
                "unrealizedPNL":"-1.5",
                "updateAt":"1769068022495"
            }]
        }"#;

        let msgs = parse_ltp_position_basic_msgs(body, Exchange::Okex).expect("parse positions");
        let pos = BasicPositionMsg::from_bytes(&msgs[0]).expect("position");
        assert_eq!(pos.inst_id, "BTC-USDT-SWAP");
        assert_eq!(pos.position_side, 'N');
        assert!((pos.position_amount + 2.0).abs() < 1e-6);
    }

    #[test]
    fn binance_standard_snapshot_types_use_generic_ltp_basic_mapping() {
        let asset_req = GenericQueryRequest::create(
            QueryRequestType::BinanceUmBalanceSnapshotStd,
            get_timestamp_us(),
            43,
            Bytes::new(),
        );
        let asset_msg = QueryRequestMsg::parse(asset_req.to_bytes().as_ref()).unwrap();
        let asset_body = r#"{
            "code":200000,
            "data":[{
                "coin":"USDT",
                "exchangeType":"BINANCE",
                "balance":"100",
                "debt":"0",
                "updateAt":"1769068022495"
            }]
        }"#;
        let asset_out =
            parse_ltp_assets_snapshot(&asset_msg, Exchange::Binance, "BINANCE", 200, asset_body);
        assert_eq!(asset_out.len(), 2);
        assert_eq!(asset_out[0].status, 200);
        assert_eq!(
            asset_out[0].req_type,
            QueryRequestType::BinanceUmBalanceSnapshotStd
        );
        let bal = BasicBalanceMsg::from_bytes(&asset_out[0].body).expect("balance");
        assert_eq!(bal.symbol, "USDT");

        let pos_req = GenericQueryRequest::create(
            QueryRequestType::BinanceUmAccountSnapshotStd,
            get_timestamp_us(),
            44,
            Bytes::new(),
        );
        let pos_msg = QueryRequestMsg::parse(pos_req.to_bytes().as_ref()).unwrap();
        let pos_body = r#"{
            "code":200000,
            "data":[{
                "sym":"BINANCE_PERP_BTC_USDT",
                "positionSide":"LONG",
                "positionQty":"1",
                "unrealizedPNL":"0",
                "updateAt":"1769068022495"
            }]
        }"#;
        let pos_out = parse_ltp_positions_snapshot(&pos_msg, Exchange::Binance, 200, pos_body);
        assert_eq!(
            pos_out[0].req_type,
            QueryRequestType::BinanceUmAccountSnapshotStd
        );
        let pos = BasicPositionMsg::from_bytes(&pos_out[0].body).expect("position");
        assert_eq!(pos.inst_id, "BTCUSDT");
    }
}
