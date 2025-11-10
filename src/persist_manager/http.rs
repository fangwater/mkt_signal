use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use base64::{engine::general_purpose, Engine as _};
use bytes::Bytes;
use chrono::Utc;
use log::{info, warn};
use polars::prelude::ParquetWriter;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::persist_manager::config::HttpConfig;
use crate::persist_manager::storage::RocksDbStore;
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::record::SignalRecordMessage;
use crate::signal::trade_signal::SignalType;

#[derive(Clone)]
pub struct AppState {
    store: Arc<RocksDbStore>,
}

pub async fn serve(cfg: HttpConfig, store: Arc<RocksDbStore>) -> Result<()> {
    let addr: SocketAddr = cfg
        .bind
        .parse()
        .with_context(|| format!("invalid persist_manager.http.bind {}", cfg.bind))?;

    let state = AppState { store };
    let app = Router::new()
        .route("/health", get(health))
        .route(
            "/signals/:kind",
            get(list_signals).delete(delete_signals_bulk),
        )
        .route("/signals/:kind/export", get(export_signals))
        .route(
            "/signals/:kind/:key",
            get(get_signal).delete(delete_signal_single),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("persist_manager HTTP 服务监听在 {}", addr);
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<usize>,
    start: Option<String>,
    direction: Option<String>,
    start_ts: Option<u64>,
    end_ts: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SignalDto {
    key: String,
    ts_us: u64,
    strategy_id: i32,
    signal_type: String,
    context: Option<Value>,
    payload_base64: String,
    record_ts_us: Option<i64>,
}

async fn list_signals(
    State(state): State<AppState>,
    Path(kind): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<impl IntoResponse, HttpError> {
    let signal_kind = SignalKind::from_path(&kind).ok_or(HttpError::not_found("未知的信号类型"))?;
    let limit = query.limit.unwrap_or(100).min(1000).max(1);
    let direction = query.direction.unwrap_or_else(|| "desc".to_string());
    let reverse = !matches!(direction.as_str(), "asc" | "ASC");
    let range = RangeFilter {
        start_ts: query.start_ts,
        end_ts: query.end_ts,
    };
    let start_bytes = resolve_start_key(&query.start, range.start_ts, reverse);

    let entries = state
        .store
        .scan(
            signal_kind.cf_name(),
            start_bytes.as_deref(),
            reverse,
            limit,
        )
        .map_err(|err| HttpError::internal(err.context("扫描 rocksdb 失败")))?;

    let mut result = Vec::with_capacity(entries.len());
    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes).map_err(|err| HttpError::internal(anyhow!(err)))?;
        let dto = build_dto(signal_kind, key, value_bytes)
            .map_err(|err| HttpError::internal(err.context("解析信号数据失败")))?;
        if !range.contains(dto.ts_us) {
            continue;
        }
        result.push(dto);
    }

    Ok(Json(result))
}

async fn get_signal(
    State(state): State<AppState>,
    Path((kind, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, HttpError> {
    let signal_kind = SignalKind::from_path(&kind).ok_or(HttpError::not_found("未知的信号类型"))?;
    let value = state
        .store
        .get(signal_kind.cf_name(), key.as_bytes())
        .map_err(|err| HttpError::internal(err.context("读取 rocksdb 失败")))?;
    let Some(value_bytes) = value else {
        return Err(HttpError::not_found("未找到指定的信号记录"));
    };

    let dto = build_dto(signal_kind, key, value_bytes)
        .map_err(|err| HttpError::internal(err.context("解析信号数据失败")))?;
    Ok(Json(dto))
}

#[derive(Debug, Deserialize)]
struct DeleteBulkReq {
    keys: Vec<String>,
}

async fn delete_signals_bulk(
    State(state): State<AppState>,
    Path(kind): Path<String>,
    Json(payload): Json<DeleteBulkReq>,
) -> Result<impl IntoResponse, HttpError> {
    if payload.keys.is_empty() {
        return Err(HttpError::bad_request("需要至少一个 key"));
    }
    let signal_kind = SignalKind::from_path(&kind).ok_or(HttpError::not_found("未知的信号类型"))?;

    for key in &payload.keys {
        state
            .store
            .delete(signal_kind.cf_name(), key.as_bytes())
            .map_err(|err| HttpError::internal(err.context("删除记录失败")))?;
    }

    Ok(Json(DeleteResp {
        deleted: payload.keys.len(),
    }))
}

async fn export_signals(
    State(state): State<AppState>,
    Path(kind): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<axum::response::Response, HttpError> {
    let signal_kind = SignalKind::from_path(&kind).ok_or(HttpError::not_found("未知的信号类型"))?;
    let range = RangeFilter {
        start_ts: query.start_ts,
        end_ts: query.end_ts,
    };
    let limit = query.limit.unwrap_or(1000).min(10_000).max(1);
    let direction = query.direction.unwrap_or_else(|| "desc".to_string());
    let reverse = !matches!(direction.as_str(), "asc" | "ASC");
    let start_bytes = resolve_start_key(&query.start, range.start_ts, reverse);

    let entries = state
        .store
        .scan(
            signal_kind.cf_name(),
            start_bytes.as_deref(),
            reverse,
            limit,
        )
        .map_err(|err| HttpError::internal(err.context("扫描 rocksdb 失败")))?;

    let parquet_bytes =
        build_parquet_bytes(signal_kind, entries, &range).map_err(HttpError::internal)?;

    let filename = format!("{}_{}.parquet", kind, Utc::now().format("%Y%m%d_%H%M%S"));

    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/vnd.apache.parquet")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .body(Body::from(parquet_bytes))
        .map_err(|err| HttpError::internal(anyhow!(err)))
}

async fn delete_signal_single(
    State(state): State<AppState>,
    Path((kind, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, HttpError> {
    let signal_kind = SignalKind::from_path(&kind).ok_or(HttpError::not_found("未知的信号类型"))?;
    state
        .store
        .delete(signal_kind.cf_name(), key.as_bytes())
        .map_err(|err| HttpError::internal(err.context("删除记录失败")))?;
    Ok(Json(DeleteResp { deleted: 1 }))
}

#[derive(Debug, Serialize)]
struct DeleteResp {
    deleted: usize,
}

#[derive(Debug)]
struct HttpError {
    status: StatusCode,
    message: String,
}

impl HttpError {
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(err: anyhow::Error) -> Self {
        warn!("{err:?}");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "内部错误".to_string(),
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(ErrorResp {
            message: self.message,
        });
        (self.status, body).into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorResp {
    message: String,
}

#[derive(Debug, Clone, Copy)]
enum SignalKind {
    ArbOpen,
    ArbHedge,
    ArbCancel,
    ArbClose,
}

impl SignalKind {
    fn from_path(value: &str) -> Option<Self> {
        match value {
            "signals_arb_open" => Some(Self::ArbOpen),
            "signals_arb_hedge" => Some(Self::ArbHedge),
            "signals_arb_cancel" => Some(Self::ArbCancel),
            "signals_arb_close" => Some(Self::ArbClose),
            _ => None,
        }
    }

    fn cf_name(&self) -> &'static str {
        match self {
            SignalKind::ArbOpen => "signals_arb_open",
            SignalKind::ArbHedge => "signals_arb_hedge",
            SignalKind::ArbCancel => "signals_arb_cancel",
            SignalKind::ArbClose => "signals_arb_close",
        }
    }
}

fn build_dto(signal_kind: SignalKind, key: String, value_bytes: Vec<u8>) -> Result<SignalDto> {
    let (ts_us, strategy_id) = parse_key(&key)?;
    let payload = Bytes::from(value_bytes.clone());
    let record =
        SignalRecordMessage::from_bytes(payload.clone()).context("SignalRecordMessage 解码失败")?;
    let context_json = decode_context(signal_kind, &record);
    let payload_base64 = general_purpose::STANDARD.encode(&value_bytes);

    Ok(SignalDto {
        key,
        ts_us,
        strategy_id,
        signal_type: signal_type_name(&record.signal_type).to_string(),
        context: context_json,
        payload_base64,
        record_ts_us: (record.timestamp_us > 0).then_some(record.timestamp_us),
    })
}

fn parse_key(key: &str) -> Result<(u64, i32)> {
    let mut parts = key.splitn(2, '_');
    let ts_str = parts
        .next()
        .ok_or_else(|| anyhow!("invalid key format: missing timestamp"))?;
    let strategy_str = parts
        .next()
        .ok_or_else(|| anyhow!("invalid key format: missing strategy id"))?;
    let ts_us = ts_str
        .parse::<u64>()
        .with_context(|| format!("invalid timestamp part: {}", ts_str))?;
    let strategy_id = strategy_str
        .parse::<i32>()
        .with_context(|| format!("invalid strategy id part: {}", strategy_str))?;
    Ok((ts_us, strategy_id))
}

fn decode_context(signal_kind: SignalKind, record: &SignalRecordMessage) -> Option<Value> {
    match signal_kind {
        SignalKind::ArbOpen | SignalKind::ArbClose => {
            let Ok(ctx) = ArbOpenCtx::from_bytes(Bytes::from(record.context.clone())) else {
                return None;
            };
            // Convert to JSON-friendly struct
            let json_obj = serde_json::json!({
                "opening_leg": {
                    "venue": ctx.opening_leg.venue,
                    "bid0": ctx.opening_leg.bid0,
                    "ask0": ctx.opening_leg.ask0,
                },
                "opening_symbol": ctx.get_opening_symbol(),
                "hedging_leg": {
                    "venue": ctx.hedging_leg.venue,
                    "bid0": ctx.hedging_leg.bid0,
                    "ask0": ctx.hedging_leg.ask0,
                },
                "hedging_symbol": ctx.get_hedging_symbol(),
                "amount": ctx.amount,
                "side": ctx.get_side().map(|s| s.as_str()).unwrap_or("Unknown"),
                "order_type": ctx.get_order_type().map(|t| t.as_str()).unwrap_or("Unknown"),
                "price": ctx.price,
                "price_tick": ctx.price_tick,
                "exp_time": ctx.exp_time,
                "create_ts": ctx.create_ts,
                "open_threshold": ctx.open_threshold,
                "hedge_timeout_us": ctx.hedge_timeout_us,
                "funding_ma": if ctx.funding_ma != 0.0 { Some(ctx.funding_ma) } else { None },
                "predicted_funding_rate": if ctx.predicted_funding_rate != 0.0 { Some(ctx.predicted_funding_rate) } else { None },
                "loan_rate": if ctx.loan_rate != 0.0 { Some(ctx.loan_rate) } else { None },
            });
            Some(json_obj)
        }
        SignalKind::ArbHedge => {
            let Ok(ctx) = ArbHedgeCtx::from_bytes(Bytes::from(record.context.clone())) else {
                return None;
            };
            let json_obj = serde_json::json!({
                "strategy_id": ctx.strategy_id,
                "client_order_id": ctx.client_order_id,
                "hedge_qty": ctx.hedge_qty,
                "hedge_side": ctx.get_side().map(|s| s.as_str()).unwrap_or("Unknown"),
                "limit_price": ctx.limit_price,
                "price_tick": ctx.price_tick,
                "maker_only": ctx.maker_only,
                "exp_time": ctx.exp_time,
                "hedging_leg": {
                    "venue": ctx.hedging_leg.venue,
                    "bid0": ctx.hedging_leg.bid0,
                    "ask0": ctx.hedging_leg.ask0,
                },
                "hedging_symbol": ctx.get_hedging_symbol(),
                "market_ts": ctx.market_ts,
            });
            Some(json_obj)
        }
        SignalKind::ArbCancel => {
            let Ok(ctx) = ArbCancelCtx::from_bytes(Bytes::from(record.context.clone())) else {
                return None;
            };
            let json_obj = serde_json::json!({
                "opening_leg": {
                    "venue": ctx.opening_leg.venue,
                    "bid0": ctx.opening_leg.bid0,
                    "ask0": ctx.opening_leg.ask0,
                },
                "opening_symbol": ctx.get_opening_symbol(),
                "hedging_leg": {
                    "venue": ctx.hedging_leg.venue,
                    "bid0": ctx.hedging_leg.bid0,
                    "ask0": ctx.hedging_leg.ask0,
                },
                "hedging_symbol": ctx.get_hedging_symbol(),
                "trigger_ts": ctx.trigger_ts,
            });
            Some(json_obj)
        }
    }
}

fn signal_type_name(signal_type: &SignalType) -> &'static str {
    match signal_type {
        SignalType::ArbOpen => "ArbOpen",
        SignalType::ArbHedge => "ArbHedge",
        SignalType::ArbCancel => "ArbCancel",
        SignalType::ArbClose => "ArbClose",
    }
}

#[derive(Debug, Clone, Copy)]
struct RangeFilter {
    start_ts: Option<u64>,
    end_ts: Option<u64>,
}

impl RangeFilter {
    fn contains(&self, ts: u64) -> bool {
        if let Some(start) = self.start_ts {
            if ts < start {
                return false;
            }
        }
        if let Some(end) = self.end_ts {
            if ts > end {
                return false;
            }
        }
        true
    }
}

fn resolve_start_key(
    explicit: &Option<String>,
    start_ts: Option<u64>,
    reverse: bool,
) -> Option<Vec<u8>> {
    if let Some(s) = explicit {
        return Some(s.as_bytes().to_vec());
    }
    start_ts.map(|ts| {
        let suffix = if reverse { "9999999999" } else { "0000000000" };
        format!("{:020}_{}", ts, suffix).into_bytes()
    })
}

fn build_parquet_bytes(
    kind: SignalKind,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    match kind {
        SignalKind::ArbOpen | SignalKind::ArbClose => build_parquet_open(entries, range),
        SignalKind::ArbCancel => build_parquet_cancel(entries, range),
        SignalKind::ArbHedge => build_parquet_hedge(entries, range),
    }
}

fn build_parquet_open(entries: Vec<(Vec<u8>, Vec<u8>)>, range: &RangeFilter) -> Result<Vec<u8>> {
    let mut key_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut ts_us_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut strategy_id_col: Vec<i32> = Vec::with_capacity(entries.len());
    let mut create_ts_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut opening_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut opening_symbol_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut opening_bid0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut opening_ask0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut hedging_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut hedging_symbol_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut hedging_bid0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut hedging_ask0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut amount_col: Vec<f32> = Vec::with_capacity(entries.len());
    let mut side_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut order_type_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut price_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut price_tick_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut exp_time_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut open_threshold_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut hedge_timeout_us_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut funding_ma_col: Vec<Option<f64>> = Vec::with_capacity(entries.len());
    let mut predicted_funding_rate_col: Vec<Option<f64>> = Vec::with_capacity(entries.len());
    let mut loan_rate_col: Vec<Option<f64>> = Vec::with_capacity(entries.len());

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let (ts_us, strategy_id) = parse_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }
        let record = SignalRecordMessage::from_bytes(Bytes::from(value_bytes))?;
        let ctx = ArbOpenCtx::from_bytes(Bytes::from(record.context.clone()))
            .map_err(|err| anyhow!("failed to decode ArbOpenCtx: {err}"))?;

        key_col.push(key);
        ts_us_col.push(ts_us as i64);
        strategy_id_col.push(strategy_id);
        create_ts_col.push(ctx.create_ts);
        opening_venue_col.push(
            TradingVenue::from_u8(ctx.opening_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        opening_symbol_col.push(ctx.get_opening_symbol());
        opening_bid0_col.push(ctx.opening_leg.bid0);
        opening_ask0_col.push(ctx.opening_leg.ask0);
        hedging_venue_col.push(
            TradingVenue::from_u8(ctx.hedging_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        hedging_symbol_col.push(ctx.get_hedging_symbol());
        hedging_bid0_col.push(ctx.hedging_leg.bid0);
        hedging_ask0_col.push(ctx.hedging_leg.ask0);
        amount_col.push(ctx.amount);
        side_col.push(
            ctx.get_side()
                .map(|s| s.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        order_type_col.push(
            ctx.get_order_type()
                .map(|t| t.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        price_col.push(ctx.price);
        price_tick_col.push(ctx.price_tick);
        exp_time_col.push(ctx.exp_time);
        open_threshold_col.push(ctx.open_threshold);
        hedge_timeout_us_col.push(ctx.hedge_timeout_us);
        funding_ma_col.push(if ctx.funding_ma != 0.0 {
            Some(ctx.funding_ma)
        } else {
            None
        });
        predicted_funding_rate_col.push(if ctx.predicted_funding_rate != 0.0 {
            Some(ctx.predicted_funding_rate)
        } else {
            None
        });
        loan_rate_col.push(if ctx.loan_rate != 0.0 {
            Some(ctx.loan_rate)
        } else {
            None
        });
    }

    let mut df = DataFrame::new(vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_us_col),
        Series::new("strategy_id".into(), strategy_id_col),
        Series::new("create_ts".into(), create_ts_col),
        Series::new("opening_venue".into(), opening_venue_col),
        Series::new("opening_symbol".into(), opening_symbol_col),
        Series::new("opening_bid0".into(), opening_bid0_col),
        Series::new("opening_ask0".into(), opening_ask0_col),
        Series::new("hedging_venue".into(), hedging_venue_col),
        Series::new("hedging_symbol".into(), hedging_symbol_col),
        Series::new("hedging_bid0".into(), hedging_bid0_col),
        Series::new("hedging_ask0".into(), hedging_ask0_col),
        Series::new("amount".into(), amount_col),
        Series::new("side".into(), side_col),
        Series::new("order_type".into(), order_type_col),
        Series::new("price".into(), price_col),
        Series::new("price_tick".into(), price_tick_col),
        Series::new("exp_time".into(), exp_time_col),
        Series::new("open_threshold".into(), open_threshold_col),
        Series::new("hedge_timeout_us".into(), hedge_timeout_us_col),
        Series::new("funding_ma".into(), funding_ma_col.as_slice()),
        Series::new(
            "predicted_funding_rate".into(),
            predicted_funding_rate_col.as_slice(),
        ),
        Series::new("loan_rate".into(), loan_rate_col.as_slice()),
    ])?;

    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer).finish(&mut df)?;
    Ok(buffer)
}

fn build_parquet_cancel(entries: Vec<(Vec<u8>, Vec<u8>)>, range: &RangeFilter) -> Result<Vec<u8>> {
    let mut key_col = Vec::with_capacity(entries.len());
    let mut ts_us_col = Vec::with_capacity(entries.len());
    let mut strategy_id_col = Vec::with_capacity(entries.len());
    let mut opening_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut opening_symbol_col = Vec::with_capacity(entries.len());
    let mut opening_bid0_col = Vec::with_capacity(entries.len());
    let mut opening_ask0_col = Vec::with_capacity(entries.len());
    let mut hedging_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut hedging_symbol_col = Vec::with_capacity(entries.len());
    let mut hedging_bid0_col = Vec::with_capacity(entries.len());
    let mut hedging_ask0_col = Vec::with_capacity(entries.len());
    let mut trigger_ts_col = Vec::with_capacity(entries.len());

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let (ts_us, strategy_id) = parse_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }
        let record = SignalRecordMessage::from_bytes(Bytes::from(value_bytes))?;
        let ctx = ArbCancelCtx::from_bytes(Bytes::from(record.context.clone()))
            .map_err(|err| anyhow!("failed to decode ArbCancelCtx: {err}"))?;

        key_col.push(key);
        ts_us_col.push(ts_us as i64);
        strategy_id_col.push(strategy_id);
        opening_venue_col.push(
            TradingVenue::from_u8(ctx.opening_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        opening_symbol_col.push(ctx.get_opening_symbol());
        opening_bid0_col.push(ctx.opening_leg.bid0);
        opening_ask0_col.push(ctx.opening_leg.ask0);
        hedging_venue_col.push(
            TradingVenue::from_u8(ctx.hedging_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        hedging_symbol_col.push(ctx.get_hedging_symbol());
        hedging_bid0_col.push(ctx.hedging_leg.bid0);
        hedging_ask0_col.push(ctx.hedging_leg.ask0);
        trigger_ts_col.push(ctx.trigger_ts);
    }

    let columns = vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_us_col),
        Series::new("strategy_id".into(), strategy_id_col),
        Series::new("opening_venue".into(), opening_venue_col),
        Series::new("opening_symbol".into(), opening_symbol_col),
        Series::new("opening_bid0".into(), opening_bid0_col),
        Series::new("opening_ask0".into(), opening_ask0_col),
        Series::new("hedging_venue".into(), hedging_venue_col),
        Series::new("hedging_symbol".into(), hedging_symbol_col),
        Series::new("hedging_bid0".into(), hedging_bid0_col),
        Series::new("hedging_ask0".into(), hedging_ask0_col),
        Series::new("trigger_ts".into(), trigger_ts_col),
    ];

    let mut df = DataFrame::new(columns)?;
    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;
    Ok(buf)
}

fn build_parquet_hedge(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    let mut key_col = Vec::with_capacity(entries.len());
    let mut ts_us_col = Vec::with_capacity(entries.len());
    let mut strategy_id_col = Vec::with_capacity(entries.len());
    let mut record_ts_col = Vec::with_capacity(entries.len());
    let mut ctx_strategy_id_col = Vec::with_capacity(entries.len());
    let mut client_order_id_col = Vec::with_capacity(entries.len());
    let mut hedge_qty_col = Vec::with_capacity(entries.len());
    let mut hedge_side_col = Vec::with_capacity(entries.len());
    let mut limit_price_col = Vec::with_capacity(entries.len());
    let mut price_tick_col = Vec::with_capacity(entries.len());
    let mut maker_only_col = Vec::with_capacity(entries.len());
    let mut exp_time_col = Vec::with_capacity(entries.len());
    let mut hedging_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut hedging_symbol_col = Vec::with_capacity(entries.len());
    let mut hedging_bid0_col = Vec::with_capacity(entries.len());
    let mut hedging_ask0_col = Vec::with_capacity(entries.len());
    let mut market_ts_col = Vec::with_capacity(entries.len());

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let (ts_us, strategy_id) = parse_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }
        let record = SignalRecordMessage::from_bytes(Bytes::from(value_bytes))?;
        let ctx = ArbHedgeCtx::from_bytes(Bytes::from(record.context.clone()))
            .map_err(|err| anyhow!("failed to decode ArbHedgeCtx: {err}"))?;

        key_col.push(key);
        ts_us_col.push(ts_us as i64);
        strategy_id_col.push(strategy_id);
        record_ts_col.push(record.timestamp_us);
        ctx_strategy_id_col.push(ctx.strategy_id);
        client_order_id_col.push(ctx.client_order_id);
        hedge_qty_col.push(ctx.hedge_qty);
        hedge_side_col.push(
            ctx.get_side()
                .map(|s| s.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        limit_price_col.push(ctx.limit_price);
        price_tick_col.push(ctx.price_tick);
        maker_only_col.push(ctx.maker_only);
        exp_time_col.push(ctx.exp_time);
        hedging_venue_col.push(
            TradingVenue::from_u8(ctx.hedging_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        hedging_symbol_col.push(ctx.get_hedging_symbol());
        hedging_bid0_col.push(ctx.hedging_leg.bid0);
        hedging_ask0_col.push(ctx.hedging_leg.ask0);
        market_ts_col.push(ctx.market_ts);
    }

    let columns = vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_us_col),
        Series::new("strategy_id".into(), strategy_id_col),
        Series::new("record_ts_us".into(), record_ts_col),
        Series::new("ctx_strategy_id".into(), ctx_strategy_id_col),
        Series::new("client_order_id".into(), client_order_id_col),
        Series::new("hedge_qty".into(), hedge_qty_col),
        Series::new("hedge_side".into(), hedge_side_col),
        Series::new("limit_price".into(), limit_price_col),
        Series::new("price_tick".into(), price_tick_col),
        Series::new("maker_only".into(), maker_only_col),
        Series::new("exp_time".into(), exp_time_col),
        Series::new("hedging_venue".into(), hedging_venue_col),
        Series::new("hedging_symbol".into(), hedging_symbol_col),
        Series::new("hedging_bid0".into(), hedging_bid0_col),
        Series::new("hedging_ask0".into(), hedging_ask0_col),
        Series::new("market_ts".into(), market_ts_col),
    ];

    let mut df = DataFrame::new(columns)?;
    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;
    Ok(buf)
}
