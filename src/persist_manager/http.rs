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
use crate::signal::binance_forward_arb_mt::{
    BinSingleForwardArbCancelCtx, BinSingleForwardArbOpenCtx,
};
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
    BinSingleForwardArbOpenMT,
    BinSingleForwardArbOpenMM,
    BinSingleForwardArbCancelMT,
    BinSingleForwardArbCancelMM,
}

impl SignalKind {
    fn from_path(value: &str) -> Option<Self> {
        match value {
            "signals_bin_single_forward_arb_open_mt" => Some(Self::BinSingleForwardArbOpenMT),
            "signals_bin_single_forward_arb_open_mm" => Some(Self::BinSingleForwardArbOpenMM),
            "signals_bin_single_forward_arb_cancel_mt" => Some(Self::BinSingleForwardArbCancelMT),
            "signals_bin_single_forward_arb_cancel_mm" => Some(Self::BinSingleForwardArbCancelMM),
            _ => None,
        }
    }

    fn cf_name(&self) -> &'static str {
        match self {
            SignalKind::BinSingleForwardArbOpenMT => "signals_bin_single_forward_arb_open_mt",
            SignalKind::BinSingleForwardArbOpenMM => "signals_bin_single_forward_arb_open_mm",
            SignalKind::BinSingleForwardArbCancelMT => "signals_bin_single_forward_arb_cancel_mt",
            SignalKind::BinSingleForwardArbCancelMM => "signals_bin_single_forward_arb_cancel_mm",
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
        SignalKind::BinSingleForwardArbOpenMT | SignalKind::BinSingleForwardArbOpenMM => {
            let Ok(ctx) =
                BinSingleForwardArbOpenCtx::from_bytes(Bytes::from(record.context.clone()))
            else {
                return None;
            };
            serde_json::to_value(ctx).ok()
        }
        SignalKind::BinSingleForwardArbCancelMT | SignalKind::BinSingleForwardArbCancelMM => {
            let Ok(ctx) =
                BinSingleForwardArbCancelCtx::from_bytes(Bytes::from(record.context.clone()))
            else {
                return None;
            };
            serde_json::to_value(ctx).ok()
        }
    }
}

fn signal_type_name(signal_type: &SignalType) -> &'static str {
    match signal_type {
        SignalType::BinSingleForwardArbOpenMT => "BinSingleForwardArbOpenMT",
        SignalType::BinSingleForwardArbOpenMM => "BinSingleForwardArbOpenMM",
        SignalType::BinSingleForwardArbHedge => "BinSingleForwardArbHedge",
        SignalType::BinSingleForwardArbCloseMargin => "BinSingleForwardArbCloseMargin",
        SignalType::BinSingleForwardArbCloseUm => "BinSingleForwardArbCloseUm",
        SignalType::BinSingleForwardArbCancelMT => "BinSingleForwardArbCancelMT",
        SignalType::BinSingleForwardArbCancelMM => "BinSingleForwardArbCancelMM",
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
        SignalKind::BinSingleForwardArbOpenMT | SignalKind::BinSingleForwardArbOpenMM => {
            build_parquet_open(entries, range)
        }
        SignalKind::BinSingleForwardArbCancelMT | SignalKind::BinSingleForwardArbCancelMM => {
            build_parquet_cancel(entries, range)
        }
    }
}

fn build_parquet_open(entries: Vec<(Vec<u8>, Vec<u8>)>, range: &RangeFilter) -> Result<Vec<u8>> {
    let mut key_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut ts_us_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut strategy_id_col: Vec<i32> = Vec::with_capacity(entries.len());
    let mut create_ts_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut spot_symbol_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut futures_symbol_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut amount_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut side_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut order_type_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut price_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut price_tick_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut exp_time_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut spot_bid0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut spot_ask0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut swap_bid0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut swap_ask0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut open_threshold_col: Vec<f64> = Vec::with_capacity(entries.len());
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
        let open_ctx = BinSingleForwardArbOpenCtx::from_bytes(Bytes::from(record.context.clone()))
            .map_err(|err| anyhow!("failed to decode BinSingleForwardArbOpenCtx: {err}"))?;

        key_col.push(key);
        ts_us_col.push(ts_us as i64);
        strategy_id_col.push(strategy_id);
        create_ts_col.push(open_ctx.create_ts);
        spot_symbol_col.push(open_ctx.spot_symbol.clone());
        futures_symbol_col.push(open_ctx.futures_symbol.clone());
        amount_col.push(open_ctx.amount as f64);
        side_col.push(open_ctx.side.as_str().to_string());
        order_type_col.push(open_ctx.order_type.as_str().to_string());
        price_col.push(open_ctx.price);
        price_tick_col.push(open_ctx.price_tick);
        exp_time_col.push(open_ctx.exp_time);
        spot_bid0_col.push(open_ctx.spot_bid0);
        spot_ask0_col.push(open_ctx.spot_ask0);
        swap_bid0_col.push(open_ctx.swap_bid0);
        swap_ask0_col.push(open_ctx.swap_ask0);
        open_threshold_col.push(open_ctx.open_threshold);
        funding_ma_col.push(open_ctx.funding_ma);
        predicted_funding_rate_col.push(open_ctx.predicted_funding_rate);
        loan_rate_col.push(open_ctx.loan_rate);
    }

    let mut df = DataFrame::new(vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_us_col),
        Series::new("strategy_id".into(), strategy_id_col),
        Series::new("create_ts".into(), create_ts_col),
        Series::new("spot_symbol".into(), spot_symbol_col),
        Series::new("futures_symbol".into(), futures_symbol_col),
        Series::new("amount".into(), amount_col),
        Series::new("side".into(), side_col),
        Series::new("order_type".into(), order_type_col),
        Series::new("price".into(), price_col),
        Series::new("price_tick".into(), price_tick_col),
        Series::new("exp_time".into(), exp_time_col),
        Series::new("spot_bid0".into(), spot_bid0_col),
        Series::new("spot_ask0".into(), spot_ask0_col),
        Series::new("swap_bid0".into(), swap_bid0_col),
        Series::new("swap_ask0".into(), swap_ask0_col),
        Series::new("open_threshold".into(), open_threshold_col),
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
    let mut spot_symbol_col = Vec::with_capacity(entries.len());
    let mut futures_symbol_col = Vec::with_capacity(entries.len());
    let mut cancel_threshold_col = Vec::with_capacity(entries.len());
    let mut spot_bid0_col = Vec::with_capacity(entries.len());
    let mut spot_ask0_col = Vec::with_capacity(entries.len());
    let mut swap_bid0_col = Vec::with_capacity(entries.len());
    let mut swap_ask0_col = Vec::with_capacity(entries.len());
    let mut trigger_ts_col = Vec::with_capacity(entries.len());

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let (ts_us, strategy_id) = parse_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }
        let record = SignalRecordMessage::from_bytes(Bytes::from(value_bytes))?;
        let ctx = BinSingleForwardArbCancelCtx::from_bytes(Bytes::from(record.context.clone()))
            .map_err(|err| anyhow!("failed to decode BinSingleForwardArbCancelCtx: {err}"))?;

        key_col.push(key);
        ts_us_col.push(ts_us as i64);
        strategy_id_col.push(strategy_id);
        spot_symbol_col.push(ctx.spot_symbol.clone());
        futures_symbol_col.push(ctx.futures_symbol.clone());
        cancel_threshold_col.push(ctx.cancel_threshold);
        spot_bid0_col.push(ctx.spot_bid0);
        spot_ask0_col.push(ctx.spot_ask0);
        swap_bid0_col.push(ctx.swap_bid0);
        swap_ask0_col.push(ctx.swap_ask0);
        trigger_ts_col.push(ctx.trigger_ts);
    }

    let columns = vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_us_col),
        Series::new("strategy_id".into(), strategy_id_col),
        Series::new("spot_symbol".into(), spot_symbol_col),
        Series::new("futures_symbol".into(), futures_symbol_col),
        Series::new("cancel_threshold".into(), cancel_threshold_col),
        Series::new("spot_bid0".into(), spot_bid0_col),
        Series::new("spot_ask0".into(), spot_ask0_col),
        Series::new("swap_bid0".into(), swap_bid0_col),
        Series::new("swap_ask0".into(), swap_ask0_col),
        Series::new("trigger_ts".into(), trigger_ts_col),
    ];

    let mut df = DataFrame::new(columns)?;
    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;
    Ok(buf)
}
