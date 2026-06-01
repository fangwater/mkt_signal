use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use log::{error, info, warn};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use super::order_update::{CF_ORDER_UPDATE, CF_ORDER_UPDATE_UNMATCHED};
use super::parquet::{
    build_order_updates_df, build_trade_updates_df, build_uniform_orders_df, RangeFilter,
};
use super::storage::RocksDbStore;
use super::sync::center_source_cf_name;
use super::trade_update::{CF_TRADE_UPDATE, CF_TRADE_UPDATE_UNMATCHED};
use super::uniform_order_persist::CF_UNIFORM_ORDER;

const DEFAULT_BIND: &str = "0.0.0.0:8822";
const DEFAULT_PRIMARY_DIR: &str = "data/persist_manager";
const DEFAULT_SECONDARY_DIR: &str = "data/persist_read_server_secondary";
const DEFAULT_MAX_CONCURRENT: usize = 8;
const DEFAULT_BATCH_ROWS: usize = 50_000;
const DEFAULT_MAX_WINDOW_SEC: u64 = 3_600;
const DEFAULT_MAX_RESULT_ROWS: usize = 5_000_000;
const DEFAULT_REQUEST_TIMEOUT_SEC: u64 = 120;
const DEFAULT_FORMAT: OutputFormat = OutputFormat::ArrowIpc;

#[derive(Debug, Clone, Deserialize)]
pub struct PersistReadServerConfig {
    pub enabled: Option<bool>,
    pub bind: Option<String>,
    pub primary_dir: Option<PathBuf>,
    pub secondary_dir: Option<PathBuf>,
    pub max_concurrent: Option<usize>,
    pub batch_rows: Option<usize>,
    pub max_window_sec: Option<u64>,
    pub max_result_rows: Option<usize>,
    pub request_timeout_sec: Option<u64>,
    pub default_format: Option<String>,
    pub enable_parquet: Option<bool>,
    pub allowed_tables: Option<Vec<String>>,
    pub source_id: Option<String>,
}

#[derive(Clone)]
pub struct PersistReadServer {
    config: ResolvedConfig,
    store: Arc<RocksDbStore>,
    semaphore: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
struct ResolvedConfig {
    bind: SocketAddr,
    primary_dir: PathBuf,
    secondary_dir: PathBuf,
    max_concurrent: usize,
    batch_rows: usize,
    max_window_sec: u64,
    max_result_rows: usize,
    request_timeout: Duration,
    default_format: OutputFormat,
    enable_parquet: bool,
    allowed_tables: HashSet<TableKind>,
    source_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
enum TableKind {
    UniformOrders,
    OrderUpdates,
    OrderUpdatesUnmatched,
    TradeUpdates,
    TradeUpdatesUnmatched,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    ArrowIpc,
    Parquet,
}

#[derive(Debug, Deserialize)]
struct ReadParams {
    table: String,
    start_us: u64,
    end_us: u64,
    columns: Option<String>,
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SchemaParams {
    table: String,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Serialize)]
struct SchemaResponse {
    table: String,
    columns: Vec<String>,
    formats: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_id: Option<String>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    message: String,
}

impl PersistReadServer {
    pub fn new(config: PersistReadServerConfig) -> Result<Self> {
        let resolved = ResolvedConfig::from_config(config)?;
        let mut cf_names = resolved.read_column_families();
        cf_names.sort_unstable();
        cf_names.dedup();
        let cf_refs = cf_names.iter().map(String::as_str).collect::<Vec<_>>();
        let tuning = crate::persist_manager::default_tuning();
        let store = Arc::new(RocksDbStore::open_secondary_with_tuning(
            &resolved.primary_dir.to_string_lossy(),
            &resolved.secondary_dir.to_string_lossy(),
            cf_refs.as_slice(),
            &tuning,
        )?);
        let semaphore = Arc::new(Semaphore::new(resolved.max_concurrent));
        Ok(Self {
            config: resolved,
            store,
            semaphore,
        })
    }

    pub async fn run(self) -> Result<()> {
        let bind = self.config.bind;
        info!(
            "persist_read_server init bind={} primary={} secondary={} source_id={} max_concurrent={} batch_rows={} max_window_sec={} max_result_rows={} timeout_sec={} enable_parquet={}",
            bind,
            self.config.primary_dir.display(),
            self.config.secondary_dir.display(),
            self.config.source_id.as_deref().unwrap_or("<base-cf>"),
            self.config.max_concurrent,
            self.config.batch_rows,
            self.config.max_window_sec,
            self.config.max_result_rows,
            self.config.request_timeout.as_secs(),
            self.config.enable_parquet,
        );

        let app = Router::new()
            .route("/healthz", get(healthz))
            .route("/v1/schema", get(get_schema))
            .route("/v1/read", get(get_read))
            .with_state(Arc::new(self));
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .with_context(|| format!("failed to bind {}", bind))?;
        info!("persist_read_server listening at http://{}", bind);
        axum::serve(listener, app)
            .await
            .context("persist_read_server exited")
    }
}

impl ResolvedConfig {
    fn from_config(config: PersistReadServerConfig) -> Result<Self> {
        let bind = config
            .bind
            .unwrap_or_else(|| DEFAULT_BIND.to_string())
            .parse()
            .context("invalid bind address")?;
        let primary_dir = config
            .primary_dir
            .unwrap_or_else(|| PathBuf::from(DEFAULT_PRIMARY_DIR));
        let secondary_dir = config
            .secondary_dir
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SECONDARY_DIR));
        if primary_dir == secondary_dir {
            return Err(anyhow!("primary_dir and secondary_dir must be different"));
        }
        let max_concurrent = config
            .max_concurrent
            .unwrap_or(DEFAULT_MAX_CONCURRENT)
            .max(1);
        let batch_rows = config.batch_rows.unwrap_or(DEFAULT_BATCH_ROWS).max(1);
        let max_window_sec = config
            .max_window_sec
            .unwrap_or(DEFAULT_MAX_WINDOW_SEC)
            .max(1);
        let max_result_rows = config
            .max_result_rows
            .unwrap_or(DEFAULT_MAX_RESULT_ROWS)
            .max(1);
        let request_timeout = Duration::from_secs(
            config
                .request_timeout_sec
                .unwrap_or(DEFAULT_REQUEST_TIMEOUT_SEC)
                .max(1),
        );
        let default_format = match config.default_format.as_deref() {
            Some(raw) => OutputFormat::parse(raw)?,
            None => DEFAULT_FORMAT,
        };
        let enable_parquet = config.enable_parquet.unwrap_or(true);
        if default_format == OutputFormat::Parquet && !enable_parquet {
            return Err(anyhow!(
                "default_format=parquet requires enable_parquet=true"
            ));
        }

        let allowed_tables = match config.allowed_tables {
            Some(names) if !names.is_empty() => names
                .into_iter()
                .map(|name| TableKind::parse(&name))
                .collect::<Result<HashSet<_>>>()?,
            _ => default_allowed_tables(),
        };
        let source_id = config
            .source_id
            .map(|source_id| source_id.trim().to_string())
            .filter(|source_id| !source_id.is_empty());

        Ok(Self {
            bind,
            primary_dir,
            secondary_dir,
            max_concurrent,
            batch_rows,
            max_window_sec,
            max_result_rows,
            request_timeout,
            default_format,
            enable_parquet,
            allowed_tables,
            source_id,
        })
    }

    fn cf_name(&self, table: TableKind) -> String {
        match self.source_id.as_deref() {
            Some(source_id) => center_source_cf_name(source_id, table.cf_name()),
            None => table.cf_name().to_string(),
        }
    }

    fn read_column_families(&self) -> Vec<String> {
        match self.source_id.as_deref() {
            Some(source_id) => self
                .allowed_tables
                .iter()
                .map(|table| center_source_cf_name(source_id, table.cf_name()))
                .collect(),
            None => crate::persist_manager::required_column_families()
                .into_iter()
                .map(str::to_string)
                .collect(),
        }
    }
}

impl TableKind {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            CF_UNIFORM_ORDER | "uniform_order" => Ok(Self::UniformOrders),
            CF_ORDER_UPDATE => Ok(Self::OrderUpdates),
            CF_ORDER_UPDATE_UNMATCHED => Ok(Self::OrderUpdatesUnmatched),
            CF_TRADE_UPDATE => Ok(Self::TradeUpdates),
            CF_TRADE_UPDATE_UNMATCHED => Ok(Self::TradeUpdatesUnmatched),
            _ => Err(anyhow!("unknown table: {raw}")),
        }
    }

    fn cf_name(self) -> &'static str {
        match self {
            Self::UniformOrders => CF_UNIFORM_ORDER,
            Self::OrderUpdates => CF_ORDER_UPDATE,
            Self::OrderUpdatesUnmatched => CF_ORDER_UPDATE_UNMATCHED,
            Self::TradeUpdates => CF_TRADE_UPDATE,
            Self::TradeUpdatesUnmatched => CF_TRADE_UPDATE_UNMATCHED,
        }
    }

    fn columns(self) -> &'static [&'static str] {
        match self {
            Self::UniformOrders => &[
                "key",
                "ts_us",
                "recv_ts_us",
                "symbol",
                "create_ts",
                "update_ts",
                "signal_ts",
                "submit_ts",
                "local_ts",
                "mkt_ts",
                "client_order_id",
                "trading_venue",
                "order_type",
                "side",
                "price",
                "price_offset",
                "amount_init",
                "amount_update",
                "status",
                "from_key",
                "from_key_hex",
                "bbo_spread",
            ],
            Self::OrderUpdates | Self::OrderUpdatesUnmatched => &[
                "key",
                "ts_us",
                "event_time",
                "symbol",
                "order_id",
                "client_order_id",
                "client_order_id_str",
                "side",
                "order_type",
                "time_in_force",
                "price",
                "quantity",
                "cumulative_filled_quantity",
                "status",
                "raw_status",
                "execution_type",
                "raw_execution_type",
                "trading_venue",
            ],
            Self::TradeUpdates | Self::TradeUpdatesUnmatched => &[
                "key",
                "ts_us",
                "event_time",
                "trade_time",
                "symbol",
                "order_id",
                "client_order_id",
                "side",
                "price",
                "is_maker",
                "trading_venue",
                "cumulative_filled_quantity",
                "order_status",
            ],
        }
    }
}

impl OutputFormat {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "arrow" | "arrow_ipc" | "arrow_ipc_stream" => Ok(Self::ArrowIpc),
            "parquet" => Ok(Self::Parquet),
            _ => Err(anyhow!("unknown format: {raw}")),
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::ArrowIpc => "application/vnd.apache.arrow.stream",
            Self::Parquet => "application/vnd.apache.parquet",
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::ArrowIpc => "arrow",
            Self::Parquet => "parquet",
        }
    }
}

fn default_allowed_tables() -> HashSet<TableKind> {
    [
        TableKind::UniformOrders,
        TableKind::OrderUpdatesUnmatched,
        TableKind::TradeUpdatesUnmatched,
    ]
    .into_iter()
    .collect()
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn get_schema(
    State(state): State<Arc<PersistReadServer>>,
    Query(params): Query<SchemaParams>,
) -> Response {
    let table = match TableKind::parse(&params.table) {
        Ok(table) => table,
        Err(err) => return bad_request(err),
    };
    if !state.config.allowed_tables.contains(&table) {
        return bad_request(anyhow!("table not allowed: {}", params.table));
    }
    let mut formats = vec!["arrow_ipc".to_string()];
    if state.config.enable_parquet {
        formats.push("parquet".to_string());
    }
    Json(SchemaResponse {
        table: table.cf_name().to_string(),
        columns: table.columns().iter().map(|s| (*s).to_string()).collect(),
        formats,
        source_id: state.config.source_id.clone(),
    })
    .into_response()
}

async fn get_read(
    State(state): State<Arc<PersistReadServer>>,
    Query(params): Query<ReadParams>,
) -> Response {
    let permit = match state.semaphore.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "1")],
                Json(ErrorResponse {
                    error: "too_many_concurrent_requests".to_string(),
                    message: format!(
                        "max_concurrent={} is already in use",
                        state.config.max_concurrent
                    ),
                }),
            )
                .into_response()
        }
    };

    let timeout = state.config.request_timeout;
    let task_state = state.clone();
    let handle = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        handle_read_blocking(&task_state, params)
    });
    let result = tokio::time::timeout(timeout, async move {
        handle
            .await
            .map_err(|err| anyhow!("read task join failed: {err}"))?
    })
    .await;

    match result {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => internal_err(err),
        Err(_) => (
            StatusCode::REQUEST_TIMEOUT,
            Json(ErrorResponse {
                error: "request_timeout".to_string(),
                message: format!("request exceeded {}s", timeout.as_secs()),
            }),
        )
            .into_response(),
    }
}

fn handle_read_blocking(state: &PersistReadServer, params: ReadParams) -> Result<Response> {
    let table = match TableKind::parse(&params.table) {
        Ok(table) => table,
        Err(err) => return Ok(bad_request(err)),
    };
    if !state.config.allowed_tables.contains(&table) {
        return Ok(bad_request(anyhow!("table not allowed: {}", params.table)));
    }
    if params.end_us <= params.start_us {
        return Ok(bad_request(anyhow!("end_us must be greater than start_us")));
    }
    let window_us = params.end_us.saturating_sub(params.start_us);
    let max_window_us = state.config.max_window_sec.saturating_mul(1_000_000);
    if window_us > max_window_us {
        return Ok(bad_request(anyhow!(
            "requested window {}us exceeds max_window_sec={} ({}us)",
            window_us,
            state.config.max_window_sec,
            max_window_us
        )));
    }

    let format = match params.format.as_deref() {
        Some(raw) => match OutputFormat::parse(raw) {
            Ok(format) => format,
            Err(err) => return Ok(bad_request(err)),
        },
        None => state.config.default_format,
    };
    if format == OutputFormat::Parquet && !state.config.enable_parquet {
        return Ok(bad_request(anyhow!("parquet output is disabled")));
    }
    let selected_columns = match parse_columns(table, params.columns.as_deref()) {
        Ok(columns) => columns,
        Err(err) => return Ok(bad_request(err)),
    };

    state.store.try_catch_up_with_primary()?;
    let start_key = format_time_key(params.start_us);
    let end_key = format_time_key(params.end_us);
    let range = RangeFilter::from_bounds(params.start_us, params.end_us.saturating_sub(1));
    let cf_name = state.config.cf_name(table);
    let (df, row_count) = match read_dataframe_batched(
        &state.store,
        cf_name.as_str(),
        table,
        start_key.as_bytes(),
        end_key.as_bytes(),
        &range,
        &selected_columns,
        state.config.batch_rows,
        state.config.max_result_rows,
    ) {
        Ok(result) => result,
        Err(err) if is_result_too_large(&err) => return Ok(bad_request(err)),
        Err(err) => return Err(err),
    };

    let body = encode_dataframe(df, format, state.config.batch_rows)?;
    info!(
        "persist_read table={} cf={} source_id={} start_us={} end_us={} rows={} format={:?} bytes={}",
        table.cf_name(),
        cf_name,
        state.config.source_id.as_deref().unwrap_or("<base-cf>"),
        params.start_us,
        params.end_us,
        row_count,
        format,
        body.len()
    );

    let filename_prefix = match state.config.source_id.as_deref() {
        Some(source_id) => format!("{}_{}", source_id, table.cf_name()),
        None => table.cf_name().to_string(),
    };
    let filename = format!(
        "{}_{}_{}.{}",
        filename_prefix,
        params.start_us,
        params.end_us,
        format.extension()
    );
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, format.content_type())
        .header(header::CONTENT_LENGTH, body.len())
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .body(Body::from(body))
        .unwrap())
}

fn read_dataframe_batched(
    store: &RocksDbStore,
    cf_name: &str,
    table: TableKind,
    start_key: &[u8],
    end_key: &[u8],
    range: &RangeFilter,
    columns: &[String],
    batch_rows: usize,
    max_result_rows: usize,
) -> Result<(DataFrame, usize)> {
    let mut row_count = 0usize;
    let mut merged: Option<DataFrame> = None;

    store.scan_range_batches(cf_name, start_key, end_key, batch_rows, |entries| {
        if row_count.saturating_add(entries.len()) > max_result_rows {
            return Err(anyhow!(
                "result exceeds max_result_rows={}",
                max_result_rows
            ));
        }

        row_count += entries.len();
        let batch_df = select_columns(build_dataframe(table, entries, range)?, columns)?;
        if let Some(acc) = merged.as_mut() {
            acc.vstack_mut(&batch_df)
                .context("failed to append dataframe batch")?;
        } else {
            merged = Some(batch_df);
        }
        Ok(())
    })?;

    let df = match merged {
        Some(df) => df,
        None => select_columns(build_dataframe(table, Vec::new(), range)?, columns)?,
    };

    Ok((df, row_count))
}

fn is_result_too_large(err: &anyhow::Error) -> bool {
    err.to_string()
        .starts_with("result exceeds max_result_rows=")
}

fn build_dataframe(
    table: TableKind,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<DataFrame> {
    match table {
        TableKind::UniformOrders => build_uniform_orders_df(entries, range),
        TableKind::OrderUpdates | TableKind::OrderUpdatesUnmatched => {
            build_order_updates_df(entries, range)
        }
        TableKind::TradeUpdates | TableKind::TradeUpdatesUnmatched => {
            build_trade_updates_df(entries, range)
        }
    }
}

fn parse_columns(table: TableKind, raw: Option<&str>) -> Result<Vec<String>> {
    let allowed = table.columns();
    match raw.map(str::trim).filter(|s| !s.is_empty()) {
        Some(raw) => {
            let mut out = Vec::new();
            for name in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                if !allowed.iter().any(|candidate| *candidate == name) {
                    return Err(anyhow!("unknown column for {}: {}", table.cf_name(), name));
                }
                out.push(name.to_string());
            }
            if out.is_empty() {
                return Err(anyhow!("columns must not be empty"));
            }
            Ok(out)
        }
        None => Ok(allowed.iter().map(|s| (*s).to_string()).collect()),
    }
}

fn select_columns(df: DataFrame, columns: &[String]) -> Result<DataFrame> {
    df.select(columns.iter().map(String::as_str))
        .context("failed to select requested columns")
}

fn encode_dataframe(mut df: DataFrame, format: OutputFormat, batch_rows: usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match format {
        OutputFormat::ArrowIpc => {
            let mut writer = IpcStreamWriter::new(&mut out);
            writer
                .finish(&mut df)
                .context("failed to encode arrow ipc stream")?;
        }
        OutputFormat::Parquet => {
            ParquetWriter::new(&mut out)
                .with_row_group_size(Some(batch_rows))
                .finish(&mut df)
                .context("failed to encode parquet")?;
        }
    }
    Ok(out)
}

fn format_time_key(ts_us: u64) -> String {
    format!("{:020}", ts_us)
}

fn bad_request(err: anyhow::Error) -> Response {
    warn!("bad request: {err:#}");
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: "bad_request".to_string(),
            message: err.to_string(),
        }),
    )
        .into_response()
}

fn internal_err(err: anyhow::Error) -> Response {
    error!("internal error: {err:#}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: "internal_error".to_string(),
            message: err.to_string(),
        }),
    )
        .into_response()
}
