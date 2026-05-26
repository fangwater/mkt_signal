use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query as AxumQuery, State as AxumState};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Datelike, Duration as ChronoDuration, TimeZone, Timelike, Utc};
use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::fs as async_fs;
use tokio::sync::Mutex;

use mkt_signal::persist_manager::{self, exporter::export_window_to_dir, RocksDbStore};

const SNAPSHOT_FILES: [&str; 3] = [
    "order_updates_unmatched.parquet",
    "trade_updates_unmatched.parquet",
    "uniform_orders.parquet",
];

#[derive(Parser, Debug)]
#[command(name = "order_export_server")]
#[command(
    about = "Per-env HTTP server that runs order_export every minute and serves recent snapshots"
)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    #[arg(long, default_value_t = 8821)]
    port: u16,

    /// Env base directory; defaults to current working directory.
    #[arg(long)]
    base_dir: Option<PathBuf>,

    /// RocksDB path; defaults to <base_dir>/data/persist_manager.
    #[arg(long)]
    persist_dir: Option<PathBuf>,

    /// Cache root; defaults to <base_dir>/data/order_export_cache.
    #[arg(long)]
    cache_dir: Option<PathBuf>,

    /// Hours of snapshots to retain.
    #[arg(long, default_value_t = 3)]
    retention_hours: u32,
}

#[derive(Clone)]
struct AppState {
    cache_dir: Arc<PathBuf>,
    persist_dir: Arc<PathBuf>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = Args::parse();
    let base_dir = args
        .base_dir
        .clone()
        .unwrap_or_else(|| std::env::current_dir().expect("cwd unavailable"));
    let persist_dir = args
        .persist_dir
        .clone()
        .unwrap_or_else(|| base_dir.join("data").join("persist_manager"));
    let cache_dir = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| base_dir.join("data").join("order_export_cache"));

    std::fs::create_dir_all(&cache_dir)
        .with_context(|| format!("create cache_dir {}", cache_dir.display()))?;

    info!(
        "order_export_server init bind={}:{} base={} persist={} cache={} retention_h={}",
        args.bind,
        args.port,
        base_dir.display(),
        persist_dir.display(),
        cache_dir.display(),
        args.retention_hours
    );

    cleanup_tmp_dirs(&cache_dir).await;

    let scheduler_lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
    let cache_dir_arc = Arc::new(cache_dir.clone());

    let scheduler_handle = tokio::spawn(run_scheduler(
        persist_dir.clone(),
        cache_dir_arc.clone(),
        args.retention_hours,
        scheduler_lock.clone(),
    ));

    let state = AppState {
        cache_dir: cache_dir_arc,
        persist_dir: Arc::new(persist_dir.clone()),
    };
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/latest", get(get_latest))
        .route("/snapshots", get(list_snapshots))
        .route("/snapshots/:name/:file", get(get_snapshot_file))
        .route("/export", get(get_export))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", args.bind, args.port).parse()?;
    info!("order_export_server listening at http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let serve = axum::serve(listener, app);

    tokio::select! {
        res = serve => res.context("axum::serve exited")?,
        res = scheduler_handle => {
            res.context("scheduler join")?;
            anyhow::bail!("scheduler task exited unexpectedly");
        }
    }
    Ok(())
}

// ===================== Scheduler =====================

async fn run_scheduler(
    persist_dir: PathBuf,
    cache_dir: Arc<PathBuf>,
    retention_hours: u32,
    lock: Arc<Mutex<()>>,
) {
    loop {
        let now = Utc::now();
        let target = next_tick_at(now);
        let sleep_for = target.signed_duration_since(now);
        let sleep_secs = sleep_for.num_milliseconds().max(0) as u64;
        tokio::time::sleep(Duration::from_millis(sleep_secs)).await;

        let guard = match lock.try_lock() {
            Ok(g) => g,
            Err(_) => {
                warn!("previous export tick still running, skipping");
                continue;
            }
        };

        let fire_ts = Utc::now();
        if let Err(err) =
            run_one_tick(&persist_dir, cache_dir.as_path(), fire_ts, retention_hours).await
        {
            error!("export tick failed: {err:#}");
        }
        drop(guard);
    }
}

/// Returns the next firing instant: next hour boundary + 1 minute.
fn next_tick_at(now: DateTime<Utc>) -> DateTime<Utc> {
    let next_hour_start = Utc
        .with_ymd_and_hms(now.year(), now.month(), now.day(), now.hour(), 0, 0)
        .single()
        .unwrap_or(now)
        + ChronoDuration::hours(1);
    next_hour_start + ChronoDuration::minutes(1)
}

/// Window = [previous full hour, current full hour). At HH:01 we export the just-ended hour.
fn window_bounds(fire_ts: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let end = Utc
        .with_ymd_and_hms(
            fire_ts.year(),
            fire_ts.month(),
            fire_ts.day(),
            fire_ts.hour(),
            0,
            0,
        )
        .single()
        .unwrap_or(fire_ts);
    let start = end - ChronoDuration::hours(1);
    (start, end)
}

fn snapshot_dir_name(start: DateTime<Utc>, end: DateTime<Utc>) -> String {
    format!(
        "{}__{}",
        format_window_component(start),
        format_window_component(end)
    )
}

fn format_window_component(ts: DateTime<Utc>) -> String {
    ts.format("%Y%m%dT%H%M%S.000000Z").to_string()
}

async fn run_one_tick(
    persist_dir: &Path,
    cache_dir: &Path,
    fire_ts: DateTime<Utc>,
    retention_hours: u32,
) -> Result<()> {
    let (start, end) = window_bounds(fire_ts);
    let start_us =
        u64::try_from(start.timestamp_micros()).map_err(|_| anyhow!("negative start timestamp"))?;
    let end_us =
        u64::try_from(end.timestamp_micros()).map_err(|_| anyhow!("negative end timestamp"))?;

    let name = snapshot_dir_name(start, end);
    let final_dir = cache_dir.join(&name);
    if final_dir.exists() {
        info!("snapshot already exists, skipping: {}", name);
        sweep_old_snapshots(cache_dir, fire_ts, retention_hours).await;
        return Ok(());
    }
    let tmp_dir = cache_dir.join(format!(".tmp.{}", name));
    if tmp_dir.exists() {
        let _ = async_fs::remove_dir_all(&tmp_dir).await;
    }

    let persist_dir = persist_dir.to_path_buf();
    let tmp_dir_clone = tmp_dir.clone();
    let export_result = tokio::task::spawn_blocking(move || -> Result<()> {
        let cf_names = persist_manager::required_column_families();
        let tuning = persist_manager::default_tuning();
        let store = RocksDbStore::open_read_only_with_tuning(
            &persist_dir.to_string_lossy(),
            &cf_names,
            &tuning,
        )?;
        export_window_to_dir(&store, &tmp_dir_clone, start_us, end_us)
    })
    .await
    .context("spawn_blocking join")?;

    if let Err(err) = export_result {
        let _ = async_fs::remove_dir_all(&tmp_dir).await;
        return Err(err.context("export_window_to_dir"));
    }

    async_fs::rename(&tmp_dir, &final_dir)
        .await
        .with_context(|| format!("rename {} -> {}", tmp_dir.display(), final_dir.display()))?;
    info!("snapshot ready: {}", name);

    sweep_old_snapshots(cache_dir, fire_ts, retention_hours).await;
    Ok(())
}

async fn sweep_old_snapshots(cache_dir: &Path, fire_ts: DateTime<Utc>, retention_hours: u32) {
    let cutoff = fire_ts - ChronoDuration::hours(retention_hours as i64);
    let mut iter = match async_fs::read_dir(cache_dir).await {
        Ok(it) => it,
        Err(err) => {
            warn!("sweep read_dir {} failed: {}", cache_dir.display(), err);
            return;
        }
    };
    while let Ok(Some(entry)) = iter.next_entry().await {
        let name = entry.file_name();
        let name_str = name.to_string_lossy().to_string();
        if name_str.starts_with(".tmp.") {
            continue;
        }
        let Some(end_ts) = parse_snapshot_end(&name_str) else {
            continue;
        };
        if end_ts <= cutoff {
            let path = entry.path();
            if let Err(err) = async_fs::remove_dir_all(&path).await {
                warn!("sweep remove {} failed: {}", path.display(), err);
            } else {
                info!("swept old snapshot: {}", name_str);
            }
        }
    }
}

fn parse_snapshot_end(name: &str) -> Option<DateTime<Utc>> {
    let (_, end_part) = name.split_once("__")?;
    parse_window_component(end_part)
}

fn parse_window_component(s: &str) -> Option<DateTime<Utc>> {
    let naive = chrono::NaiveDateTime::parse_from_str(s, "%Y%m%dT%H%M%S.%fZ").ok()?;
    Some(Utc.from_utc_datetime(&naive))
}

async fn cleanup_tmp_dirs(cache_dir: &Path) {
    let mut iter = match async_fs::read_dir(cache_dir).await {
        Ok(it) => it,
        Err(_) => return,
    };
    while let Ok(Some(entry)) = iter.next_entry().await {
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if s.starts_with(".tmp.") {
            let _ = async_fs::remove_dir_all(entry.path()).await;
        }
    }
}

// ===================== HTTP =====================

#[derive(Serialize)]
struct SnapshotEntry {
    name: String,
    start_ts: String,
    end_ts: String,
    files: Vec<String>,
}

async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true}))
}

async fn list_snapshots(AxumState(state): AxumState<AppState>) -> Response {
    match collect_snapshots(state.cache_dir.as_path()).await {
        Ok(list) => Json(list).into_response(),
        Err(err) => internal_err(err),
    }
}

async fn get_latest(AxumState(state): AxumState<AppState>) -> Response {
    match collect_snapshots(state.cache_dir.as_path()).await {
        Ok(mut list) => {
            list.sort_by(|a, b| b.end_ts.cmp(&a.end_ts));
            match list.into_iter().next() {
                Some(entry) => Json(entry).into_response(),
                None => (StatusCode::NOT_FOUND, "no snapshots available").into_response(),
            }
        }
        Err(err) => internal_err(err),
    }
}

async fn get_snapshot_file(
    AxumState(state): AxumState<AppState>,
    AxumPath((name, file)): AxumPath<(String, String)>,
) -> Response {
    if !is_valid_snapshot_name(&name) {
        return (StatusCode::BAD_REQUEST, "invalid snapshot name").into_response();
    }
    if !SNAPSHOT_FILES.iter().any(|f| *f == file) {
        return (StatusCode::BAD_REQUEST, "invalid file").into_response();
    }
    let path = state.cache_dir.join(&name).join(&file);
    let bytes = match async_fs::read(&path).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::NOT_FOUND, "snapshot file not found").into_response(),
    };
    let len = bytes.len();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, len)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", file),
        )
        .body(Body::from(bytes))
        .unwrap()
}

async fn collect_snapshots(cache_dir: &Path) -> Result<Vec<SnapshotEntry>> {
    let mut iter = async_fs::read_dir(cache_dir).await?;
    let mut out = Vec::new();
    while let Some(entry) = iter.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(".tmp.") {
            continue;
        }
        let Some((start_s, end_s)) = name.split_once("__") else {
            continue;
        };
        if parse_window_component(start_s).is_none() || parse_window_component(end_s).is_none() {
            continue;
        }
        let mut files = Vec::new();
        for f in SNAPSHOT_FILES.iter() {
            if entry.path().join(f).exists() {
                files.push((*f).to_string());
            }
        }
        out.push(SnapshotEntry {
            name: name.clone(),
            start_ts: start_s.to_string(),
            end_ts: end_s.to_string(),
            files,
        });
    }
    Ok(out)
}

fn is_valid_snapshot_name(name: &str) -> bool {
    if name.contains('/') || name.contains('\\') || name.starts_with('.') {
        return false;
    }
    let Some((s, e)) = name.split_once("__") else {
        return false;
    };
    parse_window_component(s).is_some() && parse_window_component(e).is_some()
}

fn internal_err(err: anyhow::Error) -> Response {
    error!("internal error: {err:#}");
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{err}")).into_response()
}

#[derive(Deserialize)]
struct ExportParams {
    start: String,
    end: String,
}

/// Ad-hoc export: takes start/end (ISO8601 / RFC3339), runs export_window_to_dir against a
/// temp directory, and returns the three parquet files packaged as a single tar.gz.
async fn get_export(
    AxumState(state): AxumState<AppState>,
    AxumQuery(params): AxumQuery<ExportParams>,
) -> Response {
    let start = match DateTime::parse_from_rfc3339(&params.start) {
        Ok(dt) => dt.with_timezone(&Utc),
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid start: {err} (expect RFC3339, e.g. 2026-05-26T00:00:00Z)"),
            )
                .into_response()
        }
    };
    let end = match DateTime::parse_from_rfc3339(&params.end) {
        Ok(dt) => dt.with_timezone(&Utc),
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid end: {err} (expect RFC3339, e.g. 2026-05-26T01:00:00Z)"),
            )
                .into_response()
        }
    };
    if end <= start {
        return (StatusCode::BAD_REQUEST, "end must be > start").into_response();
    }
    let start_us = match u64::try_from(start.timestamp_micros()) {
        Ok(v) => v,
        Err(_) => return (StatusCode::BAD_REQUEST, "start before unix epoch").into_response(),
    };
    let end_us = match u64::try_from(end.timestamp_micros()) {
        Ok(v) => v,
        Err(_) => return (StatusCode::BAD_REQUEST, "end before unix epoch").into_response(),
    };

    let name = snapshot_dir_name(start, end);
    let tmp_dir = state
        .cache_dir
        .join(format!(".export.{}.{}", std::process::id(), name));
    // Defensive: ensure tmp dir is fresh.
    let _ = async_fs::remove_dir_all(&tmp_dir).await;

    let persist_dir = state.persist_dir.as_path().to_path_buf();
    let tmp_dir_for_blocking = tmp_dir.clone();
    let name_for_blocking = name.clone();
    let blocking = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        let cf_names = persist_manager::required_column_families();
        let tuning = persist_manager::default_tuning();
        let store = RocksDbStore::open_read_only_with_tuning(
            &persist_dir.to_string_lossy(),
            &cf_names,
            &tuning,
        )?;
        export_window_to_dir(&store, &tmp_dir_for_blocking, start_us, end_us)?;
        pack_tar_gz(&tmp_dir_for_blocking, &name_for_blocking)
    })
    .await;

    // Always clean up the tmp dir regardless of outcome.
    let cleanup_path = tmp_dir.clone();
    let _ = async_fs::remove_dir_all(&cleanup_path).await;

    let body_bytes = match blocking {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(err)) => return internal_err(err),
        Err(join_err) => return internal_err(anyhow!("spawn_blocking join: {join_err}")),
    };

    let filename = format!("order_export_{}.tar.gz", name);
    let len = body_bytes.len();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/gzip")
        .header(header::CONTENT_LENGTH, len)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .body(Body::from(body_bytes))
        .unwrap()
}

/// Build a tar.gz of `src_dir`, placing entries under `top_dir/` inside the archive.
fn pack_tar_gz(src_dir: &Path, top_dir: &str) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let gz = GzEncoder::new(&mut buf, Compression::default());
        let mut tar_builder = tar::Builder::new(gz);
        tar_builder
            .append_dir_all(top_dir, src_dir)
            .with_context(|| format!("tar append_dir_all {} -> {}", src_dir.display(), top_dir))?;
        let gz = tar_builder.into_inner().context("tar finish")?;
        gz.finish().context("gzip finish")?;
    }
    Ok(buf)
}
