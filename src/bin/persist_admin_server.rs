use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use axum::extract::{Path as AxPath, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use log::{error, info};
use mkt_signal::persist_manager::{self, exporter, RocksDbStore};
use serde::{Deserialize, Serialize};
use tar::Builder;
use tokio::sync::Mutex;

const DEFAULT_CFG_PATH: &str = "config/persist_auto_exporter.toml";
const DEFAULT_BIND: &str = "0.0.0.0:10331";

#[derive(clap::Parser, Debug)]
#[command(name = "persist_admin_server")]
struct Args {
    #[arg(long, default_value = DEFAULT_CFG_PATH)]
    config: PathBuf,

    #[arg(long, default_value = DEFAULT_BIND)]
    bind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AutoExporterConfig {
    targets: Vec<TargetConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TargetConfig {
    name: String,
    input_dir: String,
    output_dir: String,
    interval: ExportInterval,
    #[serde(default = "default_enabled")]
    enabled: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ExportInterval {
    Hour,
    Day,
}

impl ExportInterval {
    fn as_str(self) -> &'static str {
        match self {
            ExportInterval::Hour => "hour",
            ExportInterval::Day => "day",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ApiResponse<T> {
    ok: bool,
    data: T,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    ok: bool,
    error: String,
}

#[derive(Debug, Deserialize)]
struct CreateTargetReq {
    name: String,
    input_dir: String,
    output_dir: String,
    interval: ExportInterval,
    #[serde(default = "default_enabled")]
    enabled: bool,
}

#[derive(Debug, Deserialize)]
struct UpdateTargetReq {
    input_dir: Option<String>,
    output_dir: Option<String>,
    interval: Option<ExportInterval>,
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ManualExportReq {
    mode: String,
}

#[derive(Debug, Deserialize)]
struct ExportQuery {
    limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct ExportRecord {
    ts_ms: i64,
    target: String,
    mode: String,
    interval: ExportInterval,
    start_us: u64,
    end_us: u64,
    archive_path: String,
}

#[derive(Clone)]
struct AppState {
    config_path: PathBuf,
    history: Arc<Mutex<Vec<ExportRecord>>>,
}

fn default_enabled() -> bool {
    true
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = <Args as clap::Parser>::parse();
    if !args.config.exists() {
        return Err(anyhow!("config not found: {}", args.config.display()));
    }

    let state = AppState {
        config_path: args.config.clone(),
        history: Arc::new(Mutex::new(Vec::new())),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/api/v1/targets", get(list_targets).post(create_target))
        .route(
            "/api/v1/targets/:name",
            put(update_target).delete(delete_target),
        )
        .route("/api/v1/targets/:name/export", post(export_target))
        .route("/api/v1/exports", get(list_exports))
        .with_state(state);

    info!(
        "persist_admin_server listening on {} config={}",
        args.bind,
        args.config.display()
    );
    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn list_targets(State(state): State<AppState>) -> impl IntoResponse {
    match load_config(&state.config_path) {
        Ok(cfg) => Json(ApiResponse {
            ok: true,
            data: cfg.targets,
        })
        .into_response(),
        Err(err) => json_error(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn create_target(
    State(state): State<AppState>,
    Json(req): Json<CreateTargetReq>,
) -> impl IntoResponse {
    let mut cfg = match load_config(&state.config_path) {
        Ok(c) => c,
        Err(err) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, err),
    };

    if cfg.targets.iter().any(|t| t.name == req.name) {
        return json_error(
            StatusCode::CONFLICT,
            anyhow!("target already exists: {}", req.name),
        );
    }

    let target = TargetConfig {
        name: req.name,
        input_dir: req.input_dir,
        output_dir: req.output_dir,
        interval: req.interval,
        enabled: req.enabled,
    };
    if let Err(err) = validate_target(&target) {
        return json_error(StatusCode::BAD_REQUEST, err);
    }

    cfg.targets.push(target);
    if let Err(err) = save_config(&state.config_path, &cfg) {
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, err);
    }

    Json(ApiResponse {
        ok: true,
        data: "created",
    })
    .into_response()
}

async fn update_target(
    AxPath(name): AxPath<String>,
    State(state): State<AppState>,
    Json(req): Json<UpdateTargetReq>,
) -> impl IntoResponse {
    let mut cfg = match load_config(&state.config_path) {
        Ok(c) => c,
        Err(err) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, err),
    };

    let Some(target) = cfg.targets.iter_mut().find(|t| t.name == name) else {
        return json_error(StatusCode::NOT_FOUND, anyhow!("target not found: {}", name));
    };

    if let Some(v) = req.input_dir {
        target.input_dir = v;
    }
    if let Some(v) = req.output_dir {
        target.output_dir = v;
    }
    if let Some(v) = req.interval {
        target.interval = v;
    }
    if let Some(v) = req.enabled {
        target.enabled = v;
    }

    if let Err(err) = validate_target(target) {
        return json_error(StatusCode::BAD_REQUEST, err);
    }

    if let Err(err) = save_config(&state.config_path, &cfg) {
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, err);
    }

    Json(ApiResponse {
        ok: true,
        data: "updated",
    })
    .into_response()
}

async fn delete_target(
    AxPath(name): AxPath<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut cfg = match load_config(&state.config_path) {
        Ok(c) => c,
        Err(err) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, err),
    };

    let before = cfg.targets.len();
    cfg.targets.retain(|t| t.name != name);
    if cfg.targets.len() == before {
        return json_error(StatusCode::NOT_FOUND, anyhow!("target not found: {}", name));
    }

    if let Err(err) = save_config(&state.config_path, &cfg) {
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, err);
    }

    Json(ApiResponse {
        ok: true,
        data: "deleted",
    })
    .into_response()
}

async fn export_target(
    AxPath(name): AxPath<String>,
    State(state): State<AppState>,
    Json(req): Json<ManualExportReq>,
) -> impl IntoResponse {
    let cfg = match load_config(&state.config_path) {
        Ok(c) => c,
        Err(err) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, err),
    };

    let Some(target) = cfg.targets.iter().find(|t| t.name == name) else {
        return json_error(StatusCode::NOT_FOUND, anyhow!("target not found: {}", name));
    };

    let now = Utc::now();
    let result = match req.mode.as_str() {
        "nearest_hour" => run_manual_export(target, ExportInterval::Hour, now),
        "nearest_day" => run_manual_export(target, ExportInterval::Day, now),
        _ => Err(anyhow!(
            "unsupported mode: {} (allowed: nearest_hour|nearest_day)",
            req.mode
        )),
    };

    match result {
        Ok(record) => {
            let mut hist = state.history.lock().await;
            hist.push(record.clone());
            if hist.len() > 200 {
                let drop_n = hist.len().saturating_sub(200);
                hist.drain(0..drop_n);
            }
            Json(ApiResponse {
                ok: true,
                data: record,
            })
            .into_response()
        }
        Err(err) => json_error(StatusCode::BAD_REQUEST, err),
    }
}

async fn list_exports(
    State(state): State<AppState>,
    Query(q): Query<ExportQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let hist = state.history.lock().await;
    let data: Vec<ExportRecord> = hist.iter().rev().take(limit).cloned().collect();
    Json(ApiResponse { ok: true, data }).into_response()
}

fn run_manual_export(
    target: &TargetConfig,
    interval: ExportInterval,
    now: DateTime<Utc>,
) -> Result<ExportRecord> {
    validate_target(target)?;
    let export_boundary = latest_finished_window_end(now, interval)?;

    let (start_us, end_us, label) = match interval {
        ExportInterval::Hour => {
            let start = export_boundary - Duration::hours(1) + Duration::microseconds(1);
            (
                u64::try_from(start.timestamp_micros())
                    .map_err(|_| anyhow!("negative start us"))?,
                u64::try_from(export_boundary.timestamp_micros())
                    .map_err(|_| anyhow!("negative end us"))?,
                export_boundary.format("%Y%m%d_%H").to_string(),
            )
        }
        ExportInterval::Day => {
            let start = export_boundary - Duration::days(1) + Duration::microseconds(1);
            (
                u64::try_from(start.timestamp_micros())
                    .map_err(|_| anyhow!("negative start us"))?,
                u64::try_from(export_boundary.timestamp_micros())
                    .map_err(|_| anyhow!("negative end us"))?,
                export_boundary.format("%Y%m%d").to_string(),
            )
        }
    };

    let cf_names = persist_manager::required_column_families();
    let tuning = persist_manager::default_tuning();
    let store = RocksDbStore::open_read_only_with_tuning(&target.input_dir, &cf_names, &tuning)?;

    let output_dir = Path::new(&target.output_dir);
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output_dir {}", output_dir.display()))?;

    let stage_dir = output_dir.join(".stage").join(&target.name).join(&label);
    if stage_dir.exists() {
        std::fs::remove_dir_all(&stage_dir)
            .with_context(|| format!("failed to cleanup stage {}", stage_dir.display()))?;
    }
    std::fs::create_dir_all(&stage_dir)
        .with_context(|| format!("failed to create stage {}", stage_dir.display()))?;

    exporter::export_window_to_dir(&store, &stage_dir, start_us, end_us)?;

    let ts_label = now.format("%Y%m%d_%H%M%S").to_string();
    let archive_name = format!("{}_{}_{}.tar.gz", target.name, label, ts_label);
    let archive_path = output_dir.join(archive_name);
    bundle_window_archive(&stage_dir, &archive_path)?;

    if let Err(err) = std::fs::remove_dir_all(&stage_dir) {
        error!("cleanup stage failed: {} err={err}", stage_dir.display());
    }

    Ok(ExportRecord {
        ts_ms: now.timestamp_millis(),
        target: target.name.clone(),
        mode: format!("nearest_{}", interval.as_str()),
        interval,
        start_us,
        end_us,
        archive_path: archive_path.display().to_string(),
    })
}

fn validate_target(target: &TargetConfig) -> Result<()> {
    if target.name.trim().is_empty() {
        return Err(anyhow!("target.name cannot be empty"));
    }
    let input_dir = Path::new(&target.input_dir);
    let output_dir = Path::new(&target.output_dir);
    ensure_absolute("input_dir", input_dir)?;
    ensure_absolute("output_dir", output_dir)?;
    if !input_dir.exists() {
        return Err(anyhow!("input_dir does not exist: {}", input_dir.display()));
    }
    Ok(())
}

fn load_config(path: &Path) -> Result<AutoExporterConfig> {
    let txt = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let cfg: AutoExporterConfig = toml::from_str(&txt)
        .with_context(|| format!("failed to parse config {}", path.display()))?;
    Ok(cfg)
}

fn save_config(path: &Path, cfg: &AutoExporterConfig) -> Result<()> {
    let txt = toml::to_string_pretty(cfg).context("serialize config")?;
    std::fs::write(path, txt).with_context(|| format!("failed to write {}", path.display()))
}

fn latest_finished_window_end(
    now: DateTime<Utc>,
    interval: ExportInterval,
) -> Result<DateTime<Utc>> {
    let delayed = now - Duration::seconds(60);
    match interval {
        ExportInterval::Hour => {
            let hour_end = Utc
                .with_ymd_and_hms(
                    delayed.year(),
                    delayed.month(),
                    delayed.day(),
                    delayed.hour(),
                    0,
                    0,
                )
                .single()
                .ok_or_else(|| anyhow!("invalid hour boundary from {}", delayed))?;
            Ok(hour_end - Duration::microseconds(1))
        }
        ExportInterval::Day => {
            let day_start = Utc
                .with_ymd_and_hms(delayed.year(), delayed.month(), delayed.day(), 0, 0, 0)
                .single()
                .ok_or_else(|| anyhow!("invalid day boundary from {}", delayed))?;
            Ok(day_start - Duration::microseconds(1))
        }
    }
}

fn bundle_window_archive(stage_dir: &Path, archive_path: &Path) -> Result<()> {
    let files = [
        "order_updates_unmatched.parquet",
        "trade_updates_unmatched.parquet",
        "uniform_orders.parquet",
    ];

    let tar_gz = std::fs::File::create(archive_path)
        .with_context(|| format!("failed to create {}", archive_path.display()))?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = Builder::new(enc);

    for name in files {
        let path = stage_dir.join(name);
        if !path.exists() {
            return Err(anyhow!("missing export file {}", path.display()));
        }
        tar.append_path_with_name(&path, name)
            .with_context(|| format!("failed to add {}", path.display()))?;
    }

    tar.finish().context("failed to finish tar archive")?;
    let enc = tar.into_inner().context("failed to finalize tar writer")?;
    enc.finish().context("failed to finalize gzip stream")?;
    Ok(())
}

fn ensure_absolute(field: &str, path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(anyhow!("{} must be absolute: {}", field, path.display()));
    }
    Ok(())
}

fn json_error(status: StatusCode, err: anyhow::Error) -> axum::response::Response {
    (
        status,
        Json(ErrorResponse {
            ok: false,
            error: format!("{err:#}"),
        }),
    )
        .into_response()
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Persist Export Admin</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    table { border-collapse: collapse; width: 100%; margin-top: 12px; }
    th, td { border: 1px solid #ddd; padding: 8px; }
    th { background: #f5f5f5; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
    input, select, button { padding: 6px 8px; }
    .small { font-size: 12px; color: #666; }
    pre { background: #111; color: #0f0; padding: 8px; max-height: 200px; overflow: auto; }
  </style>
  <script>
    async function api(url, opts) {
      const res = await fetch(url, opts || {});
      const data = await res.json();
      if (!res.ok || data.ok === false) {
        throw new Error(data.error || ('HTTP ' + res.status));
      }
      return data.data;
    }

    async function reload() {
      const targets = await api('api/v1/targets');
      const tbody = document.getElementById('targets');
      tbody.innerHTML = '';
      for (const t of targets) {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${t.name}</td><td>${t.interval}</td><td>${t.enabled}</td><td>${t.input_dir}</td><td>${t.output_dir}</td>` +
          `<td><button onclick="manualExport('${t.name}','nearest_hour')">导出最近Hour</button> <button onclick="manualExport('${t.name}','nearest_day')">导出最近Day</button> <button onclick="toggleTarget('${t.name}',${!t.enabled})">切换启用</button> <button onclick="delTarget('${t.name}')">删除</button></td>`;
        tbody.appendChild(tr);
      }

      const exports = await api('api/v1/exports?limit=20');
      document.getElementById('exports').textContent = JSON.stringify(exports, null, 2);
    }

    async function addTarget() {
      const payload = {
        name: document.getElementById('name').value,
        input_dir: document.getElementById('input').value,
        output_dir: document.getElementById('output').value,
        interval: document.getElementById('interval').value,
        enabled: document.getElementById('enabled').checked,
      };
      await api('api/v1/targets', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
      await reload();
    }

    async function toggleTarget(name, enabled) {
      await api('api/v1/targets/' + encodeURIComponent(name), { method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify({enabled})});
      await reload();
    }

    async function delTarget(name) {
      if (!confirm('删除 target: ' + name + ' ?')) return;
      await api('api/v1/targets/' + encodeURIComponent(name), { method:'DELETE' });
      await reload();
    }

    async function manualExport(name, mode) {
      await api('api/v1/targets/' + encodeURIComponent(name) + '/export', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({mode})});
      await reload();
    }

    window.addEventListener('load', () => reload().catch(e => alert(e.message)));
  </script>
</head>
<body>
  <h2>Persist Export Admin</h2>
  <div class="small">支持 target 配置增删改查 + 手动导出最近 Hour/Day 窗口</div>
  <div class="row">
    <input id="name" placeholder="target name" />
    <input id="input" placeholder="/abs/input_dir" size="32" />
    <input id="output" placeholder="/abs/output_dir" size="32" />
    <select id="interval"><option value="hour">hour</option><option value="day">day</option></select>
    <label><input type="checkbox" id="enabled" checked />enabled</label>
    <button onclick="addTarget()">新增 Target</button>
    <button onclick="reload()">刷新</button>
  </div>

  <table>
    <thead><tr><th>Name</th><th>Interval</th><th>Enabled</th><th>Input</th><th>Output</th><th>Actions</th></tr></thead>
    <tbody id="targets"></tbody>
  </table>

  <h3>Recent Exports</h3>
  <pre id="exports"></pre>
</body>
</html>
"#;
