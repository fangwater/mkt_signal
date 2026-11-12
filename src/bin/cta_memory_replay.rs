//! Independent, read-only Kafka factor history and in-memory threshold replay.
#[path = "cta_memory_replay/model.rs"]
mod model;
#[path = "cta_memory_replay/offline.rs"]
mod offline;
#[path = "cta_memory_replay/portfolio.rs"]
mod portfolio;
#[cfg(test)]
#[path = "cta_memory_replay/tests.rs"]
mod tests;
#[path = "cta_memory_replay/worker.rs"]
mod worker;

use anyhow::{Context, Result};
use axum::{
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use model::{Bootstrap, Memory};
use serde_json::{json, Value};
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{Arc, RwLock},
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "config/bootstrap.json")]
    bootstrap: PathBuf,
    #[arg(long, default_value = "10.61.10.22:9092")]
    brokers: String,
    #[arg(long, default_value = "127.0.0.1")]
    bind: IpAddr,
    #[arg(long, default_value_t = 10242)]
    port: u16,
    #[arg(long, default_value_t = 7)]
    history_days: u64,
    #[arg(long, default_value = "web")]
    web_root: PathBuf,
    #[arg(long)]
    check: bool,
    #[arg(long, requires = "offline_output")]
    offline_input: Option<PathBuf>,
    #[arg(long, requires = "offline_input")]
    offline_output: Option<PathBuf>,
}

#[derive(Clone)]
struct App {
    bootstrap: Arc<Bootstrap>,
    memory: Arc<RwLock<Memory>>,
    brokers: String,
    web_root: PathBuf,
    // Bound concurrent simulations while Kafka ingestion continues independently.
    replay_slots: Arc<tokio::sync::Semaphore>,
}

async fn catalog(State(app): State<App>) -> Result<Json<Value>, Response> {
    let data = app.memory.read().unwrap();
    let error = |e: anyhow::Error| failure(StatusCode::INTERNAL_SERVER_ERROR, e);
    let mut profiles = Vec::new();
    for p in &app.bootstrap.profiles {
        let mut symbols = Vec::new();
        for (symbol, rules) in p.rules().map_err(&error)? {
            let mut factors = Vec::new();
            for rule in rules {
                let rows = data.points.get(&p.factor_id(&symbol, &rule.factor));
                let defaults = p.defaults(&symbol, &rule.strategy).map_err(&error)?;
                let nq_method = if defaults.strict_nq {
                    "strict_rank"
                } else if defaults.linear_nq || defaults.bar_interval.uses_linear_nq() {
                    "linear_median"
                } else {
                    "inclusive_rank"
                };
                factors.push(json!({"factor": rule.strategy, "factor_name": rule.factor, "weight": rule.weight, "nq": rule.nq,
                    "bars": rows.map_or(0, |r| r.len()),
                    "ready_bars": rows.map_or(0, |r| r.iter().filter(|p| p.value.ready).count()),
                    "first_ms": rows.and_then(|r| r.front()).map(|r| r.ts_ms),
                    "last_ms": rows.and_then(|r| r.back()).map(|r| r.ts_ms),
                    "defaults": defaults, "nq_method": nq_method}));
            }
            symbols.push(json!({"symbol": symbol, "factors": factors}));
        }
        profiles.push(json!({"name":p.name(), "source_path":p.source_path, "source_sha256":p.source_sha256,
            "strategy_group":p.combination.strategy_group, "symbol_group":p.combination.symbol_group.as_deref().unwrap_or("all"),
            "interval":p.combination.signal_config().map_err(&error)?.bar_interval,"symbols":symbols}));
    }
    Ok(Json(
        json!({"profiles": profiles, "source_host": app.bootstrap.source_host,
        "fetched_at_ms": app.bootstrap.fetched_at_ms, "brokers": app.brokers,
        "kafka_records": data.kafka_records, "data_revision": data.revision,
        "latest_data_ms": data.latest_data_ms, "last_received_ms": data.last_received_ms,
        "caught_up": data.caught_up, "partitions": data.partitions, "error": data.error,
        "factor_points": data.points.values().map(|r| r.len()).sum::<usize>(),
        "candle_count": data.candles.values().map(|r| r.len()).sum::<usize>(),
        "server_ms": worker::now_ms()}),
    ))
}

fn failure(status: StatusCode, message: impl ToString) -> Response {
    (status, Json(json!({"error": message.to_string()}))).into_response()
}

async fn replay(
    State(app): State<App>,
    headers: HeaderMap,
    Json(request): Json<portfolio::PortfolioRequest>,
) -> Response {
    // The same-origin UI can perform simulations; no cross-origin execution.
    if let Some(origin) = headers.get(header::ORIGIN) {
        let allowed = origin
            .to_str()
            .ok()
            .and_then(|s| reqwest::Url::parse(s).ok())
            .zip(headers.get(header::HOST).and_then(|h| h.to_str().ok()))
            .is_some_and(|(url, host)| url.authority() == host);
        if !allowed {
            return failure(StatusCode::FORBIDDEN, "cross-origin replay is not allowed");
        }
    }
    let Ok(slot) = app.replay_slots.clone().try_acquire_owned() else {
        return failure(
            StatusCode::TOO_MANY_REQUESTS,
            "another replay is running; retry shortly",
        );
    };
    let result = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        portfolio::calculate(&app.bootstrap, &app.memory, request)
    })
    .await;
    match result {
        Ok(Ok(result)) => Json(result).into_response(),
        Ok(Err(error)) => failure(StatusCode::BAD_REQUEST, error),
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, "replay worker failed"),
    }
}

async fn asset(app: App, file: &str, content_type: &'static str) -> Response {
    match tokio::fs::read(app.web_root.join(file)).await {
        Ok(bytes) => ([(header::CONTENT_TYPE, content_type)], bytes).into_response(),
        Err(error) => {
            log::warn!("dashboard asset {file}: {error}");
            failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "dashboard asset is unavailable",
            )
        }
    }
}

fn router(app: App) -> Router {
    Router::new()
        .route(
            "/",
            get(|State(app): State<App>| async move {
                asset(
                    app,
                    "cta_memory_replay/index.html",
                    "text/html; charset=utf-8",
                )
                .await
            }),
        )
        .route(
            "/app.js",
            get(|State(app): State<App>| async move {
                asset(
                    app,
                    "cta_memory_replay/app.js",
                    "text/javascript; charset=utf-8",
                )
                .await
            }),
        )
        .route(
            "/assets/echarts.min.js",
            get(|State(app): State<App>| async move {
                asset(
                    app,
                    "vendor/echarts.min.js",
                    "text/javascript; charset=utf-8",
                )
                .await
            }),
        )
        .route("/api/catalog", get(catalog))
        .route("/api/replay", post(replay))
        .route(
            "/api/healthz",
            get(|State(app): State<App>| async move {
                let data = app.memory.read().unwrap();
                let healthy = data.error.is_none()
                    && data.caught_up
                    && worker::now_ms() - data.last_received_ms < 30_000;
                (
                    if healthy {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    Json(json!({"healthy":healthy,"caught_up":data.caught_up,"error":data.error})),
                )
            }),
        )
        .layer(DefaultBodyLimit::max(16 * 1024))
        .with_state(app)
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    anyhow::ensure!(
        (1..=30).contains(&args.history_days),
        "history_days must be in 1..30"
    );
    let bootstrap: Bootstrap = serde_json::from_slice(
        &std::fs::read(&args.bootstrap).context("read current Prep bootstrap snapshot")?,
    )?;
    let plan = bootstrap.plan()?;
    if let Some(input) = &args.offline_input {
        let result = offline::run(&bootstrap, input)?;
        let output = args
            .offline_output
            .as_ref()
            .context("offline output is required")?;
        serde_json::to_writer(
            std::io::BufWriter::new(std::fs::File::create(output)?),
            &result,
        )?;
        println!(
            "offline comparisons={} points={} candles={}",
            result.results.len(),
            result.point_count,
            result.candle_count
        );
        return Ok(());
    }
    if args.check {
        println!(
            "profiles={} calculations={} brokers={}",
            bootstrap.profiles.len(),
            plan.calculations.len(),
            args.brokers
        );
        return Ok(());
    }
    let app = App {
        bootstrap: Arc::new(bootstrap.clone()),
        memory: Arc::new(RwLock::new(Memory::default())),
        brokers: args.brokers.clone(),
        web_root: args.web_root,
        replay_slots: Arc::new(tokio::sync::Semaphore::new(2)),
    };
    let memory = app.memory.clone();
    std::thread::Builder::new()
        .name("factor-history".into())
        .spawn(move || {
            if let Err(error) =
                worker::run(bootstrap, args.brokers, args.history_days, memory.clone())
            {
                log::error!("factor ingestion stopped: {error:#}");
                memory.write().unwrap().error = Some(format!("ingestion stopped: {error:#}"));
            }
        })?;
    let address = SocketAddr::new(args.bind, args.port);
    let listener = tokio::net::TcpListener::bind(address).await?;
    log::info!("memory replay listening at http://{address}");
    axum::serve(listener, router(app))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
