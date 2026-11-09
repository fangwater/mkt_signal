//! One configuration surface for factor combinations and signal controls.
use anyhow::{ensure, Context, Result};
use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use clap::Parser;
use mkt_signal::common::parameter_audit;
use mkt_signal::cta_factor_calc::{
    config::{Combination, Config, Plan, SymbolGroups, Worker},
    runtime::{
        catalog_revision, edit_combination_checked, edit_symbol_groups_checked, CatalogConflict,
    },
    symbol_filter::ConfigurationSymbolFilter,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "config/cta_prep_binance_futures.toml")]
    config: PathBuf,
    #[arg(long, default_value = "127.0.0.1")]
    bind: IpAddr,
    #[arg(long, default_value_t = 19220)]
    port: u16,
    #[arg(long)]
    spread: bool,
    /// Optional independent Spread catalog shown in the same configuration page.
    #[arg(long)]
    spread_config: Option<PathBuf>,
    #[arg(long)]
    check: bool,
    #[arg(long)]
    parameter_audit_dir: Option<PathBuf>,
}
struct App {
    config: Config,
    worker: Worker,
    spread_config: Option<Config>,
    audit_dir: Option<PathBuf>,
    symbol_filter: ConfigurationSymbolFilter,
}
impl App {
    fn profile(&self, requested: Option<Worker>) -> Result<(&Config, Worker)> {
        let worker = requested.unwrap_or(self.worker);
        if worker == self.worker {
            return Ok((&self.config, worker));
        }
        ensure!(worker == Worker::Spread, "factor worker is not configured");
        Ok((
            self.spread_config
                .as_ref()
                .context("Spread catalog is not configured")?,
            worker,
        ))
    }
    fn response(&self, plan: Plan, worker: Worker) -> Result<Value> {
        let mut value = response(plan, worker)?;
        let mut workers = vec![self.worker];
        if self.spread_config.is_some() {
            workers.push(Worker::Spread);
        }
        value["workers"] = json!(workers);
        Ok(value)
    }
}
#[derive(Default, Deserialize)]
struct Profile {
    worker: Option<Worker>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Save {
    worker: Option<Worker>,
    combination: Combination,
    expected_revision: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Remove {
    worker: Option<Worker>,
    expected_revision: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SaveSymbolGroups {
    worker: Option<Worker>,
    symbol_groups: SymbolGroups,
    expected_revision: String,
}

type ApiResult = std::result::Result<Json<Value>, (StatusCode, Json<Value>)>;
#[cfg(test)]
#[path = "cta_factor_config_tests.rs"]
mod tests;
fn error(err: anyhow::Error) -> (StatusCode, Json<Value>) {
    let code = if err.downcast_ref::<CatalogConflict>().is_some()
        || err.downcast_ref::<parameter_audit::Busy>().is_some()
    {
        StatusCode::CONFLICT
    } else {
        StatusCode::BAD_REQUEST
    };
    (code, Json(json!({"error":format!("{err:#}")})))
}
fn same_origin(headers: &HeaderMap) -> Result<()> {
    ensure!(
        headers.get("sec-fetch-site").and_then(|h| h.to_str().ok()) != Some("cross-site"),
        "cross-origin writes are forbidden"
    );
    if let Some(origin) = headers.get("origin") {
        let url = reqwest::Url::parse(origin.to_str()?)?;
        let host = headers.get("host").context("Host is required")?.to_str()?;
        let request = reqwest::Url::parse(&format!("{}://{host}", url.scheme()))?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.host_str() == request.host_str()
                && url.port_or_known_default() == request.port_or_known_default(),
            "cross-origin writes are forbidden"
        );
    }
    Ok(())
}
fn response(plan: Plan, worker: Worker) -> Result<Value> {
    let position_strategies: std::collections::BTreeMap<_, _> = plan
        .combinations
        .values()
        .filter(|c| c.signal.is_some())
        .map(|c| (c.name.clone(), c.manager_strategy_name()))
        .collect();
    Ok(
        json!({"worker":worker,"revision":catalog_revision(&plan)?,"topics":plan.topics(),"unique_factor_count":plan.calculations.len(),"position_strategies":position_strategies,"plan":plan}),
    )
}

fn audited<T>(
    app: &App,
    config: &Config,
    worker: Worker,
    headers: &HeaderMap,
    name: &str,
    write: impl FnOnce() -> Result<T>,
) -> Result<T> {
    ensure!(
        name.strip_suffix(".json")
            .is_some_and(|stem| !stem.is_empty()
                && stem.len() <= 128
                && stem
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))),
        "invalid parameter object name"
    );
    let Some(root) = &app.audit_dir else {
        return write();
    };
    let metadata = parameter_audit::Metadata {
        family: if worker == Worker::Perp {
            "cta_prep"
        } else {
            "cta_spread"
        }
        .into(),
        environment: worker.name().into(),
        source: "config_page".into(),
        actor: headers
            .get("x-parameter-actor")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        source_ip: headers
            .get("x-real-ip")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        session_id: headers.get("cookie").map(|value| {
            use sha2::{Digest, Sha256};
            format!("{:x}", Sha256::digest(value.as_bytes()))[..16].to_string()
        }),
        request_path: Some(format!("{}/{}", worker.name(), name)),
    };
    let path = config.combinations_dir.join(name);
    let key = format!("{}/{}", worker.name(), name);
    parameter_audit::apply(
        root,
        metadata,
        "cta",
        || {
            let value = if path.exists() {
                let bytes = std::fs::read(&path)?;
                if name == "symbol_groups.json" {
                    serde_json::to_value(serde_json::from_slice::<SymbolGroups>(&bytes)?)?
                } else {
                    serde_json::to_value(serde_json::from_slice::<Combination>(&bytes)?)?
                }
            } else {
                Value::Null
            };
            Ok(std::collections::BTreeMap::from([(key.clone(), value)]))
        },
        write,
    )
}
async fn catalog(State(app): State<Arc<App>>, Query(profile): Query<Profile>) -> ApiResult {
    let (config, worker) = app.profile(profile.worker).map_err(error)?;
    let plan = Plan::load(&config.combinations_dir).map_err(error)?;
    worker.validate_plan(&plan).map_err(error)?;
    Ok(Json(app.response(plan, worker).map_err(error)?))
}
async fn edit(app: Arc<App>, headers: HeaderMap, request: Save, execute: bool) -> ApiResult {
    same_origin(&headers)
        .map_err(|e| (StatusCode::FORBIDDEN, Json(json!({"error":e.to_string()}))))?;
    ensure_revision(&request.expected_revision)?;
    request.combination.validate().map_err(error)?;
    let (config, worker) = app.profile(request.worker).map_err(error)?;
    let (combination, excluded_symbols) = app
        .symbol_filter
        .filter(request.combination)
        .await
        .map_err(error)?;
    let name = format!("{}.json", combination.name);
    let write = || {
        edit_combination_checked(
            config,
            Some(combination),
            None,
            execute,
            worker,
            Some(&request.expected_revision),
        )
    };
    let plan = if execute {
        audited(&app, config, worker, &headers, &name, write)
    } else {
        write()
    }
    .map_err(error)?;
    let mut response = app.response(plan, worker).map_err(error)?;
    response["excluded_symbols"] = json!(excluded_symbols);
    Ok(Json(response))
}
fn ensure_revision(revision: &str) -> std::result::Result<(), (StatusCode, Json<Value>)> {
    if revision.is_empty() {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({"error":"reload the current configuration before editing"})),
        ));
    }
    Ok(())
}
async fn save(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<Save>,
) -> ApiResult {
    edit(app, headers, request, true).await
}
async fn preview(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<Save>,
) -> ApiResult {
    edit(app, headers, request, false).await
}
async fn edit_groups(
    app: Arc<App>,
    headers: HeaderMap,
    request: SaveSymbolGroups,
    execute: bool,
) -> ApiResult {
    same_origin(&headers)
        .map_err(|e| (StatusCode::FORBIDDEN, Json(json!({"error":e.to_string()}))))?;
    ensure_revision(&request.expected_revision)?;
    let (config, worker) = app.profile(request.worker).map_err(error)?;
    let write = || {
        edit_symbol_groups_checked(
            config,
            request.symbol_groups,
            execute,
            worker,
            Some(&request.expected_revision),
        )
    };
    let plan = if execute {
        audited(&app, config, worker, &headers, "symbol_groups.json", write)
    } else {
        write()
    }
    .map_err(error)?;
    Ok(Json(app.response(plan, worker).map_err(error)?))
}
async fn save_groups(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<SaveSymbolGroups>,
) -> ApiResult {
    edit_groups(app, headers, request, true).await
}
async fn preview_groups(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<SaveSymbolGroups>,
) -> ApiResult {
    edit_groups(app, headers, request, false).await
}
async fn remove(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(request): Json<Remove>,
) -> ApiResult {
    same_origin(&headers)
        .map_err(|e| (StatusCode::FORBIDDEN, Json(json!({"error":e.to_string()}))))?;
    ensure_revision(&request.expected_revision)?;
    let (config, worker) = app.profile(request.worker).map_err(error)?;
    let plan = audited(
        &app,
        config,
        worker,
        &headers,
        &format!("{name}.json"),
        || {
            edit_combination_checked(
                config,
                None,
                Some(&name),
                true,
                worker,
                Some(&request.expected_revision),
            )
        },
    )
    .map_err(error)?;
    Ok(Json(app.response(plan, worker).map_err(error)?))
}
async fn status(State(app): State<Arc<App>>, Query(profile): Query<Profile>) -> Json<Value> {
    let read = (|| -> Result<Value> {
        let (config, worker) = app.profile(profile.worker)?;
        let path = config.history_dir.join("state.json");
        ensure!(
            std::fs::metadata(&path)?.len() <= 32 * 1024 * 1024,
            "factor status too large"
        );
        let state: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        ensure!(
            state["worker"] == json!(worker),
            "factor status belongs to another worker"
        );
        Ok(
            json!({"available":true,"worker":worker,"updated_ms":state["updated_ms"],"warming":state["warming"],"warm_records":state["warm_records"],"generation":state["generation"],"error":state["error"],"position_tables":state["position_tables"],"positions_error":state["positions_error"]}),
        )
    })();
    Json(read.unwrap_or_else(|_| json!({"available":false})))
}
fn router(app: App) -> Router {
    Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../../web/cta_factor_config/index.html")) }),
        )
        .route("/api/config", get(catalog))
        .route("/api/status", get(status))
        .route("/api/combinations", post(save))
        .route("/api/preview", post(preview))
        .route("/api/symbol-groups", post(save_groups))
        .route("/api/symbol-groups/preview", post(preview_groups))
        .route("/api/combinations/:name", delete(remove))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .layer(axum::middleware::map_response(
            |mut response: Response| async move {
                response
                    .headers_mut()
                    .insert("cache-control", "no-store".parse().unwrap());
                response
                    .headers_mut()
                    .insert("x-content-type-options", "nosniff".parse().unwrap());
                response.into_response()
            },
        ))
        .with_state(Arc::new(app))
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.bind.is_loopback(),
        "configuration service must bind loopback; use the authenticated gateway"
    );
    let worker = if args.spread {
        Worker::Spread
    } else {
        Worker::Perp
    };
    let config = Config::load(&args.config).context("load shared factor configuration")?;
    let plan = Plan::load(&config.combinations_dir)?;
    worker.validate_plan(&plan)?;
    let spread_config = args
        .spread_config
        .map(|path| -> Result<Config> {
            ensure!(worker != Worker::Spread, "duplicate Spread catalog");
            let config = Config::load(&path)?;
            Worker::Spread.validate_plan(&Plan::load(&config.combinations_dir)?)?;
            Ok(config)
        })
        .transpose()?;
    let app = App {
        config,
        worker,
        spread_config,
        symbol_filter: ConfigurationSymbolFilter::from_env()?,
        audit_dir: Some(
            args.parameter_audit_dir
                .unwrap_or(parameter_audit::default_root()?),
        ),
    };
    if args.check {
        println!(
            "{}",
            serde_json::to_string_pretty(&app.response(plan, worker)?)?
        );
        return Ok(());
    }
    let listener = tokio::net::TcpListener::bind(SocketAddr::new(args.bind, args.port)).await?;
    axum::serve(listener, router(app)).await?;
    Ok(())
}
