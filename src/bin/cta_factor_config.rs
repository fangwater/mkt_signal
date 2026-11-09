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
    config::{Combination, Config, Interval, Plan, SymbolGroups, Worker},
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
    /// Separate hourly Prep runtime, edited through this same catalog API.
    #[arg(long)]
    hourly_config: Option<PathBuf>,
    #[arg(long)]
    check: bool,
    #[arg(long)]
    parameter_audit_dir: Option<PathBuf>,
}
struct App {
    config: Config,
    worker: Worker,
    spread_config: Option<Config>,
    hourly_config: Option<Config>,
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
    fn hourly(&self, worker: Worker) -> Option<&Config> {
        (worker == Worker::Perp)
            .then_some(self.hourly_config.as_ref())
            .flatten()
    }
    fn combine(&self, worker: Worker, main: Plan, hourly: Option<Plan>) -> Result<Plan> {
        if let Some(hourly) = hourly {
            ensure!(
                main.symbol_groups == hourly.symbol_groups,
                "Prep catalogs must share symbol-group definitions"
            );
            ensure!(
                main.combinations
                    .values()
                    .all(|c| c.interval != Interval::Hour)
                    && main
                        .calculations
                        .values()
                        .all(|c| c.id.interval != Interval::Hour),
                "hourly combinations must belong to the independent hourly runtime"
            );
            ensure!(
                hourly
                    .combinations
                    .values()
                    .all(|c| c.interval == Interval::Hour)
                    && hourly
                        .calculations
                        .values()
                        .all(|c| c.id.interval == Interval::Hour),
                "hourly runtime only accepts 1h combinations"
            );
            let groups = main.symbol_groups;
            let merged = Plan::merge_with_symbol_groups(
                main.combinations
                    .into_values()
                    .chain(hourly.combinations.into_values()),
                groups,
            )?;
            worker.validate_plan(&merged)?;
            Ok(merged)
        } else {
            worker.validate_plan(&main)?;
            Ok(main)
        }
    }
    fn plan(&self, worker: Worker) -> Result<Plan> {
        let (main, _) = self.profile(Some(worker))?;
        self.combine(
            worker,
            Plan::load(&main.combinations_dir)?,
            self.hourly(worker)
                .map(|c| Plan::load(&c.combinations_dir))
                .transpose()?,
        )
    }
    fn target(&self, worker: Worker, interval: Interval) -> Result<&Config> {
        if interval == Interval::Hour {
            if let Some(config) = self.hourly(worker) {
                return Ok(config);
            }
        }
        Ok(self.profile(Some(worker))?.0)
    }
    fn replace_plan(&self, worker: Worker, target: &Config, replacement: Plan) -> Result<Plan> {
        let (main, _) = self.profile(Some(worker))?;
        if let Some(hourly) = self.hourly(worker) {
            if target.combinations_dir == hourly.combinations_dir {
                return self.combine(
                    worker,
                    Plan::load(&main.combinations_dir)?,
                    Some(replacement),
                );
            }
            return self.combine(
                worker,
                replacement,
                Some(Plan::load(&hourly.combinations_dir)?),
            );
        }
        self.combine(worker, replacement, None)
    }
    fn require_revision(&self, worker: Worker, revision: &str) -> Result<Plan> {
        let plan = self.plan(worker)?;
        if catalog_revision(&plan)? != revision {
            return Err(CatalogConflict("configuration changed; reload before saving").into());
        }
        Ok(plan)
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
    interval: Option<Interval>,
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
fn ensure_same_identity(previous: &Combination, next: &Combination) -> Result<()> {
    ensure!((previous.signal.is_none() && next.signal.is_none()) ||
        previous.manager_strategy_name()==next.manager_strategy_name(),
        "signal strategy group/symbol group/market/frequency cannot change in place; create a new combination");
    ensure!(
        previous.interval == next.interval,
        "combination cannot move between runtime directories in place"
    );
    Ok(())
}
fn ensure_shared_groups(main: &Config, hourly: &Config) -> Result<()> {
    let name = mkt_signal::cta_factor_calc::config::SYMBOL_GROUPS_FILE;
    ensure!(
        std::fs::canonicalize(main.combinations_dir.join(name))?
            == std::fs::canonicalize(hourly.combinations_dir.join(name))?,
        "hourly symbol_groups.json must link to the main Prep symbol-group file"
    );
    Ok(())
}

async fn catalog(State(app): State<Arc<App>>, Query(profile): Query<Profile>) -> ApiResult {
    let (_, worker) = app.profile(profile.worker).map_err(error)?;
    let plan = app.plan(worker).map_err(error)?;
    Ok(Json(app.response(plan, worker).map_err(error)?))
}
async fn edit(app: Arc<App>, headers: HeaderMap, request: Save, execute: bool) -> ApiResult {
    same_origin(&headers)
        .map_err(|e| (StatusCode::FORBIDDEN, Json(json!({"error":e.to_string()}))))?;
    ensure_revision(&request.expected_revision)?;
    request.combination.validate().map_err(error)?;
    let (_, worker) = app.profile(request.worker).map_err(error)?;
    let (combination, excluded_symbols) = app
        .symbol_filter
        .filter(request.combination)
        .await
        .map_err(error)?;
    let current = app
        .require_revision(worker, &request.expected_revision)
        .map_err(error)?;
    if let Some(previous) = current.combinations.get(&combination.name) {
        ensure_same_identity(previous, &combination).map_err(error)?;
    }
    let config = app.target(worker, combination.interval).map_err(error)?;
    let local = Plan::load(&config.combinations_dir).map_err(error)?;
    let revision = catalog_revision(&local).map_err(error)?;
    let candidate = edit_combination_checked(
        config,
        Some(combination.clone()),
        None,
        false,
        worker,
        Some(&revision),
    )
    .map_err(error)?;
    let preview = app.replace_plan(worker, config, candidate).map_err(error)?;
    let name = format!("{}.json", combination.name);
    let plan = if execute {
        audited(&app, config, worker, &headers, &name, || {
            edit_combination_checked(
                config,
                Some(combination),
                None,
                true,
                worker,
                Some(&revision),
            )
        })
        .map_err(error)?;
        app.plan(worker).map_err(error)?
    } else {
        preview
    };
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
    let current = app
        .require_revision(worker, &request.expected_revision)
        .map_err(error)?;
    let preview = Plan::merge_with_symbol_groups(
        current.combinations.into_values(),
        request.symbol_groups.clone(),
    )
    .map_err(error)?;
    worker.validate_plan(&preview).map_err(error)?;
    let revision =
        catalog_revision(&Plan::load(&config.combinations_dir).map_err(error)?).map_err(error)?;
    if execute {
        if let Some(hourly) = app.hourly(worker) {
            ensure_shared_groups(config, hourly).map_err(error)?;
        }
        audited(&app, config, worker, &headers, "symbol_groups.json", || {
            edit_symbol_groups_checked(config, request.symbol_groups, true, worker, Some(&revision))
        })
        .map_err(error)?;
    }
    let plan = if execute {
        app.plan(worker).map_err(error)?
    } else {
        preview
    };
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
    let (_, worker) = app.profile(request.worker).map_err(error)?;
    let current = app
        .require_revision(worker, &request.expected_revision)
        .map_err(error)?;
    let combination = current
        .combinations
        .get(&name)
        .context("unknown combination")
        .map_err(error)?;
    let config = app.target(worker, combination.interval).map_err(error)?;
    let revision =
        catalog_revision(&Plan::load(&config.combinations_dir).map_err(error)?).map_err(error)?;
    audited(
        &app,
        config,
        worker,
        &headers,
        &format!("{name}.json"),
        || edit_combination_checked(config, None, Some(&name), true, worker, Some(&revision)),
    )
    .map_err(error)?;
    let plan = app.plan(worker).map_err(error)?;
    Ok(Json(app.response(plan, worker).map_err(error)?))
}
async fn status(State(app): State<Arc<App>>, Query(profile): Query<Profile>) -> Json<Value> {
    let read = (|| -> Result<Value> {
        let (_, worker) = app.profile(profile.worker)?;
        let config = app.target(worker, profile.interval.unwrap_or(Interval::Minute))?;
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
    worker.validate_plan(&Plan::load(&config.combinations_dir)?)?;
    let spread_config = args
        .spread_config
        .map(|path| -> Result<Config> {
            ensure!(worker != Worker::Spread, "duplicate Spread catalog");
            let config = Config::load(&path)?;
            Worker::Spread.validate_plan(&Plan::load(&config.combinations_dir)?)?;
            Ok(config)
        })
        .transpose()?;
    let hourly_config = args
        .hourly_config
        .map(|path| -> Result<Config> {
            ensure!(
                worker == Worker::Perp,
                "hourly Prep requires the Prep configuration server"
            );
            let hourly = Config::load(&path)?;
            ensure!(
                hourly.combinations_dir != config.combinations_dir
                    && hourly.history_dir != config.history_dir,
                "hourly runtime must use separate combination and history directories"
            );
            ensure_shared_groups(&config, &hourly)?;
            Ok(hourly)
        })
        .transpose()?;
    let app = App {
        config,
        worker,
        spread_config,
        hourly_config,
        symbol_filter: ConfigurationSymbolFilter::from_env()?,
        audit_dir: Some(
            args.parameter_audit_dir
                .unwrap_or(parameter_audit::default_root()?),
        ),
    };
    let plan = app.plan(worker)?;
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
