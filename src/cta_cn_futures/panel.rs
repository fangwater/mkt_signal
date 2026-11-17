//! Prep configuration UI, backed by this worker's native SN lot configuration.
use super::{
    config::{Config, FrequencyConfig, Rule},
    engine::Engine,
    extras::Extras,
};
use crate::{common::parameter_audit, cta_factor_calc::runtime::CatalogConflict};
use anyhow::{ensure, Context, Result};
use axum::{
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use tokio::sync::{mpsc, oneshot};

type Failure = (StatusCode, Json<Value>);
type ApiResult = std::result::Result<Json<Value>, Failure>;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Controls {
    pub name: String,
    pub exchange: String,
    pub symbol: String,
    pub interval: String,
    pub enabled: bool,
    pub lots_per_rule: i32,
    pub entry: crate::cta_perp::config::EntryConfig,
    pub exits: crate::cta_perp::config::ExecutionConfig,
    pub rules: Vec<Rule>,
}
impl From<&Config> for Controls {
    fn from(c: &Config) -> Self {
        Self {
            name: c.name.clone(),
            exchange: c.exchange.clone(),
            symbol: c.symbol.clone(),
            interval: c.interval.clone(),
            enabled: c.enabled,
            lots_per_rule: c.lots_per_rule,
            entry: c.entry.clone(),
            exits: c.exits.clone(),
            rules: c.rules.clone(),
        }
    }
}
impl Controls {
    fn merge(self, base: &Config) -> Result<Config> {
        ensure!(
            (
                self.name.as_str(),
                self.exchange.as_str(),
                self.symbol.as_str(),
                self.interval.as_str()
            ) == (
                base.name.as_str(),
                base.exchange.as_str(),
                base.symbol.as_str(),
                base.interval.as_str()
            ),
            "strategy identity, exchange, symbol and frequency cannot change in place"
        );
        let mut next = base.clone();
        next.enabled = self.enabled;
        next.lots_per_rule = self.lots_per_rule;
        next.entry = self.entry;
        next.exits = self.exits;
        next.rules = self.rules;
        next.validate()?;
        Ok(next)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Save {
    pub configuration: Controls,
    pub expected_revision: String,
}
pub(super) struct Mutation {
    request: Save,
    headers: HeaderMap,
    execute: bool,
    reply: oneshot::Sender<std::result::Result<Value, Failure>>,
}
#[derive(Clone)]
struct App {
    catalog: Arc<RwLock<Value>>,
    tx: mpsc::Sender<Mutation>,
    access_digest: [u8; 32],
}
fn failure(error: anyhow::Error) -> Failure {
    let status = if error.downcast_ref::<CatalogConflict>().is_some()
        || error.downcast_ref::<parameter_audit::Busy>().is_some()
    {
        StatusCode::CONFLICT
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, Json(json!({"error":format!("{error:#}")})))
}
pub(super) fn revision(cfg: &Config) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(cfg)?)))
}
pub(super) fn catalog(cfg: &Config) -> Result<Value> {
    use crate::cta_factor_calc::config::Interval;
    let windows: BTreeMap<_, _> = [
        Interval::Minute,
        Interval::FiveMinutes,
        Interval::FifteenMinutes,
        Interval::Hour,
    ]
    .into_iter()
    .map(|interval| {
        (
            interval.name(),
            json!({
                "factor": super::engine::parameters(interval, false),
                "nq": super::engine::parameters(interval, true),
                "factor_rank_ties": if interval == Interval::Hour { "inclusive" } else { "midpoint" },
            }),
        )
    })
    .collect();
    let configurations: BTreeMap<_, _> = [
        Interval::Minute,
        Interval::FiveMinutes,
        Interval::FifteenMinutes,
        Interval::Hour,
    ]
    .into_iter()
    .map(|interval| (interval.name(), Controls::from(&cfg.for_interval(interval))))
    .collect();
    Ok(
        json!({"worker":"cta-cn-futures","workers":["cta-cn-futures"],"native_futures":true,"revision":revision(cfg)?,"configuration":Controls::from(cfg),"frequency_configurations":configurations,"window_parameters":windows}),
    )
}
fn authorize(app: &App, headers: &HeaderMap) -> std::result::Result<(), Failure> {
    let token = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    if token
        .is_none_or(|token| <[u8; 32]>::from(Sha256::digest(token.as_bytes())) != app.access_digest)
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"请使用配置面板的专用访问链接"})),
        ));
    }
    Ok(())
}
fn same_origin(headers: &HeaderMap) -> Result<()> {
    ensure!(
        headers.get("sec-fetch-site").and_then(|h| h.to_str().ok()) != Some("cross-site"),
        "cross-origin writes are forbidden"
    );
    if let Some(origin) = headers.get("origin") {
        let origin = reqwest::Url::parse(origin.to_str()?)?;
        let host = headers.get("host").context("Host is required")?.to_str()?;
        let request = reqwest::Url::parse(&format!("{}://{host}", origin.scheme()))?;
        ensure!(
            matches!(origin.scheme(), "http" | "https")
                && origin.host_str() == request.host_str()
                && origin.port_or_known_default() == request.port_or_known_default(),
            "cross-origin writes are forbidden"
        );
    }
    Ok(())
}
async fn read(State(app): State<App>) -> ApiResult {
    Ok(Json(
        app.catalog
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone(),
    ))
}
async fn edit(app: App, headers: HeaderMap, request: Save, execute: bool) -> ApiResult {
    authorize(&app, &headers)?;
    same_origin(&headers)
        .map_err(|e| (StatusCode::FORBIDDEN, Json(json!({"error":e.to_string()}))))?;
    let (reply, receive) = oneshot::channel();
    app.tx
        .try_send(Mutation {
            request,
            headers,
            execute,
            reply,
        })
        .map_err(|_| {
            (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"error":"configuration worker is busy; retry"})),
            )
        })?;
    match tokio::time::timeout(std::time::Duration::from_secs(60), receive).await {
        Ok(Ok(result)) => result.map(Json),
        _ => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"configuration result unavailable; reload before retrying"})),
        )),
    }
}
async fn save(State(app): State<App>, headers: HeaderMap, Json(request): Json<Save>) -> ApiResult {
    edit(app, headers, request, true).await
}
async fn preview(
    State(app): State<App>,
    headers: HeaderMap,
    Json(request): Json<Save>,
) -> ApiResult {
    edit(app, headers, request, false).await
}

pub(super) fn router(
    cfg: &Config,
    catalog: Arc<RwLock<Value>>,
    tx: mpsc::Sender<Mutation>,
) -> Result<Router> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let path = cfg.data_directory.join("config-access-token");
    if !path.exists() {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)?;
        write!(
            file,
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        )?;
        file.sync_all()?;
    }
    let metadata = std::fs::symlink_metadata(&path)?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.permissions().mode() & 0o077 == 0,
        "panel access file must be private"
    );
    let token = std::fs::read_to_string(path)?;
    ensure!(
        token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid panel access file"
    );
    let app = App {
        catalog,
        tx,
        access_digest: Sha256::digest(token.as_bytes()).into(),
    };
    Ok(Router::new()
        .route(
            "/",
            get(|| async {
                Html(
                    include_str!("../../web/cta_factor_config/index.html").replacen(
                        "<body>",
                        "<body data-native-futures=\"true\">",
                        1,
                    ),
                )
            }),
        )
        .route("/api/config", get(read).post(save))
        .route("/api/preview", post(preview))
        .layer(DefaultBodyLimit::max(128 * 1024))
        .with_state(app))
}

pub(super) fn apply(
    request: Save,
    headers: &HeaderMap,
    execute: bool,
    path: &Path,
    cfg: &mut Config,
    engine: &mut Engine,
    audit_root: &Path,
) -> Result<Value> {
    let current = Config::load(path)?;
    if request.expected_revision.is_empty()
        || request.expected_revision != revision(&current)?
        || revision(cfg)? != revision(&current)?
    {
        return Err(CatalogConflict("configuration changed; reload before saving (external file changes require a worker restart)".into()).into());
    }
    let next = request.configuration.merge(&current)?;
    let changed = revision(&next)? != revision(&current)?;
    let replacement = if changed {
        Some(engine.reconfigure(&next)?)
    } else {
        None
    };
    let value = catalog(&next)?;
    if execute && changed {
        let key = next.name.clone();
        let config_path = PathBuf::from(path);
        let metadata = parameter_audit::Metadata {
            family: "cta_prep".into(),
            environment: next.name.clone(),
            source: "config_page".into(),
            actor: None,
            source_ip: headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            session_id: None,
            request_path: Some("/api/config".into()),
        };
        parameter_audit::apply(
            audit_root,
            metadata,
            "cta",
            || {
                Ok(BTreeMap::from([(
                    key.clone(),
                    serde_json::to_value(Controls::from(&Config::load(&config_path)?))?,
                )]))
            },
            || {
                if revision(&Config::load(&config_path)?)? != request.expected_revision {
                    return Err(
                        CatalogConflict("configuration changed before saving".into()).into(),
                    );
                }
                parameter_audit::atomic(&config_path, &next)
            },
        )?;
        *engine = replacement.context("missing replacement factor engine")?;
        *cfg = next;
    }
    Ok(value)
}
pub(super) fn process(
    command: Mutation,
    path: &Path,
    cfg: &mut Config,
    engine: &mut Engine,
    extras: &mut Extras,
    catalog: &Arc<RwLock<Value>>,
) {
    if command.reply.is_closed() {
        return;
    }
    let result = parameter_audit::default_root()
        .and_then(|root| {
            if command.request.configuration.interval == "1h" {
                apply(
                    command.request,
                    &command.headers,
                    command.execute,
                    path,
                    cfg,
                    engine,
                    &root,
                )
            } else {
                apply_extra(
                    command.request,
                    &command.headers,
                    command.execute,
                    path,
                    cfg,
                    extras,
                    &root,
                )
            }
        })
        .map_err(failure);
    if command.execute {
        if let Ok(value) = &result {
            *catalog.write().unwrap_or_else(|p| p.into_inner()) = value.clone();
        }
    }
    let _ = command.reply.send(result);
}

pub(super) fn apply_extra(
    request: Save,
    headers: &HeaderMap,
    execute: bool,
    path: &Path,
    cfg: &mut Config,
    extras: &mut Extras,
    audit_root: &Path,
) -> Result<Value> {
    let current = Config::load(path)?;
    if request.expected_revision.is_empty()
        || request.expected_revision != revision(&current)?
        || revision(cfg)? != revision(&current)?
    {
        return Err(CatalogConflict("configuration changed; reload before saving (external file changes require a worker restart)".into()).into());
    }
    let interval = match request.configuration.interval.as_str() {
        "1min" => crate::cta_factor_calc::config::Interval::Minute,
        "5min" => crate::cta_factor_calc::config::Interval::FiveMinutes,
        "15min" => crate::cta_factor_calc::config::Interval::FifteenMinutes,
        _ => anyhow::bail!("unsupported native CTA frequency"),
    };
    let next_frequency = request
        .configuration
        .merge(&current.for_interval(interval))?;
    let mut next = current.clone();
    next.frequencies.insert(
        interval.name().into(),
        FrequencyConfig {
            enabled: next_frequency.enabled,
            lots_per_rule: next_frequency.lots_per_rule,
            entry: next_frequency.entry.clone(),
            exits: next_frequency.exits.clone(),
            rules: next_frequency.rules.clone(),
        },
    );
    next.validate()?;
    let changed = revision(&next)? != revision(&current)?;
    let replacement = if changed {
        Some(extras.prepare(interval, &next_frequency)?)
    } else {
        None
    };
    let value = catalog(&next)?;
    if execute && changed {
        let key = next_frequency.name.clone();
        let metadata = parameter_audit::Metadata {
            family: "cta_prep".into(),
            environment: key.clone(),
            source: "config_page".into(),
            actor: None,
            source_ip: headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            session_id: None,
            request_path: Some("/api/config".into()),
        };
        parameter_audit::apply(
            audit_root,
            metadata,
            "cta",
            || {
                Ok(BTreeMap::from([(
                    key.clone(),
                    serde_json::to_value(Controls::from(
                        &Config::load(path)?.for_interval(interval),
                    ))?,
                )]))
            },
            || {
                if revision(&Config::load(path)?)? != request.expected_revision {
                    return Err(
                        CatalogConflict("configuration changed before saving".into()).into(),
                    );
                }
                parameter_audit::atomic(path, &next)
            },
        )?;
        extras.apply(
            interval,
            next_frequency,
            replacement.context("missing frequency engine")?,
        )?;
        *cfg = next;
    }
    Ok(value)
}
