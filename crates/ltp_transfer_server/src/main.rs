//! Standalone LTP transfer service. Delist owns login; this service owns funds operations.
use anyhow::{ensure, Context, Result};
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

mod ltp;
mod model;
mod store;
#[cfg(test)]
mod tests;

use ltp::{currency_list, network_details, Ltp};
use model::{decimal_cmp, decimal_precision, Account, Catalog, Record, TransferRequest};
use store::Store;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "config/ltp_transfer_server.toml")]
    config: PathBuf,
    /// Verify real account/wallet/network reads without listening or moving funds.
    #[arg(long)]
    check: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    bind: SocketAddr,
    public_origin: String,
    delist_auth_url: String,
    ltp_rest_url: String,
    source_ip: Option<IpAddr>,
    web_dir: PathBuf,
    audit_dir: PathBuf,
}

#[derive(Clone)]
struct Prepared {
    request: TransferRequest,
    actor: String,
    expires_at: DateTime<Utc>,
}

struct App {
    ltp: Ltp,
    auth_http: reqwest::Client,
    auth_url: String,
    origin: String,
    web_dir: PathBuf,
    catalog: Mutex<Option<(Instant, Catalog)>>,
    previews: Mutex<BTreeMap<String, Prepared>>,
    store: Mutex<Store>,
}

#[derive(Clone)]
struct Actor(String);

struct ApiError(StatusCode, String);
type ApiResult<T> = std::result::Result<T, ApiError>;

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self(StatusCode::BAD_GATEWAY, format!("{error:#}"))
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}
fn bad(error: anyhow::Error) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, error.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let config: Config = toml::from_str(
        &std::fs::read_to_string(&args.config).context("read transfer service configuration")?,
    )?;
    ensure!(
        config.bind.ip().is_loopback(),
        "transfer server must listen on loopback behind Delist"
    );
    let origin = url::Url::parse(&config.public_origin)?;
    ensure!(
        matches!(origin.scheme(), "http" | "https")
            && origin.origin().ascii_serialization() == config.public_origin,
        "public_origin must be an exact HTTP origin without a trailing slash"
    );
    let auth = url::Url::parse(&config.delist_auth_url)?;
    ensure!(
        auth.host_str()
            .and_then(|v| v.parse::<IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback())
            && auth.path() == "/auth/verify-login",
        "authentication must use the local Delist verifier"
    );
    ensure!(
        matches!(
            config.ltp_rest_url.as_str(),
            "https://api.liquiditytech.com" | "https://api-az4.liquiditytech.com"
        ),
        "unsupported LTP REST origin"
    );
    let key = std::env::var("LTP_TRANSFER_API_KEY")
        .context("dedicated LTP_TRANSFER_API_KEY is required")?;
    let secret = std::env::var("LTP_TRANSFER_API_SECRET")
        .context("dedicated LTP_TRANSFER_API_SECRET is required")?;
    ensure!(
        !key.trim().is_empty() && !secret.trim().is_empty(),
        "dedicated transfer credentials are empty"
    );
    let http = reqwest::Client::builder()
        .no_proxy()
        .local_address(config.source_ip)
        .timeout(Duration::from_secs(15))
        .retry(reqwest::retry::never())
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let ltp = Ltp {
        http,
        base: config.ltp_rest_url,
        key,
        secret,
    };
    if args.check {
        let catalog = ltp.catalog().await?;
        let mut wallets = Vec::new();
        for account in &catalog.accounts {
            for wallet in &account.account_types {
                let balances = ltp.balances(account, wallet).await?;
                wallets.push(json!({"account_id": account.id, "account_type": wallet, "asset_count": balances.len()}));
            }
        }
        let mut routes = Vec::new();
        for pair in catalog.accounts.windows(2) {
            let (from, to) = (&pair[0], &pair[1]);
            if let (Some(ft), Some(tt)) = (from.account_types.first(), to.account_types.first()) {
                let metadata = ltp.route_metadata(from, ft, to, tt, Some("USDT")).await?;
                routes.push(json!({"from_account_id":from.id,"to_account_id":to.id,"currencies":currency_list(&metadata,false)?}));
            }
        }
        println!(
            "{}",
            json!({"read_only":true,"accounts":catalog.accounts,"wallets":wallets,"routes":routes,"warnings":catalog.warnings})
        );
        return Ok(());
    }
    let app = Arc::new(App {
        ltp,
        auth_http: reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        auth_url: config.delist_auth_url,
        origin: config.public_origin,
        web_dir: config.web_dir,
        catalog: Mutex::new(None),
        previews: Mutex::new(BTreeMap::new()),
        store: Mutex::new(Store::open(&config.audit_dir)?),
    });
    // Validate installed assets before opening a healthy listener.
    for asset in ["index.html", "app.js"] {
        ensure!(
            app.web_dir.join(asset).is_file(),
            "missing built web asset {asset}"
        );
    }
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    log::info!(
        "LTP transfer server listening on {}; Delist authentication enabled",
        config.bind
    );
    axum::serve(listener, router(app))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

fn router(app: Arc<App>) -> Router {
    let protected = Router::new()
        .route("/", get(index))
        .route("/app.js", get(javascript))
        .route("/app.css", get(stylesheet))
        .route("/api/catalog", get(catalog))
        .route("/api/balances", get(balances))
        .route("/api/routes", get(routes))
        .route("/api/transfers/preview", post(preview))
        .route("/api/transfers", get(history).post(submit))
        .route("/api/transfers/:id", get(query_transfer))
        .layer(DefaultBodyLimit::max(16 * 1024))
        .route_layer(middleware::from_fn_with_state(app.clone(), authenticate));
    Router::new()
        .route("/healthz", get(|| async { Json(json!({"ok": true})) }))
        .merge(protected)
        .layer(middleware::from_fn(private_response))
        .with_state(app)
}

async fn private_response(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    for (key, value) in [
        ("cache-control", "no-store"), ("x-content-type-options", "nosniff"), ("referrer-policy", "same-origin"),
        ("content-security-policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'self'; base-uri 'self'; form-action 'self'"),
    ] { response.headers_mut().insert(key, HeaderValue::from_static(value)); }
    response
}

async fn authenticate(State(app): State<Arc<App>>, mut request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let origins = request.headers().get_all(header::ORIGIN);
    if !matches!(method, Method::GET | Method::HEAD)
        && (origins.iter().count() != 1
            || origins.iter().next().and_then(|v| v.to_str().ok()) != Some(&app.origin))
    {
        return ApiError(
            StatusCode::FORBIDDEN,
            "请求来源无效，请从 Delist 页面提交".into(),
        )
        .into_response();
    }
    let Some(cookie) = request.headers().get(header::COOKIE) else {
        return ApiError(StatusCode::UNAUTHORIZED, "请先登录 Delist".into()).into_response();
    };
    let mut verification = app
        .auth_http
        .get(&app.auth_url)
        .header(header::COOKIE, cookie)
        .header("X-Original-Method", method.as_str());
    if let Some(origin) = request.headers().get(header::ORIGIN) {
        verification = verification.header(header::ORIGIN, origin);
    }
    let response = match verification.send().await {
        Ok(response) => response,
        Err(_) => {
            return ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Delist 登录验证暂不可用".into(),
            )
            .into_response()
        }
    };
    if !response.status().is_success() {
        let status = if response.status().as_u16() == 403 {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::UNAUTHORIZED
        };
        return ApiError(status, "Delist 登录已失效或请求未获授权".into()).into_response();
    }
    let actor = response
        .headers()
        .get("x-parameter-actor")
        .and_then(|h| h.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 128);
    let Some(actor) = actor else {
        return ApiError(StatusCode::BAD_GATEWAY, "登录验证未返回操作人".into()).into_response();
    };
    // Identity comes from Delist's server response, never from a browser header.
    request.extensions_mut().insert(Actor(actor.into()));
    next.run(request).await
}

async fn asset(app: &App, filename: &str, content_type: &'static str) -> ApiResult<Response> {
    let bytes = tokio::fs::read(app.web_dir.join(filename))
        .await
        .map_err(|_| ApiError(StatusCode::NOT_FOUND, "页面资源不存在".into()))?;
    Ok(([(header::CONTENT_TYPE, content_type)], bytes).into_response())
}
async fn index(State(app): State<Arc<App>>) -> ApiResult<Response> {
    asset(&app, "index.html", "text/html; charset=utf-8").await
}
async fn javascript(State(app): State<Arc<App>>) -> ApiResult<Response> {
    asset(&app, "app.js", "text/javascript; charset=utf-8").await
}
async fn stylesheet(State(app): State<Arc<App>>) -> ApiResult<Response> {
    asset(&app, "app.css", "text/css; charset=utf-8").await
}

impl App {
    async fn current_catalog(&self) -> Result<Catalog> {
        let mut cache = self.catalog.lock().await;
        if let Some((time, catalog)) = cache.as_ref() {
            if time.elapsed() < Duration::from_secs(65) {
                return Ok(catalog.clone());
            }
        }
        let catalog = self.ltp.catalog().await?;
        *cache = Some((Instant::now(), catalog.clone()));
        Ok(catalog)
    }

    async fn checked_preview(
        &self,
        request: &TransferRequest,
    ) -> Result<(String, String, String, Vec<String>, (bool, bool))> {
        request.validate()?;
        let catalog = self.current_catalog().await?;
        let from = catalog.account(&request.from_account_id, &request.from_account_type)?;
        let to = catalog.account(&request.to_account_id, &request.to_account_type)?;
        ensure!(
            self.store
                .lock()
                .await
                .blocking(&request.from_account_id)?
                .is_none(),
            "来源账户有未完成或结果未知的划转，请先查询原请求"
        );
        let on_chain = needs_network(
            from,
            &request.from_account_type,
            to,
            &request.to_account_type,
        );
        let mut warnings = Vec::new();
        if on_chain {
            let network = request
                .network
                .as_deref()
                .context("此划转可能使用链上通道，请明确选择网络")?;
            let metadata = self
                .ltp
                .route_metadata(
                    from,
                    &request.from_account_type,
                    to,
                    &request.to_account_type,
                    Some(&request.currency),
                )
                .await?;
            let details = network_details(&metadata, &request.currency, network)?;
            let precision = details
                .get("precision")
                .and_then(Value::as_u64)
                .context("网络缺少金额精度")?;
            ensure!(
                decimal_precision(&request.amount)? <= precision as usize,
                "金额超过此网络支持的 {precision} 位小数"
            );
            for key in ["minWithdraw", "minDeposit"] {
                if let Some(minimum) = details
                    .get(key)
                    .and_then(model::scalar)
                    .filter(|s| !s.is_empty())
                {
                    ensure!(
                        decimal_cmp(&request.amount, &minimum)? != Ordering::Less,
                        "金额低于网络最低要求 {minimum} {}",
                        request.currency
                    );
                }
            }
            let fee = details
                .get("estNetworkFee")
                .and_then(model::scalar)
                .unwrap_or_else(|| "未知".into());
            let fee_currency = details
                .get("estNetworkFeeCurrency")
                .and_then(Value::as_str)
                .unwrap_or("");
            warnings.push(format!("普通通道可能走链上，网络 {network}，预计网络费 {fee} {fee_currency}；实际到账以划转记录为准。"));
        } else {
            ensure!(request.network.is_none(), "同交易所内部划转无需指定网络");
        }
        if request.rapid_transfer {
            ensure!(
                from.kind != "funding" && to.kind != "funding",
                "Funding Account 不支持快速划转"
            );
            warnings
                .push("快速划转仅为优先选项，额度不足时自动转普通通道，不保证即时到账。".into());
        }
        let balances = self.ltp.balances(from, &request.from_account_type).await?;
        let rows: Vec<_> = balances
            .iter()
            .filter(|b| {
                b.currency == request.currency
                    && (b.network.is_none() || b.network.as_deref() == request.network.as_deref())
            })
            .collect();
        ensure!(
            rows.len() == 1,
            "所选来源币种或网络没有唯一可用余额，请刷新账户资产"
        );
        let available = rows[0].available.clone();
        ensure!(
            decimal_cmp(&request.amount, &available)? != Ordering::Greater,
            "划转金额超过当前可划转余额 {available} {}",
            request.currency
        );
        Ok((
            from.name.clone(),
            to.name.clone(),
            available,
            warnings,
            (from.kind == "funding", to.kind == "funding"),
        ))
    }

    async fn refresh(&self, record: Record) -> Result<Record> {
        match self.ltp.query_record(&record).await {
            Ok(mut next) => {
                let store = self.store.lock().await;
                if let Some(latest) = store.get(&record.client_order_id)? {
                    // A concurrent stale GET must never downgrade a final exchange result.
                    if !latest.unresolved() {
                        return Ok(latest);
                    }
                    next.actor = latest.actor;
                    store.update(&next)?;
                }
                Ok(next)
            }
            Err(error) => {
                let store = self.store.lock().await;
                let mut current = store.get(&record.client_order_id)?.unwrap_or(record);
                if current.unresolved() {
                    current.reason = Some(format!(
                        "结果待核实，请继续查询原请求，勿重复提交：{error:#}"
                    ));
                    current.updated_at = Utc::now();
                    if store.get(&current.client_order_id)?.is_some() {
                        store.update(&current)?;
                    }
                }
                Ok(current)
            }
        }
    }
}

fn needs_network(from: &Account, from_type: &str, to: &Account, to_type: &str) -> bool {
    from.kind == "funding"
        || to.kind == "funding"
        || ltp::route_name(from, from_type) != ltp::route_name(to, to_type)
}

async fn catalog(State(app): State<Arc<App>>) -> ApiResult<Json<Catalog>> {
    Ok(Json(app.current_catalog().await?))
}

#[derive(Deserialize)]
struct BalanceQuery {
    account_id: String,
    account_type: String,
}
async fn balances(
    State(app): State<Arc<App>>,
    Query(query): Query<BalanceQuery>,
) -> ApiResult<Json<Value>> {
    let catalog = app.current_catalog().await?;
    let account = catalog
        .account(&query.account_id, &query.account_type)
        .map_err(bad)?;
    Ok(Json(
        json!({"balances": app.ltp.balances(account, &query.account_type).await?, "updated_at": Utc::now()}),
    ))
}

#[derive(Deserialize)]
struct RouteQuery {
    from_account_id: String,
    to_account_id: String,
    from_account_type: String,
    to_account_type: String,
    currency: Option<String>,
}
async fn routes(
    State(app): State<Arc<App>>,
    Query(query): Query<RouteQuery>,
) -> ApiResult<Json<Value>> {
    let catalog = app.current_catalog().await?;
    let from = catalog
        .account(&query.from_account_id, &query.from_account_type)
        .map_err(bad)?;
    let to = catalog
        .account(&query.to_account_id, &query.to_account_type)
        .map_err(bad)?;
    let on_chain = needs_network(from, &query.from_account_type, to, &query.to_account_type);
    let currencies = if on_chain {
        currency_list(
            &app.ltp
                .route_metadata(
                    from,
                    &query.from_account_type,
                    to,
                    &query.to_account_type,
                    query.currency.as_deref(),
                )
                .await?,
            false,
        )?
    } else {
        app.ltp
            .balances(from, &query.from_account_type)
            .await?
            .into_iter()
            .map(|b| model::Currency {
                currency: b.currency,
                networks: Vec::new(),
            })
            .collect()
    };
    Ok(Json(
        json!({"currencies": currencies, "requires_network": on_chain, "updated_at": Utc::now(), "warnings": []}),
    ))
}

async fn preview(
    State(app): State<Arc<App>>,
    axum::Extension(actor): axum::Extension<Actor>,
    Json(request): Json<TransferRequest>,
) -> ApiResult<Json<Value>> {
    let (from_name, to_name, available, warnings, _) =
        app.checked_preview(&request).await.map_err(bad)?;
    let preview_id = Uuid::new_v4().to_string();
    let expires_at = Utc::now() + chrono::Duration::seconds(120);
    let mut previews = app.previews.lock().await;
    previews.retain(|_, value| value.expires_at > Utc::now());
    if previews.len() >= 1000 {
        return Err(ApiError(
            StatusCode::TOO_MANY_REQUESTS,
            "预览请求过多，请稍后再试".into(),
        ));
    }
    previews.insert(
        preview_id.clone(),
        Prepared {
            request: request.clone(),
            actor: actor.0,
            expires_at,
        },
    );
    Ok(Json(
        json!({"preview_id": preview_id, "expires_at": expires_at, "request": request, "from_name": from_name, "to_name": to_name, "available": available, "warnings": warnings}),
    ))
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Submit {
    preview_id: String,
}
async fn submit(
    State(app): State<Arc<App>>,
    axum::Extension(actor): axum::Extension<Actor>,
    Json(input): Json<Submit>,
) -> ApiResult<Response> {
    // A replay returns the original durable attempt, including after a process restart.
    if let Some(record) = app.store.lock().await.get(&input.preview_id).map_err(bad)? {
        if record.actor != actor.0 {
            return Ok(not_submitted(
                StatusCode::FORBIDDEN,
                &input.preview_id,
                "此预览属于其他操作人",
            ));
        }
        return Ok((StatusCode::ACCEPTED, Json(record.json())).into_response());
    }
    let Some(prepared) = app.previews.lock().await.get(&input.preview_id).cloned() else {
        return Ok(not_submitted(
            StatusCode::CONFLICT,
            &input.preview_id,
            "预览不存在或已失效，请重新预览",
        ));
    };
    if prepared.actor != actor.0 {
        return Ok(not_submitted(
            StatusCode::FORBIDDEN,
            &input.preview_id,
            "此预览属于其他操作人",
        ));
    }
    if prepared.expires_at <= Utc::now() {
        return Ok(not_submitted(
            StatusCode::CONFLICT,
            &input.preview_id,
            "预览已过期，请重新核对金额",
        ));
    }
    let funding = match app.checked_preview(&prepared.request).await {
        Ok((_, _, _, _, funding)) => funding,
        Err(error) => {
            return Ok(not_submitted(
                StatusCode::BAD_REQUEST,
                &input.preview_id,
                &format!("{error:#}"),
            ))
        }
    };
    let mut record = Record {
        client_order_id: input.preview_id.clone(),
        transfer_id: None,
        request: prepared.request,
        status: "submitting".into(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        actor: actor.0,
        reason: None,
        actual_rapid_transfer: None,
        amount_received: None,
        network_fee: None,
        fee_currency: None,
    };
    {
        let store = app.store.lock().await;
        if let Some(existing) = store.get(&input.preview_id)? {
            return Ok((StatusCode::ACCEPTED, Json(existing.json())).into_response());
        }
        if prepared.expires_at <= Utc::now() {
            return Ok(not_submitted(
                StatusCode::CONFLICT,
                &input.preview_id,
                "预览已过期，请重新核对金额",
            ));
        }
        if let Err(error) = store.reserve(&record) {
            // A partially written audit file is ambiguous: retain the lock and investigate it.
            if store.get(&record.client_order_id)?.is_some() {
                return Err(error.into());
            }
            return Ok(not_submitted(
                StatusCode::CONFLICT,
                &input.preview_id,
                &error.to_string(),
            ));
        }
    }
    app.previews.lock().await.remove(&input.preview_id);
    // Persisted before the only POST. Cancellation/crash leaves a queryable submitting record.
    match app
        .ltp
        .submit(&record.request, &record.client_order_id, funding)
        .await
    {
        Ok(reply) if reply.success() => {
            record.transfer_id = reply
                .body
                .get("data")
                .and_then(|v| v.get("transferId"))
                .and_then(model::scalar);
            record.status = if record.transfer_id.is_some() {
                "pending"
            } else {
                "unknown"
            }
            .into();
            record.reason = Some("请求已提交，尚需查询最终到账状态".into());
        }
        Ok(reply) => {
            // Authentication/validation rejection is definitive; server failures are ambiguous.
            record.status = if definitely_rejected(&reply) {
                "failed"
            } else {
                "unknown"
            }
            .into();
            record.reason = Some(reply.error());
        }
        Err(error) => {
            record.status = "unknown".into();
            record.reason = Some(format!("结果未知，请查询原请求，勿重复提交：{error:#}"));
        }
    }
    record.updated_at = Utc::now();
    {
        let store = app.store.lock().await;
        if let Some(latest) = store
            .get(&record.client_order_id)?
            .filter(|r| !r.unresolved())
        {
            record = latest;
        } else {
            store.update(&record)?;
        }
    }
    if record.unresolved() {
        record = app.refresh(record).await?;
    }
    log::info!(
        "transfer actor={} client_order_id={} status={}",
        record.actor,
        record.client_order_id,
        record.status
    );
    Ok((
        if record.unresolved() {
            StatusCode::ACCEPTED
        } else {
            StatusCode::OK
        },
        Json(record.json()),
    )
        .into_response())
}

fn definitely_rejected(reply: &ltp::Reply) -> bool {
    reply.http_status < 500
        && matches!(
            reply.body.get("code").and_then(Value::as_i64),
            Some(
                1000 | 1001
                    | 2000
                    | 2001
                    | 2002
                    | 2003
                    | 4000
                    | 7000
                    | 100001
                    | 100002
                    | 100003
                    | 100018
                    | 100041
                    | 100400
                    | 100429
                    | 400000
                    | 400009
                    | 403000
                    | 403001
                    | 403002
                    | 403003
                    | 403004
                    | 403005
                    | 403006
                    | 403007
                    | 403008
                    | 403009
                    | 403010
                    | 403011
                    | 403012
                    | 403013
                    | 403014
                    | 403015
                    | 403016
                    | 404004
                    | 405001
            )
        )
}

fn not_submitted(status: StatusCode, id: &str, error: &str) -> Response {
    (
        status,
        Json(json!({"error": error, "submitted": false, "preview_id": id})),
    )
        .into_response()
}

#[derive(Deserialize)]
struct HistoryQuery {
    page: Option<u32>,
    page_size: Option<u32>,
}
async fn history(
    State(app): State<Arc<App>>,
    Query(query): Query<HistoryQuery>,
) -> ApiResult<Json<Value>> {
    let page = query.page.unwrap_or(1);
    let page_size = query.page_size.unwrap_or(50);
    if page == 0 || !(1..=100).contains(&page_size) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "分页参数无效".into()));
    }
    let catalog = app.current_catalog().await?;
    let visible: BTreeSet<_> = catalog.accounts.iter().map(|a| a.id.as_str()).collect();
    let mut warnings = Vec::new();
    let mut records = match app.ltp.history(page, page_size).await {
        Ok(records) => records
            .into_iter()
            .filter(|r| {
                visible.contains(r.request.from_account_id.as_str())
                    && visible.contains(r.request.to_account_id.as_str())
            })
            .collect::<Vec<_>>(),
        Err(error) => {
            warnings.push(format!("交易所划转历史暂不可用：{error:#}"));
            Vec::new()
        }
    };
    let local_records = app.store.lock().await.records()?;
    for mut local in local_records {
        if local.unresolved() {
            local = app.refresh(local).await?;
        }
        records.retain(|r| {
            r.client_order_id != local.client_order_id
                && (local.transfer_id.is_none() || r.transfer_id != local.transfer_id)
        });
        records.push(local);
    }
    records.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    records.truncate(page_size as usize);
    Ok(Json(
        json!({"records": records.iter().map(Record::json).collect::<Vec<_>>(), "updated_at": Utc::now(), "warnings": warnings}),
    ))
}

async fn query_transfer(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let local = app.store.lock().await.get(&id).map_err(bad)?;
    if let Some(record) = local {
        return Ok(Json(app.refresh(record).await?.json()));
    }
    let catalog = app.current_catalog().await?;
    let (key, value) = if let Some(id) = id.strip_prefix("ltp-") {
        ("transferId", id)
    } else {
        ("clientOrderId", id.as_str())
    };
    let data = app
        .ltp
        .get("/api/v1/transfer/get", ltp::params(&[(key, value)]))
        .await?;
    let record = ltp::remote_record(&data)?;
    for account_id in [
        &record.request.from_account_id,
        &record.request.to_account_id,
    ] {
        if !catalog.accounts.iter().any(|a| &a.id == account_id) {
            return Err(ApiError(
                StatusCode::NOT_FOUND,
                "未找到当前账户范围内的划转".into(),
            ));
        }
    }
    Ok(Json(record.json()))
}
