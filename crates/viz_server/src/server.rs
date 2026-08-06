use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::body::{to_bytes, Body};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{OriginalUri, Request, State as AxumState};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use log::info;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;

use runtime_common::time_util::get_timestamp_us;

use super::config::HttpCfg;

const WS_SEND_TIMEOUT: Duration = Duration::from_secs(2);
const WS_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const CONFIG_PROXY_TIMEOUT: Duration = Duration::from_secs(3);
const CONFIG_PROXY_BODY_LIMIT: usize = 1_000_000;
const EXEC_DASHBOARD_HTML: &str = include_str!("../../../docs/exec_pre_trade_dashboard.html");

#[derive(Clone)]
struct HttpState {
    hub: WsHub,
    config_proxy: Option<ConfigProxy>,
}

#[derive(Clone)]
struct ConfigProxy {
    client: reqwest::Client,
    upstream_base: String,
}

impl ConfigProxy {
    fn new(raw_url: &str) -> Result<Self> {
        let parsed = reqwest::Url::parse(raw_url)
            .with_context(|| format!("invalid exec config proxy URL: {raw_url}"))?;
        anyhow::ensure!(
            matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some(),
            "exec config proxy URL must be an absolute http(s) URL"
        );
        anyhow::ensure!(
            parsed.query().is_none() && parsed.fragment().is_none(),
            "exec config proxy URL cannot contain a query or fragment"
        );
        let client = reqwest::Client::builder()
            .timeout(CONFIG_PROXY_TIMEOUT)
            .build()
            .context("build exec config proxy client")?;
        Ok(Self {
            client,
            upstream_base: raw_url.trim_end_matches('/').to_string(),
        })
    }
}

#[derive(Clone)]
pub struct WsHub {
    pub tx: broadcast::Sender<String>,
    latest: Arc<Mutex<LatestCache>>,
}

#[derive(Default)]
struct LatestCache {
    by_type: HashMap<String, CachedMessage>,
    ts_ms: i64,
    dirty: bool,
}

struct CachedMessage {
    raw: String,
    value: serde_json::Value,
}

#[derive(Serialize)]
struct Snapshot {
    ts_ms: i64,
    entries: Vec<serde_json::Value>,
}

impl WsHub {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self {
            tx,
            latest: Arc::new(Mutex::new(LatestCache::default())),
        }
    }

    pub fn broadcast(&self, msg: String) {
        self.cache_message(msg);
    }

    fn snapshot(&self) -> Snapshot {
        let cache = self.latest.lock().ok();
        if let Some(cache) = cache {
            Snapshot {
                ts_ms: cache.ts_ms,
                entries: cache
                    .by_type
                    .values()
                    .map(|entry| entry.value.clone())
                    .collect(),
            }
        } else {
            Snapshot {
                ts_ms: 0,
                entries: Vec::new(),
            }
        }
    }

    fn latest_messages(&self) -> Vec<String> {
        self.latest
            .lock()
            .ok()
            .map(|cache| {
                cache
                    .by_type
                    .values()
                    .map(|entry| entry.raw.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn take_dirty_messages(&self) -> Vec<String> {
        self.latest
            .lock()
            .ok()
            .and_then(|mut cache| {
                if !cache.dirty {
                    return None;
                }
                cache.dirty = false;
                Some(
                    cache
                        .by_type
                        .values()
                        .map(|entry| entry.raw.clone())
                        .collect(),
                )
            })
            .unwrap_or_default()
    }

    fn cache_message(&self, msg: String) {
        let value: serde_json::Value = match serde_json::from_str(&msg) {
            Ok(value) => value,
            Err(_) => return,
        };
        let msg_type = match value.get("type").and_then(|v| v.as_str()) {
            Some(msg_type) => msg_type,
            None => return,
        };
        if let Ok(mut cache) = self.latest.lock() {
            cache
                .by_type
                .insert(msg_type.to_string(), CachedMessage { raw: msg, value });
            cache.ts_ms = get_timestamp_us() / 1000;
            cache.dirty = true;
        }
    }

    fn spawn_flush_loop(&self) {
        let hub = self.clone();
        tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(WS_FLUSH_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                for msg in hub.take_dirty_messages() {
                    let _ = hub.tx.send(msg);
                }
            }
        });
    }
}

pub async fn serve_http(
    cfg: HttpCfg,
    hub: WsHub,
    exec_dashboard: bool,
    config_proxy_url: Option<String>,
) -> Result<()> {
    hub.spawn_flush_loop();

    let config_proxy = config_proxy_url
        .as_deref()
        .map(ConfigProxy::new)
        .transpose()?;
    let config_proxy_enabled = config_proxy.is_some();
    let state = HttpState { hub, config_proxy };
    let ws_path = cfg.ws_path.clone();
    let mut app = Router::new()
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"ok": true, "ts": get_timestamp_us()/1000})) }),
        )
        .route("/snapshot", get(snapshot_route))
        .route(&ws_path, get(ws_route));
    if exec_dashboard {
        app = app.route("/", get(|| async { Html(EXEC_DASHBOARD_HTML) }));
    }
    if config_proxy_enabled {
        app = app
            .route("/config", get(config_slash_redirect))
            .route("/config/", get(config_proxy_route).post(config_proxy_route))
            .route(
                "/config/*path",
                get(config_proxy_route).post(config_proxy_route),
            );
    }
    let app = app.with_state(state);

    let addr: SocketAddr = format!("{}:{}", cfg.bind, cfg.port).parse()?;
    info!(
        "viz_server listening at http://{}{} config_proxy={}",
        addr, cfg.ws_path, config_proxy_enabled
    );
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

async fn config_slash_redirect(OriginalUri(uri): OriginalUri) -> Redirect {
    let target = uri
        .query()
        .map(|query| format!("/config/?{query}"))
        .unwrap_or_else(|| "/config/".to_string());
    Redirect::temporary(&target)
}

fn config_proxy_path(uri: &Uri) -> Option<String> {
    let suffix = uri.path().strip_prefix("/config")?;
    if !suffix.is_empty() && !suffix.starts_with('/') {
        return None;
    }
    let mut path = if suffix.is_empty() { "/" } else { suffix }.to_string();
    if let Some(query) = uri.query() {
        path.push('?');
        path.push_str(query);
    }
    Some(path)
}

async fn config_proxy_route(
    AxumState(state): AxumState<HttpState>,
    OriginalUri(uri): OriginalUri,
    request: Request,
) -> Response {
    let Some(proxy) = state.config_proxy else {
        return proxy_error(StatusCode::NOT_FOUND, "config proxy is disabled");
    };
    let Some(path) = config_proxy_path(&uri) else {
        return proxy_error(StatusCode::BAD_REQUEST, "invalid config proxy path");
    };
    let (parts, body) = request.into_parts();
    let method = parts.method.clone();
    let body = match to_bytes(body, CONFIG_PROXY_BODY_LIMIT).await {
        Ok(body) => body,
        Err(_) => {
            return proxy_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "config request body exceeds limit",
            )
        }
    };
    let mut upstream = proxy
        .client
        .request(parts.method, format!("{}{}", proxy.upstream_base, path));
    for name in [header::ACCEPT, header::CONTENT_TYPE] {
        if let Some(value) = parts.headers.get(&name) {
            upstream = upstream.header(name, value);
        }
    }
    let upstream = match upstream.body(body).send().await {
        Ok(response) => response,
        Err(err) => {
            log::warn!("exec config proxy request failed: {err}");
            return proxy_error(StatusCode::BAD_GATEWAY, "config service unavailable");
        }
    };
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();
    let body = match upstream.bytes().await {
        Ok(body) => body,
        Err(err) => {
            log::warn!("exec config proxy response failed: {err}");
            return proxy_error(StatusCode::BAD_GATEWAY, "invalid config service response");
        }
    };
    if method == Method::POST {
        info!(
            "exec config proxy update: method={} path={} status={} response={}",
            method,
            path,
            status,
            String::from_utf8_lossy(&body)
        );
    }
    let mut response = Response::builder().status(status);
    for name in [
        header::CONTENT_TYPE,
        header::CACHE_CONTROL,
        header::LOCATION,
    ] {
        if let Some(value) = upstream_headers.get(&name) {
            response = response.header(name, value);
        }
    }
    response
        .body(Body::from(body))
        .unwrap_or_else(|_| proxy_error(StatusCode::BAD_GATEWAY, "invalid proxy response"))
}

fn proxy_error(status: StatusCode, message: &str) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    (
        status,
        headers,
        Json(serde_json::json!({"ok": false, "error": message})),
    )
        .into_response()
}

async fn ws_handler(socket: WebSocket, hub: WsHub) {
    let mut rx = hub.tx.subscribe();
    let (mut sender, mut receiver) = socket.split();

    for msg in hub.latest_messages() {
        let send_result =
            tokio::time::timeout(WS_SEND_TIMEOUT, sender.send(Message::Text(msg))).await;
        match send_result {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return,
        }
    }

    loop {
        tokio::select! {
            biased;

            client_msg = receiver.next() => {
                match client_msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }

            server_msg = rx.recv() => {
                match server_msg {
                    Ok(msg) => {
                        let send_result = tokio::time::timeout(
                            WS_SEND_TIMEOUT,
                            sender.send(Message::Text(msg)),
                        )
                        .await;
                        match send_result {
                            Ok(Ok(())) => {}
                            Ok(Err(_)) | Err(_) => break,
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

async fn ws_route(
    ws: WebSocketUpgrade,
    AxumState(state): AxumState<HttpState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_handler(socket, state.hub))
}

async fn snapshot_route(AxumState(state): AxumState<HttpState>) -> impl IntoResponse {
    Json(state.hub.snapshot())
}

#[cfg(test)]
mod tests {
    use super::config_proxy_path;

    #[test]
    fn config_proxy_path_strips_dashboard_prefix_and_preserves_query() {
        assert_eq!(
            config_proxy_path(&"/config".parse().unwrap()).as_deref(),
            Some("/")
        );
        assert_eq!(
            config_proxy_path(&"/config/".parse().unwrap()).as_deref(),
            Some("/")
        );
        assert_eq!(
            config_proxy_path(&"/config/api/strategy?name=alpha".parse().unwrap()).as_deref(),
            Some("/api/strategy?name=alpha")
        );
        assert!(config_proxy_path(&"/configuration".parse().unwrap()).is_none());
    }
}
