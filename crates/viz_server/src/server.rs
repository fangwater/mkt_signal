use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State as AxumState;
use axum::response::{Html, IntoResponse};
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
const EXEC_DASHBOARD_HTML: &str = include_str!("../../../docs/exec_pre_trade_dashboard.html");

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

pub async fn serve_http(cfg: HttpCfg, hub: WsHub, exec_dashboard: bool) -> Result<()> {
    hub.spawn_flush_loop();

    let hub_clone = hub.clone();
    let ws_path = cfg.ws_path.clone();
    let mut app = Router::new()
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"ok": true, "ts": get_timestamp_us()/1000})) }),
        )
        .route("/snapshot", get(snapshot_route))
        .route(&ws_path, get(ws_route))
        .with_state(hub_clone);
    if exec_dashboard {
        app = app.route("/", get(|| async { Html(EXEC_DASHBOARD_HTML) }));
    }

    let addr: SocketAddr = format!("{}:{}", cfg.bind, cfg.port).parse()?;
    info!("viz_server listening at http://{}{}", addr, cfg.ws_path);
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
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

async fn ws_route(ws: WebSocketUpgrade, AxumState(h): AxumState<WsHub>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_handler(socket, h))
}

async fn snapshot_route(AxumState(h): AxumState<WsHub>) -> impl IntoResponse {
    Json(h.snapshot())
}
