use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State as AxumState;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use log::info;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::common::time_util::get_timestamp_us;

use super::config::HttpCfg;

#[derive(Clone)]
pub struct WsHub {
    pub tx: broadcast::Sender<String>,
    latest: Arc<Mutex<LatestCache>>,
}

#[derive(Default)]
struct LatestCache {
    by_type: HashMap<String, serde_json::Value>,
    ts_ms: i64,
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
        self.cache_message(&msg);
        let _ = self.tx.send(msg);
    }

    fn snapshot(&self) -> Snapshot {
        let cache = self.latest.lock().ok();
        if let Some(cache) = cache {
            Snapshot {
                ts_ms: cache.ts_ms,
                entries: cache.by_type.values().cloned().collect(),
            }
        } else {
            Snapshot {
                ts_ms: 0,
                entries: Vec::new(),
            }
        }
    }

    fn cache_message(&self, msg: &str) {
        let value: serde_json::Value = match serde_json::from_str(msg) {
            Ok(value) => value,
            Err(_) => return,
        };
        let msg_type = match value.get("type").and_then(|v| v.as_str()) {
            Some(msg_type) => msg_type,
            None => return,
        };
        if let Ok(mut cache) = self.latest.lock() {
            cache.by_type.insert(msg_type.to_string(), value);
            cache.ts_ms = (get_timestamp_us() / 1000) as i64;
        }
    }
}

pub async fn serve_http(cfg: HttpCfg, hub: WsHub) -> Result<()> {
    let hub_clone = hub.clone();
    let ws_path = cfg.ws_path.clone();
    let app = Router::new()
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"ok": true, "ts": get_timestamp_us()/1000})) }),
        )
        .route("/snapshot", get(snapshot_route))
        .route(&ws_path, get(ws_route))
        .with_state(hub_clone);

    let addr: SocketAddr = format!("{}:{}", cfg.bind, cfg.port).parse()?;
    info!("viz_server listening at http://{}{}", addr, cfg.ws_path);
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

async fn ws_handler(mut socket: WebSocket, hub: WsHub) {
    // 订阅广播
    let mut rx = hub.tx.subscribe();
    // 简单心跳：由服务器定时广播，客户端保持被动
    while let Ok(msg) = rx.recv().await {
        if socket.send(Message::Text(msg)).await.is_err() {
            break;
        }
    }
}

async fn ws_route(ws: WebSocketUpgrade, AxumState(h): AxumState<WsHub>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_handler(socket, h))
}

async fn snapshot_route(AxumState(h): AxumState<WsHub>) -> impl IntoResponse {
    Json(h.snapshot())
}
