use std::net::SocketAddr;

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State as AxumState;
use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};
use axum::extract::Query;
use axum::routing::get;
use axum::{Json, Router};
use log::{debug, info, warn};
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

use crate::common::time_util::get_timestamp_us;

use super::config::HttpCfg;

#[derive(Clone)]
pub struct WsHub {
    pub tx: broadcast::Sender<String>,
    pub auth_token: Option<String>,
}

impl WsHub {
    pub fn new(capacity: usize, auth_token: Option<String>) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self { tx, auth_token }
    }

    pub fn broadcast(&self, msg: String) {
        let _ = self.tx.send(msg);
    }
}

pub async fn serve_http(cfg: HttpCfg, hub: WsHub) -> Result<()> {
    let cors = if let Some(origins) = cfg.cors_origins.clone() {
        let mut layer = CorsLayer::new().allow_methods(Any).allow_headers(Any);
        for o in origins {
            if let Ok(h) = o.parse::<HeaderValue>() { layer = layer.clone().allow_origin(h); }
        }
        layer
    } else {
        CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any)
    };

    let hub_clone = hub.clone();
    let ws_path = cfg.ws_path.clone();
    let app = Router::new()
        .route("/healthz", get(|| async { Json(serde_json::json!({"ok": true, "ts": get_timestamp_us()/1000})) }))
        .route(&ws_path, get(ws_route))
        .with_state(hub_clone)
        .layer(cors);

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

async fn ws_route(
    ws: WebSocketUpgrade,
    AxumState(h): AxumState<WsHub>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Response {
    if let Some(expected) = &h.auth_token {
        match q.get("token") {
            Some(t) if t == expected => {}
            _ => return axum::http::StatusCode::UNAUTHORIZED.into_response(),
        }
    }
    ws.on_upgrade(move |socket| ws_handler(socket, h))
}
