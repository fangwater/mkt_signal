use super::{manager, relay::Hub, DASHBOARD};
use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, HeaderMap},
    response::{Html, IntoResponse},
    routing::get,
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::sync::broadcast;

pub fn router(cfg: &crate::config::HttpCfg, hub: Hub, positions: manager::Positions) -> Router {
    let html = DASHBOARD
        .replace(
            "__CN_FUTURES_WS_PATH__",
            &serde_json::to_string(&cfg.ws_path).expect("string JSON"),
        )
        .replace(
            "__CN_FUTURES_STRATEGY_SOURCES__",
            &serde_json::to_string(&positions.source_ids()).expect("source IDs JSON"),
        );
    Router::new()
        .route(
            "/",
            get(move || {
                let html = html.clone();
                async move { ([(header::CACHE_CONTROL, "no-store")], Html(html)) }
            }),
        )
        .route(
            "/healthz",
            get(|State(hub): State<Hub>| async move { Json(hub.health()) }),
        )
        .route(
            "/strategy-positions/:source",
            get(move |Path(source): Path<String>, headers: HeaderMap| {
                let positions = positions.clone();
                async move {
                    let result = positions.read(&source, &headers).await;
                    ([(header::CACHE_CONTROL, "no-store")], result.map(Json))
                }
            }),
        )
        .route(&cfg.ws_path, get(ws_route))
        .with_state(hub)
}
pub async fn serve(
    cfg: crate::config::HttpCfg,
    hub: Hub,
    positions: manager::Positions,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", cfg.bind, cfg.port)).await?;
    log::info!(
        "native Viz listening at {}{}",
        listener.local_addr()?,
        cfg.ws_path
    );
    axum::serve(listener, router(&cfg, hub, positions)).await?;
    Ok(())
}
async fn ws_route(ws: WebSocketUpgrade, State(hub): State<Hub>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| stream(socket, hub))
}
async fn stream(socket: WebSocket, hub: Hub) {
    let mut rx = hub.tx.subscribe();
    let (mut sender, mut receiver) = socket.split();
    let mut pending = hub.latest();
    loop {
        for raw in pending.drain(..) {
            if !matches!(
                tokio::time::timeout(Duration::from_secs(2), sender.send(Message::Text(raw))).await,
                Ok(Ok(()))
            ) {
                return;
            }
        }
        tokio::select! {
            incoming = receiver.next() => match incoming {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                Some(Ok(_)) => {},
            },
            message = rx.recv() => match message {
                Ok(raw) => pending.push(raw),
                // All frames replace state: recover the newest value of every source.
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    rx = hub.tx.subscribe();
                    pending = hub.latest();
                },
                Err(broadcast::error::RecvError::Closed) => return,
            },
        }
    }
}
