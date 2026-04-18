//! Bybit V5 用户数据流 WebSocket 连接处理器。
//!
//! 流程：
//! 1. 连接后发送 auth
//! 2. 鉴权成功后订阅 wallet / position / order / execution
//! 3. 每 20 秒发送一次应用层 ping，等待 JSON pong

use crate::connection::connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use crate::portfolio_margin::bybit_auth::BybitCredentials;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, info, warn};
use std::time::Duration;
use tokio::time::{self, Instant};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
enum ConnectionState {
    Authenticating,
    Subscribing,
    Running,
}

pub struct BybitUserDataConnection {
    base_connection: MktConnection,
    credentials: BybitCredentials,
    subscribe_messages: Vec<serde_json::Value>,
    session_max: Option<Duration>,
    restart_count: u32,
}

impl BybitUserDataConnection {
    pub fn new(
        connection: MktConnection,
        credentials: BybitCredentials,
        subscribe_messages: Vec<serde_json::Value>,
        session_max: Option<Duration>,
    ) -> Self {
        Self {
            base_connection: connection,
            credentials,
            subscribe_messages,
            session_max,
            restart_count: 0,
        }
    }

    fn is_auth_response(text: &str) -> Option<bool> {
        let json: serde_json::Value = serde_json::from_str(text).ok()?;
        if json.get("op").and_then(|v| v.as_str()) == Some("auth") {
            return Some(
                json.get("success")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            );
        }
        None
    }

    fn is_subscribe_response(text: &str) -> Option<bool> {
        let json: serde_json::Value = serde_json::from_str(text).ok()?;
        if json.get("op").and_then(|v| v.as_str()) == Some("subscribe") {
            return Some(
                json.get("success")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            );
        }
        None
    }

    fn is_pong_response(text: &str) -> bool {
        let json: serde_json::Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return false,
        };
        (json.get("op").and_then(|v| v.as_str()) == Some("pong")
            || json.get("ret_msg").and_then(|v| v.as_str()) == Some("pong"))
            && json.get("success").is_some()
    }
}

#[async_trait]
impl MktConnectionRunner for BybitUserDataConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        let mut state = ConnectionState::Authenticating;
        let mut ping_timer = Instant::now() + Duration::from_secs(20);
        let mut waiting_pong = false;
        let connected_at = Instant::now();
        let mut pending_subscriptions = self.subscribe_messages.len();

        loop {
            if let Some(max_age) = self.session_max {
                if connected_at.elapsed() >= max_age {
                    info!("Bybit: session max age reached, reconnecting");
                    break;
                }
            }

            let mut ws_stream = self
                .base_connection
                .connection
                .as_mut()
                .unwrap()
                .ws_stream
                .lock()
                .await;

            tokio::select! {
                _ = self.base_connection.shutdown_rx.changed() => {
                    if *self.base_connection.shutdown_rx.borrow() {
                        ws_stream.close(None).await?;
                        return Ok(());
                    }
                }
                _ = time::sleep_until(ping_timer) => {
                    if state != ConnectionState::Running {
                        ping_timer = Instant::now() + Duration::from_secs(20);
                        continue;
                    }

                    if waiting_pong {
                        warn!("Bybit: ping timeout detected, reconnecting...");
                        ws_stream.close(None).await?;
                        break;
                    }

                    let ping_msg = serde_json::json!({
                        "req_id": Uuid::new_v4().to_string(),
                        "op": "ping"
                    });
                    if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string())).await {
                        error!("Bybit: failed to send ping: {:?}", e);
                        break;
                    }
                    waiting_pong = true;
                    ping_timer = Instant::now() + Duration::from_secs(20);
                }
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Text(text) => {
                                    if Self::is_pong_response(&text) {
                                        waiting_pong = false;
                                        ping_timer = Instant::now() + Duration::from_secs(20);
                                        continue;
                                    }

                                    match state {
                                        ConnectionState::Authenticating => {
                                            if let Some(success) = Self::is_auth_response(&text) {
                                                if success {
                                                    info!("Bybit: auth successful");
                                                    for sub_msg in &self.subscribe_messages {
                                                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                                                            error!("Bybit: failed to send subscribe message: {:?}", e);
                                                            break;
                                                        }
                                                    }
                                                    state = ConnectionState::Subscribing;
                                                } else {
                                                    error!("Bybit: auth failed: {}", text);
                                                    break;
                                                }
                                            }
                                        }
                                        ConnectionState::Subscribing => {
                                            if let Some(success) = Self::is_subscribe_response(&text) {
                                                if success {
                                                    pending_subscriptions = pending_subscriptions.saturating_sub(1);
                                                    if pending_subscriptions == 0 {
                                                        state = ConnectionState::Running;
                                                        ping_timer = Instant::now() + Duration::from_secs(20);
                                                        info!("Bybit: all subscriptions complete");
                                                    }
                                                } else {
                                                    warn!("Bybit: subscribe failed: {}", text);
                                                }
                                                continue;
                                            }

                                            // Bybit 可能在订阅确认前就开始推送数据。
                                            let bytes = Bytes::from(text.into_bytes());
                                            if let Err(e) = self.base_connection.tx.send(bytes) {
                                                error!("Bybit: failed to broadcast message: {}", e);
                                                break;
                                            }
                                        }
                                        ConnectionState::Running => {
                                            if !waiting_pong {
                                                ping_timer = Instant::now() + Duration::from_secs(20);
                                            }

                                            let bytes = Bytes::from(text.into_bytes());
                                            if let Err(e) = self.base_connection.tx.send(bytes) {
                                                error!("Bybit: failed to broadcast message: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                                Message::Ping(payload) => {
                                    debug!("Bybit: received protocol ping");
                                    if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                        error!("Bybit: failed to send protocol pong: {:?}", e);
                                        break;
                                    }
                                }
                                Message::Close(frame) => {
                                    warn!("Bybit: received close frame: {:?}", frame);
                                    break;
                                }
                                _ => {}
                            }
                        }
                        Err(e) => {
                            self.restart_count += 1;
                            error!("Bybit: websocket error (restart count={}): {:?}", self.restart_count, e);
                            break;
                        }
                        Ok(None) => {
                            warn!("Bybit: websocket closed by server");
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl MktConnectionHandler for BybitUserDataConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        loop {
            info!("Bybit: connecting to {}", &self.base_connection.url);

            let connect_result = if let Some(ref local_ip) = self.base_connection.local_ip {
                WsConnector::connect_with_local_ip_raw(&self.base_connection.url, local_ip).await
            } else {
                WsConnector::connect_raw(&self.base_connection.url).await
            };

            match connect_result {
                Ok(connection) => {
                    self.base_connection.connection = Some(connection);

                    {
                        let auth_msg = self.credentials.build_auth_message();
                        let mut ws_stream = self
                            .base_connection
                            .connection
                            .as_mut()
                            .unwrap()
                            .ws_stream
                            .lock()
                            .await;

                        if let Err(e) = ws_stream.send(Message::Text(auth_msg.to_string())).await {
                            error!("Bybit: failed to send auth message: {:?}", e);
                            continue;
                        }
                        info!("Bybit: auth message sent");
                    }

                    self.run_connection().await?;

                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    }

                    info!(
                        "Bybit: connection closed, reconnecting... (restart count={})",
                        self.restart_count
                    );
                    time::sleep(Duration::from_secs(2)).await;
                }
                Err(e) => {
                    error!("Bybit: failed to connect: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
