//! Bitget UTA 用户数据流 WebSocket 连接处理器
//!
//! 私有频道流程：
//! 1. 连接后发送 login 消息鉴权
//! 2. 登录成功后发送订阅消息
//! 3. 每 25 秒发送应用层 ping（字符串 "ping"）并等待 "pong"

use crate::connection::connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use crate::portfolio_margin::bitget_auth::BitgetCredentials;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, info, warn};
use std::time::Duration;
use tokio::time::{self, Instant};
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone, PartialEq)]
enum ConnectionState {
    LoggingIn,
    Subscribing,
    Running,
}

pub struct BitgetUserDataConnection {
    base_connection: MktConnection,
    credentials: BitgetCredentials,
    subscribe_messages: Vec<serde_json::Value>,
    session_max: Option<Duration>,
    restart_count: u32,
}

const BITGET_PRIVATE_PING_INTERVAL: Duration = Duration::from_secs(25);

impl BitgetUserDataConnection {
    pub fn new(
        connection: MktConnection,
        credentials: BitgetCredentials,
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

    fn ws_code_is_success(v: Option<&serde_json::Value>) -> bool {
        match v {
            Some(serde_json::Value::String(s)) => s == "0" || s == "00000",
            Some(serde_json::Value::Number(n)) => n.as_i64() == Some(0),
            _ => false,
        }
    }

    fn is_login_response(text: &str) -> Option<bool> {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
            if json.get("event").and_then(|v| v.as_str()) == Some("login") {
                return Some(Self::ws_code_is_success(json.get("code")));
            }
        }
        None
    }

    fn is_subscribe_response(text: &str) -> Option<bool> {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
            if json.get("event").and_then(|v| v.as_str()) == Some("subscribe") {
                return Some(Self::ws_code_is_success(json.get("code")));
            }
        }
        None
    }

    fn is_pong_response(text: &str) -> bool {
        text == "pong"
    }
}

#[async_trait]
impl MktConnectionRunner for BitgetUserDataConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        let mut state = ConnectionState::LoggingIn;
        let mut ping_timer = Instant::now() + BITGET_PRIVATE_PING_INTERVAL;
        let mut waiting_pong = false;
        let connected_at = Instant::now();
        let mut pending_subscriptions = self.subscribe_messages.len();

        loop {
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
                        ping_timer = Instant::now() + BITGET_PRIVATE_PING_INTERVAL;
                        continue;
                    }

                    if waiting_pong {
                        warn!("Bitget: ping timeout detected, reconnecting...");
                        ws_stream.close(None).await?;
                        break;
                    }

                    if let Err(e) = ws_stream.send(Message::Text("ping".to_string())).await {
                        error!("Bitget: failed to send ping: {:?}", e);
                        break;
                    }
                    waiting_pong = true;
                    ping_timer = Instant::now() + BITGET_PRIVATE_PING_INTERVAL;
                }
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Text(text) => {
                                    if Self::is_pong_response(&text) {
                                        waiting_pong = false;
                                        debug!("Bitget: received pong");
                                        continue;
                                    }

                                    match state {
                                        ConnectionState::LoggingIn => {
                                            if let Some(success) = Self::is_login_response(&text) {
                                                if success {
                                                    info!("Bitget: login successful");
                                                    for sub_msg in &self.subscribe_messages {
                                                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                                                            error!("Bitget: failed to send subscribe message: {:?}", e);
                                                            break;
                                                        }
                                                    }
                                                    state = ConnectionState::Subscribing;
                                                } else {
                                                    error!("Bitget: login failed: {}", text);
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
                                                        ping_timer = Instant::now() + BITGET_PRIVATE_PING_INTERVAL;
                                                        info!("Bitget: all subscriptions complete");
                                                    }
                                                } else {
                                                    warn!("Bitget: subscribe failed: {}", text);
                                                }
                                            }
                                        }
                                        ConnectionState::Running => {
                                            let bytes = Bytes::from(text.into_bytes());
                                            if let Err(e) = self.base_connection.tx.send(bytes) {
                                                error!("Bitget: failed to broadcast message: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                                Message::Ping(payload) => {
                                    debug!("Bitget: received protocol ping");
                                    if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                        error!("Bitget: failed to send pong: {:?}", e);
                                        break;
                                    }
                                }
                                Message::Close(frame) => {
                                    warn!("Bitget: received close frame: {:?}", frame);
                                    break;
                                }
                                _ => {}
                            }
                        }
                        Err(e) => {
                            self.restart_count += 1;
                            error!("Bitget: websocket error (restart count={}): {:?}", self.restart_count, e);
                            break;
                        }
                        Ok(None) => {
                            warn!("Bitget: websocket closed by server");
                            break;
                        }
                    }
                }
            }

            if let Some(max_age) = self.session_max {
                if connected_at.elapsed() >= max_age {
                    info!("Bitget: session max age reached, reconnecting");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl MktConnectionHandler for BitgetUserDataConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        loop {
            info!("Bitget: connecting to {}", &self.base_connection.url);

            let connect_result = if let Some(ref local_ip) = self.base_connection.local_ip {
                WsConnector::connect_with_local_ip_raw(&self.base_connection.url, local_ip).await
            } else {
                WsConnector::connect_raw(&self.base_connection.url).await
            };

            match connect_result {
                Ok(connection) => {
                    self.base_connection.connection = Some(connection);

                    {
                        let login_msg = self.credentials.build_login_message();
                        let mut ws_stream = self
                            .base_connection
                            .connection
                            .as_mut()
                            .unwrap()
                            .ws_stream
                            .lock()
                            .await;

                        if let Err(e) = ws_stream.send(Message::Text(login_msg.to_string())).await {
                            error!("Bitget: failed to send login message: {:?}", e);
                            continue;
                        }
                        info!("Bitget: login message sent");
                    }

                    self.run_connection().await?;

                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    }

                    info!(
                        "Bitget: connection closed, reconnecting... (restart count={})",
                        self.restart_count
                    );
                    time::sleep(Duration::from_secs(2)).await;
                }
                Err(e) => {
                    error!("Bitget: failed to connect: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
