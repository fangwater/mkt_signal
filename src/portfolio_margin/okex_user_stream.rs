//! OKX 用户数据流 WebSocket 连接处理器
//!
//! 与 Binance 不同，OKX 私有频道需要：
//! 1. 连接后发送 login 消息进行鉴权
//! 2. 登录成功后订阅所需频道
//! 3. 通过应用层 ping 保持连接 (每 25 秒)

use crate::connection::connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use crate::portfolio_margin::okex_auth::OkexCredentials;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, info, warn};
use std::time::Duration;
use tokio::time::{self, Instant};
use tokio_tungstenite::tungstenite::Message;

/// OKX 用户数据流连接状态
#[derive(Debug, Clone, PartialEq)]
enum ConnectionState {
    Connecting,
    LoggingIn,
    Subscribing,
    Running,
}

/// OKX 用户数据流连接处理器
pub struct OkexUserDataConnection {
    base_connection: MktConnection,
    credentials: OkexCredentials,
    subscribe_messages: Vec<serde_json::Value>,
    session_max: Option<Duration>,
    restart_count: u32,
}

impl OkexUserDataConnection {
    pub fn new(
        connection: MktConnection,
        credentials: OkexCredentials,
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

    /// 检查消息是否为登录响应
    fn is_login_response(text: &str) -> Option<bool> {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
            if json.get("event").and_then(|v| v.as_str()) == Some("login") {
                let code = json.get("code").and_then(|v| v.as_str()).unwrap_or("");
                return Some(code == "0");
            }
        }
        None
    }

    /// 检查消息是否为订阅响应
    fn is_subscribe_response(text: &str) -> Option<bool> {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
            if json.get("event").and_then(|v| v.as_str()) == Some("subscribe") {
                // OKX 订阅成功时没有 code 字段，或 code 为 "0"
                let code = json.get("code").and_then(|v| v.as_str());
                return Some(code.is_none() || code == Some("0"));
            }
        }
        None
    }

    /// 检查是否为 pong 响应
    fn is_pong_response(text: &str) -> bool {
        text == "pong"
    }

    /// 检查是否为错误消息
    fn is_error_message(text: &str) -> Option<String> {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
            if json.get("event").and_then(|v| v.as_str()) == Some("error") {
                let code = json
                    .get("code")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let msg = json
                    .get("msg")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                return Some(format!("code={}, msg={}", code, msg));
            }
        }
        None
    }
}

#[async_trait]
impl MktConnectionRunner for OkexUserDataConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        let mut state = ConnectionState::Connecting;
        let mut ping_timer = Instant::now() + Duration::from_secs(25);
        let mut waiting_pong = false;
        let session_start = Instant::now();
        let mut pending_subscriptions = self.subscribe_messages.len();

        loop {
            // 检查会话时长限制
            if let Some(max_duration) = self.session_max {
                if session_start.elapsed() >= max_duration {
                    info!("OKX: Session max duration reached, reconnecting...");
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
                // 处理关闭信号
                _ = self.base_connection.shutdown_rx.changed() => {
                    let should_close = *self.base_connection.shutdown_rx.borrow();
                    if should_close {
                        ws_stream.close(None).await?;
                        return Ok(());
                    }
                }

                // 处理 ping 定时器
                _ = time::sleep_until(ping_timer) => {
                    if state != ConnectionState::Running {
                        // 还在握手阶段，跳过 ping
                        ping_timer = Instant::now() + Duration::from_secs(25);
                        continue;
                    }

                    if waiting_pong {
                        warn!("OKX: Ping timeout, reconnecting...");
                        ws_stream.close(None).await?;
                        break;
                    } else {
                        // 发送 ping
                        if let Err(e) = ws_stream.send(Message::Text("ping".to_string())).await {
                            error!("OKX: Failed to send ping: {:?}", e);
                            break;
                        }
                        waiting_pong = true;
                        ping_timer = Instant::now() + Duration::from_secs(25);
                        debug!("OKX: Sent ping");
                    }
                }

                // 处理 WebSocket 消息
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Text(text) => {
                                    // 检查 pong
                                    if Self::is_pong_response(&text) {
                                        waiting_pong = false;
                                        ping_timer = Instant::now() + Duration::from_secs(25);
                                        debug!("OKX: Received pong");
                                        continue;
                                    }

                                    // 检查错误
                                    if let Some(err_msg) = Self::is_error_message(&text) {
                                        error!("OKX: Received error: {}", err_msg);
                                        // 根据状态决定是否断开
                                        if state == ConnectionState::LoggingIn {
                                            error!("OKX: Login failed, will retry...");
                                            break;
                                        }
                                        continue;
                                    }

                                    match state {
                                        ConnectionState::Connecting => {
                                            // 不应该在这个状态收到消息
                                            warn!("OKX: Unexpected message in Connecting state: {}", text);
                                        }
                                        ConnectionState::LoggingIn => {
                                            // 等待登录响应
                                            if let Some(success) = Self::is_login_response(&text) {
                                                if success {
                                                    info!("OKX: Login successful");
                                                    state = ConnectionState::Subscribing;

                                                    // 发送所有订阅消息
                                                    for sub_msg in &self.subscribe_messages {
                                                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                                                            error!("OKX: Failed to send subscribe: {:?}", e);
                                                            break;
                                                        }
                                                        debug!("OKX: Sent subscribe: {}", sub_msg);
                                                    }
                                                } else {
                                                    error!("OKX: Login failed");
                                                    break;
                                                }
                                            }
                                        }
                                        ConnectionState::Subscribing => {
                                            // 等待订阅响应
                                            if let Some(success) = Self::is_subscribe_response(&text) {
                                                if success {
                                                    pending_subscriptions = pending_subscriptions.saturating_sub(1);
                                                    info!("OKX: Subscribe successful, remaining: {}", pending_subscriptions);

                                                    if pending_subscriptions == 0 {
                                                        info!("OKX: All subscriptions complete, entering running state");
                                                        state = ConnectionState::Running;
                                                        ping_timer = Instant::now() + Duration::from_secs(25);
                                                    }
                                                } else {
                                                    warn!("OKX: Subscribe failed: {}", text);
                                                }
                                            } else {
                                                // 可能已经开始收到数据了
                                                debug!("OKX: Received data while subscribing: {}", text);
                                            }
                                        }
                                        ConnectionState::Running => {
                                            // 正常运行，重置 ping 定时器
                                            if !waiting_pong {
                                                ping_timer = Instant::now() + Duration::from_secs(25);
                                            }

                                            // 广播消息
                                            let bytes = Bytes::from(text.into_bytes());
                                            if let Err(e) = self.base_connection.tx.send(bytes) {
                                                error!("OKX: Failed to broadcast message: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                                Message::Ping(payload) => {
                                    debug!("OKX: Received protocol ping");
                                    if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                        error!("OKX: Failed to send pong: {:?}", e);
                                        break;
                                    }
                                }
                                Message::Close(frame) => {
                                    warn!("OKX: Received close frame: {:?}", frame);
                                    break;
                                }
                                _ => {
                                    debug!("OKX: Received other message type");
                                }
                            }
                        }
                        Err(e) => {
                            self.restart_count += 1;
                            error!("OKX: WebSocket error (restart count: {}): {:?}", self.restart_count, e);
                            break;
                        }
                        Ok(None) => {
                            warn!("OKX: WebSocket connection closed by server");
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
impl MktConnectionHandler for OkexUserDataConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        loop {
            info!("OKX: Connecting to {}", &self.base_connection.url);

            // 连接 WebSocket (不发送订阅消息，因为需要先登录)
            let connect_result = if let Some(ref local_ip) = self.base_connection.local_ip {
                WsConnector::connect_with_local_ip_raw(&self.base_connection.url, local_ip).await
            } else {
                WsConnector::connect_raw(&self.base_connection.url).await
            };

            match connect_result {
                Ok(connection) => {
                    debug!("OKX: Connected at {:?}", connection.connected_at);
                    self.base_connection.connection = Some(connection);

                    // 发送登录消息
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
                            error!("OKX: Failed to send login message: {:?}", e);
                            continue;
                        }
                        info!("OKX: Login message sent");
                    }

                    // 运行连接
                    self.run_connection().await?;

                    // 检查是否需要关闭
                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    } else {
                        info!(
                            "OKX: Connection closed, reconnecting... (total restart count: {})",
                            self.restart_count
                        );
                        // 短暂等待后重连
                        time::sleep(Duration::from_secs(2)).await;
                    }
                }
                Err(e) => {
                    error!("OKX: Failed to connect: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
