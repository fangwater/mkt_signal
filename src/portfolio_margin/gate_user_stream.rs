//! Gate.io 用户数据流 WebSocket 连接处理器
//!
//! Gate.io 统一账户的私有频道特点：
//! 1. 连接后直接发送带鉴权的订阅消息
//! 2. Gate WebSocket 使用协议层 ping/pong 消息（服务器发起）
//! 3. 如果客户端想主动检测连接状态，可以发送应用层 ping 消息
//!    对于统一账户，使用 "unified.ping" 频道

use crate::connection::connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use crate::portfolio_margin::gate_auth::GateCredentials;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, info, warn};
use std::time::Duration;
use tokio::time::{self, Instant};
use tokio_tungstenite::tungstenite::Message;

/// Gate.io 用户数据流连接状态
#[derive(Debug, Clone, PartialEq)]
enum ConnectionState {
    Subscribing,
    Running,
}

/// 订阅频道配置
#[derive(Clone)]
pub struct SubscribeChannel {
    pub channel: String,
    pub payload: Vec<String>,
}

/// Gate.io 用户数据流连接处理器
pub struct GateUserDataConnection {
    base_connection: MktConnection,
    credentials: GateCredentials,
    /// 要订阅的频道列表
    channels: Vec<SubscribeChannel>,
    session_max: Option<Duration>,
    restart_count: u32,
    /// ping 频道名称 (unified.ping / futures.ping / spot.ping)
    ping_channel: String,
}

impl GateUserDataConnection {
    pub fn new(
        connection: MktConnection,
        credentials: GateCredentials,
        channels: Vec<SubscribeChannel>,
        session_max: Option<Duration>,
    ) -> Self {
        // 根据订阅的频道推断 ping channel
        let ping_channel = Self::infer_ping_channel(&channels);
        Self {
            base_connection: connection,
            credentials,
            channels,
            session_max,
            restart_count: 0,
            ping_channel,
        }
    }

    /// 根据订阅的频道推断应该使用的 ping channel
    fn infer_ping_channel(channels: &[SubscribeChannel]) -> String {
        for ch in channels {
            if ch.channel.starts_with("unified.") {
                return "unified.ping".to_string();
            } else if ch.channel.starts_with("futures.") {
                return "futures.ping".to_string();
            } else if ch.channel.starts_with("spot.") {
                return "spot.ping".to_string();
            }
        }
        // 默认使用 unified.ping
        "unified.ping".to_string()
    }

    /// 生成订阅消息（每次重连时调用，确保签名时效性）
    fn build_subscribe_messages(&self) -> Vec<serde_json::Value> {
        self.channels
            .iter()
            .map(|ch| {
                let payload: Vec<&str> = ch.payload.iter().map(|s| s.as_str()).collect();
                self.credentials
                    .build_subscribe_message(&ch.channel, payload)
            })
            .collect()
    }

    /// 构建应用层 ping 消息
    fn build_ping_message(ping_channel: &str) -> String {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        format!(
            r#"{{"time": {}, "channel": "{}"}}"#,
            timestamp, ping_channel
        )
    }

    /// 检查是否为 pong 响应
    fn is_pong_response(text: &str) -> bool {
        text.contains("unified.pong") || text.contains("spot.pong") || text.contains("futures.pong")
    }

    /// 检查是否为订阅响应
    fn is_subscribe_response(text: &str) -> Option<bool> {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
            if json.get("event").and_then(|v| v.as_str()) == Some("subscribe") {
                // Gate.io 订阅成功时 result 中包含 status: "success"
                if let Some(result) = json.get("result") {
                    if let Some(status) = result.get("status").and_then(|v| v.as_str()) {
                        return Some(status == "success");
                    }
                }
                // 有些响应直接返回空 result，也视为成功
                return Some(true);
            }
        }
        None
    }

    /// 检查是否为错误消息
    fn is_error_message(text: &str) -> Option<String> {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
            if let Some(error) = json.get("error") {
                let code = error.get("code").and_then(|v| v.as_i64()).unwrap_or(0);
                let msg = error
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                return Some(format!("code={}, msg={}", code, msg));
            }
        }
        None
    }
}

#[async_trait]
impl MktConnectionRunner for GateUserDataConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        let mut state = ConnectionState::Subscribing;
        let mut ping_timer = Instant::now() + Duration::from_secs(25);
        let mut waiting_pong = false;
        let session_start = Instant::now();
        let mut pending_subscriptions = self.channels.len();
        let ping_channel = self.ping_channel.clone();

        loop {
            // 检查会话时长限制
            if let Some(max_duration) = self.session_max {
                if session_start.elapsed() >= max_duration {
                    info!("Gate: Session max duration reached, reconnecting...");
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
                        warn!("Gate: Ping timeout, reconnecting...");
                        ws_stream.close(None).await?;
                        break;
                    } else {
                        // 发送应用层 ping
                        let ping_msg = Self::build_ping_message(&ping_channel);
                        if let Err(e) = ws_stream.send(Message::Text(ping_msg.clone())).await {
                            error!("Gate: Failed to send ping: {:?}", e);
                            break;
                        }
                        waiting_pong = true;
                        ping_timer = Instant::now() + Duration::from_secs(25);
                        debug!("Gate: Sent ping: {}", ping_msg);
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
                                        debug!("Gate: Received pong");
                                        continue;
                                    }

                                    // 检查错误
                                    if let Some(err_msg) = Self::is_error_message(&text) {
                                        error!("Gate: Received error: {}", err_msg);
                                        if state == ConnectionState::Subscribing {
                                            error!("Gate: Subscribe failed, will retry...");
                                            break;
                                        }
                                        continue;
                                    }

                                    match state {
                                        ConnectionState::Subscribing => {
                                            // 等待订阅响应
                                            if let Some(success) = Self::is_subscribe_response(&text) {
                                                if success {
                                                    pending_subscriptions = pending_subscriptions.saturating_sub(1);
                                                    info!("Gate: Subscribe successful, remaining: {}", pending_subscriptions);

                                                    if pending_subscriptions == 0 {
                                                        info!("Gate: All subscriptions complete, entering running state");
                                                        state = ConnectionState::Running;
                                                        ping_timer = Instant::now() + Duration::from_secs(25);
                                                    }
                                                } else {
                                                    warn!("Gate: Subscribe failed: {}", text);
                                                }
                                            } else {
                                                // 可能已经开始收到数据了
                                                debug!("Gate: Received data while subscribing: {}", text);
                                                // 广播消息
                                                let bytes = Bytes::from(text.into_bytes());
                                                if let Err(e) = self.base_connection.tx.send(bytes) {
                                                    error!("Gate: Failed to broadcast message: {}", e);
                                                    break;
                                                }
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
                                                error!("Gate: Failed to broadcast message: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                                Message::Ping(payload) => {
                                    // 协议层 Ping，回复 Pong
                                    debug!("Gate: Received protocol ping");
                                    if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                        error!("Gate: Failed to send pong: {:?}", e);
                                        break;
                                    }
                                    // 收到服务器 ping 也重置计时器
                                    if !waiting_pong {
                                        ping_timer = Instant::now() + Duration::from_secs(25);
                                    }
                                }
                                Message::Close(frame) => {
                                    warn!("Gate: Received close frame: {:?}", frame);
                                    break;
                                }
                                Message::Binary(data) => {
                                    // Gate 也可能发送二进制消息
                                    let bytes = Bytes::from(data);
                                    if let Err(e) = self.base_connection.tx.send(bytes) {
                                        error!("Gate: Failed to broadcast binary message: {}", e);
                                        break;
                                    }
                                }
                                _ => {
                                    debug!("Gate: Received other message type");
                                }
                            }
                        }
                        Err(e) => {
                            self.restart_count += 1;
                            error!("Gate: WebSocket error (restart count: {}): {:?}", self.restart_count, e);
                            break;
                        }
                        Ok(None) => {
                            warn!("Gate: WebSocket connection closed by server");
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
impl MktConnectionHandler for GateUserDataConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        loop {
            info!("Gate: Connecting to {}", &self.base_connection.url);

            // 连接 WebSocket
            let connect_result = if let Some(ref local_ip) = self.base_connection.local_ip {
                WsConnector::connect_with_local_ip_raw(&self.base_connection.url, local_ip).await
            } else {
                WsConnector::connect_raw(&self.base_connection.url).await
            };

            match connect_result {
                Ok(connection) => {
                    debug!("Gate: Connected at {:?}", connection.connected_at);
                    self.base_connection.connection = Some(connection);

                    // 每次重连时重新生成订阅消息（确保签名时效性）
                    let subscribe_messages = self.build_subscribe_messages();

                    // 发送订阅消息 (Gate.io 订阅消息中包含鉴权信息)
                    {
                        let mut ws_stream = self
                            .base_connection
                            .connection
                            .as_mut()
                            .unwrap()
                            .ws_stream
                            .lock()
                            .await;

                        for sub_msg in &subscribe_messages {
                            if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await
                            {
                                error!("Gate: Failed to send subscribe message: {:?}", e);
                                break;
                            }
                            info!("Gate: Subscribe message sent: {}", sub_msg);
                        }
                    }

                    // 运行连接
                    self.run_connection().await?;

                    // 检查是否需要关闭
                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    } else {
                        info!(
                            "Gate: Connection closed, reconnecting... (total restart count: {})",
                            self.restart_count
                        );
                        // 短暂等待后重连
                        time::sleep(Duration::from_secs(2)).await;
                    }
                }
                Err(e) => {
                    error!("Gate: Failed to connect: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
