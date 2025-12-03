use crate::connection::connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, info, warn};
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

// Gate.io WebSocket
// https://www.gate.io/docs/developers/apiv4/ws/zh_CN/
//
// 连接地址:
// - 现货: wss://api.gateio.ws/ws/v4/
// - 合约 (USDT): wss://fx-ws.gateio.ws/v4/ws/usdt
// - 合约 (BTC): wss://fx-ws.gateio.ws/v4/ws/btc
//
// 心跳机制 (双向):
// 1. 服务器 → 客户端: 服务器发送协议层 Ping frame，客户端必须回复 Pong frame
// 2. 客户端 → 服务器: 客户端主动发送应用层 JSON ping，重置服务器超时计时器
//
// 应用层 Ping 格式:
// - 现货: {"time": 1234567890, "channel": "spot.ping"}
// - 合约: {"time": 1234567890, "channel": "futures.ping"}
//
// 应用层 Pong 响应:
// - 现货: {"time": 1234567890, "channel": "spot.pong", "event": "", "result": null}
// - 合约: {"time": 1234567890, "channel": "futures.pong", "event": "", "result": null}

pub struct GateConnection {
    base_connection: MktConnection,
    restart_count: u32,
    is_futures: bool, // 区分现货/合约，用于构建 ping channel
}

impl GateConnection {
    pub fn new(connection: MktConnection, is_futures: bool) -> Self {
        Self {
            base_connection: connection,
            restart_count: 0,
            is_futures,
        }
    }
}

/// 构建应用层 ping 消息
fn build_ping_message(is_futures: bool) -> String {
    let channel = if is_futures {
        "futures.ping"
    } else {
        "spot.ping"
    };
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!(r#"{{"time": {}, "channel": "{}"}}"#, timestamp, channel)
}

/// 检查是否为应用层 pong 响应
fn is_pong_response(text: &str) -> bool {
    text.contains("spot.pong") || text.contains("futures.pong")
}

#[async_trait]
impl MktConnectionRunner for GateConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        // 心跳机制:
        // - 每 25s 发送一次应用层 ping (略小于30s以确保安全)
        // - 收到消息后重置倒计时
        // - 服务器会发送协议层 Ping，需要回复 Pong
        let mut reset_timer: Instant = Instant::now() + Duration::from_secs(25);
        let mut waiting_pong: bool = false;

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
                // ===== 优先处理关闭信号 =====
                _ = self.base_connection.shutdown_rx.changed() => {
                    let should_close = *self.base_connection.shutdown_rx.borrow();
                    if should_close {
                        ws_stream.close(None).await?;
                        return Ok(());
                    }
                }
                // ==== 处理超时 ====
                _ = time::sleep_until(reset_timer) => {
                    if waiting_pong {
                        // 等待 pong 超时，重启连接
                        warn!("Gate: Application ping timeout detected. Reconnecting...");
                        ws_stream.close(None).await?;
                        break;
                    } else {
                        // 发送应用层 ping 消息 (JSON 格式)
                        let ping_msg = build_ping_message(self.is_futures);
                        if let Err(e) = ws_stream.send(Message::Text(ping_msg.clone())).await {
                            error!("Gate: Failed to send ping message: {:?}", e);
                            break;
                        }
                        // 设置倒计时为 25s
                        reset_timer = Instant::now() + Duration::from_secs(25);
                        waiting_pong = true;
                        debug!("Gate: Sent ping: {}, reset timer to {:?}", ping_msg, reset_timer);
                    }
                }
                // ==== 处理 WebSocket 消息 ====
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Ping(payload) => {
                                    // 服务器发送协议层 Ping，必须回复 Pong
                                    debug!("Gate: Received protocol ping, sending pong");
                                    if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                        error!("Gate: Failed to send pong: {:?}", e);
                                        break;
                                    }
                                    // 收到服务器 ping 也重置计时器
                                    if !waiting_pong {
                                        reset_timer = Instant::now() + Duration::from_secs(25);
                                    }
                                }
                                Message::Close(frame) => {
                                    warn!("Gate: Received close frame: {:?}", frame);
                                    break;
                                }
                                Message::Pong(payload) => {
                                    // 协议层 Pong (不太可能收到，因为我们不发协议层 Ping)
                                    debug!("Gate: Received protocol pong: {:?}", payload);
                                }
                                Message::Text(text) => {
                                    // 检查是否是应用层 pong 响应
                                    if is_pong_response(&text) {
                                        waiting_pong = false;
                                        reset_timer = Instant::now() + Duration::from_secs(25);
                                        debug!("Gate: Received application pong, reset timer to {:?}", reset_timer);
                                    } else {
                                        // 正常消息，如果不在等待 pong 状态则重置计时器
                                        if !waiting_pong {
                                            reset_timer = Instant::now() + Duration::from_secs(25);
                                        }
                                        // 广播消息
                                        let bytes = Bytes::from(text.into_bytes());
                                        if let Err(e) = self.base_connection.tx.send(bytes) {
                                            error!("Gate: Failed to broadcast message: {}", e);
                                            break;
                                        }
                                    }
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
                                    warn!("Gate: Received other message type: {:?}", msg);
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
impl MktConnectionHandler for GateConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        loop {
            info!(
                "Gate: Connecting to WebSocket URL: {}",
                &self.base_connection.url
            );

            let connect_result = if let Some(ref local_ip) = self.base_connection.local_ip {
                WsConnector::connect_with_local_ip(
                    &self.base_connection.url,
                    &self.base_connection.sub_msg,
                    local_ip,
                )
                .await
            } else {
                WsConnector::connect(&self.base_connection.url, &self.base_connection.sub_msg).await
            };

            match connect_result {
                Ok(connection) => {
                    debug!(
                        "Gate: Successfully connected at {:?}",
                        connection.connected_at
                    );
                    self.base_connection.connection = Some(connection);
                    self.run_connection().await?;

                    // 检查是否需要关闭
                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    } else {
                        info!(
                            "Gate: Connection closed, reconnecting... (total restart count: {})",
                            self.restart_count
                        );
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
