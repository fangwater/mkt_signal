use crate::connection::connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, info, warn};
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

// Bitget WebSocket
// https://www.bitget.com/api-doc/common/websocket-intro
// 公共频道: wss://ws.bitget.com/v2/ws/public
// 私有频道: wss://ws.bitget.com/v2/ws/private
//
// 心跳机制：
// - 用户设置一个定时器，每30秒发送字符串"ping"
// - 期待一个字符串"pong"作为回应
// - 如果未收到字符串"pong"响应，请重新连接
// - 如果2分钟内服务端没收到"ping"，会自动断开连接
//
// 限制：
// - 每秒每连接最多接受10个消息
// - 单个连接不要订阅超过50个频道

pub struct BitgetConnection {
    base_connection: MktConnection,
    restart_count: u32,
}

impl BitgetConnection {
    pub fn new(connection: MktConnection) -> Self {
        Self {
            base_connection: connection,
            restart_count: 0,
        }
    }
}

#[async_trait]
impl MktConnectionRunner for BitgetConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        // 心跳机制类似 OKEx：
        // - 初始设置25s倒计时（略小于30s以确保安全）
        // - 收到消息后重置倒计时
        // - 倒计时结束发送 "ping"，等待 "pong"
        // - 如果在下一个25s内没收到 "pong"，重启连接
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
                        warn!("Bitget: Ping timeout detected. Reconnecting...");
                        ws_stream.close(None).await?;
                        break;
                    } else {
                        // 发送 ping 消息（Bitget 使用文本 "ping"）
                        if let Err(e) = ws_stream.send(Message::Text("ping".to_string())).await {
                            error!("Bitget: Failed to send ping message: {:?}", e);
                            break;
                        }
                        // 设置倒计时为25s
                        reset_timer = Instant::now() + Duration::from_secs(25);
                        waiting_pong = true;
                        debug!("Bitget: Sent ping, reset timer to {:?}", reset_timer);
                    }
                }
                // ==== 处理 WebSocket 消息 ====
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Ping(payload) => {
                                    // Bitget 不应发送 Ping frame，记录警告
                                    warn!("Bitget: Unexpected ping frame: {:?}", payload);
                                }
                                Message::Close(frame) => {
                                    warn!("Bitget: Received close frame: {:?}", frame);
                                    break;
                                }
                                Message::Pong(payload) => {
                                    // Bitget 使用文本 pong，不是 Pong frame
                                    warn!("Bitget: Unexpected pong frame: {:?}", payload);
                                }
                                Message::Text(text) => {
                                    // 检查是否是 pong 响应
                                    if text == "pong" {
                                        waiting_pong = false;
                                        reset_timer = Instant::now() + Duration::from_secs(25);
                                        debug!("Bitget: Received pong, reset timer to {:?}", reset_timer);
                                    } else {
                                        // 正常消息，如果不在等待 pong 状态则重置计时器
                                        if !waiting_pong {
                                            reset_timer = Instant::now() + Duration::from_secs(25);
                                        }
                                        // 广播消息
                                        let bytes = Bytes::from(text.into_bytes());
                                        if let Err(e) = self.base_connection.tx.send(bytes) {
                                            error!("Bitget: Failed to broadcast message: {}", e);
                                            break;
                                        }
                                    }
                                }
                                Message::Binary(data) => {
                                    // Bitget 也可能发送二进制消息
                                    let bytes = Bytes::from(data);
                                    if let Err(e) = self.base_connection.tx.send(bytes) {
                                        error!("Bitget: Failed to broadcast binary message: {}", e);
                                        break;
                                    }
                                }
                                _ => {
                                    warn!("Bitget: Received other message type: {:?}", msg);
                                }
                            }
                        }
                        Err(e) => {
                            self.restart_count += 1;
                            error!("Bitget: WebSocket error (restart count: {}): {:?}", self.restart_count, e);
                            break;
                        }
                        Ok(None) => {
                            warn!("Bitget: WebSocket connection closed by server");
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
impl MktConnectionHandler for BitgetConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        loop {
            info!(
                "Bitget: Connecting to WebSocket URL: {}",
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
                        "Bitget: Successfully connected at {:?}",
                        connection.connected_at
                    );
                    self.base_connection.connection = Some(connection);
                    self.run_connection().await?;

                    // 检查是否需要关闭
                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    } else {
                        info!(
                            "Bitget: Connection closed, reconnecting... (total restart count: {})",
                            self.restart_count
                        );
                    }
                }
                Err(e) => {
                    error!("Bitget: Failed to connect: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
