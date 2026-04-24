//! 币安统一账户（PM）用户数据 WebSocket 连接
//!
//! 特点：
//! - 不需要发送订阅报文，URL 包含 listenKey 即可鉴权；
//! - 维护 ping/pong 与错误处理，关闭/异常后自动重连；
//! - 支持按 UTC 2 小时边界错峰主动断开重连（小于 24 小时）；
//! - 复用现有 `MktConnection`/`WsConnector` 基础设施，仅跳过订阅报文发送。
//!
//! 使用：创建 `MktConnection` 时将 `url` 设置为 `.../pm/ws/<listenKey>`，再用
//! `BinanceUserDataConnection::new(connection, restart_policy)` 启动连接。
use crate::connection::connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, warn};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

const BINANCE_SESSION_BOUNDARY_SECS: u64 = 2 * 3600;

#[derive(Clone, Copy, Debug)]
pub enum SessionRestartPolicy {
    OddTwoHourBoundary,
    EvenTwoHourBoundary,
}

impl SessionRestartPolicy {
    fn target_parity(self) -> u64 {
        match self {
            SessionRestartPolicy::EvenTwoHourBoundary => 0,
            SessionRestartPolicy::OddTwoHourBoundary => 1,
        }
    }

    fn next_deadline(self) -> Instant {
        let now_instant = Instant::now();
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut boundary_bucket = now_secs / BINANCE_SESSION_BOUNDARY_SECS + 1;
        while boundary_bucket % 2 != self.target_parity() {
            boundary_bucket += 1;
        }
        let deadline_secs = boundary_bucket * BINANCE_SESSION_BOUNDARY_SECS;
        now_instant + Duration::from_secs(deadline_secs.saturating_sub(now_secs))
    }
}

/// Binance user data stream connection using listenKey URL. No subscribe message is sent.
pub struct BinanceUserDataConnection {
    base_connection: MktConnection,
    delay_interval: Duration,
    ping_interval: Duration,
    restart_policy: Option<SessionRestartPolicy>,
}

impl BinanceUserDataConnection {
    pub fn new(connection: MktConnection, restart_policy: Option<SessionRestartPolicy>) -> Self {
        Self {
            base_connection: connection,
            delay_interval: Duration::from_secs(5),
            ping_interval: Duration::from_secs(180),
            restart_policy,
        }
    }
}

#[async_trait]
impl MktConnectionRunner for BinanceUserDataConnection {
    async fn run_connection(&mut self) -> Result<()> {
        let mut ping_send_timer = Instant::now() + self.ping_interval + self.delay_interval;
        debug!(
            "[user-ws] entering run loop (ping ~{:?}, restart_policy={:?})",
            self.ping_interval, self.restart_policy
        );
        let mut session_sleep = if let Some(policy) = self.restart_policy {
            Box::pin(time::sleep_until(policy.next_deadline()))
        } else {
            // effectively never fires
            Box::pin(time::sleep(Duration::from_secs(365 * 24 * 3600)))
        };
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
                _ = time::sleep_until(ping_send_timer) => {
                    warn!("Binance user-data: Ping timeout detected; reconnecting...");
                    ws_stream.close(None).await?;
                    break;
                }
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Ping(payload) => {
                                    if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                        error!("Failed to send pong: {:?}", e);
                                        break;
                                    }
                                    ping_send_timer = Instant::now() + self.ping_interval + self.delay_interval;
                                    debug!("[user-ws] pong sent; reset ping timer");
                                }
                                Message::Close(frame) => {
                                    warn!("User-data WS received close: {:?}", frame);
                                    break;
                                }
                                Message::Text(text) => {
                                    let bytes = Bytes::from(text.into_bytes());
                                    if let Err(e) = self.base_connection.tx.send(bytes) {
                                        error!("Broadcast user-data text failed: {}", e);
                                        break;
                                    }
                                }
                                Message::Binary(data) => {
                                    let bytes = Bytes::from(data);
                                    if let Err(e) = self.base_connection.tx.send(bytes) {
                                        error!("Broadcast user-data bin failed: {}", e);
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                        Ok(None) => { // stream closed gracefully
                            debug!("[user-ws] stream closed by server");
                            break;
                        }
                        Err(e) => {
                            error!("User-data WS error: {:?}", e);
                            break;
                        }
                    }
                }
                // session hard limit
                _ = &mut session_sleep => {
                    warn!("User-data session reached scheduled UTC restart boundary; reconnecting...");
                    ws_stream.close(None).await.ok();
                    break;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl MktConnectionHandler for BinanceUserDataConnection {
    async fn start_ws(&mut self) -> Result<()> {
        loop {
            let connect_result = if let Some(local_ip) = &self.base_connection.local_ip {
                WsConnector::connect_with_local_ip_raw(&self.base_connection.url, local_ip).await
            } else {
                WsConnector::connect_raw(&self.base_connection.url).await
            };

            match connect_result {
                Ok(connection) => {
                    debug!(
                        "Connected to Binance user-data: {:?}",
                        connection.connected_at
                    );
                    self.base_connection.connection = Some(connection);
                    self.run_connection().await?;

                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    } else {
                        debug!("User-data connection closed; reconnecting in 5s...");
                        time::sleep(Duration::from_secs(5)).await;
                    }
                }
                Err(e) => {
                    error!("Failed to connect to user-data WS: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
