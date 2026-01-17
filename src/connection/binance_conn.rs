use crate::connection::connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, info, warn};
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

///为了支持send，BinanceFuturesConnection的成员需要支持send ---> MktConnection需要send
pub struct BinanceConnection {
    base_connection: MktConnection,
    delay_interval: Duration,
    ping_interval: Duration,
}

impl BinanceConnection {
    pub fn new(connection: MktConnection) -> Self {
        Self {
            base_connection: connection,
            delay_interval: Duration::from_secs(5),
            ping_interval: Duration::from_secs(180),
        }
    }
}

#[async_trait]
impl MktConnectionRunner for BinanceConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        let mut ping_send_timer =
            Instant::now() + Duration::from_secs(180) + Duration::from_secs(5);
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
                        ws_stream.close(None).await?; // 发送 CLOSE 帧
                        return Ok(());
                    }
                }
                // ====处理ping超时====
                _ = time::sleep_until(ping_send_timer) => {
                    warn!("Binance-futures: Ping timeout detected. reset connecting...");
                    ws_stream.close(None).await?; // 发送 CLOSE 帧
                    break;
                }
                // ====处理ws消息====
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Ping(payload) => {
                                    debug!("Sent pong message to server: {:?}", payload);
                                    if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                        error!("Failed to send pong message: {:?}", e);
                                        break;
                                    }
                                    ping_send_timer = Instant::now() + self.ping_interval + self.delay_interval;
                                    debug!("Reset ping_send_timer to {:?}", ping_send_timer);
                                }
                                Message::Close(frame) => {
                                    warn!("Received close frame: {:?}", frame);
                                    if let Some(close_frame) = &frame {
                                        if close_frame.reason == "Invalid request" {
                                            error!("Received Invalid request close frame!");
                                            error!("Subscription message was: {}", self.base_connection.sub_msg);
                                            std::process::exit(1);
                                        }
                                    }
                                    break;
                                }
                                Message::Text(text) => {
                                    let bytes = Bytes::from(text.into_bytes());
                                    if let Err(e) = self.base_connection.tx.send(bytes.clone()) {
                                        //利用shutdown关闭
                                        error!("failed to broadcast message: {}", e);
                                        break;
                                    }
                                }
                                Message::Binary(data) => {
                                    let bytes = Bytes::from(data);
                                    if let Err(e) = self.base_connection.tx.send(bytes.clone()) {
                                        error!("failed to broadcast message: {}", e);
                                        break;
                                    }
                                }
                                _ => {
                                    warn!("Received other message type: {:?}", msg);
                                }
                            }
                        }
                        Err(e) => {
                            error!("WebSocket error: {:?}", e);
                            break;
                        }
                        Ok(None) => {
                            warn!("WebSocket connection closed by server");
                            break;
                        }
                    }
                }
            }
        }
        return Ok(());
    }
}

#[async_trait]
impl MktConnectionHandler for BinanceConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        let use_sbe = self.base_connection.url.contains("stream-sbe.binance.com");
        let api_key = if use_sbe {
            std::env::var("BINANCE_SBE_API_KEY")
                .or_else(|_| std::env::var("BINANCE_API_KEY"))
                .ok()
        } else {
            None
        };
        if use_sbe && api_key.is_none() {
            error!("SBE requires API key. Set BINANCE_SBE_API_KEY (or BINANCE_API_KEY).");
            return Err(anyhow::anyhow!(
                "BINANCE_SBE_API_KEY or BINANCE_API_KEY not set for SBE connection"
            ));
        }
        let headers = api_key.map(|key| vec![("X-MBX-APIKEY".to_string(), key)]);

        loop {
            let connect_result = if let Some(ref local_ip) = self.base_connection.local_ip {
                if let Some(ref header_pairs) = headers {
                    WsConnector::connect_with_local_ip_and_headers(
                        &self.base_connection.url,
                        &self.base_connection.sub_msg,
                        local_ip,
                        header_pairs,
                    )
                    .await
                } else {
                    WsConnector::connect_with_local_ip(
                        &self.base_connection.url,
                        &self.base_connection.sub_msg,
                        local_ip,
                    )
                    .await
                }
            } else if let Some(ref header_pairs) = headers {
                WsConnector::connect_with_headers(
                    &self.base_connection.url,
                    &self.base_connection.sub_msg,
                    header_pairs,
                )
                .await
            } else {
                WsConnector::connect(&self.base_connection.url, &self.base_connection.sub_msg).await
            };

            match connect_result {
                Ok(connection) => {
                    debug!(
                        "successfully connected to Binance Futures at {:?}",
                        connection.connected_at
                    );
                    self.base_connection.connection = Some(connection);
                    self.run_connection().await?;
                    //检查shutdown的当前情况，如果是true则break
                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    } else {
                        info!("Connection closed, reconnecting...");
                    }
                }
                Err(e) => {
                    error!("Failed to connect to binance-futures: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
