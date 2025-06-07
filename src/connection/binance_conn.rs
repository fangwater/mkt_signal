use futures_util::{SinkExt, TryStreamExt};
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use bytes::Bytes;
use log::{info, warn, error};
use async_trait::async_trait;
use crate::connection::connection::{MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector};

///为了支持send，BinanceFuturesConnection的成员需要支持send ---> MktConnection需要send
pub struct BinanceConnection {
    base_connection: MktConnection,
    ping_send_timer: Instant,
    delay_interval: Duration,
    ping_interval: Duration,
}

impl BinanceConnection {
    pub fn new(connection: MktConnection) -> Self {
        let ping_send_timer_init = Instant::now() + Duration::from_secs(180) + Duration::from_secs(5);
        Self {
            base_connection: connection,
            delay_interval: Duration::from_secs(5),
            ping_interval: Duration::from_secs(180),
            ping_send_timer: ping_send_timer_init,
        }
    }
}

#[async_trait]
impl MktConnectionRunner for BinanceConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        loop {
            let mut ws_stream = self.base_connection.connection.as_mut().unwrap().ws_stream.lock().await;
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
                _ = time::sleep_until(self.ping_send_timer) => {
                    warn!("Binance-futures: Ping timeout detected. reset connecting...");
                    break;
                }
                // ====处理ws消息====
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Ping(payload) => {
                                    info!("Sent pong message to server: {:?}", payload);
                                    if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                        error!("Failed to send pong message: {:?}", e);
                                        break;
                                    }
                                    self.ping_send_timer = Instant::now() + self.ping_interval + self.delay_interval;
                                    info!("Reset ping_send_timer to {:?}", self.ping_send_timer);
                                }
                                Message::Close(frame) => {
                                    warn!("Received close frame: {:?}", frame);
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
        loop {
            match WsConnector::connect(&self.base_connection.url, &self.base_connection.sub_msg).await {
                Ok(connection) => {
                    info!("successfully connected to Binance Futures at {:?}", connection.connected_at);
                    self.base_connection.connection = Some(connection);
                    self.run_connection().await?;
                    //检查shutdown的当前情况，如果是true则break
                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    }else{
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
