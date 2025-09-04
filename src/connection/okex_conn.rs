use futures_util::{SinkExt, TryStreamExt};
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use bytes::Bytes;
use log::{info, warn, error};
use async_trait::async_trait;
use crate::connection::connection::{MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector};


// okex
// https://www.okx.com/docs-v5/zh/#overview-websocket-overview
// 如果出现网络问题，系统会自动断开连接
// 如果连接成功后30s未订阅或订阅后30s内服务器未向用户推送数据，系统会自动断开连接
// 为了保持连接有效且稳定，建议您进行以下操作：
// 1. 每次接收到消息后，用户设置一个定时器，定时N秒，N 小于30。
// 2. 如果定时器被触发（N 秒内没有收到新消息），发送字符串 'ping'。
// 3. 期待一个文字字符串'pong'作为回应。如果在 N秒内未收到，请发出错误或重新连接。
pub struct OkexConnection {
    base_connection: MktConnection,
    restart_count: u32,
}

impl OkexConnection {
    pub fn new(connection: MktConnection) -> Self {
        Self {
            base_connection: connection,
            restart_count: 0,
        }
    }
}

#[async_trait]
impl MktConnectionRunner for OkexConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        //简化设计，全局其实只需要一个倒计时器
        //初始阶段设置为25s + now，随新消息刷新
        //当新消息收到后，持续重置倒计时
        //当倒计时结束，发送ping消息，并设置ping-timeout-timer为25s + now
        //在此期间，如果收到新消息也不刷新，必须期待一个pong消息，否则重启websocket
        //因此只需要一个倒计时 + bool waiting_pong 来判断是否在等待pong消息
        let mut reset_timer: Instant = Instant::now() + Duration::from_secs(25); // 倒计时，初始值25s
        let mut waiting_pong: bool = false; // 是否在等待pong消息，初始值false
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
                // ====处理超时====
                _ = time::sleep_until(reset_timer) => {
                    // 如果正在等待pong消息，则重启websocket
                    if waiting_pong {
                        warn!("Okex: Ping timeout detected. reset connecting...");
                        ws_stream.close(None).await?; // 发送 CLOSE 帧
                        break;
                    } else {
                        // 发送ping消息
                        if let Err(e) = ws_stream.send(Message::Ping(b"ping".to_vec())).await {
                            error!("Failed to send ping message: {:?}", e);
                            break;
                        }
                        // 设置倒计时为25s + now
                        reset_timer = Instant::now() + Duration::from_secs(25);
                        // 设置等待pong消息
                        waiting_pong = true;
                        log::info!("Sent ping message: {:?}, reset timer to {:?}", b"ping", reset_timer);
                    }
                }
                // ====处理ws消息====
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Ping(payload) => {
                                    //okex不会收到服务器的ping消息，只能是主动pong，因此不需要处理
                                    warn!("Unexpected ping message: {:?}", payload);
                                }
                                Message::Close(frame) => {
                                    warn!("Received close frame: {:?}", frame);
                                    break;
                                }
                                Message::Pong(payload) => {
                                    warn!("Unexpected pong message: {:?}", payload);
                                }
                                Message::Text(text) => {
                                    // 收期待一个文字字符串'pong'作为回应
                                    if text.eq("pong") {
                                        // 收到pong消息后，等待pong消息设置为false
                                        waiting_pong = false;
                                        // 重置倒计时
                                        reset_timer = Instant::now() + Duration::from_secs(25);
                                        log::info!("Received pong message: {:?}, reset timer to {:?}", text, reset_timer);
                                    }else{
                                        // 收到消息后，如果不是waiting for pong的状态，则重置倒计时
                                        if !waiting_pong {
                                            reset_timer = Instant::now() + Duration::from_secs(25);
                                        }else{
                                            log::warn!("Receive msg when waiting for pong : {}", text);
                                        }
                                        let bytes = Bytes::from(text.into_bytes());
                                        if let Err(e) = self.base_connection.tx.send(bytes.clone()) {
                                            //利用shutdown关闭
                                            error!("failed to broadcast message: {}", e);
                                            break;
                                        }
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
                            self.restart_count += 1;
                            error!("WebSocket error (restart count: {}): {:?}", self.restart_count, e);
                            // Debug: Print detailed error info and exit for troubleshooting
                            error!("OKEx connection failed with detailed info (restart #{}):", self.restart_count);
                            // error!("  URL: {}", &self.base_connection.url);
                            // error!("  Subscription message: {}", serde_json::to_string_pretty(&self.base_connection.sub_msg).unwrap_or_else(|_| "Failed to serialize".to_string()));
                            error!("  Error type: {:?}", e);
                            error!("Exiting for debugging purposes...");
                            
                            // TODO(human) - Add exit mechanism here
                            // You can either:
                            // 1. std::process::exit(1); for immediate exit
                            // 2. return Err(e.into()); to propagate error up
                            // 3. Add conditional debugging flag to control exit behavior
                            
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
impl MktConnectionHandler for OkexConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        loop {
            // Debug: Print subscription message before attempting connection
            // info!("OKEx subscription message: {}", serde_json::to_string_pretty(&self.base_connection.sub_msg).unwrap_or_else(|_| "Failed to serialize".to_string()));
            info!("OKEx WebSocket URL: {}", &self.base_connection.url);
            
            let connect_result = if let Some(ref local_ip) = self.base_connection.local_ip {
                WsConnector::connect_with_local_ip(&self.base_connection.url, &self.base_connection.sub_msg, local_ip).await
            } else {
                WsConnector::connect(&self.base_connection.url, &self.base_connection.sub_msg).await
            };
            
            match connect_result {
                Ok(connection) => {
                    info!("Successfully connected to okex at {:?}", connection.connected_at);
                    self.base_connection.connection = Some(connection);
                    self.run_connection().await?;
                    //检查shutdown的当前情况，如果是true则break
                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    }else{
                        info!("Connection closed, reconnecting... (total restart count: {})", self.restart_count);
                    }
                }
                Err(e) => {
                    error!("Failed to connect to okex: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
