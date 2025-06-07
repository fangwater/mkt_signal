use futures_util::{SinkExt, TryStreamExt};
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use bytes::Bytes;
use log::{info, warn, error};
use async_trait::async_trait;
use serde_json::json;
use crate::connection::connection::{MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector};


// bybit
// Due to network complexity, your may get disconnected at any time. Please follow the instructions below to ensure that you receive WebSocket messages on time:
// Keep connection alive by sending the heartbeat packet
// Reconnect as soon as possible if disconnected

// // req_id is a customised ID, which is optional
// ws.send(JSON.stringify({"req_id": "100001", "op": "ping"}));


// {
//     "req_id": "test",
//     "op": "pong",
//     "args": [
//         "1675418560633"
//     ],
//     "conn_id": "cfcb4ocsvfriu23r3er0-1b"
// }

// To avoid network or program issues, we recommend that you send the ping heartbeat packet every 20 seconds to maintain the WebSocket connection.
pub struct BybitConnection {
    base_connection: MktConnection,
}

impl BybitConnection {
    pub fn new(connection: MktConnection) -> Self {
        Self {
            base_connection: connection,
        }
    }
}

#[async_trait]
impl MktConnectionRunner for BybitConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        //bybit依赖于客户主动发送心跳
        //需要考虑的事件是
        //1、心跳发送计时，间隔20s，发送一次心跳
        //2、心跳发送后，等待pong消息，如果20s内没有收到pong消息，则重启websocket
        //注意bybit文档并未给出这个超时时间，因此设置为20s，和发送频率保持一致
        // 把问题转化为，必须稳定的收到pong 否则断开
        let mut reset_timer: Instant = Instant::now() + Duration::from_secs(40); // 倒计时，初始值40s，倒计时结束重启websocket
        let mut ping_timer: Instant = Instant::now() + Duration::from_secs(20); // 心跳计时，初始值20s，倒计时结束发ping消息
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
                // ====处理心跳====
                _ = time::sleep_until(ping_timer) => {
                    // 发送心跳
                    //ws.send(JSON.stringify({"req_id": "100001", "op": "ping"}));
                    // 生成心跳消息
                    // 生成一个当前时间戳作为req_id, tokio
                    let req_id = Instant::now().elapsed().as_millis();
                    let ping_msg = json!({
                        "req_id": req_id,
                        "op": "ping"
                    });
                    if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string())).await {
                        error!("Failed to send ping message: {:?}", e);
                        break;
                    }
                    // 重置心跳计时
                    ping_timer = Instant::now() + Duration::from_secs(20);
                    log::info!("Sent ping message with req_id: {:?}, reset ping timer to {:?}", req_id, ping_timer);
                }
                // ====处理超时====
                _ = time::sleep_until(reset_timer) => {
                    // 到期没有收到pong消息，则重启websocket
                    log::error!("Bybit: Ping timeout detected. reset connecting...");
                    break;
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
                                    // 收到pong消息后，重置reset_timer
                                    reset_timer = Instant::now() + Duration::from_secs(20);
                                    //parser req_id
                                    let pong_msg: serde_json::Value = serde_json::from_slice(&payload).unwrap();
                                    let req_id = pong_msg["req_id"].as_str().unwrap();
                                    log::info!("Received pong message with req_id: {:?}, reset reset timer to {:?}", req_id, reset_timer);
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
impl MktConnectionHandler for BybitConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        loop {
            match WsConnector::connect(&self.base_connection.url, &self.base_connection.sub_msg).await {
                Ok(connection) => {
                    info!("Successfully connected to bybit at {:?}", connection.connected_at);
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
                    error!("Failed to connect to bybit: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
