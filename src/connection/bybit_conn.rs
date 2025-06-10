use futures_util::{SinkExt, TryStreamExt};
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use bytes::Bytes;
use log::{info, warn, error};
use async_trait::async_trait;
use serde_json::json;
use uuid::Uuid;
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

fn is_bybit_pong_msg(msg: &serde_json::Value) -> bool {
    // 同时检查操作类型和返回消息内容
    msg.get("op").map(|v| v == "ping").unwrap_or(false) &&
    msg.get("ret_msg").map(|v| v == "pong").unwrap_or(false) &&
    msg.get("success").is_some()
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
        let mut ping_timer: Instant = Instant::now() + Duration::from_secs(20); // 心跳计时，初始值20s，倒计时结束发ping消息
        let mut reset_timer: Instant = ping_timer + Duration::from_secs(5); // 倒计时，初始值40s，倒计时结束重启websocket
        let mut waiting_pong = false;
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
                    // 生成一个当前时间戳作为req_id, tokio的uuid
                    let req_id = Uuid::new_v4().to_string();
                    let ping_msg = json!({
                        "req_id": req_id,
                        "op": "ping"
                    });
                    if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string())).await {
                        error!("Failed to send ping message: {:?}", e);
                        break;
                    }
                    waiting_pong = true;
                    // 重置心跳计时
                    ping_timer = Instant::now() + Duration::from_secs(20);
                    log::info!("Sent ping message with req_id: {:?}, reset ping timer to {:?}", req_id, ping_timer);
                }
                // ====处理超时====
                _ = time::sleep_until(reset_timer) => {
                    // 到期没有收到pong消息，则重启websocket
                    log::error!("Bybit: Ping timeout detected. reset connecting...");
                    ws_stream.close(None).await?; // 发送 CLOSE 帧
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
                                    //bybit的pong消息不走pong frame，而是走text frame，因此需要特殊处理
                                    warn!("Unexpected pong message: {:?}", payload);
                                }
                                Message::Text(text) => {
                                    if waiting_pong {
                                        // 只有在等待pong消息时，需要parser text，检查是否是pong消息
                                        let msg: serde_json::Value = serde_json::from_slice(&text.as_bytes()).unwrap();
                                        if is_bybit_pong_msg(&msg) {
                                            log::info!("Received pong message: {:?}", msg);
                                            waiting_pong = false;
                                            let req_id = msg["req_id"].as_str().unwrap();
                                            reset_timer = ping_timer + Duration::from_secs(5);
                                            log::info!("Received pong message with req_id: {:?}, reset reset timer to {:?}", req_id, reset_timer);
                                            //检查pong消息是否是suceess，如果不是，则断开连接
                                            if !msg["success"].as_bool().unwrap() {
                                                error!("Bybit: Pong message is not success: {:?}", msg);
                                                break;
                                            }
                                            continue;
                                        }
                                    }
                                    // 1、非等待pong消息，直接广播
                                    // 2、等待pong消息时，如果is_bybit_pong_msg为false，不会走到continue，而是走到这里，直接广播
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
