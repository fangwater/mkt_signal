use crate::connection::connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, info, warn};
use serde_json::json;
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

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
const BYBIT_PING_INTERVAL: Duration = Duration::from_secs(20);
const BYBIT_PONG_TIMEOUT: Duration = Duration::from_secs(10);

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
    let op = msg.get("op").and_then(|v| v.as_str());
    let ret_msg = msg.get("ret_msg").and_then(|v| v.as_str());

    matches!(op, Some("pong")) || (matches!(op, Some("ping")) && matches!(ret_msg, Some("pong")))
}

fn is_bybit_control_msg(msg: &serde_json::Value) -> bool {
    if msg.get("topic").is_some() {
        return false;
    }

    msg.get("success").is_some()
        || msg.get("ret_msg").is_some()
        || msg.get("retCode").is_some()
        || msg.get("op").is_some()
}

fn log_bybit_control_msg(msg: &serde_json::Value) {
    let op = msg.get("op").and_then(|v| v.as_str()).unwrap_or("unknown");
    let success = msg.get("success").and_then(|v| v.as_bool());
    let ret_msg = msg.get("ret_msg").and_then(|v| v.as_str()).unwrap_or("");
    let ret_code = msg
        .get("retCode")
        .or_else(|| msg.get("code"))
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string());

    match success {
        Some(true) => info!(
            "Bybit control message ok: op={} ret_code={} ret_msg={} payload={}",
            op, ret_code, ret_msg, msg
        ),
        Some(false) => warn!(
            "Bybit control message failed: op={} ret_code={} ret_msg={} payload={}",
            op, ret_code, ret_msg, msg
        ),
        None => debug!("Bybit control message: op={} payload={}", op, msg),
    }
}

#[async_trait]
impl MktConnectionRunner for BybitConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        //bybit依赖于客户主动发送心跳
        //需要考虑的事件是
        //1、心跳发送计时，间隔20s，发送一次心跳
        //2、心跳发送后，等待pong消息，如果20s内没有收到pong消息，则重启websocket
        //注意bybit文档建议20s发一次ping，这里将pong等待窗口放宽到10s，降低瞬时抖动导致的误重连
        let mut ping_timer: Instant = Instant::now() + BYBIT_PING_INTERVAL;
        let mut reset_timer: Instant = ping_timer + BYBIT_PONG_TIMEOUT;
        let mut waiting_pong = false;
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
                    reset_timer = Instant::now() + BYBIT_PONG_TIMEOUT;
                    ping_timer = Instant::now() + BYBIT_PING_INTERVAL;
                    log::info!("Sent ping message with req_id: {:?}, reset ping timer to {:?}, pong deadline {:?}", req_id, ping_timer, reset_timer);
                }
                // ====处理超时====
                _ = time::sleep_until(reset_timer) => {
                    if !waiting_pong {
                        reset_timer = ping_timer + BYBIT_PONG_TIMEOUT;
                        continue;
                    }
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
                                    if let Ok(msg) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if waiting_pong && is_bybit_pong_msg(&msg) {
                                            log::info!("Received pong message: {:?}", msg);
                                            if matches!(msg.get("success").and_then(|v| v.as_bool()), Some(false)) {
                                                error!("Bybit: Pong message is not success: {:?}", msg);
                                                break;
                                            }
                                            waiting_pong = false;
                                            let req_id = msg.get("req_id").and_then(|v| v.as_str()).unwrap_or("unknown");
                                            reset_timer = ping_timer + BYBIT_PONG_TIMEOUT;
                                            log::info!("Received pong message with req_id: {:?}, reset timer to {:?}", req_id, reset_timer);
                                            continue;
                                        }

                                        if is_bybit_control_msg(&msg) {
                                            log_bybit_control_msg(&msg);
                                            continue;
                                        }
                                    } else if waiting_pong {
                                        warn!("Failed to parse Bybit text message while waiting for pong: {}", text);
                                    }

                                    // 只将真正的 topic/data 消息继续广播，控制面文本已在上方截获
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

#[cfg(test)]
mod tests {
    use super::{is_bybit_control_msg, is_bybit_pong_msg};
    use serde_json::json;

    #[test]
    fn bybit_pong_recognizes_current_public_format() {
        let msg = json!({
            "success": true,
            "ret_msg": "pong",
            "conn_id": "test",
            "op": "ping"
        });

        assert!(is_bybit_pong_msg(&msg));
    }

    #[test]
    fn bybit_pong_recognizes_op_pong_format() {
        let msg = json!({
            "op": "pong",
            "args": ["1675418560633"],
            "conn_id": "test"
        });

        assert!(is_bybit_pong_msg(&msg));
    }

    #[test]
    fn bybit_subscribe_ack_is_control_message() {
        let msg = json!({
            "success": true,
            "ret_msg": "subscribe",
            "op": "subscribe",
            "conn_id": "test"
        });

        assert!(is_bybit_control_msg(&msg));
    }

    #[test]
    fn bybit_topic_payload_is_not_control_message() {
        let msg = json!({
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "data": []
        });

        assert!(!is_bybit_control_msg(&msg));
    }
}

#[async_trait]
impl MktConnectionHandler for BybitConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        loop {
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
                        "Successfully connected to bybit at {:?}",
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
                    error!("Failed to connect to bybit: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
