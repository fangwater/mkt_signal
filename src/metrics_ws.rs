// metrics_ws.rs
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::Duration;
use log::{error, warn, info};
use chrono;
use crate::Args;

#[derive(Clone)]
pub struct ProcessStats {
    pub msg_rate: u64,
    pub status: &'static str,
    pub bytes_per_sec: u64,
    pub signal_type: Option<&'static str>,  // 可以是 "periodic"（定期统计）或 "SIGINT"/"SIGTERM"（信号）
}

pub async fn start_ws_reporter(
    host_ip: String,
    shutdown: Arc<AtomicBool>,
    args: Arc<Args>,
    mut stats_rx: mpsc::Receiver<ProcessStats>
) {
    let server_url = format!("ws://{}:3001/ws/metrics", host_ip);
    info!("Starting WS reporter, connecting to: {}", server_url);
    
    // 消息处理循环
    while !shutdown.load(Ordering::Relaxed) {
        // 尝试建立 WebSocket 连接
        info!("Attempting to establish WebSocket connection to {}", server_url);
        let ws_stream = match connect_async(&server_url).await {
            Ok((stream, response)) => {
                info!("Successfully connected to monitor server");
                info!("WebSocket handshake response status: {}", response.status());
                info!("WebSocket connection established for exchange: {}, channel: {}", args.exchange, args.channel);
                stream
            }
            Err(e) => {
                error!("Failed to connect to monitor server: {}", e);
                error!("Connection details: host={}, port=3001, path=/ws/metrics", host_ip);
                warn!("Will retry connection in 5 seconds...");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut write, _read) = ws_stream.split();
        info!("WebSocket stream successfully split for reading and writing");
        
        // 持续处理消息
        while let Some(stats) = stats_rx.recv().await {
            if shutdown.load(Ordering::Relaxed) {
                info!("Shutdown signal received, stopping WS reporter");
                break;
            }

            let data = json!({
                "exchange": args.exchange,
                "channel": args.channel,
                "msg_sec": stats.msg_rate,
                "bytes_sec": stats.bytes_per_sec,
                "status": stats.status,
                "signal_type": stats.signal_type,
                "timestamp": chrono::Utc::now().timestamp_millis()
            });
            
            // 如果发送失败，跳出内层循环，重新建立连接
            if let Err(e) = write.send(Message::Text(data.to_string())).await {
                error!("Failed to send metrics data: {}", e);
                error!("Last attempted message: {}", data.to_string());
                error!("Will attempt to reconnect...");
                break;
            }
        }

        // 如果是因为 shutdown 信号退出，则不再重连
        if shutdown.load(Ordering::Relaxed) {
            info!("Shutdown confirmed, WS reporter stopping");
            break;
        }

        warn!("WebSocket connection lost or error occurred, waiting 1 second before reconnecting");
        // 等待一段时间后重连
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    
    info!("WS reporter shutdown complete");
}