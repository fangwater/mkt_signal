use crate::traits::{WebSocketHandler, ExchangeConfig};
use tokio::sync::broadcast;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use anyhow::Result;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use url::Url;
use log::{info, error, warn};
use serde_json::Value;
use async_trait::async_trait;

pub struct OKExSwapClient {
    symbols: Vec<String>,
    channel: String,
    ws_url: String,
}

impl OKExSwapClient {
    pub fn new(symbols: Vec<String>, channel: String) -> Self {
        Self {
            symbols,
            channel,
            ws_url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl WebSocketHandler for OKExSwapClient {
    async fn start_ws(&self, tx: broadcast::Sender<Bytes>, shutdown: Arc<AtomicBool>, batch_id: usize) -> Result<()> {
        let mut retry_count = 0;
        let max_retries = 3;
        let retry_delay = tokio::time::Duration::from_secs(5);

        while retry_count < max_retries {
            match connect_async(Url::parse(&self.ws_url)?).await {
                Ok((ws_stream, _)) => {
                    let (mut write, mut read) = ws_stream.split();
                    let subscribe_msg = self.construct_subscribe_message(&self.symbols);
                    
                    if let Err(e) = write.send(Message::Text(subscribe_msg.to_string())).await {
                        error!("failed to send subscribe message: {}", e);
                        return Err(anyhow::anyhow!("Failed to send subscribe message: {}", e));
                    }

                    while let Some(msg) = read.next().await {
                        if shutdown.load(Ordering::Relaxed) {
                            info!("shutdown signal received, stopping WebSocket connection of batch {}", batch_id);
                            return Ok(());
                        }

                        match msg {
                            Ok(Message::Binary(data)) => {
                                let bytes = Bytes::from(data);
                                if let Err(e) = tx.send(bytes) {
                                    if !shutdown.load(Ordering::Relaxed) {
                                        error!("failed to broadcast message: {}", e);
                                    }
                                }
                            }
                            Ok(Message::Text(text)) => {
                                let bytes = Bytes::from(text.into_bytes());
                                if let Err(e) = tx.send(bytes) {
                                    if !shutdown.load(Ordering::Relaxed) {
                                        error!("failed to broadcast message: {}", e);
                                    }
                                }
                            }
                            Ok(Message::Ping(ping_data)) => {
                                if let Err(e) = write.send(Message::Pong(ping_data)).await {
                                    error!("failed to send Pong response: {}", e);
                                    return Err(anyhow::anyhow!("Failed to send Pong response: {}", e));
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                warn!("received WebSocket close frame: {:?}", frame);
                                return Err(anyhow::anyhow!("WebSocket connection closed"));
                            }
                            Err(e) => {
                                error!("WebSocket error: {}", e);
                                return Err(anyhow::anyhow!("WebSocket error: {}", e));
                            }
                            _ => {}
                        }
                    }
                    return Err(anyhow::anyhow!("WebSocket connection closed unexpectedly"));
                }
                Err(e) => {
                    error!("WebSocket connection failed: {}", e);
                    retry_count += 1;
                    if retry_count < max_retries {
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                    return Err(anyhow::anyhow!("WebSocket connection failed: {}", e));
                }
            }
        }
        
        Err(anyhow::anyhow!("Failed to connect after {} attempts", max_retries))
    }

    async fn stop_ws(&self) -> Result<()> {
        Ok(())
    }

    fn get_batch_size(&self) -> usize {
        100  // OKEx的批次大小
    }

    fn construct_subscribe_message(&self, symbols: &[String]) -> Value {
        let args: Vec<Value> = symbols.iter()
            .map(|symbol| serde_json::json!({
                "channel": self.channel,
                "instId": symbol
            }))
            .collect();

        serde_json::json!({
            "op": "subscribe",
            "args": args
        })
    }
}

impl ExchangeConfig for OKExSwapClient {
    fn get_ws_url(&self) -> &str {
        &self.ws_url
    }

    fn get_channel_type(&self) -> &str {
        &self.channel
    }
} 