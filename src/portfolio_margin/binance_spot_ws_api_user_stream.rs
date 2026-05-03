use crate::connection::connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use hmac::{Hmac, Mac};
use log::{debug, error, info, warn};
use serde_json::json;
use sha2::Sha256;
use std::collections::BTreeMap;
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

type HmacSha256 = Hmac<Sha256>;

/// Binance Spot WebSocket API (v3) user-data stream connection.
///
/// - Connects to `wss://ws-api.binance.com:443/ws-api/v3`
/// - Sends `userDataStream.subscribe.signature` after every (re)connect
/// - Forwards all text/binary frames to parser layer
pub struct BinanceSpotWsApiUserDataConnection {
    base_connection: MktConnection,
    api_key: String,
    api_secret: String,
    delay_interval: Duration,
    ping_interval: Duration,
    recv_window_ms: u64,
}

impl BinanceSpotWsApiUserDataConnection {
    pub fn new(
        connection: MktConnection,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Self {
        Self {
            base_connection: connection,
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            delay_interval: Duration::from_secs(5),
            ping_interval: Duration::from_secs(180),
            recv_window_ms: 5000,
        }
    }

    fn serialize_params(params: &BTreeMap<String, String>) -> String {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in params {
            serializer.append_pair(key, value);
        }
        serializer.finish()
    }

    fn sign_params(params: &BTreeMap<String, String>, secret: &str) -> Result<String> {
        let query = Self::serialize_params(params);
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|_| anyhow!("invalid binance api secret for ws-api subscription"))?;
        mac.update(query.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    fn build_subscribe_payload(&self) -> Result<String> {
        let mut params = BTreeMap::new();
        params.insert("apiKey".to_string(), self.api_key.trim().to_string());
        params.insert(
            "timestamp".to_string(),
            chrono::Utc::now().timestamp_millis().to_string(),
        );
        params.insert("recvWindow".to_string(), self.recv_window_ms.to_string());
        let signature = Self::sign_params(&params, self.api_secret.trim())?;
        params.insert("signature".to_string(), signature);

        let payload = json!({
            "id": chrono::Utc::now().timestamp_millis(),
            "method": "userDataStream.subscribe.signature",
            "params": params,
        });

        serde_json::to_string(&payload).with_context(|| "serialize spot ws-api subscribe payload")
    }

    fn log_control_message(text: &str) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
            return;
        };

        if let Some(status) = value.get("status").and_then(|v| v.as_i64()) {
            if status == 200 {
                info!("spot ws-api user stream subscribe ack: {}", text);
            } else {
                warn!("spot ws-api control status={} body={}", status, text);
            }
            return;
        }

        if value.get("error").is_some() {
            warn!("spot ws-api error message: {}", text);
        }
    }
}

#[async_trait]
impl MktConnectionRunner for BinanceSpotWsApiUserDataConnection {
    async fn run_connection(&mut self) -> Result<()> {
        let subscribe_payload = self.build_subscribe_payload()?;
        {
            let mut ws_stream = self
                .base_connection
                .connection
                .as_mut()
                .ok_or_else(|| anyhow!("ws-api connection not initialized"))?
                .ws_stream
                .lock()
                .await;
            ws_stream
                .send(Message::Text(subscribe_payload.clone()))
                .await
                .with_context(|| "send spot ws-api subscribe request")?;
        }
        debug!("spot ws-api subscribe request sent: {}", subscribe_payload);

        let mut ping_send_timer = Instant::now() + self.ping_interval + self.delay_interval;

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
                    warn!("spot ws-api: ping timeout detected; reconnecting");
                    ws_stream.close(None).await?;
                    break;
                }
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Ping(payload) => {
                                    if let Err(err) = ws_stream.send(Message::Pong(payload)).await {
                                        error!("spot ws-api send pong failed: {:?}", err);
                                        break;
                                    }
                                    ping_send_timer = Instant::now() + self.ping_interval + self.delay_interval;
                                }
                                Message::Pong(_) => {
                                    ping_send_timer = Instant::now() + self.ping_interval + self.delay_interval;
                                }
                                Message::Close(frame) => {
                                    warn!("spot ws-api received close frame: {:?}", frame);
                                    break;
                                }
                                Message::Text(text) => {
                                    let text = text.to_string();
                                    Self::log_control_message(&text);
                                    if let Err(err) = self.base_connection.tx.send(Bytes::from(text.into_bytes())) {
                                        error!("spot ws-api broadcast text failed: {}", err);
                                        break;
                                    }
                                }
                                Message::Binary(data) => {
                                    if let Err(err) = self.base_connection.tx.send(Bytes::from(data)) {
                                        error!("spot ws-api broadcast binary failed: {}", err);
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                        Ok(None) => {
                            debug!("spot ws-api stream closed by server");
                            break;
                        }
                        Err(err) => {
                            error!("spot ws-api stream error: {:?}", err);
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
impl MktConnectionHandler for BinanceSpotWsApiUserDataConnection {
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
                        "connected to Binance spot ws-api user stream at {:?}",
                        connection.connected_at
                    );
                    self.base_connection.connection = Some(connection);

                    if let Err(err) = self.run_connection().await {
                        error!("spot ws-api user stream run error: {}", err);
                    }

                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    }

                    debug!("spot ws-api user stream disconnected; reconnecting in 5s");
                    time::sleep(Duration::from_secs(5)).await;
                }
                Err(err) => {
                    error!(
                        "failed to connect Binance spot ws-api user stream: {:?}",
                        err
                    );
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
