use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, error, info, warn};
use runtime_common::ws_connection::{
    MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector,
};
use serde_json::json;
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

const CONNECTION_GENERATION_PREFIX: &[u8] = b"\0mkt_signal:hyperliquid:connection_generation\0";

pub struct HyperliquidConnection {
    base_connection: MktConnection,
    connection_generation: Option<u64>,
}

impl HyperliquidConnection {
    pub fn new(connection: MktConnection) -> Self {
        Self {
            base_connection: connection,
            connection_generation: None,
        }
    }

    /// Inject an internal generation marker into the same broadcast queue after
    /// each successful connection and before any frames from it are forwarded.
    /// Keeping the marker and payloads in one queue preserves their ordering for
    /// consumers that need to reset session-local protocol state.
    pub fn with_connection_generation_notifications(mut self) -> Self {
        self.connection_generation = Some(0);
        self
    }
}

fn next_connection_generation(current: &mut u64) -> u64 {
    let next = current.checked_add(1).unwrap_or(1);
    *current = next;
    next
}

fn connection_generation_notification(generation: u64) -> Bytes {
    let mut notification = Vec::with_capacity(CONNECTION_GENERATION_PREFIX.len() + 8);
    notification.extend_from_slice(CONNECTION_GENERATION_PREFIX);
    notification.extend_from_slice(&generation.to_be_bytes());
    Bytes::from(notification)
}

pub fn parse_connection_generation_notification(payload: &[u8]) -> Option<u64> {
    let generation: [u8; 8] = payload
        .strip_prefix(CONNECTION_GENERATION_PREFIX)?
        .try_into()
        .ok()?;
    Some(u64::from_be_bytes(generation)).filter(|generation| *generation > 0)
}

fn is_pong_message(value: &serde_json::Value) -> bool {
    value
        .get("channel")
        .and_then(|value| value.as_str())
        .map(|channel| channel.eq_ignore_ascii_case("pong"))
        .unwrap_or(false)
}

#[async_trait]
impl MktConnectionRunner for HyperliquidConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        let ping_interval = Duration::from_secs(30);
        let pong_timeout = Duration::from_secs(20);

        let mut ping_timer = Instant::now() + ping_interval;
        let mut deadline = ping_timer + pong_timeout;
        let mut waiting_pong = false;

        loop {
            let mut ws_stream = self
                .base_connection
                .connection
                .as_mut()
                .expect("hyperliquid connection must exist")
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
                _ = time::sleep_until(ping_timer) => {
                    let ping = json!({"method": "ping"}).to_string();
                    if let Err(error) = ws_stream.send(Message::Text(ping)).await {
                        error!("hyperliquid send ping failed: {:?}", error);
                        break;
                    }
                    waiting_pong = true;
                    ping_timer = Instant::now() + ping_interval;
                    deadline = Instant::now() + pong_timeout;
                }
                _ = time::sleep_until(deadline), if waiting_pong => {
                    warn!("hyperliquid pong timeout, reconnecting");
                    ws_stream.close(None).await?;
                    break;
                }
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(Message::Ping(payload))) => {
                            if let Err(error) = ws_stream.send(Message::Pong(payload)).await {
                                error!("hyperliquid send pong failed: {:?}", error);
                                break;
                            }
                        }
                        Ok(Some(Message::Pong(_))) => {
                            waiting_pong = false;
                        }
                        Ok(Some(Message::Text(text))) => {
                            if waiting_pong {
                                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if is_pong_message(&value) {
                                        waiting_pong = false;
                                        continue;
                                    }
                                }
                            }
                            let bytes = Bytes::from(text.into_bytes());
                            if let Err(error) = self.base_connection.tx.send(bytes) {
                                error!("hyperliquid broadcast failed: {}", error);
                                break;
                            }
                        }
                        Ok(Some(Message::Binary(data))) => {
                            if let Err(error) = self.base_connection.tx.send(Bytes::from(data)) {
                                error!("hyperliquid broadcast binary failed: {}", error);
                                break;
                            }
                        }
                        Ok(Some(Message::Close(frame))) => {
                            warn!("hyperliquid close frame: {:?}", frame);
                            break;
                        }
                        Ok(Some(other)) => {
                            debug!("hyperliquid ignore msg: {:?}", other);
                        }
                        Ok(None) => {
                            warn!("hyperliquid ws closed by server");
                            break;
                        }
                        Err(error) => {
                            error!("hyperliquid ws error: {:?}", error);
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
impl MktConnectionHandler for HyperliquidConnection {
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
                        "Successfully connected to hyperliquid at {:?}",
                        connection.connected_at
                    );
                    self.base_connection.connection = Some(connection);
                    if let Some(current) = self.connection_generation.as_mut() {
                        let generation = next_connection_generation(current);
                        if let Err(error) = self
                            .base_connection
                            .tx
                            .send(connection_generation_notification(generation))
                        {
                            warn!(
                                "Hyperliquid connection generation {} notification failed: {}",
                                generation, error
                            );
                        } else {
                            debug!("Hyperliquid connection generation {} ready", generation);
                        }
                    }
                    self.run_connection().await?;

                    if *self.base_connection.shutdown_rx.borrow() {
                        break Ok(());
                    }

                    info!("Hyperliquid connection closed, reconnecting...");
                }
                Err(error) => {
                    error!("Failed to connect to hyperliquid: {:?}", error);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        connection_generation_notification, next_connection_generation,
        parse_connection_generation_notification,
    };

    #[test]
    fn optional_connection_generation_notification_is_monotonic_and_parseable() {
        let mut current = 0;
        let first = next_connection_generation(&mut current);
        assert_eq!(first, 1);
        assert_eq!(
            parse_connection_generation_notification(&connection_generation_notification(first)),
            Some(1)
        );
        let second = next_connection_generation(&mut current);
        assert_eq!(second, 2);
        assert_eq!(
            parse_connection_generation_notification(&connection_generation_notification(second)),
            Some(2)
        );
        assert_eq!(parse_connection_generation_notification(b"{}"), None);
    }
}
