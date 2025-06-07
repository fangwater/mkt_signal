use tokio::sync::{broadcast, watch, Mutex};
use std::sync::Arc;
use bytes::Bytes;
use anyhow::{Result, Context};
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream, tungstenite::Message};
use futures_util::{SinkExt};
use url::Url;
use log::{info, error, warn};
use tokio::{net::TcpStream, time::{self, Duration, Instant}};
use async_trait::async_trait;

pub struct WsConnectionResult {
    pub ws_stream: Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    pub connected_at: Instant,
}

//每个行情订阅连接，包含一个连接，一个发送通道，一个关闭标志
pub struct MktConnection {
    pub sub_msg: serde_json::Value, // 行情订阅消息
    pub url: String,    // 行情URL
    pub tx: broadcast::Sender<Bytes>, // 行情消息广播发送端
    pub shutdown_rx: watch::Receiver<bool>, // 关闭信号接收端
    pub connection: Option<WsConnectionResult>, // 连接状态
}

impl MktConnection {
    /// 创建新的MktConnection实例
    pub fn new(
        url: String,
        sub_msg: serde_json::Value,
        tx: broadcast::Sender<Bytes>,
        global_shutdown_rx: watch::Receiver<bool>,
    ) -> Self {        
        Self {
            url,
            sub_msg,
            tx,
            shutdown_rx : global_shutdown_rx,
            connection: None,
        }
    }
}
pub struct WsConnector;

impl WsConnector {
    fn is_dns_error(e: &tokio_tungstenite::tungstenite::Error) -> bool {
        match e {
            tokio_tungstenite::tungstenite::Error::Io(io_err) => {
                let is_dns_error = matches!(
                    io_err.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::NotConnected
                );
                if is_dns_error {
                    error!("WebSocket IO Error: {:?}", io_err);
                }
                is_dns_error
            }
            tokio_tungstenite::tungstenite::Error::Http(res) => {
                let is_http_error = !res.status().is_success();
                if is_http_error {
                    warn!("WebSocket HTTP Error: {} - {:?}", res.status(), res);
                }
                is_http_error
            }
            _ => false,
        }
    }

    const MAX_RETRIES: usize = 5;
    const RETRY_DELAY: Duration = Duration::from_secs(1);

    pub async fn connect(url: &str, sub_msg: &serde_json::Value) -> anyhow::Result<WsConnectionResult> {
        let url = Url::parse(url).with_context(|| "Invalid URL")?;
        for retry in 0..Self::MAX_RETRIES {
            match connect_async(url.clone()).await {
                Ok((mut ws_stream, _)) => {
                    match ws_stream.send(Message::Text(sub_msg.to_string())).await {
                        Ok(_) => {
                            info!("Successful send subscription message");
                            return Ok(WsConnectionResult { ws_stream: Arc::new(Mutex::new(ws_stream)), connected_at: Instant::now() });
                        }
                        Err(e) => {
                            error!("Failed to send subscription message: {}", e);
                            return Err(e.into());
                        }
                    }
                }
                Err(e) => {
                    if Self::is_dns_error(&e) {
                        error!("DNS error, retrying... ({}/{})", retry + 1, Self::MAX_RETRIES);
                        time::sleep(Self::RETRY_DELAY).await;
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }
        Err(anyhow::anyhow!("Failed to connect to WebSocket after {} retries", Self::MAX_RETRIES))
    }
}



//两个trait，start stop是通用trait，run_connection是交易所的具体实现
#[async_trait]
pub trait MktConnectionRunner {
    async fn run_connection(&mut self) -> Result<()>;
}

//行情connection需要满足以下trait，才能被MktConnectionManager管理
#[async_trait]
pub trait MktConnectionHandler : MktConnectionRunner + Send{
    ///通用trait 只是遵循rust的设计模式，对每个交易所都impl一次
    async fn start_ws(&mut self) -> anyhow::Result<()>;
}
