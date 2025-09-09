use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::SinkExt;
use log::{debug, error, info, warn};
use native_tls::TlsConnector as NativeTlsConnector;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{broadcast, watch, Mutex};
use tokio::{
    net::{lookup_host, TcpSocket, TcpStream},
    time::{self, Duration, Instant},
};
use tokio_native_tls::TlsConnector as TokioTlsConnector;
use tokio_tungstenite::{
    client_async, connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};
use url::Url;

pub struct WsConnectionResult {
    pub ws_stream: Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    pub connected_at: Instant,
}

//每个行情订阅连接，包含一个连接，一个发送通道，一个关闭标志
pub struct MktConnection {
    pub sub_msg: serde_json::Value,             // 行情订阅消息
    pub url: String,                            // 行情URL
    pub tx: broadcast::Sender<Bytes>,           // 行情消息广播发送端
    pub shutdown_rx: watch::Receiver<bool>,     // 关闭信号接收端
    pub connection: Option<WsConnectionResult>, // 连接状态
    pub local_ip: Option<String>,               // 绑定的本地IP地址
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
            shutdown_rx: global_shutdown_rx,
            connection: None,
            local_ip: None,
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

    pub async fn connect_with_local_ip(
        url: &str,
        sub_msg: &serde_json::Value,
        local_ip: &str,
    ) -> anyhow::Result<WsConnectionResult> {
        // 如果是默认IP，使用标准连接
        if local_ip == "0.0.0.0" || local_ip.is_empty() {
            return Self::connect(url, sub_msg).await;
        }

        // 解析本地IP
        let local_addr: IpAddr = local_ip
            .parse()
            .with_context(|| format!("Invalid local IP address: {}", local_ip))?;
        debug!(
            "Using local IP {} for WebSocket connection to {}",
            local_ip, url
        );

        // 解析URL
        let ws_url = Url::parse(url).with_context(|| "Invalid URL")?;
        let scheme = ws_url.scheme();
        let host = ws_url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("URL has no host"))?;
        let port = ws_url
            .port_or_known_default()
            .ok_or_else(|| anyhow::anyhow!("URL has no port"))?;

        // 解析远端地址，只取与本地IP同族的地址
        let mut addrs = lookup_host((host, port))
            .await
            .with_context(|| format!("Failed to resolve {}:{}", host, port))?;
        let target = addrs
            .find(|sa| match (sa, local_addr) {
                (SocketAddr::V4(_), IpAddr::V4(_)) => true,
                (SocketAddr::V6(_), IpAddr::V6(_)) => true,
                _ => false,
            })
            .ok_or_else(|| anyhow::anyhow!("No matching address family for host"))?;

        // 创建socket并绑定到指定本地IP
        let socket = match local_addr {
            IpAddr::V4(ip) => {
                let s = TcpSocket::new_v4()?;
                s.bind((ip, 0).into())
                    .with_context(|| format!("Failed to bind to IPv4 address {}", ip))?;
                s
            }
            IpAddr::V6(ip) => {
                let s = TcpSocket::new_v6()?;
                s.bind((ip, 0).into())
                    .with_context(|| format!("Failed to bind to IPv6 address {}", ip))?;
                s
            }
        };

        // 连接到目标地址
        let stream = socket
            .connect(target)
            .await
            .with_context(|| format!("Failed to connect to {}", target))?;

        // 根据 scheme 选择握手：ws 使用 Plain；wss 先做 TLS，再包裹为 NativeTls
        let (mut ws_stream, _resp) = if scheme.eq_ignore_ascii_case("wss") {
            // 使用 URL 的 host 作为 SNI
            let host = ws_url
                .host_str()
                .ok_or_else(|| anyhow::anyhow!("URL has no host"))?;
            let native = NativeTlsConnector::builder()
                .build()
                .with_context(|| "Failed to build native-tls connector")?;
            let connector = TokioTlsConnector::from(native);
            let tls_stream = connector
                .connect(host, stream)
                .await
                .with_context(|| "TLS handshake failed")?;
            let tls_wrapped = MaybeTlsStream::NativeTls(tls_stream);
            client_async(ws_url.as_str(), tls_wrapped).await?
        } else {
            let plain_stream = MaybeTlsStream::Plain(stream);
            client_async(ws_url.as_str(), plain_stream).await?
        };

        // 发送订阅消息
        ws_stream
            .send(Message::Text(sub_msg.to_string()))
            .await
            .with_context(|| "Failed to send subscription message")?;
        debug!(
            "Successfully sent subscription message via local IP {}",
            local_ip
        );

        Ok(WsConnectionResult {
            ws_stream: Arc::new(Mutex::new(ws_stream)),
            connected_at: Instant::now(),
        })
    }

    pub async fn connect(
        url: &str,
        sub_msg: &serde_json::Value,
    ) -> anyhow::Result<WsConnectionResult> {
        let url = Url::parse(url).with_context(|| "Invalid URL")?;
        for retry in 0..Self::MAX_RETRIES {
            match connect_async(url.clone()).await {
                Ok((mut ws_stream, _)) => {
                    match ws_stream.send(Message::Text(sub_msg.to_string())).await {
                        Ok(_) => {
                            info!("Successful send subscription message");
                            return Ok(WsConnectionResult {
                                ws_stream: Arc::new(Mutex::new(ws_stream)),
                                connected_at: Instant::now(),
                            });
                        }
                        Err(e) => {
                            error!("Failed to send subscription message: {}", e);
                            return Err(e.into());
                        }
                    }
                }
                Err(e) => {
                    if Self::is_dns_error(&e) {
                        error!(
                            "DNS error, retrying... ({}/{})",
                            retry + 1,
                            Self::MAX_RETRIES
                        );
                        time::sleep(Self::RETRY_DELAY).await;
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }
        Err(anyhow::anyhow!(
            "Failed to connect to WebSocket after {} retries",
            Self::MAX_RETRIES
        ))
    }
}

//两个trait，start stop是通用trait，run_connection是交易所的具体实现
#[async_trait]
pub trait MktConnectionRunner {
    async fn run_connection(&mut self) -> Result<()>;
}

//行情connection需要满足以下trait，才能被MktConnectionManager管理
#[async_trait]
pub trait MktConnectionHandler: MktConnectionRunner + Send {
    ///通用trait 只是遵循rust的设计模式，对每个交易所都impl一次
    async fn start_ws(&mut self) -> anyhow::Result<()>;
}

/// 根据交易所类型构造相应的连接处理器（带IP绑定）
pub fn construct_connection_with_ip(
    exchange: String,
    url: String,
    subscribe_msg: serde_json::Value,
    tx: broadcast::Sender<Bytes>,
    global_shutdown_rx: watch::Receiver<bool>,
    local_ip: String,
) -> anyhow::Result<Box<dyn MktConnectionHandler>> {
    use crate::connection::binance_conn::BinanceConnection;
    use crate::connection::bybit_conn::BybitConnection;
    use crate::connection::okex_conn::OkexConnection;

    let mut base_connection = MktConnection::new(url, subscribe_msg, tx, global_shutdown_rx);
    base_connection.local_ip = Some(local_ip);

    match exchange.as_str() {
        "binance-futures" | "binance" => Ok(Box::new(BinanceConnection::new(base_connection))),
        "okex-swap" | "okex" => Ok(Box::new(OkexConnection::new(base_connection))),
        "bybit" | "bybit-spot" => Ok(Box::new(BybitConnection::new(base_connection))),
        _ => Err(anyhow::anyhow!("Unsupported exchange: {}", exchange)),
    }
}

/// 根据交易所类型构造相应的连接处理器
#[allow(unused)]
pub fn construct_connection(
    exchange: String,
    url: String,
    subscribe_msg: serde_json::Value,
    tx: broadcast::Sender<Bytes>,
    global_shutdown_rx: watch::Receiver<bool>,
) -> anyhow::Result<Box<dyn MktConnectionHandler>> {
    use crate::connection::binance_conn::BinanceConnection;
    use crate::connection::bybit_conn::BybitConnection;
    use crate::connection::okex_conn::OkexConnection;

    let base_connection = MktConnection::new(url, subscribe_msg, tx, global_shutdown_rx);

    match exchange.as_str() {
        "binance-futures" | "binance" => Ok(Box::new(BinanceConnection::new(base_connection))),
        "okex-swap" | "okex" => Ok(Box::new(OkexConnection::new(base_connection))),
        "bybit" | "bybit-spot" => Ok(Box::new(BybitConnection::new(base_connection))),
        _ => Err(anyhow::anyhow!("Unsupported exchange: {}", exchange)),
    }
}
