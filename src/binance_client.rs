use tokio::sync::broadcast;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use anyhow::Result;
use tokio::time::sleep_until;
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use url::Url;
use log::{info, error, warn};
use serde_json::Value;
use async_trait::async_trait;
use tokio::time::{Duration, Instant};
use url::Url;
use tokio::{net::TcpStream, sync::broadcast, time::{self, Duration, Instant}};

pub struct WsConnectionResult {
    //TcpStream表示是一个tcp的socket流，MaybeTlsStream表示是一个tls的socket流，可能wss或者ws
    //WebSocketStream表示是一个websocket的流，实现了ws协议
    pub ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    //连接时间
    pub connected_at: tokio::time::Instant,
}

#[derive(Debug, thiserror::Error)]
pub enum WsConnectionError {
    #[error("Failed to connect to WebSocket after {0} retries")]
    MaxRetriesExceeded(usize),
    #[error("WebSocket connection failed: {0}")] 
    ConnectionFailed(String),
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
}

/// 通用连接器 (处理DNS重试和WebSocket建立)
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
                    error!("WebSocket IO Error: {:?}", io_err); // 记录错误日志
                }
                is_dns_error
            }
            tokio_tungstenite::tungstenite::Error::Http(res) => {
                let is_http_error = !res.status().is_success();
                if is_http_error {
                    warn!("WebSocket HTTP Error: {} - {}", res.status(), res); // 记录HTTP错误
                }
                is_http_error
            }
            _ => false,
        }
    }
    const MAX_RETRIES: usize = 5;
    const RETRY_DELAY: Duration = Duration::from_secs(1);
    pub async fn connect(url: &str) -> Result<WsConnectionResult, Box<dyn std::error::Error>> {
        let url = Url::parse(url).context("Invalid URL")?;
        for retry in 0..Self::MAX_RETRIES {
            match connect_async(url.clone()).await {
                Ok((ws_stream, _)) => {
                    return Ok(WsConnectionResult { ws_stream, connected_at: Instant::now() });
                }
                Err(e) => {
                    if Self::is_dns_error(&e) {
                        error!("DNS error, retrying... ({}/{})", retry + 1, Self::MAX_RETRIES);
                        time::sleep(Self::RETRY_DELAY).await;
                    } else {
                        return Err(e.into());
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Err(anyhow::anyhow!("Failed to connect to WebSocket after {} retries", Self::MAX_RETRIES))
    }
}


//binance-futures implementation
//tokio::sync::watch 是一个watch channel，可以监听一个值的变化
use tokio::sync::watch;

pub struct BinanceFutures{
    args : Args,
    ping_send_timer : Instant, //等待服务器ping的倒计时, 倒计时结束如果没有收到ping，则重新连接
}

trait construct_payload{
    fn on_ping(&self, ping_msg: &str) -> Result<String>;
    fn on_pong(&self, pong_msg: &str) -> Result<String>;
}

impl construct_payload for BinanceFutures {
    //当客户收到ping消息，必需尽快回复pong消息，同时payload需要和ping消息一致。
    fn on_ping(&self, ping_msg: &str) -> Result<String> {
        Ok(ping_msg.to_string())
    }
    fn on_pong(&self, pong_msg: &str) -> Result<String> {
        //不会收到pong消息
        log::warn!("Received unexpected pong message for binance-futures");
        Ok("".to_string())        
    }
}

impl BinanceFutures {
    //tokio::time::timeout + Instant来实现倒计时
    const DelayInterval: Duration = Duration::from_secs(30); //30秒的延迟
    const PING_INTERVAL: Duration = Duration::from_secs(180); //3分钟
    const PONG_WAIT_INTERVAL: Duration = Duration::from_secs(180); //3分钟
    pub fn new(args: Args) -> Self {
        Self {
            args,
            ping_send_timer: Instant::now() + Self::PING_INTERVAL + Self::DelayInterval, //设置为6分钟，初始阶段给定一个冗余
            pong_wait_timer: None, //没有发出ping，则pong_wait_timer为None
        }
    }
}

// 如果 websocket 服务器在10分钟内没有收到来自连接的pong frame，则连接将断开。
// 当客户收到ping消息，必需尽快回复pong消息，同时payload需要和ping消息一致。
// 未经请求的pong消息是被允许的，但是不会保证连接不断开。对于这些pong消息，建议payload为空。
impl BinanceFutures {
    // Websocket服务器每3分钟发送一个ping消息。
    pub fn new(args: Args) -> Self {
        Self { args }
    }
    async fn run_connection(&self, mut conn: WsConnectionResult, shutdown: watch::Receiver<bool>){
        //运行币安的websocket连接
        loop{
            //tokiox响应三种事件
            //1、shutdown触发，break，终止connection
            //2、正常的websocket事件
            //3、倒计时事件，通过time util触发
            tokio::select!{
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        log::info!("Shutting down binance-futures connection");
                        break;
                    }
                }
                _ = tokio::time::sleep_until(self.ping_send_timer) => {
                    log::warn!("Binance-futures: Ping timeout detected. reset connecting...");
                    break;
                }
                msg = conn.ws_stream.next() => {
                    match msg {
                        Some(msg) => {
                            //判断是哪种类型的消息
                            //1、ping消息
                            if msg {
                                let payload = self.on_ping(&msg.to_text().unwrap()).unwrap();
                                if let Err(e) = conn.ws_stream.send(Message::Text(payload)).await {
                                    log::error!("Failed to send ping message: {:?}", e);
                                    break;
                                }
                            }
                            //2、pong消息
                            //3、其他消息
                            //4、错误消息
                            //5、关闭消息
                            //6、心跳消息
                            //7、其他消息
                        }
                    }
                }
            }
        }

    }
}
    pub async fn start_ws(&self, shutdown: watch::Receiver<bool>){
        loop{
            match WsConnector::connect(&self.args.get_exchange_url().unwrap()).await {
                Ok(connection) => {
                    info!("Connected to Binance Futures");

                }
                Err(e) => {
                    error!("Failed to connect to Binance Futures: {:?}", e); 
                }
        }
    }

}








mod exchanges {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum ExchangeType {
        BinanceFutures,
        OkexSwap,
        Bybit,
    }

    // 对于okex-swap_stream
    // 1、对于每个batch的websocket，维护一个定时器，定时器时间为15s的倒计时
    // 2、如果收到新消息，则刷新定时器
    // 3、如果count down后发送字符串ping，等待pong
    // 4、记录这个过程，如果没有成功的返回字符串pong 则重新连接这个websocket，等待时间也是N秒
    // 因此需要维护
    //1、一个waiting_pong的bool，如果为true，则说明在等待pong，如果为false，则说明在等待ping
    //2、一个next_ping_timer的Instant作为倒计时，归0代表需要发送ping
    #[derive(Debug)]
    pub struct OkexSwapState {
        //下一次发送ping的时间点，倒计时结束发送ping
        ping_send_timer : Instant,
        //是否在等待pong
        waiting_pong: bool,
        //等待pong的倒计时，倒计时结束，如果waiting_pong为true，则说明需要重新连接
        pong_wait_timer: Instant,
    }

    impl ExchangeState for OkexSwapState {
        fn should_reconnect(&self) -> Option<&'static str> {
            if self.waiting_pong && self.pong_wait_timer.elapsed() > Duration::from_secs(15) {
                return Some("Waiting for pong response timeout");
            }
            None
        }
    }
        
}


//行情websocket连接
pub struct MktWsConnection {
    subscribe_msg: Value, //订阅消息，每次开始连接时，发送一次
    ws_url: String, //websocket url
    tx: broadcast::Sender<Bytes>, //收到消息后，推入tx
    shutdown: Arc<AtomicBool>, //关闭信号
    connection: Option
}

#[async_trait::async_trait]
impl WebSocketHandler for MktWsConnection {
    //启动ws连接，对应一个单独的batch，发送一次订阅消息
    async fn start_ws(&self) -> Result<()> {
        //如果当前ws连接还存在，需要关闭之前的连接

    }       
}