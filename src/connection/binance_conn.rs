use futures_util::{SinkExt, TryStreamExt};
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use reqwest::Client;
use bytes::Bytes;
use log::{info, warn, error};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashSet;
use tokio::time::sleep;
use anyhow::Result;
use crate::mkt_msg::{MktMsg, MktMsgType};
use crate::connection::connection::{MktConnection, MktConnectionHandler, MktConnectionRunner, WsConnector};


///为了支持send，BinanceFuturesConnection的成员需要支持send ---> MktConnection需要send
pub struct BinanceConnection {
    base_connection: MktConnection,
    delay_interval: Duration,
    ping_interval: Duration,
}

impl BinanceConnection {
    pub fn new(connection: MktConnection) -> Self {
        Self {
            base_connection: connection,
            delay_interval: Duration::from_secs(5),
            ping_interval: Duration::from_secs(180),
        }
    }
}

#[async_trait]
impl MktConnectionRunner for BinanceConnection {
    async fn run_connection(&mut self) -> anyhow::Result<()> {
        let mut ping_send_timer = Instant::now() + Duration::from_secs(180) + Duration::from_secs(5);
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
                // ====处理ping超时====
                _ = time::sleep_until(ping_send_timer) => {
                    warn!("Binance-futures: Ping timeout detected. reset connecting...");
                    ws_stream.close(None).await?; // 发送 CLOSE 帧
                    break;
                }
                // ====处理ws消息====
                msg = ws_stream.try_next() => {
                    match msg {
                        Ok(Some(msg)) => {
                            match msg {
                                Message::Ping(payload) => {
                                    info!("Sent pong message to server: {:?}", payload);
                                    if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                        error!("Failed to send pong message: {:?}", e);
                                        break;
                                    }
                                    ping_send_timer = Instant::now() + self.ping_interval + self.delay_interval;
                                    info!("Reset ping_send_timer to {:?}", ping_send_timer);
                                }
                                Message::Close(frame) => {
                                    warn!("Received close frame: {:?}", frame);
                                    if let Some(close_frame) = &frame {
                                        if close_frame.reason == "Invalid request" {
                                            error!("Received Invalid request close frame!");
                                            error!("Subscription message was: {}", self.base_connection.sub_msg);
                                            std::process::exit(1);
                                        }
                                    }
                                    break;
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
impl MktConnectionHandler for BinanceConnection {
    async fn start_ws(&mut self) -> anyhow::Result<()> {
        loop {
            let connect_result = if let Some(ref local_ip) = self.base_connection.local_ip {
                WsConnector::connect_with_local_ip(&self.base_connection.url, &self.base_connection.sub_msg, local_ip).await
            } else {
                WsConnector::connect(&self.base_connection.url, &self.base_connection.sub_msg).await
            };
            
            match connect_result {
                Ok(connection) => {
                    info!("successfully connected to Binance Futures at {:?}", connection.connected_at);
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
                    error!("Failed to connect to binance-futures: {:?}", e);
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}

pub struct BinanceFuturesSnapshotQuery {}

impl BinanceFuturesSnapshotQuery {
    const BASE_URL_SPOT: &str = "https://data-api.binance.vision"; // 币安rest 现货 api
    const BASE_URL_FUTURES: &str = "https://fapi.binance.com"; // 币安rest 合约 api
    const ENDPOINT_SPOT: &str = "/api/v3/depth"; // 获取深度
    const ENDPOINT_FUTURES: &str = "/fapi/v1/depth"; // 获取深度
    const LIMIT: u32 = 1000; // 获取深度限制
    const BATCH_SIZE: usize = 20; // 一次性发起的请求数量
    const MAX_RETRIES: u32 = 3; // 每个请求的最大重试次数
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(3); // 请求超时时间
    // 币安服务端限制请求频率，具体的算法为
    // https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book
    // 1000档的请求消耗Weight为50 每秒有1000的配额，对应20次请求次数
    const COOLDOWN: Duration = Duration::from_secs(60); // 请求间隔时间(币安服务端限制请求频率)

    async fn fetch_symbol_depth(
        exchange: &str,
        client: &Client,
        symbol: &str,
        invalid_symbols: &mut HashSet<String>,
        mut retry_count: u32,
    ) -> Result<MktMsg, anyhow::Error> {
        let upper_symbol = symbol.to_uppercase();

        // 如果是无效的符号，跳过请求, 返回错误
        if invalid_symbols.contains(&upper_symbol) {
            log::info!("invalid symbol: {}, skip!", upper_symbol);
            return Err(anyhow::anyhow!("Invalid symbol: {}", upper_symbol));
        }

        let base_url = match exchange {
            "binance" => Self::BASE_URL_SPOT,
            "binance-futures" => Self::BASE_URL_FUTURES,
            _ => return Err(anyhow::anyhow!("Invalid exchange: {}", exchange)),
        };
        let endpoint = match exchange {
            "binance" => Self::ENDPOINT_SPOT,
            "binance-futures" => Self::ENDPOINT_FUTURES,
            _ => return Err(anyhow::anyhow!("Invalid exchange: {}", exchange)),
        };

        loop {
            //打印请求的url
            let response = match client
                .get(&format!("{}{}", base_url, endpoint))
                .query(&[("symbol", &upper_symbol), ("limit", &Self::LIMIT.to_string())])
                .timeout(Self::REQUEST_TIMEOUT)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    if retry_count >= Self::MAX_RETRIES {
                        return Err(anyhow::anyhow!("Max retries exceeded: {} for symbol: {}", e, upper_symbol));
                    }
                    let delay: Duration = Duration::from_millis(2u64.pow(retry_count) * 500);
                    sleep(delay).await;
                    retry_count += 1;
                    continue;
                }
            };

            // 处理响应
            let text: String = match response.text().await {
                Ok(t) => t,
                Err(e) => {
                    if retry_count >= Self::MAX_RETRIES {
                        return Err(anyhow::anyhow!("Failed to get response text: {} for symbol: {}", e, upper_symbol));
                    }
                    let delay = Duration::from_millis(2u64.pow(retry_count) * 500);
                    sleep(delay).await;
                    retry_count += 1;
                    continue;
                }
            };

            // 检查是否是无效符号错误
            if let Ok(body) = serde_json::from_str::<Value>(&text) {
                if body["code"] == -1121 {
                    invalid_symbols.insert(upper_symbol.clone());
                    return Err(anyhow::anyhow!("Invalid symbol: {}", upper_symbol));
                }
            }

            // 解析响应数据
            match serde_json::from_str::<Value>(&text) {
                Ok(mut data) => {
                    // 插入 symbol 字段
                    data.as_object_mut()
                        .map(|obj| obj.insert("symbol".into(), Value::String(upper_symbol.clone())));
                    let json_bytes = match serde_json::to_vec(&data) {
                        Ok(v) => v,
                        Err(e) => {
                            log::warn!("Serialization failed for {}: {}", upper_symbol, e);
                            return Err(anyhow::anyhow!("Serialization failed for {}: {}", upper_symbol, e));
                        }
                    };
                    let bytes = Bytes::from(json_bytes);                    
                    // 创建和发送消息
                    let msg = MktMsg::create(MktMsgType::OrderBookInc, bytes);
                    return Ok(msg);
                }
                Err(e) => {
                    log::error!("Parse failed for {}: {}\nRaw: {}", upper_symbol, e, text);
                }
            }
        }
    }
    pub async fn start_fetching_depth(exchange: &str, symbols :Vec<String>, tx: tokio::sync::broadcast::Sender<Bytes>) {
        let client = Client::builder()
            .timeout(Self::REQUEST_TIMEOUT)
            .build()
            .expect("Failed to create HTTP client");
    
        // 创建一个HashSet来跟踪无效的符号
        let mut invalid_symbols = HashSet::new();
        // 将symbols分成多个批次
        let symbol_batches: Vec<_> = symbols.chunks(Self::BATCH_SIZE).collect();
        // 计算总批次数
        let total_batches = symbol_batches.len();
        log::info!("Fetching depth for {} symbols", symbols.len());
        log::info!("Total batches: {}", total_batches);
        // 遍历每个批次
        for (batch_num, batch) in symbol_batches.into_iter().enumerate() {
            log::info!("Processing batch #{}: {:?}", batch_num + 1, batch);
            // 记录每个批次的开始时间
            let batch_start = Instant::now();
            // 记录每个批次的第一个请求获得相应的时间
            let mut first_response_time = None;
            // 遍历每个批次中的每个symbol
            for symbol in batch {
                match Self::fetch_symbol_depth(exchange, &client, symbol, &mut invalid_symbols, 3).await {
                    Ok(msg) => {
                        if let Err(e) = tx.send(msg.to_bytes()) {
                            log::warn!("Send failed for {}: {}", symbol, e);
                        } else {
                            log::trace!("Snapshot sent for {}", symbol);
                        }
                        log::info!("fetch depth for {} success", symbol);
                    }
                    Err(e) => {
                        log::error!("error: {:?}", e);
                    }
                }

                if first_response_time.is_none() {
                    // 记录每个批次的第一个请求获得响应的时间
                    first_response_time = Some(Instant::now());
                }    
            }
    
            // 只有在非最后一批时才进行冷却等待
            if batch_num + 1 < total_batches {
                if let Some(first_response_time) = first_response_time {
                    //计算从第一个请求获得响应到当前批次的开始时间的时间差
                    let elapsed = first_response_time.duration_since(batch_start);
                    log::info!("Take {}ms from batch_start to first_response_time", elapsed.as_millis());
                    let remaining_wait = Self::COOLDOWN.saturating_sub(elapsed);
                    log::info!("Cooldown: {} ms", remaining_wait.as_millis());
    
                    if !remaining_wait.is_zero() {
                        log::info!("Cooldown: {} ms", remaining_wait.as_millis());
                        tokio::time::sleep(remaining_wait).await;
                    }
                }
            }
        }
        log::info!("total_symbols: {}, processed_symbols: {}", symbols.len(), symbols.len());
    }
}
