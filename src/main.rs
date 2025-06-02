mod metrics_ws;
use std::path::{Path, PathBuf};
use std::{fs};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use clap::Parser;
use anyhow::{Result, Context};
use serde_json::Value;
use url::Url;
use bytes::Bytes;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::signal::unix::{signal, SignalKind};
use futures_util::future::join_all;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use log::{info, error, warn};
use tokio::sync::mpsc;
use std::time::Instant;
use crate::metrics_ws::ProcessStats;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 交易所名称
    #[arg(long, value_parser = ["binance-futures", "okex-swap", "bybit"])]
    exchange: String,

    /// 订阅频道
    #[arg(long, value_parser = ["trade", "inc"])]
    channel: String,

    /// Symbol socket路径
    // #[arg(long, default_value = "/root/project/crypto_process/mkt_compositor/symbol_server/exchange/binance-futures.sock")]
    // #[arg(long, default_value = "/root/project/crypto_process/mkt_compositor/symbol_server/exchange/okex-swap.sock")]
    #[arg(long, default_value = "/root/project/crypto_process/mkt_compositor/symbol_server/exchange/bybit.sock")]
    symbol_socket: String,

    /// 执行目录,默认为当前目录
    #[arg(long, default_value = "./")]
    exec_dir: PathBuf,

    /// 运行时工作线程数
    #[arg(long, default_value = "1")]
    worker_threads: usize,

    /// 监控IP
    #[arg(long, default_value = "127.0.0.1")]
    monitor_ip: String,
}

#[derive(Clone)]
struct MessageStats {
    last_report: Instant,
    count: u64,
    bytes_count: u64,
    stats_tx: mpsc::Sender<ProcessStats>,
    signal_type: Option<&'static str>,
}

impl MessageStats {
    fn new() -> (Self, mpsc::Receiver<ProcessStats>) {
        let (stats_tx, stats_rx) = mpsc::channel(10);
        (Self {
            last_report: Instant::now(),
            count: 0,
            bytes_count: 0,
            stats_tx,
            signal_type: None,
        }, stats_rx)
    }

    async fn start_monitoring(&mut self, mut message_rx: broadcast::Receiver<Bytes>) {
        while let Ok(msg) = message_rx.recv().await {
            self.count += 1;
            self.bytes_count += msg.len() as u64;
            // 每秒上报一次
            if self.last_report.elapsed().as_secs_f32() >= 15.0 {
                // 忽略发送错误，继续运行
                let _ = self.stats_tx.send(ProcessStats {
                    msg_rate: self.count,
                    status: "running",
                    bytes_per_sec: self.bytes_count,
                    signal_type: Some("periodic"),  // 标记为定期统计
                }).await;
                
                self.count = 0;
                self.bytes_count = 0;
                self.last_report = Instant::now();
            }
        }
        // 当 message_rx 关闭时，才打印错误
        error!("monitor channel closed");
    }

    fn update_signal(&mut self, signal_type: &'static str) {
        self.signal_type = Some(signal_type);
    }
}

fn get_exchange_url(exchange: &str) -> Result<String> {
    match exchange {
        "binance-futures" => Ok("wss://fstream.binance.com/ws".to_string()),
        "okex-swap" => Ok("wss://ws.okx.com:8443/ws/v5/public".to_string()),
        "bybit" => Ok("wss://stream.bybit.com/v5/public/linear".to_string()),
        _ => anyhow::bail!("Unsupported exchange: {}", exchange),
    }
}

async fn get_symbols(socket_path: &str) -> Result<Vec<String>> {
    let stream = UnixStream::connect(socket_path).await
        .context("Failed to connect to symbol socket")?;
    
    let mut buffer = Vec::new();
    let (mut reader, _writer) = stream.into_split();
    
    loop {
        let mut chunk = Vec::with_capacity(4096);
        let n = reader.read_buf(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buffer.extend(chunk);
    }
    
    let buffer_str = String::from_utf8(buffer)?;
    let value: Value = serde_json::from_str(&buffer_str)?;
    
    let symbols: Vec<String> = value["symbols"]
        .as_array()
        .context("symbols field not found or not an array")?
        .iter()
        .filter(|s| s["type"].as_str() == Some("perpetual"))
        .filter(|s| {
            let symbol = s["symbol_id"].as_str().unwrap();
            let symbol_lower = symbol.to_lowercase();
            if symbol_lower.ends_with("swap") {
                // 检查中间是否包含独立的USD（不包括USDT）
                let parts: Vec<&str> = symbol_lower.split('-').collect();
                if parts.len() >= 2 {
                    // 检查是否包含独立的"usd"，而不是"usdt"
                    !parts[1].split('_')
                        .any(|part| part == "usd" || part == "USD")
                } else {
                    true
                }
            } else {
                // 对于非swap结尾的交易对，只过滤以usd结尾的
                !symbol_lower.ends_with("usd")
            }
        })
        .map(|s| s["symbol_id"].as_str().unwrap().to_string())
        .collect();
    info!("Symbols size: {:?}", symbols.len());
    Ok(symbols)
}

async fn handle_client(
    socket: tokio::net::UnixStream,
    mut rx: broadcast::Receiver<Bytes>,
) -> Result<()> {
    info!("start handle client");
    
    let mut writer = tokio::io::BufWriter::new(socket);
    
    while let Ok(msg) = rx.recv().await {
        let len = msg.len() as u32;
        let mut packet = Vec::with_capacity(8 + msg.len());
        packet.extend_from_slice(&len.to_le_bytes());
        packet.extend_from_slice(&[0u8; 4]);
        packet.extend_from_slice(&msg);   
        writer.write_all(&packet).await?;
        writer.flush().await?;
    }
    Ok(())
}

fn get_inc_channel(exchange: &str) -> String {
    match exchange {
        "binance-futures" => "depth@0ms".to_string(),
        "okex-swap" => "books".to_string(),
        "bybit" => "orderbook.500".to_string(),
        _ => panic!("Unsupported exchange: {}", exchange)
    }
}

fn get_trade_channel(exchange: &str) -> String {
    match exchange {
        "binance-futures" => "trade".to_string(),
        "okex-swap" => "trades".to_string(),
        "bybit" => "publicTrade".to_string(),
        _ => panic!("Unsupported exchange: {}", exchange)
    }
}

fn get_batch_size(exchange: &str) -> usize {
    match exchange {
        "binance-futures" => 50,
        "okex-swap" => 100,
        "bybit" => 300,
        _ => panic!("Unsupported exchange: {}", exchange)
    }
}

fn construct_subscribe_message(exchange: &str, symbols: &[String], channel: &str) -> Value {
    match exchange {
        "binance-futures" => {
            let params: Vec<String> = symbols.iter()
                .map(|symbol| format!("{}@{}", symbol.to_lowercase(), channel))
                .collect();
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": params,
                "id": 1,
            })
        },
        "okex-swap" => {
            let args: Vec<Value> = symbols.iter()
                .map(|symbol| serde_json::json!({
                    "channel": channel,
                    "instId": symbol
                }))
                .collect();
            serde_json::json!({
                "op": "subscribe",
                "args": args
            })
        },
        "bybit" => {
            let args: Vec<String> = symbols.iter()
                .map(|symbol| format!("{}.{}",channel,symbol))
                .collect();
            serde_json::json!({
                "op": "subscribe",
                "args": args
            })
        },
        _ => panic!("Unsupported exchange: {}", exchange)
    }
}

async fn handle_websocket_batch_with_reconnect(
    symbols: Vec<String>,
    exchange: &str,
    channel: &str,
    tx: broadcast::Sender<Bytes>,
    shutdown: Arc<AtomicBool>,
    batch_id: usize,
) -> Result<()> {
    let ws_url = get_exchange_url(exchange).unwrap();
    let mut retry_count = 0;
    let max_retries = 3;
    let retry_delay = tokio::time::Duration::from_secs(5);

    while retry_count < max_retries {
        match connect_async(Url::parse(&ws_url)?).await {
            Ok((ws_stream, _)) => {
                let (mut write, mut read) = ws_stream.split();
                let subscribe_msg = construct_subscribe_message(exchange, &symbols, channel);
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
                            if let Err(e) = tx.send(bytes.clone()) {
                                if !shutdown.load(Ordering::Relaxed) {
                                    error!("failed to broadcast message: {}", e);
                                }
                            }
                        }
                        Ok(Message::Text(text)) => {
                            let bytes = Bytes::from(text.into_bytes());
                            if let Err(e) = tx.send(bytes.clone()) {
                                if !shutdown.load(Ordering::Relaxed) {
                                    error!("failed to broadcast message: {}", e);
                                }
                            }
                        }
                        Ok(Message::Ping(ping_data)) => {
                            info!("received ping, sending pong");
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
                let error_str = e.to_string();
                if error_str.contains("failed to lookup address information") {
                    error!("DNS lookup failed (attempt {}/{}): {}", retry_count + 1, max_retries, e);
                    retry_count += 1;
                    if retry_count < max_retries {
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                }
                error!("WebSocket connection failed: {}", e);
                return Err(anyhow::anyhow!("WebSocket connection failed: {}", e));
            }
        }
    }
    
    Err(anyhow::anyhow!("Failed to connect after {} attempts", max_retries))
}

async fn cleanup(socket_path: &Path) {
    info!("Cleaning up...");
    if socket_path.exists() {
        if let Err(e) = fs::remove_file(socket_path) {
            error!("Failed to remove socket file: {}", e);
        }
    }
}

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<()> {
    env_logger::init();
    
    let args = Args::parse();
    let socket_path = args.exec_dir.join(format!("{}_{}.sock", args.exchange, args.channel));
    
    if socket_path.exists() {
        fs::remove_file(&socket_path)?;
    }

    let symbols = get_symbols(&args.symbol_socket).await?;
    let (tx, _) = broadcast::channel::<Bytes>(1000);

    let listener = UnixListener::bind(&socket_path)?;
    
    let mut batches = Vec::new();
    for chunk in symbols.chunks(get_batch_size(&args.exchange)) {
        batches.push(chunk.to_vec());
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    
    // 启动消息统计监控
    let tx_for_stats = tx.clone();
    let (mut stats_monitor, stats_rx) = MessageStats::new();
    let stats_monitor_for_handle = Arc::new(tokio::sync::Mutex::new(stats_monitor.clone()));
    let stats_handle = tokio::spawn(async move {
        stats_monitor.start_monitoring(tx_for_stats.subscribe()).await;
    });
    handles.push(stats_handle);

    // 启动WS上报
    let host_ip = args.monitor_ip.clone();
    let ws_handle = tokio::spawn(metrics_ws::start_ws_reporter(
        host_ip,
        shutdown.clone(),
        Arc::new(args.clone()),
        stats_rx
    ));
    handles.push(ws_handle);
    
    for (batch_id, batch) in batches.into_iter().enumerate() {
        let tx_clone = tx.clone();
        let exchange = args.exchange.clone();
        let shutdown_clone = shutdown.clone();
        let channel = match args.channel.as_str() {
            "trade" => get_trade_channel(&exchange),
            "inc" => get_inc_channel(&exchange),
            _ => panic!("unsupported channel: {}", args.channel)
        };
        
        let handle = tokio::spawn(async move {
            if let Err(e) = handle_websocket_batch_with_reconnect(
                batch,
                &exchange,
                &channel,
                tx_clone,
                shutdown_clone,
                batch_id
            ).await {
                error!("WebSocket batch {} error: {}", batch_id, e);
            }
        });
        handles.push(handle);
    }

    let tx_for_connection = tx.clone();
    let connection_handle = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let rx = tx_for_connection.subscribe();
            tokio::spawn(async move {
                if let Err(e) = handle_client(socket, rx).await {
                    error!("Client error: {}", e);
                }
            });
        }
    });

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let socket_path_for_signal = socket_path.clone();

    tokio::select! {
        _ = sigint.recv() => {
            warn!("received SIGINT signal");
            shutdown.store(true, Ordering::Relaxed);
            let mut stats = stats_monitor_for_handle.lock().await;
            stats.update_signal("SIGINT");
            let _ = stats.stats_tx.send(ProcessStats {
                msg_rate: 0,
                status: "stopped",
                bytes_per_sec: 0,
                signal_type: Some("SIGINT"),
            }).await;
            cleanup(&socket_path_for_signal).await;
        }
        _ = sigterm.recv() => {
            warn!("received SIGTERM signal");
            shutdown.store(true, Ordering::Relaxed);
            let mut stats = stats_monitor_for_handle.lock().await;
            stats.update_signal("SIGTERM");
            let _ = stats.stats_tx.send(ProcessStats {
                msg_rate: 0,
                status: "stopped",
                bytes_per_sec: 0,
                signal_type: Some("SIGTERM"),
            }).await;
            cleanup(&socket_path_for_signal).await;
        }
        _ = connection_handle => {
            warn!("Unix domain socket connection handler completed");
        }
        _ = join_all(handles) => {
            warn!("all WebSocket connections completed");
        }
    }

    Ok(())
}