use std::time::Duration;

use clap::Parser;
use futures_util::StreamExt;
use mkt_signal::connection::connection::WsConnector;
use serde_json::Value;
use tokio::time;

#[derive(Parser, Debug)]
#[command(name = "ws_bind_ip_demo", about = "Demo: connect WebSocket binding a local IP (ws/wss)")]
struct Args {
    /// WebSocket URL, e.g. wss://stream.binance.com:9443/ws
    #[arg(long)]
    url: String,

    /// Local IP to bind, e.g. 192.168.1.10 (empty or 0.0.0.0 = no binding)
    #[arg(long, default_value = "0.0.0.0")]
    local_ip: String,

    /// JSON subscription/message sent immediately after connect
    #[arg(long, default_value = "{}")]
    json: String,

    /// How many messages to print before exit
    #[arg(long, default_value_t = 3)]
    print: usize,

    /// Overall timeout in seconds
    #[arg(long, default_value_t = 30)]
    timeout_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let sub_msg: Value = serde_json::from_str(&args.json)
        .unwrap_or_else(|_| Value::Object(serde_json::Map::new()));

    println!(
        "Connecting to {} with local_ip={}...",
        args.url, args.local_ip
    );

    let conn = WsConnector::connect_with_local_ip(&args.url, &sub_msg, &args.local_ip).await?;
    println!("Connected. Waiting for messages...");

    let mut ws = conn.ws_stream.lock().await;
    let mut count = 0usize;

    let mut timeout = time::timeout(Duration::from_secs(args.timeout_secs), async {
        while count < args.print {
            match ws.next().await {
                Some(Ok(msg)) => {
                    println!("[{}] {}", count + 1, msg);
                    count += 1;
                }
                Some(Err(e)) => {
                    eprintln!("WebSocket error: {}", e);
                    break;
                }
                None => {
                    println!("Stream closed by peer");
                    break;
                }
            }
        }
    })
    .await;

    if timeout.is_err() {
        eprintln!("Timed out after {}s", args.timeout_secs);
    }

    println!("Done.");
    Ok(())
}

