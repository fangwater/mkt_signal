use clap::Parser;
use std::path::PathBuf;
use crate::client::{ClientType, ChannelType};
use std::os::unix::net::UnixStream;
use anyhow::Result;
use serde_json::Value;
use log::info;
use std::io::{Read, Write};


#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 交易所名称
    #[arg(long, value_parser = ["binance-futures", "okex-swap", "bybit"])]
    exchange: String,

    /// Symbol socket路径
    #[arg(long)]
    symbol_socket: String,

    /// 执行目录,默认为当前目录
    #[arg(long, default_value = "./")]
    exec_dir: PathBuf,

    /// 监控IP
    #[arg(long, default_value = "127.0.0.1")]
    monitor_ip: String,
}

impl Args {
    fn get_exchange(&self) -> String {
        match self.exchange.as_str() {
            "binance-futures" => "binance-futures".to_string(),
            "okex-swap" => "okex-swap".to_string(),
            "bybit" => "bybit".to_string(),
            _ => panic!("Unsupported exchange: {}", self.exchange),
        }
    }

    fn get_trade_socket_path(&self) -> PathBuf {
        self.exec_dir.join(format!("{}_{}.sock", self.exchange, "trade"))
    }

    fn get_inc_socket_path(&self) -> PathBuf {
        self.exec_dir.join(format!("{}_{}.sock", self.exchange, "inc"))
    }

    fn get_batch_size(&self) -> usize {
        match self.exchange.as_str() {
            "binance-futures" => 50,
            "okex-swap" => 100,
            "bybit" => 300,
            _ => panic!("Unsupported exchange: {}", self.exchange),
        }
    }

    fn get_exchange_url(&self) -> Result<String> {
        match self.exchange.as_str() {
            "binance-futures" => Ok("wss://fstream.binance.com/ws".to_string()),
            "okex-swap" => Ok("wss://ws.okx.com:8443/ws/v5/public".to_string()),
            "bybit" => Ok("wss://stream.bybit.com/v5/public/linear".to_string()),
            _ => anyhow::bail!("Unsupported exchange: {}", exchange),
        }
    }
    

    fn get_symbols(&self) -> Result<Vec<String>> {
        // 连接到symbol socket
        let stream = UnixStream::connect(self.symbol_socket)
        .context("Failed to connect to symbol socket")?;
        info!("Connected to symbol socket: {}", self.symbol_socket);

        // 读取symbol socket
        let mut buffer = Vec::with_capacity(4096);
        stream.read_to_end(&mut buffer)
            .context("Failed to read from symbol socket")?;

        // 解析symbol socket
        let buffer_str = String::from_utf8(buffer)?;
        let value: Value = serde_json::from_str(&buffer_str)?;
        info!("Parsed symbol socket: {:?}", value);

        // 过滤symbol
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
}   
