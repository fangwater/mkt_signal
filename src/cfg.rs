use serde::Deserialize;
use tokio::fs;
use serde_yaml;
use std::path::PathBuf;
use anyhow::{Context, Result};
use log::info;
use tokio::net::UnixStream;
use tokio::io::AsyncReadExt;

#[derive(Debug, Deserialize, Clone)]
pub struct MarketClient {
    pub exchange: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ZmqProxyCfg {
    pub ipc_path: String,
    pub primary_addr: String,
    pub secondary_addr: String,
    pub hwm: u32,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct RedisPubberCfg {
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub mode: String,
    pub max_stream_size: u32,
}


#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct Config {
    pub is_primary: bool,
    pub mkt_client: MarketClient,
    pub zmq_proxy: ZmqProxyCfg,
    pub redis_pubber: RedisPubberCfg,
    pub restart_duration: u64,
    pub snapshot_requery_time: String,
    pub exec_dir: PathBuf,
    pub symbol_socket: String,
    pub script_dir: String,
}

impl Config {
    pub async fn load_config(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path).await?;
        let config: Config = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn get_exchange(&self) -> String {
        self.mkt_client.exchange.clone()
    }

    pub fn get_batch_size(&self) -> usize {
        match self.get_exchange().as_str() {
            "binance-futures" => 50,
            "okex-swap" => 100,
            "bybit" => 300,
            _ => panic!("Unsupported exchange: {}", self.get_exchange()),
        }
    }

    pub fn get_exchange_url(&self) -> Result<String> {
        match self.get_exchange().as_str() {
            "binance-futures" => Ok("wss://fstream.binance.com/ws".to_string()),
            "okex-swap" => Ok("wss://ws.okx.com:8443/ws/v5/public".to_string()),
            "bybit" => Ok("wss://stream.bybit.com/v5/public/linear".to_string()),
            _ => anyhow::bail!("Unsupported exchange: {}", self.get_exchange()),
        }
    }
    

    pub async fn get_symbols(&self) -> Result<Vec<String>> {
        // 连接到symbol socket
        log::info!("Connecting to symbol socket: {}......", self.symbol_socket);
        let symbol_socket_path = format!("{}/{}.sock", self.symbol_socket, self.get_exchange());
        let mut stream = UnixStream::connect(&symbol_socket_path).await
            .context(format!("Failed to connect to symbol socket {}", symbol_socket_path))?;
        info!("Connected to symbol socket: {}", symbol_socket_path);

        // 读取symbol socket
        let mut buffer = Vec::with_capacity(4096);
        stream.read_to_end(&mut buffer).await
            .context("Failed to read from symbol socket")?;

        // 解析symbol socket
        let buffer_str = String::from_utf8(buffer)?;
        let value: serde_json::Value = serde_json::from_str(&buffer_str)?;
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