use serde::Deserialize;
use tokio::fs;
use serde_yaml;
use anyhow::{Context, Result};
use log::info;
use tokio::net::UnixStream;
use tokio::io::AsyncReadExt;
use prettytable::{Table, Row, Cell, format};

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
// 添加表格打印函数
fn print_symbol_comparison(
    futures_symbols: &[String],
    spot_symbols: &[String],
    common_symbols: &[String]
) {
    let mut table = Table::new();
    
    // 设置表格格式
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    table.set_titles(Row::from(vec![
        Cell::new("Symbol").style_spec("c"),
        Cell::new("Futures").style_spec("c"),
        Cell::new("Spot").style_spec("c")
    ]));
    
    // 添加公共交易对（两者都存在）
    for symbol in common_symbols {
        table.add_row(Row::from(vec![
            Cell::new(symbol),
            Cell::new("✓").style_spec("Fg"),
            Cell::new("✓").style_spec("Fg")
        ]));
    }
    
    // 添加只有 futures 的交易对
    for symbol in futures_symbols {
        if !common_symbols.contains(symbol) {
            table.add_row(Row::from(vec![
                Cell::new(symbol),
                Cell::new("✓").style_spec("Fg"),
                Cell::new("✗").style_spec("Fr")
            ]));
        }
    }
    
    // 添加只有 spot 的交易对
    for symbol in spot_symbols {
        if !common_symbols.contains(symbol) {
            table.add_row(Row::from(vec![
                Cell::new(symbol),
                Cell::new("✗").style_spec("Fr"),
                Cell::new("✓").style_spec("Fg")
            ]));
        }
    }
    
    // 打印表格
    info!("Symbol comparison:");
    table.printstd();
}


#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct Config {
    pub is_primary: bool,
    pub restart_duration_secs: u64,
    pub snapshot_requery_time: Option<String>,
    pub symbol_socket: String,
    pub exchange: String,
    pub zmq_proxy: ZmqProxyCfg,
    pub redis_pubber: RedisPubberCfg,
}

impl Config {
    pub async fn load_config(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path).await?;
        let config: Config = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn get_exchange(&self) -> String {
        self.exchange.clone()
    }

    pub fn get_batch_size(&self) -> usize {
        match self.get_exchange().as_str() {
            "binance-futures" => 50,
            "binance" => 100,
            "okex-swap" => 100,
            "okex" => 100,
            "bybit" => 300,
            "bybit-spot" => 10,
            _ => panic!("Unsupported exchange: {}", self.get_exchange()),
        }
    }

    pub fn get_exchange_url(&self) -> Result<String> {
        match self.get_exchange().as_str() {
            //币安u本位期货合约
            "binance-futures" => Ok("wss://fstream.binance.com/ws".to_string()),
            //币安u本位期货合约对应的现货
            "binance" => Ok("wss://data-stream.binance.vision/ws".to_string()),
            //OKEXu本位期货合约
            "okex-swap" => Ok("wss://ws.okx.com:8443/ws/v5/public".to_string()),
            //OKEXu本位期货合约对应的现货
            "okex" => Ok("wss://ws.okx.com:8443/ws/v5/public".to_string()),
            //Bybitu本位期货合约
            "bybit" => Ok("wss://stream.bybit.com/v5/public/linear".to_string()),
            //Bybitu本位期货合约对应的现货
            "bybit-spot" => Ok("wss://stream.bybit.com/v5/public/spot".to_string()),
            _ => anyhow::bail!("Unsupported exchange: {}", self.get_exchange()),
        }
    }
    async fn get_symbol_from_unix_socket(symbol_socket: &str, exchange: &str) -> Result<serde_json::Value> {
        // 连接到symbol socket
        log::info!("Connecting to symbol socket: {}......", symbol_socket);
        let symbol_socket_path = format!("{}/{}.sock", symbol_socket, exchange);
        let mut stream = UnixStream::connect(&symbol_socket_path).await
            .context(format!("Failed to connect to symbol socket {}", symbol_socket_path))?;
        info!("Connected to symbol socket: {}", symbol_socket_path);

        // 读取symbol socket
        let mut buffer = Vec::with_capacity(16384);
        stream.read_to_end(&mut buffer).await
            .context("Failed to read from symbol socket")?;

        // 解析symbol socket
        let buffer_str = String::from_utf8(buffer)?;
        let value: serde_json::Value = serde_json::from_str(&buffer_str)?;
        Ok(value)
    }


    async fn get_spot_symbols_related_to_binance_futures(symbol_socket: &str) -> Result<Vec<String>> {
        let value = Self::get_symbol_from_unix_socket(symbol_socket, "binance").await?;
        let symbols: Vec<String> = value["symbols"]
            .as_array()
            .context("symbols field not found or not an array")?
            .iter()
            .filter(|s| s["type"].as_str() == Some("spot"))
            .map(|s| s["symbol_id"].as_str().unwrap().to_string())
            .filter(|s| s.to_lowercase().ends_with("usdt"))
            .collect();
        info!("Binance spot USDT-denominated symbol count {:?}", symbols.len());
        let futures_symbols = Self::get_symbol_for_binance_futures(symbol_socket).await?;
        info!("Binance futures USDT-denominated symbol count {:?}", futures_symbols.len());
        let spot_symbols_related_to_futures: Vec<String> = symbols.iter().filter(|s| futures_symbols.contains(s)).cloned().collect();
        info!("Binance spot symbols related to futures {:?}", spot_symbols_related_to_futures.len());
        //用三线表打印，哪些符号在futures中存在，但是在spot中不存在
        print_symbol_comparison(
            &futures_symbols,
            &symbols,
            &spot_symbols_related_to_futures
        );
        Ok(spot_symbols_related_to_futures)
    }

    async fn get_symbol_for_binance_futures(symbol_socket: &str) -> Result<Vec<String>> {
        let value = Self::get_symbol_from_unix_socket(symbol_socket, "binance-futures").await?;
        let symbols: Vec<String> = value["symbols"]
            .as_array()
            .context("symbols field not found or not an array")?
            .iter()
            .filter(|s| s["type"].as_str() == Some("perpetual"))
            .map(|s| s["symbol_id"].as_str().unwrap().to_string())
            .filter(|s| s.to_lowercase().ends_with("usdt"))
            .collect();
        Ok(symbols)     
    }

    async fn get_symbol_for_okex_swap(symbol_socket: &str) -> Result<Vec<String>> {
        let value = Self::get_symbol_from_unix_socket(symbol_socket, "okex-swap").await?;
        let symbols: Vec<String> = value["symbols"]
            .as_array()
            .context("data field not found or not an array")?
            .iter()
            .map(|s| s["symbol_id"].as_str().unwrap().to_string())
            .filter(|s| {
                let symbol_lower = s.to_lowercase();
                if symbol_lower.ends_with("swap") {
                    // 检查中间是否包含USDT（而不是USD）
                    let parts: Vec<&str> = symbol_lower.split('-').collect();
                    //"TRX-USDT-SWAP" - 保留这样的
                    if parts.len() >= 2 && parts[1] == "usdt" {
                        return true;
                    }
                    return false;
                }
                return false;
            })
            .collect();
        Ok(symbols)
    }

    async fn get_spot_symbols_related_to_okex_swap(symbol_socket: &str) -> Result<Vec<String>> {
        let value = Self::get_symbol_from_unix_socket(symbol_socket, "okex").await?;
        let symbols: Vec<String> = value["symbols"]
            .as_array()
            .context("data field not found or not an array")?
            .iter()
            .map(|s| s["symbol_id"].as_str().unwrap().to_string())
            .filter(|s| s.to_lowercase().ends_with("usdt"))
            .collect();
        info!("OKEx spot USDT-denominated symbol count {:?}", symbols.len());
        let swap_symbols = Self::get_symbol_for_okex_swap(symbol_socket).await?;
        info!("OKEx swap USDT-denominated symbol count {:?}", swap_symbols.len());
        let spot_symbols_related_to_swap: Vec<String> = symbols.iter()
            .filter(|s| swap_symbols.contains(s))
            .cloned()
            .collect();
        info!("OKEx spot symbols related to swap {:?}", spot_symbols_related_to_swap.len());
        print_symbol_comparison(
            &swap_symbols,
            &symbols,
            &spot_symbols_related_to_swap
        );
        Ok(spot_symbols_related_to_swap)
    }


    async fn get_symbol_for_bybit_linear(symbol_socket: &str) -> Result<Vec<String>> {
        let value = Self::get_symbol_from_unix_socket(symbol_socket, "bybit").await?;
        let symbols: Vec<String> = value["symbols"]
            .as_array()
            .context("symbols field not found or not an array")?
            .iter()
            .filter(|s| s["type"].as_str() == Some("perpetual"))
            .map(|s| s["symbol_id"].as_str().unwrap().to_string())
            .filter(|s| s.to_lowercase().ends_with("usdt"))
            .collect();
        Ok(symbols)
    }
    
    async fn get_spot_symbols_related_to_bybit(symbol_socket: &str) -> Result<Vec<String>> {
        let value = Self::get_symbol_from_unix_socket(symbol_socket, "bybit-spot").await?;
        let symbols: Vec<String> = value["symbols"]
            .as_array()
            .context("symbols field not found or not an array")?
            .iter()
            .filter(|s| s["type"].as_str() == Some("spot"))
            .map(|s| s["symbol_id"].as_str().unwrap().to_string())
            .filter(|s| s.to_lowercase().ends_with("usdt"))
            .collect();
        info!("Bybit spot USDT-denominated symbol count {:?}", symbols.len());
        let linear_contract_symbols = Self::get_symbol_for_bybit_linear(symbol_socket).await?;
        info!("Bybit linear USDT-denominated symbol count {:?}", linear_contract_symbols.len());
        let spot_symbols_related_to_linear: Vec<String> = symbols.iter()
            .filter(|s| linear_contract_symbols.contains(s))
            .cloned()
            .collect();
        info!("Bybit spot symbols related to linear {:?}", spot_symbols_related_to_linear.len());
        print_symbol_comparison(
            &linear_contract_symbols,
            &symbols,
            &spot_symbols_related_to_linear
        );
        Ok(spot_symbols_related_to_linear)
    }


    pub async fn get_symbols(&self) -> Result<Vec<String>> {
        match self.get_exchange().as_str() {
            //币安u本位期货合约
            "binance-futures" => Self::get_symbol_for_binance_futures(&self.symbol_socket).await,
            //币安u本位期货合约对应的现货
            "binance" => Self::get_spot_symbols_related_to_binance_futures(&self.symbol_socket).await,
            //OKEXu本位期货合约
            "okex-swap" => Self::get_symbol_for_okex_swap(&self.symbol_socket).await,
            //OKEXu本位期货合约对应的现货
            "okex" => Self::get_spot_symbols_related_to_okex_swap(&self.symbol_socket).await,
            //Bybitu本位期货合约
            "bybit" => Self::get_symbol_for_bybit_linear(&self.symbol_socket).await,
            //Bybitu本位期货合约对应的现货
            "bybit-spot" => Self::get_spot_symbols_related_to_bybit(&self.symbol_socket).await,
            _ => anyhow::bail!("Unsupported exchange: {}", self.get_exchange()),
        }
    }
}