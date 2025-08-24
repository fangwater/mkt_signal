use serde::Deserialize;
use tokio::fs;
use serde_yaml;
use anyhow::{Context, Result};
use log::info;
use tokio::net::UnixStream;
use tokio::io::AsyncReadExt;
use prettytable::{Table, Row, Cell, format};
use crate::Exchange;

#[derive(Debug, Deserialize)]
struct ConfigFile {
    is_primary: bool,
    restart_duration_secs: u64,
    snapshot_requery_time: Option<String>,
    symbol_socket: String,
    binance: ZmqProxyCfg,
    #[serde(rename = "binance-futures")]
    binance_futures: ZmqProxyCfg,
    okex: ZmqProxyCfg,
    #[serde(rename = "okex-swap")]
    okex_swap: ZmqProxyCfg,
    bybit: ZmqProxyCfg,
    #[serde(rename = "bybit-spot")]
    bybit_spot: ZmqProxyCfg,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ZmqProxyCfg {
    pub ipc_path: String,
    pub primary_addr: String,
    pub secondary_addr: String,
    pub hwm: u32,
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
    pub exchange: Exchange,  // 在运行时设置，不从配置文件读取
    pub binance: ZmqProxyCfg,
    #[serde(rename = "binance-futures")]
    pub binance_futures: ZmqProxyCfg,
    pub okex: ZmqProxyCfg,
    #[serde(rename = "okex-swap")]
    pub okex_swap: ZmqProxyCfg,
    pub bybit: ZmqProxyCfg,
    #[serde(rename = "bybit-spot")]
    pub bybit_spot: ZmqProxyCfg,
}

impl Config {
    pub async fn load_config(path: &str, exchange: Exchange) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path).await?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;
        
        // 构造 Config 结构体
        let config = Config {
            is_primary: config_file.is_primary,
            restart_duration_secs: config_file.restart_duration_secs,
            snapshot_requery_time: config_file.snapshot_requery_time,
            symbol_socket: config_file.symbol_socket,
            exchange,  // 从命令行参数设置
            binance: config_file.binance,
            binance_futures: config_file.binance_futures,
            okex: config_file.okex,
            okex_swap: config_file.okex_swap,
            bybit: config_file.bybit,
            bybit_spot: config_file.bybit_spot,
        };
        
        Ok(config)
    }

    pub fn get_exchange(&self) -> String {
        match self.exchange {
            Exchange::Binance => "binance".to_string(),
            Exchange::BinanceFutures => "binance-futures".to_string(),
            Exchange::Okex => "okex".to_string(),
            Exchange::OkexSwap => "okex-swap".to_string(),
            Exchange::Bybit => "bybit".to_string(),
            Exchange::BybitSpot => "bybit-spot".to_string(),
        }
    }

    pub fn get_batch_size(&self) -> usize {
        match self.exchange {
            Exchange::BinanceFutures => 50,
            Exchange::Binance => 100,
            Exchange::OkexSwap => 150,
            Exchange::Okex => 150,
            Exchange::Bybit => 300,
            Exchange::BybitSpot => 10,
        }
    }

    pub fn get_zmq_proxy(&self) -> ZmqProxyCfg {
        match self.exchange {
            Exchange::BinanceFutures => self.binance_futures.clone(),
            Exchange::Binance => self.binance.clone(),
            Exchange::OkexSwap => self.okex_swap.clone(),
            Exchange::Okex => self.okex.clone(),
            Exchange::Bybit => self.bybit.clone(),
            Exchange::BybitSpot => self.bybit_spot.clone(),
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
        let spot_symbols_related_to_futures: Vec<String> = symbols.iter()
            .filter(|spot_symbol| {
                futures_symbols.iter().any(|futures_symbol| {
                    Self::is_spot_symbol_related_to_futures(spot_symbol, futures_symbol)
                })
            })
            .cloned()
            .collect();
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
            .filter(|spot_symbol| {
                swap_symbols.iter().any(|swap_symbol| {
                    Self::is_spot_symbol_related_to_okex_swap(spot_symbol, swap_symbol)
                })
            })
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
            .filter(|spot_symbol| {
                linear_contract_symbols.iter().any(|linear_symbol| {
                    Self::is_spot_symbol_related_to_futures(spot_symbol, linear_symbol)
                })
            })
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
        match self.exchange {
            //币安u本位期货合约
            Exchange::BinanceFutures => Self::get_symbol_for_binance_futures(&self.symbol_socket).await,
            //币安u本位期货合约对应的现货
            Exchange::Binance => Self::get_spot_symbols_related_to_binance_futures(&self.symbol_socket).await,
            //OKEXu本位期货合约
            Exchange::OkexSwap => Self::get_symbol_for_okex_swap(&self.symbol_socket).await,
            //OKEXu本位期货合约对应的现货
            Exchange::Okex => Self::get_spot_symbols_related_to_okex_swap(&self.symbol_socket).await,
            //Bybitu本位期货合约
            Exchange::Bybit => Self::get_symbol_for_bybit_linear(&self.symbol_socket).await,
            //Bybitu本位期货合约对应的现货
            Exchange::BybitSpot => Self::get_spot_symbols_related_to_bybit(&self.symbol_socket).await,
        }
    }

    // 添加辅助函数来检查现货符号是否与期货符号匹配
    fn is_spot_symbol_related_to_futures(spot_symbol: &str, futures_symbol: &str) -> bool {
        // 直接匹配
        if spot_symbol == futures_symbol {
            return true;
        }
        
        // 处理杠杆合约的情况：1000XXXUSDT vs XXXUSDT
        if futures_symbol.starts_with("1000") && futures_symbol.to_lowercase().ends_with("usdt") {
            let base_symbol = &futures_symbol[4..]; // 去掉"1000"前缀
            if spot_symbol == base_symbol {
                return true;
            }
        }
        false
    }
    // okex-swap 和 okex 的符号特殊，需要特殊处理
    fn is_spot_symbol_related_to_okex_swap(spot_symbol: &str, swap_symbol: &str) -> bool {
        // swap_symbol 是 "TRX-USDT-SWAP"
        // spot_symbol 是 "TRX-USDT"
        // 需要判断 swap_symbol 是否是 spot_symbol 的衍生符号
        // 衍生符号的规则是：swap_symbol 的 "SWAP" 部分去掉，然后加上 "USDT"
        // 匹配规则是，去掉swap_symbol的"SWAP"部分，然后加上"USDT"，如果和spot_symbol匹配，则返回true
        let swap_symbol_without_swap = swap_symbol.replace("-SWAP", "");
        if spot_symbol == swap_symbol_without_swap {
            return true;
        }
        // okex没有杠杆合约的情况，所以不需要处理
        false
    }
}