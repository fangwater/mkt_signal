use crate::exchange::Exchange;
use crate::signal::common::TradingVenue;
use anyhow::{Context, Result};
use log::info;
use prettytable::{format, Cell, Row, Table};
use serde::Deserialize;
use serde_yaml;
use std::time::Duration;
use tokio::fs;

#[derive(Debug, Deserialize)]
struct ConfigFile {
    restart_duration_secs: u64,
    primary_local_ip: String,
    secondary_local_ip: String,
    // 数据类型开关
    data_types: DataTypesConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DataTypesConfig {
    pub enable_incremental: bool,    // 增量行情
    pub enable_trade: bool,          // 逐笔成交
    pub enable_kline: bool,          // K线数据
    pub enable_derivatives: bool,    // 衍生品指标
    pub enable_ask_bid_spread: bool, // 买卖价差（最优买卖价）
    #[serde(default)]
    pub max_levels_per_msg: Option<usize>, // 每条增量消息最大档数限制(bids+asks)，超过则截断
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfoResponse {
    symbols: Vec<BinanceSymbolInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSymbolInfo {
    symbol: String,
    status: String,
    quote_asset: String,
    #[serde(default)]
    contract_type: Option<String>,
    #[serde(default)]
    is_spot_trading_allowed: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrumentsResponse {
    ret_code: i32,
    ret_msg: String,
    result: Option<BybitInstrumentsResult>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrumentsResult {
    #[serde(default)]
    list: Vec<BybitInstrument>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrument {
    symbol: String,
    status: String,
    #[serde(default)]
    quote_coin: Option<String>,
    #[serde(default)]
    contract_type: Option<String>,
}

// 添加表格打印函数
fn print_symbol_comparison(
    futures_symbols: &[String],
    spot_symbols: &[String],
    common_symbols: &[String],
) {
    let mut table = Table::new();

    // 设置表格格式
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    table.set_titles(Row::from(vec![
        Cell::new("Symbol").style_spec("c"),
        Cell::new("Futures").style_spec("c"),
        Cell::new("Spot").style_spec("c"),
    ]));

    // 添加公共交易对（两者都存在）
    for symbol in common_symbols {
        table.add_row(Row::from(vec![
            Cell::new(symbol),
            Cell::new("✓").style_spec("Fg"),
            Cell::new("✓").style_spec("Fg"),
        ]));
    }

    // 添加只有 futures 的交易对
    for symbol in futures_symbols {
        if !common_symbols.contains(symbol) {
            table.add_row(Row::from(vec![
                Cell::new(symbol),
                Cell::new("✓").style_spec("Fg"),
                Cell::new("✗").style_spec("Fr"),
            ]));
        }
    }

    // 添加只有 spot 的交易对
    for symbol in spot_symbols {
        if !common_symbols.contains(symbol) {
            table.add_row(Row::from(vec![
                Cell::new(symbol),
                Cell::new("✗").style_spec("Fr"),
                Cell::new("✓").style_spec("Fg"),
            ]));
        }
    }

    // 打印表格
    info!("Symbol comparison:");
    table.printstd();
}

/// 仅打印匹配到的成对交易对（左：衍生品；右：现货/杠杆）
fn print_matched_pairs(title_left: &str, title_right: &str, pairs: &[(String, String)]) {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    table.set_titles(Row::from(vec![
        Cell::new(title_left).style_spec("c"),
        Cell::new(title_right).style_spec("c"),
    ]));

    for (left, right) in pairs {
        table.add_row(Row::from(vec![Cell::new(left), Cell::new(right)]));
    }

    info!(
        "Matched symbols ({} <-> {}): {}",
        title_left,
        title_right,
        pairs.len()
    );
    table.printstd();
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct Config {
    pub restart_duration_secs: u64,
    pub primary_local_ip: String,
    pub secondary_local_ip: String,
    pub data_types: DataTypesConfig, // 数据类型开关
    pub venue: TradingVenue,         // 在运行时设置，不从配置文件读取（区分资产类型）
}

impl Config {
    pub async fn load_config(path: &str, venue: TradingVenue) -> Result<Self> {
        let content = fs::read_to_string(path)
            .await
            .with_context(|| format!("read mkt cfg: {}", path))?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)
            .with_context(|| format!("parse mkt cfg: {}", path))?;

        // 构造 Config 结构体，使用默认值如果配置不存在
        let config = Config {
            restart_duration_secs: config_file.restart_duration_secs,
            primary_local_ip: config_file.primary_local_ip,
            secondary_local_ip: config_file.secondary_local_ip,
            data_types: config_file.data_types, // 数据类型开关
            venue, // 从命令行参数设置
        };

        Ok(config)
    }

    pub fn get_exchange(&self) -> Exchange {
        Exchange::from_str(self.venue.trade_engine_exchange())
            .expect("Invalid exchange name in venue configuration")
    }

    pub fn get_batch_size(&self) -> usize {
        match self.venue {
            TradingVenue::BinanceFutures => 50,
            TradingVenue::BinanceMargin => 100,
            TradingVenue::OkexFutures | TradingVenue::OkexMargin => 50,
            TradingVenue::BybitFutures => 300,
            TradingVenue::BybitMargin => 10,
            TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => 50,
            TradingVenue::GateMargin | TradingVenue::GateFutures => 50,
        }
    }

    async fn fetch_binance_exchange_info(url: &str) -> Result<BinanceExchangeInfoResponse> {
        info!("Fetching Binance exchangeInfo from: {}", url);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("Failed to build Binance HTTP client")?;

        let response = client
            .get(url)
            .send()
            .await
            .context("Failed to request Binance exchangeInfo")?;

        let status = response.status();
        if !status.is_success() {
            anyhow::bail!(
                "Binance exchangeInfo request failed: status={} url={}",
                status,
                url
            );
        }

        let body = response
            .text()
            .await
            .context("Failed to read Binance exchangeInfo response body")?;
        let parsed: BinanceExchangeInfoResponse =
            serde_json::from_str(&body).context("Failed to parse Binance exchangeInfo JSON")?;
        Ok(parsed)
    }

    async fn get_symbol_for_binance_futures() -> Result<Vec<String>> {
        const URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
        let info = Self::fetch_binance_exchange_info(URL).await?;
        let symbols: Vec<String> = info
            .symbols
            .into_iter()
            .filter(|s| s.quote_asset == "USDT")
            .filter(|s| s.status == "TRADING")
            .filter(|s| s.contract_type.as_deref() == Some("PERPETUAL"))
            .map(|s| s.symbol)
            .collect();
        info!(
            "Binance futures USDT-denominated symbol count {:?}",
            symbols.len()
        );
        Ok(symbols)
    }

    async fn get_spot_symbols_from_binance_api() -> Result<Vec<String>> {
        const URL: &str = "https://api.binance.com/api/v3/exchangeInfo";
        let info = Self::fetch_binance_exchange_info(URL).await?;
        let symbols: Vec<String> = info
            .symbols
            .into_iter()
            .filter(|s| s.quote_asset == "USDT")
            .filter(|s| s.status == "TRADING")
            .filter(|s| s.is_spot_trading_allowed.unwrap_or(false))
            .map(|s| s.symbol)
            .collect();
        info!(
            "Binance spot USDT-denominated symbol count {:?}",
            symbols.len()
        );
        Ok(symbols)
    }

    async fn fetch_bybit_instruments(category: &str) -> Result<Vec<BybitInstrument>> {
        let url = format!(
            "https://api.bybit.com/v5/market/instruments-info?category={}",
            category
        );
        info!("Fetching Bybit instruments from: {}", url);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("Failed to build Bybit HTTP client")?;
        let response = client
            .get(&url)
            .send()
            .await
            .context("Failed to request Bybit instruments")?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!(
                "Bybit instruments request failed: status={} url={}",
                status,
                url
            );
        }

        let body = response
            .text()
            .await
            .context("Failed to read Bybit response body")?;
        let parsed: BybitInstrumentsResponse =
            serde_json::from_str(&body).context("Failed to parse Bybit response JSON")?;
        if parsed.ret_code != 0 {
            anyhow::bail!(
                "Bybit API error: code={} msg={}",
                parsed.ret_code,
                parsed.ret_msg
            );
        }
        let instruments = parsed.result.map(|r| r.list).unwrap_or_default();
        Ok(instruments)
    }

    async fn get_spot_symbols_related_to_binance_futures() -> Result<Vec<String>> {
        let symbols = Self::get_spot_symbols_from_binance_api().await?;
        let futures_symbols = Self::get_symbol_for_binance_futures().await?;
        let spot_symbols_related_to_futures: Vec<String> = symbols
            .iter()
            .filter(|spot_symbol| {
                futures_symbols.iter().any(|futures_symbol| {
                    Self::is_spot_symbol_related_to_futures(spot_symbol, futures_symbol)
                })
            })
            .cloned()
            .collect();
        info!(
            "Binance spot symbols related to futures {:?}",
            spot_symbols_related_to_futures.len()
        );
        //用三线表打印，哪些符号在futures中存在，但是在spot中不存在
        print_symbol_comparison(&futures_symbols, &symbols, &spot_symbols_related_to_futures);
        Ok(spot_symbols_related_to_futures)
    }

    /// 从 OKEx HTTP API 获取 USDT 永续合约列表
    async fn get_symbol_for_okex_swap() -> Result<Vec<String>> {
        let url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP";
        info!("Fetching OKEx swap symbols from: {}", url);
        let client = reqwest::Client::new();
        let response = client
            .get(url)
            .send()
            .await
            .context("Failed to fetch OKEx instruments")?;
        let body = response
            .text()
            .await
            .context("Failed to read OKEx response body")?;
        let json: serde_json::Value =
            serde_json::from_str(&body).context("Failed to parse OKEx response JSON")?;
        if json["code"].as_str() != Some("0") {
            return Err(anyhow::anyhow!(
                "OKEx API error: code={}, msg={}",
                json["code"],
                json["msg"]
            ));
        }
        let symbols: Vec<String> = json["data"]
            .as_array()
            .context("data field not found or not an array")?
            .iter()
            .filter(|item| item["ctType"].as_str() == Some("linear"))
            .filter(|item| item["settleCcy"].as_str() == Some("USDT"))
            .filter(|item| item["state"].as_str() == Some("live"))
            .filter_map(|item| item["instId"].as_str())
            .map(|s| s.to_string())
            .collect();
        info!("OKEx swap USDT symbol count: {}", symbols.len());
        Ok(symbols)
    }

    /// 从 OKEx HTTP API 获取现货交易对列表
    async fn get_spot_symbols_from_okex_api() -> Result<Vec<String>> {
        let url = "https://www.okx.com/api/v5/public/instruments?instType=SPOT";
        info!("Fetching OKEx spot symbols from: {}", url);
        let client = reqwest::Client::new();
        let response = client
            .get(url)
            .send()
            .await
            .context("Failed to fetch OKEx instruments")?;
        let body = response
            .text()
            .await
            .context("Failed to read OKEx response body")?;
        let json: serde_json::Value =
            serde_json::from_str(&body).context("Failed to parse OKEx response JSON")?;
        if json["code"].as_str() != Some("0") {
            return Err(anyhow::anyhow!(
                "OKEx API error: code={}, msg={}",
                json["code"],
                json["msg"]
            ));
        }
        let symbols: Vec<String> = json["data"]
            .as_array()
            .context("data field not found or not an array")?
            .iter()
            .filter(|item| item["state"].as_str() == Some("live"))
            .filter_map(|item| item["instId"].as_str())
            .filter(|s| s.ends_with("-USDT"))
            .map(|s| s.to_string())
            .collect();
        info!("OKEx spot USDT symbol count: {}", symbols.len());
        Ok(symbols)
    }

    /// 获取 OKEx 现货交易对（只返回与 SWAP 有关联的；兼容无 -SWAP 后缀）
    async fn get_spot_symbols_related_to_okex_swap() -> Result<Vec<String>> {
        use crate::symbol_match::normalize_symbol_for_pairing;
        use std::collections::HashMap;

        let spot_symbols = Self::get_spot_symbols_from_okex_api().await?;
        let swap_symbols = Self::get_symbol_for_okex_swap().await?;

        // 先按标准化符号建立现货映射，便于配对
        let mut spot_by_norm: HashMap<String, String> = HashMap::new();
        for spot in &spot_symbols {
            let norm = normalize_symbol_for_pairing(spot, "okex-spot");
            spot_by_norm.entry(norm).or_insert_with(|| spot.clone());
        }

        let mut spot_symbols_related_to_swap = Vec::new();
        let mut matched_pairs = Vec::new();
        for swap in &swap_symbols {
            let norm = normalize_symbol_for_pairing(swap, "okex-futures");
            if let Some(spot) = spot_by_norm.get(&norm) {
                spot_symbols_related_to_swap.push(spot.clone());
                matched_pairs.push((swap.clone(), spot.clone()));
            }
        }

        info!(
            "OKEx spot symbols related to swap: {}",
            spot_symbols_related_to_swap.len()
        );
        print_matched_pairs("Futures", "Spot", &matched_pairs);
        Ok(spot_symbols_related_to_swap)
    }

    async fn get_symbol_for_bybit_linear() -> Result<Vec<String>> {
        let instruments = Self::fetch_bybit_instruments("linear").await?;
        let symbols: Vec<String> = instruments
            .into_iter()
            .filter(|item| item.status == "Trading")
            .filter(|item| item.quote_coin.as_deref() == Some("USDT"))
            .filter(|item| item.contract_type.as_deref() == Some("LinearPerpetual"))
            .map(|item| item.symbol)
            .collect();
        info!(
            "Bybit linear USDT-denominated symbol count {:?}",
            symbols.len()
        );
        Ok(symbols)
    }

    async fn get_spot_symbols_related_to_bybit() -> Result<Vec<String>> {
        let spot_instruments = Self::fetch_bybit_instruments("spot").await?;
        let spot_symbols: Vec<String> = spot_instruments
            .into_iter()
            .filter(|item| item.status == "Trading")
            .filter(|item| item.quote_coin.as_deref() == Some("USDT"))
            .map(|item| item.symbol)
            .collect();
        info!(
            "Bybit spot USDT-denominated symbol count {:?}",
            spot_symbols.len()
        );
        let linear_contract_symbols = Self::get_symbol_for_bybit_linear().await?;
        let spot_symbols_related_to_linear: Vec<String> = spot_symbols
            .iter()
            .filter(|spot_symbol| {
                linear_contract_symbols.iter().any(|linear_symbol| {
                    Self::is_spot_symbol_related_to_futures(spot_symbol, linear_symbol)
                })
            })
            .cloned()
            .collect();
        info!(
            "Bybit spot symbols related to linear {:?}",
            spot_symbols_related_to_linear.len()
        );
        print_symbol_comparison(
            &linear_contract_symbols,
            &spot_symbols,
            &spot_symbols_related_to_linear,
        );
        Ok(spot_symbols_related_to_linear)
    }

    /// 从 Bitget HTTP API 获取交易对列表
    /// category: "MARGIN" 或 "USDT-FUTURES"
    async fn get_symbols_from_bitget_api(category: &str) -> Result<Vec<String>> {
        let url = format!(
            "https://api.bitget.com/api/v3/market/instruments?category={}",
            category
        );
        info!("Fetching Bitget symbols from: {}", url);

        let client = reqwest::Client::new();
        let response = client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch Bitget instruments")?;

        let body = response
            .text()
            .await
            .context("Failed to read Bitget response body")?;

        let json: serde_json::Value =
            serde_json::from_str(&body).context("Failed to parse Bitget response JSON")?;

        if json["code"].as_str() != Some("00000") {
            return Err(anyhow::anyhow!(
                "Bitget API error: code={}, msg={}",
                json["code"],
                json["msg"]
            ));
        }

        let symbols: Vec<String> = json["data"]
            .as_array()
            .context("data field not found or not an array")?
            .iter()
            .filter(|item| item["status"].as_str() == Some("online"))
            .map(|item| item["symbol"].as_str().unwrap_or("").to_string())
            .filter(|s| !s.is_empty() && s.to_lowercase().ends_with("usdt"))
            .collect();

        info!(
            "Bitget {} USDT-denominated symbol count: {}",
            category,
            symbols.len()
        );
        Ok(symbols)
    }

    /// 获取 Bitget Futures (USDT-FUTURES) 交易对
    async fn get_symbol_for_bitget_futures() -> Result<Vec<String>> {
        Self::get_symbols_from_bitget_api("USDT-FUTURES").await
    }

    /// 获取 Bitget Margin 交易对（只返回与 Futures 有关联的）
    async fn get_margin_symbols_related_to_bitget_futures() -> Result<Vec<String>> {
        let margin_symbols = Self::get_symbols_from_bitget_api("MARGIN").await?;
        let futures_symbols = Self::get_symbol_for_bitget_futures().await?;

        // Bitget 的 margin 和 futures symbol 格式相同，都是 XXXUSDT
        let margin_symbols_related_to_futures: Vec<String> = margin_symbols
            .iter()
            .filter(|margin_symbol| {
                futures_symbols.iter().any(|futures_symbol| {
                    Self::is_spot_symbol_related_to_futures(margin_symbol, futures_symbol)
                })
            })
            .cloned()
            .collect();

        info!(
            "Bitget margin symbols related to futures: {}",
            margin_symbols_related_to_futures.len()
        );
        print_symbol_comparison(
            &futures_symbols,
            &margin_symbols,
            &margin_symbols_related_to_futures,
        );
        Ok(margin_symbols_related_to_futures)
    }

    /// 从 Gate HTTP API 获取 USDT 永续合约列表
    async fn get_symbol_for_gate_futures() -> Result<Vec<String>> {
        let url = "https://api.gateio.ws/api/v4/futures/usdt/contracts";
        info!("Fetching Gate futures symbols from: {}", url);
        let client = reqwest::Client::new();
        let response = client
            .get(url)
            .send()
            .await
            .context("Failed to fetch Gate contracts")?;
        let body = response
            .text()
            .await
            .context("Failed to read Gate response body")?;
        let contracts: Vec<serde_json::Value> =
            serde_json::from_str(&body).context("Failed to parse Gate response JSON")?;
        let symbols: Vec<String> = contracts
            .iter()
            .filter(|c| c["status"].as_str() == Some("trading"))
            .filter_map(|c| c["name"].as_str())
            .filter(|s| s.to_uppercase().ends_with("_USDT"))
            .map(|s| s.to_uppercase())
            .collect();
        info!("Gate futures USDT symbol count: {}", symbols.len());
        Ok(symbols)
    }

    /// 从 Gate HTTP API 获取现货交易对列表
    async fn get_spot_symbols_from_gate_api() -> Result<Vec<String>> {
        let url = "https://api.gateio.ws/api/v4/spot/currency_pairs";
        info!("Fetching Gate spot symbols from: {}", url);
        let client = reqwest::Client::new();
        let response = client
            .get(url)
            .send()
            .await
            .context("Failed to fetch Gate currency_pairs")?;
        let body = response
            .text()
            .await
            .context("Failed to read Gate response body")?;
        let pairs: Vec<serde_json::Value> =
            serde_json::from_str(&body).context("Failed to parse Gate response JSON")?;
        let symbols: Vec<String> = pairs
            .iter()
            .filter(|p| p["trade_status"].as_str() == Some("tradable"))
            .filter_map(|p| p["id"].as_str())
            .filter(|s| s.to_uppercase().ends_with("_USDT"))
            .map(|s| s.to_uppercase())
            .collect();
        info!("Gate spot USDT symbol count: {}", symbols.len());
        Ok(symbols)
    }

    /// 获取 Gate 现货交易对（只返回与 Futures 有关联的）
    async fn get_spot_symbols_related_to_gate_futures() -> Result<Vec<String>> {
        let spot_symbols = Self::get_spot_symbols_from_gate_api().await?;
        let futures_symbols = Self::get_symbol_for_gate_futures().await?;
        // Gate 的 spot 和 futures symbol 格式相同，都是 XXX_USDT
        let spot_symbols_related_to_futures: Vec<String> = spot_symbols
            .iter()
            .filter(|spot| futures_symbols.contains(spot))
            .cloned()
            .collect();
        info!(
            "Gate spot symbols related to futures: {}",
            spot_symbols_related_to_futures.len()
        );
        print_symbol_comparison(
            &futures_symbols,
            &spot_symbols,
            &spot_symbols_related_to_futures,
        );
        Ok(spot_symbols_related_to_futures)
    }

    pub async fn get_symbols(&self) -> Result<Vec<String>> {
        match self.venue {
            //币安u本位期货合约
            TradingVenue::BinanceFutures => Self::get_symbol_for_binance_futures().await,
            TradingVenue::BinanceMargin => {
                Self::get_spot_symbols_related_to_binance_futures().await
            }
            //OKEXu本位期货合约
            TradingVenue::OkexFutures => Self::get_symbol_for_okex_swap().await,
            //OKEXu本位期货合约对应的现货
            TradingVenue::OkexMargin => Self::get_spot_symbols_related_to_okex_swap().await,
            //Bybitu本位期货合约
            TradingVenue::BybitFutures => Self::get_symbol_for_bybit_linear().await,
            //Bybitu本位期货合约对应的现货
            TradingVenue::BybitMargin => Self::get_spot_symbols_related_to_bybit().await,
            //Bitget USDT永续合约
            TradingVenue::BitgetFutures => Self::get_symbol_for_bitget_futures().await,
            //Bitget杠杆交易（与Futures关联的）
            TradingVenue::BitgetMargin => {
                Self::get_margin_symbols_related_to_bitget_futures().await
            }
            //Gate USDT永续合约
            TradingVenue::GateFutures => Self::get_symbol_for_gate_futures().await,
            //Gate现货（与Futures关联的）
            TradingVenue::GateMargin => Self::get_spot_symbols_related_to_gate_futures().await,
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
}
