use crate::mkt_msg::{SignalMsg, SignalSource, KlineMsg, LiquidationMsg, MarkPriceMsg, IndexPriceMsg, FundingRateMsg};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use tokio::sync::broadcast;

pub struct BybitSignalParser {
    source: SignalSource,
}

impl BybitSignalParser {
    pub fn new(is_ipc: bool) -> Self {
        Self {
            source: if is_ipc { SignalSource::Ipc } else { SignalSource::Tcp },
        }
    }
}

impl Parser for BybitSignalParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Bybit depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract Bybit timestamp field "ts"
                if let Some(timestamp) = json_value.get("ts").and_then(|v| v.as_i64()) {
                    // Create signal message
                    let signal_msg = SignalMsg::create(self.source, timestamp);
                    let signal_bytes = signal_msg.to_bytes();
                    
                    // Send signal
                    if let Err(_) = sender.send(signal_bytes) {
                        return 0;
                    }
                    
                    return 1;
                }
            }
        }
        0
    }
}

pub struct BybitKlineParser;

impl BybitKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitKlineParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Bybit kline message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 首先检查confirm字段 - 只处理已确认的K线数据
                if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                    if let Some(kline_data) = data_array.first() {
                        // 检查confirm字段 - 只处理已确认的K线数据
                        if let Some(confirm) = kline_data.get("confirm").and_then(|v| v.as_bool()) {
                            if confirm == false {
                                return 0; // 未确认的K线，不处理
                            }
                        } else {
                            return 0; // confirm字段无效或缺失
                        }
                        // 从topic字段提取symbol，例如从"kline.1.BTCUSDT"提取"BTCUSDT"
                        if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                            if let Some(symbol) = topic.split('.').last() {
                                // 从kline_data对象中提取OHLCV数据
                                if let (Some(open_str), Some(high_str), Some(low_str), Some(close_str), Some(volume_str), Some(timestamp)) = (
                                    kline_data.get("open").and_then(|v| v.as_str()),
                                    kline_data.get("high").and_then(|v| v.as_str()),
                                    kline_data.get("low").and_then(|v| v.as_str()),
                                    kline_data.get("close").and_then(|v| v.as_str()),
                                    kline_data.get("volume").and_then(|v| v.as_str()),
                                    kline_data.get("timestamp").and_then(|v| v.as_i64()),
                                ) {
                                    // 解析价格和成交量数据
                                    if let (Ok(open), Ok(high), Ok(low), Ok(close), Ok(volume)) = (
                                        open_str.parse::<f64>(),
                                        high_str.parse::<f64>(),
                                        low_str.parse::<f64>(),
                                        close_str.parse::<f64>(),
                                        volume_str.parse::<f64>(),
                                    ) {
                                        // 创建K线消息
                                        let kline_msg = KlineMsg::create(
                                            symbol.to_string(),
                                            open,
                                            high,
                                            low,
                                            close,
                                            volume,
                                            timestamp,
                                        );
                                        
                                        // 发送K线消息
                                        if sender.send(kline_msg.to_bytes()).is_ok() {
                                            return 1;
                                        }
                                    }
                                }
                            }
                        }
                        
                    }
                }
                
            }
        }
        0
    }
}

pub struct BybitDerivativesMetricsParser;

impl BybitDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Bybit derivatives metrics messages (liquidations + tickers)
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    // Route based on topic prefix
                    if topic.starts_with("allLiquidation.") {
                        return self.parse_liquidation_data(&json_value, sender);
                    } else if topic.starts_with("tickers.") {
                        return self.parse_ticker_data(&json_value, sender);
                    }
                }
            }
        }
        0
    }
}

impl BybitDerivativesMetricsParser {
    fn parse_liquidation_data(&self, json_value: &serde_json::Value, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse liquidation data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;
            
            for liquidation_data in data_array {
                if let (Some(symbol), Some(side), Some(volume_str), Some(price_str), Some(timestamp)) = (
                    liquidation_data.get("s").and_then(|v| v.as_str()),
                    liquidation_data.get("S").and_then(|v| v.as_str()),
                    liquidation_data.get("v").and_then(|v| v.as_str()),
                    liquidation_data.get("p").and_then(|v| v.as_str()),
                    liquidation_data.get("T").and_then(|v| v.as_i64()),
                ) {
                    // Parse volume and price
                    if let (Ok(volume), Ok(price)) = (
                        volume_str.parse::<f64>(),
                        price_str.parse::<f64>(),
                    ) {
                        // Convert Bybit side to liquidation_side char
                        // Bybit: "Buy" = long liquidated, "Sell" = short liquidated
                        let liquidation_side = match side {
                            "Buy" => 'B',
                            "Sell" => 'S',
                            _ => continue,
                        };
                        
                        // Create liquidation message
                        let liquidation_msg = LiquidationMsg::create(
                            symbol.to_string(),
                            liquidation_side,
                            volume,
                            price,
                            timestamp,
                        );
                        
                        // Send liquidation message
                        if sender.send(liquidation_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
            }
            
            return parsed_count;
        }
        0
    }
    
    fn parse_ticker_data(&self, json_value: &serde_json::Value, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse ticker data - contains mark price, index price, and funding rate
        if let Some(data) = json_value.get("data") {
            if let (Some(symbol), Some(timestamp)) = (
                data.get("symbol").and_then(|v| v.as_str()),
                json_value.get("ts").and_then(|v| v.as_i64()),
            ) {
                let mut parsed_count = 0;
                
                // Parse mark price
                if let Some(mark_price_str) = data.get("markPrice").and_then(|v| v.as_str()) {
                    if let Ok(mark_price) = mark_price_str.parse::<f64>() {
                        let mark_price_msg = MarkPriceMsg::create(
                            symbol.to_string(),
                            mark_price,
                            timestamp,
                        );
                        
                        if sender.send(mark_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
                
                // Parse index price
                if let Some(index_price_str) = data.get("indexPrice").and_then(|v| v.as_str()) {
                    if let Ok(index_price) = index_price_str.parse::<f64>() {
                        let index_price_msg = IndexPriceMsg::create(
                            symbol.to_string(),
                            index_price,
                            timestamp,
                        );
                        
                        if sender.send(index_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
                
                // Parse funding rate
                if let (Some(funding_rate_str), Some(next_funding_time_str)) = (
                    data.get("fundingRate").and_then(|v| v.as_str()),
                    data.get("nextFundingTime").and_then(|v| v.as_str()),
                ) {
                    if let (Ok(funding_rate), Ok(next_funding_time)) = (
                        funding_rate_str.parse::<f64>(),
                        next_funding_time_str.parse::<i64>(),
                    ) {
                        let funding_rate_msg = FundingRateMsg::create(
                            symbol.to_string(),
                            funding_rate,
                            next_funding_time,
                            timestamp,
                        );
                        
                        if sender.send(funding_rate_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
                
                return parsed_count;
            }
        }
        0
    }
}