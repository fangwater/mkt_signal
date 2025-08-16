use crate::mkt_msg::{SignalMsg, SignalSource, KlineMsg, LiquidationMsg, MarkPriceMsg, IndexPriceMsg, FundingRateMsg};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use tokio::sync::broadcast;
use std::collections::HashSet;

pub struct BinanceSignalParser {
    source: SignalSource,
}

impl BinanceSignalParser {
    pub fn new(is_ipc: bool) -> Self {
        Self {
            source: if is_ipc { SignalSource::Ipc } else { SignalSource::Tcp },
        }
    }
}

impl Parser for BinanceSignalParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Binance depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract Binance timestamp field "E"
                if let Some(timestamp) = json_value.get("E").and_then(|v| v.as_i64()) {
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

pub struct BinanceKlineParser;

impl BinanceKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceKlineParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Binance kline message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 从顶层s字段直接获取symbol
                if let Some(symbol) = json_value.get("s").and_then(|v| v.as_str()) {
                    // 获取k对象中的K线数据
                    if let Some(kline_obj) = json_value.get("k") {
                        // 检查x字段 - 只处理已关闭的K线
                        if let Some(is_closed) = kline_obj.get("x").and_then(|v| v.as_bool()) {
                            if !is_closed {
                                return 0; // K线未关闭，不处理
                            }
                        } else {
                            return 0; // x字段无效或缺失
                        }
                        
                        // 从k对象中提取OHLCV数据
                        if let (Some(open_str), Some(high_str), Some(low_str), Some(close_str), Some(volume_str), Some(timestamp)) = (
                            kline_obj.get("o").and_then(|v| v.as_str()),
                            kline_obj.get("h").and_then(|v| v.as_str()),
                            kline_obj.get("l").and_then(|v| v.as_str()),
                            kline_obj.get("c").and_then(|v| v.as_str()),
                            kline_obj.get("v").and_then(|v| v.as_str()),
                            kline_obj.get("t").and_then(|v| v.as_i64()),
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
        0
    }
}

pub struct BinanceDerivativesMetricsParser {
    symbols: HashSet<String>,
}

impl BinanceDerivativesMetricsParser {
    pub fn new(symbols: Vec<String>) -> Self {
        let symbols_set: HashSet<String> = symbols.into_iter().collect();
        Self {
            symbols: symbols_set,
        }
    }
}

impl Parser for BinanceDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Binance derivatives metrics messages (liquidations + mark price)
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                
                // Handle mark price array format: [{e: "markPriceUpdate", ...}, ...]
                if let Some(data_array) = json_value.as_array() {
                    return self.parse_mark_price_array(data_array, sender);
                }
                
                // Handle single liquidation event format: {e: "forceOrder", ...}
                if let Some(event_type) = json_value.get("e").and_then(|v| v.as_str()) {
                    match event_type {
                        "forceOrder" => return self.parse_liquidation_event(&json_value, sender),
                        "markPriceUpdate" => return self.parse_single_mark_price(&json_value, sender),
                        _ => return 0,
                    }
                }
            }
        }
        0
    }
}

impl BinanceDerivativesMetricsParser {
    fn parse_liquidation_event(&self, json_value: &serde_json::Value, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse liquidation order data
        if let Some(order_data) = json_value.get("o") {
            if let (Some(symbol), Some(side), Some(quantity_str), Some(avg_price_str), Some(timestamp)) = (
                order_data.get("s").and_then(|v| v.as_str()),
                order_data.get("S").and_then(|v| v.as_str()),
                order_data.get("z").and_then(|v| v.as_str()), // Order Filled Accumulated Quantity
                order_data.get("ap").and_then(|v| v.as_str()), // Average Price
                order_data.get("T").and_then(|v| v.as_i64()), // Order Trade Time
            ) {
                // Check if symbol is in the allowed list
                if !self.symbols.contains(symbol) {
                    return 0;
                }
                // Parse quantity and price
                if let (Ok(quantity), Ok(avg_price)) = (
                    quantity_str.parse::<f64>(),
                    avg_price_str.parse::<f64>(),
                ) {
                    // Convert Binance side to liquidation_side char
                    let liquidation_side = match side {
                        "BUY" => 'B',   // 买入强平
                        "SELL" => 'S',  // 卖出强平
                        _ => return 0,
                    };
                    
                    // Create liquidation message
                    let liquidation_msg = LiquidationMsg::create(
                        symbol.to_string(),
                        liquidation_side,
                        quantity,
                        avg_price,
                        timestamp,
                    );
                    
                    // Send liquidation message
                    if sender.send(liquidation_msg.to_bytes()).is_ok() {
                        return 1;
                    }
                }
            }
        }
        0
    }
    
    fn parse_mark_price_array(&self, data_array: &Vec<serde_json::Value>, sender: &broadcast::Sender<Bytes>) -> usize {
        let mut total_parsed = 0;
        
        for item in data_array {
            total_parsed += self.parse_single_mark_price(item, sender);
        }
        
        total_parsed
    }
    
    fn parse_single_mark_price(&self, item: &serde_json::Value, sender: &broadcast::Sender<Bytes>) -> usize {
        // Check if this is a markPriceUpdate event
        if let Some(event_type) = item.get("e").and_then(|v| v.as_str()) {
            if event_type == "markPriceUpdate" {
                if let (Some(symbol), Some(mark_price_str), Some(index_price_str), Some(funding_rate_str), Some(event_time), Some(next_funding_time)) = (
                    item.get("s").and_then(|v| v.as_str()),
                    item.get("p").and_then(|v| v.as_str()),
                    item.get("i").and_then(|v| v.as_str()),
                    item.get("r").and_then(|v| v.as_str()),
                    item.get("E").and_then(|v| v.as_i64()),
                    item.get("T").and_then(|v| v.as_i64()),
                ) {
                    // Check if symbol is in the allowed list
                    if !self.symbols.contains(symbol) {
                        return 0;
                    }
                    // Parse price values
                    if let (Ok(mark_price), Ok(index_price), Ok(funding_rate)) = (
                        mark_price_str.parse::<f64>(),
                        index_price_str.parse::<f64>(),
                        funding_rate_str.parse::<f64>(),
                    ) {
                        let mut parsed_count = 0;
                        
                        // Create and send MarkPriceMsg
                        let mark_price_msg = MarkPriceMsg::create(
                            symbol.to_string(),
                            mark_price,
                            event_time,
                        );
                        if sender.send(mark_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                        
                        // Create and send IndexPriceMsg
                        let index_price_msg = IndexPriceMsg::create(
                            symbol.to_string(),
                            index_price,
                            event_time,
                        );
                        if sender.send(index_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                        
                        // Create and send FundingRateMsg
                        let funding_rate_msg = FundingRateMsg::create(
                            symbol.to_string(),
                            funding_rate,
                            next_funding_time,
                            event_time,
                        );
                        if sender.send(funding_rate_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                        
                        return parsed_count;
                    }
                }
            }
        }
        0
    }
}