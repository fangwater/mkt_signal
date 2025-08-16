use crate::mkt_msg::{SignalMsg, SignalSource, KlineMsg, LiquidationMsg, MarkPriceMsg, IndexPriceMsg, FundingRateMsg};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use tokio::sync::broadcast;
use std::collections::HashSet;

pub struct OkexSignalParser {
    source: SignalSource,
}

impl OkexSignalParser {
    pub fn new(is_ipc: bool) -> Self {
        Self {
            source: if is_ipc { SignalSource::Ipc } else { SignalSource::Tcp },
        }
    }
}

impl Parser for OkexSignalParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse OKEx depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract timestamp from data[0].ts
                if let Some(timestamp) = json_value
                    .get("data").and_then(|v| v.get(0))
                    .and_then(|item| item.get("ts"))
                    .and_then(|ts| ts.as_str())
                    .and_then(|s| s.parse::<i64>().ok()) {
                    
                    // Create signal message
                    let signal_msg = SignalMsg::create(self.source, timestamp);
                    let signal_bytes = signal_msg.to_bytes();
                    
                    // Send signal
                    if sender.send(signal_bytes).is_ok() {
                        return 1;
                    }
                }
            }
        }
        0
    }
}

pub struct OkexKlineParser;

impl OkexKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for OkexKlineParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse OKEx kline message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract symbol from arg.instId
                if let Some(symbol) = json_value
                    .get("arg")
                    .and_then(|arg| arg.get("instId"))
                    .and_then(|inst_id| inst_id.as_str()) {
                    
                    // Extract kline data from data array
                    if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                        if let Some(kline_data) = data_array.first().and_then(|v| v.as_array()) {
                            if kline_data.len() >= 9 {
                                // 检查K线状态 - 只处理已完结的K线（状态为"1"）
                                if let Some(status) = kline_data[8].as_str() {
                                    if status != "1" {
                                        return 0; // 未完结的K线，不处理
                                    }
                                } else {
                                    return 0; // 状态字段无效
                                }
                                
                                // Parse kline data: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                                if let (Some(ts_str), Some(o_str), Some(h_str), Some(l_str), Some(c_str), Some(vol_str)) = (
                                    kline_data[0].as_str(),
                                    kline_data[1].as_str(),
                                    kline_data[2].as_str(),
                                    kline_data[3].as_str(),
                                    kline_data[4].as_str(),
                                    kline_data[5].as_str(),
                                ) {
                                    // Parse all values
                                    if let (Ok(timestamp), Ok(open), Ok(high), Ok(low), Ok(close), Ok(volume)) = (
                                        ts_str.parse::<i64>(),
                                        o_str.parse::<f64>(),
                                        h_str.parse::<f64>(),
                                        l_str.parse::<f64>(),
                                        c_str.parse::<f64>(),
                                        vol_str.parse::<f64>(),
                                    ) {
                                        // Create kline message
                                        let kline_msg = KlineMsg::create(
                                            symbol.to_string(),
                                            open,
                                            high,
                                            low,
                                            close,
                                            volume,
                                            timestamp,
                                        );
                                        
                                        // Send kline message
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

pub struct OkexDerivativesMetricsParser {
    symbols: HashSet<String>,
}

impl OkexDerivativesMetricsParser {
    pub fn new(symbols: Vec<String>) -> Self {
        let symbols_set: HashSet<String> = symbols.into_iter().collect();
        Self {
            symbols: symbols_set,
        }
    }
}

impl Parser for OkexDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse OKEx derivatives metrics messages (liquidations + mark price + funding rate + index price)
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(arg) = json_value.get("arg") {
                    if let Some(channel) = arg.get("channel").and_then(|v| v.as_str()) {
                        match channel {
                            "liquidation-orders" => return self.parse_liquidation_data(&json_value, sender),
                            "mark-price" => return self.parse_mark_price_data(&json_value, sender),
                            "funding-rate" => return self.parse_funding_rate_data(&json_value, sender),
                            "index-tickers" => return self.parse_index_price_data(&json_value, sender),
                            _ => return 0,
                        }
                    }
                }
            }
        }
        0
    }
}

impl OkexDerivativesMetricsParser {
    fn parse_liquidation_data(&self, json_value: &serde_json::Value, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse liquidation data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;
            
            for data_item in data_array {
                if let Some(details_array) = data_item.get("details").and_then(|v| v.as_array()) {
                    // Extract symbol from instId
                    if let Some(inst_id) = data_item.get("instId").and_then(|v| v.as_str()) {
                        // Check if symbol is in the allowed list
                        if !self.symbols.contains(inst_id) {
                            continue;
                        }
                        
                        for detail in details_array {
                            if let (Some(side), Some(sz_str), Some(bk_px_str), Some(timestamp_str)) = (
                                detail.get("side").and_then(|v| v.as_str()),
                                detail.get("sz").and_then(|v| v.as_str()),
                                detail.get("bkPx").and_then(|v| v.as_str()),
                                detail.get("ts").and_then(|v| v.as_str()),
                            ) {
                                // Parse size, price and timestamp
                                if let (Ok(size), Ok(price), Ok(timestamp)) = (
                                    sz_str.parse::<f64>(),
                                    bk_px_str.parse::<f64>(),
                                    timestamp_str.parse::<i64>(),
                                ) {
                                    // Convert OKEx side to liquidation_side char
                                    let liquidation_side = match side {
                                        "buy" => 'B',   // 买入平仓
                                        "sell" => 'S',  // 卖出平仓
                                        _ => continue,
                                    };
                                    
                                    // Create liquidation message
                                    let liquidation_msg = LiquidationMsg::create(
                                        inst_id.to_string(),
                                        liquidation_side,
                                        size,
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
                    }
                }
            }
            
            return parsed_count;
        }
        0
    }
    
    fn parse_mark_price_data(&self, json_value: &serde_json::Value, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse mark price data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;
            
            for data_item in data_array {
                if let (Some(inst_id), Some(mark_px_str), Some(timestamp_str)) = (
                    data_item.get("instId").and_then(|v| v.as_str()),
                    data_item.get("markPx").and_then(|v| v.as_str()),
                    data_item.get("ts").and_then(|v| v.as_str()),
                ) {
                    // Parse mark price and timestamp
                    if let (Ok(mark_price), Ok(timestamp)) = (
                        mark_px_str.parse::<f64>(),
                        timestamp_str.parse::<i64>(),
                    ) {
                        // Create mark price message
                        let mark_price_msg = MarkPriceMsg::create(
                            inst_id.to_string(),
                            mark_price,
                            timestamp,
                        );
                        
                        // Send mark price message
                        if sender.send(mark_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
            }
            
            return parsed_count;
        }
        0
    }
    
    fn parse_funding_rate_data(&self, json_value: &serde_json::Value, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse funding rate data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;
            
            for data_item in data_array {
                if let (Some(inst_id), Some(funding_rate_str), Some(next_funding_time_str), Some(timestamp_str)) = (
                    data_item.get("instId").and_then(|v| v.as_str()),
                    data_item.get("fundingRate").and_then(|v| v.as_str()),
                    data_item.get("nextFundingTime").and_then(|v| v.as_str()),
                    data_item.get("ts").and_then(|v| v.as_str()),
                ) {
                    // Parse funding rate, next funding time and timestamp
                    if let (Ok(funding_rate), Ok(next_funding_time), Ok(timestamp)) = (
                        funding_rate_str.parse::<f64>(),
                        next_funding_time_str.parse::<i64>(),
                        timestamp_str.parse::<i64>(),
                    ) {
                        // Create funding rate message
                        let funding_rate_msg = FundingRateMsg::create(
                            inst_id.to_string(),
                            funding_rate,
                            next_funding_time,
                            timestamp,
                        );
                        
                        // Send funding rate message
                        if sender.send(funding_rate_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
            }
            
            return parsed_count;
        }
        0
    }
    
    fn parse_index_price_data(&self, json_value: &serde_json::Value, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse index price data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;
            
            for data_item in data_array {
                if let (Some(inst_id), Some(idx_px_str), Some(timestamp_str)) = (
                    data_item.get("instId").and_then(|v| v.as_str()),
                    data_item.get("idxPx").and_then(|v| v.as_str()),
                    data_item.get("ts").and_then(|v| v.as_str()),
                ) {
                    // Parse index price and timestamp
                    if let (Ok(index_price), Ok(timestamp)) = (
                        idx_px_str.parse::<f64>(),
                        timestamp_str.parse::<i64>(),
                    ) {
                        // Create index price message
                        let index_price_msg = IndexPriceMsg::create(
                            inst_id.to_string(),
                            index_price,
                            timestamp,
                        );
                        
                        // Send index price message
                        if sender.send(index_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
            }
            
            return parsed_count;
        }
        0
    }
}