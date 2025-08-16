use crate::mkt_msg::{SignalMsg, SignalSource, KlineMsg};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use tokio::sync::broadcast;

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