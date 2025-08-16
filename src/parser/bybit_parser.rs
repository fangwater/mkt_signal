use crate::mkt_msg::{SignalMsg, SignalSource, KlineMsg};
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