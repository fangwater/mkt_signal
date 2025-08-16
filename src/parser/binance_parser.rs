use crate::mkt_msg::{SignalMsg, SignalSource, KlineMsg};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use tokio::sync::broadcast;

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