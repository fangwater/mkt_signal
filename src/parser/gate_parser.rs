use crate::common::mkt_msg::{AskBidSpreadMsg, KlineMsg, SignalMsg, SignalSource};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use tokio::sync::mpsc;

/// Gate.io Signal Parser - 从 ticker 消息中提取时间戳作为信号
#[derive(Clone)]
pub struct GateSignalParser {
    source: SignalSource,
}

impl GateSignalParser {
    pub fn new(is_ipc: bool) -> Self {
        Self {
            source: if is_ipc {
                SignalSource::Ipc
            } else {
                SignalSource::Tcp
            },
        }
    }
}

impl Parser for GateSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 ticker 频道的 update 事件
                let channel = json_value
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let event = json_value
                    .get("event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if !channel.ends_with(".tickers") || event != "update" {
                    return 0;
                }

                // 获取时间戳 (使用 time_ms)
                if let Some(timestamp) = json_value.get("time_ms").and_then(|v| v.as_i64()) {
                    let signal_msg = SignalMsg::create(self.source, timestamp);
                    if tx.send(signal_msg.to_bytes()).is_ok() {
                        return 1;
                    }
                }
            }
        }
        0
    }
}

/// Gate.io Ticker Parser - 解析 ticker 数据提取最优买卖价
///
/// Gate.io ticker 消息格式:
/// ```json
/// {
///     "time": 1764559257,
///     "time_ms": 1764559257161,
///     "channel": "spot.tickers",
///     "event": "update",
///     "result": {
///         "currency_pair": "BTC_USDT",
///         "last": "86445.2",
///         "lowest_ask": "86445.2",
///         "highest_bid": "86445.1",
///         "change_percentage": "-4.4564",
///         "base_volume": "10381.529204",
///         "quote_volume": "931959066.3514661",
///         "high_24h": "91962.8",
///         "low_24h": "86325.7"
///     }
/// }
/// ```
#[derive(Clone)]
pub struct GateTickerParser;

impl GateTickerParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for GateTickerParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 ticker 频道的 update 事件
                let channel = json_value
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let event = json_value
                    .get("event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if !channel.ends_with(".tickers") || event != "update" {
                    return 0;
                }

                // 解析 result 对象
                if let Some(result) = json_value.get("result").and_then(|v| v.as_object()) {
                    // 获取 symbol
                    let symbol = match result.get("currency_pair").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => return 0,
                    };

                    // 获取时间戳 (使用 time_ms)
                    let timestamp = json_value
                        .get("time_ms")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    // 提取 bid/ask 价格
                    // Gate.io ticker 没有提供买卖量，使用 0.0 作为占位
                    let bid_price = result
                        .get("highest_bid")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok());
                    let ask_price = result
                        .get("lowest_ask")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok());

                    if let (Some(bp), Some(ap)) = (bid_price, ask_price) {
                        // 过滤无效值
                        if bp <= 0.0 || ap <= 0.0 {
                            return 0;
                        }

                        // 创建并发送消息 (bid_amount 和 ask_amount 设为 0.0)
                        let spread_msg = AskBidSpreadMsg::create(
                            symbol.to_string(),
                            timestamp,
                            bp,
                            0.0, // Gate ticker 不提供买量
                            ap,
                            0.0, // Gate ticker 不提供卖量
                        );

                        if tx.send(spread_msg.to_bytes()).is_ok() {
                            return 1;
                        }
                    }
                }
            }
        }
        0
    }
}

/// Gate.io Kline Parser - 解析 K线数据
///
/// Gate.io K线消息格式:
/// ```json
/// {
///     "time": 1764559363,
///     "time_ms": 1764559363408,
///     "channel": "spot.candlesticks",
///     "event": "update",
///     "result": {
///         "t": "1764559320",      // K线开始时间 (秒)
///         "v": "1146373.9657193", // 成交额 (quote volume)
///         "c": "86504.5",         // 收盘价
///         "h": "86577.9",         // 最高价
///         "l": "86504.5",         // 最低价
///         "o": "86525.1",         // 开盘价
///         "n": "1m_BTC_USDT",     // 周期_交易对
///         "a": "13.244502",       // 成交量 (base volume)
///         "w": false              // 是否为窗口期结束 (K线是否完结)
///     }
/// }
/// ```
#[derive(Clone)]
pub struct GateKlineParser {
    only_closed: bool, // 是否只处理已完结的K线
}

impl GateKlineParser {
    pub fn new(only_closed: bool) -> Self {
        Self { only_closed }
    }
}

impl Parser for GateKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 candlesticks 频道的 update 事件
                let channel = json_value
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let event = json_value
                    .get("event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if !channel.ends_with(".candlesticks") || event != "update" {
                    return 0;
                }

                // 解析 result 对象
                if let Some(result) = json_value.get("result").and_then(|v| v.as_object()) {
                    // 检查K线是否完结 (w: window closed)
                    if self.only_closed {
                        let is_closed = result.get("w").and_then(|v| v.as_bool()).unwrap_or(false);
                        if !is_closed {
                            return 0;
                        }
                    }

                    // 从 "n" 字段提取交易对: "1m_BTC_USDT" -> "BTC_USDT"
                    let symbol = match result.get("n").and_then(|v| v.as_str()) {
                        Some(n) => {
                            // 跳过周期前缀，如 "1m_"
                            if let Some(pos) = n.find('_') {
                                &n[pos + 1..]
                            } else {
                                n
                            }
                        }
                        None => return 0,
                    };

                    // 解析 OHLCV 数据
                    if let (
                        Some(t_str),
                        Some(o_str),
                        Some(h_str),
                        Some(l_str),
                        Some(c_str),
                        Some(a_str),
                    ) = (
                        result.get("t").and_then(|v| v.as_str()),
                        result.get("o").and_then(|v| v.as_str()),
                        result.get("h").and_then(|v| v.as_str()),
                        result.get("l").and_then(|v| v.as_str()),
                        result.get("c").and_then(|v| v.as_str()),
                        result.get("a").and_then(|v| v.as_str()), // base volume
                    ) {
                        // 解析所有值
                        if let (
                            Ok(timestamp_sec),
                            Ok(open),
                            Ok(high),
                            Ok(low),
                            Ok(close),
                            Ok(volume),
                        ) = (
                            t_str.parse::<i64>(),
                            o_str.parse::<f64>(),
                            h_str.parse::<f64>(),
                            l_str.parse::<f64>(),
                            c_str.parse::<f64>(),
                            a_str.parse::<f64>(),
                        ) {
                            // Gate 时间戳是秒，转换为毫秒
                            let timestamp = timestamp_sec * 1000;

                            // 创建 K线消息
                            let kline_msg = KlineMsg::create(
                                symbol.to_string(),
                                open,
                                high,
                                low,
                                close,
                                volume,
                                timestamp,
                            );

                            // 发送消息
                            if tx.send(kline_msg.to_bytes()).is_ok() {
                                return 1;
                            }
                        }
                    }
                }
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gate_ticker_parse() {
        let parser = GateTickerParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let msg = r#"{
            "time": 1764559257,
            "time_ms": 1764559257161,
            "channel": "spot.tickers",
            "event": "update",
            "result": {
                "currency_pair": "BTC_USDT",
                "last": "86445.2",
                "lowest_ask": "86445.2",
                "highest_bid": "86445.1",
                "change_percentage": "-4.4564",
                "base_volume": "10381.529204",
                "quote_volume": "931959066.3514661",
                "high_24h": "91962.8",
                "low_24h": "86325.7"
            }
        }"#;

        let bytes = Bytes::from(msg);
        let count = parser.parse(bytes, &tx);
        assert_eq!(count, 1);

        // 验证消息被正确发送
        let received = rx.try_recv();
        assert!(received.is_ok());
    }

    #[test]
    fn test_gate_kline_parse() {
        let parser = GateKlineParser::new(false); // 不限制只处理完结K线
        let (tx, mut rx) = mpsc::unbounded_channel();

        let msg = r#"{
            "time": 1764559363,
            "time_ms": 1764559363408,
            "channel": "spot.candlesticks",
            "event": "update",
            "result": {
                "t": "1764559320",
                "v": "1146373.9657193",
                "c": "86504.5",
                "h": "86577.9",
                "l": "86504.5",
                "o": "86525.1",
                "n": "1m_BTC_USDT",
                "a": "13.244502",
                "w": false
            }
        }"#;

        let bytes = Bytes::from(msg);
        let count = parser.parse(bytes, &tx);
        assert_eq!(count, 1);

        // 验证消息被正确发送
        let received = rx.try_recv();
        assert!(received.is_ok());
    }

    #[test]
    fn test_gate_kline_parse_only_closed() {
        let parser = GateKlineParser::new(true); // 只处理完结K线
        let (tx, _rx) = mpsc::unbounded_channel();

        // 未完结的K线 (w: false)
        let msg_open = r#"{
            "time": 1764559363,
            "time_ms": 1764559363408,
            "channel": "spot.candlesticks",
            "event": "update",
            "result": {
                "t": "1764559320",
                "v": "1146373.9657193",
                "c": "86504.5",
                "h": "86577.9",
                "l": "86504.5",
                "o": "86525.1",
                "n": "1m_BTC_USDT",
                "a": "13.244502",
                "w": false
            }
        }"#;

        let bytes = Bytes::from(msg_open);
        let count = parser.parse(bytes, &tx);
        assert_eq!(count, 0); // 应该被过滤

        // 已完结的K线 (w: true)
        let msg_closed = r#"{
            "time": 1764559363,
            "time_ms": 1764559363408,
            "channel": "spot.candlesticks",
            "event": "update",
            "result": {
                "t": "1764559320",
                "v": "1146373.9657193",
                "c": "86504.5",
                "h": "86577.9",
                "l": "86504.5",
                "o": "86525.1",
                "n": "1m_BTC_USDT",
                "a": "13.244502",
                "w": true
            }
        }"#;

        let bytes = Bytes::from(msg_closed);
        let count = parser.parse(bytes, &tx);
        assert_eq!(count, 1); // 应该被处理
    }
}
