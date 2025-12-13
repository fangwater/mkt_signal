use crate::common::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IndexPriceMsg, KlineMsg, MarkPriceMsg, SignalMsg, SignalSource,
};
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

/// Gate.io Book Ticker Parser - 解析 futures.book_ticker 提取最优买卖价
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

                if event != "update" || !channel.ends_with(".book_ticker") {
                    return 0;
                }

                let result = json_value.get("result");

                // futures.book_ticker：字段 s/b/B/a/A，时间戳在 result.t
                if let Some(res) = result.and_then(|v| v.as_object()) {
                    let symbol = match res.get("s").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => return 0,
                    };
                    let ts = res.get("t").and_then(|v| v.as_i64()).unwrap_or(0);
                    let bid_price = res
                        .get("b")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok());
                    let ask_price = res
                        .get("a")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok());
                    let bid_amount = res
                        .get("B")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let ask_amount = res
                        .get("A")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    if let (Some(bp), Some(ap)) = (bid_price, ask_price) {
                        if bp > 0.0 && ap > 0.0 {
                            let spread_msg = AskBidSpreadMsg::create(
                                symbol.to_string(),
                                ts,
                                bp,
                                bid_amount,
                                ap,
                                ask_amount,
                            );
                            if tx.send(spread_msg.to_bytes()).is_ok() {
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

/// Gate.io 衍生品指标解析（基于 futures.tickers）
#[derive(Clone)]
pub struct GateDerivativesMetricsParser;

impl GateDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for GateDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
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

                let timestamp = json_value
                    .get("time_ms")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                let mut parsed = 0;
                let results = json_value.get("result");

                // result 可能是数组或单个对象
                let items: Vec<&serde_json::Value> = match results {
                    Some(serde_json::Value::Array(arr)) => arr.iter().collect(),
                    Some(obj @ serde_json::Value::Object(_)) => vec![obj],
                    _ => Vec::new(),
                };

                for item in items {
                    let Some(symbol) = item.get("contract").and_then(|v| v.as_str()) else {
                        continue;
                    };

                    if let Some(mark_px) = item
                        .get("mark_price")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        let msg = MarkPriceMsg::create(symbol.to_string(), mark_px, timestamp);
                        if tx.send(msg.to_bytes()).is_ok() {
                            parsed += 1;
                        }
                    }

                    if let Some(index_px) = item
                        .get("index_price")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        let msg = IndexPriceMsg::create(symbol.to_string(), index_px, timestamp);
                        if tx.send(msg.to_bytes()).is_ok() {
                            parsed += 1;
                        }
                    }

                    if let Some(fr) = item
                        .get("funding_rate")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        // Gate futures.tickers 未提供 next funding time，填 0 作为占位
                        let msg = FundingRateMsg::create(symbol.to_string(), fr, 0, timestamp);
                        if tx.send(msg.to_bytes()).is_ok() {
                            parsed += 1;
                        }
                    }
                }

                return parsed;
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
