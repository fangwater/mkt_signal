use crate::common::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IndexPriceMsg, KlineMsg, MarkPriceMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::debug;
use tokio::sync::mpsc;

/// Bitget Ticker Parser - 解析 ticker 数据提取最优买卖价
///
/// Bitget ticker 消息格式:
/// ```json
/// {
///     "action": "snapshot",
///     "arg": {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
///     "data": [{
///         "instId": "BTCUSDT",
///         "bidPr": "91380.3",
///         "askPr": "91380.4",
///         "bidSz": "6.1168",
///         "askSz": "23.7021",
///         "ts": "1764518504340"
///     }],
///     "ts": 1764518504341
/// }
/// ```
#[derive(Clone)]
pub struct BitgetAskBidSpreadParser;

impl BitgetAskBidSpreadParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetAskBidSpreadParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析 Bitget ticker 消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 ticker 频道的消息
                if let Some(arg) = json_value.get("arg").and_then(|v| v.as_object()) {
                    if arg.get("channel").and_then(|v| v.as_str()) != Some("ticker") {
                        return 0;
                    }

                    // 获取 symbol
                    let symbol = match arg.get("instId").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => return 0,
                    };

                    // 解析 data 数组
                    if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                        let mut count = 0;
                        for item in data_array {
                            if let Some(obj) = item.as_object() {
                                // 提取 bid/ask 价格和数量
                                let bid_price = obj
                                    .get("bidPr")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok());
                                let ask_price = obj
                                    .get("askPr")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok());
                                let bid_amount = obj
                                    .get("bidSz")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok());
                                let ask_amount = obj
                                    .get("askSz")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok());
                                let timestamp = obj
                                    .get("ts")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<i64>().ok())
                                    .unwrap_or(0);

                                if let (Some(bp), Some(ap), Some(ba), Some(aa)) =
                                    (bid_price, ask_price, bid_amount, ask_amount)
                                {
                                    // 过滤无效值
                                    if bp <= 0.0 || ap <= 0.0 || ba <= 0.0 || aa <= 0.0 {
                                        continue;
                                    }

                                    // 创建并发送消息
                                    let spread_msg = AskBidSpreadMsg::create(
                                        symbol.to_string(),
                                        timestamp,
                                        bp,
                                        ba,
                                        ap,
                                        aa,
                                    );

                                    if tx.send(spread_msg.to_bytes()).is_ok() {
                                        count += 1;
                                    }
                                }
                            }
                        }
                        return count;
                    }
                }
            }
        }
        0
    }
}

/// Bitget Signal Parser - 用于深度行情信号
#[derive(Clone)]
pub struct BitgetSignalParser {
    is_spot: bool,
}

impl BitgetSignalParser {
    pub fn new(is_spot: bool) -> Self {
        Self { is_spot }
    }
}

impl Parser for BitgetSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Bitget signal parser - 简单转发原始消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否有时间戳（用于信号）
                if json_value.get("ts").is_some() || json_value.get("data").is_some() {
                    debug!("Bitget signal: {}", json_str);
                    if tx.send(msg).is_ok() {
                        return 1;
                    }
                }
            }
        }
        0
    }
}

/// Bitget Kline Parser - 解析 K线数据
///
/// Bitget K线消息格式:
/// ```json
/// {
///     "action": "snapshot",
///     "arg": {
///         "instType": "USDT-FUTURES",
///         "channel": "candle1m",
///         "instId": "BTCUSDT"
///     },
///     "data": [
///         ["1695685500000", "27000", "27000.5", "27000", "27000.5", "0.057", "1539.0155", "1539.0155"]
///     ],
///     "ts": 1695715462250
/// }
/// ```
/// data 格式: [timestamp, open, high, low, close, baseVol, quoteVol, usdtVol]
#[derive(Clone)]
pub struct BitgetKlineParser;

impl BitgetKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析 Bitget K线消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 candle 频道的消息
                if let Some(arg) = json_value.get("arg").and_then(|v| v.as_object()) {
                    let channel = arg.get("channel").and_then(|v| v.as_str()).unwrap_or("");
                    if !channel.starts_with("candle") {
                        return 0;
                    }

                    // 获取 symbol
                    let symbol = match arg.get("instId").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => return 0,
                    };

                    // 解析 data 数组
                    if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                        let mut count = 0;
                        for kline_data in data_array {
                            if let Some(kline_array) = kline_data.as_array() {
                                // data 格式: [ts, o, h, l, c, vol, quoteVol, usdtVol]
                                if kline_array.len() >= 6 {
                                    if let (
                                        Some(ts_str),
                                        Some(o_str),
                                        Some(h_str),
                                        Some(l_str),
                                        Some(c_str),
                                        Some(vol_str),
                                    ) = (
                                        kline_array[0].as_str(),
                                        kline_array[1].as_str(),
                                        kline_array[2].as_str(),
                                        kline_array[3].as_str(),
                                        kline_array[4].as_str(),
                                        kline_array[5].as_str(),
                                    ) {
                                        // 解析所有值
                                        if let (
                                            Ok(timestamp),
                                            Ok(open),
                                            Ok(high),
                                            Ok(low),
                                            Ok(close),
                                            Ok(volume),
                                        ) = (
                                            ts_str.parse::<i64>(),
                                            o_str.parse::<f64>(),
                                            h_str.parse::<f64>(),
                                            l_str.parse::<f64>(),
                                            c_str.parse::<f64>(),
                                            vol_str.parse::<f64>(),
                                        ) {
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
                                                count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        return count;
                    }
                }
            }
        }
        0
    }
}

/// Bitget Derivatives Metrics Parser - 从 ticker 消息解析衍生品指标
///
/// Bitget USDT-FUTURES ticker 消息中包含 funding rate、mark price 和 index price：
/// ```json
/// {
///     "action": "snapshot",
///     "arg": {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
///     "data": [{
///         "instId": "BTCUSDT",
///         "lastPr": "27000.5",
///         "fundingRate": "0.000010",
///         "nextFundingTime": "1695722400000",
///         "markPrice": "27000.0",
///         "indexPrice": "25702.4",
///         "ts": "1695715383021"
///     }],
///     "ts": 1695715383039
/// }
/// ```
#[derive(Clone)]
pub struct BitgetDerivativesMetricsParser;

impl BitgetDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析 Bitget ticker 消息中的衍生品指标
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 ticker 频道的消息
                if let Some(arg) = json_value.get("arg").and_then(|v| v.as_object()) {
                    if arg.get("channel").and_then(|v| v.as_str()) != Some("ticker") {
                        return 0;
                    }

                    // 只处理 USDT-FUTURES 类型（衍生品）
                    let inst_type = arg.get("instType").and_then(|v| v.as_str()).unwrap_or("");
                    if inst_type != "USDT-FUTURES" {
                        return 0;
                    }

                    // 获取 symbol
                    let symbol = match arg.get("instId").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => return 0,
                    };

                    // 解析 data 数组
                    if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                        let mut count = 0;
                        for item in data_array {
                            if let Some(obj) = item.as_object() {
                                let timestamp = obj
                                    .get("ts")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<i64>().ok())
                                    .unwrap_or(0);

                                // 解析 funding rate
                                if let (Some(funding_rate_str), Some(next_funding_time_str)) = (
                                    obj.get("fundingRate").and_then(|v| v.as_str()),
                                    obj.get("nextFundingTime").and_then(|v| v.as_str()),
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
                                        if tx.send(funding_rate_msg.to_bytes()).is_ok() {
                                            count += 1;
                                        }
                                    }
                                }

                                // 解析 mark price
                                if let Some(mark_price_str) =
                                    obj.get("markPrice").and_then(|v| v.as_str())
                                {
                                    if let Ok(mark_price) = mark_price_str.parse::<f64>() {
                                        if mark_price > 0.0 {
                                            let mark_price_msg = MarkPriceMsg::create(
                                                symbol.to_string(),
                                                mark_price,
                                                timestamp,
                                            );
                                            if tx.send(mark_price_msg.to_bytes()).is_ok() {
                                                count += 1;
                                            }
                                        }
                                    }
                                }

                                // 解析 index price
                                if let Some(index_price_str) =
                                    obj.get("indexPrice").and_then(|v| v.as_str())
                                {
                                    if let Ok(index_price) = index_price_str.parse::<f64>() {
                                        if index_price > 0.0 {
                                            let index_price_msg = IndexPriceMsg::create(
                                                symbol.to_string(),
                                                index_price,
                                                timestamp,
                                            );
                                            if tx.send(index_price_msg.to_bytes()).is_ok() {
                                                count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        return count;
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
    fn test_bitget_ticker_parse() {
        let parser = BitgetAskBidSpreadParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let msg = r#"{
            "action": "snapshot",
            "arg": {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
            "data": [{
                "instId": "BTCUSDT",
                "lastPr": "91380.4",
                "bidPr": "91380.3",
                "askPr": "91380.4",
                "bidSz": "6.1168",
                "askSz": "23.7021",
                "ts": "1764518504340"
            }],
            "ts": 1764518504341
        }"#;

        let bytes = Bytes::from(msg);
        let count = parser.parse(bytes, &tx);
        assert_eq!(count, 1);

        // 验证消息被正确发送
        let received = rx.try_recv();
        assert!(received.is_ok());
    }
}
