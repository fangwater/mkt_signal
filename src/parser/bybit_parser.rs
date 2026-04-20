use crate::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, LiquidationMsg,
    MarkPriceMsg, SignalMsg, SignalSource, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct BybitSignalParser {
    source: SignalSource,
}

impl BybitSignalParser {
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

impl Parser for BybitSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Bybit depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract Bybit timestamp field "cts"
                if let Some(timestamp) = json_value.get("cts").and_then(|v| v.as_i64()) {
                    // Create signal message
                    let signal_msg = SignalMsg::create(self.source, timestamp);
                    let signal_bytes = signal_msg.to_bytes();

                    // Send signal
                    if tx.send(signal_bytes).is_ok() {
                        return 1;
                    }
                }
            }
        }
        0
    }
}

#[derive(Clone)]
pub struct BybitKlineParser;

impl BybitKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Bybit kline message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是K线消息
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    if topic.starts_with("kline.") {
                        // 检查data数组
                        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array())
                        {
                            if let Some(kline_data) = data_array.first() {
                                // 检查confirm字段 - 只处理已确认的K线数据
                                if let Some(confirm) =
                                    kline_data.get("confirm").and_then(|v| v.as_bool())
                                {
                                    if confirm == false {
                                        return 0; // 未确认的K线，不处理
                                    }
                                } else {
                                    return 0; // confirm字段无效或缺失
                                }

                                // 从topic字段提取symbol
                                if let Some(symbol) = topic.split('.').last() {
                                    // 从kline_data对象中提取OHLCV数据
                                    if let (
                                        Some(open_str),
                                        Some(high_str),
                                        Some(low_str),
                                        Some(close_str),
                                        Some(volume_str),
                                        Some(start_time),
                                    ) = (
                                        kline_data.get("open").and_then(|v| v.as_str()),
                                        kline_data.get("high").and_then(|v| v.as_str()),
                                        kline_data.get("low").and_then(|v| v.as_str()),
                                        kline_data.get("close").and_then(|v| v.as_str()),
                                        kline_data.get("volume").and_then(|v| v.as_str()),
                                        kline_data.get("start").and_then(|v| v.as_i64()),
                                    ) {
                                        // 解析价格和成交量数据
                                        if let (
                                            Ok(open),
                                            Ok(high),
                                            Ok(low),
                                            Ok(close),
                                            Ok(volume),
                                        ) = (
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
                                                start_time,
                                            );

                                            // 发送K线消息
                                            if tx.send(kline_msg.to_bytes()).is_ok() {
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
        }
        0
    }
}

#[derive(Clone)]
pub struct BybitDerivativesMetricsParser;

impl BybitDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Bybit derivatives metrics messages (liquidations + tickers)
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    // Route based on topic prefix
                    if topic.starts_with("allLiquidation.") {
                        return self.parse_liquidation_data(&json_value, tx);
                    } else if topic.starts_with("tickers.") {
                        return self.parse_ticker_data(&json_value, tx);
                    }
                }
            }
        }
        0
    }
}

impl BybitDerivativesMetricsParser {
    fn parse_liquidation_data(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // Parse liquidation data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;

            for liquidation_data in data_array {
                if let (
                    Some(symbol),
                    Some(side),
                    Some(volume_str),
                    Some(price_str),
                    Some(timestamp),
                ) = (
                    liquidation_data.get("s").and_then(|v| v.as_str()),
                    liquidation_data.get("S").and_then(|v| v.as_str()),
                    liquidation_data.get("v").and_then(|v| v.as_str()),
                    liquidation_data.get("p").and_then(|v| v.as_str()),
                    liquidation_data.get("T").and_then(|v| v.as_i64()),
                ) {
                    // Parse volume and price
                    if let (Ok(volume), Ok(price)) =
                        (volume_str.parse::<f64>(), price_str.parse::<f64>())
                    {
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
                        if tx.send(liquidation_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
            }

            return parsed_count;
        }
        0
    }

    fn parse_ticker_data(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
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
                        let mark_price_msg =
                            MarkPriceMsg::create(symbol.to_string(), mark_price, timestamp);

                        if tx.send(mark_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }

                // Parse index price
                if let Some(index_price_str) = data.get("indexPrice").and_then(|v| v.as_str()) {
                    if let Ok(index_price) = index_price_str.parse::<f64>() {
                        let index_price_msg =
                            IndexPriceMsg::create(symbol.to_string(), index_price, timestamp);

                        if tx.send(index_price_msg.to_bytes()).is_ok() {
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
                        // Build FundingRate message (prediction no longer embedded)
                        let funding_rate_msg = FundingRateMsg::create(
                            symbol.to_string(),
                            funding_rate,
                            next_funding_time,
                            timestamp,
                        );

                        if tx.send(funding_rate_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                            if symbol.to_ascii_uppercase().starts_with("BTC") {
                                log::debug!(
                                    "[Bybit][derivatives] funding_rate symbol={} rate={} next_time={} ts={}",
                                    symbol,
                                    funding_rate,
                                    next_funding_time,
                                    timestamp
                                );
                            }
                        }
                    }
                }

                return parsed_count;
            }
        }
        0
    }
}

// UUID处理辅助函数
fn hex_char_to_int(c: char) -> Result<u8, String> {
    match c {
        '0'..='9' => Ok(c as u8 - b'0'),
        'a'..='f' => Ok(10 + (c as u8 - b'a')),
        'A'..='F' => Ok(10 + (c as u8 - b'A')),
        _ => Err(format!("Invalid hex character: {}", c)),
    }
}

fn is_uuid_fast(s: &str) -> bool {
    // UUID 标准长度 36 字符（32 十六进制 + 4 短横线）
    if s.len() != 36 {
        return false;
    }

    // 检查短横线位置是否正确
    let chars: Vec<char> = s.chars().collect();
    chars[8] == '-' && chars[13] == '-' && chars[18] == '-' && chars[23] == '-'
}

fn is_numeric(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
}

// 解析 8 个十六进制字符 -> u64
fn parse_hex_u64(hex: &str) -> Result<u64, String> {
    if hex.len() < 8 {
        return Err("Hex string too short".to_string());
    }

    let mut result = 0u64;
    for (_i, c) in hex.chars().take(8).enumerate() {
        result = (result << 4) | (hex_char_to_int(c)? as u64);
    }
    Ok(result)
}

// 主函数：UUID -> int64_t（高低位混合）
fn uuid_to_int64_mixed(uuid: &str) -> Result<i64, String> {
    if uuid.len() < 36 {
        return Err("Invalid UUID format".to_string());
    }

    // 高位：前 8 字符（跳过第8位的 '-'）
    let high = parse_hex_u64(&uuid[0..8])?;

    // 低位：后 8 字符（从第24位开始，跳过版本标识）
    let low = parse_hex_u64(&uuid[24..32])?;

    // 混合高低位（异或减少冲突）
    Ok((high ^ low) as i64)
}

#[derive(Clone)]
pub struct BybitTradeParser;

impl BybitTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Bybit trade message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Check if this is a trade topic
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    if topic.starts_with("publicTrade.") {
                        return self.parse_trade_event(&json_value, tx);
                    }
                }
            }
        }
        0
    }
}

impl BybitTradeParser {
    fn parse_trade_event(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // Extract trade data from Bybit trade message
        // Bybit trade message format: data is an array with trade objects
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;

            for trade_data in data_array {
                parsed_count += self.parse_trade_item(trade_data, tx);
            }

            return parsed_count;
        }
        0
    }

    fn parse_trade_item(
        &self,
        trade_data: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        if let (
            Some(symbol),
            Some(side_str),
            Some(price_str),
            Some(volume_str),
            Some(timestamp),
            Some(id_str),
        ) = (
            trade_data.get("s").and_then(|v| v.as_str()), // 交易对
            trade_data.get("S").and_then(|v| v.as_str()), // 买卖方向
            trade_data.get("p").and_then(|v| v.as_str()), // 成交价格
            trade_data.get("v").and_then(|v| v.as_str()), // 成交数量
            trade_data.get("T").and_then(|v| v.as_i64()), // 成交时间
            trade_data.get("i").and_then(|v| v.as_str()), // 交易ID
        ) {
            let (Ok(price), Ok(amount)) = (price_str.parse::<f64>(), volume_str.parse::<f64>())
            else {
                return 0;
            };

            // Filter out zero values
            if price <= 0.0 || amount <= 0.0 {
                return 0;
            }

            let side = match side_str {
                "Sell" => 'S',
                "Buy" => 'B',
                _ => {
                    eprintln!("Unknown side: {}", side_str);
                    return 0;
                }
            };

            let trade_id = if is_uuid_fast(id_str) {
                match uuid_to_int64_mixed(id_str) {
                    Ok(id) => id,
                    Err(e) => {
                        eprintln!("Failed to parse UUID {}: {}", id_str, e);
                        return 0;
                    }
                }
            } else if is_numeric(id_str) {
                match id_str.parse::<i64>() {
                    Ok(id) => id,
                    Err(e) => {
                        eprintln!("Failed to parse numeric ID {}: {}", id_str, e);
                        return 0;
                    }
                }
            } else {
                eprintln!("Unknown ID format: {}", id_str);
                return 0;
            };

            let trade_msg =
                TradeMsg::create(symbol.to_string(), trade_id, timestamp, side, price, amount);

            if tx.send(trade_msg.to_bytes()).is_ok() {
                return 1;
            }
        }

        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bybit_trade_parser_emits_all_trade_items() {
        let parser = BybitTradeParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let msg = Bytes::from(
            r#"{
                "topic":"publicTrade.BTCUSDT",
                "type":"snapshot",
                "ts":1672304486868,
                "data":[
                    {
                        "T":1672304486865,
                        "s":"BTCUSDT",
                        "S":"Buy",
                        "v":"0.001",
                        "p":"16578.50",
                        "L":"PlusTick",
                        "i":"20f43950-d8dd-5b31-9112-a178eb6023af",
                        "BT":false,
                        "seq":1783284617
                    },
                    {
                        "T":1672304486866,
                        "s":"BTCUSDT",
                        "S":"Sell",
                        "v":"0.002",
                        "p":"16578.00",
                        "L":"MinusTick",
                        "i":"120",
                        "BT":false,
                        "seq":1783284618
                    }
                ]
            }"#,
        );

        let parsed = parser.parse(msg, &tx);

        assert_eq!(parsed, 2);
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn bybit_kline_parser_uses_start_time_as_timestamp() {
        let parser = BybitKlineParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let msg = Bytes::from(
            r#"{
                "topic":"kline.1.BTCUSDT",
                "ts":1672324988888,
                "data":[
                    {
                        "start":1672324800000,
                        "end":1672324859999,
                        "interval":"1",
                        "open":"16500.00",
                        "close":"16510.00",
                        "high":"16520.00",
                        "low":"16490.00",
                        "volume":"12.34",
                        "turnover":"203456.78",
                        "confirm":true,
                        "timestamp":1672324859123
                    }
                ]
            }"#,
        );

        assert_eq!(parser.parse(msg, &tx), 1);

        let kline = rx.try_recv().expect("kline message should be emitted");
        assert_eq!(read_kline_timestamp(&kline), 1672324800000);
        assert!(rx.try_recv().is_err());
    }

    fn read_kline_timestamp(bytes: &[u8]) -> i64 {
        let symbol_length = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
        let offset = 8 + symbol_length + 5 * 8;
        i64::from_le_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("timestamp bytes"),
        )
    }

    #[test]
    fn bybit_ask_bid_parser_accepts_legacy_orderbook_one_json() {
        let parser = BybitAskBidSpreadParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let msg = Bytes::from(
            r#"{
                "topic":"orderbook.1.BTCUSDT",
                "type":"snapshot",
                "ts":1757497309814,
                "data":{
                    "s":"BTCUSDT",
                    "b":[["112233.4","8.1"]],
                    "a":[["112233.5","6.2"]],
                    "t":1757497309814
                }
            }"#,
        );

        assert_eq!(parser.parse(msg, &tx), 1);

        let spread = rx.try_recv().expect("spread message should be emitted");
        assert_eq!(AskBidSpreadMsg::get_symbol(&spread), "BTCUSDT");
        assert_eq!(AskBidSpreadMsg::get_timestamp(&spread), 1757497309814);
        assert_eq!(AskBidSpreadMsg::get_bid_price(&spread), 112233.4);
        assert_eq!(AskBidSpreadMsg::get_bid_amount(&spread), 8.1);
        assert_eq!(AskBidSpreadMsg::get_ask_price(&spread), 112233.5);
        assert_eq!(AskBidSpreadMsg::get_ask_amount(&spread), 6.2);
    }

    #[test]
    fn bybit_ask_bid_parser_accepts_top_level_ts_without_data_t() {
        let parser = BybitAskBidSpreadParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let msg = Bytes::from(
            r#"{
                "topic":"orderbook.1.BTCUSDT",
                "type":"snapshot",
                "ts":1757497309814,
                "data":{
                    "s":"BTCUSDT",
                    "b":[["112233.4","8.1"]],
                    "a":[["112233.5","6.2"]]
                }
            }"#,
        );

        assert_eq!(parser.parse(msg, &tx), 1);

        let spread = rx.try_recv().expect("spread message should be emitted");
        assert_eq!(AskBidSpreadMsg::get_symbol(&spread), "BTCUSDT");
        assert_eq!(AskBidSpreadMsg::get_timestamp(&spread), 1757497309814);
        assert_eq!(AskBidSpreadMsg::get_bid_price(&spread), 112233.4);
        assert_eq!(AskBidSpreadMsg::get_bid_amount(&spread), 8.1);
        assert_eq!(AskBidSpreadMsg::get_ask_price(&spread), 112233.5);
        assert_eq!(AskBidSpreadMsg::get_ask_amount(&spread), 6.2);
    }

    #[test]
    fn bybit_ask_bid_parser_accepts_sbe_bbo_binary() {
        let parser = BybitAskBidSpreadParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let msg = Bytes::from(vec![
            98, 0, 32, 78, 1, 0, 0, 0, 198, 28, 26, 229, 152, 1, 0, 0, 111, 0, 0, 0, 0, 0, 0, 0,
            154, 28, 26, 229, 152, 1, 0, 0, 112, 0, 0, 0, 0, 0, 0, 0, 169, 63, 55, 67, 0, 0, 0, 0,
            16, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 168, 63, 55,
            67, 0, 0, 0, 0, 32, 78, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 254, 255, 7, 66, 84, 67, 85, 83, 68, 84,
        ]);

        assert_eq!(parser.parse(msg, &tx), 1);

        let spread = rx.try_recv().expect("spread message should be emitted");
        assert_eq!(AskBidSpreadMsg::get_symbol(&spread), "BTCUSDT");
        assert_eq!(AskBidSpreadMsg::get_timestamp(&spread), 1_756_190_350_534);
        assert!((AskBidSpreadMsg::get_bid_price(&spread) - 11_276_942.48).abs() < 1e-9);
        assert!((AskBidSpreadMsg::get_bid_amount(&spread) - 2_000.0).abs() < 1e-9);
        assert!((AskBidSpreadMsg::get_ask_price(&spread) - 11_276_942.49).abs() < 1e-9);
        assert!((AskBidSpreadMsg::get_ask_amount(&spread) - 1_000.0).abs() < 1e-9);
    }
}

#[derive(Clone)]
pub struct BybitAskBidSpreadParser;

impl BybitAskBidSpreadParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_legacy_json(&self, msg: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    if topic.starts_with("orderbook.1.") {
                        let parts: Vec<&str> = topic.split('.').collect();
                        if parts.len() >= 3 {
                            let symbol = parts[2];

                            if let Some(data) = json_value.get("data").and_then(|v| v.as_object()) {
                                let timestamp = data
                                    .get("t")
                                    .and_then(|v| v.as_i64())
                                    .or_else(|| json_value.get("ts").and_then(|v| v.as_i64()));
                                let symbol = data
                                    .get("s")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or(symbol);

                                if let (Some(bids_array), Some(asks_array), Some(timestamp)) = (
                                    data.get("b").and_then(|v| v.as_array()),
                                    data.get("a").and_then(|v| v.as_array()),
                                    timestamp,
                                ) {
                                    if let (Some(bid_item), Some(ask_item)) =
                                        (bids_array.first(), asks_array.first())
                                    {
                                        if let (Some(bid_array), Some(ask_array)) =
                                            (bid_item.as_array(), ask_item.as_array())
                                        {
                                            if bid_array.len() >= 2 && ask_array.len() >= 2 {
                                                if let (
                                                    Some(bid_price_str),
                                                    Some(bid_amount_str),
                                                    Some(ask_price_str),
                                                    Some(ask_amount_str),
                                                ) = (
                                                    bid_array[0].as_str(),
                                                    bid_array[1].as_str(),
                                                    ask_array[0].as_str(),
                                                    ask_array[1].as_str(),
                                                ) {
                                                    if let (
                                                        Ok(bid_price),
                                                        Ok(bid_amount),
                                                        Ok(ask_price),
                                                        Ok(ask_amount),
                                                    ) = (
                                                        bid_price_str.parse::<f64>(),
                                                        bid_amount_str.parse::<f64>(),
                                                        ask_price_str.parse::<f64>(),
                                                        ask_amount_str.parse::<f64>(),
                                                    ) {
                                                        if bid_price <= 0.0
                                                            || bid_amount <= 0.0
                                                            || ask_price <= 0.0
                                                            || ask_amount <= 0.0
                                                        {
                                                            return 0;
                                                        }

                                                        let spread_msg = AskBidSpreadMsg::create(
                                                            symbol.to_string(),
                                                            timestamp,
                                                            bid_price,
                                                            bid_amount,
                                                            ask_price,
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
                                }
                            }
                        }
                    }
                }
            }
        }
        0
    }

    fn parse_sbe(&self, msg: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let header = match read_sbe_header(msg) {
            Some(header) => header,
            None => return 0,
        };
        if header.template_id != 20000 {
            return 0;
        }

        let base = header.body_offset;
        if msg.len() < base + header.block_length {
            return 0;
        }

        let parsed = if header.block_length == 82 {
            Some((
                read_i64_le(msg, base),
                read_i64_le(msg, base + 32),
                read_i64_le(msg, base + 40),
                read_i64_le(msg, base + 56),
                read_i64_le(msg, base + 64),
                read_i8(msg, base + 80),
                read_i8(msg, base + 81),
                base + header.block_length,
            ))
        } else if header.block_length >= 98 {
            Some((
                read_i64_le(msg, base),
                read_i64_le(msg, base + 32),
                read_i64_le(msg, base + 40),
                read_i64_le(msg, base + 64),
                read_i64_le(msg, base + 72),
                read_i8(msg, base + 96),
                read_i8(msg, base + 97),
                base + header.block_length,
            ))
        } else {
            None
        };

        let Some((
            Some(raw_timestamp),
            Some(ask_price_raw),
            Some(ask_amount_raw),
            Some(bid_price_raw),
            Some(bid_amount_raw),
            Some(price_exponent),
            Some(size_exponent),
            symbol_offset,
        )) = parsed
        else {
            return 0;
        };

        let Some((symbol, _)) = read_var_string8(msg, symbol_offset) else {
            return 0;
        };

        let timestamp = normalize_bybit_sbe_timestamp(raw_timestamp);
        let ask_price = scale_mantissa(ask_price_raw, price_exponent);
        let ask_amount = scale_mantissa(ask_amount_raw, size_exponent);
        let bid_price = scale_mantissa(bid_price_raw, price_exponent);
        let bid_amount = scale_mantissa(bid_amount_raw, size_exponent);

        if bid_price <= 0.0 || bid_amount <= 0.0 || ask_price <= 0.0 || ask_amount <= 0.0 {
            return 0;
        }

        let spread_msg = AskBidSpreadMsg::create(
            symbol.to_uppercase(),
            timestamp,
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        );

        if tx.send(spread_msg.to_bytes()).is_ok() {
            return 1;
        }
        0
    }
}

impl Parser for BybitAskBidSpreadParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.is_empty() {
            return 0;
        }
        if msg[0] == b'{' || msg[0] == b'[' {
            return self.parse_legacy_json(&msg, tx);
        }
        self.parse_sbe(&msg, tx)
    }
}

#[derive(Clone, Copy)]
struct SbeHeader {
    block_length: usize,
    template_id: u16,
    body_offset: usize,
}

fn read_sbe_header(msg: &[u8]) -> Option<SbeHeader> {
    if msg.len() < 8 {
        return None;
    }
    let block_length = read_u16_le(msg, 0)? as usize;
    let template_id = read_u16_le(msg, 2)?;
    Some(SbeHeader {
        block_length,
        template_id,
        body_offset: 8,
    })
}

fn read_u16_le(msg: &[u8], offset: usize) -> Option<u16> {
    if msg.len() < offset + 2 {
        return None;
    }
    Some(u16::from_le_bytes([msg[offset], msg[offset + 1]]))
}

fn read_i64_le(msg: &[u8], offset: usize) -> Option<i64> {
    if msg.len() < offset + 8 {
        return None;
    }
    Some(i64::from_le_bytes([
        msg[offset],
        msg[offset + 1],
        msg[offset + 2],
        msg[offset + 3],
        msg[offset + 4],
        msg[offset + 5],
        msg[offset + 6],
        msg[offset + 7],
    ]))
}

fn read_i8(msg: &[u8], offset: usize) -> Option<i8> {
    msg.get(offset).map(|value| *value as i8)
}

fn read_var_string8(msg: &[u8], offset: usize) -> Option<(String, usize)> {
    let len = msg.get(offset).copied()? as usize;
    let start = offset + 1;
    if msg.len() < start + len {
        return None;
    }
    let value = std::str::from_utf8(&msg[start..start + len]).ok()?;
    Some((value.to_string(), start + len))
}

fn scale_mantissa(mantissa: i64, exponent: i8) -> f64 {
    let factor = 10_f64.powi(exponent as i32);
    (mantissa as f64) * factor
}

fn normalize_bybit_sbe_timestamp(timestamp: i64) -> i64 {
    if timestamp.abs() >= 1_000_000_000_000_000 {
        timestamp / 1000
    } else {
        timestamp
    }
}

// 解析订单簿层级数据（支持偏移量）
fn parse_bybit_order_book_levels_with_offset(
    bids_array: &[serde_json::Value],
    asks_array: &[serde_json::Value],
    bids_start: usize,
    bids_count: usize,
    asks_start: usize,
    asks_count: usize,
    inc_msg: &mut IncMsg,
) {
    // 解析bids（从偏移量开始）
    for i in 0..bids_count {
        let src_idx = bids_start + i;
        if src_idx >= bids_array.len() {
            break;
        }
        if let Some(bid_array) = bids_array[src_idx].as_array() {
            if bid_array.len() >= 2 {
                if let (Some(price_str), Some(amount_str)) =
                    (bid_array[0].as_str(), bid_array[1].as_str())
                {
                    let level = Level::new(price_str, amount_str);
                    inc_msg.set_bid_level(i, level);
                }
            }
        }
    }

    // 解析asks（从偏移量开始）
    for i in 0..asks_count {
        let src_idx = asks_start + i;
        if src_idx >= asks_array.len() {
            break;
        }
        if let Some(ask_array) = asks_array[src_idx].as_array() {
            if ask_array.len() >= 2 {
                if let (Some(price_str), Some(amount_str)) =
                    (ask_array[0].as_str(), ask_array[1].as_str())
                {
                    let level = Level::new(price_str, amount_str);
                    inc_msg.set_ask_level(i, level);
                }
            }
        }
    }
}

/// 计算如何拆分 levels 成多个 chunk
fn split_levels(
    total_bids: usize,
    total_asks: usize,
    max_levels: Option<usize>,
) -> Vec<(usize, usize, usize, usize)> {
    let total = total_bids + total_asks;

    match max_levels {
        Some(max) if total > max && max > 0 => {
            let mut chunks = Vec::new();
            let mut bids_sent = 0;
            let mut asks_sent = 0;

            while bids_sent < total_bids || asks_sent < total_asks {
                let bids_remaining = total_bids - bids_sent;
                let asks_remaining = total_asks - asks_sent;
                let remaining = bids_remaining + asks_remaining;

                let chunk_bids = if remaining <= max {
                    bids_remaining
                } else {
                    let ratio = bids_remaining as f64 / remaining as f64;
                    ((max as f64 * ratio).round() as usize)
                        .max(1)
                        .min(bids_remaining)
                };
                let chunk_asks = (max - chunk_bids).min(asks_remaining);

                chunks.push((bids_sent, chunk_bids, asks_sent, chunk_asks));
                bids_sent += chunk_bids;
                asks_sent += chunk_asks;
            }

            chunks
        }
        _ => vec![(0, total_bids, 0, total_asks)],
    }
}

#[derive(Clone)]
pub struct BybitIncParser {
    max_levels: Option<usize>,
}

impl BybitIncParser {
    pub fn new() -> Self {
        Self { max_levels: None }
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }
}

impl Parser for BybitIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析Bybit增量/快照消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是订单簿数据
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    if topic.starts_with("orderbook.") {
                        return self.parse_orderbook_event(&json_value, tx);
                    }
                }
            }
        }
        0
    }
}

impl BybitIncParser {
    fn parse_orderbook_event(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // 从Bybit订单簿数据中提取信息
        if let (Some(msg_type), Some(timestamp), Some(data)) = (
            json_value.get("type").and_then(|v| v.as_str()),
            json_value.get("cts").and_then(|v| v.as_i64()),
            json_value.get("data"),
        ) {
            if let (Some(symbol), Some(update_id), Some(bids_array), Some(asks_array)) = (
                data.get("s").and_then(|v| v.as_str()),
                data.get("u").and_then(|v| v.as_i64()),
                data.get("b").and_then(|v| v.as_array()),
                data.get("a").and_then(|v| v.as_array()),
            ) {
                // 判断是否为快照消息
                let is_snapshot = match msg_type {
                    "snapshot" => true,
                    "delta" => false,
                    _ => return 0,
                };

                // 计算拆分方案
                let chunks = split_levels(bids_array.len(), asks_array.len(), self.max_levels);
                let total_chunks = chunks.len();
                let mut sent_count = 0;

                for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
                    chunks.into_iter().enumerate()
                {
                    // 创建增量/快照消息
                    let mut inc_msg = IncMsg::create(
                        symbol.to_string(),
                        update_id,   // first_update_id
                        update_id,   // final_update_id (Bybit两者相同)
                        timestamp,   // 使用cts时间戳
                        is_snapshot, // 根据type字段确定
                        bids_count as u32,
                        asks_count as u32,
                    );

                    // 设置 chunk_index 和 is_last
                    inc_msg.set_chunk_index(chunk_idx as u8);
                    inc_msg.set_is_last(chunk_idx == total_chunks - 1);

                    // 解析订单簿层级（带偏移量）
                    parse_bybit_order_book_levels_with_offset(
                        bids_array,
                        asks_array,
                        bids_start,
                        bids_count,
                        asks_start,
                        asks_count,
                        &mut inc_msg,
                    );

                    // 发送消息
                    if tx.send(inc_msg.to_bytes()).is_ok() {
                        sent_count += 1;
                    }
                }
                return sent_count;
            }
        }
        0
    }
}
