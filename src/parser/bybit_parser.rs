use crate::parser::default_parser::Parser;
use bytes::Bytes;
use mkt_parsers::bybit as bybit_codec;
use mkt_parsers::msg::mkt_msg::{
    ask_bid_spread_msg_bytes_borrowed, funding_rate_msg_bytes_borrowed, inc_msg_bytes_borrowed,
    index_price_msg_bytes_borrowed, kline_msg_bytes_borrowed, liquidation_msg_bytes_borrowed,
    mark_price_msg_bytes_borrowed, signal_msg_bytes, trade_msg_bytes_borrowed, AskBidSpreadMsg,
    FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, LiquidationMsg, MarkPriceMsg,
    SignalSource, TradeMsg,
};
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
        if let Some(timestamp) = bybit_codec::parse_event_time_ms_raw(&msg) {
            return if tx.send(signal_msg_bytes(self.source, timestamp)).is_ok() {
                1
            } else {
                0
            };
        }

        // Parse Bybit depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract Bybit timestamp field "cts"
                if let Some(timestamp) = json_value.get("cts").and_then(|v| v.as_i64()) {
                    // Create signal message
                    if tx.send(signal_msg_bytes(self.source, timestamp)).is_ok() {
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

impl Default for BybitKlineParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BybitKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(kline) = bybit_codec::parse_kline_raw_borrowed(&msg) {
            let kline_bytes = kline_msg_bytes_borrowed(
                kline.symbol,
                kline.open_price,
                kline.high_price,
                kline.low_price,
                kline.close_price,
                kline.volume,
                kline.timestamp,
            );
            return if tx.send(kline_bytes).is_ok() { 1 } else { 0 };
        }

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
                                    if !confirm {
                                        return 0; // 未确认的K线，不处理
                                    }
                                } else {
                                    return 0; // confirm字段无效或缺失
                                }

                                // 从topic字段提取symbol
                                if let Some(symbol) = topic.split('.').next_back() {
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

impl Default for BybitDerivativesMetricsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BybitDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(count) = self.publish_derivatives_raw(&msg, tx) {
            return count;
        }

        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let mut parsed_count = 0;
        for derivative in bybit_codec::parse_derivatives_json(&json_value) {
            let bytes = derivative_to_legacy_bytes(derivative);
            if tx.send(bytes).is_ok() {
                parsed_count += 1;
            }
        }
        parsed_count
    }
}

impl BybitDerivativesMetricsParser {
    fn publish_derivatives_raw(
        &self,
        msg: &[u8],
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> Option<usize> {
        let mut parsed_count = 0usize;
        bybit_codec::parse_derivatives_raw_borrowed(msg, |derivative| {
            match derivative {
                bybit_codec::RawDerivative::Ticker {
                    symbol,
                    mark_price,
                    index_price,
                    funding_rate,
                    next_funding_time_us,
                    timestamp_us,
                } => {
                    if let Some(price) = mark_price.filter(|price| *price > 0.0) {
                        if tx
                            .send(mark_price_msg_bytes_borrowed(
                                symbol,
                                price,
                                bybit_codec::us_to_ms(timestamp_us),
                            ))
                            .is_ok()
                        {
                            parsed_count += 1;
                        }
                    }
                    if let Some(price) = index_price.filter(|price| *price > 0.0) {
                        if tx
                            .send(index_price_msg_bytes_borrowed(
                                symbol,
                                price,
                                bybit_codec::us_to_ms(timestamp_us),
                            ))
                            .is_ok()
                        {
                            parsed_count += 1;
                        }
                    }
                    if let Some(funding_rate) = funding_rate {
                        if tx
                            .send(funding_rate_msg_bytes_borrowed(
                                symbol,
                                funding_rate,
                                bybit_codec::us_to_ms(next_funding_time_us.unwrap_or(0)),
                                bybit_codec::us_to_ms(timestamp_us),
                            ))
                            .is_ok()
                        {
                            parsed_count += 1;
                        }
                    }
                }
                bybit_codec::RawDerivative::Liquidation {
                    symbol,
                    side,
                    amount,
                    price,
                    timestamp_us,
                } => {
                    if tx
                        .send(liquidation_msg_bytes_borrowed(
                            symbol,
                            side,
                            amount,
                            price,
                            bybit_codec::us_to_ms(timestamp_us),
                        ))
                        .is_ok()
                    {
                        parsed_count += 1;
                    }
                }
            }
            Some(())
        })?;
        Some(parsed_count)
    }
}

#[derive(Clone)]
pub struct BybitTradeParser;

impl Default for BybitTradeParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BybitTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(count) = bybit_codec::parse_trades_raw_borrowed(&msg, |trade| {
            tx.send(trade_msg_bytes_borrowed(
                trade.symbol,
                trade.trade_id,
                bybit_codec::us_to_ms(trade.timestamp_us),
                trade.side,
                trade.price,
                trade.amount,
            ))
            .ok()
            .map(|_| ())
        }) {
            return count;
        }

        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let mut parsed_count = 0;
        for trade in bybit_codec::parse_trades_json(&json_value) {
            let trade_msg = TradeMsg::create(
                trade.symbol,
                trade.trade_id,
                bybit_codec::us_to_ms(trade.timestamp_us),
                trade.side,
                trade.price,
                trade.amount,
            );
            if tx.send(trade_msg.to_bytes()).is_ok() {
                parsed_count += 1;
            }
        }
        parsed_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::{get_msg_type, MktMsgType};

    fn msg_symbol(data: &[u8]) -> &str {
        let len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        std::str::from_utf8(&data[8..8 + len]).unwrap()
    }

    fn trade_timestamp(data: &[u8]) -> i64 {
        let len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let offset = 8 + len + 8;
        i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
    }

    fn inc_first_update_id(data: &[u8]) -> i64 {
        let len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let offset = 8 + len;
        i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
    }

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
    fn bybit_trade_parser_uses_raw_trade_shape() {
        let parser = BybitTradeParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let msg = Bytes::from_static(
            br#"{"topic":"publicTrade.BTCUSDT","data":[{"T":1700000000123,"s":"BTCUSDT","S":"Buy","v":"0.2","p":"100.6","i":"120","seq":77}]}"#,
        );

        assert_eq!(parser.parse(msg, &tx), 1);

        let trade = rx.try_recv().expect("trade message should be emitted");
        assert_eq!(get_msg_type(&trade), MktMsgType::TradeInfo);
        assert_eq!(msg_symbol(&trade), "BTCUSDT");
        assert_eq!(trade_timestamp(&trade), 1_700_000_000_123);
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

    #[test]
    fn bybit_derivatives_parser_uses_raw_ticker_shape() {
        let parser = BybitDerivativesMetricsParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let msg = Bytes::from_static(
            br#"{"topic":"tickers.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"symbol":"BTCUSDT","markPrice":"100.1","indexPrice":"99.9","fundingRate":"0.0001","nextFundingTime":"1700003600000"}}"#,
        );

        assert_eq!(parser.parse(msg, &tx), 3);

        let mark = rx.try_recv().expect("mark price");
        let index = rx.try_recv().expect("index price");
        let funding = rx.try_recv().expect("funding rate");
        assert_eq!(get_msg_type(&mark), MktMsgType::MarkPrice);
        assert_eq!(get_msg_type(&index), MktMsgType::IndexPrice);
        assert_eq!(get_msg_type(&funding), MktMsgType::FundingRate);
        assert_eq!(msg_symbol(&mark), "BTCUSDT");
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
    fn bybit_incremental_parser_uses_raw_orderbook_shape() {
        let parser = BybitIncParser::with_max_levels(Some(1));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let msg = Bytes::from_static(
            br#"{"topic":"orderbook.1000.BTCUSDT","type":"delta","ts":1700000000999,"cts":1700000000123,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","3"]],"u":12345}}"#,
        );

        assert_eq!(parser.parse(msg, &tx), 2);

        let first = rx.try_recv().expect("first chunk");
        let second = rx.try_recv().expect("second chunk");
        assert_eq!(get_msg_type(&first), MktMsgType::OrderBookInc);
        assert_eq!(msg_symbol(&first), "BTCUSDT");
        assert_eq!(inc_first_update_id(&first), 12345);
        assert_eq!(get_msg_type(&second), MktMsgType::OrderBookInc);
        assert!(rx.try_recv().is_err());
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

impl Default for BybitAskBidSpreadParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BybitAskBidSpreadParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_legacy_json(&self, msg: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(bbo) = bybit_codec::parse_bbo_raw_borrowed(msg) {
            let spread_bytes = ask_bid_spread_msg_bytes_borrowed(
                bbo.symbol,
                bybit_codec::us_to_ms(bbo.timestamp_us),
                bbo.bid_price,
                bbo.bid_amount,
                bbo.ask_price,
                bbo.ask_amount,
            );
            return if tx.send(spread_bytes).is_ok() { 1 } else { 0 };
        }

        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(msg) else {
            return 0;
        };
        let Some(bbo) = bybit_codec::parse_bbo_json(&json_value) else {
            return 0;
        };
        let spread_msg = AskBidSpreadMsg::create(
            bbo.symbol,
            bybit_codec::us_to_ms(bbo.timestamp_us),
            bbo.bid_price,
            bbo.bid_amount,
            bbo.ask_price,
            bbo.ask_amount,
        );
        if tx.send(spread_msg.to_bytes()).is_ok() {
            1
        } else {
            0
        }
    }

    fn parse_sbe(&self, msg: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Some(bbo) = bybit_codec::parse_sbe_bbo(msg) else {
            return 0;
        };

        let spread_msg = AskBidSpreadMsg::create(
            bbo.symbol,
            bybit_codec::us_to_ms(bbo.timestamp_us),
            bbo.bid_price,
            bbo.bid_amount,
            bbo.ask_price,
            bbo.ask_amount,
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

impl Default for BybitIncParser {
    fn default() -> Self {
        Self::new()
    }
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
        if let Some(book) = bybit_codec::parse_incremental_raw_view(&msg) {
            return publish_raw_book_view_chunks(&book, self.max_levels, tx);
        }

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

fn publish_raw_book_view_chunks(
    book: &bybit_codec::RawBookView<'_>,
    max_levels: Option<usize>,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> usize {
    let chunks = split_levels(book.bids_count, book.asks_count, max_levels);
    let total_chunks = chunks.len();
    let mut sent_count = 0;
    let Some(mut bids_iter) = bybit_codec::raw_levels_iter(book.bids_raw) else {
        return 0;
    };
    let Some(mut asks_iter) = bybit_codec::raw_levels_iter(book.asks_raw) else {
        return 0;
    };

    for (chunk_idx, (_bids_start, bids_count, _asks_start, asks_count)) in
        chunks.into_iter().enumerate()
    {
        let inc_bytes = inc_msg_bytes_borrowed(
            book.symbol,
            book.first_update_id,
            book.final_update_id,
            bybit_codec::us_to_ms(book.timestamp_us),
            book.is_snapshot,
            chunk_idx == total_chunks - 1,
            chunk_idx as u8,
            bids_count as u32,
            asks_count as u32,
            bids_iter
                .by_ref()
                .take(bids_count)
                .map(|level| Level::from_values(level.price, level.amount)),
            asks_iter
                .by_ref()
                .take(asks_count)
                .map(|level| Level::from_values(level.price, level.amount)),
        );
        if tx.send(inc_bytes).is_ok() {
            sent_count += 1;
        }
    }

    sent_count
}

impl BybitIncParser {
    fn parse_orderbook_event(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let Some(book) = bybit_codec::parse_incremental_json(json_value) else {
            return 0;
        };
        let chunks = split_levels(book.bids.len(), book.asks.len(), self.max_levels);
        let total_chunks = chunks.len();
        let mut sent_count = 0;

        for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
            chunks.into_iter().enumerate()
        {
            let mut inc_msg = IncMsg::create(
                book.symbol.clone(),
                book.first_update_id,
                book.final_update_id,
                bybit_codec::us_to_ms(book.timestamp_us),
                book.is_snapshot,
                bids_count as u32,
                asks_count as u32,
            );
            inc_msg.set_chunk_index(chunk_idx as u8);
            inc_msg.set_is_last(chunk_idx == total_chunks - 1);

            for i in 0..bids_count {
                let src_idx = bids_start + i;
                if let Some(level) = book.bids.get(src_idx) {
                    inc_msg.set_bid_level(i, Level::from_values(level.price, level.amount));
                }
            }
            for i in 0..asks_count {
                let src_idx = asks_start + i;
                if let Some(level) = book.asks.get(src_idx) {
                    inc_msg.set_ask_level(i, Level::from_values(level.price, level.amount));
                }
            }

            if tx.send(inc_msg.to_bytes()).is_ok() {
                sent_count += 1;
            }
        }
        sent_count
    }
}

fn derivative_to_legacy_bytes(derivative: bybit_codec::Derivative) -> Bytes {
    match derivative {
        bybit_codec::Derivative::MarkPrice {
            symbol,
            price,
            timestamp_us,
        } => MarkPriceMsg::create(symbol, price, bybit_codec::us_to_ms(timestamp_us)).to_bytes(),
        bybit_codec::Derivative::IndexPrice {
            symbol,
            price,
            timestamp_us,
        } => IndexPriceMsg::create(symbol, price, bybit_codec::us_to_ms(timestamp_us)).to_bytes(),
        bybit_codec::Derivative::FundingRate {
            symbol,
            funding_rate,
            next_funding_time_us,
            timestamp_us,
        } => FundingRateMsg::create(
            symbol,
            funding_rate,
            bybit_codec::us_to_ms(next_funding_time_us),
            bybit_codec::us_to_ms(timestamp_us),
        )
        .to_bytes(),
        bybit_codec::Derivative::Liquidation {
            symbol,
            side,
            amount,
            price,
            timestamp_us,
        } => LiquidationMsg::create(
            symbol,
            side,
            amount,
            price,
            bybit_codec::us_to_ms(timestamp_us),
        )
        .to_bytes(),
    }
}
