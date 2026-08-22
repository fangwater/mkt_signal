use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::debug;
use mkt_parsers::bitget as bitget_codec;
use mkt_parsers::msg::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, MarkPriceMsg, TradeMsg,
};
use tokio::sync::mpsc;

/// Bitget 价差解析器：支持 ticker 和 books1
#[derive(Clone)]
pub struct BitgetAskBidSpreadParser;

impl Default for BitgetAskBidSpreadParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetAskBidSpreadParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetAskBidSpreadParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let mut count = 0;
        for bbo in bitget_codec::parse_bbo_v2_json(&json_value) {
            let spread_msg = AskBidSpreadMsg::create(
                bbo.symbol,
                bitget_codec::us_to_ms(bbo.timestamp_us),
                bbo.bid_price,
                bbo.bid_amount,
                bbo.ask_price,
                bbo.ask_amount,
            );
            if tx.send(spread_msg.to_bytes()).is_ok() {
                count += 1;
            }
        }
        count
    }
}

/// Bitget Signal Parser - 用于深度行情信号
#[derive(Clone)]
pub struct BitgetSignalParser;

impl Default for BitgetSignalParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetSignalParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 使用顶层 ts 或 data[0].ts 作为时间戳
                let ts = json_value
                    .get("ts")
                    .and_then(|v| v.as_i64())
                    .or_else(|| {
                        json_value
                            .get("data")
                            .and_then(|v| v.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|item| item.get("ts"))
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<i64>().ok())
                    })
                    .unwrap_or(0);

                if ts > 0 {
                    let signal_msg = mkt_parsers::msg::mkt_msg::SignalMsg::create(
                        mkt_parsers::msg::mkt_msg::SignalSource::Tcp,
                        ts,
                    );
                    if tx.send(signal_msg.to_bytes()).is_ok() {
                        return 1;
                    }
                }
                debug!("Bitget signal skipped: {}", json_str);
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

impl Default for BitgetKlineParser {
    fn default() -> Self {
        Self::new()
    }
}

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

impl Default for BitgetDerivativesMetricsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let mut count = 0;
        for derivative in bitget_codec::parse_derivatives_v2_json(&json_value) {
            let bytes = derivative_to_legacy_bytes(derivative);
            if tx.send(bytes).is_ok() {
                count += 1;
            }
        }
        count
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

/// Bitget 增量深度解析（books snapshot/update）
#[derive(Clone)]
pub struct BitgetIncParser {
    max_levels: Option<usize>,
}

impl Default for BitgetIncParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetIncParser {
    pub fn new() -> Self {
        Self { max_levels: None }
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }
}

impl Parser for BitgetIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let mut sent = 0;
        for book in bitget_codec::parse_incremental_v2_json(&json_value) {
            let chunks = split_levels(book.bids.len(), book.asks.len(), self.max_levels);
            let total_chunks = chunks.len();
            for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
                chunks.into_iter().enumerate()
            {
                let mut inc_msg = IncMsg::create(
                    book.symbol.clone(),
                    book.first_update_id,
                    book.final_update_id,
                    bitget_codec::us_to_ms(book.timestamp_us),
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
                    sent += 1;
                }
            }
        }
        sent
    }
}

/// Bitget Trade Parser — UTA v3 SBE `publicTrade` (templateId=1003)。
///
/// SBE frame layout (schemaId=1, schemaVer=3, littleEndian):
///   - header  8B: blockLength u16, templateId u16, schemaId u16, version u16
///   - root   16B: px_exp i8, sz_exp i8, sts u64 (gateway 推送时刻), +6B padding
///   - group  4B header (entryBlockLength u16, numInGroup u16) + N×40B entries
///   - entry 40B: ts u64, exec_id u64, price i64, size i64, side u8, +7B padding
///   - symbol varString8 (u8 length + UTF-8 bytes)
///
/// schema 参考: https://www.bitget.com/api-doc/uta/sbe/sbe-trade
#[derive(Clone)]
pub struct BitgetTradeParser;

impl Default for BitgetTradeParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let raw = msg.as_ref();
        // 文本帧 (subscribe ack / error event / "pong"): 不业务，丢弃
        if raw
            .first()
            .is_some_and(|b| *b < 0x80 && (*b == b'{' || *b == b'"' || *b == b'p'))
        {
            return 0;
        }
        let trades = match bitget_codec::parse_sbe_public_trades(raw) {
            Ok(trades) => trades,
            Err(err) => {
                debug!("BitgetTradeParser: {}", err);
                return 0;
            }
        };
        let mut count = 0;
        for trade in trades {
            let trade_msg = TradeMsg::create(
                trade.symbol,
                trade.trade_id,
                trade.timestamp_us,
                trade.side,
                trade.price,
                trade.amount,
            );
            if tx.send(trade_msg.to_bytes()).is_ok() {
                count += 1;
            }
        }
        count
    }
}

fn derivative_to_legacy_bytes(derivative: bitget_codec::Derivative) -> Bytes {
    match derivative {
        bitget_codec::Derivative::MarkPrice {
            symbol,
            price,
            timestamp_us,
        } => MarkPriceMsg::create(symbol, price, bitget_codec::us_to_ms(timestamp_us)).to_bytes(),
        bitget_codec::Derivative::IndexPrice {
            symbol,
            price,
            timestamp_us,
        } => IndexPriceMsg::create(symbol, price, bitget_codec::us_to_ms(timestamp_us)).to_bytes(),
        bitget_codec::Derivative::FundingRate {
            symbol,
            funding_rate,
            next_funding_time_us,
            timestamp_us,
        } => FundingRateMsg::create(
            symbol,
            funding_rate,
            bitget_codec::us_to_ms(next_funding_time_us),
            bitget_codec::us_to_ms(timestamp_us),
        )
        .to_bytes(),
    }
}

// NOTE: parser tests removed per repo usage (WS payloads are brittle and often change).
