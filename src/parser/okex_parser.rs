// use crate::exchange::Exchange;
use crate::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, LiquidationMsg,
    MarkPriceMsg, SignalMsg, SignalSource, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::info;
use mkt_parsers::okex as okex_codec;
use std::collections::HashSet;
use tokio::sync::mpsc;

fn normalize_okex_symbol(symbol: &str) -> String {
    okex_codec::normalize_okex_symbol(symbol)
}

#[derive(Clone)]
pub struct OkexSignalParser {
    source: SignalSource,
}

impl OkexSignalParser {
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

impl Parser for OkexSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse OKEx depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract timestamp from data[0].ts
                if let Some(timestamp) = json_value
                    .get("data")
                    .and_then(|v| v.get(0))
                    .and_then(|item| item.get("ts"))
                    .and_then(|ts| ts.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                {
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
pub struct OkexKlineParser;

impl Default for OkexKlineParser {
    fn default() -> Self {
        Self::new()
    }
}

impl OkexKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for OkexKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse OKEx kline message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract symbol from arg.instId
                if let Some(symbol) = json_value
                    .get("arg")
                    .and_then(|arg| arg.get("instId"))
                    .and_then(|inst_id| inst_id.as_str())
                {
                    let symbol = normalize_okex_symbol(symbol);
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
                                if let (
                                    Some(ts_str),
                                    Some(o_str),
                                    Some(h_str),
                                    Some(l_str),
                                    Some(c_str),
                                    Some(vol_str),
                                ) = (
                                    kline_data[0].as_str(),
                                    kline_data[1].as_str(),
                                    kline_data[2].as_str(),
                                    kline_data[3].as_str(),
                                    kline_data[4].as_str(),
                                    kline_data[5].as_str(),
                                ) {
                                    // Parse all values
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
                                        // Create kline message
                                        let kline_msg = KlineMsg::create(
                                            symbol.clone(),
                                            open,
                                            high,
                                            low,
                                            close,
                                            volume,
                                            timestamp,
                                        );

                                        // Send kline message
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
        0
    }
}

#[derive(Clone)]
pub struct OkexDerivativesMetricsParser {
    symbols: HashSet<String>,
}

impl OkexDerivativesMetricsParser {
    pub fn new(symbols_set: HashSet<String>) -> Self {
        Self {
            symbols: symbols_set,
        }
    }
}

impl Parser for OkexDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                return self.publish_derivatives(&json_value, tx);
            }
        }
        0
    }
}

impl OkexDerivativesMetricsParser {
    fn publish_derivatives(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut parsed_count = 0;
        for derivative in okex_codec::parse_derivatives_json(json_value, Some(&self.symbols)) {
            let bytes = match derivative {
                okex_codec::Derivative::MarkPrice {
                    symbol,
                    price,
                    timestamp_us,
                } => MarkPriceMsg::create(symbol, price, okex_codec::us_to_ms(timestamp_us))
                    .to_bytes(),
                okex_codec::Derivative::IndexPrice {
                    symbol,
                    price,
                    timestamp_us,
                } => IndexPriceMsg::create(symbol, price, okex_codec::us_to_ms(timestamp_us))
                    .to_bytes(),
                okex_codec::Derivative::FundingRate {
                    symbol,
                    funding_rate,
                    next_funding_time_us,
                    timestamp_us,
                } => {
                    let next_funding_time = okex_codec::us_to_ms(next_funding_time_us);
                    let timestamp = okex_codec::us_to_ms(timestamp_us);
                    info!(
                        "[OKEx][funding-rate] symbol={} rate={} next_time={} ts={}",
                        symbol, funding_rate, next_funding_time, timestamp
                    );
                    FundingRateMsg::create(symbol, funding_rate, next_funding_time, timestamp)
                        .to_bytes()
                }
                okex_codec::Derivative::Liquidation {
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
                    okex_codec::us_to_ms(timestamp_us),
                )
                .to_bytes(),
            };
            if tx.send(bytes).is_ok() {
                parsed_count += 1;
            }
        }
        parsed_count
    }
}

#[derive(Clone)]
pub struct OkexTradeParser;

impl Default for OkexTradeParser {
    fn default() -> Self {
        Self::new()
    }
}

impl OkexTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for OkexTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                let mut parsed_count = 0;
                for trade in okex_codec::parse_trades_json(&json_value) {
                    let trade_msg = TradeMsg::create(
                        trade.symbol,
                        trade.trade_id,
                        okex_codec::us_to_ms(trade.timestamp_us),
                        trade.side,
                        trade.price,
                        trade.amount,
                    );
                    if tx.send(trade_msg.to_bytes()).is_ok() {
                        parsed_count += 1;
                    }
                }
                return parsed_count;
            }
        }
        0
    }
}

#[derive(Clone)]
pub struct OkexAskBidSpreadParser;

impl Default for OkexAskBidSpreadParser {
    fn default() -> Self {
        Self::new()
    }
}

impl OkexAskBidSpreadParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for OkexAskBidSpreadParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                let mut parsed_count = 0;
                for bbo in okex_codec::parse_bbo_json(&json_value) {
                    let spread_msg = AskBidSpreadMsg::create(
                        bbo.symbol,
                        okex_codec::us_to_ms(bbo.timestamp_us),
                        bbo.bid_price,
                        bbo.bid_amount,
                        bbo.ask_price,
                        bbo.ask_amount,
                    );
                    if tx.send(spread_msg.to_bytes()).is_ok() {
                        parsed_count += 1;
                    }
                }
                return parsed_count;
            }
        }
        0
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
pub struct OkexIncParser {
    max_levels: Option<usize>,
}

impl Default for OkexIncParser {
    fn default() -> Self {
        Self::new()
    }
}

impl OkexIncParser {
    pub fn new() -> Self {
        Self { max_levels: None }
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }
}

impl Parser for OkexIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                return self.parse_orderbook_event(&json_value, tx);
            }
        }
        0
    }
}

impl OkexIncParser {
    fn parse_orderbook_event(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut sent_count = 0;
        for book in okex_codec::parse_incremental_json(json_value) {
            let chunks = split_levels(book.bids.len(), book.asks.len(), self.max_levels);
            let total_chunks = chunks.len();

            for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
                chunks.into_iter().enumerate()
            {
                let mut inc_msg = IncMsg::create(
                    book.symbol.clone(),
                    book.first_update_id,
                    book.final_update_id,
                    okex_codec::us_to_ms(book.timestamp_us),
                    book.is_snapshot,
                    bids_count as u32,
                    asks_count as u32,
                );
                inc_msg.set_chunk_index(chunk_idx as u8);
                inc_msg.set_is_last(chunk_idx == total_chunks - 1);

                fill_order_book_levels(
                    &book.bids,
                    &book.asks,
                    bids_start,
                    bids_count,
                    asks_start,
                    asks_count,
                    &mut inc_msg,
                );

                if tx.send(inc_msg.to_bytes()).is_ok() {
                    sent_count += 1;
                }
            }
        }
        sent_count
    }
}

fn fill_order_book_levels(
    bids: &[okex_codec::Level],
    asks: &[okex_codec::Level],
    bids_start: usize,
    bids_count: usize,
    asks_start: usize,
    asks_count: usize,
    inc_msg: &mut IncMsg,
) {
    for (dst_idx, level) in bids.iter().skip(bids_start).take(bids_count).enumerate() {
        inc_msg.set_bid_level(dst_idx, Level::from_values(level.price, level.amount));
    }
    for (dst_idx, level) in asks.iter().skip(asks_start).take(asks_count).enumerate() {
        inc_msg.set_ask_level(dst_idx, Level::from_values(level.price, level.amount));
    }
}
