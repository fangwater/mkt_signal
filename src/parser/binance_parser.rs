use crate::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, LiquidationMsg,
    MarkPriceMsg, SignalMsg, SignalSource, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use mkt_parsers::binance as binance_codec;
use std::collections::HashSet;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct BinanceSignalParser {
    source: SignalSource,
}

impl BinanceSignalParser {
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

impl Parser for BinanceSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Binance depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract Binance timestamp field "E"
                if let Some(timestamp) = json_value.get("E").and_then(|v| v.as_i64()) {
                    // Create signal message
                    let signal_msg = SignalMsg::create(self.source, timestamp);
                    let signal_bytes = signal_msg.to_bytes();

                    // Send signal
                    if tx.send(signal_bytes).is_err() {
                        return 0;
                    }

                    return 1;
                }
            }
        }
        0
    }
}

#[derive(Clone)]
pub struct BinanceKlineParser;

impl Default for BinanceKlineParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
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
                        if let (
                            Some(open_str),
                            Some(high_str),
                            Some(low_str),
                            Some(close_str),
                            Some(volume_str),
                            Some(timestamp),
                        ) = (
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
                                let kline_bytes = kline_msg.to_bytes();
                                if tx.send(kline_bytes).is_ok() {
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

#[derive(Clone)]
pub struct BinanceDerivativesMetricsParser {
    symbols: HashSet<String>,
}

impl BinanceDerivativesMetricsParser {
    pub fn new(symbols_set: HashSet<String>) -> Self {
        Self {
            // Binance WS symbols are uppercase (e.g. "BTCUSDT"), while this parser uses
            // lowercase keys for lookups.
            symbols: symbols_set.into_iter().map(|s| s.to_lowercase()).collect(),
        }
    }
}

impl Parser for BinanceDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let derivatives = binance_codec::parse_derivatives_json(&json_value);
        self.publish_derivatives(derivatives, tx)
    }
}

impl BinanceDerivativesMetricsParser {
    fn publish_derivatives(
        &self,
        derivatives: Vec<binance_codec::Derivative>,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut total_parsed = 0;
        for derivative in derivatives {
            match derivative {
                binance_codec::Derivative::MarkPrice {
                    symbol,
                    price,
                    timestamp_us,
                } => {
                    if self.symbols.contains(&symbol.to_ascii_lowercase())
                        && tx
                            .send(MarkPriceMsg::create(symbol, price, timestamp_us).to_bytes())
                            .is_ok()
                    {
                        total_parsed += 1;
                    }
                }
                binance_codec::Derivative::IndexPrice {
                    symbol,
                    price,
                    timestamp_us,
                } => {
                    if self.symbols.contains(&symbol.to_ascii_lowercase())
                        && tx
                            .send(IndexPriceMsg::create(symbol, price, timestamp_us).to_bytes())
                            .is_ok()
                    {
                        total_parsed += 1;
                    }
                }
                binance_codec::Derivative::FundingRate {
                    symbol,
                    funding_rate,
                    next_funding_time_us,
                    timestamp_us,
                } => {
                    let s_lower = symbol.to_ascii_lowercase();
                    if self.symbols.contains(&s_lower)
                        && tx
                            .send(
                                FundingRateMsg::create(
                                    symbol,
                                    funding_rate,
                                    next_funding_time_us,
                                    timestamp_us,
                                )
                                .to_bytes(),
                            )
                            .is_ok()
                    {
                        total_parsed += 1;
                    }
                }
                binance_codec::Derivative::Liquidation {
                    symbol,
                    side,
                    amount,
                    price,
                    timestamp_us,
                } => {
                    if self.symbols.contains(&symbol.to_ascii_lowercase())
                        && tx
                            .send(
                                LiquidationMsg::create(symbol, side, amount, price, timestamp_us)
                                    .to_bytes(),
                            )
                            .is_ok()
                    {
                        total_parsed += 1;
                    }
                }
            }
        }
        total_parsed
    }
}

#[derive(Clone)]
pub struct BinanceSnapshotParser {
    max_levels: Option<usize>,
}

impl Default for BinanceSnapshotParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceSnapshotParser {
    pub fn new() -> Self {
        Self { max_levels: None }
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }
}

impl Parser for BinanceSnapshotParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析币安快照消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                return self.parse_snapshot_event(&json_value, tx);
            }
        }
        0
    }
}

// 公共函数：解析订单簿层级数据（支持偏移量）
fn parse_order_book_levels_with_offset(
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
/// 返回 Vec<(bids_start, bids_count, asks_start, asks_count)>
/// 每个 chunk 的总档数不超过 max_levels
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

                // 按比例分配本次 chunk 的 bids 和 asks
                let chunk_bids = if remaining <= max {
                    bids_remaining
                } else {
                    // 按原始比例分配
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
        _ => {
            // 不需要拆分，返回单个 chunk
            vec![(0, total_bids, 0, total_asks)]
        }
    }
}

fn publish_book_chunks(
    book: &binance_codec::Book,
    max_levels: Option<usize>,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> usize {
    let chunks = split_levels(book.bids.len(), book.asks.len(), max_levels);
    let total_chunks = chunks.len();
    let mut sent_count = 0;

    for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
        chunks.into_iter().enumerate()
    {
        let mut inc_msg = IncMsg::create(
            book.symbol.clone(),
            book.first_update_id,
            book.final_update_id,
            book.timestamp_us,
            book.is_snapshot,
            bids_count as u32,
            asks_count as u32,
        );
        inc_msg.set_chunk_index(chunk_idx as u8);
        inc_msg.set_is_last(chunk_idx == total_chunks - 1);
        set_inc_levels_from_parsed(
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

    sent_count
}

fn set_inc_levels_from_parsed(
    bids: &[binance_codec::Level],
    asks: &[binance_codec::Level],
    bids_start: usize,
    bids_count: usize,
    asks_start: usize,
    asks_count: usize,
    inc_msg: &mut IncMsg,
) {
    for i in 0..bids_count {
        let src_idx = bids_start + i;
        if let Some(level) = bids.get(src_idx) {
            inc_msg.set_bid_level(i, Level::from_values(level.price, level.amount));
        }
    }
    for i in 0..asks_count {
        let src_idx = asks_start + i;
        if let Some(level) = asks.get(src_idx) {
            inc_msg.set_ask_level(i, Level::from_values(level.price, level.amount));
        }
    }
}

fn publish_trades(trades: Vec<binance_codec::Trade>, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
    let mut sent_count = 0;
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
            sent_count += 1;
        }
    }
    sent_count
}

#[derive(Clone)]
pub struct BinanceSbeDepthSnapshotParser {
    max_levels: Option<usize>,
}

impl BinanceSbeDepthSnapshotParser {
    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }
}

impl Parser for BinanceSbeDepthSnapshotParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.is_empty() {
            return 0;
        }
        if msg[0] == b'{' || msg[0] == b'[' {
            return 0;
        }
        let Some(book) = binance_codec::parse_sbe_incremental(&msg) else {
            return 0;
        };
        if !book.is_snapshot {
            return 0;
        }
        publish_book_chunks(&book, self.max_levels, tx)
    }
}

#[derive(Clone)]
pub struct BinanceSbeDepthDiffParser {
    max_levels: Option<usize>,
}

impl BinanceSbeDepthDiffParser {
    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }
}

impl Parser for BinanceSbeDepthDiffParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.is_empty() {
            return 0;
        }
        if msg[0] == b'{' || msg[0] == b'[' {
            return 0;
        }
        let Some(book) = binance_codec::parse_sbe_incremental(&msg) else {
            return 0;
        };
        if book.is_snapshot {
            return 0;
        }
        publish_book_chunks(&book, self.max_levels, tx)
    }
}

#[derive(Clone)]
pub struct BinanceSbeBestBidAskParser;

impl Default for BinanceSbeBestBidAskParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceSbeBestBidAskParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceSbeBestBidAskParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.is_empty() {
            return 0;
        }
        if msg[0] == b'{' || msg[0] == b'[' {
            return 0;
        }
        let Some(bbo) = binance_codec::parse_sbe_bbo(&msg) else {
            return 0;
        };
        let spread_msg = AskBidSpreadMsg::create(
            bbo.symbol,
            bbo.timestamp_us,
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
}

#[derive(Clone)]
pub struct BinanceSbeTradeParser;

impl Default for BinanceSbeTradeParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceSbeTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceSbeTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.is_empty() {
            return 0;
        }
        if msg[0] == b'{' || msg[0] == b'[' {
            return 0;
        }
        let trades = binance_codec::parse_sbe_trades(&msg);
        publish_trades(trades, tx)
    }
}

impl BinanceSnapshotParser {
    fn parse_snapshot_event(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // 从快照数据中提取信息
        if let (Some(symbol), Some(last_update_id), Some(bids_array), Some(asks_array)) = (
            json_value.get("s").and_then(|v| v.as_str()),
            json_value.get("lastUpdateId").and_then(|v| v.as_i64()),
            json_value.get("bids").and_then(|v| v.as_array()),
            json_value.get("asks").and_then(|v| v.as_array()),
        ) {
            // 计算拆分方案
            let chunks = split_levels(bids_array.len(), asks_array.len(), self.max_levels);
            let total_chunks = chunks.len();
            let mut sent_count = 0;

            for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
                chunks.into_iter().enumerate()
            {
                // 创建快照消息
                let mut inc_msg = IncMsg::create(
                    symbol.to_string(),
                    last_update_id + 1, // first_update_id
                    last_update_id + 1, // final_update_id（对于快照相同）
                    0,                  // timestamp（快照没有实际时间戳）
                    true,               // is_snapshot = true
                    bids_count as u32,
                    asks_count as u32,
                );

                // 设置 chunk_index 和 is_last
                inc_msg.set_chunk_index(chunk_idx as u8);
                inc_msg.set_is_last(chunk_idx == total_chunks - 1);

                // 解析订单簿层级（带偏移量）
                parse_order_book_levels_with_offset(
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
        0
    }
}

#[derive(Clone)]
pub struct BinanceIncParser {
    max_levels: Option<usize>,
    is_snapshot: bool,
    mode: BinanceDepthMode,
}

impl Default for BinanceIncParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceIncParser {
    pub fn new() -> Self {
        Self::futures_incremental(None)
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self::futures_incremental(max_levels)
    }

    pub fn with_snapshot(max_levels: Option<usize>) -> Self {
        Self::futures_snapshot(max_levels)
    }

    pub fn futures_incremental(max_levels: Option<usize>) -> Self {
        Self {
            max_levels,
            is_snapshot: false,
            mode: BinanceDepthMode::FuturesDepthUpdate,
        }
    }

    pub fn futures_snapshot(max_levels: Option<usize>) -> Self {
        Self {
            max_levels,
            is_snapshot: true,
            mode: BinanceDepthMode::FuturesDepthUpdate,
        }
    }

    pub fn spot_incremental(max_levels: Option<usize>) -> Self {
        Self {
            max_levels,
            is_snapshot: false,
            mode: BinanceDepthMode::SpotDepthUpdate,
        }
    }

    pub fn spot_snapshot(max_levels: Option<usize>) -> Self {
        Self {
            max_levels,
            is_snapshot: true,
            mode: BinanceDepthMode::SpotSnapshot,
        }
    }
}

impl Parser for BinanceIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let Some(mut book) = binance_codec::parse_incremental_json(&json_value) else {
            return 0;
        };

        match self.mode {
            BinanceDepthMode::FuturesDepthUpdate | BinanceDepthMode::SpotDepthUpdate => {
                if book.is_snapshot {
                    return 0;
                }
                book.is_snapshot = self.is_snapshot;
            }
            BinanceDepthMode::SpotSnapshot => {
                if !book.is_snapshot {
                    return 0;
                }
                book.is_snapshot = self.is_snapshot;
            }
        }

        publish_book_chunks(&book, self.max_levels, tx)
    }
}

#[derive(Clone, Copy)]
enum BinanceDepthMode {
    FuturesDepthUpdate,
    SpotDepthUpdate,
    SpotSnapshot,
}

#[derive(Clone)]
pub struct BinanceTradeParser;

impl Default for BinanceTradeParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceTradeParser {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Clone)]
pub struct BinanceAskBidSpreadParser;

impl Default for BinanceAskBidSpreadParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceAskBidSpreadParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let Some(trade) = binance_codec::parse_trade_json(&json_value) else {
            return 0;
        };
        publish_trades(vec![trade], tx)
    }
}

impl Parser for BinanceAskBidSpreadParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let Some(bbo) = binance_codec::parse_bbo_json(&json_value) else {
            return 0;
        };
        let spread_msg = AskBidSpreadMsg::create(
            bbo.symbol,
            bbo.timestamp_us,
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
}
