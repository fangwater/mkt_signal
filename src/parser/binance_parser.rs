use crate::parser::default_parser::Parser;
use bytes::Bytes;
use mkt_parsers::binance as binance_codec;
use mkt_parsers::msg::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, LiquidationMsg,
    MarkPriceMsg, SignalMsg, SignalSource, TradeMsg,
};
use std::collections::HashSet;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct BinanceSignalParser {
    source: SignalSource,
    raw_only: bool,
}

impl BinanceSignalParser {
    pub fn new(is_ipc: bool) -> Self {
        Self::with_raw_only(is_ipc, false)
    }

    pub fn raw_only(is_ipc: bool) -> Self {
        Self::with_raw_only(is_ipc, true)
    }

    fn with_raw_only(is_ipc: bool, raw_only: bool) -> Self {
        Self {
            source: if is_ipc {
                SignalSource::Ipc
            } else {
                SignalSource::Tcp
            },
            raw_only,
        }
    }
}

impl Parser for BinanceSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(timestamp) = binance_codec::parse_event_time_ms_raw(&msg) {
            let signal_msg = SignalMsg::create(self.source, timestamp);
            return if tx.send(signal_msg.to_bytes()).is_ok() {
                1
            } else {
                0
            };
        }
        if self.raw_only {
            return 0;
        }

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
pub struct BinanceKlineParser {
    raw_only: bool,
}

impl Default for BinanceKlineParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceKlineParser {
    pub fn new() -> Self {
        Self::with_raw_only(false)
    }

    pub fn raw_only() -> Self {
        Self::with_raw_only(true)
    }

    fn with_raw_only(raw_only: bool) -> Self {
        Self { raw_only }
    }
}

impl Parser for BinanceKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(kline) = binance_codec::parse_kline_raw_borrowed(&msg) {
            let kline_msg = KlineMsg::create(
                kline.symbol.to_string(),
                kline.open_price,
                kline.high_price,
                kline.low_price,
                kline.close_price,
                kline.volume,
                kline.timestamp,
            );
            return if tx.send(kline_msg.to_bytes()).is_ok() {
                1
            } else {
                0
            };
        }
        if self.raw_only {
            return 0;
        }

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
    raw_only: bool,
}

impl BinanceDerivativesMetricsParser {
    pub fn new(symbols_set: HashSet<String>) -> Self {
        Self::with_raw_only(symbols_set, false)
    }

    pub fn raw_only(symbols_set: HashSet<String>) -> Self {
        Self::with_raw_only(symbols_set, true)
    }

    fn with_raw_only(symbols_set: HashSet<String>, raw_only: bool) -> Self {
        Self {
            symbols: symbols_set
                .into_iter()
                .map(|s| s.to_ascii_uppercase())
                .collect(),
            raw_only,
        }
    }
}

impl Parser for BinanceDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(count) = self.publish_derivatives_raw(&msg, tx) {
            return count;
        }
        if self.raw_only {
            return 0;
        }

        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let derivatives = binance_codec::parse_derivatives_json(&json_value);
        self.publish_derivatives(derivatives, tx)
    }
}

impl BinanceDerivativesMetricsParser {
    fn publish_derivatives_raw(
        &self,
        msg: &[u8],
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> Option<usize> {
        let mut total_parsed = 0usize;
        binance_codec::parse_derivatives_raw_borrowed(msg, |derivative| {
            match derivative {
                binance_codec::RawDerivative::MarkPrice {
                    symbol,
                    mark_price,
                    index_price,
                    funding_rate,
                    next_funding_time_us,
                    timestamp_us,
                } => {
                    if !self.symbols.contains(symbol) {
                        return Some(());
                    }
                    if let Some(price) = mark_price.filter(|price| *price > 0.0) {
                        if tx
                            .send(
                                MarkPriceMsg::create(symbol.to_string(), price, timestamp_us)
                                    .to_bytes(),
                            )
                            .is_ok()
                        {
                            total_parsed += 1;
                        }
                    }
                    if let Some(price) = index_price.filter(|price| *price > 0.0) {
                        if tx
                            .send(
                                IndexPriceMsg::create(symbol.to_string(), price, timestamp_us)
                                    .to_bytes(),
                            )
                            .is_ok()
                        {
                            total_parsed += 1;
                        }
                    }
                    if let (Some(funding_rate), Some(next_funding_time_us)) =
                        (funding_rate, next_funding_time_us)
                    {
                        if tx
                            .send(
                                FundingRateMsg::create(
                                    symbol.to_string(),
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
                }
                binance_codec::RawDerivative::Liquidation {
                    symbol,
                    side,
                    amount,
                    price,
                    timestamp_us,
                } => {
                    if self.symbols.contains(symbol)
                        && tx
                            .send(
                                LiquidationMsg::create(
                                    symbol.to_string(),
                                    side,
                                    amount,
                                    price,
                                    timestamp_us,
                                )
                                .to_bytes(),
                            )
                            .is_ok()
                    {
                        total_parsed += 1;
                    }
                }
            }
            Some(())
        })?;
        Some(total_parsed)
    }

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
                    if self.symbols.contains(&symbol.to_ascii_uppercase())
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
                    if self.symbols.contains(&symbol.to_ascii_uppercase())
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
                    let symbol_upper = symbol.to_ascii_uppercase();
                    if self.symbols.contains(&symbol_upper)
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
                    if self.symbols.contains(&symbol.to_ascii_uppercase())
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

fn normalize_snapshot_raw_book(book: &mut binance_codec::RawBook<'_>) {
    let update_id = book.seq_id.saturating_add(1);
    book.seq_id = update_id;
    book.prev_seq_id = update_id;
    book.first_update_id = update_id;
    book.final_update_id = update_id;
}

fn normalize_snapshot_raw_book_view(book: &mut binance_codec::RawBookView<'_>) {
    let update_id = book.seq_id.saturating_add(1);
    book.seq_id = update_id;
    book.prev_seq_id = update_id;
    book.first_update_id = update_id;
    book.final_update_id = update_id;
}

impl Parser for BinanceSnapshotParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(book) = binance_codec::parse_incremental_raw(&msg) {
            match book {
                binance_codec::RawBookParse::Parsed(mut book) => {
                    if !book.is_snapshot {
                        return 0;
                    }
                    normalize_snapshot_raw_book(&mut book);
                    return publish_raw_book_chunks(&book, self.max_levels, tx);
                }
                binance_codec::RawBookParse::View(mut book) => {
                    if !book.is_snapshot {
                        return 0;
                    }
                    normalize_snapshot_raw_book_view(&mut book);
                    return publish_raw_book_view_chunks(&book, self.max_levels, tx);
                }
            }
        }

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

fn publish_raw_book_chunks(
    book: &binance_codec::RawBook<'_>,
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
            book.symbol.to_string(),
            book.first_update_id,
            book.final_update_id,
            book.timestamp_us,
            book.is_snapshot,
            bids_count as u32,
            asks_count as u32,
        );
        inc_msg.set_chunk_index(chunk_idx as u8);
        inc_msg.set_is_last(chunk_idx == total_chunks - 1);
        set_inc_levels_from_raw(
            book.bids.as_slice(),
            book.asks.as_slice(),
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

fn publish_raw_book_view_chunks(
    book: &binance_codec::RawBookView<'_>,
    max_levels: Option<usize>,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> usize {
    let chunks = split_levels(book.bids_count, book.asks_count, max_levels);
    let total_chunks = chunks.len();
    let mut sent_count = 0;

    for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
        chunks.into_iter().enumerate()
    {
        let mut inc_msg = IncMsg::create(
            book.symbol.to_string(),
            book.first_update_id,
            book.final_update_id,
            book.timestamp_us,
            book.is_snapshot,
            bids_count as u32,
            asks_count as u32,
        );
        inc_msg.set_chunk_index(chunk_idx as u8);
        inc_msg.set_is_last(chunk_idx == total_chunks - 1);
        set_inc_levels_from_raw_view(
            book.bids_raw,
            book.asks_raw,
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

fn set_inc_levels_from_raw(
    bids: &[binance_codec::Level],
    asks: &[binance_codec::Level],
    bids_start: usize,
    bids_count: usize,
    asks_start: usize,
    asks_count: usize,
    inc_msg: &mut IncMsg,
) {
    set_inc_levels_from_parsed(
        bids, asks, bids_start, bids_count, asks_start, asks_count, inc_msg,
    );
}

fn set_inc_levels_from_raw_view(
    bids_raw: &[u8],
    asks_raw: &[u8],
    bids_start: usize,
    bids_count: usize,
    asks_start: usize,
    asks_count: usize,
    inc_msg: &mut IncMsg,
) {
    if let Some(iter) = binance_codec::raw_levels_iter(bids_raw) {
        for (i, level) in iter.skip(bids_start).take(bids_count).enumerate() {
            inc_msg.set_bid_level(i, Level::from_values(level.price, level.amount));
        }
    }
    if let Some(iter) = binance_codec::raw_levels_iter(asks_raw) {
        for (i, level) in iter.skip(asks_start).take(asks_count).enumerate() {
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

fn publish_raw_trade(
    trade: binance_codec::RawTrade<'_>,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> usize {
    let trade_msg = TradeMsg::create(
        trade.symbol.to_string(),
        trade.trade_id,
        trade.timestamp_us,
        trade.side,
        trade.price,
        trade.amount,
    );
    if tx.send(trade_msg.to_bytes()).is_ok() {
        1
    } else {
        0
    }
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
    raw_only: bool,
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
        Self::with_depth_mode(max_levels, false, BinanceDepthMode::FuturesDepthUpdate, false)
    }

    pub fn futures_incremental_raw_only(max_levels: Option<usize>) -> Self {
        Self::with_depth_mode(max_levels, false, BinanceDepthMode::FuturesDepthUpdate, true)
    }

    pub fn futures_snapshot(max_levels: Option<usize>) -> Self {
        Self::with_depth_mode(max_levels, true, BinanceDepthMode::FuturesDepthUpdate, false)
    }

    pub fn futures_snapshot_raw_only(max_levels: Option<usize>) -> Self {
        Self::with_depth_mode(max_levels, true, BinanceDepthMode::FuturesDepthUpdate, true)
    }

    pub fn spot_incremental(max_levels: Option<usize>) -> Self {
        Self::with_depth_mode(max_levels, false, BinanceDepthMode::SpotDepthUpdate, false)
    }

    pub fn spot_snapshot(max_levels: Option<usize>) -> Self {
        Self::with_depth_mode(max_levels, true, BinanceDepthMode::SpotSnapshot, false)
    }

    fn with_depth_mode(
        max_levels: Option<usize>,
        is_snapshot: bool,
        mode: BinanceDepthMode,
        raw_only: bool,
    ) -> Self {
        Self {
            max_levels,
            is_snapshot,
            mode,
            raw_only,
        }
    }

    fn apply_depth_mode_raw(&self, book: &mut binance_codec::RawBook<'_>) -> bool {
        match self.mode {
            BinanceDepthMode::FuturesDepthUpdate | BinanceDepthMode::SpotDepthUpdate => {
                if book.is_snapshot {
                    return false;
                }
                book.is_snapshot = self.is_snapshot;
            }
            BinanceDepthMode::SpotSnapshot => {
                if !book.is_snapshot {
                    return false;
                }
                book.is_snapshot = self.is_snapshot;
            }
        }
        true
    }

    fn apply_depth_mode_raw_view(&self, book: &mut binance_codec::RawBookView<'_>) -> bool {
        match self.mode {
            BinanceDepthMode::FuturesDepthUpdate | BinanceDepthMode::SpotDepthUpdate => {
                if book.is_snapshot {
                    return false;
                }
                book.is_snapshot = self.is_snapshot;
            }
            BinanceDepthMode::SpotSnapshot => {
                if !book.is_snapshot {
                    return false;
                }
                book.is_snapshot = self.is_snapshot;
            }
        }
        true
    }
}

impl Parser for BinanceIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(book) = binance_codec::parse_incremental_raw(&msg) {
            match book {
                binance_codec::RawBookParse::Parsed(mut book) => {
                    if !self.apply_depth_mode_raw(&mut book) {
                        return 0;
                    }
                    return publish_raw_book_chunks(&book, self.max_levels, tx);
                }
                binance_codec::RawBookParse::View(mut book) => {
                    if !self.apply_depth_mode_raw_view(&mut book) {
                        return 0;
                    }
                    return publish_raw_book_view_chunks(&book, self.max_levels, tx);
                }
            }
        }

        if self.raw_only {
            return 0;
        }

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
pub struct BinanceTradeParser {
    raw_only: bool,
}

impl Default for BinanceTradeParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceTradeParser {
    pub fn new() -> Self {
        Self { raw_only: false }
    }

    pub fn raw_only() -> Self {
        Self { raw_only: true }
    }
}

#[derive(Clone)]
pub struct BinanceAskBidSpreadParser {
    raw_only: bool,
}

impl Default for BinanceAskBidSpreadParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceAskBidSpreadParser {
    pub fn new() -> Self {
        Self { raw_only: false }
    }

    pub fn raw_only() -> Self {
        Self { raw_only: true }
    }
}

impl Parser for BinanceTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(trade) = binance_codec::parse_trade_raw_borrowed(&msg) {
            return publish_raw_trade(trade, tx);
        }
        if self.raw_only {
            return 0;
        }

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
        if let Some(bbo) = binance_codec::parse_bbo_raw_borrowed(&msg) {
            let spread_msg = AskBidSpreadMsg::create(
                bbo.symbol.to_string(),
                bbo.timestamp_us,
                bbo.bid_price,
                bbo.bid_amount,
                bbo.ask_price,
                bbo.ask_amount,
            );
            return if tx.send(spread_msg.to_bytes()).is_ok() {
                1
            } else {
                0
            };
        }
        if self.raw_only {
            return 0;
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::{get_msg_type, MktMsgType};

    fn parse_one(parser: &dyn Parser, raw: &'static [u8]) -> Vec<Bytes> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let count = parser.parse(Bytes::from_static(raw), &tx);
        drop(tx);
        let mut out = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            out.push(msg);
        }
        assert_eq!(count, out.len());
        out
    }

    #[test]
    fn binance_trade_parser_uses_raw_trade_shape() {
        let parser = BinanceTradeParser::new();
        let out = parse_one(
            &parser,
            br#"{"e":"trade","E":1700000000001,"s":"BTCUSDT","t":1001,"p":"25.0","q":"100","m":true}"#,
        );

        assert_eq!(out.len(), 1);
        assert_eq!(get_msg_type(&out[0]), MktMsgType::TradeInfo);
        assert_eq!(msg_symbol(&out[0]), "BTCUSDT");
        assert_eq!(trade_timestamp(&out[0]), 1_700_000_000_001_000);
    }

    #[test]
    fn binance_trade_raw_only_drops_json_fallback_shape() {
        let parser = BinanceTradeParser::raw_only();
        let out = parse_one(
            &parser,
            br#"{"data":{"e":"bookTicker","s":"BTCUSDT","u":1,"b":"25","B":"1","a":"25.1","A":"1"}}"#,
        );

        assert!(out.is_empty());
    }

    #[test]
    fn binance_bbo_parser_uses_raw_depth_top_shape() {
        let parser = BinanceAskBidSpreadParser::new();
        let out = parse_one(
            &parser,
            br#"{"stream":"btcusdt@depth5@0ms","data":{"e":"depthUpdate","E":1700000000001,"s":"BTCUSDT","U":1,"u":2,
            "b":[["25.0","1"]],"a":[["25.1","3"]]}}"#,
        );

        assert_eq!(out.len(), 1);
        assert_eq!(get_msg_type(&out[0]), MktMsgType::AskBidSpread);
        assert_eq!(AskBidSpreadMsg::get_symbol(&out[0]), "BTCUSDT");
        assert_eq!(
            AskBidSpreadMsg::get_timestamp(&out[0]),
            1_700_000_000_001_000
        );
    }

    #[test]
    fn binance_bbo_raw_only_drops_json_fallback_shape() {
        let parser = BinanceAskBidSpreadParser::raw_only();
        let out = parse_one(
            &parser,
            br#"{"data":{"e":"trade","E":1700000000001,"s":"BTCUSDT","t":1001,"p":"25.0","q":"100","m":true}}"#,
        );

        assert!(out.is_empty());
    }

    #[test]
    fn binance_incremental_parser_uses_raw_depth_shape() {
        let parser = BinanceIncParser::futures_incremental(Some(1));
        let out = parse_one(
            &parser,
            br#"{"e":"depthUpdate","E":1700000000001,"s":"BTCUSDT","U":101,"u":103,
            "b":[["25.0","100"]],"a":[["25.1","50"]]}"#,
        );

        assert_eq!(out.len(), 2);
        assert_eq!(get_msg_type(&out[0]), MktMsgType::OrderBookInc);
    }

    #[test]
    fn binance_incremental_raw_only_drops_json_fallback_shape() {
        let raw = br#"{"e":"depthUpdate","E":1700000000001,"\u0073":"BTCUSDT","U":101,"u":103,
            "b":[["25.0","100"]],"a":[["25.1","50"]]}"#;

        let fallback_parser = BinanceIncParser::futures_incremental(Some(1));
        let fallback_out = parse_one(&fallback_parser, raw);
        assert_eq!(fallback_out.len(), 2);
        assert_eq!(get_msg_type(&fallback_out[0]), MktMsgType::OrderBookInc);

        let raw_only_parser = BinanceIncParser::futures_incremental_raw_only(Some(1));
        let raw_only_out = parse_one(&raw_only_parser, raw);
        assert!(raw_only_out.is_empty());
    }

    #[test]
    fn binance_derivatives_parser_uses_raw_array_shape() {
        let parser = BinanceDerivativesMetricsParser::new(HashSet::from(["BTCUSDT".to_string()]));
        let out = parse_one(
            &parser,
            br#"{"data":[
            {"e":"markPriceUpdate","E":1700000000001,"s":"BTCUSDT","p":"25.0","i":"24.9","r":"0.0001","T":1700003600000},
            {"e":"markPriceUpdate","E":1700000000001,"s":"ETHUSDT","p":"26.0","i":"25.9","r":"0.0002","T":1700003600000}
        ]}"#,
        );

        assert_eq!(out.len(), 3);
        assert_eq!(get_msg_type(&out[0]), MktMsgType::MarkPrice);
        assert_eq!(get_msg_type(&out[1]), MktMsgType::IndexPrice);
        assert_eq!(get_msg_type(&out[2]), MktMsgType::FundingRate);
    }

    #[test]
    fn binance_derivatives_raw_only_drops_json_fallback_shape() {
        let raw =
            br#"{"\u0065":"markPriceUpdate","E":1700000000001,"s":"BTCUSDT","p":"25.0"}"#;

        let fallback_parser =
            BinanceDerivativesMetricsParser::new(HashSet::from(["BTCUSDT".to_string()]));
        let fallback_out = parse_one(&fallback_parser, raw);
        assert_eq!(fallback_out.len(), 1);
        assert_eq!(get_msg_type(&fallback_out[0]), MktMsgType::MarkPrice);

        let raw_only_parser =
            BinanceDerivativesMetricsParser::raw_only(HashSet::from(["BTCUSDT".to_string()]));
        let raw_only_out = parse_one(&raw_only_parser, raw);
        assert!(raw_only_out.is_empty());
    }

    #[test]
    fn binance_signal_parser_uses_raw_event_time() {
        let parser = BinanceSignalParser::new(false);
        let out = parse_one(
            &parser,
            br#"{"e":"depthUpdate","E":1700000000001,"s":"BTCUSDT","U":101,"u":103,
            "b":[["25.0","100"]],"a":[["25.1","50"]]}"#,
        );

        assert_eq!(out.len(), 1);
        assert_eq!(get_msg_type(&out[0]), MktMsgType::TimeSignal);
        assert_eq!(signal_timestamp(&out[0]), 1_700_000_000_001);
    }

    #[test]
    fn binance_signal_raw_only_drops_json_fallback_shape() {
        let raw = br#"{"\u0045":1700000000001}"#;

        let fallback_parser = BinanceSignalParser::new(false);
        let fallback_out = parse_one(&fallback_parser, raw);
        assert_eq!(fallback_out.len(), 1);
        assert_eq!(get_msg_type(&fallback_out[0]), MktMsgType::TimeSignal);

        let raw_only_parser = BinanceSignalParser::raw_only(false);
        let raw_only_out = parse_one(&raw_only_parser, raw);
        assert!(raw_only_out.is_empty());
    }

    #[test]
    fn binance_kline_parser_uses_raw_closed_kline() {
        let parser = BinanceKlineParser::new();
        let out = parse_one(
            &parser,
            br#"{"e":"kline","E":1700000000001,"s":"BTCUSDT",
            "k":{"t":1700000000000,"o":"25.0","h":"26.0","l":"24.5","c":"25.5","v":"123.4","x":true}}"#,
        );

        assert_eq!(out.len(), 1);
        assert_eq!(get_msg_type(&out[0]), MktMsgType::Kline);
        assert_eq!(msg_symbol(&out[0]), "BTCUSDT");
        assert_eq!(kline_timestamp(&out[0]), 1_700_000_000_000);
    }

    #[test]
    fn binance_kline_raw_only_drops_json_fallback_shape() {
        let raw = br#"{"e":"kline","E":1700000000001,"\u0073":"BTCUSDT",
            "k":{"t":1700000000000,"o":"25.0","h":"26.0","l":"24.5","c":"25.5","v":"123.4","x":true}}"#;

        let fallback_parser = BinanceKlineParser::new();
        let fallback_out = parse_one(&fallback_parser, raw);
        assert_eq!(fallback_out.len(), 1);
        assert_eq!(get_msg_type(&fallback_out[0]), MktMsgType::Kline);

        let raw_only_parser = BinanceKlineParser::raw_only();
        let raw_only_out = parse_one(&raw_only_parser, raw);
        assert!(raw_only_out.is_empty());
    }

    #[test]
    fn binance_snapshot_parser_uses_raw_snapshot_shape() {
        let parser = BinanceSnapshotParser::with_max_levels(Some(1));
        let out = parse_one(
            &parser,
            br#"{"lastUpdateId":22345,"s":"BTCUSDT",
            "bids":[["25.0","100"]],"asks":[["25.1","50"]]}"#,
        );

        assert_eq!(out.len(), 2);
        assert_eq!(get_msg_type(&out[0]), MktMsgType::OrderBookInc);
        assert_eq!(msg_symbol(&out[0]), "BTCUSDT");
        assert_eq!(inc_first_update_id(&out[0]), 22346);
    }

    #[test]
    fn binance_snapshot_raw_only_drops_json_fallback_shape() {
        let raw = br#"{"e":"depthUpdate","E":1700000000001,"\u0073":"BTCUSDT","U":101,"u":103,
            "b":[["25.0","100"]],"a":[["25.1","50"]]}"#;

        let fallback_parser = BinanceIncParser::futures_snapshot(Some(1));
        let fallback_out = parse_one(&fallback_parser, raw);
        assert_eq!(fallback_out.len(), 2);
        assert_eq!(get_msg_type(&fallback_out[0]), MktMsgType::OrderBookInc);

        let raw_only_parser = BinanceIncParser::futures_snapshot_raw_only(Some(1));
        let raw_only_out = parse_one(&raw_only_parser, raw);
        assert!(raw_only_out.is_empty());
    }

    #[test]
    fn binance_incremental_parser_uses_raw_view_for_large_books() {
        let parser = BinanceIncParser::futures_incremental(Some(50));
        let mut raw = br#"{"e":"depthUpdate","s":"BTCUSDT","U":1,"u":2,"b":["#.to_vec();
        for i in 0..70 {
            if i > 0 {
                raw.push(b',');
            }
            raw.extend_from_slice(br#"["25.0","1"]"#);
        }
        raw.extend_from_slice(br#"],"a":[["25.1","2"]]}"#);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let count = parser.parse(Bytes::from(raw), &tx);
        drop(tx);
        let mut out = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            out.push(msg);
        }

        assert_eq!(count, 2);
        assert_eq!(out.len(), 2);
        assert_eq!(get_msg_type(&out[0]), MktMsgType::OrderBookInc);
        assert_eq!(msg_symbol(&out[0]), "BTCUSDT");
    }

    fn msg_symbol(data: &[u8]) -> &str {
        let len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        std::str::from_utf8(&data[8..8 + len]).unwrap()
    }

    fn trade_timestamp(data: &[u8]) -> i64 {
        let len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let offset = 8 + len + 8;
        i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ])
    }

    fn signal_timestamp(data: &[u8]) -> i64 {
        i64::from_le_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ])
    }

    fn kline_timestamp(data: &[u8]) -> i64 {
        let len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let offset = 8 + len + 5 * 8;
        i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ])
    }

    fn inc_first_update_id(data: &[u8]) -> i64 {
        let len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let offset = 8 + len;
        i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ])
    }
}
