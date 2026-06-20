//! Binance spread 适配器。
//!
//! Binance margin/futures 支持单边或 `binance-both` 运行，都会发布完整
//! BBO/trade/incremental/derivatives replacement。协议字段解码集中在 `mkt_parsers`，本文件只做
//! 订阅、venue gating 和 spread_pbs 内部 frame/bytes 转换。

use anyhow::{anyhow, Result};
use bytes::Bytes;
use mkt_parsers::binance as binance_codec;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use serde_json::Value;
use std::cell::RefCell;

use crate::spread_pbs::adapter::{
    BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};
use mkt_parsers::msg::mkt_msg::{FundingRateMsg, IndexPriceMsg, LiquidationMsg, MarkPriceMsg};
use order_common::TradingVenue;
use std::rc::Rc;

use crate::spread_pbs::publisher::SpreadDerivativesPublisher;

const BINANCE_SPOT_SBE_WS_URL: &str = "wss://stream-sbe.binance.com:9443/ws";
const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/public/stream";
const BINANCE_FUTURES_MM_WS_URL: &str = "wss://fstream-mm.binance.com/public/stream";
const BINANCE_FUTURES_DERIVATIVES_WS_URL: &str = "wss://fstream.binance.com/market/ws";
const BINANCE_FUTURES_MM_DERIVATIVES_WS_URL: &str = "wss://fstream-mm.binance.com/market/ws";
const BINANCE_SUBSCRIBE_CHUNK: usize = 200;
const SYMBOL_SLOT_CACHE_SIZE: usize = 128;
const ENV_BINANCE_FUTURES_BBO_MODE: &str = "SPREAD_PBS_BINANCE_FUTURES_BBO_MODE";
const ENV_BINANCE_FUTURES_BOOK_TICKER: &str = "SPREAD_PBS_BINANCE_FUTURES_BOOK_TICKER";
pub(crate) const ENV_BINANCE_FUTURES_MM_WS_MODE: &str = "SPREAD_PBS_BINANCE_FUTURES_MM_WS_MODE";
pub(crate) const ENV_BINANCE_FUTURES_MM_WS_LOCAL_IP: &str =
    "SPREAD_PBS_BINANCE_FUTURES_MM_WS_LOCAL_IP";

pub(crate) fn binance_futures_standard_ws_url() -> &'static str {
    BINANCE_FUTURES_WS_URL
}

pub struct BinanceAdapter {
    venue: TradingVenue,
    symbol_slot_by_symbol: RefCell<FastHashMap<String, usize>>,
    symbol_slot_cache: RefCell<SymbolSlotCache>,
}

#[derive(Clone, Copy, Default)]
struct SymbolSlotCacheEntry {
    symbol: [u8; 32],
    len: u8,
    slot: usize,
}

impl SymbolSlotCacheEntry {
    fn new(symbol: &str, slot: usize) -> Option<Self> {
        let bytes = symbol.as_bytes();
        let len = u8::try_from(bytes.len()).ok()?;
        if bytes.len() > 32 {
            return None;
        }
        let mut out = Self {
            symbol: [0; 32],
            len,
            slot,
        };
        out.symbol[..bytes.len()].copy_from_slice(bytes);
        Some(out)
    }

    fn matches(self, symbol: &str) -> bool {
        let len = self.len as usize;
        len != 0 && symbol.len() == len && symbol.as_bytes() == &self.symbol[..len]
    }
}

struct SymbolSlotCache {
    entries: [SymbolSlotCacheEntry; SYMBOL_SLOT_CACHE_SIZE],
}

impl SymbolSlotCache {
    fn new() -> Self {
        Self {
            entries: [SymbolSlotCacheEntry::default(); SYMBOL_SLOT_CACHE_SIZE],
        }
    }

    fn get(&self, symbol: &str) -> Option<usize> {
        let entry = self.entries[slot_cache_index(symbol)];
        entry.matches(symbol).then_some(entry.slot)
    }

    fn insert(&mut self, symbol: &str, slot: usize) {
        if let Some(entry) = SymbolSlotCacheEntry::new(symbol, slot) {
            self.entries[slot_cache_index(symbol)] = entry;
        }
    }

    fn clear(&mut self) {
        self.entries = [SymbolSlotCacheEntry::default(); SYMBOL_SLOT_CACHE_SIZE];
    }
}

fn slot_cache_index(symbol: &str) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for b in symbol.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as usize & (SYMBOL_SLOT_CACHE_SIZE - 1)
}

impl BinanceAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        Self {
            venue,
            symbol_slot_by_symbol: RefCell::new(fast_hash_map()),
            symbol_slot_cache: RefCell::new(SymbolSlotCache::new()),
        }
    }
}

impl VenueAdapter for BinanceAdapter {
    fn name(&self) -> &'static str {
        "binance"
    }

    fn ws_url(&self) -> String {
        match self.venue {
            TradingVenue::BinanceMargin => BINANCE_SPOT_SBE_WS_URL.to_string(),
            TradingVenue::BinanceFutures => binance_futures_ws_url().to_string(),
            other => unreachable!("BinanceAdapter created with non-binance venue: {:?}", other),
        }
    }

    fn ws_headers(&self) -> Vec<(String, String)> {
        if self.venue != TradingVenue::BinanceMargin {
            return Vec::new();
        }
        std::env::var("BINANCE_SBE_API_KEY")
            .or_else(|_| std::env::var("BINANCE_API_KEY"))
            .ok()
            .map(|key| vec![("X-MBX-APIKEY".to_string(), key)])
            .unwrap_or_default()
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        match self.venue {
            TradingVenue::BinanceFutures => match binance_futures_bbo_mode() {
                BinanceFuturesBboMode::Race => {
                    build_multi_stream_subscribe(symbols.iter().flat_map(|sym| {
                        let sym = sym.to_ascii_lowercase();
                        [format!("{}@bookTicker", sym), format!("{}@depth5@0ms", sym)]
                    }))
                }
                BinanceFuturesBboMode::BookTicker => build_stream_subscribe(symbols, "bookTicker"),
                BinanceFuturesBboMode::Depth5 => build_stream_subscribe(symbols, "depth5@0ms"),
            },
            TradingVenue::BinanceMargin => build_stream_subscribe(symbols, "bestBidAsk"),
            other => unreachable!("BinanceAdapter created with non-binance venue: {:?}", other),
        }
    }

    fn build_trade_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_stream_subscribe(symbols, "trade")
    }

    fn build_incremental_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        match self.venue {
            TradingVenue::BinanceMargin => build_stream_subscribe(symbols, "depth"),
            TradingVenue::BinanceFutures => build_stream_subscribe(symbols, "depth@0ms"),
            other => unreachable!("BinanceAdapter created with non-binance venue: {:?}", other),
        }
    }

    fn build_derivatives_subscribe(&self, _symbols: &[String]) -> Vec<Value> {
        if self.venue != TradingVenue::BinanceFutures {
            return Vec::new();
        }
        vec![
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": ["!markPrice@arr@1s"],
                "id": 1,
            }),
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": ["!forceOrder@arr"],
                "id": 2,
            }),
        ]
    }

    fn trade_ws_url(&self) -> Option<String> {
        Some(self.ws_url())
    }

    fn incremental_ws_url(&self) -> Option<String> {
        Some(self.ws_url())
    }

    fn derivatives_ws_url(&self) -> Option<String> {
        if self.venue == TradingVenue::BinanceFutures {
            Some(binance_futures_derivatives_ws_url().to_string())
        } else {
            None
        }
    }

    fn seed_symbols(&self, symbols: &[String]) {
        let mut slots = self.symbol_slot_by_symbol.borrow_mut();
        slots.clear();
        for symbol in symbols {
            let next_idx = slots.len();
            slots.entry(symbol.to_ascii_uppercase()).or_insert(next_idx);
        }
        self.symbol_slot_cache.borrow_mut().clear();
    }

    fn symbol_slot_index(&self, symbol: &str) -> Option<usize> {
        if let Some(slot) = self.symbol_slot_cache.borrow().get(symbol) {
            return Some(slot);
        }

        let slot = self.symbol_slot_by_symbol.borrow().get(symbol).copied()?;
        self.symbol_slot_cache.borrow_mut().insert(symbol, slot);
        Some(slot)
    }

    fn parse_frame(
        &self,
        value: &Value,
        emit: &mut dyn FnMut(BboFrame) -> Result<()>,
    ) -> Result<()> {
        if let Some(bbo) = binance_codec::parse_bbo_json(value) {
            emit(bbo_to_frame(bbo))?;
            return Ok(());
        }

        let payload = binance_codec::payload(value);
        if looks_like_bbo(payload)
            && payload.get("u").is_none()
            && payload.get("lastUpdateId").is_none()
        {
            let symbol = payload.get("s").and_then(|v| v.as_str()).unwrap_or("?");
            return Err(anyhow!("binance spread {} missing u/lastUpdateId", symbol));
        }
        Ok(())
    }

    fn parse_bbo_raw(
        &self,
        raw: &[u8],
        emit: &mut dyn FnMut(BboFrame) -> Result<()>,
    ) -> Result<bool> {
        let Some(bbo) = self.parse_bbo_raw_borrowed(raw) else {
            return Ok(false);
        };
        emit(raw_bbo_to_frame(bbo))?;
        Ok(true)
    }

    fn parse_bbo_raw_borrowed<'a>(&self, raw: &'a [u8]) -> Option<binance_codec::RawBbo<'a>> {
        if self.venue != TradingVenue::BinanceFutures {
            return None;
        }
        binance_codec::parse_bbo_raw_borrowed(raw)
    }

    fn parse_trade_raw_borrowed<'a>(&self, raw: &'a [u8]) -> Option<binance_codec::RawTrade<'a>> {
        if self.venue != TradingVenue::BinanceFutures {
            return None;
        }
        binance_codec::parse_trade_raw_borrowed(raw)
    }

    fn parse_incremental_raw_borrowed<'a>(
        &self,
        raw: &'a [u8],
    ) -> Option<binance_codec::RawBook<'a>> {
        if self.venue != TradingVenue::BinanceFutures {
            return None;
        }
        binance_codec::parse_incremental_raw_borrowed(raw)
    }

    fn parse_incremental_raw<'a>(&self, raw: &'a [u8]) -> Option<binance_codec::RawBookParse<'a>> {
        if self.venue != TradingVenue::BinanceFutures {
            return None;
        }
        binance_codec::parse_incremental_raw(raw)
    }

    fn parse_incremental_raw_view<'a>(
        &self,
        raw: &'a [u8],
    ) -> Option<binance_codec::RawBookView<'a>> {
        if self.venue != TradingVenue::BinanceFutures {
            return None;
        }
        binance_codec::parse_incremental_raw_view(raw)
    }

    fn parse_trade_frame(&self, value: &Value) -> Result<Vec<TradeFrame>> {
        Ok(binance_codec::parse_trade_json(value)
            .map(trade_to_frame)
            .into_iter()
            .collect())
    }

    fn parse_incremental_frame(&self, value: &Value) -> Result<Vec<IncrementalFrame>> {
        Ok(binance_codec::parse_incremental_json(value)
            .map(book_to_incremental)
            .into_iter()
            .collect())
    }

    fn parse_derivatives_frame(&self, value: &Value) -> Result<Vec<Bytes>> {
        if self.venue != TradingVenue::BinanceFutures {
            return Ok(Vec::new());
        }
        let slots = self.symbol_slot_by_symbol.borrow();
        Ok(binance_codec::parse_derivatives_json(value)
            .into_iter()
            .filter(|derivative| {
                slots.is_empty() || slots.contains_key(derivative_symbol(derivative))
            })
            .map(derivative_to_bytes)
            .collect())
    }

    fn skip_json_fallback_after_raw_miss(&self) -> bool {
        self.venue == TradingVenue::BinanceFutures
    }

    fn publish_derivatives_raw(
        &self,
        raw: &[u8],
        publisher: &Rc<SpreadDerivativesPublisher>,
        symbol_slot: &mut dyn FnMut(&str) -> Option<usize>,
        published: &mut u64,
    ) -> bool {
        if self.venue != TradingVenue::BinanceFutures {
            return false;
        }
        let active_is_empty = self.symbol_slot_by_symbol.borrow().is_empty();
        let mut handled = false;
        let parsed = binance_codec::parse_derivatives_raw_borrowed(raw, |derivative| {
            handled = true;
            let symbol = raw_derivative_symbol(&derivative);
            let slot_index = symbol_slot(symbol);
            if !active_is_empty && slot_index.is_none() {
                return Some(());
            }
            match publish_raw_derivative(publisher, slot_index, derivative) {
                Ok(count) => *published += count as u64,
                Err(e) => log::warn!("spread_pbs derivatives publish failed: {:#}", e),
            }
            Some(())
        });
        parsed.is_some() && handled
    }

    fn parse_binary_frame(
        &self,
        raw: &[u8],
        emit: &mut dyn FnMut(BboFrame) -> Result<()>,
    ) -> Result<()> {
        if self.venue != TradingVenue::BinanceMargin {
            return Ok(());
        }
        if let Some(bbo) = binance_codec::parse_sbe_bbo(raw) {
            emit(bbo_to_frame(bbo))?;
        }
        Ok(())
    }

    fn parse_trade_binary_frame(&self, raw: &[u8]) -> Result<Vec<TradeFrame>> {
        if self.venue != TradingVenue::BinanceMargin {
            return Ok(Vec::new());
        }
        Ok(binance_codec::parse_sbe_trades(raw)
            .into_iter()
            .map(trade_to_frame)
            .collect())
    }

    fn parse_incremental_binary_frame(&self, raw: &[u8]) -> Result<Vec<IncrementalFrame>> {
        if self.venue != TradingVenue::BinanceMargin {
            return Ok(Vec::new());
        }
        Ok(binance_codec::parse_sbe_incremental(raw)
            .map(book_to_incremental)
            .into_iter()
            .collect())
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        None
    }
}

fn build_stream_subscribe(symbols: &[String], channel: &str) -> Vec<Value> {
    build_multi_stream_subscribe(
        symbols
            .iter()
            .map(|sym| format!("{}@{}", sym.to_ascii_lowercase(), channel)),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BinanceFuturesBboMode {
    Race,
    BookTicker,
    Depth5,
}

fn binance_futures_bbo_mode() -> BinanceFuturesBboMode {
    if let Ok(raw) = std::env::var(ENV_BINANCE_FUTURES_BBO_MODE) {
        match raw.trim().to_ascii_lowercase().as_str() {
            "race" | "both" | "bookticker+depth5" | "book_ticker+depth5" => {
                return BinanceFuturesBboMode::Race
            }
            "bookticker" | "book_ticker" | "ticker" => return BinanceFuturesBboMode::BookTicker,
            "depth5" | "book" | "books" => return BinanceFuturesBboMode::Depth5,
            other => log::warn!(
                "invalid {}={:?}; using race mode",
                ENV_BINANCE_FUTURES_BBO_MODE,
                other
            ),
        }
    }
    if binance_futures_book_ticker_enabled() {
        BinanceFuturesBboMode::Race
    } else {
        BinanceFuturesBboMode::Depth5
    }
}

fn binance_futures_book_ticker_enabled() -> bool {
    match std::env::var(ENV_BINANCE_FUTURES_BOOK_TICKER) {
        Ok(raw) => parse_env_bool(&raw).unwrap_or(true),
        Err(_) => true,
    }
}

pub(crate) fn binance_futures_mm_ws_enabled() -> bool {
    match std::env::var(ENV_BINANCE_FUTURES_MM_WS_MODE) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "" | "off" => false,
            "on" => true,
            _ => panic!(
                "{} must be 'on' or 'off' when set; got '{}'",
                ENV_BINANCE_FUTURES_MM_WS_MODE,
                raw.trim()
            ),
        },
        Err(_) => false,
    }
}

fn binance_futures_ws_url() -> &'static str {
    if binance_futures_mm_ws_enabled() {
        BINANCE_FUTURES_MM_WS_URL
    } else {
        BINANCE_FUTURES_WS_URL
    }
}

fn binance_futures_derivatives_ws_url() -> &'static str {
    if binance_futures_mm_ws_enabled() {
        BINANCE_FUTURES_MM_DERIVATIVES_WS_URL
    } else {
        BINANCE_FUTURES_DERIVATIVES_WS_URL
    }
}

fn parse_env_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "t" | "yes" | "y" | "on" | "enable" | "enabled" => Some(true),
        "0" | "false" | "f" | "no" | "n" | "off" | "disable" | "disabled" => Some(false),
        _ => None,
    }
}

fn build_multi_stream_subscribe(streams: impl IntoIterator<Item = String>) -> Vec<Value> {
    let streams: Vec<String> = streams.into_iter().collect();
    let chunk_size = BINANCE_SUBSCRIBE_CHUNK.max(1);
    let mut out = Vec::new();
    for (i, chunk) in streams.chunks(chunk_size).enumerate() {
        out.push(serde_json::json!({
            "method": "SUBSCRIBE",
            "params": chunk,
            "id": (i as u64) + 1,
        }));
    }
    out
}

fn looks_like_bbo(payload: &Value) -> bool {
    payload.get("s").is_some() && (payload.get("b").is_some() || payload.get("a").is_some())
}

fn bbo_to_frame(bbo: binance_codec::Bbo) -> BboFrame {
    BboFrame {
        symbol: bbo.symbol,
        ts_us: bbo.timestamp_us,
        seq_id: bbo.seq_id,
        reset_seq: false,
        bid_price: bbo.bid_price,
        bid_amount: bbo.bid_amount,
        ask_price: bbo.ask_price,
        ask_amount: bbo.ask_amount,
    }
}

fn raw_bbo_to_frame(bbo: binance_codec::RawBbo<'_>) -> BboFrame {
    BboFrame {
        symbol: bbo.symbol.to_ascii_uppercase(),
        ts_us: bbo.timestamp_us,
        seq_id: bbo.seq_id,
        reset_seq: false,
        bid_price: bbo.bid_price,
        bid_amount: bbo.bid_amount,
        ask_price: bbo.ask_price,
        ask_amount: bbo.ask_amount,
    }
}

fn trade_to_frame(trade: binance_codec::Trade) -> TradeFrame {
    TradeFrame {
        symbol: trade.symbol,
        timestamp_us: trade.timestamp_us,
        seq_id: trade.seq_id,
        trade_id: trade.trade_id,
        side: trade.side,
        price: trade.price,
        amount: trade.amount,
    }
}

fn book_to_incremental(book: binance_codec::Book) -> IncrementalFrame {
    IncrementalFrame::Book {
        symbol: book.symbol,
        timestamp: book.timestamp_us,
        seq_id: book.seq_id,
        prev_seq_id: book.prev_seq_id,
        first_update_id: book.first_update_id,
        final_update_id: book.final_update_id,
        gap_check: book.gap_check,
        is_snapshot: book.is_snapshot,
        bids: book
            .bids
            .into_iter()
            .map(|level| mkt_parsers::msg::mkt_msg::Level::from_values(level.price, level.amount))
            .collect(),
        asks: book
            .asks
            .into_iter()
            .map(|level| mkt_parsers::msg::mkt_msg::Level::from_values(level.price, level.amount))
            .collect(),
    }
}

fn derivative_symbol(derivative: &binance_codec::Derivative) -> &str {
    match derivative {
        binance_codec::Derivative::MarkPrice { symbol, .. }
        | binance_codec::Derivative::IndexPrice { symbol, .. }
        | binance_codec::Derivative::FundingRate { symbol, .. }
        | binance_codec::Derivative::Liquidation { symbol, .. } => symbol.as_str(),
    }
}

fn derivative_to_bytes(derivative: binance_codec::Derivative) -> Bytes {
    match derivative {
        binance_codec::Derivative::MarkPrice {
            symbol,
            price,
            timestamp_us,
        } => MarkPriceMsg::create(symbol, price, timestamp_us).to_bytes(),
        binance_codec::Derivative::IndexPrice {
            symbol,
            price,
            timestamp_us,
        } => IndexPriceMsg::create(symbol, price, timestamp_us).to_bytes(),
        binance_codec::Derivative::FundingRate {
            symbol,
            funding_rate,
            next_funding_time_us,
            timestamp_us,
        } => FundingRateMsg::create(symbol, funding_rate, next_funding_time_us, timestamp_us)
            .to_bytes(),
        binance_codec::Derivative::Liquidation {
            symbol,
            side,
            amount,
            price,
            timestamp_us,
        } => LiquidationMsg::create(symbol, side, amount, price, timestamp_us).to_bytes(),
    }
}

fn raw_derivative_symbol<'a>(derivative: &'a binance_codec::RawDerivative<'a>) -> &'a str {
    match derivative {
        binance_codec::RawDerivative::MarkPrice { symbol, .. }
        | binance_codec::RawDerivative::Liquidation { symbol, .. } => symbol,
    }
}

fn publish_raw_derivative(
    publisher: &Rc<SpreadDerivativesPublisher>,
    slot_index: Option<usize>,
    derivative: binance_codec::RawDerivative<'_>,
) -> Result<usize> {
    match derivative {
        binance_codec::RawDerivative::MarkPrice {
            symbol,
            mark_price,
            index_price,
            funding_rate,
            next_funding_time_us,
            timestamp_us,
        } => {
            let mark_price = mark_price.filter(|price| *price > 0.0);
            let index_price = index_price.filter(|price| *price > 0.0);
            if let Some(slot_index) = slot_index {
                return publisher.publish_mark_price_bundle_for_slot(
                    slot_index,
                    symbol,
                    mark_price,
                    index_price,
                    funding_rate,
                    next_funding_time_us,
                    timestamp_us,
                );
            }

            let mut count = 0usize;
            if let Some(price) = mark_price {
                publisher.publish_mark_price(symbol, price, timestamp_us)?;
                count += 1;
            }
            if let Some(price) = index_price {
                publisher.publish_index_price(symbol, price, timestamp_us)?;
                count += 1;
            }
            if let (Some(rate), Some(next_time)) = (funding_rate, next_funding_time_us) {
                publisher.publish_funding_rate(symbol, rate, next_time, timestamp_us)?;
                count += 1;
            }
            Ok(count)
        }
        binance_codec::RawDerivative::Liquidation {
            symbol,
            side,
            amount,
            price,
            timestamp_us,
        } => {
            if let Some(slot_index) = slot_index {
                publisher.publish_liquidation_for_slot(
                    slot_index,
                    symbol,
                    side,
                    amount,
                    price,
                    timestamp_us,
                )?;
            } else {
                publisher.publish_liquidation(symbol, side, amount, price, timestamp_us)?;
            }
            Ok(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::{get_msg_type, MktMsgType};
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn v(raw: &str) -> Value {
        serde_json::from_str(raw).expect("test fixture must be valid JSON")
    }

    fn sbe_bbo_frame() -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(&50u16.to_le_bytes());
        msg.extend_from_slice(&10001u16.to_le_bytes());
        msg.extend_from_slice(&1u16.to_le_bytes());
        msg.extend_from_slice(&0u16.to_le_bytes());
        msg.extend_from_slice(&1_700_000_000_001_002i64.to_le_bytes());
        msg.extend_from_slice(&12345i64.to_le_bytes());
        msg.push(-2i8 as u8);
        msg.push(-3i8 as u8);
        msg.extend_from_slice(&2500i64.to_le_bytes());
        msg.extend_from_slice(&100_000i64.to_le_bytes());
        msg.extend_from_slice(&2510i64.to_le_bytes());
        msg.extend_from_slice(&50_000i64.to_le_bytes());
        msg.push(7);
        msg.extend_from_slice(b"btcusdt");
        msg
    }

    fn sbe_trade_frame() -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(&18u16.to_le_bytes());
        msg.extend_from_slice(&10000u16.to_le_bytes());
        msg.extend_from_slice(&1u16.to_le_bytes());
        msg.extend_from_slice(&0u16.to_le_bytes());
        msg.extend_from_slice(&1_700_000_000_001_002i64.to_le_bytes());
        msg.extend_from_slice(&1_700_000_000_001_000i64.to_le_bytes());
        msg.push(-2i8 as u8);
        msg.push(-3i8 as u8);
        msg.extend_from_slice(&25u16.to_le_bytes());
        msg.extend_from_slice(&2u32.to_le_bytes());
        msg.extend_from_slice(&1001i64.to_le_bytes());
        msg.extend_from_slice(&2500i64.to_le_bytes());
        msg.extend_from_slice(&100_000i64.to_le_bytes());
        msg.push(1);
        msg.extend_from_slice(&1002i64.to_le_bytes());
        msg.extend_from_slice(&2510i64.to_le_bytes());
        msg.extend_from_slice(&50_000i64.to_le_bytes());
        msg.push(0);
        msg.push(7);
        msg.extend_from_slice(b"btcusdt");
        msg
    }

    fn sbe_depth_diff_frame() -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(&26u16.to_le_bytes());
        msg.extend_from_slice(&10003u16.to_le_bytes());
        msg.extend_from_slice(&1u16.to_le_bytes());
        msg.extend_from_slice(&0u16.to_le_bytes());
        msg.extend_from_slice(&1_700_000_000_001_002i64.to_le_bytes());
        msg.extend_from_slice(&101i64.to_le_bytes());
        msg.extend_from_slice(&103i64.to_le_bytes());
        msg.push(-2i8 as u8);
        msg.push(-3i8 as u8);
        msg.extend_from_slice(&16u16.to_le_bytes());
        msg.extend_from_slice(&1u16.to_le_bytes());
        msg.extend_from_slice(&2500i64.to_le_bytes());
        msg.extend_from_slice(&100_000i64.to_le_bytes());
        msg.extend_from_slice(&16u16.to_le_bytes());
        msg.extend_from_slice(&1u16.to_le_bytes());
        msg.extend_from_slice(&2510i64.to_le_bytes());
        msg.extend_from_slice(&50_000i64.to_le_bytes());
        msg.push(7);
        msg.extend_from_slice(b"btcusdt");
        msg
    }

    #[test]
    fn parses_depth5_top_of_book_prefers_e_field() {
        let raw = r#"{
            "stream":"btcusdt@depth5@0ms",
            "data":{"e":"depthUpdate","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","U":12300,"u":12345,
                "b":[["25.0","100"],["24.9","2"]],"a":[["25.1","50"],["25.2","3"]]}
        }"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let frames = a.collect_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].symbol, "BTCUSDT");
        assert_eq!(frames[0].seq_id, 12345);
        assert_eq!(frames[0].ts_us, 1_700_000_000_001_000);
        assert!((frames[0].bid_price - 25.0).abs() < 1e-9);
        assert!((frames[0].ask_amount - 50.0).abs() < 1e-9);
    }

    #[test]
    fn parses_book_ticker_top_of_book() {
        let raw = r#"{
            "stream":"btcusdt@bookTicker",
            "data":{"e":"bookTicker","u":22345,"s":"BTCUSDT","b":"25.0","B":"100","a":"25.1","A":"50","E":1700000000002}
        }"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let frames = a.collect_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].symbol, "BTCUSDT");
        assert_eq!(frames[0].seq_id, 22345);
        assert_eq!(frames[0].ts_us, 1_700_000_000_002_000);
        assert!((frames[0].bid_price - 25.0).abs() < 1e-9);
        assert!((frames[0].ask_amount - 50.0).abs() < 1e-9);
    }

    #[test]
    fn missing_u_field_is_an_error() {
        let raw = r#"{"data":{"s":"BTCUSDT","b":"25","B":"1","a":"25.1","A":"1"}}"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        assert!(a.collect_frame(&v(raw)).is_err());
    }

    #[test]
    fn subscribe_chunks() {
        let _guard = env_lock().lock().unwrap();
        std::env::remove_var(ENV_BINANCE_FUTURES_BBO_MODE);
        std::env::remove_var(ENV_BINANCE_FUTURES_BOOK_TICKER);
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let symbols: Vec<String> = (0..450).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 5);
        assert_eq!(msgs[0]["params"].as_array().unwrap().len(), 200);
        assert_eq!(msgs[4]["params"].as_array().unwrap().len(), 100);
        assert_eq!(msgs[0]["params"][0], "sym0usdt@bookTicker");
        assert_eq!(msgs[0]["params"][1], "sym0usdt@depth5@0ms");
        assert_eq!(msgs[0]["params"][2], "sym1usdt@bookTicker");
    }

    #[test]
    fn binance_futures_book_ticker_can_be_disabled_by_env() {
        let _guard = env_lock().lock().unwrap();
        std::env::remove_var(ENV_BINANCE_FUTURES_BBO_MODE);
        std::env::set_var(ENV_BINANCE_FUTURES_BOOK_TICKER, "0");
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let symbols: Vec<String> = (0..450).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        std::env::remove_var(ENV_BINANCE_FUTURES_BOOK_TICKER);

        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["params"].as_array().unwrap().len(), 200);
        assert_eq!(msgs[2]["params"].as_array().unwrap().len(), 50);
        assert_eq!(msgs[0]["params"][0], "sym0usdt@depth5@0ms");
        assert_eq!(msgs[0]["params"][1], "sym1usdt@depth5@0ms");
    }

    #[test]
    fn binance_futures_bbo_mode_can_select_bookticker_only() {
        let _guard = env_lock().lock().unwrap();
        std::env::set_var(ENV_BINANCE_FUTURES_BBO_MODE, "bookticker");
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let symbols: Vec<String> = (0..450).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        std::env::remove_var(ENV_BINANCE_FUTURES_BBO_MODE);

        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["params"].as_array().unwrap().len(), 200);
        assert_eq!(msgs[2]["params"].as_array().unwrap().len(), 50);
        assert_eq!(msgs[0]["params"][0], "sym0usdt@bookTicker");
        assert_eq!(msgs[0]["params"][1], "sym1usdt@bookTicker");
    }

    #[test]
    fn symbol_slot_cache_handles_interleaved_symbols_and_seed_reset() {
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        a.seed_symbols(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        assert_eq!(a.symbol_slot_index("BTCUSDT"), Some(0));
        assert_eq!(a.symbol_slot_index("ETHUSDT"), Some(1));
        assert_eq!(a.symbol_slot_index("BTCUSDT"), Some(0));

        a.seed_symbols(&["SOLUSDT".to_string()]);
        assert_eq!(a.symbol_slot_index("BTCUSDT"), None);
        assert_eq!(a.symbol_slot_index("SOLUSDT"), Some(0));
    }

    #[test]
    fn full_replacement_subscriptions_use_derivatives_market_url() {
        let _guard = env_lock().lock().unwrap();
        std::env::remove_var(ENV_BINANCE_FUTURES_MM_WS_MODE);
        let futures = BinanceAdapter::new(TradingVenue::BinanceFutures);
        assert_eq!(
            futures.trade_ws_url().as_deref(),
            Some(BINANCE_FUTURES_WS_URL)
        );
        assert_eq!(
            futures.incremental_ws_url().as_deref(),
            Some(BINANCE_FUTURES_WS_URL)
        );
        assert_eq!(
            futures.derivatives_ws_url().as_deref(),
            Some(BINANCE_FUTURES_DERIVATIVES_WS_URL)
        );
        assert_eq!(
            futures.build_trade_subscribe(&["BTCUSDT".to_string()])[0]["params"][0],
            "btcusdt@trade"
        );
        assert_eq!(
            futures.build_incremental_subscribe(&["BTCUSDT".to_string()])[0]["params"][0],
            "btcusdt@depth@0ms"
        );
        let derivatives = futures.build_derivatives_subscribe(&["BTCUSDT".to_string()]);
        assert_eq!(derivatives.len(), 2);
        assert_eq!(derivatives[0]["params"][0], "!markPrice@arr@1s");
        assert_eq!(derivatives[1]["params"][0], "!forceOrder@arr");

        let spot = BinanceAdapter::new(TradingVenue::BinanceMargin);
        assert_eq!(
            spot.trade_ws_url().as_deref(),
            Some(BINANCE_SPOT_SBE_WS_URL)
        );
        assert_eq!(
            spot.incremental_ws_url().as_deref(),
            Some(BINANCE_SPOT_SBE_WS_URL)
        );
        assert!(spot.derivatives_ws_url().is_none());
        assert_eq!(
            spot.build_subscribe(&["BTCUSDT".to_string()])[0]["params"][0],
            "btcusdt@bestBidAsk"
        );
        assert_eq!(
            spot.build_incremental_subscribe(&["BTCUSDT".to_string()])[0]["params"][0],
            "btcusdt@depth"
        );
    }

    #[test]
    fn binance_futures_mm_ws_mode_switches_futures_urls() {
        let _guard = env_lock().lock().unwrap();
        std::env::set_var(ENV_BINANCE_FUTURES_MM_WS_MODE, "on");
        let futures = BinanceAdapter::new(TradingVenue::BinanceFutures);

        assert_eq!(
            futures.trade_ws_url().as_deref(),
            Some(BINANCE_FUTURES_MM_WS_URL)
        );
        assert_eq!(
            futures.incremental_ws_url().as_deref(),
            Some(BINANCE_FUTURES_MM_WS_URL)
        );
        assert_eq!(
            futures.derivatives_ws_url().as_deref(),
            Some(BINANCE_FUTURES_MM_DERIVATIVES_WS_URL)
        );

        let spot = BinanceAdapter::new(TradingVenue::BinanceMargin);
        assert_eq!(
            spot.trade_ws_url().as_deref(),
            Some(BINANCE_SPOT_SBE_WS_URL)
        );
        std::env::remove_var(ENV_BINANCE_FUTURES_MM_WS_MODE);
    }

    #[test]
    fn parses_sbe_best_bid_ask() {
        let a = BinanceAdapter::new(TradingVenue::BinanceMargin);
        let frames = a.collect_binary_frame(&sbe_bbo_frame()).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].symbol, "BTCUSDT");
        assert_eq!(frames[0].ts_us, 1_700_000_000_001_002);
        assert_eq!(frames[0].seq_id, 12345);
        assert!((frames[0].bid_amount - 100.0).abs() < 1e-9);
    }

    #[test]
    fn parses_json_trade_incremental_and_derivatives_as_us() {
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        a.seed_symbols(&["BTCUSDT".to_string()]);
        let trade = r#"{"data":{"e":"trade","E":1700000000001,"s":"BTCUSDT","t":1001,"p":"25.0","q":"100","m":true}}"#;
        let trades = a.parse_trade_frame(&v(trade)).unwrap();
        assert_eq!(trades[0].timestamp_us, 1_700_000_000_001_000);
        assert_eq!(trades[0].side, 'S');

        let inc = r#"{"data":{"e":"depthUpdate","E":1700000000001,"s":"BTCUSDT","U":101,"u":103,
            "b":[["25.0","100"]],"a":[["25.1","50"]]}}"#;
        let inc = a.parse_incremental_frame(&v(inc)).unwrap();
        let IncrementalFrame::Book {
            timestamp,
            seq_id,
            prev_seq_id,
            bids,
            asks,
            ..
        } = &inc[0]
        else {
            panic!("expected book incremental");
        };
        assert_eq!(*timestamp, 1_700_000_000_001_000);
        assert_eq!(*seq_id, 103);
        assert_eq!(*prev_seq_id, 100);
        assert!((bids[0].price - 25.0).abs() < 1e-9);
        assert!((asks[0].amount - 50.0).abs() < 1e-9);

        let mark = r#"{"data":{"e":"markPriceUpdate","E":1700000000001,"s":"BTCUSDT","p":"25.0","i":"24.9","r":"0.0001","T":1700003600000}}"#;
        let bytes = a.parse_derivatives_frame(&v(mark)).unwrap();
        assert_eq!(bytes.len(), 3);
        assert_eq!(
            MarkPriceMsg::get_timestamp(&bytes[0]),
            1_700_000_000_001_000
        );
        assert_eq!(
            IndexPriceMsg::get_timestamp(&bytes[1]),
            1_700_000_000_001_000
        );
        assert_eq!(
            FundingRateMsg::get_next_funding_time(&bytes[2]),
            1_700_003_600_000_000
        );

        let mark_arr = r#"{"data":[
            {"e":"markPriceUpdate","E":1700000000001,"s":"BTCUSDT","p":"25.0","i":"24.9","r":"0.0001","T":1700003600000},
            {"e":"markPriceUpdate","E":1700000000001,"s":"ETHUSDT","p":"26.0","i":"25.9","r":"0.0002","T":1700003600000}
        ]}"#;
        assert_eq!(a.parse_derivatives_frame(&v(mark_arr)).unwrap().len(), 3);

        let liquidation = r#"{"data":{"e":"forceOrder","E":1700000000001,
            "o":{"s":"BTCUSDT","S":"SELL","z":"10","ap":"25.2","T":1700000000000}}}"#;
        let liq = a.parse_derivatives_frame(&v(liquidation)).unwrap();
        assert_eq!(liq.len(), 1);
        assert_eq!(get_msg_type(&liq[0]), MktMsgType::LiquidationOrder);
    }

    #[test]
    fn parses_sbe_trade_and_depth_as_us() {
        let a = BinanceAdapter::new(TradingVenue::BinanceMargin);
        let trades = a.parse_trade_binary_frame(&sbe_trade_frame()).unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].timestamp_us, 1_700_000_000_001_002);
        assert_eq!(trades[0].trade_id, 1001);
        assert_eq!(trades[0].side, 'S');
        assert!((trades[1].amount - 50.0).abs() < 1e-9);

        let inc = a
            .parse_incremental_binary_frame(&sbe_depth_diff_frame())
            .unwrap();
        let IncrementalFrame::Book {
            timestamp,
            seq_id,
            prev_seq_id,
            bids,
            asks,
            ..
        } = &inc[0]
        else {
            panic!("expected book incremental");
        };
        assert_eq!(*timestamp, 1_700_000_000_001_002);
        assert_eq!(*seq_id, 103);
        assert_eq!(*prev_seq_id, 100);
        assert!((bids[0].amount - 100.0).abs() < 1e-9);
        assert!((asks[0].price - 25.1).abs() < 1e-9);
    }
}
