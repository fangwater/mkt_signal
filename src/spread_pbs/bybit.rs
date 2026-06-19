//! Bybit `orderbook.1.<sym>` spread 适配器（depth=1 BBO，含 delta）。
//!
//! - spot:   `wss://stream.bybit.com/v5/public/spot`
//! - linear: `wss://stream.bybit.com/v5/public/linear`
//! - subscribe: `{"op":"subscribe","args":["orderbook.1.BTCUSDT", ...]}`
//! - frame: `{"topic":"orderbook.1.BTCUSDT","type":"snapshot|delta","ts":..,
//!            "data":{"s":..,"b":[["p","s"]],"a":[["p","s"]],"u":<seq>}}`
//! - delta 只推变化的那一侧，所以本 adapter 内部维护 per-symbol BBO cache。
//!   收到 snapshot 时全量重置；收到 delta 时仅刷新非空那一侧，输出 cache 当前态。
//! - 心跳: `{"op":"ping"}` 每 20s。

use anyhow::{anyhow, Result};
use bytes::Bytes;
use mkt_parsers::bybit as bybit_codec;
use runtime_common::fast_hash::{fast_hash_map_with_capacity, FastHashMap};
use serde_json::Value;
use std::cell::RefCell;
use std::time::Duration;

use crate::spread_pbs::adapter::{
    BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};
use mkt_parsers::msg::mkt_msg::{
    FundingRateMsg, IndexPriceMsg, Level, LiquidationMsg, MarkPriceMsg,
};
use order_common::TradingVenue;

const BYBIT_SPOT_WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";
const BYBIT_LINEAR_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
/// Bybit V5 每个 `op:subscribe` 请求的 args 上限（与 `dat_pbs` `Config::get_batch_size`
/// 对齐：spot=10，linear=300；超出会被服务端静默丢弃）。
const BYBIT_SPOT_SUBSCRIBE_CHUNK: usize = 10;
const BYBIT_LINEAR_SUBSCRIBE_CHUNK: usize = 300;

#[derive(Default, Clone, Copy)]
struct BboCacheEntry {
    bid_price: f64,
    bid_amount: f64,
    ask_price: f64,
    ask_amount: f64,
    seeded: bool,
}

pub struct BybitAdapter {
    venue: TradingVenue,
    cache: RefCell<BybitBboCache>,
}

impl BybitAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        Self {
            venue,
            cache: RefCell::new(BybitBboCache::with_capacity(2048)),
        }
    }
}

struct BybitBboCache {
    index_by_symbol: FastHashMap<String, usize>,
    entries: Vec<BboCacheEntry>,
}

impl BybitBboCache {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            index_by_symbol: fast_hash_map_with_capacity(capacity),
            entries: Vec::with_capacity(capacity),
        }
    }

    fn seed_symbols(&mut self, symbols: &[String]) {
        for symbol in symbols {
            self.ensure_symbol(symbol);
        }
    }

    fn ensure_symbol(&mut self, symbol: &str) -> usize {
        if let Some(&idx) = self.index_by_symbol.get(symbol) {
            return idx;
        }
        let idx = self.entries.len();
        self.index_by_symbol.insert(symbol.to_string(), idx);
        self.entries.push(BboCacheEntry::default());
        idx
    }

    fn entry_mut(&mut self, symbol: &str) -> &mut BboCacheEntry {
        let idx = self.ensure_symbol(symbol);
        &mut self.entries[idx]
    }
}

impl VenueAdapter for BybitAdapter {
    fn name(&self) -> &'static str {
        "bybit"
    }

    fn ws_url(&self) -> String {
        match self.venue {
            TradingVenue::BybitMargin => BYBIT_SPOT_WS_URL.to_string(),
            TradingVenue::BybitFutures => BYBIT_LINEAR_WS_URL.to_string(),
            other => unreachable!("BybitAdapter created with non-bybit venue: {:?}", other),
        }
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_bybit_subscribe(self.venue, symbols, "orderbook.1")
    }

    fn build_trade_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_bybit_subscribe(self.venue, symbols, "publicTrade")
    }

    fn build_incremental_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_bybit_subscribe(self.venue, symbols, "orderbook.1000")
    }

    fn build_derivatives_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        if self.venue != TradingVenue::BybitFutures {
            return Vec::new();
        }
        let mut out = build_bybit_subscribe(self.venue, symbols, "tickers");
        out.extend(build_bybit_subscribe(self.venue, symbols, "allLiquidation"));
        out
    }

    fn derivatives_ws_url(&self) -> Option<String> {
        if self.venue == TradingVenue::BybitFutures {
            Some(BYBIT_LINEAR_WS_URL.to_string())
        } else {
            None
        }
    }

    fn seed_symbols(&self, symbols: &[String]) {
        self.cache.borrow_mut().seed_symbols(symbols);
    }

    fn parse_trade_frame(&self, value: &Value) -> Result<Vec<TradeFrame>> {
        parse_trade_frame(value)
    }

    fn parse_incremental_frame(&self, value: &Value) -> Result<Vec<IncrementalFrame>> {
        parse_incremental_frame(value)
    }

    fn parse_derivatives_frame(&self, value: &Value) -> Result<Vec<Bytes>> {
        parse_derivatives_frame(value)
    }

    fn parse_frame(
        &self,
        value: &Value,
        emit: &mut dyn FnMut(BboFrame) -> Result<()>,
    ) -> Result<()> {
        parse_bbo_frame(value, &self.cache, emit)
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        Some(KeepaliveSpec::text(
            Duration::from_secs(20),
            r#"{"op":"ping"}"#,
        ))
    }
}

fn build_bybit_subscribe(venue: TradingVenue, symbols: &[String], channel: &str) -> Vec<Value> {
    let chunk_size = match venue {
        TradingVenue::BybitMargin => BYBIT_SPOT_SUBSCRIBE_CHUNK,
        _ => BYBIT_LINEAR_SUBSCRIBE_CHUNK,
    }
    .max(1);

    let mut out = Vec::new();
    for chunk in symbols.chunks(chunk_size) {
        let args: Vec<String> = chunk
            .iter()
            .map(|sym| format!("{}.{}", channel, sym.to_ascii_uppercase()))
            .collect();
        out.push(serde_json::json!({
            "op": "subscribe",
            "args": args,
        }));
    }
    out
}

fn parse_bbo_frame(
    value: &Value,
    cache: &RefCell<BybitBboCache>,
    emit: &mut dyn FnMut(BboFrame) -> Result<()>,
) -> Result<()> {
    let topic = match value.get("topic").and_then(|v| v.as_str()) {
        Some(topic) if topic.starts_with("orderbook.1.") => topic,
        _ => return Ok(()),
    };
    let data = value
        .get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("bybit {} missing data object", topic))?;
    if !data.contains_key("u") {
        return Err(anyhow!("bybit {} missing data.u (updateId)", topic));
    }
    let Some(update) = bybit_codec::parse_bbo_update_json(value) else {
        return Ok(());
    };
    if update.seq_id == 0 {
        return Ok(());
    }

    let mut cache = cache.borrow_mut();
    let entry = cache.entry_mut(&update.symbol);

    if update.reset_seq {
        *entry = BboCacheEntry::default();
    }

    if let Some(bid) = update.bid {
        entry.bid_price = bid.price;
        entry.bid_amount = bid.amount;
    }
    if let Some(ask) = update.ask {
        entry.ask_price = ask.price;
        entry.ask_amount = ask.amount;
    }

    if update.reset_seq {
        // snapshot 必须同时给齐两侧
        if entry.bid_price > 0.0 && entry.ask_price > 0.0 {
            entry.seeded = true;
        }
    }

    if !entry.seeded {
        return Ok(());
    }
    if entry.bid_price <= 0.0
        || entry.ask_price <= 0.0
        || entry.bid_amount <= 0.0
        || entry.ask_amount <= 0.0
    {
        return Ok(());
    }

    emit(BboFrame {
        symbol: update.symbol,
        ts_us: update.timestamp_us,
        seq_id: update.seq_id,
        reset_seq: update.reset_seq,
        bid_price: entry.bid_price,
        bid_amount: entry.bid_amount,
        ask_price: entry.ask_price,
        ask_amount: entry.ask_amount,
    })?;
    Ok(())
}

fn parse_trade_frame(value: &Value) -> Result<Vec<TradeFrame>> {
    Ok(bybit_codec::parse_trades_json(value)
        .into_iter()
        .map(|t| TradeFrame {
            symbol: t.symbol,
            timestamp_us: t.timestamp_us,
            seq_id: t.seq_id,
            trade_id: t.trade_id,
            side: t.side,
            price: t.price,
            amount: t.amount,
        })
        .collect())
}

fn parse_incremental_frame(value: &Value) -> Result<Vec<IncrementalFrame>> {
    let Some(book) = bybit_codec::parse_incremental_json(value) else {
        return Ok(Vec::new());
    };
    Ok(vec![IncrementalFrame::Book {
        symbol: book.symbol,
        timestamp: book.timestamp_us,
        seq_id: book.seq_id,
        prev_seq_id: book.prev_seq_id,
        first_update_id: book.first_update_id,
        final_update_id: book.final_update_id,
        gap_check: book.gap_check,
        is_snapshot: book.is_snapshot,
        bids: book_levels_to_msg(book.bids),
        asks: book_levels_to_msg(book.asks),
    }])
}

fn parse_derivatives_frame(value: &Value) -> Result<Vec<Bytes>> {
    let mut out = Vec::new();
    for derivative in bybit_codec::parse_derivatives_json(value) {
        out.push(derivative_to_bytes(derivative));
    }
    Ok(out)
}

fn derivative_to_bytes(derivative: bybit_codec::Derivative) -> Bytes {
    match derivative {
        bybit_codec::Derivative::MarkPrice {
            symbol,
            price,
            timestamp_us,
        } => MarkPriceMsg::create(symbol, price, timestamp_us).to_bytes(),
        bybit_codec::Derivative::IndexPrice {
            symbol,
            price,
            timestamp_us,
        } => IndexPriceMsg::create(symbol, price, timestamp_us).to_bytes(),
        bybit_codec::Derivative::FundingRate {
            symbol,
            funding_rate,
            next_funding_time_us,
            timestamp_us,
        } => FundingRateMsg::create(symbol, funding_rate, next_funding_time_us, timestamp_us)
            .to_bytes(),
        bybit_codec::Derivative::Liquidation {
            symbol,
            side,
            amount,
            price,
            timestamp_us,
        } => LiquidationMsg::create(symbol, side, amount, price, timestamp_us).to_bytes(),
    }
}

fn book_levels_to_msg(levels: Vec<bybit_codec::Level>) -> Vec<Level> {
    levels
        .into_iter()
        .map(|level| Level::from_values(level.price, level.amount))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::{get_msg_type, MktMsgType};

    fn v(raw: &str) -> Value {
        serde_json::from_str(raw).expect("test fixture must be valid JSON")
    }

    fn liquidation_timestamp(data: &[u8]) -> i64 {
        let symbol_length = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let offset = 8 + symbol_length + 1 + 8 + 8;
        i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
    }

    #[test]
    fn snapshot_seeds_cache_and_emits_full_bbo() {
        let raw = r#"{
            "topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000000,
            "data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","2"]],"u":1}
        }"#;
        let a = BybitAdapter::new(TradingVenue::BybitFutures);
        let frames = a.collect_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.seq_id, 1);
        assert_eq!(f.ts_us, 1_700_000_000_000_000);
        assert!((f.bid_price - 100.0).abs() < 1e-9);
        assert!((f.ask_price - 101.0).abs() < 1e-9);
    }

    #[test]
    fn delta_before_snapshot_is_dropped() {
        let raw = r#"{
            "topic":"orderbook.1.BTCUSDT","type":"delta","ts":1700000000000,
            "data":{"s":"BTCUSDT","b":[["100","1"]],"a":[],"u":1}
        }"#;
        let a = BybitAdapter::new(TradingVenue::BybitFutures);
        assert!(a.collect_frame(&v(raw)).unwrap().is_empty());
    }

    #[test]
    fn delta_after_snapshot_emits_merged_bbo() {
        let a = BybitAdapter::new(TradingVenue::BybitFutures);
        let snap = r#"{
            "topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1,
            "data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","2"]],"u":1}
        }"#;
        a.collect_frame(&v(snap)).unwrap();
        // 只更新 ask 一侧
        let delta = r#"{
            "topic":"orderbook.1.BTCUSDT","type":"delta","ts":2,
            "data":{"s":"BTCUSDT","b":[],"a":[["101.5","3"]],"u":2}
        }"#;
        let frames = a.collect_frame(&v(delta)).unwrap();
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.seq_id, 2);
        assert!((f.bid_price - 100.0).abs() < 1e-9); // 沿用 cache
        assert!((f.ask_price - 101.5).abs() < 1e-9);
        assert!((f.ask_amount - 3.0).abs() < 1e-9);
    }

    #[test]
    fn missing_u_field_is_an_error() {
        let raw = r#"{
            "topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1,
            "data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","2"]]}
        }"#;
        let a = BybitAdapter::new(TradingVenue::BybitFutures);
        assert!(a.collect_frame(&v(raw)).is_err());
    }

    #[test]
    fn build_subscribe_chunks_300_for_linear() {
        let a = BybitAdapter::new(TradingVenue::BybitFutures);
        let symbols: Vec<String> = (0..650).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["args"].as_array().unwrap().len(), 300);
        assert_eq!(msgs[2]["args"].as_array().unwrap().len(), 50);
        let first_arg = msgs[0]["args"][0].as_str().unwrap();
        assert!(first_arg.starts_with("orderbook.1."));
    }

    #[test]
    fn build_subscribe_chunks_10_for_spot() {
        let a = BybitAdapter::new(TradingVenue::BybitMargin);
        let symbols: Vec<String> = (0..313).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        // V5 spot 每个 op:subscribe 最多 10 args；313 → 32 批（最后一批 3）
        assert_eq!(msgs.len(), 32);
        assert_eq!(msgs[0]["args"].as_array().unwrap().len(), 10);
        assert_eq!(msgs[31]["args"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn build_subscribe_spot_short_list_single_batch() {
        let a = BybitAdapter::new(TradingVenue::BybitMargin);
        let symbols: Vec<String> = (0..7).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["args"].as_array().unwrap().len(), 7);
    }

    #[test]
    fn decodes_public_trade_directly_with_microsecond_ts() {
        let raw = r#"{
            "topic":"publicTrade.BTCUSDT",
            "data":[
                {"T":1700000000123,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100.5","i":"9001","seq":77},
                {"T":1700000000124,"s":"BTCUSDT","S":"Sell","v":"0.2","p":"100.6","i":"11111111-2222-3333-4444-555555556666","seq":77}
            ]
        }"#;
        let frames = parse_trade_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].symbol, "BTCUSDT");
        assert_eq!(frames[0].timestamp_us, 1_700_000_000_123_000);
        assert_eq!(frames[0].trade_id, 9001);
        assert_eq!(frames[0].seq_id, 77_000_000);
        assert_eq!(frames[0].side, 'B');
        assert_eq!(frames[1].timestamp_us, 1_700_000_000_124_000);
        assert_ne!(frames[0].seq_id, frames[1].seq_id);
        assert_eq!(frames[1].side, 'S');
    }

    #[test]
    fn decodes_incremental_orderbook_directly_with_microsecond_ts() {
        let raw = r#"{
            "topic":"orderbook.1000.BTCUSDT","type":"snapshot","ts":1700000000999,"cts":1700000000123,
            "data":{"s":"BTCUSDT","b":[["100","1"],["99","2"]],"a":[["101","3"]],"u":12345,"seq":9}
        }"#;
        let frames = parse_incremental_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        let IncrementalFrame::Book {
            symbol,
            timestamp,
            seq_id,
            first_update_id,
            final_update_id,
            gap_check,
            is_snapshot,
            bids,
            asks,
            ..
        } = &frames[0]
        else {
            panic!("expected orderbook book frame");
        };
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(*timestamp, 1_700_000_000_123_000);
        assert_eq!(*seq_id, 12345);
        assert_eq!(*first_update_id, 12345);
        assert_eq!(*final_update_id, 12345);
        assert!(!*gap_check);
        assert!(*is_snapshot);
        assert_eq!(bids.len(), 2);
        assert_eq!(asks.len(), 1);
        assert!((bids[0].price - 100.0).abs() < 1e-9);
        assert!((asks[0].amount - 3.0).abs() < 1e-9);
    }

    #[test]
    fn decodes_derivatives_ticker_and_liquidation_with_microsecond_ts() {
        let ticker = r#"{
            "topic":"tickers.BTCUSDT","type":"snapshot","ts":1700000000123,
            "data":{"symbol":"BTCUSDT","markPrice":"100.1","indexPrice":"99.9","fundingRate":"0.0001","nextFundingTime":"1700003600000"}
        }"#;
        let bytes = parse_derivatives_frame(&v(ticker)).unwrap();
        assert_eq!(bytes.len(), 3);
        assert_eq!(
            MarkPriceMsg::get_timestamp(&bytes[0]),
            1_700_000_000_123_000
        );
        assert_eq!(
            IndexPriceMsg::get_timestamp(&bytes[1]),
            1_700_000_000_123_000
        );
        assert_eq!(
            FundingRateMsg::get_timestamp(&bytes[2]),
            1_700_000_000_123_000
        );
        assert_eq!(
            FundingRateMsg::get_next_funding_time(&bytes[2]),
            1_700_003_600_000_000
        );

        let liquidation = r#"{
            "topic":"allLiquidation.BTCUSDT","ts":1700000000999,
            "data":[{"T":1700000000124,"s":"BTCUSDT","S":"Sell","v":"1.5","p":"98.7"}]
        }"#;
        let bytes = parse_derivatives_frame(&v(liquidation)).unwrap();
        assert_eq!(bytes.len(), 1);
        assert_eq!(get_msg_type(&bytes[0]), MktMsgType::LiquidationOrder);
        assert_eq!(liquidation_timestamp(&bytes[0]), 1_700_000_000_124_000);
    }

    #[test]
    fn decodes_delta_funding_without_next_funding_time() {
        let ticker = r#"{
            "topic":"tickers.HOMEUSDT","type":"delta","ts":1700000000456,
            "data":{"symbol":"HOMEUSDT","fundingRate":"-0.00054238","markPrice":"0.0279"}
        }"#;
        let bytes = parse_derivatives_frame(&v(ticker)).unwrap();
        assert_eq!(bytes.len(), 2);
        assert_eq!(MarkPriceMsg::get_symbol(&bytes[0]), "HOMEUSDT");
        assert_eq!(FundingRateMsg::get_symbol(&bytes[1]), "HOMEUSDT");
        assert!((FundingRateMsg::get_funding_rate(&bytes[1]) + 0.00054238).abs() < 1e-12);
        assert_eq!(FundingRateMsg::get_next_funding_time(&bytes[1]), 0);
        assert_eq!(
            FundingRateMsg::get_timestamp(&bytes[1]),
            1_700_000_000_456_000
        );
    }

    #[test]
    fn derivatives_subscribe_is_futures_only() {
        let futures = BybitAdapter::new(TradingVenue::BybitFutures);
        let msgs = futures.build_derivatives_subscribe(&["BTCUSDT".to_string()]);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["args"][0], "tickers.BTCUSDT");
        assert_eq!(msgs[1]["args"][0], "allLiquidation.BTCUSDT");

        let margin = BybitAdapter::new(TradingVenue::BybitMargin);
        assert!(margin
            .build_derivatives_subscribe(&["BTCUSDT".to_string()])
            .is_empty());
    }
}
