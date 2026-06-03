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
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

use crate::common::mkt_msg::{FundingRateMsg, IndexPriceMsg, Level, LiquidationMsg, MarkPriceMsg};
use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{
    BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};

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
    index_by_symbol: HashMap<String, usize>,
    entries: Vec<BboCacheEntry>,
}

impl BybitBboCache {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            index_by_symbol: HashMap::with_capacity(capacity),
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

    fn parse_frame(&self, value: &Value) -> Result<Vec<BboFrame>> {
        parse_bbo_frame(value, &self.cache)
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

fn parse_bbo_frame(value: &Value, cache: &RefCell<BybitBboCache>) -> Result<Vec<BboFrame>> {
    let topic = match value.get("topic").and_then(|v| v.as_str()) {
        Some(t) if t.starts_with("orderbook.1.") => t,
        _ => return Ok(Vec::new()),
    };
    let push_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let data = value
        .get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("bybit {} missing data object", topic))?;
    let symbol = data
        .get("s")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
        .ok_or_else(|| anyhow!("bybit {} missing data.s", topic))?;
    let seq_id = data
        .get("u")
        .and_then(parse_i64_loose)
        .ok_or_else(|| anyhow!("bybit {} missing data.u (updateId)", topic))?;
    let ts_us = value
        .get("ts")
        .and_then(parse_i64_loose)
        .map(normalize_ts_to_us)
        .unwrap_or(0);

    let bid_levels = data.get("b").and_then(|v| v.as_array());
    let ask_levels = data.get("a").and_then(|v| v.as_array());

    let mut cache = cache.borrow_mut();
    let entry = cache.entry_mut(&symbol);

    if push_type == "snapshot" {
        *entry = BboCacheEntry::default();
    }

    if let Some(arr) = bid_levels {
        if let Some((p, a)) = pick_top_level(arr, &symbol, "b")? {
            entry.bid_price = p;
            entry.bid_amount = a;
        }
    }
    if let Some(arr) = ask_levels {
        if let Some((p, a)) = pick_top_level(arr, &symbol, "a")? {
            entry.ask_price = p;
            entry.ask_amount = a;
        }
    }

    if push_type == "snapshot" {
        // snapshot 必须同时给齐两侧
        if entry.bid_price > 0.0 && entry.ask_price > 0.0 {
            entry.seeded = true;
        }
    }

    if !entry.seeded {
        return Ok(Vec::new());
    }
    if entry.bid_price <= 0.0
        || entry.ask_price <= 0.0
        || entry.bid_amount <= 0.0
        || entry.ask_amount <= 0.0
    {
        return Ok(Vec::new());
    }

    Ok(vec![BboFrame {
        symbol,
        ts_us,
        seq_id,
        reset_seq: push_type == "snapshot",
        bid_price: entry.bid_price,
        bid_amount: entry.bid_amount,
        ask_price: entry.ask_price,
        ask_amount: entry.ask_amount,
    }])
}

/// 取 `[[price, size], ...]` 形式数组的 top 一档；空数组返回 None；解析失败返回 Err。
fn pick_top_level(arr: &[Value], symbol: &str, side: &str) -> Result<Option<(f64, f64)>> {
    let Some(level) = arr.first() else {
        return Ok(None);
    };
    let level = level
        .as_array()
        .ok_or_else(|| anyhow!("bybit {} {} top level is not an array", symbol, side))?;
    if level.len() < 2 {
        return Err(anyhow!(
            "bybit {} {} top level needs [price,size]",
            symbol,
            side
        ));
    }
    let price = level[0]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow!("bybit {} {} top price invalid", symbol, side))?;
    let amount = level[1]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow!("bybit {} {} top amount invalid", symbol, side))?;
    Ok(Some((price, amount)))
}

fn parse_trade_frame(value: &Value) -> Result<Vec<TradeFrame>> {
    let topic = match value.get("topic").and_then(|v| v.as_str()) {
        Some(topic) if topic.starts_with("publicTrade.") => topic,
        _ => return Ok(Vec::new()),
    };
    let data = match value.get("data").and_then(|v| v.as_array()) {
        Some(data) => data,
        None => return Ok(Vec::new()),
    };

    let mut out = Vec::with_capacity(data.len());
    for (idx, item) in data.iter().enumerate() {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let symbol = obj
            .get("s")
            .and_then(|v| v.as_str())
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_else(|| topic.rsplit('.').next().unwrap_or("").to_ascii_uppercase());
        if symbol.is_empty() {
            continue;
        }
        let side = match obj.get("S").and_then(|v| v.as_str()).unwrap_or("") {
            "Buy" => 'B',
            "Sell" => 'S',
            _ => continue,
        };
        let Some(price) = obj.get("p").and_then(parse_f64_loose) else {
            continue;
        };
        let Some(amount) = obj.get("v").and_then(parse_f64_loose) else {
            continue;
        };
        if price <= 0.0 || amount <= 0.0 {
            continue;
        }
        let Some(raw_ts) = obj.get("T").and_then(parse_i64_loose) else {
            continue;
        };
        let Some(id_raw) = obj.get("i").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(trade_id) = parse_trade_id(id_raw) else {
            continue;
        };
        let seq_base = obj.get("seq").and_then(parse_i64_loose).unwrap_or(trade_id);
        let seq_id = seq_base
            .saturating_mul(1_000_000)
            .saturating_add(idx as i64);
        out.push(TradeFrame {
            symbol,
            timestamp_us: normalize_ts_to_us(raw_ts),
            seq_id,
            trade_id,
            side,
            price,
            amount,
        });
    }
    Ok(out)
}

fn parse_incremental_frame(value: &Value) -> Result<Vec<IncrementalFrame>> {
    let topic = match value.get("topic").and_then(|v| v.as_str()) {
        Some(topic) if topic.starts_with("orderbook.") && !topic.starts_with("orderbook.1.") => {
            topic
        }
        _ => return Ok(Vec::new()),
    };
    let push_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let is_snapshot = match push_type {
        "snapshot" => true,
        "delta" => false,
        _ => return Ok(Vec::new()),
    };
    let data = match value.get("data").and_then(|v| v.as_object()) {
        Some(data) => data,
        None => return Ok(Vec::new()),
    };
    let symbol = data
        .get("s")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
        .unwrap_or_else(|| topic.rsplit('.').next().unwrap_or("").to_ascii_uppercase());
    if symbol.is_empty() {
        return Ok(Vec::new());
    }
    let seq_id = data
        .get("u")
        .and_then(parse_i64_loose)
        .ok_or_else(|| anyhow!("bybit {} missing data.u", topic))?;
    let timestamp = value
        .get("cts")
        .and_then(parse_i64_loose)
        .or_else(|| value.get("ts").and_then(parse_i64_loose))
        .map(normalize_ts_to_us)
        .unwrap_or(0);
    let bids = data
        .get("b")
        .and_then(|v| v.as_array())
        .map(|levels| parse_level_array(levels))
        .unwrap_or_default();
    let asks = data
        .get("a")
        .and_then(|v| v.as_array())
        .map(|levels| parse_level_array(levels))
        .unwrap_or_default();
    if bids.is_empty() && asks.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![IncrementalFrame::Book {
        symbol,
        timestamp,
        seq_id,
        prev_seq_id: i64::MIN,
        first_update_id: seq_id,
        final_update_id: seq_id,
        gap_check: false,
        is_snapshot,
        bids,
        asks,
    }])
}

fn parse_derivatives_frame(value: &Value) -> Result<Vec<Bytes>> {
    let topic = match value.get("topic").and_then(|v| v.as_str()) {
        Some(topic) => topic,
        None => return Ok(Vec::new()),
    };
    if topic.starts_with("tickers.") {
        return parse_ticker_derivatives(value);
    }
    if topic.starts_with("allLiquidation.") {
        return parse_liquidation_derivatives(value);
    }
    Ok(Vec::new())
}

fn parse_ticker_derivatives(value: &Value) -> Result<Vec<Bytes>> {
    let data = match value.get("data").and_then(|v| v.as_object()) {
        Some(data) => data,
        None => return Ok(Vec::new()),
    };
    let symbol = match data.get("symbol").and_then(|v| v.as_str()) {
        Some(symbol) => symbol.to_ascii_uppercase(),
        None => return Ok(Vec::new()),
    };
    let timestamp = value
        .get("ts")
        .and_then(parse_i64_loose)
        .map(normalize_ts_to_us)
        .unwrap_or(0);
    let mut out = Vec::new();
    if let Some(mark_price) = data.get("markPrice").and_then(parse_f64_loose) {
        if mark_price > 0.0 {
            out.push(MarkPriceMsg::create(symbol.clone(), mark_price, timestamp).to_bytes());
        }
    }
    if let Some(index_price) = data.get("indexPrice").and_then(parse_f64_loose) {
        if index_price > 0.0 {
            out.push(IndexPriceMsg::create(symbol.clone(), index_price, timestamp).to_bytes());
        }
    }
    if let (Some(funding_rate), Some(next_funding_time)) = (
        data.get("fundingRate").and_then(parse_f64_loose),
        data.get("nextFundingTime")
            .and_then(parse_i64_loose)
            .map(normalize_ts_to_us),
    ) {
        out.push(
            FundingRateMsg::create(symbol, funding_rate, next_funding_time, timestamp).to_bytes(),
        );
    }
    Ok(out)
}

fn parse_liquidation_derivatives(value: &Value) -> Result<Vec<Bytes>> {
    let data = match value.get("data").and_then(|v| v.as_array()) {
        Some(data) => data,
        None => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for item in data {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(symbol) = obj
            .get("s")
            .and_then(|v| v.as_str())
            .map(|s| s.to_ascii_uppercase())
        else {
            continue;
        };
        let side = match obj.get("S").and_then(|v| v.as_str()).unwrap_or("") {
            "Buy" => 'B',
            "Sell" => 'S',
            _ => continue,
        };
        let Some(volume) = obj.get("v").and_then(parse_f64_loose) else {
            continue;
        };
        let Some(price) = obj.get("p").and_then(parse_f64_loose) else {
            continue;
        };
        let Some(timestamp) = obj
            .get("T")
            .and_then(parse_i64_loose)
            .map(normalize_ts_to_us)
        else {
            continue;
        };
        out.push(LiquidationMsg::create(symbol, side, volume, price, timestamp).to_bytes());
    }
    Ok(out)
}

fn parse_level_array(levels: &[Value]) -> Vec<Level> {
    levels.iter().filter_map(parse_level).collect()
}

fn parse_level(value: &Value) -> Option<Level> {
    let arr = value.as_array()?;
    if arr.len() < 2 {
        return None;
    }
    let price = parse_f64_loose(&arr[0])?;
    let amount = parse_f64_loose(&arr[1])?;
    if price > 0.0 {
        Some(Level::from_values(price, amount))
    } else {
        None
    }
}

fn parse_f64_loose(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    v.as_str().and_then(|s| s.parse::<f64>().ok())
}

fn parse_i64_loose(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return i64::try_from(n).ok();
    }
    if let Some(f) = v.as_f64() {
        return Some(f as i64);
    }
    v.as_str().and_then(|s| {
        s.parse::<i64>()
            .ok()
            .or_else(|| s.parse::<f64>().ok().map(|f| f as i64))
    })
}

fn normalize_ts_to_us(timestamp: i64) -> i64 {
    let abs = timestamp.abs();
    if abs >= 1_000_000_000_000_000_000 {
        timestamp / 1000
    } else if abs >= 1_000_000_000_000_000 {
        timestamp
    } else if abs >= 1_000_000_000_000 {
        timestamp.saturating_mul(1000)
    } else {
        timestamp.saturating_mul(1_000_000)
    }
}

fn parse_trade_id(id: &str) -> Option<i64> {
    if is_uuid_fast(id) {
        uuid_to_int64_mixed(id).ok()
    } else if id.chars().all(|c| c.is_ascii_digit()) {
        id.parse::<i64>().ok()
    } else {
        None
    }
}

fn is_uuid_fast(s: &str) -> bool {
    s.len() == 36
        && s.as_bytes().get(8) == Some(&b'-')
        && s.as_bytes().get(13) == Some(&b'-')
        && s.as_bytes().get(18) == Some(&b'-')
        && s.as_bytes().get(23) == Some(&b'-')
}

fn uuid_to_int64_mixed(uuid: &str) -> Result<i64> {
    let high = i64::from_str_radix(&uuid[0..8], 16)?;
    let low = i64::from_str_radix(&uuid[24..32], 16)?;
    Ok(high ^ low)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::mkt_msg::{get_msg_type, MktMsgType};

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
        let frames = a.parse_frame(&v(raw)).unwrap();
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
        assert!(a.parse_frame(&v(raw)).unwrap().is_empty());
    }

    #[test]
    fn delta_after_snapshot_emits_merged_bbo() {
        let a = BybitAdapter::new(TradingVenue::BybitFutures);
        let snap = r#"{
            "topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1,
            "data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","2"]],"u":1}
        }"#;
        a.parse_frame(&v(snap)).unwrap();
        // 只更新 ask 一侧
        let delta = r#"{
            "topic":"orderbook.1.BTCUSDT","type":"delta","ts":2,
            "data":{"s":"BTCUSDT","b":[],"a":[["101.5","3"]],"u":2}
        }"#;
        let frames = a.parse_frame(&v(delta)).unwrap();
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
        assert!(a.parse_frame(&v(raw)).is_err());
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
