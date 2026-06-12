//! Binance spread 适配器。
//!
//! Binance margin/futures 支持单边或 `binance-both` 运行，都会发布完整
//! BBO/trade/incremental/derivatives replacement。协议字段解码集中在 `mkt_parsers`，本文件只做
//! 订阅、venue gating 和 spread_pbs 内部 frame/bytes 转换。

use anyhow::{anyhow, Result};
use bytes::Bytes;
use mkt_parsers::binance as binance_codec;
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashSet;

use crate::spread_pbs::adapter::{
    BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};
use mkt_parsers::msg::mkt_msg::{FundingRateMsg, IndexPriceMsg, LiquidationMsg, MarkPriceMsg};
use order_common::TradingVenue;

const BINANCE_SPOT_SBE_WS_URL: &str = "wss://stream-sbe.binance.com:9443/ws";
const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/public/stream";
const BINANCE_FUTURES_DERIVATIVES_WS_URL: &str = "wss://fstream.binance.com/market/ws";
const BINANCE_SUBSCRIBE_CHUNK: usize = 200;

pub struct BinanceAdapter {
    venue: TradingVenue,
    derivatives_symbols: RefCell<HashSet<String>>,
}

impl BinanceAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        Self {
            venue,
            derivatives_symbols: RefCell::new(HashSet::new()),
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
            TradingVenue::BinanceFutures => BINANCE_FUTURES_WS_URL.to_string(),
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
        let channel = match self.venue {
            TradingVenue::BinanceFutures => "depth5@0ms",
            TradingVenue::BinanceMargin => "bestBidAsk",
            other => unreachable!("BinanceAdapter created with non-binance venue: {:?}", other),
        };
        build_stream_subscribe(symbols, channel)
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
            Some(BINANCE_FUTURES_DERIVATIVES_WS_URL.to_string())
        } else {
            None
        }
    }

    fn seed_symbols(&self, symbols: &[String]) {
        let mut active = self.derivatives_symbols.borrow_mut();
        active.clear();
        active.extend(symbols.iter().map(|symbol| symbol.to_ascii_uppercase()));
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
        let active = self.derivatives_symbols.borrow();
        Ok(binance_codec::parse_derivatives_json(value)
            .into_iter()
            .filter(|derivative| {
                active.is_empty() || active.contains(derivative_symbol(derivative))
            })
            .map(derivative_to_bytes)
            .collect())
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

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::{get_msg_type, MktMsgType};

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
    fn missing_u_field_is_an_error() {
        let raw = r#"{"data":{"s":"BTCUSDT","b":"25","B":"1","a":"25.1","A":"1"}}"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        assert!(a.collect_frame(&v(raw)).is_err());
    }

    #[test]
    fn subscribe_chunks() {
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let symbols: Vec<String> = (0..450).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["params"].as_array().unwrap().len(), 200);
        assert_eq!(msgs[2]["params"].as_array().unwrap().len(), 50);
        assert_eq!(msgs[0]["params"][0], "sym0usdt@depth5@0ms");
    }

    #[test]
    fn full_replacement_subscriptions_use_derivatives_market_url() {
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
