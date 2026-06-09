//! Gate `book_ticker` spread 适配器（spot/futures 统一）。
//!
//! - spot:    `wss://api.gateio.ws/ws/v4/`           channel=`spot.book_ticker`
//! - futures: `wss://fx-ws.gateio.ws/v4/ws/usdt`     channel=`futures.book_ticker`
//! - subscribe: `{"time":<unix>,"channel":"<prefix>.book_ticker","event":"subscribe","payload":[...]}`
//! - frame: `{"channel":"<prefix>.book_ticker","event":"update",
//!            "result":{"t":<ms>,"u":<seq>,"s":..,"b":..,"B":..,"a":..,"A":..}}`
//!   注：spot 的 `B`/`A` 已是 string 量级；futures 是数值。我们都按 number-or-string 解析。
//! - 心跳: 每 15s 发 `{"time":<unix>,"channel":"<prefix>.ping"}`，否则 25s 后被服务端断开。

use anyhow::{anyhow, Result};
use bytes::Bytes;
use mkt_parsers::gate as gate_codec;
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::common::mkt_msg::{FundingRateMsg, IndexPriceMsg, MarkPriceMsg};
use order_common::TradingVenue;
use crate::spread_pbs::adapter::{
    BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};

const GATE_SPOT_WS_URL: &str = "wss://api.gateio.ws/ws/v4/";
const GATE_FUTURES_WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
const GATE_SUBSCRIBE_CHUNK: usize = 100;

pub struct GateAdapter {
    venue: TradingVenue,
}

impl GateAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        Self { venue }
    }

    fn channel_prefix(&self) -> &'static str {
        match self.venue {
            TradingVenue::GateMargin => "spot",
            TradingVenue::GateFutures => "futures",
            other => unreachable!("GateAdapter created with non-gate venue: {:?}", other),
        }
    }
}

impl VenueAdapter for GateAdapter {
    fn name(&self) -> &'static str {
        "gate"
    }

    fn ws_url(&self) -> String {
        match self.venue {
            TradingVenue::GateMargin => GATE_SPOT_WS_URL.to_string(),
            TradingVenue::GateFutures => GATE_FUTURES_WS_URL.to_string(),
            other => unreachable!("GateAdapter created with non-gate venue: {:?}", other),
        }
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_channel_subscribe(symbols, "book_ticker")
    }

    fn build_trade_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_channel_subscribe(symbols, "trades")
    }

    fn build_incremental_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_order_book_update_subscribe(symbols)
    }

    fn parse_frame(&self, value: &Value) -> Result<Vec<BboFrame>> {
        let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
        let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
        if event == "update" && channel.ends_with(".book_ticker") {
            if let Some(result) = value.get("result").and_then(|v| v.as_object()) {
                if result.get("u").is_none() {
                    return Err(anyhow!("gate {} missing result.u (updateId)", channel));
                }
            }
        }

        if let Some(bbo) = gate_codec::parse_bbo_json(value) {
            return Ok(vec![bbo_to_frame(bbo)]);
        }

        if event != "update" || !channel.ends_with(".book_ticker") {
            return Ok(Vec::new());
        }
        if let Some(result) = value.get("result").and_then(|v| v.as_object()) {
            if result.get("u").is_none() {
                return Err(anyhow!("gate {} missing result.u (updateId)", channel));
            }
        }
        Ok(Vec::new())
    }

    fn parse_trade_frame(&self, value: &Value) -> Result<Vec<TradeFrame>> {
        Ok(gate_codec::parse_trades_json(value)
            .into_iter()
            .map(trade_to_frame)
            .collect())
    }

    fn parse_incremental_frame(&self, value: &Value) -> Result<Vec<IncrementalFrame>> {
        Ok(gate_codec::parse_incremental_json(value)
            .into_iter()
            .map(book_to_incremental)
            .collect())
    }

    fn parse_derivatives_frame(&self, value: &Value) -> Result<Vec<Bytes>> {
        if self.venue != TradingVenue::GateFutures {
            return Ok(Vec::new());
        }
        Ok(gate_codec::parse_derivatives_json(value)
            .into_iter()
            .map(derivative_to_bytes)
            .collect())
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        let prefix = self.channel_prefix();
        let channel = format!("{}.ping", prefix);
        Some(KeepaliveSpec::dynamic(Duration::from_secs(15), move || {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let body = serde_json::json!({
                "time": timestamp,
                "channel": channel.clone(),
            });
            Message::Text(body.to_string())
        }))
    }
}

impl GateAdapter {
    fn build_channel_subscribe(&self, symbols: &[String], channel: &str) -> Vec<Value> {
        let chunk_size = GATE_SUBSCRIBE_CHUNK.max(1);
        let channel = format!("{}.{}", self.channel_prefix(), channel);
        let mut out = Vec::new();
        for chunk in symbols.chunks(chunk_size) {
            let payload: Vec<String> = chunk.to_vec();
            let timestamp = now_unix_secs();
            out.push(serde_json::json!({
                "time": timestamp,
                "channel": channel,
                "event": "subscribe",
                "payload": payload,
            }));
        }
        out
    }

    fn build_order_book_update_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        let channel = format!("{}.order_book_update", self.channel_prefix());
        let mut out = Vec::new();
        for symbol in symbols {
            let payload = match self.venue {
                TradingVenue::GateMargin => serde_json::json!([symbol.as_str(), "100ms"]),
                TradingVenue::GateFutures => serde_json::json!([symbol.as_str(), "100ms", "100"]),
                _ => unreachable!(),
            };
            out.push(serde_json::json!({
                "time": now_unix_secs(),
                "channel": channel,
                "event": "subscribe",
                "payload": payload,
            }));
        }
        out
    }
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub(crate) fn bbo_to_frame(bbo: gate_codec::Bbo) -> BboFrame {
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

pub(crate) fn trade_to_frame(trade: gate_codec::Trade) -> TradeFrame {
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

pub(crate) fn book_to_incremental(book: gate_codec::Book) -> IncrementalFrame {
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
            .map(|level| crate::common::mkt_msg::Level::from_values(level.price, level.amount))
            .collect(),
        asks: book
            .asks
            .into_iter()
            .map(|level| crate::common::mkt_msg::Level::from_values(level.price, level.amount))
            .collect(),
    }
}

pub(crate) fn derivative_to_bytes(derivative: gate_codec::Derivative) -> Bytes {
    match derivative {
        gate_codec::Derivative::MarkPrice {
            symbol,
            price,
            timestamp_us,
        } => MarkPriceMsg::create(symbol, price, timestamp_us).to_bytes(),
        gate_codec::Derivative::IndexPrice {
            symbol,
            price,
            timestamp_us,
        } => IndexPriceMsg::create(symbol, price, timestamp_us).to_bytes(),
        gate_codec::Derivative::FundingRate {
            symbol,
            funding_rate,
            next_funding_time_us,
            timestamp_us,
        } => FundingRateMsg::create(symbol, funding_rate, next_funding_time_us, timestamp_us)
            .to_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(raw: &str) -> Value {
        serde_json::from_str(raw).expect("test fixture must be valid JSON")
    }

    #[test]
    fn parses_futures_book_ticker_string_fields() {
        let raw = r#"{
            "time":1606293803,"time_ms":1606293803097,
            "channel":"futures.book_ticker","event":"update",
            "result":{"t":1606293803097,"u":48733182,"s":"BTC_USDT",
                      "b":"19177.79","B":"11","a":"19178.4","A":"1"}
        }"#;
        let a = GateAdapter::new(TradingVenue::GateFutures);
        let frames = a.parse_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.seq_id, 48733182);
        assert_eq!(f.ts_us, 1606293803097 * 1000);
        assert!((f.bid_price - 19177.79).abs() < 1e-6);
        assert!((f.ask_amount - 1.0).abs() < 1e-9);
    }

    #[test]
    fn parses_spot_book_ticker() {
        let raw = r#"{
            "time":1700000000,"time_ms":1700000000123,
            "channel":"spot.book_ticker","event":"update",
            "result":{"t":1700000000123,"u":111,"s":"ETH_USDT",
                      "b":"3000","B":"0.5","a":"3001","A":"1.0"}
        }"#;
        let a = GateAdapter::new(TradingVenue::GateMargin);
        let frames = a.parse_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].symbol, "ETHUSDT");
        assert_eq!(frames[0].seq_id, 111);
    }

    #[test]
    fn missing_u_is_an_error() {
        let raw = r#"{
            "channel":"futures.book_ticker","event":"update",
            "result":{"t":1,"s":"BTC_USDT","b":"1","B":"1","a":"2","A":"1"}
        }"#;
        let a = GateAdapter::new(TradingVenue::GateFutures);
        assert!(a.parse_frame(&v(raw)).is_err());
    }

    #[test]
    fn ignores_non_update_events() {
        let raw = r#"{
            "channel":"futures.book_ticker","event":"subscribe","result":{"status":"success"}
        }"#;
        let a = GateAdapter::new(TradingVenue::GateFutures);
        assert!(a.parse_frame(&v(raw)).unwrap().is_empty());
    }

    #[test]
    fn build_subscribe_chunks() {
        let a = GateAdapter::new(TradingVenue::GateFutures);
        let symbols: Vec<String> = (0..220).map(|i| format!("SYM{}_USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["channel"], "futures.book_ticker");
        assert_eq!(msgs[0]["event"], "subscribe");
        assert_eq!(msgs[0]["payload"].as_array().unwrap().len(), 100);
        assert_eq!(msgs[2]["payload"].as_array().unwrap().len(), 20);
    }

    #[test]
    fn parses_spot_trade_direct() {
        let raw = r#"{
            "time":1764559450,"time_ms":1764559450123,
            "channel":"spot.trades","event":"update",
            "result":{"id":123456789,"create_time_ms":"1764559450123",
                      "currency_pair":"BTC_USDT","side":"sell",
                      "amount":"0.12","price":"86500.5"}
        }"#;
        let a = GateAdapter::new(TradingVenue::GateMargin);
        let frames = a.parse_trade_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTC_USDT");
        assert_eq!(f.trade_id, 123456789);
        assert_eq!(f.timestamp_us, 1764559450123 * 1000);
        assert_eq!(f.side, 'S');
        assert!((f.amount - 0.12).abs() < 1e-12);
    }

    #[test]
    fn parses_futures_incremental_direct() {
        let raw = r#"{
            "time":1764559550,"time_ms":1764559550999,
            "channel":"futures.order_book_update","event":"update",
            "result":{"s":"BTC_USDT","U":100,"u":101,"t":1764559550999,
                      "b":[["86499.5","3"]],"a":[["86500.5","1"]]}
        }"#;
        let a = GateAdapter::new(TradingVenue::GateFutures);
        let frames = a.parse_incremental_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            IncrementalFrame::Book {
                symbol,
                timestamp,
                seq_id,
                prev_seq_id,
                first_update_id,
                final_update_id,
                gap_check,
                bids,
                asks,
                ..
            } => {
                assert_eq!(symbol, "BTC_USDT");
                assert_eq!(*timestamp, 1_764_559_550_999_000);
                assert_eq!(*seq_id, 101);
                assert_eq!(*prev_seq_id, i64::MIN);
                assert_eq!(*first_update_id, 100);
                assert_eq!(*final_update_id, 101);
                assert!(!gap_check);
                assert_eq!(bids.len(), 1);
                assert_eq!(asks.len(), 1);
            }
            _ => panic!("expected book frame"),
        }
    }
}
