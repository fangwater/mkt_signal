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
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::common::mkt_msg::Level;
use crate::signal::common::TradingVenue;
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
        if event != "update" || !channel.ends_with(".book_ticker") {
            return Ok(Vec::new());
        }

        let res = value
            .get("result")
            .and_then(|v| v.as_object())
            .ok_or_else(|| anyhow!("gate {} missing result object", channel))?;

        // spot: `s`，futures: `s` 也有；个别老接口用 `contract`/`currency_pair`，做兜底
        let symbol = res
            .get("s")
            .and_then(|v| v.as_str())
            .or_else(|| res.get("contract").and_then(|v| v.as_str()))
            .or_else(|| res.get("currency_pair").and_then(|v| v.as_str()))
            .map(|s| s.replace('_', "").to_ascii_uppercase())
            .ok_or_else(|| anyhow!("gate {} missing result.s", channel))?;

        let seq_id = res
            .get("u")
            .and_then(parse_i64_loose)
            .ok_or_else(|| anyhow!("gate {} missing result.u (updateId)", channel))?;

        // result.t / time_ms 都是 ms；time 是秒。统一升精度到 µs（time 升 *1_000_000）
        let ts_us = res
            .get("t")
            .and_then(parse_i64_loose)
            .or_else(|| value.get("time_ms").and_then(parse_i64_loose))
            .map(|ms| ms.saturating_mul(1000))
            .or_else(|| {
                value
                    .get("time")
                    .and_then(parse_i64_loose)
                    .map(|s| s.saturating_mul(1_000_000))
            })
            .unwrap_or(0);

        let bid_price = res
            .get("b")
            .and_then(parse_f64_loose)
            .ok_or_else(|| anyhow!("gate {} {} missing/invalid b", channel, symbol))?;
        let bid_amount = res
            .get("B")
            .and_then(parse_f64_loose)
            .ok_or_else(|| anyhow!("gate {} {} missing/invalid B", channel, symbol))?;
        let ask_price = res
            .get("a")
            .and_then(parse_f64_loose)
            .ok_or_else(|| anyhow!("gate {} {} missing/invalid a", channel, symbol))?;
        let ask_amount = res
            .get("A")
            .and_then(parse_f64_loose)
            .ok_or_else(|| anyhow!("gate {} {} missing/invalid A", channel, symbol))?;

        if bid_price <= 0.0 || ask_price <= 0.0 || bid_amount <= 0.0 || ask_amount <= 0.0 {
            return Ok(Vec::new());
        }

        Ok(vec![BboFrame {
            symbol,
            ts_us,
            seq_id,
            reset_seq: false,
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        }])
    }

    fn parse_trade_frame(&self, value: &Value) -> Result<Vec<TradeFrame>> {
        parse_trade_frame(value)
    }

    fn parse_incremental_frame(&self, value: &Value) -> Result<Vec<IncrementalFrame>> {
        parse_incremental_frame(value)
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

fn parse_trade_frame(value: &Value) -> Result<Vec<TradeFrame>> {
    let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
    if event != "update" || !channel.ends_with(".trades") {
        return Ok(Vec::new());
    }

    let timestamp_fallback_us = value
        .get("time_ms")
        .and_then(parse_i64_loose)
        .map(|ms| ms.saturating_mul(1000))
        .or_else(|| {
            value
                .get("time")
                .and_then(parse_i64_loose)
                .map(|s| s.saturating_mul(1_000_000))
        })
        .unwrap_or(0);
    let items: Vec<&Value> = match value.get("result") {
        Some(Value::Array(arr)) => arr.iter().collect(),
        Some(obj @ Value::Object(_)) => vec![obj],
        _ => Vec::new(),
    };

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let symbol = obj
            .get("contract")
            .and_then(|v| v.as_str())
            .or_else(|| obj.get("currency_pair").and_then(|v| v.as_str()))
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default();
        if symbol.is_empty() {
            continue;
        }
        let trade_id = obj.get("id").and_then(parse_i64_loose).unwrap_or(0);
        let timestamp_us = obj
            .get("create_time_ms")
            .and_then(parse_i64_loose)
            .map(|ms| ms.saturating_mul(1000))
            .or_else(|| {
                obj.get("create_time")
                    .and_then(parse_i64_loose)
                    .map(|s| s.saturating_mul(1_000_000))
            })
            .unwrap_or(timestamp_fallback_us);
        let price = obj.get("price").and_then(parse_f64_loose).unwrap_or(0.0);
        let (side, amount) = if let Some(size) = obj.get("size").and_then(parse_f64_loose) {
            (if size >= 0.0 { 'B' } else { 'S' }, size.abs())
        } else {
            let amount = obj.get("amount").and_then(parse_f64_loose).unwrap_or(0.0);
            let side = match obj.get("side").and_then(|v| v.as_str()) {
                Some("buy") => 'B',
                Some("sell") => 'S',
                _ => continue,
            };
            (side, amount)
        };
        if price <= 0.0 || amount <= 0.0 {
            continue;
        }
        out.push(TradeFrame {
            symbol,
            timestamp_us,
            seq_id: trade_id,
            trade_id,
            side,
            price,
            amount,
        });
    }
    Ok(out)
}

pub(crate) fn parse_incremental_frame(value: &Value) -> Result<Vec<IncrementalFrame>> {
    let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
    if event != "update"
        || !(channel.ends_with(".order_book_update") || channel.ends_with(".order_book"))
    {
        return Ok(Vec::new());
    }

    let items: Vec<&Value> = match value.get("result") {
        Some(Value::Array(arr)) => arr.iter().collect(),
        Some(obj @ Value::Object(_)) => vec![obj],
        _ => Vec::new(),
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let symbol = obj
            .get("s")
            .and_then(|v| v.as_str())
            .or_else(|| obj.get("contract").and_then(|v| v.as_str()))
            .or_else(|| obj.get("currency_pair").and_then(|v| v.as_str()))
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default();
        if symbol.is_empty() {
            continue;
        }
        let first_id = obj
            .get("U")
            .and_then(parse_i64_loose)
            .or_else(|| obj.get("id").and_then(parse_i64_loose))
            .unwrap_or(0);
        let last_id = obj
            .get("u")
            .and_then(parse_i64_loose)
            .or_else(|| obj.get("last_id").and_then(parse_i64_loose))
            .unwrap_or(first_id);
        let timestamp = obj
            .get("t")
            .and_then(parse_i64_loose)
            .or_else(|| value.get("time_ms").and_then(parse_i64_loose))
            .map(normalize_ts_to_us)
            .unwrap_or(0);
        let bids = obj
            .get("b")
            .or_else(|| obj.get("bids"))
            .map(parse_gate_levels)
            .unwrap_or_default();
        let asks = obj
            .get("a")
            .or_else(|| obj.get("asks"))
            .map(parse_gate_levels)
            .unwrap_or_default();
        if bids.is_empty() && asks.is_empty() {
            continue;
        }
        out.push(IncrementalFrame::Book {
            symbol,
            timestamp,
            seq_id: last_id,
            prev_seq_id: i64::MIN,
            first_update_id: first_id,
            final_update_id: last_id,
            gap_check: false,
            is_snapshot: channel.ends_with(".order_book"),
            bids,
            asks,
        });
    }
    Ok(out)
}

fn parse_gate_levels(raw: &Value) -> Vec<Level> {
    raw.as_array()
        .map(|arr| arr.iter().filter_map(parse_gate_level).collect())
        .unwrap_or_default()
}

fn parse_gate_level(value: &Value) -> Option<Level> {
    if let Some(arr) = value.as_array() {
        if arr.len() < 2 {
            return None;
        }
        let price = parse_f64_loose(&arr[0])?;
        let amount = parse_f64_loose(&arr[1])?;
        if price > 0.0 {
            return Some(Level::from_values(price, amount));
        }
        return None;
    }
    let obj = value.as_object()?;
    let price = obj
        .get("p")
        .and_then(parse_f64_loose)
        .or_else(|| obj.get("price").and_then(parse_f64_loose))?;
    let amount = obj
        .get("s")
        .and_then(parse_f64_loose)
        .or_else(|| obj.get("size").and_then(parse_f64_loose))
        .or_else(|| obj.get("amount").and_then(parse_f64_loose))
        .unwrap_or(0.0);
    if price > 0.0 {
        Some(Level::from_values(price, amount))
    } else {
        None
    }
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn parse_i64_loose(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return Some(n as i64);
    }
    if let Some(s) = v.as_str() {
        return s.parse::<i64>().ok();
    }
    None
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

fn parse_f64_loose(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    if let Some(s) = v.as_str() {
        return s.parse::<f64>().ok();
    }
    None
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
