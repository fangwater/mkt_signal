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

use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};

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
        let chunk_size = GATE_SUBSCRIBE_CHUNK.max(1);
        let prefix = self.channel_prefix();
        let channel = format!("{}.book_ticker", prefix);
        let mut out = Vec::new();
        for chunk in symbols.chunks(chunk_size) {
            let payload: Vec<String> = chunk.iter().cloned().collect();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            out.push(serde_json::json!({
                "time": timestamp,
                "channel": channel,
                "event": "subscribe",
                "payload": payload,
            }));
        }
        out
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

        let ts_ms = res
            .get("t")
            .and_then(parse_i64_loose)
            .or_else(|| value.get("time_ms").and_then(parse_i64_loose))
            .or_else(|| {
                value
                    .get("time")
                    .and_then(parse_i64_loose)
                    .map(|s| s.saturating_mul(1000))
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
            ts_ms,
            seq_id,
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        }])
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
        assert_eq!(f.ts_ms, 1606293803097);
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
}
