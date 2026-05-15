//! Bitget v2 `books1` spread 适配器（depth=1 BBO，snapshot+update 都带完整一档）。
//!
//! - WS URL（spot/futures 共用 public endpoint）: `wss://ws.bitget.com/v2/ws/public`
//! - subscribe: `{"op":"subscribe","args":[{"instType":<>,"channel":"books1","instId":<sym>}, ...]}`
//!   - spot:    instType = `SPOT`
//!   - futures: instType = `USDT-FUTURES`
//! - frame: `{"action":"snapshot|update","arg":{...},"data":[{"bids","asks","ts","seq"}]}`
//! - seq_id: `data[].seq`（字符串型递增数字）
//! - 心跳: 文本 `"ping"` 每 25s。

use anyhow::{anyhow, Result};
use serde_json::Value;
use std::time::Duration;

use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};

const BITGET_PUBLIC_WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const BITGET_SUBSCRIBE_CHUNK: usize = 50;

pub struct BitgetAdapter {
    venue: TradingVenue,
}

impl BitgetAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        Self { venue }
    }

    fn inst_type(&self) -> &'static str {
        match self.venue {
            TradingVenue::BitgetMargin => "SPOT",
            TradingVenue::BitgetFutures => "USDT-FUTURES",
            other => unreachable!("BitgetAdapter created with non-bitget venue: {:?}", other),
        }
    }
}

impl VenueAdapter for BitgetAdapter {
    fn name(&self) -> &'static str {
        "bitget"
    }

    fn ws_url(&self) -> String {
        BITGET_PUBLIC_WS_URL.to_string()
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        let chunk_size = BITGET_SUBSCRIBE_CHUNK.max(1);
        let inst_type = self.inst_type();
        let mut out = Vec::new();
        for chunk in symbols.chunks(chunk_size) {
            let args: Vec<Value> = chunk
                .iter()
                .map(|sym| {
                    serde_json::json!({
                        "instType": inst_type,
                        "channel": "books1",
                        "instId": sym,
                    })
                })
                .collect();
            out.push(serde_json::json!({
                "op": "subscribe",
                "args": args,
            }));
        }
        out
    }

    fn parse_frame(&self, value: &Value) -> Result<Vec<BboFrame>> {
        let arg = match value.get("arg").and_then(|v| v.as_object()) {
            Some(obj) => obj,
            None => return Ok(Vec::new()),
        };
        if arg.get("channel").and_then(|v| v.as_str()) != Some("books1") {
            return Ok(Vec::new());
        }
        let inst_id = arg
            .get("instId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("bitget books1 missing arg.instId"))?;
        let symbol = inst_id.to_ascii_uppercase();

        let data = match value.get("data").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return Ok(Vec::new()),
        };

        let mut out = Vec::with_capacity(data.len());
        for entry in data {
            let seq_id = entry
                .get("seq")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .or_else(|| entry.get("seq").and_then(|v| v.as_i64()))
                .ok_or_else(|| anyhow!("bitget books1 {} missing/invalid seq", symbol))?;

            let ts_us = entry
                .get("ts")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .or_else(|| entry.get("ts").and_then(|v| v.as_i64()))
                .unwrap_or(0)
                .saturating_mul(1000);

            let bid_arr = entry
                .get("bids")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow!("bitget books1 {} missing bids[0]", symbol))?;
            let ask_arr = entry
                .get("asks")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow!("bitget books1 {} missing asks[0]", symbol))?;

            let bid_price = parse_str_f64(bid_arr.first(), "bid price", &symbol)?;
            let bid_amount = parse_str_f64(bid_arr.get(1), "bid amount", &symbol)?;
            let ask_price = parse_str_f64(ask_arr.first(), "ask price", &symbol)?;
            let ask_amount = parse_str_f64(ask_arr.get(1), "ask amount", &symbol)?;

            if bid_price <= 0.0 || ask_price <= 0.0 || bid_amount <= 0.0 || ask_amount <= 0.0 {
                continue;
            }

            out.push(BboFrame {
                symbol: symbol.clone(),
                ts_us,
                seq_id,
                reset_seq: false,
                bid_price,
                bid_amount,
                ask_price,
                ask_amount,
            });
        }

        Ok(out)
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        Some(KeepaliveSpec::text(Duration::from_secs(25), "ping"))
    }
}

fn parse_str_f64(v: Option<&Value>, field: &str, symbol: &str) -> Result<f64> {
    v.and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow!("bitget books1 {} missing/invalid {}", symbol, field))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(raw: &str) -> Value {
        serde_json::from_str(raw).expect("test fixture must be valid JSON")
    }

    #[test]
    fn parses_books1_full_frame() {
        let raw = r#"{
            "action":"snapshot",
            "arg":{"instType":"SPOT","channel":"books1","instId":"BTCUSDT"},
            "data":[{
                "bids":[["27000.5","0.5"]],
                "asks":[["27000.6","0.7"]],
                "checksum":-1,
                "ts":"1700000000123",
                "seq":"123456"
            }]
        }"#;
        let a = BitgetAdapter::new(TradingVenue::BitgetMargin);
        let frames = a.parse_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.seq_id, 123456);
        assert_eq!(f.ts_us, 1700000000123 * 1000);
        assert!((f.bid_price - 27000.5).abs() < 1e-9);
        assert!((f.ask_amount - 0.7).abs() < 1e-9);
    }

    #[test]
    fn missing_seq_is_an_error() {
        let raw = r#"{
            "arg":{"channel":"books1","instId":"BTCUSDT"},
            "data":[{"bids":[["1","1"]],"asks":[["2","1"]],"ts":"1"}]
        }"#;
        let a = BitgetAdapter::new(TradingVenue::BitgetMargin);
        assert!(a.parse_frame(&v(raw)).is_err());
    }

    #[test]
    fn ignores_other_channels() {
        let raw = r#"{"arg":{"channel":"books"},"data":[]}"#;
        let a = BitgetAdapter::new(TradingVenue::BitgetMargin);
        assert!(a.parse_frame(&v(raw)).unwrap().is_empty());
    }

    #[test]
    fn build_subscribe_chunks_with_correct_inst_type() {
        let a_spot = BitgetAdapter::new(TradingVenue::BitgetMargin);
        let a_fut = BitgetAdapter::new(TradingVenue::BitgetFutures);
        let symbols: Vec<String> = (0..120).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs_spot = a_spot.build_subscribe(&symbols);
        let msgs_fut = a_fut.build_subscribe(&symbols);
        assert_eq!(msgs_spot.len(), 3);
        assert_eq!(msgs_fut.len(), 3);
        assert_eq!(msgs_spot[0]["args"][0]["instType"], "SPOT");
        assert_eq!(msgs_fut[0]["args"][0]["instType"], "USDT-FUTURES");
        assert_eq!(msgs_spot[0]["args"].as_array().unwrap().len(), 50);
    }
}
