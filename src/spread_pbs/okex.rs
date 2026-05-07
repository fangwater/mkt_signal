use anyhow::{anyhow, Result};
use serde_json::Value;
use std::time::Duration;

use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};

const OKEX_PUBLIC_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const OKEX_SUBSCRIBE_CHUNK: usize = 240;

/// 把 OKex `BTC-USDT-SWAP` / `BTC-USDT` 归一化成 `BTCUSDT`。
pub fn normalize_okex_symbol(symbol: &str) -> String {
    let mut upper = symbol.to_ascii_uppercase();
    if upper.ends_with("-SWAP") && upper.len() > 5 {
        upper.truncate(upper.len() - 5);
    }
    upper.retain(|ch| ch != '-');
    upper
}

/// OKex spot/swap 共用 bbo-tbt frame 解析。
pub fn parse_bbo_tbt(json_str: &str) -> Result<Vec<BboFrame>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| anyhow!("bbo-tbt json parse failed: {}", e))?;

    let arg = value
        .get("arg")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("bbo-tbt missing arg object"))?;

    let channel = arg.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    if channel != "bbo-tbt" {
        return Ok(Vec::new());
    }
    let inst_id = arg
        .get("instId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("bbo-tbt missing arg.instId"))?;

    let data = match value.get("data").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Ok(Vec::new()),
    };

    let symbol = normalize_okex_symbol(inst_id);
    let mut out = Vec::with_capacity(data.len());

    for entry in data {
        let ts_ms = entry
            .get("ts")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| anyhow!("bbo-tbt {} missing ts", inst_id))?;

        let seq_id = entry
            .get("seqId")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow!("bbo-tbt {} missing seqId", inst_id))?;

        let bid_arr = entry
            .get("bids")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("bbo-tbt {} missing bids[0]", inst_id))?;
        let ask_arr = entry
            .get("asks")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("bbo-tbt {} missing asks[0]", inst_id))?;

        let bid_price = parse_string_f64(bid_arr.first(), "bid price", inst_id)?;
        let bid_amount = parse_string_f64(bid_arr.get(1), "bid amount", inst_id)?;
        let ask_price = parse_string_f64(ask_arr.first(), "ask price", inst_id)?;
        let ask_amount = parse_string_f64(ask_arr.get(1), "ask amount", inst_id)?;

        if bid_price <= 0.0 || ask_price <= 0.0 || bid_amount <= 0.0 || ask_amount <= 0.0 {
            continue;
        }

        out.push(BboFrame {
            symbol: symbol.clone(),
            ts_ms,
            seq_id,
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        });
    }

    Ok(out)
}

fn parse_string_f64(value: Option<&Value>, field: &str, inst_id: &str) -> Result<f64> {
    value
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow!("bbo-tbt {} missing/invalid {}", inst_id, field))
}

/// 把 inst_id 列表切片成多条 subscribe 消息（OKex 软上限 240 args/帧）。
pub fn build_bbo_tbt_subscribe_messages(inst_ids: &[String], chunk_size: usize) -> Vec<Value> {
    let chunk_size = chunk_size.max(1);
    let mut out = Vec::new();
    for chunk in inst_ids.chunks(chunk_size) {
        let args: Vec<Value> = chunk
            .iter()
            .map(|inst| {
                serde_json::json!({
                    "channel": "bbo-tbt",
                    "instId": inst,
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

pub struct OkexAdapter {
    _venue: TradingVenue,
}

impl OkexAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        Self { _venue: venue }
    }
}

impl VenueAdapter for OkexAdapter {
    fn name(&self) -> &'static str {
        "okex"
    }

    fn ws_url(&self) -> String {
        OKEX_PUBLIC_WS_URL.to_string()
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_bbo_tbt_subscribe_messages(symbols, OKEX_SUBSCRIBE_CHUNK)
    }

    fn parse_frame(&self, raw: &str) -> Result<Vec<BboFrame>> {
        // 控制帧 / pong / event ack 直接返回空集（不要 anyhow!）。
        if !raw.contains("\"channel\":\"bbo-tbt\"") {
            return Ok(Vec::new());
        }
        parse_bbo_tbt(raw)
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        Some(KeepaliveSpec::text(Duration::from_secs(20), "ping"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_bbo_tbt_frame() {
        let raw = r#"{
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "asks": [["91234.5", "0.5", "0", "1"]],
                "bids": [["91234.4", "0.3", "0", "1"]],
                "ts": "1700000000123",
                "seqId": 12345678,
                "checksum": -1
            }]
        }"#;
        let frames = parse_bbo_tbt(raw).expect("parse ok");
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.ts_ms, 1700000000123);
        assert_eq!(f.seq_id, 12345678);
        assert!((f.bid_price - 91234.4).abs() < 1e-9);
        assert!((f.ask_price - 91234.5).abs() < 1e-9);
        assert!((f.bid_amount - 0.3).abs() < 1e-9);
        assert!((f.ask_amount - 0.5).abs() < 1e-9);
    }

    #[test]
    fn missing_seq_id_is_an_error() {
        let raw = r#"{
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "asks": [["91234.5", "0.5", "0", "1"]],
                "bids": [["91234.4", "0.3", "0", "1"]],
                "ts": "1700000000123"
            }]
        }"#;
        let err = parse_bbo_tbt(raw).unwrap_err();
        assert!(err.to_string().contains("seqId"));
    }

    #[test]
    fn ignores_other_channels() {
        let raw = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "data": []
        }"#;
        let frames = parse_bbo_tbt(raw).expect("parse ok");
        assert!(frames.is_empty());
    }

    #[test]
    fn chunks_subscribe_into_240_per_batch() {
        let inst_ids: Vec<String> = (0..500).map(|i| format!("INST-{}", i)).collect();
        let msgs = build_bbo_tbt_subscribe_messages(&inst_ids, 240);
        assert_eq!(msgs.len(), 3);
        let first_args = msgs[0]["args"].as_array().unwrap();
        assert_eq!(first_args.len(), 240);
        let last_args = msgs[2]["args"].as_array().unwrap();
        assert_eq!(last_args.len(), 20);
        assert_eq!(msgs[0]["op"], "subscribe");
    }
}
