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
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};

const BYBIT_SPOT_WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";
const BYBIT_LINEAR_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const BYBIT_SUBSCRIBE_CHUNK: usize = 100;
/// Bybit V5 spot 单 connection args 软限实测 ~200，超过会静默丢推送；
/// 截断到 100 留余量、确保单批 subscribe 后立即可订（不需要继续累积订阅）。
const BYBIT_SPOT_MAX_SYMBOLS: usize = 100;

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
    cache: RefCell<HashMap<String, BboCacheEntry>>,
}

impl BybitAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        Self {
            venue,
            cache: RefCell::new(HashMap::with_capacity(2048)),
        }
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
        let chunk_size = BYBIT_SUBSCRIBE_CHUNK.max(1);
        // spot 走 V5 单连接 args 上限较紧，强制截断；linear 上限 2000，无需截断
        let effective: &[String] = match self.venue {
            TradingVenue::BybitMargin => {
                let cap = BYBIT_SPOT_MAX_SYMBOLS.min(symbols.len());
                if cap < symbols.len() {
                    log::warn!(
                        "spread_pbs[bybit-margin] truncating symbols {} -> {} (V5 spot single-conn args cap)",
                        symbols.len(),
                        cap
                    );
                }
                &symbols[..cap]
            }
            _ => symbols,
        };

        let mut out = Vec::new();
        for chunk in effective.chunks(chunk_size) {
            let args: Vec<String> = chunk
                .iter()
                .map(|sym| format!("orderbook.1.{}", sym.to_ascii_uppercase()))
                .collect();
            out.push(serde_json::json!({
                "op": "subscribe",
                "args": args,
            }));
        }
        out
    }

    fn parse_frame(&self, value: &Value) -> Result<Vec<BboFrame>> {
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
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow!("bybit {} missing data.u (updateId)", topic))?;
        let ts_ms = data
            .get("t")
            .and_then(|v| v.as_i64())
            .or_else(|| value.get("ts").and_then(|v| v.as_i64()))
            .unwrap_or(0);

        let bid_levels = data.get("b").and_then(|v| v.as_array());
        let ask_levels = data.get("a").and_then(|v| v.as_array());

        let mut cache = self.cache.borrow_mut();
        let entry = cache.entry(symbol.clone()).or_default();

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
            ts_ms,
            seq_id,
            bid_price: entry.bid_price,
            bid_amount: entry.bid_amount,
            ask_price: entry.ask_price,
            ask_amount: entry.ask_amount,
        }])
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        Some(KeepaliveSpec::text(
            Duration::from_secs(20),
            r#"{"op":"ping"}"#,
        ))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn v(raw: &str) -> Value {
        serde_json::from_str(raw).expect("test fixture must be valid JSON")
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
    fn build_subscribe_chunks_100_for_linear() {
        let a = BybitAdapter::new(TradingVenue::BybitFutures);
        let symbols: Vec<String> = (0..250).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["args"].as_array().unwrap().len(), 100);
        assert_eq!(msgs[2]["args"].as_array().unwrap().len(), 50);
        let first_arg = msgs[0]["args"][0].as_str().unwrap();
        assert!(first_arg.starts_with("orderbook.1."));
    }

    #[test]
    fn build_subscribe_truncates_spot_to_100() {
        let a = BybitAdapter::new(TradingVenue::BybitMargin);
        let symbols: Vec<String> = (0..313).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        // 100 个 symbol 在 chunk_size=100 下恰好 1 批
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["args"].as_array().unwrap().len(), 100);
    }

    #[test]
    fn build_subscribe_spot_under_cap_no_truncate() {
        let a = BybitAdapter::new(TradingVenue::BybitMargin);
        let symbols: Vec<String> = (0..40).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["args"].as_array().unwrap().len(), 40);
    }
}
