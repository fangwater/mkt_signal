//! Binance spread 适配器。
//!
//! - spot:   `wss://stream.binance.com:9443/stream`
//! - futures: `wss://fstream.binance.com/public/stream`
//! - spot subscribe: `{"method":"SUBSCRIBE","params":["<sym>@bookTicker", ...],"id":1}`
//! - futures subscribe: `{"method":"SUBSCRIBE","params":["<sym>@depth5@0ms", ...],"id":1}`
//! - spot frame: `{"stream":"<sym>@bookTicker","data":{u,s,b,B,a,A[,T,E]}}`
//! - futures frame: `{"stream":"<sym>@depth5@0ms","data":{u,s,b:[[px,qty],...],a:[[px,qty],...],E,T}}`
//! - seq_id 字段: `u`（order book updateId，单 symbol 内单调递增）
//! - timestamp:  futures 优先用 `E`（事件推送时间）统计延迟；没有 `E` 才回退 `T`
//! - 心跳: 服务端主动 Ping，客户端只需 Pong（已在 ws.rs 通用处理）。

use anyhow::{anyhow, Result};
use serde_json::Value;

use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};

const BINANCE_SPOT_WS_URL: &str = "wss://stream.binance.com:9443/stream";
const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/public/stream";
const BINANCE_SUBSCRIBE_CHUNK: usize = 200;

pub struct BinanceAdapter {
    venue: TradingVenue,
}

impl BinanceAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        Self { venue }
    }
}

impl VenueAdapter for BinanceAdapter {
    fn name(&self) -> &'static str {
        "binance"
    }

    fn ws_url(&self) -> String {
        match self.venue {
            TradingVenue::BinanceMargin => BINANCE_SPOT_WS_URL.to_string(),
            TradingVenue::BinanceFutures => BINANCE_FUTURES_WS_URL.to_string(),
            other => unreachable!("BinanceAdapter created with non-binance venue: {:?}", other),
        }
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        let chunk_size = BINANCE_SUBSCRIBE_CHUNK.max(1);
        let mut out = Vec::new();
        for (i, chunk) in symbols.chunks(chunk_size).enumerate() {
            let params: Vec<String> = chunk
                .iter()
                .map(|sym| {
                    let stream = match self.venue {
                        TradingVenue::BinanceFutures => "depth5@0ms",
                        TradingVenue::BinanceMargin => "bookTicker",
                        other => {
                            unreachable!(
                                "BinanceAdapter created with non-binance venue: {:?}",
                                other
                            )
                        }
                    };
                    format!("{}@{}", sym.to_ascii_lowercase(), stream)
                })
                .collect();
            out.push(serde_json::json!({
                "method": "SUBSCRIBE",
                "params": params,
                "id": (i as u64) + 1,
            }));
        }
        out
    }

    fn parse_frame(&self, value: &Value) -> Result<Vec<BboFrame>> {
        // combined stream 把 payload 包在 `data` 下；裸 stream 直接是 payload
        let payload = value.get("data").unwrap_or(value);
        if !payload.is_object() {
            return Ok(Vec::new());
        }

        let symbol = match payload.get("s").and_then(|v| v.as_str()) {
            Some(s) => s.to_ascii_uppercase(),
            None => return Ok(Vec::new()),
        };
        let seq_id = parse_seq_id(payload, &symbol)?;

        let ts_us = payload
            .get("E")
            .and_then(|v| v.as_i64())
            .or_else(|| payload.get("T").and_then(|v| v.as_i64()))
            .unwrap_or(0)
            .saturating_mul(1000);

        let (bid_price, bid_amount, ask_price, ask_amount) =
            if payload.get("b").and_then(|v| v.as_array()).is_some()
                || payload.get("a").and_then(|v| v.as_array()).is_some()
            {
                parse_depth5_top(payload, &symbol)?
            } else {
                (
                    parse_obj_str_f64(payload, "b", &symbol)?,
                    parse_obj_str_f64(payload, "B", &symbol)?,
                    parse_obj_str_f64(payload, "a", &symbol)?,
                    parse_obj_str_f64(payload, "A", &symbol)?,
                )
            };

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

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        None
    }
}

fn parse_seq_id(payload: &Value, symbol: &str) -> Result<i64> {
    payload
        .get("u")
        .and_then(|v| v.as_i64())
        .or_else(|| payload.get("lastUpdateId").and_then(|v| v.as_i64()))
        .ok_or_else(|| anyhow!("binance spread {} missing u/lastUpdateId", symbol))
}

fn parse_depth5_top(payload: &Value, symbol: &str) -> Result<(f64, f64, f64, f64)> {
    let bid = payload
        .get("b")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("binance depth5 {} missing b[0]", symbol))?;
    let ask = payload
        .get("a")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("binance depth5 {} missing a[0]", symbol))?;

    Ok((
        parse_level_str_f64(bid.first(), "bid price", symbol)?,
        parse_level_str_f64(bid.get(1), "bid amount", symbol)?,
        parse_level_str_f64(ask.first(), "ask price", symbol)?,
        parse_level_str_f64(ask.get(1), "ask amount", symbol)?,
    ))
}

fn parse_obj_str_f64(obj: &Value, key: &str, symbol: &str) -> Result<f64> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow!("binance spread {} missing/invalid {}", symbol, key))
}

fn parse_level_str_f64(v: Option<&Value>, field: &str, symbol: &str) -> Result<f64> {
    v.and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow!("binance depth5 {} missing/invalid {}", symbol, field))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(raw: &str) -> Value {
        serde_json::from_str(raw).expect("test fixture must be valid JSON")
    }

    #[test]
    fn parses_depth5_top_of_book_prefers_e_field() {
        let raw = r#"{
            "stream":"btcusdt@depth5@0ms",
            "data":{
                "e":"depthUpdate","E":1700000000001,"T":1700000000000,
                "s":"BTCUSDT","U":12300,"u":12345,
                "b":[["25.0","100"],["24.9","2"]],
                "a":[["25.1","50"],["25.2","3"]]
            }
        }"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let frames = a.parse_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.seq_id, 12345);
        assert_eq!(f.ts_us, 1700000000001 * 1000);
        assert!((f.bid_price - 25.0).abs() < 1e-9);
        assert!((f.bid_amount - 100.0).abs() < 1e-9);
        assert!((f.ask_price - 25.1).abs() < 1e-9);
        assert!((f.ask_amount - 50.0).abs() < 1e-9);
    }

    #[test]
    fn parses_bookticker_prefers_e_field() {
        let raw = r#"{
            "stream":"btcusdt@bookTicker",
            "data":{
                "e":"bookTicker","u":12345,"s":"BTCUSDT",
                "b":"25.0","B":"100","a":"25.1","A":"50",
                "T":1700000000000,"E":1700000000001
            }
        }"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let frames = a.parse_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.seq_id, 12345);
        assert_eq!(f.ts_us, 1700000000001 * 1000); // 优先 E，用于延迟统计
        assert!((f.bid_price - 25.0).abs() < 1e-9);
        assert!((f.ask_price - 25.1).abs() < 1e-9);
    }

    #[test]
    fn parses_depth5_falls_back_to_t_field() {
        let raw = r#"{"data":{"u":12345,"s":"BTCUSDT","b":[["25.0","1"]],"a":[["25.1","2"]],"T":1700000000000}}"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let frames = a.parse_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].ts_us, 1700000000000 * 1000);
    }

    #[test]
    fn parses_bookticker_falls_back_to_t_field() {
        let raw = r#"{"data":{"u":12345,"s":"BTCUSDT","b":"25.0","B":"1","a":"25.1","A":"2","T":1700000000000}}"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let frames = a.parse_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].ts_us, 1700000000000 * 1000);
    }

    #[test]
    fn parses_spot_bookticker_without_ts_field() {
        let raw = r#"{"data":{"u":12345,"s":"BTCUSDT","b":"25.0","B":"1","a":"25.1","A":"2"}}"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceMargin);
        let frames = a.parse_frame(&v(raw)).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].ts_us, 0);
    }

    #[test]
    fn missing_u_field_is_an_error() {
        let raw = r#"{"data":{"s":"BTCUSDT","b":"25","B":"1","a":"25.1","A":"1"}}"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        assert!(a.parse_frame(&v(raw)).is_err());
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
    fn spot_subscribes_bookticker() {
        let a = BinanceAdapter::new(TradingVenue::BinanceMargin);
        let msgs = a.build_subscribe(&["BTCUSDT".to_string()]);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["params"][0], "btcusdt@bookTicker");
    }
}
