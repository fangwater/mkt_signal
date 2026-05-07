//! Binance bookTicker spread 适配器。
//!
//! - spot:   `wss://stream.binance.com:9443/stream`
//! - futures: `wss://fstream.binance.com/stream`
//! - subscribe: `{"method":"SUBSCRIBE","params":["<sym>@bookTicker", ...],"id":1}`
//! - frame（combined stream 形态）: `{"stream":"<sym>@bookTicker","data":{u,s,b,B,a,A[,T,E]}}`
//! - seq_id 字段: `u`（order book updateId，单 symbol 内单调递增）
//! - timestamp:  futures 有 `T`/`E`；spot bookTicker 不带 ts，回退 0
//! - 心跳: 服务端主动 Ping，客户端只需 Pong（已在 ws.rs 通用处理）。

use anyhow::{anyhow, Result};
use serde_json::Value;

use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};

const BINANCE_SPOT_WS_URL: &str = "wss://stream.binance.com:9443/stream";
const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/stream";
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
                .map(|sym| format!("{}@bookTicker", sym.to_ascii_lowercase()))
                .collect();
            out.push(serde_json::json!({
                "method": "SUBSCRIBE",
                "params": params,
                "id": (i as u64) + 1,
            }));
        }
        out
    }

    fn parse_frame(&self, raw: &str) -> Result<Vec<BboFrame>> {
        let value: Value = match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(_) => return Ok(Vec::new()),
        };

        // combined stream 把 payload 包在 `data` 下；裸 stream 直接是 payload
        let payload = value.get("data").unwrap_or(&value);
        if !payload.is_object() {
            return Ok(Vec::new());
        }

        let symbol = match payload.get("s").and_then(|v| v.as_str()) {
            Some(s) => s.to_ascii_uppercase(),
            None => return Ok(Vec::new()),
        };
        let seq_id = payload
            .get("u")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow!("binance bookTicker {} missing u (updateId)", symbol))?;

        let ts_ms = payload
            .get("T")
            .and_then(|v| v.as_i64())
            .or_else(|| payload.get("E").and_then(|v| v.as_i64()))
            .unwrap_or(0);

        let bid_price = parse_str_f64(payload, "b", &symbol)?;
        let bid_amount = parse_str_f64(payload, "B", &symbol)?;
        let ask_price = parse_str_f64(payload, "a", &symbol)?;
        let ask_amount = parse_str_f64(payload, "A", &symbol)?;

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
        None
    }
}

fn parse_str_f64(obj: &Value, key: &str, symbol: &str) -> Result<f64> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow!("binance bookTicker {} missing/invalid {}", symbol, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_futures_bookticker_with_T_field() {
        let raw = r#"{
            "stream":"btcusdt@bookTicker",
            "data":{
                "e":"bookTicker","u":12345,"s":"BTCUSDT",
                "b":"25.0","B":"100","a":"25.1","A":"50",
                "T":1700000000000,"E":1700000000001
            }
        }"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let frames = a.parse_frame(raw).unwrap();
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.seq_id, 12345);
        assert_eq!(f.ts_ms, 1700000000000); // 优先 T
        assert!((f.bid_price - 25.0).abs() < 1e-9);
        assert!((f.ask_price - 25.1).abs() < 1e-9);
    }

    #[test]
    fn parses_spot_bookticker_without_ts_field() {
        let raw = r#"{"data":{"u":12345,"s":"BTCUSDT","b":"25.0","B":"1","a":"25.1","A":"2"}}"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceMargin);
        let frames = a.parse_frame(raw).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].ts_ms, 0);
    }

    #[test]
    fn missing_u_field_is_an_error() {
        let raw = r#"{"data":{"s":"BTCUSDT","b":"25","B":"1","a":"25.1","A":"1"}}"#;
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        assert!(a.parse_frame(raw).is_err());
    }

    #[test]
    fn subscribe_chunks() {
        let a = BinanceAdapter::new(TradingVenue::BinanceFutures);
        let symbols: Vec<String> = (0..450).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["params"].as_array().unwrap().len(), 200);
        assert_eq!(msgs[2]["params"].as_array().unwrap().len(), 50);
    }
}
