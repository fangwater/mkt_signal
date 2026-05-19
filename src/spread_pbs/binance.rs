//! Binance spread 适配器。
//!
//! - spot:   `wss://stream.binance.com:9443/stream`
//! - futures: `wss://fstream.binance.com/public/stream`
//! - spot SBE spread_pbs: `wss://stream-sbe.binance.com:9443/ws`
//! - spot subscribe: `{"method":"SUBSCRIBE","params":["<sym>@bookTicker", ...],"id":1}`
//! - spot SBE subscribe: `{"method":"SUBSCRIBE","params":["<sym>@bestBidAsk", ...],"id":1}`
//! - futures subscribe: `{"method":"SUBSCRIBE","params":["<sym>@depth5@0ms", ...],"id":1}`
//! - spot frame: `{"stream":"<sym>@bookTicker","data":{u,s,b,B,a,A[,T,E]}}`
//! - spot SBE frame: BestBidAskStreamEvent(templateId=10001), timestamps already in us
//! - futures frame: `{"stream":"<sym>@depth5@0ms","data":{u,s,b:[[px,qty],...],a:[[px,qty],...],E,T}}`
//! - seq_id 字段: `u`（order book updateId，单 symbol 内单调递增）
//! - timestamp:  futures 优先用 `E`（事件推送时间）统计延迟；没有 `E` 才回退 `T`
//! - 心跳: 服务端主动 Ping，客户端只需 Pong（已在 ws.rs 通用处理）。

use anyhow::{anyhow, Result};
use serde_json::Value;

use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};

const BINANCE_SPOT_SBE_WS_URL: &str = "wss://stream-sbe.binance.com:9443/ws";
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
        let chunk_size = BINANCE_SUBSCRIBE_CHUNK.max(1);
        let mut out = Vec::new();
        for (i, chunk) in symbols.chunks(chunk_size).enumerate() {
            let params: Vec<String> = chunk
                .iter()
                .map(|sym| {
                    let stream = match self.venue {
                        TradingVenue::BinanceFutures => "depth5@0ms",
                        TradingVenue::BinanceMargin => "bestBidAsk",
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

    fn parse_binary_frame(&self, raw: &[u8]) -> Result<Vec<BboFrame>> {
        if self.venue != TradingVenue::BinanceMargin {
            return Ok(Vec::new());
        }
        parse_sbe_best_bid_ask(raw)
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        None
    }
}

fn parse_sbe_best_bid_ask(msg: &[u8]) -> Result<Vec<BboFrame>> {
    let Some(header) = read_sbe_header(msg) else {
        return Ok(Vec::new());
    };
    if header.template_id != 10001 {
        return Ok(Vec::new());
    }

    let base = header.body_offset;
    if msg.len() < base + header.block_length {
        return Ok(Vec::new());
    }

    let event_time = read_i64_le(msg, base).ok_or_else(|| anyhow!("binance sbe bbo missing E"))?;
    let book_update_id =
        read_i64_le(msg, base + 8).ok_or_else(|| anyhow!("binance sbe bbo missing u"))?;
    let price_exponent =
        read_i8(msg, base + 16).ok_or_else(|| anyhow!("binance sbe bbo missing priceExponent"))?;
    let qty_exponent =
        read_i8(msg, base + 17).ok_or_else(|| anyhow!("binance sbe bbo missing qtyExponent"))?;
    let bid_price =
        read_i64_le(msg, base + 18).ok_or_else(|| anyhow!("binance sbe bbo missing bidPrice"))?;
    let bid_qty =
        read_i64_le(msg, base + 26).ok_or_else(|| anyhow!("binance sbe bbo missing bidQty"))?;
    let ask_price =
        read_i64_le(msg, base + 34).ok_or_else(|| anyhow!("binance sbe bbo missing askPrice"))?;
    let ask_qty =
        read_i64_le(msg, base + 42).ok_or_else(|| anyhow!("binance sbe bbo missing askQty"))?;
    let (symbol, _) = read_var_string8(msg, base + header.block_length)
        .ok_or_else(|| anyhow!("binance sbe bbo missing symbol"))?;

    let bid_price = scale_mantissa(bid_price, price_exponent);
    let bid_amount = scale_mantissa(bid_qty, qty_exponent);
    let ask_price = scale_mantissa(ask_price, price_exponent);
    let ask_amount = scale_mantissa(ask_qty, qty_exponent);

    if bid_price <= 0.0 || ask_price <= 0.0 || bid_amount <= 0.0 || ask_amount <= 0.0 {
        return Ok(Vec::new());
    }

    Ok(vec![BboFrame {
        symbol: symbol.to_ascii_uppercase(),
        ts_us: event_time,
        seq_id: book_update_id,
        reset_seq: false,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    }])
}

struct SbeHeader {
    block_length: usize,
    template_id: u16,
    body_offset: usize,
}

fn read_sbe_header(msg: &[u8]) -> Option<SbeHeader> {
    if msg.len() < 8 {
        return None;
    }
    Some(SbeHeader {
        block_length: read_u16_le(msg, 0)? as usize,
        template_id: read_u16_le(msg, 2)?,
        body_offset: 8,
    })
}

fn read_u16_le(msg: &[u8], offset: usize) -> Option<u16> {
    if msg.len() < offset + 2 {
        return None;
    }
    Some(u16::from_le_bytes([msg[offset], msg[offset + 1]]))
}

fn read_i64_le(msg: &[u8], offset: usize) -> Option<i64> {
    if msg.len() < offset + 8 {
        return None;
    }
    Some(i64::from_le_bytes([
        msg[offset],
        msg[offset + 1],
        msg[offset + 2],
        msg[offset + 3],
        msg[offset + 4],
        msg[offset + 5],
        msg[offset + 6],
        msg[offset + 7],
    ]))
}

fn read_i8(msg: &[u8], offset: usize) -> Option<i8> {
    msg.get(offset).map(|v| *v as i8)
}

fn scale_mantissa(mantissa: i64, exponent: i8) -> f64 {
    (mantissa as f64) * 10_f64.powi(exponent as i32)
}

fn read_var_string8(msg: &[u8], offset: usize) -> Option<(String, usize)> {
    let len = msg.get(offset).copied()? as usize;
    let start = offset + 1;
    if msg.len() < start + len {
        return None;
    }
    let symbol = std::str::from_utf8(&msg[start..start + len])
        .ok()?
        .to_string();
    Some((symbol, start + len))
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
        assert_eq!(msgs[0]["params"][0], "btcusdt@bestBidAsk");
    }

    #[test]
    fn spot_uses_sbe_url() {
        let a = BinanceAdapter::new(TradingVenue::BinanceMargin);
        assert_eq!(a.ws_url(), BINANCE_SPOT_SBE_WS_URL);
    }

    #[test]
    fn parses_sbe_best_bid_ask() {
        let a = BinanceAdapter::new(TradingVenue::BinanceMargin);
        let frames = a.parse_binary_frame(&sbe_bbo_frame()).unwrap();
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.ts_us, 1_700_000_000_001_002);
        assert_eq!(f.seq_id, 12345);
        assert!((f.bid_price - 25.0).abs() < 1e-9);
        assert!((f.bid_amount - 100.0).abs() < 1e-9);
        assert!((f.ask_price - 25.1).abs() < 1e-9);
        assert!((f.ask_amount - 50.0).abs() < 1e-9);
    }
}
