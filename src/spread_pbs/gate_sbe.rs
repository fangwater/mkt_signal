//! Gate Futures SBE `book_ticker` spread 适配器。
//!
//! - prod URL: `wss://fx-ws.gateio.ws/v4/ws/usdt/sbe`
//! - subscribe: JSON，格式与 JSON 端一致，channel=`futures.book_ticker`
//! - data push: WebSocket 二进制帧 (opcode=2 = SBE)；文本帧 (opcode=1) 为 subscribe ack / pong
//! - SBE schema: gate_fex_ws_prod_latest.xml, schemaId=1
//!   - bbo (templateId=1): time i64 | e i8 | t i64 | u i64 | pxExp i8 | szExp i8
//!                          | askPxM i64 | askSzM i64 | bidPxM i64 | bidSzM i64
//!                          → varString8 channel + varString8 symbol
//! - ts_us: 取 `t`（撮合引擎时间戳，µs），与 JSON 端 `result.t*1000` 同语义
//! - 心跳: 每 15s 发 `{"time":<unix>,"channel":"futures.ping"}`

use anyhow::Result;
use bytes::Bytes;
use mkt_parsers::gate as gate_codec;
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::spread_pbs::adapter::{
    BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};

const GATE_SBE_FUTURES_WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt/sbe";
const GATE_JSON_FUTURES_WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
const GATE_SUBSCRIBE_CHUNK: usize = 100;

pub struct GateSbeAdapter;

impl GateSbeAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl VenueAdapter for GateSbeAdapter {
    fn name(&self) -> &'static str {
        "gate-sbe"
    }

    fn ws_url(&self) -> String {
        GATE_SBE_FUTURES_WS_URL.to_string()
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_channel_subscribe(symbols, "book_ticker")
    }

    fn build_trade_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_channel_subscribe(symbols, "trades")
    }

    fn build_incremental_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        let mut out = Vec::new();
        for symbol in symbols {
            out.push(serde_json::json!({
                "time": now_unix_secs(),
                "channel": "futures.order_book_update",
                "event": "subscribe",
                "payload": [symbol.as_str(), "100ms", "100"],
            }));
        }
        out
    }

    fn build_derivatives_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_channel_subscribe(symbols, "tickers")
    }

    fn incremental_ws_url(&self) -> Option<String> {
        Some(GATE_JSON_FUTURES_WS_URL.to_string())
    }

    fn parse_frame(&self, _value: &Value) -> Result<Vec<BboFrame>> {
        // SBE 端的文本帧只有 subscribe ack / pong；静默忽略
        Ok(Vec::new())
    }

    fn parse_incremental_frame(&self, value: &Value) -> Result<Vec<IncrementalFrame>> {
        Ok(gate_codec::parse_incremental_json(value)
            .into_iter()
            .map(crate::spread_pbs::gate::book_to_incremental)
            .collect())
    }

    fn parse_binary_frame(&self, raw: &[u8]) -> Result<Vec<BboFrame>> {
        Ok(gate_codec::parse_sbe_bbo(raw)
            .map(crate::spread_pbs::gate::bbo_to_frame)
            .into_iter()
            .collect())
    }

    fn parse_trade_binary_frame(&self, raw: &[u8]) -> Result<Vec<TradeFrame>> {
        Ok(gate_codec::parse_sbe_trades(raw)
            .into_iter()
            .map(crate::spread_pbs::gate::trade_to_frame)
            .collect())
    }

    fn parse_derivatives_binary_frame(&self, raw: &[u8]) -> Result<Vec<Bytes>> {
        Ok(gate_codec::parse_sbe_derivatives(raw)
            .into_iter()
            .map(crate::spread_pbs::gate::derivative_to_bytes)
            .collect())
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        Some(KeepaliveSpec::dynamic(Duration::from_secs(15), || {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let body = serde_json::json!({
                "time": timestamp,
                "channel": "futures.ping",
            });
            Message::Text(body.to_string())
        }))
    }
}

fn build_channel_subscribe(symbols: &[String], channel: &str) -> Vec<Value> {
    let chunk_size = GATE_SUBSCRIBE_CHUNK.max(1);
    let mut out = Vec::new();
    let channel = format!("futures.{}", channel);
    for chunk in symbols.chunks(chunk_size) {
        let payload: Vec<String> = chunk.to_vec();
        out.push(serde_json::json!({
            "time": now_unix_secs(),
            "channel": channel,
            "event": "subscribe",
            "payload": payload,
        }));
    }
    out
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::mkt_msg::{FundingRateMsg, IndexPriceMsg, MarkPriceMsg};

    fn build_bbo_frame(
        time_us: i64,
        t_us: i64,
        seq_id: i64,
        px_exp: i8,
        sz_exp: i8,
        ask_px_m: i64,
        ask_sz_m: i64,
        bid_px_m: i64,
        bid_sz_m: i64,
        channel: &str,
        symbol: &str,
    ) -> Vec<u8> {
        let block_length: u16 = 59;
        let mut buf = Vec::with_capacity(128);
        // header
        buf.extend_from_slice(&block_length.to_le_bytes());
        buf.extend_from_slice(&gate_codec::SBE_TEMPLATE_BBO.to_le_bytes());
        buf.extend_from_slice(&gate_codec::SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes()); // version
                                                    // root
        buf.extend_from_slice(&time_us.to_le_bytes()); // time @0
        buf.push(2u8); // e=Update @8
        buf.extend_from_slice(&t_us.to_le_bytes()); // t @9
        buf.extend_from_slice(&seq_id.to_le_bytes()); // u @17
        buf.push(px_exp as u8); // pxExp @25
        buf.push(sz_exp as u8); // szExp @26
        buf.extend_from_slice(&ask_px_m.to_le_bytes()); // askPxM @27
        buf.extend_from_slice(&ask_sz_m.to_le_bytes()); // askSzM @35
        buf.extend_from_slice(&bid_px_m.to_le_bytes()); // bidPxM @43
        buf.extend_from_slice(&bid_sz_m.to_le_bytes()); // bidSzM @51
                                                        // varString8 channel
        let ch = channel.as_bytes();
        buf.push(ch.len() as u8);
        buf.extend_from_slice(ch);
        // varString8 symbol
        let sym = symbol.as_bytes();
        buf.push(sym.len() as u8);
        buf.extend_from_slice(sym);
        buf
    }

    fn push_var_string(buf: &mut Vec<u8>, value: &str) {
        buf.push(value.len() as u8);
        buf.extend_from_slice(value.as_bytes());
    }

    fn build_trade_frame(
        px_exp: i8,
        sz_exp: i8,
        entries: &[(i64, u64, i64, i64)],
        symbol: &str,
    ) -> Vec<u8> {
        let block_length: u16 = 11;
        let entry_length: u16 = 32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&block_length.to_le_bytes());
        buf.extend_from_slice(&gate_codec::SBE_TEMPLATE_TRADE.to_le_bytes());
        buf.extend_from_slice(&gate_codec::SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&1_748_000_000_000_000i64.to_le_bytes());
        buf.push(2);
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf.extend_from_slice(&entry_length.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        for (t_us, id, size_m, price_m) in entries {
            buf.extend_from_slice(&t_us.to_le_bytes());
            buf.extend_from_slice(&id.to_le_bytes());
            buf.extend_from_slice(&size_m.to_le_bytes());
            buf.extend_from_slice(&price_m.to_le_bytes());
        }
        push_var_string(&mut buf, "futures.trades");
        push_var_string(&mut buf, symbol);
        buf
    }

    fn build_ticker_frame(
        time_us: i64,
        mark_px_m: i64,
        index_px_m: i64,
        funding_m: i64,
        symbol: &str,
    ) -> Vec<u8> {
        let block_length: u16 = 9;
        let entry_length: u16 = 122;
        let mut buf = Vec::new();
        buf.extend_from_slice(&block_length.to_le_bytes());
        buf.extend_from_slice(&gate_codec::SBE_TEMPLATE_TICKER.to_le_bytes());
        buf.extend_from_slice(&gate_codec::SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&time_us.to_le_bytes());
        buf.push(2);
        buf.extend_from_slice(&entry_length.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        let mut entry = vec![0u8; entry_length as usize];
        entry[41] = 0xFF; // -1
        entry[42..50].copy_from_slice(&mark_px_m.to_le_bytes());
        entry[50] = 0xFF; // -1
        entry[51..59].copy_from_slice(&index_px_m.to_le_bytes());
        entry[68] = 0xFC; // -4
        entry[69..77].copy_from_slice(&funding_m.to_le_bytes());
        buf.extend_from_slice(&entry);
        push_var_string(&mut buf, symbol);
        push_var_string(&mut buf, "0");
        push_var_string(&mut buf, "mark");
        push_var_string(&mut buf, "0");
        buf
    }

    #[test]
    fn decode_bbo_btc_usdt() {
        let raw = build_bbo_frame(
            1_748_000_000_000_000,
            1_748_000_000_001_000,
            98765432,
            -1,
            -4,
            677_358, // ask 67735.8
            33_373,  // ask sz 3.3373
            677_357, // bid 67735.7
            93_708,  // bid sz 9.3708
            "futures.book_ticker",
            "BTC_USDT",
        );
        let frames = GateSbeAdapter::new()
            .parse_binary_frame(&raw)
            .expect("decode ok");
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.seq_id, 98765432);
        assert_eq!(f.ts_us, 1_748_000_000_001_000);
        assert!((f.ask_price - 67735.8).abs() < 1e-6);
        assert!((f.bid_price - 67735.7).abs() < 1e-6);
        assert!((f.ask_amount - 3.3373).abs() < 1e-9);
        assert!((f.bid_amount - 9.3708).abs() < 1e-9);
    }

    #[test]
    fn decodes_trade_with_microsecond_ts() {
        let raw = build_trade_frame(
            -1,
            -4,
            &[(1_748_000_000_001_234, 9001, -93_708, 677_357)],
            "BTC_USDT",
        );
        let frames = GateSbeAdapter::new()
            .parse_trade_binary_frame(&raw)
            .expect("decode trade ok");
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTC_USDT");
        assert_eq!(f.timestamp_us, 1_748_000_000_001_234);
        assert_eq!(f.trade_id, 9001);
        assert_eq!(f.side, 'S');
        assert!((f.price - 67735.7).abs() < 1e-6);
        assert!((f.amount - 9.3708).abs() < 1e-9);
    }

    #[test]
    fn decodes_derivatives_ticker_bytes() {
        let ts_us = 1_748_000_000_001_000;
        let raw = build_ticker_frame(ts_us, 677_357, 677_300, 12, "BTC_USDT");
        let bytes = GateSbeAdapter::new()
            .parse_derivatives_binary_frame(&raw)
            .expect("decode derivatives ok");
        assert_eq!(bytes.len(), 3);
        assert!(!bytes.iter().any(|b| b.is_empty()));
        assert_eq!(MarkPriceMsg::get_timestamp(&bytes[0]), ts_us);
        assert_eq!(IndexPriceMsg::get_timestamp(&bytes[1]), ts_us);
        assert_eq!(FundingRateMsg::get_timestamp(&bytes[2]), ts_us);
    }

    #[test]
    fn unknown_template_returns_empty() {
        let raw = build_bbo_frame(0, 0, 0, 0, 0, 1, 1, 1, 1, "futures.book_ticker", "BTC_USDT");
        let mut patched = raw.clone();
        patched[2] = 2; // templateId=2 (publicTrade)
        patched[3] = 0;
        assert!(GateSbeAdapter::new()
            .parse_binary_frame(&patched)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn rejects_wrong_schema_id() {
        let raw = build_bbo_frame(0, 0, 0, 0, 0, 1, 1, 1, 1, "futures.book_ticker", "BTC_USDT");
        let mut patched = raw.clone();
        patched[4] = 9; // schemaId=9
        patched[5] = 0;
        assert!(GateSbeAdapter::new()
            .parse_binary_frame(&patched)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn rejects_zero_price() {
        let raw = build_bbo_frame(0, 0, 1, 0, 0, 0, 1, 1, 1, "futures.book_ticker", "BTC_USDT");
        assert!(GateSbeAdapter::new()
            .parse_binary_frame(&raw)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn subscribe_builds_json_payload() {
        let a = GateSbeAdapter::new();
        let msgs = a.build_subscribe(&["BTC_USDT".to_string(), "ETH_USDT".to_string()]);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["channel"], "futures.book_ticker");
        assert_eq!(msgs[0]["event"], "subscribe");
        let payload = msgs[0]["payload"].as_array().unwrap();
        assert_eq!(payload[0], "BTC_USDT");
        assert_eq!(payload[1], "ETH_USDT");
    }

    #[test]
    fn trade_and_derivatives_subscribe_build_json_payloads() {
        let a = GateSbeAdapter::new();
        let trades = a.build_trade_subscribe(&["BTC_USDT".to_string()]);
        assert_eq!(trades[0]["channel"], "futures.trades");
        let derivatives = a.build_derivatives_subscribe(&["BTC_USDT".to_string()]);
        assert_eq!(derivatives[0]["channel"], "futures.tickers");
    }

    #[test]
    fn subscribe_chunks_100() {
        let a = GateSbeAdapter::new();
        let symbols: Vec<String> = (0..250).map(|i| format!("SYM{}_USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["payload"].as_array().unwrap().len(), 100);
        assert_eq!(msgs[2]["payload"].as_array().unwrap().len(), 50);
    }
}
