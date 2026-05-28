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

use anyhow::{anyhow, bail, Result};
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};

const GATE_SBE_FUTURES_WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt/sbe";
const GATE_SUBSCRIBE_CHUNK: usize = 100;

const SBE_HEADER_SIZE: usize = 8;
const SBE_SCHEMA_ID: u16 = 1;
const SBE_TEMPLATE_BBO: u16 = 1;
// BBO root: 8+1+8+8+1+1+8+8+8+8 = 59 bytes
const SBE_BBO_ROOT_MIN: usize = 59;

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
        let chunk_size = GATE_SUBSCRIBE_CHUNK.max(1);
        let mut out = Vec::new();
        for chunk in symbols.chunks(chunk_size) {
            let payload: Vec<String> = chunk.to_vec();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            out.push(serde_json::json!({
                "time": timestamp,
                "channel": "futures.book_ticker",
                "event": "subscribe",
                "payload": payload,
            }));
        }
        out
    }

    fn parse_frame(&self, _value: &Value) -> Result<Vec<BboFrame>> {
        // SBE 端的文本帧只有 subscribe ack / pong；静默忽略
        Ok(Vec::new())
    }

    fn parse_binary_frame(&self, raw: &[u8]) -> Result<Vec<BboFrame>> {
        parse_sbe_bbo(raw)
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

/// Gate SBE BBO (templateId=1, schemaId=1) 解码。其他 template 返回空 Vec。
fn parse_sbe_bbo(raw: &[u8]) -> Result<Vec<BboFrame>> {
    if raw.len() < SBE_HEADER_SIZE {
        bail!("Gate SBE frame too short: {} bytes", raw.len());
    }
    let block_length = u16::from_le_bytes([raw[0], raw[1]]) as usize;
    let template_id = u16::from_le_bytes([raw[2], raw[3]]);
    let schema_id = u16::from_le_bytes([raw[4], raw[5]]);

    if schema_id != SBE_SCHEMA_ID {
        bail!(
            "Gate SBE unexpected schemaId={} (want {})",
            schema_id,
            SBE_SCHEMA_ID
        );
    }
    if template_id != SBE_TEMPLATE_BBO {
        return Ok(Vec::new());
    }

    let body_off = SBE_HEADER_SIZE;
    if raw.len() < body_off + block_length {
        bail!(
            "Gate SBE BBO frame truncated: have {} bytes, blockLength={}",
            raw.len(),
            block_length
        );
    }
    if block_length < SBE_BBO_ROOT_MIN {
        bail!(
            "Gate SBE BBO blockLength {} < expected {}",
            block_length,
            SBE_BBO_ROOT_MIN
        );
    }

    // Root block layout (offsets relative to body_off):
    //  0: time  i64 - µs when WS server sent
    //  8: e     i8  - Event type (ignore)
    //  9: t     i64 - orderbook update timestamp µs (engine time) → ts_us
    // 17: u     i64 - orderbook id → seq_id
    // 25: pxExp i8
    // 26: szExp i8
    // 27: askPxM i64
    // 35: askSzM i64
    // 43: bidPxM i64
    // 51: bidSzM i64
    let t_us = read_i64_le(raw, body_off + 9)?;
    let seq_id = read_i64_le(raw, body_off + 17)?;
    let px_exp = raw[body_off + 25] as i8;
    let sz_exp = raw[body_off + 26] as i8;
    let ask_px_m = read_i64_le(raw, body_off + 27)?;
    let ask_sz_m = read_i64_le(raw, body_off + 35)?;
    let bid_px_m = read_i64_le(raw, body_off + 43)?;
    let bid_sz_m = read_i64_le(raw, body_off + 51)?;

    // VarData after root block: channel (skip) then symbol
    let mut off = body_off + block_length;
    if raw.len() <= off {
        bail!("Gate SBE BBO missing channel varString8");
    }
    let chan_len = raw[off] as usize;
    off += 1 + chan_len;

    if raw.len() <= off {
        bail!("Gate SBE BBO missing symbol varString8");
    }
    let sym_len = raw[off] as usize;
    off += 1;
    if raw.len() < off + sym_len {
        bail!(
            "Gate SBE BBO truncated symbol: need {} have {}",
            off + sym_len,
            raw.len()
        );
    }
    let symbol = std::str::from_utf8(&raw[off..off + sym_len])
        .map_err(|e| anyhow!("Gate SBE BBO symbol not utf-8: {}", e))?
        .replace('_', "")
        .to_ascii_uppercase();

    let bid_price = mantissa_to_f64(bid_px_m, px_exp);
    let ask_price = mantissa_to_f64(ask_px_m, px_exp);
    let bid_amount = mantissa_to_f64(bid_sz_m, sz_exp);
    let ask_amount = mantissa_to_f64(ask_sz_m, sz_exp);

    if bid_price <= 0.0 || ask_price <= 0.0 || bid_amount <= 0.0 || ask_amount <= 0.0 {
        return Ok(Vec::new());
    }

    Ok(vec![BboFrame {
        symbol,
        ts_us: t_us,
        seq_id,
        reset_seq: false,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    }])
}

fn read_i64_le(buf: &[u8], off: usize) -> Result<i64> {
    if buf.len() < off + 8 {
        bail!("Gate SBE OOB read at offset {}", off);
    }
    Ok(i64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ]))
}

fn mantissa_to_f64(mantissa: i64, exponent: i8) -> f64 {
    (mantissa as f64) * 10_f64.powi(exponent as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        buf.extend_from_slice(&SBE_TEMPLATE_BBO.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
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
        let frames = parse_sbe_bbo(&raw).expect("decode ok");
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
    fn unknown_template_returns_empty() {
        let raw = build_bbo_frame(0, 0, 0, 0, 0, 1, 1, 1, 1, "futures.book_ticker", "BTC_USDT");
        let mut patched = raw.clone();
        patched[2] = 2; // templateId=2 (publicTrade)
        patched[3] = 0;
        assert!(parse_sbe_bbo(&patched).unwrap().is_empty());
    }

    #[test]
    fn rejects_wrong_schema_id() {
        let raw = build_bbo_frame(0, 0, 0, 0, 0, 1, 1, 1, 1, "futures.book_ticker", "BTC_USDT");
        let mut patched = raw.clone();
        patched[4] = 9; // schemaId=9
        patched[5] = 0;
        let err = parse_sbe_bbo(&patched).unwrap_err();
        assert!(err.to_string().contains("schemaId"));
    }

    #[test]
    fn rejects_zero_price() {
        let raw = build_bbo_frame(0, 0, 1, 0, 0, 0, 1, 1, 1, "futures.book_ticker", "BTC_USDT");
        assert!(parse_sbe_bbo(&raw).unwrap().is_empty());
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
    fn subscribe_chunks_100() {
        let a = GateSbeAdapter::new();
        let symbols: Vec<String> = (0..250).map(|i| format!("SYM{}_USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["payload"].as_array().unwrap().len(), 100);
        assert_eq!(msgs[2]["payload"].as_array().unwrap().len(), 50);
    }
}
