//! Bitget UTA v3 SBE `books1` spread 适配器。
//!
//! - WS URL: `wss://ws.bitget.com/v3/ws/public/sbe`
//! - 鉴权:   **不需要**（公开行情，与 OKEx SBE 不同）
//! - 订阅:   JSON 文本，UTA v3 命名: `topic` + `symbol` + 小写 `instType`
//!           `{"op":"subscribe","args":[{"instType":"usdt-futures","topic":"books1","symbol":"BTCUSDT"}]}`
//! - 数据帧: WebSocket 二进制帧 (opcode=2)，littleEndian
//!           header 8B + root 64B + varString8 symbol  ≈ 80B/帧
//!           BBO templateId=1002, schemaId=1, schemaVer=3
//! - 时间戳: 使用 SBE `sts` 填 `BboFrame.ts_us`（gateway 推送时刻，与现行 JSON `ts*1000` 同语义）
//!           SBE 多出的 `ts` (撮合时刻，类似 binance T) 暂未消费
//! - 心跳:   text "ping" 每 25s（与 v2 一致, ws.rs 默认行为）
//!
//! schema 参考: https://www.bitget.com/api-doc/uta/sbe/sbe-bbo

use anyhow::{anyhow, bail, Result};
use serde_json::Value;
use std::time::Duration;

use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};

const BITGET_SBE_WS_URL: &str = "wss://ws.bitget.com/v3/ws/public/sbe";
const BITGET_SUBSCRIBE_CHUNK: usize = 50;

const SBE_HEADER_SIZE: usize = 8;
const SBE_SCHEMA_ID: u16 = 1;
const SBE_TEMPLATE_BOOKS1: u16 = 1002;
/// books1 root: 8*i64/u64 + 2*i8 + u8 = 59 字节(无 padding)，blockLength=64 含 5B padding。
const SBE_BOOKS1_ROOT_MIN: usize = 59;

pub struct BitgetAdapter {
    venue: TradingVenue,
}

impl BitgetAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        Self { venue }
    }

    /// UTA v3 小写 instType。
    fn inst_type(&self) -> &'static str {
        match self.venue {
            TradingVenue::BitgetMargin => "spot",
            TradingVenue::BitgetFutures => "usdt-futures",
            other => unreachable!("BitgetAdapter created with non-bitget venue: {:?}", other),
        }
    }
}

impl VenueAdapter for BitgetAdapter {
    fn name(&self) -> &'static str {
        "bitget"
    }

    fn ws_url(&self) -> String {
        BITGET_SBE_WS_URL.to_string()
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
                        "topic": "books1",
                        "symbol": sym,
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

    fn parse_frame(&self, _value: &Value) -> Result<Vec<BboFrame>> {
        // SBE 端的 text 帧只有 subscribe ack / error event; 静默忽略
        Ok(Vec::new())
    }

    fn parse_binary_frame(&self, raw: &[u8]) -> Result<Vec<BboFrame>> {
        parse_sbe_books1(raw)
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        // text "ping" 每 25s; pong 文本 ws.rs::is_keepalive_response 已识别跳过
        Some(KeepaliveSpec::text(Duration::from_secs(25), "ping"))
    }
}

/// SBE books1 (templateId=1002) 解码。其他 template 直接返回空 Vec。
fn parse_sbe_books1(raw: &[u8]) -> Result<Vec<BboFrame>> {
    if raw.len() < SBE_HEADER_SIZE {
        bail!("Bitget SBE frame too short: {} bytes", raw.len());
    }
    let block_length = u16::from_le_bytes([raw[0], raw[1]]) as usize;
    let template_id = u16::from_le_bytes([raw[2], raw[3]]);
    let schema_id = u16::from_le_bytes([raw[4], raw[5]]);
    if schema_id != SBE_SCHEMA_ID {
        bail!(
            "Bitget SBE unexpected schemaId={} (want {})",
            schema_id,
            SBE_SCHEMA_ID
        );
    }
    if template_id != SBE_TEMPLATE_BOOKS1 {
        return Ok(Vec::new());
    }
    let body_off = SBE_HEADER_SIZE;
    if raw.len() < body_off + block_length {
        bail!(
            "Bitget SBE frame truncated: have {} bytes, header says blockLength={}",
            raw.len(),
            block_length
        );
    }
    if block_length < SBE_BOOKS1_ROOT_MIN {
        bail!(
            "Bitget SBE books1 blockLength {} < expected {}",
            block_length,
            SBE_BOOKS1_ROOT_MIN
        );
    }

    // root 字段顺序（v3, blockLength=64 含 5B 末尾 padding）：
    //  ts u64, bid1Px i64, bid1Sz i64, ask1Px i64, ask1Sz i64,
    //  px_exp i8, sz_exp i8, seq u64, sts u64, category u8
    let _ts_us = read_i64_le(raw, body_off)?;
    let bid_px_m = read_i64_le(raw, body_off + 8)?;
    let bid_sz_m = read_i64_le(raw, body_off + 16)?;
    let ask_px_m = read_i64_le(raw, body_off + 24)?;
    let ask_sz_m = read_i64_le(raw, body_off + 32)?;
    let px_exp = raw[body_off + 40] as i8;
    let sz_exp = raw[body_off + 41] as i8;
    let seq_id = read_i64_le(raw, body_off + 42)?;
    let sts_us = read_i64_le(raw, body_off + 50)?;
    // category @ body_off+58 (u8); 5B padding 到 blockLength=64; 暂不用

    // 跳过 root padding，varString8: u8 length + UTF-8 bytes
    let sym_off = body_off + block_length;
    if raw.len() <= sym_off {
        bail!("Bitget SBE frame missing symbol length");
    }
    let sym_len = raw[sym_off] as usize;
    if raw.len() < sym_off + 1 + sym_len {
        bail!(
            "Bitget SBE frame truncated symbol: need {} have {}",
            sym_off + 1 + sym_len,
            raw.len()
        );
    }
    let symbol = std::str::from_utf8(&raw[sym_off + 1..sym_off + 1 + sym_len])
        .map_err(|e| anyhow!("Bitget SBE symbol not utf-8: {}", e))?
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
        ts_us: sts_us,
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
        bail!("Bitget SBE OOB read at offset {}", off);
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

    fn build_sbe_bbo_frame(
        ts_us: i64,
        bid_px_m: i64,
        bid_sz_m: i64,
        ask_px_m: i64,
        ask_sz_m: i64,
        px_exp: i8,
        sz_exp: i8,
        seq_id: i64,
        sts_us: i64,
        symbol: &str,
    ) -> Vec<u8> {
        let block_length: u16 = 64; // schema v3
        let mut buf = Vec::with_capacity(80);
        buf.extend_from_slice(&block_length.to_le_bytes());
        buf.extend_from_slice(&SBE_TEMPLATE_BOOKS1.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&3u16.to_le_bytes()); // version
        buf.extend_from_slice(&ts_us.to_le_bytes());
        buf.extend_from_slice(&bid_px_m.to_le_bytes());
        buf.extend_from_slice(&bid_sz_m.to_le_bytes());
        buf.extend_from_slice(&ask_px_m.to_le_bytes());
        buf.extend_from_slice(&ask_sz_m.to_le_bytes());
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf.extend_from_slice(&seq_id.to_le_bytes());
        buf.extend_from_slice(&sts_us.to_le_bytes());
        buf.push(1); // category
        // padding to blockLength=64: 当前 root 写了 8+8+8+8+8+1+1+8+8+1 = 59 bytes, 补 5 bytes
        buf.extend_from_slice(&[0u8; 5]);
        // varString8 symbol
        let sym_bytes = symbol.as_bytes();
        buf.push(sym_bytes.len() as u8);
        buf.extend_from_slice(sym_bytes);
        buf
    }

    #[test]
    fn decode_books1_with_negative_exponents() {
        // BTC: bid=77635.7, ask=77635.8; size 9.3708 / 3.3373
        let raw = build_sbe_bbo_frame(
            1_700_000_000_000_000,
            776_357, // bid_px_m  → 77635.7
            93_708,  // bid_sz_m  → 9.3708
            776_358, // ask_px_m  → 77635.8
            33_373,  // ask_sz_m  → 3.3373
            -1,
            -4,
            587_635_700_001,
            1_700_000_000_001_500,
            "BTCUSDT",
        );
        let frames = parse_sbe_books1(&raw).expect("decode ok");
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.seq_id, 587_635_700_001);
        assert_eq!(f.ts_us, 1_700_000_000_001_500); // 取 sts，不取 ts
        assert!((f.bid_price - 77635.7).abs() < 1e-6);
        assert!((f.ask_price - 77635.8).abs() < 1e-6);
        assert!((f.bid_amount - 9.3708).abs() < 1e-9);
        assert!((f.ask_amount - 3.3373).abs() < 1e-9);
    }

    #[test]
    fn rejects_unknown_schema() {
        let mut raw = build_sbe_bbo_frame(0, 1, 1, 1, 1, 0, 0, 1, 0, "BTCUSDT");
        raw[4] = 9; // schemaId
        let err = parse_sbe_books1(&raw).unwrap_err();
        assert!(err.to_string().contains("schemaId"));
    }

    #[test]
    fn unknown_template_returns_empty() {
        let mut raw = build_sbe_bbo_frame(0, 1, 1, 1, 1, 0, 0, 1, 0, "BTCUSDT");
        raw[2] = 0xE9;
        raw[3] = 0x03; // → 1001 (Depth50)
        assert!(parse_sbe_books1(&raw).unwrap().is_empty());
    }

    #[test]
    fn subscribe_uses_uta_v3_naming() {
        let a = BitgetAdapter::new(TradingVenue::BitgetFutures);
        let msgs = a.build_subscribe(&["BTCUSDT".to_string()]);
        assert_eq!(msgs.len(), 1);
        let arg = &msgs[0]["args"][0];
        assert_eq!(arg["instType"], "usdt-futures");
        assert_eq!(arg["topic"], "books1");
        assert_eq!(arg["symbol"], "BTCUSDT");
        // 关键：不应该有 v2 风格的 channel/instId
        assert!(arg.get("channel").is_none());
        assert!(arg.get("instId").is_none());

        let a_spot = BitgetAdapter::new(TradingVenue::BitgetMargin);
        let msgs_spot = a_spot.build_subscribe(&["BTCUSDT".to_string()]);
        assert_eq!(msgs_spot[0]["args"][0]["instType"], "spot");
    }

    #[test]
    fn subscribe_chunks_50() {
        let a = BitgetAdapter::new(TradingVenue::BitgetFutures);
        let symbols: Vec<String> = (0..120).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["args"].as_array().unwrap().len(), 50);
    }
}
