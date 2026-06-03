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
use bytes::Bytes;
use serde_json::Value;
use std::time::Duration;

use crate::common::mkt_msg::{FundingRateMsg, IndexPriceMsg, Level, MarkPriceMsg};
use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{
    BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};

const BITGET_SBE_WS_URL: &str = "wss://ws.bitget.com/v3/ws/public/sbe";
const BITGET_V2_WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const BITGET_SUBSCRIBE_CHUNK: usize = 50;

const SBE_HEADER_SIZE: usize = 8;
const SBE_SCHEMA_ID: u16 = 1;
const SBE_TEMPLATE_BOOKS1: u16 = 1002;
const SBE_TEMPLATE_PUBLIC_TRADE: u16 = 1003;
/// books1 root: 8*i64/u64 + 2*i8 + u8 = 59 字节(无 padding)，blockLength=64 含 5B padding。
const SBE_BOOKS1_ROOT_MIN: usize = 59;
/// publicTrade root: px_exp i8 + sz_exp i8 + sts u64 + padding, blockLength=16.
const SBE_PUBLIC_TRADE_ROOT_MIN: usize = 10;
const SBE_PUBLIC_TRADE_ENTRY_MIN: usize = 33;

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
        self.build_channel_subscribe(symbols, "books1")
    }

    fn build_trade_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_channel_subscribe(symbols, "publicTrade")
    }

    fn build_incremental_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_v2_subscribe(self.venue, symbols, "books")
    }

    fn build_derivatives_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        if self.venue != TradingVenue::BitgetFutures {
            return Vec::new();
        }
        build_v2_subscribe(self.venue, symbols, "ticker")
    }

    fn incremental_ws_url(&self) -> Option<String> {
        Some(BITGET_V2_WS_URL.to_string())
    }

    fn derivatives_ws_url(&self) -> Option<String> {
        if self.venue == TradingVenue::BitgetFutures {
            Some(BITGET_V2_WS_URL.to_string())
        } else {
            None
        }
    }

    fn parse_frame(&self, _value: &Value) -> Result<Vec<BboFrame>> {
        // SBE 端的 text 帧只有 subscribe ack / error event; 静默忽略
        Ok(Vec::new())
    }

    fn parse_incremental_frame(&self, value: &Value) -> Result<Vec<IncrementalFrame>> {
        parse_v2_incremental(value)
    }

    fn parse_derivatives_frame(&self, value: &Value) -> Result<Vec<Bytes>> {
        parse_v2_derivatives(value)
    }

    fn parse_binary_frame(&self, raw: &[u8]) -> Result<Vec<BboFrame>> {
        parse_sbe_books1(raw)
    }

    fn parse_trade_binary_frame(&self, raw: &[u8]) -> Result<Vec<TradeFrame>> {
        parse_sbe_public_trade(raw)
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        // text "ping" 每 25s; pong 文本 ws.rs::is_keepalive_response 已识别跳过
        Some(KeepaliveSpec::text(Duration::from_secs(25), "ping"))
    }
}

fn v2_inst_type(venue: TradingVenue) -> &'static str {
    match venue {
        TradingVenue::BitgetMargin => "SPOT",
        TradingVenue::BitgetFutures => "USDT-FUTURES",
        other => unreachable!("Bitget v2 called for non-bitget venue: {:?}", other),
    }
}

fn build_v2_subscribe(venue: TradingVenue, symbols: &[String], channel: &str) -> Vec<Value> {
    let inst_type = v2_inst_type(venue);
    let mut out = Vec::new();
    for chunk in symbols.chunks(BITGET_SUBSCRIBE_CHUNK.max(1)) {
        let args: Vec<Value> = chunk
            .iter()
            .map(|symbol| {
                serde_json::json!({
                    "instType": inst_type,
                    "channel": channel,
                    "instId": symbol,
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

impl BitgetAdapter {
    fn build_channel_subscribe(&self, symbols: &[String], topic: &str) -> Vec<Value> {
        let chunk_size = BITGET_SUBSCRIBE_CHUNK.max(1);
        let inst_type = self.inst_type();
        let mut out = Vec::new();
        for chunk in symbols.chunks(chunk_size) {
            let args: Vec<Value> = chunk
                .iter()
                .map(|sym| {
                    serde_json::json!({
                        "instType": inst_type,
                        "topic": topic,
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
}

fn parse_v2_incremental(value: &Value) -> Result<Vec<IncrementalFrame>> {
    let action = value.get("action").and_then(|v| v.as_str()).unwrap_or("");
    if action != "snapshot" && action != "update" {
        return Ok(Vec::new());
    }
    let arg = match value.get("arg").and_then(|v| v.as_object()) {
        Some(arg) => arg,
        None => return Ok(Vec::new()),
    };
    if arg.get("channel").and_then(|v| v.as_str()) != Some("books") {
        return Ok(Vec::new());
    }
    let symbol = match arg.get("instId").and_then(|v| v.as_str()) {
        Some(symbol) => symbol.to_ascii_uppercase(),
        None => return Ok(Vec::new()),
    };
    let data = match value.get("data").and_then(|v| v.as_array()) {
        Some(data) => data,
        None => return Ok(Vec::new()),
    };

    let mut out = Vec::with_capacity(data.len());
    for item in data {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let bids = obj
            .get("bids")
            .and_then(|v| v.as_array())
            .map(|levels| parse_level_array(levels))
            .unwrap_or_default();
        let asks = obj
            .get("asks")
            .and_then(|v| v.as_array())
            .map(|levels| parse_level_array(levels))
            .unwrap_or_default();
        if bids.is_empty() && asks.is_empty() {
            continue;
        }
        let seq_id = obj.get("seq").and_then(parse_i64_loose).unwrap_or(0);
        let timestamp = obj
            .get("ts")
            .and_then(parse_i64_loose)
            .or_else(|| value.get("ts").and_then(parse_i64_loose))
            .map(normalize_ts_to_us)
            .unwrap_or(0);
        out.push(IncrementalFrame::Book {
            symbol: symbol.clone(),
            timestamp,
            seq_id,
            prev_seq_id: i64::MIN,
            first_update_id: seq_id,
            final_update_id: seq_id,
            gap_check: false,
            is_snapshot: action == "snapshot",
            bids,
            asks,
        });
    }
    Ok(out)
}

fn parse_v2_derivatives(value: &Value) -> Result<Vec<Bytes>> {
    let arg = match value.get("arg").and_then(|v| v.as_object()) {
        Some(arg) => arg,
        None => return Ok(Vec::new()),
    };
    if arg.get("channel").and_then(|v| v.as_str()) != Some("ticker")
        || arg.get("instType").and_then(|v| v.as_str()) != Some("USDT-FUTURES")
    {
        return Ok(Vec::new());
    }
    let symbol = match arg.get("instId").and_then(|v| v.as_str()) {
        Some(symbol) => symbol.to_string(),
        None => return Ok(Vec::new()),
    };
    let data = match value.get("data").and_then(|v| v.as_array()) {
        Some(data) => data,
        None => return Ok(Vec::new()),
    };

    let mut out = Vec::new();
    for item in data {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let timestamp = obj
            .get("ts")
            .and_then(parse_i64_loose)
            .or_else(|| value.get("ts").and_then(parse_i64_loose))
            .map(normalize_ts_to_us)
            .unwrap_or(0);
        if let (Some(funding_rate), Some(next_funding_time)) = (
            obj.get("fundingRate").and_then(parse_f64_loose),
            obj.get("nextFundingTime")
                .and_then(parse_i64_loose)
                .map(normalize_ts_to_us),
        ) {
            out.push(
                FundingRateMsg::create(symbol.clone(), funding_rate, next_funding_time, timestamp)
                    .to_bytes(),
            );
        }
        if let Some(mark_price) = obj.get("markPrice").and_then(parse_f64_loose) {
            if mark_price > 0.0 {
                out.push(MarkPriceMsg::create(symbol.clone(), mark_price, timestamp).to_bytes());
            }
        }
        if let Some(index_price) = obj.get("indexPrice").and_then(parse_f64_loose) {
            if index_price > 0.0 {
                out.push(IndexPriceMsg::create(symbol.clone(), index_price, timestamp).to_bytes());
            }
        }
    }
    Ok(out)
}

fn parse_level_array(levels: &[Value]) -> Vec<Level> {
    levels
        .iter()
        .filter_map(|level| {
            let arr = level.as_array()?;
            if arr.len() < 2 {
                return None;
            }
            let price = arr[0].as_str()?.parse::<f64>().ok()?;
            let amount = arr[1].as_str()?.parse::<f64>().ok()?;
            if price > 0.0 {
                Some(Level::from_values(price, amount))
            } else {
                None
            }
        })
        .collect()
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

/// SBE publicTrade (templateId=1003) 解码。其他 template 直接返回空 Vec。
fn parse_sbe_public_trade(raw: &[u8]) -> Result<Vec<TradeFrame>> {
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
    if template_id != SBE_TEMPLATE_PUBLIC_TRADE {
        return Ok(Vec::new());
    }

    let body_off = SBE_HEADER_SIZE;
    if raw.len() < body_off + block_length {
        bail!(
            "Bitget SBE trade frame truncated: have {} bytes, header says blockLength={}",
            raw.len(),
            block_length
        );
    }
    if block_length < SBE_PUBLIC_TRADE_ROOT_MIN {
        bail!(
            "Bitget SBE trade blockLength {} < expected {}",
            block_length,
            SBE_PUBLIC_TRADE_ROOT_MIN
        );
    }

    let px_exp = raw[body_off] as i8;
    let sz_exp = raw[body_off + 1] as i8;

    let grp_off = body_off + block_length;
    if raw.len() < grp_off + 4 {
        bail!("Bitget SBE trade frame missing group header");
    }
    let entry_block_len = u16::from_le_bytes([raw[grp_off], raw[grp_off + 1]]) as usize;
    let num_in_group = u16::from_le_bytes([raw[grp_off + 2], raw[grp_off + 3]]) as usize;
    if entry_block_len < SBE_PUBLIC_TRADE_ENTRY_MIN {
        bail!(
            "Bitget SBE trade entryBlockLength {} < expected {}",
            entry_block_len,
            SBE_PUBLIC_TRADE_ENTRY_MIN
        );
    }

    let entries_off = grp_off + 4;
    let entries_total = entry_block_len.saturating_mul(num_in_group);
    if raw.len() < entries_off + entries_total {
        bail!(
            "Bitget SBE trade entries truncated: need {} have {}",
            entries_off + entries_total,
            raw.len()
        );
    }

    let sym_off = entries_off + entries_total;
    if raw.len() <= sym_off {
        bail!("Bitget SBE trade missing symbol length");
    }
    let sym_len = raw[sym_off] as usize;
    if raw.len() < sym_off + 1 + sym_len {
        bail!(
            "Bitget SBE trade truncated symbol: need {} have {}",
            sym_off + 1 + sym_len,
            raw.len()
        );
    }
    let symbol = std::str::from_utf8(&raw[sym_off + 1..sym_off + 1 + sym_len])
        .map_err(|e| anyhow!("Bitget SBE trade symbol not utf-8: {}", e))?
        .to_ascii_uppercase();

    let mut out = Vec::with_capacity(num_in_group);
    for idx in 0..num_in_group {
        let off = entries_off + idx * entry_block_len;
        let timestamp_us = read_i64_le(raw, off)?;
        let trade_id = read_i64_le(raw, off + 8)?;
        let price_m = read_i64_le(raw, off + 16)?;
        let size_m = read_i64_le(raw, off + 24)?;
        let side = match raw[off + 32] {
            0 => 'B',
            1 => 'S',
            other => {
                log::warn!("Bitget SBE trade unknown side={} dropped", other);
                continue;
            }
        };
        let price = mantissa_to_f64(price_m, px_exp);
        let amount = mantissa_to_f64(size_m, sz_exp);
        if price <= 0.0 || amount <= 0.0 {
            continue;
        }
        out.push(TradeFrame {
            symbol: symbol.clone(),
            timestamp_us,
            seq_id: trade_id,
            trade_id,
            side,
            price,
            amount,
        });
    }

    Ok(out)
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

fn parse_i64_loose(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return i64::try_from(n).ok();
    }
    if let Some(f) = v.as_f64() {
        return Some(f as i64);
    }
    v.as_str().and_then(|s| {
        s.parse::<i64>()
            .ok()
            .or_else(|| s.parse::<f64>().ok().map(|f| f as i64))
    })
}

fn normalize_ts_to_us(timestamp: i64) -> i64 {
    let abs = timestamp.abs();
    if abs >= 1_000_000_000_000_000_000 {
        timestamp / 1000
    } else if abs >= 1_000_000_000_000_000 {
        timestamp
    } else if abs >= 1_000_000_000_000 {
        timestamp.saturating_mul(1000)
    } else {
        timestamp.saturating_mul(1_000_000)
    }
}

fn parse_f64_loose(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    v.as_str().and_then(|s| s.parse::<f64>().ok())
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

    fn build_sbe_trade_frame(
        px_exp: i8,
        sz_exp: i8,
        entries: &[(i64, i64, i64, i64, u8)],
        symbol: &str,
    ) -> Vec<u8> {
        let block_length: u16 = 16;
        let entry_block_len: u16 = 40;
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(&block_length.to_le_bytes());
        buf.extend_from_slice(&SBE_TEMPLATE_PUBLIC_TRADE.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&3u16.to_le_bytes());
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf.extend_from_slice(&1_700_000_000_000_500i64.to_le_bytes()); // sts
        buf.extend_from_slice(&[0u8; 6]);
        buf.extend_from_slice(&entry_block_len.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        for (ts_us, exec_id, price_m, size_m, side) in entries {
            buf.extend_from_slice(&ts_us.to_le_bytes());
            buf.extend_from_slice(&exec_id.to_le_bytes());
            buf.extend_from_slice(&price_m.to_le_bytes());
            buf.extend_from_slice(&size_m.to_le_bytes());
            buf.push(*side);
            buf.extend_from_slice(&[0u8; 7]);
        }
        buf.push(symbol.len() as u8);
        buf.extend_from_slice(symbol.as_bytes());
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
    fn trade_subscribe_uses_public_trade_topic() {
        let a = BitgetAdapter::new(TradingVenue::BitgetFutures);
        let msgs = a.build_trade_subscribe(&["BTCUSDT".to_string()]);
        assert_eq!(msgs.len(), 1);
        let arg = &msgs[0]["args"][0];
        assert_eq!(arg["instType"], "usdt-futures");
        assert_eq!(arg["topic"], "publicTrade");
        assert_eq!(arg["symbol"], "BTCUSDT");
    }

    #[test]
    fn decodes_public_trade_with_microsecond_ts() {
        let raw = build_sbe_trade_frame(
            -1,
            -4,
            &[
                (1_700_000_000_123_456, 9001, 776_357, 93_708, 0),
                (1_700_000_000_123_789, 9002, 776_358, 33_373, 1),
            ],
            "BTCUSDT",
        );
        let trades = parse_sbe_public_trade(&raw).expect("decode trade ok");
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].symbol, "BTCUSDT");
        assert_eq!(trades[0].timestamp_us, 1_700_000_000_123_456);
        assert_eq!(trades[0].trade_id, 9001);
        assert_eq!(trades[0].seq_id, 9001);
        assert_eq!(trades[0].side, 'B');
        assert!((trades[0].price - 77635.7).abs() < 1e-6);
        assert!((trades[0].amount - 9.3708).abs() < 1e-9);
        assert_eq!(trades[1].side, 'S');
    }

    #[test]
    fn decodes_v2_incremental_with_microsecond_ts() {
        let raw = serde_json::json!({
            "action": "update",
            "arg": {"instType": "USDT-FUTURES", "channel": "books", "instId": "BTCUSDT"},
            "data": [{
                "ts": "1700000000123",
                "seq": 9001,
                "bids": [["100", "1"]],
                "asks": [["101", "2"]]
            }]
        });
        let frames = parse_v2_incremental(&raw).unwrap();
        assert_eq!(frames.len(), 1);
        let IncrementalFrame::Book {
            symbol,
            timestamp,
            seq_id,
            gap_check,
            bids,
            asks,
            ..
        } = &frames[0]
        else {
            panic!("expected book frame");
        };
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(*timestamp, 1_700_000_000_123_000);
        assert_eq!(*seq_id, 9001);
        assert!(!*gap_check);
        assert_eq!(bids.len(), 1);
        assert_eq!(asks.len(), 1);
    }

    #[test]
    fn decodes_v2_derivatives_with_microsecond_ts() {
        let raw = serde_json::json!({
            "arg": {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
            "data": [{
                "ts": "1700000000123",
                "fundingRate": "0.0001",
                "nextFundingTime": "1700003600000",
                "markPrice": "100.1",
                "indexPrice": "99.9"
            }]
        });
        let bytes = parse_v2_derivatives(&raw).unwrap();
        assert_eq!(bytes.len(), 3);
        assert_eq!(
            FundingRateMsg::get_timestamp(&bytes[0]),
            1_700_000_000_123_000
        );
        assert_eq!(
            FundingRateMsg::get_next_funding_time(&bytes[0]),
            1_700_003_600_000_000
        );
        assert_eq!(
            MarkPriceMsg::get_timestamp(&bytes[1]),
            1_700_000_000_123_000
        );
        assert_eq!(
            IndexPriceMsg::get_timestamp(&bytes[2]),
            1_700_000_000_123_000
        );
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
