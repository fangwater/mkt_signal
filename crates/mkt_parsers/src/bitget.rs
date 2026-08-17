use serde_json::Value;
use std::fmt;

pub const SBE_HEADER_SIZE: usize = 8;
pub const SBE_SCHEMA_ID: u16 = 1;
pub const SBE_TEMPLATE_BOOKS50: u16 = 1001;
pub const SBE_TEMPLATE_BOOKS1: u16 = 1002;
pub const SBE_TEMPLATE_PUBLIC_TRADE: u16 = 1003;

const SBE_BOOKS1_ROOT_MIN: usize = 59;
const SBE_BOOKS50_ROOT_MIN: usize = 18;
const SBE_BOOKS50_LEVEL_MIN: usize = 16;
const SBE_PUBLIC_TRADE_ROOT_MIN: usize = 10;
const SBE_PUBLIC_TRADE_ENTRY_MIN: usize = 33;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    message: String,
}

impl DecodeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DecodeError {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Level {
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Bbo {
    pub symbol: String,
    pub timestamp_us: i64,
    pub seq_id: i64,
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    pub symbol: String,
    pub timestamp_us: i64,
    pub seq_id: i64,
    pub trade_id: i64,
    pub side: char,
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Book {
    pub symbol: String,
    pub timestamp_us: i64,
    pub seq_id: i64,
    pub prev_seq_id: i64,
    pub first_update_id: i64,
    pub final_update_id: i64,
    pub gap_check: bool,
    pub is_snapshot: bool,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Derivative {
    MarkPrice {
        symbol: String,
        price: f64,
        timestamp_us: i64,
    },
    IndexPrice {
        symbol: String,
        price: f64,
        timestamp_us: i64,
    },
    FundingRate {
        symbol: String,
        funding_rate: f64,
        next_funding_time_us: i64,
        timestamp_us: i64,
    },
}

pub fn parse_bbo_v2_json(value: &Value) -> Vec<Bbo> {
    let arg = match value.get("arg").and_then(|v| v.as_object()) {
        Some(arg) => arg,
        None => return Vec::new(),
    };
    if arg.get("channel").and_then(|v| v.as_str()) != Some("books1") {
        return Vec::new();
    }
    let Some(symbol) = arg
        .get("instId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
    else {
        return Vec::new();
    };
    let Some(data) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };

    let mut out = Vec::with_capacity(data.len());
    for item in data {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(bid) = obj
            .get("bids")
            .and_then(|v| v.as_array())
            .and_then(|levels| levels.first())
            .and_then(parse_level)
        else {
            continue;
        };
        let Some(ask) = obj
            .get("asks")
            .and_then(|v| v.as_array())
            .and_then(|levels| levels.first())
            .and_then(parse_level)
        else {
            continue;
        };
        if bid.price <= 0.0 || bid.amount <= 0.0 || ask.price <= 0.0 || ask.amount <= 0.0 {
            continue;
        }
        let timestamp_us = obj
            .get("ts")
            .and_then(parse_i64_loose)
            .or_else(|| value.get("ts").and_then(parse_i64_loose))
            .map(normalize_ts_to_us)
            .unwrap_or(0);
        let seq_id = obj.get("seq").and_then(parse_i64_loose).unwrap_or(0);
        out.push(Bbo {
            symbol: symbol.clone(),
            timestamp_us,
            seq_id,
            bid_price: bid.price,
            bid_amount: bid.amount,
            ask_price: ask.price,
            ask_amount: ask.amount,
        });
    }
    out
}

pub fn parse_incremental_v2_json(value: &Value) -> Vec<Book> {
    let action = value.get("action").and_then(|v| v.as_str()).unwrap_or("");
    if action != "snapshot" && action != "update" {
        return Vec::new();
    }
    let arg = match value.get("arg").and_then(|v| v.as_object()) {
        Some(arg) => arg,
        None => return Vec::new(),
    };
    if arg.get("channel").and_then(|v| v.as_str()) != Some("books") {
        return Vec::new();
    }
    let Some(symbol) = arg
        .get("instId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
    else {
        return Vec::new();
    };
    let Some(data) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
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
        let timestamp_us = obj
            .get("ts")
            .and_then(parse_i64_loose)
            .or_else(|| value.get("ts").and_then(parse_i64_loose))
            .map(normalize_ts_to_us)
            .unwrap_or(0);
        out.push(Book {
            symbol: symbol.clone(),
            timestamp_us,
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
    out
}

pub fn parse_derivatives_v2_json(value: &Value) -> Vec<Derivative> {
    let arg = match value.get("arg").and_then(|v| v.as_object()) {
        Some(arg) => arg,
        None => return Vec::new(),
    };
    if arg.get("channel").and_then(|v| v.as_str()) != Some("ticker")
        || arg.get("instType").and_then(|v| v.as_str()) != Some("USDT-FUTURES")
    {
        return Vec::new();
    }
    let Some(symbol) = arg
        .get("instId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
    else {
        return Vec::new();
    };
    let Some(data) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };

    let mut out = Vec::with_capacity(data.len() * 3);
    for item in data {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let timestamp_us = obj
            .get("ts")
            .and_then(parse_i64_loose)
            .or_else(|| value.get("ts").and_then(parse_i64_loose))
            .map(normalize_ts_to_us)
            .unwrap_or(0);
        if let (Some(funding_rate), Some(next_funding_time_us)) = (
            obj.get("fundingRate").and_then(parse_f64_loose),
            obj.get("nextFundingTime")
                .and_then(parse_i64_loose)
                .map(normalize_ts_to_us),
        ) {
            out.push(Derivative::FundingRate {
                symbol: symbol.clone(),
                funding_rate,
                next_funding_time_us,
                timestamp_us,
            });
        }
        if let Some(price) = obj.get("markPrice").and_then(parse_f64_loose) {
            if price > 0.0 {
                out.push(Derivative::MarkPrice {
                    symbol: symbol.clone(),
                    price,
                    timestamp_us,
                });
            }
        }
        if let Some(price) = obj.get("indexPrice").and_then(parse_f64_loose) {
            if price > 0.0 {
                out.push(Derivative::IndexPrice {
                    symbol: symbol.clone(),
                    price,
                    timestamp_us,
                });
            }
        }
    }
    out
}

pub fn parse_sbe_books1(raw: &[u8]) -> Result<Vec<Bbo>, DecodeError> {
    if raw.len() < SBE_HEADER_SIZE {
        return Err(DecodeError::new(format!(
            "Bitget SBE frame too short: {} bytes",
            raw.len()
        )));
    }
    let block_length = u16::from_le_bytes([raw[0], raw[1]]) as usize;
    let template_id = u16::from_le_bytes([raw[2], raw[3]]);
    let schema_id = u16::from_le_bytes([raw[4], raw[5]]);
    if schema_id != SBE_SCHEMA_ID {
        return Err(DecodeError::new(format!(
            "Bitget SBE unexpected schemaId={} (want {})",
            schema_id, SBE_SCHEMA_ID
        )));
    }
    if template_id != SBE_TEMPLATE_BOOKS1 {
        return Ok(Vec::new());
    }
    let body_off = SBE_HEADER_SIZE;
    if raw.len() < body_off + block_length {
        return Err(DecodeError::new(format!(
            "Bitget SBE frame truncated: have {} bytes, header says blockLength={}",
            raw.len(),
            block_length
        )));
    }
    if block_length < SBE_BOOKS1_ROOT_MIN {
        return Err(DecodeError::new(format!(
            "Bitget SBE books1 blockLength {} < expected {}",
            block_length, SBE_BOOKS1_ROOT_MIN
        )));
    }

    let bid_px_m = read_i64_le(raw, body_off + 8)?;
    let bid_sz_m = read_i64_le(raw, body_off + 16)?;
    let ask_px_m = read_i64_le(raw, body_off + 24)?;
    let ask_sz_m = read_i64_le(raw, body_off + 32)?;
    let px_exp = raw[body_off + 40] as i8;
    let sz_exp = raw[body_off + 41] as i8;
    let seq_id = read_i64_le(raw, body_off + 42)?;
    let timestamp_us = read_i64_le(raw, body_off + 50)?;

    let sym_off = body_off + block_length;
    if raw.len() <= sym_off {
        return Err(DecodeError::new("Bitget SBE frame missing symbol length"));
    }
    let sym_len = raw[sym_off] as usize;
    if raw.len() < sym_off + 1 + sym_len {
        return Err(DecodeError::new(format!(
            "Bitget SBE frame truncated symbol: need {} have {}",
            sym_off + 1 + sym_len,
            raw.len()
        )));
    }
    let symbol = std::str::from_utf8(&raw[sym_off + 1..sym_off + 1 + sym_len])
        .map_err(|e| DecodeError::new(format!("Bitget SBE symbol not utf-8: {}", e)))?
        .to_ascii_uppercase();

    let bid_price = mantissa_to_f64(bid_px_m, px_exp);
    let ask_price = mantissa_to_f64(ask_px_m, px_exp);
    let bid_amount = mantissa_to_f64(bid_sz_m, sz_exp);
    let ask_amount = mantissa_to_f64(ask_sz_m, sz_exp);
    if bid_price <= 0.0 || ask_price <= 0.0 || bid_amount <= 0.0 || ask_amount <= 0.0 {
        return Ok(Vec::new());
    }

    Ok(vec![Bbo {
        symbol,
        timestamp_us,
        seq_id,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    }])
}

/// SBE `books50` (templateId=1001) 固定 50 档整图。v2 root 无 sts；v3 起有 sts/category。
pub fn parse_sbe_books50(raw: &[u8]) -> Result<Vec<Book>, DecodeError> {
    if raw.len() < SBE_HEADER_SIZE {
        return Err(DecodeError::new(format!(
            "Bitget SBE frame too short: {} bytes",
            raw.len()
        )));
    }
    let block_length = u16::from_le_bytes([raw[0], raw[1]]) as usize;
    let template_id = u16::from_le_bytes([raw[2], raw[3]]);
    let schema_id = u16::from_le_bytes([raw[4], raw[5]]);
    if schema_id != SBE_SCHEMA_ID {
        return Err(DecodeError::new(format!(
            "Bitget SBE unexpected schemaId={} (want {})",
            schema_id, SBE_SCHEMA_ID
        )));
    }
    if template_id != SBE_TEMPLATE_BOOKS50 {
        return Ok(Vec::new());
    }
    let body_off = SBE_HEADER_SIZE;
    if raw.len() < body_off + block_length {
        return Err(DecodeError::new(format!(
            "Bitget SBE books50 truncated: have {} bytes, header says blockLength={}",
            raw.len(),
            block_length
        )));
    }
    if block_length < SBE_BOOKS50_ROOT_MIN {
        return Err(DecodeError::new(format!(
            "Bitget SBE books50 blockLength {} < expected {}",
            block_length, SBE_BOOKS50_ROOT_MIN
        )));
    }

    let match_ts = read_i64_le(raw, body_off)?;
    let seq_id = read_i64_le(raw, body_off + 8)?;
    let px_exp = raw[body_off + 16] as i8;
    let sz_exp = raw[body_off + 17] as i8;
    let timestamp_us = if block_length >= 27 {
        read_i64_le(raw, body_off + 18)?
    } else {
        match_ts
    };

    let mut off = body_off + block_length;
    let (asks, next) = decode_sbe_level_group(raw, off, px_exp, sz_exp)?;
    off = next;
    let (bids, next) = decode_sbe_level_group(raw, off, px_exp, sz_exp)?;
    off = next;
    if raw.len() <= off {
        return Err(DecodeError::new("Bitget SBE books50 missing symbol length"));
    }
    let sym_len = raw[off] as usize;
    if raw.len() < off + 1 + sym_len {
        return Err(DecodeError::new(format!(
            "Bitget SBE books50 truncated symbol: need {} have {}",
            off + 1 + sym_len,
            raw.len()
        )));
    }
    let symbol = std::str::from_utf8(&raw[off + 1..off + 1 + sym_len])
        .map_err(|e| DecodeError::new(format!("Bitget SBE books50 symbol not utf-8: {}", e)))?
        .to_ascii_uppercase();
    if bids.is_empty() || asks.is_empty() {
        return Ok(Vec::new());
    }

    Ok(vec![Book {
        symbol,
        timestamp_us,
        seq_id,
        prev_seq_id: i64::MIN,
        first_update_id: seq_id,
        final_update_id: seq_id,
        gap_check: false,
        is_snapshot: true,
        bids,
        asks,
    }])
}

fn decode_sbe_level_group(
    raw: &[u8],
    off: usize,
    px_exp: i8,
    sz_exp: i8,
) -> Result<(Vec<Level>, usize), DecodeError> {
    if raw.len() < off + 4 {
        return Err(DecodeError::new("Bitget SBE books50 missing group header"));
    }
    let entry_block_len = u16::from_le_bytes([raw[off], raw[off + 1]]) as usize;
    let num_in_group = u16::from_le_bytes([raw[off + 2], raw[off + 3]]) as usize;
    if entry_block_len < SBE_BOOKS50_LEVEL_MIN {
        return Err(DecodeError::new(format!(
            "Bitget SBE books50 entryBlockLength {} < expected {}",
            entry_block_len, SBE_BOOKS50_LEVEL_MIN
        )));
    }
    let entries_off = off + 4;
    let entries_total = entry_block_len.saturating_mul(num_in_group);
    if raw.len() < entries_off + entries_total {
        return Err(DecodeError::new(format!(
            "Bitget SBE books50 group truncated: need {} have {}",
            entries_off + entries_total,
            raw.len()
        )));
    }
    let mut levels = Vec::with_capacity(num_in_group);
    for idx in 0..num_in_group {
        let entry_off = entries_off + idx * entry_block_len;
        let price_m = read_i64_le(raw, entry_off)?;
        let size_m = read_i64_le(raw, entry_off + 8)?;
        let price = mantissa_to_f64(price_m, px_exp);
        let amount = mantissa_to_f64(size_m, sz_exp);
        if price > 0.0 {
            levels.push(Level { price, amount });
        }
    }
    Ok((levels, entries_off + entries_total))
}

pub fn parse_sbe_public_trades(raw: &[u8]) -> Result<Vec<Trade>, DecodeError> {
    if raw.len() < SBE_HEADER_SIZE {
        return Err(DecodeError::new(format!(
            "Bitget SBE frame too short: {} bytes",
            raw.len()
        )));
    }
    let block_length = u16::from_le_bytes([raw[0], raw[1]]) as usize;
    let template_id = u16::from_le_bytes([raw[2], raw[3]]);
    let schema_id = u16::from_le_bytes([raw[4], raw[5]]);
    if schema_id != SBE_SCHEMA_ID {
        return Err(DecodeError::new(format!(
            "Bitget SBE unexpected schemaId={} (want {})",
            schema_id, SBE_SCHEMA_ID
        )));
    }
    if template_id != SBE_TEMPLATE_PUBLIC_TRADE {
        return Ok(Vec::new());
    }

    let body_off = SBE_HEADER_SIZE;
    if raw.len() < body_off + block_length {
        return Err(DecodeError::new(format!(
            "Bitget SBE trade frame truncated: have {} bytes, header says blockLength={}",
            raw.len(),
            block_length
        )));
    }
    if block_length < SBE_PUBLIC_TRADE_ROOT_MIN {
        return Err(DecodeError::new(format!(
            "Bitget SBE trade blockLength {} < expected {}",
            block_length, SBE_PUBLIC_TRADE_ROOT_MIN
        )));
    }

    let px_exp = raw[body_off] as i8;
    let sz_exp = raw[body_off + 1] as i8;
    let grp_off = body_off + block_length;
    if raw.len() < grp_off + 4 {
        return Err(DecodeError::new(
            "Bitget SBE trade frame missing group header",
        ));
    }
    let entry_block_len = u16::from_le_bytes([raw[grp_off], raw[grp_off + 1]]) as usize;
    let num_in_group = u16::from_le_bytes([raw[grp_off + 2], raw[grp_off + 3]]) as usize;
    if entry_block_len < SBE_PUBLIC_TRADE_ENTRY_MIN {
        return Err(DecodeError::new(format!(
            "Bitget SBE trade entryBlockLength {} < expected {}",
            entry_block_len, SBE_PUBLIC_TRADE_ENTRY_MIN
        )));
    }

    let entries_off = grp_off + 4;
    let entries_total = entry_block_len.saturating_mul(num_in_group);
    if raw.len() < entries_off + entries_total {
        return Err(DecodeError::new(format!(
            "Bitget SBE trade entries truncated: need {} have {}",
            entries_off + entries_total,
            raw.len()
        )));
    }

    let sym_off = entries_off + entries_total;
    if raw.len() <= sym_off {
        return Err(DecodeError::new("Bitget SBE trade missing symbol length"));
    }
    let sym_len = raw[sym_off] as usize;
    if raw.len() < sym_off + 1 + sym_len {
        return Err(DecodeError::new(format!(
            "Bitget SBE trade truncated symbol: need {} have {}",
            sym_off + 1 + sym_len,
            raw.len()
        )));
    }
    let symbol = std::str::from_utf8(&raw[sym_off + 1..sym_off + 1 + sym_len])
        .map_err(|e| DecodeError::new(format!("Bitget SBE trade symbol not utf-8: {}", e)))?
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
            _ => continue,
        };
        let price = mantissa_to_f64(price_m, px_exp);
        let amount = mantissa_to_f64(size_m, sz_exp);
        if price <= 0.0 || amount <= 0.0 {
            continue;
        }
        out.push(Trade {
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

pub fn normalize_ts_to_us(timestamp: i64) -> i64 {
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

pub fn us_to_ms(timestamp_us: i64) -> i64 {
    timestamp_us / 1000
}

fn parse_level_array(levels: &[Value]) -> Vec<Level> {
    levels.iter().filter_map(parse_level).collect()
}

fn parse_level(value: &Value) -> Option<Level> {
    let arr = value.as_array()?;
    if arr.len() < 2 {
        return None;
    }
    let price = parse_f64_loose(&arr[0])?;
    let amount = parse_f64_loose(&arr[1])?;
    if price > 0.0 {
        Some(Level { price, amount })
    } else {
        None
    }
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

fn parse_f64_loose(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    v.as_str().and_then(|s| s.parse::<f64>().ok())
}

fn read_i64_le(buf: &[u8], off: usize) -> Result<i64, DecodeError> {
    if buf.len() < off + 8 {
        return Err(DecodeError::new(format!(
            "Bitget SBE OOB read at offset {}",
            off
        )));
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
pub mod test_helpers {
    use super::*;

    pub fn build_sbe_bbo_frame(
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
        let block_length: u16 = 64;
        let mut buf = Vec::with_capacity(80);
        buf.extend_from_slice(&block_length.to_le_bytes());
        buf.extend_from_slice(&SBE_TEMPLATE_BOOKS1.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&3u16.to_le_bytes());
        buf.extend_from_slice(&ts_us.to_le_bytes());
        buf.extend_from_slice(&bid_px_m.to_le_bytes());
        buf.extend_from_slice(&bid_sz_m.to_le_bytes());
        buf.extend_from_slice(&ask_px_m.to_le_bytes());
        buf.extend_from_slice(&ask_sz_m.to_le_bytes());
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf.extend_from_slice(&seq_id.to_le_bytes());
        buf.extend_from_slice(&sts_us.to_le_bytes());
        buf.push(1);
        buf.extend_from_slice(&[0u8; 5]);
        let sym_bytes = symbol.as_bytes();
        buf.push(sym_bytes.len() as u8);
        buf.extend_from_slice(sym_bytes);
        buf
    }

    pub fn build_sbe_books50_frame(
        ts_us: i64,
        seq_id: i64,
        px_exp: i8,
        sz_exp: i8,
        sts_us: i64,
        asks: &[(i64, i64)],
        bids: &[(i64, i64)],
        symbol: &str,
    ) -> Vec<u8> {
        let block_length: u16 = 32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&block_length.to_le_bytes());
        buf.extend_from_slice(&SBE_TEMPLATE_BOOKS50.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&3u16.to_le_bytes());
        buf.extend_from_slice(&ts_us.to_le_bytes());
        buf.extend_from_slice(&seq_id.to_le_bytes());
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf.extend_from_slice(&sts_us.to_le_bytes());
        buf.push(1);
        buf.extend_from_slice(&[0u8; 5]);
        buf.extend_from_slice(&16u16.to_le_bytes());
        buf.extend_from_slice(&(asks.len() as u16).to_le_bytes());
        for (price_m, size_m) in asks {
            buf.extend_from_slice(&price_m.to_le_bytes());
            buf.extend_from_slice(&size_m.to_le_bytes());
        }
        buf.extend_from_slice(&16u16.to_le_bytes());
        buf.extend_from_slice(&(bids.len() as u16).to_le_bytes());
        for (price_m, size_m) in bids {
            buf.extend_from_slice(&price_m.to_le_bytes());
            buf.extend_from_slice(&size_m.to_le_bytes());
        }
        let sym_bytes = symbol.as_bytes();
        buf.push(sym_bytes.len() as u8);
        buf.extend_from_slice(sym_bytes);
        buf
    }

    pub fn build_sbe_trade_frame(
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
        buf.extend_from_slice(&1_700_000_000_000_500i64.to_le_bytes());
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
}

#[cfg(test)]
mod tests {
    use super::test_helpers::*;
    use super::*;

    #[test]
    fn parses_books1_with_negative_exponents() {
        let raw = build_sbe_bbo_frame(
            1_700_000_000_000_000,
            776_357,
            93_708,
            776_358,
            33_373,
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
        assert_eq!(f.timestamp_us, 1_700_000_000_001_500);
        assert!((f.bid_price - 77635.7).abs() < 1e-6);
        assert!((f.ask_price - 77635.8).abs() < 1e-6);
        assert!((f.bid_amount - 9.3708).abs() < 1e-9);
        assert!((f.ask_amount - 3.3373).abs() < 1e-9);
    }

    #[test]
    fn parses_books50_snapshot_with_sts() {
        let raw = build_sbe_books50_frame(
            1_700_000_000_000_000,
            42,
            -2,
            -4,
            1_700_000_000_001_000,
            &[(6_566_578, 10_000), (6_566_678, 20_000)],
            &[(6_566_478, 30_000), (6_566_378, 40_000)],
            "BTCUSDT",
        );
        let books = parse_sbe_books50(&raw).expect("decode books50");
        assert_eq!(books.len(), 1);
        let book = &books[0];
        assert_eq!(book.symbol, "BTCUSDT");
        assert!(book.is_snapshot);
        assert_eq!(book.seq_id, 42);
        assert_eq!(book.timestamp_us, 1_700_000_000_001_000);
        assert_eq!(book.asks.len(), 2);
        assert_eq!(book.bids.len(), 2);
        assert!((book.asks[0].price - 65665.78).abs() < 1e-9);
        assert!((book.asks[0].amount - 1.0).abs() < 1e-9);
        assert!((book.bids[0].price - 65664.78).abs() < 1e-9);
        assert!((book.bids[1].amount - 4.0).abs() < 1e-9);
    }

    #[test]
    fn parses_public_trades_with_microsecond_ts() {
        let raw = build_sbe_trade_frame(
            -1,
            -4,
            &[
                (1_700_000_000_123_456, 9001, 776_357, 93_708, 0),
                (1_700_000_000_123_789, 9002, 776_358, 33_373, 1),
            ],
            "BTCUSDT",
        );
        let trades = parse_sbe_public_trades(&raw).expect("decode trade ok");
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
    fn parses_v2_bbo_incremental_and_derivatives_as_us() {
        let bbo = serde_json::json!({
            "action": "snapshot",
            "arg": {"instType": "USDT-FUTURES", "channel": "books1", "instId": "BTCUSDT"},
            "data": [{"ts": "1700000000123", "seq": "7", "bids": [["100", "1"]], "asks": [["101", "2"]]}]
        });
        let bbo = parse_bbo_v2_json(&bbo);
        assert_eq!(bbo.len(), 1);
        assert_eq!(bbo[0].timestamp_us, 1_700_000_000_123_000);
        assert_eq!(bbo[0].seq_id, 7);

        let inc = serde_json::json!({
            "action": "update",
            "arg": {"instType": "USDT-FUTURES", "channel": "books", "instId": "BTCUSDT"},
            "data": [{"ts": "1700000000123", "seq": 9001, "bids": [["100", "1"]], "asks": [["101", "2"]]}]
        });
        let books = parse_incremental_v2_json(&inc);
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].timestamp_us, 1_700_000_000_123_000);
        assert_eq!(books[0].bids.len(), 1);
        assert!(!books[0].gap_check);

        let derivatives = serde_json::json!({
            "arg": {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
            "data": [{"ts": "1700000000123", "fundingRate": "0.0001", "nextFundingTime": "1700003600000", "markPrice": "100.1", "indexPrice": "99.9"}]
        });
        let out = parse_derivatives_v2_json(&derivatives);
        assert_eq!(out.len(), 3);
        assert!(matches!(
            &out[0],
            Derivative::FundingRate {
                timestamp_us: 1_700_000_000_123_000,
                next_funding_time_us: 1_700_003_600_000_000,
                ..
            }
        ));
    }

    #[test]
    fn unknown_schema_errors_and_unknown_template_is_empty() {
        let mut raw = build_sbe_bbo_frame(0, 1, 1, 1, 1, 0, 0, 1, 0, "BTCUSDT");
        raw[4] = 9;
        let err = parse_sbe_books1(&raw).unwrap_err();
        assert!(err.to_string().contains("schemaId"));

        let mut raw = build_sbe_bbo_frame(0, 1, 1, 1, 1, 0, 0, 1, 0, "BTCUSDT");
        raw[2] = 0xE9;
        raw[3] = 0x03;
        assert!(parse_sbe_books1(&raw).unwrap().is_empty());
    }
}
