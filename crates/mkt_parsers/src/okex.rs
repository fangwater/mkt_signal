use serde_json::Value;
use std::collections::HashSet;
use std::fmt;

pub const SBE_HEADER_SIZE: usize = 8;
pub const SBE_SCHEMA_ID: u16 = 1;
pub const SBE_TEMPLATE_BBO_TBT: u16 = 1000;
pub const SBE_TEMPLATE_BOOKS_L2_TBT: u16 = 1001;
pub const SBE_TEMPLATE_BOOKS_L2_TBT_EXPONENT: u16 = 1002;
pub const SBE_TEMPLATE_TRADES: u16 = 1005;
pub const SBE_TEMPLATE_BOOKS_SNAPSHOT: u16 = 1006;
pub const SBE_BBO_TBT_BLOCK_LENGTH: usize = 74;
pub const SBE_BOOKS_L2_TBT_BLOCK_LENGTH: usize = 42;
pub const SBE_BOOKS_EXPONENT_BLOCK_LENGTH: usize = 42;
pub const SBE_TRADES_BLOCK_LENGTH: usize = 62;
pub const SBE_BOOKS_SNAPSHOT_BLOCK_LENGTH: usize = 26;
pub const SBE_BOOK_LEVEL_MIN_BLOCK_LENGTH: usize = 20;

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
pub struct SbeBbo {
    pub inst_id_code: i64,
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
pub struct SbeTrade {
    pub inst_id_code: i64,
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
pub enum SbeBook {
    Book {
        inst_id_code: i64,
        timestamp_us: i64,
        seq_id: i64,
        prev_seq_id: i64,
        first_update_id: i64,
        final_update_id: i64,
        gap_check: bool,
        is_snapshot: bool,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    SequenceOnly {
        inst_id_code: i64,
        timestamp_us: i64,
        seq_id: i64,
        prev_seq_id: i64,
    },
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
    Liquidation {
        symbol: String,
        side: char,
        amount: f64,
        price: f64,
        timestamp_us: i64,
    },
}

pub fn normalize_okex_symbol(symbol: &str) -> String {
    let mut upper = symbol.to_ascii_uppercase();
    if upper.ends_with("-SWAP") && upper.len() > 5 {
        upper.truncate(upper.len() - 5);
    }
    upper.retain(|ch| ch != '-');
    upper
}

pub fn parse_bbo_json(value: &Value) -> Vec<Bbo> {
    let arg = match value.get("arg").and_then(|v| v.as_object()) {
        Some(arg) => arg,
        None => return Vec::new(),
    };
    if arg.get("channel").and_then(|v| v.as_str()) != Some("bbo-tbt") {
        return Vec::new();
    }
    let Some(symbol) = arg
        .get("instId")
        .and_then(|v| v.as_str())
        .map(normalize_okex_symbol)
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
            .map(normalize_ts_to_us)
            .unwrap_or(0);
        let seq_id = obj.get("seqId").and_then(parse_i64_loose).unwrap_or(0);
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

pub fn parse_trades_json(value: &Value) -> Vec<Trade> {
    let Some(data) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(data.len());
    for item in data {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(symbol) = obj
            .get("instId")
            .and_then(|v| v.as_str())
            .map(normalize_okex_symbol)
        else {
            continue;
        };
        let Some(trade_id) = obj.get("tradeId").and_then(parse_i64_loose) else {
            continue;
        };
        let Some(price) = obj.get("px").and_then(parse_f64_loose) else {
            continue;
        };
        let Some(amount) = obj.get("sz").and_then(parse_f64_loose) else {
            continue;
        };
        if price <= 0.0 || amount <= 0.0 {
            continue;
        }
        let side = match obj.get("side").and_then(|v| v.as_str()).unwrap_or("") {
            "sell" => 'S',
            "buy" => 'B',
            _ => continue,
        };
        let Some(timestamp_us) = obj
            .get("ts")
            .and_then(parse_i64_loose)
            .map(normalize_ts_to_us)
        else {
            continue;
        };
        out.push(Trade {
            symbol,
            timestamp_us,
            seq_id: trade_id,
            trade_id,
            side,
            price,
            amount,
        });
    }
    out
}

pub fn parse_incremental_json(value: &Value) -> Vec<Book> {
    let action = value.get("action").and_then(|v| v.as_str()).unwrap_or("");
    if action != "snapshot" && action != "update" {
        return Vec::new();
    }
    let arg = match value.get("arg").and_then(|v| v.as_object()) {
        Some(arg) => arg,
        None => return Vec::new(),
    };
    let channel = arg.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    if !channel.starts_with("books") {
        return Vec::new();
    }
    let Some(symbol) = arg
        .get("instId")
        .and_then(|v| v.as_str())
        .map(normalize_okex_symbol)
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
        let Some(seq_id) = obj.get("seqId").and_then(parse_i64_loose) else {
            continue;
        };
        let Some(prev_seq_id) = obj.get("prevSeqId").and_then(parse_i64_loose) else {
            continue;
        };
        let Some(timestamp_us) = obj
            .get("ts")
            .and_then(parse_i64_loose)
            .map(normalize_ts_to_us)
        else {
            continue;
        };
        out.push(Book {
            symbol: symbol.clone(),
            timestamp_us,
            seq_id,
            prev_seq_id,
            first_update_id: seq_id,
            final_update_id: prev_seq_id,
            gap_check: true,
            is_snapshot: action == "snapshot",
            bids,
            asks,
        });
    }
    out
}

pub fn parse_derivatives_json(
    value: &Value,
    active_symbols: Option<&HashSet<String>>,
) -> Vec<Derivative> {
    let Some(channel) = value
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(|v| v.as_str())
    else {
        return Vec::new();
    };

    match channel {
        "liquidation-orders" => parse_liquidation_json(value, active_symbols),
        "mark-price" => parse_mark_price_json(value),
        "funding-rate" => parse_funding_rate_json(value),
        "index-tickers" => parse_index_price_json(value),
        _ => Vec::new(),
    }
}

pub fn parse_sbe_bbo_tbt(raw: &[u8]) -> Result<Option<SbeBbo>, DecodeError> {
    let (block_length, template_id) = read_sbe_header(raw)?;
    if template_id != SBE_TEMPLATE_BBO_TBT {
        return Ok(None);
    }
    if block_length < SBE_BBO_TBT_BLOCK_LENGTH {
        return Err(DecodeError::new(format!(
            "OKEx SBE bbo blockLength {} < expected {}",
            block_length, SBE_BBO_TBT_BLOCK_LENGTH
        )));
    }

    let body_off = SBE_HEADER_SIZE;
    let inst_id_code = read_i64_le(raw, body_off)?;
    let _ts_us = read_i64_le(raw, body_off + 8)?;
    let out_time_us = read_i64_le(raw, body_off + 16)?;
    let seq_id = read_i64_le(raw, body_off + 24)?;
    let ask_px_m = read_i64_le(raw, body_off + 32)?;
    let ask_sz_m = read_i64_le(raw, body_off + 40)?;
    let bid_px_m = read_i64_le(raw, body_off + 48)?;
    let bid_sz_m = read_i64_le(raw, body_off + 56)?;
    let px_exp = raw[body_off + 72] as i8;
    let sz_exp = raw[body_off + 73] as i8;

    let bid_price = mantissa_to_f64(bid_px_m, px_exp);
    let ask_price = mantissa_to_f64(ask_px_m, px_exp);
    let bid_amount = mantissa_to_f64(bid_sz_m, sz_exp);
    let ask_amount = mantissa_to_f64(ask_sz_m, sz_exp);
    if bid_price <= 0.0 || ask_price <= 0.0 || bid_amount <= 0.0 || ask_amount <= 0.0 {
        return Ok(None);
    }

    Ok(Some(SbeBbo {
        inst_id_code,
        timestamp_us: out_time_us,
        seq_id,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    }))
}

pub fn parse_sbe_books(raw: &[u8]) -> Result<Vec<SbeBook>, DecodeError> {
    let (block_length, template_id) = read_sbe_header(raw)?;
    match template_id {
        SBE_TEMPLATE_BOOKS_L2_TBT => parse_sbe_books_l2_tbt(raw, block_length),
        SBE_TEMPLATE_BOOKS_L2_TBT_EXPONENT => parse_sbe_books_exponent_update(raw, block_length),
        SBE_TEMPLATE_BOOKS_SNAPSHOT => parse_sbe_books_snapshot(raw, block_length),
        _ => Ok(Vec::new()),
    }
}

pub fn parse_sbe_trades(raw: &[u8]) -> Result<Vec<SbeTrade>, DecodeError> {
    let (block_length, template_id) = read_sbe_header(raw)?;
    if template_id != SBE_TEMPLATE_TRADES {
        return Ok(Vec::new());
    }
    if block_length < SBE_TRADES_BLOCK_LENGTH {
        return Err(DecodeError::new(format!(
            "OKEx SBE trades blockLength {} < expected {}",
            block_length, SBE_TRADES_BLOCK_LENGTH
        )));
    }

    let body_off = SBE_HEADER_SIZE;
    let inst_id_code = read_i64_le(raw, body_off)?;
    let _ts_us = read_i64_le(raw, body_off + 8)?;
    let out_time_us = read_i64_le(raw, body_off + 16)?;
    let seq_id = read_i64_le(raw, body_off + 24)?;
    let px_m = read_i64_le(raw, body_off + 32)?;
    let sz_m = read_i64_le(raw, body_off + 40)?;
    let trade_id = read_i64_le(raw, body_off + 48)?;
    let _count = read_i16_le(raw, body_off + 56)?;
    let side = match raw[body_off + 58] as i8 {
        0 => 'S',
        1 => 'B',
        _ => return Ok(Vec::new()),
    };
    let px_exp = raw[body_off + 59] as i8;
    let sz_exp = raw[body_off + 60] as i8;
    let _source = raw[body_off + 61] as i8;

    let price = mantissa_to_f64(px_m, px_exp);
    let amount = mantissa_to_f64(sz_m, sz_exp);
    if price <= 0.0 || amount <= 0.0 || out_time_us <= 0 {
        return Ok(Vec::new());
    }

    Ok(vec![SbeTrade {
        inst_id_code,
        timestamp_us: out_time_us,
        seq_id,
        trade_id,
        side,
        price,
        amount,
    }])
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

fn parse_sbe_books_l2_tbt(raw: &[u8], block_length: usize) -> Result<Vec<SbeBook>, DecodeError> {
    if block_length < SBE_BOOKS_L2_TBT_BLOCK_LENGTH {
        return Err(DecodeError::new(format!(
            "OKEx SBE books blockLength {} < expected {}",
            block_length, SBE_BOOKS_L2_TBT_BLOCK_LENGTH
        )));
    }
    let body_off = SBE_HEADER_SIZE;
    let inst_id_code = read_i64_le(raw, body_off)?;
    let ts_us = read_i64_le(raw, body_off + 8)?;
    let _out_time_us = read_i64_le(raw, body_off + 16)?;
    let seq_id = read_i64_le(raw, body_off + 24)?;
    let prev_seq_id = read_i64_le(raw, body_off + 32)?;
    let px_exp = raw[body_off + 40] as i8;
    let sz_exp = raw[body_off + 41] as i8;
    let groups_off = SBE_HEADER_SIZE + block_length;
    let (asks, off) = parse_sbe_level_group(raw, groups_off, px_exp, sz_exp)?;
    let (bids, _off) = parse_sbe_level_group(raw, off, px_exp, sz_exp)?;

    Ok(vec![SbeBook::Book {
        inst_id_code,
        timestamp_us: ts_us,
        seq_id,
        prev_seq_id,
        first_update_id: seq_id,
        final_update_id: prev_seq_id,
        gap_check: true,
        is_snapshot: false,
        bids,
        asks,
    }])
}

fn parse_sbe_books_exponent_update(
    raw: &[u8],
    block_length: usize,
) -> Result<Vec<SbeBook>, DecodeError> {
    if block_length < SBE_BOOKS_EXPONENT_BLOCK_LENGTH {
        return Err(DecodeError::new(format!(
            "OKEx SBE books exponent blockLength {} < expected {}",
            block_length, SBE_BOOKS_EXPONENT_BLOCK_LENGTH
        )));
    }
    let body_off = SBE_HEADER_SIZE;
    let inst_id_code = read_i64_le(raw, body_off)?;
    let ts_us = read_i64_le(raw, body_off + 8)?;
    let _out_time_us = read_i64_le(raw, body_off + 16)?;
    let seq_id = read_i64_le(raw, body_off + 24)?;
    let prev_seq_id = read_i64_le(raw, body_off + 32)?;

    Ok(vec![SbeBook::SequenceOnly {
        inst_id_code,
        timestamp_us: ts_us,
        seq_id,
        prev_seq_id,
    }])
}

fn parse_sbe_books_snapshot(raw: &[u8], block_length: usize) -> Result<Vec<SbeBook>, DecodeError> {
    if block_length < SBE_BOOKS_SNAPSHOT_BLOCK_LENGTH {
        return Err(DecodeError::new(format!(
            "OKEx SBE books snapshot blockLength {} < expected {}",
            block_length, SBE_BOOKS_SNAPSHOT_BLOCK_LENGTH
        )));
    }
    let body_off = SBE_HEADER_SIZE;
    let inst_id_code = read_i64_le(raw, body_off)?;
    let ts_us = read_i64_le(raw, body_off + 8)?;
    let seq_id = read_i64_le(raw, body_off + 16)?;
    let px_exp = raw[body_off + 24] as i8;
    let sz_exp = raw[body_off + 25] as i8;
    let groups_off = SBE_HEADER_SIZE + block_length;
    let (asks, off) = parse_sbe_level_group(raw, groups_off, px_exp, sz_exp)?;
    let (bids, _off) = parse_sbe_level_group(raw, off, px_exp, sz_exp)?;

    Ok(vec![SbeBook::Book {
        inst_id_code,
        timestamp_us: ts_us,
        seq_id,
        prev_seq_id: -1,
        first_update_id: seq_id,
        final_update_id: -1,
        gap_check: false,
        is_snapshot: true,
        bids,
        asks,
    }])
}

fn parse_sbe_level_group(
    raw: &[u8],
    off: usize,
    px_exp: i8,
    sz_exp: i8,
) -> Result<(Vec<Level>, usize), DecodeError> {
    let block_length = read_u16_le(raw, off)? as usize;
    let num_in_group = read_u16_le(raw, off + 2)? as usize;
    if block_length < SBE_BOOK_LEVEL_MIN_BLOCK_LENGTH {
        return Err(DecodeError::new(format!(
            "OKEx SBE level group blockLength {} < expected {}",
            block_length, SBE_BOOK_LEVEL_MIN_BLOCK_LENGTH
        )));
    }
    let mut cur = off + 4;
    let mut levels = Vec::with_capacity(num_in_group);
    for _ in 0..num_in_group {
        if raw.len() < cur + block_length {
            return Err(DecodeError::new(format!(
                "OKEx SBE level group truncated: have {} need {}",
                raw.len(),
                cur + block_length
            )));
        }
        let px_m = read_i64_le(raw, cur)?;
        let sz_m = read_i64_le(raw, cur + 8)?;
        let _ord_count = read_i32_le(raw, cur + 16)?;
        let price = mantissa_to_f64(px_m, px_exp);
        let amount = mantissa_to_f64(sz_m, sz_exp);
        if price > 0.0 {
            levels.push(Level { price, amount });
        }
        cur += block_length;
    }
    Ok((levels, cur))
}

fn parse_liquidation_json(
    value: &Value,
    active_symbols: Option<&HashSet<String>>,
) -> Vec<Derivative> {
    let Some(data_array) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in data_array {
        let Some(inst_id) = item.get("instId").and_then(|v| v.as_str()) else {
            continue;
        };
        if active_symbols.is_some_and(|symbols| !symbols.contains(inst_id)) {
            continue;
        }
        let symbol = normalize_okex_symbol(inst_id);
        let Some(details) = item.get("details").and_then(|v| v.as_array()) else {
            continue;
        };
        for detail in details {
            let (Some(side_raw), Some(amount), Some(price), Some(timestamp_us)) = (
                detail.get("side").and_then(|v| v.as_str()),
                detail.get("sz").and_then(parse_f64_loose),
                detail.get("bkPx").and_then(parse_f64_loose),
                detail
                    .get("ts")
                    .and_then(parse_i64_loose)
                    .map(normalize_ts_to_us),
            ) else {
                continue;
            };
            let side = match side_raw {
                "buy" => 'B',
                "sell" => 'S',
                _ => continue,
            };
            out.push(Derivative::Liquidation {
                symbol: symbol.clone(),
                side,
                amount,
                price,
                timestamp_us,
            });
        }
    }
    out
}

fn parse_mark_price_json(value: &Value) -> Vec<Derivative> {
    let Some(data_array) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(data_array.len());
    for item in data_array {
        let (Some(inst_id), Some(price), Some(timestamp_us)) = (
            item.get("instId").and_then(|v| v.as_str()),
            item.get("markPx").and_then(parse_f64_loose),
            item.get("ts")
                .and_then(parse_i64_loose)
                .map(normalize_ts_to_us),
        ) else {
            continue;
        };
        out.push(Derivative::MarkPrice {
            symbol: normalize_okex_symbol(inst_id),
            price,
            timestamp_us,
        });
    }
    out
}

fn parse_funding_rate_json(value: &Value) -> Vec<Derivative> {
    let Some(data_array) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(data_array.len());
    for item in data_array {
        let (Some(inst_id), Some(funding_rate), Some(next_funding_time_us), Some(timestamp_us)) = (
            item.get("instId").and_then(|v| v.as_str()),
            item.get("fundingRate").and_then(parse_f64_loose),
            item.get("nextFundingTime")
                .and_then(parse_i64_loose)
                .map(normalize_ts_to_us),
            item.get("ts")
                .and_then(parse_i64_loose)
                .map(normalize_ts_to_us),
        ) else {
            continue;
        };
        out.push(Derivative::FundingRate {
            symbol: normalize_okex_symbol(inst_id),
            funding_rate,
            next_funding_time_us,
            timestamp_us,
        });
    }
    out
}

fn parse_index_price_json(value: &Value) -> Vec<Derivative> {
    let Some(data_array) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(data_array.len());
    for item in data_array {
        let (Some(inst_id), Some(price), Some(timestamp_us)) = (
            item.get("instId").and_then(|v| v.as_str()),
            item.get("idxPx").and_then(parse_f64_loose),
            item.get("ts")
                .and_then(parse_i64_loose)
                .map(normalize_ts_to_us),
        ) else {
            continue;
        };
        out.push(Derivative::IndexPrice {
            symbol: normalize_okex_symbol(inst_id),
            price,
            timestamp_us,
        });
    }
    out
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

fn read_sbe_header(raw: &[u8]) -> Result<(usize, u16), DecodeError> {
    if raw.len() < SBE_HEADER_SIZE {
        return Err(DecodeError::new(format!(
            "OKEx SBE frame too short: {} bytes",
            raw.len()
        )));
    }
    let block_length = u16::from_le_bytes([raw[0], raw[1]]) as usize;
    let template_id = u16::from_le_bytes([raw[2], raw[3]]);
    let schema_id = u16::from_le_bytes([raw[4], raw[5]]);
    if schema_id != SBE_SCHEMA_ID {
        return Err(DecodeError::new(format!(
            "OKEx SBE unexpected schemaId={} (want {})",
            schema_id, SBE_SCHEMA_ID
        )));
    }
    if raw.len() < SBE_HEADER_SIZE + block_length {
        return Err(DecodeError::new(format!(
            "OKEx SBE frame truncated: have {} bytes, header says blockLength={}",
            raw.len(),
            block_length
        )));
    }
    Ok((block_length, template_id))
}

fn read_u16_le(buf: &[u8], off: usize) -> Result<u16, DecodeError> {
    if buf.len() < off + 2 {
        return Err(DecodeError::new(format!(
            "OKEx SBE OOB read at offset {}",
            off
        )));
    }
    Ok(u16::from_le_bytes([buf[off], buf[off + 1]]))
}

fn read_i16_le(buf: &[u8], off: usize) -> Result<i16, DecodeError> {
    if buf.len() < off + 2 {
        return Err(DecodeError::new(format!(
            "OKEx SBE OOB read at offset {}",
            off
        )));
    }
    Ok(i16::from_le_bytes([buf[off], buf[off + 1]]))
}

fn read_i32_le(buf: &[u8], off: usize) -> Result<i32, DecodeError> {
    if buf.len() < off + 4 {
        return Err(DecodeError::new(format!(
            "OKEx SBE OOB read at offset {}",
            off
        )));
    }
    Ok(i32::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
    ]))
}

fn read_i64_le(buf: &[u8], off: usize) -> Result<i64, DecodeError> {
    if buf.len() < off + 8 {
        return Err(DecodeError::new(format!(
            "OKEx SBE OOB read at offset {}",
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

fn mantissa_to_f64(mantissa: i64, exponent: i8) -> f64 {
    (mantissa as f64) * 10_f64.powi(exponent as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_sbe_bbo_frame(
        inst_id_code: i64,
        ts_us: i64,
        out_time_us: i64,
        seq_id: i64,
        ask_px_m: i64,
        ask_sz_m: i64,
        bid_px_m: i64,
        bid_sz_m: i64,
        px_exp: i8,
        sz_exp: i8,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(82);
        buf.extend_from_slice(&(SBE_BBO_TBT_BLOCK_LENGTH as u16).to_le_bytes());
        buf.extend_from_slice(&SBE_TEMPLATE_BBO_TBT.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&inst_id_code.to_le_bytes());
        buf.extend_from_slice(&ts_us.to_le_bytes());
        buf.extend_from_slice(&out_time_us.to_le_bytes());
        buf.extend_from_slice(&seq_id.to_le_bytes());
        buf.extend_from_slice(&ask_px_m.to_le_bytes());
        buf.extend_from_slice(&ask_sz_m.to_le_bytes());
        buf.extend_from_slice(&bid_px_m.to_le_bytes());
        buf.extend_from_slice(&bid_sz_m.to_le_bytes());
        buf.extend_from_slice(&1i32.to_le_bytes());
        buf.extend_from_slice(&1i32.to_le_bytes());
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf
    }

    #[test]
    fn normalizes_symbols_and_json_derivatives_to_us() {
        assert_eq!(normalize_okex_symbol("BTC-USDT-SWAP"), "BTCUSDT");
        let active = HashSet::from(["BTC-USDT-SWAP".to_string()]);
        let funding = serde_json::json!({
            "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId":"BTC-USDT-SWAP", "fundingRate":"0.0001", "nextFundingTime":"1700003600000", "ts":"1700000000000"}]
        });
        let out = parse_derivatives_json(&funding, Some(&active));
        assert!(matches!(
            &out[0],
            Derivative::FundingRate {
                symbol,
                timestamp_us: 1_700_000_000_000_000,
                next_funding_time_us: 1_700_003_600_000_000,
                ..
            } if symbol == "BTCUSDT"
        ));
    }

    #[test]
    fn parses_sbe_bbo_tbt() {
        let raw = build_sbe_bbo_frame(
            10459,
            1_779_419_555_777_000,
            1_779_419_555_777_996,
            317_862_000_001,
            776_236,
            789,
            776_235,
            512,
            -1,
            -2,
        );
        let bbo = parse_sbe_bbo_tbt(&raw).unwrap().unwrap();
        assert_eq!(bbo.inst_id_code, 10459);
        assert_eq!(bbo.timestamp_us, 1_779_419_555_777_996);
        assert_eq!(bbo.seq_id, 317_862_000_001);
        assert!((bbo.bid_price - 77623.5).abs() < 1e-9);
        assert!((bbo.ask_amount - 7.89).abs() < 1e-9);
    }

    #[test]
    fn parses_json_bbo_trade_and_incremental() {
        let bbo = serde_json::json!({
            "arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},
            "data":[{"ts":"1700000000000","seqId":"7","bids":[["100","1"]],"asks":[["101","2"]]}]
        });
        let bbo = parse_bbo_json(&bbo);
        assert_eq!(bbo.len(), 1);
        assert_eq!(bbo[0].symbol, "BTCUSDT");
        assert_eq!(bbo[0].timestamp_us, 1_700_000_000_000_000);

        let trade = serde_json::json!({
            "data":[{"instId":"BTC-USDT-SWAP","tradeId":"9","px":"100","sz":"0.1","side":"buy","ts":"1700000000000"}]
        });
        let trade = parse_trades_json(&trade);
        assert_eq!(trade.len(), 1);
        assert_eq!(trade[0].side, 'B');
        assert_eq!(trade[0].timestamp_us, 1_700_000_000_000_000);

        let book = serde_json::json!({
            "action":"update",
            "arg":{"channel":"books","instId":"BTC-USDT-SWAP"},
            "data":[{"ts":"1700000000000","seqId":200,"prevSeqId":199,"bids":[["100","1"]],"asks":[["101","2"]]}]
        });
        let books = parse_incremental_json(&book);
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].first_update_id, 200);
        assert_eq!(books[0].final_update_id, 199);
    }
}
