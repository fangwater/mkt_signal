use serde_json::Value;

pub const SBE_TEMPLATE_TRADE: u16 = 10000;
pub const SBE_TEMPLATE_BBO: u16 = 10001;
pub const SBE_TEMPLATE_DEPTH_SNAPSHOT: u16 = 10002;
pub const SBE_TEMPLATE_DEPTH_DIFF: u16 = 10003;

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
    Liquidation {
        symbol: String,
        side: char,
        amount: f64,
        price: f64,
        timestamp_us: i64,
    },
}

pub fn payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

pub fn parse_bbo_json(value: &Value) -> Option<Bbo> {
    let payload = payload(value);
    if !payload.is_object() {
        return None;
    }
    let symbol = payload.get("s")?.as_str()?.to_ascii_uppercase();
    let seq_id = payload
        .get("u")
        .and_then(parse_i64)
        .or_else(|| payload.get("lastUpdateId").and_then(parse_i64))?;
    let timestamp_us = event_time_us(payload).unwrap_or(0);

    let (bid_price, bid_amount, ask_price, ask_amount) =
        if payload.get("b").and_then(|v| v.as_array()).is_some()
            || payload.get("a").and_then(|v| v.as_array()).is_some()
        {
            parse_depth_top(payload)?
        } else {
            (
                parse_f64(payload.get("b")?)?,
                parse_f64(payload.get("B")?)?,
                parse_f64(payload.get("a")?)?,
                parse_f64(payload.get("A")?)?,
            )
        };
    if bid_price <= 0.0 || bid_amount <= 0.0 || ask_price <= 0.0 || ask_amount <= 0.0 {
        return None;
    }

    Some(Bbo {
        symbol,
        timestamp_us,
        seq_id,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    })
}

pub fn parse_trade_json(value: &Value) -> Option<Trade> {
    let payload = payload(value);
    if payload.get("e").and_then(|v| v.as_str()) != Some("trade") {
        return None;
    }
    let symbol = payload.get("s")?.as_str()?.to_ascii_uppercase();
    let trade_id = parse_i64(payload.get("t")?)?;
    let timestamp_us = event_time_us(payload)?;
    let price = parse_f64(payload.get("p")?)?;
    let amount = parse_f64(payload.get("q")?)?;
    if price <= 0.0 || amount <= 0.0 {
        return None;
    }
    let side = if payload.get("m").and_then(|v| v.as_bool()).unwrap_or(false) {
        'S'
    } else {
        'B'
    };
    Some(Trade {
        symbol,
        timestamp_us,
        seq_id: trade_id,
        trade_id,
        side,
        price,
        amount,
    })
}

pub fn parse_incremental_json(value: &Value) -> Option<Book> {
    let payload = payload(value);
    if !payload.is_object() {
        return None;
    }
    let stream_symbol = value
        .get("stream")
        .and_then(|v| v.as_str())
        .and_then(parse_stream_symbol);

    let is_update = payload.get("e").and_then(|v| v.as_str()) == Some("depthUpdate")
        || (payload.get("U").is_some() && payload.get("u").is_some());
    if is_update {
        return parse_depth_update_json(payload, stream_symbol.as_deref());
    }
    if payload.get("lastUpdateId").is_some() {
        return parse_depth_snapshot_json(payload, stream_symbol.as_deref());
    }
    None
}

pub fn parse_derivatives_json(value: &Value) -> Vec<Derivative> {
    let payload = payload(value);
    if let Some(items) = payload.as_array() {
        let mut out = Vec::new();
        for item in items {
            out.extend(parse_derivatives_json(item));
        }
        return out;
    }

    match payload.get("e").and_then(|v| v.as_str()) {
        Some("markPriceUpdate") => parse_mark_price_json(payload),
        Some("forceOrder") => parse_liquidation_json(payload),
        _ => Vec::new(),
    }
}

pub fn parse_sbe_bbo(msg: &[u8]) -> Option<Bbo> {
    let header = read_sbe_header(msg)?;
    if header.template_id != SBE_TEMPLATE_BBO {
        return None;
    }
    let base = header.body_offset;
    if msg.len() < base + header.block_length {
        return None;
    }

    let timestamp_us = read_i64_le(msg, base)?;
    let seq_id = read_i64_le(msg, base + 8)?;
    let price_exponent = read_i8(msg, base + 16)?;
    let qty_exponent = read_i8(msg, base + 17)?;
    let bid_price = scale_mantissa(read_i64_le(msg, base + 18)?, price_exponent);
    let bid_amount = scale_mantissa(read_i64_le(msg, base + 26)?, qty_exponent);
    let ask_price = scale_mantissa(read_i64_le(msg, base + 34)?, price_exponent);
    let ask_amount = scale_mantissa(read_i64_le(msg, base + 42)?, qty_exponent);
    if bid_price <= 0.0 || bid_amount <= 0.0 || ask_price <= 0.0 || ask_amount <= 0.0 {
        return None;
    }
    let (symbol, _) = read_var_string8(msg, base + header.block_length)?;

    Some(Bbo {
        symbol: symbol.to_ascii_uppercase(),
        timestamp_us,
        seq_id,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    })
}

pub fn parse_sbe_trades(msg: &[u8]) -> Vec<Trade> {
    let Some(header) = read_sbe_header(msg) else {
        return Vec::new();
    };
    if header.template_id != SBE_TEMPLATE_TRADE {
        return Vec::new();
    }
    let base = header.body_offset;
    if msg.len() < base + header.block_length {
        return Vec::new();
    }

    let Some(timestamp_us) = read_i64_le(msg, base) else {
        return Vec::new();
    };
    let Some(price_exponent) = read_i8(msg, base + 16) else {
        return Vec::new();
    };
    let Some(qty_exponent) = read_i8(msg, base + 17) else {
        return Vec::new();
    };

    let mut offset = base + header.block_length;
    if msg.len() < offset + 6 {
        return Vec::new();
    }
    let Some(entry_block_len) = read_u16_le(msg, offset).map(|v| v as usize) else {
        return Vec::new();
    };
    let Some(num_entries) = read_u32_le(msg, offset + 2).map(|v| v as usize) else {
        return Vec::new();
    };
    offset += 6;

    let mut entries = Vec::with_capacity(num_entries);
    for _ in 0..num_entries {
        if msg.len() < offset + entry_block_len || entry_block_len < 25 {
            break;
        }
        let Some(trade_id) = read_i64_le(msg, offset) else {
            break;
        };
        let Some(price_m) = read_i64_le(msg, offset + 8) else {
            break;
        };
        let Some(qty_m) = read_i64_le(msg, offset + 16) else {
            break;
        };
        let is_buyer_maker = msg.get(offset + 24).copied().unwrap_or(0) != 0;
        entries.push((trade_id, price_m, qty_m, is_buyer_maker));
        offset += entry_block_len;
    }

    let Some((symbol, _)) = read_var_string8(msg, offset) else {
        return Vec::new();
    };
    let symbol = symbol.to_ascii_uppercase();
    let mut out = Vec::with_capacity(entries.len());
    for (trade_id, price_m, qty_m, is_buyer_maker) in entries {
        let price = scale_mantissa(price_m, price_exponent);
        let amount = scale_mantissa(qty_m, qty_exponent);
        if price <= 0.0 || amount <= 0.0 {
            continue;
        }
        out.push(Trade {
            symbol: symbol.clone(),
            timestamp_us,
            seq_id: trade_id,
            trade_id,
            side: if is_buyer_maker { 'S' } else { 'B' },
            price,
            amount,
        });
    }
    out
}

pub fn parse_sbe_incremental(msg: &[u8]) -> Option<Book> {
    let header = read_sbe_header(msg)?;
    match header.template_id {
        SBE_TEMPLATE_DEPTH_SNAPSHOT => parse_sbe_depth_snapshot(msg, &header),
        SBE_TEMPLATE_DEPTH_DIFF => parse_sbe_depth_diff(msg, &header),
        _ => None,
    }
}

fn parse_depth_top(payload: &Value) -> Option<(f64, f64, f64, f64)> {
    let bid = payload.get("b")?.as_array()?.first()?.as_array()?;
    let ask = payload.get("a")?.as_array()?.first()?.as_array()?;
    Some((
        parse_f64(bid.first()?)?,
        parse_f64(bid.get(1)?)?,
        parse_f64(ask.first()?)?,
        parse_f64(ask.get(1)?)?,
    ))
}

fn parse_depth_update_json(payload: &Value, symbol_override: Option<&str>) -> Option<Book> {
    let symbol = payload
        .get("s")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
        .or_else(|| symbol_override.map(|s| s.to_ascii_uppercase()))?;
    let first_update_id = parse_i64(payload.get("U")?)?;
    let final_update_id = parse_i64(payload.get("u")?)?;
    let timestamp_us = event_time_us(payload).unwrap_or(0);
    let bids = payload
        .get("b")
        .and_then(|v| v.as_array())
        .map(|levels| parse_json_levels(levels.as_slice()))
        .unwrap_or_default();
    let asks = payload
        .get("a")
        .and_then(|v| v.as_array())
        .map(|levels| parse_json_levels(levels.as_slice()))
        .unwrap_or_default();

    Some(Book {
        symbol,
        timestamp_us,
        seq_id: final_update_id,
        prev_seq_id: first_update_id.saturating_sub(1),
        first_update_id,
        final_update_id,
        gap_check: true,
        is_snapshot: false,
        bids,
        asks,
    })
}

fn parse_depth_snapshot_json(payload: &Value, symbol_override: Option<&str>) -> Option<Book> {
    let last_update_id = parse_i64(payload.get("lastUpdateId")?)?;
    let symbol = payload
        .get("s")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
        .or_else(|| symbol_override.map(|s| s.to_ascii_uppercase()))?;
    let timestamp_us = event_time_us(payload).unwrap_or(0);
    let bids = payload
        .get("bids")
        .and_then(|v| v.as_array())
        .map(|levels| parse_json_levels(levels.as_slice()))
        .unwrap_or_default();
    let asks = payload
        .get("asks")
        .and_then(|v| v.as_array())
        .map(|levels| parse_json_levels(levels.as_slice()))
        .unwrap_or_default();

    Some(Book {
        symbol,
        timestamp_us,
        seq_id: last_update_id,
        prev_seq_id: last_update_id,
        first_update_id: last_update_id,
        final_update_id: last_update_id,
        gap_check: false,
        is_snapshot: true,
        bids,
        asks,
    })
}

fn parse_mark_price_json(payload: &Value) -> Vec<Derivative> {
    let Some(symbol) = payload
        .get("s")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
    else {
        return Vec::new();
    };
    let timestamp_us = event_time_us(payload).unwrap_or(0);
    let mut out = Vec::with_capacity(3);
    if let Some(price) = payload.get("p").and_then(parse_f64) {
        if price > 0.0 {
            out.push(Derivative::MarkPrice {
                symbol: symbol.clone(),
                price,
                timestamp_us,
            });
        }
    }
    if let Some(price) = payload.get("i").and_then(parse_f64) {
        if price > 0.0 {
            out.push(Derivative::IndexPrice {
                symbol: symbol.clone(),
                price,
                timestamp_us,
            });
        }
    }
    if let (Some(funding_rate), Some(next_funding_time_us)) = (
        payload.get("r").and_then(parse_f64),
        payload.get("T").and_then(parse_i64).map(ms_to_us),
    ) {
        out.push(Derivative::FundingRate {
            symbol,
            funding_rate,
            next_funding_time_us,
            timestamp_us,
        });
    }
    out
}

fn parse_liquidation_json(payload: &Value) -> Vec<Derivative> {
    let Some(order) = payload.get("o").and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    let Some(symbol) = order
        .get("s")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
    else {
        return Vec::new();
    };
    let side = match order.get("S").and_then(|v| v.as_str()).unwrap_or("") {
        "BUY" => 'B',
        "SELL" => 'S',
        _ => return Vec::new(),
    };
    let Some(amount) = order.get("z").and_then(parse_f64) else {
        return Vec::new();
    };
    let Some(price) = order.get("ap").and_then(parse_f64) else {
        return Vec::new();
    };
    if amount <= 0.0 || price <= 0.0 {
        return Vec::new();
    }
    let timestamp_us = order
        .get("T")
        .or_else(|| payload.get("E"))
        .and_then(parse_i64)
        .map(ms_to_us)
        .unwrap_or(0);
    vec![Derivative::Liquidation {
        symbol,
        side,
        amount,
        price,
        timestamp_us,
    }]
}

fn parse_sbe_depth_snapshot(msg: &[u8], header: &SbeHeader) -> Option<Book> {
    let base = header.body_offset;
    if msg.len() < base + header.block_length {
        return None;
    }
    let timestamp_us = read_i64_le(msg, base)?;
    let book_update_id = read_i64_le(msg, base + 8)?;
    let price_exponent = read_i8(msg, base + 16)?;
    let qty_exponent = read_i8(msg, base + 17)?;
    let mut offset = base + header.block_length;
    let (bids, next_offset) = read_group_levels(msg, offset, price_exponent, qty_exponent)?;
    offset = next_offset;
    let (asks, next_offset) = read_group_levels(msg, offset, price_exponent, qty_exponent)?;
    offset = next_offset;
    let (symbol, _) = read_var_string8(msg, offset)?;
    Some(Book {
        symbol: symbol.to_ascii_uppercase(),
        timestamp_us,
        seq_id: book_update_id,
        prev_seq_id: book_update_id,
        first_update_id: book_update_id,
        final_update_id: book_update_id,
        gap_check: false,
        is_snapshot: true,
        bids,
        asks,
    })
}

fn parse_sbe_depth_diff(msg: &[u8], header: &SbeHeader) -> Option<Book> {
    let base = header.body_offset;
    if msg.len() < base + header.block_length {
        return None;
    }
    let timestamp_us = read_i64_le(msg, base)?;
    let first_update_id = read_i64_le(msg, base + 8)?;
    let final_update_id = read_i64_le(msg, base + 16)?;
    let price_exponent = read_i8(msg, base + 24)?;
    let qty_exponent = read_i8(msg, base + 25)?;
    let mut offset = base + header.block_length;
    let (bids, next_offset) = read_group_levels(msg, offset, price_exponent, qty_exponent)?;
    offset = next_offset;
    let (asks, next_offset) = read_group_levels(msg, offset, price_exponent, qty_exponent)?;
    offset = next_offset;
    let (symbol, _) = read_var_string8(msg, offset)?;
    Some(Book {
        symbol: symbol.to_ascii_uppercase(),
        timestamp_us,
        seq_id: final_update_id,
        prev_seq_id: first_update_id.saturating_sub(1),
        first_update_id,
        final_update_id,
        gap_check: true,
        is_snapshot: false,
        bids,
        asks,
    })
}

fn parse_json_levels(levels: &[Value]) -> Vec<Level> {
    levels
        .iter()
        .filter_map(|level| {
            let arr = level.as_array()?;
            Some(Level {
                price: parse_f64(arr.first()?)?,
                amount: parse_f64(arr.get(1)?)?,
            })
        })
        .collect()
}

fn parse_i64(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse::<i64>().ok())
}

fn parse_f64(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok())
}

fn event_time_us(payload: &Value) -> Option<i64> {
    payload
        .get("E")
        .and_then(parse_i64)
        .or_else(|| payload.get("T").and_then(parse_i64))
        .map(ms_to_us)
}

fn ms_to_us(ts: i64) -> i64 {
    ts.saturating_mul(1000)
}

fn parse_stream_symbol(stream: &str) -> Option<String> {
    stream.split('@').next().map(|s| s.to_ascii_uppercase())
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

fn read_u32_le(msg: &[u8], offset: usize) -> Option<u32> {
    if msg.len() < offset + 4 {
        return None;
    }
    Some(u32::from_le_bytes([
        msg[offset],
        msg[offset + 1],
        msg[offset + 2],
        msg[offset + 3],
    ]))
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

fn read_group_levels(
    msg: &[u8],
    offset: usize,
    price_exponent: i8,
    qty_exponent: i8,
) -> Option<(Vec<Level>, usize)> {
    if msg.len() < offset + 4 {
        return None;
    }
    let block_length = read_u16_le(msg, offset)? as usize;
    let num_in_group = read_u16_le(msg, offset + 2)? as usize;
    let mut pos = offset + 4;
    let mut levels = Vec::with_capacity(num_in_group);
    for _ in 0..num_in_group {
        if msg.len() < pos + block_length || block_length < 16 {
            break;
        }
        levels.push(Level {
            price: scale_mantissa(read_i64_le(msg, pos)?, price_exponent),
            amount: scale_mantissa(read_i64_le(msg, pos + 8)?, qty_exponent),
        });
        pos += block_length;
    }
    Some((levels, pos))
}
