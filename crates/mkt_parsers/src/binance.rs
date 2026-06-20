use serde_json::Value;

pub const SBE_TEMPLATE_TRADE: u16 = 10000;
pub const SBE_TEMPLATE_BBO: u16 = 10001;
pub const SBE_TEMPLATE_DEPTH_SNAPSHOT: u16 = 10002;
pub const SBE_TEMPLATE_DEPTH_DIFF: u16 = 10003;
pub const RAW_DEPTH_LEVEL_CAP: usize = 64;

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawBbo<'a> {
    pub symbol: &'a str,
    pub timestamp_us: i64,
    pub seq_id: i64,
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawTrade<'a> {
    pub symbol: &'a str,
    pub timestamp_us: i64,
    pub seq_id: i64,
    pub trade_id: i64,
    pub side: char,
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawLevels {
    levels: [Level; RAW_DEPTH_LEVEL_CAP],
    len: usize,
}

impl RawLevels {
    fn new() -> Self {
        Self {
            levels: [Level {
                price: 0.0,
                amount: 0.0,
            }; RAW_DEPTH_LEVEL_CAP],
            len: 0,
        }
    }

    fn push(&mut self, level: Level) -> Option<()> {
        if self.len >= RAW_DEPTH_LEVEL_CAP {
            return None;
        }
        self.levels[self.len] = level;
        self.len += 1;
        Some(())
    }

    pub fn as_slice(&self) -> &[Level] {
        &self.levels[..self.len]
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for RawLevels {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawBook<'a> {
    pub symbol: &'a str,
    pub timestamp_us: i64,
    pub seq_id: i64,
    pub prev_seq_id: i64,
    pub first_update_id: i64,
    pub final_update_id: i64,
    pub gap_check: bool,
    pub is_snapshot: bool,
    pub bids: RawLevels,
    pub asks: RawLevels,
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

pub fn parse_book_ticker_bbo_raw(raw: &[u8]) -> Option<Bbo> {
    let payload = parse_book_ticker_bbo_raw_borrowed(raw)?;
    Some(Bbo {
        symbol: payload.symbol.to_ascii_uppercase(),
        timestamp_us: payload.timestamp_us,
        seq_id: payload.seq_id,
        bid_price: payload.bid_price,
        bid_amount: payload.bid_amount,
        ask_price: payload.ask_price,
        ask_amount: payload.ask_amount,
    })
}

pub fn parse_book_ticker_bbo_raw_borrowed(raw: &[u8]) -> Option<RawBbo<'_>> {
    let data = combined_payload(raw).unwrap_or(raw);
    let mut scanner = JsonObjectScanner::new(data);
    let mut seen_event = false;
    let mut symbol = None;
    let mut seq_id = None;
    let mut bid_price = None;
    let mut bid_amount = None;
    let mut ask_price = None;
    let mut ask_amount = None;
    let mut timestamp_us = 0i64;

    while let Some((key, value)) = scanner.next_field() {
        match key {
            b"e" => {
                if value.string_bytes()? != b"bookTicker" {
                    return None;
                }
                seen_event = true;
            }
            b"s" => symbol = Some(value.string_str()?),
            b"u" => seq_id = Some(value.i64()?),
            b"b" => bid_price = Some(value.f64()?),
            b"B" => bid_amount = Some(value.f64()?),
            b"a" => ask_price = Some(value.f64()?),
            b"A" => ask_amount = Some(value.f64()?),
            b"E" => timestamp_us = ms_to_us(value.i64()?),
            _ => {}
        }
    }

    if !seen_event && stream_name(raw).is_some_and(|stream| !stream.ends_with("@bookTicker")) {
        return None;
    }

    let out = RawBbo {
        symbol: symbol?,
        timestamp_us,
        seq_id: seq_id?,
        bid_price: bid_price?,
        bid_amount: bid_amount?,
        ask_price: ask_price?,
        ask_amount: ask_amount?,
    };
    if out.bid_price <= 0.0
        || out.bid_amount <= 0.0
        || out.ask_price <= 0.0
        || out.ask_amount <= 0.0
    {
        return None;
    }
    Some(out)
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

pub fn parse_trade_raw(raw: &[u8]) -> Option<Trade> {
    let payload = parse_trade_raw_borrowed(raw)?;
    Some(Trade {
        symbol: payload.symbol.to_ascii_uppercase(),
        timestamp_us: payload.timestamp_us,
        seq_id: payload.seq_id,
        trade_id: payload.trade_id,
        side: payload.side,
        price: payload.price,
        amount: payload.amount,
    })
}

pub fn parse_trade_raw_borrowed(raw: &[u8]) -> Option<RawTrade<'_>> {
    let data = combined_payload(raw).unwrap_or(raw);
    let mut scanner = JsonObjectScanner::new(data);
    let mut seen_event = false;
    let mut symbol = None;
    let mut trade_id = None;
    let mut timestamp_us = None;
    let mut price = None;
    let mut amount = None;
    let mut is_buyer_maker = false;

    while let Some((key, value)) = scanner.next_field() {
        match key {
            b"e" => {
                if value.string_bytes()? != b"trade" {
                    return None;
                }
                seen_event = true;
            }
            b"s" => symbol = Some(value.string_str()?),
            b"t" => trade_id = Some(value.i64()?),
            b"E" => timestamp_us = Some(ms_to_us(value.i64()?)),
            b"T" if timestamp_us.is_none() => timestamp_us = Some(ms_to_us(value.i64()?)),
            b"p" => price = Some(value.f64()?),
            b"q" => amount = Some(value.f64()?),
            b"m" => is_buyer_maker = value.bool()?,
            _ => {}
        }
    }

    if !seen_event && stream_name(raw).is_some_and(|stream| !stream.ends_with("@trade")) {
        return None;
    }

    let trade_id = trade_id?;
    let out = RawTrade {
        symbol: symbol?,
        timestamp_us: timestamp_us?,
        seq_id: trade_id,
        trade_id,
        side: if is_buyer_maker { 'S' } else { 'B' },
        price: price?,
        amount: amount?,
    };
    if out.price <= 0.0 || out.amount <= 0.0 {
        return None;
    }
    Some(out)
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

pub fn parse_incremental_raw_borrowed(raw: &[u8]) -> Option<RawBook<'_>> {
    let data = combined_payload(raw).unwrap_or(raw);
    let stream_symbol = stream_name(raw).and_then(parse_stream_symbol_borrowed);
    let mut scanner = JsonObjectScanner::new(data);
    let mut seen_event = false;
    let mut symbol = None;
    let mut timestamp_us = None;
    let mut first_update_id = None;
    let mut final_update_id = None;
    let mut last_update_id = None;
    let mut bids = RawLevels::new();
    let mut asks = RawLevels::new();
    let mut has_bids = false;
    let mut has_asks = false;

    while let Some((key, value)) = scanner.next_field() {
        match key {
            b"e" => {
                if value.string_bytes()? != b"depthUpdate" {
                    return None;
                }
                seen_event = true;
            }
            b"s" => symbol = Some(value.string_str()?),
            b"E" => timestamp_us = Some(ms_to_us(value.i64()?)),
            b"T" if timestamp_us.is_none() => timestamp_us = Some(ms_to_us(value.i64()?)),
            b"U" => first_update_id = Some(value.i64()?),
            b"u" => final_update_id = Some(value.i64()?),
            b"lastUpdateId" => last_update_id = Some(value.i64()?),
            b"b" | b"bids" => {
                bids = parse_raw_levels(value.array_bytes()?)?;
                has_bids = true;
            }
            b"a" | b"asks" => {
                asks = parse_raw_levels(value.array_bytes()?)?;
                has_asks = true;
            }
            _ => {}
        }
    }

    let symbol = symbol.or(stream_symbol)?;
    if let Some(last_update_id) = last_update_id {
        return Some(RawBook {
            symbol,
            timestamp_us: timestamp_us.unwrap_or(0),
            seq_id: last_update_id,
            prev_seq_id: last_update_id,
            first_update_id: last_update_id,
            final_update_id: last_update_id,
            gap_check: false,
            is_snapshot: true,
            bids,
            asks,
        });
    }

    if !seen_event && stream_name(raw).is_some_and(|stream| !stream.contains("@depth")) {
        return None;
    }
    if !has_bids && !has_asks {
        return None;
    }
    let first_update_id = first_update_id?;
    let final_update_id = final_update_id?;
    Some(RawBook {
        symbol,
        timestamp_us: timestamp_us.unwrap_or(0),
        seq_id: final_update_id,
        prev_seq_id: first_update_id.saturating_sub(1),
        first_update_id,
        final_update_id,
        gap_check: false,
        is_snapshot: false,
        bids,
        asks,
    })
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
        gap_check: false,
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
        gap_check: false,
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

fn parse_raw_levels(raw: &[u8]) -> Option<RawLevels> {
    let raw = trim_ascii(raw);
    if raw.first() != Some(&b'[') || raw.last() != Some(&b']') {
        return None;
    }

    let mut out = RawLevels::new();
    let mut pos = 1usize;
    loop {
        skip_ws_at(raw, &mut pos);
        match raw.get(pos).copied()? {
            b']' => return Some(out),
            b',' => {
                pos += 1;
                continue;
            }
            b'[' => {
                let level = parse_raw_level(raw, &mut pos)?;
                out.push(level)?;
            }
            _ => return None,
        }
    }
}

fn parse_raw_level(raw: &[u8], pos: &mut usize) -> Option<Level> {
    if raw.get(*pos) != Some(&b'[') {
        return None;
    }
    *pos += 1;
    let price = parse_raw_number(raw, pos)?;
    skip_ws_at(raw, pos);
    if raw.get(*pos) != Some(&b',') {
        return None;
    }
    *pos += 1;
    let amount = parse_raw_number(raw, pos)?;
    skip_ws_at(raw, pos);
    if raw.get(*pos) != Some(&b']') {
        return None;
    }
    *pos += 1;
    Some(Level { price, amount })
}

fn parse_raw_number(raw: &[u8], pos: &mut usize) -> Option<f64> {
    skip_ws_at(raw, pos);
    let bytes = if raw.get(*pos) == Some(&b'"') {
        *pos += 1;
        let start = *pos;
        loop {
            let b = *raw.get(*pos)?;
            match b {
                b'\\' => return None,
                b'"' => {
                    let end = *pos;
                    *pos += 1;
                    break &raw[start..end];
                }
                _ => *pos += 1,
            }
        }
    } else {
        let start = *pos;
        while let Some(&b) = raw.get(*pos) {
            if b == b',' || b == b']' || b.is_ascii_whitespace() {
                break;
            }
            *pos += 1;
        }
        &raw[start..*pos]
    };
    if bytes.is_empty() {
        return None;
    }
    std::str::from_utf8(bytes).ok()?.parse::<f64>().ok()
}

fn skip_ws_at(raw: &[u8], pos: &mut usize) {
    while raw.get(*pos).is_some_and(|b| b.is_ascii_whitespace()) {
        *pos += 1;
    }
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

fn parse_stream_symbol_borrowed(stream: &str) -> Option<&str> {
    let symbol = stream.split('@').next()?;
    if symbol.as_bytes().iter().any(|b| b.is_ascii_lowercase()) {
        return None;
    }
    Some(symbol)
}

fn combined_payload(raw: &[u8]) -> Option<&[u8]> {
    let mut scanner = JsonObjectScanner::new(raw);
    while let Some((key, value)) = scanner.next_field() {
        if key == b"data" {
            return value.object_bytes();
        }
    }
    None
}

fn stream_name(raw: &[u8]) -> Option<&str> {
    let mut scanner = JsonObjectScanner::new(raw);
    while let Some((key, value)) = scanner.next_field() {
        if key == b"stream" {
            return value.string_str();
        }
    }
    None
}

#[derive(Clone, Copy)]
struct JsonValue<'a> {
    raw: &'a [u8],
}

impl<'a> JsonValue<'a> {
    fn string_bytes(self) -> Option<&'a [u8]> {
        if self.raw.len() < 2 || self.raw.first() != Some(&b'"') || self.raw.last() != Some(&b'"') {
            return None;
        }
        let inner = &self.raw[1..self.raw.len() - 1];
        if inner.contains(&b'\\') {
            return None;
        }
        Some(inner)
    }

    fn string_str(self) -> Option<&'a str> {
        std::str::from_utf8(self.string_bytes()?).ok()
    }

    fn object_bytes(self) -> Option<&'a [u8]> {
        let raw = trim_ascii(self.raw);
        if raw.first() == Some(&b'{') && raw.last() == Some(&b'}') {
            Some(raw)
        } else {
            None
        }
    }

    fn array_bytes(self) -> Option<&'a [u8]> {
        let raw = trim_ascii(self.raw);
        if raw.first() == Some(&b'[') && raw.last() == Some(&b']') {
            Some(raw)
        } else {
            None
        }
    }

    fn number_bytes(self) -> Option<&'a [u8]> {
        let raw = trim_ascii(self.raw);
        if raw.first() == Some(&b'"') && raw.last() == Some(&b'"') {
            let inner = &raw[1..raw.len() - 1];
            if inner.contains(&b'\\') {
                return None;
            }
            Some(inner)
        } else {
            Some(raw)
        }
    }

    fn i64(self) -> Option<i64> {
        std::str::from_utf8(self.number_bytes()?)
            .ok()?
            .parse::<i64>()
            .ok()
    }

    fn f64(self) -> Option<f64> {
        std::str::from_utf8(self.number_bytes()?)
            .ok()?
            .parse::<f64>()
            .ok()
    }

    fn bool(self) -> Option<bool> {
        match trim_ascii(self.raw) {
            b"true" => Some(true),
            b"false" => Some(false),
            _ => None,
        }
    }
}

struct JsonObjectScanner<'a> {
    raw: &'a [u8],
    pos: usize,
}

impl<'a> JsonObjectScanner<'a> {
    fn new(raw: &'a [u8]) -> Self {
        Self { raw, pos: 0 }
    }

    fn next_field(&mut self) -> Option<(&'a [u8], JsonValue<'a>)> {
        self.skip_object_separators();
        if self.raw.get(self.pos) == Some(&b'}') {
            self.pos += 1;
            return None;
        }
        let key = self.take_string_inner()?;
        self.skip_ws();
        if self.raw.get(self.pos) != Some(&b':') {
            return None;
        }
        self.pos += 1;
        self.skip_ws();
        let start = self.pos;
        self.skip_value()?;
        let end = self.pos;
        Some((
            &self.raw[key.0..key.1],
            JsonValue {
                raw: &self.raw[start..end],
            },
        ))
    }

    fn skip_object_separators(&mut self) {
        loop {
            self.skip_ws();
            match self.raw.get(self.pos) {
                Some(b'{') | Some(b',') => self.pos += 1,
                _ => break,
            }
        }
    }

    fn skip_ws(&mut self) {
        while self
            .raw
            .get(self.pos)
            .is_some_and(|b| b.is_ascii_whitespace())
        {
            self.pos += 1;
        }
    }

    fn take_string_inner(&mut self) -> Option<(usize, usize)> {
        if self.raw.get(self.pos) != Some(&b'"') {
            return None;
        }
        self.pos += 1;
        let start = self.pos;
        while let Some(&b) = self.raw.get(self.pos) {
            match b {
                b'\\' => return None,
                b'"' => {
                    let end = self.pos;
                    self.pos += 1;
                    return Some((start, end));
                }
                _ => self.pos += 1,
            }
        }
        None
    }

    fn skip_value(&mut self) -> Option<()> {
        match *self.raw.get(self.pos)? {
            b'"' => self.skip_string_value(),
            b'{' | b'[' => self.skip_nested_value(),
            _ => {
                while let Some(&b) = self.raw.get(self.pos) {
                    if b == b',' || b == b'}' || b == b']' || b.is_ascii_whitespace() {
                        break;
                    }
                    self.pos += 1;
                }
                (self.pos < self.raw.len()).then_some(())
            }
        }
    }

    fn skip_string_value(&mut self) -> Option<()> {
        if self.raw.get(self.pos) != Some(&b'"') {
            return None;
        }
        self.pos += 1;
        while let Some(&b) = self.raw.get(self.pos) {
            match b {
                b'\\' => {
                    self.pos += 2;
                }
                b'"' => {
                    self.pos += 1;
                    return Some(());
                }
                _ => self.pos += 1,
            }
        }
        None
    }

    fn skip_nested_value(&mut self) -> Option<()> {
        let mut depth = 0usize;
        while let Some(&b) = self.raw.get(self.pos) {
            match b {
                b'"' => self.skip_string_value()?,
                b'{' | b'[' => {
                    depth += 1;
                    self.pos += 1;
                }
                b'}' | b']' => {
                    depth = depth.checked_sub(1)?;
                    self.pos += 1;
                    if depth == 0 {
                        return Some(());
                    }
                }
                _ => self.pos += 1,
            }
        }
        None
    }
}

fn trim_ascii(mut raw: &[u8]) -> &[u8] {
    while raw.first().is_some_and(|b| b.is_ascii_whitespace()) {
        raw = &raw[1..];
    }
    while raw.last().is_some_and(|b| b.is_ascii_whitespace()) {
        raw = &raw[..raw.len() - 1];
    }
    raw
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

#[cfg(test)]
mod tests {
    use super::{
        parse_book_ticker_bbo_raw, parse_book_ticker_bbo_raw_borrowed,
        parse_incremental_raw_borrowed, parse_trade_raw, parse_trade_raw_borrowed,
        RAW_DEPTH_LEVEL_CAP,
    };

    #[test]
    fn parses_combined_book_ticker_without_value_tree() {
        let raw = br#"{
            "stream":"btcusdt@bookTicker",
            "data":{"e":"bookTicker","u":22345,"s":"BTCUSDT","b":"25.0","B":"100","a":"25.1","A":"50","E":1700000000002}
        }"#;

        let bbo = parse_book_ticker_bbo_raw(raw).expect("book ticker");

        assert_eq!(bbo.symbol, "BTCUSDT");
        assert_eq!(bbo.seq_id, 22345);
        assert_eq!(bbo.timestamp_us, 1_700_000_000_002_000);
        assert!((bbo.bid_price - 25.0).abs() < 1e-9);
        assert!((bbo.ask_amount - 50.0).abs() < 1e-9);

        let borrowed = parse_book_ticker_bbo_raw_borrowed(raw).expect("borrowed book ticker");
        assert_eq!(borrowed.symbol, "BTCUSDT");
        assert_eq!(borrowed.seq_id, 22345);
        assert!((borrowed.bid_price - 25.0).abs() < 1e-9);
    }

    #[test]
    fn parses_direct_book_ticker_without_value_tree() {
        let raw = br#"{"e":"bookTicker","u":"22346","s":"ethusdt","b":"2500.0","B":"2","a":"2500.1","A":"3"}"#;

        let bbo = parse_book_ticker_bbo_raw(raw).expect("book ticker");

        assert_eq!(bbo.symbol, "ETHUSDT");
        assert_eq!(bbo.seq_id, 22346);
        assert_eq!(bbo.timestamp_us, 0);
        assert!((bbo.bid_amount - 2.0).abs() < 1e-9);
        assert!((bbo.ask_price - 2500.1).abs() < 1e-9);
    }

    #[test]
    fn raw_book_ticker_rejects_depth_shape() {
        let raw = br#"{"stream":"btcusdt@depth5@0ms","data":{"e":"depthUpdate","s":"BTCUSDT","U":1,"u":2,"b":[["25","1"]],"a":[["25.1","1"]]}}"#;
        assert!(parse_book_ticker_bbo_raw(raw).is_none());
    }

    #[test]
    fn parses_combined_trade_without_value_tree() {
        let raw = br#"{
            "stream":"btcusdt@trade",
            "data":{"e":"trade","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","t":1001,"p":"25.0","q":"100","m":true}
        }"#;

        let trade = parse_trade_raw(raw).expect("trade");

        assert_eq!(trade.symbol, "BTCUSDT");
        assert_eq!(trade.trade_id, 1001);
        assert_eq!(trade.seq_id, 1001);
        assert_eq!(trade.timestamp_us, 1_700_000_000_001_000);
        assert_eq!(trade.side, 'S');
        assert!((trade.price - 25.0).abs() < 1e-9);
        assert!((trade.amount - 100.0).abs() < 1e-9);

        let borrowed = parse_trade_raw_borrowed(raw).expect("borrowed trade");
        assert_eq!(borrowed.symbol, "BTCUSDT");
        assert_eq!(borrowed.side, 'S');
    }

    #[test]
    fn raw_trade_uses_trade_time_when_event_time_is_absent() {
        let raw = br#"{"e":"trade","T":1700000000000,"s":"ethusdt","t":"1002","p":"2500.0","q":"2","m":false}"#;

        let trade = parse_trade_raw(raw).expect("direct trade");

        assert_eq!(trade.symbol, "ETHUSDT");
        assert_eq!(trade.timestamp_us, 1_700_000_000_000_000);
        assert_eq!(trade.side, 'B');
        assert!((trade.amount - 2.0).abs() < 1e-9);
    }

    #[test]
    fn raw_trade_rejects_book_ticker_shape() {
        let raw = br#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"BTCUSDT","b":"25","B":"1","a":"25.1","A":"1"}}"#;
        assert!(parse_trade_raw(raw).is_none());
    }

    #[test]
    fn parses_depth_update_without_value_tree() {
        let raw = br#"{
            "stream":"btcusdt@depth@0ms",
            "data":{"e":"depthUpdate","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","U":101,"u":103,
            "b":[["25.0","100"],["24.9","0"]],"a":[["25.1","50"]]}
        }"#;

        let book = parse_incremental_raw_borrowed(raw).expect("depth update");

        assert_eq!(book.symbol, "BTCUSDT");
        assert_eq!(book.timestamp_us, 1_700_000_000_001_000);
        assert_eq!(book.seq_id, 103);
        assert_eq!(book.prev_seq_id, 100);
        assert_eq!(book.first_update_id, 101);
        assert_eq!(book.final_update_id, 103);
        assert!(!book.is_snapshot);
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 1);
        assert!((book.bids.as_slice()[0].price - 25.0).abs() < 1e-9);
        assert!((book.bids.as_slice()[1].amount - 0.0).abs() < 1e-9);
        assert!((book.asks.as_slice()[0].amount - 50.0).abs() < 1e-9);
    }

    #[test]
    fn parses_depth_snapshot_without_value_tree() {
        let raw = br#"{"lastUpdateId":22345,"s":"BTCUSDT","E":1700000000001,
            "bids":[["25.0","100"]],"asks":[["25.1","50"],["25.2","25"]]}"#;

        let book = parse_incremental_raw_borrowed(raw).expect("depth snapshot");

        assert_eq!(book.symbol, "BTCUSDT");
        assert_eq!(book.timestamp_us, 1_700_000_000_001_000);
        assert_eq!(book.seq_id, 22345);
        assert_eq!(book.prev_seq_id, 22345);
        assert!(book.is_snapshot);
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.asks.len(), 2);
        assert!((book.asks.as_slice()[1].price - 25.2).abs() < 1e-9);
    }

    #[test]
    fn raw_depth_requires_uppercase_symbol_for_stream_only_shape() {
        let raw = br#"{"stream":"btcusdt@depth@0ms","data":{"e":"depthUpdate","E":1700000000001,"U":101,"u":103,
            "b":[["25.0","100"]],"a":[["25.1","50"]]}}"#;
        assert!(parse_incremental_raw_borrowed(raw).is_none());

        let raw = br#"{"stream":"BTCUSDT@depth@0ms","data":{"e":"depthUpdate","E":1700000000001,"U":101,"u":103,
            "b":[["25.0","100"]],"a":[["25.1","50"]]}}"#;
        let book = parse_incremental_raw_borrowed(raw).expect("uppercase stream symbol");
        assert_eq!(book.symbol, "BTCUSDT");
    }

    #[test]
    fn raw_depth_over_capacity_falls_back() {
        let mut raw = br#"{"e":"depthUpdate","s":"BTCUSDT","U":1,"u":2,"b":["#.to_vec();
        for i in 0..=RAW_DEPTH_LEVEL_CAP {
            if i > 0 {
                raw.push(b',');
            }
            raw.extend_from_slice(br#"["25.0","1"]"#);
        }
        raw.extend_from_slice(br#"],"a":[]}"#);

        assert!(parse_incremental_raw_borrowed(&raw).is_none());
    }
}
