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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawBookView<'a> {
    pub symbol: &'a str,
    pub timestamp_us: i64,
    pub seq_id: i64,
    pub prev_seq_id: i64,
    pub first_update_id: i64,
    pub final_update_id: i64,
    pub gap_check: bool,
    pub is_snapshot: bool,
    pub bids_raw: &'a [u8],
    pub asks_raw: &'a [u8],
    pub bids_count: usize,
    pub asks_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RawBookParse<'a> {
    Parsed(RawBook<'a>),
    View(RawBookView<'a>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawKline<'a> {
    pub symbol: &'a str,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
    pub volume: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RawDerivative<'a> {
    MarkPrice {
        symbol: &'a str,
        mark_price: Option<f64>,
        index_price: Option<f64>,
        funding_rate: Option<f64>,
        next_funding_time_us: Option<i64>,
        timestamp_us: i64,
    },
    Liquidation {
        symbol: &'a str,
        side: char,
        amount: f64,
        price: f64,
        timestamp_us: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawDerivativeKind {
    MarkPrice,
    Liquidation,
}

#[derive(Debug, Clone, Copy)]
struct RawLiquidationFields<'a> {
    symbol: &'a str,
    side: char,
    amount: f64,
    price: f64,
    order_timestamp_us: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
struct RawKlineFields {
    open_price: f64,
    high_price: f64,
    low_price: f64,
    close_price: f64,
    volume: f64,
    timestamp: i64,
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
    let mut payload = raw_payload_object(raw);
    let mut seen_event = false;
    let mut symbol = None;
    let mut seq_id = None;
    let mut bid_price = None;
    let mut bid_amount = None;
    let mut ask_price = None;
    let mut ask_amount = None;
    let mut timestamp_us = 0i64;

    while let Some((key, value)) = payload.next_field() {
        match key {
            b"e" => {
                if value.raw != br#""bookTicker""# {
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
        if seen_event
            && symbol.is_some()
            && seq_id.is_some()
            && timestamp_us != 0
            && bid_price.is_some()
            && bid_amount.is_some()
            && ask_price.is_some()
            && ask_amount.is_some()
        {
            break;
        }
    }

    if !seen_event
        && payload
            .stream
            .and_then(|stream_value| stream_value.string_str())
            .is_some_and(|stream| !stream.ends_with("@bookTicker"))
    {
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum RawBboKind {
    BookTicker,
    Depth,
}

pub fn parse_bbo_raw_borrowed(raw: &[u8]) -> Option<RawBbo<'_>> {
    let mut payload = raw_payload_object(raw);
    let mut event_kind = None;
    let mut symbol = None;
    let mut seq_id = None;
    let mut timestamp_us = None;
    let mut bid = None;
    let mut bid_amount = None;
    let mut ask = None;
    let mut ask_amount = None;
    let mut seen = 0u8;
    const BBO_BOOK_TICKER_REQUIRED: u8 = 0b0111_1111;
    const BBO_DEPTH_REQUIRED: u8 = 0b0010_0111;

    while let Some((key, value)) = payload.next_field() {
        match key {
            b"e" => {
                event_kind = Some(match value.raw {
                    br#""bookTicker""# => RawBboKind::BookTicker,
                    br#""depthUpdate""# => RawBboKind::Depth,
                    _ => return None,
                });
            }
            b"s" => {
                symbol = Some(value.string_str()?);
                seen |= 1 << 0;
            }
            b"u" | b"lastUpdateId" => {
                seq_id = Some(value.i64()?);
                seen |= 1 << 1;
            }
            b"E" => {
                timestamp_us = Some(ms_to_us(value.i64()?));
                seen |= 1 << 2;
            }
            b"T" if timestamp_us.is_none() => {
                timestamp_us = Some(ms_to_us(value.i64()?));
                seen |= 1 << 2;
            }
            b"b" | b"bids" => {
                bid = Some(value);
                seen |= 1 << 3;
            }
            b"B" => {
                bid_amount = Some(value);
                seen |= 1 << 4;
            }
            b"a" | b"asks" => {
                ask = Some(value);
                seen |= 1 << 5;
            }
            b"A" => {
                ask_amount = Some(value);
                seen |= 1 << 6;
            }
            _ => {}
        }
        match event_kind {
            Some(RawBboKind::BookTicker) if seen == BBO_BOOK_TICKER_REQUIRED => break,
            Some(RawBboKind::Depth) if (seen & BBO_DEPTH_REQUIRED) == BBO_DEPTH_REQUIRED => break,
            _ => {}
        }
    }

    let kind = match event_kind {
        Some(kind) => kind,
        None => {
            let stream_kind = payload
                .stream
                .and_then(|stream_value| stream_value.string_str())
                .and_then(raw_bbo_stream_kind);
            let inferred_kind = infer_raw_bbo_kind(bid, bid_amount, ask, ask_amount);
            if let (Some(stream_kind), Some(inferred_kind)) = (stream_kind, inferred_kind) {
                if stream_kind != inferred_kind {
                    return None;
                }
            }
            inferred_kind.or(stream_kind)?
        }
    };
    let symbol = match symbol {
        Some(symbol) => symbol,
        None => payload
            .stream
            .and_then(|stream_value| stream_value.string_str())
            .and_then(parse_stream_symbol_borrowed)?,
    };

    let (bid_price, bid_amount, ask_price, ask_amount) = match kind {
        RawBboKind::BookTicker => (
            bid?.f64()?,
            bid_amount?.f64()?,
            ask?.f64()?,
            ask_amount?.f64()?,
        ),
        RawBboKind::Depth => {
            let bid = parse_raw_top_level(bid?.array_bytes()?)?;
            let ask = parse_raw_top_level(ask?.array_bytes()?)?;
            (bid.price, bid.amount, ask.price, ask.amount)
        }
    };
    let out = RawBbo {
        symbol,
        timestamp_us: timestamp_us.unwrap_or(0),
        seq_id: seq_id?,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
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

pub fn parse_depth_bbo_raw_borrowed(raw: &[u8]) -> Option<RawBbo<'_>> {
    let mut payload = raw_payload_object(raw);
    let mut seen_event = false;
    let mut symbol = None;
    let mut timestamp_us = None;
    let mut seq_id = None;
    let mut bid = None;
    let mut ask = None;

    while let Some((key, value)) = payload.next_field() {
        match key {
            b"e" => {
                if value.raw != br#""depthUpdate""# {
                    return None;
                }
                seen_event = true;
            }
            b"s" => symbol = Some(value.string_str()?),
            b"E" => timestamp_us = Some(ms_to_us(value.i64()?)),
            b"T" if timestamp_us.is_none() => timestamp_us = Some(ms_to_us(value.i64()?)),
            b"u" | b"lastUpdateId" => seq_id = Some(value.i64()?),
            b"b" | b"bids" => bid = Some(parse_raw_top_level(value.array_bytes()?)?),
            b"a" | b"asks" => ask = Some(parse_raw_top_level(value.array_bytes()?)?),
            _ => {}
        }
        if seen_event
            && symbol.is_some()
            && timestamp_us.is_some()
            && seq_id.is_some()
            && bid.is_some()
            && ask.is_some()
        {
            break;
        }
    }

    let symbol = match symbol {
        Some(symbol) => symbol,
        None => payload
            .stream
            .and_then(|stream_value| stream_value.string_str())
            .and_then(parse_stream_symbol_borrowed)?,
    };
    if !seen_event
        && payload
            .stream
            .and_then(|stream_value| stream_value.string_str())
            .is_some_and(|stream| !stream.contains("@depth"))
    {
        return None;
    }
    let bid = bid?;
    let ask = ask?;
    let out = RawBbo {
        symbol,
        timestamp_us: timestamp_us.unwrap_or(0),
        seq_id: seq_id?,
        bid_price: bid.price,
        bid_amount: bid.amount,
        ask_price: ask.price,
        ask_amount: ask.amount,
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
    let mut payload = raw_payload_object(raw);
    let mut seen_event = false;
    let mut symbol = None;
    let mut trade_id = None;
    let mut timestamp_us = None;
    let mut price = None;
    let mut amount = None;
    let mut is_buyer_maker = false;
    let mut seen = 0u8;
    const TRADE_REQUIRED: u8 = 0b0011_1111;

    while let Some((key, value)) = payload.next_field() {
        match key {
            b"e" => {
                if value.raw != br#""trade""# {
                    return None;
                }
                seen_event = true;
            }
            b"s" => {
                symbol = Some(value.string_str()?);
                seen |= 1 << 0;
            }
            b"t" => {
                trade_id = Some(value.i64()?);
                seen |= 1 << 1;
            }
            b"E" => {
                timestamp_us = Some(ms_to_us(value.i64()?));
                seen |= 1 << 2;
            }
            b"T" if timestamp_us.is_none() => {
                timestamp_us = Some(ms_to_us(value.i64()?));
                seen |= 1 << 2;
            }
            b"p" => {
                price = Some(value.f64()?);
                seen |= 1 << 3;
            }
            b"q" => {
                amount = Some(value.f64()?);
                seen |= 1 << 4;
            }
            b"m" => {
                is_buyer_maker = value.bool()?;
                seen |= 1 << 5;
            }
            _ => {}
        }
        if seen_event && seen == TRADE_REQUIRED {
            break;
        }
    }

    if !seen_event
        && payload
            .stream
            .and_then(|stream_value| stream_value.string_str())
            .is_some_and(|stream| !stream.ends_with("@trade"))
    {
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

pub fn parse_event_time_ms_raw(raw: &[u8]) -> Option<i64> {
    let mut payload = raw_payload_object(raw);
    while let Some((key, value)) = payload.next_field() {
        if key == b"E" {
            return value.i64();
        }
    }
    None
}

pub fn parse_kline_raw_borrowed(raw: &[u8]) -> Option<RawKline<'_>> {
    let mut payload = raw_payload_object(raw);
    let mut symbol = None;
    let mut kline = None;

    while let Some((key, value)) = payload.next_field() {
        match key {
            b"s" => symbol = Some(value.string_str()?),
            b"k" => {
                let mut scanner = JsonObjectScanner::new(value.object_bytes()?);
                kline = Some(parse_kline_object_scanner(&mut scanner)?);
            }
            _ => {}
        }
        if symbol.is_some() && kline.is_some() {
            break;
        }
    }

    let kline = kline?;
    Some(RawKline {
        symbol: symbol?,
        open_price: kline.open_price,
        high_price: kline.high_price,
        low_price: kline.low_price,
        close_price: kline.close_price,
        volume: kline.volume,
        timestamp: kline.timestamp,
    })
}

#[derive(Debug, Clone, Copy)]
struct RawBookFields<'a> {
    symbol: &'a str,
    timestamp_us: i64,
    seq_id: i64,
    prev_seq_id: i64,
    first_update_id: i64,
    final_update_id: i64,
    gap_check: bool,
    is_snapshot: bool,
    bids_raw: &'a [u8],
    asks_raw: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
struct RawLevelsParsed {
    levels: RawLevels,
    count: usize,
    over_capacity: bool,
}

pub fn parse_incremental_raw(raw: &[u8]) -> Option<RawBookParse<'_>> {
    let fields = parse_incremental_raw_fields(raw)?;
    let bids = parse_raw_levels_capped(fields.bids_raw)?;
    let asks = parse_raw_levels_capped(fields.asks_raw)?;
    if !bids.over_capacity && !asks.over_capacity {
        Some(RawBookParse::Parsed(RawBook {
            symbol: fields.symbol,
            timestamp_us: fields.timestamp_us,
            seq_id: fields.seq_id,
            prev_seq_id: fields.prev_seq_id,
            first_update_id: fields.first_update_id,
            final_update_id: fields.final_update_id,
            gap_check: fields.gap_check,
            is_snapshot: fields.is_snapshot,
            bids: bids.levels,
            asks: asks.levels,
        }))
    } else {
        Some(RawBookParse::View(RawBookView {
            symbol: fields.symbol,
            timestamp_us: fields.timestamp_us,
            seq_id: fields.seq_id,
            prev_seq_id: fields.prev_seq_id,
            first_update_id: fields.first_update_id,
            final_update_id: fields.final_update_id,
            gap_check: fields.gap_check,
            is_snapshot: fields.is_snapshot,
            bids_raw: fields.bids_raw,
            asks_raw: fields.asks_raw,
            bids_count: bids.count,
            asks_count: asks.count,
        }))
    }
}

pub fn parse_incremental_raw_borrowed(raw: &[u8]) -> Option<RawBook<'_>> {
    let fields = parse_incremental_raw_fields(raw)?;
    Some(RawBook {
        symbol: fields.symbol,
        timestamp_us: fields.timestamp_us,
        seq_id: fields.seq_id,
        prev_seq_id: fields.prev_seq_id,
        first_update_id: fields.first_update_id,
        final_update_id: fields.final_update_id,
        gap_check: fields.gap_check,
        is_snapshot: fields.is_snapshot,
        bids: parse_raw_levels(fields.bids_raw)?,
        asks: parse_raw_levels(fields.asks_raw)?,
    })
}

pub fn parse_incremental_raw_view(raw: &[u8]) -> Option<RawBookView<'_>> {
    let fields = parse_incremental_raw_fields(raw)?;
    let bids_count = raw_levels_count(fields.bids_raw)?;
    let asks_count = raw_levels_count(fields.asks_raw)?;
    Some(RawBookView {
        symbol: fields.symbol,
        timestamp_us: fields.timestamp_us,
        seq_id: fields.seq_id,
        prev_seq_id: fields.prev_seq_id,
        first_update_id: fields.first_update_id,
        final_update_id: fields.final_update_id,
        gap_check: fields.gap_check,
        is_snapshot: fields.is_snapshot,
        bids_raw: fields.bids_raw,
        asks_raw: fields.asks_raw,
        bids_count,
        asks_count,
    })
}

fn parse_incremental_raw_fields(raw: &[u8]) -> Option<RawBookFields<'_>> {
    let mut payload = raw_payload_object(raw);
    let mut seen_event = false;
    let mut symbol = None;
    let mut timestamp_us = None;
    let mut first_update_id = None;
    let mut final_update_id = None;
    let mut last_update_id = None;
    let mut bids_raw = None;
    let mut asks_raw = None;
    let mut seen = 0u8;
    const RAW_BOOK_SYMBOL: u8 = 1 << 0;
    const RAW_BOOK_TIMESTAMP: u8 = 1 << 1;
    const RAW_BOOK_FIRST_ID: u8 = 1 << 2;
    const RAW_BOOK_FINAL_ID: u8 = 1 << 3;
    const RAW_BOOK_LAST_ID: u8 = 1 << 4;
    const RAW_BOOK_BIDS: u8 = 1 << 5;
    const RAW_BOOK_ASKS: u8 = 1 << 6;
    const RAW_BOOK_SNAPSHOT_REQUIRED: u8 =
        RAW_BOOK_SYMBOL | RAW_BOOK_LAST_ID | RAW_BOOK_BIDS | RAW_BOOK_ASKS;
    const RAW_BOOK_UPDATE_REQUIRED: u8 = RAW_BOOK_SYMBOL
        | RAW_BOOK_TIMESTAMP
        | RAW_BOOK_FIRST_ID
        | RAW_BOOK_FINAL_ID
        | RAW_BOOK_BIDS
        | RAW_BOOK_ASKS;

    while let Some((key, value)) = payload.next_field() {
        match key {
            b"e" => {
                if value.raw != br#""depthUpdate""# {
                    return None;
                }
                seen_event = true;
            }
            b"s" => {
                symbol = Some(value.string_str()?);
                seen |= RAW_BOOK_SYMBOL;
            }
            b"E" => {
                timestamp_us = Some(ms_to_us(value.i64()?));
                seen |= RAW_BOOK_TIMESTAMP;
            }
            b"T" if timestamp_us.is_none() => {
                timestamp_us = Some(ms_to_us(value.i64()?));
                seen |= RAW_BOOK_TIMESTAMP;
            }
            b"U" => {
                first_update_id = Some(value.i64()?);
                seen |= RAW_BOOK_FIRST_ID;
            }
            b"u" => {
                final_update_id = Some(value.i64()?);
                seen |= RAW_BOOK_FINAL_ID;
            }
            b"lastUpdateId" => {
                last_update_id = Some(value.i64()?);
                seen |= RAW_BOOK_LAST_ID;
            }
            b"b" | b"bids" => {
                bids_raw = Some(value.array_bytes()?);
                seen |= RAW_BOOK_BIDS;
            }
            b"a" | b"asks" => {
                asks_raw = Some(value.array_bytes()?);
                seen |= RAW_BOOK_ASKS;
            }
            _ => {}
        }
        if (seen & RAW_BOOK_SNAPSHOT_REQUIRED) == RAW_BOOK_SNAPSHOT_REQUIRED {
            break;
        }
        if seen_event && (seen & RAW_BOOK_UPDATE_REQUIRED) == RAW_BOOK_UPDATE_REQUIRED {
            break;
        }
    }

    let symbol = match symbol {
        Some(symbol) => symbol,
        None => payload
            .stream
            .and_then(|stream_value| stream_value.string_str())
            .and_then(parse_stream_symbol_borrowed)?,
    };
    let bids_raw = bids_raw.unwrap_or(b"[]");
    let asks_raw = asks_raw.unwrap_or(b"[]");

    if let Some(last_update_id) = last_update_id {
        return Some(RawBookFields {
            symbol,
            timestamp_us: timestamp_us.unwrap_or(0),
            seq_id: last_update_id,
            prev_seq_id: last_update_id,
            first_update_id: last_update_id,
            final_update_id: last_update_id,
            gap_check: false,
            is_snapshot: true,
            bids_raw,
            asks_raw,
        });
    }

    if !seen_event
        && payload
            .stream
            .and_then(|stream_value| stream_value.string_str())
            .is_some_and(|stream| !stream.contains("@depth"))
    {
        return None;
    }
    if (seen & (RAW_BOOK_BIDS | RAW_BOOK_ASKS)) == 0 {
        return None;
    }
    let first_update_id = first_update_id?;
    let final_update_id = final_update_id?;
    Some(RawBookFields {
        symbol,
        timestamp_us: timestamp_us.unwrap_or(0),
        seq_id: final_update_id,
        prev_seq_id: first_update_id.saturating_sub(1),
        first_update_id,
        final_update_id,
        gap_check: false,
        is_snapshot: false,
        bids_raw,
        asks_raw,
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

pub fn parse_derivatives_raw_borrowed<'a>(
    raw: &'a [u8],
    mut emit: impl FnMut(RawDerivative<'a>) -> Option<()>,
) -> Option<()> {
    let mut scanner = JsonObjectScanner::new(raw);
    scanner.skip_ws();
    if scanner.peek_byte() == Some(b'[') {
        return parse_derivative_value_at(&mut scanner, &mut emit);
    }
    let Some(key) = scanner.next_key() else {
        return None;
    };
    if key == b"data" {
        return parse_derivative_value_at(&mut scanner, &mut emit);
    }
    if key != b"stream" {
        let derivative = parse_derivative_object_after_key(&mut scanner, key)?;
        emit(derivative)?;
        return Some(());
    }
    scanner.skip_value()?;

    while let Some(key) = scanner.next_key() {
        if key == b"data" {
            return parse_derivative_value_at(&mut scanner, &mut emit);
        }
        scanner.skip_value()?;
    }
    None
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

fn parse_kline_object_scanner(scanner: &mut JsonObjectScanner<'_>) -> Option<RawKlineFields> {
    let mut is_closed = false;
    let mut open_price = None;
    let mut high_price = None;
    let mut low_price = None;
    let mut close_price = None;
    let mut volume = None;
    let mut timestamp = None;

    while let Some(key) = scanner.next_key() {
        match key {
            b"x" => {
                is_closed = scanner.take_value()?.bool()?;
                if !is_closed {
                    return None;
                }
            }
            b"o" => open_price = Some(scanner.take_value()?.f64()?),
            b"h" => high_price = Some(scanner.take_value()?.f64()?),
            b"l" => low_price = Some(scanner.take_value()?.f64()?),
            b"c" => close_price = Some(scanner.take_value()?.f64()?),
            b"v" => volume = Some(scanner.take_value()?.f64()?),
            b"t" => timestamp = Some(scanner.take_value()?.i64()?),
            _ => scanner.skip_value()?,
        }
        if is_closed
            && open_price.is_some()
            && high_price.is_some()
            && low_price.is_some()
            && close_price.is_some()
            && volume.is_some()
            && timestamp.is_some()
        {
            break;
        }
    }

    if !is_closed {
        return None;
    }
    Some(RawKlineFields {
        open_price: open_price?,
        high_price: high_price?,
        low_price: low_price?,
        close_price: close_price?,
        volume: volume?,
        timestamp: timestamp?,
    })
}

fn parse_derivative_value_at<'a>(
    scanner: &mut JsonObjectScanner<'a>,
    emit: &mut impl FnMut(RawDerivative<'a>) -> Option<()>,
) -> Option<()> {
    scanner.skip_ws();
    match scanner.peek_byte()? {
        b'[' => {
            scanner.start_array()?;
            loop {
                scanner.skip_ws();
                match scanner.peek_byte()? {
                    b']' => {
                        scanner.pos += 1;
                        return Some(());
                    }
                    b',' => {
                        scanner.pos += 1;
                    }
                    b'{' => {
                        let derivative = parse_derivative_object_scanner(scanner)?;
                        emit(derivative)?;
                    }
                    _ => return None,
                }
            }
        }
        b'{' => {
            let derivative = parse_derivative_object_scanner(scanner)?;
            emit(derivative)?;
            Some(())
        }
        _ => None,
    }
}

fn parse_derivative_object_scanner<'a>(
    scanner: &mut JsonObjectScanner<'a>,
) -> Option<RawDerivative<'a>> {
    let first_key = scanner.next_key()?;
    parse_derivative_object_after_key(scanner, first_key)
}

fn parse_derivative_object_after_key<'a>(
    scanner: &mut JsonObjectScanner<'a>,
    first_key: &'a [u8],
) -> Option<RawDerivative<'a>> {
    let mut event = None;
    let mut symbol = None;
    let mut timestamp_us = None;
    let mut mark_price = None;
    let mut index_price = None;
    let mut funding_rate = None;
    let mut next_funding_time_us = None;
    let mut liquidation: Option<RawLiquidationFields<'a>> = None;
    let mut key = Some(first_key);

    while let Some(current_key) = key.take().or_else(|| scanner.next_key()) {
        match current_key {
            b"e" => {
                event = Some(match scanner.take_value()?.raw {
                    br#""markPriceUpdate""# => RawDerivativeKind::MarkPrice,
                    br#""forceOrder""# => RawDerivativeKind::Liquidation,
                    _ => return None,
                })
            }
            b"E" => timestamp_us = Some(ms_to_us(scanner.take_value()?.i64()?)),
            b"s" => symbol = Some(scanner.take_value()?.string_str()?),
            b"p" => mark_price = Some(scanner.take_value()?.f64()?),
            b"i" => index_price = Some(scanner.take_value()?.f64()?),
            b"r" => funding_rate = Some(scanner.take_value()?.f64()?),
            b"T" => next_funding_time_us = Some(ms_to_us(scanner.take_value()?.i64()?)),
            b"o" => liquidation = Some(parse_liquidation_object_scanner(scanner)?),
            _ => scanner.skip_value()?,
        }
        if event == Some(RawDerivativeKind::MarkPrice)
            && symbol.is_some()
            && timestamp_us.is_some()
            && mark_price.is_some()
            && index_price.is_some()
            && funding_rate.is_some()
            && next_funding_time_us.is_some()
        {
            scanner.skip_rest_of_object()?;
            break;
        }
    }

    match event? {
        RawDerivativeKind::MarkPrice => Some(RawDerivative::MarkPrice {
            symbol: symbol?,
            mark_price,
            index_price,
            funding_rate,
            next_funding_time_us,
            timestamp_us: timestamp_us.unwrap_or(0),
        }),
        RawDerivativeKind::Liquidation => {
            let liquidation = liquidation?;
            Some(RawDerivative::Liquidation {
                symbol: liquidation.symbol,
                side: liquidation.side,
                amount: liquidation.amount,
                price: liquidation.price,
                timestamp_us: liquidation
                    .order_timestamp_us
                    .unwrap_or_else(|| timestamp_us.unwrap_or(0)),
            })
        }
    }
}

fn parse_liquidation_object_scanner<'a>(
    scanner: &mut JsonObjectScanner<'a>,
) -> Option<RawLiquidationFields<'a>> {
    let mut symbol = None;
    let mut side = None;
    let mut amount = None;
    let mut price = None;
    let mut order_timestamp_us = None;

    while let Some(key) = scanner.next_key() {
        match key {
            b"s" => symbol = Some(scanner.take_value()?.string_str()?),
            b"S" => {
                side = Some(match scanner.take_value()?.raw {
                    br#""BUY""# => 'B',
                    br#""SELL""# => 'S',
                    _ => return None,
                })
            }
            b"z" => amount = Some(scanner.take_value()?.f64()?),
            b"ap" => price = Some(scanner.take_value()?.f64()?),
            b"T" => order_timestamp_us = Some(ms_to_us(scanner.take_value()?.i64()?)),
            _ => scanner.skip_value()?,
        }
        if symbol.is_some()
            && side.is_some()
            && amount.is_some()
            && price.is_some()
            && order_timestamp_us.is_some()
        {
            scanner.skip_rest_of_object()?;
            break;
        }
    }

    let out = RawLiquidationFields {
        symbol: symbol?,
        side: side?,
        amount: amount?,
        price: price?,
        order_timestamp_us,
    };
    if out.amount <= 0.0 || out.price <= 0.0 {
        return None;
    }
    Some(out)
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

fn parse_raw_levels_capped(raw: &[u8]) -> Option<RawLevelsParsed> {
    let raw = trim_ascii(raw);
    if raw.first() != Some(&b'[') || raw.last() != Some(&b']') {
        return None;
    }

    let mut levels = RawLevels::new();
    let mut count = 0usize;
    let mut over_capacity = false;
    let mut pos = 1usize;
    loop {
        skip_ws_at(raw, &mut pos);
        match raw.get(pos).copied()? {
            b']' => {
                return Some(RawLevelsParsed {
                    levels,
                    count,
                    over_capacity,
                })
            }
            b',' => {
                pos += 1;
                continue;
            }
            b'[' => {
                if levels.len() < RAW_DEPTH_LEVEL_CAP {
                    let level = parse_raw_level(raw, &mut pos)?;
                    levels.push(level)?;
                } else {
                    over_capacity = true;
                    skip_raw_level(raw, &mut pos)?;
                }
                count += 1;
            }
            _ => return None,
        }
    }
}

pub fn raw_level_at(raw: &[u8], index: usize) -> Option<Level> {
    raw_levels_iter(raw)?.nth(index)
}

pub fn raw_levels_iter(raw: &[u8]) -> Option<RawLevelIter<'_>> {
    RawLevelIter::new(raw)
}

fn parse_raw_top_level(raw: &[u8]) -> Option<Level> {
    let raw = trim_ascii(raw);
    if raw.first() != Some(&b'[') || raw.last() != Some(&b']') {
        return None;
    }

    let mut pos = 1usize;
    loop {
        skip_ws_at(raw, &mut pos);
        match raw.get(pos).copied()? {
            b']' => return None,
            b',' => pos += 1,
            b'[' => return parse_raw_level(raw, &mut pos),
            _ => return None,
        }
    }
}

fn raw_levels_count(raw: &[u8]) -> Option<usize> {
    let raw = trim_ascii(raw);
    if raw.first() != Some(&b'[') || raw.last() != Some(&b']') {
        return None;
    }

    let mut count = 0usize;
    let mut pos = 1usize;
    loop {
        skip_ws_at(raw, &mut pos);
        match raw.get(pos).copied()? {
            b']' => return Some(count),
            b',' => pos += 1,
            b'[' => {
                parse_raw_level(raw, &mut pos)?;
                count += 1;
            }
            _ => return None,
        }
    }
}

pub struct RawLevelIter<'a> {
    raw: &'a [u8],
    pos: usize,
    done: bool,
}

impl<'a> RawLevelIter<'a> {
    fn new(raw: &'a [u8]) -> Option<Self> {
        let raw = trim_ascii(raw);
        if raw.first() != Some(&b'[') || raw.last() != Some(&b']') {
            return None;
        }
        Some(Self {
            raw,
            pos: 1,
            done: false,
        })
    }
}

impl Iterator for RawLevelIter<'_> {
    type Item = Level;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            skip_ws_at(self.raw, &mut self.pos);
            match self.raw.get(self.pos).copied()? {
                b']' => {
                    self.done = true;
                    return None;
                }
                b',' => self.pos += 1,
                b'[' => return parse_raw_level(self.raw, &mut self.pos),
                _ => {
                    self.done = true;
                    return None;
                }
            }
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

fn skip_raw_level(raw: &[u8], pos: &mut usize) -> Option<()> {
    if raw.get(*pos) != Some(&b'[') {
        return None;
    }
    *pos += 1;
    let mut depth = 1usize;
    while let Some(&b) = raw.get(*pos) {
        match b {
            b'"' => skip_raw_string(raw, pos)?,
            b'[' => {
                depth += 1;
                *pos += 1;
            }
            b']' => {
                depth = depth.checked_sub(1)?;
                *pos += 1;
                if depth == 0 {
                    return Some(());
                }
            }
            _ => *pos += 1,
        }
    }
    None
}

fn skip_raw_string(raw: &[u8], pos: &mut usize) -> Option<()> {
    if raw.get(*pos) != Some(&b'"') {
        return None;
    }
    *pos += 1;
    while let Some(&b) = raw.get(*pos) {
        match b {
            b'\\' => return None,
            b'"' => {
                *pos += 1;
                return Some(());
            }
            _ => *pos += 1,
        }
    }
    None
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
    fast_float::parse::<f64, _>(bytes).ok()
}

fn parse_i64_bytes(raw: &[u8]) -> Option<i64> {
    match raw.first().copied() {
        Some(b'+') => atoi_simd::parse::<i64>(&raw[1..]).ok(),
        Some(_) => atoi_simd::parse::<i64>(raw).ok(),
        None => None,
    }
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
    let end = stream_symbol_end(stream);
    Some(stream[..end].to_ascii_uppercase())
}

fn parse_stream_symbol_borrowed(stream: &str) -> Option<&str> {
    let bytes = stream.as_bytes();
    let mut end = 0usize;
    while end < bytes.len() {
        let b = bytes[end];
        if b == b'@' {
            break;
        }
        if b.is_ascii_lowercase() {
            return None;
        }
        end += 1;
    }
    Some(&stream[..end])
}

fn raw_bbo_stream_kind(stream: &str) -> Option<RawBboKind> {
    let channel = stream_channel(stream)?;
    if channel == "bookTicker" {
        Some(RawBboKind::BookTicker)
    } else if channel.starts_with("depth") {
        Some(RawBboKind::Depth)
    } else {
        None
    }
}

fn stream_symbol_end(stream: &str) -> usize {
    let bytes = stream.as_bytes();
    let mut end = 0usize;
    while end < bytes.len() && bytes[end] != b'@' {
        end += 1;
    }
    end
}

fn stream_channel(stream: &str) -> Option<&str> {
    let bytes = stream.as_bytes();
    let mut start = 0usize;
    while start < bytes.len() && bytes[start] != b'@' {
        start += 1;
    }
    if start == bytes.len() {
        return None;
    }
    start += 1;
    let mut end = start;
    while end < bytes.len() && bytes[end] != b'@' {
        end += 1;
    }
    Some(&stream[start..end])
}

fn infer_raw_bbo_kind(
    bid: Option<JsonValue<'_>>,
    bid_amount: Option<JsonValue<'_>>,
    ask: Option<JsonValue<'_>>,
    ask_amount: Option<JsonValue<'_>>,
) -> Option<RawBboKind> {
    match (
        bid?.raw.first().copied()?,
        bid_amount.is_some(),
        ask?.raw.first().copied()?,
        ask_amount.is_some(),
    ) {
        (b'[', false, b'[', false) => Some(RawBboKind::Depth),
        (_, true, _, true) => Some(RawBboKind::BookTicker),
        _ => None,
    }
}

struct RawPayloadObject<'a> {
    scanner: JsonObjectScanner<'a>,
    stream: Option<JsonValue<'a>>,
    first_field: Option<(&'a [u8], JsonValue<'a>)>,
}

impl<'a> RawPayloadObject<'a> {
    fn next_field(&mut self) -> Option<(&'a [u8], JsonValue<'a>)> {
        self.first_field
            .take()
            .or_else(|| self.scanner.next_field())
    }
}

fn raw_payload_object(raw: &[u8]) -> RawPayloadObject<'_> {
    let mut scanner = JsonObjectScanner::new(raw);
    let mut data = None;
    let mut stream = None;
    while let Some(key) = scanner.next_key() {
        if key != b"stream" && key != b"data" && stream.is_none() && data.is_none() {
            let Some(value) = scanner.take_value() else {
                break;
            };
            return RawPayloadObject {
                scanner,
                stream: None,
                first_field: Some((key, value)),
            };
        }
        if key == b"stream" {
            if let Some(value) = scanner.take_value() {
                stream = Some(value);
                continue;
            }
            break;
        }
        if key == b"data" {
            if scanner.value_starts_object() && stream.is_some() {
                return RawPayloadObject {
                    scanner: scanner.scanner_at_value(),
                    stream,
                    first_field: None,
                };
            }
            let Some(value) = scanner.take_value() else {
                break;
            };
            data = value.object_bytes();
            continue;
        }
        if scanner.skip_value().is_none() {
            break;
        }
    }
    RawPayloadObject {
        scanner: JsonObjectScanner::new(data.unwrap_or(raw)),
        stream,
        first_field: None,
    }
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
        if self.raw.first() == Some(&b'{') && self.raw.last() == Some(&b'}') {
            Some(self.raw)
        } else {
            None
        }
    }

    fn array_bytes(self) -> Option<&'a [u8]> {
        if self.raw.first() == Some(&b'[') && self.raw.last() == Some(&b']') {
            Some(self.raw)
        } else {
            None
        }
    }

    fn number_bytes(self) -> Option<&'a [u8]> {
        if self.raw.first() == Some(&b'"') && self.raw.last() == Some(&b'"') {
            Some(&self.raw[1..self.raw.len() - 1])
        } else {
            Some(self.raw)
        }
    }

    fn i64(self) -> Option<i64> {
        parse_i64_bytes(self.number_bytes()?)
    }

    fn f64(self) -> Option<f64> {
        fast_float::parse::<f64, _>(self.number_bytes()?).ok()
    }

    fn bool(self) -> Option<bool> {
        match self.raw {
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
        let key = self.next_key()?;
        let value = self.take_value()?;
        Some((key, value))
    }

    fn next_key(&mut self) -> Option<&'a [u8]> {
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
        Some(&self.raw[key.0..key.1])
    }

    fn value_starts_object(&self) -> bool {
        self.raw.get(self.pos) == Some(&b'{')
    }

    fn peek_byte(&self) -> Option<u8> {
        self.raw.get(self.pos).copied()
    }

    fn start_array(&mut self) -> Option<()> {
        if self.raw.get(self.pos) != Some(&b'[') {
            return None;
        }
        self.pos += 1;
        Some(())
    }

    fn scanner_at_value(&self) -> JsonObjectScanner<'a> {
        JsonObjectScanner {
            raw: self.raw,
            pos: self.pos,
        }
    }

    fn take_value(&mut self) -> Option<JsonValue<'a>> {
        let start = self.pos;
        self.skip_value()?;
        let end = self.pos;
        Some(JsonValue {
            raw: &self.raw[start..end],
        })
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

    fn skip_rest_of_object(&mut self) -> Option<()> {
        loop {
            self.skip_ws();
            match self.raw.get(self.pos)? {
                b',' => {
                    self.pos += 1;
                    self.skip_ws();
                    self.take_string_inner()?;
                    self.skip_ws();
                    if self.raw.get(self.pos) != Some(&b':') {
                        return None;
                    }
                    self.pos += 1;
                    self.skip_ws();
                    self.skip_value()?;
                }
                b'}' => {
                    self.pos += 1;
                    return Some(());
                }
                _ => return None,
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
        parse_bbo_raw_borrowed, parse_book_ticker_bbo_raw, parse_book_ticker_bbo_raw_borrowed,
        parse_depth_bbo_raw_borrowed, parse_derivatives_raw_borrowed, parse_event_time_ms_raw,
        parse_i64_bytes, parse_incremental_raw, parse_incremental_raw_borrowed,
        parse_incremental_raw_view, parse_kline_raw_borrowed, parse_trade_raw,
        parse_trade_raw_borrowed, raw_level_at, RawBookParse, RawDerivative, RAW_DEPTH_LEVEL_CAP,
    };

    fn raw_derivative_symbol(derivative: RawDerivative<'_>) -> &str {
        match derivative {
            RawDerivative::MarkPrice { symbol, .. } | RawDerivative::Liquidation { symbol, .. } => {
                symbol
            }
        }
    }

    #[test]
    fn parses_i64_from_ascii_bytes() {
        assert_eq!(parse_i64_bytes(b"0"), Some(0));
        assert_eq!(parse_i64_bytes(b"+42"), Some(42));
        assert_eq!(parse_i64_bytes(b"-42"), Some(-42));
        assert_eq!(parse_i64_bytes(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64_bytes(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_i64_bytes(b"9223372036854775808"), None);
        assert_eq!(parse_i64_bytes(b"-9223372036854775809"), None);
        assert_eq!(parse_i64_bytes(b""), None);
        assert_eq!(parse_i64_bytes(b"12x"), None);
    }

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

        let routed = parse_bbo_raw_borrowed(raw).expect("routed book ticker");
        assert_eq!(routed.symbol, "BTCUSDT");
        assert_eq!(routed.seq_id, 22345);
        assert!((routed.ask_amount - 50.0).abs() < 1e-9);
    }

    #[test]
    fn parses_combined_book_ticker_when_data_precedes_stream() {
        let raw = br#"{
            "data":{"e":"bookTicker","u":22345,"s":"BTCUSDT","b":"25.0","B":"100","a":"25.1","A":"50","E":1700000000002},
            "stream":"btcusdt@bookTicker"
        }"#;

        let bbo = parse_bbo_raw_borrowed(raw).expect("routed book ticker");

        assert_eq!(bbo.symbol, "BTCUSDT");
        assert_eq!(bbo.seq_id, 22345);
        assert!((bbo.bid_amount - 100.0).abs() < 1e-9);
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
    fn parses_depth_top_bbo_without_value_tree() {
        let raw = br#"{"stream":"btcusdt@depth5@0ms","data":{"e":"depthUpdate","E":1700000000001,"s":"BTCUSDT","U":1,"u":2,
            "b":[["25.0","1"],["24.9","2"]],"a":[["25.1","3"],["25.2","4"]]}}"#;

        let bbo = parse_depth_bbo_raw_borrowed(raw).expect("depth top bbo");

        assert_eq!(bbo.symbol, "BTCUSDT");
        assert_eq!(bbo.timestamp_us, 1_700_000_000_001_000);
        assert_eq!(bbo.seq_id, 2);
        assert!((bbo.bid_price - 25.0).abs() < 1e-9);
        assert!((bbo.bid_amount - 1.0).abs() < 1e-9);
        assert!((bbo.ask_price - 25.1).abs() < 1e-9);
        assert!((bbo.ask_amount - 3.0).abs() < 1e-9);

        let routed = parse_bbo_raw_borrowed(raw).expect("routed depth top bbo");
        assert_eq!(routed.symbol, "BTCUSDT");
        assert_eq!(routed.seq_id, 2);
        assert!((routed.bid_amount - 1.0).abs() < 1e-9);
    }

    #[test]
    fn raw_depth_top_bbo_rejects_zero_top_levels() {
        let raw =
            br#"{"stream":"btcusdt@depth5@0ms","data":{"e":"depthUpdate","s":"BTCUSDT","U":1,"u":2,
            "b":[["25.0","0"]],"a":[["25.1","3"]]}}"#;

        assert!(parse_depth_bbo_raw_borrowed(raw).is_none());
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

        let routed = parse_incremental_raw(raw).expect("routed depth update");
        let RawBookParse::Parsed(routed) = routed else {
            panic!("small raw depth update should stay parsed");
        };
        assert_eq!(routed.symbol, "BTCUSDT");
        assert_eq!(routed.bids.len(), 2);
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

        let routed = parse_incremental_raw(&raw).expect("routed over-capacity depth");
        let RawBookParse::View(routed) = routed else {
            panic!("over-capacity depth should route to raw view");
        };
        assert_eq!(routed.bids_count, RAW_DEPTH_LEVEL_CAP + 1);
        assert_eq!(routed.asks_count, 0);
    }

    #[test]
    fn raw_depth_view_handles_over_capacity_books() {
        let mut raw = br#"{"e":"depthUpdate","s":"BTCUSDT","U":1,"u":2,"b":["#.to_vec();
        for i in 0..=RAW_DEPTH_LEVEL_CAP {
            if i > 0 {
                raw.push(b',');
            }
            raw.extend_from_slice(br#"["25.0","1"]"#);
        }
        raw.extend_from_slice(br#"],"a":[["25.1","2"]]}"#);

        let view = parse_incremental_raw_view(&raw).expect("raw depth view");

        assert_eq!(view.symbol, "BTCUSDT");
        assert_eq!(view.bids_count, RAW_DEPTH_LEVEL_CAP + 1);
        assert_eq!(view.asks_count, 1);
        let bid = raw_level_at(view.bids_raw, RAW_DEPTH_LEVEL_CAP).expect("last bid");
        assert!((bid.price - 25.0).abs() < 1e-9);
        assert!((bid.amount - 1.0).abs() < 1e-9);
    }

    #[test]
    fn parses_event_time_without_value_tree() {
        let raw = br#"{"e":"depthUpdate","E":1700000000001,"s":"BTCUSDT","U":101,"u":103,
            "b":[["25.0","100"]],"a":[["25.1","50"]]}"#;

        assert_eq!(parse_event_time_ms_raw(raw), Some(1_700_000_000_001));
    }

    #[test]
    fn parses_closed_kline_without_value_tree() {
        let raw = br#"{"e":"kline","E":1700000000001,"s":"BTCUSDT",
            "k":{"t":1700000000000,"o":"25.0","h":"26.0","l":"24.5","c":"25.5","v":"123.4","x":true}}"#;

        let kline = parse_kline_raw_borrowed(raw).expect("closed kline");

        assert_eq!(kline.symbol, "BTCUSDT");
        assert_eq!(kline.timestamp, 1_700_000_000_000);
        assert!((kline.open_price - 25.0).abs() < 1e-9);
        assert!((kline.high_price - 26.0).abs() < 1e-9);
        assert!((kline.low_price - 24.5).abs() < 1e-9);
        assert!((kline.close_price - 25.5).abs() < 1e-9);
        assert!((kline.volume - 123.4).abs() < 1e-9);
    }

    #[test]
    fn parses_closed_kline_when_kline_precedes_symbol() {
        let raw = br#"{"e":"kline","E":1700000000001,
            "k":{"t":1700000000000,"o":"25.0","h":"26.0","l":"24.5","c":"25.5","v":"123.4","x":true},
            "s":"BTCUSDT"}"#;

        let kline = parse_kline_raw_borrowed(raw).expect("closed kline");

        assert_eq!(kline.symbol, "BTCUSDT");
        assert_eq!(kline.timestamp, 1_700_000_000_000);
        assert!((kline.close_price - 25.5).abs() < 1e-9);
    }

    #[test]
    fn raw_kline_rejects_open_candle() {
        let raw = br#"{"e":"kline","s":"BTCUSDT",
            "k":{"t":1700000000000,"o":"25.0","h":"26.0","l":"24.5","c":"25.5","v":"123.4","x":false}}"#;

        assert!(parse_kline_raw_borrowed(raw).is_none());
    }

    #[test]
    fn parses_mark_price_derivatives_without_value_tree() {
        let raw = br#"{"data":{"e":"markPriceUpdate","E":1700000000001,"s":"BTCUSDT","p":"25.0","i":"24.9","r":"0.0001","T":1700003600000}}"#;
        let mut out = Vec::new();

        parse_derivatives_raw_borrowed(raw, |derivative| {
            out.push(derivative);
            Some(())
        })
        .expect("raw mark price");

        assert_eq!(
            out,
            vec![RawDerivative::MarkPrice {
                symbol: "BTCUSDT",
                mark_price: Some(25.0),
                index_price: Some(24.9),
                funding_rate: Some(0.0001),
                next_funding_time_us: Some(1_700_003_600_000_000),
                timestamp_us: 1_700_000_000_001_000,
            }]
        );
    }

    #[test]
    fn parses_direct_mark_price_derivative_without_data_wrapper() {
        let raw = br#"{"e":"markPriceUpdate","E":1700000000001,"s":"BTCUSDT","p":"25.0","i":"24.9","r":"0.0001","T":1700003600000}"#;
        let mut out = Vec::new();

        parse_derivatives_raw_borrowed(raw, |derivative| {
            out.push(derivative);
            Some(())
        })
        .expect("raw mark price");

        assert_eq!(out.len(), 1);
        assert_eq!(raw_derivative_symbol(out[0]), "BTCUSDT");
    }

    #[test]
    fn parses_mark_price_array_without_value_tree() {
        let raw = br#"{"data":[
            {"e":"markPriceUpdate","E":1700000000001,"s":"BTCUSDT","p":"25.0","i":"24.9","r":"0.0001","T":1700003600000},
            {"e":"markPriceUpdate","E":1700000000002,"s":"ETHUSDT","p":"26.0","i":"25.9","r":"0.0002","T":1700003600000}
        ]}"#;
        let mut symbols = Vec::new();

        parse_derivatives_raw_borrowed(raw, |derivative| {
            if let RawDerivative::MarkPrice {
                symbol,
                timestamp_us,
                ..
            } = derivative
            {
                symbols.push((symbol, timestamp_us));
            }
            Some(())
        })
        .expect("raw mark price array");

        assert_eq!(
            symbols,
            vec![
                ("BTCUSDT", 1_700_000_000_001_000),
                ("ETHUSDT", 1_700_000_000_002_000)
            ]
        );
    }

    #[test]
    fn parses_liquidation_without_value_tree() {
        let raw = br#"{"data":{"e":"forceOrder","E":1700000000001,
            "o":{"s":"BTCUSDT","S":"SELL","z":"10","ap":"25.2","T":1700000000000}}}"#;
        let mut out = Vec::new();

        parse_derivatives_raw_borrowed(raw, |derivative| {
            out.push(derivative);
            Some(())
        })
        .expect("raw liquidation");

        assert_eq!(
            out,
            vec![RawDerivative::Liquidation {
                symbol: "BTCUSDT",
                side: 'S',
                amount: 10.0,
                price: 25.2,
                timestamp_us: 1_700_000_000_000_000,
            }]
        );
    }

    #[test]
    fn parses_liquidation_with_fields_after_order() {
        let raw = br#"{"data":{"e":"forceOrder",
            "o":{"s":"BTCUSDT","S":"SELL","z":"10","ap":"25.2"},
            "E":1700000000001}}"#;
        let mut out = Vec::new();

        parse_derivatives_raw_borrowed(raw, |derivative| {
            out.push(derivative);
            Some(())
        })
        .expect("raw liquidation");

        assert_eq!(
            out,
            vec![RawDerivative::Liquidation {
                symbol: "BTCUSDT",
                side: 'S',
                amount: 10.0,
                price: 25.2,
                timestamp_us: 1_700_000_000_001_000,
            }]
        );
    }

    #[test]
    fn raw_derivatives_propagates_emit_stop() {
        let raw = br#"{"data":[
            {"e":"markPriceUpdate","E":1700000000001,"s":"BTCUSDT","p":"25.0","i":"24.9","r":"0.0001","T":1700003600000},
            {"e":"markPriceUpdate","E":1700000000002,"s":"ETHUSDT","p":"26.0","i":"25.9","r":"0.0002","T":1700003600000}
        ]}"#;
        let mut count = 0usize;

        assert!(parse_derivatives_raw_borrowed(raw, |_derivative| {
            count += 1;
            None
        })
        .is_none());
        assert_eq!(count, 1);
    }
}
