use serde_json::Value;

pub const SBE_SCHEMA_ID: u16 = 1;
pub const SBE_TEMPLATE_BBO: u16 = 1;
pub const SBE_TEMPLATE_TRADE: u16 = 2;
pub const SBE_TEMPLATE_ORDER_BOOK_UPDATE: u16 = 3;
pub const SBE_TEMPLATE_BOOK: u16 = 4;
pub const SBE_TEMPLATE_BOOK_UPDATE: u16 = 5;
pub const SBE_TEMPLATE_KLINE: u16 = 8;
pub const SBE_TEMPLATE_TICKER: u16 = 9;

const SBE_HEADER_SIZE: usize = 8;
const SBE_BBO_ROOT_MIN: usize = 59;

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

pub fn parse_bbo_json(value: &Value) -> Option<Bbo> {
    let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
    if event != "update" || !(channel.ends_with(".book_ticker") || channel.ends_with(".tickers")) {
        return None;
    }
    let res = value.get("result")?.as_object()?;
    let symbol = parse_symbol(res)?;
    let seq_id = res.get("u").and_then(parse_i64_loose).unwrap_or(0);
    let timestamp_us = res
        .get("t")
        .and_then(parse_i64_loose)
        .or_else(|| res.get("time_ms").and_then(parse_i64_loose))
        .or_else(|| value.get("time_ms").and_then(parse_i64_loose))
        .map(normalize_ts_to_us)
        .or_else(|| {
            value
                .get("time")
                .and_then(parse_i64_loose)
                .map(|s| s.saturating_mul(1_000_000))
        })
        .unwrap_or(0);
    let bid_price = res
        .get("b")
        .and_then(parse_f64_loose)
        .or_else(|| res.get("highest_bid").and_then(parse_f64_loose))?;
    let ask_price = res
        .get("a")
        .and_then(parse_f64_loose)
        .or_else(|| res.get("lowest_ask").and_then(parse_f64_loose))?;
    let bid_amount = res
        .get("B")
        .and_then(parse_f64_loose)
        .or_else(|| res.get("best_bid_size").and_then(parse_f64_loose))
        .unwrap_or(0.0);
    let ask_amount = res
        .get("A")
        .and_then(parse_f64_loose)
        .or_else(|| res.get("best_ask_size").and_then(parse_f64_loose))
        .unwrap_or(0.0);
    if bid_price <= 0.0 || ask_price <= 0.0 {
        return None;
    }
    Some(Bbo {
        symbol: symbol.replace('_', "").to_ascii_uppercase(),
        timestamp_us,
        seq_id,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    })
}

pub fn parse_spot_book_ticker_bbo_raw(raw: &[u8]) -> Option<Bbo> {
    let envelope: SpotBookTickerEnvelope<'_> = serde_json::from_slice(raw).ok()?;
    if envelope.channel != "spot.book_ticker" || envelope.event != "update" {
        return None;
    }

    let bid_price = envelope.result.bid_price.to_f64()?;
    let ask_price = envelope.result.ask_price.to_f64()?;
    if bid_price <= 0.0 || ask_price <= 0.0 {
        return None;
    }

    let timestamp_us = envelope
        .result
        .timestamp_ms
        .as_ref()
        .and_then(JsonI64Field::to_i64)
        .or_else(|| {
            envelope
                .result
                .time_ms
                .as_ref()
                .and_then(JsonI64Field::to_i64)
        })
        .or_else(|| envelope.time_ms.as_ref().and_then(JsonI64Field::to_i64))
        .map(normalize_ts_to_us)
        .or_else(|| {
            envelope
                .time
                .as_ref()
                .and_then(JsonI64Field::to_i64)
                .map(|s| s.saturating_mul(1_000_000))
        })
        .unwrap_or(0);

    Some(Bbo {
        symbol: envelope.result.symbol.replace('_', "").to_ascii_uppercase(),
        timestamp_us,
        seq_id: envelope.result.seq_id.to_i64()?,
        bid_price,
        bid_amount: envelope
            .result
            .bid_amount
            .as_ref()
            .and_then(JsonF64Field::to_f64)
            .unwrap_or(0.0),
        ask_price,
        ask_amount: envelope
            .result
            .ask_amount
            .as_ref()
            .and_then(JsonF64Field::to_f64)
            .unwrap_or(0.0),
    })
}

#[derive(serde::Deserialize)]
struct SpotBookTickerEnvelope<'a> {
    channel: &'a str,
    event: &'a str,
    #[serde(borrow)]
    result: SpotBookTickerPayload<'a>,
    #[serde(default, borrow)]
    time_ms: Option<JsonI64Field<'a>>,
    #[serde(default, borrow)]
    time: Option<JsonI64Field<'a>>,
}

#[derive(serde::Deserialize)]
struct SpotBookTickerPayload<'a> {
    #[serde(rename = "s")]
    symbol: &'a str,
    #[serde(rename = "u", borrow)]
    seq_id: JsonI64Field<'a>,
    #[serde(rename = "t", default, borrow)]
    timestamp_ms: Option<JsonI64Field<'a>>,
    #[serde(default, borrow)]
    time_ms: Option<JsonI64Field<'a>>,
    #[serde(rename = "b", borrow)]
    bid_price: JsonF64Field<'a>,
    #[serde(rename = "B", default, borrow)]
    bid_amount: Option<JsonF64Field<'a>>,
    #[serde(rename = "a", borrow)]
    ask_price: JsonF64Field<'a>,
    #[serde(rename = "A", default, borrow)]
    ask_amount: Option<JsonF64Field<'a>>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum JsonI64Field<'a> {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Str(&'a str),
}

impl JsonI64Field<'_> {
    fn to_i64(&self) -> Option<i64> {
        match self {
            Self::Signed(v) => Some(*v),
            Self::Unsigned(v) => i64::try_from(*v).ok(),
            Self::Float(v) => Some(*v as i64),
            Self::Str(v) => v
                .parse::<i64>()
                .ok()
                .or_else(|| v.parse::<f64>().ok().map(|f| f as i64)),
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum JsonF64Field<'a> {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Str(&'a str),
}

impl JsonF64Field<'_> {
    fn to_f64(&self) -> Option<f64> {
        match self {
            Self::Signed(v) => Some(*v as f64),
            Self::Unsigned(v) => Some(*v as f64),
            Self::Float(v) => Some(*v),
            Self::Str(v) => v.parse::<f64>().ok(),
        }
    }
}

pub fn parse_trades_json(value: &Value) -> Vec<Trade> {
    let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
    if !channel.ends_with(".trades") || event != "update" {
        return Vec::new();
    }
    let fallback_timestamp_us = value
        .get("time_ms")
        .and_then(parse_i64_loose)
        .map(normalize_ts_to_us)
        .or_else(|| {
            value
                .get("time")
                .and_then(parse_i64_loose)
                .map(|s| s.saturating_mul(1_000_000))
        })
        .unwrap_or(0);
    let items = result_items(value);
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(symbol) = parse_symbol(obj) else {
            continue;
        };
        let price = obj
            .get("price")
            .and_then(parse_f64_loose)
            .or_else(|| obj.get("p").and_then(parse_f64_loose))
            .or_else(|| obj.get("last").and_then(parse_f64_loose))
            .unwrap_or(0.0);
        let raw_size = obj
            .get("size")
            .and_then(parse_f64_loose)
            .or_else(|| obj.get("amount").and_then(parse_f64_loose))
            .or_else(|| obj.get("qty").and_then(parse_f64_loose));
        let side = obj
            .get("side")
            .and_then(|v| v.as_str())
            .map(|s| s.to_ascii_lowercase())
            .and_then(|s| match s.as_str() {
                "buy" | "bid" => Some('B'),
                "sell" | "ask" => Some('S'),
                _ => None,
            })
            .or_else(|| raw_size.map(|sz| if sz >= 0.0 { 'B' } else { 'S' }))
            .unwrap_or('B');
        let amount = raw_size
            .map(f64::abs)
            .or_else(|| obj.get("q").and_then(parse_f64_loose))
            .unwrap_or(0.0);
        if price <= 0.0 || amount <= 0.0 {
            continue;
        }
        let timestamp_us = obj
            .get("create_time_ms")
            .and_then(parse_i64_loose)
            .or_else(|| obj.get("timestamp_ms").and_then(parse_i64_loose))
            .or_else(|| obj.get("create_time").and_then(parse_i64_loose))
            .or_else(|| obj.get("t").and_then(parse_i64_loose))
            .map(normalize_ts_to_us)
            .unwrap_or(fallback_timestamp_us);
        let trade_id = obj
            .get("id")
            .and_then(parse_i64_loose)
            .or_else(|| obj.get("trade_id").and_then(parse_i64_loose))
            .or_else(|| obj.get("i").and_then(parse_i64_loose))
            .unwrap_or(0);
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
    let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
    if !(channel.ends_with(".order_book_update") || channel.ends_with(".order_book")) {
        return Vec::new();
    }
    if event == "subscribe" || event == "unsubscribe" {
        return Vec::new();
    }

    let items = result_items(value);
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(symbol) = parse_symbol(obj) else {
            continue;
        };
        let bids = obj
            .get("b")
            .or_else(|| obj.get("bids"))
            .map(parse_gate_levels)
            .unwrap_or_default();
        let asks = obj
            .get("a")
            .or_else(|| obj.get("asks"))
            .map(parse_gate_levels)
            .unwrap_or_default();
        if bids.is_empty() && asks.is_empty() {
            continue;
        }
        let final_update_id = obj
            .get("u")
            .and_then(parse_i64_loose)
            .or_else(|| obj.get("last_id").and_then(parse_i64_loose))
            .or_else(|| obj.get("last_update_id").and_then(parse_i64_loose))
            .or_else(|| obj.get("seq").and_then(parse_i64_loose))
            .or_else(|| obj.get("id").and_then(parse_i64_loose))
            .unwrap_or(0);
        let first_update_id = obj
            .get("U")
            .and_then(parse_i64_loose)
            .or_else(|| obj.get("first_update_id").and_then(parse_i64_loose))
            .unwrap_or(final_update_id);
        let timestamp_us = obj
            .get("t")
            .and_then(parse_i64_loose)
            .or_else(|| obj.get("timestamp").and_then(parse_i64_loose))
            .or_else(|| obj.get("time_ms").and_then(parse_i64_loose))
            .or_else(|| value.get("time_ms").and_then(parse_i64_loose))
            .map(normalize_ts_to_us)
            .or_else(|| {
                value
                    .get("time")
                    .and_then(parse_i64_loose)
                    .map(|s| s.saturating_mul(1_000_000))
            })
            .unwrap_or(0);
        let is_snapshot = obj
            .get("is_snapshot")
            .and_then(|v| v.as_bool())
            .or_else(|| {
                obj.get("type")
                    .and_then(|v| v.as_str())
                    .map(|tp| tp.eq_ignore_ascii_case("snapshot"))
            })
            .unwrap_or_else(|| {
                channel.ends_with(".order_book")
                    || event.eq_ignore_ascii_case("snapshot")
                    || event.eq_ignore_ascii_case("all")
            });
        out.push(Book {
            symbol,
            timestamp_us,
            seq_id: final_update_id,
            prev_seq_id: i64::MIN,
            first_update_id,
            final_update_id,
            gap_check: false,
            is_snapshot,
            bids,
            asks,
        });
    }
    out
}

pub fn parse_derivatives_json(value: &Value) -> Vec<Derivative> {
    let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
    if !channel.ends_with(".tickers") || event != "update" {
        return Vec::new();
    }
    let timestamp_us = value
        .get("time_ms")
        .and_then(parse_i64_loose)
        .map(normalize_ts_to_us)
        .or_else(|| {
            value
                .get("time")
                .and_then(parse_i64_loose)
                .map(|s| s.saturating_mul(1_000_000))
        })
        .unwrap_or(0);
    let items = result_items(value);
    let mut out = Vec::with_capacity(items.len() * 3);
    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(symbol) = parse_symbol(obj) else {
            continue;
        };
        if let Some(price) = obj.get("mark_price").and_then(parse_f64_loose) {
            if price > 0.0 {
                out.push(Derivative::MarkPrice {
                    symbol: symbol.clone(),
                    price,
                    timestamp_us,
                });
            }
        }
        if let Some(price) = obj.get("index_price").and_then(parse_f64_loose) {
            if price > 0.0 {
                out.push(Derivative::IndexPrice {
                    symbol: symbol.clone(),
                    price,
                    timestamp_us,
                });
            }
        }
        if let Some(funding_rate) = obj.get("funding_rate").and_then(parse_f64_loose) {
            out.push(Derivative::FundingRate {
                symbol,
                funding_rate,
                next_funding_time_us: 0,
                timestamp_us,
            });
        }
    }
    out
}

pub fn parse_sbe_bbo(raw: &[u8]) -> Option<Bbo> {
    let header = sbe_header(raw)?;
    if header.template_id != SBE_TEMPLATE_BBO {
        return None;
    }
    let body = SBE_HEADER_SIZE;
    if raw.len() < body + header.block_length || header.block_length < SBE_BBO_ROOT_MIN {
        return None;
    }
    let timestamp_us = read_i64_le(raw, body + 9)?;
    let seq_id = read_i64_le(raw, body + 17)?;
    let px_exp = read_i8(raw, body + 25)?;
    let sz_exp = read_i8(raw, body + 26)?;
    let ask_px_m = read_i64_le(raw, body + 27)?;
    let ask_sz_m = read_i64_le(raw, body + 35)?;
    let bid_px_m = read_i64_le(raw, body + 43)?;
    let bid_sz_m = read_i64_le(raw, body + 51)?;
    let mut off = body + header.block_length;
    off = sbe_var_string_skip(raw, off)?;
    let (symbol, _) = sbe_var_string(raw, off)?;
    let bid_price = mantissa_to_f64(bid_px_m, px_exp);
    let ask_price = mantissa_to_f64(ask_px_m, px_exp);
    let bid_amount = mantissa_to_f64(bid_sz_m, sz_exp);
    let ask_amount = mantissa_to_f64(ask_sz_m, sz_exp);
    if bid_price <= 0.0 || ask_price <= 0.0 || bid_amount <= 0.0 || ask_amount <= 0.0 {
        return None;
    }
    Some(Bbo {
        symbol: symbol.replace('_', "").to_ascii_uppercase(),
        timestamp_us,
        seq_id,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    })
}

pub fn parse_sbe_trades(raw: &[u8]) -> Vec<Trade> {
    let Some(header) = sbe_header(raw) else {
        return Vec::new();
    };
    if header.template_id != SBE_TEMPLATE_TRADE {
        return Vec::new();
    }
    let body = SBE_HEADER_SIZE;
    if raw.len() < body + header.block_length {
        return Vec::new();
    }
    let px_exp = read_i8(raw, body + 9).unwrap_or(0);
    let sz_exp = read_i8(raw, body + 10).unwrap_or(0);
    let Some((entry_len, num_entries, mut off)) = sbe_group(raw, body + header.block_length) else {
        return Vec::new();
    };
    let mut entries = Vec::with_capacity(num_entries);
    for _ in 0..num_entries {
        if raw.len() < off + entry_len || raw.len() < off + 32 {
            break;
        }
        let Some(timestamp_us) = read_i64_le(raw, off) else {
            break;
        };
        let Some(trade_id) = read_u64_le(raw, off + 8) else {
            break;
        };
        let Some(size_m) = read_i64_le(raw, off + 16) else {
            break;
        };
        let Some(price_m) = read_i64_le(raw, off + 24) else {
            break;
        };
        entries.push((timestamp_us, trade_id as i64, size_m, price_m));
        off += entry_len;
    }
    off = match sbe_var_string_skip(raw, off) {
        Some(off) => off,
        None => return Vec::new(),
    };
    let Some((symbol, _)) = sbe_var_string(raw, off) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(entries.len());
    for (timestamp_us, trade_id, size_m, price_m) in entries {
        let price = mantissa_to_f64(price_m, px_exp);
        let amount = mantissa_to_f64(size_m.abs(), sz_exp);
        if price <= 0.0 || amount <= 0.0 {
            continue;
        }
        out.push(Trade {
            symbol: symbol.clone(),
            timestamp_us,
            seq_id: trade_id,
            trade_id,
            side: if size_m >= 0 { 'B' } else { 'S' },
            price,
            amount,
        });
    }
    out
}

pub fn parse_sbe_derivatives(raw: &[u8]) -> Vec<Derivative> {
    let Some(header) = sbe_header(raw) else {
        return Vec::new();
    };
    if header.template_id != SBE_TEMPLATE_TICKER {
        return Vec::new();
    }
    let body = SBE_HEADER_SIZE;
    if raw.len() < body + header.block_length {
        return Vec::new();
    }
    let timestamp_us = read_i64_le(raw, body).unwrap_or(0);
    let Some((entry_len, entry_count, mut off)) = sbe_group(raw, body + header.block_length) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(entry_count * 3);
    for _ in 0..entry_count {
        if raw.len() < off + entry_len || raw.len() < off + 77 {
            break;
        }
        let mark_px_exp = read_i8(raw, off + 41).unwrap_or(0);
        let mark_px_m = read_i64_le(raw, off + 42).unwrap_or(0);
        let idx_px_exp = read_i8(raw, off + 50).unwrap_or(0);
        let idx_px_m = read_i64_le(raw, off + 51).unwrap_or(0);
        let funding_exp = read_i8(raw, off + 68).unwrap_or(0);
        let funding_m = read_i64_le(raw, off + 69).unwrap_or(0);
        off += entry_len;

        let Some((symbol, next)) = sbe_var_string(raw, off) else {
            break;
        };
        off = next;
        for _ in 0..3 {
            let Some(next) = sbe_var_string_skip(raw, off) else {
                return out;
            };
            off = next;
        }

        let mark_price = mantissa_to_f64(mark_px_m, mark_px_exp);
        if mark_price > 0.0 {
            out.push(Derivative::MarkPrice {
                symbol: symbol.clone(),
                price: mark_price,
                timestamp_us,
            });
        }
        let index_price = mantissa_to_f64(idx_px_m, idx_px_exp);
        if index_price > 0.0 {
            out.push(Derivative::IndexPrice {
                symbol: symbol.clone(),
                price: index_price,
                timestamp_us,
            });
        }
        let funding_rate = mantissa_to_f64(funding_m, funding_exp);
        out.push(Derivative::FundingRate {
            symbol,
            funding_rate,
            next_funding_time_us: 0,
            timestamp_us,
        });
    }
    out
}

fn result_items(value: &Value) -> Vec<&Value> {
    match value.get("result") {
        Some(Value::Array(arr)) => arr.iter().collect(),
        Some(obj @ Value::Object(_)) => vec![obj],
        _ => Vec::new(),
    }
}

fn parse_symbol(obj: &serde_json::Map<String, Value>) -> Option<String> {
    obj.get("s")
        .and_then(|v| v.as_str())
        .or_else(|| obj.get("contract").and_then(|v| v.as_str()))
        .or_else(|| obj.get("currency_pair").and_then(|v| v.as_str()))
        .or_else(|| obj.get("symbol").and_then(|v| v.as_str()))
        .map(str::to_string)
}

fn parse_gate_levels(raw: &Value) -> Vec<Level> {
    raw.as_array()
        .map(|arr| arr.iter().filter_map(parse_gate_level).collect())
        .unwrap_or_default()
}

fn parse_gate_level(value: &Value) -> Option<Level> {
    if let Some(arr) = value.as_array() {
        if arr.len() < 2 {
            return None;
        }
        let price = parse_f64_loose(&arr[0])?;
        let amount = parse_f64_loose(&arr[1])?;
        if price > 0.0 {
            return Some(Level { price, amount });
        }
        return None;
    }
    let obj = value.as_object()?;
    let price = obj
        .get("p")
        .and_then(parse_f64_loose)
        .or_else(|| obj.get("price").and_then(parse_f64_loose))?;
    let amount = obj
        .get("s")
        .and_then(parse_f64_loose)
        .or_else(|| obj.get("size").and_then(parse_f64_loose))
        .or_else(|| obj.get("amount").and_then(parse_f64_loose))
        .unwrap_or(0.0);
    if price > 0.0 {
        Some(Level { price, amount })
    } else {
        None
    }
}

pub fn parse_i64_loose(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return i64::try_from(n).ok();
    }
    if let Some(n) = v.as_f64() {
        return Some(n as i64);
    }
    if let Some(s) = v.as_str() {
        return s
            .parse::<i64>()
            .ok()
            .or_else(|| s.parse::<f64>().ok().map(|f| f as i64));
    }
    None
}

pub fn parse_f64_loose(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    if let Some(s) = v.as_str() {
        return s.parse::<f64>().ok();
    }
    None
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

#[derive(Debug, Clone, Copy)]
struct SbeHeader {
    block_length: usize,
    template_id: u16,
}

fn sbe_header(raw: &[u8]) -> Option<SbeHeader> {
    if raw.len() < SBE_HEADER_SIZE {
        return None;
    }
    let block_length = read_u16_le(raw, 0)? as usize;
    let template_id = read_u16_le(raw, 2)?;
    let schema_id = read_u16_le(raw, 4)?;
    if schema_id != SBE_SCHEMA_ID {
        return None;
    }
    Some(SbeHeader {
        block_length,
        template_id,
    })
}

fn read_u16_le(buf: &[u8], off: usize) -> Option<u16> {
    Some(u16::from_le_bytes([*buf.get(off)?, *buf.get(off + 1)?]))
}

fn read_i8(buf: &[u8], off: usize) -> Option<i8> {
    Some(*buf.get(off)? as i8)
}

fn read_i64_le(buf: &[u8], off: usize) -> Option<i64> {
    Some(i64::from_le_bytes(buf.get(off..off + 8)?.try_into().ok()?))
}

fn read_u64_le(buf: &[u8], off: usize) -> Option<u64> {
    Some(u64::from_le_bytes(buf.get(off..off + 8)?.try_into().ok()?))
}

fn mantissa_to_f64(mantissa: i64, exponent: i8) -> f64 {
    (mantissa as f64) * 10_f64.powi(exponent as i32)
}

fn sbe_group(buf: &[u8], off: usize) -> Option<(usize, usize, usize)> {
    if buf.len() < off + 4 {
        return None;
    }
    let entry_len = read_u16_le(buf, off)? as usize;
    let entry_count = read_u16_le(buf, off + 2)? as usize;
    Some((entry_len, entry_count, off + 4))
}

fn sbe_var_string(buf: &[u8], off: usize) -> Option<(String, usize)> {
    let len = *buf.get(off)? as usize;
    let bytes = buf.get(off + 1..off + 1 + len)?;
    let value = std::str::from_utf8(bytes).ok()?.to_string();
    Some((value, off + 1 + len))
}

fn sbe_var_string_skip(buf: &[u8], off: usize) -> Option<usize> {
    let len = *buf.get(off)? as usize;
    buf.get(off + 1..off + 1 + len)?;
    Some(off + 1 + len)
}

#[cfg(test)]
mod tests {
    use super::parse_spot_book_ticker_bbo_raw;

    #[test]
    fn parses_spot_book_ticker_without_value_tree() {
        let raw = br#"{
            "time":1700000000,
            "time_ms":1700000000123,
            "channel":"spot.book_ticker",
            "event":"update",
            "result":{
                "t":1700000000124,
                "u":"111",
                "s":"ETH_USDT",
                "b":"3000",
                "B":"0.5",
                "a":"3001",
                "A":"1.0"
            }
        }"#;

        let bbo = parse_spot_book_ticker_bbo_raw(raw).expect("spot book ticker");

        assert_eq!(bbo.symbol, "ETHUSDT");
        assert_eq!(bbo.seq_id, 111);
        assert_eq!(bbo.timestamp_us, 1_700_000_000_124_000);
        assert!((bbo.bid_price - 3000.0).abs() < 1e-12);
        assert!((bbo.bid_amount - 0.5).abs() < 1e-12);
        assert!((bbo.ask_price - 3001.0).abs() < 1e-12);
        assert!((bbo.ask_amount - 1.0).abs() < 1e-12);
    }

    #[test]
    fn raw_spot_book_ticker_uses_envelope_timestamp_fallback() {
        let raw = br#"{
            "time_ms":1700000000123,
            "channel":"spot.book_ticker",
            "event":"update",
            "result":{
                "u":111,
                "s":"ETH_USDT",
                "b":"3000",
                "B":"0.5",
                "a":"3001",
                "A":"1.0"
            }
        }"#;

        let bbo = parse_spot_book_ticker_bbo_raw(raw).expect("spot book ticker");

        assert_eq!(bbo.timestamp_us, 1_700_000_000_123_000);
    }

    #[test]
    fn raw_spot_book_ticker_rejects_non_bbo_frames() {
        let trade = br#"{
            "channel":"spot.trades",
            "event":"update",
            "result":{"id":1,"currency_pair":"ETH_USDT","price":"3000","amount":"0.1"}
        }"#;
        let futures = br#"{
            "channel":"futures.book_ticker",
            "event":"update",
            "result":{"u":111,"s":"ETH_USDT","b":"3000","a":"3001"}
        }"#;

        assert!(parse_spot_book_ticker_bbo_raw(trade).is_none());
        assert!(parse_spot_book_ticker_bbo_raw(futures).is_none());
    }
}
