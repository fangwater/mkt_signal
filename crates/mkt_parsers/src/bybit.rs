use serde_json::Value;

pub const SBE_TEMPLATE_BBO: u16 = 20000;

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
    pub reset_seq: bool,
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BboUpdate {
    pub symbol: String,
    pub timestamp_us: i64,
    pub seq_id: i64,
    pub reset_seq: bool,
    pub bid: Option<Level>,
    pub ask: Option<Level>,
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

pub fn parse_bbo_update_json(value: &Value) -> Option<BboUpdate> {
    let topic = value.get("topic").and_then(|v| v.as_str())?;
    if !topic.starts_with("orderbook.1.") {
        return None;
    }
    let push_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let data = value.get("data")?.as_object()?;
    let symbol = data
        .get("s")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
        .or_else(|| topic.rsplit('.').next().map(|s| s.to_ascii_uppercase()))?;
    let seq_id = data
        .get("u")
        .and_then(parse_i64_loose)
        .or_else(|| data.get("t").and_then(parse_i64_loose))
        .unwrap_or(0);
    let timestamp_us = data
        .get("t")
        .and_then(parse_i64_loose)
        .or_else(|| value.get("cts").and_then(parse_i64_loose))
        .or_else(|| value.get("ts").and_then(parse_i64_loose))
        .map(normalize_ts_to_us)
        .unwrap_or(0);
    let bid = data
        .get("b")
        .and_then(|v| v.as_array())
        .and_then(|levels| levels.first())
        .and_then(parse_level);
    let ask = data
        .get("a")
        .and_then(|v| v.as_array())
        .and_then(|levels| levels.first())
        .and_then(parse_level);

    Some(BboUpdate {
        symbol,
        timestamp_us,
        seq_id,
        reset_seq: push_type == "snapshot",
        bid,
        ask,
    })
}

pub fn parse_bbo_json(value: &Value) -> Option<Bbo> {
    let update = parse_bbo_update_json(value)?;
    let bid = update.bid?;
    let ask = update.ask?;
    if bid.price <= 0.0 || bid.amount <= 0.0 || ask.price <= 0.0 || ask.amount <= 0.0 {
        return None;
    }
    Some(Bbo {
        symbol: update.symbol,
        timestamp_us: update.timestamp_us,
        seq_id: update.seq_id,
        reset_seq: update.reset_seq,
        bid_price: bid.price,
        bid_amount: bid.amount,
        ask_price: ask.price,
        ask_amount: ask.amount,
    })
}

pub fn parse_trades_json(value: &Value) -> Vec<Trade> {
    let topic = match value.get("topic").and_then(|v| v.as_str()) {
        Some(topic) if topic.starts_with("publicTrade.") => topic,
        _ => return Vec::new(),
    };
    let Some(data) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };

    let mut out = Vec::with_capacity(data.len());
    for (idx, item) in data.iter().enumerate() {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let symbol = obj
            .get("s")
            .and_then(|v| v.as_str())
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_else(|| topic.rsplit('.').next().unwrap_or("").to_ascii_uppercase());
        if symbol.is_empty() {
            continue;
        }
        let side = match obj.get("S").and_then(|v| v.as_str()).unwrap_or("") {
            "Buy" => 'B',
            "Sell" => 'S',
            _ => continue,
        };
        let Some(price) = obj.get("p").and_then(parse_f64_loose) else {
            continue;
        };
        let Some(amount) = obj.get("v").and_then(parse_f64_loose) else {
            continue;
        };
        if price <= 0.0 || amount <= 0.0 {
            continue;
        }
        let Some(raw_ts) = obj.get("T").and_then(parse_i64_loose) else {
            continue;
        };
        let Some(id_raw) = obj.get("i").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(trade_id) = parse_trade_id(id_raw) else {
            continue;
        };
        let seq_base = obj.get("seq").and_then(parse_i64_loose).unwrap_or(trade_id);
        let seq_id = seq_base
            .saturating_mul(1_000_000)
            .saturating_add(idx as i64);
        out.push(Trade {
            symbol,
            timestamp_us: normalize_ts_to_us(raw_ts),
            seq_id,
            trade_id,
            side,
            price,
            amount,
        });
    }
    out
}

pub fn parse_incremental_json(value: &Value) -> Option<Book> {
    let topic = match value.get("topic").and_then(|v| v.as_str()) {
        Some(topic) if topic.starts_with("orderbook.") && !topic.starts_with("orderbook.1.") => {
            topic
        }
        _ => return None,
    };
    let push_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let is_snapshot = match push_type {
        "snapshot" => true,
        "delta" => false,
        _ => return None,
    };
    let data = value.get("data")?.as_object()?;
    let symbol = data
        .get("s")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
        .unwrap_or_else(|| topic.rsplit('.').next().unwrap_or("").to_ascii_uppercase());
    if symbol.is_empty() {
        return None;
    }
    let seq_id = data.get("u").and_then(parse_i64_loose)?;
    let timestamp_us = value
        .get("cts")
        .and_then(parse_i64_loose)
        .or_else(|| value.get("ts").and_then(parse_i64_loose))
        .map(normalize_ts_to_us)
        .unwrap_or(0);
    let bids = data
        .get("b")
        .and_then(|v| v.as_array())
        .map(|levels| parse_level_array(levels))
        .unwrap_or_default();
    let asks = data
        .get("a")
        .and_then(|v| v.as_array())
        .map(|levels| parse_level_array(levels))
        .unwrap_or_default();
    if bids.is_empty() && asks.is_empty() {
        return None;
    }
    Some(Book {
        symbol,
        timestamp_us,
        seq_id,
        prev_seq_id: i64::MIN,
        first_update_id: seq_id,
        final_update_id: seq_id,
        gap_check: false,
        is_snapshot,
        bids,
        asks,
    })
}

pub fn parse_derivatives_json(value: &Value) -> Vec<Derivative> {
    let topic = match value.get("topic").and_then(|v| v.as_str()) {
        Some(topic) => topic,
        None => return Vec::new(),
    };
    if topic.starts_with("tickers.") {
        return parse_ticker_derivatives(value);
    }
    if topic.starts_with("allLiquidation.") {
        return parse_liquidation_derivatives(value);
    }
    Vec::new()
}

pub fn parse_sbe_bbo(raw: &[u8]) -> Option<Bbo> {
    let header = read_sbe_header(raw)?;
    if header.template_id != SBE_TEMPLATE_BBO {
        return None;
    }
    let base = header.body_offset;
    if raw.len() < base + header.block_length {
        return None;
    }

    let parsed = if header.block_length == 82 {
        Some((
            read_i64_le(raw, base),
            read_i64_le(raw, base + 32),
            read_i64_le(raw, base + 40),
            read_i64_le(raw, base + 56),
            read_i64_le(raw, base + 64),
            read_i8(raw, base + 80),
            read_i8(raw, base + 81),
            base + header.block_length,
        ))
    } else if header.block_length >= 98 {
        Some((
            read_i64_le(raw, base),
            read_i64_le(raw, base + 32),
            read_i64_le(raw, base + 40),
            read_i64_le(raw, base + 64),
            read_i64_le(raw, base + 72),
            read_i8(raw, base + 96),
            read_i8(raw, base + 97),
            base + header.block_length,
        ))
    } else {
        None
    };

    let (
        Some(raw_timestamp),
        Some(ask_price_raw),
        Some(ask_amount_raw),
        Some(bid_price_raw),
        Some(bid_amount_raw),
        Some(price_exponent),
        Some(size_exponent),
        symbol_offset,
    ) = parsed?
    else {
        return None;
    };

    let (symbol, _) = read_var_string8(raw, symbol_offset)?;
    let ask_price = scale_mantissa(ask_price_raw, price_exponent);
    let ask_amount = scale_mantissa(ask_amount_raw, size_exponent);
    let bid_price = scale_mantissa(bid_price_raw, price_exponent);
    let bid_amount = scale_mantissa(bid_amount_raw, size_exponent);
    if bid_price <= 0.0 || bid_amount <= 0.0 || ask_price <= 0.0 || ask_amount <= 0.0 {
        return None;
    }

    Some(Bbo {
        symbol: symbol.to_ascii_uppercase(),
        timestamp_us: normalize_bybit_sbe_timestamp_us(raw_timestamp),
        seq_id: 0,
        reset_seq: false,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    })
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

fn parse_ticker_derivatives(value: &Value) -> Vec<Derivative> {
    let Some(data) = value.get("data").and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    let Some(symbol) = data
        .get("symbol")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_uppercase())
    else {
        return Vec::new();
    };
    let timestamp_us = value
        .get("ts")
        .and_then(parse_i64_loose)
        .map(normalize_ts_to_us)
        .unwrap_or(0);
    let mut out = Vec::with_capacity(3);
    if let Some(price) = data.get("markPrice").and_then(parse_f64_loose) {
        if price > 0.0 {
            out.push(Derivative::MarkPrice {
                symbol: symbol.clone(),
                price,
                timestamp_us,
            });
        }
    }
    if let Some(price) = data.get("indexPrice").and_then(parse_f64_loose) {
        if price > 0.0 {
            out.push(Derivative::IndexPrice {
                symbol: symbol.clone(),
                price,
                timestamp_us,
            });
        }
    }
    if let (Some(funding_rate), Some(next_funding_time_us)) = (
        data.get("fundingRate").and_then(parse_f64_loose),
        data.get("nextFundingTime")
            .and_then(parse_i64_loose)
            .map(normalize_ts_to_us),
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

fn parse_liquidation_derivatives(value: &Value) -> Vec<Derivative> {
    let Some(data) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(data.len());
    for item in data {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(symbol) = obj
            .get("s")
            .and_then(|v| v.as_str())
            .map(|s| s.to_ascii_uppercase())
        else {
            continue;
        };
        let side = match obj.get("S").and_then(|v| v.as_str()).unwrap_or("") {
            "Buy" => 'B',
            "Sell" => 'S',
            _ => continue,
        };
        let Some(amount) = obj.get("v").and_then(parse_f64_loose) else {
            continue;
        };
        let Some(price) = obj.get("p").and_then(parse_f64_loose) else {
            continue;
        };
        if amount <= 0.0 || price <= 0.0 {
            continue;
        }
        let Some(timestamp_us) = obj
            .get("T")
            .and_then(parse_i64_loose)
            .map(normalize_ts_to_us)
        else {
            continue;
        };
        out.push(Derivative::Liquidation {
            symbol,
            side,
            amount,
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

fn parse_f64_loose(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    v.as_str().and_then(|s| s.parse::<f64>().ok())
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

fn parse_trade_id(id: &str) -> Option<i64> {
    if is_uuid_fast(id) {
        uuid_to_int64_mixed(id).ok()
    } else if id.chars().all(|c| c.is_ascii_digit()) {
        id.parse::<i64>().ok()
    } else {
        None
    }
}

fn is_uuid_fast(s: &str) -> bool {
    s.len() == 36
        && s.as_bytes().get(8) == Some(&b'-')
        && s.as_bytes().get(13) == Some(&b'-')
        && s.as_bytes().get(18) == Some(&b'-')
        && s.as_bytes().get(23) == Some(&b'-')
}

fn uuid_to_int64_mixed(uuid: &str) -> Result<i64, std::num::ParseIntError> {
    let high = i64::from_str_radix(&uuid[0..8], 16)?;
    let low = i64::from_str_radix(&uuid[24..32], 16)?;
    Ok(high ^ low)
}

#[derive(Clone, Copy)]
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
    msg.get(offset).map(|value| *value as i8)
}

fn read_var_string8(msg: &[u8], offset: usize) -> Option<(String, usize)> {
    let len = msg.get(offset).copied()? as usize;
    let start = offset + 1;
    if msg.len() < start + len {
        return None;
    }
    let value = std::str::from_utf8(&msg[start..start + len]).ok()?;
    Some((value.to_string(), start + len))
}

fn scale_mantissa(mantissa: i64, exponent: i8) -> f64 {
    let factor = 10_f64.powi(exponent as i32);
    (mantissa as f64) * factor
}

fn normalize_bybit_sbe_timestamp_us(timestamp: i64) -> i64 {
    normalize_ts_to_us(timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(raw: &str) -> Value {
        serde_json::from_str(raw).expect("valid json")
    }

    #[test]
    fn parses_bbo_update_json_as_us() {
        let raw = r#"{
            "topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000000,
            "data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","2"]],"u":1}
        }"#;
        let bbo = parse_bbo_json(&v(raw)).expect("bbo");
        assert_eq!(bbo.symbol, "BTCUSDT");
        assert_eq!(bbo.timestamp_us, 1_700_000_000_000_000);
        assert_eq!(bbo.seq_id, 1);
        assert!(bbo.reset_seq);
        assert!((bbo.bid_price - 100.0).abs() < 1e-9);
        assert!((bbo.ask_amount - 2.0).abs() < 1e-9);
    }

    #[test]
    fn parses_trade_json_with_uuid_id_as_us() {
        let raw = r#"{
            "topic":"publicTrade.BTCUSDT",
            "data":[{"T":1700000000123,"s":"BTCUSDT","S":"Sell","v":"0.2","p":"100.6","i":"11111111-2222-3333-4444-555555556666","seq":77}]
        }"#;
        let trades = parse_trades_json(&v(raw));
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].symbol, "BTCUSDT");
        assert_eq!(trades[0].timestamp_us, 1_700_000_000_123_000);
        assert_eq!(trades[0].seq_id, 77_000_000);
        assert_eq!(trades[0].side, 'S');
    }

    #[test]
    fn parses_incremental_json_as_us() {
        let raw = r#"{
            "topic":"orderbook.1000.BTCUSDT","type":"snapshot","ts":1700000000999,"cts":1700000000123,
            "data":{"s":"BTCUSDT","b":[["100","1"],["99","2"]],"a":[["101","3"]],"u":12345}
        }"#;
        let book = parse_incremental_json(&v(raw)).expect("book");
        assert_eq!(book.symbol, "BTCUSDT");
        assert_eq!(book.timestamp_us, 1_700_000_000_123_000);
        assert_eq!(book.seq_id, 12345);
        assert!(book.is_snapshot);
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 1);
    }

    #[test]
    fn parses_derivatives_json_as_us() {
        let ticker = r#"{
            "topic":"tickers.BTCUSDT","type":"snapshot","ts":1700000000123,
            "data":{"symbol":"BTCUSDT","markPrice":"100.1","indexPrice":"99.9","fundingRate":"0.0001","nextFundingTime":"1700003600000"}
        }"#;
        let out = parse_derivatives_json(&v(ticker));
        assert_eq!(out.len(), 3);
        assert!(matches!(
            &out[0],
            Derivative::MarkPrice {
                symbol,
                timestamp_us: 1_700_000_000_123_000,
                ..
            } if symbol == "BTCUSDT"
        ));
        assert!(matches!(
            &out[2],
            Derivative::FundingRate {
                next_funding_time_us: 1_700_003_600_000_000,
                ..
            }
        ));
    }

    #[test]
    fn parses_sbe_bbo_binary_as_us() {
        let raw = vec![
            98, 0, 32, 78, 1, 0, 0, 0, 198, 28, 26, 229, 152, 1, 0, 0, 111, 0, 0, 0, 0, 0, 0, 0,
            154, 28, 26, 229, 152, 1, 0, 0, 112, 0, 0, 0, 0, 0, 0, 0, 169, 63, 55, 67, 0, 0, 0,
            0, 16, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            168, 63, 55, 67, 0, 0, 0, 0, 32, 78, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 254, 255, 7, 66, 84, 67, 85, 83, 68, 84,
        ];
        let bbo = parse_sbe_bbo(&raw).expect("sbe bbo");
        assert_eq!(bbo.symbol, "BTCUSDT");
        assert_eq!(bbo.timestamp_us, 1_756_190_350_534_000);
        assert!((bbo.bid_price - 11_276_942.48).abs() < 1e-9);
        assert!((bbo.ask_amount - 1_000.0).abs() < 1e-9);
    }
}
