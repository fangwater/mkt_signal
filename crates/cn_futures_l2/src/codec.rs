//! Fixed-length CN L2 records. Magic is `CN`, not the CME `CT`.

use anyhow::{bail, Result};

pub const MAGIC: [u8; 2] = *b"CN";
pub const VERSION: u8 = 1;
pub const KIND_TRADE: u8 = 1;
pub const KIND_DEPTH: u8 = 2;
pub const KIND_OI: u8 = 3;
pub const KIND_QUEUE: u8 = 4;
pub const INSTRUMENT_LEN: usize = 16;
pub const KEY_LEN: usize = 1 + INSTRUMENT_LEN + 8 + 4;
pub const TRADE_LEN: usize = 88;
pub const DEPTH_LEN: usize = 192;
pub const OI_LEN: usize = 56;
pub const QUEUE_LEN: usize = 208;
pub const QUEUE_LEVELS: usize = 10;
pub const PRICE_MISSING: i64 = i64::MIN;
pub const CF_REPLAY_META: &str = "replay_meta";
pub const PRODUCT_CF_PREFIX: &str = "p:";
pub const STATUS_WRITING: &[u8] = b"writing";
pub const STATUS_DONE: &[u8] = b"done";
pub const PRICE_SCALE: f64 = 1_000_000_000.0;

pub fn encode_instrument(instrument: &str) -> Result<[u8; INSTRUMENT_LEN]> {
    if !instrument.is_ascii() {
        bail!("instrument {instrument:?} is not ASCII");
    }
    let bytes = instrument.as_bytes();
    if bytes.len() > INSTRUMENT_LEN {
        bail!("instrument {instrument:?} exceeds {INSTRUMENT_LEN} bytes");
    }
    let mut out = [0u8; INSTRUMENT_LEN];
    out[..bytes.len()].copy_from_slice(bytes);
    Ok(out)
}

pub fn decode_instrument(bytes: &[u8]) -> Result<String> {
    if bytes.len() != INSTRUMENT_LEN {
        bail!(
            "instrument slot is {} bytes, want {INSTRUMENT_LEN}",
            bytes.len()
        );
    }
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(INSTRUMENT_LEN);
    if bytes[..end].iter().any(|b| !b.is_ascii() || *b == 0) {
        bail!("instrument slot is not ASCII");
    }
    Ok(String::from_utf8(bytes[..end].to_vec())?)
}

pub fn encode_price(value: Option<f64>) -> Result<i64> {
    let Some(price) = value else {
        return Ok(PRICE_MISSING);
    };
    if !price.is_finite() {
        return Ok(PRICE_MISSING);
    }
    let scaled = (price * PRICE_SCALE).round();
    if !scaled.is_finite() || scaled > i64::MAX as f64 || scaled < i64::MIN as f64 {
        bail!("price {price} overflows i64 e9");
    }
    Ok(scaled as i64)
}

pub fn decode_price(value: i64) -> Option<f64> {
    if value == PRICE_MISSING {
        None
    } else {
        Some(value as f64 / PRICE_SCALE)
    }
}

pub fn encode_f64(value: Option<f64>) -> [u8; 8] {
    match value {
        Some(v) if v.is_finite() => v.to_le_bytes(),
        _ => f64::NAN.to_le_bytes(),
    }
}

pub fn decode_f64(bytes: [u8; 8]) -> Option<f64> {
    let value = f64::from_le_bytes(bytes);
    value.is_finite().then_some(value)
}

pub fn encode_key(kind: u8, instrument: &str, ts_utc_ns: u64, seq: u32) -> Result<[u8; KEY_LEN]> {
    let mut key = [0u8; KEY_LEN];
    key[0] = kind;
    key[1..1 + INSTRUMENT_LEN].copy_from_slice(&encode_instrument(instrument)?);
    key[1 + INSTRUMENT_LEN..1 + INSTRUMENT_LEN + 8].copy_from_slice(&ts_utc_ns.to_be_bytes());
    key[1 + INSTRUMENT_LEN + 8..].copy_from_slice(&seq.to_be_bytes());
    Ok(key)
}

pub fn decode_key(key: &[u8]) -> Result<(u8, String, u64, u32)> {
    if key.len() != KEY_LEN {
        bail!("key is {} bytes, want {KEY_LEN}", key.len());
    }
    let kind = key[0];
    let instrument = decode_instrument(&key[1..1 + INSTRUMENT_LEN])?;
    let ts = u64::from_be_bytes(key[1 + INSTRUMENT_LEN..1 + INSTRUMENT_LEN + 8].try_into()?);
    let seq = u32::from_be_bytes(key[1 + INSTRUMENT_LEN + 8..].try_into()?);
    Ok((kind, instrument, ts, seq))
}

#[derive(Clone, Debug, PartialEq)]
pub struct TradeRecord {
    pub instrument: String,
    pub ts_utc_ns: u64,
    pub price: f64,
    pub volume: f64,
    pub turnover: Option<f64>,
    pub bid: Option<f64>,
    pub bid_size: Option<f64>,
    pub ask: Option<f64>,
    pub ask_size: Option<f64>,
    pub aggressor: u8,
}

pub fn encode_trade(record: &TradeRecord) -> Result<[u8; TRADE_LEN]> {
    if record.aggressor > 2 {
        bail!("aggressor {} is not 0/1/2", record.aggressor);
    }
    let mut out = [0u8; TRADE_LEN];
    out[0..2].copy_from_slice(&MAGIC);
    out[2] = VERSION;
    out[3] = KIND_TRADE;
    out[4..20].copy_from_slice(&encode_instrument(&record.instrument)?);
    out[20..28].copy_from_slice(&record.ts_utc_ns.to_le_bytes());
    out[28..36].copy_from_slice(&encode_price(Some(record.price))?.to_le_bytes());
    out[36..44].copy_from_slice(&encode_f64(Some(record.volume)));
    out[44..52].copy_from_slice(&encode_f64(record.turnover));
    out[52..60].copy_from_slice(&encode_price(record.bid)?.to_le_bytes());
    out[60..68].copy_from_slice(&encode_f64(record.bid_size));
    out[68..76].copy_from_slice(&encode_price(record.ask)?.to_le_bytes());
    out[76..84].copy_from_slice(&encode_f64(record.ask_size));
    out[84] = record.aggressor;
    Ok(out)
}

pub fn decode_trade(bytes: &[u8]) -> Result<TradeRecord> {
    if bytes.len() != TRADE_LEN {
        bail!("trade is {} bytes, want {TRADE_LEN}", bytes.len());
    }
    if bytes[0..2] != MAGIC || bytes[2] != VERSION || bytes[3] != KIND_TRADE {
        bail!("trade magic/version/kind mismatch");
    }
    if bytes[85..88] != [0, 0, 0] {
        bail!("trade pad is not zero");
    }
    let instrument = decode_instrument(&bytes[4..20])?;
    let ts_utc_ns = u64::from_le_bytes(bytes[20..28].try_into()?);
    let price = decode_price(i64::from_le_bytes(bytes[28..36].try_into()?))
        .ok_or_else(|| anyhow::anyhow!("trade price missing"))?;
    let volume = decode_f64(bytes[36..44].try_into()?)
        .ok_or_else(|| anyhow::anyhow!("trade volume missing"))?;
    let turnover = decode_f64(bytes[44..52].try_into()?);
    let bid = decode_price(i64::from_le_bytes(bytes[52..60].try_into()?));
    let bid_size = decode_f64(bytes[60..68].try_into()?);
    let ask = decode_price(i64::from_le_bytes(bytes[68..76].try_into()?));
    let ask_size = decode_f64(bytes[76..84].try_into()?);
    let aggressor = bytes[84];
    if aggressor > 2 {
        bail!("trade aggressor {aggressor} is not 0/1/2");
    }
    Ok(TradeRecord {
        instrument,
        ts_utc_ns,
        price,
        volume,
        turnover,
        bid,
        bid_size,
        ask,
        ask_size,
        aggressor,
    })
}

#[derive(Clone, Debug, PartialEq)]
pub struct DepthRecord {
    pub instrument: String,
    pub ts_utc_ns: u64,
    pub bid_prices: [Option<f64>; 5],
    pub bid_sizes: [Option<f64>; 5],
    pub ask_prices: [Option<f64>; 5],
    pub ask_sizes: [Option<f64>; 5],
}

pub fn encode_depth(record: &DepthRecord) -> Result<[u8; DEPTH_LEN]> {
    let mut out = [0u8; DEPTH_LEN];
    out[0..2].copy_from_slice(&MAGIC);
    out[2] = VERSION;
    out[3] = KIND_DEPTH;
    out[4..20].copy_from_slice(&encode_instrument(&record.instrument)?);
    out[20..28].copy_from_slice(&record.ts_utc_ns.to_le_bytes());
    let mut offset = 28;
    for price in record.bid_prices {
        out[offset..offset + 8].copy_from_slice(&encode_price(price)?.to_le_bytes());
        offset += 8;
    }
    for size in record.bid_sizes {
        out[offset..offset + 8].copy_from_slice(&encode_f64(size));
        offset += 8;
    }
    for price in record.ask_prices {
        out[offset..offset + 8].copy_from_slice(&encode_price(price)?.to_le_bytes());
        offset += 8;
    }
    for size in record.ask_sizes {
        out[offset..offset + 8].copy_from_slice(&encode_f64(size));
        offset += 8;
    }
    Ok(out)
}

pub fn decode_depth(bytes: &[u8]) -> Result<DepthRecord> {
    if bytes.len() != DEPTH_LEN {
        bail!("depth is {} bytes, want {DEPTH_LEN}", bytes.len());
    }
    if bytes[0..2] != MAGIC || bytes[2] != VERSION || bytes[3] != KIND_DEPTH {
        bail!("depth magic/version/kind mismatch");
    }
    let instrument = decode_instrument(&bytes[4..20])?;
    let ts_utc_ns = u64::from_le_bytes(bytes[20..28].try_into()?);
    let mut offset = 28;
    let mut bid_prices = [None; 5];
    let mut bid_sizes = [None; 5];
    let mut ask_prices = [None; 5];
    let mut ask_sizes = [None; 5];
    for slot in &mut bid_prices {
        *slot = decode_price(i64::from_le_bytes(bytes[offset..offset + 8].try_into()?));
        offset += 8;
    }
    for slot in &mut bid_sizes {
        *slot = decode_f64(bytes[offset..offset + 8].try_into()?);
        offset += 8;
    }
    for slot in &mut ask_prices {
        *slot = decode_price(i64::from_le_bytes(bytes[offset..offset + 8].try_into()?));
        offset += 8;
    }
    for slot in &mut ask_sizes {
        *slot = decode_f64(bytes[offset..offset + 8].try_into()?);
        offset += 8;
    }
    if bytes[offset..].iter().any(|&b| b != 0) {
        bail!("depth pad is not zero");
    }
    Ok(DepthRecord {
        instrument,
        ts_utc_ns,
        bid_prices,
        bid_sizes,
        ask_prices,
        ask_sizes,
    })
}

#[derive(Clone, Debug, PartialEq)]
pub struct OiRecord {
    pub instrument: String,
    pub ts_utc_ns: u64,
    pub open_int: f64,
    pub prev_open_int: Option<f64>,
    pub delta: Option<f64>,
}

pub fn encode_oi(record: &OiRecord) -> Result<[u8; OI_LEN]> {
    let mut out = [0u8; OI_LEN];
    out[0..2].copy_from_slice(&MAGIC);
    out[2] = VERSION;
    out[3] = KIND_OI;
    out[4..20].copy_from_slice(&encode_instrument(&record.instrument)?);
    out[20..28].copy_from_slice(&record.ts_utc_ns.to_le_bytes());
    out[28..36].copy_from_slice(&encode_f64(Some(record.open_int)));
    out[36..44].copy_from_slice(&encode_f64(record.prev_open_int));
    out[44..52].copy_from_slice(&encode_f64(record.delta));
    Ok(out)
}

pub fn decode_oi(bytes: &[u8]) -> Result<OiRecord> {
    if bytes.len() != OI_LEN {
        bail!("oi is {} bytes, want {OI_LEN}", bytes.len());
    }
    if bytes[0..2] != MAGIC || bytes[2] != VERSION || bytes[3] != KIND_OI {
        bail!("oi magic/version/kind mismatch");
    }
    if bytes[52..56].iter().any(|&b| b != 0) {
        bail!("oi pad is not zero");
    }
    let instrument = decode_instrument(&bytes[4..20])?;
    let ts_utc_ns = u64::from_le_bytes(bytes[20..28].try_into()?);
    let open_int = decode_f64(bytes[28..36].try_into()?)
        .ok_or_else(|| anyhow::anyhow!("oi open_int missing"))?;
    Ok(OiRecord {
        instrument,
        ts_utc_ns,
        open_int,
        prev_open_int: decode_f64(bytes[36..44].try_into()?),
        delta: decode_f64(bytes[44..52].try_into()?),
    })
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueueRecord {
    pub instrument: String,
    pub ts_utc_ns: u64,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub bid_qty: [Option<f64>; QUEUE_LEVELS],
    pub ask_qty: [Option<f64>; QUEUE_LEVELS],
}

pub fn encode_queue(record: &QueueRecord) -> Result<[u8; QUEUE_LEN]> {
    let mut out = [0u8; QUEUE_LEN];
    out[0..2].copy_from_slice(&MAGIC);
    out[2] = VERSION;
    out[3] = KIND_QUEUE;
    out[4..20].copy_from_slice(&encode_instrument(&record.instrument)?);
    out[20..28].copy_from_slice(&record.ts_utc_ns.to_le_bytes());
    out[28..36].copy_from_slice(&encode_price(record.bid_price)?.to_le_bytes());
    let mut offset = 36;
    for qty in record.bid_qty {
        out[offset..offset + 8].copy_from_slice(&encode_f64(qty));
        offset += 8;
    }
    out[offset..offset + 8].copy_from_slice(&encode_price(record.ask_price)?.to_le_bytes());
    offset += 8;
    for qty in record.ask_qty {
        out[offset..offset + 8].copy_from_slice(&encode_f64(qty));
        offset += 8;
    }
    Ok(out)
}

pub fn decode_queue(bytes: &[u8]) -> Result<QueueRecord> {
    if bytes.len() != QUEUE_LEN {
        bail!("queue is {} bytes, want {QUEUE_LEN}", bytes.len());
    }
    if bytes[0..2] != MAGIC || bytes[2] != VERSION || bytes[3] != KIND_QUEUE {
        bail!("queue magic/version/kind mismatch");
    }
    let instrument = decode_instrument(&bytes[4..20])?;
    let ts_utc_ns = u64::from_le_bytes(bytes[20..28].try_into()?);
    let bid_price = decode_price(i64::from_le_bytes(bytes[28..36].try_into()?));
    let mut offset = 36;
    let mut bid_qty = [None; QUEUE_LEVELS];
    for slot in &mut bid_qty {
        *slot = decode_f64(bytes[offset..offset + 8].try_into()?);
        offset += 8;
    }
    let ask_price = decode_price(i64::from_le_bytes(bytes[offset..offset + 8].try_into()?));
    offset += 8;
    let mut ask_qty = [None; QUEUE_LEVELS];
    for slot in &mut ask_qty {
        *slot = decode_f64(bytes[offset..offset + 8].try_into()?);
        offset += 8;
    }
    if bytes[offset..].iter().any(|&b| b != 0) {
        bail!("queue pad is not zero");
    }
    Ok(QueueRecord {
        instrument,
        ts_utc_ns,
        bid_price,
        ask_price,
        bid_qty,
        ask_qty,
    })
}

pub fn product_cf_name(year: i32, product: &str) -> Result<String> {
    if product.is_empty()
        || !product
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        bail!("product {product:?} is not a column-family name");
    }
    Ok(format!("{PRODUCT_CF_PREFIX}{year}:{product}"))
}

pub fn is_product_cf_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(PRODUCT_CF_PREFIX) else {
        return false;
    };
    let Some((year, product)) = rest.split_once(':') else {
        return false;
    };
    year.len() == 4
        && year.bytes().all(|b| b.is_ascii_digit())
        && !product.is_empty()
        && product
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

pub fn day_meta_key(exchange: &str, day: chrono::NaiveDate) -> Vec<u8> {
    format!("day:{exchange}:{}", day.format("%Y%m%d")).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_roundtrip() {
        let record = TradeRecord {
            instrument: "rb2605".into(),
            ts_utc_ns: 1_770_685_201_000_000_000,
            price: 3382.0,
            volume: 10.0,
            turnover: Some(33820.0),
            bid: Some(3379.0),
            bid_size: Some(20.0),
            ask: Some(3381.0),
            ask_size: Some(18.0),
            aggressor: 1,
        };
        let bytes = encode_trade(&record).unwrap();
        assert_eq!(bytes.len(), TRADE_LEN);
        assert_eq!(decode_trade(&bytes).unwrap(), record);
    }

    #[test]
    fn depth_roundtrip_keeps_missing_levels() {
        let mut record = DepthRecord {
            instrument: "rb2605".into(),
            ts_utc_ns: 1_770_685_200_000_000_000,
            bid_prices: [Some(3379.0), Some(3378.0), None, None, None],
            bid_sizes: [Some(20.0), Some(10.0), None, None, None],
            ask_prices: [Some(3381.0), None, None, None, None],
            ask_sizes: [Some(18.0), None, None, None, None],
        };
        record.ask_prices[1] = Some(3382.0);
        record.ask_sizes[1] = Some(9.0);
        let bytes = encode_depth(&record).unwrap();
        assert_eq!(bytes.len(), DEPTH_LEN);
        assert_eq!(decode_depth(&bytes).unwrap(), record);
    }

    #[test]
    fn key_orders_by_kind_instrument_ts_seq() {
        let a = encode_key(KIND_TRADE, "rb2605", 10, 1).unwrap();
        let b = encode_key(KIND_TRADE, "rb2605", 10, 2).unwrap();
        let c = encode_key(KIND_DEPTH, "rb2605", 10, 0).unwrap();
        assert!(a < b);
        assert!(a < c);
        assert!(is_product_cf_name("p:2026:RB"));
        assert!(!is_product_cf_name("cme_trade"));
    }

    #[test]
    fn oi_roundtrip_keeps_first_observation_sentinels() {
        let record = OiRecord {
            instrument: "rb2605".into(),
            ts_utc_ns: 1_770_685_200_500_000_000,
            open_int: 180000.0,
            prev_open_int: None,
            delta: None,
        };
        let bytes = encode_oi(&record).unwrap();
        assert_eq!(bytes.len(), OI_LEN);
        assert_eq!(decode_oi(&bytes).unwrap(), record);
    }

    #[test]
    fn queue_roundtrip_keeps_missing_levels() {
        let mut record = QueueRecord {
            instrument: "i2609".into(),
            ts_utc_ns: 1_770_685_200_000_000_000,
            bid_price: Some(751.0),
            ask_price: Some(754.0),
            bid_qty: [None; QUEUE_LEVELS],
            ask_qty: [None; QUEUE_LEVELS],
        };
        record.bid_qty[0] = Some(1.0);
        record.bid_qty[1] = Some(1.0);
        record.ask_qty[0] = Some(1.0);
        let bytes = encode_queue(&record).unwrap();
        assert_eq!(bytes.len(), QUEUE_LEN);
        assert_eq!(decode_queue(&bytes).unwrap(), record);
    }
}
