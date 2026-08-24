//! Compact, minute-end Normalized LL2 records.
//!
//! The raw LSEG source publishes full L1--L10 snapshots many times per
//! minute. This module stores only the final source snapshot of a RIC/minute,
//! with the number of source updates folded into `update_count`.

use anyhow::{anyhow, bail, Result};

use crate::{decode_ric, encode_ric, MISSING_PRICE, PRICE_SCALE, RIC_LEN};

pub const LL2_DEPTH_LEVELS: usize = 10;
pub const LL2_MINUTE_KEY_LEN: usize = 1 + 4 + 4 + RIC_LEN + 8;
pub const LL2_MINUTE_STAGE_KEY_LEN: usize = LL2_MINUTE_KEY_LEN + 2;
pub const LL2_MINUTE_VALUE_LEN: usize = 424;
pub const LL2_MINUTE_MAGIC: [u8; 2] = *b"L2";
pub const LL2_MINUTE_VERSION: u8 = 1;
pub const LL2_MINUTE_KIND: u8 = 1;

pub const CF_LL2_MINUTE: &str = "ll2_minute";
pub const CF_LL2_MINUTE_STAGE: &str = "ll2_minute_stage";
pub const CF_LL2_MINUTE_META: &str = "ll2_minute_meta";
pub const LL2_PERIOD_META_PREFIX: &str = "period:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ll2MinuteKey {
    pub exchange: String,
    pub product_root: String,
    pub trading_day: u32,
    pub ric: String,
    pub minute_utc_sec: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ll2Minute {
    pub source_ts_utc_ns: u64,
    pub source_seq: u64,
    pub update_count: u32,
    pub bid_prices: [i64; LL2_DEPTH_LEVELS],
    pub bid_sizes: [i64; LL2_DEPTH_LEVELS],
    pub bid_counts: [u32; LL2_DEPTH_LEVELS],
    pub ask_prices: [i64; LL2_DEPTH_LEVELS],
    pub ask_sizes: [i64; LL2_DEPTH_LEVELS],
    pub ask_counts: [u32; LL2_DEPTH_LEVELS],
}

impl Ll2Minute {
    pub fn empty(source_ts_utc_ns: u64, source_seq: u64) -> Self {
        Self {
            source_ts_utc_ns,
            source_seq,
            update_count: 1,
            bid_prices: [MISSING_PRICE; LL2_DEPTH_LEVELS],
            bid_sizes: [MISSING_PRICE; LL2_DEPTH_LEVELS],
            bid_counts: [0; LL2_DEPTH_LEVELS],
            ask_prices: [MISSING_PRICE; LL2_DEPTH_LEVELS],
            ask_sizes: [MISSING_PRICE; LL2_DEPTH_LEVELS],
            ask_counts: [0; LL2_DEPTH_LEVELS],
        }
    }

    pub fn ordering_tuple(&self, part: u16) -> (u64, u16, u64) {
        (self.source_ts_utc_ns, part, self.source_seq)
    }

    pub fn merge_from(&mut self, other: &Self, self_part: u16, other_part: u16) -> Result<()> {
        self.update_count = self
            .update_count
            .checked_add(other.update_count)
            .ok_or_else(|| anyhow!("LL2 minute update_count overflow"))?;
        if other.ordering_tuple(other_part) > self.ordering_tuple(self_part) {
            let count = self.update_count;
            *self = other.clone();
            self.update_count = count;
        }
        Ok(())
    }
}

fn exchange_code(exchange: &str) -> Result<u8> {
    match exchange {
        "CBOT" => Ok(1),
        "CME" => Ok(2),
        "COMEX" => Ok(3),
        "NYMEX" => Ok(4),
        _ => bail!("unknown LL2 exchange {exchange:?}"),
    }
}

fn decode_exchange(code: u8) -> Result<&'static str> {
    match code {
        1 => Ok("CBOT"),
        2 => Ok("CME"),
        3 => Ok("COMEX"),
        4 => Ok("NYMEX"),
        _ => bail!("unknown LL2 exchange code {code}"),
    }
}

fn encode_root(root: &str) -> Result<[u8; 4]> {
    if root.is_empty()
        || root.len() > 4
        || !root
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        bail!("invalid LL2 product root {root:?}");
    }
    let mut out = [0u8; 4];
    out[..root.len()].copy_from_slice(root.as_bytes());
    Ok(out)
}

fn decode_root(bytes: &[u8]) -> Result<String> {
    if bytes.len() != 4 {
        bail!("LL2 product root slot must be 4 bytes, got {}", bytes.len());
    }
    let end = bytes.iter().position(|byte| *byte == 0).unwrap_or(4);
    if end == 0 || bytes[end..].iter().any(|byte| *byte != 0) {
        bail!("invalid LL2 product root slot");
    }
    let root =
        std::str::from_utf8(&bytes[..end]).map_err(|_| anyhow!("LL2 product root is not UTF-8"))?;
    encode_root(root)?;
    Ok(root.to_string())
}

pub fn encode_ll2_minute_key(key: &Ll2MinuteKey) -> Result<[u8; LL2_MINUTE_KEY_LEN]> {
    if key.minute_utc_sec == 0 {
        bail!("LL2 minute timestamp must be nonzero");
    }
    let mut out = [0u8; LL2_MINUTE_KEY_LEN];
    out[0] = exchange_code(&key.exchange)?;
    out[1..5].copy_from_slice(&encode_root(&key.product_root)?);
    out[5..9].copy_from_slice(&key.trading_day.to_be_bytes());
    out[9..9 + RIC_LEN].copy_from_slice(&encode_ric(&key.ric)?);
    out[9 + RIC_LEN..].copy_from_slice(&key.minute_utc_sec.to_be_bytes());
    Ok(out)
}

pub fn decode_ll2_minute_key(bytes: &[u8]) -> Result<Ll2MinuteKey> {
    if bytes.len() != LL2_MINUTE_KEY_LEN {
        bail!(
            "LL2 minute key must be {LL2_MINUTE_KEY_LEN} bytes, got {}",
            bytes.len()
        );
    }
    let exchange = decode_exchange(bytes[0])?.to_string();
    let product_root = decode_root(&bytes[1..5])?;
    let trading_day = u32::from_be_bytes(bytes[5..9].try_into().expect("fixed key slice"));
    let ric = decode_ric(&bytes[9..9 + RIC_LEN])?;
    let minute_utc_sec =
        u64::from_be_bytes(bytes[9 + RIC_LEN..].try_into().expect("fixed key slice"));
    if minute_utc_sec == 0 {
        bail!("LL2 minute key has zero timestamp");
    }
    Ok(Ll2MinuteKey {
        exchange,
        product_root,
        trading_day,
        ric,
        minute_utc_sec,
    })
}

pub fn encode_ll2_minute_stage_key(
    key: &Ll2MinuteKey,
    part: u16,
) -> Result<[u8; LL2_MINUTE_STAGE_KEY_LEN]> {
    let mut out = [0u8; LL2_MINUTE_STAGE_KEY_LEN];
    out[..LL2_MINUTE_KEY_LEN].copy_from_slice(&encode_ll2_minute_key(key)?);
    out[LL2_MINUTE_KEY_LEN..].copy_from_slice(&part.to_be_bytes());
    Ok(out)
}

pub fn decode_ll2_minute_stage_key(bytes: &[u8]) -> Result<(Ll2MinuteKey, u16)> {
    if bytes.len() != LL2_MINUTE_STAGE_KEY_LEN {
        bail!(
            "LL2 minute stage key must be {LL2_MINUTE_STAGE_KEY_LEN} bytes, got {}",
            bytes.len()
        );
    }
    let key = decode_ll2_minute_key(&bytes[..LL2_MINUTE_KEY_LEN])?;
    let part = u16::from_be_bytes(
        bytes[LL2_MINUTE_KEY_LEN..]
            .try_into()
            .expect("fixed key slice"),
    );
    Ok((key, part))
}

pub fn encode_ll2_minute(value: &Ll2Minute) -> [u8; LL2_MINUTE_VALUE_LEN] {
    let mut out = [0u8; LL2_MINUTE_VALUE_LEN];
    out[0..2].copy_from_slice(&LL2_MINUTE_MAGIC);
    out[2] = LL2_MINUTE_VERSION;
    out[3] = LL2_MINUTE_KIND;
    out[4..12].copy_from_slice(&value.source_ts_utc_ns.to_le_bytes());
    out[12..20].copy_from_slice(&value.source_seq.to_le_bytes());
    out[20..24].copy_from_slice(&value.update_count.to_le_bytes());
    let mut offset = 24;
    for level in 0..LL2_DEPTH_LEVELS {
        for field in [
            value.bid_prices[level],
            value.bid_sizes[level],
            value.ask_prices[level],
            value.ask_sizes[level],
        ] {
            out[offset..offset + 8].copy_from_slice(&field.to_le_bytes());
            offset += 8;
        }
        for field in [value.bid_counts[level], value.ask_counts[level]] {
            out[offset..offset + 4].copy_from_slice(&field.to_le_bytes());
            offset += 4;
        }
    }
    debug_assert_eq!(offset, LL2_MINUTE_VALUE_LEN);
    out
}

pub fn decode_ll2_minute(bytes: &[u8]) -> Result<Ll2Minute> {
    if bytes.len() != LL2_MINUTE_VALUE_LEN {
        bail!(
            "LL2 minute value must be {LL2_MINUTE_VALUE_LEN} bytes, got {}",
            bytes.len()
        );
    }
    if bytes[0..2] != LL2_MINUTE_MAGIC
        || bytes[2] != LL2_MINUTE_VERSION
        || bytes[3] != LL2_MINUTE_KIND
    {
        bail!("invalid LL2 minute value header");
    }
    let source_ts_utc_ns = u64::from_le_bytes(bytes[4..12].try_into().expect("fixed value slice"));
    let source_seq = u64::from_le_bytes(bytes[12..20].try_into().expect("fixed value slice"));
    let update_count = u32::from_le_bytes(bytes[20..24].try_into().expect("fixed value slice"));
    if source_ts_utc_ns == 0 || update_count == 0 {
        bail!("LL2 minute value has zero source timestamp or update count");
    }
    let mut value = Ll2Minute::empty(source_ts_utc_ns, source_seq);
    value.update_count = update_count;
    let mut offset = 24;
    for level in 0..LL2_DEPTH_LEVELS {
        value.bid_prices[level] = i64::from_le_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("fixed value slice"),
        );
        offset += 8;
        value.bid_sizes[level] = i64::from_le_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("fixed value slice"),
        );
        offset += 8;
        value.ask_prices[level] = i64::from_le_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("fixed value slice"),
        );
        offset += 8;
        value.ask_sizes[level] = i64::from_le_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("fixed value slice"),
        );
        offset += 8;
        value.bid_counts[level] = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .expect("fixed value slice"),
        );
        offset += 4;
        value.ask_counts[level] = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .expect("fixed value slice"),
        );
        offset += 4;
    }
    debug_assert_eq!(offset, LL2_MINUTE_VALUE_LEN);
    Ok(value)
}

pub fn e9_to_f64(value: i64) -> Option<f64> {
    (value != MISSING_PRICE).then_some(value as f64 / PRICE_SCALE as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Ll2Minute {
        let mut value = Ll2Minute::empty(1_704_754_859_123_456_789, 7);
        value.update_count = 3;
        value.bid_prices[0] = 4_798_000_000_000;
        value.bid_sizes[0] = 5_000_000_000;
        value.bid_counts[0] = 2;
        value.ask_prices[0] = 4_798_250_000_000;
        value.ask_sizes[0] = 4_000_000_000;
        value.ask_counts[0] = 3;
        value
    }

    #[test]
    fn minute_key_and_value_round_trip() {
        let key = Ll2MinuteKey {
            exchange: "CME".to_string(),
            product_root: "ES".to_string(),
            trading_day: 20240109,
            ric: "ESH24".to_string(),
            minute_utc_sec: 1_704_754_800,
        };
        assert_eq!(
            decode_ll2_minute_key(&encode_ll2_minute_key(&key).unwrap()).unwrap(),
            key
        );
        let value = sample();
        assert_eq!(
            decode_ll2_minute(&encode_ll2_minute(&value)).unwrap(),
            value
        );
    }

    #[test]
    fn merge_uses_latest_source_and_sums_updates() {
        let mut old = sample();
        let mut new = sample();
        new.source_ts_utc_ns += 1;
        new.source_seq = 8;
        new.update_count = 2;
        new.bid_prices[0] += 250_000_000;
        old.merge_from(&new, 0, 1).unwrap();
        assert_eq!(old.update_count, 5);
        assert_eq!(old.source_ts_utc_ns, new.source_ts_utc_ns);
        assert_eq!(old.bid_prices[0], new.bid_prices[0]);
    }
}
