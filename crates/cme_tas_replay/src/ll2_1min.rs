//! Fixed binary codec for the final Normalized LL2 snapshot in one UTC minute.

use anyhow::{bail, Result};
use rocksdb::MergeOperands;

use crate::ll2_source::{NormalizedLl2Snapshot, LL2_DEPTH_LEVELS, MISSING_COUNT};
use crate::{decode_ric, encode_ric, MISSING_PRICE, RIC_LEN};

pub const LL2_MINUTE_KEY_LEN: usize = RIC_LEN + 8;
pub const LL2_MINUTE_VALUE_LEN: usize = 432;
pub const LL2_MINUTE_MAGIC: [u8; 2] = *b"L2";
pub const LL2_MINUTE_VERSION: u8 = 1;
pub const LL2_MINUTE_KIND: u8 = 1;
pub const NS_PER_MINUTE: u64 = 60_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ll2MinuteKey {
    pub ric: String,
    pub minute_utc_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ll2Minute {
    pub source_ts_utc_ns: u64,
    pub source_order: u64,
    pub exch_time_ns: u64,
    pub gmt_offset_minutes: i16,
    pub bid_prices: [i64; LL2_DEPTH_LEVELS],
    pub bid_sizes: [i64; LL2_DEPTH_LEVELS],
    pub bid_counts: [u32; LL2_DEPTH_LEVELS],
    pub ask_prices: [i64; LL2_DEPTH_LEVELS],
    pub ask_sizes: [i64; LL2_DEPTH_LEVELS],
    pub ask_counts: [u32; LL2_DEPTH_LEVELS],
}

impl Ll2Minute {
    pub fn from_source(source: NormalizedLl2Snapshot, source_order: u64) -> Self {
        Self {
            source_ts_utc_ns: source.source_ts_utc_ns,
            source_order,
            exch_time_ns: source.exch_time_ns,
            gmt_offset_minutes: source.gmt_offset_minutes,
            bid_prices: source.bid_prices,
            bid_sizes: source.bid_sizes,
            bid_counts: source.bid_counts,
            ask_prices: source.ask_prices,
            ask_sizes: source.ask_sizes,
            ask_counts: source.ask_counts,
        }
    }

    pub fn ordering_tuple(&self) -> (u64, u64) {
        (self.source_ts_utc_ns, self.source_order)
    }
}

pub fn encode_source_order(part: u16, shard: u32, source_row: u64) -> Result<u64> {
    let shard =
        u16::try_from(shard).map_err(|_| anyhow::anyhow!("LL2 shard index {shard} exceeds u16"))?;
    let source_row = u32::try_from(source_row)
        .map_err(|_| anyhow::anyhow!("LL2 shard row {source_row} exceeds u32"))?;
    if source_row == 0 {
        bail!("LL2 source row must be nonzero");
    }
    Ok((u64::from(part) << 48) | (u64::from(shard) << 32) | u64::from(source_row))
}

pub fn encode_ll2_minute_key(key: &Ll2MinuteKey) -> Result<[u8; LL2_MINUTE_KEY_LEN]> {
    if key.minute_utc_ns == 0 || key.minute_utc_ns % NS_PER_MINUTE != 0 {
        bail!("LL2 minute key timestamp must be a nonzero UTC minute in ns");
    }
    let mut out = [0u8; LL2_MINUTE_KEY_LEN];
    out[..RIC_LEN].copy_from_slice(&encode_ric(&key.ric)?);
    out[RIC_LEN..].copy_from_slice(&key.minute_utc_ns.to_be_bytes());
    Ok(out)
}

pub fn decode_ll2_minute_key(bytes: &[u8]) -> Result<Ll2MinuteKey> {
    if bytes.len() != LL2_MINUTE_KEY_LEN {
        bail!(
            "LL2 minute key must be {LL2_MINUTE_KEY_LEN} bytes, got {}",
            bytes.len()
        );
    }
    let minute_utc_ns = u64::from_be_bytes(bytes[RIC_LEN..].try_into().expect("fixed key slice"));
    if minute_utc_ns == 0 || minute_utc_ns % NS_PER_MINUTE != 0 {
        bail!("LL2 minute key timestamp is not a nonzero UTC minute");
    }
    Ok(Ll2MinuteKey {
        ric: decode_ric(&bytes[..RIC_LEN])?,
        minute_utc_ns,
    })
}

pub fn encode_ll2_minute(value: &Ll2Minute) -> [u8; LL2_MINUTE_VALUE_LEN] {
    let mut out = [0u8; LL2_MINUTE_VALUE_LEN];
    out[0..2].copy_from_slice(&LL2_MINUTE_MAGIC);
    out[2] = LL2_MINUTE_VERSION;
    out[3] = LL2_MINUTE_KIND;
    out[4..12].copy_from_slice(&value.source_ts_utc_ns.to_le_bytes());
    out[12..20].copy_from_slice(&value.source_order.to_le_bytes());
    out[20..28].copy_from_slice(&value.exch_time_ns.to_le_bytes());
    out[28..30].copy_from_slice(&value.gmt_offset_minutes.to_le_bytes());
    let mut offset = 32;
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
    if bytes[30..32].iter().any(|byte| *byte != 0) {
        bail!("LL2 minute value reserved bytes are not zero");
    }
    let source_ts_utc_ns = u64::from_le_bytes(bytes[4..12].try_into().expect("fixed value slice"));
    let source_order = u64::from_le_bytes(bytes[12..20].try_into().expect("fixed value slice"));
    let exch_time_ns = u64::from_le_bytes(bytes[20..28].try_into().expect("fixed value slice"));
    let gmt_offset_minutes =
        i16::from_le_bytes(bytes[28..30].try_into().expect("fixed value slice"));
    if source_ts_utc_ns == 0 || source_order == 0 {
        bail!("LL2 minute value has zero source timestamp or order");
    }
    let mut value = Ll2Minute {
        source_ts_utc_ns,
        source_order,
        exch_time_ns,
        gmt_offset_minutes,
        bid_prices: [MISSING_PRICE; LL2_DEPTH_LEVELS],
        bid_sizes: [MISSING_PRICE; LL2_DEPTH_LEVELS],
        bid_counts: [MISSING_COUNT; LL2_DEPTH_LEVELS],
        ask_prices: [MISSING_PRICE; LL2_DEPTH_LEVELS],
        ask_sizes: [MISSING_PRICE; LL2_DEPTH_LEVELS],
        ask_counts: [MISSING_COUNT; LL2_DEPTH_LEVELS],
    };
    let mut offset = 32;
    for level in 0..LL2_DEPTH_LEVELS {
        value.bid_prices[level] = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        value.bid_sizes[level] = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        value.ask_prices[level] = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        value.ask_sizes[level] = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        value.bid_counts[level] = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        value.ask_counts[level] = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
    }
    Ok(value)
}

fn later_value(left: &[u8], right: &[u8]) -> Result<Vec<u8>> {
    let left_value = decode_ll2_minute(left)?;
    let right_value = decode_ll2_minute(right)?;
    Ok(
        if right_value.ordering_tuple() > left_value.ordering_tuple() {
            right.to_vec()
        } else {
            left.to_vec()
        },
    )
}

pub fn ll2_latest_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut latest = existing.map(ToOwned::to_owned);
    for operand in operands {
        latest = Some(match latest {
            Some(previous) => later_value(&previous, operand).ok()?,
            None => {
                decode_ll2_minute(operand).ok()?;
                operand.to_vec()
            }
        });
    }
    latest
}

pub fn minute_key_for(source: &NormalizedLl2Snapshot) -> Ll2MinuteKey {
    Ll2MinuteKey {
        ric: source.ric.clone(),
        minute_utc_ns: (source.source_ts_utc_ns / NS_PER_MINUTE) * NS_PER_MINUTE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(ts: u64, order: u64, bid: i64) -> Ll2Minute {
        Ll2Minute {
            source_ts_utc_ns: ts,
            source_order: order,
            exch_time_ns: 82_800_000_000_000,
            gmt_offset_minutes: -360,
            bid_prices: [bid; LL2_DEPTH_LEVELS],
            bid_sizes: [1_000_000_000; LL2_DEPTH_LEVELS],
            bid_counts: [1; LL2_DEPTH_LEVELS],
            ask_prices: [bid + 250_000_000; LL2_DEPTH_LEVELS],
            ask_sizes: [2_000_000_000; LL2_DEPTH_LEVELS],
            ask_counts: [2; LL2_DEPTH_LEVELS],
        }
    }

    #[test]
    fn key_and_value_round_trip() {
        let key = Ll2MinuteKey {
            ric: "ESH24".into(),
            minute_utc_ns: 1_704_754_800_000_000_000,
        };
        assert_eq!(
            decode_ll2_minute_key(&encode_ll2_minute_key(&key).unwrap()).unwrap(),
            key
        );
        let value = sample(
            1_704_754_800_999_999_999,
            encode_source_order(3, 2, 7).unwrap(),
            4_798_000_000_000,
        );
        assert_eq!(
            decode_ll2_minute(&encode_ll2_minute(&value)).unwrap(),
            value
        );
    }

    #[test]
    fn later_source_timestamp_then_order_wins() {
        let old = encode_ll2_minute(&sample(10, encode_source_order(0, 0, 9).unwrap(), 100));
        let same_time_later_part =
            encode_ll2_minute(&sample(10, encode_source_order(1, 0, 1).unwrap(), 200));
        let later_time = encode_ll2_minute(&sample(11, encode_source_order(0, 1, 1).unwrap(), 300));
        assert_eq!(
            decode_ll2_minute(&later_value(&old, &same_time_later_part).unwrap())
                .unwrap()
                .bid_prices[0],
            200
        );
        assert_eq!(
            decode_ll2_minute(&later_value(&same_time_later_part, &later_time).unwrap())
                .unwrap()
                .bid_prices[0],
            300
        );
    }
}
