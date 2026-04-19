use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};

use crate::common::mkt_msg::MktMsgType;

pub const TRADE_FLOW_FEATURE_MSG_TYPE: u32 = MktMsgType::TradeFlowFeature as u32;
pub const TRADE_FLOW_FEATURE_DIM: usize = 32;

pub const TRADE_FLOW_FEATURE_FIELD_NAMES: [&str; TRADE_FLOW_FEATURE_DIM] = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "avg_amount",
    "count",
    "buy_count",
    "sell_count",
    "buy_amount",
    "sell_amount",
    "buy_volume",
    "sell_volume",
    "large_order",
    "medium_order",
    "small_order",
    "large_buy",
    "large_sell",
    "medium_buy",
    "medium_sell",
    "small_buy",
    "small_sell",
    "vwap",
    "buy_vwap",
    "sell_vwap",
    "net_buy_amount",
    "net_buy_volume",
    "net_buy_pct",
    "net_buy_large",
    "net_buy_medium",
    "net_buy_small",
];

#[derive(Debug, Clone)]
pub struct TradeFlowFeatureMsg {
    pub msg_type: u32,
    pub symbol_length: u32,
    pub symbol: String,
    pub venue: u8,
    pub ts: i64,
    pub values: Vec<f64>,
}

impl TradeFlowFeatureMsg {
    pub fn from_indexed_values(symbol: String, venue: u8, ts: i64, values: &[f64]) -> Result<Self> {
        if values.len() < TRADE_FLOW_FEATURE_DIM {
            bail!(
                "TradeFlowFeatureMsg expects at least {} values, got {}",
                TRADE_FLOW_FEATURE_DIM,
                values.len()
            );
        }
        Ok(Self {
            msg_type: TRADE_FLOW_FEATURE_MSG_TYPE,
            symbol_length: symbol.len() as u32,
            symbol,
            venue,
            ts,
            values: values.to_vec(),
        })
    }

    pub fn encode_from_slices(
        symbol: &str,
        venue: u8,
        ts: i64,
        head: &[f64],
        tail: &[f64],
    ) -> Result<Bytes> {
        encode_value_slices_to_bytes(symbol, venue, ts, &[head, tail])
    }

    pub fn to_bytes(&self) -> Result<Bytes> {
        if self.msg_type != TRADE_FLOW_FEATURE_MSG_TYPE {
            bail!(
                "invalid TradeFlowFeatureMsg type {}, expected {}",
                self.msg_type,
                TRADE_FLOW_FEATURE_MSG_TYPE
            );
        }
        encode_value_slices_to_bytes(&self.symbol, self.venue, self.ts, &[self.values.as_slice()])
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let min_len = 4 + 4 + 1 + 8 + (TRADE_FLOW_FEATURE_DIM * 8);
        if data.len() < min_len {
            bail!("TradeFlowFeatureMsg too short: {}", data.len());
        }

        let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if msg_type != TRADE_FLOW_FEATURE_MSG_TYPE {
            bail!(
                "invalid TradeFlowFeatureMsg type {}, expected {}",
                msg_type,
                TRADE_FLOW_FEATURE_MSG_TYPE
            );
        }

        let symbol_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let need = match symbol_len.checked_add(1 + 8 + (TRADE_FLOW_FEATURE_DIM * 8)) {
            Some(v) => v,
            None => {
                bail!(
                    "TradeFlowFeatureMsg payload size overflow: symbol_len={}",
                    symbol_len
                )
            }
        };
        let remaining = data.len().saturating_sub(8);
        if remaining < need {
            bail!(
                "TradeFlowFeatureMsg truncated before payload: remaining={} need={}",
                remaining,
                need
            );
        }

        let mut offset = 8usize;
        let symbol_end = match offset.checked_add(symbol_len) {
            Some(v) => v,
            None => {
                bail!(
                    "TradeFlowFeatureMsg symbol end overflow: offset={} symbol_len={}",
                    offset,
                    symbol_len
                )
            }
        };
        let symbol = String::from_utf8(data[offset..symbol_end].to_vec())?;
        offset = symbol_end;

        let venue = data[offset];
        offset += 1;
        let ts = i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        let mut value_end = data.len();
        let value_bytes = value_end.saturating_sub(offset);
        let remainder = value_bytes % 8;
        if remainder != 0 {
            // iceoryx fixed-size payload may append trailing zero padding bytes.
            // If misalignment only comes from zero tail bytes, trim them and continue.
            let tail_start = value_end - remainder;
            let tail = &data[tail_start..value_end];
            if tail.iter().all(|b| *b == 0) {
                value_end = tail_start;
            } else {
                bail!(
                    "TradeFlowFeatureMsg invalid value bytes: {} (not aligned to f64)",
                    value_bytes
                );
            }
        }

        let value_count = value_end.saturating_sub(offset) / 8;
        if value_count < TRADE_FLOW_FEATURE_DIM {
            bail!(
                "TradeFlowFeatureMsg value count too small: {} < {}",
                value_count,
                TRADE_FLOW_FEATURE_DIM
            );
        }

        let mut values = Vec::with_capacity(value_count);
        while offset + 8 <= value_end {
            values.push(f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]));
            offset += 8;
        }

        Ok(Self {
            msg_type,
            symbol_length: symbol.len() as u32,
            symbol,
            venue,
            ts,
            values,
        })
    }
}

fn encode_value_slices_to_bytes(
    symbol: &str,
    venue: u8,
    ts: i64,
    value_slices: &[&[f64]],
) -> Result<Bytes> {
    let value_count = value_slices.iter().try_fold(0usize, |acc, values| {
        acc.checked_add(values.len())
            .ok_or_else(|| anyhow::anyhow!("TradeFlowFeatureMsg value count overflow"))
    })?;
    if value_count < TRADE_FLOW_FEATURE_DIM {
        bail!(
            "TradeFlowFeatureMsg expects at least {} values, got {}",
            TRADE_FLOW_FEATURE_DIM,
            value_count
        );
    }

    let value_bytes = value_count
        .checked_mul(8)
        .ok_or_else(|| anyhow::anyhow!("TradeFlowFeatureMsg byte size overflow"))?;
    let total_size = 4 + 4 + symbol.len() + 1 + 8 + value_bytes;
    let mut buf = BytesMut::with_capacity(total_size);
    buf.put_u32_le(TRADE_FLOW_FEATURE_MSG_TYPE);
    buf.put_u32_le(symbol.len() as u32);
    buf.put(symbol.as_bytes());
    buf.put_u8(venue);
    buf.put_i64_le(ts);
    for values in value_slices {
        for value in *values {
            buf.put_f64_le(*value);
        }
    }
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_from_bytes_matches_original() {
        let values: Vec<f64> = (0..TRADE_FLOW_FEATURE_DIM)
            .map(|i| i as f64 * 1.25 - 3.0)
            .collect();
        let original = TradeFlowFeatureMsg::from_indexed_values(
            "BTCUSDT".to_string(),
            2,
            1_735_000_000_123,
            &values,
        )
        .expect("build message");
        let bytes = original.to_bytes().expect("serialize");
        let parsed = TradeFlowFeatureMsg::from_bytes(bytes.as_ref()).expect("parse");

        assert_eq!(parsed.msg_type, original.msg_type);
        assert_eq!(parsed.symbol_length, original.symbol_length);
        assert_eq!(parsed.symbol, original.symbol);
        assert_eq!(parsed.venue, original.venue);
        assert_eq!(parsed.ts, original.ts);
        assert_eq!(parsed.values, original.values);
    }

    #[test]
    fn supports_variable_length_payload() {
        let values: Vec<f64> = (0..(TRADE_FLOW_FEATURE_DIM + 80))
            .map(|i| i as f64 * 0.1)
            .collect();
        let original =
            TradeFlowFeatureMsg::from_indexed_values("ETHUSDT".to_string(), 2, 123_456, &values)
                .expect("build message");
        let bytes = original.to_bytes().expect("serialize");
        let parsed = TradeFlowFeatureMsg::from_bytes(bytes.as_ref()).expect("parse");
        assert_eq!(parsed.values.len(), TRADE_FLOW_FEATURE_DIM + 80);
        assert_eq!(parsed.values, values);
    }

    #[test]
    fn supports_fixed_buffer_zero_padding_with_unaligned_symbol_len() {
        let values: Vec<f64> = (0..(TRADE_FLOW_FEATURE_DIM + 80))
            .map(|i| i as f64 * 0.01 + 7.0)
            .collect();
        // len=8, which triggers non-8-aligned tail when wrapped in fixed 1024-byte IPC payload.
        let original = TradeFlowFeatureMsg::from_indexed_values(
            "BTCUSDTM".to_string(),
            2,
            1_735_000_123_456,
            &values,
        )
        .expect("build message");
        let bytes = original.to_bytes().expect("serialize");

        let mut fixed = [0u8; 1024];
        fixed[..bytes.len()].copy_from_slice(bytes.as_ref());

        let parsed = TradeFlowFeatureMsg::from_bytes(&fixed).expect("parse padded payload");
        assert_eq!(parsed.symbol, original.symbol);
        assert_eq!(parsed.venue, original.venue);
        assert_eq!(parsed.ts, original.ts);
        assert!(parsed.values.len() >= values.len());
        assert_eq!(&parsed.values[..values.len()], values.as_slice());
    }

    #[test]
    fn rejects_misaligned_non_zero_tail() {
        let values: Vec<f64> = (0..TRADE_FLOW_FEATURE_DIM).map(|i| i as f64).collect();
        let msg = TradeFlowFeatureMsg::from_indexed_values("BTCUSDT".to_string(), 2, 123, &values)
            .expect("build message");
        let bytes = msg.to_bytes().expect("serialize");

        let mut corrupted = bytes.to_vec();
        corrupted.push(1);

        let err =
            TradeFlowFeatureMsg::from_bytes(&corrupted).expect_err("non-zero tail should error");
        assert!(
            err.to_string().contains("invalid value bytes"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn encode_from_slices_roundtrips_without_materializing_values_vec() {
        let head: Vec<f64> = (0..TRADE_FLOW_FEATURE_DIM)
            .map(|i| i as f64 * 0.25 + 1.0)
            .collect();
        let tail: Vec<f64> = (0..80).map(|i| 100.0 + i as f64).collect();

        let bytes = TradeFlowFeatureMsg::encode_from_slices("BTCUSDT", 2, 42, &head, &tail)
            .expect("encode");
        let parsed = TradeFlowFeatureMsg::from_bytes(bytes.as_ref()).expect("parse");

        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.venue, 2);
        assert_eq!(parsed.ts, 42);
        assert_eq!(parsed.values.len(), TRADE_FLOW_FEATURE_DIM + 80);
        assert_eq!(&parsed.values[..TRADE_FLOW_FEATURE_DIM], head.as_slice());
        assert_eq!(&parsed.values[TRADE_FLOW_FEATURE_DIM..], tail.as_slice());
    }
}
