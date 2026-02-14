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

    pub fn to_bytes(&self) -> Result<Bytes> {
        if self.msg_type != TRADE_FLOW_FEATURE_MSG_TYPE {
            bail!(
                "invalid TradeFlowFeatureMsg type {}, expected {}",
                self.msg_type,
                TRADE_FLOW_FEATURE_MSG_TYPE
            );
        }
        let total_size = 4 + 4 + self.symbol_length as usize + 1 + 8 + (self.values.len() * 8);
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type);
        buf.put_u32_le(self.symbol_length);
        buf.put(self.symbol.as_bytes());
        buf.put_u8(self.venue);
        buf.put_i64_le(self.ts);
        for value in &self.values {
            buf.put_f64_le(*value);
        }
        Ok(buf.freeze())
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

        let value_bytes = data.len().saturating_sub(offset);
        if value_bytes % 8 != 0 {
            bail!(
                "TradeFlowFeatureMsg invalid value bytes: {} (not aligned to f64)",
                value_bytes
            );
        }
        let value_count = value_bytes / 8;
        if value_count < TRADE_FLOW_FEATURE_DIM {
            bail!(
                "TradeFlowFeatureMsg value count too small: {} < {}",
                value_count,
                TRADE_FLOW_FEATURE_DIM
            );
        }

        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
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
}
