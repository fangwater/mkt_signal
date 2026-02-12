use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    pub values: [f64; TRADE_FLOW_FEATURE_DIM],
}

impl TradeFlowFeatureMsg {
    pub fn from_indexed_values(symbol: String, venue: u8, ts: i64, values: &[f64]) -> Result<Self> {
        if values.len() != TRADE_FLOW_FEATURE_DIM {
            bail!(
                "TradeFlowFeatureMsg expects {} values, got {}",
                TRADE_FLOW_FEATURE_DIM,
                values.len()
            );
        }
        let mut arr = [0.0f64; TRADE_FLOW_FEATURE_DIM];
        arr.copy_from_slice(values);
        Ok(Self {
            msg_type: TRADE_FLOW_FEATURE_MSG_TYPE,
            symbol_length: symbol.len() as u32,
            symbol,
            venue,
            ts,
            values: arr,
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
        let total_size = 4 + 4 + self.symbol_length as usize + 1 + 8 + (TRADE_FLOW_FEATURE_DIM * 8);
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type);
        buf.put_u32_le(self.symbol_length);
        buf.put(self.symbol.as_bytes());
        buf.put_u8(self.venue);
        buf.put_i64_le(self.ts);
        for value in self.values {
            buf.put_f64_le(value);
        }
        Ok(buf.freeze())
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let min_len = 4 + 4 + 1 + 8 + (TRADE_FLOW_FEATURE_DIM * 8);
        if data.len() < min_len {
            bail!("TradeFlowFeatureMsg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != TRADE_FLOW_FEATURE_MSG_TYPE {
            bail!(
                "invalid TradeFlowFeatureMsg type {}, expected {}",
                msg_type,
                TRADE_FLOW_FEATURE_MSG_TYPE
            );
        }

        let symbol_len = cursor.get_u32_le() as usize;
        if cursor.remaining() < symbol_len + 1 + 8 + (TRADE_FLOW_FEATURE_DIM * 8) {
            bail!(
                "TradeFlowFeatureMsg truncated before payload: remaining={} need={}",
                cursor.remaining(),
                symbol_len + 1 + 8 + (TRADE_FLOW_FEATURE_DIM * 8)
            );
        }

        let symbol = String::from_utf8(cursor.copy_to_bytes(symbol_len).to_vec())?;
        let venue = cursor.get_u8();
        let ts = cursor.get_i64_le();

        let mut values = [0.0f64; TRADE_FLOW_FEATURE_DIM];
        for value in &mut values {
            *value = cursor.get_f64_le();
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
