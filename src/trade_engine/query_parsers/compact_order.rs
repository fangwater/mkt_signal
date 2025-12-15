use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

pub const COMPACT_ORDER_QUERY_RESP_LEN: usize = 8 + 8 + 1 + 8 + 1 + 8;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactOrderQueryResp {
    pub executed_qty: f64,
    pub order_id: i64,
    pub status_u8: u8,
    pub update_time_ms: i64,
    pub time_in_force_u8: u8,
    pub trade_id: i64,
}

impl CompactOrderQueryResp {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(COMPACT_ORDER_QUERY_RESP_LEN);
        buf.put_f64_le(self.executed_qty);
        buf.put_i64_le(self.order_id);
        buf.put_u8(self.status_u8);
        buf.put_i64_le(self.update_time_ms);
        buf.put_u8(self.time_in_force_u8);
        buf.put_i64_le(self.trade_id);
        buf.freeze()
    }

    pub fn from_bytes_prefix(body: &[u8]) -> Result<Self> {
        if body.len() < COMPACT_ORDER_QUERY_RESP_LEN {
            anyhow::bail!(
                "compact order query resp too short: {} < {}",
                body.len(),
                COMPACT_ORDER_QUERY_RESP_LEN
            );
        }
        let executed_qty = f64::from_le_bytes(body[0..8].try_into()?);
        let order_id = i64::from_le_bytes(body[8..16].try_into()?);
        let status_u8 = body[16];
        let update_time_ms = i64::from_le_bytes(body[17..25].try_into()?);
        let time_in_force_u8 = body[25];
        let trade_id = i64::from_le_bytes(body[26..34].try_into()?);
        Ok(Self {
            executed_qty,
            order_id,
            status_u8,
            update_time_ms,
            time_in_force_u8,
            trade_id,
        })
    }
}
