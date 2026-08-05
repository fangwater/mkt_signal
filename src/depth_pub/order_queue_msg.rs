use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
pub use persist_common::OrderQueuePositionAction;

const ORDER_QUEUE_POSITION_MSG_BYTES: usize = 64;
pub const ORDER_QUEUE_POSITION_MAX_BYTES: usize = ORDER_QUEUE_POSITION_MSG_BYTES;

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderQueuePositionMsgType {
    OrderQueuePositionUpdate = 2060,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderQueuePositionMsg {
    pub action: OrderQueuePositionAction,
    pub create_tp: i64,
    pub update_tp: i64,
    pub local_tp: i64,
    pub client_order_id: i64,
    pub tlen: f64,
    pub backlen: f64,
    pub inpos: f64,
}

impl OrderQueuePositionMsg {
    pub fn byte_size(&self) -> usize {
        ORDER_QUEUE_POSITION_MSG_BYTES
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.byte_size());
        buf.put_u32_le(OrderQueuePositionMsgType::OrderQueuePositionUpdate as u32);
        buf.put_u8(self.action.to_u8());
        buf.put(&[0u8; 3][..]);
        buf.put_i64_le(self.create_tp);
        buf.put_i64_le(self.update_tp);
        buf.put_i64_le(self.local_tp);
        buf.put_i64_le(self.client_order_id);
        buf.put_f64_le(self.tlen);
        buf.put_f64_le(self.backlen);
        buf.put_f64_le(self.inpos);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < ORDER_QUEUE_POSITION_MSG_BYTES {
            return Err(anyhow!(
                "order queue position msg too short: {} < {}",
                data.len(),
                ORDER_QUEUE_POSITION_MSG_BYTES
            ));
        }
        let msg_type = u32::from_le_bytes(data[0..4].try_into()?);
        if msg_type != OrderQueuePositionMsgType::OrderQueuePositionUpdate as u32 {
            return Err(anyhow!(
                "unexpected order queue position msg_type: {}",
                msg_type
            ));
        }
        let action = OrderQueuePositionAction::from_u8(data[4])
            .ok_or_else(|| anyhow!("invalid order queue position action: {}", data[4]))?;
        let create_tp = i64::from_le_bytes(data[8..16].try_into()?);
        let update_tp = i64::from_le_bytes(data[16..24].try_into()?);
        let local_tp = i64::from_le_bytes(data[24..32].try_into()?);
        let client_order_id = i64::from_le_bytes(data[32..40].try_into()?);
        let tlen = f64::from_le_bytes(data[40..48].try_into()?);
        let backlen = f64::from_le_bytes(data[48..56].try_into()?);
        let inpos = f64::from_le_bytes(data[56..64].try_into()?);

        Ok(Self {
            action,
            create_tp,
            update_tp,
            local_tp,
            client_order_id,
            tlen,
            backlen,
            inpos,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_queue_position_msg_roundtrip() {
        let msg = OrderQueuePositionMsg {
            action: OrderQueuePositionAction::PartiallyFilled,
            create_tp: 111,
            update_tp: 222,
            local_tp: 333,
            client_order_id: 42,
            tlen: 3.0,
            backlen: 2.0,
            inpos: 1.0,
        };
        let bytes = msg.to_bytes();
        assert_eq!(bytes.len(), 64);
        assert!(bytes.len() <= ORDER_QUEUE_POSITION_MAX_BYTES);
        assert_eq!(OrderQueuePositionMsg::from_bytes(&bytes).unwrap(), msg);
    }
}
