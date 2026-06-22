use bytes::{BufMut, Bytes, BytesMut};

pub const ORDER_TERMINATE_PAYLOAD_LEN: usize = 32;
pub const INTERNAL_OPEN_TERMINATED_ERROR_CODE: i32 = -900001;
pub const INTERNAL_OPEN_TERMINATE_TTL_US: i64 = 1_000;

const ACTION_TERMINATE_OPEN: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InternalOpenTerminateMsg {
    pub create_time: i64,
    pub client_order_id: i64,
    pub trigger_ts: i64,
}

impl InternalOpenTerminateMsg {
    pub fn new(create_time: i64, client_order_id: i64, trigger_ts: i64) -> Self {
        Self {
            create_time,
            client_order_id,
            trigger_ts,
        }
    }

    pub fn to_payload(self) -> [u8; ORDER_TERMINATE_PAYLOAD_LEN] {
        let mut buf = BytesMut::with_capacity(ORDER_TERMINATE_PAYLOAD_LEN);
        buf.put_u32_le(ACTION_TERMINATE_OPEN);
        buf.put_u32_le(0);
        buf.put_i64_le(self.create_time);
        buf.put_i64_le(self.client_order_id);
        buf.put_i64_le(self.trigger_ts);
        let bytes = buf.freeze();
        let mut payload = [0u8; ORDER_TERMINATE_PAYLOAD_LEN];
        payload.copy_from_slice(&bytes[..ORDER_TERMINATE_PAYLOAD_LEN]);
        payload
    }

    pub fn to_bytes(self) -> Bytes {
        Bytes::copy_from_slice(&self.to_payload())
    }

    pub fn parse(payload: &[u8]) -> Option<Self> {
        if payload.len() < ORDER_TERMINATE_PAYLOAD_LEN {
            return None;
        }
        let action = u32::from_le_bytes(payload[0..4].try_into().ok()?);
        if action != ACTION_TERMINATE_OPEN {
            return None;
        }
        let create_time = i64::from_le_bytes(payload[8..16].try_into().ok()?);
        let client_order_id = i64::from_le_bytes(payload[16..24].try_into().ok()?);
        let trigger_ts = i64::from_le_bytes(payload[24..32].try_into().ok()?);
        if client_order_id <= 0 {
            return None;
        }
        Some(Self {
            create_time,
            client_order_id,
            trigger_ts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::InternalOpenTerminateMsg;

    #[test]
    fn internal_open_terminate_roundtrips_fixed_payload() {
        let msg = InternalOpenTerminateMsg::new(11, 22, 33);
        let payload = msg.to_payload();
        assert_eq!(InternalOpenTerminateMsg::parse(&payload), Some(msg));
    }
}
