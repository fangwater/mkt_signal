const ORDER_QUEUE_POSITION_RECORD_FIXED_BYTES: usize = 68;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderQueuePositionAction {
    New = 1,
    PartiallyFilled = 2,
    Filled = 3,
    Canceled = 4,
    Replaced = 5,
    Rejected = 6,
    Expired = 7,
}

impl OrderQueuePositionAction {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::New),
            2 => Some(Self::PartiallyFilled),
            3 => Some(Self::Filled),
            4 => Some(Self::Canceled),
            5 => Some(Self::Replaced),
            6 => Some(Self::Rejected),
            7 => Some(Self::Expired),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::PartiallyFilled => "partially_filled",
            Self::Filled => "filled",
            Self::Canceled => "canceled",
            Self::Replaced => "replaced",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderQueuePositionRecord {
    pub recv_ts_us: i64,
    pub account_id: String,
    pub venue: u8,
    pub action: OrderQueuePositionAction,
    pub create_tp: i64,
    pub update_tp: i64,
    pub local_tp: i64,
    pub client_order_id: i64,
    pub tlen: f64,
    pub backlen: f64,
    pub inpos: f64,
}

impl OrderQueuePositionRecord {
    pub fn encoded_len(payload: &[u8]) -> Option<usize> {
        if payload.len() < 10 {
            return None;
        }
        let account_id_len = u16::from_le_bytes(payload[8..10].try_into().ok()?) as usize;
        let encoded_len = ORDER_QUEUE_POSITION_RECORD_FIXED_BYTES.checked_add(account_id_len)?;
        (payload.len() >= encoded_len).then_some(encoded_len)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let account_id = self.account_id.as_bytes();
        let account_id_len = u16::try_from(account_id.len())
            .map_err(|_| format!("account_id too long: {} bytes", account_id.len()))?;
        let mut out =
            Vec::with_capacity(ORDER_QUEUE_POSITION_RECORD_FIXED_BYTES + account_id.len());
        out.extend_from_slice(&self.recv_ts_us.to_le_bytes());
        out.extend_from_slice(&account_id_len.to_le_bytes());
        out.extend_from_slice(account_id);
        out.push(self.venue);
        out.push(self.action.to_u8());
        out.extend_from_slice(&self.create_tp.to_le_bytes());
        out.extend_from_slice(&self.update_tp.to_le_bytes());
        out.extend_from_slice(&self.local_tp.to_le_bytes());
        out.extend_from_slice(&self.client_order_id.to_le_bytes());
        out.extend_from_slice(&self.tlen.to_le_bytes());
        out.extend_from_slice(&self.backlen.to_le_bytes());
        out.extend_from_slice(&self.inpos.to_le_bytes());
        Ok(out)
    }

    pub fn from_bytes(payload: &[u8]) -> Result<Self, String> {
        let encoded_len = Self::encoded_len(payload).ok_or_else(|| {
            format!(
                "order queue position record too short: {} bytes",
                payload.len()
            )
        })?;
        if encoded_len != payload.len() {
            return Err(format!(
                "order queue position record has {} trailing bytes",
                payload.len() - encoded_len
            ));
        }

        let recv_ts_us = i64::from_le_bytes(payload[0..8].try_into().expect("checked record"));
        let account_id_len =
            u16::from_le_bytes(payload[8..10].try_into().expect("checked record")) as usize;
        let account_id_end = 10 + account_id_len;
        let account_id = String::from_utf8(payload[10..account_id_end].to_vec())
            .map_err(|err| format!("invalid account_id utf8: {err}"))?;
        let venue = payload[account_id_end];
        let action =
            OrderQueuePositionAction::from_u8(payload[account_id_end + 1]).ok_or_else(|| {
                format!(
                    "invalid order queue position action: {}",
                    payload[account_id_end + 1]
                )
            })?;
        let mut offset = account_id_end + 2;
        let mut read_i64 = || {
            let value = i64::from_le_bytes(
                payload[offset..offset + 8]
                    .try_into()
                    .expect("checked record"),
            );
            offset += 8;
            value
        };
        let create_tp = read_i64();
        let update_tp = read_i64();
        let local_tp = read_i64();
        let client_order_id = read_i64();
        let mut read_f64 = || {
            let value = f64::from_le_bytes(
                payload[offset..offset + 8]
                    .try_into()
                    .expect("checked record"),
            );
            offset += 8;
            value
        };

        Ok(Self {
            recv_ts_us,
            account_id,
            venue,
            action,
            create_tp,
            update_tp,
            local_tp,
            client_order_id,
            tlen: read_f64(),
            backlen: read_f64(),
            inpos: read_f64(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_queue_position_record_roundtrip() {
        let record = OrderQueuePositionRecord {
            recv_ts_us: 100,
            account_id: "binance-intra-arb01".to_string(),
            venue: 2,
            action: OrderQueuePositionAction::PartiallyFilled,
            create_tp: 101,
            update_tp: 102,
            local_tp: 103,
            client_order_id: 104,
            tlen: 5.0,
            backlen: 2.0,
            inpos: 1.0,
        };
        let bytes = record.to_bytes().unwrap();
        assert_eq!(
            OrderQueuePositionRecord::encoded_len(&bytes),
            Some(bytes.len())
        );
        assert_eq!(
            OrderQueuePositionRecord::from_bytes(&bytes).unwrap(),
            record
        );
    }
}
