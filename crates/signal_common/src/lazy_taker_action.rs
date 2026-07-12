use crate::common::bytes_helper::{fixed_bytes_from_str, fixed_bytes_len};

pub const LAZY_TAKER_ACTION_CHANNEL: &str = "lazy_taker_action";
pub const LAZY_TAKER_ACTION_PAYLOAD: usize = 80;
pub const LAZY_TAKER_ACTION_ENCODED_LEN: usize = 80;

const MAGIC: [u8; 4] = *b"LTA1";
const VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LazyTakerAction {
    Hold = 1,
    Take = 2,
}

impl LazyTakerAction {
    fn from_u8(raw: u8) -> Option<Self> {
        match raw {
            1 => Some(Self::Hold),
            2 => Some(Self::Take),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LazyTakerActionMsg {
    pub local_tp_us: i64,
    pub symbol: [u8; 32],
    pub model_name: [u8; 32],
    pub venue: u8,
    pub action: LazyTakerAction,
    /// +1 means a sell taker, -1 means a buy taker.
    pub direction: i8,
}

impl LazyTakerActionMsg {
    pub fn new(
        local_tp_us: i64,
        symbol: &str,
        model_name: &str,
        venue: u8,
        action: LazyTakerAction,
        direction: i8,
    ) -> Option<Self> {
        if local_tp_us <= 0 || !matches!(direction, -1 | 1) {
            return None;
        }
        Some(Self {
            local_tp_us,
            symbol: fixed_bytes_from_str(symbol),
            model_name: fixed_bytes_from_str(model_name),
            venue,
            action,
            direction,
        })
    }

    pub fn encode(&self) -> [u8; LAZY_TAKER_ACTION_PAYLOAD] {
        let mut out = [0u8; LAZY_TAKER_ACTION_PAYLOAD];
        out[..4].copy_from_slice(&MAGIC);
        out[4] = VERSION;
        out[5] = self.action as u8;
        out[6] = self.direction as u8;
        out[7] = self.venue;
        out[8..16].copy_from_slice(&self.local_tp_us.to_le_bytes());
        out[16..48].copy_from_slice(&self.symbol);
        out[48..80].copy_from_slice(&self.model_name);
        out
    }

    pub fn decode(raw: &[u8]) -> Option<Self> {
        if raw.len() < LAZY_TAKER_ACTION_ENCODED_LEN || raw[..4] != MAGIC || raw[4] != VERSION {
            return None;
        }
        let action = LazyTakerAction::from_u8(raw[5])?;
        let direction = raw[6] as i8;
        if !matches!(direction, -1 | 1) {
            return None;
        }
        let local_tp_us = i64::from_le_bytes(raw[8..16].try_into().ok()?);
        if local_tp_us <= 0 {
            return None;
        }
        let mut symbol = [0u8; 32];
        symbol.copy_from_slice(&raw[16..48]);
        let mut model_name = [0u8; 32];
        model_name.copy_from_slice(&raw[48..80]);
        Some(Self {
            local_tp_us,
            symbol,
            model_name,
            venue: raw[7],
            action,
            direction,
        })
    }

    pub fn symbol_str(&self) -> &str {
        std::str::from_utf8(&self.symbol[..fixed_bytes_len(&self.symbol)]).unwrap_or("")
    }

    pub fn model_name_str(&self) -> &str {
        std::str::from_utf8(&self.model_name[..fixed_bytes_len(&self.model_name)]).unwrap_or("")
    }
}

#[cfg(test)]
mod tests {
    use super::{LazyTakerAction, LazyTakerActionMsg};

    #[test]
    fn action_message_roundtrip() {
        let msg = LazyTakerActionMsg::new(
            123_456,
            "BTCUSDT",
            "mid-re-30s",
            2,
            LazyTakerAction::Hold,
            -1,
        )
        .unwrap();
        let decoded = LazyTakerActionMsg::decode(&msg.encode()).unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(decoded.symbol_str(), "BTCUSDT");
        assert_eq!(decoded.model_name_str(), "mid-re-30s");
    }
}
