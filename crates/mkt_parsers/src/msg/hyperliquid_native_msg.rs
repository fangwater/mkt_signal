use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use sha2::{Digest, Sha256};

use super::basic_account_msg::BasicAccountEventType;
use super::hyperliquid_account_msg::HyperliquidFactIdentity;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HyperliquidNativeSource {
    Liquidation = 1,
    NonUserCancel = 2,
    TwapStates = 3,
    ActiveAssetData = 4,
    OrderLifecycle = 5,
    Notification = 6,
    WebData = 7,
    BorrowLendUser = 8,
    BorrowLendReserves = 9,
}

impl HyperliquidNativeSource {
    fn decode(value: u8) -> Result<Self> {
        Ok(match value {
            1 => Self::Liquidation,
            2 => Self::NonUserCancel,
            3 => Self::TwapStates,
            4 => Self::ActiveAssetData,
            5 => Self::OrderLifecycle,
            6 => Self::Notification,
            7 => Self::WebData,
            8 => Self::BorrowLendUser,
            9 => Self::BorrowLendReserves,
            _ => bail!("unknown Hyperliquid native source {value}"),
        })
    }
}

/// Auditable venue JSON, including fields not consumed by strategy logic.
/// Source timestamps stay in the JSON in their native units; observed_at_us
/// is explicitly local receipt time and never an exchange event timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidNativeEventMsg {
    pub identity: HyperliquidFactIdentity,
    pub observed_at_us: i64,
    pub source: HyperliquidNativeSource,
    pub event_key: String,
    pub payload_json: String,
}

impl HyperliquidNativeEventMsg {
    pub fn create(
        observed_at_us: i64,
        source: HyperliquidNativeSource,
        event_key: String,
        payload: &serde_json::Value,
    ) -> Result<Self> {
        if observed_at_us < 0 || event_key.is_empty() {
            bail!("invalid Hyperliquid native event receipt time or identity");
        }
        let mut payload = payload.clone();
        payload.sort_all_objects();
        Ok(Self {
            identity: HyperliquidFactIdentity {
                account_hash: [0; 32],
                monitor_id: 0,
                fact_seq: 0,
            },
            observed_at_us,
            source,
            event_key,
            payload_json: serde_json::to_string(&payload)?,
        })
    }

    pub fn with_fact_identity(mut self, identity: HyperliquidFactIdentity) -> Self {
        self.identity = identity;
        self
    }

    pub fn stable_venue_key(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"mkt_signal/hyperliquid/native_event");
        hasher.update(self.identity.account_hash);
        hasher.update([self.source as u8]);
        hasher.update(self.event_key.as_bytes());
        hasher.finalize().into()
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut output =
            BytesMut::with_capacity(77 + self.event_key.len() + self.payload_json.len());
        output.put_u32_le(BasicAccountEventType::HyperliquidNativeEvent as u32);
        output.put_slice(&self.identity.account_hash);
        output.put_u64_le(self.identity.monitor_id);
        output.put_u64_le(self.identity.fact_seq);
        output.put_i64_le(self.observed_at_us);
        output.put_u8(self.source as u8);
        for value in [&self.event_key, &self.payload_json] {
            output.put_u32_le(value.len() as u32);
            output.put_slice(value.as_bytes());
        }
        output.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 69 {
            bail!("Hyperliquid native event truncated");
        }
        let mut input = data;
        if input.get_u32_le() != BasicAccountEventType::HyperliquidNativeEvent as u32 {
            bail!("invalid Hyperliquid native event type");
        }
        let mut account_hash = [0; 32];
        input.copy_to_slice(&mut account_hash);
        let identity = HyperliquidFactIdentity {
            account_hash,
            monitor_id: input.get_u64_le(),
            fact_seq: input.get_u64_le(),
        };
        let observed_at_us = input.get_i64_le();
        let source = HyperliquidNativeSource::decode(input.get_u8())?;
        let mut read_string = || -> Result<String> {
            if input.len() < 4 {
                bail!("Hyperliquid native string length truncated");
            }
            let len = input.get_u32_le() as usize;
            if input.len() < len {
                bail!("Hyperliquid native string truncated");
            }
            let value = std::str::from_utf8(&input[..len])
                .context("Hyperliquid native string is not UTF-8")?
                .to_string();
            input.advance(len);
            Ok(value)
        };
        let event_key = read_string()?;
        let payload_json = read_string()?;
        if !input.is_empty() || observed_at_us < 0 || event_key.is_empty() {
            bail!("invalid Hyperliquid native event fields or trailing data");
        }
        serde_json::from_str::<serde_json::Value>(&payload_json)
            .context("invalid Hyperliquid native JSON")?;
        Ok(Self {
            identity,
            observed_at_us,
            source,
            event_key,
            payload_json,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hyperliquid_native_roundtrip_preserves_fields_and_epoch_independent_key() {
        let msg = HyperliquidNativeEventMsg::create(
            123_000,
            HyperliquidNativeSource::Liquidation,
            "lid:7".into(),
            &serde_json::json!({"lid":7,"futureField":{"precise":"0.00100"}}),
        )
        .unwrap();
        assert_eq!(
            HyperliquidNativeEventMsg::from_bytes(&msg.to_bytes()).unwrap(),
            msg
        );
        let mut next = msg.clone();
        next.identity.monitor_id = 10;
        next.identity.fact_seq = 20;
        assert_eq!(next.stable_venue_key(), msg.stable_venue_key());
        next.identity.account_hash[0] = 1;
        assert_ne!(next.stable_venue_key(), msg.stable_venue_key());
        let bytes = msg.to_bytes();
        for end in 0..bytes.len() {
            assert!(HyperliquidNativeEventMsg::from_bytes(&bytes[..end]).is_err());
        }
    }
}
