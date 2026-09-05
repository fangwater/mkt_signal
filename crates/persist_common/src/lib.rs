pub mod order_queue_position;
pub mod unified_order;

use sha2::{Digest, Sha256};

pub use order_queue_position::{
    OrderQueuePositionAction, OrderQueuePositionMsg, OrderQueuePositionMsgType,
    OrderQueuePositionRecord, ORDER_QUEUE_POSITION_MAX_BYTES, ORDER_QUEUE_POSITION_MSG_BYTES,
};
pub use unified_order::{SignalBbo, SignalBboLeg, UnifiedOrderRecord, SIGNAL_BBO_BINARY_LEN};

pub const TRADE_UPDATE_RECORD_CHANNEL: &str = "trade_update_record";
pub const TRADE_UPDATE_UNMATCHED_RECORD_CHANNEL: &str = "trade_update_unmatched_record";
pub const ORDER_UPDATE_RECORD_CHANNEL: &str = "order_update_record";
pub const ORDER_UPDATE_UNMATCHED_RECORD_CHANNEL: &str = "order_update_unmatched_record";
pub const ORDER_QUEUE_POSITION_RECORD_CHANNEL: &str = "order_queue_position_record";
pub const ORDER_QUEUE_POSITION_RECORD_MAX_PUBLISHERS: usize = 1;
pub const UNIFORM_ORDER_RECORD_CHANNEL: &str = "uniform_order_record";
pub const HYPERLIQUID_ACCOUNT_FACT_RECORD_CHANNEL: &str = "hyperliquid_account_fact_record";
pub const HYPERLIQUID_ACCOUNT_FACT_MAX_BYTES: usize = 16 * 1024;
pub const HYPERLIQUID_ACCOUNT_FACT_ACK_CHANNEL: &str = "hyperliquid_account_fact_ack";
pub const HYPERLIQUID_ACCOUNT_FACT_ACK_MAX_BYTES: usize = 128;
pub const HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES: usize = 36;
const HYPERLIQUID_ACCOUNT_FACT_ACK_MAGIC: &[u8; 8] = b"HLFACTAK";
const HYPERLIQUID_ACCOUNT_FACT_ACK_USED_BYTES: usize = 8 + 32 + 8 + 8 + 36 + 32;
const HYPERLIQUID_ACCOUNT_FACT_DIGEST_DOMAIN: &[u8] = b"mkt_signal/hyperliquid_account_fact";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HyperliquidAccountFactAck {
    pub account_hash: [u8; 32],
    pub monitor_id: u64,
    pub fact_seq: u64,
    pub stable_key: [u8; HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES],
    pub value_digest: [u8; 32],
}

impl HyperliquidAccountFactAck {
    pub fn to_ipc_payload(self) -> [u8; HYPERLIQUID_ACCOUNT_FACT_ACK_MAX_BYTES] {
        let mut out = [0_u8; HYPERLIQUID_ACCOUNT_FACT_ACK_MAX_BYTES];
        out[..8].copy_from_slice(HYPERLIQUID_ACCOUNT_FACT_ACK_MAGIC);
        out[8..40].copy_from_slice(&self.account_hash);
        out[40..48].copy_from_slice(&self.monitor_id.to_le_bytes());
        out[48..56].copy_from_slice(&self.fact_seq.to_le_bytes());
        out[56..92].copy_from_slice(&self.stable_key);
        out[92..124].copy_from_slice(&self.value_digest);
        out
    }

    pub fn from_ipc_payload(payload: &[u8]) -> Result<Self, &'static str> {
        if payload.len() != HYPERLIQUID_ACCOUNT_FACT_ACK_MAX_BYTES {
            return Err("invalid Hyperliquid fact ACK payload length");
        }
        if &payload[..8] != HYPERLIQUID_ACCOUNT_FACT_ACK_MAGIC {
            return Err("invalid Hyperliquid fact ACK magic");
        }
        if payload[HYPERLIQUID_ACCOUNT_FACT_ACK_USED_BYTES..]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err("non-zero Hyperliquid fact ACK padding");
        }

        let mut account_hash = [0_u8; 32];
        account_hash.copy_from_slice(&payload[8..40]);
        let monitor_id = u64::from_le_bytes(
            payload[40..48]
                .try_into()
                .map_err(|_| "invalid Hyperliquid fact ACK monitor_id")?,
        );
        let fact_seq = u64::from_le_bytes(
            payload[48..56]
                .try_into()
                .map_err(|_| "invalid Hyperliquid fact ACK fact_seq")?,
        );
        if monitor_id == 0 || fact_seq == 0 {
            return Err("zero Hyperliquid fact ACK identity");
        }
        let mut stable_key = [0_u8; HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES];
        stable_key.copy_from_slice(&payload[56..92]);
        let mut value_digest = [0_u8; 32];
        value_digest.copy_from_slice(&payload[92..124]);
        Ok(Self {
            account_hash,
            monitor_id,
            fact_seq,
            stable_key,
            value_digest,
        })
    }
}

pub fn hyperliquid_account_fact_value_digest(
    stable_key: &[u8; HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES],
    value: &[u8],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(HYPERLIQUID_ACCOUNT_FACT_DIGEST_DOMAIN);
    digest.update(stable_key);
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hyperliquid_fact_ack_roundtrip_binds_key_and_value() {
        let stable_key = [3_u8; HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES];
        let value_digest = hyperliquid_account_fact_value_digest(&stable_key, b"fact-value");
        let ack = HyperliquidAccountFactAck {
            account_hash: [7; 32],
            monitor_id: 11,
            fact_seq: 12,
            stable_key,
            value_digest,
        };
        assert_eq!(
            HyperliquidAccountFactAck::from_ipc_payload(&ack.to_ipc_payload()).unwrap(),
            ack
        );
        assert_ne!(
            ack.value_digest,
            hyperliquid_account_fact_value_digest(&ack.stable_key, b"other-value")
        );
    }

    #[test]
    fn hyperliquid_fact_ack_rejects_zero_identity_and_nonzero_padding() {
        let mut payload = HyperliquidAccountFactAck {
            account_hash: [7; 32],
            monitor_id: 0,
            fact_seq: 1,
            stable_key: [0; HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES],
            value_digest: [0; 32],
        }
        .to_ipc_payload();
        assert!(HyperliquidAccountFactAck::from_ipc_payload(&payload).is_err());

        payload[40..48].copy_from_slice(&1_u64.to_le_bytes());
        payload[HYPERLIQUID_ACCOUNT_FACT_ACK_USED_BYTES] = 1;
        assert!(HyperliquidAccountFactAck::from_ipc_payload(&payload).is_err());
    }
}
