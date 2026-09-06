use anyhow::{ensure, Result};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};

pub use crate::rapidx_execution::{ACK_BYTES, MAX_BYTES};

/// Shared delivery mechanics for distinct execution and account-ledger facts.
pub trait RapidXFact: Serialize + DeserializeOwned {
    const RECORD_CHANNEL: &'static str;
    const ACK_CHANNEL: &'static str;
    const COLUMN_FAMILY: &'static str;
    fn validate(&self) -> Result<()>;
    fn stable_key(&self) -> Result<[u8; 32]>;

    fn to_json_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    fn to_ipc_payload(&self) -> Result<[u8; MAX_BYTES]> {
        let json = self.to_json_bytes()?;
        ensure!(
            json.len() <= MAX_BYTES - 4,
            "RapidX fact exceeds IPC capacity"
        );
        let mut payload = [0; MAX_BYTES];
        payload[..4].copy_from_slice(&(json.len() as u32).to_le_bytes());
        payload[4..4 + json.len()].copy_from_slice(&json);
        Ok(payload)
    }

    fn from_ipc_payload(payload: &[u8]) -> Result<Self> {
        ensure!(
            payload.len() == MAX_BYTES,
            "invalid RapidX fact payload size"
        );
        let length = u32::from_le_bytes(payload[..4].try_into()?) as usize;
        ensure!(
            length > 0 && length <= MAX_BYTES - 4,
            "invalid RapidX fact JSON length"
        );
        ensure!(
            payload[4 + length..].iter().all(|byte| *byte == 0),
            "nonzero RapidX fact padding"
        );
        let record: Self = serde_json::from_slice(&payload[4..4 + length])?;
        record.validate()?;
        Ok(record)
    }

    fn ack(&self) -> Result<[u8; ACK_BYTES]> {
        let key = self.stable_key()?;
        let json = self.to_json_bytes()?;
        let mut hash = Sha256::new();
        hash.update(b"mkt_signal/rapidx_fact/ack");
        hash.update(key);
        hash.update((json.len() as u64).to_be_bytes());
        hash.update(json);
        let mut ack = [0; ACK_BYTES];
        ack[..32].copy_from_slice(&key);
        ack[32..].copy_from_slice(&hash.finalize());
        Ok(ack)
    }
}

// Preserve the existing execution contract byte-for-byte while sharing queues.
impl RapidXFact for crate::rapidx_execution::ExecutionRecord {
    const RECORD_CHANNEL: &'static str = crate::rapidx_execution::RECORD_CHANNEL;
    const ACK_CHANNEL: &'static str = crate::rapidx_execution::ACK_CHANNEL;
    const COLUMN_FAMILY: &'static str = "rapidx_executions";
    fn validate(&self) -> Result<()> {
        self.validate()
    }
    fn stable_key(&self) -> Result<[u8; 32]> {
        self.stable_key()
    }
    fn ack(&self) -> Result<[u8; ACK_BYTES]> {
        self.ack()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rapidx_execution::{ExecutionEvidence, ExecutionRecord};
    #[test]
    fn shared_transport_preserves_execution_bytes_and_ack() {
        let execution = ExecutionEvidence::parse(&serde_json::json!({"portfolioId":"123","exchangeType":"OKX",
            "businessType":"SPOT","sym":"OKX_SPOT_BTC_USDT","transactionId":"t","orderId":"o","clientOrderId":"",
            "quantity":"0.1","price":"10","side":"BUY","createAt":"1000","rpnl":"0",
            "tradingFee":"-0.01","tradingFeeCoin":"USDT"}), "123", "OKX", false).unwrap();
        let record = ExecutionRecord {
            portfolio: "123".into(),
            exchange: "OKX".into(),
            execution,
        };
        assert_eq!(
            RapidXFact::to_ipc_payload(&record).unwrap(),
            record.to_ipc_payload().unwrap()
        );
        assert_eq!(RapidXFact::ack(&record).unwrap(), record.ack().unwrap());
        assert_eq!(
            <ExecutionRecord as RapidXFact>::from_ipc_payload(&record.to_ipc_payload().unwrap())
                .unwrap(),
            record
        );
    }
}
