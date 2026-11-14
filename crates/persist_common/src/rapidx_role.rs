//! Immutable liquidity evidence, separate from the existing fee observation.
use crate::{rapidx_execution::ExecutionRecord, rapidx_fact::RapidXFact};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ExecutionRole {
    Maker,
    Taker,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRoleRecord {
    pub record: ExecutionRecord,
    pub role: ExecutionRole,
}

impl ExecutionRoleRecord {
    pub fn from_row(record: ExecutionRecord, row: &serde_json::Value) -> Result<Option<Self>> {
        let role = match row.get("execType").and_then(serde_json::Value::as_str) {
            None | Some("") => return Ok(None),
            Some("MAKER") => ExecutionRole::Maker,
            Some("TAKER") => ExecutionRole::Taker,
            Some(_) => return Ok(None),
        };
        let role = Self { record, role };
        role.validate()?;
        Ok(Some(role))
    }
}

impl RapidXFact for ExecutionRoleRecord {
    const RECORD_CHANNEL: &'static str = "rapidx_execution_role_record";
    const ACK_CHANNEL: &'static str = "rapidx_execution_role_ack";
    const COLUMN_FAMILY: &'static str = "rapidx_execution_roles";
    fn validate(&self) -> Result<()> {
        self.record.validate()?;
        ensure!(
            !self.record.execution.transaction_id.is_empty(),
            "missing role identity"
        );
        Ok(())
    }
    fn stable_key(&self) -> Result<[u8; 32]> {
        self.record.stable_key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rapidx_execution::ExecutionEvidence;

    #[test]
    fn role_ack_binds_content_without_changing_execution_contract() {
        let row = serde_json::json!({"portfolioId":"123","exchangeType":"BINANCE",
            "businessType":"PERP","sym":"BINANCE_PERP_BTC_USDT","transactionId":"tx",
            "orderId":"o","clientOrderId":"7","side":"BUY","quantity":"1",
            "price":"2","createAt":"1000","rpnl":"0","tradingFee":"0",
            "tradingFeeCoin":"","execType":"MAKER"});
        let record = ExecutionRecord {
            portfolio: "123".into(),
            exchange: "BINANCE".into(),
            execution: ExecutionEvidence::parse(&row, "123", "BINANCE", false).unwrap(),
        };
        let role = ExecutionRoleRecord::from_row(record.clone(), &row)
            .unwrap()
            .unwrap();
        assert_eq!(role.stable_key().unwrap(), record.stable_key().unwrap());
        assert_eq!(
            ExecutionRoleRecord::from_ipc_payload(&role.to_ipc_payload().unwrap()).unwrap(),
            role
        );
        let mut changed = role.clone();
        changed.role = ExecutionRole::Taker;
        assert_ne!(role.ack().unwrap(), changed.ack().unwrap());
        assert_ne!(role.ack().unwrap(), record.ack().unwrap());
    }
}
