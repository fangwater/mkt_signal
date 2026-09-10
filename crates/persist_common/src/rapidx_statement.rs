use crate::rapidx_fact::{RapidXFact, ACK_BYTES, MAX_BYTES};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Settlement ledger evidence, never a trade fill or an invented balance delta.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatementRecord {
    pub portfolio: String,
    pub exchange: String,
    pub statement_id: String,
    pub request_id: String,
    pub coin: String,
    pub symbol: String,
    pub statement_type: String,
    pub business_type: String,
    pub before_available: String,
    pub after_available: String,
    pub before_overdraw: String,
    pub after_overdraw: String,
    pub before_borrow: String,
    pub after_borrow: String,
    pub delta_amount: String,
    pub timestamp_us: i64,
}

impl StatementRecord {
    pub fn parse(row: &Value, portfolio: &str, exchange: &str) -> Result<Self> {
        let text = |key: &str| -> Result<String> {
            row[key]
                .as_str()
                .map(str::to_owned)
                .with_context(|| format!("statement missing {key}"))
        };
        let reported_portfolio = row["portfolioId"]
            .as_u64()
            .context("statement portfolioId must be integer")?
            .to_string();
        ensure!(
            reported_portfolio == portfolio && row["exchangeType"].as_str() == Some(exchange),
            "statement account scope mismatch"
        );
        let record = Self {
            portfolio: reported_portfolio,
            exchange: text("exchangeType")?,
            statement_id: text("statementId")?,
            request_id: text("requestId")?,
            coin: text("coin")?,
            symbol: text("sym")?,
            statement_type: text("statementType")?,
            business_type: text("businessType")?,
            before_available: text("beforeAvailable")?,
            after_available: text("afterAvailable")?,
            before_overdraw: text("beforeOverdraw")?,
            after_overdraw: text("afterOverdraw")?,
            before_borrow: text("beforeBorrow")?,
            after_borrow: text("afterBorrow")?,
            delta_amount: text("deltaAmount")?,
            timestamp_us: row["createAt"]
                .as_i64()
                .and_then(|t| t.checked_mul(1_000))
                .context("invalid statement createAt milliseconds")?,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.portfolio.is_empty()
                && self.portfolio.len() <= 64
                && self.portfolio.bytes().all(|b| b.is_ascii_digit()),
            "invalid statement portfolio"
        );
        ensure!(
            matches!(self.exchange.as_str(), "BINANCE" | "OKX"),
            "unsupported statement exchange"
        );
        ensure!(
            !self.statement_id.is_empty() && !self.coin.is_empty(),
            "missing statement identity/currency"
        );
        ensure!(
            self.timestamp_us > 0 && self.timestamp_us % 1_000 == 0,
            "invalid statement timestamp"
        );
        ensure!(
            matches!(
                self.statement_type.as_str(),
                "FUNDING_FEE"
                    | "DEDUCT_INTEREST"
                    | "LIQUIDATION_FEE"
                    | "LIQ_COMPENSATION"
                    | "TRANSFER"
            ),
            "unknown statement type"
        );
        ensure!(
            matches!(
                self.business_type.as_str(),
                "SPOT" | "MARGIN" | "PERP" | "UNI"
            ),
            "unknown statement business type"
        );
        if !self.symbol.is_empty() {
            let parts: Vec<_> = self.symbol.split('_').collect();
            ensure!(
                parts.len() == 4
                    && parts[0] == self.exchange
                    && parts[1] == self.business_type
                    && !parts[2].is_empty()
                    && !parts[3].is_empty(),
                "statement symbol scope mismatch"
            );
        }
        for value in [
            &self.before_available,
            &self.after_available,
            &self.before_overdraw,
            &self.after_overdraw,
            &self.before_borrow,
            &self.after_borrow,
            &self.delta_amount,
        ] {
            ensure!(
                !value.is_empty() && value.len() <= 256 && value.parse::<f64>()?.is_finite(),
                "invalid statement decimal"
            );
        }
        Ok(())
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        RapidXFact::to_json_bytes(self)
    }
    pub fn to_ipc_payload(&self) -> Result<[u8; MAX_BYTES]> {
        RapidXFact::to_ipc_payload(self)
    }
    pub fn stable_key(&self) -> Result<[u8; 32]> {
        RapidXFact::stable_key(self)
    }
    pub fn ack(&self) -> Result<[u8; ACK_BYTES]> {
        RapidXFact::ack(self)
    }
}

impl RapidXFact for StatementRecord {
    const RECORD_CHANNEL: &'static str = "rapidx_statement_record";
    const ACK_CHANNEL: &'static str = "rapidx_statement_ack";
    const COLUMN_FAMILY: &'static str = "rapidx_statements";
    fn validate(&self) -> Result<()> {
        self.validate()
    }
    fn stable_key(&self) -> Result<[u8; 32]> {
        self.validate()?;
        let mut hash = Sha256::new();
        hash.update(b"mkt_signal/rapidx_statement");
        for value in [&self.portfolio, &self.exchange, &self.statement_id] {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        Ok(hash.finalize().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn row() -> Value {
        json!({"portfolioId":123,"exchangeType":"OKX","statementId":"s1","requestId":"r1",
            "coin":"ETH","sym":"","statementType":"DEDUCT_INTEREST","businessType":"SPOT",
            "beforeAvailable":"0","afterAvailable":"0","beforeOverdraw":"0.000000017314874246",
            "afterOverdraw":"0.000000023086525446","beforeBorrow":"0","afterBorrow":"0",
            "deltaAmount":"0","createAt":1754398800000_i64})
    }
    #[test]
    fn interest_debt_is_preserved_even_with_zero_settlement() {
        let record = StatementRecord::parse(&row(), "123", "OKX").unwrap();
        assert_eq!(record.delta_amount, "0");
        assert_ne!(record.before_overdraw, record.after_overdraw);
        assert_eq!(record.timestamp_us, 1754398800000000);
        assert_eq!(
            StatementRecord::from_ipc_payload(&record.to_ipc_payload().unwrap()).unwrap(),
            record
        );
        let mut changed = record.clone();
        changed.after_overdraw = "1".into();
        assert_eq!(record.stable_key().unwrap(), changed.stable_key().unwrap());
        assert_ne!(record.ack().unwrap(), changed.ack().unwrap());
    }

    #[test]
    fn transfer_statement_is_accepted() {
        let mut transfer = row();
        transfer["exchangeType"] = json!("BINANCE");
        transfer["coin"] = json!("USDT");
        transfer["statementType"] = json!("TRANSFER");
        let record = StatementRecord::parse(&transfer, "123", "BINANCE").unwrap();
        assert_eq!(record.statement_type, "TRANSFER");
        assert_eq!(record.business_type, "SPOT");
    }
    #[test]
    fn invalid_scope_type_decimal_and_padding_are_rejected() {
        assert!(StatementRecord::parse(&row(), "456", "OKX").is_err());
        let mut invalid = row();
        invalid["statementType"] = json!("UNKNOWN");
        assert!(StatementRecord::parse(&invalid, "123", "OKX").is_err());
        invalid = row();
        invalid["afterBorrow"] = json!("NaN");
        assert!(StatementRecord::parse(&invalid, "123", "OKX").is_err());
        let record = StatementRecord::parse(&row(), "123", "OKX").unwrap();
        let mut payload = record.to_ipc_payload().unwrap();
        payload[MAX_BYTES - 1] = 1;
        assert!(StatementRecord::from_ipc_payload(&payload).is_err());
    }
}
