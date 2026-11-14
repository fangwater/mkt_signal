use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub const RECORD_CHANNEL: &str = "rapidx_execution_record";
pub const ACK_CHANNEL: &str = "rapidx_execution_ack";
pub const MAX_BYTES: usize = 16_384;
pub const ACK_BYTES: usize = 64;

const STABLE_KEY_DOMAIN: &[u8] = b"mkt_signal/rapidx_execution/stable_key";
const ACK_DOMAIN: &[u8] = b"mkt_signal/rapidx_execution/ack";

/// A factual execution observation. REST fees/rebates stay separate; a WS signed
/// fee is not added to its REST counterpart. Neither implies an order lifecycle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvidence {
    pub transaction_id: String,
    pub order_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub price: String,
    pub timestamp_ms: i64,
    pub realized_pnl: String,
    pub signed_fee: Option<String>,
    pub signed_fee_currency: Option<String>,
    pub fee: Option<String>,
    pub fee_currency: Option<String>,
    pub reported_rebate: Option<String>,
    pub rebate_currency: Option<String>,
    pub rest: bool,
}

impl ExecutionEvidence {
    pub fn parse(row: &Value, portfolio: &str, exchange: &str, rest: bool) -> Result<Self> {
        ensure!(
            string(row, "portfolioId")? == portfolio,
            "execution portfolio mismatch"
        );
        ensure!(
            string(row, "exchangeType")? == exchange,
            "execution venue mismatch"
        );
        let symbol = string(row, "sym")?;
        let business = string(row, "businessType")?;
        ensure!(
            matches!(business, "SPOT" | "MARGIN" | "PERP"),
            "unsupported execution business"
        );
        let parts: Vec<_> = symbol.split('_').collect();
        ensure!(
            parts.len() == 4
                && parts[0] == exchange
                && parts[1] == business
                && !parts[2].is_empty()
                && !parts[3].is_empty(),
            "execution symbol scope mismatch"
        );
        let side = string(row, "side")?;
        ensure!(matches!(side, "BUY" | "SELL"), "invalid execution side");
        let timestamp_ms = string(row, "createAt")?
            .parse::<i64>()
            .context("execution timestamp")?;
        ensure!(timestamp_ms > 0, "invalid execution timestamp");
        let quantity = decimal(row, "quantity")?;
        let price = decimal(row, "price")?;
        ensure!(
            quantity.parse::<f64>()? > 0.0 && price.parse::<f64>()? > 0.0,
            "nonpositive execution quantity/price"
        );
        let (signed_fee, signed_fee_currency, fee, fee_currency, reported_rebate, rebate_currency) =
            if rest {
                let fee = decimal(row, "fee")?;
                let rebate = decimal(row, "rebate")?;
                ensure!(
                    fee.parse::<f64>()? >= 0.0 && rebate.parse::<f64>()? >= 0.0,
                    "negative split fee/rebate"
                );
                (
                    None,
                    None,
                    Some(fee.clone()),
                    Some(currency(row, "feeCoin", &fee)?),
                    Some(rebate.clone()),
                    Some(currency(row, "rebateCoin", &rebate)?),
                )
            } else {
                let fee = decimal(row, "tradingFee")?;
                (
                    Some(fee.clone()),
                    Some(currency(row, "tradingFeeCoin", &fee)?),
                    None,
                    None,
                    None,
                    None,
                )
            };
        Ok(Self {
            transaction_id: string(row, "transactionId")?.into(),
            order_id: string(row, "orderId")?.into(),
            // External/manual orders can legitimately lack a numeric client ID.
            client_order_id: row
                .get("clientOrderId")
                .and_then(Value::as_str)
                .context("execution clientOrderId must be string")?
                .into(),
            symbol: symbol.into(),
            side: side.into(),
            quantity,
            price,
            timestamp_ms,
            realized_pnl: decimal(row, "rpnl")?,
            signed_fee,
            signed_fee_currency,
            fee,
            fee_currency,
            reported_rebate,
            rebate_currency,
            rest,
        })
    }
}

fn string<'a>(row: &'a Value, key: &str) -> Result<&'a str> {
    row.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .with_context(|| format!("execution missing {key}"))
}

fn decimal(row: &Value, key: &str) -> Result<String> {
    let value = string(row, key)?;
    ensure!(
        value
            .parse::<f64>()
            .with_context(|| format!("invalid {key}"))?
            .is_finite(),
        "non-finite {key}"
    );
    Ok(value.into())
}

fn currency(row: &Value, key: &str, amount: &str) -> Result<String> {
    let value = row
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("missing {key}"))?;
    ensure!(
        !value.is_empty() || amount.parse::<f64>()? == 0.0,
        "nonzero fee/rebate missing currency"
    );
    Ok(value.into())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub portfolio: String,
    pub exchange: String,
    pub execution: ExecutionEvidence,
}

impl ExecutionRecord {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.portfolio.is_empty()
                && self.portfolio.len() <= 64
                && self.portfolio.bytes().all(|byte| byte.is_ascii_digit()),
            "execution portfolio must be 1..64 ASCII digits"
        );
        ensure!(
            matches!(self.exchange.as_str(), "BINANCE" | "OKX"),
            "unsupported execution exchange"
        );
        ensure!(
            !self.execution.transaction_id.is_empty() && !self.execution.order_id.is_empty(),
            "execution transaction_id and order_id must be nonempty"
        );
        self.execution
            .timestamp_ms
            .checked_mul(1_000)
            .context("execution timestamp milliseconds overflow")?;
        self.validate_fee_branch()?;

        let wire = self.wire_value()?;
        let parsed =
            ExecutionEvidence::parse(&wire, &self.portfolio, &self.exchange, self.execution.rest)?;
        ensure!(
            parsed == self.execution,
            "execution evidence does not round-trip through RapidX wire fields"
        );
        Ok(())
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).context("serialize RapidX execution record")
    }

    pub fn from_ipc_payload(payload: &[u8]) -> Result<Self> {
        ensure!(
            payload.len() == MAX_BYTES,
            "invalid RapidX execution IPC payload length"
        );
        let json_len = u32::from_le_bytes(
            payload[..4]
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid RapidX execution IPC length prefix"))?,
        ) as usize;
        ensure!(
            json_len > 0 && json_len <= MAX_BYTES - 4,
            "invalid RapidX execution JSON length"
        );
        let json_end = 4 + json_len;
        ensure!(
            payload[json_end..].iter().all(|byte| *byte == 0),
            "non-zero RapidX execution IPC padding"
        );
        let record: Self = serde_json::from_slice(&payload[4..json_end])
            .context("decode RapidX execution IPC JSON")?;
        record.validate()?;
        Ok(record)
    }

    pub fn to_ipc_payload(&self) -> Result<[u8; MAX_BYTES]> {
        let json = self.to_json_bytes()?;
        ensure!(
            json.len() <= MAX_BYTES - 4,
            "RapidX execution JSON exceeds IPC payload capacity"
        );
        let json_len =
            u32::try_from(json.len()).context("RapidX execution JSON length overflow")?;
        let mut payload = [0_u8; MAX_BYTES];
        payload[..4].copy_from_slice(&json_len.to_le_bytes());
        payload[4..4 + json.len()].copy_from_slice(&json);
        Ok(payload)
    }

    pub fn stable_key(&self) -> Result<[u8; 32]> {
        self.validate()?;
        let mut digest = Sha256::new();
        digest.update(STABLE_KEY_DOMAIN);
        update_length_prefixed(&mut digest, self.portfolio.as_bytes());
        update_length_prefixed(&mut digest, self.exchange.as_bytes());
        update_length_prefixed(&mut digest, self.execution.transaction_id.as_bytes());
        digest.update([u8::from(self.execution.rest)]);
        Ok(digest.finalize().into())
    }

    pub fn ack(&self) -> Result<[u8; ACK_BYTES]> {
        let stable_key = self.stable_key()?;
        let json = self.to_json_bytes()?;
        let mut digest = Sha256::new();
        digest.update(ACK_DOMAIN);
        digest.update(stable_key);
        update_length_prefixed(&mut digest, &json);
        let value_digest: [u8; 32] = digest.finalize().into();
        let mut ack = [0_u8; ACK_BYTES];
        ack[..32].copy_from_slice(&stable_key);
        ack[32..].copy_from_slice(&value_digest);
        Ok(ack)
    }

    fn validate_fee_branch(&self) -> Result<()> {
        if self.execution.rest {
            ensure!(
                self.execution.signed_fee.is_none() && self.execution.signed_fee_currency.is_none(),
                "REST execution must not contain WS signed fee fields"
            );
            let fee = self
                .execution
                .fee
                .as_deref()
                .context("REST execution missing fee")?;
            let fee_currency = self
                .execution
                .fee_currency
                .as_deref()
                .context("REST execution missing fee currency")?;
            let rebate = self
                .execution
                .reported_rebate
                .as_deref()
                .context("REST execution missing rebate")?;
            let rebate_currency = self
                .execution
                .rebate_currency
                .as_deref()
                .context("REST execution missing rebate currency")?;
            validate_currency(fee_currency, fee, "fee currency")?;
            validate_currency(rebate_currency, rebate, "rebate currency")?;
        } else {
            ensure!(
                self.execution.fee.is_none()
                    && self.execution.fee_currency.is_none()
                    && self.execution.reported_rebate.is_none()
                    && self.execution.rebate_currency.is_none(),
                "WS execution must not contain REST fee/rebate fields"
            );
            let signed_fee = self
                .execution
                .signed_fee
                .as_deref()
                .context("WS execution missing signed fee")?;
            let signed_fee_currency = self
                .execution
                .signed_fee_currency
                .as_deref()
                .context("WS execution missing signed fee currency")?;
            validate_currency(signed_fee_currency, signed_fee, "signed fee currency")?;
        }
        Ok(())
    }

    fn wire_value(&self) -> Result<Value> {
        let business = self
            .execution
            .symbol
            .split('_')
            .nth(1)
            .context("execution symbol missing business type")?;
        let mut row = json!({
            "portfolioId": self.portfolio,
            "exchangeType": self.exchange,
            "businessType": business,
            "sym": self.execution.symbol,
            "transactionId": self.execution.transaction_id,
            "orderId": self.execution.order_id,
            "clientOrderId": self.execution.client_order_id,
            "side": self.execution.side,
            "quantity": self.execution.quantity,
            "price": self.execution.price,
            "createAt": self.execution.timestamp_ms.to_string(),
            "rpnl": self.execution.realized_pnl,
        });
        let object = row
            .as_object_mut()
            .context("construct RapidX execution wire object")?;
        if self.execution.rest {
            object.insert(
                "fee".into(),
                Value::String(required_option(&self.execution.fee, "fee")?),
            );
            object.insert(
                "feeCoin".into(),
                Value::String(required_option(
                    &self.execution.fee_currency,
                    "fee_currency",
                )?),
            );
            object.insert(
                "rebate".into(),
                Value::String(required_option(
                    &self.execution.reported_rebate,
                    "reported_rebate",
                )?),
            );
            object.insert(
                "rebateCoin".into(),
                Value::String(required_option(
                    &self.execution.rebate_currency,
                    "rebate_currency",
                )?),
            );
        } else {
            object.insert(
                "tradingFee".into(),
                Value::String(required_option(&self.execution.signed_fee, "signed_fee")?),
            );
            object.insert(
                "tradingFeeCoin".into(),
                Value::String(required_option(
                    &self.execution.signed_fee_currency,
                    "signed_fee_currency",
                )?),
            );
        }
        Ok(row)
    }
}

fn required_option(value: &Option<String>, name: &str) -> Result<String> {
    value
        .clone()
        .with_context(|| format!("execution missing {name}"))
}

fn validate_currency(currency: &str, amount: &str, field: &str) -> Result<()> {
    let amount = amount
        .parse::<f64>()
        .with_context(|| format!("invalid {field} amount"))?;
    ensure!(amount.is_finite(), "non-finite {field} amount");
    if currency.is_empty() {
        ensure!(amount == 0.0, "nonzero {field} is missing currency");
        return Ok(());
    }
    ensure!(
        currency.len() <= 16
            && currency
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
        "invalid {field}"
    );
    Ok(())
}

fn update_length_prefixed(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(rest: bool) -> Value {
        json!({
            "portfolioId":"123",
            "exchangeType":"OKX",
            "businessType":"SPOT",
            "sym":"OKX_SPOT_BTC_USDT",
            "transactionId":"external_fill",
            "orderId":"external_order",
            "clientOrderId":"manual",
            "side":"BUY",
            "quantity":"0.01",
            "price":"50000",
            "createAt":"1000",
            "rpnl":"0",
            "fee":"0.00001",
            "feeCoin":"BTC",
            "rebate":"0.02",
            "rebateCoin":"USDT",
            "tradingFee":"-0.02",
            "tradingFeeCoin":"USDT",
            "rest": rest,
        })
    }

    fn record(rest: bool) -> ExecutionRecord {
        let row = trade(rest);
        ExecutionRecord {
            portfolio: "123".into(),
            exchange: "OKX".into(),
            execution: ExecutionEvidence::parse(&row, "123", "OKX", rest).unwrap(),
        }
    }

    #[test]
    fn ipc_roundtrip_is_fixed_size_and_validated() {
        let record = record(true);
        let payload = record.to_ipc_payload().unwrap();
        assert_eq!(payload.len(), MAX_BYTES);
        assert_eq!(ExecutionRecord::from_ipc_payload(&payload).unwrap(), record);
    }

    #[test]
    fn ipc_rejects_malformed_length_and_padding() {
        assert!(ExecutionRecord::from_ipc_payload(&[0; 1]).is_err());
        let mut payload = record(true).to_ipc_payload().unwrap();
        payload[..4].copy_from_slice(&((MAX_BYTES - 3) as u32).to_le_bytes());
        assert!(ExecutionRecord::from_ipc_payload(&payload).is_err());

        let mut payload = record(true).to_ipc_payload().unwrap();
        let len = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
        payload[4 + len] = 1;
        assert!(ExecutionRecord::from_ipc_payload(&payload).is_err());
    }

    #[test]
    fn validation_enforces_scope_positive_amounts_and_currency() {
        let mut invalid_scope = record(true);
        invalid_scope.portfolio = "not-numeric".into();
        assert!(invalid_scope.validate().is_err());

        let mut invalid_quantity = record(true);
        invalid_quantity.execution.quantity = "0".into();
        assert!(invalid_quantity.validate().is_err());

        let mut invalid_currency = record(true);
        invalid_currency.execution.fee_currency = Some("btc".into());
        assert!(invalid_currency.validate().is_err());
    }

    #[test]
    fn ws_and_rest_have_distinct_keys_and_branch_validation() {
        let rest = record(true);
        let ws = record(false);
        assert_ne!(rest.stable_key().unwrap(), ws.stable_key().unwrap());

        let mut mixed = record(false);
        mixed.execution.fee = Some("0".into());
        assert!(mixed.validate().is_err());
    }

    #[test]
    fn stable_key_is_scope_stable_while_ack_binds_value() {
        let original = record(true);
        let mut changed = original.clone();
        changed.execution.price = "50001".into();
        assert_eq!(
            original.stable_key().unwrap(),
            changed.stable_key().unwrap()
        );
        assert_ne!(original.ack().unwrap(), changed.ack().unwrap());
        assert_eq!(original.ack().unwrap(), original.ack().unwrap());
    }
}
