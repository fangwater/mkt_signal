use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::Ordering;
use trade_engine::ltp_rest::LtpTransferRequest;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransferRequest {
    pub from_account_id: String,
    pub to_account_id: String,
    pub from_account_type: String,
    pub to_account_type: String,
    pub currency: String,
    pub amount: String,
    pub network: Option<String>,
    #[serde(default)]
    pub rapid_transfer: bool,
}

impl TransferRequest {
    pub fn validate(&self) -> Result<()> {
        for id in [&self.from_account_id, &self.to_account_id] {
            ensure!(
                !id.is_empty() && id.bytes().all(|c| c.is_ascii_digit()),
                "账户 ID 无效"
            );
            id.parse::<u64>().context("账户 ID 超出范围")?;
        }
        ensure!(
            self.from_account_id != self.to_account_id
                || self.from_account_type != self.to_account_type,
            "来源和目标不能是同一钱包"
        );
        for value in [
            &self.from_account_type,
            &self.to_account_type,
            &self.currency,
        ] {
            validate_token(value)?;
        }
        if let Some(network) = &self.network {
            validate_token(network)?;
        }
        ensure!(
            decimal_cmp(&self.amount, "0")? == Ordering::Greater,
            "金额必须大于零"
        );
        ensure!(
            !self.rapid_transfer || self.currency == "USDT",
            "快速划转仅支持 USDT"
        );
        Ok(())
    }

    pub fn ltp_request(&self, client_order_id: &str, funding: (bool, bool)) -> Result<Value> {
        let mut body = serde_json::to_value(LtpTransferRequest {
            from_trade_account_id: self.from_account_id.parse()?,
            to_trade_account_id: self.to_account_id.parse()?,
            from_account_type: self.from_account_type.clone(),
            to_account_type: self.to_account_type.clone(),
            currency: self.currency.clone(),
            amount: self.amount.clone(),
            network: self.network.clone(),
            rapid_transfer: self.rapid_transfer,
            client_order_id: client_order_id.into(),
            loan_trans: false,
        })?;
        // LTP Funding is an account ID, not an exchange FUNDING wallet enum.
        if funding.0 {
            body.as_object_mut().unwrap().remove("fromAccountType");
        }
        if funding.1 {
            body.as_object_mut().unwrap().remove("toAccountType");
        }
        Ok(body)
    }
}

pub fn validate_token(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 40
            && value
                .bytes()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || matches!(c, b'_' | b'-')),
        "币种、网络或钱包类型无效"
    );
    Ok(())
}

fn decimal_parts(value: &str) -> Result<(&str, &str)> {
    ensure!(!value.is_empty() && value.len() <= 80, "金额格式无效");
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or("");
    ensure!(
        !whole.is_empty()
            && whole.bytes().all(|c| c.is_ascii_digit())
            && fraction.bytes().all(|c| c.is_ascii_digit())
            && parts.next().is_none()
            && (!value.contains('.') || !fraction.is_empty()),
        "金额必须是普通十进制数字，不接受科学计数法"
    );
    Ok((
        whole.trim_start_matches('0'),
        fraction.trim_end_matches('0'),
    ))
}

/// Compare decimal strings without ever rounding through a floating point type.
pub fn decimal_cmp(left: &str, right: &str) -> Result<Ordering> {
    let (lw, lf) = decimal_parts(left)?;
    let (rw, rf) = decimal_parts(right)?;
    Ok(lw
        .len()
        .cmp(&rw.len())
        .then_with(|| lw.cmp(rw))
        .then_with(|| {
            let n = lf.len().max(rf.len());
            lf.bytes()
                .chain(std::iter::repeat(b'0'))
                .take(n)
                .cmp(rf.bytes().chain(std::iter::repeat(b'0')).take(n))
        }))
}

pub fn decimal_precision(value: &str) -> Result<usize> {
    Ok(decimal_parts(value)?.1.len())
}

pub fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub client_order_id: String,
    pub transfer_id: Option<String>,
    pub request: TransferRequest,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub actor: String,
    pub reason: Option<String>,
    pub actual_rapid_transfer: Option<bool>,
    pub amount_received: Option<String>,
    pub network_fee: Option<String>,
    pub fee_currency: Option<String>,
}

impl Record {
    pub fn unresolved(&self) -> bool {
        matches!(self.status.as_str(), "submitting" | "pending" | "unknown")
    }

    pub fn json(&self) -> Value {
        let mut value = serde_json::to_value(&self.request).expect("serialize transfer scalars");
        let object = value.as_object_mut().unwrap();
        for (key, item) in json!({
            "client_order_id": self.client_order_id, "transfer_id": self.transfer_id,
            "status": self.status, "created_at": self.created_at, "updated_at": self.updated_at,
            "actor": self.actor, "reason": self.reason,
            "requested_rapid_transfer": self.request.rapid_transfer,
            "rapid_transfer": self.actual_rapid_transfer,
            "amount_received": self.amount_received, "network_fee": self.network_fee,
            "fee_currency": self.fee_currency,
        })
        .as_object()
        .unwrap()
        {
            object.insert(key.clone(), item.clone());
        }
        value
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub venue: String,
    pub account_types: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Currency {
    pub currency: String,
    pub networks: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Catalog {
    pub accounts: Vec<Account>,
    pub currencies: Vec<Currency>,
    pub warnings: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

impl Catalog {
    pub fn account(&self, id: &str, account_type: &str) -> Result<&Account> {
        let account = self
            .accounts
            .iter()
            .find(|a| a.id == id)
            .context("账户不在当前密钥的可访问范围内")?;
        ensure!(
            account.account_types.iter().any(|t| t == account_type),
            "此账户不支持所选钱包类型"
        );
        Ok(account)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Balance {
    pub currency: String,
    pub total: String,
    pub available: String,
    pub network: Option<String>,
}
