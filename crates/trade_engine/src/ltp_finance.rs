use anyhow::{anyhow, Result};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, Serialize, Default)]
pub struct PortfolioFinancialSnapshot {
    pub portfolio: String,
    pub exchange: String,
    pub observed_ms: Option<i64>,
    pub total_equity: Option<String>,
    pub net_equity: Option<String>,
    pub maintenance_margin: Option<String>,
    pub frozen_margin: Option<String>,
    pub total_loan_value: Option<String>,
    pub loan_margin: Option<String>,
    pub loan_maintenance_margin: Option<String>,
    pub loan_to_value: Option<String>,
    pub loan_status: Option<String>,
    pub source_observed_ms: BTreeMap<String, i64>,
    pub gross_margin: Option<String>,
    pub net_margin: Option<String>,
    pub valid_margin: Option<String>,
    pub net_valid_margin: Option<String>,
    pub available_margin: Option<String>,
    pub net_available_margin: Option<String>,
    pub perp_available_margin: Option<String>,
    pub debts: BTreeMap<String, String>,
    pub loans: BTreeMap<String, String>,
    pub max_borrow_capacity: BTreeMap<String, String>,
    pub assets: BTreeMap<String, FinancialAsset>,
    pub raw_account: Option<Value>,
    pub raw_margin_call: Option<Value>,
    pub raw_loan_info: Option<Value>,
    pub raw_loan_capacity: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FinancialAsset {
    pub gross_balance: Option<String>,
    pub gross_equity: Option<String>,
    pub net_equity: Option<String>,
    pub margin_value: Option<String>,
    pub net_margin_value: Option<String>,
    pub frozen: Option<String>,
    pub borrowed: Option<String>,
    pub available: Option<String>,
    pub debt: Option<String>,
    pub loan: Option<String>,
    pub raw: Value,
}

impl PortfolioFinancialSnapshot {
    pub fn new(portfolio: String, exchange: String) -> Result<Self> {
        validate_identity(&portfolio, "portfolio")?;
        validate_exchange(&exchange)?;
        Ok(Self {
            portfolio,
            exchange,
            ..Self::default()
        })
    }

    pub fn apply_account_push(
        &mut self,
        payload: &str,
        complete_assets: bool,
        observed_ms: i64,
    ) -> Result<()> {
        if observed_ms < 0 {
            return Err(anyhow!("negative observed timestamp"));
        }
        let root: Value = serde_json::from_str(payload)?;
        let channel = root.get("channel").and_then(Value::as_str).unwrap_or("");
        if !matches!(channel, "Assets" | "Accounts" | "MarginCall") {
            return Ok(());
        }
        let data = root
            .get("data")
            .ok_or_else(|| anyhow!("{channel} missing data"))?;
        let rows = rows(data)?
            .into_iter()
            .filter(|row| {
                row.get("exchangeType")
                    .and_then(Value::as_str)
                    .is_none_or(|value| value.eq_ignore_ascii_case(&self.exchange))
            })
            .collect::<Vec<_>>();
        if rows.is_empty() && (channel != "Assets" || !complete_assets) {
            return Ok(());
        }
        let mut next = self.clone();
        match channel {
            "Assets" => {
                let mut assets = next.assets.clone();
                let mut seen = HashSet::new();
                for row in rows {
                    if !required_string(row, "exchangeType")?.eq_ignore_ascii_case(&self.exchange) {
                        continue;
                    }
                    scope(row, &self.portfolio, &self.exchange)?;
                    let coin = required_string(row, "coin")?.to_ascii_uppercase();
                    if !seen.insert(coin.clone()) {
                        return Err(anyhow!("duplicate asset coin"));
                    }
                    if assets.get(&coin).is_some_and(|old| {
                        timestamp(&old.raw) > timestamp(&Value::Object(row.clone()))
                    }) {
                        continue;
                    }
                    let gross = optional_decimal(row, "balance")?;
                    let equity = optional_decimal(row, "netEquity")?;
                    let available = optional_decimal(row, "available")?;
                    let debt = optional_decimal(row, "debt")?;
                    let loan = optional_decimal(row, "loan")?;
                    assets.insert(
                        coin,
                        FinancialAsset {
                            gross_balance: gross,
                            gross_equity: optional_decimal(row, "equity")?,
                            net_equity: equity,
                            margin_value: optional_decimal(row, "marginValue")?,
                            net_margin_value: optional_decimal(row, "netMarginValue")?,
                            frozen: optional_decimal(row, "frozen")?,
                            borrowed: optional_decimal(row, "borrow")?,
                            available,
                            debt,
                            loan,
                            raw: Value::Object(row.clone()),
                        },
                    );
                }
                if complete_assets {
                    assets.retain(|coin, asset| {
                        seen.contains(coin) || timestamp(&asset.raw) > observed_ms
                    });
                }
                next.debts = assets
                    .iter()
                    .filter_map(|(coin, asset)| asset.debt.clone().map(|debt| (coin.clone(), debt)))
                    .collect();
                next.loans = assets
                    .iter()
                    .filter_map(|(coin, asset)| asset.loan.clone().map(|loan| (coin.clone(), loan)))
                    .collect();
                next.assets = assets;
            }
            "Accounts" => {
                let row = exactly_one(rows, channel)?;
                scope(row, &self.portfolio, &self.exchange)?;
                if next
                    .raw_account
                    .as_ref()
                    .is_some_and(|old| timestamp(old) > timestamp(&Value::Object(row.clone())))
                {
                    return Ok(());
                }
                next.total_equity = optional_decimal(row, "equity")?;
                next.net_equity = optional_decimal(row, "netEquity")?;
                next.maintenance_margin = optional_decimal(row, "maintainMargin")?;
                next.frozen_margin = optional_decimal(row, "frozenMargin")?;
                next.total_loan_value = optional_decimal(row, "totalLoanValue")?;
                next.gross_margin = optional_decimal(row, "marginValue")?;
                next.net_margin = optional_decimal(row, "netMarginValue")?;
                next.valid_margin = optional_decimal(row, "validMargin")?;
                next.net_valid_margin = optional_decimal(row, "netValidMargin")?;
                next.available_margin = optional_decimal(row, "availableMargin")?;
                next.net_available_margin = optional_decimal(row, "netAvailableMargin")?;
                next.perp_available_margin = optional_decimal(row, "perpAvailableMargin")?;
                next.raw_account = Some(Value::Object(row.clone()));
            }
            "MarginCall" => {
                let row = exactly_one(rows, channel)?;
                scope(row, &self.portfolio, &self.exchange)?;
                next.raw_margin_call = Some(Value::Object(row.clone()));
            }
            _ => unreachable!(),
        }
        next.observed_ms = Some(observed_ms);
        next.source_observed_ms.insert(channel.into(), observed_ms);
        *self = next;
        Ok(())
    }

    pub fn apply_loan_info(&mut self, response: &Value, observed_ms: i64) -> Result<()> {
        if observed_ms < 0 {
            return Err(anyhow!("negative observed timestamp"));
        }
        let data = success_data(response)?
            .as_object()
            .ok_or_else(|| anyhow!("loan info data must be object"))?;
        portfolio_scope(data, &self.portfolio)?;
        let accounts = rows(
            data.get("accounts")
                .ok_or_else(|| anyhow!("loan info missing accounts"))?,
        )?;
        let selected = accounts
            .into_iter()
            .filter(|row| {
                row.get("exchange")
                    .and_then(Value::as_str)
                    .is_some_and(|v| v.eq_ignore_ascii_case(&self.exchange))
            })
            .collect::<Vec<_>>();
        let account = exactly_one(selected, "loan info exchange")?;
        let coins = rows(
            account
                .get("coins")
                .ok_or_else(|| anyhow!("loan account missing coins"))?,
        )?;
        let mut next = self.clone();
        let mut loans = BTreeMap::new();
        for row in coins {
            let coin = required_string(row, "coin")?.to_ascii_uppercase();
            let value =
                optional_decimal(row, "loan")?.ok_or_else(|| anyhow!("loan info missing loan"))?;
            if value.parse::<f64>()? < 0.0 {
                return Err(anyhow!("negative loan"));
            }
            if loans.insert(coin, value).is_some() {
                return Err(anyhow!("duplicate loan coin"));
            }
        }
        next.net_margin = optional_decimal(account, "netMarginValue")?;
        next.net_valid_margin = optional_decimal(account, "netValidMargin")?;
        next.net_available_margin = optional_decimal(account, "netAvailableMargin")?;
        next.loan_margin = optional_decimal(account, "loanMargin")?;
        next.loan_maintenance_margin = optional_decimal(account, "loanMaintenanceMargin")?;
        next.loan_to_value = optional_decimal(account, "ltv")?;
        next.loan_status = account
            .get("accountStatus")
            .and_then(Value::as_str)
            .map(str::to_string);
        next.loans = loans;
        next.raw_loan_info = Some(response.clone());
        next.observed_ms = Some(observed_ms);
        next.source_observed_ms
            .insert("LoanInfo".into(), observed_ms);
        *self = next;
        Ok(())
    }

    pub fn apply_loan_capacity(&mut self, response: &Value, observed_ms: i64) -> Result<()> {
        if observed_ms < 0 {
            return Err(anyhow!("negative observed timestamp"));
        }
        let data = success_data(response)?;
        let rows = rows(data)?;
        let mut next = self.clone();
        let mut capacity = BTreeMap::new();
        for row in rows {
            if !required_string(row, "exchange")?.eq_ignore_ascii_case(&self.exchange) {
                return Err(anyhow!("exchange mismatch"));
            }
            let coin = required_string(row, "coin")?.to_ascii_uppercase();
            let value = optional_decimal(row, "portfolioMaxLoanCoin")?
                .ok_or_else(|| anyhow!("max loan row missing capacity"))?;
            if value.parse::<f64>()? < 0.0 {
                return Err(anyhow!("negative loan capacity"));
            }
            if capacity.insert(coin, value).is_some() {
                return Err(anyhow!("duplicate max loan coin"));
            }
        }
        next.max_borrow_capacity = capacity;
        next.raw_loan_capacity = Some(response.clone());
        next.observed_ms = Some(observed_ms);
        next.source_observed_ms
            .insert("LoanCapacity".into(), observed_ms);
        *self = next;
        Ok(())
    }
}

fn validate_identity(value: &str, name: &str) -> Result<()> {
    if value.is_empty() || !value.is_ascii() {
        Err(anyhow!("invalid {name}"))
    } else {
        Ok(())
    }
}

fn timestamp(row: &Value) -> i64 {
    row.get("updateAt")
        .and_then(Value::as_str)
        .and_then(|text| text.parse().ok())
        .unwrap_or(0)
}
fn validate_exchange(value: &str) -> Result<()> {
    if matches!(value, "BINANCE" | "OKX") {
        Ok(())
    } else {
        Err(anyhow!("unsupported exchange"))
    }
}
fn rows(value: &Value) -> Result<Vec<&Map<String, Value>>> {
    match value {
        Value::Array(v) => v
            .iter()
            .map(|v| v.as_object().ok_or_else(|| anyhow!("row must be object")))
            .collect(),
        Value::Object(v) => Ok(vec![v]),
        _ => Err(anyhow!("data must be object or array")),
    }
}
fn exactly_one<'a>(
    rows: Vec<&'a Map<String, Value>>,
    channel: &str,
) -> Result<&'a Map<String, Value>> {
    if rows.len() == 1 {
        Ok(rows[0])
    } else {
        Err(anyhow!("{channel} must contain one row"))
    }
}
fn required_string<'a>(row: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    row.get(key)
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("missing string {key}"))
}
fn optional_decimal(row: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    let Some(value) = row.get(key) else {
        return Ok(None);
    };
    let text = value
        .as_str()
        .ok_or_else(|| anyhow!("{key} must be string"))?;
    let number = text
        .parse::<f64>()
        .map_err(|_| anyhow!("{key} invalid decimal"))?;
    if !number.is_finite() {
        return Err(anyhow!("{key} non-finite"));
    };
    Ok(Some(text.to_string()))
}
fn scope(row: &Map<String, Value>, portfolio: &str, exchange: &str) -> Result<()> {
    portfolio_scope(row, portfolio)?;
    scope_exchange(row, exchange)
}
fn portfolio_scope(row: &Map<String, Value>, portfolio: &str) -> Result<()> {
    let value = row
        .get("portfolioId")
        .ok_or_else(|| anyhow!("missing portfolioId"))?;
    let actual = match value {
        Value::String(v) => v.clone(),
        Value::Number(v) => v
            .as_u64()
            .map(|n| n.to_string())
            .ok_or_else(|| anyhow!("invalid portfolioId"))?,
        _ => return Err(anyhow!("invalid portfolioId")),
    };
    if actual == portfolio {
        Ok(())
    } else {
        Err(anyhow!("portfolio mismatch"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delayed_financial_snapshot_preserves_newer_websocket_assets() {
        let mut state = PortfolioFinancialSnapshot::new("123".into(), "OKX".into()).unwrap();
        let mut push = serde_json::json!({"channel":"Assets","data":[{"portfolioId":"123","exchangeType":"OKX","coin":"BTC","balance":"2","updateAt":"30"}]});
        state
            .apply_account_push(&push.to_string(), false, 30)
            .unwrap();
        state
            .apply_account_push(r#"{"channel":"Assets","data":[]}"#, true, 20)
            .unwrap();
        assert_eq!(state.assets["BTC"].gross_balance.as_deref(), Some("2"));
        push["data"][0]["balance"] = Value::String("1".into());
        push["data"][0]["updateAt"] = Value::String("10".into());
        state
            .apply_account_push(&push.to_string(), true, 20)
            .unwrap();
        assert_eq!(state.assets["BTC"].gross_balance.as_deref(), Some("2"));
    }
    #[test]
    fn delta_retains_and_complete_clears_omitted_assets() {
        let mut snapshot = PortfolioFinancialSnapshot::new("p".into(), "BINANCE".into()).unwrap();
        snapshot.apply_account_push(r#"{"channel":"Assets","data":[{"portfolioId":"p","exchangeType":"BINANCE","coin":"USDT","balance":"0","available":"0","debt":"0","loan":"0"}]}"#, true, 1).unwrap();
        snapshot.apply_account_push(r#"{"channel":"Assets","data":[{"portfolioId":"p","exchangeType":"BINANCE","coin":"BTC","balance":"1"}]}"#, false, 2).unwrap();
        assert_eq!(snapshot.assets.len(), 2);
        snapshot
            .apply_account_push(r#"{"channel":"Assets","data":[]}"#, true, 3)
            .unwrap();
        assert!(snapshot.assets.is_empty());
    }
    #[test]
    fn rejects_scope_and_nonfinite_without_mutation() {
        let mut snapshot = PortfolioFinancialSnapshot::new("p".into(), "BINANCE".into()).unwrap();
        assert!(snapshot.apply_account_push(r#"{"channel":"Assets","data":[{"portfolioId":"x","exchangeType":"BINANCE","coin":"USDT","balance":"NaN"}]}"#, false, 1).is_err());
        assert!(snapshot.assets.is_empty());
    }

    #[test]
    fn parses_documented_loan_info_with_numeric_portfolio_id() {
        let mut snapshot =
            PortfolioFinancialSnapshot::new("2010936328078657".into(), "BINANCE".into()).unwrap();
        let response: Value = serde_json::from_str(r#"{"code":200000,"data":{"portfolioId":2010936328078657,"accounts":[{"exchange":"BINANCE","netMarginValue":"69.73","netValidMargin":"69.73","netAvailableMargin":"0","coins":[{"coin":"USDT","loan":"0","loanMmr":"0.05","loanLeverage":1,"loanValue":"0","netEquity":"0","netEquityValue":"0","netMarginValue":"0","portfolioMaxLoanCoin":"100"}]}]}}"#).unwrap();
        snapshot.apply_loan_info(&response, 1).unwrap();
        assert_eq!(snapshot.loans.get("USDT"), Some(&"0".to_string()));
        assert_eq!(snapshot.net_available_margin.as_deref(), Some("0"));
    }

    fn snapshot() -> PortfolioFinancialSnapshot {
        PortfolioFinancialSnapshot::new("p".into(), "BINANCE".into()).unwrap()
    }

    #[test]
    fn max_loan_schema_accepts_zero_and_rejects_duplicate_negative_or_wrong_venue_atomically() {
        let mut state = snapshot();
        let valid: Value = serde_json::from_str(r#"{"code":200000,"data":[{"exchange":"BINANCE","coin":"USDT","portfolioMaxLoanCoin":"0"}]}"#).unwrap();
        state.apply_loan_capacity(&valid, 1).unwrap();
        let original = state.max_borrow_capacity.clone();
        for body in [
            r#"{"code":200000,"data":[{"exchange":"BINANCE","coin":"USDT","portfolioMaxLoanCoin":"1"},{"exchange":"BINANCE","coin":"USDT","portfolioMaxLoanCoin":"2"}]}"#,
            r#"{"code":200000,"data":[{"exchange":"BINANCE","coin":"USDT","portfolioMaxLoanCoin":"-1"}]}"#,
            r#"{"code":200000,"data":[{"exchange":"OKX","coin":"USDT","portfolioMaxLoanCoin":"1"}]}"#,
        ] {
            assert!(state
                .apply_loan_capacity(&serde_json::from_str(body).unwrap(), 2)
                .is_err());
            assert_eq!(state.max_borrow_capacity, original);
        }
    }

    #[test]
    fn loan_info_rejects_duplicate_account_and_wrong_portfolio_without_commit() {
        let mut state = snapshot();
        let before = state.clone();
        for body in [
            r#"{"code":200000,"data":{"portfolioId":"p","accounts":[{"exchange":"BINANCE","coins":[]},{"exchange":"BINANCE","coins":[]}]}}"#,
            r#"{"code":200000,"data":{"portfolioId":"other","accounts":[{"exchange":"BINANCE","coins":[]}]}}"#,
        ] {
            assert!(state
                .apply_loan_info(&serde_json::from_str(body).unwrap(), 1)
                .is_err());
            assert_eq!(state.loans, before.loans);
            assert_eq!(state.observed_ms, before.observed_ms);
        }
    }

    #[test]
    fn accounts_keep_gross_and_net_fields_independent_and_missing_is_null() {
        let mut state = snapshot();
        state.apply_account_push(r#"{"channel":"Accounts","data":{"portfolioId":"p","exchangeType":"BINANCE","equity":"10","netEquity":"9","marginValue":"8","netMarginValue":"7","validMargin":"6","netValidMargin":"5","availableMargin":"4","netAvailableMargin":"3","perpAvailableMargin":"2"}}"#, false, 1).unwrap();
        assert_eq!(state.total_equity.as_deref(), Some("10"));
        assert_eq!(state.net_equity.as_deref(), Some("9"));
        assert_eq!(state.gross_margin.as_deref(), Some("8"));
        assert_eq!(state.net_margin.as_deref(), Some("7"));
        assert_eq!(state.valid_margin.as_deref(), Some("6"));
        assert_eq!(state.net_valid_margin.as_deref(), Some("5"));
        let mut missing = snapshot();
        missing
            .apply_account_push(
                r#"{"channel":"Accounts","data":{"portfolioId":"p","exchangeType":"BINANCE"}}"#,
                false,
                1,
            )
            .unwrap();
        assert_eq!(missing.total_equity, None);
        assert_eq!(missing.net_margin, None);
    }

    #[test]
    fn invalid_second_asset_row_does_not_commit_first_row() {
        let mut state = snapshot();
        assert!(state.apply_account_push(r#"{"channel":"Assets","data":[{"portfolioId":"p","exchangeType":"BINANCE","coin":"USDT","balance":"1"},{"portfolioId":"p","exchangeType":"BINANCE","coin":"BTC","balance":"Infinity"}]}"#, false, 1).is_err());
        assert!(state.assets.is_empty());
        assert!(state.debts.is_empty());
    }
}
fn scope_exchange(row: &Map<String, Value>, exchange: &str) -> Result<()> {
    if required_string(row, "exchangeType")?.eq_ignore_ascii_case(exchange) {
        Ok(())
    } else {
        Err(anyhow!("exchange mismatch"))
    }
}
fn success_data(response: &Value) -> Result<&Value> {
    let code = response
        .get("code")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("missing response code"))?;
    if !matches!(code, 200 | 200000) {
        return Err(anyhow!("response code {code}"));
    };
    response
        .get("data")
        .ok_or_else(|| anyhow!("missing response data"))
}
