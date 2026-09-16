//! RapidX (LTP) 自动还款 Repayer。
//!
//! - 状态查询：`GET rapidxLoan/loan/info` → `data.accounts[]`（按 `exchange` 过滤），
//!   每个 account 带 `coins[]`（`coin` / `loan` / `netEquity` / ...）与 `accountStatus`。
//! - 可用余额：`fetch_account_push("Assets", exchange)` → `data[]`（`coin` / `available`）。
//! - 还款额：`floor(min(loan, available) * 100) / 100`（LTP amount 仅接受两位小数）。
//! - 还款：`POST rapidxLoan/loan/repay`（`exchange` / `coin` / `amount` / `clientOrderId`）。

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use log::{info, warn};
use prettytable::{Cell, Row};
use serde_json::Value;
use trade_engine::ltp_rest::{LtpLoanRepayRequest, LtpRestClient};

use crate::pre_trade::auto_repay::build_three_line_table;
use crate::pre_trade::auto_repay_service::Repayer;

pub struct RapidXRepayer {
    client: LtpRestClient,
    exchange: &'static str,
    name: String,
}

impl RapidXRepayer {
    pub fn new(client: LtpRestClient, exchange: &'static str) -> Self {
        Self {
            client,
            exchange,
            name: format!("rapidx-{}", exchange.to_ascii_lowercase()),
        }
    }
}

#[async_trait]
impl Repayer for RapidXRepayer {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check_and_repay(&self) {
        let loan_info = match self.client.fetch_loan_info().await {
            Ok(v) => v,
            Err(e) => {
                warn!("{} auto-repay: 获取负债失败 {:#}", self.name, e);
                return;
            }
        };
        let account_status = loan_info
            .get("data")
            .and_then(|d| d.get("accounts"))
            .and_then(Value::as_array)
            .and_then(|accounts| {
                accounts
                    .iter()
                    .find(|a| a.get("exchange").and_then(Value::as_str) == Some(self.exchange))
            })
            .and_then(|a| a.get("accountStatus"))
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let debts = match parse_loan_debts(&loan_info, self.exchange) {
            Ok(v) => v,
            Err(e) => {
                warn!("{} auto-repay: 解析 loan/info 失败 {:#}", self.name, e);
                return;
            }
        };
        if debts.is_empty() {
            info!(
                "{} auto-repay: 无未结借头 (exchange={} accountStatus={})",
                self.name, self.exchange, account_status
            );
            return;
        }

        let assets_body = match self
            .client
            .fetch_account_push("Assets", self.exchange)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!("{} auto-repay: 获取资产快照失败 {:#}", self.name, e);
                return;
            }
        };
        let available = match parse_available_assets(&assets_body) {
            Ok(v) => v,
            Err(e) => {
                warn!("{} auto-repay: 解析资产快照失败 {:#}", self.name, e);
                return;
            }
        };

        let decisions = decide_repays(&debts, &available);
        info!(
            "{} auto-repay tick: exchange={} accountStatus={} 共 {} 项有借头，详情:\n{}",
            self.name,
            self.exchange,
            account_status,
            decisions.len(),
            render_decisions_table(&decisions)
        );

        for decision in &decisions {
            if !decision.action.is_repay() {
                continue;
            }
            let client_order_id = format!(
                "autorepay{}{}",
                chrono::Utc::now().timestamp_millis(),
                decision
                    .coin
                    .to_ascii_lowercase()
                    .chars()
                    .filter(|c| c.is_ascii_alphanumeric())
                    .collect::<String>()
            );
            let request = LtpLoanRepayRequest {
                exchange: self.exchange.to_string(),
                coin: decision.coin.clone(),
                amount: format!("{:.2}", decision.amount),
                client_order_id: Some(client_order_id),
            };
            match self.client.repay_loan(&request).await {
                Ok((200, body)) => {
                    let code = serde_json::from_str::<Value>(&body)
                        .ok()
                        .and_then(|v| v.get("code").and_then(Value::as_i64));
                    if matches!(code, Some(200 | 200000)) {
                        let parsed = serde_json::from_str::<Value>(&body).ok();
                        let repaid = parsed
                            .as_ref()
                            .and_then(|v| v.get("data"))
                            .and_then(|d| d.get("repaid"))
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".to_string());
                        let loan_balance = parsed
                            .as_ref()
                            .and_then(|v| v.get("data"))
                            .and_then(|d| d.get("loanBalance"))
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".to_string());
                        info!(
                            "{} auto-repay 成功: coin={} amount={:.2} repaid={} loanBalance={}",
                            self.name, decision.coin, decision.amount, repaid, loan_balance
                        );
                    } else {
                        warn!(
                            "{} auto-repay 失败: coin={} amount={:.2} status=200 body={}",
                            self.name, decision.coin, decision.amount, body
                        );
                    }
                }
                Ok((status, body)) => warn!(
                    "{} auto-repay 失败: coin={} amount={:.2} status={} body={}",
                    self.name, decision.coin, decision.amount, status, body
                ),
                Err(e) => warn!(
                    "{} auto-repay 失败: coin={} amount={:.2} err={:#}. \
                     RapidX repayment result is unknown; do not retry blindly — \
                     check loan history by clientOrderId",
                    self.name, decision.coin, decision.amount, e
                ),
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepayAction {
    Repay,
    SkipNoFree,
    SkipBelowMin,
    SkipNoBalanceInfo,
}

impl RepayAction {
    fn is_repay(self) -> bool {
        matches!(self, RepayAction::Repay)
    }

    fn label(self) -> &'static str {
        match self {
            RepayAction::Repay => "REPAY",
            RepayAction::SkipNoFree => "SKIP_NO_FREE",
            RepayAction::SkipBelowMin => "SKIP_BELOW_MIN",
            RepayAction::SkipNoBalanceInfo => "SKIP_NO_BALANCE_INFO",
        }
    }
}

#[derive(Debug, Clone)]
struct RepayDecision {
    coin: String,
    loan: f64,
    available: Option<f64>,
    amount: f64,
    action: RepayAction,
}

fn parse_loan_debts(value: &Value, exchange: &str) -> Result<BTreeMap<String, f64>> {
    let accounts = value
        .get("data")
        .and_then(|d| d.get("accounts"))
        .and_then(Value::as_array);
    let Some(accounts) = accounts else {
        return Ok(BTreeMap::new());
    };
    let matching: Vec<&Value> = accounts
        .iter()
        .filter(|a| a.get("exchange").and_then(Value::as_str) == Some(exchange))
        .collect();
    if matching.is_empty() {
        return Ok(BTreeMap::new());
    }
    if matching.len() > 1 {
        return Err(anyhow!(
            "loan/info returned {} accounts for exchange={}",
            matching.len(),
            exchange
        ));
    }
    let mut out = BTreeMap::new();
    let coins = matching[0]
        .get("coins")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for coin_entry in &coins {
        let coin = coin_entry
            .get("coin")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        if coin.is_empty() {
            continue;
        }
        let loan = parse_f64(coin_entry.get("loan"));
        if loan > 0.0 {
            out.insert(coin, loan);
        }
    }
    Ok(out)
}

fn parse_available_assets(body: &str) -> Result<BTreeMap<String, f64>> {
    let v: Value = serde_json::from_str(body)
        .map_err(|e| anyhow!("invalid JSON from RapidX Assets snapshot: {}", e))?;
    let rows = v
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("RapidX Assets snapshot expected data array, got: {}", v))?;
    let mut out = BTreeMap::new();
    for row in rows {
        let coin = row
            .get("coin")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        if coin.is_empty() {
            continue;
        }
        let available = match row.get("available") {
            Some(Value::String(s)) => s.trim().parse::<f64>().ok(),
            Some(Value::Number(n)) => n.as_f64(),
            _ => None,
        };
        if let Some(available) = available {
            out.insert(coin, available);
        }
    }
    Ok(out)
}

fn decide_repays(
    debts: &BTreeMap<String, f64>,
    available: &BTreeMap<String, f64>,
) -> Vec<RepayDecision> {
    debts
        .iter()
        .map(|(coin, &loan)| {
            let avail = available.get(coin).copied();
            let (amount, action) = match avail {
                None => (0.0, RepayAction::SkipNoBalanceInfo),
                Some(a) if a <= 0.0 => (0.0, RepayAction::SkipNoFree),
                Some(a) => {
                    let amount = (loan.min(a) * 100.0).floor() / 100.0;
                    if amount < 0.01 {
                        (0.0, RepayAction::SkipBelowMin)
                    } else {
                        (amount, RepayAction::Repay)
                    }
                }
            };
            RepayDecision {
                coin: coin.clone(),
                loan,
                available: avail,
                amount,
                action,
            }
        })
        .collect()
}

fn parse_f64(v: Option<&Value>) -> f64 {
    match v {
        Some(Value::String(s)) => s.trim().parse().unwrap_or(0.0),
        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn render_decisions_table(decisions: &[RepayDecision]) -> String {
    let mut table = build_three_line_table(&["coin", "loan", "available", "amount", "action"]);
    for d in decisions {
        table.add_row(Row::from(vec![
            Cell::new(&d.coin),
            Cell::new(&fmt(d.loan)),
            Cell::new(&d.available.map(fmt).unwrap_or_else(|| "-".to_string())),
            Cell::new(&fmt(d.amount)),
            Cell::new(d.action.label()),
        ]));
    }
    table.to_string()
}

fn fmt(value: f64) -> String {
    let s = format!("{:.8}", value);
    let trimmed = s.trim_end_matches('0').trim_end_matches('.').to_string();
    if trimmed.is_empty() || trimmed == "-" {
        "0".to_string()
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loan_info_body(coins: &str) -> Value {
        serde_json::from_str(&format!(
            r#"{{"code":200000,"data":{{"portfolioId":"1","accounts":[{{"exchange":"BINANCE","accountStatus":"NORMAL","ltv":"0.362","coins":[{coins}]}}]}}}}"#
        ))
        .unwrap()
    }

    #[test]
    fn parse_loan_debts_filters_positive_loans_and_uppercases() {
        let v = loan_info_body(
            r#"{"coin":"usdt","loan":"39.02"},{"coin":"BTC","loan":"0"},{"coin":"ETH","loan":1.5}"#,
        );
        let debts = parse_loan_debts(&v, "BINANCE").unwrap();
        assert_eq!(debts.len(), 2);
        assert_eq!(debts["USDT"], 39.02);
        assert_eq!(debts["ETH"], 1.5);
        assert!(!debts.contains_key("BTC"));
    }

    #[test]
    fn parse_loan_debts_selects_matching_exchange() {
        let v: Value = serde_json::from_str(
            r#"{"code":200000,"data":{"accounts":[
                {"exchange":"OKX","coins":[{"coin":"USDT","loan":"7"}]},
                {"exchange":"BINANCE","coins":[{"coin":"USDT","loan":"3"}]}
            ]}}"#,
        )
        .unwrap();
        let debts = parse_loan_debts(&v, "BINANCE").unwrap();
        assert_eq!(debts["USDT"], 3.0);
    }

    #[test]
    fn parse_loan_debts_empty_when_exchange_absent() {
        let v = loan_info_body(r#"{"coin":"USDT","loan":"5"}"#);
        assert!(parse_loan_debts(&v, "OKX").unwrap().is_empty());
    }

    #[test]
    fn parse_loan_debts_errors_on_duplicate_exchange_accounts() {
        let v: Value = serde_json::from_str(
            r#"{"code":200000,"data":{"accounts":[
                {"exchange":"BINANCE","coins":[]},
                {"exchange":"BINANCE","coins":[]}
            ]}}"#,
        )
        .unwrap();
        assert!(parse_loan_debts(&v, "BINANCE").is_err());
    }

    #[test]
    fn parse_available_assets_uses_available_and_skips_missing() {
        let body = r#"{"channel":"Assets","data":[
            {"coin":"USDT","available":"12.5","balance":"20"},
            {"coin":"BTC","available":0.001},
            {"coin":"ETH","balance":"3"},
            {"coin":"SOL","available":"bad"}
        ]}"#;
        let assets = parse_available_assets(body).unwrap();
        assert_eq!(assets.len(), 2);
        assert_eq!(assets["USDT"], 12.5);
        assert_eq!(assets["BTC"], 0.001);
    }

    #[test]
    fn decide_repays_floors_to_two_decimals() {
        let debts = BTreeMap::from([("USDT".to_string(), 0.129)]);
        let available = BTreeMap::from([("USDT".to_string(), 5.0)]);
        let decisions = decide_repays(&debts, &available);
        assert_eq!(decisions[0].action, RepayAction::Repay);
        assert_eq!(decisions[0].amount, 0.12);
    }

    #[test]
    fn decide_repays_caps_at_loan() {
        let debts = BTreeMap::from([("USDT".to_string(), 1.0)]);
        let available = BTreeMap::from([("USDT".to_string(), 500.0)]);
        let decisions = decide_repays(&debts, &available);
        assert_eq!(decisions[0].action, RepayAction::Repay);
        assert_eq!(decisions[0].amount, 1.0);
    }

    #[test]
    fn decide_repays_skips_below_min() {
        let debts = BTreeMap::from([("USDT".to_string(), 0.005)]);
        let available = BTreeMap::from([("USDT".to_string(), 5.0)]);
        let decisions = decide_repays(&debts, &available);
        assert_eq!(decisions[0].action, RepayAction::SkipBelowMin);
    }

    #[test]
    fn decide_repays_skips_missing_or_zero_available() {
        let debts = BTreeMap::from([("USDT".to_string(), 1.0), ("BTC".to_string(), 0.5)]);
        let available = BTreeMap::from([("BTC".to_string(), 0.0)]);
        let decisions = decide_repays(&debts, &available);
        assert_eq!(decisions[0].action, RepayAction::SkipNoFree);
        assert_eq!(decisions[1].action, RepayAction::SkipNoBalanceInfo);
    }

    #[test]
    fn render_table_includes_actions() {
        let debts = BTreeMap::from([("USDT".to_string(), 1.0)]);
        let available = BTreeMap::from([("USDT".to_string(), 2.0)]);
        let s = render_decisions_table(&decide_repays(&debts, &available));
        assert!(s.contains("USDT"));
        assert!(s.contains("REPAY"));
        assert!(s.matches("---").count() >= 3);
    }
}
