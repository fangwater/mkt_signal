//! Bybit UNIFIED 自动还款 Repayer。
//!
//! - 状态查询：`GET /v5/account/wallet-balance?accountType=UNIFIED`
//!   → `result.list[0].coin[].{walletBalance, borrowAmount, accruedInterest}`
//! - 还款触发：`borrowAmount > 0 且 walletBalance > 0`（per-coin）。
//! - 还款：`POST /v5/account/no-convert-repay {coin, repaymentType:"FLEXIBLE"}`。
//!
//! 关键设计选择：使用 **`no-convert-repay`** 而非 `quick-repayment`。
//! 实测（DOT，2026-05-08）：
//!   - `no-convert-repay`：纯同币——只用同币 spot available 抵同币 borrow，USDT 完全不动 ✅
//!   - `quick-repayment` ：跨币种——会自动用 USDT 折价"买"借币来抵借头 ❌
//! 用户策略原则"永不买回，避免现货双向仓位"要求严格同币 repay，故选前者。
//!
//! Bybit 服务端规则：每小时 04:00–05:30 UTC 是清算/利息结算窗口，禁止还款。
//! 我们在该窗口跳过 Bybit 这一轮（其他 Repayer 不受影响）。
//!
//! Bybit V5 wallet-balance 字段语义参见
//! `query_parsers::bybit_account_balance_snapshot`：walletBalance 是物理持仓
//! （借币卖出后会变负），borrowAmount 是借头本金，accruedInterest 是累计利息。

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{Timelike, Utc};
use log::{info, warn};
use prettytable::{Cell, Row};
use reqwest::Client;
use serde_json::json;

use crate::portfolio_margin::bybit_auth::BybitCredentials;
use crate::pre_trade::auto_repay::build_three_line_table;
use crate::pre_trade::auto_repay_service::{looks_like_no_liability, Repayer};
use crate::trade_engine::bybit_query::{bybit_rest_get, bybit_rest_post};

const WALLET_BALANCE_PATH: &str = "/v5/account/wallet-balance";
const NO_CONVERT_REPAY_PATH: &str = "/v5/account/no-convert-repay";

pub struct BybitRepayer {
    client: Client,
    creds: BybitCredentials,
}

impl BybitRepayer {
    pub fn new(creds: BybitCredentials) -> Self {
        Self {
            client: Client::new(),
            creds,
        }
    }

    async fn fetch_wallet(&self) -> Result<String> {
        let (status, body) = bybit_rest_get(
            &self.client,
            &self.creds,
            WALLET_BALANCE_PATH,
            "accountType=UNIFIED",
        )
        .await
        .map_err(|e| anyhow!("GET {} 失败: {:#}", WALLET_BALANCE_PATH, e))?;
        if !(200..300).contains(&status) {
            return Err(anyhow!(
                "GET {} status={} body={}",
                WALLET_BALANCE_PATH,
                status,
                body
            ));
        }
        Ok(body)
    }

    async fn submit_no_convert_repay(&self, coin: &str) -> Result<(u16, String)> {
        // FLEXIBLE: 同步返回 + 浮动利率借头优先（系统会先还浮动再还固定）。
        let payload = json!({ "coin": coin, "repaymentType": "FLEXIBLE" });
        let body = payload.to_string();
        bybit_rest_post(&self.client, &self.creds, NO_CONVERT_REPAY_PATH, &body)
            .await
            .map_err(|e| anyhow!("POST {} 失败: {:#}", NO_CONVERT_REPAY_PATH, e))
    }
}

/// Bybit 服务端禁还窗口：每小时 UTC 04:00–05:30。命中返回 true。
fn is_in_bybit_ban_window(now: chrono::DateTime<Utc>) -> bool {
    let h = now.hour();
    let m = now.minute();
    h == 4 || (h == 5 && m < 30)
}

#[async_trait]
impl Repayer for BybitRepayer {
    fn name(&self) -> &str {
        "bybit"
    }

    async fn check_and_repay(&self) {
        if is_in_bybit_ban_window(Utc::now()) {
            info!("bybit auto-repay: 当前在禁还窗口（UTC 04:00–05:30），本轮跳过");
            return;
        }
        let body = match self.fetch_wallet().await {
            Ok(b) => b,
            Err(e) => {
                warn!("bybit auto-repay: 获取 wallet-balance 失败 {:#}", e);
                return;
            }
        };
        let entries = match parse_borrows(&body) {
            Ok(v) => v,
            Err(e) => {
                warn!("bybit auto-repay: 解析 wallet-balance 失败 {:#}", e);
                return;
            }
        };
        let borrowed_entries: Vec<_> = entries.iter().filter(|c| c.borrow_amount > 0.0).collect();
        if borrowed_entries.is_empty() {
            info!("bybit auto-repay: 无 borrowAmount > 0 的币");
            return;
        }

        let decisions: Vec<RepayDecision> = borrowed_entries
            .iter()
            .map(|c| RepayDecision::from_entry(c))
            .collect();
        info!(
            "bybit auto-repay tick: 共 {} 个币带 borrowAmount>0，详情:\n{}",
            decisions.len(),
            render_decisions_table(&decisions)
        );

        for decision in &decisions {
            if !decision.action.is_repay() {
                continue;
            }
            match self.submit_no_convert_repay(&decision.coin).await {
                Ok((status, resp_body)) => {
                    let http_ok = (200..300).contains(&status);
                    let ret_code_zero = bybit_ret_code_ok(&resp_body);
                    let result_status = parse_result_status(&resp_body);
                    if http_ok && ret_code_zero && matches!(result_status.as_deref(), Some("SU")) {
                        info!(
                            "bybit auto-repay 成功: coin={} status={} resp={}",
                            decision.coin, status, resp_body
                        );
                    } else if http_ok
                        && ret_code_zero
                        && matches!(result_status.as_deref(), Some("P"))
                    {
                        // FLEXIBLE 在文档说同步，但若 Bybit 返回 Processing 也算正常，
                        // 下一轮 tick 会再拉一次 wallet-balance 校对。
                        info!(
                            "bybit auto-repay 提交(processing): coin={} status={} resp={}",
                            decision.coin, status, resp_body
                        );
                    } else if looks_like_no_liability(&resp_body) {
                        info!(
                            "bybit auto-repay 跳过(no liability): coin={} status={} resp={}",
                            decision.coin, status, resp_body
                        );
                    } else {
                        warn!(
                            "bybit auto-repay 失败: coin={} status={} resp={}",
                            decision.coin, status, resp_body
                        );
                    }
                }
                Err(e) => warn!(
                    "bybit auto-repay 网络错误: coin={} err={:#}",
                    decision.coin, e
                ),
            }
        }
    }
}

fn parse_result_status(body: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            v.get("result")
                .and_then(|r| r.get("resultStatus"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string())
        })
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BorrowEntry {
    pub coin: String,
    pub wallet_balance: f64,
    pub borrow_amount: f64,
    pub accrued_interest: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RepayAction {
    Repay,
    SkipNoWallet,
}

impl RepayAction {
    fn is_repay(&self) -> bool {
        matches!(self, RepayAction::Repay)
    }
    fn label(&self) -> &'static str {
        match self {
            RepayAction::Repay => "REPAY",
            RepayAction::SkipNoWallet => "SKIP_NO_WALLET",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RepayDecision {
    pub coin: String,
    pub borrow_amount: f64,
    pub accrued_interest: f64,
    pub wallet_balance: f64,
    pub action: RepayAction,
}

impl RepayDecision {
    fn from_entry(e: &BorrowEntry) -> Self {
        let action = if e.wallet_balance <= 0.0 {
            RepayAction::SkipNoWallet
        } else {
            RepayAction::Repay
        };
        Self {
            coin: e.coin.clone(),
            borrow_amount: e.borrow_amount,
            accrued_interest: e.accrued_interest,
            wallet_balance: e.wallet_balance,
            action,
        }
    }
}

pub(crate) fn parse_borrows(body: &str) -> Result<Vec<BorrowEntry>> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| anyhow!("invalid JSON from wallet-balance: {}", e))?;
    let list = v
        .get("result")
        .and_then(|r| r.get("list"))
        .and_then(|l| l.as_array())
        .ok_or_else(|| anyhow!("wallet-balance missing result.list: {}", v))?;
    let mut out = Vec::new();
    for account in list {
        let coins = match account.get("coin").and_then(|c| c.as_array()) {
            Some(arr) => arr,
            None => continue,
        };
        for coin_obj in coins {
            let coin = coin_obj
                .get("coin")
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if coin.is_empty() {
                continue;
            }
            out.push(BorrowEntry {
                coin,
                wallet_balance: parse_str_or_num(coin_obj.get("walletBalance")),
                borrow_amount: parse_str_or_num(coin_obj.get("borrowAmount")),
                accrued_interest: parse_str_or_num(coin_obj.get("accruedInterest")),
            });
        }
    }
    Ok(out)
}

fn parse_str_or_num(v: Option<&serde_json::Value>) -> f64 {
    match v {
        Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
        Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn bybit_ret_code_ok(body: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("retCode").and_then(|c| c.as_i64()))
        .map(|c| c == 0)
        .unwrap_or(false)
}

fn render_decisions_table(decisions: &[RepayDecision]) -> String {
    let mut table = build_three_line_table(&[
        "coin",
        "borrowAmount",
        "accruedInterest",
        "walletBalance",
        "action",
    ]);
    for d in decisions {
        table.add_row(Row::from(vec![
            Cell::new(&d.coin),
            Cell::new(&fmt(d.borrow_amount)),
            Cell::new(&fmt(d.accrued_interest)),
            Cell::new(&fmt(d.wallet_balance)),
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

    #[test]
    fn parse_borrows_extracts_per_coin() {
        let body = r#"{
            "retCode": 0,
            "result": {
                "list": [{
                    "accountType": "UNIFIED",
                    "coin": [
                        {"coin":"BTC","walletBalance":"0.5","borrowAmount":"0.4","accruedInterest":"0.001"},
                        {"coin":"ETH","walletBalance":"-0.0003","borrowAmount":"24.47","accruedInterest":"0.00005"},
                        {"coin":"USDT","walletBalance":"1000","borrowAmount":"0","accruedInterest":"0"}
                    ]
                }]
            }
        }"#;
        let entries = parse_borrows(body).expect("parse ok");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].coin, "BTC");
        assert_eq!(entries[0].wallet_balance, 0.5);
        assert_eq!(entries[0].borrow_amount, 0.4);
        assert!((entries[0].accrued_interest - 0.001).abs() < 1e-12);
        assert_eq!(entries[1].wallet_balance, -0.0003);
    }

    #[test]
    fn decision_repays_when_wallet_positive() {
        let e = BorrowEntry {
            coin: "BTC".into(),
            wallet_balance: 0.5,
            borrow_amount: 0.4,
            accrued_interest: 0.0,
        };
        let d = RepayDecision::from_entry(&e);
        assert_eq!(d.action, RepayAction::Repay);
    }

    #[test]
    fn decision_skips_when_wallet_negative_or_zero() {
        for w in [0.0, -0.0001, -100.0] {
            let e = BorrowEntry {
                coin: "ETH".into(),
                wallet_balance: w,
                borrow_amount: 1.0,
                accrued_interest: 0.0,
            };
            let d = RepayDecision::from_entry(&e);
            assert_eq!(d.action, RepayAction::SkipNoWallet);
        }
    }

    #[test]
    fn parse_borrows_rejects_missing_list() {
        assert!(parse_borrows(r#"{"retCode":0,"result":{}}"#).is_err());
    }

    #[test]
    fn ret_code_ok_detects_zero() {
        assert!(bybit_ret_code_ok(r#"{"retCode":0,"retMsg":"OK"}"#));
        assert!(!bybit_ret_code_ok(
            r#"{"retCode":182102,"retMsg":"no liability"}"#
        ));
        assert!(!bybit_ret_code_ok("not json"));
    }

    #[test]
    fn ban_window_covers_04_to_0530_utc() {
        use chrono::TimeZone;
        let at = |h, m| Utc.with_ymd_and_hms(2026, 5, 8, h, m, 0).unwrap();
        // 边界
        assert!(!is_in_bybit_ban_window(at(3, 59))); // 03:59 OK
        assert!(is_in_bybit_ban_window(at(4, 0))); // 04:00 禁
        assert!(is_in_bybit_ban_window(at(4, 30)));
        assert!(is_in_bybit_ban_window(at(4, 59)));
        assert!(is_in_bybit_ban_window(at(5, 0)));
        assert!(is_in_bybit_ban_window(at(5, 29)));
        assert!(!is_in_bybit_ban_window(at(5, 30))); // 05:30 OK
        assert!(!is_in_bybit_ban_window(at(5, 31)));
        // 其他小时全部 OK
        assert!(!is_in_bybit_ban_window(at(0, 0)));
        assert!(!is_in_bybit_ban_window(at(12, 30)));
        assert!(!is_in_bybit_ban_window(at(23, 55)));
    }

    #[test]
    fn parse_result_status_extracts_su() {
        let body = r#"{"retCode":0,"retMsg":"success","result":{"resultStatus":"SU"},"time":1}"#;
        assert_eq!(parse_result_status(body).as_deref(), Some("SU"));
    }

    #[test]
    fn parse_result_status_extracts_processing() {
        let body = r#"{"retCode":0,"result":{"resultStatus":"P"}}"#;
        assert_eq!(parse_result_status(body).as_deref(), Some("P"));
    }

    #[test]
    fn parse_result_status_handles_missing() {
        assert!(parse_result_status(r#"{"retCode":0}"#).is_none());
        assert!(parse_result_status("not json").is_none());
    }

    #[test]
    fn render_table_marks_skip_and_repay() {
        let entries = vec![
            BorrowEntry {
                coin: "BTC".into(),
                wallet_balance: 0.5,
                borrow_amount: 0.4,
                accrued_interest: 0.001,
            },
            BorrowEntry {
                coin: "ETH".into(),
                wallet_balance: -0.0003,
                borrow_amount: 24.47,
                accrued_interest: 0.0,
            },
        ];
        let decisions: Vec<_> = entries.iter().map(RepayDecision::from_entry).collect();
        let s = render_decisions_table(&decisions);
        assert!(s.contains("BTC"));
        assert!(s.contains("REPAY"));
        assert!(s.contains("ETH"));
        assert!(s.contains("SKIP_NO_WALLET"));
        assert!(s.matches("---").count() >= 3);
    }
}
