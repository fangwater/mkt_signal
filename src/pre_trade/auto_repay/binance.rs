//! Binance Portfolio Margin（PM）自动还款 Repayer。
//!
//! - 状态查询：`GET /papi/v1/balance` → 数组 `[{asset, crossMarginBorrowed, crossMarginInterest, crossMarginFree, ...}]`
//! - 还款触发：`crossMarginBorrowed > 0 且 crossMarginFree > 0`（per-asset）。
//! - 还款额：`min(crossMarginBorrowed + crossMarginInterest, crossMarginFree)`。
//! - 还款：`POST /papi/v1/repayLoan?asset=&amount=&timestamp=&recvWindow=&signature=`
//!
//! 仅在 `BinanceAccountMode::Unified`（PM）账户下注册。非 PM 走不通 `/papi/*`。

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use hmac::{Hmac, Mac};
use log::{debug, info, warn};
use prettytable::{Cell, Row};
use reqwest::Client;
use sha2::Sha256;
use std::collections::BTreeMap;

use crate::pre_trade::auto_repay::build_three_line_table;
use crate::pre_trade::auto_repay_service::Repayer;

type HmacSha256 = Hmac<Sha256>;

pub struct BinanceRepayer {
    client: Client,
    rest_base: String,
    api_key: String,
    api_secret: String,
    recv_window_ms: u64,
}

impl BinanceRepayer {
    pub fn new(
        rest_base: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        recv_window_ms: u64,
    ) -> Self {
        Self {
            client: Client::new(),
            rest_base: rest_base.into(),
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            recv_window_ms,
        }
    }

    fn sign_query(&self, query: &str) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .map_err(|e| anyhow!("HMAC key error: {}", e))?;
        mac.update(query.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    async fn fetch_balances(&self) -> Result<String> {
        let mut params = BTreeMap::new();
        params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        if self.recv_window_ms > 0 {
            params.insert("recvWindow".to_string(), self.recv_window_ms.to_string());
        }
        let query = build_query(&params);
        let signature = self.sign_query(&query)?;
        let url = format!(
            "{}/papi/v1/balance?{}&signature={}",
            self.rest_base, query, signature
        );
        let resp = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!(
                "/papi/v1/balance failed: status={status} body={body}"
            ));
        }
        Ok(body)
    }

    async fn repay_loan(&self, asset: &str, amount: f64) -> Result<String> {
        let mut params = BTreeMap::new();
        params.insert("asset".to_string(), asset.to_string());
        params.insert("amount".to_string(), format!("{:.8}", amount));
        params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        if self.recv_window_ms > 0 {
            params.insert("recvWindow".to_string(), self.recv_window_ms.to_string());
        }
        let query = build_query(&params);
        let signature = self.sign_query(&query)?;
        let url = format!(
            "{}/papi/v1/repayLoan?{}&signature={}",
            self.rest_base, query, signature
        );
        debug!("POST {} asset={} amount={:.8}", url, asset, amount);
        let resp = self
            .client
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!(
                "/papi/v1/repayLoan failed: status={status} body={body}"
            ));
        }
        Ok(body)
    }
}

#[async_trait]
impl Repayer for BinanceRepayer {
    fn name(&self) -> &str {
        "binance"
    }

    async fn check_and_repay(&self) {
        if self.api_key.trim().is_empty() || self.api_secret.trim().is_empty() {
            warn!("binance auto-repay: BINANCE_API_KEY/SECRET 为空，跳过");
            return;
        }
        let body = match self.fetch_balances().await {
            Ok(b) => b,
            Err(e) => {
                warn!("binance auto-repay: 获取负债失败 {:#}", e);
                return;
            }
        };
        let entries = match parse_balance_entries(&body) {
            Ok(v) => v,
            Err(e) => {
                warn!("binance auto-repay: 解析 /papi/v1/balance 失败 {:#}", e);
                return;
            }
        };
        let borrowed_entries: Vec<_> = entries.iter().filter(|e| e.borrowed > 0.0).collect();
        if borrowed_entries.is_empty() {
            info!("binance auto-repay: 无未结借头");
            return;
        }

        let decisions: Vec<RepayDecision> = borrowed_entries
            .iter()
            .map(|e| RepayDecision::from_entry(e))
            .collect();
        info!(
            "binance auto-repay tick: 共 {} 项有借头，详情:\n{}",
            decisions.len(),
            render_decisions_table(&decisions)
        );

        for decision in &decisions {
            if !decision.action.is_repay() {
                continue;
            }
            match self
                .repay_loan(&decision.asset, decision.repay_amount)
                .await
            {
                Ok(resp) => info!(
                    "binance auto-repay 成功: asset={} amount={:.8} resp={}",
                    decision.asset, decision.repay_amount, resp
                ),
                Err(e) => warn!(
                    "binance auto-repay 失败: asset={} amount={:.8} err={:#}",
                    decision.asset, decision.repay_amount, e
                ),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BalanceEntry {
    pub asset: String,
    pub borrowed: f64,
    pub interest: f64,
    pub free: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RepayAction {
    Repay,
    SkipNoFree,
}

impl RepayAction {
    fn is_repay(&self) -> bool {
        matches!(self, RepayAction::Repay)
    }
    fn label(&self) -> &'static str {
        match self {
            RepayAction::Repay => "REPAY",
            RepayAction::SkipNoFree => "SKIP_NO_FREE",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RepayDecision {
    pub asset: String,
    pub borrowed: f64,
    pub interest: f64,
    pub free: f64,
    pub repay_amount: f64,
    pub action: RepayAction,
}

impl RepayDecision {
    fn from_entry(e: &BalanceEntry) -> Self {
        let action = if e.free <= 0.0 {
            RepayAction::SkipNoFree
        } else {
            RepayAction::Repay
        };
        let repay_amount = if matches!(action, RepayAction::Repay) {
            (e.borrowed + e.interest).min(e.free)
        } else {
            0.0
        };
        Self {
            asset: e.asset.clone(),
            borrowed: e.borrowed,
            interest: e.interest,
            free: e.free,
            repay_amount,
            action,
        }
    }
}

pub(crate) fn parse_balance_entries(body: &str) -> Result<Vec<BalanceEntry>> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| anyhow!("invalid JSON from /papi/v1/balance: {}", e))?;
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow!("/papi/v1/balance expected array, got: {}", v))?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let asset = item
            .get("asset")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if asset.is_empty() {
            continue;
        }
        let borrowed = parse_f64(item.get("crossMarginBorrowed"));
        let interest = parse_f64(item.get("crossMarginInterest"));
        let free = parse_f64(item.get("crossMarginFree"));
        out.push(BalanceEntry {
            asset,
            borrowed,
            interest,
            free,
        });
    }
    Ok(out)
}

fn parse_f64(v: Option<&serde_json::Value>) -> f64 {
    match v {
        Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
        Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn build_query(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}

fn render_decisions_table(decisions: &[RepayDecision]) -> String {
    let mut table = build_three_line_table(&[
        "asset",
        "borrowed",
        "interest",
        "free",
        "repay_amount",
        "action",
    ]);
    for d in decisions {
        table.add_row(Row::from(vec![
            Cell::new(&d.asset),
            Cell::new(&fmt(d.borrowed)),
            Cell::new(&fmt(d.interest)),
            Cell::new(&fmt(d.free)),
            Cell::new(&fmt(d.repay_amount)),
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
    fn parse_balance_entries_with_borrowed() {
        let body = r#"[
            {"asset":"USDT","crossMarginBorrowed":"100.0","crossMarginInterest":"0.5","crossMarginFree":"50.0"},
            {"asset":"BTC","crossMarginBorrowed":"0","crossMarginInterest":"0","crossMarginFree":"0.001"},
            {"asset":"ETH","crossMarginBorrowed":"2.5","crossMarginInterest":"0.001","crossMarginFree":"3.0"}
        ]"#;
        let entries = parse_balance_entries(body).expect("parse ok");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].asset, "USDT");
        assert_eq!(entries[0].borrowed, 100.0);
        assert_eq!(entries[0].interest, 0.5);
        assert_eq!(entries[0].free, 50.0);
        assert_eq!(entries[2].asset, "ETH");
        assert_eq!(entries[2].borrowed, 2.5);
    }

    #[test]
    fn parse_balance_entries_skips_empty_asset() {
        let body =
            r#"[{"asset":"","crossMarginBorrowed":"1"},{"asset":"BTC","crossMarginBorrowed":"0"}]"#;
        let entries = parse_balance_entries(body).expect("parse ok");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].asset, "BTC");
    }

    #[test]
    fn parse_balance_entries_handles_numeric_fields() {
        let body = r#"[{"asset":"USDT","crossMarginBorrowed":1.5,"crossMarginFree":3.0,"crossMarginInterest":0.01}]"#;
        let entries = parse_balance_entries(body).expect("parse ok");
        assert_eq!(entries[0].borrowed, 1.5);
        assert_eq!(entries[0].free, 3.0);
        assert_eq!(entries[0].interest, 0.01);
    }

    #[test]
    fn parse_balance_entries_rejects_non_array() {
        let body = r#"{"code": -1}"#;
        assert!(parse_balance_entries(body).is_err());
    }

    #[test]
    fn decision_repays_when_free_positive() {
        let e = BalanceEntry {
            asset: "USDT".into(),
            borrowed: 100.0,
            interest: 0.5,
            free: 50.0,
        };
        let d = RepayDecision::from_entry(&e);
        assert_eq!(d.action, RepayAction::Repay);
        assert_eq!(d.repay_amount, 50.0); // min(100.5, 50)
    }

    #[test]
    fn decision_skips_when_no_free() {
        let e = BalanceEntry {
            asset: "ETH".into(),
            borrowed: 2.0,
            interest: 0.001,
            free: 0.0,
        };
        let d = RepayDecision::from_entry(&e);
        assert_eq!(d.action, RepayAction::SkipNoFree);
        assert_eq!(d.repay_amount, 0.0);
    }

    #[test]
    fn decision_caps_repay_at_total_debt() {
        let e = BalanceEntry {
            asset: "BTC".into(),
            borrowed: 1.0,
            interest: 0.001,
            free: 100.0,
        };
        let d = RepayDecision::from_entry(&e);
        assert_eq!(d.repay_amount, 1.001);
    }

    #[test]
    fn render_table_includes_three_horizontal_lines() {
        let decisions = vec![
            RepayDecision::from_entry(&BalanceEntry {
                asset: "USDT".into(),
                borrowed: 100.0,
                interest: 0.5,
                free: 50.0,
            }),
            RepayDecision::from_entry(&BalanceEntry {
                asset: "ETH".into(),
                borrowed: 2.0,
                interest: 0.0,
                free: 0.0,
            }),
        ];
        let s = render_decisions_table(&decisions);
        assert!(s.contains("USDT"));
        assert!(s.contains("REPAY"));
        assert!(s.contains("ETH"));
        assert!(s.contains("SKIP_NO_FREE"));
        assert!(s.matches("---").count() >= 3);
    }
}
