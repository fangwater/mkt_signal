//! Gate UNIFIED 自动还款 Repayer。
//!
//! - 状态查询：`GET /api/v4/unified/accounts` → `balances.{currency}.{available, borrowed, total_liab, ...}`
//! - 还款触发：`borrowed > 0 且 available > 0`（per-coin），repay_amount = `min(total_liab, available)`
//! - 还款：`POST /api/v4/unified/loans` `{currency, amount, type:"repay", repaid_all:false, text}`
//!
//! 关键设计：
//! - **`repaid_all=false`** —— 实测 `repaid_all=true` 在 `available < total_liab`（哪怕只差 1.4e-6）时
//!   会整笔 `BALANCE_NOT_ENOUGH`。`false` 模式按显式 amount 部分还，符合"满足条件尽量还"。
//! - **同币种 only** —— `/unified/loans` endpoint 实测不会跨币种 convert（USDT delta 严格 = 0），
//!   符合用户原则"永不买回，避免现货双向仓位"。
//!
//! 注意：`order_manager.rs` 给所有 GateMargin 单加的 `auto_repay=true` 只能抵销
//! 「本单 auto_borrow 触发」的借头；历史 borrowed/IM 必须显式调本端点才能清。

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use log::{info, warn};
use prettytable::{Cell, Row};
use reqwest::Client;
use serde_json::json;

use crate::portfolio_margin::gate_auth::GateCredentials;
use crate::pre_trade::auto_repay::build_three_line_table;
use crate::pre_trade::auto_repay_service::{looks_like_no_liability, Repayer};
use crate::trade_engine::gate_query::{gate_rest_get, gate_rest_post};

const UNIFIED_ACCOUNTS_PATH: &str = "/api/v4/unified/accounts";
const UNIFIED_LOANS_PATH: &str = "/api/v4/unified/loans";

pub struct GateRepayer {
    client: Client,
    creds: GateCredentials,
}

impl GateRepayer {
    pub fn new(creds: GateCredentials) -> Self {
        Self {
            client: Client::new(),
            creds,
        }
    }

    async fn fetch_accounts(&self) -> Result<String> {
        let (status, body) = gate_rest_get(&self.client, &self.creds, UNIFIED_ACCOUNTS_PATH, "")
            .await
            .map_err(|e| anyhow!("GET {} 失败: {:#}", UNIFIED_ACCOUNTS_PATH, e))?;
        if !(200..300).contains(&status) {
            return Err(anyhow!(
                "GET {} status={} body={}",
                UNIFIED_ACCOUNTS_PATH,
                status,
                body
            ));
        }
        Ok(body)
    }

    async fn submit_repay(&self, decision: &RepayDecision) -> Result<(u16, String)> {
        // 实测（2026-05-08，BTC available=0.04879<total_liab=0.04880）：
        //   repaid_all=true  → Gate 严格要求 available ≥ total_liab，差一点就 BALANCE_NOT_ENOUGH。
        //   repaid_all=false + amount=min(available,total_liab) → 部分还成功，USDT 不动。
        // 所以一律用 repaid_all=false + 显式 amount，跟"无 convert + 满足条件尽量还"对齐。
        let payload = json!({
            "currency": decision.currency,
            "amount": format_amount(decision.repay_amount),
            "type": "repay",
            "repaid_all": false,
            "text": format!("t-auto-repay-{}", Utc::now().timestamp_millis()),
        });
        let body = payload.to_string();
        gate_rest_post(&self.client, &self.creds, UNIFIED_LOANS_PATH, &body)
            .await
            .map_err(|e| anyhow!("POST {} 失败: {:#}", UNIFIED_LOANS_PATH, e))
    }
}

#[async_trait]
impl Repayer for GateRepayer {
    fn name(&self) -> &str {
        "gate"
    }

    async fn check_and_repay(&self) {
        let body = match self.fetch_accounts().await {
            Ok(b) => b,
            Err(e) => {
                warn!("gate auto-repay: 获取账户失败 {:#}", e);
                return;
            }
        };
        let entries = match parse_balances(&body) {
            Ok(v) => v,
            Err(e) => {
                warn!("gate auto-repay: 解析 /unified/accounts 失败 {:#}", e);
                return;
            }
        };
        // 只看 borrowed > 0 的币（USDT 等没借的不进表）。
        let borrowed_entries: Vec<_> = entries.iter().filter(|e| e.borrowed > 0.0).collect();
        if borrowed_entries.is_empty() {
            info!("gate auto-repay: 无未结借头");
            return;
        }

        let decisions: Vec<RepayDecision> = borrowed_entries
            .iter()
            .map(|e| RepayDecision::from_balance(e))
            .collect();
        info!(
            "gate auto-repay tick: 共 {} 项有借头，详情:\n{}",
            decisions.len(),
            render_decisions_table(&decisions)
        );

        for decision in &decisions {
            if !decision.action.is_repay() {
                continue;
            }
            match self.submit_repay(decision).await {
                Ok((status, resp_body)) => {
                    if (200..300).contains(&status) {
                        info!(
                            "gate auto-repay 成功: currency={} amount={} status={} resp={}",
                            decision.currency,
                            format_amount(decision.repay_amount),
                            status,
                            resp_body
                        );
                    } else if looks_like_no_liability(&resp_body) {
                        info!(
                            "gate auto-repay 跳过(no liability): currency={} status={} resp={}",
                            decision.currency, status, resp_body
                        );
                    } else {
                        warn!(
                            "gate auto-repay 失败: currency={} status={} resp={}",
                            decision.currency, status, resp_body
                        );
                    }
                }
                Err(e) => warn!(
                    "gate auto-repay 网络错误: currency={} err={:#}",
                    decision.currency, e
                ),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GateBalance {
    pub currency: String,
    pub available: f64,
    pub borrowed: f64,
    pub total_liab: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RepayAction {
    Repay,
    SkipNoAvailable,
    SkipZeroAmount,
}

impl RepayAction {
    fn is_repay(&self) -> bool {
        matches!(self, RepayAction::Repay)
    }
    fn label(&self) -> &'static str {
        match self {
            RepayAction::Repay => "REPAY",
            RepayAction::SkipNoAvailable => "SKIP_NO_AVAIL",
            RepayAction::SkipZeroAmount => "SKIP_ZERO",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RepayDecision {
    pub currency: String,
    pub borrowed: f64,
    pub total_liab: f64,
    pub available: f64,
    pub repay_amount: f64,
    pub action: RepayAction,
}

impl RepayDecision {
    fn from_balance(b: &GateBalance) -> Self {
        let action = if b.available <= 0.0 {
            RepayAction::SkipNoAvailable
        } else {
            RepayAction::Repay
        };
        let repay_amount = if matches!(action, RepayAction::Repay) {
            b.total_liab.min(b.available)
        } else {
            0.0
        };
        let action = if matches!(action, RepayAction::Repay) && repay_amount <= 0.0 {
            RepayAction::SkipZeroAmount
        } else {
            action
        };
        Self {
            currency: b.currency.clone(),
            borrowed: b.borrowed,
            total_liab: b.total_liab,
            available: b.available,
            repay_amount,
            action,
        }
    }
}

pub(crate) fn parse_balances(body: &str) -> Result<Vec<GateBalance>> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| anyhow!("invalid JSON from /unified/accounts: {}", e))?;
    let map = v
        .get("balances")
        .and_then(|m| m.as_object())
        .ok_or_else(|| anyhow!("/unified/accounts missing top-level balances object"))?;
    let mut out = Vec::with_capacity(map.len());
    for (currency, payload) in map {
        if currency.is_empty() {
            continue;
        }
        let available = parse_str_or_num(payload.get("available"));
        let borrowed = parse_str_or_num(payload.get("borrowed"));
        let total_liab = parse_str_or_num(payload.get("total_liab"));
        out.push(GateBalance {
            currency: currency.clone(),
            available,
            borrowed,
            total_liab,
        });
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

fn render_decisions_table(decisions: &[RepayDecision]) -> String {
    let mut table = build_three_line_table(&[
        "currency",
        "borrowed",
        "total_liab",
        "available",
        "repay_amount",
        "action",
    ]);
    for d in decisions {
        table.add_row(Row::from(vec![
            Cell::new(&d.currency),
            Cell::new(&format_amount(d.borrowed)),
            Cell::new(&format_amount(d.total_liab)),
            Cell::new(&format_amount(d.available)),
            Cell::new(&format_amount(d.repay_amount)),
            Cell::new(d.action.label()),
        ]));
    }
    table.to_string()
}

fn format_amount(value: f64) -> String {
    // Gate 各币种精度最多到 8 位，{:.8} 既覆盖了精度也避开了 f64 高位的舍入残留。
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

    fn user_quote_snapshot() -> &'static str {
        // 用户前面贴的真实快照（节选）。涵盖 borrowed 接近 available（可还）、
        // available 为负（DOT，跳过）、无借（USDT，过滤）三类。
        r#"{
            "balances": {
                "BTC": {"available":"0.048799652952","freeze":"0","borrowed":"0.048801","total_liab":"0.048801"},
                "SOL": {"available":"39.99069095","freeze":"0","borrowed":"39.999751","total_liab":"39.999751"},
                "DOT": {"available":"-0.00008868","freeze":"0","borrowed":"0.600224","total_liab":"0.60031268"},
                "DOGE": {"available":"38371.93128605","freeze":"0","borrowed":"38376.783063","total_liab":"38376.783063"},
                "USDT": {"available":"49709.711156672587","freeze":"0","borrowed":"0","total_liab":"0"}
            }
        }"#
    }

    #[test]
    fn parse_balances_extracts_relevant_fields() {
        let entries = parse_balances(user_quote_snapshot()).expect("parse ok");
        let by_ccy: std::collections::HashMap<_, _> =
            entries.iter().map(|e| (e.currency.as_str(), e)).collect();
        assert!((by_ccy["BTC"].available - 0.048799652952).abs() < 1e-12);
        assert_eq!(by_ccy["BTC"].borrowed, 0.048801);
        assert_eq!(by_ccy["DOT"].available, -0.00008868);
        assert_eq!(by_ccy["DOT"].borrowed, 0.600224);
        assert!((by_ccy["DOT"].total_liab - 0.60031268).abs() < 1e-12);
        assert_eq!(by_ccy["USDT"].borrowed, 0.0);
    }

    #[test]
    fn decision_repays_when_available_positive() {
        let b = GateBalance {
            currency: "BTC".into(),
            available: 0.048799652952,
            borrowed: 0.048801,
            total_liab: 0.048801,
        };
        let d = RepayDecision::from_balance(&b);
        assert_eq!(d.action, RepayAction::Repay);
        // 可用 < 总负债 → 只能还 available
        assert_eq!(d.repay_amount, 0.048799652952);
    }

    #[test]
    fn decision_skips_when_available_negative() {
        let b = GateBalance {
            currency: "DOT".into(),
            available: -0.00008868,
            borrowed: 0.600224,
            total_liab: 0.60031268,
        };
        let d = RepayDecision::from_balance(&b);
        assert_eq!(d.action, RepayAction::SkipNoAvailable);
        assert_eq!(d.repay_amount, 0.0);
    }

    #[test]
    fn decision_caps_repay_at_total_liab() {
        let b = GateBalance {
            currency: "ETH".into(),
            available: 100.0,
            borrowed: 1.0,
            total_liab: 1.0,
        };
        let d = RepayDecision::from_balance(&b);
        assert_eq!(d.repay_amount, 1.0);
    }

    #[test]
    fn parse_balances_rejects_non_object() {
        assert!(parse_balances(r#"{"label":"x"}"#).is_err());
    }

    #[test]
    fn format_amount_strips_trailing_zeros() {
        assert_eq!(format_amount(1.5), "1.5");
        assert_eq!(format_amount(38376.783063), "38376.783063");
        assert_eq!(format_amount(0.0), "0");
        assert_eq!(format_amount(0.048801), "0.048801");
    }

    #[test]
    fn render_table_is_three_line_format() {
        let decisions = vec![
            RepayDecision::from_balance(&GateBalance {
                currency: "BTC".into(),
                available: 0.048799652952,
                borrowed: 0.048801,
                total_liab: 0.048801,
            }),
            RepayDecision::from_balance(&GateBalance {
                currency: "DOT".into(),
                available: -0.00008868,
                borrowed: 0.600224,
                total_liab: 0.60031268,
            }),
        ];
        let table = render_decisions_table(&decisions);
        // 三条横线 + 数据行；REPAY 与 SKIP_NO_AVAIL 都在
        assert!(table.contains("currency"));
        assert!(table.contains("BTC"));
        assert!(table.contains("REPAY"));
        assert!(table.contains("DOT"));
        assert!(table.contains("SKIP_NO_AVAIL"));
        // 三条横线 ≥ 3 条 "-----"
        assert!(table.matches("---").count() >= 3);
    }
}
