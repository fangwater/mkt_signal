use std::collections::BTreeMap;

use anyhow::{ensure, Context, Result};
use persist_manager::parquet::OrderHealthObservation;
use serde::Serialize;
use serde_json::Value;

use crate::config::{Monitor, Source};

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Issue {
    pub key: String,
    pub severity: String,
    pub message: String,
}

impl Issue {
    pub fn warning(key: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            severity: "warning".into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Check {
    pub scope: String,
    /// Unknown data must never clear a previously observed risk.
    pub complete: bool,
    pub issues: Vec<Issue>,
}

impl Check {
    pub fn failed(scope: String, message: impl Into<String>) -> Self {
        Self {
            scope,
            complete: false,
            issues: vec![Issue::warning("data", message)],
        }
    }
    pub fn ok(scope: String, issues: Vec<Issue>) -> Self {
        Self {
            scope,
            complete: true,
            issues,
        }
    }
}

fn number(v: &Value, key: &str) -> Result<f64> {
    v[key]
        .as_f64()
        .filter(|n| n.is_finite())
        .with_context(|| format!("missing/invalid {key}"))
}

fn fresh(v: &Value, now: i64, stale_secs: u64) -> Result<()> {
    let ts = v["ts_ms"].as_i64().context("missing source timestamp")?;
    ensure!(
        ts > 0 && ts <= now.saturating_add(5_000),
        "invalid/future source timestamp"
    );
    ensure!(
        now.saturating_sub(ts) <= (stale_secs as i64).saturating_mul(1000),
        "source snapshot stale ({} seconds)",
        now.saturating_sub(ts) / 1000
    );
    Ok(())
}

fn entry<'a>(
    snapshot: &'a Value,
    source: &Source,
    kind: &str,
    now: i64,
    m: &Monitor,
) -> Result<&'a Value> {
    let entries = snapshot["entries"]
        .as_array()
        .context("invalid Viz snapshot entries")?;
    let matches: Vec<_> = entries
        .iter()
        .filter(|v| v["type"] == kind && v["namespace"] == source.namespace)
        .collect();
    ensure!(
        matches.len() == 1,
        "expected one {kind} for configured namespace; found {}",
        matches.len()
    );
    let value = &matches[0]["entry"];
    fresh(value, now, m.stale_secs)?;
    Ok(value)
}

pub fn check_snapshot(snapshot: &Value, s: &Source, m: &Monitor, now: i64) -> Vec<Check> {
    [
        ("risk", "pre_trade_risk"),
        ("exposure", "pre_trade_exposure"),
    ]
    .into_iter()
    .map(|(name, kind)| {
        let scope = format!("{}/{name}", s.id);
        let result = entry(snapshot, s, kind, now, m).and_then(|value| {
            if name == "risk" {
                risk(value, m, now)
            } else {
                exposure(value, s)
            }
        });
        match result {
            Ok(issues) => Check::ok(scope, issues),
            Err(e) => Check::failed(scope, format!("{} {name}: {e:#}", s.id)),
        }
    })
    .collect()
}

fn risk(v: &Value, m: &Monitor, now: i64) -> Result<Vec<Issue>> {
    let mut issues = Vec::new();
    let equity = number(v, "total_equity")?;
    if equity <= 0.0 {
        issues.push(Issue::warning(
            "equity",
            format!("账户权益非正: {equity:.2} USDT"),
        ));
    }
    let leverage = number(v, "leverage")?;
    let max = number(v, "max_leverage")?;
    if max > 0.0 && leverage > max {
        issues.push(Issue::warning(
            "leverage",
            format!("杠杆 {leverage:.3} > 上限 {max:.3}"),
        ));
    }
    let trigger = number(v, "unimmr_trigger_line")?;
    let force = number(v, "unimmr_force_close_line")?;
    let accounts = v["account_risks"]
        .as_array()
        .context("missing account risks")?;
    ensure!(!accounts.is_empty(), "no account risk snapshot received");
    for (i, account) in accounts.iter().enumerate() {
        // The outer pre-trade snapshot is regenerated even if account_monitor stops.
        fresh(account, now, m.stale_secs).with_context(|| format!("account risk #{i}"))?;
        let state = account["state"]
            .as_str()
            .context("missing account risk state")?;
        ensure!(
            matches!(
                state,
                "free_trade" | "warning" | "reduce_only" | "liquidation"
            ),
            "unknown account risk state: {state}"
        );
        let ratio = number(account, "margin_ratio")?;
        if state != "free_trade"
            || (trigger > 0.0 && ratio < trigger)
            || (force > 0.0 && ratio < force)
        {
            let critical = state == "liquidation" || (force > 0.0 && ratio < force);
            issues.push(Issue { key: format!("account-{i}"),
                severity: if critical { "critical" } else { "warning" }.into(),
                message: format!("账户风险 #{i}: {state}, UniMMR={ratio:.4}, 开仓线={trigger:.4}, 强平线={force:.4}") });
        }
    }
    Ok(issues)
}

fn exposure(v: &Value, source: &Source) -> Result<Vec<Issue>> {
    let rows = v["rows"].as_array().context("missing exposure rows")?;
    let mut issues = Vec::new();
    let mut total = 0.0;
    let mut assets = std::collections::HashSet::new();
    for row in rows {
        let is_total = row["is_total"]
            .as_bool()
            .context("missing exposure row kind")?;
        if is_total {
            continue;
        }
        let asset = row["asset"].as_str().context("missing exposure asset")?;
        ensure!(assets.insert(asset), "duplicate exposure asset {asset}");
        let net = number(row, "net_usdt")?;
        total += net.abs();
        if net.abs() > source.max_asset_exposure_usdt {
            issues.push(Issue::warning(
                format!("asset-{asset}"),
                format!(
                    "{asset} 未对冲敞口 {net:.2} USDT，阈值 {:.2}",
                    source.max_asset_exposure_usdt
                ),
            ));
        }
    }
    // Sum absolute per-asset residuals: unrelated positive/negative assets do not hedge one another.
    ensure!(total.is_finite(), "non-finite total exposure");
    if total > source.max_total_exposure_usdt {
        issues.push(Issue::warning(
            "total",
            format!(
                "绝对敞口合计 {total:.2} USDT，阈值 {:.2}",
                source.max_total_exposure_usdt
            ),
        ));
    }
    Ok(issues)
}

#[derive(Default)]
pub struct OrderBook {
    active: BTreeMap<(String, String, i64), OrderHealthObservation>,
}

impl OrderBook {
    pub fn observe(
        &mut self,
        records: &[OrderHealthObservation],
        m: &Monitor,
        now_ms: i64,
    ) -> Vec<Issue> {
        struct Order<'a> {
            latest: &'a OrderHealthObservation,
            last_us: i64,
            terminal: bool,
            uniform: bool,
        }
        let mut orders = BTreeMap::<(&str, &str, i64), Order>::new();
        for r in records {
            // Non-numeric exchange identities remain exchange evidence; don't invent strategy ownership.
            if r.client_order_id <= 0 {
                continue;
            }
            let terminal = r.status.as_deref().is_some_and(|s| {
                matches!(
                    s,
                    "FILLED"
                        | "CANCELED"
                        | "CANCELLED"
                        | "EXPIRED"
                        | "EXPIRED_IN_MATCH"
                        | "REJECTED"
                )
            });
            let o = orders
                .entry((&r.venue, &r.symbol, r.client_order_id))
                .or_insert(Order {
                    latest: r,
                    last_us: r.observed_us,
                    terminal: false,
                    uniform: false,
                });
            if r.observed_us > o.last_us {
                o.latest = r;
                o.last_us = r.observed_us;
            }
            o.terminal |= terminal;
            o.uniform |= r.uniform;
        }
        for ((venue, symbol, id), o) in orders {
            let key = (venue.to_string(), symbol.to_string(), id);
            if o.terminal {
                self.active.remove(&key);
                continue;
            }
            if o.uniform || self.active.contains_key(&key) {
                let previous = self.active.entry(key).or_insert_with(|| o.latest.clone());
                if o.last_us > previous.observed_us {
                    *previous = o.latest.clone();
                }
            }
        }
        // Retain unresolved observations across scan windows; absence is not recovery.
        // This in-memory ledger starts from the configured lookback on restart.
        let now_us = now_ms.saturating_mul(1000);
        self.active.iter().filter_map(|((venue, symbol, id), o)| {
        if o.observed_us > now_us.saturating_add(5_000_000) {
            return Some(Issue::warning(format!("clock-{venue}-{symbol}-{id}"), "订单记录时间在未来，无法判断停滞"));
        }
        let age = now_us.saturating_sub(o.observed_us) / 1_000_000;
        (age > m.order_stale_secs as i64).then(|| Issue::warning(format!("stalled-{venue}-{symbol}-{id}"),
            format!("{venue} {symbol} 订单 {id} 已 {age}s 无进展，最新状态 {}（需核对是否为预期挂单）",
                o.status.as_deref().unwrap_or("unknown"))))
    }).collect()
    }
}
