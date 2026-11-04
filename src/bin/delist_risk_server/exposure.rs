//! Read-only FR account exposure checks. Values come from account-monitor-derived
//! Viz snapshots, not strategy targets. This module never submits trading actions.

use anyhow::{bail, ensure, Context, Result};
use futures_util::stream::{self, StreamExt};
use mkt_signal::common::delist_accounts::{AccountSpec, RedisSite};
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

#[derive(Clone, Debug, Serialize)]
pub struct Settings {
    pub interval_secs: u64,
    pub max_age_secs: u64,
    pub persistence_secs: u64,
    pub asset_limit_usdt: f64,
    pub account_limit_usdt: f64,
    pub catalog_max_age_secs: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            interval_secs: 10,
            max_age_secs: 90,
            persistence_secs: 30,
            asset_limit_usdt: 100.0,
            account_limit_usdt: 500.0,
            catalog_max_age_secs: 180,
        }
    }
}

impl Settings {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.interval_secs > 0 && self.interval_secs <= 60,
            "exposure interval must be 1..60 seconds"
        );
        ensure!(
            self.max_age_secs >= self.interval_secs && self.max_age_secs <= 600,
            "exposure max age must be between interval and 600 seconds"
        );
        ensure!(
            self.persistence_secs <= 3600,
            "exposure persistence must be at most 3600 seconds"
        );
        for limit in [self.asset_limit_usdt, self.account_limit_usdt] {
            ensure!(
                limit.is_finite() && limit > 0.0,
                "exposure limits must be finite and positive"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Row {
    pub asset: String,
    pub open_qty: f64,
    pub hedge_qty: f64,
    pub net_qty: f64,
    pub open_usdt: Option<f64>,
    pub hedge_usdt: Option<f64>,
    pub net_usdt: Option<f64>,
    pub pending_hedge_qty: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Breach {
    pub asset: Option<String>,
    pub since_ms: i64,
    pub last_observed_ms: i64,
    pub confirmed: bool,
    pub value_usdt: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Account {
    pub slug: String,
    pub alias: String,
    pub exchange: String,
    pub host: String,
    pub checked_ms: i64,
    pub snapshot_ms: Option<i64>,
    pub account_ms: Option<i64>,
    pub data_status: &'static str,
    pub risk_status: &'static str,
    pub complete: bool,
    pub abs_usdt: Option<f64>,
    pub net_usdt: Option<f64>,
    pub largest_asset: Option<String>,
    pub largest_usdt: Option<f64>,
    pub rows: Vec<Row>,
    pub errors: Vec<String>,
    pub breaches: BTreeMap<String, Breach>,
}

impl Account {
    fn new(spec: &AccountSpec) -> Self {
        Self {
            slug: spec.slug.clone(),
            alias: spec.alias.clone(),
            exchange: spec.exchange.clone(),
            host: spec.host.clone(),
            checked_ms: 0,
            snapshot_ms: None,
            account_ms: None,
            data_status: "unavailable",
            risk_status: "unknown",
            complete: false,
            abs_usdt: None,
            net_usdt: None,
            largest_asset: None,
            largest_usdt: None,
            rows: Vec::new(),
            errors: vec!["等待首次检查".into()],
            breaches: BTreeMap::new(),
        }
    }

    fn classify(&mut self) {
        self.risk_status = if self.breaches.values().any(|b| b.confirmed) {
            "alert"
        } else if !self.breaches.is_empty() {
            "pending"
        } else if self.complete {
            "ok"
        } else {
            "unknown"
        };
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Report {
    pub as_of_ms: i64,
    pub checked_ms: i64,
    pub catalog_current: bool,
    pub settings: Settings,
    pub accounts: Vec<Account>,
    pub errors: Vec<String>,
}

pub struct Monitor {
    settings: Settings,
    checked_ms: i64,
    accounts: BTreeMap<String, Account>,
}

impl Monitor {
    pub fn new(settings: Settings) -> Self {
        Self {
            settings,
            checked_ms: 0,
            accounts: BTreeMap::new(),
        }
    }

    pub fn apply(
        &mut self,
        specs: &[AccountSpec],
        samples: Vec<(String, Result<Sample>)>,
        now: i64,
        catalog_current: bool,
    ) {
        let wanted: BTreeSet<_> = specs
            .iter()
            .filter(|s| s.kind == "funding_rate")
            .map(|s| s.slug.clone())
            .collect();
        if catalog_current {
            self.accounts.retain(|slug, _| wanted.contains(slug));
        }
        for spec in specs.iter().filter(|s| s.kind == "funding_rate") {
            let account = self
                .accounts
                .entry(spec.slug.clone())
                .or_insert_with(|| Account::new(spec));
            account.alias = spec.alias.clone();
            account.exchange = spec.exchange.clone();
            account.host = spec.host.clone();
        }
        for (slug, result) in samples {
            let Some(account) = self.accounts.get_mut(&slug) else {
                continue;
            };
            account.checked_ms = now;
            match result {
                Err(err) => {
                    account.complete = false;
                    account.data_status = "unavailable";
                    account.errors = vec![format!("{err:#}")];
                    // Keep the last values and all prior risk evidence.
                }
                Ok(sample) => {
                    let advanced = account.snapshot_ms.is_none_or(|ts| sample.snapshot_ms > ts);
                    account.snapshot_ms = Some(sample.snapshot_ms);
                    account.account_ms = Some(sample.account_ms);
                    account.complete = sample.errors.is_empty();
                    account.data_status = if account.complete { "fresh" } else { "partial" };
                    account.abs_usdt = Some(
                        sample
                            .rows
                            .iter()
                            .filter_map(|r| r.net_usdt)
                            .map(f64::abs)
                            .sum(),
                    );
                    account.net_usdt = Some(sample.rows.iter().filter_map(|r| r.net_usdt).sum());
                    let largest =
                        sample
                            .rows
                            .iter()
                            .filter(|r| r.net_usdt.is_some())
                            .max_by(|a, b| {
                                a.net_usdt
                                    .unwrap()
                                    .abs()
                                    .total_cmp(&b.net_usdt.unwrap().abs())
                            });
                    account.largest_asset = largest.map(|r| r.asset.clone());
                    account.largest_usdt = largest.and_then(|r| r.net_usdt);
                    let mut above = BTreeMap::new();
                    for row in &sample.rows {
                        if let Some(value) = row
                            .net_usdt
                            .filter(|v| v.abs() > self.settings.asset_limit_usdt)
                        {
                            above.insert(
                                format!("asset:{}", row.asset),
                                (Some(row.asset.clone()), value),
                            );
                        }
                    }
                    if account.abs_usdt.unwrap_or(0.0) > self.settings.account_limit_usdt {
                        above.insert("total".into(), (None, account.abs_usdt.unwrap()));
                    }
                    if account.complete && advanced {
                        account.breaches.retain(|key, _| above.contains_key(key));
                    }
                    for (key, (asset, value)) in above {
                        if !advanced {
                            continue;
                        }
                        let breach = account.breaches.entry(key).or_insert(Breach {
                            asset,
                            since_ms: now,
                            last_observed_ms: now,
                            confirmed: false,
                            value_usdt: value,
                        });
                        // A gap in observations cannot prove continuous excess.
                        if !breach.confirmed
                            && now.saturating_sub(breach.last_observed_ms)
                                > (self.settings.interval_secs * 3) as i64 * 1000
                        {
                            breach.since_ms = now;
                        }
                        breach.last_observed_ms = now;
                        breach.value_usdt = value;
                        breach.confirmed |= now.saturating_sub(breach.since_ms)
                            >= self.settings.persistence_secs as i64 * 1000;
                    }
                    account.rows = sample.rows;
                    account.errors = sample.errors;
                }
            }
            account.classify();
        }
        self.checked_ms = now;
    }

    pub fn report(&self, now: i64, catalog_current: bool) -> Report {
        let mut accounts: Vec<_> = self.accounts.values().cloned().collect();
        for account in &mut accounts {
            let stale = [account.snapshot_ms, account.account_ms]
                .into_iter()
                .flatten()
                .any(|t| !fresh(t, now, self.settings.max_age_secs));
            let stopped = account.checked_ms == 0
                || now.saturating_sub(account.checked_ms)
                    > (self.settings.interval_secs * 3) as i64 * 1000;
            if stale || stopped || !catalog_current {
                account.complete = false;
                account.data_status = "stale";
                account.errors.push(
                    if !catalog_current {
                        "账户目录不可用或已过期"
                    } else {
                        "检查结果或账户数据已过期"
                    }
                    .into(),
                );
                account.classify();
            }
        }
        accounts.sort_by(|a, b| {
            fn rank(a: &Account) -> u8 {
                match a.risk_status {
                    "alert" => 0,
                    "pending" => 1,
                    "unknown" => 2,
                    _ => 3,
                }
            }
            rank(a).cmp(&rank(b)).then_with(|| a.slug.cmp(&b.slug))
        });
        let errors = if !catalog_current {
            vec!["NAV 账户目录不可用，不能确认账户覆盖完整".into()]
        } else if accounts.is_empty() {
            vec!["账户目录中没有 FR 账户".into()]
        } else {
            Vec::new()
        };
        Report {
            as_of_ms: now,
            checked_ms: self.checked_ms,
            catalog_current,
            settings: self.settings.clone(),
            accounts,
            errors,
        }
    }
}

pub struct Sample {
    snapshot_ms: i64,
    account_ms: i64,
    rows: Vec<Row>,
    errors: Vec<String>,
}

fn fresh(ts: i64, now: i64, max_age_secs: u64) -> bool {
    ts > 0
        && ts <= now.saturating_add(5_000)
        && now.saturating_sub(ts) <= max_age_secs as i64 * 1000
}

fn entry<'a>(snapshot: &'a Value, channel: &str) -> Result<&'a Value> {
    let entries = snapshot["entries"].as_array().context("快照缺少 entries")?;
    let mut matches = entries.iter().filter(|e| {
        e.get("channel")
            .or_else(|| e.get("type"))
            .and_then(Value::as_str)
            == Some(channel)
    });
    let found = matches.next().with_context(|| format!("缺少 {channel}"))?;
    ensure!(matches.next().is_none(), "重复 {channel}");
    found.get("entry").context("快照缺少 entry")
}

fn number(value: &Value, name: &str) -> Result<f64> {
    value[name]
        .as_f64()
        .filter(|v| v.is_finite())
        .with_context(|| format!("缺少或无效的 {name}"))
}

fn parse_row(value: &Value) -> Result<Row> {
    let asset = value["asset"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .context("币种为空")?
        .to_ascii_uppercase();
    let open_qty = number(value, "open_qty")?;
    let hedge_qty = number(value, "hedge_qty")?;
    let net_qty = number(value, "net_qty")?;
    let open = number(value, "open_usdt")?;
    let hedge = number(value, "hedge_usdt")?;
    let net = number(value, "net_usdt")?;
    for (sum, actual) in [(open_qty + hedge_qty, net_qty), (open + hedge, net)] {
        ensure!(
            (sum - actual).abs() <= 1e-7 * sum.abs().max(actual.abs()).max(1.0),
            "{asset} 两腿合计与净敞口不一致"
        );
    }
    let mut priced = true;
    for (qty, usd) in [(open_qty, open), (hedge_qty, hedge)] {
        if qty.abs() > 1e-12 && usd == 0.0 {
            priced = false;
        }
        ensure!(
            qty == 0.0 && usd == 0.0
                || qty != 0.0 && (usd == 0.0 || (usd / qty).is_finite() && usd / qty > 0.0),
            "{asset} 持仓估值无效"
        );
    }
    Ok(Row {
        asset,
        open_qty,
        hedge_qty,
        net_qty,
        open_usdt: priced.then_some(open),
        hedge_usdt: priced.then_some(hedge),
        net_usdt: priced.then_some(net),
        pending_hedge_qty: value["arb_pending_hedge_qty"]
            .as_f64()
            .filter(|v| v.is_finite()),
    })
}

fn parse(snapshot: &Value, now: i64, settings: &Settings) -> Result<Sample> {
    let exposure = entry(snapshot, "pre_trade_exposure")?;
    let risk = entry(snapshot, "pre_trade_risk")?;
    let timestamp = |v: &Value| -> Result<i64> {
        let ts = v["ts_ms"].as_i64().context("缺少数据时间")?;
        ensure!(
            fresh(ts, now, settings.max_age_secs),
            "数据已过期或时间异常（ts_ms={ts}）"
        );
        Ok(ts)
    };
    let snapshot_ms = timestamp(exposure)?.min(timestamp(risk)?);
    let risks = risk["account_risks"]
        .as_array()
        .filter(|a| !a.is_empty())
        .context("缺少账户监控时间，无法确认持仓新鲜度")?;
    let mut account_ms = now;
    for r in risks {
        account_ms = account_ms.min(timestamp(r).context("账户监控")?);
    }
    let values = exposure["rows"].as_array().context("缺少敞口行")?;
    let mut rows = Vec::new();
    let mut errors = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        if value["is_total"].as_bool() == Some(true) {
            continue;
        }
        match parse_row(value) {
            Ok(row) => {
                if !seen.insert(row.asset.clone()) {
                    errors.push(format!("{} 币种重复", row.asset));
                    continue;
                }
                if row.net_usdt.is_none() {
                    errors.push(format!("{} 有持仓但缺少有效价格", row.asset));
                }
                rows.push(row);
            }
            Err(err) => errors.push(format!(
                "{}: {err:#}",
                value["asset"].as_str().unwrap_or("未知币种")
            )),
        }
    }
    if rows.is_empty() && number(risk, "total_position")? > 1.0 {
        errors.push("账户有持仓但敞口行为空".into());
    }
    ensure!(
        rows.iter()
            .filter_map(|r| r.net_usdt)
            .map(f64::abs)
            .sum::<f64>()
            .is_finite(),
        "敞口合计数值无效"
    );
    rows.sort_by(|a, b| {
        b.net_usdt
            .unwrap_or(f64::INFINITY)
            .abs()
            .total_cmp(&a.net_usdt.unwrap_or(f64::INFINITY).abs())
            .then_with(|| a.asset.cmp(&b.asset))
    });
    Ok(Sample {
        snapshot_ms,
        account_ms,
        rows,
        errors,
    })
}

pub async fn collect(
    client: &Client,
    base: &str,
    specs: &[AccountSpec],
    settings: &Settings,
) -> Vec<(String, Result<Sample>)> {
    let specs: Vec<_> = specs
        .iter()
        .filter(|s| s.kind == "funding_rate")
        .cloned()
        .collect();
    stream::iter(specs)
        .map(|spec| async move {
            let result = async {
                if spec.site == RedisSite::Unsupported {
                    bail!("账户主机不受支持");
                }
                let mut url = reqwest::Url::parse(base).context("无效的快照地址")?;
                url.path_segments_mut()
                    .map_err(|_| anyhow::anyhow!("无效的快照路径"))?
                    .pop_if_empty()
                    .push("fr")
                    .push(&spec.slug)
                    .push("snapshot");
                // One slow account cannot hold up the remaining accounts or the UI.
                let response = client
                    .get(url)
                    .timeout(Duration::from_secs(5))
                    .send()
                    .await
                    .map_err(|_| anyhow::anyhow!("读取持仓快照失败或超时"))?;
                let status = response.status();
                ensure!(status.is_success(), "持仓快照 HTTP {status}");
                let snapshot = response
                    .json::<Value>()
                    .await
                    .map_err(|_| anyhow::anyhow!("解析持仓快照失败"))?;
                parse(&snapshot, chrono::Utc::now().timestamp_millis(), settings)
            }
            .await;
            (spec.slug, result)
        })
        .buffer_unordered(16)
        .collect()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: i64 = 1_790_000_000_000;

    fn spec(slug: &str, site: RedisSite) -> AccountSpec {
        AccountSpec {
            slug: slug.into(),
            alias: slug.into(),
            exchange: "gate".into(),
            kind: "funding_rate".into(),
            host: "test".into(),
            site,
            config_url: String::new(),
        }
    }

    fn row(asset: &str, net: f64) -> Value {
        json!({"asset":asset,"open_qty":1000.0+net,"hedge_qty":-1000.0,
            "open_usdt":1000.0+net,"hedge_usdt":-1000.0,"net_qty":net,"net_usdt":net,"is_total":false})
    }

    fn snapshot(rows: Vec<Value>, now: i64) -> Value {
        json!({"entries":[
            {"channel":"pre_trade_exposure","entry":{"ts_ms":now,"rows":rows}},
            {"channel":"pre_trade_risk","entry":{"ts_ms":now,"account_risks":[{"ts_ms":now}],"total_position":0.0}}
        ]})
    }

    fn apply(m: &mut Monitor, rows: Vec<Value>, now: i64) {
        let sample = parse(&snapshot(rows, now), now, &m.settings).unwrap();
        m.apply(
            &[spec("gate_fr_arb04", RedisSite::Jp)],
            vec![("gate_fr_arb04".into(), Ok(sample))],
            now,
            true,
        );
    }

    fn account(m: &Monitor, now: i64) -> Account {
        m.report(now, true).accounts.remove(0)
    }

    #[test]
    fn absolute_risk_does_not_cancel_assets_or_include_total_row() {
        let mut m = Monitor::new(Settings::default());
        apply(
            &mut m,
            vec![
                row("BTC", 300.0),
                row("ETH", -300.0),
                json!({"asset":"TOTAL","is_total":true,"net_usdt":0.0}),
            ],
            NOW,
        );
        let a = account(&m, NOW);
        assert_eq!(a.abs_usdt, Some(600.0));
        assert_eq!(a.net_usdt, Some(0.0));
        assert_eq!(a.breaches.len(), 3);
        assert_eq!(a.rows.len(), 2);
    }

    #[test]
    fn transient_recovers_and_sustained_excess_confirms_then_clears() {
        let mut m = Monitor::new(Settings::default());
        apply(&mut m, vec![row("BTC", 160.0)], NOW);
        assert_eq!(account(&m, NOW).risk_status, "pending");
        apply(&mut m, vec![row("BTC", 1.0)], NOW + 10_000);
        assert_eq!(account(&m, NOW + 10_000).risk_status, "ok");
        for t in [20_000, 30_000, 40_000, 50_000] {
            apply(&mut m, vec![row("BTC", 160.0)], NOW + t);
        }
        assert_eq!(account(&m, NOW + 50_000).risk_status, "alert");
        apply(&mut m, vec![row("BTC", 1.0)], NOW + 60_000);
        assert!(account(&m, NOW + 60_000).breaches.is_empty());
    }

    #[test]
    fn different_assets_do_not_inherit_each_others_duration() {
        let mut m = Monitor::new(Settings::default());
        for t in [0, 10_000, 20_000] {
            apply(&mut m, vec![row("BTC", 160.0)], NOW + t);
        }
        apply(&mut m, vec![row("ETH", 160.0)], NOW + 30_000);
        let a = account(&m, NOW + 30_000);
        assert_eq!(a.risk_status, "pending");
        assert_eq!(a.breaches["asset:ETH"].since_ms, NOW + 30_000);
    }

    #[test]
    fn cumulative_small_residuals_trigger_account_limit() {
        let mut m = Monitor::new(Settings::default());
        for t in [0, 10_000, 20_000, 30_000] {
            apply(
                &mut m,
                (0..6).map(|i| row(&format!("COIN{i}"), 90.0)).collect(),
                NOW + t,
            );
        }
        let a = account(&m, NOW + 30_000);
        assert_eq!(a.abs_usdt, Some(540.0));
        assert_eq!(a.risk_status, "alert");
        assert_eq!(a.breaches.len(), 1);
        assert!(a.breaches["total"].confirmed);
    }

    #[test]
    fn failed_or_partial_refresh_never_clears_confirmed_risk() {
        let mut m = Monitor::new(Settings::default());
        for t in [0, 10_000, 20_000, 30_000] {
            apply(&mut m, vec![row("BTC", 160.0)], NOW + t);
        }
        m.apply(
            &[],
            vec![("gate_fr_arb04".into(), Err(anyhow::anyhow!("timeout")))],
            NOW + 40_000,
            false,
        );
        let a = account(&m, NOW + 40_000);
        assert_eq!(a.risk_status, "alert");
        assert!(!a.complete);
        assert_eq!(a.abs_usdt, Some(160.0));
        apply(
            &mut m,
            vec![row("BTC", 0.0), json!({"asset":"BAD"})],
            NOW + 50_000,
        );
        assert_eq!(account(&m, NOW + 50_000).risk_status, "alert");
        apply(&mut m, vec![row("BTC", 0.0)], NOW + 60_000);
        assert_eq!(account(&m, NOW + 60_000).risk_status, "ok");
    }

    #[test]
    fn unpriced_holdings_are_unknown_even_when_qty_nets_to_zero() {
        let mut m = Monitor::new(Settings::default());
        let mut r = row("UNPRICED", 0.0);
        r["open_usdt"] = json!(0);
        r["hedge_usdt"] = json!(0);
        apply(&mut m, vec![r], NOW);
        let a = account(&m, NOW);
        assert!(!a.complete);
        assert_eq!(a.data_status, "partial");
        assert_eq!(a.risk_status, "unknown");
        assert_eq!(a.rows[0].net_usdt, None);
        assert!(a.errors[0].contains("UNPRICED"));
    }

    #[test]
    fn rejects_stale_future_missing_and_duplicate_channels() {
        let settings = Settings::default();
        for path in [
            "/entries/0/entry/ts_ms",
            "/entries/1/entry/ts_ms",
            "/entries/1/entry/account_risks/0/ts_ms",
        ] {
            for time in [json!(NOW - 90_001), json!(NOW + 5_001), Value::Null] {
                let mut d = snapshot(vec![], NOW);
                *d.pointer_mut(path).unwrap() = time;
                assert!(parse(&d, NOW, &settings).is_err(), "{path}");
            }
        }
        let mut d = snapshot(vec![], NOW);
        d["entries"][1]["entry"]["account_risks"] = json!([]);
        assert!(parse(&d, NOW, &settings).is_err());
        d["entries"] = json!([]);
        assert!(parse(&d, NOW, &settings).is_err());
        let mut d = snapshot(vec![], NOW);
        let extra = d["entries"][0].clone();
        d["entries"].as_array_mut().unwrap().push(extra);
        assert!(parse(&d, NOW, &settings).is_err());
    }

    #[test]
    fn malformed_rows_duplicates_and_empty_coverage_are_not_healthy() {
        for rows in [
            vec![row("BTC", 0.0), row("btc", 0.0)],
            vec![json!({"asset":"BAD"})],
            {
                let mut bad = row("BTC", 5.0);
                bad["net_usdt"] = json!(900);
                vec![bad]
            },
        ] {
            let s = parse(&snapshot(rows, NOW), NOW, &Settings::default()).unwrap();
            assert!(!s.errors.is_empty());
        }
        let mut d = snapshot(vec![], NOW);
        assert!(parse(&d, NOW, &Settings::default())
            .unwrap()
            .errors
            .is_empty());
        d["entries"][1]["entry"]["total_position"] = json!(2000);
        assert!(!parse(&d, NOW, &Settings::default())
            .unwrap()
            .errors
            .is_empty());
    }

    #[test]
    fn cached_snapshot_cannot_confirm_duration_and_observation_gap_resets_pending() {
        let mut m = Monitor::new(Settings::default());
        apply(&mut m, vec![row("BTC", 160.0)], NOW);
        let old = parse(
            &snapshot(vec![row("BTC", 160.0)], NOW),
            NOW + 30_000,
            &m.settings,
        )
        .unwrap();
        m.apply(
            &[spec("gate_fr_arb04", RedisSite::Jp)],
            vec![("gate_fr_arb04".into(), Ok(old))],
            NOW + 30_000,
            true,
        );
        assert_eq!(account(&m, NOW + 30_000).risk_status, "pending");
        apply(&mut m, vec![row("BTC", 160.0)], NOW + 40_000);
        assert_eq!(
            account(&m, NOW + 40_000).breaches["asset:BTC"].since_ms,
            NOW + 40_000
        );
    }

    #[test]
    fn stopped_collector_and_failed_catalog_invalidate_healthy_result() {
        let mut m = Monitor::new(Settings::default());
        apply(&mut m, vec![row("BTC", 0.1)], NOW);
        assert_eq!(account(&m, NOW).risk_status, "ok");
        assert_eq!(account(&m, NOW + 30_001).risk_status, "unknown");
        let report = m.report(NOW, false);
        assert_eq!(report.accounts[0].risk_status, "unknown");
        assert!(!report.errors.is_empty());
        m.apply(&[], vec![], NOW + 10_000, false);
        assert_eq!(m.report(NOW + 10_000, false).accounts.len(), 1);
        m.apply(&[], vec![], NOW + 20_000, true);
        assert_eq!(m.report(NOW + 20_000, true).accounts.len(), 0);
    }

    #[tokio::test]
    async fn collector_discovers_gate04_sg_and_isolates_account_failures() {
        use axum::{extract::Path, routing::get, Json, Router};
        let app = Router::new().route(
            "/fr/:slug/snapshot",
            get(|Path(slug): Path<String>| async move {
                if slug == "broken" {
                    return Json(json!({"error":"bad snapshot"}));
                }
                Json(snapshot(
                    vec![row("BTC", 10.0)],
                    chrono::Utc::now().timestamp_millis(),
                ))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut cta = spec("cta", RedisSite::Jp);
        cta.kind = "cta".into();
        let specs = vec![
            spec("gate_fr_arb04", RedisSite::Jp),
            spec("bybit_fr_arb01", RedisSite::Sg),
            spec("broken", RedisSite::Jp),
            spec("unsupported", RedisSite::Unsupported),
            cta,
        ];
        let results = collect(
            &Client::builder().no_proxy().build().unwrap(),
            &base,
            &specs,
            &Settings::default(),
        )
        .await;
        assert_eq!(results.len(), 4);
        assert_eq!(results.iter().filter(|(_, r)| r.is_ok()).count(), 2);
        let mut m = Monitor::new(Settings::default());
        let now = chrono::Utc::now().timestamp_millis();
        m.apply(&specs, results, now, true);
        let report = m.report(now, true);
        assert_eq!(report.accounts.len(), 4);
        assert_eq!(
            report
                .accounts
                .iter()
                .filter(|a| a.risk_status == "unknown")
                .count(),
            2
        );
        assert_eq!(
            report
                .accounts
                .iter()
                .filter(|a| a.risk_status == "ok")
                .count(),
            2
        );
        server.abort();
    }

    #[test]
    fn supports_native_viz_type_and_rejects_invalid_settings() {
        let mut d = snapshot(vec![row("BTC", 1.0)], NOW);
        for entry in d["entries"].as_array_mut().unwrap() {
            entry["type"] = entry["channel"].clone();
            entry.as_object_mut().unwrap().remove("channel");
        }
        assert_eq!(parse(&d, NOW, &Settings::default()).unwrap().rows.len(), 1);
        for settings in [
            Settings {
                interval_secs: 0,
                ..Settings::default()
            },
            Settings {
                asset_limit_usdt: f64::NAN,
                ..Settings::default()
            },
            Settings {
                max_age_secs: 1,
                ..Settings::default()
            },
        ] {
            assert!(settings.validate().is_err());
        }
    }
}
