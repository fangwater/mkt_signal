use fr_monitor::{
    checks::{check_snapshot, Check, Issue, OrderBook},
    config::{Config, Monitor, Source},
    market::parse_quote,
    notices::Tracker,
};
use mkt_parsers::msg::mkt_msg::AskBidSpreadMsg;
use persist_manager::parquet::OrderHealthObservation;
use serde_json::{json, Value};

const NOW: i64 = 1_790_000_000_000;
fn source() -> Source {
    let cfg: Config = toml::from_str(include_str!("../../../config/fr_monitor.toml")).unwrap();
    cfg.validate().unwrap();
    cfg.sources[1].clone()
}
fn snapshot() -> Value {
    let ns = source().namespace;
    json!({"ts_ms":NOW,"entries":[
        {"type":"pre_trade_risk","namespace":ns,"ts_ms":NOW,"entry":{
            "ts_ms":NOW,"total_equity":10000.0,"leverage":1.2,"max_leverage":2.0,
            "unimmr_trigger_line":3.0,"unimmr_force_close_line":2.0,
            "account_risks":[{"ts_ms":NOW,"state":"free_trade","margin_ratio":4.0}]}},
        {"type":"pre_trade_exposure","namespace":ns,"ts_ms":NOW,"entry":{"ts_ms":NOW,"rows":[
            {"asset":"BTC","net_usdt":5.0,"is_total":false},
            {"asset":"TOTAL","net_usdt":5.0,"is_total":true}]}}
    ]})
}
fn record(id: i64, status: &str, uniform: bool) -> OrderHealthObservation {
    OrderHealthObservation {
        venue: "binance-futures".into(),
        symbol: "BTCUSDT".into(),
        client_order_id: id,
        observed_us: (NOW - 400_000) * 1000,
        status: Some(status.into()),
        uniform,
    }
}
fn check(issue: Option<Issue>, complete: bool) -> Check {
    Check {
        scope: "test/risk".into(),
        complete,
        issues: issue.into_iter().collect(),
    }
}
#[test]
fn healthy_snapshot() {
    assert!(
        check_snapshot(&snapshot(), &source(), &Monitor::default(), NOW)
            .iter()
            .all(|c| c.complete && c.issues.is_empty())
    );
}
#[test]
fn fresh_viz_does_not_mask_stale_account() {
    let mut v = snapshot();
    v["entries"][0]["entry"]["account_risks"][0]["ts_ms"] = json!(NOW - 100_000);
    let c = check_snapshot(&v, &source(), &Monitor::default(), NOW);
    assert!(!c[0].complete);
    assert!(c[1].complete);
}
#[test]
fn fresh_viz_does_not_mask_stale_pretrade() {
    let mut v = snapshot();
    v["entries"][1]["entry"]["ts_ms"] = json!(NOW - 100_000);
    let c = check_snapshot(&v, &source(), &Monitor::default(), NOW);
    assert!(!c[1].complete);
}
#[test]
fn wrong_namespace_is_unknown() {
    let mut s = source();
    s.namespace = "different".into();
    assert!(check_snapshot(&snapshot(), &s, &Monitor::default(), NOW)
        .iter()
        .all(|c| !c.complete));
}
#[test]
fn future_timestamps_are_unknown() {
    let mut v = snapshot();
    v["entries"][0]["entry"]["ts_ms"] = json!(NOW + 60_000);
    assert!(!check_snapshot(&v, &source(), &Monitor::default(), NOW)[0].complete);
}
#[test]
fn missing_and_null_risk_is_not_zero() {
    let mut v = snapshot();
    v["entries"][0]["entry"]["total_equity"] = Value::Null;
    assert!(!check_snapshot(&v, &source(), &Monitor::default(), NOW)[0].complete);
    v = snapshot();
    v["entries"][0]["entry"]["account_risks"] = json!([]);
    assert!(!check_snapshot(&v, &source(), &Monitor::default(), NOW)[0].complete);
}
#[test]
fn unimmr_forced_close_is_critical() {
    let mut v = snapshot();
    v["entries"][0]["entry"]["account_risks"][0]["margin_ratio"] = json!(1.8);
    let c = check_snapshot(&v, &source(), &Monitor::default(), NOW);
    assert_eq!(c[0].issues[0].severity, "critical");
}
#[test]
fn opposite_asset_exposures_do_not_cancel() {
    let mut v = snapshot();
    v["entries"][1]["entry"]["rows"] = json!([
        {"asset":"BTC","net_usdt":3000.0,"is_total":false},
        {"asset":"ETH","net_usdt":-3000.0,"is_total":false},
        {"asset":"TOTAL","net_usdt":0.0,"is_total":true}]);
    let c = check_snapshot(&v, &source(), &Monitor::default(), NOW);
    assert!(c[1].issues.iter().any(|i| i.key == "total"));
    assert_eq!(c[1].issues.len(), 3);
}
#[test]
fn terminal_unmatched_evidence_clears_stalled_order() {
    let mut book = OrderBook::default();
    let m = Monitor::default();
    assert_eq!(book.observe(&[record(42, "NEW", true)], &m, NOW).len(), 1);
    assert!(book
        .observe(&[record(42, "FILLED", false)], &m, NOW)
        .is_empty());
}
#[test]
fn empty_later_window_does_not_recover_unresolved_order() {
    let mut book = OrderBook::default();
    let m = Monitor::default();
    book.observe(&[record(42, "NEW", true)], &m, NOW);
    assert_eq!(book.observe(&[], &m, NOW + 4_000_000).len(), 1);
}
#[test]
fn orders_are_isolated_by_venue_and_unmatched_history_is_not_an_alarm() {
    let mut other = record(42, "FILLED", false);
    other.venue = "binance-margin".into();
    assert_eq!(
        OrderBook::default()
            .observe(&[record(42, "NEW", true), other], &Monitor::default(), NOW)
            .len(),
        1
    );
    assert!(OrderBook::default()
        .observe(&[record(44, "NEW", false)], &Monitor::default(), NOW)
        .is_empty());
}
#[test]
fn later_trade_activity_resets_stall_age() {
    let mut activity = record(42, "PARTIALLY_FILLED", false);
    activity.observed_us = NOW * 1000;
    assert!(OrderBook::default()
        .observe(
            &[record(42, "NEW", true), activity],
            &Monitor::default(),
            NOW
        )
        .is_empty());
}
#[test]
fn debounce_repeat_and_recovery() {
    let m = Monitor::default();
    let mut t = Tracker::default();
    t.update(
        &[check(Some(Issue::warning("exposure", "too large")), true)],
        NOW,
    );
    assert!(t.pending(&m, NOW).is_empty());
    let pending = t.pending(&m, NOW + 30_000);
    assert_eq!(pending.len(), 1);
    t.acknowledge(&pending, NOW + 30_000);
    assert!(t.pending(&m, NOW + 40_000).is_empty());
    assert_eq!(t.pending(&m, NOW + 330_000).len(), 1);
    t.update(&[check(None, true)], NOW + 340_000);
    let p = t.pending(&m, NOW + 340_000);
    assert!(p[0].recovery);
    t.acknowledge(&p, NOW + 340_000);
    assert!(t.active().is_empty());
}
#[test]
fn failed_delivery_remains_pending() {
    let mut t = Tracker::default();
    t.update(&[check(Some(Issue::warning("x", "problem")), true)], NOW);
    assert_eq!(t.pending(&Monitor::default(), NOW + 30_000).len(), 1);
    assert_eq!(t.pending(&Monitor::default(), NOW + 31_000).len(), 1);
}
#[test]
fn outage_is_not_recovery_and_does_not_repeat_unverified_risk() {
    let m = Monitor::default();
    let mut t = Tracker::default();
    t.update(&[check(Some(Issue::warning("x", "risk")), true)], NOW);
    t.acknowledge(&t.pending(&m, NOW + 30_000), NOW + 30_000);
    t.update(
        &[check(Some(Issue::warning("data", "offline")), false)],
        NOW + 40_000,
    );
    assert_eq!(t.active().len(), 2);
    assert!(t
        .pending(&m, NOW + 600_000)
        .iter()
        .all(|n| !n.recovery && n.key == "data"));
}
#[test]
fn severity_escalation_bypasses_repeat_delay() {
    let m = Monitor::default();
    let mut t = Tracker::default();
    let i = Issue::warning("x", "risk");
    t.update(&[check(Some(i.clone()), true)], NOW);
    t.acknowledge(&t.pending(&m, NOW + 30_000), NOW + 30_000);
    t.update(
        &[check(
            Some(Issue {
                severity: "critical".into(),
                ..i
            }),
            true,
        )],
        NOW + 31_000,
    );
    assert_eq!(t.pending(&m, NOW + 31_000).len(), 1);
}
#[test]
fn transient_issue_does_not_emit_recovery() {
    let mut t = Tracker::default();
    t.update(&[check(Some(Issue::warning("x", "risk")), true)], NOW);
    t.update(&[check(None, true)], NOW + 1000);
    assert!(t.pending(&Monitor::default(), NOW + 1000).is_empty());
}
#[test]
fn malformed_market_payload_is_safe() {
    for n in 0..128 {
        assert!(parse_quote(&vec![255; n]).is_none());
    }
    let quote =
        AskBidSpreadMsg::create("BTC_USDT".into(), NOW * 1000, 100.0, 2.0, 101.0, 2.0).to_bytes();
    assert_eq!(parse_quote(&quote), Some(("BTCUSDT".into(), NOW)));
    for n in 0..quote.len() {
        assert!(parse_quote(&quote[..n]).is_none());
    }
}
#[test]
fn external_endpoint_and_unknown_config_are_rejected() {
    let mut cfg: Config = toml::from_str(include_str!("../../../config/fr_monitor.toml")).unwrap();
    cfg.sources[0].viz_url = "http://example.com".into();
    assert!(cfg.validate().is_err());
    assert!(toml::from_str::<Config>("unknown=true\nsources=[]").is_err());
}
