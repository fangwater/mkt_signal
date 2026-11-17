use super::{
    config::Config,
    engine::{Bars, Engine, Ledger},
    history::Dominant,
};
use cn_market_proto::{Book5, BookLevel, MinuteBar, SizeBuckets};
use std::collections::BTreeMap;
fn config() -> Config {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/cta_cn_futures_legacy.json"
    ))
    .unwrap()
}
fn minute(i: i64) -> MinuteBar {
    let close = 200_000. + (i as f64 * 0.13).sin() * 500. + i as f64 * 0.1;
    MinuteBar {
        open_time_ms: (1_000_000 + i) * 60_000,
        trading_day: 20261116,
        open: close - 3.,
        high: close + 10.,
        low: close - 12.,
        close,
        snapshot_high: close + 10.,
        snapshot_low: close - 12.,
        volume_lots: 10.,
        turnover_cny: 1000.,
        snapshot_trades: 10,
        buy_volume_lots: 5.,
        sell_volume_lots: 5.,
        buy_turnover_cny: 500.,
        sell_turnover_cny: 500.,
        buy_snapshot_trades: 5.,
        sell_snapshot_trades: 5.,
        size_buckets: Some(SizeBuckets {
            large_turnover_cny: 300. + (i % 7) as f64,
            medium_turnover_cny: 300.,
            small_turnover_cny: 400. - (i % 7) as f64,
            large_buy_cny: 150.,
            large_sell_cny: 150.,
            medium_buy_cny: 150.,
            medium_sell_cny: 150.,
            small_buy_cny: 200.,
            small_sell_cny: 200.,
            ..SizeBuckets::default()
        }),
        mid_price: Some(close + 1.),
        book: Some(Book5 {
            bids: (0..5)
                .map(|k| BookLevel {
                    price: Some(close - 1. - f64::from(k)),
                    volume_lots: Some(10. + ((i + i64::from(k)) % 11) as f64),
                })
                .collect(),
            asks: (0..5)
                .map(|k| BookLevel {
                    price: Some(close + 1. + f64::from(k)),
                    volume_lots: Some(12. + ((i + i64::from(k)) % 13) as f64),
                })
                .collect(),
        }),
        ..MinuteBar::default()
    }
}
fn hour(i: i64) -> super::engine::Bar {
    let mut h = Bars::default();
    h.push(minute(i * 60), 1.).unwrap();
    h.finish(1.).unwrap().unwrap()
}
#[test]
fn native_hour_aggregation_keeps_last_book_and_mid_and_sums_turnover() {
    let mut h = Bars::default();
    for i in 0..59 {
        assert!(h.push(minute(i), 1.).unwrap().is_empty());
    }
    let rows = h.push(minute(59), 1.).unwrap();
    assert_eq!(rows.len(), 1);
    let out = &rows[0];
    assert_eq!(out.minutes, 60);
    assert_eq!(out.trade[5], 60_000.);
    assert_eq!(out.trade[4], 600.);
    assert_eq!(out.mid, minute(59).mid_price.unwrap());
    assert_eq!(out.book, minute(59).book.unwrap());
    assert_eq!(out.end_ms, minute(59).open_time_ms + 60_000);
}
#[test]
fn session_gap_keeps_short_tail_without_inventing_minutes() {
    let mut h = Bars::default();
    h.push(minute(0), 1.).unwrap();
    h.push(minute(1), 1.).unwrap();
    let rows = h.push(minute(17), 1.).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].minutes, 2);
    assert_eq!(rows[0].end_ms, minute(1).open_time_ms + 60_000);
}
#[test]
fn missing_or_invalid_mid_is_rejected_without_using_close() {
    for mid in [None, Some(0.), Some(f64::NAN)] {
        let mut r = minute(0);
        r.mid_price = mid;
        let mut h = Bars::new(crate::cta_factor_calc::config::Interval::Minute);
        assert!(h
            .push(r, 1.)
            .err()
            .unwrap()
            .to_string()
            .contains("mid_price"));
    }
}
#[test]
fn hourly_nq_falls_back_to_close_only_when_mid_is_missing_or_invalid() {
    for mid in [None, Some(0.), Some(f64::NAN)] {
        let mut r = minute(0);
        r.mid_price = mid;
        let mut h = Bars::default();
        h.push(r.clone(), 1.).unwrap();
        assert_eq!(h.finish(1.).unwrap().unwrap().mid, r.close);
    }
}

#[test]
fn hourly_cn_transform_uses_120_and_forward_fills_without_changing_prep() {
    use crate::cta_factor_calc::{config::Interval, engine::Transform};
    let cn = super::engine::parameters(Interval::Hour, false);
    let prep = crate::cta_factor_calc::config::Parameters::for_interval(Interval::Hour, false);
    assert_eq!((cn.normalize_window, cn.normalize_min_periods), (120, 60));
    assert_eq!(
        (prep.normalize_window, prep.normalize_min_periods),
        (24, 16)
    );
    let mut domestic = Transform::new(&cn, Interval::Hour);
    let mut ordinary = Transform::new(&prep, Interval::Hour);
    let mut last = None;
    for i in 0..100 {
        last = domestic.observe_with_forward_fill(Some(i as f64)).value;
        ordinary.observe(Some(i as f64), true);
    }
    let carried = domestic.observe_with_forward_fill(None);
    assert_eq!(carried.value, last);
    assert!(carried.ready);
    assert!(!ordinary.observe(None, true).ready);
}

#[test]
fn hourly_entry_is_strict_and_exit_includes_the_boundary() {
    use super::engine::{hourly_entry, hourly_exit};
    assert_eq!(hourly_entry(0.9, 0.9, 0.1), 0.);
    assert_eq!(hourly_entry(0.901, 0.9, 0.1), 1.);
    assert_eq!(hourly_entry(0.1, 0.9, 0.1), 0.);
    assert_eq!(hourly_entry(0.099, 0.9, 0.1), -1.);
    assert!(hourly_exit(1, 0.3, 0.3, 0.7));
    assert!(!hourly_exit(1, 0.301, 0.3, 0.7));
    assert!(hourly_exit(-1, 0.7, 0.3, 0.7));
    assert!(!hourly_exit(-1, 0.699, 0.3, 0.7));
}
#[test]
fn bookless_or_unclassified_history_is_rejected_instead_of_zero_filled() {
    for bookless in [false, true] {
        let mut r = minute(0);
        if bookless {
            r.book = None
        } else {
            r.size_buckets = None
        };
        let mut h = Bars::default();
        h.push(r, 1.).unwrap();
        assert!(h.finish(1.).is_err());
    }
}
#[test]
fn preheat_keeps_factual_positions_and_decision_clock() {
    let cfg = config();
    let old = Ledger {
        contract: "sn2612".into(),
        last_decision_ms: 123,
        lots_per_rule: 1,
        flags: BTreeMap::from([("baseline_076".into(), 1)]),
        retired_contracts: vec![],
    };
    let mut e = Engine::new(&cfg, old.clone()).unwrap();
    for i in 0..400 {
        e.observe(&hour(i), "sn2612", 1., false).unwrap();
    }
    assert_eq!(e.ledger.flags, old.flags);
    assert_eq!(e.ledger.last_decision_ms, 123);
    assert_eq!(e.ledger.contract, old.contract);
    assert_eq!(e.latest.len(), 12);
    assert!(e.latest.iter().all(|r| r.target_lots.abs() <= 1));
}
#[test]
fn targets_are_absolute_integer_lots_and_retain_old_contract_clears() {
    let cfg = config();
    let e = Engine::new(
        &cfg,
        Ledger {
            contract: "sn2701".into(),
            last_decision_ms: 1,
            lots_per_rule: 1,
            flags: BTreeMap::from([
                ("baseline_076".into(), 1),
                ("baseline_110".into(), 1),
                ("baseline_037".into(), -1),
            ]),
            retired_contracts: vec!["sn2612".into()],
        },
    )
    .unwrap();
    let a = e.targets();
    assert_eq!(a, e.targets());
    assert_eq!(a["targets"][0]["target_lots"], 0);
    assert_eq!(a["targets"][1]["symbol"], "sn2701");
    assert_eq!(a["targets"][1]["target_lots"], 1);
}
#[test]
fn next_trading_day_uses_explicit_scheduled_main_once_then_requires_rest_refresh() {
    let d = Dominant {
        contract: "sn2612".into(),
        trading_day: 20261116,
        source_trading_day: 20261116,
        scheduled_next: "sn2701".into(),
    };
    assert_eq!(d.for_day(20261116).unwrap().contract, "sn2612");
    let next = d.for_day(20261117).unwrap();
    assert_eq!(next.contract, "sn2701");
    assert!(next.for_day(20261118).is_err());
    assert!(d.for_day(20261113).is_err());
}

#[test]
fn zero_trade_minutes_preserve_a_single_complete_hour() {
    let mut h = Bars::default();
    let mut out = Vec::new();
    for i in 0..60 {
        let mut row = minute(i);
        if i % 2 == 0 {
            row.volume_lots = 0.;
            row.turnover_cny = 0.;
            row.snapshot_trades = 0;
            row.buy_volume_lots = 0.;
            row.sell_volume_lots = 0.;
            row.buy_turnover_cny = 0.;
            row.sell_turnover_cny = 0.;
            row.buy_snapshot_trades = 0.;
            row.sell_snapshot_trades = 0.;
            row.size_buckets = Some(SizeBuckets::default());
        }
        out.extend(h.push(row, 1.).unwrap());
    }
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].minutes, 60);
    assert_eq!(out[0].trade[4], 300.);
    assert!(h.finish(1.).unwrap().is_none());
}

#[test]
fn live_rollover_clears_old_contract_and_duplicate_hour_cannot_advance_state() {
    let cfg = config();
    let mut e = Engine::new(
        &cfg,
        Ledger {
            contract: "sn2612".into(),
            flags: BTreeMap::from([("removed_factor".into(), 1)]),
            ..Ledger::default()
        },
    )
    .unwrap();
    for i in 0..1000 {
        e.observe(&hour(i), "sn2701", 1., false).unwrap();
    }
    assert!(e.ready());
    assert_eq!(e.targets()["targets"][0]["target_lots"], 1);
    assert!(e.observe(&hour(1000), "sn2701", 1., true).unwrap());
    assert!(!e.ledger.flags.contains_key("removed_factor"));
    let targets = e.targets();
    assert_eq!(targets["targets"][0]["symbol"], "sn2612");
    assert_eq!(targets["targets"][0]["target_lots"], 0);
    assert_eq!(targets["targets"][1]["symbol"], "sn2701");
    assert!(targets["targets"][1]["target_lots"].as_i64().unwrap().abs() <= 12);
    assert!(!e.observe(&hour(1000), "sn2701", 1., true).unwrap());
    assert_eq!(targets, e.targets());
    let restored: Ledger = serde_json::from_slice(&serde_json::to_vec(&e.ledger).unwrap()).unwrap();
    let restarted = Engine::new(&cfg, restored).unwrap();
    assert_eq!(restarted.targets(), targets);
}

fn live_fixture() -> (tempfile::TempDir, Config, Dominant) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.data_directory = dir.path().into();
    (
        dir,
        cfg,
        Dominant {
            contract: "sn2612".into(),
            trading_day: 20261117,
            source_trading_day: 20261116,
            scheduled_next: "sn2612".into(),
        },
    )
}
fn quote(ms: i64, volume: u64) -> super::live::Event {
    use cn_futures_pbs::msg::{Level, MarketDataMsg};
    let mut q = MarketDataMsg {
        exchange_ts_us: ms * 1000,
        received_us: (ms * 1000) as u64,
        trading_day: 20261117,
        action_day: 20261116,
        source_sequence: ms as u64,
        volume,
        turnover: volume as f64 * 200000.,
        last_price: 200000.,
        open_interest: 1000.,
        bid: std::array::from_fn(|i| Level {
            price: 199990. - i as f64 * 10.,
            quantity: 10,
        }),
        ask: std::array::from_fn(|i| Level {
            price: 200010. + i as f64 * 10.,
            quantity: 10,
        }),
        ..MarketDataMsg::default()
    };
    q.set_symbol(b"sn2612").unwrap();
    q.set_venue("SHFE").unwrap();
    super::live::Event::Quote(Box::new(q))
}
fn night_open() -> i64 {
    chrono::DateTime::parse_from_rfc3339("2026-11-16T21:00:00+08:00")
        .unwrap()
        .timestamp_millis()
}
#[test]
fn native_live_assembly_includes_empty_minutes_and_rejects_continuity_loss() {
    let (_dir, _cfg, main) = live_fixture();
    let start = night_open();
    let mut input = super::live::Input::new(live_context(), 1., start - 1000);
    input.observe(quote(start - 1000, 89), &main).unwrap();
    let mut hours = Vec::new();
    let mut aggregate = Bars::default();
    let mut volume = 89;
    for i in 0..=60 {
        if i != 30 {
            volume += 1;
        }
        for row in input
            .observe(quote(start + i * 60000, volume), &main)
            .unwrap()
        {
            hours.extend(aggregate.push(row, 1.).unwrap());
        }
    }
    assert_eq!(hours.len(), 1);
    assert_eq!(hours[0].minutes, 60);
    assert_eq!(hours[0].start_ms, start);
    assert_eq!(hours[0].end_ms, start + 3600000);
    assert_eq!(hours[0].trade[4], 59.);
    assert_eq!(hours[0].mid, 200000.);
    assert!(input.observe(super::live::Event::Reset, &main).is_err());
}
#[test]
fn partial_first_minute_and_stale_source_cannot_become_closed_minutes() {
    let (_dir, _cfg, main) = live_fixture();
    let start = night_open();
    let quotes = [
        quote(start + 10_000, 100),
        quote(start + 60_000, 101),
        quote(start + 90_000, 102),
    ];
    let raw = quotes
        .into_iter()
        .map(|e| match e {
            super::live::Event::Quote(q) => *q,
            _ => unreachable!(),
        })
        .collect();
    let (mut input, rows) =
        super::live::Input::from_quotes(live_context(), 1., raw, &main).unwrap();
    assert!(rows.is_empty()); // 21:00 was partial; 21:01 is still in progress.
    assert!(input.clock(start + 600_000, &main).unwrap().is_empty());
    let rows = input.observe(quote(start + 120_000, 103), &main).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].open_time_ms, start + 60_000);
    assert_eq!(rows[0].volume_lots, 2.);
}

#[test]
fn input_pause_keeps_status_available_and_cannot_be_cleared_by_rest_recovery() {
    let (_dir, cfg, main) = live_fixture();
    let mut engine = Engine::new(
        &cfg,
        Ledger {
            contract: "sn2612".into(),
            last_decision_ms: 123,
            flags: BTreeMap::from([("baseline_076".into(), 1)]),
            ..Ledger::default()
        },
    )
    .unwrap();
    for i in 0..1000 {
        engine.observe(&hour(i), "sn2612", 1., false).unwrap();
    }
    let targets = engine.targets();
    let mut health = super::Health {
        dominant: Some("REST unavailable".into()),
        ..super::Health::default()
    };
    health.pause("missing native interval".into());
    health.dominant = None;
    assert_eq!(health.error(), Some("missing native interval"));
    let state = super::status(&cfg, &main, &engine, health.error(), false);
    assert_eq!(state["phase"], "paused");
    assert_eq!(state["factor_ready"], true);
    assert_eq!(state["live_ready"], false);
    assert_eq!(state["positions"], targets);
    assert_eq!(engine.ledger.last_decision_ms, 123);
}

fn warmed_engine(cfg: &Config) -> Engine {
    let mut engine = Engine::new(
        cfg,
        Ledger {
            contract: "sn2612".into(),
            last_decision_ms: 123,
            flags: BTreeMap::from([("baseline_076".into(), 1)]),
            ..Ledger::default()
        },
    )
    .unwrap();
    for i in 0..1000 {
        engine.observe(&hour(i), "sn2612", 1., false).unwrap();
    }
    assert!(engine.ready());
    engine
}
#[test]
fn panel_changes_keep_factual_lots_until_a_new_live_decision() {
    let cfg = config();
    let engine = warmed_engine(&cfg);
    let targets = engine.targets();
    let mut changed = cfg.clone();
    changed.enabled = false;
    changed.lots_per_rule = 2;
    changed.rules.remove(0);
    let mut next = engine.reconfigure(&changed).unwrap();
    assert_eq!(next.targets(), targets);
    assert_eq!(next.ledger.last_decision_ms, 123);
    assert!(!next.observe(&hour(1000), "sn2612", 1., true).unwrap());
    assert_eq!(next.targets(), targets);
    changed.enabled = true;
    let mut next = next.reconfigure(&changed).unwrap();
    assert!(next.observe(&hour(1001), "sn2612", 1., true).unwrap());
    assert_eq!(next.ledger.lots_per_rule, 2);
    let restored: Ledger =
        serde_json::from_value(serde_json::to_value(&next.ledger).unwrap()).unwrap();
    assert_eq!(
        Engine::new(&changed, restored).unwrap().targets(),
        next.targets()
    );
}
#[test]
fn configuration_preview_save_conflicts_and_invalid_rules_preserve_the_ledger() {
    use super::panel;
    let (_dir, mut cfg, _) = live_fixture();
    let path = cfg.data_directory.join("config.json");
    super::atomic(&path, &cfg).unwrap();
    let audit = cfg.data_directory.join("audit");
    let mut engine = warmed_engine(&cfg);
    let factual = engine.targets();
    let before = std::fs::read(&path).unwrap();
    let revision = panel::revision(&cfg).unwrap();
    let mut controls = panel::Controls::from(&cfg);
    controls.entry.factor_long_quantile = 0.95;
    controls.rules[0].nq = "关闭".into();
    let request = || panel::Save {
        configuration: controls.clone(),
        expected_revision: revision.clone(),
    };
    panel::apply(
        request(),
        &Default::default(),
        false,
        &path,
        &mut cfg,
        &mut engine,
        &audit,
    )
    .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(engine.targets(), factual);
    assert!(!audit.exists());
    panel::apply(
        request(),
        &Default::default(),
        true,
        &path,
        &mut cfg,
        &mut engine,
        &audit,
    )
    .unwrap();
    assert_eq!(
        Config::load(&path).unwrap().entry.factor_long_quantile,
        0.95
    );
    assert_eq!(engine.targets(), factual);
    assert!(panel::apply(
        request(),
        &Default::default(),
        true,
        &path,
        &mut cfg,
        &mut engine,
        &audit
    )
    .is_err());
    let saved = std::fs::read(&path).unwrap();
    let mut invalid = panel::Controls::from(&cfg);
    invalid.rules[0].symbol = "RB".into();
    let revision = panel::revision(&cfg).unwrap();
    assert!(panel::apply(
        panel::Save {
            configuration: invalid,
            expected_revision: revision
        },
        &Default::default(),
        true,
        &path,
        &mut cfg,
        &mut engine,
        &audit
    )
    .is_err());
    assert_eq!(std::fs::read(&path).unwrap(), saved);
    assert_eq!(engine.targets(), factual);
    assert_eq!(
        crate::common::parameter_audit::read_batch(&audit, 0)
            .unwrap()
            .records
            .len(),
        1
    );
}
#[tokio::test]
async fn native_panel_reads_without_authorization_but_protects_configuration_writes() {
    use super::panel;
    let (_dir, cfg, _) = live_fixture();
    let catalog = std::sync::Arc::new(std::sync::RwLock::new(panel::catalog(&cfg).unwrap()));
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let app = panel::router(&cfg, catalog, tx).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();
    let page = client.get(&url).send().await.unwrap().text().await.unwrap();
    assert!(page.contains("data-native-futures=\"true\""));
    assert!(page.contains("CTA 配置中心"));
    assert_eq!(
        client
            .get(format!("{url}/api/config"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let token = std::fs::read_to_string(cfg.data_directory.join("config-access-token")).unwrap();
    let response: serde_json::Value = client
        .get(format!("{url}/api/config"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["configuration"]["symbol"], "SN");
    assert_eq!(response["configuration"]["lots_per_rule"], 1);
    assert!(response["configuration"].get("zmq_endpoint").is_none());
    let request = serde_json::json!({"configuration":response["configuration"],"expected_revision":response["revision"]});
    for path in ["/api/config", "/api/preview"] {
        assert_eq!(
            client
                .post(format!("{url}{path}"))
                .json(&request)
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
    }
    assert_eq!(
        client
            .post(format!("{url}/api/preview"))
            .bearer_auth(&token)
            .header("origin", "http://other.test")
            .json(&request)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    server.abort();
}

fn live_context() -> super::live::Context {
    use cn_market_proto::*;
    super::live::Context::new(
        GetSizeThresholdsResponse {
            products: vec![ProductThresholds {
                product: "SN".into(),
                thresholds: vec![SizeThreshold {
                    applies_month: 202611,
                    p50_turnover_cny: 100000.,
                    p90_turnover_cny: 500000.,
                }],
            }],
        },
        GetTradingSessionsResponse {
            snapshot_as_of: "2026-11-01".into(),
            products: vec![ProductSessions {
                product: "SN".into(),
                windows: vec![SessionWindow {
                    begin_sec: 21 * 3600,
                    end_sec: 3600,
                }],
            }],
        },
    )
    .unwrap()
}
fn metadata() -> cn_market_proto::Contract {
    cn_market_proto::Contract {
        exchange: cn_market_proto::Exchange::Shfe as i32,
        product: "SN".into(),
        native_code: "sn2612".into(),
        canonical_code: "sn2612".into(),
        volume_multiple: 1.,
        price_tick: 10.,
        price_tick_source: "instrument".into(),
    }
}
#[test]
fn fixed_first_minute_handoff_succeeds_while_grpc_still_lags_the_local_tail() {
    use super::{history::History, recovery};
    let (_dir, cfg, main) = live_fixture();
    let ledger = Ledger {
        contract: "sn2612".into(),
        last_decision_ms: 123,
        flags: BTreeMap::from([("baseline_078".into(), 1)]),
        ..Ledger::default()
    };
    let mut history = History {
        contract: Some(metadata()),
        session_snapshot: "2026-11-01".into(),
        ..History::default()
    };
    for i in 0..1100 {
        let r = minute(i * 60);
        history.minutes.insert(r.open_time_ms, r);
    }
    let prefix_count = history.minutes.len();
    let input = super::live::Input::new(live_context(), 1., night_open());
    let local: Vec<_> = (0..10)
        .map(|i| {
            let mut r = minute(1100 * 60 + i);
            r.open_time_ms = night_open() + (i + 1) * 60_000;
            r.trading_day = main.trading_day;
            r
        })
        .collect();
    let anchor = local[0].open_time_ms;
    let attempt =
        |history: &History| recovery::recover(&cfg, &main, history, ledger.clone(), &input, &local);
    assert!(attempt(&history).is_err());
    // A newer matching row must never move the fixed first-minute boundary.
    history
        .minutes
        .insert(local[5].open_time_ms, local[5].clone());
    assert!(attempt(&history)
        .err()
        .unwrap()
        .to_string()
        .contains("first ZMQ minute"));
    let mut first = local[0].clone();
    first.provisional = true;
    history.minutes.insert(anchor, first.clone());
    history.through_ms = anchor + 59_999;
    assert!(attempt(&history)
        .err()
        .unwrap()
        .to_string()
        .contains("finalized"));
    history.through_ms = anchor + 60_000;
    let mut out = attempt(&history).unwrap();
    assert_eq!(out.anchor_ms, anchor);
    assert_eq!(out.history_rows, prefix_count + local.len());
    assert_eq!(
        out.hours.last_minute_ms(),
        Some(local.last().unwrap().open_time_ms)
    );
    assert!(out.engine.ledger == ledger);
    assert!(out.engine.ready());
    let mut completed = Vec::new();
    for i in 10..60 {
        let mut r = minute(1100 * 60 + i);
        r.open_time_ms = night_open() + (i + 1) * 60_000;
        r.trading_day = main.trading_day;
        completed.extend(out.hours.push(r, 1.).unwrap());
    }
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].minutes, 60);
    assert_eq!(completed[0].start_ms, anchor);
    // Even with a later matching minute, a conflicting first minute blocks.
    first.buy_volume_lots += 1.;
    history.minutes.insert(anchor, first);
    assert!(attempt(&history)
        .err()
        .unwrap()
        .to_string()
        .contains("inputs differ"));
}
#[test]
fn equality_requires_complete_baseline_fields_not_just_a_timestamp() {
    let a = minute(0);
    for change in 0..4 {
        let mut b = a.clone();
        match change {
            0 => b.turnover_cny += 10.,
            1 => b.size_buckets.as_mut().unwrap().large_buy_cny += 1.,
            2 => b.book.as_mut().unwrap().bids[4].volume_lots = Some(100.),
            _ => b.size_buckets = None,
        }
        assert!(!super::recovery::same_minute(&a, &b));
    }
    assert!(super::recovery::same_minute(&a, &a));
}
#[test]
fn repeated_provisional_ranges_replace_revisions_and_removed_rows() {
    use cn_market_proto::{DataState, GetMinuteBarsResponse};
    let (_dir, _cfg, main) = live_fixture();
    let mut history = super::history::History::default();
    let mut r = minute(0);
    r.provisional = true;
    let ts = r.open_time_ms;
    let response = |bars| GetMinuteBarsResponse {
        contract: Some(metadata()),
        bars,
        data_state: Some(DataState {
            latest_trading_day: main.source_trading_day,
            session_snapshot_as_of: "2026-11-01".into(),
            ..DataState::default()
        }),
    };
    history
        .merge(&main, response(vec![r.clone()]), ts, ts + 60_000)
        .unwrap();
    r.close += 10.;
    history
        .merge(&main, response(vec![r.clone()]), ts, ts + 60_000)
        .unwrap();
    assert_eq!(history.minutes[&ts].close, r.close);
    history
        .merge(&main, response(vec![]), ts, ts + 60_000)
        .unwrap();
    assert!(history.minutes.is_empty());
}
#[test]
fn reconnect_clears_the_first_minute_and_cached_tail_but_keeps_targets() {
    let (_dir, cfg, main) = live_fixture();
    let mut engine = warmed_engine(&cfg);
    let targets = engine.targets();
    let mut stream = super::Stream {
        extras: Some(super::extras::Extras::new(&cfg).unwrap()),
        ..Default::default()
    };
    let mut hours = Bars::default();
    stream
        .observe(
            quote(night_open(), 100),
            &mut engine,
            &mut hours,
            &main,
            true,
        )
        .unwrap();
    stream.prepare(live_context(), 1., &main).unwrap();
    for i in 1..=4 {
        stream
            .observe(
                quote(night_open() + i * 60_000, 100 + i as u64),
                &mut engine,
                &mut hours,
                &main,
                true,
            )
            .unwrap();
    }
    assert_eq!(
        stream.minutes.first().unwrap().open_time_ms,
        night_open() + 60_000
    );
    assert_eq!(stream.minutes.len(), 3);
    assert_eq!(engine.targets(), targets);
    assert!(stream
        .observe(
            super::live::Event::Reset,
            &mut engine,
            &mut hours,
            &main,
            true
        )
        .is_err());
    assert!(stream.minutes.is_empty());
    assert!(!stream.aligned);
    assert!(stream.input.is_none());
    assert_eq!(engine.targets(), targets);
}
#[test]
fn missing_applicable_threshold_month_blocks_classification() {
    let (_dir, _cfg, main) = live_fixture();
    let mut input = super::live::Input::new(live_context(), 1., night_open());
    let next_month = chrono::DateTime::parse_from_rfc3339("2026-12-01T21:00:00+08:00")
        .unwrap()
        .timestamp_millis();
    assert!(input
        .observe(quote(next_month, 100), &main)
        .err()
        .unwrap()
        .to_string()
        .contains("202612"));
}
#[test]
fn duplicate_snapshots_are_ignored_and_conflicting_identities_block() {
    let (_dir, _cfg, main) = live_fixture();
    let mut input = super::live::Input::new(live_context(), 1., night_open());
    input.observe(quote(night_open(), 100), &main).unwrap();
    input.observe(quote(night_open(), 100), &main).unwrap();
    assert!(input
        .observe(quote(night_open(), 101), &main)
        .err()
        .unwrap()
        .to_string()
        .contains("duplicate"));
}

#[test]
fn initialized_stream_releases_startup_cache_and_requires_restart_after_source_loss() {
    let (_dir, cfg, main) = live_fixture();
    let mut engine = warmed_engine(&cfg);
    let factual = engine.targets();
    let mut stream = super::Stream {
        extras: Some(super::extras::Extras::new(&cfg).unwrap()),
        ..Default::default()
    };
    let mut hours = Bars::default();
    stream
        .observe(
            quote(night_open(), 100),
            &mut engine,
            &mut hours,
            &main,
            true,
        )
        .unwrap();
    stream.prepare(live_context(), 1., &main).unwrap();
    for i in 1..=4 {
        stream
            .observe(
                quote(night_open() + i * 60_000, 100 + i as u64),
                &mut engine,
                &mut hours,
                &main,
                true,
            )
            .unwrap();
    }
    assert!(!stream.minutes.is_empty());
    stream.activate();
    assert_eq!(stream.minutes.capacity(), 0);
    assert!(!stream.pending.has());
    assert!(stream.prepare(live_context(), 1., &main).is_err());
    stream
        .observe(
            quote(night_open() + 5 * 60_000, 105),
            &mut engine,
            &mut hours,
            &main,
            true,
        )
        .unwrap();
    assert!(stream.minutes.is_empty());
    assert!(stream
        .observe(
            super::live::Event::Reset,
            &mut engine,
            &mut hours,
            &main,
            true
        )
        .is_err());
    assert!(stream.needs_restart);
    assert!(!stream.ready());
    assert!(stream
        .observe(
            quote(night_open() + 6 * 60_000, 106),
            &mut engine,
            &mut hours,
            &main,
            true
        )
        .is_err());
    assert!(!stream.pending.has());
    assert!(stream.prepare(live_context(), 1., &main).is_err());
    assert_eq!(engine.targets(), factual);
}

#[test]
fn ordinary_next_day_reuses_initialized_parameters_and_keeps_factor_windows() {
    let (_dir, cfg, main) = live_fixture();
    let mut engine = warmed_engine(&cfg);
    let factual = engine.targets();
    let mut stream = super::Stream {
        extras: Some(super::extras::Extras::new(&cfg).unwrap()),
        ..Default::default()
    };
    let mut hours = Bars::default();
    stream
        .observe(
            quote(night_open(), 100),
            &mut engine,
            &mut hours,
            &main,
            true,
        )
        .unwrap();
    stream.prepare(live_context(), 1., &main).unwrap();
    stream.activate();
    let next = Dominant {
        trading_day: 20261118,
        ..main.clone()
    };
    let start = night_open() + 86_400_000;
    stream
        .next_day(&main, &next, start, &mut engine, &mut hours)
        .unwrap();
    for i in 0..=2 {
        let super::live::Event::Quote(mut q) = quote(start + i * 60_000, i as u64) else {
            unreachable!()
        };
        q.trading_day = next.trading_day;
        q.action_day = 20261117;
        stream
            .observe(
                super::live::Event::Quote(q),
                &mut engine,
                &mut hours,
                &next,
                true,
            )
            .unwrap();
    }
    assert!(stream.aligned);
    assert!(!stream.needs_restart);
    assert!(stream.minutes.is_empty());
    assert!(engine.ready());
    assert_eq!(engine.targets(), factual);
    assert_eq!(hours.last_minute_ms(), Some(start + 60_000));
}

#[test]
fn displayed_windows_are_the_prep_parameters_at_each_frequency() {
    let cfg = config();
    let catalog = super::panel::catalog(&cfg).unwrap();
    for (name, norm, norm_min, rank, rank_min) in [
        ("1min", 1440, 1000, 2880, 1440),
        ("5min", 288, 200, 576, 288),
        ("15min", 96, 67, 192, 96),
        ("1h", 120, 60, 48, 24),
    ] {
        let p = &catalog["window_parameters"][name];
        assert_eq!(p["factor"]["normalize_window"], norm);
        assert_eq!(p["factor"]["normalize_min_periods"], norm_min);
        assert_eq!(p["factor"]["rank_window"], rank);
        assert_eq!(p["factor"]["rank_min_periods"], rank_min);
        assert_eq!(p["factor"]["clip_zscore"], 3.0);
        assert_eq!(
            p["factor_rank_ties"],
            if name == "1h" {
                "inclusive"
            } else {
                "midpoint"
            }
        );
        assert_eq!(p["nq"]["lookback"], 120);
        assert_eq!(p["nq"]["rank_window"], 120);
        assert_eq!(p["nq"]["rank_min_periods"], 60);
    }
}

#[test]
fn minute_frequencies_use_clock_aligned_bars_and_keep_hourly_convention() {
    use crate::cta_factor_calc::config::Interval;
    for (interval, expected, minutes_per_bar) in [
        (Interval::Minute, 30, 1),
        (Interval::FiveMinutes, 6, 5),
        (Interval::FifteenMinutes, 2, 15),
        (Interval::Hour, 0, 60),
    ] {
        let mut aggregate = Bars::new(interval);
        let mut output = Vec::new();
        // 1,000,005 is divisible by both 5 and 15.
        for i in 5..35 {
            output.extend(aggregate.push(minute(i), 1.).unwrap());
        }
        assert_eq!(output.len(), expected);
        for bar in output {
            assert_eq!(bar.minutes, minutes_per_bar);
        }
    }
    let mut five = Bars::new(Interval::FiveMinutes);
    let mut output = Vec::new();
    for i in 6..15 {
        output.extend(five.push(minute(i), 1.).unwrap());
    }
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].start_ms, minute(10).open_time_ms);
}

#[test]
fn new_frequencies_default_off_and_load_separate_fact_ledgers() {
    use crate::cta_factor_calc::config::Interval;
    let (_dir, cfg, _) = live_fixture();
    let old = Ledger {
        contract: "sn2612".into(),
        last_decision_ms: 99,
        flags: BTreeMap::from([("baseline_076".into(), -1)]),
        ..Ledger::default()
    };
    super::atomic(&cfg.data_directory.join("positions_1min.json"), &old).unwrap();
    let extras = super::extras::Extras::new(&cfg).unwrap();
    let v = extras.views(false);
    assert_eq!(v["1min"]["enabled"], false);
    assert_eq!(v["1min"]["positions"]["targets"][0]["target_lots"], -1);
    assert_eq!(
        v["5min"]["positions"]["targets"].as_array().unwrap().len(),
        0
    );
    assert_eq!(cfg.for_interval(Interval::Hour).rules.len(), 12);
    assert!(cfg.for_interval(Interval::Minute).rules.is_empty());
    assert!(cfg.frequencies.is_empty());
}

#[test]
fn extra_frequency_fault_does_not_stop_other_frequencies() {
    let (_dir, cfg, main) = live_fixture();
    let mut extras = super::extras::Extras::new(&cfg).unwrap();
    let mut row = minute(0);
    row.book = None;
    extras
        .push(row, 1., &main, &live_context(), None, false)
        .unwrap();
    let views = extras.views(false);
    assert!(views["1min"]["error"].is_string());
    assert!(views["5min"]["error"].is_null());
    assert!(views["15min"]["error"].is_null());
}

#[test]
fn configured_minute_frequency_preheats_and_persists_its_own_targets() {
    use super::panel::{self, Controls, Save};
    use crate::cta_factor_calc::config::Interval;
    let (_dir, mut cfg, main) = live_fixture();
    let path = cfg.data_directory.join("config.json");
    super::atomic(&path, &cfg).unwrap();
    let initial = Ledger {
        contract: "sn2612".into(),
        last_decision_ms: 123,
        flags: BTreeMap::from([("baseline_076".into(), 1)]),
        ..Ledger::default()
    };
    super::atomic(&cfg.data_directory.join("positions.json"), &initial).unwrap();
    let mut extras = super::extras::Extras::new(&cfg).unwrap();
    let context = live_context();
    for i in 0..4500 {
        extras
            .push(minute(i), 1., &main, &context, None, false)
            .unwrap();
    }
    let mut controls = Controls::from(&cfg.for_interval(Interval::Minute));
    controls.rules = vec![cfg
        .rules
        .iter()
        .find(|r| r.factor == "baseline_076")
        .unwrap()
        .clone()];
    controls.enabled = false;
    let request = |cfg: &Config, controls: &Controls| Save {
        configuration: controls.clone(),
        expected_revision: panel::revision(cfg).unwrap(),
    };
    let audit = cfg.data_directory.join("audit");
    panel::apply_extra(
        request(&cfg, &controls),
        &Default::default(),
        true,
        &path,
        &mut cfg,
        &mut extras,
        &audit,
    )
    .unwrap();
    assert_eq!(extras.views(false)["1min"]["factor_ready"], true);
    assert!(super::ledger(&cfg).unwrap() == initial);
    assert!(cfg.enabled);
    assert_eq!(cfg.rules.len(), 12);
    controls.enabled = true;
    panel::apply_extra(
        request(&cfg, &controls),
        &Default::default(),
        true,
        &path,
        &mut cfg,
        &mut extras,
        &audit,
    )
    .unwrap();
    let end = chrono::Utc::now().timestamp_millis().div_euclid(60_000) * 60_000;
    let mut row = minute(4501);
    row.open_time_ms = end - 60_000;
    extras
        .push(row, 1., &main, &context, Some(end - 1), true)
        .unwrap();
    extras.persist().unwrap();
    let minute_ledger = super::ledger(&cfg.for_interval(Interval::Minute)).unwrap();
    assert_eq!(minute_ledger.last_decision_ms, end);
    assert_eq!(minute_ledger.contract, "sn2612");
    assert!(super::ledger(&cfg).unwrap() == initial);
    assert!(cfg.data_directory.join("positions_1min.json").exists());
    assert!(!cfg.data_directory.join("positions_5min.json").exists());
}

#[test]
fn recovery_warms_independent_minute_ledger_without_replaying_positions() {
    use super::config::FrequencyConfig;
    use crate::cta_factor_calc::config::Interval;
    let (_dir, mut cfg, main) = live_fixture();
    cfg.enabled = false;
    cfg.frequencies.insert(
        "1min".into(),
        FrequencyConfig {
            enabled: true,
            lots_per_rule: 1,
            entry: cfg.entry.clone(),
            exits: cfg.exits.clone(),
            rules: vec![cfg
                .rules
                .iter()
                .find(|r| r.factor == "baseline_076")
                .unwrap()
                .clone()],
        },
    );
    let hourly = Ledger {
        contract: "sn2612".into(),
        last_decision_ms: 321,
        flags: BTreeMap::from([("baseline_078".into(), 1)]),
        ..Ledger::default()
    };
    let minute_ledger = Ledger {
        contract: "sn2612".into(),
        last_decision_ms: 123,
        flags: BTreeMap::from([("baseline_076".into(), -1)]),
        ..Ledger::default()
    };
    super::atomic(
        &cfg.data_directory.join("positions_1min.json"),
        &minute_ledger,
    )
    .unwrap();
    let mut history = super::history::History {
        contract: Some(metadata()),
        session_snapshot: "2026-11-01".into(),
        ..Default::default()
    };
    for i in 0..=5000 {
        let mut row = minute(i);
        row.trading_day = main.trading_day;
        history.minutes.insert(row.open_time_ms, row);
    }
    let local: Vec<_> = (5000..5010)
        .map(|i| {
            let mut row = minute(i);
            row.trading_day = main.trading_day;
            row
        })
        .collect();
    let input = super::live::Input::new(live_context(), 1., local[0].open_time_ms);
    let result =
        super::recovery::recover(&cfg, &main, &history, hourly.clone(), &input, &local).unwrap();
    assert!(result.engine.ledger == hourly);
    assert!(result.extras.ledgers()[&Interval::Minute] == minute_ledger);
    let view = result.extras.views(false);
    assert_eq!(view["1min"]["factor_ready"], true);
    assert_eq!(view["1min"]["positions"]["targets"][0]["target_lots"], -1);
    assert_eq!(result.anchor_ms, local[0].open_time_ms);
}
