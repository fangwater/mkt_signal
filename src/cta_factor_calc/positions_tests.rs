use super::*;
use crate::cta_factor_calc::{config::FactorId, engine::Field};

const TS: i64 = 1_800_000_060_000;
fn prep(interval: &str) -> Combination {
    serde_json::from_value(json!({"name":"test","strategy_group":"prep01","venue":"binance-futures","interval":interval,
        "factor_rules":[{"标的":"ETH","nq":"关闭","符号":1,"因子":"factor_036"}],
        "signal":{"enabled":true,"max_pos_u":200,"entry":{"nq_change_enabled":false},"execution":{}}})).unwrap()
}
fn spread() -> Combination {
    let mut c = prep("1min");
    c.strategy_group = "spread01".into();
    c.venue = super::super::config::Market::OkexFutures;
    c.reference_venue = Some(super::super::config::Market::BinanceFutures);
    c.factor_rules[0].venue = Some(super::super::config::Market::BinanceFutures);
    c.signal.as_mut().unwrap().spread = Some(super::super::config::SpreadFilters {
        long_quantile: 0.3,
        short_quantile: 0.7,
    });
    c.validate().unwrap();
    c
}
fn point(id: &str, ts: i64, q: f64) -> FactorPoint {
    FactorPoint {
        factor_id: id.into(),
        ts_ms: ts,
        fields: BTreeMap::from([(
            "value".into(),
            Field {
                raw: Some(1.0),
                value: Some(1.0),
                quantile: Some(q),
                median: Some(0.5),
                ready: true,
            },
        )]),
    }
}
fn book(_: &str, _: TradingVenue) -> Option<Quote> {
    Some(Quote {
        bid: 100.0,
        ask: 100.0,
        ts: (TS + 6000) * 1000,
        ..Default::default()
    })
}

#[test]
fn worker_owns_four_named_tables_and_spread_dependencies_are_implicit() {
    let mut identities = Vec::new();
    for interval in ["1min", "5min", "15min"] {
        identities.push(prep(interval).manager_strategy_name());
    }
    identities.push(spread().manager_strategy_name());
    assert_eq!(
        identities,
        [
            "cta-prep01-bn-1min-pos",
            "cta-prep01-bn-5min-pos",
            "cta-prep01-bn-15min-pos",
            "cta-spread01-okx-bn-1min-pos"
        ]
    );
    assert!(spread()
        .factors()
        .unwrap()
        .contains_key("okex-futures-binance-futures-ethusdt-1min-spread"));
}

#[test]
fn groups_size_persist_disable_and_retire_positions_independently() {
    for (mut base, symbol_group) in [prep("1min"), spread()]
        .into_iter()
        .flat_map(|base| [(base.clone(), false), (base, true)])
    {
        if symbol_group {
            base.symbol_group = Some("top-5".into());
        }
        let mut other = base.clone();
        other.name = "second".into();
        if symbol_group {
            other.symbol_group = Some("top-20".into());
        } else {
            other.strategy_group = if base.reference_venue.is_some() {
                "spread02"
            } else {
                "prep02"
            }
            .into();
        }
        other.signal.as_mut().unwrap().max_pos_u = 500.0;
        let mut first = Table::new(base, (TS - 60000) * 1000);
        let mut second = Table::new(other, (TS - 60000) * 1000);
        let id = "binance-futures-ethusdt-1min-factor_036";
        let spread_id = "okex-futures-binance-futures-ethusdt-1min-spread";
        let points = BTreeMap::from([
            (id.into(), point(id, TS, 0.99)),
            (spread_id.into(), point(spread_id, TS, 0.99)),
        ]);
        let previous = BTreeMap::from([(spread_id.into(), point(spread_id, TS - 60000, 0.2))]);
        for table in [&mut first, &mut second] {
            table
                .observe(&points, &previous, (TS + 6000) * 1000, &book)
                .unwrap();
        }
        let target = if first.combination.reference_venue.is_some() {
            "okex-futures:ETHUSDT"
        } else {
            "ETHUSDT"
        };
        assert_eq!(first.targets().unwrap()[target]["qty"], 2.0);
        assert_eq!(second.targets().unwrap()[target]["qty"], 5.0);
        let untouched = serde_json::to_value(&second).unwrap();
        let mut disabled = first.combination.clone();
        disabled.signal.as_mut().unwrap().enabled = false;
        first.update(&disabled, (TS + 7000) * 1000).unwrap();
        assert!(!first.enabled());
        assert!(second.enabled());
        first.retire((TS + 8000) * 1000).unwrap();
        assert_eq!(first.targets().unwrap()[target]["qty"], "clear");
        assert_eq!(serde_json::to_value(&second).unwrap(), untouched);
        let root = std::env::temp_dir().join(format!("cta-multi-group-{}", uuid::Uuid::new_v4()));
        for table in [&mut first, &mut second] {
            table.save(&root, (TS + 9000) * 1000).unwrap();
            let restored: Table = serde_json::from_slice(
                &fs::read(root.join(format!("{}.json", table.strategy_name))).unwrap(),
            )
            .unwrap();
            restored.validate().unwrap();
            assert_eq!(restored.targets().unwrap(), table.targets().unwrap());
            assert_eq!(
                restored.combination.strategy_group,
                table.combination.strategy_group
            );
            assert_eq!(
                restored.combination.symbol_group,
                table.combination.symbol_group
            );
        }
        assert_ne!(first.strategy_name, second.strategy_name);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn prep_uses_live_direct_factors_and_persists_frozen_independent_positions() {
    let mut table = Table::new(prep("1min"), (TS - 60000) * 1000);
    let factor = point("binance-futures-ethusdt-1min-factor_036", TS, 0.99);
    let points = BTreeMap::from([(factor.factor_id.clone(), factor)]);
    table
        .observe(&points, &BTreeMap::new(), (TS + 6000) * 1000, &book)
        .unwrap();
    assert_eq!(table.targets().unwrap()["ETHUSDT"]["qty"], 2.0);
    let mut next = table.combination.clone();
    next.signal.as_mut().unwrap().max_pos_u = 999.0;
    table.update(&next, (TS + 10000) * 1000).unwrap();
    assert_eq!(table.targets().unwrap()["ETHUSDT"]["qty"], 2.0);
    let root = std::env::temp_dir().join(format!("cta-pos-{}", uuid::Uuid::new_v4()));
    table.save(&root, (TS + 11000) * 1000).unwrap();
    let restored: Table =
        serde_json::from_slice(&fs::read(root.join("cta-prep01-bn-1min-pos.json")).unwrap())
            .unwrap();
    restored.validate().unwrap();
    assert_eq!(restored.targets().unwrap(), table.targets().unwrap());
    assert_eq!(restored.clock, table.clock);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn removing_last_rule_keeps_the_symbol_group_identity_and_publishes_clear() {
    let mut combination = prep("1min");
    combination.symbol_group = Some("top-5".into());
    let mut table = Table::new(combination, (TS - 60000) * 1000);
    let factor = point("binance-futures-ethusdt-1min-factor_036", TS, 0.99);
    table
        .observe(
            &BTreeMap::from([(factor.factor_id.clone(), factor)]),
            &BTreeMap::new(),
            (TS + 6000) * 1000,
            &book,
        )
        .unwrap();
    assert_eq!(table.targets().unwrap()["ETHUSDT"]["qty"], 2.0);
    let mut empty = table.combination.clone();
    empty.factor_rules.clear();
    empty.validate().unwrap();
    table.update(&empty, (TS + 7000) * 1000).unwrap();
    assert_eq!(table.strategy_name, "cta-top-5-prep01-bn-1min-pos");
    assert_eq!(table.targets().unwrap()["ETHUSDT"]["qty"], "clear");
    table.validate().unwrap();
}

#[test]
fn warmup_disabled_and_generation_handover_never_replay_positions() {
    let c = prep("1min");
    let mut table = Table::new(c, (TS + 1000) * 1000);
    let factor = point("binance-futures-ethusdt-1min-factor_036", TS, 0.99);
    let points = BTreeMap::from([(factor.factor_id.clone(), factor)]);
    table
        .observe(&points, &BTreeMap::new(), (TS + 6000) * 1000, &book)
        .unwrap();
    assert_eq!(table.targets().unwrap()["ETHUSDT"]["qty"], "clear");
    table.live_after_ms = TS - 60000;
    table.combination.signal.as_mut().unwrap().enabled = false;
    table
        .observe(&points, &BTreeMap::new(), (TS + 6000) * 1000, &book)
        .unwrap();
    assert_eq!(table.targets().unwrap()["ETHUSDT"]["qty"], "clear");
    let plan = Plan::merge([table.combination.clone()]).unwrap();
    assert!(table.compatible(&plan).unwrap());
    let mut changed = plan.clone();
    changed
        .calculations
        .values_mut()
        .next()
        .unwrap()
        .parameters
        .clip_zscore = 4.0;
    assert!(!table.compatible(&changed).unwrap());
}

#[test]
fn prep_nq_uses_the_same_inclusive_q50_gate_at_all_frequencies() {
    for frequency in ["1min", "5min", "15min"] {
        let mut combination = prep(frequency);
        combination.factor_rules[0].nq = "0.50".into();
        for (factor_q, long_value, short_value, ready, expected_qty) in [
            (0.99, 0.0, -0.05, true, 2.0),
            (0.99, -0.01, -0.05, true, 0.0),
            (0.01, 0.0, -0.05, true, -2.0),
            (0.01, 0.0, -0.04, true, 0.0),
            (0.99, 0.0, -0.05, false, 0.0),
            (0.01, 0.0, -0.05, false, 0.0),
        ] {
            let mut table = Table::new(combination.clone(), (TS - 900000) * 1000);
            let factor_id = format!("binance-futures-ethusdt-{frequency}-factor_036");
            let nq_id = format!("binance-futures-ethusdt-{frequency}-nq");
            let field = |value, median, rank| Field {
                raw: Some(value),
                value: Some(value),
                median: Some(median),
                quantile: Some(rank),
                ready,
            };
            let points = BTreeMap::from([
                (factor_id.clone(), point(&factor_id, TS, factor_q)),
                (
                    nq_id.clone(),
                    FactorPoint {
                        factor_id: nq_id,
                        ts_ms: TS,
                        fields: BTreeMap::from([
                            ("long".into(), field(long_value, 0.0, 0.1)),
                            ("short".into(), field(short_value, -0.05, 0.9)),
                        ]),
                    },
                ),
            ]);
            table
                .observe(&points, &BTreeMap::new(), (TS + 6000) * 1000, &book)
                .unwrap();
            let targets = table.targets().unwrap();
            let expected = if expected_qty == 0.0 {
                json!("clear")
            } else {
                json!(expected_qty)
            };
            assert_eq!(
                targets["ETHUSDT"]["qty"], expected,
                "frequency={frequency}, factor={factor_q}, ready={ready}"
            );
        }
    }
}

#[test]
fn spread_configuration_requires_strict_previous_minute_30_70_tails() {
    let mut combination = Combination::load(std::path::Path::new(
        "config/cta_factor_spread/okex-futures_binance-futures_1min.json",
    ))
    .unwrap();
    combination.signal.as_mut().unwrap().enabled = true;
    // Isolate the spread gate; the independent NQ gate has its own tests.
    combination.factor_rules = spread().factor_rules;
    let factor_id = "binance-futures-ethusdt-1min-factor_036";
    let spread_id = "okex-futures-binance-futures-ethusdt-1min-spread";
    for (factor_q, previous_q, ready, expected_qty) in [
        (0.99, Some(0.29), true, 2.0),
        (0.99, Some(0.30), true, 0.0),
        (0.99, Some(0.50), true, 0.0),
        (0.01, Some(0.71), true, -2.0),
        (0.01, Some(0.70), true, 0.0),
        (0.01, Some(0.50), true, 0.0),
        (0.99, None, false, 0.0),
        (0.01, None, false, 0.0),
        (0.99, Some(f64::NAN), true, 0.0),
        (0.01, Some(f64::NAN), true, 0.0),
        (0.99, Some(0.20), false, 0.0),
        (0.01, Some(0.80), false, 0.0),
    ] {
        let mut table = Table::new(combination.clone(), (TS - 60000) * 1000);
        // The current-minute spread favours the opposite decision: only the
        // exact preceding minute may determine whether this entry is allowed.
        let current_q = if factor_q > 0.5 { 0.99 } else { 0.01 };
        let points = BTreeMap::from([
            (factor_id.into(), point(factor_id, TS, factor_q)),
            (spread_id.into(), point(spread_id, TS, current_q)),
        ]);
        let mut lagged = point(spread_id, TS - 60000, 0.5);
        let field = lagged.fields.get_mut("value").unwrap();
        field.quantile = previous_q;
        field.ready = ready;
        if previous_q.is_none() {
            field.raw = None;
            field.value = None;
        }
        let previous = BTreeMap::from([(spread_id.into(), lagged)]);
        table
            .observe(&points, &previous, (TS + 6000) * 1000, &book)
            .unwrap();
        assert_eq!(
            table.targets().unwrap()["okex-futures:ETHUSDT"]["qty"],
            expected_qty,
            "factor={factor_q}, previous spread={previous_q:?}, ready={ready}"
        );
    }
}

#[test]
fn spread_uses_previous_bar_and_exits_without_current_quote_or_filter() {
    let mut table = Table::new(spread(), (TS - 60000) * 1000);
    let factor = point("binance-futures-ethusdt-1min-factor_036", TS, 0.99);
    let spread_id = "okex-futures-binance-futures-ethusdt-1min-spread";
    let mut points = BTreeMap::from([
        (factor.factor_id.clone(), factor),
        (spread_id.into(), point(spread_id, TS, 0.99)),
    ]);
    let previous = BTreeMap::from([(spread_id.into(), point(spread_id, TS - 60000, 0.2))]);
    table
        .observe(&points, &previous, (TS + 6000) * 1000, &book)
        .unwrap();
    let targets = table.targets().unwrap();
    assert_eq!(targets["okex-futures:ETHUSDT"]["qty"], 2.0);
    assert_eq!(targets["binance-futures:ETHUSDT"]["qty"], 0.0);
    points.insert(
        "binance-futures-ethusdt-1min-factor_036".into(),
        point("binance-futures-ethusdt-1min-factor_036", TS + 60000, 0.1),
    );
    points.remove(spread_id);
    table
        .observe(&points, &BTreeMap::new(), (TS + 66000) * 1000, &|_, _| None)
        .unwrap();
    let targets = table.targets().unwrap();
    assert_eq!(targets["okex-futures:ETHUSDT"]["qty"], 2.0);
    assert_eq!(targets["binance-futures:ETHUSDT"]["qty"], -2.0);
    table.retire((TS + 67000) * 1000).unwrap();
    assert!(table
        .targets()
        .unwrap()
        .values()
        .all(|v| v["qty"] == "clear" && v["signal"] == 1));
}

#[test]
fn missing_spread_blocks_entry_and_factor_clocks_prevent_double_decisions() {
    let mut table = Table::new(spread(), (TS - 60000) * 1000);
    let factor = point("binance-futures-ethusdt-1min-factor_036", TS, 0.99);
    let points = BTreeMap::from([(factor.factor_id.clone(), factor)]);
    table
        .observe(&points, &BTreeMap::new(), (TS + 1000) * 1000, &book)
        .unwrap();
    assert!(table.clock.is_empty());
    table
        .observe(&points, &BTreeMap::new(), (TS + 6000) * 1000, &book)
        .unwrap();
    assert_eq!(table.targets().unwrap()["okex-futures:ETHUSDT"]["qty"], 0.0);
    let id = "okex-futures-binance-futures-ethusdt-1min-spread";
    let previous = BTreeMap::from([(id.into(), point(id, TS - 60000, 0.2))]);
    table
        .observe(&points, &previous, (TS + 6000) * 1000, &book)
        .unwrap();
    assert_eq!(table.targets().unwrap()["okex-futures:ETHUSDT"]["qty"], 0.0);
    assert_eq!(FactorId::parse(id).unwrap().factor, "spread");
}

#[tokio::test]
async fn deleted_table_is_durable_before_publication_and_retires_only_after_ack() {
    use axum::{extract::State, routing::post, Json, Router};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    let root = std::env::temp_dir().join(format!("cta-owner-{}", uuid::Uuid::new_v4()));
    let received = Arc::new(Mutex::new(Vec::<Value>::new()));
    let handler = |State((root, received)): State<(PathBuf, Arc<Mutex<Vec<Value>>>)>,
                   Json(body): Json<Value>| async move {
        let saved: Value =
            serde_json::from_slice(&fs::read(root.join("cta-prep01-bn-1min-pos.json")).unwrap())
                .unwrap();
        assert!(saved["retiring"].as_bool().unwrap());
        assert_eq!(saved["ledger"]["rows"]["factor_036"]["ETHUSDT"]["qty"], 0.0);
        received.lock().unwrap().push(body);
        axum::http::StatusCode::OK
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/api", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/api/catalog/position-strategies", post(handler))
        .with_state((root.clone(), received.clone()));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut table = Table::new(prep("1min"), (TS - 60000) * 1000);
    table.ever_enabled = true;
    let Ledger::Prep(rows) = &mut table.ledger else {
        unreachable!()
    };
    rows.insert(
        "factor_036".into(),
        BTreeMap::from([(
            "ETHUSDT".into(),
            PrepPosition {
                flag: 1,
                qty: 2.0,
                entry_notional: 200.0,
            },
        )]),
    );
    let name = table.strategy_name.clone();
    let mut owner = PositionTables {
        root: root.clone(),
        tables: BTreeMap::from([(name.clone(), table)]),
        publisher: PositionPublisher::test_endpoint(&url).unwrap(),
        worker: Worker::Perp,
        quotes_initialized: false,
    };
    let now = super::super::runtime::now_ms() * 1000;
    owner.tick(&Plan::default(), None, now).unwrap();
    assert!(!owner.tables[&name].retired);
    tokio::time::timeout(Duration::from_secs(5), async {
        while received.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        loop {
            owner
                .tick(
                    &Plan::default(),
                    None,
                    super::super::runtime::now_ms() * 1000,
                )
                .unwrap();
            if owner.tables[&name].retired {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        received.lock().unwrap()[0]["targets"]["ETHUSDT"]["qty"],
        "clear"
    );
    assert!(owner.tables[&name].retired);
    server.abort();
    drop(owner);
    fs::remove_dir_all(root).unwrap();
}
