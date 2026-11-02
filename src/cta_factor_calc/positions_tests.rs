use super::*;
use crate::cta_factor_calc::{config::FactorId, engine::Field};

const TS: i64 = 1_800_000_060_000;
fn prep(interval: &str) -> Combination {
    serde_json::from_value(json!({"name":"test","venue":"binance-futures","interval":interval,
        "factor_rules":[{"标的":"ETH","nq":"关闭","符号":1,"因子":"factor_036"}],
        "signal":{"enabled":true,"max_pos_u":200,"entry":{"nq_change_enabled":false},"execution":{}}})).unwrap()
}
fn spread() -> Combination {
    let mut c = prep("1min");
    c.venue = super::super::config::Market::OkexFutures;
    c.reference_venue = Some(super::super::config::Market::BinanceFutures);
    c.factor_rules[0].venue = Some(super::super::config::Market::BinanceFutures);
    c.signal.as_mut().unwrap().spread = Some(super::super::config::SpreadFilters {
        long_quantile: 0.7,
        short_quantile: 0.3,
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
            "cta-prep-bn-1min-pos",
            "cta-prep-bn-5min-pos",
            "cta-prep-bn-15min-pos",
            "cta-spread-okx-bn-1min-pos"
        ]
    );
    assert!(spread()
        .factors()
        .unwrap()
        .contains_key("okex-futures-binance-futures-ethusdt-1min-spread"));
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
        serde_json::from_slice(&fs::read(root.join("cta-prep-bn-1min-pos.json")).unwrap()).unwrap();
    restored.validate().unwrap();
    assert_eq!(restored.targets().unwrap(), table.targets().unwrap());
    assert_eq!(restored.clock, table.clock);
    fs::remove_dir_all(root).unwrap();
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
fn spread_uses_previous_bar_and_exits_without_current_quote_or_filter() {
    let mut table = Table::new(spread(), (TS - 60000) * 1000);
    let factor = point("binance-futures-ethusdt-1min-factor_036", TS, 0.99);
    let spread_id = "okex-futures-binance-futures-ethusdt-1min-spread";
    let mut points = BTreeMap::from([
        (factor.factor_id.clone(), factor),
        (spread_id.into(), point(spread_id, TS, 0.99)),
    ]);
    let previous = BTreeMap::from([(spread_id.into(), point(spread_id, TS - 60000, 0.5))]);
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
    let previous = BTreeMap::from([(id.into(), point(id, TS - 60000, 0.5))]);
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
            serde_json::from_slice(&fs::read(root.join("cta-prep-bn-1min-pos.json")).unwrap())
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
