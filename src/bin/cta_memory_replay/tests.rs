use super::*;
use mkt_signal::cta_factor_calc::engine::Field;
use model::{Candle, Point, Profile, ReplayRequest};
use std::collections::{BTreeMap, VecDeque};

const START: i64 = 1_800_000_000_000;

fn profile() -> Profile {
    Profile {
        source_path: "fixture/prep01.json".into(), source_sha256: "fixture".into(),
        combination: serde_json::from_value(json!({"name":"replay-test","strategy_group":"prep01","venue":"binance-futures","interval":"1min",
            "factor_rules":[{"标的":"BTCUSDT","因子":"baseline_040","符号":1,"nq":"关闭"}],
            "signal":{"enabled":false,"max_pos_u":100,
            "entry":{"factor_long_quantile":0.9,"factor_short_quantile":0.1,"nq_change_enabled":false,"nq_long_quantile":0.5,"nq_short_quantile":0.5},
            "execution":{"factor_exit_quantile_long":0.3,"factor_exit_quantile_short":0.7}}})).unwrap(),
    }
}

fn request(profile: &Profile) -> ReplayRequest {
    ReplayRequest {
        profile: profile.name().to_string(),
        symbol: "BTCUSDT".into(),
        factor: "baseline_040".into(),
        start_ms: Some(START),
        end_ms: Some(START + 180_000),
        entry: profile.combination.signal_config().unwrap().entry,
        execution: profile.combination.signal_config().unwrap().execution,
        max_pos_u: 100.0,
        fee_rate: 0.0,
    }
}

fn memory(profile: &Profile, qs: &[f64]) -> Memory {
    let rows = qs
        .iter()
        .enumerate()
        .map(|(i, q)| Point {
            ts_ms: START + i as i64 * 60_000,
            available_ms: START + i as i64 * 60_000 + 5,
            value: Field {
                raw: Some(*q),
                value: Some(*q),
                quantile: Some(*q),
                median: None,
                ready: true,
            },
            short: None,
        })
        .collect();
    let candles = qs
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let price = if i == 0 {
                101.0
            } else {
                99.0 + i as f64 * 10.0
            };
            Candle {
                ts_ms: START + i as i64 * 60_000,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 10.0,
                turnover: price * 10.0,
            }
        })
        .collect();
    Memory {
        points: BTreeMap::from([(profile.factor_id("BTCUSDT", "baseline_040"), rows)]),
        candles: BTreeMap::from([(("BTCUSDT".into(), 60_000), candles)]),
        latest_data_ms: START + 180_000,
        revision: 42,
        kafka_records: 100,
        caught_up: true,
        ..Memory::default()
    }
}

fn run(profile: &Profile, request: ReplayRequest, memory: &Memory) -> model::ReplayResult {
    let input = model::prepare(memory, profile, &request).unwrap();
    model::simulate(profile, request, input).unwrap()
}

#[test]
fn threshold_changes_replay_the_same_memory_without_mutating_source() {
    let profile = profile();
    let data = memory(&profile, &[0.92, 0.2]);
    let mut req = request(&profile);
    let first = run(&profile, req.clone(), &data);
    req.entry.factor_long_quantile = 0.95;
    let second = run(&profile, req, &data);
    assert_eq!(first.closed_trades, 1);
    assert_eq!(second.closed_trades, 0);
    assert!((first.realized_pnl - 100.0 / 101.0 * 8.0).abs() < 1e-9);
    assert_eq!(first.data_revision, second.data_revision);
    assert_eq!(data.kafka_records, 100);
    assert_eq!(
        data.points.values().next().unwrap()[0].value.quantile,
        Some(0.92)
    );
    assert_eq!(
        profile
            .combination
            .signal_config()
            .unwrap()
            .entry
            .factor_long_quantile,
        0.9
    );
}

#[test]
fn fills_use_next_closed_bar_vwap_and_respect_the_cutoff() {
    let profile = profile();
    let mut data = memory(&profile, &[0.99]);
    let candle = &mut data.candles.get_mut(&("BTCUSDT".into(), 60_000)).unwrap()[0];
    candle.open = 99.0;
    candle.close = 123.0;
    candle.high = 123.0;
    candle.low = 98.0;
    let mut req = request(&profile);
    let result = run(&profile, req.clone(), &data);
    assert_eq!(result.trades[0].entry_price, 101.0);
    assert_eq!(result.trades[0].entry_ms, START + 60_000);
    req.end_ms = Some(START + 30_000);
    let result = run(&profile, req, &data);
    assert!(result.trades.is_empty());
    assert!(result.decisions.is_empty());
}

#[test]
fn sign_reversal_original_qty_and_next_bar_reentry_are_preserved() {
    let mut profile = profile();
    profile.combination.factor_rules[0].weight = -1.0;
    let data = memory(&profile, &[0.01, 0.99, 0.99]);
    let result = run(&profile, request(&profile), &data);
    assert_eq!(result.closed_trades, 1);
    assert_eq!(
        result.decisions.iter().map(|r| r.flag).collect::<Vec<_>>(),
        [1, 0, -1]
    );
    assert!((result.trades[0].qty - 100.0 / 101.0).abs() < 1e-12);
    assert!((result.trades[1].qty + 100.0 / 119.0).abs() < 1e-12);
}

#[test]
fn unready_factor_holds_but_nq_never_blocks_factor_exit() {
    let profile = profile();
    let mut data = memory(&profile, &[0.99, 0.01, 0.01]);
    data.points.values_mut().next().unwrap()[1].value.ready = false;
    let mut req = request(&profile);
    req.entry.nq_change_enabled = true;
    let mut nq = Point {
        ts_ms: START,
        available_ms: START + 5,
        value: Field {
            raw: Some(0.02),
            value: Some(0.02),
            quantile: Some(0.9),
            median: Some(0.01),
            ready: true,
        },
        short: Some(Field {
            raw: Some(-0.02),
            value: Some(-0.02),
            quantile: Some(0.1),
            median: Some(-0.01),
            ready: true,
        }),
    };
    data.points.insert(
        profile.factor_id("BTCUSDT", "nq"),
        VecDeque::from([nq.clone()]),
    );
    let result = run(&profile, req.clone(), &data);
    assert_eq!(result.closed_trades, 1);
    assert_eq!(
        result.decisions.iter().map(|r| r.flag).collect::<Vec<_>>(),
        [1, 1, 0]
    );
    nq.value.quantile = Some(0.49);
    nq.value.value = Some(0.0);
    data.points
        .insert(profile.factor_id("BTCUSDT", "nq"), VecDeque::from([nq]));
    assert!(run(&profile, req, &data).trades.is_empty());
}

#[test]
fn repeated_signals_do_not_resize_and_fees_are_counted_once_per_fill() {
    let profile = profile();
    let data = memory(&profile, &[0.99, 0.99, 0.2]);
    let mut req = request(&profile);
    req.fee_rate = 0.001;
    let result = run(&profile, req, &data);
    assert_eq!(result.trades.len(), 1);
    assert_eq!(result.decisions[0].qty, result.decisions[1].qty);
    assert!((result.fees - (0.1 + 100.0 / 101.0 * 119.0 * 0.001)).abs() < 1e-9);
    assert!((result.realized_pnl - result.trades[0].pnl).abs() < 1e-9);
    assert_eq!(result.win_rate, Some(1.0));
}

#[test]
fn exact_threshold_ties_and_missing_nq_do_not_create_entries() {
    let profile = profile();
    let data = memory(&profile, &[0.9, 0.1]);
    let result = run(&profile, request(&profile), &data);
    assert!(result.trades.is_empty());
    let mut req = request(&profile);
    req.entry.nq_change_enabled = true;
    let data = memory(&profile, &[0.99, 0.01]);
    assert!(run(&profile, req, &data).trades.is_empty());
}

#[test]
fn late_or_future_nq_cannot_delay_an_existing_factor_exit() {
    let profile = profile();
    let mut data = memory(&profile, &[0.99, 0.2]);
    let long = Field {
        raw: Some(0.02),
        value: Some(0.02),
        quantile: Some(0.9),
        median: Some(0.01),
        ready: true,
    };
    let short = Field {
        raw: Some(-0.02),
        value: Some(-0.02),
        quantile: Some(0.1),
        median: Some(-0.01),
        ready: true,
    };
    data.points.insert(
        profile.factor_id("BTCUSDT", "nq"),
        VecDeque::from([
            Point {
                ts_ms: START,
                available_ms: START + 5,
                value: long.clone(),
                short: Some(short.clone()),
            },
            Point {
                ts_ms: START + 60_000,
                available_ms: START + 180_000,
                value: long,
                short: Some(short),
            },
        ]),
    );
    let mut req = request(&profile);
    req.entry.nq_change_enabled = true;
    let result = run(&profile, req.clone(), &data);
    assert_eq!(result.closed_trades, 1);
    assert_eq!(result.trades[0].exit_ms, Some(START + 120_000));
    req.end_ms = Some(START + 120_000);
    assert_eq!(run(&profile, req, &data).closed_trades, 1);
}

#[test]
fn a_fixed_cutoff_is_stable_while_new_data_is_appended() {
    let profile = profile();
    let mut data = memory(&profile, &[0.99, 0.2]);
    let mut req = request(&profile);
    req.end_ms = Some(START + 90_000);
    let before = run(&profile, req.clone(), &data);
    let mut point = data.points.values().next().unwrap()[1].clone();
    point.ts_ms = START + 120_000;
    point.available_ms = START + 120_005;
    point.value.quantile = Some(0.01);
    data.points.values_mut().next().unwrap().push_back(point);
    data.candles
        .get_mut(&("BTCUSDT".into(), 60_000))
        .unwrap()
        .push_back(Candle {
            ts_ms: START + 120_000,
            open: 119.0,
            high: 119.0,
            low: 119.0,
            close: 119.0,
            volume: 1.0,
            turnover: 119.0,
        });
    data.revision += 1;
    let after = run(&profile, req, &data);
    assert_eq!(before.realized_pnl, after.realized_pnl);
    assert_eq!(before.closed_trades, after.closed_trades);
    assert_eq!(before.decisions.len(), after.decisions.len());
}

#[test]
fn invalid_parameters_are_rejected_before_replay() {
    let profile = profile();
    let data = memory(&profile, &[0.99]);
    let mut req = request(&profile);
    req.entry.factor_short_quantile = 0.99;
    assert!(model::prepare(&data, &profile, &req).is_err());
    req = request(&profile);
    req.factor = "unknown".into();
    assert!(model::prepare(&data, &profile, &req).is_err());
    req = request(&profile);
    req.fee_rate = -0.1;
    assert!(model::prepare(&data, &profile, &req).is_err());
}

#[test]
fn pruning_drops_expired_history_and_candles_without_affecting_new_rows() {
    let profile = profile();
    let mut data = memory(&profile, &[0.99, 0.2]);
    data.candles.insert(
        ("BTCUSDT".into(), 60_000),
        VecDeque::from([Candle {
            ts_ms: START - 60_000,
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            volume: 1.0,
            turnover: 1.0,
        }]),
    );
    data.prune(START + 30_000);
    assert_eq!(data.points.values().next().unwrap().len(), 1);
    assert!(data.candles.values().next().unwrap().is_empty());
}

#[tokio::test]
async fn http_replay_keeps_bootstrap_and_memory_immutable() {
    let profile = profile();
    let app = App {
        bootstrap: Arc::new(Bootstrap {
            fetched_at_ms: START,
            source_host: "fixture".into(),
            profiles: vec![profile.clone()],
            symbol_groups: Default::default(),
            amount_thresholds: BTreeMap::new(),
            threshold_metadata: BTreeMap::new(),
        }),
        memory: Arc::new(RwLock::new(memory(&profile, &[0.92, 0.2]))),
        brokers: "fixture".into(),
        web_root: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web"),
        replay_slots: Arc::new(tokio::sync::Semaphore::new(2)),
    };
    let memory = app.memory.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router(app)).await.unwrap();
    });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let html = client.get(format!("{url}/")).send().await.unwrap();
    assert_eq!(html.status(), StatusCode::OK);
    assert!(html.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/html"));
    let catalog: Value = client
        .get(format!("{url}/api/catalog"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        catalog["profiles"][0]["symbols"][0]["factors"][0]["nq_method"],
        "linear_median"
    );
    let first: Value = client
        .post(format!("{url}/api/replay"))
        .json(&portfolio_request())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first["summary"]["closed_trades"], 1);
    let mut req = portfolio_request();
    req.thresholds = Some(portfolio::Thresholds {
        long_entry: 0.95,
        short_entry: 0.1,
        long_exit: 0.3,
        short_exit: 0.7,
    });
    let second: Value = client
        .post(format!("{url}/api/replay"))
        .json(&req)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(second["summary"]["closed_trades"], 0);
    assert_eq!(memory.read().unwrap().kafka_records, 100);
    assert_eq!(first["data_revision_start"], second["data_revision_start"]);
    assert_eq!(
        client
            .post(format!("{url}/api/replay"))
            .header("Origin", "http://other.example")
            .json(&req)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    server.abort();
}

#[test]
fn prep_groups_keep_native_definitions_and_empty_pools_without_duplicate_calculations() {
    use mkt_signal::cta_factor_calc::config::{Interval, SymbolGroups};
    let first = profile();
    let mut second = first.clone();
    second.combination.name = "top-5_prep02_binance-futures_1min".into();
    second.combination.strategy_group = "prep02".into();
    second.combination.symbol_group = Some("top-5".into());
    second.combination.signal.as_mut().unwrap().max_pos_u = 50.0;
    let mut empty = second.clone();
    empty.combination.name = "top-20_prep02_binance-futures_5min".into();
    empty.combination.symbol_group = Some("top-20".into());
    empty.combination.interval = Interval::FiveMinutes;
    empty.combination.factor_rules.clear();
    let bootstrap = Bootstrap {
        fetched_at_ms: START,
        source_host: "fixture".into(),
        profiles: vec![first.clone(), second.clone(), empty.clone()],
        symbol_groups: SymbolGroups(BTreeMap::from([
            ("top-5".into(), vec!["BTCUSDT".into()]),
            ("top-20".into(), vec!["BTCUSDT".into()]),
        ])),
        amount_thresholds: BTreeMap::new(),
        threshold_metadata: BTreeMap::new(),
    };
    let plan = bootstrap.plan().unwrap();
    assert_eq!(plan.combinations.len(), 3);
    assert_eq!(plan.calculations.len(), 2);
    assert_eq!(plan.combinations[second.name()], second.combination);
    assert_eq!(
        second
            .defaults("BTCUSDT", "baseline_040")
            .unwrap()
            .max_pos_u,
        50.0
    );
    assert!(empty.rules().unwrap().is_empty());
}

#[test]
fn repeated_factor_with_distinct_nq_remains_separate_and_uses_the_same_memory_series() {
    let mut profile = profile();
    let mut other = profile.combination.factor_rules[0].clone();
    other.nq = "0.50".into();
    profile.combination.factor_rules.push(other);
    let rules = profile.rules().unwrap().remove("BTCUSDT").unwrap();
    assert_eq!(rules.len(), 2);
    assert_ne!(rules[0].strategy, rules[1].strategy);
    let data = memory(&profile, &[0.99, 0.2]);
    let mut first = request(&profile);
    first.factor = rules[0].strategy.clone();
    let mut second = first.clone();
    second.factor = rules[1].strategy.clone();
    second.entry.nq_change_enabled = true;
    assert_eq!(run(&profile, first, &data).closed_trades, 1);
    assert!(run(&profile, second, &data).trades.is_empty());
    assert_eq!(data.points.len(), 1);
}

#[test]
fn offline_history_matches_the_native_model_and_rejects_different_definitions_or_duplicates() {
    let profile = profile();
    let data = memory(&profile, &[0.99, 0.2]);
    let expected = run(&profile, request(&profile), &data);
    let bootstrap = Bootstrap {
        fetched_at_ms: START,
        source_host: "fixture".into(),
        profiles: vec![profile.clone()],
        symbol_groups: Default::default(),
        amount_thresholds: BTreeMap::new(),
        threshold_metadata: BTreeMap::new(),
    };
    let id = profile.factor_id("BTCUSDT", "baseline_040");
    let mut input = json!({
        "source_generation":"fixture",
        "source_plan":bootstrap.plan().unwrap(),
        "points":data.points[&id].iter().map(|p| json!({
            "factor_id":id,"ts_ms":p.ts_ms,"fields":{"value":p.value}
        })).collect::<Vec<_>>(),
        "candles":data.candles[&("BTCUSDT".into(),60_000)].iter().map(|c| json!({
            "symbol":"BTCUSDT","interval_ms":60_000,"candle":c
        })).collect::<Vec<_>>(),
        "jobs":[{"label":"fixture","request":portfolio_request()}],
        "rule_details_from_ms":START+120_000
    });
    let path = std::env::temp_dir().join(format!(
        "cta-replay-offline-{}-{}.json",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, serde_json::to_vec(&input).unwrap()).unwrap();
    let output = offline::run(&bootstrap, &path).unwrap();
    assert_eq!(output.results[0].result.summary.closed_trades, 1);
    assert!((output.results[0].result.summary.pnl - expected.realized_pnl).abs() < 1e-9);
    let details = output.results[0].rule_details.as_ref().unwrap();
    assert_eq!(details.len(), 1);
    assert_eq!(details[0].result.decisions.len(), 1);
    assert_eq!(details[0].result.decisions[0].ts_ms, START + 120_000);
    assert_eq!(details[0].result.trades.len(), 1);
    assert_eq!(details[0].result.trades[0].entry_ms, START + 60_000);
    assert!((details[0].result.realized_pnl - expected.realized_pnl).abs() < 1e-9);

    let original = input["source_plan"]["calculations"][&id]["parameters"]["clip_zscore"].clone();
    input["source_plan"]["calculations"][&id]["parameters"]["clip_zscore"] = json!(2.0);
    std::fs::write(&path, serde_json::to_vec(&input).unwrap()).unwrap();
    assert!(offline::run(&bootstrap, &path)
        .err()
        .unwrap()
        .to_string()
        .contains("historical factor definition differs"));

    input["source_plan"]["calculations"][&id]["parameters"]["clip_zscore"] = original;
    let duplicate = input["points"][0].clone();
    input["points"].as_array_mut().unwrap().push(duplicate);
    std::fs::write(&path, serde_json::to_vec(&input).unwrap()).unwrap();
    assert!(offline::run(&bootstrap, &path)
        .err()
        .unwrap()
        .to_string()
        .contains("duplicate historical factor point"));
    std::fs::remove_file(path).unwrap();
}

fn portfolio_request() -> portfolio::PortfolioRequest {
    portfolio::PortfolioRequest {
        profiles: vec![],
        start_ms: Some(START),
        end_ms: Some(START + 180_000),
        thresholds: None,
        max_pos_u: None,
        fee_rate: 0.0,
    }
}

#[test]
fn invalid_or_missing_bar_volume_never_creates_a_theoretical_fill() {
    let profile = profile();
    let mut data = memory(&profile, &[0.99]);
    data.candles.get_mut(&("BTCUSDT".into(), 60_000)).unwrap()[0].volume = 0.0;
    let result = run(&profile, request(&profile), &data);
    assert_eq!(result.missing_vwap, 1);
    assert!(result.trades.is_empty());
}

#[test]
fn portfolio_sums_independent_coins_and_preserves_rule_budgets() {
    let mut profile = profile();
    let mut rule = profile.combination.factor_rules[0].clone();
    rule.symbol = "ETHUSDT".into();
    profile.combination.factor_rules.push(rule);
    let mut data = memory(&profile, &[0.99, 0.2]);
    let btc = profile.factor_id("BTCUSDT", "baseline_040");
    data.points.insert(
        profile.factor_id("ETHUSDT", "baseline_040"),
        data.points[&btc].clone(),
    );
    data.candles.insert(
        ("ETHUSDT".into(), 60_000),
        data.candles[&("BTCUSDT".into(), 60_000)].clone(),
    );
    let bootstrap = Bootstrap {
        fetched_at_ms: START,
        source_host: "fixture".into(),
        profiles: vec![profile],
        symbol_groups: Default::default(),
        amount_thresholds: BTreeMap::new(),
        threshold_metadata: BTreeMap::new(),
    };
    let data = Arc::new(RwLock::new(data));
    let result = portfolio::calculate(&bootstrap, &data, portfolio_request()).unwrap();
    assert_eq!(result.summary.rules, 2);
    assert_eq!(result.summary.ready_rules, 2);
    assert_eq!(result.summary.closed_trades, 2);
    assert!((result.summary.pnl - 2.0 * 100.0 / 101.0 * 8.0).abs() < 1e-9);
    assert!((result.curve.last().unwrap().pnl - result.summary.pnl).abs() < 1e-9);
    assert_eq!(result.groups.len(), 1);
    assert_eq!(data.read().unwrap().kafka_records, 100);
}

#[test]
fn portfolio_drawdown_uses_the_combined_equity_curve() {
    let mut profile = profile();
    let mut rule = profile.combination.factor_rules[0].clone();
    rule.symbol = "ETHUSDT".into();
    profile.combination.factor_rules.push(rule);
    let mut data = memory(&profile, &[0.99, 0.2]);
    data.points.insert(
        profile.factor_id("ETHUSDT", "baseline_040"),
        data.points[&profile.factor_id("BTCUSDT", "baseline_040")].clone(),
    );
    let mut eth = data.candles[&("BTCUSDT".into(), 60_000)].clone();
    eth[1].turnover = 930.0;
    data.candles.insert(("ETHUSDT".into(), 60_000), eth);
    let bootstrap = Bootstrap {
        fetched_at_ms: START,
        source_host: "fixture".into(),
        profiles: vec![profile],
        symbol_groups: Default::default(),
        amount_thresholds: BTreeMap::new(),
        threshold_metadata: BTreeMap::new(),
    };
    let result = portfolio::calculate(
        &bootstrap,
        &Arc::new(RwLock::new(data)),
        portfolio_request(),
    )
    .unwrap();
    assert!(result.summary.pnl.abs() < 1e-9);
    assert!(result.summary.max_drawdown.abs() < 1e-9);
    assert_eq!(result.summary.closed_trades, 2);
    assert_eq!(result.summary.win_rate, Some(0.5));
}
