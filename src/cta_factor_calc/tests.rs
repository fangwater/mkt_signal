use super::*;
use crate::cta_factor_calc::config::*;
use period_pbs::pb::{PriceLevel, SymbolInfo};
use std::collections::BTreeSet;

fn combination(name: &str, factors: &[(&str, f64)]) -> Combination {
    Combination {
        signal: None,
        name: name.into(),
        venue: Market::BinanceFutures,
        interval: Interval::Minute,
        reference_venue: None,
        factor_rules: factors
            .iter()
            .map(|(s, w)| {
                let id = FactorId::parse(s).unwrap();
                FactorRule {
                    symbol: id.symbol,
                    weight: *w,
                    factor: id.factor,
                    nq: "关闭".into(),
                    venue: Some(id.market),
                    interval: Some(id.interval),
                    reference_venue: id.reference_market,
                }
            })
            .collect(),
        parameters: BTreeMap::new(),
    }
}
fn config() -> Config {
    Config {
        combinations_dir: PathBuf::from("unused"),
        history_dir: PathBuf::from("unused"),
        lookback_secs: 1_296_000,
        catchup_timeout_secs: 60,
        kafka: Default::default(),
        amount_thresholds: BTreeMap::new(),
    }
}
use std::path::PathBuf;

struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("cta-factor-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn names_are_market_and_period_scoped_and_spread_inputs_are_ordered() {
    for name in [
        "binance-futures-btcusdt-1min-nq",
        "okex-futures-btcusdt-5min-td_ti_024",
        "okex-futures-binance-futures-btcusdt-15min-spread",
        "binance-margin-binance-futures-btcusdt-1min-spread",
    ] {
        assert_eq!(FactorId::parse(name).unwrap().name(), name);
    }
    for bad in [
        "btcusdt-1min-nq",
        "binance-futures-BTCUSDT-1min-nq",
        "okex-futures-btcusdt-1m-nq",
        "okex-futures-btcusdt-1min-spread",
        "binance-futures-binance-futures-btcusdt-1min-spread",
        "okex-futures-binance-futures-btcusdt-1min-nq",
        "okex-futures-btcusdt-1min-unknown",
    ] {
        assert!(FactorId::parse(bad).is_err(), "{bad}");
    }
}

#[test]
fn four_column_rows_add_venue_period_and_nq_without_collapsing_signal_rules() {
    let combo:Combination=serde_json::from_value(serde_json::json!({
        "name":"okx_1min","venue":"okex-futures","interval":"1min",
        "factor_rules":[{"标的":"BTC","nq":"0.2/0.8","符号":-1,"因子":"factor_036"},{"标的":"BTCUSDT","nq":"关闭","符号":-1,"因子":"factor_036"}]
    })).unwrap();
    combo.validate().unwrap();
    assert_eq!(combo.factor_rules.len(), 2);
    let plan = Plan::merge([combo]).unwrap();
    assert_eq!(plan.calculations.len(), 2);
    assert!(plan
        .calculations
        .contains_key("okex-futures-btcusdt-1min-factor_036"));
    assert!(plan
        .calculations
        .contains_key("okex-futures-btcusdt-1min-nq"));
    assert_eq!(plan.topics(), ["okex-swap"]);
    assert!(Worker::Perp.validate_plan(&plan).is_err());
    Worker::Spread.validate_plan(&plan).unwrap();
    let rx = Combination::load(std::path::Path::new(
        "config/cta_prep_binance_futures/binance-futures_15min.json",
    ))
    .unwrap();
    assert_eq!(rx.signal_config().unwrap().factor_rules.len(), 146);
    let duplicate = rx
        .factor_rules
        .iter()
        .filter(|r| r.symbol == "SKLUSDT" && r.factor == "factor_170")
        .count();
    assert_eq!(duplicate, 2);
}

#[test]
fn duplicate_json_references_are_rejected_instead_of_overwriting_weights() {
    let json = r#"{"name":"one","venue":"binance-futures","interval":"1min","factor_rules":[{"标的":"BTC","nq":"关闭","符号":1,"因子":"nq"}],"parameters":{"binance-futures-btcusdt-1min-nq":{},"binance-futures-btcusdt-1min-nq":{}}}"#;
    assert!(serde_json::from_str::<Combination>(json).is_err());
}

#[test]
fn frequency_strategies_own_independent_allocations_and_reject_duplicate_publishers() {
    let config =
        Config::load(std::path::Path::new("config/cta_prep_binance_futures.toml")).unwrap();
    let plan = Plan::load(&config.combinations_dir).unwrap();
    let mut combinations = plan.combinations.values().cloned().collect::<Vec<_>>();
    let identities = combinations
        .iter()
        .map(Combination::manager_strategy_name)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        identities,
        BTreeSet::from([
            "cta-prep-bn-1min-pos".into(),
            "cta-prep-bn-5min-pos".into(),
            "cta-prep-bn-15min-pos".into(),
        ])
    );
    let before = combinations[1].signal.clone();
    combinations[0].signal.as_mut().unwrap().max_pos_u = 123.0;
    let changed = Plan::merge(combinations.clone()).unwrap();
    assert_eq!(changed.calculations, plan.calculations);
    assert_eq!(combinations[1].signal, before);
    let mut duplicate = combinations[0].clone();
    duplicate.name = "another_file_same_strategy".into();
    combinations.push(duplicate);
    assert!(Plan::merge(combinations)
        .unwrap_err()
        .to_string()
        .contains("only one signal"));
    assert_eq!(
        crate::cta_spread::config::SpreadMode::OkexBinance.manager_strategy_name(),
        "cta-spread-okx-bn-1min-pos"
    );
}

#[test]
fn merge_is_exactly_the_requested_union_and_rejects_parameter_conflicts() {
    let shared = "binance-futures-btcusdt-1min-factor_036";
    let a = combination(
        "a",
        &[(shared, 1.0), ("okex-futures-ethusdt-15min-nq", -1.0)],
    );
    let b = combination(
        "b",
        &[
            (shared, -1.0),
            ("okex-futures-binance-futures-btcusdt-5min-spread", 1.0),
            ("binance-futures-solusdt-1min-nq", 0.0),
        ],
    );
    let plan = Plan::merge([a.clone(), b.clone()]).unwrap();
    assert_eq!(plan.calculations.len(), 3);
    assert_eq!(plan.calculations[shared].combinations.len(), 2);
    assert_eq!(plan.topics(), ["binance-futures", "okex-swap"]);
    let after = Plan::merge([b.clone()]).unwrap();
    assert!(after.calculations.contains_key(shared));
    assert!(!after
        .calculations
        .contains_key("okex-futures-ethusdt-15min-nq"));
    let mut changed = b;
    let mut params = Parameters::for_id(&FactorId::parse(shared).unwrap());
    params.clip_zscore = 4.0;
    changed.parameters.insert(shared.into(), params);
    assert!(Plan::merge([a, changed])
        .unwrap_err()
        .to_string()
        .contains("conflicting"));
}

#[test]
fn binance_worker_merges_three_frequency_configurations_with_exact_periods() {
    let config =
        Config::load(std::path::Path::new("config/cta_prep_binance_futures.toml")).unwrap();
    let plan = Plan::load(&config.combinations_dir).unwrap();
    assert_eq!(
        plan.combinations
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [
            "binance-futures_15min",
            "binance-futures_1min",
            "binance-futures_5min"
        ]
    );
    for (name, folder, interval) in [
        (
            "binance-futures_15min",
            "cta_perp_rx01",
            Interval::FifteenMinutes,
        ),
        ("binance-futures_1min", "cta_perp_rx03", Interval::Minute),
        (
            "binance-futures_5min",
            "cta_perp_rx04",
            Interval::FiveMinutes,
        ),
    ] {
        let table: BTreeMap<String, BTreeMap<String, f64>> = serde_json::from_slice(
            &std::fs::read(format!("config/{folder}/cta_perp_symbol_factors.json")).unwrap(),
        )
        .unwrap();
        let combination = &plan.combinations[name];
        for (symbol, row) in table {
            for (factor, weight) in row {
                let id = format!(
                    "binance-futures-{}-{}-{}",
                    symbol.to_ascii_lowercase(),
                    interval.name(),
                    factor.to_ascii_lowercase()
                );
                assert_eq!(combination.factors().unwrap()[&id], weight);
            }
        }
        assert!(combination
            .factors()
            .unwrap()
            .keys()
            .all(|id| FactorId::parse(id).unwrap().interval == interval));
    }
    assert_eq!(plan.calculations.len(), 304);
    assert_eq!(plan.topics(), ["binance-futures"]);
    Worker::Perp.validate_plan(&plan).unwrap();
    assert!(Worker::Perp
        .validate_plan(
            &Plan::merge([combination("bad", &[("okex-futures-btcusdt-1min-nq", 1.0)])]).unwrap()
        )
        .is_err());
}

fn period(symbol: &str, ts: i64, mid: f64, snapshot: bool) -> PeriodMessage {
    PeriodMessage {
        period: 1000,
        ts,
        post_ts: ts,
        poster_id: "fixture".into(),
        symbol_infos: vec![SymbolInfo {
            symbol: symbol.into(),
            trades: Vec::new(),
            incs: vec![IncrementOrderBookInfo {
                timestamp: ts,
                is_snapshot: snapshot,
                bids: vec![PriceLevel {
                    price: mid - 0.5,
                    amount: 2.0,
                }],
                asks: vec![PriceLevel {
                    price: mid + 0.5,
                    amount: 2.0,
                }],
            }],
        }],
    }
}

#[test]
fn nq_uses_each_markets_own_bars_at_all_requested_frequencies() {
    let names = [
        "binance-futures-btcusdt-1min-nq",
        "binance-futures-btcusdt-5min-nq",
        "okex-futures-btcusdt-15min-nq",
    ];
    let plan = Plan::merge([combination("mixed", &names.map(|name| (name, 1.0)))]).unwrap();
    let mut engine = Engine::new(plan, &config()).unwrap();
    let start = 1_800_000_000_000i64.div_euclid(900_000) * 900_000;
    for minute in 0..=1000 {
        let ts = start + minute * 60_000;
        let bn = 100.0 + (minute % 17) as f64;
        let okx = 200.0 + (minute % 31) as f64;
        engine
            .on_period(
                Market::BinanceFutures,
                &period("BTCUSDT", ts, bn, minute == 0),
            )
            .unwrap();
        engine
            .on_period(
                Market::OkexFutures,
                &period("BTC-USDT-SWAP", ts, okx, minute == 0),
            )
            .unwrap();
    }
    assert_eq!(engine.latest.len(), 3);
    for name in names {
        let point = &engine.latest[name];
        assert!(point.ready(), "{point:?}");
        assert_eq!(
            point.ts_ms % FactorId::parse(name).unwrap().interval.millis(),
            0
        );
    }
    // NQ at the last closed 15m bucket uses the preceding bucket (minute 974).
    // Its 120-bar extrema contain only that market's prices.
    let point = &engine.latest[names[2]];
    let count = (point.ts_ms - start) / 900_000;
    let mids = (0..count - 1)
        .map(|bar| 200.0 + ((bar * 15 + 14) % 31) as f64)
        .collect::<Vec<_>>();
    let last = *mids.last().unwrap();
    let low = mids.iter().copied().fold(f64::INFINITY, f64::min);
    let high = mids.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    assert_eq!(point.fields["long"].raw, Some((last - low) / low));
    assert_eq!(point.fields["short"].raw, Some((last - high) / high));
}

#[test]
fn nq_uses_120_valid_bars_and_lags_one_bar_at_every_frequency() {
    for frequency in ["1min", "5min", "15min"] {
        let id = FactorId::parse(&format!("binance-futures-btcusdt-{frequency}-nq")).unwrap();
        let parameters = Parameters::for_id(&id);
        assert_eq!(parameters.lookback, 120);
        assert_eq!(parameters.rank_window, 120);
        assert_eq!(parameters.rank_min_periods, 60);
        assert_eq!(parameters.missing_policy, MissingPolicy::ValidSamples);
        let mut nq = Nq::new(&parameters);
        for source in 1..=120 {
            let fields = nq.observe(Some(if source == 1 { 50.0 } else { 100.0 }));
            assert_eq!(fields["long"].ready, source > 60);
            assert_eq!(fields["short"].ready, source > 60);
            if source == 1 {
                assert!(fields["long"].raw.is_none());
            } else if source == 2 {
                // Source bar 1 had only its own price, not the new higher mid.
                assert_eq!(fields["long"].raw, Some(0.0));
            } else {
                assert_eq!(fields["long"].raw, Some(1.0));
            }
        }
        // The missing bar emits the preceding valid observation, without
        // consuming a valid-sample window slot. Its unknown value appears one
        // bar later and must not be replaced by the last valid NQ.
        assert_eq!(nq.observe(None)["long"].raw, Some(1.0));
        assert_eq!(nq.closes.len(), 120);
        let after_gap = nq.observe(Some(100.0));
        assert!(!after_gap["long"].ready && after_gap["long"].raw.is_none());
        let expired = nq.observe(Some(100.0));
        assert_eq!(expired["long"].raw, Some(0.0));
        assert!(expired["long"].ready);
        assert_eq!(nq.long.len(), 120);
        assert_eq!(nq.short.len(), 120);
    }
}

#[test]
fn spread_window_is_2880_minutes_and_requires_1440_valid_pairs() {
    let id = FactorId::parse("okex-futures-binance-futures-btcusdt-1min-spread").unwrap();
    let parameters = Parameters::for_id(&id);
    assert_eq!(parameters.rank_window, 2880);
    assert_eq!(parameters.rank_min_periods, 1440);
    let mut state = Spread::new(&parameters);
    let observe = |state: &mut Spread, minute: i64, okx: Option<f64>| {
        let ts = minute * 60_000;
        assert!(state.observe(0, ts, okx).is_empty());
        let mut output = state.observe(1, ts, Some(100.0));
        assert_eq!(output.len(), 1);
        output.pop().unwrap().1
    };
    for minute in 1..1440 {
        let field = observe(&mut state, minute, Some(101.0));
        assert_eq!(field.raw, Some(1.0 / 101.0));
        assert!(!field.ready);
    }
    let missing = observe(&mut state, 1440, None);
    assert!(missing.raw.is_none() && missing.quantile.is_none() && !missing.ready);
    assert_eq!(state.transform.ranks.len(), 1439);
    assert!(observe(&mut state, 1441, Some(101.0)).ready);
    for minute in 1442..=2880 {
        assert!(!observe(&mut state, minute, None).ready);
    }
    // Missing minutes still consume window slots. Old valid pairs expire after
    // 2880 minutes even when fewer than 2880 valid pairs have arrived.
    assert!(observe(&mut state, 2881, Some(101.0)).ready);
    assert!(!observe(&mut state, 2882, None).ready);
    assert!(!observe(&mut state, 2883, Some(101.0)).ready);
    assert_eq!(state.transform.ranks.len(), 1439);
}

#[test]
fn spread_matches_timestamps_and_invalid_quotes_never_reuse_a_price() {
    let name = "okex-futures-binance-futures-btcusdt-1min-spread";
    let mut combo = combination("basis", &[(name, 1.0)]);
    let mut params = Parameters::for_id(&FactorId::parse(name).unwrap());
    params.rank_window = 3;
    params.rank_min_periods = 2;
    combo.parameters.insert(name.into(), params);
    let mut engine = Engine::new(Plan::merge([combo]).unwrap(), &config()).unwrap();
    let start = 1_800_000_000_000;
    for minute in 0..=3 {
        let ts = start + minute * 60_000;
        engine
            .on_period(
                Market::OkexFutures,
                &period("BTC-USDT-SWAP", ts, 101.0, true),
            )
            .unwrap();
        engine
            .on_period(Market::BinanceFutures, &period("BTCUSDT", ts, 100.0, true))
            .unwrap();
    }
    assert_eq!(engine.latest[name].fields["value"].raw, Some(1.0 / 101.0));
    assert!(engine.latest[name].ready());
    assert_eq!(
        engine.previous_spreads[name].ts_ms,
        engine.latest[name].ts_ms - 60_000
    );
    let mut invalid = period("BTC-USDT-SWAP", start + 4 * 60_000, 101.0, true);
    invalid.symbol_infos[0].incs[0].asks.clear();
    engine.on_period(Market::OkexFutures, &invalid).unwrap();
    engine
        .on_period(
            Market::BinanceFutures,
            &period("BTCUSDT", start + 4 * 60_000, 100.0, true),
        )
        .unwrap();
    engine
        .on_period(
            Market::OkexFutures,
            &period("BTC-USDT-SWAP", start + 5 * 60_000, 101.0, true),
        )
        .unwrap();
    engine
        .on_period(
            Market::BinanceFutures,
            &period("BTCUSDT", start + 5 * 60_000, 100.0, true),
        )
        .unwrap();
    assert_eq!(engine.latest[name].fields["value"].raw, None);
    assert!(!engine.latest[name].ready());
}

#[test]
fn deletion_releases_only_unreferenced_work_and_keeps_warmed_nq_state() {
    let nq = "binance-futures-btcusdt-1min-nq";
    let other = "okex-futures-ethusdt-1min-nq";
    let a = combination("a", &[(nq, 1.0)]);
    let b = combination("b", &[(nq, -1.0), (other, 1.0)]);
    let mut engine = Engine::new(Plan::merge([a.clone(), b]).unwrap(), &config()).unwrap();
    let start = 1_800_000_000_000;
    for minute in 0..=65 {
        engine
            .on_period(
                Market::BinanceFutures,
                &period("BTCUSDT", start + minute * 60_000, 100.0, true),
            )
            .unwrap();
    }
    assert!(engine.latest[nq].ready());
    engine.retain_plan(Plan::merge([a]).unwrap()).unwrap();
    assert_eq!(engine.series.len(), 1);
    engine
        .on_period(
            Market::BinanceFutures,
            &period("BTCUSDT", start + 66 * 60_000, 100.0, true),
        )
        .unwrap();
    assert!(engine.latest[nq].ready());
    engine.retain_plan(Plan::default()).unwrap();
    assert!(engine.series.is_empty() && engine.special.is_empty() && engine.latest.is_empty());
}

#[test]
fn same_identity_has_one_definition_and_sparse_formula_batches_do_not_cross_symbols() {
    let first = "binance-futures-btcusdt-1min-avg_price";
    let second = "binance-futures-btcusdt-1min-factor_036";
    let eth = "okex-futures-ethusdt-5min-avg_price";
    let a = combination("a", &[(first, 1.0), (second, 1.0), (eth, 1.0)]);
    let b = combination("b", &[(first, -1.0)]);
    let mut engine = Engine::new(Plan::merge([a, b.clone()]).unwrap(), &config()).unwrap();
    assert_eq!(engine.series.len(), 2);
    assert_eq!(engine.formula.len(), 2);
    assert_eq!(
        engine.formula[&(Market::BinanceFutures, "BTCUSDT".into(), Interval::Minute)]
            .names
            .len(),
        2
    );
    engine.retain_plan(Plan::merge([b]).unwrap()).unwrap();
    assert_eq!(engine.formula.len(), 1);
    assert_eq!(
        engine.formula[&(Market::BinanceFutures, "BTCUSDT".into(), Interval::Minute)].names,
        [first]
    );
}

#[test]
fn configuration_edits_are_atomic_dry_runs_and_preserve_other_references() {
    let dir = TestDir::new();
    let mut settings = config();
    settings.combinations_dir = dir.path().join("combinations");
    std::fs::create_dir(&settings.combinations_dir).unwrap();
    let name = "binance-futures-btcusdt-1min-nq";
    let a = combination("a", &[(name, 1.0)]);
    let b = combination("b", &[(name, -1.0)]);
    super::super::runtime::edit_combination(
        &settings,
        Some(a.clone()),
        None,
        false,
        Worker::Spread,
    )
    .unwrap();
    assert_eq!(
        std::fs::read_dir(&settings.combinations_dir)
            .unwrap()
            .count(),
        0
    );
    super::super::runtime::edit_combination(&settings, Some(a), None, true, Worker::Spread)
        .unwrap();
    super::super::runtime::edit_combination(&settings, Some(b), None, true, Worker::Spread)
        .unwrap();
    assert_eq!(
        Plan::load(&settings.combinations_dir)
            .unwrap()
            .calculations
            .len(),
        1
    );
    super::super::runtime::edit_combination(&settings, None, Some("a"), true, Worker::Spread)
        .unwrap();
    assert!(Plan::load(&settings.combinations_dir)
        .unwrap()
        .calculations
        .contains_key(name));
    let before = std::fs::read(settings.combinations_dir.join("b.json")).unwrap();
    assert!(super::super::runtime::edit_combination(
        &settings,
        None,
        Some("../b"),
        true,
        Worker::Spread
    )
    .is_err());
    assert_eq!(
        std::fs::read(settings.combinations_dir.join("b.json")).unwrap(),
        before
    );
}

#[test]
fn old_tables_gain_market_and_frequency_without_changing_signed_weights() {
    let table = BTreeMap::from([
        (
            "BTCUSDT".into(),
            BTreeMap::from([("factor_036".into(), -1.0)]),
        ),
        (
            "ETHUSDT".into(),
            BTreeMap::from([("TD_TI_024".into(), 1.0)]),
        ),
    ]);
    let imported = Combination::from_symbol_factors(
        "momentum".into(),
        Market::OkexFutures,
        Interval::FiveMinutes,
        table,
        true,
    )
    .unwrap();
    assert_eq!(imported.factors().unwrap().len(), 4);
    assert_eq!(
        imported.factors().unwrap()["okex-futures-btcusdt-5min-factor_036"],
        -1.0
    );
    assert_eq!(
        imported.factors().unwrap()["okex-futures-ethusdt-5min-td_ti_024"],
        1.0
    );
    assert!(imported
        .factors()
        .unwrap()
        .contains_key("okex-futures-btcusdt-5min-nq"));
}

#[test]
fn spread_price_formulas_run_without_size_metadata_while_size_formulas_wait() {
    let names = [
        "binance-futures-btcusdt-1min-baseline_115",
        "binance-futures-btcusdt-1min-baseline_126",
        "binance-futures-btcusdt-1min-td_pr_014",
        "binance-futures-btcusdt-1min-baseline_035",
        "binance-futures-btcusdt-1min-baseline_047",
    ];
    let mut combo = combination("inputs", &names.map(|name| (name, 1.0)));
    for name in names {
        let mut parameters = Parameters::for_id(&FactorId::parse(name).unwrap());
        parameters.normalize_window = 3;
        parameters.normalize_min_periods = 2;
        parameters.rank_window = 3;
        parameters.rank_min_periods = 2;
        combo.parameters.insert(name.into(), parameters);
    }
    let mut engine = Engine::new(Plan::merge([combo]).unwrap(), &config()).unwrap();
    let start = 1_800_000_000_000;
    for minute in 0..=320 {
        let ts = start + minute * 60_000;
        let mid = 100.0 + (minute % 7) as f64;
        let mut input = period("BTCUSDT", ts, mid, true);
        input.symbol_infos[0].trades = vec![
            TradeInfo {
                timestamp: ts + 1,
                side: "buy".into(),
                price: mid + 0.25,
                amount: 1.0,
            },
            TradeInfo {
                timestamp: ts + 2,
                side: "sell".into(),
                price: mid - 0.25,
                amount: 2.0,
            },
        ];
        engine.on_period(Market::BinanceFutures, &input).unwrap();
    }
    for name in &names[..3] {
        assert!(
            engine.latest[*name].ready(),
            "{name}: {:?}",
            engine.latest[*name]
        );
    }
    for name in &names[3..] {
        assert!(!engine.latest[*name].ready());
        assert_eq!(engine.latest[*name].fields["value"].raw, None);
    }
}

#[test]
fn baseline_replay_fans_out_to_all_intervals_and_keeps_missing_size_data_unknown() {
    let names = [
        "binance-futures-btcusdt-1min-avg_price",
        "binance-futures-btcusdt-5min-avg_price",
        "binance-futures-btcusdt-15min-avg_price",
        "binance-futures-btcusdt-1min-net_buy_small",
    ];
    let mut combo = combination("prices", &names.map(|id| (id, 1.0)));
    for id in names {
        let mut params = Parameters::for_id(&FactorId::parse(id).unwrap());
        params.normalize_window = 3;
        params.normalize_min_periods = 2;
        params.rank_window = 3;
        params.rank_min_periods = 2;
        combo.parameters.insert(id.into(), params);
    }
    let plan = Plan::merge([combo]).unwrap();
    let mut a = Engine::new(plan.clone(), &config()).unwrap();
    let mut b = Engine::new(plan, &config()).unwrap();
    let start = 1_800_000_000_000;
    // avg_price needs 250 native bars; include enough complete 15m bars and
    // additional bars for normalization and ranking.
    for minute in 0..=4000 {
        let ts = start + minute * 60_000;
        let mid = 100.0 + (minute % 7) as f64;
        let mut input = period("BTCUSDT", ts, mid, true);
        input.symbol_infos[0].trades = vec![
            TradeInfo {
                timestamp: ts + 1,
                side: "buy".into(),
                price: mid + 0.25,
                amount: 1.0,
            },
            TradeInfo {
                timestamp: ts + 2,
                side: "sell".into(),
                price: mid - 0.25,
                amount: 2.0,
            },
        ];
        assert_eq!(
            a.on_period(Market::BinanceFutures, &input).unwrap(),
            b.on_period(Market::BinanceFutures, &input).unwrap()
        );
    }
    for id in &names[..3] {
        assert!(a.latest[*id].ready(), "{}: {:?}", id, a.latest[*id]);
    }
    assert!(!a.latest[names[3]].ready());
    assert_eq!(a.latest[names[3]].fields["value"].raw, None);
    assert_eq!(a.series.len(), 1);
    assert_eq!(a.formula.len(), 3);
}

#[test]
fn vector_and_scalar_observations_share_one_serializable_contract() {
    let id = "okex-futures-btcusdt-1min-nq";
    let mut engine = Engine::new(
        Plan::merge([combination("one", &[(id, 1.0)])]).unwrap(),
        &config(),
    )
    .unwrap();
    engine
        .on_period(
            Market::OkexFutures,
            &period("BTC-USDT-SWAP", 1_800_000_000_000, 100.0, true),
        )
        .unwrap();
    let points = engine
        .on_period(
            Market::OkexFutures,
            &period("BTC-USDT-SWAP", 1_800_000_060_000, 101.0, true),
        )
        .unwrap();
    assert_eq!(points.len(), 1);
    let bytes = serde_json::to_vec(&points[0]).unwrap();
    assert!(bytes.len() < mkt_parsers::msg::model_ipc::MODEL_PAYLOAD_MAX_BYTES);
    assert_eq!(
        serde_json::from_slice::<FactorPoint>(&bytes).unwrap(),
        points[0]
    );
    assert_eq!(
        points[0].fields.keys().cloned().collect::<Vec<_>>(),
        ["long", "short"]
    );
    assert_ne!(
        crate::cta_factor_calc::runtime::service_path(Worker::Perp),
        crate::cta_factor_calc::runtime::service_path(Worker::Spread)
    );
}

#[test]
fn consumer_invalidates_deleted_factors_and_replaced_definitions_even_at_equal_times() {
    use crate::cta_factor_calc::client::FactorCache;
    let id = "binance-futures-btcusdt-1min-factor_036";
    let point = |value| FactorPoint {
        factor_id: id.into(),
        ts_ms: 60_000,
        fields: BTreeMap::from([(
            "value".into(),
            Field {
                raw: Some(value),
                value: Some(value),
                quantile: Some(0.5),
                median: Some(value),
                ready: true,
            },
        )]),
    };
    let mut cache = FactorCache::default();
    cache
        .apply(FactorEvent::Point {
            generation: "first".into(),
            point: point(1.0),
        })
        .unwrap();
    assert!(cache.fresh(id, 60_001, 1000).is_some());
    assert!(cache.fresh(id, 70_000, 1000).is_none());
    assert!(cache.fresh(id, 59_999, 1000).is_none());
    cache
        .apply(FactorEvent::Point {
            generation: "replacement".into(),
            point: point(2.0),
        })
        .unwrap();
    assert_eq!(cache.latest()[id].fields["value"].value, Some(2.0));
    cache
        .apply(FactorEvent::Removed {
            generation: "replacement".into(),
            factor_id: id.into(),
        })
        .unwrap();
    assert!(cache.latest().is_empty());
}

#[test]
fn identical_depth_factor_names_use_independent_okx_and_binance_books() {
    let bn = "binance-futures-btcusdt-1min-depth_imb_all";
    let okx = "okex-futures-btcusdt-1min-depth_imb_all";
    let mut combo = combination("books", &[(bn, 1.0), (okx, 1.0)]);
    for id in [bn, okx] {
        let mut params = Parameters::for_id(&FactorId::parse(id).unwrap());
        params.normalize_window = 3;
        params.normalize_min_periods = 2;
        params.rank_window = 3;
        params.rank_min_periods = 2;
        combo.parameters.insert(id.into(), params);
    }
    let mut engine = Engine::new(Plan::merge([combo]).unwrap(), &config()).unwrap();
    for minute in 0..=10 {
        let ts = 1_800_000_000_000 + minute * 60_000;
        engine
            .on_period(Market::BinanceFutures, &period("BTCUSDT", ts, 100.0, true))
            .unwrap();
        let mut book = period("BTC-USDT-SWAP", ts, 100.0, true);
        book.symbol_infos[0].incs[0].bids[0].amount = 10.0;
        engine.on_period(Market::OkexFutures, &book).unwrap();
    }
    assert!(engine.latest[bn].ready() && engine.latest[okx].ready());
    assert_ne!(
        engine.latest[bn].fields["value"].raw,
        engine.latest[okx].fields["value"].raw
    );
}
