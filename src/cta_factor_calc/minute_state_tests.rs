use super::*;
use crate::cta_factor_calc::config::*;
use period_pbs::pb::{PriceLevel, SymbolInfo};
use period_pbs::period::{period_for_timestamp_ms, DEFAULT_PERIOD_MS};

#[test]
fn minute_checkpoint_keeps_live_ranks_while_raw_inputs_replay_and_never_seeds_hours() {
    let combo: Combination = serde_json::from_value(serde_json::json!({
        "name":"resume", "strategy_group":"test", "venue":"binance-futures", "interval":"5min",
        "factor_rules":[{"标的":"BTC","因子":"TP_VPI_005","符号":1,"nq":"关闭"}]
    }))
    .unwrap();
    let plan = Plan::merge([combo]).unwrap();
    let mut config = Config::load(std::path::Path::new("config/cta_factor_spread.toml")).unwrap();
    config
        .amount_thresholds
        .insert("binance-futures:BTCUSDT".into(), [100.0, 1000.0]);
    let id = "binance-futures-btcusdt-5min-tp_vpi_005";
    let start = 1_790_000_000_000_i64.div_euclid(300_000) * 300_000;
    let record = |n: i64| {
        let end = start + n * 300_000;
        let mid = 100.0 + (n % 7) as f64;
        PeriodMessage {
            period: period_for_timestamp_ms(end, DEFAULT_PERIOD_MS),
            ts: end,
            post_ts: end,
            poster_id: "minute-resume".into(),
            symbol_infos: vec![SymbolInfo {
                symbol: "BTCUSDT".into(),
                incs: vec![IncrementOrderBookInfo {
                    timestamp: (end - 1000) * 1000,
                    is_snapshot: true,
                    bids: vec![PriceLevel {
                        price: mid - 0.5,
                        amount: 2.0,
                    }],
                    asks: vec![PriceLevel {
                        price: mid + 0.5,
                        amount: 1.0,
                    }],
                }],
                trades: vec![
                    TradeInfo {
                        timestamp: (end - 1000) * 1000,
                        side: "buy".into(),
                        price: mid,
                        amount: 1.0,
                    },
                    TradeInfo {
                        timestamp: (end - 1000) * 1000,
                        side: "sell".into(),
                        price: mid + 0.1,
                        amount: 2.0,
                    },
                ],
            }],
        }
    };
    let mut reference = Engine::new(plan.clone(), &config).unwrap();
    let mut journal = Engine::new(plan.clone(), &config).unwrap();
    for n in 1..=1000 {
        for point in reference
            .on_period(Market::BinanceFutures, &record(n))
            .unwrap()
        {
            assert!(journal.replay_minute_journal_point(point).unwrap());
        }
    }
    assert!(reference.latest[id].ready(), "{:?}", reference.latest[id]);
    let live = reference.minute_formula_state(&config.amount_thresholds);
    let migrated = journal.minute_formula_state(&config.amount_thresholds);
    assert_eq!(
        serde_json::to_value(&live).unwrap(),
        serde_json::to_value(&migrated).unwrap()
    );
    let state: MinuteFormulaState =
        serde_json::from_slice(&serde_json::to_vec(&live).unwrap()).unwrap();
    let mut resumed = Engine::new(plan.clone(), &config).unwrap();
    assert_eq!(
        resumed
            .restore_minute_formula_state(&state, &config)
            .unwrap(),
        1
    );
    let cutoff = reference.latest[id].ts_ms;
    resumed
        .validate_minute_state_start(Market::BinanceFutures, cutoff)
        .unwrap();
    assert!(resumed
        .validate_minute_state_start(Market::BinanceFutures, cutoff + 300_001)
        .is_err());
    let mut cold = Engine::new(plan.clone(), &config).unwrap();
    for n in 500..=1000 {
        assert!(resumed
            .on_period(Market::BinanceFutures, &record(n))
            .unwrap()
            .is_empty());
        cold.on_period(Market::BinanceFutures, &record(n)).unwrap();
    }
    assert!(!cold.latest[id].ready());
    assert_eq!(resumed.latest[id], reference.latest[id]);
    for n in 1001..1010 {
        assert_eq!(
            resumed
                .on_period(Market::BinanceFutures, &record(n))
                .unwrap(),
            reference
                .on_period(Market::BinanceFutures, &record(n))
                .unwrap()
        );
    }
    let mut altered = config.clone();
    altered
        .amount_thresholds
        .insert("binance-futures:BTCUSDT".into(), [200.0, 2000.0]);
    assert_eq!(
        Engine::new(plan.clone(), &altered)
            .unwrap()
            .restore_minute_formula_state(&state, &altered)
            .unwrap(),
        1
    );
    let mut changed = plan.clone();
    changed
        .calculations
        .get_mut(id)
        .unwrap()
        .parameters
        .rank_window += 1;
    assert_eq!(
        Engine::new(changed, &config)
            .unwrap()
            .restore_minute_formula_state(&state, &config)
            .unwrap(),
        0
    );
    let mut malformed = serde_json::to_value(&state).unwrap();
    let row = malformed["factors"]
        .as_object_mut()
        .unwrap()
        .remove(id)
        .unwrap();
    malformed["factors"]["binance-futures-btcusdt-1h-tp_vpi_005"] = row;
    let invalid: MinuteFormulaState = serde_json::from_value(malformed).unwrap();
    assert!(Engine::new(plan, &config)
        .unwrap()
        .restore_minute_formula_state(&invalid, &config)
        .is_err());
}

#[test]
fn minute_checkpoint_rejects_changed_required_thresholds_and_corrupt_windows() {
    let combo: Combination = serde_json::from_value(serde_json::json!({
        "name":"threshold", "strategy_group":"test", "venue":"binance-futures", "interval":"5min",
        "factor_rules":[{"标的":"BTC","因子":"baseline_196","符号":1,"nq":"关闭"}]
    }))
    .unwrap();
    let plan = Plan::merge([combo]).unwrap();
    let mut config = Config::load(std::path::Path::new("config/cta_factor_spread.toml")).unwrap();
    config
        .amount_thresholds
        .insert("binance-futures:BTCUSDT".into(), [10.0, 100.0]);
    let id = "binance-futures-btcusdt-5min-baseline_196";
    let mut engine = Engine::new(plan.clone(), &config).unwrap();
    let mut source = Transform::new(&plan.calculations[id].parameters, Interval::FiveMinutes);
    for n in 1..600 {
        let point = FactorPoint {
            factor_id: id.into(),
            ts_ms: 1_790_000_000_000_i64.div_euclid(300_000) * 300_000 + n * 300_000,
            fields: BTreeMap::from([("value".into(), source.observe(Some((n % 11) as f64), true))]),
        };
        engine.replay_minute_journal_point(point).unwrap();
    }
    let snapshot = engine.minute_formula_state(&config.amount_thresholds);
    let mut changed = config.clone();
    changed
        .amount_thresholds
        .insert("binance-futures:BTCUSDT".into(), [20.0, 200.0]);
    assert_eq!(
        Engine::new(plan.clone(), &changed)
            .unwrap()
            .restore_minute_formula_state(&snapshot, &changed)
            .unwrap(),
        0
    );
    let mut malformed = serde_json::to_value(snapshot).unwrap();
    malformed["factors"][id]["normalize"]["window"] = serde_json::json!(1);
    let malformed: MinuteFormulaState = serde_json::from_value(malformed).unwrap();
    assert!(Engine::new(plan, &config)
        .unwrap()
        .restore_minute_formula_state(&malformed, &config)
        .is_err());
}
