use super::*;
use mkt_parsers::msg::trade_flow_feature_msg::{
    TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
};

#[test]
fn remaining_rx02_factors_have_no_size_dependency_and_measure_cold_readiness() {
    let table: serde_json::Value = serde_json::from_str(include_str!(
        "cta_perp_symbol_factors.candidate.json"
    )).unwrap();
    let mut names: Vec<String> = table.as_object().unwrap().values()
        .flat_map(|row| row.as_object().unwrap().keys().cloned()).collect();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), 22);
    let plan = SymbolFactorPlan::from_factor_names("rx02-audit", names.clone()).unwrap();
    let normalize = NormalizeConfig { window_bars: 1440, min_periods: 1000, clip_zscore: 3.0 };
    let mut plain = SymbolFactorState::new(&normalize, names.len(), BarInterval::OneMinute);
    let mut classified = SymbolFactorState::new(&normalize, names.len(), BarInterval::OneMinute);
    let mut raw = BaselineReplayState::default();
    let mut first_raw = vec![None; names.len()];
    let mut first_z = first_raw.clone();
    let mut first_ready = first_raw.clone();
    for bar in 0..3070 {
        let mid = 100.0 + bar as f64 * 0.01 + (bar as f64 * 0.1).sin();
        let mut values = vec![1.0; TRADE_FLOW_FEATURE_DIM];
        for (name, value) in [
            ("open", mid - (bar as f64 * 0.17).sin() * 0.2),
            ("high", mid + 1.0), ("low", mid - 1.0), ("close", mid),
            ("volume", 5.0 + (bar % 13) as f64), ("vwap", mid),
            ("buy_vwap", mid - 0.03), ("sell_vwap", mid + 0.03),
        ] {
            values[TRADE_FLOW_FEATURE_FIELD_NAMES.iter().position(|field| *field == name).unwrap()] = value;
        }
        for side in [-1.0, 1.0] {
            for level in 0..20 {
                values.extend([mid + side * (level as f64 + 1.0) * 0.01, 10.0]);
            }
        }
        let mut changed = values.clone();
        for (i, name) in TRADE_FLOW_FEATURE_FIELD_NAMES.iter().enumerate() {
            if name.contains("large") || name.contains("medium") || name.contains("small") {
                values[i] = 0.0;
                changed[i] = (bar + 1) as f64 * (i + 1) as f64;
            }
        }
        let message = |values: &[f64]| TradeFlowFeatureMsg::from_indexed_values(
            "ETHUSDT".into(), TradingVenue::BinanceFutures.to_u8(),
            1_800_000_000_000 + bar * 60_000, values,
        ).unwrap();
        let input = message(&values);
        raw.push(input.clone()).unwrap();
        let a = plain.evaluate(input, &plan, true).unwrap().unwrap();
        let b = classified.evaluate(message(&changed), &plan, true).unwrap().unwrap();
        for (i, value) in raw.factor_values(&plan).into_iter().enumerate() {
            if value.is_finite() { first_raw[i].get_or_insert(bar + 1); }
            assert_eq!(a[i].ready, b[i].ready, "{}", names[i]);
            assert_eq!(a[i].score.is_finite(), b[i].score.is_finite(), "{}", names[i]);
            if a[i].score.is_finite() {
                assert_eq!(a[i].score, b[i].score, "{}", names[i]);
                assert_eq!(a[i].quantile, b[i].quantile, "{}", names[i]);
                first_z[i].get_or_insert(bar + 1);
            }
            if a[i].ready { first_ready[i].get_or_insert(bar + 1); }
        }
    }
    for (i, name) in names.iter().enumerate() {
        assert!(first_ready[i].is_some(), "{name} failed to become ready");
        println!("{}", serde_json::json!({"factor":name,"first_raw_bar":first_raw[i],"first_z_bar":first_z[i],"first_ready_bar":first_ready[i],"size_independent":true}));
        if name == "factor_114" { assert_eq!(first_ready[i], Some(3069)); }
        if name == "buy_avg_price" { assert_eq!(first_ready[i], Some(2938)); }
    }
}
