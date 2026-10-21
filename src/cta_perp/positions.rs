use super::config::CtaPerpConfig;
use super::factors::WeightedBar;
use anyhow::{ensure, Result};

/// Size one independent symbol/factor entry from its full USDT allocation.
pub fn full_qty(max_pos_u: f64, flag: i8, price: f64) -> Result<f64> {
    ensure!(
        flag == 1 || flag == -1,
        "opening position flag must be +/-1"
    );
    ensure!(
        max_pos_u.is_finite() && max_pos_u > 0.0,
        "invalid full-position amount"
    );
    ensure!(
        price.is_finite() && price > 0.0,
        "invalid full-position sizing price"
    );
    let qty = f64::from(flag) * max_pos_u / price;
    ensure!(
        qty.is_finite() && qty != 0.0,
        "invalid full-position quantity"
    );
    Ok(qty)
}

pub fn next_position_flag(current: i8, bar: &WeightedBar, config: &CtaPerpConfig) -> i8 {
    let exit = &config.execution;
    if (current > 0 && exit.factor_exit_quantile_long - bar.decision_value > 4.0 * f64::EPSILON)
        || (current < 0
            && bar.decision_value - exit.factor_exit_quantile_short > 4.0 * f64::EPSILON)
    {
        return 0;
    }
    if current != 0 {
        return current;
    }
    let flag = if bar.value > 0.0 {
        1
    } else if bar.value < 0.0 {
        -1
    } else {
        0
    };
    if flag == 0 {
        return 0;
    }
    if config.entry.nq_change_enabled
        && config.bar_interval == super::interval::BarInterval::FiveMinutes
    {
        let (value, threshold) = if flag > 0 {
            (bar.nq.filter_long_value, bar.nq.filter_long_threshold)
        } else {
            (bar.nq.filter_short_value, bar.nq.filter_short_threshold)
        };
        let valid = bar.nq.filter_ready
            && value.zip(threshold).is_some_and(|(value, threshold)| {
                value.is_finite()
                    && threshold.is_finite()
                    && if flag > 0 {
                        value >= threshold
                    } else {
                        value <= threshold
                    }
            });
        return if valid { flag } else { 0 };
    }
    if config.entry.nq_change_enabled {
        let q = if flag > 0 {
            bar.nq.filter_long_quantile
        } else {
            bar.nq.filter_short_quantile
        };
        let valid = bar.nq.filter_ready
            && q.is_some_and(|q| {
                q.is_finite()
                    && (0.0..=1.0).contains(&q)
                    && if flag > 0 {
                        q >= config.entry.nq_long_quantile
                    } else {
                        q <= config.entry.nq_short_quantile
                    }
            });
        if !valid {
            return 0;
        }
    }
    flag
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cta_perp::factors::{aggregate_bar, SymbolFactors};
    use trade_signal::model_output_hub::ModelOutputScoreLookupResult;
    #[test]
    fn five_minute_nq_compares_values_inclusively_and_never_forces_exits() {
        let mut c = config();
        c.bar_interval = super::super::interval::BarInterval::FiveMinutes;
        // A low percentile rank caused by ties must not reject equality to Q50.
        let mut long = bar(0.99, true);
        long.nq.filter_long_value = Some(0.0);
        long.nq.filter_long_threshold = Some(0.0);
        long.nq.filter_long_quantile = Some(0.1);
        assert_eq!(next_position_flag(0, &long, &c), 1);
        long.nq.filter_long_threshold = Some(0.01);
        assert_eq!(next_position_flag(0, &long, &c), 0);
        assert_eq!(next_position_flag(1, &long, &c), 1);
        let mut short = bar(0.01, true);
        short.nq.filter_short_value = Some(-0.05);
        short.nq.filter_short_threshold = Some(-0.05);
        short.nq.filter_short_quantile = Some(0.9);
        assert_eq!(next_position_flag(0, &short, &c), -1);
        short.nq.filter_short_threshold = Some(-0.06);
        assert_eq!(next_position_flag(0, &short, &c), 0);
        short.nq.filter_ready = false;
        assert_eq!(next_position_flag(-1, &short, &c), -1);
        assert_eq!(next_position_flag(1, &short, &c), 0); // factor exit still works
        c.entry.nq_change_enabled = false;
        assert_eq!(next_position_flag(0, &short, &c), -1);
    }
    fn config() -> CtaPerpConfig {
        serde_json::from_str(include_str!("../../config/cta_perp.json")).unwrap()
    }
    fn bar(q: f64, nq_ready: bool) -> WeightedBar {
        bar_for(q, nq_ready, &config(), 1.0)
    }
    fn bar_for(q: f64, nq_ready: bool, config: &CtaPerpConfig, sign: f64) -> WeightedBar {
        let input = ModelOutputScoreLookupResult {
            service_name: String::new(),
            symbol_key: "FILUSDT".into(),
            subscribed: true,
            score: Some(1.0),
            score_quantile: Some(q),
            score_ready: true,
            score_long_threshold: None,
            score_short_threshold: None,
            filter_long_value: None,
            filter_long_threshold: None,
            filter_short_value: None,
            filter_short_threshold: None,
            filter_long_quantile: Some(0.9),
            filter_short_quantile: Some(0.1),
            filter_ready: nq_ready,
            score_ts_ms: 60_000,
            note: String::new(),
        };
        aggregate_bar(
            &SymbolFactors {
                factors: vec!["factor_036".into()],
                weights: vec![sign],
            },
            &[input],
            &config.entry,
            config.position_algorithm,
        )
        .unwrap()
    }
    #[test]
    fn percentile_usage_changes_entries_and_exits_without_changing_sign_or_nq() {
        use crate::cta_perp::config::PositionAlgorithm;
        let mut c = config();
        c.entry.factor_long_quantile = 0.7;
        assert_eq!(next_position_flag(0, &bar_for(0.8, true, &c, 1.0), &c), 1);
        assert_eq!(next_position_flag(1, &bar_for(0.6, false, &c, 1.0), &c), 1);
        c.position_algorithm = PositionAlgorithm::V1;
        let observation = bar_for(0.8, true, &c, 1.0);
        assert!((observation.decision_value - 0.6).abs() < 1e-12);
        assert_eq!(observation.quantile, 0.8);
        assert_eq!(observation.nq.filter_long_quantile, Some(0.9));
        assert_eq!(next_position_flag(0, &observation, &c), 0);
        assert_eq!(next_position_flag(1, &bar_for(0.6, false, &c, 1.0), &c), 0);
        assert_eq!(next_position_flag(0, &bar_for(0.1, true, &c, -1.0), &c), 1);
        assert_eq!(next_position_flag(0, &bar_for(0.1, false, &c, -1.0), &c), 0);
    }

    #[test]
    fn centered_percentiles_support_negative_thresholds_and_strict_ties() {
        use crate::cta_perp::config::PositionAlgorithm;
        let mut c = config();
        c.position_algorithm = PositionAlgorithm::V1;
        c.entry.factor_long_quantile = 0.6;
        c.entry.factor_short_quantile = -0.6;
        c.execution.factor_exit_quantile_long = 0.2;
        c.execution.factor_exit_quantile_short = -0.2;
        c.validate().unwrap();
        for (current, q, next) in [
            (0, 0.8, 0),
            (0, 0.2, 0),
            (0, 0.81, 1),
            (0, 0.19, -1),
            (1, 0.6, 1),
            (1, 0.59, 0),
            (-1, 0.4, -1),
            (-1, 0.41, 0),
        ] {
            assert_eq!(
                next_position_flag(current, &bar_for(q, true, &c, 1.0), &c),
                next,
                "current={current} percentile={q}"
            );
        }
    }

    #[test]
    fn symbol_overrides_change_only_that_symbols_position_decisions() {
        use crate::cta_perp::config::{PositionAlgorithm, SymbolThresholdOverrides};
        let mut c = config();
        c.symbol_overrides.insert(
            "ETHUSDT".into(),
            SymbolThresholdOverrides {
                factor_long_quantile: Some(0.7),
                factor_short_quantile: Some(0.0),
                factor_exit_quantile_long: Some(0.6),
                factor_exit_quantile_short: Some(0.2),
            },
        );
        for algorithm in [PositionAlgorithm::V0, PositionAlgorithm::V1] {
            c.position_algorithm = algorithm;
            c.validate().unwrap();
            let eth = c.for_symbol("ETHUSDT");
            let btc = c.for_symbol("BTCUSDT");
            let q = if algorithm == PositionAlgorithm::V0 {
                0.8
            } else {
                0.9
            };
            assert_eq!(next_position_flag(0, &bar_for(q, true, &eth, 1.0), &eth), 1);
            assert_eq!(next_position_flag(0, &bar_for(q, true, &btc, 1.0), &btc), 0);
            let q = if algorithm == PositionAlgorithm::V0 {
                0.5
            } else {
                0.75
            };
            assert_eq!(next_position_flag(1, &bar_for(q, true, &eth, 1.0), &eth), 0);
            assert_eq!(next_position_flag(1, &bar_for(q, true, &btc, 1.0), &btc), 1);
        }
    }
    #[test]
    fn independent_states_hold_and_exit_without_nq() {
        let c = config();
        assert_eq!(next_position_flag(0, &bar(0.99, true), &c), 1);
        assert_eq!(next_position_flag(1, &bar(0.99, true), &c), 1);
        assert_eq!(next_position_flag(1, &bar(0.5, false), &c), 1);
        assert_eq!(next_position_flag(1, &bar(0.29, false), &c), 0);
        assert_eq!(next_position_flag(-1, &bar(0.71, false), &c), 0);
        assert_eq!(next_position_flag(0, &bar(0.99, false), &c), 0);
    }
    #[test]
    fn full_qty_is_computed_by_publisher() {
        let c = config();
        assert_eq!(full_qty(c.max_pos_u, 1, 500.0).unwrap(), 0.4);
        assert_eq!(full_qty(c.max_pos_u, -1, 500.0).unwrap(), -0.4);
        assert_eq!(full_qty(2000.0, 1, 100.0).unwrap(), 20.0);
    }

    #[test]
    fn full_qty_rejects_invalid_amount_direction_and_price() {
        assert!(full_qty(0.0, 1, 20.0).is_err());
        assert!(full_qty(f64::NAN, 1, 20.0).is_err());
        assert!(full_qty(500.0, 0, 20.0).is_err());
        assert!(full_qty(500.0, -1, 0.0).is_err());
        assert!(full_qty(500.0, -1, f64::INFINITY).is_err());
    }
}
