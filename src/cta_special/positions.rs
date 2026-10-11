use super::config::CtaSpecialConfig;
use super::factors::WeightedBar;
use anyhow::{ensure, Result};

pub fn equal_allocation(symbol_max_u: f64, factor_count: usize) -> Result<f64> {
    ensure!(
        symbol_max_u.is_finite() && symbol_max_u > 0.0,
        "invalid symbol amount"
    );
    ensure!(factor_count > 0, "symbol has no active factors");
    Ok(symbol_max_u / factor_count as f64)
}

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

pub fn next_position_flag(current: i8, bar: &WeightedBar, config: &CtaSpecialConfig) -> i8 {
    let exit = &config.execution;
    if (current > 0 && exit.factor_exit_quantile_long - bar.quantile > 4.0 * f64::EPSILON)
        || (current < 0 && bar.quantile - exit.factor_exit_quantile_short > 4.0 * f64::EPSILON)
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
    use crate::cta_special::factors::{aggregate_bar, SymbolFactors};
    use trade_signal::model_output_hub::ModelOutputScoreLookupResult;
    fn config() -> CtaSpecialConfig {
        serde_json::from_str(include_str!("../../config/cta_special.json")).unwrap()
    }
    fn bar(q: f64, nq_ready: bool) -> WeightedBar {
        let config = config();
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
                weights: vec![1.0],
            },
            &[input],
            &config.entry,
        )
        .unwrap()
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
        assert_eq!(full_qty(c.max_pos_u, 1, 500.0).unwrap(), 1.0);
        assert_eq!(full_qty(c.max_pos_u, -1, 500.0).unwrap(), -1.0);
        assert_eq!(full_qty(2000.0, 1, 100.0).unwrap(), 20.0);
    }

    #[test]
    fn equal_shares_reserve_flat_factors_and_keep_signed_quantities() {
        assert_eq!(equal_allocation(500.0, 4).unwrap(), 125.0);
        assert_eq!(
            full_qty(equal_allocation(500.0, 5).unwrap(), -1, 20.0).unwrap(),
            -5.0
        );
        assert!(equal_allocation(500.0, 0).is_err());
        assert!(equal_allocation(f64::NAN, 2).is_err());
    }
}
