use super::config::CtaSpreadConfig;
use super::factors::WeightedBar;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

/// One symbol/factor's absolute targets, including its last entry's exit hedge.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StrategyPosition {
    pub flag: i8,
    pub qty: f64,
    pub cash_qty: f64,
    pub perpetual_qty: f64,
    pub entry_notional: f64,
}

impl StrategyPosition {
    pub fn validate(&self) -> Result<()> {
        ensure!((-1..=1).contains(&self.flag), "invalid direction flag");
        ensure!(
            [
                self.qty,
                self.cash_qty,
                self.perpetual_qty,
                self.entry_notional
            ]
            .iter()
            .all(|v| v.is_finite()),
            "non-finite CTA position"
        );
        let tolerance = 1e-10 * (1.0 + self.cash_qty.abs() + self.perpetual_qty.abs());
        ensure!(
            (self.qty - self.cash_qty - self.perpetual_qty).abs() <= tolerance,
            "CTA position does not reconcile across markets"
        );
        ensure!(self.entry_notional >= 0.0, "negative entry notional");
        ensure!(
            if self.flag == 0 {
                self.qty == 0.0 && self.entry_notional == 0.0
            } else {
                self.qty * f64::from(self.flag) > 0.0 && self.entry_notional > 0.0
            },
            "CTA direction disagrees with position"
        );
        Ok(())
    }

    pub fn enter_cash(&mut self, flag: i8, amount: f64, price: f64) -> Result<()> {
        self.validate()?;
        ensure!(
            self.flag == 0,
            "cannot add to an active directional position"
        );
        ensure!(
            matches!(flag, -1 | 1)
                && amount.is_finite()
                && amount > 0.0
                && price.is_finite()
                && price > 0.0,
            "invalid entry direction, amount or price"
        );
        let qty = f64::from(flag) * amount / price;
        let mut next = self.clone();
        // A new signal replaces both market targets. A previous exit hedge is
        // not another lot to retain or add to this entry.
        next.cash_qty = qty;
        next.perpetual_qty = 0.0;
        next.flag = flag;
        next.qty = qty;
        next.entry_notional = amount;
        next.validate()?;
        *self = next;
        Ok(())
    }

    pub fn exit(&mut self) {
        self.perpetual_qty = -self.cash_qty;
        self.flag = 0;
        self.qty = 0.0;
        self.entry_notional = 0.0;
    }
}

pub fn cash_entry_allowed(flag: i8, bar: &WeightedBar, config: &CtaSpreadConfig) -> bool {
    if !matches!(flag, -1 | 1) || !bar.spread.filter_ready {
        return false;
    }
    let Some(q) = bar
        .spread
        .filter_long_quantile
        .filter(|q| q.is_finite() && (0.0..=1.0).contains(q))
    else {
        return false;
    };
    let Some(nq) = &bar.nq else {
        return false;
    };
    let nq_q = if flag > 0 {
        nq.filter_long_quantile
    } else {
        nq.filter_short_quantile
    };
    if !nq.filter_ready
        || !nq_q.is_some_and(|q| {
            q.is_finite()
                && (0.0..=1.0).contains(&q)
                && if flag > 0 {
                    q >= config.entry.nq_long_quantile
                } else {
                    q <= config.entry.nq_short_quantile
                }
        })
    {
        return false;
    }
    // Every directional entry uses cash. The basis must favour that cash side.
    (flag > 0 && config.entry.spread_long_quantile - q > 4.0 * f64::EPSILON)
        || (flag < 0 && q - config.entry.spread_short_quantile > 4.0 * f64::EPSILON)
}

pub fn next_position_flag(current: i8, bar: &WeightedBar, config: &CtaSpreadConfig) -> i8 {
    let vote = if bar.value > 0.0 {
        1
    } else if bar.value < 0.0 {
        -1
    } else {
        0
    };
    // Opposite entry votes can reverse immediately. A blocked new entry still
    // exits the old direction, leaving its cash inventory hedged by perpetuals.
    if current != 0 && vote == -current {
        return if cash_entry_allowed(vote, bar, config) {
            vote
        } else {
            0
        };
    }
    if (current > 0
        && config.execution.factor_exit_quantile_long - bar.quantile > 4.0 * f64::EPSILON)
        || (current < 0
            && bar.quantile - config.execution.factor_exit_quantile_short > 4.0 * f64::EPSILON)
    {
        return 0;
    }
    if current != 0 {
        return current;
    }
    if cash_entry_allowed(vote, bar, config) {
        vote
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cta_spread::factors::{aggregate_bar, SymbolFactors};
    use trade_signal::model_output_hub::ModelOutputScoreLookupResult;
    fn config() -> CtaSpreadConfig {
        {
            let mut config: CtaSpreadConfig =
                serde_json::from_str(include_str!("../../config/cta_spread.json")).unwrap();
            config.entry.spread_long_quantile = 0.3;
            config.entry.spread_short_quantile = 0.7;
            config
        }
    }
    fn bar(factor: f64, spread: Option<f64>) -> WeightedBar {
        let input = ModelOutputScoreLookupResult {
            service_name: String::new(),
            symbol_key: "ETHUSDT".into(),
            subscribed: true,
            score: Some(1.0),
            score_quantile: Some(factor),
            score_ready: true,
            score_long_threshold: None,
            score_short_threshold: None,
            filter_long_value: Some(0.001),
            filter_long_threshold: None,
            filter_short_value: Some(0.001),
            filter_short_threshold: None,
            filter_long_quantile: spread,
            filter_short_quantile: spread,
            filter_ready: spread.is_some(),
            score_ts_ms: 60000,
            note: String::new(),
        };
        let mut result = aggregate_bar(
            &SymbolFactors {
                factors: vec!["factor_036".into()],
                weights: vec![1.0],
            },
            &[input],
            &config().entry,
        )
        .unwrap();
        let mut nq = result.spread.clone();
        nq.filter_long_quantile = Some(0.5);
        nq.filter_short_quantile = Some(0.5);
        nq.filter_ready = true;
        result.nq = Some(nq);
        result
    }
    #[test]
    fn nq_and_spread_must_both_allow_entries_and_reversals() {
        let mut c = config();
        c.entry.spread_long_quantile = 0.7;
        c.entry.spread_short_quantile = 0.3;
        let mut long = bar(0.99, Some(0.6));
        assert_eq!(next_position_flag(0, &long, &c), 1);
        long.nq.as_mut().unwrap().filter_long_quantile = Some(0.49);
        assert_eq!(next_position_flag(0, &long, &c), 0);
        assert_eq!(next_position_flag(-1, &long, &c), 0);
        long.nq = None;
        assert_eq!(next_position_flag(0, &long, &c), 0);
        let mut short = bar(0.01, Some(0.4));
        assert_eq!(next_position_flag(0, &short, &c), -1);
        short.nq.as_mut().unwrap().filter_short_quantile = Some(0.51);
        assert_eq!(next_position_flag(0, &short, &c), 0);
        short.nq.as_mut().unwrap().filter_ready = false;
        assert_eq!(next_position_flag(1, &short, &c), 0);
    }

    #[test]
    fn cash_entries_require_spread_favouring_the_factor_direction() {
        let c = config();
        for (factor, q, expected) in [
            (0.99, 0.2, 1),
            (0.99, 0.8, 0),
            (0.01, 0.2, 0),
            (0.01, 0.8, -1),
        ] {
            assert_eq!(next_position_flag(0, &bar(factor, Some(q)), &c), expected);
        }
        for factor in [0.99, 0.01] {
            for q in [
                Some(0.3),
                Some(0.5),
                Some(0.7),
                None,
                Some(f64::NAN),
                Some(-0.1),
                Some(1.1),
            ] {
                assert_eq!(next_position_flag(0, &bar(factor, q), &c), 0);
            }
        }
    }
    #[test]
    fn spread_never_exits_or_blocks_factor_exit() {
        let c: CtaSpreadConfig = serde_json::from_str(r#"{"execution":{}}"#).unwrap();
        c.validate().unwrap();
        for (current, quantile, expected) in [
            (1, 0.8, 1),
            (1, 0.7, 1),
            (1, 0.69, 0),
            (-1, 0.2, -1),
            (-1, 0.3, -1),
            (-1, 0.31, 0),
        ] {
            assert_eq!(
                next_position_flag(current, &bar(quantile, None), &c),
                expected
            );
        }
    }
    #[test]
    fn cash_exit_is_hedged_and_later_entries_replace_both_targets() {
        let mut p = StrategyPosition::default();
        p.enter_cash(1, 200.0, 100.0).unwrap();
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (2.0, 0.0, 2.0));
        p.exit();
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (2.0, -2.0, 0.0));
        p = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
        p.validate().unwrap();
        p.enter_cash(-1, 200.0, 100.0).unwrap();
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (-2.0, 0.0, -2.0));
        p.exit();
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (-2.0, 2.0, 0.0));
        let saved = p.clone();
        assert!(p.enter_cash(1, 1.0, 0.0).is_err());
        assert_eq!(p, saved);
    }
    #[test]
    fn reversals_require_new_cash_side_spread_otherwise_only_exit() {
        let c = config();
        for (current, factor, allowed, blocked) in [(1, 0.01, 0.8, 0.2), (-1, 0.99, 0.2, 0.8)] {
            assert_eq!(
                next_position_flag(current, &bar(factor, Some(allowed)), &c),
                -current
            );
            for q in [Some(blocked), Some(0.3), Some(0.5), Some(0.7), None] {
                assert_eq!(next_position_flag(current, &bar(factor, q), &c), 0);
            }
        }
    }
    #[test]
    fn reversed_targets_replace_old_contract_hedge_and_cash() {
        for direction in [-1, 1] {
            let d = f64::from(direction);
            let mut p = StrategyPosition::default();
            p.enter_cash(direction, 200.0, 100.0).unwrap();
            p.exit();
            p.enter_cash(-direction, 300.0, 100.0).unwrap();
            p.validate().unwrap();
            assert_eq!(
                (p.cash_qty, p.perpetual_qty, p.qty),
                (-3.0 * d, 0.0, -3.0 * d)
            );
            let saved = p.clone();
            assert_eq!(
                next_position_flag(
                    p.flag,
                    &bar(if direction > 0 { 0.01 } else { 0.99 }, Some(0.5)),
                    &config()
                ),
                p.flag
            );
            assert!(p.enter_cash(-direction, 300.0, 100.0).is_err());
            assert_eq!(p, saved);
            p.exit();
            assert_eq!(
                (p.cash_qty, p.perpetual_qty, p.qty),
                (-3.0 * d, 3.0 * d, 0.0)
            );
        }
    }

    #[test]
    fn repeated_entries_and_exits_never_accumulate_market_targets() {
        for flag in [-1, 1] {
            let mut p = StrategyPosition::default();
            for cycle in 0..100 {
                // Repeated same-side signals previously accumulated a fresh
                // 100 USDT cash leg after every hedge exit.
                let price = 50.0 + f64::from(cycle);
                p.enter_cash(flag, 100.0, price).unwrap();
                assert_eq!(p.cash_qty, f64::from(flag) * 100.0 / price);
                assert_eq!(p.perpetual_qty, 0.0);
                assert!((p.cash_qty.abs() * price - 100.0).abs() < 1e-10);
                p.exit();
                assert!(((p.cash_qty.abs() + p.perpetual_qty.abs()) * price - 200.0).abs() < 1e-10);
                p = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
                p.validate().unwrap();
            }
        }
    }

    #[test]
    fn a_new_target_does_not_inherit_oversized_persisted_hedges() {
        let mut p = StrategyPosition {
            cash_qty: -10_000.0,
            perpetual_qty: 10_000.0,
            ..Default::default()
        };
        for flag in [-1, 1] {
            p.enter_cash(flag, 100.0, 50.0).unwrap();
            assert_eq!(
                (p.cash_qty, p.perpetual_qty, p.qty),
                (f64::from(flag) * 2.0, 0.0, f64::from(flag) * 2.0)
            );
            p.exit();
        }
    }
    #[test]
    fn short_cash_exits_with_long_contract_without_buying_back_cash() {
        let mut p = StrategyPosition::default();
        p.enter_cash(-1, 200.0, 100.0).unwrap();
        p.exit();
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (-2.0, 2.0, 0.0));
    }
}
