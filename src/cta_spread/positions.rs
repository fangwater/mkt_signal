use super::config::CtaSpreadConfig;
use super::factors::WeightedBar;
use anyhow::{ensure, Result};
use order_common::TradingVenue;
use serde::{Deserialize, Serialize};

/// A factor's targets include the carried basis position after a directional exit.
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

    pub fn enter(&mut self, flag: i8, venue: TradingVenue, amount: f64, price: f64) -> Result<()> {
        self.validate()?;
        ensure!(
            self.flag == 0,
            "cannot add to an active directional position"
        );
        ensure!(
            matches!(
                venue,
                TradingVenue::BinanceMargin | TradingVenue::BinanceFutures
            ),
            "unsupported entry market"
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
        if venue == TradingVenue::BinanceMargin {
            next.cash_qty += qty;
        } else {
            next.perpetual_qty += qty;
        }
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

pub fn entry_venue(flag: i8, bar: &WeightedBar, config: &CtaSpreadConfig) -> Option<TradingVenue> {
    if !matches!(flag, -1 | 1) || !bar.spread.filter_ready {
        return None;
    }
    let q = bar
        .spread
        .filter_long_quantile
        .filter(|q| q.is_finite() && (0.0..=1.0).contains(q))?;
    let low = config.entry.spread_lower_quantile - q > 4.0 * f64::EPSILON;
    let high = q - config.entry.spread_upper_quantile > 4.0 * f64::EPSILON;
    if !low && !high {
        return None;
    }
    Some(if (flag > 0 && low) || (flag < 0 && high) {
        TradingVenue::BinanceMargin
    } else {
        TradingVenue::BinanceFutures
    })
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
        return if entry_venue(vote, bar, config).is_some() {
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
    if entry_venue(vote, bar, config).is_some() {
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
        serde_json::from_str(include_str!("../../config/cta_spread.json")).unwrap()
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
        aggregate_bar(
            &SymbolFactors {
                factors: vec!["factor_036".into()],
                weights: vec![1.0],
            },
            &[input],
            &config().entry,
        )
        .unwrap()
    }
    #[test]
    fn factor_and_spread_select_all_four_entry_markets() {
        let c = config();
        for (f, q, flag, venue) in [
            (0.99, 0.2, 1, TradingVenue::BinanceMargin),
            (0.99, 0.8, 1, TradingVenue::BinanceFutures),
            (0.01, 0.2, -1, TradingVenue::BinanceFutures),
            (0.01, 0.8, -1, TradingVenue::BinanceMargin),
        ] {
            let b = bar(f, Some(q));
            assert_eq!(next_position_flag(0, &b, &c), flag);
            assert_eq!(entry_venue(flag, &b, &c), Some(venue));
        }
        for q in [Some(0.3), Some(0.5), Some(0.7), None, Some(f64::NAN)] {
            assert_eq!(next_position_flag(0, &bar(0.99, q), &c), 0);
        }
    }
    #[test]
    fn spread_never_exits_or_blocks_factor_exit() {
        let c = config();
        assert_eq!(next_position_flag(1, &bar(0.6, None), &c), 1);
        assert_eq!(next_position_flag(-1, &bar(0.4, Some(0.5)), &c), -1);
        assert_eq!(next_position_flag(1, &bar(0.1, None), &c), 0);
        assert_eq!(next_position_flag(-1, &bar(0.9, None), &c), 0);
    }
    #[test]
    fn cash_exit_is_hedged_and_restart_and_later_entries_keep_inventory() {
        let mut p = StrategyPosition::default();
        p.enter(1, TradingVenue::BinanceMargin, 200.0, 100.0)
            .unwrap();
        p.exit();
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (2.0, -2.0, 0.0));
        p = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
        p.validate().unwrap();
        p.enter(-1, TradingVenue::BinanceFutures, 200.0, 100.0)
            .unwrap();
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (2.0, -4.0, -2.0));
        p.exit();
        assert_eq!((p.cash_qty, p.perpetual_qty), (2.0, -2.0));
        p.enter(-1, TradingVenue::BinanceMargin, 200.0, 100.0)
            .unwrap();
        p.exit();
        assert_eq!((p.cash_qty, p.perpetual_qty), (0.0, 0.0));
        assert!(p.enter(1, TradingVenue::BinanceMargin, 1.0, 0.0).is_err());
    }
    #[test]
    fn reversals_are_immediate_but_missing_spread_only_exits() {
        let c = config();
        for (current, factor) in [(1, 0.01), (-1, 0.99)] {
            for q in [0.2, 0.8] {
                assert_eq!(
                    next_position_flag(current, &bar(factor, Some(q)), &c),
                    -current
                );
            }
            for q in [Some(0.5), None] {
                assert_eq!(next_position_flag(current, &bar(factor, q), &c), 0);
            }
        }
    }
    #[test]
    fn reversed_targets_include_old_hedge_and_new_direction_once() {
        for direction in [-1, 1] {
            for entry_market in [TradingVenue::BinanceMargin, TradingVenue::BinanceFutures] {
                for reverse_market in [TradingVenue::BinanceMargin, TradingVenue::BinanceFutures] {
                    let mut p = StrategyPosition::default();
                    p.enter(direction, entry_market, 200.0, 100.0).unwrap();
                    let old_cash = p.cash_qty;
                    p.exit();
                    p.enter(-direction, reverse_market, 300.0, 100.0).unwrap();
                    p.validate().unwrap();
                    assert_eq!(p.qty, -3.0 * f64::from(direction));
                    assert_eq!(
                        p.cash_qty,
                        old_cash
                            + if reverse_market == TradingVenue::BinanceMargin {
                                -3.0 * f64::from(direction)
                            } else {
                                0.0
                            }
                    );
                    assert_eq!(p.cash_qty + p.perpetual_qty, p.qty);
                    // A repeated same-direction bar must keep fixed quantities.
                    let saved = p.clone();
                    assert_eq!(
                        next_position_flag(
                            p.flag,
                            &bar(if direction > 0 { 0.01 } else { 0.99 }, Some(0.8)),
                            &config()
                        ),
                        p.flag
                    );
                    assert_eq!(p, saved);
                }
            }
        }
    }
    #[test]
    fn short_cash_and_perpetual_only_exits() {
        let mut p = StrategyPosition::default();
        p.enter(-1, TradingVenue::BinanceMargin, 200.0, 100.0)
            .unwrap();
        p.exit();
        assert_eq!((p.cash_qty, p.perpetual_qty), (-2.0, 2.0));
        let mut p = StrategyPosition::default();
        p.enter(1, TradingVenue::BinanceFutures, 200.0, 100.0)
            .unwrap();
        p.exit();
        assert_eq!(p, StrategyPosition::default());
    }
}
