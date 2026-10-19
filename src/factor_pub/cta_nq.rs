//! Causal 120-minute NQ calculation shared by CTA Perp and Spread.
use rolling_common::exact_rolling_window::ExactRollingWindow;
use std::collections::VecDeque;

pub(crate) const NQ_LOOKBACK_BARS: usize = 120;
pub(crate) const NQ_QUANTILE_WINDOW: usize = 120;
const NQ_MIN_PERIODS: usize = 60;

#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub(crate) struct NqObservation {
    pub long_value: Option<f64>,
    pub long_quantile: Option<f64>,
    pub short_value: Option<f64>,
    pub short_quantile: Option<f64>,
    pub ready: bool,
    pub execution_ts_ms: i64,
    pub execution_bid: Option<f64>,
    pub execution_ask: Option<f64>,
}

pub(crate) fn observe_nq(
    close: f64,
    closes: &mut VecDeque<f64>,
    long_window: &mut ExactRollingWindow,
    short_window: &mut ExactRollingWindow,
) -> NqObservation {
    closes.push_back(close);
    while closes.len() > NQ_LOOKBACK_BARS {
        closes.pop_front();
    }
    let finite: Vec<f64> = closes
        .iter()
        .copied()
        .filter(|value| value.is_finite() && *value > 0.0)
        .collect();
    let long_value = (close.is_finite() && !finite.is_empty()).then(|| {
        let minimum = finite.iter().copied().fold(f64::INFINITY, f64::min);
        (close - minimum) / minimum
    });
    let short_value = (close.is_finite() && !finite.is_empty()).then(|| {
        let maximum = finite.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        (close - maximum) / maximum
    });
    long_window.observe_slot(long_value.unwrap_or(f64::NAN));
    short_window.observe_slot(short_value.unwrap_or(f64::NAN));
    let long_quantile = long_window.percentile_rank_last();
    let short_quantile = short_window.percentile_rank_last();
    NqObservation {
        long_value,
        long_quantile,
        short_value,
        short_quantile,
        ready: long_value.is_some()
            && short_value.is_some()
            && long_window.len() >= NQ_MIN_PERIODS
            && short_window.len() >= NQ_MIN_PERIODS
            && long_quantile.is_some()
            && short_quantile.is_some(),
        execution_ts_ms: 0,
        execution_bid: None,
        execution_ask: None,
    }
}
