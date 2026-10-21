use crate::factor_pub::cta_nq::NqObservation;
use rolling_common::exact_rolling_window::ExactRollingWindow;
use std::collections::VecDeque;

const WINDOW: usize = 24;
const MIN_PERIODS: usize = 12;

/// Nominal two-hour windows of valid 5m observations. Missing prices do not
/// advance either window, so wall-clock coverage can exceed two hours.
pub(super) struct FiveMinuteNq {
    closes: VecDeque<f64>,
    buy_changes: ExactRollingWindow,
    sell_changes: ExactRollingWindow,
}

impl Default for FiveMinuteNq {
    fn default() -> Self {
        Self {
            closes: VecDeque::with_capacity(WINDOW),
            buy_changes: ExactRollingWindow::new(WINDOW),
            sell_changes: ExactRollingWindow::new(WINDOW),
        }
    }
}

impl FiveMinuteNq {
    pub fn observe(&mut self, mid: f64) -> NqObservation {
        if !mid.is_finite() || mid <= 0.0 {
            return NqObservation::default();
        }
        self.closes.push_back(mid);
        if self.closes.len() > WINDOW {
            self.closes.pop_front();
        }
        let low = self.closes.iter().copied().fold(f64::INFINITY, f64::min);
        let high = self
            .closes
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let buy = (mid - low) / low;
        let sell = (mid - high) / high;
        self.buy_changes.observe(buy);
        self.sell_changes.observe(sell);
        let ready = self.buy_changes.len() >= MIN_PERIODS && self.sell_changes.len() >= MIN_PERIODS;
        NqObservation {
            long_value: Some(buy),
            short_value: Some(sell),
            long_quantile: self.buy_changes.percentile_rank_last(),
            short_quantile: self.sell_changes.percentile_rank_last(),
            long_threshold: ready
                .then(|| self.buy_changes.quantile_linear(0.5))
                .flatten(),
            short_threshold: ready
                .then(|| self.sell_changes.quantile_linear(0.5))
                .flatten(),
            ready,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_sample_windows_match_linear_medians_and_skip_gaps() {
        let mut state = FiveMinuteNq::default();
        let mut prices = Vec::new();
        let mut buys = Vec::new();
        let mut sells = Vec::new();
        for i in 0..80 {
            let mid = 100.0 + ((i * 17) % 31) as f64;
            prices.push(mid);
            let recent = &prices[prices.len().saturating_sub(24)..];
            let low = recent.iter().copied().fold(f64::INFINITY, f64::min);
            let high = recent.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            buys.push((mid - low) / low);
            sells.push((mid - high) / high);
            for missing in [f64::NAN, 0.0, -1.0, f64::INFINITY] {
                assert!(!state.observe(missing).ready);
            }
            let result = state.observe(mid);
            assert_eq!(result.long_value, buys.last().copied());
            assert_eq!(result.short_value, sells.last().copied());
            assert_eq!(result.ready, i >= 11);
            for (series, threshold) in [
                (&buys, result.long_threshold),
                (&sells, result.short_threshold),
            ] {
                if i < 11 {
                    assert_eq!(threshold, None);
                    continue;
                }
                let mut sorted = series[series.len().saturating_sub(24)..].to_vec();
                sorted.sort_by(f64::total_cmp);
                let middle = sorted.len() / 2;
                let median = if sorted.len() % 2 == 0 {
                    (sorted[middle - 1] + sorted[middle]) / 2.0
                } else {
                    sorted[middle]
                };
                assert!((threshold.unwrap() - median).abs() < 1e-12);
            }
        }
    }
}
