use crate::factor_pub::cta_nq::NqObservation;
use rolling_common::exact_rolling_window::ExactRollingWindow;
use std::collections::VecDeque;

/// Fixed 120-bar windows of valid observations at every multi-minute frequency.
/// Missing prices do not advance either window, extending its wall-clock coverage.
pub(super) struct MultiMinuteNq {
    window: usize,
    min_periods: usize,
    closes: VecDeque<f64>,
    buy_changes: ExactRollingWindow,
    sell_changes: ExactRollingWindow,
}

impl Default for MultiMinuteNq {
    fn default() -> Self {
        Self::new(crate::cta_perp::interval::BarInterval::FiveMinutes)
    }
}

impl MultiMinuteNq {
    pub fn new(interval: crate::cta_perp::interval::BarInterval) -> Self {
        let window = interval.nq_window_samples();
        Self {
            window,
            min_periods: window / 2,
            closes: VecDeque::with_capacity(window),
            buy_changes: ExactRollingWindow::new(window),
            sell_changes: ExactRollingWindow::new(window),
        }
    }

    pub fn observe(&mut self, mid: f64) -> NqObservation {
        if !mid.is_finite() || mid <= 0.0 {
            return NqObservation::default();
        }
        self.closes.push_back(mid);
        if self.closes.len() > self.window {
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
        let ready = self.buy_changes.len() >= self.min_periods
            && self.sell_changes.len() >= self.min_periods;
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
        for (interval, window, min_periods) in
            [(crate::cta_perp::interval::BarInterval::FiveMinutes, 120, 60)]
        {
            let mut state = MultiMinuteNq::new(interval);
            let mut prices = Vec::new();
            let mut buys = Vec::new();
            let mut sells = Vec::new();
            for i in 0..360 {
                let mid = 100.0 + ((i * 17) % 31) as f64;
                prices.push(mid);
                let recent = &prices[prices.len().saturating_sub(window)..];
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
                assert_eq!(result.ready, i + 1 >= min_periods);
                for (series, threshold) in [
                    (&buys, result.long_threshold),
                    (&sells, result.short_threshold),
                ] {
                    if i + 1 < min_periods {
                        assert_eq!(threshold, None);
                        continue;
                    }
                    let mut sorted = series[series.len().saturating_sub(window)..].to_vec();
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

    #[test]
    fn extrema_and_change_ranks_expire_after_120_valid_bars_at_every_frequency() {
        for interval in [crate::cta_perp::interval::BarInterval::FiveMinutes] {
            let mut state = MultiMinuteNq::new(interval);
            state.observe(50.0);
            state.observe(200.0);
            for _ in 0..118 {
                state.observe(100.0);
            }
            let first_expired = state.observe(100.0);
            assert_eq!(first_expired.long_value, Some(0.0));
            assert_eq!(first_expired.short_value, Some(-0.5));
            let both_expired = state.observe(100.0);
            assert_eq!(both_expired.long_value, Some(0.0));
            assert_eq!(both_expired.short_value, Some(0.0));
            assert!(both_expired.ready);
            assert!((both_expired.long_quantile.unwrap() - 1.0 / 120.0).abs() < 1e-12);
            assert!((both_expired.short_quantile.unwrap() - 119.5 / 120.0).abs() < 1e-12);
        }
    }
}
