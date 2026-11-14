//! Causal raw clipping and normalization shared by both factor workers.
use std::collections::VecDeque;

#[derive(Default)]
struct MeanStd {
    count: usize,
    mean: f64,
    squared_deviations: f64,
}

impl MeanStd {
    fn from_values(values: impl Iterator<Item = f64>) -> Self {
        let mut stats = Self::default();
        for value in values {
            stats.count += 1;
            let delta = value - stats.mean;
            stats.mean += delta / stats.count as f64;
            stats.squared_deviations += delta * (value - stats.mean);
        }
        stats
    }

    fn std(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }
        Some(
            (self.squared_deviations / (self.count - 1) as f64)
                .max(0.0)
                .sqrt(),
        )
    }
}

/// Reclip every raw value in the current trailing window using that window's
/// mean +/- clip_zscore*sample_std, then standardize its current clipped value
/// with the recomputed clipped-window statistics. Historical emitted z-scores
/// are not rewritten. Zero variance maps to 0; unknown results carry forward.
pub(crate) struct NormalizeState {
    raw_window: VecDeque<Option<f64>>,
    window: usize,
    min_periods: usize,
    clip_zscore: f64,
    last_z: Option<f64>,
}

impl NormalizeState {
    pub(crate) fn new(window: usize, min_periods: usize, clip_zscore: f64) -> Self {
        Self {
            raw_window: VecDeque::with_capacity(window.max(1)),
            window: window.max(1),
            min_periods,
            clip_zscore,
            last_z: None,
        }
    }

    pub(crate) fn observe(&mut self, raw: f64) -> Option<f64> {
        if self.raw_window.len() == self.window {
            self.raw_window.pop_front();
        }
        self.raw_window.push_back(raw.is_finite().then_some(raw));
        if !raw.is_finite() {
            return self.last_z;
        }
        let stats = MeanStd::from_values(self.raw_window.iter().flatten().copied());
        if stats.count < self.min_periods {
            return self.last_z;
        }
        let Some(std) = stats.std().filter(|s| s.is_finite()) else {
            return self.last_z;
        };
        let lower = stats.mean - self.clip_zscore * std;
        let upper = stats.mean + self.clip_zscore * std;
        if !lower.is_finite() || !upper.is_finite() {
            return self.last_z;
        }
        let capped = MeanStd::from_values(
            self.raw_window
                .iter()
                .flatten()
                .map(|value| value.clamp(lower, upper)),
        );
        let Some(std) = capped.std() else {
            return self.last_z;
        };
        let z = if std > 0.0 {
            (raw.clamp(lower, upper) - capped.mean) / std
        } else {
            0.0
        };
        if !z.is_finite() {
            return self.last_z;
        }
        self.last_z = Some(z);
        Some(z)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch_z(window: &[f64], min_periods: usize, clip: f64) -> Option<f64> {
        let current = *window.last()?;
        if !current.is_finite() {
            return None;
        }
        let values = window
            .iter()
            .copied()
            .filter(|v| v.is_finite())
            .collect::<Vec<_>>();
        if values.len() < min_periods || values.len() < 2 {
            return None;
        }
        let mean_std = |values: &[f64]| {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            (mean, variance.sqrt())
        };
        let (mean, std) = mean_std(&values);
        let lower = mean - clip * std;
        let upper = mean + clip * std;
        let capped = values
            .into_iter()
            .map(|v| v.clamp(lower, upper))
            .collect::<Vec<_>>();
        let (mean, std) = mean_std(&capped);
        Some(if std > 0.0 {
            (current.clamp(lower, upper) - mean) / std
        } else {
            0.0
        })
    }

    #[test]
    fn reclips_historical_outliers_using_current_window() {
        let mut state = NormalizeState::new(24, 16, 3.0);
        let values = std::iter::once(100.0)
            .chain((1..24).map(f64::from))
            .collect::<Vec<_>>();
        let mut actual = None;
        for raw in &values {
            actual = state.observe(*raw);
        }
        let expected = batch_z(&values, 16, 3.0).unwrap();
        assert!((actual.unwrap() - expected).abs() < 1e-12);
        // Leaving the early outlier at its original value gives another z.
        let uncapped = batch_z(&values, 16, 100.0).unwrap();
        assert!((expected - uncapped).abs() > 1e-3);
    }

    #[test]
    fn same_current_raw_window_gives_same_z_after_different_histories() {
        let mut positive = NormalizeState::new(24, 16, 3.0);
        let mut negative = NormalizeState::new(24, 16, 3.0);
        for _ in 0..24 {
            positive.observe(10_000.0);
            negative.observe(-10_000.0);
        }
        let mut a = None;
        let mut b = None;
        for raw in (1..=24).map(f64::from) {
            a = positive.observe(raw);
            b = negative.observe(raw);
        }
        assert_eq!(a, b);
        assert!(
            (a.unwrap() - batch_z(&(1..=24).map(f64::from).collect::<Vec<_>>(), 16, 3.0).unwrap())
                .abs()
                < 1e-12
        );
    }

    #[test]
    fn rolling_results_match_independent_batch_with_missing_and_expired_values() {
        for (window, min_periods) in [(24, 16), (96, 67), (288, 200), (1440, 1000)] {
            let mut state = NormalizeState::new(window, min_periods, 3.0);
            let values = (0..window * 2 + 3)
                .map(|i| match i % 43 {
                    0 => f64::NAN,
                    1 => 1000.0,
                    2 => -500.0,
                    _ => (i % 29) as f64,
                })
                .collect::<Vec<_>>();
            let mut expected = None;
            for (i, raw) in values.iter().enumerate() {
                expected = batch_z(
                    &values[(i + 1).saturating_sub(window)..=i],
                    min_periods,
                    3.0,
                )
                .or(expected);
                let actual = state.observe(*raw);
                match (actual, expected) {
                    (Some(a), Some(b)) => {
                        assert!((a - b).abs() < 1e-10, "window={window} i={i}: {a} != {b}")
                    }
                    (None, None) => {}
                    _ => panic!("window={window} i={i}: {actual:?} != {expected:?}"),
                }
            }
        }
    }

    #[test]
    fn preserves_sample_std_zero_variance_and_causal_forward_fill() {
        let mut state = NormalizeState::new(3, 3, 3.0);
        assert_eq!(state.observe(1.0), None);
        assert_eq!(state.observe(2.0), None);
        assert_eq!(state.observe(3.0), Some(1.0));
        // Missing slots age out valid values, but do not invent a new z.
        assert_eq!(state.observe(f64::NAN), Some(1.0));
        assert_eq!(state.observe(f64::INFINITY), Some(1.0));
        assert_eq!(state.observe(5.0), Some(1.0));
        assert_eq!(state.observe(5.0), Some(1.0));
        assert_eq!(state.observe(5.0), Some(0.0));
    }
}
