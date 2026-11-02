//! Causal raw clipping and normalization shared by both factor workers.
use std::collections::VecDeque;

struct RollingMeanStd {
    capacity: usize,
    fifo: VecDeque<Option<f64>>,
    sum: f64,
    sum_sq: f64,
    count: usize,
}

impl RollingMeanStd {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            fifo: VecDeque::with_capacity(capacity.max(1)),
            sum: 0.0,
            sum_sq: 0.0,
            count: 0,
        }
    }

    fn push_slot(&mut self, value: f64) {
        let value = value.is_finite().then_some(value);
        if self.fifo.len() == self.capacity {
            if let Some(Some(expired)) = self.fifo.pop_front() {
                self.sum -= expired;
                self.sum_sq -= expired * expired;
                self.count -= 1;
            }
        }
        self.fifo.push_back(value);
        if let Some(value) = value {
            self.sum += value;
            self.sum_sq += value * value;
            self.count += 1;
        }
    }

    fn len(&self) -> usize {
        self.count
    }

    fn mean(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }

    fn std(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }
        let n = self.count as f64;
        let variance = (self.sum_sq - self.sum * self.sum / n) / (n - 1.0);
        Some(variance.max(0.0).sqrt())
    }
}

/// Matches offline `normalize_factors`: clip raw at the trailing
/// mean +/- clip_zscore*std, standardize the clipped series over the same
/// window, map zero variance to 0, and causally carry the last z-score.
pub(crate) struct NormalizeState {
    raw_window: RollingMeanStd,
    capped_window: RollingMeanStd,
    min_periods: usize,
    clip_zscore: f64,
    last_z: Option<f64>,
}

impl NormalizeState {
    pub(crate) fn new(window: usize, min_periods: usize, clip_zscore: f64) -> Self {
        Self {
            raw_window: RollingMeanStd::new(window),
            capped_window: RollingMeanStd::new(window),
            min_periods,
            clip_zscore,
            last_z: None,
        }
    }

    pub(crate) fn observe(&mut self, raw: f64) -> Option<f64> {
        if !raw.is_finite() {
            self.raw_window.push_slot(f64::NAN);
            self.capped_window.push_slot(f64::NAN);
            return self.last_z;
        }
        self.raw_window.push_slot(raw);
        let capped = if self.raw_window.len() >= self.min_periods {
            match (self.raw_window.mean(), self.raw_window.std()) {
                (Some(mean), Some(std)) => {
                    raw.clamp(mean - self.clip_zscore * std, mean + self.clip_zscore * std)
                }
                _ => raw,
            }
        } else {
            raw
        };
        self.capped_window.push_slot(capped);
        let z = if self.capped_window.len() >= self.min_periods {
            match (self.capped_window.mean(), self.capped_window.std()) {
                (Some(mean), Some(std)) if std > 0.0 => (capped - mean) / std,
                (Some(_), Some(_)) => 0.0,
                _ => return self.last_z,
            }
        } else {
            return self.last_z;
        };
        if !z.is_finite() {
            return self.last_z;
        }
        self.last_z = Some(z);
        Some(z)
    }
}
