use std::cell::RefCell;
use std::collections::HashMap;

use crate::common::exact_rolling_window::ExactRollingWindow;

pub(crate) const INLINE_VOLATILITY_WINDOW_CAPACITY: usize = 720;
pub(crate) const INLINE_VOLATILITY_MIN_SAMPLES: usize = 10;

#[derive(Debug, Clone, Copy)]
pub(crate) struct InlineVolatilitySnapshot {
    pub(crate) current: f64,
    pub(crate) threshold: Option<f64>,
    pub(crate) sample_count: usize,
    pub(crate) percentile: f64,
    pub(crate) last_recompute_tp_ms: Option<i64>,
    pub(crate) recomputed: bool,
}

#[derive(Debug)]
struct InlineQuantileWindow {
    window: ExactRollingWindow,
    last_update_tp_ms: i64,
}

impl Default for InlineQuantileWindow {
    fn default() -> Self {
        Self {
            window: ExactRollingWindow::new(INLINE_VOLATILITY_WINDOW_CAPACITY),
            last_update_tp_ms: 0,
        }
    }
}

impl InlineQuantileWindow {
    fn observe(&mut self, current: f64, percentile: f64, now_ms: i64) -> InlineVolatilitySnapshot {
        let normalized_percentile = percentile.clamp(0.0, 100.0);
        let _ = self.window.observe(current);

        let sample_count = self.window.len();
        let threshold = if sample_count >= INLINE_VOLATILITY_MIN_SAMPLES {
            self.window.quantile_floor(normalized_percentile)
        } else {
            None
        };
        let recomputed = threshold.is_some();
        if recomputed {
            self.last_update_tp_ms = now_ms;
        }

        InlineVolatilitySnapshot {
            current,
            threshold,
            sample_count,
            percentile: normalized_percentile,
            last_recompute_tp_ms: (self.last_update_tp_ms > 0).then_some(self.last_update_tp_ms),
            recomputed,
        }
    }

    fn snapshot(&self, current: f64, percentile: f64) -> InlineVolatilitySnapshot {
        let normalized_percentile = percentile.clamp(0.0, 100.0);
        let sample_count = self.window.len();
        let threshold = if sample_count >= INLINE_VOLATILITY_MIN_SAMPLES {
            self.window.quantile_floor(normalized_percentile)
        } else {
            None
        };

        InlineVolatilitySnapshot {
            current,
            threshold,
            sample_count,
            percentile: normalized_percentile,
            last_recompute_tp_ms: (self.last_update_tp_ms > 0).then_some(self.last_update_tp_ms),
            recomputed: false,
        }
    }
}

#[derive(Debug, Default)]
struct InlineQuantileStore {
    windows: HashMap<String, InlineQuantileWindow>,
}

impl InlineQuantileStore {
    fn observe(
        &mut self,
        symbol_key: &str,
        current: f64,
        percentile: f64,
        now_ms: i64,
    ) -> InlineVolatilitySnapshot {
        self.windows
            .entry(symbol_key.to_ascii_uppercase())
            .or_default()
            .observe(current, percentile, now_ms)
    }

    fn snapshot(
        &self,
        symbol_key: &str,
        current: f64,
        percentile: f64,
    ) -> InlineVolatilitySnapshot {
        self.windows
            .get(&symbol_key.to_ascii_uppercase())
            .map(|window| window.snapshot(current, percentile))
            .unwrap_or(InlineVolatilitySnapshot {
                current,
                threshold: None,
                sample_count: 0,
                percentile: percentile.clamp(0.0, 100.0),
                last_recompute_tp_ms: None,
                recomputed: false,
            })
    }
}

thread_local! {
    static INLINE_VOLATILITY_STORE: RefCell<InlineQuantileStore> =
        RefCell::new(InlineQuantileStore::default());
    static INLINE_TRADECOUNT_STORE: RefCell<InlineQuantileStore> =
        RefCell::new(InlineQuantileStore::default());
}

pub(crate) fn observe_inline_volatility(
    symbol_key: &str,
    current: f64,
    percentile: f64,
    now_ms: i64,
) -> InlineVolatilitySnapshot {
    INLINE_VOLATILITY_STORE.with(|store| {
        store
            .borrow_mut()
            .observe(symbol_key, current, percentile, now_ms)
    })
}

pub(crate) fn snapshot_inline_volatility(
    symbol_key: &str,
    current: f64,
    percentile: f64,
) -> InlineVolatilitySnapshot {
    INLINE_VOLATILITY_STORE.with(|store| store.borrow().snapshot(symbol_key, current, percentile))
}

pub(crate) fn observe_inline_tradecount(
    symbol_key: &str,
    current: f64,
    percentile: f64,
    now_ms: i64,
) -> InlineVolatilitySnapshot {
    INLINE_TRADECOUNT_STORE.with(|store| {
        store
            .borrow_mut()
            .observe(symbol_key, current, percentile, now_ms)
    })
}

pub(crate) fn snapshot_inline_tradecount(
    symbol_key: &str,
    current: f64,
    percentile: f64,
) -> InlineVolatilitySnapshot {
    INLINE_TRADECOUNT_STORE.with(|store| store.borrow().snapshot(symbol_key, current, percentile))
}

#[cfg(test)]
mod tests {
    use super::{
        InlineQuantileWindow, INLINE_VOLATILITY_MIN_SAMPLES, INLINE_VOLATILITY_WINDOW_CAPACITY,
    };

    #[test]
    fn threshold_requires_enough_samples() {
        let mut window = InlineQuantileWindow::default();
        for i in 0..(INLINE_VOLATILITY_MIN_SAMPLES - 1) {
            let snapshot = window.observe(i as f64, 70.0, i as i64);
            assert_eq!(snapshot.threshold, None);
            assert_eq!(snapshot.sample_count, i + 1);
        }
    }

    #[test]
    fn threshold_uses_sorted_index_statistic() {
        let mut window = InlineQuantileWindow::default();
        for (idx, value) in [5.0, 1.0, 8.0, 3.0, 7.0, 2.0, 9.0, 4.0, 6.0, 10.0, 11.0]
            .into_iter()
            .enumerate()
        {
            let snapshot = window.observe(value, 70.0, idx as i64);
            if idx + 1 == INLINE_VOLATILITY_MIN_SAMPLES {
                assert_eq!(snapshot.threshold, Some(7.0));
                assert!(snapshot.recomputed);
            }
        }
    }

    #[test]
    fn threshold_updates_online_with_window() {
        let mut window = InlineQuantileWindow::default();
        for i in 0..INLINE_VOLATILITY_MIN_SAMPLES {
            let _ = window.observe(i as f64, 50.0, i as i64);
        }

        let snapshot = window.observe(10_000.0, 50.0, INLINE_VOLATILITY_MIN_SAMPLES as i64);
        assert_eq!(snapshot.threshold, Some(5.0));
        assert!(snapshot.recomputed);
    }

    #[test]
    fn threshold_recomputes_after_percentile_change() {
        let mut window = InlineQuantileWindow::default();
        for i in 0..INLINE_VOLATILITY_MIN_SAMPLES {
            let _ = window.observe(i as f64, 50.0, i as i64);
        }

        let snapshot = window.observe(11.0, 80.0, 100);
        assert!(snapshot.recomputed);
        assert_eq!(snapshot.threshold, Some(8.0));
    }

    #[test]
    fn threshold_tracks_eviction_and_binary_insert() {
        let mut window = InlineQuantileWindow::default();
        for i in 0..INLINE_VOLATILITY_WINDOW_CAPACITY {
            let _ = window.observe(i as f64, 50.0, i as i64);
        }

        let snapshot = window.observe(10_000.0, 50.0, 2_000);
        assert_eq!(snapshot.sample_count, INLINE_VOLATILITY_WINDOW_CAPACITY);
        assert_eq!(snapshot.threshold, Some(360.0));
    }
}
