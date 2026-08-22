use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct ExactRollingWindow {
    capacity: usize,
    fifo: VecDeque<f64>,
    sorted: Vec<f64>,
}

impl ExactRollingWindow {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            fifo: VecDeque::with_capacity(capacity),
            sorted: Vec::with_capacity(capacity),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.fifo.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fifo.is_empty()
    }

    pub fn last(&self) -> Option<f64> {
        self.fifo.back().copied()
    }

    pub fn observe(&mut self, value: f64) -> bool {
        if !value.is_finite() {
            return false;
        }

        if self.fifo.len() == self.capacity {
            if let Some(expired) = self.fifo.pop_front() {
                self.remove_sorted(expired);
            }
        }

        self.fifo.push_back(value);
        self.insert_sorted(value);
        true
    }

    pub fn reconfigure(&mut self, capacity: usize) {
        self.capacity = capacity.max(1);
        while self.fifo.len() > self.capacity {
            let _ = self.fifo.pop_front();
        }
        self.rebuild_sorted();
    }

    pub fn quantile_floor(&self, percentile: f64) -> Option<f64> {
        if self.sorted.is_empty() || !percentile.is_finite() {
            return None;
        }
        let percentile = percentile.clamp(0.0, 100.0);
        let idx =
            ((self.sorted.len().saturating_sub(1)) as f64 * (percentile / 100.0)).floor() as usize;
        self.sorted.get(idx).copied()
    }

    pub fn quantile_linear(&self, q: f32) -> Option<f64> {
        if self.sorted.is_empty() || !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return None;
        }
        if self.sorted.len() == 1 {
            return self.sorted.first().copied();
        }

        let rank = (q as f64) * ((self.sorted.len() - 1) as f64);
        let lower_idx = rank.floor() as usize;
        let upper_idx = rank.ceil() as usize;
        let frac = rank - lower_idx as f64;
        let lower = self.sorted[lower_idx];
        if lower_idx == upper_idx {
            return Some(lower);
        }
        let upper = self.sorted[upper_idx];
        Some(lower + (upper - lower) * frac)
    }

    pub fn quantiles_linear(&self, qs: &[f32]) -> Vec<Option<f64>> {
        qs.iter().map(|&q| self.quantile_linear(q)).collect()
    }

    pub fn percentile_rank(&self, value: f64) -> Option<f64> {
        if self.sorted.is_empty() || !value.is_finite() {
            return None;
        }
        let less = self.lower_bound(value);
        let less_or_equal = self.upper_bound(value);
        let equal = less_or_equal.saturating_sub(less);
        Some((less as f64 + (equal as f64 * 0.5)) / self.sorted.len() as f64)
    }

    pub fn percentile_rank_last(&self) -> Option<f64> {
        self.last().and_then(|value| self.percentile_rank(value))
    }

    fn insert_sorted(&mut self, value: f64) {
        let idx = self.lower_bound(value);
        self.sorted.insert(idx, value);
    }

    fn remove_sorted(&mut self, value: f64) {
        let mut idx = self.lower_bound(value);
        while idx < self.sorted.len() {
            if self.sorted[idx].to_bits() == value.to_bits() {
                self.sorted.remove(idx);
                return;
            }
            if self.sorted[idx].total_cmp(&value).is_gt() {
                return;
            }
            idx += 1;
        }
    }

    fn rebuild_sorted(&mut self) {
        self.sorted.clear();
        self.sorted.extend(self.fifo.iter().copied());
        self.sorted.sort_by(|a, b| a.total_cmp(b));
    }

    fn lower_bound(&self, value: f64) -> usize {
        self.sorted
            .partition_point(|probe| probe.total_cmp(&value).is_lt())
    }

    fn upper_bound(&self, value: f64) -> usize {
        self.sorted
            .partition_point(|probe| !probe.total_cmp(&value).is_gt())
    }
}

#[cfg(test)]
mod tests {
    use super::ExactRollingWindow;

    #[test]
    fn quantile_floor_tracks_fifo_window() {
        let mut window = ExactRollingWindow::new(3);
        for value in [1.0, 3.0, 2.0] {
            assert!(window.observe(value));
        }
        assert_eq!(window.quantile_floor(50.0), Some(2.0));

        assert!(window.observe(10.0));
        assert_eq!(window.len(), 3);
        assert_eq!(window.quantile_floor(50.0), Some(3.0));
    }

    #[test]
    fn percentile_rank_uses_midpoint_rank_for_duplicates() {
        let mut window = ExactRollingWindow::new(5);
        for value in [1.0, 2.0, 2.0, 4.0] {
            assert!(window.observe(value));
        }

        assert_eq!(window.percentile_rank(2.0), Some(0.5));
        assert_eq!(window.percentile_rank_last(), Some(0.875));
    }

    #[test]
    fn reconfigure_rebuilds_from_latest_fifo_values() {
        let mut window = ExactRollingWindow::new(5);
        for value in [1.0, 2.0, 3.0, 4.0] {
            assert!(window.observe(value));
        }

        window.reconfigure(2);
        assert_eq!(window.len(), 2);
        assert_eq!(window.quantile_floor(0.0), Some(3.0));
        assert_eq!(window.quantile_floor(100.0), Some(4.0));
    }

    #[test]
    fn removes_exact_bit_pattern_when_values_compare_equal() {
        let a = 0.0f64;
        let b = -0.0f64;
        let mut window = ExactRollingWindow::new(2);
        assert!(window.observe(a));
        assert!(window.observe(b));
        assert!(window.observe(1.0));

        assert_eq!(window.len(), 2);
        assert_eq!(window.last(), Some(1.0));
        assert!(window.percentile_rank(-0.0).is_some());
    }
}
