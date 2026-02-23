use std::collections::VecDeque;

/// Fixed-window rolling mean / std using Welford's online algorithm with remove support.
/// Numerically stable for large windows (10k+ samples).
pub struct RollingWelford {
    buf: VecDeque<f64>,
    capacity: usize,
    mean: f64,
    m2: f64,
}

impl RollingWelford {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        Self {
            buf: VecDeque::with_capacity(capacity),
            capacity,
            mean: 0.0,
            m2: 0.0,
        }
    }

    /// Push a new value. If the window is full, the oldest value is evicted.
    #[inline]
    pub fn push(&mut self, x: f64) {
        if self.buf.len() == self.capacity {
            // remove before pop so self.buf.len() still equals capacity inside remove()
            let old = *self.buf.front().unwrap();
            self.remove(old);
            self.buf.pop_front();
        }
        self.add(x);
        self.buf.push_back(x);
    }

    /// Current number of samples in the window.
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Returns true when the window is fully filled.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.buf.len() == self.capacity
    }

    #[inline]
    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Population variance (divide by n).
    #[inline]
    pub fn var_pop(&self) -> f64 {
        let n = self.buf.len();
        if n < 2 {
            return 0.0;
        }
        (self.m2 / n as f64).max(0.0)
    }

    /// Sample variance (divide by n-1).
    #[inline]
    pub fn var(&self) -> f64 {
        let n = self.buf.len();
        if n < 2 {
            return 0.0;
        }
        (self.m2 / (n - 1) as f64).max(0.0)
    }

    /// Population standard deviation.
    #[inline]
    pub fn std_pop(&self) -> f64 {
        self.var_pop().sqrt()
    }

    /// Sample standard deviation.
    #[inline]
    pub fn std(&self) -> f64 {
        self.var().sqrt()
    }

    /// Z-score of the most recent value: (last - mean) / std.
    /// Returns `None` if fewer than 2 samples or std is zero.
    #[inline]
    pub fn zscore(&self) -> Option<f64> {
        let s = self.std();
        if self.buf.len() < 2 || s == 0.0 {
            return None;
        }
        let last = *self.buf.back().unwrap();
        Some((last - self.mean) / s)
    }

    /// Reset to empty state.
    pub fn reset(&mut self) {
        self.buf.clear();
        self.mean = 0.0;
        self.m2 = 0.0;
    }

    // --- internals ---

    #[inline]
    fn add(&mut self, x: f64) {
        let n = self.buf.len() as f64 + 1.0;
        let delta = x - self.mean;
        self.mean += delta / n;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    #[inline]
    fn remove(&mut self, x: f64) {
        let n = self.buf.len() as f64; // count before removal
        if n <= 1.0 {
            self.mean = 0.0;
            self.m2 = 0.0;
            return;
        }
        let delta = x - self.mean;
        self.mean -= delta / (n - 1.0);
        let delta2 = x - self.mean;
        self.m2 -= delta * delta2;
        // guard against floating-point drift
        if self.m2 < 0.0 {
            self.m2 = 0.0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn naive_mean(v: &[f64]) -> f64 {
        v.iter().sum::<f64>() / v.len() as f64
    }

    fn naive_std(v: &[f64]) -> f64 {
        let m = naive_mean(v);
        let var = v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() - 1) as f64;
        var.sqrt()
    }

    #[test]
    fn basic_rolling() {
        let mut rw = RollingWelford::new(3);
        rw.push(1.0);
        rw.push(2.0);
        rw.push(3.0);
        assert!((rw.mean() - 2.0).abs() < 1e-12);
        assert!((rw.std() - 1.0).abs() < 1e-12);

        // push evicts 1.0, window is [2, 3, 4]
        rw.push(4.0);
        assert!((rw.mean() - 3.0).abs() < 1e-12);
        assert!((rw.std() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn large_window_accuracy() {
        let cap = 17280; // 24h at 5s interval
        let mut rw = RollingWelford::new(cap);
        let mut data = Vec::with_capacity(cap);

        // fill with pseudo-random-ish values
        for i in 0..(cap + 5000) {
            let v = 100.0 + 0.01 * (i as f64).sin() * 50.0;
            rw.push(v);
            data.push(v);
        }

        // compare with naive on the last `cap` elements
        let window: Vec<f64> = data[data.len() - cap..].to_vec();
        let expected_mean = naive_mean(&window);
        let expected_std = naive_std(&window);

        assert!(
            (rw.mean() - expected_mean).abs() < 1e-8,
            "mean drift: {} vs {}",
            rw.mean(),
            expected_mean
        );
        assert!(
            (rw.std() - expected_std).abs() < 1e-6,
            "std drift: {} vs {}",
            rw.std(),
            expected_std
        );
    }

    #[test]
    fn zscore_works() {
        let mut rw = RollingWelford::new(100);
        for i in 0..100 {
            rw.push(i as f64);
        }
        let z = rw.zscore().unwrap();
        // last value is 99, mean ~49.5, std ~29.01
        assert!(z > 0.0);
        assert!((z - (99.0 - rw.mean()) / rw.std()).abs() < 1e-12);
    }
}
