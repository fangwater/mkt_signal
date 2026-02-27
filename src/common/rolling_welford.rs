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

    /// Z-score with 3σ clipping: clamp the latest value to [mean - k*std, mean + k*std]
    /// before computing (clamped - mean) / std.
    /// Returns `None` if fewer than 2 samples or std is zero.
    #[inline]
    pub fn zscore_capped(&self, k: f64) -> Option<f64> {
        let s = self.std();
        if self.buf.len() < 2 || s == 0.0 {
            return None;
        }
        let last = *self.buf.back().unwrap();
        let clamped = last.clamp(self.mean - k * s, self.mean + k * s);
        Some((clamped - self.mean) / s)
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

/// Online bivariate Welford accumulator for covariance/correlation.
/// Uses a single pass and is numerically stable for long windows.
pub struct WelfordCovariance {
    n: usize,
    mean_x: f64,
    mean_y: f64,
    m2_x: f64,
    m2_y: f64,
    c2_xy: f64,
}

impl WelfordCovariance {
    pub fn new() -> Self {
        Self {
            n: 0,
            mean_x: 0.0,
            mean_y: 0.0,
            m2_x: 0.0,
            m2_y: 0.0,
            c2_xy: 0.0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.n
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    #[inline]
    pub fn reset(&mut self) {
        self.n = 0;
        self.mean_x = 0.0;
        self.mean_y = 0.0;
        self.m2_x = 0.0;
        self.m2_y = 0.0;
        self.c2_xy = 0.0;
    }

    #[inline]
    pub fn push(&mut self, x: f64, y: f64) {
        self.n += 1;
        let n = self.n as f64;

        let dx = x - self.mean_x;
        self.mean_x += dx / n;
        let dy = y - self.mean_y;
        self.mean_y += dy / n;

        self.m2_x += dx * (x - self.mean_x);
        self.m2_y += dy * (y - self.mean_y);
        self.c2_xy += dx * (y - self.mean_y);
    }

    #[inline]
    pub fn remove(&mut self, x: f64, y: f64) {
        if self.n <= 1 {
            self.reset();
            return;
        }

        let n = self.n as f64;
        let dx = x - self.mean_x;
        let dy = y - self.mean_y;

        let next_mean_x = self.mean_x - dx / (n - 1.0);
        let next_mean_y = self.mean_y - dy / (n - 1.0);

        self.m2_x -= dx * (x - next_mean_x);
        self.m2_y -= dy * (y - next_mean_y);
        self.c2_xy -= dx * (y - next_mean_y);

        self.mean_x = next_mean_x;
        self.mean_y = next_mean_y;
        self.n -= 1;

        // Guard against tiny negative drift from floating-point arithmetic.
        if self.m2_x < 0.0 {
            self.m2_x = 0.0;
        }
        if self.m2_y < 0.0 {
            self.m2_y = 0.0;
        }
    }

    #[inline]
    pub fn corr(&self) -> Option<f64> {
        if self.n < 2 {
            return None;
        }
        let denom = self.m2_x.sqrt() * self.m2_y.sqrt();
        if denom.abs() <= 1e-12 {
            return None;
        }
        Some(self.c2_xy / denom)
    }
}

/// Fixed-window rolling correlation built on top of Welford covariance with remove support.
pub struct RollingWelfordCovariance {
    buf: VecDeque<(f64, f64, bool)>,
    capacity: usize,
    invalid_count: usize,
    stats: WelfordCovariance,
}

impl RollingWelfordCovariance {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        Self {
            buf: VecDeque::with_capacity(capacity),
            capacity,
            invalid_count: 0,
            stats: WelfordCovariance::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    pub fn reset(&mut self) {
        self.buf.clear();
        self.invalid_count = 0;
        self.stats.reset();
    }

    #[inline]
    pub fn push(&mut self, x: f64, y: f64) {
        if self.buf.len() == self.capacity {
            if let Some((ox, oy, finite)) = self.buf.pop_front() {
                if finite {
                    self.stats.remove(ox, oy);
                } else {
                    self.invalid_count = self.invalid_count.saturating_sub(1);
                }
            }
        }

        let finite = x.is_finite() && y.is_finite();
        if finite {
            self.stats.push(x, y);
        } else {
            self.invalid_count += 1;
        }
        self.buf.push_back((x, y, finite));
    }

    /// Correlation under fusion-factor semantics:
    /// - fewer than 2 samples => None
    /// - any non-finite pair in window => Some(0.0)
    /// - degenerate variance => Some(0.0)
    #[inline]
    pub fn corr(&self) -> Option<f64> {
        if self.buf.len() < 2 {
            return None;
        }
        if self.invalid_count > 0 {
            return Some(0.0);
        }
        self.stats.corr().or(Some(0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

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

    #[test]
    fn welford_covariance_corr_works() {
        let mut stats = WelfordCovariance::new();
        for i in 1..=10 {
            stats.push(i as f64, (i * 3) as f64);
        }
        let corr = stats.corr().expect("corr should exist");
        assert!((corr - 1.0).abs() < 1e-12);

        stats.reset();
        for i in 1..=10 {
            stats.push(i as f64, -(i as f64));
        }
        let corr = stats.corr().expect("corr should exist");
        assert!((corr + 1.0).abs() < 1e-12);
    }

    fn naive_corr_like_fusion(window: &VecDeque<(f64, f64)>) -> Option<f64> {
        if window.len() < 2 {
            return None;
        }
        if window.iter().any(|(x, y)| !x.is_finite() || !y.is_finite()) {
            return Some(0.0);
        }

        let n = window.len() as f64;
        let sum_x = window.iter().map(|(x, _)| *x).sum::<f64>();
        let sum_y = window.iter().map(|(_, y)| *y).sum::<f64>();
        let sum_xx = window.iter().map(|(x, _)| x * x).sum::<f64>();
        let sum_yy = window.iter().map(|(_, y)| y * y).sum::<f64>();
        let sum_xy = window.iter().map(|(x, y)| x * y).sum::<f64>();

        let cov = sum_xy - (sum_x * sum_y) / n;
        let var_x = sum_xx - (sum_x * sum_x) / n;
        let var_y = sum_yy - (sum_y * sum_y) / n;
        if var_x.abs() <= 1e-12 || var_y.abs() <= 1e-12 {
            return Some(0.0);
        }
        let out = cov / (var_x.sqrt() * var_y.sqrt());
        if out.is_finite() {
            Some(out)
        } else {
            Some(0.0)
        }
    }

    #[test]
    fn rolling_welford_covariance_matches_naive() {
        let capacity = 64usize;
        let mut rolling = RollingWelfordCovariance::new(capacity);
        let mut naive: VecDeque<(f64, f64)> = VecDeque::with_capacity(capacity);

        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            // Simple deterministic LCG for repeatable tests.
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let u = (seed >> 11) as f64 / ((1u64 << 53) as f64);
            2.0 * u - 1.0
        };

        for i in 0..5000 {
            let mut x = next() * 1000.0 + i as f64 * 1e-3;
            let mut y = next() * 1000.0 - i as f64 * 1e-3;
            if i % 127 == 0 {
                x = f64::NAN;
            }
            if i % 193 == 0 {
                y = f64::INFINITY;
            }

            rolling.push(x, y);
            if naive.len() == capacity {
                naive.pop_front();
            }
            naive.push_back((x, y));

            let got = rolling.corr();
            let expected = naive_corr_like_fusion(&naive);
            match (got, expected) {
                (None, None) => {}
                (Some(a), Some(b)) => {
                    assert!(
                        (a - b).abs() < 1e-7,
                        "corr mismatch at i={}: got={} expected={}",
                        i,
                        a,
                        b
                    );
                }
                _ => panic!(
                    "option mismatch at i={}: got={:?} expected={:?}",
                    i, got, expected
                ),
            }
        }
    }
}
