use crate::rolling_metrics::kll_quantile::segmented_quantiles_linear;

/// 累积每条 publish 后的本地接收延迟（µs），每 10000 条用 KLL 算 p90/p95/p99 输出一次。
///
/// 单线程持有，无锁。10000 条 f64 ≈ 80 KB，KLL 计算同步在 task 内完成（开销可忽略）。
pub struct LatencyKll {
    label: String,
    buffer: Vec<f64>,
    capacity: usize,
}

impl LatencyKll {
    pub const DEFAULT_CAPACITY: usize = 10_000;

    pub fn new(label: impl Into<String>) -> Self {
        Self::with_capacity(label, Self::DEFAULT_CAPACITY)
    }

    pub fn with_capacity(label: impl Into<String>, capacity: usize) -> Self {
        Self {
            label: label.into(),
            buffer: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// 推入一个延迟样本（单位 µs）。满则同步 flush。
    pub fn push(&mut self, delta_us: f64) {
        self.buffer.push(delta_us);
        if self.buffer.len() >= self.capacity {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let qs = [0.90_f32, 0.95, 0.99];
        let (n, results) = segmented_quantiles_linear(self.buffer.iter().copied(), self.capacity, &qs);
        let p90 = results.first().and_then(|v| *v).unwrap_or(f64::NAN);
        let p95 = results.get(1).and_then(|v| *v).unwrap_or(f64::NAN);
        let p99 = results.get(2).and_then(|v| *v).unwrap_or(f64::NAN);
        log::info!(
            "spread_pbs[{}] latency_us n={} p90={:.0} p95={:.0} p99={:.0}",
            self.label, n, p90, p95, p99
        );
        self.buffer.clear();
    }
}
