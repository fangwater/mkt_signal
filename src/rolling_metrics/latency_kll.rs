use crate::rolling_metrics::kll_quantile::segmented_quantiles_linear;
use std::time::{Duration, Instant};

/// 累积延迟样本（µs），按样本数或时间窗口 flush。
///
/// 单线程持有，无锁；如需跨任务共享，外层包 `Mutex`/`RefCell` 即可。
/// 默认容量 10_000 条 f64 ≈ 80 KB，KLL 计算同步在 task 内完成（开销可忽略）。
/// 日志前缀 `[<label>] latency_us ...`，由 caller 通过 `label` 提供完整上下文。
pub struct LatencyKll {
    label: String,
    buffer: Vec<f64>,
    capacity: usize,
    max_window: Duration,
    window_start: Instant,
}

impl LatencyKll {
    pub const DEFAULT_CAPACITY: usize = 10_000;
    pub const DEFAULT_MAX_WINDOW: Duration = Duration::from_secs(30);

    pub fn new(label: impl Into<String>) -> Self {
        Self::with_capacity(label, Self::DEFAULT_CAPACITY)
    }

    pub fn with_capacity(label: impl Into<String>, capacity: usize) -> Self {
        Self {
            label: label.into(),
            buffer: Vec::with_capacity(capacity),
            capacity,
            max_window: Self::DEFAULT_MAX_WINDOW,
            window_start: Instant::now(),
        }
    }

    /// 推入一个延迟样本（单位 µs）。窗口超时或满则同步 flush。
    pub fn push(&mut self, delta_us: f64) {
        if self.window_start.elapsed() >= self.max_window {
            self.flush();
        }
        self.buffer.push(delta_us);
        if self.buffer.len() >= self.capacity {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            self.window_start = Instant::now();
            return;
        }
        let qs = [0.50_f32, 0.90, 0.95, 0.99];
        let (n, results) =
            segmented_quantiles_linear(self.buffer.iter().copied(), self.capacity, &qs);
        let p50 = results.first().and_then(|v| *v).unwrap_or(f64::NAN);
        let p90 = results.get(1).and_then(|v| *v).unwrap_or(f64::NAN);
        let p95 = results.get(2).and_then(|v| *v).unwrap_or(f64::NAN);
        let p99 = results.get(3).and_then(|v| *v).unwrap_or(f64::NAN);
        log::info!(
            "[{}] latency_us n={} p50={:.0} p90={:.0} p95={:.0} p99={:.0}",
            self.label,
            n,
            p50,
            p90,
            p95,
            p99
        );
        self.buffer.clear();
        self.window_start = Instant::now();
    }
}
