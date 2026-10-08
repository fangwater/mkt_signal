use rolling_common::kll_quantile::segmented_quantiles_linear;
use rolling_common::latency_kll::LatencyStats;
use std::time::{Duration, Instant};

/// 累积被采纳消息的本地接收延迟（µs），按样本数或时间窗口 flush。
///
/// 单线程持有，无锁。10000 条 f64 ≈ 80 KB，KLL 计算同步在 task 内完成（开销可忽略）。
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
            "spread_pbs[{}] latency_us n={} p50={:.0} p90={:.0} p95={:.0} p99={:.0}",
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

    /// 取当前窗口快照并清空 buffer。空窗口返回 None。
    pub fn snapshot_and_reset(&mut self) -> Option<LatencyStats> {
        if self.buffer.is_empty() {
            self.window_start = Instant::now();
            return None;
        }
        let qs = [0.50_f32, 0.90, 0.95, 0.99];
        let (n, results) =
            segmented_quantiles_linear(self.buffer.iter().copied(), self.capacity, &qs);
        let to_i64 = |v: Option<f64>| {
            v.and_then(|x| (x.is_finite()).then_some(x as i64))
                .unwrap_or(0)
        };
        let stats = LatencyStats {
            n: n as u64,
            p50_us: to_i64(results.first().and_then(|v| *v)),
            p90_us: to_i64(results.get(1).and_then(|v| *v)),
            p95_us: to_i64(results.get(2).and_then(|v| *v)),
            p99_us: to_i64(results.get(3).and_then(|v| *v)),
        };
        self.buffer.clear();
        self.window_start = Instant::now();
        Some(stats)
    }
}

// Sampled local receive-to-IPC time, without an exchange-clock dependency.
#[derive(Default)]
pub struct PublicationLatency {
    samples: u64,
    total_us: u64,
    max_us: u64,
}

impl PublicationLatency {
    pub fn should_sample(published: u64) -> bool {
        published % 1024 == 0
    }

    pub fn record(&mut self, recv_us: i64, published_us: i64) {
        // Wall-clock steps backwards cannot represent processing time.
        let Some(delta) = published_us.checked_sub(recv_us).filter(|v| *v >= 0) else {
            return;
        };
        self.samples += 1;
        self.total_us = self.total_us.saturating_add(delta as u64);
        self.max_us = self.max_us.max(delta as u64);
    }

    pub fn log_and_reset(&mut self, venue: &str) {
        if self.samples == 0 {
            return;
        }
        log::info!(
            "spread_pbs[{}] bbo_recv_to_ipc_us sample_every=1024 samples={} mean={} max={}",
            venue,
            self.samples,
            self.total_us / self.samples,
            self.max_us
        );
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampled_local_duration_ignores_backwards_clock_steps() {
        let mut stats = PublicationLatency::default();
        assert!(PublicationLatency::should_sample(0));
        assert!(!PublicationLatency::should_sample(1));
        assert!(PublicationLatency::should_sample(1024));
        stats.record(100, 110);
        stats.record(100, 130);
        stats.record(100, 90);
        assert_eq!((stats.samples, stats.total_us, stats.max_us), (2, 40, 30));
        stats.log_and_reset("test");
        assert_eq!((stats.samples, stats.total_us, stats.max_us), (0, 0, 0));
    }
}
