//! Time-based TCP retransmission window used by websocket connection health.
//!
//! The kernel counters come from `TCP_INFO`. Any positive retransmission
//! delta is considered unhealthy; the ratio is retained only for monitoring.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<T>().ok())
        .unwrap_or(default)
}

#[derive(Clone, Debug)]
pub struct TcpLossHealthConfig {
    pub window: Duration,
    pub pause: Duration,
}

impl Default for TcpLossHealthConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_millis(1_000),
            pause: Duration::from_millis(3_000),
        }
    }
}

impl TcpLossHealthConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            window: Duration::from_millis(env_parse(
                "TRADE_ENGINE_TCP_HEALTH_WINDOW_MS",
                default.window.as_millis() as u64,
            )),
            pause: Duration::from_millis(env_parse(
                "TRADE_ENGINE_TCP_HEALTH_PAUSE_MS",
                default.pause.as_millis() as u64,
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Healthy,
    Pause,
}

pub struct TcpLossHealth {
    cfg: TcpLossHealthConfig,
    samples: VecDeque<(Instant, u64, u64)>,
    window_retrans: u64,
    window_data_segs: u64,
    last_retrans_at: Option<Instant>,
}

impl TcpLossHealth {
    pub fn new(cfg: TcpLossHealthConfig) -> Self {
        Self {
            cfg,
            samples: VecDeque::new(),
            window_retrans: 0,
            window_data_segs: 0,
            last_retrans_at: None,
        }
    }

    pub fn pause_duration(&self) -> Duration {
        self.cfg.pause
    }

    pub fn window_duration(&self) -> Duration {
        self.cfg.window
    }

    pub fn window_counts(&self) -> (u64, u64) {
        (self.window_retrans, self.window_data_segs)
    }

    pub fn window_bp(&self) -> i64 {
        if self.window_data_segs == 0 {
            return 0;
        }
        (self.window_retrans.saturating_mul(10_000) / self.window_data_segs) as i64
    }

    pub fn last_retrans_age(&self, now: Instant) -> Option<Duration> {
        self.last_retrans_at
            .map(|at| now.saturating_duration_since(at))
    }

    pub fn record(&mut self, d_retrans: u64, d_data_segs: u64, now: Instant) -> Verdict {
        self.samples.push_back((now, d_retrans, d_data_segs));
        self.window_retrans = self.window_retrans.saturating_add(d_retrans);
        self.window_data_segs = self.window_data_segs.saturating_add(d_data_segs);
        if d_retrans > 0 {
            self.last_retrans_at = Some(now);
        }

        while let Some((at, retrans, segs)) = self.samples.front().copied() {
            if now.saturating_duration_since(at) < self.cfg.window {
                break;
            }
            self.samples.pop_front();
            self.window_retrans = self.window_retrans.saturating_sub(retrans);
            self.window_data_segs = self.window_data_segs.saturating_sub(segs);
        }

        if self.window_retrans > 0 {
            Verdict::Pause
        } else {
            Verdict::Healthy
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn any_retransmission_is_unhealthy_immediately() {
        let start = Instant::now();
        let mut health = TcpLossHealth::new(TcpLossHealthConfig::default());
        assert_eq!(health.record(1, 4, start), Verdict::Pause);
        assert_eq!(health.window_counts(), (1, 4));
        assert_eq!(health.window_bp(), 2_500);
    }

    #[test]
    fn retransmission_expires_after_one_second() {
        let start = Instant::now();
        let mut health = TcpLossHealth::new(TcpLossHealthConfig::default());
        health.record(1, 4, start);
        assert_eq!(
            health.record(0, 4, start + Duration::from_millis(999)),
            Verdict::Pause
        );
        assert_eq!(
            health.record(0, 1, start + Duration::from_millis(1_000)),
            Verdict::Healthy
        );
        assert_eq!(health.window_counts(), (0, 5));
    }
}
