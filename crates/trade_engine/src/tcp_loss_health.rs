//! 基于 WS 连接底层 TCP 丢包（retransmission）的通用健康度判定（纯逻辑，可单测）。
//!
//! 每个 tick 喂入本窗口的 (Δtotal_retrans, Δdata_segs_out)，滚动窗口聚合出 retrans
//! 速率（万分比 bp），据此给出判定：
//! - `Healthy`：速率低 / 样本不足（不误判）。
//! - `Pause`：速率超软阈 → 路由绕开该连接（由 Dispatch 模式消费；自动恢复）。
//! - `Reconnect`：速率超硬阈且持续 `sustain_ticks`，且 `can_reconnect_now`（无 inflight）、
//!   且距上次换连超过最小间隔 → 拆连重建换路。
//!
//! 本模块只算信号，不涉及网络/路由；动作由 ws_client 按路由(rr/dispatch)施加。

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
    /// 滚动窗口 tick 数。
    pub window_ticks: usize,
    /// 软阈：窗口 retrans 速率(bp)超过则 Pause（路由绕开）。
    pub pause_bp: i64,
    /// 硬阈：窗口 retrans 速率(bp)超过、且持续，才考虑 Reconnect。
    pub reconnect_bp: i64,
    /// 硬阈需连续坏的 tick 数。
    pub sustain_ticks: usize,
    /// 窗口内 data_segs_out 总量下限，不足则判 Healthy（样本不足不误判）。
    pub min_segs: u64,
    /// 软暂停(route-away)的持续时长；坏 tick 持续时会每 tick 续期，恢复后自动到期。
    pub pause: Duration,
    /// 两次健康触发的换连之间的最小间隔（防重连风暴）。
    pub reconnect_min_interval: Duration,
}

impl Default for TcpLossHealthConfig {
    fn default() -> Self {
        Self {
            window_ticks: 6,
            pause_bp: 200,     // 2%
            reconnect_bp: 500, // 5%
            sustain_ticks: 3,
            min_segs: 20,
            pause: Duration::from_millis(3_000),
            reconnect_min_interval: Duration::from_millis(30_000),
        }
    }
}

impl TcpLossHealthConfig {
    /// 从 env 读阈值/窗口，缺省走 `default()`。
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            window_ticks: env_parse("TRADE_ENGINE_TCP_HEALTH_WINDOW", d.window_ticks),
            pause_bp: env_parse("TRADE_ENGINE_TCP_HEALTH_PAUSE_BP", d.pause_bp),
            reconnect_bp: env_parse("TRADE_ENGINE_TCP_HEALTH_RECONNECT_BP", d.reconnect_bp),
            sustain_ticks: env_parse("TRADE_ENGINE_TCP_HEALTH_SUSTAIN", d.sustain_ticks),
            min_segs: env_parse("TRADE_ENGINE_TCP_HEALTH_MIN_SEGS", d.min_segs),
            pause: Duration::from_millis(env_parse(
                "TRADE_ENGINE_TCP_HEALTH_PAUSE_MS",
                d.pause.as_millis() as u64,
            )),
            reconnect_min_interval: Duration::from_millis(env_parse(
                "TRADE_ENGINE_TCP_HEALTH_RECONNECT_MIN_INTERVAL_MS",
                d.reconnect_min_interval.as_millis() as u64,
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Healthy,
    Pause,
    Reconnect,
}

pub struct TcpLossHealth {
    cfg: TcpLossHealthConfig,
    ring: VecDeque<(u64, u64)>, // (d_retrans, d_data_segs)
    consecutive_bad: usize,
    /// 一旦判定「严重持续坏」就锁存换连意图，直到真正换连或明确恢复才清。
    /// 这样即便随后被路由绕开、流量变静(样本不足)也不会丢掉换连意图。
    reconnect_pending: bool,
    last_reconnect: Option<Instant>,
}

impl TcpLossHealth {
    pub fn new(cfg: TcpLossHealthConfig) -> Self {
        Self {
            cfg,
            ring: VecDeque::new(),
            consecutive_bad: 0,
            reconnect_pending: false,
            last_reconnect: None,
        }
    }

    /// 软暂停时长（供 ws_client 设置 route-away 到期时间）。
    pub fn pause_duration(&self) -> Duration {
        self.cfg.pause
    }

    /// 当前窗口 retrans 速率(bp)；窗口内 data_segs 不足 `min_segs` 时为 None。
    pub fn window_bp(&self) -> Option<i64> {
        let (sum_retrans, sum_segs) = self.sums();
        if sum_segs < self.cfg.min_segs {
            return None;
        }
        Some(rate_bp(sum_retrans, sum_segs))
    }

    fn sums(&self) -> (u64, u64) {
        let mut r = 0u64;
        let mut s = 0u64;
        for (dr, ds) in &self.ring {
            r += *dr;
            s += *ds;
        }
        (r, s)
    }

    /// 喂入一个 tick 的窗口增量并给出判定。
    /// `can_reconnect_now`：这条连接当前是否可安全重连（无 inflight/待发）。
    pub fn record(
        &mut self,
        d_retrans: u64,
        d_data_segs: u64,
        can_reconnect_now: bool,
        now: Instant,
    ) -> Verdict {
        self.ring.push_back((d_retrans, d_data_segs));
        while self.ring.len() > self.cfg.window_ticks {
            self.ring.pop_front();
        }
        let (sum_retrans, sum_segs) = self.sums();
        // 本 tick 的基础判定（不含换连锁存的兑现）。
        let base = if sum_segs < self.cfg.min_segs {
            // 样本不足：不判坏（保守）。清连续坏计数，但**不清** reconnect_pending
            //（静默 ≠ 恢复；被绕开后流量变静时保留换连意图）。
            self.consecutive_bad = 0;
            Verdict::Healthy
        } else {
            let bp = rate_bp(sum_retrans, sum_segs);
            if bp <= self.cfg.pause_bp {
                // 明确恢复：清连续坏 + 清换连锁存。
                self.consecutive_bad = 0;
                self.reconnect_pending = false;
                Verdict::Healthy
            } else {
                self.consecutive_bad += 1;
                if bp > self.cfg.reconnect_bp && self.consecutive_bad >= self.cfg.sustain_ticks {
                    self.reconnect_pending = true;
                }
                Verdict::Pause
            }
        };
        // 锁存的换连意图：安全(无 inflight) + 距上次换连过了最小间隔 → 兑现。
        if self.reconnect_pending && can_reconnect_now {
            let interval_ok = self
                .last_reconnect
                .map(|t| now.saturating_duration_since(t) >= self.cfg.reconnect_min_interval)
                .unwrap_or(true);
            if interval_ok {
                self.reconnect_pending = false;
                self.consecutive_bad = 0;
                self.last_reconnect = Some(now);
                return Verdict::Reconnect;
            }
        }
        base
    }
}

fn rate_bp(retrans: u64, segs: u64) -> i64 {
    if segs == 0 {
        return 0;
    }
    (retrans.saturating_mul(10_000) / segs) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> TcpLossHealthConfig {
        TcpLossHealthConfig {
            window_ticks: 5,
            pause_bp: 200,
            reconnect_bp: 500,
            sustain_ticks: 3,
            min_segs: 20,
            pause: Duration::from_secs(3),
            reconnect_min_interval: Duration::from_secs(30),
        }
    }

    #[test]
    fn insufficient_segs_stays_healthy() {
        let mut h = TcpLossHealth::new(cfg());
        let t = Instant::now();
        // 高重传但样本 < min_segs(20) → Healthy。
        assert_eq!(h.record(5, 5, true, t), Verdict::Healthy);
        assert_eq!(h.window_bp(), None);
    }

    #[test]
    fn mild_loss_triggers_pause_not_reconnect() {
        let mut h = TcpLossHealth::new(cfg());
        let mut t = Instant::now();
        // 每 tick 30 segs、1 retrans → bp≈333（>pause200，<reconnect500）。
        for _ in 0..5 {
            t += Duration::from_secs(1);
            assert_eq!(h.record(1, 30, true, t), Verdict::Pause);
        }
    }

    #[test]
    fn severe_sustained_triggers_reconnect() {
        let mut h = TcpLossHealth::new(cfg());
        let mut t = Instant::now();
        // 每 tick 30 segs、3 retrans → bp=1000（>reconnect500）。持续 sustain=3。
        let mut verdicts = vec![];
        for _ in 0..3 {
            t += Duration::from_secs(1);
            verdicts.push(h.record(3, 30, true, t));
        }
        assert_eq!(verdicts[0], Verdict::Pause);
        assert_eq!(verdicts[1], Verdict::Pause);
        assert_eq!(verdicts[2], Verdict::Reconnect);
    }

    #[test]
    fn reconnect_deferred_when_inflight() {
        let mut h = TcpLossHealth::new(cfg());
        let mut t = Instant::now();
        // 严重持续坏，但 can_reconnect_now=false → 一直 Pause。
        for _ in 0..5 {
            t += Duration::from_secs(1);
            assert_eq!(h.record(3, 30, false, t), Verdict::Pause);
        }
    }

    #[test]
    fn reconnect_respects_min_interval() {
        let mut h = TcpLossHealth::new(cfg());
        let mut t = Instant::now();
        // 先触发一次 Reconnect。
        for _ in 0..3 {
            t += Duration::from_secs(1);
            h.record(3, 30, true, t);
        }
        // 紧接着仍严重坏，但距上次 <30s → 只 Pause。
        for _ in 0..3 {
            t += Duration::from_secs(1);
            assert_eq!(h.record(3, 30, true, t), Verdict::Pause);
        }
        // 越过最小间隔后再持续坏 → 再次 Reconnect（因 pending/连续坏已攒够，
        // 会在跨过间隔后的第一个 tick 立即兑现，故检查这批 tick 中是否出现过）。
        t += Duration::from_secs(30);
        let mut saw_reconnect = false;
        for _ in 0..3 {
            t += Duration::from_secs(1);
            if h.record(3, 30, true, t) == Verdict::Reconnect {
                saw_reconnect = true;
            }
        }
        assert!(saw_reconnect);
    }

    #[test]
    fn reconnect_latch_survives_quiet_then_fires_when_safe() {
        let mut h = TcpLossHealth::new(cfg());
        let mut t = Instant::now();
        // 严重持续坏但有 inflight(不可重连) → 锁存换连意图，只 Pause。
        for _ in 0..3 {
            t += Duration::from_secs(1);
            assert_eq!(h.record(3, 30, false, t), Verdict::Pause);
        }
        // 之后被绕开、流量变静(样本不足)；仍有 inflight → 期间只会 Pause/Healthy，
        // 绝不 Reconnect（窗口里旧 severe 条目要若干 tick 才冲出，故不逐条断言状态）。
        for _ in 0..6 {
            t += Duration::from_secs(1);
            assert_ne!(h.record(0, 0, false, t), Verdict::Reconnect);
        }
        // 变安全(无 inflight) → 锁存的换连兑现，即便当前静默(样本不足)。
        t += Duration::from_secs(1);
        assert_eq!(h.record(0, 0, true, t), Verdict::Reconnect);
    }

    #[test]
    fn recovers_to_healthy() {
        let mut h = TcpLossHealth::new(cfg());
        let mut t = Instant::now();
        // 几个坏 tick。
        for _ in 0..2 {
            t += Duration::from_secs(1);
            h.record(3, 30, true, t);
        }
        // 之后连续健康 → 窗口冲淡，回 Healthy，连续坏清零。
        for _ in 0..5 {
            t += Duration::from_secs(1);
            let _ = h.record(0, 30, true, t);
        }
        t += Duration::from_secs(1);
        assert_eq!(h.record(0, 30, true, t), Verdict::Healthy);
    }
}
