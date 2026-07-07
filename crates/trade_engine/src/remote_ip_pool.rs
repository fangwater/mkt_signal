//! 动态 remote IP 池维护：DNS 发现 + TCP 建连探针评估有效性 + 淘汰/复活。
//!
//! 本期只做「维护 + 日志观测」，不接入连接选路（`remote_ip_override` 不动）。
//! 默认关闭，用 env `BINANCE_WS_REMOTE_POOL=on` 开启（仅 observe）。
//!
//! 纯状态机（`PoolState`，无网络、可单测）与异步 I/O（`RemoteIpPool::run`）分离：
//! - 发现：周期 `lookup_host` 多轮 union 进候选集，久未在 DNS 出现且非 Live 的老化删除
//! - 评估：周期对所有候选并行 TCP:443 建连探针，测成功率 + 建连 RTT（不 TLS/不 logon/
//!   不碰 API/不占 rate-limit）
//! - 淘汰：成功率低或 RTT p95 过高 → Evicted（冷却后可复活 Probation→Live）

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures_util::future::join_all;
use log::info;
use serde::Serialize;
use tokio::net::{lookup_host, TcpSocket};
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

/// 从 ws(s)/http(s) URL 取 (host, port)，port 缺省按 scheme 推断（wss/https→443）。
pub fn ws_host_port(url: &str) -> Option<(String, u16)> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_string();
    let port = parsed.port_or_known_default()?;
    Some((host, port))
}

#[derive(Clone, Debug)]
pub struct RemoteIpPoolConfig {
    /// 每个 IP 探针滚动窗口大小。
    pub window: usize,
    /// 判定所需最少样本数（不足则维持 Probation，不淘汰）。
    pub min_samples: usize,
    /// 健康要求：窗口成功率下限（百分比）。
    pub min_success_pct: u32,
    /// 健康要求：建连 RTT p95 上限（微秒）。
    pub max_rtt_us: i64,
    /// 淘汰后冷却时长。
    pub evict_cooldown: Duration,
    /// 候选在 DNS 中消失超过此时长且非 Live 则老化删除。
    pub dns_age_out: Duration,
    pub discovery_interval: Duration,
    pub health_interval: Duration,
    pub probe_timeout: Duration,
    pub snapshot_log_interval: Duration,
}

impl Default for RemoteIpPoolConfig {
    fn default() -> Self {
        Self {
            window: 30,
            min_samples: 3,
            min_success_pct: 90,
            max_rtt_us: 20_000,
            evict_cooldown: Duration::from_millis(30_000),
            dns_age_out: Duration::from_millis(300_000),
            discovery_interval: Duration::from_millis(1_000),
            health_interval: Duration::from_millis(1_000),
            probe_timeout: Duration::from_millis(1_000),
            snapshot_log_interval: Duration::from_millis(5_000),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum IpState {
    /// 新发现或样本不足，未定论。
    Probation,
    /// 健康，可用。
    Live,
    /// 已淘汰（不可达/明显坏），冷却中。
    Evicted,
}

struct IpHealth {
    #[allow(dead_code)]
    first_seen: Instant,
    last_seen_dns: Instant,
    /// (ok, rtt_us)；rtt_us 仅在 ok 时有意义。
    probes: VecDeque<(bool, i64)>,
    state: IpState,
    evicted_until: Option<Instant>,
}

impl IpHealth {
    fn new(now: Instant) -> Self {
        Self {
            first_seen: now,
            last_seen_dns: now,
            probes: VecDeque::new(),
            state: IpState::Probation,
            evicted_until: None,
        }
    }

    fn record(&mut self, ok: bool, rtt_us: i64, window: usize) {
        self.probes.push_back((ok, rtt_us));
        while self.probes.len() > window {
            self.probes.pop_front();
        }
    }

    /// (样本数, 成功率%, rtt_p50_us, rtt_p95_us)；rtt 仅统计成功探针，无成功样本时为 i64::MAX。
    fn stats(&self) -> (usize, u32, i64, i64) {
        let n = self.probes.len();
        if n == 0 {
            return (0, 0, i64::MAX, i64::MAX);
        }
        let ok_count = self.probes.iter().filter(|(ok, _)| *ok).count();
        let success_pct = (ok_count as u64 * 100 / n as u64) as u32;
        let mut rtts: Vec<i64> = self
            .probes
            .iter()
            .filter_map(|(ok, rtt)| ok.then_some(*rtt))
            .collect();
        if rtts.is_empty() {
            return (n, success_pct, i64::MAX, i64::MAX);
        }
        rtts.sort_unstable();
        let p = |q: f64| -> i64 {
            let idx = ((rtts.len() as f64 - 1.0) * q).round() as usize;
            rtts[idx.min(rtts.len() - 1)]
        };
        (n, success_pct, p(0.50), p(0.95))
    }
}

/// 纯状态机：无网络、无 I/O，全部靠注入的 `now: Instant`，便于单测。
pub struct PoolState {
    candidates: HashMap<IpAddr, IpHealth>,
    cfg: RemoteIpPoolConfig,
}

/// 快照条目（日志/JSON 输出/未来选路用）。rtt 为 i64::MAX（无成功样本）时序列化为 -1。
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct IpSnapshot {
    pub ip: IpAddr,
    pub state: IpState,
    pub samples: usize,
    pub success_pct: u32,
    #[serde(serialize_with = "ser_rtt")]
    pub rtt_p50_us: i64,
    #[serde(serialize_with = "ser_rtt")]
    pub rtt_p95_us: i64,
}

fn ser_rtt<S: serde::Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_i64(if *v == i64::MAX { -1 } else { *v })
}

impl PoolState {
    pub fn new(cfg: RemoteIpPoolConfig) -> Self {
        Self {
            candidates: HashMap::new(),
            cfg,
        }
    }

    /// union 新 IP、更新 last_seen_dns，并老化删除久未出现且非 Live 的候选。
    pub fn observe_dns(&mut self, ips: &[IpAddr], now: Instant) {
        for &ip in ips {
            self.candidates
                .entry(ip)
                .or_insert_with(|| IpHealth::new(now))
                .last_seen_dns = now;
        }
        let age_out = self.cfg.dns_age_out;
        self.candidates.retain(|_, h| {
            h.state == IpState::Live || now.duration_since(h.last_seen_dns) <= age_out
        });
    }

    /// 当前候选 IP 列表（供健康 ticker 采样，避免跨 await 持锁）。
    pub fn candidate_ips(&self) -> Vec<IpAddr> {
        self.candidates.keys().copied().collect()
    }

    /// 记录一次探针结果并就地重估该 IP。
    pub fn record_probe(&mut self, ip: IpAddr, ok: bool, rtt_us: i64, now: Instant) {
        let window = self.cfg.window;
        let cfg = self.cfg.clone();
        if let Some(h) = self.candidates.get_mut(&ip) {
            h.record(ok, rtt_us, window);
            Self::reevaluate_ip(h, now, &cfg);
        }
    }

    /// 重估所有候选（处理冷却到期等无新探针也需推进的转移）。
    pub fn reevaluate(&mut self, now: Instant) {
        let cfg = self.cfg.clone();
        for h in self.candidates.values_mut() {
            Self::reevaluate_ip(h, now, &cfg);
        }
    }

    fn reevaluate_ip(h: &mut IpHealth, now: Instant, cfg: &RemoteIpPoolConfig) {
        let (n, success_pct, _p50, p95) = h.stats();
        if n < cfg.min_samples {
            // 样本不足：冷却到期的 Evicted 放回 Probation 以便重新证明，其余保持 Probation。
            match h.state {
                IpState::Evicted => {
                    if h.evicted_until.map(|u| now >= u).unwrap_or(true) {
                        h.state = IpState::Probation;
                        h.evicted_until = None;
                    }
                }
                _ => h.state = IpState::Probation,
            }
            return;
        }
        let healthy = success_pct >= cfg.min_success_pct && p95 <= cfg.max_rtt_us;
        match h.state {
            IpState::Evicted => {
                if let Some(until) = h.evicted_until {
                    if now < until {
                        return; // 仍在冷却
                    }
                }
                // 冷却已过：纯按健康度决定，不再重置冷却——否则恢复期窗口里残留的
                // 旧失败样本会让中间态一直「不健康」，每次都把冷却往后推，永远复活不了。
                h.evicted_until = None;
                if healthy {
                    h.state = IpState::Live;
                }
            }
            _ => {
                if healthy {
                    h.state = IpState::Live;
                } else {
                    h.state = IpState::Evicted;
                    h.evicted_until = Some(now + cfg.evict_cooldown);
                }
            }
        }
    }

    /// Live IP 按 rtt_p50 升序（供未来选路）。
    pub fn live_ranked(&self) -> Vec<(IpAddr, i64)> {
        let mut live: Vec<(IpAddr, i64)> = self
            .candidates
            .iter()
            .filter(|(_, h)| h.state == IpState::Live)
            .map(|(ip, h)| (*ip, h.stats().2))
            .collect();
        live.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        live
    }

    pub fn snapshot(&self) -> Vec<IpSnapshot> {
        let mut out: Vec<IpSnapshot> = self
            .candidates
            .iter()
            .map(|(ip, h)| {
                let (n, sp, p50, p95) = h.stats();
                IpSnapshot {
                    ip: *ip,
                    state: h.state,
                    samples: n,
                    success_pct: sp,
                    rtt_p50_us: p50,
                    rtt_p95_us: p95,
                }
            })
            .collect();
        // Live 优先、再按 rtt_p50，便于日志阅读。
        out.sort_by(|a, b| {
            state_rank(a.state)
                .cmp(&state_rank(b.state))
                .then(a.rtt_p50_us.cmp(&b.rtt_p50_us))
                .then(a.ip.cmp(&b.ip))
        });
        out
    }
}

fn state_rank(s: IpState) -> u8 {
    match s {
        IpState::Live => 0,
        IpState::Probation => 1,
        IpState::Evicted => 2,
    }
}

/// 异步池：持有 `Rc<RefCell<PoolState>>`，跑发现 + 健康 + 快照日志。
/// 结构化输出（Redis / 文件）由调用方通过 `state_handle()` 读取 `snapshot()` 自行完成。
pub struct RemoteIpPool {
    label: String,
    host: String,
    port: u16,
    probe_source: Option<IpAddr>,
    /// 静态种子 IP（config 给的 ip 池）：每轮发现与 DNS 结果 union，永不因 DNS 缺席老化。
    seed_ips: Vec<IpAddr>,
    cfg: RemoteIpPoolConfig,
    state: Rc<RefCell<PoolState>>,
}

impl RemoteIpPool {
    pub fn new(
        label: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        probe_source: Option<IpAddr>,
        seed_ips: Vec<IpAddr>,
        cfg: RemoteIpPoolConfig,
    ) -> Self {
        Self {
            label: label.into(),
            host: host.into(),
            port,
            probe_source,
            seed_ips,
            state: Rc::new(RefCell::new(PoolState::new(cfg.clone()))),
            cfg,
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn state_handle(&self) -> Rc<RefCell<PoolState>> {
        self.state.clone()
    }

    pub async fn run(self, shutdown: CancellationToken) {
        let mut discovery = time::interval(self.cfg.discovery_interval);
        discovery.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut health = time::interval(self.cfg.health_interval);
        health.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut snapshot = time::interval(self.cfg.snapshot_log_interval);
        snapshot.set_missed_tick_behavior(MissedTickBehavior::Delay);
        discovery.tick().await;
        health.tick().await;
        snapshot.tick().await;

        info!(
            "remote_ip_pool[{}] started host={} port={} probe_source={:?} discovery_ms={} health_ms={}",
            self.label,
            self.host,
            self.port,
            self.probe_source,
            self.cfg.discovery_interval.as_millis(),
            self.cfg.health_interval.as_millis(),
        );

        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    info!("remote_ip_pool[{}] observed shutdown", self.label);
                    return;
                }
                _ = discovery.tick() => {
                    self.run_discovery_once().await;
                }
                _ = health.tick() => {
                    self.run_health_once().await;
                }
                _ = snapshot.tick() => {
                    self.log_snapshot();
                }
            }
        }
    }

    async fn run_discovery_once(&self) {
        let mut ips: Vec<IpAddr> = self.seed_ips.clone();
        match lookup_host((self.host.as_str(), self.port)).await {
            Ok(addrs) => ips.extend(addrs.map(|a| a.ip())),
            Err(err) => {
                info!(
                    "remote_ip_pool[{}] dns lookup failed host={}: {} (seed_ips still observed)",
                    self.label, self.host, err
                );
            }
        }
        ips.sort_unstable();
        ips.dedup();
        if !ips.is_empty() {
            self.state.borrow_mut().observe_dns(&ips, Instant::now());
        }
    }

    async fn run_health_once(&self) {
        let ips = self.state.borrow().candidate_ips();
        if ips.is_empty() {
            return;
        }
        let probe_source = self.probe_source;
        let port = self.port;
        let timeout = self.cfg.probe_timeout;
        let results = join_all(
            ips.iter()
                .map(|&ip| probe_one(probe_source, ip, port, timeout)),
        )
        .await;
        let now = Instant::now();
        let mut st = self.state.borrow_mut();
        for (&ip, (ok, rtt_us)) in ips.iter().zip(results.iter()) {
            st.record_probe(ip, *ok, *rtt_us, now);
        }
        st.reevaluate(now);
    }

    fn log_snapshot(&self) {
        let snap = self.state.borrow().snapshot();
        let live = snap.iter().filter(|s| s.state == IpState::Live).count();
        let evicted = snap.iter().filter(|s| s.state == IpState::Evicted).count();
        let mut detail = String::new();
        for s in &snap {
            let tag = match s.state {
                IpState::Live => "L",
                IpState::Probation => "P",
                IpState::Evicted => "E",
            };
            let p50 = if s.rtt_p50_us == i64::MAX {
                -1
            } else {
                s.rtt_p50_us
            };
            let p95 = if s.rtt_p95_us == i64::MAX {
                -1
            } else {
                s.rtt_p95_us
            };
            detail.push_str(&format!(
                " {}={}:n{}/ok{}%/p50={}/p95={}",
                s.ip, tag, s.samples, s.success_pct, p50, p95
            ));
        }
        info!(
            "remote_ip_pool[{}] total={} live={} evicted={} probation={}{}",
            self.label,
            snap.len(),
            live,
            evicted,
            snap.len() - live - evicted,
            detail
        );
    }
}

/// 单次 TCP:port 建连探针：返回 (是否成功, 建连 RTT_us)。不做 TLS/logon。
async fn probe_one(src: Option<IpAddr>, ip: IpAddr, port: u16, timeout: Duration) -> (bool, i64) {
    let socket = match ip {
        IpAddr::V4(_) => TcpSocket::new_v4(),
        IpAddr::V6(_) => TcpSocket::new_v6(),
    };
    let socket = match socket {
        Ok(s) => s,
        Err(_) => return (false, 0),
    };
    // 仅在源与目的地址族一致时 bind 源 IP；否则不 bind（不影响可达性探测）。
    if let Some(src_ip) = src {
        let compatible = matches!(
            (src_ip, ip),
            (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
        );
        if compatible {
            let _ = socket.bind(SocketAddr::new(src_ip, 0));
        }
    }
    let start = Instant::now();
    match time::timeout(timeout, socket.connect(SocketAddr::new(ip, port))).await {
        Ok(Ok(_stream)) => (true, start.elapsed().as_micros() as i64),
        _ => (false, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RemoteIpPoolConfig {
        RemoteIpPoolConfig {
            window: 10,
            min_samples: 3,
            min_success_pct: 90,
            max_rtt_us: 5_000,
            evict_cooldown: Duration::from_secs(30),
            dns_age_out: Duration::from_secs(300),
            ..Default::default()
        }
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn state_of(p: &PoolState, ip: IpAddr) -> IpState {
        p.snapshot().into_iter().find(|s| s.ip == ip).unwrap().state
    }

    #[test]
    fn dns_union_and_age_out() {
        let mut p = PoolState::new(cfg());
        let t0 = Instant::now();
        p.observe_dns(&[ip("1.1.1.1"), ip("2.2.2.2")], t0);
        assert_eq!(p.candidate_ips().len(), 2);

        // 只再看到 1.1.1.1；2.2.2.2 超过 age_out 且非 Live → 老化删除。
        let t1 = t0 + Duration::from_secs(301);
        p.observe_dns(&[ip("1.1.1.1")], t1);
        let ips = p.candidate_ips();
        assert_eq!(ips.len(), 1);
        assert_eq!(ips[0], ip("1.1.1.1"));
    }

    #[test]
    fn low_success_triggers_eviction_then_recovers() {
        let mut p = PoolState::new(cfg());
        let a = ip("10.0.0.1");
        let mut t = Instant::now();
        p.observe_dns(&[a], t);

        // 连续失败 → 样本足 + 成功率低 → Evicted。
        for _ in 0..3 {
            t += Duration::from_secs(1);
            p.record_probe(a, false, 0, t);
        }
        assert_eq!(state_of(&p, a), IpState::Evicted);

        // 冷却未到：即使恢复成功也保持 Evicted。
        t += Duration::from_secs(5);
        for _ in 0..5 {
            t += Duration::from_millis(100);
            p.record_probe(a, true, 500, t);
        }
        assert_eq!(state_of(&p, a), IpState::Evicted);

        // 冷却到期 + 窗口已被成功样本填满（成功率高）→ 复活 Live。
        t += Duration::from_secs(30);
        for _ in 0..10 {
            t += Duration::from_millis(100);
            p.record_probe(a, true, 500, t);
        }
        assert_eq!(state_of(&p, a), IpState::Live);
    }

    #[test]
    fn high_rtt_triggers_eviction() {
        let mut p = PoolState::new(cfg());
        let a = ip("10.0.0.2");
        let mut t = Instant::now();
        p.observe_dns(&[a], t);
        for _ in 0..5 {
            t += Duration::from_secs(1);
            p.record_probe(a, true, 9_000, t); // 高于 max_rtt_us=5000
        }
        assert_eq!(state_of(&p, a), IpState::Evicted);
    }

    #[test]
    fn insufficient_samples_stays_probation() {
        let mut p = PoolState::new(cfg());
        let a = ip("10.0.0.3");
        let mut t = Instant::now();
        p.observe_dns(&[a], t);
        // 只有 2 个样本（< min_samples=3），即便失败也不淘汰。
        t += Duration::from_secs(1);
        p.record_probe(a, false, 0, t);
        t += Duration::from_secs(1);
        p.record_probe(a, false, 0, t);
        assert_eq!(state_of(&p, a), IpState::Probation);
    }

    #[test]
    fn live_ranked_orders_by_rtt() {
        let mut p = PoolState::new(cfg());
        let a = ip("10.0.0.10");
        let b = ip("10.0.0.11");
        let mut t = Instant::now();
        p.observe_dns(&[a, b], t);
        for _ in 0..5 {
            t += Duration::from_millis(100);
            p.record_probe(a, true, 2_000, t);
            p.record_probe(b, true, 500, t);
        }
        let ranked = p.live_ranked();
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].0, b); // 更低 rtt 排前
        assert_eq!(ranked[1].0, a);
    }
}
