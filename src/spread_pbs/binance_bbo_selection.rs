//! Bounded Binance USD-M BBO connection selection. Two publishing sockets and,
//! only during a trial, one full-subscription shadow socket. A winning
//! socket is promoted in place; no reconnect between measurement and promotion.
use super::{
    adapter::SubscriptionAckPolicy,
    ws::{run_public_ws_observed, ConnectionEvent, FrameHandler, WsLoopParams},
};
use anyhow::{Context, Result};
use mkt_parsers::binance::{parse_book_ticker_bbo_raw_borrowed, parse_event_times_us_raw};
use runtime_common::time_util::get_timestamp_us;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::HashSet,
    future::Future,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    pin::Pin,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

const URL: &str = "wss://fstream.binance.com/public/stream";
const SLOTS: usize = 8192;
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
const TICK: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Off,
    Observe,
    Auto,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub mode: Mode,
    pub interval_secs: u64,
    pub window_secs: u64,
    pub min_matched: usize,
    pub min_coverage: f64,
    pub min_p50_gain_us: i64,
    pub min_clipped_mean_gain_us: f64,
    pub max_p99_regression_us: i64,
    pub max_cpu_fraction: f64,
    pub max_connection_age_secs: u64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            mode: Mode::Off,
            interval_secs: 900,
            window_secs: 60,
            min_matched: 500,
            min_coverage: 0.98,
            min_p50_gain_us: 10,
            min_clipped_mean_gain_us: 5.,
            max_p99_regression_us: 50,
            max_cpu_fraction: 0.85,
            max_connection_age_secs: 21600,
        }
    }
}
impl Settings {
    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            (300..=86400).contains(&self.interval_secs),
            "interval_secs must be 300..86400"
        );
        anyhow::ensure!(
            (30..=120).contains(&self.window_secs),
            "window_secs must be 30..120"
        );
        anyhow::ensure!(
            (100..=SLOTS).contains(&self.min_matched),
            "min_matched must be 100..8192"
        );
        anyhow::ensure!(
            (0.95..=1.).contains(&self.min_coverage),
            "min_coverage must be 0.95..1"
        );
        anyhow::ensure!(
            (1..=1000).contains(&self.min_p50_gain_us),
            "min_p50_gain_us must be 1..1000"
        );
        anyhow::ensure!(
            self.min_clipped_mean_gain_us.is_finite()
                && (1.0..=1000.).contains(&self.min_clipped_mean_gain_us),
            "invalid clipped mean threshold"
        );
        anyhow::ensure!(
            (0..=1000).contains(&self.max_p99_regression_us),
            "invalid p99 threshold"
        );
        anyhow::ensure!(
            (0.5..=0.95).contains(&self.max_cpu_fraction),
            "max_cpu_fraction must be 0.5..0.95"
        );
        anyhow::ensure!(
            (1800..=43200).contains(&self.max_connection_age_secs),
            "max_connection_age_secs must be 1800..43200"
        );
        Ok(())
    }
    pub fn load_optional() -> Result<Option<(PathBuf, Self)>> {
        let explicit = std::env::var_os("SPREAD_PBS_BINANCE_BBO_SELECTION_CONFIG");
        let path = explicit
            .clone()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("config/binance_bbo_selection.toml"));
        match std::fs::read_to_string(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && explicit.is_none() => Ok(None),
            Err(e) => Err(e).with_context(|| format!("read BBO selection {}", path.display())),
            Ok(raw) => {
                let settings: Self =
                    toml::from_str(&raw).context("parse BBO selection settings")?;
                settings.validate()?;
                Ok(Some((path, settings)))
            }
        }
    }
}

pub type Refresh = Rc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<Vec<Value>>>>>>;
pub struct Hooks {
    pub publish: FrameHandler,
    pub refresh: Refresh,
    pub stats: Rc<dyn Fn()>,
}

#[derive(Clone, Default)]
struct Sample {
    seq: i64,
    signature: [u64; 6],
    event_us: i64,
    recv: [i64; 3],
    conflict: bool,
}
struct Race {
    slots: Vec<Sample>,
    invalid: bool,
}
fn hash(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
impl Race {
    fn new() -> Self {
        Self {
            slots: vec![Sample::default(); SLOTS],
            invalid: false,
        }
    }
    fn clear(&mut self) {
        self.slots.fill(Sample::default());
        self.invalid = false;
    }
    fn note(&mut self, slot: usize, seq: i64, signature: [u64; 6], event_us: i64, recv_us: i64) {
        if recv_us < event_us || event_us <= 0 || seq <= 0 {
            self.invalid = true;
            return;
        }
        let s = &mut self.slots[(hash(seq as u64) >> 3) as usize % SLOTS];
        if s.seq != seq {
            if seq < s.seq {
                return;
            }
            *s = Sample {
                seq,
                signature,
                event_us,
                ..Sample::default()
            };
        }
        if s.signature != signature {
            s.conflict = true;
            self.invalid = true;
        }
        if s.recv[slot] == 0 {
            s.recv[slot] = recv_us;
        }
    }
    fn evaluate(&self, cfg: &Settings) -> Assessment {
        let union = self.slots.iter().filter(|s| s.seq > 0).count();
        let samples: Vec<_> = self
            .slots
            .iter()
            .filter(|s| !s.conflict && s.recv.iter().all(|t| *t > 0))
            .collect();
        let n = samples.len();
        let coverage = if union > 0 {
            n as f64 / union as f64
        } else {
            0.
        };
        let mut result = Assessment {
            matched: n,
            coverage,
            invalid: self.invalid,
            comparisons: Vec::new(),
            winner: None,
        };
        if self.invalid || n < cfg.min_matched || coverage < cfg.min_coverage {
            return result;
        }
        let baseline: Vec<_> = samples
            .iter()
            .map(|s| s.recv[0].min(s.recv[1]) - s.event_us)
            .collect();
        let base50 = quantile(&baseline, 50);
        let base99 = quantile(&baseline, 99);
        for victim in 0..2 {
            let other = 1 - victim;
            let proposed: Vec<_> = samples
                .iter()
                .map(|s| s.recv[other].min(s.recv[2]) - s.event_us)
                .collect();
            let p50 = quantile(&proposed, 50);
            let p99 = quantile(&proposed, 99);
            let mean = baseline
                .iter()
                .zip(&proposed)
                .map(|(a, b)| (a - b).clamp(-1000, 1000) as f64)
                .sum::<f64>()
                / n as f64;
            let gain = base50 - p50;
            let qualifies = gain >= cfg.min_p50_gain_us
                && mean >= cfg.min_clipped_mean_gain_us
                && p99 <= base99 + cfg.max_p99_regression_us;
            result.comparisons.push(Comparison {
                victim,
                baseline_p50_us: base50,
                proposed_p50_us: p50,
                baseline_p99_us: base99,
                proposed_p99_us: p99,
                gain_us: gain,
                clipped_mean_gain_us: mean,
                qualifies,
            });
        }
        result.winner = result
            .comparisons
            .iter()
            .filter(|c| c.qualifies)
            .max_by_key(|c| c.gain_us)
            .map(|c| c.victim);
        result
    }
}
fn quantile(values: &[i64], percent: usize) -> i64 {
    let mut v = values.to_vec();
    let k = ((v.len() - 1) * percent + 50) / 100;
    *v.select_nth_unstable(k).1
}
#[derive(Debug, Serialize)]
struct Comparison {
    victim: usize,
    baseline_p50_us: i64,
    proposed_p50_us: i64,
    baseline_p99_us: i64,
    proposed_p99_us: i64,
    gain_us: i64,
    clipped_mean_gain_us: f64,
    qualifies: bool,
}
#[derive(Debug, Serialize)]
struct Assessment {
    matched: usize,
    coverage: f64,
    invalid: bool,
    comparisons: Vec<Comparison>,
    winner: Option<usize>,
}

impl Assessment {
    // The first window fixes the replacement target. Its rank may change in
    // window two, but the same replacement must still pass every threshold.
    fn verified_victim(&self, cfg: &Settings, fixed: Option<usize>) -> Option<usize> {
        if self.invalid || self.matched < cfg.min_matched || self.coverage < cfg.min_coverage {
            return None;
        }
        let victim = fixed.or(self.winner)?;
        self.comparisons
            .iter()
            .any(|c| c.victim == victim && c.qualifies)
            .then_some(victim)
    }
}

#[derive(Default)]
struct Health {
    connected: Cell<bool>,
    generation: Cell<u64>,
    peer: Cell<Option<IpAddr>>,
    last_recv: Cell<i64>,
    last_btc: Cell<i64>,
    acks: RefCell<HashSet<i64>>,
    expected: HashSet<i64>,
}
impl Health {
    fn healthy(&self, now_us: i64) -> bool {
        self.connected.get()
            && self.acks.borrow().len() == self.expected.len()
            && self.last_recv.get() > 0
            && (0..=5_000_000).contains(&now_us.saturating_sub(self.last_recv.get()))
    }
}
struct Leg {
    id: u64,
    source: String,
    remote: Option<Ipv4Addr>,
    slot: Rc<Cell<usize>>,
    active: Rc<Cell<bool>>,
    health: Rc<Health>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
    born: Instant,
    revision: u64,
}
impl Leg {
    fn route(&self) -> Value {
        json!({"connection_id":self.id,"slot":self.slot.get(),"source_ip":self.source,"remote_ip":self.health.peer.get().map(|p|p.to_string()),"requested_ip":self.remote,"generation":self.health.generation.get(),"revision":self.revision,"connected":self.health.connected.get(),"acks":self.health.acks.borrow().len(),"expected_acks":self.health.expected.len()})
    }
    fn promote(&mut self, candidate: Leg) -> Leg {
        candidate.slot.set(self.slot.get());
        candidate.active.set(true);
        self.active.set(false);
        std::mem::replace(self, candidate)
    }
    async fn close(mut self) {
        self.active.set(false);
        let _ = self.stop.send(true);
        if tokio::time::timeout(Duration::from_secs(2), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
            let _ = self.task.await;
        }
    }
}
fn spawn_leg(
    slot: usize,
    source: String,
    remote: Option<Ipv4Addr>,
    revision: u64,
    subs: &[Value],
    publish: FrameHandler,
    sampling: Rc<Cell<bool>>,
    race: Rc<RefCell<Race>>,
    active: bool,
) -> Leg {
    let id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let slot = Rc::new(Cell::new(slot));
    let enabled = Rc::new(Cell::new(active));
    let health = Rc::new(Health {
        expected: subs.iter().filter_map(|s| s["id"].as_i64()).collect(),
        ..Health::default()
    });
    let h = health.clone();
    let s = slot.clone();
    let enabled_copy = enabled.clone();
    let handler: FrameHandler = Rc::new(move |recv_us, raw| {
        let raw = raw.trim_ascii_start();
        if raw == b"pong" || raw == b"\"pong\"" {
            return Ok(());
        }
        if raw.starts_with(b"{\"result\"")
            || raw.starts_with(b"{\"id\"")
            || raw.starts_with(b"{\"code\"")
        {
            let ack: Value = serde_json::from_slice(raw)?;
            anyhow::ensure!(
                ack.get("code").is_none(),
                "Binance subscription rejected: {}",
                ack
            );
            if let Some(id) = ack["id"].as_i64().filter(|id| h.expected.contains(id)) {
                anyhow::ensure!(
                    ack.get("result").is_some_and(Value::is_null),
                    "invalid Binance subscription ack"
                );
                h.acks.borrow_mut().insert(id);
            }
            return Ok(());
        }
        h.last_recv.set(recv_us);
        if (sampling.get() || !enabled_copy.get())
            && raw.windows(7).any(|s| s == b"BTCUSDT" || s == b"btcusdt")
        {
            if let Some(bbo) =
                parse_book_ticker_bbo_raw_borrowed(raw).filter(|b| b.symbol == "BTCUSDT")
            {
                h.last_btc.set(recv_us);
                if sampling.get() && hash(bbo.seq_id as u64) & 7 == 0 {
                    if let Some((Some(e), Some(t))) = parse_event_times_us_raw(raw, "bookTicker") {
                        let signature = [
                            e as u64,
                            t as u64,
                            bbo.bid_price.to_bits(),
                            bbo.bid_amount.to_bits(),
                            bbo.ask_price.to_bits(),
                            bbo.ask_amount.to_bits(),
                        ];
                        race.borrow_mut()
                            .note(s.get(), bbo.seq_id, signature, e, recv_us);
                    } else {
                        race.borrow_mut().invalid = true;
                    }
                }
            }
        }
        if enabled_copy.get() {
            publish(recv_us, raw)?;
        }
        Ok(())
    });
    let h = health.clone();
    let observer = Rc::new(move |event| match event {
        ConnectionEvent::Connected(peer) => {
            h.connected.set(true);
            h.peer.set(peer);
            h.generation.set(h.generation.get() + 1);
            h.last_recv.set(0);
            h.last_btc.set(0);
            h.acks.borrow_mut().clear();
        }
        ConnectionEvent::Disconnected => {
            h.connected.set(false);
            h.last_recv.set(0);
            h.acks.borrow_mut().clear();
        }
    });
    let (stop, rx) = watch::channel(false);
    let task = tokio::task::spawn_local(run_public_ws_observed(
        WsLoopParams {
            label: format!("binance-bbo-select-{id}"),
            url: URL.into(),
            local_ip: source.clone(),
            remote_ip: remote.map(|ip| ip.to_string()),
            headers: Vec::new(),
            subscribe_msgs: subs.to_vec(),
            keepalive: None,
            parse_okex_notices: false,
            business_idle_timeout: Some(Duration::from_secs(15)),
            rolling_restart: None,
        },
        handler,
        rx,
        SubscriptionAckPolicy::None,
        Some(observer),
    ));
    Leg {
        id,
        source,
        remote,
        slot,
        active: enabled,
        health,
        stop,
        task,
        born: Instant::now(),
        revision,
    }
}

struct Trial {
    leg: Leg,
    started: Instant,
    window_started: Option<Instant>,
    epochs: [u64; 3],
    first_winner: Option<usize>,
    windows: usize,
}

fn process_cpu_seconds() -> f64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) } != 0 {
        return f64::NAN;
    }
    ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
}
async fn resolve() -> Vec<Ipv4Addr> {
    match tokio::time::timeout(
        Duration::from_secs(3),
        tokio::net::lookup_host(("fstream.binance.com", 443)),
    )
    .await
    {
        Ok(Ok(addresses)) => {
            let mut ips: Vec<_> = addresses
                .filter_map(|a| match a.ip() {
                    IpAddr::V4(ip) => Some(ip),
                    _ => None,
                })
                .collect();
            ips.sort_unstable();
            ips.dedup();
            ips.truncate(16);
            ips
        }
        _ => Vec::new(),
    }
}

pub async fn run(
    path: PathBuf,
    mut cfg: Settings,
    sources: [String; 2],
    mut subs: Vec<Value>,
    refresh_secs: u64,
    hooks: Hooks,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let sampling = Rc::new(Cell::new(false));
    let race = Rc::new(RefCell::new(Race::new()));
    let mut revision = 0;
    let mut active: Vec<Leg> = (0..2)
        .map(|i| {
            spawn_leg(
                i,
                sources[i].clone(),
                None,
                revision,
                &subs,
                hooks.publish.clone(),
                sampling.clone(),
                race.clone(),
                true,
            )
        })
        .collect();
    let mut trial: Option<Trial> = None;
    let mut dns_task = Some(tokio::task::spawn_local(resolve()));
    let mut ips = Vec::new();
    let mut dns_last_success: Option<Instant> = None;
    let mut cursor = 0usize;
    let mut refresh_task: Option<JoinHandle<Result<Vec<Value>>>> = None;
    let mut next_dns = Instant::now() + Duration::from_secs(300);
    let mut next_refresh = Instant::now() + Duration::from_secs(refresh_secs.max(60));
    let mut next_probe = Instant::now() + Duration::from_secs(30);
    let mut next_reload = Instant::now() + Duration::from_secs(30);
    let mut next_stats = Instant::now() + Duration::from_secs(30);
    let mut next_status_log = Instant::now() + Duration::from_secs(300);
    let mut retry_after = Instant::now();
    let mut hot_ticks = 0usize;
    let mut last_now = Instant::now();
    let mut last_wall = get_timestamp_us();
    let mut last_cpu = process_cpu_seconds();
    let mut clock_quiet_until = Instant::now();
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    log::info!(
        "binance_bbo_selection {}",
        json!({"event":"started","settings":cfg,"sources":sources,"sample_stride":8,"max_samples":SLOTS})
    );
    loop {
        tokio::select! {
            changed=shutdown.changed()=>{if changed.is_err() || *shutdown.borrow(){break;}continue;},
            _=ticker.tick()=>{},
        }
        let now = Instant::now();
        let wall = get_timestamp_us();
        let cpu = process_cpu_seconds();
        let elapsed = now.duration_since(last_now).as_secs_f64();
        let cpu_fraction = (cpu - last_cpu) / elapsed.max(0.001);
        let clock_step = (wall - last_wall) - (elapsed * 1e6) as i64;
        last_now = now;
        last_wall = wall;
        last_cpu = cpu;
        if clock_step.unsigned_abs() > 500 {
            clock_quiet_until = now + Duration::from_secs(60);
            race.borrow_mut().invalid = true;
        }
        hot_ticks = if !cpu_fraction.is_finite() || cpu_fraction > cfg.max_cpu_fraction {
            hot_ticks + 1
        } else {
            0
        };
        if now >= next_stats {
            (hooks.stats)();
            next_stats = now + Duration::from_secs(30);
        }
        if now >= next_status_log {
            log::info!(
                "binance_bbo_selection {}",
                json!({"event":"status","mode":cfg.mode,"active":active.iter().map(Leg::route).collect::<Vec<_>>(),"candidate":trial.as_ref().map(|t|t.leg.route()),"cpu_fraction":cpu_fraction,"sampling":sampling.get()})
            );
            next_status_log = now + Duration::from_secs(300);
        }
        if now >= next_reload {
            let loaded = tokio::fs::read_to_string(&path)
                .await
                .context("read selector settings")
                .and_then(|s| toml::from_str::<Settings>(&s).context("parse selector settings"))
                .and_then(|s| {
                    s.validate()?;
                    Ok(s)
                });
            match loaded {
                Ok(s) if s != cfg => {
                    cfg = s;
                    sampling.set(false);
                    if let Some(t) = trial.take() {
                        t.leg.close().await;
                    }
                    next_probe = now + Duration::from_secs(30);
                    log::info!(
                        "binance_bbo_selection {}",
                        json!({"event":"settings_reloaded","settings":cfg})
                    );
                }
                Ok(_) => {}
                Err(e) => log::warn!("binance_bbo_selection settings reload rejected: {e:#}"),
            }
            next_reload = now + Duration::from_secs(30);
        }
        if now >= next_dns && dns_task.is_none() {
            dns_task = Some(tokio::task::spawn_local(resolve()));
            next_dns = now + Duration::from_secs(300);
        }
        if dns_task.as_ref().is_some_and(|t| t.is_finished()) {
            if let Ok(found) = dns_task.take().unwrap().await {
                if !found.is_empty() {
                    ips = found;
                    dns_last_success = Some(now);
                } else {
                    log::warn!("binance_bbo_selection DNS refresh returned no IPv4 addresses");
                }
            }
        }
        if now >= next_refresh && refresh_task.is_none() {
            refresh_task = Some(tokio::task::spawn_local((hooks.refresh)()));
            next_refresh = now + Duration::from_secs(refresh_secs.max(60));
        }
        if refresh_task.as_ref().is_some_and(|t| t.is_finished()) {
            match refresh_task.take().unwrap().await {
                Ok(Ok(new_subs)) if !new_subs.is_empty() && new_subs != subs => {
                    subs = new_subs;
                    revision += 1;
                    sampling.set(false);
                    if let Some(t) = trial.take() {
                        t.leg.close().await;
                    }
                    log::info!("binance_bbo_selection subscriptions refreshed revision={revision}");
                }
                Ok(Ok(_)) => {}
                other => log::warn!("binance_bbo_selection symbol refresh failed: {other:?}"),
            }
        }
        // Async reload/refresh above can yield to socket handlers. Their last
        // receive time may then exceed the tick's cached wall time; take a new
        // snapshot before freshness checks to avoid spurious future timestamps.
        let wall = get_timestamp_us();
        // Recovery and required subscription/age maintenance use ordinary DNS,
        // staggered one leg at a time. Maintenance does not depend on a
        // successful performance trial or spare CPU for a third full stream.
        if now >= retry_after {
            if let Some(i) = (0..2)
                .filter(|&i| {
                    let failed = !active[i].health.healthy(wall)
                        && active[i].born.elapsed() > Duration::from_secs(20);
                    let maintenance = active[1 - i].health.healthy(wall)
                        && (active[i].revision < revision
                            || active[i].born.elapsed()
                                >= Duration::from_secs(cfg.max_connection_age_secs));
                    failed || maintenance
                })
                .min_by_key(|&i| active[i].born)
            {
                let reason = if !active[i].health.healthy(wall) {
                    "unhealthy"
                } else if active[i].revision < revision {
                    "subscriptions"
                } else {
                    "age"
                };
                sampling.set(false);
                if let Some(t) = trial.take() {
                    t.leg.close().await;
                }
                let replacement = spawn_leg(
                    i,
                    active[i].source.clone(),
                    None,
                    revision,
                    &subs,
                    hooks.publish.clone(),
                    sampling.clone(),
                    race.clone(),
                    true,
                );
                let old = std::mem::replace(&mut active[i], replacement);
                old.close().await;
                retry_after = now + Duration::from_secs(20);
                next_probe = now + Duration::from_secs(60);
                log::info!(
                    "binance_bbo_selection {}",
                    json!({"event":"dns_replacement","slot":i,"reason":reason})
                );
                continue;
            }
        }
        if let Some(mut t) = trial.take() {
            let unhealthy = !active.iter().all(|l| l.health.healthy(wall));
            let expired = t.started.elapsed() > Duration::from_secs(30 + 2 * cfg.window_secs + 10);
            if unhealthy || hot_ticks >= 3 || expired || now < clock_quiet_until {
                sampling.set(false);
                log::info!(
                    "binance_bbo_selection {}",
                    json!({"event":"trial_aborted","unhealthy":unhealthy,"cpu_fraction":cpu_fraction,"expired":expired,"clock_unstable":now<clock_quiet_until})
                );
                t.leg.close().await;
                next_probe = now + Duration::from_secs(cfg.interval_secs);
                retry_after = now + Duration::from_secs(20);
                continue;
            }
            let ready = t.leg.health.healthy(wall)
                && (0..=5_000_000).contains(&wall.saturating_sub(t.leg.health.last_btc.get()));
            let epochs = [
                active[0].health.generation.get(),
                active[1].health.generation.get(),
                t.leg.health.generation.get(),
            ];
            if t.window_started.is_some() && (epochs != t.epochs || !ready) {
                sampling.set(false);
                log::info!(
                    "binance_bbo_selection trial discarded after reconnect or loss of freshness"
                );
                t.leg.close().await;
                next_probe = now + Duration::from_secs(cfg.interval_secs);
                continue;
            }
            if t.window_started.is_none() && ready && t.started.elapsed() >= Duration::from_secs(10)
            {
                t.epochs = epochs;
                t.window_started = Some(now);
                race.borrow_mut().clear();
                sampling.set(true);
            }
            if t.window_started
                .is_some_and(|start| start.elapsed() >= Duration::from_secs(cfg.window_secs))
            {
                let result = race.borrow().evaluate(&cfg);
                let victim = result.verified_victim(&cfg, t.first_winner);
                log::info!(
                    "binance_bbo_selection {}",
                    json!({"event":"window","window":t.windows+1,"candidate":t.leg.route(),"assessment":result,"fixed_victim":t.first_winner,"verified_victim":victim})
                );
                t.windows += 1;
                if victim.is_none() {
                    sampling.set(false);
                    t.leg.close().await;
                    next_probe = now + Duration::from_secs(cfg.interval_secs);
                    retry_after = now + Duration::from_secs(60);
                    continue;
                }
                if t.windows == 1 {
                    t.first_winner = victim;
                    t.window_started = Some(now);
                    race.borrow_mut().clear();
                } else {
                    sampling.set(false);
                    let victim = victim.unwrap();
                    let promote = cfg.mode == Mode::Auto;
                    if promote {
                        let old = active[victim].promote(t.leg);
                        log::info!(
                            "binance_bbo_selection {}",
                            json!({"event":"promoted","victim":victim,"old":old.route(),"new":active[victim].route()})
                        );
                        old.close().await;
                    } else {
                        log::info!(
                            "binance_bbo_selection {}",
                            json!({"event":"would_promote","victim":victim,"candidate":t.leg.route()})
                        );
                        t.leg.close().await;
                    }
                    race.borrow_mut().clear();
                    next_probe = now + Duration::from_secs(cfg.interval_secs);
                    retry_after = now + Duration::from_secs(30);
                    continue;
                }
            }
            trial = Some(t);
            continue;
        }
        if now < retry_after
            || now < clock_quiet_until
            || hot_ticks > 0
            || !active.iter().all(|l| l.health.healthy(wall))
        {
            continue;
        }
        if cfg.mode == Mode::Off || now < next_probe {
            continue;
        }
        if ips.is_empty()
            || !dns_last_success.is_some_and(|t| t.elapsed() < Duration::from_secs(600))
        {
            next_probe = now + Duration::from_secs(60);
            continue;
        }
        let (source, remote) = {
            let mut route = None;
            for _ in 0..ips.len() * 2 {
                let i = cursor % (ips.len() * 2);
                cursor = cursor.wrapping_add(1);
                let source = sources[i % 2].clone();
                let ip = ips[i / 2];
                if !active
                    .iter()
                    .any(|l| l.source == source && l.health.peer.get() == Some(IpAddr::V4(ip)))
                {
                    route = Some((source, Some(ip)));
                    break;
                }
            }
            let Some(route) = route else {
                next_probe = now + Duration::from_secs(cfg.interval_secs);
                continue;
            };
            route
        };
        let leg = spawn_leg(
            2,
            source,
            remote,
            revision,
            &subs,
            hooks.publish.clone(),
            sampling.clone(),
            race.clone(),
            false,
        );
        log::info!(
            "binance_bbo_selection {}",
            json!({"event":"trial_started","candidate":leg.route()})
        );
        trial = Some(Trial {
            leg,
            started: now,
            window_started: None,
            epochs: [0; 3],
            first_winner: None,
            windows: 0,
        });
    }
    sampling.set(false);
    if let Some(t) = trial {
        t.leg.close().await;
    }
    for leg in active {
        leg.close().await;
    }
    if let Some(task) = dns_task {
        task.abort();
    }
    if let Some(task) = refresh_task {
        task.abort();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn settings() -> Settings {
        Settings {
            min_matched: 100,
            ..Settings::default()
        }
    }
    fn race_with(delays: impl Fn(i64) -> [i64; 3]) -> Race {
        let mut r = Race::new();
        for seq in 1..=2000 {
            let d = delays(seq);
            for i in 0..3 {
                r.note(i, seq, [seq as u64; 6], 1_000_000, 1_000_000 + d[i]);
            }
        }
        r
    }
    #[test]
    fn picks_complementary_candidate_not_merely_fastest_single() {
        let r = race_with(|n| {
            if n % 5 < 3 {
                [1000, 1200, 900]
            } else {
                [1200, 1000, 1100]
            }
        });
        assert_eq!(r.evaluate(&settings()).winner, Some(0));
    }
    #[test]
    fn rejects_tail_regression_even_with_median_gain() {
        let r = race_with(|n| {
            if n % 50 == 0 {
                [1000, 3000, 5000]
            } else {
                [1000, 1100, 900]
            }
        });
        let a = r.evaluate(&settings());
        assert!(!a.comparisons[0].qualifies);
    }
    #[test]
    fn missing_and_conflicting_events_cannot_win() {
        let mut r = race_with(|_| [1200, 1100, 900]);
        for s in r.slots.iter_mut().filter(|s| s.seq > 0).take(100) {
            s.recv[2] = 0;
        }
        assert_eq!(r.evaluate(&settings()).winner, None);
        let mut r = race_with(|_| [1200, 1100, 900]);
        r.note(2, 2000, [9; 6], 1_000_000, 1_000_900);
        assert!(r.evaluate(&settings()).invalid);
    }
    #[test]
    fn duplicate_arrival_preserves_first_and_clock_error_rejects_window() {
        let mut r = Race::new();
        r.note(0, 7, [0; 6], 100, 200);
        r.note(0, 7, [0; 6], 100, 900);
        assert_eq!(r.slots[(hash(7) >> 3) as usize % SLOTS].recv[0], 200);
        r.note(1, 8, [0; 6], 100, 99);
        assert!(r.invalid);
    }
    #[test]
    fn health_requires_all_acks_connection_and_fresh_data() {
        let h = Health {
            expected: [1, 2].into_iter().collect(),
            ..Health::default()
        };
        h.connected.set(true);
        h.last_recv.set(1_000_000);
        h.acks.borrow_mut().insert(1);
        assert!(!h.healthy(1_000_100));
        h.acks.borrow_mut().insert(2);
        assert!(h.healthy(1_000_100));
        assert!(!h.healthy(7_000_000));
        assert!(!h.healthy(999_999));
        h.connected.set(false);
        assert!(!h.healthy(1_000_100));
    }
    #[tokio::test(flavor = "current_thread")]
    async fn promotion_preserves_candidate_task_and_disables_retired_publisher() {
        fn fake_leg(id: u64, slot: usize, active: bool) -> Leg {
            let (stop, _) = watch::channel(false);
            Leg {
                id,
                source: "127.0.0.1".into(),
                remote: None,
                slot: Rc::new(Cell::new(slot)),
                active: Rc::new(Cell::new(active)),
                health: Rc::new(Health::default()),
                stop,
                task: tokio::spawn(std::future::pending()),
                born: Instant::now(),
                revision: 0,
            }
        }
        let mut old = fake_leg(1, 0, true);
        let candidate = fake_leg(3, 2, false);
        let task_id = candidate.task.id();
        let candidate_enabled = candidate.active.clone();
        let candidate_slot = candidate.slot.clone();
        let old_enabled = old.active.clone();
        let retired = old.promote(candidate);
        assert_eq!(old.task.id(), task_id);
        assert_eq!(old.id, 3);
        assert!(candidate_enabled.get());
        assert_eq!(candidate_slot.get(), 0);
        assert!(!old_enabled.get());
        old.task.abort();
        retired.task.abort();
    }
    #[test]
    fn second_window_verifies_fixed_target_even_when_rank_changes() {
        let cfg = settings();
        let mut first = race_with(|_| [1200, 1100, 900]).evaluate(&cfg);
        assert!(first.comparisons.iter().all(|c| c.qualifies));
        first.winner = Some(0);
        let fixed = first.verified_victim(&cfg, None);
        assert_eq!(fixed, Some(0));
        let mut second = race_with(|_| [1200, 1100, 900]).evaluate(&cfg);
        second.winner = Some(1);
        assert_eq!(second.verified_victim(&cfg, fixed), Some(0));
        second.comparisons[0].qualifies = false;
        assert_eq!(second.verified_victim(&cfg, fixed), None);
        second.comparisons[0].qualifies = true;
        second.invalid = true;
        assert_eq!(second.verified_victim(&cfg, fixed), None);
        second.invalid = false;
        second.coverage = cfg.min_coverage - 0.01;
        assert_eq!(second.verified_victim(&cfg, fixed), None);
        second.coverage = 1.0;
        second.matched = cfg.min_matched - 1;
        assert_eq!(second.verified_victim(&cfg, fixed), None);
    }
    #[test]
    fn no_tiny_gain_or_invalid_settings() {
        assert_eq!(
            race_with(|_| [1100, 1000, 999])
                .evaluate(&settings())
                .winner,
            None
        );
        let mut s = settings();
        s.max_cpu_fraction = f64::NAN;
        assert!(s.validate().is_err());
    }
}
