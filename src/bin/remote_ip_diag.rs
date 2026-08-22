//! 独立进程：remote IP 池诊断与维护 → 定时刷新写入 Redis。
//!
//! 给 config 的 url 池 + 对应 ip 池（seed）+ 出站 IP 池（source_ips），
//! 对每个 target 的「每个出站 IP × 每个候选 remote」都做 TCP:443 建连探针
//! （成功率 + 建连 RTT）→ 打分排名 + 淘汰/复活。定时把每 target 的
//! **最优 (出站IP → remote) 组合排名** + 各出站明细写入 Redis。
//!
//! 定位：TCP 探针打分是粗信号——能筛掉「某出站/某 remote 路径系统性偏烂」，
//! 但分辨不了逐连接的端口级 ECMP 抖动。独立进程运行，不占订单核；本期只诊断+发布。
//!
//! 用法：`remote_ip_diag --config config/remote_ip_diag.toml`

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use runtime_common::redis_client::{RedisClient, RedisSettings};
use serde::{Deserialize, Serialize};
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use trade_engine::remote_ip_pool::{
    ws_host_port, IpSnapshot, IpState, PoolState, RemoteIpPool, RemoteIpPoolConfig,
};

const DEFAULT_REDIS_KEY_PREFIX: &str = "remote_ip_diag:";

#[derive(Parser)]
struct Args {
    /// config toml 路径
    #[arg(long, default_value = "config/remote_ip_diag.toml")]
    config: String,
    /// 绑定运行核（可选，sched_setaffinity）
    #[arg(long)]
    core: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct DiagConfig {
    /// 出站 IP 池：对每个候选 remote 从每个出站 IP 都探一遍，打分单元 = (出站, remote)。
    /// 留空则回落到 probe_source_ip（单出站）或不 bind。
    #[serde(default)]
    source_ips: Vec<String>,
    /// 单出站兜底（source_ips 为空时用）。
    probe_source_ip: Option<String>,
    window: Option<usize>,
    min_samples: Option<usize>,
    min_success_pct: Option<u32>,
    max_rtt_us: Option<i64>,
    evict_cooldown_ms: Option<u64>,
    dns_age_out_ms: Option<u64>,
    discovery_interval_ms: Option<u64>,
    health_interval_ms: Option<u64>,
    probe_timeout_ms: Option<u64>,
    snapshot_log_ms: Option<u64>,
    /// Redis 连接（[redis] 表）；缺省用 127.0.0.1:6379。
    redis: Option<RedisSettings>,
    redis_key_prefix: Option<String>,
    redis_refresh_ms: Option<u64>,
    targets: Vec<TargetConfig>,
}

#[derive(Debug, Deserialize)]
struct TargetConfig {
    name: String,
    url: String,
    /// 该 url 对应的静态 IP 池（与 DNS 发现结果 union，永不因 DNS 缺席老化）。
    #[serde(default, alias = "seed_ips")]
    ips: Vec<String>,
}

impl DiagConfig {
    fn pool_config(&self) -> RemoteIpPoolConfig {
        let d = RemoteIpPoolConfig::default();
        RemoteIpPoolConfig {
            window: self.window.unwrap_or(d.window),
            min_samples: self.min_samples.unwrap_or(d.min_samples),
            min_success_pct: self.min_success_pct.unwrap_or(d.min_success_pct),
            max_rtt_us: self.max_rtt_us.unwrap_or(d.max_rtt_us),
            evict_cooldown: self
                .evict_cooldown_ms
                .map(Duration::from_millis)
                .unwrap_or(d.evict_cooldown),
            dns_age_out: self
                .dns_age_out_ms
                .map(Duration::from_millis)
                .unwrap_or(d.dns_age_out),
            discovery_interval: self
                .discovery_interval_ms
                .map(Duration::from_millis)
                .unwrap_or(d.discovery_interval),
            health_interval: self
                .health_interval_ms
                .map(Duration::from_millis)
                .unwrap_or(d.health_interval),
            probe_timeout: self
                .probe_timeout_ms
                .map(Duration::from_millis)
                .unwrap_or(d.probe_timeout),
            snapshot_log_interval: self
                .snapshot_log_ms
                .map(Duration::from_millis)
                .unwrap_or(d.snapshot_log_interval),
        }
    }
}

/// 一个出站视角的池句柄。
struct SourcePool {
    src: Option<IpAddr>,
    state: Rc<RefCell<PoolState>>,
}

/// 一个 target（url）：元信息 + 多个出站视角。
struct PublishTarget {
    name: String,
    host: String,
    port: u16,
    sources: Vec<SourcePool>,
}

/// 写入 Redis 的每 target 快照。
#[derive(Serialize)]
struct RedisSnapshot<'a> {
    name: &'a str,
    host: &'a str,
    port: u16,
    updated_unix_ms: i64,
    /// 最优 (出站 → remote) 组合，按建连 RTT 从优到劣。消费方直接取前几名。
    best_pairs: Vec<BestPair>,
    /// 每个出站的明细。
    by_source: BTreeMap<String, SourceView>,
}

#[derive(Serialize)]
struct BestPair {
    src: String,
    remote: IpAddr,
    success_pct: u32,
    rtt_p50_us: i64,
    rtt_p95_us: i64,
}

#[derive(Serialize)]
struct SourceView {
    total: usize,
    live: usize,
    evicted: usize,
    probation: usize,
    /// 该出站下健康的 remote，按 RTT 排名。
    live_remotes: Vec<IpAddr>,
    ips: Vec<IpSnapshot>,
}

fn src_label(src: Option<IpAddr>) -> String {
    src.map(|ip| ip.to_string())
        .unwrap_or_else(|| "default".to_string())
}

fn build_snapshot<'a>(t: &'a PublishTarget, now_ms: i64) -> RedisSnapshot<'a> {
    let mut best_pairs: Vec<BestPair> = Vec::new();
    let mut by_source: BTreeMap<String, SourceView> = BTreeMap::new();
    for sp in &t.sources {
        let (snapshot, ranked) = {
            let st = sp.state.borrow();
            (st.snapshot(), st.live_ranked())
        };
        let live = snapshot.iter().filter(|s| s.state == IpState::Live).count();
        let evicted = snapshot
            .iter()
            .filter(|s| s.state == IpState::Evicted)
            .count();
        let total = snapshot.len();
        let label = src_label(sp.src);
        for s in &snapshot {
            if s.state == IpState::Live {
                best_pairs.push(BestPair {
                    src: label.clone(),
                    remote: s.ip,
                    success_pct: s.success_pct,
                    rtt_p50_us: s.rtt_p50_us,
                    rtt_p95_us: s.rtt_p95_us,
                });
            }
        }
        let live_remotes: Vec<IpAddr> = ranked.into_iter().map(|(ip, _)| ip).collect();
        by_source.insert(
            label,
            SourceView {
                total,
                live,
                evicted,
                probation: total - live - evicted,
                live_remotes,
                ips: snapshot,
            },
        );
    }
    // 最优组合：RTT p50 升序，再成功率降序，再 remote 稳定序。
    best_pairs.sort_by(|a, b| {
        a.rtt_p50_us
            .cmp(&b.rtt_p50_us)
            .then(b.success_pct.cmp(&a.success_pct))
            .then(a.remote.cmp(&b.remote))
    });
    RedisSnapshot {
        name: &t.name,
        host: &t.host,
        port: t.port,
        updated_unix_ms: now_ms,
        best_pairs,
        by_source,
    }
}

async fn run_redis_publisher(
    settings: RedisSettings,
    key_prefix: String,
    refresh: Duration,
    targets: Vec<PublishTarget>,
    shutdown: CancellationToken,
) {
    let mut interval = time::interval(refresh);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;
    let mut client: Option<RedisClient> = None;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {
                let now_ms = runtime_common::time_util::get_timestamp_us() / 1_000;
                let payloads: Vec<(String, String)> = targets
                    .iter()
                    .filter_map(|t| {
                        let dto = build_snapshot(t, now_ms);
                        match serde_json::to_string(&dto) {
                            Ok(json) => Some((format!("{}{}", key_prefix, t.name), json)),
                            Err(err) => {
                                eprintln!("remote_ip_diag: serialize {} failed: {}", t.name, err);
                                None
                            }
                        }
                    })
                    .collect();

                if client.is_none() {
                    match RedisClient::connect(settings.clone()).await {
                        Ok(c) => client = Some(c),
                        Err(err) => {
                            eprintln!("remote_ip_diag: redis connect failed: {}", err);
                            continue;
                        }
                    }
                }
                let c = client.as_mut().unwrap();
                for (key, json) in &payloads {
                    if let Err(err) = c.set_string(key, json).await {
                        eprintln!("remote_ip_diag: redis set {} failed: {}", key, err);
                        client = None;
                        break;
                    }
                }
            }
        }
    }
}

fn parse_ip(s: &str, what: &str) -> Result<IpAddr> {
    s.trim()
        .parse::<IpAddr>()
        .with_context(|| format!("invalid {} `{}`", what, s))
}

/// 解析出站 IP 列表：source_ips 优先；否则 probe_source_ip 单出站；再否则不 bind。
fn resolve_sources(cfg: &DiagConfig) -> Result<Vec<Option<IpAddr>>> {
    let mut sources: Vec<Option<IpAddr>> = Vec::new();
    for s in &cfg.source_ips {
        if s.trim().is_empty() {
            continue;
        }
        sources.push(Some(parse_ip(s, "source_ip")?));
    }
    if sources.is_empty() {
        match cfg.probe_source_ip.as_deref() {
            Some(s) if !s.trim().is_empty() => sources.push(Some(parse_ip(s, "probe_source_ip")?)),
            _ => sources.push(None),
        }
    }
    Ok(sources)
}

fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();
    let args = Args::parse();

    if let Some(core) = args.core {
        if let Err(err) =
            runtime_common::affinity::maybe_pin_current_thread(Some(core), "REMOTE_IP_DIAG_CORE")
        {
            eprintln!("remote_ip_diag: pin core {} failed: {}", core, err);
        }
    }

    let raw = std::fs::read_to_string(&args.config)
        .with_context(|| format!("read config {}", args.config))?;
    let cfg: DiagConfig = toml::from_str(&raw).with_context(|| "parse config toml")?;
    if cfg.targets.is_empty() {
        bail!("config has no targets");
    }

    let sources = resolve_sources(&cfg)?;
    let pool_cfg = cfg.pool_config();
    let redis_settings = cfg.redis.clone().unwrap_or_default();
    let redis_key_prefix = cfg
        .redis_key_prefix
        .clone()
        .unwrap_or_else(|| DEFAULT_REDIS_KEY_PREFIX.to_string());
    let redis_refresh = cfg
        .redis_refresh_ms
        .or(cfg.snapshot_log_ms)
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(1_000));

    // 每 target × 每出站 一个池。
    let mut pools: Vec<RemoteIpPool> = Vec::new();
    let mut publish_targets: Vec<PublishTarget> = Vec::new();
    for t in &cfg.targets {
        let (host, port) =
            ws_host_port(&t.url).with_context(|| format!("parse url `{}`", t.url))?;
        let mut seeds = Vec::new();
        for s in &t.ips {
            if s.trim().is_empty() {
                continue;
            }
            seeds.push(parse_ip(s, "ip")?);
        }
        eprintln!(
            "remote_ip_diag: target {} url={} host={}:{} ips={} sources={}",
            t.name,
            t.url,
            host,
            port,
            seeds.len(),
            sources.len()
        );
        let mut source_pools = Vec::new();
        for &src in &sources {
            let label = format!("{}@{}", t.name, src_label(src));
            let pool = RemoteIpPool::new(
                label,
                host.clone(),
                port,
                src,
                seeds.clone(),
                pool_cfg.clone(),
            );
            source_pools.push(SourcePool {
                src,
                state: pool.state_handle(),
            });
            pools.push(pool);
        }
        publish_targets.push(PublishTarget {
            name: t.name.clone(),
            host,
            port,
            sources: source_pools,
        });
    }

    eprintln!(
        "remote_ip_diag: redis {}:{} db={} prefix={} refresh_ms={} pools={}",
        redis_settings.host,
        redis_settings.port,
        redis_settings.db,
        redis_key_prefix,
        redis_refresh.as_millis(),
        pools.len()
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    let shutdown = CancellationToken::new();

    local.block_on(&rt, async move {
        let shutdown_ctrlc = shutdown.clone();
        tokio::task::spawn_local(async move {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("remote_ip_diag: ctrl-c, shutting down");
            shutdown_ctrlc.cancel();
        });

        let publisher = tokio::task::spawn_local(run_redis_publisher(
            redis_settings,
            redis_key_prefix,
            redis_refresh,
            publish_targets,
            shutdown.clone(),
        ));

        let mut handles = Vec::new();
        for pool in pools {
            let sd = shutdown.clone();
            handles.push(tokio::task::spawn_local(async move {
                pool.run(sd).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        let _ = publisher.await;
    });
    Ok(())
}
