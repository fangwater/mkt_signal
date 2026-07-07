//! 独立进程：remote IP 池诊断与维护 → 定时刷新写入 Redis。
//!
//! 给 config 的 url 池 + 对应 ip 池（seed），对每个 target：DNS 发现 IP + 合并 ips →
//! 周期 TCP:443 建连探针评估（成功率 + 建连 RTT）→ 打分排名 + 淘汰/复活。
//! 定时把每个 target 的健康快照（含 Live IP 排名）写入 Redis，供消费方读取。
//!
//! 定位：这套 TCP 探针打分不追求完善，只求给出「哪些 ip→url 质量不错」的信号。
//! 独立进程运行（不进 trade_engine），默认由 OS 调度落在管家核，不占订单核。
//!
//! 用法：`remote_ip_diag --config config/remote_ip_diag.toml`

use std::cell::RefCell;
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
    /// 探针绑定的本地源 IP；缺省不 bind。
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
    /// Redis key 前缀，最终 key = `<prefix><target.name>`。
    redis_key_prefix: Option<String>,
    /// 写 Redis 刷新周期；缺省取 snapshot_log_ms（或 1000ms）。
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

/// 发布目标：一个 target 的元信息 + 状态句柄。
struct PublishTarget {
    name: String,
    host: String,
    port: u16,
    state: Rc<RefCell<PoolState>>,
}

/// 写入 Redis 的每 target 快照。
#[derive(Serialize)]
struct RedisSnapshot<'a> {
    name: &'a str,
    host: &'a str,
    port: u16,
    updated_unix_ms: i64,
    total: usize,
    live: usize,
    evicted: usize,
    probation: usize,
    /// Live IP 按建连 RTT 从优到劣排名（消费方直接取前几名即可）。
    live_ips: Vec<IpAddr>,
    /// 全量明细（状态/成功率/RTT）。
    ips: Vec<IpSnapshot>,
}

fn build_snapshot<'a>(t: &'a PublishTarget, now_ms: i64) -> RedisSnapshot<'a> {
    let st = t.state.borrow();
    let ips = st.snapshot();
    let live_ips: Vec<IpAddr> = st.live_ranked().into_iter().map(|(ip, _)| ip).collect();
    let live = ips.iter().filter(|s| s.state == IpState::Live).count();
    let evicted = ips.iter().filter(|s| s.state == IpState::Evicted).count();
    let total = ips.len();
    RedisSnapshot {
        name: &t.name,
        host: &t.host,
        port: t.port,
        updated_unix_ms: now_ms,
        total,
        live,
        evicted,
        probation: total - live - evicted,
        live_ips,
        ips,
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
                // 先同步构建所有 payload（短暂借状态，不跨 await）。
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
                        client = None; // 下轮重连
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

    let probe_source: Option<IpAddr> = match cfg.probe_source_ip.as_deref() {
        Some(s) if !s.trim().is_empty() => Some(parse_ip(s, "probe_source_ip")?),
        _ => None,
    };
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

    // 预构建各 target 的池 + 发布目标。
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
            "remote_ip_diag: target {} url={} host={}:{} ips={}",
            t.name,
            t.url,
            host,
            port,
            seeds.len()
        );
        let pool = RemoteIpPool::new(
            t.name.clone(),
            host.clone(),
            port,
            probe_source,
            seeds,
            pool_cfg.clone(),
        );
        publish_targets.push(PublishTarget {
            name: t.name.clone(),
            host,
            port,
            state: pool.state_handle(),
        });
        pools.push(pool);
    }

    eprintln!(
        "remote_ip_diag: redis {}:{} db={} prefix={} refresh_ms={}",
        redis_settings.host,
        redis_settings.port,
        redis_settings.db,
        redis_key_prefix,
        redis_refresh.as_millis()
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

        // Redis 发布 ticker。
        let publisher = tokio::task::spawn_local(run_redis_publisher(
            redis_settings,
            redis_key_prefix,
            redis_refresh,
            publish_targets,
            shutdown.clone(),
        ));

        // 各 target 的维护（发现 + 探针 + 日志）。
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
