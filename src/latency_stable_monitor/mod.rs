pub mod cfg;

use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::latency_stable_monitor::cfg::{
    default_spread_service, default_spread_topic, default_trade_engine_service,
    default_trade_engine_topic, LatencyStableMonitorConfig, SpreadPbsSourceConfig,
    TradeEngineSourceConfig,
};
use crate::rolling_metrics::latency_snapshot::LATENCY_SNAPSHOT_PAYLOAD_LEN;

const SOURCE_PUBLIC: &str = "public";
const SOURCE_PRIVATE: &str = "private";
const KIND_SPREAD_PBS: &str = "spread_pbs";
const KIND_TRADE_ENGINE: &str = "trade_engine";

pub struct LatencyStableMonitorApp {
    cfg: LatencyStableMonitorConfig,
}

impl LatencyStableMonitorApp {
    pub fn new(cfg: LatencyStableMonitorConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let node_name = format!("latency_stable_monitor_{}", std::process::id());
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let zmq_ctx = zmq::Context::new();
        let pub_socket = zmq_ctx
            .socket(zmq::PUB)
            .context("failed to create ZMQ PUB socket")?;
        pub_socket
            .set_sndhwm(self.cfg.zmq.sndhwm)
            .context("failed to set ZMQ PUB sndhwm")?;
        pub_socket
            .bind(self.cfg.zmq.bind.trim())
            .with_context(|| format!("failed to bind ZMQ PUB on {}", self.cfg.zmq.bind.trim()))?;

        let mut sources = build_sources(&self.cfg);
        log::info!(
            "latency_stable_monitor started: bind={} sources={} payload=raw_binary_{}B poll_ms={} reconnect_ms={} idle_log_secs={} stats_log_secs={}",
            self.cfg.zmq.bind.trim(),
            sources.len(),
            LATENCY_SNAPSHOT_PAYLOAD_LEN,
            self.cfg.poll_ms,
            self.cfg.reconnect_ms,
            self.cfg.idle_log_secs,
            self.cfg.stats_log_secs,
        );
        for source in &sources {
            log::info!(
                "latency source configured: name={} dimension={} kind={} service={} topic={}",
                source.identity.name,
                source.identity.dimension,
                source.identity.kind,
                source.identity.service,
                source.identity.topic,
            );
        }

        let poll_interval = Duration::from_millis(self.cfg.poll_ms);
        let reconnect_interval = Duration::from_millis(self.cfg.reconnect_ms);
        let idle_log_interval = Duration::from_secs(self.cfg.idle_log_secs);
        let stats_log_interval = Duration::from_secs(self.cfg.stats_log_secs);
        let mut last_idle_log = Instant::now();
        let mut last_stats_log = Instant::now();
        let mut poll = tokio::time::interval(poll_interval);
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = poll.tick() => {
                    let now = Instant::now();
                    let mut published_this_tick = 0u64;
                    for source in &mut sources {
                        if source.subscriber.is_none() && now >= source.next_retry_at {
                            source.try_open(&node, reconnect_interval);
                        }
                        if source.subscriber.is_some() {
                            published_this_tick = published_this_tick.saturating_add(
                                source.drain_and_publish(&pub_socket)
                            );
                        }
                    }

                    if published_this_tick > 0 {
                        last_idle_log = Instant::now();
                    } else if self.cfg.idle_log_secs > 0 && last_idle_log.elapsed() >= idle_log_interval {
                        let connected = sources.iter().filter(|s| s.subscriber.is_some()).count();
                        log::info!(
                            "latency_stable_monitor idle: connected_sources={}/{}",
                            connected,
                            sources.len()
                        );
                        last_idle_log = Instant::now();
                    }

                    if self.cfg.stats_log_secs > 0 && last_stats_log.elapsed() >= stats_log_interval {
                        log_source_stats(&sources);
                        last_stats_log = Instant::now();
                    }
                }
            }
        }

        log::info!("latency_stable_monitor shutdown");
        Ok(())
    }
}

struct SourceRuntime {
    identity: SourceIdentity,
    subscriber: Option<Subscriber<ipc::Service, [u8; LATENCY_SNAPSHOT_PAYLOAD_LEN], ()>>,
    next_retry_at: Instant,
    received: u64,
    published: u64,
    publish_errors: u64,
    last_open_warn_at: Option<Instant>,
}

#[derive(Clone)]
struct SourceIdentity {
    name: String,
    dimension: &'static str,
    kind: &'static str,
    service: String,
    topic: String,
}

impl SourceRuntime {
    fn try_open(&mut self, node: &Node<ipc::Service>, reconnect_interval: Duration) {
        self.next_retry_at = Instant::now() + reconnect_interval;
        let service_result = node
            .service_builder(
                &ServiceName::new(&self.identity.service).expect("validated service name"),
            )
            .publish_subscribe::<[u8; LATENCY_SNAPSHOT_PAYLOAD_LEN]>()
            .open();

        let service = match service_result {
            Ok(service) => service,
            Err(err) => {
                if should_log_open_warn(self.last_open_warn_at) {
                    log::warn!(
                        "latency source not ready: name={} service={} err={:?}",
                        self.identity.name,
                        self.identity.service,
                        err
                    );
                    self.last_open_warn_at = Some(Instant::now());
                }
                return;
            }
        };

        match service.subscriber_builder().create() {
            Ok(subscriber) => {
                self.subscriber = Some(subscriber);
                self.next_retry_at = Instant::now();
                self.last_open_warn_at = None;
                log::info!(
                    "latency source connected: name={} service={} topic={}",
                    self.identity.name,
                    self.identity.service,
                    self.identity.topic
                );
            }
            Err(err) => {
                if should_log_open_warn(self.last_open_warn_at) {
                    log::warn!(
                        "latency source subscriber create failed: name={} service={} err={:?}",
                        self.identity.name,
                        self.identity.service,
                        err
                    );
                    self.last_open_warn_at = Some(Instant::now());
                }
            }
        }
    }

    fn drain_and_publish(&mut self, pub_socket: &zmq::Socket) -> u64 {
        let Some(subscriber) = self.subscriber.as_ref() else {
            return 0;
        };

        let mut published = 0u64;
        loop {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    self.received = self.received.saturating_add(1);
                    let payload = sample.payload();
                    if let Err(err) = pub_socket
                        .send_multipart([&self.identity.topic.as_bytes()[..], &payload[..]], 0)
                    {
                        self.publish_errors = self.publish_errors.saturating_add(1);
                        log::warn!(
                            "latency raw zmq publish failed: topic={} service={} err={}",
                            self.identity.topic,
                            self.identity.service,
                            err
                        );
                    } else {
                        self.published = self.published.saturating_add(1);
                        published = published.saturating_add(1);
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    log::warn!(
                        "latency source receive failed, will reconnect: name={} service={} err={}",
                        self.identity.name,
                        self.identity.service,
                        err
                    );
                    self.subscriber = None;
                    self.next_retry_at = Instant::now();
                    break;
                }
            }
        }
        published
    }
}

fn log_source_stats(sources: &[SourceRuntime]) {
    for source in sources {
        log::info!(
            "latency source stats: name={} topic={} connected={} received={} published={} publish_errors={}",
            source.identity.name,
            source.identity.topic,
            source.subscriber.is_some(),
            source.received,
            source.published,
            source.publish_errors,
        );
    }
}

fn build_sources(cfg: &LatencyStableMonitorConfig) -> Vec<SourceRuntime> {
    let mut sources = Vec::new();
    for source in &cfg.public.spread_pbs {
        sources.push(SourceRuntime::new_spread(source));
    }
    for source in &cfg.private.trade_engine {
        sources.push(SourceRuntime::new_trade_engine(source));
    }
    sources
}

impl SourceRuntime {
    fn new_spread(cfg: &SpreadPbsSourceConfig) -> Self {
        let venue = cfg.venue.trim().to_owned();
        let service = cfg
            .service
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| default_spread_service(&venue));
        let topic = cfg
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| default_spread_topic(&venue));
        let name = cfg
            .name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("spread_pbs:{venue}"));
        Self::new(SourceIdentity {
            name,
            dimension: SOURCE_PUBLIC,
            kind: KIND_SPREAD_PBS,
            service,
            topic,
        })
    }

    fn new_trade_engine(cfg: &TradeEngineSourceConfig) -> Self {
        let namespace = cfg.namespace.trim().to_owned();
        let exchange = cfg.exchange.trim().to_owned();
        let service = cfg
            .service
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| default_trade_engine_service(&namespace, &exchange));
        let topic = cfg
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| default_trade_engine_topic(&namespace));
        let name = cfg
            .name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("te:{namespace}:{exchange}"));
        Self::new(SourceIdentity {
            name,
            dimension: SOURCE_PRIVATE,
            kind: KIND_TRADE_ENGINE,
            service,
            topic,
        })
    }

    fn new(identity: SourceIdentity) -> Self {
        Self {
            identity,
            subscriber: None,
            next_retry_at: Instant::now(),
            received: 0,
            published: 0,
            publish_errors: 0,
            last_open_warn_at: None,
        }
    }
}

fn should_log_open_warn(last: Option<Instant>) -> bool {
    last.map(|t| t.elapsed() >= Duration::from_secs(30))
        .unwrap_or(true)
}
