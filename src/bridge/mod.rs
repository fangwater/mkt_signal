pub mod cfg;
mod iceoryx;
mod redis_sync;

use anyhow::{anyhow, Result};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use runtime_common::redis_client::RedisSettings;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::bridge::cfg::{BridgeConfig, EndpointType, RedisKeyType, RouteConfig};
use crate::bridge::iceoryx::{PublisherEnum, SubscriberEnum};
use crate::bridge::redis_sync::RedisSyncClient;

pub struct BridgeApp {
    cfg: BridgeConfig,
}

#[derive(Clone)]
struct RouteCounter {
    route_id: Arc<str>,
    direction: &'static str,
    forwarded: Arc<AtomicU64>,
}

impl RouteCounter {
    fn new(route_id: String, direction: &'static str) -> Self {
        Self {
            route_id: Arc::<str>::from(route_id),
            direction,
            forwarded: Arc::new(AtomicU64::new(0)),
        }
    }

    fn inc(&self) {
        self.forwarded.fetch_add(1, Ordering::Relaxed);
    }
}

struct RedisTarget {
    settings: RedisSettings,
    client: Option<RedisSyncClient>,
    key: String,
    key_type: RedisKeyType,
    counter: RouteCounter,
}

impl RedisTarget {
    async fn apply(&mut self, payload: &[u8]) -> Result<()> {
        let value = redis_sync::decode(payload)?;
        if self.client.is_none() {
            self.client = Some(RedisSyncClient::connect(&self.settings).await?);
        }
        let result = self
            .client
            .as_mut()
            .expect("Redis client initialized")
            .apply(&self.key, self.key_type, value)
            .await;
        if result.is_err() {
            self.client = None;
        }
        result
    }
}

impl BridgeApp {
    pub fn new(cfg: BridgeConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self) -> Result<()> {
        let node_name = format!("ipc_bridge_{}", std::process::id());
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let mut outgoing_ipc = Vec::<RouteConfig>::new();
        let mut incoming_ipc = Vec::<RouteConfig>::new();
        let mut local_routes = Vec::<RouteConfig>::new();
        let mut outgoing_redis = Vec::<RouteConfig>::new();
        let mut incoming_redis = Vec::<RouteConfig>::new();
        for route in &self.cfg.routes {
            match (route.from.kind, route.to.kind) {
                (EndpointType::Ipc, EndpointType::Zmq) => outgoing_ipc.push(route.clone()),
                (EndpointType::Zmq, EndpointType::Ipc) => incoming_ipc.push(route.clone()),
                (EndpointType::Ipc, EndpointType::Ipc) => local_routes.push(route.clone()),
                (EndpointType::Redis, EndpointType::Zmq) => outgoing_redis.push(route.clone()),
                (EndpointType::Zmq, EndpointType::Redis) => incoming_redis.push(route.clone()),
                _ => {
                    return Err(anyhow!(
                        "route '{}' does not support {:?}->{:?}",
                        route.id,
                        route.from.kind,
                        route.to.kind
                    ));
                }
            }
        }

        info!(
            "ipc_bridge routes: ipc_to_zmq={} zmq_to_ipc={} ipc_to_ipc={} redis_to_zmq={} zmq_to_redis={}",
            outgoing_ipc.len(),
            incoming_ipc.len(),
            local_routes.len(),
            outgoing_redis.len(),
            incoming_redis.len()
        );

        let needs_zmq = !outgoing_ipc.is_empty()
            || !incoming_ipc.is_empty()
            || !outgoing_redis.is_empty()
            || !incoming_redis.is_empty();
        let zmq_ctx = needs_zmq.then(|| Arc::new(zmq::Context::new()));
        let zmq_source_ip = self
            .cfg
            .zmq_source_ip
            .as_deref()
            .map(str::trim)
            .map(str::to_owned);
        let mut route_counters = Vec::<RouteCounter>::new();

        let mut publishers = HashMap::<String, (PublisherEnum, RouteCounter)>::new();
        for route in &incoming_ipc {
            let service = bridge_service_name(&route.to.endpoint);
            let publisher = PublisherEnum::new(&node, &service, route.to.size, &route.to)?;
            let counter = RouteCounter::new(route.id.clone(), "zmq->ipc");
            route_counters.push(counter.clone());
            publishers.insert(route.id.clone(), (publisher, counter));
        }

        let mut redis_senders = HashMap::<String, mpsc::Sender<Vec<u8>>>::new();
        for route in &incoming_redis {
            let counter = RouteCounter::new(route.id.clone(), "zmq->redis");
            route_counters.push(counter.clone());
            let route_id = route.id.clone();
            let mut target = RedisTarget {
                settings: route.to.redis.clone(),
                client: None,
                key: route.to.endpoint.trim().to_string(),
                key_type: route.to.redis_type,
                counter,
            };
            let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);
            tokio::task::spawn_local(async move {
                while let Some(payload) = rx.recv().await {
                    match target.apply(&payload).await {
                        Ok(()) => target.counter.inc(),
                        Err(err) => warn!(
                            "apply Redis sync failed (route='{}' key='{}'): {err:#}",
                            route_id, target.key
                        ),
                    }
                }
            });
            redis_senders.insert(route.id.clone(), tx);
        }

        if !incoming_ipc.is_empty() || !incoming_redis.is_empty() {
            let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel::<(String, Vec<u8>)>();
            let bind_addrs: HashSet<String> = incoming_ipc
                .iter()
                .chain(incoming_redis.iter())
                .map(|route| route.from.endpoint.trim().to_string())
                .collect();

            for bind_addr in bind_addrs {
                let ctx = zmq_ctx.as_ref().expect("ZMQ context required").clone();
                let tx = incoming_tx.clone();
                tokio::task::spawn_blocking(move || {
                    let pull = ctx
                        .socket(zmq::PULL)
                        .map_err(|err| anyhow!("failed to create PULL socket: {err}"))?;
                    pull.bind(&bind_addr)
                        .map_err(|err| anyhow!("failed to bind PULL on {bind_addr}: {err}"))?;
                    info!("ZMQ PULL bound on {}", bind_addr);
                    loop {
                        match pull.recv_multipart(0) {
                            Ok(parts) if parts.len() >= 2 => {
                                let route_id = String::from_utf8_lossy(&parts[0]).to_string();
                                if tx.send((route_id, parts[1].clone())).is_err() {
                                    break;
                                }
                            }
                            Ok(parts) => warn!(
                                "ZMQ message too short on {}: frames={}",
                                bind_addr,
                                parts.len()
                            ),
                            Err(err) => {
                                warn!("ZMQ recv error on {}: {err}", bind_addr);
                                std::thread::sleep(Duration::from_millis(200));
                            }
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                });
            }

            tokio::task::spawn_local(async move {
                while let Some((route_id, payload)) = incoming_rx.recv().await {
                    if let Some((publisher, counter)) = publishers.get(&route_id) {
                        match publisher.publish(&payload) {
                            Ok(()) => counter.inc(),
                            Err(err) => {
                                warn!("publish iceoryx failed (route='{}'): {err}", route_id)
                            }
                        }
                    } else if let Some(tx) = redis_senders.get(&route_id) {
                        match tx.try_send(payload) {
                            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                warn!("Redis sync worker stopped (route='{}')", route_id);
                            }
                        }
                    } else {
                        warn!(
                            "received unknown route '{}' ({} bytes)",
                            route_id,
                            payload.len()
                        );
                    }
                }
            });
        }

        for route in outgoing_ipc {
            let remote_addr = route.to.endpoint.trim().to_string();
            let route_id = route.id.clone();
            let from_service = bridge_service_name(&route.from.endpoint);
            let route_from = route.from.clone();
            let counter = RouteCounter::new(route_id.clone(), "ipc->zmq");
            route_counters.push(counter.clone());
            let tx = spawn_zmq_sender(
                zmq_ctx.as_ref().expect("ZMQ context required").clone(),
                route_id.clone(),
                remote_addr.clone(),
                zmq_source_ip.clone(),
            );

            tokio::task::spawn_local(async move {
                info!(
                    "route '{}' ipc->zmq started: from='{}' size={} -> to='{}'",
                    route_id, from_service, route_from.size, remote_addr
                );
                let route_node =
                    match create_route_node("ipc_to_zmq_src", &route_id, std::process::id()) {
                        Ok(node) => node,
                        Err(err) => {
                            warn!(
                                "route '{}' failed to create subscriber node: {err:#}",
                                route_id
                            );
                            return;
                        }
                    };
                let mut subscriber = None::<SubscriberEnum>;
                loop {
                    if subscriber.is_none() {
                        match SubscriberEnum::new(
                            &route_node,
                            &from_service,
                            route_from.size,
                            &route_from,
                        ) {
                            Ok(value) => {
                                info!("route '{}' connected source '{}'", route_id, from_service);
                                subscriber = Some(value);
                            }
                            Err(err) => {
                                warn!(
                                    "route '{}' waiting for source '{}': {err:#}",
                                    route_id, from_service
                                );
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue;
                            }
                        }
                    }
                    match subscriber
                        .as_ref()
                        .expect("subscriber initialized")
                        .receive_msg()
                    {
                        Ok(Some(bytes)) => {
                            if tx.send(bytes.to_vec()).is_err() {
                                break;
                            }
                            counter.inc();
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
                            warn!(
                                "iceoryx receive error (route='{}'), reconnecting: {err}",
                                route_id
                            );
                            subscriber = None;
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            });
        }

        for route in outgoing_redis {
            let route_id = route.id.clone();
            let remote_addr = route.to.endpoint.trim().to_string();
            let settings = route.from.redis.clone();
            let key = route.from.endpoint.trim().to_string();
            let key_type = route.from.redis_type;
            let poll_interval = Duration::from_millis(route.from.poll_interval_ms);
            let counter = RouteCounter::new(route_id.clone(), "redis->zmq");
            route_counters.push(counter.clone());
            let tx = spawn_zmq_sender(
                zmq_ctx.as_ref().expect("ZMQ context required").clone(),
                route_id.clone(),
                remote_addr.clone(),
                zmq_source_ip.clone(),
            );

            tokio::task::spawn_local(async move {
                info!(
                    "route '{}' redis->zmq started: key='{}' type={:?} interval_ms={} -> '{}'",
                    route_id,
                    key,
                    key_type,
                    poll_interval.as_millis(),
                    remote_addr
                );
                let mut client = None::<RedisSyncClient>;
                let mut interval = tokio::time::interval(poll_interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if client.is_none() {
                        match RedisSyncClient::connect(&settings).await {
                            Ok(value) => client = Some(value),
                            Err(err) => {
                                warn!("route '{}' Redis connect failed: {err:#}", route_id);
                                continue;
                            }
                        }
                    }
                    let result = client
                        .as_mut()
                        .expect("Redis client initialized")
                        .read(&key, key_type)
                        .await;
                    match result.and_then(|value| redis_sync::encode(&value)) {
                        Ok(payload) => {
                            if tx.send(payload).is_err() {
                                break;
                            }
                            counter.inc();
                        }
                        Err(err) => {
                            warn!(
                                "route '{}' Redis read failed (key='{}'): {err:#}",
                                route_id, key
                            );
                            client = None;
                        }
                    }
                }
            });
        }

        for route in local_routes {
            let route_id = route.id.clone();
            let from_service = bridge_service_name(&route.from.endpoint);
            let to_service = bridge_service_name(&route.to.endpoint);
            let route_from = route.from.clone();
            let publisher = PublisherEnum::new(&node, &to_service, route.to.size, &route.to)?;
            let counter = RouteCounter::new(route_id.clone(), "ipc->ipc");
            route_counters.push(counter.clone());

            tokio::task::spawn_local(async move {
                let route_node =
                    match create_route_node("ipc_to_ipc_src", &route_id, std::process::id()) {
                        Ok(node) => node,
                        Err(err) => {
                            warn!(
                                "route '{}' failed to create local subscriber node: {err:#}",
                                route_id
                            );
                            return;
                        }
                    };
                let mut subscriber = None::<SubscriberEnum>;
                loop {
                    if subscriber.is_none() {
                        match SubscriberEnum::new(
                            &route_node,
                            &from_service,
                            route_from.size,
                            &route_from,
                        ) {
                            Ok(value) => subscriber = Some(value),
                            Err(err) => {
                                warn!(
                                    "route '{}' waiting for local source '{}': {err:#}",
                                    route_id, from_service
                                );
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue;
                            }
                        }
                    }
                    match subscriber
                        .as_ref()
                        .expect("subscriber initialized")
                        .receive_msg()
                    {
                        Ok(Some(bytes)) => match publisher.publish(&bytes) {
                            Ok(()) => counter.inc(),
                            Err(err) => {
                                warn!("local bridge publish error (route='{}'): {err}", route_id);
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                        },
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
                            warn!(
                                "local bridge receive error (route='{}'), reconnecting: {err}",
                                route_id
                            );
                            subscriber = None;
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            });
        }

        if !route_counters.is_empty() {
            tokio::task::spawn_local(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    for counter in &route_counters {
                        let delta = counter.forwarded.swap(0, Ordering::Relaxed);
                        info!(
                            "route '{}' {} count_30s={}",
                            counter.route_id, counter.direction, delta
                        );
                    }
                }
            });
        }

        let shutdown_signal = wait_shutdown_signal().await?;
        info!("ipc_bridge shutdown: {}", shutdown_signal);
        Ok(())
    }
}

fn spawn_zmq_sender(
    ctx: Arc<zmq::Context>,
    route_id: String,
    remote_addr: String,
    source_ip: Option<String>,
) -> mpsc::UnboundedSender<Vec<u8>> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    tokio::task::spawn_blocking(move || {
        let connect_addr = zmq_connect_addr(&remote_addr, source_ip.as_deref())?;
        let push = ctx
            .socket(zmq::PUSH)
            .map_err(|err| anyhow!("failed to create PUSH socket: {err}"))?;
        push.connect(&connect_addr)
            .map_err(|err| anyhow!("failed to connect PUSH to {connect_addr}: {err}"))?;
        info!(
            "ZMQ PUSH connected: route='{}' -> {} source_ip='{}'",
            route_id,
            remote_addr,
            source_ip.as_deref().unwrap_or("default")
        );
        while let Some(payload) = rx.blocking_recv() {
            if let Err(err) = push.send_multipart([route_id.as_bytes(), &payload], 0) {
                warn!(
                    "ZMQ send error (route='{}' addr='{}'): {err}",
                    route_id, remote_addr
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
        Ok::<(), anyhow::Error>(())
    });
    tx
}

fn zmq_connect_addr(remote_addr: &str, source_ip: Option<&str>) -> Result<String> {
    let Some(source_ip) = source_ip else {
        return Ok(remote_addr.to_owned());
    };
    let destination = remote_addr.strip_prefix("tcp://").ok_or_else(|| {
        anyhow!("zmq_source_ip requires a tcp:// destination, got '{remote_addr}'")
    })?;
    if destination.contains(';') {
        return Err(anyhow!(
            "ZMQ destination already contains a source address: '{remote_addr}'"
        ));
    }
    Ok(format!("tcp://{source_ip}:0;{destination}"))
}

async fn wait_shutdown_signal() -> Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                Ok("SIGINT")
            }
            _ = sigterm.recv() => Ok("SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok("SIGINT")
    }
}

fn bridge_service_name(endpoint: &str) -> String {
    endpoint.trim().to_string()
}

fn create_route_node(prefix: &str, route_id: &str, pid: u32) -> Result<Node<ipc::Service>> {
    let route_tag: String = route_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let node_name = format!("{}_{}_{}", prefix, pid, route_tag);
    Ok(NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?)
}

#[cfg(test)]
mod tests {
    use super::zmq_connect_addr;

    #[test]
    fn adds_zmq_tcp_source_ip() {
        assert_eq!(
            zmq_connect_addr("tcp://47.131.162.78:6360", Some("172.31.46.90")).unwrap(),
            "tcp://172.31.46.90:0;47.131.162.78:6360"
        );
    }

    #[test]
    fn keeps_zmq_endpoint_without_source_ip() {
        assert_eq!(
            zmq_connect_addr("ipc:///tmp/test", None).unwrap(),
            "ipc:///tmp/test"
        );
    }

    #[test]
    fn rejects_non_tcp_endpoint_with_source_ip() {
        assert!(zmq_connect_addr("ipc:///tmp/test", Some("172.31.46.90"))
            .expect_err("source IP requires TCP")
            .to_string()
            .contains("requires a tcp:// destination"));
    }
}
