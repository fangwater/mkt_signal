pub mod cfg;
mod iceoryx;

use anyhow::{anyhow, Result};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::bridge::cfg::{BridgeConfig, EndpointType, RouteConfig};
use crate::bridge::iceoryx::{PublisherEnum, SubscriberEnum};

/// Bridge app that forwards Iceoryx2 IPC messages across network with ZMQ.
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

impl BridgeApp {
    pub fn new(cfg: BridgeConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self) -> Result<()> {
        let node_name = format!("ipc_bridge_{}", std::process::id());
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let mut outgoing_routes: Vec<RouteConfig> = Vec::new();
        let mut incoming_routes: Vec<RouteConfig> = Vec::new();
        let mut local_routes: Vec<RouteConfig> = Vec::new();
        for r in &self.cfg.routes {
            match (r.from.kind, r.to.kind) {
                (EndpointType::Ipc, EndpointType::Zmq) => outgoing_routes.push(r.clone()),
                (EndpointType::Zmq, EndpointType::Ipc) => incoming_routes.push(r.clone()),
                (EndpointType::Ipc, EndpointType::Ipc) => local_routes.push(r.clone()),
                (EndpointType::Zmq, EndpointType::Zmq) => {
                    return Err(anyhow!("route '{}' does not support zmq->zmq", r.id));
                }
            }
        }

        info!(
            "ipc_bridge routes: ipc_to_zmq={} zmq_to_ipc={} ipc_to_ipc={}",
            outgoing_routes.len(),
            incoming_routes.len(),
            local_routes.len()
        );

        let needs_zmq = !outgoing_routes.is_empty() || !incoming_routes.is_empty();
        let zmq_ctx = if needs_zmq {
            Some(Arc::new(zmq::Context::new()))
        } else {
            None
        };
        let mut route_counters: Vec<RouteCounter> = Vec::new();

        let mut publishers: HashMap<String, PublisherEnum> = HashMap::new();
        for r in &incoming_routes {
            let svc = bridge_service_name(&r.to.endpoint);
            let pub_enum = PublisherEnum::new(&node, &svc, r.to.size, &r.to)?;
            publishers.insert(r.id.clone(), pub_enum);
            route_counters.push(RouteCounter::new(r.id.clone(), "zmq->ipc"));
        }

        if !incoming_routes.is_empty() {
            let incoming_counter_map: Arc<HashMap<String, RouteCounter>> = Arc::new(
                route_counters
                    .iter()
                    .filter(|counter| counter.direction == "zmq->ipc")
                    .map(|counter| (counter.route_id.to_string(), counter.clone()))
                    .collect(),
            );
            let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel::<(String, Vec<u8>)>();
            let bind_addrs: HashSet<String> = incoming_routes
                .iter()
                .map(|r| r.from.endpoint.trim().to_string())
                .collect();

            for bind_addr in bind_addrs {
                let ctx = zmq_ctx
                    .as_ref()
                    .expect("zmq ctx required for incoming routes")
                    .clone();
                let tx = incoming_tx.clone();
                tokio::task::spawn_blocking(move || {
                    let pull = ctx
                        .socket(zmq::PULL)
                        .map_err(|e| anyhow!("failed to create PULL socket: {e}"))?;
                    pull.bind(&bind_addr)
                        .map_err(|e| anyhow!("failed to bind PULL on {bind_addr}: {e}"))?;
                    info!("ZMQ PULL bound on {}", bind_addr);

                    loop {
                        match pull.recv_multipart(0) {
                            Ok(parts) => {
                                if parts.len() < 2 {
                                    warn!(
                                        "ZMQ message too short on {}: frames={}",
                                        bind_addr,
                                        parts.len()
                                    );
                                    continue;
                                }
                                let route_id = String::from_utf8_lossy(&parts[0]).to_string();
                                let payload = parts[1].clone();
                                if tx.send((route_id, payload)).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("ZMQ recv error on {}: {e}", bind_addr);
                                std::thread::sleep(Duration::from_millis(200));
                            }
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                });
            }

            tokio::task::spawn_local(async move {
                while let Some((route_id, payload)) = incoming_rx.recv().await {
                    match publishers.get(&route_id) {
                        Some(pub_) => {
                            if let Err(e) = pub_.publish(&payload) {
                                warn!("publish iceoryx failed (route='{}'): {e}", route_id);
                            } else if let Some(counter) = incoming_counter_map.get(&route_id) {
                                counter.inc();
                            }
                        }
                        None => {
                            warn!(
                                "received unknown route '{}' ({} bytes)",
                                route_id,
                                payload.len()
                            );
                        }
                    }
                }
            });
        }

        for r in outgoing_routes {
            let remote_addr = r.to.endpoint.trim().to_string();
            let route_id = r.id.clone();
            let from_service = bridge_service_name(&r.from.endpoint);
            let route_from = r.from.clone();
            let route_counter = RouteCounter::new(route_id.clone(), "ipc->zmq");
            route_counters.push(route_counter.clone());
            let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

            {
                let ctx = zmq_ctx
                    .as_ref()
                    .expect("zmq ctx required for outgoing routes")
                    .clone();
                let rid = route_id.clone();
                let addr = remote_addr.clone();
                tokio::task::spawn_blocking(move || {
                    let push = ctx
                        .socket(zmq::PUSH)
                        .map_err(|e| anyhow!("failed to create PUSH socket: {e}"))?;
                    push.connect(&addr)
                        .map_err(|e| anyhow!("failed to connect PUSH to {addr}: {e}"))?;
                    info!("ZMQ PUSH connected: route='{}' -> {}", rid, addr);

                    while let Some(payload) = rx.blocking_recv() {
                        if let Err(e) = push.send_multipart([rid.as_bytes(), &payload], 0) {
                            warn!("ZMQ send error (route='{}' addr='{}'): {e}", rid, addr);
                            std::thread::sleep(Duration::from_millis(200));
                        }
                    }

                    Ok::<(), anyhow::Error>(())
                });
            }

            tokio::task::spawn_local(async move {
                info!(
                    "route '{}' ipc->zmq started: from='{}' size={} -> to='{}' size={}",
                    route_id, from_service, r.from.size, remote_addr, r.to.size
                );
                let route_node =
                    match create_route_node("ipc_to_zmq_src", &route_id, std::process::id()) {
                        Ok(node) => node,
                        Err(e) => {
                            warn!(
                                "route '{}' failed to create subscriber node for '{}': {e:#}",
                                route_id, from_service
                            );
                            return;
                        }
                    };
                let mut subscriber: Option<SubscriberEnum> = None;
                loop {
                    if subscriber.is_none() {
                        match SubscriberEnum::new(
                            &route_node,
                            &from_service,
                            route_from.size,
                            &route_from,
                        ) {
                            Ok(sub) => {
                                info!(
                                    "route '{}' connected iceoryx source '{}'",
                                    route_id, from_service
                                );
                                subscriber = Some(sub);
                            }
                            Err(e) => {
                                warn!(
                                    "route '{}' waiting for iceoryx source '{}': {e:#}",
                                    route_id, from_service
                                );
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue;
                            }
                        }
                    }

                    match subscriber
                        .as_ref()
                        .expect("subscriber must exist after successful open")
                        .receive_msg()
                    {
                        Ok(Some(bytes)) => {
                            if tx.send(bytes.to_vec()).is_err() {
                                break;
                            }
                            route_counter.inc();
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(e) => {
                            warn!(
                                "iceoryx receive error (route='{}' service='{}'), reconnecting: {e}",
                                route_id, from_service
                            );
                            subscriber = None;
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            });
        }

        for r in local_routes {
            let route_id = r.id.clone();
            let from_service = bridge_service_name(&r.from.endpoint);
            let to_service = bridge_service_name(&r.to.endpoint);
            let route_from = r.from.clone();
            let publisher = PublisherEnum::new(&node, &to_service, r.to.size, &r.to)?;
            let route_counter = RouteCounter::new(route_id.clone(), "ipc->ipc");
            route_counters.push(route_counter.clone());

            tokio::task::spawn_local(async move {
                info!(
                    "route '{}' ipc->ipc started: from='{}' size={} -> to='{}' size={}",
                    route_id, from_service, r.from.size, to_service, r.to.size
                );
                let route_node =
                    match create_route_node("ipc_to_ipc_src", &route_id, std::process::id()) {
                        Ok(node) => node,
                        Err(e) => {
                            warn!(
                                "route '{}' failed to create subscriber node for '{}': {e:#}",
                                route_id, from_service
                            );
                            return;
                        }
                    };
                let mut subscriber: Option<SubscriberEnum> = None;
                loop {
                    if subscriber.is_none() {
                        match SubscriberEnum::new(
                            &route_node,
                            &from_service,
                            route_from.size,
                            &route_from,
                        ) {
                            Ok(sub) => {
                                info!(
                                    "route '{}' connected local iceoryx source '{}'",
                                    route_id, from_service
                                );
                                subscriber = Some(sub);
                            }
                            Err(e) => {
                                warn!(
                                    "route '{}' waiting for local iceoryx source '{}': {e:#}",
                                    route_id, from_service
                                );
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue;
                            }
                        }
                    }

                    match subscriber
                        .as_ref()
                        .expect("subscriber must exist after successful open")
                        .receive_msg()
                    {
                        Ok(Some(bytes)) => {
                            if let Err(e) = publisher.publish(&bytes) {
                                warn!(
                                    "local bridge publish error (route='{}' from='{}' to='{}'): {e}",
                                    route_id, from_service, to_service
                                );
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            } else {
                                route_counter.inc();
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(e) => {
                            warn!(
                                "local bridge receive error (route='{}' service='{}'), reconnecting: {e}",
                                route_id, from_service
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

        tokio::signal::ctrl_c().await?;
        info!("ipc_bridge shutdown");
        Ok(())
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
