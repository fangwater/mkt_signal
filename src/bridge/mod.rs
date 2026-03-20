pub mod cfg;
mod iceoryx;

use anyhow::{anyhow, Result};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::bridge::cfg::{BridgeConfig, EndpointType, RouteConfig};
use crate::bridge::iceoryx::{PublisherEnum, SubscriberEnum};
use crate::common::ipc_service_name::build_service_name;

/// Bridge app that forwards Iceoryx2 IPC messages across network with ZMQ.
pub struct BridgeApp {
    cfg: BridgeConfig,
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

        let mut publishers: HashMap<String, PublisherEnum> = HashMap::new();
        for r in &incoming_routes {
            let svc = build_service_name(&r.to.endpoint);
            let pub_enum = PublisherEnum::new(&node, &svc, r.to.size, &r.to)?;
            publishers.insert(r.id.clone(), pub_enum);
        }

        if !incoming_routes.is_empty() {
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
            let from_service = build_service_name(&r.from.endpoint);
            let subscriber = SubscriberEnum::new(&node, &from_service, r.from.size, &r.from)?;
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
                        if let Err(e) = push.send_multipart(&[rid.as_bytes(), &payload], 0) {
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
                loop {
                    match subscriber.receive_msg() {
                        Ok(Some(bytes)) => {
                            if tx.send(bytes.to_vec()).is_err() {
                                break;
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(e) => {
                            warn!(
                                "iceoryx receive error (route='{}' service='{}'): {e}",
                                route_id, from_service
                            );
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            });
        }

        for r in local_routes {
            let route_id = r.id.clone();
            let from_service = build_service_name(&r.from.endpoint);
            let to_service = build_service_name(&r.to.endpoint);
            let subscriber = SubscriberEnum::new(&node, &from_service, r.from.size, &r.from)?;
            let publisher = PublisherEnum::new(&node, &to_service, r.to.size, &r.to)?;

            tokio::task::spawn_local(async move {
                info!(
                    "route '{}' ipc->ipc started: from='{}' size={} -> to='{}' size={}",
                    route_id, from_service, r.from.size, to_service, r.to.size
                );
                loop {
                    match subscriber.receive_msg() {
                        Ok(Some(bytes)) => {
                            if let Err(e) = publisher.publish(&bytes) {
                                warn!(
                                    "local bridge publish error (route='{}' from='{}' to='{}'): {e}",
                                    route_id, from_service, to_service
                                );
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(e) => {
                            warn!(
                                "local bridge receive error (route='{}' service='{}'): {e}",
                                route_id, from_service
                            );
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            });
        }

        tokio::signal::ctrl_c().await?;
        info!("ipc_bridge shutdown");
        Ok(())
    }
}
