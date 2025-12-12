pub mod cfg;
mod iceoryx;

use anyhow::{anyhow, Result};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::bridge::cfg::{BridgeConfig, RouteConfig};
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
        let self_id = self.cfg.self_id.clone();
        let self_addr = self.cfg.self_node().addr.clone();

        info!(
            "ipc_bridge starting; self_id='{}' bind_addr='{}'",
            self_id, self_addr
        );

        let node_name = format!("ipc_bridge_{}", sanitize_node_suffix(&self_id));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let zmq_ctx = Arc::new(zmq::Context::new());

        // Split routes by direction relative to self
        let mut outgoing_routes: Vec<RouteConfig> = Vec::new();
        let mut incoming_routes: Vec<RouteConfig> = Vec::new();
        for r in &self.cfg.routes {
            if r.from.node == self_id && r.to.node != self_id {
                outgoing_routes.push(r.clone());
            } else if r.to.node == self_id && r.from.node != self_id {
                incoming_routes.push(r.clone());
            } else if r.from.node == self_id && r.to.node == self_id {
                warn!(
                    "route '{}' has from.node == to.node == self_id; skipping",
                    r.id
                );
            }
        }

        info!(
            "ipc_bridge routes: outgoing={} incoming={}",
            outgoing_routes.len(),
            incoming_routes.len()
        );

        // Incoming dispatcher: ZMQ -> iceoryx publisher
        let mut publishers: HashMap<String, PublisherEnum> = HashMap::new();
        for r in &incoming_routes {
            let svc = build_service_name(&r.to.service);
            let pub_enum = PublisherEnum::new(&node, &svc, r.to.size)?;
            publishers.insert(r.id.clone(), pub_enum);
        }

        let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel::<(String, Vec<u8>)>();

        // ZMQ receiver thread
        {
            let ctx = zmq_ctx.clone();
            let bind_addr = self_addr.clone();
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
                                warn!("ZMQ message too short: frames={}", parts.len());
                                continue;
                            }
                            let route_id = String::from_utf8_lossy(&parts[0]).to_string();
                            let payload = parts[1].clone();
                            if tx.send((route_id, payload)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            warn!("ZMQ recv error: {e}");
                            std::thread::sleep(Duration::from_millis(200));
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            });
        }

        // Async incoming dispatcher task (local)
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

        // Outgoing routes: iceoryx subscriber -> ZMQ PUSH
        for r in outgoing_routes {
            let remote_addr = self
                .cfg
                .node_addr(&r.to.node)
                .ok_or_else(|| anyhow!("remote node '{}' missing addr", r.to.node))?
                .to_string();
            let route_id = r.id.clone();

            let from_service = build_service_name(&r.from.service);
            let subscriber = SubscriberEnum::new(&node, &from_service, r.from.size)?;

            let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

            // ZMQ sender thread for this route
            {
                let ctx = zmq_ctx.clone();
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

            // Iceoryx polling task
            tokio::task::spawn_local(async move {
                info!(
                    "route '{}' outgoing started: from='{}' size={} -> node='{}'",
                    route_id, from_service, r.from.size, r.to.node
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

        tokio::signal::ctrl_c().await?;
        info!("ipc_bridge shutdown");
        Ok(())
    }
}

fn sanitize_node_suffix(raw: &str) -> String {
    let normalized = raw.trim();
    if normalized.is_empty() {
        return "default".to_string();
    }
    normalized
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
