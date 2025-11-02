use super::client::{TradeWsClient, WsCommand};
use crate::common::exchange::Exchange;
use crate::trade_engine::config::WsCfg;
use crate::trade_engine::trade_request::TradeRequestMsg;
use crate::trade_engine::trade_response_handle::TradeExecOutcome;
use anyhow::{anyhow, Result};
use log::{debug, info, warn};
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct TradeWsDispatcher {
    inner: Arc<TradeWsDispatcherInner>,
}

struct TradeWsDispatcherInner {
    endpoints: Vec<mpsc::UnboundedSender<WsCommand>>,
    rr_cursor: AtomicUsize,
}

impl TradeWsDispatcher {
    pub fn new(
        cfg: WsCfg,
        local_ips: Vec<IpAddr>,
        exchange: Exchange,
        resp_tx: mpsc::UnboundedSender<TradeExecOutcome>,
    ) -> Result<Self> {
        if cfg.urls.is_empty() {
            return Err(anyhow!("ws urls configuration is empty"));
        }
        if cfg.urls.len() != local_ips.len() {
            return Err(anyhow!(
                "ws urls count ({}) must match local_ips count ({})",
                cfg.urls.len(),
                local_ips.len()
            ));
        }

        let connect_timeout_ms = cfg.connect_timeout_ms.unwrap_or(5_000);
        let ping_interval_ms = cfg.ping_interval_ms.unwrap_or(15_000);
        let max_inflight = cfg.max_inflight.unwrap_or(128);
        let login_payload = cfg.login_payload.clone();

        let mut endpoints = Vec::with_capacity(cfg.urls.len());
        for (idx, (ip, url)) in local_ips.into_iter().zip(cfg.urls.into_iter()).enumerate() {
            let (tx, rx) = mpsc::unbounded_channel();
            let client = TradeWsClient::new(
                idx,
                exchange,
                ip,
                url,
                connect_timeout_ms,
                ping_interval_ms,
                max_inflight,
                login_payload.clone(),
                rx,
                resp_tx.clone(),
            );
            info!(
                "trade ws dispatcher spawning client id={} ip={} max_inflight={}",
                idx,
                client.local_ip(),
                max_inflight
            );
            tokio::task::spawn_local(async move {
                client.run().await;
            });
            endpoints.push(tx);
        }

        Ok(Self {
            inner: Arc::new(TradeWsDispatcherInner {
                endpoints,
                rr_cursor: AtomicUsize::new(0),
            }),
        })
    }

    pub fn submit(&self, msg: TradeRequestMsg) -> Result<()> {
        let len = self.inner.endpoints.len();
        if len == 0 {
            return Err(anyhow!("no websocket endpoints available"));
        }
        let start = self.inner.rr_cursor.fetch_add(1, Ordering::Relaxed);
        for offset in 0..len {
            let idx = (start + offset) % len;
            debug!(
                "trade ws dispatcher routing order client_order_id={} to endpoint index={}",
                msg.client_order_id, idx
            );
            if self.inner.endpoints[idx]
                .send(WsCommand::Send(msg.clone()))
                .is_ok()
            {
                return Ok(());
            } else {
                warn!("ws endpoint {} not accepting messages, trying next", idx);
            }
        }
        Err(anyhow!("all websocket endpoints are unavailable"))
    }

    pub fn broadcast_shutdown(&self) {
        for tx in &self.inner.endpoints {
            let _ = tx.send(WsCommand::Shutdown);
        }
    }
}
