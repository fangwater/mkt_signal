use super::{config::Counter, now_us};
use anyhow::{ensure, Context, Result};
use cn_futures_state::Snapshot;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct Hub {
    pub tx: broadcast::Sender<String>,
    latest: Arc<Mutex<BTreeMap<String, Frame>>>,
}
struct Frame {
    value: Value,
    raw: String,
}
impl Hub {
    pub fn new(sources: &[Counter], stale_after_ms: u64) -> Self {
        let (tx, _) = broadcast::channel(128);
        let latest = sources
            .iter()
            .map(|s| {
                let value = json!({"type":"cn_futures_state", "source_id":s.id, "label":s.label,
                "account":s.account, "adapter":s.adapter, "namespace":s.position_namespace,
                "stale_after_ms":stale_after_ms, "entry":null, "error":"等待持仓 IPC"});
                (
                    s.id.clone(),
                    Frame {
                        raw: value.to_string(),
                        value,
                    },
                )
            })
            .collect();
        Self {
            tx,
            latest: Arc::new(Mutex::new(latest)),
        }
    }
    /// Relay the producer's complete state immediately; no projection or timer.
    pub fn apply(&self, source: &Counter, snapshot: Snapshot) -> Result<()> {
        snapshot.validate()?;
        ensure!(
            snapshot.adapter == source.adapter
                && snapshot.account_hash.as_str() == source.expected_account_hash,
            "position IPC account/adapter identity mismatch"
        );
        let mut cache = self.latest.lock().unwrap_or_else(|e| e.into_inner());
        let frame = cache.get_mut(&source.id).context("unknown native source")?;
        let previous = &frame.value["entry"];
        if previous["publisher_epoch"].as_str() == Some(&snapshot.publisher_epoch.to_string()) {
            let sequence = previous["sequence"]
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            ensure!(
                snapshot.sequence >= sequence,
                "position IPC sequence regressed"
            );
            if snapshot.sequence == sequence {
                return Ok(());
            }
        }
        let mut entry = serde_json::to_value(&snapshot)?;
        entry
            .as_object_mut()
            .context("invalid native snapshot")?
            .remove("account_hash");
        entry["publisher_epoch"] = json!(snapshot.publisher_epoch.to_string());
        entry["sequence"] = json!(snapshot.sequence.to_string());
        entry["replaced_snapshots"] = json!(snapshot.replaced_snapshots.to_string());
        entry["l1_dropped"] = json!(snapshot.l1_dropped.to_string());
        frame.value["entry"] = entry;
        frame.value["error"] = Value::Null;
        frame.raw = frame.value.to_string();
        let _ = self.tx.send(frame.raw.clone());
        Ok(())
    }
    pub fn unavailable(&self, source: &Counter, error: &str) {
        let mut cache = self.latest.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(frame) = cache.get_mut(&source.id) {
            if frame.value["error"].as_str() == Some(error) {
                return;
            }
            frame.value["error"] = json!(error);
            frame.raw = frame.value.to_string();
            let _ = self.tx.send(frame.raw.clone());
        }
    }
    pub fn latest(&self) -> Vec<String> {
        self.latest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|f| f.raw.clone())
            .collect()
    }
    /// Operations-only transport health; the dashboard consumes WebSocket frames.
    pub fn health(&self) -> Value {
        let now = now_us();
        let cache = self.latest.lock().unwrap_or_else(|e| e.into_inner());
        let sources: Vec<_> = cache.values().map(|f| {
            let e = &f.value["entry"];
            let fresh = e["observed_us"].as_u64().is_some_and(|ts| now >= ts && now - ts <= f.value["stale_after_ms"].as_u64().unwrap_or(0) * 1000);
            json!({"source_id":f.value["source_id"], "error":f.value["error"], "fresh":fresh,
                "observed_us":e["observed_us"], "runtime_status":e["runtime_status"],
                "position_ready":e["position_ready"], "connected":e["connected"], "ready":e["ready"],
                "can_trade":e["can_trade"], "transport_ready":e["transport_ready"],
                "trading_day":e["trading_day"], "position_rows":e["positions"].as_array().map(Vec::len),
                "l1_connected":e["l1_connected"], "mark_rows":e["marks"].as_array().map(Vec::len)})
        }).collect();
        json!({"ok":true, "sources":sources})
    }
}
