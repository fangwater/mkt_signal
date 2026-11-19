use super::{config::Counter, now_us};
use anyhow::{ensure, Context, Result};
use cn_futures_state::{checkpoint, Snapshot};
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
    saved: Option<Snapshot>,
    live_confirmed: bool,
}
fn display_entry(snapshot: &Snapshot) -> Result<Value> {
    let mut entry = serde_json::to_value(snapshot)?;
    entry
        .as_object_mut()
        .context("invalid native snapshot")?
        .remove("account_hash");
    entry["publisher_epoch"] = json!(snapshot.publisher_epoch.to_string());
    entry["sequence"] = json!(snapshot.sequence.to_string());
    entry["replaced_snapshots"] = json!(snapshot.replaced_snapshots.to_string());
    entry["l1_dropped"] = json!(snapshot.l1_dropped.to_string());
    Ok(entry)
}

impl Frame {
    fn render_saved(&mut self) -> Result<()> {
        let next = if self.live_confirmed {
            None
        } else {
            self.saved.as_ref().map(display_entry).transpose()?
        };
        if let Some(next) = next {
            self.value["saved_entry"] = next;
        } else if let Some(fields) = self.value.as_object_mut() {
            fields.remove("saved_entry");
        }
        Ok(())
    }
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
                        saved: None,
                        live_confirmed: false,
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
        let entry = display_entry(&snapshot)?;
        frame.live_confirmed = checkpoint::confirmed(&snapshot);
        if source.account_snapshot_path.is_some() && frame.live_confirmed {
            let mut observed = checkpoint::for_display(snapshot);
            if observed.account.is_none() {
                observed.account = frame
                    .saved
                    .as_ref()
                    .filter(|saved| saved.trading_day == observed.trading_day)
                    .and_then(|saved| saved.account);
            }
            frame.saved = Some(observed);
        }
        frame.value["entry"] = entry;
        frame.value["error"] = Value::Null;
        frame.render_saved()?;
        frame.raw = frame.value.to_string();
        let _ = self.tx.send(frame.raw.clone());
        Ok(())
    }
    pub fn retained(&self, source: &Counter) -> Option<Snapshot> {
        let cache = self.latest.lock().unwrap_or_else(|e| e.into_inner());
        let frame = cache.get(&source.id)?;
        (!frame.live_confirmed)
            .then(|| frame.saved.clone())
            .flatten()
    }
    pub fn restore(&self, source: &Counter, snapshot: Snapshot) -> Result<()> {
        snapshot.validate()?;
        ensure!(
            snapshot.position_ready
                && snapshot.adapter == source.adapter
                && snapshot.account_hash.as_str() == source.expected_account_hash,
            "account snapshot identity mismatch"
        );
        let mut cache = self.latest.lock().unwrap_or_else(|e| e.into_inner());
        let frame = cache.get_mut(&source.id).context("unknown native source")?;
        if frame.live_confirmed
            || frame
                .saved
                .as_ref()
                .is_some_and(|s| s.observed_us > snapshot.observed_us)
        {
            return Ok(());
        }
        frame.saved = Some(checkpoint::for_display(snapshot));
        frame.render_saved()?;
        let raw = frame.value.to_string();
        if raw != frame.raw {
            frame.raw = raw;
            let _ = self.tx.send(frame.raw.clone());
        }
        Ok(())
    }
    pub fn unavailable(&self, source: &Counter, error: &str) {
        let mut cache = self.latest.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(frame) = cache.get_mut(&source.id) {
            if frame.value["error"].as_str() == Some(error) {
                return;
            }
            frame.value["error"] = json!(error);
            frame.live_confirmed = false;
            // The observation has already been validated when it entered the cache.
            if let Err(error) = frame.render_saved() {
                log::warn!("format cached native account {}: {error:#}", source.id);
            }
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
            let account_fresh = fresh && f.value["error"].is_null()
                && e["runtime_status"].as_str() == Some("running")
                && e["connected"].as_bool() == Some(true)
                && e["transport_ready"].as_bool() == Some(true)
                && e["account"]["observed_us"].as_u64().is_some_and(|ts| now >= ts && now - ts <= 30_000_000);
            json!({"source_id":f.value["source_id"], "error":f.value["error"], "fresh":fresh,
                "observed_us":e["observed_us"], "runtime_status":e["runtime_status"],
                "position_ready":e["position_ready"], "connected":e["connected"], "ready":e["ready"],
                "can_trade":e["can_trade"], "transport_ready":e["transport_ready"],
                "trading_day":e["trading_day"], "position_rows":e["positions"].as_array().map(Vec::len),
                "account_available":e["account"].is_object(), "account_fresh":account_fresh,
                "account_observed_us":e["account"]["observed_us"],
                "l1_connected":e["l1_connected"], "mark_rows":e["marks"].as_array().map(Vec::len)})
        }).collect();
        json!({"ok":true, "sources":sources})
    }
}
