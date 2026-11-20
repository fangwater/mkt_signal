//! One native product per strategy-group publisher, with durable ambiguous-outcome retry.
mod client;
#[cfg(test)]
mod tests;

use super::{config::Config, engine::Engine, extras::Extras};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::{sync::watch, task::JoinHandle};

pub type Targets = BTreeMap<String, i64>;
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyPosition {
    pub decision_ms: i64,
    pub targets: Targets,
}
#[derive(Clone)]
struct Desired {
    signature: String,
    positions: BTreeMap<String, StrategyPosition>,
    trigger_us: i64,
    trading_day: Option<u32>,
    live_ready: bool,
}

#[derive(Default, Serialize)]
struct Status {
    strategy_group: String,
    source_id: String,
    pending_batch: Option<String>,
    acknowledged_batch: Option<String>,
    error: Option<String>,
}

pub struct Publisher {
    positions: BTreeMap<String, StrategyPosition>,
    latest: Option<Desired>,
    tx: watch::Sender<Option<Desired>>,
    state: Arc<Mutex<Status>>,
    task: JoinHandle<()>,
}
impl Drop for Publisher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub fn positions(engine: &Engine, extras: &Extras) -> Result<BTreeMap<String, StrategyPosition>> {
    let mut positions = engine.publication_positions()?;
    for (name, position) in extras.publication_positions()? {
        ensure!(
            positions.insert(name, position).is_none(),
            "duplicate native publication strategy identity"
        );
    }
    Ok(positions)
}
fn signature(positions: &BTreeMap<String, StrategyPosition>) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(positions)?)))
}
impl Publisher {
    pub fn start(
        cfg: &Config,
        positions: BTreeMap<String, StrategyPosition>,
    ) -> Result<Option<Self>> {
        let Some(settings) = cfg
            .publication_settings()
            .filter(|p| p.enabled_for(&cfg.symbol))
        else {
            return Ok(None);
        };
        let group = cfg.strategy_group()?;
        let client = client::Client::new(
            settings,
            group.clone(),
            cfg.state_directory().join("manager_publication.json"),
        )?;
        if !cfg.additional_products.is_empty() || cfg.is_additional_product() {
            client.check_pending_members(positions.keys())?;
        }
        let state = Arc::new(Mutex::new(Status {
            strategy_group: group,
            source_id: settings.source_id.clone(),
            ..Status::default()
        }));
        let (tx, rx) = watch::channel(None);
        let task = tokio::spawn(client.run(rx, state.clone()));
        Ok(Some(Self {
            positions,
            latest: None,
            tx,
            state,
            task,
        }))
    }
    pub fn observe(
        &mut self,
        positions: BTreeMap<String, StrategyPosition>,
        live_decision: Option<(i64, u32)>,
        active_trading_day: u32,
        live_ready: bool,
    ) -> Result<()> {
        let changed = self.positions != positions;
        if changed {
            if let Some((trigger_ms, trading_day)) = live_decision {
                let trigger_us = trigger_ms
                    .checked_mul(1000)
                    .context("native decision time overflow")?;
                let signature = signature(&positions)?;
                if let Some(latest) = self.latest.as_mut().filter(|latest| {
                    latest.trading_day == Some(trading_day) && latest.trigger_us > trigger_us
                }) {
                    latest.signature = signature;
                    latest.positions.clone_from(&positions);
                } else {
                    self.latest = Some(Desired {
                        signature,
                        positions: positions.clone(),
                        trigger_us,
                        trading_day: Some(trading_day),
                        live_ready,
                    });
                }
            }
            self.positions = positions;
        }
        if let Some(latest) = &mut self.latest {
            latest.live_ready = live_ready && latest.trading_day == Some(active_trading_day);
            self.tx.send_replace(Some(latest.clone()));
        }
        Ok(())
    }
    pub fn status(&self) -> Value {
        json!(*self.state.lock().unwrap_or_else(|p| p.into_inner()))
    }
}
