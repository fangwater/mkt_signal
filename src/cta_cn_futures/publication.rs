//! One complete strategy-group publication, with a durable ambiguous-outcome retry.
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
#[derive(Clone, Serialize, Deserialize)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub observed_us: i64,
    pub source: String,
}
#[derive(Clone)]
struct Desired {
    signature: String,
    positions: BTreeMap<String, StrategyPosition>,
    trigger_us: i64,
    trading_day: Option<u32>,
    live_ready: bool,
    quotes: BTreeMap<String, Quote>,
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
    baseline: String,
    latest: Option<Desired>,
    quotes: BTreeMap<String, Quote>,
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
    pub fn start(cfg: &Config, engine: &Engine, extras: &Extras) -> Result<Option<Self>> {
        let Some(settings) = cfg.publication.as_ref().filter(|p| p.enabled) else {
            return Ok(None);
        };
        let baseline = signature(&positions(engine, extras)?)?;
        let group = cfg.strategy_group()?;
        let client = client::Client::new(
            settings,
            group.clone(),
            cfg.data_directory.join("manager_publication.json"),
        )?;
        let state = Arc::new(Mutex::new(Status {
            strategy_group: group,
            source_id: settings.source_id.clone(),
            ..Status::default()
        }));
        let (tx, rx) = watch::channel(None);
        let task = tokio::spawn(client.run(rx, state.clone()));
        Ok(Some(Self {
            baseline,
            latest: None,
            quotes: BTreeMap::new(),
            tx,
            state,
            task,
        }))
    }
    pub fn reset(&mut self) {
        self.quotes.clear();
    }
    pub fn quote(&mut self, q: &cn_futures_pbs::msg::MarketDataMsg) {
        if q.flags & cn_futures_pbs::msg::SOURCE_RESET != 0 {
            self.quotes.clear();
            return;
        }
        if q.bid[0].price.is_finite()
            && q.ask[0].price.is_finite()
            && q.bid[0].price > 0.0
            && q.ask[0].price >= q.bid[0].price
            && q.exchange_ts_us > 0
        {
            let Ok(symbol) = std::str::from_utf8(q.symbol()) else {
                return;
            };
            let venue = String::from_utf8_lossy(&q.venue)
                .trim_end_matches('\0')
                .to_owned();
            let market = format!("{venue}:{symbol}");
            if self
                .quotes
                .get(&market)
                .is_none_or(|old| old.observed_us <= q.exchange_ts_us)
            {
                self.quotes.insert(
                    market,
                    Quote {
                        bid: q.bid[0].price,
                        ask: q.ask[0].price,
                        observed_us: q.exchange_ts_us,
                        source: "company_zmq".into(),
                    },
                );
            }
        }
    }
    pub fn observe(
        &mut self,
        engine: &Engine,
        extras: &Extras,
        active_trading_day: u32,
        live_ready: bool,
    ) -> Result<()> {
        let positions = positions(engine, extras)?;
        let current = signature(&positions)?;
        if current != self.baseline {
            // Only a factual new decision can replace the startup baseline;
            // historical formula warm-up never publishes old targets.
            let trigger_ms = positions.values().map(|p| p.decision_ms).max().unwrap_or(0);
            let trigger_us = trigger_ms
                .checked_mul(1000)
                .context("native decision time overflow")?;
            let trading_day = engine
                .last_decision_day
                .filter(|_| engine.ledger.last_decision_ms == trigger_ms)
                .or_else(|| {
                    extras
                        .last_live_decision()
                        .filter(|&(at, _)| at == trigger_ms)
                        .map(|(_, day)| day)
                });
            self.latest = Some(Desired {
                signature: current.clone(),
                positions,
                trigger_us,
                trading_day,
                live_ready,
                quotes: self.quotes.clone(),
            });
            self.baseline = current;
        }
        if let Some(latest) = &mut self.latest {
            latest.live_ready = live_ready
                && latest
                    .trading_day
                    .is_none_or(|day| day == active_trading_day);
            latest.quotes = self.quotes.clone();
            self.tx.send_replace(Some(latest.clone()));
        }
        Ok(())
    }
    pub fn status(&self) -> Value {
        json!(*self.state.lock().unwrap_or_else(|p| p.into_inner()))
    }
}
