use super::{config::Counter, state::stale};
use anyhow::{ensure, Result};
use cn_futures_state::{transport::StateSubscriber, RuntimeStatus, Snapshot};
use serde_json::{json, Value};
use std::{cell::RefCell, rc::Rc, time::Duration};

pub struct Positions {
    source: Counter,
    latest: Option<Snapshot>,
    error: Option<String>,
}
impl Positions {
    pub fn new(source: Counter) -> Self {
        Self {
            source,
            latest: None,
            error: None,
        }
    }
    pub fn apply(&mut self, snapshot: Snapshot) -> Result<()> {
        snapshot.validate()?;
        ensure!(
            snapshot.adapter == self.source.adapter
                && snapshot.account_hash.as_str() == self.source.expected_account_hash,
            "position IPC account/adapter identity mismatch"
        );
        if let Some(last) = &self.latest {
            if last.publisher_epoch == snapshot.publisher_epoch {
                ensure!(
                    snapshot.sequence >= last.sequence,
                    "position IPC sequence regressed"
                );
                if snapshot.sequence == last.sequence {
                    return Ok(());
                }
            }
        }
        self.latest = Some(snapshot);
        self.error = None;
        Ok(())
    }
    pub fn unavailable(&mut self, error: &str) {
        self.error = Some(error.into());
    }
    pub fn view(&self, now: u64, stale_ms: u64) -> Value {
        let data_stale = stale(now, self.latest.as_ref().map(|s| s.observed_us), stale_ms);
        let status = if self.error.is_some() {
            "unavailable"
        } else if let Some(s) = &self.latest {
            if s.runtime_status == RuntimeStatus::Stopped {
                "stopped"
            } else if s.runtime_status == RuntimeStatus::Failed {
                "failed"
            } else if data_stale {
                "stale"
            } else if !s.position_ready {
                "recovering"
            } else if !s.connected {
                "disconnected"
            } else if !s.ready {
                "blocked"
            } else if !s.transport_ready {
                "transport_unavailable"
            } else {
                "observing"
            }
        } else {
            "waiting"
        };
        let mut snapshot = self
            .latest
            .as_ref()
            .map(|s| serde_json::to_value(s).expect("finite validated position snapshot"));
        if let Some(value) = &mut snapshot {
            value
                .as_object_mut()
                .expect("snapshot object")
                .remove("account_hash");
            if let Some(s) = &self.latest {
                value["sequence"] = json!(s.sequence.to_string());
                value["publisher_epoch"] = json!(s.publisher_epoch.to_string());
            }
        }
        json!({"namespace":self.source.position_namespace,"status":status,"data_stale":data_stale,"error":self.error,"snapshot":snapshot})
    }
}
pub fn spawn(source: Counter) -> (Rc<RefCell<Positions>>, tokio::task::JoinHandle<()>) {
    let state = Rc::new(RefCell::new(Positions::new(source.clone())));
    let listener = state.clone();
    let handle = tokio::task::spawn_local(async move {
        let mut subscriber = None;
        let mut retry = tokio::time::Instant::now();
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if subscriber.is_none() && tokio::time::Instant::now() >= retry {
                match StateSubscriber::open(&source.position_namespace) {
                    Ok(s) => subscriber = Some(s),
                    Err(_) => listener
                        .borrow_mut()
                        .unavailable("waiting for position IPC"),
                }
                retry = tokio::time::Instant::now() + Duration::from_secs(1);
            }
            if let Some(s) = &subscriber {
                let result = (|| -> Result<()> {
                    if let Some(snapshot) = s.latest()? {
                        listener.borrow_mut().apply(snapshot)?;
                    }
                    Ok(())
                })();
                if let Err(error) = result {
                    log::warn!("native positions {}: {error:#}", source.id);
                    listener
                        .borrow_mut()
                        .unavailable("invalid position IPC; check namespace and account binding");
                    subscriber = None;
                }
            }
        }
    });
    (state, handle)
}
