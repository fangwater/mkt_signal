//! Inline position snapshots from the business loop, like crypto's in-loop IPC
//! publication: encode into a reused buffer and copy into shared memory. No
//! worker thread, mailbox, mutex or acknowledgement.
use crate::{checkpoint, transport::StatePublisher, Adapter, Config, Snapshot};
use anyhow::{Context, Result};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
pub struct Producer {
    publisher: StatePublisher,
    snapshot: Snapshot,
    next: Instant,
    interval: Duration,
    sequence: u64,
    epoch: u64,
    namespace: String,
    /// Last broker-confirmed state, written once on shutdown when configured.
    checkpoint: Option<(std::path::PathBuf, Option<Snapshot>)>,
}
impl Producer {
    pub fn start(config: &Config, adapter: Adapter, account_hash: &str) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            publisher: StatePublisher::new(&config.namespace)?,
            snapshot: Snapshot::empty(adapter, account_hash)?,
            next: Instant::now(),
            interval: Duration::from_millis(config.interval_ms),
            sequence: 0,
            epoch: now_us(),
            namespace: config.namespace.clone(),
            checkpoint: config
                .account_snapshot_path
                .clone()
                .map(|path| (path, None)),
        })
    }
    /// Captures and publishes when due (or forced). A failed fill never
    /// publishes a partial book: it publishes an explicitly failed, empty state.
    pub fn capture(
        &mut self,
        force: bool,
        fill: impl FnOnce(&mut Snapshot) -> Result<()>,
    ) -> Result<bool> {
        let now = Instant::now();
        if !force && now < self.next {
            return Ok(false);
        }
        self.next = now + self.interval;
        self.sequence = self
            .sequence
            .checked_add(1)
            .context("position IPC sequence exhausted")?;
        let snapshot = &mut self.snapshot;
        snapshot.positions.clear();
        snapshot.marks.clear();
        snapshot.position_costs.observed_us = 0;
        snapshot.position_costs.rows.clear();
        snapshot.publisher_epoch = self.epoch;
        snapshot.sequence = self.sequence;
        snapshot.observed_us = now_us();
        if let Err(error) = fill(snapshot) {
            snapshot.positions.clear();
            snapshot.marks.clear();
            snapshot.position_costs.observed_us = 0;
            snapshot.position_costs.rows.clear();
            snapshot.position_ready = false;
            snapshot.ready = false;
            snapshot.can_trade = false;
            snapshot.runtime_status = crate::RuntimeStatus::Failed;
            snapshot.reason =
                crate::Text::new("position snapshot capture failed; monitor unavailable")?;
            self.publisher.publish(snapshot)?;
            return Err(error);
        }
        self.publisher.publish(snapshot)?;
        if let Some((_, confirmed)) = &mut self.checkpoint {
            if checkpoint::confirmed(snapshot) {
                let account = confirmed
                    .as_ref()
                    .filter(|previous| previous.trading_day == snapshot.trading_day)
                    .and_then(|previous| previous.account);
                let retained = confirmed.get_or_insert_with(|| snapshot.clone());
                retained.clone_from(snapshot);
                if retained.account.is_none() {
                    retained.account = account;
                }
            }
        }
        Ok(true)
    }
    /// Shutdown only: persist the last confirmed account state once.
    pub fn finish(self) -> Result<()> {
        if let Some((path, Some(snapshot))) = self.checkpoint {
            checkpoint::save(&path, &self.namespace, snapshot)?;
        }
        Ok(())
    }
}
