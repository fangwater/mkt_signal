//! Cold-created three-buffer mailbox. Capture never waits for serialization,
//! IPC subscribers, a mutex, a database or a worker acknowledgment.
use crate::{transport::StatePublisher, Adapter, Config, RuntimeStatus, Snapshot, Text};
use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::{
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
pub struct Producer {
    free: Receiver<Snapshot>,
    ready: Sender<Snapshot>,
    pending: Receiver<Snapshot>,
    worker: Option<JoinHandle<Result<()>>>,
    next: Instant,
    interval: Duration,
    sequence: u64,
    epoch: u64,
    replaced: u64,
}
impl Producer {
    /// Call during cold initialization on the configured background CPU domain;
    /// the worker inherits that affinity and NUMA policy.
    pub fn start(config: &Config, adapter: Adapter, account_hash: &str) -> Result<Self> {
        config.validate()?;
        let (free_tx, free) = bounded(3);
        for _ in 0..3 {
            free_tx
                .send(Snapshot::empty(adapter, account_hash)?)
                .map_err(|_| anyhow::anyhow!("initialize position pool"))?;
        }
        let (ready, pending) = bounded::<Snapshot>(1);
        let (init_tx, init_rx) = std::sync::mpsc::sync_channel(1);
        let namespace = config.namespace.clone();
        let worker_rx = pending.clone();
        let worker = std::thread::spawn(move || -> Result<()> {
            let publisher = match StatePublisher::new(&namespace) {
                Ok(p) => {
                    let _ = init_tx.send(Ok(()));
                    p
                }
                Err(e) => {
                    let _ = init_tx.send(Err(format!("{e:#}")));
                    return Err(e);
                }
            };
            while let Ok(snapshot) = worker_rx.recv() {
                publisher.publish(&snapshot)?;
                if free_tx.send(snapshot).is_err() {
                    break;
                }
            }
            Ok(())
        });
        init_rx
            .recv()
            .context("start position IPC worker")?
            .map_err(anyhow::Error::msg)?;
        Ok(Self {
            free,
            ready,
            pending,
            worker: Some(worker),
            next: Instant::now(),
            interval: Duration::from_millis(config.interval_ms),
            sequence: 0,
            epoch: now_us(),
            replaced: 0,
        })
    }
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
        let mut snapshot = match self.pending.try_recv() {
            Ok(buffer) => {
                self.replaced = self.replaced.saturating_add(1);
                buffer
            }
            Err(_) => match self.free.try_recv() {
                Ok(buffer) => buffer,
                Err(_) => anyhow::bail!("position IPC worker unavailable"),
            },
        };
        snapshot.positions.clear();
        self.sequence = self
            .sequence
            .checked_add(1)
            .context("position IPC sequence exhausted")?;
        snapshot.publisher_epoch = self.epoch;
        snapshot.sequence = self.sequence;
        snapshot.observed_us = now_us();
        snapshot.replaced_snapshots = self.replaced;
        if let Err(error) = fill(&mut snapshot) {
            // Invalidate the entire snapshot rather than publishing a partial book.
            snapshot.positions.clear();
            snapshot.position_ready = false;
            snapshot.ready = false;
            snapshot.can_trade = false;
            snapshot.runtime_status = RuntimeStatus::Failed;
            snapshot.reason = Text::new("position snapshot capture failed; monitor unavailable")?;
            self.ready
                .try_send(snapshot)
                .map_err(|_| anyhow::anyhow!("position IPC worker unavailable"))?;
            return Err(error);
        }
        self.ready
            .try_send(snapshot)
            .map_err(|_| anyhow::anyhow!("position IPC worker unavailable"))?;
        Ok(true)
    }
    /// Only after the business loop has ended. Drain the latest terminal state.
    pub fn finish(mut self) -> Result<()> {
        drop(self.ready);
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("position IPC worker panicked"))??;
        }
        Ok(())
    }
}
