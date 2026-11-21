//! Domestic Exec persistor, like the crypto uniform-order and trade-update
//! persistors: Exec publishes final records (`cn_futures::persist`); this side
//! appends the two stored kinds unchanged and prints operator records as the
//! trader log. Exec never waits for any of it.
use std::io::Write;
use std::sync::Arc;

use anyhow::Result;
use cn_futures::persist::{ipc::Subscriber, materialize};

use crate::cn_futures::NativeLog;
use crate::polling::{PollStats, MAX_DRAIN_PER_CHANNEL};
use crate::RocksDbStore;

pub(crate) struct CnNativePersistor {
    subscriber: Subscriber,
    log: NativeLog,
}

impl CnNativePersistor {
    pub(crate) fn new(store: Arc<RocksDbStore>) -> Result<Self> {
        Ok(Self {
            subscriber: Subscriber::new()?,
            log: NativeLog::attach(store)?,
        })
    }

    pub(crate) fn poll_available(&mut self) -> PollStats {
        let mut stats = PollStats::default();
        let mut writes = Vec::new();
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        for _ in 0..MAX_DRAIN_PER_CHANNEL {
            let log = &mut self.log;
            let received = self.subscriber.receive(|record| {
                let result = (|| -> Result<()> {
                    let row = materialize(record)?;
                    if let Some(line) = &row.display {
                        let _ = writeln!(out, "{line}");
                    }
                    if let Some((cf, bytes)) = row.store {
                        writes.push((
                            cf.to_string(),
                            log.key(cf, row.received_us)?,
                            bytes.to_vec(),
                        ));
                    }
                    Ok(())
                })();
                if let Err(error) = result {
                    log::error!("domestic record rejected: {error:#}");
                }
            });
            match received {
                Ok(true) => stats.record_received(),
                Ok(false) => break,
                Err(error) => {
                    log::warn!("domestic record receive failed: {error:#}");
                    stats.record_error();
                    break;
                }
            }
        }
        let _ = out.flush();
        if !writes.is_empty() {
            if let Err(error) = self.log.append(&writes) {
                log::error!("domestic records not persisted: {error:#}");
                stats.record_error();
            }
        }
        stats
    }
}
