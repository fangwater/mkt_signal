use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub const SIGNAL_JOURNAL_DIR: &str = "data/cta_perp/signal_events";

#[derive(Debug, Clone, Default, Serialize)]
pub struct GridPublication {
    pub levels: Vec<PublishedLevel>,
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublishedLevel {
    pub level_index: usize,
    pub price: f64,
    pub qty: f64,
    pub notional: f64,
    pub offset: f64,
    pub published_ts_us: i64,
}

#[derive(Debug, Serialize)]
struct JournalRecord {
    event_id: String,
    signal_id: String,
    symbol: String,
    model_ts_ms: i64,
    event_ts_us: i64,
    state: &'static str,
    reason: &'static str,
    snapshot: Value,
    publication: GridPublication,
}

/// A dedicated writer keeps filesystem I/O off the signal reactor. Published
/// grids sync to disk; every record is one complete, immutable JSON line.
pub struct SignalJournal {
    sender: Option<mpsc::Sender<JournalRecord>>,
    worker: Option<JoinHandle<()>>,
    healthy: Arc<AtomicBool>,
    stopping: Arc<AtomicBool>,
    instance: String,
    sequence: u64,
}

fn journal_path(root: &Path, ts_us: i64) -> Result<PathBuf> {
    let date = DateTime::<Utc>::from_timestamp_micros(ts_us)
        .context("invalid CTA journal event timestamp")?
        .format("%Y%m%d");
    Ok(root.join(format!("{date}.jsonl")))
}

fn open_journal(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open CTA signal journal {}", path.display()))
}

impl SignalJournal {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).context("create CTA signal journal directory")?;
        let initial = journal_path(&root, Utc::now().timestamp_micros())?;
        open_journal(&initial)?;
        let (sender, receiver) = mpsc::channel::<JournalRecord>();
        let healthy = Arc::new(AtomicBool::new(true));
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_healthy = healthy.clone();
        let worker_stopping = stopping.clone();
        let worker = thread::Builder::new()
            .name("cta-signal-journal".into())
            .spawn(move || {
                let mut current: Option<(PathBuf, File)> = None;
                for record in receiver {
                    let result = (|| -> Result<(PathBuf, Vec<u8>)> {
                        let path = journal_path(&root, record.event_ts_us)?;
                        let mut bytes = serde_json::to_vec(&record)?;
                        bytes.push(b'\n');
                        Ok((path, bytes))
                    })();
                    let (path, bytes) = match result {
                        Ok(value) => value,
                        Err(err) => {
                            worker_healthy.store(false, Ordering::Relaxed);
                            log::error!("CTA signal journal record rejected: {err:#}");
                            continue;
                        }
                    };
                    loop {
                        let result = (|| -> Result<()> {
                            if current
                                .as_ref()
                                .is_none_or(|(previous, _)| previous != &path)
                            {
                                current = Some((path.clone(), open_journal(&path)?));
                            }
                            let file = &mut current.as_mut().unwrap().1;
                            // Restore the previous length after a partial write so retries do
                            // not leave a corrupt prefix before the next complete JSON line.
                            let previous_len = file.metadata()?.len();
                            if let Err(err) = file.write_all(&bytes) {
                                let _ = file.set_len(previous_len);
                                return Err(err.into());
                            }
                            if !record.publication.levels.is_empty()
                                || record.publication.target.is_some()
                            {
                                file.sync_data()?;
                            }
                            Ok(())
                        })();
                        match result {
                            Ok(()) => {
                                worker_healthy.store(true, Ordering::Relaxed);
                                break;
                            }
                            Err(err) => {
                                worker_healthy.store(false, Ordering::Relaxed);
                                log::error!("CTA signal journal write failed: {err:#}");
                                if worker_stopping.load(Ordering::Relaxed) {
                                    return;
                                }
                                thread::sleep(Duration::from_secs(1));
                            }
                        }
                    }
                }
                if let Some((_, file)) = current {
                    let _ = file.sync_data();
                }
            })
            .context("start CTA signal journal writer")?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
            healthy,
            stopping,
            instance: uuid::Uuid::new_v4().to_string(),
            sequence: 0,
        })
    }

    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    pub fn record(
        &mut self,
        symbol: &str,
        model_ts_ms: i64,
        event_ts_us: i64,
        state: &'static str,
        reason: &'static str,
        snapshot: Value,
        publication: GridPublication,
    ) {
        self.sequence += 1;
        let record = JournalRecord {
            event_id: format!("{}:{}", self.instance, self.sequence),
            signal_id: format!(
                "{}:{}:{symbol}:{model_ts_ms}",
                self.instance,
                snapshot
                    .get("strategy_name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
            ),
            symbol: symbol.into(),
            model_ts_ms,
            event_ts_us,
            state,
            reason,
            snapshot,
            publication,
        };
        if self
            .sender
            .as_ref()
            .is_none_or(|sender| sender.send(record).is_err())
        {
            self.healthy.store(false, Ordering::Relaxed);
            log::error!("CTA signal journal writer is unavailable");
        }
    }
}

impl Drop for SignalJournal {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Relaxed);
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn journal_preserves_snapshots_publication_and_utc_rotation() {
        let root = std::env::temp_dir().join(format!("cta-journal-test-{}", uuid::Uuid::new_v4()));
        let mut journal = SignalJournal::new(&root).unwrap();
        let ts = 1_728_086_399_000_000;
        let original = serde_json::json!({"factor_votes":[{"factor":"factor_036","weight":2,"sign":"-","score":1.3}], "coefficient":0.333});
        journal.record(
            "ETHUSDT",
            ts / 1000,
            ts,
            "scheduled",
            "allowed",
            original.clone(),
            GridPublication::default(),
        );
        journal.record(
            "ETHUSDT",
            ts / 1000,
            ts + 2_000_000,
            "published",
            "allowed",
            original.clone(),
            GridPublication {
                target: None,
                levels: vec![PublishedLevel {
                    level_index: 1,
                    price: 100.0,
                    qty: 0.333,
                    notional: 33.3,
                    offset: 0.0,
                    published_ts_us: ts + 2_000_000,
                }],
                error: None,
            },
        );
        drop(journal);
        let first: Value = serde_json::from_str(
            fs::read_to_string(journal_path(&root, ts).unwrap())
                .unwrap()
                .trim(),
        )
        .unwrap();
        let second: Value = serde_json::from_str(
            fs::read_to_string(journal_path(&root, ts + 2_000_000).unwrap())
                .unwrap()
                .trim(),
        )
        .unwrap();
        assert_ne!(first["event_id"], second["event_id"]);
        assert_eq!(first["signal_id"], second["signal_id"]);
        assert_eq!(second["snapshot"], original);
        assert_eq!(second["publication"]["levels"][0]["notional"], 33.3);
        fs::remove_dir_all(root).unwrap();
    }
}
