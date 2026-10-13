//! Keep env_logger filtering/formatting and receive-time timestamps, but never
//! write to the filesystem on the BBO runtime thread. At most 64 MiB is queued.
use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const QUEUE_RECORDS: usize = 1024;
const MAX_RECORD_BYTES: usize = 64 * 1024;

struct QueueWriter {
    sender: Sender<Vec<u8>>,
    dropped: Arc<AtomicU64>,
}

impl Write for QueueWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_RECORD_BYTES || self.sender.try_send(bytes.to_vec()).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        // A diagnostic drop must not trigger a synchronous fallback or retry.
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn drain(receiver: Receiver<Vec<u8>>, dropped: Arc<AtomicU64>, mut output: impl Write) {
    let mut reported_drops = 0;
    let mut write_errors = 0u64;
    let mut reported_errors = 0;
    loop {
        match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(bytes) => {
                if output
                    .write_all(&bytes)
                    .and_then(|_| output.flush())
                    .is_err()
                {
                    write_errors = write_errors.saturating_add(1);
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
        let total = dropped.load(Ordering::Relaxed);
        if total != reported_drops || write_errors != reported_errors {
            // Never recurse through log!: this warning also survives a full queue.
            let warning = format!(
                "spread_async_log_loss dropped_total={total} write_errors_total={write_errors}; diagnostic coverage incomplete\n"
            );
            if output
                .write_all(warning.as_bytes())
                .and_then(|_| output.flush())
                .is_ok()
            {
                reported_drops = total;
                reported_errors = write_errors;
            }
        }
    }
}

pub fn init(core: usize) -> Result<()> {
    anyhow::ensure!(core < libc::CPU_SETSIZE as usize, "invalid log writer CPU");
    let (sender, receiver) = bounded(QUEUE_RECORDS);
    let dropped = Arc::new(AtomicU64::new(0));
    let worker_dropped = dropped.clone();
    let (ready_tx, ready_rx) = bounded(1);
    std::thread::Builder::new()
        .name("bbo-log-writer".into())
        .spawn(move || {
            // taskset/parent affinity is inherited: explicitly move this thread.
            let ready = runtime_common::affinity::pin_to_core(core);
            let ok = ready.is_ok();
            if ready_tx.send(ready).is_err() || !ok {
                return;
            }
            drain(receiver, worker_dropped, io::stderr());
        })
        .context("spawn BBO log writer")?;
    ready_rx
        .recv_timeout(Duration::from_secs(5))
        .context("initialize BBO log writer")??;
    env_logger::Builder::from_default_env()
        .target(env_logger::Target::Pipe(Box::new(QueueWriter {
            sender,
            dropped,
        })))
        .try_init()
        .context("install BBO async logger")?;
    log::info!("BBO async log writer core={core} queue_records={QUEUE_RECORDS} max_record_bytes={MAX_RECORD_BYTES}");
    // Deliberately detached: shutdown must never join a disk-blocked writer.
    // SIGKILL/process exit can lose the bounded tail of diagnostic records.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::Log;
    use std::sync::Mutex;

    #[test]
    fn write_failure_is_counted_and_later_records_survive() {
        struct FailOnce {
            failed: bool,
            bytes: Vec<u8>,
        }
        impl Write for FailOnce {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if !self.failed {
                    self.failed = true;
                    return Err(io::Error::other("simulated disk failure"));
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (sender, receiver) = bounded(2);
        sender.send(b"lost\n".to_vec()).unwrap();
        sender.send(b"survived\n".to_vec()).unwrap();
        drop(sender);
        let mut sink = FailOnce {
            failed: false,
            bytes: Vec::new(),
        };
        drain(receiver, Arc::new(AtomicU64::new(0)), &mut sink);
        let text = String::from_utf8(sink.bytes).unwrap();
        assert!(text.contains("dropped_total=0 write_errors_total=1"));
        assert!(text.ends_with("survived\n"));
    }

    #[test]
    fn stalled_output_cannot_block_producer_and_loss_is_reported_on_recovery() {
        struct Blocked {
            entered: Sender<()>,
            release: Receiver<()>,
            output: Arc<Mutex<Vec<u8>>>,
            first: bool,
        }
        impl Write for Blocked {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.first {
                    self.first = false;
                    self.entered.send(()).unwrap();
                    self.release.recv().unwrap();
                }
                self.output.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (sender, receiver) = bounded(2);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut writer = QueueWriter {
            sender,
            dropped: dropped.clone(),
        };
        let (entered_tx, entered_rx) = bounded(1);
        let (release_tx, release_rx) = bounded(1);
        let output = Arc::new(Mutex::new(Vec::new()));
        let sink = Blocked {
            entered: entered_tx,
            release: release_rx,
            output: output.clone(),
            first: true,
        };
        let counts = dropped.clone();
        let worker = std::thread::spawn(move || drain(receiver, counts, sink));
        writer.write_all(b"first\n").unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (done_tx, done_rx) = bounded(1);
        let producer = std::thread::spawn(move || {
            writer.write_all(b"second\n").unwrap();
            writer.write_all(b"third\n").unwrap();
            for _ in 0..100 {
                writer.write_all(b"dropped\n").unwrap();
            }
            writer.write_all(&vec![0; MAX_RECORD_BYTES + 1]).unwrap();
            writer.flush().unwrap();
            done_tx.send(()).unwrap();
        });
        let completed = done_rx.recv_timeout(Duration::from_secs(2));
        release_tx.send(()).unwrap(); // Release even on failure; no hanging test.
        producer.join().unwrap();
        worker.join().unwrap();
        assert!(completed.is_ok());
        assert_eq!(dropped.load(Ordering::Relaxed), 101);
        let text = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(text.starts_with("first\n"));
        assert!(text.contains("dropped_total=101 write_errors_total=0"));
        assert!(text.ends_with("second\nthird\n"));
    }

    #[test]
    fn preserves_env_logger_format_filter_and_complete_record() {
        let (sender, receiver) = bounded(2);
        let logger = env_logger::Builder::new()
            .parse_filters("warn,mkt_signal::spread_pbs::binance_latency=info")
            .target(env_logger::Target::Pipe(Box::new(QueueWriter {
                sender,
                dropped: Arc::new(AtomicU64::new(0)),
            })))
            .build();
        logger.log(
            &log::Record::builder()
                .level(log::Level::Info)
                .target("other")
                .args(format_args!("hidden"))
                .build(),
        );
        logger.log(
            &log::Record::builder()
                .level(log::Level::Info)
                .target("mkt_signal::spread_pbs::binance_latency")
                .args(format_args!("binance_feed_latency {{\"count\":1}}"))
                .build(),
        );
        let line = String::from_utf8(receiver.try_recv().unwrap()).unwrap();
        assert!(line.starts_with('['));
        assert!(line.contains(
            "Z INFO  mkt_signal::spread_pbs::binance_latency] binance_feed_latency {\"count\":1}\n"
        ));
        assert!(receiver.try_recv().is_err());
    }
}
