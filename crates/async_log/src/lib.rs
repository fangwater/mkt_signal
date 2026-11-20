//! Asynchronous `log` backend. The calling thread formats each record with
//! `env_logger` (same `RUST_LOG` filters and layout) and hands the bytes to a
//! bounded queue with a non-blocking `try_send`; one logger thread writes
//! stderr. A full queue drops and counts records instead of blocking the caller.
//! Trading runtimes move the logger thread off their business CPU through
//! [`thread_id`].
use std::io::Write;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;

/// Records buffered before the caller starts dropping.
const CAPACITY: usize = 65_536;

struct QueueWriter {
    sender: SyncSender<Vec<u8>>,
    dropped: Arc<AtomicU64>,
}
impl Write for QueueWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Err(TrySendError::Full(_)) = self.sender.try_send(bytes.to_vec()) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Keep alive for the process lifetime; dropping it drains the queue and joins
/// the logger thread, reporting how many records were dropped.
pub struct LogGuard {
    end: SyncSender<Vec<u8>>,
    worker: Option<JoinHandle<()>>,
}

static THREAD_ID: AtomicI32 = AtomicI32::new(0);
/// Kernel thread id of the logger thread; 0 before `init` or where
/// unsupported. Trading runtimes use it to place the thread.
pub fn thread_id() -> i32 {
    THREAD_ID.load(Ordering::Acquire)
}
fn current_thread_id() -> i32 {
    #[cfg(target_os = "linux")]
    {
        unsafe { libc::syscall(libc::SYS_gettid) as i32 }
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}
impl Drop for LogGuard {
    fn drop(&mut self) {
        // Shutdown only: an empty record ends the writer after every queued one.
        let _ = self.end.send(Vec::new());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn drain(receiver: Receiver<Vec<u8>>, dropped: Arc<AtomicU64>) {
    let stderr = std::io::stderr();
    while let Ok(record) = receiver.recv() {
        if record.is_empty() {
            break;
        }
        let mut out = stderr.lock();
        let _ = out.write_all(&record);
        // Write everything already queued in one locked burst.
        while let Ok(record) = receiver.try_recv() {
            if record.is_empty() {
                let _ = out.flush();
                return report(&dropped);
            }
            let _ = out.write_all(&record);
        }
        let _ = out.flush();
    }
    report(&dropped);
}

fn report(dropped: &AtomicU64) {
    let dropped = dropped.load(Ordering::Relaxed);
    if dropped > 0 {
        let _ = writeln!(std::io::stderr(), "async_log: dropped {dropped} records");
    }
}

/// Installs `builder` as the global logger with an asynchronous stderr writer.
pub fn init(mut builder: env_logger::Builder) -> std::io::Result<LogGuard> {
    let (sender, receiver) = sync_channel(CAPACITY);
    let dropped = Arc::new(AtomicU64::new(0));
    let (started, thread_id) = sync_channel(1);
    let worker = std::thread::Builder::new()
        .name("async-log".into())
        .spawn({
            let dropped = dropped.clone();
            move || {
                let _ = started.send(current_thread_id());
                drain(receiver, dropped)
            }
        })?;
    let thread_id = thread_id
        .recv()
        .map_err(|_| std::io::Error::other("async-log thread exited at start"))?;
    THREAD_ID.store(thread_id, Ordering::Release);
    builder
        .target(env_logger::Target::Pipe(Box::new(QueueWriter {
            sender: sender.clone(),
            dropped,
        })))
        .init();
    Ok(LogGuard {
        end: sender,
        worker: Some(worker),
    })
}

/// `RUST_LOG` when set, otherwise `default_filter`.
pub fn init_default(default_filter: &str) -> std::io::Result<LogGuard> {
    init(env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(default_filter),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_queue_drops_instead_of_blocking_and_counts() {
        let (sender, receiver) = sync_channel(1);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut writer = QueueWriter {
            sender,
            dropped: dropped.clone(),
        };
        assert_eq!(writer.write(b"one\n").unwrap(), 4);
        assert_eq!(writer.write(b"two\n").unwrap(), 4);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert_eq!(receiver.try_recv().unwrap(), b"one\n");
    }
}
