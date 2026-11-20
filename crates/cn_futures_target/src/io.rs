//! Network I/O owned by the business thread, like crypto's single-threaded
//! Tokio reactor: a current-thread runtime whose local tasks the loop drives at
//! a bounded rate. Driving never blocks; no I/O thread exists. Tokio may still
//! start short-lived blocking threads (DNS resolution); `on_thread_start` runs
//! on each of them so the runtime can move them off the business CPU.
use anyhow::{Context, Result};
use std::future::Future;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// Bounded so idle spinning does not issue an epoll call per iteration.
const DRIVE_INTERVAL: Duration = Duration::from_millis(1);
/// Resolver threads only; nothing else of ours uses the blocking pool.
const MAX_BLOCKING_THREADS: usize = 2;

pub struct IoLoop {
    runtime: tokio::runtime::Runtime,
    local: Rc<tokio::task::LocalSet>,
    next: Instant,
}

/// Queues local tasks on the business thread's `IoLoop`.
#[derive(Clone)]
pub struct Spawner(Rc<tokio::task::LocalSet>);
impl Spawner {
    pub fn spawn(&self, task: impl Future<Output = ()> + 'static) {
        self.0.spawn_local(task);
    }
}

impl IoLoop {
    pub fn new(on_thread_start: impl Fn() + Send + Sync + 'static) -> Result<Self> {
        Ok(Self {
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(MAX_BLOCKING_THREADS)
                .on_thread_start(on_thread_start)
                .build()
                .context("build in-loop I/O runtime")?,
            local: Rc::new(tokio::task::LocalSet::new()),
            next: Instant::now(),
        })
    }

    pub fn spawner(&self) -> Spawner {
        Spawner(self.local.clone())
    }

    /// Queues a task; it first runs on a later `drive`.
    pub fn spawn(&self, task: impl Future<Output = ()> + 'static) {
        self.local.spawn_local(task);
    }

    /// Runs ready tasks once and polls the I/O driver without waiting.
    /// Returns whether a drive happened.
    pub fn drive(&mut self, now: Instant) -> bool {
        if now < self.next {
            return false;
        }
        self.next = now + DRIVE_INTERVAL;
        self.local.block_on(&self.runtime, tokio::task::yield_now());
        true
    }
}
