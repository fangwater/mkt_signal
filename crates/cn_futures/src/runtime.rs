//! Operational I/O stays off the business thread. No automatic order cancellation,
//! cross-account routing, or strategy attribution is performed on startup/recovery.
use crate::compact::{CancelAudit, FillAudit, OrderAudit, SubmitAudit};
use crate::model::{Intent, OrderKey};
use anyhow::{Context, Result};
#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk"))]
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use persist_common::cn_futures::{NativeMutation, NativeTradeRecord, StoreIdentity};
use persist_manager::cn_futures::NativeStore;
use serde::Deserialize;
use serde_json::Value;
use smallvec::SmallVec;
use std::cell::Cell;
use std::fs::{File, OpenOptions};
#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk"))]
use std::io::BufRead;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Submit {
        intent: Intent,
    },
    Cancel {
        key: OrderKey,
        #[serde(default)]
        execution_route: Option<crate::model::ExecutionRoute>,
    },
    SubmitBatch {
        intents: Vec<Intent>,
    },
    CancelBatch {
        keys: Vec<OrderKey>,
        #[serde(default)]
        execution_route: Option<crate::model::ExecutionRoute>,
    },
    Connections {},
    State {},
    Reconcile {},
    Shutdown {},
}

#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk"))]
pub(crate) fn command_order_keys(command: &Command) -> Vec<OrderKey> {
    match command {
        Command::Cancel { key, .. } => vec![key.clone()],
        Command::CancelBatch { keys, .. } => keys.clone(),
        _ => vec![],
    }
}

struct Record {
    payload: AuditPayload,
    display: bool,
    mutation: NativeMutation,
    ack: Option<Sender<std::result::Result<(), String>>>,
}
// Native messages occupy preallocated queue storage. Building JSON and owned
// persistence records stays on the writer; no Box or String is allocated here.
#[allow(clippy::large_enum_variant)] // Intentional preallocated inline audit storage.
enum AuditPayload {
    Json(Value),
    Yida {
        event: crate::yida::RawEvent,
        received_us: u64,
        shutdown_tail: bool,
    },
    Ctp {
        event: crate::ctp::ingress::Event,
        received_us: u64,
        shutdown_tail: bool,
    },
    PreparedSubmit(SubmitAudit),
    PreparedCancel(CancelAudit),
    StrategyResult(crate::ctp::strategy::ActionOutcome),
    OrderProjection(SmallVec<[OrderAudit; 1]>),
    NormalizedTrade,
    ProjectedTrade {
        fill: FillAudit,
        order: Option<OrderAudit>,
        commission: Option<f64>,
        native_time: Option<i32>,
        received_us: u64,
    },
}
impl AuditPayload {
    fn materialize(self, mut mutation: NativeMutation) -> (Value, NativeMutation) {
        let value = match self {
            Self::Json(value) => value,
            Self::Yida {
                event,
                received_us,
                shutdown_tail,
            } => {
                let mut value = serde_json::json!({"type":"yida_event","received_us":received_us,"event":event});
                if shutdown_tail {
                    value["shutdown_tail"] = Value::Bool(true);
                }
                value
            }
            Self::Ctp {
                event,
                received_us,
                shutdown_tail,
            } => {
                let mut value =
                    serde_json::json!({"type":"ctp_event","event":event.audit(received_us)});
                if shutdown_tail {
                    value["shutdown_tail"] = Value::Bool(true);
                }
                value
            }
            Self::PreparedSubmit(audit) => {
                let order = audit.owned();
                mutation = crate::persistence::prepared_at(audit.day(), &order, audit.received_us);
                serde_json::json!({"type":"prepared_submit","key":order.key,"intent":order.intent,"limit_price":order.limit_price})
            }
            Self::PreparedCancel(audit) => {
                let order = audit.owned();
                serde_json::json!({"type":"prepared_cancel","key":order.key,"venue":order.venue,"symbol":order.symbol,"system_id":order.system_id,"execution_route":order.execution_route})
            }
            Self::StrategyResult(result) => match result {
                crate::ctp::strategy::ActionOutcome::Submitted { tag, receipt } => {
                    serde_json::json!({"type":"strategy_submit_receipt","tag":tag,"receipt":receipt})
                }
                crate::ctp::strategy::ActionOutcome::CancelledLocally { tag, key } => {
                    serde_json::json!({"type":"strategy_cancel_submitted_locally","tag":tag,"key":key})
                }
                crate::ctp::strategy::ActionOutcome::Rejected { tag, error } => {
                    serde_json::json!({"type":"strategy_action_rejected","tag":tag,"error":error})
                }
            },
            Self::OrderProjection(orders) => {
                for order in orders {
                    order.append(&mut mutation);
                }
                serde_json::json!({"type":"order_projection"})
            }
            Self::NormalizedTrade => {
                let trade = mutation
                    .trades
                    .last_mut()
                    .expect("normalized trade payload has one factual trade");
                trade.commission_currency = trade.commission.map(|_| "CNY".into());
                serde_json::json!({"type":"normalized_trade","identity":trade.fill.identity})
            }
            Self::ProjectedTrade {
                fill,
                order,
                commission,
                native_time,
                received_us,
            } => {
                if let Some(order) = order {
                    order.append(&mut mutation);
                }
                let fill = fill.owned();
                let value = serde_json::json!({"type":"normalized_trade","identity":fill.identity});
                mutation.trades.push(NativeTradeRecord {
                    fill,
                    commission,
                    commission_currency: commission.map(|_| "CNY".into()),
                    native_time,
                    received_us,
                    evidence_seq: 0,
                });
                value
            }
        };
        (value, mutation)
    }
}
pub(crate) fn redact(value: &mut Value, secrets: &[String]) {
    match value {
        Value::String(text) => {
            for secret in secrets {
                if !secret.is_empty() {
                    *text = text.replace(secret, "[redacted]");
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                redact(value, secrets);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                redact(value, secrets);
            }
        }
        _ => {}
    }
}
#[derive(Default)]
struct WriterStats {
    records: AtomicU64,
    confirmed: AtomicU64,
    batches: AtomicU64,
    max_batch: AtomicU64,
}
#[derive(Debug, Clone, Copy)]
pub struct JournalStats {
    pub enqueued: u64,
    pub durable: u64,
    pub synced_batches: u64,
    pub max_batch: u64,
    pub backlog: u64,
}
/// A persistence checkpoint is polled by the event loop; it never waits on a channel.
pub struct DurabilityTicket {
    target: u64,
    deadline: Instant,
}
pub struct Journal {
    sender: Option<Sender<Record>>,
    enqueued: Cell<u64>, // Enforces the single business-thread producer contract.
    stats: Arc<WriterStats>,
    pub failed: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
    sync_before_send: bool,
    reference_floors: std::collections::HashMap<(String, String), u64>,
}
impl Journal {
    pub fn start(
        path: &Path,
        capacity: usize,
        redactions: Vec<String>,
        database: &Path,
        identity: StoreIdentity,
        sync_before_send: bool,
    ) -> Result<Self> {
        let mut store = NativeStore::open(database, &identity)?;
        let reference_floors = store.reference_floors();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("create native journal directory")?;
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path).context("open native trading journal")?;
        let (sender, receiver) = crossbeam_channel::bounded::<Record>(capacity);
        let failed = Arc::new(AtomicBool::new(false));
        let flag = failed.clone();
        let stats = Arc::new(WriterStats::default());
        let writer_stats = stats.clone();
        let worker = std::thread::Builder::new()
            .name("cn-futures-journal".into())
            .spawn(move || {
                let write = || -> Result<()> {
                    let mut writer = BufWriter::new(file);
                    let mut last_sync = Instant::now();
                    const MAX_BATCH: usize = 64;
                    let mut records = Vec::with_capacity(MAX_BATCH);
                    let mut events = Vec::with_capacity(MAX_BATCH);
                    let mut output = Vec::with_capacity(MAX_BATCH);
                    loop {
                        match receiver.recv_timeout(Duration::from_millis(100)) {
                            Ok(first) => {
                                records.push(first);
                                while records.len() < MAX_BATCH {
                                    match receiver.try_recv() {
                                        Ok(record) => records.push(record),
                                        Err(_) => break,
                                    }
                                }
                                for record in records.drain(..) {
                                    let (mut value, mutation) =
                                        record.payload.materialize(record.mutation);
                                    redact(&mut value, &redactions);
                                    let line = serde_json::to_string(&value)?;
                                    events.push((crate::persistence::now_us(), value, mutation));
                                    output.push((record.display, record.ack, line));
                                }
                                let result = store.append_batch(events.drain(..));
                                let (committed, failure) = match result {
                                    Ok(batch) => (batch.committed, batch.projection_error),
                                    Err(error) => (0, Some(error.to_string())),
                                };
                                if committed > 0 {
                                    writer_stats
                                        .records
                                        .fetch_add(committed as u64, Ordering::Release);
                                    writer_stats.batches.fetch_add(1, Ordering::Relaxed);
                                    writer_stats
                                        .max_batch
                                        .fetch_max(committed as u64, Ordering::Relaxed);
                                }
                                let good = if failure.is_some() {
                                    committed.saturating_sub(1)
                                } else {
                                    committed
                                };
                                writer_stats
                                    .confirmed
                                    .fetch_add(good as u64, Ordering::Release);
                                for (index, (display, ack, line)) in output.drain(..).enumerate() {
                                    if let Some(ack) = ack {
                                        let result = if index < good {
                                            Ok(())
                                        } else {
                                            Err(failure.clone().unwrap_or_else(|| {
                                                "native write was not committed".into()
                                            }))
                                        };
                                        let _ = ack.send(result);
                                    }
                                    if index < committed {
                                        writeln!(writer, "{line}")?;
                                    }
                                    if display && index < good {
                                        let stdout = std::io::stdout();
                                        let mut out = stdout.lock();
                                        writeln!(out, "{line}")?;
                                        out.flush()?;
                                    }
                                }
                                if let Some(error) = failure {
                                    anyhow::bail!(error);
                                }
                            }
                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                                writer.flush()?;
                                writer.get_ref().sync_all()?;
                                return Ok(());
                            }
                        }
                        if last_sync.elapsed() >= Duration::from_millis(100) {
                            writer.flush()?;
                            writer.get_ref().sync_data()?;
                            last_sync = Instant::now();
                        }
                    }
                };
                if let Err(error) = write() {
                    flag.store(true, Ordering::Release);
                    let mut error =
                        Value::String(format!("native persistence writer failed: {error:#}"));
                    redact(&mut error, &redactions);
                    eprintln!(
                        "{}",
                        error.as_str().unwrap_or("native persistence writer failed")
                    );
                }
            })
            .context("spawn native journal writer")?;
        Ok(Self {
            sender: Some(sender),
            enqueued: Cell::new(0),
            stats,
            failed,
            worker: Some(worker),
            sync_before_send,
            reference_floors,
        })
    }
    pub fn record(&self, value: Value, display: bool) -> Result<()> {
        self.record_with(value, display, NativeMutation::default(), false)
    }
    pub fn strategy_result(&self, outcome: crate::ctp::strategy::ActionOutcome) -> Result<()> {
        self.enqueue(
            AuditPayload::StrategyResult(outcome),
            false,
            NativeMutation::default(),
            false,
        )
    }
    pub fn record_with(
        &self,
        value: Value,
        display: bool,
        mutation: NativeMutation,
        wait: bool,
    ) -> Result<()> {
        self.enqueue(AuditPayload::Json(value), display, mutation, wait)
    }
    fn enqueue(
        &self,
        payload: AuditPayload,
        display: bool,
        mutation: NativeMutation,
        wait: bool,
    ) -> Result<()> {
        anyhow::ensure!(
            !self.failed.load(Ordering::Acquire),
            "native persistence writer failed"
        );
        let (ack, receiver) = if wait {
            let (s, r) = crossbeam_channel::bounded(1);
            (Some(s), Some(r))
        } else {
            (None, None)
        };
        let result = self
            .sender
            .as_ref()
            .context("journal closed")?
            .try_send(Record {
                payload,
                display,
                mutation,
                ack,
            });
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
            anyhow::bail!("reliable native persistence queue full/disconnected");
        }
        self.enqueued.set(self.enqueued.get().wrapping_add(1));
        if let Some(receiver) = receiver {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                match receiver.recv_timeout(Duration::from_millis(10)) {
                    Ok(Ok(())) => break,
                    Ok(Err(error)) => {
                        self.failed.store(true, Ordering::Release);
                        anyhow::bail!(error);
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout)
                        if Instant::now() < deadline && !self.failed.load(Ordering::Acquire) => {}
                    Err(_) => {
                        self.failed.store(true, Ordering::Release);
                        anyhow::bail!("native persistence acknowledgement timed out/disconnected or writer failed");
                    }
                }
            }
        }
        Ok(())
    }
    pub fn yida_event(
        &self,
        event: &crate::yida::RawEvent,
        received_us: u64,
        shutdown_tail: bool,
    ) -> Result<()> {
        self.enqueue(
            AuditPayload::Yida {
                event: *event,
                received_us,
                shutdown_tail,
            },
            false,
            NativeMutation::default(),
            false,
        )
    }
    pub fn ctp_event(
        &self,
        event: &crate::ctp::ingress::Event,
        received_us: u64,
        shutdown_tail: bool,
    ) -> Result<()> {
        self.enqueue(
            AuditPayload::Ctp {
                event: event.clone(),
                received_us,
                shutdown_tail,
            },
            false,
            NativeMutation::default(),
            false,
        )
    }
    pub fn prepared_submit(&self, day: &str, p: &crate::model::PreparedOrder) -> Result<()> {
        self.enqueue(
            AuditPayload::PreparedSubmit(SubmitAudit::new(day, p, crate::persistence::now_us())?),
            false,
            NativeMutation::default(),
            self.sync_before_send,
        )
    }
    pub fn prepared_cancel(&self, p: &crate::model::PreparedCancel) -> Result<()> {
        self.enqueue(
            AuditPayload::PreparedCancel(CancelAudit::new(p)?),
            false,
            NativeMutation::default(),
            self.sync_before_send,
        )
    }
    pub fn trade(
        &self,
        fill: crate::model::Fill,
        commission: Option<f64>,
        native_time: Option<i32>,
        received_us: u64,
    ) -> Result<()> {
        self.trade_with_projection(
            fill,
            commission,
            native_time,
            received_us,
            NativeMutation::default(),
        )
    }
    pub fn trade_with_projection(
        &self,
        fill: crate::model::Fill,
        commission: Option<f64>,
        native_time: Option<i32>,
        received_us: u64,
        mut mutation: NativeMutation,
    ) -> Result<()> {
        mutation.trades.push(NativeTradeRecord {
            fill,
            commission,
            commission_currency: None,
            native_time,
            received_us,
            evidence_seq: 0,
        });
        self.enqueue(AuditPayload::NormalizedTrade, false, mutation, false)
    }
    pub fn trade_for(
        &self,
        core: &crate::core::TradingCore,
        fill: &crate::model::Fill,
        commission: Option<f64>,
        native_time: Option<i32>,
        received_us: u64,
        applied: bool,
    ) -> Result<()> {
        let order = if applied {
            core.fill_order_key(fill)
                .and_then(|key| core.order(key))
                .map(|order| {
                    OrderAudit::new(
                        core.trading_day(),
                        order,
                        core.instrument_handle(order.request.venue, &order.request.symbol),
                        core.is_ready(),
                        false,
                        crate::persistence::now_us(),
                    )
                })
                .transpose()?
        } else {
            None
        };
        self.enqueue(
            AuditPayload::ProjectedTrade {
                fill: FillAudit::new(fill)?,
                order,
                commission,
                native_time,
                received_us,
            },
            false,
            NativeMutation::default(),
            false,
        )
    }
    pub fn state(&self, core: &crate::core::TradingCore, observed: &[OrderKey]) -> Result<()> {
        self.enqueue(
            AuditPayload::OrderProjection(SmallVec::new()),
            false,
            crate::persistence::projection(core, observed, false),
            false,
        )
    }
    pub fn orders(
        &self,
        core: &crate::core::TradingCore,
        keys: &[OrderKey],
        observed: &[OrderKey],
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut orders = SmallVec::new();
        for key in keys {
            if let Some(order) = core.order(key) {
                orders.push(OrderAudit::new(
                    core.trading_day(),
                    order,
                    core.instrument_handle(order.request.venue, &order.request.symbol),
                    core.is_ready(),
                    observed.contains(key),
                    crate::persistence::now_us(),
                )?);
            }
        }
        self.enqueue(
            AuditPayload::OrderProjection(orders),
            false,
            NativeMutation::default(),
            false,
        )
    }
    pub fn command_state(
        &self,
        core: &crate::core::TradingCore,
        result: &Result<Value>,
        mut keys: Vec<OrderKey>,
    ) -> Result<()> {
        if let Ok(value) = result {
            if let Some(children) = value["receipt"]["children"].as_array() {
                for child in children {
                    let key: OrderKey = serde_json::from_value(child["key"].clone())?;
                    if !keys.contains(&key) {
                        keys.push(key);
                    }
                }
            }
        }
        self.orders(core, &keys, &[])
    }
    pub fn reconciled(
        &self,
        core: &mut crate::core::TradingCore,
        trades: Vec<crate::model::Fill>,
    ) -> Result<()> {
        self.recovery_checkpoint(core, trades, true)
    }
    fn recovery_checkpoint(
        &self,
        core: &mut crate::core::TradingCore,
        trades: Vec<crate::model::Fill>,
        wait: bool,
    ) -> Result<()> {
        core.apply_reference_floors(&self.reference_floors)?;
        let mut mutation = crate::persistence::projection(core, &[], true);
        mutation.trades = trades
            .into_iter()
            .map(|fill| NativeTradeRecord {
                fill,
                commission: None,
                commission_currency: None,
                native_time: None,
                received_us: crate::persistence::now_us(),
                evidence_seq: 0,
            })
            .collect();
        // A new process may trade only after durable unresolved requests have reconciled.
        self.record_with(
            serde_json::json!({"type":if wait { "reconciled" } else { "recovery_checkpoint" },"state":core.view()}),
            wait,
            mutation,
            wait,
        )
    }
    pub fn begin_recovery(
        &self,
        core: &mut crate::core::TradingCore,
        trades: Vec<crate::model::Fill>,
        now: Instant,
    ) -> Result<DurabilityTicket> {
        self.recovery_checkpoint(core, trades, false)?;
        core.recovery_pending(true);
        Ok(DurabilityTicket {
            target: self.enqueued.get(),
            deadline: now + Duration::from_secs(30),
        })
    }
    pub fn poll_checkpoint(&self, ticket: &DurabilityTicket, now: Instant) -> Result<bool> {
        anyhow::ensure!(
            !self.failed.load(Ordering::Acquire),
            "native recovery writer failed"
        );
        if self.stats.confirmed.load(Ordering::Acquire) >= ticket.target {
            anyhow::ensure!(
                !self.failed.load(Ordering::Acquire),
                "native recovery writer failed"
            );
            return Ok(true);
        }
        if now >= ticket.deadline {
            self.failed.store(true, Ordering::Release);
            anyhow::bail!("native recovery checkpoint timed out");
        }
        Ok(false)
    }
    pub fn close_async(&mut self) -> Result<()> {
        self.record(serde_json::json!({"type":"persistence_flushed"}), false)?;
        self.sender.take();
        Ok(())
    }
    pub fn poll_closed(&mut self) -> Result<bool> {
        anyhow::ensure!(
            self.sender.is_none(),
            "close_async must precede poll_closed"
        );
        if self
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            return Ok(false);
        }
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("native persistence worker panicked"))?;
        }
        anyhow::ensure!(
            !self.failed.load(Ordering::Acquire),
            "native persistence writer failed on close"
        );
        Ok(true)
    }
    pub fn stats(&self) -> JournalStats {
        let enqueued = self.enqueued.get();
        let durable = self.stats.records.load(Ordering::Acquire);
        JournalStats {
            enqueued,
            durable,
            synced_batches: self.stats.batches.load(Ordering::Relaxed),
            max_batch: self.stats.max_batch.load(Ordering::Relaxed),
            backlog: enqueued.saturating_sub(durable),
        }
    }
    pub fn finish(mut self) -> Result<()> {
        self.record_with(
            serde_json::json!({"type":"persistence_flushed"}),
            false,
            NativeMutation::default(),
            true,
        )?;
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("native persistence worker panicked"))?;
        }
        anyhow::ensure!(
            !self.failed.load(Ordering::Acquire),
            "native persistence writer failed on close"
        );
        Ok(())
    }
}
impl Drop for Journal {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk"))]
pub(crate) fn input_commands() -> Result<Receiver<Result<Command>>> {
    let (sender, receiver) = crossbeam_channel::bounded(64);
    std::thread::Builder::new()
        .name("cn-futures-commands".into())
        .spawn(move || {
            let stdin = std::io::stdin();
            for line in stdin.lock().lines() {
                let command = match line {
                    Ok(line) if line.len() <= 16384 => serde_json::from_str(&line)
                        .map_err(|_| anyhow::anyhow!("invalid command schema")),
                    Ok(_) => Err(anyhow::anyhow!("command exceeds 16 KiB")),
                    Err(_) => Err(anyhow::anyhow!("command input error")),
                };
                if sender.send(command).is_err() {
                    break;
                }
            }
        })
        .context("spawn native command input")?;
    Ok(receiver)
}

pub fn lock_flow(path: &Path) -> Result<File> {
    std::fs::create_dir_all(path).context("create CN futures flow directory")?;
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(path.join(".cn_futures.lock"))
        .context("open CN futures flow lock")?;
    file.try_lock()
        .context("CN futures flow directory already in use")?;
    Ok(file)
}

#[cfg(feature = "ctp-sdk")]
pub fn run(config: crate::config::Config, execute: bool) -> Result<()> {
    run_with_strategy(
        config,
        None,
        execute,
        crate::ctp::strategy::NoStrategy,
        &AtomicBool::new(false),
    )
}

/// Strategy hooks, feed consumption, risk and order submission share this caller thread.
#[cfg(feature = "ctp-sdk")]
pub fn run_with_strategy<S: crate::ctp::strategy::Strategy>(
    config: crate::config::Config,
    market_data: Option<crate::ctp::market_data::Config>,
    execute: bool,
    mut strategy: S,
    stop: &AtomicBool,
) -> Result<()> {
    use crate::core::{SingleLoop, TradingCore};
    use crate::ctp::strategy::{Actions, StrategyContext};
    use crate::ctp::{self, sdk::CtpTrader, session::Session};
    use serde_json::json;
    config.validate()?;
    if let Some(md) = &market_data {
        config.validate_market_data(md)?;
    }
    let placement = if let Some(md) = &market_data {
        crate::placement::Placement::enter_combined(&config.placement, &md.placement)?
    } else {
        crate::placement::Placement::enter(&config.placement)?
    };
    let _flow_lock = lock_flow(&config.flow_directory)?;
    let credentials = config.credentials()?;
    let redactions = [
        &credentials.password,
        &credentials.auth_code,
        &credentials.app_id,
    ]
    .into_iter()
    .map(|s| s.to_string_lossy().into_owned())
    .filter(|s| !s.is_empty())
    .collect();
    let database = config
        .persistence_directory
        .clone()
        .unwrap_or_else(|| config.flow_directory.join("persist_manager"));
    let identity = crate::persistence::store_identity(
        "ctp",
        &[
            config.front.as_bytes(),
            credentials.broker.as_bytes(),
            credentials.investor.as_bytes(),
        ],
    );
    let mut position_ipc = placement.on_background(|| {
        config
            .position_ipc
            .as_ref()
            .map(|cfg| {
                cn_futures_state::publisher::Producer::start(
                    cfg,
                    cn_futures_state::Adapter::Ctp,
                    &identity.account_hash,
                )
            })
            .transpose()
    })?;
    let mut journal = placement.on_background(|| {
        Journal::start(
            &config.journal,
            config.event_capacity,
            redactions,
            &database,
            identity,
            false, // CTP always queues intent audit without waiting for durability.
        )
    })?;
    let trader = placement.on_sdk(|| CtpTrader::new(&config, credentials, placement.callback()))?;
    let investor = trader
        .investor()
        .to_str()
        .context("invalid InvestorID")?
        .to_owned();
    let mut session = Session::new(config.clone(), investor.clone(), Instant::now());
    let mut core = TradingCore::new(config.risk.clone(), ctp::scope())?;
    core.reserve_orders(config.preallocated_orders)?;
    let mut runtime = SingleLoop {
        core,
        trader,
        execute,
    };
    let input = placement.on_background(input_commands)?;
    let sdk_version = placement.on_sdk(|| ctp::sdk::version(&config.sdk_library))?;
    journal.record(json!({"type":"runtime_start","execute":execute,"exchanges":crate::model::Venue::ALL.map(|v|v.as_str()),"production_mode":config.production_mode,"sdk":sdk_version}), true)?;
    crate::position_ipc::publish(
        &mut position_ipc,
        &runtime.core,
        false,
        false,
        cn_futures_state::RuntimeStatus::Running,
        true,
        crate::position_ipc::MarketState::default(),
    );
    placement.on_sdk(|| {
        runtime.trader.start(
            &std::iter::once(config.front.as_str())
                .chain(config.fallback_fronts.iter().map(String::as_str))
                .collect::<Vec<_>>()
                .join(","),
        )
    })?;
    let mut feed = market_data
        .as_ref()
        .map(|md| ctp::market_data::Feed::new(md, &placement))
        .transpose()?;
    let mut actions = Actions::default();
    let mut stopping = false;
    let mut commands_open = true;
    let mut transport_online = false;
    let mut transport_progress = Instant::now();
    let mut recovery: Option<DurabilityTicket> = None;
    let outcome = (|| -> Result<()> {
        while !stopping && !stop.load(Ordering::Acquire) {
            if runtime.trader.placement_fault.load(Ordering::Acquire) {
                runtime.core.block("CTP SDK CPU/NUMA binding failed");
                anyhow::bail!("CTP SDK CPU/NUMA binding failed; no fallback is permitted");
            }
            if runtime.trader.fault.load(Ordering::Acquire)
                || journal.failed.load(Ordering::Acquire)
            {
                runtime.core.block(
                    "reliable event ingress/journal failed; stop and reconcile before resubmitting",
                );
                anyhow::bail!(
                "CTP reliable event pipeline failed; existing venue orders have not been cancelled"
            );
            }
            let mut work = false;
            let batch_now = Instant::now();
            for _ in 0..256 {
                let Ok(received) = runtime.trader.events.try_recv() else {
                    break;
                };
                work = true;
                let event = received.event;
                // Persist factual evidence before attempting any parser or strategy association.
                journal.ctp_event(&event, received.received_us, false)?;
                let owns = matches!(
                    event.kind,
                    ctp::ORDER | ctp::TRADE | ctp::QUERY_ORDER | ctp::QUERY_TRADE | ctp::POSITION
                ) && event.present != 0
                    && ctp::text_ref(&event.investor)? == investor
                    && crate::model::Venue::parse(ctp::text_ref(&event.exchange)?).is_ok();
                let decoded = if owns {
                    ctp::decode_event(&event)
                } else {
                    Ok(crate::model::DecodedUpdate::None)
                };
                if event.kind == ctp::QUERY_TRADE || (event.kind == ctp::TRADE && !session.ready())
                {
                    if let Ok(crate::model::DecodedUpdate::Trade(fill)) = &decoded {
                        journal.trade(fill.clone(), None, None, received.received_us)?;
                    }
                }
                if event.kind == ctp::CONNECTED {
                    transport_online = true;
                    transport_progress = Instant::now();
                    recovery = None;
                    runtime.core.connected();
                }
                if event.kind == ctp::DISCONNECTED {
                    transport_online = false;
                    transport_progress = Instant::now();
                    recovery = None;
                    runtime.core.disconnect();
                }
                if event.kind == ctp::INSTRUMENT_STATUS {
                    runtime.core.set_trading_status(
                        crate::model::Venue::parse(ctp::text_ref(&event.exchange)?)?,
                        &ctp::text(&event.symbol)?,
                        event.status == i32::from(b'2'),
                    );
                }
                if let Some(snapshot) = session.on_event(event.as_ref(), batch_now, &decoded)? {
                    let trades = snapshot.trades.clone();
                    runtime.core.restore(snapshot)?;
                    if !session.settlement_confirmed() {
                        runtime
                            .core
                            .block("settlement not confirmed; read-only queries remain available");
                    }
                    recovery =
                        Some(journal.begin_recovery(&mut runtime.core, trades, batch_now)?);
                }
                if let Some(available) = session.take_cash_update() {
                    runtime.core.constrain_cash(available)?;
                }
                if let Some(rules) = session.take_rule_update() {
                    runtime.core.replace_instruments(rules)?;
                }
                if session.ready() {
                    let outcome = match event.kind {
                        ctp::ORDER | ctp::TRADE if owns => decoded
                            .as_ref()
                            .map_err(|error| anyhow::anyhow!(error.to_string()))
                            .and_then(|parsed| match parsed {
                                crate::model::DecodedUpdate::Order(order) => {
                                    runtime.core.on_order(order)
                                }
                                crate::model::DecodedUpdate::Trade(fill) => {
                                    runtime.core.on_fill(fill).map(|_| ())
                                }
                                _ => Ok(()),
                            }),
                        ctp::INSERT_RESPONSE if event.error != 0 => runtime
                            .trader
                            .response_key(&event)
                            .and_then(|key| runtime.core.reject(&key)),
                        ctp::CANCEL_RESPONSE if event.error != 0 => runtime
                            .trader
                            .response_key(&event)
                            .map(|key| runtime.core.cancel_failed(&key)),
                        ctp::ERROR if event.error != 0 => Err(anyhow::anyhow!(
                            "uncorrelated CTP error response {}; query before resubmitting",
                            event.error
                        )),
                        _ => Ok(()),
                    };
                    if event.kind == ctp::TRADE {
                        if let Ok(crate::model::DecodedUpdate::Trade(fill)) = &decoded {
                            journal.trade_for(
                                &runtime.core,
                                fill,
                                None,
                                None,
                                received.received_us,
                                outcome.is_ok(),
                            )?;
                        }
                    }
                    if outcome.is_ok()
                        && matches!(
                            event.kind,
                            ctp::ORDER | ctp::INSERT_RESPONSE | ctp::CANCEL_RESPONSE
                        )
                    {
                        let observed = if event.kind == ctp::ORDER {
                            decoded
                                .as_ref()
                                .ok()
                                .and_then(|p| p.order())
                                .map(|o| o.key.clone())
                                .into_iter()
                                .collect::<Vec<_>>()
                        } else {
                            vec![]
                        };
                        let keys = if !observed.is_empty() {
                            observed.clone()
                        } else {
                            runtime
                                .trader
                                .response_key(&event)
                                .ok()
                                .into_iter()
                                .collect()
                        };
                        let observed = if event.kind == ctp::INSERT_RESPONSE && event.error != 0 {
                            keys.clone()
                        } else {
                            observed
                        };
                        journal.orders(&runtime.core, &keys, &observed)?;
                    }
                    if let Err(error) = outcome {
                        runtime.core.block(
                        "native event could not be reconciled; factual event retained in journal",
                    );
                        journal.record(
                            json!({"type":"trading_blocked","error":error.to_string()}),
                            true,
                        )?;
                    } else if let Ok(update) = &decoded {
                        strategy.on_update(
                            update,
                            StrategyContext {
                                core: &runtime.core,
                                can_trade: execute
                                    && runtime.core.is_ready()
                                    && feed.as_ref().is_some_and(|md| md.ready())
                                    && runtime.trader.events.is_empty(),
                            },
                        )?;
                    }
                }
            }
            let now = Instant::now();
            if let Some(ticket) = &recovery {
                if journal.poll_checkpoint(ticket, now)? {
                    runtime.core.recovery_pending(false);
                    recovery = None;
                    journal.record(
                        json!({"type":"reconciled","state":runtime.core.view()}),
                        true,
                    )?;
                    work = true;
                }
            }
            if runtime.trader.events.is_empty()
                && (session.timed_out(now)
                    || (!transport_online
                        && now.saturating_duration_since(transport_progress)
                            >= Duration::from_millis(config.response_timeout_ms)))
            {
                runtime.core.disconnect();
                journal.state(&runtime.core, &[])?;
                journal.record(json!({"type":"restart_required","reason":"transport or control response timeout","venue_orders_cancelled":false}), true)?;
                anyhow::bail!("CTP timeout requires supervisor restart; SDK lifecycle work is excluded from the business loop");
            }
            if let Some(kind) = session.request_due(Instant::now())? {
                let id = runtime.trader.next_id()?;
                let code = runtime.trader.control(kind, id, session.query_target())?;
                session.sent(kind, id, code, Instant::now())?;
                work = true;
            }
            // Tick consumption, strategy, risk, OMS and the SDK call all use this thread.
            if let Some(md) = &mut feed {
                for _ in 0..256 {
                    let queued = md.has_backlog();
                    let event = md.poll(Instant::now())?;
                    work |= queued;
                    let Some(event) = event else {
                        if queued {
                            continue;
                        }
                        break;
                    };
                    match event {
                        ctp::market_data::FeedEvent::Tick(tick) => {
                            let can_trade = execute
                                && runtime.core.is_ready()
                                && md.ready()
                                && !md.has_backlog()
                                && runtime.trader.events.is_empty()
                                && ctp::text_ref(&tick.trading_day)? == runtime.core.trading_day();
                            strategy.on_tick(
                                &tick,
                                StrategyContext {
                                    core: &runtime.core,
                                    can_trade,
                                },
                                &mut actions,
                            )?;
                            for action in actions.drain() {
                                let allowed = can_trade
                                    && md.ready()
                                    && runtime.trader.events.is_empty()
                                    && !md.has_backlog()
                                    && !journal.failed.load(Ordering::Acquire)
                                    && !runtime.trader.fault.load(Ordering::Acquire);
                                let outcome = ctp::strategy::dispatch(
                                    &mut runtime,
                                    &journal,
                                    action,
                                    allowed,
                                    Instant::now(),
                                )?;
                                strategy.on_result(
                                    &outcome,
                                    StrategyContext {
                                        core: &runtime.core,
                                        can_trade: allowed && runtime.core.is_ready(),
                                    },
                                )?;
                                journal.strategy_result(outcome)?;
                            }
                        }
                        ctp::market_data::FeedEvent::Connected { generation } => {
                            strategy.on_market_state(false, generation)?;
                            journal.record(
                                json!({"type":"md_connected","generation":generation}),
                                true,
                            )?;
                        }
                        ctp::market_data::FeedEvent::Disconnected { reason } => {
                            strategy.on_market_state(false, md.generation())?;
                            journal.record(
                                json!({"type":"md_disconnected","reason":reason,"gap":true}),
                                true,
                            )?;
                        }
                        ctp::market_data::FeedEvent::Ready { generation } => {
                            strategy.on_market_state(true, generation)?;
                            journal
                                .record(json!({"type":"md_ready","generation":generation}), true)?;
                        }
                    }
                }
            }
            // Never submit an opening decision ahead of a known backlog of venue events.
            if commands_open && runtime.trader.events.is_empty() {
                match input.try_recv() {
                    Ok(command) => {
                        work = true;
                        let day = runtime.core.trading_day().to_owned();
                        let cancel_keys = command
                            .as_ref()
                            .ok()
                            .map(command_order_keys)
                            .unwrap_or_default();
                        let result = match command {
                            Ok(Command::State {}) => Ok(
                                json!({"type":"state","state":runtime.core.view(),"account":session.account(),"settlement_confirmed":session.settlement_confirmed()}),
                            ),
                            Ok(Command::Shutdown {}) => {
                                stopping = true;
                                Ok(json!({"type":"shutdown","venue_orders_cancelled":false}))
                            }
                            Ok(Command::Reconcile {}) => {
                                session.restart_queries(Instant::now()).map(|_| {
                                    recovery = None;
                                    runtime.core.block("operator-requested reconciliation");
                                    json!({"type":"reconciling"})
                                })
                            }
                            Ok(Command::Submit { .. })
                                if market_data.is_some()
                                    && !feed.as_ref().is_some_and(|md| md.ready()) =>
                            {
                                Err(anyhow::anyhow!(
                                    "MD is not ready; opening decisions are blocked"
                                ))
                            }
                            Ok(Command::Submit { intent }) => runtime
                                .submit_planned_with(intent, Instant::now(), |prepared| {
                                    journal.prepared_submit(&day, prepared)
                                })
                                .map(|receipt| json!({"type":"submit_receipt","receipt":receipt})),
                            Ok(Command::Cancel {
                                key,
                                execution_route: None,
                            }) => runtime
                                .cancel_with(&key, Instant::now(), |prepared| {
                                    journal.prepared_cancel(prepared)
                                })
                                .map(|_| json!({"type":"cancel_submitted_locally","key":key})),
                            Ok(
                                Command::SubmitBatch { .. }
                                | Command::CancelBatch { .. }
                                | Command::Connections {}
                                | Command::Cancel {
                                    execution_route: Some(_),
                                    ..
                                },
                            ) => Err(anyhow::anyhow!(
                                "native batch/seat commands are available on the Yida runtime only"
                            )),
                            Err(error) => Err(error),
                        };
                        journal.command_state(&runtime.core, &result, cancel_keys)?;
                        journal.record(
                        result.unwrap_or_else(
                            |error| json!({"type":"command_rejected","error":error.to_string()}),
                        ),
                        true,
                    )?;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        commands_open = false;
                        stopping = market_data.is_none();
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {}
                }
            }
            let md_ready = feed
                .as_ref()
                .is_none_or(|md| md.ready() && !md.has_backlog());
            crate::position_ipc::publish(
                &mut position_ipc,
                &runtime.core,
                transport_online,
                runtime.execute
                    && runtime.core.is_ready()
                    && transport_online
                    && md_ready
                    && runtime.trader.events.is_empty(),
                cn_futures_state::RuntimeStatus::Running,
                false,
                crate::position_ipc::MarketState::default(),
            );
            if !work {
                std::hint::spin_loop();
            }
        }
        Ok(())
    })();
    if outcome.is_err() {
        runtime
            .core
            .block("runtime failed; complete recovery required before trading");
    }
    // The active business loop has ended. SDK lifecycle calls below are cold shutdown work.
    drop(feed.take());
    // SDK Release joins callbacks; a cloned receiver retains the final delivered evidence.
    let tail = runtime.trader.events.clone();
    let fault = runtime.trader.fault.clone();
    let mut core = runtime.core;
    drop(runtime.trader);
    for received in tail.try_iter() {
        let event = received.event;
        journal.ctp_event(&event, received.received_us, true)?;
        if ctp::text(&event.investor).is_ok_and(|v| v == investor)
            && ctp::text(&event.exchange).is_ok_and(|v| crate::model::Venue::parse(&v).is_ok())
        {
            if matches!(event.kind, ctp::TRADE | ctp::QUERY_TRADE) {
                if let Ok(fill) = event.to_fill() {
                    journal.trade(fill.clone(), None, None, received.received_us)?;
                    if event.kind == ctp::TRADE && core.on_fill(&fill).is_err() {
                        core.block("shutdown fill requires recovery");
                    }
                }
            }
            if event.kind == ctp::ORDER {
                if let Ok(update) = event.to_order() {
                    let key = update.key.clone();
                    if core.on_order(&update).is_ok() {
                        journal.state(&core, &[key])?;
                    } else {
                        core.block("shutdown order requires recovery");
                    }
                }
            }
        }
    }
    journal.state(&core, &[])?;
    let shutdown_ok = !fault.load(Ordering::Acquire);
    crate::position_ipc::publish(
        &mut position_ipc,
        &core,
        false,
        false,
        if outcome.is_ok() && shutdown_ok {
            cn_futures_state::RuntimeStatus::Stopped
        } else {
            cn_futures_state::RuntimeStatus::Failed
        },
        true,
        crate::position_ipc::MarketState::default(),
    );
    journal.record(
        serde_json::json!({"type":"runtime_stopped","venue_orders_cancelled":false}),
        true,
    )?;
    journal.close_async()?;
    while !journal.poll_closed()? {
        std::hint::spin_loop();
    }
    if let Some(publisher) = position_ipc {
        publisher.finish()?;
    }
    anyhow::ensure!(
        shutdown_ok,
        "CTP callback pipeline failed on shutdown; evidence retained"
    );
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_and_malformed_commands_fail() {
        assert!(serde_json::from_str::<Command>(r#"{"op":"flatten"}"#).is_err());
        assert!(serde_json::from_str::<Command>(r#"{"op":"state","password":"x"}"#).is_err());
    }
    #[test]
    fn flow_directory_cannot_be_shared_concurrently() {
        let path =
            std::env::temp_dir().join(format!("cn-futures-flow-lock-{}", std::process::id()));
        let lock = lock_flow(&path).unwrap();
        assert!(lock_flow(&path).is_err());
        drop(lock);
        let lock = lock_flow(&path).unwrap();
        drop(lock);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn credential_redaction_happens_before_json_escaping() {
        let secret = "quoted\"and\\escaped".to_owned();
        let mut value = serde_json::json!({"nested":[{"message":format!("bad {secret}")} ]});
        redact(&mut value, &[secret]);
        assert_eq!(value["nested"][0]["message"], "bad [redacted]");
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    fn fixture() -> (Journal, crossbeam_channel::Receiver<Record>) {
        let (sender, receiver) = crossbeam_channel::bounded(8);
        (
            Journal {
                sender: Some(sender),
                enqueued: Cell::new(0),
                stats: Arc::new(WriterStats::default()),
                failed: Arc::new(AtomicBool::new(false)),
                worker: None,
                sync_before_send: false,
                reference_floors: Default::default(),
            },
            receiver,
        )
    }
    #[test]
    fn checkpoint_polling_never_reads_an_ack_channel_and_requires_a_valid_prefix() {
        let (journal, receiver) = fixture();
        journal
            .record(serde_json::json!({"type":"fixture"}), false)
            .unwrap();
        assert!(receiver.try_recv().unwrap().ack.is_none());
        let now = Instant::now();
        let ticket = DurabilityTicket {
            target: 1,
            deadline: now + Duration::from_secs(30),
        };
        assert!(!journal.poll_checkpoint(&ticket, now).unwrap());
        // An error-evidence record may be durable without being a valid projection.
        journal.stats.records.store(1, Ordering::Release);
        assert!(!journal.poll_checkpoint(&ticket, now).unwrap());
        journal.stats.confirmed.store(1, Ordering::Release);
        assert!(journal.poll_checkpoint(&ticket, now).unwrap());
        journal.failed.store(true, Ordering::Release);
        assert!(journal.poll_checkpoint(&ticket, now).is_err());
    }
    #[test]
    fn checkpoint_timeout_latches_failure_without_waiting() {
        let (journal, _receiver) = fixture();
        let now = Instant::now();
        let ticket = DurabilityTicket {
            target: 1,
            deadline: now,
        };
        assert!(journal.poll_checkpoint(&ticket, now).is_err());
        assert!(journal.failed.load(Ordering::Acquire));
        assert!(journal.record(serde_json::Value::Null, false).is_err());
    }
    #[test]
    fn async_close_disconnects_the_queue_and_has_no_ack_request() {
        let (mut journal, receiver) = fixture();
        journal.close_async().unwrap();
        assert!(receiver.try_recv().unwrap().ack.is_none());
        assert!(matches!(
            receiver.try_recv(),
            Err(crossbeam_channel::TryRecvError::Disconnected)
        ));
        assert!(journal.poll_closed().unwrap());
    }
}

#[cfg(test)]
mod persistence_tests {
    use super::*;
    use crate::model::*;
    use persist_manager::cn_futures::{COLUMN_FAMILIES, EVENTS, ORDERS};
    use std::path::PathBuf;
    fn identity() -> StoreIdentity {
        crate::persistence::store_identity("fixture", &[b"account", b"endpoint"])
    }
    fn prepared() -> PreparedOrder {
        PreparedOrder {
            key: OrderKey {
                session: "yida:20261110:group:1".into(),
                order_ref: "101".into(),
            },
            intent: Intent {
                venue: Venue::Shfe,
                symbol: "cu2612".into(),
                side: Side::Sell,
                offset: Offset::Open,
                lots: 1,
                price_type: PriceType::Limit,
                price_ticks: 20000,
                time_in_force: TimeInForce::Day,
                execution_route: None,
            },
            limit_price: 40000.0,
            option_market_buy_ceiling: 0.0,
        }
    }
    fn path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cn-journal-{label}-{}-{}",
            std::process::id(),
            crate::persistence::now_us()
        ))
    }
    fn journal(path: &Path) -> Journal {
        Journal::start(
            &path.join("audit.jsonl"),
            64,
            vec!["fixture-secret".into()],
            &path.join("db"),
            identity(),
            true,
        )
        .unwrap()
    }
    #[test]
    fn shutdown_drains_async_records_and_redacts_rocksdb_evidence() {
        let path = path("drain");
        let journal = journal(&path);
        for i in 0..20 {
            journal
                .record(serde_json::json!({"i":i,"message":"fixture-secret"}), false)
                .unwrap();
        }
        journal.prepared_submit("20261110", &prepared()).unwrap();
        journal.finish().unwrap();
        let store = persist_manager::RocksDbStore::open_read_only(
            path.join("db").to_str().unwrap(),
            COLUMN_FAMILIES,
        )
        .unwrap();
        let events = store.scan(EVENTS, None, false, None).unwrap();
        assert_eq!(events.len(), 22);
        assert!(events
            .iter()
            .all(|(_, v)| !String::from_utf8_lossy(v).contains("fixture-secret")));
        assert_eq!(store.scan(ORDERS, None, false, None).unwrap().len(), 1);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn writer_failure_is_acknowledged_and_blocks_subsequent_records() {
        let path = path("failure");
        let journal = journal(&path);
        journal.prepared_submit("20261110", &prepared()).unwrap();
        let mut conflict = crate::persistence::prepared("20261110", &prepared());
        conflict.orders[0].order.request.lots = 2;
        assert!(journal
            .record_with(
                serde_json::json!({"type":"conflict"}),
                false,
                conflict,
                true
            )
            .is_err());
        // Drop joins even when a write error disconnected the acknowledgement channel.
        assert!(journal.finish().is_err());
        let store = persist_manager::RocksDbStore::open_read_only(
            path.join("db").to_str().unwrap(),
            COLUMN_FAMILIES,
        )
        .unwrap();
        let events = store.scan(EVENTS, None, true, Some(1)).unwrap();
        assert!(
            serde_json::from_slice::<serde_json::Value>(&events[0].1).unwrap()["projection_error"]
                .is_string()
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn process_exit_without_destructors_retains_acknowledged_intent_and_reference_floor() {
        let path = path("crash");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "runtime::persistence_tests::crash_writer",
            ])
            .env("CN_TEST_CRASH_DB", &path)
            .status()
            .unwrap();
        assert!(status.success());
        let mut store = NativeStore::open(&path.join("db"), &identity()).unwrap();
        assert_eq!(
            store
                .reference_floors()
                .get(&("20261110".into(), prepared().key.session)),
            Some(&109)
        );
        let error = store
            .append(
                1,
                serde_json::json!({"type":"recovery"}),
                NativeMutation {
                    reconciliation_day: Some("20261110".into()),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("absent from complete recovery"));
        drop(store);
        let store = persist_manager::RocksDbStore::open_read_only(
            path.join("db").to_str().unwrap(),
            COLUMN_FAMILIES,
        )
        .unwrap();
        assert_eq!(store.scan(ORDERS, None, false, None).unwrap().len(), 8);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    #[ignore = "child process fixture; invoked by the crash recovery test"]
    fn crash_writer() {
        let path = PathBuf::from(
            std::env::var_os("CN_TEST_CRASH_DB").expect("parent crash test sets path"),
        );
        let journal = journal(&path);
        for reference in 101..109 {
            let mut order = prepared();
            order.key.order_ref = reference.to_string();
            journal
                .record_with(
                    serde_json::json!({"type":"async_crash_intent","reference":reference}),
                    false,
                    crate::persistence::prepared("20261110", &order),
                    false,
                )
                .unwrap();
        }
        journal
            .record_with(
                serde_json::json!({"type":"crash_barrier"}),
                false,
                NativeMutation::default(),
                true,
            )
            .unwrap();
        std::process::exit(0);
    }
}

#[cfg(test)]
mod deferred_audit_tests {
    use super::*;
    #[test]
    fn copied_native_events_keep_ids_float_bits_tail_flags_and_redaction_on_disk() {
        let path = std::env::temp_dir().join(format!(
            "cn-deferred-audit-{}-{}",
            std::process::id(),
            crate::persistence::now_us()
        ));
        let j = Journal::start(
            &path.join("audit.jsonl"),
            64,
            vec!["fixture-secret".into()],
            &path.join("db"),
            crate::persistence::store_identity("fixture", &[b"deferred-audit"]),
            false,
        )
        .unwrap();
        let mut yida = crate::yida::RawEvent {
            kind: crate::yida::TRADE,
            trade_id: 9_007_199_254_740_997,
            system_id: 9_007_199_254_740_993,
            commission: 52.660000000000004,
            ..Default::default()
        };
        yida.original_trade_id[0] = 255;
        j.yida_event(&yida, 123, false).unwrap();
        j.yida_event(&yida, 124, true).unwrap();
        yida.trade_id = 17; // The producer may immediately reuse its event buffer.
        let mut ctp = crate::ctp::RawEvent {
            hot: crate::ctp::HotEvent {
                kind: crate::ctp::ERROR,
                error: 31,
                ..crate::ctp::RawEvent::default().hot
            },

            ..Default::default()
        };
        ctp.message[..14].copy_from_slice(b"fixture-secret");
        let pool = crate::ctp::ingress::ControlPool::new(1).unwrap();
        let event = crate::ctp::ingress::Event::Control(pool.capture(&ctp).unwrap());
        j.ctp_event(&event, 125, true).unwrap();
        let prepared = crate::model::PreparedOrder {
            key: OrderKey {
                session: "yida:20261110:group:1".into(),
                order_ref: "101".into(),
            },
            intent: Intent {
                venue: crate::model::Venue::Shfe,
                symbol: "cu2612".into(),
                side: crate::model::Side::Buy,
                offset: crate::model::Offset::Open,
                lots: 1,
                price_type: crate::model::PriceType::Limit,
                price_ticks: 20000,
                time_in_force: crate::model::TimeInForce::Day,
                execution_route: None,
            },
            limit_price: 100000.0,
            option_market_buy_ceiling: 0.0,
        };
        let before = crate::persistence::now_us();
        j.prepared_submit("20261110", &prepared).unwrap();
        let enqueued = crate::persistence::now_us();
        j.finish().unwrap();
        let store = persist_manager::RocksDbStore::open_read_only(
            path.join("db").to_str().unwrap(),
            persist_manager::cn_futures::COLUMN_FAMILIES,
        )
        .unwrap();
        let rows = store
            .scan(persist_manager::cn_futures::EVENTS, None, false, None)
            .unwrap();
        let records: Vec<Value> = rows
            .iter()
            .map(|(_, v)| serde_json::from_slice::<Value>(v).unwrap()["value"].clone())
            .collect();
        assert_eq!(
            records[0]["event"]["trade_id"].as_i64(),
            Some(9_007_199_254_740_997)
        );
        assert_eq!(
            records[0]["event"]["system_id"].as_i64(),
            Some(9_007_199_254_740_993)
        );
        assert_eq!(
            records[0]["event"]["commission"]
                .as_f64()
                .unwrap()
                .to_bits(),
            yida.commission.to_bits()
        );
        assert_eq!(records[0]["event"]["original_trade_id"][0], 255);
        assert_eq!(records[0]["received_us"], 123);
        assert!(records[0].get("shutdown_tail").is_none());
        assert_eq!(records[1]["shutdown_tail"], true);
        assert_eq!(records[2]["event"]["received_us"], 125);
        assert_eq!(records[2]["event"]["message"], "[redacted]");
        let orders = store
            .scan(persist_manager::cn_futures::ORDERS, None, false, None)
            .unwrap();
        let order: persist_common::cn_futures::NativeOrderRecord =
            serde_json::from_slice(&orders[0].1).unwrap();
        assert!(
            (before..=enqueued).contains(&order.first_received_us),
            "prepared time must be captured on the producer, before the writer drains its backlog"
        );
        let audit = std::fs::read_to_string(path.join("audit.jsonl")).unwrap();
        assert!(!audit.contains("fixture-secret"));
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
#[path = "runtime/projection_tests.rs"]
mod projection_tests;
