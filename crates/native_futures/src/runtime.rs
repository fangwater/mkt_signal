//! Operational I/O stays off the business thread. No automatic order cancellation,
//! cross-account routing, or strategy attribution is performed on startup/recovery.
use crate::model::{Intent, OrderKey};
use anyhow::{Context, Result};
#[cfg(feature = "ctp-sdk")]
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use serde::Deserialize;
use serde_json::Value;
use std::fs::{File, OpenOptions};
#[cfg(feature = "ctp-sdk")]
use std::io::BufRead;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Submit { intent: Intent },
    Cancel { key: OrderKey },
    State {},
    Reconcile {},
    Shutdown {},
}

struct Record {
    value: Value,
    display: bool,
}
fn redact(value: &mut Value, secrets: &[String]) {
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
pub struct Journal {
    sender: Option<Sender<Record>>,
    pub failed: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Journal {
    pub fn start(path: &Path, capacity: usize, redactions: Vec<String>) -> Result<Self> {
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
        let worker = std::thread::Builder::new()
            .name("ctp-journal".into())
            .spawn(move || {
                let write = || -> Result<()> {
                    let mut writer = BufWriter::new(file);
                    let mut last_sync = Instant::now();
                    loop {
                        match receiver.recv_timeout(Duration::from_millis(100)) {
                            Ok(mut record) => {
                                redact(&mut record.value, &redactions);
                                let line = serde_json::to_string(&record.value)?;
                                writeln!(writer, "{line}")?;
                                if record.display {
                                    let stdout = std::io::stdout();
                                    let mut out = stdout.lock();
                                    writeln!(out, "{line}")?;
                                    out.flush()?;
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
                if write().is_err() {
                    flag.store(true, Ordering::Release);
                }
            })
            .context("spawn native journal writer")?;
        Ok(Self {
            sender: Some(sender),
            failed,
            worker: Some(worker),
        })
    }
    pub fn record(&self, value: Value, display: bool) -> Result<()> {
        anyhow::ensure!(
            !self.failed.load(Ordering::Acquire),
            "native journal writer failed"
        );
        let result = self
            .sender
            .as_ref()
            .context("journal closed")?
            .try_send(Record { value, display });
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
            anyhow::bail!("reliable native journal queue full/disconnected");
        }
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

#[cfg(feature = "ctp-sdk")]
fn input_commands() -> Result<Receiver<Result<Command>>> {
    let (sender, receiver) = crossbeam_channel::bounded(64);
    std::thread::Builder::new()
        .name("ctp-commands".into())
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
    std::fs::create_dir_all(path).context("create CTP flow directory")?;
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(path.join(".native_futures.lock"))
        .context("open CTP flow lock")?;
    file.try_lock()
        .context("CTP flow directory already in use")?;
    Ok(file)
}

#[cfg(feature = "ctp-sdk")]
pub fn run(config: crate::config::Config, execute: bool) -> Result<()> {
    use crate::core::{SingleLoop, TradingCore};
    use crate::ctp::{self, sdk::CtpTrader, session::Session};
    use serde_json::json;
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
    let journal = Journal::start(&config.journal, config.event_capacity, redactions)?;
    let trader = CtpTrader::new(&config, credentials)?;
    let investor = trader
        .investor()
        .to_str()
        .context("invalid InvestorID")?
        .to_owned();
    let mut session = Session::new(config.clone(), investor.clone(), Instant::now());
    let core = TradingCore::new(config.risk.clone())?;
    let mut runtime = SingleLoop {
        core,
        trader,
        execute,
    };
    let input = input_commands()?;
    journal.record(json!({"type":"runtime_start","execute":execute,"exchange":"CFFEX","production_mode":config.production_mode,"sdk":ctp::sdk::version(&config.sdk_library)?}), true)?;
    runtime.trader.start(&config.front)?;
    let mut stopping = false;
    while !stopping {
        if runtime.trader.fault.load(Ordering::Acquire) || journal.failed.load(Ordering::Acquire) {
            runtime.core.block(
                "reliable event ingress/journal failed; stop and reconcile before resubmitting",
            );
            anyhow::bail!(
                "CTP reliable event pipeline failed; existing venue orders have not been cancelled"
            );
        }
        let mut work = false;
        for _ in 0..256 {
            let Ok(received) = runtime.trader.events.try_recv() else {
                break;
            };
            work = true;
            let event = received.event;
            // Persist factual evidence before attempting any parser or strategy association.
            journal.record(
                json!({"type":"ctp_event","event":event.audit(received.received_us)}),
                false,
            )?;
            if event.kind == ctp::CONNECTED {
                runtime.core.connected();
            }
            if event.kind == ctp::DISCONNECTED {
                runtime.core.disconnect();
            }
            if event.kind == ctp::INSTRUMENT_STATUS && ctp::text(&event.exchange)? == "CFFEX" {
                runtime.core.set_trading_status(
                    &ctp::text(&event.symbol)?,
                    event.status == i32::from(b'2'),
                );
            }
            if let Some(snapshot) = session.on_event(&event, Instant::now())? {
                runtime.core.restore(snapshot)?;
                journal.record(
                    json!({"type":"reconciled","state":runtime.core.view()}),
                    true,
                )?;
            }
            if let Some(available) = session.take_cash_update() {
                runtime.core.constrain_cash(available)?;
            }
            if let Some(rules) = session.take_rule_update() {
                runtime.core.replace_instruments(rules)?;
            }
            if session.ready() {
                let outcome = match event.kind {
                    ctp::ORDER | ctp::TRADE
                        if ctp::text(&event.investor)? == investor
                            && ctp::text(&event.exchange)? == "CFFEX" =>
                    {
                        if event.kind == ctp::ORDER {
                            event.to_order().and_then(|o| runtime.core.on_order(o))
                        } else {
                            event
                                .to_fill()
                                .and_then(|f| runtime.core.on_fill(f).map(|_| ()))
                        }
                    }
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
                if let Err(error) = outcome {
                    runtime.core.block(
                        "native event could not be reconciled; factual event retained in journal",
                    );
                    journal.record(
                        json!({"type":"trading_blocked","error":error.to_string()}),
                        true,
                    )?;
                }
            }
        }
        if let Some(kind) = session.request_due(Instant::now())? {
            let id = runtime.trader.next_id()?;
            let code = runtime.trader.control(kind, id)?;
            session.sent(kind, id, code, Instant::now())?;
            work = true;
        }
        // Never submit an opening decision ahead of a known backlog of venue events.
        if runtime.trader.events.is_empty() {
            match input.try_recv() {
                Ok(command) => {
                    work = true;
                    let result = match command {
                        Ok(Command::State {}) => {
                            Ok(json!({"type":"state","state":runtime.core.view()}))
                        }
                        Ok(Command::Shutdown {}) => {
                            stopping = true;
                            Ok(json!({"type":"shutdown","venue_orders_cancelled":false}))
                        }
                        Ok(Command::Reconcile {}) => {
                            session.restart_queries(Instant::now()).map(|_| {
                                runtime.core.block("operator-requested reconciliation");
                                json!({"type":"reconciling"})
                            })
                        }
                        Ok(Command::Submit { intent }) => {
                            runtime
                                .submit_with(intent, Instant::now(), |prepared| journal.record(json!({"type":"prepared_submit","key":prepared.key,"intent":prepared.intent,"limit_price":prepared.limit_price}), false))
                                .map(|key| json!({"type":"submitted_locally","key":key}))
                        }
                        Ok(Command::Cancel { key }) => {
                            runtime
                                .cancel_with(&key, Instant::now(), |prepared| journal.record(json!({"type":"prepared_cancel","key":prepared.key,"symbol":prepared.symbol,"system_id":prepared.system_id}), false))
                                .map(|_| json!({"type":"cancel_submitted_locally","key":key}))
                        }
                        Err(error) => Err(error),
                    };
                    journal.record(
                        result.unwrap_or_else(
                            |error| json!({"type":"command_rejected","error":error.to_string()}),
                        ),
                        true,
                    )?;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => stopping = true,
                Err(crossbeam_channel::TryRecvError::Empty) => {}
            }
        }
        if !work {
            if config.idle_sleep_us == 0 {
                std::hint::spin_loop();
            } else {
                std::thread::sleep(Duration::from_micros(config.idle_sleep_us));
            }
        }
    }
    // Destroy the SDK (and join callbacks) before closing the evidence writer.
    drop(runtime);
    journal.record(
        serde_json::json!({"type":"runtime_stopped","venue_orders_cancelled":false}),
        true,
    )?;
    Ok(())
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
            std::env::temp_dir().join(format!("native-futures-flow-lock-{}", std::process::id()));
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
