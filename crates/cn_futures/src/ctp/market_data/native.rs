use super::*;
use crate::placement::{CallbackPlacement, Placement};
use anyhow::bail;
use crossbeam_channel::{Receiver, Sender};
use serde_json::{json, Value};
use std::ffi::{c_char, c_void, CStr, CString};
use std::io::{BufWriter, Write};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

extern "C" {
    fn nf_md_event_size() -> usize;
    fn nf_md_version(path: *const c_char, out: *mut c_char, size: usize) -> i32;
    fn nf_md_create(
        path: *const c_char,
        flow: *const c_char,
        production: bool,
        callback: unsafe extern "C" fn(*mut c_void, *const RawEvent),
        context: *mut c_void,
        error: *mut c_char,
        size: usize,
    ) -> *mut c_void;
    fn nf_md_destroy(handle: *mut c_void);
    fn nf_md_start(handle: *mut c_void, fronts: *const c_char) -> i32;
    fn nf_md_login(
        handle: *mut c_void,
        id: i32,
        broker: *const c_char,
        user: *const c_char,
        password: *const c_char,
    ) -> i32;
    fn nf_md_subscribe(handle: *mut c_void, symbols: *const *const c_char, count: usize) -> i32;
}
struct ContextState {
    sender: Sender<(RawEvent, u64)>,
    failed: Arc<AtomicBool>,
    placement: Arc<CallbackPlacement>,
    placement_fault: Arc<AtomicBool>,
}
unsafe extern "C" fn callback(context: *mut c_void, event: *const RawEvent) {
    if context.is_null() || event.is_null() {
        return;
    }
    let context = unsafe { &*context.cast::<ContextState>() };
    if !context.placement.enforce() {
        context.placement_fault.store(true, Ordering::Release);
        context.failed.store(true, Ordering::Release);
        return;
    }
    if context
        .sender
        .try_send((unsafe { *event }, crate::persistence::now_us()))
        .is_err()
    {
        context.failed.store(true, Ordering::Release);
    }
}
pub fn version(path: &Path) -> Result<String> {
    let path = CString::new(path.to_str().context("non-UTF8 MD SDK path")?)?;
    let mut out = [0i8; 256];
    ensure!(
        unsafe { nf_md_version(path.as_ptr(), out.as_mut_ptr(), out.len()) } == 0,
        "cannot load official MD SDK"
    );
    Ok(unsafe { CStr::from_ptr(out.as_ptr()) }.to_str()?.to_owned())
}
pub(super) fn credential(name: &str, capacity: usize) -> Result<CString> {
    let value = std::env::var(name).unwrap_or_default();
    ensure!(
        !value.is_empty() && value.len() < capacity && value.is_ascii() && !value.contains('\0'),
        "missing or invalid MD credential variable {name}"
    );
    Ok(CString::new(value)?)
}
pub(super) struct Native {
    handle: NonNull<c_void>,
    _context: Box<ContextState>,
    pub(super) events: Receiver<(RawEvent, u64)>,
    pub(super) failed: Arc<AtomicBool>,
    pub(super) placement_fault: Arc<AtomicBool>,
    _symbols: Vec<CString>,
    symbol_pointers: Vec<*const c_char>,
}
impl Native {
    pub(super) fn new(config: &Config, placement: Arc<CallbackPlacement>) -> Result<Self> {
        ensure!(
            unsafe { nf_md_event_size() } == std::mem::size_of::<RawEvent>(),
            "MD FFI layout mismatch"
        );
        let (sender, events) = crossbeam_channel::bounded(config.event_capacity);
        let failed = Arc::new(AtomicBool::new(false));
        let placement_fault = Arc::new(AtomicBool::new(false));
        let mut context = Box::new(ContextState {
            sender,
            failed: failed.clone(),
            placement,
            placement_fault: placement_fault.clone(),
        });
        let path = CString::new(
            config
                .sdk_library
                .to_str()
                .context("non-UTF8 MD SDK path")?,
        )?;
        let flow = CString::new(format!("{}/", config.flow_directory.display()))?;
        let mut error = [0i8; 256];
        let raw = unsafe {
            nf_md_create(
                path.as_ptr(),
                flow.as_ptr(),
                config.production_mode,
                callback,
                (&mut *context as *mut ContextState).cast(),
                error.as_mut_ptr(),
                error.len(),
            )
        };
        let handle =
            NonNull::new(raw).context("cannot create official CTP MdApi; check SDK version")?;
        let symbols: Vec<CString> = config
            .symbols
            .iter()
            .map(|s| CString::new(s.symbol.as_str()))
            .collect::<std::result::Result<_, _>>()?;
        let symbol_pointers = symbols.iter().map(|s| s.as_ptr()).collect();
        Ok(Self {
            handle,
            _context: context,
            events,
            failed,
            placement_fault,
            _symbols: symbols,
            symbol_pointers,
        })
    }
    pub(super) fn start(&self, config: &Config) -> Result<()> {
        let fronts = CString::new(config.fronts.join(","))?;
        ensure!(
            unsafe { nf_md_start(self.handle.as_ptr(), fronts.as_ptr()) } == 0,
            "cannot start MD transport"
        );
        Ok(())
    }
    pub(super) fn login(
        &self,
        id: i32,
        broker: &CString,
        user: &CString,
        password: &CString,
    ) -> Result<()> {
        ensure!(
            unsafe {
                nf_md_login(
                    self.handle.as_ptr(),
                    id,
                    broker.as_ptr(),
                    user.as_ptr(),
                    password.as_ptr(),
                )
            } == 0,
            "MD login request rejected locally"
        );
        Ok(())
    }
    pub(super) fn subscribe(&self) -> Result<()> {
        ensure!(
            unsafe {
                nf_md_subscribe(
                    self.handle.as_ptr(),
                    self.symbol_pointers.as_ptr(),
                    self.symbol_pointers.len(),
                )
            } == 0,
            "MD subscription rejected locally"
        );
        Ok(())
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        unsafe { nf_md_destroy(self.handle.as_ptr()) };
    }
}

#[allow(clippy::large_enum_variant)] // Inline preallocated tick storage; no Box allocation on the business thread.
enum OutputMessage {
    Status(Value),
    Tick(TickRecord),
}
#[derive(serde::Serialize)]
struct TickLine {
    r#type: &'static str,
    tick: Tick,
}
struct Output {
    sender: Option<Sender<OutputMessage>>,
    failed: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Output {
    fn new(config: &Config, secrets: Vec<String>) -> Result<Self> {
        if let Some(parent) = config.journal.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut options = std::fs::OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&config.journal).context("open MD journal")?;
        let (sender, receiver) = crossbeam_channel::bounded::<OutputMessage>(config.event_capacity);
        let failed = Arc::new(AtomicBool::new(false));
        let flag = failed.clone();
        let worker = std::thread::Builder::new()
            .name("ctp-md-output".into())
            .spawn(move || {
                let write = || -> Result<()> {
                    let mut file = BufWriter::new(file);
                    let stdout = std::io::stdout();
                    let mut stdout = BufWriter::with_capacity(65536, stdout.lock());
                    let mut last_flush = Instant::now();
                    let encode = |message: OutputMessage| -> Result<String> {
                        match message {
                            OutputMessage::Status(mut value) => {
                                crate::runtime::redact(&mut value, &secrets);
                                Ok(serde_json::to_string(&value)?)
                            }
                            OutputMessage::Tick(record) => {
                                let mut tick = record.decode()?;
                                for value in [
                                    &mut tick.symbol,
                                    &mut tick.exchange_reported,
                                    &mut tick.trading_day,
                                    &mut tick.action_day,
                                    &mut tick.update_time,
                                ] {
                                    for secret in &secrets {
                                        if !secret.is_empty() {
                                            *value = value.replace(secret, "[redacted]");
                                        }
                                    }
                                }
                                Ok(serde_json::to_string(&TickLine {
                                    r#type: "tick",
                                    tick,
                                })?)
                            }
                        }
                    };
                    loop {
                        let first = match receiver.recv_timeout(Duration::from_millis(100)) {
                            Ok(value) => Some(value),
                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => None,
                            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                        };
                        if let Some(first) = first {
                            let line = encode(first)?;
                            writeln!(file, "{line}")?;
                            writeln!(stdout, "{line}")?;
                            // Drain only records already available; never wait to fill a batch.
                            for _ in 1..64 {
                                let Ok(message) = receiver.try_recv() else {
                                    break;
                                };
                                let line = encode(message)?;
                                writeln!(file, "{line}")?;
                                writeln!(stdout, "{line}")?;
                            }
                            stdout.flush()?;
                        }
                        if last_flush.elapsed() >= Duration::from_millis(100) {
                            file.flush()?;
                            last_flush = Instant::now();
                        }
                    }
                    stdout.flush()?;
                    file.flush()?;
                    file.get_ref().sync_all()?;
                    Ok(())
                };
                if write().is_err() {
                    flag.store(true, Ordering::Release);
                    eprintln!("CTP MD output failed; stop before treating the stream as complete");
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            failed,
            worker: Some(worker),
        })
    }
    fn send_tick(&self, record: TickRecord) -> Result<()> {
        ensure!(!self.failed.load(Ordering::Acquire), "MD output failed");
        self.sender
            .as_ref()
            .context("MD output stopped")?
            .try_send(OutputMessage::Tick(record))
            .context("MD output queue full; stream incomplete")
    }
    fn send(&self, value: Value) -> Result<()> {
        ensure!(!self.failed.load(Ordering::Acquire), "MD output failed");
        self.sender
            .as_ref()
            .context("MD output stopped")?
            .try_send(OutputMessage::Status(value))
            .context("MD output queue full; stream incomplete")
    }
    fn finish(&mut self) -> Result<()> {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("CTP MD output worker panicked; stream incomplete"))?;
        }
        ensure!(
            !self.failed.load(Ordering::Acquire),
            "CTP MD output flush failed; stream incomplete"
        );
        Ok(())
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            eprintln!("{error}");
        }
    }
}

pub fn run(config: Config, duration: Option<Duration>, stop: &AtomicBool) -> Result<()> {
    config.validate()?;
    let placement = Placement::enter(&config.placement)?;
    let _lock = crate::runtime::lock_flow(&config.flow_directory)?;
    let broker = CString::new(config.broker_id.as_str())?;
    let user = credential(&config.user_id_env, 16)?;
    let password = credential(&config.password_env, 41)?;
    let mut output = placement.on_background(|| {
        Output::new(
            &config,
            vec![user.to_str()?.to_owned(), password.to_str()?.to_owned()],
        )
    })?;
    let sdk_version = placement.on_sdk(|| version(&config.sdk_library))?;
    output.send(json!({"type":"runtime_start","received_us":crate::persistence::now_us(),"production_mode":config.production_mode,"sdk":sdk_version}))?;
    let mut session = Session::new(&config);
    let mut native: Option<Native> = None;
    let started = Instant::now();
    let mut progress = Instant::now();
    let mut next_connect = Instant::now();
    let mut request = 0i32;
    let mut ticks = 0u64;
    let result = (|| -> Result<()> {
        while !stop.load(Ordering::Acquire)
            && duration.is_none_or(|limit| started.elapsed() < limit)
        {
            ensure!(!output.failed.load(Ordering::Acquire), "MD output failed");
            if native.is_none() && Instant::now() >= next_connect {
                let transport = placement.on_sdk(|| {
                    let transport = Native::new(&config, placement.callback())?;
                    transport.start(&config)?;
                    Ok(transport)
                })?;
                native = Some(transport);
                progress = Instant::now();
                output.send(json!({"type":"connecting","received_us":crate::persistence::now_us(),"fronts":config.fronts}))?;
            }
            let mut work = false;
            if let Some(transport) = &native {
                ensure!(
                    !transport.placement_fault.load(Ordering::Acquire),
                    "MD SDK CPU/NUMA binding failed; no fallback is permitted"
                );
                ensure!(
                    !transport.failed.load(Ordering::Acquire),
                    "MD callback queue overflow; stream incomplete"
                );
                for _ in 0..256 {
                    let Ok((event, received_us)) = transport.events.try_recv() else {
                        break;
                    };
                    work = true;
                    match event.kind {
                        1 => {
                            request = request.checked_add(1).context("MD request IDs exhausted")?;
                            session.connected(request)?;
                            progress = Instant::now();
                            transport.login(request, &broker, &user, &password)?;
                            output.send(json!({"type":"connected","received_us":received_us,"generation":session.generation}))?;
                        }
                        2 => {
                            session.disconnect();
                            progress = Instant::now();
                            output.send(json!({"type":"disconnected","received_us":received_us,"reason":event.error,"gap":true}))?;
                        }
                        4 => {
                            if session.login(&event)? {
                                progress = Instant::now();
                                transport.subscribe()?;
                                output.send(json!({"type":"login_ok","received_us":received_us,"trading_day":super::super::text(&event.trading_day)?,"generation":session.generation}))?;
                            }
                        }
                        21 => {
                            session.subscribed(&event)?;
                            progress = Instant::now();
                            output.send(json!({"type":"subscribed","received_us":received_us,"symbol":super::super::text(&event.symbol)?,"ready":session.ready(),"generation":session.generation}))?;
                        }
                        22 => {
                            let venue = session.tick_venue(&event)?;
                            let tick =
                                TickRecord::capture(&event, received_us, session.generation, venue);
                            ticks = ticks.checked_add(1).context("MD tick count overflow")?;
                            output.send_tick(tick)?;
                        }
                        16 => bail!(
                            "MD error response {}; stop to avoid repeating invalid credentials",
                            event.error
                        ),
                        _ => {}
                    }
                }
            }
            if native.is_some()
                && !session.ready()
                && progress.elapsed() >= Duration::from_millis(config.response_timeout_ms)
            {
                native.take();
                session.disconnect();
                next_connect = Instant::now() + Duration::from_millis(config.reconnect_backoff_ms);
                output.send(json!({"type":"reconnecting","received_us":crate::persistence::now_us(),"reason":"connection/login/subscription timeout","gap":true}))?;
            }
            if !work {
                std::hint::spin_loop();
            }
        }
        Ok(())
    })();
    native.take();
    let result = result.and_then(|()| {
        ensure!(
            duration.is_none() || session.ready(),
            "MD session was not ready before the bounded run ended"
        );
        Ok(())
    });
    if let Err(error) = &result {
        let _=output.send(json!({"type":"stream_failed","received_us":crate::persistence::now_us(),"error":error.to_string(),"gap":true}));
    }
    output.send(json!({"type":"runtime_stopped","received_us":crate::persistence::now_us(),"connected":session.connected,"ready":session.ready(),"generations":session.generation,"ticks":ticks,"ok":result.is_ok()}))?;
    output.finish()?;
    result
}
