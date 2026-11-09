use super::*;
use anyhow::{ensure, Context, Result};
use crossbeam_channel::{Receiver, Sender};
use std::{
    collections::HashMap,
    ffi::{c_char, c_void, CStr, CString},
    marker::PhantomData,
    path::Path,
    ptr::NonNull,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
extern "C" {
    fn cn_yida_event_size() -> usize;
    fn cn_yida_version(path: *const c_char, out: *mut c_char, cap: usize) -> i32;
    fn cn_yida_create(
        lib: *const c_char,
        config: *const c_char,
        callback: unsafe extern "C" fn(*mut c_void, *const RawEvent),
        context: *mut c_void,
        error: *mut c_char,
        cap: usize,
    ) -> *mut c_void;
    fn cn_yida_destroy(handle: *mut c_void) -> i32;
    fn cn_yida_start(handle: *mut c_void) -> i32;
    fn cn_yida_login(
        handle: *mut c_void,
        user: *const c_char,
        password: *const c_char,
        app: *const c_char,
        auth: *const c_char,
    ) -> i32;
    fn cn_yida_reference(handle: *mut c_void) -> i32;
    fn cn_yida_insert(
        handle: *mut c_void,
        exchange: *const c_char,
        symbol: *const c_char,
        reference: i32,
        direction: i32,
        offset: i32,
        tif: i32,
        lots: i32,
        price: f64,
    ) -> i32;
    fn cn_yida_cancel(handle: *mut c_void, exchange: *const c_char, system_id: i32) -> i32;
    fn cn_yida_disconnect(handle: *mut c_void);
}
fn path_string(p: &Path) -> Result<CString> {
    CString::new(p.to_str().context("Yida path must be UTF-8")?).context("invalid Yida path")
}
pub fn version(path: &Path) -> Result<String> {
    let path = path_string(path)?;
    let mut out = [0i8; 512];
    let code = unsafe { cn_yida_version(path.as_ptr(), out.as_mut_ptr(), out.len()) };
    let text = unsafe { CStr::from_ptr(out.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    ensure!(code == 0, "Yida library version check failed: {text}");
    Ok(text)
}
#[derive(Clone, Copy)]
pub struct Received {
    pub event: RawEvent,
    pub received_us: u64,
}
struct ContextData {
    sender: Sender<Received>,
    fault: Arc<AtomicBool>,
}
unsafe extern "C" fn callback(context: *mut c_void, event: *const RawEvent) {
    if context.is_null() || event.is_null() {
        return;
    }
    let context = unsafe { &*context.cast::<ContextData>() };
    let event = unsafe { *event };
    let received_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;
    if context
        .sender
        .try_send(Received { event, received_us })
        .is_err()
    {
        context.fault.store(true, Ordering::Release);
    }
}
pub struct YidaTrader {
    handle: NonNull<c_void>,
    context: Option<Box<ContextData>>,
    pub events: Receiver<Received>,
    pub fault: Arc<AtomicBool>,
    credentials: config::Credentials,
    references: HashMap<i32, (Venue, OrderKey)>,
    _single_owner: PhantomData<Rc<()>>,
}
impl YidaTrader {
    pub fn new(config: &config::Config, credentials: config::Credentials) -> Result<Self> {
        config.validate_sdk_config()?;
        ensure!(
            unsafe { cn_yida_event_size() } == std::mem::size_of::<RawEvent>(),
            "Yida FFI event layout mismatch"
        );
        let (sender, events) = crossbeam_channel::bounded(config.event_capacity);
        let fault = Arc::new(AtomicBool::new(false));
        let mut context = Box::new(ContextData {
            sender,
            fault: fault.clone(),
        });
        let lib = path_string(&config.sdk_library)?;
        let native_config = path_string(&config.sdk_config)?;
        let mut error = [0i8; 512];
        let raw = unsafe {
            cn_yida_create(
                lib.as_ptr(),
                native_config.as_ptr(),
                callback,
                (&mut *context as *mut ContextData).cast(),
                error.as_mut_ptr(),
                error.len(),
            )
        };
        let handle = NonNull::new(raw).ok_or_else(|| {
            anyhow::anyhow!(
                "Yida SDK creation failed: {}",
                unsafe { CStr::from_ptr(error.as_ptr()) }.to_string_lossy()
            )
        })?;
        Ok(Self {
            handle,
            context: Some(context),
            events,
            fault,
            credentials,
            references: HashMap::new(),
            _single_owner: PhantomData,
        })
    }
    pub fn start(&mut self) -> Result<()> {
        ensure!(
            unsafe { cn_yida_start(self.handle.as_ptr()) } == 0,
            "native Yida start failed"
        );
        Ok(())
    }
    pub fn login(&mut self) -> Result<()> {
        let c = &self.credentials;
        ensure!(
            unsafe {
                cn_yida_login(
                    self.handle.as_ptr(),
                    c.user.as_ptr(),
                    c.password.as_ptr(),
                    c.app_id.as_ptr(),
                    c.auth_code.as_ptr(),
                )
            } == 0,
            "native Yida login dispatch failed; no automatic retry"
        );
        Ok(())
    }
    pub fn reconnect(&mut self) {
        unsafe { cn_yida_disconnect(self.handle.as_ptr()) };
    }
    pub fn key(&self, e: &RawEvent) -> Result<OrderKey> {
        if let Some((venue, key)) = self.references.get(&e.reference) {
            if *venue == e.venue()? && key.session == session_key(e.day) {
                return Ok(key.clone());
            }
        }
        e.recovered_key()
    }
    pub fn failed_key(&self, e: &RawEvent) -> Result<OrderKey> {
        let (venue, key) = self
            .references
            .get(&e.reference)
            .context("unmatched native failed-order reference")?;
        ensure!(
            *venue == e.venue()? && key.session == session_key(e.day),
            "failed-order venue/day mismatch"
        );
        Ok(key.clone())
    }
}
fn send_result(code: i32) -> Result<i32> {
    ensure!(
        code >= 0,
        "native send/bridge outcome uncertain; retain reservation and reconcile"
    );
    Ok(code)
}
impl Trader for YidaTrader {
    fn allocate_reference(&mut self, minimum: u64) -> Result<u64> {
        let reference = unsafe { cn_yida_reference(self.handle.as_ptr()) };
        ensure!(
            reference > 0 && reference as u64 >= minimum,
            "native allocator returned exhausted/non-monotone reference"
        );
        Ok(reference as u64)
    }
    fn insert(&mut self, o: &PreparedOrder) -> Result<i32> {
        ensure!(
            self.references.len() < 200_000,
            "native reference correlation capacity exhausted"
        );
        let reference = o
            .key
            .order_ref
            .parse::<i32>()
            .context("native reference exceeds positive int")?;
        ensure!(
            reference > 0 && !self.references.contains_key(&reference),
            "duplicate native reference"
        );
        self.references
            .insert(reference, (o.intent.venue, o.key.clone()));
        let symbol = CString::new(o.intent.symbol.as_str())?;
        let exchange = CString::new(o.intent.venue.as_str())?;
        let offset = match o.intent.offset {
            Offset::Open => 0,
            Offset::CloseToday => 3,
            Offset::CloseYesterday => 4,
            Offset::Close => anyhow::bail!("unplanned SHFE/INE close"),
        };
        let tif = match o.intent.time_in_force {
            TimeInForce::Day => 0,
            TimeInForce::Fak => 1,
            TimeInForce::Fok => 3,
        };
        send_result(unsafe {
            cn_yida_insert(
                self.handle.as_ptr(),
                exchange.as_ptr(),
                symbol.as_ptr(),
                reference,
                match o.intent.side {
                    Side::Buy => 0,
                    Side::Sell => 1,
                },
                offset,
                tif,
                o.intent.lots,
                o.limit_price,
            )
        })
    }
    fn cancel(&mut self, o: &PreparedCancel) -> Result<i32> {
        let exchange = CString::new(o.venue.as_str())?;
        let id = o
            .system_id
            .parse::<i32>()
            .context("invalid native system ID")?;
        ensure!(id > 0, "native cancel requires a positive system ID");
        send_result(unsafe { cn_yida_cancel(self.handle.as_ptr(), exchange.as_ptr(), id) })
    }
}
impl Drop for YidaTrader {
    fn drop(&mut self) {
        let code = unsafe { cn_yida_destroy(self.handle.as_ptr()) };
        if code != 0 {
            if let Some(c) = self.context.take() {
                Box::leak(c);
            }
            eprintln!("Yida asynchronous SDK destruction did not finish; callback memory retained until process exit");
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn callback_overflow_latches_fault() {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let fault = Arc::new(AtomicBool::new(false));
        let mut context = ContextData {
            sender,
            fault: fault.clone(),
        };
        let e = RawEvent::default();
        unsafe {
            callback((&mut context as *mut ContextData).cast(), &e);
            callback((&mut context as *mut ContextData).cast(), &e);
        }
        assert!(fault.load(Ordering::Acquire));
        assert_eq!(receiver.len(), 1);
    }
}
