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
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
extern "C" {
    fn cn_yida_event_size() -> usize;
    fn cn_yida_version(path: *const c_char, out: *mut c_char, cap: usize) -> i32;
    fn cn_yida_create(
        lib: *const c_char,
        config: *const c_char,
        protocol: i32,
        group: i32,
        callback: unsafe extern "C" fn(*mut c_void, *const RawEvent),
        context: *mut c_void,
        error: *mut c_char,
        cap: usize,
    ) -> *mut c_void;
    fn cn_yida_destroy(handle: *mut c_void) -> i32;
    fn cn_yida_start(handle: *mut c_void) -> i32;
    fn cn_yida_allow_instrument(
        handle: *mut c_void,
        exchange: *const c_char,
        symbol: *const c_char,
    ) -> i32;
    fn cn_yida_login(
        handle: *mut c_void,
        user: *const c_char,
        password: *const c_char,
        app: *const c_char,
        auth: *const c_char,
    ) -> i32;
    fn cn_yida_reference(handle: *mut c_void) -> i32;
    fn cn_yida_insert(handle: *mut c_void, request: *const NativeOrder) -> i32;
    fn cn_yida_cancel(handle: *mut c_void, request: *const NativeCancel) -> i32;
    fn cn_yida_insert_batch(
        handle: *mut c_void,
        requests: *const NativeOrder,
        count: i32,
        failed: *mut i32,
    ) -> i32;
    fn cn_yida_cancel_batch(
        handle: *mut c_void,
        requests: *const NativeCancel,
        count: i32,
        failed: *mut i32,
    ) -> i32;
    fn cn_yida_transport_ready(handle: *mut c_void) -> i32;
    fn cn_yida_public_simulation_target(handle: *mut c_void) -> i32;
    fn cn_yida_disconnect(handle: *mut c_void);
}
#[repr(C)]
struct NativeOrder {
    exchange: [u8; 12],
    symbol: [u8; 32],
    reference: i32,
    direction: i32,
    offset: i32,
    tif: i32,
    market: i32,
    lots: i32,
    selection: i32,
    connection: i32,
    price: f64,
    option_market_buy_ceiling: f64,
}
#[repr(C)]
struct NativeCancel {
    exchange: [u8; 12],
    group: i32,
    reference: i32,
    selection: i32,
    connection: i32,
    system_id: i64,
}
fn native_text<const N: usize>(s: &str) -> Result<[u8; N]> {
    ensure!(
        s.is_ascii() && !s.as_bytes().contains(&0) && s.len() < N,
        "invalid native text field"
    );
    let mut out = [0; N];
    out[..s.len()].copy_from_slice(s.as_bytes());
    Ok(out)
}
fn native_route(route: ExecutionRoute) -> Result<(i32, i32)> {
    route.validate()?;
    Ok((
        match route.mode {
            ConnectionSelection::Any => 0,
            ConnectionSelection::Fixed => 1,
            ConnectionSelection::Preferred => 2,
            ConnectionSelection::Packed => 3,
        },
        i32::from(route.connection_id.unwrap_or(0)),
    ))
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
    pending_acks: HashMap<i32, Instant>,
    pending_cancels: HashMap<OrderKey, Instant>,
    order_group: i32,
    execution_route: ExecutionRoute,
    ack_timeout_reported: bool,
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
                config.trading_protocol.native_code(),
                i32::from(config.order_group),
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
        let trader = Self {
            handle,
            context: Some(context),
            events,
            fault,
            credentials,
            references: HashMap::new(),
            pending_acks: HashMap::new(),
            pending_cancels: HashMap::new(),
            order_group: i32::from(config.order_group),
            execution_route: config.execution_route,
            ack_timeout_reported: false,
            _single_owner: PhantomData,
        };
        for contract in &config.symbols {
            let venue = CString::new(contract.venue.as_str())?;
            let symbol = CString::new(contract.symbol.as_str())?;
            ensure!(
                unsafe {
                    cn_yida_allow_instrument(
                        trader.handle.as_ptr(),
                        venue.as_ptr(),
                        symbol.as_ptr(),
                    )
                } == 0,
                "native instrument allowlist registration failed"
            );
        }
        if config.public_simulation {
            ensure!(
                unsafe { cn_yida_public_simulation_target(trader.handle.as_ptr()) } == 1,
                "SDK did not load the official public-simulation endpoint"
            );
        }
        Ok(trader)
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
    pub fn transport_ready(&self) -> bool {
        unsafe { cn_yida_transport_ready(self.handle.as_ptr()) == 1 }
    }
    pub fn ensure_transport_ready(&self) -> Result<()> {
        ensure!(
            self.transport_ready(),
            "configured native trading transport unavailable; no send attempted"
        );
        Ok(())
    }
    pub fn observe_order(&mut self, e: &RawEvent) -> Result<()> {
        if e.kind == ORDER {
            if let Some((venue, key)) = self.references.get(&e.reference) {
                if e.order_group == self.order_group
                    && *venue == e.venue()?
                    && key.session == group_session_key(e.day, self.order_group)
                {
                    self.pending_acks.remove(&e.reference);
                }
            }
            if matches!(e.status, 2..=4) {
                let key = self.key(e)?;
                self.pending_cancels.remove(&key);
            }
        }
        Ok(())
    }
    pub fn expired_response(&mut self, now: Instant, timeout: Duration) -> Option<ResponseTimeout> {
        if self.ack_timeout_reported {
            return None;
        }
        let expired = self
            .pending_acks
            .iter()
            .find(|(_, sent)| now.duration_since(**sent) >= timeout)
            .map(|(reference, _)| ResponseTimeout::Submit {
                order_ref: *reference,
            })
            .or_else(|| {
                self.pending_cancels
                    .iter()
                    .find(|(_, sent)| now.duration_since(**sent) >= timeout)
                    .map(|(key, _)| ResponseTimeout::Cancel { key: key.clone() })
            });
        self.ack_timeout_reported = expired.is_some();
        expired
    }
    pub fn recovered(&mut self) {
        self.ack_timeout_reported = false;
        // A complete snapshot restores current working/terminal orders. It never
        // releases their position/cash reservations or resends a cancellation.
        self.pending_cancels.clear();
    }
    pub fn key(&self, e: &RawEvent) -> Result<OrderKey> {
        ensure!(
            (0..=255).contains(&e.order_group),
            "unsupported native order group; evidence retained"
        );
        if let Some((venue, key)) = self.references.get(&e.reference) {
            if e.order_group == self.order_group
                && *venue == e.venue()?
                && key.session == group_session_key(e.day, self.order_group)
            {
                return Ok(key.clone());
            }
        }
        e.recovered_key()
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
        let request = self.order_request(o)?;
        self.register_order(o)?;
        let result = send_result(unsafe { cn_yida_insert(self.handle.as_ptr(), &request) });
        if matches!(result,Ok(code) if code>0) {
            self.pending_acks.remove(&request.reference);
        }
        result
    }
    fn cancel(&mut self, o: &PreparedCancel) -> Result<i32> {
        let request = self.cancel_request(o)?;
        self.pending_cancels.insert(o.key.clone(), Instant::now());
        let result = send_result(unsafe { cn_yida_cancel(self.handle.as_ptr(), &request) });
        if matches!(result,Ok(code) if code>0) {
            self.pending_cancels.remove(&o.key);
        }
        result
    }
    fn insert_batch(&mut self, orders: &[PreparedOrder]) -> Result<BatchSend> {
        let requests = orders
            .iter()
            .map(|o| self.order_request(o))
            .collect::<Result<Vec<_>>>()?;
        for o in orders {
            self.register_order(o)?;
        }
        let mut failed = -1;
        let result = batch_result(
            unsafe {
                cn_yida_insert_batch(
                    self.handle.as_ptr(),
                    requests.as_ptr(),
                    requests.len() as i32,
                    &mut failed,
                )
            },
            failed,
            orders.len(),
        );
        if matches!(result, Ok(BatchSend::RejectedBeforeSend { .. })) {
            for r in &requests {
                self.pending_acks.remove(&r.reference);
            }
        }
        result
    }
    fn cancel_batch(&mut self, orders: &[PreparedCancel]) -> Result<BatchSend> {
        let requests = orders
            .iter()
            .map(|o| self.cancel_request(o))
            .collect::<Result<Vec<_>>>()?;
        for o in orders {
            self.pending_cancels.insert(o.key.clone(), Instant::now());
        }
        let mut failed = -1;
        let result = batch_result(
            unsafe {
                cn_yida_cancel_batch(
                    self.handle.as_ptr(),
                    requests.as_ptr(),
                    requests.len() as i32,
                    &mut failed,
                )
            },
            failed,
            orders.len(),
        );
        if matches!(result, Ok(BatchSend::RejectedBeforeSend { .. })) {
            for o in orders {
                self.pending_cancels.remove(&o.key);
            }
        }
        result
    }
}
impl YidaTrader {
    fn order_request(&self, o: &PreparedOrder) -> Result<NativeOrder> {
        let (day, group, reference) = parse_group_key(&o.key)?;
        ensure!(
            group == self.order_group && o.key.session == group_session_key(day, group),
            "submission group differs from runtime"
        );
        let (selection, connection) =
            native_route(o.intent.execution_route.unwrap_or(self.execution_route))?;
        Ok(NativeOrder {
            option_market_buy_ceiling: o.option_market_buy_ceiling,
            exchange: native_text(o.intent.venue.as_str())?,
            symbol: native_text(&o.intent.symbol)?,
            reference,
            direction: if o.intent.side == Side::Buy { 0 } else { 1 },
            offset: match o.intent.offset {
                Offset::Open => 0,
                Offset::CloseToday => 3,
                Offset::CloseYesterday => 4,
                Offset::Close => anyhow::bail!("unplanned SHFE/INE close"),
            },
            tif: match o.intent.time_in_force {
                TimeInForce::Day => 0,
                TimeInForce::Fak => 1,
                TimeInForce::Fok => 3,
            },
            market: i32::from(o.intent.price_type == PriceType::Market),
            lots: o.intent.lots,
            selection,
            connection,
            price: o.limit_price,
        })
    }
    fn register_order(&mut self, o: &PreparedOrder) -> Result<()> {
        let reference = o.key.order_ref.parse::<i32>()?;
        ensure!(
            self.references.len() < 200000 && !self.references.contains_key(&reference),
            "native reference correlation capacity/identity invalid"
        );
        self.references
            .insert(reference, (o.intent.venue, o.key.clone()));
        self.pending_acks.insert(reference, Instant::now());
        Ok(())
    }
    fn cancel_request(&self, o: &PreparedCancel) -> Result<NativeCancel> {
        let (group, reference, system_id) =
            if let Ok((_, group, reference)) = parse_group_key(&o.key) {
                (group, reference, 0)
            } else {
                let system_id = o
                    .system_id
                    .parse::<i64>()
                    .context("external order cancellation requires a positive system ID")?;
                ensure!(
                    system_id > 0,
                    "external order cancellation requires a positive system ID"
                );
                (0, 0, system_id)
            };
        let (selection, connection) =
            native_route(o.execution_route.unwrap_or(self.execution_route))?;
        Ok(NativeCancel {
            exchange: native_text(o.venue.as_str())?,
            group,
            reference,
            selection,
            connection,
            system_id,
        })
    }
}
fn batch_result(code: i32, failed: i32, count: usize) -> Result<BatchSend> {
    send_result(code)?;
    if code == 0 {
        return Ok(BatchSend::SubmittedLocally);
    }
    ensure!(
        failed >= 0 && (failed as usize) < count,
        "invalid native batch rejection index"
    );
    Ok(BatchSend::RejectedBeforeSend {
        index: failed as usize,
        code,
    })
}
#[derive(Debug, serde::Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ResponseTimeout {
    Submit { order_ref: i32 },
    Cancel { key: OrderKey },
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
