use super::*;
use anyhow::{ensure, Context, Result};
use crossbeam_channel::{Receiver, Sender};
use std::{
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
    #[cfg(feature = "latency-metrics")]
    pub received_at: Instant,
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
    #[cfg(feature = "latency-metrics")]
    let received_at = Instant::now();
    let event = unsafe { *event };
    let received_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;
    if context
        .sender
        .try_send(Received {
            event,
            received_us,
            #[cfg(feature = "latency-metrics")]
            received_at,
        })
        .is_err()
    {
        context.fault.store(true, Ordering::Release);
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct PendingIdentity {
    venue: Venue,
    day: i32,
    group: i32,
    reference: i32,
    system_id: i64,
    cancel: bool,
}
impl PendingIdentity {
    fn submit(venue: Venue, day: i32, group: i32, reference: i32) -> Self {
        Self {
            venue,
            day,
            group,
            reference,
            system_id: 0,
            cancel: false,
        }
    }
    fn cancel(venue: Venue, day: i32, group: i32, reference: i32, system_id: i64) -> Self {
        Self {
            venue,
            day,
            group,
            reference: if group == 0 { 0 } else { reference },
            system_id: if group == 0 { system_id } else { 0 },
            cancel: true,
        }
    }
}
pub struct YidaTrader {
    handle: NonNull<c_void>,
    #[cfg(feature = "latency-metrics")]
    last_insert_ns: Option<u64>,
    context: Option<Box<ContextData>>,
    pub events: Receiver<Received>,
    pub fault: Arc<AtomicBool>,
    credentials: config::Credentials,
    pending: crate::deadline::DeadlineQueue<PendingIdentity, Option<OrderKey>>,
    response_timeout: Duration,
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
            #[cfg(feature = "latency-metrics")]
            last_insert_ns: None,
            context: Some(context),
            events,
            fault,
            credentials,
            pending: Default::default(),
            response_timeout: Duration::from_millis(config.response_timeout_ms),
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
    pub fn observe_order(&mut self, e: &RawEvent, order: &OrderUpdate) -> Result<()> {
        if e.kind == ORDER {
            let venue = order.venue;
            self.pending.remove(&PendingIdentity::submit(
                venue,
                e.day,
                e.order_group,
                e.reference,
            ));
            if matches!(e.status, 2..=4) {
                self.pending.remove(&PendingIdentity::cancel(
                    venue,
                    e.day,
                    e.order_group,
                    e.reference,
                    e.system_id,
                ));
            }
        }
        Ok(())
    }
    pub fn next_response_deadline(&self) -> Option<Instant> {
        if self.ack_timeout_reported {
            None
        } else {
            self.pending.next_deadline()
        }
    }
    #[cfg(feature = "latency-metrics")]
    pub fn take_insert_ns(&mut self) -> Option<u64> {
        self.last_insert_ns.take()
    }
    pub fn pending_response_count(&self) -> usize {
        self.pending.len()
    }
    pub fn expired_response(&mut self, now: Instant) -> Option<ResponseTimeout> {
        if self.ack_timeout_reported {
            return None;
        }
        let (identity, key) = self.pending.pop_due(now)?;
        self.ack_timeout_reported = true;
        Some(if identity.cancel {
            ResponseTimeout::Cancel {
                key: key.expect("cancel requests retain their native identity"),
            }
        } else {
            ResponseTimeout::Submit {
                order_ref: identity.reference,
            }
        })
    }
    pub fn recovered(&mut self) {
        self.ack_timeout_reported = false;
        // Complete, durably validated recovery has accounted for every unresolved request.
        self.pending.clear();
    }
    pub fn key(&self, e: &RawEvent) -> Result<OrderKey> {
        // Nonzero groups carry the complete day/group/reference identity. Keeping a
        // second historical reference table added no correlation information.
        e.recovered_key()
    }
    fn register_order(&mut self, o: &PreparedOrder) -> Result<PendingIdentity> {
        let (day, group, reference) = parse_group_key(&o.key)?;
        let id = PendingIdentity::submit(o.intent.venue, day, group, reference);
        ensure!(
            self.pending
                .insert(
                    id,
                    Instant::now()
                        .checked_add(self.response_timeout)
                        .context("native response deadline overflow")?,
                    None
                )
                .is_ok(),
            "duplicate pending native submission identity"
        );
        Ok(id)
    }
    fn register_cancel(&mut self, o: &PreparedCancel, r: &NativeCancel) -> Result<PendingIdentity> {
        let day = o
            .key
            .session
            .split(':')
            .nth(1)
            .context("missing native cancel day")?
            .parse()?;
        let id = PendingIdentity::cancel(o.venue, day, r.group, r.reference, r.system_id);
        ensure!(
            self.pending
                .insert(
                    id,
                    Instant::now()
                        .checked_add(self.response_timeout)
                        .context("native response deadline overflow")?,
                    Some(o.key.clone())
                )
                .is_ok(),
            "duplicate pending native cancellation identity"
        );
        Ok(id)
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
        // The server maximum excludes requests rejected before sending. Consume those
        // references again locally after a restart, never reuse their durable identities.
        let mut reference = unsafe { cn_yida_reference(self.handle.as_ptr()) };
        while reference > 0 && (reference as u64) < minimum {
            let next = unsafe { cn_yida_reference(self.handle.as_ptr()) };
            ensure!(
                next > reference,
                "native reference allocator did not advance"
            );
            reference = next;
        }
        ensure!(
            reference > 0 && reference as u64 >= minimum,
            "native allocator returned exhausted/non-monotone reference"
        );
        Ok(reference as u64)
    }
    fn insert(&mut self, o: &PreparedOrder) -> Result<i32> {
        let request = self.order_request(o)?;
        let pending = self.register_order(o)?;
        #[cfg(feature = "latency-metrics")]
        let before = Instant::now();
        let code = unsafe { cn_yida_insert(self.handle.as_ptr(), &request) };
        #[cfg(feature = "latency-metrics")]
        {
            self.last_insert_ns = Some(before.elapsed().as_nanos() as u64);
        }
        let result = send_result(code);
        if matches!(result,Ok(code) if code>0) {
            self.pending.remove(&pending);
        }
        result
    }
    fn cancel(&mut self, o: &PreparedCancel) -> Result<i32> {
        let request = self.cancel_request(o)?;
        let pending = self.register_cancel(o, &request)?;
        let result = send_result(unsafe { cn_yida_cancel(self.handle.as_ptr(), &request) });
        if matches!(result,Ok(code) if code>0) {
            self.pending.remove(&pending);
        }
        result
    }
    fn insert_batch(&mut self, orders: &[PreparedOrder]) -> Result<BatchSend> {
        let requests = orders
            .iter()
            .map(|o| self.order_request(o))
            .collect::<Result<Vec<_>>>()?;
        let pending = orders
            .iter()
            .map(|o| self.register_order(o))
            .collect::<Result<Vec<_>>>()?;
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
            for id in &pending {
                self.pending.remove(id);
            }
        }
        result
    }
    fn cancel_batch(&mut self, orders: &[PreparedCancel]) -> Result<BatchSend> {
        let requests = orders
            .iter()
            .map(|o| self.cancel_request(o))
            .collect::<Result<Vec<_>>>()?;
        let pending = orders
            .iter()
            .zip(&requests)
            .map(|(o, r)| self.register_cancel(o, r))
            .collect::<Result<Vec<_>>>()?;
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
            for id in &pending {
                self.pending.remove(id);
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
            self.fault.store(true, Ordering::Release);
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
    fn deadline_identity_is_scoped_by_venue_day_group_and_operation() {
        let now = Instant::now();
        let mut pending = crate::deadline::DeadlineQueue::default();
        let identities = [
            PendingIdentity::submit(Venue::Shfe, 20261110, 1, 17),
            PendingIdentity::submit(Venue::Ine, 20261110, 1, 17),
            PendingIdentity::submit(Venue::Shfe, 20261111, 1, 17),
            PendingIdentity::submit(Venue::Shfe, 20261110, 2, 17),
            PendingIdentity::cancel(Venue::Shfe, 20261110, 1, 17, 900),
            PendingIdentity::cancel(Venue::Shfe, 20261110, 0, 0, 900),
            PendingIdentity::cancel(Venue::Shfe, 20261110, 0, 0, 901),
        ];
        for (n, id) in identities.iter().enumerate() {
            pending.insert(*id, now, n).unwrap();
        }
        // Nonzero groups use the stable reference even when a later report adds a system ID.
        assert_eq!(
            pending.remove(&PendingIdentity::cancel(Venue::Shfe, 20261110, 1, 17, 999)),
            Some(4)
        );
        for (n, id) in identities.iter().enumerate() {
            if n != 4 {
                assert_eq!(pending.remove(id), Some(n));
            }
        }
        assert!(pending.is_empty());
    }
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
