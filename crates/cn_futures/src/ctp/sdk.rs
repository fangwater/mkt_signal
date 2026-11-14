use super::{
    ingress::{ControlPool, Event},
    HotEvent, RawEvent,
};
use crate::config::{Config, Credentials};
use crate::model::*;
use crate::placement::CallbackPlacement;
use anyhow::{ensure, Context, Result};
use crossbeam_channel::{Receiver, Sender};
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::marker::PhantomData;
use std::path::Path;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

extern "C" {
    fn nf_ctp_event_size() -> usize;
    fn nf_ctp_hot_event_size() -> usize;
    fn nf_ctp_version(path: *const c_char, out: *mut c_char, cap: usize) -> i32;
    fn nf_ctp_create(
        path: *const c_char,
        flow: *const c_char,
        production: bool,
        cb: unsafe extern "C" fn(*mut c_void, *const HotEvent, *const RawEvent),
        context: *mut c_void,
        error: *mut c_char,
        cap: usize,
    ) -> *mut c_void;
    fn nf_ctp_destroy(h: *mut c_void);
    fn nf_ctp_start(h: *mut c_void, front: *const c_char) -> i32;
    fn nf_ctp_authenticate(
        h: *mut c_void,
        id: i32,
        broker: *const c_char,
        user: *const c_char,
        app: *const c_char,
        auth: *const c_char,
    ) -> i32;
    fn nf_ctp_login(
        h: *mut c_void,
        id: i32,
        broker: *const c_char,
        user: *const c_char,
        password: *const c_char,
    ) -> i32;
    fn nf_ctp_settlement(
        h: *mut c_void,
        id: i32,
        broker: *const c_char,
        investor: *const c_char,
    ) -> i32;
    fn nf_ctp_query(
        h: *mut c_void,
        kind: i32,
        id: i32,
        broker: *const c_char,
        investor: *const c_char,
        exchange: *const c_char,
        symbol: *const c_char,
    ) -> i32;
    fn nf_ctp_prepare(
        h: *mut c_void,
        slot: u32,
        broker: *const c_char,
        investor: *const c_char,
        user: *const c_char,
        exchange: *const c_char,
        symbol: *const c_char,
    ) -> i32;
    fn nf_ctp_insert(
        h: *mut c_void,
        slot: u32,
        id: i32,
        reference: u64,
        direction: i32,
        offset: i32,
        tif: i32,
        lots: i32,
        price: f64,
    ) -> i32;
    fn nf_ctp_cancel(
        h: *mut c_void,
        slot: u32,
        id: i32,
        reference: u64,
        system: *const u8,
        length: usize,
        front: i32,
        session: i32,
    ) -> i32;
}

#[derive(Clone)]
pub struct Received {
    pub event: Event,
    pub received_us: u64,
}
struct CallbackContext {
    sender: Sender<Received>,
    fault: Arc<AtomicBool>,
    control: Arc<ControlPool>,
    placement: Option<Arc<CallbackPlacement>>,
    placement_fault: Arc<AtomicBool>,
}

/// Bounded queue handoff with placement enforcement; no business parsing or heap allocation.
unsafe extern "C" fn callback(
    context: *mut c_void,
    hot: *const HotEvent,
    control: *const RawEvent,
) {
    if context.is_null() {
        return;
    }
    let context = unsafe { &*context.cast::<CallbackContext>() };
    if let Some(placement) = &context.placement {
        if !placement.enforce() {
            context.placement_fault.store(true, Ordering::Release);
            context.fault.store(true, Ordering::Release);
            return;
        }
    }
    let received_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;
    let event = if !hot.is_null() && control.is_null() {
        Event::Hot(unsafe { *hot })
    } else if hot.is_null() && !control.is_null() {
        let Some(event) = context.control.capture(unsafe { &*control }) else {
            context.fault.store(true, Ordering::Release);
            return;
        };
        Event::Control(event)
    } else {
        context.fault.store(true, Ordering::Release);
        return;
    };
    if context
        .sender
        .try_send(Received { event, received_us })
        .is_err()
    {
        context.fault.store(true, Ordering::Release);
    }
}

fn path_string(path: &Path) -> Result<CString> {
    CString::new(path.to_str().context("CTP path must be UTF-8")?).context("CTP path contains NUL")
}
fn local_result(code: i32) -> Result<i32> {
    ensure!(
        matches!(code, -3..=0),
        "unknown SDK/bridge local result {code}; submission outcome must be reconciled"
    );
    Ok(code)
}

pub fn version(path: &Path) -> Result<String> {
    let path = path_string(path)?;
    let mut output = [0i8; 512];
    let code = unsafe { nf_ctp_version(path.as_ptr(), output.as_mut_ptr(), output.len()) };
    let message = unsafe { CStr::from_ptr(output.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    ensure!(code == 0, "cannot load CTP SDK: {message}");
    Ok(message)
}

pub struct CtpTrader {
    handle: NonNull<c_void>,
    context: Box<CallbackContext>,
    pub events: Receiver<Received>,
    pub fault: Arc<AtomicBool>,
    pub placement_fault: Arc<AtomicBool>,
    credentials: Credentials,
    next_request: i32,
    contracts: [HashMap<String, (u32, bool)>; 6],
    next_contract: u32,
    correlation: super::ResponseCorrelation,
    _single_owner: PhantomData<Rc<()>>,
}
impl CtpTrader {
    /// Creating this object does not connect. start() is the explicit network boundary.
    pub fn new(
        config: &Config,
        credentials: Credentials,
        placement: Arc<CallbackPlacement>,
    ) -> Result<Self> {
        ensure!(
            unsafe { nf_ctp_event_size() } == std::mem::size_of::<RawEvent>()
                && unsafe { nf_ctp_hot_event_size() } == std::mem::size_of::<HotEvent>(),
            "CTP FFI event layout mismatch"
        );
        std::fs::create_dir_all(&config.flow_directory).context("create CTP flow directory")?;
        let (sender, events) = crossbeam_channel::bounded(config.event_capacity);
        let fault = Arc::new(AtomicBool::new(false));
        let placement_fault = Arc::new(AtomicBool::new(false));
        let mut context = Box::new(CallbackContext {
            sender,
            fault: fault.clone(),
            control: ControlPool::new(
                config
                    .event_capacity
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(256))
                    .context("CTP control pool capacity overflow")?,
            )?,
            placement: Some(placement),
            placement_fault: placement_fault.clone(),
        });
        let path = path_string(&config.sdk_library)?;
        let mut flow = config.flow_directory.to_string_lossy().into_owned();
        if !flow.ends_with('/') {
            flow.push('/');
        }
        let flow = CString::new(flow).context("invalid CTP flow path")?;
        let mut error = [0i8; 512];
        let raw = unsafe {
            nf_ctp_create(
                path.as_ptr(),
                flow.as_ptr(),
                config.production_mode,
                callback,
                (&mut *context as *mut CallbackContext).cast(),
                error.as_mut_ptr(),
                error.len(),
            )
        };
        let handle = NonNull::new(raw).ok_or_else(|| {
            anyhow::anyhow!(
                "CTP SDK initialization failed: {}",
                unsafe { CStr::from_ptr(error.as_ptr()) }.to_string_lossy()
            )
        })?;
        let mut trader = Self {
            handle,
            context,
            events,
            fault,
            credentials,
            placement_fault,
            next_request: 1,
            contracts: std::array::from_fn(|_| HashMap::new()),
            next_contract: 0,
            correlation: super::ResponseCorrelation::default(),
            _single_owner: PhantomData,
        };
        trader.correlation.reserve(config.preallocated_requests)?;
        for symbol in &config.symbols {
            trader.prepare_contract(symbol.venue, &symbol.symbol, true)?;
        }
        Ok(trader)
    }
    fn prepare_contract(&mut self, venue: Venue, symbol: &str, can_insert: bool) -> Result<u32> {
        let slot = self.next_contract;
        let exchange = CString::new(venue.as_str())?;
        let symbol_c = CString::new(symbol)?;
        let c = &self.credentials;
        ensure!(
            unsafe {
                nf_ctp_prepare(
                    self.handle.as_ptr(),
                    slot,
                    c.broker.as_ptr(),
                    c.investor.as_ptr(),
                    c.user.as_ptr(),
                    exchange.as_ptr(),
                    symbol_c.as_ptr(),
                )
            } == 0,
            "prepare CTP contract request templates failed"
        );
        self.next_contract = self
            .next_contract
            .checked_add(1)
            .context("CTP template slots exhausted")?;
        self.contracts[venue as usize].insert(symbol.to_owned(), (slot, can_insert));
        Ok(slot)
    }
    pub fn start(&mut self, front: &str) -> Result<()> {
        let front = CString::new(front).context("invalid CTP front")?;
        let result = unsafe { nf_ctp_start(self.handle.as_ptr(), front.as_ptr()) };
        ensure!(result == 0, "CTP SDK start failed {result}");
        Ok(())
    }
    pub fn next_id(&mut self) -> Result<i32> {
        let id = self.next_request;
        self.next_request = id.checked_add(1).context("CTP request ID exhausted")?;
        Ok(id)
    }
    pub fn inherit_requests(&mut self, previous: &mut Self) {
        self.next_request = previous.next_request;
        std::mem::swap(&mut self.correlation, &mut previous.correlation);
    }
    pub fn control(&mut self, kind: i32, id: i32, target: Option<(Venue, &str)>) -> Result<i32> {
        let c = &self.credentials;
        let exchange = CString::new(target.map(|t| t.0.as_str()).unwrap_or(""))?;
        let symbol = CString::new(target.map(|t| t.1).unwrap_or(""))?;
        local_result(unsafe {
            match kind {
                super::AUTHENTICATED => nf_ctp_authenticate(
                    self.handle.as_ptr(),
                    id,
                    c.broker.as_ptr(),
                    c.user.as_ptr(),
                    c.app_id.as_ptr(),
                    c.auth_code.as_ptr(),
                ),
                super::LOGGED_IN => nf_ctp_login(
                    self.handle.as_ptr(),
                    id,
                    c.broker.as_ptr(),
                    c.user.as_ptr(),
                    c.password.as_ptr(),
                ),
                super::SETTLED => nf_ctp_settlement(
                    self.handle.as_ptr(),
                    id,
                    c.broker.as_ptr(),
                    c.investor.as_ptr(),
                ),
                _ => nf_ctp_query(
                    self.handle.as_ptr(),
                    kind,
                    id,
                    c.broker.as_ptr(),
                    c.investor.as_ptr(),
                    exchange.as_ptr(),
                    symbol.as_ptr(),
                ),
            }
        })
    }
    pub fn investor(&self) -> &CStr {
        &self.credentials.investor
    }
    pub fn response_key(&self, event: &HotEvent) -> Result<OrderKey> {
        self.correlation.resolve(event)
    }
}
impl Trader for CtpTrader {
    fn insert(&mut self, order: &PreparedOrder) -> Result<i32> {
        ensure!(
            order.intent.price_type == PriceType::Limit && order.intent.execution_route.is_none(),
            "CTP native market orders are not enabled"
        );
        let slot = self.contracts[order.intent.venue as usize]
            .get(order.intent.symbol.as_str())
            .filter(|(_, insert)| *insert)
            .context("contract absent from configured CTP insertion templates")?
            .0;
        let reference = order
            .key
            .order_ref
            .parse::<u64>()
            .context("invalid local CTP reference")?;
        let (front, session) = super::parse_session(&order.key.session)?;
        let id = self.next_id()?;
        self.correlation
            .register_numeric(id, super::INSERT_RESPONSE, front, session, reference)?;
        local_result(unsafe {
            nf_ctp_insert(
                self.handle.as_ptr(),
                slot,
                id,
                reference,
                match order.intent.side {
                    Side::Buy => 48,
                    Side::Sell => 49,
                },
                match order.intent.offset {
                    Offset::Open => 48,
                    Offset::Close => 49,
                    Offset::CloseToday => 51,
                    Offset::CloseYesterday => 52,
                },
                match order.intent.time_in_force {
                    TimeInForce::Day => 0,
                    TimeInForce::Fak => 1,
                    TimeInForce::Fok => 2,
                },
                order.intent.lots,
                order.limit_price,
            )
        })
    }
    fn cancel(&mut self, order: &PreparedCancel) -> Result<i32> {
        let slot = if let Some((slot, _)) =
            self.contracts[order.venue as usize].get(order.symbol.as_str())
        {
            *slot
        } else {
            self.prepare_contract(order.venue, &order.symbol, false)?
        };
        let reference = order
            .key
            .order_ref
            .parse::<u64>()
            .context("invalid local CTP cancel reference")?;
        ensure!(
            order.system_id.len() < 21 && !order.system_id.as_bytes().contains(&0),
            "invalid CTP system order ID"
        );
        let (front, session) = super::parse_session(&order.key.session)?;
        let id = self.next_id()?;
        self.correlation
            .register_numeric(id, super::CANCEL_RESPONSE, front, session, reference)?;
        local_result(unsafe {
            nf_ctp_cancel(
                self.handle.as_ptr(),
                slot,
                id,
                reference,
                order.system_id.as_ptr(),
                order.system_id.len(),
                front,
                session,
            )
        })
    }
}
impl Drop for CtpTrader {
    fn drop(&mut self) {
        // Release joins SDK transport work before the callback context is dropped.
        unsafe {
            nf_ctp_destroy(self.handle.as_ptr());
        }
        let _ = &self.context;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hot_and_control_callbacks_keep_queue_order_and_own_their_source_data() {
        let (sender, receiver) = crossbeam_channel::bounded(3);
        let fault = Arc::new(AtomicBool::new(false));
        let mut context = CallbackContext {
            sender,
            fault: fault.clone(),
            control: ControlPool::new(1).unwrap(),
            placement: None,
            placement_fault: Arc::new(AtomicBool::new(false)),
        };
        let mut hot = HotEvent {
            kind: super::super::INSERT_RESPONSE,
            request: 1,
            error: 31,
            ..HotEvent::default()
        };
        hot.message[..80].fill(b'x');
        let mut control = RawEvent {
            hot: HotEvent {
                kind: super::super::ACCOUNT,
                request: 2,
                ..HotEvent::default()
            },
            available: 123.0,
            ..RawEvent::default()
        };
        let ptr = (&mut context as *mut CallbackContext).cast();
        unsafe {
            callback(ptr, &hot, std::ptr::null());
            callback(ptr, std::ptr::null(), &control);
            hot.request = 3;
            callback(ptr, &hot, std::ptr::null());
        }
        hot.message.fill(0);
        control.available = 0.0;
        assert_eq!(control.available, 0.0);
        let first = receiver.try_recv().unwrap().event;
        let second = receiver.try_recv().unwrap().event;
        let third = receiver.try_recv().unwrap().event;
        assert_eq!([first.request, second.request, third.request], [1, 2, 3]);
        assert_eq!(first.message[..80], [b'x'; 80]);
        assert_eq!(second.as_ref().control().unwrap().available, 123.0);
        assert!(matches!(first, Event::Hot(_)));
        assert!(matches!(third, Event::Hot(_)));
        assert!(!fault.load(Ordering::Acquire));
    }
    #[test]
    fn callback_overflow_is_latched_outside_the_full_queue() {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let fault = Arc::new(AtomicBool::new(false));
        let mut context = CallbackContext {
            sender,
            fault: fault.clone(),
            control: ControlPool::new(1).unwrap(),
            placement: None,
            placement_fault: Arc::new(AtomicBool::new(false)),
        };
        let event = RawEvent::default();
        let ptr = (&mut context as *mut CallbackContext).cast();
        unsafe {
            callback(ptr, std::ptr::null(), &event);
            callback(ptr, std::ptr::null(), &event);
        }
        assert!(fault.load(Ordering::Acquire));
        assert_eq!(receiver.len(), 1);
    }
    #[test]
    #[ignore = "authorized external host only; requires CTP_TEST_SDK_LIBRARY and CTP_TEST_PLACEMENT_CONFIG; no network connection"]
    fn official_sdk_constructs_and_releases_without_connecting() {
        let path = std::env::var_os("CTP_TEST_SDK_LIBRARY").expect("set CTP_TEST_SDK_LIBRARY");
        assert!(version(Path::new(&path))
            .unwrap()
            .trim_start_matches('v')
            .starts_with("6.7.11"));
        let mut config: Config =
            toml::from_str(include_str!("../../../../config/ctp_trade.example.toml")).unwrap();
        let placement_path = std::env::var_os("CTP_TEST_PLACEMENT_CONFIG")
            .expect("set CTP_TEST_PLACEMENT_CONFIG to the external host's placement TOML");
        config.placement =
            toml::from_str(&std::fs::read_to_string(placement_path).unwrap()).unwrap();
        config.sdk_library = path.into();
        config.flow_directory =
            std::env::temp_dir().join(format!("mkt-ctp-sdk-offline-{}", std::process::id()));
        let credentials = Credentials {
            broker: CString::new("TEST").unwrap(),
            user: CString::new("test").unwrap(),
            investor: CString::new("test").unwrap(),
            password: CString::new("test-only").unwrap(),
            app_id: CString::new("").unwrap(),
            auth_code: CString::new("").unwrap(),
        };
        let placement = crate::placement::Placement::enter(&config.placement).unwrap();
        let trader = placement
            .on_sdk(|| CtpTrader::new(&config, credentials, placement.callback()))
            .unwrap();
        assert!(trader.events.is_empty());
        drop(trader);
        std::fs::remove_dir_all(&config.flow_directory).unwrap();
    }
}
