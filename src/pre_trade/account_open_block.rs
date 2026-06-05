use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::query_eng_channel::QueryEngHub;
use crate::signal::common::TradingVenue;
use crate::trade_engine::query_request::{GenericQueryRequest, QueryRequestType};
use bytes::Bytes;
use log::{info, warn};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

const BINANCE_PM_USDT_OPEN_BLOCK_THRESHOLD: f64 = 2_000.0;
const BINANCE_PM_CAPACITY_POLL_INTERVAL_US: i64 = 60_000_000;
const BINANCE_PM_CAPACITY_LOW_ERROR_CODE: i32 = 0;
const OKEX_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD: f64 = 2_000.0;
const OKEX_UNIFIED_CAPACITY_POLL_INTERVAL_US: i64 = 60_000_000;
const OKEX_UNIFIED_CAPACITY_LOW_ERROR_CODE: i32 = 0;
const GATE_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD: f64 = 2_000.0;
const GATE_UNIFIED_CAPACITY_POLL_INTERVAL_US: i64 = 60_000_000;
const GATE_UNIFIED_CAPACITY_LOW_ERROR_CODE: i32 = 0;
const BITGET_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD: f64 = 2_000.0;
const BITGET_UNIFIED_CAPACITY_POLL_INTERVAL_US: i64 = 60_000_000;
const BITGET_UNIFIED_CAPACITY_LOW_ERROR_CODE: i32 = 0;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum AccountOpenBlockReason {
    BinancePmInsufficientMargin,
    OkexUnifiedInsufficientMargin,
    GateUnifiedInsufficientMargin,
    BitgetUnifiedInsufficientMargin,
}

impl AccountOpenBlockReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BinancePmInsufficientMargin => "binance_pm_insufficient_margin",
            Self::OkexUnifiedInsufficientMargin => "okex_unified_insufficient_margin",
            Self::GateUnifiedInsufficientMargin => "gate_unified_insufficient_margin",
            Self::BitgetUnifiedInsufficientMargin => "bitget_unified_insufficient_margin",
        }
    }
}

#[derive(Debug, Clone)]
struct AccountOpenBlockEntry {
    first_seen_us: i64,
    updated_at_us: i64,
    last_error_code: i32,
}

#[derive(Debug, Clone)]
pub struct AccountOpenBlockHit {
    pub reason: AccountOpenBlockReason,
    pub first_seen_us: i64,
    pub updated_at_us: i64,
    pub last_error_code: i32,
}

#[derive(Debug, Clone)]
pub struct UsdtMaxAvailableMarginSnapshot {
    pub venue: &'static str,
    pub available_label: &'static str,
    pub available: f64,
    pub max_borrowable: f64,
    pub usdt_max_available_margin: f64,
    pub threshold: f64,
    pub ts_us: i64,
}

#[derive(Debug, Clone, Default)]
struct CapacityPollState {
    last_query_sent_us: i64,
    available_query_id: Option<i64>,
    max_borrowable_query_id: Option<i64>,
    last_usdt_available: Option<f64>,
    last_usdt_max_borrowable: Option<f64>,
    last_capacity_check_us: i64,
    last_completed_usdt_available: Option<f64>,
    last_completed_usdt_max_borrowable: Option<f64>,
    last_usdt_max_available_margin: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapacityVenue {
    BinancePm,
    OkexUnified,
    GateUnified,
    BitgetUnified,
}

impl CapacityVenue {
    fn label(self) -> &'static str {
        match self {
            Self::BinancePm => "binance_pm",
            Self::OkexUnified => "okex_unified",
            Self::GateUnified => "gate_unified",
            Self::BitgetUnified => "bitget_unified",
        }
    }

    fn available_label(self) -> &'static str {
        match self {
            Self::BinancePm => "free",
            Self::OkexUnified => "available",
            Self::GateUnified => "available",
            Self::BitgetUnified => "available",
        }
    }

    fn exchange(self) -> &'static str {
        match self {
            Self::BinancePm => "binance",
            Self::OkexUnified => "okex",
            Self::GateUnified => "gate",
            Self::BitgetUnified => "bitget",
        }
    }

    fn threshold(self) -> f64 {
        match self {
            Self::BinancePm => BINANCE_PM_USDT_OPEN_BLOCK_THRESHOLD,
            Self::OkexUnified => OKEX_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD,
            Self::GateUnified => GATE_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD,
            Self::BitgetUnified => BITGET_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD,
        }
    }

    fn poll_interval_us(self) -> i64 {
        match self {
            Self::BinancePm => BINANCE_PM_CAPACITY_POLL_INTERVAL_US,
            Self::OkexUnified => OKEX_UNIFIED_CAPACITY_POLL_INTERVAL_US,
            Self::GateUnified => GATE_UNIFIED_CAPACITY_POLL_INTERVAL_US,
            Self::BitgetUnified => BITGET_UNIFIED_CAPACITY_POLL_INTERVAL_US,
        }
    }

    fn low_error_code(self) -> i32 {
        match self {
            Self::BinancePm => BINANCE_PM_CAPACITY_LOW_ERROR_CODE,
            Self::OkexUnified => OKEX_UNIFIED_CAPACITY_LOW_ERROR_CODE,
            Self::GateUnified => GATE_UNIFIED_CAPACITY_LOW_ERROR_CODE,
            Self::BitgetUnified => BITGET_UNIFIED_CAPACITY_LOW_ERROR_CODE,
        }
    }

    fn block_reason(self) -> AccountOpenBlockReason {
        match self {
            Self::BinancePm => AccountOpenBlockReason::BinancePmInsufficientMargin,
            Self::OkexUnified => AccountOpenBlockReason::OkexUnifiedInsufficientMargin,
            Self::GateUnified => AccountOpenBlockReason::GateUnifiedInsufficientMargin,
            Self::BitgetUnified => AccountOpenBlockReason::BitgetUnifiedInsufficientMargin,
        }
    }

    fn available_req_type(self) -> QueryRequestType {
        match self {
            Self::BinancePm => QueryRequestType::BinancePmUsdtFreeSnapshot,
            Self::OkexUnified => QueryRequestType::OkexUsdtAvailableSnapshot,
            Self::GateUnified => QueryRequestType::GateUnifiedUsdtAvailableSnapshot,
            Self::BitgetUnified => QueryRequestType::BitgetUsdtAvailableSnapshot,
        }
    }

    fn max_borrowable_req_type(self) -> QueryRequestType {
        match self {
            Self::BinancePm => QueryRequestType::BinancePmUsdtMaxBorrowable,
            Self::OkexUnified => QueryRequestType::OkexUsdtMaxLoan,
            Self::GateUnified => QueryRequestType::GateUnifiedUsdtMaxBorrowable,
            Self::BitgetUnified => QueryRequestType::BitgetUsdtMaxTransferable,
        }
    }

    fn available_params(self) -> Bytes {
        match self {
            Self::BinancePm => Bytes::from_static(b"asset=USDT"),
            Self::OkexUnified => Bytes::from_static(b"ccy=USDT"),
            Self::GateUnified => Bytes::new(),
            Self::BitgetUnified => Bytes::new(),
        }
    }

    fn max_borrowable_params(self) -> Bytes {
        match self {
            Self::BinancePm => Bytes::from_static(b"asset=USDT"),
            Self::OkexUnified => Bytes::from_static(b"ccy=USDT&mgnMode=cross"),
            Self::GateUnified => Bytes::from_static(b"currency=USDT"),
            Self::BitgetUnified => Bytes::from_static(b"coin=USDT"),
        }
    }
}

static ACCOUNT_OPEN_BLOCKS: Lazy<Mutex<HashMap<AccountOpenBlockReason, AccountOpenBlockEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static BINANCE_PM_CAPACITY_POLL: Lazy<Mutex<CapacityPollState>> =
    Lazy::new(|| Mutex::new(CapacityPollState::default()));
static OKEX_UNIFIED_CAPACITY_POLL: Lazy<Mutex<CapacityPollState>> =
    Lazy::new(|| Mutex::new(CapacityPollState::default()));
static GATE_UNIFIED_CAPACITY_POLL: Lazy<Mutex<CapacityPollState>> =
    Lazy::new(|| Mutex::new(CapacityPollState::default()));
static BITGET_UNIFIED_CAPACITY_POLL: Lazy<Mutex<CapacityPollState>> =
    Lazy::new(|| Mutex::new(CapacityPollState::default()));
static NEXT_CAPACITY_QUERY_ID: AtomicI64 = AtomicI64::new(-9_100_000);

pub fn register_account_open_block(reason: AccountOpenBlockReason, error_code: i32) {
    register_account_open_block_at(reason, error_code, get_timestamp_us());
}

pub fn check_account_open_block() -> Option<AccountOpenBlockHit> {
    check_account_open_block_at()
}

pub fn clear_account_open_block(reason: AccountOpenBlockReason) -> bool {
    ACCOUNT_OPEN_BLOCKS.lock().remove(&reason).is_some()
}

pub fn latest_usdt_max_available_margin_snapshot() -> Option<UsdtMaxAvailableMarginSnapshot> {
    capacity_venue_for_monitor().and_then(latest_usdt_max_available_margin_snapshot_for_venue)
}

pub fn drive_account_open_block_capacity_poll(now_us: i64) {
    if binance_pm_capacity_poll_enabled() {
        drive_capacity_poll(CapacityVenue::BinancePm, now_us);
    }
    if okex_unified_capacity_poll_enabled() {
        drive_capacity_poll(CapacityVenue::OkexUnified, now_us);
    }
    if gate_unified_capacity_poll_enabled() {
        drive_capacity_poll(CapacityVenue::GateUnified, now_us);
    }
    if bitget_unified_capacity_poll_enabled() {
        drive_capacity_poll(CapacityVenue::BitgetUnified, now_us);
    }
}

fn drive_capacity_poll(venue: CapacityVenue, now_us: i64) {
    let (available_query_id, max_borrowable_query_id) = {
        let mut state = capacity_poll_state(venue).lock();
        if state.last_query_sent_us > 0
            && now_us.saturating_sub(state.last_query_sent_us) < venue.poll_interval_us()
        {
            return;
        }
        let available_query_id = next_capacity_query_id();
        let max_borrowable_query_id = next_capacity_query_id();
        state.last_query_sent_us = now_us;
        state.available_query_id = Some(available_query_id);
        state.max_borrowable_query_id = Some(max_borrowable_query_id);
        state.last_usdt_available = None;
        state.last_usdt_max_borrowable = None;
        (available_query_id, max_borrowable_query_id)
    };

    info!(
        "AccountOpenBlock: {} capacity poll sent available_query_id={} max_borrowable_query_id={} threshold={:.8}",
        venue.label(),
        available_query_id,
        max_borrowable_query_id,
        venue.threshold()
    );

    let available_req = GenericQueryRequest::create(
        venue.available_req_type(),
        now_us,
        available_query_id,
        venue.available_params(),
    );
    if let Err(err) =
        QueryEngHub::publish_query_request(venue.exchange(), &available_req.to_bytes())
    {
        warn!(
            "AccountOpenBlock: publish {} USDT {} query failed: {err:#}",
            venue.label(),
            venue.available_label()
        );
    }

    let max_borrow_req = GenericQueryRequest::create(
        venue.max_borrowable_req_type(),
        now_us,
        max_borrowable_query_id,
        venue.max_borrowable_params(),
    );
    if let Err(err) =
        QueryEngHub::publish_query_request(venue.exchange(), &max_borrow_req.to_bytes())
    {
        warn!(
            "AccountOpenBlock: publish {} USDT maxBorrowable query failed: {err:#}",
            venue.label()
        );
    }
}

pub fn handle_account_open_block_query_response(
    req_type: QueryRequestType,
    client_query_id: i64,
    body: &Bytes,
) -> bool {
    match req_type {
        QueryRequestType::BinancePmUsdtFreeSnapshot => {
            match parse_binance_pm_usdt_free(body) {
                Some(value) => {
                    update_capacity_snapshot(
                        CapacityVenue::BinancePm,
                        req_type,
                        client_query_id,
                        value,
                    );
                }
                None => warn!(
                    "AccountOpenBlock: parse USDT free failed body={}",
                    trim_body(body)
                ),
            }
            true
        }
        QueryRequestType::BinancePmUsdtMaxBorrowable => {
            match parse_binance_pm_max_borrowable(body) {
                Some(value) => {
                    update_capacity_snapshot(
                        CapacityVenue::BinancePm,
                        req_type,
                        client_query_id,
                        value,
                    );
                }
                None => warn!(
                    "AccountOpenBlock: parse USDT maxBorrowable failed body={}",
                    trim_body(body)
                ),
            }
            true
        }
        QueryRequestType::OkexUsdtAvailableSnapshot => {
            match parse_okex_unified_usdt_available(body) {
                Some(value) => {
                    update_capacity_snapshot(
                        CapacityVenue::OkexUnified,
                        req_type,
                        client_query_id,
                        value,
                    );
                }
                None => warn!(
                    "AccountOpenBlock: parse OKX USDT available failed body={}",
                    trim_body(body)
                ),
            }
            true
        }
        QueryRequestType::OkexUsdtMaxLoan => {
            match parse_okex_unified_max_loan(body) {
                Some(value) => {
                    update_capacity_snapshot(
                        CapacityVenue::OkexUnified,
                        req_type,
                        client_query_id,
                        value,
                    );
                }
                None => warn!(
                    "AccountOpenBlock: parse OKX USDT maxLoan failed body={}",
                    trim_body(body)
                ),
            }
            true
        }
        QueryRequestType::GateUnifiedUsdtAvailableSnapshot => {
            match parse_gate_unified_usdt_available(body) {
                Some(value) => {
                    update_capacity_snapshot(
                        CapacityVenue::GateUnified,
                        req_type,
                        client_query_id,
                        value,
                    );
                }
                None => warn!(
                    "AccountOpenBlock: parse Gate USDT available failed body={}",
                    trim_body(body)
                ),
            }
            true
        }
        QueryRequestType::GateUnifiedUsdtMaxBorrowable => {
            match parse_gate_unified_max_borrowable(body) {
                Some(value) => {
                    update_capacity_snapshot(
                        CapacityVenue::GateUnified,
                        req_type,
                        client_query_id,
                        value,
                    );
                }
                None => warn!(
                    "AccountOpenBlock: parse Gate USDT maxBorrowable failed body={}",
                    trim_body(body)
                ),
            }
            true
        }
        QueryRequestType::BitgetUsdtAvailableSnapshot => {
            match parse_bitget_unified_usdt_available(body) {
                Some(value) => {
                    update_capacity_snapshot(
                        CapacityVenue::BitgetUnified,
                        req_type,
                        client_query_id,
                        value,
                    );
                }
                None => warn!(
                    "AccountOpenBlock: parse Bitget USDT available failed body={}",
                    trim_body(body)
                ),
            }
            true
        }
        QueryRequestType::BitgetUsdtMaxTransferable => {
            match parse_bitget_unified_borrow_max_transfer(body) {
                Some(value) => {
                    update_capacity_snapshot(
                        CapacityVenue::BitgetUnified,
                        req_type,
                        client_query_id,
                        value,
                    );
                }
                None => warn!(
                    "AccountOpenBlock: parse Bitget USDT borrowMaxTransfer failed body={}",
                    trim_body(body)
                ),
            }
            true
        }
        _ => false,
    }
}

fn binance_pm_capacity_poll_enabled() -> bool {
    let Some(order_manager) = MonitorChannel::try_order_manager() else {
        return false;
    };
    if order_manager.borrow().binance_is_standard() {
        return false;
    }

    let monitor = MonitorChannel::instance();
    monitor.open_venue() == TradingVenue::BinanceMargin
        && monitor.hedge_venue() == TradingVenue::BinanceFutures
}

fn okex_unified_capacity_poll_enabled() -> bool {
    let monitor = MonitorChannel::instance();
    monitor.open_venue() == TradingVenue::OkexMargin
        && monitor.hedge_venue() == TradingVenue::OkexFutures
}

fn gate_unified_capacity_poll_enabled() -> bool {
    let monitor = MonitorChannel::instance();
    monitor.open_venue() == TradingVenue::GateMargin
        && monitor.hedge_venue() == TradingVenue::GateFutures
}

fn bitget_unified_capacity_poll_enabled() -> bool {
    let monitor = MonitorChannel::instance();
    monitor.open_venue() == TradingVenue::BitgetMargin
        && monitor.hedge_venue() == TradingVenue::BitgetFutures
}

fn capacity_venue_for_monitor() -> Option<CapacityVenue> {
    if binance_pm_capacity_poll_enabled() {
        Some(CapacityVenue::BinancePm)
    } else if okex_unified_capacity_poll_enabled() {
        Some(CapacityVenue::OkexUnified)
    } else if gate_unified_capacity_poll_enabled() {
        Some(CapacityVenue::GateUnified)
    } else if bitget_unified_capacity_poll_enabled() {
        Some(CapacityVenue::BitgetUnified)
    } else {
        None
    }
}

fn register_account_open_block_at(reason: AccountOpenBlockReason, error_code: i32, now_us: i64) {
    let mut guard = ACCOUNT_OPEN_BLOCKS.lock();
    guard
        .entry(reason)
        .and_modify(|entry| {
            entry.updated_at_us = now_us;
            entry.last_error_code = error_code;
        })
        .or_insert(AccountOpenBlockEntry {
            first_seen_us: now_us,
            updated_at_us: now_us,
            last_error_code: error_code,
        });
    warn!(
        "AccountOpenBlock: register reason={} code={} first_seen_us={} updated_at_us={}",
        reason.as_str(),
        error_code,
        guard
            .get(&reason)
            .map(|entry| entry.first_seen_us)
            .unwrap_or(now_us),
        now_us
    );
}

fn ensure_account_open_block_from_capacity_low(venue: CapacityVenue, now_us: i64) -> bool {
    let reason = venue.block_reason();
    let mut guard = ACCOUNT_OPEN_BLOCKS.lock();
    if let Some(entry) = guard.get_mut(&reason) {
        entry.updated_at_us = now_us;
        return false;
    }
    guard.insert(
        reason,
        AccountOpenBlockEntry {
            first_seen_us: now_us,
            updated_at_us: now_us,
            last_error_code: venue.low_error_code(),
        },
    );
    true
}

fn check_account_open_block_at() -> Option<AccountOpenBlockHit> {
    let guard = ACCOUNT_OPEN_BLOCKS.lock();
    guard
        .iter()
        .min_by_key(|(reason, entry)| (entry.first_seen_us, reason.as_str()))
        .map(|(reason, entry)| AccountOpenBlockHit {
            reason: *reason,
            first_seen_us: entry.first_seen_us,
            updated_at_us: entry.updated_at_us,
            last_error_code: entry.last_error_code,
        })
}

fn capacity_poll_state(venue: CapacityVenue) -> &'static Mutex<CapacityPollState> {
    match venue {
        CapacityVenue::BinancePm => &BINANCE_PM_CAPACITY_POLL,
        CapacityVenue::OkexUnified => &OKEX_UNIFIED_CAPACITY_POLL,
        CapacityVenue::GateUnified => &GATE_UNIFIED_CAPACITY_POLL,
        CapacityVenue::BitgetUnified => &BITGET_UNIFIED_CAPACITY_POLL,
    }
}

fn latest_usdt_max_available_margin_snapshot_for_venue(
    venue: CapacityVenue,
) -> Option<UsdtMaxAvailableMarginSnapshot> {
    let state = capacity_poll_state(venue).lock();
    let available = state.last_completed_usdt_available?;
    let max_borrowable = state.last_completed_usdt_max_borrowable?;
    let usdt_max_available_margin = state.last_usdt_max_available_margin?;
    if state.last_capacity_check_us <= 0 {
        return None;
    }
    Some(UsdtMaxAvailableMarginSnapshot {
        venue: venue.label(),
        available_label: venue.available_label(),
        available,
        max_borrowable,
        usdt_max_available_margin,
        threshold: venue.threshold(),
        ts_us: state.last_capacity_check_us,
    })
}

fn update_capacity_snapshot(
    venue: CapacityVenue,
    req_type: QueryRequestType,
    client_query_id: i64,
    value: f64,
) {
    let now_us = get_timestamp_us();
    let (available, max_borrowable, poll_sent_us) = {
        let mut state = capacity_poll_state(venue).lock();
        if req_type == venue.available_req_type() {
            if state.available_query_id != Some(client_query_id) {
                warn!(
                    "AccountOpenBlock: ignore stale {} USDT {} query response client_query_id={} expected={:?}",
                    venue.label(),
                    venue.available_label(),
                    client_query_id,
                    state.available_query_id
                );
                return;
            }
            state.last_usdt_available = Some(value);
        } else if req_type == venue.max_borrowable_req_type() {
            if state.max_borrowable_query_id != Some(client_query_id) {
                warn!(
                    "AccountOpenBlock: ignore stale {} USDT maxBorrowable query response client_query_id={} expected={:?}",
                    venue.label(),
                    client_query_id,
                    state.max_borrowable_query_id
                );
                return;
            }
            state.last_usdt_max_borrowable = Some(value);
        } else {
            return;
        }

        let available = state.last_usdt_available;
        let max_borrowable = state.last_usdt_max_borrowable;
        let poll_sent_us = state.last_query_sent_us;
        let (Some(available), Some(max_borrowable)) = (available, max_borrowable) else {
            info!(
                "AccountOpenBlock: {} capacity pending {}={:?} max_borrowable={:?} available_query_id={:?} max_borrowable_query_id={:?} state=pending",
                venue.label(),
                venue.available_label(),
                available,
                max_borrowable,
                state.available_query_id,
                state.max_borrowable_query_id
            );
            return;
        };
        let capacity = available + max_borrowable;
        state.last_capacity_check_us = now_us;
        state.last_completed_usdt_available = Some(available);
        state.last_completed_usdt_max_borrowable = Some(max_borrowable);
        state.last_usdt_max_available_margin = Some(capacity);
        (available, max_borrowable, poll_sent_us)
    };

    evaluate_capacity(venue, available, max_borrowable, now_us, poll_sent_us);
}

fn evaluate_capacity(
    venue: CapacityVenue,
    available: f64,
    max_borrowable: f64,
    now_us: i64,
    poll_sent_us: i64,
) {
    let capacity = available + max_borrowable;
    let query_latency_us = now_us.saturating_sub(poll_sent_us);
    if capacity > venue.threshold() {
        let cleared = clear_account_open_block(venue.block_reason());
        let state = if cleared { "unlock" } else { "unlocked" };
        warn!(
            "AccountOpenBlock: {} capacity {}={:.8} max_borrowable={:.8} capacity={:.8} threshold={:.8} query_latency_us={} state={}",
            venue.label(),
            venue.available_label(),
            available,
            max_borrowable,
            capacity,
            venue.threshold(),
            query_latency_us,
            state
        );
    } else {
        let inserted = ensure_account_open_block_from_capacity_low(venue, now_us);
        let state = if inserted { "lock" } else { "stay_locked" };
        warn!(
            "AccountOpenBlock: {} capacity {}={:.8} max_borrowable={:.8} capacity={:.8} threshold={:.8} query_latency_us={} state={}",
            venue.label(),
            venue.available_label(),
            available,
            max_borrowable,
            capacity,
            venue.threshold(),
            query_latency_us,
            state
        );
    }
}

fn next_capacity_query_id() -> i64 {
    NEXT_CAPACITY_QUERY_ID.fetch_sub(1, Ordering::Relaxed)
}

fn parse_binance_pm_usdt_free(body: &Bytes) -> Option<f64> {
    let text = trim_body(body);
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    if let Some(rows) = value.as_array() {
        rows.iter()
            .find(|row| {
                row.get("asset")
                    .and_then(|v| v.as_str())
                    .is_some_and(|asset| asset.eq_ignore_ascii_case("USDT"))
            })
            .and_then(|row| parse_json_f64(row.get("crossMarginFree")))
    } else if value
        .get("asset")
        .and_then(|v| v.as_str())
        .is_some_and(|asset| asset.eq_ignore_ascii_case("USDT"))
    {
        parse_json_f64(value.get("crossMarginFree"))
    } else {
        None
    }
}

fn parse_binance_pm_max_borrowable(body: &Bytes) -> Option<f64> {
    let text = trim_body(body);
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    parse_json_f64(value.get("amount"))
}

fn parse_okex_unified_usdt_available(body: &Bytes) -> Option<f64> {
    let text = trim_body(body);
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    if !okex_response_ok(&value) {
        return None;
    }
    let account = value.get("data")?.as_array()?.first()?;
    let details = account.get("details")?.as_array()?;
    let row = details.iter().find(|row| {
        row.get("ccy")
            .and_then(|v| v.as_str())
            .is_some_and(|ccy| ccy.eq_ignore_ascii_case("USDT"))
    })?;
    parse_json_f64(row.get("availEq")).or_else(|| parse_json_f64(row.get("availBal")))
}

fn parse_okex_unified_max_loan(body: &Bytes) -> Option<f64> {
    let text = trim_body(body);
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    if !okex_response_ok(&value) {
        return None;
    }
    if let Some(max_loan) = parse_json_f64(value.get("maxLoan")) {
        return Some(max_loan);
    }
    let rows = value.get("data")?.as_array()?;
    let mut best: Option<f64> = None;
    for row in rows {
        let ccy_matches = row
            .get("ccy")
            .and_then(|v| v.as_str())
            .is_none_or(|ccy| ccy.eq_ignore_ascii_case("USDT"));
        if !ccy_matches {
            continue;
        }
        if let Some(max_loan) = parse_json_f64(row.get("maxLoan")) {
            best = Some(best.map_or(max_loan, |current| current.max(max_loan)));
        }
    }
    best
}

fn parse_gate_unified_usdt_available(body: &Bytes) -> Option<f64> {
    let text = trim_body(body);
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    let details = if let Some(balances) = value.get("balances").and_then(|v| v.as_object()) {
        balances.get("USDT").or_else(|| {
            balances.iter().find_map(|(asset, detail)| {
                if asset.eq_ignore_ascii_case("USDT") {
                    Some(detail)
                } else {
                    None
                }
            })
        })?
    } else if let Some(rows) = value.as_array() {
        rows.iter().find(|row| {
            row.get("currency")
                .or_else(|| row.get("asset"))
                .or_else(|| row.get("coin"))
                .or_else(|| row.get("symbol"))
                .and_then(|v| v.as_str())
                .is_some_and(|asset| asset.eq_ignore_ascii_case("USDT"))
        })?
    } else if value
        .get("currency")
        .or_else(|| value.get("asset"))
        .or_else(|| value.get("coin"))
        .or_else(|| value.get("symbol"))
        .and_then(|v| v.as_str())
        .is_some_and(|asset| asset.eq_ignore_ascii_case("USDT"))
    {
        &value
    } else {
        return None;
    };
    parse_json_f64(details.get("available"))
}

fn parse_gate_unified_max_borrowable(body: &Bytes) -> Option<f64> {
    let text = trim_body(body);
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    if let Some(amount) = parse_json_f64(value.get("amount")) {
        return Some(amount);
    }
    if let Some(data) = value.get("data") {
        if let Some(amount) = parse_json_f64(data.get("amount")) {
            return Some(amount);
        }
    }
    None
}

fn parse_bitget_unified_usdt_available(body: &Bytes) -> Option<f64> {
    let text = trim_body(body);
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    if !bitget_response_ok(&value) {
        return None;
    }
    let assets = value.get("data")?.get("assets")?.as_array()?;
    let row = assets.iter().find(|row| {
        row.get("coin")
            .and_then(|v| v.as_str())
            .is_some_and(|coin| coin.eq_ignore_ascii_case("USDT"))
    })?;
    parse_json_f64(row.get("available"))
        .or_else(|| parse_json_f64(row.get("balance")))
        .or_else(|| parse_json_f64(row.get("equity")))
}

fn parse_bitget_unified_borrow_max_transfer(body: &Bytes) -> Option<f64> {
    let text = trim_body(body);
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    if !bitget_response_ok(&value) {
        return None;
    }
    let data = value.get("data")?;
    parse_json_f64(data.get("borrowMaxTransfer"))
        .or_else(|| parse_json_f64(data.get("borrow_max_transfer")))
}

fn bitget_response_ok(value: &serde_json::Value) -> bool {
    value
        .get("code")
        .and_then(|v| v.as_str())
        .is_some_and(|code| code == "00000" || code == "0")
}

fn okex_response_ok(value: &serde_json::Value) -> bool {
    value
        .get("code")
        .and_then(|v| v.as_str())
        .is_some_and(|code| code == "0")
}

fn parse_json_f64(value: Option<&serde_json::Value>) -> Option<f64> {
    match value? {
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn trim_body(body: &Bytes) -> String {
    let bytes = body.as_ref();
    let actual_len = bytes
        .iter()
        .rposition(|&b| b != 0)
        .map(|pos| pos + 1)
        .unwrap_or(0);
    String::from_utf8_lossy(&bytes[..actual_len]).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    fn clear_all() {
        ACCOUNT_OPEN_BLOCKS.lock().clear();
        *BINANCE_PM_CAPACITY_POLL.lock() = CapacityPollState::default();
        *OKEX_UNIFIED_CAPACITY_POLL.lock() = CapacityPollState::default();
        *GATE_UNIFIED_CAPACITY_POLL.lock() = CapacityPollState::default();
        *BITGET_UNIFIED_CAPACITY_POLL.lock() = CapacityPollState::default();
    }

    fn seed_poll_state(
        venue: CapacityVenue,
        available_query_id: i64,
        max_borrowable_query_id: i64,
    ) {
        *capacity_poll_state(venue).lock() = CapacityPollState {
            last_query_sent_us: 3_000_000,
            available_query_id: Some(available_query_id),
            max_borrowable_query_id: Some(max_borrowable_query_id),
            last_usdt_available: None,
            last_usdt_max_borrowable: None,
            last_capacity_check_us: 0,
            ..CapacityPollState::default()
        };
    }

    #[test]
    fn registers_persistent_account_open_block() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        let now_us = 3_000_000;

        register_account_open_block_at(
            AccountOpenBlockReason::BinancePmInsufficientMargin,
            -2019,
            now_us,
        );
        register_account_open_block_at(
            AccountOpenBlockReason::BinancePmInsufficientMargin,
            -2018,
            now_us + 10,
        );

        let hit = check_account_open_block_at().expect("persistent block must be hit");
        assert_eq!(
            hit.reason,
            AccountOpenBlockReason::BinancePmInsufficientMargin
        );
        assert_eq!(hit.first_seen_us, now_us);
        assert_eq!(hit.updated_at_us, now_us + 10);
        assert_eq!(hit.last_error_code, -2018);
    }

    #[test]
    fn unlocks_when_binance_pm_usdt_capacity_exceeds_threshold() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        register_account_open_block_at(
            AccountOpenBlockReason::BinancePmInsufficientMargin,
            -2019,
            3_000_000,
        );
        seed_poll_state(CapacityVenue::BinancePm, -1, -2);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BinancePmUsdtFreeSnapshot,
            -1,
            &Bytes::from_static(br#"{"asset":"USDT","crossMarginFree":"100.5"}"#),
        ));
        assert!(check_account_open_block().is_some());

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BinancePmUsdtMaxBorrowable,
            -2,
            &Bytes::from_static(br#"{"amount":"2000.0","borrowLimit":"100000"}"#),
        ));
        assert!(check_account_open_block().is_none());
    }

    #[test]
    fn stays_locked_when_binance_pm_usdt_capacity_is_below_threshold() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        register_account_open_block_at(
            AccountOpenBlockReason::BinancePmInsufficientMargin,
            -2019,
            3_000_000,
        );
        seed_poll_state(CapacityVenue::BinancePm, -3, -4);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BinancePmUsdtFreeSnapshot,
            -3,
            &Bytes::from_static(br#"[{"asset":"USDT","crossMarginFree":"10"}]"#),
        ));
        assert!(handle_account_open_block_query_response(
            QueryRequestType::BinancePmUsdtMaxBorrowable,
            -4,
            &Bytes::from_static(br#"{"amount":"1989.0"}"#),
        ));
        assert!(check_account_open_block().is_some());
    }

    #[test]
    fn locks_when_binance_pm_usdt_capacity_is_below_threshold_without_existing_block() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        seed_poll_state(CapacityVenue::BinancePm, -5, -6);

        assert!(check_account_open_block().is_none());
        assert!(handle_account_open_block_query_response(
            QueryRequestType::BinancePmUsdtFreeSnapshot,
            -5,
            &Bytes::from_static(br#"{"asset":"USDT","crossMarginFree":"10"}"#),
        ));
        assert!(check_account_open_block().is_none());

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BinancePmUsdtMaxBorrowable,
            -6,
            &Bytes::from_static(br#"{"amount":"1989.0"}"#),
        ));
        let hit = check_account_open_block().expect("low capacity must lock ArbOpen");
        assert_eq!(
            hit.reason,
            AccountOpenBlockReason::BinancePmInsufficientMargin
        );
        assert_eq!(hit.last_error_code, BINANCE_PM_CAPACITY_LOW_ERROR_CODE);
    }

    #[test]
    fn locks_when_okex_unified_usdt_capacity_is_below_threshold_without_existing_block() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        seed_poll_state(CapacityVenue::OkexUnified, -15, -16);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::OkexUsdtAvailableSnapshot,
            -15,
            &Bytes::from_static(
                br#"{"code":"0","data":[{"details":[{"ccy":"USDT","availEq":"10","availBal":"10"}]}]}"#,
            ),
        ));
        assert!(check_account_open_block().is_none());

        assert!(handle_account_open_block_query_response(
            QueryRequestType::OkexUsdtMaxLoan,
            -16,
            &Bytes::from_static(br#"{"code":"0","data":[{"ccy":"USDT","maxLoan":"1989.0"}]}"#),
        ));
        let hit = check_account_open_block().expect("low OKX capacity must lock ArbOpen");
        assert_eq!(
            hit.reason,
            AccountOpenBlockReason::OkexUnifiedInsufficientMargin
        );
        assert_eq!(hit.last_error_code, OKEX_UNIFIED_CAPACITY_LOW_ERROR_CODE);
    }

    #[test]
    fn unlocks_when_okex_unified_usdt_capacity_exceeds_threshold() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        register_account_open_block_at(
            AccountOpenBlockReason::OkexUnifiedInsufficientMargin,
            51008,
            3_000_000,
        );
        seed_poll_state(CapacityVenue::OkexUnified, -17, -18);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::OkexUsdtAvailableSnapshot,
            -17,
            &Bytes::from_static(
                br#"{"code":"0","data":[{"details":[{"ccy":"USDT","availEq":"100.5","availBal":"100.5"}]}]}"#,
            ),
        ));
        assert!(check_account_open_block().is_some());

        assert!(handle_account_open_block_query_response(
            QueryRequestType::OkexUsdtMaxLoan,
            -18,
            &Bytes::from_static(br#"{"code":"0","data":[{"ccy":"USDT","maxLoan":"2000.0"}]}"#),
        ));
        assert!(check_account_open_block().is_none());
    }

    #[test]
    fn completed_capacity_snapshot_survives_next_pending_poll() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        seed_poll_state(CapacityVenue::GateUnified, -19, -20);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtAvailableSnapshot,
            -19,
            &Bytes::from_static(br#"{"balances":{"USDT":{"available":"100.0","equity":"100.0"}}}"#,),
        ));
        assert!(handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtMaxBorrowable,
            -20,
            &Bytes::from_static(br#"{"amount":"2000.0"}"#),
        ));
        let completed =
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                .expect("completed snapshot");
        assert_eq!(completed.usdt_max_available_margin, 2100.0);

        {
            let mut state = capacity_poll_state(CapacityVenue::GateUnified).lock();
            state.last_query_sent_us = 4_000_000;
            state.available_query_id = Some(-21);
            state.max_borrowable_query_id = Some(-22);
            state.last_usdt_available = None;
            state.last_usdt_max_borrowable = None;
        }

        let still_completed =
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                .expect("completed snapshot survives pending poll");
        assert_eq!(still_completed.usdt_max_available_margin, 2100.0);
    }

    #[test]
    fn locks_when_gate_unified_usdt_capacity_is_below_threshold_without_existing_block() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        seed_poll_state(CapacityVenue::GateUnified, -7, -8);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtAvailableSnapshot,
            -7,
            &Bytes::from_static(
                br#"{"balances":{"USDT":{"available":"10","equity":"10","total_liab":"0"}}}"#,
            ),
        ));
        assert!(check_account_open_block().is_none());

        assert!(handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtMaxBorrowable,
            -8,
            &Bytes::from_static(br#"{"currency":"USDT","amount":"1989.0"}"#),
        ));
        let hit = check_account_open_block().expect("low Gate capacity must lock ArbOpen");
        assert_eq!(
            hit.reason,
            AccountOpenBlockReason::GateUnifiedInsufficientMargin
        );
        assert_eq!(hit.last_error_code, GATE_UNIFIED_CAPACITY_LOW_ERROR_CODE);
    }

    #[test]
    fn unlocks_when_gate_unified_usdt_capacity_exceeds_threshold() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        register_account_open_block_at(
            AccountOpenBlockReason::GateUnifiedInsufficientMargin,
            -100_508,
            3_000_000,
        );
        seed_poll_state(CapacityVenue::GateUnified, -9, -10);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtAvailableSnapshot,
            -9,
            &Bytes::from_static(br#"[{"currency":"USDT","available":"100.5","equity":"100.5"}]"#,),
        ));
        assert!(check_account_open_block().is_some());

        assert!(handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtMaxBorrowable,
            -10,
            &Bytes::from_static(br#"{"amount":"2000.0"}"#),
        ));
        assert!(check_account_open_block().is_none());
    }

    #[test]
    fn locks_when_bitget_unified_usdt_capacity_is_below_threshold_without_existing_block() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        seed_poll_state(CapacityVenue::BitgetUnified, -11, -12);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BitgetUsdtAvailableSnapshot,
            -11,
            &Bytes::from_static(
                br#"{"code":"00000","data":{"assets":[{"coin":"USDT","available":"10","balance":"10"}]}}"#,
            ),
        ));
        assert!(check_account_open_block().is_none());

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BitgetUsdtMaxTransferable,
            -12,
            &Bytes::from_static(
                br#"{"code":"00000","data":{"coin":"USDT","borrowMaxTransfer":"1989.0","maxTransfer":"100"}}"#,
            ),
        ));
        let hit = check_account_open_block().expect("low Bitget capacity must lock ArbOpen");
        assert_eq!(
            hit.reason,
            AccountOpenBlockReason::BitgetUnifiedInsufficientMargin
        );
        assert_eq!(hit.last_error_code, BITGET_UNIFIED_CAPACITY_LOW_ERROR_CODE);
    }

    #[test]
    fn unlocks_when_bitget_unified_usdt_capacity_exceeds_threshold() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        register_account_open_block_at(
            AccountOpenBlockReason::BitgetUnifiedInsufficientMargin,
            40800,
            3_000_000,
        );
        seed_poll_state(CapacityVenue::BitgetUnified, -13, -14);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BitgetUsdtAvailableSnapshot,
            -13,
            &Bytes::from_static(
                br#"{"code":"00000","data":{"assets":[{"coin":"USDT","available":"100.5","balance":"100.5"}]}}"#,
            ),
        ));
        assert!(check_account_open_block().is_some());

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BitgetUsdtMaxTransferable,
            -14,
            &Bytes::from_static(br#"{"code":"00000","data":{"borrowMaxTransfer":"2000.0"}}"#),
        ));
        assert!(check_account_open_block().is_none());
    }
}
