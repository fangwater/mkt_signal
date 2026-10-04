use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::query_eng_channel::QueryEngHub;
use crate::pre_trade::signal_throttle::clear_gate_margin_signal_throttles;
use bytes::Bytes;
use log::{info, warn};
use mkt_parsers::msg::basic_account_msg::{BasicAccountRiskMsg, BasicAccountScope};
use once_cell::sync::Lazy;
use order_common::TradingVenue;
use parking_lot::Mutex;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use runtime_common::time_util::get_timestamp_us;
use std::cell::RefCell;
use std::sync::atomic::{AtomicI64, Ordering};
use trade_engine::query_request::{GenericQueryRequest, QueryRequestType};

const BINANCE_PM_USDT_OPEN_BLOCK_THRESHOLD: f64 = 2_000.0;
const BINANCE_PM_CAPACITY_POLL_INTERVAL_US: i64 = 60_000_000;
const BINANCE_PM_CAPACITY_LOW_ERROR_CODE: i32 = 0;
const OKEX_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD: f64 = 2_000.0;
const OKEX_UNIFIED_CAPACITY_POLL_INTERVAL_US: i64 = 60_000_000;
const OKEX_UNIFIED_CAPACITY_LOW_ERROR_CODE: i32 = 0;
const OKEX_USDT_MAX_LOAN_PARAMS: &[u8] = b"instId=BTC-USDT&mgnMode=cross&mgnCcy=USDT";
const GATE_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD: f64 = 2_000.0;
const GATE_UNIFIED_CAPACITY_POLL_INTERVAL_US: i64 = 60_000_000;
const GATE_MARGIN_SNAPSHOT_MAX_AGE_US: i64 = 60_000_000;
const GATE_UNIFIED_CAPACITY_LOW_ERROR_CODE: i32 = 0;
const BITGET_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD: f64 = 2_000.0;
const BITGET_UNIFIED_CAPACITY_LOW_ERROR_CODE: i32 = 0;
const BYBIT_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD: f64 = 2_000.0;
const BYBIT_UNIFIED_CAPACITY_LOW_ERROR_CODE: i32 = 0;
pub const BYBIT_INTERNAL_SYSTEM_OPEN_BLOCK_TTL_US: i64 = 30 * 60 * 1_000_000;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum AccountOpenBlockReason {
    BinancePmInsufficientMargin,
    OkexUnifiedInsufficientMargin,
    GateUnifiedInsufficientMargin,
    BitgetUnifiedInsufficientMargin,
    BybitUnifiedInsufficientMargin,
    BinanceStdUsdtRebalance,
    BybitInternalSystemError,
}

impl AccountOpenBlockReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BinancePmInsufficientMargin => "binance_pm_insufficient_margin",
            Self::OkexUnifiedInsufficientMargin => "okex_unified_insufficient_margin",
            Self::GateUnifiedInsufficientMargin => "gate_unified_insufficient_margin",
            Self::BitgetUnifiedInsufficientMargin => "bitget_unified_insufficient_margin",
            Self::BybitUnifiedInsufficientMargin => "bybit_unified_insufficient_margin",
            Self::BinanceStdUsdtRebalance => "binance_std_usdt_rebalance",
            Self::BybitInternalSystemError => "bybit_internal_system_error",
        }
    }
}

#[derive(Debug, Clone)]
struct AccountOpenBlockEntry {
    first_seen_us: i64,
    updated_at_us: i64,
    last_error_code: i32,
    expires_at_us: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct AccountOpenBlockHit {
    pub reason: AccountOpenBlockReason,
    pub first_seen_us: i64,
    pub updated_at_us: i64,
    pub last_error_code: i32,
}

impl AccountOpenBlockHit {
    pub fn allows_reducing_open(&self) -> bool {
        !matches!(
            self.reason,
            AccountOpenBlockReason::BybitInternalSystemError
                | AccountOpenBlockReason::BinanceStdUsdtRebalance
        )
    }
}

#[derive(Debug, Clone)]
pub struct UsdtMaxAvailableMarginSnapshot {
    pub venue: &'static str,
    pub available_label: &'static str,
    pub available: f64,
    pub max_borrowable: f64,
    pub margin_ratio: Option<f64>,
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
    last_margin_ratio: Option<f64>,
    last_capacity_check_us: i64,
    last_completed_usdt_available: Option<f64>,
    last_completed_usdt_max_borrowable: Option<f64>,
    last_usdt_max_available_margin: Option<f64>,
    last_exchange_snapshot_us: i64,
    last_gate_ws_snapshot_us: i64,
}

#[derive(Debug, PartialEq)]
struct GateAvailableMarginSnapshot {
    available: f64,
    timestamp_us: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapacityVenue {
    BinancePm,
    OkexUnified,
    GateUnified,
    BitgetUnified,
    BybitUnified,
}

impl CapacityVenue {
    fn label(self) -> &'static str {
        match self {
            Self::BinancePm => "binance_pm",
            Self::OkexUnified => "okex_unified",
            Self::GateUnified => "gate_unified",
            Self::BitgetUnified => "bitget_unified",
            Self::BybitUnified => "bybit_unified",
        }
    }

    fn available_label(self) -> &'static str {
        match self {
            Self::BinancePm => "total_available_balance",
            Self::OkexUnified => "available",
            Self::GateUnified => "total_available_margin",
            // Bitget `assets.USDT.available` is a wallet balance, not the
            // unified-account initial-margin headroom for a new UTA order.
            Self::BitgetUnified => "initial_margin_headroom",
            // Bybit `totalAvailableBalance` is the exchange-calculated UTA headroom.
            Self::BybitUnified => "total_available_balance",
        }
    }

    fn exchange(self) -> &'static str {
        match self {
            Self::BinancePm => "binance",
            Self::OkexUnified => "okex",
            Self::GateUnified => "gate",
            Self::BitgetUnified => "bitget",
            Self::BybitUnified => "bybit",
        }
    }

    fn threshold(self) -> f64 {
        match self {
            Self::BinancePm => BINANCE_PM_USDT_OPEN_BLOCK_THRESHOLD,
            Self::OkexUnified => OKEX_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD,
            Self::GateUnified => GATE_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD,
            Self::BitgetUnified => BITGET_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD,
            Self::BybitUnified => BYBIT_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD,
        }
    }

    fn poll_interval_us(self) -> i64 {
        match self {
            Self::BinancePm => BINANCE_PM_CAPACITY_POLL_INTERVAL_US,
            Self::OkexUnified => OKEX_UNIFIED_CAPACITY_POLL_INTERVAL_US,
            Self::GateUnified => GATE_UNIFIED_CAPACITY_POLL_INTERVAL_US,
            Self::BitgetUnified => 0,
            Self::BybitUnified => 0,
        }
    }

    fn low_error_code(self) -> i32 {
        match self {
            Self::BinancePm => BINANCE_PM_CAPACITY_LOW_ERROR_CODE,
            Self::OkexUnified => OKEX_UNIFIED_CAPACITY_LOW_ERROR_CODE,
            Self::GateUnified => GATE_UNIFIED_CAPACITY_LOW_ERROR_CODE,
            Self::BitgetUnified => BITGET_UNIFIED_CAPACITY_LOW_ERROR_CODE,
            Self::BybitUnified => BYBIT_UNIFIED_CAPACITY_LOW_ERROR_CODE,
        }
    }

    fn block_reason(self) -> AccountOpenBlockReason {
        match self {
            Self::BinancePm => AccountOpenBlockReason::BinancePmInsufficientMargin,
            Self::OkexUnified => AccountOpenBlockReason::OkexUnifiedInsufficientMargin,
            Self::GateUnified => AccountOpenBlockReason::GateUnifiedInsufficientMargin,
            Self::BitgetUnified => AccountOpenBlockReason::BitgetUnifiedInsufficientMargin,
            Self::BybitUnified => AccountOpenBlockReason::BybitUnifiedInsufficientMargin,
        }
    }

    fn available_req_type(self) -> QueryRequestType {
        match self {
            Self::BinancePm => QueryRequestType::BinancePmAccountSnapshot,
            Self::OkexUnified => QueryRequestType::OkexUsdtAvailableSnapshot,
            Self::GateUnified => QueryRequestType::GateUnifiedUsdtAvailableSnapshot,
            Self::BitgetUnified => QueryRequestType::BitgetUsdtAvailableSnapshot,
            Self::BybitUnified => QueryRequestType::BybitAccountBalanceSnapshot,
        }
    }

    fn max_borrowable_req_type(self) -> Option<QueryRequestType> {
        match self {
            Self::BinancePm => None,
            Self::OkexUnified => Some(QueryRequestType::OkexUsdtMaxLoan),
            // Borrowing capacity is not additive initial-margin headroom.
            Self::GateUnified => None,
            Self::BitgetUnified => Some(QueryRequestType::BitgetUsdtMaxTransferable),
            Self::BybitUnified => Some(QueryRequestType::BybitAccountBalanceSnapshot),
        }
    }

    fn available_params(self) -> Bytes {
        match self {
            Self::BinancePm => Bytes::new(),
            Self::OkexUnified => Bytes::from_static(b"ccy=USDT"),
            Self::GateUnified => Bytes::from_static(b"currency=USDT"),
            Self::BitgetUnified => Bytes::new(),
            Self::BybitUnified => Bytes::from_static(b"accountType=UNIFIED"),
        }
    }

    fn max_borrowable_params(self) -> Bytes {
        match self {
            Self::BinancePm => Bytes::new(),
            Self::OkexUnified => Bytes::from_static(OKEX_USDT_MAX_LOAN_PARAMS),
            Self::GateUnified => Bytes::from_static(b"currency=USDT"),
            Self::BitgetUnified => Bytes::from_static(b"coin=USDT"),
            Self::BybitUnified => Bytes::from_static(b"accountType=UNIFIED"),
        }
    }
}

thread_local! {
    static ACCOUNT_OPEN_BLOCKS: RefCell<FastHashMap<AccountOpenBlockReason, AccountOpenBlockEntry>> =
        RefCell::new(fast_hash_map());
}
static BINANCE_PM_CAPACITY_POLL: Lazy<Mutex<CapacityPollState>> =
    Lazy::new(|| Mutex::new(CapacityPollState::default()));
static OKEX_UNIFIED_CAPACITY_POLL: Lazy<Mutex<CapacityPollState>> =
    Lazy::new(|| Mutex::new(CapacityPollState::default()));
static GATE_UNIFIED_CAPACITY_POLL: Lazy<Mutex<CapacityPollState>> =
    Lazy::new(|| Mutex::new(CapacityPollState::default()));
static BITGET_UNIFIED_CAPACITY_POLL: Lazy<Mutex<CapacityPollState>> =
    Lazy::new(|| Mutex::new(CapacityPollState::default()));
static BYBIT_UNIFIED_CAPACITY_POLL: Lazy<Mutex<CapacityPollState>> =
    Lazy::new(|| Mutex::new(CapacityPollState::default()));
static NEXT_CAPACITY_QUERY_ID: AtomicI64 = AtomicI64::new(-9_100_000);

pub fn register_account_open_block(reason: AccountOpenBlockReason, error_code: i32) {
    register_account_open_block_at(reason, error_code, get_timestamp_us());
}

pub fn register_bybit_internal_system_open_block(error_code: i32) {
    register_account_open_block_with_ttl_at(
        AccountOpenBlockReason::BybitInternalSystemError,
        error_code,
        get_timestamp_us(),
        BYBIT_INTERNAL_SYSTEM_OPEN_BLOCK_TTL_US,
    );
}

pub fn check_account_open_block() -> Option<AccountOpenBlockHit> {
    check_account_open_block_at()
}

pub fn clear_account_open_block(reason: AccountOpenBlockReason) -> bool {
    ACCOUNT_OPEN_BLOCKS.with(|blocks| blocks.borrow_mut().remove(&reason).is_some())
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
}

/// Gate supplies margin headroom directly in `unified.assets.a`.
pub fn apply_gate_unified_account_risk(scope: BasicAccountScope, msg: &BasicAccountRiskMsg) {
    if scope == BasicAccountScope::GateUnified && gate_unified_capacity_poll_enabled() {
        apply_gate_margin_snapshot_at(
            msg.available_margin_usd,
            msg.timestamp.saturating_mul(1_000),
            get_timestamp_us(),
            msg.margin_ratio.is_finite().then_some(msg.margin_ratio),
            true,
        );
    }
}

fn apply_gate_margin_snapshot_at(
    available: f64,
    snapshot_us: i64,
    now_us: i64,
    margin_ratio: Option<f64>,
    from_ws: bool,
) {
    if !available.is_finite()
        || snapshot_us <= 0
        || snapshot_us > now_us
        || now_us.saturating_sub(snapshot_us) > GATE_MARGIN_SNAPSHOT_MAX_AGE_US
    {
        return;
    }
    {
        let mut state = GATE_UNIFIED_CAPACITY_POLL.lock();
        // A REST reply can arrive after a newer WS update, and vice versa.
        if snapshot_us < state.last_exchange_snapshot_us {
            return;
        }
        state.last_exchange_snapshot_us = snapshot_us;
        if from_ws {
            state.last_gate_ws_snapshot_us = snapshot_us;
        }
        state.last_completed_usdt_available = Some(available);
        state.last_completed_usdt_max_borrowable = Some(0.0);
        state.last_usdt_max_available_margin = Some(available);
        state.last_capacity_check_us = now_us;
        if margin_ratio.is_some() {
            state.last_margin_ratio = margin_ratio;
        }
    }

    evaluate_capacity(
        CapacityVenue::GateUnified,
        available,
        0.0,
        now_us,
        snapshot_us,
    );
    if available > GATE_UNIFIED_USDT_OPEN_BLOCK_THRESHOLD {
        clear_gate_margin_signal_throttles(snapshot_us, now_us);
    }
}

fn handle_gate_margin_query_response_at(
    client_query_id: i64,
    snapshot: GateAvailableMarginSnapshot,
    now_us: i64,
) {
    let snapshot_us = {
        let mut state = GATE_UNIFIED_CAPACITY_POLL.lock();
        if state.available_query_id != Some(client_query_id) {
            return;
        }
        state.available_query_id = None;
        // If Gate omits refresh_time, request start is a conservative time bound.
        snapshot.timestamp_us.unwrap_or(state.last_query_sent_us)
    };
    apply_gate_margin_snapshot_at(snapshot.available, snapshot_us, now_us, None, false);
}

/// Applies Bitget unified-account risk to the account-wide ArbOpen gate.
pub fn apply_bitget_unified_account_risk(msg: &BasicAccountRiskMsg) {
    if bitget_unified_capacity_poll_enabled() {
        apply_unified_account_risk_at(CapacityVenue::BitgetUnified, msg, get_timestamp_us());
    }
}

/// Applies Bybit UTA wallet risk to both the account-wide capacity gate and Viz.
/// The parser maps `adj_equity - initial_margin` exactly to Bybit
/// `totalAvailableBalance`.
pub fn apply_bybit_unified_account_risk(msg: &BasicAccountRiskMsg) {
    if bybit_unified_capacity_poll_enabled() {
        apply_unified_account_risk_at(CapacityVenue::BybitUnified, msg, get_timestamp_us());
    }
}

fn apply_unified_account_risk_at(venue: CapacityVenue, msg: &BasicAccountRiskMsg, now_us: i64) {
    if !msg.adj_equity_usd.is_finite() || !msg.initial_margin_usd.is_finite() {
        warn!(
            "AccountOpenBlock: ignore invalid {} risk snapshot adj_equity_usd={} initial_margin_usd={}",
            venue.label(),
            msg.adj_equity_usd,
            msg.initial_margin_usd
        );
        return;
    }

    let headroom = msg.adj_equity_usd - msg.initial_margin_usd;
    if !headroom.is_finite() {
        warn!(
            "AccountOpenBlock: ignore invalid {} capacity headroom={}",
            venue.label(),
            headroom
        );
        return;
    }
    {
        let mut state = capacity_poll_state(venue).lock();
        state.last_query_sent_us = now_us;
        state.available_query_id = None;
        state.max_borrowable_query_id = None;
        state.last_usdt_available = Some(headroom);
        state.last_usdt_max_borrowable = Some(0.0);
        state.last_margin_ratio = msg.margin_ratio.is_finite().then_some(msg.margin_ratio);
        state.last_capacity_check_us = now_us;
        state.last_completed_usdt_available = Some(headroom);
        state.last_completed_usdt_max_borrowable = Some(0.0);
        state.last_usdt_max_available_margin = Some(headroom);
    }

    evaluate_capacity(venue, headroom, 0.0, now_us, now_us);
}

fn drive_capacity_poll(venue: CapacityVenue, now_us: i64) {
    let (available_query_id, max_borrowable_query_id) = {
        let mut state = capacity_poll_state(venue).lock();
        // REST remains the startup/reconnect fallback when WS is unavailable.
        if venue == CapacityVenue::GateUnified
            && state.last_gate_ws_snapshot_us > 0
            && now_us.saturating_sub(state.last_gate_ws_snapshot_us)
                < GATE_MARGIN_SNAPSHOT_MAX_AGE_US
        {
            return;
        }
        if state.last_query_sent_us > 0
            && now_us.saturating_sub(state.last_query_sent_us) < venue.poll_interval_us()
        {
            return;
        }
        let available_query_id = next_capacity_query_id();
        let max_borrowable_query_id = venue
            .max_borrowable_req_type()
            .map(|_| next_capacity_query_id());
        state.last_query_sent_us = now_us;
        state.available_query_id = Some(available_query_id);
        state.max_borrowable_query_id = max_borrowable_query_id;
        state.last_usdt_available = None;
        state.last_usdt_max_borrowable = venue.max_borrowable_req_type().is_none().then_some(0.0);
        (available_query_id, max_borrowable_query_id)
    };

    info!(
        "AccountOpenBlock: {} capacity poll sent available_query_id={} max_borrowable_query_id={:?} threshold={:.8}",
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

    if let (Some(req_type), Some(query_id)) =
        (venue.max_borrowable_req_type(), max_borrowable_query_id)
    {
        let max_borrow_req =
            GenericQueryRequest::create(req_type, now_us, query_id, venue.max_borrowable_params());
        if let Err(err) =
            QueryEngHub::publish_query_request(venue.exchange(), &max_borrow_req.to_bytes())
        {
            warn!(
                "AccountOpenBlock: publish {} USDT maxBorrowable query failed: {err:#}",
                venue.label()
            );
        }
    }
}

pub fn handle_account_open_block_query_response(
    req_type: QueryRequestType,
    client_query_id: i64,
    body: &Bytes,
) -> bool {
    match req_type {
        QueryRequestType::BinancePmAccountSnapshot => {
            match parse_binance_pm_total_available_balance(body) {
                Some(value) => {
                    update_capacity_snapshot(
                        CapacityVenue::BinancePm,
                        req_type,
                        client_query_id,
                        value,
                    );
                }
                None => warn!(
                    "AccountOpenBlock: parse Binance PM totalAvailableBalance failed body={}",
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
            match parse_gate_unified_available_margin(body) {
                Some(snapshot) => {
                    handle_gate_margin_query_response_at(
                        client_query_id,
                        snapshot,
                        get_timestamp_us(),
                    );
                }
                None => warn!(
                    "AccountOpenBlock: parse Gate unified available margin failed body={}",
                    trim_body(body)
                ),
            }
            true
        }
        QueryRequestType::BitgetUsdtAvailableSnapshot => {
            // Bitget UTA ArbOpen capacity comes from AccountRisk (effEquity - imr),
            // not this wallet-level `assets.USDT.available` response.
            let _ = (client_query_id, body);
            true
        }
        QueryRequestType::BitgetUsdtMaxTransferable => {
            // `borrowMaxTransfer` is not additive initial-margin capacity for a
            // unified-account futures order.
            let _ = (client_query_id, body);
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
        && matches!(
            monitor.hedge_venue(),
            TradingVenue::BitgetFutures | TradingVenue::BitgetCoinFutures
        )
}

fn bybit_unified_capacity_poll_enabled() -> bool {
    let monitor = MonitorChannel::instance();
    monitor.open_venue() == TradingVenue::BybitMargin
        && monitor.hedge_venue() == TradingVenue::BybitFutures
}

fn capacity_venue_for_monitor() -> Option<CapacityVenue> {
    let Some((open_venue, hedge_venue)) = MonitorChannel::try_venues() else {
        return None;
    };

    if binance_pm_capacity_poll_enabled() {
        Some(CapacityVenue::BinancePm)
    } else if open_venue == TradingVenue::OkexMargin && hedge_venue == TradingVenue::OkexFutures {
        Some(CapacityVenue::OkexUnified)
    } else if open_venue == TradingVenue::GateMargin && hedge_venue == TradingVenue::GateFutures {
        Some(CapacityVenue::GateUnified)
    } else if open_venue == TradingVenue::BitgetMargin
        && matches!(
            hedge_venue,
            TradingVenue::BitgetFutures | TradingVenue::BitgetCoinFutures
        )
    {
        Some(CapacityVenue::BitgetUnified)
    } else if open_venue == TradingVenue::BybitMargin && hedge_venue == TradingVenue::BybitFutures {
        Some(CapacityVenue::BybitUnified)
    } else {
        None
    }
}

fn register_account_open_block_at(reason: AccountOpenBlockReason, error_code: i32, now_us: i64) {
    register_account_open_block_entry_at(reason, error_code, now_us, None);
}

fn register_account_open_block_with_ttl_at(
    reason: AccountOpenBlockReason,
    error_code: i32,
    now_us: i64,
    ttl_us: i64,
) {
    let expires_at_us = now_us.saturating_add(ttl_us.max(0));
    register_account_open_block_entry_at(reason, error_code, now_us, Some(expires_at_us));
}

fn register_account_open_block_entry_at(
    reason: AccountOpenBlockReason,
    error_code: i32,
    now_us: i64,
    expires_at_us: Option<i64>,
) {
    let (first_seen_us, active_expires_at_us) = ACCOUNT_OPEN_BLOCKS.with(|blocks| {
        let mut guard = blocks.borrow_mut();
        cleanup_expired_account_open_blocks(&mut guard, now_us);
        let entry = guard
            .entry(reason)
            .and_modify(|entry| {
                entry.updated_at_us = now_us;
                entry.last_error_code = error_code;
                entry.expires_at_us = match (entry.expires_at_us, expires_at_us) {
                    (None, _) | (_, None) => None,
                    (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                };
            })
            .or_insert(AccountOpenBlockEntry {
                first_seen_us: now_us,
                updated_at_us: now_us,
                last_error_code: error_code,
                expires_at_us,
            });
        (entry.first_seen_us, entry.expires_at_us)
    });
    warn!(
        "AccountOpenBlock: register reason={} code={} first_seen_us={} updated_at_us={} expires_at_us={:?}",
        reason.as_str(),
        error_code,
        first_seen_us,
        now_us,
        active_expires_at_us
    );
}

fn ensure_account_open_block_from_capacity_low(venue: CapacityVenue, now_us: i64) -> bool {
    let reason = venue.block_reason();
    ACCOUNT_OPEN_BLOCKS.with(|blocks| {
        let mut guard = blocks.borrow_mut();
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
                expires_at_us: None,
            },
        );
        true
    })
}

fn cancel_all_arb_open_orders_for_capacity_low(now_us: i64) -> usize {
    let Some(strategy_mgr) = MonitorChannel::try_strategy_mgr() else {
        return 0;
    };
    let ids_and_sides = strategy_mgr.borrow().all_arb_open_strategy_ids_and_sides();
    let mut cancelled = 0usize;
    for (strategy_id, side) in ids_and_sides {
        if strategy_mgr.borrow_mut().cancel_arb_open_by_id(
            strategy_id,
            side,
            "account_capacity_low_open_block",
            now_us,
        ) {
            cancelled = cancelled.saturating_add(1);
        }
    }
    cancelled
}

fn check_account_open_block_at() -> Option<AccountOpenBlockHit> {
    check_account_open_block_at_ts(get_timestamp_us())
}

fn check_account_open_block_at_ts(now_us: i64) -> Option<AccountOpenBlockHit> {
    ACCOUNT_OPEN_BLOCKS.with(|blocks| {
        let mut guard = blocks.borrow_mut();
        cleanup_expired_account_open_blocks(&mut guard, now_us);
        guard
            .iter()
            .min_by_key(|(reason, entry)| (entry.first_seen_us, reason.as_str()))
            .map(|(reason, entry)| AccountOpenBlockHit {
                reason: *reason,
                first_seen_us: entry.first_seen_us,
                updated_at_us: entry.updated_at_us,
                last_error_code: entry.last_error_code,
            })
    })
}

fn cleanup_expired_account_open_blocks(
    map: &mut FastHashMap<AccountOpenBlockReason, AccountOpenBlockEntry>,
    now_us: i64,
) {
    map.retain(|_, entry| {
        entry
            .expires_at_us
            .is_none_or(|expires_at_us| expires_at_us > now_us)
    });
}

fn capacity_poll_state(venue: CapacityVenue) -> &'static Mutex<CapacityPollState> {
    match venue {
        CapacityVenue::BinancePm => &BINANCE_PM_CAPACITY_POLL,
        CapacityVenue::OkexUnified => &OKEX_UNIFIED_CAPACITY_POLL,
        CapacityVenue::GateUnified => &GATE_UNIFIED_CAPACITY_POLL,
        CapacityVenue::BitgetUnified => &BITGET_UNIFIED_CAPACITY_POLL,
        CapacityVenue::BybitUnified => &BYBIT_UNIFIED_CAPACITY_POLL,
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
        margin_ratio: state.last_margin_ratio,
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
        } else if venue.max_borrowable_req_type() == Some(req_type) {
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
        let Some(available) = available else {
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
        let max_borrowable = match max_borrowable {
            Some(max_borrowable) => max_borrowable,
            None if available > venue.threshold() => {
                info!(
                    "AccountOpenBlock: {} capacity {}={:.8} exceeds threshold={:.8}; evaluate without pending max_borrowable",
                    venue.label(),
                    venue.available_label(),
                    available,
                    venue.threshold()
                );
                0.0
            }
            None => {
                info!(
                    "AccountOpenBlock: {} capacity pending {}={:?} max_borrowable={:?} available_query_id={:?} max_borrowable_query_id={:?} state=pending",
                    venue.label(),
                    venue.available_label(),
                    Some(available),
                    max_borrowable,
                    state.available_query_id,
                    state.max_borrowable_query_id
                );
                return;
            }
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
        info!(
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
        let cancelled = if inserted && venue == CapacityVenue::BinancePm {
            cancel_all_arb_open_orders_for_capacity_low(now_us)
        } else {
            0
        };
        let state = if inserted { "lock" } else { "stay_locked" };
        warn!(
            "AccountOpenBlock: {} capacity {}={:.8} max_borrowable={:.8} capacity={:.8} threshold={:.8} query_latency_us={} state={} cancel_open_count={}",
            venue.label(),
            venue.available_label(),
            available,
            max_borrowable,
            capacity,
            venue.threshold(),
            query_latency_us,
            state,
            cancelled
        );
    }
}

fn next_capacity_query_id() -> i64 {
    NEXT_CAPACITY_QUERY_ID.fetch_sub(1, Ordering::Relaxed)
}

fn parse_binance_pm_total_available_balance(body: &Bytes) -> Option<f64> {
    let text = trim_body(body);
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    parse_json_f64(value.get("totalAvailableBalance"))
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

fn parse_gate_unified_available_margin(body: &Bytes) -> Option<GateAvailableMarginSnapshot> {
    let text = trim_body(body);
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    // Gate exposes account-wide headroom for multi-currency/portfolio mode.
    // In single-currency mode only USDT's available_margin is meaningful;
    // the top-level total_available_margin is documented to be zero.
    let margin = match value.get("mode")?.as_str()? {
        "multi_currency" | "portfolio" => value.get("total_available_margin"),
        "single_currency" => value.get("balances")?.get("USDT")?.get("available_margin"),
        _ => return None,
    };
    let available = parse_json_f64(margin).filter(|margin| margin.is_finite())?;
    let timestamp_us = match value.get("refresh_time") {
        Some(timestamp) => Some(timestamp.as_i64()?.checked_mul(1_000)?),
        None => None,
    };
    Some(GateAvailableMarginSnapshot {
        available: if value.get("locked").and_then(|locked| locked.as_bool()) == Some(true) {
            0.0
        } else {
            available
        },
        timestamp_us,
    })
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
        ACCOUNT_OPEN_BLOCKS.with(|blocks| blocks.borrow_mut().clear());
        *BINANCE_PM_CAPACITY_POLL.lock() = CapacityPollState::default();
        *OKEX_UNIFIED_CAPACITY_POLL.lock() = CapacityPollState::default();
        *GATE_UNIFIED_CAPACITY_POLL.lock() = CapacityPollState::default();
        *BITGET_UNIFIED_CAPACITY_POLL.lock() = CapacityPollState::default();
    }

    #[test]
    fn gate_unified_available_query_is_scoped_to_usdt() {
        assert_eq!(
            CapacityVenue::GateUnified.available_params().as_ref(),
            b"currency=USDT"
        );
        assert!(CapacityVenue::GateUnified
            .max_borrowable_req_type()
            .is_none());
    }

    #[test]
    fn okex_unified_max_loan_query_has_instrument_context() {
        assert_eq!(
            CapacityVenue::OkexUnified.max_borrowable_params().as_ref(),
            OKEX_USDT_MAX_LOAN_PARAMS
        );
        let params = std::str::from_utf8(OKEX_USDT_MAX_LOAN_PARAMS).unwrap();
        assert!(params.contains("instId=BTC-USDT"));
        assert!(params.contains("mgnMode=cross"));
        assert!(params.contains("mgnCcy=USDT"));
    }

    #[test]
    fn latest_capacity_snapshot_is_none_without_monitor_channel() {
        let _guard = TEST_LOCK.lock();
        clear_all();

        assert!(latest_usdt_max_available_margin_snapshot().is_none());
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

    fn seed_single_poll_state(venue: CapacityVenue, available_query_id: i64) {
        *capacity_poll_state(venue).lock() = CapacityPollState {
            last_query_sent_us: get_timestamp_us().saturating_sub(1_000),
            available_query_id: Some(available_query_id),
            max_borrowable_query_id: None,
            last_usdt_available: None,
            last_usdt_max_borrowable: Some(0.0),
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
    fn bybit_internal_system_block_expires_after_ttl() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        let now_us = 3_000_000;

        register_account_open_block_with_ttl_at(
            AccountOpenBlockReason::BybitInternalSystemError,
            10016,
            now_us,
            BYBIT_INTERNAL_SYSTEM_OPEN_BLOCK_TTL_US,
        );

        let hit =
            check_account_open_block_at_ts(now_us + BYBIT_INTERNAL_SYSTEM_OPEN_BLOCK_TTL_US - 1)
                .expect("Bybit internal system block must remain active before ttl");
        assert_eq!(hit.reason, AccountOpenBlockReason::BybitInternalSystemError);
        assert_eq!(hit.last_error_code, 10016);
        assert!(
            check_account_open_block_at_ts(now_us + BYBIT_INTERNAL_SYSTEM_OPEN_BLOCK_TTL_US)
                .is_none()
        );
    }

    #[test]
    fn bybit_internal_system_block_does_not_allow_reducing_open() {
        let hit = AccountOpenBlockHit {
            reason: AccountOpenBlockReason::BybitInternalSystemError,
            first_seen_us: 1,
            updated_at_us: 2,
            last_error_code: 10016,
        };
        assert!(!hit.allows_reducing_open());

        let rebalance_hit = AccountOpenBlockHit {
            reason: AccountOpenBlockReason::BinanceStdUsdtRebalance,
            first_seen_us: 1,
            updated_at_us: 2,
            last_error_code: 0,
        };
        assert!(!rebalance_hit.allows_reducing_open());

        let margin_hit = AccountOpenBlockHit {
            reason: AccountOpenBlockReason::BinancePmInsufficientMargin,
            first_seen_us: 1,
            updated_at_us: 2,
            last_error_code: -2019,
        };
        assert!(margin_hit.allows_reducing_open());
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
        seed_single_poll_state(CapacityVenue::BinancePm, -1);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BinancePmAccountSnapshot,
            -1,
            &Bytes::from_static(br#"{"totalAvailableBalance":"2000.01"}"#),
        ));
        assert!(check_account_open_block().is_none());
        let state = BINANCE_PM_CAPACITY_POLL.lock();
        assert_eq!(state.last_completed_usdt_available, Some(2000.01));
        assert_eq!(state.last_completed_usdt_max_borrowable, Some(0.0));
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
        seed_single_poll_state(CapacityVenue::BinancePm, -3);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BinancePmAccountSnapshot,
            -3,
            &Bytes::from_static(br#"{"totalAvailableBalance":"1999.99"}"#),
        ));
        assert!(check_account_open_block().is_some());
    }

    #[test]
    fn locks_when_binance_pm_usdt_capacity_is_below_threshold_without_existing_block() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        seed_single_poll_state(CapacityVenue::BinancePm, -5);

        assert!(check_account_open_block().is_none());
        assert!(handle_account_open_block_query_response(
            QueryRequestType::BinancePmAccountSnapshot,
            -5,
            &Bytes::from_static(br#"{"totalAvailableBalance":"1999.99"}"#),
        ));
        let hit = check_account_open_block().expect("low capacity must lock ArbOpen");
        assert_eq!(
            hit.reason,
            AccountOpenBlockReason::BinancePmInsufficientMargin
        );
        assert_eq!(hit.last_error_code, BINANCE_PM_CAPACITY_LOW_ERROR_CODE);
    }

    #[test]
    fn ignores_empty_binance_pm_total_available_balance() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        register_account_open_block_at(
            AccountOpenBlockReason::BinancePmInsufficientMargin,
            -2019,
            3_000_000,
        );
        seed_single_poll_state(CapacityVenue::BinancePm, -7);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::BinancePmAccountSnapshot,
            -7,
            &Bytes::from_static(br#"{"totalAvailableBalance":""}"#),
        ));
        assert!(check_account_open_block().is_some());
        assert_eq!(
            BINANCE_PM_CAPACITY_POLL
                .lock()
                .last_completed_usdt_available,
            None
        );
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
    fn unlocks_okex_when_available_alone_exceeds_threshold() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        register_account_open_block_at(
            AccountOpenBlockReason::OkexUnifiedInsufficientMargin,
            51008,
            3_000_000,
        );
        seed_poll_state(CapacityVenue::OkexUnified, -31, -32);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::OkexUsdtAvailableSnapshot,
            -31,
            &Bytes::from_static(
                br#"{"code":"0","data":[{"details":[{"ccy":"USDT","availEq":"62334.69548106461","availBal":"62334.69548106461"}]}]}"#,
            ),
        ));
        assert!(check_account_open_block().is_none());
        let snapshot =
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::OkexUnified)
                .expect("OKX available-only snapshot");
        assert_eq!(snapshot.available, 62334.69548106461);
        assert_eq!(snapshot.max_borrowable, 0.0);
        assert_eq!(snapshot.usdt_max_available_margin, 62334.69548106461);
    }

    #[test]
    fn completed_capacity_snapshot_survives_next_pending_poll() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        seed_single_poll_state(CapacityVenue::GateUnified, -19);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtAvailableSnapshot,
            -19,
            &Bytes::from_static(
                br#"{"mode":"multi_currency","total_available_margin":"2100.0","balances":{"USDT":{"available":"100.0"}}}"#,
            ),
        ));
        let completed =
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                .expect("completed snapshot");
        assert_eq!(completed.usdt_max_available_margin, 2100.0);

        {
            let mut state = capacity_poll_state(CapacityVenue::GateUnified).lock();
            state.last_query_sent_us = 4_000_000;
            state.available_query_id = Some(-21);
            state.max_borrowable_query_id = None;
            state.last_usdt_available = None;
            state.last_usdt_max_borrowable = Some(0.0);
        }

        let still_completed =
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                .expect("completed snapshot survives pending poll");
        assert_eq!(still_completed.usdt_max_available_margin, 2100.0);
    }

    #[test]
    fn gate_low_margin_locks_despite_high_wallet_balance_and_borrowable() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        seed_single_poll_state(CapacityVenue::GateUnified, -7);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtAvailableSnapshot,
            -7,
            &Bytes::from_static(
                br#"{"mode":"multi_currency","total_available_margin":"1989.0","balances":{"USDT":{"available":"5289.0"}}}"#,
            ),
        ));
        assert!(!handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtMaxBorrowable,
            -8,
            &Bytes::from_static(br#"{"currency":"USDT","amount":"10800.0"}"#),
        ));
        let hit = check_account_open_block().expect("low Gate capacity must lock ArbOpen");
        assert_eq!(
            hit.reason,
            AccountOpenBlockReason::GateUnifiedInsufficientMargin
        );
        assert_eq!(hit.last_error_code, GATE_UNIFIED_CAPACITY_LOW_ERROR_CODE);
        let snapshot =
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                .expect("Gate actual margin snapshot");
        assert_eq!(snapshot.available_label, "total_available_margin");
        assert_eq!(snapshot.available, 1989.0);
        assert_eq!(snapshot.max_borrowable, 0.0);
        assert_eq!(snapshot.usdt_max_available_margin, 1989.0);
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
        seed_single_poll_state(CapacityVenue::GateUnified, -9);

        assert!(handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtAvailableSnapshot,
            -9,
            &Bytes::from_static(
                br#"{"mode":"portfolio","total_available_margin":"3000.0","balances":{"USDT":{"available":"100.5"}}}"#,
            ),
        ));
        assert!(check_account_open_block().is_none());
        let snapshot =
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                .expect("Gate recovered margin snapshot");
        assert_eq!(snapshot.usdt_max_available_margin, 3000.0);
        assert_eq!(snapshot.max_borrowable, 0.0);
    }

    #[test]
    fn gate_margin_parser_respects_account_mode_and_exchange_lock() {
        for mode in ["multi_currency", "portfolio"] {
            let body = Bytes::from(format!(
                r#"{{"mode":"{mode}","locked":false,"total_available_margin":"3626.739382948874","balances":{{"USDT":{{"available":"5289.018742226709","available_margin":"0"}}}}}}"#
            ));
            assert_eq!(
                parse_gate_unified_available_margin(&body).map(|snapshot| snapshot.available),
                Some(3626.739382948874)
            );
        }
        assert_eq!(
            parse_gate_unified_available_margin(&Bytes::from_static(
                br#"{"mode":"single_currency","total_available_margin":"0","balances":{"USDT":{"available":"5289","available_margin":"2500"}}}"#,
            )).map(|snapshot| snapshot.available),
            Some(2500.0)
        );
        assert_eq!(
            parse_gate_unified_available_margin(&Bytes::from_static(
                br#"{"mode":"multi_currency","locked":true,"total_available_margin":"3000"}"#,
            ))
            .map(|snapshot| snapshot.available),
            Some(0.0)
        );
        assert_eq!(
            parse_gate_unified_available_margin(&Bytes::from_static(
                br#"{"mode":"multi_currency","total_available_margin":-50}"#,
            ))
            .map(|snapshot| snapshot.available),
            Some(-50.0)
        );
    }

    #[test]
    fn gate_ws_margin_controls_capacity_and_recovers_both_rejection_locks() {
        use crate::pre_trade::signal_throttle::{
            check_account_signal_throttle, check_signal_throttle, register_signal_throttle,
            GATE_SIGNAL_THROTTLE_TTL_US,
        };
        use order_common::{trade_error_code::gate, Side};
        use runtime_common::exchange::Exchange;

        let _guard = TEST_LOCK.lock();
        clear_all();
        register_signal_throttle(
            "ETHUSDT",
            Side::Buy,
            Some(Exchange::Gate),
            gate::INITIAL_MARGIN_TOO_LOW,
        );
        let error_us =
            check_account_signal_throttle().unwrap().until_us - GATE_SIGNAL_THROTTLE_TTL_US;
        let now_us = error_us + 2_000_000;
        apply_gate_margin_snapshot_at(1_999.0, error_us - 1_000, now_us, Some(8.9), true);
        assert!(check_account_open_block().is_some());
        // A snapshot predating the rejection must not recover its throttle.
        apply_gate_margin_snapshot_at(3_496.19, error_us - 500, now_us, Some(8.9), true);
        assert!(check_account_signal_throttle().is_some());
        assert!(check_signal_throttle("ETHUSDT", Side::Buy).is_some());
        apply_gate_margin_snapshot_at(2_000.0, error_us + 500_000, now_us, Some(8.9), true);
        assert!(check_account_open_block().is_some());
        assert!(check_account_signal_throttle().is_some());

        let mut risk = BasicAccountRiskMsg::create(
            (error_us + 1_000_000) / 1_000,
            19_806.24,
            19_806.24,
            718.0,
            2_914.0,
            8.9,
            0.0,
            0.0,
        );
        risk.available_margin_usd = 3_496.19;
        let decoded = BasicAccountRiskMsg::from_bytes(&risk.to_bytes()).unwrap();
        apply_gate_margin_snapshot_at(
            decoded.available_margin_usd,
            decoded.timestamp * 1_000,
            now_us,
            Some(decoded.margin_ratio),
            true,
        );
        assert!(check_account_open_block().is_none());
        assert!(check_account_signal_throttle().is_none());
        assert!(check_signal_throttle("ETHUSDT", Side::Buy).is_none());
        let snapshot =
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                .unwrap();
        assert_eq!(snapshot.usdt_max_available_margin, 3_496.19);
        assert_eq!(snapshot.max_borrowable, 0.0);
        assert_eq!(snapshot.margin_ratio, Some(8.9));
    }

    #[test]
    fn gate_invalid_stale_or_future_ws_margin_cannot_unlock() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        let now_us = 100_000_000;
        apply_gate_margin_snapshot_at(-50.0, now_us - 1_000, now_us, None, true);
        for (available, timestamp_us) in [
            (f64::NAN, now_us),
            (f64::INFINITY, now_us),
            (3_000.0, 0),
            (3_000.0, now_us - GATE_MARGIN_SNAPSHOT_MAX_AGE_US - 1),
            (3_000.0, now_us + 1),
            (3_000.0, now_us - 2_000),
        ] {
            apply_gate_margin_snapshot_at(available, timestamp_us, now_us, None, true);
            assert!(check_account_open_block().is_some());
            assert_eq!(
                latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                    .unwrap()
                    .usdt_max_available_margin,
                -50.0
            );
        }
        let mut other_risk =
            BasicAccountRiskMsg::create(100_000, 20_000.0, 20_000.0, 100.0, 100.0, 20.0, 0.0, 0.0);
        other_risk.available_margin_usd = 20_000.0;
        apply_gate_unified_account_risk(BasicAccountScope::BybitUnified, &other_risk);
        assert!(check_account_open_block().is_some());
    }

    #[test]
    fn gate_late_rest_response_cannot_replace_newer_ws_margin() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        seed_single_poll_state(CapacityVenue::GateUnified, -19);
        GATE_UNIFIED_CAPACITY_POLL.lock().last_query_sent_us = 8_000_000;
        apply_gate_margin_snapshot_at(1_500.0, 9_000_000, 10_000_000, Some(8.0), true);
        handle_gate_margin_query_response_at(
            -19,
            GateAvailableMarginSnapshot {
                available: 5_000.0,
                timestamp_us: Some(8_500_000),
            },
            11_000_000,
        );
        assert!(check_account_open_block().is_some());
        assert_eq!(
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                .unwrap()
                .usdt_max_available_margin,
            1_500.0
        );
        // A fresh REST fallback can recover after WS stops updating.
        GATE_UNIFIED_CAPACITY_POLL.lock().available_query_id = Some(-20);
        handle_gate_margin_query_response_at(
            -20,
            GateAvailableMarginSnapshot {
                available: 3_000.0,
                timestamp_us: Some(11_000_000),
            },
            12_000_000,
        );
        assert!(check_account_open_block().is_none());
        apply_gate_margin_snapshot_at(1_000.0, 9_000_000, 12_000_000, None, true);
        assert_eq!(
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                .unwrap()
                .usdt_max_available_margin,
            3_000.0
        );
    }

    #[test]
    fn gate_ws_updates_keep_rest_as_fallback() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        apply_gate_margin_snapshot_at(3_000.0, 9_000_000, 10_000_000, None, true);
        drive_capacity_poll(CapacityVenue::GateUnified, 11_000_000);
        let state = GATE_UNIFIED_CAPACITY_POLL.lock();
        assert!(state.available_query_id.is_none());
        assert_eq!(state.last_query_sent_us, 0);
    }

    #[test]
    fn gate_rest_without_refresh_time_uses_request_start_for_recovery() {
        use crate::pre_trade::signal_throttle::{
            check_account_signal_throttle, check_signal_throttle, register_signal_throttle,
            GATE_SIGNAL_THROTTLE_TTL_US,
        };
        use order_common::{trade_error_code::gate, Side};
        use runtime_common::exchange::Exchange;

        let _guard = TEST_LOCK.lock();
        clear_all();
        register_signal_throttle(
            "ETHUSDT",
            Side::Buy,
            Some(Exchange::Gate),
            gate::INITIAL_MARGIN_TOO_LOW,
        );
        let error_us =
            check_account_signal_throttle().unwrap().until_us - GATE_SIGNAL_THROTTLE_TTL_US;
        for (query_id, request_us) in [(-31, error_us - 1_000), (-32, error_us + 1_000)] {
            *GATE_UNIFIED_CAPACITY_POLL.lock() = CapacityPollState {
                last_query_sent_us: request_us,
                available_query_id: Some(query_id),
                ..CapacityPollState::default()
            };
            handle_gate_margin_query_response_at(
                query_id,
                GateAvailableMarginSnapshot {
                    available: 3_000.0,
                    timestamp_us: None,
                },
                error_us + 2_000,
            );
            assert_eq!(
                check_account_signal_throttle().is_some(),
                request_us < error_us
            );
            assert_eq!(
                check_signal_throttle("ETHUSDT", Side::Buy).is_some(),
                request_us < error_us
            );
        }
    }

    #[test]
    fn gate_rest_parser_retains_exchange_refresh_time() {
        let body = Bytes::from_static(br#"{"mode":"multi_currency","total_available_margin":"3496.18508","refresh_time":1791125187300}"#);
        assert_eq!(
            parse_gate_unified_available_margin(&body),
            Some(GateAvailableMarginSnapshot {
                available: 3_496.18508,
                timestamp_us: Some(1_791_125_187_300_000),
            })
        );
    }

    #[test]
    fn gate_invalid_margin_snapshot_preserves_open_block_and_completed_snapshot() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        seed_single_poll_state(CapacityVenue::GateUnified, -9);
        handle_account_open_block_query_response(
            QueryRequestType::GateUnifiedUsdtAvailableSnapshot,
            -9,
            &Bytes::from_static(br#"{"mode":"multi_currency","total_available_margin":"100"}"#),
        );
        for body in [
            br#"{"mode":"multi_currency","balances":{"USDT":{"available":"99999"}}}"#.as_slice(),
            br#"{"mode":"multi_currency","total_available_margin":"NaN"}"#.as_slice(),
            br#"{"mode":"multi_currency","total_available_margin":"inf"}"#.as_slice(),
            br#"{"mode":"classic","total_available_margin":"99999"}"#.as_slice(),
            br#"{"total_available_margin":"99999"}"#.as_slice(),
        ] {
            assert!(handle_account_open_block_query_response(
                QueryRequestType::GateUnifiedUsdtAvailableSnapshot,
                -9,
                &Bytes::copy_from_slice(body),
            ));
            assert!(check_account_open_block().is_some());
            assert_eq!(
                latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::GateUnified)
                    .unwrap()
                    .usdt_max_available_margin,
                100.0
            );
        }
    }

    #[test]
    fn bitget_risk_headroom_keeps_25203_locked_until_margin_recovers() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        register_account_open_block_at(
            AccountOpenBlockReason::BitgetUnifiedInsufficientMargin,
            25203,
            3_000_000,
        );
        let low_headroom =
            BasicAccountRiskMsg::create(0, 23_000.0, 76_000.0, 6_000.0, 22_000.0, 3.8, 0.0, 0.0);
        apply_unified_account_risk_at(CapacityVenue::BitgetUnified, &low_headroom, 3_100_000);
        let hit = check_account_open_block().expect("low risk headroom must keep ArbOpen locked");
        assert_eq!(
            hit.reason,
            AccountOpenBlockReason::BitgetUnifiedInsufficientMargin
        );
        assert_eq!(hit.last_error_code, 25203);

        let recovered_headroom =
            BasicAccountRiskMsg::create(0, 25_000.0, 76_000.0, 6_000.0, 22_000.0, 4.1, 0.0, 0.0);
        apply_unified_account_risk_at(CapacityVenue::BitgetUnified, &recovered_headroom, 3_200_000);
        assert!(check_account_open_block().is_none());
        let state = BITGET_UNIFIED_CAPACITY_POLL.lock();
        assert_eq!(state.last_usdt_max_available_margin, Some(3_000.0));
        assert_eq!(state.last_margin_ratio, Some(4.1));
    }

    #[test]
    fn bybit_wallet_headroom_locks_and_recovers_the_arb_open_gate() {
        let _guard = TEST_LOCK.lock();
        clear_all();

        let low_headroom =
            BasicAccountRiskMsg::create(0, 4_999.0, 7_000.0, 500.0, 3_000.0, 14.0, 0.0, 0.0);
        apply_unified_account_risk_at(CapacityVenue::BybitUnified, &low_headroom, 3_100_000);
        let hit = check_account_open_block().expect("low Bybit wallet headroom must lock ArbOpen");
        assert_eq!(
            hit.reason,
            AccountOpenBlockReason::BybitUnifiedInsufficientMargin
        );

        let recovered_headroom =
            BasicAccountRiskMsg::create(0, 5_001.0, 7_000.0, 500.0, 3_000.0, 14.0, 0.0, 0.0);
        apply_unified_account_risk_at(CapacityVenue::BybitUnified, &recovered_headroom, 3_200_000);
        assert!(check_account_open_block().is_none());
        let snapshot =
            latest_usdt_max_available_margin_snapshot_for_venue(CapacityVenue::BybitUnified)
                .expect("Bybit capacity snapshot");
        assert_eq!(snapshot.venue, "bybit_unified");
        assert_eq!(snapshot.available_label, "total_available_balance");
        assert_eq!(snapshot.usdt_max_available_margin, 2_001.0);
        assert_eq!(snapshot.margin_ratio, Some(14.0));
    }
}
