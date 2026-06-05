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

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum AccountOpenBlockReason {
    BinancePmInsufficientMargin,
}

impl AccountOpenBlockReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BinancePmInsufficientMargin => "binance_pm_insufficient_margin",
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

#[derive(Debug, Clone, Default)]
struct BinancePmCapacityPollState {
    last_query_sent_us: i64,
    free_query_id: Option<i64>,
    max_borrowable_query_id: Option<i64>,
    last_usdt_free: Option<f64>,
    last_usdt_max_borrowable: Option<f64>,
    last_capacity_check_us: i64,
}

static ACCOUNT_OPEN_BLOCKS: Lazy<Mutex<HashMap<AccountOpenBlockReason, AccountOpenBlockEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static BINANCE_PM_CAPACITY_POLL: Lazy<Mutex<BinancePmCapacityPollState>> =
    Lazy::new(|| Mutex::new(BinancePmCapacityPollState::default()));
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

pub fn drive_account_open_block_capacity_poll(now_us: i64) {
    if !binance_pm_capacity_poll_enabled() {
        return;
    }

    let (free_query_id, max_borrowable_query_id) = {
        let mut state = BINANCE_PM_CAPACITY_POLL.lock();
        if state.last_query_sent_us > 0
            && now_us.saturating_sub(state.last_query_sent_us)
                < BINANCE_PM_CAPACITY_POLL_INTERVAL_US
        {
            return;
        }
        let free_query_id = next_capacity_query_id();
        let max_borrowable_query_id = next_capacity_query_id();
        *state = BinancePmCapacityPollState {
            last_query_sent_us: now_us,
            free_query_id: Some(free_query_id),
            max_borrowable_query_id: Some(max_borrowable_query_id),
            last_usdt_free: None,
            last_usdt_max_borrowable: None,
            last_capacity_check_us: 0,
        };
        (free_query_id, max_borrowable_query_id)
    };

    info!(
        "AccountOpenBlock: binance_pm capacity poll sent free_query_id={} max_borrowable_query_id={} threshold={:.8}",
        free_query_id, max_borrowable_query_id, BINANCE_PM_USDT_OPEN_BLOCK_THRESHOLD
    );

    let free_req = GenericQueryRequest::create(
        QueryRequestType::BinancePmUsdtFreeSnapshot,
        now_us,
        free_query_id,
        Bytes::from_static(b"asset=USDT"),
    );
    if let Err(err) = QueryEngHub::publish_query_request("binance", &free_req.to_bytes()) {
        warn!("AccountOpenBlock: publish USDT free query failed: {err:#}");
    }

    let max_borrow_req = GenericQueryRequest::create(
        QueryRequestType::BinancePmUsdtMaxBorrowable,
        now_us,
        max_borrowable_query_id,
        Bytes::from_static(b"asset=USDT"),
    );
    if let Err(err) = QueryEngHub::publish_query_request("binance", &max_borrow_req.to_bytes()) {
        warn!("AccountOpenBlock: publish USDT maxBorrowable query failed: {err:#}");
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
                    update_binance_pm_capacity_snapshot(req_type, client_query_id, value);
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
                    update_binance_pm_capacity_snapshot(req_type, client_query_id, value);
                }
                None => warn!(
                    "AccountOpenBlock: parse USDT maxBorrowable failed body={}",
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

fn ensure_account_open_block_from_capacity_low(now_us: i64) -> bool {
    let reason = AccountOpenBlockReason::BinancePmInsufficientMargin;
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
            last_error_code: BINANCE_PM_CAPACITY_LOW_ERROR_CODE,
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

fn update_binance_pm_capacity_snapshot(
    req_type: QueryRequestType,
    client_query_id: i64,
    value: f64,
) {
    let now_us = get_timestamp_us();
    let (free, max_borrowable, poll_sent_us) = {
        let mut state = BINANCE_PM_CAPACITY_POLL.lock();
        match req_type {
            QueryRequestType::BinancePmUsdtFreeSnapshot => {
                if state.free_query_id != Some(client_query_id) {
                    warn!(
                        "AccountOpenBlock: ignore stale USDT free query response client_query_id={} expected={:?}",
                        client_query_id, state.free_query_id
                    );
                    return;
                }
                state.last_usdt_free = Some(value);
            }
            QueryRequestType::BinancePmUsdtMaxBorrowable => {
                if state.max_borrowable_query_id != Some(client_query_id) {
                    warn!(
                        "AccountOpenBlock: ignore stale USDT maxBorrowable query response client_query_id={} expected={:?}",
                        client_query_id, state.max_borrowable_query_id
                    );
                    return;
                }
                state.last_usdt_max_borrowable = Some(value);
            }
            _ => return,
        }

        let free = state.last_usdt_free;
        let max_borrowable = state.last_usdt_max_borrowable;
        let poll_sent_us = state.last_query_sent_us;
        let (Some(free), Some(max_borrowable)) = (free, max_borrowable) else {
            info!(
                "AccountOpenBlock: binance_pm capacity pending free={:?} max_borrowable={:?} free_query_id={:?} max_borrowable_query_id={:?} state=pending",
                free, max_borrowable, state.free_query_id, state.max_borrowable_query_id
            );
            return;
        };
        state.last_capacity_check_us = now_us;
        (free, max_borrowable, poll_sent_us)
    };

    evaluate_binance_pm_capacity(free, max_borrowable, now_us, poll_sent_us);
}

fn evaluate_binance_pm_capacity(free: f64, max_borrowable: f64, now_us: i64, poll_sent_us: i64) {
    let capacity = free + max_borrowable;
    let query_latency_us = now_us.saturating_sub(poll_sent_us);
    if capacity > BINANCE_PM_USDT_OPEN_BLOCK_THRESHOLD {
        let cleared = clear_account_open_block(AccountOpenBlockReason::BinancePmInsufficientMargin);
        let state = if cleared { "unlock" } else { "unlocked" };
        warn!(
            "AccountOpenBlock: binance_pm capacity free={:.8} max_borrowable={:.8} capacity={:.8} threshold={:.8} query_latency_us={} state={}",
            free,
            max_borrowable,
            capacity,
            BINANCE_PM_USDT_OPEN_BLOCK_THRESHOLD,
            query_latency_us,
            state
        );
    } else {
        let inserted = ensure_account_open_block_from_capacity_low(now_us);
        let state = if inserted { "lock" } else { "stay_locked" };
        warn!(
            "AccountOpenBlock: binance_pm capacity free={:.8} max_borrowable={:.8} capacity={:.8} threshold={:.8} query_latency_us={} state={}",
            free,
            max_borrowable,
            capacity,
            BINANCE_PM_USDT_OPEN_BLOCK_THRESHOLD,
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
        *BINANCE_PM_CAPACITY_POLL.lock() = BinancePmCapacityPollState::default();
    }

    fn seed_poll_state(free_query_id: i64, max_borrowable_query_id: i64) {
        *BINANCE_PM_CAPACITY_POLL.lock() = BinancePmCapacityPollState {
            last_query_sent_us: 3_000_000,
            free_query_id: Some(free_query_id),
            max_borrowable_query_id: Some(max_borrowable_query_id),
            last_usdt_free: None,
            last_usdt_max_borrowable: None,
            last_capacity_check_us: 0,
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
        seed_poll_state(-1, -2);

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
        seed_poll_state(-3, -4);

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
        seed_poll_state(-5, -6);

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
}
