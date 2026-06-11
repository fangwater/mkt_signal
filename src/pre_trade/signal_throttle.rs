use crate::pre_trade::order_manager::Side;
use log::{debug, info, warn};
use once_cell::sync::Lazy;
use order_common::trade_error_code::{bybit, gate};
use parking_lot::Mutex;
use runtime_common::exchange::Exchange;
use runtime_common::time_util::get_timestamp_us;
use std::collections::{BTreeSet, HashMap};

pub const SIGNAL_THROTTLE_TTL_US: i64 = 2 * 60 * 60 * 1_000_000;
pub const GATE_SIGNAL_THROTTLE_TTL_US: i64 = 30 * 60 * 1_000_000;
pub const SIGNAL_THROTTLE_ERROR_CODE_UM_COLLATERAL_LIMIT: i32 = 51169;
pub const SIGNAL_THROTTLE_ERROR_CODE_MARGIN_INSUFFICIENT: i32 = -2019;
pub const SIGNAL_THROTTLE_ERROR_CODE_MAX_BORROWABLE_EXCEEDED: i32 = 51006;
pub const SIGNAL_THROTTLE_ERROR_CODE_BITGET_LENDING_LIMIT: i32 = 25116;
pub const SIGNAL_THROTTLE_ERROR_CODE_BYBIT_MARGIN_UNSUPPORTED: i32 = 170344;
pub const SIGNAL_THROTTLE_ERROR_CODE_BYBIT_COLLATERAL_NOT_ENABLED: i32 = 170037;
pub const SIGNAL_THROTTLE_ERROR_CODE_BYBIT_CONTRACT_NOT_LIVE: i32 = bybit::CONTRACT_NOT_LIVE;
// 51061: 借币池可借资产不足（Binance/OKX 都可能返回该 code）
pub const SIGNAL_THROTTLE_ERROR_CODE_LOANABLE_ASSET_UNAVAILABLE: i32 = 51061;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct SignalThrottleKey {
    symbol: String,
    dir: u8,
}

#[derive(Debug, Clone)]
struct SignalThrottleEntry {
    ban_until_us: i64,
    last_error_code: i32,
    updated_at_us: i64,
}

#[derive(Debug, Clone)]
pub struct SignalThrottleHit {
    pub remaining_us: i64,
    pub until_us: i64,
    pub last_error_code: i32,
}

#[derive(Debug, Clone)]
pub struct ActiveSignalThrottle {
    pub symbol: String,
    pub dir: String,
    pub remaining_us: i64,
    pub until_us: i64,
    pub last_error_code: i32,
}

static SIGNAL_THROTTLE_MAP: Lazy<Mutex<HashMap<SignalThrottleKey, SignalThrottleEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static ACCOUNT_SIGNAL_THROTTLE: Lazy<Mutex<Option<SignalThrottleEntry>>> =
    Lazy::new(|| Mutex::new(None));

impl SignalThrottleKey {
    fn new(symbol: &str, dir: Side) -> Self {
        Self {
            symbol: symbol.trim().to_ascii_uppercase(),
            dir: dir.to_u8(),
        }
    }
}

pub fn is_throttle_error_code(exchange: Option<Exchange>, error_code: i32) -> bool {
    match error_code {
        SIGNAL_THROTTLE_ERROR_CODE_UM_COLLATERAL_LIMIT
        | SIGNAL_THROTTLE_ERROR_CODE_MARGIN_INSUFFICIENT => true,
        SIGNAL_THROTTLE_ERROR_CODE_MAX_BORROWABLE_EXCEEDED => {
            matches!(exchange, Some(Exchange::Binance))
        }
        SIGNAL_THROTTLE_ERROR_CODE_LOANABLE_ASSET_UNAVAILABLE => {
            matches!(exchange, Some(Exchange::Binance))
        }
        SIGNAL_THROTTLE_ERROR_CODE_BITGET_LENDING_LIMIT => {
            matches!(exchange, Some(Exchange::Bitget))
        }
        SIGNAL_THROTTLE_ERROR_CODE_BYBIT_MARGIN_UNSUPPORTED
        | SIGNAL_THROTTLE_ERROR_CODE_BYBIT_COLLATERAL_NOT_ENABLED
        | SIGNAL_THROTTLE_ERROR_CODE_BYBIT_CONTRACT_NOT_LIVE => {
            matches!(exchange, Some(Exchange::Bybit))
        }
        gate::BALANCE_NOT_ENOUGH
        | gate::MARGIN_NOT_ENOUGH
        | gate::POSITION_MARGIN_TOO_LOW
        | gate::LIQUIDITY_NOT_ENOUGH
        | gate::AUTO_BORROW_TOO_MUCH
        | gate::INITIAL_MARGIN_TOO_LOW => matches!(exchange, Some(Exchange::Gate)),
        _ => false,
    }
}

fn is_account_wide_reduce_only_error_code(exchange: Option<Exchange>, error_code: i32) -> bool {
    matches!(
        (exchange, error_code),
        (
            Some(Exchange::Gate),
            gate::INITIAL_MARGIN_TOO_LOW | gate::MARGIN_NOT_ENOUGH | gate::POSITION_MARGIN_TOO_LOW
        )
    )
}

fn signal_throttle_ttl_us(exchange: Option<Exchange>) -> i64 {
    if matches!(exchange, Some(Exchange::Gate)) {
        GATE_SIGNAL_THROTTLE_TTL_US
    } else {
        SIGNAL_THROTTLE_TTL_US
    }
}

pub fn register_signal_throttle(
    symbol: &str,
    dir: Side,
    exchange: Option<Exchange>,
    error_code: i32,
) -> bool {
    let now_us = get_timestamp_us();
    register_signal_throttle_at(
        symbol,
        dir,
        exchange,
        error_code,
        now_us,
        signal_throttle_ttl_us(exchange),
    )
}

pub fn check_signal_throttle(symbol: &str, dir: Side) -> Option<SignalThrottleHit> {
    let now_us = get_timestamp_us();
    check_signal_throttle_at(symbol, dir, now_us)
}

pub fn check_account_signal_throttle() -> Option<SignalThrottleHit> {
    let now_us = get_timestamp_us();
    check_account_signal_throttle_at(now_us)
}

pub fn snapshot_active_signal_throttles() -> Vec<ActiveSignalThrottle> {
    let now_us = get_timestamp_us();
    let mut guard = SIGNAL_THROTTLE_MAP.lock();
    cleanup_expired(&mut guard, now_us);

    let mut rows = Vec::with_capacity(guard.len());
    for (key, entry) in guard.iter() {
        rows.push(ActiveSignalThrottle {
            symbol: key.symbol.clone(),
            dir: side_label_from_u8(key.dir).to_string(),
            remaining_us: entry.ban_until_us.saturating_sub(now_us),
            until_us: entry.ban_until_us,
            last_error_code: entry.last_error_code,
        });
    }
    drop(guard);

    rows.sort_by(|a, b| (&a.symbol, &a.dir, a.until_us).cmp(&(&b.symbol, &b.dir, b.until_us)));
    rows
}

pub fn log_active_signal_throttles(max_details: usize) {
    let rows = snapshot_active_signal_throttles();
    if rows.is_empty() {
        return;
    }

    let symbols: BTreeSet<&str> = rows.iter().map(|row| row.symbol.as_str()).collect();
    let symbol_count = symbols.len();
    let symbol_list = symbols.iter().copied().collect::<Vec<_>>().join(",");

    info!(
        "SignalThrottle: active_blocks={} active_symbols={} [{}]",
        rows.len(),
        symbol_count,
        symbol_list
    );

    let detail_limit = max_details.max(1);
    for row in rows.iter().take(detail_limit) {
        debug!(
            "SignalThrottle: blocked symbol={} dir={} remain_s={} until_us={} code={}",
            row.symbol,
            row.dir,
            row.remaining_us / 1_000_000,
            row.until_us,
            row.last_error_code
        );
    }

    if rows.len() > detail_limit {
        debug!(
            "SignalThrottle: ... {} more blocked entries omitted",
            rows.len() - detail_limit
        );
    }
}

fn register_signal_throttle_at(
    symbol: &str,
    dir: Side,
    exchange: Option<Exchange>,
    error_code: i32,
    now_us: i64,
    ttl_us: i64,
) -> bool {
    if !is_throttle_error_code(exchange, error_code) {
        return false;
    }

    let ttl_us = ttl_us.max(0);
    let ban_until_us = now_us.saturating_add(ttl_us);
    let registered_account_block = if is_account_wide_reduce_only_error_code(exchange, error_code) {
        let mut account_guard = ACCOUNT_SIGNAL_THROTTLE.lock();
        cleanup_account_expired(&mut account_guard, now_us);
        match account_guard.as_mut() {
            Some(entry) => {
                entry.ban_until_us = entry.ban_until_us.max(ban_until_us);
                entry.last_error_code = error_code;
                entry.updated_at_us = now_us;
            }
            None => {
                *account_guard = Some(SignalThrottleEntry {
                    ban_until_us,
                    last_error_code: error_code,
                    updated_at_us: now_us,
                });
            }
        }
        true
    } else {
        false
    };

    let mut guard = SIGNAL_THROTTLE_MAP.lock();
    cleanup_expired(&mut guard, now_us);

    let key = SignalThrottleKey::new(symbol, dir);
    guard
        .entry(key.clone())
        .and_modify(|entry| {
            entry.ban_until_us = entry.ban_until_us.max(ban_until_us);
            entry.last_error_code = error_code;
            entry.updated_at_us = now_us;
        })
        .or_insert(SignalThrottleEntry {
            ban_until_us,
            last_error_code: error_code,
            updated_at_us: now_us,
        });

    warn!(
        "SignalThrottle: register block symbol={} dir={} code={} block_for={}s until_us={}",
        key.symbol,
        dir.as_str(),
        error_code,
        ttl_us / 1_000_000,
        ban_until_us
    );
    if registered_account_block {
        warn!(
            "SignalThrottle: register account-wide reduce-only block code={} block_for={}s until_us={}",
            error_code,
            ttl_us / 1_000_000,
            ban_until_us
        );
    }
    true
}

fn check_signal_throttle_at(symbol: &str, dir: Side, now_us: i64) -> Option<SignalThrottleHit> {
    let key = SignalThrottleKey::new(symbol, dir);
    let mut guard = SIGNAL_THROTTLE_MAP.lock();
    cleanup_expired(&mut guard, now_us);
    let entry = guard.get(&key)?;
    Some(SignalThrottleHit {
        remaining_us: entry.ban_until_us.saturating_sub(now_us),
        until_us: entry.ban_until_us,
        last_error_code: entry.last_error_code,
    })
}

fn check_account_signal_throttle_at(now_us: i64) -> Option<SignalThrottleHit> {
    let mut guard = ACCOUNT_SIGNAL_THROTTLE.lock();
    cleanup_account_expired(&mut guard, now_us);
    let entry = guard.as_ref()?;
    Some(SignalThrottleHit {
        remaining_us: entry.ban_until_us.saturating_sub(now_us),
        until_us: entry.ban_until_us,
        last_error_code: entry.last_error_code,
    })
}

fn cleanup_expired(map: &mut HashMap<SignalThrottleKey, SignalThrottleEntry>, now_us: i64) {
    map.retain(|_, entry| entry.ban_until_us > now_us);
}

fn cleanup_account_expired(entry: &mut Option<SignalThrottleEntry>, now_us: i64) {
    if entry
        .as_ref()
        .is_some_and(|entry| entry.ban_until_us <= now_us)
    {
        *entry = None;
    }
}

fn side_label_from_u8(value: u8) -> &'static str {
    Side::from_u8(value)
        .map(|side| side.as_str())
        .unwrap_or("UNKNOWN")
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    fn clear_all() {
        SIGNAL_THROTTLE_MAP.lock().clear();
        *ACCOUNT_SIGNAL_THROTTLE.lock() = None;
    }

    #[test]
    fn gate_throttle_ttl_is_shorter_than_default() {
        let _guard = TEST_LOCK.lock();
        assert_eq!(
            signal_throttle_ttl_us(Some(Exchange::Gate)),
            GATE_SIGNAL_THROTTLE_TTL_US
        );
        assert_eq!(GATE_SIGNAL_THROTTLE_TTL_US, 30 * 60 * 1_000_000);
        assert_eq!(
            signal_throttle_ttl_us(Some(Exchange::Binance)),
            SIGNAL_THROTTLE_TTL_US
        );
        assert_eq!(
            signal_throttle_ttl_us(Some(Exchange::Okex)),
            SIGNAL_THROTTLE_TTL_US
        );
        assert_eq!(signal_throttle_ttl_us(None), SIGNAL_THROTTLE_TTL_US);
    }

    #[test]
    fn detects_throttle_error_code() {
        let _guard = TEST_LOCK.lock();
        assert!(is_throttle_error_code(Some(Exchange::Binance), 51169));
        assert!(is_throttle_error_code(Some(Exchange::Binance), -2019));
        assert!(is_throttle_error_code(Some(Exchange::Binance), 51006));
        assert!(is_throttle_error_code(Some(Exchange::Binance), 51061));
        assert!(is_throttle_error_code(Some(Exchange::Bitget), 25116));
        assert!(is_throttle_error_code(Some(Exchange::Bybit), 170344));
        assert!(is_throttle_error_code(Some(Exchange::Bybit), 170037));
        assert!(is_throttle_error_code(
            Some(Exchange::Bybit),
            bybit::CONTRACT_NOT_LIVE
        ));
        assert!(is_throttle_error_code(
            Some(Exchange::Gate),
            gate::AUTO_BORROW_TOO_MUCH
        ));
        assert!(is_throttle_error_code(
            Some(Exchange::Gate),
            gate::BALANCE_NOT_ENOUGH
        ));
        assert!(is_throttle_error_code(
            Some(Exchange::Gate),
            gate::INITIAL_MARGIN_TOO_LOW
        ));
        assert!(is_account_wide_reduce_only_error_code(
            Some(Exchange::Gate),
            gate::INITIAL_MARGIN_TOO_LOW
        ));
        assert!(!is_account_wide_reduce_only_error_code(
            Some(Exchange::Gate),
            gate::AUTO_BORROW_TOO_MUCH
        ));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), 25116));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), 170344));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), 170037));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), 170344));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), 170037));
        assert!(!is_throttle_error_code(
            Some(Exchange::Okex),
            bybit::CONTRACT_NOT_LIVE
        ));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), 25116));
        assert!(!is_throttle_error_code(
            Some(Exchange::Binance),
            gate::AUTO_BORROW_TOO_MUCH
        ));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), 51006));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), 51061));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), 51168));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), 516001));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), -2018));
    }

    #[test]
    fn registers_account_throttle_for_gate_initial_margin_low() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        let now_us = 2_000_000;
        let ttl_us = 50;

        assert!(register_signal_throttle_at(
            "ethusdt",
            Side::Sell,
            Some(Exchange::Gate),
            gate::INITIAL_MARGIN_TOO_LOW,
            now_us,
            ttl_us
        ));

        let account_hit =
            check_account_signal_throttle_at(now_us + 1).expect("account throttle must be hit");
        assert_eq!(account_hit.last_error_code, gate::INITIAL_MARGIN_TOO_LOW);
        assert!(check_account_signal_throttle_at(now_us + ttl_us).is_none());
    }

    #[test]
    fn registers_and_expires_throttle() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        let symbol = "btcusdt";
        let now_us = 1_000_000;
        let ttl_us = 30;

        assert!(register_signal_throttle_at(
            symbol,
            Side::Buy,
            Some(Exchange::Binance),
            51169,
            now_us,
            ttl_us
        ));

        let hit1 = check_signal_throttle_at("BTCUSDT", Side::Buy, now_us + 1)
            .expect("throttle must be hit");
        assert_eq!(hit1.last_error_code, 51169);
        assert!(check_account_signal_throttle_at(now_us + 1).is_none());

        let hit2 = check_signal_throttle_at("BTCUSDT", Side::Buy, now_us + ttl_us - 1);
        assert!(hit2.is_some());

        let hit3 = check_signal_throttle_at("BTCUSDT", Side::Buy, now_us + ttl_us);
        assert!(hit3.is_none());
    }
}
