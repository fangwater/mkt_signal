use crate::pre_trade::order_manager::Side;
use std::cell::RefCell;
use std::collections::BTreeSet;

use log::{debug, info, warn};
use order_common::trade_error_code::{bitget, bybit, gate};
use runtime_common::exchange::Exchange;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use runtime_common::time_util::get_timestamp_us;
use trade_signal::ArbMode;

pub const SIGNAL_THROTTLE_TTL_US: i64 = 2 * 60 * 60 * 1_000_000;
pub const INTRA_SIGNAL_THROTTLE_TTL_US: i64 = SIGNAL_THROTTLE_TTL_US;
pub const GATE_SIGNAL_THROTTLE_TTL_US: i64 = 30 * 60 * 1_000_000;
pub const SIGNAL_THROTTLE_ERROR_CODE_BALANCE_INSUFFICIENT: i32 = -2018;
pub const SIGNAL_THROTTLE_ERROR_CODE_UM_COLLATERAL_LIMIT: i32 = 51169;
pub const SIGNAL_THROTTLE_ERROR_CODE_MARGIN_INSUFFICIENT: i32 = -2019;
pub const SIGNAL_THROTTLE_ERROR_CODE_MAX_BORROWABLE_EXCEEDED: i32 = 51006;
pub const SIGNAL_THROTTLE_ERROR_CODE_BITGET_LENDING_LIMIT: i32 = 25116;
pub const SIGNAL_THROTTLE_ERROR_CODE_BITGET_POSITION_TIER_LIMIT: i32 =
    bitget::POSITION_TIER_LIMIT_EXCEEDED;
pub const SIGNAL_THROTTLE_ERROR_CODE_BYBIT_LIABILITY_OVERFLOW: i32 =
    bybit::LIABILITY_OVERFLOW_SPOT_LEVERAGE;
pub const SIGNAL_THROTTLE_ERROR_CODE_BYBIT_MARGIN_UNSUPPORTED: i32 =
    bybit::MARGIN_TRADING_UNSUPPORTED;
pub const SIGNAL_THROTTLE_ERROR_CODE_BYBIT_COLLATERAL_NOT_ENABLED: i32 =
    bybit::COLLATERAL_NOT_ENABLED;
pub const SIGNAL_THROTTLE_ERROR_CODE_BYBIT_CONTRACT_NOT_LIVE: i32 = bybit::CONTRACT_NOT_LIVE;
pub const SIGNAL_THROTTLE_ERROR_CODE_BYBIT_PLATFORM_LOAN_NOT_ENOUGH: i32 =
    bybit::PLATFORM_LOAN_AMOUNT_NOT_ENOUGH;
pub const SIGNAL_THROTTLE_ERROR_CODE_BYBIT_OPEN_INTEREST_POSITION_LIMIT: i32 =
    bybit::OPEN_INTEREST_POSITION_LIMIT_EXCEEDED;
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
    source: SignalThrottleSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignalThrottleSource {
    ExchangeError,
    BinanceFuturesInsufficientMargin,
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

thread_local! {
    static SIGNAL_THROTTLE_MAP: RefCell<FastHashMap<SignalThrottleKey, SignalThrottleEntry>> =
        RefCell::new(fast_hash_map());
    static ACCOUNT_SIGNAL_THROTTLE: RefCell<Option<SignalThrottleEntry>> = const {
        RefCell::new(None)
    };
}

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
        SIGNAL_THROTTLE_ERROR_CODE_BALANCE_INSUFFICIENT => {
            matches!(exchange, Some(Exchange::Binance))
        }
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
        SIGNAL_THROTTLE_ERROR_CODE_BITGET_POSITION_TIER_LIMIT => {
            matches!(exchange, Some(Exchange::Bitget))
        }
        SIGNAL_THROTTLE_ERROR_CODE_BYBIT_LIABILITY_OVERFLOW
        | SIGNAL_THROTTLE_ERROR_CODE_BYBIT_MARGIN_UNSUPPORTED
        | SIGNAL_THROTTLE_ERROR_CODE_BYBIT_COLLATERAL_NOT_ENABLED
        | SIGNAL_THROTTLE_ERROR_CODE_BYBIT_CONTRACT_NOT_LIVE
        | SIGNAL_THROTTLE_ERROR_CODE_BYBIT_PLATFORM_LOAN_NOT_ENOUGH
        | SIGNAL_THROTTLE_ERROR_CODE_BYBIT_OPEN_INTEREST_POSITION_LIMIT => {
            matches!(exchange, Some(Exchange::Bybit))
        }
        gate::BALANCE_NOT_ENOUGH
        | gate::MARGIN_NOT_ENOUGH
        | gate::POSITION_MARGIN_TOO_LOW
        | gate::LIQUIDITY_NOT_ENOUGH
        | gate::AUTO_BORROW_TOO_MUCH
        | gate::INITIAL_MARGIN_TOO_LOW
        | gate::RISK_CHECK_MARKET_FORBIDDEN => matches!(exchange, Some(Exchange::Gate)),
        _ => false,
    }
}

fn is_account_wide_reduce_only_error_code(exchange: Option<Exchange>, error_code: i32) -> bool {
    matches!(
        (exchange, error_code),
        (
            Some(Exchange::Gate),
            gate::INITIAL_MARGIN_TOO_LOW
                | gate::MARGIN_NOT_ENOUGH
                | gate::POSITION_MARGIN_TOO_LOW
                | gate::RISK_CHECK_MARKET_FORBIDDEN
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

fn signal_throttle_ttl_us_for_mode(exchange: Option<Exchange>, arb_mode: ArbMode) -> i64 {
    if matches!(arb_mode, ArbMode::IntraArb) && matches!(exchange, Some(Exchange::Binance)) {
        INTRA_SIGNAL_THROTTLE_TTL_US
    } else {
        signal_throttle_ttl_us(exchange)
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

pub fn register_signal_throttle_for_mode(
    symbol: &str,
    dir: Side,
    exchange: Option<Exchange>,
    error_code: i32,
    arb_mode: ArbMode,
) -> bool {
    let now_us = get_timestamp_us();
    register_signal_throttle_at(
        symbol,
        dir,
        exchange,
        error_code,
        now_us,
        signal_throttle_ttl_us_for_mode(exchange, arb_mode),
    )
}

pub fn register_binance_futures_margin_signal_throttle_for_mode(
    symbol: &str,
    dir: Side,
    error_code: i32,
    arb_mode: ArbMode,
) -> bool {
    let now_us = get_timestamp_us();
    register_signal_throttle_at_with_source(
        symbol,
        dir,
        Some(Exchange::Binance),
        error_code,
        now_us,
        signal_throttle_ttl_us_for_mode(Some(Exchange::Binance), arb_mode),
        SignalThrottleSource::BinanceFuturesInsufficientMargin,
    )
}

pub fn clear_binance_futures_margin_signal_throttles() -> usize {
    SIGNAL_THROTTLE_MAP.with(|map| {
        let mut guard = map.borrow_mut();
        let before = guard.len();
        guard.retain(|_, entry| {
            entry.source != SignalThrottleSource::BinanceFuturesInsufficientMargin
        });
        before.saturating_sub(guard.len())
    })
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
    let mut rows = SIGNAL_THROTTLE_MAP.with(|map| {
        let mut guard = map.borrow_mut();
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
        rows
    });

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
    register_signal_throttle_at_with_source(
        symbol,
        dir,
        exchange,
        error_code,
        now_us,
        ttl_us,
        SignalThrottleSource::ExchangeError,
    )
}

fn register_signal_throttle_at_with_source(
    symbol: &str,
    dir: Side,
    exchange: Option<Exchange>,
    error_code: i32,
    now_us: i64,
    ttl_us: i64,
    source: SignalThrottleSource,
) -> bool {
    if !is_throttle_error_code(exchange, error_code) {
        return false;
    }

    let ttl_us = ttl_us.max(0);
    let ban_until_us = now_us.saturating_add(ttl_us);
    let registered_account_block = if is_account_wide_reduce_only_error_code(exchange, error_code) {
        ACCOUNT_SIGNAL_THROTTLE.with(|slot| {
            let mut account_guard = slot.borrow_mut();
            cleanup_account_expired(&mut account_guard, now_us);
            match account_guard.as_mut() {
                Some(entry) => {
                    entry.ban_until_us = entry.ban_until_us.max(ban_until_us);
                    entry.last_error_code = error_code;
                    entry.updated_at_us = now_us;
                    entry.source = source;
                }
                None => {
                    *account_guard = Some(SignalThrottleEntry {
                        ban_until_us,
                        last_error_code: error_code,
                        updated_at_us: now_us,
                        source,
                    });
                }
            }
        });
        true
    } else {
        false
    };

    let key = SignalThrottleKey::new(symbol, dir);
    SIGNAL_THROTTLE_MAP.with(|map| {
        let mut guard = map.borrow_mut();
        cleanup_expired(&mut guard, now_us);
        guard
            .entry(key.clone())
            .and_modify(|entry| {
                entry.ban_until_us = entry.ban_until_us.max(ban_until_us);
                entry.last_error_code = error_code;
                entry.updated_at_us = now_us;
                if source == SignalThrottleSource::ExchangeError {
                    entry.source = source;
                }
            })
            .or_insert(SignalThrottleEntry {
                ban_until_us,
                last_error_code: error_code,
                updated_at_us: now_us,
                source,
            });
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
    SIGNAL_THROTTLE_MAP.with(|map| {
        let mut guard = map.borrow_mut();
        cleanup_expired(&mut guard, now_us);
        guard.get(&key).map(|entry| SignalThrottleHit {
            remaining_us: entry.ban_until_us.saturating_sub(now_us),
            until_us: entry.ban_until_us,
            last_error_code: entry.last_error_code,
        })
    })
}

fn check_account_signal_throttle_at(now_us: i64) -> Option<SignalThrottleHit> {
    ACCOUNT_SIGNAL_THROTTLE.with(|slot| {
        let mut guard = slot.borrow_mut();
        cleanup_account_expired(&mut guard, now_us);
        guard.as_ref().map(|entry| SignalThrottleHit {
            remaining_us: entry.ban_until_us.saturating_sub(now_us),
            until_us: entry.ban_until_us,
            last_error_code: entry.last_error_code,
        })
    })
}

fn cleanup_expired(map: &mut FastHashMap<SignalThrottleKey, SignalThrottleEntry>, now_us: i64) {
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
    use once_cell::sync::Lazy;
    use parking_lot::Mutex;

    static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    fn clear_all() {
        SIGNAL_THROTTLE_MAP.with(|map| map.borrow_mut().clear());
        ACCOUNT_SIGNAL_THROTTLE.with(|slot| *slot.borrow_mut() = None);
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
    fn binance_intra_throttle_ttl_matches_cross_two_hours() {
        let _guard = TEST_LOCK.lock();
        assert_eq!(
            signal_throttle_ttl_us_for_mode(Some(Exchange::Binance), ArbMode::IntraArb),
            INTRA_SIGNAL_THROTTLE_TTL_US
        );
        assert_eq!(INTRA_SIGNAL_THROTTLE_TTL_US, SIGNAL_THROTTLE_TTL_US);
        assert_eq!(INTRA_SIGNAL_THROTTLE_TTL_US, 2 * 60 * 60 * 1_000_000);
        assert_eq!(
            signal_throttle_ttl_us_for_mode(Some(Exchange::Binance), ArbMode::FundingArb),
            SIGNAL_THROTTLE_TTL_US
        );
        assert_eq!(
            signal_throttle_ttl_us_for_mode(Some(Exchange::Okex), ArbMode::IntraArb),
            SIGNAL_THROTTLE_TTL_US
        );
        assert_eq!(
            signal_throttle_ttl_us_for_mode(Some(Exchange::Gate), ArbMode::IntraArb),
            GATE_SIGNAL_THROTTLE_TTL_US
        );
        assert_eq!(
            signal_throttle_ttl_us_for_mode(Some(Exchange::Gate), ArbMode::CrossArb),
            GATE_SIGNAL_THROTTLE_TTL_US
        );
    }

    #[test]
    fn detects_throttle_error_code() {
        let _guard = TEST_LOCK.lock();
        assert!(!is_throttle_error_code(Some(Exchange::Binance), -2010));
        assert!(is_throttle_error_code(Some(Exchange::Binance), -2018));
        assert!(is_throttle_error_code(Some(Exchange::Binance), 51169));
        assert!(is_throttle_error_code(Some(Exchange::Binance), -2019));
        assert!(is_throttle_error_code(Some(Exchange::Binance), 51006));
        assert!(is_throttle_error_code(Some(Exchange::Binance), 51061));
        assert!(is_throttle_error_code(Some(Exchange::Bitget), 25116));
        assert!(is_throttle_error_code(
            Some(Exchange::Bitget),
            bitget::POSITION_TIER_LIMIT_EXCEEDED
        ));
        assert!(is_throttle_error_code(
            Some(Exchange::Bybit),
            bybit::LIABILITY_OVERFLOW_SPOT_LEVERAGE
        ));
        assert!(is_throttle_error_code(
            Some(Exchange::Bybit),
            bybit::MARGIN_TRADING_UNSUPPORTED
        ));
        assert!(is_throttle_error_code(Some(Exchange::Bybit), 170037));
        assert!(is_throttle_error_code(
            Some(Exchange::Bybit),
            bybit::PLATFORM_LOAN_AMOUNT_NOT_ENOUGH
        ));
        assert!(is_throttle_error_code(
            Some(Exchange::Bybit),
            bybit::CONTRACT_NOT_LIVE
        ));
        assert!(is_throttle_error_code(
            Some(Exchange::Bybit),
            bybit::OPEN_INTEREST_POSITION_LIMIT_EXCEEDED
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
        assert!(is_throttle_error_code(
            Some(Exchange::Gate),
            gate::RISK_CHECK_MARKET_FORBIDDEN
        ));
        assert!(is_account_wide_reduce_only_error_code(
            Some(Exchange::Gate),
            gate::RISK_CHECK_MARKET_FORBIDDEN
        ));
        assert!(!is_account_wide_reduce_only_error_code(
            Some(Exchange::Gate),
            gate::AUTO_BORROW_TOO_MUCH
        ));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), 25116));
        assert!(!is_throttle_error_code(
            Some(Exchange::Binance),
            bybit::LIABILITY_OVERFLOW_SPOT_LEVERAGE
        ));
        assert!(!is_throttle_error_code(
            Some(Exchange::Binance),
            bybit::MARGIN_TRADING_UNSUPPORTED
        ));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), 170037));
        assert!(!is_throttle_error_code(
            Some(Exchange::Binance),
            bybit::PLATFORM_LOAN_AMOUNT_NOT_ENOUGH
        ));
        assert!(!is_throttle_error_code(
            Some(Exchange::Okex),
            bybit::LIABILITY_OVERFLOW_SPOT_LEVERAGE
        ));
        assert!(!is_throttle_error_code(
            Some(Exchange::Okex),
            bybit::MARGIN_TRADING_UNSUPPORTED
        ));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), 170037));
        assert!(!is_throttle_error_code(
            Some(Exchange::Okex),
            bybit::PLATFORM_LOAN_AMOUNT_NOT_ENOUGH
        ));
        assert!(!is_throttle_error_code(
            Some(Exchange::Okex),
            bybit::CONTRACT_NOT_LIVE
        ));
        assert!(!is_throttle_error_code(
            Some(Exchange::Okex),
            bybit::OPEN_INTEREST_POSITION_LIMIT_EXCEEDED
        ));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), 25116));
        assert!(!is_throttle_error_code(
            Some(Exchange::Okex),
            bitget::POSITION_TIER_LIMIT_EXCEEDED
        ));
        assert!(!is_throttle_error_code(
            Some(Exchange::Binance),
            gate::AUTO_BORROW_TOO_MUCH
        ));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), 51006));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), 51061));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), -2010));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), 51168));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), 516001));
        assert!(!is_throttle_error_code(Some(Exchange::Okex), -2018));
    }

    #[test]
    fn clears_only_binance_futures_margin_locks() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        assert!(register_binance_futures_margin_signal_throttle_for_mode(
            "BTCUSDT",
            Side::Buy,
            SIGNAL_THROTTLE_ERROR_CODE_MARGIN_INSUFFICIENT,
            ArbMode::IntraArb,
        ));
        assert!(register_signal_throttle_at(
            "ETHUSDT",
            Side::Buy,
            Some(Exchange::Binance),
            SIGNAL_THROTTLE_ERROR_CODE_MARGIN_INSUFFICIENT,
            100,
            1_000,
        ));

        assert_eq!(clear_binance_futures_margin_signal_throttles(), 1);
        assert!(check_signal_throttle_at("BTCUSDT", Side::Buy, 100).is_none());
        assert!(check_signal_throttle_at("ETHUSDT", Side::Buy, 100).is_some());
        clear_all();
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
    fn registers_gate_market_forbidden_as_account_reduce_only_throttle() {
        let _guard = TEST_LOCK.lock();
        clear_all();
        let now_us = 2_000_000;

        assert!(register_signal_throttle_at(
            "driftusdt",
            Side::Buy,
            Some(Exchange::Gate),
            gate::RISK_CHECK_MARKET_FORBIDDEN,
            now_us,
            GATE_SIGNAL_THROTTLE_TTL_US,
        ));

        let symbol_hit = check_signal_throttle_at("DRIFTUSDT", Side::Buy, now_us + 1)
            .expect("symbol-side throttle must be hit");
        assert_eq!(
            symbol_hit.last_error_code,
            gate::RISK_CHECK_MARKET_FORBIDDEN
        );
        let account_hit = check_account_signal_throttle_at(now_us + 1)
            .expect("account reduce-only throttle must be hit");
        assert_eq!(
            account_hit.last_error_code,
            gate::RISK_CHECK_MARKET_FORBIDDEN
        );
        assert_eq!(account_hit.remaining_us, GATE_SIGNAL_THROTTLE_TTL_US - 1);

        let expires_at_us = now_us + GATE_SIGNAL_THROTTLE_TTL_US;
        assert!(check_signal_throttle_at("DRIFTUSDT", Side::Buy, expires_at_us).is_none());
        assert!(check_account_signal_throttle_at(expires_at_us).is_none());
        clear_all();
    }

    #[test]
    fn registers_single_side_throttle_for_bitget_position_tier_limit() {
        let _guard = TEST_LOCK.lock();
        clear_all();

        assert!(register_signal_throttle_for_mode(
            "filusdt",
            Side::Sell,
            Some(Exchange::Bitget),
            bitget::POSITION_TIER_LIMIT_EXCEEDED,
            ArbMode::FundingArb,
        ));

        assert!(check_signal_throttle("FILUSDT", Side::Buy).is_none());
        assert_eq!(
            check_signal_throttle("FILUSDT", Side::Sell)
                .expect("sell must be blocked")
                .last_error_code,
            bitget::POSITION_TIER_LIMIT_EXCEEDED
        );
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
