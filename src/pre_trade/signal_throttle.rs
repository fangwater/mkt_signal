use crate::common::exchange::Exchange;
use crate::common::time_util::get_timestamp_us;
use crate::common::trade_error_code::gate;
use crate::pre_trade::order_manager::Side;
use log::{debug, info, warn};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap};

pub const SIGNAL_THROTTLE_TTL_US: i64 = 2 * 60 * 60 * 1_000_000;
pub const SIGNAL_THROTTLE_ERROR_CODE_UM_COLLATERAL_LIMIT: i32 = 51169;
pub const SIGNAL_THROTTLE_ERROR_CODE_MARGIN_INSUFFICIENT: i32 = -2019;
pub const SIGNAL_THROTTLE_ERROR_CODE_MAX_BORROWABLE_EXCEEDED: i32 = 51006;
pub const SIGNAL_THROTTLE_ERROR_CODE_BITGET_LENDING_LIMIT: i32 = 25116;
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
        gate::BALANCE_NOT_ENOUGH
        | gate::MARGIN_NOT_ENOUGH
        | gate::POSITION_MARGIN_TOO_LOW
        | gate::LIQUIDITY_NOT_ENOUGH
        | gate::AUTO_BORROW_TOO_MUCH => matches!(exchange, Some(Exchange::Gate)),
        _ => false,
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
        SIGNAL_THROTTLE_TTL_US,
    )
}

pub fn check_signal_throttle(symbol: &str, dir: Side) -> Option<SignalThrottleHit> {
    let now_us = get_timestamp_us();
    check_signal_throttle_at(symbol, dir, now_us)
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

fn cleanup_expired(map: &mut HashMap<SignalThrottleKey, SignalThrottleEntry>, now_us: i64) {
    map.retain(|_, entry| entry.ban_until_us > now_us);
}

fn side_label_from_u8(value: u8) -> &'static str {
    Side::from_u8(value)
        .map(|side| side.as_str())
        .unwrap_or("UNKNOWN")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clear_all() {
        SIGNAL_THROTTLE_MAP.lock().clear();
    }

    #[test]
    fn detects_throttle_error_code() {
        assert!(is_throttle_error_code(Some(Exchange::Binance), 51169));
        assert!(is_throttle_error_code(Some(Exchange::Binance), -2019));
        assert!(is_throttle_error_code(Some(Exchange::Binance), 51006));
        assert!(is_throttle_error_code(Some(Exchange::Binance), 51061));
        assert!(is_throttle_error_code(Some(Exchange::Bitget), 25116));
        assert!(is_throttle_error_code(
            Some(Exchange::Gate),
            gate::AUTO_BORROW_TOO_MUCH
        ));
        assert!(is_throttle_error_code(
            Some(Exchange::Gate),
            gate::BALANCE_NOT_ENOUGH
        ));
        assert!(!is_throttle_error_code(Some(Exchange::Binance), 25116));
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
    fn registers_and_expires_throttle() {
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

        let hit2 = check_signal_throttle_at("BTCUSDT", Side::Buy, now_us + ttl_us - 1);
        assert!(hit2.is_some());

        let hit3 = check_signal_throttle_at("BTCUSDT", Side::Buy, now_us + ttl_us);
        assert!(hit3.is_none());
    }
}
