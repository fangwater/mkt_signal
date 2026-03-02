use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::order_manager::Side;
use log::warn;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;

pub const SIGNAL_THROTTLE_TTL_US: i64 = 30 * 60 * 1_000_000;
pub const SIGNAL_THROTTLE_ERROR_CODE: i32 = 51169;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct SignalThrottleKey {
    symbol: String,
    dir: u8,
    tp: String,
}

#[derive(Debug, Clone)]
struct SignalThrottleEntry {
    ban_until_us: i64,
    last_error_code: i32,
    updated_at_us: i64,
}

#[derive(Debug, Clone)]
pub struct SignalThrottleHit {
    pub tp: String,
    pub remaining_us: i64,
    pub until_us: i64,
    pub last_error_code: i32,
}

static SIGNAL_THROTTLE_MAP: Lazy<Mutex<HashMap<SignalThrottleKey, SignalThrottleEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

impl SignalThrottleKey {
    fn new(symbol: &str, dir: Side, from_key: &str) -> Self {
        Self {
            symbol: symbol.trim().to_ascii_uppercase(),
            dir: dir.to_u8(),
            tp: extract_tp_from_key(from_key),
        }
    }
}

pub fn is_throttle_error_code(error_code: i32) -> bool {
    error_code == SIGNAL_THROTTLE_ERROR_CODE
}

pub fn register_signal_throttle(symbol: &str, dir: Side, from_key: &str, error_code: i32) -> bool {
    let now_us = get_timestamp_us();
    register_signal_throttle_at(
        symbol,
        dir,
        from_key,
        error_code,
        now_us,
        SIGNAL_THROTTLE_TTL_US,
    )
}

pub fn check_signal_throttle(symbol: &str, dir: Side, from_key: &str) -> Option<SignalThrottleHit> {
    let now_us = get_timestamp_us();
    check_signal_throttle_at(symbol, dir, from_key, now_us)
}

fn register_signal_throttle_at(
    symbol: &str,
    dir: Side,
    from_key: &str,
    error_code: i32,
    now_us: i64,
    ttl_us: i64,
) -> bool {
    if !is_throttle_error_code(error_code) {
        return false;
    }

    let ttl_us = ttl_us.max(0);
    let ban_until_us = now_us.saturating_add(ttl_us);
    let mut guard = SIGNAL_THROTTLE_MAP.lock();
    cleanup_expired(&mut guard, now_us);

    let key = SignalThrottleKey::new(symbol, dir, from_key);
    let tp = key.tp.clone();
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
        "SignalThrottle: register block symbol={} dir={} tp={} code={} block_for={}s until_us={}",
        key.symbol,
        dir.as_str(),
        tp,
        error_code,
        ttl_us / 1_000_000,
        ban_until_us
    );
    true
}

fn check_signal_throttle_at(
    symbol: &str,
    dir: Side,
    from_key: &str,
    now_us: i64,
) -> Option<SignalThrottleHit> {
    let key = SignalThrottleKey::new(symbol, dir, from_key);
    let tp = key.tp.clone();
    let mut guard = SIGNAL_THROTTLE_MAP.lock();
    cleanup_expired(&mut guard, now_us);
    let entry = guard.get(&key)?;
    Some(SignalThrottleHit {
        tp,
        remaining_us: entry.ban_until_us.saturating_sub(now_us),
        until_us: entry.ban_until_us,
        last_error_code: entry.last_error_code,
    })
}

fn cleanup_expired(map: &mut HashMap<SignalThrottleKey, SignalThrottleEntry>, now_us: i64) {
    map.retain(|_, entry| entry.ban_until_us > now_us);
}

fn extract_tp_from_key_value_token(token: &str) -> Option<String> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return None;
    }

    for separator in ['=', ':'] {
        if let Some((k, v)) = trimmed.split_once(separator) {
            if k.trim().eq_ignore_ascii_case("tp") {
                let value = v.trim();
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

fn extract_tp_from_key(from_key: &str) -> String {
    let trimmed = from_key.trim();
    if trimmed.is_empty() {
        return "default".to_string();
    }

    for token in trimmed.split(|c: char| c == '|' || c == ',' || c == ';' || c.is_whitespace()) {
        if let Some(tp) = extract_tp_from_key_value_token(token) {
            return tp;
        }
    }

    if let Some(first) = trimmed.split(':').next() {
        let first = first.trim();
        if !first.is_empty() {
            return first.to_string();
        }
    }

    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clear_all() {
        SIGNAL_THROTTLE_MAP.lock().clear();
    }

    #[test]
    fn detects_throttle_error_code() {
        assert!(is_throttle_error_code(51169));
        assert!(!is_throttle_error_code(51168));
        assert!(!is_throttle_error_code(516001));
        assert!(!is_throttle_error_code(-2019));
    }

    #[test]
    fn extracts_tp_from_key() {
        assert_eq!(extract_tp_from_key("tp=12345|foo=bar"), "12345");
        assert_eq!(extract_tp_from_key("x=1,tp:abc"), "abc");
        assert_eq!(
            extract_tp_from_key("1722333123000000:0.1:0.2"),
            "1722333123000000"
        );
        assert_eq!(extract_tp_from_key(""), "default");
    }

    #[test]
    fn registers_and_expires_throttle() {
        clear_all();
        let symbol = "btcusdt";
        let from_key = "1722333123000000:0.1:0.2";
        let now_us = 1_000_000;
        let ttl_us = 30;

        assert!(register_signal_throttle_at(
            symbol,
            Side::Buy,
            from_key,
            51169,
            now_us,
            ttl_us
        ));

        let hit1 = check_signal_throttle_at("BTCUSDT", Side::Buy, from_key, now_us + 1)
            .expect("throttle must be hit");
        assert_eq!(hit1.tp, "1722333123000000");
        assert_eq!(hit1.last_error_code, 51169);

        let hit2 = check_signal_throttle_at("BTCUSDT", Side::Buy, from_key, now_us + ttl_us - 1);
        assert!(hit2.is_some());

        let hit3 = check_signal_throttle_at("BTCUSDT", Side::Buy, from_key, now_us + ttl_us);
        assert!(hit3.is_none());
    }
}
