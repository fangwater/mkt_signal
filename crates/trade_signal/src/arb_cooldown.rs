use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;

use order_common::TradingVenue;

use super::common::ThresholdKey;

const INLINE_SYMBOL_CAPACITY: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CooldownSymbolKey {
    Inline {
        len: u8,
        bytes: [u8; INLINE_SYMBOL_CAPACITY],
    },
    Heap(Box<str>),
}

impl CooldownSymbolKey {
    fn new(symbol: &str) -> Self {
        if symbol.len() <= INLINE_SYMBOL_CAPACITY {
            let mut bytes = [0; INLINE_SYMBOL_CAPACITY];
            bytes[..symbol.len()].copy_from_slice(symbol.as_bytes());
            Self::Inline {
                len: symbol.len() as u8,
                bytes,
            }
        } else {
            Self::Heap(symbol.into())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CooldownSymbolPairKey {
    Same(CooldownSymbolKey),
    Different(Box<[CooldownSymbolKey; 2]>),
}

/// Compact runtime key for cooldown maps. Typical intra symbols are stored inline once.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SignalCooldownKey {
    venues: (TradingVenue, TradingVenue),
    symbols: CooldownSymbolPairKey,
    side: u8,
}

impl SignalCooldownKey {
    pub fn new(
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        side: u8,
    ) -> Self {
        let open_symbol = uppercase_cow(open_symbol);
        let hedge_symbol = uppercase_cow(hedge_symbol);
        Self::from_canonical(
            open_symbol.as_ref(),
            hedge_symbol.as_ref(),
            open_venue,
            hedge_venue,
            side,
        )
    }

    pub fn from_canonical(
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        side: u8,
    ) -> Self {
        let symbols = if open_symbol == hedge_symbol {
            CooldownSymbolPairKey::Same(CooldownSymbolKey::new(open_symbol))
        } else {
            CooldownSymbolPairKey::Different(Box::new([
                CooldownSymbolKey::new(open_symbol),
                CooldownSymbolKey::new(hedge_symbol),
            ]))
        };
        Self {
            venues: (open_venue, hedge_venue),
            symbols,
            side,
        }
    }
}

fn uppercase_cow(symbol: &str) -> Cow<'_, str> {
    if symbol.is_ascii() && !symbol.bytes().any(|byte| byte.is_ascii_lowercase()) {
        Cow::Borrowed(symbol)
    } else {
        Cow::Owned(symbol.to_uppercase())
    }
}

pub fn threshold_key(
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> ThresholdKey {
    (
        open_venue,
        open_symbol.to_uppercase(),
        hedge_venue,
        hedge_symbol.to_uppercase(),
    )
}

pub fn is_cooldown_hit<K>(
    last_ts_map: &RefCell<HashMap<K, i64>>,
    key: &K,
    now: i64,
    signal_cooldown_us: i64,
) -> bool
where
    K: Eq + Hash,
{
    if let Some(&last_ts) = last_ts_map.borrow().get(key) {
        let elapsed = now - last_ts;
        if elapsed < signal_cooldown_us {
            return true;
        }
    }
    false
}

pub fn update_last_ts<K>(last_ts_map: &RefCell<HashMap<K, i64>>, key: K, now: i64)
where
    K: Eq + Hash,
{
    last_ts_map.borrow_mut().insert(key, now);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_key_is_case_insensitive_and_keeps_side_distinct() {
        let lower = SignalCooldownKey::new(
            "btcusdt",
            "btcusdt",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
            1,
        );
        let upper = SignalCooldownKey::new(
            "BTCUSDT",
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
            1,
        );
        let other_side = SignalCooldownKey::new(
            "BTCUSDT",
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
            2,
        );

        assert_eq!(lower, upper);
        assert_ne!(upper, other_side);
        assert!(matches!(upper.symbols, CooldownSymbolPairKey::Same(_)));
    }

    #[test]
    fn compact_key_supports_long_and_different_symbols() {
        let key = SignalCooldownKey::new(
            "AN_EXCEPTIONALLY_LONG_OPEN_SYMBOL_OVER_32_BYTES",
            "AN_EXCEPTIONALLY_LONG_HEDGE_SYMBOL_OVER_32_BYTES",
            TradingVenue::OkexMargin,
            TradingVenue::OkexFutures,
            1,
        );

        assert!(matches!(key.symbols, CooldownSymbolPairKey::Different(_)));
    }
}
