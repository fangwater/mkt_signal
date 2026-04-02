use std::cell::RefCell;
use std::collections::HashMap;

use crate::signal::common::TradingVenue;

use super::common::ThresholdKey;

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

pub fn is_cooldown_hit(
    last_ts_map: &RefCell<HashMap<ThresholdKey, i64>>,
    key: &ThresholdKey,
    now: i64,
    signal_cooldown_us: i64,
) -> bool {
    if let Some(&last_ts) = last_ts_map.borrow().get(key) {
        let elapsed = now - last_ts;
        if elapsed < signal_cooldown_us {
            return true;
        }
    }
    false
}

pub fn update_last_ts(
    last_ts_map: &RefCell<HashMap<ThresholdKey, i64>>,
    key: ThresholdKey,
    now: i64,
) {
    last_ts_map.borrow_mut().insert(key, now);
}
