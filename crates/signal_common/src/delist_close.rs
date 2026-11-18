//! Deadline schedule and attribution shared by Delist, trade_signal and pre_trade.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const DELIST_CLOSE_FROM_KEY: &[u8] = b"delist_force_close";
pub const DELIST_CLOSE_QUOTE_MAX_AGE_US: i64 = 10_000_000;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DelistCloseSchedule {
    pub valid_until_us: i64,
    pub deadlines_us: BTreeMap<String, i64>,
}

pub fn redis_key(account: &str, venue_pair: &str) -> String {
    format!("{account}:fr_delist_force_close:{venue_pair}")
}
