//! FR delist dump list and the handoff from pre_trade to the flatten script.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const DELIST_CLOSE_FROM_KEY: &[u8] = b"delist_force_close";
pub const DELIST_CLOSE_QUOTE_MAX_AGE_US: i64 = 10_000_000;
pub const DELIST_DUMP_LIST: &str = "delist_dump_symbols";
pub const DELIST_DUMP_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
pub const FLATTEN_WINDOW_MS: i64 = 12 * 60 * 60 * 1_000;

pub type DelistDumpList = BTreeMap<String, DelistDumpEntry>;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct DelistDumpEntry {
    pub deadline_us: i64,
    pub valid_until_us: i64,
    pub flatten_requested_us: Option<i64>,
}

impl DelistDumpEntry {
    pub fn maker_deadline(self, now_us: i64) -> Option<i64> {
        let deadline = self.deadline_us.min(self.valid_until_us);
        (self.flatten_requested_us.is_none() && now_us < deadline).then_some(deadline)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FlattenReady {
    pub checked_us: i64,
    pub requests: BTreeMap<String, FlattenDrain>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct FlattenDrain {
    pub requested_us: i64,
    pub confirmed_us: i64,
}

pub fn redis_key(account: &str, venue_pair: &str) -> String {
    format!("{account}:fr_{DELIST_DUMP_LIST}:{venue_pair}")
}

pub fn flatten_ready_key(account: &str, venue_pair: &str) -> String {
    format!("{account}:fr_delist_flatten_ready:{venue_pair}")
}
