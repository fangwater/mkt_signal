//! FR delist dump list shared by Delist and trade_signal.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    pub fn close_deadline(self, now_us: i64) -> Option<i64> {
        let deadline = self.deadline_us.min(self.valid_until_us);
        (self.flatten_requested_us.is_none() && now_us < deadline).then_some(deadline)
    }
}

pub fn redis_key(account: &str, venue_pair: &str) -> String {
    format!("{account}:fr_{DELIST_DUMP_LIST}:{venue_pair}")
}
