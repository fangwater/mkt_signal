use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize)]
pub struct OrderRequestEvent {
    pub endpoint: String, // e.g. "/papi/v1/um/order"
    pub method: String,   // "POST" | "DELETE" | ...
    pub params: BTreeMap<String, String>, // key/value params (will be signed)
    pub weight: Option<u32>,              // default 1
    pub account: Option<String>,          // choose specific account key
    pub req_id: Option<String>,           // optional correlation id
}

impl OrderRequestEvent {
    pub fn weight(&self) -> u32 { self.weight.unwrap_or(1) }
}

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct OrderResponseEvent {
    pub ok: bool,
    pub status: u16,
    pub ip: String,
    pub account: String,
    pub endpoint: String,
    pub method: String,
    pub req_id: Option<String>,
    pub used_weight_1m: Option<u32>,
    pub order_count_1m: Option<u32>,
    pub body: String,
}
