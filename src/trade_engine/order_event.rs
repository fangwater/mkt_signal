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

// 仅保留请求事件，响应已统一改为二进制封装
