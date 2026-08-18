#[derive(Debug, Clone)]
pub struct OrderRequestEvent {
    pub req_type: Option<String>, // human-readable request type for logging
    pub endpoint: String,         // e.g. "/papi/v1/um/order"
    pub method: String,           // "POST" | "DELETE" | ...
    /// Signed key/value params, sorted by key before dispatch.
    pub params: Vec<(String, String)>,
    pub weight: Option<u32>,     // default 1
    pub account: Option<String>, // choose specific account key
    pub req_id: Option<String>,  // optional correlation id
    pub counts_toward_order_limit: bool,
    /// Override Binance `recvWindow`. `None` uses the trading default.
    pub recv_window_ms: Option<u64>,
}

impl OrderRequestEvent {
    pub fn weight(&self) -> u32 {
        self.weight.unwrap_or(1)
    }
}

// 仅保留请求事件，响应已统一改为二进制封装
