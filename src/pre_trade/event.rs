#[derive(Debug)]
pub struct AccountEvent {
    pub service: String,
    pub received_at: i64,
    pub payload: Vec<u8>,
    pub payload_len: usize,
    pub event_type: Option<String>,
    pub event_time_ms: Option<i64>,
}

#[derive(Debug)]
pub struct TradeEngineResponse {
    pub service: String,
    pub received_at: i64,
    pub payload_len: usize,
    pub req_type: u32,
    pub client_order_id: i64,
    pub exchange: u32,
    pub status: u16,
    pub error_code: i32,
}
