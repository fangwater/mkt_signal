use crate::signal::trade_signal;

#[derive(Debug)]
pub enum PreTradeEvent {
    Account(AccountEvent),
    TradeResponse(TradeEngineResponse),
    Signal(trade_signal::TradeSignal),
} 

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
    pub local_recv_time: i64,
    pub client_order_id: i64,
    pub exchange: u32,
    pub status: u16,
    pub ip_used_weight_1m: Option<u32>,
    pub order_count_1m: Option<u32>,
    pub body: Vec<u8>,
    pub body_truncated: bool,
} 

