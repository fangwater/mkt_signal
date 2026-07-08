use prost::Message;

#[derive(Clone, PartialEq, Message)]
pub struct PriceLevel {
    #[prost(double, tag = "1")]
    pub price: f64,
    #[prost(double, tag = "2")]
    pub amount: f64,
}

#[derive(Clone, PartialEq, Message)]
pub struct IncrementOrderBookInfo {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
    #[prost(bool, tag = "2")]
    pub is_snapshot: bool,
    #[prost(message, repeated, tag = "3")]
    pub bids: Vec<PriceLevel>,
    #[prost(message, repeated, tag = "4")]
    pub asks: Vec<PriceLevel>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TradeInfo {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
    #[prost(string, tag = "2")]
    pub side: String,
    #[prost(double, tag = "3")]
    pub price: f64,
    #[prost(double, tag = "4")]
    pub amount: f64,
}

#[derive(Clone, PartialEq, Message)]
pub struct SymbolInfo {
    #[prost(string, tag = "1")]
    pub symbol: String,
    #[prost(message, repeated, tag = "2")]
    pub trades: Vec<TradeInfo>,
    #[prost(message, repeated, tag = "3")]
    pub incs: Vec<IncrementOrderBookInfo>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PeriodMessage {
    #[prost(int64, tag = "1")]
    pub period: i64,
    #[prost(int64, tag = "2")]
    pub ts: i64,
    #[prost(int64, tag = "3")]
    pub post_ts: i64,
    #[prost(string, tag = "4")]
    pub poster_id: String,
    #[prost(message, repeated, tag = "5")]
    pub symbol_infos: Vec<SymbolInfo>,
}

pub fn encode_period_message(message: &PeriodMessage) -> Vec<u8> {
    message.encode_to_vec()
}
