pub mod unified_order;

pub use unified_order::UnifiedOrderRecord;

pub const TRADE_UPDATE_RECORD_CHANNEL: &str = "trade_update_record";
pub const TRADE_UPDATE_UNMATCHED_RECORD_CHANNEL: &str = "trade_update_unmatched_record";
pub const ORDER_UPDATE_RECORD_CHANNEL: &str = "order_update_record";
pub const ORDER_UPDATE_UNMATCHED_RECORD_CHANNEL: &str = "order_update_unmatched_record";
pub const UNIFORM_ORDER_RECORD_CHANNEL: &str = "uniform_order_record";
