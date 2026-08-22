pub mod order_queue_position;
pub mod unified_order;

pub use order_queue_position::{
    OrderQueuePositionAction, OrderQueuePositionMsg, OrderQueuePositionMsgType,
    OrderQueuePositionRecord, ORDER_QUEUE_POSITION_MAX_BYTES, ORDER_QUEUE_POSITION_MSG_BYTES,
};
pub use unified_order::{SignalBbo, SignalBboLeg, UnifiedOrderRecord, SIGNAL_BBO_BINARY_LEN};

pub const TRADE_UPDATE_RECORD_CHANNEL: &str = "trade_update_record";
pub const TRADE_UPDATE_UNMATCHED_RECORD_CHANNEL: &str = "trade_update_unmatched_record";
pub const ORDER_UPDATE_RECORD_CHANNEL: &str = "order_update_record";
pub const ORDER_UPDATE_UNMATCHED_RECORD_CHANNEL: &str = "order_update_unmatched_record";
pub const ORDER_QUEUE_POSITION_RECORD_CHANNEL: &str = "order_queue_position_record";
pub const ORDER_QUEUE_POSITION_RECORD_MAX_PUBLISHERS: usize = 1;
pub const UNIFORM_ORDER_RECORD_CHANNEL: &str = "uniform_order_record";
