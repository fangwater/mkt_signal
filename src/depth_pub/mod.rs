//! Depth Publisher 模块
//!
//! 订阅 mkt_pub 的 incremental 数据，维护订单簿状态，
//! 并发布 depth25/depth50 深度快照

pub mod app;
pub mod cfg;
pub mod depth_msg;
pub mod orderbook;
pub mod publisher;
pub mod query_msg;
