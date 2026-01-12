//! Depth Publisher 模块
//!
//! 订阅 mkt_pub 的 incremental 数据，维护订单簿状态，
//! 并发布 depth5/depth20/depth50 深度快照

pub mod cfg;
pub mod orderbook;
pub mod depth_msg;
pub mod publisher;
pub mod app;
