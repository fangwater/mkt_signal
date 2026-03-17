// Normal module declarations
pub mod account;
pub mod bridge;
pub mod common;
pub mod connection;
pub mod depth_pub;
pub mod factor_pub;
pub mod fr_signal_dashboard;
pub mod funding_rate;
pub mod kline_pub;
pub mod market_maker;
pub mod mkt_pub;
pub mod parser;
pub mod persist_manager;
pub mod portfolio_margin;
pub mod pre_trade;
pub mod rolling_metrics;
pub mod signal;
pub mod strategy;
pub mod symbol_match;
pub mod trade_engine;
pub mod viz;

// Re-export frequently used modules for backward compatibility
pub use common::{exchange, mkt_msg};
pub use mkt_pub::{app, cfg, iceoryx_forwarder, proxy, sub_msg};
pub use trade_engine::{config::ApiKey, TradeEngine};
