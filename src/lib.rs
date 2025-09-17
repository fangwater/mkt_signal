// Normal module declarations
pub mod common;
pub mod connection;
pub mod market_state;
pub mod mkt_pub;
pub mod parser;
pub mod portfolio_margin;
pub mod pre_trade;
pub mod trade_engine;

// Re-export frequently used modules for backward compatibility
pub use common::{exchange, mkt_msg, signal_event};
pub use mkt_pub::{app, cfg, iceoryx_forwarder, proxy, sub_msg};
pub use trade_engine::{
    config::{ApiKey, TradeEngineCfg},
    TradeEngine,
};
