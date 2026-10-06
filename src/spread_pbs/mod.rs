//! spread_pbs：独立的 askbidspread 高速发布进程。
//!
//! - 单进程可覆盖多个 venue，`current_thread` runtime + sched_setaffinity 绑核
//! - Binance futures 入口同时连接 USD-M / COIN-M，按合约类型保留各自的 IPC 服务
//! - 双路 ws（primary/secondary）按 per-venue seq 字段去重
//! - IceOryx 服务名 `spread_pbs/<venue>/ask_bid_spread`，与 dat_pbs 完全独立
//! - Binance USD-M/COIN-M 行情共用 `binance-futures`，通过 symbol 区分原始市场
//!
//! 已支持的 venue：OKex/Binance/Bybit/Gate/Bitget/Hyperliquid。

pub mod adapter;
pub mod app;
pub mod binance;
pub mod binance_fix_sbe;
pub mod bitget;
pub mod bybit;
pub mod gate;
pub mod gate_sbe;
pub mod hyperliquid;
pub mod latency;
pub mod okex;
pub mod okex_derivatives;
pub mod publisher;
pub mod rapidx;
pub mod ws;
pub mod zmq_forward;

pub use adapter::{
    create_adapter, BboFrame, IncrementalDedupPolicy, IncrementalFrame, KeepaliveSpec,
    TradeDedupPolicy, TradeFrame, VenueAdapter,
};
pub use app::{BinanceFuturesRole, BybitRole, MarketDataProvider, SpreadPbsApp};
