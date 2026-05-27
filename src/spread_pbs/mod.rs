//! spread_pbs：独立的 askbidspread 高速发布进程。
//!
//! - 单 venue 单进程，`current_thread` runtime + sched_setaffinity 绑核
//! - 双路 ws（primary/secondary）按 per-venue seq 字段去重
//! - IceOryx 服务名 `spread_pbs/<venue>/ask_bid_spread`，与 dat_pbs 完全独立
//! - 每 10000 条用 KLL 算法输出延迟 p90/p95/p99
//!
//! 已支持的 venue（OKex/Binance/Bybit/Gate/Bitget × spot+futures = 10 个）。

pub mod adapter;
pub mod app;
pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod gate;
pub mod gate_sbe;
pub mod latency;
pub mod okex;
pub mod publisher;
pub mod ws;

pub use adapter::{create_adapter, BboFrame, KeepaliveSpec, VenueAdapter};
pub use app::SpreadPbsApp;
