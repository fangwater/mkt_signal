//! spread_pbs：独立的 askbidspread 高速发布进程。
//!
//! - 单 venue 单进程，`current_thread` runtime + sched_setaffinity 绑核
//! - 双路 ws（primary/secondary）按 OKex `seqId` 去重
//! - IceOryx 服务名 `spread_pbs/<venue>/ask_bid_spread`，与 dat_pbs 完全独立
//! - 每 10000 条用 KLL 算法输出延迟 p90/p95/p99

pub mod affinity;
pub mod app;
pub mod latency;
pub mod okex;
pub mod publisher;
pub mod ws;

pub use app::SpreadPbsApp;
