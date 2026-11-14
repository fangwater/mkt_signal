#[cfg(feature = "runtime")]
mod bbo_spread;
#[cfg(feature = "runtime")]
pub mod exporter;
#[cfg(feature = "runtime")]
mod hyperliquid_account_fact;
#[cfg(feature = "runtime")]
mod iceoryx;
#[cfg(feature = "runtime")]
pub mod native_fill_import;
#[cfg(feature = "runtime")]
mod order_queue_position;
#[cfg(feature = "runtime")]
mod order_update;
#[cfg(feature = "runtime")]
pub mod parquet;
#[cfg(feature = "runtime")]
mod polling;
#[cfg(feature = "runtime")]
pub mod rapidx_execution;
#[cfg(feature = "runtime")]
pub mod rapidx_reconcile;
#[cfg(feature = "runtime")]
pub mod rapidx_statement;
#[cfg(feature = "runtime")]
pub mod read_server;
#[cfg(feature = "runtime")]
mod runtime_common;
#[cfg(feature = "runtime")]
pub mod sync;
#[cfg(feature = "runtime")]
mod trade_update;
#[cfg(feature = "runtime")]
pub mod unified_order;
#[cfg(feature = "runtime")]
mod uniform_order_persist;

#[cfg(feature = "embedded")]
pub mod cn_futures;
#[cfg(feature = "embedded")]
mod storage;
#[cfg(feature = "embedded")]
pub use storage::{RocksDbStore, RocksDbTuning};

#[cfg(feature = "runtime")]
include!("manager.rs");
