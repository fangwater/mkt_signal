//! Parallel Tonglian L2 replay into a year+product RocksDB.
//!
//! Writes simulated trades (`Volume` deltas), the last two-sided five-level
//! book of each local second, `OpenInt` changes, and DCE/GFEX best-price
//! order queues (one source row per record). Python 1s CSV / ClickHouse
//! writers are retired.

pub mod baseline_1min;
pub mod codec;
pub mod db;
pub mod events;
pub mod export_1min;
pub mod export_1s;
pub mod export_hfq;
pub mod export_ylabel;
pub mod hfq;
pub mod multipliers;
pub mod session;
pub mod source;
pub mod universe;
pub mod ylabel_1m;

pub use codec::{
    decode_depth, decode_key, decode_oi, decode_queue, decode_trade, encode_depth, encode_key,
    encode_oi, encode_queue, encode_trade, DepthRecord, OiRecord, QueueRecord, TradeRecord,
    CF_REPLAY_META, KIND_DEPTH, KIND_OI, KIND_QUEUE, KIND_TRADE, STATUS_DONE, STATUS_WRITING,
};
pub use db::{
    open_rocksdb, open_rocksdb_read_only, run_replay, Job, ReplayArgs, DEFAULT_ROCKSDB_DIR,
};
pub use export_1s::{exchange_of, is_cffex_product, split_depth_segments, ExportArgs, ExportStats};
pub use session::Exchange;
pub use source::{parse_day, DEFAULT_LOOKBACK_DAYS, DEFAULT_OVERLAP_CUT};
