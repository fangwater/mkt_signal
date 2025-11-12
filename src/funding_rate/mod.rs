//! Funding Rate 资金费率套利信号模块
//!
//! 该模块实现了基于资金费率的套利信号生成逻辑，包括：
//! - 参数从 Redis 动态加载（param_loader）
//! - 状态维护和管理（state）
//! - 信号生成和评估（signal_generator）
//! - 行情频道订阅（mkt_channel）

pub mod param_loader;
pub mod state;
pub mod signal_generator;
pub mod mkt_channel;
pub mod symbol_list;
pub mod rate_fetcher;

// 公共导出
pub use param_loader::{
    SymbolThreshold,
    RateThresholds,
    ParamsSnapshot,
    FundingThresholdEntry,
    StrategyConfig,
    OrderConfig,
    OrderMode,
    approx_equal,
    approx_equal_slice,
};

pub use state::{
    Quote,
    FundingRateData,
    EvaluateDecision,
    QtyStepInfo,
    EngineStats,
};

pub use signal_generator::{
    FundingRateSignalGenerator,
    SignalRequest,
    BackwardSignalSubscriber,
};

pub use mkt_channel::MktChannel;
pub use symbol_list::SymbolList;
pub use rate_fetcher::RateFetcher;
