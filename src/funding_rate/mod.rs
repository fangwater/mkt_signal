//! Funding Rate 资金费率套利信号模块
//!
//! 该模块实现了基于资金费率的套利信号生成逻辑，包括：
//! - 参数从 Redis 动态加载（param_loader）
//! - 状态维护和管理（state）
//! - 信号生成和评估（signal_generator）
//! - 行情频道订阅（mkt_channel）

pub mod common;
pub mod decision;
pub mod funding_rate_factor;
pub mod mkt_channel;
pub mod param_loader;
pub mod rate_fetcher;
pub mod signal_generator;
pub mod spread_factor;
pub mod state;
pub mod symbol_list;

// 公共导出
pub use param_loader::{
    approx_equal, approx_equal_slice, FundingThresholdEntry, OrderConfig, OrderMode,
    ParamsSnapshot, RateThresholds, StrategyConfig, SymbolThreshold,
};

pub use state::{EngineStats, EvaluateDecision, FundingRateData, QtyStepInfo, Quote, SymbolState};

pub use signal_generator::{BackwardSignalSubscriber, FundingRateSignalGenerator, SignalRequest};

pub use mkt_channel::MktChannel;
pub use rate_fetcher::RateFetcher;
pub use symbol_list::SymbolList;

// 导出通用定义
pub use common::{
    ArbDirection, CompareOp, FactorMode, OperationType, SymbolPair, ThresholdKey, VenuePair,
};

// 导出价差因子相关
pub use spread_factor::{SpreadFactor, SpreadThresholdConfig, SpreadType};

// 导出资金费率因子相关
pub use funding_rate_factor::{FrThresholdConfig, FundingRateFactor};

// 导出决策模块
pub use decision::FrDecision;
