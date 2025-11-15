//! Funding Rate 资金费率套利信号模块
//!
//! 该模块实现了基于资金费率的套利信号生成逻辑，包括：
//! - 配置数据结构定义（config）
//! - 辅助工具函数（utils）
//! - 策略参数加载器（strategy_loader）
//! - 状态维护和管理（state）
//! - 行情频道订阅（mkt_channel）

pub mod common;
pub mod config;
pub mod decision;
pub mod funding_rate_factor;
pub mod mkt_channel;
pub mod rate_fetcher;
pub mod spread_factor;
pub mod state;
pub mod strategy_loader;
pub mod symbol_list;
pub mod utils;

// 公共导出 - 配置相关
pub use config::{
    FundingThresholdEntry, OrderConfig, OrderMode, ParamsSnapshot, RateThresholds,
    StrategyConfig, StrategyParams, SymbolThreshold,
};

// 公共导出 - 辅助函数
pub use utils::{approx_equal, approx_equal_slice, parse_numeric_list};

// 公共导出 - 策略加载器
pub use strategy_loader::{load_params_once, spawn_params_loader};

// 公共导出 - 状态相关
pub use state::{FundingRateData, Quote};

// 公共导出 - 单例访问器
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
