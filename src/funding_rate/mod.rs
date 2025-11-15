//! Funding Rate 资金费率套利信号模块
//!
//! 该模块实现了基于资金费率的套利信号生成逻辑，包括：
//! - 通用定义（common）：枚举类型、数据结构、辅助函数
//! - 配置管理（config）：配置数据结构定义
//! - 策略加载器（strategy_loader）：Redis 参数动态加载
//! - 行情频道（mkt_channel）：市场数据订阅

pub mod common;
pub mod config;
pub mod decision;
pub mod funding_rate_factor;
pub mod mkt_channel;
pub mod rate_fetcher;
pub mod spread_factor;
pub mod strategy_loader;
pub mod symbol_list;

// 公共导出 - 通用定义（枚举、数据结构、辅助函数）
pub use common::{
    approx_equal, approx_equal_slice, parse_numeric_list, ArbDirection, CompareOp, FactorMode,
    FundingRateData, OperationType, Quote, SymbolPair, ThresholdKey, VenuePair,
};

// 公共导出 - 配置相关
pub use config::{FundingThresholdEntry, ParamsSnapshot, RateThresholds, SymbolThreshold};

// 公共导出 - 策略加载器
pub use strategy_loader::{load_params_once, spawn_params_loader};

// 公共导出 - 单例访问器
pub use decision::FrDecision;
pub use mkt_channel::MktChannel;
pub use rate_fetcher::RateFetcher;
pub use symbol_list::SymbolList;

// 公共导出 - 价差因子
pub use spread_factor::{SpreadFactor, SpreadThresholdConfig, SpreadType};

// 公共导出 - 资金费率因子
pub use funding_rate_factor::{FrThresholdConfig, FundingRateFactor};
