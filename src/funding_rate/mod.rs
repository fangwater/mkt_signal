//! Funding Rate 资金费率套利信号模块
//!
//! 该模块实现了基于资金费率的套利信号生成逻辑，包括：
//! - 通用定义（common）：枚举类型、数据结构、辅助函数
//! - 配置加载器（config_loader）：统一的 Redis 配置热加载
//! - 策略参数（strategy_loader）：策略参数结构定义
//! - 阈值加载器：
//!   - fr_threshold_loader：资金费率阈值加载
//!   - spread_threshold_loader：价差阈值加载
//! - 行情频道（mkt_channel）：市场数据订阅

pub mod common;
pub mod config_loader;
pub mod decision;
pub mod fr_threshold_loader;
pub mod funding_rate_factor;
pub mod mkt_channel;
pub mod rate_fetcher;
pub mod spread_factor;
pub mod spread_threshold_loader;
pub mod strategy_loader;
pub mod symbol_list;

// 公共导出 - 通用定义（枚举、数据结构、辅助函数）
pub use common::{
    approx_equal, approx_equal_slice, parse_numeric_list, ArbDirection, CompareOp, FactorMode,
    FundingRateData, FundingRatePeriod, OperationType, Quote, RateFetcherTrait, SymbolPair,
    ThresholdKey, VenuePair,
};

// 公共导出 - 统一配置加载器
pub use config_loader::{load_all_once, spawn_config_loader};

// 公共导出 - 阈值加载器
pub use fr_threshold_loader::load_from_redis as load_fr_thresholds;
pub use spread_threshold_loader::load_from_redis as load_spread_thresholds;

// 公共导出 - 单例访问器
pub use decision::FrDecision;
pub use mkt_channel::MktChannel;
pub use rate_fetcher::{ExchangeConfig, RateFetcher, BINANCE_CONFIG, OKEX_CONFIG};
pub use symbol_list::SymbolList;

// 公共导出 - 价差因子
pub use spread_factor::{SpreadFactor, SpreadThresholdConfig, SpreadType};

// 公共导出 - 资金费率因子
pub use funding_rate_factor::{FrThresholdConfig, FundingRateFactor};
