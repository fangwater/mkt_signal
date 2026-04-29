//! Funding Rate / Arbitrage decision module.
//!
//! Runtime decision logic is centralized in `arb_decision`.
//! Legacy FR/XARB implementations have been folded into that single arb core.

pub mod arb_cancel_context;
pub mod arb_cancel_emit;
pub mod arb_cooldown;
pub mod arb_decision;
pub mod arb_emit;
pub mod arb_from_key;
pub mod arb_mode;
pub mod arb_open_context;
pub mod arb_open_filter;
pub mod arb_qty_align;
pub mod arb_quote_plan;
pub mod arb_tlen_cancel;
pub mod common;
pub mod config_loader;
pub mod decision_router;
pub mod factor_value_hub;
pub mod fr_threshold_loader;
pub mod funding_rate_factor;
pub mod inline_volatility;
pub mod mkt_channel;
pub mod mm_decision;
pub mod rate_fetcher;
pub mod rolling_threshold_sync;
pub mod spread_factor;
pub mod spread_threshold_loader;
pub mod strategy_loader;
pub mod symbol_list;
pub mod tlen_threshold_loader;
pub mod xarb_funding_threshold_loader;

// 公共导出 - 通用定义（枚举、数据结构、辅助函数）
pub use common::{
    approx_equal, approx_equal_slice, parse_numeric_list, venue_pair_for_exchange, ArbDirection,
    CompareOp, FactorMode, FundingRateData, FundingRatePeriod, OperationType, Quote,
    RateFetcherTrait, SymbolPair, ThresholdKey, VenuePair,
};

// 公共导出 - 统一配置加载器
pub use config_loader::{
    load_all_once, load_all_once_with_namespace, spawn_config_loader,
    spawn_config_loader_with_namespace,
};

// 公共导出 - 阈值加载器
pub use fr_threshold_loader::load_from_redis as load_fr_thresholds;
pub use spread_threshold_loader::load_from_redis as load_spread_thresholds;

// 公共导出 - 单例访问器
pub use arb_decision::{
    ArbDecision, ArbSignalKind, DEFAULT_ARBITRAGE_BACKWARD_CHANNEL,
    DEFAULT_ARBITRAGE_SIGNAL_CHANNEL,
};
pub use arb_mode::ArbMode;
pub use decision_router::{init_decision_branch, trigger_decision, DecisionBranch};
pub use mkt_channel::MktChannel;
pub use mm_decision::MmDecision;
pub use rate_fetcher::{ExchangeConfig, RateFetcher, BINANCE_CONFIG, OKEX_CONFIG};
pub use symbol_list::SymbolList;

// 公共导出 - 价差因子
pub use spread_factor::{SpreadFactor, SpreadThresholdConfig, SpreadType};

// 公共导出 - 资金费率因子
pub use funding_rate_factor::{FrThresholdConfig, FundingRateFactor};
