//! 配置数据结构定义
//!
//! 包含阈值和状态相关的配置数据结构：
//! - SymbolThreshold: 价差阈值
//! - RateThresholds: 资金费率阈值
//! - FundingThresholdEntry: 阈值条目
//! - ParamsSnapshot: 参数快照
//!
//! 注意：策略参数现在从 Redis 动态加载，参见 strategy_loader.rs

// ===== 阈值和状态 =====

/// 价差阈值配置
#[derive(Debug, Clone)]
pub struct SymbolThreshold {
    pub spot_symbol: String,
    pub futures_symbol: String,
    // BidAskSR 阈值（开/关）: (spot_bid - fut_ask) / spot_bid
    pub forward_open_threshold: f64,
    pub forward_cancel_threshold: f64,
    // AskBidSR 阈值（平仓）: (spot_ask - fut_bid) / spot_ask
    pub forward_close_threshold: f64,
    // AskBidSR 阈值（平仓辅助，用于撤单优化）
    pub forward_cancel_close_threshold: Option<f64>,
}

/// 资金费率阈值（按频率区分）
#[derive(Debug, Clone, Copy)]
pub struct RateThresholds {
    pub open_upper: f64,
    pub open_lower: f64,
    pub close_lower: f64,
    pub close_upper: f64,
}

impl RateThresholds {
    pub const fn for_8h() -> Self {
        Self {
            open_upper: 0.00008,
            open_lower: -0.00008,
            close_lower: -0.001,
            close_upper: 0.001,
        }
    }

    pub const fn for_4h() -> Self {
        Self {
            open_upper: 0.00004,
            open_lower: -0.00004,
            close_lower: -0.0008,
            close_upper: 0.0008,
        }
    }
}

impl Default for RateThresholds {
    fn default() -> Self {
        Self::for_8h()
    }
}

/// 参数快照（用于检测参数变更）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParamsSnapshot {
    pub interval: u64,
    pub predict_num: u64,
    pub refresh_secs: u64,
    pub fetch_secs: u64,
    pub fetch_offset_secs: u64,
    pub history_limit: u64,
    pub settlement_offset_secs: i64,
}

/// 资金费率阈值条目
#[derive(Debug, Clone, Default)]
pub struct FundingThresholdEntry {
    pub symbol: String,
    pub predict_funding_rate: f64,
    pub lorn_rate: f64,
    pub funding_frequency: String, // "4h" | "8h"
    pub open_upper_threshold: f64,
    pub open_lower_threshold: f64,
    pub close_lower_threshold: f64,
    pub close_upper_threshold: f64,
}
