//! 信号生成器模块
//!
//! 核心资金费率套利信号生成器，负责：
//! - 订阅行情和资金费率数据
//! - 评估信号条件
//! - 生成开仓/平仓/撤单信号
//! - 处理 MM backward 信号
//! - 定期重采样和发布

use anyhow::Result;

// ===== 占位结构（实际实现在 fr_signal.rs 中）=====

pub enum SignalRequest {} 
pub struct BackwardSignalSubscriber {} 

// ===== 主信号生成器 =====

/// 资金费率套利信号生成器
///
/// 注意：完整实现在 funding_rate_strategy_shared.rs 中，
/// 此处仅作为接口定义，实际使用时直接使用 shared.rs 中的 StrategyEngine
pub struct FundingRateSignalGenerator {
    // 实现细节见 funding_rate_strategy_shared.rs 的 StrategyEngine
}

// ===== 辅助函数 =====
pub fn forward_open_ready(bidask_sr: Option<f64>, threshold: f64) -> bool {
    bidask_sr.map(|sr| sr <= threshold).unwrap_or(false)
}

pub fn forward_close_ready(askbid_sr: Option<f64>, threshold: f64) -> bool {
    threshold.is_finite() && askbid_sr.map(|sr| sr >= threshold).unwrap_or(false)
} 

pub fn opt_finite(val: f64) -> Option<f64> {
    if val.is_finite() { Some(val) } else { None }
} 

pub fn opt_active_threshold(val: f64) -> Option<f64> {
    if val.is_finite() && val.abs() > f64::EPSILON { Some(val) } else { None }
}

pub fn approx_zero(x: f64) -> bool {
    x.abs() < 1e-12
}
