//! 资金费率模块通用定义
//!
//! 包含套利策略中通用的枚举类型和类型别名

use crate::signal::common::TradingVenue;

/// 比较方向枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompareOp {
    /// 大于
    GreaterThan,
    /// 小于
    LessThan,
}

impl CompareOp {
    /// 判断给定值是否满足比较条件
    pub fn check(&self, value: f64, threshold: f64) -> bool {
        match self {
            CompareOp::GreaterThan => value > threshold,
            CompareOp::LessThan => value < threshold,
        }
    }
}

/// 套利方向枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArbDirection {
    /// 正套
    Forward,
    /// 反套
    Backward,
}

/// 操作类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    /// 开仓
    Open,
    /// 撤单
    Cancel,
    /// 平仓
    Close,
}

/// 交易所对 key: (venue1, venue2)
pub type VenuePair = (TradingVenue, TradingVenue);

/// 交易对 key: (symbol1, symbol2)
pub type SymbolPair = (String, String);

/// 完整的阈值 key: (venue1, symbol1, venue2, symbol2)
pub type ThresholdKey = (TradingVenue, String, TradingVenue, String);
