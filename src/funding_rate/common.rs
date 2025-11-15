//! 资金费率模块通用定义
//!
//! 包含：
//! - 枚举类型：套利方向、操作类型、因子模式等
//! - 数据结构：行情报价、资金费率数据
//! - 辅助函数：浮点数比较、数字列表解析

use std::collections::VecDeque;

use crate::signal::common::TradingVenue;

// ========== 枚举类型定义 ==========

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

/// 因子模式（MM/MT 模式通用定义）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FactorMode {
    /// Maker-Maker 模式
    MM,
    /// Maker-Taker 模式（对冲是 taker 方）
    MT,
}

impl Default for FactorMode {
    fn default() -> Self {
        FactorMode::MM
    }
}

// ========== 数据结构定义 ==========

const MAX_SERIES_LEN: usize = 60;

/// 行情报价
#[derive(Debug, Clone, Copy, Default)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub ts: i64,
}

impl Quote {
    pub fn update(&mut self, bid: f64, ask: f64, ts: i64) {
        self.bid = bid;
        self.ask = ask;
        self.ts = ts;
    }

    pub fn is_valid(&self) -> bool {
        self.bid > 0.0 && self.ask > 0.0
    }
}

/// Funding Rate 数据（维护60条 + rolling sum/mean）
#[derive(Debug, Clone)]
pub struct FundingRateData {
    series: VecDeque<f64>,
    sum: f64,
    mean: Option<f64>,
}

impl FundingRateData {
    pub fn new() -> Self {
        Self {
            series: VecDeque::with_capacity(MAX_SERIES_LEN),
            sum: 0.0,
            mean: None,
        }
    }

    /// 更新 Funding Rate（立刻重算均值）
    pub fn push(&mut self, funding_rate: f64) {
        // 如果队列满了，移除最旧的值
        if self.series.len() >= MAX_SERIES_LEN {
            if let Some(oldest) = self.series.pop_front() {
                self.sum -= oldest;
            }
        }

        // 添加新值
        self.series.push_back(funding_rate);
        self.sum += funding_rate;

        // 立刻重算均值
        let count = self.series.len() as f64;
        self.mean = Some(self.sum / count);
    }

    /// 获取均值（O(1) 查询）
    pub fn get_mean(&self) -> Option<f64> {
        self.mean
    }

    /// 获取最新值
    pub fn get_latest(&self) -> Option<f64> {
        self.series.back().copied()
    }
}

// ========== 辅助函数 ==========

/// 浮点数近似相等比较
pub fn approx_equal(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-12
}

/// 浮点数数组近似相等比较
pub fn approx_equal_slice(a: &[f64], b: &[f64]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| approx_equal(*x, *y))
}

/// 解析数字列表（支持 JSON 数组、逗号分隔、单个数字）
///
/// # Examples
/// ```
/// use mkt_signal::funding_rate::common::parse_numeric_list;
///
/// // JSON 数组
/// assert_eq!(parse_numeric_list("[1.0, 2.0, 3.0]").unwrap(), vec![1.0, 2.0, 3.0]);
///
/// // 逗号分隔
/// assert_eq!(parse_numeric_list("1.0, 2.0, 3.0").unwrap(), vec![1.0, 2.0, 3.0]);
///
/// // 单个数字
/// assert_eq!(parse_numeric_list("1.0").unwrap(), vec![1.0]);
/// ```
pub fn parse_numeric_list(raw: &str) -> Result<Vec<f64>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.starts_with('[') {
        serde_json::from_str::<Vec<f64>>(trimmed)
            .map_err(|err| format!("JSON array parse error: {err}"))
    } else if trimmed.contains(',') {
        let mut out = Vec::new();
        for part in trimmed.split(',') {
            let piece = part.trim();
            if piece.is_empty() {
                continue;
            }
            match piece.parse::<f64>() {
                Ok(v) => out.push(v),
                Err(err) => {
                    return Err(format!("invalid float '{}': {}", piece, err));
                }
            }
        }
        Ok(out)
    } else {
        trimmed
            .parse::<f64>()
            .map(|v| vec![v])
            .map_err(|err| format!("invalid float: {}", err))
    }
}

// ========== 测试 ==========

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_approx_equal() {
        assert!(approx_equal(1.0, 1.0));
        assert!(approx_equal(1.0, 1.0 + 1e-13));
        assert!(!approx_equal(1.0, 1.0 + 1e-10));
    }

    #[test]
    fn test_approx_equal_slice() {
        assert!(approx_equal_slice(&[1.0, 2.0], &[1.0, 2.0]));
        assert!(approx_equal_slice(&[1.0, 2.0], &[1.0 + 1e-13, 2.0 - 1e-13]));
        assert!(!approx_equal_slice(&[1.0, 2.0], &[1.0, 2.1]));
        assert!(!approx_equal_slice(&[1.0], &[1.0, 2.0]));
    }

    #[test]
    fn test_parse_numeric_list_json() {
        assert_eq!(
            parse_numeric_list("[1.0, 2.0, 3.0]").unwrap(),
            vec![1.0, 2.0, 3.0]
        );
    }

    #[test]
    fn test_parse_numeric_list_comma() {
        assert_eq!(
            parse_numeric_list("1.0, 2.0, 3.0").unwrap(),
            vec![1.0, 2.0, 3.0]
        );
    }

    #[test]
    fn test_parse_numeric_list_single() {
        assert_eq!(parse_numeric_list("1.0").unwrap(), vec![1.0]);
    }

    #[test]
    fn test_parse_numeric_list_empty() {
        assert_eq!(parse_numeric_list("").unwrap(), Vec::<f64>::new());
    }

    #[test]
    fn test_parse_numeric_list_invalid() {
        assert!(parse_numeric_list("invalid").is_err());
        assert!(parse_numeric_list("[1.0, invalid]").is_err());
    }
}
