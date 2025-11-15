//! 状态维护模块
//!
//! 只负责维护行情数据，不处理计算逻辑

use std::collections::VecDeque;

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
