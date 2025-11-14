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

/// 统计信息
#[derive(Debug, Default)]
pub struct EngineStats {
    pub open_signals: u64,
    pub ladder_cancel_signals: u64,
}

/// 信号评估决策
#[derive(Debug, Clone)]
pub struct EvaluateDecision {
    pub symbol_key: String,
    pub final_signal: i32,
    pub can_emit: bool,
    pub bidask_sr: Option<f64>,
    pub askbid_sr: Option<f64>,
}

/// 数量步长信息
#[derive(Debug, Clone)]
pub struct QtyStepInfo {
    pub spot_min: f64,
    pub futures_min: f64,
    pub step: f64,
}

/// 交易对状态
#[derive(Debug, Clone)]
pub struct SymbolState {
    // 基础信息
    pub spot_symbol: String,
    pub futures_symbol: String,

    // 盘口数据
    pub spot_quote: Quote,
    pub futures_quote: Quote,

    // 资金费率数据
    pub funding_rate: f64,
    pub funding_ts: i64,
    pub next_funding_time: i64,
    pub funding_rate_data: FundingRateData,
    pub predicted_rate: f64, // 预测资金费率

    // 阈值配置（从Redis加载）
    pub forward_open_threshold: f64,
    pub forward_cancel_threshold: f64,
    pub forward_close_threshold: f64,
    pub forward_cancel_close_threshold: Option<f64>,

    // 资金费率阈值
    pub fr_open_upper: f64,  // 0.00008 for 8h
    pub fr_open_lower: f64,  // -0.00008 for 8h
    pub fr_close_upper: f64, // 0.0005
    pub fr_close_lower: f64, // -0.0005

    // 借贷利率
    pub loan_rate: f64,

    // 信号状态
    pub last_signal: i32, // -1撤单/0无/1开仓
    pub last_signal_ts: i64,

    // 价差因子缓存
    pub bidask_sr: Option<f64>,   // (spot_bid - fut_ask) / spot_bid
    pub askbid_sr: Option<f64>,   // (spot_ask - fut_bid) / spot_ask
    pub spread_rate: Option<f64>, // mid_price spread
}

impl SymbolState {
    pub fn new(th: super::param_loader::SymbolThreshold) -> Self {
        Self {
            spot_symbol: th.spot_symbol,
            futures_symbol: th.futures_symbol,
            spot_quote: Quote::default(),
            futures_quote: Quote::default(),
            funding_rate: 0.0,
            funding_ts: 0,
            next_funding_time: 0,
            funding_rate_data: FundingRateData::new(),
            predicted_rate: 0.0,
            forward_open_threshold: th.forward_open_threshold,
            forward_cancel_threshold: th.forward_cancel_threshold,
            forward_close_threshold: th.forward_close_threshold,
            forward_cancel_close_threshold: th.forward_cancel_close_threshold,
            fr_open_upper: 0.00008,
            fr_open_lower: -0.00008,
            fr_close_upper: 0.0005,
            fr_close_lower: -0.0005,
            loan_rate: 0.0,
            last_signal: 0,
            last_signal_ts: 0,
            bidask_sr: None,
            askbid_sr: None,
            spread_rate: None,
        }
    }

    pub fn update_threshold(&mut self, th: super::param_loader::SymbolThreshold) {
        self.forward_open_threshold = th.forward_open_threshold;
        self.forward_cancel_threshold = th.forward_cancel_threshold;
        self.forward_close_threshold = th.forward_close_threshold;
        self.forward_cancel_close_threshold = th.forward_cancel_close_threshold;
    }

    /// 刷新价差因子
    pub fn refresh_factors(&mut self) {
        let spot_bid = self.spot_quote.bid;
        let spot_ask = self.spot_quote.ask;
        let fut_bid = self.futures_quote.bid;
        let fut_ask = self.futures_quote.ask;

        // bidask_sr = (spot_bid - fut_ask) / spot_bid (正套开仓指标)
        if spot_bid > 0.0 && fut_ask > 0.0 {
            self.bidask_sr = Some((spot_bid - fut_ask) / spot_bid);
        } else {
            self.bidask_sr = None;
        }

        // askbid_sr = (spot_ask - fut_bid) / spot_ask (正套平仓指标)
        if spot_ask > 0.0 && fut_bid > 0.0 {
            self.askbid_sr = Some((spot_ask - fut_bid) / spot_ask);
        } else {
            self.askbid_sr = None;
        }

        // spread_rate = (mid_spot - mid_fut) / mid_spot
        if spot_bid > 0.0 && spot_ask > 0.0 && fut_bid > 0.0 && fut_ask > 0.0 {
            let mid_spot = (spot_bid + spot_ask) / 2.0;
            let mid_fut = (fut_bid + fut_ask) / 2.0;
            if mid_spot > 0.0 {
                self.spread_rate = Some((mid_spot - mid_fut) / mid_spot);
            }
        } else {
            self.spread_rate = None;
        }
    }
}
