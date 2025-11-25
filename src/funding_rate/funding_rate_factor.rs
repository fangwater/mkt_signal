//! 资金费率因子单例模块
//!
//! 提供基于资金费率的套利判断功能。
//! 支持 4h 和 8h 两种周期，MM 和 MT 两种模式。
//! 阈值为公共阈值，所有 symbol 共享同一套阈值配置。

use super::common::{ArbDirection, CompareOp, FactorMode, OperationType};
use super::mkt_channel::MktChannel;
use super::rate_fetcher::{FundingRatePeriod, RateFetcher};
use crate::signal::common::TradingVenue;
use std::cell::RefCell;
use std::collections::HashMap;

/// 阈值配置键：(周期, 模式, 操作, 方向)
type ThresholdKey = (FundingRatePeriod, FactorMode, OperationType, ArbDirection);

/// 资金费率阈值配置
#[derive(Debug, Clone)]
pub struct FrThresholdConfig {
    /// 比较操作 (大于/小于)
    pub compare_op: CompareOp,
    /// 套利方向 (正套/反套)
    pub arb_direction: ArbDirection,
    /// 操作类型 (开仓/平仓)
    pub operation: OperationType,
    /// 资金费率周期 (4h/8h)
    pub period: FundingRatePeriod,
    /// 模式 (MM/MT)
    pub mode: FactorMode,
    /// 阈值
    pub threshold: f64,
}

/// 资金费率因子单例
pub struct FundingRateFactor {
    /// 阈值表：(周期, 模式, 操作, 方向) -> FrThresholdConfig
    /// 公共阈值，每个周期和模式有独立的阈值配置
    thresholds: RefCell<HashMap<ThresholdKey, FrThresholdConfig>>,

    /// 当前模式 (MM 或 MT)
    current_mode: RefCell<FactorMode>,
}

impl FundingRateFactor {
    /// 创建新实例
    fn new() -> Self {
        Self {
            thresholds: RefCell::new(HashMap::new()),
            current_mode: RefCell::new(FactorMode::default()),
        }
    }

    /// 获取全局单例实例
    ///
    /// 使用 thread_local 实现单线程单例
    pub fn instance() -> &'static FundingRateFactor {
        thread_local! {
            static INSTANCE: std::cell::OnceCell<FundingRateFactor> = std::cell::OnceCell::new();
        }

        INSTANCE.with(|cell| {
            // SAFETY: 我们确保只在单线程中使用,并且实例一旦创建就不会被销毁
            // 通过 thread_local 保证每个线程有自己的实例
            unsafe {
                let ptr = cell as *const std::cell::OnceCell<FundingRateFactor>
                    as *mut std::cell::OnceCell<FundingRateFactor>;
                (*ptr).get_or_init(|| FundingRateFactor::new())
            }
        })
    }

    // ===== 模式管理 =====

    /// 设置当前模式
    pub fn set_mode(&self, mode: FactorMode) {
        *self.current_mode.borrow_mut() = mode;
    }

    /// 获取当前模式
    pub fn get_mode(&self) -> FactorMode {
        *self.current_mode.borrow()
    }

    /// 获取指定方向和操作的阈值配置
    pub fn get_threshold_config(
        &self,
        period: FundingRatePeriod,
        direction: ArbDirection,
        operation: OperationType,
    ) -> Option<FrThresholdConfig> {
        let mode = self.get_mode();
        let key = (period, mode, operation, direction);
        self.thresholds.borrow().get(&key).cloned()
    }

    // ===== 4个 update 函数 =====

    /// 更新正套开仓阈值
    ///
    /// 判断条件：predict_fr > threshold
    pub fn update_forward_open_threshold(
        &self,
        period: FundingRatePeriod,
        mode: FactorMode,
        threshold: f64,
    ) {
        let key = (period, mode, OperationType::Open, ArbDirection::Forward);
        let config = FrThresholdConfig {
            compare_op: CompareOp::GreaterThan,
            arb_direction: ArbDirection::Forward,
            operation: OperationType::Open,
            period,
            mode,
            threshold,
        };

        self.thresholds.borrow_mut().insert(key, config);
    }

    /// 更新反套开仓阈值
    ///
    /// 判断条件：(predict_fr + predict_loan_rate) < threshold
    pub fn update_backward_open_threshold(
        &self,
        period: FundingRatePeriod,
        mode: FactorMode,
        threshold: f64,
    ) {
        let key = (period, mode, OperationType::Open, ArbDirection::Backward);
        let config = FrThresholdConfig {
            compare_op: CompareOp::LessThan,
            arb_direction: ArbDirection::Backward,
            operation: OperationType::Open,
            period,
            mode,
            threshold,
        };

        self.thresholds.borrow_mut().insert(key, config);
    }

    /// 更新正套平仓阈值
    ///
    /// 判断条件：current_fr_ma < threshold
    pub fn update_forward_close_threshold(
        &self,
        period: FundingRatePeriod,
        mode: FactorMode,
        threshold: f64,
    ) {
        let key = (period, mode, OperationType::Close, ArbDirection::Forward);
        let config = FrThresholdConfig {
            compare_op: CompareOp::LessThan,
            arb_direction: ArbDirection::Forward,
            operation: OperationType::Close,
            period,
            mode,
            threshold,
        };

        self.thresholds.borrow_mut().insert(key, config);
    }

    /// 更新反套平仓阈值
    ///
    /// 判断条件：(current_fr_ma + current_loan_rate) > threshold
    pub fn update_backward_close_threshold(
        &self,
        period: FundingRatePeriod,
        mode: FactorMode,
        threshold: f64,
    ) {
        let key = (period, mode, OperationType::Close, ArbDirection::Backward);
        let config = FrThresholdConfig {
            compare_op: CompareOp::GreaterThan,
            arb_direction: ArbDirection::Backward,
            operation: OperationType::Close,
            period,
            mode,
            threshold,
        };

        self.thresholds.borrow_mut().insert(key, config);
    }

    // ===== 辅助方法：获取因子数据 =====

    /// 获取预测资金费率 (predict_fr)
    ///
    /// RateFetcher 会返回 (symbol 周期, 预测值)，周期不匹配时忽略
    fn get_predict_fr(&self, symbol: &str, period: FundingRatePeriod) -> Option<f64> {
        RateFetcher::instance()
            .get_predicted_funding_rate(symbol, TradingVenue::BinanceUm)
            .and_then(|(sym_period, value)| {
                if sym_period == period {
                    Some(value)
                } else {
                    None
                }
            })
    }

    /// 获取预测借贷利率 (predict_loan_rate)
    fn get_predict_loan_rate(&self, symbol: &str, period: FundingRatePeriod) -> Option<f64> {
        RateFetcher::instance()
            .get_predict_loan_rate(symbol, TradingVenue::BinanceUm)
            .and_then(|(sym_period, value)| {
                if sym_period == period {
                    Some(value)
                } else {
                    None
                }
            })
    }

    /// 获取当前资金费率移动平均 (current_fr_ma)
    ///
    /// 从 MktChannel 获取
    fn get_current_fr_ma(&self, symbol: &str, venue: TradingVenue) -> Option<f64> {
        MktChannel::instance().get_funding_rate_mean(symbol, venue)
    }

    /// 获取当前借贷利率 (current_loan_rate)
    ///
    /// 使用最近 3 条历史数据的均值
    fn get_current_loan_rate(&self, symbol: &str, period: FundingRatePeriod) -> Option<f64> {
        RateFetcher::instance()
            .get_current_loan_rate(symbol, TradingVenue::BinanceUm)
            .and_then(|(sym_period, value)| {
                if sym_period == period {
                    Some(value)
                } else {
                    None
                }
            })
    }

    // ===== 4个 satisfy 函数 =====

    /// 检查是否满足正套开仓条件
    ///
    /// 判断：predict_fr > threshold（根据 symbol 的周期和当前模式）
    pub fn satisfy_forward_open(&self, symbol: &str, period: FundingRatePeriod) -> bool {
        let current_mode = self.get_mode();
        let key = (
            period,
            current_mode,
            OperationType::Open,
            ArbDirection::Forward,
        );

        let thresholds = self.thresholds.borrow();
        if let Some(config) = thresholds.get(&key) {
            if let Some(predict_fr) = self.get_predict_fr(symbol, period) {
                return config.compare_op.check(predict_fr, config.threshold);
            }
        }

        false
    }

    /// 检查是否满足反套开仓条件
    ///
    /// 判断：(predict_fr + predict_loan_rate) < threshold（根据 symbol 的周期和当前模式）
    pub fn satisfy_backward_open(&self, symbol: &str, period: FundingRatePeriod) -> bool {
        let current_mode = self.get_mode();
        let key = (
            period,
            current_mode,
            OperationType::Open,
            ArbDirection::Backward,
        );

        let thresholds = self.thresholds.borrow();
        if let Some(config) = thresholds.get(&key) {
            if let (Some(predict_fr), Some(loan_rate)) = (
                self.get_predict_fr(symbol, period),
                self.get_predict_loan_rate(symbol, period),
            ) {
                let factor = predict_fr + loan_rate;
                return config.compare_op.check(factor, config.threshold);
            }
        }

        false
    }

    /// 检查是否满足正套平仓条件
    ///
    /// 判断：current_fr_ma < threshold（根据 symbol 的周期和当前模式）
    pub fn satisfy_forward_close(
        &self,
        symbol: &str,
        period: FundingRatePeriod,
        venue: TradingVenue,
    ) -> bool {
        let current_mode = self.get_mode();
        let key = (
            period,
            current_mode,
            OperationType::Close,
            ArbDirection::Forward,
        );

        let thresholds = self.thresholds.borrow();
        if let Some(config) = thresholds.get(&key) {
            if let Some(current_fr_ma) = self.get_current_fr_ma(symbol, venue) {
                return config.compare_op.check(current_fr_ma, config.threshold);
            }
        }

        false
    }

    /// 检查是否满足反套平仓条件
    ///
    /// 判断：(current_fr_ma + current_loan_rate) > threshold（根据 symbol 的周期和当前模式）
    pub fn satisfy_backward_close(
        &self,
        symbol: &str,
        period: FundingRatePeriod,
        venue: TradingVenue,
    ) -> bool {
        let current_mode = self.get_mode();
        let key = (
            period,
            current_mode,
            OperationType::Close,
            ArbDirection::Backward,
        );

        let thresholds = self.thresholds.borrow();
        if let Some(config) = thresholds.get(&key) {
            if let (Some(current_fr_ma), Some(current_loan)) = (
                self.get_current_fr_ma(symbol, venue),
                self.get_current_loan_rate(symbol, period),
            ) {
                let factor = current_fr_ma + current_loan;
                return config.compare_op.check(factor, config.threshold);
            }
        }

        false
    }
}
