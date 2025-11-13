//! 信号生成器模块
//!
//! 实现基于资金费率和价差的交易信号生成逻辑

use super::state::{EvaluateDecision, SymbolState};

// ===== 占位结构（用于兼容）=====
pub enum SignalRequest {}
pub struct BackwardSignalSubscriber {}
pub struct FundingRateSignalGenerator {}

// ===== 核心信号评估逻辑 =====

impl SymbolState {
    /// 评估交易信号（基于4个核心条件）
    ///
    /// 条件1: predict_fr > 0.00008 & spread_rate < p05 => 正套开仓(spot多+swap空)
    /// 条件2: (predict_fr + loan_rate) < -0.00008 & spread_rate > p95 => 反套开仓(spot空+swap多)
    /// 条件3: current_fr_ma < -0.0005 & spread_rate > p95 => 平仓
    /// 条件4: current_fr_ma > 0.0005 & spread_rate < p05 => 平仓
    ///
    /// # 参数
    /// - `current_fr_ma`: 当前资金费率移动平均值（从 MktChannel 获取）
    pub fn evaluate_signal(&self, current_fr_ma: f64) -> EvaluateDecision {
        let predict_fr = self.predicted_rate;
        let spread_rate = self.spread_rate.unwrap_or(0.0);
        let loan_rate = self.loan_rate;

        // 使用阈值代替 percentile (从 forward_open_threshold/forward_close_threshold)
        let p05 = self.forward_open_threshold;   // 低阈值 (用于正套开仓)
        let p10 = self.forward_cancel_threshold; // 撤单阈值
        let p90 = self.forward_cancel_close_threshold.unwrap_or(-0.0001); // 反套撤单
        let p95 = self.forward_close_threshold;  // 高阈值 (用于反套开仓)

        // ========== 优先级1: 平仓条件（最高优先级）==========
        // 条件3: 平仓 (current_fr_ma < -0.0005 & spread_rate > p95)
        if current_fr_ma < self.fr_close_lower && spread_rate > p95 {
            return EvaluateDecision {
                symbol_key: self.spot_symbol.clone(),
                final_signal: 2, // 平仓信号
                can_emit: true,
                bidask_sr: self.bidask_sr,
                askbid_sr: self.askbid_sr,
            };
        }

        // 条件4: 平仓 (current_fr_ma > 0.0005 & spread_rate < p05)
        if current_fr_ma > self.fr_close_upper && spread_rate < p05 {
            return EvaluateDecision {
                symbol_key: self.spot_symbol.clone(),
                final_signal: 2, // 平仓信号
                can_emit: true,
                bidask_sr: self.bidask_sr,
                askbid_sr: self.askbid_sr,
            };
        }

        // ========== 优先级2: 撤单条件 ==========
        // 条件1撤单: 如果之前是正套开仓(signal=1)，现在 spread_rate > p10 则撤单
        if self.last_signal == 1 && spread_rate > p10 {
            return EvaluateDecision {
                symbol_key: self.spot_symbol.clone(),
                final_signal: -1, // 撤单
                can_emit: true,
                bidask_sr: self.bidask_sr,
                askbid_sr: self.askbid_sr,
            };
        }

        // 条件2撤单: 如果之前是反套开仓(signal=-2)，现在 spread_rate < p90 则撤单
        if self.last_signal == -2 && spread_rate < p90 {
            return EvaluateDecision {
                symbol_key: self.spot_symbol.clone(),
                final_signal: -1, // 撤单
                can_emit: true,
                bidask_sr: self.bidask_sr,
                askbid_sr: self.askbid_sr,
            };
        }

        // ========== 优先级3: 开仓条件 ==========
        // 条件1: 正套开仓 (predict_fr > 0.00008 & spread_rate < p05)
        if predict_fr > self.fr_open_upper && spread_rate < p05 {
            return EvaluateDecision {
                symbol_key: self.spot_symbol.clone(),
                final_signal: 1, // 正套开仓
                can_emit: true,
                bidask_sr: self.bidask_sr,
                askbid_sr: self.askbid_sr,
            };
        }

        // 条件2: 反套开仓 ((predict_fr + loan_rate) < -0.00008 & spread_rate > p95)
        if (predict_fr + loan_rate) < self.fr_open_lower && spread_rate > p95 {
            return EvaluateDecision {
                symbol_key: self.spot_symbol.clone(),
                final_signal: -2, // 反套开仓 (用负数表示反向)
                can_emit: true,
                bidask_sr: self.bidask_sr,
                askbid_sr: self.askbid_sr,
            };
        }

        // 无信号
        EvaluateDecision {
            symbol_key: self.spot_symbol.clone(),
            final_signal: 0,
            can_emit: false,
            bidask_sr: self.bidask_sr,
            askbid_sr: self.askbid_sr,
        }
    }
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
