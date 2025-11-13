//! 价差因子单例模块
//!
//! 提供价差因子计算、存储和阈值判断功能。
//! 维护 askbid 和 bidask 两种价差因子，支持正套/反套开仓/撤单/平仓判断。

use super::common::{ArbDirection, CompareOp, FactorMode, OperationType, ThresholdKey, VenuePair, SymbolPair};
use crate::signal::common::TradingVenue;
use std::cell::RefCell;
use std::collections::HashMap;

/// 价差类型 (bidask 或 askbid)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpreadType {
    BidAsk,
    AskBid,
}

impl SpreadType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SpreadType::BidAsk => "bidask",
            SpreadType::AskBid => "askbid",
        }
    }
}

/// 价差阈值配置
#[derive(Debug, Clone)]
pub struct SpreadThresholdConfig {
    /// 比较操作 (大于/小于)
    pub compare_op: CompareOp,
    /// 套利方向 (正套/反套)
    pub arb_direction: ArbDirection,
    /// 操作类型 (开仓/撤单/平仓)
    pub operation: OperationType,
    /// 价差类型 ("bidask" 或 "askbid")
    pub spread_type: SpreadType,
    /// MM模式阈值
    pub mm_threshold: f64,
    /// MT模式阈值
    pub mt_threshold: f64,
}

/// 价差因子单例
pub struct SpreadFactor {
    /// askbid 价差因子: (venue1, venue2) -> { (symbol1, symbol2) -> value }
    /// askbid_sr = (spot_ask - fut_bid) / spot_ask
    askbid: RefCell<HashMap<VenuePair, HashMap<SymbolPair, f64>>>,

    /// bidask 价差因子: (venue1, venue2) -> { (symbol1, symbol2) -> value }
    /// bidask_sr = (spot_bid - fut_ask) / spot_bid
    bidask: RefCell<HashMap<VenuePair, HashMap<SymbolPair, f64>>>,

    /// spread_rate 价差因子: (venue1, venue2) -> { (symbol1, symbol2) -> value }
    /// mid_price = (ask0 + bid0) / 2
    /// spread_rate = (mid_price_spot - mid_price_swap) / mid_price_spot
    spread_rate: RefCell<HashMap<VenuePair, HashMap<SymbolPair, f64>>>,

    /// 阈值表: (venue1, symbol1, venue2, symbol2) -> SpreadThresholdConfig
    thresholds: RefCell<HashMap<ThresholdKey, SpreadThresholdConfig>>,

    /// 模式: MM 或 MT
    mode: RefCell<FactorMode>,
}

impl SpreadFactor {
    /// 创建新实例
    fn new() -> Self {
        Self {
            askbid: RefCell::new(HashMap::new()),
            bidask: RefCell::new(HashMap::new()),
            spread_rate: RefCell::new(HashMap::new()),
            thresholds: RefCell::new(HashMap::new()),
            mode: RefCell::new(FactorMode::default()),
        }
    }

    /// 获取全局单例实例
    ///
    /// 使用 thread_local 实现单线程单例
    pub fn instance() -> &'static SpreadFactor {
        thread_local! {
            static INSTANCE: std::cell::OnceCell<SpreadFactor> = std::cell::OnceCell::new();
        }

        INSTANCE.with(|cell| {
            // SAFETY: 我们确保只在单线程中使用,并且实例一旦创建就不会被销毁
            // 通过 thread_local 保证每个线程有自己的实例
            unsafe {
                let ptr = cell as *const std::cell::OnceCell<SpreadFactor> as *mut std::cell::OnceCell<SpreadFactor>;
                (*ptr).get_or_init(|| SpreadFactor::new())
            }
        })
    }

    /// 统一更新所有价差因子
    ///
    /// 接受完整盘口数据，一次性更新三个价差因子：
    /// - askbid_sr = (venue1_ask - venue2_bid) / venue1_ask
    /// - bidask_sr = (venue1_bid - venue2_ask) / venue1_bid
    /// - spread_rate = (mid_price_venue1 - mid_price_venue2) / mid_price_venue1
    ///   其中 mid_price = (ask + bid) / 2
    ///
    /// 返回 (askbid, bidask, spread_rate) 元组，任何一个计算失败则为 None
    pub fn update(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        venue1_bid: f64,
        venue1_ask: f64,
        venue2_bid: f64,
        venue2_ask: f64,
    ) -> (Option<f64>, Option<f64>, Option<f64>) {
        let venue_pair = (venue1, venue2);
        let symbol_pair = (symbol1.to_string(), symbol2.to_string());

        // 计算 askbid_sr = (venue1_ask - venue2_bid) / venue1_ask
        let askbid = if venue1_ask > 0.0 && venue2_bid > 0.0 {
            let value = (venue1_ask - venue2_bid) / venue1_ask;
            self.askbid
                .borrow_mut()
                .entry(venue_pair)
                .or_insert_with(HashMap::new)
                .insert(symbol_pair.clone(), value);
            Some(value)
        } else {
            None
        };

        // 计算 bidask_sr = (venue1_bid - venue2_ask) / venue1_bid
        let bidask = if venue1_bid > 0.0 && venue2_ask > 0.0 {
            let value = (venue1_bid - venue2_ask) / venue1_bid;
            self.bidask
                .borrow_mut()
                .entry(venue_pair)
                .or_insert_with(HashMap::new)
                .insert(symbol_pair.clone(), value);
            Some(value)
        } else {
            None
        };

        // 计算 spread_rate = (mid_price_venue1 - mid_price_venue2) / mid_price_venue1
        let spread_rate = if venue1_bid > 0.0 && venue1_ask > 0.0 && venue2_bid > 0.0 && venue2_ask > 0.0 {
            let mid_price_venue1 = (venue1_ask + venue1_bid) / 2.0;
            let mid_price_venue2 = (venue2_ask + venue2_bid) / 2.0;

            if mid_price_venue1 > 0.0 {
                let value = (mid_price_venue1 - mid_price_venue2) / mid_price_venue1;
                self.spread_rate
                    .borrow_mut()
                    .entry(venue_pair)
                    .or_insert_with(HashMap::new)
                    .insert(symbol_pair, value);
                Some(value)
            } else {
                None
            }
        } else {
            None
        };

        (askbid, bidask, spread_rate)
    }

    /// 获取 askbid 价差因子
    pub fn get_askbid(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> Option<f64> {
        let venue_pair = (venue1, venue2);
        let symbol_pair = (symbol1.to_string(), symbol2.to_string());

        self.askbid
            .borrow()
            .get(&venue_pair)
            .and_then(|inner| inner.get(&symbol_pair))
            .copied()
    }

    /// 获取 bidask 价差因子
    pub fn get_bidask(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> Option<f64> {
        let venue_pair = (venue1, venue2);
        let symbol_pair = (symbol1.to_string(), symbol2.to_string());

        self.bidask
            .borrow()
            .get(&venue_pair)
            .and_then(|inner| inner.get(&symbol_pair))
            .copied()
    }

    /// 获取 spread_rate 价差因子
    pub fn get_spread_rate(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> Option<f64> {
        let venue_pair = (venue1, venue2);
        let symbol_pair = (symbol1.to_string(), symbol2.to_string());

        self.spread_rate
            .borrow()
            .get(&venue_pair)
            .and_then(|inner| inner.get(&symbol_pair))
            .copied()
    }

    // ===== 模式管理 =====

    /// 设置价差因子模式
    pub fn set_mode(&self, mode: FactorMode) {
        *self.mode.borrow_mut() = mode;
    }

    /// 获取当前价差因子模式
    pub fn get_mode(&self) -> FactorMode {
        *self.mode.borrow()
    }

    // ===== 6个 set 函数 =====

    /// 设置正套开仓阈值
    /// forward_arb_open_tr: ("mm", "bidask", 10.0)
    pub fn set_forward_open_threshold(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        mm_threshold: f64,
        mt_threshold: f64,
    ) {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let config = SpreadThresholdConfig {
            compare_op: CompareOp::LessThan,
            arb_direction: ArbDirection::Forward,
            operation: OperationType::Open,
            spread_type: SpreadType::BidAsk,
            mm_threshold,
            mt_threshold,
        };

        self.thresholds.borrow_mut().insert(key, config);
    }

    /// 设置正套撤单阈值
    /// forward_arb_cancel_tr: ("mm", "bidask", 15.0)
    pub fn set_forward_cancel_threshold(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        mm_threshold: f64,
        mt_threshold: f64,
    ) {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let config = SpreadThresholdConfig {
            compare_op: CompareOp::LessThan,
            arb_direction: ArbDirection::Forward,
            operation: OperationType::Cancel,
            spread_type: SpreadType::BidAsk,
            mm_threshold,
            mt_threshold,
        };

        self.thresholds.borrow_mut().insert(key, config);
    }

    /// 设置正套平仓阈值
    /// forward_arb_close_tr: ("mm", "askbid", 90.0)
    pub fn set_forward_close_threshold(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        mm_threshold: f64,
        mt_threshold: f64,
    ) {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let config = SpreadThresholdConfig {
            compare_op: CompareOp::GreaterThan,
            arb_direction: ArbDirection::Forward,
            operation: OperationType::Close,
            spread_type: SpreadType::AskBid,
            mm_threshold,
            mt_threshold,
        };

        self.thresholds.borrow_mut().insert(key, config);
    }

    /// 设置反套开仓阈值
    /// backward_arb_open_tr: ("mm", "askbid", 5.0)
    pub fn set_backward_open_threshold(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        mm_threshold: f64,
        mt_threshold: f64,
    ) {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let config = SpreadThresholdConfig {
            compare_op: CompareOp::LessThan,
            arb_direction: ArbDirection::Backward,
            operation: OperationType::Open,
            spread_type: SpreadType::AskBid,
            mm_threshold,
            mt_threshold,
        };

        self.thresholds.borrow_mut().insert(key, config);
    }

    /// 设置反套撤单阈值
    /// backward_arb_cancel_tr: ("mm", "askbid", 10.0)
    pub fn set_backward_cancel_threshold(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        mm_threshold: f64,
        mt_threshold: f64,
    ) {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let config = SpreadThresholdConfig {
            compare_op: CompareOp::LessThan,
            arb_direction: ArbDirection::Backward,
            operation: OperationType::Cancel,
            spread_type: SpreadType::AskBid,
            mm_threshold,
            mt_threshold,
        };

        self.thresholds.borrow_mut().insert(key, config);
    }

    /// 设置反套平仓阈值
    /// backward_arb_close_tr: ("mm", "bidask", 95.0)
    pub fn set_backward_close_threshold(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        mm_threshold: f64,
        mt_threshold: f64,
    ) {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let config = SpreadThresholdConfig {
            compare_op: CompareOp::GreaterThan,
            arb_direction: ArbDirection::Backward,
            operation: OperationType::Close,
            spread_type: SpreadType::BidAsk,
            mm_threshold,
            mt_threshold,
        };

        self.thresholds.borrow_mut().insert(key, config);
    }

    // ===== 6个 satisfy 函数 =====

    /// 检查是否满足正套开仓条件
    /// 根据当前模式,只需满足对应模式的阈值
    pub fn satisfy_forward_open(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> bool {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let thresholds = self.thresholds.borrow();
        let current_mode = self.get_mode();

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == ArbDirection::Forward
                && config.operation == OperationType::Open
            {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                };

                if let Some(v) = value {
                    // 根据当前模式选择对应的阈值
                    let threshold = match current_mode {
                        FactorMode::MM => config.mm_threshold,
                        FactorMode::MT => config.mt_threshold,
                    };
                    return config.compare_op.check(v, threshold);
                }
            }
        }

        false
    }

    /// 检查是否满足正套撤单条件
    /// 根据当前模式,只需满足对应模式的阈值
    pub fn satisfy_forward_cancel(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> bool {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let thresholds = self.thresholds.borrow();
        let current_mode = self.get_mode();

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == ArbDirection::Forward
                && config.operation == OperationType::Cancel
            {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                };

                if let Some(v) = value {
                    // 根据当前模式选择对应的阈值
                    let threshold = match current_mode {
                        FactorMode::MM => config.mm_threshold,
                        FactorMode::MT => config.mt_threshold,
                    };
                    return config.compare_op.check(v, threshold);
                }
            }
        }

        false
    }

    /// 检查是否满足正套平仓条件
    /// 根据当前模式,只需满足对应模式的阈值
    pub fn satisfy_forward_close(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> bool {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let thresholds = self.thresholds.borrow();
        let current_mode = self.get_mode();

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == ArbDirection::Forward
                && config.operation == OperationType::Close
            {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                };

                if let Some(v) = value {
                    // 根据当前模式选择对应的阈值
                    let threshold = match current_mode {
                        FactorMode::MM => config.mm_threshold,
                        FactorMode::MT => config.mt_threshold,
                    };
                    return config.compare_op.check(v, threshold);
                }
            }
        }

        false
    }

    /// 检查是否满足反套开仓条件
    /// 根据当前模式,只需满足对应模式的阈值
    pub fn satisfy_backward_open(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> bool {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let thresholds = self.thresholds.borrow();
        let current_mode = self.get_mode();

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == ArbDirection::Backward
                && config.operation == OperationType::Open
            {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                };

                if let Some(v) = value {
                    // 根据当前模式选择对应的阈值
                    let threshold = match current_mode {
                        FactorMode::MM => config.mm_threshold,
                        FactorMode::MT => config.mt_threshold,
                    };
                    return config.compare_op.check(v, threshold);
                }
            }
        }

        false
    }

    /// 检查是否满足反套撤单条件
    /// 根据当前模式,只需满足对应模式的阈值
    pub fn satisfy_backward_cancel(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> bool {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let thresholds = self.thresholds.borrow();
        let current_mode = self.get_mode();

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == ArbDirection::Backward
                && config.operation == OperationType::Cancel
            {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                };

                if let Some(v) = value {
                    // 根据当前模式选择对应的阈值
                    let threshold = match current_mode {
                        FactorMode::MM => config.mm_threshold,
                        FactorMode::MT => config.mt_threshold,
                    };
                    return config.compare_op.check(v, threshold);
                }
            }
        }

        false
    }

    /// 检查是否满足反套平仓条件
    /// 根据当前模式,只需满足对应模式的阈值
    pub fn satisfy_backward_close(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> bool {
        let key = (venue1, symbol1.to_string(), venue2, symbol2.to_string());
        let thresholds = self.thresholds.borrow();
        let current_mode = self.get_mode();

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == ArbDirection::Backward
                && config.operation == OperationType::Close
            {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                };

                if let Some(v) = value {
                    // 根据当前模式选择对应的阈值
                    let threshold = match current_mode {
                        FactorMode::MM => config.mm_threshold,
                        FactorMode::MT => config.mt_threshold,
                    };
                    return config.compare_op.check(v, threshold);
                }
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spread_factor_singleton() {
        let sf1 = SpreadFactor::instance();
        let sf2 = SpreadFactor::instance();

        // 验证是同一个实例
        assert!(std::ptr::eq(sf1, sf2));
    }

    #[test]
    fn test_update_and_get_all_factors() {
        let sf = SpreadFactor::instance();

        let venue1 = TradingVenue::BinanceSpot;
        let symbol1 = "BTCUSDT";
        let venue2 = TradingVenue::BinanceSwap;
        let symbol2 = "BTCUSDT";

        // venue1(spot): bid=50000, ask=50100
        // venue2(swap): bid=49900, ask=50000
        // bidask_sr = (50000 - 50000) / 50000 = 0.0
        // askbid_sr = (50100 - 49900) / 50100 = 0.00399...
        // mid_price_spot = (50000 + 50100) / 2 = 50050
        // mid_price_swap = (49900 + 50000) / 2 = 49950
        // spread_rate = (50050 - 49950) / 50050 = 0.001998...
        let (askbid, bidask, spread_rate) = sf.update(
            venue1, symbol1, venue2, symbol2,
            50000.0, 50100.0,  // venue1 bid, ask
            49900.0, 50000.0,  // venue2 bid, ask
        );

        assert!(bidask.is_some());
        assert!((bidask.unwrap() - 0.0).abs() < 0.0001);

        assert!(askbid.is_some());
        assert!((askbid.unwrap() - 0.00399).abs() < 0.0001);

        assert!(spread_rate.is_some());
        assert!((spread_rate.unwrap() - 0.001998).abs() < 0.0001);

        // 验证 getter 方法
        let retrieved_bidask = sf.get_bidask(venue1, symbol1, venue2, symbol2);
        assert_eq!(retrieved_bidask, bidask);

        let retrieved_askbid = sf.get_askbid(venue1, symbol1, venue2, symbol2);
        assert_eq!(retrieved_askbid, askbid);

        let retrieved_spread_rate = sf.get_spread_rate(venue1, symbol1, venue2, symbol2);
        assert_eq!(retrieved_spread_rate, spread_rate);
    }

    #[test]
    fn test_forward_open_threshold() {
        let sf = SpreadFactor::instance();

        let venue1 = TradingVenue::BinanceSpot;
        let symbol1 = "SOLUSDT";
        let venue2 = TradingVenue::BinanceSwap;
        let symbol2 = "SOLUSDT";

        // 设置正套开仓阈值: MM模式 <= 0.005, MT模式 <= 0.006
        sf.set_forward_open_threshold(venue1, symbol1, venue2, symbol2, 0.005, 0.006);

        // 测试 MM 模式
        sf.set_mode(FactorMode::MM);

        // 更新 bidask = 0.003 (满足MM条件: < 0.005)
        // venue1_bid=100.0, venue2_ask=99.7 => bidask = (100 - 99.7) / 100 = 0.003
        sf.update(venue1, symbol1, venue2, symbol2, 100.0, 100.5, 99.5, 99.7);
        assert!(sf.satisfy_forward_open(venue1, symbol1, venue2, symbol2));

        // 更新 bidask = 0.0055 (不满足MM条件: > 0.005)
        // venue1_bid=100.0, venue2_ask=99.45 => bidask = (100 - 99.45) / 100 = 0.0055
        sf.update(venue1, symbol1, venue2, symbol2, 100.0, 100.5, 99.3, 99.45);
        assert!(!sf.satisfy_forward_open(venue1, symbol1, venue2, symbol2));

        // 测试 MT 模式
        sf.set_mode(FactorMode::MT);

        // bidask = 0.0055 (满足MT条件: < 0.006)
        assert!(sf.satisfy_forward_open(venue1, symbol1, venue2, symbol2));

        // 更新 bidask = 0.007 (不满足MT条件: > 0.006)
        // venue1_bid=100.0, venue2_ask=99.3 => bidask = (100 - 99.3) / 100 = 0.007
        sf.update(venue1, symbol1, venue2, symbol2, 100.0, 100.5, 99.1, 99.3);
        assert!(!sf.satisfy_forward_open(venue1, symbol1, venue2, symbol2));
    }

    #[test]
    fn test_forward_close_threshold() {
        let sf = SpreadFactor::instance();

        let venue1 = TradingVenue::BinanceSpot;
        let symbol1 = "ADAUSDT";
        let venue2 = TradingVenue::BinanceSwap;
        let symbol2 = "ADAUSDT";

        // 设置正套平仓阈值: MM模式 >= 0.01, MT模式 >= 0.009
        sf.set_forward_close_threshold(venue1, symbol1, venue2, symbol2, 0.01, 0.009);

        // 测试 MM 模式
        sf.set_mode(FactorMode::MM);

        // 更新 askbid = 0.015 (满足MM条件: > 0.01)
        // venue1_ask=1.0, venue2_bid=0.985 => askbid = (1.0 - 0.985) / 1.0 = 0.015
        sf.update(venue1, symbol1, venue2, symbol2, 0.99, 1.0, 0.985, 0.99);
        assert!(sf.satisfy_forward_close(venue1, symbol1, venue2, symbol2));

        // 更新 askbid = 0.0095 (不满足MM条件: < 0.01)
        // venue1_ask=1.0, venue2_bid=0.9905 => askbid = (1.0 - 0.9905) / 1.0 = 0.0095
        sf.update(venue1, symbol1, venue2, symbol2, 0.99, 1.0, 0.9905, 0.995);
        assert!(!sf.satisfy_forward_close(venue1, symbol1, venue2, symbol2));

        // 测试 MT 模式
        sf.set_mode(FactorMode::MT);

        // askbid = 0.0095 (满足MT条件: > 0.009)
        assert!(sf.satisfy_forward_close(venue1, symbol1, venue2, symbol2));

        // 更新 askbid = 0.008 (不满足MT条件: < 0.009)
        // venue1_ask=1.0, venue2_bid=0.992 => askbid = (1.0 - 0.992) / 1.0 = 0.008
        sf.update(venue1, symbol1, venue2, symbol2, 0.99, 1.0, 0.992, 0.995);
        assert!(!sf.satisfy_forward_close(venue1, symbol1, venue2, symbol2));
    }

    #[test]
    fn test_backward_open_threshold() {
        let sf = SpreadFactor::instance();

        let venue1 = TradingVenue::BinanceSpot;
        let symbol1 = "DOGEUSDT";
        let venue2 = TradingVenue::BinanceSwap;
        let symbol2 = "DOGEUSDT";

        // 设置反套开仓阈值: MM模式 <= 0.003, MT模式 <= 0.004
        sf.set_backward_open_threshold(venue1, symbol1, venue2, symbol2, 0.003, 0.004);

        // 测试 MM 模式
        sf.set_mode(FactorMode::MM);

        // 更新 askbid = 0.002 (满足MM条件: < 0.003)
        // venue1_ask=0.1, venue2_bid=0.0998 => askbid = (0.1 - 0.0998) / 0.1 = 0.002
        sf.update(venue1, symbol1, venue2, symbol2, 0.099, 0.1, 0.0998, 0.0999);
        assert!(sf.satisfy_backward_open(venue1, symbol1, venue2, symbol2));

        // 更新 askbid = 0.0035 (不满足MM条件: > 0.003)
        // venue1_ask=0.1, venue2_bid=0.09965 => askbid = (0.1 - 0.09965) / 0.1 = 0.0035
        sf.update(venue1, symbol1, venue2, symbol2, 0.099, 0.1, 0.09965, 0.0998);
        assert!(!sf.satisfy_backward_open(venue1, symbol1, venue2, symbol2));

        // 测试 MT 模式
        sf.set_mode(FactorMode::MT);

        // askbid = 0.0035 (满足MT条件: < 0.004)
        assert!(sf.satisfy_backward_open(venue1, symbol1, venue2, symbol2));

        // 更新 askbid = 0.005 (不满足MT条件: > 0.004)
        // venue1_ask=0.1, venue2_bid=0.0995 => askbid = (0.1 - 0.0995) / 0.1 = 0.005
        sf.update(venue1, symbol1, venue2, symbol2, 0.099, 0.1, 0.0995, 0.0997);
        assert!(!sf.satisfy_backward_open(venue1, symbol1, venue2, symbol2));
    }

    #[test]
    fn test_mode_setting() {
        let sf = SpreadFactor::instance();

        // 验证默认模式为 MM
        assert_eq!(sf.get_mode(), FactorMode::MM);

        // 设置为 MT 模式
        sf.set_mode(FactorMode::MT);
        assert_eq!(sf.get_mode(), FactorMode::MT);

        // 切换回 MM 模式
        sf.set_mode(FactorMode::MM);
        assert_eq!(sf.get_mode(), FactorMode::MM);
    }
}
