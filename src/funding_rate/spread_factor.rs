//! 价差因子单例模块
//!
//! 提供价差因子计算、存储和阈值判断功能。
//! 维护 askbid 和 bidask 两种价差因子，支持正套/反套开仓/撤单/平仓判断。

use super::common::{
    ArbDirection, CompareOp, FactorMode, OperationType, SymbolPair, ThresholdKey, VenuePair,
};
use crate::signal::common::TradingVenue;
use std::cell::RefCell;
use std::collections::HashMap;

/// 价差类型 (bidask、askbid或者基于mid price 计算的spread rate)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpreadType {
    BidAsk,
    AskBid,
    SpreadRate,
}

impl SpreadType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SpreadType::BidAsk => "bidask",
            SpreadType::AskBid => "askbid",
            SpreadType::SpreadRate => "spread_rate",
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
    /// 价差类型 ("bidask" 或 "askbid" 或者 “spread_rate”, 之后可以是更多的因子)
    pub spread_type: SpreadType,
    /// 阈值
    pub threshold: f64,
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
    mm_thresholds: RefCell<HashMap<ThresholdKey, SpreadThresholdConfig>>,

    mt_thresholds: RefCell<HashMap<ThresholdKey, SpreadThresholdConfig>>,
    /// 模式: MM 或 MT， mm采用mm的阈值，mt模式就用mt的阈值配置
    mode: RefCell<FactorMode>,
}

impl SpreadFactor {
    /// 创建新实例
    fn new() -> Self {
        Self {
            askbid: RefCell::new(HashMap::new()),
            bidask: RefCell::new(HashMap::new()),
            spread_rate: RefCell::new(HashMap::new()),
            mm_thresholds: RefCell::new(HashMap::new()),
            mt_thresholds: RefCell::new(HashMap::new()),
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
                let ptr = cell as *const std::cell::OnceCell<SpreadFactor>
                    as *mut std::cell::OnceCell<SpreadFactor>;
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
        let spread_rate =
            if venue1_bid > 0.0 && venue1_ask > 0.0 && venue2_bid > 0.0 && venue2_ask > 0.0 {
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
        // 映射：BinanceMargin 使用 BinanceSpot 的价差数据（现货杠杆和现货共享盘口）
        let query_venue1 = match venue1 {
            TradingVenue::BinanceMargin => TradingVenue::BinanceSpot,
            _ => venue1,
        };

        let venue_pair = (query_venue1, venue2);
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
        // 映射：BinanceMargin 使用 BinanceSpot 的价差数据（现货杠杆和现货共享盘口）
        let query_venue1 = match venue1 {
            TradingVenue::BinanceMargin => TradingVenue::BinanceSpot,
            _ => venue1,
        };

        let venue_pair = (query_venue1, venue2);
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
        // 映射：BinanceMargin 使用 BinanceSpot 的价差数据（现货杠杆和现货共享盘口）
        let query_venue1 = match venue1 {
            TradingVenue::BinanceMargin => TradingVenue::BinanceSpot,
            _ => venue1,
        };

        let venue_pair = (query_venue1, venue2);
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

    // ===== 内部辅助方法 =====

    /// Venue 映射：BinanceMargin -> BinanceSpot（现货杠杆和现货共享盘口）
    #[inline]
    fn map_venue(venue: TradingVenue) -> TradingVenue {
        match venue {
            TradingVenue::BinanceMargin => TradingVenue::BinanceSpot,
            _ => venue,
        }
    }

    // ===== 4 个 set 函数，简化，因为对价差而言只有正开、反开 =====
    // ===== 因此只需要正反开的开仓阈值和撤单阈值

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
        // 映射：BinanceMargin 使用 BinanceSpot 的阈值（现货杠杆和现货共享盘口）
        let store_venue1 = match venue1 {
            TradingVenue::BinanceMargin => TradingVenue::BinanceSpot,
            _ => venue1,
        };

        let key = (
            store_venue1,
            symbol1.to_string(),
            venue2,
            symbol2.to_string(),
        );
        let mt_config = SpreadThresholdConfig {
            compare_op: CompareOp::LessThan,
            arb_direction: ArbDirection::Forward,
            operation: OperationType::Open,
            spread_type: SpreadType::BidAsk,
            threshold: mt_threshold,
        };

        let mm_config = SpreadThresholdConfig {
            compare_op: CompareOp::LessThan,
            arb_direction: ArbDirection::Forward,
            operation: OperationType::Open,
            spread_type: SpreadType::SpreadRate,
            threshold: mm_threshold,
        };

        self.mt_thresholds
            .borrow_mut()
            .insert(key.clone(), mt_config);
        self.mm_thresholds.borrow_mut().insert(key, mm_config);
    }

    /// 设置正套open撤单阈值
    pub fn set_forward_open_cancel_threshold(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        mm_threshold: f64,
        mt_threshold: f64,
    ) {
        // 映射：BinanceMargin 使用 BinanceSpot 的阈值（现货杠杆和现货共享盘口）
        let store_venue1 = match venue1 {
            TradingVenue::BinanceMargin => TradingVenue::BinanceSpot,
            _ => venue1,
        };

        let key = (
            store_venue1,
            symbol1.to_string(),
            venue2,
            symbol2.to_string(),
        );
        let mt_config = SpreadThresholdConfig {
            compare_op: CompareOp::GreaterThan,
            arb_direction: ArbDirection::Forward,
            operation: OperationType::Cancel,
            spread_type: SpreadType::BidAsk,
            threshold: mt_threshold,
        };

        let mm_config = SpreadThresholdConfig {
            compare_op: CompareOp::GreaterThan,
            arb_direction: ArbDirection::Forward,
            operation: OperationType::Cancel,
            spread_type: SpreadType::SpreadRate,
            threshold: mm_threshold,
        };

        self.mt_thresholds
            .borrow_mut()
            .insert(key.clone(), mt_config);
        self.mm_thresholds.borrow_mut().insert(key, mm_config);
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
        // 映射：BinanceMargin 使用 BinanceSpot 的阈值（现货杠杆和现货共享盘口）
        let store_venue1 = match venue1 {
            TradingVenue::BinanceMargin => TradingVenue::BinanceSpot,
            _ => venue1,
        };

        let key = (
            store_venue1,
            symbol1.to_string(),
            venue2,
            symbol2.to_string(),
        );
        let mt_config = SpreadThresholdConfig {
            compare_op: CompareOp::GreaterThan,
            arb_direction: ArbDirection::Backward,
            operation: OperationType::Open,
            spread_type: SpreadType::AskBid,
            threshold: mt_threshold,
        };

        let mm_config = SpreadThresholdConfig {
            compare_op: CompareOp::GreaterThan,
            arb_direction: ArbDirection::Backward,
            operation: OperationType::Open,
            spread_type: SpreadType::SpreadRate,
            threshold: mm_threshold,
        };

        self.mt_thresholds
            .borrow_mut()
            .insert(key.clone(), mt_config);
        self.mm_thresholds.borrow_mut().insert(key, mm_config);
    }

    /// 设置反套撤单阈值
    pub fn set_backward_cancel_threshold(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        mm_threshold: f64,
        mt_threshold: f64,
    ) {
        // 映射：BinanceMargin 使用 BinanceSpot 的阈值（现货杠杆和现货共享盘口）
        let store_venue1 = match venue1 {
            TradingVenue::BinanceMargin => TradingVenue::BinanceSpot,
            _ => venue1,
        };

        let key = (
            store_venue1,
            symbol1.to_string(),
            venue2,
            symbol2.to_string(),
        );
        let mt_config = SpreadThresholdConfig {
            compare_op: CompareOp::LessThan,
            arb_direction: ArbDirection::Backward,
            operation: OperationType::Cancel,
            spread_type: SpreadType::AskBid,
            threshold: mt_threshold,
        };

        let mm_config = SpreadThresholdConfig {
            compare_op: CompareOp::LessThan,
            arb_direction: ArbDirection::Backward,
            operation: OperationType::Cancel,
            spread_type: SpreadType::SpreadRate,
            threshold: mm_threshold,
        };

        self.mt_thresholds
            .borrow_mut()
            .insert(key.clone(), mt_config);
        self.mm_thresholds.borrow_mut().insert(key, mm_config);
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
        // 映射：BinanceMargin 使用 BinanceSpot 的阈值（现货杠杆和现货共享盘口）
        let query_venue1 = match venue1 {
            TradingVenue::BinanceMargin => TradingVenue::BinanceSpot,
            _ => venue1,
        };

        let key = (
            query_venue1,
            symbol1.to_string(),
            venue2,
            symbol2.to_string(),
        );

        // 根据当前模式选择对应的 config
        let current_mode = self.get_mode();
        let thresholds = match current_mode {
            FactorMode::MM => self.mm_thresholds.borrow(),
            FactorMode::MT => self.mt_thresholds.borrow(),
        };

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == ArbDirection::Forward
                && config.operation == OperationType::Open
            {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                    SpreadType::SpreadRate => {
                        self.get_spread_rate(venue1, symbol1, venue2, symbol2)
                    }
                };

                if let Some(v) = value {
                    return config.compare_op.check(v, config.threshold);
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
        let query_venue1 = Self::map_venue(venue1);
        let key = (
            query_venue1,
            symbol1.to_string(),
            venue2,
            symbol2.to_string(),
        );

        // 根据当前模式选择对应的 config
        let current_mode = self.get_mode();
        let thresholds = match current_mode {
            FactorMode::MM => self.mm_thresholds.borrow(),
            FactorMode::MT => self.mt_thresholds.borrow(),
        };

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == ArbDirection::Forward
                && config.operation == OperationType::Cancel
            {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                    SpreadType::SpreadRate => {
                        self.get_spread_rate(venue1, symbol1, venue2, symbol2)
                    }
                };

                if let Some(v) = value {
                    return config.compare_op.check(v, config.threshold);
                }
            }
        }

        false
    }

    /// 检查是否满足正套平仓条件
    /// 正套平仓的操作方向和反套开仓一样，直接调用反套开仓逻辑
    pub fn satisfy_forward_close(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> bool {
        self.satisfy_backward_open(venue1, symbol1, venue2, symbol2)
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
        let query_venue1 = Self::map_venue(venue1);
        let key = (
            query_venue1,
            symbol1.to_string(),
            venue2,
            symbol2.to_string(),
        );

        // 根据当前模式选择对应的 config
        let current_mode = self.get_mode();
        let thresholds = match current_mode {
            FactorMode::MM => self.mm_thresholds.borrow(),
            FactorMode::MT => self.mt_thresholds.borrow(),
        };

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == ArbDirection::Backward
                && config.operation == OperationType::Open
            {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                    SpreadType::SpreadRate => {
                        self.get_spread_rate(venue1, symbol1, venue2, symbol2)
                    }
                };

                if let Some(v) = value {
                    return config.compare_op.check(v, config.threshold);
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
        let query_venue1 = Self::map_venue(venue1);
        let key = (
            query_venue1,
            symbol1.to_string(),
            venue2,
            symbol2.to_string(),
        );

        // 根据当前模式选择对应的 config
        let current_mode = self.get_mode();
        let thresholds = match current_mode {
            FactorMode::MM => self.mm_thresholds.borrow(),
            FactorMode::MT => self.mt_thresholds.borrow(),
        };

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == ArbDirection::Backward
                && config.operation == OperationType::Cancel
            {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                    SpreadType::SpreadRate => {
                        self.get_spread_rate(venue1, symbol1, venue2, symbol2)
                    }
                };

                if let Some(v) = value {
                    return config.compare_op.check(v, config.threshold);
                }
            }
        }

        false
    }

    /// 检查是否满足反套平仓条件
    /// 反套平仓的操作方向和正套开仓一样，直接调用正套开仓逻辑
    pub fn satisfy_backward_close(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> bool {
        self.satisfy_forward_open(venue1, symbol1, venue2, symbol2)
    }

    /// 获取价差检查详情（用于调试日志）
    /// 返回 (实际价差值, 阈值, 比较操作, 价差类型)
    pub fn get_spread_check_detail(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        arb_direction: ArbDirection,
        operation: OperationType,
    ) -> Option<(f64, f64, CompareOp, SpreadType)> {
        // 映射：BinanceMargin 使用 BinanceSpot 的阈值（现货杠杆和现货共享盘口）
        let query_venue1 = match venue1 {
            TradingVenue::BinanceMargin => TradingVenue::BinanceSpot,
            _ => venue1,
        };

        let key = (
            query_venue1,
            symbol1.to_string(),
            venue2,
            symbol2.to_string(),
        );

        // 根据当前模式选择对应的 config
        let current_mode = self.get_mode();
        let thresholds = match current_mode {
            FactorMode::MM => self.mm_thresholds.borrow(),
            FactorMode::MT => self.mt_thresholds.borrow(),
        };

        if let Some(config) = thresholds.get(&key) {
            if config.arb_direction == arb_direction && config.operation == operation {
                let value = match config.spread_type {
                    SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                    SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                    SpreadType::SpreadRate => {
                        self.get_spread_rate(venue1, symbol1, venue2, symbol2)
                    }
                };

                if let Some(v) = value {
                    return Some((v, config.threshold, config.compare_op, config.spread_type));
                } else {
                    log::debug!(
                        "SpreadFactor: 缺少 {:?} 价差数据 ({} {:?} -> {} {:?}), 方向={:?} 操作={:?}",
                        config.spread_type,
                        symbol1,
                        venue1,
                        symbol2,
                        venue2,
                        arb_direction,
                        operation
                    );
                }
            }
        } else {
            log::debug!(
                "SpreadFactor: 未找到阈值 ({} {:?} -> {} {:?}), 方向={:?} 操作={:?}",
                symbol1,
                venue1,
                symbol2,
                venue2,
                arb_direction,
                operation
            );
        }

        None
    }

    /// 调试：打印所有存储的价差数据
    pub fn debug_print_stored_spreads(&self, venue1: TradingVenue, venue2: TradingVenue) {
        // 映射：BinanceMargin -> BinanceSpot，保持与 get_* 查询一致
        let query_venue1 = Self::map_venue(venue1);
        let venue_pair = (query_venue1, venue2);

        if query_venue1 != venue1 {
            log::info!(
                "=== SpreadFactor 存储数据 ({:?} -> {:?} <-> {:?}) ===",
                venue1,
                query_venue1,
                venue2
            );
        } else {
            log::info!(
                "=== SpreadFactor 存储数据 ({:?} <-> {:?}) ===",
                venue1,
                venue2
            );
        }

        let askbid = self.askbid.borrow();
        if let Some(inner) = askbid.get(&venue_pair) {
            log::info!("  AskBid spreads: {} 个", inner.len());
            for (symbol_pair, value) in inner.iter().take(5) {
                log::info!("    {:?} = {:.6}", symbol_pair, value);
            }

            // 打印完整的 symbol 列表（用于排查）
            let mut symbols: Vec<String> = inner
                .keys()
                .map(|(s1, s2)| {
                    if s1 == s2 {
                        s1.clone()
                    } else {
                        format!("{}<->{}", s1, s2)
                    }
                })
                .collect();
            symbols.sort();
            log::info!("  完整 Symbol 列表: {}", symbols.join(", "));
        } else {
            log::info!("  AskBid spreads: 无数据");
        }

        let bidask = self.bidask.borrow();
        if let Some(inner) = bidask.get(&venue_pair) {
            log::info!("  BidAsk spreads: {} 个", inner.len());
            for (symbol_pair, value) in inner.iter().take(5) {
                log::info!("    {:?} = {:.6}", symbol_pair, value);
            }

            // 打印完整的 symbol 列表（用于排查）
            let mut symbols: Vec<String> = inner
                .keys()
                .map(|(s1, s2)| {
                    if s1 == s2 {
                        s1.clone()
                    } else {
                        format!("{}<->{}", s1, s2)
                    }
                })
                .collect();
            symbols.sort();
            log::info!("  完整 Symbol 列表: {}", symbols.join(", "));
        } else {
            log::info!("  BidAsk spreads: 无数据");
        }

        let spread_rate = self.spread_rate.borrow();
        if let Some(inner) = spread_rate.get(&venue_pair) {
            log::info!("  SpreadRate (mid price): {} 个", inner.len());
            for (symbol_pair, value) in inner.iter().take(5) {
                log::info!("    {:?} = {:.6}", symbol_pair, value);
            }

            // 打印完整的 symbol 列表（用于排查）
            let mut symbols: Vec<String> = inner
                .keys()
                .map(|(s1, s2)| {
                    if s1 == s2 {
                        s1.clone()
                    } else {
                        format!("{}<->{}", s1, s2)
                    }
                })
                .collect();
            symbols.sort();
            log::info!("  完整 Symbol 列表: {}", symbols.join(", "));
        } else {
            log::info!("  SpreadRate (mid price): 无数据");
        }
    }
}
