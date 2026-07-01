//! 价差因子单例模块
//!
//! 提供价差因子计算、存储和阈值判断功能。
//! 维护 askbid 和 bidask 两种价差因子，支持正套/反套开仓/撤单/平仓判断。

use super::common::{
    ArbDirection, CompareOp, FactorMode, OperationType, SymbolPair, ThresholdKey, VenuePair,
};
use mkt_parsers::symbol_match::normalize_symbol_for_whitelist;
use order_common::TradingVenue;
use std::cell::RefCell;
use std::collections::HashMap;

type SpreadThresholdEntryKey = (ThresholdKey, ArbDirection, OperationType);

const DEFAULT_FR_FWD_OPEN_SPREAD_LIMIT: f64 = 0.05;
const DEFAULT_FR_BWD_OPEN_SPREAD_LIMIT: f64 = -0.05;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FrOpenSpreadLimitOverride {
    pub fwd_open_spread: f64,
    pub bwd_open_spread: f64,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpreadCheckStatus {
    Pass,
    Blocked,
    MissingThreshold,
    MissingValue,
}

impl SpreadCheckStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Blocked => "blocked",
            Self::MissingThreshold => "miss_threshold",
            Self::MissingValue => "miss_value",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpreadCheckDiagnostic {
    pub threshold_key: ThresholdKey,
    pub value: Option<f64>,
    pub threshold: Option<f64>,
    pub compare_op: Option<CompareOp>,
    pub spread_type: Option<SpreadType>,
    pub status: SpreadCheckStatus,
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

    /// 阈值表: ((venue1, symbol1, venue2, symbol2), 方向, 操作) -> SpreadThresholdConfig
    mm_thresholds: RefCell<HashMap<SpreadThresholdEntryKey, SpreadThresholdConfig>>,

    mt_thresholds: RefCell<HashMap<SpreadThresholdEntryKey, SpreadThresholdConfig>>,
    /// 模式: MM 或 MT， mm采用mm的阈值，mt模式就用mt的阈值配置
    mode: RefCell<FactorMode>,

    /// FR open 固定 spread_rate 限制；启用后 open 还必须满足方向固定阈值。
    fr_open_spread_limit_enabled: RefCell<bool>,
    fr_fwd_open_spread_limit: RefCell<f64>,
    fr_bwd_open_spread_limit: RefCell<f64>,
    fr_open_spread_limit_overrides: RefCell<HashMap<String, FrOpenSpreadLimitOverride>>,
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
            fr_open_spread_limit_enabled: RefCell::new(false),
            fr_fwd_open_spread_limit: RefCell::new(DEFAULT_FR_FWD_OPEN_SPREAD_LIMIT),
            fr_bwd_open_spread_limit: RefCell::new(DEFAULT_FR_BWD_OPEN_SPREAD_LIMIT),
            fr_open_spread_limit_overrides: RefCell::new(HashMap::new()),
        }
    }

    /// 获取全局单例实例
    ///
    /// 使用 thread_local 实现单线程单例
    pub fn instance() -> &'static SpreadFactor {
        thread_local! {
            static INSTANCE: std::cell::OnceCell<SpreadFactor> = const { std::cell::OnceCell::new() };
        }

        INSTANCE.with(|cell| {
            // SAFETY: 我们确保只在单线程中使用,并且实例一旦创建就不会被销毁
            // 通过 thread_local 保证每个线程有自己的实例
            unsafe {
                let ptr = cell as *const std::cell::OnceCell<SpreadFactor>
                    as *mut std::cell::OnceCell<SpreadFactor>;
                (*ptr).get_or_init(SpreadFactor::new)
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
        let symbol_pair = (
            Self::normalize_symbol_key(symbol1),
            Self::normalize_symbol_key(symbol2),
        );

        // 计算 askbid_sr = (venue1_ask - venue2_bid) / venue1_ask
        let askbid = if venue1_ask > 0.0 && venue2_bid > 0.0 {
            let value = (venue1_ask - venue2_bid) / venue1_ask;
            self.askbid
                .borrow_mut()
                .entry(venue_pair)
                .or_default()
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
                .or_default()
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
                        .or_default()
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
        let query_venue1 = venue1;

        let venue_pair = (query_venue1, venue2);
        let symbol_pair = (
            Self::normalize_symbol_key(symbol1),
            Self::normalize_symbol_key(symbol2),
        );

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
        let query_venue1 = venue1;

        let venue_pair = (query_venue1, venue2);
        let symbol_pair = (
            Self::normalize_symbol_key(symbol1),
            Self::normalize_symbol_key(symbol2),
        );

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
        let query_venue1 = venue1;

        let venue_pair = (query_venue1, venue2);
        let symbol_pair = (
            Self::normalize_symbol_key(symbol1),
            Self::normalize_symbol_key(symbol2),
        );

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

    pub fn clear_thresholds(&self) {
        self.mm_thresholds.borrow_mut().clear();
        self.mt_thresholds.borrow_mut().clear();
    }

    // ===== 内部辅助方法 =====

    /// Venue 映射：BinanceMargin -> BinanceSpot（现货杠杆和现货共享盘口）
    #[inline]
    fn map_venue(venue: TradingVenue) -> TradingVenue {
        match venue {
            TradingVenue::BinanceMargin => TradingVenue::BinanceMargin,
            _ => venue,
        }
    }

    #[inline]
    fn normalize_symbol_key(symbol: &str) -> String {
        // Keep consistent with rolling_metrics/symbol_list: uppercase, remove '-'/'_', strip trailing "SWAP".
        normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
    }

    #[inline]
    fn threshold_pair_key(
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> ThresholdKey {
        (
            Self::map_venue(venue1),
            Self::normalize_symbol_key(symbol1),
            venue2,
            Self::normalize_symbol_key(symbol2),
        )
    }

    #[inline]
    fn threshold_entry_key(
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        arb_direction: ArbDirection,
        operation: OperationType,
    ) -> SpreadThresholdEntryKey {
        (
            Self::threshold_pair_key(venue1, symbol1, venue2, symbol2),
            arb_direction,
            operation,
        )
    }

    pub fn update_fr_open_spread_limit(
        &self,
        enabled: bool,
        fwd_open_spread: f64,
        bwd_open_spread: f64,
        overrides: HashMap<String, FrOpenSpreadLimitOverride>,
    ) {
        if !fwd_open_spread.is_finite() || !bwd_open_spread.is_finite() {
            log::warn!(
                "FR open spread limit ignored: non-finite fwd={} bwd={}",
                fwd_open_spread,
                bwd_open_spread
            );
            return;
        }
        for (symbol, value) in &overrides {
            if !value.fwd_open_spread.is_finite() || !value.bwd_open_spread.is_finite() {
                log::warn!(
                    "FR open spread limit override ignored: non-finite symbol={} fwd={} bwd={}",
                    symbol,
                    value.fwd_open_spread,
                    value.bwd_open_spread
                );
                return;
            }
        }

        let override_count = overrides.len();
        *self.fr_open_spread_limit_enabled.borrow_mut() = enabled;
        *self.fr_fwd_open_spread_limit.borrow_mut() = fwd_open_spread;
        *self.fr_bwd_open_spread_limit.borrow_mut() = bwd_open_spread;
        *self.fr_open_spread_limit_overrides.borrow_mut() = overrides;
        log::info!(
            "FR open spread limit updated: enabled={} fwd_open_spread={} bwd_open_spread={} override_symbols={}",
            enabled,
            fwd_open_spread,
            bwd_open_spread,
            override_count
        );
    }

    pub fn update_fr_open_spread_limit_global(
        &self,
        enabled: bool,
        fwd_open_spread: f64,
        bwd_open_spread: f64,
    ) {
        self.update_fr_open_spread_limit(enabled, fwd_open_spread, bwd_open_spread, HashMap::new());
    }

    fn resolve_fr_open_spread_limit(&self, symbol1: &str) -> FrOpenSpreadLimitOverride {
        let symbol_key = Self::normalize_symbol_key(symbol1);
        self.fr_open_spread_limit_overrides
            .borrow()
            .get(&symbol_key)
            .copied()
            .unwrap_or(FrOpenSpreadLimitOverride {
                fwd_open_spread: *self.fr_fwd_open_spread_limit.borrow(),
                bwd_open_spread: *self.fr_bwd_open_spread_limit.borrow(),
            })
    }

    fn fr_open_spread_limit_allows(
        &self,
        direction: ArbDirection,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
    ) -> bool {
        if !*self.fr_open_spread_limit_enabled.borrow() {
            return true;
        }

        let Some(spread_rate) = self.get_spread_rate(venue1, symbol1, venue2, symbol2) else {
            log::debug!(
                "FR open spread limit blocked: missing spread_rate direction={:?} for {} {:?} -> {} {:?}",
                direction,
                symbol1,
                venue1,
                symbol2,
                venue2
            );
            return false;
        };

        if !spread_rate.is_finite() {
            log::debug!(
                "FR open spread limit blocked: non-finite spread_rate={} direction={:?} for {} {:?} -> {} {:?}",
                spread_rate,
                direction,
                symbol1,
                venue1,
                symbol2,
                venue2
            );
            return false;
        }

        match direction {
            ArbDirection::Forward => {
                let limit = self.resolve_fr_open_spread_limit(symbol1).fwd_open_spread;
                if spread_rate < limit {
                    true
                } else {
                    log::debug!(
                        "FR forward open blocked: spread_rate={:.6} >= fwd_open_spread={:.6} for {} {:?} -> {} {:?}",
                        spread_rate,
                        limit,
                        symbol1,
                        venue1,
                        symbol2,
                        venue2
                    );
                    false
                }
            }
            ArbDirection::Backward => {
                let limit = self.resolve_fr_open_spread_limit(symbol1).bwd_open_spread;
                if spread_rate > limit {
                    true
                } else {
                    log::debug!(
                        "FR backward open blocked: spread_rate={:.6} <= bwd_open_spread={:.6} for {} {:?} -> {} {:?}",
                        spread_rate,
                        limit,
                        symbol1,
                        venue1,
                        symbol2,
                        venue2
                    );
                    false
                }
            }
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
        let store_venue1 = venue1;

        let key = (
            (
                store_venue1,
                Self::normalize_symbol_key(symbol1),
                venue2,
                Self::normalize_symbol_key(symbol2),
            ),
            ArbDirection::Forward,
            OperationType::Open,
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
        let store_venue1 = venue1;

        let key = (
            (
                store_venue1,
                Self::normalize_symbol_key(symbol1),
                venue2,
                Self::normalize_symbol_key(symbol2),
            ),
            ArbDirection::Forward,
            OperationType::Cancel,
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
        let store_venue1 = venue1;

        let key = (
            (
                store_venue1,
                Self::normalize_symbol_key(symbol1),
                venue2,
                Self::normalize_symbol_key(symbol2),
            ),
            ArbDirection::Backward,
            OperationType::Open,
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
        let store_venue1 = venue1;

        let key = (
            (
                store_venue1,
                Self::normalize_symbol_key(symbol1),
                venue2,
                Self::normalize_symbol_key(symbol2),
            ),
            ArbDirection::Backward,
            OperationType::Cancel,
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
        self.satisfy_forward_open_inner(venue1, symbol1, venue2, symbol2, true)
    }

    fn satisfy_forward_open_inner(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        apply_open_guard: bool,
    ) -> bool {
        if apply_open_guard
            && !self.fr_open_spread_limit_allows(
                ArbDirection::Forward,
                venue1,
                symbol1,
                venue2,
                symbol2,
            )
        {
            return false;
        }

        // 映射：BinanceMargin 使用 BinanceSpot 的阈值（现货杠杆和现货共享盘口）
        let query_venue1 = venue1;

        let key = (
            (
                query_venue1,
                Self::normalize_symbol_key(symbol1),
                venue2,
                Self::normalize_symbol_key(symbol2),
            ),
            ArbDirection::Forward,
            OperationType::Open,
        );

        // 根据当前模式选择对应的 config
        let current_mode = self.get_mode();
        let thresholds = match current_mode {
            FactorMode::MM => self.mm_thresholds.borrow(),
            FactorMode::MT => self.mt_thresholds.borrow(),
        };

        if let Some(config) = thresholds.get(&key) {
            let value = match config.spread_type {
                SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                SpreadType::SpreadRate => self.get_spread_rate(venue1, symbol1, venue2, symbol2),
            };

            if let Some(v) = value {
                return config.compare_op.check(v, config.threshold);
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
            (
                query_venue1,
                Self::normalize_symbol_key(symbol1),
                venue2,
                Self::normalize_symbol_key(symbol2),
            ),
            ArbDirection::Forward,
            OperationType::Cancel,
        );

        // 根据当前模式选择对应的 config
        let current_mode = self.get_mode();
        let thresholds = match current_mode {
            FactorMode::MM => self.mm_thresholds.borrow(),
            FactorMode::MT => self.mt_thresholds.borrow(),
        };

        if let Some(config) = thresholds.get(&key) {
            let value = match config.spread_type {
                SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                SpreadType::SpreadRate => self.get_spread_rate(venue1, symbol1, venue2, symbol2),
            };

            if let Some(v) = value {
                return config.compare_op.check(v, config.threshold);
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
        self.satisfy_backward_open_inner(venue1, symbol1, venue2, symbol2, false)
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
        self.satisfy_backward_open_inner(venue1, symbol1, venue2, symbol2, true)
    }

    fn satisfy_backward_open_inner(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        apply_open_guard: bool,
    ) -> bool {
        if apply_open_guard
            && !self.fr_open_spread_limit_allows(
                ArbDirection::Backward,
                venue1,
                symbol1,
                venue2,
                symbol2,
            )
        {
            return false;
        }

        let query_venue1 = Self::map_venue(venue1);
        let key = (
            (
                query_venue1,
                Self::normalize_symbol_key(symbol1),
                venue2,
                Self::normalize_symbol_key(symbol2),
            ),
            ArbDirection::Backward,
            OperationType::Open,
        );

        // 根据当前模式选择对应的 config
        let current_mode = self.get_mode();
        let thresholds = match current_mode {
            FactorMode::MM => self.mm_thresholds.borrow(),
            FactorMode::MT => self.mt_thresholds.borrow(),
        };

        if let Some(config) = thresholds.get(&key) {
            let value = match config.spread_type {
                SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                SpreadType::SpreadRate => self.get_spread_rate(venue1, symbol1, venue2, symbol2),
            };

            if let Some(v) = value {
                return config.compare_op.check(v, config.threshold);
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
            (
                query_venue1,
                Self::normalize_symbol_key(symbol1),
                venue2,
                Self::normalize_symbol_key(symbol2),
            ),
            ArbDirection::Backward,
            OperationType::Cancel,
        );

        // 根据当前模式选择对应的 config
        let current_mode = self.get_mode();
        let thresholds = match current_mode {
            FactorMode::MM => self.mm_thresholds.borrow(),
            FactorMode::MT => self.mt_thresholds.borrow(),
        };

        if let Some(config) = thresholds.get(&key) {
            let value = match config.spread_type {
                SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                SpreadType::SpreadRate => self.get_spread_rate(venue1, symbol1, venue2, symbol2),
            };

            if let Some(v) = value {
                return config.compare_op.check(v, config.threshold);
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
        self.satisfy_forward_open_inner(venue1, symbol1, venue2, symbol2, false)
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
        let query_venue1 = venue1;

        let key = Self::threshold_entry_key(
            query_venue1,
            symbol1,
            venue2,
            symbol2,
            arb_direction,
            operation,
        );

        // 根据当前模式选择对应的 config
        let current_mode = self.get_mode();
        let thresholds = match current_mode {
            FactorMode::MM => self.mm_thresholds.borrow(),
            FactorMode::MT => self.mt_thresholds.borrow(),
        };

        if let Some(config) = thresholds.get(&key) {
            let value = match config.spread_type {
                SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
                SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
                SpreadType::SpreadRate => self.get_spread_rate(venue1, symbol1, venue2, symbol2),
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

    pub fn get_spread_check_diagnostic(
        &self,
        venue1: TradingVenue,
        symbol1: &str,
        venue2: TradingVenue,
        symbol2: &str,
        arb_direction: ArbDirection,
        operation: OperationType,
    ) -> SpreadCheckDiagnostic {
        let query_venue1 = venue1;
        let threshold_key = Self::threshold_pair_key(query_venue1, symbol1, venue2, symbol2);
        let key = (threshold_key.clone(), arb_direction, operation);

        let current_mode = self.get_mode();
        let config = {
            let thresholds = match current_mode {
                FactorMode::MM => self.mm_thresholds.borrow(),
                FactorMode::MT => self.mt_thresholds.borrow(),
            };
            thresholds.get(&key).cloned()
        };

        let Some(config) = config else {
            return SpreadCheckDiagnostic {
                threshold_key,
                value: None,
                threshold: None,
                compare_op: None,
                spread_type: None,
                status: SpreadCheckStatus::MissingThreshold,
            };
        };

        let value = match config.spread_type {
            SpreadType::BidAsk => self.get_bidask(venue1, symbol1, venue2, symbol2),
            SpreadType::AskBid => self.get_askbid(venue1, symbol1, venue2, symbol2),
            SpreadType::SpreadRate => self.get_spread_rate(venue1, symbol1, venue2, symbol2),
        };
        let status = match value {
            Some(value) if config.compare_op.check(value, config.threshold) => {
                SpreadCheckStatus::Pass
            }
            Some(_) => SpreadCheckStatus::Blocked,
            None => SpreadCheckStatus::MissingValue,
        };

        SpreadCheckDiagnostic {
            threshold_key,
            value,
            threshold: Some(config.threshold),
            compare_op: Some(config.compare_op),
            spread_type: Some(config.spread_type),
            status,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fr_open_spread_limit_is_disabled_by_default() {
        let spread_factor = SpreadFactor::new();
        spread_factor.set_mode(FactorMode::MM);

        spread_factor.set_forward_open_threshold(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            1.0,
            0.0,
        );
        spread_factor.set_backward_open_threshold(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            -1.0,
            0.0,
        );

        let _ = spread_factor.update(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            0.085,
            0.086,
            0.035,
            0.036,
        );

        assert!(spread_factor.satisfy_forward_open(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
        ));
        assert!(spread_factor.satisfy_backward_open(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
        ));
    }

    #[test]
    fn fr_open_spread_limit_blocks_forward_and_backward_open() {
        let spread_factor = SpreadFactor::new();
        spread_factor.set_mode(FactorMode::MM);
        spread_factor.update_fr_open_spread_limit_global(true, 0.05, -0.05);

        spread_factor.set_forward_open_threshold(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            1.0,
            0.0,
        );
        spread_factor.set_backward_open_threshold(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            -1.0,
            0.0,
        );

        let _ = spread_factor.update(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            0.085,
            0.086,
            0.035,
            0.036,
        );

        assert!(!spread_factor.satisfy_forward_open(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
        ));

        let _ = spread_factor.update(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            100.0,
            100.2,
            106.0,
            106.2,
        );

        assert!(!spread_factor.satisfy_backward_open(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
        ));
    }

    #[test]
    fn fr_open_spread_limit_symbol_override_replaces_global_limits() {
        let spread_factor = SpreadFactor::new();
        spread_factor.set_mode(FactorMode::MM);
        spread_factor.update_fr_open_spread_limit(
            true,
            0.05,
            -0.05,
            HashMap::from([(
                "SIRENUSDT".to_string(),
                FrOpenSpreadLimitOverride {
                    fwd_open_spread: 1.0,
                    bwd_open_spread: -0.10,
                },
            )]),
        );

        spread_factor.set_forward_open_threshold(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            1.0,
            0.0,
        );
        spread_factor.set_backward_open_threshold(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            -1.0,
            0.0,
        );

        let _ = spread_factor.update(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            0.085,
            0.086,
            0.035,
            0.036,
        );

        assert!(spread_factor.satisfy_forward_open(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
        ));

        let _ = spread_factor.update(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
            100.0,
            100.2,
            106.0,
            106.2,
        );

        assert!(spread_factor.satisfy_backward_open(
            TradingVenue::BinanceMargin,
            "SIRENUSDT",
            TradingVenue::BinanceFutures,
            "SIRENUSDT",
        ));

        spread_factor.set_forward_open_threshold(
            TradingVenue::BinanceMargin,
            "HNTUSDT",
            TradingVenue::BinanceFutures,
            "HNTUSDT",
            1.0,
            0.0,
        );
        let _ = spread_factor.update(
            TradingVenue::BinanceMargin,
            "HNTUSDT",
            TradingVenue::BinanceFutures,
            "HNTUSDT",
            0.085,
            0.086,
            0.035,
            0.036,
        );
        assert!(!spread_factor.satisfy_forward_open(
            TradingVenue::BinanceMargin,
            "HNTUSDT",
            TradingVenue::BinanceFutures,
            "HNTUSDT",
        ));
    }

    #[test]
    fn fr_open_spread_limit_allows_normal_price_match() {
        let spread_factor = SpreadFactor::new();
        spread_factor.set_mode(FactorMode::MM);
        spread_factor.update_fr_open_spread_limit_global(true, 0.05, -0.05);

        spread_factor.set_forward_open_threshold(
            TradingVenue::GateMargin,
            "HNTUSDT",
            TradingVenue::GateFutures,
            "HNTUSDT",
            0.03,
            0.0,
        );
        spread_factor.set_backward_open_threshold(
            TradingVenue::GateMargin,
            "HNTUSDT",
            TradingVenue::GateFutures,
            "HNTUSDT",
            0.01,
            0.0,
        );

        let _ = spread_factor.update(
            TradingVenue::GateMargin,
            "HNTUSDT",
            TradingVenue::GateFutures,
            "HNTUSDT",
            100.0,
            100.2,
            98.0,
            98.2,
        );

        assert!(spread_factor.satisfy_forward_open(
            TradingVenue::GateMargin,
            "HNTUSDT",
            TradingVenue::GateFutures,
            "HNTUSDT",
        ));
        assert!(spread_factor.satisfy_backward_open(
            TradingVenue::GateMargin,
            "HNTUSDT",
            TradingVenue::GateFutures,
            "HNTUSDT",
        ));
    }

    #[test]
    fn fr_open_spread_limit_does_not_apply_to_close() {
        let spread_factor = SpreadFactor::new();
        spread_factor.set_mode(FactorMode::MM);
        spread_factor.update_fr_open_spread_limit_global(true, 0.05, -0.05);

        spread_factor.set_backward_open_threshold(
            TradingVenue::GateMargin,
            "HNTUSDT",
            TradingVenue::GateFutures,
            "HNTUSDT",
            -1.0,
            0.0,
        );

        let _ = spread_factor.update(
            TradingVenue::GateMargin,
            "HNTUSDT",
            TradingVenue::GateFutures,
            "HNTUSDT",
            100.0,
            100.2,
            106.0,
            106.2,
        );

        assert!(!spread_factor.satisfy_backward_open(
            TradingVenue::GateMargin,
            "HNTUSDT",
            TradingVenue::GateFutures,
            "HNTUSDT",
        ));
        assert!(spread_factor.satisfy_forward_close(
            TradingVenue::GateMargin,
            "HNTUSDT",
            TradingVenue::GateFutures,
            "HNTUSDT",
        ));
    }

    #[test]
    fn forward_and_backward_open_thresholds_do_not_overwrite_each_other() {
        let spread_factor = SpreadFactor::new();
        spread_factor.set_mode(FactorMode::MM);

        spread_factor.set_forward_open_threshold(
            TradingVenue::BinanceMargin,
            "BTCUSDT",
            TradingVenue::BinanceFutures,
            "BTCUSDT",
            0.0010,
            0.0,
        );
        spread_factor.set_backward_open_threshold(
            TradingVenue::BinanceMargin,
            "BTCUSDT",
            TradingVenue::BinanceFutures,
            "BTCUSDT",
            0.0004,
            0.0,
        );

        let _ = spread_factor.update(
            TradingVenue::BinanceMargin,
            "BTCUSDT",
            TradingVenue::BinanceFutures,
            "BTCUSDT",
            100.0,
            100.0,
            99.95,
            99.95,
        );

        let forward = spread_factor.get_spread_check_detail(
            TradingVenue::BinanceMargin,
            "BTCUSDT",
            TradingVenue::BinanceFutures,
            "BTCUSDT",
            ArbDirection::Forward,
            OperationType::Open,
        );
        let backward = spread_factor.get_spread_check_detail(
            TradingVenue::BinanceMargin,
            "BTCUSDT",
            TradingVenue::BinanceFutures,
            "BTCUSDT",
            ArbDirection::Backward,
            OperationType::Open,
        );

        assert!(forward.is_some());
        assert!(backward.is_some());
        let (_, forward_threshold, _, _) = forward.unwrap();
        let (_, backward_threshold, _, _) = backward.unwrap();
        assert!((forward_threshold - 0.0010).abs() < 1e-12);
        assert!((backward_threshold - 0.0004).abs() < 1e-12);
    }
}
