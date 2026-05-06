use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use crate::common::basic_account_msg::{
    split_basic_account_event, BasicAccountEventType, BasicAccountScope, BasicBalanceMsg,
    BasicBorrowInterestMsg, BasicPositionMsg, BasicUmUnrealizedMsg, BinanceBasicOrderMsg,
    GateBasicOrderMsg, OkexOrderMsg,
};
use crate::common::bitget_account_msg::BitgetBasicOrderMsg;
use crate::common::bybit_account_msg::BybitBasicOrderMsg;
use crate::common::exchange::Exchange;
use crate::common::ipc_service_name::build_service_name;
use crate::common::min_qty_table::MinQtyTable;
use crate::common::symbol_util::{min_qty_symbol_key, normalize_symbol_for_internal};
use crate::common::time_util::get_timestamp_us;
use crate::portfolio_margin::pm_forwarder::{
    PM_HISTORY_SIZE, PM_MAX_SUBSCRIBERS, PM_SUBSCRIBER_MAX_BUFFER_SIZE,
};
use crate::pre_trade::basic_balance_manager::BasicBalanceManager;
use crate::pre_trade::basic_exposure_manager::{BasicExposureEntry, BasicExposureManager};
use crate::pre_trade::basic_um_manager::BasicUmManager;
use crate::pre_trade::net_position::NetPosition;
use crate::pre_trade::order_manager::Side;
use crate::pre_trade::price_table::PriceTable;
use crate::pre_trade::symbol_mapper::create_symbol_mapper;
use crate::pre_trade::symbol_util::extract_base_asset;
use crate::pre_trade::usdt_balance_manager::{UsdtBalanceManager, UsdtBalanceSnapshot};
use crate::pre_trade::PersistChannel;
use crate::signal::cancel_signal::{ArbCancelCtx, ArbCancelReason, MmCancelCtx, MmCancelReason};
use crate::signal::common::{ExecutionType, OrderStatus, SignalBytes, TradingLeg, TradingVenue};
use crate::signal::trade_signal::{SignalType, TradeSignal};

const ACCOUNT_PAYLOAD: usize = 16_384;
const DERIVATIVES_PAYLOAD: usize = 128;
const DERIVATIVES_HISTORY_SIZE: usize = 50;
const DERIVATIVES_MAX_SUBSCRIBERS: usize = 64;
const DERIVATIVES_SUBSCRIBER_MAX_BUFFER: usize = 8192;
const BINANCE_DERIVATIVES_SERVICE: &str = "bridge/binance-futures/derivatives";
const OKEX_DERIVATIVES_SERVICE: &str = "bridge/okex-futures/derivatives";
const BYBIT_DERIVATIVES_SERVICE: &str = "bridge/bybit-futures/derivatives";
const BITGET_DERIVATIVES_SERVICE: &str = "bridge/bitget-futures/derivatives";
const GATE_DERIVATIVES_SERVICE: &str = "bridge/gate-futures/derivatives";
const DEFAULT_NODE_PRE_TRADE_DERIVATIVES: &str = "pre_trade_derivatives";

// ==================== Helper Functions ====================

fn is_margin_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceMargin
            | TradingVenue::OkexMargin
            | TradingVenue::GateMargin
            | TradingVenue::BitgetMargin
            | TradingVenue::BybitMargin
    )
}

fn is_futures_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures
            | TradingVenue::OkexFutures
            | TradingVenue::GateFutures
            | TradingVenue::BitgetFutures
            | TradingVenue::BybitFutures
    )
}

fn exchange_from_venue(venue: TradingVenue) -> Exchange {
    match venue {
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => Exchange::Binance,
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => Exchange::Okex,
        TradingVenue::GateMargin | TradingVenue::GateFutures => Exchange::Gate,
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => Exchange::Bitget,
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => Exchange::Bybit,
        _ => panic!("unsupported venue for pre_trade: {:?}", venue),
    }
}

fn scope_for_venue(
    venue: TradingVenue,
    binance_account_mode: Option<BinanceAccountMode>,
) -> BasicAccountScope {
    match venue {
        TradingVenue::BinanceMargin => {
            if binance_account_mode == Some(BinanceAccountMode::Standard) {
                BasicAccountScope::BinanceStdSpot
            } else {
                BasicAccountScope::BinanceUnified
            }
        }
        TradingVenue::BinanceFutures => {
            if binance_account_mode == Some(BinanceAccountMode::Standard) {
                BasicAccountScope::BinanceStdUm
            } else {
                BasicAccountScope::BinanceUnified
            }
        }
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => BasicAccountScope::OkexUnified,
        TradingVenue::GateMargin | TradingVenue::GateFutures => BasicAccountScope::GateUnified,
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
            BasicAccountScope::BitgetUnified
        }
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => BasicAccountScope::BybitUnified,
        _ => BasicAccountScope::Unknown,
    }
}

fn scope_matches_venue(
    incoming_scope: BasicAccountScope,
    source_exchange: Exchange,
    venue: TradingVenue,
    binance_account_mode: Option<BinanceAccountMode>,
) -> bool {
    if incoming_scope == BasicAccountScope::Unknown {
        return exchange_from_venue(venue) == source_exchange;
    }
    incoming_scope == scope_for_venue(venue, binance_account_mode)
}

// ==================== Deduplication Cache ====================

/// 简单的去重缓存（固定容量，FIFO 淘汰）
pub struct DedupCache {
    set: HashSet<u64>,
    queue: VecDeque<u64>,
    capacity: usize,
}

impl DedupCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            queue: VecDeque::new(),
            capacity: capacity.max(1024),
        }
    }

    /// 插入并返回是否为新条目；false 表示重复，应丢弃
    pub fn insert_check(&mut self, key: u64) -> bool {
        if self.set.contains(&key) {
            return false;
        }
        if self.queue.len() >= self.capacity {
            if let Some(old) = self.queue.pop_front() {
                self.set.remove(&old);
            }
        }
        self.queue.push_back(key);
        self.set.insert(key);
        true
    }
}

/// 组合多个 u64 片段生成稳定的 64 位哈希
pub fn hash64(parts: &[u64]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for p in parts {
        p.hash(&mut hasher);
    }
    hasher.finish()
}

// ==================== Monitor Channel ====================

use crate::common::binance_account_mode::BinanceAccountMode;
use crate::common::msg_parser::{get_msg_type, parse_index_price, parse_mark_price, MktMsgType};
use crate::pre_trade::order_manager::OrderManager;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::signal::common::{align_price_ceil, align_price_floor};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::OrphanStrategyManager;
use bytes::Bytes;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

// Thread-local 单例存储
thread_local! {
    static MONITOR_CHANNEL: RefCell<Option<MonitorChannelInner>> = const { RefCell::new(None) };
}

/// MonitorChannel 单例访问器（零大小类型）
pub struct MonitorChannel;

/// 每条腿的基础管理器（类似 C++ variant）
#[derive(Clone)]
enum LegMgr {
    /// 现货/保证金腿，sz=标的资产数量
    Margin {
        exchange: Exchange,
        bal: Rc<RefCell<BasicBalanceManager>>,
    },
    /// U 本位合约腿：Binance 按 contracts(mult=1) 处理，OKX/Gate 按 contracts(需合约乘数)处理
    Futures {
        exchange: Exchange,
        um: Rc<RefCell<BasicUmManager>>,
        min_qty_table: Rc<RefCell<MinQtyTable>>,
    },
}

impl LegMgr {
    fn as_balance_mgr(&self) -> Option<Rc<RefCell<BasicBalanceManager>>> {
        match self {
            LegMgr::Margin { bal, .. } => Some(bal.clone()),
            _ => None,
        }
    }

    fn as_um_mgr(&self) -> Option<(Rc<RefCell<BasicUmManager>>, Rc<RefCell<MinQtyTable>>)> {
        match self {
            LegMgr::Futures {
                um, min_qty_table, ..
            } => Some((um.clone(), min_qty_table.clone())),
            _ => None,
        }
    }
}

/// MonitorChannel 内部实现，包含所有状态
struct MonitorChannelInner {
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    open_leg: LegMgr,
    hedge_leg: LegMgr,
    /// USDT 单独维护：account_scope -> manager（Binance standard 下 margin/futures 分离）
    usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>>,
    /// 价格表（仍使用 Binance mark/index 价格作为统一估值源）
    price_table: Rc<RefCell<PriceTable>>,
    /// 各交易场所的最小下单量/步进信息
    venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>>,
    /// 策略管理器
    strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    /// orphan 策略管理器（统一承载 mm orphan / arb orphan）
    orphan_strategy_mgr: Rc<RefCell<OrphanStrategyManager>>,
    /// 订单管理器，所有订单维护在其中，完全成交或者撤单会被移除
    order_manager: Rc<RefCell<OrderManager>>,
    /// Monotonic counter incremented when a TradeUpdate is received.
    trade_update_seq: u64,
}

struct BasicState {
    // asset -> (open_qty, hedge_qty), both in base units
    exposures: HashMap<String, (f64, f64)>,
    total_equity_usdt: f64,
    abs_total_exposure_usdt: f64,
    total_position_usdt: f64,
    total_um_unrealized_usdt: f64,
}

impl MonitorChannel {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        MonitorChannel
    }

    /// 访问内部状态的辅助方法（内部使用）
    fn with_inner<F, R>(f: F) -> R
    where
        F: FnOnce(&MonitorChannelInner) -> R,
    {
        MONITOR_CHANNEL.with(|mc| {
            let mc_ref = mc.borrow();
            let inner = mc_ref.as_ref().expect("MonitorChannel not initialized");
            f(inner)
        })
    }

    fn try_with_inner<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&MonitorChannelInner) -> R,
    {
        MONITOR_CHANNEL
            .try_with(|mc| {
                let mc_ref = mc.borrow();
                mc_ref.as_ref().map(f)
            })
            .ok()
            .flatten()
    }

    fn with_inner_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut MonitorChannelInner) -> R,
    {
        MONITOR_CHANNEL.with(|mc| {
            let mut mc_ref = mc.borrow_mut();
            let inner = mc_ref.as_mut().expect("MonitorChannel not initialized");
            f(inner)
        })
    }

    pub fn bump_trade_update_seq(&self) {
        Self::with_inner_mut(|inner| {
            inner.trade_update_seq = inner.trade_update_seq.saturating_add(1);
        });
    }

    pub fn trade_update_seq(&self) -> u64 {
        Self::with_inner(|inner| inner.trade_update_seq)
    }

    /// 获取指定交易场所的最小下单量表
    pub fn venue_min_qty_table(&self, venue: TradingVenue) -> Option<Rc<VenueMinQtyTable>> {
        Self::with_inner(|inner| inner.venue_min_qty_tables.get(&venue).cloned())
    }

    /// 尝试获取指定交易场所的最小下单量表（若 MonitorChannel 未初始化则返回 None）
    pub fn try_venue_min_qty_table(&self, venue: TradingVenue) -> Option<Rc<VenueMinQtyTable>> {
        Self::try_with_inner(|inner| inner.venue_min_qty_tables.get(&venue).cloned()).flatten()
    }

    /// 获取 venue qty -> base qty 的乘数。需要合约乘数的交易所缺失配置时返回错误。
    pub fn qty_multiplier_for_venue(
        &self,
        venue: TradingVenue,
        symbol: &str,
    ) -> Result<f64, String> {
        Self::with_inner(|inner| Self::qty_multiplier_for_venue_inner(inner, venue, symbol))
    }

    /// 获取 order_manager 的引用
    pub fn order_manager(&self) -> Rc<RefCell<OrderManager>> {
        Self::with_inner(|inner| inner.order_manager.clone())
    }

    pub fn try_order_manager() -> Option<Rc<RefCell<OrderManager>>> {
        Self::try_with_inner(|inner| inner.order_manager.clone())
    }

    /// 获取 price_table 的引用
    pub fn price_table(&self) -> Rc<RefCell<PriceTable>> {
        Self::with_inner(|inner| inner.price_table.clone())
    }

    pub fn try_price_table(&self) -> Option<Rc<RefCell<PriceTable>>> {
        Self::try_with_inner(|inner| inner.price_table.clone())
    }

    pub fn open_venue(&self) -> TradingVenue {
        Self::with_inner(|inner| inner.open_venue)
    }

    pub fn hedge_venue(&self) -> TradingVenue {
        Self::with_inner(|inner| inner.hedge_venue)
    }

    fn cancel_mm_open_strategies_for_symbol_side(
        &self,
        symbol: &str,
        side: Side,
        trigger_ts: i64,
        reason: MmCancelReason,
    ) -> usize {
        let normalized_symbol = normalize_symbol_for_internal(symbol);
        if normalized_symbol.is_empty() {
            return 0;
        }

        let strategy_mgr = self.strategy_mgr();
        let candidate_ids: Vec<i32> = {
            let mgr = strategy_mgr.borrow();
            mgr.ids_for_symbol(&normalized_symbol)
                .map(|set| set.iter().copied().collect())
                .unwrap_or_default()
        };
        if candidate_ids.is_empty() {
            return 0;
        }

        let open_venue = self.open_venue();
        let mut cancelled = 0usize;
        for strategy_id in candidate_ids {
            let mut strategy = {
                let mut mgr = strategy_mgr.borrow_mut();
                let Some(entry) = mgr.mm_open_price_map_entry(strategy_id).cloned() else {
                    continue;
                };
                if entry.side != side {
                    continue;
                }
                match mgr.take(strategy_id) {
                    Some(strategy) => strategy,
                    None => continue,
                }
            };

            let mut cancel_ctx = MmCancelCtx::new();
            cancel_ctx.opening_leg = TradingLeg {
                venue: open_venue.to_u8(),
                bid0: 0.0,
                ask0: 0.0,
                ts: trigger_ts,
            };
            cancel_ctx.set_opening_symbol(&normalized_symbol);
            cancel_ctx.set_side(side);
            cancel_ctx.set_reason(reason);
            cancel_ctx.trigger_ts = trigger_ts;
            cancel_ctx.set_from_key(b"mm_position_risk".to_vec());
            if let Some(entry) = strategy_mgr.borrow().mm_open_price_map_entry(strategy_id) {
                cancel_ctx.set_target_strategy(strategy_id, entry.client_order_id);
            } else {
                cancel_ctx.set_target_strategy(strategy_id, 0);
            }

            let signal = TradeSignal::create(
                SignalType::MMCancel,
                trigger_ts,
                trigger_ts as f64,
                cancel_ctx.to_bytes(),
            );
            strategy.handle_signal(&signal);
            if strategy.is_active() {
                strategy_mgr.borrow_mut().insert(strategy);
            }
            cancelled += 1;
        }

        cancelled
    }

    fn handle_mm_position_risk_after_update(&self, symbol: &str) {
        let normalized_symbol = normalize_symbol_for_internal(symbol);
        if normalized_symbol.is_empty() {
            return;
        }
        if self.open_venue() != self.hedge_venue() {
            return;
        }
        if self.check_symbol_exposure(&normalized_symbol).is_ok() {
            return;
        }

        let venue = self.open_venue();
        let net_qty = self.get_position_qty(&normalized_symbol, venue);
        let Some(cancel_side) = (if net_qty > 0.0 {
            Some(Side::Buy)
        } else if net_qty < 0.0 {
            Some(Side::Sell)
        } else {
            None
        }) else {
            return;
        };

        let trigger_ts = get_timestamp_us();
        let cancelled = self.cancel_mm_open_strategies_for_symbol_side(
            &normalized_symbol,
            cancel_side,
            trigger_ts,
            MmCancelReason::PositionRisk,
        );
        if cancelled > 0 {
            warn!(
                "MM position risk cancel triggered: symbol={} venue={:?} net_qty={:.8} cancel_side={:?} cancelled_strategies={} trigger_ts={}",
                normalized_symbol, venue, net_qty, cancel_side, cancelled, trigger_ts
            );
        }
    }

    fn cancel_arb_open_strategies_for_symbol_side(
        &self,
        symbol: &str,
        side: Side,
        trigger_ts: i64,
        reason: ArbCancelReason,
    ) -> usize {
        let normalized_symbol = normalize_symbol_for_internal(symbol);
        if normalized_symbol.is_empty() {
            return 0;
        }

        let strategy_mgr = self.strategy_mgr();
        let candidate_ids: Vec<i32> = {
            let mgr = strategy_mgr.borrow();
            mgr.ids_for_symbol(&normalized_symbol)
                .map(|set| set.iter().copied().collect())
                .unwrap_or_default()
        };
        if candidate_ids.is_empty() {
            return 0;
        }

        let open_venue = self.open_venue();
        let hedge_venue = self.hedge_venue();
        let mut cancelled = 0usize;
        for strategy_id in candidate_ids {
            let mut strategy = {
                let mut mgr = strategy_mgr.borrow_mut();
                let Some(entry) = mgr.arb_open_price_map_entry(strategy_id).cloned() else {
                    continue;
                };
                if entry.side != side {
                    continue;
                }
                match mgr.take(strategy_id) {
                    Some(strategy) => strategy,
                    None => continue,
                }
            };

            let mut cancel_ctx = ArbCancelCtx::new();
            cancel_ctx.opening_leg = TradingLeg {
                venue: open_venue.to_u8(),
                bid0: 0.0,
                ask0: 0.0,
                ts: trigger_ts,
            };
            cancel_ctx.set_opening_symbol(&normalized_symbol);
            cancel_ctx.hedging_leg = TradingLeg {
                venue: hedge_venue.to_u8(),
                bid0: 0.0,
                ask0: 0.0,
                ts: trigger_ts,
            };
            cancel_ctx.set_hedging_symbol(&normalized_symbol);
            cancel_ctx.set_side(side);
            cancel_ctx.set_reason(reason);
            cancel_ctx.trigger_ts = trigger_ts;
            cancel_ctx.set_from_key(b"arb_position_risk".to_vec());
            cancel_ctx.set_target_strategy(strategy_id);

            let signal = TradeSignal::create(
                SignalType::ArbCancel,
                trigger_ts,
                trigger_ts as f64,
                cancel_ctx.to_bytes(),
            );
            strategy.handle_signal(&signal);
            if strategy.is_active() {
                strategy_mgr.borrow_mut().insert(strategy);
            }
            cancelled += 1;
        }

        cancelled
    }

    fn handle_arb_position_risk_after_update(&self, symbol: &str) {
        let normalized_symbol = normalize_symbol_for_internal(symbol);
        if normalized_symbol.is_empty() {
            return;
        }
        if self.open_venue() == self.hedge_venue() {
            return;
        }
        if self.check_symbol_exposure(&normalized_symbol).is_ok() {
            return;
        }

        let open_venue = self.open_venue();
        let hedge_venue = self.hedge_venue();
        let net_qty = self.get_position_qty(&normalized_symbol, open_venue);
        let Some(cancel_side) = (if net_qty > 0.0 {
            Some(Side::Buy)
        } else if net_qty < 0.0 {
            Some(Side::Sell)
        } else {
            None
        }) else {
            return;
        };

        let trigger_ts = get_timestamp_us();
        let cancelled = self.cancel_arb_open_strategies_for_symbol_side(
            &normalized_symbol,
            cancel_side,
            trigger_ts,
            ArbCancelReason::PositionRisk,
        );
        if cancelled > 0 {
            warn!(
                "Arb position risk cancel triggered: symbol={} open_venue={:?} hedge_venue={:?} net_qty={:.8} cancel_side={:?} cancelled_strategies={} trigger_ts={}",
                normalized_symbol, open_venue, hedge_venue, net_qty, cancel_side, cancelled, trigger_ts
            );
        }
    }

    pub fn mark_price_exchange(&self) -> Exchange {
        Self::with_inner(|inner| {
            Self::mark_price_exchange_for_venues(inner.open_venue, inner.hedge_venue)
        })
    }

    pub fn try_mark_price_exchange(&self) -> Option<Exchange> {
        Self::try_with_inner(|inner| {
            Self::mark_price_exchange_for_venues(inner.open_venue, inner.hedge_venue)
        })
    }

    fn mark_price_exchange_for_venues(
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Exchange {
        let open_exchange = exchange_from_venue(open_venue);
        let hedge_exchange = exchange_from_venue(hedge_venue);
        if open_exchange == hedge_exchange {
            return open_exchange;
        }

        for preferred in [Exchange::Okex, Exchange::Bybit, Exchange::Binance] {
            if open_exchange == preferred || hedge_exchange == preferred {
                return preferred;
            }
        }

        Exchange::Binance
    }

    fn derivatives_service_for_mark_price_source(
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> &'static str {
        match Self::mark_price_exchange_for_venues(open_venue, hedge_venue) {
            Exchange::Okex => OKEX_DERIVATIVES_SERVICE,
            Exchange::Bybit => BYBIT_DERIVATIVES_SERVICE,
            Exchange::Bitget => BITGET_DERIVATIVES_SERVICE,
            Exchange::Gate => GATE_DERIVATIVES_SERVICE,
            _ => BINANCE_DERIVATIVES_SERVICE,
        }
    }

    pub fn usdt_mgr(&self, scope: BasicAccountScope) -> Option<Rc<RefCell<UsdtBalanceManager>>> {
        Self::with_inner(|inner| inner.usdt_mgrs.get(&scope).cloned())
    }

    pub fn usdt_snapshot_all(&self) -> Vec<(BasicAccountScope, UsdtBalanceSnapshot)> {
        Self::with_inner(|inner| {
            let mut out: Vec<(BasicAccountScope, UsdtBalanceSnapshot)> = inner
                .usdt_mgrs
                .iter()
                .map(|(scope, mgr)| (*scope, mgr.borrow().snapshot()))
                .collect();
            out.sort_by_key(|(scope, _)| *scope as u32);
            out
        })
    }

    pub fn usdt_snapshot_for_venue(&self, venue: TradingVenue) -> Option<UsdtBalanceSnapshot> {
        Self::with_inner(|inner| {
            let binance_mode = if inner.order_manager.borrow().binance_is_standard() {
                Some(BinanceAccountMode::Standard)
            } else {
                Some(BinanceAccountMode::Unified)
            };
            let scope = scope_for_venue(venue, binance_mode);
            inner
                .usdt_mgrs
                .get(&scope)
                .map(|mgr| mgr.borrow().snapshot())
        })
    }

    pub fn account_scope_for_venue(&self, venue: TradingVenue) -> BasicAccountScope {
        Self::with_inner(|inner| {
            let binance_mode = if inner.order_manager.borrow().binance_is_standard() {
                Some(BinanceAccountMode::Standard)
            } else {
                Some(BinanceAccountMode::Unified)
            };
            scope_for_venue(venue, binance_mode)
        })
    }

    /// 获取当前基础风控口径的快照（用于 resample/viz）
    ///
    /// 返回：
    /// - `exposures`: asset -> (open_qty, hedge_qty)，都按标的数量（base qty）表达
    /// - `total_equity_usdt`: USDT 总权益（eq 口径；若涉及合约 venue，会叠加 UPL）
    /// - `abs_total_exposure_usdt`: 各资产净敞口按 USDT 估值后取绝对值求和
    /// - `total_position_usdt`: 各资产现货/合约头寸按 USDT 估值后取绝对值求和
    /// - `total_um_unrealized_usdt`: 合约未实现盈亏（USDT 计价）
    pub fn basic_state_snapshot(&self) -> (HashMap<String, (f64, f64)>, f64, f64, f64, f64) {
        Self::with_inner(|inner| {
            let state = Self::compute_basic_state(inner);
            (
                state.exposures,
                state.total_equity_usdt,
                state.abs_total_exposure_usdt,
                state.total_position_usdt,
                state.total_um_unrealized_usdt,
            )
        })
    }

    /// 获取 strategy_mgr 的引用
    pub fn strategy_mgr(&self) -> Rc<RefCell<crate::strategy::StrategyManager>> {
        Self::with_inner(|inner| inner.strategy_mgr.clone())
    }

    pub fn try_strategy_mgr() -> Option<Rc<RefCell<crate::strategy::StrategyManager>>> {
        Self::try_with_inner(|inner| inner.strategy_mgr.clone())
    }

    pub fn orphan_strategy_mgr(&self) -> Rc<RefCell<OrphanStrategyManager>> {
        Self::with_inner(|inner| inner.orphan_strategy_mgr.clone())
    }

    pub fn try_orphan_strategy_mgr() -> Option<Rc<RefCell<OrphanStrategyManager>>> {
        Self::try_with_inner(|inner| inner.orphan_strategy_mgr.clone())
    }

    /// 获取开仓腿的基础余额管理器（margin/spot）
    pub fn open_balance_mgr(&self) -> Option<Rc<RefCell<BasicBalanceManager>>> {
        Self::with_inner(|inner| inner.open_leg.as_balance_mgr())
    }

    /// 获取对冲腿的基础余额管理器（margin/spot）
    pub fn hedge_balance_mgr(&self) -> Option<Rc<RefCell<BasicBalanceManager>>> {
        Self::with_inner(|inner| inner.hedge_leg.as_balance_mgr())
    }

    /// 获取开仓腿的基础合约管理器（futures）
    pub fn open_um_mgr(&self) -> Option<(Rc<RefCell<BasicUmManager>>, Rc<RefCell<MinQtyTable>>)> {
        Self::with_inner(|inner| inner.open_leg.as_um_mgr())
    }

    /// 获取对冲腿的基础合约管理器（futures）
    pub fn hedge_um_mgr(&self) -> Option<(Rc<RefCell<BasicUmManager>>, Rc<RefCell<MinQtyTable>>)> {
        Self::with_inner(|inner| inner.hedge_leg.as_um_mgr())
    }

    /// 查询指定 venue+asset 的现货/保证金净头寸（base qty），非 margin venue 返回 0
    pub fn balance_position_for_venue(&self, venue: TradingVenue, asset: &str) -> f64 {
        Self::with_inner(|inner| {
            let leg = if venue == inner.open_venue {
                &inner.open_leg
            } else if venue == inner.hedge_venue {
                &inner.hedge_leg
            } else {
                return 0.0;
            };
            if asset.eq_ignore_ascii_case("USDT") {
                let binance_mode = if inner.order_manager.borrow().binance_is_standard() {
                    Some(BinanceAccountMode::Standard)
                } else {
                    Some(BinanceAccountMode::Unified)
                };
                let scope = scope_for_venue(venue, binance_mode);
                return inner
                    .usdt_mgrs
                    .get(&scope)
                    .map(|m| m.borrow().net_usdt_position())
                    .unwrap_or(0.0);
            }
            match leg {
                LegMgr::Margin { bal, .. } => bal.borrow().net_position(asset, None),
                _ => 0.0,
            }
        })
    }

    /// 初始化 pre-trade 的账户与风控管理器（仅 open/hedge 两条腿）
    ///
    /// - 按 venue 的 market type 映射到 BasicBalanceManager / BasicUmManager
    /// - 订阅 account_pubs/<exchange>_pm（期望收到 BasicAccountEventMsg）
    /// - 初始化各 venue 的 min_qty/price_tick 表用于对齐
    pub async fn init_singleton(
        strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        binance_account_mode: Option<BinanceAccountMode>,
    ) -> Result<()> {
        // 仅支持当前已接入 pre_trade 的交易所
        for v in [open_venue, hedge_venue] {
            if !matches!(
                v,
                TradingVenue::BinanceMargin
                    | TradingVenue::BinanceFutures
                    | TradingVenue::OkexMargin
                    | TradingVenue::OkexFutures
                    | TradingVenue::BybitMargin
                    | TradingVenue::BybitFutures
                    | TradingVenue::BitgetMargin
                    | TradingVenue::BitgetFutures
                    | TradingVenue::GateMargin
                    | TradingVenue::GateFutures
            ) {
                panic!("pre_trade does not support venue {:?}", v);
            }
        }

        let open_exchange = exchange_from_venue(open_venue);
        let hedge_exchange = exchange_from_venue(hedge_venue);

        // 初始化 USDT 管理器（按账户 scope 维度，Binance standard 下 margin/futures 分离）
        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        for (scope, ex) in [
            (
                scope_for_venue(open_venue, binance_account_mode),
                open_exchange,
            ),
            (
                scope_for_venue(hedge_venue, binance_account_mode),
                hedge_exchange,
            ),
        ] {
            usdt_mgrs
                .entry(scope)
                .or_insert_with(|| Rc::new(RefCell::new(UsdtBalanceManager::new(ex))));
        }

        // 初始化开仓腿基础管理器
        let open_leg = if is_margin_venue(open_venue) {
            LegMgr::Margin {
                exchange: open_exchange,
                bal: Rc::new(RefCell::new(BasicBalanceManager::new(open_exchange))),
            }
        } else if is_futures_venue(open_venue) {
            let mut min_qty_table = MinQtyTable::new(open_exchange);
            if let Err(err) = min_qty_table.refresh().await {
                warn!(
                    "failed to refresh min_qty_table for {:?}: {err:#}",
                    open_exchange
                );
            }
            LegMgr::Futures {
                exchange: open_exchange,
                um: Rc::new(RefCell::new(BasicUmManager::new(open_exchange))),
                min_qty_table: Rc::new(RefCell::new(min_qty_table)),
            }
        } else {
            unreachable!()
        };

        // 初始化对冲腿基础管理器
        let hedge_leg = if is_margin_venue(hedge_venue) {
            LegMgr::Margin {
                exchange: hedge_exchange,
                bal: Rc::new(RefCell::new(BasicBalanceManager::new(hedge_exchange))),
            }
        } else if is_futures_venue(hedge_venue) {
            let mut min_qty_table = MinQtyTable::new(hedge_exchange);
            if let Err(err) = min_qty_table.refresh().await {
                warn!(
                    "failed to refresh min_qty_table for {:?}: {err:#}",
                    hedge_exchange
                );
            }
            LegMgr::Futures {
                exchange: hedge_exchange,
                um: Rc::new(RefCell::new(BasicUmManager::new(hedge_exchange))),
                min_qty_table: Rc::new(RefCell::new(min_qty_table)),
            }
        } else {
            unreachable!()
        };

        // 创建价格表（价格由 derivatives stream 持续更新）
        let price_table = Rc::new(RefCell::new(PriceTable::new()));

        // 加载交易对 LOT_SIZE/PRICE_FILTER（按 venue 区分），用于数量/价格对齐
        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        for venue in [open_venue, hedge_venue] {
            if venue_min_qty_tables.contains_key(&venue) {
                continue;
            }
            let mut table = VenueMinQtyTable::new(venue);
            if let Err(err) = table.refresh().await {
                warn!("failed to refresh filters for venue {:?}: {err:#}", venue);
            }
            venue_min_qty_tables.insert(venue, Rc::new(table));
        }

        // 为涉及的交易所启动 basic 账户监听（可能是一个或两个）
        let mut exchanges: HashSet<Exchange> = HashSet::new();
        exchanges.insert(open_exchange);
        exchanges.insert(hedge_exchange);
        for ex in exchanges {
            let service_name = build_service_name(&format!("account_pubs/{}_pm", ex.as_str()));
            let node_name = format!("pre_trade_account_pubs_{}_pm", ex.as_str());
            Self::spawn_basic_listener(
                service_name,
                node_name,
                ex,
                open_venue,
                hedge_venue,
                open_leg.clone(),
                hedge_leg.clone(),
                usdt_mgrs.clone(),
                binance_account_mode,
                strategy_mgr.clone(),
            );
        }

        // 启动衍生品价格监听任务（mark_price, index_price）
        //
        // 约定：默认使用 Binance Futures 的衍生品指标；当 open/hedge 两腿都属于 OKX 时，
        // 切换到 OKX Futures 的 mark/index price。统一从 bridge 订阅，避免继续占用
        // dat_pbs 的 subscriber 配额。
        let node_name = DEFAULT_NODE_PRE_TRADE_DERIVATIVES.to_string();
        let service_name =
            Self::derivatives_service_for_mark_price_source(open_venue, hedge_venue).to_string();
        Self::spawn_derivatives_listener(price_table.clone(), node_name, service_name);

        // 创建内部实例并保存到 thread-local
        let inner = MonitorChannelInner {
            open_venue,
            hedge_venue,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table,
            venue_min_qty_tables,
            strategy_mgr,
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(binance_account_mode))),
            trade_update_seq: 0,
        };

        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

        Ok(())
    }

    /// 将订单数量（按 venue 语义）转换为 base qty（标的数量）
    fn order_qty_to_base(
        inner: &MonitorChannelInner,
        venue: TradingVenue,
        symbol: &str,
        qty: f64,
    ) -> f64 {
        match venue {
            TradingVenue::BinanceFutures => qty,
            TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                let symbol_key = min_qty_symbol_key(venue, symbol);
                let mult = inner
                    .venue_min_qty_tables
                    .get(&venue)
                    .map(|t| t.contract_multiplier(&symbol_key))
                    .unwrap_or(1.0);
                qty * mult
            }
            _ => qty,
        }
    }

    /// 将订单数量（按 venue 语义）转换为 base qty（标的数量）
    ///
    /// 用于风控等关键路径：对于需要合约乘数的 venue，若乘数缺失则直接返回错误，避免默认 1 导致风险口径失真。
    fn order_qty_to_base_checked(
        inner: &MonitorChannelInner,
        venue: TradingVenue,
        symbol: &str,
        qty: f64,
    ) -> Result<f64, String> {
        match venue {
            TradingVenue::BinanceFutures => Ok(qty),
            TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                let mult = Self::qty_multiplier_for_venue_inner(inner, venue, symbol)?;
                Ok(qty * mult)
            }
            _ => Ok(qty),
        }
    }

    fn qty_multiplier_for_venue_inner(
        inner: &MonitorChannelInner,
        venue: TradingVenue,
        symbol: &str,
    ) -> Result<f64, String> {
        match venue {
            TradingVenue::BinanceFutures => Ok(1.0),
            TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                let symbol_key = min_qty_symbol_key(venue, symbol);
                let Some(table) = inner.venue_min_qty_tables.get(&venue) else {
                    return Err(format!(
                        "未初始化 {:?} 的最小下单量表，无法获取乘数 symbol={}",
                        venue, symbol_key
                    ));
                };
                let Some(multiplier) = table.contract_multiplier_opt(&symbol_key) else {
                    return Err(format!(
                        "symbol={} 缺少 {:?} 合约乘数，无法转换 qty 口径",
                        symbol_key, venue
                    ));
                };
                if multiplier <= 0.0 {
                    return Err(format!(
                        "symbol={} {:?} contract multiplier invalid: {}",
                        symbol_key, venue, multiplier
                    ));
                }
                Ok(multiplier)
            }
            _ => Ok(1.0),
        }
    }

    /// 将订单数量（按 venue 语义）转换为 base qty（标的数量）
    ///
    /// - Binance futures: qty 按 contracts(mult=1) 处理，等价于 base qty
    /// - OKX/Gate futures: qty 是 contracts，需要乘以合约面值（contract multiplier）
    pub fn qty_to_base(&self, venue: TradingVenue, symbol: &str, qty: f64) -> f64 {
        Self::with_inner(|inner| Self::order_qty_to_base(inner, venue, symbol, qty))
    }

    /// 基于 open/hedge 两腿的基础管理器计算敞口与总量指标
    fn compute_basic_state(inner: &MonitorChannelInner) -> BasicState {
        let price_snap = inner.price_table.borrow().snapshot();
        // MM 模式下 open_venue == hedge_venue 时，两条腿实际指向同一账户数据，
        // 若同时统计会造成敞口翻倍；此时仅以 open 单边为准。
        let same_venue = inner.open_venue == inner.hedge_venue;

        fn collect_leg_entries(leg: &LegMgr) -> Vec<BasicExposureEntry> {
            match leg {
                LegMgr::Margin { exchange, bal } => {
                    let mgr = bal.borrow();
                    let mgr_ref: &BasicBalanceManager = &mgr;
                    BasicExposureManager::compute_exposures_for_exchange(
                        *exchange,
                        std::slice::from_ref(&mgr_ref),
                        &[],
                    )
                }
                LegMgr::Futures {
                    exchange,
                    um,
                    min_qty_table,
                } => {
                    let um_mgr = um.borrow();
                    let min_qty = min_qty_table.borrow();
                    let um_pair = (&*um_mgr, &*min_qty);
                    BasicExposureManager::compute_exposures_for_exchange(
                        *exchange,
                        &[],
                        std::slice::from_ref(&um_pair),
                    )
                }
            }
        }

        let open_entries = collect_leg_entries(&inner.open_leg);
        let hedge_entries = if same_venue {
            Vec::new()
        } else {
            collect_leg_entries(&inner.hedge_leg)
        };

        let price_mapper = create_symbol_mapper(Self::mark_price_exchange_for_venues(
            inner.open_venue,
            inner.hedge_venue,
        ));
        let mark_price_usdt = |asset: &str| -> f64 {
            if asset.eq_ignore_ascii_case("USDT") {
                1.0
            } else {
                let symbol = price_mapper.asset_to_price_symbol(asset);
                price_snap.get(&symbol).map(|p| p.mark_price).unwrap_or(0.0)
            }
        };

        let mut exposures: HashMap<String, (f64, f64)> = HashMap::new();
        for entry in open_entries {
            if entry.exposure.abs() <= 1e-12 {
                continue;
            }
            let asset = entry.asset.to_uppercase();
            exposures.entry(asset).or_insert((0.0, 0.0)).0 += entry.exposure;
        }
        for entry in hedge_entries {
            if entry.exposure.abs() <= 1e-12 {
                continue;
            }
            let asset = entry.asset.to_uppercase();
            exposures.entry(asset).or_insert((0.0, 0.0)).1 += entry.exposure;
        }

        // total_equity(eq) 口径：
        // - 非 USDT 资产：从 balance manager 统计净资产估值
        // - USDT：按交易所维度单独维护
        // - Binance/OKX/Bitget 等 futures UPL 单独来自 BasicUmManager 并叠加
        // - Gate unified 的 balance/equity 已隐含账户级合约影响，因此只保留 UPL 展示，不再重复叠加
        let mut total_equity_usdt: f64 = 0.0;
        for (idx, leg) in [&inner.open_leg, &inner.hedge_leg].iter().enumerate() {
            if same_venue && idx == 1 {
                continue;
            }
            if let LegMgr::Margin { exchange, bal } = leg {
                let mgr = bal.borrow();
                let mgr_ref: &BasicBalanceManager = &mgr;
                let mut exposure_mgr = BasicExposureManager::new_from_sources(
                    *exchange,
                    std::slice::from_ref(&mgr_ref),
                    &[],
                );
                exposure_mgr.revalue_with_prices(&price_snap);
                total_equity_usdt += exposure_mgr.total_equity();
            }
        }
        // 加上各账户 scope 的 USDT 净头寸（Binance standard 下 margin/futures 分离）
        for (scope, mgr) in &inner.usdt_mgrs {
            let net = mgr.borrow().net_usdt_position();
            if net.abs() <= 1e-12 {
                continue;
            }
            debug!("USDT net position: scope={} net={:.6}", scope.as_str(), net);
            total_equity_usdt += net;
        }

        let mut total_um_unrealized_usdt = 0.0;
        for (idx, leg) in [&inner.open_leg, &inner.hedge_leg].iter().enumerate() {
            if same_venue && idx == 1 {
                continue;
            }
            if let LegMgr::Futures { exchange, um, .. } = leg {
                let upl = um.borrow().total_unrealized_pnl_usdt();
                total_um_unrealized_usdt += upl;
                if *exchange != Exchange::Gate {
                    total_equity_usdt += upl;
                }
            }
        }
        let mut total_position_usdt = 0.0;
        let mut abs_total_exposure_usdt = 0.0;
        for (asset, (open_qty, hedge_qty)) in &exposures {
            if asset == "USDT" {
                continue;
            }
            let mark = mark_price_usdt(asset);
            if mark <= 0.0 {
                continue;
            }
            total_position_usdt += (open_qty.abs() + hedge_qty.abs()) * mark;
            abs_total_exposure_usdt += ((open_qty + hedge_qty) * mark).abs();
        }

        BasicState {
            exposures,
            total_equity_usdt,
            abs_total_exposure_usdt,
            total_position_usdt,
            total_um_unrealized_usdt,
        }
    }

    // 检查杠杆率是否超过配置阈值
    pub fn check_leverage(&self) -> Result<(), String> {
        Self::with_inner(|inner| {
            let limit = PreTradeParamsLoader::instance().max_leverage();
            if limit <= 0.0 {
                return Ok(());
            }

            let state = Self::compute_basic_state(inner);
            let total_equity = state.total_equity_usdt;
            let um_unrealized = state.total_um_unrealized_usdt;
            let total_position = state.total_position_usdt;

            if total_equity <= f64::EPSILON {
                return Err("账户总权益(eq，含UPL如有合约)近似为 0，无法计算杠杆率".to_string());
            }

            let leverage = total_position / total_equity;
            if leverage > limit {
                debug!(
                    "当前杠杆 {:.4} 超过阈值 {:.4} (仓位={:.6}, 权益eq={:.6}, UPL={:.6})",
                    leverage, limit, total_position, total_equity, um_unrealized
                );
                return Err(format!("杠杆率 {:.2} 超过限制 {:.2}", leverage, limit));
            }

            Ok(())
        })
    }

    fn align_order_with_table(
        symbol: &str,
        raw_qty: f64,
        raw_price: f64,
        table: &VenueMinQtyTable,
        enforce_min_notional: bool,
    ) -> Result<(f64, f64), String> {
        if raw_qty <= 0.0 {
            return Err(format!(
                "symbol={} 原始下单量无效 raw_qty={}",
                symbol, raw_qty
            ));
        }
        if raw_price <= 0.0 {
            return Err(format!(
                "symbol={} 原始价格无效 raw_price={}",
                symbol, raw_price
            ));
        }

        // 1. 价格按 tick 对齐
        let price_tick = table.price_tick(symbol).unwrap_or(0.0);
        let price = if price_tick > 0.0 {
            align_price_floor(raw_price, price_tick)
        } else {
            raw_price
        };
        if price <= 0.0 {
            return Err(format!("symbol={} 对齐后价格无效 price={}", symbol, price));
        }

        // 2. 数量按 step 对齐
        let step = table.step_size(symbol).unwrap_or(0.0);
        let mut qty = if step > 0.0 {
            align_price_floor(raw_qty, step)
        } else {
            raw_qty
        };

        // 3. 补齐最小下单量
        if let Some(min_qty) = table.min_qty(symbol) {
            if min_qty > 0.0 && qty < min_qty {
                qty = min_qty;
            }
        }

        // 4. 补齐最小名义金额（仅限 futures 场景）
        if enforce_min_notional {
            if let Some(min_notional) = table.min_notional(symbol) {
                if min_notional > 0.0 {
                    let required_qty = min_notional / price;
                    if qty < required_qty {
                        let before = qty;
                        qty = if step > 0.0 {
                            align_price_ceil(required_qty, step)
                        } else {
                            required_qty
                        };
                        debug!(
                            "symbol={} 名义金额要求从 {} 调整到 {} (min_notional={}, price={})",
                            symbol, before, qty, min_notional, price
                        );
                    }
                }
            }
        }

        if qty <= 0.0 {
            return Err(format!("symbol={} 对齐后数量无效 qty={}", symbol, qty));
        }

        Ok((qty, price))
    }

    /// 根据交易场所对齐订单量和价格
    /// 返回 (对齐后的数量, 对齐后的价格)
    pub fn align_order_by_venue(
        &self,
        venue: TradingVenue,
        symbol: &str,
        raw_qty: f64,
        raw_price: f64,
    ) -> Result<(f64, f64), String> {
        Self::with_inner(|inner| {
            let symbol_key = min_qty_symbol_key(venue, symbol);

            let Some(table) = inner.venue_min_qty_tables.get(&venue) else {
                return Err(format!(
                    "未初始化 {:?} 的最小下单量表，请检查启动参数",
                    venue
                ));
            };

            match venue {
                TradingVenue::BinanceFutures => {
                    // Binance U 本地统一按 contracts(multiplier=1.0) 处理
                    Self::align_order_with_table(
                        &symbol_key,
                        raw_qty,
                        raw_price,
                        table.as_ref(),
                        true,
                    )
                }
                TradingVenue::BinanceMargin => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    false,
                ),
                TradingVenue::OkexMargin => {
                    // OKX 现货/保证金 sz 使用标的资产数量，与 BinanceMargin 语义一致
                    Self::align_order_with_table(
                        &symbol_key,
                        raw_qty,
                        raw_price,
                        table.as_ref(),
                        false,
                    )
                }
                TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                    // OKX/Gate 永续/交割合约 sz 使用“张数”，需要用合约乘数将 base qty 转成 contracts
                    let contract_size = table.contract_multiplier_opt(&symbol_key).ok_or_else(|| {
                        format!(
                            "symbol={} 缺少 {:?} 合约乘数，无法将 base qty 转成 contracts（请刷新 filters/multipliers）",
                            symbol_key,
                            venue
                        )
                    })?;
                    if contract_size <= 0.0 {
                        return Err(format!(
                            "symbol={} {:?} contract multiplier invalid: {}",
                            symbol_key, venue, contract_size
                        ));
                    }
                    let raw_contracts = raw_qty / contract_size;
                    debug!(
                        "futures qty convert: venue={:?} symbol={} raw_base_qty={:.8} contract_size={:.8} -> raw_contracts={:.8}",
                        venue, symbol_key, raw_qty, contract_size, raw_contracts
                    );
                    Self::align_order_with_table(
                        &symbol_key,
                        raw_contracts,
                        raw_price,
                        table.as_ref(),
                        true,
                    )
                }
                TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
                    Err("尚未实现 Bitget 的订单对齐".to_string())
                }
                TradingVenue::BybitMargin => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    false,
                ),
                TradingVenue::BybitFutures => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    true,
                ),
                TradingVenue::GateMargin => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    false,
                ),
                TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures => {
                    Err("尚未实现 Hyperliquid 的订单对齐".to_string())
                }
                TradingVenue::AsterMargin | TradingVenue::AsterFutures => {
                    Err("尚未实现 Aster 的订单对齐".to_string())
                }
            }
        })
    }

    /// 检查交易量是否满足最小要求
    /// 包括最小下单量和最小名义金额检查
    pub fn check_min_trading_requirements(
        &self,
        venue: TradingVenue,
        symbol: &str,
        qty: f64,
        price_hint: Option<f64>,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            let symbol_key = min_qty_symbol_key(venue, symbol);

            let Some(table) = inner.venue_min_qty_tables.get(&venue) else {
                return Err(format!(
                    "未初始化 {:?} 的最小下单量表，请检查启动参数",
                    venue
                ));
            };

            // 1. 检查最小下单量
            let min_qty = table.min_qty(&symbol_key).unwrap_or(0.0);

            if min_qty > 0.0 && qty + 1e-12 < min_qty {
                return Err(format!("交易量 {:.8} 小于最小下单量 {:.8}", qty, min_qty));
            }

            // 2. 检查最小名义金额（仅对 UM 合约）
            if matches!(
                venue,
                TradingVenue::BinanceFutures
                    | TradingVenue::OkexFutures
                    | TradingVenue::BitgetFutures
                    | TradingVenue::BybitFutures
                    | TradingVenue::GateFutures
            ) {
                let min_notional = table.min_notional(&symbol_key).unwrap_or(0.0);

                if min_notional > 0.0 {
                    // 如果没有提供价格提示，尝试从价格表获取
                    let price = if let Some(p) = price_hint {
                        p
                    } else {
                        inner
                            .price_table
                            .borrow()
                            .mark_price(&symbol_key)
                            .unwrap_or(0.0)
                    };

                    if price <= 0.0 {
                        return Err(format!("缺少 {} 的价格信息，无法验证名义金额", symbol));
                    }

                    let notional = price * qty;
                    if notional + 1e-8 < min_notional {
                        return Err(format!(
                            "名义金额 {:.8} 低于最小要求 {:.8} (价格={:.8} 数量={:.8})",
                            notional, min_notional, price, qty
                        ));
                    }
                }
            }

            Ok(())
        })
    }

    fn spawn_basic_listener(
        service_name: String,
        node_name: String,
        exchange: Exchange,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        open_leg: LegMgr,
        hedge_leg: LegMgr,
        usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>>,
        binance_account_mode: Option<BinanceAccountMode>,
        strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    ) {
        tokio::task::spawn_local(async move {
            let service_name_for_error = service_name.clone();

            let result: Result<()> = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;

                // Gate 依赖 account_monitor 的 client_order_id，对缺失严格报错；
                // 其他交易所允许先启动 pre_trade，再由 account_monitor 补上。
                let service_name_obj = ServiceName::new(&service_name)?;
                let service_builder = || {
                    node.service_builder(&service_name_obj)
                        .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
                        .max_publishers(1)
                        .max_subscribers(PM_MAX_SUBSCRIBERS)
                        .history_size(PM_HISTORY_SIZE)
                        .subscriber_max_buffer_size(PM_SUBSCRIBER_MAX_BUFFER_SIZE)
                };
                let require_existing_service = exchange == Exchange::Gate
                    || (exchange == Exchange::Binance
                        && binance_account_mode == Some(BinanceAccountMode::Standard));
                let service = if require_existing_service {
                    loop {
                        match service_builder().open() {
                            Ok(service) => break service,
                            Err(err) => {
                                warn!(
                                    "waiting for account_monitor service: service={} exchange={:?} err={:?}",
                                    service_name, exchange, err
                                );
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                } else {
                    match service_builder().open() {
                        Ok(service) => service,
                        Err(err) => {
                            warn!(
                                "account_monitor service missing, continue with open_or_create: service={} err={:?}",
                                service_name, err
                            );
                            service_builder().open_or_create().unwrap_or_else(|err| {
                                panic!(
                                    "创建账户 IceOryx service 失败: service={} err={:?}",
                                    service_name, err
                                )
                            })
                        }
                    }
                };
                let subscriber: Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()> = service
                    .subscriber_builder()
                    .create()
                    .unwrap_or_else(|err| {
                        panic!(
                            "创建账户 IceOryx subscriber 失败: service={} err={:?}",
                            service_name, err
                        )
                    });

                info!(
                    "basic account stream subscribed: service={} exchange={:?}",
                    service_name, exchange
                );

                let mut dedup = DedupCache::new(8192);

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = sample.payload();
                            let Some((msg_type, account_scope, data)) =
                                split_basic_account_event(payload)
                            else {
                                continue;
                            };

                            let mut hasher = std::collections::hash_map::DefaultHasher::new();
                            payload.hash(&mut hasher);
                            let key = hasher.finish();
                            if !dedup.insert_check(key) {
                                continue;
                            }

                            match msg_type {
                                BasicAccountEventType::BalanceUpdate => {
                                    if let Ok(msg) = BasicBalanceMsg::from_bytes(data) {
                                        if msg.symbol.eq_ignore_ascii_case("USDT") {
                                            if let Some(mgr) = usdt_mgrs.get(&account_scope) {
                                                mgr.borrow_mut().apply_balance(&msg);
                                            }
                                        }
                                        if scope_matches_venue(
                                            account_scope,
                                            exchange,
                                            open_venue,
                                            binance_account_mode,
                                        ) {
                                            if let LegMgr::Margin { bal, .. } = &open_leg {
                                                bal.borrow_mut().apply_balance(&msg);
                                            }
                                        }
                                        if scope_matches_venue(
                                            account_scope,
                                            exchange,
                                            hedge_venue,
                                            binance_account_mode,
                                        ) {
                                            if let LegMgr::Margin { bal, .. } = &hedge_leg {
                                                bal.borrow_mut().apply_balance(&msg);
                                            }
                                        }
                                    }
                                }
                                BasicAccountEventType::PositionUpdate => {
                                    if let Ok(msg) = BasicPositionMsg::from_bytes(data) {
                                        if exchange == Exchange::Okex
                                            && !msg.inst_id.contains('-')
                                            && !msg.inst_id.contains("-SWAP")
                                        {
                                            warn!(
                                                "drop malformed OKX position update (unexpected inst_id format): exchange={:?} inst_id={} side={} amt={} ts={}",
                                                exchange,
                                                msg.inst_id,
                                                msg.position_side,
                                                msg.position_amount,
                                                msg.timestamp
                                            );
                                            continue;
                                        }
                                        if scope_matches_venue(
                                            account_scope,
                                            exchange,
                                            open_venue,
                                            binance_account_mode,
                                        ) {
                                            if let LegMgr::Futures { um, .. } = &open_leg {
                                                um.borrow_mut().apply_position(&msg);
                                            }
                                        }
                                        if scope_matches_venue(
                                            account_scope,
                                            exchange,
                                            hedge_venue,
                                            binance_account_mode,
                                        ) {
                                            if let LegMgr::Futures { um, .. } = &hedge_leg {
                                                um.borrow_mut().apply_position(&msg);
                                            }
                                        }
                                        let symbol = normalize_symbol_for_internal(&msg.inst_id);
                                        if !symbol.is_empty() {
                                            if open_venue == hedge_venue {
                                                MonitorChannel::instance()
                                                    .handle_mm_position_risk_after_update(&symbol);
                                            } else {
                                                MonitorChannel::instance()
                                                    .handle_arb_position_risk_after_update(&symbol);
                                            }
                                        }
                                    }
                                }
                                BasicAccountEventType::UnrealizedPnlUpdate => {
                                    if let Ok(msg) = BasicUmUnrealizedMsg::from_bytes(data) {
                                        if scope_matches_venue(
                                            account_scope,
                                            exchange,
                                            open_venue,
                                            binance_account_mode,
                                        ) {
                                            if let LegMgr::Futures { um, .. } = &open_leg {
                                                um.borrow_mut().apply_unrealized_pnl(&msg);
                                            }
                                        }
                                        if scope_matches_venue(
                                            account_scope,
                                            exchange,
                                            hedge_venue,
                                            binance_account_mode,
                                        ) {
                                            if let LegMgr::Futures { um, .. } = &hedge_leg {
                                                um.borrow_mut().apply_unrealized_pnl(&msg);
                                            }
                                        }
                                    }
                                }
                                BasicAccountEventType::BorrowInterest => {
                                    if let Ok(msg) = BasicBorrowInterestMsg::from_bytes(data) {
                                        if msg.symbol.eq_ignore_ascii_case("USDT") {
                                            if let Some(mgr) = usdt_mgrs.get(&account_scope) {
                                                mgr.borrow_mut().apply_borrow_interest(&msg);
                                            }
                                        }
                                        if scope_matches_venue(
                                            account_scope,
                                            exchange,
                                            open_venue,
                                            binance_account_mode,
                                        ) {
                                            if let LegMgr::Margin { bal, .. } = &open_leg {
                                                bal.borrow_mut().apply_borrow_interest(&msg);
                                            }
                                        }
                                        if scope_matches_venue(
                                            account_scope,
                                            exchange,
                                            hedge_venue,
                                            binance_account_mode,
                                        ) {
                                            if let LegMgr::Margin { bal, .. } = &hedge_leg {
                                                bal.borrow_mut().apply_borrow_interest(&msg);
                                            }
                                        }
                                    }
                                }
                                BasicAccountEventType::OrderUpdate => match exchange {
                                    Exchange::Okex => {
                                        if let Ok(msg) = OkexOrderMsg::from_bytes(data) {
                                            dispatch_order_update_generic(&strategy_mgr, &msg);
                                        }
                                    }
                                    Exchange::Binance => {
                                        if let Ok(msg) = BinanceBasicOrderMsg::from_bytes(data) {
                                            dispatch_order_update_generic(&strategy_mgr, &msg);
                                        }
                                    }
                                    Exchange::Gate => {
                                        if let Ok(msg) = GateBasicOrderMsg::from_bytes(data) {
                                            dispatch_order_update_generic(&strategy_mgr, &msg);
                                        }
                                    }
                                    Exchange::Bitget => {
                                        if let Ok(msg) = BitgetBasicOrderMsg::from_bytes(data) {
                                            dispatch_order_update_generic(&strategy_mgr, &msg);
                                        }
                                    }
                                    Exchange::Bybit => {
                                        if let Ok(msg) = BybitBasicOrderMsg::from_bytes(data) {
                                            dispatch_order_update_generic(&strategy_mgr, &msg);
                                        }
                                    }
                                    _ => {}
                                },
                                BasicAccountEventType::TradeUpdateLite => {}
                                BasicAccountEventType::Error => {}
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
                            warn!("account stream receive error: {err}");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            }
            .await;

            if let Err(err) = result {
                warn!(
                    "account listener {} exited: {err:?}",
                    service_name_for_error
                );
            }
        });
    }
    // ==================== 风控方法（从 RiskChecker 迁移） ====================

    /// 检查当前 symbol 的限价挂单数量（MM 路径，使用 max_pending_limit_buy/sell_orders）
    pub fn check_pending_limit_order(&self, symbol: &str, side: Side) -> Result<(), String> {
        let params = PreTradeParamsLoader::instance();
        let side_limit = match side {
            Side::Buy => params.max_pending_limit_buy_orders(),
            Side::Sell => params.max_pending_limit_sell_orders(),
        };
        Self::check_pending_limit_order_with_side_limit(symbol, side, side_limit)
    }

    /// 检查当前 symbol 的限价挂单数量（套利路径，使用 arb_max_pending_limit_buy/sell_orders）
    pub fn check_pending_limit_order_for_arb(
        &self,
        symbol: &str,
        side: Side,
    ) -> Result<(), String> {
        let params = PreTradeParamsLoader::instance();
        let side_limit = match side {
            Side::Buy => params.arb_max_pending_limit_buy_orders(),
            Side::Sell => params.arb_max_pending_limit_sell_orders(),
        };
        Self::check_pending_limit_order_with_side_limit(symbol, side, side_limit)
    }

    fn check_pending_limit_order_with_side_limit(
        symbol: &str,
        side: Side,
        side_limit: i32,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            let params = PreTradeParamsLoader::instance();
            let max_pending_limit_orders = params.max_pending_limit_orders();

            let symbol_upper = symbol.to_uppercase();
            let order_manager = inner.order_manager.borrow();

            if max_pending_limit_orders > 0 {
                let count = order_manager.get_symbol_pending_limit_order_count(&symbol_upper);
                if count >= max_pending_limit_orders {
                    return Err(format!(
                        "symbol={} 当前限价挂单数={}，达到总上限 {}",
                        symbol, count, max_pending_limit_orders
                    ));
                }
            }

            if side_limit > 0 {
                let side_count =
                    order_manager.get_symbol_pending_limit_order_count_by_side(&symbol_upper, side);
                if side_count >= side_limit {
                    return Err(format!(
                        "symbol={} side={} 当前限价挂单数={}，达到方向上限 {}",
                        symbol,
                        side.as_str(),
                        side_count,
                        side_limit
                    ));
                }
            }

            Ok(())
        })
    }

    /// 检查当前symbol的敞口是否超过总资产比例限制
    pub fn check_symbol_exposure(&self, symbol: &str) -> Result<(), String> {
        Self::with_inner(|inner| {
            let loader = PreTradeParamsLoader::instance();
            let limit = loader.max_symbol_exposure_ratio();
            if limit <= 0.0 {
                return Ok(());
            }
            let max_pos_u = loader.max_pos_u_for_symbol(inner.open_venue, symbol);
            if max_pos_u <= f64::EPSILON {
                return Err("max_pos_u 配置无效，无法校验敞口比例".to_string());
            }

            let symbol_upper = symbol.to_uppercase();
            let Some(base_asset) = extract_base_asset(&symbol_upper) else {
                return Err(format!(
                    "无法识别 symbol={} 的基础资产，无法校验敞口比例",
                    symbol
                ));
            };

            let state = Self::compute_basic_state(inner);
            let net_exposure = state
                .exposures
                .get(&base_asset.to_uppercase())
                .map(|(open, hedge)| open + hedge)
                .unwrap_or(0.0);

            let price_mapper = create_symbol_mapper(Self::mark_price_exchange_for_venues(
                inner.open_venue,
                inner.hedge_venue,
            ));
            let mark = if base_asset.eq_ignore_ascii_case("USDT") {
                1.0
            } else {
                let sym = price_mapper.asset_to_price_symbol(&base_asset);
                let snap = inner.price_table.borrow().snapshot();
                snap.get(&sym).map(|e| e.mark_price).unwrap_or(0.0)
            };

            let exposure_usdt = if mark > 0.0 { net_exposure * mark } else { 0.0 };

            if mark == 0.0 && net_exposure != 0.0 {
                let ratio = net_exposure.abs() / max_pos_u;
                if ratio > limit {
                    debug!(
                        "资产 {} 敞口占比(数量) {:.4}% 超过阈值 {:.2}% (敞口qty={:.6}, max_pos_u={:.6})",
                        base_asset,
                        ratio * 100.0,
                        limit * 100.0,
                        net_exposure,
                        max_pos_u
                    );
                    return Err(format!("symbol={} 敞口比例超过限制 {}", symbol, limit));
                }
                return Ok(());
            }

            let ratio = exposure_usdt.abs() / max_pos_u;
            if ratio > limit {
                debug!(
                    "资产 {} 敞口占比 {:.4}% 超过阈值 {:.2}% (敞口USDT={:.6}, max_pos_u={:.6})",
                    base_asset,
                    ratio * 100.0,
                    limit * 100.0,
                    exposure_usdt,
                    max_pos_u
                );
                return Err(format!("symbol={} 敞口比例超过限制 {}", symbol, limit));
            }

            Ok(())
        })
    }

    /// 检查总敞口是否超过配置阈值（分母为 eq，若涉及合约 venue 则含 UPL）
    pub fn check_total_exposure(&self) -> Result<(), String> {
        Self::with_inner(|inner| {
            let limit = PreTradeParamsLoader::instance().max_total_exposure_ratio();
            if limit <= 0.0 {
                return Ok(());
            }

            let state = Self::compute_basic_state(inner);
            let total_equity = state.total_equity_usdt;
            let abs_total_usdt = state.abs_total_exposure_usdt;

            if total_equity <= f64::EPSILON {
                return Err("账户总权益(eq，含UPL如有合约)近似为 0，无法计算总敞口占比".to_string());
            }

            let ratio = abs_total_usdt / total_equity;
            if ratio > limit {
                debug!(
                    "总敞口占比 {:.4}% 超过阈值 {:.2}% (总敞口USDT={:.6}, 权益eq={:.6})",
                    ratio * 100.0,
                    limit * 100.0,
                    abs_total_usdt,
                    total_equity
                );
                return Err(format!(
                    "总敞口比例 {:.2}% 超过限制 {:.2}%",
                    ratio * 100.0,
                    limit * 100.0
                ));
            }

            Ok(())
        })
    }

    /// 检查最大持仓限制
    pub fn ensure_max_pos_u(
        &self,
        symbol: &str,
        additional_qty: f64,
        price_hint: f64,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            let max_pos_u =
                PreTradeParamsLoader::instance().max_pos_u_for_symbol(inner.open_venue, symbol);
            if !(max_pos_u > 0.0) {
                panic!("max_pos_u not set!!");
            }

            let open_venue = inner.open_venue;
            let symbol_upper = symbol.to_uppercase();
            let base_asset = extract_base_asset(&symbol_upper).ok_or_else(|| {
                format!("无法识别 symbol={} 的基础资产，无法校验 max_pos_u", symbol)
            })?;

            let state = Self::compute_basic_state(inner);
            // 只取 open 腿的持仓量，而非整体敞口 (open + hedge)
            let current_open_qty = state
                .exposures
                .get(&base_asset.to_uppercase())
                .map(|(open, _hedge)| *open)
                .unwrap_or(0.0);

            let base_upper = base_asset.to_uppercase();
            let price_mapper = create_symbol_mapper(Self::mark_price_exchange_for_venues(
                inner.open_venue,
                inner.hedge_venue,
            ));
            let mark_symbol = price_mapper.asset_to_price_symbol(&base_upper);
            let price_from_table = {
                let table = inner.price_table.borrow();
                table.mark_price(&mark_symbol)
            };
            let price = price_from_table.or({
                if price_hint > 0.0 {
                    Some(price_hint)
                } else {
                    None
                }
            });

            let Some(price) = price else {
                warn!("symbol={} 缺少 USDT 标记价格，无法校验 max_pos_u", symbol);
                return Err(format!(
                    "symbol={} 缺少价格信息，无法校验 max_pos_u",
                    symbol
                ));
            };

            let price_source = if price_from_table.is_some() {
                "mark_price_table"
            } else {
                "price_hint"
            };
            let (qty_unit, fut_symbol_key, qty_multiplier) = match open_venue {
                TradingVenue::BinanceFutures => {
                    ("contracts(mult=1)", Some(symbol_upper.clone()), Some(1.0))
                }
                TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                    let symbol_key = min_qty_symbol_key(open_venue, &symbol_upper);
                    let mult = inner
                        .venue_min_qty_tables
                        .get(&open_venue)
                        .and_then(|t| t.contract_multiplier_opt(&symbol_key));
                    ("contracts", Some(symbol_key), mult)
                }
                _ => ("base_qty", None, None),
            };

            let add_base_qty = match Self::order_qty_to_base_checked(
                inner,
                open_venue,
                symbol,
                additional_qty,
            ) {
                Ok(v) => v,
                Err(e) => {
                    info!(
                            "max_pos_u check qty convert failed: symbol={} base_asset={} venue={:?} qty_unit={} raw_qty={:.8} fut_symbol_key={:?} qty_multiplier={:?} err={}",
                            symbol,
                            base_asset,
                            open_venue,
                            qty_unit,
                            additional_qty,
                            fut_symbol_key,
                            qty_multiplier,
                            e
                        );
                    return Err(e);
                }
            };
            let projected_qty = current_open_qty + add_base_qty;
            let current_usdt = current_open_qty.abs() * price;
            let order_usdt = add_base_qty.abs() * price;
            let projected_usdt = projected_qty.abs() * price;
            let limit_eps = 1e-6_f64;

            if projected_usdt <= current_usdt + limit_eps {
                return Ok(());
            }

            if projected_usdt > max_pos_u + limit_eps {
                info!(
                    "max_pos_u check reject detail: symbol={} base_asset={} venue={:?} price_source={} mark_symbol={} price={:.8} qty_unit={} raw_qty={:.8} fut_symbol_key={:?} qty_multiplier={:?} current_open_qty(base)={:.8} add_base_qty={:.8} projected_qty(base)={:.8} current_usdt={:.4} order_usdt={:.4} projected_usdt={:.4} max_pos_u={:.4}",
                    symbol,
                    base_asset,
                    open_venue,
                    price_source,
                    mark_symbol,
                    price,
                    qty_unit,
                    additional_qty,
                    fut_symbol_key,
                    qty_multiplier,
                    current_open_qty,
                    add_base_qty,
                    projected_qty,
                    current_usdt,
                    order_usdt,
                    projected_usdt,
                    max_pos_u
                );
                warn!(
                    "symbol={} 当前持仓={:.6}({:.4}USDT) 下单数量={:.6}({:.4}USDT) 预计持仓={:.4}USDT 超过阈值 {:.4}USDT",
                    symbol,
                    current_open_qty,
                    current_usdt,
                    add_base_qty,
                    order_usdt,
                    projected_usdt,
                    max_pos_u
                );
                return Err(format!(
                    "symbol={} 预计持仓 {:.4}USDT 超过阈值 {:.4}USDT",
                    symbol, projected_usdt, max_pos_u
                ));
            }

            Ok(())
        })
    }

    /// 获取指定交易对和交易场所的持仓数量（带符号）
    /// 返回持仓数量，正数表示多头，负数表示空头
    pub fn get_position_qty(&self, symbol: &str, venue: TradingVenue) -> f64 {
        Self::with_inner(|inner| {
            let leg = if venue == inner.open_venue {
                &inner.open_leg
            } else if venue == inner.hedge_venue {
                &inner.hedge_leg
            } else {
                return 0.0;
            };

            match leg {
                LegMgr::Margin { bal, .. } => {
                    let symbol_upper = symbol.to_uppercase();
                    let Some(base_asset) = extract_base_asset(&symbol_upper) else {
                        return 0.0;
                    };
                    bal.borrow().net_position(&base_asset, None)
                }
                LegMgr::Futures {
                    um, min_qty_table, ..
                } => {
                    let table_ref = min_qty_table.borrow();
                    um.borrow().net_position(symbol, Some(&table_ref))
                }
            }
        })
    }

    // ==================== 内部辅助方法 ====================

    fn spawn_derivatives_listener(
        price_table: Rc<RefCell<PriceTable>>,
        node_name: String,
        service_name: String,
    ) {
        tokio::task::spawn_local(async move {
            let result: Result<()> = async move {
                let print_each_mark_price =
                    std::env::var_os("PRE_TRADE_PRINT_EACH_MARKPRICE").is_some();
                let mark_price_log_interval = std::env::var("PRE_TRADE_MARKPRICE_LOG_INTERVAL_SECS")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|secs| Duration::from_secs(secs.max(1)))
                    .unwrap_or_else(|| Duration::from_secs(5));
                let mut last_mark_price_log_at = Instant::now();
                let mut mark_price_samples_since_log: u64 = 0;
                let mut last_mark_price: Option<(String, f64, i64)> = None;

                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;

                let mut subscriber: Option<Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()>> =
                    None;

                loop {
                    if subscriber.is_none() {
                        let service = match node
                            .service_builder(&ServiceName::new(&service_name)?)
                            .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
                            .max_publishers(1)
                            .max_subscribers(DERIVATIVES_MAX_SUBSCRIBERS)
                            .history_size(DERIVATIVES_HISTORY_SIZE)
                            .subscriber_max_buffer_size(DERIVATIVES_SUBSCRIBER_MAX_BUFFER)
                            .open()
                        {
                            Ok(service) => service,
                            Err(err) => {
                                warn!(
                                    "waiting for derivatives service: node={} service={} err={:?}",
                                    node_name, service_name, err
                                );
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue;
                            }
                        };
                        let created_subscriber = service.subscriber_builder().create()?;
                        info!(
                            "derivatives price stream subscribed: node={} service={}",
                            node_name, service_name
                        );
                        subscriber = Some(created_subscriber);
                    }

                    match subscriber
                        .as_ref()
                        .expect("subscriber must exist after successful service open")
                        .receive()
                    {
                        Ok(Some(sample)) => {
                            let payload = Bytes::copy_from_slice(sample.payload());
                            if payload.is_empty() {
                                continue;
                            }
                            let Some(msg_type) = get_msg_type(&payload) else {
                                continue;
                            };
                            match msg_type {
                                MktMsgType::MarkPrice => match parse_mark_price(&payload) {
                                    Ok(msg) => {
                                        mark_price_samples_since_log += 1;
                                        let is_first_mark_price = last_mark_price.is_none();
                                        last_mark_price = Some((
                                            msg.symbol.clone(),
                                            msg.mark_price,
                                            msg.timestamp,
                                        ));
                                        if print_each_mark_price {
                                            info!(
                                                "mark price received: symbol={} mark_price={} ts={}",
                                                msg.symbol, msg.mark_price, msg.timestamp
                                            );
                                        } else if is_first_mark_price {
                                            let (symbol, mark_price, ts) = last_mark_price
                                                .as_ref()
                                                .expect("last mark price set above");
                                            info!(
                                                "mark price stream live: samples={} last_symbol={} last_mark_price={} last_ts={}",
                                                mark_price_samples_since_log,
                                                symbol,
                                                mark_price,
                                                ts
                                            );
                                            mark_price_samples_since_log = 0;
                                            last_mark_price_log_at = Instant::now();
                                        } else if last_mark_price_log_at.elapsed()
                                            >= mark_price_log_interval
                                        {
                                            let (symbol, mark_price, ts) = last_mark_price
                                                .as_ref()
                                                .expect("last mark price set above");
                                            debug!(
                                                "mark price stream live: samples={} last_symbol={} last_mark_price={} last_ts={}",
                                                mark_price_samples_since_log,
                                                symbol,
                                                mark_price,
                                                ts
                                            );
                                            mark_price_samples_since_log = 0;
                                            last_mark_price_log_at = Instant::now();
                                        }

                                        let mut table = price_table.borrow_mut();
                                        table.update_mark_price(
                                            &msg.symbol,
                                            msg.mark_price,
                                            msg.timestamp,
                                        );
                                    }
                                    Err(err) => warn!("parse mark price failed: {err:?}"),
                                },
                                MktMsgType::IndexPrice => match parse_index_price(&payload) {
                                    Ok(msg) => {
                                        let mut table = price_table.borrow_mut();
                                        table.update_index_price(
                                            &msg.symbol,
                                            msg.index_price,
                                            msg.timestamp,
                                        );
                                    }
                                    Err(err) => warn!("parse index price failed: {err:?}"),
                                },
                                _ => {}
                            }
                        }
                        Ok(None) => {
                            tokio::task::yield_now().await;
                        }
                        Err(err) => {
                            warn!(
                                "derivatives stream receive error, reconnecting: node={} service={} err={}",
                                node_name, service_name, err
                            );
                            subscriber = None;
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            }
            .await;

            if let Err(err) = result {
                log::error!("derivatives listener exited: {err:?}");
            }
        });
    }
}

// ==================== Helper Functions ====================

/// 通用订单/成交回报分发：适用于实现了 OrderUpdate + TradeUpdate 的消息
struct NormalizedUpdate<'a, T> {
    inner: &'a T,
    symbol: String,
}

impl<'a, T> NormalizedUpdate<'a, T>
where
    T: OrderUpdate + TradeUpdate,
{
    fn new(inner: &'a T) -> Self {
        Self {
            inner,
            symbol: normalize_symbol_for_internal(OrderUpdate::symbol(inner)),
        }
    }
}

impl<T> OrderUpdate for NormalizedUpdate<'_, T>
where
    T: OrderUpdate + TradeUpdate,
{
    fn event_time(&self) -> i64 {
        OrderUpdate::event_time(self.inner)
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn order_id(&self) -> i64 {
        OrderUpdate::order_id(self.inner)
    }

    fn client_order_id(&self) -> i64 {
        OrderUpdate::client_order_id(self.inner)
    }

    fn side(&self) -> crate::pre_trade::order_manager::Side {
        OrderUpdate::side(self.inner)
    }

    fn order_type(&self) -> crate::pre_trade::order_manager::OrderType {
        OrderUpdate::order_type(self.inner)
    }

    fn time_in_force(&self) -> crate::signal::common::TimeInForce {
        OrderUpdate::time_in_force(self.inner)
    }

    fn price(&self) -> f64 {
        OrderUpdate::price(self.inner)
    }

    fn quantity(&self) -> f64 {
        OrderUpdate::quantity(self.inner)
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        OrderUpdate::cumulative_filled_quantity(self.inner)
    }

    fn status(&self) -> OrderStatus {
        OrderUpdate::status(self.inner)
    }

    fn raw_status(&self) -> &str {
        OrderUpdate::raw_status(self.inner)
    }

    fn execution_type(&self) -> ExecutionType {
        OrderUpdate::execution_type(self.inner)
    }

    fn raw_execution_type(&self) -> &str {
        OrderUpdate::raw_execution_type(self.inner)
    }

    fn trading_venue(&self) -> TradingVenue {
        OrderUpdate::trading_venue(self.inner)
    }

    fn client_order_id_str(&self) -> Option<&str> {
        OrderUpdate::client_order_id_str(self.inner)
    }
}

impl<T> TradeUpdate for NormalizedUpdate<'_, T>
where
    T: OrderUpdate + TradeUpdate,
{
    fn event_time(&self) -> i64 {
        TradeUpdate::event_time(self.inner)
    }

    fn trade_time(&self) -> i64 {
        TradeUpdate::trade_time(self.inner)
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn order_id(&self) -> i64 {
        TradeUpdate::order_id(self.inner)
    }

    fn client_order_id(&self) -> i64 {
        TradeUpdate::client_order_id(self.inner)
    }

    fn side(&self) -> crate::pre_trade::order_manager::Side {
        TradeUpdate::side(self.inner)
    }

    fn price(&self) -> f64 {
        TradeUpdate::price(self.inner)
    }

    fn is_maker(&self) -> bool {
        TradeUpdate::is_maker(self.inner)
    }

    fn trading_venue(&self) -> TradingVenue {
        TradeUpdate::trading_venue(self.inner)
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        TradeUpdate::cumulative_filled_quantity(self.inner)
    }

    fn order_status(&self) -> Option<OrderStatus> {
        TradeUpdate::order_status(self.inner)
    }
}

fn dispatch_order_update_generic<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    update: &T,
) where
    T: OrderUpdate + TradeUpdate,
{
    let normalized_update = NormalizedUpdate::new(update);

    if normalized_update.execution_type() == ExecutionType::Trade {
        MonitorChannel::instance().bump_trade_update_seq();
    }

    let order_id = OrderUpdate::client_order_id(&normalized_update);
    let strategy_ids: Vec<i32> = strategy_mgr.borrow().iter_ids().cloned().collect();
    let mut matched = false;

    for strategy_id in strategy_ids {
        let strategy_opt = {
            let mut mgr = strategy_mgr.borrow_mut();
            mgr.take(strategy_id)
        };

        if let Some(mut strategy) = strategy_opt {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                match normalized_update.execution_type() {
                    ExecutionType::New | ExecutionType::Canceled => {
                        strategy.apply_order_update(&normalized_update);
                    }
                    ExecutionType::Trade => {
                        strategy.apply_trade_update(&normalized_update);
                    }
                    ExecutionType::Expired | ExecutionType::Rejected => {
                        warn!(
                            "Unexpected execution type: {:?}, sym={} cli_id={} ord_id={}",
                            normalized_update.execution_type(),
                            OrderUpdate::symbol(&normalized_update),
                            OrderUpdate::client_order_id(&normalized_update),
                            OrderUpdate::order_id(&normalized_update)
                        );
                        strategy.apply_order_update(&normalized_update);
                    }
                    _ => {
                        log::error!(
                            "Unhandled execution type: {:?}, sym={} cli_id={} ord_id={}",
                            normalized_update.execution_type(),
                            OrderUpdate::symbol(&normalized_update),
                            OrderUpdate::client_order_id(&normalized_update),
                            OrderUpdate::order_id(&normalized_update)
                        );
                    }
                }
            }
            if strategy.is_active() {
                strategy_mgr.borrow_mut().insert(strategy);
            }
        }
    }

    if !matched {
        let orphan_strategy_mgr = MonitorChannel::instance().orphan_strategy_mgr();
        let adopted_by_orphan = if normalized_update.execution_type() == ExecutionType::Trade {
            orphan_strategy_mgr
                .borrow_mut()
                .apply_trade_update(&normalized_update)
        } else {
            orphan_strategy_mgr
                .borrow_mut()
                .apply_order_update(&normalized_update)
        };

        if !adopted_by_orphan {
            PersistChannel::with(|ch| {
                if normalized_update.execution_type() == ExecutionType::Trade {
                    ch.publish_trade_update_unmatched(&normalized_update);
                } else {
                    ch.publish_order_update_unmatched(&normalized_update);
                }
            });
        }
        debug!(
            "order update unmatched: sym={} cli_id={} ord_id={} x={:?} X={:?} orphan_adopted={}",
            OrderUpdate::symbol(&normalized_update),
            OrderUpdate::client_order_id(&normalized_update),
            OrderUpdate::order_id(&normalized_update),
            normalized_update.execution_type(),
            normalized_update.status(),
            adopted_by_orphan
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::BasicPositionMsg;
    use crate::common::min_qty_table::MinQtyTable;
    use crate::common::tick_math::QuantizedValue;
    use crate::pre_trade::price_table::PriceTable;
    use crate::pre_trade::usdt_balance_manager::UsdtBalanceManager;
    use crate::signal::cancel_signal::MmCancelCtx;
    use crate::signal::common::SignalBytes;
    use crate::signal::trade_signal::{SignalType, TradeSignal};
    use crate::strategy::manager::OpenPriceMapEntry;
    use crate::strategy::{Strategy, StrategyManager};
    use std::any::Any;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    struct TestMmOpenStrategy {
        id: i32,
        symbol: String,
        side: Side,
        client_order_id: i64,
        cancel_trigger_count: usize,
        last_trigger_ts: i64,
        active: bool,
    }

    impl TestMmOpenStrategy {
        fn new(id: i32, symbol: &str, side: Side, client_order_id: i64) -> Self {
            Self {
                id,
                symbol: normalize_symbol_for_internal(symbol),
                side,
                client_order_id,
                cancel_trigger_count: 0,
                last_trigger_ts: 0,
                active: true,
            }
        }
    }

    impl Strategy for TestMmOpenStrategy {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn get_id(&self) -> i32 {
            self.id
        }

        fn is_strategy_order(&self, order_id: i64) -> bool {
            order_id == self.client_order_id
        }

        fn handle_signal(&mut self, signal: &TradeSignal) {
            if signal.signal_type.clone() as u32 != SignalType::MMCancel as u32 {
                return;
            }
            let ctx = MmCancelCtx::from_bytes(signal.context.clone()).expect("mm cancel ctx");
            self.cancel_trigger_count += 1;
            self.last_trigger_ts = ctx.trigger_ts;
        }

        fn apply_order_update(&mut self, _update: &dyn crate::strategy::order_update::OrderUpdate) {
        }

        fn apply_trade_update(&mut self, _trade: &dyn crate::strategy::trade_update::TradeUpdate) {}

        fn handle_period_clock(&mut self, _current_tp: i64) {}

        fn is_active(&self) -> bool {
            self.active
        }

        fn symbol(&self) -> Option<&str> {
            Some(&self.symbol)
        }

        fn mm_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
            Some(OpenPriceMapEntry {
                symbol: self.symbol.clone(),
                side: self.side,
                client_order_id: self.client_order_id,
                price_qv: QuantizedValue::from_parts(1, 0, 1).into(),
            })
        }
    }

    #[test]
    fn okex_futures_qty_to_base_uses_contract_multiplier() {
        let mut okx_table = VenueMinQtyTable::new(TradingVenue::OkexFutures);
        okx_table.set_contract_multiplier_for_test("FILUSDT", 0.1);

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Okex,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Okex))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Okex))),
        };
        let hedge_leg = LegMgr::Margin {
            exchange: Exchange::Binance,
            bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance))),
        };

        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        venue_min_qty_tables.insert(TradingVenue::OkexFutures, Rc::new(okx_table));

        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        usdt_mgrs.insert(
            BasicAccountScope::OkexUnified,
            Rc::new(RefCell::new(UsdtBalanceManager::new(Exchange::Okex))),
        );

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::OkexFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table: Rc::new(RefCell::new(PriceTable::new())),
            venue_min_qty_tables,
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            trade_update_seq: 0,
        };

        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

        let base_qty =
            MonitorChannel::instance().qty_to_base(TradingVenue::OkexFutures, "FIL-USDT-SWAP", 1.0);
        assert!((base_qty - 0.1).abs() < 1e-12);
        let overhedge_factor = 1.0 / base_qty;
        assert!((overhedge_factor - 10.0).abs() < 1e-12);
    }

    #[test]
    fn ensure_max_pos_u_rejects_when_okex_multiplier_missing() {
        let okx_table = VenueMinQtyTable::new(TradingVenue::OkexFutures);

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Okex,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Okex))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Okex))),
        };
        let hedge_leg = LegMgr::Margin {
            exchange: Exchange::Binance,
            bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance))),
        };

        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        venue_min_qty_tables.insert(TradingVenue::OkexFutures, Rc::new(okx_table));

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::OkexFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables,
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            trade_update_seq: 0,
        };

        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

        let err = MonitorChannel::instance()
            .ensure_max_pos_u("FIL-USDT-SWAP", 2.0, 100.0)
            .unwrap_err();
        assert!(err.contains("缺少 OkexFutures 合约乘数"), "err={err}");
    }

    #[test]
    fn ensure_max_pos_u_uses_okex_multiplier_in_risk_calc() {
        let mut okx_table = VenueMinQtyTable::new(TradingVenue::OkexFutures);
        okx_table.set_contract_multiplier_for_test("FILUSDT", 10.0);

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Okex,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Okex))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Okex))),
        };
        let hedge_leg = LegMgr::Margin {
            exchange: Exchange::Binance,
            bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance))),
        };

        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        venue_min_qty_tables.insert(TradingVenue::OkexFutures, Rc::new(okx_table));

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::OkexFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables,
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            trade_update_seq: 0,
        };

        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

        // max_pos_u default = 1000.0 (PreTradeParamsLoader::default)
        // FIL mark = 100.0, contracts=2, mult=10 => base=20 => notional=2000 > 1000
        assert!(MonitorChannel::instance()
            .ensure_max_pos_u("FIL-USDT-SWAP", 2.0, 100.0)
            .is_err());
    }

    #[test]
    fn ensure_max_pos_u_allows_reducing_when_over_limit() {
        let mut um_mgr = BasicUmManager::new(Exchange::Binance);
        let pos_msg = BasicPositionMsg::create(0, "FILUSDT".to_string(), 'L', 20.0);
        um_mgr.apply_position(&pos_msg);

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(um_mgr)),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };
        let hedge_leg = LegMgr::Margin {
            exchange: Exchange::Binance,
            bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            trade_update_seq: 0,
        };

        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

        // 当前持仓 20 * 100 = 2000 > max_pos_u(1000)，但减少仓位应放行。
        assert!(MonitorChannel::instance()
            .ensure_max_pos_u("FILUSDT", -5.0, 100.0)
            .is_ok());
    }

    #[test]
    fn mm_position_risk_cancel_targets_open_strategies_by_side() {
        let mut um_mgr = BasicUmManager::new(Exchange::Binance);
        um_mgr.apply_position(&BasicPositionMsg::create(
            0,
            "FILUSDT".to_string(),
            'L',
            20.0,
        ));

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(um_mgr)),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Binance))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let strategy_mgr = Rc::new(RefCell::new(StrategyManager::new()));
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                101,
                "FILUSDT",
                Side::Buy,
                101_0001,
            )));
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                102,
                "FILUSDT",
                Side::Sell,
                102_0001,
            )));
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                103,
                "BTCUSDT",
                Side::Buy,
                103_0001,
            )));

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: strategy_mgr.clone(),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            trade_update_seq: 0,
        };

        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

        MonitorChannel::instance().handle_mm_position_risk_after_update("FILUSDT");

        let mut mgr = strategy_mgr.borrow_mut();
        let buy = mgr
            .take(101)
            .expect("buy strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("buy strategy type")
            .cancel_trigger_count;
        let sell = mgr
            .take(102)
            .expect("sell strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("sell strategy type")
            .cancel_trigger_count;
        let other = mgr
            .take(103)
            .expect("other strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("other strategy type")
            .cancel_trigger_count;

        assert_eq!(buy, 1);
        assert_eq!(sell, 0);
        assert_eq!(other, 0);
    }

    #[test]
    fn mark_price_source_uses_okex_when_both_venues_are_okex() {
        assert_eq!(
            MonitorChannel::mark_price_exchange_for_venues(
                TradingVenue::OkexMargin,
                TradingVenue::OkexFutures,
            ),
            Exchange::Okex
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::OkexMargin,
                TradingVenue::OkexFutures,
            ),
            OKEX_DERIVATIVES_SERVICE
        );
    }

    #[test]
    fn mark_price_source_uses_bybit_when_both_venues_are_bybit() {
        assert_eq!(
            MonitorChannel::mark_price_exchange_for_venues(
                TradingVenue::BybitMargin,
                TradingVenue::BybitFutures,
            ),
            Exchange::Bybit
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::BybitMargin,
                TradingVenue::BybitFutures,
            ),
            BYBIT_DERIVATIVES_SERVICE
        );
    }

    #[test]
    fn mark_price_source_uses_gate_when_both_venues_are_gate() {
        assert_eq!(
            MonitorChannel::mark_price_exchange_for_venues(
                TradingVenue::GateMargin,
                TradingVenue::GateFutures,
            ),
            Exchange::Gate
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::GateMargin,
                TradingVenue::GateFutures,
            ),
            GATE_DERIVATIVES_SERVICE
        );
    }

    #[test]
    fn mark_price_source_uses_bitget_when_both_venues_are_bitget() {
        assert_eq!(
            MonitorChannel::mark_price_exchange_for_venues(
                TradingVenue::BitgetMargin,
                TradingVenue::BitgetFutures,
            ),
            Exchange::Bitget
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::BitgetMargin,
                TradingVenue::BitgetFutures,
            ),
            BITGET_DERIVATIVES_SERVICE
        );
    }
}
