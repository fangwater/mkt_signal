use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::Duration;

use crate::common::basic_account_msg::{
    get_basic_event_type, BasicAccountEventType, BasicBalanceMsg, BasicBorrowInterestMsg,
    BasicPositionMsg, BinanceBasicOrderMsg, OkexOrderMsg,
};
use crate::common::exchange::Exchange;
use crate::common::ipc_service_name::build_service_name;
use crate::common::min_qty_table::MinQtyTable;
use crate::portfolio_margin::pm_forwarder::{PM_HISTORY_SIZE, PM_MAX_SUBSCRIBERS};
use crate::pre_trade::basic_balance_manager::BasicBalanceManager;
use crate::pre_trade::basic_um_manager::BasicUmManager;
use crate::pre_trade::net_position::NetPosition;
use crate::pre_trade::price_table::{PriceEntry, PriceTable};
use crate::pre_trade::symbol_util::extract_base_asset;
use crate::signal::common::{ExecutionType, TradingVenue};

const ACCOUNT_PAYLOAD: usize = 16_384;
const DERIVATIVES_PAYLOAD: usize = 128;
const DERIVATIVES_SERVICE: &str = "data_pubs/binance-futures/derivatives";
const NODE_PRE_TRADE_DERIVATIVES: &str = "pre_trade_derivatives";

// ==================== Helper Functions ====================

fn is_margin_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceMargin | TradingVenue::OkexMargin
    )
}

fn is_futures_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures | TradingVenue::OkexFutures
    )
}

fn exchange_from_venue(venue: TradingVenue) -> Exchange {
    match venue {
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => Exchange::Binance,
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => Exchange::Okex,
        _ => panic!("unsupported venue for pre_trade: {:?}", venue),
    }
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

use crate::common::msg_parser::{get_msg_type, parse_index_price, parse_mark_price, MktMsgType};
use crate::pre_trade::order_manager::OrderManager;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::signal::common::{align_price_ceil, align_price_floor};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use bytes::Bytes;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::thread::AccessError;

// Thread-local 单例存储
thread_local! {
    static MONITOR_CHANNEL: RefCell<Option<MonitorChannelInner>> = RefCell::new(None);
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
    /// U 本位合约腿，sz=张数（OKX）或标的数量（Binance）
    Futures {
        exchange: Exchange,
        um: Rc<RefCell<BasicUmManager>>,
        min_qty_table: Rc<RefCell<MinQtyTable>>,
    },
}

impl LegMgr {
    fn exchange(&self) -> Exchange {
        match self {
            LegMgr::Margin { exchange, .. } | LegMgr::Futures { exchange, .. } => *exchange,
        }
    }

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
    /// 价格表（仍使用 Binance mark/index 价格作为统一估值源）
    price_table: Rc<RefCell<PriceTable>>,
    /// 各交易场所的最小下单量/步进信息
    venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>>,
    /// 策略管理器
    strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    /// 订单管理器，所有订单维护在其中，完全成交或者撤单会被移除
    order_manager: Rc<RefCell<OrderManager>>,
    /// 对冲残余量哈希表，key=(symbol, venue)，value=残余量
    hedge_residual_map: Rc<RefCell<HashMap<(String, TradingVenue), f64>>>,
    /// Monotonic counter incremented when a TradeUpdate is received.
    trade_update_seq: u64,
}

struct BasicState {
    // asset -> (open_qty, hedge_qty), both in base units
    exposures: HashMap<String, (f64, f64)>,
    total_equity_usdt: f64,
    abs_total_exposure_usdt: f64,
    total_position_usdt: f64,
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

    fn try_with_inner<F, R>(f: F) -> Result<R, AccessError>
    where
        F: FnOnce(&MonitorChannelInner) -> R,
    {
        MONITOR_CHANNEL.try_with(|mc| {
            let mc_ref = mc.borrow();
            let inner = mc_ref.as_ref().expect("MonitorChannel not initialized");
            f(inner)
        })
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

    /// 获取 order_manager 的引用
    pub fn order_manager(&self) -> Rc<RefCell<OrderManager>> {
        Self::with_inner(|inner| inner.order_manager.clone())
    }

    pub fn try_order_manager() -> Option<Rc<RefCell<OrderManager>>> {
        Self::try_with_inner(|inner| inner.order_manager.clone()).ok()
    }

    /// 获取 price_table 的引用
    pub fn price_table(&self) -> Rc<RefCell<PriceTable>> {
        Self::with_inner(|inner| inner.price_table.clone())
    }

    pub fn open_venue(&self) -> TradingVenue {
        Self::with_inner(|inner| inner.open_venue)
    }

    pub fn hedge_venue(&self) -> TradingVenue {
        Self::with_inner(|inner| inner.hedge_venue)
    }

    /// 获取当前基础风控口径的快照（用于 resample/viz）
    ///
    /// 返回：
    /// - `exposures`: asset -> (open_qty, hedge_qty)，都按标的数量（base qty）表达
    /// - `total_equity_usdt`: 以现货净头寸估算的 USDT 权益（与风控口径一致）
    /// - `abs_total_exposure_usdt`: 各资产净敞口按 USDT 估值后取绝对值求和
    /// - `total_position_usdt`: 各资产现货/合约头寸按 USDT 估值后取绝对值求和
    pub fn basic_state_snapshot(&self) -> (HashMap<String, (f64, f64)>, f64, f64, f64) {
        Self::with_inner(|inner| {
            let state = Self::compute_basic_state(inner);
            (
                state.exposures,
                state.total_equity_usdt,
                state.abs_total_exposure_usdt,
                state.total_position_usdt,
            )
        })
    }

    /// 获取 strategy_mgr 的引用
    pub fn strategy_mgr(&self) -> Rc<RefCell<crate::strategy::StrategyManager>> {
        Self::with_inner(|inner| inner.strategy_mgr.clone())
    }

    pub fn try_strategy_mgr() -> Option<Rc<RefCell<crate::strategy::StrategyManager>>> {
        Self::try_with_inner(|inner| inner.strategy_mgr.clone()).ok()
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
    ) -> Result<()> {
        // 仅支持 Binance / OKX
        for v in [open_venue, hedge_venue] {
            if !matches!(
                v,
                TradingVenue::BinanceMargin
                    | TradingVenue::BinanceFutures
                    | TradingVenue::OkexMargin
                    | TradingVenue::OkexFutures
            ) {
                panic!("pre_trade does not support venue {:?}", v);
            }
        }

        let open_exchange = exchange_from_venue(open_venue);
        let hedge_exchange = exchange_from_venue(hedge_venue);

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

        // 创建价格表（使用 Binance premiumIndex 初始化为空即可）
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
                open_leg.clone(),
                hedge_leg.clone(),
                strategy_mgr.clone(),
            );
        }

        // 启动衍生品价格监听任务（mark_price, index_price）
        Self::spawn_derivatives_listener(price_table.clone());

        // 创建内部实例并保存到 thread-local
        let inner = MonitorChannelInner {
            open_venue,
            hedge_venue,
            open_leg,
            hedge_leg,
            price_table,
            venue_min_qty_tables,
            strategy_mgr,
            order_manager: Rc::new(RefCell::new(OrderManager::new())),
            hedge_residual_map: Rc::new(RefCell::new(HashMap::new())),
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
            TradingVenue::OkexFutures => {
                let mult = inner
                    .venue_min_qty_tables
                    .get(&venue)
                    .map(|t| t.contract_multiplier(symbol))
                    .unwrap_or(1.0);
                qty * mult
            }
            _ => qty,
        }
    }

    /// 基于 open/hedge 两腿的基础管理器计算敞口与总量指标
    fn compute_basic_state(inner: &MonitorChannelInner) -> BasicState {
        let price_snap = inner.price_table.borrow().snapshot();

        fn collect_leg_positions(leg: &LegMgr) -> HashMap<String, f64> {
            let mut positions: HashMap<String, f64> = HashMap::new();
            match leg {
                LegMgr::Margin { bal, .. } => {
                    let mgr = bal.borrow();
                    for entry in mgr.snapshot() {
                        let asset = entry.symbol.to_uppercase();
                        let qty = mgr.balance_position_of(&asset);
                        if qty.abs() <= 1e-12 {
                            continue;
                        }
                        *positions.entry(asset).or_insert(0.0) += qty;
                    }
                }
                LegMgr::Futures {
                    um, min_qty_table, ..
                } => {
                    let mgr = um.borrow();
                    let min_qty = min_qty_table.borrow();
                    for pos in mgr.snapshot() {
                        if (pos.amount as f64).abs() <= 1e-12 {
                            continue;
                        }
                        let mut symbol = pos.inst_id.to_uppercase();
                        if symbol.contains('-') {
                            symbol = symbol.replace("-SWAP", "").replace('-', "");
                        }
                        let asset = extract_base_asset(&symbol).unwrap_or_else(|| symbol.clone());
                        let mult = min_qty.contract_multiplier(&pos.inst_id);
                        let signed_base_qty = (pos.amount as f64) * mult;
                        if signed_base_qty.abs() <= 1e-12 {
                            continue;
                        }
                        *positions.entry(asset).or_insert(0.0) += signed_base_qty;
                    }
                }
            }
            positions
        }

        fn mark_price_usdt(
            price_snap: &std::collections::BTreeMap<String, PriceEntry>,
            asset: &str,
        ) -> f64 {
            if asset.eq_ignore_ascii_case("USDT") {
                1.0
            } else {
                price_snap
                    .get(&format!("{}USDT", asset))
                    .map(|p| p.mark_price)
                    .unwrap_or(0.0)
            }
        }

        let open_positions = collect_leg_positions(&inner.open_leg);
        let hedge_positions = collect_leg_positions(&inner.hedge_leg);

        let mut exposures: HashMap<String, (f64, f64)> = HashMap::new();
        for asset in open_positions.keys().chain(hedge_positions.keys()) {
            let asset = asset.to_uppercase();
            let open_qty = open_positions.get(&asset).copied().unwrap_or(0.0);
            let hedge_qty = hedge_positions.get(&asset).copied().unwrap_or(0.0);
            exposures.insert(asset, (open_qty, hedge_qty));
        }

        // total_equity 仍按“现货净头寸”估算：只从 margin balance 统计
        let mut total_equity_usdt: f64 = 0.0;
        for leg in [&inner.open_leg, &inner.hedge_leg] {
            if let LegMgr::Margin { bal, .. } = leg {
                let mgr = bal.borrow();
                for entry in mgr.snapshot() {
                    let asset = entry.symbol.to_uppercase();
                    let qty = mgr.balance_position_of(&asset);
                    if qty.abs() <= 1e-12 {
                        continue;
                    }
                    let mark = mark_price_usdt(&price_snap, &asset);
                    if mark > 0.0 {
                        total_equity_usdt += qty * mark;
                    }
                }
            }
        }

        let mut total_position_usdt = 0.0;
        let mut abs_total_exposure_usdt = 0.0;
        for (asset, (open_qty, hedge_qty)) in &exposures {
            if asset == "USDT" {
                continue;
            }
            let mark = mark_price_usdt(&price_snap, asset);
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
            let total_position = state.total_position_usdt;

            if total_equity <= f64::EPSILON {
                return Err("账户总权益近似为 0，无法计算杠杆率".to_string());
            }

            let leverage = total_position / total_equity;
            if leverage > limit {
                debug!(
                    "当前杠杆 {:.4} 超过阈值 {:.4} (仓位={:.6}, 权益={:.6})",
                    leverage, limit, total_position, total_equity
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
            let symbol_key = match venue {
                TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                    symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
                }
                _ => symbol.to_uppercase(),
            };

            let Some(table) = inner.venue_min_qty_tables.get(&venue) else {
                return Err(format!(
                    "未初始化 {:?} 的最小下单量表，请检查启动参数",
                    venue
                ));
            };

            match venue {
                TradingVenue::BinanceFutures => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    true,
                ),
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
                TradingVenue::OkexFutures => {
                    // OKX 永续/交割合约 sz 使用“张数”，需要用 ctVal×ctMult 将 base qty 转成合约张数
                    let contract_size = table.contract_multiplier(&symbol_key);
                    if contract_size <= 0.0 {
                        return Err(format!(
                            "symbol={} OKX contract multiplier invalid: {}",
                            symbol_key, contract_size
                        ));
                    }
                    let raw_contracts = raw_qty / contract_size;
                    debug!(
                        "OKX futures qty convert: symbol={} raw_base_qty={:.8} contract_size={:.8} -> raw_contracts={:.8}",
                        symbol_key, raw_qty, contract_size, raw_contracts
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
                TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
                    Err("尚未实现 Bybit 的订单对齐".to_string())
                }
                TradingVenue::GateMargin | TradingVenue::GateFutures => {
                    Err("尚未实现 Gate 的订单对齐".to_string())
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
            let symbol_key = match venue {
                TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                    symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
                }
                _ => symbol.to_uppercase(),
            };

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
        open_leg: LegMgr,
        hedge_leg: LegMgr,
        strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    ) {
        tokio::task::spawn_local(async move {
            let service_name_for_error = service_name.clone();

            let result: Result<()> = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;

                let service = node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
                    .max_publishers(1)
                    .max_subscribers(PM_MAX_SUBSCRIBERS)
                    .history_size(PM_HISTORY_SIZE)
                    .open_or_create()?;
                let subscriber: Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!(
                    "basic account stream subscribed: service={} exchange={:?}",
                    service_name, exchange
                );

                let mut dedup = DedupCache::new(8192);

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = sample.payload();
                            if payload.len() < 8 {
                                continue;
                            }

                            let msg_type = get_basic_event_type(payload);
                            if msg_type == BasicAccountEventType::Error {
                                continue;
                            }

                            let body_len = u32::from_le_bytes([
                                payload[4], payload[5], payload[6], payload[7],
                            ]) as usize;
                            let total = 8 + body_len;
                            if total > payload.len() {
                                continue;
                            }
                            let data = &payload[8..total];

                            let mut hasher = std::collections::hash_map::DefaultHasher::new();
                            payload[..total].hash(&mut hasher);
                            let key = hasher.finish();
                            if !dedup.insert_check(key) {
                                continue;
                            }

                            match msg_type {
                                BasicAccountEventType::BalanceUpdate => {
                                    if let Ok(msg) = BasicBalanceMsg::from_bytes(data) {
                                        if exchange == open_leg.exchange() {
                                            if let LegMgr::Margin { bal, .. } = &open_leg {
                                                bal.borrow_mut().apply_balance(&msg);
                                            }
                                        }
                                        if exchange == hedge_leg.exchange() {
                                            if let LegMgr::Margin { bal, .. } = &hedge_leg {
                                                bal.borrow_mut().apply_balance(&msg);
                                            }
                                        }
                                    }
                                }
                                BasicAccountEventType::PositionUpdate => {
                                    if let Ok(msg) = BasicPositionMsg::from_bytes(data) {
                                        if exchange == open_leg.exchange() {
                                            if let LegMgr::Futures { um, .. } = &open_leg {
                                                um.borrow_mut().apply_position(&msg);
                                            }
                                        }
                                        if exchange == hedge_leg.exchange() {
                                            if let LegMgr::Futures { um, .. } = &hedge_leg {
                                                um.borrow_mut().apply_position(&msg);
                                            }
                                        }
                                    }
                                }
                                BasicAccountEventType::BorrowInterest => {
                                    if let Ok(msg) = BasicBorrowInterestMsg::from_bytes(data) {
                                        if exchange == open_leg.exchange() {
                                            if let LegMgr::Margin { bal, .. } = &open_leg {
                                                bal.borrow_mut().apply_borrow_interest(&msg);
                                            }
                                        }
                                        if exchange == hedge_leg.exchange() {
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
                                    _ => {}
                                },
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

    /// 检查当前 symbol 的限价挂单数量
    pub fn check_pending_limit_order(&self, symbol: &str) -> Result<(), String> {
        Self::with_inner(|inner| {
            let max_pending_limit_orders =
                PreTradeParamsLoader::instance().max_pending_limit_orders();
            if max_pending_limit_orders <= 0 {
                return Ok(());
            }

            let symbol_upper = symbol.to_uppercase();
            let count = inner
                .order_manager
                .borrow()
                .get_symbol_pending_limit_order_count(&symbol_upper);

            if count >= max_pending_limit_orders {
                return Err(format!(
                    "symbol={} 当前限价挂单数={}，达到上限 {}",
                    symbol, count, max_pending_limit_orders
                ));
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
            let max_pos_u = loader.max_pos_u();
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

            let mark = if base_asset.eq_ignore_ascii_case("USDT") {
                1.0
            } else {
                let sym = format!("{}USDT", base_asset);
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

    /// 检查总敞口是否超过配置阈值
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
                return Err("账户总权益近似为 0，无法计算总敞口占比".to_string());
            }

            let ratio = abs_total_usdt / total_equity;
            if ratio > limit {
                debug!(
                    "总敞口占比 {:.4}% 超过阈值 {:.2}% (总敞口USDT={:.6}, 权益={:.6})",
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
            let max_pos_u = PreTradeParamsLoader::instance().max_pos_u();
            if !(max_pos_u > 0.0) {
                panic!("max_pos_u not set!!");
            }

            let symbol_upper = symbol.to_uppercase();
            let base_asset = extract_base_asset(&symbol_upper).ok_or_else(|| {
                format!("无法识别 symbol={} 的基础资产，无法校验 max_pos_u", symbol)
            })?;

            let state = Self::compute_basic_state(inner);
            let current_exposure_qty = state
                .exposures
                .get(&base_asset.to_uppercase())
                .map(|(open, hedge)| open + hedge)
                .unwrap_or(0.0);

            let base_upper = base_asset.to_uppercase();
            let mark_symbol = format!("{}USDT", base_upper);
            let price_from_table = {
                let table = inner.price_table.borrow();
                table.mark_price(&mark_symbol)
            };
            let price = price_from_table.or_else(|| {
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

            let add_base_qty =
                Self::order_qty_to_base(inner, inner.open_venue, symbol, additional_qty);
            let projected_qty = current_exposure_qty + add_base_qty;
            let current_usdt = current_exposure_qty.abs() * price;
            let order_usdt = add_base_qty.abs() * price;
            let projected_usdt = projected_qty.abs() * price;
            let limit_eps = 1e-6_f64;

            if projected_usdt > max_pos_u + limit_eps {
                warn!(
                    "symbol={} 当前敞口={:.6}({:.4}USDT) 下单数量={:.6}({:.4}USDT) 预计敞口={:.4}USDT 超过阈值 {:.4}USDT",
                    symbol,
                    current_exposure_qty,
                    current_usdt,
                    add_base_qty,
                    order_usdt,
                    projected_usdt,
                    max_pos_u
                );
                return Err(format!(
                    "symbol={} 预计敞口 {:.4}USDT 超过阈值 {:.4}USDT",
                    symbol, projected_usdt, max_pos_u
                ));
            }

            Ok(())
        })
    }

    // ==================== 对冲残余量管理方法 ====================

    /// 增加对冲残余量
    pub fn inc_hedge_residual(&self, symbol: String, venue: TradingVenue, delta: f64) {
        Self::with_inner(|inner| {
            if delta <= 1e-12 {
                return;
            }
            let mut map = inner.hedge_residual_map.borrow_mut();
            let key = (symbol.to_uppercase(), venue);
            let current = map.get(&key).copied().unwrap_or(0.0);
            let new_value = current + delta;
            map.insert(key.clone(), new_value);
            debug!(
                "对冲残余量增加: symbol={} venue={:?} delta={:.8} 当前值={:.8} -> {:.8}",
                key.0, venue, delta, current, new_value
            );
        })
    }

    /// 减少对冲残余量
    pub fn dec_hedge_residual(&self, symbol: String, venue: TradingVenue, delta: f64) -> f64 {
        Self::with_inner(|inner| {
            if delta <= 1e-12 {
                return 0.0;
            }
            let mut map = inner.hedge_residual_map.borrow_mut();
            let key = (symbol.to_uppercase(), venue);
            let current = map.get(&key).copied().unwrap_or(0.0);
            let actual_dec = delta.min(current);
            let new_value = (current - actual_dec).max(0.0);

            if new_value <= 1e-12 {
                map.remove(&key);
                debug!(
                    "对冲残余量清零: symbol={} venue={:?} 原值={:.8} 减少={:.8}",
                    key.0, venue, current, actual_dec
                );
            } else {
                map.insert(key.clone(), new_value);
                debug!(
                    "对冲残余量减少: symbol={} venue={:?} delta={:.8} 当前值={:.8} -> {:.8}",
                    key.0, venue, actual_dec, current, new_value
                );
            }

            actual_dec
        })
    }

    /// 查询对冲残余量
    pub fn get_hedge_residual(&self, symbol: &str, venue: TradingVenue) -> f64 {
        Self::with_inner(|inner| {
            let map = inner.hedge_residual_map.borrow();
            let key = (symbol.to_uppercase(), venue);
            map.get(&key).copied().unwrap_or(0.0)
        })
    }

    /// 清除指定 symbol 和 venue 的对冲残余量
    pub fn clear_hedge_residual(&self, symbol: &str, venue: TradingVenue) -> f64 {
        Self::with_inner(|inner| {
            let mut map = inner.hedge_residual_map.borrow_mut();
            let key = (symbol.to_uppercase(), venue);
            let removed = map.remove(&key).unwrap_or(0.0);
            if removed > 1e-12 {
                debug!(
                    "对冲残余量清除: symbol={} venue={:?} 清除量={:.8}",
                    key.0, venue, removed
                );
            }
            removed
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

    fn spawn_derivatives_listener(price_table: Rc<RefCell<PriceTable>>) {
        tokio::task::spawn_local(async move {
            let result: Result<()> = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(NODE_PRE_TRADE_DERIVATIVES)?)
                    .create::<ipc::Service>()?;

                let service = node
                    .service_builder(&ServiceName::new(DERIVATIVES_SERVICE)?)
                    .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
                    .open_or_create()?;
                let subscriber: Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!(
                    "derivatives price stream subscribed: service={}",
                    DERIVATIVES_SERVICE
                );

                loop {
                    match subscriber.receive() {
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
                            warn!("derivatives stream receive error: {err}");
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
fn dispatch_order_update_generic<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    update: &T,
) where
    T: OrderUpdate + TradeUpdate,
{
    if update.execution_type() == ExecutionType::Trade {
        MonitorChannel::instance().bump_trade_update_seq();
    }

    let order_id = OrderUpdate::client_order_id(update);
    let strategy_ids: Vec<i32> = strategy_mgr.borrow().iter_ids().cloned().collect();
    let mut matched = false;

    for strategy_id in strategy_ids {
        let mut mgr = strategy_mgr.borrow_mut();
        if let Some(mut strategy) = mgr.take(strategy_id) {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                match update.execution_type() {
                    ExecutionType::New | ExecutionType::Canceled => {
                        strategy.apply_order_update_with_record(update);
                    }
                    ExecutionType::Trade => {
                        strategy.apply_trade_update_with_record(update);
                    }
                    ExecutionType::Expired | ExecutionType::Rejected => {
                        warn!(
                            "Unexpected execution type: {:?}, sym={} cli_id={} ord_id={}",
                            update.execution_type(),
                            OrderUpdate::symbol(update),
                            OrderUpdate::client_order_id(update),
                            OrderUpdate::order_id(update)
                        );
                        strategy.apply_order_update_with_record(update);
                    }
                    _ => {
                        log::error!(
                            "Unhandled execution type: {:?}, sym={} cli_id={} ord_id={}",
                            update.execution_type(),
                            OrderUpdate::symbol(update),
                            OrderUpdate::client_order_id(update),
                            OrderUpdate::order_id(update)
                        );
                    }
                }
            }
            if strategy.is_active() {
                mgr.insert(strategy);
            }
        }
    }

    if !matched {
        debug!(
            "order update unmatched: sym={} cli_id={} ord_id={} x={:?} X={:?}",
            OrderUpdate::symbol(update),
            OrderUpdate::client_order_id(update),
            OrderUpdate::order_id(update),
            update.execution_type(),
            update.status()
        );
    }
}
