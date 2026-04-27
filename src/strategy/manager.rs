use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::arb_hedge_strategy::{ArbHedgeSnapshot, ArbHedgeStrategy};
use crate::strategy::arb_orphan_strategy::ArbOrphanLeg;
use crate::strategy::mm_hedge_strategy::{MarketMakerHedgeStrategy, MmHedgeSnapshot};
use crate::strategy::uniform_arb_publish::ArbUniformPublishCtx;
use crate::strategy::uniform_mm_publish::MmUniformPublishCtx;
use crate::strategy::{
    order_update::OrderUpdate, trade_engine_response::TradeEngineResponse,
    trade_update::TradeUpdate,
};
use log::info;
use std::any::Any;
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::OnceLock;
use std::thread::{self, ThreadId};

const STRATEGY_ID_MASK: i64 = 0x7FFF_FFFF;

static STRATEGY_ID_OWNER_THREAD: OnceLock<ThreadId> = OnceLock::new();

thread_local! {
    static STRATEGY_ID_ALLOCATOR: Cell<i64> = Cell::new(initial_strategy_id_seed());
}

fn initial_strategy_id_seed() -> i64 {
    let seed = get_timestamp_us() & STRATEGY_ID_MASK;
    if seed <= 0 {
        1
    } else {
        seed
    }
}

fn next_strategy_id_state(current: i64) -> (i32, i64) {
    let current = if current <= 0 || current > STRATEGY_ID_MASK {
        1
    } else {
        current
    };
    let next = if current >= STRATEGY_ID_MASK {
        1
    } else {
        current + 1
    };
    (current as i32, next)
}

fn assert_strategy_id_owner_thread() {
    let current = thread::current().id();
    let owner = *STRATEGY_ID_OWNER_THREAD.get_or_init(|| current);
    assert_eq!(
        current, owner,
        "StrategyManager::generate_strategy_id must run on a single owner thread"
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuantizedValueKey {
    tick_i64: i64,
    tick_exp: i32,
    count: i64,
}

impl From<QuantizedValue> for QuantizedValueKey {
    fn from(value: QuantizedValue) -> Self {
        let (tick_i64, tick_exp) = value.get_tick_parts();
        Self {
            tick_i64,
            tick_exp,
            count: value.get_count(),
        }
    }
}

impl QuantizedValueKey {
    pub fn count(&self) -> i64 {
        self.count
    }

    pub fn to_quantized_value(&self) -> QuantizedValue {
        QuantizedValue::from_parts(self.tick_i64, self.tick_exp, self.count)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MmOpenPriceMapKey {
    pub symbol: String,
    pub price_qv: QuantizedValueKey,
}

impl MmOpenPriceMapKey {
    pub fn new(symbol: impl Into<String>, price_qv: QuantizedValue) -> Self {
        Self {
            symbol: normalize_symbol_for_internal(&symbol.into()),
            price_qv: price_qv.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MmOpenPriceMapEntry {
    pub symbol: String,
    pub side: Side,
    pub client_order_id: i64,
    pub price_qv: QuantizedValueKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArbOpenPriceMapEntry {
    pub symbol: String,
    pub client_order_id: i64,
    pub price_qv: QuantizedValueKey,
}

/// 控制策略是否处于强平模式的 Trait
pub trait ForceCloseControl {
    fn set_force_close_mode(&mut self, enabled: bool);
    fn is_force_close_mode(&self) -> bool;
}

pub trait Strategy: ForceCloseControl {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn get_id(&self) -> i32;
    fn is_strategy_order(&self, order_id: i64) -> bool;
    fn handle_signal(&mut self, signal: &TradeSignal);
    fn apply_order_update(&mut self, update: &dyn OrderUpdate);
    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate);
    fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}
    fn adopt_order_id(&mut self, _handoff: &MmOrphanHandoff) -> bool {
        false
    }
    fn adopt_arb_orphan_order_id(&mut self, _handoff: &ArbOrphanHandoff) -> bool {
        false
    }
    fn adopt_hedge_orphan_order_id(&mut self, _handoff: &HedgeOrphanHandoff) -> bool {
        false
    }
    fn adopt_arb_orphan_residual(&mut self, _residual: &ArbOrphanResidualHandoff) -> bool {
        false
    }
    fn handle_period_clock(&mut self, current_tp: i64);
    fn is_active(&self) -> bool;
    fn symbol(&self) -> Option<&str>;
    fn mm_open_price_map_entry(&self) -> Option<MmOpenPriceMapEntry> {
        None
    }
    fn arb_open_price_map_entry(&self) -> Option<ArbOpenPriceMapEntry> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MmOrphanSourceKind {
    Open,
    Hedge,
}

#[derive(Debug, Clone)]
pub struct MmOrphanHandoff {
    pub client_order_id: i64,
    pub source_strategy_id: i32,
    pub source_kind: MmOrphanSourceKind,
    pub uniform_ctx: MmUniformPublishCtx,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgeOrphanSourceKind {
    Open,
    Hedge,
}

#[derive(Debug, Clone)]
pub struct HedgeOrphanHandoff {
    pub client_order_id: i64,
    pub source_strategy_id: i32,
    pub source_kind: HedgeOrphanSourceKind,
    pub uniform_ctx: ArbUniformPublishCtx,
    pub recorded_base_qty: f64,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct ArbOrphanHandoff {
    pub client_order_id: i64,
    pub source_strategy_id: i32,
    pub leg: ArbOrphanLeg,
    pub uniform_ctx: Option<ArbOrphanUniformCtx>,
}

pub type ArbOrphanUniformCtx = ArbUniformPublishCtx;

#[derive(Debug, Clone)]
pub struct ArbOrphanResidualHandoff {
    pub symbol: String,
    pub venue: TradingVenue,
    pub signed_base_qty: f64,
    pub source_strategy_id: i32,
}

/// Strategy id -> Strategy 映射的简单管理器
pub struct StrategyManager {
    strategies: HashMap<i32, Box<dyn Strategy>>,
    strategy_queue: VecDeque<i32>,
    known_ids: HashSet<i32>,
    symbol_index: HashMap<String, BTreeSet<i32>>,
    mm_open_price_index: HashMap<String, HashMap<QuantizedValueKey, BTreeSet<i32>>>,
    mm_open_strategy_index: HashMap<i32, MmOpenPriceMapEntry>,
    arb_open_price_index: HashMap<String, HashMap<QuantizedValueKey, BTreeSet<i32>>>,
    arb_open_strategy_index: HashMap<i32, ArbOpenPriceMapEntry>,
}

impl StrategyManager {
    /// 创建空的策略管理器
    pub fn new() -> Self {
        Self {
            strategies: HashMap::new(),
            strategy_queue: VecDeque::new(),
            known_ids: HashSet::new(),
            symbol_index: HashMap::new(),
            mm_open_price_index: HashMap::new(),
            mm_open_strategy_index: HashMap::new(),
            arb_open_price_index: HashMap::new(),
            arb_open_strategy_index: HashMap::new(),
        }
    }

    fn register_mm_open_price_entry(&mut self, strategy_id: i32, mut entry: MmOpenPriceMapEntry) {
        entry.symbol = normalize_symbol_for_internal(&entry.symbol);
        self.mm_open_price_index
            .entry(entry.symbol.clone())
            .or_default()
            .entry(entry.price_qv)
            .or_default()
            .insert(strategy_id);
        self.mm_open_strategy_index.insert(strategy_id, entry);
    }

    fn unregister_mm_open_price_entry(&mut self, strategy_id: i32) {
        let Some(entry) = self.mm_open_strategy_index.remove(&strategy_id) else {
            return;
        };
        let mut should_remove_symbol = false;
        if let Some(price_map) = self.mm_open_price_index.get_mut(&entry.symbol) {
            let mut should_remove_price = false;
            if let Some(strategy_ids) = price_map.get_mut(&entry.price_qv) {
                strategy_ids.remove(&strategy_id);
                should_remove_price = strategy_ids.is_empty();
            }
            if should_remove_price {
                price_map.remove(&entry.price_qv);
            }
            should_remove_symbol = price_map.is_empty();
        }
        if should_remove_symbol {
            self.mm_open_price_index.remove(&entry.symbol);
        }
    }

    fn register_arb_open_price_entry(&mut self, strategy_id: i32, mut entry: ArbOpenPriceMapEntry) {
        entry.symbol = normalize_symbol_for_internal(&entry.symbol);
        self.arb_open_price_index
            .entry(entry.symbol.clone())
            .or_default()
            .entry(entry.price_qv)
            .or_default()
            .insert(strategy_id);
        self.arb_open_strategy_index.insert(strategy_id, entry);
    }

    fn unregister_arb_open_price_entry(&mut self, strategy_id: i32) {
        let Some(entry) = self.arb_open_strategy_index.remove(&strategy_id) else {
            return;
        };
        let mut should_remove_symbol = false;
        if let Some(price_map) = self.arb_open_price_index.get_mut(&entry.symbol) {
            let mut should_remove_price = false;
            if let Some(strategy_ids) = price_map.get_mut(&entry.price_qv) {
                strategy_ids.remove(&strategy_id);
                should_remove_price = strategy_ids.is_empty();
            }
            if should_remove_price {
                price_map.remove(&entry.price_qv);
            }
            should_remove_symbol = price_map.is_empty();
        }
        if should_remove_symbol {
            self.arb_open_price_index.remove(&entry.symbol);
        }
    }

    /// 当前维护的策略数量
    pub fn len(&self) -> usize {
        self.strategies.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.strategies.is_empty()
    }

    /// 是否存在指定策略
    pub fn contains(&self, strategy_id: i32) -> bool {
        self.strategies.contains_key(&strategy_id)
    }

    /// 插入策略，如果已有同 id 策略则返回旧值
    pub fn insert(&mut self, strategy: Box<dyn Strategy>) -> Option<Box<dyn Strategy>> {
        let id = strategy.get_id();
        let new_symbol = strategy.symbol().map(normalize_symbol_for_internal);
        let mm_open_entry = strategy.mm_open_price_map_entry();
        let arb_open_entry = strategy.arb_open_price_map_entry();
        if let Some(old_symbol) = self
            .strategies
            .get(&id)
            .and_then(|existing| existing.symbol().map(normalize_symbol_for_internal))
        {
            if let Some(set) = self.symbol_index.get_mut(&old_symbol) {
                set.remove(&id);
                if set.is_empty() {
                    self.symbol_index.remove(&old_symbol);
                }
            }
        }
        let is_known = self.known_ids.contains(&id);
        let old = self.strategies.insert(id, strategy);
        self.unregister_mm_open_price_entry(id);
        self.unregister_arb_open_price_entry(id);
        if let Some(entry) = mm_open_entry {
            self.register_mm_open_price_entry(id, entry);
        }
        if let Some(entry) = arb_open_entry {
            self.register_arb_open_price_entry(id, entry);
        }
        if let Some(symbol) = new_symbol {
            self.symbol_index.entry(symbol).or_default().insert(id);
        }
        if old.is_none() {
            self.strategy_queue.push_back(id);
        }
        if !is_known {
            self.known_ids.insert(id);
            info!(
                "策略管理器: 新增策略 id={}，当前活跃策略数={}",
                id,
                self.strategies.len()
            );
        }
        if old.is_some() {
            info!(
                "策略管理器: 替换已有策略 id={}，当前活跃策略数={}",
                id,
                self.strategies.len()
            );
        }
        old
    }

    /// 移除策略
    pub fn remove(&mut self, strategy_id: i32) -> Option<Box<dyn Strategy>> {
        self.strategy_queue.retain(|id| *id != strategy_id);
        let removed = self.strategies.remove(&strategy_id);
        if removed.is_some() {
            self.unregister_mm_open_price_entry(strategy_id);
            self.unregister_arb_open_price_entry(strategy_id);
            self.known_ids.remove(&strategy_id);
            if let Some(symbol) = removed
                .as_ref()
                .and_then(|strategy| strategy.symbol().map(normalize_symbol_for_internal))
            {
                if let Some(set) = self.symbol_index.get_mut(&symbol) {
                    set.remove(&strategy_id);
                    if set.is_empty() {
                        self.symbol_index.remove(&symbol);
                    }
                }
            }
            info!(
                "策略管理器: 移除策略 id={}，剩余活跃策略数={}",
                strategy_id,
                self.strategies.len()
            );
        }
        removed
    }

    /// 取出指定策略，调用方处理后可重新插入
    pub fn take(&mut self, strategy_id: i32) -> Option<Box<dyn Strategy>> {
        self.strategy_queue.retain(|id| *id != strategy_id);
        let removed = self.strategies.remove(&strategy_id);
        self.unregister_mm_open_price_entry(strategy_id);
        self.unregister_arb_open_price_entry(strategy_id);
        if let Some(symbol) = removed
            .as_ref()
            .and_then(|strategy| strategy.symbol().map(normalize_symbol_for_internal))
        {
            if let Some(set) = self.symbol_index.get_mut(&symbol) {
                set.remove(&strategy_id);
                if set.is_empty() {
                    self.symbol_index.remove(&symbol);
                }
            }
        }
        removed
    }

    /// 按当前调度队列顺序取出下一个策略，调用方处理后可重新插入。
    pub fn take_next_queued(&mut self) -> Option<Box<dyn Strategy>> {
        let strategy_id = self.strategy_queue.pop_front()?;
        self.take(strategy_id)
    }

    /// 遍历所有策略 id
    pub fn iter_ids(&self) -> impl Iterator<Item = &i32> {
        self.strategies.keys()
    }

    /// 查询指定 symbol 的所有活跃策略 id。
    pub fn ids_for_symbol(&self, symbol: &str) -> Option<&BTreeSet<i32>> {
        let symbol = normalize_symbol_for_internal(symbol);
        self.symbol_index.get(&symbol)
    }

    /// 指定 symbol 是否存在活跃策略。
    pub fn has_symbol(&self, symbol: &str) -> bool {
        let symbol = normalize_symbol_for_internal(symbol);
        self.symbol_index.contains_key(&symbol)
    }

    pub fn mm_open_strategy_ids_by_price_qv(
        &self,
        symbol: &str,
        price_qv: QuantizedValue,
    ) -> Vec<i32> {
        let symbol = normalize_symbol_for_internal(symbol);
        let price_qv = QuantizedValueKey::from(price_qv);
        self.mm_open_price_index
            .get(&symbol)
            .and_then(|price_map| price_map.get(&price_qv))
            .map(|strategy_ids| strategy_ids.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default()
    }

    pub fn mm_open_strategy_ids_by_price_qv_and_side(
        &self,
        symbol: &str,
        price_qv: QuantizedValue,
        side: Side,
    ) -> Vec<i32> {
        let symbol = normalize_symbol_for_internal(symbol);
        let price_qv = QuantizedValueKey::from(price_qv);
        let Some(strategy_ids) = self
            .mm_open_price_index
            .get(&symbol)
            .and_then(|price_map| price_map.get(&price_qv))
        else {
            return Vec::new();
        };
        strategy_ids
            .iter()
            .copied()
            .filter(|strategy_id| {
                self.mm_open_strategy_index
                    .get(strategy_id)
                    .is_some_and(|entry| entry.side == side)
            })
            .collect()
    }

    pub fn mm_open_price_map_snapshot(&self) -> Vec<(MmOpenPriceMapKey, Vec<i32>)> {
        let mut rows: Vec<(MmOpenPriceMapKey, Vec<i32>)> = self
            .mm_open_price_index
            .iter()
            .flat_map(|(symbol, price_map)| {
                price_map.iter().map(move |(price_qv, strategy_ids)| {
                    (
                        MmOpenPriceMapKey {
                            symbol: symbol.clone(),
                            price_qv: *price_qv,
                        },
                        strategy_ids.iter().copied().collect::<Vec<_>>(),
                    )
                })
            })
            .collect();
        rows.sort_by(|(lhs, _), (rhs, _)| {
            lhs.symbol
                .cmp(&rhs.symbol)
                .then(lhs.price_qv.count.cmp(&rhs.price_qv.count))
                .then(lhs.price_qv.tick_i64.cmp(&rhs.price_qv.tick_i64))
                .then(lhs.price_qv.tick_exp.cmp(&rhs.price_qv.tick_exp))
        });
        rows
    }

    pub fn mm_open_price_map_entry(&self, strategy_id: i32) -> Option<&MmOpenPriceMapEntry> {
        self.mm_open_strategy_index.get(&strategy_id)
    }

    pub fn arb_open_strategy_ids_by_price_qv(
        &self,
        symbol: &str,
        price_qv: QuantizedValue,
    ) -> Vec<i32> {
        let symbol = normalize_symbol_for_internal(symbol);
        let price_qv = QuantizedValueKey::from(price_qv);
        self.arb_open_price_index
            .get(&symbol)
            .and_then(|price_map| price_map.get(&price_qv))
            .map(|strategy_ids| strategy_ids.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default()
    }

    pub fn arb_open_price_map_snapshot(&self) -> Vec<(MmOpenPriceMapKey, Vec<i32>)> {
        let mut rows: Vec<(MmOpenPriceMapKey, Vec<i32>)> = self
            .arb_open_price_index
            .iter()
            .flat_map(|(symbol, price_map)| {
                price_map.iter().map(move |(price_qv, strategy_ids)| {
                    (
                        MmOpenPriceMapKey {
                            symbol: symbol.clone(),
                            price_qv: *price_qv,
                        },
                        strategy_ids.iter().copied().collect::<Vec<_>>(),
                    )
                })
            })
            .collect();
        rows.sort_by(|(lhs, _), (rhs, _)| {
            lhs.symbol
                .cmp(&rhs.symbol)
                .then(lhs.price_qv.count.cmp(&rhs.price_qv.count))
                .then(lhs.price_qv.tick_i64.cmp(&rhs.price_qv.tick_i64))
                .then(lhs.price_qv.tick_exp.cmp(&rhs.price_qv.tick_exp))
        });
        rows
    }

    /// 获取只读引用（用于快照）
    pub fn get(&self, id: i32) -> Option<&dyn Strategy> {
        self.strategies.get(&id).map(|b| b.as_ref())
    }

    /// 查询指定 symbol 的 MM 对冲策略 id（symbol 不区分大小写）
    pub fn find_mm_hedge_id(&self, symbol: &str) -> Option<i32> {
        let symbol_upper = normalize_symbol_for_internal(symbol);
        let Some(ids) = self.symbol_index.get(&symbol_upper) else {
            return None;
        };
        for id in ids {
            if let Some(strategy) = self.strategies.get(id) {
                if strategy.as_any().is::<MarketMakerHedgeStrategy>() {
                    return Some(*id);
                }
            }
        }
        None
    }

    /// 确保指定 symbol 存在 MM 对冲策略（symbol 不区分大小写）
    pub fn ensure_mm_hedge_strategy(&mut self, symbol: &str) -> i32 {
        let symbol_upper = normalize_symbol_for_internal(symbol);
        if let Some(id) = self.find_mm_hedge_id(&symbol_upper) {
            return id;
        }
        let strategy_id = StrategyManager::generate_strategy_id();
        let mut strategy = MarketMakerHedgeStrategy::new(strategy_id, symbol_upper.clone());
        let open_venue = MonitorChannel::instance().open_venue();
        let net_qty = MonitorChannel::instance().get_position_qty(&symbol_upper, open_venue);
        if net_qty.abs() > 1e-12 {
            let seed_price = MonitorChannel::instance()
                .price_table()
                .borrow()
                .mark_price(&symbol_upper)
                .unwrap_or(0.0);
            let buy_qty = if net_qty > 0.0 { net_qty } else { 0.0 };
            let sell_qty = if net_qty < 0.0 { -net_qty } else { 0.0 };
            strategy.record_fill(get_timestamp_us(), net_qty, buy_qty, sell_qty, seed_price);
            info!(
                "MMHedge init: symbol={} venue={:?} net={:.8} buy={:.8} sell={:.8} seed_price={:.8} (seed from position)",
                symbol_upper, open_venue, net_qty, buy_qty, sell_qty, seed_price
            );
        } else {
            info!(
                "MMHedge init: symbol={} venue={:?} net=0.0 (no seed)",
                symbol_upper, open_venue
            );
        }
        self.insert(Box::new(strategy));
        strategy_id
    }

    /// 记录 MM 开仓成交，累加到对应 symbol 的对冲策略
    pub fn record_mm_hedge_fill(
        &mut self,
        symbol: &str,
        signed_qty: f64,
        buy_qty: f64,
        sell_qty: f64,
        fill_ts: i64,
        price: f64,
    ) -> bool {
        let symbol_upper = normalize_symbol_for_internal(symbol);
        let Some(id) = self.find_mm_hedge_id(&symbol_upper) else {
            return false;
        };
        let Some(strategy) = self.strategies.get_mut(&id) else {
            return false;
        };
        let Some(mm_hedge) = strategy
            .as_any_mut()
            .downcast_mut::<MarketMakerHedgeStrategy>()
        else {
            return false;
        };
        mm_hedge.record_fill(fill_ts, signed_qty, buy_qty, sell_qty, price);
        true
    }

    /// 获取所有 MM 对冲策略的快照
    pub fn mm_hedge_snapshots(&self) -> Vec<MmHedgeSnapshot> {
        let mut snapshots = Vec::new();
        for strategy in self.strategies.values() {
            if let Some(mm_hedge) = strategy.as_any().downcast_ref::<MarketMakerHedgeStrategy>() {
                snapshots.push(mm_hedge.snapshot());
            }
        }
        snapshots
    }

    /// 查询指定 symbol 的 Arb 对冲状态策略 id（symbol 不区分大小写）
    pub fn find_arb_hedge_id(&self, symbol: &str) -> Option<i32> {
        let symbol_upper = normalize_symbol_for_internal(symbol);
        let Some(ids) = self.symbol_index.get(&symbol_upper) else {
            return None;
        };
        for id in ids {
            if let Some(strategy) = self.strategies.get(id) {
                if strategy.as_any().is::<ArbHedgeStrategy>() {
                    return Some(*id);
                }
            }
        }
        None
    }

    /// 确保指定 symbol 存在 Arb 对冲状态策略（symbol 不区分大小写）
    pub fn ensure_arb_hedge_strategy(&mut self, symbol: &str) -> i32 {
        let symbol_upper = normalize_symbol_for_internal(symbol);
        if let Some(id) = self.find_arb_hedge_id(&symbol_upper) {
            return id;
        }
        let strategy_id = StrategyManager::generate_strategy_id();
        let open_venue = MonitorChannel::instance().open_venue();
        let hedge_venue = MonitorChannel::instance().hedge_venue();
        let strategy =
            ArbHedgeStrategy::new(strategy_id, symbol_upper.clone(), open_venue, hedge_venue);
        self.insert(Box::new(strategy));
        info!(
            "ArbHedge init: symbol={} open_venue={:?} hedge_venue={:?}",
            symbol_upper, open_venue, hedge_venue
        );
        strategy_id
    }

    pub fn record_arb_open_fill(
        &mut self,
        symbol: &str,
        side: Side,
        base_qty: f64,
        fill_ts: i64,
        price: f64,
        close_ts: i64,
    ) -> bool {
        let symbol_upper = normalize_symbol_for_internal(symbol);
        let id = self.ensure_arb_hedge_strategy(&symbol_upper);
        let Some(strategy) = self.strategies.get_mut(&id) else {
            return false;
        };
        let Some(arb_hedge) = strategy.as_any_mut().downcast_mut::<ArbHedgeStrategy>() else {
            return false;
        };
        arb_hedge
            .record_open_fill(fill_ts, side, base_qty, price, close_ts)
            .is_some()
    }

    pub fn record_arb_hedge_fill(
        &mut self,
        symbol: &str,
        side: Side,
        base_qty: f64,
        fill_ts: i64,
        price: f64,
    ) -> bool {
        let symbol_upper = normalize_symbol_for_internal(symbol);
        let id = self.ensure_arb_hedge_strategy(&symbol_upper);
        let Some(strategy) = self.strategies.get_mut(&id) else {
            return false;
        };
        let Some(arb_hedge) = strategy.as_any_mut().downcast_mut::<ArbHedgeStrategy>() else {
            return false;
        };
        arb_hedge
            .record_hedge_fill(fill_ts, side, base_qty, price)
            .is_some()
    }

    pub fn arb_hedge_snapshots(&self, now_ts: i64) -> Vec<ArbHedgeSnapshot> {
        let mut snapshots = Vec::new();
        for strategy in self.strategies.values() {
            if let Some(arb_hedge) = strategy.as_any().downcast_ref::<ArbHedgeStrategy>() {
                snapshots.push(arb_hedge.snapshot(now_ts));
            }
        }
        snapshots
    }

    /// 触发全部策略的周期检查，返回本次检查到的策略数量
    pub fn handle_period_clock(&mut self, current_tp: i64) -> usize {
        let iterations = self.strategy_queue.len();
        let mut inspected: usize = 0usize;
        for _ in 0..iterations {
            let Some(strategy_id) = self.strategy_queue.pop_front() else {
                break;
            };

            let mut remove = true;
            if let Some(strategy) = self.strategies.get_mut(&strategy_id) {
                inspected += 1;
                strategy.handle_period_clock(current_tp);
                remove = !strategy.is_active();
            }

            if remove {
                let _ = self.remove(strategy_id);
            } else {
                self.strategy_queue.push_back(strategy_id);
            }
        }
        inspected
    }

    /// 基于当前时间戳生成策略 ID
    /// 首次调用线程会成为 owner 线程；之后仅允许该线程单调分配 strategy_id。
    pub fn generate_strategy_id() -> i32 {
        assert_strategy_id_owner_thread();
        STRATEGY_ID_ALLOCATOR.with(|allocator| {
            let (assigned, next) = next_strategy_id_state(allocator.get());
            allocator.set(next);
            assigned
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        next_strategy_id_state, ForceCloseControl, MmOpenPriceMapEntry, MmOpenPriceMapKey,
        QuantizedValueKey, Strategy, StrategyManager, STRATEGY_ID_MASK,
    };
    use crate::common::tick_math::QuantizedValue;
    use crate::pre_trade::order_manager::Side;
    use crate::signal::trade_signal::TradeSignal;
    use crate::strategy::{order_update::OrderUpdate, trade_update::TradeUpdate};
    use std::any::Any;

    struct DummyMmOpenStrategy {
        id: i32,
        symbol: String,
        side: Side,
        client_order_id: i64,
        price_qv: QuantizedValue,
    }

    impl ForceCloseControl for DummyMmOpenStrategy {
        fn set_force_close_mode(&mut self, _enabled: bool) {}

        fn is_force_close_mode(&self) -> bool {
            false
        }
    }

    impl Strategy for DummyMmOpenStrategy {
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

        fn handle_signal(&mut self, _signal: &TradeSignal) {}

        fn apply_order_update(&mut self, _update: &dyn OrderUpdate) {}

        fn apply_trade_update(&mut self, _trade: &dyn TradeUpdate) {}

        fn handle_period_clock(&mut self, _current_tp: i64) {}

        fn is_active(&self) -> bool {
            true
        }

        fn symbol(&self) -> Option<&str> {
            Some(&self.symbol)
        }

        fn mm_open_price_map_entry(&self) -> Option<MmOpenPriceMapEntry> {
            Some(MmOpenPriceMapEntry {
                symbol: self.symbol.clone(),
                side: self.side,
                client_order_id: self.client_order_id,
                price_qv: self.price_qv.into(),
            })
        }
    }

    #[test]
    fn mm_open_price_map_supports_multiple_ids_per_same_price() {
        let mut manager = StrategyManager::new();
        let qv = QuantizedValue::from_parts(1, -1, 1234);

        manager.insert(Box::new(DummyMmOpenStrategy {
            id: 11,
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            client_order_id: 101,
            price_qv: qv,
        }));
        manager.insert(Box::new(DummyMmOpenStrategy {
            id: 12,
            symbol: "BTCUSDT".to_string(),
            side: Side::Sell,
            client_order_id: 102,
            price_qv: qv,
        }));

        assert_eq!(
            manager.mm_open_strategy_ids_by_price_qv("btcusdt", qv),
            vec![11, 12]
        );
        assert_eq!(
            manager.mm_open_strategy_ids_by_price_qv_and_side("BTCUSDT", qv, Side::Buy),
            vec![11]
        );
        assert_eq!(
            manager.mm_open_strategy_ids_by_price_qv_and_side("BTCUSDT", qv, Side::Sell),
            vec![12]
        );
    }

    #[test]
    fn mm_open_price_map_is_removed_on_take_and_restore_on_reinsert() {
        let mut manager = StrategyManager::new();
        let qv = QuantizedValue::from_parts(5, -2, 777);
        let strategy_id = 21;

        manager.insert(Box::new(DummyMmOpenStrategy {
            id: strategy_id,
            symbol: "ETHUSDT".to_string(),
            side: Side::Buy,
            client_order_id: 201,
            price_qv: qv,
        }));
        assert_eq!(
            manager.mm_open_strategy_ids_by_price_qv("ETHUSDT", qv),
            vec![21]
        );

        let strategy = manager.take(strategy_id).expect("strategy should exist");
        assert!(manager
            .mm_open_strategy_ids_by_price_qv("ETHUSDT", qv)
            .is_empty());

        manager.insert(strategy);
        assert_eq!(
            manager.mm_open_strategy_ids_by_price_qv("ETHUSDT", qv),
            vec![21]
        );
    }

    #[test]
    fn mm_open_price_map_snapshot_contains_key_and_ids() {
        let mut manager = StrategyManager::new();
        let qv = QuantizedValue::from_parts(1, -3, 456);
        manager.insert(Box::new(DummyMmOpenStrategy {
            id: 31,
            symbol: "TRXUSDT".to_string(),
            side: Side::Buy,
            client_order_id: 301,
            price_qv: qv,
        }));

        let snapshot = manager.mm_open_price_map_snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot[0].0,
            MmOpenPriceMapKey {
                symbol: "TRXUSDT".to_string(),
                price_qv: QuantizedValueKey::from(qv),
            }
        );
        assert_eq!(snapshot[0].1, vec![31]);
    }

    #[test]
    fn mm_open_price_map_normalizes_symbol_variants() {
        let mut manager = StrategyManager::new();
        let qv = QuantizedValue::from_parts(1, -3, 789);
        manager.insert(Box::new(DummyMmOpenStrategy {
            id: 41,
            symbol: "SOL-USDT-SWAP".to_string(),
            side: Side::Buy,
            client_order_id: 401,
            price_qv: qv,
        }));

        assert_eq!(
            manager.mm_open_strategy_ids_by_price_qv("SOLUSDT", qv),
            vec![41]
        );
        assert_eq!(
            manager.mm_open_strategy_ids_by_price_qv("SOL-USDT-SWAP", qv),
            vec![41]
        );
    }

    #[test]
    fn next_strategy_id_state_is_monotonic_and_wraps() {
        assert_eq!(next_strategy_id_state(123), (123, 124));
        assert_eq!(
            next_strategy_id_state(STRATEGY_ID_MASK),
            (STRATEGY_ID_MASK as i32, 1)
        );
        assert_eq!(next_strategy_id_state(0), (1, 2));
    }
}
