use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::Side;
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::arb_hedge_strategy::{ArbHedgeSnapshot, ArbHedgeStrategy};
use crate::strategy::arb_open_strategy::ArbOpenStrategy;
use crate::strategy::mm_hedge_strategy::{MarketMakerHedgeStrategy, MmHedgeSnapshot};
use crate::strategy::open_strategy_common::{OpenCancelInput, OpenStrategyCommon};
use crate::strategy::uniform_order_helper::UniformPublishCtx;
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
pub struct OpenPriceMapKey {
    pub symbol: String,
    pub price_qv: QuantizedValueKey,
}

impl OpenPriceMapKey {
    pub fn new(symbol: impl Into<String>, price_qv: QuantizedValue) -> Self {
        Self {
            symbol: normalize_symbol_for_internal(&symbol.into()),
            price_qv: price_qv.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenPriceMapEntry {
    pub symbol: String,
    pub side: Side,
    pub client_order_id: i64,
    pub price_qv: QuantizedValueKey,
}

pub trait Strategy {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn get_id(&self) -> i32;
    fn is_strategy_order(&self, order_id: i64) -> bool;
    fn handle_signal(&mut self, signal: &TradeSignal);
    fn apply_order_update(&mut self, update: &dyn OrderUpdate);
    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate);
    fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}
    fn handle_period_clock(&mut self, current_tp: i64);
    fn is_active(&self) -> bool;
    fn symbol(&self) -> Option<&str>;
    fn mm_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
        None
    }
    fn arb_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
        None
    }
    fn has_order_terminal_recorder(&self) -> bool {
        false
    }
    fn order_terminal_recorder_mut(&mut self) -> Option<&mut dyn OrderTerminalRecorder> {
        None
    }
}

pub trait OrderTerminalRecorder {
    fn record_open_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
        close_ts: i64,
        open_client_order_id: i64,
    ) -> bool;

    fn record_hedge_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
        bound_open_client_order_id: i64,
    ) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanSourceKind {
    Open,
    Hedge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanStrategyRole {
    Mm,
    Hedge,
    Arb,
}

impl OrphanStrategyRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mm => "mm_orphan",
            Self::Hedge => "hedge_orphan",
            Self::Arb => "arb_orphan",
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrphanHandoff {
    pub client_order_id: i64,
    pub source_strategy_id: i32,
    pub source_kind: OrphanSourceKind,
    pub uniform_ctx: UniformPublishCtx,
    pub reason: String,
}

impl OrphanHandoff {
    pub fn from_open(
        client_order_id: i64,
        source_strategy_id: i32,
        uniform_ctx: UniformPublishCtx,
        reason: &str,
    ) -> Self {
        Self {
            client_order_id,
            source_strategy_id,
            source_kind: OrphanSourceKind::Open,
            uniform_ctx,
            reason: reason.to_string(),
        }
    }

    pub fn from_hedge(
        client_order_id: i64,
        source_strategy_id: i32,
        uniform_ctx: UniformPublishCtx,
        reason: &str,
    ) -> Self {
        Self {
            client_order_id,
            source_strategy_id,
            source_kind: OrphanSourceKind::Hedge,
            uniform_ctx,
            reason: reason.to_string(),
        }
    }
}

/// Strategy id -> Strategy 映射的简单管理器
pub struct StrategyManager {
    strategies: HashMap<i32, Box<dyn Strategy>>,
    strategy_queue: VecDeque<i32>,
    known_ids: HashSet<i32>,
    symbol_index: HashMap<String, BTreeSet<i32>>,
    mm_open_price_index: HashMap<String, HashMap<QuantizedValueKey, BTreeSet<i32>>>,
    mm_open_strategy_index: HashMap<i32, OpenPriceMapEntry>,
    arb_open_price_index: HashMap<String, HashMap<QuantizedValueKey, BTreeSet<i32>>>,
    arb_open_strategy_index: HashMap<i32, OpenPriceMapEntry>,
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

    fn register_open_price_entry(
        price_index: &mut HashMap<String, HashMap<QuantizedValueKey, BTreeSet<i32>>>,
        strategy_index: &mut HashMap<i32, OpenPriceMapEntry>,
        strategy_id: i32,
        mut entry: OpenPriceMapEntry,
    ) {
        entry.symbol = normalize_symbol_for_internal(&entry.symbol);
        price_index
            .entry(entry.symbol.clone())
            .or_default()
            .entry(entry.price_qv)
            .or_default()
            .insert(strategy_id);
        strategy_index.insert(strategy_id, entry);
    }

    fn unregister_open_price_entry(
        price_index: &mut HashMap<String, HashMap<QuantizedValueKey, BTreeSet<i32>>>,
        strategy_index: &mut HashMap<i32, OpenPriceMapEntry>,
        strategy_id: i32,
    ) {
        let Some(entry) = strategy_index.remove(&strategy_id) else {
            return;
        };
        let mut should_remove_symbol = false;
        if let Some(price_map) = price_index.get_mut(&entry.symbol) {
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
            price_index.remove(&entry.symbol);
        }
    }

    fn register_mm_open_price_entry(&mut self, strategy_id: i32, entry: OpenPriceMapEntry) {
        Self::register_open_price_entry(
            &mut self.mm_open_price_index,
            &mut self.mm_open_strategy_index,
            strategy_id,
            entry,
        );
    }

    fn unregister_mm_open_price_entry(&mut self, strategy_id: i32) {
        Self::unregister_open_price_entry(
            &mut self.mm_open_price_index,
            &mut self.mm_open_strategy_index,
            strategy_id,
        );
    }

    fn register_arb_open_price_entry(&mut self, strategy_id: i32, entry: OpenPriceMapEntry) {
        Self::register_open_price_entry(
            &mut self.arb_open_price_index,
            &mut self.arb_open_strategy_index,
            strategy_id,
            entry,
        );
    }

    fn unregister_arb_open_price_entry(&mut self, strategy_id: i32) {
        Self::unregister_open_price_entry(
            &mut self.arb_open_price_index,
            &mut self.arb_open_strategy_index,
            strategy_id,
        );
    }

    fn open_strategy_ids_by_price_qv(
        price_index: &HashMap<String, HashMap<QuantizedValueKey, BTreeSet<i32>>>,
        symbol: &str,
        price_qv: QuantizedValue,
    ) -> Vec<i32> {
        let symbol = normalize_symbol_for_internal(symbol);
        let price_qv = QuantizedValueKey::from(price_qv);
        price_index
            .get(&symbol)
            .and_then(|price_map| price_map.get(&price_qv))
            .map(|strategy_ids| strategy_ids.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default()
    }

    fn open_strategy_ids_by_price_qv_and_side(
        price_index: &HashMap<String, HashMap<QuantizedValueKey, BTreeSet<i32>>>,
        strategy_index: &HashMap<i32, OpenPriceMapEntry>,
        symbol: &str,
        price_qv: QuantizedValue,
        side: Side,
    ) -> Vec<i32> {
        let symbol = normalize_symbol_for_internal(symbol);
        let price_qv = QuantizedValueKey::from(price_qv);
        let Some(strategy_ids) = price_index
            .get(&symbol)
            .and_then(|price_map| price_map.get(&price_qv))
        else {
            return Vec::new();
        };
        strategy_ids
            .iter()
            .copied()
            .filter(|strategy_id| {
                strategy_index
                    .get(strategy_id)
                    .is_some_and(|entry| entry.side == side)
            })
            .collect()
    }

    fn open_price_map_snapshot(
        price_index: &HashMap<String, HashMap<QuantizedValueKey, BTreeSet<i32>>>,
    ) -> Vec<(OpenPriceMapKey, Vec<i32>)> {
        let mut rows: Vec<(OpenPriceMapKey, Vec<i32>)> = price_index
            .iter()
            .flat_map(|(symbol, price_map)| {
                price_map.iter().map(move |(price_qv, strategy_ids)| {
                    (
                        OpenPriceMapKey {
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
        Self::open_strategy_ids_by_price_qv(&self.mm_open_price_index, symbol, price_qv)
    }

    pub fn mm_open_strategy_ids_by_price_qv_and_side(
        &self,
        symbol: &str,
        price_qv: QuantizedValue,
        side: Side,
    ) -> Vec<i32> {
        Self::open_strategy_ids_by_price_qv_and_side(
            &self.mm_open_price_index,
            &self.mm_open_strategy_index,
            symbol,
            price_qv,
            side,
        )
    }

    pub fn mm_open_price_map_snapshot(&self) -> Vec<(OpenPriceMapKey, Vec<i32>)> {
        Self::open_price_map_snapshot(&self.mm_open_price_index)
    }

    pub fn mm_open_price_map_entry(&self, strategy_id: i32) -> Option<&OpenPriceMapEntry> {
        self.mm_open_strategy_index.get(&strategy_id)
    }

    pub fn arb_open_strategy_ids_by_price_qv(
        &self,
        symbol: &str,
        price_qv: QuantizedValue,
    ) -> Vec<i32> {
        Self::open_strategy_ids_by_price_qv(&self.arb_open_price_index, symbol, price_qv)
    }

    pub fn arb_open_strategy_ids_by_price_qv_and_side(
        &self,
        symbol: &str,
        price_qv: QuantizedValue,
        side: Side,
    ) -> Vec<i32> {
        Self::open_strategy_ids_by_price_qv_and_side(
            &self.arb_open_price_index,
            &self.arb_open_strategy_index,
            symbol,
            price_qv,
            side,
        )
    }

    /// 按 symbol + side 查找所有 ArbOpen 策略 id（不限 price_qv）。
    /// 51008 应急专用：用于扫出同 symbol 同 open 方向的全部挂单 strategy。
    pub fn arb_open_strategy_ids_by_symbol_and_side(&self, symbol: &str, side: Side) -> Vec<i32> {
        let symbol_norm = normalize_symbol_for_internal(symbol);
        self.arb_open_strategy_index
            .iter()
            .filter(|(_, entry)| entry.symbol == symbol_norm && entry.side == side)
            .map(|(id, _)| *id)
            .collect()
    }

    /// 应急撤单入口：给指定 ArbOpen 策略走一次 handle_open_cancel_signal_common。
    /// 调用方需保证 strategy_id 对应 ArbOpenStrategy；不是的话返回 false。
    pub fn cancel_arb_open_by_id(
        &mut self,
        strategy_id: i32,
        cancel_side: Side,
        cancel_reason: &'static str,
        trigger_ts: i64,
    ) -> bool {
        let Some(strategy) = self.strategies.get_mut(&strategy_id) else {
            return false;
        };
        let Some(arb_open) = strategy.as_any_mut().downcast_mut::<ArbOpenStrategy>() else {
            return false;
        };
        arb_open.handle_open_cancel_signal_common(OpenCancelInput {
            signal_name: "EmergencyInsufficientMargin",
            target_strategy_id: strategy_id,
            target_client_order_id: 0,
            cancel_side,
            cancel_reason,
            trigger_ts,
            from_key: Vec::new(),
            mkt_ts: 0,
        });
        true
    }

    pub fn arb_open_price_map_snapshot(&self) -> Vec<(OpenPriceMapKey, Vec<i32>)> {
        Self::open_price_map_snapshot(&self.arb_open_price_index)
    }

    pub fn arb_open_price_map_entry(&self, strategy_id: i32) -> Option<&OpenPriceMapEntry> {
        self.arb_open_strategy_index.get(&strategy_id)
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

    fn find_order_terminal_recorder_id(&self, symbol: &str) -> Option<i32> {
        let symbol_upper = normalize_symbol_for_internal(symbol);
        let ids = self.symbol_index.get(&symbol_upper)?;
        // 这里按 symbol 找 terminal recorder，依赖部署侧保证同一 symbol 不会同时启用
        // MM hedge 和 Arb hedge recorder；否则两个账本都能接 terminal record，会出现串账。
        for id in ids {
            if self
                .strategies
                .get(id)
                .is_some_and(|strategy| strategy.has_order_terminal_recorder())
            {
                return Some(*id);
            }
        }
        None
    }

    pub fn record_open_order_terminal(
        &mut self,
        symbol: &str,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        terminal_ts: i64,
        price: f64,
        close_ts: i64,
        open_client_order_id: i64,
    ) -> bool {
        let Some(id) = self.find_order_terminal_recorder_id(symbol) else {
            return false;
        };
        let Some(strategy) = self.strategies.get_mut(&id) else {
            return false;
        };
        let Some(recorder) = strategy.order_terminal_recorder_mut() else {
            return false;
        };
        recorder.record_open_order_terminal(
            terminal_ts,
            side,
            order_base_qty,
            filled_base_qty,
            price,
            close_ts,
            open_client_order_id,
        )
    }

    pub fn record_hedge_order_terminal(
        &mut self,
        symbol: &str,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        terminal_ts: i64,
        price: f64,
        bound_open_client_order_id: i64,
    ) -> bool {
        let Some(id) = self.find_order_terminal_recorder_id(symbol) else {
            return false;
        };
        let Some(strategy) = self.strategies.get_mut(&id) else {
            return false;
        };
        let Some(recorder) = strategy.order_terminal_recorder_mut() else {
            return false;
        };
        recorder.record_hedge_order_terminal(
            terminal_ts,
            side,
            order_base_qty,
            filled_base_qty,
            price,
            bound_open_client_order_id,
        )
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
        next_strategy_id_state, OpenPriceMapEntry, OpenPriceMapKey, QuantizedValueKey, Strategy,
        StrategyManager, STRATEGY_ID_MASK,
    };
    use crate::common::tick_math::QuantizedValue;
    use crate::pre_trade::order_manager::Side;
    use crate::signal::trade_signal::TradeSignal;
    use crate::strategy::{order_update::OrderUpdate, trade_update::TradeUpdate};
    use std::any::Any;

    struct DummyOpenStrategy {
        id: i32,
        symbol: String,
        side: Side,
        client_order_id: i64,
        price_qv: QuantizedValue,
    }

    impl Strategy for DummyOpenStrategy {
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

        fn mm_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
            Some(OpenPriceMapEntry {
                symbol: self.symbol.clone(),
                side: self.side,
                client_order_id: self.client_order_id,
                price_qv: self.price_qv.into(),
            })
        }

        fn arb_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
            Some(OpenPriceMapEntry {
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

        manager.insert(Box::new(DummyOpenStrategy {
            id: 11,
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            client_order_id: 101,
            price_qv: qv,
        }));
        manager.insert(Box::new(DummyOpenStrategy {
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
        assert_eq!(
            manager.arb_open_strategy_ids_by_price_qv("btcusdt", qv),
            vec![11, 12]
        );
        assert_eq!(
            manager.arb_open_strategy_ids_by_price_qv_and_side("BTCUSDT", qv, Side::Buy),
            vec![11]
        );
        assert_eq!(
            manager.arb_open_strategy_ids_by_price_qv_and_side("BTCUSDT", qv, Side::Sell),
            vec![12]
        );
    }

    #[test]
    fn mm_open_price_map_is_removed_on_take_and_restore_on_reinsert() {
        let mut manager = StrategyManager::new();
        let qv = QuantizedValue::from_parts(5, -2, 777);
        let strategy_id = 21;

        manager.insert(Box::new(DummyOpenStrategy {
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
        manager.insert(Box::new(DummyOpenStrategy {
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
            OpenPriceMapKey {
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
        manager.insert(Box::new(DummyOpenStrategy {
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
