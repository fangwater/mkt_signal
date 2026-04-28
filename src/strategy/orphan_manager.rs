use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::strategy::arb_orphan_strategy::{ArbOrphanSnapshot, ArbOrphanStrategy};
use crate::strategy::hedge_orphan_order_strategy::HedgeOrphanOrderStrategy;
use crate::strategy::manager::{OrphanHandoff, OrphanStrategyRole, Strategy, StrategyManager};
use crate::strategy::mm_orphan_order_strategy::MmOrphanOrderStrategy;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use log::info;
use std::collections::{BTreeSet, HashMap, VecDeque};

pub struct OrphanStrategyManager {
    strategies: HashMap<i32, Box<dyn Strategy>>,
    strategy_queue: VecDeque<i32>,
    symbol_index: HashMap<String, BTreeSet<i32>>,
}

impl OrphanStrategyManager {
    pub fn new() -> Self {
        Self {
            strategies: HashMap::new(),
            strategy_queue: VecDeque::new(),
            symbol_index: HashMap::new(),
        }
    }

    pub fn contains(&self, strategy_id: i32) -> bool {
        self.strategies.contains_key(&strategy_id)
    }

    pub fn len(&self) -> usize {
        self.strategies.len()
    }

    pub fn is_empty(&self) -> bool {
        self.strategies.is_empty()
    }

    pub(crate) fn insert(&mut self, strategy: Box<dyn Strategy>) -> Option<Box<dyn Strategy>> {
        let id = strategy.get_id();
        let new_symbol = strategy.symbol().map(normalize_symbol_for_internal);
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
        let old = self.strategies.insert(id, strategy);
        if let Some(symbol) = new_symbol {
            self.symbol_index.entry(symbol).or_default().insert(id);
        }
        if old.is_none() {
            self.strategy_queue.push_back(id);
            info!(
                "OrphanStrategyManager: add strategy_id={} active_strategies={}",
                id,
                self.strategies.len()
            );
        }
        old
    }

    fn remove(&mut self, strategy_id: i32) -> Option<Box<dyn Strategy>> {
        self.strategy_queue.retain(|id| *id != strategy_id);
        let removed = self.strategies.remove(&strategy_id);
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
        if removed.is_some() {
            info!(
                "OrphanStrategyManager: remove strategy_id={} active_strategies={}",
                strategy_id,
                self.strategies.len()
            );
        }
        removed
    }

    pub(crate) fn take(&mut self, strategy_id: i32) -> Option<Box<dyn Strategy>> {
        self.strategy_queue.retain(|id| *id != strategy_id);
        let removed = self.strategies.remove(&strategy_id);
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

    pub(crate) fn take_next_queued(&mut self) -> Option<Box<dyn Strategy>> {
        let strategy_id = self.strategy_queue.pop_front()?;
        self.take(strategy_id)
    }

    fn find_strategy_id_by<T: 'static>(&self, symbol: &str) -> Option<i32> {
        let symbol = normalize_symbol_for_internal(symbol);
        let ids = self.symbol_index.get(&symbol)?;
        for id in ids {
            if let Some(strategy) = self.strategies.get(id) {
                if strategy.as_any().is::<T>() {
                    return Some(*id);
                }
            }
        }
        None
    }

    fn ensure_mm_orphan_strategy(&mut self, symbol: &str) -> i32 {
        let symbol = normalize_symbol_for_internal(symbol);
        if let Some(strategy_id) = self.find_strategy_id_by::<MmOrphanOrderStrategy>(&symbol) {
            return strategy_id;
        }
        let strategy_id = StrategyManager::generate_strategy_id();
        self.insert(Box::new(MmOrphanOrderStrategy::new(
            strategy_id,
            symbol.clone(),
        )));
        strategy_id
    }

    fn ensure_arb_orphan_strategy(&mut self, symbol: &str) -> i32 {
        let symbol = normalize_symbol_for_internal(symbol);
        if let Some(strategy_id) = self.find_strategy_id_by::<ArbOrphanStrategy>(&symbol) {
            return strategy_id;
        }
        let strategy_id = StrategyManager::generate_strategy_id();
        self.insert(Box::new(ArbOrphanStrategy::new(
            strategy_id,
            symbol.clone(),
        )));
        strategy_id
    }

    fn ensure_hedge_orphan_strategy(&mut self, symbol: &str) -> i32 {
        let symbol = normalize_symbol_for_internal(symbol);
        if let Some(strategy_id) = self.find_strategy_id_by::<HedgeOrphanOrderStrategy>(&symbol) {
            return strategy_id;
        }
        let strategy_id = StrategyManager::generate_strategy_id();
        self.insert(Box::new(HedgeOrphanOrderStrategy::new(
            strategy_id,
            symbol.clone(),
        )));
        strategy_id
    }

    pub fn adopt_orphan_order_id(
        &mut self,
        role: OrphanStrategyRole,
        handoff: &OrphanHandoff,
    ) -> bool {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(handoff.client_order_id) else {
            return false;
        };
        let symbol = order.symbol.clone();
        drop(order);
        let strategy_id = match role {
            OrphanStrategyRole::Mm => self.ensure_mm_orphan_strategy(&symbol),
            OrphanStrategyRole::Hedge => self.ensure_hedge_orphan_strategy(&symbol),
            OrphanStrategyRole::Arb => self.ensure_arb_orphan_strategy(&symbol),
        };
        let Some(strategy) = self.strategies.get_mut(&strategy_id) else {
            return false;
        };
        match role {
            OrphanStrategyRole::Mm => strategy
                .as_any_mut()
                .downcast_mut::<MmOrphanOrderStrategy>()
                .is_some_and(|strategy| strategy.adopt_orphan_order_id(handoff)),
            OrphanStrategyRole::Hedge => strategy
                .as_any_mut()
                .downcast_mut::<HedgeOrphanOrderStrategy>()
                .is_some_and(|strategy| strategy.adopt_orphan_order_id(handoff)),
            OrphanStrategyRole::Arb => strategy
                .as_any_mut()
                .downcast_mut::<ArbOrphanStrategy>()
                .is_some_and(|strategy| strategy.adopt_orphan_order_id(handoff)),
        }
    }

    pub fn apply_order_update(&mut self, update: &dyn OrderUpdate) -> bool {
        if update.client_order_id() <= 0 {
            return false;
        }
        let symbol = normalize_symbol_for_internal(update.symbol());
        let candidate_ids: Vec<i32> = self
            .symbol_index
            .get(&symbol)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
        for strategy_id in candidate_ids {
            let mut remove = false;
            let matched = if let Some(strategy) = self.strategies.get_mut(&strategy_id) {
                if !strategy.is_strategy_order(update.client_order_id()) {
                    false
                } else {
                    strategy.apply_order_update(update);
                    remove = !strategy.is_active();
                    true
                }
            } else {
                false
            };
            if matched {
                if remove {
                    let _ = self.remove(strategy_id);
                }
                return true;
            }
        }
        false
    }

    pub fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        if trade.client_order_id() <= 0 {
            return false;
        }
        let symbol = normalize_symbol_for_internal(trade.symbol());
        let candidate_ids: Vec<i32> = self
            .symbol_index
            .get(&symbol)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
        for strategy_id in candidate_ids {
            let mut remove = false;
            let matched = if let Some(strategy) = self.strategies.get_mut(&strategy_id) {
                if !strategy.is_strategy_order(trade.client_order_id()) {
                    false
                } else {
                    strategy.apply_trade_update(trade);
                    remove = !strategy.is_active();
                    true
                }
            } else {
                false
            };
            if matched {
                if remove {
                    let _ = self.remove(strategy_id);
                }
                return true;
            }
        }
        false
    }

    pub fn arb_orphan_snapshots(&self) -> Vec<ArbOrphanSnapshot> {
        self.strategies
            .values()
            .filter_map(|strategy| {
                strategy
                    .as_any()
                    .downcast_ref::<ArbOrphanStrategy>()
                    .map(|arb| arb.snapshot())
            })
            .collect()
    }

    pub fn handle_period_clock(&mut self, current_tp: i64) -> usize {
        let iterations = self.strategy_queue.len();
        let mut inspected = 0usize;
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
}
