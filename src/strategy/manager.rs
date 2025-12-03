use crate::common::time_util::get_timestamp_us;
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::{
    order_update::OrderUpdate, trade_engine_response::TradeEngineResponse,
    trade_update::TradeUpdate,
};
use log::info;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// 控制策略是否处于强平模式的 Trait
pub trait ForceCloseControl {
    fn set_force_close_mode(&mut self, enabled: bool);
    fn is_force_close_mode(&self) -> bool;
}

pub trait Strategy: ForceCloseControl {
    fn get_id(&self) -> i32;
    fn is_strategy_order(&self, order_id: i64) -> bool;
    fn handle_signal_with_record(&mut self, signal: &TradeSignal);
    fn apply_order_update_with_record(&mut self, update: &dyn OrderUpdate);
    fn apply_trade_update_with_record(&mut self, trade: &dyn TradeUpdate);
    fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}
    fn handle_period_clock(&mut self, current_tp: i64);
    fn is_active(&self) -> bool;
    fn symbol(&self) -> Option<&str>;
}

/// Strategy id -> Strategy 映射的简单管理器
pub struct StrategyManager {
    strategies: HashMap<i32, Box<dyn Strategy>>,
    order: VecDeque<i32>,
    known_ids: HashSet<i32>,
    symbol_index: HashMap<String, BTreeSet<i32>>,
}

impl StrategyManager {
    /// 创建空的策略管理器
    pub fn new() -> Self {
        Self {
            strategies: HashMap::new(),
            order: VecDeque::new(),
            known_ids: HashSet::new(),
            symbol_index: HashMap::new(),
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
        let new_symbol = strategy.symbol().map(|s| s.to_ascii_uppercase());
        if let Some(old_symbol) = self
            .strategies
            .get(&id)
            .and_then(|existing| existing.symbol().map(|s| s.to_ascii_uppercase()))
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
        if let Some(symbol) = new_symbol {
            self.symbol_index.entry(symbol).or_default().insert(id);
        }
        if old.is_none() {
            self.order.push_back(id);
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
        self.order.retain(|id| *id != strategy_id);
        let removed = self.strategies.remove(&strategy_id);
        if removed.is_some() {
            self.known_ids.remove(&strategy_id);
            if let Some(symbol) = removed
                .as_ref()
                .and_then(|strategy| strategy.symbol().map(|s| s.to_ascii_uppercase()))
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
        self.order.retain(|id| *id != strategy_id);
        let removed = self.strategies.remove(&strategy_id);
        if let Some(symbol) = removed
            .as_ref()
            .and_then(|strategy| strategy.symbol().map(|s| s.to_ascii_uppercase()))
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

    /// 遍历所有策略 id
    pub fn iter_ids(&self) -> impl Iterator<Item = &i32> {
        self.strategies.keys()
    }

    /// 查询指定 symbol 的所有活跃策略 id（symbol 需传入大写）
    pub fn ids_for_symbol(&self, symbol: &str) -> Option<&BTreeSet<i32>> {
        self.symbol_index.get(symbol)
    }

    /// 指定 symbol 是否存在活跃策略（symbol 需传入大写）
    pub fn has_symbol(&self, symbol: &str) -> bool {
        self.symbol_index.contains_key(symbol)
    }

    /// 获取只读引用（用于快照）
    pub fn get(&self, id: i32) -> Option<&dyn Strategy> {
        self.strategies.get(&id).map(|b| b.as_ref())
    }

    /// 触发全部策略的周期检查，返回本次检查到的策略数量
    pub fn handle_period_clock(&mut self, current_tp: i64) -> usize {
        let iterations = self.order.len();
        let mut inspected: usize = 0usize;
        for _ in 0..iterations {
            let Some(strategy_id) = self.order.pop_front() else {
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
                self.order.push_back(strategy_id);
            }
        }
        inspected
    }

    /// 基于当前时间戳生成策略 ID
    /// 使用时间戳的低31位，确保为正数，约35分钟循环周期
    pub fn generate_strategy_id() -> i32 {
        (get_timestamp_us() & 0x7FFF_FFFF) as i32
    }
}
