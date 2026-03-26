use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::Side;
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::mm_hedge_strategy::{MarketMakerHedgeStrategy, MmHedgeSnapshot};
use crate::strategy::query_engine_response::QueryEngineResponse;
use crate::strategy::{
    order_update::OrderUpdate, trade_engine_response::TradeEngineResponse,
    trade_update::TradeUpdate,
};
use log::info;
use std::any::Any;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MmOpenPriceMapKey {
    pub symbol: String,
    pub price_qv: QuantizedValueKey,
}

impl MmOpenPriceMapKey {
    pub fn new(symbol: impl Into<String>, price_qv: QuantizedValue) -> Self {
        Self {
            symbol: symbol.into().to_ascii_uppercase(),
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
pub struct MmOpenCancelCandidate {
    pub strategy_id: i32,
    pub symbol: String,
    pub price_qv: QuantizedValue,
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
    fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse);
    fn apply_query_engine_response(&mut self, _response: &dyn QueryEngineResponse) {}
    fn handle_period_clock(&mut self, current_tp: i64);
    fn is_active(&self) -> bool;
    fn symbol(&self) -> Option<&str>;
    fn mm_open_price_map_entry(&self) -> Option<MmOpenPriceMapEntry> {
        None
    }
}

/// Strategy id -> Strategy 映射的简单管理器
pub struct StrategyManager {
    strategies: HashMap<i32, Box<dyn Strategy>>,
    order: VecDeque<i32>,
    known_ids: HashSet<i32>,
    symbol_index: HashMap<String, BTreeSet<i32>>,
    mm_open_price_index: HashMap<MmOpenPriceMapKey, BTreeSet<i32>>,
    mm_open_strategy_index: HashMap<i32, MmOpenPriceMapEntry>,
}

impl StrategyManager {
    /// 创建空的策略管理器
    pub fn new() -> Self {
        Self {
            strategies: HashMap::new(),
            order: VecDeque::new(),
            known_ids: HashSet::new(),
            symbol_index: HashMap::new(),
            mm_open_price_index: HashMap::new(),
            mm_open_strategy_index: HashMap::new(),
        }
    }

    fn register_mm_open_price_entry(&mut self, strategy_id: i32, entry: MmOpenPriceMapEntry) {
        let key = MmOpenPriceMapKey::new(
            entry.symbol.clone(),
            QuantizedValue::from_parts(
                entry.price_qv.tick_i64,
                entry.price_qv.tick_exp,
                entry.price_qv.count,
            ),
        );
        self.mm_open_price_index
            .entry(key)
            .or_default()
            .insert(strategy_id);
        self.mm_open_strategy_index.insert(strategy_id, entry);
    }

    fn unregister_mm_open_price_entry(&mut self, strategy_id: i32) {
        let Some(entry) = self.mm_open_strategy_index.remove(&strategy_id) else {
            return;
        };
        let key = MmOpenPriceMapKey::new(
            entry.symbol,
            QuantizedValue::from_parts(
                entry.price_qv.tick_i64,
                entry.price_qv.tick_exp,
                entry.price_qv.count,
            ),
        );
        if let Some(strategy_ids) = self.mm_open_price_index.get_mut(&key) {
            strategy_ids.remove(&strategy_id);
            if strategy_ids.is_empty() {
                self.mm_open_price_index.remove(&key);
            }
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
        let mm_open_entry = strategy.mm_open_price_map_entry();
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
        self.unregister_mm_open_price_entry(id);
        if let Some(entry) = mm_open_entry {
            self.register_mm_open_price_entry(id, entry);
        }
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
            self.unregister_mm_open_price_entry(strategy_id);
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
        self.unregister_mm_open_price_entry(strategy_id);
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

    pub fn mm_open_strategy_ids_by_price_qv(
        &self,
        symbol: &str,
        price_qv: QuantizedValue,
    ) -> Vec<i32> {
        let key = MmOpenPriceMapKey::new(symbol.to_ascii_uppercase(), price_qv);
        self.mm_open_price_index
            .get(&key)
            .map(|strategy_ids| strategy_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn mm_open_strategy_ids_by_price_qv_and_side(
        &self,
        symbol: &str,
        price_qv: QuantizedValue,
        side: Side,
    ) -> Vec<i32> {
        let key = MmOpenPriceMapKey::new(symbol.to_ascii_uppercase(), price_qv);
        let Some(strategy_ids) = self.mm_open_price_index.get(&key) else {
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
            .map(|(key, strategy_ids)| (key.clone(), strategy_ids.iter().copied().collect()))
            .collect();
        rows.sort_by(|(lhs, _), (rhs, _)| lhs.symbol.cmp(&rhs.symbol));
        rows
    }

    pub fn mm_open_price_map_entry(&self, strategy_id: i32) -> Option<&MmOpenPriceMapEntry> {
        self.mm_open_strategy_index.get(&strategy_id)
    }

    pub fn mm_open_cancel_candidates(&self) -> Vec<MmOpenCancelCandidate> {
        let mut rows: Vec<MmOpenCancelCandidate> = self
            .mm_open_strategy_index
            .iter()
            .map(|(strategy_id, entry)| MmOpenCancelCandidate {
                strategy_id: *strategy_id,
                symbol: entry.symbol.clone(),
                price_qv: QuantizedValue::from_parts(
                    entry.price_qv.tick_i64,
                    entry.price_qv.tick_exp,
                    entry.price_qv.count,
                ),
            })
            .collect();
        rows.sort_by(|lhs, rhs| {
            lhs.symbol
                .cmp(&rhs.symbol)
                .then(lhs.strategy_id.cmp(&rhs.strategy_id))
        });
        rows
    }

    /// 获取只读引用（用于快照）
    pub fn get(&self, id: i32) -> Option<&dyn Strategy> {
        self.strategies.get(&id).map(|b| b.as_ref())
    }

    /// 查询指定 symbol 的 MM 对冲策略 id（symbol 不区分大小写）
    pub fn find_mm_hedge_id(&self, symbol: &str) -> Option<i32> {
        let symbol_upper = symbol.to_ascii_uppercase();
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
        let symbol_upper = symbol.to_ascii_uppercase();
        if let Some(id) = self.find_mm_hedge_id(&symbol_upper) {
            return id;
        }
        let strategy_id = StrategyManager::generate_strategy_id();
        let mut strategy = MarketMakerHedgeStrategy::new(strategy_id, symbol_upper.clone());
        let open_venue = MonitorChannel::instance().open_venue();
        let net_qty = MonitorChannel::instance().get_position_qty(&symbol_upper, open_venue);
        if net_qty.abs() > 1e-12 {
            let buy_qty = if net_qty > 0.0 { net_qty } else { 0.0 };
            let sell_qty = if net_qty < 0.0 { -net_qty } else { 0.0 };
            strategy.record_fill(net_qty, buy_qty, sell_qty);
            info!(
                "MMHedge init: symbol={} venue={:?} net={:.8} buy={:.8} sell={:.8} (seed from position)",
                symbol_upper, open_venue, net_qty, buy_qty, sell_qty
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
    ) -> bool {
        let symbol_upper = symbol.to_ascii_uppercase();
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
        mm_hedge.record_fill(signed_qty, buy_qty, sell_qty);
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

    pub fn apply_query_engine_response(
        &mut self,
        strategy_id: i32,
        response: &dyn QueryEngineResponse,
    ) {
        if let Some(strategy) = self.strategies.get_mut(&strategy_id) {
            strategy.apply_query_engine_response(response);
        }
    }

    /// 基于当前时间戳生成策略 ID
    /// 使用时间戳的低31位，确保为正数，约35分钟循环周期
    pub fn generate_strategy_id() -> i32 {
        (get_timestamp_us() & 0x7FFF_FFFF) as i32
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ForceCloseControl, MmOpenPriceMapEntry, MmOpenPriceMapKey, QuantizedValueKey, Strategy,
        StrategyManager,
    };
    use crate::common::tick_math::QuantizedValue;
    use crate::pre_trade::order_manager::Side;
    use crate::signal::trade_signal::TradeSignal;
    use crate::strategy::query_engine_response::QueryEngineResponse;
    use crate::strategy::trade_engine_response::TradeEngineResponse;
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

        fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}

        fn apply_query_engine_response(&mut self, _response: &dyn QueryEngineResponse) {}

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
}
