use crate::{
    common::{
        account_msg::{ExecutionReportMsg, OrderTradeUpdateMsg},
        time_util::get_timestamp_us,
    },
    trade_engine::trade_response_handle::TradeExecOutcome,
};
use bytes::Bytes;
use log::info;
use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};

pub struct StrategySnapshot<'a> {
    pub type_name: &'a str,
    pub payload: Bytes,
}

pub trait Strategy: Any {
    fn get_id(&self) -> i32;
    fn is_strategy_order(&self, order_id: i64) -> bool;
    fn handle_trade_signal(&mut self, signal_raws: &Bytes);
    fn handle_trade_response(&mut self, engine_out: &TradeExecOutcome);
    fn handle_binance_margin_order_update(&mut self, report: &ExecutionReportMsg);
    fn handle_binance_futures_order_update(&mut self, update: &OrderTradeUpdateMsg);
    fn hanle_period_clock(&mut self, current_tp: i64);
    fn is_active(&self) -> bool;
    fn snapshot(&self) -> Option<StrategySnapshot<'_>> {
        None
    }
}

/// Strategy id -> Strategy 映射的简单管理器
pub struct StrategyManager {
    strategies: HashMap<i32, Box<dyn Strategy>>,
    order: VecDeque<i32>,
    known_ids: HashSet<i32>,
}

impl StrategyManager {
    /// 创建空的策略管理器
    pub fn new() -> Self {
        Self {
            strategies: HashMap::new(),
            order: VecDeque::new(),
            known_ids: HashSet::new(),
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
        let is_known = self.known_ids.contains(&id);
        let old = self.strategies.insert(id, strategy);
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
        self.strategies.remove(&strategy_id)
    }

    /// 遍历所有策略 id
    pub fn iter_ids(&self) -> impl Iterator<Item = &i32> {
        self.strategies.keys()
    }

    /// 获取只读引用（用于快照）
    pub fn get(&self, id: i32) -> Option<&dyn Strategy> {
        self.strategies.get(&id).map(|b| b.as_ref())
    }

    /// 触发全部策略的周期检查
    pub fn handle_period_clock(&mut self, current_tp: i64) {
        let iterations = self.order.len();
        for _ in 0..iterations {
            let Some(strategy_id) = self.order.pop_front() else {
                break;
            };

            let mut remove = true;
            if let Some(strategy) = self.strategies.get_mut(&strategy_id) {
                strategy.hanle_period_clock(current_tp);
                remove = !strategy.is_active();
            } 

            if remove {
                if self.strategies.remove(&strategy_id).is_some() {
                    self.known_ids.remove(&strategy_id);
                    info!(
                        "策略管理器: 策略 id={} 在周期检查中被清理，剩余活跃策略数={}",
                        strategy_id,
                        self.strategies.len()
                    );
                }
            } else {
                self.order.push_back(strategy_id);
            }
        }
    }

    /// 基于策略类型和当前时间戳生成策略 ID
    pub fn generate_strategy_id(strategy_type: u8) -> i32 {
        let timestamp_us = get_timestamp_us() as u64;
        let lower_bits = (timestamp_us & 0x00FF_FFFF) as u32;
        let composed = ((strategy_type as u32) << 24) | lower_bits;
        composed as i32
    } 
} 
