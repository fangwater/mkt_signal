use crate::common::account_msg::{
    AccountEventType, AccountPositionMsg, AccountUpdateBalanceMsg, AccountUpdatePositionMsg,
    BalanceUpdateMsg, ExecutionReportMsg, OrderTradeUpdateMsg,
};
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};

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

fn hash_str64(s: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

// ---- Key helpers for account event types ----

pub fn key_balance_update(msg: &BalanceUpdateMsg) -> u64 {
    hash64(&[
        AccountEventType::BalanceUpdate as u32 as u64,
        msg.update_id as u64,
        msg.event_time as u64,
    ])
}

pub fn key_account_update_balance(msg: &AccountUpdateBalanceMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountUpdateBalance as u32 as u64,
        msg.event_time as u64,
        msg.transaction_time as u64,
        hash_str64(&msg.asset),
    ])
}

pub fn key_account_position(msg: &AccountPositionMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountPosition as u32 as u64,
        msg.update_id as u64,
        msg.event_time as u64,
        hash_str64(&msg.asset),
    ])
}

pub fn key_account_update_position(msg: &AccountUpdatePositionMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountUpdatePosition as u32 as u64,
        msg.event_time as u64,
        msg.transaction_time as u64,
        msg.symbol_length as u64,
        msg.position_side as u8 as u64,
    ])
}

pub fn key_execution_report(msg: &ExecutionReportMsg) -> u64 {
    hash64(&[
        AccountEventType::ExecutionReport as u32 as u64,
        msg.order_id as u64,
        msg.trade_id as u64,
        msg.update_id as u64,
        msg.event_time as u64,
    ])
}

pub fn key_order_trade_update(msg: &OrderTradeUpdateMsg) -> u64 {
    hash64(&[
        AccountEventType::OrderTradeUpdate as u32 as u64,
        msg.order_id as u64,
        msg.trade_id as u64,
        msg.event_time as u64,
    ])
}
