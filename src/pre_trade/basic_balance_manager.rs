use std::collections::HashMap;

use log::info;

use crate::common::{
    basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg},
    exchange::Exchange,
    min_qty_table::MinQtyTable,
};
use crate::pre_trade::net_position::NetPosition;

/// 最小化的余额管理器：仅维护 symbol、余额、累计利息。
#[derive(Debug, Clone)]
pub struct BasicBalance {
    pub exchange: Exchange,
    pub symbol: String,
    pub balance: f64,
    pub borrowed: f64,
    pub cumulative_interest: f64,
    pub last_timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct BasicBalanceManager {
    exchange: Exchange,
    balances: HashMap<String, BasicBalance>,
}

impl BasicBalanceManager {
    pub fn new(exchange: Exchange) -> Self {
        Self {
            exchange,
            balances: HashMap::new(),
        }
    }

    /// 应用 balance 消息：覆盖当前余额，更新时间戳。
    pub fn apply_balance(&mut self, msg: &BasicBalanceMsg) {
        let symbol = msg.symbol.to_ascii_uppercase();
        let entry = self
            .balances
            .entry(symbol.clone())
            .or_insert_with(|| BasicBalance {
                exchange: self.exchange,
                symbol: symbol.clone(),
                balance: 0.0,
                borrowed: 0.0,
                cumulative_interest: 0.0,
                last_timestamp: msg.timestamp,
            });
        entry.symbol = symbol.clone();
        entry.balance = msg.balance;
        entry.last_timestamp = msg.timestamp;

        info!(
            "balance updated: symbol={} balance={} ts={}",
            entry.symbol, entry.balance, entry.last_timestamp
        );
    }

    /// 应用借贷利息消息：累加利息，保留余额不变。
    pub fn apply_borrow_interest(&mut self, msg: &BasicBorrowInterestMsg) {
        let symbol = msg.symbol.clone();
        let entry = self
            .balances
            .entry(symbol.clone())
            .or_insert_with(|| BasicBalance {
                exchange: self.exchange,
                symbol: symbol.clone(),
                balance: 0.0,
                borrowed: msg.borrowed,
                cumulative_interest: 0.0,
                last_timestamp: msg.timestamp,
            });
        entry.borrowed = msg.borrowed;
        entry.cumulative_interest += msg.interest;
        entry.last_timestamp = entry.last_timestamp.max(msg.timestamp);

        info!(
            "borrow interest applied: symbol={} borrowed={} interest={} cumulative_interest={}",
            entry.symbol, msg.borrowed, msg.interest, entry.cumulative_interest
        );
    }

    /// 获取某个 symbol 的余额视图。
    pub fn get(&self, symbol: &str) -> Option<&BasicBalance> {
        self.balances
            .get(&symbol.to_string().to_ascii_uppercase())
            .or_else(|| self.balances.get(symbol))
    }

    /// 返回当前全部余额的快照副本。
    pub fn snapshot(&self) -> Vec<BasicBalance> {
        self.balances.values().cloned().collect()
    }

    /// 获取指定币种的余额头寸。仅支持 OKX（balance 已含借贷影响），其他交易所会 panic。
    pub fn balance_position_of(&self, symbol: &str) -> f64 {
        let mapped = match self.exchange {
            Exchange::Okex => symbol.to_ascii_uppercase(),
            _ => panic!(
                "balance_position_of not implemented for {:?}",
                self.exchange
            ),
        };
        self.balances.get(&mapped).map(|b| b.balance).unwrap_or(0.0)
    }
}

impl NetPosition for BasicBalanceManager {
    fn net_position(&self, symbol: &str, _min_qty_table: Option<&MinQtyTable>) -> f64 {
        self.balance_position_of(symbol)
    }
}
