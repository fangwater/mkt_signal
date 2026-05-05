use std::collections::HashMap;

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

    pub fn exchange(&self) -> Exchange {
        self.exchange
    }

    /// 应用 balance 消息：覆盖当前余额，更新时间戳。
    pub fn apply_balance(&mut self, msg: &BasicBalanceMsg) {
        let symbol = msg.symbol.to_ascii_uppercase();
        // USDT 单独维护（按 exchange 维度），不进入 BasicBalanceManager。
        if symbol == "USDT" {
            return;
        }
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
    }

    /// 应用借贷利息消息：累加利息，保留余额不变。
    pub fn apply_borrow_interest(&mut self, msg: &BasicBorrowInterestMsg) {
        // 与 balance 更新保持一致：内部统一用大写 key，避免大小写不一致导致 borrowed/interest 丢失。
        let symbol = msg.symbol.to_ascii_uppercase();
        // USDT 单独维护（按 exchange 维度），不进入 BasicBalanceManager。
        if symbol == "USDT" {
            return;
        }
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
        // interest 字段为“当前应计利息总额”，按最新值覆盖即可。
        entry.cumulative_interest = msg.interest;
        entry.last_timestamp = entry.last_timestamp.max(msg.timestamp);
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

    /// 获取指定币种的净余额头寸（base qty）。
    ///
    /// - Binance margin: balance 为总余额，需要扣除 borrowed 与累计 interest 才是净头寸
    /// - OKX/Gate/Bitget margin/spot: balance 已经与负债轧差，直接使用 balance
    pub fn balance_position_of(&self, symbol: &str) -> f64 {
        let mapped = symbol.to_ascii_uppercase();
        let entry = self
            .balances
            .get(&mapped)
            .or_else(|| self.balances.get(symbol));

        let Some(b) = entry else {
            return 0.0;
        };

        match self.exchange {
            Exchange::Okex | Exchange::Gate | Exchange::Bitget => b.balance,
            Exchange::Binance | Exchange::Hyperliquid | Exchange::Bybit => {
                b.balance - b.borrowed - b.cumulative_interest
            }
        }
    }
}

impl NetPosition for BasicBalanceManager {
    fn net_position(&self, symbol: &str, _min_qty_table: Option<&MinQtyTable>) -> f64 {
        self.balance_position_of(symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_balance_position_uses_equity_directly() {
        let mut mgr = BasicBalanceManager::new(Exchange::Gate);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "USDT".to_string(), 100.0));
        mgr.apply_balance(&BasicBalanceMsg::create(1, "BTC".to_string(), 5.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "BTC".to_string(),
            2.0,
            0.5,
        ));

        assert!((mgr.balance_position_of("BTC") - 5.0).abs() < 1e-12);
    }

    #[test]
    fn binance_balance_position_keeps_netting_liability() {
        let mut mgr = BasicBalanceManager::new(Exchange::Binance);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "BTC".to_string(), 5.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "BTC".to_string(),
            2.0,
            0.5,
        ));

        assert!((mgr.balance_position_of("BTC") - 2.5).abs() < 1e-12);
    }

    #[test]
    fn bitget_balance_position_uses_equity_directly() {
        let mut mgr = BasicBalanceManager::new(Exchange::Bitget);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "BTC".to_string(), 5.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "BTC".to_string(),
            2.0,
            0.5,
        ));

        assert!((mgr.balance_position_of("BTC") - 5.0).abs() < 1e-12);
    }

    #[test]
    fn bybit_balance_position_keeps_netting_liability() {
        let mut mgr = BasicBalanceManager::new(Exchange::Bybit);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "BTC".to_string(), 5.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "BTC".to_string(),
            2.0,
            0.5,
        ));

        assert!((mgr.balance_position_of("BTC") - 2.5).abs() < 1e-12);
    }
}
