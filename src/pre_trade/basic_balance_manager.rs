use std::collections::HashMap;

use crate::common::{
    basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg},
    exchange::Exchange,
    min_qty_table::MinQtyTable,
};
use crate::pre_trade::net_position::NetPosition;

/// 最小化的余额管理器：维护 symbol、钱包余额、借币本金、累计利息。
#[derive(Debug, Clone)]
pub struct BasicBalance {
    pub exchange: Exchange,
    pub symbol: String,
    pub wallet: f64,
    pub borrowed: f64,
    pub cumulative_interest: f64,
    pub last_timestamp: i64,
}

impl BasicBalance {
    /// 净头寸 = 物理钱包余额 - 借币本金 - 累计利息。
    pub fn net(&self) -> f64 {
        self.wallet - self.borrowed - self.cumulative_interest
    }
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

    /// 应用 balance 消息：覆盖当前钱包余额，更新时间戳。
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
                wallet: 0.0,
                borrowed: 0.0,
                cumulative_interest: 0.0,
                last_timestamp: msg.timestamp,
            });
        entry.symbol = symbol.clone();
        entry.wallet = msg.wallet;
        entry.last_timestamp = msg.timestamp;
    }

    /// 应用借贷利息消息：覆盖本金/利息，保留钱包余额不变。
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
                wallet: 0.0,
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
    /// 全交易所统一口径：BasicBalanceMsg.wallet 是 gross 钱包余额，借款/利息由
    /// BasicBorrowInterestMsg 维护，读取时统一计算净额。
    pub fn balance_position_of(&self, symbol: &str) -> f64 {
        let mapped = symbol.to_ascii_uppercase();
        let entry = self
            .balances
            .get(&mapped)
            .or_else(|| self.balances.get(symbol));

        entry.map(|b| b.net()).unwrap_or(0.0)
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
    fn manager_net_equals_wallet_minus_borrow_minus_interest_gate() {
        let mut mgr = BasicBalanceManager::new(Exchange::Gate);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "USDT".to_string(), 100.0));
        mgr.apply_balance(&BasicBalanceMsg::create(1, "BTC".to_string(), 100.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "BTC".to_string(),
            30.0,
            2.0,
        ));

        assert!((mgr.balance_position_of("BTC") - 68.0).abs() < 1e-12);
    }

    #[test]
    fn manager_net_equals_wallet_minus_borrow_minus_interest_all_exchanges() {
        for exchange in [
            Exchange::Binance,
            Exchange::Okex,
            Exchange::Gate,
            Exchange::Bybit,
            Exchange::Bitget,
        ] {
            let mut mgr = BasicBalanceManager::new(exchange);
            mgr.apply_balance(&BasicBalanceMsg::create(1, "BTC".to_string(), 100.0));
            mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
                1,
                "BTC".to_string(),
                30.0,
                2.0,
            ));

            assert!(
                (mgr.balance_position_of("BTC") - 68.0).abs() < 1e-12,
                "exchange={:?}",
                exchange
            );
        }
    }
}
