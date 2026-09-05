use std::collections::HashMap;

use crate::common::min_qty_table::MinQtyTable;
use crate::pre_trade::net_position::NetPosition;
use mkt_parsers::msg::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use runtime_common::exchange::Exchange;

/// 最小化的余额管理器：维护 symbol、钱包余额、借币本金、累计利息。
#[derive(Debug, Clone)]
pub struct BasicBalance {
    pub exchange: Exchange,
    pub symbol: String,
    pub wallet: f64,
    pub borrowed: f64,
    pub cumulative_interest: f64,
    pub wallet_timestamp: i64,
    pub liability_timestamp: i64,
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

    fn settlement_asset(&self) -> &'static str {
        if self.exchange == Exchange::Hyperliquid {
            "USDC"
        } else {
            "USDT"
        }
    }

    /// 应用 balance 消息：覆盖当前钱包余额，更新时间戳。
    pub fn apply_balance(&mut self, msg: &BasicBalanceMsg) {
        let symbol = msg.symbol.to_ascii_uppercase();
        // The settlement asset is maintained by UsdtBalanceManager and must
        // not also enter the per-asset balance map.
        if symbol == self.settlement_asset() {
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
                wallet_timestamp: msg.timestamp,
                liability_timestamp: 0,
                last_timestamp: msg.timestamp,
            });
        if msg.timestamp < entry.wallet_timestamp {
            return;
        }
        entry.symbol = symbol.clone();
        entry.wallet = msg.wallet;
        entry.wallet_timestamp = msg.timestamp;
        entry.last_timestamp = entry.last_timestamp.max(msg.timestamp);
    }

    /// 应用借贷利息消息：覆盖本金/利息，保留钱包余额不变。
    pub fn apply_borrow_interest(&mut self, msg: &BasicBorrowInterestMsg) {
        // 与 balance 更新保持一致：内部统一用大写 key，避免大小写不一致导致 borrowed/interest 丢失。
        let symbol = msg.symbol.to_ascii_uppercase();
        if symbol == self.settlement_asset() {
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
                wallet_timestamp: 0,
                liability_timestamp: msg.timestamp,
                last_timestamp: msg.timestamp,
            });
        if msg.timestamp < entry.liability_timestamp {
            return;
        }
        entry.borrowed = msg.borrowed;
        // interest 字段为“当前应计利息总额”，按最新值覆盖即可。
        entry.cumulative_interest = msg.interest;
        entry.liability_timestamp = msg.timestamp;
        entry.last_timestamp = entry.last_timestamp.max(msg.timestamp);
    }

    /// 获取某个 symbol 的余额视图。
    pub fn get(&self, symbol: &str) -> Option<&BasicBalance> {
        if symbol.bytes().all(|b| !b.is_ascii_lowercase()) {
            return self.balances.get(symbol);
        }

        let upper = symbol.to_ascii_uppercase();
        self.balances
            .get(&upper)
            .or_else(|| self.balances.get(symbol))
    }

    /// 返回当前全部余额的只读迭代器，避免只读汇总路径 clone 整张表。
    pub fn balances_iter(&self) -> impl Iterator<Item = &BasicBalance> {
        self.balances.values()
    }

    pub fn len(&self) -> usize {
        self.balances.len()
    }

    /// Discard the previous complete snapshot before applying a replacement.
    pub fn clear(&mut self) {
        self.balances.clear();
    }

    /// 返回当前全部余额的快照副本。
    pub fn snapshot(&self) -> Vec<BasicBalance> {
        self.balances_iter().cloned().collect()
    }

    /// 获取指定币种的净余额头寸（base qty）。
    ///
    /// 全交易所统一口径：BasicBalanceMsg.wallet 是 gross 钱包余额，借款/利息由
    /// BasicBorrowInterestMsg 维护，读取时统一计算净额。
    pub fn balance_position_of(&self, symbol: &str) -> f64 {
        self.get(symbol).map(|b| b.net()).unwrap_or(0.0)
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
    fn hyperliquid_keeps_usdc_out_of_asset_balances() {
        let mut mgr = BasicBalanceManager::new(Exchange::Hyperliquid);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "USDC".to_string(), 100.0));
        mgr.apply_balance(&BasicBalanceMsg::create(1, "HYPE".to_string(), 2.0));
        assert!(mgr.get("USDC").is_none());
        assert_eq!(mgr.balance_position_of("HYPE"), 2.0);
    }

    #[test]
    fn clear_removes_balances_from_the_previous_snapshot() {
        let mut mgr = BasicBalanceManager::new(Exchange::Hyperliquid);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "HYPE".to_string(), 2.0));
        assert_eq!(mgr.len(), 1);
        mgr.clear();
        assert_eq!(mgr.len(), 0);
        assert_eq!(mgr.balance_position_of("HYPE"), 0.0);
    }

    #[test]
    fn stale_borrow_interest_does_not_override_newer_balance_state() {
        let mut mgr = BasicBalanceManager::new(Exchange::Okex);
        mgr.apply_balance(&BasicBalanceMsg::create(
            1_780_495_229_000,
            "BTC".to_string(),
            0.0,
        ));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1_780_495_229_000,
            "BTC".to_string(),
            0.2728088547403847,
            0.0,
        ));
        assert!((mgr.balance_position_of("BTC") + 0.2728088547403847).abs() < 1e-12);

        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1_780_484_400_000,
            "BTC".to_string(),
            0.3711443959357258,
            0.0000002152637497,
        ));
        assert!((mgr.balance_position_of("BTC") + 0.2728088547403847).abs() < 1e-12);
    }

    #[test]
    fn stale_balance_does_not_override_newer_wallet_state() {
        let mut mgr = BasicBalanceManager::new(Exchange::Okex);
        mgr.apply_balance(&BasicBalanceMsg::create(20, "XTZ".to_string(), 10.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            25,
            "XTZ".to_string(),
            1.0,
            0.1,
        ));
        mgr.apply_balance(&BasicBalanceMsg::create(10, "XTZ".to_string(), 99.0));

        let balance = mgr.get("XTZ").expect("balance");
        assert_eq!(balance.wallet, 10.0);
        assert_eq!(balance.wallet_timestamp, 20);
        assert_eq!(balance.liability_timestamp, 25);
        assert_eq!(balance.last_timestamp, 25);
        assert!((mgr.balance_position_of("XTZ") - 8.9).abs() < 1e-12);
    }

    #[test]
    fn same_timestamp_borrow_interest_can_clear_current_liability() {
        let mut mgr = BasicBalanceManager::new(Exchange::Gate);
        mgr.apply_balance(&BasicBalanceMsg::create(10, "ETH".to_string(), 5.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            10,
            "ETH".to_string(),
            2.0,
            0.5,
        ));
        assert!((mgr.balance_position_of("ETH") - 2.5).abs() < 1e-12);

        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            10,
            "ETH".to_string(),
            0.0,
            0.0,
        ));
        assert!((mgr.balance_position_of("ETH") - 5.0).abs() < 1e-12);
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
