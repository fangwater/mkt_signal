use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use crate::common::exchange::Exchange;

#[derive(Debug, Clone, Copy, Default)]
pub struct UsdtBalanceSnapshot {
    pub balance: f64,
    pub borrowed: f64,
    pub cumulative_interest: f64,
    pub last_timestamp: i64,
}

/// USDT 余额/负债维护（按 exchange 维度），不进入 BasicBalanceManager。
#[derive(Debug, Clone)]
pub struct UsdtBalanceManager {
    exchange: Exchange,
    state: UsdtBalanceSnapshot,
}

impl UsdtBalanceManager {
    pub fn new(exchange: Exchange) -> Self {
        Self {
            exchange,
            state: UsdtBalanceSnapshot::default(),
        }
    }

    pub fn exchange(&self) -> Exchange {
        self.exchange
    }

    pub fn snapshot(&self) -> UsdtBalanceSnapshot {
        self.state
    }

    pub fn apply_balance(&mut self, msg: &BasicBalanceMsg) {
        if !msg.symbol.eq_ignore_ascii_case("USDT") {
            return;
        }
        self.state.balance = msg.balance;
        self.state.last_timestamp = msg.timestamp;
    }

    pub fn apply_borrow_interest(&mut self, msg: &BasicBorrowInterestMsg) {
        if !msg.symbol.eq_ignore_ascii_case("USDT") {
            return;
        }
        self.state.borrowed = msg.borrowed;
        self.state.cumulative_interest = msg.interest;
        self.state.last_timestamp = self.state.last_timestamp.max(msg.timestamp);
    }

    /// 返回 USDT 的“净头寸”（与 BasicBalanceManager::balance_position_of 语义保持一致）。
    ///
    /// 全交易所统一口径：上游 BasicBalanceMsg.balance 已是净额（已与负债轧差），直接使用。
    pub fn net_usdt_position(&self) -> f64 {
        self.state.balance
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_usdt_position_uses_equity_directly() {
        let mut mgr = UsdtBalanceManager::new(Exchange::Gate);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "USDT".to_string(), -100.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "USDT".to_string(),
            50.0,
            0.0,
        ));

        assert!((mgr.net_usdt_position() + 100.0).abs() < 1e-12);
    }

    #[test]
    fn binance_usdt_position_uses_equity_directly() {
        let mut mgr = UsdtBalanceManager::new(Exchange::Binance);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "USDT".to_string(), -100.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "USDT".to_string(),
            50.0,
            0.0,
        ));

        assert!((mgr.net_usdt_position() + 100.0).abs() < 1e-12);
    }

    #[test]
    fn bitget_usdt_position_uses_equity_directly() {
        let mut mgr = UsdtBalanceManager::new(Exchange::Bitget);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "USDT".to_string(), 100.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "USDT".to_string(),
            30.0,
            0.5,
        ));

        assert!((mgr.net_usdt_position() - 100.0).abs() < 1e-12);
    }

    #[test]
    fn bybit_usdt_position_uses_equity_directly() {
        let mut mgr = UsdtBalanceManager::new(Exchange::Bybit);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "USDT".to_string(), 100.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "USDT".to_string(),
            30.0,
            0.5,
        ));

        assert!((mgr.net_usdt_position() - 100.0).abs() < 1e-12);
    }
}
