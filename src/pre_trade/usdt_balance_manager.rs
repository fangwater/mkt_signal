use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use crate::common::exchange::Exchange;

#[derive(Debug, Clone, Copy, Default)]
pub struct UsdtBalanceSnapshot {
    pub wallet: f64,
    pub borrowed: f64,
    pub cumulative_interest: f64,
    pub last_timestamp: i64,
}

impl UsdtBalanceSnapshot {
    /// 净头寸 = 物理钱包余额 - 借币本金 - 累计利息。
    pub fn net(&self) -> f64 {
        self.wallet - self.borrowed - self.cumulative_interest
    }
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
        self.state.wallet = msg.wallet;
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
    /// 全交易所统一口径：BasicBalanceMsg.wallet 是 gross 钱包余额，借款/利息由
    /// BasicBorrowInterestMsg 维护，读取时统一计算净额。
    pub fn net_usdt_position(&self) -> f64 {
        self.state.net()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_usdt_position_uses_wallet_minus_borrow_minus_interest() {
        let mut mgr = UsdtBalanceManager::new(Exchange::Gate);
        mgr.apply_balance(&BasicBalanceMsg::create(1, "USDT".to_string(), 100.0));
        mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "USDT".to_string(),
            50.0,
            2.0,
        ));

        assert!((mgr.net_usdt_position() - 48.0).abs() < 1e-12);
    }

    #[test]
    fn usdt_net_equals_wallet_minus_borrow_minus_interest_all_exchanges() {
        for exchange in [
            Exchange::Binance,
            Exchange::Okex,
            Exchange::Gate,
            Exchange::Bybit,
            Exchange::Bitget,
        ] {
            let mut mgr = UsdtBalanceManager::new(exchange);
            mgr.apply_balance(&BasicBalanceMsg::create(1, "USDT".to_string(), 100.0));
            mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
                1,
                "USDT".to_string(),
                30.0,
                2.0,
            ));

            assert!(
                (mgr.net_usdt_position() - 68.0).abs() < 1e-12,
                "exchange={:?}",
                exchange
            );
        }
    }
}
