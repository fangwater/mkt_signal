use log::info;

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
        info!(
            "usdt balance updated: exchange={:?} balance={} ts={}",
            self.exchange, self.state.balance, self.state.last_timestamp
        );
    }

    pub fn apply_borrow_interest(&mut self, msg: &BasicBorrowInterestMsg) {
        if !msg.symbol.eq_ignore_ascii_case("USDT") {
            return;
        }
        self.state.borrowed = msg.borrowed;
        match self.exchange {
            Exchange::Okex | Exchange::Binance => self.state.cumulative_interest = msg.interest,
            _ => {}
        }
        self.state.last_timestamp = self.state.last_timestamp.max(msg.timestamp);
        info!(
            "usdt borrow/interest updated: exchange={:?} borrowed={} interest={} ts={}",
            self.exchange,
            self.state.borrowed,
            self.state.cumulative_interest,
            self.state.last_timestamp
        );
    }

    /// 返回 USDT 的“净头寸”（与 BasicBalanceManager::balance_position_of 语义保持一致）。
    pub fn net_usdt_position(&self) -> f64 {
        match self.exchange {
            Exchange::Okex => self.state.balance,
            Exchange::Binance => {
                self.state.balance - self.state.borrowed - self.state.cumulative_interest
            }
            _ => self.state.balance,
        }
    }
}
