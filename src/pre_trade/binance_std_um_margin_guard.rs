use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::Side;
use crate::pre_trade::signal_throttle::clear_binance_futures_margin_signal_throttles;
use log::warn;
use mkt_parsers::msg::basic_account_msg::BinanceStdUmWalletSnapshotMsg;
use order_common::TradingVenue;
use runtime_common::time_util::get_timestamp_us;
use std::cell::RefCell;
use trade_signal::ArbMode;

pub const BINANCE_STD_UM_MIN_AVAILABLE_RATIO: f64 = 0.10;
const SNAPSHOT_STALE_AFTER_US: i64 = 30_000_000;
const BLOCK_SUMMARY_INTERVAL_US: i64 = 60_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarginState {
    Unknown,
    Healthy,
    Locked,
}

#[derive(Debug)]
struct BinanceStdUmMarginState {
    state: MarginState,
    available_balance: f64,
    cross_equity: f64,
    available_ratio: f64,
    snapshot_received_us: i64,
    blocked_count: u64,
    last_blocked_symbol: String,
    last_blocked_side: Option<Side>,
    last_summary_us: i64,
    cleared_margin_locks: usize,
}

impl Default for BinanceStdUmMarginState {
    fn default() -> Self {
        Self {
            state: MarginState::Unknown,
            available_balance: 0.0,
            cross_equity: 0.0,
            available_ratio: f64::NAN,
            snapshot_received_us: 0,
            blocked_count: 0,
            last_blocked_symbol: String::new(),
            last_blocked_side: None,
            last_summary_us: get_timestamp_us(),
            cleared_margin_locks: 0,
        }
    }
}

thread_local! {
    static BINANCE_STD_UM_MARGIN_STATE: RefCell<BinanceStdUmMarginState> =
        RefCell::new(BinanceStdUmMarginState::default());
}

pub struct BinanceStdUmMarginGuard;

impl BinanceStdUmMarginGuard {
    pub fn apply_wallet_snapshot(msg: &BinanceStdUmWalletSnapshotMsg) {
        if !msg.asset.trim().eq_ignore_ascii_case("USDT") || !Self::is_enabled() {
            return;
        }

        let cross_equity = msg.cross_equity();
        let available_ratio = available_ratio(msg.available_balance, cross_equity);
        let next_state =
            if available_ratio.is_some_and(|ratio| ratio >= BINANCE_STD_UM_MIN_AVAILABLE_RATIO) {
                MarginState::Healthy
            } else {
                MarginState::Locked
            };
        let now_us = get_timestamp_us();
        BINANCE_STD_UM_MARGIN_STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.state = next_state;
            state.available_balance = msg.available_balance;
            state.cross_equity = cross_equity;
            state.available_ratio = available_ratio.unwrap_or(f64::NAN);
            state.snapshot_received_us = now_us;
        });

        if next_state == MarginState::Healthy {
            let cleared = clear_binance_futures_margin_signal_throttles();
            BINANCE_STD_UM_MARGIN_STATE.with(|state| {
                let mut state = state.borrow_mut();
                state.cleared_margin_locks = state.cleared_margin_locks.saturating_add(cleared);
            });
        }
    }

    pub fn should_block_arb_open(symbol: &str, side: Side, is_reducing: bool) -> bool {
        if !Self::is_enabled() || is_reducing {
            return false;
        }

        let now_us = get_timestamp_us();
        BINANCE_STD_UM_MARGIN_STATE.with(|state| {
            let mut state = state.borrow_mut();
            let snapshot_stale = state.snapshot_received_us <= 0
                || now_us.saturating_sub(state.snapshot_received_us) > SNAPSHOT_STALE_AFTER_US;
            let blocked = snapshot_stale || state.state != MarginState::Healthy;
            if !blocked {
                return false;
            }

            state.blocked_count = state.blocked_count.saturating_add(1);
            state.last_blocked_symbol.clear();
            state.last_blocked_symbol.push_str(symbol);
            state.last_blocked_side = Some(side);
            state.maybe_log_summary(now_us, snapshot_stale);
            true
        })
    }

    pub fn is_enabled() -> bool {
        if MonitorChannel::try_order_manager()
            .is_none_or(|manager| !manager.borrow().binance_is_standard())
        {
            return false;
        }
        MonitorChannel::try_venues().is_some_and(|(open_venue, hedge_venue)| {
            MonitorChannel::instance().arb_mode() == ArbMode::IntraArb
                && open_venue == TradingVenue::BinanceMargin
                && hedge_venue == TradingVenue::BinanceFutures
        })
    }
}

impl BinanceStdUmMarginState {
    fn maybe_log_summary(&mut self, now_us: i64, snapshot_stale: bool) {
        if now_us.saturating_sub(self.last_summary_us) < BLOCK_SUMMARY_INTERVAL_US {
            return;
        }
        let side = self
            .last_blocked_side
            .map(|side| side.as_str())
            .unwrap_or("UNKNOWN");
        warn!(
            "Binance std UM margin guard blocked ArbOpen summary: total={} last_symbol={} last_side={} available={:.8} cross_equity={:.8} available_ratio={:.6} threshold={:.2} snapshot_stale={} cleared_margin_locks={}",
            self.blocked_count,
            self.last_blocked_symbol,
            side,
            self.available_balance,
            self.cross_equity,
            self.available_ratio,
            BINANCE_STD_UM_MIN_AVAILABLE_RATIO,
            snapshot_stale,
            self.cleared_margin_locks
        );
        self.blocked_count = 0;
        self.cleared_margin_locks = 0;
        self.last_summary_us = now_us;
    }
}

fn available_ratio(available_balance: f64, cross_equity: f64) -> Option<f64> {
    if !available_balance.is_finite() || !cross_equity.is_finite() || cross_equity <= 0.0 {
        return None;
    }
    Some(available_balance / cross_equity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn available_ratio_uses_fixed_ten_percent_boundary() {
        assert_eq!(available_ratio(100.0, 1_000.0), Some(0.1));
        assert!(available_ratio(99.0, 1_000.0).unwrap() < BINANCE_STD_UM_MIN_AVAILABLE_RATIO);
        assert!(available_ratio(101.0, 1_000.0).unwrap() > BINANCE_STD_UM_MIN_AVAILABLE_RATIO);
    }

    #[test]
    fn invalid_cross_equity_is_not_treated_as_safe() {
        assert_eq!(available_ratio(100.0, 0.0), None);
        assert_eq!(available_ratio(100.0, -1.0), None);
        assert_eq!(available_ratio(f64::NAN, 1_000.0), None);
    }
}
