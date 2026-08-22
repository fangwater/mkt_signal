use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::Side;
use crate::pre_trade::symbol_util::extract_base_asset;
use log::warn;
use mkt_parsers::msg::basic_account_msg::BinanceStdUmWalletSnapshotMsg;
use order_common::TradingVenue;
use runtime_common::time_util::get_timestamp_us;
use std::cell::RefCell;
use std::collections::HashMap;
use trade_signal::ArbMode;

pub const BINANCE_STD_CM_MIN_AVAILABLE_RATIO: f64 = 0.10;
const SNAPSHOT_STALE_AFTER_US: i64 = 30_000_000;
const BLOCK_SUMMARY_INTERVAL_US: i64 = 60_000_000;

#[derive(Debug, Clone, Copy)]
struct AssetMarginState {
    available_ratio: f64,
    snapshot_received_us: i64,
    healthy: bool,
}

#[derive(Default)]
struct CmMarginState {
    assets: HashMap<String, AssetMarginState>,
    blocked_count: u64,
    last_summary_us: i64,
}

thread_local! {
    static STATE: RefCell<CmMarginState> = RefCell::new(CmMarginState::default());
}

pub struct BinanceStdCmMarginGuard;

impl BinanceStdCmMarginGuard {
    pub fn apply_wallet_snapshot(msg: &BinanceStdUmWalletSnapshotMsg) {
        if !Self::is_enabled() {
            return;
        }
        let asset = msg.asset.trim().to_ascii_uppercase();
        if asset.is_empty() {
            return;
        }
        let cross_equity = msg.cross_equity();
        let ratio = available_ratio(msg.available_balance, cross_equity);
        let healthy = msg.margin_available != 0
            && ratio.is_some_and(|value| value >= BINANCE_STD_CM_MIN_AVAILABLE_RATIO);
        STATE.with(|state| {
            state.borrow_mut().assets.insert(
                asset,
                AssetMarginState {
                    available_ratio: ratio.unwrap_or(f64::NAN),
                    snapshot_received_us: get_timestamp_us(),
                    healthy,
                },
            );
        });
    }

    pub fn should_block_arb_open(symbol: &str, side: Side, is_reducing: bool) -> bool {
        if !Self::is_enabled() || is_reducing {
            return false;
        }
        let Some(asset) = extract_base_asset(symbol) else {
            return true;
        };
        let now_us = get_timestamp_us();
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            let snapshot = state.assets.get(&asset).copied();
            let stale = snapshot.is_none_or(|snapshot| {
                snapshot.snapshot_received_us <= 0
                    || now_us.saturating_sub(snapshot.snapshot_received_us)
                        > SNAPSHOT_STALE_AFTER_US
            });
            let blocked = stale || snapshot.is_none_or(|snapshot| !snapshot.healthy);
            if blocked {
                state.blocked_count = state.blocked_count.saturating_add(1);
                if state.last_summary_us == 0
                    || now_us.saturating_sub(state.last_summary_us) >= BLOCK_SUMMARY_INTERVAL_US
                {
                    warn!(
                        "Binance std CM margin guard blocked ArbOpen summary: total={} symbol={} asset={} side={} available_ratio={:.6} threshold={:.2} snapshot_stale={}",
                        state.blocked_count,
                        symbol,
                        asset,
                        side.as_str(),
                        snapshot.map_or(f64::NAN, |value| value.available_ratio),
                        BINANCE_STD_CM_MIN_AVAILABLE_RATIO,
                        stale
                    );
                    state.blocked_count = 0;
                    state.last_summary_us = now_us;
                }
            }
            blocked
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
                && hedge_venue == TradingVenue::BinanceCoinFutures
        })
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
    fn available_ratio_uses_same_ten_percent_boundary_as_um() {
        assert_eq!(available_ratio(0.1, 1.0), Some(0.1));
        assert!(available_ratio(0.09, 1.0).unwrap() < BINANCE_STD_CM_MIN_AVAILABLE_RATIO);
        assert_eq!(available_ratio(1.0, 0.0), None);
    }
}
