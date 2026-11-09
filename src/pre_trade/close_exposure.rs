//! Bounded temporary exposure for paired arbitrage exits.
//!
//! Existing account imbalance must not disable deleveraging. A close reserves
//! both its inventory and a separate, bounded amount of unfinished paired work.
//! Only factual first-leg fills authorize the opposite, reducing hedge leg.

use order_common::{Side, TradingVenue};
use std::collections::HashMap;

const EPS: f64 = 1e-9;

pub(crate) struct CloseExposureRequest<'a> {
    pub id: i64,
    pub asset: &'a str,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub side: Side,
    pub qty: f64,
    pub open_position: f64,
    pub hedge_position: f64,
    pub symbol_limit_usdt: f64,
    pub total_limit_usdt: f64,
}

#[derive(Debug)]
struct PairedClose {
    asset: String,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    first_side: Side,
    reserved: f64,
    first_filled: f64,
    hedge_filled: f64,
    first_finished: bool,
}

#[derive(Debug, Default)]
pub(crate) struct CloseExposureLedger {
    orders: HashMap<i64, PairedClose>,
}

impl CloseExposureLedger {
    pub fn contains(&self, id: i64) -> bool {
        self.orders.contains_key(&id)
    }

    pub fn matches_asset(&self, id: i64, asset: &str) -> bool {
        self.orders.get(&id).is_some_and(|r| r.asset == asset)
    }

    pub fn reserve(
        &mut self,
        request: CloseExposureRequest<'_>,
        price: impl Fn(&str) -> Option<f64>,
    ) -> Result<f64, String> {
        let r = request;
        if r.id <= 0 || self.contains(r.id) || r.open_venue == r.hedge_venue {
            return Err("invalid or duplicate paired-close identity/venues".into());
        }
        if ![r.qty, r.open_position, r.hedge_position]
            .iter()
            .all(|v| v.is_finite())
            || r.qty <= EPS
            || r.symbol_limit_usdt.is_nan()
            || r.total_limit_usdt.is_nan()
        {
            return Err("invalid paired-close quantity or risk limits".into());
        }
        let sign = if r.side == Side::Buy { 1.0 } else { -1.0 };
        if r.open_position * sign >= 0.0 || r.hedge_position * sign <= 0.0 {
            return Err("paired close requires opposite live positions on both legs".into());
        }
        let mut symbol_held = 0.0;
        let mut total_held_usdt = 0.0;
        for order in self.orders.values() {
            let held = (order.reserved - order.hedge_filled).max(0.0);
            let mark = valid_price(&price, &order.asset)?;
            total_held_usdt += held * mark;
            if order.asset == r.asset {
                if order.first_side != r.side || order.hedge_venue != r.hedge_venue {
                    return Err("opposite paired close is still unfinished".into());
                }
                symbol_held += held;
            }
        }
        let mark = valid_price(&price, r.asset)?;
        let granted = r
            .qty
            .min(r.open_position.abs())
            .min((r.hedge_position.abs() - symbol_held).max(0.0))
            .min((r.symbol_limit_usdt / mark - symbol_held).max(0.0))
            .min(((r.total_limit_usdt - total_held_usdt) / mark).max(0.0));
        if granted <= EPS {
            return Err("paired-close inventory or temporary exposure budget exhausted".into());
        }
        self.orders.insert(
            r.id,
            PairedClose {
                asset: r.asset.to_string(),
                open_venue: r.open_venue,
                hedge_venue: r.hedge_venue,
                first_side: r.side,
                reserved: granted,
                first_filled: 0.0,
                hedge_filled: 0.0,
                first_finished: false,
            },
        );
        Ok(granted)
    }

    pub fn matches_first(&self, id: i64, venue: TradingVenue, side: Side, qty: f64) -> bool {
        self.orders.get(&id).is_some_and(|r| {
            r.open_venue == venue
                && r.first_side == side
                && qty.is_finite()
                && qty > 0.0
                && qty <= r.reserved + EPS
        })
    }

    pub fn unrecorded_first_fill(&self, id: i64, cumulative: f64) -> Option<f64> {
        self.orders
            .get(&id)
            .map(|r| (cumulative.min(r.reserved) - r.first_filled).max(0.0))
    }

    /// Called from the same deduplicated terminal/incremental fill recorder as hedge work.
    pub fn first_fill(&mut self, id: i64, qty: f64) {
        if !qty.is_finite() || qty <= 0.0 {
            return;
        }
        if let Some(r) = self.orders.get_mut(&id) {
            r.first_filled = (r.first_filled + qty).min(r.reserved);
        }
    }

    pub fn finish_first(&mut self, id: i64) {
        if let Some(r) = self.orders.get_mut(&id) {
            r.first_finished = true;
            r.reserved = r.first_filled;
        }
        self.retire(id);
    }

    pub fn check_hedge(
        &self,
        id: i64,
        venue: TradingVenue,
        side: Side,
        qty: f64,
        position: f64,
    ) -> Result<(), String> {
        let r = self
            .orders
            .get(&id)
            .ok_or("missing paired-close fill credit")?;
        let sign = if side == Side::Buy { 1.0 } else { -1.0 };
        if venue != r.hedge_venue
            || side == r.first_side
            || !qty.is_finite()
            || qty <= 0.0
            || !position.is_finite()
            || position * sign >= 0.0
            || qty > position.abs() + EPS
            || qty > (r.first_filled - r.hedge_filled).max(0.0) + EPS
        {
            return Err(
                "paired-close hedge exceeds factual fills or would increase its position".into(),
            );
        }
        Ok(())
    }

    pub fn hedge_fill(&mut self, id: i64, qty: f64) {
        if !qty.is_finite() || qty <= 0.0 {
            return;
        }
        if let Some(r) = self.orders.get_mut(&id) {
            r.hedge_filled = (r.hedge_filled + qty).min(r.first_filled);
        }
        self.retire(id);
    }

    fn retire(&mut self, id: i64) {
        if self
            .orders
            .get(&id)
            .is_some_and(|r| r.first_finished && r.reserved - r.hedge_filled <= EPS)
        {
            self.orders.remove(&id);
        }
    }
}

fn valid_price(price: &impl Fn(&str) -> Option<f64>, asset: &str) -> Result<f64, String> {
    price(asset)
        .filter(|v| v.is_finite() && *v > 0.0)
        .ok_or_else(|| format!("missing paired-close mark price: {asset}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(id: i64, qty: f64) -> CloseExposureRequest<'static> {
        CloseExposureRequest {
            id,
            asset: "FIL",
            open_venue: TradingVenue::GateMargin,
            hedge_venue: TradingVenue::GateFutures,
            side: Side::Sell,
            qty,
            open_position: 100.0,
            hedge_position: -150.0,
            symbol_limit_usdt: 800.0,
            total_limit_usdt: 1_000.0,
        }
    }
    #[test]
    fn existing_unbalanced_account_can_close_but_concurrent_unfinished_work_is_bounded() {
        let mut l = CloseExposureLedger::default();
        l.reserve(request(1, 6.0), |_| Some(100.0)).unwrap();
        assert_eq!(l.reserve(request(2, 3.0), |_| Some(100.0)).unwrap(), 2.0);
        assert!(l.reserve(request(3, 1.0), |_| Some(100.0)).is_err());
    }
    #[test]
    fn cancelled_partial_first_leg_retains_only_factual_hedge_credit() {
        let mut l = CloseExposureLedger::default();
        l.reserve(request(1, 6.0), |_| Some(100.0)).unwrap();
        assert!(l
            .check_hedge(1, TradingVenue::GateFutures, Side::Buy, 1.0, -150.0)
            .is_err());
        l.first_fill(1, 2.0);
        l.finish_first(1);
        l.check_hedge(1, TradingVenue::GateFutures, Side::Buy, 2.0, -150.0)
            .unwrap();
        assert!(l
            .check_hedge(1, TradingVenue::GateFutures, Side::Buy, 3.0, -150.0)
            .is_err());
        assert!(l
            .check_hedge(1, TradingVenue::GateFutures, Side::Sell, 2.0, -150.0)
            .is_err());
        l.hedge_fill(1, 1.0);
        assert!(l.contains(1));
        l.hedge_fill(1, 1.0);
        assert!(!l.contains(1));
    }
    #[test]
    fn first_leg_cancel_does_not_release_unhedged_fill_budget() {
        let mut l = CloseExposureLedger::default();
        l.reserve(request(1, 8.0), |_| Some(100.0)).unwrap();
        l.first_fill(1, 8.0);
        l.finish_first(1);
        assert!(l.reserve(request(2, 1.0), |_| Some(100.0)).is_err());
        l.hedge_fill(1, 1.0);
        l.reserve(request(2, 1.0), |_| Some(100.0)).unwrap();
    }
    #[test]
    fn budget_reprices_other_symbols_and_enforces_account_cap() {
        let mut l = CloseExposureLedger::default();
        l.reserve(request(1, 6.0), |_| Some(100.0)).unwrap();
        let mut r = request(2, 4.0);
        r.asset = "ETH";
        assert!(
            (l.reserve(r, |asset| Some(if asset == "FIL" { 110.0 } else { 100.0 }))
                .unwrap()
                - 3.4)
                .abs()
                < EPS
        );
    }
    #[test]
    fn unmatched_or_overclosing_orders_never_receive_paired_privileges() {
        let mut l = CloseExposureLedger::default();
        let mut r = request(1, 1.0);
        r.hedge_position = 0.0;
        assert!(l.reserve(r, |_| Some(100.0)).is_err());
        let mut r = request(2, 2.0);
        r.hedge_position = -1.0;
        assert_eq!(l.reserve(r, |_| Some(100.0)).unwrap(), 1.0);
        l.finish_first(2);
        l.reserve(request(3, 2.0), |_| Some(100.0)).unwrap();
        assert!(l
            .check_hedge(3, TradingVenue::GateFutures, Side::Buy, 1.0, 1.0)
            .is_err());
        l.finish_first(3);
        assert!(!l.contains(3));
    }

    #[test]
    fn orphan_terminal_credits_only_fills_not_already_recorded_by_the_source() {
        let mut l = CloseExposureLedger::default();
        l.reserve(request(1, 5.0), |_| Some(100.0)).unwrap();
        l.first_fill(1, 2.0);
        assert_eq!(l.unrecorded_first_fill(1, 5.0), Some(3.0));
        l.first_fill(1, 3.0);
        assert_eq!(l.unrecorded_first_fill(1, 5.0), Some(0.0));
        assert_eq!(l.unrecorded_first_fill(1, 4.0), Some(0.0));
        l.finish_first(1);
        l.check_hedge(1, TradingVenue::GateFutures, Side::Buy, 5.0, -150.0)
            .unwrap();
    }
}
