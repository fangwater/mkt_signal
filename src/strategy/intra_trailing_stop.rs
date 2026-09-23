use crate::strategy::cta_special_strategy::cta_factor_decayed;
use order_common::Side;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use serde::Deserialize;

const EPS: f64 = 1e-12;
const TRIGGER_STEP: f64 = 0.001;
const MOVE_STEP: f64 = 0.0005;
const CTA_FACTOR_MAX_AGE_US: i64 = 120_000_000;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CtaExitConfig {
    pub factor_exit_quantile_long: f64,
    pub factor_exit_quantile_short: f64,
    pub trailing_stop_enabled: bool,
    pub trailing_stop_trigger_step: f64,
    pub trailing_stop_move_step: f64,
}

impl CtaExitConfig {
    pub fn from_open_from_key(from_key: &[u8]) -> Option<Self> {
        let raw = std::str::from_utf8(from_key).ok()?;
        if !raw.split(':').any(|field| field.starts_with("cta_rule=")) {
            return None;
        }
        let value = |key: &str| raw.split(':').find_map(|field| field.strip_prefix(key));
        let factor_exit_quantile_long = value("cta_exit_long=")?.parse::<f64>().ok()?;
        let factor_exit_quantile_short = value("cta_exit_short=")?.parse::<f64>().ok()?;
        let trailing_stop_enabled = match value("cta_trailing=")? {
            "1" | "true" => true,
            "0" | "false" => false,
            _ => return None,
        };
        let trailing_stop_trigger_step = value("cta_trigger=")?.parse::<f64>().ok()?;
        let trailing_stop_move_step = value("cta_move=")?.parse::<f64>().ok()?;
        if !factor_exit_quantile_long.is_finite()
            || !(0.0..0.9).contains(&factor_exit_quantile_long)
            || !factor_exit_quantile_short.is_finite()
            || !(factor_exit_quantile_short > 0.1 && factor_exit_quantile_short <= 1.0)
            || (trailing_stop_enabled
                && (!trailing_stop_trigger_step.is_finite()
                    || trailing_stop_trigger_step <= 0.0
                    || !trailing_stop_move_step.is_finite()
                    || trailing_stop_move_step <= 0.0
                    || trailing_stop_move_step >= trailing_stop_trigger_step))
        {
            return None;
        }
        Some(Self {
            factor_exit_quantile_long,
            factor_exit_quantile_short,
            trailing_stop_enabled,
            trailing_stop_trigger_step,
            trailing_stop_move_step,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrailingStopConfig {
    pub take_profit: f64,
    pub reward_risk_ratio: f64,
}

impl TrailingStopConfig {
    pub fn validate(self) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.take_profit.is_finite()
                && self.take_profit > 0.0
                && self.take_profit < 1.0
                && self.reward_risk_ratio.is_finite()
                && self.reward_risk_ratio > 0.0
                && self.take_profit / self.reward_risk_ratio < 1.0,
            "take_profit must be in (0,1), reward_risk_ratio > 0, and initial stop distance < 1"
        );
        Ok(self)
    }
}

#[derive(Debug, Clone)]
pub struct TrailingPosition {
    pub entry_price: f64,
    pub signed_qty: f64,
    pub reserved_qty: f64,
    pub stop_price: Option<f64>,
    pub trailing_level: u64,
    pub exit_reason: Option<&'static str>,
    pub cta_exit: Option<CtaExitConfig>,
    opened_ts: i64,
    ts: i64,
    close_ts: i64,
    pending_seq: u64,
}

impl TrailingPosition {
    fn available(&self) -> f64 {
        (self.signed_qty.abs() - self.reserved_qty).max(0.0)
    }

    fn evaluate(&mut self, price: f64, config: TrailingStopConfig) {
        if self.exit_reason.is_some()
            || !price.is_finite()
            || price <= 0.0
            || !self.entry_price.is_finite()
            || self.entry_price <= 0.0
        {
            return;
        }
        let direction = self.signed_qty.signum();
        let progress = direction * (price / self.entry_price - 1.0);
        let level = ((progress.max(0.0) / TRIGGER_STEP) + 1e-10).floor() as u64;
        self.trailing_level = self.trailing_level.max(level);
        let candidate = self.entry_price
            * (1.0
                + direction
                    * (-config.take_profit / config.reward_risk_ratio
                        + self.trailing_level as f64 * MOVE_STEP));
        let stop = match self.stop_price {
            Some(previous) if direction > 0.0 => previous.max(candidate),
            Some(previous) => previous.min(candidate),
            None => candidate,
        };
        self.stop_price = Some(stop);
        if direction * (price - stop) <= self.entry_price * EPS {
            self.exit_reason = Some("intra_stop_loss");
        } else if progress + EPS >= config.take_profit {
            self.exit_reason = Some("intra_take_profit");
        }
    }

    fn evaluate_cta(
        &mut self,
        now_ts: i64,
        price: f64,
        config: CtaExitConfig,
        quantile: Option<f64>,
    ) {
        if now_ts < self.opened_ts.saturating_add(1_000_000) {
            return;
        }
        let side = if self.signed_qty > 0.0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let factor_exit = cta_factor_decayed(
            side,
            quantile,
            config.factor_exit_quantile_long,
            config.factor_exit_quantile_short,
        );
        if self.exit_reason.is_some() {
            return;
        }
        if !price.is_finite()
            || price <= 0.0
            || !self.entry_price.is_finite()
            || self.entry_price <= 0.0
        {
            if factor_exit {
                self.exit_reason = Some("cta_factor_exit");
            }
            return;
        }
        let direction = self.signed_qty.signum();
        let progress = direction * (price / self.entry_price - 1.0);
        if config.trailing_stop_enabled {
            let level =
                ((progress.max(0.0) / config.trailing_stop_trigger_step) + 1e-10).floor() as u64;
            self.trailing_level = self.trailing_level.max(level);
        }
        if self.trailing_level > 0 {
            let candidate = self.entry_price
                * (1.0 + direction * self.trailing_level as f64 * config.trailing_stop_move_step);
            self.stop_price = Some(match self.stop_price {
                Some(previous) if direction > 0.0 => previous.max(candidate),
                Some(previous) => previous.min(candidate),
                None => candidate,
            });
        }
        if self.trailing_level > 0
            && self
                .stop_price
                .is_some_and(|stop| direction * (price - stop) <= self.entry_price * EPS)
        {
            self.exit_reason = Some("cta_trailing_stop");
        } else if factor_exit {
            self.exit_reason = Some("cta_factor_exit");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProtectiveTrigger {
    pub open_id: i64,
    pub available_qv: f64,
    pub reserved_qty: f64,
    pub reason: &'static str,
}

#[derive(Debug, Clone)]
pub struct HedgeAllocation {
    pub open_id: i64,
    pub qv: f64,
    pub entry_price: f64,
    pub filled_qty: f64,
}

#[derive(Debug, Default)]
pub struct IntraTrailingBook {
    pub positions: FastHashMap<i64, TrailingPosition>,
    next_seq: u64,
    latest_factor_quantile: Option<f64>,
    latest_factor_ts_ms: i64,
}

impl IntraTrailingBook {
    pub fn new() -> Self {
        Self {
            positions: fast_hash_map(),
            next_seq: 0,
            latest_factor_quantile: None,
            latest_factor_ts_ms: 0,
        }
    }

    pub fn apply_factor_update(
        &mut self,
        model_ts_ms: i64,
        quantile: Option<f64>,
        ready: bool,
    ) -> bool {
        if model_ts_ms <= self.latest_factor_ts_ms {
            return false;
        }
        self.latest_factor_ts_ms = model_ts_ms;
        self.latest_factor_quantile =
            quantile.filter(|value| ready && value.is_finite() && (0.0..=1.0).contains(value));
        true
    }

    pub fn record_open(
        &mut self,
        id: i64,
        ts: i64,
        close_ts: i64,
        qv: f64,
        price: f64,
        open_from_key: &[u8],
    ) {
        if qv.abs() <= EPS {
            return;
        }
        let mut remaining = qv.abs();
        let mut opposite: Vec<_> = self
            .positions
            .iter()
            .filter(|(_, p)| p.signed_qty * qv < 0.0 && p.available() > EPS)
            .map(|(&id, p)| ((p.close_ts, p.ts, p.pending_seq), id))
            .collect();
        opposite.sort_unstable();
        for (_, other_id) in opposite {
            let p = self.positions.get_mut(&other_id).unwrap();
            let matched = remaining.min(p.available());
            p.signed_qty -= p.signed_qty.signum() * matched;
            remaining -= matched;
            if remaining <= EPS {
                break;
            }
        }
        self.positions.retain(|_, p| p.signed_qty.abs() > EPS);
        if remaining <= EPS {
            return;
        }
        self.next_seq = self.next_seq.wrapping_add(1);
        let cta_exit = CtaExitConfig::from_open_from_key(open_from_key);
        let p = self.positions.entry(id).or_insert(TrailingPosition {
            entry_price: price,
            signed_qty: 0.0,
            reserved_qty: 0.0,
            stop_price: None,
            trailing_level: 0,
            exit_reason: None,
            cta_exit,
            opened_ts: ts,
            ts,
            close_ts,
            pending_seq: self.next_seq,
        });
        if p.signed_qty.abs() <= EPS {
            p.opened_ts = ts;
        } else {
            p.opened_ts = p.opened_ts.min(ts);
        }
        if p.available() <= EPS {
            p.ts = ts;
            p.close_ts = close_ts;
        } else {
            p.ts = p.ts.min(ts);
            p.close_ts = if p.close_ts <= 0 || close_ts <= 0 {
                0
            } else {
                p.close_ts.min(close_ts)
            };
        }
        if p.cta_exit.is_none() {
            p.cta_exit = cta_exit;
        }
        p.pending_seq = self.next_seq;
        let old_qty = p.signed_qty.abs();
        p.entry_price = (p.entry_price * old_qty + price * remaining) / (old_qty + remaining);
        p.signed_qty += qv.signum() * remaining;
    }

    pub fn reserve(&mut self, id: i64, qv: f64, price: f64) -> HedgeAllocation {
        if let Some(p) = self.positions.get_mut(&id) {
            p.reserved_qty += qv.abs();
        }
        HedgeAllocation {
            open_id: id,
            qv,
            entry_price: price,
            filled_qty: 0.0,
        }
    }

    pub fn restore_unsubmitted(&mut self, id: i64, now_ts: i64, qty: f64) {
        self.next_seq = self.next_seq.wrapping_add(1);
        if let Some(p) = self.positions.get_mut(&id) {
            p.ts = if p.available() - qty <= EPS {
                now_ts
            } else {
                p.ts.min(now_ts)
            };
            p.close_ts = 0;
            p.pending_seq = self.next_seq;
        }
    }

    // Cumulative exchange fills are allocated in reservation order; repeats are idempotent.
    pub fn apply_fills(&mut self, allocations: &mut [HedgeAllocation], cumulative: f64) {
        let mut remaining = cumulative.max(0.0);
        for allocation in allocations {
            let filled = remaining.min(allocation.qv.abs());
            remaining = (remaining - allocation.qv.abs()).max(0.0);
            let delta = (filled - allocation.filled_qty).max(0.0);
            if let Some(p) = self.positions.get_mut(&allocation.open_id) {
                p.signed_qty -= allocation.qv.signum() * delta;
                p.reserved_qty = (p.reserved_qty - delta).max(0.0);
            }
            allocation.filled_qty += delta;
        }
        self.positions.retain(|_, p| p.signed_qty.abs() > EPS);
    }

    pub fn release(&mut self, now_ts: i64, allocation: &HedgeAllocation) {
        if allocation.qv.abs() - allocation.filled_qty <= EPS {
            return;
        }
        let mut available = 0.0;
        self.next_seq = self.next_seq.wrapping_add(1);
        if let Some(p) = self.positions.get_mut(&allocation.open_id) {
            p.ts = if p.available() <= EPS {
                now_ts
            } else {
                p.ts.min(now_ts)
            };
            p.pending_seq = self.next_seq;
            available = (allocation.qv.abs() - allocation.filled_qty).max(0.0);
            p.reserved_qty = (p.reserved_qty - available).max(0.0);
            p.close_ts = 0;
        }
        // Opens arriving while a hedge was reserved can leave opposite pending work.
        let mut opposite: Vec<_> = self
            .positions
            .iter()
            .filter(|(_, p)| p.signed_qty * allocation.qv < 0.0 && p.available() > EPS)
            .map(|(&id, p)| ((p.close_ts, p.ts, p.pending_seq), id))
            .collect();
        opposite.sort_unstable();
        for (_, id) in opposite {
            let p = self.positions.get_mut(&id).unwrap();
            let matched = available.min(p.available());
            p.signed_qty -= p.signed_qty.signum() * matched;
            available -= matched;
            if let Some(own) = self.positions.get_mut(&allocation.open_id) {
                own.signed_qty -= allocation.qv.signum() * matched;
            }
            if available <= EPS {
                break;
            }
        }
        self.positions.retain(|_, p| p.signed_qty.abs() > EPS);
    }

    pub fn triggers(
        &mut self,
        now_ts: i64,
        config: Option<TrailingStopConfig>,
        open_bid: f64,
        open_ask: f64,
        hedge_bid: f64,
        hedge_ask: f64,
    ) -> Vec<ProtectiveTrigger> {
        let factor_quantile = (self.latest_factor_ts_ms > 0
            && now_ts.saturating_sub(self.latest_factor_ts_ms.saturating_mul(1_000))
                <= CTA_FACTOR_MAX_AGE_US)
            .then_some(self.latest_factor_quantile)
            .flatten();
        if config.is_none() {
            for p in self.positions.values_mut().filter(|p| p.cta_exit.is_none()) {
                p.stop_price = None;
                p.trailing_level = 0;
                p.exit_reason = None;
            }
        }
        let mut triggered = Vec::new();
        for (&id, p) in &mut self.positions {
            if let Some(cta_exit) = p.cta_exit {
                p.evaluate_cta(
                    now_ts,
                    if p.signed_qty > 0.0 {
                        hedge_bid
                    } else {
                        hedge_ask
                    },
                    cta_exit,
                    factor_quantile,
                );
            } else if let Some(config) = config {
                p.evaluate(
                    if p.signed_qty > 0.0 {
                        open_bid
                    } else {
                        open_ask
                    },
                    config,
                );
            }
            if let Some(reason) = p.exit_reason {
                let available = p.available();
                if available > EPS || p.reserved_qty > EPS {
                    triggered.push(ProtectiveTrigger {
                        open_id: id,
                        available_qv: p.signed_qty.signum() * available,
                        reserved_qty: p.reserved_qty,
                        reason,
                    });
                }
            }
        }
        triggered.sort_unstable_by_key(|trigger| trigger.open_id);
        triggered
    }

    pub fn has_cta_positions(&self) -> bool {
        self.positions.values().any(|p| p.cta_exit.is_some())
    }

    pub fn has_opposite_cta_position(&self, opening_qv: f64) -> bool {
        opening_qv.abs() > EPS
            && self.positions.values().any(|position| {
                position.cta_exit.is_some() && position.signed_qty * opening_qv < -EPS
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> TrailingStopConfig {
        TrailingStopConfig {
            take_profit: 0.005,
            reward_risk_ratio: 2.0,
        }
    }

    fn cta_from_key() -> &'static [u8] {
        b"1:cta_rule=r:cta_exit_long=0.3:cta_exit_short=0.7:cta_trailing=1:cta_trigger=0.01:cta_move=0.005"
    }

    #[test]
    fn independent_longs_and_latched_stop() {
        let mut book = IntraTrailingBook::new();
        book.record_open(1, 1, 0, 1.0, 100.0, b"");
        book.record_open(2, 2, 0, 1.0, 99.8, b"");
        assert!(book
            .triggers(2, Some(config()), 100.1, 100.11, 0.0, 0.0)
            .is_empty());
        assert!((book.positions[&1].stop_price.unwrap() - 99.8).abs() < 1e-9);
        let triggers = book.triggers(3, Some(config()), 99.79, 99.8, 0.0, 0.0);
        assert_eq!(
            triggers,
            vec![ProtectiveTrigger {
                open_id: 1,
                available_qv: 1.0,
                reserved_qty: 0.0,
                reason: "intra_stop_loss",
            }]
        );
        assert_eq!(
            book.triggers(4, Some(config()), 100.0, 100.01, 0.0, 0.0),
            triggers
        );
    }

    #[test]
    fn partial_aggregate_fill_cancel_and_retry_keep_identity() {
        let mut book = IntraTrailingBook::new();
        book.record_open(1, 1, 0, 1.0, 100.0, b"");
        book.record_open(2, 2, 0, 1.0, 99.0, b"");
        let mut allocations = vec![book.reserve(1, 1.0, 100.0), book.reserve(2, 0.5, 99.0)];
        book.apply_fills(&mut allocations, 1.2);
        book.apply_fills(&mut allocations, 1.2);
        assert!(!book.positions.contains_key(&1));
        assert!((book.positions[&2].signed_qty - 0.8).abs() < 1e-9);
        assert!((book.positions[&2].available() - 0.5).abs() < 1e-9);
        for a in &allocations {
            book.release(3, a);
        }
        assert!((book.positions[&2].available() - 0.8).abs() < 1e-9);
        assert_eq!(book.positions[&2].entry_price, 99.0);
    }

    #[test]
    fn short_uses_ask_and_config_removal_disables() {
        let mut book = IntraTrailingBook::new();
        book.record_open(1, 1, 0, -1.0, 100.0, b"");
        assert!(book
            .triggers(2, Some(config()), 99.4, 99.9, 0.0, 0.0)
            .is_empty());
        assert!((book.positions[&1].stop_price.unwrap() - 100.2).abs() < 1e-9);
        assert_eq!(
            book.triggers(3, Some(config()), 99.4, 99.5, 0.0, 0.0)[0].reason,
            "intra_take_profit"
        );
        assert!(book.triggers(4, None, 0.0, 0.0, 0.0, 0.0).is_empty());
        assert!(book.positions[&1].exit_reason.is_none());
    }

    #[test]
    fn interleaved_opens_reservations_and_cancels_match_execution_quantities() {
        use crate::strategy::net_qty_queue::TimedNetQtyQueue;
        let mut queue = TimedNetQtyQueue::new();
        let mut book = IntraTrailingBook::new();
        let mut reservations = Vec::new();
        let mut seed = 7_u64;
        for step in 1..2000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let now = step / 3;
            match seed % 3 {
                0 => {
                    let qv = if seed & 8 == 0 { 1.0 } else { -1.0 };
                    let id = 3000 - step;
                    let close = if seed & 16 == 0 { 0 } else { 5000 };
                    book.record_open(id, now, close, qv, 100.0, b"");
                    queue.upsert_open_lot(now, close, qv, 100.0, id);
                }
                1 => {
                    if let Some(lot) = queue.lots().first() {
                        let id = lot.open_client_order_id.unwrap();
                        let borrowed = queue.borrow_open_id(now, lot.qv * 0.5, id);
                        reservations.push(book.reserve(id, borrowed.qv, lot.price));
                    }
                }
                _ => {
                    if let Some(mut allocation) = reservations.pop() {
                        let fill = allocation.qv.abs() * 0.25;
                        book.apply_fills(std::slice::from_mut(&mut allocation), fill);
                        book.release(now, &allocation);
                        queue.release_with_id(
                            now,
                            allocation.qv.signum() * (allocation.qv.abs() - fill),
                            allocation.entry_price,
                            allocation.open_id,
                        );
                    }
                }
            }
            for (&id, p) in &book.positions {
                let expected = queue
                    .find_lot_by_open_id(id)
                    .map(|lot| lot.qty)
                    .unwrap_or(0.0);
                assert!(
                    (p.available() - expected).abs() < 1e-9,
                    "step={step} id={id}: available={} expected={expected}",
                    p.available()
                );
            }
            for lot in queue.lots() {
                let p = &book.positions[&lot.open_client_order_id.unwrap()];
                assert!((p.available() - lot.qty).abs() < 1e-9);
            }
        }
    }

    #[test]
    fn cta_uses_hedge_quote_and_preserves_reserved_trigger() {
        let mut book = IntraTrailingBook::new();
        book.record_open(7, 1_000_000, 0, 1.0, 100.0, cta_from_key());
        assert!(book.apply_factor_update(1_000, Some(0.2), true));
        let allocation = book.reserve(7, 1.0, 100.0);
        let triggers = book.triggers(2_000_000, Some(config()), 200.0, 201.0, 99.49, 99.5);
        assert_eq!(
            triggers,
            vec![ProtectiveTrigger {
                open_id: 7,
                available_qv: 0.0,
                reserved_qty: 1.0,
                reason: "cta_factor_exit",
            }]
        );
        book.release(2_000_000, &allocation);
        assert_eq!(
            book.triggers(2_000_001, None, 0.0, 0.0, 110.0, 111.0)[0].available_qv,
            1.0
        );
    }

    #[test]
    fn cta_factor_exit_does_not_require_a_fresh_quote() {
        let mut book = IntraTrailingBook::new();
        book.record_open(7, 1_000_000, 0, 1.0, 100.0, cta_from_key());
        assert!(book.apply_factor_update(1_000, Some(0.2), true));
        assert_eq!(
            book.triggers(2_000_000, None, f64::NAN, f64::NAN, f64::NAN, f64::NAN)[0].reason,
            "cta_factor_exit"
        );
    }

    #[test]
    fn cta_does_not_reuse_expired_factor_for_new_lot() {
        let mut book = IntraTrailingBook::new();
        assert!(book.apply_factor_update(1_000, Some(0.2), true));
        book.record_open(7, 122_000_000, 0, 1.0, 100.0, cta_from_key());
        assert!(book
            .triggers(123_000_000, None, 0.0, 0.0, 100.0, 100.0)
            .is_empty());
    }

    #[test]
    fn cta_trailing_needs_a_favorable_move_and_no_hard_stop() {
        let mut trailing = IntraTrailingBook::new();
        trailing.record_open(1, 1_000_000, 0, 1.0, 100.0, cta_from_key());
        assert!(trailing
            .triggers(2_000_000, None, 0.0, 0.0, 99.0, 99.1)
            .is_empty());
        assert!(trailing
            .triggers(3_000_000, None, 0.0, 0.0, 101.1, 101.2)
            .is_empty());
        assert_eq!(trailing.positions[&1].trailing_level, 1);
        let trigger = trailing.triggers(4_000_000, None, 0.0, 0.0, 100.4, 100.5);
        assert_eq!(trigger[0].reason, "cta_trailing_stop");
    }

    #[test]
    fn cta_exit_config_rejects_move_not_below_trigger() {
        let invalid = b"1:cta_rule=r:cta_exit_long=0.3:cta_exit_short=0.7:cta_trailing=1:cta_trigger=0.001:cta_move=0.001";
        assert!(CtaExitConfig::from_open_from_key(invalid).is_none());
    }

    #[test]
    fn cta_opposite_position_gate_uses_live_lot_direction() {
        let mut book = IntraTrailingBook::new();
        book.record_open(1, 1, 0, 1.0, 100.0, cta_from_key());
        assert!(!book.has_opposite_cta_position(1.0));
        assert!(book.has_opposite_cta_position(-1.0));
    }
}
