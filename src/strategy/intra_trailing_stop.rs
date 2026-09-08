use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use serde::Deserialize;

const EPS: f64 = 1e-12;
const TRIGGER_STEP: f64 = 0.001;
const MOVE_STEP: f64 = 0.0005;

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
}

impl IntraTrailingBook {
    pub fn new() -> Self {
        Self {
            positions: fast_hash_map(),
            next_seq: 0,
        }
    }

    pub fn record_open(&mut self, id: i64, ts: i64, close_ts: i64, qv: f64, price: f64) {
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
        let p = self.positions.entry(id).or_insert(TrailingPosition {
            entry_price: price,
            signed_qty: 0.0,
            reserved_qty: 0.0,
            stop_price: None,
            trailing_level: 0,
            exit_reason: None,
            ts,
            close_ts,
            pending_seq: self.next_seq,
        });
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
        config: Option<TrailingStopConfig>,
        bid: f64,
        ask: f64,
    ) -> Vec<(i64, f64, &'static str)> {
        let Some(config) = config else {
            for p in self.positions.values_mut() {
                p.stop_price = None;
                p.trailing_level = 0;
                p.exit_reason = None;
            }
            return Vec::new();
        };
        let mut triggered = Vec::new();
        for (&id, p) in &mut self.positions {
            p.evaluate(if p.signed_qty > 0.0 { bid } else { ask }, config);
            if let Some(reason) = p.exit_reason {
                if p.available() > EPS {
                    triggered.push((id, p.signed_qty.signum() * p.available(), reason));
                }
            }
        }
        triggered.sort_unstable_by_key(|(id, _, _)| *id);
        triggered
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

    #[test]
    fn independent_longs_and_latched_stop() {
        let mut book = IntraTrailingBook::new();
        book.record_open(1, 1, 0, 1.0, 100.0);
        book.record_open(2, 2, 0, 1.0, 99.8);
        assert!(book.triggers(Some(config()), 100.1, 100.11).is_empty());
        assert!((book.positions[&1].stop_price.unwrap() - 99.8).abs() < 1e-9);
        let triggers = book.triggers(Some(config()), 99.79, 99.8);
        assert_eq!(triggers, vec![(1, 1.0, "intra_stop_loss")]);
        assert_eq!(book.triggers(Some(config()), 100.0, 100.01), triggers);
    }

    #[test]
    fn partial_aggregate_fill_cancel_and_retry_keep_identity() {
        let mut book = IntraTrailingBook::new();
        book.record_open(1, 1, 0, 1.0, 100.0);
        book.record_open(2, 2, 0, 1.0, 99.0);
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
        book.record_open(1, 1, 0, -1.0, 100.0);
        assert!(book.triggers(Some(config()), 99.4, 99.9).is_empty());
        assert!((book.positions[&1].stop_price.unwrap() - 100.2).abs() < 1e-9);
        assert_eq!(
            book.triggers(Some(config()), 99.4, 99.5),
            vec![(1, -1.0, "intra_take_profit")]
        );
        assert!(book.triggers(None, 0.0, 0.0).is_empty());
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
                    book.record_open(id, now, close, qv, 100.0);
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
}
