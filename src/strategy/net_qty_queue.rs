use std::collections::BTreeMap;

const NET_QTY_EPS: f64 = 1e-12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetQtyDirection {
    Positive,
    Negative,
    Flat,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NetQtyLot {
    pub ts: i64,
    pub qv: f64,
    pub qty: f64,
    pub price: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NetQtyApplyResult {
    pub matched_qty: f64,
    pub appended_qty: f64,
    pub net_qty: f64,
    pub direction: NetQtyDirection,
}

#[derive(Debug, Default, Clone)]
pub struct NetQtyQueue {
    lots: BTreeMap<(i64, u64), NetQtyLot>,
    next_seq: u64,
    net_qty: f64,
}

impl NetQtyQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn net_qty(&self) -> f64 {
        self.net_qty
    }

    pub fn direction(&self) -> NetQtyDirection {
        direction_from_signed_qty(self.net_qty)
    }

    pub fn len(&self) -> usize {
        self.lots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lots.is_empty()
    }

    pub fn lots(&self) -> Vec<NetQtyLot> {
        self.lots.values().cloned().collect()
    }

    pub fn weighted_avg_price(&self) -> Option<f64> {
        let (total_notional, total_qty) =
            self.lots
                .values()
                .fold((0.0, 0.0), |(notional, qty), lot| {
                    if lot.qty <= NET_QTY_EPS {
                        (notional, qty)
                    } else {
                        (notional + lot.price * lot.qty, qty + lot.qty)
                    }
                });
        if total_qty > NET_QTY_EPS {
            Some(total_notional / total_qty)
        } else {
            None
        }
    }

    pub fn apply_fill(&mut self, ts: i64, qv: f64, price: f64) -> NetQtyApplyResult {
        if qv.abs() <= NET_QTY_EPS {
            return NetQtyApplyResult {
                matched_qty: 0.0,
                appended_qty: 0.0,
                net_qty: self.net_qty,
                direction: self.direction(),
            };
        }

        let incoming_direction = direction_from_signed_qty(qv);
        let mut remaining_qty = qv.abs();
        let mut matched_qty = 0.0;

        if self.direction() != NetQtyDirection::Flat && self.direction() != incoming_direction {
            while remaining_qty > NET_QTY_EPS {
                let Some(oldest_key) = self.lots.keys().next().copied() else {
                    break;
                };

                let oldest_qty = self.lots.get(&oldest_key).map(|lot| lot.qty).unwrap_or(0.0);
                if oldest_qty <= NET_QTY_EPS {
                    self.lots.remove(&oldest_key);
                    continue;
                }

                if remaining_qty + NET_QTY_EPS < oldest_qty {
                    let existing_direction = self.direction();
                    if let Some(oldest_lot) = self.lots.get_mut(&oldest_key) {
                        oldest_lot.qty = oldest_qty - remaining_qty;
                        oldest_lot.qv =
                            signed_qty_for_direction(existing_direction, oldest_lot.qty);
                    }
                    matched_qty += remaining_qty;
                    remaining_qty = 0.0;
                    break;
                }

                matched_qty += oldest_qty;
                remaining_qty -= oldest_qty;
                self.lots.remove(&oldest_key);
            }
        }

        let appended_qty = if remaining_qty > NET_QTY_EPS {
            let appended_qv = signed_qty_for_direction(incoming_direction, remaining_qty);
            self.insert_lot(ts, appended_qv, price);
            remaining_qty
        } else {
            0.0
        };

        self.net_qty += qv;
        if self.net_qty.abs() <= NET_QTY_EPS {
            self.net_qty = 0.0;
            self.lots.clear();
        }

        NetQtyApplyResult {
            matched_qty,
            appended_qty,
            net_qty: self.net_qty,
            direction: self.direction(),
        }
    }

    fn insert_lot(&mut self, ts: i64, qv: f64, price: f64) {
        let qty = qv.abs();
        if qty <= NET_QTY_EPS {
            return;
        }
        self.next_seq = self.next_seq.wrapping_add(1);
        self.lots
            .insert((ts, self.next_seq), NetQtyLot { ts, qv, qty, price });
    }
}

fn direction_from_signed_qty(qv: f64) -> NetQtyDirection {
    if qv > NET_QTY_EPS {
        NetQtyDirection::Positive
    } else if qv < -NET_QTY_EPS {
        NetQtyDirection::Negative
    } else {
        NetQtyDirection::Flat
    }
}

fn signed_qty_for_direction(direction: NetQtyDirection, qty: f64) -> f64 {
    match direction {
        NetQtyDirection::Positive => qty.abs(),
        NetQtyDirection::Negative => -qty.abs(),
        NetQtyDirection::Flat => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::{NetQtyDirection, NetQtyQueue};

    #[test]
    fn same_direction_lots_are_sorted_by_time() {
        let mut queue = NetQtyQueue::new();
        queue.apply_fill(30, 2.0, 103.0);
        queue.apply_fill(10, 1.0, 101.0);
        queue.apply_fill(20, 3.0, 102.0);

        let lots = queue.lots();
        assert_eq!(queue.direction(), NetQtyDirection::Positive);
        assert_eq!(queue.net_qty(), 6.0);
        assert_eq!(lots.len(), 3);
        assert_eq!(lots[0].ts, 10);
        assert_eq!(lots[0].qv, 1.0);
        assert_eq!(lots[1].ts, 20);
        assert_eq!(lots[1].qv, 3.0);
        assert_eq!(lots[2].ts, 30);
        assert_eq!(lots[2].qv, 2.0);
    }

    #[test]
    fn opposite_fill_offsets_oldest_lots_fifo() {
        let mut queue = NetQtyQueue::new();
        queue.apply_fill(10, 2.0, 100.0);
        queue.apply_fill(20, 3.0, 101.0);

        let result = queue.apply_fill(30, -4.0, 99.0);
        let lots = queue.lots();

        assert_eq!(result.matched_qty, 4.0);
        assert_eq!(result.appended_qty, 0.0);
        assert_eq!(queue.direction(), NetQtyDirection::Positive);
        assert_eq!(queue.net_qty(), 1.0);
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].ts, 20);
        assert_eq!(lots[0].qv, 1.0);
        assert_eq!(lots[0].price, 101.0);
    }

    #[test]
    fn remaining_opposite_fill_is_appended_as_new_side() {
        let mut queue = NetQtyQueue::new();
        queue.apply_fill(10, 2.0, 100.0);
        queue.apply_fill(20, 1.0, 101.0);

        let result = queue.apply_fill(30, -5.0, 99.0);
        let lots = queue.lots();

        assert_eq!(result.matched_qty, 3.0);
        assert_eq!(result.appended_qty, 2.0);
        assert_eq!(queue.direction(), NetQtyDirection::Negative);
        assert_eq!(queue.net_qty(), -2.0);
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].ts, 30);
        assert_eq!(lots[0].qv, -2.0);
        assert_eq!(lots[0].qty, 2.0);
        assert_eq!(lots[0].price, 99.0);
    }

    #[test]
    fn equal_and_opposite_fills_flatten_queue() {
        let mut queue = NetQtyQueue::new();
        queue.apply_fill(10, -1.5, 100.0);
        let result = queue.apply_fill(20, 1.5, 101.0);

        assert_eq!(result.matched_qty, 1.5);
        assert_eq!(result.appended_qty, 0.0);
        assert_eq!(queue.direction(), NetQtyDirection::Flat);
        assert_eq!(queue.net_qty(), 0.0);
        assert!(queue.is_empty());
    }

    #[test]
    fn same_timestamp_keeps_fifo_in_insertion_order() {
        let mut queue = NetQtyQueue::new();
        queue.apply_fill(10, 1.0, 100.0);
        queue.apply_fill(10, 2.0, 101.0);
        queue.apply_fill(20, -1.5, 99.0);

        let lots = queue.lots();
        assert_eq!(queue.direction(), NetQtyDirection::Positive);
        assert_eq!(queue.net_qty(), 1.5);
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].ts, 10);
        assert_eq!(lots[0].qv, 1.5);
        assert_eq!(lots[0].price, 101.0);
    }

    #[test]
    fn weighted_avg_price_uses_absolute_lot_qty() {
        let mut queue = NetQtyQueue::new();
        queue.apply_fill(10, 1.0, 100.0);
        queue.apply_fill(20, 3.0, 200.0);

        assert_eq!(queue.weighted_avg_price(), Some(175.0));
    }

    #[test]
    fn weighted_avg_price_tracks_remaining_fifo_lots() {
        let mut queue = NetQtyQueue::new();
        queue.apply_fill(10, 2.0, 100.0);
        queue.apply_fill(20, 3.0, 200.0);
        queue.apply_fill(30, -4.0, 90.0);

        assert_eq!(queue.weighted_avg_price(), Some(200.0));
    }

    #[test]
    fn weighted_avg_price_is_direction_agnostic() {
        let mut queue = NetQtyQueue::new();
        queue.apply_fill(10, -1.0, 100.0);
        queue.apply_fill(20, -3.0, 200.0);

        assert_eq!(queue.weighted_avg_price(), Some(175.0));
    }

    #[test]
    fn weighted_avg_price_is_none_for_flat_queue() {
        let mut queue = NetQtyQueue::new();
        queue.apply_fill(10, 1.0, 100.0);
        queue.apply_fill(20, -1.0, 110.0);

        assert_eq!(queue.weighted_avg_price(), None);
    }
}
