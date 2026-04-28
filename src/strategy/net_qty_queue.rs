use std::collections::BTreeMap;

pub const NET_QTY_EPS: f64 = 1e-12;

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

#[derive(Debug, Clone, PartialEq)]
pub struct TimedNetQtyLot {
    pub ts: i64,
    pub close_ts: i64,
    pub qv: f64,
    pub qty: f64,
    pub price: f64,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct TimedNetQtyBorrow {
    pub qv: f64,
    pub qty: f64,
    pub lots: Vec<TimedNetQtyLot>,
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
    inner: TimedNetQtyQueue,
}

#[derive(Debug, Default, Clone)]
pub struct TimedNetQtyQueue {
    lots: BTreeMap<(i64, i64, u64), TimedNetQtyLot>,
    next_seq: u64,
    net_qty: f64,
}

impl NetQtyQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn net_qty(&self) -> f64 {
        self.inner.net_qty()
    }

    pub fn direction(&self) -> NetQtyDirection {
        self.inner.direction()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn lots(&self) -> Vec<NetQtyLot> {
        self.inner
            .lots()
            .into_iter()
            .map(|lot| NetQtyLot {
                ts: lot.ts,
                qv: lot.qv,
                qty: lot.qty,
                price: lot.price,
            })
            .collect()
    }

    pub fn weighted_avg_price(&self) -> Option<f64> {
        self.inner.weighted_avg_price()
    }

    pub fn apply_fill(&mut self, ts: i64, qv: f64, price: f64) -> NetQtyApplyResult {
        self.inner.put(ts, ts, qv, price)
    }
}

impl TimedNetQtyQueue {
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

    pub fn lots(&self) -> Vec<TimedNetQtyLot> {
        self.lots.values().cloned().collect()
    }

    pub fn due_lots(&self, now_ts: i64) -> Vec<TimedNetQtyLot> {
        self.lots
            .values()
            .filter(|lot| lot.close_ts <= 0 || lot.close_ts <= now_ts)
            .cloned()
            .collect()
    }

    pub fn due_qty(&self, now_ts: i64) -> f64 {
        let due = self
            .lots
            .values()
            .filter(|lot| lot.close_ts <= 0 || lot.close_ts <= now_ts)
            .map(|lot| lot.qv)
            .sum::<f64>();
        if due.abs() <= NET_QTY_EPS {
            0.0
        } else {
            due
        }
    }

    pub fn weighted_avg_price(&self) -> Option<f64> {
        let (total_notional, total_qty) =
            self.lots.values().fold((0.0, 0.0), |(notional, qty), lot| {
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

    pub fn put(&mut self, ts: i64, close_ts: i64, qv: f64, price: f64) -> NetQtyApplyResult {
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
            self.insert_lot(ts, close_ts, appended_qv, price);
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

    /// 从待处理队列里借出一段同方向数量，用于挂单先占用 hedge 需求。
    ///
    /// `qv` 使用 pending 需求方向，而不是 hedge 订单方向。比如 open 买入形成 `+2` 的
    /// pending，挂 sell hedge 时这里借出的仍然是 `+2`。函数只做数量扣减，不表达成交。
    pub fn borrow(&mut self, qv: f64) -> TimedNetQtyBorrow {
        if qv.abs() <= NET_QTY_EPS {
            return TimedNetQtyBorrow::default();
        }

        let direction = direction_from_signed_qty(qv);
        if direction == NetQtyDirection::Flat || self.direction() != direction {
            return TimedNetQtyBorrow::default();
        }

        let mut remaining_qty = qv.abs();
        let mut borrowed_qty = 0.0;
        let mut borrowed_lots = Vec::new();

        while remaining_qty > NET_QTY_EPS {
            let Some(oldest_key) = self.lots.keys().next().copied() else {
                break;
            };

            let Some(oldest_lot) = self.lots.get(&oldest_key).cloned() else {
                break;
            };
            if direction_from_signed_qty(oldest_lot.qv) != direction {
                break;
            }

            let take_qty = remaining_qty.min(oldest_lot.qty);
            if take_qty <= NET_QTY_EPS {
                self.lots.remove(&oldest_key);
                continue;
            }

            let borrowed_qv = signed_qty_for_direction(direction, take_qty);
            borrowed_lots.push(TimedNetQtyLot {
                ts: oldest_lot.ts,
                close_ts: oldest_lot.close_ts,
                qv: borrowed_qv,
                qty: take_qty,
                price: oldest_lot.price,
            });

            if take_qty + NET_QTY_EPS < oldest_lot.qty {
                if let Some(lot) = self.lots.get_mut(&oldest_key) {
                    lot.qty = oldest_lot.qty - take_qty;
                    lot.qv = signed_qty_for_direction(direction, lot.qty);
                }
            } else {
                self.lots.remove(&oldest_key);
            }

            borrowed_qty += take_qty;
            remaining_qty -= take_qty;
        }

        let borrowed_qv = signed_qty_for_direction(direction, borrowed_qty);
        self.net_qty -= borrowed_qv;
        if self.net_qty.abs() <= NET_QTY_EPS {
            self.net_qty = 0.0;
            self.lots.clear();
        }

        TimedNetQtyBorrow {
            qv: borrowed_qv,
            qty: borrowed_qty,
            lots: borrowed_lots,
        }
    }

    /// 释放未成交的借用数量。已到 close_ts 的借用不再加回 pending，按 0 处理。
    pub fn release(
        &mut self,
        now_ts: i64,
        borrowed_lots: &[TimedNetQtyLot],
        release_qty: f64,
        price: f64,
    ) -> f64 {
        if release_qty <= NET_QTY_EPS {
            return 0.0;
        }

        let mut remaining_qty = release_qty;
        let mut released_qv = 0.0;
        for lot in borrowed_lots {
            if remaining_qty <= NET_QTY_EPS {
                break;
            }
            if lot.close_ts <= 0 || lot.close_ts <= now_ts {
                continue;
            }

            let release_qty = remaining_qty.min(lot.qty);
            if release_qty <= NET_QTY_EPS {
                continue;
            }

            let direction = direction_from_signed_qty(lot.qv);
            let qv = signed_qty_for_direction(direction, release_qty);
            self.put(now_ts, lot.close_ts, qv, price);
            released_qv += qv;
            remaining_qty -= release_qty;
        }

        if released_qv.abs() <= NET_QTY_EPS {
            0.0
        } else {
            released_qv
        }
    }

    fn insert_lot(&mut self, ts: i64, close_ts: i64, qv: f64, price: f64) {
        let qty = qv.abs();
        if qty <= NET_QTY_EPS {
            return;
        }
        self.next_seq = self.next_seq.wrapping_add(1);
        self.lots.insert(
            (close_ts, ts, self.next_seq),
            TimedNetQtyLot {
                ts,
                close_ts,
                qv,
                qty,
                price,
            },
        );
    }
}

pub fn direction_from_signed_qty(qv: f64) -> NetQtyDirection {
    if qv > NET_QTY_EPS {
        NetQtyDirection::Positive
    } else if qv < -NET_QTY_EPS {
        NetQtyDirection::Negative
    } else {
        NetQtyDirection::Flat
    }
}

pub fn signed_qty_for_direction(direction: NetQtyDirection, qty: f64) -> f64 {
    match direction {
        NetQtyDirection::Positive => qty.abs(),
        NetQtyDirection::Negative => -qty.abs(),
        NetQtyDirection::Flat => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::{NetQtyDirection, NetQtyQueue, TimedNetQtyQueue};

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

    #[test]
    fn timed_queue_tracks_due_qty_by_close_ts() {
        let mut queue = TimedNetQtyQueue::new();
        queue.put(10, 100, 2.0, 100.0);
        queue.put(20, 200, 3.0, 101.0);

        assert_eq!(queue.due_qty(99), 0.0);
        assert_eq!(queue.due_qty(100), 2.0);
        assert_eq!(queue.due_qty(250), 5.0);
    }

    #[test]
    fn timed_queue_offsets_oldest_close_ts_first() {
        let mut queue = TimedNetQtyQueue::new();
        queue.put(10, 200, 2.0, 100.0);
        queue.put(20, 100, 3.0, 101.0);
        let result = queue.put(30, 0, -4.0, 102.0);

        assert_eq!(result.matched_qty, 4.0);
        assert_eq!(queue.net_qty(), 1.0);
        let lots = queue.lots();
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].close_ts, 200);
        assert_eq!(lots[0].qty, 1.0);
    }

    #[test]
    fn timed_queue_borrow_reserves_pending_qty() {
        let mut queue = TimedNetQtyQueue::new();
        queue.put(10, 100, 2.0, 100.0);
        queue.put(20, 200, 3.0, 101.0);

        let borrowed = queue.borrow(4.0);

        assert_eq!(borrowed.qv, 4.0);
        assert_eq!(borrowed.qty, 4.0);
        assert_eq!(borrowed.lots.len(), 2);
        assert_eq!(queue.net_qty(), 1.0);
        let lots = queue.lots();
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].close_ts, 200);
        assert_eq!(lots[0].qv, 1.0);
    }

    #[test]
    fn timed_queue_release_restores_unexpired_borrow() {
        let mut queue = TimedNetQtyQueue::new();
        queue.put(10, 100, 2.0, 100.0);

        let borrowed = queue.borrow(2.0);
        let released = queue.release(50, &borrowed.lots, 0.75, 101.0);

        assert_eq!(released, 0.75);
        assert_eq!(queue.net_qty(), 0.75);
        assert_eq!(queue.due_qty(99), 0.0);
        assert_eq!(queue.due_qty(100), 0.75);
    }

    #[test]
    fn timed_queue_release_after_close_ts_is_zero() {
        let mut queue = TimedNetQtyQueue::new();
        queue.put(10, 100, 2.0, 100.0);

        let borrowed = queue.borrow(2.0);
        let released = queue.release(100, &borrowed.lots, 2.0, 101.0);

        assert_eq!(released, 0.0);
        assert_eq!(queue.net_qty(), 0.0);
        assert!(queue.is_empty());
    }
}
