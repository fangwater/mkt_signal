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
    /// 可选的 open 端 client_order_id，作为外部稳定身份。
    /// None 表示该 lot 不绑定任何 open 订单（例如 NetQtyQueue 的纯净敞口用途）。
    pub open_client_order_id: Option<i64>,
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
        self.put_inner(ts, close_ts, qv, price, None)
    }

    /// 与 [`put`] 行为一致，但额外把 open 端 client_order_id 写入新追加的 lot；
    /// 若进入反向冲销路径，仅作普通 put 使用，不做身份合并。
    pub fn put_with_id(
        &mut self,
        ts: i64,
        close_ts: i64,
        qv: f64,
        price: f64,
        open_client_order_id: i64,
    ) -> NetQtyApplyResult {
        self.put_inner(ts, close_ts, qv, price, Some(open_client_order_id))
    }

    fn put_inner(
        &mut self,
        ts: i64,
        close_ts: i64,
        qv: f64,
        price: f64,
        open_client_order_id: Option<i64>,
    ) -> NetQtyApplyResult {
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
            self.insert_lot(ts, close_ts, appended_qv, price, open_client_order_id);
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

    /// 把开仓 terminal 形成的 pending hedge 量写入队列。同 open_client_order_id
    /// 的 lot 已存在则合并 qty / 加权价，否则插入新 lot。仅适用于同方向语义；
    /// 若新增 qv 与现有 lot 方向相反，回退到普通 put 行为。
    pub fn upsert_open_lot(
        &mut self,
        ts: i64,
        close_ts: i64,
        qv: f64,
        price: f64,
        open_client_order_id: i64,
    ) -> NetQtyApplyResult {
        if qv.abs() <= NET_QTY_EPS {
            return NetQtyApplyResult {
                matched_qty: 0.0,
                appended_qty: 0.0,
                net_qty: self.net_qty,
                direction: self.direction(),
            };
        }
        if let Some(existing_key) = self.find_key_by_open_id(open_client_order_id) {
            if let Some(existing) = self.lots.get(&existing_key).cloned() {
                if direction_from_signed_qty(existing.qv) == direction_from_signed_qty(qv) {
                    self.lots.remove(&existing_key);
                    let merged_qty = existing.qty + qv.abs();
                    let merged_price = if merged_qty > NET_QTY_EPS {
                        (existing.price * existing.qty + price * qv.abs()) / merged_qty
                    } else {
                        price
                    };
                    let merged_qv = signed_qty_for_direction(
                        direction_from_signed_qty(existing.qv),
                        merged_qty,
                    );
                    let merged_ts = existing.ts.min(ts);
                    let merged_close_ts = if existing.close_ts <= 0 || close_ts <= 0 {
                        0
                    } else {
                        existing.close_ts.min(close_ts)
                    };
                    self.insert_lot(
                        merged_ts,
                        merged_close_ts,
                        merged_qv,
                        merged_price,
                        Some(open_client_order_id),
                    );
                    self.net_qty += qv;
                    if self.net_qty.abs() <= NET_QTY_EPS {
                        self.net_qty = 0.0;
                        self.lots.clear();
                    }
                    return NetQtyApplyResult {
                        matched_qty: 0.0,
                        appended_qty: qv.abs(),
                        net_qty: self.net_qty,
                        direction: self.direction(),
                    };
                }
            }
        }
        self.put_inner(ts, close_ts, qv, price, Some(open_client_order_id))
    }

    /// 从待处理队列里借出一段已到期的同方向数量，用于挂单先占用 hedge 需求。
    ///
    /// `qv` 使用 pending 需求方向，而不是 hedge 订单方向。比如 open 买入形成 `+2` 的
    /// pending，挂 sell hedge 时这里借出的仍然是 `+2`。函数只做数量扣减，不表达成交。
    pub fn borrow(&mut self, now_ts: i64, qv: f64) -> TimedNetQtyBorrow {
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
            if oldest_lot.close_ts > 0 && oldest_lot.close_ts > now_ts {
                break;
            }
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
                open_client_order_id: oldest_lot.open_client_order_id,
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

    /// 释放未成交的借用数量。release 回来的数量按 close_ts=0 放回，下一轮优先 borrow。
    pub fn release(&mut self, now_ts: i64, qv: f64, price: f64) -> f64 {
        if qv.abs() <= NET_QTY_EPS {
            return 0.0;
        }

        self.put(now_ts, 0, qv, price);
        qv
    }

    /// 与 [`release`] 类似，但回写时绑定 open 端 client_order_id。
    /// 同 open_client_order_id 已存在则合并 qty / 加权价并强制 close_ts=0；
    /// 不存在则按 close_ts=0 插入新 lot 并写入身份。
    pub fn release_with_id(
        &mut self,
        now_ts: i64,
        qv: f64,
        price: f64,
        open_client_order_id: i64,
    ) -> f64 {
        if qv.abs() <= NET_QTY_EPS {
            return 0.0;
        }
        self.upsert_open_lot(now_ts, 0, qv, price, open_client_order_id);
        qv
    }

    /// 通过 open_client_order_id 查找队列中的 lot；用于上层对账。
    pub fn find_lot_by_open_id(
        &self,
        open_client_order_id: i64,
    ) -> Option<&TimedNetQtyLot> {
        self.lots
            .values()
            .find(|lot| lot.open_client_order_id == Some(open_client_order_id))
    }

    fn find_key_by_open_id(&self, open_client_order_id: i64) -> Option<(i64, i64, u64)> {
        self.lots
            .iter()
            .find(|(_, lot)| lot.open_client_order_id == Some(open_client_order_id))
            .map(|(key, _)| *key)
    }

    fn insert_lot(
        &mut self,
        ts: i64,
        close_ts: i64,
        qv: f64,
        price: f64,
        open_client_order_id: Option<i64>,
    ) {
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
                open_client_order_id,
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

        let borrowed = queue.borrow(150, 4.0);

        assert_eq!(borrowed.qv, 2.0);
        assert_eq!(borrowed.qty, 2.0);
        assert_eq!(borrowed.lots.len(), 1);
        assert_eq!(queue.net_qty(), 3.0);
        let lots = queue.lots();
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].close_ts, 200);
        assert_eq!(lots[0].qv, 3.0);
    }

    #[test]
    fn timed_queue_release_restores_borrow_as_due_qty() {
        let mut queue = TimedNetQtyQueue::new();
        queue.put(10, 100, 2.0, 100.0);

        let _borrowed = queue.borrow(100, 2.0);
        let released = queue.release(150, 0.75, 101.0);

        assert_eq!(released, 0.75);
        assert_eq!(queue.net_qty(), 0.75);
        assert_eq!(queue.due_qty(150), 0.75);
        assert_eq!(queue.lots()[0].close_ts, 0);

        let borrowed_again = queue.borrow(150, 0.5);
        assert_eq!(borrowed_again.qv, 0.5);
        assert_eq!(queue.net_qty(), 0.25);
        assert_eq!(queue.lots()[0].close_ts, 0);
    }

    #[test]
    fn timed_queue_borrow_before_close_ts_is_zero() {
        let mut queue = TimedNetQtyQueue::new();
        queue.put(10, 100, 2.0, 100.0);

        let borrowed = queue.borrow(99, 2.0);

        assert_eq!(borrowed.qty, 0.0);
        assert_eq!(queue.net_qty(), 2.0);
        assert_eq!(queue.due_qty(99), 0.0);
    }

    #[test]
    fn timed_queue_release_then_opposite_put_offsets_direction() {
        let mut queue = TimedNetQtyQueue::new();
        queue.put(10, 100, 2.0, 100.0);

        let _borrowed = queue.borrow(100, 2.0);
        let released = queue.release(150, 1.5, 101.0);
        let result = queue.put(160, 0, -1.0, 99.0);

        assert_eq!(released, 1.5);
        assert_eq!(result.matched_qty, 1.0);
        assert_eq!(result.appended_qty, 0.0);
        assert_eq!(queue.net_qty(), 0.5);
        let lots = queue.lots();
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].close_ts, 0);
        assert_eq!(lots[0].qv, 0.5);
    }

    #[test]
    fn put_with_id_records_open_client_order_id_on_lot() {
        let mut queue = TimedNetQtyQueue::new();
        queue.put_with_id(10, 100, 2.0, 100.0, 42);
        let lot = queue.find_lot_by_open_id(42).expect("lot bound to id=42");
        assert_eq!(lot.qty, 2.0);
        assert_eq!(lot.open_client_order_id, Some(42));
    }

    #[test]
    fn upsert_open_lot_merges_same_id_with_weighted_price() {
        let mut queue = TimedNetQtyQueue::new();
        queue.upsert_open_lot(10, 100, 1.0, 100.0, 42);
        queue.upsert_open_lot(20, 100, 0.5, 110.0, 42);
        assert_eq!(queue.len(), 1);
        let lot = queue.find_lot_by_open_id(42).unwrap();
        assert!((lot.qty - 1.5).abs() < 1e-9);
        assert!((lot.price - (100.0 + 110.0 * 0.5) / 1.5).abs() < 1e-9);
        assert!((queue.net_qty() - 1.5).abs() < 1e-9);
    }

    #[test]
    fn upsert_open_lot_inserts_new_when_id_unknown() {
        let mut queue = TimedNetQtyQueue::new();
        queue.upsert_open_lot(10, 100, 1.0, 100.0, 1);
        queue.upsert_open_lot(20, 200, 2.0, 105.0, 2);
        assert_eq!(queue.len(), 2);
        assert!(queue.find_lot_by_open_id(1).is_some());
        assert!(queue.find_lot_by_open_id(2).is_some());
    }

    #[test]
    fn release_with_id_writes_back_under_bound_id_with_close_ts_zero() {
        let mut queue = TimedNetQtyQueue::new();
        queue.put_with_id(10, 200, 3.0, 100.0, 42);
        let borrowed = queue.borrow(200, 2.0);
        assert_eq!(borrowed.qty, 2.0);
        assert_eq!(borrowed.lots[0].open_client_order_id, Some(42));
        assert!((queue.net_qty() - 1.0).abs() < 1e-9);

        // release 1.5 回到 OPEN_ID=42 的身份；既有 1.0 (close_ts=200) 应该合并到一起
        // 并且按照 release 语义 close_ts 强制变 0（立即 due）
        let released = queue.release_with_id(300, 1.5, 101.0, 42);
        assert!((released - 1.5).abs() < 1e-9);
        let lot = queue.find_lot_by_open_id(42).unwrap();
        assert!((lot.qty - 2.5).abs() < 1e-9);
        assert_eq!(lot.close_ts, 0);
    }
}
