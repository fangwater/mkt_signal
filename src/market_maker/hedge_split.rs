use crate::pre_trade::order_manager::Side;

#[derive(Debug, Clone, Copy)]
pub struct HedgeLevel {
    pub price: f64,
    pub qty_venue_one_hand: f64,
    pub qty_venue_tick: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct HedgeSplitOrder {
    pub level_index: usize,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
}

#[derive(Debug, Clone)]
pub struct HedgeSplitResult {
    pub orders: Vec<HedgeSplitOrder>,
    pub level_stats: Vec<HedgeSplitLevelStat>,
    pub remaining_qty_base: f64,
    pub total_qty_base: f64,
    pub total_usdt: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct HedgeSplitLevelStat {
    pub level_index: usize,
    pub price: f64,
    pub qty_venue_one_hand: f64,
    pub qty_base_one_hand: f64,
    pub hand_count: u64,
    pub order_qty_venue: f64,
    pub order_qty_base: f64,
}

pub fn split_hedge_orders_round_robin(
    side: Option<Side>,
    net_qty_base: f64,
    levels: &[HedgeLevel],
    qty_multiplier: f64,
) -> HedgeSplitResult {
    let Some(side) = side else {
        return HedgeSplitResult {
            orders: Vec::new(),
            level_stats: Vec::new(),
            remaining_qty_base: 0.0,
            total_qty_base: 0.0,
            total_usdt: 0.0,
        };
    };

    if net_qty_base.abs() <= 0.0 || levels.is_empty() {
        return HedgeSplitResult {
            orders: Vec::new(),
            level_stats: Vec::new(),
            remaining_qty_base: net_qty_base.abs(),
            total_qty_base: 0.0,
            total_usdt: 0.0,
        };
    }

    let multiplier = if qty_multiplier.is_finite() && qty_multiplier > 0.0 {
        qty_multiplier
    } else {
        1.0
    };

    let mut hand_base_qty: Vec<f64> = Vec::with_capacity(levels.len());
    for level in levels {
        let qty_venue = level.qty_venue_one_hand;
        if qty_venue.is_finite() && qty_venue > 0.0 {
            hand_base_qty.push(qty_venue * multiplier);
        } else {
            hand_base_qty.push(0.0);
        }
    }

    let mut hand_count: Vec<u64> = vec![0; levels.len()];
    let mut remaining_qty_base = net_qty_base.abs();
    let mut last_assigned_idx: Option<usize> = None;

    loop {
        let can_take_any = hand_base_qty
            .iter()
            .any(|q_base| *q_base > 0.0 && remaining_qty_base + 1e-12 >= *q_base);
        if !can_take_any {
            break;
        }

        for (idx, q_base) in hand_base_qty.iter().enumerate() {
            if *q_base <= 0.0 {
                continue;
            }
            if remaining_qty_base + 1e-12 >= *q_base {
                hand_count[idx] = hand_count[idx].saturating_add(1);
                remaining_qty_base -= *q_base;
                last_assigned_idx = Some(idx);
            }
        }
    }

    let mut tail_qty_base: Vec<f64> = vec![0.0; levels.len()];
    if remaining_qty_base > 1e-12 {
        let tail_idx = last_assigned_idx.or_else(|| hand_base_qty.iter().position(|q| *q > 0.0));
        if let Some(idx) = tail_idx {
            tail_qty_base[idx] = remaining_qty_base;
            remaining_qty_base = 0.0;
        }
    }

    let mut orders: Vec<HedgeSplitOrder> = Vec::new();
    let mut level_stats: Vec<HedgeSplitLevelStat> = Vec::with_capacity(levels.len());
    let mut total_qty_base = 0.0;
    let mut total_usdt = 0.0;
    for (idx, level) in levels.iter().enumerate() {
        let n = hand_count[idx] as f64;
        let tail_base = tail_qty_base[idx];
        let tail_qty_venue_raw = tail_base / multiplier;
        let tail_qty_venue = if level.qty_venue_tick.is_finite() && level.qty_venue_tick > 0.0 {
            (tail_qty_venue_raw / level.qty_venue_tick + 1e-12).floor() * level.qty_venue_tick
        } else {
            tail_qty_venue_raw
        };
        let tail_qty_base_aligned = tail_qty_venue * multiplier;
        let dropped_tail_base = (tail_base - tail_qty_base_aligned).max(0.0);
        remaining_qty_base += dropped_tail_base;

        let order_qty_venue = level.qty_venue_one_hand * n + tail_qty_venue;
        let order_qty_base = hand_base_qty[idx] * n + tail_qty_base_aligned;
        level_stats.push(HedgeSplitLevelStat {
            level_index: idx,
            price: level.price,
            qty_venue_one_hand: level.qty_venue_one_hand,
            qty_base_one_hand: hand_base_qty[idx],
            hand_count: hand_count[idx],
            order_qty_venue,
            order_qty_base,
        });
        let qty = order_qty_venue;
        if qty <= 0.0 {
            continue;
        }
        orders.push(HedgeSplitOrder {
            level_index: idx,
            side,
            price: level.price,
            qty,
        });
        total_qty_base += order_qty_base;
        total_usdt += level.price * qty;
    }

    HedgeSplitResult {
        orders,
        level_stats,
        remaining_qty_base,
        total_qty_base,
        total_usdt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_hedge_orders_round_robin_sub_hand_sends_partial_order() {
        let levels = vec![HedgeLevel {
            price: 100.0,
            qty_venue_one_hand: 1.0,
            qty_venue_tick: 0.1,
        }];

        let result = split_hedge_orders_round_robin(Some(Side::Buy), 0.6, &levels, 1.0);

        assert_eq!(result.orders.len(), 1);
        assert_eq!(result.orders[0].level_index, 0);
        assert!((result.orders[0].qty - 0.6).abs() < 1e-12);
        assert!((result.total_qty_base - 0.6).abs() < 1e-12);
        assert!(result.remaining_qty_base.abs() < 1e-12);
    }

    #[test]
    fn test_split_hedge_orders_round_robin_puts_remainder_on_last_round_order() {
        let levels = vec![
            HedgeLevel {
                price: 100.0,
                qty_venue_one_hand: 1.0,
                qty_venue_tick: 0.1,
            },
            HedgeLevel {
                price: 101.0,
                qty_venue_one_hand: 1.0,
                qty_venue_tick: 0.1,
            },
        ];

        let result = split_hedge_orders_round_robin(Some(Side::Sell), 2.5, &levels, 1.0);

        assert_eq!(result.orders.len(), 2);
        assert_eq!(result.orders[0].level_index, 0);
        assert!((result.orders[0].qty - 1.0).abs() < 1e-12);
        assert_eq!(result.orders[1].level_index, 1);
        assert!((result.orders[1].qty - 1.5).abs() < 1e-12);
        assert!((result.total_qty_base - 2.5).abs() < 1e-12);
        assert!(result.remaining_qty_base.abs() < 1e-12);
    }

    #[test]
    fn test_split_hedge_orders_round_robin_drops_sub_tick_tail_and_keeps_remainder() {
        let levels = vec![HedgeLevel {
            price: 100.0,
            qty_venue_one_hand: 7.0,
            qty_venue_tick: 1.0,
        }];

        // contract multiplier = 0.1 base/contract, so 0.27 base tail = 2.7 contracts.
        // tail should be floor-aligned to 2 contracts, with 0.07 base left for next round.
        let result = split_hedge_orders_round_robin(Some(Side::Sell), 0.27, &levels, 0.1);

        assert_eq!(result.orders.len(), 1);
        assert!((result.orders[0].qty - 2.0).abs() < 1e-12);
        assert!((result.total_qty_base - 0.2).abs() < 1e-12);
        assert!((result.remaining_qty_base - 0.07).abs() < 1e-12);
    }
}
