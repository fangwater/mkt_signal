use crate::pre_trade::order_manager::Side;

#[derive(Debug, Clone, Copy)]
pub struct HedgeLevel {
    pub price: f64,
    pub qty_venue_one_hand: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct HedgeSplitOrder {
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
            }
        }
    }

    let mut orders: Vec<HedgeSplitOrder> = Vec::new();
    let mut level_stats: Vec<HedgeSplitLevelStat> = Vec::with_capacity(levels.len());
    let mut total_qty_base = 0.0;
    let mut total_usdt = 0.0;
    for (idx, level) in levels.iter().enumerate() {
        let n = hand_count[idx] as f64;
        let order_qty_venue = level.qty_venue_one_hand * n;
        let order_qty_base = hand_base_qty[idx] * n;
        level_stats.push(HedgeSplitLevelStat {
            level_index: idx,
            price: level.price,
            qty_venue_one_hand: level.qty_venue_one_hand,
            qty_base_one_hand: hand_base_qty[idx],
            hand_count: hand_count[idx],
            order_qty_venue,
            order_qty_base,
        });
        if n <= 0.0 {
            continue;
        }
        let qty = order_qty_venue;
        if qty <= 0.0 {
            continue;
        }
        orders.push(HedgeSplitOrder {
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
