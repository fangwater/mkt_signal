use ahash::RandomState;
use std::collections::{HashMap, VecDeque};

pub type OrderId = i64;
pub type PriceKey = i64;
pub type Qty = f64;

const DEFAULT_EPS: Qty = 1e-12;

type FastHashMap<K, V> = HashMap<K, V, RandomState>;

#[inline]
fn fast_hash_map<K, V>() -> FastHashMap<K, V> {
    HashMap::with_hasher(RandomState::new())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    #[inline]
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BookSide {
    Bid,
    Ask,
}

impl BookSide {
    #[inline]
    pub fn from_order_side(side: Side) -> Self {
        match side {
            Side::Buy => Self::Bid,
            Side::Sell => Self::Ask,
        }
    }

    #[inline]
    pub fn consumed_by_trade_side(trade_side: Side) -> Self {
        match trade_side {
            Side::Buy => Self::Ask,
            Side::Sell => Self::Bid,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LevelKey {
    pub symbol: String,
    pub side: BookSide,
    pub price_key: PriceKey,
}

impl LevelKey {
    pub fn new(symbol: impl Into<String>, side: BookSide, price_key: PriceKey) -> Self {
        Self {
            symbol: normalize_symbol(symbol.into()),
            side,
            price_key,
        }
    }

    pub fn from_order(symbol: impl Into<String>, side: Side, price_key: PriceKey) -> Self {
        Self::new(symbol, BookSide::from_order_side(side), price_key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FrontQtyMode {
    /// `visible_level_qty` already includes this order, so subtract the order's
    /// own remaining quantity before using it as queue-ahead.
    LevelIncludesOwnOrder,
    /// `visible_level_qty` is the queue visible before this order entered.
    LevelExcludesOwnOrder,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TrackedOrder {
    pub order_id: OrderId,
    pub symbol: String,
    pub side: Side,
    pub price_key: PriceKey,
    pub initial_qty: Qty,
    pub remaining_qty: Qty,
    pub public_consumed_own_qty: Qty,
    pub inpos: Qty,
    pub last_cumulative_filled_qty: Qty,
}

impl TrackedOrder {
    #[inline]
    pub fn level_key(&self) -> LevelKey {
        LevelKey::from_order(self.symbol.clone(), self.side, self.price_key)
    }

    #[inline]
    pub fn queue_remaining_qty(&self) -> Qty {
        non_negative(self.remaining_qty - self.public_consumed_own_qty)
    }

    #[inline]
    pub fn backlen(&self, current_tlen: Qty) -> Qty {
        non_negative(current_tlen - self.inpos - self.queue_remaining_qty())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderSnapshot {
    pub order_id: OrderId,
    pub symbol: String,
    pub side: Side,
    pub price_key: PriceKey,
    pub initial_qty: Qty,
    pub remaining_qty: Qty,
    pub queue_remaining_qty: Qty,
    pub public_consumed_own_qty: Qty,
    pub inpos: Qty,
    pub backlen: Qty,
    pub tlen: Qty,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddOrder {
    pub order_id: OrderId,
    pub symbol: String,
    pub side: Side,
    pub price_key: PriceKey,
    pub qty: Qty,
    pub visible_level_qty: Qty,
    pub front_qty_mode: FrontQtyMode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FillUpdate {
    pub order_id: OrderId,
    pub cumulative_filled_qty: Qty,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LevelUpdate {
    pub symbol: String,
    pub side: BookSide,
    pub price_key: PriceKey,
    pub old_qty: Qty,
    pub new_qty: Qty,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicTrade {
    pub symbol: String,
    /// Aggressor side: buy consumes asks, sell consumes bids.
    pub side: Side,
    pub price_key: PriceKey,
    pub qty: Qty,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LevelSummary {
    pub level: LevelKey,
    pub current_tlen: Qty,
    pub tracked_orders: usize,
}

#[derive(Debug, Clone)]
pub struct QueuePositionEngine {
    orders: FastHashMap<OrderId, TrackedOrder>,
    level_orders: FastHashMap<LevelKey, VecDeque<OrderId>>,
    level_qty: FastHashMap<LevelKey, Qty>,
    eps: Qty,
}

impl Default for QueuePositionEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl QueuePositionEngine {
    pub fn new() -> Self {
        Self {
            orders: fast_hash_map(),
            level_orders: fast_hash_map(),
            level_qty: fast_hash_map(),
            eps: DEFAULT_EPS,
        }
    }

    pub fn with_epsilon(eps: Qty) -> Self {
        let mut engine = Self::new();
        if eps.is_finite() && eps >= 0.0 {
            engine.eps = eps;
        }
        engine
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    #[inline]
    pub fn contains_order(&self, order_id: OrderId) -> bool {
        self.orders.contains_key(&order_id)
    }

    pub fn level_qty(&self, level: &LevelKey) -> Qty {
        self.level_qty.get(level).copied().unwrap_or(0.0)
    }

    pub fn tracked_order(&self, order_id: OrderId) -> Option<&TrackedOrder> {
        self.orders.get(&order_id)
    }

    pub fn order_snapshot(&self, order_id: OrderId) -> Option<OrderSnapshot> {
        let order = self.orders.get(&order_id)?;
        let level = order.level_key();
        let tlen = self.level_qty(&level);
        Some(OrderSnapshot {
            order_id: order.order_id,
            symbol: order.symbol.clone(),
            side: order.side,
            price_key: order.price_key,
            initial_qty: order.initial_qty,
            remaining_qty: order.remaining_qty,
            queue_remaining_qty: order.queue_remaining_qty(),
            public_consumed_own_qty: order.public_consumed_own_qty,
            inpos: order.inpos,
            backlen: order.backlen(tlen),
            tlen,
        })
    }

    pub fn order_snapshots(&self) -> Vec<OrderSnapshot> {
        self.orders
            .keys()
            .filter_map(|order_id| self.order_snapshot(*order_id))
            .collect()
    }

    pub fn level_summary(&self, level: &LevelKey) -> LevelSummary {
        LevelSummary {
            level: level.clone(),
            current_tlen: self.level_qty(level),
            tracked_orders: self
                .level_orders
                .get(level)
                .map(|orders| orders.len())
                .unwrap_or(0),
        }
    }

    pub fn add_order(&mut self, input: AddOrder) -> bool {
        if input.order_id <= 0
            || !valid_positive(input.qty)
            || !valid_non_negative(input.visible_level_qty)
        {
            return false;
        }
        if self.orders.contains_key(&input.order_id) {
            return false;
        }

        let symbol = normalize_symbol(input.symbol);
        if symbol.is_empty() {
            return false;
        }

        let level = LevelKey::from_order(symbol.clone(), input.side, input.price_key);
        self.level_qty
            .insert(level.clone(), input.visible_level_qty);

        let inpos = match input.front_qty_mode {
            FrontQtyMode::LevelIncludesOwnOrder => {
                non_negative(input.visible_level_qty - input.qty)
            }
            FrontQtyMode::LevelExcludesOwnOrder => input.visible_level_qty,
        };

        let order = TrackedOrder {
            order_id: input.order_id,
            symbol,
            side: input.side,
            price_key: input.price_key,
            initial_qty: input.qty,
            remaining_qty: input.qty,
            public_consumed_own_qty: 0.0,
            inpos,
            last_cumulative_filled_qty: 0.0,
        };

        self.level_orders
            .entry(level)
            .or_default()
            .push_back(input.order_id);
        self.orders.insert(input.order_id, order);
        true
    }

    pub fn remove_order(&mut self, order_id: OrderId) -> Option<TrackedOrder> {
        let order = self.orders.remove(&order_id)?;
        let level = order.level_key();
        if let Some(queue) = self.level_orders.get_mut(&level) {
            queue.retain(|id| *id != order_id);
            if queue.is_empty() {
                self.level_orders.remove(&level);
                self.level_qty.remove(&level);
            }
        }
        Some(order)
    }

    pub fn apply_fill_update(&mut self, update: FillUpdate) -> Option<TrackedOrder> {
        if !valid_non_negative(update.cumulative_filled_qty) {
            return None;
        }
        let order = self.orders.get_mut(&update.order_id)?;
        if update.cumulative_filled_qty + self.eps < order.last_cumulative_filled_qty {
            return None;
        }

        order.last_cumulative_filled_qty = update.cumulative_filled_qty;
        order.remaining_qty = non_negative(order.initial_qty - update.cumulative_filled_qty);
        order.public_consumed_own_qty = order.public_consumed_own_qty.min(order.remaining_qty);
        if order.remaining_qty <= self.eps {
            return self.remove_order(update.order_id);
        }
        None
    }

    pub fn apply_level_update(&mut self, update: LevelUpdate) {
        if !valid_non_negative(update.old_qty) || !valid_non_negative(update.new_qty) {
            return;
        }

        let level = LevelKey::new(update.symbol, update.side, update.price_key);
        let Some(current_qty) = self.level_qty.get_mut(&level) else {
            return;
        };
        *current_qty = update.new_qty;
        let decrease = update.old_qty - update.new_qty;
        if decrease <= self.eps {
            return;
        }

        self.allocate_level_decrease(&level, update.old_qty, decrease);
    }

    pub fn apply_level_qty(
        &mut self,
        symbol: impl Into<String>,
        side: BookSide,
        price_key: PriceKey,
        new_qty: Qty,
    ) {
        if !valid_non_negative(new_qty) {
            return;
        }
        let level = LevelKey::new(symbol, side, price_key);
        let old_qty = self.level_qty(&level);
        self.apply_level_update(LevelUpdate {
            symbol: level.symbol.clone(),
            side: level.side,
            price_key: level.price_key,
            old_qty,
            new_qty,
        });
    }

    pub fn apply_public_trade(&mut self, trade: PublicTrade) {
        if !valid_positive(trade.qty) {
            return;
        }

        let level = LevelKey::new(
            trade.symbol,
            BookSide::consumed_by_trade_side(trade.side),
            trade.price_key,
        );
        let mut remaining_trade_qty = trade.qty;
        let Some(order_ids) = self.level_orders.get(&level).cloned() else {
            return;
        };

        for order_id in order_ids {
            if remaining_trade_qty <= self.eps {
                break;
            }
            let Some(order) = self.orders.get_mut(&order_id) else {
                continue;
            };
            let consumed_front = order.inpos.min(remaining_trade_qty);
            order.inpos -= consumed_front;
            remaining_trade_qty -= consumed_front;

            if remaining_trade_qty <= self.eps {
                break;
            }

            let queue_remaining_qty =
                non_negative(order.remaining_qty - order.public_consumed_own_qty);
            let own_queue_qty = queue_remaining_qty.min(remaining_trade_qty);
            order.public_consumed_own_qty += own_queue_qty;
            remaining_trade_qty -= own_queue_qty;
        }
    }

    fn allocate_level_decrease(&mut self, level: &LevelKey, old_qty: Qty, decrease: Qty) {
        let Some(order_ids) = self.level_orders.get(level).cloned() else {
            return;
        };

        for order_id in order_ids {
            let Some(order) = self.orders.get_mut(&order_id) else {
                continue;
            };
            if order.inpos <= self.eps {
                continue;
            }

            let backlen = non_negative(old_qty - order.inpos - order.queue_remaining_qty());
            let external_qty = order.inpos + backlen;
            if external_qty <= self.eps {
                continue;
            }

            let front_decrease = decrease * order.inpos / external_qty;
            order.inpos = non_negative(order.inpos - front_decrease);
        }
    }
}

fn normalize_symbol(symbol: String) -> String {
    symbol.trim().to_ascii_uppercase()
}

#[inline]
fn valid_positive(value: Qty) -> bool {
    value.is_finite() && value > 0.0
}

#[inline]
fn valid_non_negative(value: Qty) -> bool {
    value.is_finite() && value >= 0.0
}

#[inline]
fn non_negative(value: Qty) -> Qty {
    if value.is_finite() && value > 0.0 {
        value
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_sell(engine: &mut QueuePositionEngine, order_id: OrderId, qty: Qty, visible: Qty) {
        assert!(engine.add_order(AddOrder {
            order_id,
            symbol: "btcusdt".to_string(),
            side: Side::Sell,
            price_key: 100,
            qty,
            visible_level_qty: visible,
            front_qty_mode: FrontQtyMode::LevelExcludesOwnOrder,
        }));
    }

    #[test]
    fn add_order_initializes_inpos_from_visible_level() {
        let mut engine = QueuePositionEngine::new();
        add_sell(&mut engine, 1, 2.0, 10.0);

        let snapshot = engine.order_snapshot(1).unwrap();
        assert_eq!(snapshot.symbol, "BTCUSDT");
        assert_eq!(snapshot.inpos, 10.0);
        assert_eq!(snapshot.remaining_qty, 2.0);
        assert_eq!(snapshot.backlen, 0.0);
    }

    #[test]
    fn add_order_can_subtract_own_qty_when_level_already_includes_it() {
        let mut engine = QueuePositionEngine::new();
        assert!(engine.add_order(AddOrder {
            order_id: 1,
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            price_key: 100,
            qty: 2.0,
            visible_level_qty: 12.0,
            front_qty_mode: FrontQtyMode::LevelIncludesOwnOrder,
        }));

        let snapshot = engine.order_snapshot(1).unwrap();
        assert_eq!(snapshot.inpos, 10.0);
        assert_eq!(snapshot.backlen, 0.0);
    }

    #[test]
    fn public_trade_consumes_front_before_own_order() {
        let mut engine = QueuePositionEngine::new();
        add_sell(&mut engine, 1, 2.0, 10.0);

        engine.apply_public_trade(PublicTrade {
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            price_key: 100,
            qty: 3.0,
        });
        assert_eq!(engine.order_snapshot(1).unwrap().inpos, 7.0);

        engine.apply_public_trade(PublicTrade {
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            price_key: 100,
            qty: 8.0,
        });
        let snapshot = engine.order_snapshot(1).unwrap();
        assert_eq!(snapshot.inpos, 0.0);
        assert_eq!(snapshot.remaining_qty, 2.0);
        assert_eq!(snapshot.public_consumed_own_qty, 1.0);
    }

    #[test]
    fn public_trade_does_not_remove_without_account_update() {
        let mut engine = QueuePositionEngine::new();
        add_sell(&mut engine, 1, 2.0, 1.0);

        engine.apply_public_trade(PublicTrade {
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            price_key: 100,
            qty: 3.0,
        });

        assert!(engine.contains_order(1));
        let snapshot = engine.order_snapshot(1).unwrap();
        assert_eq!(snapshot.remaining_qty, 2.0);
        assert_eq!(snapshot.public_consumed_own_qty, 2.0);
    }

    #[test]
    fn public_trade_crosses_prior_own_queue_for_later_order() {
        let mut engine = QueuePositionEngine::new();
        add_sell(&mut engine, 1, 2.0, 1.0);
        add_sell(&mut engine, 2, 1.5, 3.0);

        engine.apply_public_trade(PublicTrade {
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            price_key: 100,
            qty: 3.5,
        });

        assert_eq!(engine.order_snapshot(1).unwrap().inpos, 0.0);
        assert_eq!(
            engine.order_snapshot(1).unwrap().public_consumed_own_qty,
            2.0
        );
        assert_eq!(engine.order_snapshot(2).unwrap().inpos, 2.5);
    }

    #[test]
    fn level_increase_only_updates_tlen_and_backlen() {
        let mut engine = QueuePositionEngine::new();
        add_sell(&mut engine, 1, 2.0, 10.0);

        engine.apply_level_update(LevelUpdate {
            symbol: "BTCUSDT".to_string(),
            side: BookSide::Ask,
            price_key: 100,
            old_qty: 10.0,
            new_qty: 15.0,
        });

        let snapshot = engine.order_snapshot(1).unwrap();
        assert_eq!(snapshot.inpos, 10.0);
        assert_eq!(snapshot.backlen, 3.0);
    }

    #[test]
    fn level_decrease_allocates_cancel_between_front_and_back() {
        let mut engine = QueuePositionEngine::new();
        add_sell(&mut engine, 1, 2.0, 12.0);
        engine.apply_level_update(LevelUpdate {
            symbol: "BTCUSDT".to_string(),
            side: BookSide::Ask,
            price_key: 100,
            old_qty: 12.0,
            new_qty: 20.0,
        });

        engine.apply_level_update(LevelUpdate {
            symbol: "BTCUSDT".to_string(),
            side: BookSide::Ask,
            price_key: 100,
            old_qty: 20.0,
            new_qty: 16.0,
        });

        let snapshot = engine.order_snapshot(1).unwrap();
        assert!((snapshot.inpos - 9.333333333333334).abs() < 1e-12);
        assert!((snapshot.backlen - 4.666666666666666).abs() < 1e-12);
    }

    #[test]
    fn fill_update_changes_remaining_and_terminal_fill_removes() {
        let mut engine = QueuePositionEngine::new();
        add_sell(&mut engine, 1, 2.0, 10.0);

        assert!(engine
            .apply_fill_update(FillUpdate {
                order_id: 1,
                cumulative_filled_qty: 1.25,
            })
            .is_none());
        assert_eq!(engine.order_snapshot(1).unwrap().remaining_qty, 0.75);

        let removed = engine.apply_fill_update(FillUpdate {
            order_id: 1,
            cumulative_filled_qty: 2.0,
        });
        assert!(removed.is_some());
        assert!(!engine.contains_order(1));
    }

    #[test]
    fn remove_order_cleans_level_index() {
        let mut engine = QueuePositionEngine::new();
        add_sell(&mut engine, 1, 2.0, 10.0);

        let level = LevelKey::new("BTCUSDT", BookSide::Ask, 100);
        assert_eq!(engine.level_summary(&level).tracked_orders, 1);
        assert!(engine.remove_order(1).is_some());
        assert_eq!(engine.level_summary(&level).tracked_orders, 0);
    }
}
