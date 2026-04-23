use arc_swap::ArcSwap;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;

use crate::common::symbol_util::normalize_symbol_for_internal;

use super::orderbook::{key_to_price, OrderBook};
use super::query_logic::DepthQuerySource;
use super::query_msg::price_to_tick_index;

#[derive(Debug, Clone)]
pub struct SymbolQuerySnapshot {
    pub timestamp: i64,
    pub price_tick: Option<f64>,
    pub book_valid: bool,
    pub top5_ready: bool,
    pub top5_bids: Vec<(i64, f64)>,
    pub top5_asks: Vec<(i64, f64)>,
    bid_amounts: HashMap<i64, f64>,
    ask_amounts: HashMap<i64, f64>,
}

impl SymbolQuerySnapshot {
    pub fn from_orderbook(orderbook: &OrderBook, price_tick: Option<f64>) -> Self {
        Self::from_orderbook_with_amount_scale(orderbook, price_tick, 1.0)
    }

    pub fn from_orderbook_with_amount_scale(
        orderbook: &OrderBook,
        price_tick: Option<f64>,
        amount_scale: f64,
    ) -> Self {
        let book_valid = orderbook.is_valid();
        let mut top5_ready = false;
        let mut top5_bids = Vec::new();
        let mut top5_asks = Vec::new();

        if book_valid {
            if let Some(tick) = price_tick {
                let (bids, asks) = orderbook.get_depth_keys(5);
                if let (Some(bid_levels), Some(ask_levels)) = (
                    depth_levels_to_tick_indices(&bids, tick, amount_scale),
                    depth_levels_to_tick_indices(&asks, tick, amount_scale),
                ) {
                    top5_bids = bid_levels;
                    top5_asks = ask_levels;
                    top5_ready = true;
                }
            }
        }

        let bid_amounts = scale_price_key_amounts(orderbook.bid_levels_keys(), amount_scale);
        let ask_amounts = scale_price_key_amounts(orderbook.ask_levels_keys(), amount_scale);

        Self {
            timestamp: orderbook.timestamp,
            price_tick,
            book_valid,
            top5_ready,
            top5_bids,
            top5_asks,
            bid_amounts,
            ask_amounts,
        }
    }

    pub fn amount_at_price_key(&self, price_key: i64) -> Option<f64> {
        self.bid_amounts
            .get(&price_key)
            .copied()
            .or_else(|| self.ask_amounts.get(&price_key).copied())
    }
}

pub struct QuerySnapshotStore {
    venue_slug: String,
    snapshots: DashMap<String, Arc<ArcSwap<SymbolQuerySnapshot>>>,
}

impl QuerySnapshotStore {
    pub fn new(venue_slug: impl Into<String>) -> Self {
        Self {
            venue_slug: venue_slug.into(),
            snapshots: DashMap::new(),
        }
    }

    pub fn venue_slug(&self) -> &str {
        &self.venue_slug
    }

    pub fn publish(&self, symbol: &str, snapshot: SymbolQuerySnapshot) {
        let snapshot = Arc::new(snapshot);
        let mut keys = vec![symbol.to_string()];
        let normalized = normalize_symbol_for_internal(symbol);
        if !normalized.is_empty() && normalized != symbol {
            keys.push(normalized);
        }

        for key in keys {
            match self.snapshots.entry(key) {
                Entry::Occupied(entry) => {
                    entry.get().store(Arc::clone(&snapshot));
                }
                Entry::Vacant(entry) => {
                    entry.insert(Arc::new(ArcSwap::new(Arc::clone(&snapshot))));
                }
            }
        }
    }

    pub fn load(&self, symbol: &str) -> Option<Arc<SymbolQuerySnapshot>> {
        self.lookup_cell(symbol).map(|cell| cell.load_full())
    }

    fn lookup_cell(&self, symbol: &str) -> Option<Arc<ArcSwap<SymbolQuerySnapshot>>> {
        if let Some(cell) = self.snapshots.get(symbol) {
            return Some(Arc::clone(cell.value()));
        }

        let upper = symbol.to_ascii_uppercase();
        if upper == symbol {
            return None;
        }

        self.snapshots
            .get(&upper)
            .map(|cell| Arc::clone(cell.value()))
    }
}

impl DepthQuerySource for QuerySnapshotStore {
    fn venue_slug(&self) -> &str {
        self.venue_slug()
    }

    fn resolve_snapshot(&self, symbol: &str) -> Option<Arc<SymbolQuerySnapshot>> {
        self.load(symbol)
    }
}

fn depth_levels_to_tick_indices(
    levels: &[(i64, f64)],
    tick: f64,
    amount_scale: f64,
) -> Option<Vec<(i64, f64)>> {
    let mut out = Vec::with_capacity(levels.len());
    for (price_key, amount) in levels {
        let price = key_to_price(*price_key);
        let tick_index = price_to_tick_index(price, tick)?;
        out.push((tick_index, amount * amount_scale));
    }
    Some(out)
}

fn scale_price_key_amounts(levels: Vec<(i64, f64)>, amount_scale: f64) -> HashMap<i64, f64> {
    levels
        .into_iter()
        .map(|(price_key, amount)| (price_key, amount * amount_scale))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_contains_amounts_and_top5() {
        let mut orderbook = OrderBook::new();
        orderbook.apply_update(
            &[(100.0, 1.0), (99.5, 2.0)],
            &[(100.5, 3.0), (101.0, 4.0)],
            1,
            1234,
        );

        let snapshot = SymbolQuerySnapshot::from_orderbook(&orderbook, Some(0.5));
        assert_eq!(snapshot.timestamp, 1234);
        assert!(snapshot.book_valid);
        assert!(snapshot.top5_ready);
        assert_eq!(snapshot.top5_bids[0], (200, 1.0));
        assert_eq!(snapshot.top5_asks[0], (201, 3.0));
        assert_eq!(snapshot.amount_at_price_key(10_000_000_000), Some(1.0));
    }

    #[test]
    fn store_load_supports_uppercase_fallback() {
        let store = QuerySnapshotStore::new("binance-futures");
        let mut orderbook = OrderBook::new();
        orderbook.apply_update(&[(100.0, 1.0)], &[(101.0, 2.0)], 1, 1);
        store.publish(
            "BTCUSDT",
            SymbolQuerySnapshot::from_orderbook(&orderbook, Some(1.0)),
        );

        let loaded = store.load("btcusdt").expect("snapshot should exist");
        assert_eq!(loaded.timestamp, 1);
        assert_eq!(loaded.amount_at_price_key(10_000_000_000), Some(1.0));
    }

    #[test]
    fn store_load_supports_normalized_alias_for_gate_style_symbol() {
        let store = QuerySnapshotStore::new("gate-futures");
        let mut orderbook = OrderBook::new();
        orderbook.apply_update(&[(100.0, 1.0)], &[(101.0, 2.0)], 1, 1);
        store.publish(
            "BTC_USDT",
            SymbolQuerySnapshot::from_orderbook(&orderbook, Some(1.0)),
        );

        let loaded = store.load("BTCUSDT").expect("normalized alias should exist");
        assert_eq!(loaded.timestamp, 1);
        assert_eq!(loaded.amount_at_price_key(10_000_000_000), Some(1.0));
    }

    #[test]
    fn snapshot_scales_amounts_for_output_qty() {
        let mut orderbook = OrderBook::new();
        orderbook.apply_update(
            &[(100.0, 2.0), (99.5, 3.0)],
            &[(100.5, 4.0), (101.0, 5.0)],
            1,
            1234,
        );

        let snapshot =
            SymbolQuerySnapshot::from_orderbook_with_amount_scale(&orderbook, Some(0.5), 0.1);

        assert_eq!(snapshot.top5_bids[0], (200, 0.2));
        assert_eq!(snapshot.top5_asks[0], (201, 0.4));
        assert_eq!(snapshot.amount_at_price_key(10_000_000_000), Some(0.2));
    }
}
