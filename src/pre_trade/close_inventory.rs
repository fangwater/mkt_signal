use std::collections::HashMap;

use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;
use log::{debug, info, warn};

const CLOSE_INVENTORY_EPS: f64 = 1e-12;

#[derive(Debug, Clone)]
pub struct CloseReservation {
    pub client_order_id: i64,
    pub side: Side,
    pub reserved_base_qty: f64,
    pub filled_base_qty: f64,
}

#[derive(Debug, Clone, Default)]
struct CloseInventoryEntry {
    seeded: bool,
    closable_inventory_base: f64,
    reserved_sell_close_base: f64,
    reserved_buy_close_base: f64,
    orders: HashMap<i64, CloseReservation>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CloseReservationGrant {
    pub requested_base_qty: f64,
    pub granted_base_qty: f64,
    pub available_before_base: f64,
    pub closable_inventory_base: f64,
}

#[derive(Debug, Default)]
pub struct CloseInventoryLedger {
    entries: HashMap<(TradingVenue, String), CloseInventoryEntry>,
}

impl CloseInventoryLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn seed_if_absent(&mut self, venue: TradingVenue, symbol: &str, snapshot_pos_base: f64) {
        let symbol = normalize_symbol_for_internal(symbol);
        if symbol.is_empty() {
            return;
        }
        let entry = self.entries.entry((venue, symbol.clone())).or_default();
        if entry.seeded {
            return;
        }
        entry.seeded = true;
        entry.closable_inventory_base = finite_or_zero(snapshot_pos_base);
        info!(
            "CloseInventory: seed symbol={} venue={:?} closable_inventory_base={:.8}",
            symbol, venue, entry.closable_inventory_base
        );
    }

    pub fn reserve_close(
        &mut self,
        venue: TradingVenue,
        symbol: &str,
        side: Side,
        requested_base_qty: f64,
        client_order_id: i64,
        snapshot_pos_base: f64,
    ) -> CloseReservationGrant {
        let symbol = normalize_symbol_for_internal(symbol);
        if symbol.is_empty() || requested_base_qty <= CLOSE_INVENTORY_EPS {
            return CloseReservationGrant {
                requested_base_qty,
                granted_base_qty: 0.0,
                available_before_base: 0.0,
                closable_inventory_base: 0.0,
            };
        }
        self.seed_if_absent(venue, &symbol, snapshot_pos_base);
        let entry = self
            .entries
            .entry((venue, symbol.clone()))
            .or_insert_with(|| CloseInventoryEntry {
                seeded: true,
                closable_inventory_base: finite_or_zero(snapshot_pos_base),
                ..CloseInventoryEntry::default()
            });

        if entry.orders.contains_key(&client_order_id) {
            warn!(
                "CloseInventory: duplicate reserve ignored symbol={} venue={:?} side={:?} client_order_id={}",
                symbol, venue, side, client_order_id
            );
            return CloseReservationGrant {
                requested_base_qty,
                granted_base_qty: 0.0,
                available_before_base: Self::available_for_side(entry, side),
                closable_inventory_base: entry.closable_inventory_base,
            };
        }

        let available_before_base = Self::available_for_side(entry, side);
        let granted_base_qty = requested_base_qty.min(available_before_base).max(0.0);
        if granted_base_qty <= CLOSE_INVENTORY_EPS {
            debug!(
                "CloseInventory: reserve denied symbol={} venue={:?} side={:?} requested={:.8} available={:.8} inventory={:.8}",
                symbol,
                venue,
                side,
                requested_base_qty,
                available_before_base,
                entry.closable_inventory_base
            );
            return CloseReservationGrant {
                requested_base_qty,
                granted_base_qty: 0.0,
                available_before_base,
                closable_inventory_base: entry.closable_inventory_base,
            };
        }

        match side {
            Side::Sell => entry.reserved_sell_close_base += granted_base_qty,
            Side::Buy => entry.reserved_buy_close_base += granted_base_qty,
        }
        entry.orders.insert(
            client_order_id,
            CloseReservation {
                client_order_id,
                side,
                reserved_base_qty: granted_base_qty,
                filled_base_qty: 0.0,
            },
        );
        info!(
            "CloseInventory: reserve symbol={} venue={:?} side={:?} client_order_id={} requested={:.8} granted={:.8} available_before={:.8} inventory={:.8}",
            symbol,
            venue,
            side,
            client_order_id,
            requested_base_qty,
            granted_base_qty,
            available_before_base,
            entry.closable_inventory_base
        );
        CloseReservationGrant {
            requested_base_qty,
            granted_base_qty,
            available_before_base,
            closable_inventory_base: entry.closable_inventory_base,
        }
    }

    pub fn apply_open_fill_delta(
        &mut self,
        venue: TradingVenue,
        symbol: &str,
        side: Side,
        filled_base_delta: f64,
    ) {
        if filled_base_delta <= CLOSE_INVENTORY_EPS {
            return;
        }
        let symbol = normalize_symbol_for_internal(symbol);
        // Open fills are local events. If this is the first local event for a symbol,
        // start from zero rather than a live account snapshot to avoid double-counting
        // when the account stream has already reflected the same fill.
        self.seed_if_absent(venue, &symbol, 0.0);
        let Some(entry) = self.entries.get_mut(&(venue, symbol.clone())) else {
            return;
        };
        match side {
            Side::Buy => entry.closable_inventory_base += filled_base_delta,
            Side::Sell => entry.closable_inventory_base -= filled_base_delta,
        }
        info!(
            "CloseInventory: open fill symbol={} venue={:?} side={:?} delta={:.8} inventory={:.8}",
            symbol, venue, side, filled_base_delta, entry.closable_inventory_base
        );
    }

    pub fn apply_close_fill_delta(&mut self, client_order_id: i64, filled_base_delta: f64) -> bool {
        if filled_base_delta <= CLOSE_INVENTORY_EPS {
            return false;
        }
        let Some(((venue, symbol), entry)) = self.find_order_entry_mut(client_order_id) else {
            warn!(
                "CloseInventory: close fill without reservation client_order_id={} delta={:.8}",
                client_order_id, filled_base_delta
            );
            return false;
        };
        let Some(reservation) = entry.orders.get_mut(&client_order_id) else {
            return false;
        };
        let remaining_reserved =
            (reservation.reserved_base_qty - reservation.filled_base_qty).max(0.0);
        let applied_delta = filled_base_delta.min(remaining_reserved);
        if applied_delta <= CLOSE_INVENTORY_EPS {
            return false;
        }

        reservation.filled_base_qty += applied_delta;
        match reservation.side {
            Side::Sell => {
                entry.closable_inventory_base -= applied_delta;
                entry.reserved_sell_close_base =
                    (entry.reserved_sell_close_base - applied_delta).max(0.0);
            }
            Side::Buy => {
                entry.closable_inventory_base += applied_delta;
                entry.reserved_buy_close_base =
                    (entry.reserved_buy_close_base - applied_delta).max(0.0);
            }
        }
        info!(
            "CloseInventory: close fill symbol={} venue={:?} side={:?} client_order_id={} delta={:.8} applied={:.8} filled={:.8}/{:.8} inventory={:.8}",
            symbol,
            venue,
            reservation.side,
            client_order_id,
            filled_base_delta,
            applied_delta,
            reservation.filled_base_qty,
            reservation.reserved_base_qty,
            entry.closable_inventory_base
        );
        true
    }

    pub fn release_close_unfilled(&mut self, client_order_id: i64, reason: &str) -> bool {
        let Some(((venue, symbol), entry)) = self.find_order_entry_mut(client_order_id) else {
            return false;
        };
        let Some(reservation) = entry.orders.remove(&client_order_id) else {
            return false;
        };
        let unfilled = (reservation.reserved_base_qty - reservation.filled_base_qty).max(0.0);
        if unfilled > CLOSE_INVENTORY_EPS {
            match reservation.side {
                Side::Sell => {
                    entry.reserved_sell_close_base =
                        (entry.reserved_sell_close_base - unfilled).max(0.0);
                }
                Side::Buy => {
                    entry.reserved_buy_close_base =
                        (entry.reserved_buy_close_base - unfilled).max(0.0);
                }
            }
        }
        info!(
            "CloseInventory: release symbol={} venue={:?} side={:?} client_order_id={} reason={} unfilled={:.8} filled={:.8}/{:.8} inventory={:.8}",
            symbol,
            venue,
            reservation.side,
            client_order_id,
            reason,
            unfilled,
            reservation.filled_base_qty,
            reservation.reserved_base_qty,
            entry.closable_inventory_base
        );
        true
    }

    pub fn has_reservation(&self, client_order_id: i64) -> bool {
        self.entries
            .values()
            .any(|entry| entry.orders.contains_key(&client_order_id))
    }

    pub fn closable_inventory_base(&self, venue: TradingVenue, symbol: &str) -> Option<f64> {
        let symbol = normalize_symbol_for_internal(symbol);
        self.entries
            .get(&(venue, symbol))
            .map(|entry| entry.closable_inventory_base)
    }

    pub fn reserved_for_side(&self, venue: TradingVenue, symbol: &str, side: Side) -> f64 {
        let symbol = normalize_symbol_for_internal(symbol);
        self.entries
            .get(&(venue, symbol))
            .map(|entry| match side {
                Side::Sell => entry.reserved_sell_close_base,
                Side::Buy => entry.reserved_buy_close_base,
            })
            .unwrap_or(0.0)
    }

    fn available_for_side(entry: &CloseInventoryEntry, side: Side) -> f64 {
        match side {
            Side::Sell => entry.closable_inventory_base.max(0.0) - entry.reserved_sell_close_base,
            Side::Buy => (-entry.closable_inventory_base).max(0.0) - entry.reserved_buy_close_base,
        }
        .max(0.0)
    }

    fn find_order_entry_mut(
        &mut self,
        client_order_id: i64,
    ) -> Option<((TradingVenue, String), &mut CloseInventoryEntry)> {
        self.entries
            .iter_mut()
            .find(|(_, entry)| entry.orders.contains_key(&client_order_id))
            .map(|(key, entry)| (key.clone(), entry))
    }
}

fn finite_or_zero(value: f64) -> f64 {
    if value.is_finite() {
        value
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sell_close_batch_reserve_does_not_exceed_long_inventory() {
        let mut ledger = CloseInventoryLedger::new();
        let venue = TradingVenue::BinanceMargin;

        let first = ledger.reserve_close(venue, "COTIUSDT", Side::Sell, 2_000.0, 1, 4_211.0);
        let second = ledger.reserve_close(venue, "COTIUSDT", Side::Sell, 2_000.0, 2, 4_211.0);
        let third = ledger.reserve_close(venue, "COTIUSDT", Side::Sell, 2_000.0, 3, 4_211.0);

        assert_eq!(first.granted_base_qty, 2_000.0);
        assert_eq!(second.granted_base_qty, 2_000.0);
        assert_eq!(third.granted_base_qty, 211.0);
        assert_eq!(
            ledger.reserved_for_side(venue, "COTIUSDT", Side::Sell),
            4_211.0
        );
    }

    #[test]
    fn buy_close_batch_reserve_does_not_exceed_short_inventory() {
        let mut ledger = CloseInventoryLedger::new();
        let venue = TradingVenue::BinanceMargin;

        let first = ledger.reserve_close(venue, "COTIUSDT", Side::Buy, 1_500.0, 1, -2_200.0);
        let second = ledger.reserve_close(venue, "COTIUSDT", Side::Buy, 1_500.0, 2, -2_200.0);

        assert_eq!(first.granted_base_qty, 1_500.0);
        assert_eq!(second.granted_base_qty, 700.0);
        assert_eq!(
            ledger.reserved_for_side(venue, "COTIUSDT", Side::Buy),
            2_200.0
        );
    }

    #[test]
    fn close_fill_updates_inventory_and_reserved() {
        let mut ledger = CloseInventoryLedger::new();
        let venue = TradingVenue::BinanceMargin;
        ledger.reserve_close(venue, "COTIUSDT", Side::Sell, 1_000.0, 1, 4_000.0);

        assert!(ledger.apply_close_fill_delta(1, 400.0));

        assert_eq!(
            ledger.closable_inventory_base(venue, "COTIUSDT"),
            Some(3_600.0)
        );
        assert_eq!(
            ledger.reserved_for_side(venue, "COTIUSDT", Side::Sell),
            600.0
        );
    }

    #[test]
    fn terminal_release_only_releases_unfilled_qty() {
        let mut ledger = CloseInventoryLedger::new();
        let venue = TradingVenue::BinanceMargin;
        ledger.reserve_close(venue, "COTIUSDT", Side::Sell, 1_000.0, 1, 4_000.0);
        ledger.apply_close_fill_delta(1, 400.0);

        assert!(ledger.release_close_unfilled(1, "canceled"));

        assert_eq!(
            ledger.closable_inventory_base(venue, "COTIUSDT"),
            Some(3_600.0)
        );
        assert_eq!(ledger.reserved_for_side(venue, "COTIUSDT", Side::Sell), 0.0);
        assert!(!ledger.has_reservation(1));
    }

    #[test]
    fn open_fills_move_inventory_by_side() {
        let mut ledger = CloseInventoryLedger::new();
        let venue = TradingVenue::BinanceMargin;

        ledger.apply_open_fill_delta(venue, "COTIUSDT", Side::Buy, 100.0);
        ledger.apply_open_fill_delta(venue, "COTIUSDT", Side::Sell, 30.0);

        assert_eq!(
            ledger.closable_inventory_base(venue, "COTIUSDT"),
            Some(70.0)
        );
    }
}
