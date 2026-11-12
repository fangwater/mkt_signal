//! Transient numeric indexes; external order identities and persisted records stay unchanged.
use crate::model::*;
use std::{collections::HashMap, num::NonZeroUsize};
pub(super) const INITIAL_ORDER_SLOTS: usize = 4096;
// Internal ordinal slots only; no ordinal is persisted or sent to a venue.
pub(super) const VENUE_SLOTS: usize = Venue::ALL.len();
pub(super) fn venue_index(venue: Venue) -> usize {
    venue as usize
}
pub(super) fn side_index(side: Side) -> usize {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct SymbolId(pub usize);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct OrderId(pub usize);
#[derive(Default)]
pub(super) struct Symbols {
    indexes: [HashMap<String, SymbolId>; VENUE_SLOTS],
    identities: Vec<(Venue, String)>,
}
impl Symbols {
    pub fn id(&self, venue: Venue, symbol: &str) -> Option<SymbolId> {
        self.indexes[venue_index(venue)].get(symbol).copied()
    }
    pub fn intern(&mut self, venue: Venue, symbol: &str) -> SymbolId {
        if let Some(id) = self.id(venue, symbol) {
            return id;
        }
        let id = SymbolId(self.identities.len());
        self.identities.push((venue, symbol.into()));
        self.indexes[venue_index(venue)].insert(symbol.into(), id);
        id
    }
    pub fn identity(&self, id: SymbolId) -> (Venue, &str) {
        let (venue, symbol) = &self.identities[id.0];
        (*venue, symbol)
    }
}
/// Dense, string-free state touched by risk checks and order/fill updates.
/// Request scalars are immutable after insertion; mutable state lives only here.
#[derive(Clone, Copy)]
pub(super) struct OrderSlot {
    pub symbol: SymbolId,
    pub working_index: Option<NonZeroUsize>,
    pub limit_price: f64,
    pub local_cash_held: f64,
    pub cash_reserved_per_lot: f64,
    pub lots: i32,
    pub filled: i32,
    pub reported_filled: i32,
    pub cancelled: i32,
    pub reserved_today: i32,
    pub reserved_yesterday: i32,
    pub side: Side,
    pub offset: Offset,
    pub price_type: PriceType,
    pub status: OrderStatus,
    pub cancel_pending: bool,
    pub owned: bool,
}
impl OrderSlot {
    pub fn remaining(&self) -> i32 {
        self.lots - self.filled - self.cancelled
    }
    fn new(order: &Order, symbol: SymbolId, working_index: Option<NonZeroUsize>) -> Self {
        Self {
            symbol,
            working_index,
            limit_price: order.limit_price,
            local_cash_held: order.local_cash_held,
            cash_reserved_per_lot: order.cash_reserved_per_lot,
            lots: order.request.lots,
            side: order.request.side,
            offset: order.request.offset,
            price_type: order.request.price_type,
            status: order.status,
            filled: order.filled,
            reported_filled: order.reported_filled,
            cancelled: order.cancelled,
            reserved_today: order.reserved_today,
            reserved_yesterday: order.reserved_yesterday,
            cancel_pending: order.cancel_pending,
            owned: order.owned,
        }
    }
}
pub(super) struct OrderIdentity {
    pub request: Intent,
    pub key: OrderKey,
    pub system_id: String,
}
impl From<Order> for OrderIdentity {
    fn from(order: Order) -> Self {
        Self {
            request: order.request,
            key: order.key,
            system_id: order.system_id,
        }
    }
}
pub(super) struct OrderArena {
    pub slots: Vec<OrderSlot>,
    pub identities: Vec<OrderIdentity>,
    by_session: HashMap<String, References>,
}
#[derive(Default)]
struct References {
    numbers: HashMap<u64, OrderId>,
    // Noncanonical external references keep their exact spelling and identity.
    text: HashMap<String, OrderId>,
}
fn numeric_reference(reference: &str) -> Option<u64> {
    if reference.is_empty()
        || (reference.len() > 1 && reference.starts_with('0'))
        || !reference.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    reference.parse().ok()
}
impl References {
    fn reserve(&mut self, total: usize) -> anyhow::Result<()> {
        self.numbers
            .try_reserve(total.saturating_sub(self.numbers.len()))?;
        if self.numbers.is_empty() {
            // Touch index buckets before trading. No real reference is consumed.
            for reference in 0..total as u64 {
                self.numbers.insert(reference, OrderId(0));
            }
            self.numbers.clear();
        }
        Ok(())
    }
    fn id(&self, reference: &str) -> Option<OrderId> {
        match numeric_reference(reference) {
            Some(number) => self.numbers.get(&number).copied(),
            None => self.text.get(reference).copied(),
        }
    }
    fn insert(&mut self, reference: &str, id: OrderId) {
        match numeric_reference(reference) {
            Some(number) => {
                self.numbers.insert(number, id);
            }
            None => {
                self.text.insert(reference.into(), id);
            }
        }
    }
}
impl Default for OrderArena {
    fn default() -> Self {
        Self {
            slots: Vec::with_capacity(INITIAL_ORDER_SLOTS),
            identities: Vec::with_capacity(INITIAL_ORDER_SLOTS),
            by_session: HashMap::with_capacity(8),
        }
    }
}
impl OrderArena {
    pub fn reserve(&mut self, total: usize) -> anyhow::Result<()> {
        self.slots
            .try_reserve(total.saturating_sub(self.slots.len()))?;
        self.identities
            .try_reserve(total.saturating_sub(self.identities.len()))?;
        self.identities
            .spare_capacity_mut()
            .fill_with(std::mem::MaybeUninit::zeroed);
        // Initialise only spare MaybeUninit storage. This faults pages in on the
        // cold path without constructing orders or changing the live Vec length.
        self.slots
            .spare_capacity_mut()
            .fill_with(std::mem::MaybeUninit::zeroed);
        Ok(())
    }
    pub fn reserve_session(&mut self, session: &str, count: usize) -> anyhow::Result<()> {
        let refs = self.by_session.entry(session.to_owned()).or_default();
        refs.reserve(count)?;
        Ok(())
    }
    pub fn id_parts(&self, session: &str, reference: &str) -> Option<OrderId> {
        self.by_session.get(session)?.id(reference)
    }
    pub fn id(&self, key: &OrderKey) -> Option<OrderId> {
        self.id_parts(&key.session, &key.order_ref)
    }
    pub fn contains_key(&self, key: &OrderKey) -> bool {
        self.id(key).is_some()
    }
    pub fn view(&self, id: OrderId) -> OrderView<'_> {
        let hot = &self.slots[id.0];
        let cold = &self.identities[id.0];
        OrderView {
            request: &cold.request,
            key: &cold.key,
            system_id: &cold.system_id,
            limit_price: hot.limit_price,
            status: hot.status,
            filled: hot.filled,
            reported_filled: hot.reported_filled,
            cancelled: hot.cancelled,
            reserved_today: hot.reserved_today,
            reserved_yesterday: hot.reserved_yesterday,
            local_cash_held: hot.local_cash_held,
            cash_reserved_per_lot: hot.cash_reserved_per_lot,
            cancel_pending: hot.cancel_pending,
            owned: hot.owned,
        }
    }
    pub fn get(&self, key: &OrderKey) -> Option<OrderView<'_>> {
        self.id(key).map(|id| self.view(id))
    }
    pub fn values(&self) -> impl Iterator<Item = OrderView<'_>> {
        (0..self.slots.len()).map(|index| self.view(OrderId(index)))
    }
    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = (&OrderKey, OrderView<'_>)> {
        self.values().map(|o| (o.key, o))
    }
    pub fn insert(&mut self, order: Order, symbol: SymbolId) -> (OrderId, i32) {
        if let Some(id) = self.id(&order.key) {
            let old = self.slots[id.0].remaining();
            self.slots[id.0] = OrderSlot::new(&order, symbol, self.slots[id.0].working_index);
            self.identities[id.0] = order.into();
            return (id, old);
        }
        let id = OrderId(self.slots.len());
        let capacity = if order.key.session.contains(":sys:") {
            1
        } else {
            INITIAL_ORDER_SLOTS
        };
        if let Some(refs) = self.by_session.get_mut(order.key.session.as_str()) {
            refs.insert(&order.key.order_ref, id);
        } else {
            let mut refs = References {
                numbers: HashMap::with_capacity(capacity),
                text: HashMap::new(),
            };
            refs.insert(&order.key.order_ref, id);
            self.by_session.insert(order.key.session.clone(), refs);
        }
        self.slots.push(OrderSlot::new(&order, symbol, None));
        self.identities.push(order.into());
        (id, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numeric_index_preserves_noncanonical_and_overflowing_reference_identities() {
        let mut refs = References::default();
        refs.reserve(4096).unwrap();
        assert!(refs.numbers.is_empty() && refs.text.is_empty());
        let identities = [
            "0",
            "1",
            "01",
            "0001",
            "+1",
            "18446744073709551615",
            "18446744073709551616",
            "manual-ref",
        ];
        for (i, reference) in identities.iter().enumerate() {
            refs.insert(reference, OrderId(i));
        }
        refs.reserve(8192).unwrap();
        for (i, reference) in identities.iter().enumerate() {
            assert_eq!(refs.id(reference), Some(OrderId(i)));
        }
        assert_eq!(refs.id("0000"), None);
        assert_eq!(refs.numbers.len(), 3);
    }
}
