//! Transient numeric indexes; external order identities and persisted records stay unchanged.
use crate::model::*;
use std::collections::HashMap;
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
pub(super) struct OrderSlot {
    pub order: Order,
    pub symbol: SymbolId,
    pub working_index: Option<usize>,
}
pub(super) struct OrderArena {
    pub slots: Vec<OrderSlot>,
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
            by_session: HashMap::with_capacity(8),
        }
    }
}
impl OrderArena {
    pub fn reserve(&mut self, total: usize) -> anyhow::Result<()> {
        self.slots
            .try_reserve(total.saturating_sub(self.slots.len()))?;
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
    pub fn get(&self, key: &OrderKey) -> Option<&Order> {
        self.id(key).map(|id| &self.slots[id.0].order)
    }
    pub fn get_mut(&mut self, key: &OrderKey) -> Option<&mut Order> {
        let id = self.id(key)?;
        Some(&mut self.slots[id.0].order)
    }
    pub fn values(&self) -> impl Iterator<Item = &Order> {
        self.slots.iter().map(|s| &s.order)
    }
    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = (&OrderKey, &Order)> {
        self.values().map(|o| (&o.key, o))
    }
    pub fn insert(&mut self, order: Order, symbol: SymbolId) -> (OrderId, i32) {
        if let Some(id) = self.id(&order.key) {
            let old = self.slots[id.0].order.remaining();
            self.slots[id.0].order = order;
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
        self.slots.push(OrderSlot {
            order,
            symbol,
            working_index: None,
        });
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
