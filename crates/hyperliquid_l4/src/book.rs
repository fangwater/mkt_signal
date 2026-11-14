//! Optional consumer: O(1) oid lookup and intrusive FIFO queue mutations.
//! A snapshot/block becomes visible only after every binary chunk is received.
use crate::wire::{Coin, Event, Header, Kind, Op, Side, EVENT_BYTES, HAS_ANCHOR, HAS_USER};
use ahash::AHashMap;
use anyhow::{ensure, Context, Result};

#[derive(Clone, Copy)]
struct Node {
    event: Event,
    previous: Option<usize>,
    next: Option<usize>,
}
#[derive(Default)]
struct Level {
    head: Option<usize>,
    tail: Option<usize>,
    count: usize,
    size: u128,
}
#[derive(Default)]
struct State {
    nodes: Vec<Node>,
    free: Vec<usize>,
    by_id: AHashMap<u64, usize>,
    levels: AHashMap<(Side, u64), Level>,
}
impl State {
    fn clear(&mut self) {
        self.nodes.clear();
        self.free.clear();
        self.by_id.clear();
        self.levels.clear();
    }
    fn insert(&mut self, event: Event, limit: usize) -> Result<()> {
        ensure!(self.by_id.len() < limit, "L4 order capacity exceeded");
        ensure!(
            !self.by_id.contains_key(&event.oid),
            "duplicate resting oid"
        );
        let key = (event.side, event.price);
        let anchor = if event.flags & HAS_ANCHOR != 0 {
            let slot = *self
                .by_id
                .get(&event.insert_before)
                .context("insertBefore target missing")?;
            let node = self.nodes[slot];
            ensure!(
                (node.event.side, node.event.price) == key,
                "insertBefore level mismatch"
            );
            Some(slot)
        } else {
            None
        };
        let previous = if let Some(anchor) = anchor {
            self.nodes[anchor].previous
        } else {
            self.levels.get(&key).and_then(|level| level.tail)
        };
        let node = Node {
            event,
            previous,
            next: anchor,
        };
        let slot = if let Some(slot) = self.free.pop() {
            self.nodes[slot] = node;
            slot
        } else {
            self.nodes.push(node);
            self.nodes.len() - 1
        };
        if let Some(previous) = previous {
            self.nodes[previous].next = Some(slot);
        }
        if let Some(next) = anchor {
            self.nodes[next].previous = Some(slot);
        }
        let level = self.levels.entry(key).or_default();
        if previous.is_none() {
            level.head = Some(slot);
        }
        if anchor.is_none() {
            level.tail = Some(slot);
        }
        level.count += 1;
        level.size += event.size as u128;
        self.by_id.insert(event.oid, slot);
        Ok(())
    }
    fn apply(&mut self, event: Event, limit: usize) -> Result<()> {
        if event.op == Op::New {
            return self.insert(event, limit);
        }
        ensure!(
            matches!(event.op, Op::Update | Op::Remove),
            "snapshot record in update batch"
        );
        let slot = *self
            .by_id
            .get(&event.oid)
            .context("mutation for unknown oid")?;
        let node = self.nodes[slot];
        ensure!(event.price == node.event.price, "mutation price mismatch");
        ensure!(
            event.flags & HAS_USER == 0
                || node.event.flags & HAS_USER == 0
                || node.event.user == event.user,
            "mutation user mismatch"
        );
        let key = (node.event.side, node.event.price);
        let level = self.levels.get_mut(&key).context("missing order level")?;
        if event.op == Op::Update {
            ensure!(
                event.original_size == node.event.size,
                "original size mismatch"
            );
            level.size = level.size - node.event.size as u128 + event.size as u128;
            self.nodes[slot].event.size = event.size;
            if event.flags & HAS_USER != 0 && node.event.flags & HAS_USER == 0 {
                self.nodes[slot].event.user = event.user;
                self.nodes[slot].event.flags |= HAS_USER;
            }
        } else {
            if let Some(previous) = node.previous {
                self.nodes[previous].next = node.next;
            } else {
                level.head = node.next;
            }
            if let Some(next) = node.next {
                self.nodes[next].previous = node.previous;
            } else {
                level.tail = node.previous;
            }
            level.count -= 1;
            level.size -= node.event.size as u128;
            if level.count == 0 {
                self.levels.remove(&key);
            }
            self.by_id.remove(&event.oid);
            self.free.push(slot);
        }
        Ok(())
    }
}

pub struct OrderBook {
    coin: Coin,
    active: State,
    staging: State,
    pending: Vec<Event>,
    pending_header: Option<Header>,
    next_chunk: u32,
    epoch: Option<[u8; 16]>,
    sequence: u64,
    height: u64,
    ready: bool,
    max_orders: usize,
    max_events: usize,
    last_receipt_time_us: u64,
}
impl OrderBook {
    pub fn new(coin: Coin, max_orders: usize, max_events: usize) -> Result<Self> {
        ensure!(max_orders > 0 && max_events > 0, "invalid book capacity");
        Ok(Self {
            coin,
            active: State::default(),
            staging: State::default(),
            pending: Vec::new(),
            pending_header: None,
            next_chunk: 0,
            epoch: None,
            sequence: 0,
            height: 0,
            ready: false,
            max_orders,
            max_events,
            last_receipt_time_us: 0,
        })
    }
    pub fn is_ready(&self) -> bool {
        self.ready
    }
    /// Consumers must also apply their own freshness policy; a silent transport
    /// cannot prove that a quiet market's last book is still live.
    pub fn is_fresh(&self, now_us: u64, max_age_us: u64) -> bool {
        self.ready
            && now_us >= self.last_receipt_time_us
            && now_us - self.last_receipt_time_us <= max_age_us
    }
    pub fn height(&self) -> Option<u64> {
        self.ready.then_some(self.height)
    }
    pub fn len(&self) -> usize {
        if self.ready {
            self.active.by_id.len()
        } else {
            0
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn order(&self, oid: u64) -> Option<&Event> {
        if !self.ready {
            return None;
        }
        self.active
            .by_id
            .get(&oid)
            .map(|slot| &self.active.nodes[*slot].event)
    }
    pub fn orders_at_price(&self, side: Side, price: u64) -> Orders<'_> {
        Orders {
            state: &self.active,
            next: if self.ready {
                self.active
                    .levels
                    .get(&(side, price))
                    .and_then(|level| level.head)
            } else {
                None
            },
        }
    }
    pub fn level_size(&self, side: Side, price: u64) -> Option<u128> {
        if !self.ready {
            return None;
        }
        self.active
            .levels
            .get(&(side, price))
            .map(|level| level.size)
    }
    pub fn invalidate(&mut self) {
        self.ready = false;
        self.pending.clear();
        self.pending_header = None;
        self.next_chunk = 0;
        self.staging.clear();
    }
    /// true = a complete snapshot/block committed. Any error revokes readiness.
    pub fn ingest(&mut self, raw: &[u8]) -> Result<bool> {
        let result = self.ingest_inner(raw);
        if result.is_err() {
            self.invalidate();
        }
        result
    }
    fn ingest_inner(&mut self, raw: &[u8]) -> Result<bool> {
        let (header, records) = Header::decode(raw)?;
        ensure!(header.coin == self.coin, "book coin mismatch");
        ensure!(
            header.chunk_count as usize <= self.max_events,
            "chunk count exceeds consumer bound"
        );
        ensure!(
            header.chunk_count == 1 || !records.is_empty(),
            "empty intermediate chunk"
        );
        if header.kind == Kind::Reset {
            self.invalidate();
            self.epoch = Some(header.epoch);
            self.sequence = 0;
            self.height = 0;
            return Ok(false);
        }
        if header.chunk_index == 0 {
            if header.kind == Kind::Snapshot && self.epoch != Some(header.epoch) {
                self.pending_header = None;
                self.pending.clear();
                self.next_chunk = 0;
            }
            ensure!(self.pending_header.is_none(), "incomplete preceding batch");
            if header.kind == Kind::Snapshot {
                ensure!(
                    header.sequence == 1 && header.previous_height == 0,
                    "invalid snapshot baseline"
                );
                ensure!(
                    !self.ready || self.epoch != Some(header.epoch),
                    "snapshot replay within active epoch"
                );
                self.ready = false;
                self.staging.clear();
            } else {
                ensure!(
                    self.ready && self.epoch == Some(header.epoch),
                    "snapshot required for epoch"
                );
                ensure!(
                    header.sequence == self.sequence.checked_add(1).context("sequence overflow")?
                        && header.previous_height == self.height,
                    "missing L4 batch"
                );
                ensure!(header.height > self.height, "non-increasing block height");
            }
            self.pending_header = Some(header);
            self.next_chunk = 0;
            self.pending.clear();
        }
        let expected = self.pending_header.context("orphan batch chunk")?;
        let mut normalized = header;
        normalized.chunk_index = 0;
        ensure!(
            normalized == expected && header.chunk_index == self.next_chunk,
            "inconsistent/missing batch chunk"
        );
        ensure!(
            self.pending.len() + records.len() / EVENT_BYTES <= self.max_events,
            "consumer event capacity exceeded"
        );
        for raw_event in records.as_chunks::<EVENT_BYTES>().0 {
            let event = Event::decode(raw_event)?;
            if header.kind == Kind::Snapshot {
                ensure!(event.op == Op::Snapshot, "non-snapshot record in snapshot");
            }
            self.pending.push(event);
        }
        self.next_chunk += 1;
        if self.next_chunk < header.chunk_count {
            return Ok(false);
        }
        if header.kind == Kind::Snapshot {
            for &event in &self.pending {
                self.staging.insert(event, self.max_orders)?;
            }
            std::mem::swap(&mut self.active, &mut self.staging);
            self.staging.clear();
        } else {
            // No observer can access the book during this synchronous call. A bad
            // mutation revokes readiness before returning, never exposes partial state.
            for &event in &self.pending {
                self.active.apply(event, self.max_orders)?;
            }
        }
        self.epoch = Some(header.epoch);
        self.sequence = header.sequence;
        self.height = header.height;
        self.ready = true;
        self.last_receipt_time_us = header.receipt_time_us;
        self.pending_header = None;
        self.pending.clear();
        Ok(true)
    }
}
pub struct Orders<'a> {
    state: &'a State,
    next: Option<usize>,
}
impl<'a> Iterator for Orders<'a> {
    type Item = &'a Event;
    fn next(&mut self) -> Option<Self::Item> {
        let slot = self.next?;
        let node = &self.state.nodes[slot];
        self.next = node.next;
        Some(&node.event)
    }
}
