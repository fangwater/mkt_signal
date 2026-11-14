//! One ordered ingress queue. Rare complete responses live in preallocated slots;
//! cloning their audit token does not allocate or copy the large response.
use super::{HotEvent, RawEvent};
use anyhow::{ensure, Result};
use crossbeam_channel::{Receiver, Sender};
use std::cell::UnsafeCell;
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[repr(align(64))]
struct Slot {
    references: AtomicUsize,
    value: UnsafeCell<RawEvent>,
}
pub struct ControlPool {
    slots: Box<[Slot]>,
    free: Receiver<usize>,
    release: Sender<usize>,
}
// A slot is mutable only while its unique index is leased from `free`.
// Once published, every token reads immutable data; the last token returns its
// index with an atomic release before any subsequent writer may obtain it.
unsafe impl Sync for ControlPool {}
impl ControlPool {
    pub fn new(capacity: usize) -> Result<Arc<Self>> {
        ensure!(capacity > 0, "empty CTP control pool");
        let (release, free) = crossbeam_channel::bounded(capacity);
        let mut slots = Vec::new();
        slots.try_reserve_exact(capacity)?;
        for index in 0..capacity {
            slots.push(Slot {
                references: AtomicUsize::new(0),
                value: UnsafeCell::new(RawEvent::default()),
            });
            release
                .try_send(index)
                .map_err(|_| anyhow::anyhow!("initialize CTP control pool"))?;
        }
        Ok(Arc::new(Self {
            slots: slots.into_boxed_slice(),
            free,
            release,
        }))
    }
    pub fn capture(self: &Arc<Self>, value: &RawEvent) -> Option<ControlEvent> {
        let index = self.free.try_recv().ok()?;
        let slot = &self.slots[index];
        unsafe { slot.value.get().write(*value) };
        slot.references.store(1, Ordering::Release);
        Some(ControlEvent {
            pool: self.clone(),
            index,
        })
    }
}
pub struct ControlEvent {
    pool: Arc<ControlPool>,
    index: usize,
}
impl Clone for ControlEvent {
    fn clone(&self) -> Self {
        self.pool.slots[self.index]
            .references
            .fetch_add(1, Ordering::Relaxed);
        Self {
            pool: self.pool.clone(),
            index: self.index,
        }
    }
}
impl Deref for ControlEvent {
    type Target = RawEvent;
    fn deref(&self) -> &RawEvent {
        unsafe { &*self.pool.slots[self.index].value.get() }
    }
}
impl Drop for ControlEvent {
    fn drop(&mut self) {
        if self.pool.slots[self.index]
            .references
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            let _ = self.pool.release.try_send(self.index);
        }
    }
}
#[derive(Clone)]
#[allow(clippy::large_enum_variant)] // Keep compact facts inline in the preallocated queue.
pub enum Event {
    Hot(HotEvent),
    Control(ControlEvent),
}
impl Deref for Event {
    type Target = HotEvent;
    fn deref(&self) -> &HotEvent {
        match self {
            Self::Hot(event) => event,
            Self::Control(event) => &event.hot,
        }
    }
}
#[derive(Clone, Copy)]
pub enum EventRef<'a> {
    Hot(&'a HotEvent),
    Control(&'a RawEvent),
}
impl Event {
    pub fn as_ref(&self) -> EventRef<'_> {
        match self {
            Self::Hot(event) => EventRef::Hot(event),
            Self::Control(event) => EventRef::Control(event),
        }
    }
    pub fn audit(&self, received_us: u64) -> serde_json::Value {
        match self {
            Self::Hot(event) => event.audit(received_us),
            Self::Control(event) => event.audit(received_us),
        }
    }
}
impl<'a> From<&'a RawEvent> for EventRef<'a> {
    fn from(e: &'a RawEvent) -> Self {
        Self::Control(e)
    }
}
impl Deref for EventRef<'_> {
    type Target = HotEvent;
    fn deref(&self) -> &HotEvent {
        match self {
            Self::Hot(e) => e,
            Self::Control(e) => &e.hot,
        }
    }
}
impl EventRef<'_> {
    pub fn control(&self) -> Result<&RawEvent> {
        match self {
            Self::Control(e) => Ok(e),
            Self::Hot(_) => {
                anyhow::bail!("CTP control response arrived without its complete payload")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slot_is_not_reused_until_both_business_and_audit_release_it() {
        let pool = ControlPool::new(1).unwrap();
        let raw = RawEvent {
            available: 123.0,
            ..RawEvent::default()
        };
        let first = pool.capture(&raw).unwrap();
        let audit = first.clone();
        drop(first);
        assert!(pool.capture(&raw).is_none());
        assert_eq!(audit.available, 123.0);
        drop(audit);
        assert!(pool.capture(&raw).is_some());
    }
    #[test]
    fn hot_envelope_does_not_contain_full_account_payload() {
        assert!(std::mem::size_of::<Event>() < std::mem::size_of::<RawEvent>() / 2);
    }
    #[test]
    fn audit_thread_keeps_its_control_slot_until_reading_has_finished() {
        let pool = ControlPool::new(1).unwrap();
        let raw = RawEvent {
            available: 19.5,
            ..RawEvent::default()
        };
        let business = pool.capture(&raw).unwrap();
        let audit = business.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let writer_barrier = barrier.clone();
        let writer = std::thread::spawn(move || {
            assert_eq!(audit.available, 19.5);
            writer_barrier.wait();
            writer_barrier.wait();
            drop(audit);
        });
        barrier.wait();
        drop(business);
        assert!(pool.capture(&raw).is_none());
        barrier.wait();
        writer.join().unwrap();
        assert!(pool.capture(&raw).is_some());
    }
}
