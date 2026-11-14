//! An eagerly removable deadline heap. Idle polling reads one head; completed
//! requests leave no tombstones or historical scan work behind.
use std::{collections::HashMap, hash::Hash, time::Instant};

struct Entry<K, V> {
    key: K,
    deadline: Instant,
    value: V,
}
pub struct DeadlineQueue<K, V> {
    heap: Vec<Entry<K, V>>,
    positions: HashMap<K, usize>,
}
impl<K: Copy + Eq + Hash, V> Default for DeadlineQueue<K, V> {
    fn default() -> Self {
        Self {
            heap: Vec::with_capacity(256),
            positions: HashMap::with_capacity(256),
        }
    }
}
impl<K: Copy + Eq + Hash, V> DeadlineQueue<K, V> {
    pub fn len(&self) -> usize {
        self.heap.len()
    }
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        self.heap.first().map(|n| n.deadline)
    }
    pub fn insert(&mut self, key: K, deadline: Instant, value: V) -> Result<(), V> {
        if self.positions.contains_key(&key) {
            return Err(value);
        }
        let i = self.heap.len();
        self.positions.insert(key, i);
        self.heap.push(Entry {
            key,
            deadline,
            value,
        });
        self.up(i);
        Ok(())
    }
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let i = self.positions.remove(key)?;
        let removed = self.heap.swap_remove(i);
        if i < self.heap.len() {
            self.positions.insert(self.heap[i].key, i);
            if i > 0 && self.heap[i].deadline < self.heap[(i - 1) / 2].deadline {
                self.up(i)
            } else {
                self.down(i)
            }
        }
        Some(removed.value)
    }
    pub fn pop_due(&mut self, now: Instant) -> Option<(K, V)> {
        let first = self.heap.first()?;
        if first.deadline > now {
            return None;
        }
        let key = first.key;
        self.remove(&key).map(|v| (key, v))
    }
    pub fn clear(&mut self) {
        self.heap.clear();
        self.positions.clear();
    }
    fn swap(&mut self, a: usize, b: usize) {
        self.heap.swap(a, b);
        self.positions.insert(self.heap[a].key, a);
        self.positions.insert(self.heap[b].key, b);
    }
    fn up(&mut self, mut i: usize) {
        while i > 0 {
            let p = (i - 1) / 2;
            if self.heap[p].deadline <= self.heap[i].deadline {
                break;
            }
            self.swap(p, i);
            i = p;
        }
    }
    fn down(&mut self, mut i: usize) {
        loop {
            let left = i * 2 + 1;
            if left >= self.heap.len() {
                break;
            }
            let right = left + 1;
            let c = if right < self.heap.len()
                && self.heap[right].deadline < self.heap[left].deadline
            {
                right
            } else {
                left
            };
            if self.heap[i].deadline <= self.heap[c].deadline {
                break;
            }
            self.swap(i, c);
            i = c;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, time::Duration};
    #[test]
    fn cancellation_and_expiration_match_a_reference_queue() {
        let now = Instant::now();
        let mut q = DeadlineQueue::default();
        let mut reference = BTreeMap::new();
        for i in 0..2000u64 {
            let due = now + Duration::from_nanos((i * 997) % 2000);
            q.insert(i, due, i).unwrap();
            reference.insert((due, i), i);
        }
        for i in (0..2000u64).step_by(3) {
            let due = now + Duration::from_nanos((i * 997) % 2000);
            assert_eq!(q.remove(&i), Some(i));
            reference.remove(&(due, i));
        }
        while let Some((due, _)) = reference.first_key_value().map(|(k, v)| (*k, *v)) {
            let (key, value) = q.pop_due(due.0).unwrap();
            assert_eq!(reference.remove(&(due.0, key)), Some(value));
            assert_eq!(q.len(), reference.len());
        }
        assert!(q.is_empty());
        assert_eq!(q.next_deadline(), None);
    }
    #[test]
    fn completed_requests_have_no_history_limit_or_stale_deadlines() {
        let mut q = DeadlineQueue::default();
        let now = Instant::now();
        for i in 0..250_000u64 {
            q.insert(i, now, i).unwrap();
            assert_eq!(q.remove(&i), Some(i));
            assert!(q.is_empty());
        }
        q.insert(1, now, 2).unwrap();
        assert_eq!(q.insert(1, now, 3), Err(3));
        assert_eq!(q.pop_due(now), Some((1, 2)));
    }
}
