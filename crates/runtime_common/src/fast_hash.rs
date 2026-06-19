use ahash::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

pub type FastHashMap<K, V> = HashMap<K, V, RandomState>;
pub type FastHashSet<T> = HashSet<T, RandomState>;

#[inline]
pub fn fast_hash_map<K, V>() -> FastHashMap<K, V> {
    HashMap::with_hasher(RandomState::new())
}

#[inline]
pub fn fast_hash_map_with_capacity<K, V>(capacity: usize) -> FastHashMap<K, V> {
    HashMap::with_capacity_and_hasher(capacity, RandomState::new())
}

#[inline]
pub fn fast_hash_set<T>() -> FastHashSet<T> {
    HashSet::with_hasher(RandomState::new())
}

#[inline]
pub fn fast_hash_set_with_capacity<T>(capacity: usize) -> FastHashSet<T> {
    HashSet::with_capacity_and_hasher(capacity, RandomState::new())
}

#[inline]
pub fn fast_hash_set_from_iter<T, I>(iter: I) -> FastHashSet<T>
where
    T: Eq + Hash,
    I: IntoIterator<Item = T>,
{
    let iter = iter.into_iter();
    let (lower, _) = iter.size_hint();
    let mut out = fast_hash_set_with_capacity(lower);
    out.extend(iter);
    out
}
