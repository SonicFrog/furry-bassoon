use std::collections::hash_map::DefaultHasher;
use std::cell::{Ref, RefCell};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::{RwLock, RwLockReadGuard};
use std::ops::DerefMut;
use std::vec::Vec;

pub struct LoanMap<'a, K: 'a, V: 'a>
    where K: Eq + Hash,
{
    bucket_count: usize,
    buckets: Vec<RwLock<InternalBucket<'a, K, V>>>,
}

impl<'a, K, V> LoanMap<'a, K, V>
    where K: Eq + Hash,
{
    fn hash(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.bucket_count
    }

    pub fn new(count: usize, size: usize) -> LoanMap<'a, K, V> {
        LoanMap{
            bucket_count: count,
            buckets: Vec::with_capacity(size),
        }
    }

    pub fn get(&self, key: &K) -> Option<Rc<RwLockReadGuard<&'a V>>> {
        let bucket = &self.buckets[self.hash(key)];

        bucket.read().unwrap().entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| entry.get())
    }

    pub fn put(&self, key: &'a K, val: &'a V) {
        let mut guard = self.buckets[self.hash(key)].write().unwrap();
        guard.deref_mut().insert(key, val);
    }
}

struct InternalBucket<'a, K: 'a, V: 'a>
    where K: Eq + Hash,
{
    entries: Vec<Rc<InternalEntry<'a, K, V>>>,
}

impl<'a, K, V> InternalBucket<'a, K, V>
    where K: Eq + Hash,
{
    /// Get a value for some key in this bucket
    #[inline]
    fn get(&self, key: &K) -> Option<Rc<InternalEntry<'a, K, V>>> {
        self.entries
            .iter()
            .find(|entry|
                  *entry.key == *key
            )
            .map(|entry| Rc::clone(&entry))
    }

    /// Inserts a new key/value pair in this bucket
    /// The key must hash to the index of this bucket in this map!
    #[inline]
    fn insert(&mut self, key: &'a K, val: &'a V) {
        if self.contains(key) {
            self.entries
                .iter()
                .find(|entry| {
                *entry.key == *key
            }).unwrap().replace(val);
        } else {
            self.entries.push(
                Rc::new(InternalEntry{
                    key: key,
                    value: RefCell::new(RwLock::new(val)),
                })
            );
        }
    }

    fn clean(&mut self, key: &K) -> usize {
        for i in 0..self.entries.len() {
            let entry = self.entries[i];

            if entry.key != key {
                continue;
            }

            let lock = entry.try_write();

            if entry.is_ok() {
                self.entries.remove(i);
            }
        }
    }

    /// Checks whether this bucket contains a given key
    #[inline]
    fn contains(&self, key: &'a K) -> bool {
        self.entries
            .iter()
            .find(|entry| *entry.key == *key)
            .is_some()
    }

    /// Removes all matching keys from this bucket
    #[inline]
    fn remove(&mut self, key: &'a K) {
        for i in 0..self.entries.len() {
            if self.entries[i].key == key {
                self.entries.remove(i);
            }
        }
    }
}

struct InternalEntry<'a, K: 'a, V: 'a>
    where K: Eq + Hash,
{
    key: &'a K,
    value: RwLock<&'a V>,
}

impl <'a, K, V> InternalEntry<'a, K, V>
    where K: Eq + Hash,
          V: Sized,
{
    /// Returns a read locked reference counted pointer to this value
    /// The lock is only released when all copies of this Rc are dropped
    #[inline]
    fn get(&self) -> Rc<RwLockReadGuard<&'a V>> {
        Rc::new(self.value.read().unwrap())
    }

    /// Gets the entry out while keeping the lock locked inside the map
    #[inline]
    fn get_locked(&self) -> Rc<LockedMapEntry<'a, K, V>> {
        Rc::new(LockedMapEntry {
            key: self.key.clone(),
            value: Rc::new(self.value.read().unwrap()),
        })
    }

    /// Replaces the value of this entry and returns the old one
    #[inline]
    fn replace(&self, new: &'a V) {
        self.value.replace(RwLock::new(new));
    }
}

/// Holds the guard that locks the actual entry inside the map
pub struct LockedMapEntry<'a, K: 'a, V: 'a>
    where K: Hash + Eq
{
    key: &'a K,
    value: Rc<RwLockReadGuard<'a, &'a V>>,
}

impl<'a, K, V> Clone for LockedMapEntry<'a, K, V>
    where K: Hash + Eq,
{
    fn clone(&self) -> Self {
        LockedMapEntry{
            key: self.key,
            value: self.value.clone(),
        }
    }
}
