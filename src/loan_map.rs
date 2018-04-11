extern crate owning_ref;

use std::collections::hash_map::RandomState;
use std::hash::{Hash, BuildHasher, Hasher};
use std::mem;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::Deref;
use std::vec::Vec;

use self::owning_ref::{OwningHandle, OwningRef};

const DEFAULT_TABLE_CAPACITY: usize = 128;
const DEFAULT_BUCKET_CAPACITY: usize = 128;

enum Bucket<K, V>
{
    Empty,
    Contains(K, V),
}

impl<K, V> Bucket<K, V>
{
    fn is_free(&self) -> bool {
        match *self {
            Bucket::Empty => true,
            _ => false,
        }
    }

    fn value(self) -> Option<V> {
        if let Bucket::Contains(_, v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn value_ref(&self) -> Result<&V, ()> {
        if let Bucket::Contains(_, ref val) = *self {
            Ok(val)
        } else {
            Err(())
        }
    }

    fn key_matches(&self, key: &K) -> bool
        where K: PartialEq,
    {
        if let Bucket::Contains(ref ckey, _) = *self {
            ckey == key
        } else {
            false
        }
    }
}

struct Table<K, V>
    where K: PartialEq + Hash,
{
    hash_builder: RandomState,
    buckets: Vec<RwLock<Bucket<K, V>>>,
}

impl<K, V> Table<K, V>
    where K: PartialEq + Hash,
{
    fn with_capacity(cap: usize) -> Table<K, V> {
        Table{
            hash_builder: RandomState::new(),
            buckets: (0..cap).map(|_| RwLock::new(Bucket::Empty)).collect(),
        }
    }

    fn hash(&self, key: &K) -> usize {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }

    fn scan<F>(&self, key: &K, matches: F) -> RwLockReadGuard<Bucket<K, V>>
        where F: Fn(&Bucket<K, V>) -> bool,
    {
        let hash = self.hash(key);
        let lock = self.buckets[hash % self.buckets.len()].read().unwrap();

        if matches(&lock) {
            return lock;
        }

        unreachable!();
    }

    fn scan_mut<F>(&self, key: &K, matches: F) -> RwLockWriteGuard<Bucket<K, V>>
        where F: Fn(&Bucket<K, V>) -> bool,
    {
        let hash = self.hash(key);
        let lock = self.buckets[hash % self.buckets.len()].write().unwrap();

        if matches(&lock) {
            return lock;
        }

        unreachable!();
    }

    fn lookup(&self, key: &K) -> RwLockReadGuard<Bucket<K, V>> {
        self.scan(key, |x| {
            if let &Bucket::Contains(ref ckey, _) = x {
                ckey == key
            } else {
                false
            }
        })
    }

    fn lookup_or_free(&self, key: &K) -> RwLockWriteGuard<Bucket<K, V>> {
        self.scan_mut(key, |x| {
            if let &Bucket::Contains(ref ckey, _) = x {
                true
            } else {
                x.is_free()
            }
        })
    }

    fn lookup_mut(&self, key: &K) -> RwLockWriteGuard<Bucket<K, V>> {
        self.scan_mut(key, |x| {
            if let &Bucket::Contains(ref ckey, _) = x {
            ckey == key
            } else {
                false
            }
        })
    }

    fn lookup_free_bucket(&self, key: &K) -> RwLockWriteGuard<Bucket<K, V>> {
        self.scan_mut(key, |x| x.is_free())
    }
}

pub struct ReadGuard<'a, K: 'a + PartialEq + Hash, V: 'a> {
    inner: OwningRef<OwningHandle<RwLockReadGuard<'a, Table<K, V>>, RwLockReadGuard<'a, Bucket<K, V>>>, V>,
}

impl<'a, K, V> Deref for ReadGuard<'a, K, V>
    where K: PartialEq + Hash,
{
    type Target = V;

    fn deref(&self) -> &V {
        &self.inner
    }
}

impl<'a, K, V: PartialEq> PartialEq for ReadGuard<'a, K, V>
    where K: PartialEq + Hash,
{
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

impl<'a, K, V: PartialEq> Eq for ReadGuard<'a, K, V>
    where K: PartialEq + Hash, {}

pub struct LoanMap<K, V>
    where K: PartialEq + Hash,
{
    table: RwLock<Table<K, V>>,
}

impl<K, V> LoanMap<K, V>
    where K: PartialEq + Hash,
{
    pub fn new() -> LoanMap<K, V> {
        LoanMap {
            table: RwLock::new(Table::with_capacity(DEFAULT_TABLE_CAPACITY)),
        }
    }

    pub fn with_capacity(cap: usize) -> LoanMap<K, V> {
        LoanMap {
            table: RwLock::new(Table::with_capacity(cap)),
        }
    }

    pub fn get(&self, key: &K) -> Option<ReadGuard<K, V>> {
        if let Ok(inner) = OwningRef::new(
            OwningHandle::new_with_fn(self.table.read().unwrap(),
                                      |x| unsafe { &*x }.lookup(key)))
            .try_map(|x| x.value_ref()) {
                Some(ReadGuard{inner: inner})
            } else {
                None
            }
    }

    pub fn put(&self, key: K, val: V) -> Option<V> {
        let lock = self.table.read().unwrap();
        let mut bucket = lock.lookup_mut(&key);

        mem::replace(&mut *bucket, Bucket::Contains(key, val)).value()
    }
}

#[cfg(test)]
mod tests {
    use super::LoanMap;

    #[test]
    fn loan_map_insert_then_get() {
        let map: LoanMap<String, String> = LoanMap::new();

        let (key, value) = (String::from("i'm a key"), String::from("i'm a value"));

        assert_eq!(map.put(key, value), None);
        assert_eq!(map.get(&key).map(|x| x.deref()), Some(value));
    }

    #[test]
    fn loan_map_double_insert_updates_value() {
        let map: LoanMap<String, String> = LoanMap::new();

        let key = String::from("i'm a key");
        let value1 = String::from("i'm the first value");
        let value2 = String::from("i'm the second value");

        map.put(key, value1);
        map.put(key, value2);

        assert_eq!(map.get(&key).map(|x| x.deref()), Some(value2));
    }
}
