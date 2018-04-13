extern crate owning_ref;

use std::collections::hash_map::RandomState;
use std::fmt;
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
    Removed,
    Contains(K, V),
}

impl<K, V> Bucket<K, V>
{
    fn is_free(&self) -> bool {
        match *self {
            Bucket::Empty | Bucket::Removed => true,
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

        for i in 0.. {
            let lock = self.buckets[(hash + i) % self.buckets.len()].read().unwrap();

            if matches(&lock) {
                return lock;
            }
        }

        unreachable!();
    }

    fn scan_mut<F>(&self, key: &K, matches: F) -> RwLockWriteGuard<Bucket<K, V>>
        where F: Fn(&Bucket<K, V>) -> bool,
    {
        let hash = self.hash(key);

        for i in 0.. {
            let lock = self.buckets[(hash + i) % self.buckets.len()].write().unwrap();

            if matches(&lock) {
                return lock;
            }
        }

        unreachable!();
    }

    fn lookup(&self, key: &K) -> RwLockReadGuard<Bucket<K, V>> {
        self.scan(key, |x| match *x {
            Bucket::Contains(ref ckey, _) if ckey == key => true,
            Bucket::Removed => true,
            _ => false,
        })
    }

    #[inline]
    fn match_free(bucket: &Bucket<K, V>) -> bool {
        if let &Bucket::Contains(ref ckey, _) = bucket {
            true
        } else {
            bucket.is_free()
        }
    }

    fn lookup_or_free(&self, key: &K) -> RwLockWriteGuard<Bucket<K, V>> {
        self.scan_mut(key, Table::match_free)
    }

    fn lookup_or_free_mut(&self, key: &K) -> RwLockWriteGuard<Bucket<K, V>> {
        self.scan_mut(key, Table::match_free)
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
}

pub struct ReadGuard<'a, K: 'a + PartialEq + Hash, V: 'a> {
    inner: OwningRef<OwningHandle<RwLockReadGuard<'a, Table<K, V>>, RwLockReadGuard<'a, Bucket<K, V>>>, V>,
}

impl<'a, K: Hash + PartialEq, V: fmt::Display> fmt::Debug for ReadGuard<'a, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ReadGard: {}", *self.inner)
    }
}

impl<'a, K, V> Deref for ReadGuard<'a, K, V>
    where K: PartialEq + Hash,
{
    type Target = V;

    fn deref(&self) -> &V {
        &*self.inner
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
        let mut bucket = lock.lookup_or_free_mut(&key);

        mem::replace(&mut *bucket, Bucket::Contains(key, val)).value()
    }
}

#[cfg(test)]
mod loan_map_tests {
    extern crate test;
    extern crate rand;

    use self::rand::distributions::Alphanumeric;
    use self::rand::Rng;
    use self::test::Bencher;
    use super::LoanMap;

    #[test]
    fn insert_then_get() {
        let map: LoanMap<String, String> = LoanMap::new();

        let (key, value) = (String::from("i'm a key"), String::from("i'm a value"));

        assert_eq!(map.put(key.clone(), value.clone()), None);

        let val = map.get(&key).expect("no value in map after insert");
        assert_eq!(*val, value);
    }

    #[test]
     fn get_inexistent_value() {
        let map: LoanMap<String, String> = LoanMap::new();

        assert_eq!(map.get(&String::from("some key")), None);
    }

    #[test]
    fn double_insert_updates_value() {
        let map: LoanMap<String, String> = LoanMap::new();

        let key = String::from("i'm a key");
        let value1 = String::from("i'm the first value");
        let value2 = String::from("i'm the second value");

        map.put(key.clone(), value1);
        map.put(key.clone(), value2.clone());

        let from_map = map.get(&key).expect("map does not insert");

        assert_eq!(*from_map, value2);
    }

    #[bench]
    fn bench_access_string_key_string_value(b: &mut Bencher) {
        let set_size = 8192;
        let key_size = 128;
        let map: LoanMap<String, String> = LoanMap::with_capacity(8192);
        let mut rng = rand::thread_rng();

        let keys: Vec<String> = (0..set_size).map(|_| {
            rng.gen_ascii_chars()
                .take(key_size)
                .collect::<String>()
        }).collect();

        for key in &keys {
            map.put(key.clone(), rng.gen_ascii_chars().take(1024).collect::<String>());
        }

        b.iter(|| {
            let key = &keys[rng.gen::<usize>() % set_size];

            assert_eq!(map.get(key).is_some(), true);
        })
    }
}
