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

struct Table<K: PartialEq, V>
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

        for i in 0..self.buckets.len() {
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

        for i in 0..self.buckets.len() {
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
            Bucket::Empty => true,
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
        self.scan_mut(key, |x| {
            if let Bucket::Contains(ref ckey, _) = *x {
                ckey == key
            } else {
                true
            }
        })
    }

    fn remove(&self, key: &K) -> Option<V> {
        let mut bucket = self.scan_mut(key, |x| {
            match *x {
                Bucket::Contains(ref ckey, _) => ckey == key,
                _ => false,
            }
        });

        mem::replace(&mut *bucket, Bucket::Removed).value()
    }

    fn lookup_or_free_mut(&self, key: &K) -> RwLockWriteGuard<Bucket<K, V>> {
        self.scan_mut(key, |x| {
            match *x {
                Bucket::Contains(ref ckey, _) => ckey == key,
                _ => true
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

    pub fn remove(&self, key: K) -> Option<V> {
        let lock = self.table.read().unwrap();
        let mut bucket = lock.lookup_or_free_mut(&key);

        mem::replace(&mut *bucket, Bucket::Removed).value()
    }
}

#[cfg(test)]
mod tests {
    extern crate crossbeam_utils;
    extern crate time;
    extern crate test;
    extern crate rand;

    use std::sync::atomic::{AtomicI64, Ordering};
    use std::thread;

    use self::crossbeam_utils::scoped;
    use self::rand::distributions::Alphanumeric;
    use self::rand::Rng;
    use self::test::Bencher;

    use super::LoanMap;

    /// Generates a random map with `set_size` elements and keys of size `key_size`
    fn gen_rand_map(set_size: usize, key_size: usize) -> (LoanMap<String, String>, Vec<String>) {
        let map: LoanMap<String, String> = LoanMap::with_capacity(16384);
        let mut rng = rand::thread_rng();

        let keys: Vec<String> = (0..set_size).map(|_| {
            rng.gen_ascii_chars()
                .take(key_size)
                .collect::<String>()
        }).collect();

        for key in &keys {
            map.put(key.clone(), rng.gen_ascii_chars().take(1024).collect::<String>());
        }

        (map, keys)
    }

    #[test]
    fn insert_then_get() {
        let map: LoanMap<String, String> = LoanMap::new();

        let (key, value) = (String::from("i'm a key"), String::from("i'm a value"));

        assert_eq!(map.put(key.clone(), value.clone()), None);

        let val = map.get(&key).expect("no value in map after insert");
        assert_eq!(*val, value);
    }

    #[test]
    fn insert_then_remove() {
        let map: LoanMap<String, String> = LoanMap::new();
        let (key, value) = (String::from("key1"), String::from("value1"));

        assert_eq!(map.put(key.clone(), value.clone()), None);
        assert_eq!(map.remove(key), Some(value));
    }

    #[test]
    fn get_inexistent_value() {
        let map: LoanMap<String, String> = LoanMap::new();

        assert_eq!(map.get(&String::from("some key")), None);
    }

    #[test]
    fn concurrent_insert() {
        let keys: Vec<String> = vec!["key1", "key2", "key3", "key4"]
            .iter()
            .map(|x| String::from(*x)).collect();
        let map: LoanMap<String, u32> = LoanMap::with_capacity(8192);
        let thread_num = 15;
        const NUM_PUT: usize = 2048;
        const NUM_GET: usize = 4096;
        let guards : Vec<ScopedJoinHandle<()>> = Vec::new();
        let get_time = AtomicI64::new(0);
        let put_time = AtomicI64::new(0);

        fn make_get_thread(scope: Scope, time_counter: &AtomicI64) {
            guards.push(scope.spawn(|| {
                for i in 0..NUM_PUT {
                    let key = &keys[i % keys.len()];
                    let start = time::now();

                    {
                        let guard = map.get(key).expect("missing key");
                        map.put(*key, *guard + 1);
                    } // let the thread do all map related ops and then calculate runtime

                    put_time.fetch_add((time::now() - start).num_nanoseconds().expect("overflow!"),
                                       Ordering::Relaxed);
                }
            }));
        }

        // TODO: adapt this for crossbeam scopes
        fn make_put_thread(scope: Scope, time_counter: &AtomicI64) {
            guards.push(scope.spawn(|| {
                for i in 0..NUM_GET {
                    let key = &keys[i % keys.len()];
                    let start = time::now();

                    {
                        map.get(key).expect("missing key");
                    }

                    get_time.fetch_add((time::now() -start).num_nanoseconds().expect("overflow!"),
                                       Ordering::Relaxed);
                }
            }));
        }

        let scope = scoped::scope(|s| {
            s.defer(println!("done running benchmark"));
            for i in 0..thread_num {
                if i % 2 == 0 {
                    // TODO: spawn get thread
                } else {
                    // TODO: spawn put thread
                }
                s.spawn(|| {
                });

                for i in &keys {
                    map.put(*i, 0);
                }

                let put_guard = scope.scoped();

                let get_guards = (0..thread_num - 1).map(|_| {
                    scope.scoped(|| {
                        let mut rng = rand::thread_rng();
                        let key = &keys[rng.gen::<usize>() % keys.len()];
                        let now = time::now();
                        assert_eq!(map.get(key).is_some(), true);
                    })
                });

                for g in get_guards {
                    g.join();
                }

                // compute time per get/put
                let per_put = put_time.load(Ordering::Relaxed) / NUM_PUT as i64;
                let per_get = get_time.load(Ordering::Relaxed) / (NUM_GET * (thread_num - 1)) as i64;

                println!("GET time: {}ns", per_get);
                println!("PUT time: {}ns", per_put);
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
                let mut rng = rand::thread_rng();
                let key_size = 128;
                let set_size = 8192;
                let (map, keys) = gen_rand_map(8192, 128);

                b.iter(|| {
                    let key = &keys[rng.gen::<usize>() % set_size];

                    assert_eq!(map.get(key).is_some(), true);
                })
            }

            #[bench]
            fn bench_access_u32_key_u32_value(b: &mut Bencher) {
                let mut rng = rand::thread_rng();
                let ksize: usize = 128;
                let ssize: usize = 8192;
                let keys: Vec<u32> = (0..ssize).map(|_| rng.gen::<u32>()).collect();
                let map: LoanMap<u32, u32> = LoanMap::with_capacity(2 * ssize);

                for key in &keys {
                    map.put(*key, rng.gen::<u32>());
                }

                b.iter(|| {
                    let key = &keys[rng.gen::<usize>() % ssize];
                    assert_eq!(map.get(key).is_some(), true);
                })
            }
        }
