#![feature(test)]
#![feature(integer_atomics)]

mod map;
mod loan_map;

use self::map::ConcurrentHashMap;

fn main() {
    let map: ConcurrentHashMap<u32, String> = ConcurrentHashMap::new(32, 32);

    for i in 0..10 {
        map.insert(&(i as u32), i.to_string());
    }

    for i in 0..10 {
        println!("{:?}", map.get(&i).unwrap());
    }
}

#[cfg(test)]
mod tests {
    extern crate test;
    use map::ConcurrentHashMap;
    use self::test::Bencher;

    const TEST_SET_SIZE: u32 = 4096;

    #[test]
    fn non_existent_key_is_none() {
        let map: ConcurrentHashMap<String, u32> = ConcurrentHashMap::new(32, 32);
        let s = String::from("non_existent");
        assert_eq!(map.get(&s), None);
    }

    #[bench]
    fn bench_access_32_32(b: &mut Bencher) {
        let map: ConcurrentHashMap<String, u32> = ConcurrentHashMap::new(32, 32);

        for i in 0..TEST_SET_SIZE {
            map.insert(&i.to_string(), i);
        }

        b.iter(|| for i in 0..TEST_SET_SIZE { map.get(&(i.to_string())); })
    }

    #[test]
    fn existing_key_returns_some_value() {
        let map: ConcurrentHashMap<String, u32> = ConcurrentHashMap::new(32, 32);
        let k = String::from("valid key");
        let v = 2000;

        map.insert(&k, v);

        assert_eq!(map.get(&k), Some(2000));
    }

    #[test]
    fn remove_test() {
        let map: ConcurrentHashMap<String, u32> = ConcurrentHashMap::new(32, 32);
        let k = String::from("valid key");
        let v = 2000;

        map.insert(&k, v);
        map.remove(&k);
        assert_eq!(map.get(&k), None);
    }
}
