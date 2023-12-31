use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::RwLock;
use std::time::SystemTime;
use crate::common_maps::GetResult::{Expired, Found, NotFound};
use crate::resp_encoder::resp_encode_binary_string;

struct Value {
    value: Vec<u8>,
    created_at: u64,
    expires_at: Option<u64>,
}

impl Value {
    fn new(value: Vec<u8>, created_at: u64, expiration: Option<u64>) -> Value {
        let expires_at = expiration.map(|e| created_at + e);
        Value {
            value,
            created_at,
            expires_at,
        }
    }

    fn is_expired(&self, start_time: SystemTime) -> bool {
        if let Some(e) = self.expires_at {
            let now = SystemTime::now().duration_since(start_time).unwrap().as_millis() as u64;
            if now >= e {
                return true;
            }
        }
        false
    }

    fn get_value(&self) -> &Vec<u8> {
        &self.value
    }
}

pub struct CommonMaps {
    max_memory: usize,
    current_memory: usize,
    map: HashMap<Vec<u8>, Value>,
    map_by_time: BTreeMap<u64, HashSet<Vec<u8>>>,
    map_by_expiration: BTreeMap<u64, HashSet<Vec<u8>>>,
}

fn build_map(max_memory: usize) -> CommonMaps {
    CommonMaps {
        current_memory: 0,
        max_memory,
        map: HashMap::new(),
        map_by_time: BTreeMap::new(),
        map_by_expiration: BTreeMap::new(),
    }
}

pub fn build_maps(vector_size: usize, all_memory: usize) -> Vec<RwLock<CommonMaps>> {
    let max_memory = all_memory / vector_size;
    (0..vector_size)
        .map(|_i| RwLock::new(build_map(max_memory)))
        .collect()
}

#[derive(PartialEq, Debug)]
pub enum GetResult {
    NotFound,
    Found,
    Expired,
}

fn calculate_record_size(key_size: usize, value_size: usize) -> usize {
    3 * key_size + value_size + 16
}

impl CommonMaps {
    pub fn flush(&mut self) {
        self.current_memory = 0;
        self.map.clear();
        self.map_by_expiration.clear();
        self.map_by_time.clear();
    }

    fn remove_from_btree(&mut self, key: &Vec<u8>, value: Value) {
        if let Some(ex) = value.expires_at {
            let h = self.map_by_expiration.get_mut(&ex).unwrap();
            if h.len() == 1 {
                self.map_by_expiration.remove(&ex);
            } else {
                h.remove(key);
            }
        }
        let h = self.map_by_time.get_mut(&value.created_at).unwrap();
        if h.len() == 1 {
            self.map_by_time.remove(&value.created_at);
        } else {
            h.remove(key);
        }
    }

    pub fn removekey(&mut self, key: &Vec<u8>) -> isize {
        if let Some(value) = self.map.remove(key) {
            self.current_memory -= calculate_record_size(key.len(), value.value.len());
            self.remove_from_btree(key, value);
            return 1;
        }
        0
    }

    pub fn removekeys(&mut self, keys: Vec<&Vec<u8>>) -> isize {
        keys.into_iter().map(|k| self.removekey(k)).sum()
    }

    pub fn get(&self, key: &Vec<u8>, result: &mut Vec<u8>, start_time: SystemTime) -> GetResult {
        return match self.map.get(key) {
            Some(value) => {
                if value.is_expired(start_time) {
                    Expired
                } else {
                    resp_encode_binary_string(value.get_value(), result);
                    Found
                }
            }
            None => NotFound
        };
    }

    fn remove_expired(&mut self, start_time: SystemTime) {
        let now = SystemTime::now().duration_since(start_time).unwrap().as_millis() as u64;
        let mut to_remove = Vec::new();
        for (k, v) in &self.map_by_expiration {
            let kk = *k;
            if kk < now {
                for k in v {
                    to_remove.push(k.clone());
                }
            }
        }
        for k in to_remove {
            self.removekey(&k);
        }
    }

    fn cleanup(&mut self, start_time: SystemTime) {
        if self.current_memory >= self.max_memory {
            self.remove_expired(start_time);
            while self.current_memory >= self.max_memory {
                //remove by lru
                let (_k, v) = self.map_by_time.first_key_value().unwrap();
                v.clone().iter().for_each(|k| { let _ = self.removekey(k); });
            }
        }
    }

    pub fn set(&mut self, key: &Vec<u8>, value: &Vec<u8>, expiry: Option<u64>, start_time: SystemTime) {
        let size = calculate_record_size(key.len(), value.len());
        self.current_memory += size;
        self.cleanup(start_time);
        let created_at = SystemTime::now().duration_since(start_time).unwrap().as_millis() as u64;
        let v = Value::new(value.clone(), created_at, expiry);
        let created_at = v.created_at;
        let expires_at = v.expires_at;
        if let Some(old) = self.map.insert(key.clone(), v) {
            self.current_memory = self.current_memory - size + value.len() - old.value.len();
            self.remove_from_btree(key, old);
        }
        if let Some(ex) = expires_at {
            match self.map_by_expiration.get_mut(&ex) {
                Some(v) => { let _ = v.insert(key.clone()); }
                None => {
                    let mut s = HashSet::new();
                    s.insert(key.clone());
                    self.map_by_expiration.insert(ex, s);
                }
            };
        }
        match self.map_by_time.get_mut(&created_at) {
            Some(v) => { let _ = v.insert(key.clone()); }
            None => {
                let mut s = HashSet::new();
                s.insert(key.clone());
                self.map_by_time.insert(created_at, s);
            }
        };
    }

    pub fn size(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::{Duration, SystemTime};
    use rand::distributions::{Alphanumeric, DistString};
    use rand::Rng;
    use crate::common_maps::build_map;
    use crate::common_maps::GetResult::{Expired, Found, NotFound};

    #[test]
    fn test_set_delete() {
        let mut rng = rand::thread_rng();
        let mut keys = Vec::new();
        let mut maps = build_map(100000000);
        let start_time = SystemTime::now();
        for _i in 0..1000 {
            let key_length = (rng.gen::<usize>() % 100) + 10;
            let value_length = (rng.gen::<usize>() % 200) + 10;
            let key = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
            let value = Alphanumeric.sample_string(&mut rng, value_length).into_bytes();
            maps.set(&key, &value, None, start_time);
            keys.push(key);
        }

        for key in &keys {
            let value_length = (rng.gen::<usize>() % 200) + 10;
            let value = Alphanumeric.sample_string(&mut rng, value_length).into_bytes();
            maps.set(key, &value, None, start_time);
        }

        for key in keys {
            maps.removekey(&key);
        }

        assert_eq!(maps.map.len(), 0);
        assert_eq!(maps.map_by_time.len(), 0);
        assert_eq!(maps.map_by_expiration.len(), 0);
        assert_eq!(maps.current_memory, 0);
    }

    #[test]
    fn test_cleanup() {
        let mut rng = rand::thread_rng();
        let mut maps = build_map(100000);
        let start_time = SystemTime::now();
        for _i in 0..1000 {
            let key_length = (rng.gen::<usize>() % 100) + 10;
            let value_length = (rng.gen::<usize>() % 200) + 10;
            let key = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
            let value = Alphanumeric.sample_string(&mut rng, value_length).into_bytes();
            maps.set(&key, &value, None, start_time);
        }

        assert!(maps.current_memory - 1000 < maps.max_memory);
    }

    #[test]
    fn test_cleanup2() {
        let mut rng = rand::thread_rng();
        let mut maps = build_map(100000);
        let start_time = SystemTime::now();
        for _i in 0..1000 {
            let key_length = (rng.gen::<usize>() % 100) + 10;
            let value_length = (rng.gen::<usize>() % 200) + 10;
            let key = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
            let value = Alphanumeric.sample_string(&mut rng, value_length).into_bytes();
            maps.set(&key, &value, Some(100), start_time);
        }

        thread::sleep(Duration::from_millis(200));

        let key_length = (rng.gen::<usize>() % 100) + 10;
        let key = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
        let value = Alphanumeric.sample_string(&mut rng, 20000).into_bytes();
        maps.set(&key, &value, None, start_time);

        assert_eq!(maps.size(), 1);
    }

    #[test]
    fn test_set_get() {
        let mut rng = rand::thread_rng();
        let mut maps = build_map(1000000);
        let start_time = SystemTime::now();
        let key_length = (rng.gen::<usize>() % 100) + 10;
        let value_length = (rng.gen::<usize>() % 200) + 10;
        let key = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
        let key2 = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
        let value = Alphanumeric.sample_string(&mut rng, value_length).into_bytes();
        maps.set(&key, &value, Some(100), start_time);

        let mut result = Vec::new();

        assert_eq!(maps.get(&key, &mut result, start_time), Found);
        assert_eq!(maps.get(&key2, &mut result, start_time), NotFound);

        thread::sleep(Duration::from_millis(200));

        assert_eq!(maps.get(&key, &mut result, start_time), Expired);
    }
}