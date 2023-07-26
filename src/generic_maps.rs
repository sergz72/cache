use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

pub trait RecordSizeCalculator<K, V> {
    fn calculate_record_size(&self, key: &K, value: &V) -> usize;
}

struct GenericValue<V> {
    value: V,
    last_access_time: AtomicU64,
    expires_at: Option<u64>,
    is_updated: bool
}

pub struct GenericMaps<K, V, C> {
    max_memory: usize,
    current_memory: usize,
    use_map_by_time: bool,
    map: HashMap<K, GenericValue<V>>,
    map_by_time: BTreeMap<u64, HashSet<K>>,
    map_by_expiration: BTreeMap<u64, HashSet<K>>,
    record_size_calculator: C
}

impl<V> GenericValue<V> {
    pub fn new(value: V, created_at: u64, expiration: Option<u64>) -> GenericValue<V> {
        let expires_at = expiration.map(|e| created_at + e);
        GenericValue {
            value,
            last_access_time: AtomicU64::new(created_at),
            expires_at,
            is_updated: false
        }
    }

    pub fn is_expired(&self, start_time: SystemTime) -> bool {
        if let Some(e) = self.expires_at {
            let now = SystemTime::now().duration_since(start_time).unwrap().as_millis() as u64;
            if now >= e {
                return true;
            }
        }
        false
    }
}

impl<K: Eq + Hash + Clone, V, C: RecordSizeCalculator<K, V>> GenericMaps<K, V, C> {
    pub fn new(use_map_by_time: bool, record_size_calculator: C, max_memory: usize) -> GenericMaps<K, V, C> {
        GenericMaps{
            max_memory,
            current_memory: 0,
            use_map_by_time,
            map: HashMap::new(),
            map_by_time: BTreeMap::new(),
            map_by_expiration: BTreeMap::new(),
            record_size_calculator
        }
    }

    fn update_map_by_time(&mut self, key: &K, value: &GenericValue<V>, start_time: SystemTime) {
        let now = SystemTime::now().duration_since(start_time).unwrap().as_millis() as u64;
        let old = value.last_access_time.swap(now, Ordering::Relaxed);
        if now != old {
            self.remove_from_map_by_time(key, old);
            self.insert_into_map_by_time(key, now);
        }
    }

    pub fn clear(&mut self) {
        self.current_memory = 0;
        self.map.clear();
        self.map_by_time.clear();
        self.map_by_expiration.clear();
    }

    fn remove_from_map_by_time(&mut self, key: &K, value: u64) {
        let h = self.map_by_time.get_mut(&value).unwrap();
        if h.len() == 1 {
            self.map_by_time.remove(&value);
        } else {
            h.remove(key);
        }
    }

    fn first_value(&self) -> HashSet<K> {
        self.map_by_time.first_key_value().unwrap().1.clone()
    }

    fn insert_into_map_by_time(&mut self, key: &K, created_at: u64) {
        match self.map_by_time.get_mut(&created_at) {
            Some(v) => { let _ = v.insert(key.clone()); }
            None => {
                let mut s = HashSet::new();
                s.insert(key.clone());
                self.map_by_time.insert(created_at, s);
            }
        };
    }

    fn remove_from_aux_maps(&mut self, key: &K, value: &GenericValue<V>) {
        if let Some(ex) = value.expires_at {
            let h = self.map_by_expiration.get_mut(&ex).unwrap();
            if h.len() == 1 {
                self.map_by_expiration.remove(&ex);
            } else {
                h.remove(key);
            }
        }
        if self.use_map_by_time {
            self.remove_from_map_by_time(key, value.last_access_time.load(Ordering::Relaxed));
        }
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

    fn add_to_aux_maps(&mut self, key: &K, value: &GenericValue<V>) {
        if let Some(ex) = value.expires_at {
            match self.map_by_expiration.get_mut(&ex) {
                Some(v) => { let _ = v.insert(key.clone()); }
                None => {
                    let mut s = HashSet::new();
                    s.insert(key.clone());
                    self.map_by_expiration.insert(ex, s);
                }
            };
        }
        if self.use_map_by_time {
            self.insert_into_map_by_time(key, value.last_access_time.load(Ordering::Relaxed));
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn removekey(&mut self, key: &K) -> isize {
        if let Some(value) = self.map.remove(key) {
            self.current_memory -= self.record_size_calculator.calculate_record_size(key, &value.value);
            self.remove_from_aux_maps(key, &value);
            return 1;
        }
        0
    }

    pub fn removekeys(&mut self, keys: Vec<&K>) -> isize {
        keys.into_iter().map(|k| self.removekey(k)).sum()
    }

    fn cleanup(&mut self, start_time: SystemTime) -> bool {
        if self.current_memory >= self.max_memory {
            self.remove_expired(start_time);
            if self.use_map_by_time {
                while self.current_memory >= self.max_memory {
                    //remove by lru
                    let v = self.first_value();
                    v.clone().iter().for_each(|k| { let _ = self.removekey(k); });
                }
            } else if self.current_memory >= self.max_memory {
                return false;
            }
        }
        true
    }
}