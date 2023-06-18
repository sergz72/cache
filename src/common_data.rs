use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::TcpStream;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::SystemTime;
use crate::resp_encoder::resp_encode_binary_string;

struct Value {
    value: Vec<u8>,
    created_at: u64,
    expires_at: Option<u64>
}

impl Value {
    fn new(value: Vec<u8>, created_at: u64, expiration: Option<u64>) -> Value {
        let expires_at = expiration.map(|e|created_at + e * 1000);
        Value{
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

struct CommonMaps {
    map: HashMap<Vec<u8>, Value>,
    map_by_time: BTreeMap<u64, HashSet<Vec<u8>>>,
    map_by_expiration: BTreeMap<u64, HashSet<Vec<u8>>>,
}

pub struct CommonData {
    start_time: SystemTime,
    pub verbose: bool,
    max_memory: usize,
    current_memory: AtomicUsize,
    pub configuration: HashMap<Vec<u8>, Vec<u8>>,
    maps: RwLock<CommonMaps>,
    pub exit_flag: AtomicBool,
    pub threads: RwLock<HashMap<usize, Arc<Mutex<TcpStream>>>>
}

impl CommonData {
    fn expire(&self, key: &Vec<u8>) {
        let mut lock = self.maps.write().unwrap();
        let value = lock.map.remove(key).unwrap();
        lock.map_by_expiration.get_mut(&value.expires_at.unwrap()).unwrap().remove(key);
        lock.map_by_time.get_mut(&value.created_at).unwrap().remove(key);
        self.current_memory.fetch_sub(key.len() + value.value.len(), Ordering::Relaxed);
    }

    pub fn get(&self, key: &Vec<u8>, result: &mut Vec<u8>) -> bool {
        let lock = self.maps.read().unwrap();
        let value_option = lock.map.get(key);
        return match value_option {
            Some(value) => {
                if value.is_expired(self.start_time) {
                    drop(lock);
                    self.expire(key);
                    false
                } else {
                    resp_encode_binary_string(value.get_value(), result);
                    true
                }
            },
            None => false
        }
    }

    fn cleanup(&self) {
        //todo
    }

    pub fn add(&self, key: &Vec<u8>, value: &Vec<u8>, expiry: Option<u64>) {
        self.cleanup();
        let created_at = SystemTime::now().duration_since(self.start_time).unwrap().as_millis() as u64;
        let v = Value::new(value.clone(), created_at, expiry);
        let created_at = v.created_at;
        let expires_at = v.expires_at;
        let mut add = key.len() + value.len();
        let mut lock = self.maps.write().unwrap();
        if let Some(vv) = lock.map.insert(key.clone(), v) {
            add -= vv.value.len();
        }
        if let Some(ex) = expires_at {
            match lock.map_by_expiration.get_mut(&ex) {
                Some(v) => { let _ = v.insert(key.clone()); },
                None => {
                    let mut s = HashSet::new();
                    s.insert(key.clone());
                    let _ = lock.map_by_expiration.insert(ex, s);
                }
            };
        }
        match lock.map_by_time.get_mut(&created_at) {
            Some(v) => { let _ = v.insert(key.clone()); },
            None => {
                let mut s = HashSet::new();
                s.insert(key.clone());
                let _ = lock.map_by_time.insert(created_at, s);
            }
        };
        self.current_memory.fetch_add(add, Ordering::Relaxed);
    }

    pub fn size(&self) -> usize {
        self.maps.read().unwrap().map.len()
    }
}

fn build_configuration() -> HashMap<Vec<u8>, Vec<u8>> {
    HashMap::from([
        ("save".to_string().into_bytes(), "".to_string().into_bytes()),
        ("appendonly".to_string().into_bytes(), "no".to_string().into_bytes())])
}

pub fn build_common_data(verbose: bool, max_memory: usize) -> CommonData {
    CommonData{ start_time: SystemTime::now(), verbose, max_memory, current_memory: AtomicUsize::new(0),
        configuration: build_configuration(),
        maps: RwLock::new(CommonMaps{map: HashMap::new(), map_by_time: BTreeMap::new(), map_by_expiration: BTreeMap::new()}),
        exit_flag:  AtomicBool::new(false), threads: RwLock::new(HashMap::new()) }
}
