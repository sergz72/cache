use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::TcpStream;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::AtomicBool;
use std::time::SystemTime;
use crate::common_data::GetResult::{Expired, Found, NotFound};
use crate::resp_encoder::resp_encode_binary_string;

struct Value {
    value: Vec<u8>,
    created_at: u64,
    expires_at: Option<u64>
}

impl Value {
    fn new(value: Vec<u8>, created_at: u64, expiration: Option<u64>) -> Value {
        let expires_at = expiration.map(|e|created_at + e);
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
    current_memory: usize,
    map: HashMap<Vec<u8>, Value>,
    map_by_time: BTreeMap<u64, HashSet<Vec<u8>>>,
    map_by_expiration: BTreeMap<u64, HashSet<Vec<u8>>>,
}

enum GetResult {
    NotFound,
    Found,
    Expired
}

fn calculate_record_size(key_size: usize, value_size: usize) -> usize {
    3 * key_size + value_size + 16
}

impl CommonMaps {
    fn flush(&mut self) {
        self.current_memory = 0;
        self.map.clear();
        self.map_by_expiration.clear();
        self.map_by_time.clear();
    }

    fn removekey(&mut self, key: &Vec<u8>) -> isize {
        if let Some(value) = self.map.remove(key) {
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
            self.current_memory -= calculate_record_size(key.len(), value.value.len());
            return 1;
        }
        0
    }

    fn removekeys(&mut self, keys: Vec<&Vec<u8>>) -> isize {
        keys.into_iter().map(|k|self.removekey(k)).sum()
    }

    fn get(&self, key: &Vec<u8>, result: &mut Vec<u8>, start_time: SystemTime) -> GetResult {
        return match self.map.get(key) {
            Some(value) => {
                if value.is_expired(start_time) {
                    Expired
                } else {
                    resp_encode_binary_string(value.get_value(), result);
                    Found
                }
            },
            None => NotFound
        }
    }

    fn cleanup(&mut self) {
        //todo
    }

    fn set(&mut self, key: &Vec<u8>, value: &Vec<u8>, expiry: Option<u64>, start_time: SystemTime) {
        self.cleanup();
        let created_at = SystemTime::now().duration_since(start_time).unwrap().as_millis() as u64;
        let v = Value::new(value.clone(), created_at, expiry);
        let created_at = v.created_at;
        let expires_at = v.expires_at;
        self.removekey(key);
        self.map.insert(key.clone(), v);
        if let Some(ex) = expires_at {
            match self.map_by_expiration.get_mut(&ex) {
                Some(v) => { let _ = v.insert(key.clone()); },
                None => {
                    let mut s = HashSet::new();
                    s.insert(key.clone());
                    self.map_by_expiration.insert(ex, s);
                }
            };
        }
        match self.map_by_time.get_mut(&created_at) {
            Some(v) => { let _ = v.insert(key.clone()); },
            None => {
                let mut s = HashSet::new();
                s.insert(key.clone());
                self.map_by_time.insert(created_at, s);
            }
        };
        self.current_memory += calculate_record_size(key.len(), value.len());
    }

    fn size(&self) -> usize {
        self.map.len()
    }
}

pub struct CommonData {
    start_time: SystemTime,
    pub verbose: bool,
    max_memory: usize,
    pub configuration: HashMap<Vec<u8>, Vec<u8>>,
    maps: RwLock<CommonMaps>,
    pub exit_flag: AtomicBool,
    pub threads: RwLock<HashMap<usize, Arc<Mutex<TcpStream>>>>
}

impl CommonData {
    pub fn flush(&self) {
        self.maps.write().unwrap().flush();
    }

    pub fn removekeys(&self, keys: Vec<&Vec<u8>>) -> isize {
        self.maps.write().unwrap().removekeys(keys)
    }

    pub fn set(&self, key: &Vec<u8>, value: &Vec<u8>, expiry: Option<u64>) {
        self.maps.write().unwrap().set(key, value, expiry, self.start_time);
    }

    pub fn get(&self, key: &Vec<u8>, result: &mut Vec<u8>) -> bool {
        let lock = self.maps.read().unwrap();
        match lock.get(key, result, self.start_time) {
            Found => true,
            NotFound => false,
            Expired => {
                drop(lock);
                self.maps.write().unwrap().removekey(key);
                false
            }
        }
    }

    pub fn size(&self) -> usize {
        self.maps.read().unwrap().size()
    }
}

fn build_configuration() -> HashMap<Vec<u8>, Vec<u8>> {
    HashMap::from([
        ("save".to_string().into_bytes(), "".to_string().into_bytes()),
        ("appendonly".to_string().into_bytes(), "no".to_string().into_bytes())])
}

pub fn build_common_data(verbose: bool, max_memory: usize) -> CommonData {
    CommonData{ start_time: SystemTime::now(), verbose, max_memory, configuration: build_configuration(),
        maps: RwLock::new(CommonMaps{current_memory: 0, map: HashMap::new(), map_by_time: BTreeMap::new(),
            map_by_expiration: BTreeMap::new()}),
        exit_flag:  AtomicBool::new(false), threads: RwLock::new(HashMap::new()) }
}
