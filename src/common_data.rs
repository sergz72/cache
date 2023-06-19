use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::AtomicBool;
use std::time::SystemTime;
use crate::common_maps;
use crate::common_maps::{build_maps, CommonMaps};
use crate::hash_builders::HashBuilder;

pub struct CommonData {
    start_time: SystemTime,
    hash_builder: Box<dyn HashBuilder + Send + Sync>,
    pub verbose: bool,
    pub configuration: HashMap<Vec<u8>, Vec<u8>>,
    maps: Vec<RwLock<CommonMaps>>,
    pub exit_flag: AtomicBool,
    pub threads: RwLock<HashMap<usize, Arc<Mutex<TcpStream>>>>,
}

impl CommonData {
    pub fn flush(&self) {
        self.maps.iter().for_each(|m|m.write().unwrap().flush());
    }

    pub fn removekeys(&self, keys: Vec<&Vec<u8>>) -> isize {
        let mut key_map: HashMap<usize, Vec<&Vec<u8>>> = HashMap::new();
        for key in keys {
            let hash = self.hash_builder.build_hash(key);
            match key_map.get_mut(&hash) {
                Some(v) => v.push(key),
                None => {
                    let mut s = Vec::new();
                    s.push(key);
                    key_map.insert(hash, s);
                }
            }
        }
        key_map.into_iter()
            .map(|(idx, keys)|self.maps[idx].write().unwrap().removekeys(keys))
            .sum()
    }

    pub fn set(&self, key: &Vec<u8>, value: &Vec<u8>, expiry: Option<u64>) {
        let idx = self.hash_builder.build_hash(key);
        self.maps[idx].write().unwrap().set(key, value, expiry, self.start_time);
    }

    pub fn get(&self, key: &Vec<u8>, result: &mut Vec<u8>) -> bool {
        let idx = self.hash_builder.build_hash(key);
        let lock = self.maps[idx].read().unwrap();
        match lock.get(key, result, self.start_time) {
            common_maps::GetResult::Found => true,
            common_maps::GetResult::NotFound => false,
            common_maps::GetResult::Expired => {
                drop(lock);
                self.maps[idx].write().unwrap().removekey(key);
                false
            }
        }
    }

    pub fn size(&self) -> usize {
        self.maps.iter().map(|m|m.read().unwrap().size()).sum()
    }
}

fn build_configuration() -> HashMap<Vec<u8>, Vec<u8>> {
    HashMap::from([
        ("save".to_string().into_bytes(), "".to_string().into_bytes()),
        ("appendonly".to_string().into_bytes(), "no".to_string().into_bytes())])
}

pub fn build_common_data(verbose: bool, max_memory: usize, vector_size: usize,
                         hash_builder: Box<dyn HashBuilder + Send + Sync>) -> CommonData {
    CommonData {
        start_time: SystemTime::now(),
        hash_builder,
        verbose,
        configuration: build_configuration(),
        maps: build_maps(vector_size, max_memory),
        exit_flag: AtomicBool::new(false),
        threads: RwLock::new(HashMap::new()),
    }
}
