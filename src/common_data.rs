use std::collections::{BTreeMap, HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::AtomicBool;
use std::time::SystemTime;
use crate::common_maps;
use crate::common_maps::{build_maps, CommonMaps, load_maps};
use crate::hash_builders::HashBuilder;
use crate::server::WorkerData;

struct CommonDataMap {
    map: Vec<RwLock<CommonMaps>>,
    last_access_time: u64
}

pub struct CommonData {
    start_time: SystemTime,
    hash_builder: Box<dyn HashBuilder + Send + Sync>,
    pub verbose: bool,
    pub configuration: HashMap<Vec<u8>, Vec<u8>>,
    maps_map: RwLock<HashMap<String, Arc<CommonDataMap>>>,
    pub exit_flag: AtomicBool,
    pub threads: RwLock<HashMap<usize, Arc<Mutex<TcpStream>>>>,
    max_memory: usize,
    vector_size: usize,
    cleanup_using_lru: bool,
    max_open_databases: usize,
    map_by_time: RwLock<BTreeMap<u64, HashSet<String>>>,
}

pub fn build_wrong_data_type_error() -> Error {
    Error::new(ErrorKind::InvalidData, "-Operation against a key holding the wrong kind of value\r\n")
}

impl CommonData {
    pub fn select(&self, db_name: String) -> Arc<Vec<RwLock<CommonMaps>>> {
        self.maps_map.write().unwrap().entry(db_name).or_insert(
            build_maps(self.vector_size, self.max_memory, self.cleanup_using_lru)).clone()
    }

    fn insert_to_map_by_time(&self, db_name: String) {
        let now = SystemTime::now().duration_since(self.start_time).unwrap().as_millis() as u64;
        match self.map_by_time.write().unwrap().entry(now) {
            Entry::Occupied(e) => e.get().insert(db_name),
            Entry::Vacant(e) => e.insert(HashSet::from([db_name]))
        };
    }

    fn move_to_top(&self, db_name: String) {
        let now = SystemTime::now().duration_since(self.start_time).unwrap().as_millis() as u64;
    }

    pub fn createdb(&self, db_name: String) -> Result<Arc<Vec<RwLock<CommonMaps>>>, Error> {
        match self.maps_map.write().unwrap().entry(db_name.clone()) {
            Entry::Occupied(_) => Err(Error::new(ErrorKind::AlreadyExists, "-database already exists\r\n")),
            Entry::Vacant(e) => {
                let maps = build_maps(self.vector_size, self.max_memory, self.cleanup_using_lru);
                e.insert(maps.clone());
                self.insert_to_map_by_time(db_name);
                Ok(maps)
            }
        }
    }

    pub fn loaddb(&self, db_name: String) -> Result<Arc<Vec<RwLock<CommonMaps>>>, Error> {
        let db_name_clone = db_name.clone();
        match self.maps_map.write().unwrap().entry(db_name.clone()) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
                let maps = load_maps(db_name_clone, self.vector_size, self.max_memory, self.cleanup_using_lru)?;
                e.insert(maps.clone());
                self.insert_to_map_by_time(db_name);
                Ok(maps)
            }
        }
    }

    pub fn flush_all(&self) {
        self.maps_map.read().unwrap().values()
            .for_each(|db|db.iter().for_each(|m|m.write().unwrap().flush()))
    }

    pub fn flush(worker_data: &WorkerData) {
        worker_data.current_db.iter().for_each(|m|m.write().unwrap().flush());
    }

    pub fn removekeys(&self, keys: HashSet<&Vec<u8>>, worker_data: &WorkerData) -> isize {
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
            .map(|(idx, keys)|worker_data.current_db[idx].write().unwrap().removekeys(keys))
            .sum()
    }

    pub fn hdel(&self, key: &Vec<u8>, keys: HashSet<&Vec<u8>>, worker_data: &WorkerData) -> Result<isize, Error> {
        let idx = self.hash_builder.build_hash(key);
        worker_data.current_db[idx].write().unwrap().hdel(key, keys)
    }

    pub fn set(&self, key: &Vec<u8>, value: &Vec<u8>, expiry: Option<u64>, worker_data: &WorkerData) -> Result<(), Error> {
        let idx = self.hash_builder.build_hash(key);
        worker_data.current_db[idx].write().unwrap().set(key, value, expiry, self.start_time)
    }

    pub fn hset(&self, key: &Vec<u8>, values: HashMap<Vec<u8>, Vec<u8>>, worker_data: &WorkerData) -> Result<isize, Error> {
        let idx = self.hash_builder.build_hash(key);
        worker_data.current_db[idx].write().unwrap().hset(key, values, self.start_time)
    }

    pub fn get(&self, key: &Vec<u8>, result: &mut Vec<u8>, worker_data: &WorkerData) -> Result<bool, Error> {
        let idx = self.hash_builder.build_hash(key);
        let lock = worker_data.current_db[idx].read().unwrap();
        match lock.get(key, result, self.start_time) {
            common_maps::GetResult::Found => Ok(true),
            common_maps::GetResult::NotFound => Ok(false),
            common_maps::GetResult::WrongValue => Err(build_wrong_data_type_error()),
            common_maps::GetResult::Expired => {
                drop(lock);
                worker_data.current_db[idx].write().unwrap().removekey(key);
                Ok(false)
            }
        }
    }

    pub fn hget(&self, key: &Vec<u8>, map_key: &Vec<u8>, result: &mut Vec<u8>, worker_data: &WorkerData) -> Result<bool, Error> {
        let idx = self.hash_builder.build_hash(key);
        let lock = worker_data.current_db[idx].read().unwrap();
        match lock.hget(key, map_key, result, self.start_time) {
            common_maps::GetResult::Found => Ok(true),
            common_maps::GetResult::NotFound => Ok(false),
            common_maps::GetResult::WrongValue => Err(build_wrong_data_type_error()),
            common_maps::GetResult::Expired => {
                drop(lock);
                worker_data.current_db[idx].write().unwrap().removekey(key);
                Ok(false)
            }
        }
    }

    pub fn hgetall(&self, key: &Vec<u8>, result: &mut Vec<u8>, worker_data: &WorkerData) -> Result<bool, Error> {
        let idx = self.hash_builder.build_hash(key);
        let lock = worker_data.current_db[idx].read().unwrap();
        match lock.hgetall(key, result, self.start_time) {
            common_maps::GetResult::Found => Ok(true),
            common_maps::GetResult::NotFound => Ok(false),
            common_maps::GetResult::WrongValue => Err(build_wrong_data_type_error()),
            common_maps::GetResult::Expired => {
                drop(lock);
                worker_data.current_db[idx].write().unwrap().removekey(key);
                Ok(false)
            }
        }
    }

    pub fn size(worker_data: &WorkerData) -> usize {
        worker_data.current_db.iter().map(|m|m.read().unwrap().size()).sum()
    }
}

fn build_configuration() -> HashMap<Vec<u8>, Vec<u8>> {
    HashMap::from([
        ("save".to_string().into_bytes(), "".to_string().into_bytes()),
        ("appendonly".to_string().into_bytes(), "no".to_string().into_bytes())])
}

pub fn build_common_data(verbose: bool, max_memory: usize, vector_size: usize, cleanup_using_lru: bool,
                         max_open_databases: usize, hash_builder: Box<dyn HashBuilder + Send + Sync>) -> CommonData {
    CommonData {
        start_time: SystemTime::now(),
        hash_builder,
        verbose,
        configuration: build_configuration(),
        maps_map: RwLock::new(HashMap::new()),
        exit_flag: AtomicBool::new(false),
        threads: RwLock::new(HashMap::new()),
        max_memory,
        vector_size,
        cleanup_using_lru,
        max_open_databases,
        map_by_time: BTreeMap::new()
    }
}
