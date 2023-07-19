use std::collections::{BTreeMap, HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;
use crate::common_maps::{build_maps, CommonMaps, load_maps};
use crate::hash_builders::HashBuilder;

pub struct CommonDataMap {
    map: Vec<RwLock<CommonMaps>>,
    last_access_time: AtomicU64,
    is_updated: AtomicBool
}

pub struct CommonData {
    pub start_time: SystemTime,
    pub hash_builder: Arc<dyn HashBuilder + Send + Sync>,
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

impl CommonDataMap {
    pub fn flush(&self) {
        let deleted: usize = self.map.iter().map(|m|m.write().unwrap().flush()).sum();
        if deleted != 0 {
            self.is_updated.store(true, Ordering::Relaxed);
        }
    }

    pub fn get_read_lock(&self, idx: usize) -> RwLockReadGuard<CommonMaps> {
        self.map[idx].read().unwrap()
    }

    pub fn get_write_lock(&self, idx: usize) -> RwLockWriteGuard<CommonMaps> {
        self.map[idx].write().unwrap()
    }

    pub fn size(&self) -> usize {
        self.map.iter().map(|m|m.read().unwrap().size()).sum()
    }
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
            .for_each(|db|db.flush())
    }
}

fn build_configuration() -> HashMap<Vec<u8>, Vec<u8>> {
    HashMap::from([
        ("save".to_string().into_bytes(), "".to_string().into_bytes()),
        ("appendonly".to_string().into_bytes(), "no".to_string().into_bytes())])
}

pub fn build_common_data(verbose: bool, max_memory: usize, vector_size: usize, cleanup_using_lru: bool,
                         max_open_databases: usize, hash_builder: Arc<dyn HashBuilder + Send + Sync>) -> CommonData {
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
        map_by_time: RwLock::new(BTreeMap::new())
    }
}
