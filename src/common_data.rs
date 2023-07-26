use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::Entry;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::AtomicBool;
use std::time::SystemTime;
use crate::common_data_map::CommonDataMap;
use crate::generic_maps::{GenericMaps, RecordSizeCalculator};
use crate::hash_builders::HashBuilder;

struct DBCountCalculator{}

impl RecordSizeCalculator<String, CommonDataMap> for DBCountCalculator {
    fn calculate_record_size(&self, key: &String, value: &CommonDataMap) -> usize {
        1
    }
}

pub struct CommonData {
    pub start_time: SystemTime,
    pub hash_builder: Arc<dyn HashBuilder + Send + Sync>,
    pub verbose: bool,
    pub configuration: HashMap<Vec<u8>, Vec<u8>>,
    maps_map: RwLock<GenericMaps<String, CommonDataMap, DBCountCalculator>>,
    pub exit_flag: AtomicBool,
    pub threads: RwLock<HashMap<usize, Arc<Mutex<TcpStream>>>>,
    max_memory: usize,
    vector_size: usize,
    cleanup_using_lru: bool
}

impl CommonData {
    pub fn select(&self, db_name: String) -> Arc<CommonDataMap> {
        self.maps_map.write().unwrap().entry(db_name).or_insert(
            CommonDataMap::new(self.vector_size, self.max_memory, self.cleanup_using_lru, self.start_time)).clone()
    }

    pub fn createdb(&self, db_name: String) -> Result<Arc<CommonDataMap>, Error> {
        match self.maps_map.write().unwrap().entry(db_name.clone()) {
            Entry::Occupied(_) => Err(Error::new(ErrorKind::AlreadyExists, "-database already exists\r\n")),
            Entry::Vacant(e) => {
                self.cleanup();
                let maps = CommonDataMap::new(self.vector_size,
                                              self.max_memory, self.cleanup_using_lru, self.start_time);
                e.insert(maps.clone());
                self.insert_to_map_by_time(db_name);
                Ok(maps)
            }
        }
    }

    pub fn loaddb(&self, db_name: String) -> Result<Arc<CommonDataMap>, Error> {
        let db_name_clone = db_name.clone();
        match self.maps_map.write().unwrap().entry(db_name.clone()) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
                self.cleanup();
                let maps = CommonDataMap::load(db_name_clone, self.vector_size, self.max_memory, self.cleanup_using_lru)?;
                e.insert(maps.clone());
                self.insert_to_map_by_time(db_name);
                Ok(maps)
            }
        }
    }

    pub fn flush_all(&self) {
        self.maps_map.read().unwrap().iter()
            .for_each(|(db_name, db)|{db.flush();self.move_to_top(db_name, &db.last_access_time);})
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
