use std::io::Error;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;
use crate::common_maps::{build_map, CommonMaps};
use crate::generic_maps::SizedValue;

pub struct CommonDataMap {
    map: Vec<RwLock<CommonMaps>>,
}

impl SizedValue for CommonDataMap {
    fn size(&self) -> usize {
        1
    }

    fn len(&self) -> usize {
        1
    }
}

impl CommonDataMap {
    pub fn new(vector_size: usize, all_memory: usize, cleanup_using_lru: bool, start_time: SystemTime) -> Arc<CommonDataMap> {
        let max_memory = all_memory / vector_size;
        let now = SystemTime::now().duration_since(start_time).unwrap().as_millis() as u64;
        Arc::new(CommonDataMap {
            map: (0..vector_size)
                .map(|_i| RwLock::new(build_map(max_memory, cleanup_using_lru)))
                .collect(),
        })
    }

    pub fn load(db_name: String, vector_size: usize, all_memory: usize, cleanup_using_lru: bool) -> Result<Arc<CommonDataMap>, Error> {
        /*let max_memory = all_memory / vector_size;
        Arc::new((0..vector_size)
            .map(|_i| RwLock::new(build_map(max_memory, cleanup_using_lru)))
            .collect())*/
        todo!()
    }

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

    pub fn dbsize(&self) -> usize {
        self.map.iter().map(|m|m.read().unwrap().size()).sum()
    }
}
