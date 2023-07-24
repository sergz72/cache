use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::SystemTime;
use crate::common_data::CommonData;
use crate::common_data_map::CommonDataMap;
use crate::common_maps;
use crate::errors::build_wrong_data_type_error;
use crate::hash_builders::HashBuilder;
use crate::value::ValueHolder;

pub struct WorkerData {
    pub current_db_name: String,
    current_db: Arc<CommonDataMap>,
    start_time: SystemTime,
    hash_builder: Arc<dyn HashBuilder + Send + Sync>,
}

fn parse_db_name(db_name: Vec<u8>) -> Result<String, Error> {
    String::from_utf8(db_name).map_err(|_|Error::new(ErrorKind::InvalidData, "-invalid database name\r\n"))
}

impl WorkerData {
    pub fn new(db_name: Vec<u8>, common_data: Arc<CommonData>) -> Result<WorkerData, Error> {
        let current_db_name = parse_db_name(db_name)?;
        let db_name_clone = current_db_name.clone();
        Ok(WorkerData{ current_db_name, current_db: common_data.select(db_name_clone),
            start_time: common_data.start_time, hash_builder: common_data.hash_builder.clone() })
    }

    pub fn select(&mut self, db_name: Vec<u8>, common_data: Arc<CommonData>) -> Result<(), Error> {
        let current_db_name = parse_db_name(db_name)?;
        self.current_db_name = current_db_name.clone();
        self.current_db = common_data.select(current_db_name);
        Ok(())
    }

    pub fn createdb(&mut self, db_name: Vec<u8>, common_data: Arc<CommonData>) -> Result<(), Error> {
        let current_db_name = parse_db_name(db_name)?;
        let current_db = common_data.createdb(current_db_name.clone())?;
        self.current_db_name = current_db_name;
        self.current_db = current_db;
        Ok(())
    }

    pub fn loaddb(&mut self, db_name: Vec<u8>, common_data: Arc<CommonData>) -> Result<(), Error> {
        let current_db_name = parse_db_name(db_name)?;
        let current_db = common_data.loaddb(current_db_name.clone())?;
        self.current_db_name = current_db_name;
        self.current_db = current_db;
        Ok(())
    }

    pub fn hgetall(&self, key: &Vec<u8>, result: &mut Vec<u8>) -> Result<bool, Error> {
        let idx = self.hash_builder.build_hash(key);
        let lock = self.current_db.get_read_lock(idx);
        match lock.hgetall(key, result, self.start_time) {
            common_maps::GetResult::Found => Ok(true),
            common_maps::GetResult::NotFound => Ok(false),
            common_maps::GetResult::WrongValue => Err(build_wrong_data_type_error()),
            common_maps::GetResult::Expired => {
                drop(lock);
                self.current_db.get_write_lock(idx).removekey(key);
                Ok(false)
            }
        }
    }

    pub fn size(&self) -> usize {
        self.current_db.size()
    }

    pub fn get(&self, key: &Vec<u8>, result: &mut Vec<u8>) -> Result<bool, Error> {
        let idx = self.hash_builder.build_hash(key);
        let lock = self.current_db.get_read_lock(idx);
        match lock.get(key, result, self.start_time) {
            common_maps::GetResult::Found => Ok(true),
            common_maps::GetResult::NotFound => Ok(false),
            common_maps::GetResult::WrongValue => Err(build_wrong_data_type_error()),
            common_maps::GetResult::Expired => {
                drop(lock);
                self.current_db.get_write_lock(idx).removekey(key);
                Ok(false)
            }
        }
    }

    pub fn hdel(&self, key: &Vec<u8>, keys: HashSet<&Vec<u8>>) -> Result<isize, Error> {
        let idx = self.hash_builder.build_hash(key);
        self.current_db.get_write_lock(idx).hdel(key, keys)
    }

    pub fn set(&self, key: &Vec<u8>, value: ValueHolder, expiry: Option<u64>) -> Result<(), Error> {
        let idx = self.hash_builder.build_hash(key);
        self.current_db.get_write_lock(idx).set(key, value, expiry, self.start_time)
    }

    pub fn hset(&self, key: &Vec<u8>, values: HashMap<Vec<u8>, Vec<u8>>) -> Result<isize, Error> {
        let idx = self.hash_builder.build_hash(key);
        self.current_db.get_write_lock(idx).hset(key, values, self.start_time)
    }

    pub fn hget(&self, key: &Vec<u8>, map_key: &Vec<u8>, result: &mut Vec<u8>) -> Result<bool, Error> {
        let idx = self.hash_builder.build_hash(key);
        let lock = self.current_db.get_read_lock(idx);
        match lock.hget(key, map_key, result, self.start_time) {
            common_maps::GetResult::Found => Ok(true),
            common_maps::GetResult::NotFound => Ok(false),
            common_maps::GetResult::WrongValue => Err(build_wrong_data_type_error()),
            common_maps::GetResult::Expired => {
                drop(lock);
                self.current_db.get_write_lock(idx).removekey(key);
                Ok(false)
            }
        }
    }

    pub fn flush(&self) {
        self.current_db.flush();
    }

    pub fn removekeys(&self, keys: HashSet<&Vec<u8>>) -> isize {
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
            .map(|(idx, keys)|self.current_db.get_write_lock(idx).removekeys(keys))
            .sum()
    }

    pub fn get_last_access_time(&self) -> &AtomicU64 {
        &self.current_db.last_access_time
    }
}
