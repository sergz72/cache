use std::collections::{HashMap, HashSet};
use std::io::Error;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use crate::common_maps::GetResult::{Expired, Found, NotFound, WrongValue};
use crate::errors::build_out_of_memory_error;
use crate::generic_maps::{GenericMaps, RecordSizeCalculator};
use crate::resp_encoder::{resp_encode_binary_string, resp_encode_int, resp_encode_map};
use crate::value::{Value, ValueHolder};

struct ValueHolderSizeCalculator{}
impl RecordSizeCalculator<Vec<u8>, ValueHolder> for ValueHolderSizeCalculator {
    fn calculate_record_size(&self, key: &Vec<u8>, value: &ValueHolder) -> usize {
        3 * key.len() + value.size() + 16
    }
}

pub struct CommonMaps {
    cleanup_using_lru: bool,
    maps: GenericMaps<Vec<u8>, ValueHolder, ValueHolderSizeCalculator>,
}

pub fn build_map(max_memory: usize, cleanup_using_lru: bool) -> CommonMaps {
    CommonMaps {
        cleanup_using_lru,
        maps: GenericMaps::new(cleanup_using_lru, ValueHolderSizeCalculator{}, max_memory)
    }
}

#[derive(PartialEq, Debug)]
pub enum GetResult {
    NotFound,
    Found,
    Expired,
    WrongValue,
}

fn calculate_record_size(key_size: usize, value_size: usize) -> usize {
    3 * key_size + value_size + 16
}

impl CommonMaps {
    pub fn flush(&mut self) -> usize {
        let counter = self.maps.len();
        self.maps.clear();
        counter
    }

    pub fn get(&self, key: &Vec<u8>, result: &mut Vec<u8>, start_time: SystemTime) -> GetResult {
        return match self.maps.get(key) {
            Some(value) => {
                if value.is_expired(start_time) {
                    Expired
                } else if let Some(v) = value.get_value() {
                    resp_encode_binary_string(v, result);
                    if self.cleanup_using_lru {
                        self.aux_maps.write().unwrap().update_map_by_time(key, value, start_time);
                    }
                    Found
                } else if let Some(v) = value.get_ivalue() {
                    resp_encode_int(v, result);
                    if self.cleanup_using_lru {
                        self.aux_maps.write().unwrap().update_map_by_time(key, value, start_time);
                    }
                    Found
                } else {
                    WrongValue
                }
            }
            None => NotFound
        };
    }

    pub fn hget(&self, key: &Vec<u8>, map_key: &Vec<u8>, result: &mut Vec<u8>, start_time: SystemTime) -> GetResult {
        return match self.map.get(key) {
            Some(value) => {
                if value.is_expired(start_time) {
                    Expired
                } else if let Some(v) = value.get_hvalue() {
                    if let Some(vv) = v.get(map_key) {
                        resp_encode_binary_string(vv, result);
                        if self.cleanup_using_lru {
                            self.aux_maps.write().unwrap().update_map_by_time(key, value, start_time);
                        }
                        Found
                    } else {
                        NotFound
                    }
                } else {
                    WrongValue
                }
            }
            None => NotFound
        };
    }

    pub fn hgetall(&self, key: &Vec<u8>, result: &mut Vec<u8>, start_time: SystemTime) -> GetResult {
        return match self.map.get(key) {
            Some(value) => {
                if value.is_expired(start_time) {
                    Expired
                } else if let Some(v) = value.get_hvalue() {
                    resp_encode_map(v, result);
                    if self.cleanup_using_lru {
                        self.aux_maps.write().unwrap().update_map_by_time(key, value, start_time);
                    }
                    Found
                } else {
                    WrongValue
                }
            }
            None => NotFound
        };
    }

    pub fn set(&mut self, key: &Vec<u8>, value: ValueHolder, expiry: Option<u64>, start_time: SystemTime) -> Result<(), Error> {
        let created_at = SystemTime::now().duration_since(start_time).unwrap().as_millis() as u64;
        let v = Value::new(value, created_at, expiry);
        let size = calculate_record_size(key.len(), v.size());
        self.current_memory += size;
        if !self.cleanup(start_time) {
            self.current_memory -= size;
            return Err(build_out_of_memory_error());
        }
        let created_at = v.last_access_time.load(Ordering::Relaxed);
        let expires_at = v.expires_at;
        let v_size = v.size();
        if let Some(old) = self.map.insert(key.clone(), v) {
            self.current_memory = self.current_memory - size + v_size - old.size();
            self.aux_maps.write().unwrap().remove(key, old.expires_at,
                                                  old.last_access_time.load(Ordering::Relaxed),
                                                  self.cleanup_using_lru);
        }
        self.aux_maps.write().unwrap().add(key, created_at, expires_at, self.cleanup_using_lru);
        Ok(())
    }

    pub fn hset(&mut self, key: &Vec<u8>, values: HashMap<Vec<u8>, Vec<u8>>, start_time: SystemTime) -> Result<isize, Error> {
        let created_at = SystemTime::now().duration_since(start_time).unwrap().as_millis() as u64;
        let values_len = values.len() as isize;
        let v = Value::new_hash(values, created_at);
        let size = calculate_record_size(key.len(), v.size());
        self.current_memory += size;
        if !self.cleanup(start_time) {
            self.current_memory -= size;
            return Err(build_out_of_memory_error());
        }
        let created_at = v.last_access_time.load(Ordering::Relaxed);
        let expires_at = v.expires_at;
        match self.map.get_mut(key) {
            Some(existing) => {
                let size_before = existing.size();
                let inserted = existing.merge(v.get_hvalue().unwrap())?;
                self.current_memory -= size_before;
                self.current_memory += existing.size();
                Ok(inserted)
            }
            None => {
                self.map.insert(key.clone(), v);
                self.aux_maps.write().unwrap().add(key, created_at, expires_at, self.cleanup_using_lru);
                Ok(values_len)
            }
        }
    }

    pub fn hdel(&mut self, key: &Vec<u8>, values: HashSet<&Vec<u8>>) -> Result<isize, Error> {
        match self.map.get_mut(key) {
            Some(existing) => {
                let size_before = existing.size();
                let (deleted, l) = existing.delete(values)?;
                self.current_memory -= size_before;
                self.current_memory += existing.size();
                if l == 0 {
                    self.removekey(key);
                }
                Ok(deleted)
            }
            None => Ok(0),
        }
    }

    pub fn size(&self) -> usize {
        self.maps.len()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Error;
    use std::thread;
    use std::time::{Duration, SystemTime};
    use rand::distributions::{Alphanumeric, DistString};
    use rand::Rng;
    use crate::common_maps::build_map;
    use crate::common_maps::GetResult::{Expired, Found, NotFound};
    use crate::value::ValueHolder::StringValue;

    #[test]
    fn test_set_delete() -> Result<(), Error> {
        let mut rng = rand::thread_rng();
        let mut keys = Vec::new();
        let mut maps = build_map(100000000, true);
        let start_time = SystemTime::now();
        for _i in 0..1000 {
            let key_length = (rng.gen::<usize>() % 100) + 10;
            let value_length = (rng.gen::<usize>() % 200) + 10;
            let key = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
            let value = StringValue(Alphanumeric.sample_string(&mut rng, value_length).into_bytes());
            maps.set(&key, value, None, start_time)?;
            keys.push(key);
        }

        for key in &keys {
            let value_length = (rng.gen::<usize>() % 200) + 10;
            let value = StringValue(Alphanumeric.sample_string(&mut rng, value_length).into_bytes());
            maps.set(key, value, None, start_time)?;
        }

        for key in keys {
            maps.removekey(&key);
        }

        assert_eq!(maps.map.len(), 0);
        assert_eq!(maps.aux_maps.read().unwrap().map_by_time.len(), 0);
        assert_eq!(maps.aux_maps.read().unwrap().map_by_expiration.len(), 0);
        assert_eq!(maps.current_memory, 0);

        Ok(())
    }

    #[test]
    fn test_cleanup() -> Result<(), Error> {
        let mut rng = rand::thread_rng();
        let mut maps = build_map(100000, true);
        let start_time = SystemTime::now();
        for _i in 0..1000 {
            let key_length = (rng.gen::<usize>() % 100) + 10;
            let value_length = (rng.gen::<usize>() % 200) + 10;
            let key = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
            let value = StringValue(Alphanumeric.sample_string(&mut rng, value_length).into_bytes());
            maps.set(&key, value, None, start_time)?;
        }

        assert!(maps.current_memory - 1000 < maps.max_memory);

        Ok(())
    }

    #[test]
    fn test_cleanup2() -> Result<(), Error> {
        let mut rng = rand::thread_rng();
        let mut maps = build_map(100000, true);
        let start_time = SystemTime::now();
        for _i in 0..1000 {
            let key_length = (rng.gen::<usize>() % 100) + 10;
            let value_length = (rng.gen::<usize>() % 200) + 10;
            let key = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
            let value = StringValue(Alphanumeric.sample_string(&mut rng, value_length).into_bytes());
            maps.set(&key, value, Some(100), start_time)?;
        }

        thread::sleep(Duration::from_millis(200));

        let key_length = (rng.gen::<usize>() % 100) + 10;
        let key = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
        let value = StringValue(Alphanumeric.sample_string(&mut rng, 20000).into_bytes());
        maps.set(&key, value, None, start_time)?;

        assert_eq!(maps.size(), 1);

        Ok(())
    }

    #[test]
    fn test_set_get() -> Result<(), Error> {
        let mut rng = rand::thread_rng();
        let mut maps = build_map(1000000, true);
        let start_time = SystemTime::now();
        let key_length = (rng.gen::<usize>() % 100) + 10;
        let value_length = (rng.gen::<usize>() % 200) + 10;
        let key = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
        let key2 = Alphanumeric.sample_string(&mut rng, key_length).into_bytes();
        let value = StringValue(Alphanumeric.sample_string(&mut rng, value_length).into_bytes());
        maps.set(&key, value, Some(100), start_time)?;

        let mut result = Vec::new();

        assert_eq!(maps.get(&key, &mut result, start_time), Found);
        assert_eq!(maps.get(&key2, &mut result, start_time), NotFound);

        thread::sleep(Duration::from_millis(200));

        assert_eq!(maps.get(&key, &mut result, start_time), Expired);

        Ok(())
    }
}