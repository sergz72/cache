use std::collections::{HashMap, HashSet};
use std::io::Error;
use std::time::SystemTime;
use crate::errors::build_wrong_data_type_error;
use crate::value::ValueHolder::{HashMapValue, HashSetValue, IntValue, StringValue};

enum ValueHolder {
    IntValue(isize),
    StringValue(Vec<u8>),
    HashMapValue(HashMap<Vec<u8>, Vec<u8>>),
    HashSetValue(HashSet<Vec<u8>>)
}

fn calculate_map_size(map: &HashMap<Vec<u8>, Vec<u8>>) -> usize {
    map.iter().fold(0, |s, (k, v)| s + k.len() + v.len())
}

fn calculate_set_size(set: &HashSet<Vec<u8>>) -> usize {
    set.iter().map(|i|i.len()).sum()
}

impl ValueHolder {
    fn size(&self) -> usize {
        match self {
            IntValue(_) => 8,
            StringValue(v) => v.len(),
            HashMapValue(m) => calculate_map_size(m),
            HashSetValue(s) => calculate_set_size(s)
        }
    }
}

pub struct Value {
    value: ValueHolder,
    pub last_access_time: u64,
    pub expires_at: Option<u64>,
}

impl Value {
    pub fn new(value: Vec<u8>, created_at: u64, expiration: Option<u64>) -> Value {
        let expires_at = expiration.map(|e| created_at + e);
        Value {
            value: StringValue(value),
            last_access_time: created_at,
            expires_at,
        }
    }

    pub fn new_int(value: isize, created_at: u64, expiration: Option<u64>) -> Value {
        let expires_at = expiration.map(|e| created_at + e);
        Value {
            value: IntValue(value),
            last_access_time: created_at,
            expires_at,
        }
    }

    pub fn new_hash(map: HashMap<Vec<u8>, Vec<u8>>, created_at: u64) -> Value {
        Value {
            value: HashMapValue(map),
            last_access_time: created_at,
            expires_at: None,
        }
    }

    pub fn new_set(set: HashSet<Vec<u8>>, created_at: u64) -> Value {
        Value {
            value: HashSetValue(set),
            last_access_time: created_at,
            expires_at: None,
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

    pub fn get_value(&self) -> Option<&Vec<u8>> {
        match &self.value {
            StringValue(v) => Some(v),
            _ => None
        }
    }

    pub fn get_ivalue(&self) -> Option<isize> {
        match &self.value {
            IntValue(v) => Some(*v),
            _ => None
        }
    }

    pub fn get_hvalue(&self) -> Option<&HashMap<Vec<u8>, Vec<u8>>> {
        match &self.value {
            HashMapValue(v) => Some(v),
            _ => None
        }
    }

    pub fn get_mut_hvalue(&mut self) -> Option<&mut HashMap<Vec<u8>, Vec<u8>>> {
        match &mut self.value {
            HashMapValue(v) => Some(v),
            _ => None
        }
    }

    pub fn get_svalue(&self) -> Option<&HashSet<Vec<u8>>> {
        match &self.value {
            HashSetValue(v) => Some(v),
            _ => None
        }
    }

    pub fn size(&self) -> usize {
        self.value.size()
    }

    pub fn merge(&mut self, source: &HashMap<Vec<u8>, Vec<u8>>) -> Result<isize, Error> {
        let hv = self.get_mut_hvalue().ok_or(build_wrong_data_type_error())?;
        let mut inserted = 0;
        for (k, v) in source {
            if None == hv.insert(k.clone(), v.clone()) {
                inserted += 1;
            }
        }
        Ok(inserted)
    }

    pub fn delete(&mut self, source: HashSet<&Vec<u8>>) -> Result<(isize, usize), Error> {
        let hv = self.get_mut_hvalue().ok_or(build_wrong_data_type_error())?;
        let mut deleted = 0;
        for k in source {
            if let Some(_) = hv.remove(k) {
                deleted += 1;
            }
        }
        Ok((deleted, hv.len()))
    }
}
