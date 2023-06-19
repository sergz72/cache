use std::io::{Error, ErrorKind};

pub trait HashBuilder {
    fn build_hash(&self, key: &Vec<u8>) -> usize;
    fn get_name(&self) -> &'static str;
}

struct SumHashBuilder {
    max_value: usize
}

impl SumHashBuilder {
    fn new(max_value: usize) -> SumHashBuilder {
        SumHashBuilder{ max_value }
    }
}

impl HashBuilder for SumHashBuilder {
    fn build_hash(&self, key: &Vec<u8>) -> usize {
        let hash_sum: usize = key.iter().map(|i| *i as usize).sum();
        hash_sum % self.max_value
    }

    fn get_name(&self) -> &'static str {
        "sum"
    }
}

struct XorHashBuilder {
    max_value: u8
}

impl XorHashBuilder {
    fn new(max_value: u8) -> XorHashBuilder {
        XorHashBuilder{ max_value }
    }
}

impl HashBuilder for XorHashBuilder {
    fn build_hash(&self, key: &Vec<u8>) -> usize {
        let hash_sum: u8 = key.iter().fold(0, |sum, v|sum ^ *v);
        (hash_sum % self.max_value) as usize
    }

    fn get_name(&self) -> &'static str {
        "xor"
    }
}

struct XorHashBuilder256;

impl XorHashBuilder256 {
    fn new() -> XorHashBuilder256 {
        XorHashBuilder256{}
    }
}

impl HashBuilder for XorHashBuilder256 {
    fn build_hash(&self, key: &Vec<u8>) -> usize {
        let hash_sum: u8 = key.iter().fold(0, |sum, v|sum ^ *v);
        hash_sum as usize
    }

    fn get_name(&self) -> &'static str {
        "xor256"
    }
}

struct ZeroHashBuilder;

impl ZeroHashBuilder {
    fn new() -> ZeroHashBuilder {
        ZeroHashBuilder{}
    }
}

impl HashBuilder for ZeroHashBuilder {
    fn build_hash(&self, _key: &Vec<u8>) -> usize {
        0
    }

    fn get_name(&self) -> &'static str {
        "zero"
    }
}

pub fn create_hash_builder(name: String, max_value: usize) -> Result<Box<dyn HashBuilder + Send + Sync>, Error> {
    if max_value == 1 {
        return Ok(Box::new(ZeroHashBuilder::new()))
    }
    match name.as_str() {
        "xor" => {
            if max_value < 256 {
                Ok(Box::new(XorHashBuilder::new(max_value as u8)))
            } else if max_value == 256 {
                Ok(Box::new(XorHashBuilder256::new()))
            } else {
                Err(Error::new(ErrorKind::InvalidInput, "xor hash builder supports only max_value <= 256"))
            }
        },
        "sum" => Ok(Box::new(SumHashBuilder::new(max_value))),
        _ => Err(Error::new(ErrorKind::InvalidInput, "invalid hash builder type"))
    }
}