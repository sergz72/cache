use std::sync::Arc;
use crate::resp_encoder::{resp_encode_array, resp_encode_binary_string, resp_encode_int};
use crate::resp_parser::{check_name, INVALID_COMMAND_ERROR, RespCommand, RespToken};
use crate::resp_parser::RespToken::{RespBinaryString, RespInteger};
use crate::work_handler::CommonData;

static NULL_STRING: &[u8] = "$-1\r\n".as_bytes();
static PONG: &[u8] = "+PONG\r\n".as_bytes();
pub static RN: &[u8] = "\r\n".as_bytes();
static OK: &[u8] = "+OK\r\n".as_bytes();
static NULL_ARRAY: &[u8] = "*-1\r\n".as_bytes();

pub struct PingCommand;

impl RespCommand for PingCommand {
    fn run(&self, _common_data: Arc<CommonData>) -> Vec<u8> {
        Vec::from(PONG)
    }
}

impl PingCommand {
    pub fn new() -> Result<Box<dyn RespCommand>, &'static str> {
        Ok(Box::new(PingCommand {}))
    }
}

pub struct DbSizeCommand;

impl RespCommand for DbSizeCommand {
    fn run(&self, common_data: Arc<CommonData>) -> Vec<u8> {
        resp_encode_int(common_data.map.read().unwrap().len() as isize)
    }
}

impl DbSizeCommand {
    pub fn new() -> Result<Box<dyn RespCommand>, &'static str> {
        Ok(Box::new(DbSizeCommand {}))
    }
}

pub struct GetCommand {
    key: Vec<u8>
}

impl RespCommand for GetCommand {
    fn run(&self, _common_data: Arc<CommonData>) -> Vec<u8> {
        if let Some(v) = _common_data.map.read().unwrap().get(&self.key) {
            let mut result = Vec::new();
            resp_encode_binary_string(v, &mut result);
            return result;
        }
        Vec::from(NULL_STRING)
    }
}

impl GetCommand {
    pub fn new(v: Vec<RespToken>) -> Result<Box<dyn RespCommand>, &'static str> {
        if v.len() == 2 {
            if let RespBinaryString(s) = &v[1] {
                return Ok(Box::new(GetCommand{key: s.clone()}))
            }
        }
        Err(INVALID_COMMAND_ERROR)
    }
}

pub struct SetCommand {
    key: Vec<u8>,
    value: Vec<u8>,
    expiry: Option<usize>
}

impl RespCommand for SetCommand {
    fn run(&self, common_data: Arc<CommonData>) -> Vec<u8> {
        common_data.map.write().unwrap().insert(self.key.clone(), self.value.clone());
        Vec::from(OK)
    }
}

impl SetCommand {
    pub fn new(v: Vec<RespToken>) -> Result<Box<dyn RespCommand>, &'static str> {
        let l = v.len();
        if l >= 3 {
            if let RespBinaryString(k) = &v[1] {
                if let RespBinaryString(vv) = &v[2] {
                    if l == 3 {
                        return Ok(Box::new(SetCommand { key: k.clone(), value: vv.clone(), expiry: None }));
                    } else if l == 5 {
                        if let RespBinaryString(option) = &v[3] {
                            check_name(option, 0, "ex")?;
                            if let RespInteger(ex) = &v[4] {
                                return Ok(Box::new(SetCommand { key: k.clone(), value: vv.clone(), expiry: Some(*ex as usize) }))
                            }
                        }
                    }
                }
            }
        }
        Err(INVALID_COMMAND_ERROR)
    }
}

pub struct ConfigurationCommand {
    key: Vec<u8>
}

impl RespCommand for ConfigurationCommand {
    fn run(&self, common_data: Arc<CommonData>) -> Vec<u8> {
        if let Some(v) = common_data.configuration.get(&self.key) {
            return resp_encode_array(&vec![&self.key, v]);
        }
        Vec::from(NULL_ARRAY)
    }
}

impl ConfigurationCommand {
    pub fn new(v: Vec<RespToken>) -> Result<Box<dyn RespCommand>, &'static str> {
        if v.len() == 3 {
            if let RespBinaryString(subcommand) = &v[1] {
                check_name(subcommand, 0, "get")?;
                if let RespBinaryString(key) = &v[2] {
                    return Ok(Box::new(ConfigurationCommand { key: key.clone() }));
                }
            }
        }
        Err(INVALID_COMMAND_ERROR)
    }
}
