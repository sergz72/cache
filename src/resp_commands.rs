use crate::resp_parser::{check_name, INVALID_COMMAND_ERROR, RespCommand, RespToken};
use crate::resp_parser::RespToken::{RespBinaryString, RespInteger};

pub struct PingCommand;

impl RespCommand for PingCommand {
    fn run(&self) -> String {
        "+PONG\r\n".to_string()
    }
}

impl PingCommand {
    pub fn new(_v: Vec<RespToken>) -> Result<Box<dyn RespCommand>, &'static str> {
        Ok(Box::new(PingCommand {}))
    }
}

pub struct GetCommand {
    key: Vec<u8>
}

impl RespCommand for GetCommand {
    fn run(&self) -> String {
        "$-1\r\n".to_string()
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
    fn run(&self) -> String {
        "+OK\r\n".to_string()
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
    fn run(&self) -> String {
        "+PONG\r\n".to_string()
    }
}

impl ConfigurationCommand {
    pub fn new(v: Vec<RespToken>) -> Result<Box<dyn RespCommand>, &'static str> {
        if v.len() == 3 {
            if let RespBinaryString(subcommand) = &v[1] {
                check_name(subcommand, 0, "get")?;
                if let RespBinaryString(key) = &v[2] {
                    return Ok(Box::new(ConfigurationCommand{ key: key.clone() }));
                }
            }
        }
        Err(INVALID_COMMAND_ERROR)
    }
}
