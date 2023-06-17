use std::sync::Arc;
use crate::resp_encoder::{resp_encode_array2, resp_encode_binary_string, resp_encode_int};
use crate::resp_parser::{check_name, INVALID_COMMAND_ERROR, RespToken};
use crate::resp_parser::RespToken::{RespBinaryString, RespInteger};
use crate::work_handler::CommonData;

static NULL_STRING: &[u8] = "$-1\r\n".as_bytes();
static PONG: &[u8] = "+PONG\r\n".as_bytes();
pub static RN: &[u8] = "\r\n".as_bytes();
static OK: &[u8] = "+OK\r\n".as_bytes();
static NULL_ARRAY: &[u8] = "*-1\r\n".as_bytes();

pub fn run_ping_command(result: &mut Vec<u8>) {
    result.extend_from_slice(PONG);
}

pub fn run_dbsize_command(result: &mut Vec<u8>, common_data: Arc<CommonData>) {
    resp_encode_int(common_data.map.read().unwrap().len() as isize, result)
}

pub fn run_get_command(v: Vec<RespToken>, result: &mut Vec<u8>, common_data: Arc<CommonData>) {
    if v.len() == 2 {
        if let RespBinaryString(key) = &v[1] {
            if let Some(v) = common_data.map.read().unwrap().get(key) {
                resp_encode_binary_string(v, result);
                return;
            }
            result.extend_from_slice(NULL_STRING);
            return;
        }
    }
    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
}

fn set(key: &Vec<u8>, value: &Vec<u8>, _expiry: Option<usize>, common_data: Arc<CommonData>) {
    common_data.map.write().unwrap().insert(key.clone(), value.clone());
}

pub fn run_set_command(v: Vec<RespToken>, result: &mut Vec<u8>, common_data: Arc<CommonData>) {
    let l = v.len();
    if l >= 3 {
        if let RespBinaryString(k) = &v[1] {
            if let RespBinaryString(vv) = &v[2] {
                if l == 3 {
                    set(k, vv, None, common_data);
                    result.extend_from_slice(OK);
                    return;
                } else if l == 5 {
                    if let RespBinaryString(option) = &v[3] {
                        if check_name(option, 0, "ex") {
                            if let RespInteger(ex) = &v[4] {
                                set(k, vv, Some(*ex as usize), common_data);
                                result.extend_from_slice(OK);
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
}

pub fn run_config_command(v: Vec<RespToken>, result: &mut Vec<u8>, common_data: Arc<CommonData>) {
    if v.len() == 3 {
        if let RespBinaryString(subcommand) = &v[1] {
            if check_name(subcommand, 0, "get") {
                if let RespBinaryString(key) = &v[2] {
                    if let Some(v) = common_data.configuration.get(key) {
                        resp_encode_array2(key, v, result);
                        return;
                    }
                    result.extend_from_slice(NULL_ARRAY);
                    return;
                }
            }
        }
    }
    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
}
