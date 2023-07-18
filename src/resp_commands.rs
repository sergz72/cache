use std::collections::HashSet;
use std::sync::Arc;
use crate::resp_encoder::{resp_encode_array2, resp_encode_binary_string, resp_encode_int};
use crate::resp_parser::{check_name, INVALID_COMMAND_ERROR, RespToken};
use crate::resp_parser::RespToken::{RespBinaryString, RespInteger};
use crate::common_data::CommonData;
use crate::server::WorkerData;

static NULL_STRING: &[u8] = "$-1\r\n".as_bytes();
static PONG: &[u8] = "+PONG\r\n".as_bytes();
static OK: &[u8] = "+OK\r\n".as_bytes();
static NULL_ARRAY: &[u8] = "*-1\r\n".as_bytes();

pub fn run_ping_command(v: Vec<RespToken>, result: &mut Vec<u8>) {
    if v.len() >= 2 {
        if let RespBinaryString(s) = &v[1] {
            resp_encode_binary_string(s, result);
            return;
        }
    }
    result.extend_from_slice(PONG);
}

pub fn run_select_command(result: &mut Vec<u8>) {
    result.extend_from_slice(OK);
}

pub fn run_flush_command(result: &mut Vec<u8>, worker_data: &WorkerData) {
    CommonData::flush(worker_data);
    result.extend_from_slice(OK);
}

pub fn run_flushall_command(result: &mut Vec<u8>, common_data: Arc<CommonData>) {
    common_data.flush_all();
    result.extend_from_slice(OK);
}

pub fn run_dbsize_command(result: &mut Vec<u8>, worker_data: &WorkerData) {
    resp_encode_int(CommonData::size(worker_data) as isize, result)
}

pub fn run_del_command(v: Vec<RespToken>, result: &mut Vec<u8>, common_data: Arc<CommonData>, worker_data: &WorkerData) {
    if v.len() >= 2 {
        let mut keys = HashSet::new();
        for i in 1..v.len() {
            if let RespBinaryString(v) = &v[i] {
                keys.insert(v);
            } else {
                result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                return;
            }
        }
        let removed = common_data.removekeys(keys, worker_data);
        resp_encode_int(removed, result);
        return;
    }
    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
}

pub fn run_hdel_command(v: Vec<RespToken>, result: &mut Vec<u8>, common_data: Arc<CommonData>, worker_data: &WorkerData) {
    if v.len() >= 3 {
        if let RespBinaryString(key) = &v[1] {
            let mut keys = HashSet::new();
            for i in 2..v.len() {
                if let RespBinaryString(v) = &v[i] {
                    keys.insert(v);
                } else {
                    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                    return;
                }
            }
            match common_data.hdel(key, keys, worker_data) {
                Ok(removed) => resp_encode_int(removed, result),
                Err(e) => result.extend_from_slice(e.to_string().as_bytes())
            }
            return;
        }
    }
    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
}

pub fn run_get_command(v: Vec<RespToken>, result: &mut Vec<u8>, common_data: Arc<CommonData>, worker_data: &WorkerData) {
    if v.len() == 2 {
        if let RespBinaryString(key) = &v[1] {
            match common_data.get(key, result, worker_data) {
                Ok(b) => {
                    if !b {
                        result.extend_from_slice(NULL_STRING);
                    }
                }
                Err(e) => result.extend_from_slice(e.to_string().as_bytes())
            }
            return;
        }
    }
    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
}

pub fn run_hgetall_command(v: Vec<RespToken>, result: &mut Vec<u8>, common_data: Arc<CommonData>, worker_data: &WorkerData) {
    if v.len() == 2 {
        if let RespBinaryString(key) = &v[1] {
            match common_data.hgetall(key, result, worker_data) {
                Ok(b) => {
                    if !b {
                        result.extend_from_slice(NULL_STRING);
                    }
                }
                Err(e) => result.extend_from_slice(e.to_string().as_bytes())
            }
            return;
        }
    }
    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
}

pub fn run_hget_command(v: Vec<RespToken>, result: &mut Vec<u8>, common_data: Arc<CommonData>, worker_data: &WorkerData) {
    if v.len() == 3 {
        if let RespBinaryString(key) = &v[1] {
            if let RespBinaryString(map_key) = &v[2] {
                match common_data.hget(key, map_key, result, worker_data) {
                    Ok(b) => {
                        if !b {
                            result.extend_from_slice(NULL_STRING);
                        }
                    }
                    Err(e) => result.extend_from_slice(e.to_string().as_bytes())
                }
                return;
            }
        }
    }
    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
}

fn set_with_result(k: &Vec<u8>, vv: &Vec<u8>, e: isize, result: &mut Vec<u8>, common_data: Arc<CommonData>, worker_data: &WorkerData) {
    if e <= 0 {
        result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
    } else {
        common_data.set(k, vv, Some(e as u64), worker_data);
        result.extend_from_slice(OK);
    }
}

fn parse_number_from_vec(v: &Vec<u8>) -> Option<isize> {
    let mut sign = 1;
    let mut result = 0;
    for c in v {
        match *c as char {
            '-' => sign = -sign,
            '0'..='9' => result = result * 10 + (c - '0' as u8) as isize,
            _ => return None
        }
    }
    Some(result)
}

pub fn run_set_command(v: Vec<RespToken>, result: &mut Vec<u8>, common_data: Arc<CommonData>, worker_data: &WorkerData) {
    let l = v.len();
    if l >= 3 {
        if let RespBinaryString(k) = &v[1] {
            if let RespBinaryString(vv) = &v[2] {
                if l == 3 {
                    common_data.set(k, vv, None, worker_data);
                    result.extend_from_slice(OK);
                    return;
                } else if l == 5 {
                    if let RespBinaryString(option) = &v[3] {
                        if option.len() == 2 {
                            let c2 = option[1];
                            if c2 == 'x' as u8 || c2 == 'X' as u8 {
                                match option[0] as char {
                                    'e' | 'E' => {
                                        match &v[4] {
                                            RespInteger(ex) => {
                                                set_with_result(k, vv, *ex * 1000, result, common_data, worker_data);
                                            }
                                            RespBinaryString(v) => {
                                                if let Some(ex) = parse_number_from_vec(v) {
                                                    set_with_result(k, vv, ex * 1000, result, common_data, worker_data);
                                                } else {
                                                    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                                }
                                            }
                                            _ => result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes())
                                        }
                                        return;
                                    }
                                    'p'|'P' => {
                                        match &v[4] {
                                            RespInteger(ex) => {
                                                set_with_result(k, vv, *ex, result, common_data, worker_data);
                                            }
                                            RespBinaryString(v) => {
                                                if let Some(ex) = parse_number_from_vec(v) {
                                                    set_with_result(k, vv, ex, result, common_data, worker_data);
                                                } else {
                                                    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                                }
                                            }
                                            _ => result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes())
                                        }
                                        return;
                                    }
                                    _ => {
                                        result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
}

pub fn run_hset_command(v: Vec<RespToken>, result: &mut Vec<u8>, common_data: Arc<CommonData>) {
    todo!()
}

pub fn run_save_command(result: &mut Vec<u8>, common_data: Arc<CommonData>) {
    todo!()
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
