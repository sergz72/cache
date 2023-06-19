use std::sync::Arc;
use crate::resp_commands::{run_config_command, run_dbsize_command, run_del_command, run_flush_command, run_get_command, run_ping_command, run_select_command, run_set_command};
use crate::resp_parser::RespToken::{RespArray, RespBinaryString, RespInteger, RespNullArray, RespNullString, RespString};
use crate::common_data::CommonData;

pub trait RespCommand {
    fn run(&self, common_data: Arc<CommonData>) -> Vec<u8>;
}

#[derive(PartialEq, Debug)]
pub enum RespToken {
    RespArray(Vec<RespToken>),
    RespString(Vec<u8>),
    RespBinaryString(Vec<u8>),
    RespInteger(isize),
    RespNullArray,
    RespNullString
}

pub static INVALID_COMMAND_ERROR: &str = "-invalid command\r\n";

pub fn resp_parse(buffer: &[u8], amt: usize, common_data: Arc<CommonData>) -> Vec<u8> {
    let tokens = match parse_tokens(buffer, amt) {
        Ok(t) => t,
        Err(e) => return Vec::from(e)
    };
    let mut result = Vec::new();
    for token in tokens {
        run_command(token, &mut result, common_data.clone());
    }
    result
}

pub fn check_name(s: &Vec<u8>, idx: usize, expected: &str) -> bool {
    if s.len() == idx + expected.len() {
        let b = expected.as_bytes();
        for i in idx..s.len() {
            let v1 = s[i];
            let v2 = b[i - idx];
            if v1 != v2 && v1 + 0x20 != v2 {
                return false;
            }
        }
        return true;
    }
    false
}

fn run_command(token: RespToken, result: &mut Vec<u8>, common_data: Arc<CommonData>) {
    match token {
        RespArray(v) => {
            if v.len() > 0 {
                match &v[0] {
                    RespBinaryString(s) => {
                        if s.len() > 0 {
                            match s[0] as char {
                                'c'|'C' => {
                                    if check_name(s, 1, "onfig") {
                                        run_config_command(v, result, common_data);
                                    } else {
                                        result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                    }
                                },
                                'd'|'D' => {
                                    match s.len() {
                                        3 => if check_name(s, 1, "el") {
                                            run_del_command(v, result, common_data);
                                        } else {
                                            result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                        },
                                        6 => if check_name(s, 1, "bsize") {
                                            run_dbsize_command(result, common_data);
                                        } else {
                                            result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                        },
                                        _ => result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes())
                                    }
                                },
                                'f'|'F' => {
                                    match s.len() {
                                        7 => {
                                            if check_name(s, 1, "lushdb") {
                                                run_flush_command(result, common_data);
                                            } else {
                                                result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                            }
                                        }
                                        8 => {
                                            if check_name(s, 1, "lushall") {
                                                run_flush_command(result, common_data);
                                            } else {
                                                result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                            }
                                        }
                                        _ => result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes())
                                    }
                                },
                                'g'|'G' => {
                                    if check_name(s, 1, "et") {
                                        run_get_command(v, result, common_data);
                                    } else {
                                        result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                    }
                                },
                                's'|'S' => {
                                    match s.len() {
                                        3 => if check_name(s, 1, "et") {
                                            run_set_command(v, result, common_data);
                                        } else {
                                            result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                        },
                                        6 => if check_name(s, 1, "elect") {
                                            run_select_command(result);
                                        } else {
                                            result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                        },
                                        _ => result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes())
                                    }
                                },
                                'p'|'P' => {
                                    if check_name(s, 1, "ing") {
                                        run_ping_command(v, result);
                                    } else {
                                        result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                                    }
                                },
                                _ => result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes())
                            }
                            return;
                        }
                        result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                    }
                    _ => result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes())
                }
                return;
            }
            result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
        }
        RespString(s) => {
            if s.len() > 0 {
                match s[0] as char {
                    'p'|'P' => {
                        if check_name(&s, 1, "ing") {
                            run_ping_command(Vec::new(), result);
                        } else {
                            result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes());
                        }
                    },
                    _ => result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes())
                }
                return;
            }
            result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes())
        }
        _ => result.extend_from_slice(INVALID_COMMAND_ERROR.as_bytes())
    }
}

fn parse_tokens(buffer: &[u8], amt: usize) -> Result<Vec<RespToken>, &'static str>  {
    let mut idx = 0;
    let mut tokens = Vec::new();
    while idx < amt {
        let (new_idx, token) = parse_token(buffer, idx, amt)?;
        idx = new_idx;
        tokens.push(token);
    }
    if idx != amt {
        return Err(INVALID_COMMAND_ERROR);
    }
    Ok(tokens)
}

fn parse_token(buffer: &[u8], idx: usize, amt: usize) -> Result<(usize, RespToken), &'static str> {
    if idx < amt {
        match buffer[idx] as char {
            '*' => {
                let (new_idx, token) = parse_array(buffer, idx + 1, amt)?;
                Ok((new_idx, token))
            }
            '$' => {
                let (new_idx, token) = parse_binary_string(buffer, idx + 1, amt)?;
                Ok((new_idx, token))
            }
            ':' => {
                let (new_idx, n) = parse_number(buffer, idx + 1, amt)?;
                Ok((new_idx, RespInteger(n)))
            }
            _ => {
                let (new_idx, s) = parse_string(buffer, idx, amt)?;
                Ok((new_idx, RespString(s)))
            }
        }
    } else {
        Err(INVALID_COMMAND_ERROR)
    }
}

fn parse_string(buffer: &[u8], idx: usize, amt: usize) -> Result<(usize, Vec<u8>), &'static str> {
    let mut new_idx = idx;
    while new_idx < amt {
        if buffer[new_idx] == '\r' as u8 {
            return Ok((new_idx+2, Vec::from(&buffer[idx..new_idx])));
        }
        new_idx += 1;
    }
    Err(INVALID_COMMAND_ERROR)
}

fn parse_binary_string(buffer: &[u8], idx: usize, amt: usize) -> Result<(usize, RespToken), &'static str> {
    let (new_idx, count) = parse_number(buffer, idx, amt)?;
    if count == -1 {
        return Ok((new_idx, RespNullString));
    }
    if count == 0 {
        return Ok((new_idx + 2, RespBinaryString(Vec::new())))
    }
    if count < 0 {
        return Err(INVALID_COMMAND_ERROR);
    }
    let string_end = new_idx + (count as usize);
    let end = string_end + 2;
    if end > amt {
        return Err(INVALID_COMMAND_ERROR);
    }
    Ok((end, RespBinaryString(Vec::from(&buffer[new_idx..string_end]))))
}

fn parse_array(buffer: &[u8], idx: usize, amt: usize) -> Result<(usize, RespToken), &'static str> {
    let (mut new_idx, n) = parse_number(buffer, idx, amt)?;
    if n == -1 {
        return Ok((new_idx, RespNullArray));
    }
    if n < 0 {
        return Err(INVALID_COMMAND_ERROR);
    }
    let mut result = Vec::new();
    for _i in 0..n {
        let (new_idx2, token) = parse_token(buffer, new_idx, amt)?;
        new_idx = new_idx2;
        result.push(token);
    }
    Ok((new_idx, RespArray(result)))
}

fn parse_number(buffer: &[u8], idx: usize, amt: usize) -> Result<(usize, isize), &'static str> {
    let mut result = 0;
    let mut sign = 1;
    let mut new_idx = idx;
    loop {
        if new_idx >= amt {
            break
        }
        let c = buffer[new_idx];
        match c as char {
            '-' => sign = -sign,
            '0'..='9' => result = result * 10 + (c - '0' as u8) as isize,
            '\r' => {
                if idx == new_idx {
                    break;
                }
                return Ok((new_idx + 2, result * sign));
            }
            _ => break
        }
        new_idx += 1;
    }
    Err(INVALID_COMMAND_ERROR)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::build_common_data;
    use crate::resp_parser::{parse_tokens, resp_parse};
    use crate::resp_parser::RespToken::{RespArray, RespBinaryString, RespInteger, RespString};

    const BUFFER: &[u8] = "PING\r\n*5\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\nex\r\n:10\r\n*3\r\n$6\r\nconfig\r\n$3\r\nget\r\n$4\r\nsave\r\n".as_bytes();

    #[test]
    fn test_parse_tokens() -> Result<(), &'static str> {
        let result = parse_tokens(BUFFER, BUFFER.len())?;
        assert_eq!(result.len(), 3);
        match &result[0] {
            RespString(s) => {
                assert_eq!(s, &"PING".to_string().into_bytes());
            },
            _ => return Err("error")
        }
        match &result[1] {
            RespArray(v) => {
                assert_eq!(v.len(), 5);
                assert_eq!(v[0], RespBinaryString(Vec::from(['s' as u8, 'e' as u8, 't' as u8])));
                assert_eq!(v[1], RespBinaryString(Vec::from(['a' as u8])));
                assert_eq!(v[2], RespBinaryString(Vec::from(['b' as u8])));
                assert_eq!(v[3], RespBinaryString(Vec::from(['e' as u8, 'x' as u8])));
                assert_eq!(v[4], RespInteger(10));
            },
            _ => return Err("error")
        }
        match &result[2] {
            RespArray(v) => {
                assert_eq!(v.len(), 3);
                assert_eq!(v[0], RespBinaryString(Vec::from(['c' as u8, 'o' as u8, 'n' as u8, 'f' as u8, 'i' as u8, 'g' as u8])));
                assert_eq!(v[1], RespBinaryString(Vec::from(['g' as u8, 'e' as u8, 't' as u8])));
                assert_eq!(v[2], RespBinaryString(Vec::from(['s' as u8, 'a' as u8, 'v' as u8, 'e' as u8])));
                Ok(())
            },
            _ => Err("error")
        }
    }

    #[test]
    fn test_parse() {
        let common_data = Arc::new(build_common_data(false, 1000));
        let result = resp_parse(BUFFER, BUFFER.len(), common_data);
        assert_eq!(result.as_slice(), "+PONG\r\n+OK\r\n*2\r\n$4\r\nsave\r\n$0\r\n\r\n".as_bytes());
    }
}
