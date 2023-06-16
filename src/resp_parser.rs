use crate::resp_commands::{ConfigurationCommand, GetCommand, PingCommand, SetCommand};
use crate::resp_parser::RespToken::{RespArray, RespBinaryString, RespInteger, RespNullArray, RespNullString};

pub trait RespCommand {
    fn run(&self) -> String;
}

#[derive(PartialEq, Debug)]
pub enum RespToken {
    RespArray(Vec<RespToken>),
//    RespString(String),
    RespBinaryString(Vec<u8>),
    RespInteger(isize),
    RespNullArray,
    RespNullString
}

pub static INVALID_COMMAND_ERROR: &str = "-invalid command\r\n";

pub fn resp_parse(buffer: &[u8], amt: usize) -> Result<Vec<Box<dyn RespCommand>>, &'static str> {
    let tokens = parse_tokens(buffer, amt)?;
    let mut result = Vec::new();
    for token in tokens {
        result.push(build_command(token)?);
    }
    Ok(result)
}

pub fn check_name(s: &Vec<u8>, idx: usize, expected: &str) -> Result<(), &'static str> {
    if s.len() == idx + expected.len() {
        let b = expected.as_bytes();
        for i in idx..s.len() {
            let v1 = s[i];
            let v2 = b[i - idx];
            if v1 != v2 && v1 + 0x20 != v2 {
                return Err(INVALID_COMMAND_ERROR);
            }
        }
        return Ok(());
    }
    Err(INVALID_COMMAND_ERROR)
}

fn build_command(token: RespToken) -> Result<Box<dyn RespCommand>, &'static str> {
    match token {
        RespArray(v) => {
            if v.len() > 0 {
                return match &v[0] {
                    RespBinaryString(s) => {
                        if s.len() > 0 {
                            return match s[0] as char {
                                'c' => { check_name(s, 1, "onfig")?; ConfigurationCommand::new(v) },
                                'g' => { check_name(s, 1, "et")?; GetCommand::new(v) },
                                's' => { check_name(s, 1, "et")?; SetCommand::new(v) },
                                'p' => { check_name(s, 1, "ing")?; PingCommand::new(v) },
                                _ => Err(INVALID_COMMAND_ERROR)
                            }
                        }
                        Err(INVALID_COMMAND_ERROR)
                    }
                    _ => Err(INVALID_COMMAND_ERROR)
                }
            }
            Err(INVALID_COMMAND_ERROR)
        }
        _ => Err(INVALID_COMMAND_ERROR)
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
            _ => Err(INVALID_COMMAND_ERROR)
        }
    } else {
        Err(INVALID_COMMAND_ERROR)
    }
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
    use crate::resp_parser::parse_tokens;
    use crate::resp_parser::RespToken::{RespArray, RespBinaryString, RespInteger};

    #[test]
    fn test_parse_tokens() -> Result<(), &'static str> {
        let buffer = "*5\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\nex\r\n:10\r\n*3\r\n$6\r\nconfig\r\n$3\r\nget\r\n$4\r\nsave\r\n".as_bytes();
        let result = parse_tokens(buffer, buffer.len())?;
        assert_eq!(result.len(), 2);
        match &result[0] {
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
        match &result[1] {
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
}
