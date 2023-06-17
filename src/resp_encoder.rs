use crate::resp_commands::RN;

pub fn resp_encode_array(commands: &Vec<&Vec<u8>>) -> Vec<u8> {
    let mut result = Vec::new();
    result.push('*' as u8);
    result.extend(commands.len().to_string().into_bytes());
    result.extend_from_slice(RN);
    for command in commands {
        resp_encode_binary_string(*command, &mut result);
    }
    result
}

pub fn resp_encode_binary_string(string: &Vec<u8>, result: &mut Vec<u8>) {
    result.push('$' as u8);
    result.extend(string.len().to_string().into_bytes());
    result.extend_from_slice(RN);
    result.extend(string);
    result.extend_from_slice(RN);
}

pub fn resp_encode_string(string: &String, result: &mut Vec<u8>) {
    result.push('$' as u8);
    result.extend(string.len().to_string().into_bytes());
    result.extend_from_slice(RN);
    result.extend_from_slice(string.as_bytes());
    result.extend_from_slice(RN);
}

pub fn resp_encode_strings(commands: &Vec<String>) -> Vec<u8> {
    let mut result = Vec::new();
    result.push('*' as u8);
    result.extend(commands.len().to_string().into_bytes());
    result.extend_from_slice(RN);
    for command in commands {
        resp_encode_string(command, &mut result);
    }
    result
}

pub fn resp_encode_int(value: isize) -> Vec<u8> {
    let mut result = Vec::new();
    result.push(':' as u8);
    result.extend(value.to_string().into_bytes());
    result.extend_from_slice(RN);
    result
}
