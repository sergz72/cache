use std::io::{Error, ErrorKind};

pub fn build_out_of_memory_error() -> Error {
    Error::new(ErrorKind::OutOfMemory, "-out of memory\r\n")
}

pub fn build_wrong_data_type_error() -> Error {
    Error::new(ErrorKind::InvalidData, "-Operation against a key holding the wrong kind of value\r\n")
}
