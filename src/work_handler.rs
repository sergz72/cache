use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use crate::resp_parser::resp_parse;

pub struct CommonData {
    pub verbose: bool,
    pub max_memory: usize,
    pub configuration: HashMap<Vec<u8>, Vec<u8>>,
    pub map: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    pub exit_flag: AtomicBool,
    pub threads: RwLock<HashMap<usize, Arc<Mutex<TcpStream>>>>
}

fn build_configuration() -> HashMap<Vec<u8>, Vec<u8>> {
    HashMap::from([
        ("save".to_string().into_bytes(), "".to_string().into_bytes()),
        ("appendonly".to_string().into_bytes(), "no".to_string().into_bytes())])
}

pub fn build_common_data(verbose: bool, max_memory: usize) -> CommonData {
    CommonData{ verbose, max_memory, configuration: build_configuration(), map: build_map(),
        exit_flag:  AtomicBool::new(false), threads: RwLock::new(HashMap::new()) }
}

fn build_map() -> RwLock<HashMap<Vec<u8>, Vec<u8>>> {
    RwLock::new(HashMap::new())
}

pub fn work_handler<'a>(idx: usize, stream: Arc<Mutex<TcpStream>>, common_data: Arc<CommonData>) {
    let mut buffer = [0; 1000000];
    loop {
        let mut guard = stream.lock().unwrap();
        let s = guard.deref_mut();
        let result = s.read(&mut buffer);
        match result {
            Ok(amt) => {
                if amt == 0 {
                    break;
                }
                let _ = s.write_all(resp_parse(&buffer, amt, common_data.clone()).as_slice());
            },
            Err(e) => {
                if common_data.exit_flag.load(Ordering::Relaxed) {
                    if common_data.verbose {
                        println!("Stopping thread...");
                    }
                } else {
                    println!("Stream read error {}", e);
                }
                break;
            }
        }
    }
    common_data.threads.write().unwrap().remove(&idx);
}