use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use crate::resp_parser::resp_parse;

pub struct Command {
    stream: Option<TcpStream>,
    stop: bool,
}

impl Command {
    pub fn stop() -> Command {
        Command { stream: None, stop: true }
    }

    pub fn stream(stream: TcpStream) -> Command {
        Command { stream: Some(stream), stop: false }
    }
}

pub struct CommonData {
    pub verbose: bool,
    pub configuration: HashMap<Vec<u8>, Vec<u8>>,
    pub map: RwLock<HashMap<Vec<u8>, Vec<u8>>>
}

pub fn create_thread_pool(threads: isize, rx: Receiver<Command>, common_data: CommonData) -> Vec<JoinHandle<()>> {
    let arc = Arc::new(Mutex::new(rx));
    let carc = Arc::new(common_data);
    let mut result = Vec::new();
    for _i in 0..threads {
        let a = arc.clone();
        let c = carc.clone();
        result.push(thread::spawn(|| {
            work_handler(a, c);
        }));
    }
    result
}

fn work_handler(data: Arc<Mutex<Receiver<Command>>>, common_data: Arc<CommonData>) {
    loop {
        let mut command = data.lock().unwrap().recv().unwrap();
        if command.stop {
            if common_data.verbose {
                println!("Stopping thread...");
            }
            return;
        }
        let mut buffer = [0; 1000000];
        let s = command.stream.as_mut().unwrap();
        loop {
            match s.read(&mut buffer) {
                Ok(amt) => {
                    if amt == 0 {
                        break;
                    }
                    let _ = s.write_all(resp_parse(&buffer, amt, common_data.clone()).as_slice());
                },
                Err(e) => {
                    println!("Stream read error {}", e);
                    break;
                }
            }
        }
    }
}