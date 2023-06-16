use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
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

struct HandlerData {
    receiver: Mutex<Receiver<Command>>,
    verbose: bool
}

pub fn create_thread_pool(threads: isize, rx: Receiver<Command>, verbose: bool) -> Vec<JoinHandle<()>> {
    let arc = Arc::new(HandlerData{ receiver: Mutex::new(rx), verbose });
    let mut result = Vec::new();
    for _i in 0..threads {
        let a = arc.clone();
        result.push(thread::spawn(|| {
            work_handler(a);
        }));
    }
    result
}

fn work_handler(data: Arc<HandlerData>) {
    loop {
        let mut command = data.receiver.lock().unwrap().recv().unwrap();
        if command.stop {
            if data.verbose {
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
                    match resp_parse(&buffer, amt) {
                        Ok(c) => {
                            for command in c {
                                if s.write_all(command.run().as_bytes()).is_err() {
                                    break;
                                }
                            }
                        },
                        Err(e) => {
                            if data.verbose {
                                let s = String::from_utf8(Vec::from(&buffer[0..amt])).unwrap();
                                println!("{} {}", s, e);
                            }
                            let _ = s.write_all(e.as_bytes());
                        }
                    }
                    //let _ = s.shutdown(Shutdown::Both);
                },
                Err(e) => {
                    println!("Stream read error {}", e);
                    break;
                }
            }
        }
    }
}