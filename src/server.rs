use std::io::{Error, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::Ordering;
use std::thread;
use crate::common_data::CommonData;
use crate::common_maps::CommonMaps;
use crate::resp_parser::resp_parse;

pub struct WorkerData<'a> {
    pub current_db: &'a Vec<RwLock<CommonMaps>>
}

pub fn work_handler<'a>(idx: usize, stream: Arc<Mutex<TcpStream>>, common_data: Arc<CommonData>) {
    let mut buffer = [0; 1000000];
    let mut worker_data = WorkerData{ current_db: common_data.select("0".to_string().into_bytes()) };
    loop {
        let mut guard = stream.lock().unwrap();
        let s = guard.deref_mut();
        let result = s.read(&mut buffer);
        match result {
            Ok(amt) => {
                if amt == 0 {
                    break;
                }
                let _ = s.write_all(resp_parse(&buffer, amt, common_data.clone(), &mut worker_data).as_slice());
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

pub fn server_start(port: u16, common_data: Arc<CommonData>) -> Result<(), Error> {
    let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port)))?;
    println!("Server listening on port {}", port);
    let mut idx = 0;
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if common_data.exit_flag.load(Ordering::Relaxed) {
                    break;
                }
                let c = common_data.clone();
                let ss = Arc::new(Mutex::new(s));
                let cloned = ss.clone();
                thread::spawn(move ||{
                    work_handler(idx, cloned, c);
                });
                common_data.threads.write().unwrap().insert(idx, ss);
                idx += 1;
            }
            Err(e) => {
                if common_data.exit_flag.load(Ordering::Relaxed) {
                    break;
                }
                println!("Connection error {}", e);
            }
        }
    }
    Ok(())
}