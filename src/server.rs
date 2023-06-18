use std::io::Error;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use std::thread;
use crate::work_handler::{CommonData, work_handler};

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