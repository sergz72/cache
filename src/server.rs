use std::io::Error;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use crate::work_handler::Command;

pub fn server_start(tx: Arc<Mutex<Sender<Command>>>, port: u16, exit_flag: Arc<AtomicBool>) -> Result<(), Error> {
    let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port)))?;
    println!("Server listening on port {}", port);
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if exit_flag.load(Ordering::Relaxed) {
                    break;
                }
                tx.lock().unwrap().send(Command::stream(s)).unwrap();
            }
            Err(e) => {
                if exit_flag.load(Ordering::Relaxed) {
                    break;
                }
                println!("Connection error {}", e);
            }
        }
    }
    Ok(())
}