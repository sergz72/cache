mod work_handler;
mod server;
mod resp_parser;
mod resp_commands;
mod resp_encoder;

use std::collections::HashMap;
use std::env::args;
use std::io::{Error, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::thread::available_parallelism;
use arguments_parser::{Arguments, IntParameter, SizeParameter, BoolParameter, Switch, StringParameter};
use crate::work_handler::{Command, CommonData, create_thread_pool};
use ctrlc;
use crate::resp_encoder::resp_encode_strings;
use crate::server::server_start;

fn main() -> Result<(), Error> {
    let host_parameter = StringParameter::new("127.0.0.1");
    let port_parameter = IntParameter::new(6379);
    let max_memory_parameter = SizeParameter::new(1024 * 1024 * 1024);//1G
    let threads_parameter = IntParameter::new(available_parallelism().unwrap().get() as isize);//1G
    let verbose_parameter = BoolParameter::new();
    let client_parameter = BoolParameter::new();
    let switches = [
        Switch::new("host", Some('h'), None, &host_parameter),
        Switch::new("port", Some('p'), None, &port_parameter),
        Switch::new("maximum_memory", Some('m'), None, &max_memory_parameter),
        Switch::new("threads", Some('t'), None, &threads_parameter),
        Switch::new("verbose", Some('v'), None, &verbose_parameter),
        Switch::new("client", Some('c'), None, &client_parameter),
    ];
    let mut arguments = Arguments::new("cache", &switches);
    if let Err(e) = arguments.build(args().skip(1).collect()) {
        println!("{}", e);
        arguments.usage();
        return Ok(());
    }
    let port = port_parameter.get_value();
    if port <= 0 || port > 0xFFFF {
        println!("Invalid port value");
        return Ok(());
    }
    let max_memory = max_memory_parameter.get_value();
    if max_memory <= 0 {
        println!("Invalid maximum_memory value");
        return Ok(());
    }
    let threads = threads_parameter.get_value();
    if threads <= 0 {
        println!("Invalid threads value");
        return Ok(());
    }
    let verbose = verbose_parameter.get_value();
    if verbose {
        println!("Port = {}\nMaximum memory = {}\nThreads = {}", port, max_memory, threads);
    }
    if client_parameter.get_value() {
        if arguments.get_other_arguments().len() != 0 {
            let data = resp_encode_strings(arguments.get_other_arguments());
            let mut connection = TcpStream::connect(format!("{}:{}", host_parameter.get_value(), port))?;
            connection.write_all(data.as_slice())?;
            let mut buffer = [0; 10000];
            let amt = connection.read(&mut buffer)?;
            match String::from_utf8(Vec::from(&buffer[0..amt])) {
                Ok(s) => print!("{}", s),
                Err(e) => println!("{}", e)
            };
        } else {
            println!("No commands specified");
            return Ok(());
        }
    } else {
        let (tx, rx) = channel();
        let exit_flag = Arc::new(AtomicBool::new(false));
        let s = tx.clone();
        let f = exit_flag.clone();
        let p = port as u16;
        ctrlc::set_handler(move || {
            f.store(true, Ordering::Relaxed);
            //stopping the server
            TcpStream::connect(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), p)).unwrap();
            //stopping working threads
            for _i in 0..threads {
                s.send(Command::stop()).unwrap();
            }
        }).unwrap();
        let common_data = build_common_data(verbose);
        let pool = create_thread_pool(threads, rx, common_data);
        server_start(tx, p, exit_flag)?;
        println!("Waiting for all threads to be finished...");
        for h in pool {
            h.join().unwrap();
        }
        println!("Exiting...");
    }
    Ok(())
}

pub fn build_common_data(verbose: bool) -> CommonData {
    CommonData{ verbose, configuration: build_configuration(), map: build_map() }
}

fn build_map() -> RwLock<HashMap<Vec<u8>, Vec<u8>>> {
    RwLock::new(HashMap::new())
}

fn build_configuration() -> HashMap<Vec<u8>, Vec<u8>> {
    HashMap::from([
        ("save".to_string().into_bytes(), "".to_string().into_bytes()),
        ("appendonly".to_string().into_bytes(), "no".to_string().into_bytes())])
}
