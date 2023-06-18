mod work_handler;
mod server;
mod resp_parser;
mod resp_commands;
mod resp_encoder;

use std::env::args;
use std::io::{Error, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use arguments_parser::{Arguments, IntParameter, SizeParameter, BoolParameter, Switch, StringParameter};
use crate::work_handler::build_common_data;
use ctrlc;
use crate::resp_encoder::resp_encode_strings;
use crate::server::server_start;

fn main() -> Result<(), Error> {
    let host_parameter = StringParameter::new("127.0.0.1");
    let port_parameter = IntParameter::new(6379);
    let max_memory_parameter = SizeParameter::new(1024 * 1024 * 1024);//1G
    let verbose_parameter = BoolParameter::new();
    let client_parameter = BoolParameter::new();
    let switches = [
        Switch::new("host", Some('h'), None, &host_parameter),
        Switch::new("port", Some('p'), None, &port_parameter),
        Switch::new("maximum_memory", Some('m'), None, &max_memory_parameter),
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
    let verbose = verbose_parameter.get_value();
    if verbose {
        println!("Port = {}\nMaximum memory = {}", port, max_memory);
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
        let common_data = Arc::new(build_common_data(verbose, max_memory as usize));
        let p = port as u16;
        let c = common_data.clone();
        ctrlc::set_handler(move || {
            c.exit_flag.store(true, Ordering::Relaxed);
            //stopping the server
            TcpStream::connect(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), p)).unwrap();
        }).unwrap();
        server_start(p, common_data.clone())?;
        println!("Waiting for all threads to be finished...");
        let v: Vec<usize> = common_data.threads.read().unwrap().iter()
            .map(|(k, _v)|*k)
            .collect();
        for idx in v  {
            if let Some(t) = common_data.threads.read().unwrap().get(&idx) {
                let _ = t.lock().unwrap().shutdown(Shutdown::Both);
            }
        }
        let d = Duration::from_millis(500);
        while common_data.threads.read().unwrap().len() > 0 {
            thread::sleep(d);
        }
        println!("Exiting...");
    }
    Ok(())
}
