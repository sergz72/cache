mod common_data;
mod server;
mod resp_parser;
mod resp_commands;
mod resp_encoder;
mod benchmark;
mod common_maps;
mod hash_builders;

use std::env::args;
use std::io::{Error, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use arguments_parser::{Arguments, IntParameter, SizeParameter, BoolParameter, Switch, StringParameter};
use crate::common_data::build_common_data;
use ctrlc;
use crate::benchmark::{benchmark_mode, BenchmarkCommand};
use crate::benchmark::BenchmarkCommand::{Get, Ping, Set, SetPX};
use crate::hash_builders::{create_hash_builder, HashBuilder};
use crate::resp_encoder::resp_encode_strings;
use crate::server::server_start;

fn main() -> Result<(), Error> {
    let host_parameter = StringParameter::new("127.0.0.1");
    let port_parameter = IntParameter::new(6379);
    let max_memory_parameter = SizeParameter::new(1024 * 1024 * 1024);//1G
    let verbose_parameter = BoolParameter::new();
    let client_parameter = BoolParameter::new();
    let benchmark_parameter = BoolParameter::new();
    let keys_parameter = IntParameter::new(50000);
    let requests_parameter = IntParameter::new(50000);
    let threads_parameter = IntParameter::new(10);
    let types_parameter = StringParameter::new("get,set,get,setpx");
    let expiration_parameter = IntParameter::new(100);
    let vector_size_parameter = IntParameter::new(256);
    let hash_type_parameter = StringParameter::new("sum");
    let switches = [
        Switch::new("host for client to connect", Some('h'), None, &host_parameter),
        Switch::new("port", Some('p'), None, &port_parameter),
        Switch::new("maximum memory for server", Some('m'), None, &max_memory_parameter),
        Switch::new("verbose", Some('v'), None, &verbose_parameter),
        Switch::new("client mode", Some('c'), None, &client_parameter),
        Switch::new("benchmark mode", Some('b'), None, &benchmark_parameter),
        Switch::new("number of keys for benchmark", Some('k'), None, &keys_parameter),
        Switch::new("number of requests per thread for benchmark", Some('r'), None, &requests_parameter),
        Switch::new("number of threads for benchmark", None, Some("th"), &threads_parameter),
        Switch::new("request types for benchmark", Some('t'), None, &types_parameter),
        Switch::new("key expiration in ms for benchmark", None, Some("nx"), &expiration_parameter),
        Switch::new("numer of key maps", None, Some("km"), &vector_size_parameter),
        Switch::new("hash builder type", None, Some("hb"), &hash_type_parameter),
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
    let p = port as u16;
    let verbose = verbose_parameter.get_value();
    if benchmark_parameter.get_value() {
        let keys = keys_parameter.get_value();
        if keys <= 0 {
            println!("Invalid keys value");
            return Ok(());
        }
        let requests = requests_parameter.get_value();
        if requests <= 0 {
            println!("Invalid requests value");
            return Ok(());
        }
        let threads = threads_parameter.get_value();
        if threads <= 0 {
            println!("Invalid threads value");
            return Ok(());
        }
        let expiration = expiration_parameter.get_value();
        if expiration <= 0 {
            println!("Invalid expiration value");
            return Ok(());
        }
        let types_string = types_parameter.get_value();
        let types: Vec<Option<BenchmarkCommand>> = types_string.split(',')
            .map(|s|{
                match s {
                    "get" => Some(Get),
                    "set" => Some(Set),
                    "setpx" => Some(SetPX),
                    "ping" => Some(Ping),
                    _ => None
                }
            }).collect();
        if types.len() != 4 || types[0].is_none() || types[1].is_none() || types[2].is_none() || types[3].is_none() {
            println!("Invalid request types value");
            return Ok(());
        }
        let host = host_parameter.get_value();
        if verbose {
            println!("Port = {}\nHost = {}\nKeys= {}\nRequests per thread = {}\nThreads = {}\nExpiration = {} ms\nRequest types = {}",
                     port, host, keys, requests, threads, expiration, types_string);
        }
        benchmark_mode(p, host, keys as usize, requests as usize,
                       threads as usize, expiration as usize,
                       [types[0].as_ref().unwrap().clone(), types[1].as_ref().unwrap().clone(),
                           types[2].as_ref().unwrap().clone(), types[3].as_ref().unwrap().clone()])
    } else if client_parameter.get_value() {
        let host= host_parameter.get_value();
        if verbose {
            println!("Port = {}\nHost = {}", port, host);
        }
        client_mode(arguments.get_other_arguments(), p, host)
    } else {
        let max_memory = max_memory_parameter.get_value();
        if max_memory <= 0 {
            println!("Invalid maximum_memory value");
            return Ok(());
        }
        let vector_size = vector_size_parameter.get_value();
        if vector_size <= 0 {
            println!("Invalid vector size value");
            return Ok(());
        }
        let vs = vector_size as usize;
        let hash_builder = create_hash_builder(hash_type_parameter.get_value(), vs)?;
        if verbose {
            println!("Port = {}\nMaximum memory = {}\nVector size = {}\nHash builder = {}", port,
                     max_memory, vector_size, hash_builder.get_name());
        }
        server_mode(verbose, max_memory as usize, p, vs, hash_builder)
    }
}

fn client_mode(other_arguments: &Vec<String>, port: u16, host: String) -> Result<(), Error> {
    if other_arguments.len() != 0 {
        let data = resp_encode_strings(other_arguments);
        let mut connection = TcpStream::connect(format!("{}:{}", host, port))?;
        connection.write_all(data.as_slice())?;
        let mut buffer = [0; 10000];
        let amt = connection.read(&mut buffer)?;
        match String::from_utf8(Vec::from(&buffer[0..amt])) {
            Ok(s) => print!("{}", s),
            Err(e) => println!("{}", e)
        };
    } else {
        println!("No commands specified");
    }
    Ok(())
}

fn server_mode(verbose: bool, max_memory: usize, port: u16, vector_size: usize,
               hash_builder: Box<dyn HashBuilder + Sync + Send>) -> Result<(), Error> {
    let common_data = Arc::new(build_common_data(verbose, max_memory, vector_size, hash_builder));
    let c = common_data.clone();
    ctrlc::set_handler(move || {
        c.exit_flag.store(true, Ordering::Relaxed);
        //stopping the server
        TcpStream::connect(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)).unwrap();
    }).unwrap();
    server_start(port, common_data.clone())?;
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
    Ok(())
}