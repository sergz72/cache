use std::io::{Error, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::SystemTime;
use rand::Rng;
use crate::benchmark;
use crate::resp_encoder::resp_encode_strings;

#[derive(Clone)]
pub enum BenchmarkCommand {
    Get,
    Set
}

pub fn benchmark_mode(port: u16, host: String, keys: usize, requests: usize, threads: usize,
                      expiration: usize, types: [BenchmarkCommand; 2]) -> Result<(), Error> {
    let mut tasks = Vec::new();
    let start = SystemTime::now();
    for _i in 0..threads {
        let h = host.clone();
        let t: [BenchmarkCommand; 2] = [types[0].clone(), types[1].clone()];
        tasks.push(thread::spawn(move ||{
            if let Err(e) = benchmark_worker(port, h, keys, requests, expiration, t) {
                println!("{}", e);
            }
        }))
    }
    for task in tasks {
        task.join().unwrap();
    }
    let elapsed = start.elapsed().unwrap().as_millis() as usize;
    println!("Elapsed: {} ms, {} requests per second", elapsed, requests * threads * 1000 / elapsed);
    Ok(())
}

fn benchmark_worker(port: u16, host: String, keys: usize, requests: usize, expiration: usize,
                    types: [BenchmarkCommand; 2]) -> Result<(), Error> {
    let mut rng = rand::thread_rng();
    let keys2 = keys * 2;
    let mut connection = TcpStream::connect(format!("{}:{}", host, port))?;
    let mut buffer = [0; 1000];
    let ex = expiration.to_string();
    for _i in 0..requests {
        let n = rng.gen::<usize>() % keys2;
        let key = (n / 2).to_string();
        let data = match &types[n & 1] {
            benchmark::BenchmarkCommand::Get => resp_encode_strings(&vec!["get".to_string(), key]),
            _ => resp_encode_strings(&vec!["set".to_string(), key.clone(), key, "nx".to_string(), ex.clone()])
        };
        connection.write_all(data.as_slice())?;
        connection.read(&mut buffer)?;
    }
    Ok(())
}
