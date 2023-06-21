use std::io::{Error, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::SystemTime;
use rand::Rng;
use crate::resp_encoder::resp_encode_strings;

#[derive(Clone)]
pub enum BenchmarkCommand {
    Get,
    Set,
    SetPX,
    Ping
}

pub fn benchmark_mode(port: u16, host: String, keys: usize, requests: usize, threads: usize,
                      expiration: usize, types: [BenchmarkCommand; 4]) -> Result<(), Error> {
    let mut tasks = Vec::new();
    let error_count = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(threads + 1));
    for _i in 0..threads {
        let h = host.clone();
        let t: [BenchmarkCommand; 4] = [types[0].clone(), types[1].clone(), types[2].clone(), types[3].clone()];
        let ec = error_count.clone();
        let b = barrier.clone();
        tasks.push(thread::spawn(move ||{
            if let Err(e) = benchmark_worker(port, h, keys, requests, expiration, t,
                                             ec, b) {
                println!("{}", e);
            }
        }))
    }
    println!("Preparing data for tests...");
    let start = SystemTime::now();
    barrier.wait();
    println!("{} ms Starting tests...", start.elapsed().unwrap().as_millis());
    for task in tasks {
        task.join().unwrap();
    }
    let elapsed = start.elapsed().unwrap().as_millis() as usize;
    println!("Elapsed: {} ms, {} requests per second {} errors",
             elapsed, requests * threads * 1000 / elapsed, error_count.load(Ordering::Relaxed));
    Ok(())
}

fn benchmark_worker(port: u16, host: String, keys: usize, requests: usize, expiration: usize,
                    types: [BenchmarkCommand; 4], error_count: Arc<AtomicUsize>, barrier: Arc<Barrier>) -> Result<(), Error> {
    let mut rng = rand::thread_rng();
    let keys4 = keys * 4;
    let ex = expiration.to_string();
    let mut commands = Vec::new();
    for _i in 0..requests {
        let n = rng.gen::<usize>() % keys4;
        let key = (n / 4).to_string();
        let data = match &types[n & 3] {
            BenchmarkCommand::Get => resp_encode_strings(&vec!["get".to_string(), key]),
            BenchmarkCommand::Set => resp_encode_strings(&vec!["set".to_string(), key.clone(), key]),
            BenchmarkCommand::Ping => "ping\r\n".to_string().into_bytes(),
            _ => resp_encode_strings(&vec!["set".to_string(), key.clone(), key, "px".to_string(), ex.clone()])
        };
        commands.push(data);
    }
    let mut buffer = [0; 1000];
    let mut error_counter = 0;
    let mut connection = TcpStream::connect(format!("{}:{}", host, port))?;
    barrier.wait();
    for command in commands {
        connection.write_all(&command)?;
        connection.read(&mut buffer)?;
        if buffer[0] == '-' as u8 {
            error_counter += 1;
        }
    }
    error_count.fetch_add(error_counter, Ordering::Relaxed);
    Ok(())
}
