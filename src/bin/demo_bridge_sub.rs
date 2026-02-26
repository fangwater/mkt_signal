//! Demo subscriber: polls an Iceoryx2 service and prints received messages.
//! Used to test ipc_bridge forwarding.
//!
//! Usage:
//!   cargo run --bin demo_bridge_sub -- --service "order_reqs/binance" --timeout-s 30

use anyhow::Result;
use clap::Parser;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use std::thread;
use std::time::{Duration, Instant};

const PAYLOAD_SIZE: usize = 4096;

#[derive(Parser, Debug)]
#[command(name = "demo_bridge_sub")]
struct Args {
    /// Iceoryx service name (e.g. "order_reqs/binance")
    #[arg(long, default_value = "order_reqs/binance")]
    service: String,

    /// Timeout in seconds (0 = run forever until Ctrl-C)
    #[arg(long, default_value_t = 30)]
    timeout_s: u64,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    let node = NodeBuilder::new()
        .name(&NodeName::new("demo_bridge_sub")?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&args.service)?)
        .publish_subscribe::<[u8; PAYLOAD_SIZE]>()
        .open_or_create()?;

    let subscriber = service.subscriber_builder().create()?;

    println!(
        "Subscribing to '{}' (timeout={}s)",
        args.service, args.timeout_s
    );

    let start = Instant::now();
    let timeout = if args.timeout_s == 0 {
        Duration::from_secs(u64::MAX)
    } else {
        Duration::from_secs(args.timeout_s)
    };

    let mut count = 0u64;
    loop {
        if start.elapsed() >= timeout {
            break;
        }

        match subscriber.receive()? {
            Some(sample) => {
                let payload = sample.payload();
                // Find the end of the actual message (first 0 byte or end)
                let end = payload.iter().position(|&b| b == 0).unwrap_or(PAYLOAD_SIZE);
                let msg = String::from_utf8_lossy(&payload[..end]);
                count += 1;
                println!("[SUB] #{}: {}", count, msg);
            }
            None => {
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    println!("[SUB] done, received {} messages", count);
    Ok(())
}
