//! Demo publisher: sends numbered messages to an Iceoryx2 service.
//! Used to test ipc_bridge forwarding.
//!
//! Usage:
//!   cargo run --bin demo_bridge_pub -- --service "order_reqs/binance" --count 10 --interval-ms 500

use anyhow::Result;
use clap::Parser;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use std::thread;
use std::time::Duration;

const PAYLOAD_SIZE: usize = 4096;

#[derive(Parser, Debug)]
#[command(name = "demo_bridge_pub")]
struct Args {
    /// Iceoryx service name (e.g. "order_reqs/binance")
    #[arg(long, default_value = "order_reqs/binance")]
    service: String,

    /// Number of messages to send
    #[arg(long, default_value_t = 20)]
    count: u32,

    /// Interval between messages in milliseconds
    #[arg(long, default_value_t = 500)]
    interval_ms: u64,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    let node = NodeBuilder::new()
        .name(&NodeName::new("demo_bridge_pub")?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&args.service)?)
        .publish_subscribe::<[u8; PAYLOAD_SIZE]>()
        .open_or_create()?;

    let publisher = service.publisher_builder().create()?;

    println!(
        "Publishing {} messages to '{}' (interval={}ms)",
        args.count, args.service, args.interval_ms
    );

    for i in 1..=args.count {
        let msg = format!("bridge_test_msg_{:04}", i);
        let mut buf = [0u8; PAYLOAD_SIZE];
        buf[..msg.len()].copy_from_slice(msg.as_bytes());

        let sample = publisher.loan_uninit()?;
        let sample = sample.write_payload(buf);
        sample.send()?;

        println!("[PUB] sent #{}: {}", i, msg);
        thread::sleep(Duration::from_millis(args.interval_ms));
    }

    println!("[PUB] done");
    Ok(())
}
