use anyhow::Result;
use clap::Parser;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_signal::common::mkt_msg::FeatureMsg;
use mkt_signal::factor_pub::fusion_factor_pub::publisher::FUSION_FACTOR_PAYLOAD_MAX_BYTES;
use std::thread;
use std::time::{Duration, Instant};

const INPUT_SUBSCRIBER_MAX_BUFFER_SIZE: usize = 8192;
const INPUT_HISTORY_SIZE: usize = 128;
const INPUT_MAX_PUBLISHERS: usize = 1;
const INPUT_MAX_SUBSCRIBERS: usize = 10;
const DEFAULT_POLL_MS: u64 = 20;

#[derive(Parser, Debug)]
#[command(name = "inspect_model_input")]
#[command(
    about = "Subscribe to a fusion_factor Iceoryx service and print decoded FeatureMsg values"
)]
struct Args {
    /// Iceoryx service name, e.g. fusion_factor/binance-futures
    #[arg(long, default_value = "fusion_factor/binance-futures")]
    service: String,

    /// Optional symbol filter, e.g. BTCUSDT
    #[arg(long)]
    symbol: Option<String>,

    /// Exit after receiving this many matching messages (0 = unlimited until timeout)
    #[arg(long, default_value_t = 0)]
    limit: u64,

    /// Timeout in seconds (0 = run forever until Ctrl-C)
    #[arg(long, default_value_t = 30)]
    timeout_s: u64,

    /// Poll interval in milliseconds
    #[arg(long, default_value_t = DEFAULT_POLL_MS)]
    poll_ms: u64,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let symbol_filter = args.symbol.as_ref().map(|s| s.trim().to_uppercase());

    let node = NodeBuilder::new()
        .name(&NodeName::new("inspect_model_input")?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&args.service)?)
        .publish_subscribe::<[u8; FUSION_FACTOR_PAYLOAD_MAX_BYTES]>()
        .max_publishers(INPUT_MAX_PUBLISHERS)
        .max_subscribers(INPUT_MAX_SUBSCRIBERS)
        .subscriber_max_buffer_size(INPUT_SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(INPUT_HISTORY_SIZE)
        .open()?;

    let subscriber = service
        .subscriber_builder()
        .buffer_size(INPUT_SUBSCRIBER_MAX_BUFFER_SIZE)
        .create()?;

    println!(
        "Subscribing to '{}' symbol_filter={:?} timeout={}s limit={}",
        args.service, symbol_filter, args.timeout_s, args.limit
    );

    let start = Instant::now();
    let timeout = if args.timeout_s == 0 {
        Duration::from_secs(u64::MAX)
    } else {
        Duration::from_secs(args.timeout_s)
    };

    let mut received = 0u64;
    loop {
        if start.elapsed() >= timeout {
            break;
        }
        if args.limit > 0 && received >= args.limit {
            break;
        }

        match subscriber.receive()? {
            Some(sample) => match FeatureMsg::from_bytes(sample.payload()) {
                Ok(msg) => {
                    if let Some(filter) = &symbol_filter {
                        if msg.symbol.to_uppercase() != *filter {
                            continue;
                        }
                    }
                    received += 1;
                    println!(
                        "[FEATURE] #{} symbol={} status={} feature_dim={} ts_ms={}",
                        received,
                        msg.symbol,
                        msg.status,
                        msg.features.len(),
                        msg.ts_ms
                    );
                }
                Err(err) => {
                    println!("[FEATURE] decode_error err={}", err);
                }
            },
            None => {
                thread::sleep(Duration::from_millis(args.poll_ms));
            }
        }
    }

    println!("[FEATURE] done, received {} matching messages", received);
    Ok(())
}
