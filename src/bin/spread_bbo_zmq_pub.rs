use anyhow::{Context, Result};
use clap::Parser;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use std::time::{Duration, Instant};

use mkt_signal::spread_pbs::latency::LatencyKll;
use mkt_signal::spread_pbs::publisher::SPREAD_PAYLOAD_BYTES;
use mkt_signal::spread_pbs::zmq_forward::{
    bbo_service_name, bbo_topic, decode_bbo_meta, is_latency_symbol, tcp_endpoint, WireHeader,
    DEFAULT_ZMQ_HWM, DEFAULT_ZMQ_PORT, DEFAULT_ZMQ_SOCKET_BUFFER_BYTES,
};
use runtime_common::affinity::pin_to_core;
use runtime_common::time_util::get_timestamp_us;

const SERVICE_HISTORY_SIZE: usize = 100;
const SERVICE_MAX_SUBSCRIBERS: usize = 64;
const SERVICE_SUBSCRIBER_MAX_BUFFER: usize = 8192;
const LOCAL_SUBSCRIBER_BUFFER: usize = 128;
const STATS_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Parser)]
#[command(name = "spread_bbo_zmq_pub")]
#[command(about = "Forward spread_pbs BBO IPC messages through a lossy ZMQ PUB socket")]
struct Args {
    #[arg(long, default_value = "binance-futures")]
    venue: String,

    #[arg(long, default_value = "spread_pbs")]
    service_root: String,

    #[arg(long, default_value = "0.0.0.0")]
    bind_ip: String,

    #[arg(long, default_value_t = DEFAULT_ZMQ_PORT)]
    port: u16,

    #[arg(long, default_value_t = DEFAULT_ZMQ_HWM)]
    sndhwm: i32,

    #[arg(long)]
    core: Option<usize>,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    anyhow::ensure!(args.sndhwm > 0, "--sndhwm must be positive");
    if let Some(core) = args.core {
        pin_to_core(core)?;
    }

    let service_name = bbo_service_name(&args.service_root, &args.venue)?;
    let topic = bbo_topic(&args.venue)?;
    let endpoint = tcp_endpoint(&args.bind_ip, args.port)?;
    let (_node, subscriber) = open_subscriber(&service_name)?;

    let context = zmq::Context::new();
    let socket = context
        .socket(zmq::PUB)
        .context("create BBO ZMQ PUB socket")?;
    socket
        .set_sndhwm(args.sndhwm)
        .context("set BBO ZMQ PUB sndhwm")?;
    socket
        .set_sndbuf(DEFAULT_ZMQ_SOCKET_BUFFER_BYTES)
        .context("set BBO ZMQ PUB socket buffer")?;
    socket.set_linger(0).context("set BBO ZMQ PUB linger")?;
    socket
        .bind(&endpoint)
        .with_context(|| format!("bind BBO ZMQ PUB on {endpoint}"))?;

    let session_id = get_timestamp_us().max(1) as u64;
    log::info!(
        "spread_bbo_zmq_pub ready service={} endpoint={} topic={} session_id={} sndhwm={} core={:?}",
        service_name,
        endpoint,
        topic,
        session_id,
        args.sndhwm,
        args.core
    );

    let mut sequence = 0_u64;
    let mut input_count = 0_u64;
    let mut sent_count = 0_u64;
    let mut dropped_count = 0_u64;
    let mut invalid_count = 0_u64;
    let mut stats_started = Instant::now();
    let mut ingress_latency = LatencyKll::new("bbo-zmq-pub-colo-ingress");

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                input_count = input_count.saturating_add(1);
                let payload = sample.payload();
                let meta = match decode_bbo_meta(payload) {
                    Ok(meta) => meta,
                    Err(err) => {
                        invalid_count = invalid_count.saturating_add(1);
                        log::warn!("drop invalid BBO IPC payload: {err:#}");
                        continue;
                    }
                };

                let sent_ts_us = get_timestamp_us();
                if meta.event_ts_us > 0 && is_latency_symbol(meta.symbol) {
                    ingress_latency.push((sent_ts_us - meta.event_ts_us) as f64);
                }
                sequence = sequence.wrapping_add(1).max(1);
                let header = WireHeader {
                    session_id,
                    sequence,
                    sent_ts_us,
                }
                .encode();

                match socket.send_multipart(
                    [topic.as_bytes(), header.as_slice(), payload.as_slice()],
                    zmq::DONTWAIT,
                ) {
                    Ok(()) => sent_count = sent_count.saturating_add(1),
                    Err(zmq::Error::EAGAIN) => {
                        dropped_count = dropped_count.saturating_add(1);
                    }
                    Err(err) => {
                        dropped_count = dropped_count.saturating_add(1);
                        log::warn!("BBO ZMQ PUB send error: {err}");
                    }
                }
            }
            Ok(None) => std::thread::yield_now(),
            Err(err) => {
                log::warn!("BBO Iceoryx receive error: {err}");
                std::thread::sleep(Duration::from_millis(100));
            }
        }

        if stats_started.elapsed() >= STATS_INTERVAL {
            log::info!(
                "spread_bbo_zmq_pub stats_30s input={} sent={} dropped={} invalid={} last_sequence={}",
                input_count,
                sent_count,
                dropped_count,
                invalid_count,
                sequence
            );
            input_count = 0;
            sent_count = 0;
            dropped_count = 0;
            invalid_count = 0;
            stats_started = Instant::now();
        }
    }
}

fn open_subscriber(
    service_name: &str,
) -> Result<(
    Node<ipc::Service>,
    Subscriber<ipc::Service, [u8; SPREAD_PAYLOAD_BYTES], ()>,
)> {
    let node_name = format!("spread_bbo_zmq_pub_{}", std::process::id());
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;
    let service = node
        .service_builder(&ServiceName::new(service_name)?)
        .publish_subscribe::<[u8; SPREAD_PAYLOAD_BYTES]>()
        .max_publishers(1)
        .max_subscribers(SERVICE_MAX_SUBSCRIBERS)
        .history_size(SERVICE_HISTORY_SIZE)
        .subscriber_max_buffer_size(SERVICE_SUBSCRIBER_MAX_BUFFER)
        .open()
        .with_context(|| {
            format!("open BBO IPC service {service_name}; spread_pbs must be running first")
        })?;
    let subscriber = service
        .subscriber_builder()
        .buffer_size(LOCAL_SUBSCRIBER_BUFFER)
        .create()
        .with_context(|| format!("create BBO IPC subscriber for {service_name}"))?;
    Ok((node, subscriber))
}
