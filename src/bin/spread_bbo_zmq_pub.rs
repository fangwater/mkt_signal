use anyhow::{Context, Result};
use clap::Parser;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use std::time::{Duration, Instant};

use mkt_signal::spread_pbs::latency::LatencyKll;
use mkt_signal::spread_pbs::publisher::{DERIVATIVES_PAYLOAD_BYTES, SPREAD_PAYLOAD_BYTES};
use mkt_signal::spread_pbs::zmq_forward::{
    bbo_service_name, bbo_topic, decode_bbo_meta, derivatives_service_name, derivatives_topic,
    is_latency_symbol, tcp_endpoint, WireHeader, WirePayloadKind, DEFAULT_ZMQ_HWM,
    DEFAULT_ZMQ_PORT, DEFAULT_ZMQ_SOCKET_BUFFER_BYTES,
};
use runtime_common::affinity::pin_to_core;
use runtime_common::time_util::get_timestamp_us;

const BBO_SERVICE_HISTORY_SIZE: usize = 100;
const DERIVATIVES_SERVICE_HISTORY_SIZE: usize = 50;
const SERVICE_MAX_SUBSCRIBERS: usize = 64;
const SERVICE_SUBSCRIBER_MAX_BUFFER: usize = 8192;
const BBO_LOCAL_SUBSCRIBER_BUFFER: usize = 128;
const DERIVATIVES_LOCAL_SUBSCRIBER_BUFFER: usize = 8192;
const STATS_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Parser)]
#[command(name = "spread_bbo_zmq_pub")]
#[command(about = "Forward spread_pbs BBO and derivatives IPC messages over one lossy ZMQ PUB")]
struct Args {
    #[arg(long, default_value = "binance-futures")]
    venue: String,

    #[arg(long, default_value = "spread_pbs")]
    service_root: String,

    #[arg(long, default_value = "dat_pbs")]
    derivatives_service_root: String,

    #[arg(long, default_value = "0.0.0.0")]
    bind_ip: String,

    #[arg(long, default_value_t = DEFAULT_ZMQ_PORT)]
    port: u16,

    #[arg(long, default_value_t = DEFAULT_ZMQ_HWM)]
    sndhwm: i32,

    #[arg(long)]
    core: Option<usize>,
}

#[derive(Debug, Default)]
struct ForwardStats {
    sequence: u64,
    input: u64,
    sent: u64,
    dropped: u64,
    invalid: u64,
}

impl ForwardStats {
    fn reset_interval(&mut self) {
        self.input = 0;
        self.sent = 0;
        self.dropped = 0;
        self.invalid = 0;
    }
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    anyhow::ensure!(args.sndhwm > 0, "--sndhwm must be positive");
    if let Some(core) = args.core {
        pin_to_core(core)?;
    }

    let bbo_service = bbo_service_name(&args.service_root, &args.venue)?;
    let bbo_topic = bbo_topic(&args.venue)?;
    let derivatives_service =
        derivatives_service_name(&args.derivatives_service_root, &args.venue)?;
    let derivatives_topic = derivatives_topic(&args.venue)?;
    let endpoint = tcp_endpoint(&args.bind_ip, args.port)?;

    let node_name = format!("spread_bbo_zmq_pub_{}", std::process::id());
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;
    let bbo_subscriber = open_subscriber::<SPREAD_PAYLOAD_BYTES>(
        &node,
        &bbo_service,
        BBO_SERVICE_HISTORY_SIZE,
        BBO_LOCAL_SUBSCRIBER_BUFFER,
        "BBO",
    )?;
    let derivatives_subscriber = open_subscriber::<DERIVATIVES_PAYLOAD_BYTES>(
        &node,
        &derivatives_service,
        DERIVATIVES_SERVICE_HISTORY_SIZE,
        DERIVATIVES_LOCAL_SUBSCRIBER_BUFFER,
        "derivatives",
    )?;

    let context = zmq::Context::new();
    let socket = context
        .socket(zmq::PUB)
        .context("create market-data ZMQ PUB socket")?;
    socket
        .set_sndhwm(args.sndhwm)
        .context("set market-data ZMQ PUB sndhwm")?;
    socket
        .set_sndbuf(DEFAULT_ZMQ_SOCKET_BUFFER_BYTES)
        .context("set market-data ZMQ PUB socket buffer")?;
    socket
        .set_linger(0)
        .context("set market-data ZMQ PUB linger")?;
    socket
        .bind(&endpoint)
        .with_context(|| format!("bind market-data ZMQ PUB on {endpoint}"))?;

    let session_id = get_timestamp_us().max(1) as u64;
    log::info!(
        "spread_bbo_zmq_pub ready bbo_service={} bbo_topic={} derivatives_service={} derivatives_topic={} endpoint={} session_id={} sndhwm={} core={:?}",
        bbo_service,
        bbo_topic,
        derivatives_service,
        derivatives_topic,
        endpoint,
        session_id,
        args.sndhwm,
        args.core
    );

    let mut bbo_stats = ForwardStats::default();
    let mut derivatives_stats = ForwardStats::default();
    let mut stats_started = Instant::now();
    let mut ingress_latency = LatencyKll::new("bbo-zmq-pub-colo-ingress");

    loop {
        let mut received_any = false;
        let mut receive_error = false;

        match bbo_subscriber.receive() {
            Ok(Some(sample)) => {
                received_any = true;
                bbo_stats.input = bbo_stats.input.saturating_add(1);
                let payload = sample.payload();
                match decode_bbo_meta(payload) {
                    Ok(meta) => {
                        let sent_ts_us = get_timestamp_us();
                        if meta.event_ts_us > 0 && is_latency_symbol(meta.symbol) {
                            ingress_latency.push((sent_ts_us - meta.event_ts_us) as f64);
                        }
                        forward_payload(
                            &socket,
                            &bbo_topic,
                            WirePayloadKind::Bbo,
                            payload,
                            session_id,
                            sent_ts_us,
                            "BBO",
                            &mut bbo_stats,
                        );
                    }
                    Err(err) => {
                        bbo_stats.invalid = bbo_stats.invalid.saturating_add(1);
                        log::warn!("drop invalid BBO IPC payload: {err:#}");
                    }
                }
            }
            Ok(None) => {}
            Err(err) => {
                receive_error = true;
                log::warn!("BBO Iceoryx receive error: {err}");
            }
        }

        match derivatives_subscriber.receive() {
            Ok(Some(sample)) => {
                received_any = true;
                derivatives_stats.input = derivatives_stats.input.saturating_add(1);
                forward_payload(
                    &socket,
                    &derivatives_topic,
                    WirePayloadKind::Derivatives,
                    sample.payload(),
                    session_id,
                    get_timestamp_us(),
                    "derivatives",
                    &mut derivatives_stats,
                );
            }
            Ok(None) => {}
            Err(err) => {
                receive_error = true;
                log::warn!("derivatives Iceoryx receive error: {err}");
            }
        }

        if receive_error {
            std::thread::sleep(Duration::from_millis(100));
        } else if !received_any {
            std::thread::yield_now();
        }

        if stats_started.elapsed() >= STATS_INTERVAL {
            log::info!(
                "spread_bbo_zmq_pub stats_30s bbo_input={} bbo_sent={} bbo_dropped={} bbo_invalid={} bbo_last_sequence={} derivatives_input={} derivatives_sent={} derivatives_dropped={} derivatives_invalid={} derivatives_last_sequence={}",
                bbo_stats.input,
                bbo_stats.sent,
                bbo_stats.dropped,
                bbo_stats.invalid,
                bbo_stats.sequence,
                derivatives_stats.input,
                derivatives_stats.sent,
                derivatives_stats.dropped,
                derivatives_stats.invalid,
                derivatives_stats.sequence
            );
            bbo_stats.reset_interval();
            derivatives_stats.reset_interval();
            stats_started = Instant::now();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn forward_payload(
    socket: &zmq::Socket,
    topic: &str,
    kind: WirePayloadKind,
    payload: &[u8],
    session_id: u64,
    sent_ts_us: i64,
    label: &str,
    stats: &mut ForwardStats,
) {
    stats.sequence = stats.sequence.wrapping_add(1).max(1);
    let header = WireHeader {
        session_id,
        sequence: stats.sequence,
        sent_ts_us,
    }
    .encode_for(kind);

    match socket.send_multipart(
        [topic.as_bytes(), header.as_slice(), payload],
        zmq::DONTWAIT,
    ) {
        Ok(()) => stats.sent = stats.sent.saturating_add(1),
        Err(zmq::Error::EAGAIN) => {
            stats.dropped = stats.dropped.saturating_add(1);
        }
        Err(err) => {
            stats.dropped = stats.dropped.saturating_add(1);
            log::warn!("{label} ZMQ PUB send error: {err}");
        }
    }
}

fn open_subscriber<const PAYLOAD_BYTES: usize>(
    node: &Node<ipc::Service>,
    service_name: &str,
    history_size: usize,
    local_buffer_size: usize,
    label: &str,
) -> Result<Subscriber<ipc::Service, [u8; PAYLOAD_BYTES], ()>> {
    let service = node
        .service_builder(&ServiceName::new(service_name)?)
        .publish_subscribe::<[u8; PAYLOAD_BYTES]>()
        .max_publishers(1)
        .max_subscribers(SERVICE_MAX_SUBSCRIBERS)
        .history_size(history_size)
        .subscriber_max_buffer_size(SERVICE_SUBSCRIBER_MAX_BUFFER)
        .open()
        .with_context(|| {
            format!("open {label} IPC service {service_name}; spread_pbs must be running first")
        })?;
    service
        .subscriber_builder()
        .buffer_size(local_buffer_size)
        .create()
        .with_context(|| format!("create {label} IPC subscriber for {service_name}"))
}
