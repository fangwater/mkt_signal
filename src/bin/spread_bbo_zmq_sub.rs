use anyhow::{Context, Result};
use clap::Parser;
use std::time::{Duration, Instant};

use mkt_signal::spread_pbs::latency::LatencyKll;
use mkt_signal::spread_pbs::publisher::{SpreadPublisher, SPREAD_PAYLOAD_BYTES};
use mkt_signal::spread_pbs::zmq_forward::{
    bbo_service_name, bbo_topic, decode_bbo_meta, is_latency_symbol, tcp_endpoint, WireHeader,
    DEFAULT_COLO_HOST, DEFAULT_ZMQ_HWM, DEFAULT_ZMQ_PORT, DEFAULT_ZMQ_SOCKET_BUFFER_BYTES,
};
use runtime_common::affinity::pin_to_core;
use runtime_common::time_util::get_timestamp_us;

const STATS_INTERVAL: Duration = Duration::from_secs(30);
const RECEIVE_TIMEOUT_MS: i32 = 1_000;
const STALE_WARN_AFTER: Duration = Duration::from_secs(3);
const STALE_WARN_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Parser)]
#[command(name = "spread_bbo_zmq_sub")]
#[command(about = "Republish lossy ZMQ BBO messages into a spread_pbs Iceoryx service")]
struct Args {
    #[arg(long, default_value = "binance-futures")]
    venue: String,

    #[arg(long, default_value = "spread_pbs")]
    service_root: String,

    #[arg(long, default_value = DEFAULT_COLO_HOST)]
    host: String,

    #[arg(long, default_value_t = DEFAULT_ZMQ_PORT)]
    port: u16,

    #[arg(long, default_value_t = DEFAULT_ZMQ_HWM)]
    rcvhwm: i32,

    #[arg(long)]
    core: Option<usize>,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    anyhow::ensure!(args.rcvhwm > 0, "--rcvhwm must be positive");
    if let Some(core) = args.core {
        pin_to_core(core)?;
    }

    let service_name = bbo_service_name(&args.service_root, &args.venue)?;
    let topic = bbo_topic(&args.venue)?;
    let endpoint = tcp_endpoint(&args.host, args.port)?;
    let publisher = SpreadPublisher::new_with_root(&args.venue, &args.service_root)
        .with_context(|| format!("create replacement BBO IPC publisher {service_name}"))?;

    let context = zmq::Context::new();
    let socket = context
        .socket(zmq::SUB)
        .context("create BBO ZMQ SUB socket")?;
    socket
        .set_rcvhwm(args.rcvhwm)
        .context("set BBO ZMQ SUB rcvhwm")?;
    socket
        .set_rcvbuf(DEFAULT_ZMQ_SOCKET_BUFFER_BYTES)
        .context("set BBO ZMQ SUB socket buffer")?;
    socket
        .set_rcvtimeo(RECEIVE_TIMEOUT_MS)
        .context("set BBO ZMQ SUB receive timeout")?;
    socket.set_linger(0).context("set BBO ZMQ SUB linger")?;
    socket
        .set_subscribe(topic.as_bytes())
        .with_context(|| format!("subscribe BBO ZMQ topic {topic}"))?;
    socket
        .connect(&endpoint)
        .with_context(|| format!("connect BBO ZMQ SUB to {endpoint}"))?;

    log::info!(
        "spread_bbo_zmq_sub ready endpoint={} topic={} service={} rcvhwm={} core={:?}",
        endpoint,
        topic,
        publisher.service_name(),
        args.rcvhwm,
        args.core
    );

    let mut active_session = None::<u64>;
    let mut last_sequence = 0_u64;
    let mut received_count = 0_u64;
    let mut published_count = 0_u64;
    let mut invalid_count = 0_u64;
    let mut sequence_gap_count = 0_u64;
    let mut duplicate_count = 0_u64;
    let mut session_count = 0_u64;
    let mut stats_started = Instant::now();
    let mut last_message_at = Instant::now();
    let mut last_stale_warning = None::<Instant>;
    let mut link_latency = LatencyKll::new("bbo-zmq-sub-colo-to-el01");
    let mut e2e_latency = LatencyKll::new("bbo-zmq-sub-exchange-to-el01");

    loop {
        let parts = match socket.recv_multipart(0) {
            Ok(parts) => parts,
            Err(zmq::Error::EINTR) => continue,
            Err(zmq::Error::EAGAIN) => {
                if last_message_at.elapsed() >= STALE_WARN_AFTER
                    && last_stale_warning.is_none_or(|last| last.elapsed() >= STALE_WARN_INTERVAL)
                {
                    log::warn!(
                        "spread_bbo_zmq_sub no BBO received for {}ms endpoint={}",
                        last_message_at.elapsed().as_millis(),
                        endpoint
                    );
                    last_stale_warning = Some(Instant::now());
                }
                continue;
            }
            Err(err) => {
                log::warn!("BBO ZMQ SUB receive error: {err}");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        last_message_at = Instant::now();
        last_stale_warning = None;
        received_count = received_count.saturating_add(1);

        let (header, payload) = match decode_message(&parts, &topic) {
            Ok(message) => message,
            Err(err) => {
                invalid_count = invalid_count.saturating_add(1);
                log::warn!("drop invalid BBO ZMQ message: {err:#}");
                continue;
            }
        };
        let meta = match decode_bbo_meta(payload) {
            Ok(meta) => meta,
            Err(err) => {
                invalid_count = invalid_count.saturating_add(1);
                log::warn!("drop invalid BBO payload: {err:#}");
                continue;
            }
        };

        if active_session != Some(header.session_id) {
            active_session = Some(header.session_id);
            last_sequence = header.sequence;
            session_count = session_count.saturating_add(1);
            log::info!(
                "spread_bbo_zmq_sub sender session={} first_sequence={}",
                header.session_id,
                header.sequence
            );
        } else if header.sequence <= last_sequence {
            duplicate_count = duplicate_count.saturating_add(1);
            continue;
        } else {
            sequence_gap_count = sequence_gap_count.saturating_add(
                header
                    .sequence
                    .saturating_sub(last_sequence)
                    .saturating_sub(1),
            );
            last_sequence = header.sequence;
        }

        let recv_ts_us = get_timestamp_us();
        if is_latency_symbol(meta.symbol) {
            link_latency.push((recv_ts_us - header.sent_ts_us) as f64);
            if meta.event_ts_us > 0 {
                e2e_latency.push((recv_ts_us - meta.event_ts_us) as f64);
            }
        }

        if let Err(err) = publisher.publish(payload) {
            log::warn!("publish replacement BBO IPC payload failed: {err:#}");
            continue;
        }
        published_count = published_count.saturating_add(1);

        if stats_started.elapsed() >= STATS_INTERVAL {
            log::info!(
                "spread_bbo_zmq_sub stats_30s received={} published={} invalid={} sequence_gaps={} duplicates={} sender_sessions={} last_sequence={}",
                received_count,
                published_count,
                invalid_count,
                sequence_gap_count,
                duplicate_count,
                session_count,
                last_sequence
            );
            received_count = 0;
            published_count = 0;
            invalid_count = 0;
            sequence_gap_count = 0;
            duplicate_count = 0;
            session_count = 0;
            stats_started = Instant::now();
        }
    }
}

fn decode_message<'a>(
    parts: &'a [Vec<u8>],
    expected_topic: &str,
) -> Result<(WireHeader, &'a [u8])> {
    anyhow::ensure!(
        parts.len() == 3,
        "invalid multipart frame count: got={} expected=3",
        parts.len()
    );
    anyhow::ensure!(
        parts[0].as_slice() == expected_topic.as_bytes(),
        "unexpected BBO ZMQ topic"
    );
    let header = WireHeader::decode(&parts[1])?;
    anyhow::ensure!(
        parts[2].len() == SPREAD_PAYLOAD_BYTES,
        "invalid BBO ZMQ payload frame length: got={} expected={}",
        parts[2].len(),
        SPREAD_PAYLOAD_BYTES
    );
    Ok((header, &parts[2]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::AskBidSpreadMsg;

    #[test]
    fn decodes_expected_multipart_message() {
        let topic = bbo_topic("binance-futures").unwrap();
        let header = WireHeader {
            session_id: 7,
            sequence: 9,
            sent_ts_us: 11,
        };
        let msg = AskBidSpreadMsg::create("BTCUSDT".to_string(), 10, 1.0, 2.0, 1.1, 3.0);
        let bytes = msg.to_bytes();
        let mut payload = vec![0_u8; SPREAD_PAYLOAD_BYTES];
        payload[..bytes.len()].copy_from_slice(&bytes);
        let parts = vec![topic.as_bytes().to_vec(), header.encode().to_vec(), payload];

        let (decoded_header, decoded_payload) = decode_message(&parts, &topic).unwrap();
        assert_eq!(decoded_header, header);
        assert_eq!(decoded_payload.len(), SPREAD_PAYLOAD_BYTES);
    }

    #[test]
    fn rejects_wrong_topic_or_frame_count() {
        let topic = bbo_topic("binance-futures").unwrap();
        assert!(decode_message(&[], &topic).is_err());

        let parts = vec![
            b"wrong".to_vec(),
            WireHeader {
                session_id: 1,
                sequence: 1,
                sent_ts_us: 1,
            }
            .encode()
            .to_vec(),
            vec![0_u8; SPREAD_PAYLOAD_BYTES],
        ];
        assert!(decode_message(&parts, &topic).is_err());
    }
}
