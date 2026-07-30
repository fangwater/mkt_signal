use anyhow::{Context, Result};
use clap::Parser;
use std::time::{Duration, Instant};

use mkt_signal::spread_pbs::latency::LatencyKll;
use mkt_signal::spread_pbs::publisher::{SpreadDerivativesPublisher, SpreadPublisher};
use mkt_signal::spread_pbs::zmq_forward::{
    bbo_service_name, bbo_topic, decode_bbo_meta, derivatives_service_name, derivatives_topic,
    is_latency_symbol, tcp_endpoint, WireHeader, WirePayloadKind, DEFAULT_COLO_HOST,
    DEFAULT_ZMQ_HWM, DEFAULT_ZMQ_PORT, DEFAULT_ZMQ_SOCKET_BUFFER_BYTES,
};
use runtime_common::affinity::pin_to_core;
use runtime_common::time_util::get_timestamp_us;

const STATS_INTERVAL: Duration = Duration::from_secs(30);
const RECEIVE_TIMEOUT_MS: i32 = 1_000;
const STALE_WARN_AFTER: Duration = Duration::from_secs(3);
const STALE_WARN_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Parser)]
#[command(name = "spread_bbo_zmq_sub")]
#[command(about = "Republish lossy ZMQ BBO and derivatives messages into Iceoryx services")]
struct Args {
    #[arg(long, default_value = "binance-futures")]
    venue: String,

    #[arg(long, default_value = "spread_pbs")]
    service_root: String,

    #[arg(long, default_value = "dat_pbs")]
    derivatives_service_root: String,

    #[arg(long, default_value = DEFAULT_COLO_HOST)]
    host: String,

    #[arg(long, default_value_t = DEFAULT_ZMQ_PORT)]
    port: u16,

    #[arg(long, default_value_t = DEFAULT_ZMQ_HWM)]
    rcvhwm: i32,

    #[arg(long)]
    core: Option<usize>,
}

#[derive(Debug, Default)]
struct ReceiveStats {
    active_session: Option<u64>,
    last_sequence: u64,
    received: u64,
    published: u64,
    invalid: u64,
    sequence_gaps: u64,
    duplicates: u64,
    sender_sessions: u64,
}

impl ReceiveStats {
    fn accept_header(&mut self, label: &str, header: WireHeader) -> bool {
        if self.active_session != Some(header.session_id) {
            self.active_session = Some(header.session_id);
            self.last_sequence = header.sequence;
            self.sender_sessions = self.sender_sessions.saturating_add(1);
            log::info!(
                "spread_bbo_zmq_sub {label} sender session={} first_sequence={}",
                header.session_id,
                header.sequence
            );
            return true;
        }
        if header.sequence <= self.last_sequence {
            self.duplicates = self.duplicates.saturating_add(1);
            return false;
        }
        self.sequence_gaps = self.sequence_gaps.saturating_add(
            header
                .sequence
                .saturating_sub(self.last_sequence)
                .saturating_sub(1),
        );
        self.last_sequence = header.sequence;
        true
    }

    fn reset_interval(&mut self) {
        self.received = 0;
        self.published = 0;
        self.invalid = 0;
        self.sequence_gaps = 0;
        self.duplicates = 0;
        self.sender_sessions = 0;
    }
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    anyhow::ensure!(args.rcvhwm > 0, "--rcvhwm must be positive");
    if let Some(core) = args.core {
        pin_to_core(core)?;
    }

    let bbo_service = bbo_service_name(&args.service_root, &args.venue)?;
    let bbo_topic = bbo_topic(&args.venue)?;
    let derivatives_service =
        derivatives_service_name(&args.derivatives_service_root, &args.venue)?;
    let derivatives_topic = derivatives_topic(&args.venue)?;
    let endpoint = tcp_endpoint(&args.host, args.port)?;
    let bbo_publisher = SpreadPublisher::new_with_root(&args.venue, &args.service_root)
        .with_context(|| format!("create replacement BBO IPC publisher {bbo_service}"))?;
    let derivatives_publisher = SpreadDerivativesPublisher::new_open_or_create_with_root(
        &args.venue,
        &args.derivatives_service_root,
    )
    .with_context(|| {
        format!("create replacement derivatives IPC publisher {derivatives_service}")
    })?;

    let context = zmq::Context::new();
    let socket = context
        .socket(zmq::SUB)
        .context("create market-data ZMQ SUB socket")?;
    socket
        .set_rcvhwm(args.rcvhwm)
        .context("set market-data ZMQ SUB rcvhwm")?;
    socket
        .set_rcvbuf(DEFAULT_ZMQ_SOCKET_BUFFER_BYTES)
        .context("set market-data ZMQ SUB socket buffer")?;
    socket
        .set_rcvtimeo(RECEIVE_TIMEOUT_MS)
        .context("set market-data ZMQ SUB receive timeout")?;
    socket
        .set_linger(0)
        .context("set market-data ZMQ SUB linger")?;
    socket
        .set_subscribe(bbo_topic.as_bytes())
        .with_context(|| format!("subscribe BBO ZMQ topic {bbo_topic}"))?;
    socket
        .set_subscribe(derivatives_topic.as_bytes())
        .with_context(|| format!("subscribe derivatives ZMQ topic {derivatives_topic}"))?;
    socket
        .connect(&endpoint)
        .with_context(|| format!("connect market-data ZMQ SUB to {endpoint}"))?;

    log::info!(
        "spread_bbo_zmq_sub ready endpoint={} bbo_topic={} bbo_service={} derivatives_topic={} derivatives_service={} rcvhwm={} core={:?}",
        endpoint,
        bbo_topic,
        bbo_publisher.service_name(),
        derivatives_topic,
        derivatives_publisher.service_name(),
        args.rcvhwm,
        args.core
    );

    let mut bbo_stats = ReceiveStats::default();
    let mut derivatives_stats = ReceiveStats::default();
    let mut unknown_invalid = 0_u64;
    let mut stats_started = Instant::now();
    let mut last_bbo_at = Instant::now();
    let mut last_derivatives_at = Instant::now();
    let mut last_bbo_stale_warning = None::<Instant>;
    let mut last_derivatives_stale_warning = None::<Instant>;
    let mut link_latency = LatencyKll::new("bbo-zmq-sub-colo-to-el01");
    let mut e2e_latency = LatencyKll::new("bbo-zmq-sub-exchange-to-el01");

    loop {
        let parts = match socket.recv_multipart(0) {
            Ok(parts) => parts,
            Err(zmq::Error::EINTR) => continue,
            Err(zmq::Error::EAGAIN) => {
                warn_if_stale("BBO", last_bbo_at, &mut last_bbo_stale_warning, &endpoint);
                warn_if_stale(
                    "derivatives",
                    last_derivatives_at,
                    &mut last_derivatives_stale_warning,
                    &endpoint,
                );
                continue;
            }
            Err(err) => {
                log::warn!("market-data ZMQ SUB receive error: {err}");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };

        let (kind, header, payload) = match decode_message(&parts, &bbo_topic, &derivatives_topic) {
            Ok(message) => message,
            Err(err) => {
                unknown_invalid = unknown_invalid.saturating_add(1);
                log::warn!("drop invalid market-data ZMQ message: {err:#}");
                continue;
            }
        };

        match kind {
            WirePayloadKind::Bbo => {
                last_bbo_at = Instant::now();
                last_bbo_stale_warning = None;
                bbo_stats.received = bbo_stats.received.saturating_add(1);
                let meta = match decode_bbo_meta(payload) {
                    Ok(meta) => meta,
                    Err(err) => {
                        bbo_stats.invalid = bbo_stats.invalid.saturating_add(1);
                        log::warn!("drop invalid BBO payload: {err:#}");
                        continue;
                    }
                };
                if !bbo_stats.accept_header("BBO", header) {
                    continue;
                }

                let recv_ts_us = get_timestamp_us();
                if is_latency_symbol(meta.symbol) {
                    link_latency.push((recv_ts_us - header.sent_ts_us) as f64);
                    if meta.event_ts_us > 0 {
                        e2e_latency.push((recv_ts_us - meta.event_ts_us) as f64);
                    }
                }

                if let Err(err) = bbo_publisher.publish(payload) {
                    log::warn!("publish replacement BBO IPC payload failed: {err:#}");
                    continue;
                }
                bbo_stats.published = bbo_stats.published.saturating_add(1);
            }
            WirePayloadKind::Derivatives => {
                last_derivatives_at = Instant::now();
                last_derivatives_stale_warning = None;
                derivatives_stats.received = derivatives_stats.received.saturating_add(1);
                if !derivatives_stats.accept_header("derivatives", header) {
                    continue;
                }
                if let Err(err) = derivatives_publisher.publish(payload) {
                    log::warn!("publish replacement derivatives IPC payload failed: {err:#}");
                    continue;
                }
                derivatives_stats.published = derivatives_stats.published.saturating_add(1);
            }
        }

        warn_if_stale("BBO", last_bbo_at, &mut last_bbo_stale_warning, &endpoint);
        warn_if_stale(
            "derivatives",
            last_derivatives_at,
            &mut last_derivatives_stale_warning,
            &endpoint,
        );

        if stats_started.elapsed() >= STATS_INTERVAL {
            log::info!(
                "spread_bbo_zmq_sub stats_30s bbo_received={} bbo_published={} bbo_invalid={} bbo_sequence_gaps={} bbo_duplicates={} bbo_sender_sessions={} bbo_last_sequence={} derivatives_received={} derivatives_published={} derivatives_invalid={} derivatives_sequence_gaps={} derivatives_duplicates={} derivatives_sender_sessions={} derivatives_last_sequence={} unknown_invalid={}",
                bbo_stats.received,
                bbo_stats.published,
                bbo_stats.invalid,
                bbo_stats.sequence_gaps,
                bbo_stats.duplicates,
                bbo_stats.sender_sessions,
                bbo_stats.last_sequence,
                derivatives_stats.received,
                derivatives_stats.published,
                derivatives_stats.invalid,
                derivatives_stats.sequence_gaps,
                derivatives_stats.duplicates,
                derivatives_stats.sender_sessions,
                derivatives_stats.last_sequence,
                unknown_invalid
            );
            bbo_stats.reset_interval();
            derivatives_stats.reset_interval();
            unknown_invalid = 0;
            stats_started = Instant::now();
        }
    }
}

fn warn_if_stale(
    label: &str,
    last_message_at: Instant,
    last_warning: &mut Option<Instant>,
    endpoint: &str,
) {
    if last_message_at.elapsed() >= STALE_WARN_AFTER
        && last_warning.is_none_or(|last| last.elapsed() >= STALE_WARN_INTERVAL)
    {
        log::warn!(
            "spread_bbo_zmq_sub no {label} received for {}ms endpoint={endpoint}",
            last_message_at.elapsed().as_millis()
        );
        *last_warning = Some(Instant::now());
    }
}

fn decode_message<'a>(
    parts: &'a [Vec<u8>],
    bbo_topic: &str,
    derivatives_topic: &str,
) -> Result<(WirePayloadKind, WireHeader, &'a [u8])> {
    anyhow::ensure!(
        parts.len() == 3,
        "invalid multipart frame count: got={} expected=3",
        parts.len()
    );
    let kind = if parts[0].as_slice() == bbo_topic.as_bytes() {
        WirePayloadKind::Bbo
    } else if parts[0].as_slice() == derivatives_topic.as_bytes() {
        WirePayloadKind::Derivatives
    } else {
        anyhow::bail!("unexpected market-data ZMQ topic");
    };
    let header = WireHeader::decode_for(&parts[1], kind)?;
    anyhow::ensure!(
        parts[2].len() == kind.payload_len(),
        "invalid ZMQ payload frame length: got={} expected={}",
        parts[2].len(),
        kind.payload_len()
    );
    Ok((kind, header, &parts[2]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::AskBidSpreadMsg;
    use mkt_signal::spread_pbs::publisher::{DERIVATIVES_PAYLOAD_BYTES, SPREAD_PAYLOAD_BYTES};

    #[test]
    fn decodes_expected_bbo_multipart_message() {
        let bbo_topic = bbo_topic("binance-futures").unwrap();
        let derivatives_topic = derivatives_topic("binance-futures").unwrap();
        let header = WireHeader {
            session_id: 7,
            sequence: 9,
            sent_ts_us: 11,
        };
        let msg = AskBidSpreadMsg::create("BTCUSDT".to_string(), 10, 1.0, 2.0, 1.1, 3.0);
        let bytes = msg.to_bytes();
        let mut payload = vec![0_u8; SPREAD_PAYLOAD_BYTES];
        payload[..bytes.len()].copy_from_slice(&bytes);
        let parts = vec![
            bbo_topic.as_bytes().to_vec(),
            header.encode().to_vec(),
            payload,
        ];

        let (kind, decoded_header, decoded_payload) =
            decode_message(&parts, &bbo_topic, &derivatives_topic).unwrap();
        assert_eq!(kind, WirePayloadKind::Bbo);
        assert_eq!(decoded_header, header);
        assert_eq!(decoded_payload.len(), SPREAD_PAYLOAD_BYTES);
    }

    #[test]
    fn decodes_expected_derivatives_multipart_message() {
        let bbo_topic = bbo_topic("binance-futures").unwrap();
        let derivatives_topic = derivatives_topic("binance-futures").unwrap();
        let header = WireHeader {
            session_id: 17,
            sequence: 19,
            sent_ts_us: 21,
        };
        let parts = vec![
            derivatives_topic.as_bytes().to_vec(),
            header.encode_for(WirePayloadKind::Derivatives).to_vec(),
            vec![0_u8; DERIVATIVES_PAYLOAD_BYTES],
        ];

        let (kind, decoded_header, decoded_payload) =
            decode_message(&parts, &bbo_topic, &derivatives_topic).unwrap();
        assert_eq!(kind, WirePayloadKind::Derivatives);
        assert_eq!(decoded_header, header);
        assert_eq!(decoded_payload.len(), DERIVATIVES_PAYLOAD_BYTES);
    }

    #[test]
    fn rejects_wrong_topic_or_frame_count() {
        let bbo_topic = bbo_topic("binance-futures").unwrap();
        let derivatives_topic = derivatives_topic("binance-futures").unwrap();
        assert!(decode_message(&[], &bbo_topic, &derivatives_topic).is_err());

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
        assert!(decode_message(&parts, &bbo_topic, &derivatives_topic).is_err());
    }
}
