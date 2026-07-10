use anyhow::{bail, Context, Result};
use clap::Parser;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use rolling_common::health_snapshot::{
    ip_from_bytes, HEALTH_FLAG_CONNECTED, HEALTH_FLAG_LAST_ROUTE_PROTECTED,
    HEALTH_FLAG_RECONNECTING, HEALTH_FLAG_RECONNECT_PENDING, HEALTH_FLAG_ROUTE_PAUSED,
    HEALTH_FLAG_TCP_LOSS_ACT, HEALTH_MARKET_FUTURES, HEALTH_MARKET_SPOT, HEALTH_SNAPSHOT_MSG_TYPE,
    HEALTH_SNAPSHOT_PAYLOAD_LEN, HEALTH_SNAPSHOT_SCHEMA_VER, HEALTH_STATE_DISCONNECTED,
    HEALTH_STATE_DRAINING, HEALTH_STATE_HEALTHY, HEALTH_STATE_PAUSED, HEALTH_STATE_PROTECTED,
    HEALTH_STATE_RECONNECTING,
};
use serde::Serialize;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(name = "inspect_health_snapshot")]
#[command(about = "Subscribe to a trade-engine health snapshot service")]
struct Args {
    #[arg(long)]
    service: String,

    #[arg(long, default_value_t = 10)]
    timeout_secs: u64,

    #[arg(long, default_value_t = 0)]
    limit: u64,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Serialize)]
struct HealthOut {
    venue_id: u32,
    endpoint_id: u32,
    snapshot_time_us: i64,
    snapshot_age_us: i64,
    sample_interval_ms: u32,
    window_ms: u32,
    group_id: Option<u32>,
    market: &'static str,
    state: &'static str,
    connected: bool,
    route_paused: bool,
    reconnect_pending: bool,
    reconnecting: bool,
    tcp_loss_act: bool,
    last_route_protected: bool,
    local_ip: String,
    remote_addr: Option<String>,
    window_data_segs_out: u64,
    window_retrans: u64,
    retrans_rate_bp: u64,
    total_retrans: u32,
    rtt_us: u32,
    rttvar_us: u32,
    last_retrans_age_ms: Option<u32>,
    pending: u32,
    inflight: u32,
    query_pending: u32,
    query_inflight: u32,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let node = NodeBuilder::new()
        .name(&NodeName::new(&format!(
            "inspect_health_snapshot_{}",
            std::process::id()
        ))?)
        .create::<ipc::Service>()?;
    let service = node
        .service_builder(&ServiceName::new(args.service.trim())?)
        .publish_subscribe::<[u8; HEALTH_SNAPSHOT_PAYLOAD_LEN]>()
        .open()
        .with_context(|| format!("open health service {}", args.service))?;
    let subscriber = service.subscriber_builder().create()?;

    let deadline = Instant::now() + Duration::from_secs(args.timeout_secs.max(1));
    let mut received = 0u64;
    while Instant::now() < deadline && (args.limit == 0 || received < args.limit) {
        match subscriber.receive()? {
            Some(sample) => {
                let decoded = decode(sample.payload())?;
                if args.json {
                    println!("{}", serde_json::to_string(&decoded)?);
                } else {
                    println!(
                        "[HEALTH] venue={} market={} endpoint={} state={} local={} remote={} window={}/{} rate_bp={} total_retrans={} rtt_us={} rttvar_us={} inflight={} pending={} age_us={}",
                        decoded.venue_id,
                        decoded.market,
                        decoded.endpoint_id,
                        decoded.state,
                        decoded.local_ip,
                        decoded.remote_addr.as_deref().unwrap_or("-"),
                        decoded.window_retrans,
                        decoded.window_data_segs_out,
                        decoded.retrans_rate_bp,
                        decoded.total_retrans,
                        decoded.rtt_us,
                        decoded.rttvar_us,
                        decoded.inflight + decoded.query_inflight,
                        decoded.pending + decoded.query_pending,
                        decoded.snapshot_age_us,
                    );
                }
                received = received.saturating_add(1);
            }
            None => std::thread::sleep(Duration::from_millis(1)),
        }
    }

    if received == 0 {
        bail!(
            "no health snapshots received from {} within {}s",
            args.service,
            args.timeout_secs
        );
    }
    Ok(())
}

fn decode(payload: &[u8; HEALTH_SNAPSHOT_PAYLOAD_LEN]) -> Result<HealthOut> {
    let msg_type = read_u32(payload, 0);
    if msg_type != HEALTH_SNAPSHOT_MSG_TYPE {
        bail!(
            "unexpected msg_type={} expected={}",
            msg_type,
            HEALTH_SNAPSHOT_MSG_TYPE
        );
    }
    let schema_ver = read_u32(payload, 4);
    if schema_ver != HEALTH_SNAPSHOT_SCHEMA_VER {
        bail!(
            "unexpected schema_ver={} expected={}",
            schema_ver,
            HEALTH_SNAPSHOT_SCHEMA_VER
        );
    }

    let flags = read_u16(payload, 38);
    let local_ip = ip_from_bytes(payload[40..56].try_into().unwrap()).to_string();
    let remote_ip_bytes: [u8; 16] = payload[56..72].try_into().unwrap();
    let remote_port = read_u16(payload, 72);
    let remote_addr = if remote_ip_bytes == [0; 16] {
        None
    } else {
        Some(format!(
            "{}:{}",
            ip_from_bytes(remote_ip_bytes),
            remote_port
        ))
    };
    let window_data_segs_out = read_u64(payload, 80);
    let window_retrans = read_u64(payload, 88);
    let snapshot_time_us = read_i64(payload, 16);

    Ok(HealthOut {
        venue_id: read_u32(payload, 8),
        endpoint_id: read_u32(payload, 12),
        snapshot_time_us,
        snapshot_age_us: now_us().saturating_sub(snapshot_time_us),
        sample_interval_ms: read_u32(payload, 24),
        window_ms: read_u32(payload, 28),
        group_id: match read_u32(payload, 32) {
            u32::MAX => None,
            value => Some(value),
        },
        market: market_name(payload[36]),
        state: state_name(payload[37]),
        connected: flags & HEALTH_FLAG_CONNECTED != 0,
        route_paused: flags & HEALTH_FLAG_ROUTE_PAUSED != 0,
        reconnect_pending: flags & HEALTH_FLAG_RECONNECT_PENDING != 0,
        reconnecting: flags & HEALTH_FLAG_RECONNECTING != 0,
        tcp_loss_act: flags & HEALTH_FLAG_TCP_LOSS_ACT != 0,
        last_route_protected: flags & HEALTH_FLAG_LAST_ROUTE_PROTECTED != 0,
        local_ip,
        remote_addr,
        window_data_segs_out,
        window_retrans,
        retrans_rate_bp: if window_data_segs_out == 0 {
            0
        } else {
            window_retrans.saturating_mul(10_000) / window_data_segs_out
        },
        total_retrans: read_u32(payload, 96),
        rtt_us: read_u32(payload, 100),
        rttvar_us: read_u32(payload, 104),
        last_retrans_age_ms: match read_u32(payload, 108) {
            u32::MAX => None,
            value => Some(value),
        },
        pending: read_u32(payload, 112),
        inflight: read_u32(payload, 116),
        query_pending: read_u32(payload, 120),
        query_inflight: read_u32(payload, 124),
    })
}

fn market_name(value: u8) -> &'static str {
    match value {
        HEALTH_MARKET_SPOT => "spot",
        HEALTH_MARKET_FUTURES => "futures",
        _ => "unknown",
    }
}

fn state_name(value: u8) -> &'static str {
    match value {
        HEALTH_STATE_DISCONNECTED => "disconnected",
        HEALTH_STATE_HEALTHY => "healthy",
        HEALTH_STATE_PAUSED => "paused",
        HEALTH_STATE_DRAINING => "draining",
        HEALTH_STATE_RECONNECTING => "reconnecting",
        HEALTH_STATE_PROTECTED => "protected",
        _ => "unknown",
    }
}

fn read_u16(payload: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap())
}

fn read_u32(payload: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap())
}

fn read_u64(payload: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap())
}

fn read_i64(payload: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap())
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_micros()).ok())
        .unwrap_or(0)
}
