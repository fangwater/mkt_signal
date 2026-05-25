use anyhow::{bail, Result};
use clap::Parser;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_signal::rolling_metrics::latency_snapshot::{
    ACTION_ID_CANCEL, ACTION_ID_MARKET_DATA, ACTION_ID_NEW, LATENCY_SNAPSHOT_MAX_BUCKETS,
    LATENCY_SNAPSHOT_MSG_TYPE, LATENCY_SNAPSHOT_PAYLOAD_LEN, LATENCY_SNAPSHOT_SCHEMA_VER,
    METRIC_ID_DOWNLINK, METRIC_ID_IPC_TO_WS, METRIC_ID_RTT, METRIC_ID_SERVER, METRIC_ID_SPREAD_E2E,
    METRIC_ID_SPREAD_NET, METRIC_ID_UPLINK,
};
use serde::Serialize;
use std::env;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(name = "inspect_latency_snapshot")]
#[command(about = "Subscribe to a 512B latency snapshot Iceoryx service and print decoded buckets")]
struct Args {
    /// Full Iceoryx service name, e.g. bybit_intra_arb01/te_pubs/bybit/latency.
    #[arg(long)]
    service: Option<String>,

    /// Source kind used when --service is omitted: te/trade_engine or spread/spread_pbs.
    #[arg(long, default_value = "te")]
    source: String,

    /// IPC namespace for trade_engine services. Defaults to IPC_NAMESPACE when --service is omitted.
    #[arg(long)]
    namespace: Option<String>,

    /// Exchange slug for trade_engine services, e.g. bybit, binance, okex, gate, bitget.
    #[arg(long)]
    exchange: Option<String>,

    /// Venue slug for spread_pbs services, e.g. bybit-futures. Also accepted as exchange fallback.
    #[arg(long)]
    venue: Option<String>,

    /// Exit after receiving this many snapshots (0 = unlimited until timeout).
    #[arg(long, default_value_t = 1)]
    limit: u64,

    /// Timeout in seconds (0 = run forever until Ctrl-C).
    #[arg(long, default_value_t = 70)]
    timeout_s: u64,

    /// Poll interval in milliseconds.
    #[arg(long, default_value_t = 20)]
    poll_ms: u64,

    /// Print a waiting line every N seconds when no snapshot arrives (0 = disabled).
    #[arg(long, default_value_t = 15)]
    idle_log_s: u64,

    /// Subscriber queue size. Must not exceed the service subscriber_max_buffer_size.
    #[arg(long, default_value_t = 8)]
    buffer_size: usize,

    /// Only show one metric, e.g. ipc_to_ws, uplink, server, downlink, rtt.
    #[arg(long)]
    metric: Option<String>,

    /// Only show one action: new, cancel, market_data.
    #[arg(long)]
    action: Option<String>,

    /// Emit one JSON object per snapshot and suppress human-readable status lines.
    #[arg(long)]
    json: bool,

    /// Hide metric/action legend in human-readable output.
    #[arg(long)]
    no_legend: bool,

    /// Hide service static/dynamic status in human-readable output.
    #[arg(long)]
    no_service_status: bool,
}

#[derive(Debug, Serialize)]
struct SnapshotOut {
    seq: u64,
    service: String,
    msg_type: u32,
    schema_ver: u32,
    venue_id: u32,
    snapshot_time_us: i64,
    snapshot_age_us: Option<i64>,
    raw_n_buckets: u32,
    n_buckets: usize,
    shown_buckets: usize,
    buckets: Vec<BucketOut>,
}

#[derive(Debug, Serialize)]
struct BucketOut {
    bucket: usize,
    metric_id: u8,
    metric: &'static str,
    metric_desc: &'static str,
    action_id: u8,
    action: &'static str,
    n: u64,
    p50_us: i64,
    p90_us: i64,
    p95_us: i64,
    p99_us: i64,
}

#[derive(Debug)]
struct Filters {
    metric_id: Option<u8>,
    action_id: Option<u8>,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let service_name = resolve_service_name(&args)?;
    let filters = Filters {
        metric_id: parse_metric_filter(args.metric.as_deref())?,
        action_id: parse_action_filter(args.action.as_deref())?,
    };

    let node_name = format!("inspect_latency_snapshot_{}", std::process::id());
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&service_name)?)
        .publish_subscribe::<[u8; LATENCY_SNAPSHOT_PAYLOAD_LEN]>()
        .open()?;

    if !args.json && !args.no_service_status {
        print_service_status(&service_name, &service);
    }

    let subscriber = service
        .subscriber_builder()
        .buffer_size(args.buffer_size)
        .create()?;

    if !args.json {
        println!(
            "[LAT] subscribing service='{}' payload={} timeout={}s limit={} buffer_size={}",
            service_name,
            LATENCY_SNAPSHOT_PAYLOAD_LEN,
            args.timeout_s,
            args.limit,
            args.buffer_size
        );
        if !args.no_legend {
            print_legend();
        }
    }

    let start = Instant::now();
    let timeout = if args.timeout_s == 0 {
        Duration::from_secs(u64::MAX)
    } else {
        Duration::from_secs(args.timeout_s)
    };
    let idle_log_interval = (args.idle_log_s > 0).then(|| Duration::from_secs(args.idle_log_s));
    let mut last_idle_log = Instant::now();

    let mut received = 0u64;
    loop {
        if start.elapsed() >= timeout {
            break;
        }
        if args.limit > 0 && received >= args.limit {
            break;
        }

        match subscriber.receive()? {
            Some(sample) => {
                received = received.saturating_add(1);
                let snapshot =
                    decode_snapshot(received, &service_name, sample.payload(), &filters)?;
                if args.json {
                    println!("{}", serde_json::to_string(&snapshot)?);
                } else {
                    print_snapshot_text(&snapshot);
                }
                last_idle_log = Instant::now();
            }
            None => {
                if !args.json {
                    if let Some(interval) = idle_log_interval {
                        if last_idle_log.elapsed() >= interval {
                            println!(
                                "[LAT] waiting elapsed_s={} received={} service='{}'",
                                start.elapsed().as_secs(),
                                received,
                                service_name
                            );
                            last_idle_log = Instant::now();
                        }
                    }
                }
                thread::sleep(Duration::from_millis(args.poll_ms));
            }
        }
    }

    if !args.json {
        println!("[LAT] done, received {} snapshots", received);
    }
    Ok(())
}

fn resolve_service_name(args: &Args) -> Result<String> {
    if let Some(service) = args
        .service
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Ok(service.to_owned());
    }

    let source = args.source.trim().to_ascii_lowercase().replace('-', "_");
    match source.as_str() {
        "te" | "trade_engine" | "tradeengine" => {
            let namespace = args
                .namespace
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .or_else(|| env::var("IPC_NAMESPACE").ok())
                .ok_or_else(|| anyhow::anyhow!("--namespace or IPC_NAMESPACE is required when --service is omitted for source=te"))?;
            let exchange = args
                .exchange
                .as_deref()
                .or(args.venue.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "--exchange is required when --service is omitted for source=te"
                    )
                })?;
            Ok(format!("{}/te_pubs/{}/latency", namespace, exchange))
        }
        "spread" | "spread_pbs" => {
            let venue = args
                .venue
                .as_deref()
                .or(args.exchange.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "--venue is required when --service is omitted for source=spread"
                    )
                })?;
            Ok(format!("spread_pbs/{}/latency", venue))
        }
        other => bail!("unsupported --source '{}'; use te or spread", other),
    }
}

fn print_service_status(
    service_name: &str,
    service: &iceoryx2::service::port_factory::publish_subscribe::PortFactory<
        ipc::Service,
        [u8; LATENCY_SNAPSHOT_PAYLOAD_LEN],
        (),
    >,
) {
    println!(
        "[LAT] service='{}' payload={} max_publishers={} max_subscribers={} subscriber_max_buffer_size={} history_size={} active_publishers={} active_subscribers={}",
        service_name,
        LATENCY_SNAPSHOT_PAYLOAD_LEN,
        service.static_config().max_publishers(),
        service.static_config().max_subscribers(),
        service.static_config().subscriber_max_buffer_size(),
        service.static_config().history_size(),
        service.dynamic_config().number_of_publishers(),
        service.dynamic_config().number_of_subscribers(),
    );
}

fn print_legend() {
    println!("[LAT] legend metric/action:");
    println!("[LAT]   ipc_to_ws = T1-T0 local IPC receive/parse -> WS send path");
    println!("[LAT]   uplink    = T2-T1 local send wall-clock -> exchange server timestamp");
    println!("[LAT]   server    = T3-T2 exchange processing; Bybit reports one server timestamp so this is 0");
    println!(
        "[LAT]   downlink  = T4-T3 exchange server timestamp -> local response receive wall-clock"
    );
    println!("[LAT]   rtt       = T4-T1 local monotonic send -> response receive");
    println!("[LAT]   action    = new / cancel / market_data");
}

fn decode_snapshot(
    seq: u64,
    service: &str,
    payload: &[u8; LATENCY_SNAPSHOT_PAYLOAD_LEN],
    filters: &Filters,
) -> Result<SnapshotOut> {
    let msg_type = read_u32(payload, 0)?;
    let schema_ver = read_u32(payload, 4)?;
    let venue_id = read_u32(payload, 8)?;
    let raw_n_buckets = read_u32(payload, 12)?;
    let snapshot_time_us = read_i64(payload, 16)?;

    if msg_type != LATENCY_SNAPSHOT_MSG_TYPE {
        bail!(
            "unexpected msg_type={} expected={}",
            msg_type,
            LATENCY_SNAPSHOT_MSG_TYPE
        );
    }
    if schema_ver != LATENCY_SNAPSHOT_SCHEMA_VER {
        bail!(
            "unexpected schema_ver={} expected={}",
            schema_ver,
            LATENCY_SNAPSHOT_SCHEMA_VER
        );
    }

    let n_buckets = (raw_n_buckets as usize).min(LATENCY_SNAPSHOT_MAX_BUCKETS);
    let mut buckets = Vec::with_capacity(n_buckets);
    for idx in 0..n_buckets {
        let off = 32 + idx * 48;
        let metric_id = payload[off];
        let action_id = payload[off + 1];
        if filters.metric_id.is_some_and(|filter| filter != metric_id)
            || filters.action_id.is_some_and(|filter| filter != action_id)
        {
            continue;
        }
        buckets.push(BucketOut {
            bucket: idx,
            metric_id,
            metric: metric_name(metric_id),
            metric_desc: metric_desc(metric_id),
            action_id,
            action: action_name(action_id),
            n: read_u64(payload, off + 8)?,
            p50_us: read_i64(payload, off + 16)?,
            p90_us: read_i64(payload, off + 24)?,
            p95_us: read_i64(payload, off + 32)?,
            p99_us: read_i64(payload, off + 40)?,
        });
    }

    Ok(SnapshotOut {
        seq,
        service: service.to_owned(),
        msg_type,
        schema_ver,
        venue_id,
        snapshot_time_us,
        snapshot_age_us: now_us().map(|now| now.saturating_sub(snapshot_time_us)),
        raw_n_buckets,
        n_buckets,
        shown_buckets: buckets.len(),
        buckets,
    })
}

fn print_snapshot_text(snapshot: &SnapshotOut) {
    let age_ms = snapshot
        .snapshot_age_us
        .map(|age| format!("{:.3}", age as f64 / 1000.0))
        .unwrap_or_else(|| "NA".to_owned());
    println!(
        "[LAT] #{} venue_id={} snapshot_time_us={} age_ms={} n_buckets={} raw_n_buckets={} shown_buckets={}",
        snapshot.seq,
        snapshot.venue_id,
        snapshot.snapshot_time_us,
        age_ms,
        snapshot.n_buckets,
        snapshot.raw_n_buckets,
        snapshot.shown_buckets
    );
    println!(
        "[LAT]   {:>2} {:<10} {:<6} {:>8} {:>10} {:>10} {:>10} {:>10}",
        "#", "metric", "action", "n", "p50_us", "p90_us", "p95_us", "p99_us"
    );
    for b in &snapshot.buckets {
        println!(
            "[LAT]   {:>2} {:<10} {:<6} {:>8} {:>10} {:>10} {:>10} {:>10}",
            b.bucket, b.metric, b.action, b.n, b.p50_us, b.p90_us, b.p95_us, b.p99_us
        );
    }
}

fn parse_metric_filter(raw: Option<&str>) -> Result<Option<u8>> {
    let Some(raw) = raw else { return Ok(None) };
    let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
    let id = match normalized.as_str() {
        "ipc_to_ws" | "ipc" => METRIC_ID_IPC_TO_WS,
        "uplink" => METRIC_ID_UPLINK,
        "server" => METRIC_ID_SERVER,
        "downlink" => METRIC_ID_DOWNLINK,
        "rtt" => METRIC_ID_RTT,
        "spread_net" => METRIC_ID_SPREAD_NET,
        "spread_e2e" => METRIC_ID_SPREAD_E2E,
        _ => bail!(
            "unknown --metric '{}'; use ipc_to_ws/uplink/server/downlink/rtt",
            raw
        ),
    };
    Ok(Some(id))
}

fn parse_action_filter(raw: Option<&str>) -> Result<Option<u8>> {
    let Some(raw) = raw else { return Ok(None) };
    let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
    let id = match normalized.as_str() {
        "new" => ACTION_ID_NEW,
        "cancel" => ACTION_ID_CANCEL,
        "market_data" | "market" => ACTION_ID_MARKET_DATA,
        _ => bail!("unknown --action '{}'; use new/cancel/market_data", raw),
    };
    Ok(Some(id))
}

fn metric_name(id: u8) -> &'static str {
    match id {
        METRIC_ID_IPC_TO_WS => "ipc_to_ws",
        METRIC_ID_UPLINK => "uplink",
        METRIC_ID_SERVER => "server",
        METRIC_ID_DOWNLINK => "downlink",
        METRIC_ID_RTT => "rtt",
        METRIC_ID_SPREAD_NET => "spread_net",
        METRIC_ID_SPREAD_E2E => "spread_e2e",
        _ => "unknown",
    }
}

fn metric_desc(id: u8) -> &'static str {
    match id {
        METRIC_ID_IPC_TO_WS => "T1-T0 local IPC receive/parse to WS send path",
        METRIC_ID_UPLINK => "T2-T1 local send wall-clock to exchange server timestamp",
        METRIC_ID_SERVER => "T3-T2 exchange processing",
        METRIC_ID_DOWNLINK => "T4-T3 exchange server timestamp to local receive wall-clock",
        METRIC_ID_RTT => "T4-T1 local monotonic send to response receive",
        METRIC_ID_SPREAD_NET => "spread_pbs recv_us minus event_time_us",
        METRIC_ID_SPREAD_E2E => "spread_pbs accepted_us minus event_time_us",
        _ => "unknown metric",
    }
}

fn action_name(id: u8) -> &'static str {
    match id {
        ACTION_ID_NEW => "new",
        ACTION_ID_CANCEL => "cancel",
        ACTION_ID_MARKET_DATA => "market_data",
        _ => "unknown",
    }
}

fn now_us() -> Option<i64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let micros = duration.as_micros();
    i64::try_from(micros).ok()
}

fn read_u32(payload: &[u8], off: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(payload[off..off + 4].try_into()?))
}

fn read_u64(payload: &[u8], off: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(payload[off..off + 8].try_into()?))
}

fn read_i64(payload: &[u8], off: usize) -> Result<i64> {
    Ok(i64::from_le_bytes(payload[off..off + 8].try_into()?))
}
