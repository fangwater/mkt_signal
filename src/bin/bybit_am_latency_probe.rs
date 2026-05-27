use anyhow::{Context, Result};
use clap::Parser;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_signal::common::basic_account_msg::{
    split_basic_account_event, BasicAccountEventType, BasicAccountScope, BasicTradeLiteMsg,
};
use mkt_signal::common::bybit_account_msg::BybitBasicOrderMsg;
use mkt_signal::common::ipc_service_name::build_service_name;
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::portfolio_margin::pm_forwarder::{
    PM_HISTORY_SIZE, PM_MAX_SUBSCRIBERS, PM_SUBSCRIBER_MAX_BUFFER_SIZE,
};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

const ACCOUNT_PAYLOAD: usize = 16_384;
const STATS_WINDOW: usize = 100;

#[derive(Debug, Parser)]
#[command(
    name = "bybit_am_latency_probe",
    about = "Measure Bybit account-monitor trade update latency in microseconds"
)]
struct Args {
    #[arg(long, default_value = "account_pubs/bybit_pm")]
    service: String,

    #[arg(long, default_value = "bybit_am_latency_probe")]
    node_name: String,

    #[arg(long, default_value = "logs/bybit_am_latency_probe.log")]
    output: PathBuf,

    #[arg(long, default_value_t = STATS_WINDOW)]
    stats_window: usize,

    #[arg(long, default_value_t = 1_000)]
    idle_sleep_us: u64,
}

#[derive(Debug, Clone, Copy)]
enum LatencyKind {
    OrderTrade,
    TradeLite,
}

impl LatencyKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::OrderTrade => "order_trade",
            Self::TradeLite => "trade_lite",
        }
    }
}

#[derive(Debug)]
struct LatencySample {
    kind: LatencyKind,
    scope: BasicAccountScope,
    venue: u8,
    event_ts_us: i64,
    local_ts_us: i64,
    latency_us: i64,
    symbol: String,
    order_id: i64,
    client_order_id: i64,
    trade_id: String,
    side: u8,
    is_maker: bool,
    price: f64,
    quantity: f64,
    raw_execution_type: String,
    raw_status: String,
}

#[derive(Debug)]
struct LatencyStats {
    kind: LatencyKind,
    count_total: u64,
    window: Vec<i64>,
    stats_window: usize,
}

impl LatencyStats {
    fn new(kind: LatencyKind, stats_window: usize) -> Self {
        Self {
            kind,
            count_total: 0,
            window: Vec::with_capacity(stats_window),
            stats_window,
        }
    }

    fn push(&mut self, latency_us: i64) -> Option<StatsSnapshot> {
        self.count_total += 1;
        self.window.push(latency_us);
        if self.window.len() >= self.stats_window {
            let snapshot = StatsSnapshot::from_window(self.kind, self.count_total, &self.window);
            self.window.clear();
            Some(snapshot)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct StatsSnapshot {
    kind: LatencyKind,
    total_count: u64,
    window_count: usize,
    min_us: i64,
    max_us: i64,
    avg_us: f64,
    p50_us: i64,
    p90_us: i64,
    p95_us: i64,
    p99_us: i64,
}

impl StatsSnapshot {
    fn from_window(kind: LatencyKind, total_count: u64, values: &[i64]) -> Self {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let sum: i128 = sorted.iter().map(|v| i128::from(*v)).sum();
        let avg_us = sum as f64 / sorted.len() as f64;
        Self {
            kind,
            total_count,
            window_count: sorted.len(),
            min_us: *sorted.first().unwrap_or(&0),
            max_us: *sorted.last().unwrap_or(&0),
            avg_us,
            p50_us: percentile_nearest_rank(&sorted, 50),
            p90_us: percentile_nearest_rank(&sorted, 90),
            p95_us: percentile_nearest_rank(&sorted, 95),
            p99_us: percentile_nearest_rank(&sorted, 99),
        }
    }
}

fn percentile_nearest_rank(sorted: &[i64], percentile: usize) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (percentile * sorted.len()).div_ceil(100);
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[idx]
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.stats_window == 0 {
        anyhow::bail!("--stats-window must be positive");
    }
    if let Some(parent) = args.output.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create output directory {}", parent.display()))?;
        }
    }

    let output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.output)
        .with_context(|| format!("open output log {}", args.output.display()))?;
    let mut writer = BufWriter::new(output);
    writeln!(
        writer,
        "kind=probe_start local_ts_us={} service={} output={} stats_window={} idle_sleep_us={} ipc_namespace={}",
        get_timestamp_us(),
        args.service,
        args.output.display(),
        args.stats_window,
        args.idle_sleep_us,
        std::env::var("IPC_NAMESPACE").unwrap_or_default()
    )?;
    writer.flush()?;

    let service_name = build_service_name(&args.service);
    let subscriber = create_subscriber(&args.node_name, &service_name)
        .with_context(|| format!("subscribe service {}", service_name))?;

    let mut order_trade_stats = LatencyStats::new(LatencyKind::OrderTrade, args.stats_window);
    let mut trade_lite_stats = LatencyStats::new(LatencyKind::TradeLite, args.stats_window);
    let idle_sleep = Duration::from_micros(args.idle_sleep_us);

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                if let Some(latency) = parse_latency_sample(sample.payload()) {
                    write_sample(&mut writer, &latency)?;
                    let stats = match latency.kind {
                        LatencyKind::OrderTrade => order_trade_stats.push(latency.latency_us),
                        LatencyKind::TradeLite => trade_lite_stats.push(latency.latency_us),
                    };
                    if let Some(snapshot) = stats {
                        write_stats(&mut writer, &snapshot)?;
                    }
                    writer.flush()?;
                }
            }
            Ok(None) => thread::sleep(idle_sleep),
            Err(err) => {
                writeln!(
                    writer,
                    "kind=receive_error local_ts_us={} err={:?}",
                    get_timestamp_us(),
                    err
                )?;
                writer.flush()?;
                thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn create_subscriber(
    node_name: &str,
    service_name: &str,
) -> Result<Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()>> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(node_name)?)
        .create::<ipc::Service>()?;
    let service_name = ServiceName::new(service_name)?;
    let service = node
        .service_builder(&service_name)
        .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
        .max_publishers(1)
        .max_subscribers(PM_MAX_SUBSCRIBERS)
        .history_size(PM_HISTORY_SIZE)
        .subscriber_max_buffer_size(PM_SUBSCRIBER_MAX_BUFFER_SIZE)
        .open()?;
    Ok(service.subscriber_builder().create()?)
}

fn parse_latency_sample(payload: &[u8]) -> Option<LatencySample> {
    let local_ts_us = get_timestamp_us();
    let (event_type, scope, data) = split_basic_account_event(payload)?;
    match event_type {
        BasicAccountEventType::OrderUpdate => {
            let msg = BybitBasicOrderMsg::from_bytes(data).ok()?;
            if msg.execution_type != 5 {
                return None;
            }
            let event_ts_us = ms_to_us(msg.event_time);
            Some(LatencySample {
                kind: LatencyKind::OrderTrade,
                scope,
                venue: msg.venue,
                event_ts_us,
                local_ts_us,
                latency_us: local_ts_us.saturating_sub(event_ts_us),
                symbol: msg.symbol,
                order_id: msg.order_id,
                client_order_id: msg.client_order_id,
                trade_id: String::new(),
                side: msg.side,
                is_maker: msg.is_maker != 0,
                price: if msg.last_executed_price > 0.0 {
                    msg.last_executed_price
                } else {
                    msg.price
                },
                quantity: msg.cumulative_filled_quantity,
                raw_execution_type: msg.raw_execution_type,
                raw_status: msg.raw_status,
            })
        }
        BasicAccountEventType::TradeUpdateLite => {
            let msg = BasicTradeLiteMsg::from_bytes(data).ok()?;
            let event_ts_us = ms_to_us(msg.event_time);
            let trade_id = msg.trade_id_str().to_string();
            Some(LatencySample {
                kind: LatencyKind::TradeLite,
                scope,
                venue: msg.venue,
                event_ts_us,
                local_ts_us,
                latency_us: local_ts_us.saturating_sub(event_ts_us),
                symbol: msg.symbol,
                order_id: 0,
                client_order_id: msg.client_order_id,
                trade_id,
                side: msg.side,
                is_maker: msg.is_maker != 0,
                price: msg.last_executed_price,
                quantity: msg.last_executed_quantity,
                raw_execution_type: "TradeUpdateLite".to_string(),
                raw_status: String::new(),
            })
        }
        _ => None,
    }
}

fn ms_to_us(ts_ms: i64) -> i64 {
    ts_ms.saturating_mul(1_000)
}

fn write_sample(writer: &mut BufWriter<File>, sample: &LatencySample) -> Result<()> {
    writeln!(
        writer,
        "kind=sample type={} scope={} venue={} local_ts_us={} event_ts_us={} latency_us={} symbol={} order_id={} client_order_id={} trade_id={} side={} maker={} price={:.12} qty={:.12} raw_exec={} raw_status={}",
        sample.kind.as_str(),
        sample.scope.as_str(),
        sample.venue,
        sample.local_ts_us,
        sample.event_ts_us,
        sample.latency_us,
        sample.symbol,
        sample.order_id,
        sample.client_order_id,
        sample.trade_id,
        sample.side,
        sample.is_maker,
        sample.price,
        sample.quantity,
        sample.raw_execution_type,
        sample.raw_status
    )?;
    Ok(())
}

fn write_stats(writer: &mut BufWriter<File>, stats: &StatsSnapshot) -> Result<()> {
    writeln!(
        writer,
        "kind=stats type={} total_count={} window_count={} min_us={} max_us={} avg_us={:.3} p50_us={} p90_us={} p95_us={} p99_us={}",
        stats.kind.as_str(),
        stats.total_count,
        stats.window_count,
        stats.min_us,
        stats.max_us,
        stats.avg_us,
        stats.p50_us,
        stats.p90_us,
        stats.p95_us,
        stats.p99_us
    )?;
    Ok(())
}
