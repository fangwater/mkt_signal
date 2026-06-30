use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::watch;

use mkt_signal::cfg::Config;
use mkt_signal::spread_pbs::ws::{run_public_ws, FrameHandler, WsLoopParams};
use order_common::TradingVenue;
use rolling_common::latency_kll::LatencyKll;
use runtime_common::affinity::pin_to_core;

const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/public/stream";
const BINANCE_SUBSCRIBE_CHUNK: usize = 200;

#[derive(Parser)]
#[command(name = "binance_bbo_time_probe")]
#[command(about = "Observe Binance futures BBO E-vs-T latency without publishing IPC.")]
struct Args {
    /// CPU core to pin the probe to.
    #[arg(long)]
    core: Option<usize>,

    /// Streams to subscribe.
    #[arg(long, value_enum, default_value = "both")]
    mode: ProbeMode,

    /// Optional comma-separated symbols. Defaults to the same Binance futures symbol set as spread_pbs.
    #[arg(long)]
    symbols: Option<String>,

    /// Path to mkt_cfg.yaml. Defaults to $HOME/spread_pbs/config/mkt_cfg.yaml, then repo config.
    #[arg(long)]
    config: Option<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProbeMode {
    Both,
    Depth5,
    Bookticker,
}

struct StatsState {
    depth_e: LatencyKll,
    depth_t: LatencyKll,
    book_e: LatencyKll,
    book_t: LatencyKll,
    depth_missing_e: u64,
    depth_missing_t: u64,
    book_missing_e: u64,
    book_missing_t: u64,
    depth_count: u64,
    book_count: u64,
}

impl StatsState {
    fn new() -> Self {
        Self {
            depth_e: LatencyKll::new("binance-bbo-time-probe depth5 recv_minus_E"),
            depth_t: LatencyKll::new("binance-bbo-time-probe depth5 recv_minus_T"),
            book_e: LatencyKll::new("binance-bbo-time-probe bookTicker recv_minus_E"),
            book_t: LatencyKll::new("binance-bbo-time-probe bookTicker recv_minus_T"),
            depth_missing_e: 0,
            depth_missing_t: 0,
            book_missing_e: 0,
            book_missing_t: 0,
            depth_count: 0,
            book_count: 0,
        }
    }

    fn record(
        &mut self,
        stream_kind: StreamKind,
        recv_us: i64,
        e_ms: Option<i64>,
        t_ms: Option<i64>,
    ) {
        match stream_kind {
            StreamKind::Depth5 => {
                self.depth_count += 1;
                record_latency(recv_us, e_ms, &mut self.depth_e, &mut self.depth_missing_e);
                record_latency(recv_us, t_ms, &mut self.depth_t, &mut self.depth_missing_t);
            }
            StreamKind::BookTicker => {
                self.book_count += 1;
                record_latency(recv_us, e_ms, &mut self.book_e, &mut self.book_missing_e);
                record_latency(recv_us, t_ms, &mut self.book_t, &mut self.book_missing_t);
            }
        }
    }

    fn log_counts(&mut self) {
        log::info!(
            "binance-bbo-time-probe counts depth={} bookTicker={} missing depth.E={} depth.T={} book.E={} book.T={}",
            self.depth_count,
            self.book_count,
            self.depth_missing_e,
            self.depth_missing_t,
            self.book_missing_e,
            self.book_missing_t,
        );
        self.depth_count = 0;
        self.book_count = 0;
        self.depth_missing_e = 0;
        self.depth_missing_t = 0;
        self.book_missing_e = 0;
        self.book_missing_t = 0;
    }
}

#[derive(Clone, Copy)]
enum StreamKind {
    Depth5,
    BookTicker,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    if let Some(core) = args.core {
        pin_to_core(core)?;
        log::info!("binance-bbo-time-probe pinned to core {}", core);
    }

    let local = tokio::task::LocalSet::new();
    local.run_until(run(args)).await
}

async fn run(args: Args) -> Result<()> {
    let config_path = resolve_cfg_path(args.config.as_deref())?;
    let cfg = Config::load_config(&config_path, TradingVenue::BinanceFutures).await?;
    let symbols = match args.symbols.as_deref() {
        Some(raw) => parse_symbols(raw),
        None => cfg.wait_for_symbols().await,
    };
    anyhow::ensure!(!symbols.is_empty(), "empty Binance futures symbol set");

    let subscribe_msgs = build_subscribe(&symbols, args.mode);
    log::info!(
        "binance-bbo-time-probe starting symbols={} mode={:?} subscribe_batches={} local_ip={} cfg={}",
        symbols.len(),
        args.mode,
        subscribe_msgs.len(),
        cfg.primary_local_ip,
        config_path,
    );

    let state = Rc::new(RefCell::new(StatsState::new()));
    let handler_state = state.clone();
    let handler: FrameHandler = Rc::new(move |recv_us, raw| {
        let Ok(value) = serde_json::from_slice::<Value>(raw) else {
            return;
        };
        let Some((kind, e_ms, t_ms)) = parse_bbo_times(&value) else {
            return;
        };
        handler_state.borrow_mut().record(kind, recv_us, e_ms, t_ms);
    });

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let ws_task = tokio::task::spawn_local(run_public_ws(
        WsLoopParams {
            label: "binance-bbo-time-probe",
            url: BINANCE_FUTURES_WS_URL.to_string(),
            local_ip: cfg.primary_local_ip.clone(),
            remote_ip: None,
            headers: Vec::new(),
            subscribe_msgs,
            keepalive: None,
            parse_okex_notices: false,
        },
        handler,
        shutdown_rx,
    ));

    let stats_task = {
        let state = state.clone();
        tokio::task::spawn_local(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                state.borrow_mut().log_counts();
            }
        })
    };

    let ctrl_c_task = tokio::task::spawn_local(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            log::warn!("binance-bbo-time-probe ctrl-c listener failed: {:#}", e);
        }
        let _ = shutdown_tx.send(true);
    });

    let mut tasks = FuturesUnordered::new();
    tasks.push(ws_task);
    tasks.push(stats_task);
    tasks.push(ctrl_c_task);
    while let Some(join) = tasks.next().await {
        if let Err(e) = join {
            log::error!("binance-bbo-time-probe task join failed: {:#}", e);
        }
        break;
    }
    Ok(())
}

fn build_subscribe(symbols: &[String], mode: ProbeMode) -> Vec<Value> {
    let streams = symbols.iter().flat_map(|symbol| {
        let symbol = symbol.to_ascii_lowercase();
        match mode {
            ProbeMode::Both => vec![
                format!("{}@depth5@0ms", symbol),
                format!("{}@bookTicker", symbol),
            ],
            ProbeMode::Depth5 => vec![format!("{}@depth5@0ms", symbol)],
            ProbeMode::Bookticker => vec![format!("{}@bookTicker", symbol)],
        }
    });
    build_multi_stream_subscribe(streams)
}

fn build_multi_stream_subscribe(streams: impl IntoIterator<Item = String>) -> Vec<Value> {
    let streams: Vec<String> = streams.into_iter().collect();
    streams
        .chunks(BINANCE_SUBSCRIBE_CHUNK)
        .enumerate()
        .map(|(i, chunk)| {
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": chunk,
                "id": (i as u64) + 1,
            })
        })
        .collect()
}

fn parse_bbo_times(value: &Value) -> Option<(StreamKind, Option<i64>, Option<i64>)> {
    let stream = value.get("stream").and_then(|v| v.as_str()).unwrap_or("");
    let payload = value.get("data").unwrap_or(value);
    if !payload.is_object() {
        return None;
    }
    let kind = if stream.ends_with("@depth5@0ms")
        || payload.get("e").and_then(|v| v.as_str()) == Some("depthUpdate")
    {
        StreamKind::Depth5
    } else if stream.ends_with("@bookTicker")
        || payload.get("e").and_then(|v| v.as_str()) == Some("bookTicker")
    {
        StreamKind::BookTicker
    } else {
        return None;
    };

    let has_bbo = (payload.get("b").is_some() || payload.get("a").is_some())
        && (payload.get("u").is_some() || payload.get("lastUpdateId").is_some());
    if !has_bbo {
        return None;
    }
    Some((
        kind,
        payload.get("E").and_then(parse_i64),
        payload.get("T").and_then(parse_i64),
    ))
}

fn record_latency(recv_us: i64, ts_ms: Option<i64>, kll: &mut LatencyKll, missing: &mut u64) {
    match ts_ms {
        Some(ts_ms) => kll.push((recv_us - ts_ms.saturating_mul(1000)) as f64),
        None => *missing += 1,
    }
}

fn parse_i64(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse::<i64>().ok())
}

fn parse_symbols(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_uppercase())
        .collect()
}

fn resolve_cfg_path(explicit: Option<&str>) -> Result<String> {
    if let Some(path) = explicit {
        return Ok(path.to_string());
    }
    let home = std::env::var("HOME").context("HOME not set")?;
    let deployed = format!("{home}/spread_pbs/config/mkt_cfg.yaml");
    if std::path::Path::new(&deployed).exists() {
        return Ok(deployed);
    }
    Ok("config/mkt_cfg.yaml".to_string())
}
