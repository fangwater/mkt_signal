use anyhow::{Context, Result};
use clap::Parser;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_signal::cfg::Config;
use mkt_signal::spread_pbs::ws::{run_public_ws, FrameHandler, WsLoopParams};
use order_common::TradingVenue;
use runtime_common::affinity::pin_to_core;
use runtime_common::ipc_service_name::build_service_name;
use runtime_common::time_util::get_timestamp_us;
use serde::Serialize;
use serde_json::Value;
use signal_common::lazy_taker_action::{
    LazyTakerAction, LazyTakerActionMsg, LAZY_TAKER_ACTION_CHANNEL, LAZY_TAKER_ACTION_PAYLOAD,
};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::watch;

const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/public/stream";
const BINANCE_SUBSCRIBE_CHUNK: usize = 200;

#[derive(Parser)]
#[command(name = "hedge_lazy_taker_eval")]
#[command(about = "Evaluate unit-size lazy taker actions against Binance futures BBO.")]
struct Args {
    #[arg(long)]
    core: Option<usize>,

    #[arg(long)]
    symbols: Option<String>,

    #[arg(long)]
    config: Option<String>,

    #[arg(long, default_value = "data/hedge_lazy_taker_eval")]
    output_dir: PathBuf,

    #[arg(long, default_value_t = 2)]
    delay_ms: u64,

    #[arg(long, default_value_t = 600)]
    buffer_secs: u64,

    #[arg(long, default_value_t = 200_000)]
    max_points_per_symbol: usize,
}

#[derive(Clone, Copy)]
struct BboPoint {
    local_tp_us: i64,
    bid: f64,
    ask: f64,
}

#[derive(Default)]
struct SymbolBboBuffer {
    points: Vec<BboPoint>,
    start: usize,
}

impl SymbolBboBuffer {
    fn push(&mut self, point: BboPoint, cutoff_us: i64, max_points: usize) {
        self.points.push(point);
        while self.start < self.points.len()
            && (self.points[self.start].local_tp_us < cutoff_us
                || self.points.len().saturating_sub(self.start) > max_points)
        {
            self.start += 1;
        }
        if self.start >= 4096 && self.start.saturating_mul(2) >= self.points.len() {
            self.points.drain(..self.start);
            self.start = 0;
        }
    }

    fn at_or_before(&self, target_us: i64) -> Option<BboPoint> {
        let active = self.points.get(self.start..)?;
        let index = active.partition_point(|point| point.local_tp_us <= target_us);
        index.checked_sub(1).map(|i| active[i])
    }
}

#[derive(Clone)]
struct HeldAction {
    direct_tp_us: i64,
    direction: i8,
    model_name: String,
    venue: u8,
}

struct PendingEvaluation {
    symbol: String,
    model_name: String,
    venue: u8,
    direction: i8,
    direct_tp_us: i64,
    take_tp_us: i64,
    held: bool,
}

#[derive(Debug, Default, Serialize)]
struct BucketStats {
    events: u64,
    wins: u64,
    losses: u64,
    flat: u64,
    missing_bbo: u64,
    cumulative_pnl: f64,
    win_rate: f64,
}

#[derive(Debug, Default, Serialize)]
struct ModelStats {
    held: BucketStats,
    no_hold: BucketStats,
    repeated_holds: u64,
    direction_resets: u64,
}

struct EvaluationResult {
    symbol: String,
    model_name: String,
    venue: u8,
    direction: i8,
    category: &'static str,
    direct_tp_us: i64,
    take_tp_us: i64,
    direct_target_us: i64,
    lazy_target_us: i64,
    direct_price: Option<f64>,
    lazy_price: Option<f64>,
    pnl: Option<f64>,
    status: &'static str,
}

struct OutputStore {
    dir: PathBuf,
    events: BufWriter<File>,
}

impl OutputStore {
    fn new(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir)
            .with_context(|| format!("create output dir failed: {}", dir.display()))?;
        let events_path = dir.join("events.csv");
        let needs_header = fs::metadata(&events_path)
            .map(|meta| meta.len() == 0)
            .unwrap_or(true);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&events_path)
            .with_context(|| format!("open events file failed: {}", events_path.display()))?;
        let mut events = BufWriter::new(file);
        if needs_header {
            writeln!(
                events,
                "take_tp_us,direct_tp_us,hold_us,symbol,model_name,venue,direction,category,direct_target_us,lazy_target_us,direct_price,lazy_price,pnl,status"
            )?;
            events.flush()?;
        }
        Ok(Self { dir, events })
    }

    fn persist(
        &mut self,
        result: &EvaluationResult,
        delay_us: i64,
        stats: &HashMap<String, ModelStats>,
    ) -> Result<()> {
        writeln!(
            self.events,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            result.take_tp_us,
            result.direct_tp_us,
            result.take_tp_us.saturating_sub(result.direct_tp_us),
            csv_text(&result.symbol),
            csv_text(&result.model_name),
            result.venue,
            result.direction,
            result.category,
            result.direct_target_us,
            result.lazy_target_us,
            fmt_optional(result.direct_price),
            fmt_optional(result.lazy_price),
            fmt_optional(result.pnl),
            result.status,
        )?;
        self.events.flush()?;

        let summary = serde_json::json!({
            "updated_tp_us": get_timestamp_us(),
            "delay_us": delay_us,
            "models": stats,
        });
        let tmp_path = self.dir.join("summary.json.tmp");
        let final_path = self.dir.join("summary.json");
        fs::write(&tmp_path, serde_json::to_vec_pretty(&summary)?)
            .with_context(|| format!("write summary failed: {}", tmp_path.display()))?;
        fs::rename(&tmp_path, &final_path).with_context(|| {
            format!(
                "replace summary failed: {} -> {}",
                tmp_path.display(),
                final_path.display()
            )
        })?;
        Ok(())
    }
}

struct AnalyzerState {
    delay_us: i64,
    buffer_us: i64,
    max_points_per_symbol: usize,
    books: HashMap<String, SymbolBboBuffer>,
    holds: HashMap<String, HeldAction>,
    pending: VecDeque<PendingEvaluation>,
    stats: HashMap<String, ModelStats>,
    output: OutputStore,
}

impl AnalyzerState {
    fn new(args: &Args) -> Result<Self> {
        Ok(Self {
            delay_us: i64::try_from(args.delay_ms)
                .unwrap_or(i64::MAX / 1000)
                .saturating_mul(1000),
            buffer_us: i64::try_from(args.buffer_secs)
                .unwrap_or(i64::MAX / 1_000_000)
                .saturating_mul(1_000_000),
            max_points_per_symbol: args.max_points_per_symbol.max(1),
            books: HashMap::new(),
            holds: HashMap::new(),
            pending: VecDeque::new(),
            stats: HashMap::new(),
            output: OutputStore::new(args.output_dir.clone())?,
        })
    }

    fn push_bbo(&mut self, symbol: String, point: BboPoint) {
        let cutoff_us = point.local_tp_us.saturating_sub(self.buffer_us);
        self.books
            .entry(symbol)
            .or_default()
            .push(point, cutoff_us, self.max_points_per_symbol);
    }

    fn handle_action(&mut self, msg: LazyTakerActionMsg) -> Result<()> {
        if msg.venue != TradingVenue::BinanceFutures.to_u8() {
            return Ok(());
        }
        let symbol = msg.symbol_str().trim().to_ascii_uppercase();
        if symbol.is_empty() {
            return Ok(());
        }
        let model_name = {
            let value = msg.model_name_str().trim();
            if value.is_empty() {
                "unknown".to_string()
            } else {
                value.to_string()
            }
        };

        match msg.action {
            LazyTakerAction::Hold => {
                match self.holds.get(&symbol) {
                    Some(existing)
                        if existing.direction == msg.direction
                            && existing.model_name == model_name
                            && existing.venue == msg.venue =>
                    {
                        self.stats.entry(model_name).or_default().repeated_holds += 1;
                        return Ok(());
                    }
                    Some(existing) => {
                        self.stats
                            .entry(existing.model_name.clone())
                            .or_default()
                            .direction_resets += 1;
                    }
                    None => {}
                }
                self.holds.insert(
                    symbol,
                    HeldAction {
                        direct_tp_us: msg.local_tp_us,
                        direction: msg.direction,
                        model_name,
                        venue: msg.venue,
                    },
                );
            }
            LazyTakerAction::Take => {
                let held = match self.holds.remove(&symbol) {
                    Some(existing)
                        if existing.direction == msg.direction
                            && existing.model_name == model_name
                            && existing.venue == msg.venue =>
                    {
                        Some(existing)
                    }
                    Some(existing) => {
                        self.stats
                            .entry(existing.model_name)
                            .or_default()
                            .direction_resets += 1;
                        None
                    }
                    None => None,
                };
                if held.is_none() {
                    self.persist_no_hold(
                        symbol,
                        model_name,
                        msg.venue,
                        msg.direction,
                        msg.local_tp_us,
                    )?;
                } else if let Some(held) = held {
                    self.pending.push_back(PendingEvaluation {
                        symbol,
                        model_name,
                        venue: msg.venue,
                        direction: msg.direction,
                        direct_tp_us: held.direct_tp_us,
                        take_tp_us: msg.local_tp_us,
                        held: true,
                    });
                }
            }
        }
        Ok(())
    }

    fn persist_no_hold(
        &mut self,
        symbol: String,
        model_name: String,
        venue: u8,
        direction: i8,
        take_tp_us: i64,
    ) -> Result<()> {
        let target_us = take_tp_us.saturating_add(self.delay_us);
        let result = EvaluationResult {
            symbol,
            model_name: model_name.clone(),
            venue,
            direction,
            category: "no_hold",
            direct_tp_us: take_tp_us,
            take_tp_us,
            direct_target_us: target_us,
            lazy_target_us: target_us,
            direct_price: None,
            lazy_price: None,
            pnl: Some(0.0),
            status: "no_hold",
        };
        let bucket = &mut self.stats.entry(model_name).or_default().no_hold;
        bucket.events += 1;
        bucket.flat += 1;
        self.output.persist(&result, self.delay_us, &self.stats)
    }

    fn process_ready(&mut self, now_us: i64) -> Result<()> {
        let mut remaining = VecDeque::with_capacity(self.pending.len());
        while let Some(item) = self.pending.pop_front() {
            let lazy_target_us = item.take_tp_us.saturating_add(self.delay_us);
            if now_us < lazy_target_us {
                remaining.push_back(item);
                continue;
            }
            self.evaluate_held(item)?;
        }
        self.pending = remaining;
        Ok(())
    }

    fn evaluate_held(&mut self, item: PendingEvaluation) -> Result<()> {
        debug_assert!(item.held);
        let direct_target_us = item.direct_tp_us.saturating_add(self.delay_us);
        let lazy_target_us = item.take_tp_us.saturating_add(self.delay_us);
        let direct_bbo = self
            .books
            .get(&item.symbol)
            .and_then(|book| book.at_or_before(direct_target_us));
        let lazy_bbo = self
            .books
            .get(&item.symbol)
            .and_then(|book| book.at_or_before(lazy_target_us));
        let direct_price = direct_bbo.map(|bbo| taker_price(bbo, item.direction));
        let lazy_price = lazy_bbo.map(|bbo| taker_price(bbo, item.direction));
        let pnl = direct_price
            .zip(lazy_price)
            .map(|(direct, lazy)| item.direction as f64 * (lazy - direct));
        let status = if pnl.is_some() { "ok" } else { "missing_bbo" };
        let result = EvaluationResult {
            symbol: item.symbol,
            model_name: item.model_name.clone(),
            venue: item.venue,
            direction: item.direction,
            category: "held",
            direct_tp_us: item.direct_tp_us,
            take_tp_us: item.take_tp_us,
            direct_target_us,
            lazy_target_us,
            direct_price,
            lazy_price,
            pnl,
            status,
        };

        let bucket = &mut self.stats.entry(item.model_name).or_default().held;
        bucket.events += 1;
        match pnl {
            Some(value) if value > 1e-12 => {
                bucket.wins += 1;
                bucket.cumulative_pnl += value;
            }
            Some(value) if value < -1e-12 => {
                bucket.losses += 1;
                bucket.cumulative_pnl += value;
            }
            Some(value) => {
                bucket.flat += 1;
                bucket.cumulative_pnl += value;
            }
            None => bucket.missing_bbo += 1,
        }
        let decided = bucket.wins + bucket.losses;
        bucket.win_rate = if decided == 0 {
            0.0
        } else {
            bucket.wins as f64 / decided as f64
        };
        self.output.persist(&result, self.delay_us, &self.stats)
    }
}

struct ActionSubscriber {
    _node: Node<ipc::Service>,
    subscriber: Subscriber<ipc::Service, [u8; LAZY_TAKER_ACTION_PAYLOAD], ()>,
}

impl ActionSubscriber {
    fn new() -> Result<Self> {
        let node_name = format!("hedge_lazy_taker_eval_{}", std::process::id());
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;
        let service_name =
            build_service_name(&format!("signal_pubs/{}", LAZY_TAKER_ACTION_CHANNEL));
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; LAZY_TAKER_ACTION_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let subscriber = service.subscriber_builder().buffer_size(256).create()?;
        Ok(Self {
            _node: node,
            subscriber,
        })
    }

    fn drain<F>(&self, mut handler: F) -> Result<usize>
    where
        F: FnMut(LazyTakerActionMsg) -> Result<()>,
    {
        let mut count = 0usize;
        while count < 1024 {
            let Some(sample) = self.subscriber.receive()? else {
                break;
            };
            if let Some(msg) = LazyTakerActionMsg::decode(sample.payload()) {
                handler(msg)?;
            }
            count += 1;
        }
        Ok(count)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    if let Some(core) = args.core {
        pin_to_core(core)?;
    }
    anyhow::ensure!(args.delay_ms > 0, "delay_ms must be > 0");
    anyhow::ensure!(args.buffer_secs > 0, "buffer_secs must be > 0");

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

    let state = Rc::new(RefCell::new(AnalyzerState::new(&args)?));
    let handler_state = state.clone();
    let handler: FrameHandler = Rc::new(move |recv_us, raw| {
        let Some((symbol, bid, ask)) = parse_book_ticker(raw) else {
            return;
        };
        handler_state.borrow_mut().push_bbo(
            symbol,
            BboPoint {
                local_tp_us: recv_us,
                bid,
                ask,
            },
        );
    });

    let subscribe_msgs = build_subscribe(&symbols);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let ws_task = tokio::task::spawn_local(run_public_ws(
        WsLoopParams {
            label: "hedge-lazy-taker-eval",
            url: BINANCE_FUTURES_WS_URL.to_string(),
            local_ip: cfg.primary_local_ip.clone(),
            remote_ip: None,
            headers: Vec::new(),
            subscribe_msgs,
            keepalive: None,
            parse_okex_notices: false,
        },
        handler,
        shutdown_rx.clone(),
    ));

    let eval_state = state.clone();
    let eval_shutdown = shutdown_rx.clone();
    let eval_task = tokio::task::spawn_local(async move {
        let result = async {
            let subscriber = ActionSubscriber::new()?;
            let mut ticker = tokio::time::interval(Duration::from_millis(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if *eval_shutdown.borrow() {
                    break;
                }
                subscriber.drain(|msg| eval_state.borrow_mut().handle_action(msg))?;
                eval_state.borrow_mut().process_ready(get_timestamp_us())?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(err) = result {
            log::error!("hedge lazy taker eval action loop failed: {err:#}");
        }
    });

    let ctrl_c_task = tokio::task::spawn_local(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                let _ = shutdown_tx.send(true);
            }
            Err(err) => log::error!("hedge lazy taker eval ctrl-c failed: {err:#}"),
        }
    });

    log::info!(
        "binance lazy taker eval started symbols={} delay_ms={} buffer_secs={} output_dir={} service=signal_pubs/{}",
        symbols.len(),
        args.delay_ms,
        args.buffer_secs,
        args.output_dir.display(),
        LAZY_TAKER_ACTION_CHANNEL,
    );

    let mut tasks = FuturesUnordered::new();
    tasks.push(ws_task);
    tasks.push(eval_task);
    tasks.push(ctrl_c_task);
    while let Some(joined) = tasks.next().await {
        if let Err(err) = joined {
            log::error!("hedge lazy taker eval join failed: {err:#}");
        }
        break;
    }
    Ok(())
}

fn build_subscribe(symbols: &[String]) -> Vec<Value> {
    let streams: Vec<String> = symbols
        .iter()
        .map(|symbol| format!("{}@bookTicker", symbol.to_ascii_lowercase()))
        .collect();
    streams
        .chunks(BINANCE_SUBSCRIBE_CHUNK)
        .enumerate()
        .map(|(index, chunk)| {
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": chunk,
                "id": index + 1,
            })
        })
        .collect()
}

fn parse_book_ticker(raw: &[u8]) -> Option<(String, f64, f64)> {
    let value: Value = serde_json::from_slice(raw).ok()?;
    let payload = value.get("data").unwrap_or(&value);
    let symbol = payload.get("s")?.as_str()?.to_ascii_uppercase();
    let bid = parse_f64(payload.get("b")?)?;
    let ask = parse_f64(payload.get("a")?)?;
    (bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0).then_some((symbol, bid, ask))
}

fn parse_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
}

fn taker_price(bbo: BboPoint, direction: i8) -> f64 {
    if direction > 0 {
        bbo.bid
    } else {
        bbo.ask
    }
}

fn parse_symbols(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect()
}

fn resolve_cfg_path(explicit: Option<&str>) -> Result<String> {
    if let Some(path) = explicit {
        return Ok(path.to_string());
    }
    if Path::new("config/mkt_cfg.yaml").exists() {
        return Ok("config/mkt_cfg.yaml".to_string());
    }
    let home = std::env::var("HOME").context("HOME not set")?;
    Ok(format!("{home}/spread_pbs/config/mkt_cfg.yaml"))
}

fn csv_text(value: &str) -> String {
    if value
        .bytes()
        .any(|byte| byte == 44 || byte == 34 || byte == 10)
    {
        format!("\"{}\"", value.replace(char::from(34), "\"\""))
    } else {
        value.to_string()
    }
}

fn fmt_optional(value: Option<f64>) -> String {
    value
        .map(|number| format!("{number:.12}"))
        .unwrap_or_default()
}
