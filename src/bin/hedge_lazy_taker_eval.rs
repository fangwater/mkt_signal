use anyhow::{Context, Result};
use clap::Parser;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_parsers::msg::mkt_msg::{get_msg_type, AskBidSpreadMsg, MktMsgType};
use order_common::TradingVenue;
use runtime_common::affinity::pin_to_core;
use runtime_common::ipc_service_name::build_service_name;
use runtime_common::time_util::get_timestamp_us;
use serde::Serialize;
use signal_common::lazy_taker_action::{
    LazyTakerAction, LazyTakerActionMsg, LAZY_TAKER_ACTION_CHANNEL, LAZY_TAKER_ACTION_PAYLOAD,
};
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::time::Duration;

const BBO_SERVICE: &str = "spread_pbs/binance-futures/ask_bid_spread";
const BBO_PAYLOAD: usize = 128;
const BBO_DRAIN_BUDGET: usize = 8192;
const EVENTS_HEADER: &str = "take_tp_us,direct_tp_us,hold_us,symbol,model_name,venue,direction,category,direct_target_us,lazy_target_us,direct_price,lazy_price,hold_count,return_rate,position,pnl,status";

#[derive(Parser)]
#[command(name = "hedge_lazy_taker_eval")]
#[command(about = "Evaluate unit-size lazy taker actions against Binance futures BBO.")]
struct Args {
    #[arg(long)]
    core: Option<usize>,

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
    hold_tp_us: Vec<i64>,
    direction: i8,
    model_name: String,
    venue: u8,
}

struct PendingEvaluation {
    symbol: String,
    model_name: String,
    venue: u8,
    direction: i8,
    hold_tp_us: Vec<i64>,
    take_tp_us: i64,
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
    hold_count: usize,
    return_rate: Option<f64>,
    position: f64,
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
        if !needs_header {
            let mut header = String::new();
            BufReader::new(File::open(&events_path).with_context(|| {
                format!("open events header failed: {}", events_path.display())
            })?)
            .read_line(&mut header)
            .with_context(|| format!("read events header failed: {}", events_path.display()))?;
            anyhow::ensure!(
                header.trim_end_matches(['\r', '\n']) == EVENTS_HEADER,
                "events CSV schema mismatch: convert {} to the stepped-return schema before starting",
                events_path.display()
            );
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&events_path)
            .with_context(|| format!("open events file failed: {}", events_path.display()))?;
        let mut events = BufWriter::new(file);
        if needs_header {
            writeln!(events, "{EVENTS_HEADER}")?;
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
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
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
            result.hold_count,
            fmt_optional(result.return_rate),
            result.position,
            fmt_optional(result.pnl),
            result.status,
        )?;
        self.events.flush()?;

        let summary = serde_json::json!({
            "updated_tp_us": get_timestamp_us(),
            "delay_us": delay_us,
            "position_mode": "each_hold_adds_one_then_take_clears",
            "pnl_basis": "sum_of_segment_return_rate_x_active_count",
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
                match self.holds.get_mut(&symbol) {
                    Some(existing)
                        if existing.direction == msg.direction
                            && existing.model_name == model_name
                            && existing.venue == msg.venue =>
                    {
                        existing.hold_tp_us.push(msg.local_tp_us);
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
                        hold_tp_us: vec![msg.local_tp_us],
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
                        hold_tp_us: held.hold_tp_us,
                        take_tp_us: msg.local_tp_us,
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
            hold_count: 0,
            return_rate: Some(0.0),
            position: 0.0,
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
        debug_assert!(!item.hold_tp_us.is_empty());
        let direct_tp_us = item.hold_tp_us[0];
        let direct_target_us = direct_tp_us.saturating_add(self.delay_us);
        let lazy_target_us = item.take_tp_us.saturating_add(self.delay_us);
        let hold_count = item.hold_tp_us.len();

        let mut target_us = item
            .hold_tp_us
            .iter()
            .map(|tp_us| tp_us.saturating_add(self.delay_us))
            .collect::<Vec<_>>();
        target_us.push(lazy_target_us);

        let prices = self.books.get(&item.symbol).and_then(|book| {
            target_us
                .iter()
                .map(|target| {
                    book.at_or_before(*target)
                        .map(|bbo| taker_price(bbo, item.direction))
                })
                .collect::<Option<Vec<_>>>()
        });
        let direct_price = prices.as_ref().and_then(|values| values.first().copied());
        let lazy_price = prices.as_ref().and_then(|values| values.last().copied());
        let stepped = prices
            .as_deref()
            .and_then(|values| stepped_position_result(values, item.direction));
        let return_rate = stepped.map(|result| result.average_return_rate);
        let position = hold_count as f64;
        let pnl = stepped.map(|result| result.pnl);
        let status = if pnl.is_some() { "ok" } else { "missing_bbo" };
        let result = EvaluationResult {
            symbol: item.symbol,
            model_name: item.model_name.clone(),
            venue: item.venue,
            direction: item.direction,
            category: "held",
            direct_tp_us,
            take_tp_us: item.take_tp_us,
            direct_target_us,
            lazy_target_us,
            direct_price,
            lazy_price,
            hold_count,
            return_rate,
            position,
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

struct BboSubscriber {
    _node: Node<ipc::Service>,
    subscriber: Subscriber<ipc::Service, [u8; BBO_PAYLOAD], ()>,
}

impl BboSubscriber {
    fn new() -> Result<Self> {
        let node_name = format!("hedge_lazy_taker_eval_bbo_{}", std::process::id());
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;
        let service_name = build_service_name(BBO_SERVICE);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; BBO_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(64)
            .history_size(100)
            .subscriber_max_buffer_size(8192)
            .open_or_create()?;
        let subscriber = service.subscriber_builder().create()?;
        Ok(Self {
            _node: node,
            subscriber,
        })
    }

    fn drain<F>(&self, mut handler: F) -> Result<usize>
    where
        F: FnMut(String, BboPoint),
    {
        let mut count = 0usize;
        while count < BBO_DRAIN_BUDGET {
            let Some(sample) = self.subscriber.receive()? else {
                break;
            };
            if let Some((symbol, point)) = decode_bbo(sample.payload(), get_timestamp_us()) {
                handler(symbol, point);
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
    let mut state = AnalyzerState::new(&args)?;
    let bbo_subscriber = BboSubscriber::new()?;
    let action_subscriber = ActionSubscriber::new()?;
    let mut ticker = tokio::time::interval(Duration::from_millis(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    log::info!(
        "binance lazy taker eval started delay_ms={} buffer_secs={} output_dir={} bbo_service={} action_service=signal_pubs/{}",
        args.delay_ms,
        args.buffer_secs,
        args.output_dir.display(),
        BBO_SERVICE,
        LAZY_TAKER_ACTION_CHANNEL,
    );

    loop {
        tokio::select! {
            result = &mut ctrl_c => {
                result.context("install ctrl-c handler failed")?;
                break;
            }
            _ = ticker.tick() => {
                bbo_subscriber.drain(|symbol, point| state.push_bbo(symbol, point))?;
                action_subscriber.drain(|msg| state.handle_action(msg))?;
                state.process_ready(get_timestamp_us())?;
            }
        }
    }
    Ok(())
}

fn decode_bbo(payload: &[u8], local_tp_us: i64) -> Option<(String, BboPoint)> {
    if payload.len() < 8 || get_msg_type(payload) != MktMsgType::AskBidSpread {
        return None;
    }
    let symbol_len = u32::from_le_bytes(payload.get(4..8)?.try_into().ok()?) as usize;
    if payload.len() < 8 + symbol_len + 40 {
        return None;
    }
    let symbol = AskBidSpreadMsg::get_symbol(payload)
        .trim()
        .to_ascii_uppercase();
    let bid = AskBidSpreadMsg::get_bid_price(payload);
    let ask = AskBidSpreadMsg::get_ask_price(payload);
    if symbol.is_empty() || !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0 {
        return None;
    }
    Some((
        symbol,
        BboPoint {
            local_tp_us,
            bid,
            ask,
        },
    ))
}

fn taker_price(bbo: BboPoint, direction: i8) -> f64 {
    if direction > 0 {
        bbo.bid
    } else {
        bbo.ask
    }
}

#[derive(Debug, Clone, Copy)]
struct SteppedPositionResult {
    average_return_rate: f64,
    pnl: f64,
}

fn stepped_position_result(prices: &[f64], direction: i8) -> Option<SteppedPositionResult> {
    let hold_count = prices.len().checked_sub(1)?;
    if hold_count == 0 || !matches!(direction, -1 | 1) {
        return None;
    }
    let mut pnl = 0.0;
    for (index, segment) in prices.windows(2).enumerate() {
        let [start, end] = segment else {
            unreachable!("windows(2) always yields pairs");
        };
        if !start.is_finite() || !end.is_finite() || *start <= 0.0 || *end <= 0.0 {
            return None;
        }
        let return_rate = direction as f64 * (end - start) / start;
        pnl += return_rate * (index + 1) as f64;
    }
    Some(SteppedPositionResult {
        average_return_rate: pnl / hold_count as f64,
        pnl,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_bbo_ipc_payload_with_local_receive_time() {
        let encoded =
            AskBidSpreadMsg::create("btcusdt".to_string(), 123, 100.0, 2.0, 101.0, 3.0).to_bytes();
        let mut payload = [0u8; BBO_PAYLOAD];
        payload[..encoded.len()].copy_from_slice(&encoded);

        let (symbol, point) = decode_bbo(&payload, 456).expect("valid BBO");
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(point.local_tp_us, 456);
        assert_eq!(point.bid, 100.0);
        assert_eq!(point.ask, 101.0);
    }

    #[test]
    fn sell_hold_count_uses_stepped_segment_positions() {
        let result = stepped_position_result(&[100.0, 101.0, 102.0], 1).unwrap();
        let expected = (101.0 - 100.0) / 100.0 + 2.0 * (102.0 - 101.0) / 101.0;
        assert!((result.pnl - expected).abs() < 1e-12);
        assert!((result.average_return_rate - expected / 2.0).abs() < 1e-12);
    }

    #[test]
    fn buy_hold_count_uses_stepped_segment_positions() {
        let result = stepped_position_result(&[100.0, 99.0, 98.0], -1).unwrap();
        let expected = (100.0 - 99.0) / 100.0 + 2.0 * (99.0 - 98.0) / 99.0;
        assert!((result.pnl - expected).abs() < 1e-12);
        assert!((result.average_return_rate - expected / 2.0).abs() < 1e-12);
    }
}
