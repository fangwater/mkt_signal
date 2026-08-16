use anyhow::{bail, Context, Result};
use bytes::Bytes;
use clap::Parser;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_parsers::msg::mkt_msg::MktMsgType;
use mkt_signal::spread_pbs::publisher::{
    DEFAULT_DAT_SERVICE_ROOT, DEFAULT_SPREAD_SERVICE_ROOT, TEST_DAT_SERVICE_ROOT,
    TEST_SPREAD_SERVICE_ROOT,
};
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "spread_pbs_compare")]
#[command(about = "Compare production and test spread_pbs IceOryx market-data channels.")]
struct Args {
    /// Venue slug, for example binance-futures.
    #[arg(long, default_value = "binance-futures")]
    venue: String,

    /// Comma-separated channels: bbo,trade,incremental,derivatives,all.
    #[arg(long, default_value = "bbo")]
    channels: String,

    /// Optional comma-separated symbol filter, for example DOGEUSDT. Empty means all symbols.
    #[arg(long, default_value = "")]
    symbols: String,

    /// Production BBO root.
    #[arg(long, default_value = DEFAULT_SPREAD_SERVICE_ROOT)]
    left_spread_root: String,

    /// Test BBO root.
    #[arg(long, default_value = TEST_SPREAD_SERVICE_ROOT)]
    right_spread_root: String,

    /// Production trade/incremental/derivatives root.
    #[arg(long, default_value = DEFAULT_DAT_SERVICE_ROOT)]
    left_dat_root: String,

    /// Test trade/incremental/derivatives root.
    #[arg(long, default_value = TEST_DAT_SERVICE_ROOT)]
    right_dat_root: String,

    /// Label used in stats for the production/current side.
    #[arg(long, default_value = "left")]
    left_label: String,

    /// Label used in stats for the test/modified side.
    #[arg(long, default_value = "right")]
    right_label: String,

    /// Print a rolling window after this many paired messages. 0 disables count-window prints.
    #[arg(long, default_value_t = 0)]
    window_matches: u64,

    /// Print win-rate buckets by exchange event timestamp. 0 disables event-time windows.
    #[arg(long, default_value_t = 60_000_000)]
    event_window_us: i64,

    /// Drain at most this many messages from one subscriber before rotating to the next.
    #[arg(long, default_value_t = 1)]
    drain_per_turn: usize,

    /// Evict unmatched messages older than this many receive-sequence steps. 0 disables age eviction.
    #[arg(long, default_value_t = 200_000)]
    max_pending_age: u64,

    /// Keep at most this many unmatched messages per side for the same event key.
    #[arg(long, default_value_t = 8)]
    max_pending_per_key: usize,

    /// Log at most this many consistency mismatches with payload details.
    #[arg(long, default_value_t = 20)]
    max_mismatch_log: u64,

    /// Open wait timeout in seconds. 0 waits forever.
    #[arg(long, default_value_t = 30)]
    open_timeout_secs: u64,

    /// Sleep when no messages are available. Default 0 yields without timer overhead.
    #[arg(long, default_value_t = 0)]
    idle_sleep_us: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum CompareChannel {
    AskBidSpread,
    Trade,
    Incremental,
    Derivatives,
}

impl CompareChannel {
    fn service_channel(self) -> &'static str {
        match self {
            Self::AskBidSpread => "ask_bid_spread",
            Self::Trade => "trade",
            Self::Incremental => "incremental",
            Self::Derivatives => "derivatives",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::AskBidSpread => "bbo",
            Self::Trade => "trade",
            Self::Incremental => "incremental",
            Self::Derivatives => "derivatives",
        }
    }

    fn uses_spread_root(self) -> bool {
        matches!(self, Self::AskBidSpread)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Side {
    Left,
    Right,
}

impl Side {
    fn other(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

struct Labels<'a> {
    left: &'a str,
    right: &'a str,
}

impl<'a> Labels<'a> {
    fn get(&self, side: Side) -> &'a str {
        match side {
            Side::Left => self.left,
            Side::Right => self.right,
        }
    }
}

#[derive(Clone, Debug)]
struct SymbolFilter {
    symbols: Vec<String>,
}

impl SymbolFilter {
    fn parse(raw: &str) -> Self {
        let mut symbols = Vec::new();
        for item in raw.split(',') {
            let symbol = item.trim();
            if symbol.is_empty() {
                continue;
            }
            let normalized = symbol.to_ascii_uppercase();
            if !symbols.contains(&normalized) {
                symbols.push(normalized);
            }
        }
        Self { symbols }
    }

    fn matches(&self, symbol: &str) -> bool {
        self.symbols.is_empty()
            || self
                .symbols
                .iter()
                .any(|configured| configured.as_str() == symbol)
    }

    fn label(&self) -> String {
        if self.symbols.is_empty() {
            "all".to_string()
        } else {
            self.symbols.join(",")
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct EventKey {
    channel: CompareChannel,
    msg_type: u32,
    symbol: String,
    k1: i64,
    k2: i64,
    k3: i64,
}

#[derive(Clone)]
struct PendingMsg {
    seq: u64,
    payload: Vec<u8>,
}

#[derive(Default)]
struct PendingQueues {
    left: VecDeque<PendingMsg>,
    right: VecDeque<PendingMsg>,
}

impl PendingQueues {
    fn queue_mut(&mut self, side: Side) -> &mut VecDeque<PendingMsg> {
        match side {
            Side::Left => &mut self.left,
            Side::Right => &mut self.right,
        }
    }

    fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty()
    }
}

#[derive(Clone)]
struct DecodedMsg {
    key: EventKey,
    event_ts_us: i64,
    payload: Vec<u8>,
}

#[derive(Clone, Default)]
struct ChannelStats {
    left_received: u64,
    right_received: u64,
    matched: u64,
    left_first: u64,
    right_first: u64,
    mismatched: u64,
    decode_errors: u64,
    pending_evicted: u64,
}

#[derive(Clone)]
struct StatsBook {
    by_channel: FastHashMap<CompareChannel, ChannelStats>,
}

impl StatsBook {
    fn new(channels: &[CompareChannel]) -> Self {
        let mut by_channel = fast_hash_map();
        for channel in channels {
            by_channel.insert(*channel, ChannelStats::default());
        }
        Self { by_channel }
    }

    fn channel_mut(&mut self, channel: CompareChannel) -> &mut ChannelStats {
        self.by_channel.entry(channel).or_default()
    }

    fn record_receive(&mut self, channel: CompareChannel, side: Side) {
        let stats = self.channel_mut(channel);
        match side {
            Side::Left => stats.left_received += 1,
            Side::Right => stats.right_received += 1,
        }
    }

    fn record_match(&mut self, channel: CompareChannel, first: Side, consistent: bool) {
        let stats = self.channel_mut(channel);
        stats.matched += 1;
        match first {
            Side::Left => stats.left_first += 1,
            Side::Right => stats.right_first += 1,
        }
        if !consistent {
            stats.mismatched += 1;
        }
    }

    fn record_decode_error(&mut self, channel: CompareChannel) {
        self.channel_mut(channel).decode_errors += 1;
    }

    fn record_evicted(&mut self, channel: CompareChannel, count: u64) {
        self.channel_mut(channel).pending_evicted += count;
    }

    fn matched(&self) -> u64 {
        self.by_channel.values().map(|stats| stats.matched).sum()
    }
}

struct ChannelSub {
    side: Side,
    channel: CompareChannel,
    service_name: String,
    subscriber: SubscriberEnum,
}

enum SubscriberEnum {
    Size128(Subscriber<ipc::Service, [u8; 128], ()>),
    Size2048(Subscriber<ipc::Service, [u8; 2048], ()>),
}

impl SubscriberEnum {
    fn receive_msg(&self) -> Result<Option<Bytes>> {
        match self {
            Self::Size128(subscriber) => receive_from_subscriber(subscriber),
            Self::Size2048(subscriber) => receive_from_subscriber(subscriber),
        }
    }
}

fn receive_from_subscriber<const SIZE: usize>(
    subscriber: &Subscriber<ipc::Service, [u8; SIZE], ()>,
) -> Result<Option<Bytes>> {
    Ok(subscriber
        .receive()?
        .map(|sample| Bytes::copy_from_slice(sample.payload())))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let channels = parse_channels(&args.channels)?;
    if channels.is_empty() {
        bail!("no compare channels selected");
    }
    let symbol_filter = SymbolFilter::parse(&args.symbols);
    let labels = Labels {
        left: &args.left_label,
        right: &args.right_label,
    };

    let node_name = format!("spread_pbs_compare_{}", std::process::id());
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let mut subscribers = Vec::with_capacity(channels.len() * 2);
    for channel in channels.iter().copied() {
        subscribers.push(
            open_channel_sub(&node, &args, channel, Side::Left).with_context(|| {
                format!(
                    "open {} subscriber for {}",
                    labels.get(Side::Left),
                    channel.label()
                )
            })?,
        );
        subscribers.push(
            open_channel_sub(&node, &args, channel, Side::Right).with_context(|| {
                format!(
                    "open {} subscriber for {}",
                    labels.get(Side::Right),
                    channel.label()
                )
            })?,
        );
    }

    println!(
        "[COMPARE] venue={} channels={} symbols={} {}_spread_root={} {}_spread_root={} {}_dat_root={} {}_dat_root={} window_matches={} event_window_us={} drain_per_turn={}",
        args.venue,
        channels
            .iter()
            .map(|channel| channel.label())
            .collect::<Vec<_>>()
            .join(","),
        symbol_filter.label(),
        labels.get(Side::Left),
        args.left_spread_root,
        labels.get(Side::Right),
        args.right_spread_root,
        labels.get(Side::Left),
        args.left_dat_root,
        labels.get(Side::Right),
        args.right_dat_root,
        args.window_matches,
        args.event_window_us,
        args.drain_per_turn,
    );
    for sub in &subscribers {
        println!(
            "[COMPARE] subscribed side={} channel={} service={}",
            labels.get(sub.side),
            sub.channel.label(),
            sub.service_name
        );
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_task = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                shutdown.store(true, Ordering::Relaxed);
            }
        })
    };

    let mut total = StatsBook::new(&channels);
    let mut window = StatsBook::new(&channels);
    let mut event_windows = fast_hash_map::<i64, StatsBook>();
    let mut max_event_bucket: Option<i64> = None;
    let mut window_start = Instant::now();
    let mut pending = fast_hash_map::<EventKey, PendingQueues>();
    let mut seq = 0u64;
    let mut mismatch_logs = 0u64;
    let mut poll_index = 0usize;
    let drain_per_turn = args.drain_per_turn.max(1);

    while !shutdown.load(Ordering::Relaxed) {
        let mut made_progress = false;
        for _ in 0..subscribers.len() {
            let idx = poll_index;
            poll_index = (poll_index + 1) % subscribers.len();
            let sub = &subscribers[idx];

            for _ in 0..drain_per_turn {
                let Some(payload) = sub.subscriber.receive_msg().with_context(|| {
                    format!(
                        "receive side={} channel={} service={}",
                        labels.get(sub.side),
                        sub.channel.label(),
                        sub.service_name
                    )
                })?
                else {
                    break;
                };
                made_progress = true;
                seq = seq.saturating_add(1);
                handle_payload(
                    sub.side,
                    sub.channel,
                    seq,
                    &payload,
                    &mut pending,
                    &mut total,
                    &mut window,
                    &mut event_windows,
                    &mut max_event_bucket,
                    &channels,
                    &labels,
                    &args,
                    &symbol_filter,
                    &mut mismatch_logs,
                );

                if args.max_pending_age > 0 && seq % 4096 == 0 {
                    evict_old_pending(
                        seq,
                        args.max_pending_age,
                        &mut pending,
                        &mut total,
                        &mut window,
                    );
                }

                if args.window_matches > 0 && window.matched() >= args.window_matches {
                    print_stats(
                        "window",
                        &channels,
                        &window,
                        &labels,
                        Some(window_start.elapsed()),
                    );
                    window = StatsBook::new(&channels);
                    window_start = Instant::now();
                }
            }
        }

        if !made_progress {
            if args.idle_sleep_us == 0 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_micros(args.idle_sleep_us));
            }
        }
    }

    shutdown_task.abort();
    evict_old_pending(seq, 0, &mut pending, &mut total, &mut window);
    flush_event_windows(&channels, &mut event_windows, &labels, None);
    print_stats("final", &channels, &total, &labels, None);
    Ok(())
}

fn parse_channels(raw: &str) -> Result<Vec<CompareChannel>> {
    let mut channels = Vec::new();
    for item in raw.split(',') {
        let normalized = item.trim().to_ascii_lowercase().replace('-', "_");
        if normalized.is_empty() {
            continue;
        }
        if normalized == "all" {
            return Ok(vec![
                CompareChannel::AskBidSpread,
                CompareChannel::Trade,
                CompareChannel::Incremental,
                CompareChannel::Derivatives,
            ]);
        }
        let channel = match normalized.as_str() {
            "bbo" | "ask_bid" | "ask_bid_spread" | "spread" => CompareChannel::AskBidSpread,
            "trade" | "trades" => CompareChannel::Trade,
            "incremental" | "inc" | "orderbook_inc" | "book" => CompareChannel::Incremental,
            "derivatives" | "derivative" | "der" | "funding" | "mark" => {
                CompareChannel::Derivatives
            }
            "latency" | "lat" => bail!("unsupported compare channel: {}", item),
            _ => bail!("unsupported compare channel: {}", item),
        };
        if !channels.contains(&channel) {
            channels.push(channel);
        }
    }
    Ok(channels)
}

fn open_channel_sub(
    node: &Node<ipc::Service>,
    args: &Args,
    channel: CompareChannel,
    side: Side,
) -> Result<ChannelSub> {
    let root = if channel.uses_spread_root() {
        match side {
            Side::Left => &args.left_spread_root,
            Side::Right => &args.right_spread_root,
        }
    } else {
        match side {
            Side::Left => &args.left_dat_root,
            Side::Right => &args.right_dat_root,
        }
    };
    let service_name = format!(
        "{}/{}/{}",
        clean_root(root)?,
        args.venue,
        channel.service_channel()
    );
    let subscriber = wait_open_subscriber(node, &service_name, channel, args.open_timeout_secs)
        .with_context(|| format!("service={}", service_name))?;
    Ok(ChannelSub {
        side,
        channel,
        service_name,
        subscriber,
    })
}

fn clean_root(root: &str) -> Result<String> {
    let root = root.trim().trim_matches('/');
    if root.is_empty() || root.contains('/') {
        bail!(
            "service root must be one non-empty path component: {:?}",
            root
        );
    }
    Ok(root.to_string())
}

fn wait_open_subscriber(
    node: &Node<ipc::Service>,
    service_name: &str,
    channel: CompareChannel,
    timeout_secs: u64,
) -> Result<SubscriberEnum> {
    let start = Instant::now();
    loop {
        match open_subscriber(node, service_name, channel) {
            Ok(subscriber) => return Ok(subscriber),
            Err(err) => {
                if timeout_secs > 0 && start.elapsed() >= Duration::from_secs(timeout_secs) {
                    return Err(err).with_context(|| {
                        format!(
                            "timed out after {}s waiting for publisher-created service",
                            timeout_secs
                        )
                    });
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn open_subscriber(
    node: &Node<ipc::Service>,
    service_name: &str,
    channel: CompareChannel,
) -> Result<SubscriberEnum> {
    match channel {
        CompareChannel::Incremental => {
            let service = node
                .service_builder(&ServiceName::new(service_name)?)
                .publish_subscribe::<[u8; 2048]>()
                .open()?;
            Ok(SubscriberEnum::Size2048(
                service.subscriber_builder().create()?,
            ))
        }
        CompareChannel::AskBidSpread | CompareChannel::Trade | CompareChannel::Derivatives => {
            let service = node
                .service_builder(&ServiceName::new(service_name)?)
                .publish_subscribe::<[u8; 128]>()
                .open()?;
            Ok(SubscriberEnum::Size128(
                service.subscriber_builder().create()?,
            ))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_payload(
    side: Side,
    channel: CompareChannel,
    seq: u64,
    raw: &[u8],
    pending: &mut FastHashMap<EventKey, PendingQueues>,
    total: &mut StatsBook,
    window: &mut StatsBook,
    event_windows: &mut FastHashMap<i64, StatsBook>,
    max_event_bucket: &mut Option<i64>,
    channels: &[CompareChannel],
    labels: &Labels<'_>,
    args: &Args,
    symbol_filter: &SymbolFilter,
    mismatch_logs: &mut u64,
) {
    let decoded = match decode_payload(channel, raw) {
        Ok(decoded) => decoded,
        Err(err) => {
            total.record_decode_error(channel);
            window.record_decode_error(channel);
            if *mismatch_logs < args.max_mismatch_log {
                *mismatch_logs += 1;
                eprintln!(
                    "[COMPARE][DECODE] side={} channel={} seq={} err={:#} raw_prefix={}",
                    labels.get(side),
                    channel.label(),
                    seq,
                    err,
                    hex_prefix(raw, 64)
                );
            }
            return;
        }
    };
    if !symbol_filter.matches(&decoded.key.symbol) {
        return;
    }

    total.record_receive(channel, side);
    window.record_receive(channel, side);

    let entry = pending.entry(decoded.key.clone()).or_default();
    let other_queue = entry.queue_mut(side.other());
    if let Some(match_index) = other_queue
        .iter()
        .position(|other| other.payload == decoded.payload)
    {
        other_queue
            .remove(match_index)
            .expect("match_index came from VecDeque::position");
        total.record_match(channel, side.other(), true);
        window.record_match(channel, side.other(), true);
        record_event_window(
            channel,
            decoded.event_ts_us,
            side.other(),
            true,
            event_windows,
            max_event_bucket,
            args.event_window_us,
            labels,
            channels,
        );
        if entry.is_empty() {
            pending.remove(&decoded.key);
        }
        return;
    }

    let same_side_queue = entry.queue_mut(side);
    same_side_queue.push_back(PendingMsg {
        seq,
        payload: decoded.payload,
    });
    if same_side_queue.len() > args.max_pending_per_key {
        same_side_queue.pop_front();
        total.record_evicted(channel, 1);
        window.record_evicted(channel, 1);
    }
}

fn decode_payload(channel: CompareChannel, raw: &[u8]) -> Result<DecodedMsg> {
    let msg_type = read_u32(raw, 0)?;
    let symbol_len = read_u32(raw, 4)? as usize;
    let symbol_start = 8usize;
    let symbol_end = symbol_start
        .checked_add(symbol_len)
        .context("symbol length overflow")?;
    anyhow::ensure!(
        raw.len() >= symbol_end,
        "payload too short for symbol: len={} symbol_end={}",
        raw.len(),
        symbol_end
    );
    let symbol = std::str::from_utf8(&raw[symbol_start..symbol_end])
        .context("symbol is not utf8")?
        .to_string();
    let base = symbol_end;

    match channel {
        CompareChannel::AskBidSpread => {
            ensure_msg_type(channel, msg_type, &[MktMsgType::AskBidSpread])?;
            let timestamp = read_i64(raw, base)?;
            let used_len = checked_used_len(base, 8 + 32)?;
            let payload = used_payload(raw, used_len)?;
            Ok(DecodedMsg {
                key: EventKey {
                    channel,
                    msg_type,
                    symbol: symbol.clone(),
                    k1: timestamp,
                    k2: 0,
                    k3: 0,
                },
                event_ts_us: timestamp,
                payload,
            })
        }
        CompareChannel::Trade => {
            ensure_msg_type(channel, msg_type, &[MktMsgType::TradeInfo])?;
            let trade_id = read_i64(raw, base)?;
            let timestamp = read_i64(raw, base + 8)?;
            let used_len = checked_used_len(base, 8 + 8 + 8 + 8 + 8)?;
            let payload = used_payload(raw, used_len)?;
            Ok(DecodedMsg {
                key: EventKey {
                    channel,
                    msg_type,
                    symbol: symbol.clone(),
                    k1: trade_id,
                    k2: timestamp,
                    k3: 0,
                },
                event_ts_us: timestamp,
                payload,
            })
        }
        CompareChannel::Incremental => {
            ensure_msg_type(channel, msg_type, &[MktMsgType::OrderBookInc])?;
            let first_update_id = read_i64(raw, base)?;
            let final_update_id = read_i64(raw, base + 8)?;
            let timestamp = read_i64(raw, base + 16)?;
            let is_snapshot = raw.get(base + 24).copied().unwrap_or(0) != 0;
            let is_last = raw.get(base + 25).copied().unwrap_or(0) != 0;
            let chunk_index = raw.get(base + 26).copied().unwrap_or(0);
            let bids_count = read_u32(raw, base + 32)? as usize;
            let asks_count = read_u32(raw, base + 36)? as usize;
            let levels_len = bids_count
                .checked_add(asks_count)
                .and_then(|count| count.checked_mul(16))
                .context("incremental levels length overflow")?;
            let used_len = checked_used_len(base, 40 + levels_len)?;
            let payload = used_payload(raw, used_len)?;
            let flags_key =
                (i64::from(is_snapshot) << 16) | (i64::from(is_last) << 8) | i64::from(chunk_index);
            Ok(DecodedMsg {
                key: EventKey {
                    channel,
                    msg_type,
                    symbol: symbol.clone(),
                    k1: first_update_id,
                    k2: final_update_id,
                    k3: flags_key,
                },
                event_ts_us: timestamp,
                payload,
            })
        }
        CompareChannel::Derivatives => decode_derivatives(channel, msg_type, symbol, base, raw),
    }
}

fn decode_derivatives(
    channel: CompareChannel,
    msg_type: u32,
    symbol: String,
    base: usize,
    raw: &[u8],
) -> Result<DecodedMsg> {
    ensure_msg_type(
        channel,
        msg_type,
        &[
            MktMsgType::MarkPrice,
            MktMsgType::IndexPrice,
            MktMsgType::FundingRate,
            MktMsgType::LiquidationOrder,
        ],
    )?;

    match msg_type {
        t if t == MktMsgType::MarkPrice as u32 || t == MktMsgType::IndexPrice as u32 => {
            let timestamp = read_i64(raw, base + 8)?;
            let used_len = checked_used_len(base, 16)?;
            let payload = used_payload(raw, used_len)?;
            Ok(DecodedMsg {
                key: EventKey {
                    channel,
                    msg_type,
                    symbol: symbol.clone(),
                    k1: timestamp,
                    k2: 0,
                    k3: 0,
                },
                event_ts_us: timestamp,
                payload,
            })
        }
        t if t == MktMsgType::FundingRate as u32 => {
            let next_funding_time = read_i64(raw, base + 8)?;
            let timestamp = read_i64(raw, base + 16)?;
            let used_len = checked_used_len(base, 24)?;
            let payload = used_payload(raw, used_len)?;
            Ok(DecodedMsg {
                key: EventKey {
                    channel,
                    msg_type,
                    symbol: symbol.clone(),
                    k1: timestamp,
                    k2: next_funding_time,
                    k3: 0,
                },
                event_ts_us: timestamp,
                payload,
            })
        }
        t if t == MktMsgType::LiquidationOrder as u32 => {
            let timestamp = read_i64(raw, base + 17)?;
            let used_len = checked_used_len(base, 25)?;
            let payload = used_payload(raw, used_len)?;
            Ok(DecodedMsg {
                key: EventKey {
                    channel,
                    msg_type,
                    symbol: symbol.clone(),
                    k1: timestamp,
                    k2: 0,
                    k3: 0,
                },
                event_ts_us: timestamp,
                payload,
            })
        }
        _ => bail!("unsupported derivatives msg_type={}", msg_type),
    }
}

fn ensure_msg_type(channel: CompareChannel, msg_type: u32, expected: &[MktMsgType]) -> Result<()> {
    if expected.iter().any(|item| *item as u32 == msg_type) {
        return Ok(());
    }
    bail!(
        "channel={} got unexpected msg_type={} expected={:?}",
        channel.label(),
        msg_type,
        expected.iter().map(|item| *item as u32).collect::<Vec<_>>()
    )
}

fn checked_used_len(base: usize, tail: usize) -> Result<usize> {
    base.checked_add(tail).context("payload length overflow")
}

fn used_payload(raw: &[u8], used_len: usize) -> Result<Vec<u8>> {
    anyhow::ensure!(
        raw.len() >= used_len,
        "payload too short: len={} used_len={}",
        raw.len(),
        used_len
    );
    Ok(raw[..used_len].to_vec())
}

fn read_u32(data: &[u8], offset: usize) -> Result<u32> {
    let slice = data
        .get(offset..offset + 4)
        .with_context(|| format!("read_u32 out of bounds offset={}", offset))?;
    Ok(u32::from_le_bytes(slice.try_into()?))
}

fn read_i64(data: &[u8], offset: usize) -> Result<i64> {
    let slice = data
        .get(offset..offset + 8)
        .with_context(|| format!("read_i64 out of bounds offset={}", offset))?;
    Ok(i64::from_le_bytes(slice.try_into()?))
}

fn evict_old_pending(
    seq: u64,
    max_age: u64,
    pending: &mut FastHashMap<EventKey, PendingQueues>,
    total: &mut StatsBook,
    window: &mut StatsBook,
) {
    let min_seq = seq.saturating_sub(max_age);
    let mut evicted_by_channel = fast_hash_map::<CompareChannel, u64>();
    pending.retain(|key, queues| {
        let left_before = queues.left.len();
        let right_before = queues.right.len();
        while queues
            .left
            .front()
            .is_some_and(|msg| max_age == 0 || msg.seq < min_seq)
        {
            queues.left.pop_front();
        }
        while queues
            .right
            .front()
            .is_some_and(|msg| max_age == 0 || msg.seq < min_seq)
        {
            queues.right.pop_front();
        }
        let evicted = (left_before - queues.left.len()) + (right_before - queues.right.len());
        if evicted > 0 {
            *evicted_by_channel.entry(key.channel).or_default() += evicted as u64;
        }
        !queues.is_empty()
    });
    for (channel, count) in evicted_by_channel {
        total.record_evicted(channel, count);
        window.record_evicted(channel, count);
    }
}

#[allow(clippy::too_many_arguments)]
fn record_event_window(
    channel: CompareChannel,
    event_ts_us: i64,
    first: Side,
    consistent: bool,
    event_windows: &mut FastHashMap<i64, StatsBook>,
    max_event_bucket: &mut Option<i64>,
    event_window_us: i64,
    labels: &Labels<'_>,
    channels: &[CompareChannel],
) {
    if event_window_us <= 0 || event_ts_us <= 0 {
        return;
    }
    let bucket = event_ts_us.div_euclid(event_window_us) * event_window_us;
    let bucket_stats = event_windows
        .entry(bucket)
        .or_insert_with(|| StatsBook::new(channels));
    bucket_stats.record_receive(channel, Side::Left);
    bucket_stats.record_receive(channel, Side::Right);
    bucket_stats.record_match(channel, first, consistent);

    let previous_max = *max_event_bucket;
    let newest = previous_max.map_or(bucket, |max| max.max(bucket));
    *max_event_bucket = Some(newest);
    if previous_max.is_some_and(|old| newest > old) {
        let flush_before = newest.saturating_sub(event_window_us);
        flush_event_windows(channels, event_windows, labels, Some(flush_before));
    }
}

fn flush_event_windows(
    channels: &[CompareChannel],
    event_windows: &mut FastHashMap<i64, StatsBook>,
    labels: &Labels<'_>,
    flush_through: Option<i64>,
) {
    let mut ready: Vec<i64> = event_windows
        .keys()
        .copied()
        .filter(|bucket| flush_through.is_none_or(|limit| *bucket <= limit))
        .collect();
    ready.sort_unstable();
    for bucket in ready {
        if let Some(stats) = event_windows.remove(&bucket) {
            let label = format!("event_window_start_us={}", bucket);
            print_stats(&label, channels, &stats, labels, None);
        }
    }
}

fn print_stats(
    label: &str,
    channels: &[CompareChannel],
    stats: &StatsBook,
    labels: &Labels<'_>,
    elapsed: Option<Duration>,
) {
    let elapsed_text = elapsed
        .map(|duration| format!(" elapsed_secs={:.3}", duration.as_secs_f64()))
        .unwrap_or_default();
    println!("[COMPARE][{}]{}", label, elapsed_text);
    for channel in channels {
        let s = stats.by_channel.get(channel).cloned().unwrap_or_default();
        let left_pct = pct(s.left_first, s.matched);
        let right_pct = pct(s.right_first, s.matched);
        println!(
            "[COMPARE][{}] channel={} matched={} {}_first={} ({:.2}%) {}_first={} ({:.2}%) mismatched={} recv_{}={} recv_{}={} decode_errors={} pending_evicted={}",
            label,
            channel.label(),
            s.matched,
            labels.get(Side::Left),
            s.left_first,
            left_pct,
            labels.get(Side::Right),
            s.right_first,
            right_pct,
            s.mismatched,
            labels.get(Side::Left),
            s.left_received,
            labels.get(Side::Right),
            s.right_received,
            s.decode_errors,
            s.pending_evicted,
        );
    }
}

fn pct(n: u64, d: u64) -> f64 {
    if d == 0 {
        0.0
    } else {
        n as f64 * 100.0 / d as f64
    }
}

fn hex_prefix(data: &[u8], max_len: usize) -> String {
    data.iter()
        .take(max_len)
        .map(|byte| format!("{:02x}", byte))
        .collect::<Vec<_>>()
        .join("")
}
