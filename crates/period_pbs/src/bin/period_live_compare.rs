use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use ipc_common::iceoryx_subscriber::{ChannelType, MultiChannelSubscriber, SubscribeParams};
use period_pbs::collector::{CollectorConfig, CompletedPeriod, PeriodCollector};
use period_pbs::decode::{
    decode_market_msg, peek_market_msg, validate_trade_side, DecodedMarketMsg, MarketMsgKind,
};
use period_pbs::kafka::{
    decode_period_payload, format_rewrites, KafkaConsumerConfig, RawKafkaConsumer,
};
use period_pbs::pb;
use period_pbs::period::{
    period_lower_bound_ms, period_upper_bound_ms, DEFAULT_DELAY_MS, DEFAULT_PERIOD_MS,
};
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(name = "period_live_compare")]
#[command(about = "Compare local IPC-cut PeriodMessage with reference PeriodMessage by period.")]
struct Args {
    /// TOML config path.
    #[arg(long, default_value = "config/period_live_compare.toml")]
    config: PathBuf,

    /// Stop after this many matched stream+period rows.
    #[arg(long)]
    compare_periods: Option<usize>,

    /// Stop after this many seconds even if not enough rows have matched.
    #[arg(long)]
    max_wait_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct StreamConfig {
    venue: String,
    topic: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct LiveCompareConfig {
    service_root: String,
    period_ms: i64,
    delay_ms: i64,
    poll_batch: usize,
    idle_sleep_us: u64,
    compare_periods: usize,
    max_wait_secs: u64,
    skip_initial_matches: usize,
    dump_extra_limit: usize,
    compare_symbols: Vec<String>,
    streams: Vec<StreamConfig>,
    kafka: KafkaConsumerConfig,
}

impl Default for LiveCompareConfig {
    fn default() -> Self {
        let mut kafka = KafkaConsumerConfig::default();
        kafka.poll_timeout_ms = 0;
        Self {
            service_root: "dat_pbs".to_string(),
            period_ms: DEFAULT_PERIOD_MS,
            delay_ms: DEFAULT_DELAY_MS,
            poll_batch: 65536,
            idle_sleep_us: 0,
            compare_periods: 10,
            max_wait_secs: 90,
            skip_initial_matches: 0,
            dump_extra_limit: 32,
            compare_symbols: default_compare_symbols(),
            streams: vec![
                StreamConfig {
                    venue: "binance-futures".to_string(),
                    topic: "binance-futures".to_string(),
                },
                StreamConfig {
                    venue: "binance-margin".to_string(),
                    topic: "binance-spot".to_string(),
                },
            ],
            kafka,
        }
    }
}

#[derive(Debug, Clone)]
struct PeriodStats {
    ts: i64,
    post_ts: i64,
    symbols: usize,
    trades: usize,
    incs: usize,
    pb_bytes: usize,
    per_symbol: BTreeMap<String, SymbolStats>,
}

#[derive(Debug, Clone, Default)]
struct SymbolStats {
    present: bool,
    trades: usize,
    incs: usize,
    trade_keys: Vec<RecordKey>,
    inc_keys: Vec<RecordKey>,
    inc_ts_keys: Vec<RecordKey>,
}

#[derive(Debug, Clone, Default)]
struct RecordKey {
    match_key: String,
    display: String,
    timestamp_ms: i64,
}

#[derive(Debug, Default, Clone)]
struct CompareEntry {
    local: Option<PeriodStats>,
    kafka: Option<PeriodStats>,
}

#[derive(Debug, Clone)]
struct CompareRow {
    stream: String,
    period: i64,
    local: PeriodStats,
    kafka: PeriodStats,
}

#[derive(Debug, Clone)]
struct LocalLateCompletion {
    stream: String,
    period: i64,
    stats: PeriodStats,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let mut config = load_config(&args.config)?;
    if let Some(compare_periods) = args.compare_periods {
        config.compare_periods = compare_periods;
    }
    if let Some(max_wait_secs) = args.max_wait_secs {
        config.max_wait_secs = max_wait_secs;
    }

    let mut subscriber = MultiChannelSubscriber::new("period_live_compare")?;
    let mut params = Vec::with_capacity(config.streams.len() * 2);
    for stream in &config.streams {
        params.push(SubscribeParams {
            service_root: Some(config.service_root.clone()),
            topic_prefix: stream.venue.clone(),
            channel: ChannelType::Trade,
        });
        params.push(SubscribeParams {
            service_root: Some(config.service_root.clone()),
            topic_prefix: stream.venue.clone(),
            channel: ChannelType::Incremental,
        });
    }
    subscriber.subscribe_channels(params)?;

    let mut collectors: HashMap<String, PeriodCollector> = config
        .streams
        .iter()
        .map(|stream| {
            let collector_config = CollectorConfig {
                period_ms: config.period_ms,
                delay_ms: config.delay_ms,
                poster_id: format!(
                    "period_live_compare_{}",
                    sanitize_node_component(&stream.venue)
                ),
            };
            (stream.venue.clone(), PeriodCollector::new(collector_config))
        })
        .collect();

    let consumer = RawKafkaConsumer::new(&config.kafka)?;
    let topic_to_venue: HashMap<String, String> = config
        .streams
        .iter()
        .map(|stream| (stream.topic.clone(), stream.venue.clone()))
        .collect();

    let target_rows = config.compare_periods + config.skip_initial_matches;

    log::info!(
        "period_live_compare started streams={} symbols={} service_root={} period_ms={} delay_ms={} reference_source_brokers={} reference_source_topics={} reference_poll_timeout_ms={} rewrites={} compare_rows={} skip_initial_matches={} target_rows={} max_wait_secs={}",
        config
            .streams
            .iter()
            .map(|stream| format!("{}<={}", stream.venue, stream.topic))
            .collect::<Vec<_>>()
            .join(","),
        config.compare_symbols.join(","),
        config.service_root,
        config.period_ms,
        config.delay_ms,
        config.kafka.brokers,
        config.kafka.topics.join(","),
        config.kafka.poll_timeout_ms,
        format_rewrites(&config.kafka.broker_addr_rewrites),
        config.compare_periods,
        config.skip_initial_matches,
        target_rows,
        config.max_wait_secs,
    );

    let deadline = Instant::now() + Duration::from_secs(config.max_wait_secs);
    let mut entries: BTreeMap<(String, i64), CompareEntry> = BTreeMap::new();
    let mut matched_keys = HashSet::new();
    let mut rows = Vec::with_capacity(target_rows);
    let mut local_late_completions = Vec::new();

    while rows.len() < target_rows && Instant::now() < deadline {
        let mut made_progress = false;

        for stream in &config.streams {
            let trade_count = subscriber.poll_channel_from_with(
                &config.service_root,
                &stream.venue,
                &ChannelType::Trade,
                Some(config.poll_batch),
                |message| {
                    process_local_market_message(
                        &stream.venue,
                        "trade",
                        message,
                        &mut collectors,
                        &mut entries,
                        &mut matched_keys,
                        &mut rows,
                        &mut local_late_completions,
                        target_rows,
                        &config.compare_symbols,
                    )
                },
            )?;
            if trade_count > 0 {
                made_progress = true;
            }

            let inc_count = subscriber.poll_channel_from_with(
                &config.service_root,
                &stream.venue,
                &ChannelType::Incremental,
                Some(config.poll_batch),
                |message| {
                    process_local_market_message(
                        &stream.venue,
                        "incremental",
                        message,
                        &mut collectors,
                        &mut entries,
                        &mut matched_keys,
                        &mut rows,
                        &mut local_late_completions,
                        target_rows,
                        &config.compare_symbols,
                    )
                },
            )?;
            if inc_count > 0 {
                made_progress = true;
            }
        }

        while let Some(kafka_result) = consumer.poll(config.kafka.poll_timeout_ms) {
            made_progress = true;
            match kafka_result {
                Ok(message) => {
                    if let Some(venue) = topic_to_venue.get(&message.topic) {
                        match decode_period_payload(&message.payload, config.kafka.payload_compression) {
                            Ok((_compression, pb_bytes, period)) => {
                                handle_kafka_period(
                                    venue,
                                    &message.topic,
                                    message.partition,
                                    message.offset,
                                    message.payload.len(),
                                    pb_bytes,
                                    period,
                                    &mut entries,
                                    &mut matched_keys,
                                    &mut rows,
                                    target_rows,
                                    &config.compare_symbols,
                                );
                            }
                            Err(err) => log::error!(
                                "decode reference PeriodMessage failed topic={} partition={} offset={} bytes={} err={:#}",
                                message.topic,
                                message.partition,
                                message.offset,
                                message.payload.len(),
                                err
                            ),
                        }
                    } else {
                        log::warn!(
                            "ignore reference topic not in compare streams: {}",
                            message.topic
                        );
                    }
                }
                Err(err) => log::error!("reference source poll error: {:#}", err),
            }
        }

        if !made_progress && config.idle_sleep_us > 0 {
            thread::sleep(Duration::from_micros(config.idle_sleep_us));
        }
    }

    let compared_rows: Vec<CompareRow> = rows
        .iter()
        .skip(config.skip_initial_matches)
        .take(config.compare_periods)
        .cloned()
        .collect();
    print_compare_table(&compared_rows, config.dump_extra_limit);
    print_local_late_completion_table(&local_late_completions, config.dump_extra_limit);
    if compared_rows.len() < config.compare_periods {
        log::warn!(
            "period_live_compare timed out with {}/{} compared rows after skipping {} initial matches",
            compared_rows.len(),
            config.compare_periods,
            config.skip_initial_matches
        );
    }

    Ok(())
}

fn load_config(path: &PathBuf) -> Result<LiveCompareConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read live compare config {}", path.display()))?;
    let mut config: LiveCompareConfig = toml::from_str(&text)
        .with_context(|| format!("parse live compare config {}", path.display()))?;
    if config.streams.is_empty() {
        anyhow::bail!("live compare config {} has empty streams", path.display());
    }
    config.kafka.topics = config
        .streams
        .iter()
        .map(|stream| stream.topic.clone())
        .collect();
    if config.kafka.poll_timeout_ms != 0 {
        log::warn!(
            "period_live_compare overrides reference source poll_timeout_ms={} to 0 for busy pull",
            config.kafka.poll_timeout_ms
        );
        config.kafka.poll_timeout_ms = 0;
    }
    config.compare_symbols = normalize_compare_symbols(config.compare_symbols);
    if config.compare_symbols.is_empty() {
        anyhow::bail!(
            "live compare config {} has empty compare_symbols",
            path.display()
        );
    }
    config.kafka.validate()?;
    Ok(config)
}

#[allow(clippy::too_many_arguments)]
fn process_local_market_message(
    venue: &str,
    channel_label: &str,
    message: &[u8],
    collectors: &mut HashMap<String, PeriodCollector>,
    entries: &mut BTreeMap<(String, i64), CompareEntry>,
    matched_keys: &mut HashSet<(String, i64)>,
    rows: &mut Vec<CompareRow>,
    local_late_completions: &mut Vec<LocalLateCompletion>,
    target_rows: usize,
    compare_symbols: &[String],
) -> Result<()> {
    let (kind, symbol) = peek_market_msg(message)
        .with_context(|| format!("peek local {} venue={}", channel_label, venue))?;
    if kind == MarketMsgKind::Other || !is_compare_symbol(symbol, compare_symbols) {
        return Ok(());
    }

    let completed = match decode_market_msg(message)
        .with_context(|| format!("decode local {} venue={}", channel_label, venue))?
    {
        DecodedMarketMsg::Trade(trade) => {
            if !is_compare_symbol(&trade.symbol, compare_symbols) {
                Vec::new()
            } else {
                validate_trade_side(trade.side)?;
                collectors
                    .get_mut(venue)
                    .expect("collector exists")
                    .push_trade(trade)?
            }
        }
        DecodedMarketMsg::Incremental(inc) => {
            if !is_compare_symbol(&inc.symbol, compare_symbols) {
                Vec::new()
            } else {
                collectors
                    .get_mut(venue)
                    .expect("collector exists")
                    .push_incremental(inc)?
            }
        }
        DecodedMarketMsg::Other => Vec::new(),
    };
    handle_local_completed(
        venue,
        completed,
        entries,
        matched_keys,
        rows,
        local_late_completions,
        target_rows,
        compare_symbols,
    );
    Ok(())
}

fn handle_local_completed(
    venue: &str,
    completed: Vec<CompletedPeriod>,
    entries: &mut BTreeMap<(String, i64), CompareEntry>,
    matched_keys: &mut HashSet<(String, i64)>,
    rows: &mut Vec<CompareRow>,
    local_late_completions: &mut Vec<LocalLateCompletion>,
    target_rows: usize,
    compare_symbols: &[String],
) {
    for period in completed {
        let key = (venue.to_string(), period.period);
        let stats = PeriodStats::from_local(&period, compare_symbols);
        if matched_keys.contains(&key) {
            local_late_completions.push(LocalLateCompletion {
                stream: key.0,
                period: key.1,
                stats,
            });
            continue;
        }
        let entry = entries.entry(key.clone()).or_default();
        entry.local = Some(stats);
        maybe_record_match(key, entry, matched_keys, rows, target_rows);
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_kafka_period(
    venue: &str,
    _topic: &str,
    _partition: i32,
    _offset: i64,
    _compressed_bytes: usize,
    _pb_bytes: usize,
    period: pb::PeriodMessage,
    entries: &mut BTreeMap<(String, i64), CompareEntry>,
    matched_keys: &mut HashSet<(String, i64)>,
    rows: &mut Vec<CompareRow>,
    target_rows: usize,
    compare_symbols: &[String],
) {
    let key = (venue.to_string(), period.period);
    let entry = entries.entry(key.clone()).or_default();
    entry.kafka = Some(PeriodStats::from_reference(&period, compare_symbols));
    maybe_record_match(key, entry, matched_keys, rows, target_rows);
}

fn maybe_record_match(
    key: (String, i64),
    entry: &CompareEntry,
    matched_keys: &mut HashSet<(String, i64)>,
    rows: &mut Vec<CompareRow>,
    target_rows: usize,
) {
    if rows.len() >= target_rows || matched_keys.contains(&key) {
        return;
    }
    let (Some(local), Some(kafka)) = (&entry.local, &entry.kafka) else {
        return;
    };
    matched_keys.insert(key.clone());
    rows.push(CompareRow {
        stream: key.0,
        period: key.1,
        local: local.clone(),
        kafka: kafka.clone(),
    });
}

impl PeriodStats {
    fn from_local(period: &CompletedPeriod, compare_symbols: &[String]) -> Self {
        Self::from_message(&period.message, compare_symbols)
    }

    fn from_reference(period: &pb::PeriodMessage, compare_symbols: &[String]) -> Self {
        Self::from_message(period, compare_symbols)
    }

    fn from_message(period: &pb::PeriodMessage, compare_symbols: &[String]) -> Self {
        let mut per_symbol = BTreeMap::new();
        let mut filtered_infos = Vec::new();

        for symbol in compare_symbols {
            if let Some(info) = period
                .symbol_infos
                .iter()
                .find(|info| info.symbol.eq_ignore_ascii_case(symbol))
            {
                per_symbol.insert(
                    symbol.clone(),
                    SymbolStats {
                        present: true,
                        trades: info.trades.len(),
                        incs: info.incs.len(),
                        trade_keys: info.trades.iter().map(trade_key).collect(),
                        inc_keys: info.incs.iter().map(inc_key).collect(),
                        inc_ts_keys: info.incs.iter().map(inc_timestamp_key).collect(),
                    },
                );
                filtered_infos.push(info.clone());
            } else {
                per_symbol.insert(symbol.clone(), SymbolStats::default());
            }
        }

        let symbols = per_symbol.values().filter(|stats| stats.present).count();
        let trades = per_symbol.values().map(|stats| stats.trades).sum();
        let incs = per_symbol.values().map(|stats| stats.incs).sum();
        let filtered_message = pb::PeriodMessage {
            period: period.period,
            ts: period.ts,
            post_ts: period.post_ts,
            poster_id: period.poster_id.clone(),
            symbol_infos: filtered_infos,
        };

        Self {
            ts: period.ts,
            post_ts: period.post_ts,
            symbols,
            trades,
            incs,
            pb_bytes: pb::encode_period_message(&filtered_message).len(),
            per_symbol,
        }
    }
}

fn print_compare_table(rows: &[CompareRow], dump_extra_limit: usize) {
    const STREAM_W: usize = 15;
    const PERIOD_W: usize = 10;
    const TS_W: usize = 13;
    const NUM_W: usize = 8;
    const PB_W: usize = 9;
    let width = STREAM_W + PERIOD_W + TS_W * 2 + NUM_W * 9 + PB_W * 3 + 28;
    let line = "-".repeat(width);

    println!("+{}+", line);
    println!(
        "|{:^width$}|",
        "PeriodMessage PB Content Compare",
        width = width
    );
    println!("+{}+", line);
    println!(
        "| {:<STREAM_W$} | {:>PERIOD_W$} | {:>TS_W$} | {:>TS_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>PB_W$} | {:>PB_W$} | {:>PB_W$} |",
        "Stream",
        "Period",
        "LocalTs",
        "RefTs",
        "LSym",
        "RefSym",
        "dSym",
        "LTrade",
        "RefTrade",
        "dTrade",
        "LInc",
        "RefInc",
        "dInc",
        "LPb",
        "RefPb",
        "dPb"
    );
    println!("+{}+", line);
    for row in rows {
        println!(
            "| {:<STREAM_W$} | {:>PERIOD_W$} | {:>TS_W$} | {:>TS_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>PB_W$} | {:>PB_W$} | {:>PB_W$} |",
            row.stream,
            row.period,
            row.local.ts,
            row.kafka.ts,
            row.local.symbols,
            row.kafka.symbols,
            signed_diff(row.local.symbols, row.kafka.symbols),
            row.local.trades,
            row.kafka.trades,
            signed_diff(row.local.trades, row.kafka.trades),
            row.local.incs,
            row.kafka.incs,
            signed_diff(row.local.incs, row.kafka.incs),
            row.local.pb_bytes,
            row.kafka.pb_bytes,
            signed_diff(row.local.pb_bytes, row.kafka.pb_bytes),
        );
    }
    println!("+{}+", line);

    let exact = rows
        .iter()
        .filter(|row| row.local.ts == row.kafka.ts && selected_counts_equal(row))
        .count();
    println!(
        "matched_rows={} count_exact_rows={} count_mismatched_rows={}",
        rows.len(),
        exact,
        rows.len().saturating_sub(exact)
    );

    let post_lag_sum: i64 = rows
        .iter()
        .map(|row| row.local.post_ts - row.kafka.post_ts)
        .sum();
    if !rows.is_empty() {
        println!(
            "avg_local_post_minus_ref_post_ms={}",
            post_lag_sum / rows.len() as i64
        );
        print_symbol_diff_table(rows);
        print_inc_timestamp_diff_table(rows, dump_extra_limit);
        print_reference_extra_table(rows, dump_extra_limit);
    }
}

fn print_symbol_diff_table(rows: &[CompareRow]) {
    const STREAM_W: usize = 15;
    const SYMBOL_W: usize = 10;
    const PERIOD_W: usize = 10;
    const NUM_W: usize = 8;
    let width = STREAM_W + PERIOD_W + SYMBOL_W + NUM_W * 6 + 22;
    let line = "-".repeat(width);

    println!("+{}+", line);
    println!("|{:^width$}|", "Selected Symbol PB Diff", width = width);
    println!("+{}+", line);
    println!(
        "| {:<STREAM_W$} | {:>PERIOD_W$} | {:<SYMBOL_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} |",
        "Stream", "Period", "Symbol", "LTrade", "RefTrade", "dTrade", "LInc", "RefInc", "dInc"
    );
    println!("+{}+", line);
    for row in rows {
        for (symbol, local) in &row.local.per_symbol {
            let kafka = row
                .kafka
                .per_symbol
                .get(symbol)
                .cloned()
                .unwrap_or_default();
            if local.trades == kafka.trades
                && local.incs == kafka.incs
                && local.present == kafka.present
            {
                continue;
            }
            println!(
                "| {:<STREAM_W$} | {:>PERIOD_W$} | {:<SYMBOL_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} |",
                row.stream,
                row.period,
                symbol,
                local.trades,
                kafka.trades,
                signed_diff(local.trades, kafka.trades),
                local.incs,
                kafka.incs,
                signed_diff(local.incs, kafka.incs),
            );
        }
    }
    println!("+{}+", line);
}

fn print_inc_timestamp_diff_table(rows: &[CompareRow], dump_extra_limit: usize) {
    let mut printed = false;

    for row in rows {
        let lower = period_lower_bound_ms(row.period, DEFAULT_PERIOD_MS);
        let upper = period_upper_bound_ms(row.period, DEFAULT_PERIOD_MS);
        for (symbol, local) in &row.local.per_symbol {
            let reference = row
                .kafka
                .per_symbol
                .get(symbol)
                .cloned()
                .unwrap_or_default();

            for key in extra_multiset_record_keys(
                &reference.inc_ts_keys,
                &local.inc_ts_keys,
                dump_extra_limit,
            ) {
                printed = true;
                println!(
                    "ref_inc_ts_extra stream={} period={} symbol={} {} lower_delta_ms={} upper_delta_ms={}",
                    row.stream,
                    row.period,
                    symbol,
                    key.display,
                    key.timestamp_ms - lower,
                    upper - key.timestamp_ms,
                );
            }
            for key in extra_multiset_record_keys(
                &local.inc_ts_keys,
                &reference.inc_ts_keys,
                dump_extra_limit,
            ) {
                printed = true;
                println!(
                    "local_inc_ts_extra stream={} period={} symbol={} {} lower_delta_ms={} upper_delta_ms={}",
                    row.stream,
                    row.period,
                    symbol,
                    key.display,
                    key.timestamp_ms - lower,
                    upper - key.timestamp_ms,
                );
            }
        }
    }

    if !printed {
        println!("inc_ts_extra_records=0");
    }
}

fn print_reference_extra_table(rows: &[CompareRow], dump_extra_limit: usize) {
    let mut printed = false;

    for row in rows {
        for (symbol, local) in &row.local.per_symbol {
            let reference = row
                .kafka
                .per_symbol
                .get(symbol)
                .cloned()
                .unwrap_or_default();

            if reference.trades > local.trades {
                for key in
                    extra_multiset_keys(&reference.trade_keys, &local.trade_keys, dump_extra_limit)
                {
                    printed = true;
                    println!(
                        "ref_pb_extra stream={} period={} symbol={} kind=trade {}",
                        row.stream, row.period, symbol, key
                    );
                }
            }
            if reference.incs > local.incs {
                for key in
                    extra_multiset_keys(&reference.inc_keys, &local.inc_keys, dump_extra_limit)
                {
                    printed = true;
                    println!(
                        "ref_pb_extra stream={} period={} symbol={} kind=inc {}",
                        row.stream, row.period, symbol, key
                    );
                }
            }
            if local.trades > reference.trades {
                for key in
                    extra_multiset_keys(&local.trade_keys, &reference.trade_keys, dump_extra_limit)
                {
                    printed = true;
                    println!(
                        "local_pb_extra stream={} period={} symbol={} kind=trade {}",
                        row.stream, row.period, symbol, key
                    );
                }
            }
            if local.incs > reference.incs {
                for key in
                    extra_multiset_keys(&local.inc_keys, &reference.inc_keys, dump_extra_limit)
                {
                    printed = true;
                    println!(
                        "local_pb_extra stream={} period={} symbol={} kind=inc {}",
                        row.stream, row.period, symbol, key
                    );
                }
            }
        }
    }

    if !printed {
        println!("pb_extra_records=0");
    }
}

fn print_local_late_completion_table(rows: &[LocalLateCompletion], dump_limit: usize) {
    if rows.is_empty() {
        println!("local_late_completions=0");
        return;
    }

    for row in rows {
        for (symbol, stats) in &row.stats.per_symbol {
            if stats.trades > 0 {
                for key in stats.trade_keys.iter().take(dump_limit) {
                    println!(
                        "local_late_completion stream={} period={} symbol={} kind=trade count={} {}",
                        row.stream, row.period, symbol, stats.trades, key.display
                    );
                }
            }
            if stats.incs > 0 {
                for key in stats.inc_keys.iter().take(dump_limit) {
                    println!(
                        "local_late_completion stream={} period={} symbol={} kind=inc count={} {}",
                        row.stream, row.period, symbol, stats.incs, key.display
                    );
                }
            }
        }
    }
    println!("local_late_completions={}", rows.len());
}

fn extra_multiset_record_keys<'a>(
    reference: &'a [RecordKey],
    local: &[RecordKey],
    limit: usize,
) -> Vec<&'a RecordKey> {
    let mut local_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for key in local {
        *local_counts.entry(key.match_key.as_str()).or_default() += 1;
    }

    let mut extras = Vec::new();
    for key in reference {
        if let Some(count) = local_counts.get_mut(key.match_key.as_str()) {
            if *count > 0 {
                *count -= 1;
                continue;
            }
        }
        extras.push(key);
        if extras.len() >= limit {
            break;
        }
    }
    extras
}

fn extra_multiset_keys(kafka: &[RecordKey], local: &[RecordKey], limit: usize) -> Vec<String> {
    let mut local_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for key in local {
        *local_counts.entry(key.match_key.as_str()).or_default() += 1;
    }

    let mut extras = Vec::new();
    for key in kafka {
        if let Some(count) = local_counts.get_mut(key.match_key.as_str()) {
            if *count > 0 {
                *count -= 1;
                continue;
            }
        }
        extras.push(key.display.clone());
        if extras.len() >= limit {
            break;
        }
    }
    extras
}

fn trade_key(trade: &pb::TradeInfo) -> RecordKey {
    let tp_ms = normalize_record_timestamp(trade.timestamp);
    let detail = format!(
        "side={} price={:.12} amount={:.12}",
        trade.side, trade.price, trade.amount
    );
    RecordKey {
        match_key: format!("tp_ms={} {}", tp_ms, detail),
        display: format!("tp={} tp_ms={} {}", trade.timestamp, tp_ms, detail),
        timestamp_ms: tp_ms,
    }
}

fn inc_key(inc: &pb::IncrementOrderBookInfo) -> RecordKey {
    let tp_ms = normalize_record_timestamp(inc.timestamp);
    let detail = format!(
        "snapshot={} bids_count={} asks_count={} bids=[{}] asks=[{}]",
        inc.is_snapshot,
        inc.bids.len(),
        inc.asks.len(),
        levels_key(&inc.bids),
        levels_key(&inc.asks)
    );
    RecordKey {
        match_key: format!("tp_ms={} {}", tp_ms, detail),
        display: format!("tp={} tp_ms={} {}", inc.timestamp, tp_ms, detail),
        timestamp_ms: tp_ms,
    }
}

fn inc_timestamp_key(inc: &pb::IncrementOrderBookInfo) -> RecordKey {
    let tp_ms = normalize_record_timestamp(inc.timestamp);
    RecordKey {
        match_key: format!("tp_ms={}", tp_ms),
        display: format!("tp={} tp_ms={}", inc.timestamp, tp_ms),
        timestamp_ms: tp_ms,
    }
}

fn normalize_record_timestamp(timestamp: i64) -> i64 {
    if timestamp >= 10_000_000_000_000 {
        timestamp / 1_000
    } else {
        timestamp
    }
}

fn levels_key(levels: &[pb::PriceLevel]) -> String {
    levels
        .iter()
        .map(|level| format!("{:.12}@{:.12}", level.price, level.amount))
        .collect::<Vec<_>>()
        .join(",")
}

fn selected_counts_equal(row: &CompareRow) -> bool {
    row.local.symbols == row.kafka.symbols
        && row.local.trades == row.kafka.trades
        && row.local.incs == row.kafka.incs
        && row.local.per_symbol.iter().all(|(symbol, local)| {
            let kafka = row
                .kafka
                .per_symbol
                .get(symbol)
                .cloned()
                .unwrap_or_default();
            local.present == kafka.present
                && local.trades == kafka.trades
                && local.incs == kafka.incs
        })
}

fn signed_diff(left: usize, right: usize) -> String {
    let diff = left as i128 - right as i128;
    if diff > 0 {
        format!("+{}", diff)
    } else {
        diff.to_string()
    }
}

fn default_compare_symbols() -> Vec<String> {
    ["BTCUSDT"].into_iter().map(str::to_string).collect()
}

fn normalize_compare_symbols(symbols: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    symbols
        .into_iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .filter(|symbol| seen.insert(symbol.clone()))
        .collect()
}

fn is_compare_symbol(symbol: &str, compare_symbols: &[String]) -> bool {
    compare_symbols.iter().any(|target| target == symbol)
}

fn sanitize_node_component(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
