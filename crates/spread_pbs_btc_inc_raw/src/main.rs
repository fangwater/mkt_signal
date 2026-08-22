use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use mkt_parsers::binance::parse_incremental_raw_view;
use period_pbs::kafka::{
    decode_period_payload, format_rewrites, KafkaConsumerConfig, RawKafkaConsumer,
};
use period_pbs::pb;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/public/stream";
const DEFAULT_CONFIG: &str = "config/period_live_compare.toml";
const INIT_TP_MS: i64 = 1_704_067_200_000;
const DEFAULT_PERIOD_MS: i64 = 3_000;
const RECONNECT_BACKOFF_SECS: u64 = 3;
const KAFKA_DRAIN_BATCH: usize = 65_536;

#[derive(Parser)]
#[command(name = "spread_pbs_btc_inc_raw")]
#[command(
    about = "Subscribe Binance futures BTC incremental stream, optionally compare with Kafka PeriodMessage."
)]
struct Args {
    /// Symbol to subscribe/compare. Defaults to BTCUSDT.
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

    /// Override websocket URL.
    #[arg(long, default_value = BINANCE_FUTURES_WS_URL)]
    url: String,

    /// Stop raw capture after this many parsed incremental rows.
    #[arg(long)]
    max_rows: Option<u64>,

    /// Do not include the original raw websocket message in raw JSONL rows.
    #[arg(long)]
    omit_raw: bool,

    /// Write raw JSONL rows to this path. In raw-only mode, stdout is used if omitted.
    #[arg(long)]
    raw_output: Option<PathBuf>,

    /// Also consume Kafka PeriodMessage and compare by period/timestamp/levels.
    #[arg(long)]
    compare_kafka: bool,

    /// TOML config path. Reuses the kafka section from period_live_compare.toml.
    #[arg(long, default_value = DEFAULT_CONFIG)]
    config: PathBuf,

    /// Kafka topic to compare. Defaults to binance-futures.
    #[arg(long, default_value = "binance-futures")]
    topic: String,

    /// Stop after this many comparable periods after skipping initial periods.
    #[arg(long)]
    compare_periods: Option<usize>,

    /// Skip this many initial comparable periods to avoid startup partial periods.
    #[arg(long)]
    skip_initial_periods: Option<usize>,

    /// Stop compare after this many seconds.
    #[arg(long)]
    max_wait_secs: Option<u64>,

    /// Max extra records to print per side per period.
    #[arg(long)]
    dump_extra_limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct CompareConfig {
    compare_periods: usize,
    max_wait_secs: u64,
    skip_initial_matches: usize,
    dump_extra_limit: usize,
    kafka: KafkaConsumerConfig,
}

impl Default for CompareConfig {
    fn default() -> Self {
        let mut kafka = KafkaConsumerConfig::default();
        kafka.poll_timeout_ms = 0;
        Self {
            compare_periods: 20,
            max_wait_secs: 180,
            skip_initial_matches: 1,
            dump_extra_limit: 64,
            kafka,
        }
    }
}

struct OutputState {
    row_no: u64,
    prev_final_update_id: Option<i64>,
    max_rows: Option<u64>,
    omit_raw: bool,
    raw_writer: Option<Box<dyn Write>>,
}

#[derive(Debug, Clone)]
struct RecordKey {
    match_key: String,
    display: String,
    tp_ms: i64,
}

#[derive(Debug, Clone, Default)]
struct PeriodRecords {
    records: Vec<RecordKey>,
}

impl PeriodRecords {
    fn push(&mut self, record: RecordKey) {
        self.records.push(record);
    }

    fn len(&self) -> usize {
        self.records.len()
    }

    fn min_tp_ms(&self) -> Option<i64> {
        self.records.iter().map(|record| record.tp_ms).min()
    }

    fn max_tp_ms(&self) -> Option<i64> {
        self.records.iter().map(|record| record.tp_ms).max()
    }
}

#[derive(Debug)]
struct CompareRow {
    period: i64,
    raw: PeriodRecords,
    kafka: PeriodRecords,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    if args.compare_kafka {
        run_compare(args).await
    } else {
        run_capture(args).await
    }
}

async fn run_capture(args: Args) -> Result<()> {
    let symbol = normalize_symbol(&args.symbol);
    anyhow::ensure!(!symbol.is_empty(), "empty symbol");
    let stream = stream_name(&symbol);
    let subscribe_msg = build_incremental_subscribe(&stream);
    let raw_writer = open_raw_writer(args.raw_output.as_ref())?;
    let mut state = OutputState {
        row_no: 0,
        prev_final_update_id: None,
        max_rows: args.max_rows,
        omit_raw: args.omit_raw,
        raw_writer,
    };

    log::info!(
        "spread_pbs_btc_inc_raw starting raw capture symbol={} stream={} url={} local_ip=0.0.0.0 raw_output={}",
        symbol,
        stream,
        args.url,
        args.raw_output
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "stdout".to_string()),
    );

    loop {
        tokio::select! {
            res = run_one_ws_session(&args.url, &subscribe_msg, &symbol, &mut state, None) => {
                match res {
                    Ok(SessionExit::MaxRows) => return Ok(()),
                    Ok(SessionExit::Disconnected) => log::warn!("raw capture disconnected; reconnect in {}s", RECONNECT_BACKOFF_SECS),
                    Err(e) => log::warn!("raw capture session failed: {:#}; reconnect in {}s", e, RECONNECT_BACKOFF_SECS),
                }
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(e) = signal {
                    log::warn!("ctrl-c listener failed: {:#}", e);
                }
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_secs(RECONNECT_BACKOFF_SECS)).await;
    }
}

async fn run_compare(args: Args) -> Result<()> {
    let symbol = normalize_symbol(&args.symbol);
    anyhow::ensure!(!symbol.is_empty(), "empty symbol");
    let mut config = load_compare_config(&args.config)?;
    config.kafka.topics = vec![args.topic.clone()];
    config.kafka.poll_timeout_ms = 0;
    config.kafka.group_id = format!("{}_raw_ws_{}", config.kafka.group_id, std::process::id());
    config.kafka.client_id = format!("{}_raw_ws_{}", config.kafka.client_id, std::process::id());
    if let Some(compare_periods) = args.compare_periods {
        config.compare_periods = compare_periods;
    }
    if let Some(skip) = args.skip_initial_periods {
        config.skip_initial_matches = skip;
    }
    if let Some(max_wait_secs) = args.max_wait_secs {
        config.max_wait_secs = max_wait_secs;
    }
    if let Some(limit) = args.dump_extra_limit {
        config.dump_extra_limit = limit;
    }

    let raw_writer = open_raw_writer(args.raw_output.as_ref())?;
    let mut output = OutputState {
        row_no: 0,
        prev_final_update_id: None,
        max_rows: None,
        omit_raw: args.omit_raw,
        raw_writer,
    };
    let consumer = RawKafkaConsumer::new(&config.kafka)?;
    let stream = stream_name(&symbol);
    let subscribe_msg = build_incremental_subscribe(&stream);

    log::info!(
        "raw-vs-kafka compare started symbol={} stream={} topic={} url={} brokers={} rewrites={} compare_periods={} skip_initial={} max_wait_secs={} raw_output={}",
        symbol,
        stream,
        args.topic,
        args.url,
        config.kafka.brokers,
        format_rewrites(&config.kafka.broker_addr_rewrites),
        config.compare_periods,
        config.skip_initial_matches,
        config.max_wait_secs,
        args.raw_output
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "off".to_string()),
    );

    let deadline = Instant::now() + Duration::from_secs(config.max_wait_secs);
    let mut raw_periods: BTreeMap<i64, PeriodRecords> = BTreeMap::new();
    let mut kafka_periods: BTreeMap<i64, PeriodRecords> = BTreeMap::new();
    let mut compared = HashSet::new();
    let mut skipped = 0usize;
    let mut rows = Vec::new();
    let mut first_raw_period: Option<i64> = None;
    let mut latest_raw_period: Option<i64> = None;

    let (mut sink, mut read) = connect_ws(&args.url, &subscribe_msg).await?;

    while rows.len() < config.compare_periods && Instant::now() < deadline {
        drain_kafka(
            &consumer,
            &config.kafka,
            &args.topic,
            &symbol,
            &mut kafka_periods,
        )?;

        match tokio::time::timeout(Duration::from_millis(2), read.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                if !is_keepalive_response(&text) {
                    let recv_us = now_us();
                    if let Some(record) =
                        handle_raw_row(&symbol, recv_us, text.as_bytes(), &mut output)?
                    {
                        first_raw_period.get_or_insert(record_period(&record));
                        latest_raw_period = Some(record_period(&record));
                        raw_periods
                            .entry(record_period(&record))
                            .or_default()
                            .push(record);
                    }
                }
            }
            Ok(Some(Ok(Message::Binary(bin)))) => {
                let recv_us = now_us();
                if let Some(record) = handle_raw_row(&symbol, recv_us, bin.as_slice(), &mut output)?
                {
                    first_raw_period.get_or_insert(record_period(&record));
                    latest_raw_period = Some(record_period(&record));
                    raw_periods
                        .entry(record_period(&record))
                        .or_default()
                        .push(record);
                }
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                let _ = sink.send(Message::Pong(payload)).await;
            }
            Ok(Some(Ok(Message::Pong(_)))) | Ok(Some(Ok(Message::Frame(_)))) => {}
            Ok(Some(Ok(Message::Close(frame)))) => {
                log::warn!("raw websocket close frame: {:?}; reconnecting", frame);
                let connected = connect_ws(&args.url, &subscribe_msg).await?;
                sink = connected.0;
                read = connected.1;
            }
            Ok(Some(Err(e))) => {
                log::warn!("raw websocket read failed: {:#}; reconnecting", e);
                let connected = connect_ws(&args.url, &subscribe_msg).await?;
                sink = connected.0;
                read = connected.1;
            }
            Ok(None) => {
                log::warn!("raw websocket ended; reconnecting");
                let connected = connect_ws(&args.url, &subscribe_msg).await?;
                sink = connected.0;
                read = connected.1;
            }
            Err(_) => {}
        }

        promote_compare_rows(
            &raw_periods,
            &kafka_periods,
            first_raw_period,
            latest_raw_period,
            &mut compared,
            &mut skipped,
            &mut rows,
            config.skip_initial_matches,
            config.compare_periods,
        );
    }

    drain_kafka(
        &consumer,
        &config.kafka,
        &args.topic,
        &symbol,
        &mut kafka_periods,
    )?;
    promote_compare_rows(
        &raw_periods,
        &kafka_periods,
        first_raw_period,
        latest_raw_period,
        &mut compared,
        &mut skipped,
        &mut rows,
        config.skip_initial_matches,
        config.compare_periods,
    );

    print_compare_report(&rows, config.dump_extra_limit);
    if rows.len() < config.compare_periods {
        println!(
            "compare_incomplete rows={} requested={} skipped={} first_raw_period={:?} latest_raw_period={:?}",
            rows.len(), config.compare_periods, skipped, first_raw_period, latest_raw_period
        );
    }
    Ok(())
}

type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;
type WsRead = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

async fn connect_ws(url: &str, subscribe_msg: &Value) -> Result<(WsSink, WsRead)> {
    let (ws, _) = connect_async(url).await.context("connect websocket")?;
    let (mut sink, read) = ws.split();
    sink.send(Message::Text(subscribe_msg.to_string()))
        .await
        .context("send subscribe payload")?;
    log::info!("connected raw websocket to {}", url);
    Ok((sink, read))
}

enum SessionExit {
    Disconnected,
    MaxRows,
}

async fn run_one_ws_session(
    url: &str,
    subscribe_msg: &Value,
    expected_symbol: &str,
    state: &mut OutputState,
    mut records: Option<&mut BTreeMap<i64, PeriodRecords>>,
) -> Result<SessionExit> {
    let (mut sink, mut read) = connect_ws(url, subscribe_msg).await?;

    while let Some(next) = read.next().await {
        let recv_us = now_us();
        match next {
            Ok(Message::Text(text)) => {
                if is_keepalive_response(&text) {
                    continue;
                }
                if let Some(record) =
                    handle_raw_row(expected_symbol, recv_us, text.as_bytes(), state)?
                {
                    if let Some(records) = records.as_deref_mut() {
                        records
                            .entry(record_period(&record))
                            .or_default()
                            .push(record);
                    }
                    if state.max_rows.is_some_and(|max| state.row_no >= max) {
                        let _ = sink.close().await;
                        return Ok(SessionExit::MaxRows);
                    }
                }
            }
            Ok(Message::Binary(bin)) => {
                if let Some(record) =
                    handle_raw_row(expected_symbol, recv_us, bin.as_slice(), state)?
                {
                    if let Some(records) = records.as_deref_mut() {
                        records
                            .entry(record_period(&record))
                            .or_default()
                            .push(record);
                    }
                    if state.max_rows.is_some_and(|max| state.row_no >= max) {
                        let _ = sink.close().await;
                        return Ok(SessionExit::MaxRows);
                    }
                }
            }
            Ok(Message::Ping(payload)) => {
                let _ = sink.send(Message::Pong(payload)).await;
            }
            Ok(Message::Pong(_)) => {}
            Ok(Message::Close(frame)) => {
                log::warn!("raw websocket close frame: {:?}", frame);
                return Ok(SessionExit::Disconnected);
            }
            Ok(Message::Frame(_)) => {}
            Err(e) => return Err(e).context("websocket read"),
        }
    }
    Ok(SessionExit::Disconnected)
}

fn drain_kafka(
    consumer: &RawKafkaConsumer,
    kafka_config: &KafkaConsumerConfig,
    topic: &str,
    symbol: &str,
    out: &mut BTreeMap<i64, PeriodRecords>,
) -> Result<()> {
    for _ in 0..KAFKA_DRAIN_BATCH {
        let Some(message) = consumer.poll(0) else {
            break;
        };
        let message = message?;
        if message.topic != topic {
            continue;
        }
        let (_, _, period) =
            decode_period_payload(&message.payload, kafka_config.payload_compression)
                .with_context(|| {
                    format!(
                        "decode kafka topic={} offset={}",
                        message.topic, message.offset
                    )
                })?;
        let mut records = PeriodRecords::default();
        if let Some(info) = period
            .symbol_infos
            .iter()
            .find(|info| info.symbol.eq_ignore_ascii_case(symbol))
        {
            for inc in &info.incs {
                records.push(kafka_inc_key(inc));
            }
        }
        out.insert(period.period, records);
    }
    Ok(())
}

fn promote_compare_rows(
    raw_periods: &BTreeMap<i64, PeriodRecords>,
    kafka_periods: &BTreeMap<i64, PeriodRecords>,
    first_raw_period: Option<i64>,
    latest_raw_period: Option<i64>,
    compared: &mut HashSet<i64>,
    skipped: &mut usize,
    rows: &mut Vec<CompareRow>,
    skip_initial: usize,
    target_rows: usize,
) {
    let Some(first_raw_period) = first_raw_period else {
        return;
    };
    let Some(latest_raw_period) = latest_raw_period else {
        return;
    };
    let mut periods: Vec<i64> = kafka_periods.keys().copied().collect();
    periods.sort_unstable();
    for period in periods {
        if rows.len() >= target_rows {
            return;
        }
        if compared.contains(&period) {
            continue;
        }
        if period <= first_raw_period || period >= latest_raw_period {
            continue;
        }
        if !kafka_periods.contains_key(&period) {
            continue;
        }
        compared.insert(period);
        if *skipped < skip_initial {
            *skipped += 1;
            continue;
        }
        rows.push(CompareRow {
            period,
            raw: raw_periods.get(&period).cloned().unwrap_or_default(),
            kafka: kafka_periods.get(&period).cloned().unwrap_or_default(),
        });
    }
}

fn handle_raw_row(
    expected_symbol: &str,
    recv_us: i64,
    raw: &[u8],
    state: &mut OutputState,
) -> Result<Option<RecordKey>> {
    let Some(view) = parse_incremental_raw_view(raw) else {
        return Ok(None);
    };
    let symbol = view.symbol.to_ascii_uppercase();
    if symbol != expected_symbol {
        return Ok(None);
    }

    let value = serde_json::from_slice::<Value>(raw).context("parse raw JSON for diagnostics")?;
    let payload = value.get("data").unwrap_or(&value);
    let event_time_ms = payload
        .get("E")
        .and_then(parse_i64)
        .or_else(|| payload.get("T").and_then(parse_i64));
    let transaction_time_ms = payload.get("T").and_then(parse_i64);
    let pu = payload.get("pu").and_then(parse_i64);
    let bids = parse_json_levels(payload.get("b").or_else(|| payload.get("bids")));
    let asks = parse_json_levels(payload.get("a").or_else(|| payload.get("asks")));

    let prev_final_update_id = state.prev_final_update_id;
    let expected_first_update_id = prev_final_update_id.map(|id| id.saturating_add(1));
    let id_gap = expected_first_update_id
        .map(|expected| view.first_update_id > expected)
        .unwrap_or(false);
    let id_overlap = expected_first_update_id
        .map(|expected| view.first_update_id < expected)
        .unwrap_or(false);
    let pu_mismatch = match (prev_final_update_id, pu) {
        (Some(prev), Some(pu)) => pu != prev,
        _ => false,
    };

    state.row_no = state.row_no.saturating_add(1);
    state.prev_final_update_id = Some(view.final_update_id);

    let parser_tp_ms = view.timestamp_us / 1_000;
    let tp_ms = transaction_time_ms
        .or(event_time_ms)
        .unwrap_or(parser_tp_ms);
    let tp_us = tp_ms.saturating_mul(1_000);
    let detail = record_detail(view.is_snapshot, &bids, &asks);
    let record = RecordKey {
        match_key: format!("tp_ms={} {}", tp_ms, detail),
        display: format!(
            "tp={} tp_ms={} E_ms={:?} T_ms={:?} parser_tp_ms={} U={} u={} pu={:?} prev_u={:?} exp_U={:?} id_gap={} id_overlap={} pu_mismatch={} {}",
            tp_us,
            tp_ms,
            event_time_ms,
            transaction_time_ms,
            parser_tp_ms,
            view.first_update_id,
            view.final_update_id,
            pu,
            prev_final_update_id,
            expected_first_update_id,
            id_gap,
            id_overlap,
            pu_mismatch,
            detail,
        ),
        tp_ms,
    };

    if state.raw_writer.is_some() || state.max_rows.is_some() {
        let mut row = json!({
            "row_no": state.row_no,
            "venue": "binance-futures",
            "stream": stream_name(&symbol),
            "symbol": symbol,
            "recv_us": recv_us,
            "tp_us": tp_us,
            "tp_ms": tp_ms,
            "parser_tp_us": view.timestamp_us,
            "parser_tp_ms": parser_tp_ms,
            "event_time_ms": event_time_ms,
            "transaction_time_ms": transaction_time_ms,
            "period": period_for_timestamp_ms(tp_ms),
            "seq_id": view.seq_id,
            "prev_seq_id": view.prev_seq_id,
            "first_update_id": view.first_update_id,
            "final_update_id": view.final_update_id,
            "pu": pu,
            "prev_final_update_id": prev_final_update_id,
            "expected_first_update_id": expected_first_update_id,
            "id_gap": id_gap,
            "id_overlap": id_overlap,
            "pu_mismatch": pu_mismatch,
            "is_snapshot": view.is_snapshot,
            "gap_check": view.gap_check,
            "bids_count": bids.len(),
            "asks_count": asks.len(),
            "raw_len": raw.len(),
            "latency_us": recv_us.saturating_sub(view.timestamp_us),
        });
        if !state.omit_raw {
            row["raw"] = Value::String(String::from_utf8_lossy(raw).into_owned());
        }
        write_raw_row(state, &row)?;
    }

    Ok(Some(record))
}

fn write_raw_row(state: &mut OutputState, row: &Value) -> Result<()> {
    let line = serde_json::to_string(row)?;
    if let Some(writer) = state.raw_writer.as_mut() {
        writeln!(writer, "{}", line)?;
        writer.flush()?;
    } else {
        println!("{}", line);
        let _ = std::io::stdout().flush();
    }
    Ok(())
}

fn kafka_inc_key(inc: &pb::IncrementOrderBookInfo) -> RecordKey {
    let tp_ms = normalize_record_timestamp(inc.timestamp);
    let bids: Vec<(f64, f64)> = inc
        .bids
        .iter()
        .map(|level| (level.price, level.amount))
        .collect();
    let asks: Vec<(f64, f64)> = inc
        .asks
        .iter()
        .map(|level| (level.price, level.amount))
        .collect();
    let detail = record_detail(inc.is_snapshot, &bids, &asks);
    RecordKey {
        match_key: format!("tp_ms={} {}", tp_ms, detail),
        display: format!("tp={} tp_ms={} {}", inc.timestamp, tp_ms, detail),
        tp_ms,
    }
}

fn print_compare_report(rows: &[CompareRow], dump_limit: usize) {
    const PERIOD_W: usize = 10;
    const NUM_W: usize = 8;
    const TS_W: usize = 13;
    let width = PERIOD_W + NUM_W * 3 + TS_W * 4 + 24;
    let line = "-".repeat(width);
    println!("+{}+", line);
    println!(
        "|{:^width$}|",
        "Direct WS Raw vs Kafka PeriodMessage BTC Inc",
        width = width
    );
    println!("+{}+", line);
    println!(
        "| {:>PERIOD_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>TS_W$} | {:>TS_W$} | {:>TS_W$} | {:>TS_W$} |",
        "Period", "RawInc", "KafkaInc", "Diff", "RawMinMs", "RawMaxMs", "KfkMinMs", "KfkMaxMs"
    );
    println!("+{}+", line);
    for row in rows {
        println!(
            "| {:>PERIOD_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>NUM_W$} | {:>TS_W$} | {:>TS_W$} | {:>TS_W$} | {:>TS_W$} |",
            row.period,
            row.raw.len(),
            row.kafka.len(),
            signed_diff(row.raw.len(), row.kafka.len()),
            opt_i64(row.raw.min_tp_ms()),
            opt_i64(row.raw.max_tp_ms()),
            opt_i64(row.kafka.min_tp_ms()),
            opt_i64(row.kafka.max_tp_ms()),
        );
    }
    println!("+{}+", line);
    let exact = rows
        .iter()
        .filter(|row| multisets_equal(&row.raw.records, &row.kafka.records))
        .count();
    println!(
        "matched_rows={} exact_rows={} mismatched_rows={}",
        rows.len(),
        exact,
        rows.len().saturating_sub(exact)
    );

    let mut printed = false;
    for row in rows {
        for extra in extra_multiset_keys(&row.raw.records, &row.kafka.records, dump_limit) {
            printed = true;
            println!(
                "raw_ws_extra period={} symbol=BTCUSDT kind=inc {}",
                row.period, extra
            );
        }
        for extra in extra_multiset_keys(&row.kafka.records, &row.raw.records, dump_limit) {
            printed = true;
            println!(
                "kafka_pb_extra period={} symbol=BTCUSDT kind=inc {}",
                row.period, extra
            );
        }
    }
    if !printed {
        println!("timeline_extra_records=0");
    }
}

fn extra_multiset_keys(left: &[RecordKey], right: &[RecordKey], limit: usize) -> Vec<String> {
    let mut right_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for key in right {
        *right_counts.entry(key.match_key.as_str()).or_default() += 1;
    }
    let mut extras = Vec::new();
    for key in left {
        if let Some(count) = right_counts.get_mut(key.match_key.as_str()) {
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

fn multisets_equal(left: &[RecordKey], right: &[RecordKey]) -> bool {
    extra_multiset_keys(left, right, 1).is_empty() && extra_multiset_keys(right, left, 1).is_empty()
}

fn parse_json_levels(value: Option<&Value>) -> Vec<(f64, f64)> {
    value
        .and_then(Value::as_array)
        .map(|levels| {
            levels
                .iter()
                .filter_map(|level| {
                    let pair = level.as_array()?;
                    let price = pair.first().and_then(parse_f64)?;
                    let amount = pair.get(1).and_then(parse_f64)?;
                    Some((price, amount))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn record_detail(is_snapshot: bool, bids: &[(f64, f64)], asks: &[(f64, f64)]) -> String {
    format!(
        "snapshot={} bids_count={} asks_count={} bids=[{}] asks=[{}]",
        is_snapshot,
        bids.len(),
        asks.len(),
        levels_key(bids),
        levels_key(asks)
    )
}

fn levels_key(levels: &[(f64, f64)]) -> String {
    levels
        .iter()
        .map(|(price, amount)| format!("{:.12}@{:.12}", price, amount))
        .collect::<Vec<_>>()
        .join(",")
}

fn record_period(record: &RecordKey) -> i64 {
    period_for_timestamp_ms(record.tp_ms)
}

fn period_for_timestamp_ms(timestamp_ms: i64) -> i64 {
    (timestamp_ms - INIT_TP_MS - 1).div_euclid(DEFAULT_PERIOD_MS)
}

fn normalize_record_timestamp(timestamp: i64) -> i64 {
    if timestamp >= 10_000_000_000_000 {
        timestamp / 1_000
    } else {
        timestamp
    }
}

fn build_incremental_subscribe(stream: &str) -> Value {
    json!({
        "method": "SUBSCRIBE",
        "params": [stream],
        "id": 1,
    })
}

fn stream_name(symbol: &str) -> String {
    format!("{}@depth@0ms", symbol.to_ascii_lowercase())
}

fn normalize_symbol(raw: &str) -> String {
    raw.trim().to_ascii_uppercase()
}

fn parse_i64(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse::<i64>().ok())
}

fn parse_f64(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok())
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

fn is_keepalive_response(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed == "pong"
        || trimmed == "\"pong\""
        || trimmed.starts_with("{\"result\":")
        || trimmed.contains("\"op\":\"pong\"")
        || trimmed.contains("\"op\":\"ping\"")
        || trimmed.contains("\"event\":\"subscribe\"")
        || trimmed.contains("\"event\":\"unsubscribe\"")
}

fn open_raw_writer(path: Option<&PathBuf>) -> Result<Option<Box<dyn Write>>> {
    match path {
        Some(path) => Ok(Some(Box::new(BufWriter::new(
            File::create(path).with_context(|| format!("create raw output {}", path.display()))?,
        )))),
        None => Ok(None),
    }
}

fn load_compare_config(path: &PathBuf) -> Result<CompareConfig> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parse {}", path.display()))
}

fn signed_diff(left: usize, right: usize) -> String {
    let diff = left as i64 - right as i64;
    if diff >= 0 {
        format!("+{}", diff)
    } else {
        diff.to_string()
    }
}

fn opt_i64(value: Option<i64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string())
}
