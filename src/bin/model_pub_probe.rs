use anyhow::{Context, Result};
use clap::Parser;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_parsers::msg::mkt_msg::{
    ModelMsg, MODEL_STATUS_BAD_DIM, MODEL_STATUS_DECODE_ERR, MODEL_STATUS_INFER_ERR,
    MODEL_STATUS_OK,
};
use mkt_parsers::msg::model_ipc::MODEL_PAYLOAD_MAX_BYTES;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

const MODEL_OUTPUT_SUBSCRIBER_MAX_BUFFER_SIZE: usize = 256;
const MODEL_OUTPUT_HISTORY_SIZE: usize = 128;
const MODEL_OUTPUT_MAX_PUBLISHERS: usize = 1;
const MODEL_OUTPUT_MAX_SUBSCRIBERS: usize = 10;
const DEFAULT_POLL_MS: u64 = 20;
const DEFAULT_PRINT_INTERVAL_SECS: u64 = 5;

#[derive(Parser, Debug)]
#[command(name = "model_pub_probe")]
#[command(about = "Track the latest ModelMsg for each symbol and report model readiness")]
struct Args {
    /// Model name used to derive model_output/<model_name>.
    model_name: String,

    /// Override the Iceoryx service name.
    #[arg(long)]
    service: Option<String>,

    /// Symbols that must be observed before the aggregate state can be ALL_READY.
    #[arg(long, value_delimiter = ',')]
    expected_symbols: Vec<String>,

    /// Print interval in seconds.
    #[arg(long, default_value_t = DEFAULT_PRINT_INTERVAL_SECS)]
    interval_s: u64,

    /// Stop after this many seconds (0 = run until Ctrl-C).
    #[arg(long, default_value_t = 0)]
    timeout_s: u64,

    /// Stop as soon as the aggregate state becomes ALL_READY.
    #[arg(long, default_value_t = false)]
    exit_when_ready: bool,

    /// Poll interval in milliseconds.
    #[arg(long, default_value_t = DEFAULT_POLL_MS)]
    poll_ms: u64,
}

struct SymbolState {
    latest: ModelMsg,
    total_messages: u64,
    ready_messages: u64,
    last_received_at: Instant,
}

impl SymbolState {
    fn new(msg: ModelMsg) -> Self {
        let ready_messages = u64::from(msg.score_ready);
        Self {
            latest: msg,
            total_messages: 1,
            ready_messages,
            last_received_at: Instant::now(),
        }
    }

    fn update(&mut self, msg: ModelMsg) {
        self.total_messages = self.total_messages.saturating_add(1);
        self.ready_messages = self
            .ready_messages
            .saturating_add(u64::from(msg.score_ready));
        self.latest = msg;
        self.last_received_at = Instant::now();
    }

    fn is_ready(&self) -> bool {
        self.latest.score_ready
            && self.latest.status == MODEL_STATUS_OK
            && self.latest.score.is_finite()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateState {
    NoData,
    WaitingSymbols,
    NotReady,
    AllReady,
}

impl AggregateState {
    fn as_str(self) -> &'static str {
        match self {
            Self::NoData => "NO_DATA",
            Self::WaitingSymbols => "WAITING_SYMBOLS",
            Self::NotReady => "NOT_READY",
            Self::AllReady => "ALL_READY",
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let service_name = args
        .service
        .clone()
        .unwrap_or_else(|| format!("model_output/{}", args.model_name.trim()));
    let expected_symbols = normalize_expected_symbols(&args.expected_symbols);
    let node_name = format!(
        "model_pub_probe_{}_{}",
        sanitize_node_suffix(&args.model_name),
        std::process::id()
    );

    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;
    let service = node
        .service_builder(&ServiceName::new(&service_name)?)
        .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
        .max_publishers(MODEL_OUTPUT_MAX_PUBLISHERS)
        .max_subscribers(MODEL_OUTPUT_MAX_SUBSCRIBERS)
        .subscriber_max_buffer_size(MODEL_OUTPUT_SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(MODEL_OUTPUT_HISTORY_SIZE)
        .open()
        .with_context(|| format!("open model output service failed: {service_name}"))?;
    let subscriber = service
        .subscriber_builder()
        .buffer_size(MODEL_OUTPUT_SUBSCRIBER_MAX_BUFFER_SIZE)
        .create()
        .context("create model output subscriber failed")?;

    println!(
        "[MODEL_PROBE] model={} service={} expected_symbols={} interval={}s timeout={}s exit_when_ready={}",
        args.model_name,
        service_name,
        format_expected_symbols(&expected_symbols),
        args.interval_s.max(1),
        args.timeout_s,
        args.exit_when_ready
    );

    let started_at = Instant::now();
    let print_interval = Duration::from_secs(args.interval_s.max(1));
    let poll_interval = Duration::from_millis(args.poll_ms.max(1));
    let timeout = (args.timeout_s > 0).then(|| Duration::from_secs(args.timeout_s));
    let mut last_print = Instant::now();
    let mut states = HashMap::<String, SymbolState>::new();
    let mut received = 0u64;
    let mut decode_errors = 0u64;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    let stop_reason = loop {
        while let Some(sample) = subscriber.receive()? {
            match ModelMsg::from_bytes(sample.payload()) {
                Ok(msg) => {
                    received = received.saturating_add(1);
                    let symbol = normalize_symbol(&msg.symbol);
                    if symbol.is_empty() {
                        decode_errors = decode_errors.saturating_add(1);
                        continue;
                    }
                    match states.get_mut(&symbol) {
                        Some(state) => state.update(msg),
                        None => {
                            states.insert(symbol, SymbolState::new(msg));
                        }
                    }
                }
                Err(err) => {
                    decode_errors = decode_errors.saturating_add(1);
                    eprintln!("[MODEL_PROBE] decode_error={} err={}", decode_errors, err);
                }
            }
        }

        let aggregate = aggregate_state(&states, &expected_symbols);
        if args.exit_when_ready && aggregate == AggregateState::AllReady {
            break "all_ready";
        }
        if timeout.is_some_and(|limit| started_at.elapsed() >= limit) {
            break "timeout";
        }
        if last_print.elapsed() >= print_interval {
            print_snapshot(
                false,
                &service_name,
                &states,
                &expected_symbols,
                received,
                decode_errors,
            );
            last_print = Instant::now();
        }

        tokio::select! {
            result = &mut ctrl_c => {
                result.context("wait for Ctrl-C failed")?;
                break "ctrl_c";
            }
            _ = tokio::time::sleep(poll_interval) => {}
        }
    };

    print_snapshot(
        true,
        &service_name,
        &states,
        &expected_symbols,
        received,
        decode_errors,
    );
    println!("[MODEL_PROBE] stopped reason={stop_reason}");
    Ok(())
}

fn print_snapshot(
    final_snapshot: bool,
    service_name: &str,
    states: &HashMap<String, SymbolState>,
    expected_symbols: &HashSet<String>,
    received: u64,
    decode_errors: u64,
) {
    let label = if final_snapshot { "FINAL" } else { "SNAPSHOT" };
    println!("\n[{label}] service={service_name}");
    println!("{}", build_symbol_table(states, expected_symbols));

    let ready = states.values().filter(|state| state.is_ready()).count();
    let not_ready = states.len().saturating_sub(ready);
    let missing = expected_symbols
        .iter()
        .filter(|symbol| !states.contains_key(*symbol))
        .count();
    let aggregate = aggregate_state(states, expected_symbols);
    println!(
        "[{label}] state={} observed={} ready={} not_ready={} expected={} missing={} received={} decode_errors={}",
        aggregate.as_str(),
        states.len(),
        ready,
        not_ready,
        expected_symbols.len(),
        missing,
        received,
        decode_errors
    );
}

fn aggregate_state(
    states: &HashMap<String, SymbolState>,
    expected_symbols: &HashSet<String>,
) -> AggregateState {
    if states.is_empty() {
        return AggregateState::NoData;
    }
    if expected_symbols
        .iter()
        .any(|symbol| !states.contains_key(symbol))
    {
        return AggregateState::WaitingSymbols;
    }
    if states.values().all(SymbolState::is_ready) {
        AggregateState::AllReady
    } else {
        AggregateState::NotReady
    }
}

fn build_symbol_table(
    states: &HashMap<String, SymbolState>,
    expected_symbols: &HashSet<String>,
) -> String {
    let headers = [
        "symbol",
        "msg_ready",
        "symbol_ready",
        "ready_msgs",
        "status",
        "score",
        "quantile",
        "feature_dim",
        "age_ms",
    ];
    let mut symbols = states.keys().cloned().collect::<HashSet<_>>();
    symbols.extend(expected_symbols.iter().cloned());
    let mut symbols = symbols.into_iter().collect::<Vec<_>>();
    symbols.sort_unstable();

    let rows = symbols
        .iter()
        .map(|symbol| match states.get(symbol) {
            Some(state) => vec![
                symbol.clone(),
                state.latest.score_ready.to_string(),
                if state.is_ready() { "true" } else { "false" }.to_string(),
                format!("{}/{}", state.ready_messages, state.total_messages),
                status_name(state.latest.status),
                format!("{:.8}", state.latest.score),
                state
                    .latest
                    .score_quantile
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_else(|| "NA".to_string()),
                state.latest.feature_dim.to_string(),
                state.last_received_at.elapsed().as_millis().to_string(),
            ],
            None => vec![
                symbol.clone(),
                "NOT_SEEN".to_string(),
                "NOT_SEEN".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
            ],
        })
        .collect::<Vec<_>>();

    build_three_line_table(&headers, &rows)
}

fn build_three_line_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }

    let format_row = |values: &[&str]| -> String {
        values
            .iter()
            .enumerate()
            .map(|(index, value)| format!("{:<width$}", value, width = widths[index]))
            .collect::<Vec<_>>()
            .join("  ")
    };

    let header_line = format_row(headers);
    let rule_len = header_line.len();
    let mut lines = Vec::with_capacity(rows.len() + 4);
    lines.push("=".repeat(rule_len));
    lines.push(header_line);
    lines.push("-".repeat(rule_len));
    for row in rows {
        let values = row.iter().map(String::as_str).collect::<Vec<_>>();
        lines.push(format_row(&values));
    }
    lines.push("=".repeat(rule_len));
    lines.join("\n")
}

fn normalize_expected_symbols(symbols: &[String]) -> HashSet<String> {
    symbols
        .iter()
        .map(|symbol| normalize_symbol(symbol))
        .filter(|symbol| !symbol.is_empty())
        .collect()
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase()
}

fn format_expected_symbols(symbols: &HashSet<String>) -> String {
    if symbols.is_empty() {
        return "-".to_string();
    }
    let mut symbols = symbols.iter().cloned().collect::<Vec<_>>();
    symbols.sort_unstable();
    symbols.join(",")
}

fn status_name(status: u8) -> String {
    match status {
        MODEL_STATUS_OK => "OK".to_string(),
        MODEL_STATUS_BAD_DIM => "BAD_DIM".to_string(),
        MODEL_STATUS_INFER_ERR => "INFER_ERR".to_string(),
        MODEL_STATUS_DECODE_ERR => "DECODE_ERR".to_string(),
        other => format!("UNKNOWN({other})"),
    }
}

fn sanitize_node_suffix(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model_msg(symbol: &str, ready: bool, status: u8) -> ModelMsg {
        ModelMsg::create(
            symbol.to_string(),
            1,
            2,
            0,
            0.25,
            Some(0.75),
            ready,
            status,
            vec![1, 2],
            vec![0.1, 0.2],
        )
    }

    #[test]
    fn aggregate_requires_data_and_latest_ready_messages() {
        let mut states = HashMap::new();
        let expected = HashSet::new();
        assert_eq!(aggregate_state(&states, &expected), AggregateState::NoData);

        states.insert(
            "BTCUSDT".to_string(),
            SymbolState::new(model_msg("BTCUSDT", true, MODEL_STATUS_OK)),
        );
        assert_eq!(
            aggregate_state(&states, &expected),
            AggregateState::AllReady
        );

        states
            .get_mut("BTCUSDT")
            .unwrap()
            .update(model_msg("BTCUSDT", false, MODEL_STATUS_OK));
        assert_eq!(
            aggregate_state(&states, &expected),
            AggregateState::NotReady
        );
    }

    #[test]
    fn aggregate_waits_for_expected_symbols() {
        let mut states = HashMap::new();
        states.insert(
            "BTCUSDT".to_string(),
            SymbolState::new(model_msg("BTCUSDT", true, MODEL_STATUS_OK)),
        );
        let expected = ["BTCUSDT".to_string(), "ETHUSDT".to_string()]
            .into_iter()
            .collect();

        assert_eq!(
            aggregate_state(&states, &expected),
            AggregateState::WaitingSymbols
        );
        let table = build_symbol_table(&states, &expected);
        assert!(table.contains("ETHUSDT"));
        assert!(table.contains("NOT_SEEN"));
    }

    #[test]
    fn status_error_prevents_ready_state() {
        let state = SymbolState::new(model_msg("BTCUSDT", true, MODEL_STATUS_INFER_ERR));
        assert!(!state.is_ready());
    }
}
