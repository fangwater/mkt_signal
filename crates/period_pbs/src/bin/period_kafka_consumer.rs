use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use period_pbs::kafka::{
    decode_period_payload, format_rewrites, KafkaConsumerConfig, PayloadCompressionMode,
    RawKafkaConsumer,
};
use period_pbs::pb;

#[derive(Debug, Parser)]
#[command(name = "period_kafka_consumer")]
#[command(about = "Subscribe Kafka topics carrying compressed PeriodMessage protobuf payloads.")]
struct Args {
    /// TOML config path.
    #[arg(long, default_value = "config/period_kafka_consumer.toml")]
    config: PathBuf,

    /// Override Kafka topics from TOML. Can be supplied more than once.
    #[arg(long = "topic")]
    topics: Vec<String>,

    /// Override Kafka bootstrap servers from TOML.
    #[arg(long)]
    brokers: Option<String>,

    /// Override payload compression from TOML.
    #[arg(long, value_enum)]
    payload_compression: Option<PayloadCompressionMode>,

    /// Override max_messages from TOML. Use this for one-shot probes.
    #[arg(long)]
    max_messages: Option<u64>,

    /// Force per-symbol inc/trade count table.
    #[arg(long, default_value_t = false)]
    print_symbols: bool,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let mut config = load_config(&args.config)?;
    apply_overrides(&mut config, &args);

    let consumer = RawKafkaConsumer::new(&config)?;

    log::info!(
        "period_kafka_consumer subscribed config={} topics={} brokers={} group_id={} offset_reset={} payload_compression={:?} auto_commit={} rewrites={}",
        args.config.display(),
        config.topics.join(","),
        config.brokers,
        config.group_id,
        config.offset_reset,
        config.payload_compression,
        config.enable_auto_commit,
        format_rewrites(&config.broker_addr_rewrites),
    );

    let mut decoded_count = 0u64;
    loop {
        match consumer.poll(config.poll_timeout_ms) {
            None => continue,
            Some(Err(err)) => {
                log::error!("Kafka poll error: {:#}", err);
                continue;
            }
            Some(Ok(message)) => {
                if message.payload.is_empty() {
                    log::warn!(
                        "empty Kafka payload topic={} partition={} offset={}",
                        message.topic,
                        message.partition,
                        message.offset
                    );
                    continue;
                }

                match decode_period_payload(&message.payload, config.payload_compression) {
                    Ok((compression, decompressed_len, period)) => {
                        decoded_count += 1;
                        print_period_message(
                            &period,
                            config.print_symbols,
                            &message.topic,
                            message.partition,
                            message.offset,
                            compression,
                            message.payload.len(),
                            decompressed_len,
                        );
                        if config.max_messages.is_some_and(|max| decoded_count >= max) {
                            break;
                        }
                    }
                    Err(err) => {
                        log::error!(
                            "decode failed topic={} partition={} offset={} compressed_bytes={} err={:#}",
                            message.topic,
                            message.partition,
                            message.offset,
                            message.payload.len(),
                            err
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

fn load_config(path: &PathBuf) -> Result<KafkaConsumerConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read Kafka consumer config {}", path.display()))?;
    let config: KafkaConsumerConfig = toml::from_str(&text)
        .with_context(|| format!("parse Kafka consumer config {}", path.display()))?;
    config.validate()?;
    Ok(config)
}

fn apply_overrides(config: &mut KafkaConsumerConfig, args: &Args) {
    if !args.topics.is_empty() {
        config.topics = args.topics.clone();
    }
    if let Some(brokers) = &args.brokers {
        config.brokers = brokers.clone();
    }
    if let Some(payload_compression) = args.payload_compression {
        config.payload_compression = payload_compression;
    }
    if let Some(max_messages) = args.max_messages {
        config.max_messages = Some(max_messages);
    }
    if args.print_symbols {
        config.print_symbols = true;
    }
}

#[allow(clippy::too_many_arguments)]
fn print_period_message(
    period: &pb::PeriodMessage,
    print_symbols: bool,
    topic: &str,
    partition: i32,
    offset: i64,
    compression: &str,
    compressed_len: usize,
    decompressed_len: usize,
) {
    let trade_count: usize = period.symbol_infos.iter().map(|s| s.trades.len()).sum();
    let inc_count: usize = period.symbol_infos.iter().map(|s| s.incs.len()).sum();
    println!(
        "topic={} partition={} offset={} compression={} compressed_bytes={} pb_bytes={} period={} ts={} post_ts={} poster_id={} symbols={} trades={} incs={}",
        topic,
        partition,
        offset,
        compression,
        compressed_len,
        decompressed_len,
        period.period,
        period.ts,
        period.post_ts,
        period.poster_id,
        period.symbol_infos.len(),
        trade_count,
        inc_count
    );

    if print_symbols {
        print_symbol_table(period, inc_count, trade_count);
    }
}

fn print_symbol_table(period: &pb::PeriodMessage, inc_count: usize, trade_count: usize) {
    const SYMBOL_W: usize = 24;
    const INC_W: usize = 12;
    const TRADE_W: usize = 12;
    let width = SYMBOL_W + INC_W + TRADE_W + 10;
    let line = "-".repeat(width);

    println!("+{}+", line);
    println!("|{:^width$}|", "Kafka PeriodMessage", width = width);
    println!(
        "| {:<width$} |",
        format!("period: {} ts: {}", period.period, period.ts),
        width = width - 2
    );
    println!("+{}+", line);
    println!(
        "| {:<SYMBOL_W$} | {:>INC_W$} | {:>TRADE_W$} |",
        "Symbol", "Inc_Count", "Trade_Count"
    );
    println!("+{}+", line);
    for symbol_info in &period.symbol_infos {
        println!(
            "| {:<SYMBOL_W$} | {:>INC_W$} | {:>TRADE_W$} |",
            symbol_info.symbol,
            symbol_info.incs.len(),
            symbol_info.trades.len()
        );
    }
    println!("+{}+", line);
    println!(
        "| {:<SYMBOL_W$} | {:>INC_W$} | {:>TRADE_W$} |",
        "TOTAL", inc_count, trade_count
    );
    println!("+{}+", line);
}
