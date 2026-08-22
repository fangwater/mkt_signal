use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use period_pbs::kafka::{
    decode_period_payload, format_rewrites, KafkaConsumerConfig, KafkaPartitionWatermark,
    PayloadCompressionMode, RawKafkaConsumer,
};
use period_pbs::pb;

#[derive(Debug, Parser)]
#[command(name = "period_kafka_consumer")]
#[command(about = "Subscribe Kafka topics carrying compressed PeriodMessage protobuf payloads.")]
struct Args {
    /// TOML config path.
    #[arg(long, default_value = "config/period_kafka_consumer.toml")]
    config: PathBuf,

    /// Override Kafka topics from TOML. Can be supplied more than once or comma-separated.
    #[arg(long = "topic", alias = "topics", value_delimiter = ',')]
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

    /// Print earliest/latest offsets for configured topics and exit without consuming messages.
    #[arg(long, default_value_t = false)]
    print_offsets: bool,

    /// Override metadata fetch timeout in milliseconds.
    #[arg(long)]
    metadata_timeout_ms: Option<u64>,

    /// Override watermark query timeout in milliseconds.
    #[arg(long)]
    watermark_timeout_ms: Option<u64>,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let mut config = load_config(&args.config)?;
    apply_overrides(&mut config, &args);
    config.validate()?;

    let consumer = RawKafkaConsumer::new(&config)?;

    if args.print_offsets {
        let watermarks = consumer.query_topic_watermarks(
            &config.topics,
            config.metadata_timeout_ms,
            config.watermark_timeout_ms,
        )?;
        print_topic_watermarks(&watermarks);
        return Ok(());
    }

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
        config.topics = normalize_topics(&args.topics);
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
    if let Some(metadata_timeout_ms) = args.metadata_timeout_ms {
        config.metadata_timeout_ms = metadata_timeout_ms;
    }
    if let Some(watermark_timeout_ms) = args.watermark_timeout_ms {
        config.watermark_timeout_ms = watermark_timeout_ms;
    }
}

fn normalize_topics(raw_topics: &[String]) -> Vec<String> {
    raw_topics
        .iter()
        .flat_map(|topic| topic.split(','))
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn print_topic_watermarks(watermarks: &[KafkaPartitionWatermark]) {
    println!(
        "Kafka watermarks: earliest_offset is the low watermark; high_offset is the next offset"
    );
    for watermark in watermarks {
        let available_messages = if watermark.low >= 0 && watermark.high >= watermark.low {
            watermark.high - watermark.low
        } else {
            -1
        };
        println!(
            "topic={} partition={} earliest_offset={} high_offset={} available_messages={}",
            watermark.topic, watermark.partition, watermark.low, watermark.high, available_messages
        );
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
