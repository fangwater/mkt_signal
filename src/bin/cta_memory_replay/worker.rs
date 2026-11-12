use super::model::{Bootstrap, Candle, Memory, Point};
use anyhow::{ensure, Context, Result};
use mkt_signal::cta_factor_calc::{
    config::{Config, Market},
    engine::Engine,
};
use period_pbs::{
    kafka::{decode_period_payload, KafkaConsumerConfig, PayloadCompressionMode},
    period::normalize_timestamp_ms,
};
use rdkafka::{
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
    Message, Offset, TopicPartitionList,
};
use runtime_common::symbol_util::normalize_symbol_for_internal;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[derive(Default)]
struct Bars {
    candles: BTreeMap<(String, i64), Candle>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn closed_bar_vwap_is_weighted_by_trade_amount() {
        use period_pbs::pb::{PeriodMessage, SymbolInfo, TradeInfo};
        let start = 1_800_000_000_000;
        let period = PeriodMessage {
            period: 0,
            ts: start + 60_000,
            post_ts: start + 60_010,
            poster_id: "fixture".into(),
            symbol_infos: vec![SymbolInfo {
                symbol: "BTCUSDT".into(),
                incs: vec![],
                trades: vec![
                    TradeInfo {
                        timestamp: start + 1_000,
                        side: "buy".into(),
                        price: 100.0,
                        amount: 1.0,
                    },
                    TradeInfo {
                        timestamp: start + 30_000,
                        side: "sell".into(),
                        price: 200.0,
                        amount: 3.0,
                    },
                ],
            }],
        };
        let rows = Bars::default().ingest(&period, &BTreeSet::from(["BTCUSDT".into()]));
        assert_eq!(rows.len(), 1);
        let ((symbol, interval), bar) = &rows[0];
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(*interval, 60_000);
        assert_eq!(bar.volume, 4.0);
        assert_eq!(bar.turnover, 700.0);
        assert_eq!(bar.vwap(), Some(175.0));
        assert_eq!(bar.open, 100.0);
        assert_eq!(bar.close, 200.0);
    }
}

impl Bars {
    fn ingest(
        &mut self,
        period: &period_pbs::pb::PeriodMessage,
        symbols: &BTreeSet<String>,
    ) -> Vec<((String, i64), Candle)> {
        let mut closed = Vec::new();
        for info in &period.symbol_infos {
            let symbol = normalize_symbol_for_internal(&info.symbol);
            if !symbols.contains(&symbol) {
                continue;
            }
            let mut trades = info.trades.iter().collect::<Vec<_>>();
            trades.sort_by_key(|trade| normalize_timestamp_ms(trade.timestamp));
            for trade in trades {
                let ts = normalize_timestamp_ms(trade.timestamp);
                let notional = trade.price * trade.amount;
                if ts <= 0
                    || !trade.price.is_finite()
                    || trade.price <= 0.0
                    || !trade.amount.is_finite()
                    || trade.amount <= 0.0
                    || !notional.is_finite()
                {
                    continue;
                }
                for interval in [60_000, 300_000, 900_000] {
                    let key = (symbol.clone(), interval);
                    let start = ts / interval * interval;
                    let fresh = Candle {
                        ts_ms: start,
                        open: trade.price,
                        high: trade.price,
                        low: trade.price,
                        close: trade.price,
                        volume: 0.0,
                        turnover: 0.0,
                    };
                    let current = self.candles.entry(key.clone()).or_insert(fresh);
                    if start < current.ts_ms {
                        continue;
                    }
                    if start > current.ts_ms {
                        closed.push((key, *current));
                        *current = fresh;
                    }
                    current.high = current.high.max(trade.price);
                    current.low = current.low.min(trade.price);
                    current.close = trade.price;
                    current.volume += trade.amount;
                    current.turnover += notional;
                }
            }
        }
        let watermark = normalize_timestamp_ms(period.ts);
        let finished = self
            .candles
            .iter()
            .filter(|((_, interval), bar)| bar.ts_ms + interval <= watermark)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in finished {
            if let Some(bar) = self.candles.remove(&key) {
                closed.push((key, bar));
            }
        }
        closed.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.ts_ms.cmp(&b.1.ts_ms)));
        closed
    }
}

pub fn run(
    bootstrap: Bootstrap,
    brokers: String,
    history_days: u64,
    memory: Arc<RwLock<Memory>>,
) -> Result<()> {
    let plan = bootstrap.plan()?;
    let symbols = plan
        .calculations
        .values()
        .map(|calculation| calculation.id.symbol.clone())
        .collect::<BTreeSet<_>>();
    let topics = plan.topics();
    ensure!(
        topics == ["binance-futures"],
        "unexpected topics in current Prep profiles"
    );
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("client.id", "cta-memory-replay")
        .set(
            "group.id",
            format!("cta-memory-replay-{}", uuid::Uuid::new_v4()),
        )
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("allow.auto.create.topics", "false")
        .set("auto.offset.reset", "error")
        .set("fetch.message.max.bytes", "10485760")
        .set("fetch.max.bytes", "33554432")
        .set("queued.max.messages.kbytes", "65536")
        .create()
        .context("create memory-replay Kafka client")?;
    let apply_month = bootstrap
        .threshold_metadata
        .get("apply_month")
        .context("missing threshold apply_month")?;
    ensure!(
        apply_month == &chrono::Utc::now().format("%Y-%m").to_string(),
        "refresh JP monthly amount thresholds before starting replay"
    );
    let month_start = chrono::NaiveDate::parse_from_str(&format!("{apply_month}-01"), "%Y-%m-%d")?
        .and_hms_opt(0, 0, 0)
        .context("invalid threshold month")?
        .and_utc()
        .timestamp_millis();
    // Never apply this month's size classification to a preceding month.
    let start_ms = (now_ms() - (history_days as i64) * 86_400_000).max(month_start);
    let mut timestamps = TopicPartitionList::new();
    let mut ranges = BTreeMap::new();
    for topic in &topics {
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .context("fetch Kafka metadata")?;
        for topic_info in metadata.topics() {
            ensure!(topic_info.error().is_none(), "Kafka topic metadata error");
            for part in topic_info.partitions() {
                let (low, high) =
                    consumer.fetch_watermarks(topic, part.id(), Duration::from_secs(10))?;
                ensure!(high > low, "Kafka topic {topic} is empty");
                timestamps.add_partition_offset(topic, part.id(), Offset::Offset(start_ms))?;
                ranges.insert((topic.clone(), part.id()), (low, high));
            }
        }
    }
    let offsets = consumer
        .offsets_for_times(timestamps, Duration::from_secs(10))
        .context("locate historical Kafka start")?;
    let mut assignment = TopicPartitionList::new();
    let mut targets = BTreeMap::new();
    for element in offsets.elements() {
        element.error().context("Kafka timestamp lookup")?;
        let key = (element.topic().to_string(), element.partition());
        let (low, high) = ranges[&key];
        let offset = match element.offset() {
            Offset::Offset(value) => value.clamp(low, high),
            _ => high,
        };
        assignment.add_partition_offset(
            element.topic(),
            element.partition(),
            Offset::Offset(offset),
        )?;
        targets.insert(key.clone(), high - 1);
        memory.write().unwrap().partitions.insert(
            format!("{}:{}", key.0, key.1),
            (offset, offset - 1, high - 1),
        );
    }
    // Explicit assignment does not join or rebalance another consumer group.
    consumer.assign(&assignment)?;
    let engine_config = Config {
        combinations_dir: PathBuf::new(),
        history_dir: PathBuf::new(),
        lookback_secs: history_days * 86_400,
        catchup_timeout_secs: 5400,
        kafka: KafkaConsumerConfig::default(),
        amount_thresholds: bootstrap.amount_thresholds,
    };
    let mut engine = Engine::new(plan, &engine_config)?;
    let mut bars = Bars::default();
    let mut last_prune = Instant::now();
    let mut last_log = Instant::now();
    loop {
        let Some(message) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        let message = match message {
            Ok(value) => value,
            Err(error) => {
                memory.write().unwrap().error = Some(format!("Kafka: {error}"));
                std::thread::sleep(Duration::from_millis(200));
                continue;
            }
        };
        let processed = (|| -> Result<_> {
            let payload = message.payload().context("empty Kafka payload")?;
            let (_, _, period) = decode_period_payload(payload, PayloadCompressionMode::Zlib)?;
            let available =
                normalize_timestamp_ms(period.post_ts).max(normalize_timestamp_ms(period.ts));
            ensure!(
                available > 0 && available <= now_ms() + 60_000,
                "invalid source period timestamp"
            );
            ensure!(
                chrono::DateTime::from_timestamp_millis(available)
                    .is_some_and(|date| date.format("%Y-%m").to_string() == *apply_month),
                "source data crossed the loaded amount-threshold month; refresh the JP bootstrap"
            );
            let points = engine.on_period(Market::BinanceFutures, &period)?;
            let candles = bars.ingest(&period, &symbols);
            Ok((available, points, candles))
        })();
        let mut data = memory.write().unwrap();
        let (available, points, candles) = match processed {
            Ok(value) => value,
            Err(error) => {
                data.error = Some(format!(
                    "decode/calculation offset {}: {error:#}",
                    message.offset()
                ));
                continue;
            }
        };
        for point in points {
            let field = point
                .fields
                .get("value")
                .or_else(|| point.fields.get("long"))
                .cloned()
                .unwrap_or_default();
            let rows = data.points.entry(point.factor_id).or_default();
            if rows.back().is_none_or(|row| point.ts_ms > row.ts_ms) {
                rows.push_back(Point {
                    ts_ms: point.ts_ms,
                    available_ms: available.max(point.ts_ms),
                    value: field,
                    short: point.fields.get("short").cloned(),
                });
            }
        }
        for (key, candle) in candles {
            let rows = data.candles.entry(key).or_default();
            if rows.back().is_none_or(|row| candle.ts_ms > row.ts_ms) {
                rows.push_back(candle);
            }
        }
        data.kafka_records += 1;
        data.revision += 1;
        data.latest_data_ms = data.latest_data_ms.max(available);
        data.last_received_ms = now_ms();
        let key = format!("{}:{}", message.topic(), message.partition());
        if let Some(progress) = data.partitions.get_mut(&key) {
            progress.1 = message.offset();
        }
        data.caught_up = targets.iter().all(|((topic, partition), target)| {
            data.partitions
                .get(&format!("{topic}:{partition}"))
                .is_some_and(|row| row.1 >= *target)
        });
        // Decode errors remain visible rather than silently claiming complete history.
        if last_prune.elapsed() > Duration::from_secs(30) {
            let cutoff = data.latest_data_ms - (history_days as i64) * 86_400_000;
            data.prune(cutoff);
            last_prune = Instant::now();
        }
        if last_log.elapsed() > Duration::from_secs(30) {
            log::info!(
                "memory replay: records={} latest={} caught_up={} factor_series={}",
                data.kafka_records,
                data.latest_data_ms,
                data.caught_up,
                data.points.len()
            );
            last_log = Instant::now();
        }
    }
}
