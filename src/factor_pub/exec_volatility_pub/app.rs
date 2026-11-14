use super::cfg::VolatilityPubConfig;
use super::state::{VolatilityFactors, VolatilityOutputs};
use anyhow::{ensure, Context, Result};
use period_pbs::kafka::{decode_period_payload, RawKafkaConsumer, RawKafkaMessage};
use period_pbs::period::normalize_timestamp_ms;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// One shared owner of the existing execution-volatility IPC services.
pub struct ExecVolatilityPubApp {
    config: VolatilityPubConfig,
    consumer: RawKafkaConsumer,
    factors: VolatilityFactors,
    outputs: Option<VolatilityOutputs>,
    status_path: PathBuf,
    history_start_ms: i64,
    caught_up: bool,
    records: u64,
    decode_errors: u64,
    published: u64,
    publish_failed: u64,
    last_status: Instant,
}

impl ExecVolatilityPubApp {
    pub fn new(path: &str, status_path: &Path, wait_for_publishers: bool) -> Result<Self> {
        let config = VolatilityPubConfig::load(path)?;
        let now = runtime_common::time_util::get_timestamp_us() / 1000;
        let mut consumer_config = config.kafka.consumer.clone();
        consumer_config.group_id =
            format!("{}-{}-{now}", consumer_config.group_id, std::process::id());
        consumer_config.client_id =
            format!("{}-{}-{now}", consumer_config.client_id, std::process::id());
        consumer_config.offset_reset = "earliest".into();
        consumer_config.enable_auto_commit = false;
        let consumer = RawKafkaConsumer::new(&consumer_config)?;
        let mut app = Self {
            history_start_ms: now.saturating_sub(
                config
                    .kafka
                    .lookback_secs
                    .saturating_mul(1000)
                    .min(i64::MAX as u64) as i64,
            ),
            config,
            consumer,
            factors: VolatilityFactors::default(),
            outputs: None,
            status_path: status_path.to_path_buf(),
            caught_up: false,
            records: 0,
            decode_errors: 0,
            published: 0,
            publish_failed: 0,
            last_status: Instant::now(),
        };
        app.write_status()?;
        app.catch_up()?;
        ensure!(
            app.decode_errors == 0,
            "execution volatility replay had decode errors"
        );
        app.caught_up = true;
        app.write_status()?;
        let mut last_warning = None;
        loop {
            match VolatilityOutputs::new() {
                Ok(outputs) => {
                    app.outputs = Some(outputs);
                    let latest = app.factors.latest();
                    app.publish(&latest);
                    app.write_status()?;
                    log::info!("execution volatility publisher owns shared outputs");
                    break;
                }
                Err(error) if wait_for_publishers => {
                    if last_warning
                        .is_none_or(|last: Instant| last.elapsed() >= Duration::from_secs(60))
                    {
                        log::info!(
                            "execution volatility ready; waiting for previous publisher: {error:#}"
                        );
                        last_warning = Some(Instant::now());
                    }
                    app.poll();
                    app.maybe_write_status();
                }
                Err(error) => return Err(error).context("create execution volatility outputs"),
            }
        }
        Ok(app)
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            self.poll();
            self.maybe_write_status();
        }
    }

    fn poll(&mut self) {
        if let Some(record) = self
            .consumer
            .poll(self.config.kafka.consumer.poll_timeout_ms.max(1))
        {
            match record {
                Ok(record) => self.consume(record),
                Err(error) => log::warn!("execution volatility Kafka read failed: {error:#}"),
            }
        }
    }

    fn consume(&mut self, record: RawKafkaMessage) {
        self.records += 1;
        if record.topic != self.config.kafka.topic && record.topic != self.config.kafka.cash_topic {
            return;
        }
        let period = match decode_period_payload(
            &record.payload,
            self.config.kafka.consumer.payload_compression,
        ) {
            Ok((_, _, period)) => period,
            Err(error) => {
                self.decode_errors += 1;
                log::warn!("execution volatility PeriodMessage decode failed: {error:#}");
                return;
            }
        };
        if normalize_timestamp_ms(period.ts) < self.history_start_ms {
            return;
        }
        let observations = self
            .factors
            .consume_period(&period, record.topic == self.config.kafka.cash_topic);
        self.publish(&observations);
    }

    fn publish(&mut self, observations: &[super::state::VolatilityObservation]) {
        if let Some(outputs) = &mut self.outputs {
            let now = runtime_common::time_util::get_timestamp_us() / 1000;
            let (published, failed) = outputs.publish(observations, now);
            self.published += published;
            self.publish_failed += failed;
        }
    }

    fn catch_up(&mut self) -> Result<()> {
        let config = &self.config.kafka.consumer;
        let watermarks = self.consumer.query_topic_watermarks(
            &config.topics,
            config.metadata_timeout_ms,
            config.watermark_timeout_ms,
        )?;
        let targets: HashMap<_, _> = watermarks
            .into_iter()
            .filter(|watermark| watermark.high > watermark.low)
            .map(|watermark| ((watermark.topic, watermark.partition), watermark.high - 1))
            .collect();
        ensure!(
            config
                .topics
                .iter()
                .all(|topic| targets.keys().any(|(name, _)| name == topic)),
            "both volatility markets must have retained records"
        );
        let deadline = Instant::now() + Duration::from_secs(self.config.kafka.catchup_timeout_secs);
        let mut reached = HashSet::new();
        while reached.len() < targets.len() {
            ensure!(
                Instant::now() < deadline,
                "execution volatility Kafka catch-up timed out"
            );
            if let Some(record) = self
                .consumer
                .poll(self.config.kafka.consumer.poll_timeout_ms.max(1))
            {
                let record = record.context("read execution volatility replay record")?;
                let key = (record.topic.clone(), record.partition);
                if targets
                    .get(&key)
                    .is_some_and(|target| record.offset >= *target)
                {
                    reached.insert(key);
                }
                self.consume(record);
            }
            self.maybe_write_status();
        }
        Ok(())
    }

    fn maybe_write_status(&mut self) {
        if self.last_status.elapsed() >= Duration::from_secs(1) {
            if let Err(error) = self.write_status() {
                log::warn!("write execution volatility status failed: {error:#}");
            }
        }
    }

    fn write_status(&mut self) -> Result<()> {
        let now = runtime_common::time_util::get_timestamp_us() / 1000;
        let samples: Vec<_> = self.factors.latest().into_iter()
            .filter(|observation| matches!(observation.symbol.as_str(), "BTCUSDT" | "ETHUSDT"))
            .map(|observation| serde_json::json!({"symbol":observation.symbol,"cash":observation.cash,"ts_ms":observation.ts_ms,"value":observation.value}))
            .collect();
        let status = serde_json::json!({
            "pid":std::process::id(),"updated_ms":now,"caught_up":self.caught_up,
            "publishing":self.outputs.is_some(),"records":self.records,"decode_errors":self.decode_errors,
            "published":self.published,"publish_failed":self.publish_failed,
            "symbols":self.factors.symbol_counts(),"fresh_ready":self.factors.ready_counts(now,15000),
            "samples":samples,
        });
        if let Some(parent) = self.status_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = self.status_path.with_extension("json.tmp");
        std::fs::write(&temporary, serde_json::to_vec(&status)?)?;
        std::fs::rename(temporary, &self.status_path)?;
        self.last_status = Instant::now();
        Ok(())
    }
}
