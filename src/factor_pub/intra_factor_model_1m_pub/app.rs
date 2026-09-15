use anyhow::{bail, Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mkt_parsers::msg::mkt_msg::Level;
use mkt_parsers::msg::mkt_msg::{ModelMsg, MODEL_STATUS_OK};
use mkt_parsers::msg::trade_flow_feature_msg::{
    TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_HISTORY_SIZE, TRADE_FLOW_FEATURE_MAX_BYTES,
};
use order_common::TradingVenue;
use period_pbs::kafka::{decode_period_payload, RawKafkaConsumer};
use period_pbs::pb::{IncrementOrderBookInfo, PeriodMessage, TradeInfo};
use period_pbs::period::normalize_timestamp_ms;
use runtime_common::symbol_util::normalize_symbol_for_venue;

use crate::common::amount_threshold::AmountThreshold;
use crate::common::msg_parser::parse_trade_flow_feature;
use crate::common::sliding_quantile::SlidingQuantileWindow;
use crate::factor_pub::fusion_factor_pub::app::{
    load_amount_thresholds_from_tlen_server, load_online_symbols_from_tlen_server,
    BaselineReplayState,
};
use crate::factor_pub::fusion_factor_pub::cfg::TlenServerConfig;
use crate::factor_pub::fusion_factor_pub::plan::load_symbol_factor_plans_from_tlen_server;
use crate::factor_pub::fusion_factor_pub::SymbolFactorPlan;
use crate::factor_pub::model_output_publisher::ModelPublisher;
use crate::factor_pub::trade_flow_feature_pub::local_baseline::{
    BaselineBar, LocalBaselineAggregator,
};

use super::cfg::{IntraFactorModelPubConfig, KafkaWarmupConfig};

const IDLE_SLEEP_MICROS: u64 = 200;
const STATS_LOG_INTERVAL_SECS: u64 = 60;
const SYMBOL_RELOAD_WARN_INTERVAL_SECS: u64 = 60;
const TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE: usize = 8192;
const TRADE_FLOW_MAX_SUBSCRIBERS: usize = 10;
const FACTOR_PLAN_CONFIG_TYPE: &str = "factor_plan_1m";
const AMOUNT_THRESHOLD_CONFIG_TYPE: &str = "amount_thresholds_1m";
const TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE: &str = "amount_thresholds";
const ONE_MINUTE_MS: i64 = 60_000;

pub const INTRA_FACTOR_NAMES: [&str; 9] = [
    "baseline_035",
    "TD_PR_011",
    "baseline_053",
    "TP_VPI_006",
    "TD_PR_005",
    "factor_116",
    "net_buy_medium",
    "factor_004",
    "baseline_091",
];

struct FactorOutput {
    factor_name: &'static str,
    service_path: String,
    publisher: ModelPublisher,
    seq_no: u64,
}

struct SymbolState {
    evaluator: BaselineReplayState,
    windows: Vec<SlidingQuantileWindow>,
    last_trade_flow_ts: Option<i64>,
}

impl SymbolState {
    fn new(window_size: usize) -> Self {
        Self {
            evaluator: BaselineReplayState::default(),
            windows: (0..INTRA_FACTOR_NAMES.len())
                .map(|_| SlidingQuantileWindow::new(window_size, window_size))
                .collect(),
            last_trade_flow_ts: None,
        }
    }

    fn observe(&mut self, raw_values: Vec<f64>, min_samples: usize) -> Vec<FactorObservation> {
        debug_assert_eq!(raw_values.len(), self.windows.len());
        raw_values
            .into_iter()
            .zip(self.windows.iter_mut())
            .map(|(score, window)| {
                let score_quantile = window
                    .push_f64(score)
                    .then(|| window.percentile_rank_last())
                    .flatten();
                let score_ready = score_quantile.is_some() && window.sample_size() >= min_samples;
                FactorObservation {
                    score,
                    score_quantile,
                    score_ready,
                }
            })
            .collect()
    }

    fn evaluate(
        &mut self,
        msg: TradeFlowFeatureMsg,
        plan: &SymbolFactorPlan,
        min_samples: usize,
        record_percentile: bool,
    ) -> Result<Option<Vec<FactorObservation>>> {
        if self
            .last_trade_flow_ts
            .is_some_and(|last_ts| msg.ts <= last_ts)
        {
            return Ok(None);
        }
        self.evaluator.push(msg.clone())?;
        self.last_trade_flow_ts = Some(msg.ts);
        let raw_values = self.evaluator.factor_values(plan);
        Ok(record_percentile.then(|| self.observe(raw_values, min_samples)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct FactorObservation {
    score: f64,
    score_quantile: Option<f64>,
    score_ready: bool,
}

#[derive(Default)]
struct IntraFactorModelStats {
    raw_messages: u64,
    accepted_messages: u64,
    decode_errors: u64,
    evaluation_errors: u64,
    published: u64,
    publish_failed: u64,
    ready: u64,
}

#[derive(Default)]
struct KafkaWarmupStats {
    kafka_records: u64,
    decoded_periods: u64,
    decode_errors: u64,
    ignored_symbols: u64,
    missing_thresholds: u64,
    synthetic_book_seeds: u64,
    replayed_events: u64,
    sixty_second_bars: u64,
    invalid_price_bars: u64,
    invalid_depth_bars: u64,
    historical_bars: u64,
    percentile_samples: u64,
    evaluation_errors: u64,
}

/// Publishes each notebook factor as a separate raw-value virtual model.
///
/// `ModelMsg.score` is the raw factor value. `score_quantile` is the current
/// percentile rank of that raw value within its own per-symbol rolling window.
pub struct IntraFactorModel1mPubApp {
    venue: TradingVenue,
    venue_slug: String,
    tlen_server: TlenServerConfig,
    window_size: usize,
    min_samples: usize,
    plan: SymbolFactorPlan,
    trade_flow_subscriber: Option<Subscriber<ipc::Service, [u8; TRADE_FLOW_FEATURE_MAX_BYTES], ()>>,
    outputs: Vec<FactorOutput>,
    allowed_symbols: HashSet<String>,
    states: HashMap<String, SymbolState>,
    last_symbol_reload: Instant,
    symbol_reload_interval: Duration,
    last_symbol_reload_warn: Instant,
    last_stats_log: Instant,
    stats: IntraFactorModelStats,
}

enum PeriodEvent<'a> {
    Book(&'a IncrementOrderBookInfo),
    Trade(&'a TradeInfo),
}

impl PeriodEvent<'_> {
    fn timestamp_ms(&self) -> i64 {
        let timestamp = match self {
            Self::Book(book) => book.timestamp,
            Self::Trade(trade) => trade.timestamp,
        };
        normalize_timestamp_ms(timestamp)
    }

    fn sort_order(&self) -> u8 {
        // PeriodMessage keeps trade and book vectors separately, so equal-time
        // ordering is unavailable. Applying the book first makes the right-edge
        // snapshot deterministic before a same-timestamp trade closes a bucket.
        match self {
            Self::Book(_) => 0,
            Self::Trade(_) => 1,
        }
    }
}

impl IntraFactorModel1mPubApp {
    pub async fn new(config_path: &str, venue: TradingVenue) -> Result<Self> {
        let config = IntraFactorModelPubConfig::load(config_path)?;
        let venue_slug = venue.data_pub_slug().to_string();
        let plan = SymbolFactorPlan::from_factor_names(
            "intra_factor_model_1m",
            INTRA_FACTOR_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        )?;
        BaselineReplayState::validate_factor_plan(&plan)
            .context("validate intra 1m raw factor plan")?;

        let allowed_symbols = load_enabled_symbols(&config.tlen_server, venue, &venue_slug)
            .await
            .with_context(|| {
                format!(
                    "load intra 1m enabled symbols failed: venue={} factor_plan={}",
                    venue_slug, FACTOR_PLAN_CONFIG_TYPE
                )
            })?;
        if allowed_symbols.is_empty() {
            bail!(
                "no online symbols have the required 1m intra factor plan: venue={} factors={:?}",
                venue_slug,
                INTRA_FACTOR_NAMES
            );
        }

        // Subscribe before the synchronous Kafka replay so its large buffer bridges
        // records produced while warming. Historical overlap is removed by timestamp.
        let trade_flow_subscriber = create_trade_flow_subscriber(&venue_slug)?;
        let mut outputs = Vec::with_capacity(INTRA_FACTOR_NAMES.len());
        for factor_name in INTRA_FACTOR_NAMES {
            let service_path = output_service_path(&venue_slug, factor_name);
            let node_name = format!(
                "intra_factor_model_1m_{}_{}",
                sanitize_node_component(&venue_slug),
                sanitize_node_component(factor_name)
            );
            let publisher = ModelPublisher::new(&node_name, &service_path).with_context(|| {
                format!(
                    "create intra factor model publisher failed: factor={} service={}",
                    factor_name, service_path
                )
            })?;
            outputs.push(FactorOutput {
                factor_name,
                service_path,
                publisher,
                seq_no: 0,
            });
        }

        let output_services: Vec<String> = outputs
            .iter()
            .map(|output| output.service_path.clone())
            .collect();
        let mut app = Self {
            venue,
            venue_slug,
            tlen_server: config.tlen_server.clone(),
            window_size: config.percentile.window_size,
            min_samples: config.percentile.min_samples,
            plan,
            trade_flow_subscriber: Some(trade_flow_subscriber),
            outputs,
            allowed_symbols,
            states: HashMap::new(),
            last_symbol_reload: Instant::now(),
            symbol_reload_interval: Duration::from_secs(config.tlen_server.symbol_reload_secs),
            last_symbol_reload_warn: Instant::now()
                - Duration::from_secs(SYMBOL_RELOAD_WARN_INTERVAL_SECS),
            last_stats_log: Instant::now(),
            stats: IntraFactorModelStats::default(),
        };

        if config.warmup.enabled {
            match load_amount_thresholds_from_tlen_server(
                &config.tlen_server,
                venue,
                &app.venue_slug,
                TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE,
            )
            .await
            {
                Ok(thresholds) => {
                    if let Err(err) = app.warm_from_kafka(&config.warmup, &thresholds) {
                        if config.warmup.required {
                            return Err(err).context("required Kafka warmup failed");
                        }
                        warn!(
                            "Kafka warmup failed; starting cold with score_ready=false until live history accumulates: venue={} err={:#}",
                            app.venue_slug, err
                        );
                    }
                }
                Err(err) if config.warmup.required => {
                    return Err(err).context("load required warmup amount thresholds failed");
                }
                Err(err) => warn!(
                    "Kafka warmup skipped because realtime amount thresholds could not be loaded; starting cold: venue={} err={:#}",
                    app.venue_slug, err
                ),
            }
        }

        info!(
            "IntraFactorModel1mPubApp started: venue={} input=factor_pub/{}/trade_flow_feature_1m symbols={} sample={} percentile_window={} min_samples={} output_services={:?}",
            app.venue_slug,
            app.venue_slug,
            app.allowed_symbols.len(),
            format_symbol_sample(&app.allowed_symbols),
            app.window_size,
            app.min_samples,
            output_services,
        );

        Ok(app)
    }

    pub async fn run(&mut self) -> Result<()> {
        self.prepare_run()?;

        loop {
            self.maybe_reload_symbols().await;
            let has_message = self.poll_trade_flow()?;
            self.maybe_log_stats();
            if !has_message {
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }
        }
    }

    fn prepare_run(&mut self) -> Result<()> {
        info!(
            "IntraFactorModel1mPubApp ready: venue={} symbols={} window={} min_samples={}",
            self.venue_slug,
            self.allowed_symbols.len(),
            self.window_size,
            self.min_samples,
        );
        Ok(())
    }

    async fn maybe_reload_symbols(&mut self) {
        if self.last_symbol_reload.elapsed() < self.symbol_reload_interval {
            return;
        }
        self.last_symbol_reload = Instant::now();

        match load_enabled_symbols(&self.tlen_server, self.venue, &self.venue_slug).await {
            Ok(symbols) if symbols.is_empty() => self.warn_reload_throttled(&format!(
                "intra factor 1m symbol reload returned no enabled symbols; retaining previous set: venue={}",
                self.venue_slug
            )),
            Ok(symbols) if symbols == self.allowed_symbols => {}
            Ok(symbols) => {
                let retired: HashSet<String> = self
                    .allowed_symbols
                    .difference(&symbols)
                    .cloned()
                    .collect();
                self.allowed_symbols = symbols;
                self.states
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
                info!(
                    "IntraFactorModel1mPubApp symbols reloaded: venue={} enabled={} sample={} retired={} retired_sample={}",
                    self.venue_slug,
                    self.allowed_symbols.len(),
                    format_symbol_sample(&self.allowed_symbols),
                    retired.len(),
                    format_symbol_sample(&retired),
                );
            }
            Err(err) => self.warn_reload_throttled(&format!(
                "intra factor 1m symbol reload failed: venue={} err={:#}",
                self.venue_slug, err
            )),
        }
    }

    fn warn_reload_throttled(&mut self, message: &str) {
        if self.last_symbol_reload_warn.elapsed()
            >= Duration::from_secs(SYMBOL_RELOAD_WARN_INTERVAL_SECS)
        {
            warn!("{}", message);
            self.last_symbol_reload_warn = Instant::now();
        }
    }

    fn poll_trade_flow(&mut self) -> Result<bool> {
        let mut has_message = false;
        loop {
            let parsed = {
                let subscriber = self
                    .trade_flow_subscriber
                    .as_ref()
                    .context("trade-flow subscriber was not initialized")?;
                let Some(sample) = subscriber.receive()? else {
                    break;
                };
                parse_trade_flow_feature(sample.payload())
            };
            has_message = true;
            self.stats.raw_messages = self.stats.raw_messages.saturating_add(1);
            let msg = match parsed {
                Ok(msg) => msg,
                Err(err) => {
                    self.stats.decode_errors = self.stats.decode_errors.saturating_add(1);
                    warn!(
                        "intra factor 1m trade-flow decode failed: venue={} err={}",
                        self.venue_slug, err
                    );
                    continue;
                }
            };
            let symbol = normalize_symbol_for_venue(&msg.symbol, self.venue);
            if !self.allowed_symbols.contains(&symbol) {
                continue;
            }
            self.on_trade_flow(symbol, msg);
        }
        Ok(has_message)
    }

    fn on_trade_flow(&mut self, symbol: String, msg: TradeFlowFeatureMsg) {
        let ts_in_ms = msg.ts / 1_000;
        let observations = match {
            let state = self
                .states
                .entry(symbol.clone())
                .or_insert_with(|| SymbolState::new(self.window_size));
            state.evaluate(msg, &self.plan, self.min_samples, true)
        } {
            Ok(Some(observations)) => observations,
            Ok(None) => return,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!(
                    "intra factor 1m raw evaluation failed: venue={} symbol={} err={:#}",
                    self.venue_slug, symbol, err
                );
                return;
            }
        };
        self.stats.accepted_messages = self.stats.accepted_messages.saturating_add(1);

        for (output, observation) in self.outputs.iter_mut().zip(observations) {
            output.seq_no = output.seq_no.saturating_add(1);
            let model_msg = ModelMsg::create(
                symbol.clone(),
                ts_in_ms,
                now_millis(),
                output.seq_no,
                observation.score,
                observation.score_quantile,
                observation.score_ready,
                MODEL_STATUS_OK,
                Vec::new(),
                Vec::new(),
            );
            let published = model_msg
                .to_bytes()
                .map(|bytes| output.publisher.publish(&bytes))
                .unwrap_or(false);
            if published {
                self.stats.published = self.stats.published.saturating_add(1);
                if observation.score_ready {
                    self.stats.ready = self.stats.ready.saturating_add(1);
                }
            } else {
                self.stats.publish_failed = self.stats.publish_failed.saturating_add(1);
                warn!(
                    "intra factor 1m publish failed: venue={} symbol={} factor={} service={}",
                    self.venue_slug, symbol, output.factor_name, output.service_path
                );
            }
        }
    }

    fn warm_from_kafka(
        &mut self,
        config: &KafkaWarmupConfig,
        thresholds: &HashMap<String, AmountThreshold>,
    ) -> Result<()> {
        let now_ms = now_millis();
        let history_end_ms = align_to_minute(
            now_ms.saturating_sub(config.tail_guard_secs.saturating_mul(1_000) as i64),
        );
        let history_start_ms =
            history_end_ms.saturating_sub(config.lookback_secs.saturating_mul(1_000) as i64);
        if history_end_ms <= history_start_ms {
            bail!(
                "invalid Kafka warmup range: start={} end={}",
                history_start_ms,
                history_end_ms
            );
        }

        let mut kafka_config = config.kafka.clone();
        let run_id = format!("{}-{}", std::process::id(), now_ms);
        kafka_config.group_id = format!("{}-{}", kafka_config.group_id, run_id);
        kafka_config.client_id = format!("{}-{}", kafka_config.client_id, run_id);
        // A warmup must always read retained history and must never commit offsets.
        kafka_config.offset_reset = "earliest".to_string();
        kafka_config.enable_auto_commit = false;
        let consumer = RawKafkaConsumer::new(&kafka_config).context("create Kafka consumer")?;
        let watermarks = consumer
            .query_topic_watermarks(
                &kafka_config.topics,
                kafka_config.metadata_timeout_ms,
                kafka_config.watermark_timeout_ms,
            )
            .context("query Kafka warmup watermarks")?;
        let target_offsets: HashMap<(String, i32), i64> = watermarks
            .iter()
            .filter(|watermark| watermark.high > watermark.low)
            .map(|watermark| {
                (
                    (watermark.topic.clone(), watermark.partition),
                    watermark.high.saturating_sub(1),
                )
            })
            .collect();
        if target_offsets.is_empty() {
            bail!(
                "Kafka warmup has no retained records: topics={:?}",
                kafka_config.topics
            );
        }

        info!(
            "Kafka warmup starting: venue={} topics={:?} symbols={} range=[{}, {}) partitions={} tail_guard_secs={}",
            self.venue_slug,
            kafka_config.topics,
            self.allowed_symbols.len(),
            history_start_ms,
            history_end_ms,
            target_offsets.len(),
            config.tail_guard_secs,
        );

        let deadline = Instant::now() + Duration::from_secs(config.max_wait_secs);
        let mut reached_offsets = HashSet::new();
        let mut aggregators: HashMap<String, LocalBaselineAggregator> = HashMap::new();
        let mut seeded_books = HashSet::new();
        let mut stats = KafkaWarmupStats::default();

        while reached_offsets.len() < target_offsets.len() {
            if Instant::now() >= deadline {
                bail!(
                    "Kafka warmup timed out after {}s: reached_partitions={} total_partitions={}",
                    config.max_wait_secs,
                    reached_offsets.len(),
                    target_offsets.len()
                );
            }
            let Some(record) = consumer.poll(kafka_config.poll_timeout_ms.max(1)) else {
                continue;
            };
            let record = record.context("read Kafka warmup record")?;
            stats.kafka_records = stats.kafka_records.saturating_add(1);
            let partition_key = (record.topic.clone(), record.partition);
            if let Some(target) = target_offsets.get(&partition_key) {
                if record.offset >= *target {
                    reached_offsets.insert(partition_key);
                }
            }

            let period = match decode_period_payload(
                &record.payload,
                kafka_config.payload_compression,
            ) {
                Ok((_, _, period)) => period,
                Err(err) => {
                    stats.decode_errors = stats.decode_errors.saturating_add(1);
                    warn!(
                        "Kafka warmup PeriodMessage decode failed: venue={} topic={} partition={} offset={} err={:#}",
                        self.venue_slug, record.topic, record.partition, record.offset, err
                    );
                    continue;
                }
            };
            stats.decoded_periods = stats.decoded_periods.saturating_add(1);
            self.replay_period_message(
                &period,
                thresholds,
                &mut aggregators,
                &mut seeded_books,
                history_start_ms,
                history_end_ms,
                &mut stats,
            );
        }

        for (symbol, aggregator) in aggregators.iter_mut() {
            aggregator.flush_until_ms(history_end_ms);
            for bar in aggregator.drain_sixty_second_bars() {
                self.consume_historical_bar(symbol, bar, history_start_ms, &mut stats);
            }
        }
        info!(
            "Kafka warmup completed: venue={} kafka_records={} decoded_periods={} decode_errors={} ignored_symbols={} missing_thresholds={} synthetic_book_seeds={} replayed_events={} sixty_second_bars={} invalid_price_bars={} invalid_depth_bars={} historical_bars={} percentile_samples={} evaluation_errors={} state_symbols={}",
            self.venue_slug,
            stats.kafka_records,
            stats.decoded_periods,
            stats.decode_errors,
            stats.ignored_symbols,
            stats.missing_thresholds,
            stats.synthetic_book_seeds,
            stats.replayed_events,
            stats.sixty_second_bars,
            stats.invalid_price_bars,
            stats.invalid_depth_bars,
            stats.historical_bars,
            stats.percentile_samples,
            stats.evaluation_errors,
            self.states.len(),
        );
        Ok(())
    }

    fn replay_period_message(
        &mut self,
        period: &PeriodMessage,
        thresholds: &HashMap<String, AmountThreshold>,
        aggregators: &mut HashMap<String, LocalBaselineAggregator>,
        seeded_books: &mut HashSet<String>,
        history_start_ms: i64,
        history_end_ms: i64,
        stats: &mut KafkaWarmupStats,
    ) {
        for symbol_info in &period.symbol_infos {
            let symbol = normalize_symbol_for_venue(&symbol_info.symbol, self.venue);
            if !self.allowed_symbols.contains(&symbol) {
                stats.ignored_symbols = stats.ignored_symbols.saturating_add(1);
                continue;
            }
            let Some(threshold) = thresholds.get(&symbol).copied() else {
                stats.missing_thresholds = stats.missing_thresholds.saturating_add(1);
                continue;
            };

            let mut events = Vec::with_capacity(symbol_info.trades.len() + symbol_info.incs.len());
            events.extend(symbol_info.incs.iter().map(PeriodEvent::Book));
            events.extend(symbol_info.trades.iter().map(PeriodEvent::Trade));
            events.sort_unstable_by_key(|event| (event.timestamp_ms(), event.sort_order()));

            let aggregator = aggregators.entry(symbol.clone()).or_default();
            for event in events {
                if event.timestamp_ms() >= history_end_ms {
                    continue;
                }
                match event {
                    PeriodEvent::Book(book) => {
                        let bids: Vec<Level> = book
                            .bids
                            .iter()
                            .map(|level| Level::from_values(level.price, level.amount))
                            .collect();
                        let asks: Vec<Level> = book
                            .asks
                            .iter()
                            .map(|level| Level::from_values(level.price, level.amount))
                            .collect();
                        // Kafka retains deltas but need not retain the original L2 snapshot.
                        // Seed from the first two-sided update solely during history replay.
                        let is_seed =
                            !seeded_books.contains(&symbol) && !bids.is_empty() && !asks.is_empty();
                        let is_snapshot = book.is_snapshot || is_seed;
                        if is_snapshot && seeded_books.insert(symbol.clone()) && !book.is_snapshot {
                            stats.synthetic_book_seeds =
                                stats.synthetic_book_seeds.saturating_add(1);
                        }
                        aggregator.on_book(
                            timestamp_as_micros(book.timestamp),
                            is_snapshot,
                            &bids,
                            &asks,
                        );
                    }
                    PeriodEvent::Trade(trade) => {
                        let Some(is_buy) = parse_trade_side(&trade.side) else {
                            continue;
                        };
                        aggregator.on_trade_with_threshold(
                            timestamp_as_micros(trade.timestamp),
                            is_buy,
                            trade.price,
                            trade.amount,
                            threshold,
                        );
                    }
                }
                stats.replayed_events = stats.replayed_events.saturating_add(1);
                for bar in aggregator.drain_sixty_second_bars() {
                    self.consume_historical_bar(&symbol, bar, history_start_ms, stats);
                }
            }
        }
    }

    fn consume_historical_bar(
        &mut self,
        symbol: &str,
        bar: BaselineBar,
        history_start_ms: i64,
        stats: &mut KafkaWarmupStats,
    ) {
        stats.sixty_second_bars = stats.sixty_second_bars.saturating_add(1);
        if !historical_bar_has_valid_prices(&bar) {
            stats.invalid_price_bars = stats.invalid_price_bars.saturating_add(1);
            return;
        }
        if !historical_bar_has_valid_depth(&bar) {
            stats.invalid_depth_bars = stats.invalid_depth_bars.saturating_add(1);
            return;
        }
        let payload = match bar.to_trade_flow_feature_payload(symbol, self.venue.to_u8()) {
            Ok(payload) => payload,
            Err(err) => {
                stats.evaluation_errors = stats.evaluation_errors.saturating_add(1);
                warn!(
                    "Kafka warmup feature encoding failed: venue={} symbol={} bar_start={} err={:#}",
                    self.venue_slug, symbol, bar.start_ms, err
                );
                return;
            }
        };
        let msg = match TradeFlowFeatureMsg::from_bytes(payload.as_ref()) {
            Ok(msg) => msg,
            Err(err) => {
                stats.evaluation_errors = stats.evaluation_errors.saturating_add(1);
                warn!(
                    "Kafka warmup feature decode failed: venue={} symbol={} bar_start={} err={:#}",
                    self.venue_slug, symbol, bar.start_ms, err
                );
                return;
            }
        };
        let record_percentile = bar.start_ms >= history_start_ms;
        let state = self
            .states
            .entry(symbol.to_string())
            .or_insert_with(|| SymbolState::new(self.window_size));
        match state.evaluate(msg, &self.plan, self.min_samples, record_percentile) {
            Ok(Some(observations)) => {
                stats.historical_bars = stats.historical_bars.saturating_add(1);
                stats.percentile_samples = stats.percentile_samples.saturating_add(
                    observations
                        .iter()
                        .filter(|observation| observation.score_quantile.is_some())
                        .count() as u64,
                );
            }
            Ok(None) => {}
            Err(err) => {
                stats.evaluation_errors = stats.evaluation_errors.saturating_add(1);
                warn!(
                    "Kafka warmup factor evaluation failed: venue={} symbol={} bar_start={} err={:#}",
                    self.venue_slug, symbol, bar.start_ms, err
                );
            }
        }
    }

    fn maybe_log_stats(&mut self) {
        if self.last_stats_log.elapsed() < Duration::from_secs(STATS_LOG_INTERVAL_SECS) {
            return;
        }
        info!(
            "IntraFactorModel1mPubApp stats: venue={} raw_msgs={} accepted={} decode_errors={} eval_errors={} state_symbols={} published={} publish_failed={} ready={}",
            self.venue_slug,
            self.stats.raw_messages,
            self.stats.accepted_messages,
            self.stats.decode_errors,
            self.stats.evaluation_errors,
            self.states.len(),
            self.stats.published,
            self.stats.publish_failed,
            self.stats.ready,
        );
        self.stats = IntraFactorModelStats::default();
        self.last_stats_log = Instant::now();
    }
}

fn timestamp_as_micros(timestamp: i64) -> i64 {
    if timestamp >= 10_000_000_000_000 {
        timestamp
    } else {
        timestamp.saturating_mul(1_000)
    }
}

fn align_to_minute(timestamp_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(ONE_MINUTE_MS)
}

fn parse_trade_side(side: &str) -> Option<bool> {
    match side.trim().to_ascii_lowercase().as_str() {
        "b" | "buy" => Some(true),
        "s" | "sell" => Some(false),
        _ => None,
    }
}

fn historical_bar_has_valid_prices(bar: &BaselineBar) -> bool {
    [
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.vwap,
        bar.buy_vwap,
        bar.sell_vwap,
    ]
    .iter()
    .all(|value| value.is_finite() && *value > 0.0)
}

fn historical_bar_has_valid_depth(bar: &BaselineBar) -> bool {
    [bar.depth20.bids[0].0, bar.depth20.asks[0].0]
        .iter()
        .all(|value| value.is_finite() && *value > 0.0)
}

async fn load_enabled_symbols(
    tlen_server: &TlenServerConfig,
    venue: TradingVenue,
    venue_slug: &str,
) -> Result<HashSet<String>> {
    let online_symbols = load_online_symbols_from_tlen_server(
        tlen_server,
        venue,
        venue_slug,
        AMOUNT_THRESHOLD_CONFIG_TYPE,
    )
    .await?;
    let plans =
        load_symbol_factor_plans_from_tlen_server(tlen_server, venue_slug, FACTOR_PLAN_CONFIG_TYPE)
            .await?;

    Ok(select_enabled_symbols(venue, &online_symbols, plans))
}

fn select_enabled_symbols(
    venue: TradingVenue,
    online_symbols: &HashSet<String>,
    plans: HashMap<String, SymbolFactorPlan>,
) -> HashSet<String> {
    plans
        .into_iter()
        .filter_map(|(raw_symbol, plan)| {
            let symbol = normalize_symbol_for_venue(&raw_symbol, venue);
            (online_symbols.contains(&symbol) && plan_has_required_factors(&plan)).then_some(symbol)
        })
        .collect()
}

fn plan_has_required_factors(plan: &SymbolFactorPlan) -> bool {
    let names: Vec<&str> = plan.factor_names().collect();
    names.len() == INTRA_FACTOR_NAMES.len()
        && INTRA_FACTOR_NAMES
            .iter()
            .all(|required| names.contains(required))
}

fn create_trade_flow_subscriber(
    venue_slug: &str,
) -> Result<Subscriber<ipc::Service, [u8; TRADE_FLOW_FEATURE_MAX_BYTES], ()>> {
    let node_name = format!(
        "intra_factor_model_1m_sub_{}",
        sanitize_node_component(venue_slug)
    );
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;
    let service_name = format!("factor_pub/{}/trade_flow_feature_1m", venue_slug);
    let service = node
        .service_builder(&ServiceName::new(&service_name)?)
        .publish_subscribe::<[u8; TRADE_FLOW_FEATURE_MAX_BYTES]>()
        .max_publishers(1)
        .max_subscribers(TRADE_FLOW_MAX_SUBSCRIBERS)
        .subscriber_max_buffer_size(TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE)
        .history_size(TRADE_FLOW_FEATURE_HISTORY_SIZE)
        .open_or_create()?;
    let service_max_buffer = service.static_config().subscriber_max_buffer_size();
    if service_max_buffer < TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE {
        bail!(
            "trade-flow service buffer is too small: service={} actual={} required_min={}",
            service_name,
            service_max_buffer,
            TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE
        );
    }
    let subscriber = service
        .subscriber_builder()
        .buffer_size(TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE)
        .create()?;
    info!(
        "IntraFactorModel1mPubApp subscribed: service={} buffer={} history={}",
        service_name,
        TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE,
        service.static_config().history_size(),
    );
    Ok(subscriber)
}

pub fn output_service_path(venue_slug: &str, factor_name: &str) -> String {
    format!(
        "model_output/intra-{}-1m-{}",
        venue_slug,
        factor_name.to_lowercase()
    )
}

fn sanitize_node_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn format_symbol_sample(symbols: &HashSet<String>) -> String {
    const MAX_SAMPLE: usize = 8;
    let mut values: Vec<&str> = symbols.iter().map(String::as_str).collect();
    values.sort_unstable();
    let mut sample = values
        .iter()
        .take(MAX_SAMPLE)
        .copied()
        .collect::<Vec<_>>()
        .join(",");
    if values.len() > MAX_SAMPLE {
        sample.push_str(",...");
    }
    sample
}

fn now_millis() -> i64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis().min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::{
        align_to_minute, output_service_path, parse_trade_side, plan_has_required_factors,
        select_enabled_symbols, timestamp_as_micros, FactorObservation, SymbolState,
        INTRA_FACTOR_NAMES,
    };
    use crate::factor_pub::fusion_factor_pub::SymbolFactorPlan;
    use mkt_parsers::msg::trade_flow_feature_msg::TradeFlowFeatureMsg;
    use order_common::TradingVenue;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn output_services_are_distinct_per_factor() {
        let services: HashSet<String> = INTRA_FACTOR_NAMES
            .iter()
            .map(|factor| output_service_path("binance-futures", factor))
            .collect();

        assert_eq!(services.len(), INTRA_FACTOR_NAMES.len());
        assert!(services.contains("model_output/intra-binance-futures-1m-baseline_035"));
    }

    #[test]
    fn plan_selection_requires_the_complete_factor_set() {
        let complete = SymbolFactorPlan::from_factor_names(
            "BTCUSDT",
            INTRA_FACTOR_NAMES
                .iter()
                .rev()
                .map(|name| (*name).to_string())
                .collect(),
        )
        .expect("complete plan");
        let incomplete = SymbolFactorPlan::from_factor_names(
            "ETHUSDT",
            INTRA_FACTOR_NAMES[..8]
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        )
        .expect("incomplete plan");
        assert!(plan_has_required_factors(&complete));
        assert!(!plan_has_required_factors(&incomplete));

        let symbols = select_enabled_symbols(
            TradingVenue::BinanceFutures,
            &HashSet::from(["BTCUSDT".to_string(), "ETHUSDT".to_string()]),
            HashMap::from([
                ("BTCUSDT".to_string(), complete),
                ("ETHUSDT".to_string(), incomplete),
            ]),
        );
        assert_eq!(symbols, HashSet::from(["BTCUSDT".to_string()]));
    }

    #[test]
    fn percentile_readiness_requires_valid_minimum_samples() {
        let mut state = SymbolState::new(3);
        let first = state.observe(vec![1.0; INTRA_FACTOR_NAMES.len()], 2);
        let second = state.observe(vec![2.0; INTRA_FACTOR_NAMES.len()], 2);
        let invalid = state.observe(vec![f64::NAN; INTRA_FACTOR_NAMES.len()], 2);

        assert_eq!(
            first[0],
            FactorObservation {
                score: 1.0,
                score_quantile: Some(0.5),
                score_ready: false,
            }
        );
        assert_eq!(second[0].score_quantile, Some(0.75));
        assert!(second[0].score_ready);
        assert!(invalid[0].score.is_nan());
        assert_eq!(invalid[0].score_quantile, None);
        assert!(!invalid[0].score_ready);
    }

    #[test]
    fn period_event_helpers_preserve_epoch_units_and_side_semantics() {
        assert_eq!(
            timestamp_as_micros(1_704_067_200_123),
            1_704_067_200_123_000
        );
        assert_eq!(
            timestamp_as_micros(1_704_067_200_123_456),
            1_704_067_200_123_456
        );
        assert_eq!(align_to_minute(123_456), 120_000);
        assert_eq!(parse_trade_side("BUY"), Some(true));
        assert_eq!(parse_trade_side("s"), Some(false));
        assert_eq!(parse_trade_side("unknown"), None);
    }

    #[test]
    fn ignores_overlapping_live_bar_after_historical_warmup() {
        let plan = SymbolFactorPlan::from_factor_names(
            "BTCUSDT",
            INTRA_FACTOR_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        )
        .expect("factor plan");
        let msg = TradeFlowFeatureMsg::from_indexed_values(
            "BTCUSDT".to_string(),
            TradingVenue::BinanceFutures.to_u8(),
            1_704_067_200_000,
            &vec![1.0; 112],
        )
        .expect("trade-flow message");
        let mut state = SymbolState::new(3);

        assert!(state
            .evaluate(msg.clone(), &plan, 1, true)
            .expect("first evaluation")
            .is_some());
        assert!(state
            .evaluate(msg, &plan, 1, true)
            .expect("duplicate evaluation")
            .is_none());
    }
}
