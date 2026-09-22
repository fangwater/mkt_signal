use anyhow::{bail, Context, Result};
use log::{info, warn};
use mkt_parsers::msg::mkt_msg::{Level, ModelMsg, MODEL_STATUS_OK};
use mkt_parsers::msg::trade_flow_feature_msg::TradeFlowFeatureMsg;
use order_common::TradingVenue;
use period_pbs::kafka::{decode_period_payload, PayloadCompressionMode, RawKafkaConsumer};
use period_pbs::pb::{IncrementOrderBookInfo, PeriodMessage, TradeInfo};
use period_pbs::period::normalize_timestamp_ms;
use rolling_common::exact_rolling_window::ExactRollingWindow;
use runtime_common::symbol_util::normalize_symbol_for_venue;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::common::amount_threshold::AmountThreshold;
use crate::depth_pub::orderbook::OrderBook;
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

use super::cfg::{CtaSpecialFactorPubConfig, KafkaInputConfig, NormalizeConfig};

const FACTOR_NAMES: [&str; 2] = ["TP_VPI_018", "baseline_104"];
const FACTOR_WINDOW: usize = 2_880;
const FACTOR_MIN_SAMPLES: usize = 1_440;
const NQ_LOOKBACK_BARS: usize = 1_440;
const NQ_QUANTILE_WINDOW: usize = 1_440;
const NQ_MIN_PERIODS: usize = 720;
const BAR_MS: i64 = 60_000;
const PENDING_JOIN_RETENTION_MS: i64 = 6 * 60 * 60 * 1_000;
const FACTOR_PLAN_CONFIG_TYPE: &str = "factor_plan_1m";
const AMOUNT_THRESHOLD_CONFIG_TYPE: &str = "amount_thresholds_1m";
const TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE: &str = "amount_thresholds";
const STATS_LOG_INTERVAL_SECS: u64 = 60;

struct FactorOutput {
    name: &'static str,
    factor_index: usize,
    service: String,
    publisher: ModelPublisher,
    seq_no: u64,
}

#[derive(Debug, Clone, Copy)]
struct FactorObservation {
    score: f64,
    quantile: Option<f64>,
    ready: bool,
}

/// Rolling statistics over fixed bar slots. Non-finite values consume a slot
/// but are excluded from mean/std, matching pandas rolling calculations.
struct RollingMeanStd {
    capacity: usize,
    fifo: VecDeque<Option<f64>>,
    sum: f64,
    sum_sq: f64,
    count: usize,
}

impl RollingMeanStd {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            fifo: VecDeque::with_capacity(capacity.max(1)),
            sum: 0.0,
            sum_sq: 0.0,
            count: 0,
        }
    }

    fn push_slot(&mut self, value: f64) {
        let value = value.is_finite().then_some(value);
        if self.fifo.len() == self.capacity {
            if let Some(Some(expired)) = self.fifo.pop_front() {
                self.sum -= expired;
                self.sum_sq -= expired * expired;
                self.count -= 1;
            }
        }
        self.fifo.push_back(value);
        if let Some(value) = value {
            self.sum += value;
            self.sum_sq += value * value;
            self.count += 1;
        }
    }

    fn len(&self) -> usize {
        self.count
    }

    fn mean(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }

    fn std(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }
        let n = self.count as f64;
        let variance = (self.sum_sq - self.sum * self.sum / n) / (n - 1.0);
        Some(variance.max(0.0).sqrt())
    }
}

/// Matches offline `normalize_factors`: clip raw at the trailing
/// mean +/- clip_zscore*std, standardize the clipped series over the same
/// window, map zero variance to 0, and causally carry the last z-score.
struct NormalizeState {
    raw_window: RollingMeanStd,
    capped_window: RollingMeanStd,
    min_periods: usize,
    clip_zscore: f64,
    last_z: Option<f64>,
}

impl NormalizeState {
    fn new(config: &NormalizeConfig) -> Self {
        Self {
            raw_window: RollingMeanStd::new(config.window_bars),
            capped_window: RollingMeanStd::new(config.window_bars),
            min_periods: config.min_periods,
            clip_zscore: config.clip_zscore,
            last_z: None,
        }
    }

    fn observe(&mut self, raw: f64) -> Option<f64> {
        if !raw.is_finite() {
            self.raw_window.push_slot(f64::NAN);
            self.capped_window.push_slot(f64::NAN);
            return self.last_z;
        }
        self.raw_window.push_slot(raw);
        let capped = if self.raw_window.len() >= self.min_periods {
            match (self.raw_window.mean(), self.raw_window.std()) {
                (Some(mean), Some(std)) => {
                    raw.clamp(mean - self.clip_zscore * std, mean + self.clip_zscore * std)
                }
                _ => raw,
            }
        } else {
            raw
        };
        self.capped_window.push_slot(capped);
        let z = if self.capped_window.len() >= self.min_periods {
            match (self.capped_window.mean(), self.capped_window.std()) {
                (Some(mean), Some(std)) if std > 0.0 => (capped - mean) / std,
                (Some(_), Some(_)) => 0.0,
                _ => return self.last_z,
            }
        } else {
            return self.last_z;
        };
        if !z.is_finite() {
            return self.last_z;
        }
        self.last_z = Some(z);
        Some(z)
    }
}

struct SymbolFactorState {
    evaluator: BaselineReplayState,
    windows: Vec<ExactRollingWindow>,
    normalize: Vec<NormalizeState>,
    last_ts: Option<i64>,
}

impl SymbolFactorState {
    fn new(normalize: &NormalizeConfig) -> Self {
        Self {
            evaluator: BaselineReplayState::default(),
            windows: FACTOR_NAMES
                .iter()
                .map(|_| ExactRollingWindow::new(FACTOR_WINDOW))
                .collect(),
            normalize: FACTOR_NAMES
                .iter()
                .map(|_| NormalizeState::new(normalize))
                .collect(),
            last_ts: None,
        }
    }

    fn evaluate(
        &mut self,
        msg: TradeFlowFeatureMsg,
        plan: &SymbolFactorPlan,
        record: bool,
    ) -> Result<Option<Vec<FactorObservation>>> {
        if self.last_ts.is_some_and(|last| msg.ts <= last) {
            return Ok(None);
        }
        self.evaluator.push(msg.clone())?;
        self.last_ts = Some(msg.ts);
        let raw_values = self.evaluator.factor_values(plan);
        if !record {
            return Ok(None);
        }
        Ok(Some(
            raw_values
                .into_iter()
                .zip(self.windows.iter_mut())
                .zip(self.normalize.iter_mut())
                .map(|((raw, window), normalize)| {
                    let score = normalize.observe(raw).unwrap_or(f64::NAN);
                    let observed = window.observe_slot(score);
                    let quantile = observed.then(|| window.percentile_rank_last()).flatten();
                    FactorObservation {
                        score,
                        quantile,
                        ready: observed && window.len() >= FACTOR_MIN_SAMPLES && quantile.is_some(),
                    }
                })
                .collect(),
        ))
    }
}

#[derive(Debug, Clone, Copy)]
struct NqObservation {
    long_value: Option<f64>,
    long_quantile: Option<f64>,
    short_value: Option<f64>,
    short_quantile: Option<f64>,
    ready: bool,
}

struct NqState {
    orderbook: OrderBook,
    next_update_id: i64,
    current_start_ms: Option<i64>,
    current_close: f64,
    closes: VecDeque<f64>,
    long_window: ExactRollingWindow,
    short_window: ExactRollingWindow,
}

impl Default for NqState {
    fn default() -> Self {
        Self {
            orderbook: OrderBook::new(),
            next_update_id: 1,
            current_start_ms: None,
            current_close: f64::NAN,
            closes: VecDeque::with_capacity(NQ_LOOKBACK_BARS),
            long_window: ExactRollingWindow::new(NQ_QUANTILE_WINDOW),
            short_window: ExactRollingWindow::new(NQ_QUANTILE_WINDOW),
        }
    }
}

impl NqState {
    fn on_book(&mut self, book: &IncrementOrderBookInfo) -> Vec<(i64, NqObservation)> {
        let timestamp_ms = normalize_timestamp_ms(book.timestamp);
        let target_start = timestamp_ms.div_euclid(BAR_MS).saturating_mul(BAR_MS);
        let mut closed = Vec::new();
        match self.current_start_ms {
            None => self.current_start_ms = Some(target_start),
            Some(current) if target_start < current => return closed,
            Some(mut current) => {
                while current < target_start {
                    closed.push((current.saturating_add(BAR_MS), self.close_bar()));
                    current = current.saturating_add(BAR_MS);
                    self.current_start_ms = Some(current);
                    self.current_close = f64::NAN;
                }
            }
        }
        let bids: Vec<(f64, f64)> = book.bids.iter().map(|v| (v.price, v.amount)).collect();
        let asks: Vec<(f64, f64)> = book.asks.iter().map(|v| (v.price, v.amount)).collect();
        let update_id = self.next_update_id;
        self.next_update_id = self.next_update_id.saturating_add(1);
        self.orderbook
            .apply_update(&bids, &asks, update_id, timestamp_as_micros(book.timestamp));
        if !self.orderbook.is_valid() {
            self.orderbook.prune_crossed_by_best_update_id();
        }
        if let (Some(bid), Some(ask)) = (
            self.orderbook.best_bid_price(),
            self.orderbook.best_ask_price(),
        ) {
            if bid.is_finite() && ask.is_finite() && bid > 0.0 && ask >= bid {
                self.current_close = 0.5 * (bid + ask);
            }
        }
        closed
    }

    fn close_bar(&mut self) -> NqObservation {
        let close = self.current_close;
        self.closes.push_back(close);
        while self.closes.len() > NQ_LOOKBACK_BARS {
            self.closes.pop_front();
        }
        let finite: Vec<f64> = self
            .closes
            .iter()
            .copied()
            .filter(|value| value.is_finite() && *value > 0.0)
            .collect();
        let long_value = (close.is_finite() && !finite.is_empty()).then(|| {
            let minimum = finite.iter().copied().fold(f64::INFINITY, f64::min);
            (close - minimum) / minimum
        });
        let short_value = (close.is_finite() && !finite.is_empty()).then(|| {
            let maximum = finite.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            (close - maximum) / maximum
        });
        self.long_window
            .observe_slot(long_value.unwrap_or(f64::NAN));
        self.short_window
            .observe_slot(short_value.unwrap_or(f64::NAN));
        let long_quantile = self.long_window.percentile_rank_last();
        let short_quantile = self.short_window.percentile_rank_last();
        NqObservation {
            long_value,
            long_quantile,
            short_value,
            short_quantile,
            ready: long_value.is_some()
                && short_value.is_some()
                && self.long_window.len() >= NQ_MIN_PERIODS
                && self.short_window.len() >= NQ_MIN_PERIODS
                && long_quantile.is_some()
                && short_quantile.is_some(),
        }
    }
}

#[derive(Default)]
struct Stats {
    records: u64,
    decode_errors: u64,
    evaluation_errors: u64,
    published: u64,
    publish_failed: u64,
}

enum PeriodEvent<'a> {
    Book(&'a IncrementOrderBookInfo),
    Trade(&'a TradeInfo),
}

impl PeriodEvent<'_> {
    fn timestamp_ms(&self) -> i64 {
        normalize_timestamp_ms(match self {
            Self::Book(value) => value.timestamp,
            Self::Trade(value) => value.timestamp,
        })
    }
}

pub struct CtaSpecialFactorModel1mPubApp {
    venue: TradingVenue,
    venue_slug: String,
    tlen_server: TlenServerConfig,
    plan: SymbolFactorPlan,
    consumer: RawKafkaConsumer,
    poll_timeout_ms: u64,
    compression: PayloadCompressionMode,
    topic: String,
    history_start_ms: i64,
    amount_thresholds: HashMap<String, AmountThreshold>,
    allowed_symbols: HashSet<String>,
    aggregators: HashMap<String, LocalBaselineAggregator>,
    factor_states: HashMap<String, SymbolFactorState>,
    normalize: NormalizeConfig,
    nq_states: HashMap<String, NqState>,
    pending_factors: HashMap<(String, i64), Vec<FactorObservation>>,
    pending_nq: HashMap<(String, i64), NqObservation>,
    outputs: Vec<FactorOutput>,
    publish_enabled: bool,
    symbol_reload_interval: Duration,
    last_symbol_reload: Instant,
    last_pending_prune: Instant,
    last_stats_log: Instant,
    stats: Stats,
}

impl CtaSpecialFactorModel1mPubApp {
    pub async fn new(path: &str, venue: TradingVenue, wait_for_publishers: bool) -> Result<Self> {
        anyhow::ensure!(
            venue == TradingVenue::BinanceFutures,
            "CTA special factor publisher only supports binance-futures"
        );
        let config = CtaSpecialFactorPubConfig::load(path)?;
        let venue_slug = venue.data_pub_slug().to_string();
        let plan = SymbolFactorPlan::from_factor_names(
            "cta_special_factor_model_1m",
            FACTOR_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        )?;
        BaselineReplayState::validate_factor_plan(&plan)
            .context("validate CTA special factor plan")?;
        let allowed_symbols = load_enabled_symbols(&config.tlen_server, venue, &venue_slug).await?;
        if allowed_symbols.is_empty() {
            bail!("no symbols contain both CTA special factors");
        }
        let amount_thresholds = load_amount_thresholds_from_tlen_server(
            &config.tlen_server,
            venue,
            &venue_slug,
            TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE,
        )
        .await?;
        let startup_ms = now_millis();
        let mut kafka_config = config.kafka.consumer.clone();
        kafka_config.group_id = format!(
            "{}-{}-{}",
            kafka_config.group_id,
            std::process::id(),
            startup_ms
        );
        kafka_config.client_id = format!(
            "{}-{}-{}",
            kafka_config.client_id,
            std::process::id(),
            startup_ms
        );
        kafka_config.offset_reset = "earliest".to_string();
        kafka_config.enable_auto_commit = false;
        let consumer = RawKafkaConsumer::new(&kafka_config)
            .context("create CTA special factor Kafka consumer")?;
        let mut app = Self {
            venue,
            venue_slug,
            tlen_server: config.tlen_server.clone(),
            plan,
            consumer,
            poll_timeout_ms: kafka_config.poll_timeout_ms.max(1),
            compression: kafka_config.payload_compression,
            topic: config.kafka.topic.clone(),
            history_start_ms: startup_ms
                .saturating_sub(config.kafka.lookback_secs.saturating_mul(1_000) as i64),
            amount_thresholds,
            allowed_symbols,
            aggregators: HashMap::new(),
            factor_states: HashMap::new(),
            normalize: config.normalize.clone(),
            nq_states: HashMap::new(),
            pending_factors: HashMap::new(),
            pending_nq: HashMap::new(),
            outputs: Vec::new(),
            publish_enabled: false,
            symbol_reload_interval: Duration::from_secs(config.tlen_server.symbol_reload_secs),
            last_symbol_reload: Instant::now(),
            last_pending_prune: Instant::now(),
            last_stats_log: Instant::now(),
            stats: Stats::default(),
        };
        app.catch_up(&config.kafka)?;
        loop {
            match create_shared_outputs(&app.venue_slug) {
                Ok(outputs) => {
                    app.outputs = outputs;
                    break;
                }
                Err(err) if wait_for_publishers => {
                    warn!(
                        "CTA special factor catch-up complete; waiting for shared publisher ownership: {err:#}"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(err) => return Err(err),
            }
        }
        app.publish_enabled = true;
        info!(
            "CTA special shared factor publisher ready venue={} symbols={} outputs={:?} zscore={}/{}/clip{} percentile=2880/1440 nq=1440/1440/720",
            app.venue_slug,
            app.allowed_symbols.len(),
            app.outputs.iter().map(|output| output.service.as_str()).collect::<Vec<_>>(),
            app.normalize.window_bars,
            app.normalize.min_periods,
            app.normalize.clip_zscore,
        );
        Ok(app)
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            self.maybe_reload_symbols().await;
            if let Some(record) = self.consumer.poll(self.poll_timeout_ms) {
                let record = record.context("read CTA special factor Kafka record")?;
                self.consume_record(&record.topic, &record.payload);
            }
            self.maybe_log_stats();
        }
    }

    fn consume_record(&mut self, topic: &str, payload: &[u8]) {
        self.stats.records = self.stats.records.saturating_add(1);
        if topic != self.topic {
            return;
        }
        let period = match decode_period_payload(payload, self.compression) {
            Ok((_, _, period)) => period,
            Err(err) => {
                self.stats.decode_errors = self.stats.decode_errors.saturating_add(1);
                warn!("CTA special PeriodMessage decode failed: {err:#}");
                return;
            }
        };
        self.consume_period(&period);
        self.maybe_prune_pending_joins();
    }

    fn consume_period(&mut self, period: &PeriodMessage) {
        for info in &period.symbol_infos {
            let symbol = normalize_symbol_for_venue(&info.symbol, self.venue);
            if !self.allowed_symbols.contains(&symbol) {
                continue;
            }
            self.consume_nq_books(&symbol, &info.incs);
            let Some(threshold) = self.amount_thresholds.get(&symbol).copied() else {
                continue;
            };
            let mut events = Vec::with_capacity(info.incs.len() + info.trades.len());
            events.extend(info.incs.iter().map(PeriodEvent::Book));
            events.extend(info.trades.iter().map(PeriodEvent::Trade));
            events.sort_by_key(PeriodEvent::timestamp_ms);
            let bars = {
                let aggregator = self.aggregators.entry(symbol.clone()).or_default();
                for event in events {
                    match event {
                        PeriodEvent::Book(book) => {
                            let bids: Vec<Level> = book
                                .bids
                                .iter()
                                .map(|value| Level::from_values(value.price, value.amount))
                                .collect();
                            let asks: Vec<Level> = book
                                .asks
                                .iter()
                                .map(|value| Level::from_values(value.price, value.amount))
                                .collect();
                            aggregator.on_retained_incremental_book(
                                timestamp_as_micros(book.timestamp),
                                &bids,
                                &asks,
                            );
                        }
                        PeriodEvent::Trade(trade) => {
                            if let Some(is_buy) = parse_trade_side(&trade.side) {
                                aggregator.on_trade_with_threshold(
                                    timestamp_as_micros(trade.timestamp),
                                    is_buy,
                                    trade.price,
                                    trade.amount,
                                    threshold,
                                );
                            }
                        }
                    }
                }
                aggregator.drain_sixty_second_bars()
            };
            for mut bar in bars {
                if !valid_bar(&bar) {
                    continue;
                }
                bar.start_ms = bar.start_ms.saturating_add(BAR_MS);
                self.consume_bar(&symbol, bar);
            }
        }
    }

    fn consume_nq_books(&mut self, symbol: &str, books: &[IncrementOrderBookInfo]) {
        let mut books: Vec<&IncrementOrderBookInfo> = books.iter().collect();
        books.sort_by_key(|book| normalize_timestamp_ms(book.timestamp));
        let mut completed = Vec::new();
        let state = self.nq_states.entry(symbol.to_string()).or_default();
        for book in books {
            completed.extend(state.on_book(book));
        }
        for (ts, observation) in completed {
            if ts >= self.history_start_ms {
                self.pending_nq
                    .insert((symbol.to_string(), ts), observation);
                self.publish_if_complete(symbol, ts);
            }
        }
    }

    fn consume_bar(&mut self, symbol: &str, bar: BaselineBar) {
        let payload = match bar.to_trade_flow_feature_payload(symbol, self.venue.to_u8()) {
            Ok(value) => value,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!("CTA special bar encode failed symbol={symbol}: {err:#}");
                return;
            }
        };
        let msg = match TradeFlowFeatureMsg::from_bytes(payload.as_ref()) {
            Ok(value) => value,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!("CTA special feature decode failed symbol={symbol}: {err:#}");
                return;
            }
        };
        let ts = msg.ts;
        let observations = match self
            .factor_states
            .entry(symbol.to_string())
            .or_insert_with(|| SymbolFactorState::new(&self.normalize))
            .evaluate(msg, &self.plan, ts >= self.history_start_ms)
        {
            Ok(Some(value)) => value,
            Ok(None) => return,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!("CTA special factor evaluation failed symbol={symbol}: {err:#}");
                return;
            }
        };
        self.pending_factors
            .insert((symbol.to_string(), ts), observations);
        self.publish_if_complete(symbol, ts);
    }

    fn publish_if_complete(&mut self, symbol: &str, ts: i64) {
        let key = (symbol.to_string(), ts);
        if !(self.pending_factors.contains_key(&key) && self.pending_nq.contains_key(&key)) {
            return;
        }
        let factors = self.pending_factors.remove(&key).expect("checked above");
        let nq = self.pending_nq.remove(&key).expect("checked above");
        if !self.publish_enabled {
            return;
        }
        for output in &mut self.outputs {
            let Some(factor) = factors.get(output.factor_index) else {
                self.stats.publish_failed = self.stats.publish_failed.saturating_add(1);
                warn!(
                    "CTA special factor output index missing factor={} index={} observations={}",
                    output.name,
                    output.factor_index,
                    factors.len()
                );
                continue;
            };
            output.seq_no = output.seq_no.saturating_add(1);
            let msg = ModelMsg::create(
                symbol.to_string(),
                ts,
                now_millis(),
                output.seq_no,
                factor.score,
                factor.quantile,
                factor.ready,
                MODEL_STATUS_OK,
                Vec::new(),
                Vec::new(),
            )
            .with_decision_context(
                None,
                None,
                nq.long_value,
                None,
                nq.short_value,
                None,
                nq.ready,
            )
            .with_filter_quantiles(nq.long_quantile, nq.short_quantile);
            let published = msg
                .to_bytes()
                .map(|payload| output.publisher.publish(&payload))
                .unwrap_or(false);
            if published {
                self.stats.published = self.stats.published.saturating_add(1);
            } else {
                self.stats.publish_failed = self.stats.publish_failed.saturating_add(1);
                warn!(
                    "CTA special model publish failed symbol={} factor={} service={}",
                    symbol, output.name, output.service
                );
            }
        }
    }

    fn maybe_prune_pending_joins(&mut self) {
        if self.last_pending_prune.elapsed() < Duration::from_secs(STATS_LOG_INTERVAL_SECS) {
            return;
        }
        self.last_pending_prune = Instant::now();
        let cutoff = now_millis().saturating_sub(PENDING_JOIN_RETENTION_MS);
        let factors_before = self.pending_factors.len();
        let nq_before = self.pending_nq.len();
        self.pending_factors.retain(|(_, ts), _| *ts >= cutoff);
        self.pending_nq.retain(|(_, ts), _| *ts >= cutoff);
        let dropped_factors = factors_before.saturating_sub(self.pending_factors.len());
        let dropped_nq = nq_before.saturating_sub(self.pending_nq.len());
        if dropped_factors > 0 || dropped_nq > 0 {
            warn!(
                "CTA special pruned stale unmatched joins factor={} nq={} cutoff_ms={}",
                dropped_factors, dropped_nq, cutoff
            );
        }
    }

    fn catch_up(&mut self, config: &KafkaInputConfig) -> Result<()> {
        let watermarks = self.consumer.query_topic_watermarks(
            &config.consumer.topics,
            config.consumer.metadata_timeout_ms,
            config.consumer.watermark_timeout_ms,
        )?;
        let targets: HashMap<(String, i32), i64> = watermarks
            .into_iter()
            .filter(|watermark| watermark.high > watermark.low)
            .map(|watermark| {
                (
                    (watermark.topic, watermark.partition),
                    watermark.high.saturating_sub(1),
                )
            })
            .collect();
        if targets.is_empty() {
            bail!("CTA special Kafka topic has no retained records");
        }
        let deadline = Instant::now() + Duration::from_secs(config.catchup_timeout_secs);
        let mut reached = HashSet::new();
        while reached.len() < targets.len() {
            if Instant::now() >= deadline {
                bail!(
                    "CTA special Kafka catch-up timed out reached={}/{}",
                    reached.len(),
                    targets.len()
                );
            }
            let Some(record) = self.consumer.poll(self.poll_timeout_ms) else {
                continue;
            };
            let record = record.context("read CTA special catch-up record")?;
            let key = (record.topic.clone(), record.partition);
            if targets
                .get(&key)
                .is_some_and(|target| record.offset >= *target)
            {
                reached.insert(key);
            }
            self.consume_record(&record.topic, &record.payload);
        }
        Ok(())
    }

    async fn maybe_reload_symbols(&mut self) {
        if self.last_symbol_reload.elapsed() < self.symbol_reload_interval {
            return;
        }
        self.last_symbol_reload = Instant::now();
        match load_enabled_symbols(&self.tlen_server, self.venue, &self.venue_slug).await {
            Ok(symbols) if !symbols.is_empty() => {
                self.allowed_symbols = symbols;
                self.factor_states
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
                self.nq_states
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
                self.aggregators
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
            }
            Ok(_) => warn!("CTA special symbol reload returned empty set; retaining previous"),
            Err(err) => warn!("CTA special symbol reload failed: {err:#}"),
        }
    }

    fn maybe_log_stats(&mut self) {
        if self.last_stats_log.elapsed() < Duration::from_secs(STATS_LOG_INTERVAL_SECS) {
            return;
        }
        info!(
            "CTA special factor stats records={} decode_errors={} eval_errors={} published={} publish_failed={} factor_symbols={} nq_symbols={}",
            self.stats.records,
            self.stats.decode_errors,
            self.stats.evaluation_errors,
            self.stats.published,
            self.stats.publish_failed,
            self.factor_states.len(),
            self.nq_states.len()
        );
        self.stats = Stats::default();
        self.last_stats_log = Instant::now();
    }
}

fn create_shared_outputs(venue_slug: &str) -> Result<Vec<FactorOutput>> {
    let mut outputs = Vec::with_capacity(FACTOR_NAMES.len());
    for (factor_index, name) in FACTOR_NAMES.iter().copied().enumerate() {
        let service = output_service_path(venue_slug, name);
        let node_name = format!("cta_special_factor_{}_{}", venue_slug, name)
            .replace('-', "_")
            .to_ascii_lowercase();
        outputs.push(FactorOutput {
            name,
            factor_index,
            publisher: ModelPublisher::new(&node_name, &service)?,
            service,
            seq_no: 0,
        });
    }
    Ok(outputs)
}

async fn load_enabled_symbols(
    tlen: &TlenServerConfig,
    venue: TradingVenue,
    venue_slug: &str,
) -> Result<HashSet<String>> {
    let online =
        load_online_symbols_from_tlen_server(tlen, venue, venue_slug, AMOUNT_THRESHOLD_CONFIG_TYPE)
            .await?;
    let plans =
        load_symbol_factor_plans_from_tlen_server(tlen, venue_slug, FACTOR_PLAN_CONFIG_TYPE)
            .await?;
    Ok(plans
        .into_iter()
        .filter_map(|(raw_symbol, plan)| {
            let symbol = normalize_symbol_for_venue(&raw_symbol, venue);
            let names: HashSet<&str> = plan.factor_names().collect();
            (online.contains(&symbol)
                && FACTOR_NAMES.iter().all(|required| names.contains(required)))
            .then_some(symbol)
        })
        .collect())
}

pub fn output_service_path(venue_slug: &str, factor: &str) -> String {
    format!(
        "model_output/one-{}-1m-{}",
        venue_slug,
        factor.to_ascii_lowercase()
    )
}

fn parse_trade_side(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "b" | "buy" => Some(true),
        "s" | "sell" => Some(false),
        _ => None,
    }
}

fn valid_bar(bar: &BaselineBar) -> bool {
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

fn timestamp_as_micros(value: i64) -> i64 {
    if value >= 10_000_000_000_000 {
        value
    } else {
        value.saturating_mul(1_000)
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalize_config(window_bars: usize, min_periods: usize) -> NormalizeConfig {
        NormalizeConfig {
            window_bars,
            min_periods,
            clip_zscore: 3.0,
        }
    }

    #[test]
    fn output_contract_is_isolated_from_intra() {
        assert_eq!(
            output_service_path("binance-futures", "TP_VPI_018"),
            "model_output/one-binance-futures-1m-tp_vpi_018"
        );
    }

    #[test]
    fn publisher_exposes_both_shared_factor_services() {
        assert_eq!(
            FACTOR_NAMES
                .iter()
                .map(|factor| output_service_path("binance-futures", factor))
                .collect::<Vec<_>>(),
            vec![
                "model_output/one-binance-futures-1m-tp_vpi_018",
                "model_output/one-binance-futures-1m-baseline_104",
            ]
        );
    }

    #[test]
    fn factor_normalization_precedes_percentile_window() {
        let mut normalize = NormalizeState::new(&normalize_config(3, 2));
        assert_eq!(normalize.observe(1.0), None);
        let second = normalize.observe(2.0).expect("second z-score");
        let third = normalize.observe(3.0).expect("third z-score");
        assert!((second - std::f64::consts::FRAC_1_SQRT_2).abs() < 1e-12);
        assert!((third - 1.0).abs() < 1e-12);

        // Missing raw slots causally carry the previous z-score.
        assert_eq!(normalize.observe(f64::NAN), Some(third));
    }

    #[test]
    fn nq_uses_1440_bar_min_max_and_median_gate() {
        let mut state = NqState::default();
        for _ in 0..719 {
            state.current_close = 100.0;
            assert!(!state.close_bar().ready);
        }
        state.current_close = 101.0;
        let observation = state.close_bar();
        assert!(observation.ready);
        assert!(observation.long_quantile.is_some());
        assert!(observation.short_quantile.is_some());
    }
}
