use anyhow::{bail, Context, Result};
use log::{info, warn};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mkt_parsers::msg::mkt_msg::Level;
use mkt_parsers::msg::mkt_msg::{ModelMsg, MODEL_STATUS_OK};
use mkt_parsers::msg::trade_flow_feature_msg::TradeFlowFeatureMsg;
use order_common::TradingVenue;
use period_pbs::kafka::{decode_period_payload, PayloadCompressionMode, RawKafkaConsumer};
use period_pbs::pb::{IncrementOrderBookInfo, PeriodMessage, TradeInfo};
use period_pbs::period::normalize_timestamp_ms;
use rolling_common::exact_rolling_window::ExactRollingWindow;
use runtime_common::symbol_util::normalize_symbol_for_venue;

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

use super::cfg::{IntraFactorModelPubConfig, KafkaInputConfig, NormalizeConfig};

const STATS_LOG_INTERVAL_SECS: u64 = 60;
const SYMBOL_RELOAD_WARN_INTERVAL_SECS: u64 = 60;
const FACTOR_PLAN_CONFIG_TYPE: &str = "factor_plan_1m";
const AMOUNT_THRESHOLD_CONFIG_TYPE: &str = "amount_thresholds_1m";
const TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE: &str = "amount_thresholds";
const BAR_MS: i64 = 60_000;
const FACTOR_LONG_QUANTILE: f64 = 0.9;
const FACTOR_SHORT_QUANTILE: f64 = 0.1;
const NQ_LOOKBACK_BARS: usize = 60;
const NQ_QUANTILE_WINDOW: usize = 1_440;
const NQ_MIN_PERIODS: usize = 720;
const NQ_QUANTILE: f64 = 0.95;
const PENDING_JOIN_RETENTION_MS: i64 = 6 * 60 * 60 * 1_000;

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

/// Rolling mean/std over the trailing `capacity` bar slots, matching pandas
/// `rolling(window).mean()/std()`: non-finite values occupy a slot but are
/// excluded from the statistics.
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

    /// Sample standard deviation (ddof=1), matching pandas `.std()`.
    fn std(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }
        let n = self.count as f64;
        let variance = (self.sum_sq - self.sum * self.sum / n) / (n - 1.0);
        Some(variance.max(0.0).sqrt())
    }
}

/// Per-factor rolling z-score state mirroring the offline `normalize_factors`
/// contract (`build_data_pipeline`): clip the raw value at the trailing
/// mean ± clip_zscore*std, then standardize against the trailing mean/std of
/// the clipped series. Zero-variance maps to 0 and unresolved values carry
/// the previous z-score forward (causal ffill).
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

struct SymbolState {
    evaluator: BaselineReplayState,
    windows: Vec<ExactRollingWindow>,
    normalize: Vec<NormalizeState>,
    last_trade_flow_ts: Option<i64>,
}

impl SymbolState {
    fn new(window_size: usize, normalize: &NormalizeConfig) -> Self {
        Self {
            evaluator: BaselineReplayState::default(),
            windows: (0..INTRA_FACTOR_NAMES.len())
                .map(|_| ExactRollingWindow::new(window_size))
                .collect(),
            normalize: (0..INTRA_FACTOR_NAMES.len())
                .map(|_| NormalizeState::new(normalize))
                .collect(),
            last_trade_flow_ts: None,
        }
    }

    fn observe(&mut self, raw_values: Vec<f64>, min_samples: usize) -> Vec<FactorObservation> {
        debug_assert_eq!(raw_values.len(), self.windows.len());
        raw_values
            .into_iter()
            .zip(self.windows.iter_mut())
            .zip(self.normalize.iter_mut())
            .map(|((raw, window), normalize)| {
                let score = normalize.observe(raw).unwrap_or(f64::NAN);
                let observed = window.observe_slot(score);
                let score_quantile = observed.then(|| window.percentile_rank_last()).flatten();
                let score_long_threshold = window.quantile_linear(FACTOR_LONG_QUANTILE);
                let score_short_threshold = window.quantile_linear(FACTOR_SHORT_QUANTILE);
                let score_ready = observed
                    && window.len() >= min_samples
                    && score_long_threshold.is_some()
                    && score_short_threshold.is_some();
                FactorObservation {
                    score,
                    score_quantile,
                    score_long_threshold,
                    score_short_threshold,
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
    score_long_threshold: Option<f64>,
    score_short_threshold: Option<f64>,
    score_ready: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct NqObservation {
    long_value: Option<f64>,
    long_threshold: Option<f64>,
    short_value: Option<f64>,
    short_threshold: Option<f64>,
    ready: bool,
}

struct SpotNqState {
    orderbook: OrderBook,
    next_update_id: i64,
    current_start_ms: Option<i64>,
    current_close: f64,
    closes: VecDeque<f64>,
    long_window: ExactRollingWindow,
    short_window: ExactRollingWindow,
}

impl Default for SpotNqState {
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

impl SpotNqState {
    fn on_book(&mut self, book: &IncrementOrderBookInfo) -> Vec<(i64, NqObservation)> {
        let timestamp_ms = normalize_timestamp_ms(book.timestamp);
        let target_start = timestamp_ms.div_euclid(BAR_MS).saturating_mul(BAR_MS);
        let mut closed = Vec::new();
        match self.current_start_ms {
            None => self.current_start_ms = Some(target_start),
            Some(current) if target_start < current => return closed,
            Some(mut current) => {
                while current < target_start {
                    closed.push((current.saturating_add(BAR_MS), self.close_current_bar()));
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

    fn close_current_bar(&mut self) -> NqObservation {
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
        let long_value = close.is_finite().then(|| {
            let minimum = finite.iter().copied().fold(f64::INFINITY, f64::min);
            (close - minimum) / minimum
        });
        let short_value = close.is_finite().then(|| {
            let maximum = finite.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            (close - maximum) / maximum
        });
        let long_raw = long_value.unwrap_or(f64::NAN);
        let short_raw = short_value.unwrap_or(f64::NAN);
        self.long_window.observe_slot(long_raw);
        self.short_window.observe_slot(short_raw);
        let long_threshold = self.long_window.quantile_linear(NQ_QUANTILE);
        let short_threshold = self.short_window.quantile_linear(NQ_QUANTILE);
        let ready = long_value.is_some()
            && short_value.is_some()
            && self.long_window.len() >= NQ_MIN_PERIODS
            && self.short_window.len() >= NQ_MIN_PERIODS
            && long_threshold.is_some()
            && short_threshold.is_some();
        NqObservation {
            long_value,
            long_threshold,
            short_value,
            short_threshold,
            ready,
        }
    }
}

struct PendingFactorBar {
    observations: Vec<FactorObservation>,
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

/// Publishes each notebook factor as a separate z-scored virtual model.
///
/// `ModelMsg.score` is the rolling z-score of the raw factor value, matching
/// the offline `normalize_factors` contract used to build `factor_data_1m`.
/// `score_quantile` is the current percentile rank of that z-score within its
/// own per-symbol rolling window, and `score_long/short_threshold` are the
/// window's q0.9/q0.1 quantiles.
pub struct IntraFactorModel1mPubApp {
    venue: TradingVenue,
    venue_slug: String,
    tlen_server: TlenServerConfig,
    window_size: usize,
    min_samples: usize,
    normalize: NormalizeConfig,
    plan: SymbolFactorPlan,
    kafka_consumer: RawKafkaConsumer,
    kafka_poll_timeout_ms: u64,
    kafka_payload_compression: PayloadCompressionMode,
    factor_topic: String,
    nq_topic: String,
    amount_thresholds: HashMap<String, AmountThreshold>,
    aggregators: HashMap<String, LocalBaselineAggregator>,
    nq_states: HashMap<String, SpotNqState>,
    pending_factors: HashMap<(String, i64), PendingFactorBar>,
    pending_nq: HashMap<(String, i64), NqObservation>,
    history_start_ms: i64,
    outputs: Vec<FactorOutput>,
    allowed_symbols: HashSet<String>,
    states: HashMap<String, SymbolState>,
    last_symbol_reload: Instant,
    symbol_reload_interval: Duration,
    last_symbol_reload_warn: Instant,
    last_pending_prune: Instant,
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
}

fn sort_period_events(events: &mut [PeriodEvent<'_>]) {
    // The producer preserves causal order inside each `incs` vector. Multiple
    // book updates commonly share an exchange timestamp, so an unstable sort
    // can turn a later delete into an earlier update and corrupt the replayed
    // book. Events are assembled as books followed by trades, making a stable
    // timestamp sort also retain the intended book-before-trade tie-breaker.
    events.sort_by_key(PeriodEvent::timestamp_ms);
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

        let amount_thresholds = load_amount_thresholds_from_tlen_server(
            &config.tlen_server,
            venue,
            &venue_slug,
            TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE,
        )
        .await
        .with_context(|| {
            format!(
                "load Kafka trade-flow amount thresholds failed: venue={}",
                venue_slug
            )
        })?;
        let startup_ms = now_millis();
        let history_start_ms =
            startup_ms.saturating_sub(config.kafka.lookback_secs.saturating_mul(1_000) as i64);
        let mut kafka_config = config.kafka.consumer.clone();
        let run_id = format!("{}-{}", std::process::id(), startup_ms);
        kafka_config.group_id = format!("{}-{}", kafka_config.group_id, run_id);
        kafka_config.client_id = format!("{}-{}", kafka_config.client_id, run_id);
        // Each restart replays retained history and never commits a shared offset.
        kafka_config.offset_reset = "earliest".to_string();
        kafka_config.enable_auto_commit = false;
        let kafka_consumer =
            RawKafkaConsumer::new(&kafka_config).context("create intra 1m Kafka consumer")?;

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
            normalize: config.normalize.clone(),
            plan,
            kafka_consumer,
            kafka_poll_timeout_ms: kafka_config.poll_timeout_ms.max(1),
            kafka_payload_compression: kafka_config.payload_compression,
            factor_topic: config.kafka.factor_topic.clone(),
            nq_topic: config.kafka.nq_topic.clone(),
            amount_thresholds,
            aggregators: HashMap::new(),
            nq_states: HashMap::new(),
            pending_factors: HashMap::new(),
            pending_nq: HashMap::new(),
            history_start_ms,
            outputs,
            allowed_symbols,
            states: HashMap::new(),
            last_symbol_reload: Instant::now(),
            symbol_reload_interval: Duration::from_secs(config.tlen_server.symbol_reload_secs),
            last_symbol_reload_warn: Instant::now()
                - Duration::from_secs(SYMBOL_RELOAD_WARN_INTERVAL_SECS),
            last_pending_prune: Instant::now(),
            last_stats_log: Instant::now(),
            stats: IntraFactorModelStats::default(),
        };

        app.catch_up_kafka(&config.kafka)?;

        info!(
            "IntraFactorModel1mPubApp started: venue={} input=Kafka PeriodMessage symbols={} sample={} percentile_window={} min_samples={} output_services={:?}",
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
            self.poll_kafka()?;
            self.maybe_log_stats();
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
                self.aggregators
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
                self.nq_states
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
                self.pending_factors
                    .retain(|(symbol, _), _| self.allowed_symbols.contains(symbol));
                self.pending_nq
                    .retain(|(symbol, _), _| self.allowed_symbols.contains(symbol));
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

    fn poll_kafka(&mut self) -> Result<()> {
        let Some(record) = self.kafka_consumer.poll(self.kafka_poll_timeout_ms) else {
            return Ok(());
        };
        let record = record.context("read live intra 1m Kafka record")?;
        self.consume_kafka_record(
            &record.topic,
            record.partition,
            record.offset,
            &record.payload,
        );
        self.maybe_prune_pending_joins();
        Ok(())
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
                "intra factor 1m pruned stale unmatched joins: factor={} nq={} cutoff_ms={}",
                dropped_factors, dropped_nq, cutoff
            );
        }
    }

    fn on_trade_flow(&mut self, symbol: String, msg: TradeFlowFeatureMsg, record_percentile: bool) {
        let ts_in_ms = msg.ts / 1_000;
        let observations = match {
            let state = self
                .states
                .entry(symbol.clone())
                .or_insert_with(|| SymbolState::new(self.window_size, &self.normalize));
            state.evaluate(msg, &self.plan, self.min_samples, record_percentile)
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
        self.pending_factors.insert(
            (symbol.clone(), ts_in_ms),
            PendingFactorBar { observations },
        );
        self.publish_if_complete(&symbol, ts_in_ms);
    }

    fn on_nq_observation(&mut self, symbol: &str, ts_in_ms: i64, observation: NqObservation) {
        if ts_in_ms < self.history_start_ms {
            return;
        }
        self.pending_nq
            .insert((symbol.to_string(), ts_in_ms), observation);
        self.publish_if_complete(symbol, ts_in_ms);
    }

    fn publish_if_complete(&mut self, symbol: &str, ts_in_ms: i64) {
        let key = (symbol.to_string(), ts_in_ms);
        if !(self.pending_factors.contains_key(&key) && self.pending_nq.contains_key(&key)) {
            return;
        }
        let Some(factor_bar) = self.pending_factors.remove(&key) else {
            return;
        };
        let Some(nq) = self.pending_nq.remove(&key) else {
            return;
        };

        for (output, observation) in self.outputs.iter_mut().zip(factor_bar.observations) {
            output.seq_no = output.seq_no.saturating_add(1);
            let model_msg = ModelMsg::create(
                symbol.to_string(),
                ts_in_ms,
                now_millis(),
                output.seq_no,
                observation.score,
                observation.score_quantile,
                observation.score_ready,
                MODEL_STATUS_OK,
                Vec::new(),
                Vec::new(),
            )
            .with_decision_context(
                observation.score_long_threshold,
                observation.score_short_threshold,
                nq.long_value,
                nq.long_threshold,
                nq.short_value,
                nq.short_threshold,
                nq.ready,
            );
            let published = model_msg
                .to_bytes()
                .map(|bytes| output.publisher.publish(&bytes))
                .unwrap_or(false);
            if published {
                self.stats.published = self.stats.published.saturating_add(1);
                if observation.score_ready && nq.ready {
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

    fn catch_up_kafka(&mut self, config: &KafkaInputConfig) -> Result<()> {
        let watermarks = self
            .kafka_consumer
            .query_topic_watermarks(
                &config.consumer.topics,
                config.consumer.metadata_timeout_ms,
                config.consumer.watermark_timeout_ms,
            )
            .context("query Kafka input watermarks")?;
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
                "Kafka input has no retained records: topics={:?}",
                config.consumer.topics
            );
        }

        info!(
            "Kafka input catch-up starting: venue={} topics={:?} symbols={} history_start={} partitions={}",
            self.venue_slug,
            config.consumer.topics,
            self.allowed_symbols.len(),
            self.history_start_ms,
            target_offsets.len(),
        );

        let deadline = Instant::now() + Duration::from_secs(config.catchup_timeout_secs);
        let mut reached_offsets = HashSet::new();
        let start_raw_messages = self.stats.raw_messages;

        while reached_offsets.len() < target_offsets.len() {
            if Instant::now() >= deadline {
                bail!(
                    "Kafka input catch-up timed out after {}s: reached_partitions={} total_partitions={}",
                    config.catchup_timeout_secs,
                    reached_offsets.len(),
                    target_offsets.len()
                );
            }
            let Some(record) = self.kafka_consumer.poll(self.kafka_poll_timeout_ms) else {
                continue;
            };
            let record = record.context("read Kafka input catch-up record")?;
            let partition_key = (record.topic.clone(), record.partition);
            if let Some(target) = target_offsets.get(&partition_key) {
                if record.offset >= *target {
                    reached_offsets.insert(partition_key);
                }
            }
            self.consume_kafka_record(
                &record.topic,
                record.partition,
                record.offset,
                &record.payload,
            );
        }

        info!(
            "Kafka input catch-up completed: venue={} kafka_records={} reached_partitions={} state_symbols={} active_books={}",
            self.venue_slug,
            self.stats.raw_messages.saturating_sub(start_raw_messages),
            reached_offsets.len(),
            self.states.len(),
            self.aggregators.len(),
        );
        Ok(())
    }

    fn consume_kafka_record(&mut self, topic: &str, partition: i32, offset: i64, payload: &[u8]) {
        self.stats.raw_messages = self.stats.raw_messages.saturating_add(1);
        let period = match decode_period_payload(payload, self.kafka_payload_compression) {
            Ok((_, _, period)) => period,
            Err(err) => {
                self.stats.decode_errors = self.stats.decode_errors.saturating_add(1);
                warn!(
                    "intra factor 1m Kafka PeriodMessage decode failed: venue={} topic={} partition={} offset={} err={:#}",
                    self.venue_slug, topic, partition, offset, err
                );
                return;
            }
        };
        if topic == self.factor_topic {
            self.consume_factor_period_message(&period);
        } else if topic == self.nq_topic {
            self.consume_nq_period_message(&period);
        } else {
            warn!(
                "intra factor 1m ignored unexpected Kafka topic={} factor_topic={} nq_topic={}",
                topic, self.factor_topic, self.nq_topic
            );
        }
    }

    fn consume_factor_period_message(&mut self, period: &PeriodMessage) {
        for symbol_info in &period.symbol_infos {
            let symbol = normalize_symbol_for_venue(&symbol_info.symbol, self.venue);
            if !self.allowed_symbols.contains(&symbol) {
                continue;
            }
            let Some(threshold) = self.amount_thresholds.get(&symbol).copied() else {
                warn!(
                    "intra factor 1m Kafka record has no amount threshold: venue={} symbol={}",
                    self.venue_slug, symbol
                );
                continue;
            };

            let mut events = Vec::with_capacity(symbol_info.trades.len() + symbol_info.incs.len());
            events.extend(symbol_info.incs.iter().map(PeriodEvent::Book));
            events.extend(symbol_info.trades.iter().map(PeriodEvent::Trade));
            sort_period_events(&mut events);

            let bars = {
                let aggregator = self.aggregators.entry(symbol.clone()).or_default();
                for event in events {
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
                            // Retention can start at any delta. Both historical and newly
                            // synthesized snapshots use this partial-book-safe update path.
                            aggregator.on_retained_incremental_book(
                                timestamp_as_micros(book.timestamp),
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
                }
                aggregator.drain_sixty_second_bars()
            };
            for bar in bars {
                self.consume_kafka_bar(&symbol, bar);
            }
        }
    }

    fn consume_nq_period_message(&mut self, period: &PeriodMessage) {
        for symbol_info in &period.symbol_infos {
            let symbol = normalize_symbol_for_venue(&symbol_info.symbol, self.venue);
            if !self.allowed_symbols.contains(&symbol) {
                continue;
            }
            let mut books: Vec<&IncrementOrderBookInfo> = symbol_info.incs.iter().collect();
            books.sort_by_key(|book| normalize_timestamp_ms(book.timestamp));
            let mut completed = Vec::new();
            {
                let state = self.nq_states.entry(symbol.clone()).or_default();
                for book in books {
                    completed.extend(state.on_book(book));
                }
            }
            for (ts_in_ms, observation) in completed {
                self.on_nq_observation(&symbol, ts_in_ms, observation);
            }
        }
    }

    fn consume_kafka_bar(&mut self, symbol: &str, mut bar: BaselineBar) {
        if !historical_bar_has_valid_prices(&bar) {
            return;
        }
        // Research rows use the right-edge label: row t contains [t-60s, t).
        bar.start_ms = bar.start_ms.saturating_add(BAR_MS);
        let payload = match bar.to_trade_flow_feature_payload(symbol, self.venue.to_u8()) {
            Ok(payload) => payload,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!(
                    "Kafka trade-flow feature encoding failed: venue={} symbol={} bar_start={} err={:#}",
                    self.venue_slug, symbol, bar.start_ms, err
                );
                return;
            }
        };
        let msg = match TradeFlowFeatureMsg::from_bytes(payload.as_ref()) {
            Ok(msg) => msg,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!(
                    "Kafka trade-flow feature decode failed: venue={} symbol={} bar_start={} err={:#}",
                    self.venue_slug, symbol, bar.start_ms, err
                );
                return;
            }
        };
        self.on_trade_flow(
            symbol.to_string(),
            msg,
            bar.start_ms >= self.history_start_ms,
        );
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
        output_service_path, parse_trade_side, plan_has_required_factors, select_enabled_symbols,
        sort_period_events, timestamp_as_micros, NormalizeConfig, PeriodEvent, SpotNqState,
        SymbolState, INTRA_FACTOR_NAMES,
    };
    use crate::factor_pub::fusion_factor_pub::SymbolFactorPlan;
    use mkt_parsers::msg::trade_flow_feature_msg::TradeFlowFeatureMsg;
    use order_common::TradingVenue;
    use period_pbs::pb::{IncrementOrderBookInfo, PriceLevel, TradeInfo};
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
        let normalize = NormalizeConfig {
            window_bars: 8,
            min_periods: 2,
            clip_zscore: 3.0,
        };
        let mut state = SymbolState::new(4, &normalize);
        let first = state.observe(vec![1.0; INTRA_FACTOR_NAMES.len()], 2);
        let second = state.observe(vec![2.0; INTRA_FACTOR_NAMES.len()], 2);
        let third = state.observe(vec![3.0; INTRA_FACTOR_NAMES.len()], 2);
        let missing = state.observe(vec![f64::NAN; INTRA_FACTOR_NAMES.len()], 2);

        // One bar is not enough to define the capped-series statistics, so the
        // z-score stays unresolved and the slot is not observed.
        assert!(first[0].score.is_nan());
        assert_eq!(first[0].score_quantile, None);
        assert_eq!(first[0].score_long_threshold, None);
        assert_eq!(first[0].score_short_threshold, None);
        assert!(!first[0].score_ready);
        // Capped window [1.0, 2.0] -> z = (2.0 - 1.5) / std(1.0, 2.0) ≈ 0.7071.
        assert!((second[0].score - 0.7071).abs() < 1e-3);
        assert_eq!(second[0].score_quantile, Some(0.5));
        assert!(!second[0].score_ready);
        // Capped window [1.0, 2.0, 3.0] -> z = (3.0 - 2.0) / std = 1.0, and the
        // percentile window now holds two finite values meeting min_samples.
        assert!((third[0].score - 1.0).abs() < 1e-6);
        assert!(third[0].score_ready);
        // A missing bar carries the previous z-score forward (causal ffill).
        assert!((missing[0].score - 1.0).abs() < 1e-6);
        assert!(missing[0].score_ready);
    }

    #[test]
    fn nq_threshold_includes_current_bar_and_requires_720_valid_values() {
        let mut state = SpotNqState::default();
        for _ in 0..719 {
            state.current_close = 100.0;
            assert!(!state.close_current_bar().ready);
        }
        state.current_close = 101.0;
        let observation = state.close_current_bar();
        assert!(observation.ready);
        assert!((observation.long_value.unwrap() - 0.01).abs() < 1e-12);
        assert_eq!(observation.short_value, Some(0.0));
        assert_eq!(observation.long_threshold, Some(0.0));
        assert_eq!(observation.short_threshold, Some(0.0));

        state.current_close = f64::NAN;
        let missing = state.close_current_bar();
        assert!(!missing.ready);
        assert_eq!(missing.long_value, None);
        assert_eq!(missing.short_value, None);
    }

    #[test]
    fn nq_bar_is_labeled_by_right_edge_and_uses_last_bbo() {
        let first = IncrementOrderBookInfo {
            timestamp: 59_000,
            is_snapshot: false,
            bids: vec![PriceLevel {
                price: 102.0,
                amount: 1.0,
            }],
            asks: vec![PriceLevel {
                price: 104.0,
                amount: 1.0,
            }],
        };
        let next = IncrementOrderBookInfo {
            timestamp: 60_000,
            is_snapshot: false,
            bids: vec![
                PriceLevel {
                    price: 102.0,
                    amount: 0.0,
                },
                PriceLevel {
                    price: 103.0,
                    amount: 1.0,
                },
            ],
            asks: vec![
                PriceLevel {
                    price: 104.0,
                    amount: 0.0,
                },
                PriceLevel {
                    price: 105.0,
                    amount: 1.0,
                },
            ],
        };
        let mut state = SpotNqState::default();
        assert!(state.on_book(&first).is_empty());
        let closed = state.on_book(&next);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].0, 60_000);
        assert_eq!(closed[0].1.long_value, Some(0.0));
        assert_eq!(state.current_close, 104.0);
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
        assert_eq!(parse_trade_side("BUY"), Some(true));
        assert_eq!(parse_trade_side("s"), Some(false));
        assert_eq!(parse_trade_side("unknown"), None);
    }

    #[test]
    fn stable_sort_preserves_same_timestamp_book_sequence() {
        let first = IncrementOrderBookInfo {
            timestamp: 1_704_067_200_123_000,
            is_snapshot: false,
            bids: vec![PriceLevel {
                price: 100.0,
                amount: 1.0,
            }],
            asks: Vec::new(),
        };
        let second = IncrementOrderBookInfo {
            timestamp: first.timestamp,
            is_snapshot: false,
            bids: vec![PriceLevel {
                price: 100.0,
                amount: 0.0,
            }],
            asks: Vec::new(),
        };
        let trade = TradeInfo {
            timestamp: first.timestamp,
            side: "buy".to_string(),
            price: 100.0,
            amount: 1.0,
        };
        let mut events = vec![
            PeriodEvent::Book(&first),
            PeriodEvent::Book(&second),
            PeriodEvent::Trade(&trade),
        ];

        sort_period_events(&mut events);

        assert!(matches!(events[0], PeriodEvent::Book(book) if book.bids[0].amount == 1.0));
        assert!(matches!(events[1], PeriodEvent::Book(book) if book.bids[0].amount == 0.0));
        assert!(matches!(events[2], PeriodEvent::Trade(_)));
    }

    #[test]
    fn ignores_replayed_kafka_bar_with_same_timestamp() {
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
        let mut state = SymbolState::new(3, &NormalizeConfig::default());

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
