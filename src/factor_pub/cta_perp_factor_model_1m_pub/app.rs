use super::nq_multi_minute::MultiMinuteNq;
use crate::cta_perp::interval::BarInterval;
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
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::common::amount_threshold::AmountThreshold;
use crate::depth_pub::orderbook::OrderBook;
use crate::factor_pub::cta_factor_universe::{load_enabled_symbols, CtaFactorUniverse};
use crate::factor_pub::fusion_factor_pub::app::{
    load_amount_thresholds_from_tlen_server, BaselineReplayState,
};
use crate::factor_pub::fusion_factor_pub::cfg::TlenServerConfig;
use crate::factor_pub::fusion_factor_pub::SymbolFactorPlan;
use crate::factor_pub::model_output_publisher::ModelPublisher;
use crate::factor_pub::trade_flow_feature_pub::local_baseline::{
    BaselineBar, LocalBaselineAggregator, MultiMinuteBaseline,
};

use super::cfg::{CtaPerpFactorPubConfig, KafkaInputConfig, NormalizeConfig};

use crate::factor_pub::cta_nq::{observe_nq, NqObservation, NQ_LOOKBACK_BARS, NQ_QUANTILE_WINDOW};
const PENDING_JOIN_RETENTION_MS: i64 = 6 * 60 * 60 * 1_000;
const TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE: &str = "amount_thresholds";
const STATS_LOG_INTERVAL_SECS: u64 = 60;

// These formulas use OHLCV or depth only. Unreviewed formulas retain the
// existing strict requirement for real trade-size classification metadata.
fn requires_amount_thresholds(factors: &[String]) -> bool {
    factors.iter().any(|factor| {
        !matches!(
            factor.as_str(),
            "baseline_200"
                | "baseline_164"
                | "TD_MT_019"
                | "factor_113"
                | "baseline_036"
                | "baseline_146"
                | "baseline_141"
                | "baseline_005"
                | "TD_TI_017"
                | "baseline_028"
                | "TD_MT_029"
                | "factor_051"
                | "baseline_116"
                | "baseline_117"
                | "factor_170"
                | "factor_036"
                | "baseline_185"
                | "baseline_001"
                | "baseline_013"
                | "baseline_022"
                | "baseline_040"
                | "baseline_051"
                | "baseline_104"
                | "baseline_111"
                | "baseline_118"
                | "baseline_119"
                | "baseline_120"
                | "baseline_132"
                | "baseline_138"
                | "baseline_195"
                | "factor_032"
                | "factor_049"
                | "factor_094"
                | "factor_128"
                | "factor_171"
                | "TD_MT_032"
                | "TD_MT_040"
                | "TD_MT_042"
                | "TD_PR_010"
                | "TD_TI_008"
                | "TD_TI_027"
                | "TD_TI_033"
                | "TP_VPI_001"
                | "TP_VPI_006"
                | "TP_VPI_009"
                | "TP_VPI_018"
                | "TP_VPI_019"
                | "avg_price"
                | "sell_avg_price"
                | "buy_avg_price"
                | "baseline_020"
                | "baseline_091"
                | "baseline_140"
                | "baseline_160"
                | "baseline_169"
                | "factor_114"
                | "TD_PR_013"
                | "TD_TI_013"
                | "TD_TI_018"
                | "baseline_151"
                | "TD_CI_008"
                | "TD_PT_002"
                | "TD_TI_024"
                | "TD_TI_043"
                | "TP_VPI_002"
                | "factor_trades_040"
        )
    })
}

struct FactorOutput {
    name: String,
    factor_index: usize,
    service: String,
    publisher: ModelPublisher,
    seq_no: u64,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
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
    min_samples: usize,
    tp_vpi_006_min_periods: usize,
    evaluator: BaselineReplayState,
    windows: Vec<ExactRollingWindow>,
    normalize: Vec<NormalizeState>,
    last_ts: Option<i64>,
}

impl SymbolFactorState {
    fn new(normalize: &NormalizeConfig, factor_count: usize, interval: BarInterval) -> Self {
        Self {
            min_samples: interval.factor_min_samples(),
            tp_vpi_006_min_periods: match interval {
                BarInterval::OneMinute => 360,
                BarInterval::FiveMinutes | BarInterval::FifteenMinutes => 280,
            },
            evaluator: BaselineReplayState::default(),
            windows: (0..factor_count)
                .map(|_| ExactRollingWindow::new(interval.factor_quantile_window()))
                .collect(),
            normalize: (0..factor_count)
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
        let raw_values = self
            .evaluator
            .factor_values_with_tp_vpi_006_min_periods(plan, self.tp_vpi_006_min_periods);
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
                        ready: observed && window.len() >= self.min_samples && quantile.is_some(),
                    }
                })
                .collect(),
        ))
    }
}

struct NqState {
    interval: BarInterval,
    multi_minute: MultiMinuteNq,
    last_valid_book_ms: Option<i64>,
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
        Self::new(BarInterval::OneMinute)
    }
}

impl NqState {
    fn new(interval: BarInterval) -> Self {
        Self {
            interval,
            multi_minute: MultiMinuteNq::new(interval),
            last_valid_book_ms: None,
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
        let bar_ms = self.interval.millis();
        let target_start = timestamp_ms.div_euclid(bar_ms).saturating_mul(bar_ms);
        let mut closed = Vec::new();
        match self.current_start_ms {
            None => self.current_start_ms = Some(target_start),
            Some(current) if target_start < current => return closed,
            Some(mut current) => {
                while current < target_start {
                    closed.push((current.saturating_add(bar_ms), self.close_bar()));
                    current = current.saturating_add(bar_ms);
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
                self.last_valid_book_ms = Some(timestamp_ms);
                for (_, observation) in &mut closed {
                    observation.execution_ts_ms = timestamp_ms;
                    observation.execution_bid = Some(bid);
                    observation.execution_ask = Some(ask);
                }
            }
        }
        closed
    }

    fn close_bar(&mut self) -> NqObservation {
        if self.interval.uses_linear_nq() {
            let end = self.current_start_ms.unwrap_or(0) + self.interval.millis();
            let mid = if self
                .last_valid_book_ms
                .is_some_and(|ts| ts >= end - 60_000 && ts < end)
            {
                self.current_close
            } else {
                f64::NAN
            };
            return self.multi_minute.observe(mid);
        }
        observe_nq(
            self.current_close,
            &mut self.closes,
            &mut self.long_window,
            &mut self.short_window,
        )
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

pub struct CtaPerpFactorModelPubApp {
    interval: BarInterval,
    multi_minute_bars: HashMap<String, MultiMinuteBaseline>,
    venue: TradingVenue,
    venue_slug: String,
    tlen_server: TlenServerConfig,
    universe: CtaFactorUniverse,
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
    history: FactorHistoryWriter,
}

/// The same causal factor/NQ observations are saved during warm-up and live
/// operation. Reweighting history never requires an execution publisher.
struct FactorHistoryWriter {
    interval: BarInterval,
    root: PathBuf,
    current: Option<(String, BufWriter<File>)>,
    generation: String,
    caught_up: bool,
    latest: std::collections::BTreeMap<String, i64>,
    last_status: Instant,
}

impl FactorHistoryWriter {
    fn new(root: &Path, interval: BarInterval) -> Result<Self> {
        fs::create_dir_all(root).context("create CTA factor history directory")?;
        let writer = Self {
            interval,
            root: root.to_path_buf(),
            current: None,
            generation: uuid::Uuid::new_v4().to_string(),
            caught_up: false,
            latest: Default::default(),
            last_status: Instant::now(),
        };
        writer.write_status()?;
        Ok(writer)
    }

    fn write_status(&self) -> Result<()> {
        let path = self.root.join("recovery.json");
        let tmp = path.with_extension("json.tmp");
        fs::write(
            &tmp,
            serde_json::to_vec(&serde_json::json!({
                "bar_interval": self.interval,
                "generation": self.generation, "caught_up": self.caught_up,
                "latest": self.latest, "updated_ms": now_millis(),
            }))?,
        )?;
        fs::rename(tmp, path).context("save factor recovery progress")
    }

    fn record(
        &mut self,
        symbol: &str,
        ts: i64,
        names: &[String],
        factors: &[FactorObservation],
        nq: &NqObservation,
    ) -> Result<()> {
        let date = chrono::DateTime::from_timestamp_millis(ts)
            .context("invalid CTA factor history timestamp")?
            .format("%Y%m%d")
            .to_string();
        if self
            .current
            .as_ref()
            .is_none_or(|(previous, _)| previous != &date)
        {
            if let Some((_, writer)) = &mut self.current {
                writer.flush()?;
            }
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.root.join(format!("{date}.jsonl")))
                .context("open CTA factor history")?;
            self.current = Some((date, BufWriter::new(file)));
        }
        let inputs: std::collections::BTreeMap<_, _> = names.iter().zip(factors).collect();
        let writer = &mut self
            .current
            .as_mut()
            .context("missing CTA history writer")?
            .1;
        serde_json::to_writer(
            &mut *writer,
            &serde_json::json!({
                "generation": self.generation,
                "bar_interval": self.interval, "bar_start_ms": ts - self.interval.millis(),
                "symbol":symbol, "model_ts_ms":ts, "inputs":inputs, "nq":nq,
            }),
        )?;
        writer.write_all(b"\n")?;
        writer.flush().context("flush CTA factor history")?;
        self.latest.insert(symbol.to_string(), ts);
        Ok(())
    }
}

impl CtaPerpFactorModelPubApp {
    pub async fn new(
        path: &str,
        venue: TradingVenue,
        wait_for_publishers: bool,
        history_path: &Path,
        replay_only: bool,
        interval: BarInterval,
    ) -> Result<Self> {
        anyhow::ensure!(
            venue == TradingVenue::BinanceFutures,
            "CTA perp factor publisher only supports binance-futures"
        );
        let config = CtaPerpFactorPubConfig::load(path)?;
        anyhow::ensure!(
            config.bar_interval == interval,
            "publisher bin/config bar_interval mismatch"
        );
        let venue_slug = venue.data_pub_slug().to_string();
        let plan = SymbolFactorPlan::from_factor_names(
            &format!("cta_perp_factor_model_{}", interval.as_str()),
            config.universe.factors.clone(),
        )?;
        BaselineReplayState::validate_factor_plan(&plan)
            .context("validate CTA perp factor plan")?;
        let needs_thresholds = requires_amount_thresholds(&config.universe.factors);
        let allowed_symbols = if needs_thresholds {
            load_enabled_symbols(&config.tlen_server, venue, &venue_slug, &config.universe).await?
        } else {
            config
                .universe
                .symbols
                .iter()
                .map(|s| normalize_symbol_for_venue(s, venue))
                .collect()
        };
        config.universe.require_complete(&allowed_symbols, venue)?;
        let amount_thresholds = if needs_thresholds {
            load_amount_thresholds_from_tlen_server(
                &config.tlen_server,
                venue,
                &venue_slug,
                TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE,
            )
            .await?
        } else {
            HashMap::new()
        };
        for symbol in allowed_symbols.iter().filter(|_| needs_thresholds) {
            anyhow::ensure!(
                amount_thresholds.contains_key(symbol),
                "missing trade-flow amount thresholds: {symbol}"
            );
        }
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
            .context("create CTA perp factor Kafka consumer")?;
        let mut app = Self {
            interval,
            multi_minute_bars: HashMap::new(),
            venue,
            venue_slug,
            tlen_server: config.tlen_server.clone(),
            universe: config.universe.clone(),
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
            history: FactorHistoryWriter::new(history_path, interval)?,
        };
        app.catch_up(&config.kafka)?;
        if replay_only {
            anyhow::ensure!(
                app.stats.decode_errors == 0 && app.stats.evaluation_errors == 0,
                "CTA replay had decode/evaluation errors"
            );
            info!("CTA history replay complete; no IPC publishers created");
            return Ok(app);
        }
        loop {
            match create_shared_outputs(&app.venue_slug, &app.universe.factors, interval) {
                Ok(outputs) => {
                    app.outputs = outputs;
                    break;
                }
                Err(err) if wait_for_publishers => {
                    warn!(
                        "CTA perp factor catch-up complete; waiting for shared publisher ownership: {err:#}"
                    );
                    if let Some(record) = app.consumer.poll(app.poll_timeout_ms) {
                        let record = record.context(
                            "read CTA perp factor Kafka record while waiting for publishers",
                        )?;
                        app.consume_record(&record.topic, &record.payload);
                    }
                }
                Err(err) => return Err(err),
            }
        }
        app.publish_enabled = true;
        app.history.caught_up = true;
        app.history.write_status()?;
        info!(
            "CTA perp shared factor publisher ready venue={} symbols={} outputs={:?} zscore={}/{}/clip{} percentile={}/{} interval={}",
            app.venue_slug,
            app.allowed_symbols.len(),
            app.outputs
                .iter()
                .map(|output| output.service.as_str())
                .collect::<Vec<_>>(),
            app.normalize.window_bars,
            app.normalize.min_periods,
            app.normalize.clip_zscore,
            interval.factor_quantile_window(),
            interval.factor_min_samples(),
            interval.as_str(),
        );
        Ok(app)
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            self.maybe_reload_symbols().await;
            if let Some(record) = self.consumer.poll(self.poll_timeout_ms) {
                let record = record.context("read CTA perp factor Kafka record")?;
                self.consume_record(&record.topic, &record.payload);
            }
            self.maybe_log_stats();
            if self.history.last_status.elapsed() >= Duration::from_secs(1) {
                self.history.write_status()?;
                self.history.last_status = Instant::now();
            }
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
                warn!("CTA perp PeriodMessage decode failed: {err:#}");
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
            let threshold = self.amount_thresholds.get(&symbol).copied();
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
                                if let Some(threshold) = threshold {
                                    aggregator.on_trade_with_threshold(
                                        timestamp_as_micros(trade.timestamp),
                                        is_buy,
                                        trade.price,
                                        trade.amount,
                                        threshold,
                                    );
                                } else {
                                    aggregator.on_trade(
                                        timestamp_as_micros(trade.timestamp),
                                        is_buy,
                                        trade.price,
                                        trade.amount,
                                    );
                                }
                            }
                        }
                    }
                }
                // This consumer only uses configured minute output; do not retain unused 10s bars.
                aggregator.drain_ten_second_bars();
                aggregator.drain_sixty_second_bars()
            };
            let bars = if self.interval.uses_linear_nq() {
                let resampler = self
                    .multi_minute_bars
                    .entry(symbol.clone())
                    .or_insert_with(|| MultiMinuteBaseline::new(self.interval.millis()));
                bars.iter().flat_map(|bar| resampler.push(bar)).collect()
            } else {
                bars
            };
            for mut bar in bars {
                if !valid_bar(&bar) {
                    continue;
                }
                bar.start_ms = bar.start_ms.saturating_add(self.interval.millis());
                self.consume_bar(&symbol, bar);
            }
        }
    }

    fn consume_nq_books(&mut self, symbol: &str, books: &[IncrementOrderBookInfo]) {
        let mut books: Vec<&IncrementOrderBookInfo> = books.iter().collect();
        books.sort_by_key(|book| normalize_timestamp_ms(book.timestamp));
        let mut completed = Vec::new();
        let state = self
            .nq_states
            .entry(symbol.to_string())
            .or_insert_with(|| NqState::new(self.interval));
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
                warn!("CTA perp bar encode failed symbol={symbol}: {err:#}");
                return;
            }
        };
        let msg = match TradeFlowFeatureMsg::from_bytes(payload.as_ref()) {
            Ok(value) => value,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!("CTA perp feature decode failed symbol={symbol}: {err:#}");
                return;
            }
        };
        let ts = msg.ts;
        let observations = match self
            .factor_states
            .entry(symbol.to_string())
            .or_insert_with(|| {
                SymbolFactorState::new(
                    &self.normalize,
                    self.plan.factor_names().len(),
                    self.interval,
                )
            })
            .evaluate(msg, &self.plan, ts >= self.history_start_ms)
        {
            Ok(Some(value)) => value,
            Ok(None) => return,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!("CTA perp factor evaluation failed symbol={symbol}: {err:#}");
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
        if let Err(err) = self
            .history
            .record(symbol, ts, &self.universe.factors, &factors, &nq)
        {
            self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
            warn!("CTA factor history write failed symbol={symbol} ts={ts}: {err:#}");
        }
        if !self.publish_enabled {
            return;
        }
        for output in &mut self.outputs {
            let Some(factor) = factors.get(output.factor_index) else {
                self.stats.publish_failed = self.stats.publish_failed.saturating_add(1);
                warn!(
                    "CTA perp factor output index missing factor={} index={} observations={}",
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
                nq.long_threshold,
                nq.short_value,
                nq.short_threshold,
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
                    "CTA perp model publish failed symbol={} factor={} service={}",
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
                "CTA perp pruned stale unmatched joins factor={} nq={} cutoff_ms={}",
                dropped_factors, dropped_nq, cutoff
            );
        }
    }

    fn catch_up(&mut self, config: &KafkaInputConfig) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(config.catchup_timeout_secs);
        let mut consumed: HashMap<(String, i32), i64> = HashMap::new();
        loop {
            let targets: HashMap<(String, i32), i64> = self
                .consumer
                .query_topic_watermarks(
                    &config.consumer.topics,
                    config.consumer.metadata_timeout_ms,
                    config.consumer.watermark_timeout_ms,
                )?
                .into_iter()
                .filter(|w| w.high > w.low)
                .map(|w| ((w.topic, w.partition), w.high - 1))
                .collect();
            if targets.is_empty() {
                bail!("CTA perp Kafka topic has no retained records");
            }
            if targets
                .iter()
                .all(|(key, target)| consumed.get(key).is_some_and(|offset| offset >= target))
            {
                break;
            }
            while !targets
                .iter()
                .all(|(key, target)| consumed.get(key).is_some_and(|offset| offset >= target))
            {
                if Instant::now() >= deadline {
                    bail!("CTA perp Kafka catch-up timed out");
                }
                let Some(record) = self.consumer.poll(self.poll_timeout_ms) else {
                    continue;
                };
                let record = record.context("read CTA perp catch-up record")?;
                consumed.insert((record.topic.clone(), record.partition), record.offset);
                self.consume_record(&record.topic, &record.payload);
            }
            // Re-read the head: replay time itself must not leave a live backlog.
        }
        anyhow::ensure!(
            self.stats.decode_errors == 0 && self.stats.evaluation_errors == 0,
            "CTA perp recovery encountered decode/evaluation errors"
        );
        Ok(())
    }

    async fn maybe_reload_symbols(&mut self) {
        if !requires_amount_thresholds(&self.universe.factors) {
            return;
        }
        if self.last_symbol_reload.elapsed() < self.symbol_reload_interval {
            return;
        }
        self.last_symbol_reload = Instant::now();
        match load_enabled_symbols(
            &self.tlen_server,
            self.venue,
            &self.venue_slug,
            &self.universe,
        )
        .await
        {
            Ok(symbols) if !symbols.is_empty() => {
                self.allowed_symbols = symbols;
                self.factor_states
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
                self.nq_states
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
                self.aggregators
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
            }
            Ok(_) => warn!("CTA perp symbol reload returned empty set; retaining previous"),
            Err(err) => warn!("CTA perp symbol reload failed: {err:#}"),
        }
    }

    fn maybe_log_stats(&mut self) {
        if self.last_stats_log.elapsed() < Duration::from_secs(STATS_LOG_INTERVAL_SECS) {
            return;
        }
        info!(
            "CTA perp factor stats records={} decode_errors={} eval_errors={} published={} publish_failed={} factor_symbols={} nq_symbols={}",
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

fn create_shared_outputs(
    venue_slug: &str,
    factors: &[String],
    interval: BarInterval,
) -> Result<Vec<FactorOutput>> {
    let mut outputs = Vec::with_capacity(factors.len());
    for (factor_index, name) in factors.iter().enumerate() {
        let service = crate::cta_perp::model_service::model_service_path_for_interval(
            venue_slug, name, interval,
        );
        let node_name = format!(
            "cta_perp_factor_{}_{}_{}",
            venue_slug,
            interval.as_str(),
            name
        )
        .replace('-', "_")
        .to_ascii_lowercase();
        outputs.push(FactorOutput {
            name: name.clone(),
            factor_index,
            publisher: ModelPublisher::new(&node_name, &service)?,
            service,
            seq_no: 0,
        });
    }
    Ok(outputs)
}

pub fn output_service_path(venue_slug: &str, factor: &str) -> String {
    crate::cta_perp::model_service::model_service_path(venue_slug, factor)
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

    fn nq_book(ts: i64, bid: f64, ask: f64) -> IncrementOrderBookInfo {
        use period_pbs::pb::PriceLevel;
        IncrementOrderBookInfo {
            timestamp: ts,
            is_snapshot: false,
            bids: vec![PriceLevel {
                price: bid,
                amount: 1.0,
            }],
            asks: vec![PriceLevel {
                price: ask,
                amount: 1.0,
            }],
        }
    }

    #[test]
    fn multi_minute_nq_uses_own_last_minute_book_only_after_close() {
        for interval in [BarInterval::FiveMinutes, BarInterval::FifteenMinutes] {
            let bar_ms = interval.millis();
            let start = 1_800_000_000_000_i64; // UTC 5m boundary
            let mut state = NqState::new(interval);
            let mut other_symbol = NqState::new(interval);
            assert!(state.on_book(&nq_book(start, 99.0, 101.0)).is_empty());
            assert!(other_symbol
                .on_book(&nq_book(start + (bar_ms - 1), 9.0, 11.0))
                .is_empty());
            assert!(state
                .on_book(&nq_book(start + (bar_ms - 1), 109.0, 111.0))
                .is_empty());
            // The 5m close uses the old 110 mid, not the new 210 execution quote.
            let rows = state.on_book(&nq_book(start + bar_ms, 209.0, 211.0));
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, start + bar_ms);
            assert_eq!(rows[0].1.long_value, Some(0.0));
            assert_eq!(rows[0].1.execution_ask, Some(211.0));
            assert!(state
                .on_book(&nq_book(start + (2 * bar_ms - 1), 219.0, 221.0))
                .is_empty());
            let next = state.on_book(&nq_book(start + (2 * bar_ms), 219.0, 221.0));
            assert_eq!(next[0].1.long_value, Some(1.0)); // (220 - 110) / 110
                                                         // No book in the last minute of the third bucket: no valid NQ sample.
            let gap = state.on_book(&nq_book(start + (3 * bar_ms), 219.0, 221.0));
            assert_eq!(gap[0].1.long_value, None);
            assert!(!gap[0].1.ready);
            let other = other_symbol.on_book(&nq_book(start + bar_ms, 9.0, 11.0));
            assert_eq!(other[0].1.long_value, Some(0.0));
        }
    }

    fn normalize_config(window_bars: usize, min_periods: usize) -> NormalizeConfig {
        NormalizeConfig {
            window_bars,
            min_periods,
            clip_zscore: 3.0,
        }
    }

    #[test]
    fn five_minute_tp_vpi_006_uses_280_inner_samples_and_keeps_outer_mean() {
        use mkt_parsers::msg::trade_flow_feature_msg::{
            TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
        };
        let plan = SymbolFactorPlan::from_factor_names("test", vec!["TP_VPI_006".into()]).unwrap();
        let mut raw = BaselineReplayState::default();
        let mut five_minute =
            SymbolFactorState::new(&normalize_config(288, 200), 1, BarInterval::FiveMinutes);
        let one_minute =
            SymbolFactorState::new(&normalize_config(1440, 1000), 1, BarInterval::OneMinute);
        assert_eq!(one_minute.tp_vpi_006_min_periods, 360);
        let mut first_z = None;
        let mut first_ready = None;
        for bar in 0..900 {
            let close = (1000 + (bar * 17) % 71) as f64;
            let volume = (1 + (bar * 13) % 23) as f64;
            let mut values = vec![1.0; TRADE_FLOW_FEATURE_DIM];
            for (name, value) in [
                ("open", close),
                ("high", close + 1.0),
                ("low", close - 1.0),
                ("close", close),
                ("volume", volume),
                ("vwap", close),
                ("buy_vwap", close),
                ("sell_vwap", close),
            ] {
                values[TRADE_FLOW_FEATURE_FIELD_NAMES
                    .iter()
                    .position(|f| *f == name)
                    .unwrap()] = value;
            }
            let msg = TradeFlowFeatureMsg::from_indexed_values(
                "SKLUSDT".into(),
                TradingVenue::BinanceFutures.to_u8(),
                1_800_000_000_000 + bar * 300_000,
                &values,
            )
            .unwrap();
            raw.push(msg.clone()).unwrap();
            let actual = raw.factor_values_with_tp_vpi_006_min_periods(&plan, 280)[0];
            let original = raw.factor_values(&plan)[0];
            let count = bar + 1;
            assert_eq!(actual.is_finite(), count >= 293);
            assert_eq!(original.is_finite(), count >= 373);
            if original.is_finite() {
                assert!((actual - original).abs() < 1e-10);
            }
            // Oracle: pandas rolling(360, min_periods=280).sum().rolling(14).mean().
            let expected = match count {
                293 => Some(1916.642857142857),
                300 => Some(1958.857142857143),
                359 => Some(2351.285714285714),
                360 => Some(2360.0714285714284),
                372 => Some(2398.714285714286),
                373 => Some(2396.9285714285716),
                600 => Some(2290.6428571428573),
                779 => Some(2322.285714285714),
                _ => None,
            };
            if let Some(expected) = expected {
                assert!((actual - expected).abs() < 1e-9);
            }
            let observation = five_minute.evaluate(msg, &plan, true).unwrap().unwrap()[0];
            if observation.score.is_finite() {
                first_z.get_or_insert(count);
            }
            if observation.ready {
                first_ready.get_or_insert(count);
            }
        }
        assert_eq!(first_z, Some(492));
        assert_eq!(first_ready, Some(779));
    }

    #[test]
    fn multi_minute_factor_readiness_and_signed_ranks_use_two_day_window() {
        use mkt_parsers::msg::trade_flow_feature_msg::{
            TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
        };
        for (interval, norm_window, norm_min, rank_window, rank_min) in [
            (BarInterval::FiveMinutes, 288, 200, 576, 288),
            (BarInterval::FifteenMinutes, 96, 67, 192, 96),
        ] {
            let plan =
                SymbolFactorPlan::from_factor_names("test", vec!["factor_001".into()]).unwrap();
            let mut state =
                SymbolFactorState::new(&normalize_config(norm_window, norm_min), 1, interval);
            let mut scores = VecDeque::new();
            let mut saw_ready = false;
            for bar in 0..1000 {
                let mid = 100.0 + ((bar * 17) % 31) as f64;
                let mut values = vec![1.0; TRADE_FLOW_FEATURE_DIM];
                for name in [
                    "open",
                    "high",
                    "low",
                    "close",
                    "vwap",
                    "buy_vwap",
                    "sell_vwap",
                ] {
                    values[TRADE_FLOW_FEATURE_FIELD_NAMES
                        .iter()
                        .position(|field| *field == name)
                        .unwrap()] = mid;
                }
                for side in [-1.0, 1.0] {
                    for level in 0..20 {
                        values.extend([mid + side * (level as f64 + 1.0), 10.0 + (bar % 7) as f64]);
                    }
                }
                let msg = TradeFlowFeatureMsg::from_indexed_values(
                    "BELUSDT".into(),
                    TradingVenue::BinanceFutures.to_u8(),
                    1_800_000_000_000 + bar * interval.millis(),
                    &values,
                )
                .unwrap();
                let observed = state.evaluate(msg, &plan, true).unwrap().unwrap()[0];
                scores.push_back(observed.score);
                if scores.len() > rank_window {
                    scores.pop_front();
                }
                let valid: Vec<_> = scores
                    .iter()
                    .copied()
                    .filter(|value| value.is_finite())
                    .collect();
                assert_eq!(
                    observed.ready,
                    observed.score.is_finite() && valid.len() >= rank_min
                );
                if let Some(q) = observed.quantile {
                    let negative_rank = (valid.iter().filter(|x| **x > observed.score).count()
                        as f64
                        + 0.5 * valid.iter().filter(|x| **x == observed.score).count() as f64)
                        / valid.len() as f64;
                    assert!((1.0 - q - negative_rank).abs() < 1e-12);
                }
                saw_ready |= observed.ready;
            }
            assert!(saw_ready);
        }
    }
    #[test]
    fn reviewed_ohlcv_depth_factors_ignore_trade_size_buckets() {
        use mkt_parsers::msg::trade_flow_feature_msg::{
            TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
        };
        let mut names: Vec<String> = [
            "baseline_200",
            "baseline_164",
            "TD_MT_019",
            "factor_113",
            "baseline_036",
            "baseline_146",
            "baseline_141",
            "baseline_005",
            "TD_TI_017",
            "baseline_028",
            "TD_MT_029",
            "factor_051",
            "baseline_116",
            "baseline_117",
            "factor_170",
            "factor_036",
            "baseline_185",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        let rx04: super::CtaPerpFactorPubConfig = toml::from_str(include_str!(
            "../../../config/cta_perp_rx04/cta_perp_factor_model_5m_pub.toml"
        ))
        .unwrap();
        names.extend(rx04.universe.factors);
        let rx02: super::CtaPerpFactorPubConfig = toml::from_str(include_str!(
            "../../../config/cta_perp_rx02/cta_perp_factor_model_1m_pub.toml"
        ))
        .unwrap();
        names.extend(rx02.universe.factors);
        let rx03: super::CtaPerpFactorPubConfig = toml::from_str(include_str!(
            "../../../config/cta_perp_rx03/cta_perp_factor_model_1m_pub.toml"
        ))
        .unwrap();
        names.extend(rx03.universe.factors);
        names.sort();
        names.dedup();
        assert!(!requires_amount_thresholds(&names));
        assert!(requires_amount_thresholds(&["net_buy_medium".into()]));
        assert!(requires_amount_thresholds(&["baseline_047".into()]));
        assert!(requires_amount_thresholds(&["unknown".into()]));
        let plan = SymbolFactorPlan::from_factor_names("test", names.clone()).unwrap();
        let mut plain = BaselineReplayState::default();
        let mut classified = BaselineReplayState::default();
        let mut observed_finite = HashSet::new();
        for bar in 0..900 {
            let mid = 100.0 + bar as f64 * 0.01 + (bar as f64 * 0.1).sin();
            let mut values = vec![1.0; TRADE_FLOW_FEATURE_DIM];
            for (name, value) in [
                ("open", mid - 0.05),
                ("high", mid + 1.0),
                ("low", mid - 1.0),
                ("close", mid),
                ("vwap", mid),
                ("buy_vwap", mid - 0.03),
                ("sell_vwap", mid + 0.03),
            ] {
                values[TRADE_FLOW_FEATURE_FIELD_NAMES
                    .iter()
                    .position(|s| *s == name)
                    .unwrap()] = value;
            }
            for side in [-1.0, 1.0] {
                for level in 0..20 {
                    values.extend([mid + side * (level as f64 + 1.0) * 0.01, 10.0]);
                }
            }
            let mut different = values.clone();
            for (i, name) in TRADE_FLOW_FEATURE_FIELD_NAMES.iter().enumerate() {
                if name.contains("large") || name.contains("medium") || name.contains("small") {
                    values[i] = 0.0;
                    different[i] = (bar + 1) as f64 * (i + 1) as f64;
                }
            }
            let ts = 1_704_067_200_000 + bar * 60_000;
            plain
                .push(
                    TradeFlowFeatureMsg::from_indexed_values(
                        "ETHUSDT".into(),
                        TradingVenue::BinanceFutures.to_u8(),
                        ts,
                        &values,
                    )
                    .unwrap(),
                )
                .unwrap();
            classified
                .push(
                    TradeFlowFeatureMsg::from_indexed_values(
                        "ETHUSDT".into(),
                        TradingVenue::BinanceFutures.to_u8(),
                        ts,
                        &different,
                    )
                    .unwrap(),
                )
                .unwrap();
            if bar >= 300 {
                for ((name, a), b) in names
                    .iter()
                    .zip(plain.factor_values(&plan))
                    .zip(classified.factor_values(&plan))
                {
                    assert_eq!(a.is_finite(), b.is_finite(), "{name}: {a} != {b}");
                    if a.is_finite() {
                        assert!((a - b).abs() < 1e-10, "{name}: {a} != {b}");
                        observed_finite.insert(name.clone());
                    }
                }
            }
        }
        assert_eq!(observed_finite.len(), names.len());
    }

    #[test]
    fn output_contract_is_isolated_from_intra() {
        assert_eq!(
            output_service_path("binance-futures", "TP_VPI_018"),
            "model_output/cta_perp-binance-futures-1m-tp_vpi_018"
        );
    }

    #[test]
    fn publisher_exposes_configured_factor_services() {
        assert_eq!(
            toml::from_str::<super::CtaPerpFactorPubConfig>(include_str!(
                "../../../config/cta_perp_factor_model_1m_pub.toml"
            ))
            .expect("repository config")
            .universe
            .factors
            .iter()
            .map(|factor| output_service_path("binance-futures", factor))
            .collect::<Vec<_>>(),
            vec![
                "model_output/cta_perp-binance-futures-1m-baseline_141",
                "model_output/cta_perp-binance-futures-1m-baseline_200",
                "model_output/cta_perp-binance-futures-1m-td_ti_024",
                "model_output/cta_perp-binance-futures-1m-factor_036",
                "model_output/cta_perp-binance-futures-1m-baseline_164",
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
    fn nq_is_ready_after_60_valid_minutes() {
        let mut state = NqState::default();
        for _ in 0..59 {
            state.current_close = 100.0;
            assert!(!state.close_bar().ready);
        }
        state.current_close = 101.0;
        let observation = state.close_bar();
        assert!(observation.ready);
        assert!(observation.long_quantile.is_some());
        assert!(observation.short_quantile.is_some());
    }

    #[test]
    fn nq_expires_extremes_and_quantile_samples_after_120_minutes() {
        let mut state = NqState::default();
        state.current_close = 50.0;
        state.close_bar();
        state.current_close = 200.0;
        state.close_bar();
        state.current_close = 100.0;
        for _ in 0..118 {
            state.close_bar();
        }
        let first_expired = state.close_bar();
        assert_eq!(first_expired.long_value, Some(0.0));
        assert_eq!(first_expired.short_value, Some(-0.5));
        let both_expired = state.close_bar();
        assert_eq!(both_expired.long_value, Some(0.0));
        assert_eq!(both_expired.short_value, Some(0.0));
        assert_eq!(state.closes.len(), 120);
        assert_eq!(state.long_window.len(), 120);
        assert_eq!(state.short_window.len(), 120);
        assert!((both_expired.long_quantile.unwrap() - 1.0 / 120.0).abs() < 1e-12);
        assert!((both_expired.short_quantile.unwrap() - 119.5 / 120.0).abs() < 1e-12);
    }

    #[test]
    fn nq_missing_minutes_age_samples_without_counting_toward_readiness() {
        let mut state = NqState::default();
        state.current_close = 100.0;
        for _ in 0..60 {
            state.close_bar();
        }
        state.current_close = f64::NAN;
        for _ in 0..61 {
            assert!(!state.close_bar().ready);
        }
        state.current_close = 100.0;
        assert!(!state.close_bar().ready);
        assert_eq!(state.long_window.len(), 59);
        assert_eq!(state.short_window.len(), 59);
    }

    #[test]
    fn history_export_keeps_missing_values_and_execution_context() {
        let root = std::env::temp_dir().join(format!("cta-history-{}", uuid::Uuid::new_v4()));
        let mut writer = FactorHistoryWriter::new(&root, BarInterval::OneMinute).unwrap();
        let mut state = NqState::default();
        state.current_close = 100.0;
        let mut nq = state.close_bar();
        nq.execution_ts_ms = 1_791_590_400_003;
        nq.execution_bid = Some(99.0);
        nq.execution_ask = Some(101.0);
        writer
            .record(
                "ETHUSDT",
                1_791_590_400_000,
                &["baseline_004".into()],
                &[FactorObservation {
                    score: f64::NAN,
                    quantile: None,
                    ready: false,
                }],
                &nq,
            )
            .unwrap();
        let record: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(root.join("20261010.jsonl")).unwrap())
                .unwrap();
        assert!(record["inputs"]["baseline_004"]["score"].is_null());
        assert_eq!(record["nq"]["execution_bid"], 99.0);
        assert_eq!(record["nq"]["execution_ts_ms"], 1_791_590_400_003_i64);
        drop(writer);
        fs::remove_dir_all(root).unwrap();
    }
}
