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
use crate::factor_pub::cta_factor_universe::CtaFactorUniverse;
use crate::factor_pub::cta_nq::{observe_nq, NqObservation, NQ_QUANTILE_WINDOW};
use crate::factor_pub::fusion_factor_pub::app::{
    load_amount_thresholds_from_tlen_server, BaselineReplayState,
};
use crate::factor_pub::fusion_factor_pub::SymbolFactorPlan;
use crate::factor_pub::model_output_publisher::ModelPublisher;
use crate::factor_pub::trade_flow_feature_pub::local_baseline::{
    BaselineBar, LocalBaselineAggregator,
};

use super::cfg::{CtaSpreadFactorPubConfig, KafkaInputConfig, NormalizeConfig};
use super::volatility::{VolatilityFactors, VolatilityObservation, VolatilityOutputs};

const FACTOR_WINDOW: usize = 2_880;
const FACTOR_MIN_SAMPLES: usize = 1_440;
const BAR_MS: i64 = 60_000;
const PENDING_JOIN_RETENTION_MS: i64 = 4 * 24 * 60 * 60 * 1_000;
const TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE: &str = "amount_thresholds";
const STATS_LOG_INTERVAL_SECS: u64 = 60;

// Reviewed OHLCV/depth formulas do not need trade-size buckets. Unreviewed
// formulas remain unavailable without real size metadata.
fn requires_amount_thresholds(factor: &str) -> bool {
    !matches!(
        factor,
        "TD_MT_013"
            | "TD_MT_016"
            | "TD_MT_022"
            | "TD_PR_008"
            | "TD_PR_011"
            | "TD_PR_013"
            | "TD_PT_003"
            | "TD_PT_025"
            | "TD_PT_027"
            | "TD_TI_024"
            | "TD_TI_026"
            | "TD_TI_027"
            | "TD_VI_010"
            | "TD_VI_020"
            | "TP_VPI_003"
            | "TP_VPI_017"
            | "baseline_005"
            | "baseline_019"
            | "baseline_022"
            | "baseline_026"
            | "baseline_028"
            | "baseline_065"
            | "baseline_068"
            | "baseline_077"
            | "baseline_079"
            | "baseline_081"
            | "baseline_092"
            | "baseline_105"
            | "baseline_117"
            | "baseline_140"
            | "baseline_141"
            | "baseline_148"
            | "baseline_163"
            | "baseline_164"
            | "baseline_195"
            | "baseline_198"
            | "buy_avg_price"
            | "factor_022"
            | "factor_023"
            | "factor_110"
            | "factor_114"
            | "factor_125"
            | "factor_152"
            | "factor_163"
            | "factor_164"
            | "factor_165"
            | "factor_169"
            | "factor_170"
            | "factor_171"
            | "factor_trades_008"
            | "factor_trades_025"
            | "factor_trades_034"
            | "factor_trades_035"
            | "factor_trades_050"
    )
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
    evaluator: BaselineReplayState,
    windows: Vec<ExactRollingWindow>,
    normalize: Vec<NormalizeState>,
    last_ts: Option<i64>,
}

impl SymbolFactorState {
    fn new(normalize: &NormalizeConfig, factor_count: usize) -> Self {
        Self {
            evaluator: BaselineReplayState::default(),
            windows: (0..factor_count)
                .map(|_| ExactRollingWindow::new(FACTOR_WINDOW))
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
        size_metadata_ready: bool,
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
                .zip(plan.factor_names())
                .map(|(((raw, window), normalize), name)| {
                    if !size_metadata_ready && requires_amount_thresholds(name) {
                        normalize.observe(f64::NAN);
                        window.observe_slot(f64::NAN);
                        return FactorObservation {
                            score: f64::NAN,
                            quantile: None,
                            ready: false,
                        };
                    }
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

#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
struct SpreadObservation {
    value: Option<f64>,
    quantile: Option<f64>,
    ready: bool,
    source_ts_ms: Option<i64>,
    #[serde(skip)]
    nq: NqObservation,
}

struct MinuteBook {
    orderbook: OrderBook,
    next_update_id: i64,
    current_start_ms: Option<i64>,
    current_close: f64,
}
impl Default for MinuteBook {
    fn default() -> Self {
        Self {
            orderbook: OrderBook::new(),
            next_update_id: 1,
            current_start_ms: None,
            current_close: f64::NAN,
        }
    }
}
impl MinuteBook {
    fn on_book(&mut self, book: &IncrementOrderBookInfo) -> Vec<(i64, f64)> {
        let timestamp_ms = normalize_timestamp_ms(book.timestamp);
        let target_start = timestamp_ms.div_euclid(BAR_MS).saturating_mul(BAR_MS);
        let mut closed = Vec::new();
        match self.current_start_ms {
            None => self.current_start_ms = Some(target_start),
            Some(current) if target_start < current => return closed,
            Some(mut current) => {
                while current < target_start {
                    closed.push((current.saturating_add(BAR_MS), self.current_close));
                    current = current.saturating_add(BAR_MS);
                    self.current_start_ms = Some(current);
                    self.current_close = f64::NAN;
                }
            }
        }
        let bids: Vec<_> = book.bids.iter().map(|v| (v.price, v.amount)).collect();
        let asks: Vec<_> = book.asks.iter().map(|v| (v.price, v.amount)).collect();
        self.orderbook.apply_update(
            &bids,
            &asks,
            self.next_update_id,
            timestamp_as_micros(book.timestamp),
        );
        self.next_update_id = self.next_update_id.saturating_add(1);
        if !self.orderbook.is_valid() {
            self.orderbook.prune_crossed_by_best_update_id();
        }
        // An invalid book invalidates this minute's close instead of carrying an old BBO.
        self.current_close = match (
            self.orderbook.best_bid_price(),
            self.orderbook.best_ask_price(),
        ) {
            (Some(bid), Some(ask))
                if bid.is_finite() && ask.is_finite() && bid > 0.0 && ask >= bid =>
            {
                (bid + ask) * 0.5
            }
            _ => f64::NAN,
        };
        closed
    }
}

/// Join the two closed-minute clocks before updating one causal spread window.
struct SpreadState {
    books: [MinuteBook; 2],
    watermarks: [i64; 2],
    pending: std::collections::BTreeMap<i64, [Option<f64>; 2]>,
    window: ExactRollingWindow,
    last_ts: i64,
    previous: Option<(i64, SpreadObservation)>,
    nq_closes: VecDeque<f64>,
    nq_long: ExactRollingWindow,
    nq_short: ExactRollingWindow,
}
impl Default for SpreadState {
    fn default() -> Self {
        Self {
            books: [MinuteBook::default(), MinuteBook::default()],
            watermarks: [0; 2],
            pending: Default::default(),
            window: ExactRollingWindow::new(FACTOR_WINDOW),
            last_ts: 0,
            previous: None,
            nq_closes: VecDeque::new(),
            nq_long: ExactRollingWindow::new(NQ_QUANTILE_WINDOW),
            nq_short: ExactRollingWindow::new(NQ_QUANTILE_WINDOW),
        }
    }
}
impl SpreadState {
    fn on_book(
        &mut self,
        cash: bool,
        book: &IncrementOrderBookInfo,
    ) -> Vec<(i64, SpreadObservation)> {
        let mut joined = Vec::new();
        for (ts, close) in self.books[usize::from(cash)].on_book(book) {
            joined.extend(self.on_close(cash, ts, close));
        }
        joined
    }
    fn on_close(&mut self, cash: bool, ts: i64, close: f64) -> Vec<(i64, SpreadObservation)> {
        let side = usize::from(cash);
        if ts <= self.watermarks[side] || ts <= self.last_ts {
            return Vec::new();
        }
        self.watermarks[side] = ts;
        self.pending.entry(ts).or_insert([None; 2])[side] = Some(close);
        let watermark = self.watermarks[0].min(self.watermarks[1]);
        let mut joined = Vec::new();
        while self
            .pending
            .first_key_value()
            .is_some_and(|(ts, _)| *ts <= watermark)
        {
            let Some((ts, [perp, cash])) = self.pending.pop_first() else {
                break;
            };
            let value = cash
                .zip(perp)
                .filter(|(s, f)| s.is_finite() && f.is_finite() && *s > 0.0 && *f > 0.0)
                .map(|(s, f)| (s - f) / s);
            self.window.observe_slot(value.unwrap_or(f64::NAN));
            let quantile = value.and_then(|_| self.window.percentile_rank_last());
            let nq = observe_nq(
                perp.unwrap_or(f64::NAN),
                &mut self.nq_closes,
                &mut self.nq_long,
                &mut self.nq_short,
            );
            let current = SpreadObservation {
                value,
                quantile,
                ready: value.is_some()
                    && quantile.is_some()
                    && self.window.len() >= FACTOR_MIN_SAMPLES,
                source_ts_ms: Some(ts),
                nq: NqObservation::default(),
            };
            // Entry at t uses only the spread observed at t-1 minute. Never
            // carry a previous value across a missing minute.
            let mut lagged = self
                .previous
                .filter(|(old, _)| *old == ts - BAR_MS)
                .map(|(_, observation)| observation)
                .unwrap_or_default();
            lagged.nq = nq;
            joined.push((ts, lagged));
            self.previous = Some((ts, current));
            self.last_ts = ts;
        }
        // A stalled venue must not grow memory indefinitely.
        let cutoff = ts.saturating_sub(PENDING_JOIN_RETENTION_MS);
        self.pending.retain(|ts, _| *ts >= cutoff);
        joined
    }
}

#[derive(Default)]
struct Stats {
    records: u64,
    decode_errors: u64,
    evaluation_errors: u64,
    published: u64,
    publish_failed: u64,
    vol_published: u64,
    vol_publish_failed: u64,
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

pub struct CtaSpreadFactorModel1mPubApp {
    venue: TradingVenue,
    venue_slug: String,
    universe: CtaFactorUniverse,
    plan: SymbolFactorPlan,
    consumer: RawKafkaConsumer,
    poll_timeout_ms: u64,
    compression: PayloadCompressionMode,
    topic: String,
    cash_topic: String,
    history_start_ms: i64,
    amount_thresholds: HashMap<String, AmountThreshold>,
    allowed_symbols: HashSet<String>,
    aggregators: HashMap<String, LocalBaselineAggregator>,
    factor_states: HashMap<String, SymbolFactorState>,
    normalize: NormalizeConfig,
    spread_states: HashMap<String, SpreadState>,
    pending_factors: HashMap<(String, i64), Vec<FactorObservation>>,
    pending_spread: HashMap<(String, i64), SpreadObservation>,
    outputs: Vec<FactorOutput>,
    volatility: VolatilityFactors,
    volatility_outputs: Option<VolatilityOutputs>,
    publish_enabled: bool,
    last_pending_prune: Instant,
    last_stats_log: Instant,
    stats: Stats,
    history: FactorHistoryWriter,
}

/// The same causal factor/spread observations are saved during warm-up and live
/// operation. Reweighting history never requires an execution publisher.
struct FactorHistoryWriter {
    root: PathBuf,
    active: bool,
    current: Option<(String, BufWriter<File>)>,
    generation: String,
    caught_up: bool,
    first: std::collections::BTreeMap<String, i64>,
    latest: std::collections::BTreeMap<String, i64>,
    last_status: Instant,
    missing_amount_thresholds: Vec<String>,
    size_dependent_factors: Vec<String>,
}

impl FactorHistoryWriter {
    fn new(root: &Path, active: bool) -> Result<Self> {
        fs::create_dir_all(root).context("create CTA factor history directory")?;
        let writer = Self {
            root: root.to_path_buf(),
            active,
            current: None,
            generation: uuid::Uuid::new_v4().to_string(),
            caught_up: false,
            first: Default::default(),
            latest: Default::default(),
            last_status: Instant::now(),
            missing_amount_thresholds: Vec::new(),
            size_dependent_factors: Vec::new(),
        };
        writer.write_status(&Stats::default())?;
        Ok(writer)
    }

    fn write_status(&self, stats: &Stats) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        let path = self.root.join("warmup.json");
        let temp = path.with_extension("json.tmp");
        fs::write(
            &temp,
            serde_json::to_vec(&serde_json::json!({
                "generation":self.generation,"caught_up":self.caught_up,
                "first":self.first,"latest":self.latest,"updated_ms":now_millis(),
                "records":stats.records,"published":stats.published,"publish_failed":stats.publish_failed,
                "vol_published":stats.vol_published,"vol_publish_failed":stats.vol_publish_failed,
                "decode_errors":stats.decode_errors,"evaluation_errors":stats.evaluation_errors,
                "missing_amount_thresholds":self.missing_amount_thresholds,
                "size_dependent_factors":self.size_dependent_factors,
            }))?,
        )?;
        fs::rename(temp, path).context("save Spread factor warm-up progress")
    }

    fn record(
        &mut self,
        symbol: &str,
        ts: i64,
        names: &[String],
        factors: &[FactorObservation],
        spread: &SpreadObservation,
    ) -> Result<()> {
        self.first.entry(symbol.to_string()).or_insert(ts);
        self.latest.insert(symbol.to_string(), ts);
        // A replacement may replay alongside the current publisher. Only the
        // owner of the shared outputs may append history or replace its status.
        if !self.active {
            return Ok(());
        }
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
                "generation":self.generation,
                "symbol":symbol, "model_ts_ms":ts, "inputs":inputs, "spread":spread, "nq":spread.nq,
            }),
        )?;
        writer.write_all(b"\n")?;
        writer.flush().context("flush CTA factor history")?;
        Ok(())
    }
}

impl CtaSpreadFactorModel1mPubApp {
    pub async fn new(
        path: &str,
        venue: TradingVenue,
        wait_for_publishers: bool,
        history_path: &Path,
        replay_only: bool,
    ) -> Result<Self> {
        anyhow::ensure!(
            venue == TradingVenue::BinanceFutures,
            "CTA spread factor publisher only supports binance-futures"
        );
        let config = CtaSpreadFactorPubConfig::load(path)?;
        let venue_slug = venue.data_pub_slug().to_string();
        let plan = SymbolFactorPlan::from_factor_names(
            "cta_spread_factor_model_1m",
            config.universe.factors.clone(),
        )?;
        BaselineReplayState::validate_factor_plan(&plan)
            .context("validate CTA spread factor plan")?;
        let allowed_symbols: HashSet<String> = config
            .universe
            .symbols
            .iter()
            .map(|symbol| normalize_symbol_for_venue(symbol, venue))
            .collect();
        let amount_thresholds = load_amount_thresholds_from_tlen_server(
            &config.tlen_server,
            venue,
            &venue_slug,
            TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE,
        )
        .await?;
        let mut missing: Vec<String> = allowed_symbols
            .iter()
            .filter(|symbol| !amount_thresholds.contains_key(*symbol))
            .cloned()
            .collect();
        missing.sort();
        if !missing.is_empty() {
            warn!("CTA Spread trade-size thresholds absent; only size-dependent factors stay unready: {missing:?}");
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
            .context("create CTA spread factor Kafka consumer")?;
        let mut app = Self {
            venue,
            venue_slug,
            universe: config.universe.clone(),
            plan,
            consumer,
            poll_timeout_ms: kafka_config.poll_timeout_ms.max(1),
            compression: kafka_config.payload_compression,
            topic: config.kafka.topic.clone(),
            cash_topic: config.kafka.cash_topic.clone(),
            history_start_ms: startup_ms
                .saturating_sub(config.kafka.lookback_secs.saturating_mul(1_000) as i64),
            amount_thresholds,
            allowed_symbols,
            aggregators: HashMap::new(),
            factor_states: HashMap::new(),
            normalize: config.normalize.clone(),
            spread_states: HashMap::new(),
            pending_factors: HashMap::new(),
            pending_spread: HashMap::new(),
            outputs: Vec::new(),
            volatility: VolatilityFactors::default(),
            volatility_outputs: None,
            publish_enabled: false,
            last_pending_prune: Instant::now(),
            last_stats_log: Instant::now(),
            stats: Stats::default(),
            history: FactorHistoryWriter::new(history_path, !wait_for_publishers)?,
        };
        app.history.missing_amount_thresholds = missing;
        app.history.size_dependent_factors = app
            .universe
            .factors
            .iter()
            .filter(|name| requires_amount_thresholds(name))
            .cloned()
            .collect();
        app.history.write_status(&app.stats)?;
        app.catch_up(&config.kafka)?;
        anyhow::ensure!(
            app.stats.decode_errors == 0 && app.stats.evaluation_errors == 0,
            "CTA Spread Kafka replay had decode/evaluation errors"
        );
        if replay_only {
            app.history.caught_up = true;
            app.history.write_status(&app.stats)?;
            info!("CTA history replay complete; no IPC publishers created");
            return Ok(app);
        }
        if wait_for_publishers {
            info!(
                "CTA spread standby replay complete records={} latest_ms={} vol_symbols={:?}; existing history preserved until publisher takeover",
                app.stats.records,
                app.history.latest.values().copied().max().unwrap_or(0),
                app.volatility.symbol_counts(),
            );
        }
        loop {
            let outputs = create_shared_outputs(&app.venue_slug, &app.universe.factors)
                .and_then(|outputs| Ok((outputs, VolatilityOutputs::new()?)));
            match outputs {
                Ok((outputs, volatility_outputs)) => {
                    app.outputs = outputs;
                    app.volatility_outputs = Some(volatility_outputs);
                    break;
                }
                Err(err) if wait_for_publishers => {
                    warn!(
                        "CTA spread factor catch-up complete; waiting for shared publisher ownership: {err:#}"
                    );
                    if let Some(record) = app.consumer.poll(app.poll_timeout_ms) {
                        let record = record.context(
                            "read CTA spread factor Kafka record while waiting for publishers",
                        )?;
                        app.consume_record(&record.topic, &record.payload);
                    }
                }
                Err(err) => return Err(err),
            }
        }
        app.publish_enabled = true;
        app.history.active = true;
        app.publish_volatility(&app.volatility.latest());
        app.history.caught_up = true;
        app.history.write_status(&app.stats)?;
        info!(
            "CTA spread shared factor publisher ready venue={} symbols={} outputs={:?} zscore={}/{}/clip{} percentile=2880/1440 spread=2880/1440/lag1m nq=120/120/60 vol=5s/24/720/unscaled/cash+futures vol_symbols={:?}",
            app.venue_slug,
            app.allowed_symbols.len(),
            app.outputs.iter().map(|output| output.service.as_str()).collect::<Vec<_>>(),
            app.normalize.window_bars,
            app.normalize.min_periods,
            app.normalize.clip_zscore,
            app.volatility.symbol_counts(),
        );
        Ok(app)
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            if let Some(record) = self.consumer.poll(self.poll_timeout_ms) {
                let record = record.context("read CTA spread factor Kafka record")?;
                self.consume_record(&record.topic, &record.payload);
            }
            self.flush_unfiltered_factors();
            self.maybe_log_stats();
            if self.history.last_status.elapsed() >= Duration::from_secs(1) {
                self.history.write_status(&self.stats)?;
                self.history.last_status = Instant::now();
            }
        }
    }

    fn consume_record(&mut self, topic: &str, payload: &[u8]) {
        self.stats.records = self.stats.records.saturating_add(1);
        if topic != self.topic && topic != self.cash_topic {
            return;
        }
        let period = match decode_period_payload(payload, self.compression) {
            Ok((_, _, period)) => period,
            Err(err) => {
                self.stats.decode_errors = self.stats.decode_errors.saturating_add(1);
                warn!("CTA spread PeriodMessage decode failed: {err:#}");
                return;
            }
        };
        self.consume_period(&period, topic == self.cash_topic);
        self.maybe_prune_pending_joins();
        self.maybe_log_stats();
    }

    fn consume_period(&mut self, period: &PeriodMessage, cash: bool) {
        let volatility = self.volatility.consume_period(period, cash);
        self.publish_volatility(&volatility);
        for info in &period.symbol_infos {
            let symbol = normalize_symbol_for_venue(&info.symbol, self.venue);
            if !self.allowed_symbols.contains(&symbol) {
                continue;
            }
            self.consume_spread_books(&symbol, &info.incs, cash);
            if cash {
                continue;
            }
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

    fn publish_volatility(&mut self, observations: &[VolatilityObservation]) {
        if !self.publish_enabled {
            return;
        }
        if let Some(outputs) = &mut self.volatility_outputs {
            let (published, failed) = outputs.publish(observations, now_millis());
            self.stats.vol_published = self.stats.vol_published.saturating_add(published);
            self.stats.vol_publish_failed = self.stats.vol_publish_failed.saturating_add(failed);
        }
    }

    fn consume_spread_books(&mut self, symbol: &str, books: &[IncrementOrderBookInfo], cash: bool) {
        let mut books: Vec<&IncrementOrderBookInfo> = books.iter().collect();
        books.sort_by_key(|book| normalize_timestamp_ms(book.timestamp));
        let mut completed = Vec::new();
        let state = self.spread_states.entry(symbol.to_string()).or_default();
        for book in books {
            completed.extend(state.on_book(cash, book));
        }
        for (ts, observation) in completed {
            if ts >= self.history_start_ms {
                self.pending_spread
                    .insert((symbol.to_string(), ts), observation);
                self.publish_if_complete(symbol, ts);
            }
        }
    }

    fn consume_bar(&mut self, symbol: &str, mut bar: BaselineBar) {
        let size_metadata_ready = self.amount_thresholds.contains_key(symbol);
        if !size_metadata_ready {
            bar.large_order = f64::NAN;
            bar.medium_order = f64::NAN;
            bar.small_order = f64::NAN;
            bar.large_buy = f64::NAN;
            bar.large_sell = f64::NAN;
            bar.medium_buy = f64::NAN;
            bar.medium_sell = f64::NAN;
            bar.small_buy = f64::NAN;
            bar.small_sell = f64::NAN;
            bar.net_buy_large = f64::NAN;
            bar.net_buy_medium = f64::NAN;
            bar.net_buy_small = f64::NAN;
        }
        let payload = match bar.to_trade_flow_feature_payload(symbol, self.venue.to_u8()) {
            Ok(value) => value,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!("CTA spread bar encode failed symbol={symbol}: {err:#}");
                return;
            }
        };
        let msg = match TradeFlowFeatureMsg::from_bytes(payload.as_ref()) {
            Ok(value) => value,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!("CTA spread feature decode failed symbol={symbol}: {err:#}");
                return;
            }
        };
        let ts = msg.ts;
        let observations = match self
            .factor_states
            .entry(symbol.to_string())
            .or_insert_with(|| {
                SymbolFactorState::new(&self.normalize, self.plan.factor_names().len())
            })
            .evaluate(
                msg,
                &self.plan,
                ts >= self.history_start_ms,
                size_metadata_ready,
            ) {
            Ok(Some(value)) => value,
            Ok(None) => return,
            Err(err) => {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!("CTA spread factor evaluation failed symbol={symbol}: {err:#}");
                return;
            }
        };
        self.pending_factors
            .insert((symbol.to_string(), ts), observations);
        self.publish_if_complete(symbol, ts);
    }

    fn publish_if_complete(&mut self, symbol: &str, ts: i64) {
        let key = (symbol.to_string(), ts);
        if !(self.pending_factors.contains_key(&key) && self.pending_spread.contains_key(&key)) {
            return;
        }
        let factors = self.pending_factors.remove(&key).expect("checked above");
        let spread = self.pending_spread.remove(&key).expect("checked above");
        if let Err(err) = self
            .history
            .record(symbol, ts, &self.universe.factors, &factors, &spread)
        {
            self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
            warn!("CTA factor history write failed symbol={symbol} ts={ts}: {err:#}");
        }
        if !self.publish_enabled {
            return;
        }
        for output in &mut self.outputs {
            if output.name == "nq" {
                output.seq_no = output.seq_no.saturating_add(1);
                let nq = spread.nq;
                let msg = ModelMsg::create(
                    symbol.to_string(),
                    ts,
                    now_millis(),
                    output.seq_no,
                    nq.long_value.unwrap_or(f64::NAN),
                    nq.long_quantile,
                    nq.ready,
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
                    .map(|p| output.publisher.publish(&p))
                    .unwrap_or(false);
                if published {
                    self.stats.published += 1;
                } else {
                    self.stats.publish_failed += 1;
                }
                continue;
            }
            let Some(factor) = factors.get(output.factor_index) else {
                self.stats.publish_failed = self.stats.publish_failed.saturating_add(1);
                warn!(
                    "CTA spread factor output index missing factor={} index={} observations={}",
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
                spread.value,
                None,
                spread.value,
                None,
                spread.ready,
            )
            .with_filter_quantiles(spread.quantile, spread.quantile);
            let published = msg
                .to_bytes()
                .map(|payload| output.publisher.publish(&payload))
                .unwrap_or(false);
            if published {
                self.stats.published = self.stats.published.saturating_add(1);
            } else {
                self.stats.publish_failed = self.stats.publish_failed.saturating_add(1);
                warn!(
                    "CTA spread model publish failed symbol={} factor={} service={}",
                    symbol, output.name, output.service
                );
            }
        }
    }

    fn flush_unfiltered_factors(&mut self) {
        if !self.publish_enabled {
            return;
        }
        let cutoff = now_millis().saturating_sub(5_000);
        let mut missing: Vec<_> = self
            .pending_factors
            .keys()
            .filter(|(_, ts)| *ts <= cutoff)
            .cloned()
            .collect();
        missing.sort_by_key(|(symbol, ts)| (*ts, symbol.clone()));
        for (symbol, ts) in missing {
            self.pending_spread
                .entry((symbol.clone(), ts))
                .or_insert(SpreadObservation {
                    value: None,
                    quantile: None,
                    ready: false,
                    ..Default::default()
                });
            self.publish_if_complete(&symbol, ts);
        }
    }

    fn maybe_prune_pending_joins(&mut self) {
        if self.last_pending_prune.elapsed() < Duration::from_secs(STATS_LOG_INTERVAL_SECS) {
            return;
        }
        self.last_pending_prune = Instant::now();
        let cutoff = now_millis().saturating_sub(PENDING_JOIN_RETENTION_MS);
        let factors_before = self.pending_factors.len();
        let spread_before = self.pending_spread.len();
        self.pending_factors.retain(|(_, ts), _| *ts >= cutoff);
        self.pending_spread.retain(|(_, ts), _| *ts >= cutoff);
        let dropped_factors = factors_before.saturating_sub(self.pending_factors.len());
        let dropped_spread = spread_before.saturating_sub(self.pending_spread.len());
        if dropped_factors > 0 || dropped_spread > 0 {
            warn!(
                "CTA spread pruned stale unmatched joins factor={} spread={} cutoff_ms={}",
                dropped_factors, dropped_spread, cutoff
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
            bail!("CTA spread Kafka topic has no retained records");
        }
        let deadline = Instant::now() + Duration::from_secs(config.catchup_timeout_secs);
        let mut reached = HashSet::new();
        while reached.len() < targets.len() {
            if Instant::now() >= deadline {
                bail!(
                    "CTA spread Kafka catch-up timed out reached={}/{}",
                    reached.len(),
                    targets.len()
                );
            }
            let Some(record) = self.consumer.poll(self.poll_timeout_ms) else {
                continue;
            };
            let record = record.context("read CTA spread catch-up record")?;
            let key = (record.topic.clone(), record.partition);
            if targets
                .get(&key)
                .is_some_and(|target| record.offset >= *target)
            {
                reached.insert(key);
            }
            self.consume_record(&record.topic, &record.payload);
            if self.history.last_status.elapsed() >= Duration::from_secs(1) {
                self.history.write_status(&self.stats)?;
                self.history.last_status = Instant::now();
            }
        }
        Ok(())
    }

    fn maybe_log_stats(&mut self) {
        if self.last_stats_log.elapsed() < Duration::from_secs(STATS_LOG_INTERVAL_SECS) {
            return;
        }
        let ready = self.volatility.ready_counts(now_millis(), 15_000);
        info!(
            "CTA spread factor stats records={} decode_errors={} eval_errors={} published={} publish_failed={} factor_symbols={} spread_symbols={} vol_published={} vol_publish_failed={} latest_ms={} vol_futures_ready={}/{} vol_cash_ready={}/{}",
            self.stats.records,
            self.stats.decode_errors,
            self.stats.evaluation_errors,
            self.stats.published,
            self.stats.publish_failed,
            self.factor_states.len(),
            self.spread_states.len(),
            self.stats.vol_published,
            self.stats.vol_publish_failed,
            self.history.latest.values().copied().max().unwrap_or(0),
            ready[0],
            self.volatility.symbol_counts()[0],
            ready[1],
            self.volatility.symbol_counts()[1],
        );
        self.last_stats_log = Instant::now();
    }
}

fn create_shared_outputs(venue_slug: &str, factors: &[String]) -> Result<Vec<FactorOutput>> {
    let mut outputs = Vec::with_capacity(factors.len());
    // NQ arrives before the factors that consume it at the same closed minute.
    for (factor_index, name) in std::iter::once((usize::MAX, "nq"))
        .chain(factors.iter().enumerate().map(|(i, f)| (i, f.as_str())))
    {
        let service = output_service_path(venue_slug, name);
        let node_name = format!("cta_spread_factor_{}_{}", venue_slug, name)
            .replace('-', "_")
            .to_ascii_lowercase();
        outputs.push(FactorOutput {
            name: name.to_string(),
            factor_index,
            publisher: ModelPublisher::new(&node_name, &service)?,
            service,
            seq_no: 0,
        });
    }
    Ok(outputs)
}

pub fn output_service_path(venue_slug: &str, factor: &str) -> String {
    format!(
        "model_output/cta_spread-{}-1m-{}",
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

pub(super) fn timestamp_as_micros(value: i64) -> i64 {
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

    fn feature(bar: i64, missing_buckets: bool) -> TradeFlowFeatureMsg {
        use mkt_parsers::msg::trade_flow_feature_msg::{
            TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
        };
        let mid = 100.0 + bar as f64 * 0.01 + (bar as f64 * 0.1).sin();
        let mut values = vec![1.0; TRADE_FLOW_FEATURE_DIM];
        for (index, name) in TRADE_FLOW_FEATURE_FIELD_NAMES.iter().enumerate() {
            values[index] = match *name {
                "open" => mid - 0.1,
                "high" => mid + 1.0,
                "low" => mid - 1.0,
                "close" | "vwap" | "buy_vwap" | "sell_vwap" => mid,
                "volume" => 100.0 + (bar % 13) as f64,
                "amount" => mid * (100.0 + (bar % 13) as f64),
                name if name.contains("large")
                    || name.contains("medium")
                    || name.contains("small") =>
                {
                    if missing_buckets {
                        f64::NAN
                    } else {
                        (bar + 1) as f64 * (index + 1) as f64
                    }
                }
                _ => 1.0,
            };
        }
        for side in [-1.0, 1.0] {
            for level in 0..20 {
                values.extend([
                    mid + side * (level as f64 + 1.0) * 0.01,
                    10.0 + (bar % 7) as f64,
                ]);
            }
        }
        TradeFlowFeatureMsg::from_indexed_values(
            "ETHUSDT".into(),
            TradingVenue::BinanceFutures.to_u8(),
            1_704_067_200_000 + bar * BAR_MS,
            &values,
        )
        .unwrap()
    }

    #[test]
    fn reviewed_factors_do_not_depend_on_missing_size_buckets() {
        let config =
            CtaSpreadFactorPubConfig::load("config/cta_spread_factor_model_1m_pub.toml").unwrap();
        let names: Vec<_> = config
            .universe
            .factors
            .into_iter()
            .filter(|f| !requires_amount_thresholds(f))
            .collect();
        assert_eq!(names.len(), 54);
        assert!(requires_amount_thresholds("baseline_046"));
        assert!(requires_amount_thresholds("net_buy_small"));
        assert!(requires_amount_thresholds("unreviewed"));
        let plan = SymbolFactorPlan::from_factor_names("size independent", names.clone()).unwrap();
        let (mut classified, mut missing) = (
            BaselineReplayState::default(),
            BaselineReplayState::default(),
        );
        let mut seen = vec![false; names.len()];
        for minute in 0..700 {
            classified.push(feature(minute, false)).unwrap();
            missing.push(feature(minute, true)).unwrap();
            for (index, (a, b)) in classified
                .factor_values(&plan)
                .into_iter()
                .zip(missing.factor_values(&plan))
                .enumerate()
            {
                assert!(
                    a == b || a.is_nan() && b.is_nan(),
                    "{}: {a} != {b}",
                    names[index]
                );
                seen[index] |= a.is_finite();
            }
        }
        for (name, seen) in names.iter().zip(seen) {
            assert!(seen, "{name} never exercised after warm-up");
        }
    }

    #[test]
    fn missing_metadata_blocks_size_factors_even_after_previous_readiness() {
        let names = vec![
            "baseline_005".into(),
            "baseline_046".into(),
            "net_buy_small".into(),
        ];
        let plan = SymbolFactorPlan::from_factor_names("missing size metadata", names).unwrap();
        let mut state = SymbolFactorState::new(&normalize_config(2, 2), 3);
        for minute in 0..1600 {
            let observations = state
                .evaluate(feature(minute, false), &plan, true, true)
                .unwrap()
                .unwrap();
            if minute == 1599 {
                assert!(observations.iter().all(|o| o.ready), "{observations:?}");
            }
        }
        let observations = state
            .evaluate(feature(1600, true), &plan, true, false)
            .unwrap()
            .unwrap();
        assert!(observations[0].ready);
        for observation in &observations[1..] {
            assert!(!observation.ready);
            assert!(observation.score.is_nan() && observation.quantile.is_none());
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
    fn output_contract_is_isolated_from_intra() {
        assert_eq!(
            output_service_path("binance-futures", "TP_VPI_018"),
            "model_output/cta_spread-binance-futures-1m-tp_vpi_018"
        );
    }

    #[test]
    fn publisher_exposes_configured_factor_services() {
        let config: CtaSpreadFactorPubConfig = toml::from_str(include_str!(
            "../../../config/cta_spread_factor_model_1m_pub.toml"
        ))
        .unwrap();
        config.universe.validate().unwrap();
        assert_eq!(config.universe.symbols.len(), 48);
        assert_eq!(config.universe.factors.len(), 56);
        assert!(config
            .universe
            .factors
            .contains(&"factor_trades_034".to_string()));
        assert!(config
            .universe
            .factors
            .contains(&"net_buy_small".to_string()));
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
    fn spread_joins_same_minute_and_missing_cash_does_not_reuse_a_price() {
        let mut state = SpreadState::default();
        assert!(state.on_close(false, 60_000, 101.0).is_empty());
        assert!(state.on_close(false, 120_000, 102.0).is_empty());
        let first = state.on_close(true, 60_000, 100.0);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].1.value, None);
        let second = state.on_close(true, 120_000, f64::NAN);
        assert_eq!(second[0].1.value, Some(-0.01));
        assert_eq!(second[0].1.source_ts_ms, Some(60_000));
        state.on_close(false, 180_000, 102.0);
        let third = state.on_close(true, 180_000, 100.0);
        assert_eq!(third[0].1.value, None);
        assert_eq!(third[0].1.quantile, None);
        assert!(!third[0].1.ready);
        assert!(state.on_close(true, 60_000, 999.0).is_empty());
    }
    #[test]
    fn spread_needs_a_full_warmup_and_keeps_a_single_rank() {
        let mut state = SpreadState::default();
        for n in 1..=FACTOR_MIN_SAMPLES + 1 {
            let ts = n as i64 * BAR_MS;
            state.on_close(false, ts, 100.0);
            let bars = state.on_close(true, ts, 100.0 + n as f64 / 1000.0);
            assert_eq!(bars[0].1.ready, n > FACTOR_MIN_SAMPLES);
            assert_eq!(bars[0].1.quantile.is_some(), n > 1);
            assert_eq!(bars[0].1.nq.ready, n >= 60);
            if n > 1 {
                assert_eq!(bars[0].1.source_ts_ms, Some(ts - BAR_MS));
            }
        }
    }

    #[test]
    fn warmup_status_tracks_only_the_current_publisher_run() {
        let root = std::env::temp_dir().join(format!("spread-warmup-{}", uuid::Uuid::new_v4()));
        let mut writer = FactorHistoryWriter::new(&root, true).unwrap();
        let status = || -> serde_json::Value {
            serde_json::from_slice(&fs::read(root.join("warmup.json")).unwrap()).unwrap()
        };
        assert_eq!(status()["caught_up"], false);
        let generation = writer.generation.clone();
        writer
            .record(
                "ETHUSDT",
                1_791_590_400_000,
                &["factor_169".into()],
                &[FactorObservation {
                    score: 1.0,
                    quantile: Some(0.95),
                    ready: true,
                }],
                &SpreadObservation {
                    quantile: Some(0.4),
                    ready: true,
                    ..Default::default()
                },
            )
            .unwrap();
        writer.caught_up = true;
        writer
            .write_status(&Stats {
                records: 123,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(status()["generation"], generation);
        assert_eq!(status()["caught_up"], true);
        assert_eq!(status()["records"], 123);
        assert_eq!(status()["first"]["ETHUSDT"], 1_791_590_400_000_i64);
        assert_eq!(status()["latest"]["ETHUSDT"], 1_791_590_400_000_i64);
        let row: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(root.join("20261010.jsonl")).unwrap())
                .unwrap();
        assert_eq!(row["generation"], generation);
        drop(writer);
        let restarted = FactorHistoryWriter::new(&root, true).unwrap();
        assert_ne!(status()["generation"], generation);
        assert_eq!(status()["caught_up"], false);
        assert_eq!(status()["latest"], serde_json::json!({}));
        drop(restarted);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn standby_preserves_live_history_and_status_until_takeover() {
        let root = std::env::temp_dir().join(format!("spread-standby-{}", uuid::Uuid::new_v4()));
        let mut live = FactorHistoryWriter::new(&root, true).unwrap();
        let ts = 1_791_590_400_000;
        let spread = SpreadObservation::default();
        live.record("ETHUSDT", ts, &[], &[], &spread).unwrap();
        live.caught_up = true;
        live.write_status(&Stats::default()).unwrap();
        let path = root.join("20261010.jsonl");
        let history_before = fs::read(&path).unwrap();
        let status_before = fs::read(root.join("warmup.json")).unwrap();

        let mut standby = FactorHistoryWriter::new(&root, false).unwrap();
        standby
            .record("ETHUSDT", ts - BAR_MS, &[], &[], &spread)
            .unwrap();
        standby.record("ETHUSDT", ts, &[], &[], &spread).unwrap();
        standby.write_status(&Stats::default()).unwrap();
        assert_eq!(fs::read(&path).unwrap(), history_before);
        assert_eq!(fs::read(root.join("warmup.json")).unwrap(), status_before);
        assert!(!root.join("20261009.jsonl").exists());

        drop(live);
        standby.active = true;
        standby.caught_up = true;
        standby
            .record("ETHUSDT", ts + BAR_MS, &[], &[], &spread)
            .unwrap();
        standby.write_status(&Stats::default()).unwrap();
        let history_after = fs::read(&path).unwrap();
        assert!(history_after.starts_with(&history_before));
        assert_eq!(String::from_utf8(history_after).unwrap().lines().count(), 2);
        let status: serde_json::Value =
            serde_json::from_slice(&fs::read(root.join("warmup.json")).unwrap()).unwrap();
        assert_eq!(status["generation"], standby.generation);
        assert_eq!(status["caught_up"], true);
        assert_eq!(status["latest"]["ETHUSDT"], ts + BAR_MS);
        drop(standby);
        fs::remove_dir_all(root).unwrap();
    }
}
