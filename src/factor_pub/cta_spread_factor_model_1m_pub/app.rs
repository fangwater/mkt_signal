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
    BaselineBar, LocalBaselineAggregator,
};

use super::cfg::{CtaSpreadFactorPubConfig, KafkaInputConfig, NormalizeConfig};

const FACTOR_WINDOW: usize = 2_880;
const FACTOR_MIN_SAMPLES: usize = 1_440;
const BAR_MS: i64 = 60_000;
const PENDING_JOIN_RETENTION_MS: i64 = 4 * 24 * 60 * 60 * 1_000;
const TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE: &str = "amount_thresholds";
const STATS_LOG_INTERVAL_SECS: u64 = 60;

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

#[derive(Debug, Clone, Copy, serde::Serialize)]
struct SpreadObservation {
    value: Option<f64>,
    quantile: Option<f64>,
    ready: bool,
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
}
impl Default for SpreadState {
    fn default() -> Self {
        Self {
            books: [MinuteBook::default(), MinuteBook::default()],
            watermarks: [0; 2],
            pending: Default::default(),
            window: ExactRollingWindow::new(FACTOR_WINDOW),
            last_ts: 0,
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
            joined.push((
                ts,
                SpreadObservation {
                    value,
                    quantile,
                    ready: value.is_some()
                        && quantile.is_some()
                        && self.window.len() >= FACTOR_MIN_SAMPLES,
                },
            ));
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
    tlen_server: TlenServerConfig,
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
    publish_enabled: bool,
    symbol_reload_interval: Duration,
    last_symbol_reload: Instant,
    last_pending_prune: Instant,
    last_stats_log: Instant,
    stats: Stats,
    history: FactorHistoryWriter,
}

/// The same causal factor/spread observations are saved during warm-up and live
/// operation. Reweighting history never requires an execution publisher.
struct FactorHistoryWriter {
    root: PathBuf,
    current: Option<(String, BufWriter<File>)>,
}

impl FactorHistoryWriter {
    fn new(root: &Path) -> Result<Self> {
        fs::create_dir_all(root).context("create CTA factor history directory")?;
        Ok(Self {
            root: root.to_path_buf(),
            current: None,
        })
    }

    fn record(
        &mut self,
        symbol: &str,
        ts: i64,
        names: &[String],
        factors: &[FactorObservation],
        spread: &SpreadObservation,
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
                "symbol":symbol, "model_ts_ms":ts, "inputs":inputs, "spread":spread,
            }),
        )?;
        writer.write_all(b"\n")?;
        writer.flush().context("flush CTA factor history")
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
        let allowed_symbols =
            load_enabled_symbols(&config.tlen_server, venue, &venue_slug, &config.universe).await?;
        config.universe.require_complete(&allowed_symbols, venue)?;
        let amount_thresholds = load_amount_thresholds_from_tlen_server(
            &config.tlen_server,
            venue,
            &venue_slug,
            TRADE_FLOW_AMOUNT_THRESHOLD_CONFIG_TYPE,
        )
        .await?;
        for symbol in &allowed_symbols {
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
            .context("create CTA spread factor Kafka consumer")?;
        let mut app = Self {
            venue,
            venue_slug,
            tlen_server: config.tlen_server.clone(),
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
            publish_enabled: false,
            symbol_reload_interval: Duration::from_secs(config.tlen_server.symbol_reload_secs),
            last_symbol_reload: Instant::now(),
            last_pending_prune: Instant::now(),
            last_stats_log: Instant::now(),
            stats: Stats::default(),
            history: FactorHistoryWriter::new(history_path)?,
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
            match create_shared_outputs(&app.venue_slug, &app.universe.factors) {
                Ok(outputs) => {
                    app.outputs = outputs;
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
        info!(
            "CTA spread shared factor publisher ready venue={} symbols={} outputs={:?} zscore={}/{}/clip{} percentile=2880/1440 spread=1440/1440/720",
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
                let record = record.context("read CTA spread factor Kafka record")?;
                self.consume_record(&record.topic, &record.payload);
            }
            self.flush_unfiltered_factors();
            self.maybe_log_stats();
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
    }

    fn consume_period(&mut self, period: &PeriodMessage, cash: bool) {
        for info in &period.symbol_infos {
            let symbol = normalize_symbol_for_venue(&info.symbol, self.venue);
            if !self.allowed_symbols.contains(&symbol) {
                continue;
            }
            self.consume_spread_books(&symbol, &info.incs, cash);
            if cash {
                continue;
            }
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

    fn consume_bar(&mut self, symbol: &str, bar: BaselineBar) {
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
            .evaluate(msg, &self.plan, ts >= self.history_start_ms)
        {
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
        }
        Ok(())
    }

    async fn maybe_reload_symbols(&mut self) {
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
                self.spread_states
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
                self.aggregators
                    .retain(|symbol, _| self.allowed_symbols.contains(symbol));
            }
            Ok(_) => warn!("CTA spread symbol reload returned empty set; retaining previous"),
            Err(err) => warn!("CTA spread symbol reload failed: {err:#}"),
        }
    }

    fn maybe_log_stats(&mut self) {
        if self.last_stats_log.elapsed() < Duration::from_secs(STATS_LOG_INTERVAL_SECS) {
            return;
        }
        info!(
            "CTA spread factor stats records={} decode_errors={} eval_errors={} published={} publish_failed={} factor_symbols={} spread_symbols={}",
            self.stats.records,
            self.stats.decode_errors,
            self.stats.evaluation_errors,
            self.stats.published,
            self.stats.publish_failed,
            self.factor_states.len(),
            self.spread_states.len()
        );
        self.stats = Stats::default();
        self.last_stats_log = Instant::now();
    }
}

fn create_shared_outputs(venue_slug: &str, factors: &[String]) -> Result<Vec<FactorOutput>> {
    let mut outputs = Vec::with_capacity(factors.len());
    for (factor_index, name) in factors.iter().enumerate() {
        let service = output_service_path(venue_slug, name);
        let node_name = format!("cta_spread_factor_{}_{}", venue_slug, name)
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
            "model_output/cta_spread-binance-futures-1m-tp_vpi_018"
        );
    }

    #[test]
    fn publisher_exposes_configured_factor_services() {
        assert_eq!(
            toml::from_str::<super::CtaSpreadFactorPubConfig>(include_str!(
                "../../../config/cta_spread_factor_model_1m_pub.toml"
            ))
            .expect("repository config")
            .universe
            .factors
            .iter()
            .map(|factor| output_service_path("binance-futures", factor))
            .collect::<Vec<_>>(),
            vec![
                "model_output/cta_spread-binance-futures-1m-factor_036",
                "model_output/cta_spread-binance-futures-1m-factor_051",
                "model_output/cta_spread-binance-futures-1m-factor_170",
                "model_output/cta_spread-binance-futures-1m-factor_004",
                "model_output/cta_spread-binance-futures-1m-td_ti_002",
                "model_output/cta_spread-binance-futures-1m-baseline_004",
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
    fn spread_joins_same_minute_and_missing_cash_does_not_reuse_a_price() {
        let mut state = SpreadState::default();
        assert!(state.on_close(false, 60_000, 101.0).is_empty());
        assert!(state.on_close(false, 120_000, 102.0).is_empty());
        let first = state.on_close(true, 60_000, 100.0);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].1.value, Some(-0.01));
        let second = state.on_close(true, 120_000, f64::NAN);
        assert_eq!(second[0].1.value, None);
        assert_eq!(second[0].1.quantile, None);
        assert!(!second[0].1.ready);
        assert!(state.on_close(true, 60_000, 999.0).is_empty());
    }
    #[test]
    fn spread_needs_a_full_warmup_and_keeps_a_single_rank() {
        let mut state = SpreadState::default();
        for n in 1..=FACTOR_MIN_SAMPLES {
            let ts = n as i64 * BAR_MS;
            state.on_close(false, ts, 100.0);
            let bars = state.on_close(true, ts, 100.0 + n as f64 / 1000.0);
            assert_eq!(bars[0].1.ready, n == FACTOR_MIN_SAMPLES);
            assert!(bars[0].1.quantile.is_some());
        }
    }
}
