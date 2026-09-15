use anyhow::{bail, Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mkt_parsers::msg::mkt_msg::{ModelMsg, MODEL_STATUS_OK};
use mkt_parsers::msg::trade_flow_feature_msg::{
    TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_HISTORY_SIZE, TRADE_FLOW_FEATURE_MAX_BYTES,
};
use order_common::TradingVenue;
use runtime_common::symbol_util::normalize_symbol_for_venue;

use crate::common::msg_parser::parse_trade_flow_feature;
use crate::common::sliding_quantile::SlidingQuantileWindow;
use crate::factor_pub::fusion_factor_pub::app::{
    load_online_symbols_from_tlen_server, BaselineReplayState,
};
use crate::factor_pub::fusion_factor_pub::cfg::TlenServerConfig;
use crate::factor_pub::fusion_factor_pub::plan::load_symbol_factor_plans_from_tlen_server;
use crate::factor_pub::fusion_factor_pub::SymbolFactorPlan;
use crate::factor_pub::model_output_publisher::ModelPublisher;

use super::cfg::IntraFactorModelPubConfig;

const IDLE_SLEEP_MICROS: u64 = 200;
const STATS_LOG_INTERVAL_SECS: u64 = 60;
const SYMBOL_RELOAD_WARN_INTERVAL_SECS: u64 = 60;
const TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE: usize = 8192;
const TRADE_FLOW_MAX_SUBSCRIBERS: usize = 10;
const FACTOR_PLAN_CONFIG_TYPE: &str = "factor_plan_1m";
const AMOUNT_THRESHOLD_CONFIG_TYPE: &str = "amount_thresholds_1m";

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
}

impl SymbolState {
    fn new(window_size: usize) -> Self {
        Self {
            evaluator: BaselineReplayState::default(),
            windows: (0..INTRA_FACTOR_NAMES.len())
                .map(|_| SlidingQuantileWindow::new(window_size, window_size))
                .collect(),
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
    trade_flow_subscriber: Subscriber<ipc::Service, [u8; TRADE_FLOW_FEATURE_MAX_BYTES], ()>,
    outputs: Vec<FactorOutput>,
    allowed_symbols: HashSet<String>,
    states: HashMap<String, SymbolState>,
    last_symbol_reload: Instant,
    symbol_reload_interval: Duration,
    last_symbol_reload_warn: Instant,
    last_stats_log: Instant,
    stats: IntraFactorModelStats,
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

        let output_services: Vec<&str> = outputs
            .iter()
            .map(|output| output.service_path.as_str())
            .collect();
        info!(
            "IntraFactorModel1mPubApp started: venue={} input=factor_pub/{}/trade_flow_feature_1m symbols={} sample={} percentile_window={} min_samples={} output_services={:?}",
            venue_slug,
            venue_slug,
            allowed_symbols.len(),
            format_symbol_sample(&allowed_symbols),
            config.percentile.window_size,
            config.percentile.min_samples,
            output_services,
        );

        Ok(Self {
            venue,
            venue_slug,
            tlen_server: config.tlen_server.clone(),
            window_size: config.percentile.window_size,
            min_samples: config.percentile.min_samples,
            plan,
            trade_flow_subscriber,
            outputs,
            allowed_symbols,
            states: HashMap::new(),
            last_symbol_reload: Instant::now(),
            symbol_reload_interval: Duration::from_secs(config.tlen_server.symbol_reload_secs),
            last_symbol_reload_warn: Instant::now()
                - Duration::from_secs(SYMBOL_RELOAD_WARN_INTERVAL_SECS),
            last_stats_log: Instant::now(),
            stats: IntraFactorModelStats::default(),
        })
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
        let mut drained = 0u64;
        while self.trade_flow_subscriber.receive()?.is_some() {
            drained = drained.saturating_add(1);
        }
        info!(
            "IntraFactorModel1mPubApp ready: venue={} symbols={} drained_stale={} window={} min_samples={}",
            self.venue_slug,
            self.allowed_symbols.len(),
            drained,
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
        while let Some(sample) = self.trade_flow_subscriber.receive()? {
            has_message = true;
            self.stats.raw_messages = self.stats.raw_messages.saturating_add(1);
            let msg = match parse_trade_flow_feature(sample.payload()) {
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
        let observations = {
            let state = self
                .states
                .entry(symbol.clone())
                .or_insert_with(|| SymbolState::new(self.window_size));
            if let Err(err) = state.evaluator.push(msg) {
                self.stats.evaluation_errors = self.stats.evaluation_errors.saturating_add(1);
                warn!(
                    "intra factor 1m raw evaluation failed: venue={} symbol={} err={:#}",
                    self.venue_slug, symbol, err
                );
                return;
            }
            let raw_values = state.evaluator.factor_values(&self.plan);
            state.observe(raw_values, self.min_samples)
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
        output_service_path, plan_has_required_factors, select_enabled_symbols, FactorObservation,
        SymbolState, INTRA_FACTOR_NAMES,
    };
    use crate::factor_pub::fusion_factor_pub::SymbolFactorPlan;
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
}
