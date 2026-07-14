use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::pre_trade::kalman_filter::{LocalLevelKalmanFilter, DEFAULT_LOCAL_LEVEL_KALMAN_Q};
use crate::pre_trade::order_manager::Side;
use mkt_parsers::msg::mkt_msg::{ModelMsg, MODEL_STATUS_OK};
use mkt_parsers::msg::model_ipc::MODEL_PAYLOAD_MAX_BYTES;
use order_common::TradingVenue;
use runtime_common::redis_client::{RedisClient, RedisSettings};
use runtime_common::symbol_util::normalize_symbol_for_internal;

const MODEL_OUTPUT_HISTORY_SIZE: usize = 128;
const MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE: usize = 256;
const MODEL_OUTPUT_POLL_MAX_PER_LOOP: usize = 256;
const MODEL_OUTPUT_STATS_LOG_INTERVAL_SECS: u64 = 60;
const TAKER_DECISION_MODEL_REFRESH_INTERVAL_SECS: u64 = 60;
const MODEL_SCORE_THRESHOLD_KEY_PREFIX: &str = "model_score_rolling_thresholds_";
const DEFAULT_SCORE_ROLLING_MEAN_WINDOW: usize = 3;
const DEFAULT_ZMQ_TOPIC_PREFIX: &str = "model_output/";
const TAKER_DECISION_SCORE_ROLLING_MEAN_WINDOW_KEY: &str =
    "taker_decsion_model_score_rolling_mean_window";
const TAKER_DECISION_NN_MODEL_KALMAN_Q_KEY: &str = "taker_decsion_nn_model_kalman_q";

thread_local! {
    static TAKER_DECISION_MODEL: RefCell<Option<PreTradeTakerDecisionModel>> = const { RefCell::new(None) };
    static TAKER_DECISION_MODEL_TRANSITION: RefCell<Option<TakerDecisionModelTransition>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakerDecisionModelType {
    TreeModel,
    NnModel,
}

impl TakerDecisionModelType {
    fn parse(redis_key: &str, raw: Option<&String>) -> Result<Self> {
        match raw.map(|value| value.trim().to_ascii_lowercase()) {
            None => Ok(Self::TreeModel),
            Some(value) if value.is_empty() || value == "tree_model" => Ok(Self::TreeModel),
            Some(value) if value == "nn_model" => Ok(Self::NnModel),
            Some(value) => anyhow::bail!(
                "Redis hash '{}' taker_decsion_model_type must be tree_model or nn_model: {}",
                redis_key,
                value
            ),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::TreeModel => "tree_model",
            Self::NnModel => "nn_model",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TakerDecisionConfig {
    pub enabled: bool,
    pub enable_cancel: bool,
    pub model_type: TakerDecisionModelType,
    pub service: String,
    pub nn_zmq_ipc: Option<String>,
    pub nn_keep_long_score: Option<f64>,
    pub nn_keep_short_score: Option<f64>,
    pub nn_open_cancel_long_score: Option<f64>,
    pub nn_open_cancel_short_score: Option<f64>,
    pub nn_kalman_q: f64,
    pub score_rolling_mean_window: usize,
    pub keep_long_percentile: f64,
    pub keep_short_percentile: f64,
    pub open_cancel_long_percentile: f64,
    pub open_cancel_short_percentile: f64,
    pub symbol_configs: HashMap<String, TakerDecisionSymbolConfig>,
}

#[derive(Debug, Clone)]
pub struct TakerDecisionSymbolConfig {
    pub keep_long_percentile: f64,
    pub keep_short_percentile: f64,
    pub open_cancel_long_percentile: f64,
    pub open_cancel_short_percentile: f64,
}

#[derive(Debug, Deserialize)]
struct RawTakerDecisionSymbolConfig {
    #[serde(default, alias = "keep_long")]
    keep_long_percentile: Option<f64>,
    #[serde(default, alias = "keep_short")]
    keep_short_percentile: Option<f64>,
    #[serde(default, alias = "open_cancel_long")]
    open_cancel_long_percentile: Option<f64>,
    #[serde(default, alias = "open_cancel_short")]
    open_cancel_short_percentile: Option<f64>,
}

impl TakerDecisionConfig {
    fn symbol_config(&self, symbol: &str) -> TakerDecisionSymbolConfig {
        self.symbol_configs
            .get(symbol)
            .cloned()
            .unwrap_or_else(|| TakerDecisionSymbolConfig {
                keep_long_percentile: self.keep_long_percentile,
                keep_short_percentile: self.keep_short_percentile,
                open_cancel_long_percentile: self.open_cancel_long_percentile,
                open_cancel_short_percentile: self.open_cancel_short_percentile,
            })
    }

    fn nn_fixed_thresholds(&self) -> Option<TakerDecisionSymbolScoreThresholds> {
        let thresholds = TakerDecisionSymbolScoreThresholds {
            keep_long_score_threshold: self.nn_keep_long_score,
            keep_short_score_threshold: self.nn_keep_short_score,
            open_cancel_long_score_threshold: self.nn_open_cancel_long_score,
            open_cancel_short_score_threshold: self.nn_open_cancel_short_score,
        };
        (thresholds.keep_thresholds_present()
            && (!self.enable_cancel || thresholds.cancel_thresholds_present()))
        .then_some(thresholds)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LazyHedgeDecision {
    Hedge,
    KeepLong,
    KeepShort,
}

#[derive(Debug, Clone)]
pub struct LazyHedgeDecisionSnapshot {
    pub decision: LazyHedgeDecision,
    pub ready: bool,
    pub symbol: String,
    pub score: Option<f64>,
    pub percentile: Option<f64>,
    pub update_count: usize,
    pub note: String,
}

#[derive(Debug, Clone)]
pub struct ModelUpdateEvent {
    pub symbol: String,
    pub score: f64,
    pub percentile: Option<f64>,
    pub score_ready: bool,
    pub update_count: usize,
}

#[derive(Debug, Clone)]
pub struct TakerDecisionModelTransition {
    pub previous_service: String,
    pub next_service: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct TakerDecisionOpenGateSnapshot {
    pub allowed: bool,
    pub symbol: String,
    pub score: Option<f64>,
    pub percentile: Option<f64>,
    pub update_count: usize,
    pub note: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakerDecisionOpenCancel {
    CancelLong,
    CancelShort,
    KeepOpen,
}

#[derive(Debug, Clone)]
pub struct TakerDecisionOpenCancelSnapshot {
    pub decision: TakerDecisionOpenCancel,
    pub ready: bool,
    pub symbol: String,
    pub score: Option<f64>,
    pub percentile: Option<f64>,
    pub update_count: usize,
    pub note: String,
}

#[derive(Debug, Clone)]
pub struct TakerDecisionThresholdReloadStats {
    pub output_hash_key: String,
    pub fields: usize,
    pub ready_symbols: usize,
    pub cached_symbols: usize,
    pub invalid_payloads: usize,
}

#[derive(Debug, Clone, Default)]
struct TakerDecisionSymbolScoreThresholds {
    keep_long_score_threshold: Option<f64>,
    keep_short_score_threshold: Option<f64>,
    open_cancel_long_score_threshold: Option<f64>,
    open_cancel_short_score_threshold: Option<f64>,
}

impl TakerDecisionSymbolScoreThresholds {
    fn has_any(&self) -> bool {
        self.keep_long_score_threshold.is_some()
            || self.keep_short_score_threshold.is_some()
            || self.open_cancel_long_score_threshold.is_some()
            || self.open_cancel_short_score_threshold.is_some()
    }

    fn keep_thresholds_present(&self) -> bool {
        self.keep_long_score_threshold.is_some() && self.keep_short_score_threshold.is_some()
    }

    fn cancel_thresholds_present(&self) -> bool {
        self.open_cancel_long_score_threshold.is_some()
            && self.open_cancel_short_score_threshold.is_some()
    }
}

#[derive(Debug, Deserialize)]
struct RawModelScoreThresholdPayload {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    ready: bool,
    #[serde(default)]
    quantiles: Vec<f64>,
    #[serde(default)]
    thresholds: Vec<f64>,
}

struct ModelScoreThresholdPointsLoad {
    output_hash_key: String,
    fields: usize,
    ready_symbols: usize,
    invalid_payloads: usize,
    by_symbol: HashMap<String, HashMap<u16, f64>>,
}

struct SymbolScoreState {
    latest_raw_score: Option<f64>,
    latest_score: Option<f64>,
    nn_filter: LocalLevelKalmanFilter,
    score_window: VecDeque<f64>,
    score_sum: f64,
    latest_percentile: Option<f64>,
    latest_score_ready: bool,
    update_count: usize,
}

impl SymbolScoreState {
    fn new(nn_kalman_q: f64) -> Result<Self> {
        Ok(Self {
            latest_raw_score: None,
            latest_score: None,
            nn_filter: LocalLevelKalmanFilter::new(nn_kalman_q)
                .context("create NN local-level Kalman filter")?,
            score_window: VecDeque::new(),
            score_sum: 0.0,
            latest_percentile: None,
            latest_score_ready: false,
            update_count: 0,
        })
    }

    fn set_nn_kalman_q(&mut self, q: f64) -> Result<()> {
        self.nn_filter
            .set_q(q)
            .context("update NN local-level Kalman filter q")
    }

    fn observe(
        &mut self,
        score: f64,
        score_quantile: Option<f64>,
        score_ready: bool,
        model_type: TakerDecisionModelType,
        rolling_mean_window: usize,
    ) -> Option<f64> {
        self.latest_raw_score = Some(score);
        match model_type {
            TakerDecisionModelType::NnModel => {
                self.latest_score = self.nn_filter.update(score);
            }
            TakerDecisionModelType::TreeModel => {
                let window_len = rolling_mean_window.max(1);
                self.score_window.push_back(score);
                self.score_sum += score;
                while self.score_window.len() > window_len {
                    if let Some(old_score) = self.score_window.pop_front() {
                        self.score_sum -= old_score;
                    }
                }
                self.latest_score = Some(self.score_sum / self.score_window.len() as f64);
            }
        }
        self.latest_percentile = score_quantile.map(|value| value * 100.0);
        self.latest_score_ready = score_ready;
        self.update_count = self.update_count.saturating_add(1);
        self.latest_percentile
    }

    fn latest_observation(&self, model_type: TakerDecisionModelType) -> Option<f64> {
        match model_type {
            TakerDecisionModelType::NnModel => self.latest_score,
            TakerDecisionModelType::TreeModel => {
                self.latest_percentile.map(|percentile| percentile / 100.0)
            }
        }
        .filter(|value| value.is_finite())
    }

    fn score_ready(&self) -> bool {
        self.latest_score_ready
    }

    fn update_count(&self) -> usize {
        self.update_count
    }
}

enum ModelReceiveResult {
    Empty,
    Skipped,
    Message(ModelMsg),
}

enum TakerDecisionSubscriber {
    Iceoryx {
        _node: Node<ipc::Service>,
        subscriber: Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>,
    },
    ZmqIpc {
        _context: zmq::Context,
        subscriber: zmq::Socket,
        topic_prefix: String,
    },
}

impl TakerDecisionSubscriber {
    fn try_receive(&self) -> Result<ModelReceiveResult> {
        match self {
            Self::Iceoryx { subscriber, .. } => match subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = sample.payload();
                    if payload.iter().all(|&byte| byte == 0) {
                        return Ok(ModelReceiveResult::Skipped);
                    }
                    let msg = ModelMsg::from_bytes(payload)
                        .context("decode Iceoryx taker decision ModelMsg failed")?;
                    Ok(ModelReceiveResult::Message(msg))
                }
                Ok(None) => Ok(ModelReceiveResult::Empty),
                Err(err) => Err(err).context("receive Iceoryx taker decision ModelMsg failed"),
            },
            Self::ZmqIpc {
                subscriber,
                topic_prefix,
                ..
            } => {
                let frames = match subscriber.recv_multipart(zmq::DONTWAIT) {
                    Ok(frames) => frames,
                    Err(zmq::Error::EAGAIN) => return Ok(ModelReceiveResult::Empty),
                    Err(err) => {
                        return Err(err).context("receive ZMQ IPC taker decision ModelMsg failed")
                    }
                };
                if frames.len() != 2 {
                    anyhow::bail!(
                        "ZMQ IPC ModelMsg expected 2 frames, received {}",
                        frames.len()
                    );
                }
                let topic = std::str::from_utf8(&frames[0])
                    .context("ZMQ IPC ModelMsg topic is not valid UTF-8")?;
                let msg = ModelMsg::from_bytes(&frames[1])
                    .context("decode ZMQ IPC taker decision ModelMsg failed")?;
                let expected_topic = format!("{}{}", topic_prefix, msg.symbol);
                if topic != expected_topic {
                    anyhow::bail!(
                        "ZMQ IPC ModelMsg topic/symbol mismatch: topic={:?} expected={:?}",
                        topic,
                        expected_topic
                    );
                }
                Ok(ModelReceiveResult::Message(msg))
            }
        }
    }
}

pub struct PreTradeTakerDecisionModel {
    cfg: TakerDecisionConfig,
    service_name: String,
    threshold_output_key: Option<String>,
    subscriber: TakerDecisionSubscriber,
    states: HashMap<String, SymbolScoreState>,
    score_thresholds: HashMap<String, TakerDecisionSymbolScoreThresholds>,
    fixed_score_thresholds: Option<TakerDecisionSymbolScoreThresholds>,
    recv_count: u64,
    parse_err_count: u64,
    last_log: Instant,
}

impl PreTradeTakerDecisionModel {
    pub async fn load_config_from_redis(
        redis: &RedisSettings,
        namespace: Option<&str>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<TakerDecisionConfig> {
        let mut client = RedisClient::connect(redis.clone()).await?;
        let ns = strategy_namespace_from_env(namespace);
        let key = strategy_params_key(&ns, open_venue, hedge_venue);
        let map = client.hgetall_map(&key).await?;
        let enabled = match map.get("enable_taker_decsion_model") {
            Some(raw) => parse_bool(&key, "enable_taker_decsion_model", raw)?,
            None => false,
        };
        let enable_cancel = match map.get("enable_taker_decsion_model_cancel") {
            Some(raw) => parse_bool(&key, "enable_taker_decsion_model_cancel", raw)?,
            None => true,
        };
        let model_type = TakerDecisionModelType::parse(&key, map.get("taker_decsion_model_type"))?;
        let nn_kalman_q = if model_type == TakerDecisionModelType::NnModel {
            match map.get(TAKER_DECISION_NN_MODEL_KALMAN_Q_KEY) {
                Some(raw) => {
                    parse_nonnegative_finite_f64(&key, TAKER_DECISION_NN_MODEL_KALMAN_Q_KEY, raw)?
                }
                None => DEFAULT_LOCAL_LEVEL_KALMAN_Q,
            }
        } else {
            DEFAULT_LOCAL_LEVEL_KALMAN_Q
        };
        let service = map
            .get("taker_decsion_model_service")
            .map(|raw| raw.trim().to_string())
            .unwrap_or_else(|| "-".to_string());
        let (
            nn_zmq_ipc,
            nn_keep_long_score,
            nn_keep_short_score,
            nn_open_cancel_long_score,
            nn_open_cancel_short_score,
        ) = if model_type == TakerDecisionModelType::NnModel {
            (
                map.get("taker_decsion_nn_model_zmq_ipc")
                    .map(|raw| raw.trim().to_string())
                    .filter(|value| !value.is_empty()),
                parse_optional_finite_score(
                    &key,
                    "taker_decsion_nn_model_keep_long_score",
                    map.get("taker_decsion_nn_model_keep_long_score"),
                )?,
                parse_optional_finite_score(
                    &key,
                    "taker_decsion_nn_model_keep_short_score",
                    map.get("taker_decsion_nn_model_keep_short_score"),
                )?,
                parse_optional_finite_score(
                    &key,
                    "taker_decsion_nn_model_open_cancel_long_score",
                    map.get("taker_decsion_nn_model_open_cancel_long_score"),
                )?,
                parse_optional_finite_score(
                    &key,
                    "taker_decsion_nn_model_open_cancel_short_score",
                    map.get("taker_decsion_nn_model_open_cancel_short_score"),
                )?,
            )
        } else {
            (None, None, None, None, None)
        };
        if enabled && model_type == TakerDecisionModelType::NnModel {
            let endpoint = nn_zmq_ipc.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "Redis hash '{}' taker_decsion_nn_model_zmq_ipc is required for nn_model",
                    key
                )
            })?;
            if !endpoint.starts_with("ipc://") {
                anyhow::bail!(
                    "Redis hash '{}' taker_decsion_nn_model_zmq_ipc must start with ipc://: {}",
                    key,
                    endpoint
                );
            }
            let required_scores = [
                ("taker_decsion_nn_model_keep_long_score", nn_keep_long_score),
                (
                    "taker_decsion_nn_model_keep_short_score",
                    nn_keep_short_score,
                ),
            ];
            for (field, value) in required_scores {
                if value.is_none() {
                    anyhow::bail!("Redis hash '{}' {} is required for nn_model", key, field);
                }
            }
            if nn_keep_short_score > nn_keep_long_score {
                anyhow::bail!(
                    "Redis hash '{}' nn keep_short_score must be <= keep_long_score",
                    key
                );
            }
            if enable_cancel {
                let required_cancel_scores = [
                    (
                        "taker_decsion_nn_model_open_cancel_long_score",
                        nn_open_cancel_long_score,
                    ),
                    (
                        "taker_decsion_nn_model_open_cancel_short_score",
                        nn_open_cancel_short_score,
                    ),
                ];
                for (field, value) in required_cancel_scores {
                    if value.is_none() {
                        anyhow::bail!("Redis hash '{}' {} is required for nn_model", key, field);
                    }
                }
                if nn_open_cancel_short_score > nn_open_cancel_long_score {
                    anyhow::bail!(
                        "Redis hash '{}' nn open_cancel_short_score must be <= open_cancel_long_score",
                        key
                    );
                }
            }
        }

        let (
            score_rolling_mean_window,
            keep_long_percentile,
            keep_short_percentile,
            open_cancel_long_percentile,
            open_cancel_short_percentile,
        ) = if model_type == TakerDecisionModelType::TreeModel {
            let score_rolling_mean_window = match map
                .get(TAKER_DECISION_SCORE_ROLLING_MEAN_WINDOW_KEY)
            {
                Some(raw) => {
                    parse_positive_usize(&key, TAKER_DECISION_SCORE_ROLLING_MEAN_WINDOW_KEY, raw)?
                }
                None => DEFAULT_SCORE_ROLLING_MEAN_WINDOW,
            };
            let keep_long_percentile = match map.get("taker_decsion_model_keep_long_percentile") {
                Some(raw) => {
                    parse_percentile(&key, "taker_decsion_model_keep_long_percentile", raw)?
                }
                None => 80.0,
            };
            let keep_short_percentile = match map.get("taker_decsion_model_keep_short_percentile") {
                Some(raw) => {
                    parse_percentile(&key, "taker_decsion_model_keep_short_percentile", raw)?
                }
                None => 20.0,
            };
            let open_cancel_long_percentile = match map
                .get("taker_decsion_model_open_cancel_long_percentile")
            {
                Some(raw) => {
                    parse_percentile(&key, "taker_decsion_model_open_cancel_long_percentile", raw)?
                }
                None => keep_long_percentile,
            };
            let open_cancel_short_percentile =
                match map.get("taker_decsion_model_open_cancel_short_percentile") {
                    Some(raw) => parse_percentile(
                        &key,
                        "taker_decsion_model_open_cancel_short_percentile",
                        raw,
                    )?,
                    None => keep_short_percentile,
                };
            if keep_short_percentile > keep_long_percentile {
                anyhow::bail!(
                    "Redis hash '{}' taker_decsion_model_keep_short_percentile must be <= keep_long: short={} long={}",
                    key,
                    keep_short_percentile,
                    keep_long_percentile
                );
            }
            if open_cancel_short_percentile > open_cancel_long_percentile {
                anyhow::bail!(
                    "Redis hash '{}' taker_decsion_model_open_cancel_short_percentile must be <= open_cancel_long: short={} long={}",
                    key,
                    open_cancel_short_percentile,
                    open_cancel_long_percentile
                );
            }
            (
                score_rolling_mean_window,
                keep_long_percentile,
                keep_short_percentile,
                open_cancel_long_percentile,
                open_cancel_short_percentile,
            )
        } else {
            (1, 80.0, 20.0, 80.0, 20.0)
        };
        let symbol_configs = if enabled && model_type == TakerDecisionModelType::TreeModel {
            match taker_decision_model_overrides_key(namespace, open_venue, hedge_venue) {
                Some(overrides_key) => match client.get_string(&overrides_key).await? {
                    Some(raw) => parse_symbol_configs(
                        &raw,
                        &overrides_key,
                        keep_long_percentile,
                        keep_short_percentile,
                        open_cancel_long_percentile,
                        open_cancel_short_percentile,
                    )?,
                    None => HashMap::new(),
                },
                None => HashMap::new(),
            }
        } else {
            HashMap::new()
        };
        let (force_taker, lazy_taker) = arb_hedge_taker_env_flags()?;
        if enabled {
            if force_taker {
                anyhow::bail!(
                    "Redis hash '{}' enable_taker_decsion_model=true conflicts with ARB_HEDGE_FORCE_TAKER=on",
                    key
                );
            }
            if !lazy_taker {
                anyhow::bail!(
                    "Redis hash '{}' enable_taker_decsion_model=true requires ARB_HEDGE_LAZY_TAKER=on or ARB_HEDGE_lazy_TAKER=on",
                    key
                );
            }
        }
        Ok(TakerDecisionConfig {
            enabled,
            enable_cancel,
            model_type,
            service,
            nn_zmq_ipc,
            nn_keep_long_score,
            nn_keep_short_score,
            nn_open_cancel_long_score,
            nn_open_cancel_short_score,
            nn_kalman_q,
            score_rolling_mean_window,
            keep_long_percentile,
            keep_short_percentile,
            open_cancel_long_percentile,
            open_cancel_short_percentile,
            symbol_configs,
        })
    }

    pub fn initialize(cfg: TakerDecisionConfig) -> Result<bool> {
        let result = Self::apply_config_global(cfg);
        if result.is_err() {
            Self::clear_global("model_initialization_failed");
        }
        result
    }

    fn apply_config_global(cfg: TakerDecisionConfig) -> Result<bool> {
        if !cfg.enabled {
            let cleared = Self::clear_global("model_disabled");
            if cleared {
                info!("pre_trade taker decision model disabled; pending lazy exposure will fallback to direct taker");
            } else {
                debug!("pre_trade taker decision model remains disabled");
            }
            return Ok(false);
        }

        let service_name = Self::connection_name_for_config(&cfg)?;
        let current_service = Self::service_name_global();
        if current_service.as_deref() == Some(service_name.as_str()) {
            let model_type = cfg.model_type;
            let nn_kalman_q = cfg.nn_kalman_q;
            let effective_rolling_mean_window = if model_type == TakerDecisionModelType::NnModel {
                1
            } else {
                cfg.score_rolling_mean_window
            };
            TAKER_DECISION_MODEL.with(|cell| -> Result<()> {
                if let Some(model) = cell.borrow_mut().as_mut() {
                    if model_type == TakerDecisionModelType::NnModel {
                        for state in model.states.values_mut() {
                            state.set_nn_kalman_q(nn_kalman_q)?;
                        }
                    }
                    model.fixed_score_thresholds = cfg.nn_fixed_thresholds();
                    model.cfg = cfg;
                }
                Ok(())
            })?;
            debug!(
                "pre_trade taker decision model config updated without reconnect model_type={} service={} effective_score_rolling_mean_window={} nn_kalman_q={}",
                model_type.as_str(),
                service_name,
                effective_rolling_mean_window,
                nn_kalman_q
            );
            return Ok(true);
        }

        let model = Self::build_model(cfg, service_name.clone())?;
        let model_type = model.cfg.model_type;
        let threshold_output_key = model
            .threshold_output_key
            .as_deref()
            .unwrap_or("-")
            .to_string();
        let effective_rolling_mean_window = if model_type == TakerDecisionModelType::NnModel {
            1
        } else {
            model.cfg.score_rolling_mean_window
        };
        let nn_kalman_q = model.cfg.nn_kalman_q;
        let previous_model = TAKER_DECISION_MODEL.with(|cell| cell.borrow_mut().replace(model));
        let previous_service = previous_model
            .as_ref()
            .map(|model| model.service_name.clone());
        drop(previous_model);
        if let Some(previous_service) = previous_service {
            Self::publish_transition(
                previous_service,
                Some(service_name.clone()),
                "model_service_replaced",
            );
        }
        info!(
            "pre_trade taker decision model connected model_type={} service={} threshold_key={} effective_score_rolling_mean_window={} nn_kalman_q={}",
            model_type.as_str(),
            service_name,
            threshold_output_key,
            effective_rolling_mean_window,
            nn_kalman_q
        );
        Ok(true)
    }

    fn connection_name_for_config(cfg: &TakerDecisionConfig) -> Result<String> {
        match cfg.model_type {
            TakerDecisionModelType::TreeModel => normalize_service_name(&cfg.service)
                .ok_or_else(|| anyhow::anyhow!("taker_decsion_model_service is disabled")),
            TakerDecisionModelType::NnModel => {
                let endpoint = cfg.nn_zmq_ipc.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("taker_decsion_nn_model_zmq_ipc is required for nn_model")
                })?;
                Ok(format!("nn_model:{endpoint}"))
            }
        }
    }

    fn build_model(cfg: TakerDecisionConfig, service_name: String) -> Result<Self> {
        if cfg.model_type == TakerDecisionModelType::NnModel {
            LocalLevelKalmanFilter::new(cfg.nn_kalman_q)
                .context("invalid taker_decsion_nn_model_kalman_q")?;
        }
        let fixed_score_thresholds = cfg.nn_fixed_thresholds();
        let (threshold_output_key, subscriber) = match cfg.model_type {
            TakerDecisionModelType::TreeModel => {
                let model_name = model_name_from_service_name(&service_name).ok_or_else(|| {
                    anyhow::anyhow!(
                        "cannot infer model name from taker_decsion_model_service={service_name}"
                    )
                })?;
                let threshold_output_key = Some(model_score_threshold_output_key(&model_name));
                let node_name = format!(
                    "pre_trade_taker_decision_{}",
                    sanitize_node_suffix(&service_name)
                );
                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;
                let service = node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
                    .max_publishers(1)
                    .max_subscribers(10)
                    .history_size(MODEL_OUTPUT_HISTORY_SIZE)
                    .subscriber_max_buffer_size(MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE)
                    .open_or_create()
                    .with_context(|| {
                        format!("open taker decision model service failed: {service_name}")
                    })?;
                let subscriber = service
                    .subscriber_builder()
                    .buffer_size(MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE)
                    .create()
                    .with_context(|| {
                        format!("create taker decision model subscriber failed: {service_name}")
                    })?;
                (
                    threshold_output_key,
                    TakerDecisionSubscriber::Iceoryx {
                        _node: node,
                        subscriber,
                    },
                )
            }
            TakerDecisionModelType::NnModel => {
                let endpoint = cfg.nn_zmq_ipc.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("taker_decsion_nn_model_zmq_ipc is required for nn_model")
                })?;
                if cfg.nn_fixed_thresholds().is_none() {
                    anyhow::bail!(
                        "nn_model keep thresholds and enabled cancel thresholds are required"
                    );
                }
                let context = zmq::Context::new();
                let subscriber = context
                    .socket(zmq::SUB)
                    .context("create nn_model ZMQ IPC SUB socket failed")?;
                subscriber
                    .set_linger(0)
                    .context("set nn_model ZMQ IPC linger failed")?;
                subscriber
                    .set_rcvhwm(MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE as i32)
                    .context("set nn_model ZMQ IPC receive HWM failed")?;
                subscriber
                    .set_subscribe(DEFAULT_ZMQ_TOPIC_PREFIX.as_bytes())
                    .context("subscribe nn_model ZMQ IPC topic failed")?;
                subscriber
                    .connect(endpoint)
                    .with_context(|| format!("connect nn_model ZMQ IPC failed: {endpoint}"))?;
                (
                    None,
                    TakerDecisionSubscriber::ZmqIpc {
                        _context: context,
                        subscriber,
                        topic_prefix: DEFAULT_ZMQ_TOPIC_PREFIX.to_string(),
                    },
                )
            }
        };

        Ok(Self {
            cfg,
            service_name,
            threshold_output_key,
            subscriber,
            states: HashMap::new(),
            score_thresholds: HashMap::new(),
            fixed_score_thresholds,
            recv_count: 0,
            parse_err_count: 0,
            last_log: Instant::now(),
        })
    }

    fn service_name_global() -> Option<String> {
        TAKER_DECISION_MODEL.with(|cell| {
            cell.borrow()
                .as_ref()
                .map(|model| model.service_name.clone())
        })
    }

    fn publish_transition(previous_service: String, next_service: Option<String>, reason: &str) {
        TAKER_DECISION_MODEL_TRANSITION.with(|cell| {
            *cell.borrow_mut() = Some(TakerDecisionModelTransition {
                previous_service,
                next_service,
                reason: reason.to_string(),
            });
        });
    }

    fn clear_global(reason: &str) -> bool {
        let previous_model = TAKER_DECISION_MODEL.with(|cell| cell.borrow_mut().take());
        let previous_service = previous_model
            .as_ref()
            .map(|model| model.service_name.clone());
        drop(previous_model);
        if let Some(previous_service) = previous_service {
            Self::publish_transition(previous_service, None, reason);
            true
        } else {
            false
        }
    }

    pub fn take_transition_global() -> Option<TakerDecisionModelTransition> {
        TAKER_DECISION_MODEL_TRANSITION.with(|cell| cell.borrow_mut().take())
    }

    fn transition_pending_global() -> bool {
        TAKER_DECISION_MODEL_TRANSITION.with(|cell| cell.borrow().is_some())
    }

    pub async fn reload_config_global(
        redis: &RedisSettings,
        namespace: Option<&str>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Option<TakerDecisionThresholdReloadStats>> {
        let cfg =
            match Self::load_config_from_redis(redis, namespace, open_venue, hedge_venue).await {
                Ok(cfg) => cfg,
                Err(err) => {
                    Self::clear_global("model_config_load_failed");
                    return Err(err);
                }
            };
        if let Err(err) = Self::initialize(cfg) {
            return Err(err);
        }
        Self::reload_thresholds_global(redis).await
    }

    pub async fn reload_thresholds_global(
        redis: &RedisSettings,
    ) -> Result<Option<TakerDecisionThresholdReloadStats>> {
        let Some(output_hash_key) = Self::threshold_output_key_global() else {
            return Ok(None);
        };
        let mut client = RedisClient::connect(redis.clone()).await?;
        let raw = client.hgetall_map(&output_hash_key).await?;
        let load = parse_model_score_thresholds_hash(&output_hash_key, &raw);
        let stats = Self::apply_score_thresholds_global(load);
        Ok(Some(stats))
    }

    pub fn start_config_background_refresh(
        redis: RedisSettings,
        namespace: Option<String>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) {
        tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                TAKER_DECISION_MODEL_REFRESH_INTERVAL_SECS,
            ));
            interval.tick().await;
            loop {
                interval.tick().await;
                match Self::reload_config_global(
                    &redis,
                    namespace.as_deref(),
                    open_venue,
                    hedge_venue,
                )
                .await
                {
                    Ok(Some(stats)) => {
                        debug!(
                            "pre_trade taker decision model config refreshed key={} fields={} ready_symbols={} cached_symbols={} invalid_payloads={}",
                            stats.output_hash_key,
                            stats.fields,
                            stats.ready_symbols,
                            stats.cached_symbols,
                            stats.invalid_payloads
                        );
                    }
                    Ok(None) => {
                        debug!("pre_trade taker decision model config refreshed: model disabled or fixed-threshold mode");
                    }
                    Err(err) => {
                        warn!(
                            "pre_trade taker decision model config refresh failed; fallback to no model: {:#}",
                            err
                        );
                    }
                }
            }
        });
        info!(
            "pre_trade taker decision model config refresh started (interval: {}s)",
            TAKER_DECISION_MODEL_REFRESH_INTERVAL_SECS
        );
    }

    pub fn poll_updates_global() -> Vec<ModelUpdateEvent> {
        Self::poll_updates_global_limit(MODEL_OUTPUT_POLL_MAX_PER_LOOP)
    }

    pub fn poll_updates_global_limit(max_updates: usize) -> Vec<ModelUpdateEvent> {
        if Self::transition_pending_global() {
            return Vec::new();
        }
        TAKER_DECISION_MODEL.with(|cell| {
            let mut guard = cell.borrow_mut();
            let Some(model) = guard.as_mut() else {
                return Vec::new();
            };
            model.poll_updates_limit(max_updates)
        })
    }

    pub fn evaluate_global(symbol: &str, due_hedge_qty: f64) -> Option<LazyHedgeDecisionSnapshot> {
        TAKER_DECISION_MODEL.with(|cell| {
            let guard = cell.borrow();
            let model = guard.as_ref()?;
            model.evaluate(symbol, due_hedge_qty)
        })
    }

    pub fn evaluation_model_name_global() -> Option<String> {
        TAKER_DECISION_MODEL.with(|cell| {
            let guard = cell.borrow();
            let model = guard.as_ref()?;
            let source = match model.cfg.model_type {
                TakerDecisionModelType::TreeModel => model.service_name.as_str(),
                TakerDecisionModelType::NnModel => {
                    let configured = model.cfg.service.trim();
                    if configured.is_empty() || configured == "-" {
                        model.cfg.nn_zmq_ipc.as_deref().unwrap_or("nn_model")
                    } else {
                        configured
                    }
                }
            };
            model_name_from_service_name(source)
                .or_else(|| Some(model.cfg.model_type.as_str().to_string()))
        })
    }

    pub fn nn_score_global(symbol: &str) -> Option<f64> {
        TAKER_DECISION_MODEL.with(|cell| {
            let guard = cell.borrow();
            let model = guard.as_ref()?;
            if model.cfg.model_type != TakerDecisionModelType::NnModel {
                return None;
            }
            model
                .states
                .get(&normalize_symbol_for_internal(symbol))?
                .latest_score
                .filter(|score| score.is_finite())
        })
    }

    pub fn latest_observation_global(symbol: &str) -> Option<f64> {
        TAKER_DECISION_MODEL.with(|cell| {
            let guard = cell.borrow();
            let model = guard.as_ref()?;
            model
                .states
                .get(&normalize_symbol_for_internal(symbol))?
                .latest_observation(model.cfg.model_type)
        })
    }

    pub fn arb_open_gate_global(symbol: &str) -> Option<TakerDecisionOpenGateSnapshot> {
        TAKER_DECISION_MODEL.with(|cell| {
            let guard = cell.borrow();
            let model = guard.as_ref()?;
            model.arb_open_gate(symbol)
        })
    }

    pub fn arb_open_cancel_global(
        symbol: &str,
        open_side: Side,
    ) -> Option<TakerDecisionOpenCancelSnapshot> {
        TAKER_DECISION_MODEL.with(|cell| {
            let guard = cell.borrow();
            let model = guard.as_ref()?;
            if !model.cfg.enable_cancel {
                return None;
            }
            model.arb_open_cancel(symbol, open_side)
        })
    }

    fn threshold_output_key_global() -> Option<String> {
        TAKER_DECISION_MODEL.with(|cell| {
            let guard = cell.borrow();
            guard
                .as_ref()
                .and_then(|model| model.threshold_output_key.clone())
        })
    }

    fn apply_score_thresholds_global(
        load: ModelScoreThresholdPointsLoad,
    ) -> TakerDecisionThresholdReloadStats {
        TAKER_DECISION_MODEL.with(|cell| {
            let mut guard = cell.borrow_mut();
            let Some(model) = guard.as_mut() else {
                return TakerDecisionThresholdReloadStats {
                    output_hash_key: load.output_hash_key,
                    fields: load.fields,
                    ready_symbols: load.ready_symbols,
                    cached_symbols: 0,
                    invalid_payloads: load.invalid_payloads,
                };
            };
            model.apply_score_thresholds(load)
        })
    }

    fn poll_updates_limit(&mut self, max_updates: usize) -> Vec<ModelUpdateEvent> {
        let mut events = Vec::new();
        let mut polled = 0usize;
        while polled < max_updates {
            let msg = match self.subscriber.try_receive() {
                Ok(ModelReceiveResult::Empty) => break,
                Ok(ModelReceiveResult::Skipped) => {
                    polled += 1;
                    continue;
                }
                Ok(ModelReceiveResult::Message(msg)) => {
                    polled += 1;
                    msg
                }
                Err(err) => {
                    self.parse_err_count = self.parse_err_count.saturating_add(1);
                    warn!(
                        "pre_trade taker decision model receive/decode failed service={} err={:#}",
                        self.service_name, err
                    );
                    break;
                }
            };
            if msg.status != MODEL_STATUS_OK || !msg.score.is_finite() {
                continue;
            }
            let symbol = normalize_symbol_for_internal(&msg.symbol);
            if symbol.is_empty() {
                continue;
            }
            if !self.states.contains_key(&symbol) {
                match SymbolScoreState::new(self.cfg.nn_kalman_q) {
                    Ok(state) => {
                        self.states.insert(symbol.clone(), state);
                    }
                    Err(err) => {
                        warn!(
                            "pre_trade taker decision model failed to create symbol filter service={} symbol={} err={:#}",
                            self.service_name, symbol, err
                        );
                        continue;
                    }
                }
            }
            let Some(state) = self.states.get_mut(&symbol) else {
                continue;
            };
            let rolling_mean_window = if self.cfg.model_type == TakerDecisionModelType::NnModel {
                1
            } else {
                self.cfg.score_rolling_mean_window
            };
            let percentile = state.observe(
                msg.score,
                msg.score_quantile,
                msg.score_ready,
                self.cfg.model_type,
                rolling_mean_window,
            );
            let smoothed_score = state.latest_score.unwrap_or(msg.score);
            self.recv_count = self.recv_count.saturating_add(1);
            events.push(ModelUpdateEvent {
                symbol,
                score: smoothed_score,
                percentile,
                score_ready: msg.score_ready,
                update_count: state.update_count(),
            });
        }
        if self.last_log.elapsed().as_secs() >= MODEL_OUTPUT_STATS_LOG_INTERVAL_SECS {
            info!(
                "pre_trade taker decision model stats model_type={} service={} override_symbols={} active_symbols={} recv={} parse_err={} nn_kalman_q={}",
                self.cfg.model_type.as_str(),
                self.service_name,
                self.cfg.symbol_configs.len(),
                self.states.len(),
                self.recv_count,
                self.parse_err_count,
                self.cfg.nn_kalman_q
            );
            self.recv_count = 0;
            self.parse_err_count = 0;
            self.last_log = Instant::now();
        }
        events
    }

    fn score_thresholds_for_symbol(
        &self,
        symbol: &str,
    ) -> Option<&TakerDecisionSymbolScoreThresholds> {
        match self.cfg.model_type {
            TakerDecisionModelType::TreeModel => self.score_thresholds.get(symbol),
            TakerDecisionModelType::NnModel => self.fixed_score_thresholds.as_ref(),
        }
    }

    fn arb_open_gate(&self, symbol: &str) -> Option<TakerDecisionOpenGateSnapshot> {
        let symbol_key = normalize_symbol_for_internal(symbol);
        let state = self.states.get(&symbol_key)?;
        let score = state.latest_score;
        let percentile = state.latest_percentile;
        let update_count = state.update_count();
        if !state.score_ready() {
            return Some(TakerDecisionOpenGateSnapshot {
                allowed: true,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "score_not_ready".to_string(),
            });
        }
        if self
            .score_thresholds_for_symbol(&symbol_key)
            .map_or(true, |thresholds| !thresholds.has_any())
        {
            return Some(TakerDecisionOpenGateSnapshot {
                allowed: false,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "missing_score_thresholds".to_string(),
            });
        }
        Some(TakerDecisionOpenGateSnapshot {
            allowed: true,
            symbol: symbol_key,
            score,
            percentile,
            update_count,
            note: "ready".to_string(),
        })
    }

    fn evaluate(&self, symbol: &str, due_hedge_qty: f64) -> Option<LazyHedgeDecisionSnapshot> {
        let symbol_key = normalize_symbol_for_internal(symbol);
        let state = self.states.get(&symbol_key)?;
        let score = state.latest_score;
        let percentile = state.latest_percentile;
        let update_count = state.update_count();
        if !state.score_ready() {
            return Some(LazyHedgeDecisionSnapshot {
                decision: LazyHedgeDecision::Hedge,
                ready: false,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "score_not_ready".to_string(),
            });
        }
        let Some(score_value) = score.filter(|value| value.is_finite()) else {
            return Some(LazyHedgeDecisionSnapshot {
                decision: LazyHedgeDecision::Hedge,
                ready: false,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "missing_score".to_string(),
            });
        };
        let Some(score_thresholds) = self.score_thresholds_for_symbol(&symbol_key) else {
            return Some(LazyHedgeDecisionSnapshot {
                decision: LazyHedgeDecision::Hedge,
                ready: false,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "missing_score_thresholds".to_string(),
            });
        };
        let decision = if due_hedge_qty > 0.0 {
            let Some(threshold) = score_thresholds.keep_long_score_threshold else {
                return Some(LazyHedgeDecisionSnapshot {
                    decision: LazyHedgeDecision::Hedge,
                    ready: false,
                    symbol: symbol_key,
                    score,
                    percentile,
                    update_count,
                    note: "missing_keep_long_score_threshold".to_string(),
                });
            };
            if score_value > threshold {
                LazyHedgeDecision::KeepLong
            } else {
                LazyHedgeDecision::Hedge
            }
        } else if due_hedge_qty < 0.0 {
            let Some(threshold) = score_thresholds.keep_short_score_threshold else {
                return Some(LazyHedgeDecisionSnapshot {
                    decision: LazyHedgeDecision::Hedge,
                    ready: false,
                    symbol: symbol_key,
                    score,
                    percentile,
                    update_count,
                    note: "missing_keep_short_score_threshold".to_string(),
                });
            };
            if score_value < threshold {
                LazyHedgeDecision::KeepShort
            } else {
                LazyHedgeDecision::Hedge
            }
        } else {
            LazyHedgeDecision::Hedge
        };
        let note = match decision {
            LazyHedgeDecision::Hedge => "hedge".to_string(),
            LazyHedgeDecision::KeepLong => "keep_long".to_string(),
            LazyHedgeDecision::KeepShort => "keep_short".to_string(),
        };
        Some(LazyHedgeDecisionSnapshot {
            decision,
            ready: true,
            symbol: symbol_key,
            score,
            percentile,
            update_count,
            note,
        })
    }

    fn arb_open_cancel(
        &self,
        symbol: &str,
        open_side: Side,
    ) -> Option<TakerDecisionOpenCancelSnapshot> {
        let symbol_key = normalize_symbol_for_internal(symbol);
        let state = self.states.get(&symbol_key)?;
        let score = state.latest_score;
        let percentile = state.latest_percentile;
        let update_count = state.update_count();
        if !state.score_ready() {
            return Some(TakerDecisionOpenCancelSnapshot {
                decision: TakerDecisionOpenCancel::KeepOpen,
                ready: false,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "score_not_ready".to_string(),
            });
        }
        let Some(score_value) = score.filter(|value| value.is_finite()) else {
            return Some(TakerDecisionOpenCancelSnapshot {
                decision: TakerDecisionOpenCancel::KeepOpen,
                ready: false,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "missing_score".to_string(),
            });
        };
        let Some(score_thresholds) = self.score_thresholds_for_symbol(&symbol_key) else {
            return Some(TakerDecisionOpenCancelSnapshot {
                decision: TakerDecisionOpenCancel::KeepOpen,
                ready: false,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "missing_score_thresholds".to_string(),
            });
        };
        let decision = decide_arb_open_cancel(open_side, score_value, score_thresholds);
        let note = match decision {
            TakerDecisionOpenCancel::CancelLong => "cancel_open_long".to_string(),
            TakerDecisionOpenCancel::CancelShort => "cancel_open_short".to_string(),
            TakerDecisionOpenCancel::KeepOpen => "keep_open".to_string(),
        };
        Some(TakerDecisionOpenCancelSnapshot {
            decision,
            ready: true,
            symbol: symbol_key,
            score,
            percentile,
            update_count,
            note,
        })
    }

    fn apply_score_thresholds(
        &mut self,
        load: ModelScoreThresholdPointsLoad,
    ) -> TakerDecisionThresholdReloadStats {
        let mut next = HashMap::with_capacity(load.by_symbol.len());
        for (symbol, points) in load.by_symbol {
            let symbol_cfg = self.cfg.symbol_config(&symbol);
            let thresholds = TakerDecisionSymbolScoreThresholds {
                keep_long_score_threshold: select_score_threshold(
                    &points,
                    symbol_cfg.keep_long_percentile,
                ),
                keep_short_score_threshold: select_score_threshold(
                    &points,
                    symbol_cfg.keep_short_percentile,
                ),
                open_cancel_long_score_threshold: select_score_threshold(
                    &points,
                    symbol_cfg.open_cancel_long_percentile,
                ),
                open_cancel_short_score_threshold: select_score_threshold(
                    &points,
                    symbol_cfg.open_cancel_short_percentile,
                ),
            };
            if thresholds.has_any() {
                next.insert(symbol, thresholds);
            }
        }
        let cached_symbols = next.len();
        self.score_thresholds = next;
        TakerDecisionThresholdReloadStats {
            output_hash_key: load.output_hash_key,
            fields: load.fields,
            ready_symbols: load.ready_symbols,
            cached_symbols,
            invalid_payloads: load.invalid_payloads,
        }
    }
}

fn normalize_env_name(namespace: Option<&str>) -> Option<String> {
    let env_name = namespace?
        .trim()
        .trim_end_matches(['_', '-', ':'])
        .to_ascii_lowercase();
    if env_name.is_empty() {
        None
    } else {
        Some(env_name)
    }
}

fn taker_decision_model_overrides_key(
    namespace: Option<&str>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<String> {
    let env_name = normalize_env_name(namespace)?;
    Some(format!(
        "{env_name}:{}:{}:taker_decsion_model_overrides",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    ))
}

fn parse_symbol_configs(
    raw: &str,
    redis_key: &str,
    default_keep_long_percentile: f64,
    default_keep_short_percentile: f64,
    default_open_cancel_long_percentile: f64,
    default_open_cancel_short_percentile: f64,
) -> Result<HashMap<String, TakerDecisionSymbolConfig>> {
    let decoded: HashMap<String, RawTakerDecisionSymbolConfig> = serde_json::from_str(raw)
        .with_context(|| format!("parse Redis JSON failed: key={redis_key}"))?;
    let mut out = HashMap::with_capacity(decoded.len());
    for (raw_symbol, raw_cfg) in decoded {
        let symbol = normalize_symbol_for_internal(&raw_symbol);
        if symbol.is_empty() {
            anyhow::bail!("Redis key '{redis_key}' contains empty symbol: {raw_symbol:?}");
        }
        let keep_long_percentile = raw_cfg
            .keep_long_percentile
            .unwrap_or(default_keep_long_percentile);
        let keep_short_percentile = raw_cfg
            .keep_short_percentile
            .unwrap_or(default_keep_short_percentile);
        let open_cancel_long_percentile = raw_cfg
            .open_cancel_long_percentile
            .or(raw_cfg.keep_long_percentile)
            .unwrap_or(default_open_cancel_long_percentile);
        let open_cancel_short_percentile = raw_cfg
            .open_cancel_short_percentile
            .or(raw_cfg.keep_short_percentile)
            .unwrap_or(default_open_cancel_short_percentile);
        validate_percentile_value(
            redis_key,
            &symbol,
            "keep_long_percentile",
            keep_long_percentile,
        )?;
        validate_percentile_value(
            redis_key,
            &symbol,
            "keep_short_percentile",
            keep_short_percentile,
        )?;
        validate_percentile_value(
            redis_key,
            &symbol,
            "open_cancel_long_percentile",
            open_cancel_long_percentile,
        )?;
        validate_percentile_value(
            redis_key,
            &symbol,
            "open_cancel_short_percentile",
            open_cancel_short_percentile,
        )?;
        if keep_short_percentile > keep_long_percentile {
            anyhow::bail!(
                "Redis key '{redis_key}' symbol={symbol} keep_short_percentile must be <= keep_long_percentile: short={} long={}",
                keep_short_percentile,
                keep_long_percentile
            );
        }
        if open_cancel_short_percentile > open_cancel_long_percentile {
            anyhow::bail!(
                "Redis key '{redis_key}' symbol={symbol} open_cancel_short_percentile must be <= open_cancel_long_percentile: short={} long={}",
                open_cancel_short_percentile,
                open_cancel_long_percentile
            );
        }
        out.insert(
            symbol,
            TakerDecisionSymbolConfig {
                keep_long_percentile,
                keep_short_percentile,
                open_cancel_long_percentile,
                open_cancel_short_percentile,
            },
        );
    }
    Ok(out)
}

fn validate_percentile_value(redis_key: &str, symbol: &str, field: &str, value: f64) -> Result<()> {
    if !(value.is_finite() && (0.0..=100.0).contains(&value)) {
        anyhow::bail!(
            "Redis key '{redis_key}' symbol={symbol} {field} percentile out of range [0,100]: {value}"
        );
    }
    Ok(())
}

fn parse_model_score_thresholds_hash(
    output_hash_key: &str,
    raw: &HashMap<String, String>,
) -> ModelScoreThresholdPointsLoad {
    let mut by_symbol = HashMap::with_capacity(raw.len());
    let mut ready_symbols = 0usize;
    let mut invalid_payloads = 0usize;

    for (field_symbol, text) in raw {
        let payload = match serde_json::from_str::<RawModelScoreThresholdPayload>(text) {
            Ok(payload) => payload,
            Err(_) => {
                invalid_payloads = invalid_payloads.saturating_add(1);
                continue;
            }
        };
        if !payload.ready {
            continue;
        }
        if payload.quantiles.is_empty() || payload.quantiles.len() != payload.thresholds.len() {
            invalid_payloads = invalid_payloads.saturating_add(1);
            continue;
        }

        let raw_symbol = payload.symbol.as_deref().unwrap_or(field_symbol.as_str());
        let symbol = normalize_symbol_for_internal(raw_symbol);
        if symbol.is_empty() {
            invalid_payloads = invalid_payloads.saturating_add(1);
            continue;
        }

        let mut points = HashMap::with_capacity(payload.quantiles.len());
        for (quantile, threshold) in payload.quantiles.iter().zip(payload.thresholds.iter()) {
            let Some(key) = percentile_key_from_quantile(*quantile) else {
                continue;
            };
            if threshold.is_finite() {
                points.insert(key, *threshold);
            }
        }
        if points.is_empty() {
            invalid_payloads = invalid_payloads.saturating_add(1);
            continue;
        }
        ready_symbols = ready_symbols.saturating_add(1);
        by_symbol.insert(symbol, points);
    }

    ModelScoreThresholdPointsLoad {
        output_hash_key: output_hash_key.to_string(),
        fields: raw.len(),
        ready_symbols,
        invalid_payloads,
        by_symbol,
    }
}

fn percentile_key_from_quantile(raw: f64) -> Option<u16> {
    let percentile = if raw.is_finite() && raw <= 1.0 {
        raw * 100.0
    } else {
        raw
    };
    percentile_key_from_percentile(percentile)
}

fn percentile_key_from_percentile(percentile: f64) -> Option<u16> {
    if !(percentile.is_finite() && (0.0..=100.0).contains(&percentile)) {
        return None;
    }
    let key = (percentile * 100.0).round();
    if !(0.0..=10000.0).contains(&key) {
        return None;
    }
    Some(key as u16)
}

fn select_score_threshold(points: &HashMap<u16, f64>, percentile: f64) -> Option<f64> {
    let key = percentile_key_from_percentile(percentile)?;
    points.get(&key).copied()
}

fn decide_arb_open_cancel(
    open_side: Side,
    score: f64,
    thresholds: &TakerDecisionSymbolScoreThresholds,
) -> TakerDecisionOpenCancel {
    match open_side {
        Side::Buy
            if thresholds
                .open_cancel_long_score_threshold
                .is_some_and(|v| score < v) =>
        {
            TakerDecisionOpenCancel::CancelLong
        }
        Side::Sell
            if thresholds
                .open_cancel_short_score_threshold
                .is_some_and(|v| score > v) =>
        {
            TakerDecisionOpenCancel::CancelShort
        }
        _ => TakerDecisionOpenCancel::KeepOpen,
    }
}

fn strategy_namespace_from_env(namespace: Option<&str>) -> String {
    let raw = namespace.unwrap_or("").trim().to_ascii_lowercase();
    let normalized = raw.replace('_', "-");
    let parts: Vec<&str> = normalized
        .split('-')
        .filter(|part| !part.is_empty())
        .collect();
    if parts.len() >= 2 && parts[1] == "intra" {
        return "intra".to_string();
    }
    if parts.len() >= 3 && parts[2] == "cross" {
        return "cross".to_string();
    }
    if parts.len() >= 2 && parts[1] == "fr" {
        return "fr".to_string();
    }
    if matches!(raw.as_str(), "intra" | "cross" | "fr" | "mm") {
        return raw;
    }
    normalize_namespace(&raw)
}

fn normalize_namespace(namespace: &str) -> String {
    let ns = namespace
        .trim()
        .trim_end_matches(['_', '-', ':'])
        .to_ascii_lowercase();
    if ns.is_empty() {
        "fr".to_string()
    } else {
        ns
    }
}

fn strategy_params_key(
    namespace: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> String {
    let ns = normalize_namespace(namespace);
    let prefix = if ns == "fr" {
        "fr_strategy_params".to_string()
    } else {
        format!("{ns}_strategy_params")
    };
    format!(
        "{}_{}_{}",
        prefix,
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn parse_bool(redis_key: &str, field: &str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" | "" => Ok(false),
        _ => anyhow::bail!("Redis hash '{}' {} invalid bool: {}", redis_key, field, raw),
    }
}

fn parse_percentile(redis_key: &str, field: &str, raw: &str) -> Result<f64> {
    let value = raw.trim().parse::<f64>().with_context(|| {
        format!(
            "Redis hash '{}' {} invalid percentile: {}",
            redis_key, field, raw
        )
    })?;
    if !(value.is_finite() && (0.0..=100.0).contains(&value)) {
        anyhow::bail!(
            "Redis hash '{}' {} percentile out of range [0,100]: {}",
            redis_key,
            field,
            value
        );
    }
    Ok(value)
}

fn parse_optional_finite_score(
    redis_key: &str,
    field: &str,
    raw: Option<&String>,
) -> Result<Option<f64>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let value = raw.trim().parse::<f64>().with_context(|| {
        format!(
            "Redis hash '{}' {} invalid score: {}",
            redis_key, field, raw
        )
    })?;
    if !value.is_finite() {
        anyhow::bail!(
            "Redis hash '{}' {} score must be finite: {}",
            redis_key,
            field,
            raw
        );
    }
    Ok(Some(value))
}

fn parse_nonnegative_finite_f64(redis_key: &str, field: &str, raw: &str) -> Result<f64> {
    let value = raw.trim().parse::<f64>().with_context(|| {
        format!(
            "Redis hash '{}' {} invalid non-negative number: {}",
            redis_key, field, raw
        )
    })?;
    if !value.is_finite() || value < 0.0 {
        anyhow::bail!(
            "Redis hash '{}' {} must be finite and >= 0: {}",
            redis_key,
            field,
            raw
        );
    }
    Ok(value)
}

fn parse_positive_usize(redis_key: &str, field: &str, raw: &str) -> Result<usize> {
    let value = raw.trim().parse::<usize>().with_context(|| {
        format!(
            "Redis hash {} {} invalid positive integer: {}",
            redis_key, field, raw
        )
    })?;
    if value == 0 {
        anyhow::bail!("Redis hash {} {} must be > 0: {}", redis_key, field, value);
    }
    Ok(value)
}

fn env_flag_enabled(names: &[&str]) -> bool {
    names.iter().any(|name| {
        std::env::var(name)
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "True" | "on" | "ON"))
            .unwrap_or(false)
    })
}

fn arb_hedge_taker_env_flags() -> Result<(bool, bool)> {
    let force_taker = env_flag_enabled(&["ARB_HEDGE_FORCE_TAKER"]);
    let lazy_taker = env_flag_enabled(&["ARB_HEDGE_LAZY_TAKER", "ARB_HEDGE_lazy_TAKER"]);
    if force_taker && lazy_taker {
        anyhow::bail!(
            "ARB_HEDGE_FORCE_TAKER and ARB_HEDGE_LAZY_TAKER/ARB_HEDGE_lazy_TAKER are mutually exclusive"
        );
    }
    Ok((force_taker, lazy_taker))
}

fn normalize_service_name(service_name: &str) -> Option<String> {
    let trimmed = service_name.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return None;
    }
    if trimmed.contains('/') {
        Some(trimmed.to_string())
    } else {
        Some(format!("model_output/{trimmed}"))
    }
}

fn model_name_from_service_name(service_name: &str) -> Option<String> {
    service_name
        .trim()
        .rsplit('/')
        .find(|part| !part.trim().is_empty())
        .map(|part| part.trim().to_string())
}

fn model_score_threshold_output_key(model_name: &str) -> String {
    format!("{MODEL_SCORE_THRESHOLD_KEY_PREFIX}{}", model_name.trim())
}

fn sanitize_node_suffix(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    let out = out.trim_matches('_').to_string();
    if out.is_empty() {
        "model".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pre_trade::order_manager::Side;
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn parse_symbol_configs_supports_open_cancel_threshold_overrides() {
        let raw = r#"{
            "BTCUSDT": {
                "keep_long_percentile": 81,
                "keep_short_percentile": 19,
                "open_cancel_long_percentile": 77,
                "open_cancel_short_percentile": 23
            },
            "ETH-USDT": {
                "keep_long_percentile": 88,
                "keep_short_percentile": 12
            }
        }"#;
        let cfg = parse_symbol_configs(raw, "redis:test", 80.0, 20.0, 75.0, 25.0)
            .expect("parse symbol configs");

        let btc = cfg.get("BTCUSDT").expect("btc config");
        assert_eq!(btc.keep_long_percentile, 81.0);
        assert_eq!(btc.keep_short_percentile, 19.0);
        assert_eq!(btc.open_cancel_long_percentile, 77.0);
        assert_eq!(btc.open_cancel_short_percentile, 23.0);

        let eth = cfg.get("ETHUSDT").expect("eth config");
        assert_eq!(eth.keep_long_percentile, 88.0);
        assert_eq!(eth.keep_short_percentile, 12.0);
        assert_eq!(eth.open_cancel_long_percentile, 88.0);
        assert_eq!(eth.open_cancel_short_percentile, 12.0);
    }

    #[test]
    fn parse_symbol_configs_rejects_invalid_open_cancel_threshold_order() {
        let raw = r#"{
            "BTCUSDT": {
                "open_cancel_long_percentile": 20,
                "open_cancel_short_percentile": 80
            }
        }"#;
        let err = parse_symbol_configs(raw, "redis:test", 80.0, 20.0, 80.0, 20.0)
            .expect_err("should reject invalid open cancel threshold ordering");
        let msg = format!("{err:#}");
        assert!(msg.contains("open_cancel_short_percentile must be <= open_cancel_long_percentile"));
    }

    #[test]
    fn decide_arb_open_cancel_is_side_specific() {
        let thresholds = TakerDecisionSymbolScoreThresholds {
            open_cancel_long_score_threshold: Some(0.8),
            open_cancel_short_score_threshold: Some(0.2),
            ..Default::default()
        };

        assert_eq!(
            decide_arb_open_cancel(Side::Buy, 0.5, &thresholds),
            TakerDecisionOpenCancel::CancelLong
        );
        assert_eq!(
            decide_arb_open_cancel(Side::Sell, 0.5, &thresholds),
            TakerDecisionOpenCancel::CancelShort
        );
        assert_eq!(
            decide_arb_open_cancel(Side::Sell, 0.1, &thresholds),
            TakerDecisionOpenCancel::KeepOpen
        );
        assert_eq!(
            decide_arb_open_cancel(Side::Buy, 0.9, &thresholds),
            TakerDecisionOpenCancel::KeepOpen
        );
    }

    #[test]
    fn parse_model_score_thresholds_selects_configured_quantiles() {
        let raw = HashMap::from([(
            "BTCUSDT".to_string(),
            r#"{
                "symbol":"BTCUSDT",
                "ready":true,
                "quantiles":[0.9,0.8,0.2,0.1],
                "thresholds":[0.9,0.8,0.2,0.1]
            }"#
            .to_string(),
        )]);
        let load = parse_model_score_thresholds_hash("model_score_rolling_thresholds_test", &raw);
        let points = load.by_symbol.get("BTCUSDT").expect("btc points");

        assert_eq!(select_score_threshold(points, 80.0), Some(0.8));
        assert_eq!(select_score_threshold(points, 20.0), Some(0.2));
        assert_eq!(select_score_threshold(points, 75.0), None);
    }

    #[test]
    fn symbol_score_state_keeps_rolling_mean_per_instance() {
        let mut btc = SymbolScoreState::new(DEFAULT_LOCAL_LEVEL_KALMAN_Q).expect("state");
        assert_eq!(
            btc.observe(1.0, Some(0.1), true, TakerDecisionModelType::TreeModel, 3),
            Some(10.0)
        );
        assert_eq!(btc.latest_raw_score, Some(1.0));
        assert_eq!(btc.latest_score, Some(1.0));

        btc.observe(2.0, Some(0.2), true, TakerDecisionModelType::TreeModel, 3);
        assert_eq!(btc.latest_raw_score, Some(2.0));
        assert_eq!(btc.latest_score, Some(1.5));

        btc.observe(4.0, Some(0.4), true, TakerDecisionModelType::TreeModel, 3);
        assert_eq!(btc.latest_score, Some(7.0 / 3.0));

        btc.observe(10.0, Some(0.9), true, TakerDecisionModelType::TreeModel, 3);
        assert_eq!(
            btc.latest_observation(TakerDecisionModelType::TreeModel),
            Some(0.9)
        );
        assert_eq!(btc.latest_score, Some(16.0 / 3.0));

        let mut eth = SymbolScoreState::new(DEFAULT_LOCAL_LEVEL_KALMAN_Q).expect("state");
        eth.observe(9.0, None, true, TakerDecisionModelType::TreeModel, 3);
        assert_eq!(eth.latest_score, Some(9.0));
        assert_eq!(btc.latest_score, Some(16.0 / 3.0));

        btc.observe(20.0, None, true, TakerDecisionModelType::TreeModel, 1);
        assert_eq!(btc.latest_score, Some(20.0));
    }

    #[test]
    fn nn_symbol_score_state_exposes_filtered_value() {
        let mut state = SymbolScoreState::new(0.02).expect("state");
        state.observe(0.5, Some(0.5), true, TakerDecisionModelType::NnModel, 1);
        state.observe(-0.5, Some(0.25), true, TakerDecisionModelType::NnModel, 1);

        assert_eq!(state.latest_raw_score, Some(-0.5));
        let filtered = state.latest_score.expect("filtered score");
        assert!((filtered - -0.004_950_495_049_504_955).abs() < 1e-12);
        assert_eq!(
            state.latest_observation(TakerDecisionModelType::NnModel),
            Some(filtered)
        );
    }

    #[test]
    fn nn_global_resample_accessors_expose_filtered_value() {
        let cfg = nn_test_config(unique_test_ipc_endpoint());
        let service_name = PreTradeTakerDecisionModel::connection_name_for_config(&cfg)
            .expect("nn connection name");
        let mut model = PreTradeTakerDecisionModel::build_model(cfg, service_name)
            .expect("build nn model subscriber");
        let mut state = SymbolScoreState::new(DEFAULT_LOCAL_LEVEL_KALMAN_Q).expect("state");
        state.observe(0.5, Some(0.5), true, TakerDecisionModelType::NnModel, 1);
        state.observe(-0.5, Some(0.25), true, TakerDecisionModelType::NnModel, 1);
        let filtered = state.latest_score.expect("filtered score");
        model.states.insert("BTCUSDT".to_string(), state);

        let previous = TAKER_DECISION_MODEL.with(|cell| cell.borrow_mut().replace(model));
        assert_eq!(
            PreTradeTakerDecisionModel::nn_score_global("BTCUSDT"),
            Some(filtered)
        );
        assert_eq!(
            PreTradeTakerDecisionModel::latest_observation_global("BTCUSDT"),
            Some(filtered)
        );
        TAKER_DECISION_MODEL.with(|cell| {
            let _ = cell.borrow_mut().take();
            if let Some(previous) = previous {
                cell.borrow_mut().replace(previous);
            }
        });
    }

    #[test]
    fn invalid_model_config_values_return_errors_without_panicking() {
        assert!(parse_bool("redis:test", "enabled", "invalid").is_err());
        assert!(parse_percentile("redis:test", "percentile", "nan").is_err());
        assert!(parse_nonnegative_finite_f64("redis:test", "q", "-0.01").is_err());
        assert!(parse_nonnegative_finite_f64("redis:test", "q", "inf").is_err());
        assert!(parse_positive_usize("redis:test", "window", "0").is_err());
    }

    #[test]
    fn model_service_normalization_treats_bare_and_prefixed_names_equally() {
        assert_eq!(
            normalize_service_name("return_v2"),
            Some("model_output/return_v2".to_string())
        );
        assert_eq!(
            normalize_service_name("model_output/return_v2"),
            Some("model_output/return_v2".to_string())
        );
        assert_eq!(normalize_service_name("-"), None);
    }

    #[test]
    fn model_transition_is_consumed_once() {
        let _ = PreTradeTakerDecisionModel::take_transition_global();
        PreTradeTakerDecisionModel::publish_transition(
            "model_output/old".to_string(),
            Some("model_output/new".to_string()),
            "model_service_replaced",
        );

        let transition = PreTradeTakerDecisionModel::take_transition_global()
            .expect("transition should be published");
        assert_eq!(transition.previous_service, "model_output/old");
        assert_eq!(transition.next_service.as_deref(), Some("model_output/new"));
        assert_eq!(transition.reason, "model_service_replaced");
        assert!(PreTradeTakerDecisionModel::take_transition_global().is_none());
    }

    #[test]
    fn model_transition_blocks_polling_until_consumed() {
        let _ = PreTradeTakerDecisionModel::take_transition_global();
        PreTradeTakerDecisionModel::publish_transition(
            "model_output/old".to_string(),
            Some("model_output/new".to_string()),
            "model_service_replaced",
        );

        assert!(PreTradeTakerDecisionModel::transition_pending_global());
        assert!(PreTradeTakerDecisionModel::poll_updates_global_limit(1).is_empty());
        assert!(PreTradeTakerDecisionModel::transition_pending_global());

        let _ = PreTradeTakerDecisionModel::take_transition_global();
        assert!(!PreTradeTakerDecisionModel::transition_pending_global());
    }
    fn nn_test_config(endpoint: String) -> TakerDecisionConfig {
        TakerDecisionConfig {
            enabled: true,
            enable_cancel: true,
            model_type: TakerDecisionModelType::NnModel,
            service: "-".to_string(),
            nn_zmq_ipc: Some(endpoint),
            nn_keep_long_score: Some(0.2),
            nn_keep_short_score: Some(-0.2),
            nn_open_cancel_long_score: Some(0.1),
            nn_open_cancel_short_score: Some(-0.1),
            nn_kalman_q: DEFAULT_LOCAL_LEVEL_KALMAN_Q,
            score_rolling_mean_window: 99,
            keep_long_percentile: 80.0,
            keep_short_percentile: 20.0,
            open_cancel_long_percentile: 80.0,
            open_cancel_short_percentile: 20.0,
            symbol_configs: HashMap::new(),
        }
    }

    fn unique_test_ipc_endpoint() -> String {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        format!(
            "ipc:///tmp/mkt_signal_nn_taker_{}_{}.ipc",
            std::process::id(),
            nonce
        )
    }

    #[test]
    fn model_type_defaults_to_tree_and_rejects_unknown_values() {
        assert_eq!(
            TakerDecisionModelType::parse("redis:test", None).expect("default model type"),
            TakerDecisionModelType::TreeModel
        );
        assert_eq!(
            TakerDecisionModelType::parse("redis:test", Some(&"nn_model".to_string()))
                .expect("nn model type"),
            TakerDecisionModelType::NnModel
        );
        assert!(TakerDecisionModelType::parse("redis:test", Some(&"other".to_string())).is_err());
    }

    #[test]
    fn nn_endpoint_change_changes_connection_identity() {
        let first = nn_test_config("ipc:///tmp/predict_rnn_layer_a.ipc".to_string());
        let second = nn_test_config("ipc:///tmp/predict_rnn_layer_b.ipc".to_string());
        let first_name = PreTradeTakerDecisionModel::connection_name_for_config(&first)
            .expect("first connection name");
        let second_name = PreTradeTakerDecisionModel::connection_name_for_config(&second)
            .expect("second connection name");
        assert_ne!(first_name, second_name);
        assert_eq!(first_name, "nn_model:ipc:///tmp/predict_rnn_layer_a.ipc");
        assert_eq!(second_name, "nn_model:ipc:///tmp/predict_rnn_layer_b.ipc");
    }

    #[test]
    fn cancel_switch_does_not_disable_lazy_hedge_model() {
        let mut cfg = nn_test_config(unique_test_ipc_endpoint());
        cfg.enable_cancel = false;
        cfg.nn_open_cancel_long_score = None;
        cfg.nn_open_cancel_short_score = None;
        let nn_kalman_q = cfg.nn_kalman_q;
        let service_name = PreTradeTakerDecisionModel::connection_name_for_config(&cfg)
            .expect("nn connection name");
        let mut model = PreTradeTakerDecisionModel::build_model(cfg, service_name)
            .expect("build nn model subscriber");
        let mut state = SymbolScoreState::new(nn_kalman_q).expect("state");
        state.observe(0.5, Some(0.5), true, TakerDecisionModelType::NnModel, 1);
        model.states.insert("BTCUSDT".to_string(), state);

        let previous = TAKER_DECISION_MODEL.with(|cell| cell.borrow_mut().replace(model));
        assert!(previous.is_none());
        assert_eq!(
            PreTradeTakerDecisionModel::evaluate_global("BTCUSDT", 1.0)
                .expect("lazy hedge decision")
                .decision,
            LazyHedgeDecision::KeepLong
        );
        assert!(PreTradeTakerDecisionModel::arb_open_cancel_global("BTCUSDT", Side::Buy).is_none());
        let _ = TAKER_DECISION_MODEL.with(|cell| cell.borrow_mut().take());
    }

    #[test]
    fn nn_zmq_ipc_receive_is_nonblocking_and_decodes_model_msg() {
        let endpoint = unique_test_ipc_endpoint();
        let socket_path = endpoint.trim_start_matches("ipc://");
        let _ = std::fs::remove_file(socket_path);

        let context = zmq::Context::new();
        let publisher = context.socket(zmq::PUB).expect("create publisher");
        publisher.set_linger(0).expect("set publisher linger");
        publisher.bind(&endpoint).expect("bind test IPC endpoint");

        let cfg = nn_test_config(endpoint.clone());
        let service_name = PreTradeTakerDecisionModel::connection_name_for_config(&cfg)
            .expect("nn connection name");
        let mut model = PreTradeTakerDecisionModel::build_model(cfg, service_name)
            .expect("build nn model subscriber");

        let empty_started = Instant::now();
        assert!(model.poll_updates_limit(256).is_empty());
        assert!(
            empty_started.elapsed() < Duration::from_millis(250),
            "empty ZMQ drain blocked for {:?}",
            empty_started.elapsed()
        );

        std::thread::sleep(Duration::from_millis(100));
        let msg = ModelMsg::create(
            "btcusdt".to_string(),
            1_783_820_499_000,
            1_783_820_499_200,
            42,
            0.5,
            Some(0.5),
            true,
            MODEL_STATUS_OK,
            vec![0],
            vec![0.5],
        );
        let payload = msg.to_bytes().expect("encode ModelMsg");
        publisher
            .send_multipart([b"model_output/btcusdt".as_slice(), payload.as_ref()], 0)
            .expect("publish ModelMsg");

        let deadline = Instant::now() + Duration::from_secs(2);
        let event = loop {
            if let Some(event) = model.poll_updates_limit(256).into_iter().next() {
                break event;
            }
            assert!(
                Instant::now() < deadline,
                "timed out draining test ModelMsg"
            );
            std::thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.score, 0.5);
        assert_eq!(event.percentile, Some(50.0));
        assert!(event.score_ready);
        assert_eq!(event.update_count, 1);

        let long = model.evaluate("BTCUSDT", 1.0).expect("long decision");
        assert_eq!(long.decision, LazyHedgeDecision::KeepLong);
        assert!(long.ready);
        let short = model.evaluate("BTCUSDT", -1.0).expect("short decision");
        assert_eq!(short.decision, LazyHedgeDecision::Hedge);
        let cancel = model
            .arb_open_cancel("BTCUSDT", Side::Sell)
            .expect("open cancel decision");
        assert_eq!(cancel.decision, TakerDecisionOpenCancel::CancelShort);
        assert!(cancel.ready);

        let second_msg = ModelMsg::create(
            "btcusdt".to_string(),
            1_783_820_502_000,
            1_783_820_502_200,
            43,
            -1.0,
            Some(0.25),
            true,
            MODEL_STATUS_OK,
            vec![0],
            vec![-1.0],
        );
        let second_payload = second_msg.to_bytes().expect("encode second ModelMsg");
        publisher
            .send_multipart(
                [b"model_output/btcusdt".as_slice(), second_payload.as_ref()],
                0,
            )
            .expect("publish second ModelMsg");
        let second_deadline = Instant::now() + Duration::from_secs(2);
        let second_event = loop {
            if let Some(event) = model.poll_updates_limit(256).into_iter().next() {
                break event;
            }
            assert!(
                Instant::now() < second_deadline,
                "timed out draining second test ModelMsg"
            );
            std::thread::sleep(Duration::from_millis(5));
        };
        assert!((second_event.score - -0.257_425_742_574_257_43).abs() < 1e-12);
        assert_eq!(second_event.update_count, 2);
        assert_eq!(
            model
                .evaluate("BTCUSDT", -1.0)
                .expect("updated short decision")
                .decision,
            LazyHedgeDecision::KeepShort
        );
        assert_eq!(
            model
                .arb_open_cancel("BTCUSDT", Side::Buy)
                .expect("updated open cancel decision")
                .decision,
            TakerDecisionOpenCancel::CancelLong
        );

        drop(model);
        drop(publisher);
        drop(context);
        let _ = std::fs::remove_file(socket_path);
    }
}
