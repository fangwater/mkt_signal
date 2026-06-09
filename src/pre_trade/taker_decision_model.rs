use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Instant;

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::pre_trade::order_manager::Side;
use mkt_parsers::msg::mkt_msg::{ModelMsg, MODEL_STATUS_OK};
use mkt_parsers::msg::model_ipc::MODEL_PAYLOAD_MAX_BYTES;
use order_common::TradingVenue;

const MODEL_OUTPUT_HISTORY_SIZE: usize = 128;
const MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE: usize = 256;
const MODEL_OUTPUT_POLL_MAX_PER_LOOP: usize = 256;
const MODEL_OUTPUT_STATS_LOG_INTERVAL_SECS: u64 = 60;

thread_local! {
    static TAKER_DECISION_MODEL: RefCell<Option<PreTradeTakerDecisionModel>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone)]
pub struct TakerDecisionConfig {
    pub enabled: bool,
    pub service: String,
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

struct SymbolScoreState {
    latest_score: Option<f64>,
    latest_percentile: Option<f64>,
    latest_score_ready: bool,
    update_count: usize,
}

impl SymbolScoreState {
    fn new() -> Self {
        Self {
            latest_score: None,
            latest_percentile: None,
            latest_score_ready: false,
            update_count: 0,
        }
    }

    fn observe(
        &mut self,
        score: f64,
        score_quantile: Option<f64>,
        score_ready: bool,
    ) -> Option<f64> {
        self.latest_score = Some(score);
        self.latest_percentile = score_quantile.map(|value| value * 100.0);
        self.latest_score_ready = score_ready;
        self.update_count = self.update_count.saturating_add(1);
        self.latest_percentile
    }

    fn score_ready(&self) -> bool {
        self.latest_score_ready
    }

    fn update_count(&self) -> usize {
        self.update_count
    }
}

pub struct PreTradeTakerDecisionModel {
    cfg: TakerDecisionConfig,
    service_name: String,
    _node: Node<ipc::Service>,
    subscriber: Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>,
    states: HashMap<String, SymbolScoreState>,
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
        let enabled = map
            .get("enable_taker_decsion_model")
            .map(|raw| parse_bool(&key, "enable_taker_decsion_model", raw))
            .unwrap_or(false);
        let service = map
            .get("taker_decsion_model_service")
            .map(|raw| raw.trim().to_string())
            .unwrap_or_else(|| "-".to_string());
        let keep_long_percentile = map
            .get("taker_decsion_model_keep_long_percentile")
            .map(|raw| parse_percentile(&key, "taker_decsion_model_keep_long_percentile", raw))
            .unwrap_or(80.0);
        let keep_short_percentile = map
            .get("taker_decsion_model_keep_short_percentile")
            .map(|raw| parse_percentile(&key, "taker_decsion_model_keep_short_percentile", raw))
            .unwrap_or(20.0);
        let open_cancel_long_percentile = map
            .get("taker_decsion_model_open_cancel_long_percentile")
            .map(|raw| {
                parse_percentile(&key, "taker_decsion_model_open_cancel_long_percentile", raw)
            })
            .unwrap_or(keep_long_percentile);
        let open_cancel_short_percentile = map
            .get("taker_decsion_model_open_cancel_short_percentile")
            .map(|raw| {
                parse_percentile(
                    &key,
                    "taker_decsion_model_open_cancel_short_percentile",
                    raw,
                )
            })
            .unwrap_or(keep_short_percentile);
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
        let symbol_configs = if enabled {
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
            if service.trim().is_empty() || service.trim() == "-" {
                anyhow::bail!(
                    "Redis hash '{}' enable_taker_decsion_model=true requires taker_decsion_model_service",
                    key
                );
            }
        }
        Ok(TakerDecisionConfig {
            enabled,
            service,
            keep_long_percentile,
            keep_short_percentile,
            open_cancel_long_percentile,
            open_cancel_short_percentile,
            symbol_configs,
        })
    }

    pub fn initialize(cfg: TakerDecisionConfig) -> Result<bool> {
        if !cfg.enabled {
            info!("pre_trade taker decision model disabled");
            TAKER_DECISION_MODEL.with(|cell| {
                *cell.borrow_mut() = None;
            });
            return Ok(false);
        }
        let service_name = normalize_service_name(&cfg.service)
            .ok_or_else(|| anyhow::anyhow!("taker_decsion_model_service is disabled"))?;
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
            .with_context(|| format!("open taker decision model service failed: {service_name}"))?;
        let subscriber = service
            .subscriber_builder()
            .buffer_size(MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE)
            .create()
            .with_context(|| {
                format!("create taker decision model subscriber failed: {service_name}")
            })?;
        let model = Self {
            cfg,
            service_name: service_name.clone(),
            _node: node,
            subscriber,
            states: HashMap::new(),
            recv_count: 0,
            parse_err_count: 0,
            last_log: Instant::now(),
        };
        let keep_long = model.cfg.keep_long_percentile;
        let keep_short = model.cfg.keep_short_percentile;
        let open_cancel_long = model.cfg.open_cancel_long_percentile;
        let open_cancel_short = model.cfg.open_cancel_short_percentile;
        let symbol_config_count = model.cfg.symbol_configs.len();
        TAKER_DECISION_MODEL.with(|cell| {
            *cell.borrow_mut() = Some(model);
        });
        info!(
            "pre_trade taker decision model enabled service={} default_keep_long={} default_keep_short={} default_open_cancel_long={} default_open_cancel_short={} override_symbols={}",
            service_name,
            keep_long,
            keep_short,
            open_cancel_long,
            open_cancel_short,
            symbol_config_count
        );
        Ok(true)
    }

    pub fn poll_updates_global() -> Vec<ModelUpdateEvent> {
        TAKER_DECISION_MODEL.with(|cell| {
            let mut guard = cell.borrow_mut();
            let Some(model) = guard.as_mut() else {
                return Vec::new();
            };
            model.poll_updates()
        })
    }

    pub fn evaluate_global(symbol: &str, due_hedge_qty: f64) -> Option<LazyHedgeDecisionSnapshot> {
        TAKER_DECISION_MODEL.with(|cell| {
            let guard = cell.borrow();
            let model = guard.as_ref()?;
            model.evaluate(symbol, due_hedge_qty)
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
            model.arb_open_cancel(symbol, open_side)
        })
    }

    fn poll_updates(&mut self) -> Vec<ModelUpdateEvent> {
        let mut events = Vec::new();
        let mut polled = 0usize;
        while polled < MODEL_OUTPUT_POLL_MAX_PER_LOOP {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    polled += 1;
                    let payload = sample.payload();
                    if payload.iter().all(|&b| b == 0) {
                        continue;
                    }
                    let msg = match ModelMsg::from_bytes(payload) {
                        Ok(msg) => msg,
                        Err(err) => {
                            self.parse_err_count = self.parse_err_count.saturating_add(1);
                            warn!(
                                "pre_trade taker decision model parse failed service={} err={}",
                                self.service_name, err
                            );
                            continue;
                        }
                    };
                    if msg.status != MODEL_STATUS_OK || !msg.score.is_finite() {
                        continue;
                    }
                    let symbol = normalize_symbol_for_internal(&msg.symbol);
                    if symbol.is_empty() {
                        continue;
                    }
                    let state = self
                        .states
                        .entry(symbol.clone())
                        .or_insert_with(SymbolScoreState::new);
                    let percentile = state.observe(msg.score, msg.score_quantile, msg.score_ready);
                    self.recv_count = self.recv_count.saturating_add(1);
                    events.push(ModelUpdateEvent {
                        symbol,
                        score: msg.score,
                        percentile,
                        score_ready: msg.score_ready,
                        update_count: state.update_count(),
                    });
                }
                Ok(None) => break,
                Err(err) => {
                    warn!(
                        "pre_trade taker decision model receive failed service={} err={}",
                        self.service_name, err
                    );
                    break;
                }
            }
        }
        if self.last_log.elapsed().as_secs() >= MODEL_OUTPUT_STATS_LOG_INTERVAL_SECS {
            info!(
                "pre_trade taker decision model stats service={} override_symbols={} active_symbols={} recv={} parse_err={}",
                self.service_name,
                self.cfg.symbol_configs.len(),
                self.states.len(),
                self.recv_count,
                self.parse_err_count
            );
            self.recv_count = 0;
            self.parse_err_count = 0;
            self.last_log = Instant::now();
        }
        events
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
        if percentile.filter(|value| value.is_finite()).is_none() {
            return Some(TakerDecisionOpenGateSnapshot {
                allowed: false,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "missing_percentile".to_string(),
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
        let symbol_cfg = self.cfg.symbol_config(&symbol_key);
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
        let Some(q) = percentile.filter(|value| value.is_finite()) else {
            return Some(LazyHedgeDecisionSnapshot {
                decision: LazyHedgeDecision::Hedge,
                ready: false,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "missing_percentile".to_string(),
            });
        };
        let decision = if due_hedge_qty > 0.0 && q > symbol_cfg.keep_long_percentile {
            LazyHedgeDecision::KeepLong
        } else if due_hedge_qty < 0.0 && q < symbol_cfg.keep_short_percentile {
            LazyHedgeDecision::KeepShort
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
        let symbol_cfg = self.cfg.symbol_config(&symbol_key);
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
        let Some(q) = percentile.filter(|value| value.is_finite()) else {
            return Some(TakerDecisionOpenCancelSnapshot {
                decision: TakerDecisionOpenCancel::KeepOpen,
                ready: false,
                symbol: symbol_key,
                score,
                percentile,
                update_count,
                note: "missing_percentile".to_string(),
            });
        };
        let decision = decide_arb_open_cancel(open_side, q, &symbol_cfg);
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

fn decide_arb_open_cancel(
    open_side: Side,
    percentile: f64,
    symbol_cfg: &TakerDecisionSymbolConfig,
) -> TakerDecisionOpenCancel {
    match open_side {
        Side::Buy if percentile < symbol_cfg.open_cancel_long_percentile => {
            TakerDecisionOpenCancel::CancelLong
        }
        Side::Sell if percentile > symbol_cfg.open_cancel_short_percentile => {
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

fn parse_bool(redis_key: &str, field: &str, raw: &str) -> bool {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => true,
        "false" | "0" | "no" | "off" | "" => false,
        _ => panic!("Redis hash '{}' {} invalid bool: {}", redis_key, field, raw),
    }
}

fn parse_percentile(redis_key: &str, field: &str, raw: &str) -> f64 {
    let value = raw.trim().parse::<f64>().unwrap_or_else(|_| {
        panic!(
            "Redis hash '{}' {} invalid percentile: {}",
            redis_key, field, raw
        )
    });
    if !(value.is_finite() && (0.0..=100.0).contains(&value)) {
        panic!(
            "Redis hash '{}' {} percentile out of range [0,100]: {}",
            redis_key, field, value
        );
    }
    value
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
    use super::{
        decide_arb_open_cancel, parse_symbol_configs, TakerDecisionOpenCancel,
        TakerDecisionSymbolConfig,
    };
    use crate::pre_trade::order_manager::Side;

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
        let cfg = TakerDecisionSymbolConfig {
            keep_long_percentile: 80.0,
            keep_short_percentile: 20.0,
            open_cancel_long_percentile: 80.0,
            open_cancel_short_percentile: 20.0,
        };

        assert_eq!(
            decide_arb_open_cancel(Side::Buy, 50.0, &cfg),
            TakerDecisionOpenCancel::CancelLong
        );
        assert_eq!(
            decide_arb_open_cancel(Side::Sell, 50.0, &cfg),
            TakerDecisionOpenCancel::CancelShort
        );
        assert_eq!(
            decide_arb_open_cancel(Side::Sell, 10.0, &cfg),
            TakerDecisionOpenCancel::KeepOpen
        );
        assert_eq!(
            decide_arb_open_cancel(Side::Buy, 90.0, &cfg),
            TakerDecisionOpenCancel::KeepOpen
        );
    }
}
