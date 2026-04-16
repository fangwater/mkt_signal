use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{error, info, warn};
use redis::Commands;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use crate::common::mkt_msg::{FactorValueMsg, ModelMsg, MODEL_STATUS_OK};
use crate::common::model_ipc::MODEL_PAYLOAD_MAX_BYTES;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::time_util::get_timestamp_us;
use crate::factor_pub::factor_index::factor_name_to_index;
use crate::signal::common::TradingVenue;

const FACTOR_VALUE_PAYLOAD_MAX_BYTES: usize = 256;
const FACTOR_VALUE_SUBSCRIBER_BUFFER_SIZE: usize = 8192;
const MODEL_OUTPUT_HISTORY_SIZE: usize = 128;
const MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE: usize = 256;
const MODEL_OUTPUT_POLL_MAX_PER_CHANNEL: usize = 256;
const MODEL_OUTPUT_STATS_LOG_INTERVAL_SECS: u64 = 60;
const DEFAULT_PNLU_MAX_AGE_SECS: i64 = 30 * 60;
const FACTOR_VALUE_ISSUE_LOG_INTERVAL_SECS: u64 = 10;

#[derive(Debug, Clone)]
pub struct FactorValueLookupResult {
    pub key: String,
    pub symbol_key: String,
    pub ready: Option<bool>,
    pub target_factor_value: Option<f64>,
    pub ts_ms: Option<i64>,
    pub factor_index: Option<u16>,
    pub note: String,
}

#[derive(Debug, Deserialize)]
struct PnluFactorPayload {
    ts: Option<i64>,
    target_ts: Option<i64>,
    factor: Option<f64>,
    quantiles: Option<Vec<f64>>,
    thresholds: Option<Vec<f64>>,
    ready: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct PnluCheckResult {
    pub ok: bool,
    pub reason: String,
    pub factor: Option<f64>,
    pub threshold: Option<f64>,
    pub ts: Option<i64>,
    pub target_ts: Option<i64>,
    pub age_secs: Option<i64>,
    pub ready: Option<bool>,
    pub quantiles: Vec<f64>,
    pub thresholds: Vec<f64>,
}

impl PnluCheckResult {
    pub fn fail(reason: impl Into<String>) -> Self {
        Self {
            ok: false,
            reason: reason.into(),
            factor: None,
            threshold: None,
            ts: None,
            target_ts: None,
            age_secs: None,
            ready: None,
            quantiles: Vec::new(),
            thresholds: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum EnvironmentSignalSource {
    ModelOutput,
    PnluFallback,
}

#[derive(Debug, Clone)]
pub struct EnvironmentSignalResult {
    pub source: EnvironmentSignalSource,
    pub allow_open: bool,
    pub class_label: i8,
    pub service_name: Option<String>,
    pub symbol_key: String,
    pub score: Option<f64>,
    pub score_quantile: Option<f64>,
    pub threshold: Option<f64>,
    pub note: String,
}

#[derive(Debug, Clone)]
pub struct ModelOutputScoreLookupResult {
    pub service_name: String,
    pub symbol_key: String,
    pub subscribed: bool,
    pub score: Option<f64>,
    pub score_quantile: Option<f64>,
    pub note: String,
}

#[derive(Debug, Clone)]
pub struct ModelOutputUpdateEvent {
    pub service_name: String,
    pub symbol_key: String,
    pub score: f64,
    pub score_quantile: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
struct FactorValueSnapshot {
    value: f64,
    ready: bool,
    timestamp_ms: i64,
    factor_index: u16,
}

#[derive(Debug, Clone, Copy)]
struct ModelOutputSnapshot {
    score: f64,
    score_quantile: Option<f64>,
}

struct FactorIssueLogState {
    note: String,
    last_log_at: Instant,
}

struct ModelOutputSubscriberEntry {
    service_name: String,
    subscriber: Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>,
}

struct PnluRedis {
    settings: RedisSettings,
    client: redis::Client,
    conn: Option<redis::Connection>,
}

impl fmt::Debug for PnluRedis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PnluRedis")
            .field("host", &self.settings.host)
            .field("port", &self.settings.port)
            .field("db", &self.settings.db)
            .finish()
    }
}

impl PnluRedis {
    fn new(settings: RedisSettings) -> Result<Self> {
        let url = settings.connection_url();
        let client = redis::Client::open(url.clone())
            .with_context(|| format!("PnluRedis: invalid redis url: {url}"))?;
        Ok(Self {
            settings,
            client,
            conn: None,
        })
    }

    fn get_string(&mut self, key: &str) -> Result<Option<String>> {
        if self.conn.is_none() {
            self.conn =
                Some(self.client.get_connection().with_context(|| {
                    format!("PnluRedis: connect failed {}", self.settings.host)
                })?);
        }
        let conn = self
            .conn
            .as_mut()
            .expect("PnluRedis: connection missing after init");
        let result: redis::RedisResult<Option<String>> = conn.get(key);
        match result {
            Ok(value) => Ok(value),
            Err(err) => {
                self.conn = None;
                Err(anyhow::anyhow!("PnluRedis: get failed: {}", err))
            }
        }
    }
}

pub struct FactorValueHub {
    hedge_venue: TradingVenue,
    pnlu_profile: String,
    target_factor_index: u16,
    target_factor_key_prefix: String,
    factor_value_service_name: String,
    factor_value_max_age_ms: i64,
    factor_value_sub: Subscriber<ipc::Service, [u8; FACTOR_VALUE_PAYLOAD_MAX_BYTES], ()>,
    factor_value_cache: HashMap<(u16, String), FactorValueSnapshot>,
    last_valid_factor_value_cache: HashMap<(u16, String), FactorValueSnapshot>,
    factor_issue_log_state: HashMap<String, FactorIssueLogState>,
    model_output_subscribers: Vec<ModelOutputSubscriberEntry>,
    model_output_services: Vec<String>,
    model_output_latest_scores: HashMap<(String, String), ModelOutputSnapshot>,
    model_output_msg_count: u64,
    model_output_parse_err_count: u64,
    model_output_last_log: Instant,
    pnlu_redis: PnluRedis,
    pnlu_key_suffix: String,
    pnlu_max_age_secs: i64,
}

impl FactorValueHub {
    pub fn new(
        node: &Node<ipc::Service>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        target_factor_name: &str,
        target_factor_key_prefix: &str,
        pnlu_settings: RedisSettings,
        pnlu_key_suffix: String,
        pnlu_max_age_secs: i64,
        factor_value_max_age_ms: i64,
    ) -> Result<Self> {
        let factor_value_sub =
            Self::create_factor_value_subscriber(node, hedge_venue, target_factor_name)?;
        let target_factor_index = factor_name_to_index(target_factor_name).ok_or_else(|| {
            anyhow::anyhow!("missing factor index mapping for {target_factor_name}")
        })?;
        let pnlu_redis = PnluRedis::new(pnlu_settings)?;
        let pnlu_max_age_secs = if pnlu_max_age_secs > 0 {
            pnlu_max_age_secs
        } else {
            DEFAULT_PNLU_MAX_AGE_SECS
        };
        let pnlu_profile = Self::build_pnlu_profile(open_venue, hedge_venue);
        let factor_value_service_name = format!(
            "factor_pub/{}/{}",
            hedge_venue.data_pub_slug(),
            target_factor_name
        );

        Ok(Self {
            hedge_venue,
            pnlu_profile,
            target_factor_index,
            target_factor_key_prefix: target_factor_key_prefix.to_string(),
            factor_value_service_name,
            factor_value_max_age_ms,
            factor_value_sub,
            factor_value_cache: HashMap::new(),
            last_valid_factor_value_cache: HashMap::new(),
            factor_issue_log_state: HashMap::new(),
            model_output_subscribers: Vec::new(),
            model_output_services: Vec::new(),
            model_output_latest_scores: HashMap::new(),
            model_output_msg_count: 0,
            model_output_parse_err_count: 0,
            model_output_last_log: Instant::now(),
            pnlu_redis,
            pnlu_key_suffix,
            pnlu_max_age_secs,
        })
    }

    fn build_pnlu_profile(open_venue: TradingVenue, hedge_venue: TradingVenue) -> String {
        format!(
            "{}-{}",
            open_venue.data_pub_slug(),
            hedge_venue.data_pub_slug()
        )
    }

    fn build_pnlu_key(symbol_key: &str, key_suffix: &str, profile: &str) -> String {
        format!("{symbol_key}{key_suffix}_{profile}")
    }

    fn create_factor_value_subscriber(
        node: &Node<ipc::Service>,
        hedge_venue: TradingVenue,
        factor_name: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; FACTOR_VALUE_PAYLOAD_MAX_BYTES], ()>> {
        let service_name = format!("factor_pub/{}/{}", hedge_venue.data_pub_slug(), factor_name);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; FACTOR_VALUE_PAYLOAD_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .open()
            .with_context(|| format!("failed to open factor subscriber service={service_name}"))?;
        let service_max_buffer = service.static_config().subscriber_max_buffer_size();
        let service_history = service.static_config().history_size();
        let requested_buffer = service_max_buffer
            .min(FACTOR_VALUE_SUBSCRIBER_BUFFER_SIZE)
            .max(1);

        info!(
            "FactorValueHub: subscribed factor stream service={} subscriber_buffer_size={} service_subscriber_max_buffer_size={} service_history_size={}",
            service_name,
            requested_buffer,
            service_max_buffer,
            service_history,
        );
        service
            .subscriber_builder()
            .buffer_size(requested_buffer)
            .create()
            .context("failed to create factor subscriber")
    }

    fn validate_factor_snapshot(
        snapshot: &FactorValueSnapshot,
        now_ms: i64,
        max_age_ms: i64,
    ) -> std::result::Result<(), String> {
        if max_age_ms <= 0 {
            return Ok(());
        }
        if snapshot.timestamp_ms <= 0 {
            return Err(format!(
                "invalid_ipc_timestamp(ts_ms={})",
                snapshot.timestamp_ms
            ));
        }
        if snapshot.timestamp_ms > now_ms {
            return Err(format!(
                "future_ipc_timestamp(ts_ms={} now_ms={})",
                snapshot.timestamp_ms, now_ms
            ));
        }
        let age_ms = now_ms - snapshot.timestamp_ms;
        if age_ms > max_age_ms {
            return Err(format!(
                "factor_ipc_timeout(age_ms={} max_age_ms={})",
                age_ms, max_age_ms
            ));
        }
        Ok(())
    }

    fn log_factor_issue(&mut self, symbol_key: &str, note: &str, ts_ms: Option<i64>) {
        let now = Instant::now();
        let should_log = match self.factor_issue_log_state.get(symbol_key) {
            Some(state) => {
                state.note != note
                    || now.duration_since(state.last_log_at)
                        >= Duration::from_secs(FACTOR_VALUE_ISSUE_LOG_INTERVAL_SECS)
            }
            None => true,
        };
        if !should_log {
            return;
        }

        error!(
            "FactorValueHub: factor snapshot unavailable service={} symbol={} note={} ts_ms={:?} max_age_ms={}",
            self.factor_value_service_name,
            symbol_key,
            note,
            ts_ms,
            self.factor_value_max_age_ms,
        );
        self.factor_issue_log_state.insert(
            symbol_key.to_string(),
            FactorIssueLogState {
                note: note.to_string(),
                last_log_at: now,
            },
        );
    }

    fn create_model_output_subscriber(
        node: &Node<ipc::Service>,
        service_name: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>> {
        let service = node
            .service_builder(&ServiceName::new(service_name)?)
            .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(MODEL_OUTPUT_HISTORY_SIZE)
            .subscriber_max_buffer_size(MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE)
            .open_or_create()
            .with_context(|| {
                format!("failed to open/create model_output subscriber service={service_name}")
            })?;

        service
            .subscriber_builder()
            .buffer_size(MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE)
            .create()
            .with_context(|| format!("failed to create model_output subscriber: {service_name}"))
    }

    fn normalize_model_output_service_name(service_name: &str) -> Option<String> {
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

    fn normalize_model_output_services(services: Vec<String>) -> Vec<String> {
        let mut normalized = Vec::new();
        for raw in services {
            let Some(service) = Self::normalize_model_output_service_name(&raw) else {
                continue;
            };
            if !normalized.iter().any(|s| s == &service) {
                normalized.push(service);
            }
        }
        normalized
    }

    fn poll_factor_value_updates(&mut self) {
        loop {
            match self.factor_value_sub.receive() {
                Ok(Some(sample)) => {
                    let payload = sample.payload();
                    if payload.iter().all(|&b| b == 0) {
                        continue;
                    }
                    let msg = match FactorValueMsg::from_bytes(payload) {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!("FactorValueHub: parse factor payload failed: {}", err);
                            continue;
                        }
                    };

                    let symbol_key = normalize_symbol_for_venue(&msg.symbol, self.hedge_venue);
                    for (factor_index, value, ready) in msg.factors() {
                        let cache_key = (factor_index, symbol_key.clone());
                        let snapshot = FactorValueSnapshot {
                            value,
                            ready,
                            timestamp_ms: msg.timestamp_ms,
                            factor_index,
                        };
                        let should_update = self
                            .factor_value_cache
                            .get(&cache_key)
                            .map(|prev| snapshot.timestamp_ms >= prev.timestamp_ms)
                            .unwrap_or(true);
                        if should_update {
                            self.factor_value_cache.insert(cache_key.clone(), snapshot);
                            if ready && value.is_finite() {
                                let should_update_last_valid = self
                                    .last_valid_factor_value_cache
                                    .get(&cache_key)
                                    .map(|prev| snapshot.timestamp_ms >= prev.timestamp_ms)
                                    .unwrap_or(true);
                                if should_update_last_valid {
                                    self.last_valid_factor_value_cache.insert(cache_key, snapshot);
                                }
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    error!(
                        "FactorValueHub: factor subscriber receive error service={} err={}",
                        self.factor_value_service_name, err
                    );
                    break;
                }
            }
        }
    }

    pub fn poll_model_output_updates(&mut self) -> Vec<ModelOutputUpdateEvent> {
        let mut events = Vec::new();
        if self.model_output_subscribers.is_empty() {
            return events;
        }

        for entry in &mut self.model_output_subscribers {
            let mut polled = 0usize;
            while polled < MODEL_OUTPUT_POLL_MAX_PER_CHANNEL {
                match entry.subscriber.receive() {
                    Ok(Some(sample)) => {
                        polled += 1;
                        let payload = sample.payload();
                        if payload.iter().all(|&b| b == 0) {
                            continue;
                        }

                        let msg = match ModelMsg::from_bytes(payload) {
                            Ok(msg) => msg,
                            Err(err) => {
                                self.model_output_parse_err_count =
                                    self.model_output_parse_err_count.saturating_add(1);
                                warn!(
                                    "FactorValueHub: parse model_output payload failed service={} err={}",
                                    entry.service_name, err
                                );
                                continue;
                            }
                        };

                        if msg.status != MODEL_STATUS_OK {
                            continue;
                        }

                        let symbol_key = normalize_symbol_for_venue(&msg.symbol, self.hedge_venue);
                        let cache_key = (entry.service_name.clone(), symbol_key);
                        let event = ModelOutputUpdateEvent {
                            service_name: entry.service_name.clone(),
                            symbol_key: cache_key.1.clone(),
                            score: msg.score,
                            score_quantile: msg.score_quantile,
                        };
                        self.model_output_latest_scores.insert(
                            cache_key,
                            ModelOutputSnapshot {
                                score: msg.score,
                                score_quantile: msg.score_quantile,
                            },
                        );
                        self.model_output_msg_count = self.model_output_msg_count.saturating_add(1);
                        events.push(event);
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!(
                            "FactorValueHub: model_output subscriber receive error service={} err={}",
                            entry.service_name, err
                        );
                        break;
                    }
                }
            }
        }

        if self.model_output_last_log.elapsed().as_secs() >= MODEL_OUTPUT_STATS_LOG_INTERVAL_SECS {
            info!(
                "FactorValueHub: model_output stats services={} latest_scores={} recv={} parse_err={}",
                self.model_output_services.len(),
                self.model_output_latest_scores.len(),
                self.model_output_msg_count,
                self.model_output_parse_err_count
            );
            self.model_output_last_log = Instant::now();
            self.model_output_msg_count = 0;
            self.model_output_parse_err_count = 0;
        }

        events
    }

    pub fn lookup_factor_value(
        &mut self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> FactorValueLookupResult {
        self.poll_factor_value_updates();

        let symbol_key = normalize_symbol_for_venue(hedge_symbol, hedge_venue);
        let cache_key = (self.target_factor_index, symbol_key.clone());
        let key = format!(
            "{}_{}_{}",
            self.target_factor_key_prefix,
            hedge_venue.data_pub_slug(),
            symbol_key
        );

        if let Some(snapshot) = self.factor_value_cache.get(&cache_key) {
            let snapshot = *snapshot;
            let now_ms = get_timestamp_us() / 1000;
            match Self::validate_factor_snapshot(&snapshot, now_ms, self.factor_value_max_age_ms) {
                Ok(()) => {
                    self.factor_issue_log_state.remove(&symbol_key);
                    FactorValueLookupResult {
                        key,
                        symbol_key,
                        ready: Some(snapshot.ready),
                        target_factor_value: Some(snapshot.value),
                        ts_ms: Some(snapshot.timestamp_ms),
                        factor_index: Some(snapshot.factor_index),
                        note: "ok".to_string(),
                    }
                }
                Err(note) => {
                    self.log_factor_issue(&symbol_key, &note, Some(snapshot.timestamp_ms));
                    FactorValueLookupResult {
                        key,
                        symbol_key,
                        ready: Some(false),
                        target_factor_value: None,
                        ts_ms: Some(snapshot.timestamp_ms),
                        factor_index: Some(snapshot.factor_index),
                        note,
                    }
                }
            }
        } else {
            self.log_factor_issue(&symbol_key, "missing_ipc_snapshot", None);
            FactorValueLookupResult {
                key,
                symbol_key,
                ready: None,
                target_factor_value: None,
                ts_ms: None,
                factor_index: None,
                note: "missing_ipc_snapshot".to_string(),
            }
        }
    }

    pub fn lookup_factor_value_with_last_valid_fallback(
        &mut self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> FactorValueLookupResult {
        let strict = self.lookup_factor_value(hedge_symbol, hedge_venue);
        if strict.target_factor_value.is_some() {
            return strict;
        }

        let cache_key = (self.target_factor_index, strict.symbol_key.clone());
        if let Some(snapshot) = self.last_valid_factor_value_cache.get(&cache_key).copied() {
            return FactorValueLookupResult {
                key: strict.key,
                symbol_key: strict.symbol_key,
                ready: Some(snapshot.ready),
                target_factor_value: Some(snapshot.value),
                ts_ms: Some(snapshot.timestamp_ms),
                factor_index: Some(snapshot.factor_index),
                note: format!("fallback_last_valid({})", strict.note),
            };
        }

        strict
    }

    pub fn update_model_output_services(
        &mut self,
        node: &Node<ipc::Service>,
        services: Vec<String>,
    ) {
        let normalized = Self::normalize_model_output_services(services);
        if normalized == self.model_output_services {
            return;
        }

        if normalized.is_empty() {
            self.model_output_services.clear();
            self.model_output_subscribers.clear();
            self.model_output_latest_scores.clear();
            info!("FactorValueHub: model_output subscriptions cleared");
            return;
        }

        let mut subscribers = Vec::new();
        for service_name in &normalized {
            match Self::create_model_output_subscriber(node, service_name) {
                Ok(subscriber) => {
                    subscribers.push(ModelOutputSubscriberEntry {
                        service_name: service_name.clone(),
                        subscriber,
                    });
                }
                Err(err) => {
                    warn!(
                        "FactorValueHub: subscribe model_output failed service={} err={:#}",
                        service_name, err
                    );
                }
            }
        }

        if subscribers.is_empty() {
            warn!(
                "FactorValueHub: no model_output subscriber created, keep previous subscriptions count={}",
                self.model_output_subscribers.len()
            );
            return;
        }

        self.model_output_services = normalized;
        self.model_output_subscribers = subscribers;
        self.model_output_latest_scores.clear();
        self.model_output_msg_count = 0;
        self.model_output_parse_err_count = 0;
        self.model_output_last_log = Instant::now();
        info!(
            "FactorValueHub: model_output subscriptions updated count={} services={:?} buffer_size={}",
            self.model_output_subscribers.len(),
            self.model_output_services,
            MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE
        );
    }

    fn normalize_pnlu_ts_us(ts: i64) -> Option<i64> {
        if ts <= 0 {
            return None;
        }
        let abs = ts.abs();
        let ts_us = if abs > 1_000_000_000_000_000 {
            ts
        } else if abs > 1_000_000_000_000 {
            ts.saturating_mul(1_000)
        } else {
            ts.saturating_mul(1_000_000)
        };
        Some(ts_us)
    }

    pub fn check_pnlu_factor(&mut self, symbol_key: &str, now_us: i64) -> PnluCheckResult {
        let key = Self::build_pnlu_key(symbol_key, &self.pnlu_key_suffix, &self.pnlu_profile);
        let raw = match self.pnlu_redis.get_string(&key) {
            Ok(Some(text)) => text,
            Ok(None) => return PnluCheckResult::fail("missing_key"),
            Err(err) => return PnluCheckResult::fail(format!("redis_error: {err}")),
        };

        let payload: PnluFactorPayload = match serde_json::from_str(&raw) {
            Ok(val) => val,
            Err(err) => return PnluCheckResult::fail(format!("invalid_json: {err}")),
        };

        let PnluFactorPayload {
            ts,
            target_ts,
            factor,
            quantiles,
            thresholds,
            ready,
        } = payload;
        let quantiles = quantiles.unwrap_or_default();
        let thresholds = thresholds.unwrap_or_default();
        let threshold = thresholds.get(1).copied();

        let ts_us = ts.and_then(Self::normalize_pnlu_ts_us);
        let age_secs = match ts_us {
            Some(ts_us) if ts_us <= now_us => Some((now_us - ts_us) / 1_000_000),
            Some(_) => return PnluCheckResult::fail("ts_in_future"),
            None => None,
        };
        let fresh = match age_secs {
            Some(age) => age <= self.pnlu_max_age_secs,
            None => false,
        };
        let missing_factor_or_threshold = factor.is_none() || threshold.is_none();
        let factor_ok = match (factor, threshold) {
            (Some(f), Some(t)) => f > t,
            _ => false,
        };

        let ok = fresh && !missing_factor_or_threshold && factor_ok;
        let reason = if ok {
            "ok".to_string()
        } else if ts_us.is_none() {
            "missing_ts".to_string()
        } else if !fresh {
            "stale_ts".to_string()
        } else if missing_factor_or_threshold {
            "missing_factor_or_threshold".to_string()
        } else {
            "factor_not_gt_threshold".to_string()
        };

        PnluCheckResult {
            ok,
            reason,
            factor,
            threshold,
            ts,
            target_ts,
            age_secs,
            ready,
            quantiles,
            thresholds,
        }
    }

    pub fn evaluate_environment_signal(
        &mut self,
        environment_model_service: Option<&str>,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
        model_true_threshold: f64,
        pnlu_symbol_key: &str,
        now_us: i64,
    ) -> EnvironmentSignalResult {
        let normalized_service =
            environment_model_service.and_then(Self::normalize_model_output_service_name);

        if let Some(service_name) = normalized_service {
            let symbol_key = normalize_symbol_for_venue(hedge_symbol, hedge_venue);
            let cache_key = (service_name.clone(), symbol_key.clone());
            match self.model_output_latest_scores.get(&cache_key).copied() {
                Some(snapshot) if snapshot.score.is_finite() => {
                    let allow_open = snapshot.score >= model_true_threshold;
                    EnvironmentSignalResult {
                        source: EnvironmentSignalSource::ModelOutput,
                        allow_open,
                        class_label: if allow_open { 1 } else { 0 },
                        service_name: Some(service_name),
                        symbol_key,
                        score: Some(snapshot.score),
                        score_quantile: snapshot.score_quantile,
                        threshold: Some(model_true_threshold),
                        note: if allow_open {
                            "model_score_ge_threshold".to_string()
                        } else {
                            "model_score_lt_threshold".to_string()
                        },
                    }
                }
                Some(_) => EnvironmentSignalResult {
                    source: EnvironmentSignalSource::ModelOutput,
                    allow_open: false,
                    class_label: 0,
                    service_name: Some(service_name),
                    symbol_key,
                    score: None,
                    score_quantile: None,
                    threshold: Some(model_true_threshold),
                    note: "invalid_model_score".to_string(),
                },
                None => EnvironmentSignalResult {
                    source: EnvironmentSignalSource::ModelOutput,
                    allow_open: false,
                    class_label: 0,
                    service_name: Some(service_name),
                    symbol_key,
                    score: None,
                    score_quantile: None,
                    threshold: Some(model_true_threshold),
                    note: "missing_model_score".to_string(),
                },
            }
        } else {
            let pnlu_check = self.check_pnlu_factor(pnlu_symbol_key, now_us);
            let allow_open = pnlu_check.ok;
            EnvironmentSignalResult {
                source: EnvironmentSignalSource::PnluFallback,
                allow_open,
                class_label: if allow_open { 1 } else { 0 },
                service_name: None,
                symbol_key: pnlu_symbol_key.to_string(),
                score: pnlu_check.factor,
                score_quantile: None,
                threshold: pnlu_check.threshold,
                note: format!("pnlu_fallback:{}", pnlu_check.reason),
            }
        }
    }

    pub fn lookup_model_output_score(
        &mut self,
        model_service: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> ModelOutputScoreLookupResult {
        let _ = self.poll_model_output_updates();
        self.cached_model_output_score(model_service, hedge_symbol, hedge_venue)
    }

    pub fn cached_model_output_score(
        &self,
        model_service: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> ModelOutputScoreLookupResult {
        let Some(service_name) = Self::normalize_model_output_service_name(model_service) else {
            return ModelOutputScoreLookupResult {
                service_name: model_service.trim().to_string(),
                symbol_key: normalize_symbol_for_venue(hedge_symbol, hedge_venue),
                subscribed: false,
                score: None,
                score_quantile: None,
                note: "service_disabled".to_string(),
            };
        };
        let symbol_key = normalize_symbol_for_venue(hedge_symbol, hedge_venue);

        let subscribed = self
            .model_output_services
            .iter()
            .any(|s| s == &service_name);
        if !subscribed {
            return ModelOutputScoreLookupResult {
                service_name,
                symbol_key,
                subscribed: false,
                score: None,
                score_quantile: None,
                note: "service_not_subscribed".to_string(),
            };
        }

        let cache_key = (service_name.clone(), symbol_key.clone());
        match self.model_output_latest_scores.get(&cache_key).copied() {
            Some(snapshot) if snapshot.score.is_finite() => ModelOutputScoreLookupResult {
                service_name,
                symbol_key,
                subscribed: true,
                score: Some(snapshot.score),
                score_quantile: snapshot.score_quantile,
                note: "ok".to_string(),
            },
            Some(_) => ModelOutputScoreLookupResult {
                service_name,
                symbol_key,
                subscribed: true,
                score: None,
                score_quantile: None,
                note: "invalid_model_score".to_string(),
            },
            None => ModelOutputScoreLookupResult {
                service_name,
                symbol_key,
                subscribed: true,
                score: None,
                score_quantile: None,
                note: "missing_model_score".to_string(),
            },
        }
    }

    pub fn update_pnlu_key_suffix(&mut self, key_suffix: String) {
        let trimmed = key_suffix.trim();
        if trimmed.is_empty() {
            warn!("FactorValueHub: ignore empty pnlu key suffix update");
            return;
        }
        self.pnlu_key_suffix = trimmed.to_string();
        info!(
            "FactorValueHub: pnlu key suffix updated suffix={}",
            self.pnlu_key_suffix
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{FactorValueHub, FactorValueSnapshot};
    use crate::signal::common::TradingVenue;

    const TEST_MODEL_OUTPUT_NAME: &str = "binance_futures_direction_model";
    const TEST_MODEL_OUTPUT_SERVICE: &str = "model_output/binance_futures_direction_model";

    #[test]
    fn builds_profile_from_open_and_hedge_venues() {
        let profile = FactorValueHub::build_pnlu_profile(
            TradingVenue::OkexFutures,
            TradingVenue::BinanceFutures,
        );
        assert_eq!(profile, "okex-futures-binance-futures");
    }

    #[test]
    fn builds_pnlu_key_with_profile_suffix() {
        let key = FactorValueHub::build_pnlu_key(
            "ETHUSDT",
            "_pnlu_factor_thresholds",
            "binance-margin-binance-futures",
        );
        assert_eq!(
            key,
            "ETHUSDT_pnlu_factor_thresholds_binance-margin-binance-futures"
        );
    }

    #[test]
    fn normalizes_bare_model_output_service_name() {
        assert_eq!(
            FactorValueHub::normalize_model_output_service_name(TEST_MODEL_OUTPUT_NAME),
            Some(TEST_MODEL_OUTPUT_SERVICE.to_string())
        );
        assert_eq!(
            FactorValueHub::normalize_model_output_service_name(TEST_MODEL_OUTPUT_SERVICE),
            Some(TEST_MODEL_OUTPUT_SERVICE.to_string())
        );
    }

    #[test]
    fn ignores_disabled_model_output_service_name() {
        assert_eq!(
            FactorValueHub::normalize_model_output_service_name(""),
            None
        );
        assert_eq!(
            FactorValueHub::normalize_model_output_service_name("-"),
            None
        );
    }

    #[test]
    fn factor_snapshot_is_fresh_within_max_age() {
        let snapshot = FactorValueSnapshot {
            value: 1.0,
            ready: true,
            timestamp_ms: 9_500,
            factor_index: 0,
        };
        assert_eq!(
            FactorValueHub::validate_factor_snapshot(&snapshot, 10_000, 10_000),
            Ok(())
        );
    }

    #[test]
    fn factor_snapshot_is_rejected_when_stale() {
        let snapshot = FactorValueSnapshot {
            value: 1.0,
            ready: true,
            timestamp_ms: 9_000,
            factor_index: 0,
        };
        assert_eq!(
            FactorValueHub::validate_factor_snapshot(&snapshot, 20_000, 10_000),
            Err("factor_ipc_timeout(age_ms=11000 max_age_ms=10000)".to_string())
        );
    }
}
