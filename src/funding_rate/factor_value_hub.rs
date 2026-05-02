use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{error, info, warn};
use redis::Commands;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

use crate::common::mkt_msg::FactorValueMsg;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::time_util::get_timestamp_us;
use crate::common::trade_flow_feature_msg::{
    TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_HISTORY_SIZE, TRADE_FLOW_FEATURE_MAX_BYTES,
};
use crate::factor_pub::factor_index::{factor_name_to_channel, factor_name_to_index};
use crate::funding_rate::inline_volatility::{
    observe_inline_tradecount, observe_inline_volatility, InlineVolatilitySnapshot,
};
use crate::funding_rate::model_output_hub::ModelOutputHub;
use crate::signal::common::TradingVenue;

const FACTOR_VALUE_PAYLOAD_MAX_BYTES: usize = 256;
const FACTOR_VALUE_SUBSCRIBER_BUFFER_SIZE: usize = 8192;
const DEFAULT_PNLU_MAX_AGE_SECS: i64 = 30 * 60;
const FACTOR_VALUE_ISSUE_LOG_INTERVAL_SECS: u64 = 10;
const TRADE_FLOW_FEATURE_SUBSCRIBER_BUFFER_SIZE: usize = 1024;
const TRADE_FLOW_FEATURE_COUNT_INDEX: usize = 7;
const TRADECOUNT_ROLLING_MEAN_WINDOW: usize = 30;
const TRADECOUNT_ROLLING_MEAN_MIN_PERIODS: usize = 25;

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
    pub target_age_secs: Option<i64>,
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
            target_age_secs: None,
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

#[derive(Debug, Clone, Copy)]
struct FactorValueSnapshot {
    value: f64,
    ready: bool,
    timestamp_ms: i64,
    factor_index: u16,
}

struct FactorIssueLogState {
    note: String,
    last_log_at: Instant,
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
    inline_volatility_percentile: Option<f64>,
    factor_value_service_name: String,
    factor_value_max_age_ms: i64,
    factor_value_sub: Subscriber<ipc::Service, [u8; FACTOR_VALUE_PAYLOAD_MAX_BYTES], ()>,
    trade_flow_feature_sub: Subscriber<ipc::Service, [u8; TRADE_FLOW_FEATURE_MAX_BYTES], ()>,
    factor_value_cache: HashMap<(u16, String), FactorValueSnapshot>,
    last_valid_factor_value_cache: HashMap<(u16, String), FactorValueSnapshot>,
    tradecount_windows: HashMap<String, VecDeque<f64>>,
    latest_tradecount_means: HashMap<String, f64>,
    factor_issue_log_state: HashMap<String, FactorIssueLogState>,
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
        inline_volatility_percentile: Option<f64>,
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
        let trade_flow_feature_sub = Self::create_trade_flow_feature_subscriber(node, hedge_venue)?;
        let pnlu_max_age_secs = if pnlu_max_age_secs > 0 {
            pnlu_max_age_secs
        } else {
            DEFAULT_PNLU_MAX_AGE_SECS
        };
        let pnlu_profile = Self::build_pnlu_profile(open_venue, hedge_venue);
        let factor_value_service_name = format!(
            "factor_pub/{}/{}",
            hedge_venue.data_pub_slug(),
            factor_name_to_channel(target_factor_name)
        );

        Ok(Self {
            hedge_venue,
            pnlu_profile,
            target_factor_index,
            target_factor_key_prefix: target_factor_key_prefix.to_string(),
            inline_volatility_percentile: inline_volatility_percentile
                .filter(|v| v.is_finite())
                .map(|v| v.clamp(0.0, 100.0)),
            factor_value_service_name,
            factor_value_max_age_ms,
            factor_value_sub,
            trade_flow_feature_sub,
            factor_value_cache: HashMap::new(),
            last_valid_factor_value_cache: HashMap::new(),
            tradecount_windows: HashMap::new(),
            latest_tradecount_means: HashMap::new(),
            factor_issue_log_state: HashMap::new(),
            pnlu_redis,
            pnlu_key_suffix,
            pnlu_max_age_secs,
        })
    }

    pub fn set_inline_volatility_percentile(&mut self, percentile: Option<f64>) {
        self.inline_volatility_percentile = percentile
            .filter(|v| v.is_finite())
            .map(|v| v.clamp(0.0, 100.0));
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
        let service_name = format!(
            "factor_pub/{}/{}",
            hedge_venue.data_pub_slug(),
            factor_name_to_channel(factor_name)
        );
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

    fn create_trade_flow_feature_subscriber(
        node: &Node<ipc::Service>,
        venue: TradingVenue,
    ) -> Result<Subscriber<ipc::Service, [u8; TRADE_FLOW_FEATURE_MAX_BYTES], ()>> {
        let service_name = format!("factor_pub/{}/trade_flow_feature", venue.data_pub_slug());
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_FLOW_FEATURE_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(TRADE_FLOW_FEATURE_HISTORY_SIZE)
            .subscriber_max_buffer_size(TRADE_FLOW_FEATURE_SUBSCRIBER_BUFFER_SIZE)
            .open()
            .with_context(|| {
                format!("failed to open trade_flow_feature subscriber service={service_name}")
            })?;
        service
            .subscriber_builder()
            .buffer_size(TRADE_FLOW_FEATURE_SUBSCRIBER_BUFFER_SIZE)
            .create()
            .context("failed to create trade_flow_feature subscriber")
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
                            if ready && value.is_finite() && value > 0.0 {
                                let should_update_last_valid = self
                                    .last_valid_factor_value_cache
                                    .get(&cache_key)
                                    .map(|prev| snapshot.timestamp_ms >= prev.timestamp_ms)
                                    .unwrap_or(true);
                                if should_update_last_valid {
                                    self.last_valid_factor_value_cache
                                        .insert(cache_key, snapshot);
                                }
                            }
                        }

                        if factor_index == self.target_factor_index
                            && ready
                            && value.is_finite()
                            && should_update
                        {
                            if let Some(percentile) = self.inline_volatility_percentile {
                                let _ = observe_inline_volatility(
                                    &symbol_key,
                                    value,
                                    percentile,
                                    msg.timestamp_ms,
                                );
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

    pub(crate) fn poll_factor_value_updates_with_inline_sampling(
        &mut self,
        percentile: Option<f64>,
    ) -> Vec<(String, InlineVolatilitySnapshot)> {
        let mut sampled = Vec::new();
        let Some(percentile) = percentile.filter(|v| v.is_finite()) else {
            self.poll_factor_value_updates();
            return sampled;
        };

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
                            if ready && value.is_finite() && value > 0.0 {
                                let should_update_last_valid = self
                                    .last_valid_factor_value_cache
                                    .get(&cache_key)
                                    .map(|prev| snapshot.timestamp_ms >= prev.timestamp_ms)
                                    .unwrap_or(true);
                                if should_update_last_valid {
                                    self.last_valid_factor_value_cache
                                        .insert(cache_key, snapshot);
                                }
                            }
                        }

                        if factor_index == self.target_factor_index && ready && value.is_finite() {
                            let inline_snapshot = observe_inline_volatility(
                                &symbol_key,
                                value,
                                percentile,
                                msg.timestamp_ms,
                            );
                            sampled.push((symbol_key.clone(), inline_snapshot));
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

        sampled
    }

    pub(crate) fn poll_trade_flow_tradecount_updates(
        &mut self,
        percentile: Option<f64>,
    ) -> Vec<(String, InlineVolatilitySnapshot)> {
        let mut sampled = Vec::new();
        let Some(percentile) = percentile.filter(|v| v.is_finite()) else {
            return sampled;
        };

        loop {
            match self.trade_flow_feature_sub.receive() {
                Ok(Some(sample)) => {
                    let payload = sample.payload();
                    if payload.iter().all(|&b| b == 0) {
                        continue;
                    }
                    let msg = match TradeFlowFeatureMsg::from_bytes(payload) {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!(
                                "FactorValueHub: parse trade_flow_feature payload failed: {}",
                                err
                            );
                            continue;
                        }
                    };
                    let Some(tradecount) = msg
                        .values
                        .get(TRADE_FLOW_FEATURE_COUNT_INDEX)
                        .copied()
                        .filter(|v| v.is_finite())
                    else {
                        continue;
                    };
                    let symbol_key = normalize_symbol_for_venue(&msg.symbol, self.hedge_venue);
                    let window = self
                        .tradecount_windows
                        .entry(symbol_key.clone())
                        .or_insert_with(|| VecDeque::with_capacity(TRADECOUNT_ROLLING_MEAN_WINDOW));
                    if window.len() == TRADECOUNT_ROLLING_MEAN_WINDOW {
                        let _ = window.pop_front();
                    }
                    window.push_back(tradecount);
                    if window.len() < TRADECOUNT_ROLLING_MEAN_MIN_PERIODS {
                        continue;
                    }
                    let mean = window.iter().sum::<f64>() / window.len() as f64;
                    self.latest_tradecount_means
                        .insert(symbol_key.clone(), mean);
                    let snapshot = observe_inline_tradecount(&symbol_key, mean, percentile, msg.ts);
                    sampled.push((symbol_key, snapshot));
                }
                Ok(None) => break,
                Err(err) => {
                    error!(
                        "FactorValueHub: trade_flow_feature subscriber receive error venue={} err={}",
                        self.hedge_venue.data_pub_slug(),
                        err
                    );
                    break;
                }
            }
        }

        sampled
    }

    pub(crate) fn latest_tradecount_mean(
        &self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> Option<f64> {
        let symbol_key = normalize_symbol_for_venue(hedge_symbol, hedge_venue);
        self.latest_tradecount_means
            .get(&symbol_key)
            .copied()
            .filter(|v| v.is_finite())
    }

    pub fn lookup_factor_value_with_last_valid_fallback(
        &mut self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> FactorValueLookupResult {
        let strict = self.lookup_factor_value(hedge_symbol, hedge_venue);
        if strict
            .target_factor_value
            .filter(|value| value.is_finite() && *value > 0.0)
            .is_some()
        {
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
        let target_ts_us = target_ts.and_then(Self::normalize_pnlu_ts_us);
        let target_age_secs = match target_ts_us {
            Some(target_ts_us) if target_ts_us <= now_us => {
                Some((now_us - target_ts_us) / 1_000_000)
            }
            Some(_) => return PnluCheckResult::fail("target_ts_in_future"),
            None => None,
        };

        let ts_fresh = match age_secs {
            Some(age) => age <= self.pnlu_max_age_secs,
            None => false,
        };
        let target_ts_fresh = match target_age_secs {
            Some(age) => age <= self.pnlu_max_age_secs,
            None => false,
        };
        let factor_ok = match (factor, threshold) {
            (Some(f), Some(t)) => f > t,
            _ => false,
        };

        let ok = ts_fresh
            && target_ts_fresh
            && ready == Some(true)
            && factor.is_some()
            && threshold.is_some()
            && factor_ok;
        let reason = if ok {
            "ok".to_string()
        } else if ts_us.is_none() {
            "missing_ts".to_string()
        } else if !ts_fresh {
            "ts_timeout".to_string()
        } else if target_ts_us.is_none() {
            "missing_target_ts".to_string()
        } else if !target_ts_fresh {
            "target_ts_timeout".to_string()
        } else if ready != Some(true) {
            match ready {
                Some(false) => "ready_false".to_string(),
                None => "missing_ready".to_string(),
                Some(true) => unreachable!("ready=true should have been handled by ok branch"),
            }
        } else if factor.is_none() {
            "missing_factor".to_string()
        } else if threshold.is_none() {
            "missing_threshold".to_string()
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
            target_age_secs,
            ready,
            quantiles,
            thresholds,
        }
    }

    pub fn evaluate_environment_signal(
        &mut self,
        model_output_hub: &ModelOutputHub,
        environment_model_service: Option<&str>,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
        model_true_threshold: f64,
        pnlu_symbol_key: &str,
        now_us: i64,
    ) -> EnvironmentSignalResult {
        if let Some(env_service) = environment_model_service {
            let lookup = model_output_hub.cached_score(env_service, hedge_symbol, hedge_venue);
            if !matches!(lookup.note.as_str(), "service_disabled") {
                let (allow_open, note) = match (lookup.score, lookup.note.as_str()) {
                    (Some(score), _) if score.is_finite() => {
                        let allow = score >= model_true_threshold;
                        (
                            allow,
                            if allow {
                                "model_score_ge_threshold".to_string()
                            } else {
                                "model_score_lt_threshold".to_string()
                            },
                        )
                    }
                    _ => (false, lookup.note.clone()),
                };
                return EnvironmentSignalResult {
                    source: EnvironmentSignalSource::ModelOutput,
                    allow_open,
                    class_label: if allow_open { 1 } else { 0 },
                    service_name: Some(lookup.service_name),
                    symbol_key: lookup.symbol_key,
                    score: lookup.score,
                    score_quantile: lookup.score_quantile,
                    threshold: Some(model_true_threshold),
                    note,
                };
            }
        }

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
