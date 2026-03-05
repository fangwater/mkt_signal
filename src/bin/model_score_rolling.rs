//! model_score_rolling
//!
//! - subscribe ModelMsg from model_output/*
//! - compute rolling score quantiles
//! - periodically write Redis hash: model_score_rolling_thresholds_{model_name}

use anyhow::{Context, Result};
use clap::Parser;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use mkt_signal::common::mkt_msg::{ModelMsg, MODEL_STATUS_OK};
use mkt_signal::common::redis_client::{RedisClient, RedisSettings};
use mkt_signal::factor_pub::model_pub::publisher::MODEL_PAYLOAD_MAX_BYTES;
use mkt_signal::rolling_metrics::quantile::quantiles_linear_select_unstable;
use mkt_signal::rolling_metrics::ring::RingBuffer;

const IDLE_SLEEP_MICROS: u64 = 200;
const LOG_INTERVAL_SECS: u64 = 60;

#[derive(Parser, Debug)]
#[command(name = "model_score_rolling")]
#[command(about = "Subscribe model_output, compute rolling score thresholds, write Redis")]
struct Args {
    /// Model name, used to render service/key templates
    model_name: String,
}

#[derive(Debug, Clone)]
struct BootstrapConfig {
    redis: RedisSettings,

    input_service_template: String,

    params_hash_key_template: String,

    output_hash_key_template: String,

    refresh_sec: u64,

    reload_param_sec: u64,

    max_length: usize,

    rolling_window: usize,

    min_periods: usize,

    quantiles: Vec<f32>,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            redis: RedisSettings::default(),
            input_service_template: default_input_service_template(),
            params_hash_key_template: default_params_hash_key_template(),
            output_hash_key_template: default_output_hash_key_template(),
            refresh_sec: default_refresh_sec(),
            reload_param_sec: default_reload_param_sec(),
            max_length: default_max_length(),
            rolling_window: default_rolling_window(),
            min_periods: default_min_periods(),
            quantiles: default_quantiles(),
        }
    }
}

impl BootstrapConfig {
    fn build_runtime(&self, model_name: &str) -> Result<RuntimeConfig> {
        let normalized_model = normalize_model_name(model_name)?;
        let input_service = self
            .input_service_template
            .replace("{model_name}", &normalized_model);
        let params_hash_key = self
            .params_hash_key_template
            .replace("{model_name}", &normalized_model);
        let output_hash_key = self
            .output_hash_key_template
            .replace("{model_name}", &normalized_model);

        let quantiles = normalize_quantiles(self.quantiles.clone())?;
        let max_length = self.max_length.max(1);
        let rolling_window = self.rolling_window.min(max_length).max(1);
        let min_periods = self.min_periods.min(rolling_window).max(1);

        Ok(RuntimeConfig {
            input_services: vec![input_service],
            params_hash_key,
            output_hash_key,
            refresh_sec: self.refresh_sec.max(1),
            reload_param_sec: self.reload_param_sec.max(1),
            max_length,
            rolling_window,
            min_periods,
            quantiles,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RuntimeConfig {
    input_services: Vec<String>,
    params_hash_key: String,
    output_hash_key: String,
    refresh_sec: u64,
    reload_param_sec: u64,
    max_length: usize,
    rolling_window: usize,
    min_periods: usize,
    quantiles: Vec<f32>,
}

impl RuntimeConfig {
    fn apply_overrides(&mut self, map: &HashMap<String, String>) -> Result<Vec<String>> {
        if map.is_empty() {
            return Ok(Vec::new());
        }

        let mut changed = Vec::new();

        if let Some(v) = map.get("input_services") {
            let parsed = parse_input_services(v)?;
            if !parsed.is_empty() && parsed != self.input_services {
                changed.push(format!(
                    "input_services {:?}->{:?}",
                    self.input_services, parsed
                ));
                self.input_services = parsed;
            }
        } else if let Some(v) = map.get("input_service") {
            let one = v.trim();
            if !one.is_empty() {
                let parsed = vec![one.to_string()];
                if parsed != self.input_services {
                    changed.push(format!(
                        "input_services {:?}->{:?}",
                        self.input_services, parsed
                    ));
                    self.input_services = parsed;
                }
            }
        }

        if let Some(v) = map.get("output_hash_key") {
            let trimmed = v.trim();
            if !trimmed.is_empty() && trimmed != self.output_hash_key {
                changed.push(format!(
                    "output_hash_key {}->{}",
                    self.output_hash_key, trimmed
                ));
                self.output_hash_key = trimmed.to_string();
            }
        }

        if let Some(v) = parse_u64(map, "refresh_sec") {
            let next = v.max(1);
            if next != self.refresh_sec {
                changed.push(format!("refresh_sec {}->{}", self.refresh_sec, next));
                self.refresh_sec = next;
            }
        }

        if let Some(v) = parse_u64(map, "reload_param_sec") {
            let next = v.max(1);
            if next != self.reload_param_sec {
                changed.push(format!(
                    "reload_param_sec {}->{}",
                    self.reload_param_sec, next
                ));
                self.reload_param_sec = next;
            }
        }

        if let Some(v) = parse_usize(map, "max_length") {
            let next = v.max(1);
            if next != self.max_length {
                changed.push(format!("max_length {}->{}", self.max_length, next));
                self.max_length = next;
            }
        }

        if let Some(v) = parse_usize(map, "rolling_window") {
            let next = v.max(1);
            if next != self.rolling_window {
                changed.push(format!("rolling_window {}->{}", self.rolling_window, next));
                self.rolling_window = next;
            }
        }

        if let Some(v) = parse_usize(map, "min_periods") {
            let next = v.max(1);
            if next != self.min_periods {
                changed.push(format!("min_periods {}->{}", self.min_periods, next));
                self.min_periods = next;
            }
        }

        if let Some(v) = map.get("quantiles") {
            let parsed = parse_quantiles_text(v)?;
            if !parsed.is_empty() && parsed != self.quantiles {
                changed.push(format!("quantiles {:?}->{:?}", self.quantiles, parsed));
                self.quantiles = parsed;
            }
        }

        self.max_length = self.max_length.max(1);
        self.rolling_window = self.rolling_window.min(self.max_length).max(1);
        self.min_periods = self.min_periods.min(self.rolling_window).max(1);
        self.input_services = normalize_services(self.input_services.clone());

        if self.input_services.is_empty() {
            anyhow::bail!("input_services is empty after overrides");
        }

        Ok(changed)
    }
}

#[derive(Debug, Serialize)]
struct ScoreThresholdPayload {
    model_name: String,
    symbol: String,
    ts: i64,
    target_ts: i64,
    factor: Option<f64>,
    quantiles: Vec<f32>,
    thresholds: Vec<f64>,
    ready: bool,
    sample_size: usize,
    window_size: usize,
    min_periods: usize,
    source: String,
    score_ts_out_ms: Option<i64>,
}

struct ModelOutputSub {
    service_name: String,
    subscriber: Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>,
}

struct SymbolState {
    ring: RingBuffer,
    latest_score: AtomicU64,
    latest_ts_out_ms: AtomicI64,
}

impl SymbolState {
    fn new(capacity: usize) -> Self {
        Self {
            ring: RingBuffer::new(capacity.max(1)),
            latest_score: AtomicU64::new(f64::NAN.to_bits()),
            latest_ts_out_ms: AtomicI64::new(0),
        }
    }

    fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    fn push(&self, score: f64, ts_out_ms: i64) {
        self.ring.push(score as f32);
        self.latest_score.store(score.to_bits(), Ordering::Release);
        self.latest_ts_out_ms.store(ts_out_ms, Ordering::Release);
    }

    fn latest_score(&self) -> Option<f64> {
        let v = f64::from_bits(self.latest_score.load(Ordering::Acquire));
        if v.is_finite() {
            Some(v)
        } else {
            None
        }
    }

    fn latest_ts_out_ms(&self) -> Option<i64> {
        let ts = self.latest_ts_out_ms.load(Ordering::Acquire);
        if ts > 0 {
            Some(ts)
        } else {
            None
        }
    }
}

#[derive(Default)]
struct AppStats {
    recv_total: u64,
    parse_fail: u64,
    status_skip: u64,
    invalid_score_skip: u64,
    push_ok: u64,
    flush_ok: u64,
    flush_fail: u64,
    reload_ok: u64,
    reload_fail: u64,
    last_flush_symbols: usize,
    last_flush_ready: usize,
}

fn default_input_service_template() -> String {
    "model_output/{model_name}".to_string()
}

fn default_params_hash_key_template() -> String {
    "model_score_rolling_params_{model_name}".to_string()
}

fn default_output_hash_key_template() -> String {
    "model_score_rolling_thresholds_{model_name}".to_string()
}

fn default_refresh_sec() -> u64 {
    30
}

fn default_reload_param_sec() -> u64 {
    3
}

fn default_max_length() -> usize {
    150_000
}

fn default_rolling_window() -> usize {
    17_800
}

fn default_min_periods() -> usize {
    100
}

fn default_quantiles() -> Vec<f32> {
    vec![0.9, 0.8, 0.2, 0.1]
}

fn normalize_model_name(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("model_name must not be empty");
    }
    Ok(trimmed.to_string())
}

fn sanitize_node_suffix(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "default".to_string();
    }

    trimmed
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn normalize_symbol(raw: &str) -> String {
    raw.trim().to_uppercase()
}

fn parse_u64(map: &HashMap<String, String>, key: &str) -> Option<u64> {
    map.get(key)?.trim().parse::<u64>().ok()
}

fn parse_usize(map: &HashMap<String, String>, key: &str) -> Option<usize> {
    map.get(key)?.trim().parse::<usize>().ok()
}

fn normalize_quantiles(values: Vec<f32>) -> Result<Vec<f32>> {
    let mut cleaned = values
        .into_iter()
        .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0)
        .collect::<Vec<_>>();
    cleaned.sort_by(|a, b| a.partial_cmp(b).unwrap_or(a.total_cmp(b)));
    cleaned.dedup_by(|a, b| (*a - *b).abs() <= f32::EPSILON);
    if cleaned.is_empty() {
        anyhow::bail!("quantiles empty after normalization");
    }
    Ok(cleaned)
}

fn parse_quantiles_text(raw: &str) -> Result<Vec<f32>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("quantiles is empty");
    }

    if trimmed.starts_with('[') {
        let parsed = serde_json::from_str::<Vec<f32>>(trimmed)
            .with_context(|| format!("parse quantiles json failed: {}", trimmed))?;
        return normalize_quantiles(parsed);
    }

    let parsed = trimmed
        .split(',')
        .map(|x| x.trim())
        .filter(|x| !x.is_empty())
        .map(|x| {
            x.parse::<f32>()
                .with_context(|| format!("invalid quantile value: {}", x))
        })
        .collect::<Result<Vec<_>>>()?;
    normalize_quantiles(parsed)
}

fn normalize_services(services: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for item in services {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let s = trimmed.to_string();
        if !out.iter().any(|x| x == &s) {
            out.push(s);
        }
    }
    out
}

fn parse_input_services(raw: &str) -> Result<Vec<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let parsed = if trimmed.starts_with('[') {
        let list = serde_json::from_str::<Vec<String>>(trimmed)
            .with_context(|| format!("parse input_services json failed: {}", trimmed))?;
        normalize_services(list)
    } else {
        normalize_services(
            trimmed
                .split(',')
                .map(|x| x.trim().to_string())
                .collect::<Vec<_>>(),
        )
    };

    Ok(parsed)
}

fn now_ms() -> i64 {
    let Ok(duration) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };
    let ms = duration.as_millis();
    if ms > i64::MAX as u128 {
        i64::MAX
    } else {
        ms as i64
    }
}

fn get_or_insert_state(
    states: &DashMap<String, Arc<SymbolState>>,
    symbol: &str,
    capacity: usize,
) -> Arc<SymbolState> {
    match states.entry(symbol.to_string()) {
        Entry::Occupied(mut occ) => {
            if occ.get().capacity() != capacity {
                let replacement = Arc::new(SymbolState::new(capacity));
                *occ.get_mut() = replacement.clone();
                replacement
            } else {
                occ.get().clone()
            }
        }
        Entry::Vacant(vac) => vac.insert(Arc::new(SymbolState::new(capacity))).clone(),
    }
}

fn create_subscribers(
    node: &Node<ipc::Service>,
    services: &[String],
) -> Result<Vec<ModelOutputSub>> {
    let mut out = Vec::new();
    for service_name in services {
        let service = node
            .service_builder(&ServiceName::new(service_name)?)
            .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(128)
            .subscriber_max_buffer_size(1024)
            .open_or_create()?;

        let subscriber = service.subscriber_builder().buffer_size(1024).create()?;
        info!("model_score_rolling subscribed service={}", service_name);
        out.push(ModelOutputSub {
            service_name: service_name.clone(),
            subscriber,
        });
    }

    if out.is_empty() {
        anyhow::bail!("no input services configured");
    }
    Ok(out)
}

fn process_model_payload(
    payload: &[u8],
    cfg: &RuntimeConfig,
    states: &DashMap<String, Arc<SymbolState>>,
    stats: &mut AppStats,
) {
    if payload.iter().all(|&b| b == 0) {
        return;
    }

    let msg = match ModelMsg::from_bytes(payload) {
        Ok(v) => v,
        Err(err) => {
            stats.parse_fail = stats.parse_fail.saturating_add(1);
            warn!("model_score_rolling parse ModelMsg failed: {}", err);
            return;
        }
    };

    if msg.status != MODEL_STATUS_OK {
        stats.status_skip = stats.status_skip.saturating_add(1);
        return;
    }

    if !msg.score.is_finite() {
        stats.invalid_score_skip = stats.invalid_score_skip.saturating_add(1);
        return;
    }

    let symbol = normalize_symbol(&msg.symbol);
    if symbol.is_empty() {
        stats.invalid_score_skip = stats.invalid_score_skip.saturating_add(1);
        return;
    }

    let state = get_or_insert_state(states, &symbol, cfg.max_length);
    state.push(msg.score, msg.ts_out_ms);
    stats.push_ok = stats.push_ok.saturating_add(1);
}

async fn flush_thresholds_to_redis(
    redis: &mut RedisClient,
    model_name: &str,
    cfg: &RuntimeConfig,
    states: &DashMap<String, Arc<SymbolState>>,
    last_fields: &mut HashSet<String>,
    stats: &mut AppStats,
) -> Result<()> {
    let mut current_fields = HashSet::new();
    let mut payloads: Vec<(String, String)> = Vec::new();
    let mut ready_symbols = 0usize;
    let mut buf = Vec::<f32>::new();
    let now = now_ms();
    let source = cfg.input_services.join(",");

    for entry in states.iter() {
        let symbol = entry.key().clone();
        let state = entry.value().clone();

        let sample_size = state.ring.copy_latest(cfg.rolling_window, &mut buf);
        let ready = sample_size >= cfg.min_periods;

        let thresholds = if ready {
            let mut work = buf.clone();
            let values = quantiles_linear_select_unstable(work.as_mut_slice(), &cfg.quantiles);
            values
                .into_iter()
                .filter(|v| v.is_finite())
                .map(|v| v as f64)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        if ready {
            ready_symbols += 1;
        }

        let latest_ts = state.latest_ts_out_ms();
        let payload = ScoreThresholdPayload {
            model_name: model_name.to_string(),
            symbol: symbol.clone(),
            ts: now,
            target_ts: latest_ts.unwrap_or(now),
            factor: state.latest_score(),
            quantiles: cfg.quantiles.clone(),
            thresholds,
            ready,
            sample_size,
            window_size: cfg.rolling_window,
            min_periods: cfg.min_periods,
            source: source.clone(),
            score_ts_out_ms: latest_ts,
        };

        let text = serde_json::to_string(&payload)
            .with_context(|| format!("serialize threshold payload failed: symbol={}", symbol))?;

        current_fields.insert(symbol.clone());
        payloads.push((symbol, text));
    }

    let removals = last_fields
        .difference(&current_fields)
        .cloned()
        .collect::<Vec<_>>();

    *last_fields = current_fields;

    redis
        .hset_multiple_str(&cfg.output_hash_key, &payloads)
        .await
        .with_context(|| {
            format!(
                "HSET thresholds failed: key={} fields={}",
                cfg.output_hash_key,
                payloads.len()
            )
        })?;

    redis
        .hdel_fields(&cfg.output_hash_key, &removals)
        .await
        .with_context(|| {
            format!(
                "HDEL thresholds failed: key={} fields={}",
                cfg.output_hash_key,
                removals.len()
            )
        })?;

    stats.flush_ok = stats.flush_ok.saturating_add(1);
    stats.last_flush_symbols = payloads.len();
    stats.last_flush_ready = ready_symbols;

    info!(
        "model_score_rolling flush done: model={} key={} symbols={} ready={} removed={}",
        model_name,
        cfg.output_hash_key,
        payloads.len(),
        ready_symbols,
        removals.len()
    );

    Ok(())
}

fn log_stats(model_name: &str, cfg: &RuntimeConfig, states: usize, stats: &AppStats) {
    info!(
        "model_score_rolling[{}] recv={} push={} parse_fail={} status_skip={} invalid_score={} flush_ok={} flush_fail={} reload_ok={} reload_fail={} symbols={} last_flush_symbols={} ready={} refresh={}s window={} min={} quantiles={:?}",
        model_name,
        stats.recv_total,
        stats.push_ok,
        stats.parse_fail,
        stats.status_skip,
        stats.invalid_score_skip,
        stats.flush_ok,
        stats.flush_fail,
        stats.reload_ok,
        stats.reload_fail,
        states,
        stats.last_flush_symbols,
        stats.last_flush_ready,
        cfg.refresh_sec,
        cfg.rolling_window,
        cfg.min_periods,
        cfg.quantiles
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = Args::parse();
    let model_name = normalize_model_name(&args.model_name)?;

    let boot_cfg = BootstrapConfig::default();
    let mut runtime_cfg = boot_cfg.build_runtime(&model_name)?;

    let node_name = format!(
        "model_score_rolling_{}",
        sanitize_node_suffix(model_name.as_str())
    );
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let mut redis = RedisClient::connect(boot_cfg.redis.clone())
        .await
        .with_context(|| "connect redis for model_score_rolling failed")?;

    // Initial override load
    match redis.hgetall_map(&runtime_cfg.params_hash_key).await {
        Ok(map) => {
            let changes = runtime_cfg.apply_overrides(&map)?;
            if !changes.is_empty() {
                info!(
                    "model_score_rolling initial overrides applied: model={} changes={:?}",
                    model_name, changes
                );
            }
        }
        Err(err) => {
            warn!(
                "model_score_rolling initial override load failed: model={} key={} err={:#}",
                model_name, runtime_cfg.params_hash_key, err
            );
        }
    }

    let mut subscribers = create_subscribers(&node, &runtime_cfg.input_services)?;
    let states: DashMap<String, Arc<SymbolState>> = DashMap::new();
    let mut last_fields: HashSet<String> = HashSet::new();
    let mut stats = AppStats::default();

    let shutdown = CancellationToken::new();
    let shutdown_sig = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_sig.cancel();
    });

    let mut last_flush = Instant::now() - Duration::from_secs(runtime_cfg.refresh_sec.max(1));
    let mut last_reload = Instant::now() - Duration::from_secs(runtime_cfg.reload_param_sec.max(1));
    let mut last_stats_log = Instant::now();

    info!(
        "model_score_rolling started: model={} input_services={:?} output_key={} params_key={} refresh={}s reload={}s max_length={} window={} min={} quantiles={:?}",
        model_name,
        runtime_cfg.input_services,
        runtime_cfg.output_hash_key,
        runtime_cfg.params_hash_key,
        runtime_cfg.refresh_sec,
        runtime_cfg.reload_param_sec,
        runtime_cfg.max_length,
        runtime_cfg.rolling_window,
        runtime_cfg.min_periods,
        runtime_cfg.quantiles
    );

    while !shutdown.is_cancelled() {
        let mut has_message = false;

        for sub in &subscribers {
            loop {
                match sub.subscriber.receive() {
                    Ok(Some(sample)) => {
                        has_message = true;
                        stats.recv_total = stats.recv_total.saturating_add(1);
                        process_model_payload(sample.payload(), &runtime_cfg, &states, &mut stats);
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!(
                            "model_score_rolling receive failed: service={} err={:?}",
                            sub.service_name, err
                        );
                        break;
                    }
                }
            }
        }

        if last_reload.elapsed() >= Duration::from_secs(runtime_cfg.reload_param_sec.max(1)) {
            last_reload = Instant::now();
            match redis.hgetall_map(&runtime_cfg.params_hash_key).await {
                Ok(map) => match runtime_cfg.apply_overrides(&map) {
                    Ok(changes) => {
                        if !changes.is_empty() {
                            let services_changed = subscribers
                                .iter()
                                .map(|s| s.service_name.clone())
                                .collect::<Vec<_>>()
                                != runtime_cfg.input_services;

                            if services_changed {
                                subscribers =
                                    create_subscribers(&node, &runtime_cfg.input_services)?;
                                info!(
                                    "model_score_rolling subscribers reloaded: model={} services={:?}",
                                    model_name, runtime_cfg.input_services
                                );
                            }

                            if states
                                .iter()
                                .next()
                                .map(|e| e.value().capacity())
                                .unwrap_or(runtime_cfg.max_length)
                                != runtime_cfg.max_length
                            {
                                states.clear();
                                info!(
                                    "model_score_rolling state reset due max_length change: model={} max_length={} (stale redis fields will be removed on next flush)",
                                    model_name, runtime_cfg.max_length
                                );
                            }

                            info!(
                                "model_score_rolling overrides applied: model={} changes={:?}",
                                model_name, changes
                            );
                        }
                        stats.reload_ok = stats.reload_ok.saturating_add(1);
                    }
                    Err(err) => {
                        stats.reload_fail = stats.reload_fail.saturating_add(1);
                        warn!(
                            "model_score_rolling apply overrides failed: model={} key={} err={:#}",
                            model_name, runtime_cfg.params_hash_key, err
                        );
                    }
                },
                Err(err) => {
                    stats.reload_fail = stats.reload_fail.saturating_add(1);
                    warn!(
                        "model_score_rolling reload failed: model={} key={} err={:#}",
                        model_name, runtime_cfg.params_hash_key, err
                    );
                }
            }
        }

        if last_flush.elapsed() >= Duration::from_secs(runtime_cfg.refresh_sec.max(1)) {
            last_flush = Instant::now();
            if let Err(err) = flush_thresholds_to_redis(
                &mut redis,
                &model_name,
                &runtime_cfg,
                &states,
                &mut last_fields,
                &mut stats,
            )
            .await
            {
                stats.flush_fail = stats.flush_fail.saturating_add(1);
                warn!(
                    "model_score_rolling flush failed: model={} key={} err={:#}",
                    model_name, runtime_cfg.output_hash_key, err
                );
            }
        }

        if last_stats_log.elapsed() >= Duration::from_secs(LOG_INTERVAL_SECS) {
            log_stats(&model_name, &runtime_cfg, states.len(), &stats);
            last_stats_log = Instant::now();
        }

        if !has_message {
            tokio::time::sleep(Duration::from_micros(IDLE_SLEEP_MICROS)).await;
        }
    }

    info!("model_score_rolling stopping: model={}", model_name);
    Ok(())
}
