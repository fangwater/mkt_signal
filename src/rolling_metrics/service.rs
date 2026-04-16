use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use dashmap::DashMap;
use log::{debug, info, warn};
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::rolling_metrics::config::{
    FactorConfig, RollingConfig, DEFAULT_OUTPUT_HASH_KEY, FACTOR_ASKBID, FACTOR_BIDASK,
    FACTOR_HEDGE_PREMIUM_RATE, FACTOR_OPEN_PREMIUM_RATE, FACTOR_SPREAD, FACTOR_SPREAD_FR,
};
use crate::rolling_metrics::ring::RingBuffer;

static LOG_PREFIX: OnceLock<String> = OnceLock::new();

pub fn init_log_prefix(prefix: String) {
    let _ = LOG_PREFIX.set(prefix);
}

pub fn log_prefix() -> &'static str {
    LOG_PREFIX
        .get()
        .map(|s| s.as_str())
        .unwrap_or("rolling-metrics")
}

pub type SeriesMap = DashMap<String, Arc<SymbolSeries>>;

pub struct SymbolSeries {
    pub bidask: Arc<RingBuffer>,
    pub askbid: Arc<RingBuffer>,
    pub spread: Arc<RingBuffer>,
    spread_rate: AtomicU64,
    pub open_premium_rate: Arc<RingBuffer>,
    pub hedge_premium_rate: Arc<RingBuffer>,
    pub spread_fr: Arc<RingBuffer>,
    open_premium_rate_latest: AtomicU64,
    hedge_premium_rate_latest: AtomicU64,
    spread_fr_latest: AtomicU64,
}

impl SymbolSeries {
    pub fn new(capacity: usize) -> Self {
        Self {
            bidask: Arc::new(RingBuffer::new(capacity)),
            askbid: Arc::new(RingBuffer::new(capacity)),
            spread: Arc::new(RingBuffer::new(capacity)),
            spread_rate: AtomicU64::new(f64::NAN.to_bits()),
            open_premium_rate: Arc::new(RingBuffer::new(capacity)),
            hedge_premium_rate: Arc::new(RingBuffer::new(capacity)),
            spread_fr: Arc::new(RingBuffer::new(capacity)),
            open_premium_rate_latest: AtomicU64::new(f64::NAN.to_bits()),
            hedge_premium_rate_latest: AtomicU64::new(f64::NAN.to_bits()),
            spread_fr_latest: AtomicU64::new(f64::NAN.to_bits()),
        }
    }

    pub fn set_spread_rate(&self, spread_rate: Option<f64>) {
        store_option_f64(&self.spread_rate, spread_rate);
    }

    pub fn spread_rate(&self) -> Option<f64> {
        load_option_f64(&self.spread_rate)
    }

    pub fn set_open_premium_rate_latest(&self, value: Option<f64>) {
        store_option_f64(&self.open_premium_rate_latest, value);
    }

    pub fn open_premium_rate_latest(&self) -> Option<f64> {
        load_option_f64(&self.open_premium_rate_latest)
    }

    pub fn set_hedge_premium_rate_latest(&self, value: Option<f64>) {
        store_option_f64(&self.hedge_premium_rate_latest, value);
    }

    pub fn hedge_premium_rate_latest(&self) -> Option<f64> {
        load_option_f64(&self.hedge_premium_rate_latest)
    }

    pub fn set_spread_fr_latest(&self, value: Option<f64>) {
        store_option_f64(&self.spread_fr_latest, value);
    }

    pub fn spread_fr_latest(&self) -> Option<f64> {
        load_option_f64(&self.spread_fr_latest)
    }

    pub fn ring(&self, factor: &str) -> Option<&Arc<RingBuffer>> {
        match factor {
            crate::rolling_metrics::config::FACTOR_BIDASK => Some(&self.bidask),
            crate::rolling_metrics::config::FACTOR_ASKBID => Some(&self.askbid),
            crate::rolling_metrics::config::FACTOR_SPREAD => Some(&self.spread),
            crate::rolling_metrics::config::FACTOR_OPEN_PREMIUM_RATE => {
                Some(&self.open_premium_rate)
            }
            crate::rolling_metrics::config::FACTOR_HEDGE_PREMIUM_RATE => {
                Some(&self.hedge_premium_rate)
            }
            crate::rolling_metrics::config::FACTOR_SPREAD_FR => Some(&self.spread_fr),
            _ => None,
        }
    }
}

pub fn new_series_map() -> SeriesMap {
    DashMap::new()
}

pub fn ensure_series_capacity(series_map: &SeriesMap, new_capacity: usize) {
    for mut entry in series_map.iter_mut() {
        if entry.value().bidask.capacity() != new_capacity {
            *entry.value_mut() = Arc::new(SymbolSeries::new(new_capacity));
        }
    }
}

#[derive(Debug)]
pub struct ComputeStats {
    pub total_symbols: usize,
    pub processed: usize,
    pub skipped: usize,
    pub duration_ms: u128,
}

pub struct OutputBatch {
    pub output_key: String,
    pub payloads: Vec<(String, String)>,
    pub removals: Vec<String>,
}

pub struct ComputeResult {
    pub batches: Vec<OutputBatch>,
    pub stats: ComputeStats,
}

#[derive(Serialize)]
struct QuantilePoint {
    quantile: f32,
    threshold: Option<f64>,
}

pub fn spawn_compute_thread(
    series_map: Arc<SeriesMap>,
    config: Arc<RwLock<RollingConfig>>,
    series_capacity: Arc<AtomicUsize>,
    open_venue: String,
    hedge_venue: String,
    sender: Sender<ComputeResult>,
) {
    thread::spawn(move || {
        let mut last_fields_by_hash: HashMap<String, HashSet<String>> = HashMap::new();
        let mut last_refresh_sec: u64 = 0;

        loop {
            let cfg_snapshot = { config.read().clone() };
            let desired_capacity = cfg_snapshot.max_length;
            series_capacity.store(desired_capacity, Ordering::SeqCst);
            ensure_series_capacity(&series_map, desired_capacity);

            if cfg_snapshot.refresh_sec != last_refresh_sec {
                info!(
                    "{}: compute interval refresh_sec={}s (prev={}s)",
                    log_prefix(),
                    cfg_snapshot.refresh_sec,
                    last_refresh_sec
                );
                last_refresh_sec = cfg_snapshot.refresh_sec;
            }

            let start = Instant::now();
            let now_ms = chrono::Utc::now().timestamp_millis();
            let snapshot = series_map
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect::<Vec<_>>();
            let total = snapshot.len();
            let mut all_ready_count = 0usize;
            let mut processed = 0usize;
            let mut skipped = 0usize;
            let pair_output_key = cfg_snapshot.output_hash_key.clone();
            let open_output_key = derive_single_side_output_key(
                &pair_output_key,
                &open_venue,
                &hedge_venue,
                &open_venue,
            );
            let hedge_output_key = derive_single_side_output_key(
                &pair_output_key,
                &open_venue,
                &hedge_venue,
                &hedge_venue,
            );
            let tracked_output_keys = vec![
                pair_output_key.clone(),
                open_output_key.clone(),
                hedge_output_key.clone(),
            ];
            let mut payloads_by_hash: HashMap<String, Vec<(String, String)>> = HashMap::new();
            let mut current_fields_by_hash: HashMap<String, HashSet<String>> = HashMap::new();
            for output_key in &tracked_output_keys {
                current_fields_by_hash
                    .entry(output_key.clone())
                    .or_default();
            }

            for (symbol_pair, series) in snapshot {
                let (ready_count, expected_total) =
                    factor_ready_counts(series.as_ref(), &cfg_snapshot);
                if expected_total > 0 && ready_count == expected_total {
                    all_ready_count += 1;
                }

                let entries = build_entries(
                    &symbol_pair,
                    &cfg_snapshot,
                    &series,
                    now_ms,
                    &mut processed,
                    &mut skipped,
                    &pair_output_key,
                    &open_output_key,
                    &hedge_output_key,
                    &open_venue,
                    &hedge_venue,
                );

                for (output_key, field, json) in entries {
                    current_fields_by_hash
                        .entry(output_key.clone())
                        .or_default()
                        .insert(field.clone());
                    payloads_by_hash
                        .entry(output_key)
                        .or_default()
                        .push((field, json));
                }
            }

            let mut batch_keys: HashSet<String> = tracked_output_keys.into_iter().collect();
            batch_keys.extend(last_fields_by_hash.keys().cloned());

            let mut batches = Vec::new();
            for output_key in batch_keys {
                let current_fields = current_fields_by_hash
                    .get(&output_key)
                    .cloned()
                    .unwrap_or_default();
                let last_fields = last_fields_by_hash
                    .get(&output_key)
                    .cloned()
                    .unwrap_or_default();
                let removals: Vec<String> =
                    last_fields.difference(&current_fields).cloned().collect();
                let payloads = payloads_by_hash.remove(&output_key).unwrap_or_default();
                batches.push(OutputBatch {
                    output_key,
                    payloads,
                    removals,
                });
            }
            last_fields_by_hash = current_fields_by_hash;

            let duration = start.elapsed();
            let stats = ComputeStats {
                total_symbols: total,
                processed,
                skipped,
                duration_ms: duration.as_millis(),
            };

            info!(
                "{}: computed {} symbols (processed={}, skipped={}) in {:?}",
                log_prefix(),
                total,
                processed,
                skipped,
                duration
            );

            let coverage_rows = build_factor_coverage_rows(total, all_ready_count);
            let coverage_table =
                build_three_line_table_str(["metric", "ready/total", "percent"], &coverage_rows);
            info!("{}: factor coverage\n{}", log_prefix(), coverage_table);

            if sender.send(ComputeResult { batches, stats }).is_err() {
                warn!(
                    "{}: compute thread exiting because receiver dropped",
                    log_prefix()
                );
                break;
            }

            let interval = cfg_snapshot.interval();
            sleep_until(interval, start);
        }
    });
}

fn build_entries(
    symbol_pair: &str,
    config: &RollingConfig,
    series: &Arc<SymbolSeries>,
    now_ms: i64,
    processed: &mut usize,
    skipped: &mut usize,
    pair_output_key: &str,
    open_output_key: &str,
    hedge_output_key: &str,
    open_venue: &str,
    hedge_venue: &str,
) -> Vec<(String, String, String)> {
    let parts: Vec<&str> = symbol_pair.split("::").collect();
    let base_symbol = parts.last().copied().unwrap_or(symbol_pair);

    let latest_bidask = series.bidask.last();
    let latest_askbid = series.askbid.last();
    let spread_rate = series.spread_rate();
    let latest_open_premium_rate = series.open_premium_rate_latest();
    let latest_hedge_premium_rate = series.hedge_premium_rate_latest();
    let latest_spread_fr = series.spread_fr_latest();

    let mut bidask_quantiles: Vec<QuantilePoint> = Vec::new();
    let mut askbid_quantiles: Vec<QuantilePoint> = Vec::new();
    let mut spread_quantiles: Vec<QuantilePoint> = Vec::new();
    let mut open_premium_rate_quantiles: Vec<QuantilePoint> = Vec::new();
    let mut hedge_premium_rate_quantiles: Vec<QuantilePoint> = Vec::new();
    let mut spread_fr_quantiles: Vec<QuantilePoint> = Vec::new();
    let mut sample_counts: Vec<usize> = Vec::new();
    let mut factors_with_quantiles = 0usize;
    let mut factors_ready = 0usize;
    let mut ready_factors: Vec<&str> = Vec::new();
    let mut missing_ready: Vec<&str> = Vec::new();
    let mut factor_counts: HashMap<String, usize> = HashMap::new();

    for (factor_name, factor_cfg) in config.factors_iter() {
        let Some(ring) = series.ring(factor_name) else {
            continue;
        };
        let (count, points, ready) = match factor_name {
            FACTOR_BIDASK
            | FACTOR_ASKBID
            | FACTOR_SPREAD
            | FACTOR_OPEN_PREMIUM_RATE
            | FACTOR_HEDGE_PREMIUM_RATE
            | FACTOR_SPREAD_FR => compute_factor_quantiles(ring.as_ref(), factor_cfg),
            _ => continue,
        };

        if !factor_cfg.quantiles.is_empty() {
            factors_with_quantiles += 1;
            if ready {
                factors_ready += 1;
                ready_factors.push(factor_name);
            } else {
                missing_ready.push(factor_name);
            }
            sample_counts.push(count);
        }
        factor_counts.insert(factor_name.to_string(), count);

        match factor_name {
            FACTOR_BIDASK => bidask_quantiles = points,
            FACTOR_ASKBID => askbid_quantiles = points,
            FACTOR_SPREAD => spread_quantiles = points,
            FACTOR_OPEN_PREMIUM_RATE => open_premium_rate_quantiles = points,
            FACTOR_HEDGE_PREMIUM_RATE => hedge_premium_rate_quantiles = points,
            FACTOR_SPREAD_FR => spread_fr_quantiles = points,
            _ => {}
        }
    }

    if factors_with_quantiles > 0 {
        if factors_ready == factors_with_quantiles {
            *processed += 1;
            debug!(
                "{}: symbol {} factors refreshed {:?}",
                log_prefix(),
                base_symbol,
                ready_factors
            );
        } else {
            *skipped += 1;
            debug!(
                "{}: symbol {} insufficient samples for factors {:?}",
                log_prefix(),
                base_symbol,
                missing_ready
            );
        }
    } else {
        *processed += 1;
    }

    if sample_counts.is_empty() {
        let fallback = config
            .factors_iter()
            .filter_map(|(factor_name, _)| series.ring(factor_name).map(|ring| ring.len()))
            .min()
            .unwrap_or(0);
        sample_counts.push(fallback);
    }
    let sample_size = sample_counts.into_iter().min().unwrap_or(0);

    if pair_output_key.is_empty()
        || open_output_key.is_empty()
        || hedge_output_key.is_empty()
        || open_venue.is_empty()
        || hedge_venue.is_empty()
    {
        return Vec::new();
    }

    let pair_field = symbol_pair.to_string();
    let open_field = same_venue_symbol_pair(open_venue, base_symbol);
    let hedge_field = same_venue_symbol_pair(hedge_venue, base_symbol);
    let open_sample_size = sample_size_for_factors(
        series.as_ref(),
        config,
        &[FACTOR_OPEN_PREMIUM_RATE],
        &factor_counts,
    );
    let hedge_sample_size = sample_size_for_factors(
        series.as_ref(),
        config,
        &[FACTOR_HEDGE_PREMIUM_RATE],
        &factor_counts,
    );

    let mut pair_payload = base_payload_map(symbol_pair, base_symbol, now_ms, sample_size);
    insert_optional_f64(
        &mut pair_payload,
        "bidask_sr",
        latest_bidask.and_then(to_option_f64),
    );
    insert_optional_f64(
        &mut pair_payload,
        "askbid_sr",
        latest_askbid.and_then(to_option_f64),
    );
    insert_optional_f64(&mut pair_payload, "spread_rate", spread_rate);
    insert_optional_f64(&mut pair_payload, "spread_fr", latest_spread_fr);
    insert_quantiles(&mut pair_payload, "bidask_quantiles", &bidask_quantiles);
    insert_quantiles(&mut pair_payload, "askbid_quantiles", &askbid_quantiles);
    insert_quantiles(&mut pair_payload, "spread_quantiles", &spread_quantiles);
    insert_quantiles(
        &mut pair_payload,
        "spread_fr_quantiles",
        &spread_fr_quantiles,
    );

    let open_side_payload = build_single_side_payload(
        open_venue,
        &open_field,
        base_symbol,
        now_ms,
        open_sample_size,
        latest_open_premium_rate,
        &open_premium_rate_quantiles,
    );
    let hedge_side_payload = build_single_side_payload(
        hedge_venue,
        &hedge_field,
        base_symbol,
        now_ms,
        hedge_sample_size,
        latest_hedge_premium_rate,
        &hedge_premium_rate_quantiles,
    );

    let mut outputs = vec![(pair_output_key.to_string(), pair_field, pair_payload)];
    if let Some(open_payload) = open_side_payload {
        if open_output_key == pair_output_key && open_field == outputs[0].1 {
            merge_payload_maps(&mut outputs[0].2, open_payload);
        } else {
            outputs.push((open_output_key.to_string(), open_field, open_payload));
        }
    }
    if let Some(hedge_payload) = hedge_side_payload {
        if hedge_output_key == pair_output_key && hedge_field == outputs[0].1 {
            merge_payload_maps(&mut outputs[0].2, hedge_payload);
        } else {
            outputs.push((hedge_output_key.to_string(), hedge_field, hedge_payload));
        }
    }
    serialize_output_maps(outputs)
}

fn derive_single_side_output_key(
    pair_output_key: &str,
    open_venue: &str,
    hedge_venue: &str,
    target_venue: &str,
) -> String {
    let pair_suffix = format!("_{}_{}", open_venue, hedge_venue);
    if let Some(prefix) = pair_output_key.strip_suffix(&pair_suffix) {
        return format!("{prefix}_{}_{}", target_venue, target_venue);
    }
    format!(
        "{DEFAULT_OUTPUT_HASH_KEY}_{}_{}",
        target_venue, target_venue
    )
}

fn same_venue_symbol_pair(venue: &str, base_symbol: &str) -> String {
    format!("{venue}_{venue}::{base_symbol}")
}

fn sample_size_for_factors(
    series: &SymbolSeries,
    config: &RollingConfig,
    factor_names: &[&str],
    factor_counts: &HashMap<String, usize>,
) -> usize {
    let mut counts = Vec::new();
    for factor_name in factor_names {
        if config.factor(factor_name).is_none() {
            continue;
        }
        if let Some(count) = factor_counts.get(*factor_name) {
            counts.push(*count);
        } else if let Some(ring) = series.ring(factor_name) {
            counts.push(ring.len());
        }
    }
    counts.into_iter().min().unwrap_or(0)
}

fn base_payload_map(
    symbol_pair: &str,
    base_symbol: &str,
    update_tp: i64,
    sample_size: usize,
) -> Map<String, Value> {
    let mut payload = Map::new();
    payload.insert(
        "symbol_pair".to_string(),
        Value::String(symbol_pair.to_string()),
    );
    payload.insert(
        "base_symbol".to_string(),
        Value::String(base_symbol.to_string()),
    );
    payload.insert("update_tp".to_string(), Value::Number(update_tp.into()));
    payload.insert(
        "sample_size".to_string(),
        Value::Number((sample_size as u64).into()),
    );
    payload
}

fn build_single_side_payload(
    venue: &str,
    symbol_pair: &str,
    base_symbol: &str,
    update_tp: i64,
    sample_size: usize,
    premium_rate: Option<f64>,
    premium_rate_quantiles: &[QuantilePoint],
) -> Option<Map<String, Value>> {
    let mut payload = base_payload_map(symbol_pair, base_symbol, update_tp, sample_size);
    let premium_field = format!("{venue}_premium_rate");
    let premium_quantiles_field = format!("{venue}_premium_rate_quantiles");

    if premium_rate.is_none() && premium_rate_quantiles.is_empty() {
        return None;
    }

    insert_optional_f64(&mut payload, &premium_field, premium_rate);
    insert_quantiles(
        &mut payload,
        &premium_quantiles_field,
        premium_rate_quantiles,
    );
    Some(payload)
}

fn insert_optional_f64(payload: &mut Map<String, Value>, key: &str, value: Option<f64>) {
    let json_value = match value.and_then(|v| serde_json::Number::from_f64(v).map(Value::Number)) {
        Some(number) => number,
        None => Value::Null,
    };
    payload.insert(key.to_string(), json_value);
}

fn insert_quantiles(payload: &mut Map<String, Value>, key: &str, quantiles: &[QuantilePoint]) {
    if quantiles.is_empty() {
        return;
    }
    match serde_json::to_value(quantiles) {
        Ok(value) => {
            payload.insert(key.to_string(), value);
        }
        Err(err) => {
            debug!(
                "{}: serialize quantiles for key {} failed: {}",
                log_prefix(),
                key,
                err
            );
        }
    }
}

fn merge_payload_maps(target: &mut Map<String, Value>, source: Map<String, Value>) {
    for (key, value) in source {
        target.insert(key, value);
    }
}

fn serialize_output_maps(
    outputs: Vec<(String, String, Map<String, Value>)>,
) -> Vec<(String, String, String)> {
    let mut serialized = Vec::new();
    for (output_key, field, payload) in outputs {
        match serde_json::to_string(&payload) {
            Ok(json) => serialized.push((output_key, field, json)),
            Err(err) => {
                debug!(
                    "{}: serialize payload for {} / {} failed: {}",
                    log_prefix(),
                    output_key,
                    field,
                    err
                );
            }
        }
    }
    serialized
}

fn compute_factor_quantiles(
    ring: &RingBuffer,
    cfg: &FactorConfig,
) -> (usize, Vec<QuantilePoint>, bool) {
    let (count, values) = ring.quantiles_linear(cfg.rolling_window, &cfg.quantiles);
    if cfg.quantiles.is_empty() {
        return (count, Vec::new(), true);
    }

    if count >= cfg.min_periods {
        let points = cfg
            .quantiles
            .iter()
            .zip(values.into_iter())
            .map(|(q, value)| QuantilePoint {
                quantile: *q,
                threshold: value,
            })
            .collect();
        (count, points, true)
    } else {
        let points = cfg
            .quantiles
            .iter()
            .map(|q| QuantilePoint {
                quantile: *q,
                threshold: None,
            })
            .collect();
        (count, points, false)
    }
}

fn factor_ready_counts(series: &SymbolSeries, config: &RollingConfig) -> (usize, usize) {
    let mut ready = 0usize;
    let mut total = 0usize;

    for (factor_name, _) in config.factors_iter() {
        total += 1;
        let is_ready = match factor_name {
            FACTOR_BIDASK => series.bidask.last().and_then(to_option_f64).is_some(),
            FACTOR_ASKBID => series.askbid.last().and_then(to_option_f64).is_some(),
            FACTOR_SPREAD => series.spread_rate().is_some(),
            FACTOR_OPEN_PREMIUM_RATE => series.open_premium_rate_latest().is_some(),
            FACTOR_HEDGE_PREMIUM_RATE => series.hedge_premium_rate_latest().is_some(),
            FACTOR_SPREAD_FR => series.spread_fr_latest().is_some(),
            _ => false,
        };
        if is_ready {
            ready += 1;
        }
    }

    (ready, total)
}

fn build_factor_coverage_rows(total: usize, ready: usize) -> Vec<(String, String, String)> {
    let ready_ratio = if total > 0 {
        (ready as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    let missing = total.saturating_sub(ready);
    let missing_ratio = if total > 0 {
        (missing as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    vec![
        (
            "all_ready".to_string(),
            format!("{}/{}", ready, total),
            format!("{ready_ratio:.2}%"),
        ),
        (
            "not_ready".to_string(),
            format!("{}/{}", missing, total),
            format!("{missing_ratio:.2}%"),
        ),
    ]
}

fn build_three_line_table_str(headers: [&str; 3], rows: &[(String, String, String)]) -> String {
    let mut widths = headers.iter().map(|h| h.len()).collect::<Vec<_>>();

    for row in rows {
        widths[0] = widths[0].max(row.0.len());
        widths[1] = widths[1].max(row.1.len());
        widths[2] = widths[2].max(row.2.len());
    }

    let format_row = |values: [&str; 3]| -> String {
        let mut parts = Vec::with_capacity(3);
        for (idx, value) in values.iter().enumerate() {
            parts.push(format!("{:<width$}", value, width = widths[idx]));
        }
        parts.join("  ")
    };

    let header_line = format_row(headers);
    let top_rule = "=".repeat(header_line.len());
    let mid_rule = "-".repeat(header_line.len());
    let bot_rule = "=".repeat(header_line.len());

    let mut lines = Vec::with_capacity(rows.len() + 4);
    lines.push(top_rule);
    lines.push(header_line);
    lines.push(mid_rule);

    for row in rows {
        let formatted = format_row([row.0.as_str(), row.1.as_str(), row.2.as_str()]);
        lines.push(formatted);
    }

    lines.push(bot_rule);
    lines.join("\n")
}

fn to_option_f64(value: f32) -> Option<f64> {
    if value.is_finite() {
        Some(value as f64)
    } else {
        None
    }
}

fn store_option_f64(cell: &AtomicU64, value: Option<f64>) {
    let bits = value.unwrap_or(f64::NAN).to_bits();
    cell.store(bits, Ordering::Release);
}

fn load_option_f64(cell: &AtomicU64) -> Option<f64> {
    let bits = cell.load(Ordering::Acquire);
    let value = f64::from_bits(bits);
    if value.is_nan() {
        None
    } else {
        Some(value)
    }
}

fn sleep_until(period: Duration, started: Instant) {
    if period.is_zero() {
        return;
    }
    let deadline = started + period;
    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline - now;
        thread::sleep(remaining.min(Duration::from_secs(1)));
    }
}
