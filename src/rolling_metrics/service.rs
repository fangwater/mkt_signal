use std::collections::HashSet;
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

use crate::rolling_metrics::config::{
    FactorConfig, RollingConfig, FACTOR_ASKBID, FACTOR_BIDASK, FACTOR_HEDGE_PREMIUM_RATE,
    FACTOR_OPEN_PREMIUM_RATE, FACTOR_SPREAD, FACTOR_SPREAD_FR,
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

pub struct ComputeResult {
    pub output_key: String,
    pub payloads: Vec<(String, String)>,
    pub removals: Vec<String>,
    pub stats: ComputeStats,
}

#[derive(Serialize)]
struct QuantilePoint {
    quantile: f32,
    threshold: Option<f64>,
}

#[derive(Serialize)]
struct ThresholdPayload<'a> {
    symbol_pair: &'a str,
    base_symbol: &'a str,
    update_tp: i64,
    sample_size: usize,
    bidask_sr: Option<f64>,
    askbid_sr: Option<f64>,
    spread_rate: Option<f64>,
    open_premium_rate: Option<f64>,
    hedge_premium_rate: Option<f64>,
    spread_fr: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    bidask_quantiles: Vec<QuantilePoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    askbid_quantiles: Vec<QuantilePoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    spread_quantiles: Vec<QuantilePoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    open_premium_rate_quantiles: Vec<QuantilePoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    hedge_premium_rate_quantiles: Vec<QuantilePoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    spread_fr_quantiles: Vec<QuantilePoint>,
}

pub fn spawn_compute_thread(
    series_map: Arc<SeriesMap>,
    config: Arc<RwLock<RollingConfig>>,
    series_capacity: Arc<AtomicUsize>,
    sender: Sender<ComputeResult>,
) {
    thread::spawn(move || {
        let mut last_keys: HashSet<String> = HashSet::new();
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
            let mut payloads: Vec<(String, String)> = Vec::with_capacity(total);
            let mut current_keys: HashSet<String> = HashSet::with_capacity(total);

            for (symbol_pair, series) in snapshot {
                current_keys.insert(symbol_pair.clone());
                let (ready_count, expected_total) =
                    factor_ready_counts(series.as_ref(), &cfg_snapshot);
                if expected_total > 0 && ready_count == expected_total {
                    all_ready_count += 1;
                }

                let entry = build_entry(
                    &symbol_pair,
                    &cfg_snapshot,
                    &series,
                    now_ms,
                    &mut processed,
                    &mut skipped,
                );

                if let Some((field, json)) = entry {
                    payloads.push((field, json));
                }
            }

            let removals: Vec<String> = last_keys.difference(&current_keys).cloned().collect();
            last_keys = current_keys;

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

            if sender
                .send(ComputeResult {
                    output_key: cfg_snapshot.output_hash_key.clone(),
                    payloads,
                    removals,
                    stats,
                })
                .is_err()
            {
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

fn build_entry(
    symbol_pair: &str,
    config: &RollingConfig,
    series: &Arc<SymbolSeries>,
    now_ms: i64,
    processed: &mut usize,
    skipped: &mut usize,
) -> Option<(String, String)> {
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

    let payload = ThresholdPayload {
        symbol_pair,
        base_symbol,
        update_tp: now_ms,
        sample_size,
        bidask_sr: latest_bidask.and_then(to_option_f64),
        askbid_sr: latest_askbid.and_then(to_option_f64),
        spread_rate,
        open_premium_rate: latest_open_premium_rate,
        hedge_premium_rate: latest_hedge_premium_rate,
        spread_fr: latest_spread_fr,
        bidask_quantiles,
        askbid_quantiles,
        spread_quantiles,
        open_premium_rate_quantiles,
        hedge_premium_rate_quantiles,
        spread_fr_quantiles,
    };

    match serde_json::to_string(&payload) {
        Ok(json) => Some((symbol_pair.to_string(), json)),
        Err(err) => {
            debug!(
                "{}: serialize payload for {} failed: {}",
                log_prefix(),
                symbol_pair,
                err
            );
            None
        }
    }
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
