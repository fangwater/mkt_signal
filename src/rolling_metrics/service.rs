use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use dashmap::DashMap;
use log::{debug, info, warn};
use parking_lot::RwLock;
use serde::Serialize;

use crate::rolling_metrics::config::{
    FactorConfig, RollingConfig, FACTOR_ASKBID, FACTOR_BIDASK, FACTOR_MID_SPOT, FACTOR_MID_SWAP,
    FACTOR_SPREAD,
};
use crate::rolling_metrics::quantile::quantiles_linear_select_unstable;
use crate::rolling_metrics::ring::RingBuffer;

const LOG_PREFIX: &str = "binance-rolling-metrics";

pub type SeriesMap = DashMap<String, Arc<SymbolSeries>>;

pub struct SymbolSeries {
    pub bidask: Arc<RingBuffer>,
    pub askbid: Arc<RingBuffer>,
    pub mid_spot: Arc<RingBuffer>,
    pub mid_swap: Arc<RingBuffer>,
    pub spread: Arc<RingBuffer>,
    mid_price_spot: AtomicU64,
    mid_price_swap: AtomicU64,
    spread_rate: AtomicU64,
}

impl SymbolSeries {
    pub fn new(capacity: usize) -> Self {
        Self {
            bidask: Arc::new(RingBuffer::new(capacity)),
            askbid: Arc::new(RingBuffer::new(capacity)),
            mid_spot: Arc::new(RingBuffer::new(capacity)),
            mid_swap: Arc::new(RingBuffer::new(capacity)),
            spread: Arc::new(RingBuffer::new(capacity)),
            mid_price_spot: AtomicU64::new(f64::NAN.to_bits()),
            mid_price_swap: AtomicU64::new(f64::NAN.to_bits()),
            spread_rate: AtomicU64::new(f64::NAN.to_bits()),
        }
    }

    pub fn set_mid_metrics(
        &self,
        mid_spot: Option<f64>,
        mid_swap: Option<f64>,
        spread_rate: Option<f64>,
    ) {
        store_option_f64(&self.mid_price_spot, mid_spot);
        store_option_f64(&self.mid_price_swap, mid_swap);
        store_option_f64(&self.spread_rate, spread_rate);
    }

    pub fn mid_price_spot(&self) -> Option<f64> {
        load_option_f64(&self.mid_price_spot)
    }

    pub fn mid_price_swap(&self) -> Option<f64> {
        load_option_f64(&self.mid_price_swap)
    }

    pub fn spread_rate(&self) -> Option<f64> {
        load_option_f64(&self.spread_rate)
    }

    pub fn ring(&self, factor: &str) -> Option<&Arc<RingBuffer>> {
        match factor {
            crate::rolling_metrics::config::FACTOR_BIDASK => Some(&self.bidask),
            crate::rolling_metrics::config::FACTOR_ASKBID => Some(&self.askbid),
            crate::rolling_metrics::config::FACTOR_MID_SPOT => Some(&self.mid_spot),
            crate::rolling_metrics::config::FACTOR_MID_SWAP => Some(&self.mid_swap),
            crate::rolling_metrics::config::FACTOR_SPREAD => Some(&self.spread),
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
    mid_price_spot: Option<f64>,
    mid_price_swap: Option<f64>,
    spread_rate: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    bidask_quantiles: Vec<QuantilePoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    askbid_quantiles: Vec<QuantilePoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mid_spot_quantiles: Vec<QuantilePoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mid_swap_quantiles: Vec<QuantilePoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    spread_quantiles: Vec<QuantilePoint>,
}

pub fn spawn_compute_thread(
    series_map: Arc<SeriesMap>,
    config: Arc<RwLock<RollingConfig>>,
    series_capacity: Arc<AtomicUsize>,
    sender: Sender<ComputeResult>,
) {
    thread::spawn(move || {
        let mut bidask_buf: Vec<f32> = Vec::new();
        let mut askbid_buf: Vec<f32> = Vec::new();
        let mut mid_spot_buf: Vec<f32> = Vec::new();
        let mut mid_swap_buf: Vec<f32> = Vec::new();
        let mut spread_buf: Vec<f32> = Vec::new();
        let mut last_keys: HashSet<String> = HashSet::new();
        let mut last_refresh_sec: u64 = 0;

        loop {
            let cfg_snapshot = { config.read().clone() };
            let desired_capacity = cfg_snapshot.max_length;
            series_capacity.store(desired_capacity, Ordering::SeqCst);
            ensure_series_capacity(&series_map, desired_capacity);

            if cfg_snapshot.refresh_sec != last_refresh_sec {
                info!(
                    "{LOG_PREFIX}: compute interval refresh_sec={}s (prev={}s)",
                    cfg_snapshot.refresh_sec, last_refresh_sec
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
            let mut processed = 0usize;
            let mut skipped = 0usize;
            let mut payloads: Vec<(String, String)> = Vec::with_capacity(total);
            let mut current_keys: HashSet<String> = HashSet::with_capacity(total);

            for (symbol_pair, series) in snapshot {
                current_keys.insert(symbol_pair.clone());
                let entry = build_entry(
                    &symbol_pair,
                    &cfg_snapshot,
                    &series,
                    &mut bidask_buf,
                    &mut askbid_buf,
                    &mut mid_spot_buf,
                    &mut mid_swap_buf,
                    &mut spread_buf,
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
                "{LOG_PREFIX}: computed {} symbols (processed={}, skipped={}) in {:?}",
                total, processed, skipped, duration
            );

            if sender
                .send(ComputeResult {
                    output_key: cfg_snapshot.output_hash_key.clone(),
                    payloads,
                    removals,
                    stats,
                })
                .is_err()
            {
                warn!("{LOG_PREFIX}: compute thread exiting because receiver dropped");
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
    bidask_buf: &mut Vec<f32>,
    askbid_buf: &mut Vec<f32>,
    mid_spot_buf: &mut Vec<f32>,
    mid_swap_buf: &mut Vec<f32>,
    spread_buf: &mut Vec<f32>,
    now_ms: i64,
    processed: &mut usize,
    skipped: &mut usize,
) -> Option<(String, String)> {
    let parts: Vec<&str> = symbol_pair.split("::").collect();
    let base_symbol = parts.last().copied().unwrap_or(symbol_pair);

    let latest_bidask = series.bidask.last();
    let latest_askbid = series.askbid.last();

    let mid_price_spot = series.mid_price_spot();
    let mid_price_swap = series.mid_price_swap();
    let spread_rate = series.spread_rate();

    let mut bidask_quantiles: Vec<QuantilePoint> = Vec::new();

    let mut askbid_quantiles: Vec<QuantilePoint> = Vec::new();
    let mut mid_spot_quantiles: Vec<QuantilePoint> = Vec::new();
    let mut mid_swap_quantiles: Vec<QuantilePoint> = Vec::new();
    let mut spread_quantiles: Vec<QuantilePoint> = Vec::new();
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
            FACTOR_BIDASK => compute_factor_quantiles(ring.as_ref(), bidask_buf, factor_cfg),
            FACTOR_ASKBID => compute_factor_quantiles(ring.as_ref(), askbid_buf, factor_cfg),
            FACTOR_MID_SPOT => compute_factor_quantiles(ring.as_ref(), mid_spot_buf, factor_cfg),
            FACTOR_MID_SWAP => compute_factor_quantiles(ring.as_ref(), mid_swap_buf, factor_cfg),
            FACTOR_SPREAD => compute_factor_quantiles(ring.as_ref(), spread_buf, factor_cfg),
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
            FACTOR_MID_SPOT => mid_spot_quantiles = points,
            FACTOR_MID_SWAP => mid_swap_quantiles = points,
            FACTOR_SPREAD => spread_quantiles = points,
            _ => {}
        }
    }

    if factors_with_quantiles > 0 {
        if factors_ready == factors_with_quantiles {
            *processed += 1;
            debug!(
                "{LOG_PREFIX}: symbol {} factors refreshed {:?}",
                base_symbol, ready_factors
            );
        } else {
            *skipped += 1;
            debug!(
                "{LOG_PREFIX}: symbol {} insufficient samples for factors {:?}",
                base_symbol, missing_ready
            );
        }
    } else {
        *processed += 1;
    }

    if sample_counts.is_empty() {
        let fallback = series.bidask.len().min(series.askbid.len());
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
        mid_price_spot,
        mid_price_swap,
        spread_rate,
        bidask_quantiles,
        askbid_quantiles,
        mid_spot_quantiles,
        mid_swap_quantiles,
        spread_quantiles,
    };

    match serde_json::to_string(&payload) {
        Ok(json) => Some((symbol_pair.to_string(), json)),
        Err(err) => {
            debug!(
                "{LOG_PREFIX}: serialize payload for {} failed: {}",
                symbol_pair, err
            );
            None
        }
    }
}

fn compute_factor_quantiles(
    ring: &RingBuffer,
    buffer: &mut Vec<f32>,
    cfg: &FactorConfig,
) -> (usize, Vec<QuantilePoint>, bool) {
    let count = ring.copy_latest(cfg.rolling_window, buffer);
    if cfg.quantiles.is_empty() {
        return (count, Vec::new(), true);
    }

    if count >= cfg.min_periods {
        let values = quantiles_linear_select_unstable(buffer.as_mut_slice(), &cfg.quantiles);
        let points = cfg
            .quantiles
            .iter()
            .zip(values.into_iter())
            .map(|(q, value)| QuantilePoint {
                quantile: *q,
                threshold: to_option_f64(value),
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
