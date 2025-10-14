use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use dashmap::DashMap;
use log::{debug, info, warn};
use parking_lot::RwLock;
use serde::Serialize;

use crate::rolling_metrics::config::RollingConfig;
use crate::rolling_metrics::quantile::quantiles_linear_select_unstable;
use crate::rolling_metrics::ring::RingBuffer;

pub type SeriesMap = DashMap<String, Arc<SymbolSeries>>;

#[derive(Clone)]
pub struct SymbolSeries {
    pub bidask: Arc<RingBuffer>,
    pub askbid: Arc<RingBuffer>,
}

impl SymbolSeries {
    pub fn new(capacity: usize) -> Self {
        Self {
            bidask: Arc::new(RingBuffer::new(capacity)),
            askbid: Arc::new(RingBuffer::new(capacity)),
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
    pub stats: ComputeStats,
}

#[derive(Serialize)]
struct ThresholdPayload<'a> {
    symbol_pair: &'a str,
    base_symbol: &'a str,
    update_tp: i64,
    sample_size: usize,
    bidask_sr: Option<f64>,
    askbid_sr: Option<f64>,
    bidask_lower: Option<f64>,
    bidask_upper: Option<f64>,
    askbid_lower: Option<f64>,
    askbid_upper: Option<f64>,
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

        loop {
            let cfg_snapshot = { config.read().clone() };
            let desired_capacity = cfg_snapshot.max_length;
            series_capacity.store(desired_capacity, Ordering::SeqCst);
            ensure_series_capacity(&series_map, desired_capacity);

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

            for (symbol_pair, series) in snapshot {
                let entry = build_entry(
                    &symbol_pair,
                    &cfg_snapshot,
                    &series,
                    &mut bidask_buf,
                    &mut askbid_buf,
                    now_ms,
                    &mut processed,
                    &mut skipped,
                );

                if let Some((field, json)) = entry {
                    payloads.push((field, json));
                }
            }

            let duration = start.elapsed();
            let stats = ComputeStats {
                total_symbols: total,
                processed,
                skipped,
                duration_ms: duration.as_millis(),
            };

            info!(
                "rolling_metrics: computed {} symbols (processed={}, skipped={}) in {:?}",
                total, processed, skipped, duration
            );

            if sender
                .send(ComputeResult {
                    output_key: cfg_snapshot.output_hash_key.clone(),
                    payloads,
                    stats,
                })
                .is_err()
            {
                warn!("rolling_metrics: compute thread exiting because receiver dropped");
                break;
            }

            let interval = cfg_snapshot.interval();
            sleep_precise(interval, start.elapsed());
        }
    });
}

fn build_entry(
    symbol_pair: &str,
    config: &RollingConfig,
    series: &Arc<SymbolSeries>,
    bidask_buf: &mut Vec<f32>,
    askbid_buf: &mut Vec<f32>,
    now_ms: i64,
    processed: &mut usize,
    skipped: &mut usize,
) -> Option<(String, String)> {
    let base_symbol = symbol_pair.splitn(2, "::").nth(1).unwrap_or(symbol_pair);

    let latest_bidask = series.bidask.last();
    let latest_askbid = series.askbid.last();

    let bidask_count = series.bidask.copy_latest(config.rolling_window, bidask_buf);
    let askbid_count = series.askbid.copy_latest(config.rolling_window, askbid_buf);

    let enough_bidask = bidask_count >= config.min_periods;
    let enough_askbid = askbid_count >= config.min_periods;

    let mut bidask_lower = None;
    let mut bidask_upper = None;
    if enough_bidask {
        let qs = config.bidask_quantiles();
        let values = quantiles_linear_select_unstable(bidask_buf.as_mut_slice(), &qs);
        bidask_lower = values.get(0).copied();
        bidask_upper = values.get(1).copied();
    }

    let mut askbid_lower = None;
    let mut askbid_upper = None;
    if enough_askbid {
        let qs = config.askbid_quantiles();
        let values = quantiles_linear_select_unstable(askbid_buf.as_mut_slice(), &qs);
        askbid_lower = values.get(0).copied();
        askbid_upper = values.get(1).copied();
    }

    if enough_bidask && enough_askbid {
        *processed += 1;
    } else {
        *skipped += 1;
    }

    let payload = ThresholdPayload {
        symbol_pair,
        base_symbol,
        update_tp: now_ms,
        sample_size: bidask_count.min(askbid_count),
        bidask_sr: latest_bidask.and_then(to_option_f64),
        askbid_sr: latest_askbid.and_then(to_option_f64),
        bidask_lower: bidask_lower.and_then(to_option_f64),
        bidask_upper: bidask_upper.and_then(to_option_f64),
        askbid_lower: askbid_lower.and_then(to_option_f64),
        askbid_upper: askbid_upper.and_then(to_option_f64),
    };

    match serde_json::to_string(&payload) {
        Ok(json) => Some((symbol_pair.to_string(), json)),
        Err(err) => {
            debug!(
                "rolling_metrics: serialize payload for {} failed: {}",
                symbol_pair, err
            );
            None
        }
    }
}

fn to_option_f64(value: f32) -> Option<f64> {
    if value.is_finite() {
        Some(value as f64)
    } else {
        None
    }
}

fn sleep_precise(period: Duration, elapsed: Duration) {
    if elapsed >= period {
        return;
    }
    let remaining = period - elapsed;
    if remaining > Duration::from_millis(5) {
        thread::sleep(remaining - Duration::from_millis(5));
    }
    let start = Instant::now();
    while start.elapsed() < remaining {
        std::hint::spin_loop();
    }
}
