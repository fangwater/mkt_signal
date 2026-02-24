use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::cfg::FeatureNormConfig;
use crate::common::mkt_msg::FeatureMsg;
use crate::common::rolling_welford::RollingWelford;
use crate::factor_pub::fusion_factor_pub::publisher::{
    FusionFactorPublisher, FUSION_FACTOR_PAYLOAD_MAX_BYTES,
};

const INPUT_MAX_BYTES: usize = FUSION_FACTOR_PAYLOAD_MAX_BYTES;
const IDLE_SLEEP_MICROS: u64 = 200;
const LOG_INTERVAL_SECS: u64 = 60;

#[derive(Default)]
struct Stats {
    recv_total: u64,
    publish_ok: u64,
    publish_fail: u64,
    decode_err: u64,
}

struct SymbolState {
    welford_vec: Vec<RollingWelford>,
    sample_count: u64,
}

pub struct FeatureNormApp {
    window_size: usize,
    subscriber: Subscriber<ipc::Service, [u8; INPUT_MAX_BYTES], ()>,
    publisher: FusionFactorPublisher,
    symbols: HashMap<String, SymbolState>,
    stats: Stats,
    last_log: Instant,
    btc_last_zscore: Vec<f64>,
}

/// Infer venue from CWD directory name (e.g. ~/feature_norm/binance-futures → "binance-futures").
fn infer_venue_from_cwd() -> Result<String> {
    let cwd = std::env::current_dir().context("failed to get current directory")?;
    let name = cwd
        .file_name()
        .context("CWD has no directory name")?
        .to_string_lossy()
        .to_ascii_lowercase();
    Ok(name)
}

impl FeatureNormApp {
    pub fn new(config_path: &str) -> Result<Self> {
        let config = FeatureNormConfig::load(config_path)?;
        let venue = infer_venue_from_cwd()?;

        let input_service = format!("fusion_factor/{}/{}", venue, config.model_name);
        let output_service = format!("feature_norm/{}/{}", venue, config.model_name);

        let subscriber = Self::create_subscriber(&input_service)?;
        let publisher = FusionFactorPublisher::new("feature_norm_out", &output_service)?;

        info!(
            "FeatureNormApp started: venue={} model_name={} input={} output={} window={}",
            venue, config.model_name, input_service, output_service, config.window_size
        );

        Ok(Self {
            window_size: config.window_size,
            subscriber,
            publisher,
            symbols: HashMap::new(),
            stats: Stats::default(),
            last_log: Instant::now(),
            btc_last_zscore: Vec::new(),
        })
    }

    fn create_subscriber(
        service_path: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; INPUT_MAX_BYTES], ()>> {
        let node = NodeBuilder::new()
            .name(&NodeName::new("feature_norm_in")?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(service_path)?)
            .publish_subscribe::<[u8; INPUT_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to feature channel: {}", service_path);
        Ok(subscriber)
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            let mut has_message = false;
            while let Some(sample) = self.subscriber.receive()? {
                has_message = true;
                self.stats.recv_total += 1;

                let data = sample.payload().to_vec();
                self.process_input(&data);
            }

            if !has_message {
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }

            if self.last_log.elapsed() >= Duration::from_secs(LOG_INTERVAL_SECS) {
                self.log_stats();
                self.last_log = Instant::now();
            }
        }
    }

    fn log_stats(&self) {
        let btc_info = if self.btc_last_zscore.is_empty() {
            "BTCUSDT: no data".to_string()
        } else {
            let btc_state = self.symbols.get("BTCUSDT");
            let samples = btc_state.map_or(0, |s| s.sample_count);
            let first5: Vec<String> = self.btc_last_zscore.iter().take(5).map(|v| format!("{:.4}", v)).collect();
            format!("BTCUSDT: samples={} zscore=[{}{}]",
                samples,
                first5.join(", "),
                if self.btc_last_zscore.len() > 5 { ", ..." } else { "" },
            )
        };

        info!(
            "feature_norm stats: recv={} pub_ok={} pub_fail={} decode_err={} symbols={} | {}",
            self.stats.recv_total,
            self.stats.publish_ok,
            self.stats.publish_fail,
            self.stats.decode_err,
            self.symbols.len(),
            btc_info,
        );
    }

    fn process_input(&mut self, data: &[u8]) {
        let feature = match FeatureMsg::from_bytes(data) {
            Ok(f) => f,
            Err(err) => {
                self.stats.decode_err += 1;
                warn!("feature_norm decode failed: {}", err);
                return;
            }
        };

        let dim = feature.features.len();
        let window_size = self.window_size;

        let state = self.symbols.entry(feature.symbol.clone()).or_insert_with(|| SymbolState {
            welford_vec: (0..dim).map(|_| RollingWelford::new(window_size)).collect(),
            sample_count: 0,
        });

        // If dimension changed, reset
        if state.welford_vec.len() != dim {
            state.welford_vec = (0..dim).map(|_| RollingWelford::new(window_size)).collect();
            state.sample_count = 0;
        }

        // Always push to accumulate statistics
        for (i, &val) in feature.features.iter().enumerate() {
            state.welford_vec[i].push(val);
        }
        state.sample_count += 1;

        // Compute z-scores
        let normalized: Vec<f64> = state
            .welford_vec
            .iter()
            .map(|rw| rw.zscore_capped(3.0).unwrap_or(0.0))
            .collect();

        // Track BTCUSDT for heartbeat
        if feature.symbol == "BTCUSDT" {
            self.btc_last_zscore = normalized.clone();
        }

        let out_msg = FeatureMsg::create(
            feature.symbol.clone(),
            feature.ts_ms,
            feature.status,
            normalized,
        );

        match out_msg.to_bytes() {
            Ok(bytes) => {
                if self.publisher.publish(&bytes) {
                    self.stats.publish_ok += 1;
                } else {
                    self.stats.publish_fail += 1;
                }
            }
            Err(err) => {
                self.stats.publish_fail += 1;
                warn!(
                    "feature_norm serialize failed: symbol={} err={}",
                    feature.symbol, err
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_produces_zscore() {
        let window_size = 5;
        let mut welford_vec: Vec<RollingWelford> =
            (0..2).map(|_| RollingWelford::new(window_size)).collect();

        let inputs: Vec<Vec<f64>> = vec![
            vec![1.0, 10.0],
            vec![2.0, 20.0],
            vec![3.0, 30.0],
            vec![4.0, 40.0],
            vec![5.0, 50.0],
        ];

        let mut last_normalized = vec![0.0; 2];
        for features in &inputs {
            let mut normalized = Vec::with_capacity(2);
            for (i, &val) in features.iter().enumerate() {
                welford_vec[i].push(val);
                normalized.push(welford_vec[i].zscore().unwrap_or(0.0));
            }
            last_normalized = normalized;
        }

        assert!(
            (last_normalized[0] - 1.2649).abs() < 0.01,
            "got {}",
            last_normalized[0]
        );
        assert!(
            (last_normalized[1] - 1.2649).abs() < 0.01,
            "got {}",
            last_normalized[1]
        );
    }

    #[test]
    fn single_sample_returns_zero() {
        let mut rw = RollingWelford::new(10);
        rw.push(42.0);
        assert_eq!(rw.zscore(), None);
        assert_eq!(rw.zscore().unwrap_or(0.0), 0.0);
    }

    #[test]
    fn constant_values_return_zero() {
        let mut rw = RollingWelford::new(10);
        for _ in 0..10 {
            rw.push(5.0);
        }
        assert_eq!(rw.zscore().unwrap_or(0.0), 0.0);
    }
}
