use anyhow::Result;
use log::{info, warn};
use std::time::{Duration, Instant};

use crate::common::mkt_msg::PairMmResampleMsg;
use crate::common::time_util::get_timestamp_us;
use super::cfg::PairMmResampleConfig;
use super::publisher::PairMmPublisher;

const LOG_INTERVAL_SECS: u64 = 60;

#[derive(Debug)]
struct PairMmState {
    history_threshold: f64,
    model_threshold: f64,
    cancel_threshold: f64,
    factor1: f64,
    factor2: f64,
    last_ts_ms: i64,
    rng: SimpleRng,
}

impl PairMmState {
    fn new() -> Self {
        Self {
            history_threshold: 0.0,
            model_threshold: 0.0,
            cancel_threshold: 0.0,
            factor1: 0.0,
            factor2: 0.0,
            last_ts_ms: 0,
            rng: SimpleRng::new((get_timestamp_us() / 1000) as u64),
        }
    }

    fn resample(&mut self) -> [f64; 5] {
        self.history_threshold = self.rng.gen_range(0.05, 0.30);
        self.model_threshold = self.rng.gen_range(0.05, 0.30);
        self.cancel_threshold = self.rng.gen_range(0.05, 0.30);
        self.factor1 = self.rng.gen_range(0.0, 1.0);
        self.factor2 = self.rng.gen_range(0.0, 1.0);
        self.last_ts_ms = get_timestamp_us() / 1000;
        [
            self.history_threshold,
            self.model_threshold,
            self.cancel_threshold,
            self.factor1,
            self.factor2,
        ]
    }
}

pub struct PairMmResampleApp {
    venue_slug: String,
    venue_u8: u8,
    config_path: String,
    config: PairMmResampleConfig,
    publisher: PairMmPublisher,
    state: PairMmState,
    last_log_stats: Instant,
}

impl PairMmResampleApp {
    pub fn new(config_path: &str, venue_slug: &str, venue_u8: u8) -> Result<Self> {
        let config = PairMmResampleConfig::load(config_path)?;
        config.validate()?;
        let publisher = PairMmPublisher::new(venue_slug, &config)?;

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            venue_u8,
            config_path: config_path.to_string(),
            config,
            publisher,
            state: PairMmState::new(),
            last_log_stats: Instant::now(),
        })
    }

    pub fn run(&mut self) -> Result<()> {
        info!(
            "PairMmResampleApp[{}] started: resample_interval_ms={}, symbol={}",
            self.venue_slug, self.config.resample_interval_ms, self.config.symbol
        );

        let mut next_tick = Instant::now();
        loop {
            let now = Instant::now();
            if now >= next_tick {
                self.tick();
                next_tick = now + Duration::from_millis(self.config.resample_interval_ms);
            }

            if self.last_log_stats.elapsed() >= Duration::from_secs(LOG_INTERVAL_SECS) {
                self.publisher.log_stats();
                self.last_log_stats = Instant::now();
            }

            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn tick(&mut self) {
        let values = self.state.resample();
        let values = values.to_vec();
        let msg = match PairMmResampleMsg::create(
            self.config.symbol.clone(),
            self.venue_u8,
            values,
        ) {
            Ok(msg) => msg,
            Err(err) => {
                warn!("PairMmResampleMsg create failed: {err}");
                return;
            }
        };

        if !self.publisher.publish(&msg) {
            warn!(
                "PairMmResample publish failed: venue={} symbol={}",
                self.venue_slug, self.config.symbol
            );
        }
    }

    #[allow(dead_code)]
    fn reload_config(&mut self) {
        match PairMmResampleConfig::load(&self.config_path) {
            Ok(new_cfg) => match new_cfg.validate() {
                Ok(()) => {
                    if new_cfg != self.config {
                        info!(
                            "PairMmResample config reloaded: resample_interval_ms={}, symbol={}",
                            new_cfg.resample_interval_ms, new_cfg.symbol
                        );
                        self.config = new_cfg;
                    }
                }
                Err(err) => warn!("PairMmResample config reload invalid: {err}"),
            },
            Err(err) => warn!("PairMmResample config reload failed: {err}"),
        }
    }
}

struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        let state = if seed == 0 { 0x9E3779B97F4A7C15 } else { seed };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_f64(&mut self) -> f64 {
        let v = self.next_u64() >> 11;
        (v as f64) * (1.0 / ((1u64 << 53) as f64))
    }

    fn gen_range(&mut self, min: f64, max: f64) -> f64 {
        min + (max - min) * self.next_f64()
    }
}
