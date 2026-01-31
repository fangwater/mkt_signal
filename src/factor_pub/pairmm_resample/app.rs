use anyhow::Result;
use log::{info, warn};
use std::time::{Duration, Instant};

use crate::common::mkt_msg::PairMmResampleMsg;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::time_util::get_timestamp_us;
use crate::signal::common::TradingVenue;
use super::cfg::PairMmResampleConfig;
use super::publisher::PairMmPublisher;

const LOG_INTERVAL_SECS: u64 = 60;

#[derive(Debug)]
struct PairMmState {
    values: std::collections::HashMap<String, [f64; 5]>,
    last_ts_ms: i64,
    rng: SimpleRng,
}

impl PairMmState {
    fn new() -> Self {
        Self {
            values: std::collections::HashMap::new(),
            last_ts_ms: 0,
            rng: SimpleRng::new((get_timestamp_us() / 1000) as u64),
        }
    }

    fn resample(&mut self, symbols: &[String]) -> Vec<(String, [f64; 5])> {
        let ts_ms = get_timestamp_us() / 1000;
        self.last_ts_ms = ts_ms;
        let mut out = Vec::with_capacity(symbols.len());
        for symbol in symbols {
            let values = [
                self.rng.gen_range(0.05, 0.30),
                self.rng.gen_range(0.05, 0.30),
                self.rng.gen_range(0.05, 0.30),
                self.rng.gen_range(0.0, 1.0),
                self.rng.gen_range(0.0, 1.0),
            ];
            self.values.insert(symbol.clone(), values);
            out.push((symbol.clone(), values));
        }
        out
    }
}

pub struct PairMmResampleApp {
    venue_slug: String,
    venue_u8: u8,
    venue: TradingVenue,
    config_path: String,
    config: PairMmResampleConfig,
    symbols: Vec<String>,
    factor_names: Vec<String>,
    factor_indices: Vec<usize>,
    publisher: PairMmPublisher,
    state: PairMmState,
    last_log_stats: Instant,
    last_reload_at: Instant,
}

impl PairMmResampleApp {
    pub fn new(
        config_path: &str,
        venue_slug: &str,
        venue_u8: u8,
        venue: TradingVenue,
    ) -> Result<Self> {
        let config = PairMmResampleConfig::load(config_path)?;
        config.validate()?;
        let symbols = normalize_symbols(&config.online_symbols, venue);
        let factor_names = config.factor_names.clone();
        let factor_indices = config.factor_indices()?;
        let publisher = PairMmPublisher::new(venue_slug, &config)?;

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            venue_u8,
            venue,
            config_path: config_path.to_string(),
            config,
            symbols,
            factor_names,
            factor_indices,
            publisher,
            state: PairMmState::new(),
            last_log_stats: Instant::now(),
            last_reload_at: Instant::now(),
        })
    }

    pub fn run(&mut self) -> Result<()> {
        info!(
            "PairMmResampleApp[{}] started: resample_interval_ms={}, symbols={}",
            self.venue_slug,
            self.config.resample_interval_ms,
            self.symbols.len()
        );

        let mut next_tick = Instant::now();
        loop {
            let now = Instant::now();
            if now >= next_tick {
                self.tick();
                next_tick = now + Duration::from_millis(self.config.resample_interval_ms);
            }

            if self.last_reload_at.elapsed() >= Duration::from_secs(60) {
                self.reload_config();
                self.last_reload_at = Instant::now();
            }

            if self.last_log_stats.elapsed() >= Duration::from_secs(LOG_INTERVAL_SECS) {
                self.publisher.log_stats();
                self.last_log_stats = Instant::now();
            }

            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn tick(&mut self) {
        for (symbol, values) in self.state.resample(&self.symbols) {
            let values = values.to_vec();
            let mut buf = String::new();
            for (i, name) in self.factor_names.iter().enumerate() {
                if let Some(idx) = self.factor_indices.get(i) {
                    let val = values.get(*idx).copied().unwrap_or(0.0);
                    if !buf.is_empty() {
                        buf.push(' ');
                    }
                    buf.push_str(name);
                    buf.push('=');
                    buf.push_str(&format!("{}", val));
                }
            }
            info!(
                "pairmm_resample: venue={} symbol={} {}",
                self.venue_slug, symbol, buf
            );
            let mapped: Vec<f64> = self
                .factor_indices
                .iter()
                .filter_map(|idx| values.get(*idx).copied())
                .collect();
            let msg = match PairMmResampleMsg::create(symbol.clone(), self.venue_u8, mapped) {
                Ok(msg) => msg,
                Err(err) => {
                    warn!("PairMmResampleMsg create failed: {err}");
                    continue;
                }
            };

            if !self.publisher.publish(&msg) {
                warn!(
                    "PairMmResample publish failed: venue={} symbol={}",
                    self.venue_slug, symbol
                );
            }
        }
    }

    #[allow(dead_code)]
    fn reload_config(&mut self) {
        match PairMmResampleConfig::load(&self.config_path) {
            Ok(new_cfg) => match new_cfg.validate() {
                Ok(()) => {
                    if new_cfg != self.config {
                        info!(
                            "PairMmResample config reloaded: resample_interval_ms={}, symbols={}",
                            new_cfg.resample_interval_ms,
                            new_cfg.online_symbols.len()
                        );
                        self.config = new_cfg;
                        self.symbols = normalize_symbols(&self.config.online_symbols, self.venue);
                        self.factor_names = self.config.factor_names.clone();
                        match self.config.factor_indices() {
                            Ok(indices) => self.factor_indices = indices,
                            Err(err) => warn!("PairMmResample factor map invalid: {err}"),
                        }
                    }
                }
                Err(err) => warn!("PairMmResample config reload invalid: {err}"),
            },
            Err(err) => warn!("PairMmResample config reload failed: {err}"),
        }
    }
}

#[derive(Debug)]
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

fn normalize_symbols(symbols: &[String], venue: TradingVenue) -> Vec<String> {
    use std::collections::HashSet;

    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(symbols.len());
    for raw in symbols {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let normalized = normalize_symbol_for_venue(trimmed, venue);
        if seen.insert(normalized.clone()) {
            out.push(normalized);
        }
    }
    out
}
