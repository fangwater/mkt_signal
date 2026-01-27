//! RL Return Volatility 应用主模块
//!
//! 订阅 kline 数据并计算 rolling return volatility

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use polars::prelude::{NamedFrom, RollingOptionsFixedWindow, Series};
use polars_time::prelude::SeriesOpsTime;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::cfg::RlReturnVolatilityConfig;
use super::publisher::FactorPublisher;
use crate::common::mkt_msg::RlReturnVolatilityMsg;
use crate::common::mkt_msg::MktMsgType;
use crate::common::symbol_util::extract_assets_from_symbol;

const KLINE_MAX_BYTES: usize = 128;
const IDLE_SLEEP_MICROS: u64 = 200;
const KLINE_CHANNEL_LABEL: &str = "kline5s";
const LOG_BASE_SYMBOLS: [&str; 3] = ["BTC", "ETH", "SOL"];

#[derive(Debug)]
struct KlineTick {
    symbol: String,
    close: f64,
    timestamp_ms: i64,
}

pub struct RlReturnVolatilityApp {
    venue_slug: String,
    config_path: String,
    config: RlReturnVolatilityConfig,
    subscriber: Subscriber<ipc::Service, [u8; KLINE_MAX_BYTES], ()>,
    publisher: FactorPublisher,
    series: HashMap<String, VecDeque<f64>>,
    kline_count: u64,
    last_reload_at: u64,
    last_log_stats: Instant,
}

impl RlReturnVolatilityApp {
    pub fn new(config_path: &str, venue_slug: &str) -> Result<Self> {
        let config = RlReturnVolatilityConfig::load(config_path)?;
        config.validate()?;

        let subscriber = Self::create_subscriber(venue_slug)?;
        let publisher = FactorPublisher::new(venue_slug)?;

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            config_path: config_path.to_string(),
            config,
            subscriber,
            publisher,
            series: HashMap::new(),
            kline_count: 0,
            last_reload_at: 0,
            last_log_stats: Instant::now(),
        })
    }

    fn create_subscriber(
        venue: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; KLINE_MAX_BYTES], ()>> {
        let node_name = format!("factor_sub_{}_rl_return_vol", venue.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_name = format!("kline_pubs/{}/{}", venue, KLINE_CHANNEL_LABEL);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; KLINE_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to kline channel: {}", service_name);
        Ok(subscriber)
    }

    pub fn run(&mut self) -> Result<()> {
        info!(
            "RlReturnVolatilityApp[{}] started with config: pct_change_period={}, rolling_window={}, clip=[{}, {}], max_keep_count={}, scale_factor={}",
            self.venue_slug,
            self.config.pct_change_period,
            self.config.rolling_window,
            self.config.clip_min,
            self.config.clip_max,
            self.config.max_keep_count,
            self.config.scale_factor
        );

        loop {
            let mut has_message = false;
            while let Some(sample) = self.subscriber.receive()? {
                has_message = true;
                let data = sample.payload().to_vec();
                if let Some(kline) = parse_kline(&data) {
                    self.handle_kline(kline)?;
                }
            }

            if !has_message {
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }

            if self.last_log_stats.elapsed() >= Duration::from_secs(60) {
                self.publisher.log_stats();
                self.last_log_stats = Instant::now();
            }
        }
    }

    fn handle_kline(&mut self, kline: KlineTick) -> Result<()> {
        self.kline_count += 1;
        self.maybe_reload_config();

        let series = self
            .series
            .entry(kline.symbol.clone())
            .or_insert_with(VecDeque::new);
        series.push_back(kline.close);
        if series.len() > self.config.max_keep_count {
            series.pop_front();
        }

        let (value, ready) = match compute_factor(series, &self.config)? {
            Some(value) => (value, true),
            None => (0.0, false),
        };
        let published_value = if ready {
            value * self.config.scale_factor
        } else {
            0.0
        };

        if should_log_factor_symbol(&kline.symbol) {
            info!(
                "rl_return_volatility factor: venue={} symbol={} value={} ready={} ts_ms={}",
                self.venue_slug, kline.symbol, published_value, ready, kline.timestamp_ms
            );
        }

        let msg = RlReturnVolatilityMsg::create(
            kline.symbol.clone(),
            published_value,
            kline.timestamp_ms,
            ready,
        );
        if !self.publisher.publish(&msg) {
            warn!(
                "Failed to publish rl_return_volatility for {}",
                kline.symbol
            );
        }

        Ok(())
    }

    fn maybe_reload_config(&mut self) {
        let reload_every = self.config.pct_change_period as u64;
        if reload_every == 0 {
            return;
        }
        if self.kline_count - self.last_reload_at < reload_every {
            return;
        }

        self.last_reload_at = self.kline_count;
        match RlReturnVolatilityConfig::load(&self.config_path) {
            Ok(new_cfg) => match new_cfg.validate() {
                Ok(()) => {
                    if new_cfg != self.config {
                        info!(
                            "Reloaded config: pct_change_period={}, rolling_window={}, clip=[{}, {}], max_keep_count={}, scale_factor={}",
                            new_cfg.pct_change_period,
                            new_cfg.rolling_window,
                            new_cfg.clip_min,
                            new_cfg.clip_max,
                            new_cfg.max_keep_count,
                            new_cfg.scale_factor
                        );
                        self.config = new_cfg;
                        self.shrink_series();
                    }
                }
                Err(err) => {
                    warn!("Invalid config reload ignored: {}", err);
                }
            },
            Err(err) => {
                warn!("Failed to reload config: {}", err);
            }
        }
    }

    fn shrink_series(&mut self) {
        let max_len = self.config.max_keep_count;
        for series in self.series.values_mut() {
            while series.len() > max_len {
                series.pop_front();
            }
        }
    }
}

fn parse_kline(data: &[u8]) -> Option<KlineTick> {
    if data.len() < 8 {
        return None;
    }

    let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if msg_type != MktMsgType::Kline as u32 {
        return None;
    }

    let symbol_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let min_len = 8 + symbol_len + 5 * 8 + 8;
    if data.len() < min_len {
        return None;
    }

    let symbol = std::str::from_utf8(&data[8..8 + symbol_len])
        .ok()?
        .to_string();

    let mut offset = 8 + symbol_len;
    let _open = read_f64(data, &mut offset)?;
    let _high = read_f64(data, &mut offset)?;
    let _low = read_f64(data, &mut offset)?;
    let close = read_f64(data, &mut offset)?;
    let _volume = read_f64(data, &mut offset)?;

    let timestamp_ms = i64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);

    Some(KlineTick {
        symbol,
        close,
        timestamp_ms,
    })
}

fn read_f64(data: &[u8], offset: &mut usize) -> Option<f64> {
    let value = f64::from_le_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
        data[*offset + 4],
        data[*offset + 5],
        data[*offset + 6],
        data[*offset + 7],
    ]);
    *offset += 8;
    Some(value)
}

fn compute_factor(
    closes: &VecDeque<f64>,
    cfg: &RlReturnVolatilityConfig,
) -> Result<Option<f64>> {
    let required = cfg.min_required_len();
    if closes.len() < required {
        return Ok(None);
    }

    let start = closes.len() - required;
    let window: Vec<f64> = closes.iter().skip(start).copied().collect();

    let mut returns: Vec<Option<f64>> = vec![None; window.len()];
    for idx in cfg.pct_change_period..window.len() {
        let prev = window[idx - cfg.pct_change_period];
        if prev <= 0.0 {
            returns[idx] = None;
            continue;
        }
        returns[idx] = Some(window[idx] / prev - 1.0);
    }

    let series = Series::new("returns".into(), returns);
    let options = RollingOptionsFixedWindow {
        window_size: cfg.rolling_window,
        min_periods: cfg.rolling_window,
        ..Default::default()
    };

    let rolling = series.rolling_std(options)?;
    let last_idx = rolling.len().saturating_sub(1);
    let Some(value) = rolling.f64()?.get(last_idx) else {
        return Ok(None);
    };
    if !value.is_finite() {
        return Ok(None);
    }
    Ok(Some(value.clamp(cfg.clip_min, cfg.clip_max)))
}

fn should_log_factor_symbol(symbol: &str) -> bool {
    let (base, _) = extract_assets_from_symbol(symbol);
    LOG_BASE_SYMBOLS
        .iter()
        .any(|candidate| base.eq_ignore_ascii_case(candidate))
}
