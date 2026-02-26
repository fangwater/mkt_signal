//! 因子应用主模块
//!
//! 订阅 kline 数据并计算多个 rolling 因子

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use polars::prelude::{NamedFrom, RollingOptionsFixedWindow, Series};
use polars_time::prelude::SeriesOpsTime;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::cfg::{FactorDefinition, FactorKind, FactorPubConfig};
use super::publisher::FactorPublisher;
use crate::common::mkt_msg::MktMsgType;
use crate::common::symbol_util::extract_assets_from_symbol;

const KLINE_MAX_BYTES: usize = 128;
const IDLE_SLEEP_MICROS: u64 = 200;
const LOG_BASE_SYMBOLS: [&str; 3] = ["BTC", "ETH", "SOL"];

#[derive(Debug)]
struct KlineTick {
    symbol: String,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    count: u64,
    timestamp_ms: i64,
}

#[derive(Debug, Clone)]
struct FactorOutput {
    value: f64,
    ready: bool,
}

pub struct KlineFactorPubApp {
    venue_slug: String,
    config_path: String,
    config: FactorPubConfig,
    subscriber: Subscriber<ipc::Service, [u8; KLINE_MAX_BYTES], ()>,
    publisher: FactorPublisher,
    closes: HashMap<String, VecDeque<f64>>,
    highs: HashMap<String, VecDeque<f64>>,
    lows: HashMap<String, VecDeque<f64>>,
    volumes: HashMap<String, VecDeque<f64>>,
    counts: HashMap<String, VecDeque<f64>>,
    kline_count: u64,
    last_reload_at: u64,
    last_log_stats: Instant,
}

impl KlineFactorPubApp {
    pub fn new(config_path: &str, venue_slug: &str) -> Result<Self> {
        let config = FactorPubConfig::load(config_path)?;
        config.validate()?;

        let subscriber = Self::create_subscriber(venue_slug, &config.data_source.kline_channel)?;
        let factor_names = config
            .factors
            .iter()
            .map(|f| f.name.clone())
            .collect::<Vec<_>>();
        let publisher = FactorPublisher::new(venue_slug, &factor_names)?;

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            config_path: config_path.to_string(),
            config,
            subscriber,
            publisher,
            closes: HashMap::new(),
            highs: HashMap::new(),
            lows: HashMap::new(),
            volumes: HashMap::new(),
            counts: HashMap::new(),
            kline_count: 0,
            last_reload_at: 0,
            last_log_stats: Instant::now(),
        })
    }

    fn create_subscriber(
        venue: &str,
        channel: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; KLINE_MAX_BYTES], ()>> {
        let node_name = format!("factor_sub_{}_multi_factor", venue.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_name = format!("kline_pubs/{}/{}", venue, channel);
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
            "FactorPubApp[{}] started with config: channel={}, reload_every={}, max_keep_count={}, factors={}",
            self.venue_slug,
            self.config.data_source.kline_channel,
            self.config.runtime.reload_every,
            self.config.runtime.max_keep_count,
            self.config.factors.len()
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

        self.push_kline_point(&kline);

        let outputs = self.compute_all_factors(&kline.symbol)?;
        for (factor_name, out) in outputs {
            if should_log_factor_symbol(&kline.symbol) {
                info!(
                    "factor: venue={} factor={} symbol={} value={} ready={} ts_ms={}",
                    self.venue_slug,
                    factor_name,
                    kline.symbol,
                    out.value,
                    out.ready,
                    kline.timestamp_ms
                );
            }

            if !self.publisher.publish_factor(
                &factor_name,
                &kline.symbol,
                out.value,
                kline.timestamp_ms,
                out.ready,
            ) {
                warn!(
                    "Failed to publish factor={} symbol={}",
                    factor_name, kline.symbol
                );
            }
        }

        Ok(())
    }

    fn push_kline_point(&mut self, kline: &KlineTick) {
        let closes = self
            .closes
            .entry(kline.symbol.clone())
            .or_insert_with(VecDeque::new);
        closes.push_back(kline.close);
        if closes.len() > self.config.runtime.max_keep_count {
            closes.pop_front();
        }

        let highs = self
            .highs
            .entry(kline.symbol.clone())
            .or_insert_with(VecDeque::new);
        highs.push_back(kline.high);
        if highs.len() > self.config.runtime.max_keep_count {
            highs.pop_front();
        }

        let lows = self
            .lows
            .entry(kline.symbol.clone())
            .or_insert_with(VecDeque::new);
        lows.push_back(kline.low);
        if lows.len() > self.config.runtime.max_keep_count {
            lows.pop_front();
        }

        let volumes = self
            .volumes
            .entry(kline.symbol.clone())
            .or_insert_with(VecDeque::new);
        volumes.push_back(kline.volume);
        if volumes.len() > self.config.runtime.max_keep_count {
            volumes.pop_front();
        }

        let counts = self
            .counts
            .entry(kline.symbol.clone())
            .or_insert_with(VecDeque::new);
        counts.push_back(kline.count as f64);
        if counts.len() > self.config.runtime.max_keep_count {
            counts.pop_front();
        }
    }

    fn compute_all_factors(&self, symbol: &str) -> Result<Vec<(String, FactorOutput)>> {
        let mut out = Vec::with_capacity(self.config.factors.len());
        let Some(closes) = self.closes.get(symbol) else {
            return Ok(out);
        };
        let Some(highs) = self.highs.get(symbol) else {
            return Ok(out);
        };
        let Some(lows) = self.lows.get(symbol) else {
            return Ok(out);
        };
        let Some(volumes) = self.volumes.get(symbol) else {
            return Ok(out);
        };
        let Some(counts) = self.counts.get(symbol) else {
            return Ok(out);
        };

        for factor in &self.config.factors {
            if !factor.enabled {
                continue;
            }

            let raw = compute_factor_value(factor, closes, highs, lows, volumes, counts)?;
            let (value, ready) = match raw {
                Some(v) => {
                    let v = apply_clip(v * factor.scale_factor, factor);
                    (v, true)
                }
                None => (0.0, false),
            };
            out.push((factor.name.clone(), FactorOutput { value, ready }));
        }

        Ok(out)
    }

    fn maybe_reload_config(&mut self) {
        let reload_every = self.config.runtime.reload_every as u64;
        if reload_every == 0 {
            return;
        }
        if self.kline_count - self.last_reload_at < reload_every {
            return;
        }

        self.last_reload_at = self.kline_count;
        match FactorPubConfig::load(&self.config_path) {
            Ok(new_cfg) => match new_cfg.validate() {
                Ok(()) => {
                    if new_cfg != self.config {
                        let current_names: Vec<&str> = self
                            .config
                            .factors
                            .iter()
                            .map(|f| f.name.as_str())
                            .collect();
                        let new_names: Vec<&str> =
                            new_cfg.factors.iter().map(|f| f.name.as_str()).collect();
                        if new_names != current_names {
                            warn!(
                                "Config reload ignored: factor list changed (requires restart). current={:?} new={:?}",
                                current_names, new_names
                            );
                            return;
                        }
                        if new_cfg.data_source.kline_channel
                            != self.config.data_source.kline_channel
                        {
                            warn!(
                                "Config reload ignored: kline_channel changed from '{}' to '{}' (requires restart)",
                                self.config.data_source.kline_channel, new_cfg.data_source.kline_channel
                            );
                            return;
                        }
                        info!(
                            "Reloaded config: channel={}, reload_every={}, max_keep_count={}, factors={}",
                            new_cfg.data_source.kline_channel,
                            new_cfg.runtime.reload_every,
                            new_cfg.runtime.max_keep_count,
                            new_cfg.factors.len(),
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
        let max_len = self.config.runtime.max_keep_count;
        for series in self.closes.values_mut() {
            while series.len() > max_len {
                series.pop_front();
            }
        }
        for series in self.highs.values_mut() {
            while series.len() > max_len {
                series.pop_front();
            }
        }
        for series in self.lows.values_mut() {
            while series.len() > max_len {
                series.pop_front();
            }
        }
        for series in self.volumes.values_mut() {
            while series.len() > max_len {
                series.pop_front();
            }
        }
        for series in self.counts.values_mut() {
            while series.len() > max_len {
                series.pop_front();
            }
        }
    }
}

fn apply_clip(value: f64, factor: &FactorDefinition) -> f64 {
    match &factor.clip {
        Some(clip) => value.clamp(clip.min, clip.max),
        None => value,
    }
}

pub fn compute_factor_value(
    factor: &FactorDefinition,
    closes: &VecDeque<f64>,
    highs: &VecDeque<f64>,
    lows: &VecDeque<f64>,
    volumes: &VecDeque<f64>,
    counts: &VecDeque<f64>,
) -> Result<Option<f64>> {
    match factor.kind {
        FactorKind::RlReturnVolatility {
            pct_change_period,
            rolling_window,
        } => compute_rl_return_volatility(closes, pct_change_period, rolling_window),
        FactorKind::HfVolStd { window } => compute_hf_vol_std(closes, window),
        FactorKind::HfHighlowRange { window } => compute_hf_highlow_range(highs, lows, window),
        FactorKind::HfSpreadReturn {
            return_period,
            ma_window,
        } => compute_hf_spread_return(closes, return_period, ma_window),
        FactorKind::HfCountMean { window } => compute_hf_count_mean(counts, window),
        FactorKind::HfVolAbsPctByVol { window } => {
            compute_hf_vol_abs_pct_by_vol(closes, volumes, window)
        }
        FactorKind::HfVolumeMean { window } => compute_hf_volume_mean(volumes, window),
        FactorKind::HfPriceVolumeCorr { window } => {
            compute_hf_price_volume_corr(closes, volumes, window)
        }
        FactorKind::HfVolVolumeCombined {
            vol_window,
            volu_window,
        } => compute_hf_vol_volume_combined(closes, volumes, vol_window, volu_window),
    }
}

pub fn compute_rl_return_volatility(
    closes: &VecDeque<f64>,
    pct_change_period: usize,
    rolling_window: usize,
) -> Result<Option<f64>> {
    let required = pct_change_period + rolling_window;
    if closes.len() < required {
        return Ok(None);
    }

    let start = closes.len() - required;
    let window: Vec<f64> = closes.iter().skip(start).copied().collect();

    let mut returns: Vec<Option<f64>> = vec![None; window.len()];
    for idx in pct_change_period..window.len() {
        let prev = window[idx - pct_change_period];
        if prev <= 0.0 {
            returns[idx] = None;
            continue;
        }
        returns[idx] = Some(window[idx] / prev - 1.0);
    }

    rolling_std_last(returns, rolling_window)
}

pub fn compute_hf_vol_std(closes: &VecDeque<f64>, window: usize) -> Result<Option<f64>> {
    let required = window + 1;
    if closes.len() < required {
        return Ok(None);
    }

    let start = closes.len() - required;
    let data: Vec<f64> = closes.iter().skip(start).copied().collect();
    let mut returns: Vec<Option<f64>> = vec![None; data.len()];
    for idx in 1..data.len() {
        let prev = data[idx - 1];
        if prev <= 0.0 {
            returns[idx] = None;
            continue;
        }
        returns[idx] = Some(data[idx] / prev - 1.0);
    }
    rolling_std_last(returns, window)
}

pub fn compute_hf_highlow_range(
    highs: &VecDeque<f64>,
    lows: &VecDeque<f64>,
    window: usize,
) -> Result<Option<f64>> {
    if highs.len() < window || lows.len() < window {
        return Ok(None);
    }

    let start_h = highs.len() - window;
    let start_l = lows.len() - window;
    let ranges: Vec<Option<f64>> = highs
        .iter()
        .skip(start_h)
        .zip(lows.iter().skip(start_l))
        .map(|(h, l)| Some(h - l))
        .collect();

    rolling_mean_last(ranges, window)
}

pub fn compute_hf_spread_return(
    closes: &VecDeque<f64>,
    return_period: usize,
    ma_window: usize,
) -> Result<Option<f64>> {
    let required = return_period + ma_window;
    if closes.len() < required {
        return Ok(None);
    }

    let start = closes.len() - required;
    let data: Vec<f64> = closes.iter().skip(start).copied().collect();

    let mut returns: Vec<Option<f64>> = vec![None; data.len()];
    for idx in return_period..data.len() {
        let prev = data[idx - return_period];
        if prev <= 0.0 {
            returns[idx] = None;
            continue;
        }
        returns[idx] = Some(data[idx] / prev - 1.0);
    }

    rolling_mean_last(returns, ma_window)
}

pub fn compute_hf_count_mean(counts: &VecDeque<f64>, window: usize) -> Result<Option<f64>> {
    if counts.len() < window {
        return Ok(None);
    }

    let start = counts.len() - window;
    let data: Vec<Option<f64>> = counts.iter().skip(start).copied().map(Some).collect();

    rolling_mean_last(data, window)
}

pub fn compute_hf_vol_abs_pct_by_vol(
    closes: &VecDeque<f64>,
    volumes: &VecDeque<f64>,
    window: usize,
) -> Result<Option<f64>> {
    let required = window + 1;
    if closes.len() < required || volumes.len() < required {
        return Ok(None);
    }

    let start = closes.len() - required;
    let close_data: Vec<f64> = closes.iter().skip(start).copied().collect();
    let volume_data: Vec<f64> = volumes.iter().skip(start).copied().collect();

    let mut values: Vec<Option<f64>> = vec![None; close_data.len()];
    for idx in 1..close_data.len() {
        let prev = close_data[idx - 1];
        let vol = volume_data[idx];
        if prev <= 0.0 || vol <= 0.0 {
            values[idx] = None;
            continue;
        }
        let ret_abs = (close_data[idx] / prev - 1.0).abs();
        values[idx] = Some(ret_abs / vol);
    }

    rolling_mean_last(values, window)
}

pub fn compute_hf_volume_mean(volumes: &VecDeque<f64>, window: usize) -> Result<Option<f64>> {
    if volumes.len() < window {
        return Ok(None);
    }

    let start = volumes.len() - window;
    let data: Vec<Option<f64>> = volumes.iter().skip(start).copied().map(Some).collect();
    rolling_mean_last(data, window)
}

pub fn compute_hf_price_volume_corr(
    closes: &VecDeque<f64>,
    volumes: &VecDeque<f64>,
    window: usize,
) -> Result<Option<f64>> {
    if closes.len() < window || volumes.len() < window {
        return Ok(None);
    }

    let start_close = closes.len() - window;
    let start_volume = volumes.len() - window;
    let close_data: Vec<f64> = closes.iter().skip(start_close).copied().collect();
    let volume_data: Vec<f64> = volumes.iter().skip(start_volume).copied().collect();

    let value = pearson_corr_last_window(&close_data, &volume_data);
    Ok(value)
}

pub fn compute_hf_vol_volume_combined(
    closes: &VecDeque<f64>,
    volumes: &VecDeque<f64>,
    vol_window: usize,
    volu_window: usize,
) -> Result<Option<f64>> {
    let vol = compute_hf_vol_std(closes, vol_window)?;
    let volu = compute_hf_volume_mean(volumes, volu_window)?;
    match (vol, volu) {
        (Some(v1), Some(v2)) => {
            let out = v1 * v2;
            if out.is_finite() {
                Ok(Some(out))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

pub fn rolling_std_last(values: Vec<Option<f64>>, window: usize) -> Result<Option<f64>> {
    let series = Series::new("x".into(), values);
    let options = RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
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
    Ok(Some(value))
}

pub fn rolling_mean_last(values: Vec<Option<f64>>, window: usize) -> Result<Option<f64>> {
    let series = Series::new("x".into(), values);
    let options = RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        ..Default::default()
    };
    let rolling = series.rolling_mean(options)?;
    let last_idx = rolling.len().saturating_sub(1);
    let Some(value) = rolling.f64()?.get(last_idx) else {
        return Ok(None);
    };
    if !value.is_finite() {
        return Ok(None);
    }
    Ok(Some(value))
}

pub fn pearson_corr_last_window(xs: &[f64], ys: &[f64]) -> Option<f64> {
    if xs.len() != ys.len() || xs.is_empty() {
        return None;
    }

    let n = xs.len() as f64;
    let mean_x = xs.iter().sum::<f64>() / n;
    let mean_y = ys.iter().sum::<f64>() / n;

    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;

    for (x, y) in xs.iter().zip(ys.iter()) {
        let dx = *x - mean_x;
        let dy = *y - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }

    if var_x <= 0.0 || var_y <= 0.0 {
        return None;
    }

    let corr = cov / (var_x.sqrt() * var_y.sqrt());
    if corr.is_finite() {
        Some(corr)
    } else {
        None
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
    let min_len = 8 + symbol_len + 5 * 8 + 8 + 8;
    if data.len() < min_len {
        return None;
    }

    let symbol = std::str::from_utf8(&data[8..8 + symbol_len])
        .ok()?
        .to_string();

    let mut offset = 8 + symbol_len;
    let _open = read_f64(data, &mut offset)?;
    let high = read_f64(data, &mut offset)?;
    let low = read_f64(data, &mut offset)?;
    let close = read_f64(data, &mut offset)?;
    let volume = read_f64(data, &mut offset)?;

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
    offset += 8;

    let count = read_u64(data, &mut offset)?;

    Some(KlineTick {
        symbol,
        high,
        low,
        close,
        volume,
        count,
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

fn read_u64(data: &[u8], offset: &mut usize) -> Option<u64> {
    let value = u64::from_le_bytes([
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

fn should_log_factor_symbol(symbol: &str) -> bool {
    let (base, _) = extract_assets_from_symbol(symbol);
    LOG_BASE_SYMBOLS
        .iter()
        .any(|candidate| base.eq_ignore_ascii_case(candidate))
}
