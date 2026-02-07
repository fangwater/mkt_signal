//! 深度因子应用主模块
//!
//! 订阅 depth 数据并计算盘口结构因子

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::cfg::{DepthFactorPubConfig, FactorDefinition, FactorKind};
use super::publisher::DepthFactorPublisher;
use crate::common::symbol_util::extract_assets_from_symbol;
use crate::depth_pub::depth_msg::DepthMsgType;

const DEPTH_MAX_BYTES: usize = 2048;
const IDLE_SLEEP_MICROS: u64 = 200;
const LOG_BASE_SYMBOLS: [&str; 3] = ["BTC", "ETH", "SOL"];

#[derive(Debug)]
struct DepthTick {
    symbol: String,
    buy_amount: f64,
    sell_amount: f64,
    timestamp_ms: i64,
}

#[derive(Debug, Clone)]
struct FactorOutput {
    value: f64,
    ready: bool,
}

pub struct DepthFactorPubApp {
    venue_slug: String,
    config_path: String,
    config: DepthFactorPubConfig,
    subscriber: Subscriber<ipc::Service, [u8; DEPTH_MAX_BYTES], ()>,
    publisher: DepthFactorPublisher,
    buy_amounts: HashMap<String, VecDeque<f64>>,
    sell_amounts: HashMap<String, VecDeque<f64>>,
    depth_count: u64,
    last_reload_at: u64,
    last_log_stats: Instant,
}

impl DepthFactorPubApp {
    pub fn new(config_path: &str, venue_slug: &str) -> Result<Self> {
        let config = DepthFactorPubConfig::load(config_path)?;
        config.validate()?;

        let subscriber = Self::create_subscriber(venue_slug, &config.data_source.depth_channel)?;
        let factor_names = config
            .factors
            .iter()
            .map(|f| f.name.clone())
            .collect::<Vec<_>>();
        let publisher = DepthFactorPublisher::new(venue_slug, &factor_names)?;

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            config_path: config_path.to_string(),
            config,
            subscriber,
            publisher,
            buy_amounts: HashMap::new(),
            sell_amounts: HashMap::new(),
            depth_count: 0,
            last_reload_at: 0,
            last_log_stats: Instant::now(),
        })
    }

    fn create_subscriber(
        venue: &str,
        channel: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; DEPTH_MAX_BYTES], ()>> {
        let node_name = format!("factor_sub_{}_depth_factor", venue.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_name = format!("depth_pubs/{}/{}", venue, channel);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; DEPTH_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to depth channel: {}", service_name);
        Ok(subscriber)
    }

    pub fn run(&mut self) -> Result<()> {
        info!(
            "DepthFactorPubApp[{}] started with config: channel={}, reload_every={}, max_keep_count={}, factors={}",
            self.venue_slug,
            self.config.data_source.depth_channel,
            self.config.runtime.reload_every,
            self.config.runtime.max_keep_count,
            self.config.factors.len()
        );

        loop {
            let mut has_message = false;
            while let Some(sample) = self.subscriber.receive()? {
                has_message = true;
                let data = sample.payload().to_vec();
                if let Some(depth) =
                    parse_depth(&data, self.config.data_source.depth_channel.as_str())
                {
                    self.handle_depth(depth)?;
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

    fn handle_depth(&mut self, depth: DepthTick) -> Result<()> {
        self.depth_count += 1;
        self.maybe_reload_config();

        self.push_depth_point(&depth);

        let outputs = self.compute_all_factors(&depth.symbol)?;
        for (factor_name, out) in outputs {
            if should_log_factor_symbol(&depth.symbol) {
                info!(
                    "depth-factor: venue={} factor={} symbol={} value={} ready={} ts_ms={}",
                    self.venue_slug,
                    factor_name,
                    depth.symbol,
                    out.value,
                    out.ready,
                    depth.timestamp_ms
                );
            }

            if !self.publisher.publish_factor(
                &factor_name,
                &depth.symbol,
                out.value,
                depth.timestamp_ms,
                out.ready,
            ) {
                warn!(
                    "Failed to publish factor={} symbol={}",
                    factor_name, depth.symbol
                );
            }
        }

        Ok(())
    }

    fn push_depth_point(&mut self, depth: &DepthTick) {
        let buys = self
            .buy_amounts
            .entry(depth.symbol.clone())
            .or_insert_with(VecDeque::new);
        buys.push_back(depth.buy_amount);
        if buys.len() > self.config.runtime.max_keep_count {
            buys.pop_front();
        }

        let sells = self
            .sell_amounts
            .entry(depth.symbol.clone())
            .or_insert_with(VecDeque::new);
        sells.push_back(depth.sell_amount);
        if sells.len() > self.config.runtime.max_keep_count {
            sells.pop_front();
        }
    }

    fn compute_all_factors(&self, symbol: &str) -> Result<Vec<(String, FactorOutput)>> {
        let mut out = Vec::with_capacity(self.config.factors.len());
        let Some(buys) = self.buy_amounts.get(symbol) else {
            return Ok(out);
        };
        let Some(sells) = self.sell_amounts.get(symbol) else {
            return Ok(out);
        };

        for factor in &self.config.factors {
            if !factor.enabled {
                continue;
            }

            let raw = compute_factor_value(factor, buys, sells);
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
        if self.depth_count - self.last_reload_at < reload_every {
            return;
        }

        self.last_reload_at = self.depth_count;
        match DepthFactorPubConfig::load(&self.config_path) {
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
                        if new_cfg.data_source.depth_channel
                            != self.config.data_source.depth_channel
                        {
                            warn!(
                                "Config reload ignored: depth_channel changed from '{}' to '{}' (requires restart)",
                                self.config.data_source.depth_channel, new_cfg.data_source.depth_channel
                            );
                            return;
                        }
                        info!(
                            "Reloaded config: channel={}, reload_every={}, max_keep_count={}, factors={}",
                            new_cfg.data_source.depth_channel,
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
        for series in self.buy_amounts.values_mut() {
            while series.len() > max_len {
                series.pop_front();
            }
        }
        for series in self.sell_amounts.values_mut() {
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

fn compute_factor_value(
    factor: &FactorDefinition,
    buys: &VecDeque<f64>,
    sells: &VecDeque<f64>,
) -> Option<f64> {
    match factor.kind {
        FactorKind::HfOrderbookBuyAmount => buys.back().copied(),
        FactorKind::HfOrderbookSellAmount => sells.back().copied(),
        FactorKind::HfOrderbookSkew { window } => compute_orderbook_skew(buys, sells, window),
    }
}

fn compute_orderbook_skew(
    buys: &VecDeque<f64>,
    sells: &VecDeque<f64>,
    window: usize,
) -> Option<f64> {
    if buys.len() < window || sells.len() < window {
        return None;
    }

    let start_buy = buys.len() - window;
    let start_sell = sells.len() - window;
    let mut ratios = Vec::with_capacity(window);

    for (buy, sell) in buys
        .iter()
        .skip(start_buy)
        .zip(sells.iter().skip(start_sell))
    {
        if *sell <= 0.0 || !buy.is_finite() || !sell.is_finite() {
            return None;
        }
        let ratio = *buy / *sell;
        if !ratio.is_finite() {
            return None;
        }
        ratios.push(ratio);
    }

    sample_skewness(&ratios)
}

fn sample_skewness(values: &[f64]) -> Option<f64> {
    let n = values.len();
    if n < 3 {
        return None;
    }

    let n_f = n as f64;
    let mean = values.iter().sum::<f64>() / n_f;

    let mut m2 = 0.0;
    let mut m3 = 0.0;
    for x in values {
        let d = *x - mean;
        m2 += d * d;
        m3 += d * d * d;
    }

    if m2 <= 0.0 {
        return None;
    }

    let m2n = m2 / n_f;
    let m3n = m3 / n_f;
    if m2n <= 0.0 {
        return None;
    }

    let g1 = m3n / m2n.powf(1.5);
    let adj = (n_f * (n_f - 1.0)).sqrt() / (n_f - 2.0);
    let skew = adj * g1;
    if skew.is_finite() {
        Some(skew)
    } else {
        None
    }
}

fn parse_depth(data: &[u8], channel: &str) -> Option<DepthTick> {
    if data.len() < 8 {
        return None;
    }

    let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let expected_type = expected_depth_msg_type(channel)?;
    if msg_type != expected_type {
        return None;
    }

    let levels = match msg_type {
        x if x == DepthMsgType::Depth5 as u32 => 5usize,
        x if x == DepthMsgType::Depth20 as u32 => 20usize,
        x if x == DepthMsgType::Depth50 as u32 => 50usize,
        _ => return None,
    };

    let symbol_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let min_len = 8 + symbol_len + 8 + levels * 16 * 2;
    if data.len() < min_len {
        return None;
    }

    let symbol = std::str::from_utf8(&data[8..8 + symbol_len])
        .ok()?
        .to_string();

    let mut offset = 8 + symbol_len;

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

    let mut buy_amount = 0.0;
    for _ in 0..levels {
        let price = read_f64(data, &mut offset)?;
        let amount = read_f64(data, &mut offset)?;
        if price > 0.0 && amount > 0.0 {
            buy_amount += price * amount;
        }
    }

    let mut sell_amount = 0.0;
    for _ in 0..levels {
        let price = read_f64(data, &mut offset)?;
        let amount = read_f64(data, &mut offset)?;
        if price > 0.0 && amount > 0.0 {
            sell_amount += price * amount;
        }
    }

    Some(DepthTick {
        symbol,
        buy_amount,
        sell_amount,
        timestamp_ms,
    })
}

fn expected_depth_msg_type(channel: &str) -> Option<u32> {
    match channel {
        "depth5" => Some(DepthMsgType::Depth5 as u32),
        "depth20" => Some(DepthMsgType::Depth20 as u32),
        "depth50" => Some(DepthMsgType::Depth50 as u32),
        _ => None,
    }
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

fn should_log_factor_symbol(symbol: &str) -> bool {
    let (base, _) = extract_assets_from_symbol(symbol);
    LOG_BASE_SYMBOLS
        .iter()
        .any(|candidate| base.eq_ignore_ascii_case(candidate))
}
