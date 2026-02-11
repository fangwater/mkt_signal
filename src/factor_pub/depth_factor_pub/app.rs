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
const TIMER_CHECK_INTERVAL_MICROS: u64 = 500;
const LOG_BASE_SYMBOLS: [&str; 3] = ["BTC", "ETH", "SOL"];

#[derive(Debug, Clone)]
struct DepthBar {
    start_ms: i64,
    buy_amount: f64,
    sell_amount: f64,
}

impl DepthBar {
    fn new(start_ms: i64, buy_amount: f64, sell_amount: f64) -> Self {
        Self {
            start_ms,
            buy_amount,
            sell_amount,
        }
    }

    fn empty(start_ms: i64) -> Self {
        Self {
            start_ms,
            buy_amount: 0.0,
            sell_amount: 0.0,
        }
    }
}

struct SymbolState {
    bar: Option<DepthBar>,
    last_bar_start_ms: Option<i64>,
    closed_bars: Vec<DepthBar>,
}

impl SymbolState {
    fn new() -> Self {
        Self {
            bar: None,
            last_bar_start_ms: None,
            closed_bars: Vec::new(),
        }
    }

    fn apply_depth(&mut self, depth: &DepthTick, bar_ms: i64) {
        let depth_bar_start_ms = align_to_period(depth.timestamp_ms, bar_ms);

        match self.bar.as_mut() {
            None => {
                if let Some(last_start) = self.last_bar_start_ms {
                    if depth_bar_start_ms <= last_start {
                        return;
                    }
                }
                self.fill_empty_until(depth_bar_start_ms, bar_ms);
                self.bar = Some(DepthBar::new(
                    depth_bar_start_ms,
                    depth.buy_amount,
                    depth.sell_amount,
                ));
            }
            Some(bar) => {
                if depth_bar_start_ms == bar.start_ms {
                    bar.buy_amount = depth.buy_amount;
                    bar.sell_amount = depth.sell_amount;
                } else if depth_bar_start_ms > bar.start_ms {
                    if let Some(closed_bar) = self.bar.take().map(|b| self.finalize_bar(b)) {
                        self.closed_bars.push(closed_bar);
                    }
                    self.fill_empty_until(depth_bar_start_ms, bar_ms);
                    self.bar = Some(DepthBar::new(
                        depth_bar_start_ms,
                        depth.buy_amount,
                        depth.sell_amount,
                    ));
                }
            }
        }
    }

    fn finalize_bar(&mut self, bar: DepthBar) -> DepthBar {
        self.last_bar_start_ms = Some(bar.start_ms);
        bar
    }

    fn close_due_bars(&mut self, now_ms: i64, period_ms: i64) {
        if let Some(bar) = self.bar.take() {
            let close_at_ms = bar.start_ms + period_ms;
            if now_ms >= close_at_ms {
                let finalized = self.finalize_bar(bar);
                self.closed_bars.push(finalized);
            } else {
                self.bar = Some(bar);
            }
        }

        if self.bar.is_none() {
            let mut next_start = match self.last_bar_start_ms {
                Some(start) => start + period_ms,
                None => return,
            };

            while next_start + period_ms <= now_ms {
                let finalized = self.finalize_bar(DepthBar::empty(next_start));
                self.closed_bars.push(finalized);
                next_start += period_ms;
            }
        }
    }

    fn fill_empty_until(&mut self, target_start_ms: i64, period_ms: i64) {
        let Some(last_start) = self.last_bar_start_ms else {
            return;
        };

        let mut next_start = last_start + period_ms;
        while next_start < target_start_ms {
            let finalized = self.finalize_bar(DepthBar::empty(next_start));
            self.closed_bars.push(finalized);
            next_start += period_ms;
        }
    }

    fn take_closed_bars(&mut self) -> Vec<DepthBar> {
        std::mem::take(&mut self.closed_bars)
    }
}

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
    symbols: HashMap<String, SymbolState>,
    buy_amounts: HashMap<String, VecDeque<f64>>,
    sell_amounts: HashMap<String, VecDeque<f64>>,
    depth_count: u64,
    last_reload_at: u64,
    last_log_stats: Instant,
    timer_check_interval: Duration,
    last_timer_check: Instant,
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
            symbols: HashMap::new(),
            buy_amounts: HashMap::new(),
            sell_amounts: HashMap::new(),
            depth_count: 0,
            last_reload_at: 0,
            last_log_stats: Instant::now(),
            timer_check_interval: Duration::from_micros(TIMER_CHECK_INTERVAL_MICROS),
            last_timer_check: Instant::now(),
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
                    self.maybe_close_due_bars()?;
                }
            }

            if !has_message {
                self.maybe_close_due_bars()?;
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

        let state = self
            .symbols
            .entry(depth.symbol.clone())
            .or_insert_with(SymbolState::new);
        state.apply_depth(&depth, self.config.runtime.bar_ms);

        Ok(())
    }

    fn maybe_close_due_bars(&mut self) -> Result<()> {
        if self.last_timer_check.elapsed() < self.timer_check_interval {
            return Ok(());
        }

        self.last_timer_check = Instant::now();
        self.close_due_bars(now_millis())
    }

    fn close_due_bars(&mut self, now_ms: i64) -> Result<()> {
        let mut to_publish = Vec::new();
        for (symbol, state) in self.symbols.iter_mut() {
            state.close_due_bars(now_ms, self.config.runtime.bar_ms);
            let closed = state.take_closed_bars();
            if !closed.is_empty() {
                to_publish.push((symbol.clone(), closed));
            }
        }

        for (symbol, closed_bars) in to_publish {
            self.process_closed_bars(&symbol, closed_bars)?;
        }

        Ok(())
    }

    fn process_closed_bars(&mut self, symbol: &str, closed_bars: Vec<DepthBar>) -> Result<()> {
        for bar in closed_bars {
            self.push_depth_bar(symbol, &bar);
            let outputs = self.compute_all_factors(symbol)?;
            for (factor_name, out) in outputs {
                if should_log_factor_symbol(symbol) {
                    info!(
                        "depth-factor: venue={} factor={} symbol={} value={} ready={} ts_ms={}",
                        self.venue_slug, factor_name, symbol, out.value, out.ready, bar.start_ms
                    );
                }

                if !self.publisher.publish_factor(
                    &factor_name,
                    symbol,
                    out.value,
                    bar.start_ms,
                    out.ready,
                ) {
                    warn!("Failed to publish factor={} symbol={}", factor_name, symbol);
                }
            }
        }

        Ok(())
    }

    fn push_depth_bar(&mut self, symbol: &str, bar: &DepthBar) {
        let buys = self
            .buy_amounts
            .entry(symbol.to_string())
            .or_insert_with(VecDeque::new);
        buys.push_back(bar.buy_amount);
        if buys.len() > self.config.runtime.max_keep_count {
            buys.pop_front();
        }

        let sells = self
            .sell_amounts
            .entry(symbol.to_string())
            .or_insert_with(VecDeque::new);
        sells.push_back(bar.sell_amount);
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
                        if new_cfg.runtime.bar_ms != self.config.runtime.bar_ms {
                            warn!(
                                "Config reload ignored: bar_ms changed from '{}' to '{}' (requires restart)",
                                self.config.runtime.bar_ms, new_cfg.runtime.bar_ms
                            );
                            return;
                        }
                        info!(
                            "Reloaded config: channel={}, reload_every={}, max_keep_count={}, bar_ms={}, factors={}",
                            new_cfg.data_source.depth_channel,
                            new_cfg.runtime.reload_every,
                            new_cfg.runtime.max_keep_count,
                            new_cfg.runtime.bar_ms,
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
        x if x == DepthMsgType::Depth25 as u32 => 25usize,
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
        "depth25" => Some(DepthMsgType::Depth25 as u32),
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

fn align_to_period(ts_ms: i64, period_ms: i64) -> i64 {
    if period_ms <= 0 {
        return ts_ms;
    }
    ts_ms - (ts_ms % period_ms)
}

fn now_millis() -> i64 {
    let Ok(duration) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };

    let millis = duration.as_millis();
    if millis > i64::MAX as u128 {
        i64::MAX
    } else {
        millis as i64
    }
}

fn should_log_factor_symbol(symbol: &str) -> bool {
    let (base, _) = extract_assets_from_symbol(symbol);
    LOG_BASE_SYMBOLS
        .iter()
        .any(|candidate| base.eq_ignore_ascii_case(candidate))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth_tick(symbol: &str, ts_ms: i64, buy_amount: f64, sell_amount: f64) -> DepthTick {
        DepthTick {
            symbol: symbol.to_string(),
            buy_amount,
            sell_amount,
            timestamp_ms: ts_ms,
        }
    }

    #[test]
    fn close_due_bars_flushes_open_and_empty_depth_bars() {
        let mut state = SymbolState::new();
        let bar_ms = 5_000;

        state.apply_depth(&depth_tick("BTCUSDT", 1_000, 11.0, 22.0), bar_ms);
        assert!(state.take_closed_bars().is_empty());

        state.close_due_bars(5_000, bar_ms);
        let first_close = state.take_closed_bars();
        assert_eq!(first_close.len(), 1);
        assert_eq!(first_close[0].start_ms, 0);
        assert!((first_close[0].buy_amount - 11.0).abs() < 1e-12);
        assert!((first_close[0].sell_amount - 22.0).abs() < 1e-12);

        state.close_due_bars(15_000, bar_ms);
        let empties = state.take_closed_bars();
        assert_eq!(empties.len(), 2);
        assert_eq!(empties[0].start_ms, 5_000);
        assert_eq!(empties[1].start_ms, 10_000);
        assert!(empties
            .iter()
            .all(|bar| { bar.buy_amount.abs() < 1e-12 && bar.sell_amount.abs() < 1e-12 }));
    }

    #[test]
    fn same_bar_depth_uses_latest_snapshot_value() {
        let mut state = SymbolState::new();
        let bar_ms = 5_000;

        state.apply_depth(&depth_tick("BTCUSDT", 1_000, 10.0, 20.0), bar_ms);
        state.apply_depth(&depth_tick("BTCUSDT", 3_000, 15.0, 25.0), bar_ms);

        state.close_due_bars(5_000, bar_ms);
        let closed = state.take_closed_bars();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].start_ms, 0);
        assert!((closed[0].buy_amount - 15.0).abs() < 1e-12);
        assert!((closed[0].sell_amount - 25.0).abs() < 1e-12);
    }
}
