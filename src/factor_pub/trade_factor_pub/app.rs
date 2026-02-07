//! 交易因子应用主模块
//!
//! 订阅 trade 数据并计算成交行为因子

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use polars::prelude::{NamedFrom, RollingOptionsFixedWindow, Series};
use polars_time::prelude::SeriesOpsTime;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::cfg::{FactorDefinition, FactorKind, RuntimeConfig, TradeFactorPubConfig};
use super::publisher::TradeFactorPublisher;
use crate::common::mkt_msg::MktMsgType;
use crate::common::symbol_util::extract_assets_from_symbol;

const TRADE_MAX_BYTES: usize = 64;
const IDLE_SLEEP_MICROS: u64 = 200;
const LOG_BASE_SYMBOLS: [&str; 3] = ["BTC", "ETH", "SOL"];

#[derive(Debug, Clone, Copy)]
enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug)]
struct TradeTick {
    symbol: String,
    timestamp_ms: i64,
    side: TradeSide,
    price: f64,
    amount: f64,
}

#[derive(Debug, Clone)]
struct TradeBar {
    start_ms: i64,
    buy_amount: f64,
    sell_amount: f64,
    buy_volume: f64,
    sell_volume: f64,
    amount: f64,
    net_buy: f64,
    net_buy_pct: f64,
    large_order: f64,
    medium_order: f64,
    small_order: f64,
    buy_avg_price: f64,
    sell_avg_price: f64,
}

impl TradeBar {
    fn new(start_ms: i64) -> Self {
        Self {
            start_ms,
            buy_amount: 0.0,
            sell_amount: 0.0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            amount: 0.0,
            net_buy: 0.0,
            net_buy_pct: 0.0,
            large_order: 0.0,
            medium_order: 0.0,
            small_order: 0.0,
            buy_avg_price: f64::NAN,
            sell_avg_price: f64::NAN,
        }
    }

    fn empty(start_ms: i64) -> Self {
        Self::new(start_ms)
    }

    fn update(&mut self, trade: &TradeTick, cfg: &RuntimeConfig) {
        let notional = trade.price * trade.amount;
        if !notional.is_finite() || notional <= 0.0 {
            return;
        }

        match trade.side {
            TradeSide::Buy => {
                self.buy_amount += notional;
                self.buy_volume += trade.amount;
            }
            TradeSide::Sell => {
                self.sell_amount += notional;
                self.sell_volume += trade.amount;
            }
        }

        self.amount += notional;

        if notional >= cfg.large_order_notional {
            self.large_order += notional;
        } else if notional >= cfg.medium_order_notional {
            self.medium_order += notional;
        } else {
            self.small_order += notional;
        }
    }

    fn finalize(mut self) -> Self {
        self.net_buy = self.buy_amount - self.sell_amount;
        self.net_buy_pct = if self.amount > 0.0 {
            self.net_buy / self.amount
        } else {
            0.0
        };

        self.buy_avg_price = if self.buy_volume > 0.0 {
            self.buy_amount / self.buy_volume
        } else {
            f64::NAN
        };
        self.sell_avg_price = if self.sell_volume > 0.0 {
            self.sell_amount / self.sell_volume
        } else {
            f64::NAN
        };

        self
    }
}

struct SymbolState {
    bar: Option<TradeBar>,
    last_bar_start_ms: Option<i64>,
}

impl SymbolState {
    fn new() -> Self {
        Self {
            bar: None,
            last_bar_start_ms: None,
        }
    }

    fn apply_trade(&mut self, trade: &TradeTick, runtime: &RuntimeConfig) -> (Vec<TradeBar>, bool) {
        let mut closed = Vec::new();
        let mut late_trade = false;
        let trade_bar_start_ms = align_to_period(trade.timestamp_ms, runtime.bar_ms);

        match self.bar.as_mut() {
            None => {
                if let Some(last_start) = self.last_bar_start_ms {
                    if trade_bar_start_ms <= last_start {
                        return (closed, true);
                    }
                }
                self.fill_empty_until(trade_bar_start_ms, runtime.bar_ms, &mut closed);
                let mut bar = TradeBar::new(trade_bar_start_ms);
                bar.update(trade, runtime);
                self.bar = Some(bar);
            }
            Some(bar) => {
                if trade_bar_start_ms == bar.start_ms {
                    bar.update(trade, runtime);
                } else if trade_bar_start_ms > bar.start_ms {
                    if let Some(closed_bar) = self.bar.take().map(|b| self.finalize_bar(b)) {
                        closed.push(closed_bar);
                    }
                    self.fill_empty_until(trade_bar_start_ms, runtime.bar_ms, &mut closed);
                    let mut bar = TradeBar::new(trade_bar_start_ms);
                    bar.update(trade, runtime);
                    self.bar = Some(bar);
                } else {
                    late_trade = true;
                }
            }
        }

        (closed, late_trade)
    }

    fn finalize_bar(&mut self, bar: TradeBar) -> TradeBar {
        let bar = bar.finalize();
        self.last_bar_start_ms = Some(bar.start_ms);
        bar
    }

    fn fill_empty_until(&mut self, target_start_ms: i64, period_ms: i64, out: &mut Vec<TradeBar>) {
        let Some(last_start) = self.last_bar_start_ms else {
            return;
        };

        let mut next_start = last_start + period_ms;
        while next_start < target_start_ms {
            let bar = TradeBar::empty(next_start);
            out.push(self.finalize_bar(bar));
            next_start += period_ms;
        }
    }
}

#[derive(Debug, Clone)]
struct FactorOutput {
    value: f64,
    ready: bool,
}

pub struct TradeFactorPubApp {
    venue_slug: String,
    config_path: String,
    config: TradeFactorPubConfig,
    subscriber: Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()>,
    publisher: TradeFactorPublisher,
    symbols: HashMap<String, SymbolState>,

    buy_amounts: HashMap<String, VecDeque<f64>>,
    sell_amounts: HashMap<String, VecDeque<f64>>,
    amounts: HashMap<String, VecDeque<f64>>,
    net_buys: HashMap<String, VecDeque<f64>>,
    net_buy_pcts: HashMap<String, VecDeque<f64>>,
    large_orders: HashMap<String, VecDeque<f64>>,
    medium_orders: HashMap<String, VecDeque<f64>>,
    small_orders: HashMap<String, VecDeque<f64>>,
    buy_avg_prices: HashMap<String, VecDeque<f64>>,
    sell_avg_prices: HashMap<String, VecDeque<f64>>,

    trade_count: u64,
    late_trade_count: u64,
    last_reload_at: u64,
    last_log_stats: Instant,
}

impl TradeFactorPubApp {
    pub fn new(config_path: &str, venue_slug: &str) -> Result<Self> {
        let config = TradeFactorPubConfig::load(config_path)?;
        config.validate()?;

        let subscriber = Self::create_subscriber(venue_slug, &config.data_source.trade_channel)?;
        let factor_names = config
            .factors
            .iter()
            .map(|f| f.name.clone())
            .collect::<Vec<_>>();
        let publisher = TradeFactorPublisher::new(venue_slug, &factor_names)?;

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            config_path: config_path.to_string(),
            config,
            subscriber,
            publisher,
            symbols: HashMap::new(),

            buy_amounts: HashMap::new(),
            sell_amounts: HashMap::new(),
            amounts: HashMap::new(),
            net_buys: HashMap::new(),
            net_buy_pcts: HashMap::new(),
            large_orders: HashMap::new(),
            medium_orders: HashMap::new(),
            small_orders: HashMap::new(),
            buy_avg_prices: HashMap::new(),
            sell_avg_prices: HashMap::new(),

            trade_count: 0,
            late_trade_count: 0,
            last_reload_at: 0,
            last_log_stats: Instant::now(),
        })
    }

    fn create_subscriber(
        venue: &str,
        channel: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()>> {
        let node_name = format!("factor_sub_{}_trade_factor", venue.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_name = format!("data_pubs/{}/{}", venue, channel);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to trade channel: {}", service_name);
        Ok(subscriber)
    }

    pub fn run(&mut self) -> Result<()> {
        info!(
            "TradeFactorPubApp[{}] started with config: channel={}, reload_every={}, max_keep_count={}, bar_ms={}, factors={}",
            self.venue_slug,
            self.config.data_source.trade_channel,
            self.config.runtime.reload_every,
            self.config.runtime.max_keep_count,
            self.config.runtime.bar_ms,
            self.config.factors.len()
        );

        loop {
            let mut has_message = false;
            while let Some(sample) = self.subscriber.receive()? {
                has_message = true;
                let data = sample.payload().to_vec();
                if let Some(trade) = parse_trade(&data) {
                    self.handle_trade(trade)?;
                }
            }

            if !has_message {
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }

            if self.last_log_stats.elapsed() >= Duration::from_secs(60) {
                info!(
                    "TradeFactorPubApp[{}] stats: trades={}, late_trades={}",
                    self.venue_slug, self.trade_count, self.late_trade_count
                );
                self.publisher.log_stats();
                self.last_log_stats = Instant::now();
                self.trade_count = 0;
                self.late_trade_count = 0;
            }
        }
    }

    fn handle_trade(&mut self, trade: TradeTick) -> Result<()> {
        self.trade_count += 1;
        self.maybe_reload_config();

        let (closed_bars, late_trade) = {
            let state = self
                .symbols
                .entry(trade.symbol.clone())
                .or_insert_with(SymbolState::new);
            state.apply_trade(&trade, &self.config.runtime)
        };

        if late_trade {
            self.late_trade_count += 1;
        }

        for bar in closed_bars {
            self.push_trade_bar(&trade.symbol, &bar);
            let outputs = self.compute_all_factors(&trade.symbol)?;
            for (factor_name, out) in outputs {
                if should_log_factor_symbol(&trade.symbol) {
                    info!(
                        "trade-factor: venue={} factor={} symbol={} value={} ready={} ts_ms={}",
                        self.venue_slug,
                        factor_name,
                        trade.symbol,
                        out.value,
                        out.ready,
                        bar.start_ms,
                    );
                }

                if !self.publisher.publish_factor(
                    &factor_name,
                    &trade.symbol,
                    out.value,
                    bar.start_ms,
                    out.ready,
                ) {
                    warn!(
                        "Failed to publish factor={} symbol={}",
                        factor_name, trade.symbol
                    );
                }
            }
        }

        Ok(())
    }

    fn push_trade_bar(&mut self, symbol: &str, bar: &TradeBar) {
        push_value(
            &mut self.buy_amounts,
            symbol,
            bar.buy_amount,
            self.config.runtime.max_keep_count,
        );
        push_value(
            &mut self.sell_amounts,
            symbol,
            bar.sell_amount,
            self.config.runtime.max_keep_count,
        );
        push_value(
            &mut self.amounts,
            symbol,
            bar.amount,
            self.config.runtime.max_keep_count,
        );
        push_value(
            &mut self.net_buys,
            symbol,
            bar.net_buy,
            self.config.runtime.max_keep_count,
        );
        push_value(
            &mut self.net_buy_pcts,
            symbol,
            bar.net_buy_pct,
            self.config.runtime.max_keep_count,
        );
        push_value(
            &mut self.large_orders,
            symbol,
            bar.large_order,
            self.config.runtime.max_keep_count,
        );
        push_value(
            &mut self.medium_orders,
            symbol,
            bar.medium_order,
            self.config.runtime.max_keep_count,
        );
        push_value(
            &mut self.small_orders,
            symbol,
            bar.small_order,
            self.config.runtime.max_keep_count,
        );
        push_value(
            &mut self.buy_avg_prices,
            symbol,
            bar.buy_avg_price,
            self.config.runtime.max_keep_count,
        );
        push_value(
            &mut self.sell_avg_prices,
            symbol,
            bar.sell_avg_price,
            self.config.runtime.max_keep_count,
        );
    }

    fn compute_all_factors(&self, symbol: &str) -> Result<Vec<(String, FactorOutput)>> {
        let mut out = Vec::with_capacity(self.config.factors.len());

        let Some(buy_amounts) = self.buy_amounts.get(symbol) else {
            return Ok(out);
        };
        let Some(sell_amounts) = self.sell_amounts.get(symbol) else {
            return Ok(out);
        };
        let Some(amounts) = self.amounts.get(symbol) else {
            return Ok(out);
        };
        let Some(net_buys) = self.net_buys.get(symbol) else {
            return Ok(out);
        };
        let Some(net_buy_pcts) = self.net_buy_pcts.get(symbol) else {
            return Ok(out);
        };
        let Some(large_orders) = self.large_orders.get(symbol) else {
            return Ok(out);
        };
        let Some(medium_orders) = self.medium_orders.get(symbol) else {
            return Ok(out);
        };
        let Some(small_orders) = self.small_orders.get(symbol) else {
            return Ok(out);
        };
        let Some(buy_avg_prices) = self.buy_avg_prices.get(symbol) else {
            return Ok(out);
        };
        let Some(sell_avg_prices) = self.sell_avg_prices.get(symbol) else {
            return Ok(out);
        };

        for factor in &self.config.factors {
            if !factor.enabled {
                continue;
            }

            let raw = compute_factor_value(
                factor,
                buy_amounts,
                sell_amounts,
                amounts,
                net_buys,
                net_buy_pcts,
                large_orders,
                medium_orders,
                small_orders,
                buy_avg_prices,
                sell_avg_prices,
            )?;

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
        if self.trade_count - self.last_reload_at < reload_every {
            return;
        }

        self.last_reload_at = self.trade_count;
        match TradeFactorPubConfig::load(&self.config_path) {
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
                        if new_cfg.data_source.trade_channel
                            != self.config.data_source.trade_channel
                        {
                            warn!(
                                "Config reload ignored: trade_channel changed from '{}' to '{}' (requires restart)",
                                self.config.data_source.trade_channel, new_cfg.data_source.trade_channel
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
                            new_cfg.data_source.trade_channel,
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
        for map in [
            &mut self.buy_amounts,
            &mut self.sell_amounts,
            &mut self.amounts,
            &mut self.net_buys,
            &mut self.net_buy_pcts,
            &mut self.large_orders,
            &mut self.medium_orders,
            &mut self.small_orders,
            &mut self.buy_avg_prices,
            &mut self.sell_avg_prices,
        ] {
            for series in map.values_mut() {
                while series.len() > max_len {
                    series.pop_front();
                }
            }
        }
    }
}

fn push_value(
    map: &mut HashMap<String, VecDeque<f64>>,
    symbol: &str,
    value: f64,
    max_keep_count: usize,
) {
    let series = map.entry(symbol.to_string()).or_insert_with(VecDeque::new);
    series.push_back(value);
    if series.len() > max_keep_count {
        series.pop_front();
    }
}

fn apply_clip(value: f64, factor: &FactorDefinition) -> f64 {
    match &factor.clip {
        Some(clip) => value.clamp(clip.min, clip.max),
        None => value,
    }
}

#[allow(clippy::too_many_arguments)]
fn compute_factor_value(
    factor: &FactorDefinition,
    buy_amounts: &VecDeque<f64>,
    sell_amounts: &VecDeque<f64>,
    amounts: &VecDeque<f64>,
    net_buys: &VecDeque<f64>,
    net_buy_pcts: &VecDeque<f64>,
    large_orders: &VecDeque<f64>,
    medium_orders: &VecDeque<f64>,
    small_orders: &VecDeque<f64>,
    buy_avg_prices: &VecDeque<f64>,
    sell_avg_prices: &VecDeque<f64>,
) -> Result<Option<f64>> {
    match factor.kind {
        FactorKind::HfActiveRateStd { short, long } => {
            compute_hf_active_rate_std(buy_amounts, sell_amounts, short, long)
        }
        FactorKind::HfActiveRateRank { short, long } => {
            compute_hf_active_rate_rank(buy_amounts, sell_amounts, short, long)
        }
        FactorKind::HfNetbuyStd { window } => compute_hf_netbuy_std(net_buy_pcts, window),
        FactorKind::HfNetbuyRank { window } => compute_hf_netbuy_rank(net_buys, window),
        FactorKind::HfAbRate { window } => compute_hf_ab_rate(net_buy_pcts, window),
        FactorKind::HfLargeOrderStd { window } => compute_hf_large_order_std(large_orders, window),
        FactorKind::HfLargeOrderRateStd { window } => {
            compute_hf_order_rate_std(large_orders, amounts, window)
        }
        FactorKind::HfMediumOrderRateStd { window } => {
            compute_hf_order_rate_std(medium_orders, amounts, window)
        }
        FactorKind::HfSmallOrderRateStd { window } => {
            compute_hf_order_rate_std(small_orders, amounts, window)
        }
        FactorKind::HfSmallOrderRateMean { window } => {
            compute_hf_order_rate_mean(small_orders, amounts, window)
        }
        FactorKind::HfVwapDiffStd { window } => {
            compute_hf_vwap_diff_std(buy_avg_prices, sell_avg_prices, window)
        }
    }
}

fn compute_hf_active_rate_std(
    buy_amounts: &VecDeque<f64>,
    sell_amounts: &VecDeque<f64>,
    short: usize,
    long: usize,
) -> Result<Option<f64>> {
    let ratios = active_ratio_series(buy_amounts, sell_amounts, short);
    rolling_std_last(ratios, long)
}

fn compute_hf_active_rate_rank(
    buy_amounts: &VecDeque<f64>,
    sell_amounts: &VecDeque<f64>,
    short: usize,
    long: usize,
) -> Result<Option<f64>> {
    let ratios = active_ratio_series(buy_amounts, sell_amounts, short);
    Ok(rolling_rank_last(ratios, long))
}

fn active_ratio_series(
    buy_amounts: &VecDeque<f64>,
    sell_amounts: &VecDeque<f64>,
    short: usize,
) -> Vec<Option<f64>> {
    let n = buy_amounts.len().min(sell_amounts.len());
    if n == 0 || short == 0 {
        return Vec::new();
    }

    let buys: Vec<f64> = buy_amounts.iter().copied().take(n).collect();
    let sells: Vec<f64> = sell_amounts.iter().copied().take(n).collect();

    let mut out = vec![None; n];
    let mut sum_buy = 0.0;
    let mut sum_sell = 0.0;

    for idx in 0..n {
        sum_buy += buys[idx];
        sum_sell += sells[idx];

        if idx >= short {
            sum_buy -= buys[idx - short];
            sum_sell -= sells[idx - short];
        }

        if idx + 1 >= short {
            if sum_sell <= 0.0 || !sum_buy.is_finite() || !sum_sell.is_finite() {
                out[idx] = None;
            } else {
                let ratio = sum_buy / sum_sell;
                if ratio.is_finite() {
                    out[idx] = Some(ratio);
                }
            }
        }
    }

    out
}

fn compute_hf_netbuy_std(net_buy_pcts: &VecDeque<f64>, window: usize) -> Result<Option<f64>> {
    let data: Vec<Option<f64>> = net_buy_pcts.iter().copied().map(Some).collect();
    rolling_std_last(data, window)
}

fn compute_hf_netbuy_rank(net_buys: &VecDeque<f64>, window: usize) -> Result<Option<f64>> {
    let data: Vec<Option<f64>> = net_buys.iter().copied().map(Some).collect();
    Ok(rolling_rank_last(data, window))
}

fn compute_hf_ab_rate(net_buy_pcts: &VecDeque<f64>, window: usize) -> Result<Option<f64>> {
    let data: Vec<Option<f64>> = net_buy_pcts.iter().copied().map(Some).collect();
    rolling_mean_last(data, window)
}

fn compute_hf_large_order_std(large_orders: &VecDeque<f64>, window: usize) -> Result<Option<f64>> {
    let data: Vec<Option<f64>> = large_orders.iter().copied().map(Some).collect();
    rolling_std_last(data, window)
}

fn compute_hf_order_rate_std(
    order_values: &VecDeque<f64>,
    amounts: &VecDeque<f64>,
    window: usize,
) -> Result<Option<f64>> {
    let n = order_values.len().min(amounts.len());
    let data: Vec<Option<f64>> = order_values
        .iter()
        .take(n)
        .zip(amounts.iter().take(n))
        .map(|(order, amount)| {
            if *amount > 0.0 {
                let r = *order / *amount;
                if r.is_finite() {
                    Some(r)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();
    rolling_std_last(data, window)
}

fn compute_hf_order_rate_mean(
    order_values: &VecDeque<f64>,
    amounts: &VecDeque<f64>,
    window: usize,
) -> Result<Option<f64>> {
    let n = order_values.len().min(amounts.len());
    let data: Vec<Option<f64>> = order_values
        .iter()
        .take(n)
        .zip(amounts.iter().take(n))
        .map(|(order, amount)| {
            if *amount > 0.0 {
                let r = *order / *amount;
                if r.is_finite() {
                    Some(r)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();
    rolling_mean_last(data, window)
}

fn compute_hf_vwap_diff_std(
    buy_avg_prices: &VecDeque<f64>,
    sell_avg_prices: &VecDeque<f64>,
    window: usize,
) -> Result<Option<f64>> {
    let n = buy_avg_prices.len().min(sell_avg_prices.len());
    let data: Vec<Option<f64>> = buy_avg_prices
        .iter()
        .take(n)
        .zip(sell_avg_prices.iter().take(n))
        .map(|(buy_avg, sell_avg)| {
            if buy_avg.is_finite() && sell_avg.is_finite() {
                let diff = *buy_avg - *sell_avg;
                if diff.is_finite() {
                    Some(diff)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();
    rolling_std_last(data, window)
}

fn rolling_std_last(values: Vec<Option<f64>>, window: usize) -> Result<Option<f64>> {
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

fn rolling_mean_last(values: Vec<Option<f64>>, window: usize) -> Result<Option<f64>> {
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

fn rolling_rank_last(values: Vec<Option<f64>>, window: usize) -> Option<f64> {
    if values.len() < window {
        return None;
    }

    let start = values.len() - window;
    let window_values = &values[start..];

    let mut concrete = Vec::with_capacity(window);
    for value in window_values {
        concrete.push((*value)?);
    }

    let last = *concrete.last()?;
    let less = concrete.iter().filter(|x| **x < last).count() as f64;
    let equal = concrete.iter().filter(|x| **x == last).count() as f64;
    let rank = less + (equal + 1.0) / 2.0;

    if rank.is_finite() {
        Some(rank)
    } else {
        None
    }
}

fn parse_trade(data: &[u8]) -> Option<TradeTick> {
    if data.len() < 8 {
        return None;
    }

    let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if msg_type != MktMsgType::TradeInfo as u32 {
        return None;
    }

    let symbol_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let min_len = 8 + symbol_len + 8 + 8 + 8 + 8;
    if data.len() < min_len {
        return None;
    }

    let symbol = std::str::from_utf8(&data[8..8 + symbol_len])
        .ok()?
        .to_string();

    let mut offset = 8 + symbol_len;

    let _trade_id = i64::from_le_bytes([
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

    let side = match data[offset] as char {
        'B' | 'b' => TradeSide::Buy,
        'S' | 's' => TradeSide::Sell,
        _ => return None,
    };

    // side(1) + padding(7)
    offset += 8;

    let price = f64::from_le_bytes([
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

    let amount = f64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);

    if !price.is_finite()
        || !amount.is_finite()
        || price <= 0.0
        || amount <= 0.0
        || timestamp_ms <= 0
    {
        return None;
    }

    Some(TradeTick {
        symbol,
        timestamp_ms,
        side,
        price,
        amount,
    })
}

fn align_to_period(ts_ms: i64, period_ms: i64) -> i64 {
    if period_ms <= 0 {
        return ts_ms;
    }
    ts_ms - (ts_ms % period_ms)
}

fn should_log_factor_symbol(symbol: &str) -> bool {
    let (base, _) = extract_assets_from_symbol(symbol);
    LOG_BASE_SYMBOLS
        .iter()
        .any(|candidate| base.eq_ignore_ascii_case(candidate))
}
