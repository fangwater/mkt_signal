use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use crossbeam_channel::unbounded;
use dashmap::mapref::entry::Entry;
use log::{debug, error, info, warn};
use parking_lot::RwLock;
use reqwest::Client;
use serde::Deserialize;
use tokio::signal;
use tokio::time::{interval, sleep, Instant};
use tokio_util::sync::CancellationToken;

use mkt_signal::common::iceoryx_subscriber::{
    ChannelType, MultiChannelSubscriber, SubscribeParams,
};
use mkt_signal::common::mkt_msg::{
    get_msg_type, AskBidSpreadMsg, FundingRateMsg, IndexPriceMsg, MarkPriceMsg, MktMsgType,
};
use mkt_signal::common::redis_client::{RedisClient, RedisSettings};
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::rolling_metrics::config::{
    load_config_from_redis, FactorConfig, RollingConfig, DEFAULT_CONFIG_HASH_KEY,
    DEFAULT_OUTPUT_HASH_KEY, FACTOR_ASKBID, FACTOR_BIDASK, FACTOR_HEDGE_PREMIUM_RATE,
    FACTOR_OPEN_PREMIUM_RATE, FACTOR_SPREAD, FACTOR_SPREAD_FR,
};
use mkt_signal::rolling_metrics::ring::RingBuffer;
use mkt_signal::rolling_metrics::service::{
    ensure_series_capacity, init_log_prefix, log_prefix, new_series_map, spawn_compute_thread,
    ComputeResult, SeriesMap, SymbolSeries,
};
use mkt_signal::symbol_match::normalize_symbol_for_pairing;

#[derive(Parser, Debug)]
#[command(
    name = "rolling-metrics",
    about = "滑窗分位计算服务 - 任意 venue 组合",
    long_about = "滑窗分位计算服务。\n\n示例 venue：\n  - binance-spot / binance-futures\n  - okex-spot / okex-futures\n  - bybit-spot / bybit-futures\n  - bitget-spot / bitget-futures\n  - gate-spot / gate-futures\n\nopen/hedge 可自由组合，对应 Redis key: rolling_metrics_params_{open}_{hedge} / rolling_metrics_thresholds_{open}_{hedge}"
)]
struct Args {
    #[arg(long)]
    redis_prefix: Option<String>,
    #[arg(long)]
    params_hash_key: Option<String>,
    #[arg(long)]
    output_hash_key: Option<String>,
    #[arg(long)]
    iceoryx_node: Option<String>,
    #[arg(long, default_value = "info,rolling_metrics=info,mkt_signal=info")]
    log_filter: String,
    #[arg(long, default_value_t = 1800)]
    symbol_refresh_sec: u64,
    /// open 侧 dat_pbs 前缀（必填，如 binance-spot，可与 hedge 任意组合）
    #[arg(long)]
    open_venue: String,
    /// hedge 侧 dat_pbs 前缀（必填，如 binance-futures，可与 open 任意组合）
    #[arg(long)]
    hedge_venue: String,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceSymbolInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSymbolInfo {
    symbol: String,
    status: String,
    quote_asset: String,
    #[serde(default)]
    contract_type: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct QuoteState {
    bid: f64,
    ask: f64,
    ts: i64,
    ready: bool,
}

impl QuoteState {
    fn update(&mut self, bid: f64, ask: f64, ts: i64) {
        self.bid = bid;
        self.ask = ask;
        self.ts = ts;
        self.ready = true;
    }
}

#[derive(Debug, Default, Clone)]
struct PremiumPriceState {
    mark_price: Option<f64>,
    index_price: Option<f64>,
}

impl PremiumPriceState {
    fn update_mark_price(&mut self, mark_price: f64) {
        self.mark_price = Some(mark_price);
    }

    fn update_index_price(&mut self, index_price: f64) {
        self.index_price = Some(index_price);
    }

    fn premium_rate(&self) -> Option<f64> {
        compute_premium_rate(self.mark_price, self.index_price)
    }
}

#[derive(Debug, Clone)]
struct SymbolQuotes {
    spot: QuoteState,
    swap: QuoteState,
    open_funding_rate: Option<f64>,
    hedge_funding_rate: Option<f64>,
    open_premium: PremiumPriceState,
    hedge_premium: PremiumPriceState,
    factor_states: HashMap<String, FactorResampleState>,
    started: bool,
}

impl Default for SymbolQuotes {
    fn default() -> Self {
        Self {
            spot: QuoteState::default(),
            swap: QuoteState::default(),
            open_funding_rate: None,
            hedge_funding_rate: None,
            open_premium: PremiumPriceState::default(),
            hedge_premium: PremiumPriceState::default(),
            factor_states: HashMap::new(),
            started: false,
        }
    }
}

#[derive(Debug, Clone)]
struct FactorResampleState {
    interval_ms: u64,
    current_bucket_start: Option<i64>,
    samples: Vec<f32>,
    last_value: Option<f32>,
    last_emitted_bucket: Option<i64>,
}

impl Default for FactorResampleState {
    fn default() -> Self {
        Self {
            interval_ms: 0,
            current_bucket_start: None,
            samples: Vec::with_capacity(8),
            last_value: None,
            last_emitted_bucket: None,
        }
    }
}

impl FactorResampleState {
    fn record(&mut self, interval_ms: u64, ts_ms: i64, value: f32, ring: &RingBuffer) {
        let interval_ms = interval_ms.max(1);
        self.ensure_interval(interval_ms, ring);

        let interval_i64 = interval_ms.min(i64::MAX as u64) as i64;
        let bucket_start = align_timestamp(ts_ms, interval_i64);

        if let Some(current_start) = self.current_bucket_start {
            if bucket_start < current_start {
                // 过时消息，直接忽略以避免污染
                return;
            }
            if bucket_start > current_start {
                self.flush_current(ring, current_start);
                let mut next_start = current_start.saturating_add(interval_i64);
                while next_start < bucket_start {
                    self.emit_ffill(ring, next_start);
                    next_start = next_start.saturating_add(interval_i64);
                }
                self.current_bucket_start = Some(bucket_start);
            }
        } else {
            self.current_bucket_start = Some(bucket_start);
        }

        self.samples.push(value);
    }

    fn flush_due(&mut self, interval_ms: u64, now_ms: i64, ring: &RingBuffer) {
        if self.current_bucket_start.is_none()
            && self.last_value.is_none()
            && self.samples.is_empty()
        {
            self.ensure_interval(interval_ms.max(1), ring);
            return;
        }

        let interval_ms = interval_ms.max(1);
        self.ensure_interval(interval_ms, ring);
        let interval_i64 = interval_ms.min(i64::MAX as u64) as i64;

        loop {
            let current_start = match self.current_bucket_start {
                Some(start) => start,
                None => break,
            };
            if now_ms < current_start.saturating_add(interval_i64) {
                break;
            }
            self.flush_current(ring, current_start);
            let next_start = current_start.saturating_add(interval_i64);
            self.current_bucket_start = Some(next_start);
        }
    }

    fn ensure_interval(&mut self, interval_ms: u64, ring: &RingBuffer) {
        let interval_ms = interval_ms.max(1);
        if self.interval_ms == interval_ms {
            return;
        }
        if self.interval_ms != 0 {
            if let Some(bucket) = self.current_bucket_start {
                self.flush_current(ring, bucket);
            }
        }
        self.interval_ms = interval_ms;
        self.current_bucket_start = None;
        self.samples.clear();
    }

    fn flush_current(&mut self, ring: &RingBuffer, bucket_start: i64) {
        if !self.samples.is_empty() {
            let avg = average(&self.samples);
            ring.push(avg);
            self.last_value = Some(avg);
            self.last_emitted_bucket = Some(bucket_start);
        } else {
            self.emit_ffill(ring, bucket_start);
        }
        self.samples.clear();
    }

    fn emit_ffill(&mut self, ring: &RingBuffer, bucket_start: i64) {
        if let Some(last) = self.last_value {
            ring.push(last);
            self.last_emitted_bucket = Some(bucket_start);
        }
    }
}

fn align_timestamp(ts_ms: i64, interval: i64) -> i64 {
    if interval <= 0 {
        return ts_ms;
    }
    (ts_ms / interval) * interval
}

fn average(values: &[f32]) -> f32 {
    if values.is_empty() {
        return 0.0;
    }
    let sum: f64 = values.iter().map(|v| *v as f64).sum();
    (sum / values.len() as f64) as f32
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&args.log_filter))
        .init();

    let open_topic = args.open_venue.trim().to_string();
    let hedge_topic = args.hedge_venue.trim().to_string();
    if open_topic.is_empty() || hedge_topic.is_empty() {
        anyhow::bail!("--open-venue 与 --hedge-venue 不能为空");
    }
    let iceoryx_node = args.iceoryx_node.clone().unwrap_or_else(|| {
        format!(
            "rolling_metrics_{}_{}",
            open_topic.replace('-', "_"),
            hedge_topic.replace('-', "_")
        )
    });

    let log_prefix_str = format!("rolling-metrics:{}_{}", open_topic, hedge_topic);
    init_log_prefix(log_prefix_str.clone());

    info!(
        "{}: starting (open_topic={}, hedge_topic={}, node={})",
        log_prefix_str, open_topic, hedge_topic, iceoryx_node
    );

    let redis_settings = build_redis_settings(&args);
    let redis_url = redis_settings.connection_url();

    // 根据 open/hedge 组合生成独立的 hash key
    let params_hash_key = args
        .params_hash_key
        .clone()
        .unwrap_or_else(|| format!("{}_{}_{}", DEFAULT_CONFIG_HASH_KEY, open_topic, hedge_topic));
    let default_output_hash_key =
        format!("{}_{}_{}", DEFAULT_OUTPUT_HASH_KEY, open_topic, hedge_topic);

    let mut cfg_client = RedisClient::connect(redis_settings.clone()).await?;
    let (mut config, _) = load_config_from_redis(&mut cfg_client, &params_hash_key).await?;

    // 设置输出 hash key
    config.output_hash_key = args
        .output_hash_key
        .as_ref()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or(default_output_hash_key);

    config.finalize();
    let factor_summary = config
        .factors_iter()
        .map(|(name, cfg)| {
            format!(
                "{}(win={} min={} res={}ms)",
                name, cfg.rolling_window, cfg.min_periods, cfg.resample_interval_ms
            )
        })
        .collect::<Vec<_>>()
        .join(" ");

    info!(
        "{}: config init (max_length={} refresh={}s reload={}s params={} output={} factors=[{}])",
        log_prefix_str,
        config.max_length,
        config.refresh_sec,
        config.reload_param_sec,
        params_hash_key,
        config.output_hash_key,
        factor_summary
    );

    let prefix = format!("{}_{}", open_topic, hedge_topic);
    let symbol_refresh_secs = args.symbol_refresh_sec;

    let series_map = Arc::new(new_series_map());
    let series_capacity = Arc::new(AtomicUsize::new(config.max_length));
    let config_lock = Arc::new(RwLock::new(config.clone()));

    let (tx, rx) = unbounded();
    spawn_compute_thread(
        Arc::clone(&series_map),
        Arc::clone(&config_lock),
        Arc::clone(&series_capacity),
        tx,
    );
    spawn_writer_thread(redis_url.clone(), rx);
    spawn_ringbuffer_monitor(Arc::clone(&series_map), Duration::from_secs(300));

    if hedge_topic.eq_ignore_ascii_case("binance-futures") {
        let series_map_task = Arc::clone(&series_map);
        let capacity_task = Arc::clone(&series_capacity);
        let swap_task = hedge_topic.clone();
        let prefix_task = prefix.clone();
        let interval = Duration::from_secs(symbol_refresh_secs.max(60));
        tokio::spawn(async move {
            symbol_refresh_loop(
                swap_task,
                prefix_task,
                interval,
                series_map_task,
                capacity_task,
            )
            .await;
        });
    }

    let config_clone = Arc::clone(&config_lock);
    let series_map_clone = Arc::clone(&series_map);
    let capacity_clone = Arc::clone(&series_capacity);
    let settings_clone = redis_settings.clone();
    let output_override = args.output_hash_key.clone();
    let params_key_clone = params_hash_key.clone();
    let log_prefix_clone = log_prefix_str.clone();
    tokio::spawn(async move {
        if let Err(err) = config_reload_loop(
            settings_clone,
            params_key_clone,
            config_clone,
            series_map_clone,
            capacity_clone,
            output_override,
        )
        .await
        {
            error!(
                "{}: config reload loop exited with error: {err:?}",
                log_prefix_clone
            );
        }
    });

    run_reader_loop(
        &iceoryx_node,
        &open_topic,
        &hedge_topic,
        series_map,
        series_capacity,
        Arc::clone(&config_lock),
    )
    .await?;
    Ok(())
}

fn build_redis_settings(args: &Args) -> RedisSettings {
    let mut settings = RedisSettings::default();
    settings.prefix = args.redis_prefix.clone();
    settings
}

async fn config_reload_loop(
    settings: RedisSettings,
    params_key: String,
    config_lock: Arc<RwLock<RollingConfig>>,
    series_map: Arc<SeriesMap>,
    series_capacity: Arc<AtomicUsize>,
    output_override: Option<String>,
) -> Result<()> {
    let mut client = RedisClient::connect(settings.clone()).await?;
    loop {
        let interval = {
            let cfg = config_lock.read();
            cfg.reload_interval()
        };
        tokio::time::sleep(interval).await;

        match load_config_from_redis(&mut client, &params_key).await {
            Ok((mut new_cfg, _map)) => {
                if let Some(ref override_key) = output_override {
                    if !override_key.trim().is_empty() {
                        new_cfg.output_hash_key = override_key.trim().to_string();
                    }
                }
                // 如果 output_hash_key 为空，使用从 params_key 推断的默认值
                if new_cfg.output_hash_key.is_empty() {
                    // 从 params_key 中提取后缀（如果有）
                    if let Some(suffix) = params_key.strip_prefix(DEFAULT_CONFIG_HASH_KEY) {
                        let clean_suffix = suffix.trim_start_matches('_');
                        new_cfg.output_hash_key = if clean_suffix.is_empty() {
                            DEFAULT_OUTPUT_HASH_KEY.to_string()
                        } else {
                            format!("{}_{}", DEFAULT_OUTPUT_HASH_KEY, clean_suffix)
                        };
                    } else {
                        new_cfg.output_hash_key = DEFAULT_OUTPUT_HASH_KEY.to_string();
                    }
                }
                let change_detail: Option<String>;
                {
                    let mut guard = config_lock.write();
                    let prev = guard.clone();
                    *guard = new_cfg.clone();
                    change_detail = describe_config_changes(&prev, &new_cfg);
                }
                series_capacity.store(new_cfg.max_length, Ordering::SeqCst);
                ensure_series_capacity(&series_map, new_cfg.max_length);
                if let Some(detail) = change_detail {
                    info!("{}: config updated -> {}", log_prefix(), detail);
                }
            }
            Err(err) => {
                warn!("{}: reload config failed: {err:?}", log_prefix());
            }
        }
    }
}

async fn run_reader_loop(
    iceoryx_node: &str,
    open_topic: &str,
    hedge_topic: &str,
    series_map: Arc<SeriesMap>,
    series_capacity: Arc<AtomicUsize>,
    config: Arc<RwLock<RollingConfig>>,
) -> Result<()> {
    let mut subscriber = MultiChannelSubscriber::new(iceoryx_node)?;
    subscriber.subscribe_channels(vec![
        SubscribeParams {
            service_root: Some("bridge".to_string()),
            topic_prefix: open_topic.to_string(),
            channel: ChannelType::AskBidSpread,
        },
        SubscribeParams {
            service_root: Some("bridge".to_string()),
            topic_prefix: hedge_topic.to_string(),
            channel: ChannelType::AskBidSpread,
        },
        SubscribeParams {
            service_root: Some("bridge".to_string()),
            topic_prefix: open_topic.to_string(),
            channel: ChannelType::Derivatives,
        },
        SubscribeParams {
            service_root: Some("bridge".to_string()),
            topic_prefix: hedge_topic.to_string(),
            channel: ChannelType::Derivatives,
        },
    ])?;

    let prefix = format!("{}_{}", open_topic, hedge_topic);
    let mut quotes: HashMap<String, SymbolQuotes> = HashMap::new();
    let mut last_symbol_count: usize = 0;
    let shutdown = CancellationToken::new();
    setup_signal_handlers(&shutdown)?;

    info!(
        "{}: reader loop started (prefix={}, open={}, hedge={})",
        log_prefix(),
        prefix,
        open_topic,
        hedge_topic
    );

    let mut next_log = Instant::now() + Duration::from_secs(30);

    loop {
        if shutdown.is_cancelled() {
            info!("{}: shutdown requested", log_prefix());
            break;
        }

        for msg in subscriber.poll_channel(open_topic, &ChannelType::AskBidSpread, Some(64)) {
            process_quote_msg(
                &msg,
                &prefix,
                open_topic,
                true,
                &mut quotes,
                &series_map,
                &series_capacity,
                &config,
            );
        }

        for msg in subscriber.poll_channel(hedge_topic, &ChannelType::AskBidSpread, Some(64)) {
            process_quote_msg(
                &msg,
                &prefix,
                hedge_topic,
                false,
                &mut quotes,
                &series_map,
                &series_capacity,
                &config,
            );
        }

        for msg in subscriber.poll_channel(open_topic, &ChannelType::Derivatives, Some(64)) {
            process_derivatives_msg(
                &msg,
                &prefix,
                open_topic,
                true,
                &mut quotes,
                &series_map,
                &series_capacity,
                &config,
            );
        }

        for msg in subscriber.poll_channel(hedge_topic, &ChannelType::Derivatives, Some(64)) {
            process_derivatives_msg(
                &msg,
                &prefix,
                hedge_topic,
                false,
                &mut quotes,
                &series_map,
                &series_capacity,
                &config,
            );
        }

        let cfg_snapshot = { config.read().clone() };
        let current_total = series_map.len();
        let current_symbols = quotes.len();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let capacity_snapshot = series_capacity.load(Ordering::SeqCst).max(1);
        for (symbol, state) in quotes.iter_mut() {
            let key = format!("{}::{}", prefix, symbol);
            let series = get_or_insert_series(&*series_map, &key, capacity_snapshot);
            for (factor_name, factor_cfg) in cfg_snapshot.factors_iter() {
                let Some(ring) = series.ring(factor_name) else {
                    continue;
                };
                let entry = state
                    .factor_states
                    .entry(factor_name.to_string())
                    .or_insert_with(FactorResampleState::default);
                entry.flush_due(
                    factor_cfg.resample_interval_ms.max(1),
                    now_ms,
                    ring.as_ref(),
                );
            }
        }
        if Instant::now() >= next_log {
            info!(
                "{}: symbols tracked={} (series entries={})",
                log_prefix(),
                current_symbols,
                current_total
            );
            next_log += Duration::from_secs(30);
        }
        if current_symbols < last_symbol_count {
            info!(
                "{}: symbol registry shrink {} -> {} ({} removed)",
                log_prefix(),
                last_symbol_count,
                current_symbols,
                last_symbol_count - current_symbols
            );
        }
        last_symbol_count = current_symbols;

        tokio::task::yield_now().await;
    }

    Ok(())
}

fn process_quote_msg(
    msg: &[u8],
    prefix: &str,
    venue_topic: &str,
    is_open_side: bool,
    quotes: &mut HashMap<String, SymbolQuotes>,
    series_map: &Arc<SeriesMap>,
    series_capacity: &Arc<AtomicUsize>,
    config: &Arc<RwLock<RollingConfig>>,
) {
    let raw_symbol = AskBidSpreadMsg::get_symbol(msg).to_uppercase();
    let symbol = normalize_symbol_for_pairing(&raw_symbol, venue_topic);
    if should_skip_symbol(&raw_symbol) {
        return;
    }
    let bid = AskBidSpreadMsg::get_bid_price(msg);
    let ask = AskBidSpreadMsg::get_ask_price(msg);
    let ts = AskBidSpreadMsg::get_timestamp(msg);
    if bid <= 0.0 || ask <= 0.0 {
        return;
    }
    let entry = quotes.entry(symbol.clone()).or_default();
    if is_open_side {
        entry.spot.update(bid, ask, ts);
    } else {
        entry.swap.update(bid, ask, ts);
    }
    maybe_push_sr(prefix, &symbol, entry, series_map, series_capacity, config);
}

fn process_derivatives_msg(
    msg: &[u8],
    prefix: &str,
    venue_topic: &str,
    is_open_side: bool,
    quotes: &mut HashMap<String, SymbolQuotes>,
    series_map: &Arc<SeriesMap>,
    series_capacity: &Arc<AtomicUsize>,
    config: &Arc<RwLock<RollingConfig>>,
) {
    match get_msg_type(msg) {
        MktMsgType::FundingRate => process_funding_msg(
            msg,
            prefix,
            venue_topic,
            is_open_side,
            quotes,
            series_map,
            series_capacity,
            config,
        ),
        MktMsgType::MarkPrice => process_premium_price_msg(
            msg,
            prefix,
            venue_topic,
            is_open_side,
            PremiumPriceField::MarkPrice,
            quotes,
            series_map,
            series_capacity,
            config,
        ),
        MktMsgType::IndexPrice => process_premium_price_msg(
            msg,
            prefix,
            venue_topic,
            is_open_side,
            PremiumPriceField::IndexPrice,
            quotes,
            series_map,
            series_capacity,
            config,
        ),
        _ => {}
    }
}

fn process_funding_msg(
    msg: &[u8],
    prefix: &str,
    venue_topic: &str,
    is_open_side: bool,
    quotes: &mut HashMap<String, SymbolQuotes>,
    series_map: &Arc<SeriesMap>,
    series_capacity: &Arc<AtomicUsize>,
    config: &Arc<RwLock<RollingConfig>>,
) {
    let raw_symbol = FundingRateMsg::get_symbol(msg).to_uppercase();
    let symbol = normalize_symbol_for_pairing(&raw_symbol, venue_topic);
    if should_skip_symbol(&raw_symbol) {
        return;
    }

    let funding_rate = FundingRateMsg::get_funding_rate(msg);
    if !funding_rate.is_finite() {
        return;
    }

    let entry = quotes.entry(symbol.clone()).or_default();
    if is_open_side {
        entry.open_funding_rate = Some(funding_rate);
    } else {
        entry.hedge_funding_rate = Some(funding_rate);
    }

    maybe_push_spread_fr(
        prefix,
        &symbol,
        entry,
        FundingRateMsg::get_timestamp(msg),
        series_map,
        series_capacity,
        config,
    );
}

enum PremiumPriceField {
    MarkPrice,
    IndexPrice,
}

fn process_premium_price_msg(
    msg: &[u8],
    prefix: &str,
    venue_topic: &str,
    is_open_side: bool,
    field: PremiumPriceField,
    quotes: &mut HashMap<String, SymbolQuotes>,
    series_map: &Arc<SeriesMap>,
    series_capacity: &Arc<AtomicUsize>,
    config: &Arc<RwLock<RollingConfig>>,
) {
    let (raw_symbol, value, ts_ms) = match field {
        PremiumPriceField::MarkPrice => (
            MarkPriceMsg::get_symbol(msg).to_uppercase(),
            MarkPriceMsg::get_mark_price(msg),
            MarkPriceMsg::get_timestamp(msg),
        ),
        PremiumPriceField::IndexPrice => (
            IndexPriceMsg::get_symbol(msg).to_uppercase(),
            IndexPriceMsg::get_index_price(msg),
            IndexPriceMsg::get_timestamp(msg),
        ),
    };

    let symbol = normalize_symbol_for_pairing(&raw_symbol, venue_topic);
    if should_skip_symbol(&raw_symbol) || !value.is_finite() || value <= 0.0 {
        return;
    }

    let entry = quotes.entry(symbol.clone()).or_default();
    let premium_state = if is_open_side {
        &mut entry.open_premium
    } else {
        &mut entry.hedge_premium
    };

    match field {
        PremiumPriceField::MarkPrice => premium_state.update_mark_price(value),
        PremiumPriceField::IndexPrice => premium_state.update_index_price(value),
    }

    maybe_push_premium_rates(
        prefix,
        &symbol,
        entry,
        ts_ms,
        series_map,
        series_capacity,
        config,
    );
}

fn maybe_push_sr(
    prefix: &str,
    symbol: &str,
    quotes: &mut SymbolQuotes,
    series_map: &Arc<SeriesMap>,
    series_capacity: &Arc<AtomicUsize>,
    config: &Arc<RwLock<RollingConfig>>,
) {
    if !quotes.spot.ready || !quotes.swap.ready {
        return;
    }
    if should_skip_symbol(symbol) {
        return;
    }
    let spot_bid = quotes.spot.bid;
    let spot_ask = quotes.spot.ask;
    let swap_bid = quotes.swap.bid;
    let swap_ask = quotes.swap.ask;
    let bidask = compute_bidask_sr(spot_bid, swap_ask);
    let askbid = compute_askbid_sr(spot_ask, swap_bid);
    let (Some(bidask_sr), Some(askbid_sr)) = (bidask, askbid) else {
        return;
    };

    let cfg_snapshot = { config.read().clone() };
    let capacity = series_capacity.load(Ordering::SeqCst).max(1);
    let spread_rate = match (
        compute_mid_price(spot_bid, spot_ask),
        compute_mid_price(swap_bid, swap_ask),
    ) {
        (Some(spot_mid), Some(swap_mid)) if spot_mid > 0.0 => {
            let rate = (spot_mid - swap_mid) / spot_mid;
            if rate.is_finite() {
                Some(rate)
            } else {
                None
            }
        }
        _ => None,
    };
    let ts_ms = (get_timestamp_us() / 1000) as i64;
    let key = format!("{}::{}", prefix, symbol);
    let series = get_or_insert_series(&*series_map, &key, capacity);
    series.set_spread_rate(spread_rate);
    if !quotes.started {
        quotes.started = true;
        info!("{}: start collecting symbol {}", log_prefix(), key);
    }

    record_factor_samples(
        quotes,
        &series,
        &cfg_snapshot,
        ts_ms,
        |factor_name| match factor_name {
            FACTOR_BIDASK => Some(bidask_sr),
            FACTOR_ASKBID => Some(askbid_sr),
            FACTOR_SPREAD => spread_rate.and_then(f64_to_f32),
            _ => None,
        },
    );
}

fn maybe_push_premium_rates(
    prefix: &str,
    symbol: &str,
    quotes: &mut SymbolQuotes,
    ts_ms: i64,
    series_map: &Arc<SeriesMap>,
    series_capacity: &Arc<AtomicUsize>,
    config: &Arc<RwLock<RollingConfig>>,
) {
    if should_skip_symbol(symbol) {
        return;
    }

    let cfg_snapshot = { config.read().clone() };
    let capacity = series_capacity.load(Ordering::SeqCst).max(1);
    let key = format!("{}::{}", prefix, symbol);
    let series = get_or_insert_series(&*series_map, &key, capacity);
    let open_premium_rate = quotes.open_premium.premium_rate();
    let hedge_premium_rate = quotes.hedge_premium.premium_rate();
    series.set_open_premium_rate_latest(open_premium_rate);
    series.set_hedge_premium_rate_latest(hedge_premium_rate);

    if !quotes.started {
        quotes.started = true;
        info!("{}: start collecting symbol {}", log_prefix(), key);
    }

    let open_premium_rate_value = open_premium_rate.and_then(f64_to_f32);
    let hedge_premium_rate_value = hedge_premium_rate.and_then(f64_to_f32);

    record_factor_samples(
        quotes,
        &series,
        &cfg_snapshot,
        ts_ms,
        |factor_name| match factor_name {
            FACTOR_OPEN_PREMIUM_RATE => open_premium_rate_value,
            FACTOR_HEDGE_PREMIUM_RATE => hedge_premium_rate_value,
            _ => None,
        },
    );
}

fn maybe_push_spread_fr(
    prefix: &str,
    symbol: &str,
    quotes: &mut SymbolQuotes,
    ts_ms: i64,
    series_map: &Arc<SeriesMap>,
    series_capacity: &Arc<AtomicUsize>,
    config: &Arc<RwLock<RollingConfig>>,
) {
    if should_skip_symbol(symbol) {
        return;
    }

    let cfg_snapshot = { config.read().clone() };
    let capacity = series_capacity.load(Ordering::SeqCst).max(1);
    let key = format!("{}::{}", prefix, symbol);
    let series = get_or_insert_series(&*series_map, &key, capacity);
    let spread_fr = compute_spread_fr(quotes.open_funding_rate, quotes.hedge_funding_rate);
    series.set_spread_fr_latest(spread_fr);

    if !quotes.started {
        quotes.started = true;
        info!("{}: start collecting symbol {}", log_prefix(), key);
    }

    let spread_fr_value = spread_fr.and_then(f64_to_f32);

    record_factor_samples(
        quotes,
        &series,
        &cfg_snapshot,
        ts_ms,
        |factor_name| match factor_name {
            FACTOR_SPREAD_FR => spread_fr_value,
            _ => None,
        },
    );
}

fn compute_premium_rate(mark_price: Option<f64>, index_price: Option<f64>) -> Option<f64> {
    match (mark_price, index_price) {
        (Some(mark_price), Some(index_price)) if index_price > 0.0 => {
            let value = (mark_price - index_price) / index_price;
            value.is_finite().then_some(value)
        }
        _ => None,
    }
}

fn compute_spread_fr(open_fr: Option<f64>, hedge_fr: Option<f64>) -> Option<f64> {
    match (open_fr, hedge_fr) {
        (Some(open_fr), Some(hedge_fr)) => {
            let value = hedge_fr - open_fr;
            value.is_finite().then_some(value)
        }
        _ => None,
    }
}

fn record_factor_samples<F>(
    quotes: &mut SymbolQuotes,
    series: &Arc<SymbolSeries>,
    cfg_snapshot: &RollingConfig,
    ts_ms: i64,
    sample_for: F,
) where
    F: Fn(&str) -> Option<f32>,
{
    for (factor_name, factor_cfg) in cfg_snapshot.factors_iter() {
        let Some(value) = sample_for(factor_name) else {
            continue;
        };

        let Some(ring) = series.ring(factor_name) else {
            continue;
        };

        let state = quotes
            .factor_states
            .entry(factor_name.to_string())
            .or_insert_with(FactorResampleState::default);
        state.record(
            factor_cfg.resample_interval_ms.max(1),
            ts_ms,
            value,
            ring.as_ref(),
        );
    }
}

fn get_or_insert_series(series_map: &SeriesMap, key: &str, capacity: usize) -> Arc<SymbolSeries> {
    if let Some(existing) = series_map.get(key) {
        let series = existing.clone();
        if series.bidask.capacity() == capacity {
            return series;
        }
    }

    match series_map.entry(key.to_string()) {
        Entry::Occupied(mut occ) => {
            let value = occ.get_mut();
            if value.bidask.capacity() != capacity {
                *value = Arc::new(SymbolSeries::new(capacity));
            }
            value.clone()
        }
        Entry::Vacant(vac) => vac.insert(Arc::new(SymbolSeries::new(capacity))).clone(),
    }
}

fn spawn_ringbuffer_monitor(series_map: Arc<SeriesMap>, period: Duration) {
    let period = if period.is_zero() {
        Duration::from_secs(300)
    } else {
        period
    };

    tokio::spawn(async move {
        let mut ticker = interval(period);
        ticker.tick().await;
        loop {
            ticker.tick().await;

            let mut entries: Vec<(String, usize, usize)> = Vec::new();
            let mut total_bidask = 0usize;
            let mut total_askbid = 0usize;

            for entry in series_map.iter() {
                let key = entry.key().clone();
                let series = entry.value().clone();
                let bidask_len = series.bidask.len();
                let askbid_len = series.askbid.len();
                total_bidask += bidask_len;
                total_askbid += askbid_len;
                entries.push((key, bidask_len, askbid_len));
            }

            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let table = build_three_line_table(&entries);
            let rss_kb = process_memory_kb().unwrap_or(0);

            info!(
                "{}: ringbuffer_snapshot rss_kb={} symbols={} total_bidask={} total_askbid={}\n{}",
                log_prefix(),
                rss_kb,
                entries.len(),
                total_bidask,
                total_askbid,
                table
            );
        }
    });
}

fn process_memory_kb() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let mut parts = statm.split_whitespace();
    let _ = parts.next()?;
    let rss_pages = parts.next()?.parse::<u64>().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    Some(rss_pages.saturating_mul(page_size as u64) / 1024)
}

fn build_three_line_table(entries: &[(String, usize, usize)]) -> String {
    let headers = ["symbol", "bidask_len", "askbid_len"];
    let mut widths = headers.iter().map(|h| h.len()).collect::<Vec<_>>();

    for (symbol, bidask, askbid) in entries {
        widths[0] = widths[0].max(symbol.len());
        widths[1] = widths[1].max(bidask.to_string().len());
        widths[2] = widths[2].max(askbid.to_string().len());
    }

    let format_row = |values: [&str; 3]| -> String {
        let mut parts = Vec::with_capacity(3);
        for (idx, value) in values.iter().enumerate() {
            parts.push(format!("{:<width$}", value, width = widths[idx]));
        }
        parts.join("  ")
    };

    let header_line = format_row(headers);
    let top_rule = "=".repeat(header_line.len());
    let mid_rule = "-".repeat(header_line.len());
    let bot_rule = "=".repeat(header_line.len());

    let mut lines = Vec::with_capacity(entries.len() + 4);
    lines.push(top_rule);
    lines.push(header_line);
    lines.push(mid_rule);

    for (symbol, bidask, askbid) in entries {
        let row = format_row([symbol.as_str(), &bidask.to_string(), &askbid.to_string()]);
        lines.push(row);
    }

    lines.push(bot_rule);
    lines.join("\n")
}

struct SymbolSyncStats {
    total: usize,
    added: usize,
    removed: usize,
}

async fn symbol_refresh_loop(
    swap_topic: String,
    prefix: String,
    interval: Duration,
    series_map: Arc<SeriesMap>,
    series_capacity: Arc<AtomicUsize>,
) {
    debug!(
        "{}: symbol refresh loop started (swap_topic={}, interval={}s)",
        log_prefix(),
        swap_topic,
        interval.as_secs()
    );

    let mut first = true;
    loop {
        if !first {
            sleep(interval).await;
        } else {
            first = false;
        }

        match fetch_binance_futures_symbols().await {
            Ok(mut symbols) => {
                symbols.sort();
                symbols.dedup();
                let capacity = series_capacity.load(Ordering::SeqCst).max(1);
                let stats = apply_symbol_snapshot(&prefix, &symbols, capacity, &series_map);
                info!(
                    "{}: symbol snapshot applied (symbols={}, added={}, removed={})",
                    log_prefix(),
                    stats.total,
                    stats.added,
                    stats.removed
                );
            }
            Err(err) => {
                warn!(
                    "{}: refresh binance-futures symbols failed: {err:?}",
                    log_prefix()
                );
            }
        }
    }
}

async fn fetch_binance_futures_symbols() -> Result<Vec<String>> {
    const URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("构建 Binance HTTP 客户端失败")?;
    let response = client
        .get(URL)
        .send()
        .await
        .context("请求 Binance exchangeInfo 失败")?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("Binance exchangeInfo 请求失败: status={}", status);
    }

    let body = response
        .text()
        .await
        .context("读取 Binance exchangeInfo 响应失败")?;
    let info: BinanceExchangeInfo =
        serde_json::from_str(&body).context("解析 Binance exchangeInfo JSON 失败")?;

    let symbols = info
        .symbols
        .into_iter()
        .filter(|entry| entry.quote_asset == "USDT")
        .filter(|entry| entry.status == "TRADING")
        .filter(|entry| entry.contract_type.as_deref() == Some("PERPETUAL"))
        .filter_map(|entry| {
            let upper = entry.symbol.to_uppercase();
            if !upper.ends_with("USDT") {
                return None;
            }
            let base = &upper[..upper.len().saturating_sub(4)];
            let digit_prefix = base
                .chars()
                .take_while(|ch: &char| ch.is_ascii_digit())
                .count();
            if digit_prefix >= 3 {
                // 跳过自带乘数的合约（如 1000PEPEUSDT），与现货单位不一致。
                None
            } else {
                Some(upper)
            }
        })
        .collect();
    Ok(symbols)
}

fn apply_symbol_snapshot(
    prefix: &str,
    symbols: &[String],
    capacity: usize,
    series_map: &SeriesMap,
) -> SymbolSyncStats {
    let mut added_symbols = 0usize;
    let mut keep: HashSet<String> = HashSet::with_capacity(symbols.len());

    for sym in symbols {
        let key = format!("{}::{}", prefix, sym);
        keep.insert(key.clone());
        let existed = series_map.contains_key(&key);
        let _ = get_or_insert_series(series_map, &key, capacity);
        if !existed {
            added_symbols += 1;
        }
    }

    let prefix_token = format!("{}::", prefix);
    let mut remove_keys = Vec::new();
    let mut removed_symbols: HashSet<String> = HashSet::new();
    for entry in series_map.iter() {
        let key = entry.key();
        if key.starts_with(&prefix_token) && !keep.contains(key.as_str()) {
            remove_keys.push(key.clone());
        }
    }
    for key in remove_keys.iter() {
        if let Some(symbol_part) = key.rsplit("::").next() {
            removed_symbols.insert(symbol_part.to_string());
        }
        series_map.remove(key);
        info!("{}: removed inactive series {}", log_prefix(), key);
    }

    SymbolSyncStats {
        total: symbols.len(),
        added: added_symbols,
        removed: removed_symbols.len(),
    }
}

fn compute_bidask_sr(spot_bid: f64, swap_ask: f64) -> Option<f32> {
    if spot_bid <= 0.0 || swap_ask <= 0.0 {
        return None;
    }
    let sr = (spot_bid - swap_ask) / spot_bid;
    if sr.is_finite() {
        Some(sr as f32)
    } else {
        None
    }
}

fn compute_askbid_sr(spot_ask: f64, swap_bid: f64) -> Option<f32> {
    if spot_ask <= 0.0 || swap_bid <= 0.0 {
        return None;
    }
    let sr = (spot_ask - swap_bid) / spot_ask;
    if sr.is_finite() {
        Some(sr as f32)
    } else {
        None
    }
}

fn compute_mid_price(bid: f64, ask: f64) -> Option<f64> {
    if bid <= 0.0 || ask <= 0.0 {
        return None;
    }
    let mid = (bid + ask) * 0.5;
    if mid.is_finite() {
        Some(mid)
    } else {
        None
    }
}

fn f64_to_f32(value: f64) -> Option<f32> {
    if value.is_finite() {
        Some(value as f32)
    } else {
        None
    }
}

fn should_skip_symbol(symbol: &str) -> bool {
    if !symbol.ends_with("USDT") {
        return false;
    }
    let base = symbol.strip_suffix("USDT").unwrap_or(symbol);
    base.chars().take_while(|ch| ch.is_ascii_digit()).count() >= 3
}

fn describe_config_changes(prev: &RollingConfig, new: &RollingConfig) -> Option<String> {
    let mut parts = Vec::new();
    if prev.max_length != new.max_length {
        parts.push(format!(
            "max_length {}->{}",
            prev.max_length, new.max_length
        ));
    }
    let fmt_quantiles = |vals: &[f32]| -> String {
        if vals.is_empty() {
            "[]".to_string()
        } else {
            let inner = vals
                .iter()
                .map(|v| format!("{:.6}", v))
                .collect::<Vec<String>>()
                .join(",");
            format!("[{}]", inner)
        }
    };
    let fmt_factor = |cfg: &FactorConfig| -> String {
        format!(
            "resample={}ms window={} min={} quantiles={}",
            cfg.resample_interval_ms,
            cfg.rolling_window,
            cfg.min_periods,
            fmt_quantiles(&cfg.quantiles)
        )
    };
    let prev_factors = &prev.factors;
    let new_factors = &new.factors;
    let mut factor_names: Vec<&str> = prev_factors
        .keys()
        .chain(new_factors.keys())
        .map(|k| k.as_str())
        .collect();
    factor_names.sort();
    factor_names.dedup();
    for factor_name in factor_names {
        let before = prev_factors.get(factor_name);
        let after = new_factors.get(factor_name);
        match (before, after) {
            (Some(bf), Some(af)) => {
                if bf.resample_interval_ms != af.resample_interval_ms {
                    parts.push(format!(
                        "factor {} resample_interval_ms {}->{}",
                        factor_name, bf.resample_interval_ms, af.resample_interval_ms
                    ));
                }
                if bf.rolling_window != af.rolling_window {
                    parts.push(format!(
                        "factor {} rolling_window {}->{}",
                        factor_name, bf.rolling_window, af.rolling_window
                    ));
                }
                if bf.min_periods != af.min_periods {
                    parts.push(format!(
                        "factor {} min_periods {}->{}",
                        factor_name, bf.min_periods, af.min_periods
                    ));
                }
                if bf.quantiles != af.quantiles {
                    parts.push(format!(
                        "factor {} quantiles {}->{}",
                        factor_name,
                        fmt_quantiles(&bf.quantiles),
                        fmt_quantiles(&af.quantiles)
                    ));
                }
            }
            (None, Some(af)) => {
                parts.push(format!("factor {} added ({})", factor_name, fmt_factor(af)));
            }
            (Some(bf), None) => {
                parts.push(format!(
                    "factor {} removed ({})",
                    factor_name,
                    fmt_factor(bf)
                ));
            }
            (None, None) => {}
        }
    }
    if prev.refresh_sec != new.refresh_sec {
        parts.push(format!(
            "refresh_sec {}->{}",
            prev.refresh_sec, new.refresh_sec
        ));
    }
    if prev.reload_param_sec != new.reload_param_sec {
        parts.push(format!(
            "reload_param_sec {}->{}",
            prev.reload_param_sec, new.reload_param_sec
        ));
    }
    if prev.output_hash_key != new.output_hash_key {
        parts.push(format!(
            "output_hash_key {}->{}",
            prev.output_hash_key, new.output_hash_key
        ));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

fn spawn_writer_thread(redis_url: String, receiver: crossbeam_channel::Receiver<ComputeResult>) {
    thread::spawn(move || {
        let client = loop {
            match redis::Client::open(redis_url.as_str()) {
                Ok(c) => break c,
                Err(err) => {
                    error!(
                        "{}: failed to create Redis client: {err:?}, retrying in 5s",
                        log_prefix()
                    );
                    thread::sleep(Duration::from_secs(5));
                }
            }
        };

        let mut conn = loop {
            match client.get_connection() {
                Ok(c) => break c,
                Err(err) => {
                    error!(
                        "{}: failed to connect Redis: {err:?}, retrying in 5s",
                        log_prefix()
                    );
                    thread::sleep(Duration::from_secs(5));
                }
            }
        };

        let mut initial_cleanup_done = false;
        while let Ok(result) = receiver.recv() {
            if result.payloads.is_empty() && result.removals.is_empty() {
                if !initial_cleanup_done {
                    if let Ok(stale) =
                        load_stale_fields(&mut conn, &result.output_key, &result.payloads)
                    {
                        initial_cleanup_done = true;
                        if stale.is_empty() {
                            continue;
                        }
                        if let Err(err) =
                            write_hash_and_cleanup(&mut conn, &result.output_key, &[], &stale)
                        {
                            error!(
                                "{}: redis cleanup error: {err:?}, attempting reconnect",
                                log_prefix()
                            );
                            loop {
                                match client.get_connection() {
                                    Ok(c) => {
                                        conn = c;
                                        break;
                                    }
                                    Err(err) => {
                                        error!(
                                            "{}: reconnect redis failed: {err:?}, retrying in 5s",
                                            log_prefix()
                                        );
                                        thread::sleep(Duration::from_secs(5));
                                    }
                                }
                            }
                            let _ =
                                write_hash_and_cleanup(&mut conn, &result.output_key, &[], &stale);
                        } else {
                            info!(
                                "{}: initial cleanup removed {} fields from {}",
                                log_prefix(),
                                stale.len(),
                                result.output_key
                            );
                        }
                        continue;
                    }
                } else {
                    continue;
                }
            }
            let mut removals = result.removals.clone();
            if removals.is_empty() && !initial_cleanup_done {
                if let Ok(stale) =
                    load_stale_fields(&mut conn, &result.output_key, &result.payloads)
                {
                    removals.extend(stale);
                }
                initial_cleanup_done = true;
            }

            if result.payloads.is_empty() && removals.is_empty() {
                continue;
            }

            if let Err(err) =
                write_hash_and_cleanup(&mut conn, &result.output_key, &result.payloads, &removals)
            {
                error!(
                    "{}: redis write error: {err:?}, attempting reconnect",
                    log_prefix()
                );
                loop {
                    match client.get_connection() {
                        Ok(c) => {
                            conn = c;
                            break;
                        }
                        Err(err) => {
                            error!(
                                "{}: reconnect redis failed: {err:?}, retrying in 5s",
                                log_prefix()
                            );
                            thread::sleep(Duration::from_secs(5));
                        }
                    }
                }
                let _ = write_hash_and_cleanup(
                    &mut conn,
                    &result.output_key,
                    &result.payloads,
                    &removals,
                );
            } else {
                info!(
                    "{}: wrote {} fields, removed {} from {} (processed={}, skipped={}, duration={}ms)",
                    log_prefix(),
                    result.payloads.len(),
                    removals.len(),
                    result.output_key,
                    result.stats.processed,
                    result.stats.skipped,
                    result.stats.duration_ms
                );
            }
        }

        info!("{}: writer thread exiting (channel closed)", log_prefix());
    });
}

fn write_hash_and_cleanup(
    conn: &mut redis::Connection,
    output_key: &str,
    payloads: &[(String, String)],
    removals: &[String],
) -> redis::RedisResult<()> {
    if !payloads.is_empty() {
        let mut cmd = redis::cmd("HSET");
        cmd.arg(output_key);
        for (field, value) in payloads {
            cmd.arg(field).arg(value);
        }
        cmd.query::<()>(conn)?;
    }

    if !removals.is_empty() {
        let mut cmd = redis::cmd("HDEL");
        cmd.arg(output_key);
        for field in removals {
            cmd.arg(field);
        }
        cmd.query::<()>(conn)?;
    }

    Ok(())
}

fn load_stale_fields(
    conn: &mut redis::Connection,
    output_key: &str,
    payloads: &[(String, String)],
) -> redis::RedisResult<Vec<String>> {
    let existing: Vec<String> = redis::cmd("HKEYS")
        .arg(output_key)
        .query(conn)
        .unwrap_or_default();
    if existing.is_empty() {
        return Ok(Vec::new());
    }
    let active: std::collections::HashSet<&str> =
        payloads.iter().map(|(field, _)| field.as_str()).collect();
    let mut stale = Vec::new();
    for field in existing {
        if !active.contains(field.as_str()) {
            stale.push(field);
        }
    }
    Ok(stale)
}

fn setup_signal_handlers(token: &CancellationToken) -> Result<()> {
    let ctrl_c = token.clone();
    tokio::spawn(async move {
        if let Err(e) = signal::ctrl_c().await {
            error!("rolling-metrics: listen ctrl_c failed: {}", e);
            return;
        }
        info!("rolling-metrics: received Ctrl+C");
        ctrl_c.cancel();
    });
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let term_token = token.clone();
        tokio::spawn(async move {
            match signal(SignalKind::terminate()) {
                Ok(mut sig) => {
                    if sig.recv().await.is_some() {
                        info!("rolling-metrics: received SIGTERM");
                        term_token.cancel();
                    }
                }
                Err(e) => error!("rolling-metrics: listen SIGTERM failed: {}", e),
            }
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{compute_premium_rate, compute_spread_fr};

    #[test]
    fn premium_rate_uses_mark_minus_index_over_index() {
        let value = compute_premium_rate(Some(101.5), Some(100.0)).expect("open_premium_rate");
        assert!((value - 0.015).abs() < 1e-12);
    }

    #[test]
    fn premium_rate_requires_positive_index() {
        assert_eq!(compute_premium_rate(Some(100.0), None), None);
        assert_eq!(compute_premium_rate(None, Some(100.0)), None);
        assert_eq!(compute_premium_rate(Some(100.0), Some(0.0)), None);
    }

    #[test]
    fn spread_fr_uses_hedge_minus_open() {
        let value = compute_spread_fr(Some(0.0025), Some(0.0031)).expect("spread_fr");
        assert!((value - 0.0006).abs() < 1e-12);
    }

    #[test]
    fn spread_fr_requires_both_sides() {
        assert_eq!(compute_spread_fr(Some(0.001), None), None);
        assert_eq!(compute_spread_fr(None, Some(0.001)), None);
    }
}
