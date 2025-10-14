use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
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
use serde::Deserialize;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::signal;
use tokio::time::{interval, sleep, Instant};
use tokio_util::sync::CancellationToken;

use mkt_signal::common::iceoryx_subscriber::{
    ChannelType, MultiChannelSubscriber, SubscribeParams,
};
use mkt_signal::common::mkt_msg::AskBidSpreadMsg;
use mkt_signal::common::redis_client::{RedisClient, RedisSettings};
use mkt_signal::rolling_metrics::config::{
    load_config_from_redis, RollingConfig, DEFAULT_CONFIG_HASH_KEY, DEFAULT_OUTPUT_HASH_KEY,
};
use mkt_signal::rolling_metrics::service::{
    ensure_series_capacity, new_series_map, spawn_compute_thread, ComputeResult, SeriesMap,
    SymbolSeries,
};
#[derive(Parser, Debug)]
#[command(name = "rolling_metrics", about = "Binance 价差滑窗分位计算服务")]
struct Args {
    #[arg(long)]
    redis_url: Option<String>,
    #[arg(long, default_value = "127.0.0.1")]
    redis_host: String,
    #[arg(long, default_value_t = 6379)]
    redis_port: u16,
    #[arg(long, default_value_t = 0)]
    redis_db: i64,
    #[arg(long)]
    redis_username: Option<String>,
    #[arg(long)]
    redis_password: Option<String>,
    #[arg(long)]
    redis_prefix: Option<String>,
    #[arg(long, default_value = DEFAULT_CONFIG_HASH_KEY)]
    params_hash_key: String,
    #[arg(long)]
    output_hash_key: Option<String>,
    #[arg(long, default_value = "rolling_metrics_node")]
    iceoryx_node: String,
    #[arg(long, default_value = "binance")]
    spot_exchange: String,
    #[arg(long, default_value = "binance-futures")]
    swap_exchange: String,
    #[arg(long, default_value = "info,rolling_metrics=info,mkt_signal=info")]
    log_filter: String,
    #[arg(long)]
    symbol_socket: Option<String>,
    #[arg(long, default_value_t = 1800)]
    symbol_refresh_sec: u64,
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
struct SymbolQuotes {
    spot: QuoteState,
    swap: QuoteState,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&args.log_filter))
        .init();

    info!("rolling_metrics: starting with args {:?}", args);

    let redis_settings = build_redis_settings(&args)?;
    let redis_url = redis_settings.connection_url();

    let mut cfg_client = RedisClient::connect(redis_settings.clone()).await?;
    let (mut config, _) = load_config_from_redis(&mut cfg_client, &args.params_hash_key).await?;
    if let Some(ref custom) = args.output_hash_key {
        if !custom.trim().is_empty() {
            config.output_hash_key = custom.trim().to_string();
        }
    }
    config.finalize();
    info!(
        "rolling_metrics: config init (max_length={}, window={}, min_periods={}, refresh={}s, reload={}s, output={})",
        config.max_length,
        config.rolling_window,
        config.min_periods,
        config.refresh_sec,
        config.reload_param_sec,
        config.output_hash_key
    );

    let spot_exchange = args.spot_exchange.clone();
    let swap_exchange = args.swap_exchange.clone();
    let prefix = format!("{}_{}", spot_exchange, swap_exchange);
    let symbol_refresh_secs = args.symbol_refresh_sec;
    let symbol_socket_path = resolve_symbol_socket(&args.symbol_socket);

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

    if swap_exchange == "binance-futures" {
        match symbol_socket_path {
            Some(socket_dir) => {
                let series_map_task = Arc::clone(&series_map);
                let capacity_task = Arc::clone(&series_capacity);
                let swap_task = swap_exchange.clone();
                let prefix_task = prefix.clone();
                let interval = Duration::from_secs(symbol_refresh_secs.max(60));
                tokio::spawn(async move {
                    symbol_refresh_loop(
                        socket_dir,
                        swap_task,
                        prefix_task,
                        interval,
                        series_map_task,
                        capacity_task,
                    )
                    .await;
                });
            }
            None => {
                warn!("rolling_metrics: symbol_socket 未配置，跳过 binance-futures 定期符号刷新");
            }
        }
    }

    let params_key = args.params_hash_key.clone();
    let config_clone = Arc::clone(&config_lock);
    let series_map_clone = Arc::clone(&series_map);
    let capacity_clone = Arc::clone(&series_capacity);
    let settings_clone = redis_settings.clone();
    let output_override = args.output_hash_key.clone();
    tokio::spawn(async move {
        if let Err(err) = config_reload_loop(
            settings_clone,
            params_key,
            config_clone,
            series_map_clone,
            capacity_clone,
            output_override,
        )
        .await
        {
            error!("rolling_metrics: config reload loop exited with error: {err:?}");
        }
    });

    run_reader_loop(args, series_map, series_capacity).await?;
    Ok(())
}

fn build_redis_settings(args: &Args) -> Result<RedisSettings> {
    let mut settings = RedisSettings::default();
    if let Some(url) = &args.redis_url {
        let parsed =
            url::Url::parse(url).with_context(|| format!("解析 redis_url 失败: {}", url))?;
        if let Some(host) = parsed.host_str() {
            settings.host = host.to_string();
        }
        if let Some(port) = parsed.port() {
            settings.port = port;
        }
        if let Some(pass) = parsed.password() {
            settings.password = Some(pass.to_string());
        }
        let user = parsed.username();
        if !user.is_empty() {
            settings.username = Some(user.to_string());
        }
        let path = parsed.path().trim_start_matches('/');
        if !path.is_empty() {
            let db_str = path.split('/').next().unwrap_or(path);
            if let Ok(db) = db_str.parse::<i64>() {
                settings.db = db;
            }
        }
        if parsed.scheme() == "rediss" {
            warn!("rediss:// 暂未启用 TLS，按 redis:// 处理");
        }
    } else {
        settings.host = args.redis_host.clone();
        settings.port = args.redis_port;
        settings.db = args.redis_db;
        settings.username = args.redis_username.clone();
        settings.password = args.redis_password.clone();
    }
    settings.prefix = args.redis_prefix.clone();
    Ok(settings)
}

#[derive(Deserialize)]
struct SymbolSocketConfig {
    symbol_socket: Option<String>,
}

fn resolve_symbol_socket(cli_value: &Option<String>) -> Option<String> {
    if let Some(path) = cli_value.as_ref() {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    let config_path = Path::new("config/mkt_cfg.yaml");
    if !config_path.exists() {
        return None;
    }

    match fs::read_to_string(config_path) {
        Ok(content) => match serde_yaml::from_str::<SymbolSocketConfig>(&content) {
            Ok(cfg) => cfg.symbol_socket.and_then(|s| {
                let trimmed = s.trim().to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            }),
            Err(err) => {
                warn!(
                    "rolling_metrics: 解析 config/mkt_cfg.yaml 失败，无法加载 symbol_socket: {err:?}"
                );
                None
            }
        },
        Err(err) => {
            warn!(
                "rolling_metrics: 读取 config/mkt_cfg.yaml 失败，无法加载 symbol_socket: {err:?}"
            );
            None
        }
    }
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
                    new_cfg.output_hash_key = override_key.clone();
                } else if new_cfg.output_hash_key.is_empty() {
                    new_cfg.output_hash_key = DEFAULT_OUTPUT_HASH_KEY.to_string();
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
                    info!("rolling_metrics: config updated -> {}", detail);
                }
            }
            Err(err) => {
                warn!("rolling_metrics: reload config failed: {err:?}");
            }
        }
    }
}

async fn run_reader_loop(
    args: Args,
    series_map: Arc<SeriesMap>,
    series_capacity: Arc<AtomicUsize>,
) -> Result<()> {
    let mut subscriber = MultiChannelSubscriber::new(&args.iceoryx_node)?;
    subscriber.subscribe_channels(vec![
        SubscribeParams {
            exchange: args.spot_exchange.clone(),
            channel: ChannelType::AskBidSpread,
        },
        SubscribeParams {
            exchange: args.swap_exchange.clone(),
            channel: ChannelType::AskBidSpread,
        },
    ])?;

    let prefix = format!("{}_{}", args.spot_exchange, args.swap_exchange);
    let mut quotes: HashMap<String, SymbolQuotes> = HashMap::new();
    let mut last_symbol_count: usize = 0;
    let shutdown = CancellationToken::new();
    setup_signal_handlers(&shutdown)?;

    info!(
        "rolling_metrics: reader loop started (prefix={}, spot={}, futures={})",
        prefix, args.spot_exchange, args.swap_exchange
    );

    let mut next_log = Instant::now() + Duration::from_secs(30);

    loop {
        if shutdown.is_cancelled() {
            info!("rolling_metrics: shutdown requested");
            break;
        }

        for msg in
            subscriber.poll_channel(&args.spot_exchange, &ChannelType::AskBidSpread, Some(64))
        {
            process_spot_msg(&msg, &prefix, &mut quotes, &series_map, &series_capacity);
        }

        for msg in
            subscriber.poll_channel(&args.swap_exchange, &ChannelType::AskBidSpread, Some(64))
        {
            process_swap_msg(&msg, &prefix, &mut quotes, &series_map, &series_capacity);
        }

        let current_total = series_map.len();
        if Instant::now() >= next_log {
            info!(
                "rolling_metrics: symbols tracked={}, registry={}",
                quotes.len(),
                current_total
            );
            next_log += Duration::from_secs(30);
        }
        if current_total < last_symbol_count {
            info!(
                "rolling_metrics: symbol registry shrink {} -> {} ({} removed)",
                last_symbol_count,
                current_total,
                last_symbol_count - current_total
            );
        }
        last_symbol_count = current_total;

        tokio::task::yield_now().await;
    }

    Ok(())
}

fn process_spot_msg(
    msg: &[u8],
    prefix: &str,
    quotes: &mut HashMap<String, SymbolQuotes>,
    series_map: &Arc<SeriesMap>,
    series_capacity: &Arc<AtomicUsize>,
) {
    let symbol = AskBidSpreadMsg::get_symbol(msg).to_uppercase();
    if should_skip_symbol(&symbol) {
        return;
    }
    let bid = AskBidSpreadMsg::get_bid_price(msg);
    let ask = AskBidSpreadMsg::get_ask_price(msg);
    let ts = AskBidSpreadMsg::get_timestamp(msg);
    if bid <= 0.0 || ask <= 0.0 {
        return;
    }
    let entry = quotes.entry(symbol.clone()).or_default();
    entry.spot.update(bid, ask, ts);
    maybe_push_sr(prefix, &symbol, entry, series_map, series_capacity);
}

fn process_swap_msg(
    msg: &[u8],
    prefix: &str,
    quotes: &mut HashMap<String, SymbolQuotes>,
    series_map: &Arc<SeriesMap>,
    series_capacity: &Arc<AtomicUsize>,
) {
    let symbol = AskBidSpreadMsg::get_symbol(msg).to_uppercase();
    if should_skip_symbol(&symbol) {
        return;
    }
    let bid = AskBidSpreadMsg::get_bid_price(msg);
    let ask = AskBidSpreadMsg::get_ask_price(msg);
    let ts = AskBidSpreadMsg::get_timestamp(msg);
    if bid <= 0.0 || ask <= 0.0 {
        return;
    }
    let entry = quotes.entry(symbol.clone()).or_default();
    entry.swap.update(bid, ask, ts);
    let key = format!("{}::{}", prefix, &symbol);
    let capacity = series_capacity.load(Ordering::SeqCst).max(1);
    let _ = get_or_insert_series(&*series_map, &key, capacity);
    maybe_push_sr(prefix, &symbol, entry, series_map, series_capacity);
}

fn maybe_push_sr(
    prefix: &str,
    symbol: &str,
    quotes: &SymbolQuotes,
    series_map: &Arc<SeriesMap>,
    series_capacity: &Arc<AtomicUsize>,
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

    let key = format!("{}::{}", prefix, symbol);
    let capacity = series_capacity.load(Ordering::SeqCst).max(1);
    let series = get_or_insert_series(&*series_map, &key, capacity);
    series.bidask.push(bidask_sr);
    series.askbid.push(askbid_sr);
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
                "rolling_metrics: ringbuffer_snapshot rss_kb={} symbols={} total_bidask={} total_askbid={}\n{}",
                rss_kb, entries.len(), total_bidask, total_askbid, table
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
    let mut widths = headers
        .iter()
        .map(|h| h.len())
        .collect::<Vec<_>>();

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
        let row = format_row([
            symbol.as_str(),
            &bidask.to_string(),
            &askbid.to_string(),
        ]);
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
    symbol_socket: String,
    swap_exchange: String,
    prefix: String,
    interval: Duration,
    series_map: Arc<SeriesMap>,
    series_capacity: Arc<AtomicUsize>,
) {
    debug!(
        "rolling_metrics: symbol refresh loop started (swap={}, socket={}, interval={}s)",
        swap_exchange,
        symbol_socket,
        interval.as_secs()
    );

    let mut first = true;
    loop {
        if !first {
            sleep(interval).await;
        } else {
            first = false;
        }

        match fetch_binance_futures_symbols(&symbol_socket).await {
            Ok(mut symbols) => {
                symbols.sort();
                symbols.dedup();
                let capacity = series_capacity.load(Ordering::SeqCst).max(1);
                let stats = apply_symbol_snapshot(&prefix, &symbols, capacity, &series_map);
                info!(
                    "rolling_metrics: symbol snapshot applied (total={}, added={}, removed={})",
                    stats.total, stats.added, stats.removed
                );
            }
            Err(err) => {
                warn!("rolling_metrics: refresh binance-futures symbols failed: {err:?}");
            }
        }
    }
}

async fn fetch_binance_futures_symbols(socket_dir: &str) -> Result<Vec<String>> {
    let base = socket_dir.trim_end_matches('/');
    let socket_path = format!("{}/binance-futures.sock", base);
    let mut stream = UnixStream::connect(&socket_path)
        .await
        .with_context(|| format!("连接 symbol socket 失败: {}", socket_path))?;

    let mut buffer = Vec::with_capacity(16 * 1024);
    stream
        .read_to_end(&mut buffer)
        .await
        .context("读取 symbol socket 失败")?;

    let value: Value = serde_json::from_slice(&buffer)?;
    let symbols = value["symbols"]
        .as_array()
        .context("symbol socket 响应缺少 symbols 数组")?
        .iter()
        .filter(|entry| entry["type"].as_str() == Some("perpetual"))
        .filter_map(|entry| entry["symbol_id"].as_str())
        .filter(|sym| sym.to_ascii_lowercase().ends_with("usdt"))
        .filter_map(|sym| {
            let upper = sym.to_uppercase();
            let base = upper.strip_suffix("USDT").unwrap_or(&upper);
            let digit_prefix = base
                .chars()
                .take_while(|ch| ch.is_ascii_digit())
                .count();
            if digit_prefix >= 3 {
                // 跳过自带乘数的合约（如 1000PEPEUSDT），与现货单位不一致。
                None
            } else {
                Some(upper)
            }
        })
        .collect::<Vec<_>>();
    Ok(symbols)
}

fn apply_symbol_snapshot(
    prefix: &str,
    symbols: &[String],
    capacity: usize,
    series_map: &SeriesMap,
) -> SymbolSyncStats {
    let mut added = 0usize;
    let mut keep: HashSet<String> = HashSet::with_capacity(symbols.len());

    for sym in symbols {
        let key = format!("{}::{}", prefix, sym);
        if keep.insert(key.clone()) && !series_map.contains_key(&key) {
            added += 1;
        }
        let _ = get_or_insert_series(series_map, &key, capacity);
    }

    let mut remove_keys = Vec::new();
    for entry in series_map.iter() {
        let key = entry.key();
        if key.starts_with(prefix) && !keep.contains(key.as_str()) {
            remove_keys.push(key.clone());
        }
    }
    for key in remove_keys.iter() {
        series_map.remove(key);
        info!("rolling_metrics: removed inactive symbol {}", key);
    }

    SymbolSyncStats {
        total: keep.len(),
        added,
        removed: remove_keys.len(),
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

fn should_skip_symbol(symbol: &str) -> bool {
    if !symbol.ends_with("USDT") {
        return false;
    }
    let base = symbol.strip_suffix("USDT").unwrap_or(symbol);
    base.chars()
        .take_while(|ch| ch.is_ascii_digit())
        .count()
        >= 3
}

fn describe_config_changes(prev: &RollingConfig, new: &RollingConfig) -> Option<String> {
    let mut parts = Vec::new();
    if prev.max_length != new.max_length {
        parts.push(format!("max_length {}->{}", prev.max_length, new.max_length));
    }
    if prev.rolling_window != new.rolling_window {
        parts.push(format!("rolling_window {}->{}", prev.rolling_window, new.rolling_window));
    }
    if prev.min_periods != new.min_periods {
        parts.push(format!("min_periods {}->{}", prev.min_periods, new.min_periods));
    }
    if (prev.bidask_lower_quantile - new.bidask_lower_quantile).abs() > f32::EPSILON {
        parts.push(format!(
            "bidask_lower_quantile {:.6}-> {:.6}",
            prev.bidask_lower_quantile, new.bidask_lower_quantile
        ));
    }
    if (prev.bidask_upper_quantile - new.bidask_upper_quantile).abs() > f32::EPSILON {
        parts.push(format!(
            "bidask_upper_quantile {:.6}-> {:.6}",
            prev.bidask_upper_quantile, new.bidask_upper_quantile
        ));
    }
    if (prev.askbid_lower_quantile - new.askbid_lower_quantile).abs() > f32::EPSILON {
        parts.push(format!(
            "askbid_lower_quantile {:.6}-> {:.6}",
            prev.askbid_lower_quantile, new.askbid_lower_quantile
        ));
    }
    if (prev.askbid_upper_quantile - new.askbid_upper_quantile).abs() > f32::EPSILON {
        parts.push(format!(
            "askbid_upper_quantile {:.6}-> {:.6}",
            prev.askbid_upper_quantile, new.askbid_upper_quantile
        ));
    }
    if prev.refresh_sec != new.refresh_sec {
        parts.push(format!("refresh_sec {}->{}", prev.refresh_sec, new.refresh_sec));
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
                        "rolling_metrics: failed to create Redis client: {err:?}, retrying in 5s"
                    );
                    thread::sleep(Duration::from_secs(5));
                }
            }
        };

        let mut conn = loop {
            match client.get_connection() {
                Ok(c) => break c,
                Err(err) => {
                    error!("rolling_metrics: failed to connect Redis: {err:?}, retrying in 5s");
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
                                "rolling_metrics: redis cleanup error: {err:?}, attempting reconnect"
                            );
                            loop {
                                match client.get_connection() {
                                    Ok(c) => {
                                        conn = c;
                                        break;
                                    }
                                    Err(err) => {
                                        error!(
                                            "rolling_metrics: reconnect redis failed: {err:?}, retrying in 5s"
                                        );
                                        thread::sleep(Duration::from_secs(5));
                                    }
                                }
                            }
                            let _ = write_hash_and_cleanup(
                                &mut conn,
                                &result.output_key,
                                &[],
                                &stale,
                            );
                        } else {
                            info!(
                                "rolling_metrics: initial cleanup removed {} fields from {}",
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
                error!("rolling_metrics: redis write error: {err:?}, attempting reconnect");
                loop {
                    match client.get_connection() {
                        Ok(c) => {
                            conn = c;
                            break;
                        }
                        Err(err) => {
                            error!(
                                "rolling_metrics: reconnect redis failed: {err:?}, retrying in 5s"
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
                    "rolling_metrics: wrote {} fields, removed {} from {} (processed={}, skipped={}, duration={}ms)",
                    result.payloads.len(),
                    removals.len(),
                    result.output_key,
                    result.stats.processed,
                    result.stats.skipped,
                    result.stats.duration_ms
                );
            }
        }

        info!("rolling_metrics: writer thread exiting (channel closed)");
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
            error!("rolling_metrics: listen ctrl_c failed: {}", e);
            return;
        }
        info!("rolling_metrics: received Ctrl+C");
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
                        info!("rolling_metrics: received SIGTERM");
                        term_token.cancel();
                    }
                }
                Err(e) => error!("rolling_metrics: listen SIGTERM failed: {}", e),
            }
        });
    }
    Ok(())
}
