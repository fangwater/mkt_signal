use anyhow::Result;
use chrono::Utc;
use log::{info, warn};
use reqwest::Client;
use serde::Deserialize;
use mkt_signal::common::redis_client::{RedisSettings, RedisClient};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const DEFAULT_CFG_PATH: &str = "config/funding_rate_strategy.toml";
const DEFAULT_TRACKING_PATH: &str = "config/tracking_symbol.json";

#[derive(Debug, Clone, Deserialize, Default)]
struct StrategyParams {
    #[serde(default = "default_interval")] 
    interval: usize,
    #[serde(default)]
    predict_num: usize,
    #[serde(default = "default_compute_secs")] 
    refresh_secs: u64,
    #[serde(default = "default_fetch_secs")] 
    fetch_secs: u64,
    #[serde(default = "default_fetch_offset_secs")] 
    fetch_offset_secs: u64,
    #[serde(default = "default_fetch_limit")] 
    history_limit: usize,
}
const fn default_interval() -> usize { 6 }
const fn default_compute_secs() -> u64 { 30 }
const fn default_fetch_secs() -> u64 { 7200 }
const fn default_fetch_offset_secs() -> u64 { 120 }
const fn default_fetch_limit() -> usize { 100 }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DataMode { LocalJson, Redis }
impl Default for DataMode { fn default() -> Self { DataMode::LocalJson } }

#[derive(Debug, Clone, Deserialize, Default)]
struct StrategyConfig {
    #[serde(default)]
    mode: DataMode,
    #[serde(default)]
    tracking_path: Option<String>,
    #[serde(default)]
    redis: Option<RedisSettings>,
    #[serde(default)]
    redis_key: Option<String>,
    #[serde(default)]
    strategy: StrategyParams,
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let cfg = load_cfg()?;
    let symbols = load_binance_symbols(&cfg).await?;
    let http = Client::new();

    info!(
        "demo 启动: symbols={} interval={} predict_num={} refresh={}s fetch={}s offset={}s limit={}",
        symbols.len(), cfg.strategy.interval, cfg.strategy.predict_num,
        cfg.strategy.refresh_secs, cfg.strategy.fetch_secs, cfg.strategy.fetch_offset_secs,
        cfg.strategy.history_limit
    );

    let mut history_map: HashMap<String, Vec<f64>> = HashMap::new();
    let mut predicted: HashMap<String, f64> = HashMap::new();

    // 1) 启动即 fetch + compute
    fetch_all(&http, &symbols, cfg.strategy.history_limit, &mut history_map).await;
    compute_all(&history_map, cfg.strategy.interval, cfg.strategy.predict_num, &mut predicted);
    log_pred(&predicted);

    // 2) 定时：按 refresh_secs 重算；按 fetch_secs(+offset) 对齐拉取
    let mut next_compute = Instant::now() + Duration::from_secs(cfg.strategy.refresh_secs);
    let mut next_fetch = next_fetch_instant(cfg.strategy.fetch_secs, cfg.strategy.fetch_offset_secs);

    loop {
        let now = Instant::now();
        if now >= next_fetch {
            fetch_all(&http, &symbols, cfg.strategy.history_limit, &mut history_map).await;
            next_fetch = next_fetch_instant(cfg.strategy.fetch_secs, cfg.strategy.fetch_offset_secs);
        }
        if now >= next_compute {
            compute_all(&history_map, cfg.strategy.interval, cfg.strategy.predict_num, &mut predicted);
            log_pred(&predicted);
            next_compute = Instant::now() + Duration::from_secs(cfg.strategy.refresh_secs);
        }
        sleep(Duration::from_millis(200)).await;
    }
}

fn load_cfg() -> Result<StrategyConfig> {
    let path = std::env::var("FUNDING_RATE_CFG").unwrap_or_else(|_| DEFAULT_CFG_PATH.to_string());
    let s = fs::read_to_string(&path)?;
    let mut cfg: StrategyConfig = toml::from_str(&s)?;
    if cfg.tracking_path.is_none() { cfg.tracking_path = Some(DEFAULT_TRACKING_PATH.to_string()); }
    Ok(cfg)
}

async fn load_binance_symbols(cfg: &StrategyConfig) -> Result<Vec<String>> {
    match cfg.mode {
        DataMode::LocalJson => {
            let path = PathBuf::from(cfg.tracking_path.clone().unwrap_or_else(|| DEFAULT_TRACKING_PATH.to_string()));
            let data = fs::read_to_string(&path)?;
            parse_binance_symbols(&data)
        }
        DataMode::Redis => {
            let settings = cfg.redis.clone().ok_or_else(|| anyhow::anyhow!("Redis 模式需要配置 redis 设置"))?;
            let key = cfg.redis_key.clone().unwrap_or_else(|| "tracking_symbol_json".to_string());
            let mut client = RedisClient::connect(settings).await?;
            let text = client.get_string(&key).await?.ok_or_else(|| anyhow::anyhow!("Redis 未找到 tracking JSON"))?;
            parse_binance_symbols(&text)
        }
    }
}

fn parse_binance_symbols(text: &str) -> Result<Vec<String>> {
    let value: serde_json::Value = serde_json::from_str(text)?;
    let mut out = Vec::new();
    if let Some(arr) = value.get("binance").and_then(|v| v.as_array()) {
        for entry in arr {
            match entry {
                serde_json::Value::Array(items) if items.len() >= 2 => {
                    if let Some(sym) = items.get(1).and_then(|v| v.as_str()) { out.push(sym.to_uppercase()); }
                }
                serde_json::Value::Object(map) => {
                    if let Some(sym) = map.get("futures_symbol").and_then(|v| v.as_str()) { out.push(sym.to_uppercase()); }
                }
                _ => {}
            }
        }
    }
    if out.is_empty() { anyhow::bail!("tracking_symbol 未解析到 binance futures_symbol"); }
    Ok(out)
}

fn next_fetch_instant(fetch_secs: u64, offset_secs: u64) -> Instant {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    let fetch = fetch_secs.max(600);
    let offset = offset_secs.min(fetch - 1);
    let next_slot = ((now / fetch) + 1) * fetch + offset;
    let dur = next_slot.saturating_sub(now);
    Instant::now() + Duration::from_secs(dur)
}

#[derive(Deserialize)]
struct BinanceFundingHistItem { #[serde(rename="fundingRate")] rate: String, #[serde(rename="fundingTime")] _t: Option<i64> }

async fn fetch_binance_funding_history(client: &Client, symbol: &str, limit: usize) -> Vec<f64> {
    let url = "https://fapi.binance.com/fapi/v1/fundingRate";
    let end_time = Utc::now().timestamp_millis();
    let start_time = end_time - 3 * 24 * 3600 * 1000;
    let lim = limit.max(1).min(1000).to_string();
    match client.get(url).query(&[
        ("symbol", symbol),
        ("startTime", &start_time.to_string()),
        ("endTime", &end_time.to_string()),
        ("limit", &lim),
    ]).send().await {
        Ok(resp) if resp.status().is_success() => {
            match resp.json::<Vec<BinanceFundingHistItem>>().await {
                Ok(items) => items.into_iter().filter_map(|i| i.rate.parse::<f64>().ok()).collect(),
                Err(_) => vec![],
            }
        }
        Ok(r) => { warn!("fetch {} status={}", symbol, r.status()); vec![] }
        Err(e) => { warn!("fetch {} error={}", symbol, e); vec![] }
    }
}

async fn fetch_all(client: &Client, symbols: &[String], limit: usize, out: &mut HashMap<String, Vec<f64>>) {
    let mut tasks = Vec::new();
    for s in symbols { tasks.push((s.clone(), fetch_binance_funding_history(client, s, limit))); }
    for (sym, fut) in tasks { let rates = fut.await; out.insert(sym, rates); }
}

fn compute_predict(rates: &[f64], interval: usize, predict_num: usize) -> f64 {
    if rates.is_empty() || interval == 0 { return 0.0; }
    let n = rates.len();
    if n - 1 < predict_num { return 0.0; }
    let end = n - 1 - predict_num;
    if end + 1 < interval { return 0.0; }
    let start = end + 1 - interval;
    let sum: f64 = rates[start..=end].iter().copied().sum();
    sum / (interval as f64)
}

fn compute_all(history: &HashMap<String, Vec<f64>>, interval: usize, predict_num: usize, out: &mut HashMap<String, f64>) {
    out.clear();
    for (sym, rates) in history { out.insert(sym.clone(), compute_predict(rates, interval, predict_num)); }
}

fn log_pred(predicted: &HashMap<String, f64>) {
    let mut items: Vec<_> = predicted.iter().collect();
    items.sort_by_key(|(s, _)| *s);
    let mut lines = Vec::new();
    for (s, p) in items { lines.push(format!("{}:{:.8}", s, p)); }
    info!("预测快照: {}", lines.join(", "));
}
