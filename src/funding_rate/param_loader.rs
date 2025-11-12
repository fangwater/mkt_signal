//! 参数加载模块 
//!
//! 从 Redis 动态加载所有配置参数，包括：
//! - 资金费率阈值（4h/8h）
//! - 订单参数
//! - 策略参数
//! - 从 Binance API 获取历史资金费率数据

use anyhow::Result;
use chrono::Utc;
use log::{debug, info, warn};
use reqwest::Client;
use serde::de::{self, Deserializer};
use serde::Deserialize;

use crate::common::redis_client::RedisSettings;

// ===== 配置结构体 =====

/// 下单模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderMode {
    Normal,
    Ladder,
}

impl Default for OrderMode {
    fn default() -> Self {
        Self::Normal
    }
}

impl OrderMode {
    pub fn from_raw(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        let lowered = trimmed.to_ascii_lowercase();
        match lowered.as_str() {
            "normal" | "ladder" => Some(if lowered == "normal" { Self::Normal } else { Self::Ladder }),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for OrderMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        OrderMode::from_raw(&raw)
            .ok_or_else(|| de::Error::custom(format!("invalid order_mode: {}", raw)))
    }
}

/// 下单配置
#[derive(Debug, Clone, Deserialize)]
pub struct OrderConfig {
    #[serde(default)]
    pub mode: OrderMode,
    #[serde(default = "default_open_ranges")]
    pub open_ranges: Vec<f64>,
    #[serde(default = "default_order_amount")]
    pub amount_u: f64,
    #[serde(default = "default_max_open_keep")]
    pub max_open_order_keep_s: u64,
    #[serde(default = "default_max_hedge_keep")]
    pub max_hedge_order_keep_s: u64,
}

const fn default_order_amount() -> f64 { 50.0 }
const fn default_max_open_keep() -> u64 { 5 }
const fn default_max_hedge_keep() -> u64 { 5 }
fn default_open_ranges() -> Vec<f64> { vec![0.0002] }

impl Default for OrderConfig {
    fn default() -> Self {
        Self {
            mode: OrderMode::default(),
            open_ranges: default_open_ranges(),
            amount_u: default_order_amount(),
            max_open_order_keep_s: default_max_open_keep(),
            max_hedge_order_keep_s: default_max_hedge_keep(),
        }
    }
}

impl OrderConfig {
    pub fn normal_open_range(&self) -> f64 {
        self.open_ranges.first().copied().unwrap_or(0.0002)
    }

    pub fn ladder_open_ranges(&self) -> &[f64] {
        if self.open_ranges.len() > 1 { &self.open_ranges[1..] } else { &[] }
    }
}

/// 策略参数
#[derive(Debug, Clone, Deserialize)]
pub struct StrategyParams {
    #[serde(default = "default_interval")]
    pub interval: usize,
    #[serde(default)]
    pub predict_num: usize,
    #[serde(default = "default_refresh_secs")]
    pub refresh_secs: u64,
    #[serde(default = "default_fetch_secs")]
    pub fetch_secs: u64,
    #[serde(default = "default_fetch_offset_secs")]
    pub fetch_offset_secs: u64,
    #[serde(default = "default_history_limit")]
    pub history_limit: usize,
    #[serde(default = "default_resample_ms")]
    pub resample_ms: u64,
    #[serde(default = "default_funding_ma_size")]
    pub funding_ma_size: usize,
    #[serde(default)]
    pub settlement_offset_secs: i64,
}

const fn default_interval() -> usize { 6 }
const fn default_refresh_secs() -> u64 { 30 }
const fn default_fetch_secs() -> u64 { 7200 }
const fn default_fetch_offset_secs() -> u64 { 120 }
const fn default_history_limit() -> usize { 100 }
const fn default_resample_ms() -> u64 { 3000 }
const fn default_funding_ma_size() -> usize { 60 }

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            interval: default_interval(),
            predict_num: 0,
            refresh_secs: default_refresh_secs(),
            fetch_secs: default_fetch_secs(),
            fetch_offset_secs: default_fetch_offset_secs(),
            history_limit: default_history_limit(),
            resample_ms: default_resample_ms(),
            funding_ma_size: default_funding_ma_size(),
            settlement_offset_secs: 0,
        }
    }
}

/// 策略配置（顶层）
#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    #[serde(default)]
    pub redis: Option<RedisSettings>,
    #[serde(default)]
    pub redis_key: Option<String>,
    #[serde(default)]
    pub order: OrderConfig,
    #[serde(default)]
    pub strategy: StrategyParams,
    #[serde(default = "default_reload_interval_secs")]
    pub reload_interval_secs: u64,
    #[serde(default = "default_signal_interval_ms")]
    pub signal_min_interval_ms: u64,
    #[serde(default)]
    pub loan_redis_key: Option<String>,
    #[serde(default = "default_loan_refresh_secs")]
    pub loan_refresh_secs: u64,
}

const fn default_reload_interval_secs() -> u64 { 60 }
const fn default_signal_interval_ms() -> u64 { 1_000 }
const fn default_loan_refresh_secs() -> u64 { 60 }

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            redis: Some(RedisSettings::default()),
            redis_key: None,
            order: OrderConfig::default(),
            strategy: StrategyParams::default(),
            reload_interval_secs: default_reload_interval_secs(),
            signal_min_interval_ms: default_signal_interval_ms(),
            loan_redis_key: None,
            loan_refresh_secs: default_loan_refresh_secs(),
        }
    }
}

impl StrategyConfig {
    pub fn load() -> Result<Self> {
        let mut cfg = Self::default();
        let redis_cfg = cfg.redis.get_or_insert_with(RedisSettings::default);

        if let Ok(host) = std::env::var("FUNDING_RATE_REDIS_HOST") {
            if !host.trim().is_empty() {
                redis_cfg.host = host;
            }
        }
        if let Ok(port) = std::env::var("FUNDING_RATE_REDIS_PORT") {
            if let Ok(value) = port.parse::<u16>() {
                redis_cfg.port = value;
            }
        }
        if let Ok(db) = std::env::var("FUNDING_RATE_REDIS_DB") {
            if let Ok(value) = db.parse::<i64>() {
                redis_cfg.db = value;
            }
        }
        if let Ok(redis_key) = std::env::var("FUNDING_RATE_REDIS_KEY") {
            if !redis_key.trim().is_empty() {
                cfg.redis_key = Some(redis_key);
            }
        }

        cfg.strategy.funding_ma_size = 60;
        cfg.strategy.settlement_offset_secs = cfg.strategy.fetch_offset_secs as i64;

        if let Some(redis_cfg) = cfg.redis.as_ref() {
            info!("Redis配置: {}:{} db={}", redis_cfg.host, redis_cfg.port, redis_cfg.db);
        }
        Ok(cfg)
    }

    pub fn max_open_keep_us(&self) -> i64 {
        (self.order.max_open_order_keep_s.max(1) * 1_000_000) as i64
    }

    pub fn max_hedge_keep_us(&self) -> i64 {
        (self.order.max_hedge_order_keep_s.max(1) * 1_000_000) as i64
    }

    pub fn min_signal_gap_us(&self) -> i64 {
        (self.signal_min_interval_ms * 1_000) as i64
    }
}

// ===== 阈值和状态 =====

/// 价差阈值配置
#[derive(Debug, Clone)]
pub struct SymbolThreshold {
    pub spot_symbol: String,
    pub futures_symbol: String,
    // BidAskSR 阈值（开/关）: (spot_bid - fut_ask) / spot_bid
    pub forward_open_threshold: f64,
    pub forward_cancel_threshold: f64,
    // AskBidSR 阈值（平仓）: (spot_ask - fut_bid) / spot_ask
    pub forward_close_threshold: f64,
    // AskBidSR 阈值（平仓辅助，用于撤单优化）
    pub forward_cancel_close_threshold: Option<f64>,
}

/// 资金费率阈值（按频率区分）
#[derive(Debug, Clone, Copy)]
pub struct RateThresholds {
    pub open_upper: f64,
    pub open_lower: f64,
    pub close_lower: f64,
    pub close_upper: f64,
}

impl RateThresholds {
    pub const fn for_8h() -> Self {
        Self {
            open_upper: 0.00008,
            open_lower: -0.00008,
            close_lower: -0.001,
            close_upper: 0.001,
        }
    }

    pub const fn for_4h() -> Self {
        Self {
            open_upper: 0.00004,
            open_lower: -0.00004,
            close_lower: -0.0008,
            close_upper: 0.0008,
        }
    }
}

impl Default for RateThresholds {
    fn default() -> Self {
        Self::for_8h()
    }
}

/// 参数快照（用于检测参数变更）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParamsSnapshot {
    pub interval: u64,
    pub predict_num: u64,
    pub refresh_secs: u64,
    pub fetch_secs: u64,
    pub fetch_offset_secs: u64,
    pub history_limit: u64,
    pub settlement_offset_secs: i64,
}

/// 资金费率阈值条目
#[derive(Debug, Clone, Default)] 
pub struct FundingThresholdEntry {
    pub symbol: String,
    pub predict_funding_rate: f64,
    pub lorn_rate: f64,
    pub funding_frequency: String, // "4h" | "8h"
    pub open_upper_threshold: f64,
    pub open_lower_threshold: f64,
    pub close_lower_threshold: f64,
    pub close_upper_threshold: f64,
} 

/// Binance 资金费率历史数据项
#[derive(Debug, Deserialize)]
pub struct BinanceFundingHistItem {
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    #[serde(rename = "fundingTime")]
    pub funding_time: Option<i64>,
}

/// 从 Binance API 获取资金费率数据（近期）
pub async fn fetch_binance_funding_items(
    client: &Client,
    symbol: &str,
    limit: usize,
) -> Result<Vec<BinanceFundingHistItem>> {
    let url = "https://fapi.binance.com/fapi/v1/fundingRate";
    let end_time = Utc::now().timestamp_millis();
    let start_time = end_time - 3 * 24 * 3600 * 1000; // 3d window
    let limit_s = limit.max(1).min(1000).to_string();
    let params = [
        ("symbol", symbol),
        ("startTime", &start_time.to_string()),
        ("endTime", &end_time.to_string()),
        ("limit", &limit_s),
    ];
    let resp = client.get(url).query(&params).send().await?;
    if !resp.status().is_success() {
        return Ok(vec![]);
    }
    let mut items: Vec<BinanceFundingHistItem> = resp.json().await.unwrap_or_default();
    items.sort_by_key(|it| it.funding_time.unwrap_or_default());
    Ok(items)
}

/// 从 Binance API 获取指定时间范围的资金费率历史
pub async fn fetch_binance_funding_history_range(
    client: &Client,
    symbol: &str,
    start_time: i64,
    end_time: i64,
    limit: usize,
) -> Result<Vec<f64>> {
    let url = "https://fapi.binance.com/fapi/v1/fundingRate";
    let end = end_time.max(0);
    let start = start_time.min(end).max(0);
    let limit_s = limit.max(1).min(1000).to_string();
    let params = [
        ("symbol", symbol),
        ("startTime", &start.to_string()),
        ("endTime", &end.to_string()),
        ("limit", &limit_s),
    ];
    let resp = client.get(url).query(&params).send().await?;
    if !resp.status().is_success() {
        return Ok(vec![]);
    }
    let mut items: Vec<BinanceFundingHistItem> = resp.json().await.unwrap_or_default();
    items.sort_by_key(|it| it.funding_time.unwrap_or_default());
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        if let Ok(v) = it.funding_rate.parse::<f64>() {
            out.push(v);
        }
    }
    if out.len() > limit {
        let drop_n = out.len() - limit;
        out.drain(0..drop_n);
    }
    Ok(out)
}

/// 推断 Binance 合约的资金费率频率（4h 或 8h）
pub async fn infer_binance_funding_frequency(client: &Client, symbol: &str) -> Option<String> {
    let items = fetch_binance_funding_items(client, symbol, 40).await.ok()?;
    let mut times: Vec<i64> = items.iter().filter_map(|it| it.funding_time).collect();
    if times.len() < 3 {
        return Some("8h".to_string());
    }
    times.sort_unstable();
    let mut diffs: Vec<i64> = Vec::with_capacity(times.len().saturating_sub(1));
    for w in times.windows(2) {
        if let [a, b] = w {
            diffs.push(b - a);
        }
    }
    if diffs.is_empty() {
        return Some("8h".to_string());
    }
    diffs.sort_unstable();
    let median = diffs[diffs.len() / 2];
    // 阈值 6 小时分界
    let six_hours_ms = 6 * 3600 * 1000;
    let freq = if median <= six_hours_ms { "4h" } else { "8h" };
    debug!("频率推断: {} median={}ms => {}", symbol, median, freq);
    Some(freq.to_string())
}

// ===== 辅助函数 =====

/// 浮点数近似相等比较
pub fn approx_equal(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-12
}

/// 浮点数数组近似相等比较
pub fn approx_equal_slice(a: &[f64], b: &[f64]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| approx_equal(*x, *y))
}

/// 解析数字列表（支持 JSON 数组、逗号分隔、单个数字）
pub fn parse_numeric_list(raw: &str) -> Result<Vec<f64>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.starts_with('[') {
        serde_json::from_str::<Vec<f64>>(trimmed)
            .map_err(|err| format!("JSON array parse error: {err}"))
    } else if trimmed.contains(',') {
        let mut out = Vec::new();
        for part in trimmed.split(',') {
            let piece = part.trim();
            if piece.is_empty() {
                continue;
            }
            match piece.parse::<f64>() {
                Ok(v) => out.push(v),
                Err(err) => {
                    return Err(format!("invalid float '{}': {}", piece, err));
                }
            }
        }
        Ok(out)
    } else {
        trimmed
            .parse::<f64>()
            .map(|v| vec![v])
            .map_err(|err| format!("invalid float: {}", err))
    }
}
