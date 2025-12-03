use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde_json::Value;

use crate::common::redis_client::RedisClient;

const DEFAULT_MAX_LENGTH: usize = 150_000;
const DEFAULT_REFRESH_SEC: u64 = 60;
const DEFAULT_RELOAD_PARAM_SEC: u64 = 3;
const DEFAULT_RESAMPLE_INTERVAL_MS: u64 = 1_000;
const DEFAULT_ROLLING_WINDOW: usize = 100_000;
const DEFAULT_MIN_PERIODS: usize = 90_000;

pub const DEFAULT_CONFIG_HASH_KEY: &str = "rolling_metrics_params";
pub const DEFAULT_OUTPUT_HASH_KEY: &str = "rolling_metrics_thresholds";

pub const FACTOR_BIDASK: &str = "bidask";
pub const FACTOR_ASKBID: &str = "askbid";
pub const FACTOR_SPREAD: &str = "spread";

#[derive(Debug, Clone)]
pub struct RollingConfig {
    pub max_length: usize,
    pub refresh_sec: u64,
    pub reload_param_sec: u64,
    pub params_hash_key: String,
    pub output_hash_key: String,
    pub factors: BTreeMap<String, FactorConfig>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FactorConfig {
    pub resample_interval_ms: u64,
    pub rolling_window: usize,
    pub min_periods: usize,
    pub quantiles: Vec<f32>,
}

impl Default for RollingConfig {
    fn default() -> Self {
        Self {
            max_length: DEFAULT_MAX_LENGTH,
            refresh_sec: DEFAULT_REFRESH_SEC,
            reload_param_sec: DEFAULT_RELOAD_PARAM_SEC,
            params_hash_key: DEFAULT_CONFIG_HASH_KEY.to_string(),
            output_hash_key: DEFAULT_OUTPUT_HASH_KEY.to_string(),
            factors: default_factors(),
        }
    }
}

fn default_factors() -> BTreeMap<String, FactorConfig> {
    let mut factors = BTreeMap::new();
    factors.insert(
        FACTOR_BIDASK.to_string(),
        FactorConfig::new(
            DEFAULT_RESAMPLE_INTERVAL_MS,
            DEFAULT_ROLLING_WINDOW,
            DEFAULT_MIN_PERIODS,
            Vec::new(),
        ),
    );
    factors.insert(
        FACTOR_ASKBID.to_string(),
        FactorConfig::new(
            DEFAULT_RESAMPLE_INTERVAL_MS,
            DEFAULT_ROLLING_WINDOW,
            DEFAULT_MIN_PERIODS,
            Vec::new(),
        ),
    );
    factors.insert(
        FACTOR_SPREAD.to_string(),
        FactorConfig::new(
            DEFAULT_RESAMPLE_INTERVAL_MS,
            DEFAULT_ROLLING_WINDOW,
            DEFAULT_MIN_PERIODS,
            Vec::new(),
        ),
    );
    factors
}

impl FactorConfig {
    fn new(
        resample_interval_ms: u64,
        rolling_window: usize,
        min_periods: usize,
        quantiles: Vec<f32>,
    ) -> Self {
        Self {
            resample_interval_ms,
            rolling_window,
            min_periods,
            quantiles: normalize_quantiles(quantiles, "quantiles"),
        }
    }
}

impl RollingConfig {
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.refresh_sec.max(1))
    }

    pub fn reload_interval(&self) -> Duration {
        Duration::from_secs(self.reload_param_sec.max(1))
    }

    pub fn quantile_union(&self) -> Vec<f32> {
        let mut qs: Vec<f32> = Vec::new();
        for factor in self.factors.values() {
            qs.extend(factor.quantiles.iter().copied());
        }
        qs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(a.total_cmp(b)));
        qs.dedup_by(|a, b| (*a - *b).abs() <= f32::EPSILON);
        qs
    }

    pub fn factor(&self, name: &str) -> Option<&FactorConfig> {
        self.factors.get(name)
    }

    pub fn factors_iter(&self) -> impl Iterator<Item = (&str, &FactorConfig)> {
        self.factors.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn update_from_map(&mut self, map: &HashMap<String, String>) {
        if let Some(val) = parse_usize(map, "MAX_LENGTH") {
            if val > 0 {
                self.max_length = val;
            } else {
                warn!("忽略无效 MAX_LENGTH={}", val);
            }
        }
        if let Some(val) = parse_u64(map, "refresh_sec") {
            if val > 0 {
                self.refresh_sec = val;
            }
        }
        if let Some(val) = parse_u64(map, "reload_param_sec") {
            if val > 0 {
                self.reload_param_sec = val;
            }
        }
        if let Some(val) = map.get("output_hash_key") {
            if !val.trim().is_empty() {
                self.output_hash_key = val.trim().to_string();
            }
        }
        if let Some(raw_factors) = map.get("factors") {
            if let Some(parsed) = parse_factors(raw_factors) {
                if parsed.is_empty() {
                    warn!("factors 解析结果为空，保留原有因子配置");
                } else {
                    self.factors = parsed;
                }
            }
        } else if let Some(raw_profiles) = map.get("resample_profiles") {
            // 兼容旧字段，仅取第一项
            if let Some(parsed) = parse_profiles_compat(raw_profiles) {
                if parsed.is_empty() {
                    warn!("resample_profiles 解析结果为空，保留原有因子配置");
                } else {
                    self.factors = parsed;
                }
            }
        }
    }

    pub fn finalize(&mut self) {
        if self.factors.is_empty() {
            warn!("未配置任何因子，回退到默认设置");
            self.factors = default_factors();
        }
        for (factor_name, factor) in self.factors.iter_mut() {
            if factor.rolling_window > self.max_length {
                warn!(
                    "factor {}: rolling_window={} 大于 MAX_LENGTH={}，自动调整为 MAX_LENGTH",
                    factor_name, factor.rolling_window, self.max_length
                );
                factor.rolling_window = self.max_length;
            }
            if factor.min_periods > factor.rolling_window {
                warn!(
                    "factor {}: min_periods={} 大于 rolling_window={}，自动调整为 rolling_window",
                    factor_name, factor.min_periods, factor.rolling_window
                );
                factor.min_periods = factor.rolling_window;
            }
            if factor.resample_interval_ms == 0 {
                warn!(
                    "factor {}: resample_interval_ms=0，回退到默认值 {}",
                    factor_name, DEFAULT_RESAMPLE_INTERVAL_MS
                );
                factor.resample_interval_ms = DEFAULT_RESAMPLE_INTERVAL_MS;
            }
            factor.quantiles = normalize_quantiles(factor.quantiles.clone(), factor_name);
        }
    }
}

fn parse_usize(map: &HashMap<String, String>, key: &str) -> Option<usize> {
    map.get(key)?.trim().parse::<usize>().ok()
}

fn parse_u64(map: &HashMap<String, String>, key: &str) -> Option<u64> {
    map.get(key)?.trim().parse::<u64>().ok()
}

fn parse_factors(raw: &str) -> Option<BTreeMap<String, FactorConfig>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Some(BTreeMap::new());
    }
    let value: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(err) => {
            warn!("解析 factors 失败: {}", err);
            return None;
        }
    };
    let obj = match value {
        Value::Object(obj) => obj,
        other => {
            warn!("factors 需为 JSON 对象，实际={}", other);
            return None;
        }
    };
    let mut factors = BTreeMap::new();
    for (factor_name, cfg_value) in obj {
        match parse_factor_value(&cfg_value, factor_name.as_str()) {
            Ok(cfg) => {
                factors.insert(factor_name, cfg);
            }
            Err(err) => {
                warn!("{}", err);
                return None;
            }
        }
    }
    Some(factors)
}

fn parse_profiles_compat(raw: &str) -> Option<BTreeMap<String, FactorConfig>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Some(BTreeMap::new());
    }
    let value: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(err) => {
            warn!("解析 resample_profiles 失败: {}", err);
            return None;
        }
    };
    let arr = match value {
        Value::Array(arr) => arr,
        other => {
            warn!("resample_profiles 需为 JSON 数组，实际={}", other);
            return None;
        }
    };
    let first = arr.into_iter().next().unwrap_or(Value::Null);
    let factors_value = first
        .as_object()
        .and_then(|obj| obj.get("factors"))
        .and_then(|v| v.as_object());
    let Some(factors_obj) = factors_value else {
        warn!("resample_profiles 兼容模式：未找到 factors 字段");
        return None;
    };
    let mut factors = BTreeMap::new();
    for (factor_name, cfg_value) in factors_obj {
        match parse_factor_value(cfg_value, factor_name.as_str()) {
            Ok(cfg) => {
                factors.insert(factor_name.clone(), cfg);
            }
            Err(err) => {
                warn!("{}", err);
                return None;
            }
        }
    }
    Some(factors)
}

fn parse_factor_value(value: &Value, factor_name: &str) -> Result<FactorConfig, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| format!("factors.{} 必须是对象", factor_name))?;
    let resample_interval_ms = obj
        .get("resample_interval_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_RESAMPLE_INTERVAL_MS);
    let rolling_window = obj
        .get("rolling_window")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(DEFAULT_ROLLING_WINDOW);
    let min_periods = obj
        .get("min_periods")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(DEFAULT_MIN_PERIODS);
    let quantiles = obj
        .get("quantiles")
        .and_then(parse_quantiles_from_value)
        .unwrap_or_default();
    Ok(FactorConfig::new(
        resample_interval_ms,
        rolling_window,
        min_periods,
        quantiles,
    ))
}

fn parse_quantiles_from_value(value: &Value) -> Option<Vec<f32>> {
    match value {
        Value::Array(arr) => {
            let mut list = Vec::with_capacity(arr.len());
            for item in arr {
                match item.as_f64() {
                    Some(v) if v.is_finite() => list.push(v as f32),
                    _ => return None,
                }
            }
            Some(normalize_quantiles(list, "factors.quantiles"))
        }
        Value::String(text) => parse_quantile_list(text, "factors.quantiles")
            .map(|vals| normalize_quantiles(vals, "factors.quantiles")),
        _ => None,
    }
}

fn parse_quantile_list(raw: &str, key: &str) -> Option<Vec<f32>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        warn!("{} 为空字符串，忽略", key);
        return Some(Vec::new());
    }
    if trimmed.starts_with('[') {
        match serde_json::from_str::<Vec<f32>>(trimmed) {
            Ok(list) => Some(list),
            Err(err) => {
                warn!("解析 {}={} 失败: {}", key, trimmed, err);
                None
            }
        }
    } else {
        let mut values: Vec<f32> = Vec::new();
        for token in trimmed.split(|c| c == ',' || c == ';' || c == ' ') {
            let part = token.trim();
            if part.is_empty() {
                continue;
            }
            match part.parse::<f32>() {
                Ok(v) => values.push(v),
                Err(err) => {
                    warn!("解析 {} 的分位值 '{}' 失败: {}", key, part, err);
                    return None;
                }
            }
        }
        Some(values)
    }
}

fn normalize_quantiles(values: Vec<f32>, key: &str) -> Vec<f32> {
    let mut cleaned: Vec<f32> = Vec::with_capacity(values.len());
    for (idx, raw) in values.into_iter().enumerate() {
        if !raw.is_finite() {
            warn!("{}[{}]={} 非有限数，忽略该分位", key, idx, raw);
            continue;
        }
        let mut val = raw;
        if val > 1.0 {
            val /= 100.0;
        }
        if !(0.0..=1.0).contains(&val) {
            warn!("{}[{}]={} 超出 [0,1]，自动裁剪", key, idx, val);
        }
        cleaned.push(val.clamp(0.0, 1.0));
    }
    cleaned.sort_by(|a, b| a.partial_cmp(b).unwrap_or_else(|| a.total_cmp(b)));
    cleaned.dedup_by(|a, b| (*a - *b).abs() <= f32::EPSILON);
    cleaned
}

pub async fn load_config_from_redis(
    client: &mut RedisClient,
    key: &str,
) -> Result<(RollingConfig, HashMap<String, String>)> {
    let mut cfg = RollingConfig::default();
    cfg.params_hash_key = key.to_string();
    match client.hgetall_map(key).await {
        Ok(map) => {
            if map.is_empty() {
                info!(
                    "Redis HASH {} 为空，使用默认配置: max_length={} factors=[{}]",
                    key,
                    cfg.max_length,
                    factor_summary(&cfg.factors)
                );
                cfg.finalize();
                Ok((cfg, map))
            } else {
                cfg.update_from_map(&map);
                cfg.finalize();
                debug!(
                    "加载 RollingConfig: max_length={} refresh={}s reload={}s factors=[{}]",
                    cfg.max_length,
                    cfg.refresh_sec,
                    cfg.reload_param_sec,
                    factor_summary(&cfg.factors)
                );
                Ok((cfg, map))
            }
        }
        Err(err) => Err(err).with_context(|| format!("读取 Redis HASH {} 失败", key)),
    }
}

fn factor_summary(factors: &BTreeMap<String, FactorConfig>) -> String {
    factors
        .iter()
        .map(|(name, cfg)| {
            format!(
                "{}(win={} min={} resample={}ms)",
                name, cfg.rolling_window, cfg.min_periods, cfg.resample_interval_ms
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}
