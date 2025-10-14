use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use log::{debug, info, warn};

use crate::common::redis_client::RedisClient;

const DEFAULT_MAX_LENGTH: usize = 150_000;
const DEFAULT_ROLLING_WINDOW: usize = 100_000;
const DEFAULT_MIN_PERIODS: usize = 90_000;
const DEFAULT_BIDASK_LOWER_Q: f32 = 0.05;
const DEFAULT_BIDASK_UPPER_Q: f32 = 0.70;
const DEFAULT_ASKBID_LOWER_Q: f32 = 0.30;
const DEFAULT_ASKBID_UPPER_Q: f32 = 0.95;
const DEFAULT_REFRESH_SEC: u64 = 60;
const DEFAULT_RELOAD_PARAM_SEC: u64 = 3;
const DEFAULT_RESAMPLE_INTERVAL_MS: u64 = 1_000;
pub const DEFAULT_CONFIG_HASH_KEY: &str = "rolling_metrics_params";
pub const DEFAULT_OUTPUT_HASH_KEY: &str = "rolling_metrics_thresholds";

#[derive(Debug, Clone)]
pub struct RollingConfig {
    pub max_length: usize,
    pub rolling_window: usize,
    pub min_periods: usize,
    pub bidask_lower_quantile: f32,
    pub bidask_upper_quantile: f32,
    pub askbid_lower_quantile: f32,
    pub askbid_upper_quantile: f32,
    pub resample_interval_ms: u64,
    pub refresh_sec: u64,
    pub reload_param_sec: u64,
    pub params_hash_key: String,
    pub output_hash_key: String,
}

impl Default for RollingConfig {
    fn default() -> Self {
        Self {
            max_length: DEFAULT_MAX_LENGTH,
            rolling_window: DEFAULT_ROLLING_WINDOW,
            min_periods: DEFAULT_MIN_PERIODS,
            bidask_lower_quantile: DEFAULT_BIDASK_LOWER_Q,
            bidask_upper_quantile: DEFAULT_BIDASK_UPPER_Q,
            askbid_lower_quantile: DEFAULT_ASKBID_LOWER_Q,
            askbid_upper_quantile: DEFAULT_ASKBID_UPPER_Q,
            resample_interval_ms: DEFAULT_RESAMPLE_INTERVAL_MS,
            refresh_sec: DEFAULT_REFRESH_SEC,
            reload_param_sec: DEFAULT_RELOAD_PARAM_SEC,
            params_hash_key: DEFAULT_CONFIG_HASH_KEY.to_string(),
            output_hash_key: DEFAULT_OUTPUT_HASH_KEY.to_string(),
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

    pub fn bidask_quantiles(&self) -> [f32; 2] {
        [self.bidask_lower_quantile, self.bidask_upper_quantile]
    }

    pub fn askbid_quantiles(&self) -> [f32; 2] {
        [self.askbid_lower_quantile, self.askbid_upper_quantile]
    }

    pub fn quantile_union(&self) -> Vec<f32> {
        let mut qs = vec![
            self.bidask_lower_quantile,
            self.bidask_upper_quantile,
            self.askbid_lower_quantile,
            self.askbid_upper_quantile,
        ];
        qs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(a.total_cmp(b)));
        qs.dedup_by(|a, b| (*a - *b).abs() <= f32::EPSILON);
        qs
    }

    pub fn update_from_map(&mut self, map: &HashMap<String, String>) {
        if let Some(val) = parse_usize(map, "MAX_LENGTH") {
            if val > 0 {
                self.max_length = val;
            } else {
                warn!("忽略无效 MAX_LENGTH={}", val);
            }
        }
        if let Some(val) = parse_usize(map, "ROLLING_WINDOW") {
            if val > 0 {
                self.rolling_window = val;
            } else {
                warn!("忽略无效 ROLLING_WINDOW={}", val);
            }
        }
        if let Some(val) = parse_usize(map, "MIN_PERIODS") {
            if val > 0 {
                self.min_periods = val;
            } else {
                warn!("忽略无效 MIN_PERIODS={}", val);
            }
        }
        if let Some(val) = parse_f32(map, "bidask_lower_quantile") {
            self.bidask_lower_quantile = clamp_unit(val, "bidask_lower_quantile");
        }
        if let Some(val) = parse_f32(map, "bidask_upper_quantile") {
            self.bidask_upper_quantile = clamp_unit(val, "bidask_upper_quantile");
        }
        if let Some(val) = parse_f32(map, "askbid_lower_quantile") {
            self.askbid_lower_quantile = clamp_unit(val, "askbid_lower_quantile");
        }
        if let Some(val) = parse_f32(map, "askbid_upper_quantile") {
            self.askbid_upper_quantile = clamp_unit(val, "askbid_upper_quantile");
        }
        if let Some(val) = parse_u64(map, "RESAMPLE_INTERVAL_MS") {
            if val > 0 {
                self.resample_interval_ms = val;
            } else {
                warn!("忽略无效 RESAMPLE_INTERVAL_MS={}", val);
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
    }

    pub fn finalize(&mut self) {
        if self.rolling_window > self.max_length {
            warn!(
                "ROLLING_WINDOW={} 大于 MAX_LENGTH={}，自动调整为 MAX_LENGTH",
                self.rolling_window, self.max_length
            );
            self.rolling_window = self.max_length;
        }
        if self.min_periods > self.rolling_window {
            warn!(
                "MIN_PERIODS={} 大于 ROLLING_WINDOW={}，自动调整为 ROLLING_WINDOW",
                self.min_periods, self.rolling_window
            );
            self.min_periods = self.rolling_window;
        }
        if self.resample_interval_ms == 0 {
            warn!(
                "RESAMPLE_INTERVAL_MS 设置为 0，回退到默认值 {}",
                DEFAULT_RESAMPLE_INTERVAL_MS
            );
            self.resample_interval_ms = DEFAULT_RESAMPLE_INTERVAL_MS;
        }
    }
}

fn parse_usize(map: &HashMap<String, String>, key: &str) -> Option<usize> {
    map.get(key)?.trim().parse::<usize>().ok()
}

fn parse_u64(map: &HashMap<String, String>, key: &str) -> Option<u64> {
    map.get(key)?.trim().parse::<u64>().ok()
}

fn parse_f32(map: &HashMap<String, String>, key: &str) -> Option<f32> {
    map.get(key)?.trim().parse::<f32>().ok()
}

fn clamp_unit(val: f32, key: &str) -> f32 {
    if !(0.0..=1.0).contains(&val) {
        warn!("{}={} 超出 [0,1]，自动裁剪", key, val);
    }
    val.clamp(0.0, 1.0)
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
                    "Redis HASH {} 为空，使用默认配置: max_length={} rolling_window={} min_periods={}",
                    key, cfg.max_length, cfg.rolling_window, cfg.min_periods
                );
                cfg.finalize();
                Ok((cfg, map))
            } else {
                cfg.update_from_map(&map);
                cfg.finalize();
                debug!(
                    "加载 RollingConfig: max_length={} rolling_window={} min_periods={} refresh={}s reload={}s",
                    cfg.max_length, cfg.rolling_window, cfg.min_periods, cfg.refresh_sec, cfg.reload_param_sec
                );
                Ok((cfg, map))
            }
        }
        Err(err) => Err(err).with_context(|| format!("读取 Redis HASH {} 失败", key)),
    }
}
