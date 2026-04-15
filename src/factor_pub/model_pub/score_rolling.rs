use anyhow::{Context, Result};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::common::sliding_quantile::SlidingQuantileWindow;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ScoreRollingConfig {
    #[serde(default)]
    pub redis: RedisSettings,
    #[serde(default = "default_params_hash_key_template")]
    pub params_hash_key_template: String,
    #[serde(default = "default_output_hash_key_template")]
    pub output_hash_key_template: String,
    #[serde(default = "default_reload_param_sec")]
    pub reload_param_sec: u64,
    #[serde(default = "default_max_length")]
    pub max_length: usize,
    #[serde(default = "default_rolling_window")]
    pub rolling_window: usize,
    #[serde(default = "default_min_periods")]
    pub min_periods: usize,
    #[serde(default = "default_quantiles")]
    pub quantiles: Vec<f32>,
}

impl Default for ScoreRollingConfig {
    fn default() -> Self {
        Self {
            redis: RedisSettings::default(),
            params_hash_key_template: default_params_hash_key_template(),
            output_hash_key_template: default_output_hash_key_template(),
            reload_param_sec: default_reload_param_sec(),
            max_length: default_max_length(),
            rolling_window: default_rolling_window(),
            min_periods: default_min_periods(),
            quantiles: default_quantiles(),
        }
    }
}

impl ScoreRollingConfig {
    pub fn validate(&self) -> Result<()> {
        if self.params_hash_key_template.trim().is_empty() {
            anyhow::bail!("score_rolling.params_hash_key_template must not be empty");
        }
        if self.output_hash_key_template.trim().is_empty() {
            anyhow::bail!("score_rolling.output_hash_key_template must not be empty");
        }
        if self.reload_param_sec == 0 {
            anyhow::bail!("score_rolling.reload_param_sec must be > 0");
        }
        if self.max_length == 0 {
            anyhow::bail!("score_rolling.max_length must be > 0");
        }
        if self.rolling_window == 0 {
            anyhow::bail!("score_rolling.rolling_window must be > 0");
        }
        if self.min_periods == 0 {
            anyhow::bail!("score_rolling.min_periods must be > 0");
        }
        let _ = normalize_quantiles(self.quantiles.clone())?;
        Ok(())
    }

    pub fn build_runtime(&self, model_name: &str) -> Result<ScoreRollingRuntimeConfig> {
        let model_name = normalize_model_name(model_name)?;
        let quantiles = normalize_quantiles(self.quantiles.clone())?;
        let max_length = self.max_length.max(1);
        let rolling_window = self.rolling_window.min(max_length).max(1);
        let min_periods = self.min_periods.min(rolling_window).max(1);

        Ok(ScoreRollingRuntimeConfig {
            source: format!("model_pub/{model_name}"),
            params_hash_key: self
                .params_hash_key_template
                .replace("{model_name}", &model_name),
            output_hash_key: self
                .output_hash_key_template
                .replace("{model_name}", &model_name),
            reload_param_sec: self.reload_param_sec.max(1),
            max_length,
            rolling_window,
            min_periods,
            quantiles,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScoreRollingRuntimeConfig {
    pub source: String,
    pub params_hash_key: String,
    pub output_hash_key: String,
    pub reload_param_sec: u64,
    pub max_length: usize,
    pub rolling_window: usize,
    pub min_periods: usize,
    pub quantiles: Vec<f32>,
}

impl ScoreRollingRuntimeConfig {
    pub fn apply_overrides(&mut self, map: &HashMap<String, String>) -> Result<Vec<String>> {
        if map.is_empty() {
            return Ok(Vec::new());
        }

        let mut changed = Vec::new();

        if let Some(v) = map.get("output_hash_key") {
            let trimmed = v.trim();
            if !trimmed.is_empty() && trimmed != self.output_hash_key {
                changed.push(format!(
                    "output_hash_key {}->{}",
                    self.output_hash_key, trimmed
                ));
                self.output_hash_key = trimmed.to_string();
            }
        }

        if let Some(v) = parse_u64(map, "reload_param_sec") {
            let next = v.max(1);
            if next != self.reload_param_sec {
                changed.push(format!(
                    "reload_param_sec {}->{}",
                    self.reload_param_sec, next
                ));
                self.reload_param_sec = next;
            }
        }

        if let Some(v) = parse_usize(map, "max_length") {
            let next = v.max(1);
            if next != self.max_length {
                changed.push(format!("max_length {}->{}", self.max_length, next));
                self.max_length = next;
            }
        }

        if let Some(v) = parse_usize(map, "rolling_window") {
            let next = v.max(1);
            if next != self.rolling_window {
                changed.push(format!("rolling_window {}->{}", self.rolling_window, next));
                self.rolling_window = next;
            }
        }

        if let Some(v) = parse_usize(map, "min_periods") {
            let next = v.max(1);
            if next != self.min_periods {
                changed.push(format!("min_periods {}->{}", self.min_periods, next));
                self.min_periods = next;
            }
        }

        if let Some(v) = map.get("quantiles") {
            let parsed = parse_quantiles_text(v)?;
            if parsed != self.quantiles {
                changed.push(format!("quantiles {:?}->{:?}", self.quantiles, parsed));
                self.quantiles = parsed;
            }
        }

        self.max_length = self.max_length.max(1);
        self.rolling_window = self.rolling_window.min(self.max_length).max(1);
        self.min_periods = self.min_periods.min(self.rolling_window).max(1);

        Ok(changed)
    }
}

#[derive(Debug, Serialize)]
struct ScoreThresholdPayload {
    model_name: String,
    symbol: String,
    ts: i64,
    target_ts: i64,
    factor: Option<f64>,
    score_quantile: Option<f64>,
    quantiles: Vec<f32>,
    thresholds: Vec<f64>,
    ready: bool,
    sample_size: usize,
    window_size: usize,
    min_periods: usize,
    source: String,
    score_ts_out_ms: Option<i64>,
}

struct SymbolScoreState {
    window: SlidingQuantileWindow,
    latest_score: Option<f64>,
    latest_score_quantile: Option<f64>,
    latest_ts_out_ms: Option<i64>,
}

impl SymbolScoreState {
    fn new(capacity: usize, rolling_window: usize) -> Self {
        Self {
            window: SlidingQuantileWindow::new(capacity.max(1), rolling_window.max(1)),
            latest_score: None,
            latest_score_quantile: None,
            latest_ts_out_ms: None,
        }
    }

    fn push(&mut self, score: f64, ts_out_ms: i64) -> Option<f64> {
        let score_quantile = if self.window.push_f64(score) {
            self.window.percentile_rank_last()
        } else {
            None
        };
        self.latest_score = Some(score);
        self.latest_score_quantile = score_quantile;
        self.latest_ts_out_ms = (ts_out_ms > 0).then_some(ts_out_ms);
        score_quantile
    }
}

pub struct InlineScoreRolling {
    model_name: String,
    redis: RedisClient,
    runtime_cfg: ScoreRollingRuntimeConfig,
    states: HashMap<String, SymbolScoreState>,
    known_fields: HashSet<String>,
    last_reload: Instant,
}

impl InlineScoreRolling {
    pub async fn new(model_name: &str, cfg: &ScoreRollingConfig) -> Result<Self> {
        let model_name = normalize_model_name(model_name)?;
        let mut redis = RedisClient::connect(cfg.redis.clone())
            .await
            .with_context(|| {
                format!(
                    "connect redis for inline score_rolling failed: {}",
                    model_name
                )
            })?;
        let mut runtime_cfg = cfg.build_runtime(&model_name)?;

        match redis.hgetall_map(&runtime_cfg.params_hash_key).await {
            Ok(map) => {
                let changes = runtime_cfg.apply_overrides(&map)?;
                if !changes.is_empty() {
                    info!(
                        "inline score_rolling initial overrides applied: model={} changes={:?}",
                        model_name, changes
                    );
                }
            }
            Err(err) => {
                warn!(
                    "inline score_rolling initial override load failed: model={} key={} err={:#}",
                    model_name, runtime_cfg.params_hash_key, err
                );
            }
        }

        info!(
            "inline score_rolling started: model={} source={} output_key={} params_key={} reload={}s max_length={} window={} min={} quantiles={:?}",
            model_name,
            runtime_cfg.source,
            runtime_cfg.output_hash_key,
            runtime_cfg.params_hash_key,
            runtime_cfg.reload_param_sec,
            runtime_cfg.max_length,
            runtime_cfg.rolling_window,
            runtime_cfg.min_periods,
            runtime_cfg.quantiles
        );

        Ok(Self {
            model_name,
            redis,
            runtime_cfg,
            states: HashMap::new(),
            known_fields: HashSet::new(),
            last_reload: Instant::now(),
        })
    }

    pub async fn post_process_score(
        &mut self,
        symbol: &str,
        score: f64,
        ts_out_ms: i64,
    ) -> Result<Option<f64>> {
        if !score.is_finite() {
            return Ok(None);
        }

        let symbol = normalize_symbol(symbol);
        if symbol.is_empty() {
            return Ok(None);
        }

        let capacity = self.runtime_cfg.max_length;
        let rolling_window = self.runtime_cfg.rolling_window;
        let state = self
            .states
            .entry(symbol.clone())
            .or_insert_with(|| SymbolScoreState::new(capacity, rolling_window));

        if state.window.history_capacity() != capacity
            || state.window.active_window() != rolling_window
        {
            state.window.reconfigure(capacity, rolling_window);
        }

        let score_quantile = state.push(score, ts_out_ms);
        self.maybe_reload_and_flush().await;

        Ok(score_quantile)
    }

    pub fn preview_score_quantile(&mut self, symbol: &str, score: f64) -> Option<f64> {
        if !score.is_finite() {
            return None;
        }

        let symbol = normalize_symbol(symbol);
        if symbol.is_empty() {
            return None;
        }

        let capacity = self.runtime_cfg.max_length;
        let rolling_window = self.runtime_cfg.rolling_window;
        let state = self
            .states
            .entry(symbol)
            .or_insert_with(|| SymbolScoreState::new(capacity, rolling_window));

        if state.window.history_capacity() != capacity
            || state.window.active_window() != rolling_window
        {
            state.window.reconfigure(capacity, rolling_window);
        }

        state.window.percentile_rank_if_pushed_f64(score)
    }

    async fn maybe_reload_and_flush(&mut self) {
        if self.last_reload.elapsed()
            < Duration::from_secs(self.runtime_cfg.reload_param_sec.max(1))
        {
            return;
        }

        self.last_reload = Instant::now();
        let mut should_flush = true;
        match self
            .redis
            .hgetall_map(&self.runtime_cfg.params_hash_key)
            .await
        {
            Ok(map) => match self.runtime_cfg.apply_overrides(&map) {
                Ok(changes) => {
                    if !changes.is_empty() {
                        for state in self.states.values_mut() {
                            state.window.reconfigure(
                                self.runtime_cfg.max_length,
                                self.runtime_cfg.rolling_window,
                            );
                        }

                        info!(
                            "inline score_rolling overrides applied: model={} changes={:?}",
                            self.model_name, changes
                        );
                    }
                }
                Err(err) => {
                    should_flush = false;
                    warn!(
                        "inline score_rolling apply overrides failed: model={} key={} err={:#}",
                        self.model_name, self.runtime_cfg.params_hash_key, err
                    );
                }
            },
            Err(err) => {
                should_flush = false;
                warn!(
                    "inline score_rolling reload failed: model={} key={} err={:#}",
                    self.model_name, self.runtime_cfg.params_hash_key, err
                );
            }
        }

        if should_flush {
            if let Err(err) = self.flush_all_thresholds().await {
                warn!(
                    "inline score_rolling flush failed: model={} key={} err={:#}",
                    self.model_name, self.runtime_cfg.output_hash_key, err
                );
            }
        }
    }

    async fn flush_all_thresholds(&mut self) -> Result<()> {
        let mut current_fields = HashSet::with_capacity(self.states.len());
        let mut payloads = Vec::<(String, String)>::with_capacity(self.states.len());
        let now = now_ms();

        for (symbol, state) in &self.states {
            let sample_size = state.window.sample_size();
            let ready = sample_size >= self.runtime_cfg.min_periods;

            let thresholds = if ready {
                state
                    .window
                    .quantiles_linear(&self.runtime_cfg.quantiles)
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };

            let payload = ScoreThresholdPayload {
                model_name: self.model_name.clone(),
                symbol: symbol.clone(),
                ts: now,
                target_ts: state.latest_ts_out_ms.unwrap_or(now),
                factor: state.latest_score,
                score_quantile: state.latest_score_quantile,
                quantiles: self.runtime_cfg.quantiles.clone(),
                thresholds,
                ready,
                sample_size,
                window_size: self.runtime_cfg.rolling_window,
                min_periods: self.runtime_cfg.min_periods,
                source: self.runtime_cfg.source.clone(),
                score_ts_out_ms: state.latest_ts_out_ms,
            };
            let text = serde_json::to_string(&payload).with_context(|| {
                format!("serialize threshold payload failed: symbol={}", symbol)
            })?;
            current_fields.insert(symbol.clone());
            payloads.push((symbol.clone(), text));
        }

        let removals = self
            .known_fields
            .difference(&current_fields)
            .cloned()
            .collect::<Vec<_>>();

        self.redis
            .hset_multiple_str(&self.runtime_cfg.output_hash_key, &payloads)
            .await
            .with_context(|| {
                format!(
                    "HSET thresholds failed: key={} fields={}",
                    self.runtime_cfg.output_hash_key,
                    payloads.len()
                )
            })?;

        self.redis
            .hdel_fields(&self.runtime_cfg.output_hash_key, &removals)
            .await
            .with_context(|| {
                format!(
                    "HDEL thresholds failed: key={} fields={}",
                    self.runtime_cfg.output_hash_key,
                    removals.len()
                )
            })?;

        self.known_fields = current_fields;

        info!(
            "inline score_rolling flush done: model={} key={} symbols={} removed={}",
            self.model_name,
            self.runtime_cfg.output_hash_key,
            payloads.len(),
            removals.len()
        );

        Ok(())
    }
}

fn default_params_hash_key_template() -> String {
    "model_score_rolling_params_{model_name}".to_string()
}

fn default_output_hash_key_template() -> String {
    "model_score_rolling_thresholds_{model_name}".to_string()
}

fn default_reload_param_sec() -> u64 {
    60
}

fn default_max_length() -> usize {
    150_000
}

fn default_rolling_window() -> usize {
    17_800
}

fn default_min_periods() -> usize {
    100
}

fn default_quantiles() -> Vec<f32> {
    vec![0.9, 0.8, 0.2, 0.1]
}

fn normalize_model_name(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("model_name must not be empty");
    }
    Ok(trimmed.to_string())
}

fn normalize_symbol(raw: &str) -> String {
    raw.trim().to_uppercase()
}

fn parse_u64(map: &HashMap<String, String>, key: &str) -> Option<u64> {
    map.get(key)?.trim().parse::<u64>().ok()
}

fn parse_usize(map: &HashMap<String, String>, key: &str) -> Option<usize> {
    map.get(key)?.trim().parse::<usize>().ok()
}

fn normalize_quantiles(values: Vec<f32>) -> Result<Vec<f32>> {
    let mut cleaned = values
        .into_iter()
        .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0)
        .collect::<Vec<_>>();
    cleaned.sort_by(|a, b| a.partial_cmp(b).unwrap_or(a.total_cmp(b)));
    cleaned.dedup_by(|a, b| (*a - *b).abs() <= f32::EPSILON);
    if cleaned.is_empty() {
        anyhow::bail!("quantiles empty after normalization");
    }
    Ok(cleaned)
}

fn parse_quantiles_text(raw: &str) -> Result<Vec<f32>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("quantiles is empty");
    }

    if trimmed.starts_with('[') {
        let parsed = serde_json::from_str::<Vec<f32>>(trimmed)
            .with_context(|| format!("parse quantiles json failed: {}", trimmed))?;
        return normalize_quantiles(parsed);
    }

    let parsed = trimmed
        .split(',')
        .map(|x| x.trim())
        .filter(|x| !x.is_empty())
        .map(|x| {
            x.parse::<f32>()
                .with_context(|| format!("invalid quantile value: {}", x))
        })
        .collect::<Result<Vec<_>>>()?;
    normalize_quantiles(parsed)
}

fn now_ms() -> i64 {
    let Ok(duration) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };
    let ms = duration.as_millis();
    if ms > i64::MAX as u128 {
        i64::MAX
    } else {
        ms as i64
    }
}

#[cfg(test)]
mod tests {
    use super::{ScoreRollingConfig, ScoreRollingRuntimeConfig};
    use std::collections::HashMap;

    #[test]
    fn build_runtime_normalizes_quantiles_and_windows() {
        let cfg = ScoreRollingConfig {
            quantiles: vec![0.9, 0.2, 0.9, 1.2, -0.1],
            max_length: 10,
            rolling_window: 20,
            min_periods: 99,
            ..ScoreRollingConfig::default()
        };

        let runtime = cfg.build_runtime("demo").expect("build runtime");
        assert_eq!(runtime.quantiles, vec![0.2, 0.9]);
        assert_eq!(runtime.max_length, 10);
        assert_eq!(runtime.rolling_window, 10);
        assert_eq!(runtime.min_periods, 10);
        assert_eq!(runtime.source, "model_pub/demo");
    }

    #[test]
    fn apply_overrides_updates_runtime_fields() {
        let mut runtime = ScoreRollingRuntimeConfig {
            source: "model_pub/demo".to_string(),
            params_hash_key: "params".to_string(),
            output_hash_key: "out".to_string(),
            reload_param_sec: 60,
            max_length: 100,
            rolling_window: 20,
            min_periods: 10,
            quantiles: vec![0.1, 0.9],
        };

        let mut map = HashMap::new();
        map.insert("output_hash_key".to_string(), "out2".to_string());
        map.insert("max_length".to_string(), "50".to_string());
        map.insert("rolling_window".to_string(), "80".to_string());
        map.insert("min_periods".to_string(), "90".to_string());
        map.insert("quantiles".to_string(), "0.8,0.2,0.8".to_string());

        let changes = runtime.apply_overrides(&map).expect("apply overrides");
        assert!(!changes.is_empty());
        assert_eq!(runtime.source, "model_pub/demo");
        assert_eq!(runtime.output_hash_key, "out2");
        assert_eq!(runtime.max_length, 50);
        assert_eq!(runtime.rolling_window, 50);
        assert_eq!(runtime.min_periods, 50);
        assert_eq!(runtime.quantiles, vec![0.2, 0.8]);
    }
}
