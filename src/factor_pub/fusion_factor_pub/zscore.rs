use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

use super::cfg::TlenServerConfig;
use crate::common::rolling_welford::RollingWelford;

const TLEN_SHARED_CONFIG_FIELD: &str = "__shared__";

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub(crate) struct ZscoreRuntimeConfig {
    pub(crate) window_size: usize,
    pub(crate) min_samples: u64,
    pub(crate) zscore_cap: f64,
}

impl ZscoreRuntimeConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.window_size == 0 {
            anyhow::bail!("zscore.window_size must be > 0");
        }
        if self.min_samples == 0 {
            anyhow::bail!("zscore.min_samples must be > 0");
        }
        if !self.zscore_cap.is_finite() || self.zscore_cap <= 0.0 {
            anyhow::bail!("zscore.zscore_cap must be finite and > 0");
        }
        Ok(())
    }
}

pub(crate) struct SymbolNormState {
    pub(crate) welford_vec: Vec<RollingWelford>,
    pub(crate) sample_count: u64,
}

impl SymbolNormState {
    pub(crate) fn new(window_size: usize, feature_dim: usize) -> Self {
        Self {
            welford_vec: (0..feature_dim)
                .map(|_| RollingWelford::new(window_size))
                .collect(),
            sample_count: 0,
        }
    }
}

pub(crate) fn normalize_feature_values(
    norm_state: &mut SymbolNormState,
    feature_values: &[f64],
    cfg: &ZscoreRuntimeConfig,
) -> Option<Vec<f64>> {
    if norm_state.welford_vec.len() != feature_values.len() {
        *norm_state = SymbolNormState::new(cfg.window_size, feature_values.len());
    }

    for (welford, value) in norm_state
        .welford_vec
        .iter_mut()
        .zip(feature_values.iter().copied())
    {
        welford.push(value);
    }
    norm_state.sample_count = norm_state.sample_count.saturating_add(1);

    if norm_state.sample_count < cfg.min_samples {
        return None;
    }

    Some(
        norm_state
            .welford_vec
            .iter()
            .map(|w| w.zscore_capped(cfg.zscore_cap).unwrap_or(0.0))
            .collect(),
    )
}

#[derive(Debug, Deserialize)]
struct ZscoreConfigResp {
    thresholds: HashMap<String, ZscoreRuntimeConfig>,
}

pub(crate) async fn load_zscore_config_from_tlen_server(
    tlen: &TlenServerConfig,
    venue_slug: &str,
) -> Result<ZscoreRuntimeConfig> {
    let base_url = tlen.base_url.trim_end_matches('/');
    let client = Client::builder()
        .timeout(Duration::from_millis(tlen.request_timeout_ms))
        .build()
        .context("build reqwest client for tlen_server failed")?;

    let url = format!("{}/api/thresholds", base_url);
    let resp = client
        .get(&url)
        .query(&[("venue", venue_slug), ("config_type", "zscore")])
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .error_for_status()
        .with_context(|| format!("GET {} returned error status", url))?;

    let payload: ZscoreConfigResp = resp
        .json()
        .await
        .with_context(|| format!("decode zscore config response failed: {}", url))?;

    let cfg = payload
        .thresholds
        .get(TLEN_SHARED_CONFIG_FIELD)
        .or_else(|| payload.thresholds.values().next())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("tlen_server returned empty zscore config"))?;
    cfg.validate()?;
    Ok(cfg)
}
