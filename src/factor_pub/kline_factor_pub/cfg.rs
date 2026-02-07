//! 因子配置模块

use anyhow::Result;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::fs;

#[derive(Debug, Clone, PartialEq)]
pub struct FactorPubConfig {
    pub data_source: DataSourceConfig,
    pub runtime: RuntimeConfig,
    pub factors: Vec<FactorDefinition>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DataSourceConfig {
    #[serde(default = "default_kline_channel")]
    pub kline_channel: String,
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self {
            kline_channel: default_kline_channel(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct RuntimeConfig {
    #[serde(default = "default_reload_every")]
    pub reload_every: usize,
    pub max_keep_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FactorDefinition {
    pub name: String,
    pub enabled: bool,
    pub kind: FactorKind,
    pub scale_factor: f64,
    pub clip: Option<ClipRange>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FactorKind {
    /// close 的 N 周期收益率 rolling std
    RlReturnVolatility {
        pct_change_period: usize,
        rolling_window: usize,
    },
    /// close 的 1 周期收益率 rolling std
    HfVolStd { window: usize },
    /// (high - low) rolling mean
    HfHighlowRange { window: usize },
    /// close 的 return_period 收益 rolling mean
    HfSpreadReturn {
        return_period: usize,
        ma_window: usize,
    },
    /// count rolling mean
    HfCountMean { window: usize },
    /// abs(close pct_change) / volume rolling mean
    HfVolAbsPctByVol { window: usize },
    /// volume rolling mean
    HfVolumeMean { window: usize },
    /// rolling corr(close, volume)
    HfPriceVolumeCorr { window: usize },
    /// (rolling std of close return) * (rolling mean of volume)
    HfVolVolumeCombined {
        vol_window: usize,
        volu_window: usize,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ClipRange {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigFile {
    Wrapped { factor_pub: FactorPubRawConfig },
    Direct(FactorPubRawConfig),
}

#[derive(Debug, Deserialize)]
struct FactorPubRawConfig {
    #[serde(default)]
    data_source: DataSourceConfig,
    runtime: RuntimeConfig,
    factors: BTreeMap<String, RawFactorConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
struct RawFactorConfig {
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(default = "default_scale_factor")]
    scale_factor: f64,
    #[serde(default)]
    clip: Option<ClipRange>,

    #[serde(default)]
    pct_change_period: Option<usize>,
    #[serde(default)]
    rolling_window: Option<usize>,
    #[serde(default)]
    window: Option<usize>,
    #[serde(default)]
    return_period: Option<usize>,
    #[serde(default)]
    ma_window: Option<usize>,
    #[serde(default)]
    vol_window: Option<usize>,
    #[serde(default)]
    volu_window: Option<usize>,
}

impl FactorPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;
        let raw = match config_file {
            ConfigFile::Wrapped { factor_pub } => factor_pub,
            ConfigFile::Direct(raw) => raw,
        };
        Self::from_raw(raw)
    }

    fn from_raw(raw: FactorPubRawConfig) -> Result<Self> {
        let factors = raw
            .factors
            .into_iter()
            .map(|(name, cfg)| cfg.into_definition(name))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            data_source: raw.data_source,
            runtime: raw.runtime,
            factors,
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.data_source.kline_channel.trim().is_empty() {
            anyhow::bail!("data_source.kline_channel must not be empty");
        }
        if self.runtime.reload_every == 0 {
            anyhow::bail!("runtime.reload_every must be > 0");
        }
        if self.factors.is_empty() {
            anyhow::bail!("factors must not be empty");
        }

        let mut seen = HashSet::new();
        let mut min_required = 0usize;
        for factor in &self.factors {
            factor.validate()?;
            if !seen.insert(factor.name.clone()) {
                anyhow::bail!("duplicate factor name: {}", factor.name);
            }
            min_required = min_required.max(factor.required_len());
        }

        if self.runtime.max_keep_count <= min_required {
            anyhow::bail!(
                "runtime.max_keep_count must be > max required length ({} <= {})",
                self.runtime.max_keep_count,
                min_required
            );
        }

        Ok(())
    }
}

impl RawFactorConfig {
    fn into_definition(self, name: String) -> Result<FactorDefinition> {
        let kind = match name.as_str() {
            "rl_return_volatility" => FactorKind::RlReturnVolatility {
                pct_change_period: self
                    .pct_change_period
                    .ok_or_else(|| anyhow::anyhow!("{}: missing pct_change_period", name))?,
                rolling_window: self
                    .rolling_window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing rolling_window", name))?,
            },
            "hf_vol_std" => FactorKind::HfVolStd {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_highlow_range" => FactorKind::HfHighlowRange {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_spread_return" => FactorKind::HfSpreadReturn {
                return_period: self
                    .return_period
                    .ok_or_else(|| anyhow::anyhow!("{}: missing return_period", name))?,
                ma_window: self
                    .ma_window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing ma_window", name))?,
            },
            "hf_count_mean" => FactorKind::HfCountMean {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_vol_abs_pct_by_vol" => FactorKind::HfVolAbsPctByVol {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_volume_mean" => FactorKind::HfVolumeMean {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_price_volume_corr" => FactorKind::HfPriceVolumeCorr {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_vol_volume_combined" => FactorKind::HfVolVolumeCombined {
                vol_window: self
                    .vol_window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing vol_window", name))?,
                volu_window: self
                    .volu_window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing volu_window", name))?,
            },
            _ => {
                anyhow::bail!(
                    "unsupported factor name '{}' (supported: rl_return_volatility, hf_vol_std, hf_highlow_range, hf_spread_return, hf_count_mean, hf_vol_abs_pct_by_vol, hf_volume_mean, hf_price_volume_corr, hf_vol_volume_combined)",
                    name
                )
            }
        };

        Ok(FactorDefinition {
            name,
            enabled: self.enabled,
            kind,
            scale_factor: self.scale_factor,
            clip: self.clip,
        })
    }
}

impl FactorDefinition {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            anyhow::bail!("factor name must not be empty");
        }
        if !self.scale_factor.is_finite() || self.scale_factor <= 0.0 {
            anyhow::bail!("factor '{}' scale_factor must be finite and > 0", self.name);
        }
        if let Some(clip) = &self.clip {
            clip.validate(&self.name)?;
        }
        self.kind.validate(&self.name)?;
        Ok(())
    }

    pub fn required_len(&self) -> usize {
        self.kind.required_len()
    }
}

impl FactorKind {
    fn validate(&self, name: &str) -> Result<()> {
        match self {
            Self::RlReturnVolatility {
                pct_change_period,
                rolling_window,
            } => {
                if *pct_change_period == 0 {
                    anyhow::bail!("factor '{}' pct_change_period must be > 0", name);
                }
                if *rolling_window == 0 {
                    anyhow::bail!("factor '{}' rolling_window must be > 0", name);
                }
            }
            Self::HfVolStd { window }
            | Self::HfHighlowRange { window }
            | Self::HfCountMean { window }
            | Self::HfVolAbsPctByVol { window }
            | Self::HfVolumeMean { window }
            | Self::HfPriceVolumeCorr { window } => {
                if *window == 0 {
                    anyhow::bail!("factor '{}' window must be > 0", name);
                }
            }
            Self::HfSpreadReturn {
                return_period,
                ma_window,
            } => {
                if *return_period == 0 {
                    anyhow::bail!("factor '{}' return_period must be > 0", name);
                }
                if *ma_window == 0 {
                    anyhow::bail!("factor '{}' ma_window must be > 0", name);
                }
            }
            Self::HfVolVolumeCombined {
                vol_window,
                volu_window,
            } => {
                if *vol_window == 0 {
                    anyhow::bail!("factor '{}' vol_window must be > 0", name);
                }
                if *volu_window == 0 {
                    anyhow::bail!("factor '{}' volu_window must be > 0", name);
                }
            }
        }
        Ok(())
    }

    fn required_len(&self) -> usize {
        match self {
            Self::RlReturnVolatility {
                pct_change_period,
                rolling_window,
            } => pct_change_period + rolling_window,
            Self::HfVolStd { window } => window + 1,
            Self::HfHighlowRange { window } => *window,
            Self::HfSpreadReturn {
                return_period,
                ma_window,
            } => return_period + ma_window,
            Self::HfCountMean { window } => *window,
            Self::HfVolAbsPctByVol { window } => window + 1,
            Self::HfVolumeMean { window } => *window,
            Self::HfPriceVolumeCorr { window } => *window,
            Self::HfVolVolumeCombined {
                vol_window,
                volu_window,
            } => (vol_window + 1).max(*volu_window),
        }
    }
}

impl ClipRange {
    fn validate(&self, name: &str) -> Result<()> {
        if !self.min.is_finite() || !self.max.is_finite() {
            anyhow::bail!("factor '{}' clip min/max must be finite", name);
        }
        if self.min >= self.max {
            anyhow::bail!("factor '{}' clip min must be < max", name);
        }
        Ok(())
    }
}

fn default_enabled() -> bool {
    true
}

fn default_scale_factor() -> f64 {
    1.0
}

fn default_reload_every() -> usize {
    12
}

fn default_kline_channel() -> String {
    "kline5s".to_string()
}
