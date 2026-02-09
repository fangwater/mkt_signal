//! Trade 因子配置模块

use anyhow::Result;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::fs;

#[derive(Debug, Clone, PartialEq)]
pub struct TradeFactorPubConfig {
    pub data_source: DataSourceConfig,
    pub runtime: RuntimeConfig,
    pub factors: Vec<FactorDefinition>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DataSourceConfig {
    #[serde(default = "default_trade_channel")]
    pub trade_channel: String,
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self {
            trade_channel: default_trade_channel(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct RuntimeConfig {
    #[serde(default = "default_reload_every")]
    pub reload_every: usize,
    pub max_keep_count: usize,
    #[serde(default = "default_bar_ms")]
    pub bar_ms: i64,
    #[serde(default = "default_large_order_notional")]
    pub large_order_notional: f64,
    #[serde(default = "default_medium_order_notional")]
    pub medium_order_notional: f64,
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
    HfActiveRateStd { short: usize, long: usize },
    HfActiveRateRank { short: usize, long: usize },
    HfNetbuyStd { window: usize },
    HfNetbuyRank { window: usize },
    HfAbRate { window: usize },
    HfLargeOrderStd { window: usize },
    HfLargeOrderRateStd { window: usize },
    HfMediumOrderRateStd { window: usize },
    HfSmallOrderRateStd { window: usize },
    HfSmallOrderRateMean { window: usize },
    HfVwapDiffStd { window: usize },
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ClipRange {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigFile {
    Wrapped { factor_pub: TradeFactorRawConfig },
    Direct(TradeFactorRawConfig),
}

#[derive(Debug, Deserialize)]
struct TradeFactorRawConfig {
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
    short: Option<usize>,
    #[serde(default)]
    long: Option<usize>,
    #[serde(default)]
    window: Option<usize>,
}

impl TradeFactorPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;
        let raw = match config_file {
            ConfigFile::Wrapped { factor_pub } => factor_pub,
            ConfigFile::Direct(raw) => raw,
        };
        Self::from_raw(raw)
    }

    fn from_raw(raw: TradeFactorRawConfig) -> Result<Self> {
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
        if self.data_source.trade_channel.trim().is_empty() {
            anyhow::bail!("data_source.trade_channel must not be empty");
        }

        if self.runtime.reload_every == 0 {
            anyhow::bail!("runtime.reload_every must be > 0");
        }
        if self.runtime.bar_ms <= 0 {
            anyhow::bail!("runtime.bar_ms must be > 0");
        }
        if self.runtime.medium_order_notional <= 0.0
            || !self.runtime.medium_order_notional.is_finite()
        {
            anyhow::bail!("runtime.medium_order_notional must be finite and > 0");
        }
        if self.runtime.large_order_notional <= 0.0
            || !self.runtime.large_order_notional.is_finite()
        {
            anyhow::bail!("runtime.large_order_notional must be finite and > 0");
        }
        if self.runtime.medium_order_notional >= self.runtime.large_order_notional {
            anyhow::bail!("runtime.medium_order_notional must be < runtime.large_order_notional");
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
            "hf_active_rate_std" => FactorKind::HfActiveRateStd {
                short: self
                    .short
                    .ok_or_else(|| anyhow::anyhow!("{}: missing short", name))?,
                long: self
                    .long
                    .ok_or_else(|| anyhow::anyhow!("{}: missing long", name))?,
            },
            "hf_active_rate_rank" => FactorKind::HfActiveRateRank {
                short: self
                    .short
                    .ok_or_else(|| anyhow::anyhow!("{}: missing short", name))?,
                long: self
                    .long
                    .ok_or_else(|| anyhow::anyhow!("{}: missing long", name))?,
            },
            "hf_netbuy_std" => FactorKind::HfNetbuyStd {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_netbuy_rank" => FactorKind::HfNetbuyRank {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_ab_rate" => FactorKind::HfAbRate {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_large_order_std" => FactorKind::HfLargeOrderStd {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_large_order_rate_std" => FactorKind::HfLargeOrderRateStd {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_medium_order_rate_std" => FactorKind::HfMediumOrderRateStd {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_small_order_rate_std" => FactorKind::HfSmallOrderRateStd {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_small_order_rate_mean" => FactorKind::HfSmallOrderRateMean {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            "hf_vwap_diff_std" => FactorKind::HfVwapDiffStd {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            _ => {
                anyhow::bail!(
                    "unsupported factor name '{}' (supported: hf_active_rate_std, hf_active_rate_rank, hf_netbuy_std, hf_netbuy_rank, hf_ab_rate, hf_large_order_std, hf_large_order_rate_std, hf_medium_order_rate_std, hf_small_order_rate_std, hf_small_order_rate_mean, hf_vwap_diff_std)",
                    name
                );
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
            Self::HfActiveRateStd { short, long } | Self::HfActiveRateRank { short, long } => {
                if *short == 0 {
                    anyhow::bail!("factor '{}' short must be > 0", name);
                }
                if *long == 0 {
                    anyhow::bail!("factor '{}' long must be > 0", name);
                }
            }
            Self::HfNetbuyStd { window }
            | Self::HfNetbuyRank { window }
            | Self::HfAbRate { window }
            | Self::HfLargeOrderStd { window }
            | Self::HfLargeOrderRateStd { window }
            | Self::HfMediumOrderRateStd { window }
            | Self::HfSmallOrderRateStd { window }
            | Self::HfSmallOrderRateMean { window }
            | Self::HfVwapDiffStd { window } => {
                if *window == 0 {
                    anyhow::bail!("factor '{}' window must be > 0", name);
                }
            }
        }
        Ok(())
    }

    fn required_len(&self) -> usize {
        match self {
            Self::HfActiveRateStd { short, long } | Self::HfActiveRateRank { short, long } => {
                short + long - 1
            }
            Self::HfNetbuyStd { window }
            | Self::HfNetbuyRank { window }
            | Self::HfAbRate { window }
            | Self::HfLargeOrderStd { window }
            | Self::HfLargeOrderRateStd { window }
            | Self::HfMediumOrderRateStd { window }
            | Self::HfSmallOrderRateStd { window }
            | Self::HfSmallOrderRateMean { window }
            | Self::HfVwapDiffStd { window } => *window,
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
    120
}

fn default_trade_channel() -> String {
    "trade".to_string()
}

fn default_bar_ms() -> i64 {
    5_000
}

fn default_large_order_notional() -> f64 {
    100_000.0
}

fn default_medium_order_notional() -> f64 {
    10_000.0
}
