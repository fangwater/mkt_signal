//! Depth 因子配置模块

use anyhow::Result;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::fs;

#[derive(Debug, Clone, PartialEq)]
pub struct DepthFactorPubConfig {
    pub data_source: DataSourceConfig,
    pub runtime: RuntimeConfig,
    pub factors: Vec<FactorDefinition>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DataSourceConfig {
    #[serde(default = "default_depth_channel")]
    pub depth_channel: String,
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self {
            depth_channel: default_depth_channel(),
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
    /// sum(bid_price * bid_amount)
    HfOrderbookBuyAmount,
    /// sum(ask_price * ask_amount)
    HfOrderbookSellAmount,
    /// skew(rolling((buy_amount / sell_amount), window))
    HfOrderbookSkew { window: usize },
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ClipRange {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigFile {
    Wrapped { factor_pub: DepthFactorRawConfig },
    Direct(DepthFactorRawConfig),
}

#[derive(Debug, Deserialize)]
struct DepthFactorRawConfig {
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
    window: Option<usize>,
}

impl DepthFactorPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;
        let raw = match config_file {
            ConfigFile::Wrapped { factor_pub } => factor_pub,
            ConfigFile::Direct(raw) => raw,
        };
        Self::from_raw(raw)
    }

    fn from_raw(raw: DepthFactorRawConfig) -> Result<Self> {
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
        if self.data_source.depth_channel.trim().is_empty() {
            anyhow::bail!("data_source.depth_channel must not be empty");
        }
        match self.data_source.depth_channel.as_str() {
            "depth5" | "depth20" | "depth50" => {}
            _ => {
                anyhow::bail!(
                    "data_source.depth_channel must be one of depth5/depth20/depth50, got '{}'",
                    self.data_source.depth_channel
                );
            }
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
            "hf_orderbook_buy_amount" => FactorKind::HfOrderbookBuyAmount,
            "hf_orderbook_sell_amount" => FactorKind::HfOrderbookSellAmount,
            "hf_orderbook_skew" => FactorKind::HfOrderbookSkew {
                window: self
                    .window
                    .ok_or_else(|| anyhow::anyhow!("{}: missing window", name))?,
            },
            _ => {
                anyhow::bail!(
                    "unsupported factor name '{}' (supported: hf_orderbook_buy_amount, hf_orderbook_sell_amount, hf_orderbook_skew)",
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
            Self::HfOrderbookBuyAmount | Self::HfOrderbookSellAmount => {}
            Self::HfOrderbookSkew { window } => {
                if *window < 3 {
                    anyhow::bail!("factor '{}' window must be >= 3", name);
                }
            }
        }
        Ok(())
    }

    fn required_len(&self) -> usize {
        match self {
            Self::HfOrderbookBuyAmount | Self::HfOrderbookSellAmount => 1,
            Self::HfOrderbookSkew { window } => *window,
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

fn default_depth_channel() -> String {
    "depth20".to_string()
}
