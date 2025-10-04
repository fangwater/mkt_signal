use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct VizCfg {
    #[serde(default)]
    pub http: HttpCfg,
    #[serde(default)]
    pub sources: SourcesCfg,
    #[serde(default)]
    pub sampling: SamplingCfg,
    #[serde(default)]
    pub thresholds: ThresholdCfg,
}

impl VizCfg {
    pub async fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data = tokio::fs::read_to_string(path).await?;
        let cfg: Self = toml::from_str(&data)?;
        Ok(cfg)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpCfg {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_ws_path")]
    pub ws_path: String,
    #[serde(default)]
    pub cors_origins: Option<Vec<String>>,
    #[serde(default)]
    pub auth_token: Option<String>,
}

impl Default for HttpCfg {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            port: default_port(),
            ws_path: default_ws_path(),
            cors_origins: None,
            auth_token: None,
        }
    }
}

fn default_bind() -> String {
    "0.0.0.0".to_string()
}
const fn default_port() -> u16 {
    8801
}
fn default_ws_path() -> String {
    "/ws".to_string()
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SourcesCfg {
    #[serde(default)]
    pub account: Option<AccountSrcCfg>,
    #[serde(default)]
    pub derivatives: Option<DerivativesSrcCfg>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountSrcCfg {
    pub service: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default = "default_account_payload")]
    pub max_payload_bytes: usize,
}

const fn default_account_payload() -> usize {
    16_384
}

#[derive(Debug, Clone, Deserialize)]
pub struct DerivativesSrcCfg {
    pub service: String,
    #[serde(default = "default_derivatives_payload")]
    pub max_payload_bytes: usize,
}

const fn default_derivatives_payload() -> usize {
    128
}

#[derive(Debug, Clone, Deserialize)]
pub struct SamplingCfg {
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
    #[serde(default)]
    pub send_if_changed: bool,
    #[serde(default)]
    pub symbols: Option<Vec<String>>, // 可选过滤输出
}

impl Default for SamplingCfg {
    fn default() -> Self {
        Self {
            interval_ms: default_interval_ms(),
            send_if_changed: false,
            symbols: None,
        }
    }
}

const fn default_interval_ms() -> u64 {
    200
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThresholdCfg {
    #[serde(default = "default_open_threshold")]
    pub open: f64,
    #[serde(default = "default_close_threshold")]
    pub close: f64,
}

impl Default for ThresholdCfg {
    fn default() -> Self {
        Self {
            open: default_open_threshold(),
            close: default_close_threshold(),
        }
    }
}

const fn default_open_threshold() -> f64 {
    0.002
}
const fn default_close_threshold() -> f64 {
    0.0005
}
