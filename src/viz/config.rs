use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct VizCfg {
    #[serde(default)]
    pub servers: Vec<VizServerCfg>,
}

impl VizCfg {
    pub async fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data = tokio::fs::read_to_string(path).await?;
        let cfg: Self = toml::from_str(&data)?;
        anyhow::ensure!(
            !cfg.servers.is_empty(),
            "viz config: missing required `[[servers]]` entries"
        );
        Ok(cfg)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct VizServerCfg {
    #[serde(default)]
    pub http: HttpCfg,
    /// IPC namespaces (IPC_NAMESPACE groups) to subscribe for this server.
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default)]
    pub pre_trade: PreTradeSrcCfg,
    #[serde(default)]
    pub fr_state: FrStateSrcCfg,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpCfg {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_ws_path")]
    pub ws_path: String,
}

impl Default for HttpCfg {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            port: default_port(),
            ws_path: default_ws_path(),
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

const fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PreTradeSrcCfg {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Optional namespaces override for pre-trade resample subscription.
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default)]
    pub instances: Vec<PreTradeInstanceCfg>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PreTradeInstanceCfg {
    pub label: String,
    #[serde(default)]
    pub namespace: Option<String>,
    pub exposure_channel: String,
    pub risk_channel: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FrStateSrcCfg {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub namespaces: Vec<String>,
}
