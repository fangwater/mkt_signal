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
    pub exec_pre_trade: ExecPreTradeSrcCfg,
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
pub struct ExecPreTradeSrcCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Exec 必须显式使用独立 namespace，不继承 server.namespaces。
    #[serde(default)]
    pub namespace: String,
}

#[cfg(test)]
mod tests {
    use super::VizCfg;

    #[test]
    fn exec_pre_trade_namespace_is_explicit_and_disabled_by_default() {
        let cfg: VizCfg = toml::from_str(
            r#"
                [[servers]]
                namespaces = ["normal_trade"]
            "#,
        )
        .unwrap();

        assert!(!cfg.servers[0].exec_pre_trade.enabled);
        assert!(cfg.servers[0].exec_pre_trade.namespace.is_empty());
    }

    #[test]
    fn parses_dedicated_exec_pre_trade_namespace() {
        let cfg: VizCfg = toml::from_str(
            r#"
                [[servers]]

                [servers.pre_trade]
                enabled = false

                [servers.exec_pre_trade]
                enabled = true
                namespace = "cta_exec_trade"
            "#,
        )
        .unwrap();

        assert!(cfg.servers[0].exec_pre_trade.enabled);
        assert_eq!(cfg.servers[0].exec_pre_trade.namespace, "cta_exec_trade");
        assert!(!cfg.servers[0].pre_trade.enabled);
        assert!(cfg.servers[0].namespaces.is_empty());
    }
}
