use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::{collections::HashSet, path::Path};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_http", deserialize_with = "native_http")]
    pub http: crate::config::HttpCfg,
    #[serde(default = "stale_default")]
    pub stale_after_ms: u64,
    pub counters: Vec<Counter>,
}
fn default_http() -> crate::config::HttpCfg {
    crate::config::HttpCfg {
        bind: "127.0.0.1".into(),
        port: 8811,
        ws_path: "/ws".into(),
    }
}
fn native_http<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<crate::config::HttpCfg, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Fields {
        bind: Option<String>,
        port: Option<u16>,
        ws_path: Option<String>,
    }
    let fields = Fields::deserialize(deserializer)?;
    let defaults = default_http();
    Ok(crate::config::HttpCfg {
        bind: fields.bind.unwrap_or(defaults.bind),
        port: fields.port.unwrap_or(defaults.port),
        ws_path: fields.ws_path.unwrap_or(defaults.ws_path),
    })
}
fn stale_default() -> u64 {
    15_000
}
pub use cn_futures_state::Adapter;
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Counter {
    pub id: String,
    pub label: String,
    /// Display alias, never a credential or native investor ID.
    pub account: String,
    pub adapter: Adapter,
    pub position_namespace: String,
    pub expected_account_hash: String,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path).context("read native viz config")?;
        let cfg: Self = toml::from_str(&raw).context("parse native viz config")?;
        cfg.validate()?;
        Ok(cfg)
    }
    pub fn validate(&self) -> Result<()> {
        let _: std::net::SocketAddr = format!("{}:{}", self.http.bind, self.http.port)
            .parse()
            .context("invalid native HTTP bind/port")?;
        ensure!(self.http.port > 0, "native HTTP port cannot be zero");
        ensure!(!self.counters.is_empty(), "configure at least one counter");
        ensure!(
            (100..=3_600_000).contains(&self.stale_after_ms),
            "invalid stale_after_ms"
        );
        ensure!(
            self.http.ws_path.starts_with('/')
                && self
                    .http
                    .ws_path
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'_' | b'-'))
                && !["/", "/snapshot", "/healthz"].contains(&self.http.ws_path.as_str()),
            "invalid ws_path"
        );
        let mut ids = HashSet::new();
        let mut namespaces = HashSet::new();
        for counter in &self.counters {
            ensure!(
                !counter.id.is_empty()
                    && counter
                        .id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
                    && ids.insert(&counter.id),
                "invalid or duplicate source id"
            );
            ensure!(
                !counter.label.trim().is_empty() && !counter.account.trim().is_empty(),
                "counter labels cannot be empty"
            );
            cn_futures_state::validate_hash(&counter.expected_account_hash)?;
            cn_futures_state::service_name(&counter.position_namespace)?;
            ensure!(
                namespaces.insert(&counter.position_namespace),
                "duplicate position IPC namespace"
            );
        }
        Ok(())
    }
}
