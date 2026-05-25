use anyhow::{anyhow, ensure, Context, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct LatencyStableMonitorConfig {
    pub zmq: ZmqConfig,
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
    #[serde(default = "default_reconnect_ms")]
    pub reconnect_ms: u64,
    #[serde(default = "default_idle_log_secs")]
    pub idle_log_secs: u64,
    #[serde(default = "default_stats_log_secs")]
    pub stats_log_secs: u64,
    #[serde(default)]
    pub public: PublicSources,
    #[serde(default)]
    pub private: PrivateSources,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ZmqConfig {
    /// ZMQ PUB bind endpoint, e.g. tcp://0.0.0.0:6370.
    pub bind: String,
    #[serde(default = "default_sndhwm")]
    pub sndhwm: i32,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PublicSources {
    #[serde(default)]
    pub spread_pbs: Vec<SpreadPbsSourceConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PrivateSources {
    #[serde(default)]
    pub trade_engine: Vec<TradeEngineSourceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpreadPbsSourceConfig {
    #[serde(default)]
    pub name: Option<String>,
    pub venue: String,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub topic: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradeEngineSourceConfig {
    #[serde(default)]
    pub name: Option<String>,
    pub namespace: String,
    pub exchange: String,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub topic: Option<String>,
}

impl LatencyStableMonitorConfig {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(&path).with_context(|| {
            format!(
                "failed to read latency monitor cfg {}",
                path.as_ref().display()
            )
        })?;
        let cfg: Self =
            serde_yaml::from_str(&content).context("failed to parse latency monitor cfg yaml")?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        ensure!(!self.zmq.bind.trim().is_empty(), "zmq.bind cannot be empty");
        ensure!(self.zmq.sndhwm >= 0, "zmq.sndhwm must be >= 0");
        ensure!(self.poll_ms > 0, "poll_ms must be > 0");
        ensure!(self.reconnect_ms > 0, "reconnect_ms must be > 0");

        let source_count = self.public.spread_pbs.len() + self.private.trade_engine.len();
        ensure!(
            source_count > 0,
            "at least one latency source must be configured"
        );

        let mut topics = HashSet::new();
        for source in &self.public.spread_pbs {
            let venue = source.venue.trim();
            ensure!(!venue.is_empty(), "public.spread_pbs venue cannot be empty");
            validate_override("public.spread_pbs.service", &source.service)?;
            validate_override("public.spread_pbs.topic", &source.topic)?;
            let topic = source
                .topic
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| default_spread_topic(venue));
            ensure!(
                topics.insert(topic.clone()),
                "duplicate zmq topic configured: {topic}"
            );
        }

        for source in &self.private.trade_engine {
            let namespace = source.namespace.trim();
            let exchange = source.exchange.trim();
            ensure!(
                !namespace.is_empty(),
                "private.trade_engine namespace cannot be empty"
            );
            ensure!(
                !exchange.is_empty(),
                "private.trade_engine exchange cannot be empty"
            );
            validate_override("private.trade_engine.service", &source.service)?;
            validate_override("private.trade_engine.topic", &source.topic)?;
            let topic = source
                .topic
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| default_trade_engine_topic(namespace));
            ensure!(
                topics.insert(topic.clone()),
                "duplicate zmq topic configured: {topic}"
            );
        }

        Ok(())
    }
}

pub fn default_spread_service(venue: &str) -> String {
    format!("spread_pbs/{}/latency", venue.trim())
}

pub fn default_spread_topic(venue: &str) -> String {
    format!("latency.public.spread_pbs.{}", venue.trim())
}

pub fn default_trade_engine_service(namespace: &str, exchange: &str) -> String {
    format!("{}/te_pubs/{}/latency", namespace.trim(), exchange.trim())
}

pub fn default_trade_engine_topic(namespace: &str) -> String {
    format!("latency.private.te.{}", namespace.trim())
}

fn validate_override(field: &str, value: &Option<String>) -> Result<()> {
    if let Some(raw) = value {
        if raw.trim().is_empty() {
            return Err(anyhow!("{field} cannot be empty when provided"));
        }
    }
    Ok(())
}

fn default_poll_ms() -> u64 {
    20
}

fn default_reconnect_ms() -> u64 {
    1_000
}

fn default_idle_log_secs() -> u64 {
    30
}

fn default_stats_log_secs() -> u64 {
    30
}

fn default_sndhwm() -> i32 {
    10_000
}
