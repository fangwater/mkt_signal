use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub monitor: Monitor,
    pub sources: Vec<Source>,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Monitor {
    pub bind: std::net::SocketAddr,
    pub host_tag: String,
    pub poll_secs: u64,
    pub stale_secs: u64,
    pub market_stale_secs: u64,
    pub order_stale_secs: u64,
    pub order_lookback_secs: u64,
    pub max_order_records: usize,
    pub alert_delay_secs: u64,
    pub repeat_secs: u64,
    pub request_timeout_secs: u64,
    pub market_webhook_url_env: String,
    pub order_webhook_url_env: String,
    pub market_secret_env: Option<String>,
    pub order_secret_env: Option<String>,
}

impl Default for Monitor {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:18180".parse().unwrap(),
            host_tag: "FR".into(),
            poll_secs: 10,
            stale_secs: 90,
            market_stale_secs: 30,
            order_stale_secs: 300,
            order_lookback_secs: 3600,
            max_order_records: 50_000,
            alert_delay_secs: 30,
            repeat_secs: 300,
            request_timeout_secs: 5,
            market_webhook_url_env: "CTA_DINGTALK_MARKET_WEBHOOK_URL".into(),
            order_webhook_url_env: "CTA_DINGTALK_ORDER_WEBHOOK_URL".into(),
            market_secret_env: None,
            order_secret_env: None,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub id: String,
    pub namespace: String,
    pub viz_url: String,
    pub rocksdb_path: PathBuf,
    pub markets: Vec<Market>,
    /// Sustained per-asset residual; deliberately explicit for each account.
    pub max_asset_exposure_usdt: f64,
    pub max_total_exposure_usdt: f64,
    #[serde(default = "yes")]
    pub enabled: bool,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Market {
    pub venue: String,
    /// Explicit symbols: one busy symbol must not hide another stopped stream.
    pub symbols: Vec<String>,
}

fn yes() -> bool {
    true
}

pub fn loopback_url(raw: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw).context("invalid monitor endpoint URL")?;
    ensure!(
        url.scheme() == "http",
        "monitor endpoints must use local HTTP"
    );
    ensure!(
        matches!(url.host_str(), Some("127.0.0.1" | "[::1]")),
        "monitor endpoints must use a literal loopback address"
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "monitor endpoint must not contain credentials, query or fragment"
    );
    Ok(url)
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let cfg: Self = toml::from_str(
            &std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?,
        )
        .context("parse FR monitor config")?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        let m = &self.monitor;
        ensure!(
            m.bind.ip().is_loopback(),
            "dashboard must bind to loopback; use the existing authenticated gateway remotely"
        );
        ensure!(
            [
                m.poll_secs,
                m.stale_secs,
                m.market_stale_secs,
                m.order_stale_secs,
                m.repeat_secs
            ]
            .into_iter()
            .all(|s| (1..=86_400).contains(&s))
                && (1..=10).contains(&m.request_timeout_secs)
                && m.alert_delay_secs <= 86_400,
            "invalid monitor interval (1..86400s; HTTP timeout 1..10s)"
        );
        ensure!(m.host_tag.len() <= 80, "host tag is too long");
        ensure!(
            m.order_lookback_secs > m.order_stale_secs && m.order_lookback_secs <= 86_400,
            "order lookback must exceed stale threshold and be at most one day"
        );
        ensure!(
            (1..=1_000_000).contains(&m.max_order_records),
            "invalid max_order_records"
        );
        ensure!(
            self.sources.iter().any(|s| s.enabled),
            "no enabled FR sources"
        );
        let mut ids = HashSet::new();
        let mut namespaces = HashSet::new();
        let mut stores = HashSet::new();
        for s in &self.sources {
            ensure!(
                !s.id.is_empty()
                    && s.id.len() <= 96
                    && s.id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
                    && ids.insert(&s.id),
                "empty or duplicate source id"
            );
            ensure!(
                !s.namespace.trim().is_empty(),
                "{}: namespace is required",
                s.id
            );
            if s.enabled {
                ensure!(namespaces.insert(&s.namespace) && stores.insert(&s.rocksdb_path),
                    "duplicate enabled namespace or order store; one account must not be counted twice");
            }
            loopback_url(&s.viz_url).with_context(|| format!("{}: invalid Viz origin", s.id))?;
            ensure!(
                s.rocksdb_path.is_absolute(),
                "{}: RocksDB path must be absolute",
                s.id
            );
            ensure!(
                s.max_asset_exposure_usdt.is_finite()
                    && s.max_asset_exposure_usdt > 0.0
                    && s.max_total_exposure_usdt.is_finite()
                    && s.max_total_exposure_usdt > 0.0,
                "{}: exposure thresholds must be finite and positive",
                s.id
            );
            ensure!(!s.markets.is_empty(), "{}: markets are required", s.id);
            for market in &s.markets {
                ensure!(
                    !market.venue.is_empty()
                        && market
                            .venue
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || c == b'-'),
                    "{}: invalid market venue",
                    s.id
                );
                ensure!(
                    !market.symbols.is_empty()
                        && market.symbols.iter().all(|s| !s.trim().is_empty()),
                    "{}: each market needs explicit symbols",
                    s.id
                );
            }
        }
        Ok(())
    }
}
