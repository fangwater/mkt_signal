use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_http", deserialize_with = "native_http")]
    pub http: crate::config::HttpCfg,
    #[serde(default = "poll_default")]
    pub poll_interval_ms: u64,
    #[serde(default = "stale_default")]
    pub stale_after_ms: u64,
    #[serde(default = "limit_default")]
    pub row_limit: usize,
    #[serde(default)]
    pub counters: Vec<Counter>,
    #[serde(default)]
    pub market_data: Vec<MarketData>,
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
fn poll_default() -> u64 {
    1000
}
fn stale_default() -> u64 {
    15_000
}
fn limit_default() -> usize {
    200
}
fn symbol_limit() -> usize {
    2000
}
fn venues_default() -> Vec<String> {
    cn_futures_pbs::msg::VENUES
        .iter()
        .map(|s| s.to_string())
        .collect()
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Adapter {
    Ctp,
    Yida,
}
impl Adapter {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ctp => "ctp",
            Self::Yida => "yida",
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Counter {
    pub id: String,
    pub label: String,
    /// Display alias, never a credential or native investor ID.
    pub account: String,
    pub adapter: Adapter,
    pub rocksdb_path: PathBuf,
    pub secondary_path: PathBuf,
    pub expected_account_hash: Option<String>,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    CompanyIntranet,
    Colo,
    PublicNetwork,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketData {
    pub id: String,
    pub label: String,
    pub environment: Environment,
    pub service_root: String,
    #[serde(default = "venues_default")]
    pub venues: Vec<String>,
    /// Empty means all ordinary contracts, subject to max_symbols.
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default = "symbol_limit")]
    pub max_symbols: usize,
    #[serde(default = "stale_default")]
    pub stale_after_ms: u64,
}

/// Resolve symlinks even when the leaf directory does not exist yet.
pub fn resolved(path: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "native viz paths must be absolute");
    if path.exists() {
        return path.canonicalize().context("resolve native viz path");
    }
    let parent = path.parent().context("path has no parent")?;
    let name = path.file_name().context("invalid directory name")?;
    ensure!(name != "." && name != "..", "invalid path component");
    Ok(resolved(parent)?.join(name))
}
fn disjoint(a: &Path, b: &Path) -> bool {
    !a.starts_with(b) && !b.starts_with(a)
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
        ensure!(
            !self.counters.is_empty() || !self.market_data.is_empty(),
            "configure a counter or market source"
        );
        ensure!(
            (100..=60_000).contains(&self.poll_interval_ms),
            "poll_interval_ms must be 100..60000"
        );
        ensure!(
            self.stale_after_ms >= self.poll_interval_ms && self.stale_after_ms <= 3_600_000,
            "invalid stale_after_ms"
        );
        ensure!(
            (1..=1000).contains(&self.row_limit),
            "row_limit must be 1..1000"
        );
        ensure!(
            self.http.ws_path.starts_with('/')
                && !self.http.ws_path.contains(['?', '#', '*', ':'])
                && !["/", "/snapshot", "/healthz"].contains(&self.http.ws_path.as_str()),
            "invalid ws_path"
        );
        let mut ids = HashSet::new();
        for id in self
            .counters
            .iter()
            .map(|c| &c.id)
            .chain(self.market_data.iter().map(|m| &m.id))
        {
            ensure!(
                !id.is_empty()
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
                    && ids.insert(id),
                "invalid or duplicate source id"
            );
        }
        let primary = self
            .counters
            .iter()
            .map(|c| resolved(&c.rocksdb_path))
            .collect::<Result<Vec<_>>>()?;
        let secondary = self
            .counters
            .iter()
            .map(|c| resolved(&c.secondary_path))
            .collect::<Result<Vec<_>>>()?;
        for (i, counter) in self.counters.iter().enumerate() {
            ensure!(
                !counter.label.trim().is_empty() && !counter.account.trim().is_empty(),
                "counter labels cannot be empty"
            );
            if let Some(hash) = &counter.expected_account_hash {
                ensure!(
                    hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
                    "invalid expected_account_hash"
                );
            }
            for p in &primary {
                ensure!(
                    disjoint(p, &secondary[i]),
                    "secondary must be outside every primary"
                );
            }
            for j in 0..i {
                ensure!(
                    disjoint(&primary[i], &primary[j]),
                    "duplicate/overlapping execution stores"
                );
                ensure!(
                    disjoint(&secondary[i], &secondary[j]),
                    "secondary directories must be independent"
                );
            }
        }
        let mut roots = HashSet::new();
        for md in &self.market_data {
            cn_futures_pbs::ipc::validate_root(&md.service_root)?;
            ensure!(
                roots.insert(&md.service_root),
                "duplicate market service_root"
            );
            ensure!(
                !md.label.trim().is_empty()
                    && (1..=20_000).contains(&md.max_symbols)
                    && (100..=3_600_000).contains(&md.stale_after_ms),
                "invalid market settings"
            );
            let mut venues = HashSet::new();
            ensure!(!md.venues.is_empty(), "empty market venues");
            for venue in &md.venues {
                cn_futures_pbs::ipc::service_name(&md.service_root, venue)?;
                ensure!(venues.insert(venue), "duplicate market venue");
            }
            let mut symbols = HashSet::new();
            for symbol in &md.symbols {
                let mut msg = cn_futures_pbs::msg::MarketDataMsg::default();
                msg.set_symbol(symbol.as_bytes())?;
                ensure!(symbols.insert(symbol), "duplicate market symbol");
            }
            ensure!(
                md.symbols.len() <= md.max_symbols,
                "symbol filter exceeds max_symbols"
            );
        }
        Ok(())
    }
}
