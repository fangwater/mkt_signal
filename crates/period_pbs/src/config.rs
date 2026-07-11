use std::collections::HashMap;
use std::fs;
use std::path::Path;

use ahash::{AHashMap, AHashSet};
use anyhow::{ensure, Context, Result};
use serde::Deserialize;

use crate::period::{DEFAULT_DELAY_MS, DEFAULT_PERIOD_MS};

pub const SERVICE_ROOT: &str = "dat_pbs";
pub const PERIOD_MS: i64 = DEFAULT_PERIOD_MS;
pub const DEFAULT_POLL_BATCH: usize = 65_536;
pub const DEFAULT_IDLE_SLEEP_US: u64 = 0;
pub const STATS_LOG_SECS: u64 = 30;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PeriodPbsConfig {
    pub core: Option<usize>,
    pub online_symbols: Vec<String>,
    pub zmq: ZmqConfig,
    pub venues: Vec<VenueConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ZmqConfig {
    pub bind: String,
    pub sndhwm: i32,
    pub linger_ms: i32,
    pub send_dontwait: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct VenueConfig {
    pub name: String,
    pub topic: Option<String>,
    pub poster_id: Option<String>,
    pub poll_batch: Option<usize>,
    pub idle_sleep_us: Option<u64>,
    pub delay_ms: Option<i64>,
    pub symbol_map: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeVenueConfig {
    pub name: String,
    pub topic: String,
    pub poster_id: String,
    pub poll_batch: usize,
    pub idle_sleep_us: u64,
    pub delay_ms: i64,
    pub symbols: VenueSymbolMap,
}

#[derive(Debug, Clone)]
pub struct VenueSymbolMap {
    source_to_canonical: AHashMap<String, String>,
}

impl Default for PeriodPbsConfig {
    fn default() -> Self {
        Self {
            core: None,
            online_symbols: Vec::new(),
            zmq: ZmqConfig::default(),
            venues: Vec::new(),
        }
    }
}

impl Default for ZmqConfig {
    fn default() -> Self {
        Self {
            bind: String::new(),
            sndhwm: 10_000,
            linger_ms: 0,
            send_dontwait: true,
        }
    }
}

impl Default for VenueConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            topic: None,
            poster_id: None,
            poll_batch: None,
            idle_sleep_us: None,
            delay_ms: None,
            symbol_map: HashMap::new(),
        }
    }
}

impl PeriodPbsConfig {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let text = fs::read_to_string(&path)
            .with_context(|| format!("read period_pbs config {}", path.as_ref().display()))?;
        let cfg: Self = toml::from_str(&text)
            .with_context(|| format!("parse period_pbs config {}", path.as_ref().display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            PERIOD_MS == 3_000,
            "period_pbs period must stay fixed at 3000ms"
        );
        ensure!(!self.zmq.bind.trim().is_empty(), "zmq.bind cannot be empty");
        ensure!(self.zmq.sndhwm >= 0, "zmq.sndhwm must be >= 0");
        ensure!(self.zmq.linger_ms >= -1, "zmq.linger_ms must be >= -1");
        ensure!(
            !self.online_symbols.is_empty(),
            "online_symbols must not be empty"
        );
        ensure!(!self.venues.is_empty(), "venues must not be empty");

        let online_symbols = normalize_online_symbols(&self.online_symbols)?;
        let online_set: AHashSet<&str> = online_symbols.iter().map(String::as_str).collect();

        let mut venue_names = AHashSet::new();
        let mut topics = AHashSet::new();
        for venue in &self.venues {
            let name = venue.name.trim();
            ensure!(!name.is_empty(), "venue.name cannot be empty");
            ensure!(
                venue_names.insert(name.to_string()),
                "duplicate venue configured: {name}"
            );
            if let Some(poll_batch) = venue.poll_batch {
                ensure!(poll_batch > 0, "venue {name} poll_batch must be > 0");
            }
            ensure!(
                venue.idle_sleep_us.is_some(),
                "venue {name} idle_sleep_us must be configured"
            );
            ensure!(
                venue.delay_ms.is_some(),
                "venue {name} delay_ms must be configured"
            );
            if let Some(delay_ms) = venue.delay_ms {
                ensure!(delay_ms >= 0, "venue {name} delay_ms must be >= 0");
            }
            if let Some(poster_id) = &venue.poster_id {
                ensure!(
                    !poster_id.trim().is_empty(),
                    "venue {name} poster_id cannot be empty when provided"
                );
            }
            let topic = venue
                .topic
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .unwrap_or(name);
            ensure!(
                topics.insert(topic.to_string()),
                "duplicate zmq topic configured: {topic}"
            );
            validate_symbol_map(name, &venue.symbol_map, &online_set)?;
            let _ = build_venue_symbol_map(name, &online_symbols, &venue.symbol_map)?;
        }

        Ok(())
    }

    pub fn runtime_venues(&self) -> Result<Vec<RuntimeVenueConfig>> {
        let online_symbols = normalize_online_symbols(&self.online_symbols)?;
        self.venues
            .iter()
            .map(|venue| {
                let name = venue.name.trim().to_string();
                let topic = venue
                    .topic
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .unwrap_or(&name)
                    .to_string();
                let poster_id = venue
                    .poster_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("period_pbs_{name}"));
                Ok(RuntimeVenueConfig {
                    name: name.clone(),
                    topic,
                    poster_id,
                    poll_batch: venue.poll_batch.unwrap_or(DEFAULT_POLL_BATCH),
                    idle_sleep_us: venue.idle_sleep_us.unwrap_or(DEFAULT_IDLE_SLEEP_US),
                    delay_ms: venue.delay_ms.unwrap_or(DEFAULT_DELAY_MS),
                    symbols: build_venue_symbol_map(&name, &online_symbols, &venue.symbol_map)?,
                })
            })
            .collect()
    }
}

impl VenueSymbolMap {
    pub fn canonical_for_source(&self, source_symbol: &str) -> Option<&str> {
        let source_symbol = source_symbol.trim();
        self.source_to_canonical
            .get(source_symbol)
            .or_else(|| {
                let without_numeric_prefix =
                    source_symbol.trim_start_matches(|ch: char| ch.is_ascii_digit());
                (without_numeric_prefix != source_symbol)
                    .then(|| self.source_to_canonical.get(without_numeric_prefix))
                    .flatten()
            })
            .map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.source_to_canonical.len()
    }

    pub fn canonical_symbols(&self) -> Vec<String> {
        let mut symbols: Vec<String> = self.source_to_canonical.values().cloned().collect();
        symbols.sort();
        symbols
    }
}

fn normalize_online_symbols(raw_symbols: &[String]) -> Result<Vec<String>> {
    let mut seen = AHashSet::new();
    let mut symbols = Vec::with_capacity(raw_symbols.len());
    for raw in raw_symbols {
        let symbol = raw.trim();
        ensure!(!symbol.is_empty(), "online_symbols contains empty symbol");
        let symbol = symbol.to_ascii_uppercase();
        ensure!(
            seen.insert(symbol.clone()),
            "duplicate online symbol configured: {symbol}"
        );
        symbols.push(symbol);
    }
    Ok(symbols)
}

fn validate_symbol_map(
    venue_name: &str,
    symbol_map: &HashMap<String, String>,
    online_symbols: &AHashSet<&str>,
) -> Result<()> {
    for (canonical, source) in symbol_map {
        let canonical = canonical.trim().to_ascii_uppercase();
        let source = source.trim().to_ascii_uppercase();
        ensure!(
            !canonical.is_empty(),
            "venue {venue_name} symbol_map contains empty canonical symbol"
        );
        ensure!(
            !source.is_empty(),
            "venue {venue_name} symbol_map for {canonical} has empty source symbol"
        );
        ensure!(
            online_symbols.contains(canonical.as_str()),
            "venue {venue_name} symbol_map key {canonical} is not in online_symbols"
        );
    }
    Ok(())
}

fn build_venue_symbol_map(
    venue_name: &str,
    online_symbols: &[String],
    symbol_map: &HashMap<String, String>,
) -> Result<VenueSymbolMap> {
    let normalized_symbol_map: AHashMap<String, String> = symbol_map
        .iter()
        .map(|(canonical, source)| {
            (
                canonical.trim().to_ascii_uppercase(),
                source.trim().to_ascii_uppercase(),
            )
        })
        .collect();
    let mut source_to_canonical = AHashMap::with_capacity(online_symbols.len());
    for canonical in online_symbols {
        let source = normalized_symbol_map
            .get(canonical)
            .map(String::as_str)
            .unwrap_or(canonical)
            .trim();
        ensure!(
            !source.is_empty(),
            "venue {venue_name} source symbol for {canonical} cannot be empty"
        );
        if let Some(existing) = source_to_canonical.insert(source.to_string(), canonical.clone()) {
            anyhow::bail!(
                "venue {venue_name} maps duplicate source symbol {source} to both {existing} and {canonical}"
            );
        }
    }
    Ok(VenueSymbolMap {
        source_to_canonical,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_numeric_contract_prefixes_to_online_symbols() {
        let cfg: PeriodPbsConfig = toml::from_str(
            r#"
online_symbols = ["BTCUSDT", "PUMPUSDT", "1INCHUSDT"]

[zmq]
bind = "ipc:///tmp/period_pbs_test.ipc"

[[venues]]
name = "binance-margin"
topic = "binance-spot"
idle_sleep_us = 0
delay_ms = 5

[[venues]]
name = "binance-futures"
topic = "binance-futures"
poll_batch = 123
idle_sleep_us = 0
delay_ms = 5
"#,
        )
        .expect("parse config");

        cfg.validate().expect("validate config");
        let venues = cfg.runtime_venues().expect("runtime venues");
        let futures = venues
            .iter()
            .find(|venue| venue.name == "binance-futures")
            .expect("futures venue");
        assert_eq!(futures.poll_batch, 123);
        assert_eq!(
            futures.symbols.canonical_for_source("1000PUMPUSDT"),
            Some("PUMPUSDT")
        );
        assert_eq!(
            futures.symbols.canonical_for_source("1INCHUSDT"),
            Some("1INCHUSDT")
        );
        assert_eq!(
            futures.symbols.canonical_for_source("BTCUSDT"),
            Some("BTCUSDT")
        );
        assert_eq!(futures.symbols.canonical_for_source("ETHUSDT"), None);
    }

    #[test]
    fn rejects_mapping_for_symbol_not_online() {
        let cfg: PeriodPbsConfig = toml::from_str(
            r#"
online_symbols = ["BTCUSDT"]

[zmq]
bind = "tcp://127.0.0.1:19999"

[[venues]]
name = "binance-futures"
idle_sleep_us = 0
delay_ms = 5
symbol_map = { PUMPUSDT = "1000PUMPUSDT" }
"#,
        )
        .expect("parse config");

        assert!(cfg.validate().is_err());
    }
}
