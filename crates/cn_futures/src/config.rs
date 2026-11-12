use crate::model::{is_future_symbol, RiskLimits, Venue};
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::ffi::CString;
use std::path::{Path, PathBuf};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub sdk_library: PathBuf,
    pub placement: crate::placement::PlacementConfig,
    #[serde(default = "default_preallocated_requests")]
    pub preallocated_requests: usize,
    pub flow_directory: PathBuf,
    pub journal: PathBuf,
    /// Defaults to flow_directory/persist_manager. Never shared across accounts.
    pub persistence_directory: Option<PathBuf>,
    #[serde(default)]
    pub preallocated_orders: usize,
    pub front: String,
    #[serde(default)]
    pub fallback_fronts: Vec<String>,
    #[serde(default)]
    pub confirm_settlement: bool,
    pub broker_id: String,
    pub production_mode: bool,
    pub user_id_env: String,
    pub password_env: String,
    pub app_id_env: String,
    pub auth_code_env: String,
    pub require_authentication: bool,
    pub investor_id_env: Option<String>,
    pub event_capacity: usize,
    pub query_interval_ms: u64,
    pub response_timeout_ms: u64,
    pub symbols: Vec<SymbolConfig>,
    pub risk: RiskLimits,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_combined_examples_validate_without_reading_credentials_or_applying_affinity() {
        let trade: Config =
            toml::from_str(include_str!("../../../config/ctp_trade.example.toml")).unwrap();
        let mut md: crate::ctp::market_data::Config =
            toml::from_str(include_str!("../../../config/ctp_market_data.example.toml")).unwrap();
        trade.validate_market_data(&md).unwrap();
        md.placement.business_cpu = 9;
        assert!(trade.validate_market_data(&md).is_err());
        md.placement = crate::placement::PlacementConfig {
            sdk_cpus: vec![2],
            ..trade.placement.clone()
        };
        md.symbols.retain(|s| s.symbol != "IF2612");
        assert!(trade.validate_market_data(&md).is_err());
    }
    #[test]
    fn blocking_ctp_configuration_knobs_are_rejected() {
        let trade = include_str!("../../../config/ctp_trade.example.toml");
        for knob in [
            "sync_before_send = true\n",
            "sync_before_send = false\n",
            "idle_sleep_us = 0\n",
            "idle_sleep_us = 100\n",
        ] {
            assert!(toml::from_str::<Config>(&format!("{knob}{trade}")).is_err());
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolConfig {
    pub venue: Venue,
    pub symbol: String,
    pub cash_budget_per_open_lot: f64,
}

/// Intentionally not Debug / Serialize. Secrets are only passed to the official SDK.
pub struct Credentials {
    pub broker: CString,
    pub user: CString,
    pub investor: CString,
    pub password: CString,
    pub app_id: CString,
    pub auth_code: CString,
}

fn default_preallocated_requests() -> usize {
    16384
}

impl Config {
    pub fn validate_market_data(&self, md: &crate::ctp::market_data::Config) -> Result<()> {
        self.validate()?;
        md.validate()?;
        self.placement.validate_combined(&md.placement)?;
        ensure!(
            self.broker_id == md.broker_id
                && self.user_id_env == md.user_id_env
                && self.password_env == md.password_env,
            "combined Md and Trader must use the same broker and credential variable names"
        );
        ensure!(
            self.flow_directory != md.flow_directory,
            "Md and Trader require separate flow directories"
        );
        ensure!(
            self.symbols.iter().all(|symbol| md
                .symbols
                .iter()
                .any(|s| s.venue == symbol.venue && s.symbol == symbol.symbol)),
            "every trading symbol must have an explicit MD subscription"
        );
        Ok(())
    }
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path).context("read CTP config")?;
        let config: Self = toml::from_str(&raw)
            .map_err(|_| anyhow::anyhow!("invalid CTP config: check schema and value types"))?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        self.placement.validate_fixed()?;
        ensure!(
            (1024..=2_000_000).contains(&self.preallocated_requests),
            "preallocated_requests must be 1024..2000000; it is a capacity hint, not a history limit"
        );
        ensure!(
            self.front.starts_with("tcp://")
                && self.front.len() > 9
                && !self.front.as_bytes().contains(&0),
            "CTP front must be an explicit tcp:// address"
        );
        ensure!(
            self.fallback_fronts
                .iter()
                .all(|front| front.starts_with("tcp://")
                    && front.len() > 9
                    && !front.as_bytes().contains(&0)),
            "invalid fallback CTP front"
        );
        ensure!(
            !self.broker_id.is_empty()
                && self.broker_id.len() <= 10
                && self.broker_id.is_ascii()
                && !self.broker_id.as_bytes().contains(&0),
            "invalid BrokerID"
        );
        ensure!(
            self.event_capacity >= 64 && self.event_capacity <= 1_048_576,
            "event_capacity must be 64..1048576"
        );
        ensure!(
            self.query_interval_ms >= 1000,
            "CTP query interval must be at least 1000 ms"
        );
        ensure!(self.response_timeout_ms >= 5000, "invalid response timeout");
        ensure!(
            self.flow_directory.is_absolute()
                && self.journal.is_absolute()
                && self.sdk_library.is_absolute(),
            "SDK, flow and journal paths must be absolute"
        );
        ensure!(
            !self.symbols.is_empty(),
            "explicit domestic futures contract allowlist required"
        );
        let mut symbols = HashSet::new();
        for s in &self.symbols {
            ensure!(
                is_future_symbol(s.venue, &s.symbol) && symbols.insert((s.venue, &s.symbol)),
                "invalid or duplicate domestic future {}",
                s.symbol
            );
            ensure!(
                s.cash_budget_per_open_lot.is_finite() && s.cash_budget_per_open_lot > 0.0,
                "positive local cash budget required for {}",
                s.symbol
            );
        }
        for name in [
            &self.user_id_env,
            &self.password_env,
            &self.app_id_env,
            &self.auth_code_env,
        ]
        .into_iter()
        .chain(self.investor_id_env.as_ref())
        {
            ensure!(
                !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "invalid credential environment variable name"
            );
        }
        if let Some(path) = &self.persistence_directory {
            ensure!(path.is_absolute(), "persistence_directory must be absolute");
        }
        self.risk.validate()
    }
    pub fn credentials(&self) -> Result<Credentials> {
        fn env(name: &str, max: usize, required: bool) -> Result<CString> {
            let value = std::env::var(name).unwrap_or_default();
            ensure!(
                (!required || !value.is_empty())
                    && value.len() <= max
                    && value.is_ascii()
                    && !value.as_bytes().contains(&0),
                "missing or invalid credential environment variable {name}"
            );
            CString::new(value).with_context(|| format!("invalid credential variable {name}"))
        }
        let user = env(&self.user_id_env, 15, true)?;
        let investor = match &self.investor_id_env {
            Some(name) => env(name, 12, true)?,
            None => {
                ensure!(
                    user.as_bytes().len() <= 12,
                    "InvestorID must be configured separately for UserID longer than 12 bytes"
                );
                user.clone()
            }
        };
        Ok(Credentials {
            broker: CString::new(self.broker_id.clone()).context("invalid BrokerID")?,
            user,
            investor,
            password: env(&self.password_env, 40, true)?,
            app_id: env(&self.app_id_env, 32, self.require_authentication)?,
            auth_code: env(&self.auth_code_env, 16, self.require_authentication)?,
        })
    }
}
