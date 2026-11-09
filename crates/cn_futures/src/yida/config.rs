use crate::model::{is_future_symbol, RiskLimits, Venue};
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::{
    collections::HashSet,
    ffi::CString,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub sdk_library: PathBuf,
    /// Broker-supplied native Yida server config; no account secrets in this file.
    pub sdk_config: PathBuf,
    pub flow_directory: PathBuf,
    pub journal: PathBuf,
    pub user_id_env: String,
    pub password_env: String,
    pub app_id_env: String,
    pub auth_code_env: String,
    pub event_capacity: usize,
    pub response_timeout_ms: u64,
    pub idle_sleep_us: u64,
    pub symbols: Vec<SymbolConfig>,
    pub risk: RiskLimits,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolConfig {
    pub venue: Venue,
    pub symbol: String,
    pub cash_budget_per_open_lot: f64,
}
/// Deliberately not Debug/Serialize.
pub struct Credentials {
    pub user: CString,
    pub password: CString,
    pub app_id: CString,
    pub auth_code: CString,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let value = std::fs::read_to_string(path).context("read Yida trading config")?;
        let config: Self = toml::from_str(&value)
            .map_err(|_| anyhow::anyhow!("invalid Yida config schema/value types"))?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        for p in [
            &self.sdk_library,
            &self.sdk_config,
            &self.flow_directory,
            &self.journal,
        ] {
            ensure!(
                p.is_absolute(),
                "Yida SDK/config/flow/journal paths must be absolute"
            );
        }
        ensure!(
            (64..=1_048_576).contains(&self.event_capacity),
            "invalid event capacity"
        );
        ensure!(
            self.response_timeout_ms >= 5000 && self.idle_sleep_us <= 1000,
            "invalid timeout/idle sleep"
        );
        let mut seen = HashSet::new();
        ensure!(
            !self.symbols.is_empty(),
            "explicit SHFE/INE contract allowlist required"
        );
        for s in &self.symbols {
            ensure!(
                matches!(s.venue, Venue::Shfe | Venue::Ine)
                    && is_future_symbol(s.venue, &s.symbol)
                    && seen.insert((s.venue, &s.symbol)),
                "invalid/duplicate SHFE/INE future"
            );
            ensure!(
                s.cash_budget_per_open_lot.is_finite() && s.cash_budget_per_open_lot > 0.0,
                "positive conservative cash budget required"
            );
        }
        for name in [
            &self.user_id_env,
            &self.password_env,
            &self.app_id_env,
            &self.auth_code_env,
        ] {
            ensure!(
                !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "invalid credential environment variable name"
            );
        }
        self.risk.validate()
    }
    /// Validate the broker-owned native config before loading the vendor library. This
    /// first implementation only permits TCP trading with all Yida market data disabled.
    pub fn validate_sdk_config(&self) -> Result<()> {
        let text =
            std::fs::read_to_string(&self.sdk_config).context("read native Yida server config")?;
        let mut values = std::collections::HashMap::new();
        for raw in text.lines() {
            let line = raw.split('#').next().unwrap_or_default().trim();
            if line.is_empty() {
                continue;
            }
            let (key, value) = line
                .split_once('=')
                .context("native Yida config expects key=value")?;
            ensure!(
                values.insert(key.trim(), value.trim()).is_none(),
                "duplicate native Yida config key"
            );
        }
        for (key, expected) in [
            ("UDPTrading", "no"),
            ("ConnectTCPMarketData", "no"),
            ("ReceiveUDPMarketData", "no"),
            ("RecalcMode", "off"),
        ] {
            ensure!(
                values.get(key) == Some(&expected),
                "native Yida config must explicitly set {key}={expected}"
            );
        }
        for key in ["TradingServerIP", "TradingServerPort"] {
            ensure!(
                values.get(key).is_some_and(|v| !v.is_empty()),
                "missing native Yida server config key {key}"
            );
        }
        Ok(())
    }
    pub fn credentials(&self) -> Result<Credentials> {
        fn get(name: &str, required: bool) -> Result<CString> {
            let value = std::env::var(name).unwrap_or_default();
            ensure!(
                (!required || !value.is_empty())
                    && value.len() <= 128
                    && value.is_ascii()
                    && !value.as_bytes().contains(&0),
                "missing/invalid credential environment variable {name}"
            );
            Ok(CString::new(value)?)
        }
        Ok(Credentials {
            user: get(&self.user_id_env, true)?,
            password: get(&self.password_env, true)?,
            app_id: get(&self.app_id_env, true)?,
            auth_code: get(&self.auth_code_env, true)?,
        })
    }
}
