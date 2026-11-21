use crate::model::{is_future_symbol, option_symbol_parts, ExecutionRoute, RiskLimits, Venue};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
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
    pub trading_protocol: TradingProtocol,
    #[serde(default = "default_order_group")]
    pub order_group: u8,
    #[serde(default)]
    pub execution_route: ExecutionRoute,
    /// Endpoint-pinned one-lot test mode for the official SHFE/INE simulator.
    #[serde(default)]
    pub public_simulation: bool,
    /// Optional for offline fixtures. Configured placement is strict: no fallback.
    pub placement: Option<crate::placement::PlacementConfig>,
    pub flow_directory: PathBuf,
    /// Defaults to flow_directory/persist_manager: persist_manager's store, read
    /// once at startup. Never shared across accounts.
    pub persistence_directory: Option<PathBuf>,
    pub position_ipc: Option<cn_futures_state::Config>,
    pub exec: Option<crate::ctp::exec::YidaSettings>,
    pub position_target_ingress: Option<cn_futures_target::Config>,
    /// Read-only L1 for held/configured contracts, independent of trading permissions.
    #[serde(default)]
    pub observation_l1: bool,
    #[serde(default)]
    pub preallocated_orders: usize,
    pub user_id_env: String,
    pub password_env: String,
    pub app_id_env: String,
    pub auth_code_env: String,
    pub event_capacity: usize,
    pub response_timeout_ms: u64,
    /// `busy` on an isolated core, `sleep` (at most 1 ms) on a shared host.
    #[serde(default)]
    pub poll: crate::poll::PollMode,
    pub symbols: Vec<SymbolConfig>,
    pub risk: RiskLimits,
}
fn default_order_group() -> u8 {
    1
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TradingProtocol {
    Tcp,
    Udp,
    Xtcp,
}
impl TradingProtocol {
    pub fn native_name(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
            Self::Xtcp => "XTCP",
        }
    }
    pub fn native_code(self) -> i32 {
        match self {
            Self::Tcp => 0,
            Self::Udp => 1,
            Self::Xtcp => 2,
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolConfig {
    pub venue: Venue,
    pub symbol: String,
    pub cash_budget_per_open_lot: f64,
    pub cash_buffer_per_buy_lot: Option<f64>,
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
        ensure!(
            self.risk.max_order_lots.is_some()
                && self.risk.max_position_lots_per_side.is_some()
                && self.risk.max_working_lots.is_some(),
            "Yida requires explicit account lot limits"
        );
        if let Some(exec) = &self.exec {
            exec.validate(&self.risk)?;
            ensure!(
                self.observation_l1
                    && self.symbols.len() <= cn_futures_target::MAX_CONTRACTS
                    && self
                        .symbols
                        .iter()
                        .all(|s| is_future_symbol(s.venue, &s.symbol)),
                "Yida exec requires live L1 and explicit futures-only contract scope"
            );
        }
        if let Some(ingress) = &self.position_target_ingress {
            ingress.validate()?;
            ensure!(
                self.exec
                    .as_ref()
                    .is_some_and(|exec| exec.source_id == ingress.source_id),
                "position_target_ingress requires exec with the same source_id"
            );
        }
        if let Some(ipc) = &self.position_ipc {
            ipc.validate()?;
        }
        ensure!(
            self.order_group != 0,
            "nonzero Yida order_group (1..255) required for pre-ack cancellation"
        );
        self.execution_route.validate()?;
        if self.public_simulation {
            ensure!(
                self.risk.max_order_lots == Some(1)
                    && self.risk.max_working_lots.is_some_and(|limit| limit <= 2),
                "public_simulation requires max_order_lots=1 and max_working_lots<=2"
            );
        }
        if let Some(placement) = &self.placement {
            placement.validate()?;
        }
        for p in [&self.sdk_library, &self.sdk_config, &self.flow_directory] {
            ensure!(
                p.is_absolute(),
                "Yida SDK/config/flow paths must be absolute"
            );
        }
        ensure!(
            (64..=1_048_576).contains(&self.event_capacity),
            "invalid event capacity"
        );
        ensure!(self.response_timeout_ms >= 5000, "invalid response timeout");
        self.poll.validate(crate::poll::MAX_SLEEP_US)?;
        let mut seen = HashSet::new();
        ensure!(
            !self.symbols.is_empty(),
            "explicit SHFE/INE contract allowlist required"
        );
        for s in &self.symbols {
            ensure!(
                matches!(s.venue, Venue::Shfe | Venue::Ine)
                    && (is_future_symbol(s.venue, &s.symbol)
                        || option_symbol_parts(s.venue, &s.symbol).is_some())
                    && seen.insert((s.venue, &s.symbol)),
                "invalid/duplicate SHFE/INE future or option"
            );
            if option_symbol_parts(s.venue, &s.symbol).is_some() {
                ensure!(
                    s.cash_buffer_per_buy_lot
                        .is_some_and(|v| v.is_finite() && v > 0.0),
                    "option requires a positive cash_buffer_per_buy_lot"
                );
            } else {
                ensure!(
                    s.cash_buffer_per_buy_lot.is_none(),
                    "cash_buffer_per_buy_lot is option-specific"
                );
            }
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
        if let Some(path) = &self.persistence_directory {
            ensure!(path.is_absolute(), "persistence_directory must be absolute");
        }
        self.risk.validate()
    }
    /// Check the broker-owned native config before loading any vendor code. The
    /// transport must be explicit and match the business configuration.
    pub fn validate_sdk_config(&self) -> Result<()> {
        let text =
            std::fs::read_to_string(&self.sdk_config).context("read native Yida server config")?;
        self.validate_sdk_config_text(&text)
    }
    pub(super) fn validate_sdk_config_text(&self, text: &str) -> Result<()> {
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
        ensure!(
            !values.contains_key("UDPTrading"),
            "remove legacy UDPTrading; explicitly configure TradingProtocol"
        );
        for (key, expected) in [
            ("TradingProtocol", self.trading_protocol.native_name()),
            (
                "ConnectTCPMarketData",
                if self.observation_l1 { "yes" } else { "no" },
            ),
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
        let port = values["TradingServerPort"].parse::<u16>();
        ensure!(port.is_ok_and(|p| p > 0), "invalid TCP TradingServerPort");
        if self.public_simulation {
            ensure!(
                values["TradingServerIP"] == "118.190.175.212"
                    && values["TradingServerPort"] == "41600",
                "public_simulation is restricted to the official SHFE/INE simulator endpoint"
            );
            for key in values.keys() {
                ensure!(
                    matches!(
                        *key,
                        "TradingServerIP"
                            | "TradingServerPort"
                            | "TradingProtocol"
                            | "UDPTradingType"
                            | "XTCPTradingType"
                            | "ConnectTCPMarketData"
                            | "ReceiveUDPMarketData"
                            | "RecalcMode"
                            | "TradingServerTimeout"
                            | "TCPMarketDataCPUID"
                            | "TCPTradingCPUID"
                            | "XTCPTradingCPUID"
                            | "TimerCPUID"
                    ),
                    "native config key {key} is not supported in public_simulation"
                );
            }
        }
        match self.trading_protocol {
            TradingProtocol::Udp => ensure!(
                matches!(values.get("UDPTradingType"), Some(&"socket" | &"sf" | &"exa" | &"yusnic")),
                "UDPTradingType must explicitly select socket, sf, exa or yusnic; automatic can silently fall back"
            ),
            TradingProtocol::Xtcp => ensure!(
                matches!(values.get("XTCPTradingType"), Some(&"automatic" | &"socket")),
                "XTCPTradingType must explicitly select automatic or socket"
            ),
            TradingProtocol::Tcp => {}
        }
        if let Some(placement) = &self.placement {
            let mut keys = vec!["TCPTradingCPUID", "TimerCPUID"];
            if self.trading_protocol == TradingProtocol::Xtcp {
                keys.push("XTCPTradingCPUID");
            }
            if self.observation_l1 {
                keys.push("TCPMarketDataCPUID");
            }
            for key in keys {
                let sdk_core = values
                    .get(key)
                    .and_then(|v| v.parse::<usize>().ok())
                    .with_context(|| format!("explicit {key} required with placement"))?;
                ensure!(
                    placement.sdk_cpus.contains(&sdk_core),
                    "{key} must belong to placement.sdk_cpus"
                );
            }
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
