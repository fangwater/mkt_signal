//! Depth Publisher 配置模块

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::fs;

pub const DEFAULT_ACCOUNT_SUBSCRIPTIONS_PATH: &str = "config/depth_account_subscriptions.toml";
pub const DEFAULT_ACCOUNT_RELOAD_INTERVAL_SECS: u64 = 30 * 60;
pub const DEFAULT_ORDER_TTL_SECS: u64 = 30 * 60;

/// 推送配置
#[derive(Debug, Deserialize, Clone)]
pub struct PushConfig {
    pub min_push_interval_ms: u64,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            min_push_interval_ms: 100,
        }
    }
}

/// 配置文件结构
#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    push_config: PushConfig,
}

/// Depth Publisher 配置
#[derive(Debug, Clone)]
pub struct DepthPubConfig {
    pub push_config: PushConfig,
}

impl DepthPubConfig {
    /// 从配置文件加载配置
    pub async fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path).await?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;

        Ok(Self {
            push_config: config_file.push_config,
        })
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct DepthAccountRuntimeConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_account_reload_interval_secs")]
    pub reload_interval_secs: u64,
    #[serde(default = "default_order_ttl_secs")]
    pub order_ttl_secs: u64,
}

impl Default for DepthAccountRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            reload_interval_secs: DEFAULT_ACCOUNT_RELOAD_INTERVAL_SECS,
            order_ttl_secs: DEFAULT_ORDER_TTL_SECS,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct DepthAccountSubscriptionConfig {
    /// Stable account key used by depth query disambiguation and logs.
    pub account_id: String,
    /// Full iceoryx service name, including private namespace prefix.
    pub service_name: String,
    /// Venue carried by the account monitor payloads this subscriber should accept.
    pub venue: order_common::TradingVenue,
    #[serde(default = "default_amount_scale")]
    pub amount_scale: f64,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct DepthAccountSubscriptionFile {
    #[serde(default)]
    runtime: DepthAccountRuntimeConfig,
    #[serde(default)]
    accounts: Vec<DepthAccountSubscriptionConfig>,
}

#[derive(Debug, Clone)]
pub struct DepthAccountSubscriptionsConfig {
    pub runtime: DepthAccountRuntimeConfig,
    pub accounts: Vec<DepthAccountSubscriptionConfig>,
}

impl Default for DepthAccountSubscriptionsConfig {
    fn default() -> Self {
        Self {
            runtime: DepthAccountRuntimeConfig::default(),
            accounts: Vec::new(),
        }
    }
}

impl DepthAccountSubscriptionsConfig {
    pub async fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path).await.with_context(|| {
            format!("failed to read depth account subscriptions config: {path}")
        })?;
        parse_toml_config::<DepthAccountSubscriptionFile>(&content)
            .with_context(|| format!("failed to parse depth account subscriptions config: {path}"))
            .map(|file| Self {
                runtime: normalize_runtime_config(file.runtime),
                accounts: normalize_account_configs(file.accounts),
            })
    }

    pub fn load_sync(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path).with_context(|| {
            format!("failed to read depth account subscriptions config: {path}")
        })?;
        parse_toml_config::<DepthAccountSubscriptionFile>(&content)
            .with_context(|| format!("failed to parse depth account subscriptions config: {path}"))
            .map(|file| Self {
                runtime: normalize_runtime_config(file.runtime),
                accounts: normalize_account_configs(file.accounts),
            })
    }
}

fn parse_toml_config<T: DeserializeOwned>(content: &str) -> Result<T> {
    Ok(toml::from_str(content)?)
}

fn normalize_runtime_config(mut runtime: DepthAccountRuntimeConfig) -> DepthAccountRuntimeConfig {
    if runtime.reload_interval_secs == 0 {
        runtime.reload_interval_secs = DEFAULT_ACCOUNT_RELOAD_INTERVAL_SECS;
    }
    if runtime.order_ttl_secs == 0 {
        runtime.order_ttl_secs = DEFAULT_ORDER_TTL_SECS;
    }
    runtime
}

fn normalize_account_configs(
    accounts: Vec<DepthAccountSubscriptionConfig>,
) -> Vec<DepthAccountSubscriptionConfig> {
    accounts
        .into_iter()
        .filter_map(|mut account| {
            account.account_id = account.account_id.trim().to_string();
            account.service_name = account.service_name.trim().to_string();
            if account.account_id.is_empty() || account.service_name.is_empty() {
                return None;
            }
            if !account.amount_scale.is_finite() || account.amount_scale <= 0.0 {
                account.amount_scale = default_amount_scale();
            }
            Some(account)
        })
        .collect()
}

const fn default_enabled() -> bool {
    true
}

const fn default_account_reload_interval_secs() -> u64 {
    DEFAULT_ACCOUNT_RELOAD_INTERVAL_SECS
}

const fn default_order_ttl_secs() -> u64 {
    DEFAULT_ORDER_TTL_SECS
}

const fn default_amount_scale() -> f64 {
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_subscription_toml_parses_full_service_names() {
        let raw = r#"
            [runtime]
            enabled = true
            reload_interval_secs = 1800
            order_ttl_secs = 1800

            [[accounts]]
            account_id = "gate_fr_arb01"
            service_name = "gate_fr_arb01/account_pubs/gate_pm"
            venue = "gate_futures"
        "#;

        let parsed: DepthAccountSubscriptionFile = parse_toml_config(raw).unwrap();
        let cfg = DepthAccountSubscriptionsConfig {
            runtime: normalize_runtime_config(parsed.runtime),
            accounts: normalize_account_configs(parsed.accounts),
        };
        assert_eq!(cfg.runtime.reload_interval_secs, 1800);
        assert_eq!(cfg.runtime.order_ttl_secs, 1800);
        assert_eq!(cfg.accounts.len(), 1);
        assert_eq!(cfg.accounts[0].account_id, "gate_fr_arb01");
        assert_eq!(
            cfg.accounts[0].service_name,
            "gate_fr_arb01/account_pubs/gate_pm"
        );
        assert_eq!(
            cfg.accounts[0].venue,
            order_common::TradingVenue::GateFutures
        );
    }
}
