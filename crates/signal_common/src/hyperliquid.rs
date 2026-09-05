use anyhow::{bail, Context, Result};
use reqwest::Url;

pub const HYPERLIQUID_MAINNET_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
pub const HYPERLIQUID_MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const HYPERLIQUID_TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";
pub const HYPERLIQUID_TESTNET_WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";
pub const DEFAULT_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS: u64 = 15_000;
pub const MIN_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS: u64 = 1_000;
pub const MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS: u64 = 60_000;
pub const HYPERLIQUID_ACTION_COMMIT_CLOCK_MARGIN_MS: u64 = 5_000;

pub fn hyperliquid_action_expires_after_ms() -> Result<u64> {
    parse_action_expires_after_ms(nonempty_env("HYPERLIQUID_ACTION_EXPIRES_AFTER_MS").as_deref())
}

fn parse_action_expires_after_ms(raw: Option<&str>) -> Result<u64> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(DEFAULT_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS);
    };
    let value = raw
        .parse::<u64>()
        .context("HYPERLIQUID_ACTION_EXPIRES_AFTER_MS must be an integer")?;
    if !(MIN_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS..=MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS)
        .contains(&value)
    {
        bail!(
            "HYPERLIQUID_ACTION_EXPIRES_AFTER_MS must be within {}..={}",
            MIN_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS,
            MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS
        );
    }
    Ok(value)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HyperliquidEndpoints {
    pub testnet: bool,
    pub info_url: String,
    pub ws_url: String,
}

impl HyperliquidEndpoints {
    pub fn from_env() -> Result<Self> {
        let testnet = parse_env_flag("HYPERLIQUID_TESTNET")?.unwrap_or(false);
        Self::resolve(
            testnet,
            nonempty_env("HYPERLIQUID_INFO_URL").as_deref(),
            nonempty_env("HYPERLIQUID_WS_URL").as_deref(),
        )
    }

    pub fn mainnet() -> Self {
        Self {
            testnet: false,
            info_url: HYPERLIQUID_MAINNET_INFO_URL.to_string(),
            ws_url: HYPERLIQUID_MAINNET_WS_URL.to_string(),
        }
    }

    pub fn resolve(
        testnet: bool,
        info_override: Option<&str>,
        ws_override: Option<&str>,
    ) -> Result<Self> {
        let default_info = if testnet {
            HYPERLIQUID_TESTNET_INFO_URL
        } else {
            HYPERLIQUID_MAINNET_INFO_URL
        };
        let default_ws = if testnet {
            HYPERLIQUID_TESTNET_WS_URL
        } else {
            HYPERLIQUID_MAINNET_WS_URL
        };
        let info_url = info_override
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(default_info)
            .to_string();
        let ws_url = ws_override
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(default_ws)
            .to_string();

        let info_network =
            validate_endpoint("HYPERLIQUID_INFO_URL", &info_url, EndpointKind::Info)?;
        let ws_network = validate_endpoint("HYPERLIQUID_WS_URL", &ws_url, EndpointKind::Ws)?;
        if let (Some(info_network), Some(ws_network)) = (info_network, ws_network) {
            if info_network != ws_network {
                bail!(
                    "Hyperliquid endpoint network mismatch: HYPERLIQUID_INFO_URL={} HYPERLIQUID_WS_URL={}",
                    info_url,
                    ws_url
                );
            }
        }
        for (name, url, network) in [
            ("HYPERLIQUID_INFO_URL", info_url.as_str(), info_network),
            ("HYPERLIQUID_WS_URL", ws_url.as_str(), ws_network),
        ] {
            if network.is_some_and(|endpoint_testnet| endpoint_testnet != testnet) {
                bail!(
                    "{}={} disagrees with HYPERLIQUID_TESTNET={}",
                    name,
                    url,
                    if testnet { 1 } else { 0 }
                );
            }
        }

        Ok(Self {
            testnet,
            info_url,
            ws_url,
        })
    }
}

#[derive(Clone, Copy)]
enum EndpointKind {
    Info,
    Ws,
}

fn validate_endpoint(name: &str, raw: &str, kind: EndpointKind) -> Result<Option<bool>> {
    let url = Url::parse(raw).with_context(|| format!("invalid {name}={raw:?}"))?;
    let valid_scheme = match kind {
        EndpointKind::Info => matches!(url.scheme(), "http" | "https"),
        EndpointKind::Ws => matches!(url.scheme(), "ws" | "wss"),
    };
    if !valid_scheme || url.host_str().is_none() {
        bail!("invalid {name}={raw:?}: unexpected scheme or missing host");
    }
    Ok(
        match url
            .host_str()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "api.hyperliquid.xyz" => Some(false),
            "api.hyperliquid-testnet.xyz" => Some(true),
            _ => None,
        },
    )
}

fn parse_env_flag(name: &str) -> Result<Option<bool>> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(None);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" | "" => Ok(Some(false)),
        _ => bail!("invalid {name}={raw:?}; expected a boolean"),
    }
}

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_follow_selected_network() {
        assert_eq!(
            HyperliquidEndpoints::resolve(false, None, None).unwrap(),
            HyperliquidEndpoints::mainnet()
        );
        let testnet = HyperliquidEndpoints::resolve(true, None, None).unwrap();
        assert_eq!(testnet.info_url, HYPERLIQUID_TESTNET_INFO_URL);
        assert_eq!(testnet.ws_url, HYPERLIQUID_TESTNET_WS_URL);
    }

    #[test]
    fn rejects_known_cross_network_or_wrong_scheme_endpoints() {
        assert!(
            HyperliquidEndpoints::resolve(false, Some(HYPERLIQUID_TESTNET_INFO_URL), None,)
                .is_err()
        );
        assert!(HyperliquidEndpoints::resolve(
            true,
            Some(HYPERLIQUID_TESTNET_INFO_URL),
            Some(HYPERLIQUID_MAINNET_WS_URL),
        )
        .is_err());
        assert!(
            HyperliquidEndpoints::resolve(false, Some(HYPERLIQUID_MAINNET_WS_URL), None,).is_err()
        );
    }

    #[test]
    fn permits_same_network_custom_proxies() {
        let endpoints = HyperliquidEndpoints::resolve(
            true,
            Some("https://hl-proxy.internal/info"),
            Some("wss://hl-proxy.internal/ws"),
        )
        .unwrap();
        assert!(endpoints.testnet);
        assert_eq!(endpoints.info_url, "https://hl-proxy.internal/info");
        assert_eq!(endpoints.ws_url, "wss://hl-proxy.internal/ws");
    }

    #[test]
    fn action_expiry_uses_bounded_shared_configuration() {
        assert_eq!(
            parse_action_expires_after_ms(None).unwrap(),
            DEFAULT_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS
        );
        assert_eq!(
            parse_action_expires_after_ms(Some("60000")).unwrap(),
            MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS
        );
        assert!(parse_action_expires_after_ms(Some("60001")).is_err());
        assert!(parse_action_expires_after_ms(Some("not-a-number")).is_err());
    }
}
