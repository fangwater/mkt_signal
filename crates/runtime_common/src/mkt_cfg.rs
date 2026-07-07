use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const BINANCE_UM_IP_WHITELIST_MODE_ENV: &str = "BINANCE_UM_IP_WHITELIST_MODE";

#[derive(Debug, Deserialize)]
struct LocalIpConfig {
    primary_local_ip: String,
    secondary_local_ip: String,
}

#[derive(Debug, Default, Deserialize)]
struct TradeEngineTomlConfig {
    #[serde(default)]
    local_ips: Vec<String>,
    primary_local_ip: Option<String>,
    secondary_local_ip: Option<String>,
    #[serde(alias = "binance_um_ip_whitelist_ip")]
    binance_um_whitelist_ip: Option<String>,
    #[serde(default, alias = "binance_um_ws_direct_ip")]
    binance_um_ws_direct_ips: Vec<String>,
    // 统一的 WS 下单路由（不再绑定 Binance-UM）。
    ws_route: Option<WsRouteTomlConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeEngineLocalIpConfig {
    pub local_ips: Vec<String>,
    pub binance_um_whitelist_ip: Option<String>,
    pub binance_um_ws_direct_ips: Vec<String>,
    pub ws_route: WsRouteConfig,
}

/// WS 下单端点的路由方式。
/// - `Rr`：最基础的 round-robin（只看 `is_available()`），不受 TCP 丢包健康度影响。
/// - `Dispatch`：在 RR 基础上叠加 TCP 丢包健康度——绕开被判定为丢包暂停的端点，
///   并在持续丢包且无 inflight 时触发换连接（换源端口=换 ECMP 路径）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WsRouteKind {
    #[default]
    Rr,
    Dispatch,
}

impl WsRouteKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rr => "rr",
            Self::Dispatch => "dispatch",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsRouteConfig {
    pub route: WsRouteKind,
}

impl Default for WsRouteConfig {
    fn default() -> Self {
        Self {
            route: WsRouteKind::Rr,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WsRouteTomlConfig {
    route: Option<String>,
}

pub fn home_mkt_cfg_path() -> Result<PathBuf> {
    if let Ok(home) = std::env::var("HOME") {
        if !home.trim().is_empty() {
            return Ok(PathBuf::from(home).join("dat_pbs/config/mkt_cfg.yaml"));
        }
    }
    if let Ok(user) = std::env::var("USER") {
        if !user.trim().is_empty() {
            return Ok(PathBuf::from(format!(
                "/home/{}/dat_pbs/config/mkt_cfg.yaml",
                user
            )));
        }
    }
    Err(anyhow!(
        "HOME/USER not set; cannot resolve /home/<user>/dat_pbs/config/mkt_cfg.yaml"
    ))
}

pub async fn load_local_ips_from_path(path: &Path) -> Result<(String, String)> {
    let content = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("read mkt cfg: {}", path.display()))?;
    let cfg: LocalIpConfig = serde_yaml::from_str(&content)
        .with_context(|| format!("parse mkt cfg: {}", path.display()))?;

    let primary = cfg.primary_local_ip.trim().to_string();
    let secondary = cfg.secondary_local_ip.trim().to_string();
    if primary.is_empty() || secondary.is_empty() {
        return Err(anyhow!(
            "primary_local_ip/secondary_local_ip is empty in {}",
            path.display()
        ));
    }
    Ok((primary, secondary))
}

pub async fn load_local_ips_preferring_trade_engine() -> Result<((String, String), String)> {
    let (cfg, source) = load_trade_engine_local_ip_config_preferring_trade_engine().await?;
    if cfg.local_ips.len() < 2 {
        return Err(anyhow!(
            "trade_engine config {} must provide at least 2 local IPs for account monitors",
            source
        ));
    }

    Ok(((cfg.local_ips[0].clone(), cfg.local_ips[1].clone()), source))
}

pub async fn load_trade_engine_local_ip_config_preferring_trade_engine(
) -> Result<(TradeEngineLocalIpConfig, String)> {
    if let Some(path) = find_trade_engine_local_cfg_path()? {
        let cfg = load_trade_engine_local_ip_config_from_toml_path(&path).await?;
        return Ok((cfg, path.display().to_string()));
    }

    let cfg_path = home_mkt_cfg_path()?;
    let (primary_ip, secondary_ip) = load_local_ips_from_path(&cfg_path).await?;
    Ok((
        TradeEngineLocalIpConfig {
            local_ips: vec![primary_ip, secondary_ip],
            binance_um_whitelist_ip: None,
            binance_um_ws_direct_ips: Vec::new(),
            ws_route: WsRouteConfig::default(),
        },
        format!("{} (fallback mkt_cfg.yaml)", cfg_path.display()),
    ))
}

pub fn binance_um_ip_whitelist_mode_enabled() -> bool {
    match std::env::var(BINANCE_UM_IP_WHITELIST_MODE_ENV) {
        Ok(raw) => {
            let value = raw.trim().to_ascii_lowercase();
            match value.as_str() {
                "" | "off" => false,
                "on" => true,
                _ => panic!(
                    "{} must be 'on' or 'off' when set; got '{}'",
                    BINANCE_UM_IP_WHITELIST_MODE_ENV,
                    raw.trim()
                ),
            }
        }
        Err(_) => false,
    }
}

pub fn validate_binance_um_whitelist_ip_config(
    local_ips: &[String],
    whitelist_ip: Option<&str>,
    whitelist_mode_enabled: bool,
    source: &str,
    context: &str,
) {
    if let Some(ip) = whitelist_ip {
        let trimmed = ip.trim();
        if !local_ips.iter().any(|local_ip| local_ip.trim() == trimmed) {
            panic!(
                "{}: binance_um_whitelist_ip={} from {} must also be present in local_ips",
                context, trimmed, source
            );
        }
        log::info!(
            "{}: binance UM whitelist IP configured: {}",
            context,
            trimmed
        );
    }

    if whitelist_mode_enabled {
        let ip = whitelist_ip.map(str::trim).filter(|ip| !ip.is_empty());
        let Some(ip) = ip else {
            panic!(
                "{}: {}=on requires binance_um_whitelist_ip in local trade_engine.toml",
                context, BINANCE_UM_IP_WHITELIST_MODE_ENV
            )
        };
        log::info!(
            "{}: {}=on; binance UM whitelist IP is {}",
            context,
            BINANCE_UM_IP_WHITELIST_MODE_ENV,
            ip
        );
    }
}

pub fn load_primary_local_ip_preferring_trade_engine_sync() -> Result<(String, String)> {
    if let Some(path) = find_trade_engine_local_cfg_path()? {
        let local_ips = load_trade_engine_local_ips_from_toml_path_sync(&path)?;
        return Ok((local_ips[0].clone(), path.display().to_string()));
    }

    let cfg_path = home_mkt_cfg_path()?;
    let content = std::fs::read_to_string(&cfg_path)
        .with_context(|| format!("read mkt cfg: {}", cfg_path.display()))?;
    let cfg: LocalIpConfig = serde_yaml::from_str(&content)
        .with_context(|| format!("parse mkt cfg: {}", cfg_path.display()))?;
    let primary = cfg.primary_local_ip.trim().to_string();
    if primary.is_empty() {
        return Err(anyhow!(
            "primary_local_ip is empty in {}",
            cfg_path.display()
        ));
    }
    Ok((
        primary,
        format!("{} (fallback mkt_cfg.yaml)", cfg_path.display()),
    ))
}

pub fn load_primary_local_ip_from_trade_engine_sync() -> Result<(String, String)> {
    let Some(path) = find_trade_engine_local_cfg_path()? else {
        let cwd = std::env::current_dir().context("resolve current dir for trade_engine config")?;
        return Err(anyhow!(
            "trade_engine local IP config not found in {} (expected trade_engine.toml or trade engine.toml)",
            cwd.display()
        ));
    };

    let local_ips = load_trade_engine_local_ips_from_toml_path_sync(&path)?;
    Ok((local_ips[0].clone(), path.display().to_string()))
}

pub fn find_trade_engine_local_cfg_path() -> Result<Option<PathBuf>> {
    let cwd = std::env::current_dir().context("resolve current dir for trade_engine config")?;
    let candidates = [cwd.join("trade_engine.toml"), cwd.join("trade engine.toml")];
    let existing: Vec<PathBuf> = candidates
        .into_iter()
        .filter(|path| path.is_file())
        .collect();

    match existing.len() {
        0 => Ok(None),
        1 => Ok(existing.into_iter().next()),
        _ => Err(anyhow!(
            "multiple local trade_engine configs found: {}",
            existing
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn push_trimmed_ip(
    local_ips: &mut Vec<String>,
    value: Option<String>,
    field_name: &str,
    path: &Path,
) -> Result<()> {
    if let Some(ip) = value {
        let trimmed = ip.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("{} is empty in {}", field_name, path.display()));
        }
        local_ips.push(trimmed.to_string());
    }
    Ok(())
}

fn parse_trade_engine_local_ips_toml(content: &str, path: &Path) -> Result<Vec<String>> {
    Ok(parse_trade_engine_local_ip_config_toml(content, path)?.local_ips)
}

fn trim_optional_empty_ok(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn parse_trimmed_ip_list(
    values: Vec<String>,
    field_name: &str,
    path: &Path,
) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for (idx, value) in values.into_iter().enumerate() {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(anyhow!(
                "{}[{}] is empty in {}",
                field_name,
                idx,
                path.display()
            ));
        }
        out.push(trimmed.to_string());
    }
    Ok(out)
}

fn parse_ws_route_kind(raw: Option<String>, path: &Path) -> Result<WsRouteKind> {
    let Some(raw) = raw else {
        return Ok(WsRouteKind::Rr);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "rr" | "round_robin" | "round-robin" => Ok(WsRouteKind::Rr),
        "dispatch" => Ok(WsRouteKind::Dispatch),
        other => Err(anyhow!(
            "ws_route.route must be one of rr/dispatch in {}: {}",
            path.display(),
            other
        )),
    }
}

fn parse_ws_route_config(raw: Option<WsRouteTomlConfig>, path: &Path) -> Result<WsRouteConfig> {
    let defaults = WsRouteConfig::default();
    let Some(raw) = raw else {
        return Ok(defaults);
    };
    let route = parse_ws_route_kind(raw.route, path)?;
    Ok(WsRouteConfig { route })
}

fn parse_trade_engine_local_ip_config_toml(
    content: &str,
    path: &Path,
) -> Result<TradeEngineLocalIpConfig> {
    let cfg: TradeEngineTomlConfig = toml::from_str(content)
        .with_context(|| format!("parse trade_engine toml: {}", path.display()))?;

    let mut local_ips = Vec::new();
    for (idx, ip) in cfg.local_ips.into_iter().enumerate() {
        let trimmed = ip.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("local_ips[{}] is empty in {}", idx, path.display()));
        }
        local_ips.push(trimmed.to_string());
    }
    push_trimmed_ip(
        &mut local_ips,
        cfg.primary_local_ip,
        "primary_local_ip",
        path,
    )?;
    push_trimmed_ip(
        &mut local_ips,
        cfg.secondary_local_ip,
        "secondary_local_ip",
        path,
    )?;

    if local_ips.is_empty() {
        return Err(anyhow!(
            "trade_engine config {} must provide local_ips = [\"1.2.3.4\", \"5.6.7.8\"]",
            path.display()
        ));
    }
    let ws_route = parse_ws_route_config(cfg.ws_route, path)?;
    Ok(TradeEngineLocalIpConfig {
        local_ips,
        binance_um_whitelist_ip: trim_optional_empty_ok(cfg.binance_um_whitelist_ip),
        binance_um_ws_direct_ips: parse_trimmed_ip_list(
            cfg.binance_um_ws_direct_ips,
            "binance_um_ws_direct_ips",
            path,
        )?,
        ws_route,
    })
}

pub async fn load_trade_engine_local_ip_config_from_toml_path(
    path: &Path,
) -> Result<TradeEngineLocalIpConfig> {
    let content = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("read trade_engine toml: {}", path.display()))?;
    parse_trade_engine_local_ip_config_toml(&content, path)
}

pub async fn load_trade_engine_local_ips_from_toml_path(path: &Path) -> Result<Vec<String>> {
    let content = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("read trade_engine toml: {}", path.display()))?;
    parse_trade_engine_local_ips_toml(&content, path)
}

pub fn load_trade_engine_local_ips_from_toml_path_sync(path: &Path) -> Result<Vec<String>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("read trade_engine toml: {}", path.display()))?;
    parse_trade_engine_local_ips_toml(&content, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_trade_engine_toml_uses_local_ips_array() {
        let parsed = parse_trade_engine_local_ips_toml(
            r#"
                local_ips = [" 172.31.33.133 ", "172.31.46.90", "0.0.0.0"]
            "#,
            Path::new("trade_engine.toml"),
        )
        .unwrap();

        assert_eq!(
            parsed,
            vec![
                "172.31.33.133".to_string(),
                "172.31.46.90".to_string(),
                "0.0.0.0".to_string()
            ]
        );
    }

    #[test]
    fn parse_trade_engine_toml_accepts_legacy_primary_secondary_keys() {
        let parsed = parse_trade_engine_local_ips_toml(
            r#"
                primary_local_ip = "172.31.33.133"
                secondary_local_ip = "172.31.46.90"
            "#,
            Path::new("trade_engine.toml"),
        )
        .unwrap();

        assert_eq!(
            parsed,
            vec!["172.31.33.133".to_string(), "172.31.46.90".to_string()]
        );
    }

    #[test]
    fn parse_trade_engine_toml_accepts_binance_um_ws_direct_ips() {
        let parsed = parse_trade_engine_local_ip_config_toml(
            r#"
                local_ips = ["172.31.33.133", "172.31.46.90"]
                binance_um_whitelist_ip = "172.31.46.90"
                binance_um_ws_direct_ips = [" 13.112.240.202 ", "13.158.151.48"]
                [ws_route]
                route = "dispatch"
            "#,
            Path::new("trade_engine.toml"),
        )
        .unwrap();

        assert_eq!(
            parsed.binance_um_ws_direct_ips,
            vec!["13.112.240.202".to_string(), "13.158.151.48".to_string()]
        );
        assert_eq!(
            parsed.ws_route,
            WsRouteConfig {
                route: WsRouteKind::Dispatch,
            }
        );
    }

    #[test]
    fn parse_trade_engine_toml_defaults_ws_route_to_rr() {
        let parsed = parse_trade_engine_local_ip_config_toml(
            r#"
                local_ips = ["172.31.33.133"]
            "#,
            Path::new("trade_engine.toml"),
        )
        .unwrap();

        assert_eq!(parsed.ws_route.route, WsRouteKind::Rr);
    }

    #[test]
    fn parse_trade_engine_toml_rejects_unknown_route() {
        let err = parse_trade_engine_local_ip_config_toml(
            r#"
                local_ips = ["172.31.33.133"]
                [ws_route]
                route = "latency"
            "#,
            Path::new("trade_engine.toml"),
        )
        .unwrap_err();

        assert!(format!("{err:#}").contains("rr/dispatch"));
    }
}
