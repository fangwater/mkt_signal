use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
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
    binance_um_ws_health: Option<BinanceUmWsHealthTomlConfig>,
    binance_um_ws_route_eval: Option<BinanceUmWsRouteEvalTomlConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeEngineLocalIpConfig {
    pub local_ips: Vec<String>,
    pub binance_um_whitelist_ip: Option<String>,
    pub binance_um_ws_direct_ips: Vec<String>,
    pub binance_um_ws_health: BinanceUmWsHealthConfig,
    pub binance_um_ws_route_eval: BinanceUmWsRouteEvalConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceUmWsHealthConfig {
    pub new_rolling_window: usize,
    pub new_min_period: usize,
    pub cancel_rolling_window: usize,
    pub cancel_min_period: usize,
    pub percentile: u8,
    pub pause_ms: u64,
    pub select_recent: usize,
    pub cancel_probe_rate_limit_guard_pct: u32,
}

impl Default for BinanceUmWsHealthConfig {
    fn default() -> Self {
        Self {
            new_rolling_window: 200,
            new_min_period: 10,
            cancel_rolling_window: 200,
            cancel_min_period: 10,
            percentile: 85,
            pause_ms: 3_000,
            select_recent: 3,
            cancel_probe_rate_limit_guard_pct: 70,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceUmWsRouteEvalConfig {
    pub enabled: bool,
    pub redis_key_suffix: String,
    pub write_interval_ms: u64,
    pub route_family: String,
    pub min_samples: usize,
}

impl Default for BinanceUmWsRouteEvalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            redis_key_suffix: "binance_um_ws_route_eval".to_string(),
            write_interval_ms: 1_000,
            route_family: "default".to_string(),
            min_samples: 3,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BinanceUmWsHealthTomlConfig {
    new_rolling_window: Option<usize>,
    new_min_period: Option<usize>,
    cancel_rolling_window: Option<usize>,
    cancel_min_period: Option<usize>,
    percentile: Option<u8>,
    pause_ms: Option<u64>,
    select_recent: Option<usize>,
    cancel_probe_rate_limit_guard_pct: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BinanceUmWsRouteEvalTomlConfig {
    enabled: Option<bool>,
    redis_key_suffix: Option<String>,
    write_interval_ms: Option<u64>,
    route_family: Option<String>,
    min_samples: Option<usize>,
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
            binance_um_ws_health: BinanceUmWsHealthConfig::default(),
            binance_um_ws_route_eval: BinanceUmWsRouteEvalConfig::default(),
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

fn parse_binance_um_ws_health_config(
    raw: Option<BinanceUmWsHealthTomlConfig>,
    path: &Path,
) -> Result<BinanceUmWsHealthConfig> {
    let defaults = BinanceUmWsHealthConfig::default();
    let Some(raw) = raw else {
        return Ok(defaults);
    };

    let cfg = BinanceUmWsHealthConfig {
        new_rolling_window: raw.new_rolling_window.ok_or_else(|| {
            anyhow!(
                "binance_um_ws_health.new_rolling_window is required in {}",
                path.display()
            )
        })?,
        new_min_period: raw.new_min_period.ok_or_else(|| {
            anyhow!(
                "binance_um_ws_health.new_min_period is required in {}",
                path.display()
            )
        })?,
        cancel_rolling_window: raw.cancel_rolling_window.ok_or_else(|| {
            anyhow!(
                "binance_um_ws_health.cancel_rolling_window is required in {}",
                path.display()
            )
        })?,
        cancel_min_period: raw.cancel_min_period.ok_or_else(|| {
            anyhow!(
                "binance_um_ws_health.cancel_min_period is required in {}",
                path.display()
            )
        })?,
        percentile: raw.percentile.unwrap_or(defaults.percentile),
        pause_ms: raw.pause_ms.unwrap_or(defaults.pause_ms),
        select_recent: raw.select_recent.unwrap_or(defaults.select_recent),
        cancel_probe_rate_limit_guard_pct: raw
            .cancel_probe_rate_limit_guard_pct
            .unwrap_or(defaults.cancel_probe_rate_limit_guard_pct),
    };
    if cfg.new_rolling_window == 0 {
        return Err(anyhow!(
            "binance_um_ws_health.new_rolling_window must be > 0 in {}",
            path.display()
        ));
    }
    if cfg.cancel_rolling_window == 0 {
        return Err(anyhow!(
            "binance_um_ws_health.cancel_rolling_window must be > 0 in {}",
            path.display()
        ));
    }
    if cfg.new_min_period == 0 || cfg.new_min_period > cfg.new_rolling_window {
        return Err(anyhow!(
            "binance_um_ws_health.new_min_period must be in 1..=new_rolling_window in {}",
            path.display()
        ));
    }
    if cfg.cancel_min_period == 0 || cfg.cancel_min_period > cfg.cancel_rolling_window {
        return Err(anyhow!(
            "binance_um_ws_health.cancel_min_period must be in 1..=cancel_rolling_window in {}",
            path.display()
        ));
    }
    if cfg.percentile > 100 {
        return Err(anyhow!(
            "binance_um_ws_health.percentile must be <= 100 in {}",
            path.display()
        ));
    }
    if cfg.select_recent == 0 {
        return Err(anyhow!(
            "binance_um_ws_health.select_recent must be > 0 in {}",
            path.display()
        ));
    }
    if cfg.cancel_probe_rate_limit_guard_pct == 0 || cfg.cancel_probe_rate_limit_guard_pct > 100 {
        return Err(anyhow!(
            "binance_um_ws_health.cancel_probe_rate_limit_guard_pct must be in 1..=100 in {}",
            path.display()
        ));
    }
    Ok(cfg)
}

fn parse_binance_um_ws_route_eval_config(
    raw: Option<BinanceUmWsRouteEvalTomlConfig>,
    path: &Path,
) -> Result<BinanceUmWsRouteEvalConfig> {
    let defaults = BinanceUmWsRouteEvalConfig::default();
    let Some(raw) = raw else {
        return Ok(defaults);
    };

    let redis_key_suffix = raw
        .redis_key_suffix
        .unwrap_or(defaults.redis_key_suffix)
        .trim()
        .to_string();
    if redis_key_suffix.is_empty() {
        return Err(anyhow!(
            "binance_um_ws_route_eval.redis_key_suffix must not be empty in {}",
            path.display()
        ));
    }
    let route_family = raw
        .route_family
        .unwrap_or(defaults.route_family)
        .trim()
        .to_string();
    if route_family.is_empty() {
        return Err(anyhow!(
            "binance_um_ws_route_eval.route_family must not be empty in {}",
            path.display()
        ));
    }

    let cfg = BinanceUmWsRouteEvalConfig {
        enabled: raw.enabled.unwrap_or(defaults.enabled),
        redis_key_suffix,
        write_interval_ms: raw.write_interval_ms.unwrap_or(defaults.write_interval_ms),
        route_family,
        min_samples: raw.min_samples.unwrap_or(defaults.min_samples),
    };
    if cfg.write_interval_ms == 0 {
        return Err(anyhow!(
            "binance_um_ws_route_eval.write_interval_ms must be > 0 in {}",
            path.display()
        ));
    }
    if cfg.min_samples == 0 {
        return Err(anyhow!(
            "binance_um_ws_route_eval.min_samples must be > 0 in {}",
            path.display()
        ));
    }
    Ok(cfg)
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
    let binance_um_ws_health = parse_binance_um_ws_health_config(cfg.binance_um_ws_health, path)?;
    let binance_um_ws_route_eval =
        parse_binance_um_ws_route_eval_config(cfg.binance_um_ws_route_eval, path)?;
    if binance_um_ws_route_eval.enabled
        && binance_um_ws_route_eval.min_samples > binance_um_ws_health.select_recent
    {
        return Err(anyhow!(
            "binance_um_ws_route_eval.min_samples must be <= binance_um_ws_health.select_recent in {}",
            path.display()
        ));
    }

    Ok(TradeEngineLocalIpConfig {
        local_ips,
        binance_um_whitelist_ip: trim_optional_empty_ok(cfg.binance_um_whitelist_ip),
        binance_um_ws_direct_ips: parse_trimmed_ip_list(
            cfg.binance_um_ws_direct_ips,
            "binance_um_ws_direct_ips",
            path,
        )?,
        binance_um_ws_health,
        binance_um_ws_route_eval,
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
                [binance_um_ws_health]
                new_rolling_window = 200
                new_min_period = 10
                cancel_rolling_window = 200
                cancel_min_period = 10
                percentile = 85
                pause_ms = 3000
                select_recent = 3
                cancel_probe_rate_limit_guard_pct = 70
                [binance_um_ws_route_eval]
                enabled = true
                redis_key_suffix = "binance_um_ws_route_eval"
                write_interval_ms = 500
                route_family = "jp-binance-um-v1"
                min_samples = 3
            "#,
            Path::new("trade_engine.toml"),
        )
        .unwrap();

        assert_eq!(
            parsed.binance_um_ws_direct_ips,
            vec!["13.112.240.202".to_string(), "13.158.151.48".to_string()]
        );
        assert_eq!(
            parsed.binance_um_ws_health,
            BinanceUmWsHealthConfig {
                new_rolling_window: 200,
                new_min_period: 10,
                cancel_rolling_window: 200,
                cancel_min_period: 10,
                percentile: 85,
                pause_ms: 3_000,
                select_recent: 3,
                cancel_probe_rate_limit_guard_pct: 70,
            }
        );
        assert_eq!(
            parsed.binance_um_ws_route_eval,
            BinanceUmWsRouteEvalConfig {
                enabled: true,
                redis_key_suffix: "binance_um_ws_route_eval".to_string(),
                write_interval_ms: 500,
                route_family: "jp-binance-um-v1".to_string(),
                min_samples: 3,
            }
        );
    }

    #[test]
    fn parse_trade_engine_toml_rejects_empty_route_eval_key() {
        let err = parse_trade_engine_local_ip_config_toml(
            r#"
                local_ips = ["172.31.33.133"]
                [binance_um_ws_route_eval]
                enabled = true
                redis_key_suffix = " "
            "#,
            Path::new("trade_engine.toml"),
        )
        .unwrap_err();

        assert!(format!("{err:#}").contains("redis_key_suffix"));
    }

    #[test]
    fn parse_trade_engine_toml_rejects_route_eval_min_samples_above_select_recent() {
        let err = parse_trade_engine_local_ip_config_toml(
            r#"
                local_ips = ["172.31.33.133"]
                [binance_um_ws_health]
                new_rolling_window = 200
                new_min_period = 10
                cancel_rolling_window = 200
                cancel_min_period = 10
                select_recent = 3
                [binance_um_ws_route_eval]
                enabled = true
                min_samples = 4
            "#,
            Path::new("trade_engine.toml"),
        )
        .unwrap_err();

        assert!(format!("{err:#}").contains("min_samples"));
    }

    #[test]
    fn parse_trade_engine_toml_rejects_legacy_binance_um_ws_health_window() {
        let err = parse_trade_engine_local_ip_config_toml(
            r#"
                local_ips = ["172.31.33.133"]
                [binance_um_ws_health]
                rolling_window = 111
            "#,
            Path::new("trade_engine.toml"),
        )
        .unwrap_err();

        assert!(format!("{err:#}").contains("rolling_window"));
    }
}
