use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

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
    if let Some(path) = find_trade_engine_local_cfg_path()? {
        let local_ips = load_trade_engine_local_ips_from_toml_path(&path).await?;
        if local_ips.len() < 2 {
            return Err(anyhow!(
                "trade_engine config {} must provide at least 2 local IPs for account monitors",
                path.display()
            ));
        }
        return Ok((
            (local_ips[0].clone(), local_ips[1].clone()),
            path.display().to_string(),
        ));
    }

    let cfg_path = home_mkt_cfg_path()?;
    let ips = load_local_ips_from_path(&cfg_path).await?;
    Ok((
        ips,
        format!("{} (fallback mkt_cfg.yaml)", cfg_path.display()),
    ))
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
    Ok(local_ips)
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
}
