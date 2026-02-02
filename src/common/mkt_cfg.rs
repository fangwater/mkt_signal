use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
struct LocalIpConfig {
    primary_local_ip: String,
    secondary_local_ip: String,
}

pub fn home_mkt_cfg_path() -> Result<PathBuf> {
    if let Ok(home) = std::env::var("HOME") {
        if !home.trim().is_empty() {
            return Ok(PathBuf::from(home).join("mkt_pub/config/mkt_cfg.yaml"));
        }
    }
    if let Ok(user) = std::env::var("USER") {
        if !user.trim().is_empty() {
            return Ok(PathBuf::from(format!(
                "/home/{}/mkt_pub/config/mkt_cfg.yaml",
                user
            )));
        }
    }
    Err(anyhow!(
        "HOME/USER not set; cannot resolve /home/<user>/mkt_pub/config/mkt_cfg.yaml"
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
