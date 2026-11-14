//! Current-snapshot CTP volume multiples.
//!
//! Reads `market_metadata.public.domestic_future_product_multipliers` the same
//! way Python `cn_futures.multipliers` does. Missing / unverified / non-positive
//! values are hard errors. Do not fall back to 1.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;

pub const DEFAULT_PSQL: &str = "/mnt/nvme-raid0-28t/apps/pgsql16/bin/psql";
pub const DEFAULT_SOCKET: &str = "/mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run";
pub const DEFAULT_PORT: &str = "5433";
pub const DEFAULT_DB: &str = "market_metadata";
pub const DEFAULT_USER: &str = "u171";

#[derive(Clone, Debug)]
pub struct ProductMultiplier {
    pub product: String,
    pub exchange: String,
    pub volume_multiple: f64,
    pub verified: bool,
}

pub fn load_multiplier_catalog() -> Result<HashMap<String, ProductMultiplier>> {
    if !Path::new(DEFAULT_PSQL).is_file() {
        bail!("psql not found: {DEFAULT_PSQL}");
    }
    let output = Command::new(DEFAULT_PSQL)
        .args([
            "-h",
            DEFAULT_SOCKET,
            "-p",
            DEFAULT_PORT,
            "-U",
            DEFAULT_USER,
            "-d",
            DEFAULT_DB,
            "-A",
            "-t",
            "-F",
            ",",
            "-c",
            "SELECT product, exchange, volume_multiple, verified FROM public.domestic_future_product_multipliers",
        ])
        .output()
        .context("run psql for domestic_future_product_multipliers")?;
    if !output.status.success() {
        bail!(
            "failed to load domestic_future_product_multipliers: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8(output.stdout).context("psql stdout utf8")?;
    let mut catalog = HashMap::new();
    for line in stdout.lines() {
        let text = line.trim();
        if text.is_empty() {
            continue;
        }
        let mut parts = text.splitn(4, ',');
        let product = parts.next().unwrap_or("").trim().to_ascii_uppercase();
        let exchange = parts.next().unwrap_or("").trim().to_ascii_lowercase();
        let multiple = parts
            .next()
            .unwrap_or("")
            .trim()
            .parse::<f64>()
            .with_context(|| format!("bad volume_multiple in {text}"))?;
        let verified_raw = parts.next().unwrap_or("").trim().to_ascii_lowercase();
        let verified = matches!(verified_raw.as_str(), "t" | "true" | "1");
        if !(multiple.is_finite() && multiple > 0.0) {
            bail!("non-positive volume_multiple for {product}: {multiple}");
        }
        catalog.insert(
            product.clone(),
            ProductMultiplier {
                product,
                exchange,
                volume_multiple: multiple,
                verified,
            },
        );
    }
    if catalog.is_empty() {
        bail!("domestic_future_product_multipliers is empty");
    }
    Ok(catalog)
}

pub fn require_multiplier(
    catalog: &HashMap<String, ProductMultiplier>,
    product: &str,
) -> Result<f64> {
    let key = product.trim().to_ascii_uppercase();
    let Some(row) = catalog.get(&key) else {
        bail!("missing verified volume_multiple for product={key}");
    };
    if !row.verified || !(row.volume_multiple.is_finite() && row.volume_multiple > 0.0) {
        bail!("missing verified volume_multiple for product={key}");
    }
    Ok(row.volume_multiple)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_rejects_missing() {
        let catalog = HashMap::new();
        assert!(require_multiplier(&catalog, "RB").is_err());
    }
}
