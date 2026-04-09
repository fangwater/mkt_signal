use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

use super::cfg::ModelPubConfig;
use crate::factor_pub::fusion_factor_pub::{ExtraFactorId, FusionFactorId};

const TLEN_SHARED_CONFIG_FIELD: &str = "__shared__";

#[derive(Debug, Deserialize)]
struct VenueFactorPlanResp {
    #[serde(default)]
    thresholds: HashMap<String, VenueFactorPlanItem>,
}

#[derive(Debug, Deserialize)]
struct VenueFactorPlanItem {
    #[serde(default)]
    factors: Vec<String>,
}

pub(crate) fn parse_venue_slug_from_input_service(input_service: &str) -> Result<String> {
    let trimmed = input_service.trim().trim_matches('/');
    let venue_slug = trimmed
        .rsplit('/')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("failed to parse venue from input_service"))?;
    Ok(venue_slug.to_string())
}

pub(crate) async fn load_venue_factor_names_from_tlen_server(
    config: &ModelPubConfig,
    venue_slug: &str,
) -> Result<Vec<String>> {
    let client = Client::builder()
        .timeout(Duration::from_millis(config.tlen_server_request_timeout_ms))
        .build()
        .context("build tlen_server client failed")?;
    let url = format!(
        "{}/api/thresholds",
        config.tlen_server_base_url.trim_end_matches('/')
    );
    let resp = client
        .get(&url)
        .query(&[("venue", venue_slug), ("config_type", "factor_plan")])
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .error_for_status()
        .with_context(|| format!("GET {} returned error status", url))?;

    let payload: VenueFactorPlanResp = resp
        .json()
        .await
        .with_context(|| format!("decode venue factor plan failed: {}", url))?;
    let item = payload
        .thresholds
        .get(TLEN_SHARED_CONFIG_FIELD)
        .ok_or_else(|| anyhow::anyhow!("tlen_server returned no shared factor plan"))?;
    if item.factors.is_empty() {
        anyhow::bail!("shared factor plan must not be empty: venue={}", venue_slug);
    }
    Ok(item.factors.clone())
}

pub(crate) fn build_factor_position_map(factor_names: &[String]) -> Result<HashMap<String, usize>> {
    let mut positions = HashMap::with_capacity(factor_names.len());
    for (idx, name) in factor_names.iter().enumerate() {
        let key = name.trim();
        if key.is_empty() {
            anyhow::bail!(
                "venue factor plan contains empty factor name at index {}",
                idx
            );
        }
        if positions.insert(key.to_string(), idx).is_some() {
            anyhow::bail!("duplicate factor in venue factor plan: {}", key);
        }
    }
    Ok(positions)
}

fn factor_name_to_index(name: &str) -> Option<u16> {
    if let Some(fid) = FusionFactorId::from_name(name) {
        return Some(fid.as_index());
    }
    if let Some(eid) = ExtraFactorId::from_name(name) {
        return Some(eid.as_index());
    }
    None
}

pub(crate) fn build_factor_indices(
    model_name: &str,
    symbol: &str,
    factor_names: &[String],
) -> Vec<u16> {
    let mut indices = Vec::with_capacity(factor_names.len());
    for name in factor_names {
        match factor_name_to_index(name) {
            Some(idx) => indices.push(idx),
            None => {
                panic!(
                    "unknown factor in model plan: model_name={} symbol={} factor={}",
                    model_name, symbol, name
                );
            }
        }
    }
    indices
}

pub(crate) fn build_extract_indices(
    model_name: &str,
    symbol: &str,
    factor_names: &[String],
    venue_factor_positions: &HashMap<String, usize>,
) -> Vec<usize> {
    let mut extract_indices = Vec::with_capacity(factor_names.len());
    for name in factor_names {
        let Some(position) = venue_factor_positions.get(name).copied() else {
            panic!(
                "model factor missing from venue factor plan: model_name={} symbol={} factor={}",
                model_name, symbol, name
            );
        };
        extract_indices.push(position);
    }
    extract_indices
}
