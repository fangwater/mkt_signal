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

pub(crate) async fn load_symbol_factor_names_from_tlen_server(
    config: &ModelPubConfig,
    venue_slug: &str,
) -> Result<HashMap<String, Vec<String>>> {
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
        .with_context(|| format!("decode symbol factor plans failed: {}", url))?;
    let mut plans = HashMap::with_capacity(payload.thresholds.len());
    for (symbol, item) in payload.thresholds {
        let key = symbol.trim();
        if key.is_empty() || key == TLEN_SHARED_CONFIG_FIELD || item.factors.is_empty() {
            continue;
        }
        let venue_symbol = key.to_uppercase();
        validate_factor_names(&venue_symbol, &item.factors)?;
        insert_symbol_plan_alias(&mut plans, &venue_symbol, &item.factors)?;

        let model_symbol = normalize_symbol_key(&venue_symbol);
        if model_symbol != venue_symbol {
            insert_symbol_plan_alias(&mut plans, &model_symbol, &item.factors)?;
        }
    }
    Ok(plans)
}

pub(crate) fn normalize_symbol_key(raw: &str) -> String {
    let normalized = raw
        .trim()
        .to_uppercase()
        .chars()
        .filter(|ch| !matches!(ch, '-' | '_' | '/' | ':'))
        .collect::<String>();

    for suffix in ["PERPETUAL", "SWAP", "PERP"] {
        if normalized.len() > suffix.len() && normalized.ends_with(suffix) {
            return normalized[..normalized.len() - suffix.len()].to_string();
        }
    }

    normalized
}

fn insert_symbol_plan_alias(
    plans: &mut HashMap<String, Vec<String>>,
    symbol: &str,
    factors: &[String],
) -> Result<()> {
    if symbol.trim().is_empty() {
        return Ok(());
    }
    if let Some(existing) = plans.get(symbol) {
        if existing != factors {
            anyhow::bail!(
                "conflicting factor plan aliases for symbol={} existing={} incoming={}",
                symbol,
                existing.len(),
                factors.len()
            );
        }
        return Ok(());
    }
    plans.insert(symbol.to_string(), factors.to_vec());
    Ok(())
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

pub(crate) fn build_factor_position_map(
    symbol: &str,
    factor_names: &[String],
) -> Result<HashMap<String, usize>> {
    let mut positions = HashMap::with_capacity(factor_names.len());
    for (idx, name) in factor_names.iter().enumerate() {
        let key = name.trim();
        if key.is_empty() {
            anyhow::bail!(
                "symbol factor plan contains empty factor name: symbol={} index={}",
                symbol,
                idx
            );
        }
        if positions.insert(key.to_string(), idx).is_some() {
            anyhow::bail!(
                "duplicate factor in symbol factor plan: symbol={} factor={}",
                symbol,
                key
            );
        }
    }
    Ok(positions)
}

pub(crate) fn build_extract_indices(
    model_name: &str,
    symbol: &str,
    factor_names: &[String],
    symbol_factor_positions: &HashMap<String, usize>,
) -> Vec<usize> {
    let mut extract_indices = Vec::with_capacity(factor_names.len());
    for name in factor_names {
        let Some(position) = symbol_factor_positions.get(name).copied() else {
            panic!(
                "model factor missing from symbol factor plan: model_name={} symbol={} factor={}",
                model_name, symbol, name
            );
        };
        extract_indices.push(position);
    }
    extract_indices
}

fn validate_factor_names(symbol: &str, factor_names: &[String]) -> Result<()> {
    let _ = build_factor_position_map(symbol, factor_names)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_factor_position_map_rejects_duplicates() {
        let err = build_factor_position_map(
            "BTCUSDT",
            &["factor_001".to_string(), "factor_001".to_string()],
        )
        .expect_err("duplicate factor should fail");
        assert!(err.to_string().contains("duplicate factor"));
    }

    #[test]
    fn normalize_symbol_key_handles_okex_swap_symbols() {
        assert_eq!(normalize_symbol_key("BTCUSDT"), "BTCUSDT");
        assert_eq!(normalize_symbol_key("BTC-USDT-SWAP"), "BTCUSDT");
        assert_eq!(normalize_symbol_key("bnb-usdt-swap"), "BNBUSDT");
        assert_eq!(normalize_symbol_key("BTC_USDT_PERP"), "BTCUSDT");
        assert_eq!(normalize_symbol_key("ETH/USDT:SWAP"), "ETHUSDT");
    }
}
