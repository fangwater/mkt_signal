//! Read factual factor signals from the CTA worker over the local observation tunnel.
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize)]
struct Worker {
    #[serde(flatten)]
    primary: Product,
    #[serde(default)]
    products: BTreeMap<String, Product>,
}

#[derive(Deserialize)]
struct Product {
    strategy_group: String,
    manager_publication: Option<Publication>,
    factor_signals: Option<FactorSignals>,
}

#[derive(Deserialize)]
struct Publication {
    source_id: String,
}

#[derive(Deserialize, Serialize)]
pub struct FactorSignals {
    market: Option<String>,
    decision_ms: i64,
    rules: Vec<FactorSignal>,
}

#[derive(Deserialize, Serialize)]
struct FactorSignal {
    factor: String,
    id: Option<String>,
    target_lots: Option<i64>,
}

pub async fn factor_signals(
    client: &reqwest::Client,
    port: u16,
    source: &str,
    groups: &BTreeSet<&str>,
) -> Result<BTreeMap<String, (String, FactorSignals)>> {
    let response = client
        .get(format!("http://127.0.0.1:{port}/state"))
        .send()
        .await?;
    let worker: Worker = super::manager::decode_json(response)
        .await
        .context("read CTA product signals")?;
    Ok(std::iter::once(worker.primary)
        .chain(worker.products.into_values())
        .filter_map(|product| {
            if !groups.contains(product.strategy_group.as_str())
                || product.manager_publication?.source_id != source
            {
                return None;
            }
            let signals = product.factor_signals?;
            let market = signals.market.clone()?;
            Some((market, (product.strategy_group, signals)))
        })
        .collect())
}
