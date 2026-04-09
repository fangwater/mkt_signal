use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

use super::cfg::TlenServerConfig;
use super::factor_enum::FusionFactorId;

const TLEN_SHARED_CONFIG_FIELD: &str = "__shared__";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SymbolFactorPlan {
    pub(crate) ordered_factors: Vec<FactorBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FactorBinding {
    pub(crate) name: String,
    pub(crate) factor_id: Option<FusionFactorId>,
    pub(crate) extra_factor_id: Option<ExtraFactorId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtraFactorId {
    AvgPrice,
    BuyAvgPrice,
    SellAvgPrice,
    SmallBuy,
    SmallSell,
    NetBuyLarge,
}

impl ExtraFactorId {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "avg_price" => Some(Self::AvgPrice),
            "buy_avg_price" => Some(Self::BuyAvgPrice),
            "sell_avg_price" => Some(Self::SellAvgPrice),
            "small_buy" => Some(Self::SmallBuy),
            "small_sell" => Some(Self::SmallSell),
            "net_buy_large" => Some(Self::NetBuyLarge),
            _ => None,
        }
    }

    pub const fn as_index(self) -> u16 {
        const EXTRA_FACTOR_BASE: u16 = 1000;
        match self {
            Self::AvgPrice => EXTRA_FACTOR_BASE,
            Self::BuyAvgPrice => EXTRA_FACTOR_BASE + 1,
            Self::SellAvgPrice => EXTRA_FACTOR_BASE + 2,
            Self::SmallBuy => EXTRA_FACTOR_BASE + 3,
            Self::SmallSell => EXTRA_FACTOR_BASE + 4,
            Self::NetBuyLarge => EXTRA_FACTOR_BASE + 5,
        }
    }

    pub fn from_index(index: u16) -> Option<Self> {
        const EXTRA_FACTOR_BASE: u16 = 1000;
        match index {
            x if x == EXTRA_FACTOR_BASE => Some(Self::AvgPrice),
            x if x == EXTRA_FACTOR_BASE + 1 => Some(Self::BuyAvgPrice),
            x if x == EXTRA_FACTOR_BASE + 2 => Some(Self::SellAvgPrice),
            x if x == EXTRA_FACTOR_BASE + 3 => Some(Self::SmallBuy),
            x if x == EXTRA_FACTOR_BASE + 4 => Some(Self::SmallSell),
            x if x == EXTRA_FACTOR_BASE + 5 => Some(Self::NetBuyLarge),
            _ => None,
        }
    }

    pub fn index_to_name(index: u16) -> Option<&'static str> {
        Self::from_index(index).map(|id| match id {
            Self::AvgPrice => "avg_price",
            Self::BuyAvgPrice => "buy_avg_price",
            Self::SellAvgPrice => "sell_avg_price",
            Self::SmallBuy => "small_buy",
            Self::SmallSell => "small_sell",
            Self::NetBuyLarge => "net_buy_large",
        })
    }
}

#[derive(Debug, Deserialize)]
struct FactorPlanResp {
    thresholds: HashMap<String, FactorPlanItem>,
}

#[derive(Debug, Clone, Deserialize)]
struct FactorPlanItem {
    #[serde(default)]
    factors: Vec<String>,
}

fn build_factor_plan(scope: &str, factors: Vec<String>) -> SymbolFactorPlan {
    let mut ordered_factors = Vec::with_capacity(factors.len());
    for name in factors {
        let factor_id = FusionFactorId::from_name(&name);
        let extra_factor_id = ExtraFactorId::from_name(&name);
        if factor_id.is_none() && extra_factor_id.is_none() {
            panic!("factor-plan unmapped: {} {}", scope, name);
        }
        ordered_factors.push(FactorBinding {
            name,
            factor_id,
            extra_factor_id,
        });
    }

    SymbolFactorPlan { ordered_factors }
}

pub(crate) async fn load_venue_factor_plan_from_tlen_server(
    tlen: &TlenServerConfig,
    venue_slug: &str,
) -> Result<SymbolFactorPlan> {
    let base_url = tlen.base_url.trim_end_matches('/');
    let client = Client::builder()
        .timeout(Duration::from_millis(tlen.request_timeout_ms))
        .build()
        .context("build reqwest client for tlen_server failed")?;

    let url = format!("{}/api/thresholds", base_url);
    let resp = client
        .get(&url)
        .query(&[("venue", venue_slug), ("config_type", "factor_plan")])
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .error_for_status()
        .with_context(|| format!("GET {} returned error status", url))?;

    let payload: FactorPlanResp = resp
        .json()
        .await
        .with_context(|| format!("decode factor plan response failed: {}", url))?;

    let shared = payload
        .thresholds
        .get(TLEN_SHARED_CONFIG_FIELD)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("tlen_server returned no shared factor plan"))?;
    let plan = build_factor_plan(venue_slug, shared.factors);
    if plan.ordered_factors.is_empty() {
        anyhow::bail!("shared factor plan must not be empty: venue={}", venue_slug);
    }
    Ok(plan)
}
