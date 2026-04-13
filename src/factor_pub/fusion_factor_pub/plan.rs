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
    Open,
    High,
    Low,
    Close,
    Volume,
    Amount,
    AvgAmount,
    Count,
    BuyCount,
    SellCount,
    BuyAmount,
    SellAmount,
    BuyVolume,
    SellVolume,
    LargeOrder,
    MediumOrder,
    SmallOrder,
    LargeBuy,
    LargeSell,
    MediumBuy,
    MediumSell,
    Vwap,
    BuyVwap,
    SellVwap,
    NetBuyAmount,
    NetBuyVolume,
    NetBuyPct,
    NetBuyMedium,
    NetBuySmall,
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
            "open" => Some(Self::Open),
            "high" => Some(Self::High),
            "low" => Some(Self::Low),
            "close" => Some(Self::Close),
            "volume" => Some(Self::Volume),
            "amount" => Some(Self::Amount),
            "avg_amount" => Some(Self::AvgAmount),
            "count" => Some(Self::Count),
            "buy_count" => Some(Self::BuyCount),
            "sell_count" => Some(Self::SellCount),
            "buy_amount" => Some(Self::BuyAmount),
            "sell_amount" => Some(Self::SellAmount),
            "buy_volume" => Some(Self::BuyVolume),
            "sell_volume" => Some(Self::SellVolume),
            "large_order" => Some(Self::LargeOrder),
            "medium_order" => Some(Self::MediumOrder),
            "small_order" => Some(Self::SmallOrder),
            "large_buy" => Some(Self::LargeBuy),
            "large_sell" => Some(Self::LargeSell),
            "medium_buy" => Some(Self::MediumBuy),
            "medium_sell" => Some(Self::MediumSell),
            "vwap" => Some(Self::Vwap),
            "buy_vwap" => Some(Self::BuyVwap),
            "sell_vwap" => Some(Self::SellVwap),
            "net_buy_amount" => Some(Self::NetBuyAmount),
            "net_buy_volume" => Some(Self::NetBuyVolume),
            "net_buy_pct" => Some(Self::NetBuyPct),
            "net_buy_medium" => Some(Self::NetBuyMedium),
            "net_buy_small" => Some(Self::NetBuySmall),
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
            Self::Open => EXTRA_FACTOR_BASE + 6,
            Self::High => EXTRA_FACTOR_BASE + 7,
            Self::Low => EXTRA_FACTOR_BASE + 8,
            Self::Close => EXTRA_FACTOR_BASE + 9,
            Self::Volume => EXTRA_FACTOR_BASE + 10,
            Self::Amount => EXTRA_FACTOR_BASE + 11,
            Self::AvgAmount => EXTRA_FACTOR_BASE + 12,
            Self::Count => EXTRA_FACTOR_BASE + 13,
            Self::BuyCount => EXTRA_FACTOR_BASE + 14,
            Self::SellCount => EXTRA_FACTOR_BASE + 15,
            Self::BuyAmount => EXTRA_FACTOR_BASE + 16,
            Self::SellAmount => EXTRA_FACTOR_BASE + 17,
            Self::BuyVolume => EXTRA_FACTOR_BASE + 18,
            Self::SellVolume => EXTRA_FACTOR_BASE + 19,
            Self::LargeOrder => EXTRA_FACTOR_BASE + 20,
            Self::MediumOrder => EXTRA_FACTOR_BASE + 21,
            Self::SmallOrder => EXTRA_FACTOR_BASE + 22,
            Self::LargeBuy => EXTRA_FACTOR_BASE + 23,
            Self::LargeSell => EXTRA_FACTOR_BASE + 24,
            Self::MediumBuy => EXTRA_FACTOR_BASE + 25,
            Self::MediumSell => EXTRA_FACTOR_BASE + 26,
            Self::Vwap => EXTRA_FACTOR_BASE + 27,
            Self::BuyVwap => EXTRA_FACTOR_BASE + 28,
            Self::SellVwap => EXTRA_FACTOR_BASE + 29,
            Self::NetBuyAmount => EXTRA_FACTOR_BASE + 30,
            Self::NetBuyVolume => EXTRA_FACTOR_BASE + 31,
            Self::NetBuyPct => EXTRA_FACTOR_BASE + 32,
            Self::NetBuyMedium => EXTRA_FACTOR_BASE + 33,
            Self::NetBuySmall => EXTRA_FACTOR_BASE + 34,
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
            x if x == EXTRA_FACTOR_BASE + 6 => Some(Self::Open),
            x if x == EXTRA_FACTOR_BASE + 7 => Some(Self::High),
            x if x == EXTRA_FACTOR_BASE + 8 => Some(Self::Low),
            x if x == EXTRA_FACTOR_BASE + 9 => Some(Self::Close),
            x if x == EXTRA_FACTOR_BASE + 10 => Some(Self::Volume),
            x if x == EXTRA_FACTOR_BASE + 11 => Some(Self::Amount),
            x if x == EXTRA_FACTOR_BASE + 12 => Some(Self::AvgAmount),
            x if x == EXTRA_FACTOR_BASE + 13 => Some(Self::Count),
            x if x == EXTRA_FACTOR_BASE + 14 => Some(Self::BuyCount),
            x if x == EXTRA_FACTOR_BASE + 15 => Some(Self::SellCount),
            x if x == EXTRA_FACTOR_BASE + 16 => Some(Self::BuyAmount),
            x if x == EXTRA_FACTOR_BASE + 17 => Some(Self::SellAmount),
            x if x == EXTRA_FACTOR_BASE + 18 => Some(Self::BuyVolume),
            x if x == EXTRA_FACTOR_BASE + 19 => Some(Self::SellVolume),
            x if x == EXTRA_FACTOR_BASE + 20 => Some(Self::LargeOrder),
            x if x == EXTRA_FACTOR_BASE + 21 => Some(Self::MediumOrder),
            x if x == EXTRA_FACTOR_BASE + 22 => Some(Self::SmallOrder),
            x if x == EXTRA_FACTOR_BASE + 23 => Some(Self::LargeBuy),
            x if x == EXTRA_FACTOR_BASE + 24 => Some(Self::LargeSell),
            x if x == EXTRA_FACTOR_BASE + 25 => Some(Self::MediumBuy),
            x if x == EXTRA_FACTOR_BASE + 26 => Some(Self::MediumSell),
            x if x == EXTRA_FACTOR_BASE + 27 => Some(Self::Vwap),
            x if x == EXTRA_FACTOR_BASE + 28 => Some(Self::BuyVwap),
            x if x == EXTRA_FACTOR_BASE + 29 => Some(Self::SellVwap),
            x if x == EXTRA_FACTOR_BASE + 30 => Some(Self::NetBuyAmount),
            x if x == EXTRA_FACTOR_BASE + 31 => Some(Self::NetBuyVolume),
            x if x == EXTRA_FACTOR_BASE + 32 => Some(Self::NetBuyPct),
            x if x == EXTRA_FACTOR_BASE + 33 => Some(Self::NetBuyMedium),
            x if x == EXTRA_FACTOR_BASE + 34 => Some(Self::NetBuySmall),
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
            Self::Open => "open",
            Self::High => "high",
            Self::Low => "low",
            Self::Close => "close",
            Self::Volume => "volume",
            Self::Amount => "amount",
            Self::AvgAmount => "avg_amount",
            Self::Count => "count",
            Self::BuyCount => "buy_count",
            Self::SellCount => "sell_count",
            Self::BuyAmount => "buy_amount",
            Self::SellAmount => "sell_amount",
            Self::BuyVolume => "buy_volume",
            Self::SellVolume => "sell_volume",
            Self::LargeOrder => "large_order",
            Self::MediumOrder => "medium_order",
            Self::SmallOrder => "small_order",
            Self::LargeBuy => "large_buy",
            Self::LargeSell => "large_sell",
            Self::MediumBuy => "medium_buy",
            Self::MediumSell => "medium_sell",
            Self::Vwap => "vwap",
            Self::BuyVwap => "buy_vwap",
            Self::SellVwap => "sell_vwap",
            Self::NetBuyAmount => "net_buy_amount",
            Self::NetBuyVolume => "net_buy_volume",
            Self::NetBuyPct => "net_buy_pct",
            Self::NetBuyMedium => "net_buy_medium",
            Self::NetBuySmall => "net_buy_small",
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

fn decode_symbol_factor_plans(
    payload: FactorPlanResp,
) -> Result<HashMap<String, SymbolFactorPlan>> {
    let mut plans = HashMap::with_capacity(payload.thresholds.len());
    for (symbol, item) in payload.thresholds {
        let key = symbol.trim();
        if key.is_empty() || key == TLEN_SHARED_CONFIG_FIELD {
            continue;
        }
        if item.factors.is_empty() {
            continue;
        }
        let normalized_symbol = key.to_uppercase();
        let plan = build_factor_plan(&normalized_symbol, item.factors);
        if plan.ordered_factors.is_empty() {
            continue;
        }
        plans.insert(normalized_symbol, plan);
    }
    Ok(plans)
}

pub(crate) async fn load_symbol_factor_plans_from_tlen_server(
    tlen: &TlenServerConfig,
    venue_slug: &str,
) -> Result<HashMap<String, SymbolFactorPlan>> {
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
    decode_symbol_factor_plans(payload).with_context(|| {
        format!(
            "decode symbol factor plans from tlen_server failed: venue={}",
            venue_slug
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::trade_flow_feature_msg::TRADE_FLOW_FEATURE_FIELD_NAMES;

    fn factor_item(factors: &[&str]) -> FactorPlanItem {
        FactorPlanItem {
            factors: factors.iter().map(|name| (*name).to_string()).collect(),
        }
    }

    #[test]
    fn decode_symbol_factor_plans_ignores_shared_and_empty_entries() {
        let payload = FactorPlanResp {
            thresholds: HashMap::from([
                (
                    "__shared__".to_string(),
                    factor_item(&["factor_001", "avg_price"]),
                ),
                ("BTCUSDT".to_string(), factor_item(&["factor_001", "avg_price"])),
                ("ETHUSDT".to_string(), factor_item(&[])),
            ]),
        };

        let plans = decode_symbol_factor_plans(payload).expect("decode symbol plans");

        assert_eq!(plans.len(), 1);
        let btc = plans.get("BTCUSDT").expect("btc plan");
        let factor_names: Vec<&str> = btc
            .ordered_factors
            .iter()
            .map(|binding| binding.name.as_str())
            .collect();
        assert_eq!(factor_names, vec!["factor_001", "avg_price"]);
        assert!(!plans.contains_key("__shared__"));
        assert!(!plans.contains_key("ETHUSDT"));
    }

    #[test]
    fn all_trade_flow_feature_fields_have_extra_factor_ids() {
        for name in TRADE_FLOW_FEATURE_FIELD_NAMES {
            assert!(
                ExtraFactorId::from_name(name).is_some(),
                "missing ExtraFactorId mapping for {}",
                name
            );
        }
    }
}
