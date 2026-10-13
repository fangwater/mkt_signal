use anyhow::{ensure, Context, Result};
use order_common::TradingVenue;
use runtime_common::symbol_util::normalize_symbol_for_venue;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

use super::fusion_factor_pub::app::{load_online_symbols_from_tlen_server, BaselineReplayState};
use super::fusion_factor_pub::cfg::TlenServerConfig;
use super::fusion_factor_pub::plan::load_symbol_factor_plans_from_tlen_server;
use super::fusion_factor_pub::SymbolFactorPlan;

/// Explicit calculation scope; trading environments select their own subscriptions.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CtaFactorUniverse {
    pub symbols: Vec<String>,
    pub factors: Vec<String>,
}

impl CtaFactorUniverse {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.symbols.is_empty(),
            "universe.symbols must not be empty"
        );
        ensure!(
            !self.factors.is_empty(),
            "universe.factors must not be empty"
        );
        let mut symbols = HashSet::new();
        for symbol in &self.symbols {
            ensure!(
                !symbol.is_empty() && symbol.trim() == symbol,
                "invalid universe symbol: {symbol:?}"
            );
            ensure!(
                symbols.insert(symbol.to_ascii_uppercase()),
                "duplicate universe symbol: {symbol}"
            );
        }
        let mut services = HashSet::new();
        for factor in &self.factors {
            ensure!(
                services.insert(factor.to_ascii_lowercase()),
                "duplicate universe factor output: {factor}"
            );
        }
        let plan = SymbolFactorPlan::from_factor_names("CTA universe", self.factors.clone())?;
        BaselineReplayState::validate_factor_plan(&plan).context("validate CTA universe factors")
    }

    pub fn require_complete(&self, enabled: &HashSet<String>, venue: TradingVenue) -> Result<()> {
        let missing: Vec<&str> = self
            .symbols
            .iter()
            .filter(|symbol| !enabled.contains(&normalize_symbol_for_venue(symbol, venue)))
            .map(String::as_str)
            .collect();
        ensure!(
            missing.is_empty(),
            "CTA universe symbols missing online amount_thresholds_1m or required factor_plan_1m: {missing:?}"
        );
        Ok(())
    }
}

pub(crate) async fn load_enabled_symbols(
    tlen: &TlenServerConfig,
    venue: TradingVenue,
    venue_slug: &str,
    universe: &CtaFactorUniverse,
) -> Result<HashSet<String>> {
    let online =
        load_online_symbols_from_tlen_server(tlen, venue, venue_slug, "amount_thresholds_1m")
            .await?;
    let plans =
        load_symbol_factor_plans_from_tlen_server(tlen, venue_slug, "factor_plan_1m").await?;
    Ok(select_enabled_symbols(venue, &online, &plans, universe))
}

fn select_enabled_symbols(
    venue: TradingVenue,
    online: &HashSet<String>,
    plans: &HashMap<String, SymbolFactorPlan>,
    universe: &CtaFactorUniverse,
) -> HashSet<String> {
    universe
        .symbols
        .iter()
        .map(|symbol| normalize_symbol_for_venue(symbol, venue))
        .filter(|symbol| {
            online.contains(symbol)
                && plans.get(symbol).is_some_and(|plan| {
                    let names: HashSet<&str> = plan.factor_names().collect();
                    universe
                        .factors
                        .iter()
                        .all(|name| names.contains(name.as_str()))
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn universe() -> CtaFactorUniverse {
        CtaFactorUniverse {
            symbols: vec!["FILUSDT".into(), "ETHUSDT".into()],
            factors: vec!["factor_036".into(), "TD_TI_002".into()],
        }
    }

    #[test]
    fn only_configured_online_symbols_with_complete_plans_are_enabled() {
        let config = universe();
        let complete =
            SymbolFactorPlan::from_factor_names("complete", config.factors.clone()).unwrap();
        let incomplete =
            SymbolFactorPlan::from_factor_names("incomplete", vec!["factor_036".into()]).unwrap();
        let online = HashSet::from(["FILUSDT".into(), "ETHUSDT".into(), "BTCUSDT".into()]);
        let plans = HashMap::from([
            ("FILUSDT".into(), complete.clone()),
            ("ETHUSDT".into(), incomplete),
            ("BTCUSDT".into(), complete),
        ]);
        let enabled =
            select_enabled_symbols(TradingVenue::BinanceFutures, &online, &plans, &config);
        assert_eq!(enabled, HashSet::from(["FILUSDT".into()]));
        assert!(config
            .require_complete(&enabled, TradingVenue::BinanceFutures)
            .is_err());
        assert!(select_enabled_symbols(
            TradingVenue::BinanceFutures,
            &HashSet::new(),
            &plans,
            &config
        )
        .is_empty());
    }

    #[test]
    fn invalid_or_duplicate_entries_are_rejected() {
        let mut config = universe();
        config.validate().unwrap();
        config.symbols.push("filusdt".into());
        assert!(config.validate().is_err());
        config = universe();
        config.factors.push("factor_036".into());
        assert!(config.validate().is_err());
        config = universe();
        config.factors = vec!["not_a_factor".into()];
        assert!(config.validate().is_err());
    }

    #[test]
    fn configured_factors_match_raw_formulas_after_warmup() {
        use mkt_parsers::msg::trade_flow_feature_msg::{
            TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
        };

        let config: super::super::cta_perp_factor_model_1m_pub::cfg::CtaPerpFactorPubConfig =
            toml::from_str(include_str!(
                "../../config/cta_perp_factor_model_1m_pub.toml"
            ))
            .unwrap();
        config.universe.validate().unwrap();
        let plan =
            SymbolFactorPlan::from_factor_names("test", config.universe.factors.clone()).unwrap();
        let mut evaluator = BaselineReplayState::default();
        let mut raw = Vec::new();
        for bar in 0..500 {
            let mid = 100.0 + bar as f64 * 0.01;
            let mut values = vec![1.0; TRADE_FLOW_FEATURE_DIM];
            for (field, value) in [
                ("open", mid),
                ("high", mid + 1.0),
                ("low", mid - 1.0),
                ("close", mid),
                ("vwap", mid),
                ("buy_vwap", mid - 0.03),
                ("sell_vwap", mid + 0.03),
            ] {
                values[TRADE_FLOW_FEATURE_FIELD_NAMES
                    .iter()
                    .position(|name| *name == field)
                    .unwrap()] = value;
            }
            for level in 0..20 {
                values.extend([mid - 0.01 - level as f64 * 0.01, 10.0]);
            }
            for level in 0..20 {
                values.extend([mid + 0.01 + level as f64 * 0.01, 10.0]);
            }
            evaluator
                .push(
                    TradeFlowFeatureMsg::from_indexed_values(
                        "FILUSDT".into(),
                        TradingVenue::BinanceFutures.to_u8(),
                        1_704_067_200_000 + bar * 60_000,
                        &values,
                    )
                    .unwrap(),
                )
                .unwrap();
            raw = evaluator.factor_values(&plan);
        }
        // Closed-form expectations for a linear price path and a 20-level book.
        let expected = HashMap::from([
            ("factor_036", 149.5 / (300.0_f64 * 301.0 / 12.0).sqrt()),
            ("factor_051", 105.02 / 103.525),
            ("factor_170", 104.99 / 103.79 - 1.0),
            ("factor_004", 0.03),
            ("TD_TI_002", 102.495),
            // Six successive 30-bar means each lag this linear path by 0.145.
            ("baseline_004", -3.0 * 104.99 + 21.0 * 0.145),
        ]);
        assert_eq!(raw.len(), config.universe.factors.len());
        for (name, actual) in config.universe.factors.iter().zip(raw) {
            assert!(
                (actual - expected[name.as_str()]).abs() < 1e-6,
                "{name}: {actual}"
            );
        }
    }
}
