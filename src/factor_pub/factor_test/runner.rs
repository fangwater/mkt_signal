//! Factor computation runner for testing.
//!
//! Feeds synthetic data through the real factor computation pipeline
//! and collects final factor values for cross-validation with Python.

use std::collections::{HashMap, VecDeque};

use anyhow::Result;

use crate::factor_pub::fusion_factor_pub::app::{
    DepthDerived, ExtraFactorId, FactorBinding, FusionFactorPubApp, SymbolCalcState,
};
use crate::factor_pub::fusion_factor_pub::FusionFactorId;
use crate::factor_pub::kline_factor_pub::app as kline_app;
use crate::factor_pub::kline_factor_pub::cfg::{FactorDefinition, FactorKind};

use super::synthetic::ScenarioData;

pub struct ScenarioResult {
    pub name: String,
    pub kline_factors: HashMap<String, f64>,
    pub fusion_factors: HashMap<String, f64>,
}

/// Run all factors for a single scenario.
pub fn run_scenario(scenario: &ScenarioData) -> Result<ScenarioResult> {
    let kline_factors = run_kline_factors(scenario)?;
    let fusion_factors = run_fusion_factors(scenario)?;

    Ok(ScenarioResult {
        name: scenario.name.clone(),
        kline_factors,
        fusion_factors,
    })
}

/// Feed all bars into kline VecDeques, then compute each FactorKind with default params.
fn run_kline_factors(scenario: &ScenarioData) -> Result<HashMap<String, f64>> {
    let mut closes = VecDeque::new();
    let mut highs = VecDeque::new();
    let mut lows = VecDeque::new();
    let mut volumes = VecDeque::new();
    let mut counts = VecDeque::new();

    for msg in &scenario.trade_flow_msgs {
        closes.push_back(msg.values[3]); // close
        highs.push_back(msg.values[1]); // high
        lows.push_back(msg.values[2]); // low
        volumes.push_back(msg.values[4]); // volume
        counts.push_back(msg.values[7]); // count
    }

    let factor_defs = default_kline_factor_defs();
    let mut results = HashMap::new();

    for def in &factor_defs {
        let value =
            kline_app::compute_factor_value(def, &closes, &highs, &lows, &volumes, &counts)?;
        let v = match value {
            Some(v) if v.is_finite() => v,
            _ => f64::NAN,
        };
        results.insert(def.name.clone(), v);
    }

    Ok(results)
}

/// Feed all bars into SymbolCalcState (simulating bootstrap), then compute all fusion factors.
fn run_fusion_factors(scenario: &ScenarioData) -> Result<HashMap<String, f64>> {
    let mut state = SymbolCalcState::default();

    // Feed all bars sequentially (simulating real bootstrap)
    for (msg, depth) in scenario
        .trade_flow_msgs
        .iter()
        .zip(scenario.depth_snapshots.iter())
    {
        state.push_trade_flow(msg);
        let depth_derived = DepthDerived::from_snapshot(depth);
        state.push_depth_metrics_derived(&depth_derived);
    }

    let series = FusionFactorPubApp::build_symbol_series_from_state(&mut state);
    let last_depth = scenario
        .depth_snapshots
        .last()
        .map(DepthDerived::from_snapshot);

    // Build bindings for all FusionFactorId variants
    let mut results = HashMap::new();

    for &factor_id in FusionFactorId::ALL.iter() {
        let binding = FactorBinding {
            name: factor_id.as_name().to_string(),
            factor_id: Some(factor_id),
            extra_factor_id: None,
        };

        let result = FusionFactorPubApp::compute_supported_factor(
            &binding,
            None, // factor_118_result: skip factor_118 (requires special state)
            last_depth.as_ref(),
            Some(&series),
        );

        let v = match result {
            Some((val, true, _)) if val.is_finite() => val,
            Some((val, _, _)) if val.is_finite() => val,
            _ => f64::NAN,
        };
        results.insert(factor_id.as_name().to_string(), v);
    }

    // Also compute ExtraFactorId variants
    let extra_names = [
        "avg_price",
        "buy_avg_price",
        "sell_avg_price",
        "small_buy",
        "small_sell",
        "net_buy_large",
    ];
    for name in &extra_names {
        if let Some(extra_id) = ExtraFactorId::from_name(name) {
            let binding = FactorBinding {
                name: name.to_string(),
                factor_id: None,
                extra_factor_id: Some(extra_id),
            };
            let result = FusionFactorPubApp::compute_supported_factor(
                &binding,
                None,
                last_depth.as_ref(),
                Some(&series),
            );
            let v = match result {
                Some((val, _, _)) if val.is_finite() => val,
                _ => f64::NAN,
            };
            results.insert(name.to_string(), v);
        }
    }

    Ok(results)
}

/// Default kline factor definitions covering all 9 FactorKind variants.
fn default_kline_factor_defs() -> Vec<FactorDefinition> {
    vec![
        FactorDefinition {
            name: "rl_return_volatility".to_string(),
            enabled: true,
            kind: FactorKind::RlReturnVolatility {
                pct_change_period: 1,
                rolling_window: 60,
            },
            scale_factor: 1.0,
            clip: None,
        },
        FactorDefinition {
            name: "hf_vol_std".to_string(),
            enabled: true,
            kind: FactorKind::HfVolStd { window: 60 },
            scale_factor: 1.0,
            clip: None,
        },
        FactorDefinition {
            name: "hf_highlow_range".to_string(),
            enabled: true,
            kind: FactorKind::HfHighlowRange { window: 60 },
            scale_factor: 1.0,
            clip: None,
        },
        FactorDefinition {
            name: "hf_spread_return".to_string(),
            enabled: true,
            kind: FactorKind::HfSpreadReturn {
                return_period: 1,
                ma_window: 60,
            },
            scale_factor: 1.0,
            clip: None,
        },
        FactorDefinition {
            name: "hf_count_mean".to_string(),
            enabled: true,
            kind: FactorKind::HfCountMean { window: 60 },
            scale_factor: 1.0,
            clip: None,
        },
        FactorDefinition {
            name: "hf_vol_abs_pct_by_vol".to_string(),
            enabled: true,
            kind: FactorKind::HfVolAbsPctByVol { window: 60 },
            scale_factor: 1.0,
            clip: None,
        },
        FactorDefinition {
            name: "hf_volume_mean".to_string(),
            enabled: true,
            kind: FactorKind::HfVolumeMean { window: 60 },
            scale_factor: 1.0,
            clip: None,
        },
        FactorDefinition {
            name: "hf_price_volume_corr".to_string(),
            enabled: true,
            kind: FactorKind::HfPriceVolumeCorr { window: 60 },
            scale_factor: 1.0,
            clip: None,
        },
        FactorDefinition {
            name: "hf_vol_volume_combined".to_string(),
            enabled: true,
            kind: FactorKind::HfVolVolumeCombined {
                vol_window: 60,
                volu_window: 60,
            },
            scale_factor: 1.0,
            clip: None,
        },
    ]
}
