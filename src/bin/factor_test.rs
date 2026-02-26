//! Factor test binary: generates synthetic data, computes all factors,
//! outputs JSON for cross-validation with Python.

use anyhow::Result;
use clap::Parser;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::fs;

use mkt_signal::common::trade_flow_feature_msg::TRADE_FLOW_FEATURE_FIELD_NAMES;
use mkt_signal::factor_pub::factor_test::runner::run_scenario;
use mkt_signal::factor_pub::factor_test::synthetic::generate_all_scenarios;

#[derive(Parser)]
#[command(name = "factor_test")]
struct Args {
    /// Output JSON file path
    #[arg(long, default_value = "/tmp/factor_test_output.json")]
    output: String,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let scenarios = generate_all_scenarios();
    let mut scenarios_json = Map::new();

    for scenario in &scenarios {
        let result = run_scenario(scenario)?;

        // Build input data for Python to replay
        let input = build_input_json(scenario);

        // Factor results
        let kline_factors = factor_map_to_json(&result.kline_factors);
        let fusion_factors = factor_map_to_json(&result.fusion_factors);

        scenarios_json.insert(
            scenario.name.clone(),
            json!({
                "input": input,
                "kline_factors": kline_factors,
                "fusion_factors": fusion_factors,
            }),
        );

        let kline_ready = result
            .kline_factors
            .values()
            .filter(|v| v.is_finite())
            .count();
        let fusion_ready = result
            .fusion_factors
            .values()
            .filter(|v| v.is_finite())
            .count();
        eprintln!(
            "scenario={} bars={} kline_factors={}/{} fusion_factors={}/{}",
            scenario.name,
            scenario.trade_flow_msgs.len(),
            kline_ready,
            result.kline_factors.len(),
            fusion_ready,
            result.fusion_factors.len(),
        );
    }

    let output = json!({ "scenarios": scenarios_json });
    let json_str = serde_json::to_string_pretty(&output)?;
    fs::write(&args.output, &json_str)?;
    eprintln!("wrote {}", args.output);

    Ok(())
}

fn build_input_json(
    scenario: &mkt_signal::factor_pub::factor_test::synthetic::ScenarioData,
) -> Value {
    // Trade flow fields as named arrays
    let num_bars = scenario.trade_flow_msgs.len();
    let mut field_arrays: Vec<Vec<f64>> = vec![Vec::with_capacity(num_bars); 32];

    for msg in &scenario.trade_flow_msgs {
        for (idx, arr) in field_arrays.iter_mut().enumerate() {
            arr.push(msg.values[idx]);
        }
    }

    let mut input = Map::new();
    for (idx, name) in TRADE_FLOW_FEATURE_FIELD_NAMES.iter().enumerate() {
        input.insert(name.to_string(), json!(field_arrays[idx]));
    }

    // Depth snapshots
    let mut depth_bids: Vec<Vec<[f64; 2]>> = Vec::with_capacity(num_bars);
    let mut depth_asks: Vec<Vec<[f64; 2]>> = Vec::with_capacity(num_bars);
    for depth in &scenario.depth_snapshots {
        let bids: Vec<[f64; 2]> = depth.bids.iter().map(|l| [l.price, l.amount]).collect();
        let asks: Vec<[f64; 2]> = depth.asks.iter().map(|l| [l.price, l.amount]).collect();
        depth_bids.push(bids);
        depth_asks.push(asks);
    }
    input.insert("depth_bids".to_string(), json!(depth_bids));
    input.insert("depth_asks".to_string(), json!(depth_asks));

    Value::Object(input)
}

fn factor_map_to_json(map: &HashMap<String, f64>) -> Value {
    let mut obj = Map::new();
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();
    for key in keys {
        let v = map[key];
        if v.is_finite() {
            obj.insert(key.clone(), json!(v));
        } else {
            obj.insert(key.clone(), Value::Null);
        }
    }
    Value::Object(obj)
}
