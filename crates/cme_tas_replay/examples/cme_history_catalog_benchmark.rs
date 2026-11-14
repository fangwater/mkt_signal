use anyhow::{Context, Result};
use cme_tas_replay::history_catalog::Catalog;
use std::{hint::black_box, time::Instant};

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let cases: serde_json::Value =
        serde_json::from_slice(&std::fs::read(args.get(1).context("fixture path")?)?)?;
    let case = &cases[0];
    let catalog: Catalog = serde_json::from_value(case["catalog"].clone())?;
    let products = serde_json::from_value::<Vec<String>>(case["products"].clone())?;
    let start = case["start"].as_str().context("start")?.parse()?;
    let end = case["end"].as_str().context("end")?.parse()?;
    let repeats: usize = args.get(2).context("repeats")?.parse()?;
    let first = catalog.project(&products, start, end)?;
    // The multiplier contract is f64. JSON metadata can spell an exact nominal
    // multiplier as 1000 or 1000.0; serde_json::Number distinguishes those tokens.
    // Normalize only this typed floating field, retaining exact integer checks
    // for all timestamps and strict comparison of every other field.
    let mut expected = case["expected"].clone();
    for day in expected.as_array_mut().context("expected days")? {
        day["volume_multiple"] = serde_json::json!(day["volume_multiple"]
            .as_f64()
            .context("expected multiplier")?);
    }
    anyhow::ensure!(serde_json::to_value(&first)? == expected, "Python parity");
    let before = Instant::now();
    for _ in 0..repeats {
        black_box(catalog.project(black_box(&products), start, end)?);
    }
    println!(
        "{}",
        serde_json::json!({"repeats":repeats,"projected_days_per_repeat":first.len(),"seconds":before.elapsed().as_secs_f64(),"python_expected_matches":true})
    );
    Ok(())
}
