use anyhow::{ensure, Context, Result};
use cme_tas_replay::session_status::{project, Event};
use std::{hint::black_box, time::Instant};

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let cases: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(args.get(1).context("cases")?)?)?;
    let repeats: usize = args.get(2).context("repeats")?.parse()?;
    let mut checked = 0;
    for case in &cases {
        let result = (|| -> Result<_> {
            project(
                &serde_json::from_value::<Vec<(u64, u64)>>(case["planned"].clone())?,
                &serde_json::from_value::<Vec<Event>>(case["events"].clone())?,
                case["product"].as_str().context("product")?,
                case["ric"].as_str().context("ric")?,
            )
        })();
        if case["valid"] == true {
            ensure!(
                serde_json::to_value(result?)? == case["expected"],
                "fixture mismatch"
            );
        } else {
            ensure!(result.is_err(), "invalid fixture accepted");
        }
        checked += 1;
    }
    let first = &cases[0];
    let planned = serde_json::from_value::<Vec<(u64, u64)>>(first["planned"].clone())?;
    let events = serde_json::from_value::<Vec<Event>>(first["events"].clone())?;
    let started = Instant::now();
    for _ in 0..repeats {
        black_box(project(
            black_box(&planned),
            black_box(&events),
            first["product"].as_str().unwrap(),
            first["ric"].as_str().unwrap(),
        )?);
    }
    println!(
        "{}",
        serde_json::json!({"checked_cases":checked,"repeats":repeats,"seconds":started.elapsed().as_secs_f64()})
    );
    Ok(())
}
