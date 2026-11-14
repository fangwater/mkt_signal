//! Fixed-fixture CPU benchmark; excludes RocksDB I/O and calendar certification.
use anyhow::Result;
use cme_tas_replay::daily_inventory::{add, Event};
use serde::Deserialize;
use std::{collections::BTreeMap, time::Instant};

fn main() -> Result<()> {
    #[derive(Deserialize)]
    struct Fixture {
        events: Vec<Event>,
    }
    let f: Fixture =
        serde_json::from_str(include_str!("../tests/fixtures/tas_daily_inventory.json"))?;
    let repeat: u64 = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "100000".to_string())
        .parse()?;
    let mut output = BTreeMap::new();
    let start = Instant::now();
    for _ in 0..repeat {
        for event in &f.events {
            add(&mut output, event)?;
        }
    }
    println!(
        "{}",
        serde_json::json!({"events":repeat*f.events.len() as u64,
        "elapsed_seconds":start.elapsed().as_secs_f64(),"rows":output.into_values().collect::<Vec<_>>()})
    );
    Ok(())
}
