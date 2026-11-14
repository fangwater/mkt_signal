//! Fixed-fixture replay CPU benchmark; excludes JSON parsing and file I/O.
use anyhow::Result;
use cme_tas_replay::causal_roll::{replay, Input};
use std::time::Instant;
fn main() -> Result<()> {
    let raw = match std::env::args().nth(2) {
        Some(path) => std::fs::read_to_string(path)?,
        None => include_str!("../tests/fixtures/causal_roll.json").to_string(),
    };
    let input: Input = serde_json::from_str(&raw)?;
    let repetitions: u64 = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "1000".to_string())
        .parse()?;
    let start = Instant::now();
    let mut output = Vec::new();
    for _ in 0..repetitions {
        output = replay(std::hint::black_box(&input))?;
    }
    println!(
        "{}",
        serde_json::json!({"repetitions":repetitions,"elapsed_seconds":start.elapsed().as_secs_f64(),"decisions":output})
    );
    Ok(())
}
