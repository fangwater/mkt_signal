use anyhow::{Context, Result};
use cme_tas_replay::causal_volumes::{Accumulator, Request, Trade};
use std::{hint::black_box, time::Instant};

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let fixture: serde_json::Value =
        serde_json::from_slice(&std::fs::read(args.get(1).context("oracle input")?)?)?;
    let request: Request = serde_json::from_value(fixture["request"].clone())?;
    let mut events: Vec<Trade> = serde_json::from_value(fixture["events"].clone())?;
    events.sort_by(|a, b| {
        (&a.ric, a.published_ns, a.part, a.seq).cmp(&(&b.ric, b.published_ns, b.part, b.seq))
    });
    let rics: Vec<String> = serde_json::from_value(fixture["completed_rics"].clone())?;
    let run = || -> Result<_> {
        let mut acc = Accumulator::new(request.clone())?;
        for event in &events {
            acc.add(black_box(event))?;
        }
        for ric in &rics {
            acc.finish_ric(ric)?;
        }
        Ok(acc.finish())
    };
    anyhow::ensure!(
        serde_json::to_value(run()?)? == fixture["expected"],
        "Python parity failed"
    );
    let repeats: usize = args.get(2).context("repeats")?.parse()?;
    let start = Instant::now();
    for _ in 0..repeats {
        black_box(run()?);
    }
    println!(
        "{}",
        serde_json::json!({"repeats":repeats,"input_rows":events.len(),"seconds":start.elapsed().as_secs_f64(),"python_expected_matches":true})
    );
    Ok(())
}
