//! Forward-only minute status overlay. Oracle: scripts/cme_session_status.py.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

pub const MINUTE_NS: u64 = 60_000_000_000;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Open,
    Halted,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Event {
    pub product: String,
    pub ric: Option<String>,
    pub known_at_ns: u64,
    pub effective_ns: u64,
    pub state: State,
    pub source: String,
}

pub fn validate(events: &[Event]) -> Result<()> {
    for event in events {
        ensure!(!event.product.trim().is_empty(), "missing status product");
        ensure!(
            event.ric.as_ref().is_none_or(|s| !s.trim().is_empty()),
            "invalid RIC scope"
        );
        ensure!(!event.source.trim().is_empty(), "missing status source");
        ensure!(
            event.known_at_ns <= event.effective_ns
                && event.effective_ns < i64::MAX as u64 + 1 - MINUTE_NS,
            "status cannot backdate knowledge or overflow timestamps"
        );
    }
    Ok(())
}

/// Project half-open nanosecond plans onto the causal minute grid. An immediate
/// halt applies at the next minute boundary; already started minutes survive.
pub fn project(
    planned: &[(u64, u64)],
    events: &[Event],
    product: &str,
    ric: &str,
) -> Result<Vec<(u64, u64)>> {
    validate(events)?;
    let mut last = None;
    for &(lo, hi) in planned {
        ensure!(
            lo < hi
                && hi <= i64::MAX as u64
                && lo % MINUTE_NS == 0
                && hi % MINUTE_NS == 0
                && last.is_none_or(|end| lo >= end),
            "invalid planned segment"
        );
        last = Some(hi);
    }
    let mut matching = events
        .iter()
        .filter(|e| e.product == product && e.ric.as_ref().is_none_or(|s| s == ric))
        .collect::<Vec<_>>();
    matching.sort_by_key(|e| (e.effective_ns, e.known_at_ns));
    for pair in matching.windows(2) {
        ensure!(
            (pair[0].effective_ns, pair[0].known_at_ns)
                != (pair[1].effective_ns, pair[1].known_at_ns)
                || pair[0].state == pair[1].state,
            "ambiguous simultaneous status"
        );
    }
    let mut result = Vec::new();
    for &(lo, hi) in planned {
        let mut boundaries = vec![lo, hi];
        for event in &matching {
            let at = event.effective_ns.div_ceil(MINUTE_NS) * MINUTE_NS;
            if lo < at && at < hi {
                boundaries.push(at);
            }
        }
        boundaries.sort_unstable();
        boundaries.dedup();
        let mut local: Vec<(u64, u64)> = Vec::new();
        for pair in boundaries.windows(2) {
            let state = matching
                .iter()
                .rev()
                .find(|e| e.effective_ns <= pair[0])
                .map(|e| &e.state);
            if state == Some(&State::Halted) {
                continue;
            }
            if let Some(last) = local.last_mut().filter(|last| last.1 == pair[0]) {
                last.1 = pair[1];
            } else {
                local.push((pair[0], pair[1]));
            }
        }
        result.extend(local);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn minutes(spans: &[(u64, u64)], cutoff: u64) -> BTreeSet<u64> {
        spans
            .iter()
            .flat_map(|&(lo, hi)| (lo..hi).step_by(MINUTE_NS as usize))
            .filter(|t| t + MINUTE_NS <= cutoff)
            .collect()
    }

    #[test]
    fn python_fixtures_and_every_closed_prefix_match() -> Result<()> {
        let cases: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("../tests/fixtures/session_status.json"))?;
        for case in cases {
            let run = || -> Result<_> {
                let events: Vec<Event> = serde_json::from_value(case["events"].clone())?;
                let planned = serde_json::from_value::<Vec<(u64, u64)>>(case["planned"].clone())?;
                let product = case["product"].as_str().unwrap();
                let ric = case["ric"].as_str().unwrap();
                Ok((project(&planned, &events, product, ric)?, planned, events))
            };
            if case["valid"] != true {
                assert!(run().is_err(), "{}", case["name"]);
                continue;
            }
            let (full, planned, events) = run()?;
            assert_eq!(
                serde_json::to_value(&full)?,
                case["expected"],
                "{}",
                case["name"]
            );
            let mut cutoffs = (0..=600_000_000_000)
                .step_by(MINUTE_NS as usize)
                .collect::<BTreeSet<_>>();
            for e in &events {
                cutoffs.extend([
                    e.known_at_ns,
                    e.known_at_ns + 1,
                    e.effective_ns,
                    e.effective_ns + 1,
                ]);
            }
            for cutoff in cutoffs {
                let visible = events
                    .iter()
                    .filter(|e| e.known_at_ns < cutoff)
                    .cloned()
                    .collect::<Vec<_>>();
                let prefix = project(
                    &planned,
                    &visible,
                    case["product"].as_str().unwrap(),
                    case["ric"].as_str().unwrap(),
                )?;
                assert_eq!(
                    minutes(&full, cutoff),
                    minutes(&prefix, cutoff),
                    "{} at {cutoff}",
                    case["name"]
                );
            }
        }
        Ok(())
    }
}
