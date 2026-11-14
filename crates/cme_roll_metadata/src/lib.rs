//! Day-local causal metadata shared by HFQ price and factor consumers.
//! Python oracle: scripts/cme_causal_roll_tables.py. Never read future gaps.
use anyhow::{ensure, Context, Result};
use chrono::NaiveDate;
use serde::Deserialize;
use std::{collections::BTreeMap, path::Path};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Day {
    pub product: String,
    pub trading_day: NaiveDate,
    pub previous_day: NaiveDate,
    pub contract_id: String,
    pub ric: String,
    pub decision_ns: i64,
    pub effective_ns: i64,
    pub segment_id: Option<i64>,
    pub offset_e9: Option<i64>,
    pub reason: String,
    pub adjustment_status: String,
}

impl Day {
    pub fn selected(&self) -> bool {
        !self.ric.is_empty()
    }
    pub fn offset(&self) -> f64 {
        self.offset_e9.expect("offset requires a selected day") as f64 / 1e9
    }
}

pub fn exchange(product: &str) -> Option<&'static str> {
    match product {
        "URO" | "JY" => Some("CME"),
        "HG" | "SI" => Some("COMEX"),
        "TU" | "FV" | "TY" | "TN" | "US" | "AUL" => Some("CBOT"),
        _ => None,
    }
}

pub fn read_daily(path: &Path, product: &str) -> Result<BTreeMap<NaiveDate, Day>> {
    ensure!(
        exchange(product).is_some(),
        "unsupported causal product {product}"
    );
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("open causal daily metadata {}", path.display()))?;
    let mut result = BTreeMap::new();
    let mut previous: Option<Day> = None;
    for row in reader.deserialize::<Day>() {
        let row = row.with_context(|| format!("parse causal metadata {}", path.display()))?;
        ensure!(row.product == product, "wrong product {}", row.product);
        ensure!(
            ["seed", "hold", "volume", "deadline", "exit_only"].contains(&row.reason.as_str()),
            "blocked or unknown decision {} {}",
            row.trading_day,
            row.reason
        );
        ensure!(
            row.previous_day < row.trading_day
                && row.decision_ns >= 0
                && row.decision_ns < row.effective_ns,
            "invalid decision timing"
        );
        if row.reason == "exit_only" {
            ensure!(
                !row.selected()
                    && row.contract_id.is_empty()
                    && row.segment_id.is_none()
                    && row.offset_e9.is_none(),
                "exit_only selects a contract"
            );
        } else {
            ensure!(
                row.selected()
                    && !row.contract_id.is_empty()
                    && row.segment_id.is_some_and(|s| s > 0)
                    && row.offset_e9.is_some(),
                "missing selected identity/segment"
            );
        }
        if let Some(prior) = &previous {
            ensure!(
                row.trading_day > prior.trading_day && row.previous_day == prior.trading_day,
                "daily table has a missing/out-of-order decision"
            );
            ensure!(
                row.decision_ns > prior.decision_ns && row.effective_ns > prior.effective_ns,
                "decision timestamps are not increasing"
            );
            if let (Some(current), Some(old)) = (row.segment_id, prior.segment_id) {
                ensure!(current >= old, "segment goes backwards");
            }
            if row.reason == "hold" {
                ensure!(
                    (row.ric.as_str(), row.segment_id, row.offset_e9)
                        == (prior.ric.as_str(), prior.segment_id, prior.offset_e9),
                    "hold changes identity/adjustment"
                );
            }
        }
        previous = Some(row.clone());
        result.insert(row.trading_day, row);
    }
    ensure!(!result.is_empty(), "empty causal daily table");
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    const FIXTURE: &str =
        include_str!("../../cme_tas_replay/tests/fixtures/causal_roll.dominant.csv");
    fn read(text: &str) -> Result<BTreeMap<NaiveDate, Day>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("dominant.csv");
        std::fs::write(&path, text)?;
        read_daily(&path, "TY")
    }
    #[test]
    fn prefix_offsets_do_not_depend_on_future_rolls() {
        let all = read(FIXTURE).unwrap();
        let values: Vec<_> = all.values().map(Day::offset).collect();
        assert_eq!(values, vec![0.0, 0.0, 0.0625, 0.0625, 0.0625]);
        for n in 1..=5 {
            let text = FIXTURE.lines().take(n + 1).collect::<Vec<_>>().join("\n");
            let prefix = read(&text).unwrap();
            assert_eq!(
                prefix,
                all.iter().take(n).map(|(k, v)| (*k, v.clone())).collect()
            );
        }
    }
    #[test]
    fn reject_holes_and_blocked_rows() {
        let lines: Vec<_> = FIXTURE.lines().collect();
        let missing = [&lines[..2], &lines[3..]].concat().join("\n");
        assert!(read(&missing).is_err());
        assert!(read(&FIXTURE.replace(",deadline,", ",blocked,")).is_err());
        assert!(read(&FIXTURE.replace(",62500000,hold", ",62500001,hold")).is_err());
    }
}
