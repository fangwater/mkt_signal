//! Explicit event clock; Python oracle is scripts/cme_trade_timing.py.
use crate::{product::exch_event_time_ns, MISSING_EXCH_HMS_NS};
use anyhow::{ensure, Result};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

const MINUTE_NS: u64 = 60_000_000_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TimeBasis {
    #[default]
    Exchange,
    Publication,
}

pub fn bar_time(published_ns: u64, exchange_hms_ns: u64, basis: TimeBasis) -> Result<u64> {
    ensure!(published_ns <= i64::MAX as u64, "invalid source timestamp");
    if basis == TimeBasis::Publication || exchange_hms_ns == MISSING_EXCH_HMS_NS {
        Ok(published_ns)
    } else {
        exch_event_time_ns(published_ns, exchange_hms_ns)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimingAudit {
    pub later_minute: u64,
    pub later_minute_volume: u64,
    pub earlier_minute: u64,
    pub missing_exchange_time: u64,
}

impl TimingAudit {
    pub fn merge(&mut self, other: Self) {
        self.later_minute += other.later_minute;
        self.later_minute_volume += other.later_minute_volume;
        self.earlier_minute += other.earlier_minute;
        self.missing_exchange_time += other.missing_exchange_time;
    }
    pub fn add(&mut self, published_ns: u64, exchange_hms_ns: u64, volume: u64) -> Result<()> {
        let recovered = bar_time(published_ns, exchange_hms_ns, TimeBasis::Exchange)?;
        let source_minute = published_ns / MINUTE_NS;
        let exchange_minute = recovered / MINUTE_NS;
        self.later_minute += u64::from(source_minute > exchange_minute);
        if source_minute > exchange_minute {
            self.later_minute_volume += volume;
        }
        self.earlier_minute += u64::from(source_minute < exchange_minute);
        self.missing_exchange_time += u64::from(exchange_hms_ns == MISSING_EXCH_HMS_NS);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matches_python_and_never_backfills_a_closed_minute() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/trade_timing.json")).unwrap();
        for case in cases.as_array().unwrap() {
            let published = case["published_ns"].as_u64().unwrap();
            let hms = case["exchange_hms_ns"]
                .as_u64()
                .unwrap_or(MISSING_EXCH_HMS_NS);
            let volume = case["volume"].as_u64().unwrap();
            for basis in [TimeBasis::Exchange, TimeBasis::Publication] {
                let output = bar_time(published, hms, basis).unwrap();
                let key = if basis == TimeBasis::Publication {
                    "publication_ns"
                } else {
                    "exchange_ns"
                };
                assert_eq!(output, case[key].as_u64().unwrap());
                if basis == TimeBasis::Publication {
                    assert!(published < (output / MINUTE_NS + 1) * MINUTE_NS);
                }
            }
            let mut audit = TimingAudit::default();
            audit.add(published, hms, volume).unwrap();
            assert_eq!(serde_json::to_value(audit).unwrap(), case["audit"]);
        }
    }
}
