//! Source-prefix volume batches. Python oracle: scripts/cme_causal_volumes.py.
use anyhow::{ensure, Context, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const HALF_DAY: u64 = 43_200_000_000_000;
const MINUTE: u64 = 60_000_000_000;
const MAX: u64 = i64::MAX as u64;

#[derive(Clone, Deserialize, Serialize)]
pub struct Identity {
    pub ric: String,
    pub listed_ns: u64,
    pub known_at_ns: u64,
    pub source: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Request {
    pub product: String,
    pub trading_day: NaiveDate,
    pub calendar_known_at_ns: u64,
    pub calendar_source: String,
    pub segments: Vec<(u64, u64)>,
    /// Exclusive source snapshot boundary; must precede the decision.
    pub freeze_ns: u64,
    pub decision_ns: u64,
    pub contracts: Vec<Identity>,
}

impl Request {
    pub fn validate(&self) -> Result<u64> {
        ensure!(
            ["URO", "JY", "HG", "SI", "TU", "FV", "TY", "TN", "US", "AUL"]
                .contains(&self.product.as_str()),
            "unsupported product"
        );
        ensure!(
            self.freeze_ns < self.decision_ns && self.decision_ns <= MAX,
            "freeze must precede decision"
        );
        ensure!(
            self.calendar_known_at_ns < self.freeze_ns && !self.calendar_source.trim().is_empty(),
            "calendar not known at freeze"
        );
        let mut last = None;
        for &(lo, hi) in &self.segments {
            ensure!(
                lo < hi
                    && hi <= MAX
                    && lo % MINUTE == 0
                    && hi % MINUTE == 0
                    && last.is_none_or(|previous| lo >= previous),
                "invalid reference segment"
            );
            last = Some(hi);
        }
        ensure!(
            last.is_some_and(|close| close < self.freeze_ns),
            "reference day not complete before freeze"
        );
        let mut rics = BTreeSet::new();
        for c in &self.contracts {
            ensure!(
                !c.ric.is_empty()
                    && c.ric.len() <= 16
                    && c.ric.is_ascii()
                    && !c.ric.contains('\0')
                    && rics.insert(&c.ric),
                "invalid/duplicate RIC"
            );
            ensure!(
                c.known_at_ns < self.freeze_ns
                    && c.listed_ns < self.freeze_ns
                    && !c.source.trim().is_empty(),
                "identity not known/listed at freeze"
            );
        }
        ensure!(!rics.is_empty(), "empty candidate universe");
        self.segments[0]
            .0
            .checked_sub(HALF_DAY)
            .context("scan bound underflow")
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Trade {
    pub ric: String,
    pub published_ns: u64,
    pub exchange_hms_ns: Option<u64>,
    pub price_e9: Option<i64>,
    pub volume: u64,
    pub part: u16,
    pub seq: u32,
}

#[derive(Default, Serialize)]
pub struct Audit {
    pub source_start_ns: u64,
    pub source_cutoff_ns: u64,
    pub source_max_published_ns: Option<u64>,
    pub inspected: u64,
    pub included: u64,
    pub outside_session: u64,
    pub invalid_price: u64,
    pub missing_exchange_time: u64,
    pub future_exchange_time: u64,
    pub future_exchange_same_session: u64,
}

#[derive(Serialize)]
pub struct Quantity {
    pub ric: String,
    pub volume: Option<u64>,
    pub volume_lower: Option<u64>,
    pub volume_upper: Option<u64>,
}

#[derive(Serialize)]
pub struct Batch {
    pub product: String,
    pub trading_day: NaiveDate,
    pub available_at_ns: u64,
    pub complete: bool,
    pub source: &'static str,
    pub rows: Vec<Quantity>,
}

#[derive(Serialize)]
pub struct Frozen {
    pub batch: Batch,
    pub audit: Audit,
    pub blocked_reasons: Vec<&'static str>,
}

pub struct Accumulator {
    request: Request,
    quantities: BTreeMap<String, u64>,
    uncertain: BTreeMap<String, u64>,
    completed: BTreeSet<String>,
    // Inputs are streamed in RIC/source-key order, so one key is sufficient.
    last_key: Option<(String, u64, u16, u32)>,
    audit: Audit,
}

impl Accumulator {
    pub fn new(request: Request) -> Result<Self> {
        let source_start_ns = request.validate()?;
        Ok(Self {
            quantities: request
                .contracts
                .iter()
                .map(|c| (c.ric.clone(), 0))
                .collect(),
            uncertain: request
                .contracts
                .iter()
                .map(|c| (c.ric.clone(), 0))
                .collect(),
            audit: Audit {
                source_start_ns,
                source_cutoff_ns: request.freeze_ns,
                ..Audit::default()
            },
            request,
            completed: BTreeSet::new(),
            last_key: None,
        })
    }

    pub fn add(&mut self, e: &Trade) -> Result<()> {
        ensure!(e.published_ns <= MAX, "invalid source timestamp");
        if e.published_ns < self.audit.source_start_ns
            || e.published_ns >= self.request.freeze_ns
            || !self.quantities.contains_key(&e.ric)
        {
            return Ok(());
        }
        let key = (e.ric.clone(), e.published_ns, e.part, e.seq);
        ensure!(
            self.last_key.as_ref().is_none_or(|old| old < &key),
            "duplicate/unordered source key"
        );
        self.last_key = Some(key);
        self.audit.inspected += 1;
        self.audit.source_max_published_ns = Some(
            self.audit
                .source_max_published_ns
                .unwrap_or(0)
                .max(e.published_ns),
        );
        if e.price_e9.is_none_or(|price| price <= 0) {
            self.audit.invalid_price += 1;
            return Ok(());
        }
        ensure!(
            e.volume > 0 && e.volume < u32::MAX as u64,
            "invalid printable volume"
        );
        let event = match e.exchange_hms_ns {
            Some(hms) => crate::product::exch_event_time_ns(e.published_ns, hms)?,
            None => {
                self.audit.missing_exchange_time += 1;
                e.published_ns
            }
        };
        if event > e.published_ns {
            self.audit.future_exchange_time += 1;
            // Request validation requires all segments to have ended before
            // freeze. Both clocks then imply the same daily quantity without
            // changing either timestamp or authorizing a minute backfill.
            if !self.request.segments.iter().any(|&(lo, hi)| {
                lo <= e.published_ns && e.published_ns < hi && lo <= event && event < hi
            }) {
                let lower = self.quantities[&e.ric];
                let extra = self.uncertain.get_mut(&e.ric).context("reviewed RIC")?;
                *extra = extra
                    .checked_add(e.volume)
                    .filter(|value| value.checked_add(lower).is_some_and(|sum| sum <= MAX))
                    .context("volume upper bound overflow")?;
                return Ok(());
            }
            self.audit.future_exchange_same_session += 1;
        }
        if !self
            .request
            .segments
            .iter()
            .any(|&(lo, hi)| lo <= event && event < hi)
        {
            self.audit.outside_session += 1;
            return Ok(());
        }
        let sum = self.quantities.get_mut(&e.ric).context("reviewed RIC")?;
        *sum = sum
            .checked_add(e.volume)
            .filter(|value| {
                value
                    .checked_add(self.uncertain[&e.ric])
                    .is_some_and(|upper| upper <= MAX)
            })
            .context("volume overflow")?;
        self.audit.included += 1;
        Ok(())
    }

    pub fn finish_ric(&mut self, ric: &str) -> Result<()> {
        ensure!(
            self.quantities.contains_key(ric) && self.completed.insert(ric.to_string()),
            "invalid scan completion"
        );
        Ok(())
    }

    pub fn finish(self) -> Frozen {
        let mut blocked_reasons = Vec::new();
        if self.completed.len() != self.quantities.len() {
            blocked_reasons.push("source_scan_incomplete");
        }
        let complete = blocked_reasons.is_empty();
        Frozen {
            batch: Batch {
                product: self.request.product,
                trading_day: self.request.trading_day,
                available_at_ns: self.request.freeze_ns,
                complete,
                source: "tas_printable",
                rows: self
                    .quantities
                    .into_iter()
                    .map(|(ric, volume)| {
                        let upper = volume + self.uncertain[&ric];
                        Quantity {
                            ric,
                            volume: (complete && volume == upper).then_some(volume),
                            volume_lower: complete.then_some(volume),
                            volume_upper: complete.then_some(upper),
                        }
                    })
                    .collect(),
            },
            audit: self.audit,
            blocked_reasons,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn python_parity_and_future_prefix_invariance() -> Result<()> {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/causal_volumes.json"))?;
        for c in cases.as_array().context("cases")? {
            let run = || -> Result<serde_json::Value> {
                let mut a = Accumulator::new(serde_json::from_value(c["request"].clone())?)?;
                let mut events: Vec<Trade> = serde_json::from_value(c["events"].clone())?;
                events.sort_by(|a, b| {
                    (&a.ric, a.published_ns, a.part, a.seq).cmp(&(
                        &b.ric,
                        b.published_ns,
                        b.part,
                        b.seq,
                    ))
                });
                for event in events {
                    a.add(&event)?;
                }
                for ric in c["completed_rics"].as_array().context("rics")? {
                    a.finish_ric(ric.as_str().context("ric")?)?;
                }
                Ok(serde_json::to_value(a.finish())?)
            };
            if c["valid"] == true {
                assert_eq!(run()?, c["expected"], "{}", c["name"]);
            } else {
                assert!(run().is_err(), "{}", c["name"]);
            }
        }
        Ok(())
    }
}
