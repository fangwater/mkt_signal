//! Reviewed historical inputs. Python oracle: scripts/cme_history_catalog.py.
use anyhow::{ensure, Context, Result};
use chrono::{Duration, NaiveDate, TimeZone};
use chrono_tz::America::Chicago;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const MINUTE_NS: u64 = 60_000_000_000;

#[derive(Clone, Deserialize, Serialize)]
pub struct CalendarDay {
    pub product: String,
    pub trading_day: NaiveDate,
    pub known_at_ns: u64,
    pub source: String,
    pub segments: Vec<(u64, u64)>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Contract {
    pub product: String,
    pub ric: String,
    pub contract_id: String,
    pub from: NaiveDate,
    pub until: NaiveDate,
    pub known_at_ns: u64,
    pub source: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Multiplier {
    pub product: String,
    pub exchange: String,
    pub volume_multiple: f64,
    pub from: NaiveDate,
    pub until: NaiveDate,
    pub known_at_ns: u64,
    pub source: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Catalog {
    pub days: Vec<CalendarDay>,
    pub contracts: Vec<Contract>,
    pub multipliers: Vec<Multiplier>,
    /// Bind a planned calendar to its mandatory causal status overlay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_session_status_sha256: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Identity {
    pub ric: String,
    pub contract_id: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ProjectedDay {
    pub product: String,
    pub trading_day: NaiveDate,
    pub segments: Vec<(u64, u64)>,
    pub volume_multiple: f64,
    pub contracts: Vec<Identity>,
}

fn evidence(known: u64, source: &str) -> Result<()> {
    ensure!(
        known <= i64::MAX as u64 && !source.trim().is_empty(),
        "missing evidence or invalid known_at"
    );
    Ok(())
}

impl Catalog {
    pub fn project(
        &self,
        products: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<ProjectedDay>> {
        ensure!(
            end > start && !products.is_empty(),
            "invalid catalog request"
        );
        let unique = products.iter().collect::<std::collections::BTreeSet<_>>();
        ensure!(
            unique.len() == products.len(),
            "duplicate requested products"
        );
        let mut days = BTreeMap::new();
        let mut intervals: BTreeMap<&str, Vec<(u64, u64)>> = BTreeMap::new();
        for day in &self.days {
            evidence(day.known_at_ns, &day.source)?;
            if day.segments.is_empty() {
                let nominal = Chicago
                    .from_local_datetime(
                        &(day.trading_day - Duration::days(1))
                            .and_hms_opt(17, 0, 0)
                            .context("nominal open")?,
                    )
                    .single()
                    .context("nominal Chicago open")?
                    .timestamp();
                ensure!(
                    nominal > 0 && day.known_at_ns < nominal as u64 * 1_000_000_000,
                    "closure not known before nominal open"
                );
            }
            ensure!(
                days.insert((day.product.as_str(), day.trading_day), day)
                    .is_none(),
                "duplicate calendar day"
            );
            let mut last = None;
            for &(lo, hi) in &day.segments {
                ensure!(
                    hi <= i64::MAX as u64
                        && lo % MINUTE_NS == 0
                        && hi % MINUTE_NS == 0
                        && hi > lo
                        && last.is_none_or(|last| lo >= last),
                    "invalid calendar segment"
                );
                ensure!(day.known_at_ns < lo, "calendar not known before open");
                last = Some(hi);
                intervals.entry(&day.product).or_default().push((lo, hi));
            }
        }
        for spans in intervals.values_mut() {
            spans.sort_unstable();
            ensure!(
                spans.windows(2).all(|pair| pair[1].0 >= pair[0].1),
                "overlapping official trade dates"
            );
        }
        let mut identities: BTreeMap<(&str, &str), Vec<(NaiveDate, NaiveDate)>> = BTreeMap::new();
        for c in &self.contracts {
            evidence(c.known_at_ns, &c.source)?;
            ensure!(
                c.until > c.from && !c.ric.is_empty(),
                "invalid identity range"
            );
            let fields = c.contract_id.split(':').collect::<Vec<_>>();
            ensure!(
                fields.len() == 3 && !fields[0].is_empty() && fields[1] == c.product,
                "invalid contract identity"
            );
            NaiveDate::parse_from_str(&format!("{}-01", fields[2]), "%Y-%m-%d")?;
            let versions = identities.entry((&c.product, &c.ric)).or_default();
            ensure!(
                !versions.iter().any(|&(a, b)| c.from < b && a < c.until),
                "overlapping contract identity"
            );
            versions.push((c.from, c.until));
        }
        for m in &self.multipliers {
            evidence(m.known_at_ns, &m.source)?;
            ensure!(
                m.volume_multiple.is_finite() && m.volume_multiple > 0.0 && m.until > m.from,
                "invalid multiplier"
            );
        }
        let mut result = Vec::new();
        for product in products {
            let candidates = self
                .multipliers
                .iter()
                .filter(|m| m.product == *product && m.from <= start && m.until >= end)
                .collect::<Vec<_>>();
            ensure!(
                candidates.len() == 1,
                "missing or ambiguous bounded multiplier for {product}"
            );
            let multiplier = candidates[0];
            let mut day = start;
            while day < end {
                let record = days
                    .get(&(product.as_str(), day))
                    .with_context(|| format!("missing explicit calendar date {product} {day}"))?;
                if let Some(&(open, _)) = record.segments.first() {
                    ensure!(
                        multiplier.known_at_ns < open,
                        "multiplier not known before open"
                    );
                    let mut contracts = Vec::new();
                    for c in &self.contracts {
                        if c.product != *product || !(c.from <= day && day < c.until) {
                            continue;
                        }
                        ensure!(c.known_at_ns < open, "identity not known before open");
                        ensure!(
                            c.contract_id.split(':').next() == Some(multiplier.exchange.as_str()),
                            "contract exchange mismatch"
                        );
                        contracts.push(Identity {
                            ric: c.ric.clone(),
                            contract_id: c.contract_id.clone(),
                        });
                    }
                    ensure!(
                        !contracts.is_empty(),
                        "no reviewed contracts on open day {product} {day}"
                    );
                    contracts.sort_by(|a, b| a.ric.cmp(&b.ric));
                    result.push(ProjectedDay {
                        product: product.clone(),
                        trading_day: day,
                        segments: record.segments.clone(),
                        volume_multiple: multiplier.volume_multiple,
                        contracts,
                    });
                }
                day += Duration::days(1);
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reviewed_catalog_matches_python_including_holiday_and_rejections() -> Result<()> {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/history_catalog.json"))?;
        for case in cases.as_array().context("cases")? {
            let result = (|| -> Result<_> {
                let catalog: Catalog = serde_json::from_value(case["catalog"].clone())?;
                catalog.project(
                    &serde_json::from_value::<Vec<String>>(case["products"].clone())?,
                    case["start"].as_str().context("start")?.parse()?,
                    case["end"].as_str().context("end")?.parse()?,
                )
            })();
            if case["valid"] == true {
                assert_eq!(
                    serde_json::to_value(result?)?,
                    case["expected"],
                    "{}",
                    case["name"]
                );
            } else {
                assert!(result.is_err(), "{}", case["name"]);
            }
        }
        Ok(())
    }
}
