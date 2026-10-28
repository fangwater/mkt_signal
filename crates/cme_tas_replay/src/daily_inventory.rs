//! Unfiltered daily TAS inventory. This is not a session-approved roll input.
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, TimeZone, Timelike, Utc};
use chrono_tz::America::Chicago;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Deserialize)]
pub struct Event {
    pub ric: String,
    pub published_ns: u64,
    pub exchange_hms_ns: Option<u64>,
    pub price_e9: Option<i64>,
    pub volume: u64,
}

#[derive(Default, Serialize, Deserialize, PartialEq, Debug)]
pub struct Daily {
    pub ric: String,
    pub trading_day: String,
    pub volume: u64,
    pub trades: u64,
    pub invalid_price: u64,
    pub max_published_ns: u64,
    pub min_event_ns: u64,
    pub max_event_ns: u64,
    pub exchange_time_fallback: u64,
    pub late_after_1655: u64,
}

pub fn event_time(e: &Event) -> Result<u64> {
    match e.exchange_hms_ns {
        Some(hms) => crate::product::exch_event_time_ns(e.published_ns, hms),
        None => Ok(e.published_ns),
    }
}

pub fn trading_day(event: u64) -> Result<chrono::NaiveDate> {
    let dt = DateTime::<Utc>::from_timestamp((event / 1_000_000_000) as i64, 0)
        .context("event timestamp out of range")?
        .with_timezone(&Chicago);
    Ok(dt.date_naive() + Duration::days(i64::from(dt.hour() >= 17)))
}

pub fn add(output: &mut BTreeMap<(String, String), Daily>, e: &Event) -> Result<()> {
    let event = event_time(e)?;
    let day = trading_day(event)?;
    let label = day.to_string();
    let a = output
        .entry((e.ric.clone(), label.clone()))
        .or_insert_with(|| Daily {
            ric: e.ric.clone(),
            trading_day: label,
            min_event_ns: event,
            max_event_ns: event,
            ..Daily::default()
        });
    a.max_published_ns = a.max_published_ns.max(e.published_ns);
    a.min_event_ns = a.min_event_ns.min(event);
    a.max_event_ns = a.max_event_ns.max(event);
    a.exchange_time_fallback += u64::from(e.exchange_hms_ns.is_none());
    let cutoff = Chicago
        .from_local_datetime(&day.and_hms_opt(16, 55, 0).unwrap())
        .single()
        .context("ambiguous freeze time")?
        .timestamp() as u64
        * 1_000_000_000;
    a.late_after_1655 += u64::from(e.published_ns >= cutoff);
    if e.price_e9.is_none_or(|p| p <= 0) {
        a.invalid_price += 1;
        return Ok(());
    }
    if e.volume == 0 || e.volume == u32::MAX as u64 {
        bail!("invalid printable volume {}", e.volume);
    }
    a.volume = a.volume.checked_add(e.volume).context("volume overflow")?;
    a.trades += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matches_python_fixed_fixture() {
        #[derive(Deserialize)]
        struct Fixture {
            events: Vec<Event>,
            expected: Vec<Daily>,
        }
        let f: Fixture =
            serde_json::from_str(include_str!("../tests/fixtures/tas_daily_inventory.json"))
                .unwrap();
        let mut actual = BTreeMap::new();
        for e in f.events {
            add(&mut actual, &e).unwrap();
        }
        assert_eq!(actual.into_values().collect::<Vec<_>>(), f.expected);
    }
}
