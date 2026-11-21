//! Per-minute MD latency evidence: exchange stamp -> SDK callback receipt,
//! receipt -> business-thread processing, and the deepest queue observed.
use super::super::text_ref;
use super::TickRecord;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(60);
const SAMPLES: usize = 65_536;

pub struct LatencyStats {
    started: Instant,
    ticks: u64,
    max_queued: usize,
    exchange_to_receive_ms: Vec<i64>,
    receive_to_process_us: Vec<u64>,
}

impl LatencyStats {
    pub fn new(now: Instant) -> Self {
        Self {
            started: now,
            ticks: 0,
            max_queued: 0,
            exchange_to_receive_ms: Vec::with_capacity(SAMPLES),
            receive_to_process_us: Vec::with_capacity(SAMPLES),
        }
    }

    /// Bounded CPU work with preallocated storage; no I/O.
    pub fn observe(&mut self, tick: &TickRecord, processed_us: u64, queued: usize) {
        self.ticks += 1;
        self.max_queued = self.max_queued.max(queued);
        if self.receive_to_process_us.len() < SAMPLES {
            self.receive_to_process_us
                .push(processed_us.saturating_sub(tick.received_us));
        }
        if self.exchange_to_receive_ms.len() < SAMPLES {
            if let Some(exchange_us) = exchange_us(tick) {
                self.exchange_to_receive_ms
                    .push((tick.received_us as i64 - exchange_us).div_euclid(1000));
            }
        }
    }

    pub fn take_due(&mut self, now: Instant) -> Option<Value> {
        if now.saturating_duration_since(self.started) < WINDOW {
            return None;
        }
        let ticks = std::mem::take(&mut self.ticks);
        let max_queued = std::mem::take(&mut self.max_queued);
        self.started = now;
        let exchange = summary(&mut self.exchange_to_receive_ms);
        let process = summary(&mut self.receive_to_process_us);
        self.exchange_to_receive_ms.clear();
        self.receive_to_process_us.clear();
        (ticks > 0).then(|| {
            json!({"type":"md_latency","window_s":WINDOW.as_secs(),"ticks":ticks,
                "max_queued_events":max_queued,
                "exchange_to_receive_ms":exchange,"receive_to_process_us":process})
        })
    }
}

fn summary<T: Copy + Ord + serde::Serialize>(values: &mut [T]) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_unstable();
    let at = |q: usize| values[(values.len() - 1) * q / 100];
    json!({"p50":at(50),"p90":at(90),"p99":at(99),"max":values[values.len() - 1]})
}

/// Exchange ActionDay + UpdateTime + UpdateMillisec as Unix microseconds
/// (China Standard Time, UTC+8, no DST).
pub(crate) fn exchange_us(tick: &TickRecord) -> Option<i64> {
    let day = text_ref(&tick.action_day).ok()?.as_bytes();
    let time = text_ref(&tick.update_time).ok()?.as_bytes();
    if day.len() != 8 || time.len() != 8 || time[2] != b':' || time[5] != b':' {
        return None;
    }
    let num = |s: &[u8]| -> Option<i64> {
        s.iter().try_fold(0_i64, |acc, &b| {
            b.is_ascii_digit().then(|| acc * 10 + i64::from(b - b'0'))
        })
    };
    let (y, m, d) = (num(&day[..4])?, num(&day[4..6])?, num(&day[6..])?);
    let (hh, mm, ss) = (num(&time[..2])?, num(&time[3..5])?, num(&time[6..])?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    // Howard Hinnant's days_from_civil.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let seconds = days * 86_400 + hh * 3600 + mm * 60 + ss - 8 * 3600;
    Some(seconds * 1_000_000 + i64::from(tick.millisec.clamp(0, 999)) * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn tick(day: &[u8], time: &[u8], millisec: i32, received_us: u64) -> TickRecord {
        let mut e = super::super::RawEvent::default();
        e.action_day[..day.len()].copy_from_slice(day);
        e.update_time[..time.len()].copy_from_slice(time);
        e.millisec = millisec;
        TickRecord::capture(&e, received_us, 1, crate::model::Venue::Shfe)
    }
    #[test]
    fn exchange_stamp_is_china_standard_time_and_stats_summarize_each_window() {
        // 2026-11-19 11:25:48.500 CST == 1795058748.5 s.
        let t = tick(b"20261119", b"11:25:48", 500, 1_795_058_748_530_000);
        assert_eq!(exchange_us(&t), Some(1_795_058_748_500_000));
        assert_eq!(exchange_us(&tick(b"2026111", b"11:25:48", 0, 0)), None);
        let start = Instant::now();
        let mut stats = LatencyStats::new(start);
        stats.observe(&t, 1_795_058_748_530_040, 3);
        assert!(stats.take_due(start + Duration::from_secs(59)).is_none());
        let v = stats.take_due(start + WINDOW).unwrap();
        assert_eq!(v["ticks"], 1);
        assert_eq!(v["max_queued_events"], 3);
        assert_eq!(v["exchange_to_receive_ms"]["p50"], 30);
        assert_eq!(v["receive_to_process_us"]["max"], 40);
        assert!(
            stats.take_due(start + WINDOW * 2).is_none(),
            "empty window is silent"
        );
    }
}
