//! Hourly trade-notional KLL, same sketch as crypto `tardis_ipc_replay` kll_only.
//!
//! Samples are `price * volume` on printable `cme_trade` only. Special is not
//! a sample. Empty hours are not emitted.

use anyhow::{anyhow, bail, Result};
use rolling_common::kll_quantile::{FrozenKllSketch, StreamingKllSketch};

pub const HOUR_MS: i64 = 3_600_000;
pub const CME_TAS_KLL_VENUE: u8 = 100;
pub const CME_TAS_KLL_TABLE: &str = "trade_notional_kll_cme_tas_hourly";

#[derive(Debug, Clone)]
pub struct HourlyNotionalKllSnapshot {
    pub hour_start_ms: i64,
    pub sketch: FrozenKllSketch,
}

pub struct HourlyNotionalKll {
    current_hour_start_ms: Option<i64>,
    sketch: StreamingKllSketch,
    late_trades: u64,
}

impl Default for HourlyNotionalKll {
    fn default() -> Self {
        Self::new()
    }
}

impl HourlyNotionalKll {
    pub fn new() -> Self {
        Self {
            current_hour_start_ms: None,
            sketch: StreamingKllSketch::new(),
            late_trades: 0,
        }
    }

    pub fn on_notional(
        &mut self,
        timestamp_us: i64,
        notional: f64,
    ) -> Option<HourlyNotionalKllSnapshot> {
        if !notional.is_finite() || notional <= 0.0 {
            return None;
        }
        let hour_start_ms = align_ms(timestamp_us.div_euclid(1_000), HOUR_MS);
        let snapshot = match self.current_hour_start_ms {
            None => {
                self.current_hour_start_ms = Some(hour_start_ms);
                None
            }
            Some(current) if hour_start_ms < current => {
                self.late_trades = self.late_trades.saturating_add(1);
                return None;
            }
            Some(current) if hour_start_ms > current => {
                let snapshot = self.freeze_current();
                self.current_hour_start_ms = Some(hour_start_ms);
                snapshot
            }
            Some(_) => None,
        };
        self.sketch.insert(notional);
        snapshot
    }

    pub fn flush(&mut self) -> Option<HourlyNotionalKllSnapshot> {
        self.freeze_current()
    }

    pub fn late_trades(&self) -> u64 {
        self.late_trades
    }

    fn freeze_current(&mut self) -> Option<HourlyNotionalKllSnapshot> {
        let hour_start_ms = self.current_hour_start_ms.take()?;
        if self.sketch.is_empty() {
            return None;
        }
        let snapshot = HourlyNotionalKllSnapshot {
            hour_start_ms,
            sketch: self.sketch.freeze(),
        };
        self.sketch.reset();
        Some(snapshot)
    }
}

pub fn align_ms(timestamp_ms: i64, bar_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(bar_ms)
}

pub fn timestamp_us_from_utc_ns(ts_utc_ns: u64) -> Result<i64> {
    i64::try_from(ts_utc_ns / 1_000).map_err(|_| anyhow!("ts_utc_ns {ts_utc_ns} exceeds i64 us"))
}

pub fn trade_notional(price_e9: i64, volume: u32) -> Result<f64> {
    use crate::{price_e9_to_f64, MISSING_PRICE, MISSING_VOLUME};
    if price_e9 == MISSING_PRICE {
        bail!("printable trade missing Price");
    }
    if volume == MISSING_VOLUME {
        bail!("printable trade missing Volume");
    }
    let price = price_e9_to_f64(price_e9).ok_or_else(|| anyhow!("printable trade missing Price"))?;
    Ok(price * f64::from(volume))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_price_e9;

    #[test]
    fn freezes_on_the_next_utc_hour_and_skips_non_positive() {
        let mut kll = HourlyNotionalKll::new();
        assert!(kll.on_notional(HOUR_MS * 1_000 - 1, 100.0).is_none());
        assert!(kll.on_notional(HOUR_MS * 1_000 - 1, 0.0).is_none());
        let closed = kll
            .on_notional(HOUR_MS * 1_000, 50.0)
            .expect("previous hour should freeze");
        assert_eq!(closed.hour_start_ms, 0);
        assert_eq!(closed.sketch.sample_count, 1);
        let trailing = kll.flush().expect("trailing hour should flush");
        assert_eq!(trailing.hour_start_ms, HOUR_MS);
        assert_eq!(trailing.sketch.sample_count, 1);
    }

    #[test]
    fn notional_is_price_times_contract_volume() {
        let price = parse_price_e9("2999.75").unwrap();
        let notional = trade_notional(price, 2).unwrap();
        assert!((notional - 5999.5).abs() < 1e-9);
    }
}
