//! Closed-bar ylabel prices for crypto replay.
//!
//! ClickHouse stores venue-native `twap`, `vwap`, and `midp`. Horizons are
//! 5s, 10s, 30s, 1m, and 5m. Row `ts=t` stores the closed bucket `[t-horizon, t)`.
//! Wide HDF SID suffixes are an export concern and do not belong here.

use anyhow::{bail, Result};

pub const YLABEL_HORIZON_MS: [i64; 5] = [5_000, 10_000, 30_000, 60_000, 300_000];
pub const YLABEL_COLUMNS: [&str; 3] = ["twap", "vwap", "midp"];

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YlabelBar {
    pub ts_ms: i64,
    pub twap: f64,
    pub vwap: f64,
    pub midp: f64,
}

#[derive(Debug, Clone, Copy)]
struct OpenBucket {
    start_ms: i64,
    last_trade_us: i64,
    last_trade_price: f64,
    twap_area: f64,
    trade_notional: f64,
    trade_volume: f64,
    last_midp: Option<f64>,
}

impl OpenBucket {
    fn new(
        start_ms: i64,
        last_trade_us: i64,
        last_trade_price: f64,
        last_midp: Option<f64>,
    ) -> Self {
        Self {
            start_ms,
            last_trade_us,
            last_trade_price,
            twap_area: 0.0,
            trade_notional: 0.0,
            trade_volume: 0.0,
            last_midp,
        }
    }

    fn on_trade(&mut self, timestamp_us: i64, price: f64, amount: f64) {
        if timestamp_us > self.last_trade_us && self.last_trade_price.is_finite() {
            self.twap_area += self.last_trade_price * (timestamp_us - self.last_trade_us) as f64;
        }
        self.last_trade_us = timestamp_us.max(self.last_trade_us);
        self.last_trade_price = price;
        if amount.is_finite() && amount > 0.0 && price.is_finite() && price > 0.0 {
            self.trade_notional += price * amount;
            self.trade_volume += amount;
        }
    }

    fn on_midp(&mut self, midp: f64) {
        if midp.is_finite() && midp > 0.0 {
            self.last_midp = Some(midp);
        }
    }

    fn close(mut self, end_ms: i64, horizon_us: i64) -> YlabelBar {
        let end_us = end_ms.saturating_mul(1_000);
        if end_us > self.last_trade_us && self.last_trade_price.is_finite() {
            self.twap_area += self.last_trade_price * (end_us - self.last_trade_us) as f64;
        }
        let twap = if self.last_trade_price.is_finite() && self.last_trade_price > 0.0 {
            self.twap_area / horizon_us as f64
        } else {
            f64::NAN
        };
        let vwap = if self.trade_volume > 0.0 {
            self.trade_notional / self.trade_volume
        } else {
            twap
        };
        YlabelBar {
            ts_ms: end_ms,
            twap,
            vwap,
            midp: self.last_midp.unwrap_or(f64::NAN),
        }
    }
}

#[derive(Debug)]
pub struct YlabelAggregator {
    horizon_ms: i64,
    current: Option<OpenBucket>,
    last_trade_us: i64,
    last_trade_price: f64,
    last_midp: Option<f64>,
}

impl YlabelAggregator {
    pub fn new(horizon_ms: i64) -> Result<Self> {
        if !YLABEL_HORIZON_MS.contains(&horizon_ms) {
            bail!("unsupported ylabel horizon_ms={horizon_ms}");
        }
        Ok(Self {
            horizon_ms,
            current: None,
            last_trade_us: 0,
            last_trade_price: f64::NAN,
            last_midp: None,
        })
    }

    pub fn on_trade(
        &mut self,
        timestamp_us: i64,
        price: f64,
        amount: f64,
    ) -> Result<Vec<YlabelBar>> {
        if !price.is_finite() || price <= 0.0 || !amount.is_finite() || amount <= 0.0 {
            bail!("ylabel trade requires positive finite price and amount");
        }
        let closed = self.advance_to(timestamp_us)?;
        self.ensure_bucket(timestamp_us);
        if let Some(bucket) = self.current.as_mut() {
            bucket.on_trade(timestamp_us, price, amount);
        }
        self.last_trade_us = timestamp_us;
        self.last_trade_price = price;
        Ok(closed)
    }

    pub fn on_midp(&mut self, timestamp_us: i64, midp: f64) -> Result<Vec<YlabelBar>> {
        if !midp.is_finite() || midp <= 0.0 {
            return Ok(Vec::new());
        }
        let closed = self.advance_to(timestamp_us)?;
        self.ensure_bucket(timestamp_us);
        if let Some(bucket) = self.current.as_mut() {
            bucket.on_midp(midp);
        }
        self.last_midp = Some(midp);
        Ok(closed)
    }

    pub fn start_at(&mut self, start_ms: i64) {
        if self.current.is_some() {
            return;
        }
        self.current = Some(OpenBucket::new(
            align_ms(start_ms, self.horizon_ms),
            start_ms.saturating_mul(1_000),
            self.last_trade_price,
            self.last_midp,
        ));
    }

    pub fn seed(&mut self, last_trade_us: i64, last_trade_price: f64, last_midp: Option<f64>) {
        if last_trade_us > 0 {
            self.last_trade_us = last_trade_us;
        }
        if last_trade_price.is_finite() && last_trade_price > 0.0 {
            self.last_trade_price = last_trade_price;
        }
        if let Some(midp) = last_midp {
            if midp.is_finite() && midp > 0.0 {
                self.last_midp = Some(midp);
            }
        }
    }

    pub fn finish_until_ms(&mut self, end_exclusive_ms: i64) -> Result<Vec<YlabelBar>> {
        if end_exclusive_ms <= 0 || self.current.is_none() {
            return Ok(Vec::new());
        }
        let mut closed = Vec::new();
        while let Some(bucket) = self.current.as_ref() {
            let end_ms = bucket.start_ms + self.horizon_ms;
            if end_ms > end_exclusive_ms {
                break;
            }
            let current = self.current.take().expect("bucket present");
            closed.push(current.close(end_ms, self.horizon_us()));
            if end_ms < end_exclusive_ms {
                self.current = Some(OpenBucket::new(
                    end_ms,
                    end_ms.saturating_mul(1_000),
                    self.last_trade_price,
                    self.last_midp,
                ));
            }
        }
        Ok(closed)
    }

    fn advance_to(&mut self, timestamp_us: i64) -> Result<Vec<YlabelBar>> {
        let target_start = align_ms(timestamp_us.div_euclid(1_000), self.horizon_ms);
        let mut out = Vec::new();
        while let Some(bucket) = self.current.as_ref() {
            if target_start < bucket.start_ms {
                break;
            }
            if target_start == bucket.start_ms {
                break;
            }
            let current = self.current.take().expect("bucket present");
            let end_ms = current.start_ms + self.horizon_ms;
            out.push(current.close(end_ms, self.horizon_us()));
            if end_ms < target_start {
                self.current = Some(OpenBucket::new(
                    end_ms,
                    end_ms.saturating_mul(1_000),
                    self.last_trade_price,
                    self.last_midp,
                ));
            }
        }
        Ok(out)
    }

    fn ensure_bucket(&mut self, timestamp_us: i64) {
        if self.current.is_some() {
            return;
        }
        let start_ms = align_ms(timestamp_us.div_euclid(1_000), self.horizon_ms);
        self.current = Some(OpenBucket::new(
            start_ms,
            if self.last_trade_price.is_finite() {
                start_ms.saturating_mul(1_000)
            } else {
                timestamp_us
            },
            self.last_trade_price,
            self.last_midp,
        ));
    }

    fn horizon_us(&self) -> i64 {
        self.horizon_ms.saturating_mul(1_000)
    }
}

pub fn ylabel_table_name(venue_slug: &str, horizon_ms: i64) -> String {
    format!(
        "ylabel_{}_{}",
        venue_slug.replace('-', "_"),
        horizon_label(horizon_ms)
    )
}

pub fn ylabel_clickhouse_columns_sql() -> String {
    [
        "ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD)",
        "symbol String",
        "twap Float64",
        "vwap Float64",
        "midp Float64",
    ]
    .join(", ")
}

pub fn encode_ylabel_clickhouse_row(symbol: &str, bar: &YlabelBar) -> Vec<u8> {
    let mut row = Vec::with_capacity(16 + symbol.len() + 24);
    row.extend_from_slice(&bar.ts_ms.to_le_bytes());
    append_var_uint(&mut row, symbol.len() as u64);
    row.extend_from_slice(symbol.as_bytes());
    for value in [bar.twap, bar.vwap, bar.midp] {
        row.extend_from_slice(&value.to_le_bytes());
    }
    row
}

pub fn horizon_label(horizon_ms: i64) -> &'static str {
    match horizon_ms {
        5_000 => "5s",
        10_000 => "10s",
        30_000 => "30s",
        60_000 => "1m",
        300_000 => "5m",
        _ => "unknown",
    }
}

pub fn ylabel_expected_rows_per_day(horizon_ms: i64) -> i64 {
    86_400_000 / horizon_ms
}

fn align_ms(timestamp_ms: i64, bar_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(bar_ms)
}

fn append_var_uint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_venue_native() {
        assert_eq!(
            ylabel_table_name("binance-futures", 5_000),
            "ylabel_binance_futures_5s"
        );
        assert_eq!(
            ylabel_table_name("binance-futures", 10_000),
            "ylabel_binance_futures_10s"
        );
        assert_eq!(
            ylabel_table_name("binance-futures", 60_000),
            "ylabel_binance_futures_1m"
        );
        assert!(!ylabel_clickhouse_columns_sql().contains("_1"));
        assert!(!ylabel_clickhouse_columns_sql().contains("_6"));
    }

    #[test]
    fn five_second_row_uses_closed_bucket() {
        let mut agg = YlabelAggregator::new(5_000).unwrap();
        assert!(agg.on_trade(1_000, 100.0, 1.0).unwrap().is_empty());
        assert!(agg.on_midp(2_000, 100.5).unwrap().is_empty());
        let closed = agg.on_trade(5_001_000, 102.0, 1.0).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].ts_ms, 5_000);
        assert!((closed[0].vwap - 100.0).abs() < 1e-12);
        assert_eq!(closed[0].midp, 100.5);
        assert!(closed[0].twap.is_finite());
    }

    #[test]
    fn ten_second_row_uses_closed_bucket() {
        let mut agg = YlabelAggregator::new(10_000).unwrap();
        assert!(agg.on_trade(1_000, 100.0, 1.0).unwrap().is_empty());
        assert!(agg.on_midp(2_000, 100.5).unwrap().is_empty());
        let closed = agg.on_trade(10_001_000, 102.0, 1.0).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].ts_ms, 10_000);
        assert!((closed[0].vwap - 100.0).abs() < 1e-12);
        assert_eq!(closed[0].midp, 100.5);
        assert!(closed[0].twap.is_finite());
    }

    #[test]
    fn finish_until_closes_aligned_horizons() {
        assert_eq!(ylabel_expected_rows_per_day(5_000), 17_280);
        assert_eq!(ylabel_expected_rows_per_day(10_000), 8_640);
        assert_eq!(ylabel_expected_rows_per_day(30_000), 2_880);
        assert_eq!(ylabel_expected_rows_per_day(60_000), 1_440);
        assert_eq!(ylabel_expected_rows_per_day(300_000), 288);

        let mut agg = YlabelAggregator::new(30_000).unwrap();
        assert!(agg.on_trade(1_000, 100.0, 2.0).unwrap().is_empty());
        assert!(agg.on_trade(10_000_000, 110.0, 2.0).unwrap().is_empty());
        let closed = agg.finish_until_ms(30_000).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].ts_ms, 30_000);
        assert!((closed[0].vwap - 105.0).abs() < 1e-12);
        assert!(closed[0].twap.is_finite());
    }

    #[test]
    fn start_at_emits_a_full_aligned_day() {
        let mut agg = YlabelAggregator::new(10_000).unwrap();
        agg.seed(0, 100.0, Some(100.5));
        agg.start_at(0);
        let closed = agg.finish_until_ms(86_400_000).unwrap();
        assert_eq!(closed.len(), 8_640);
        assert_eq!(closed[0].ts_ms, 10_000);
        assert_eq!(closed[8_639].ts_ms, 86_400_000);
        assert_eq!(closed[0].midp, 100.5);
    }

    #[test]
    fn seed_carries_last_price_into_the_next_bucket() {
        let mut agg = YlabelAggregator::new(10_000).unwrap();
        agg.seed(9_999_000, 99.0, Some(99.5));
        agg.start_at(0);
        let closed = agg.finish_until_ms(20_000).unwrap();
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[0].ts_ms, 10_000);
        assert_eq!(closed[0].midp, 99.5);
        assert!(closed[0].twap.is_finite());
        assert_eq!(closed[1].ts_ms, 20_000);
    }

    #[test]
    fn empty_trade_bucket_falls_back_vwap_to_twap() {
        let mut agg = YlabelAggregator::new(10_000).unwrap();
        assert!(agg.on_trade(1_000, 100.0, 1.0).unwrap().is_empty());
        let closed = agg.finish_until_ms(20_000).unwrap();
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[1].ts_ms, 20_000);
        assert!(closed[1].twap.is_finite());
        assert_eq!(closed[1].vwap, closed[1].twap);
    }
}
