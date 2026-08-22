//! Crypto-aligned 1-minute trades from printable `cme_trade` plus Special volume.
//!
//! Python correctness baseline: `lseg/cme_tas_drop_special_1min.py`.
//! Special has no Price: only `special_count` / `special_volume`. ylabel is not
//! written here. Minutes with neither a printable trade nor a Special are skipped.

use anyhow::{anyhow, bail, Context, Result};
use polars::prelude::{DataFrame, NamedFrom, ParquetWriter, Series};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use crate::{
    decade_base_from_utc_ns, minute_left_edge_ns, parse_contract_id, price_e9_to_f64,
    tradeday_yyyymmdd, SlimTrade, MISSING_PRICE, MISSING_VOLUME, PRICE_SCALE,
};

pub const NS_PER_SEC: u64 = 1_000_000_000;
pub const NS_PER_US: u64 = 1_000;
pub const US_PER_MINUTE: i64 = 60_000_000;

pub const OUTPUT_COLUMNS: &[&str] = &[
    "contract_id",
    "ric",
    "ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "count",
    "special_count",
    "special_volume",
    "avg_amount",
    "vwap",
    "twap",
    "buy_count",
    "sell_count",
    "buy_volume",
    "sell_volume",
    "buy_amount",
    "sell_amount",
    "buy_vwap",
    "sell_vwap",
    "buy_twap",
    "sell_twap",
    "buy_high",
    "sell_low",
    "net_buy_amount",
    "net_buy_volume",
    "net_buy_pct",
    "implied_count",
    "implied_volume",
    "implied_amount",
    "implied_vwap",
    "implied_twap",
    "large_order",
    "medium_order",
    "small_order",
    "large_buy",
    "large_sell",
    "medium_buy",
    "medium_sell",
    "small_buy",
    "small_sell",
    "net_buy_large",
    "net_buy_medium",
    "net_buy_small",
];

const NULLABLE_FLOAT: &[&str] = &[
    "open",
    "high",
    "low",
    "close",
    "vwap",
    "twap",
    "buy_vwap",
    "sell_vwap",
    "buy_twap",
    "sell_twap",
    "buy_high",
    "sell_low",
    "net_buy_pct",
    "implied_vwap",
    "implied_twap",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SizeBucket {
    Small,
    Medium,
    Large,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropSpecialMinute {
    pub contract_id: String,
    pub ric: String,
    pub ts: i64,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: f64,
    pub amount: f64,
    pub count: f64,
    pub special_count: f64,
    pub special_volume: f64,
    pub avg_amount: f64,
    pub vwap: Option<f64>,
    pub twap: Option<f64>,
    pub buy_count: f64,
    pub sell_count: f64,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub buy_amount: f64,
    pub sell_amount: f64,
    pub buy_vwap: Option<f64>,
    pub sell_vwap: Option<f64>,
    pub buy_twap: Option<f64>,
    pub sell_twap: Option<f64>,
    pub buy_high: Option<f64>,
    pub sell_low: Option<f64>,
    pub net_buy_amount: f64,
    pub net_buy_volume: f64,
    pub net_buy_pct: Option<f64>,
    pub implied_count: f64,
    pub implied_volume: f64,
    pub implied_amount: f64,
    pub implied_vwap: Option<f64>,
    pub implied_twap: Option<f64>,
    pub large_order: f64,
    pub medium_order: f64,
    pub small_order: f64,
    pub large_buy: f64,
    pub large_sell: f64,
    pub medium_buy: f64,
    pub medium_sell: f64,
    pub small_buy: f64,
    pub small_sell: f64,
    pub net_buy_large: f64,
    pub net_buy_medium: f64,
    pub net_buy_small: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct SizeThresholds {
    pub p50: f64,
    pub p90: f64,
}

fn ratio(numer: f64, denom: f64) -> Option<f64> {
    if denom == 0.0 {
        None
    } else {
        Some(numer / denom)
    }
}

fn classify_size(amount: f64, thresholds: Option<SizeThresholds>) -> Option<SizeBucket> {
    let SizeThresholds { p50, p90 } = thresholds?;
    if amount >= p90 {
        Some(SizeBucket::Large)
    } else if amount >= p50 {
        Some(SizeBucket::Medium)
    } else {
        Some(SizeBucket::Small)
    }
}

/// Last-print step function over `[start_us, end_us)`, divided by the full bucket.
///
/// Prefix before the first print is skipped from the numerator. After the last
/// print the price is held to the right edge. Equal timestamps keep the last
/// print. No prints → None. Does not carry a prior from the previous minute.
pub fn exact_twap_one_bucket(
    timestamps_us: &[i64],
    prices: &[f64],
    start_us: i64,
    end_us: i64,
) -> Result<Option<f64>> {
    if end_us <= start_us {
        bail!("TWAP bucket end must be after start");
    }
    let mut ordered: Vec<(i64, f64)> = timestamps_us
        .iter()
        .zip(prices.iter())
        .filter_map(|(&ts_us, &price)| {
            if ts_us < start_us || ts_us >= end_us || !(price > 0.0) {
                None
            } else {
                Some((ts_us, price))
            }
        })
        .collect();
    if ordered.is_empty() {
        return Ok(None);
    }
    ordered.sort_by_key(|(ts, _)| *ts);
    let mut unique: Vec<(i64, f64)> = Vec::with_capacity(ordered.len());
    for (ts_us, price) in ordered {
        if let Some(last) = unique.last_mut() {
            if last.0 == ts_us {
                *last = (ts_us, price);
                continue;
            }
        }
        unique.push((ts_us, price));
    }
    let mut weighted = 0.0;
    let (mut prev_t, mut prev_px) = unique[0];
    for &(ts_us, price) in unique.iter().skip(1) {
        weighted += prev_px * (ts_us - prev_t) as f64;
        prev_t = ts_us;
        prev_px = price;
    }
    weighted += prev_px * (end_us - prev_t) as f64;
    Ok(Some(weighted / (end_us - start_us) as f64))
}

struct MinuteAcc {
    ric: String,
    ts_ns: u64,
    contract_id: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: f64,
    amount: f64,
    count: f64,
    buy_count: f64,
    sell_count: f64,
    implied_count: f64,
    buy_volume: f64,
    sell_volume: f64,
    implied_volume: f64,
    buy_amount: f64,
    sell_amount: f64,
    implied_amount: f64,
    buy_high: Option<f64>,
    sell_low: Option<f64>,
    large_order: f64,
    medium_order: f64,
    small_order: f64,
    large_buy: f64,
    large_sell: f64,
    medium_buy: f64,
    medium_sell: f64,
    small_buy: f64,
    small_sell: f64,
    all_ts_us: Vec<i64>,
    all_px: Vec<f64>,
    buy_ts_us: Vec<i64>,
    buy_px: Vec<f64>,
    sell_ts_us: Vec<i64>,
    sell_px: Vec<f64>,
    implied_ts_us: Vec<i64>,
    implied_px: Vec<f64>,
    special_count: f64,
    special_volume: f64,
}

impl MinuteAcc {
    fn new(ric: String, ts_ns: u64, contract_id: String) -> Self {
        Self {
            ric,
            ts_ns,
            contract_id,
            open: None,
            high: None,
            low: None,
            close: None,
            volume: 0.0,
            amount: 0.0,
            count: 0.0,
            buy_count: 0.0,
            sell_count: 0.0,
            implied_count: 0.0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            implied_volume: 0.0,
            buy_amount: 0.0,
            sell_amount: 0.0,
            implied_amount: 0.0,
            buy_high: None,
            sell_low: None,
            large_order: 0.0,
            medium_order: 0.0,
            small_order: 0.0,
            large_buy: 0.0,
            large_sell: 0.0,
            medium_buy: 0.0,
            medium_sell: 0.0,
            small_buy: 0.0,
            small_sell: 0.0,
            all_ts_us: Vec::new(),
            all_px: Vec::new(),
            buy_ts_us: Vec::new(),
            buy_px: Vec::new(),
            sell_ts_us: Vec::new(),
            sell_px: Vec::new(),
            implied_ts_us: Vec::new(),
            implied_px: Vec::new(),
            special_count: 0.0,
            special_volume: 0.0,
        }
    }

    fn add_trade(&mut self, rec: &SlimTrade, thresholds: Option<SizeThresholds>) -> Result<()> {
        let price = price_e9_to_f64(rec.price)
            .ok_or_else(|| anyhow!("{} printable trade missing Price", rec.ric))?;
        if rec.volume == MISSING_VOLUME {
            bail!("{} printable trade missing Volume", rec.ric);
        }
        let volume = f64::from(rec.volume);
        let amount = price * volume;
        let ts_us = i64::try_from(rec.ts_utc_ns / NS_PER_US)
            .map_err(|_| anyhow!("{} ts overflowed i64 us", rec.ric))?;
        match self.open {
            None => {
                self.open = Some(price);
                self.high = Some(price);
                self.low = Some(price);
            }
            Some(_) => {
                self.high = Some(self.high.unwrap().max(price));
                self.low = Some(self.low.unwrap().min(price));
            }
        }
        self.close = Some(price);
        self.volume += volume;
        self.amount += amount;
        self.count += 1.0;
        self.all_ts_us.push(ts_us);
        self.all_px.push(price);
        let bucket = classify_size(amount, thresholds);
        match bucket {
            Some(SizeBucket::Large) => self.large_order += amount,
            Some(SizeBucket::Medium) => self.medium_order += amount,
            Some(SizeBucket::Small) => self.small_order += amount,
            None => {}
        }
        match rec.aggressor {
            1 => {
                self.buy_count += 1.0;
                self.buy_volume += volume;
                self.buy_amount += amount;
                self.buy_high = Some(self.buy_high.map_or(price, |high| high.max(price)));
                self.buy_ts_us.push(ts_us);
                self.buy_px.push(price);
                match bucket {
                    Some(SizeBucket::Large) => self.large_buy += amount,
                    Some(SizeBucket::Medium) => self.medium_buy += amount,
                    Some(SizeBucket::Small) => self.small_buy += amount,
                    None => {}
                }
            }
            2 => {
                self.sell_count += 1.0;
                self.sell_volume += volume;
                self.sell_amount += amount;
                self.sell_low = Some(self.sell_low.map_or(price, |low| low.min(price)));
                self.sell_ts_us.push(ts_us);
                self.sell_px.push(price);
                match bucket {
                    Some(SizeBucket::Large) => self.large_sell += amount,
                    Some(SizeBucket::Medium) => self.medium_sell += amount,
                    Some(SizeBucket::Small) => self.small_sell += amount,
                    None => {}
                }
            }
            0 => {
                self.implied_count += 1.0;
                self.implied_volume += volume;
                self.implied_amount += amount;
                self.implied_ts_us.push(ts_us);
                self.implied_px.push(price);
            }
            other => bail!("{} aggressor {other} is not 0/1/2", rec.ric),
        }
        Ok(())
    }

    fn add_special(&mut self, rec: &SlimTrade) -> Result<()> {
        if rec.price != MISSING_PRICE {
            bail!("{} special unexpectedly has Price", rec.ric);
        }
        if rec.volume == MISSING_VOLUME {
            bail!("{} special missing Volume", rec.ric);
        }
        if rec.aggressor != 0 {
            bail!("{} special unexpectedly has aggressor", rec.ric);
        }
        self.special_count += 1.0;
        self.special_volume += f64::from(rec.volume);
        Ok(())
    }

    fn close_row(self) -> Result<DropSpecialMinute> {
        let start_us = i64::try_from(self.ts_ns / NS_PER_US)
            .map_err(|_| anyhow!("{} minute overflowed i64 us", self.ric))?;
        let end_us = start_us + US_PER_MINUTE;
        let net_buy_amount = self.buy_amount - self.sell_amount;
        let directed = self.buy_amount + self.sell_amount;
        Ok(DropSpecialMinute {
            contract_id: self.contract_id,
            ric: self.ric,
            ts: i64::try_from(self.ts_ns / NS_PER_SEC)
                .map_err(|_| anyhow!("minute ts overflowed i64"))?,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            amount: self.amount,
            count: self.count,
            special_count: self.special_count,
            special_volume: self.special_volume,
            avg_amount: if self.count == 0.0 {
                0.0
            } else {
                self.amount / self.count
            },
            vwap: ratio(self.amount, self.volume),
            twap: exact_twap_one_bucket(&self.all_ts_us, &self.all_px, start_us, end_us)?,
            buy_count: self.buy_count,
            sell_count: self.sell_count,
            buy_volume: self.buy_volume,
            sell_volume: self.sell_volume,
            buy_amount: self.buy_amount,
            sell_amount: self.sell_amount,
            buy_vwap: ratio(self.buy_amount, self.buy_volume),
            sell_vwap: ratio(self.sell_amount, self.sell_volume),
            buy_twap: exact_twap_one_bucket(&self.buy_ts_us, &self.buy_px, start_us, end_us)?,
            sell_twap: exact_twap_one_bucket(&self.sell_ts_us, &self.sell_px, start_us, end_us)?,
            buy_high: self.buy_high,
            sell_low: self.sell_low,
            net_buy_amount,
            net_buy_volume: self.buy_volume - self.sell_volume,
            net_buy_pct: ratio(net_buy_amount, directed),
            implied_count: self.implied_count,
            implied_volume: self.implied_volume,
            implied_amount: self.implied_amount,
            implied_vwap: ratio(self.implied_amount, self.implied_volume),
            implied_twap: exact_twap_one_bucket(
                &self.implied_ts_us,
                &self.implied_px,
                start_us,
                end_us,
            )?,
            large_order: self.large_order,
            medium_order: self.medium_order,
            small_order: self.small_order,
            large_buy: self.large_buy,
            large_sell: self.large_sell,
            medium_buy: self.medium_buy,
            medium_sell: self.medium_sell,
            small_buy: self.small_buy,
            small_sell: self.small_sell,
            net_buy_large: self.large_buy - self.large_sell,
            net_buy_medium: self.medium_buy - self.medium_sell,
            net_buy_small: self.small_buy - self.small_sell,
        })
    }
}

fn contract_id_for(rec: &SlimTrade) -> Result<String> {
    let decade_base = decade_base_from_utc_ns(rec.ts_utc_ns)?;
    parse_contract_id(&rec.ric, decade_base)?
        .map(|(_, _, contract_id)| contract_id)
        .ok_or_else(|| anyhow!("RIC {} is not a research-root expiry", rec.ric))
}

fn bar_for<'a>(
    acc: &'a mut BTreeMap<(String, u64), MinuteAcc>,
    rec: &SlimTrade,
) -> Result<&'a mut MinuteAcc> {
    let minute = minute_left_edge_ns(rec.ts_utc_ns);
    let key = (rec.ric.clone(), minute);
    if !acc.contains_key(&key) {
        acc.insert(
            key.clone(),
            MinuteAcc::new(rec.ric.clone(), minute, contract_id_for(rec)?),
        );
    }
    Ok(acc.get_mut(&key).expect("bar just inserted"))
}

/// Aggregate printable trades and Specials onto UTC minute-left-edge bars.
///
/// Printable `cme_trade` owns OHLC, VWAP/TWAP, direction, size buckets.
/// `cme_special` only adds `special_count` / `special_volume`.
pub fn synthesize_drop_special_1min(
    trades: &[SlimTrade],
    specials: &[SlimTrade],
    thresholds: Option<SizeThresholds>,
) -> Result<Vec<DropSpecialMinute>> {
    let mut acc: BTreeMap<(String, u64), MinuteAcc> = BTreeMap::new();
    let mut last_trade_ts: BTreeMap<&str, u64> = BTreeMap::new();
    for rec in trades {
        if rec.price == MISSING_PRICE || rec.volume == MISSING_VOLUME {
            bail!("{} printable trade missing Price or Volume", rec.ric);
        }
        if let Some(&prev) = last_trade_ts.get(rec.ric.as_str()) {
            if rec.ts_utc_ns < prev {
                bail!("{} trades are not non-decreasing in ts_utc_ns", rec.ric);
            }
        }
        last_trade_ts.insert(rec.ric.as_str(), rec.ts_utc_ns);
        bar_for(&mut acc, rec)?.add_trade(rec, thresholds)?;
    }
    let mut last_special_ts: BTreeMap<&str, u64> = BTreeMap::new();
    for rec in specials {
        if rec.price != MISSING_PRICE {
            bail!("{} special unexpectedly has Price", rec.ric);
        }
        if let Some(&prev) = last_special_ts.get(rec.ric.as_str()) {
            if rec.ts_utc_ns < prev {
                bail!("{} specials are not non-decreasing in ts_utc_ns", rec.ric);
            }
        }
        last_special_ts.insert(rec.ric.as_str(), rec.ts_utc_ns);
        bar_for(&mut acc, rec)?.add_special(rec)?;
    }
    acc.into_values().map(MinuteAcc::close_row).collect()
}

fn float_series(name: &str, values: Vec<Option<f64>>) -> Series {
    if NULLABLE_FLOAT.contains(&name) {
        Series::new(name.into(), values)
    } else {
        Series::new(
            name.into(),
            values
                .into_iter()
                .map(|value| value.unwrap_or(0.0))
                .collect::<Vec<f64>>(),
        )
    }
}

pub fn drop_special_minutes_to_dataframe(rows: &[DropSpecialMinute]) -> Result<DataFrame> {
    let n = rows.len();
    let mut contract_id = Vec::with_capacity(n);
    let mut ric = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    let mut columns: BTreeMap<&str, Vec<Option<f64>>> = BTreeMap::new();
    for name in OUTPUT_COLUMNS.iter().copied().skip(3) {
        columns.insert(name, Vec::with_capacity(n));
    }
    for row in rows {
        contract_id.push(row.contract_id.clone());
        ric.push(row.ric.clone());
        ts.push(row.ts);
        let values: [(&str, Option<f64>); 44] = [
            ("open", row.open),
            ("high", row.high),
            ("low", row.low),
            ("close", row.close),
            ("volume", Some(row.volume)),
            ("amount", Some(row.amount)),
            ("count", Some(row.count)),
            ("special_count", Some(row.special_count)),
            ("special_volume", Some(row.special_volume)),
            ("avg_amount", Some(row.avg_amount)),
            ("vwap", row.vwap),
            ("twap", row.twap),
            ("buy_count", Some(row.buy_count)),
            ("sell_count", Some(row.sell_count)),
            ("buy_volume", Some(row.buy_volume)),
            ("sell_volume", Some(row.sell_volume)),
            ("buy_amount", Some(row.buy_amount)),
            ("sell_amount", Some(row.sell_amount)),
            ("buy_vwap", row.buy_vwap),
            ("sell_vwap", row.sell_vwap),
            ("buy_twap", row.buy_twap),
            ("sell_twap", row.sell_twap),
            ("buy_high", row.buy_high),
            ("sell_low", row.sell_low),
            ("net_buy_amount", Some(row.net_buy_amount)),
            ("net_buy_volume", Some(row.net_buy_volume)),
            ("net_buy_pct", row.net_buy_pct),
            ("implied_count", Some(row.implied_count)),
            ("implied_volume", Some(row.implied_volume)),
            ("implied_amount", Some(row.implied_amount)),
            ("implied_vwap", row.implied_vwap),
            ("implied_twap", row.implied_twap),
            ("large_order", Some(row.large_order)),
            ("medium_order", Some(row.medium_order)),
            ("small_order", Some(row.small_order)),
            ("large_buy", Some(row.large_buy)),
            ("large_sell", Some(row.large_sell)),
            ("medium_buy", Some(row.medium_buy)),
            ("medium_sell", Some(row.medium_sell)),
            ("small_buy", Some(row.small_buy)),
            ("small_sell", Some(row.small_sell)),
            ("net_buy_large", Some(row.net_buy_large)),
            ("net_buy_medium", Some(row.net_buy_medium)),
            ("net_buy_small", Some(row.net_buy_small)),
        ];
        for (name, value) in values {
            columns.get_mut(name).expect(name).push(value);
        }
    }
    let mut series = vec![
        Series::new("contract_id".into(), contract_id),
        Series::new("ric".into(), ric),
        Series::new("ts".into(), ts),
    ];
    for name in OUTPUT_COLUMNS.iter().copied().skip(3) {
        series.push(float_series(name, columns.remove(name).expect(name)));
    }
    DataFrame::new(series).context("build drop_special 1min dataframe")
}

pub fn write_drop_special_parquet(path: &Path, rows: &[DropSpecialMinute]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create parquet parent {}", parent.display()))?;
        }
    }
    let mut df = drop_special_minutes_to_dataframe(rows)?;
    let tmp = path.with_extension("parquet.tmp");
    let file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    ParquetWriter::new(file)
        .finish(&mut df)
        .with_context(|| format!("write parquet {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

pub fn chicago_trade_date_yyyymmdd(ts_sec: i64) -> Result<u32> {
    let ns = u64::try_from(ts_sec)
        .ok()
        .and_then(|sec| sec.checked_mul(NS_PER_SEC))
        .ok_or_else(|| anyhow!("ts {ts_sec} overflowed ns"))?;
    tradeday_yyyymmdd(ns)
}

pub fn route_for_row(row: &DropSpecialMinute) -> Result<(String, String, u32)> {
    let ns = u64::try_from(row.ts)
        .ok()
        .and_then(|sec| sec.checked_mul(NS_PER_SEC))
        .ok_or_else(|| anyhow!("ts {} overflowed ns", row.ts))?;
    let decade_base = decade_base_from_utc_ns(ns)?;
    let (exchange, product, _) = parse_contract_id(&row.ric, decade_base)?
        .ok_or_else(|| anyhow!("RIC {} is not a research-root expiry", row.ric))?;
    Ok((exchange, product, tradeday_yyyymmdd(ns)?))
}

pub fn trade_notional_f64(price_e9: i64, volume: u32) -> Result<f64> {
    if price_e9 == MISSING_PRICE {
        bail!("printable trade missing Price");
    }
    if volume == MISSING_VOLUME {
        bail!("printable trade missing Volume");
    }
    Ok((price_e9 as f64 / PRICE_SCALE as f64) * f64::from(volume))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_date_time_ns, parse_price_e9, MISSING_EXCH_HMS_NS};

    const MINUTE: &str = "2026-01-02T00:00:00Z";

    fn trade(offset_s: u64, price: &str, volume: u32, aggressor: u8) -> SlimTrade {
        SlimTrade {
            ric: "CLG24".to_string(),
            ts_utc_ns: parse_date_time_ns(MINUTE).unwrap() + offset_s * NS_PER_SEC,
            exch_hms_ns: MISSING_EXCH_HMS_NS,
            price: parse_price_e9(price).unwrap(),
            volume,
            bid: MISSING_PRICE,
            bid_size: MISSING_VOLUME,
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            aggressor,
        }
    }

    fn special(offset_s: u64, volume: u32) -> SlimTrade {
        SlimTrade {
            ric: "CLG24".to_string(),
            ts_utc_ns: parse_date_time_ns(MINUTE).unwrap() + offset_s * NS_PER_SEC,
            exch_hms_ns: MISSING_EXCH_HMS_NS,
            price: MISSING_PRICE,
            volume,
            bid: MISSING_PRICE,
            bid_size: MISSING_VOLUME,
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            aggressor: 0,
        }
    }

    fn almost(left: f64, right: f64) {
        assert!(
            (left - right).abs() < 1e-12,
            "{left} != {right}"
        );
    }

    #[test]
    fn exact_twap_skips_prefix_and_divides_by_full_minute() {
        let twap = exact_twap_one_bucket(&[1_000_000, 50_000_000], &[100.0, 110.0], 0, 60_000_000)
            .unwrap()
            .unwrap();
        almost(twap, (100.0 * 49.0 + 110.0 * 10.0) / 60.0);
    }

    #[test]
    fn exact_twap_equal_timestamp_keeps_last() {
        let twap = exact_twap_one_bucket(
            &[10_000_000, 10_000_000, 40_000_000],
            &[100.0, 120.0, 130.0],
            0,
            60_000_000,
        )
        .unwrap()
        .unwrap();
        almost(twap, (120.0 * 30.0 + 130.0 * 20.0) / 60.0);
    }

    #[test]
    fn identities_and_implied_split() {
        let rows = synthesize_drop_special_1min(
            &[
                trade(1, "100", 1, 1),
                trade(10, "101", 2, 2),
                trade(20, "102", 4, 0),
            ],
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.contract_id, "NYMEX:CL:2024-02");
        assert_eq!(row.ric, "CLG24");
        assert_eq!(row.open, Some(100.0));
        assert_eq!(row.high, Some(102.0));
        assert_eq!(row.low, Some(100.0));
        assert_eq!(row.close, Some(102.0));
        almost(row.volume, 7.0);
        almost(row.count, 3.0);
        almost(row.amount, 100.0 + 202.0 + 408.0);
        almost(row.volume, row.buy_volume + row.sell_volume + row.implied_volume);
        almost(row.count, row.buy_count + row.sell_count + row.implied_count);
        almost(row.amount, row.buy_amount + row.sell_amount + row.implied_amount);
        almost(row.special_count, 0.0);
        almost(row.special_volume, 0.0);
        almost(row.twap.unwrap(), (100.0 * 9.0 + 101.0 * 10.0 + 102.0 * 40.0) / 60.0);
        almost(row.buy_twap.unwrap(), 100.0 * 59.0 / 60.0);
        almost(row.sell_twap.unwrap(), 101.0 * 50.0 / 60.0);
        almost(row.implied_twap.unwrap(), 102.0 * 40.0 / 60.0);
    }

    #[test]
    fn special_fills_only_count_and_volume() {
        let rows =
            synthesize_drop_special_1min(&[trade(1, "100", 1, 1)], &[special(2, 5)], None).unwrap();
        let row = &rows[0];
        almost(row.special_count, 1.0);
        almost(row.special_volume, 5.0);
        almost(row.volume, 1.0);
        almost(row.count, 1.0);
        almost(row.amount, 100.0);
        assert_eq!(row.open, Some(100.0));
        almost(row.implied_volume, 0.0);
    }

    #[test]
    fn special_only_minute_has_null_ohlc() {
        let rows = synthesize_drop_special_1min(&[], &[special(3, 2)], None).unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        almost(row.special_count, 1.0);
        almost(row.special_volume, 2.0);
        almost(row.volume, 0.0);
        almost(row.count, 0.0);
        assert!(row.open.is_none());
        assert!(row.vwap.is_none());
        assert!(row.twap.is_none());
        assert!(row.buy_vwap.is_none());
        assert!(row.implied_vwap.is_none());
    }

    #[test]
    fn size_buckets_skip_implied_on_directed_columns() {
        let rows = synthesize_drop_special_1min(
            &[
                trade(1, "10", 1, 1),
                trade(2, "25", 2, 2),
                trade(3, "50", 4, 0),
            ],
            &[],
            Some(SizeThresholds {
                p50: 20.0,
                p90: 100.0,
            }),
        )
        .unwrap();
        let row = &rows[0];
        almost(row.small_order, 10.0);
        almost(row.medium_order, 50.0);
        almost(row.large_order, 200.0);
        almost(row.small_buy, 10.0);
        almost(row.medium_sell, 50.0);
        almost(row.large_buy, 0.0);
        almost(row.large_sell, 0.0);
        almost(row.net_buy_small, 10.0);
        almost(row.net_buy_medium, -50.0);
        almost(row.net_buy_large, 0.0);
    }

    #[test]
    fn empty_minutes_are_not_emitted() {
        let rows =
            synthesize_drop_special_1min(&[trade(0, "100", 1, 1), trade(120, "101", 1, 1)], &[], None)
                .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].ts - rows[0].ts, 120);
    }

    #[test]
    fn parquet_keeps_null_ohlc_on_special_only() {
        let rows = synthesize_drop_special_1min(
            &[trade(1, "100", 1, 1)],
            &[special(120, 2)],
            None,
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("drop_special.parquet");
        write_drop_special_parquet(&path, &rows).unwrap();
        use polars::prelude::{ParquetReader, SerReader};
        let file = std::fs::File::open(&path).unwrap();
        let df = ParquetReader::new(file).finish().unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(
            df.get_column_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            OUTPUT_COLUMNS
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
        assert!(df.column("open").unwrap().f64().unwrap().get(1).is_none());
        assert_eq!(
            df.column("special_volume")
                .unwrap()
                .f64()
                .unwrap()
                .get(1)
                .unwrap(),
            2.0
        );
    }
}
