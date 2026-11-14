//! Additive back-adjusted (hfq) continuous 1-minute series.
//!
//! Latest dominant segment stays unmoved. Earlier segments add every later
//! `prev_close_spread`. Volume / amount / counts are not adjusted. Book prices
//! shift with the same additive gap; sizes stay raw.

use anyhow::{bail, Context, Result};
use chrono::{Datelike, NaiveDate};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::baseline_1min::{BaselineMinute, Book5};

#[derive(Clone, Debug)]
pub struct DominantDay {
    pub trad_day: NaiveDate,
    pub product_id: String,
    pub instrument_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AdjustmentRow {
    pub product_id: String,
    pub effective_trading_day: NaiveDate,
    pub adjustment_value: Option<f64>,
    pub skipped: bool,
}

pub fn normalize_instrument(id: &str) -> String {
    id.trim().to_ascii_lowercase()
}

/// Expand a Zhengzhou 3-digit `YMM` code to 4-digit `YYMM` using `trad_day`.
/// Already-4-digit codes are returned unchanged. Matches Python
/// `cn_futures.instrument.canonicalize_instrument_id`.
pub fn canonicalize_instrument_id(instrument_id: &str, trad_day: NaiveDate) -> String {
    let text = instrument_id.trim();
    let mut letters = String::new();
    let mut digits = String::new();
    let mut seen_digit = false;
    for ch in text.chars() {
        if ch.is_ascii_alphabetic() {
            if seen_digit {
                return text.to_string();
            }
            letters.push(ch.to_ascii_uppercase());
        } else if ch.is_ascii_digit() {
            seen_digit = true;
            digits.push(ch);
        } else {
            return text.to_string();
        }
    }
    if letters.is_empty() || digits.len() != 3 {
        return text.to_string();
    }
    let month: u32 = match digits[1..].parse() {
        Ok(m) if (1..=12).contains(&m) => m,
        _ => return text.to_string(),
    };
    let year_ones: i32 = digits[..1].parse().unwrap_or(0);
    let decade = (trad_day.year() / 10) * 10;
    let trad_months = trad_day.year() * 12 + trad_day.month() as i32;
    let mut best_year: Option<i32> = None;
    let mut best_key: Option<(i32, i32)> = None;
    for adj in [-10, 0, 10] {
        let year = decade + year_ones + adj;
        if !(1990..=2100).contains(&year) {
            continue;
        }
        let delta = year * 12 + month as i32 - trad_months;
        let key = (delta.abs(), if delta >= 0 { 0 } else { 1 });
        if best_key.map(|prev| key < prev).unwrap_or(true) {
            best_key = Some(key);
            best_year = Some(year);
        }
    }
    match best_year {
        Some(year) => format!("{letters}{:02}{month:02}", year % 100),
        None => text.to_string(),
    }
}

pub fn instrument_match_key(id: &str, trad_day: NaiveDate) -> String {
    normalize_instrument(&canonicalize_instrument_id(id, trad_day))
}

fn parse_csv_date(text: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d").with_context(|| format!("bad date {text:?}"))
}

pub fn load_dominants(path: &Path) -> Result<Vec<DominantDay>> {
    let mut reader =
        csv::Reader::from_path(path).with_context(|| format!("open {}", path.display()))?;
    let mut out = Vec::new();
    for rec in reader.records() {
        let rec = rec.with_context(|| format!("read {}", path.display()))?;
        let trad_day = parse_csv_date(rec.get(0).unwrap_or(""))?;
        let product_id = rec.get(1).unwrap_or("").trim().to_string();
        let instrument = rec.get(2).unwrap_or("").trim();
        out.push(DominantDay {
            trad_day,
            product_id,
            instrument_id: if instrument.is_empty() {
                None
            } else {
                Some(instrument.to_string())
            },
        });
    }
    Ok(out)
}

pub fn load_adjustments(path: &Path) -> Result<Vec<AdjustmentRow>> {
    let mut reader =
        csv::Reader::from_path(path).with_context(|| format!("open {}", path.display()))?;
    let mut out = Vec::new();
    for rec in reader.records() {
        let rec = rec.with_context(|| format!("read {}", path.display()))?;
        let skipped = rec.get(10).unwrap_or("0").trim() == "1";
        let value = rec.get(8).unwrap_or("").trim();
        out.push(AdjustmentRow {
            product_id: rec.get(0).unwrap_or("").trim().to_string(),
            effective_trading_day: parse_csv_date(rec.get(1).unwrap_or(""))?,
            adjustment_value: if value.is_empty() {
                None
            } else {
                Some(value.parse::<f64>()?)
            },
            skipped,
        });
    }
    Ok(out)
}

/// Additive shift that leaves the latest segment unmoved.
pub fn gap_before(factors: &[AdjustmentRow], trad_day: NaiveDate) -> Result<Option<f64>> {
    let mut shift = 0.0;
    for factor in factors {
        if trad_day < factor.effective_trading_day && factor.skipped {
            return Ok(None);
        }
        if factor.skipped {
            continue;
        }
        let Some(value) = factor.adjustment_value else {
            continue;
        };
        if trad_day < factor.effective_trading_day {
            shift += value;
        }
    }
    Ok(Some(shift))
}

fn shift_px(value: Option<f64>, gap: f64) -> Option<f64> {
    value.filter(|px| px.is_finite()).map(|px| px + gap)
}

fn shift_book(book: Option<Book5>, gap: f64) -> Option<Book5> {
    let mut book = book?;
    for i in 0..5 {
        book.bid_prices[i] = shift_px(book.bid_prices[i], gap);
        book.ask_prices[i] = shift_px(book.ask_prices[i], gap);
    }
    Some(book)
}

pub fn apply_hfq(row: &BaselineMinute, gap: f64) -> BaselineMinute {
    let mut out = row.clone();
    out.open = shift_px(row.open, gap);
    out.high = shift_px(row.high, gap);
    out.low = shift_px(row.low, gap);
    out.close = shift_px(row.close, gap);
    out.vwap = shift_px(row.vwap, gap);
    out.buy_vwap = shift_px(row.buy_vwap, gap);
    out.sell_vwap = shift_px(row.sell_vwap, gap);
    out.twap = shift_px(row.twap, gap);
    out.mid_price = shift_px(row.mid_price, gap);
    out.book = shift_book(row.book.clone(), gap);
    out
}

/// Apply one additive gap to a sorted contract-day series.
///
/// Raw TWAP divides by the full minute even when the first print of a trading
/// segment arrives after the minute opens. Without the uncovered duration, an
/// additive shift cannot reconstruct that partial TWAP. Blank the first
/// trade-bearing TWAP in each contiguous minute segment; after that print has
/// seeded the segment, adding the full gap is exact.
pub fn apply_hfq_series(rows: Vec<BaselineMinute>, gap: f64) -> Vec<BaselineMinute> {
    let mut out = Vec::with_capacity(rows.len());
    let mut previous_ts = None;
    let mut has_prior_trade = false;
    for row in rows {
        if previous_ts.is_none_or(|ts| row.ts != ts + 60) {
            has_prior_trade = false;
        }
        let has_trade = row.volume.is_finite() && row.volume > 0.0;
        let partial_twap = has_trade && !has_prior_trade;
        let ts = row.ts;
        let mut adjusted = apply_hfq(&row, gap);
        if partial_twap {
            adjusted.twap = None;
        }
        if has_trade {
            has_prior_trade = true;
        }
        previous_ts = Some(ts);
        out.push(adjusted);
    }
    out
}

pub fn index_dominants(days: &[DominantDay], product: &str) -> HashMap<NaiveDate, String> {
    let mut out = HashMap::new();
    for day in days {
        if day.product_id != product {
            continue;
        }
        if let Some(instrument) = &day.instrument_id {
            out.insert(day.trad_day, instrument.clone());
        }
    }
    out
}

pub fn list_roll_exchanges(root: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(ex) = name.strip_suffix("_dominant.csv") {
            out.push(ex.to_string());
        }
    }
    out.sort();
    if out.is_empty() {
        bail!("no *_dominant.csv under {}", root.display());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minute(close: f64, bid: f64, ask: f64) -> BaselineMinute {
        BaselineMinute {
            contract_id: "rb2305".into(),
            ts: 0,
            open: Some(close),
            high: Some(close + 1.0),
            low: Some(close - 1.0),
            close: Some(close),
            volume: 10.0,
            amount: 100.0,
            avg_amount: 10.0,
            count: 1.0,
            buy_count: 1.0,
            sell_count: 0.0,
            buy_amount: 100.0,
            sell_amount: 0.0,
            buy_volume: 10.0,
            sell_volume: 0.0,
            vwap: Some(close),
            buy_vwap: Some(close),
            sell_vwap: None,
            twap: Some(close),
            mid_price: Some((bid + ask) / 2.0),
            net_buy_amount: 100.0,
            net_buy_volume: 10.0,
            net_buy_pct: Some(1.0),
            large_order: 0.0,
            medium_order: 0.0,
            small_order: 0.0,
            large_buy: 0.0,
            large_sell: 0.0,
            medium_buy: 0.0,
            medium_sell: 0.0,
            small_buy: 0.0,
            small_sell: 0.0,
            net_buy_large: 0.0,
            net_buy_medium: 0.0,
            net_buy_small: 0.0,
            book: Some(Book5 {
                bid_prices: [Some(bid), None, None, None, None],
                bid_sizes: [Some(3.0), None, None, None, None],
                ask_prices: [Some(ask), None, None, None, None],
                ask_sizes: [Some(4.0), None, None, None, None],
            }),
        }
    }

    #[test]
    fn czce_three_digit_expands_after_overlap_cut() {
        let day = NaiveDate::from_ymd_opt(2025, 11, 4).unwrap();
        assert_eq!(canonicalize_instrument_id("AP601", day), "AP2601");
        assert_eq!(canonicalize_instrument_id("AP2601", day), "AP2601");
        assert_eq!(
            instrument_match_key("AP601", day),
            instrument_match_key("AP2601", day)
        );
        let early = NaiveDate::from_ymd_opt(2029, 12, 1).unwrap();
        assert_eq!(canonicalize_instrument_id("AP001", early), "AP3001");
    }

    #[test]
    fn latest_segment_unmoved() {
        let factors = [AdjustmentRow {
            product_id: "RB".into(),
            effective_trading_day: NaiveDate::from_ymd_opt(2023, 5, 16).unwrap(),
            adjustment_value: Some(20.0),
            skipped: false,
        }];
        let before = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        let after = NaiveDate::from_ymd_opt(2023, 5, 16).unwrap();
        assert_eq!(gap_before(&factors, before).unwrap(), Some(20.0));
        assert_eq!(gap_before(&factors, after).unwrap(), Some(0.0));
    }

    #[test]
    fn skipped_later_roll_blanks_history() {
        let factors = [
            AdjustmentRow {
                product_id: "RB".into(),
                effective_trading_day: NaiveDate::from_ymd_opt(2023, 5, 16).unwrap(),
                adjustment_value: Some(20.0),
                skipped: false,
            },
            AdjustmentRow {
                product_id: "RB".into(),
                effective_trading_day: NaiveDate::from_ymd_opt(2023, 10, 16).unwrap(),
                adjustment_value: None,
                skipped: true,
            },
        ];
        let early = NaiveDate::from_ymd_opt(2023, 5, 1).unwrap();
        assert!(gap_before(&factors, early).unwrap().is_none());
    }

    #[test]
    fn additive_shift_moves_prices_not_size() {
        let row = apply_hfq(&minute(100.0, 99.0, 101.0), 20.0);
        assert_eq!(row.close, Some(120.0));
        assert_eq!(row.high, Some(121.0));
        assert_eq!(row.mid_price, Some(120.0));
        assert_eq!(row.volume, 10.0);
        assert_eq!(row.book.as_ref().unwrap().bid_prices[0], Some(119.0));
        assert_eq!(row.book.as_ref().unwrap().bid_sizes[0], Some(3.0));
    }

    #[test]
    fn segmented_hfq_blanks_only_unseeded_partial_twap() {
        let mut first = minute(100.0, 99.0, 101.0);
        first.twap = Some(1.0);
        let mut seeded = minute(101.0, 100.0, 102.0);
        seeded.ts = 60;
        let mut after_gap = minute(102.0, 101.0, 103.0);
        after_gap.ts = 180;

        let rows = apply_hfq_series(vec![first, seeded, after_gap], 10.0);

        assert_eq!(rows[0].twap, None);
        assert_eq!(rows[1].twap, Some(111.0));
        assert_eq!(rows[2].twap, None);
        assert_eq!(rows[0].close, Some(110.0));
        assert_eq!(rows[2].close, Some(112.0));
    }
}
