//! CME TAS 1-minute, 60-column supervision labels.
//!
//! Input minute prices remain sparse: printable trades supply TWAP/VWAP and
//! exported 1-second backtest books supply minute-end midprice.  The output
//! row at `ts=t` only exposes the closed bucket `[t-60, t)`.

use anyhow::{bail, Context, Result};
use polars::prelude::{DataFrame, NamedFrom, ParquetWriter, Series};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use crate::drop_special_1min::DropSpecialMinute;

pub const BENCHMARKS: [&str; 3] = ["twap", "vwap", "midp"];
pub const HORIZON_MINUTES: [i64; 5] = [5, 15, 30, 60, 240];
pub const VOLATILITY_WINDOW: usize = 30;
pub const RANK_WINDOW: usize = 1440;
pub const LABEL_COUNT: usize = 60;

#[derive(Debug, Clone, Copy, Default)]
pub struct CausalPrices {
    pub twap: Option<f64>,
    pub vwap: Option<f64>,
    pub midp: Option<f64>,
}

impl CausalPrices {
    fn benchmark(self, index: usize) -> Option<f64> {
        match index {
            0 => self.twap,
            1 => self.vwap,
            2 => self.midp,
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct YlabelRow {
    pub contract_id: String,
    pub ric: String,
    pub ts: i64,
    pub labels: [Option<f64>; LABEL_COUNT],
}

fn valid_price(value: Option<f64>) -> Option<f64> {
    value.filter(|price| price.is_finite() && *price > 0.0)
}

fn label_index(benchmark: usize, horizon: usize, variant: usize) -> usize {
    benchmark * HORIZON_MINUTES.len() * 4 + horizon * 4 + variant
}

pub fn ylabel_columns() -> Vec<String> {
    let mut columns = Vec::with_capacity(LABEL_COUNT);
    for benchmark in BENCHMARKS {
        for horizon in HORIZON_MINUTES {
            for variant in ["chg", "dir", "vol30", "re"] {
                columns.push(format!("{benchmark}_{variant}_{horizon}m"));
            }
        }
    }
    columns
}

/// Move real-minute benchmark prices to their first causal decision timestamp.
/// `trade_rows` must be the raw sparse result of `synthesize_drop_special_1min`
/// with no session fill. `midp_minutes` is keyed by raw UTC minute left edge.
pub fn causal_prices_from_minutes(
    trade_rows: &[DropSpecialMinute],
    midp_minutes: &BTreeMap<i64, f64>,
) -> Result<BTreeMap<i64, CausalPrices>> {
    let mut output = BTreeMap::new();
    for row in trade_rows {
        if row.ts.rem_euclid(60) != 0 {
            bail!("{} trade minute {} is not minute-aligned", row.ric, row.ts);
        }
        let target = row.ts + 60;
        let entry = output.entry(target).or_insert_with(CausalPrices::default);
        entry.twap = valid_price(row.twap);
        entry.vwap = valid_price(row.vwap);
    }
    for (&minute, &midp) in midp_minutes {
        if minute.rem_euclid(60) != 0 {
            bail!("backtest minute {minute} is not minute-aligned");
        }
        if !midp.is_finite() || midp <= 0.0 {
            continue;
        }
        output
            .entry(minute + 60)
            .or_insert_with(CausalPrices::default)
            .midp = Some(midp);
    }
    Ok(output)
}

fn horizon_returns(prices: &BTreeMap<i64, f64>, horizon_seconds: i64) -> BTreeMap<i64, f64> {
    let mut output = BTreeMap::new();
    for (&ts, &price) in prices {
        let Some(&prior) = prices.get(&(ts - horizon_seconds)) else {
            continue;
        };
        if price.is_finite() && price > 0.0 && prior.is_finite() && prior > 0.0 {
            output.insert(ts, price / prior - 1.0);
        }
    }
    output
}

fn window_values(returns: &BTreeMap<i64, f64>, end_ts: i64, width: usize) -> Option<Vec<f64>> {
    let start = end_ts - i64::try_from(width.checked_sub(1)?).ok()? * 60;
    let mut values = Vec::with_capacity(width);
    for index in 0..width {
        let ts = start + i64::try_from(index).ok()? * 60;
        let value = *returns.get(&ts)?;
        if !value.is_finite() {
            return None;
        }
        values.push(value);
    }
    Some(values)
}

fn sample_std(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let sum_sq = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>();
    Some((sum_sq / (values.len() - 1) as f64).sqrt())
}

fn average_rank(values: &[f64]) -> Option<f64> {
    let &current = values.last()?;
    let less = values.iter().filter(|&&value| value < current).count();
    let equal = values.iter().filter(|&&value| value == current).count();
    Some(1.0 + less as f64 + (equal.saturating_sub(1) as f64) / 2.0)
}

/// Build the 60 future labels from causal sparse minute prices.
///
/// Rolling windows advance one clock minute at a time, matching the reference
/// `pandas.Series.rolling(...).shift(-h)` implementation. Missing minutes are
/// never forward-filled and invalidate a window.
pub fn build_ylabel_rows(
    contract_id: &str,
    ric: &str,
    prices_by_ts: &BTreeMap<i64, CausalPrices>,
) -> Vec<YlabelRow> {
    let benchmark_prices = std::array::from_fn::<_, 3, _>(|benchmark| {
        prices_by_ts
            .iter()
            .filter_map(|(&ts, prices)| prices.benchmark(benchmark).map(|price| (ts, price)))
            .filter(|(_, price)| price.is_finite() && *price > 0.0)
            .collect::<BTreeMap<_, _>>()
    });
    let returns = std::array::from_fn::<_, 3, _>(|benchmark| {
        std::array::from_fn::<_, 5, _>(|horizon| {
            horizon_returns(&benchmark_prices[benchmark], HORIZON_MINUTES[horizon] * 60)
        })
    });

    prices_by_ts
        .keys()
        .copied()
        .map(|ts| {
            let mut labels = [None; LABEL_COUNT];
            for benchmark in 0..BENCHMARKS.len() {
                for horizon in 0..HORIZON_MINUTES.len() {
                    let end_ts = ts + HORIZON_MINUTES[horizon] * 60;
                    let return_map = &returns[benchmark][horizon];
                    let future = return_map.get(&end_ts).copied();
                    labels[label_index(benchmark, horizon, 0)] = future;
                    labels[label_index(benchmark, horizon, 1)] =
                        future.map(|value| f64::from(value > 0.0));
                    labels[label_index(benchmark, horizon, 2)] =
                        window_values(return_map, end_ts, VOLATILITY_WINDOW)
                            .and_then(|values| sample_std(&values));
                    labels[label_index(benchmark, horizon, 3)] =
                        window_values(return_map, end_ts, RANK_WINDOW)
                            .and_then(|values| average_rank(&values));
                }
            }
            YlabelRow {
                contract_id: contract_id.to_string(),
                ric: ric.to_string(),
                ts,
                labels,
            }
        })
        .collect()
}

pub fn rows_to_dataframe(rows: &[YlabelRow]) -> Result<DataFrame> {
    let mut contract_id = Vec::with_capacity(rows.len());
    let mut ric = Vec::with_capacity(rows.len());
    let mut ts = Vec::with_capacity(rows.len());
    let mut values = vec![Vec::with_capacity(rows.len()); LABEL_COUNT];
    for row in rows {
        contract_id.push(row.contract_id.clone());
        ric.push(row.ric.clone());
        ts.push(row.ts);
        for (index, value) in row.labels.iter().enumerate() {
            values[index].push(*value);
        }
    }
    let mut columns = vec![
        Series::new("contract_id".into(), contract_id),
        Series::new("ric".into(), ric),
        Series::new("ts".into(), ts),
    ];
    for (name, values) in ylabel_columns().into_iter().zip(values) {
        columns.push(Series::new(name.into(), values));
    }
    DataFrame::new(columns).context("build CME ylabel 1m dataframe")
}

pub fn write_ylabel_parquet(path: &Path, rows: &[YlabelRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create parquet parent {}", parent.display()))?;
        }
    }
    let mut df = rows_to_dataframe(rows)?;
    let tmp = path.with_extension("parquet.tmp");
    let file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    ParquetWriter::new(file)
        .finish(&mut df)
        .with_context(|| format!("write parquet {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prices() -> CausalPrices {
        CausalPrices {
            twap: Some(100.0),
            vwap: Some(100.0),
            midp: Some(100.0),
        }
    }

    #[test]
    fn future_return_has_no_extra_causal_shift() {
        let mut input = BTreeMap::new();
        input.insert(0, prices());
        input.insert(
            5 * 60,
            CausalPrices {
                twap: Some(104.0),
                vwap: Some(104.0),
                midp: Some(104.0),
            },
        );
        let rows = build_ylabel_rows("NYMEX:CL:2024-02", "CLG24", &input);
        assert_eq!(ylabel_columns().len(), LABEL_COUNT);
        assert!((rows[0].labels[label_index(2, 0, 0)].unwrap() - 0.04).abs() < 1e-12);
        assert_eq!(rows[0].labels[label_index(2, 0, 1)], Some(1.0));
        assert_eq!(rows[1].labels[label_index(2, 0, 0)], None);
    }

    #[test]
    fn rank_is_average_rank_and_clock_gaps_invalidate_windows() {
        let mut input = BTreeMap::new();
        for minute in -1445..=5 {
            input.insert(minute * 60, prices());
        }
        let rows = build_ylabel_rows("NYMEX:CL:2024-02", "CLG24", &input);
        let row = rows.iter().find(|row| row.ts == 0).unwrap();
        assert_eq!(row.labels[label_index(2, 0, 2)], Some(0.0));
        assert_eq!(row.labels[label_index(2, 0, 3)], Some(720.5));

        input.remove(&(-3 * 60));
        let sparse_rows = build_ylabel_rows("NYMEX:CL:2024-02", "CLG24", &input);
        let sparse_row = sparse_rows.iter().find(|row| row.ts == 0).unwrap();
        assert_eq!(sparse_row.labels[label_index(2, 0, 2)], None);
    }

    #[test]
    fn causal_prices_do_not_fill_missing_trade_minutes() {
        let mut midps = BTreeMap::new();
        midps.insert(0, 100.0);
        midps.insert(120, 102.0);
        let prices = causal_prices_from_minutes(&[], &midps).unwrap();
        assert!(prices.contains_key(&60));
        assert!(prices.contains_key(&180));
        assert!(!prices.contains_key(&120));
    }

    #[test]
    fn parquet_frame_has_the_documented_identity_and_label_order() {
        let mut input = BTreeMap::new();
        input.insert(0, prices());
        let rows = build_ylabel_rows("NYMEX:CL:2024-02", "CLG24", &input);
        let frame = rows_to_dataframe(&rows).unwrap();
        let columns = frame.get_column_names();
        assert_eq!(&columns[..3], ["contract_id", "ric", "ts"]);
        assert_eq!(columns.len(), 3 + LABEL_COUNT);
        assert_eq!(columns[3], "twap_chg_5m");
        assert_eq!(columns.last().unwrap().as_str(), "midp_re_240m");
    }
}
