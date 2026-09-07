//! Domestic 1-minute 60-column supervision labels.
//!
//! `P[t]` is the closed minute `[t-60, t)`. TWAP/VWAP come from sparse trade
//! minutes (no empty-minute carry). Mid comes from that minute's last book.
//! Empty minutes stay missing and invalidate rolling windows.

use crate::baseline_1min::BaselineMinute;
use std::collections::BTreeMap;

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
    pub ts: i64,
    pub labels: [Option<f64>; LABEL_COUNT],
}

fn valid_price(value: Option<f64>) -> Option<f64> {
    value.filter(|price| price.is_finite())
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

/// Shift observed minute prices one clock minute forward so `ts=t` is `[t-60, t)`.
pub fn causal_prices_from_minutes(rows: &[BaselineMinute]) -> BTreeMap<i64, CausalPrices> {
    let mut output = BTreeMap::new();
    for row in rows {
        // Carried empty minutes (volume=0, vwap forwarded) must not become P[t].
        let entry = output
            .entry(row.ts + 60)
            .or_insert_with(CausalPrices::default);
        if row.volume > 0.0 {
            if let Some(twap) = valid_price(row.twap) {
                entry.twap = Some(twap);
            }
            if let Some(vwap) = valid_price(row.vwap) {
                entry.vwap = Some(vwap);
            }
        }
        if let Some(midp) = valid_price(row.mid_price) {
            entry.midp = Some(midp);
        }
        if entry.twap.is_none() && entry.vwap.is_none() && entry.midp.is_none() {
            output.remove(&(row.ts + 60));
        }
    }
    output
}

fn horizon_returns(prices: &BTreeMap<i64, f64>, horizon_seconds: i64) -> BTreeMap<i64, f64> {
    let mut output = BTreeMap::new();
    for (&ts, &price) in prices {
        let Some(&prior) = prices.get(&(ts - horizon_seconds)) else {
            continue;
        };
        if price.is_finite() && prior.is_finite() && prior != 0.0 {
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

pub fn build_ylabel_rows(
    contract_id: &str,
    prices_by_ts: &BTreeMap<i64, CausalPrices>,
) -> Vec<YlabelRow> {
    let benchmark_prices = std::array::from_fn::<_, 3, _>(|benchmark| {
        prices_by_ts
            .iter()
            .filter_map(|(&ts, prices)| prices.benchmark(benchmark).map(|price| (ts, price)))
            .filter(|(_, price)| price.is_finite())
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
                ts,
                labels,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prices(v: f64) -> CausalPrices {
        CausalPrices {
            twap: Some(v),
            vwap: Some(v),
            midp: Some(v),
        }
    }

    #[test]
    fn future_return_is_p_t_plus_h_over_p_t() {
        let mut input = BTreeMap::new();
        input.insert(0, prices(100.0));
        input.insert(5 * 60, prices(104.0));
        let rows = build_ylabel_rows("rb2405", &input);
        let row = rows.iter().find(|row| row.ts == 0).unwrap();
        assert!((row.labels[0].unwrap() - 0.04).abs() < 1e-12);
        assert_eq!(row.labels[1], Some(1.0));
    }

    #[test]
    fn additive_hfq_negative_prices_use_the_same_ratio_return() {
        let mut input = BTreeMap::new();
        input.insert(0, prices(-100.0));
        input.insert(5 * 60, prices(-104.0));
        let rows = build_ylabel_rows("CL", &input);
        let row = rows.iter().find(|row| row.ts == 0).unwrap();
        assert!((row.labels[0].unwrap() - 0.04).abs() < 1e-12);
        assert_eq!(row.labels[1], Some(1.0));
    }

    #[test]
    fn zero_price_is_not_a_return_denominator() {
        let mut input = BTreeMap::new();
        input.insert(0, prices(0.0));
        input.insert(5 * 60, prices(-104.0));
        let rows = build_ylabel_rows("CL", &input);
        let row = rows.iter().find(|row| row.ts == 0).unwrap();
        assert!(row.labels[0].is_none());
    }

    #[test]
    fn zero_future_price_has_a_defined_return() {
        let mut input = BTreeMap::new();
        input.insert(0, prices(-100.0));
        input.insert(5 * 60, prices(0.0));
        let rows = build_ylabel_rows("CL", &input);
        let row = rows.iter().find(|row| row.ts == 0).unwrap();
        assert_eq!(row.labels[0], Some(-1.0));
    }

    #[test]
    fn missing_minute_does_not_fill() {
        let mut input = BTreeMap::new();
        input.insert(0, prices(100.0));
        input.insert(10 * 60, prices(110.0));
        let rows = build_ylabel_rows("rb2405", &input);
        let row = rows.iter().find(|row| row.ts == 0).unwrap();
        assert!(row.labels[0].is_none());
    }

    #[test]
    fn carried_empty_minute_is_not_p_t() {
        let rows = vec![
            crate::baseline_1min::BaselineMinute {
                contract_id: "rb2405".into(),
                ts: 0,
                open: Some(100.0),
                high: Some(100.0),
                low: Some(100.0),
                close: Some(100.0),
                volume: 1.0,
                amount: 100.0,
                avg_amount: 100.0,
                count: 1.0,
                buy_count: 1.0,
                sell_count: 0.0,
                buy_amount: 100.0,
                sell_amount: 0.0,
                buy_volume: 1.0,
                sell_volume: 0.0,
                vwap: Some(100.0),
                buy_vwap: Some(100.0),
                sell_vwap: None,
                twap: Some(100.0),
                mid_price: Some(101.0),
                net_buy_amount: 100.0,
                net_buy_volume: 1.0,
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
                book: None,
            },
            crate::baseline_1min::BaselineMinute {
                contract_id: "rb2405".into(),
                ts: 60,
                open: Some(100.0),
                high: Some(100.0),
                low: Some(100.0),
                close: Some(100.0),
                volume: 0.0,
                amount: 0.0,
                avg_amount: 0.0,
                count: 0.0,
                buy_count: 0.0,
                sell_count: 0.0,
                buy_amount: 0.0,
                sell_amount: 0.0,
                buy_volume: 0.0,
                sell_volume: 0.0,
                vwap: Some(100.0),
                buy_vwap: Some(100.0),
                sell_vwap: Some(100.0),
                twap: None,
                mid_price: None,
                net_buy_amount: 0.0,
                net_buy_volume: 0.0,
                net_buy_pct: Some(0.0),
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
                book: None,
            },
        ];
        let prices = causal_prices_from_minutes(&rows);
        assert!(prices.get(&60).is_some());
        assert!(prices.get(&120).is_none());
    }
}
