//! CME 1-minute 60-column supervision labels.
//!
//! `P[t]` is the closed minute `[t-60, t)`. Trade-derived TWAP/VWAP are
//! sparse: carried values from an empty minute never become an observation.
//! Additively adjusted CME prices may be negative, so a finite nonzero price
//! is valid for a label return; zero remains undefined as a denominator.

use std::collections::BTreeMap;

pub const BENCHMARKS: [&str; 3] = ["twap", "vwap", "midp"];
pub const HORIZON_MINUTES: [i64; 5] = [5, 15, 30, 60, 240];
pub const VOLATILITY_WINDOW: usize = 30;
pub const RANK_WINDOW: usize = 1440;
pub const LABEL_COUNT: usize = 60;

#[derive(Clone, Copy, Debug, Default)]
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

    pub fn observed(self) -> bool {
        self.twap.is_some() || self.vwap.is_some() || self.midp.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct YlabelRow {
    pub contract_id: String,
    pub ts: i64,
    pub labels: [Option<f64>; LABEL_COUNT],
}

pub fn valid_label_price(value: Option<f64>) -> Option<f64> {
    value.filter(|price| price.is_finite() && *price != 0.0)
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

fn horizon_returns(prices: &BTreeMap<i64, f64>, horizon_seconds: i64) -> BTreeMap<i64, f64> {
    let mut output = BTreeMap::new();
    for (&ts, &price) in prices {
        let Some(&prior) = prices.get(&(ts - horizon_seconds)) else {
            continue;
        };
        if let Some(value) = (price / prior - 1.0)
            .is_finite()
            .then_some(price / prior - 1.0)
        {
            output.insert(ts, value);
        }
    }
    output
}

fn visit_contiguous_segments(returns: &BTreeMap<i64, f64>, mut visit: impl FnMut(&[(i64, f64)])) {
    let mut segment = Vec::new();
    let mut previous = None;
    for (&ts, &value) in returns {
        if previous.is_some_and(|last| ts != last + 60) {
            visit(&segment);
            segment.clear();
        }
        segment.push((ts, value));
        previous = Some(ts);
    }
    if !segment.is_empty() {
        visit(&segment);
    }
}

fn rolling_std(returns: &BTreeMap<i64, f64>, width: usize) -> BTreeMap<i64, f64> {
    let mut output = BTreeMap::new();
    visit_contiguous_segments(returns, |segment| {
        if segment.len() < width || width < 2 {
            return;
        }
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        for (index, &(_, value)) in segment.iter().enumerate() {
            sum += value;
            sum_sq += value * value;
            if index >= width {
                let removed = segment[index - width].1;
                sum -= removed;
                sum_sq -= removed * removed;
            }
            if index + 1 >= width {
                let n = width as f64;
                let variance = ((sum_sq - sum * sum / n) / (n - 1.0)).max(0.0);
                output.insert(segment[index].0, variance.sqrt());
            }
        }
    });
    output
}

struct Fenwick {
    tree: Vec<u32>,
}

impl Fenwick {
    fn new(len: usize) -> Self {
        Self {
            tree: vec![0; len + 1],
        }
    }

    fn add(&mut self, index: usize, delta: i32) {
        let mut position = index + 1;
        while position < self.tree.len() {
            self.tree[position] = (self.tree[position] as i32 + delta) as u32;
            position += position & position.wrapping_neg();
        }
    }

    fn prefix_sum(&self, exclusive: usize) -> u32 {
        let mut position = exclusive;
        let mut result = 0u32;
        while position > 0 {
            result += self.tree[position];
            position &= position - 1;
        }
        result
    }
}

fn rank_value(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        value
    }
}

fn rolling_average_rank(returns: &BTreeMap<i64, f64>, width: usize) -> BTreeMap<i64, f64> {
    let mut output = BTreeMap::new();
    visit_contiguous_segments(returns, |segment| {
        if segment.len() < width {
            return;
        }
        let mut coordinates = segment
            .iter()
            .map(|(_, value)| rank_value(*value))
            .collect::<Vec<_>>();
        coordinates.sort_by(|left, right| left.total_cmp(right));
        coordinates.dedup_by(|left, right| *left == *right);
        let index_of = |value: f64| {
            coordinates
                .binary_search_by(|candidate| candidate.total_cmp(&rank_value(value)))
                .expect("return coordinate exists")
        };
        let mut counts = Fenwick::new(coordinates.len());
        for &(_, value) in &segment[..width] {
            counts.add(index_of(value), 1);
        }
        for index in width - 1..segment.len() {
            let current_index = index_of(segment[index].1);
            let less = counts.prefix_sum(current_index);
            let equal = counts.prefix_sum(current_index + 1) - less;
            output.insert(
                segment[index].0,
                1.0 + less as f64 + (equal.saturating_sub(1) as f64) / 2.0,
            );
            if index + 1 < segment.len() {
                counts.add(index_of(segment[index + 1 - width].1), -1);
                counts.add(index_of(segment[index + 1].1), 1);
            }
        }
    });
    output
}

pub fn build_ylabel_rows(
    contract_id: &str,
    prices_by_ts: &BTreeMap<i64, CausalPrices>,
) -> Vec<YlabelRow> {
    let benchmark_prices = std::array::from_fn::<_, 3, _>(|benchmark| {
        prices_by_ts
            .iter()
            .filter_map(|(&ts, prices)| prices.benchmark(benchmark).map(|price| (ts, price)))
            .filter_map(|(ts, price)| valid_label_price(Some(price)).map(|price| (ts, price)))
            .collect::<BTreeMap<_, _>>()
    });
    let returns = std::array::from_fn::<_, 3, _>(|benchmark| {
        std::array::from_fn::<_, 5, _>(|horizon| {
            horizon_returns(&benchmark_prices[benchmark], HORIZON_MINUTES[horizon] * 60)
        })
    });
    let volatility = std::array::from_fn::<_, 3, _>(|benchmark| {
        std::array::from_fn::<_, 5, _>(|horizon| {
            rolling_std(&returns[benchmark][horizon], VOLATILITY_WINDOW)
        })
    });
    let ranks = std::array::from_fn::<_, 3, _>(|benchmark| {
        std::array::from_fn::<_, 5, _>(|horizon| {
            rolling_average_rank(&returns[benchmark][horizon], RANK_WINDOW)
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
                        volatility[benchmark][horizon].get(&end_ts).copied();
                    labels[label_index(benchmark, horizon, 3)] =
                        ranks[benchmark][horizon].get(&end_ts).copied();
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

    fn prices(value: f64) -> CausalPrices {
        CausalPrices {
            twap: Some(value),
            vwap: Some(value),
            midp: Some(value),
        }
    }

    #[test]
    fn future_return_is_p_t_plus_h_over_p_t() {
        let input = BTreeMap::from([(0, prices(100.0)), (5 * 60, prices(104.0))]);
        let rows = build_ylabel_rows("ESH24", &input);
        let row = rows.iter().find(|row| row.ts == 0).unwrap();
        assert!((row.labels[0].unwrap() - 0.04).abs() < 1e-12);
        assert_eq!(row.labels[1], Some(1.0));
    }

    #[test]
    fn missing_clock_minute_does_not_fill() {
        let input = BTreeMap::from([(0, prices(100.0)), (10 * 60, prices(110.0))]);
        let rows = build_ylabel_rows("ESH24", &input);
        let row = rows.iter().find(|row| row.ts == 0).unwrap();
        assert!(row.labels[0].is_none());
    }

    #[test]
    fn adjusted_negative_prices_remain_label_observations() {
        let input = BTreeMap::from([(0, prices(-10.0)), (5 * 60, prices(-8.0))]);
        let rows = build_ylabel_rows("CLJ20", &input);
        let row = rows.iter().find(|row| row.ts == 0).unwrap();
        assert!((row.labels[0].unwrap() + 0.2).abs() < 1e-12);
        assert_eq!(row.labels[1], Some(0.0));
    }

    #[test]
    fn rolling_metrics_require_an_unbroken_clock_window() {
        let returns = BTreeMap::from([
            (0, 1.0),
            (60, 2.0),
            (120, 3.0),
            (240, 4.0),
            (300, 5.0),
            (360, 6.0),
        ]);
        let std = rolling_std(&returns, 3);
        let ranks = rolling_average_rank(&returns, 3);
        assert!(std.contains_key(&120));
        assert!(!std.contains_key(&240));
        assert_eq!(ranks.get(&120), Some(&3.0));
        assert_eq!(ranks.get(&360), Some(&3.0));
    }
}
