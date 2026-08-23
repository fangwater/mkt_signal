use std::ops::Index;

pub trait F64SeriesView {
    fn len(&self) -> usize;
    fn value_at(&self, idx: usize) -> f64;
}

pub trait OptF64SeriesView {
    fn len(&self) -> usize;
    fn value_at(&self, idx: usize) -> Option<f64>;
}

impl F64SeriesView for [f64] {
    fn len(&self) -> usize {
        self.len()
    }

    fn value_at(&self, idx: usize) -> f64 {
        self[idx]
    }
}

impl F64SeriesView for Vec<f64> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn value_at(&self, idx: usize) -> f64 {
        self[idx]
    }
}

impl OptF64SeriesView for [Option<f64>] {
    fn len(&self) -> usize {
        self.len()
    }

    fn value_at(&self, idx: usize) -> Option<f64> {
        self[idx]
    }
}

impl OptF64SeriesView for Vec<Option<f64>> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn value_at(&self, idx: usize) -> Option<f64> {
        self[idx]
    }
}

#[derive(Clone, Copy)]
pub struct SplitSlice<'a, T> {
    first: &'a [T],
    second: &'a [T],
}

impl<'a, T> SplitSlice<'a, T> {
    pub fn from_parts(parts: (&'a [T], &'a [T])) -> Self {
        Self {
            first: parts.0,
            second: parts.1,
        }
    }

    pub fn len(&self) -> usize {
        self.first.len() + self.second.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, idx: usize) -> Option<&'a T> {
        if idx < self.first.len() {
            return self.first.get(idx);
        }
        self.second.get(idx.saturating_sub(self.first.len()))
    }

    pub fn last(&self) -> Option<&'a T> {
        self.second.last().or_else(|| self.first.last())
    }

    pub fn iter(&self) -> impl Iterator<Item = &'a T> {
        self.first.iter().chain(self.second.iter())
    }

    pub fn range_iter(&self, start: usize, end: usize) -> impl Iterator<Item = &'a T> {
        let len = self.len();
        let start = start.min(len);
        let end = end.min(len);
        if start >= end {
            return self.first[0..0].iter().chain(self.second[0..0].iter());
        }

        let left_len = self.first.len();
        let left_start = start.min(left_len);
        let left_end = end.min(left_len);
        let right_start = start.saturating_sub(left_len).min(self.second.len());
        let right_end = end.saturating_sub(left_len).min(self.second.len());

        self.first[left_start..left_end]
            .iter()
            .chain(self.second[right_start..right_end].iter())
    }
}

impl<T> Index<usize> for SplitSlice<'_, T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index)
            .unwrap_or_else(|| panic!("split-slice index out of bounds: {}", index))
    }
}

impl F64SeriesView for SplitSlice<'_, f64> {
    fn len(&self) -> usize {
        SplitSlice::len(self)
    }

    fn value_at(&self, idx: usize) -> f64 {
        self[idx]
    }
}

impl OptF64SeriesView for SplitSlice<'_, Option<f64>> {
    fn len(&self) -> usize {
        SplitSlice::len(self)
    }

    fn value_at(&self, idx: usize) -> Option<f64> {
        self[idx]
    }
}

pub struct LsegSeries<'a> {
    pub open: SplitSlice<'a, f64>,
    pub high: SplitSlice<'a, f64>,
    pub low: SplitSlice<'a, f64>,
    pub close: SplitSlice<'a, f64>,
    pub volume: SplitSlice<'a, f64>,
    pub amount: SplitSlice<'a, f64>,
    pub avg_amount: SplitSlice<'a, f64>,
    pub count: SplitSlice<'a, f64>,
    pub trade_time: SplitSlice<'a, f64>,
    pub buy_count: SplitSlice<'a, f64>,
    pub sell_count: SplitSlice<'a, f64>,
    pub buy_amount: SplitSlice<'a, f64>,
    pub sell_amount: SplitSlice<'a, f64>,
    pub buy_volume: SplitSlice<'a, f64>,
    pub sell_volume: SplitSlice<'a, f64>,
    pub large_order: SplitSlice<'a, f64>,
    pub medium_order: SplitSlice<'a, f64>,
    pub large_buy: SplitSlice<'a, f64>,
    pub large_sell: SplitSlice<'a, f64>,
    pub medium_buy: SplitSlice<'a, f64>,
    pub medium_sell: SplitSlice<'a, f64>,
    pub small_order: SplitSlice<'a, f64>,
    pub small_buy: SplitSlice<'a, f64>,
    pub small_sell: SplitSlice<'a, f64>,
    pub vwap: SplitSlice<'a, f64>,
    pub buy_vwap: SplitSlice<'a, f64>,
    pub sell_vwap: SplitSlice<'a, f64>,
    pub net_buy_volume: SplitSlice<'a, f64>,
    pub net_buy_pct: SplitSlice<'a, f64>,
    pub net_buy_large: SplitSlice<'a, f64>,
    pub net_buy_medium: SplitSlice<'a, f64>,
    pub net_buy_small: SplitSlice<'a, f64>,
    pub net_buy_amount: SplitSlice<'a, f64>,
    pub active_buy_ratio_5m: SplitSlice<'a, f64>,
    pub large_pct_30m: SplitSlice<'a, f64>,
    pub large_pct_120m: SplitSlice<'a, f64>,
    pub small_pct_30m: SplitSlice<'a, f64>,
    pub small_pct_120m: SplitSlice<'a, f64>,
    pub net_buy_small_pct_15m: SplitSlice<'a, f64>,
    pub active_buy_ratio_240m: SplitSlice<'a, f64>,
    pub bid0v: SplitSlice<'a, f64>,
    pub mid_price: SplitSlice<'a, f64>,
    pub spread: SplitSlice<'a, f64>,
    pub relative_spread: SplitSlice<'a, f64>,
    pub bid_vwap5: SplitSlice<'a, f64>,
    pub total_bid5: SplitSlice<'a, f64>,
    pub total_ask5: SplitSlice<'a, f64>,
    pub top3_bid_volume: SplitSlice<'a, f64>,
    pub top3_ask_volume: SplitSlice<'a, f64>,
    pub top3_bid_mean: SplitSlice<'a, f64>,
    pub top3_ask_mean: SplitSlice<'a, f64>,
    pub outer_bid_amount: SplitSlice<'a, f64>,
    pub outer_ask_amount: SplitSlice<'a, f64>,
    pub outer_bid_price: SplitSlice<'a, f64>,
    pub outer_ask_price: SplitSlice<'a, f64>,
    pub outer_level_volume: SplitSlice<'a, f64>,
    pub mean_bid_amount5: SplitSlice<'a, f64>,
    pub mean_bid_price5: SplitSlice<'a, f64>,
    pub mean_bid_price_full: SplitSlice<'a, f64>,
    pub mean_ask_price5: SplitSlice<'a, f64>,
    pub mean_ask_price_full: SplitSlice<'a, f64>,
    pub ask_pv5_mean: SplitSlice<'a, f64>,
    pub bid_pv5_mean: SplitSlice<'a, f64>,
    pub factor_031_ratio: SplitSlice<'a, f64>,
    pub factor_119_mid_minus_ask_vwap5: SplitSlice<'a, f64>,
    pub total_volume5_sum: SplitSlice<'a, f64>,
    pub median_all_price10: SplitSlice<'a, f64>,
    pub factor_152_pct_mean: SplitSlice<'a, Option<f64>>,
    pub ask_vwap_diff_3_5: SplitSlice<'a, f64>,
    pub ask_mean_amount5: SplitSlice<'a, f64>,
    pub ask0v: SplitSlice<'a, f64>,
    pub ask_vwap5: SplitSlice<'a, f64>,
    pub factor_113_bid_price_pct_hmean: SplitSlice<'a, Option<f64>>,
    pub factor_114_ask_price_pct_mean: SplitSlice<'a, Option<f64>>,
    pub factor_127_bid_price_kurt: SplitSlice<'a, f64>,
    pub factor_128_skew: SplitSlice<'a, Option<f64>>,
    pub factor_131_ask_price_kurt: SplitSlice<'a, f64>,
    pub factor_157_bid_ask_diff_std: SplitSlice<'a, Option<f64>>,
    pub factor_160_pct_change_mean: SplitSlice<'a, Option<f64>>,
    pub corr_close_volume_14_last: Option<f64>,
}
