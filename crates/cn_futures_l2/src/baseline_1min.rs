//! Domestic 1-minute baseline wide table for `cn_features`.
//!
//! One row = one contract-minute. No implied / Special columns. Unknown
//! aggressor splits 50/50 into buy and sell. Five-level book is the last
//! two-sided depth inside `[t, t+60)`. `twap` is last-print time-weighted
//! inside the minute; `mid_price` is that last book's mid, not the causal
//! prior at `t`. VWAP is `amount / volume / volume_multiple` so it shares
//! the quoted-price unit with OHLC. `amount` itself is not divided.

use anyhow::{bail, Result};
use std::collections::BTreeMap;

use crate::codec::{DepthRecord, TradeRecord};
use crate::export_1s::{is_session_break, shanghai};
use crate::universe::product_id;

#[derive(Clone, Debug)]
pub struct PrintTrade {
    pub ts_utc_ns: u64,
    pub price: f64,
    pub volume: f64,
    pub amount: f64,
    pub aggressor: u8,
}

impl From<&TradeRecord> for PrintTrade {
    fn from(rec: &TradeRecord) -> Self {
        Self {
            ts_utc_ns: rec.ts_utc_ns,
            price: rec.price,
            volume: rec.volume,
            amount: rec.turnover.unwrap_or(rec.price * rec.volume),
            aggressor: rec.aggressor,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Book5 {
    pub bid_prices: [Option<f64>; 5],
    pub bid_sizes: [Option<f64>; 5],
    pub ask_prices: [Option<f64>; 5],
    pub ask_sizes: [Option<f64>; 5],
}

impl Book5 {
    pub fn from_depth(rec: &DepthRecord) -> Option<Self> {
        let bid = rec.bid_prices[0]?;
        let ask = rec.ask_prices[0]?;
        let bid_v = rec.bid_sizes[0]?;
        let ask_v = rec.ask_sizes[0]?;
        if !(bid > 0.0 && ask >= bid && bid_v >= 0.0 && ask_v >= 0.0) {
            return None;
        }
        Some(Self {
            bid_prices: rec.bid_prices,
            bid_sizes: rec.bid_sizes,
            ask_prices: rec.ask_prices,
            ask_sizes: rec.ask_sizes,
        })
    }

    pub fn mid(self) -> Option<f64> {
        let bid = self.bid_prices[0]?;
        let ask = self.ask_prices[0]?;
        Some((bid + ask) / 2.0)
    }

    pub fn two_sided(&self) -> bool {
        match (self.bid_prices[0], self.ask_prices[0]) {
            (Some(bid), Some(ask)) => bid > 0.0 && ask >= bid && bid.is_finite() && ask.is_finite(),
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BaselineMinute {
    pub contract_id: String,
    pub ts: i64,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: f64,
    pub amount: f64,
    pub avg_amount: f64,
    pub count: f64,
    pub buy_count: f64,
    pub sell_count: f64,
    pub buy_amount: f64,
    pub sell_amount: f64,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub vwap: Option<f64>,
    pub buy_vwap: Option<f64>,
    pub sell_vwap: Option<f64>,
    pub twap: Option<f64>,
    pub mid_price: Option<f64>,
    pub net_buy_amount: f64,
    pub net_buy_volume: f64,
    pub net_buy_pct: Option<f64>,
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
    pub book: Option<Book5>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SizeBuckets {
    pub large_order: f64,
    pub medium_order: f64,
    pub small_order: f64,
    pub large_buy: f64,
    pub large_sell: f64,
    pub medium_buy: f64,
    pub medium_sell: f64,
    pub small_buy: f64,
    pub small_sell: f64,
}

impl SizeBuckets {
    pub fn add(&mut self, amount: f64, aggressor: u8, p50: f64, p90: f64) {
        if !(amount.is_finite() && amount > 0.0) {
            return;
        }
        let bucket = if amount >= p90 {
            2
        } else if amount >= p50 {
            1
        } else {
            0
        };
        let (buy, sell) = match aggressor {
            1 => (amount, 0.0),
            2 => (0.0, amount),
            _ => (amount / 2.0, amount / 2.0),
        };
        match bucket {
            2 => {
                self.large_order += amount;
                self.large_buy += buy;
                self.large_sell += sell;
            }
            1 => {
                self.medium_order += amount;
                self.medium_buy += buy;
                self.medium_sell += sell;
            }
            _ => {
                self.small_order += amount;
                self.small_buy += buy;
                self.small_sell += sell;
            }
        }
    }

    pub fn nets(self) -> (f64, f64, f64) {
        (
            self.large_buy - self.large_sell,
            self.medium_buy - self.medium_sell,
            self.small_buy - self.small_sell,
        )
    }
}

/// Linear-interpolated sample quantile, matching numpy.percentile default.
pub fn linear_percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() || !(p >= 0.0 && p <= 1.0) {
        return None;
    }
    if sorted.len() == 1 {
        return Some(sorted[0]);
    }
    let pos = p * (sorted.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        Some(sorted[lo])
    } else {
        let w = pos - lo as f64;
        Some(sorted[lo] * (1.0 - w) + sorted[hi] * w)
    }
}

pub fn minute_left_sec(ts_utc_ns: u64) -> i64 {
    (ts_utc_ns / 1_000_000_000 / 60 * 60) as i64
}

fn normalized_twap_prints(
    timestamps_us: &[i64],
    prices: &[f64],
    start_us: i64,
    end_us: i64,
) -> Vec<(i64, f64)> {
    let mut ordered: Vec<(i64, f64)> = timestamps_us
        .iter()
        .zip(prices.iter())
        .filter_map(|(&ts_us, &price)| {
            if ts_us < start_us || ts_us >= end_us || !(price.is_finite() && price > 0.0) {
                None
            } else {
                Some((ts_us, price))
            }
        })
        .collect();
    // Stable ordering preserves source order for equal exchange timestamps.
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
    unique
}

fn twap_from_prints(
    prints: &[(i64, f64)],
    start_us: i64,
    end_us: i64,
    prior_price: Option<f64>,
) -> Option<f64> {
    let mut previous_time = start_us;
    let mut previous_price = prior_price.filter(|price| price.is_finite() && *price > 0.0);
    let mut weighted = 0.0;
    for &(ts_us, price) in prints {
        if let Some(previous_price) = previous_price {
            weighted += previous_price * (ts_us - previous_time) as f64;
        }
        previous_time = ts_us;
        previous_price = Some(price);
    }
    let previous_price = previous_price?;
    weighted += previous_price * (end_us - previous_time) as f64;
    Some(weighted / (end_us - start_us) as f64)
}

/// Last-print step function over `[start_us, end_us)`, divided by the full bucket.
///
/// `prior_price` is the last valid print in the same continuous trading segment.
/// When it is absent, time before this bucket's first print remains uncovered.
pub fn exact_twap_one_bucket(
    timestamps_us: &[i64],
    prices: &[f64],
    start_us: i64,
    end_us: i64,
    prior_price: Option<f64>,
) -> Result<Option<f64>> {
    if end_us <= start_us {
        bail!("TWAP bucket end must be after start");
    }
    let prints = normalized_twap_prints(timestamps_us, prices, start_us, end_us);
    Ok(twap_from_prints(&prints, start_us, end_us, prior_price))
}

/// Stateful last-print TWAP cursor for an already-published minute grid.
///
/// A grid discontinuity is a trading-segment boundary. Keeping this state
/// outside the grid lets a patcher process adjacent parquet partitions without
/// accidentally carrying a print over a true session break.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TwapGridState {
    pub last_minute: Option<i64>,
    pub last_price: Option<f64>,
}

/// Recalculate only the TWAP values for `minutes`, preserving the supplied
/// grid exactly. `minutes` must be strictly increasing and minute aligned.
///
/// Trades outside the grid are ignored. A valid final print carries to the
/// next adjacent grid minute; any missing grid minute resets that carry.
pub fn twap_for_existing_grid(
    minutes: &[i64],
    trades: &[PrintTrade],
    state: &mut TwapGridState,
) -> Result<Vec<Option<f64>>> {
    let mut by_minute: BTreeMap<i64, (Vec<i64>, Vec<f64>)> = BTreeMap::new();
    for trade in trades {
        let minute = minute_left_sec(trade.ts_utc_ns);
        let entry = by_minute.entry(minute).or_default();
        entry.0.push((trade.ts_utc_ns / 1_000) as i64);
        entry.1.push(trade.price);
    }

    let mut out = Vec::with_capacity(minutes.len());
    for &ts in minutes {
        if ts.rem_euclid(60) != 0 {
            bail!("TWAP grid minute {ts} is not minute aligned");
        }
        if let Some(previous) = state.last_minute {
            if ts <= previous {
                bail!("TWAP grid is not strictly increasing: {previous} then {ts}");
            }
            if ts != previous + 60 {
                state.last_price = None;
            }
        }
        let start_us = ts * 1_000_000;
        let end_us = (ts + 60) * 1_000_000;
        let (timestamps_us, prices) = by_minute
            .get(&ts)
            .map(|(timestamps_us, prices)| (timestamps_us.as_slice(), prices.as_slice()))
            .unwrap_or((&[], &[]));
        let prints = normalized_twap_prints(timestamps_us, prices, start_us, end_us);
        out.push(twap_from_prints(
            &prints,
            start_us,
            end_us,
            state.last_price,
        ));
        if let Some((_, price)) = prints.last() {
            state.last_price = Some(*price);
        }
        state.last_minute = Some(ts);
    }
    Ok(out)
}

struct Acc {
    contract_id: String,
    ts: i64,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: f64,
    amount: f64,
    count: f64,
    buy_count: f64,
    sell_count: f64,
    buy_volume: f64,
    sell_volume: f64,
    buy_amount: f64,
    sell_amount: f64,
    ts_us: Vec<i64>,
    px: Vec<f64>,
    book: Option<Book5>,
    volume_multiple: f64,
    sizes: SizeBuckets,
}

impl Acc {
    fn new(contract_id: String, ts: i64, volume_multiple: f64) -> Self {
        Self {
            contract_id,
            ts,
            open: None,
            high: None,
            low: None,
            close: None,
            volume: 0.0,
            amount: 0.0,
            count: 0.0,
            buy_count: 0.0,
            sell_count: 0.0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            buy_amount: 0.0,
            sell_amount: 0.0,
            ts_us: Vec::new(),
            px: Vec::new(),
            book: None,
            volume_multiple,
            sizes: SizeBuckets::default(),
        }
    }

    fn add_trade(&mut self, trade: &PrintTrade) {
        let price = trade.price;
        let volume = trade.volume;
        let amount = trade.amount;
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
        self.ts_us.push((trade.ts_utc_ns / 1_000) as i64);
        self.px.push(price);
        match trade.aggressor {
            1 => {
                self.buy_count += 1.0;
                self.buy_volume += volume;
                self.buy_amount += amount;
            }
            2 => {
                self.sell_count += 1.0;
                self.sell_volume += volume;
                self.sell_amount += amount;
            }
            _ => {
                self.buy_count += 0.5;
                self.sell_count += 0.5;
                self.buy_volume += volume / 2.0;
                self.sell_volume += volume / 2.0;
                self.buy_amount += amount / 2.0;
                self.sell_amount += amount / 2.0;
            }
        }
    }

    fn add_book(&mut self, book: Book5) {
        self.book = Some(book);
    }

    fn close_row(self) -> Result<BaselineMinute> {
        let twap = exact_twap_one_bucket(
            &self.ts_us,
            &self.px,
            self.ts * 1_000_000,
            (self.ts + 60) * 1_000_000,
            None,
        )?;
        let mid_price = self.book.clone().and_then(Book5::mid);
        let vwap = quoted_vwap(self.amount, self.volume, self.volume_multiple);
        let buy_vwap = quoted_vwap(self.buy_amount, self.buy_volume, self.volume_multiple);
        let sell_vwap = quoted_vwap(self.sell_amount, self.sell_volume, self.volume_multiple);
        let net_buy_amount = self.buy_amount - self.sell_amount;
        let net_buy_volume = self.buy_volume - self.sell_volume;
        let directed = self.buy_amount + self.sell_amount;
        Ok(BaselineMinute {
            contract_id: self.contract_id,
            ts: self.ts,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            amount: self.amount,
            avg_amount: if self.count > 0.0 {
                self.amount / self.count
            } else {
                0.0
            },
            count: self.count,
            buy_count: self.buy_count,
            sell_count: self.sell_count,
            buy_amount: self.buy_amount,
            sell_amount: self.sell_amount,
            buy_volume: self.buy_volume,
            sell_volume: self.sell_volume,
            vwap,
            buy_vwap,
            sell_vwap,
            twap,
            mid_price,
            net_buy_amount,
            net_buy_volume,
            net_buy_pct: if directed > 0.0 {
                Some(net_buy_amount / directed)
            } else {
                Some(0.0)
            },
            large_order: self.sizes.large_order,
            medium_order: self.sizes.medium_order,
            small_order: self.sizes.small_order,
            large_buy: self.sizes.large_buy,
            large_sell: self.sizes.large_sell,
            medium_buy: self.sizes.medium_buy,
            medium_sell: self.sizes.medium_sell,
            small_buy: self.sizes.small_buy,
            small_sell: self.sizes.small_sell,
            net_buy_large: self.sizes.large_buy - self.sizes.large_sell,
            net_buy_medium: self.sizes.medium_buy - self.sizes.medium_sell,
            net_buy_small: self.sizes.small_buy - self.sizes.small_sell,
            book: self.book,
        })
    }
}

pub fn quoted_vwap(amount: f64, volume: f64, volume_multiple: f64) -> Option<f64> {
    if volume > 0.0 && volume_multiple > 0.0 && amount.is_finite() {
        let value = amount / volume / volume_multiple;
        value.is_finite().then_some(value)
    } else {
        None
    }
}

pub fn scale_quoted_vwap(value: Option<f64>, volume_multiple: f64) -> Option<f64> {
    let px = value.filter(|px| px.is_finite() && *px > 0.0)?;
    if !(volume_multiple.is_finite() && volume_multiple > 0.0) {
        return None;
    }
    let scaled = px / volume_multiple;
    scaled.is_finite().then_some(scaled)
}

fn looks_quoted(vwap: Option<f64>, close: Option<f64>) -> bool {
    match (vwap, close) {
        (Some(v), Some(c)) if v.is_finite() && c > 0.0 && v > 0.0 => (v / c - 1.0).abs() < 0.05,
        _ => false,
    }
}

/// Convert a stored 1min row onto quoted VWAP. Trade minutes are rebuilt from
/// amount/volume. Empty minutes that already stored a close fallback stay put;
/// carried amount/volume VWAP is divided. A second pass is a no-op.
pub fn rewrite_quoted_vwap(row: &mut BaselineMinute, volume_multiple: f64) {
    if row.volume > 0.0 {
        row.vwap = quoted_vwap(row.amount, row.volume, volume_multiple);
    } else if !looks_quoted(row.vwap, row.close) {
        row.vwap = scale_quoted_vwap(row.vwap, volume_multiple);
    }
    if row.buy_volume > 0.0 {
        row.buy_vwap = quoted_vwap(row.buy_amount, row.buy_volume, volume_multiple);
    } else if !looks_quoted(row.buy_vwap, row.close) {
        row.buy_vwap = scale_quoted_vwap(row.buy_vwap, volume_multiple);
    }
    if row.sell_volume > 0.0 {
        row.sell_vwap = quoted_vwap(row.sell_amount, row.sell_volume, volume_multiple);
    } else if !looks_quoted(row.sell_vwap, row.close) {
        row.sell_vwap = scale_quoted_vwap(row.sell_vwap, volume_multiple);
    }
}

pub fn synthesize_minutes(
    contract_id: &str,
    trades: &[PrintTrade],
    depths: &[DepthRecord],
    volume_multiple: f64,
) -> Result<Vec<BaselineMinute>> {
    if !(volume_multiple.is_finite() && volume_multiple > 0.0) {
        bail!("volume_multiple must be finite and positive, got {volume_multiple}");
    }
    let mut acc: BTreeMap<i64, Acc> = BTreeMap::new();
    for trade in trades {
        let ts = minute_left_sec(trade.ts_utc_ns);
        acc.entry(ts)
            .or_insert_with(|| Acc::new(contract_id.to_string(), ts, volume_multiple))
            .add_trade(trade);
    }
    for depth in depths {
        let Some(book) = Book5::from_depth(depth) else {
            continue;
        };
        let ts = minute_left_sec(depth.ts_utc_ns);
        acc.entry(ts)
            .or_insert_with(|| Acc::new(contract_id.to_string(), ts, volume_multiple))
            .add_book(book);
    }
    acc.into_values().map(Acc::close_row).collect()
}

#[derive(Clone, Debug, Default)]
struct FillState {
    close: Option<f64>,
    vwap: Option<f64>,
    buy_vwap: Option<f64>,
    sell_vwap: Option<f64>,
    book: Option<Book5>,
}

fn empty_minute(
    contract_id: &str,
    ts: i64,
    state: FillState,
    book: Option<Book5>,
) -> BaselineMinute {
    let close = state.close;
    BaselineMinute {
        contract_id: contract_id.to_string(),
        ts,
        open: close,
        high: close,
        low: close,
        close,
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
        vwap: state.vwap.or(close),
        buy_vwap: state.buy_vwap.or(close),
        sell_vwap: state.sell_vwap.or(close),
        twap: None,
        mid_price: book.clone().and_then(Book5::mid),
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
        book,
    }
}

fn apply_fill(row: &mut BaselineMinute, state: &mut FillState) {
    if row.close.filter(|px| px.is_finite() && *px > 0.0).is_none() {
        row.open = state.close;
        row.high = state.close;
        row.low = state.close;
        row.close = state.close;
    }
    if row.vwap.filter(|px| px.is_finite() && *px > 0.0).is_none() {
        row.vwap = state.vwap.or(row.close);
    }
    if row
        .buy_vwap
        .filter(|px| px.is_finite() && *px > 0.0)
        .is_none()
    {
        row.buy_vwap = state.buy_vwap.or(row.close);
    }
    if row
        .sell_vwap
        .filter(|px| px.is_finite() && *px > 0.0)
        .is_none()
    {
        row.sell_vwap = state.sell_vwap.or(row.close);
    }
    let missing_book = row
        .book
        .as_ref()
        .map(|book| !book.two_sided())
        .unwrap_or(true);
    if missing_book {
        row.book = state.book.clone();
        if row
            .mid_price
            .filter(|px| px.is_finite() && *px > 0.0)
            .is_none()
        {
            row.mid_price = row.book.clone().and_then(Book5::mid);
        }
    }
    if let Some(close) = row.close.filter(|px| px.is_finite() && *px > 0.0) {
        state.close = Some(close);
    }
    if let Some(vwap) = row.vwap.filter(|px| px.is_finite() && *px > 0.0) {
        state.vwap = Some(vwap);
    }
    if let Some(vwap) = row.buy_vwap.filter(|px| px.is_finite() && *px > 0.0) {
        state.buy_vwap = Some(vwap);
    }
    if let Some(vwap) = row.sell_vwap.filter(|px| px.is_finite() && *px > 0.0) {
        state.sell_vwap = Some(vwap);
    }
    if row
        .book
        .as_ref()
        .map(|book| book.two_sided())
        .unwrap_or(false)
    {
        state.book = row.book.clone();
    }
}

pub fn is_stock_index_product(product: &str) -> bool {
    matches!(product, "IC" | "IF" | "IH" | "IM")
}

/// Equity-index continuous session ends at 15:00 Shanghai. Keep the 15:00
/// minute; drop 15:01 onward. Bond products (T/TF/TL/TS) still close 15:15.
fn product_key(contract_id: &str) -> String {
    product_id(contract_id).unwrap_or_else(|| contract_id.trim().to_ascii_uppercase())
}

pub fn after_equity_index_close(ts_sec: i64, contract_id: &str) -> bool {
    if !is_stock_index_product(&product_key(contract_id)) {
        return false;
    }
    let local = shanghai(ts_sec);
    use chrono::Timelike;
    local.hour() > 15 || (local.hour() == 15 && local.minute() >= 1)
}

/// Floor a UTC second onto its minute left edge.
pub fn minute_floor_sec(ts_sec: i64) -> i64 {
    ts_sec.div_euclid(60) * 60
}

/// Continuous minute intervals from backtest_1s second segments.
/// Each 1s segment `[first_sec, last_sec]` (inclusive) maps to minutes that
/// overlap it, matching the dense 1s grid at 1-minute resolution.
pub fn minute_segments_from_seconds(sec_segments: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut out: Vec<(i64, i64)> = Vec::new();
    for &(lo, hi) in sec_segments {
        if hi < lo {
            continue;
        }
        let start = minute_floor_sec(lo);
        let end = minute_floor_sec(hi);
        if let Some(last) = out.last_mut() {
            if start <= last.1 + 60 && !is_session_break(last.1, start) {
                last.1 = last.1.max(end);
                continue;
            }
        }
        out.push((start, end));
    }
    out
}

/// Fill empty minutes inside backtest-aligned segments.
/// Tea / lunch / overnight stay as breaks. OHLC/vwap carry. Empty minutes
/// inside a session inherit the last two-sided book; equity-index minutes
/// after 15:00 Shanghai are dropped.
pub fn fill_session_minutes(
    contract_id: &str,
    rows: Vec<BaselineMinute>,
    segments: &[(i64, i64)],
) -> Vec<BaselineMinute> {
    let mut by_ts: BTreeMap<i64, BaselineMinute> = BTreeMap::new();
    for row in rows {
        by_ts.insert(row.ts, row);
    }
    let mut out = Vec::new();
    for &(lo, hi) in segments {
        let mut state = FillState::default();
        let mut ts = lo;
        while ts <= hi {
            if after_equity_index_close(ts, contract_id) {
                ts += 60;
                continue;
            }
            if let Some(mut row) = by_ts.remove(&ts) {
                apply_fill(&mut row, &mut state);
                out.push(row);
            } else {
                out.push(empty_minute(
                    contract_id,
                    ts,
                    state.clone(),
                    state.book.clone(),
                ));
            }
            ts += 60;
        }
    }
    out
}

/// Recalculate TWAP after the minute grid has been filled. Every continuous
/// segment starts without a seed; within it, the final valid print seeds the
/// following minute, including empty trade minutes.
pub fn fill_session_minutes_with_twap(
    contract_id: &str,
    rows: Vec<BaselineMinute>,
    segments: &[(i64, i64)],
    trades: &[PrintTrade],
) -> Result<Vec<BaselineMinute>> {
    let mut filled = fill_session_minutes(contract_id, rows, segments);
    let mut trade_by_minute: BTreeMap<i64, (Vec<i64>, Vec<f64>)> = BTreeMap::new();
    for trade in trades {
        let minute = minute_left_sec(trade.ts_utc_ns);
        let entry = trade_by_minute.entry(minute).or_default();
        entry.0.push((trade.ts_utc_ns / 1_000) as i64);
        entry.1.push(trade.price);
    }

    let mut row_index = 0usize;
    for &(lo, hi) in segments {
        let mut prior_price = None;
        let mut ts = lo;
        while ts <= hi {
            if after_equity_index_close(ts, contract_id) {
                ts += 60;
                continue;
            }
            let row = filled.get_mut(row_index).ok_or_else(|| {
                anyhow::anyhow!("missing filled minute {ts} while recalculating TWAP")
            })?;
            if row.ts != ts {
                bail!(
                    "filled minute order mismatch while recalculating TWAP: got {}, want {ts}",
                    row.ts
                );
            }
            let start_us = ts * 1_000_000;
            let end_us = (ts + 60) * 1_000_000;
            let (timestamps_us, prices) = trade_by_minute
                .get(&ts)
                .map(|(timestamps_us, prices)| (timestamps_us.as_slice(), prices.as_slice()))
                .unwrap_or((&[], &[]));
            let prints = normalized_twap_prints(timestamps_us, prices, start_us, end_us);
            row.twap = twap_from_prints(&prints, start_us, end_us, prior_price);
            if let Some((_, price)) = prints.last() {
                prior_price = Some(*price);
            }
            row_index += 1;
            ts += 60;
        }
    }
    if row_index != filled.len() {
        bail!(
            "filled minute count mismatch while recalculating TWAP: consumed {row_index}, have {}",
            filled.len()
        );
    }
    Ok(filled)
}

/// Drop equity-index minutes after 15:00 and carry the last two-sided book
/// onto empty minutes inside a day file. Used to repair already-written parquet.
pub fn repair_session_books(rows: Vec<BaselineMinute>) -> Vec<BaselineMinute> {
    let mut by_id: BTreeMap<String, Vec<BaselineMinute>> = BTreeMap::new();
    for row in rows {
        by_id.entry(row.contract_id.clone()).or_default().push(row);
    }
    let mut out = Vec::new();
    for mut series in by_id.into_values() {
        series.sort_by_key(|row| row.ts);
        let mut last_book: Option<Book5> = None;
        let mut last_ts: Option<i64> = None;
        for mut row in series {
            if after_equity_index_close(row.ts, &row.contract_id) {
                last_book = None;
                last_ts = None;
                continue;
            }
            if let Some(prev) = last_ts {
                if is_session_break(prev, row.ts) {
                    last_book = None;
                }
            }
            let missing_book = row
                .book
                .as_ref()
                .map(|book| !book.two_sided())
                .unwrap_or(true);
            if missing_book {
                row.book = last_book.clone();
                if row
                    .mid_price
                    .filter(|px| px.is_finite() && *px > 0.0)
                    .is_none()
                {
                    row.mid_price = row.book.clone().and_then(Book5::mid);
                }
            }
            if row
                .book
                .as_ref()
                .map(|book| book.two_sided())
                .unwrap_or(false)
            {
                last_book = row.book.clone();
            }
            last_ts = Some(row.ts);
            out.push(row);
        }
    }
    out.sort_by(|a, b| a.contract_id.cmp(&b.contract_id).then(a.ts.cmp(&b.ts)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(sec: i64, nano: u32, price: f64, volume: f64, aggressor: u8) -> PrintTrade {
        PrintTrade {
            ts_utc_ns: (sec as u64) * 1_000_000_000 + nano as u64,
            price,
            volume,
            amount: price * volume,
            aggressor,
        }
    }

    fn depth(sec: i64, bid: f64, ask: f64) -> DepthRecord {
        DepthRecord {
            instrument: "rb2405".into(),
            ts_utc_ns: (sec as u64) * 1_000_000_000,
            bid_prices: [Some(bid), None, None, None, None],
            bid_sizes: [Some(1.0), None, None, None, None],
            ask_prices: [Some(ask), None, None, None, None],
            ask_sizes: [Some(1.0), None, None, None, None],
        }
    }

    #[test]
    fn unknown_aggressor_splits_fifty_fifty() {
        let trades = [trade(100, 0, 10.0, 4.0, 0)];
        let rows = synthesize_minutes("rb2405", &trades, &[], 1.0).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].buy_volume, 2.0);
        assert_eq!(rows[0].sell_volume, 2.0);
        assert_eq!(rows[0].net_buy_volume, 0.0);
        assert_eq!(rows[0].buy_count, 0.5);
        assert_eq!(rows[0].count, 1.0);
    }

    #[test]
    fn twap_holds_last_print_to_bucket_end() {
        let start = 0i64;
        let trades = [
            trade(start, 0, 100.0, 1.0, 1),
            trade(start + 30, 0, 110.0, 1.0, 1),
        ];
        let rows = synthesize_minutes("rb2405", &trades, &[], 1.0).unwrap();
        let twap = rows[0].twap.unwrap();
        assert!((twap - 105.0).abs() < 1e-9);
    }

    #[test]
    fn twap_carries_last_print_within_a_segment_only() {
        let trades = [
            trade(59, 0, 100.0, 1.0, 1),
            trade(65, 0, 100.0, 1.0, 1),
            trade(80, 500_000_000, 101.0, 1.0, 1),
            trade(110, 0, 98.0, 1.0, 1),
            trade(110, 0, 99.0, 1.0, 1),
            trade(305, 0, 104.0, 1.0, 1),
        ];
        let sparse = synthesize_minutes("rb2405", &trades, &[], 1.0).unwrap();
        let rows =
            fill_session_minutes_with_twap("rb2405", sparse, &[(0, 120), (300, 300)], &trades)
                .unwrap();
        assert!((rows[1].twap.unwrap() - 100.325).abs() < 1e-9);
        assert_eq!(rows[2].twap, Some(99.0));
        // The second segment starts unseeded, so its first print at :05 only
        // contributes the remaining 55 seconds of the minute.
        assert!((rows[3].twap.unwrap() - 104.0 * 55.0 / 60.0).abs() < 1e-9);
    }

    #[test]
    fn grid_twap_keeps_adjacent_state_and_resets_on_a_gap() {
        let trades = vec![
            PrintTrade {
                ts_utc_ns: 59_000_000_000,
                price: 100.0,
                volume: 1.0,
                amount: 100.0,
                aggressor: 1,
            },
            PrintTrade {
                ts_utc_ns: 185_000_000_000,
                price: 104.0,
                volume: 1.0,
                amount: 104.0,
                aggressor: 1,
            },
        ];
        let mut state = TwapGridState::default();
        let values = twap_for_existing_grid(&[0, 60, 180], &trades, &mut state).unwrap();
        assert_eq!(values[0], Some(100.0 / 60.0));
        assert_eq!(values[1], Some(100.0));
        assert_eq!(values[2], Some(104.0 * 55.0 / 60.0));
        assert_eq!(state.last_price, Some(104.0));
    }

    #[test]
    fn vwap_divides_by_volume_multiple() {
        let trades = [trade(100, 0, 10.0, 2.0, 1)];
        let rows = synthesize_minutes("rb2405", &trades, &[], 10.0).unwrap();
        assert_eq!(rows[0].amount, 20.0);
        assert_eq!(rows[0].vwap, Some(1.0));
        assert_eq!(rows[0].buy_vwap, Some(1.0));
        assert_eq!(rows[0].close, Some(10.0));
    }

    #[test]
    fn rewrite_trade_minute_recomputed() {
        let mut row = minute(0, 10.0);
        row.volume = 2.0;
        row.amount = 200.0;
        row.buy_volume = 2.0;
        row.buy_amount = 200.0;
        row.vwap = Some(100.0);
        row.buy_vwap = Some(100.0);
        rewrite_quoted_vwap(&mut row, 10.0);
        assert_eq!(row.vwap, Some(10.0));
        assert_eq!(row.buy_vwap, Some(10.0));
        rewrite_quoted_vwap(&mut row, 10.0);
        assert_eq!(row.vwap, Some(10.0));
    }

    #[test]
    fn rewrite_empty_close_fallback_untouched() {
        let mut row = minute(0, 10.0);
        row.volume = 0.0;
        row.amount = 0.0;
        row.buy_volume = 0.0;
        row.buy_amount = 0.0;
        row.vwap = Some(10.0);
        row.buy_vwap = Some(10.0);
        rewrite_quoted_vwap(&mut row, 10.0);
        assert_eq!(row.vwap, Some(10.0));
    }

    #[test]
    fn rewrite_empty_carried_amount_vwap_scaled() {
        let mut row = minute(0, 10.0);
        row.volume = 0.0;
        row.amount = 0.0;
        row.buy_volume = 0.0;
        row.buy_amount = 0.0;
        row.vwap = Some(100.0);
        row.buy_vwap = Some(100.0);
        rewrite_quoted_vwap(&mut row, 10.0);
        assert_eq!(row.vwap, Some(10.0));
        assert_eq!(row.buy_vwap, Some(10.0));
    }

    #[test]
    fn last_depth_in_minute_is_mid_price() {
        let depths = [depth(10, 100.0, 102.0), depth(40, 101.0, 103.0)];
        let rows = synthesize_minutes("rb2405", &[], &depths, 1.0).unwrap();
        assert_eq!(rows[0].mid_price, Some(102.0));
        assert_eq!(rows[0].book.as_ref().unwrap().bid_prices[0], Some(101.0));
    }

    fn minute(ts: i64, close: f64) -> BaselineMinute {
        BaselineMinute {
            contract_id: "rb2405".into(),
            ts,
            open: Some(close),
            high: Some(close),
            low: Some(close),
            close: Some(close),
            volume: 1.0,
            amount: close,
            avg_amount: close,
            count: 1.0,
            buy_count: 1.0,
            sell_count: 0.0,
            buy_amount: close,
            sell_amount: 0.0,
            buy_volume: 1.0,
            sell_volume: 0.0,
            vwap: Some(close),
            buy_vwap: Some(close),
            sell_vwap: None,
            twap: Some(close),
            mid_price: Some(close),
            net_buy_amount: close,
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
        }
    }

    #[test]
    fn fill_inside_segment_keeps_prior_close() {
        let filled = fill_session_minutes(
            "rb2405",
            vec![minute(0, 1.0), minute(120, 2.0)],
            &[(0, 120)],
        );
        assert_eq!(
            filled.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![0, 60, 120]
        );
        assert_eq!(filled[1].close, Some(1.0));
        assert_eq!(filled[1].volume, 0.0);
        assert!(filled[1].twap.is_none());
        assert!(filled[1].book.is_none());
    }

    #[test]
    fn fill_does_not_cross_session_break() {
        let filled = fill_session_minutes(
            "rb2405",
            vec![minute(0, 1.0), minute(1_800, 2.0)],
            &[(0, 0), (1_800, 1_800)],
        );
        assert_eq!(
            filled.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![0, 1_800]
        );
    }

    fn two_sided_book(bid: f64, ask: f64) -> Book5 {
        Book5 {
            bid_prices: [Some(bid), None, None, None, None],
            bid_sizes: [Some(1.0), None, None, None, None],
            ask_prices: [Some(ask), None, None, None, None],
            ask_sizes: [Some(1.0), None, None, None, None],
        }
    }

    #[test]
    fn fill_carries_last_book_on_empty_minute() {
        let mut first = minute(0, 1.0);
        first.book = Some(two_sided_book(10.0, 11.0));
        let filled = fill_session_minutes("rb2405", vec![first, minute(120, 2.0)], &[(0, 120)]);
        assert_eq!(filled[1].ts, 60);
        assert_eq!(filled[1].volume, 0.0);
        assert_eq!(filled[1].book.as_ref().unwrap().bid_prices[0], Some(10.0));
        assert_eq!(filled[1].mid_price, Some(10.5));
        assert!(filled[1].twap.is_none());
    }

    fn shanghai_ts(hour: u32, minute: u32) -> i64 {
        use chrono::TimeZone;
        chrono_tz::Asia::Shanghai
            .with_ymd_and_hms(2024, 1, 2, hour, minute, 0)
            .single()
            .unwrap()
            .timestamp()
    }

    #[test]
    fn equity_index_drops_after_1500() {
        let ts_1500 = shanghai_ts(15, 0);
        let ts_1501 = shanghai_ts(15, 1);
        let ts_1525 = shanghai_ts(15, 25);
        let mut open = minute(ts_1500, 1.0);
        open.book = Some(two_sided_book(10.0, 11.0));
        let filled = fill_session_minutes(
            "IF2401",
            vec![open, minute(ts_1525, 1.0)],
            &[(ts_1500, ts_1525)],
        );
        assert!(filled.iter().all(|row| row.ts <= ts_1500));
        assert!(filled.iter().all(|row| row.ts != ts_1501));
        assert_eq!(filled.last().unwrap().ts, ts_1500);
    }

    #[test]
    fn bond_keeps_1515() {
        let ts_1514 = shanghai_ts(15, 14);
        let filled =
            fill_session_minutes("T2403", vec![minute(ts_1514, 1.0)], &[(ts_1514, ts_1514)]);
        assert_eq!(filled.len(), 1);
    }

    #[test]
    fn repair_fills_book_per_contract() {
        let mut a = minute(0, 1.0);
        a.contract_id = "rb2401".into();
        a.book = Some(two_sided_book(10.0, 11.0));
        let mut b = minute(60, 1.0);
        b.contract_id = "rb2401".into();
        b.volume = 0.0;
        b.book = None;
        let mut other = minute(60, 9.0);
        other.contract_id = "rb2405".into();
        other.volume = 0.0;
        other.book = None;
        let out = repair_session_books(vec![a, b, other]);
        let rb2401 = out
            .iter()
            .find(|row| row.contract_id == "rb2401" && row.ts == 60)
            .unwrap();
        assert_eq!(rb2401.book.as_ref().unwrap().bid_prices[0], Some(10.0));
        let rb2405 = out.iter().find(|row| row.contract_id == "rb2405").unwrap();
        assert!(rb2405.book.is_none());
    }

    #[test]
    fn minute_segments_follow_second_grid() {
        let segs = minute_segments_from_seconds(&[(10, 125), (2000, 2060)]);
        assert_eq!(segs, vec![(0, 120), (1980, 2040)]);
    }

    #[test]
    fn linear_percentile_matches_numpy() {
        let xs = [1.0, 2.0, 3.0, 4.0];
        assert!((linear_percentile(&xs, 0.5).unwrap() - 2.5).abs() < 1e-12);
        assert!((linear_percentile(&xs, 0.9).unwrap() - 3.7).abs() < 1e-12);
        assert_eq!(linear_percentile(&[7.0], 0.5), Some(7.0));
        assert!(linear_percentile(&[], 0.5).is_none());
    }

    #[test]
    fn size_bucket_splits_unknown_aggressor() {
        let mut sizes = SizeBuckets::default();
        sizes.add(10.0, 0, 20.0, 50.0);
        sizes.add(30.0, 1, 20.0, 50.0);
        sizes.add(80.0, 2, 20.0, 50.0);
        assert_eq!(sizes.small_order, 10.0);
        assert_eq!(sizes.small_buy, 5.0);
        assert_eq!(sizes.small_sell, 5.0);
        assert_eq!(sizes.medium_order, 30.0);
        assert_eq!(sizes.medium_buy, 30.0);
        assert_eq!(sizes.large_order, 80.0);
        assert_eq!(sizes.large_sell, 80.0);
        let (nl, nm, ns) = sizes.nets();
        assert_eq!(nl, -80.0);
        assert_eq!(nm, 30.0);
        assert_eq!(ns, 0.0);
    }
}
