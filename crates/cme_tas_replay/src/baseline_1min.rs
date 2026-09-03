//! CN-aligned contract-level CME `baseline_data_1min` aggregation.

use anyhow::{bail, Result};
use std::collections::BTreeMap;

use crate::{AGGRESSOR_BUY, AGGRESSOR_IMPLIED, AGGRESSOR_SELL};

pub const BASELINE_DEPTH_LEVELS: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SizeThresholds {
    pub p50: f64,
    pub p90: f64,
}

impl SizeThresholds {
    pub fn new(p50: f64, p90: f64) -> Result<Self> {
        if !(p50.is_finite() && p90.is_finite() && p50 > 0.0 && p90 >= p50) {
            bail!("size thresholds must satisfy 0 < p50 <= p90, got {p50}, {p90}");
        }
        Ok(Self { p50, p90 })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
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
    /// Add one printable CME trade. Implied prints contribute to the total
    /// size bucket but not to either directional side.
    pub fn add(&mut self, amount: f64, aggressor: u8, thresholds: SizeThresholds) -> Result<()> {
        if !(amount.is_finite() && amount > 0.0) {
            bail!("size-bucket amount must be finite and positive, got {amount}");
        }
        if !matches!(
            aggressor,
            AGGRESSOR_IMPLIED | AGGRESSOR_BUY | AGGRESSOR_SELL
        ) {
            bail!("invalid size-bucket aggressor {aggressor}");
        }
        let (order, buy, sell) = if amount >= thresholds.p90 {
            (
                &mut self.large_order,
                &mut self.large_buy,
                &mut self.large_sell,
            )
        } else if amount >= thresholds.p50 {
            (
                &mut self.medium_order,
                &mut self.medium_buy,
                &mut self.medium_sell,
            )
        } else {
            (
                &mut self.small_order,
                &mut self.small_buy,
                &mut self.small_sell,
            )
        };
        *order += amount;
        match aggressor {
            AGGRESSOR_BUY => *buy += amount,
            AGGRESSOR_SELL => *sell += amount,
            AGGRESSOR_IMPLIED => {}
            _ => unreachable!(),
        }
        Ok(())
    }

    pub fn total(self) -> f64 {
        self.large_order + self.medium_order + self.small_order
    }

    pub fn directional_total(self) -> f64 {
        self.large_buy
            + self.large_sell
            + self.medium_buy
            + self.medium_sell
            + self.small_buy
            + self.small_sell
    }

    pub fn nets(self) -> (f64, f64, f64) {
        (
            self.large_buy - self.large_sell,
            self.medium_buy - self.medium_sell,
            self.small_buy - self.small_sell,
        )
    }
}

/// Linear-interpolated sample percentile, matching NumPy's default method.
pub fn linear_percentile(sorted: &[f64], percentile: f64) -> Option<f64> {
    if sorted.is_empty() || !(0.0..=1.0).contains(&percentile) {
        return None;
    }
    if sorted.len() == 1 {
        return Some(sorted[0]);
    }
    let position = percentile * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        Some(sorted[lower])
    } else {
        let weight = position - lower as f64;
        Some(sorted[lower] * (1.0 - weight) + sorted[upper] * weight)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BaselineTrade {
    pub event_ns: u64,
    pub source_order: Vec<u8>,
    pub price: f64,
    pub volume: f64,
    pub aggressor: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BaselineSpecial {
    pub event_ns: u64,
    pub volume: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Book10 {
    pub bid_prices: [Option<f64>; BASELINE_DEPTH_LEVELS],
    pub bid_sizes: [Option<f64>; BASELINE_DEPTH_LEVELS],
    pub ask_prices: [Option<f64>; BASELINE_DEPTH_LEVELS],
    pub ask_sizes: [Option<f64>; BASELINE_DEPTH_LEVELS],
}

impl Book10 {
    pub fn valid(&self) -> bool {
        match (
            self.bid_prices[0],
            self.bid_sizes[0],
            self.ask_prices[0],
            self.ask_sizes[0],
        ) {
            (Some(bid), Some(bid_size), Some(ask), Some(ask_size)) => {
                bid.is_finite()
                    && bid > 0.0
                    && ask.is_finite()
                    && ask >= bid
                    && bid_size.is_finite()
                    && bid_size >= 0.0
                    && ask_size.is_finite()
                    && ask_size >= 0.0
            }
            _ => false,
        }
    }

    pub fn mid(&self) -> Option<f64> {
        self.valid()
            .then(|| (self.bid_prices[0].unwrap() + self.ask_prices[0].unwrap()) / 2.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BaselineMinute {
    pub contract_id: String,
    pub ric: String,
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
    pub special_count: f64,
    pub special_volume: f64,
    pub implied_count: f64,
    pub implied_volume: f64,
    pub implied_amount: f64,
    pub implied_vwap: Option<f64>,
    pub implied_twap: Option<f64>,
    pub book: Book10,
}

#[derive(Clone, Debug, Default)]
struct FillState {
    close: Option<f64>,
    vwap: Option<f64>,
    buy_vwap: Option<f64>,
    sell_vwap: Option<f64>,
    implied_vwap: Option<f64>,
    book: Option<Book10>,
}

fn minute_left_sec(event_ns: u64) -> i64 {
    (event_ns / 1_000_000_000 / 60 * 60) as i64
}

fn valid_price(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite() && *value > 0.0)
}

fn quoted_vwap(amount: f64, volume: f64, multiplier: f64) -> Option<f64> {
    if volume > 0.0 {
        let value = amount / volume / multiplier;
        value.is_finite().then_some(value)
    } else {
        None
    }
}

fn exact_twap(trades: &[BaselineTrade], minute: i64, prior_price: Option<f64>) -> Option<f64> {
    let start_ns = u64::try_from(minute).ok()?.checked_mul(1_000_000_000)?;
    let end_ns = u64::try_from(minute + 60)
        .ok()?
        .checked_mul(1_000_000_000)?;
    let mut previous_ns = start_ns;
    let mut previous_price = valid_price(prior_price);
    let mut weighted = 0.0;
    for trade in trades {
        if let Some(price) = previous_price {
            weighted += price * trade.event_ns.saturating_sub(previous_ns) as f64;
        }
        previous_ns = trade.event_ns;
        previous_price = Some(trade.price);
    }
    let previous_price = previous_price?;
    weighted += previous_price * end_ns.saturating_sub(previous_ns) as f64;
    Some(weighted / 60_000_000_000.0)
}

fn side_sums(trades: &[BaselineTrade], multiplier: f64) -> (f64, f64) {
    trades.iter().fold((0.0, 0.0), |(volume, amount), trade| {
        (
            volume + trade.volume,
            amount + trade.price * trade.volume * multiplier,
        )
    })
}

fn carried(value: Option<f64>, previous: Option<f64>, close: Option<f64>) -> Option<f64> {
    valid_price(value)
        .or_else(|| valid_price(previous))
        .or_else(|| valid_price(close))
}

pub fn build_minutes(
    contract_id: &str,
    ric: &str,
    segments: &[(i64, i64)],
    trades: &[BaselineTrade],
    specials: &[BaselineSpecial],
    books: &BTreeMap<i64, Book10>,
    volume_multiple: f64,
) -> Result<Vec<BaselineMinute>> {
    build_minutes_inner(
        contract_id,
        ric,
        segments,
        trades,
        specials,
        Some(books),
        volume_multiple,
    )
}

/// Build a TAS-only minute grid. Quote-derived fields deliberately remain
/// null; no synthetic L1 or depth state is carried into the result.
pub fn build_trade_only_minutes(
    contract_id: &str,
    ric: &str,
    segments: &[(i64, i64)],
    trades: &[BaselineTrade],
    specials: &[BaselineSpecial],
    volume_multiple: f64,
) -> Result<Vec<BaselineMinute>> {
    build_minutes_inner(
        contract_id,
        ric,
        segments,
        trades,
        specials,
        None,
        volume_multiple,
    )
}

fn build_minutes_inner(
    contract_id: &str,
    ric: &str,
    segments: &[(i64, i64)],
    trades: &[BaselineTrade],
    specials: &[BaselineSpecial],
    books: Option<&BTreeMap<i64, Book10>>,
    volume_multiple: f64,
) -> Result<Vec<BaselineMinute>> {
    if !(volume_multiple.is_finite() && volume_multiple > 0.0) {
        bail!("volume_multiple must be finite and positive, got {volume_multiple}");
    }
    let mut trade_by_minute: BTreeMap<i64, Vec<BaselineTrade>> = BTreeMap::new();
    for trade in trades {
        if !matches!(
            trade.aggressor,
            AGGRESSOR_IMPLIED | AGGRESSOR_BUY | AGGRESSOR_SELL
        ) {
            bail!("invalid aggressor {}", trade.aggressor);
        }
        if !(trade.price.is_finite() && trade.price > 0.0) {
            bail!("invalid trade price {}", trade.price);
        }
        if !(trade.volume.is_finite() && trade.volume > 0.0) {
            bail!("invalid trade volume {}", trade.volume);
        }
        trade_by_minute
            .entry(minute_left_sec(trade.event_ns))
            .or_default()
            .push(trade.clone());
    }
    for rows in trade_by_minute.values_mut() {
        rows.sort_by(|left, right| {
            left.event_ns
                .cmp(&right.event_ns)
                .then(left.source_order.cmp(&right.source_order))
        });
    }
    let mut special_by_minute: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
    for special in specials {
        if !(special.volume.is_finite() && special.volume > 0.0) {
            bail!("invalid special volume {}", special.volume);
        }
        let entry = special_by_minute
            .entry(minute_left_sec(special.event_ns))
            .or_default();
        entry.0 += 1.0;
        entry.1 += special.volume;
    }

    let mut output = Vec::new();
    for &(start, end) in segments {
        if start % 60 != 0 || end % 60 != 0 || end < start {
            bail!("invalid minute segment [{start}, {end}]");
        }
        let mut state = FillState::default();
        let mut prior_twap = None;
        let mut prior_implied_twap = None;
        let mut minute = start;
        while minute <= end {
            let book = if let Some(books) = books {
                if let Some(book) = books.get(&minute).filter(|book| book.valid()) {
                    state.book = Some(book.clone());
                }
                let Some(book) = state.book.clone() else {
                    minute += 60;
                    continue;
                };
                book
            } else {
                Book10::default()
            };
            let minute_trades = trade_by_minute.get(&minute).cloned().unwrap_or_default();
            let buy = minute_trades
                .iter()
                .filter(|trade| trade.aggressor == AGGRESSOR_BUY)
                .cloned()
                .collect::<Vec<_>>();
            let sell = minute_trades
                .iter()
                .filter(|trade| trade.aggressor == AGGRESSOR_SELL)
                .cloned()
                .collect::<Vec<_>>();
            let implied = minute_trades
                .iter()
                .filter(|trade| trade.aggressor == AGGRESSOR_IMPLIED)
                .cloned()
                .collect::<Vec<_>>();
            let (volume, amount) = side_sums(&minute_trades, volume_multiple);
            let (buy_volume, buy_amount) = side_sums(&buy, volume_multiple);
            let (sell_volume, sell_amount) = side_sums(&sell, volume_multiple);
            let (implied_volume, implied_amount) = side_sums(&implied, volume_multiple);
            let count = minute_trades.len() as f64;
            let close = minute_trades
                .last()
                .map(|trade| trade.price)
                .or(state.close);
            let open = minute_trades.first().map(|trade| trade.price).or(close);
            let high = minute_trades
                .iter()
                .map(|trade| trade.price)
                .reduce(f64::max)
                .or(close);
            let low = minute_trades
                .iter()
                .map(|trade| trade.price)
                .reduce(f64::min)
                .or(close);
            if let Some(price) = minute_trades.last().map(|trade| trade.price) {
                state.close = Some(price);
            }

            let vwap = carried(
                quoted_vwap(amount, volume, volume_multiple),
                state.vwap,
                close,
            );
            let buy_vwap = carried(
                quoted_vwap(buy_amount, buy_volume, volume_multiple),
                state.buy_vwap,
                close,
            );
            let sell_vwap = carried(
                quoted_vwap(sell_amount, sell_volume, volume_multiple),
                state.sell_vwap,
                close,
            );
            let implied_vwap = carried(
                quoted_vwap(implied_amount, implied_volume, volume_multiple),
                state.implied_vwap,
                close,
            );
            if volume > 0.0 {
                state.vwap = vwap;
            }
            if buy_volume > 0.0 {
                state.buy_vwap = buy_vwap;
            }
            if sell_volume > 0.0 {
                state.sell_vwap = sell_vwap;
            }
            if implied_volume > 0.0 {
                state.implied_vwap = implied_vwap;
            }
            let twap = exact_twap(&minute_trades, minute, prior_twap);
            let implied_twap = exact_twap(&implied, minute, prior_implied_twap);
            if let Some(price) = minute_trades.last().map(|trade| trade.price) {
                prior_twap = Some(price);
            }
            if let Some(price) = implied.last().map(|trade| trade.price) {
                prior_implied_twap = Some(price);
            }
            let directed = buy_amount + sell_amount;
            let (special_count, special_volume) =
                special_by_minute.get(&minute).copied().unwrap_or_default();
            output.push(BaselineMinute {
                contract_id: contract_id.to_string(),
                ric: ric.to_string(),
                ts: minute,
                open,
                high,
                low,
                close,
                volume,
                amount,
                avg_amount: if count > 0.0 { amount / count } else { 0.0 },
                count,
                buy_count: buy.len() as f64,
                sell_count: sell.len() as f64,
                buy_amount,
                sell_amount,
                buy_volume,
                sell_volume,
                vwap,
                buy_vwap,
                sell_vwap,
                twap,
                mid_price: book.mid(),
                net_buy_amount: buy_amount - sell_amount,
                net_buy_volume: buy_volume - sell_volume,
                net_buy_pct: Some(if directed > 0.0 {
                    (buy_amount - sell_amount) / directed
                } else {
                    0.0
                }),
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
                special_count,
                special_volume,
                implied_count: implied.len() as f64,
                implied_volume,
                implied_amount,
                implied_vwap,
                implied_twap,
                book,
            });
            minute += 60;
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(sec: u64, price: f64, volume: f64, aggressor: u8) -> BaselineTrade {
        BaselineTrade {
            event_ns: sec * 1_000_000_000,
            source_order: sec.to_be_bytes().to_vec(),
            price,
            volume,
            aggressor,
        }
    }

    fn book(bid: f64, ask: f64) -> Book10 {
        let mut book = Book10::default();
        book.bid_prices[0] = Some(bid);
        book.bid_sizes[0] = Some(2.0);
        book.ask_prices[0] = Some(ask);
        book.ask_sizes[0] = Some(3.0);
        book
    }

    #[test]
    fn cn_fill_special_implied_and_multiplier() {
        let rows = build_minutes(
            "CME:ES:2024-03",
            "ESH24",
            &[(0, 120)],
            &[
                trade(10, 100.0, 2.0, AGGRESSOR_BUY),
                trade(30, 102.0, 3.0, AGGRESSOR_IMPLIED),
                trade(125, 105.0, 1.0, AGGRESSOR_SELL),
            ],
            &[BaselineSpecial {
                event_ns: 65_000_000_000,
                volume: 7.0,
            }],
            &BTreeMap::from([(0, book(99.0, 101.0)), (120, book(104.0, 106.0))]),
            50.0,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].amount, 25_300.0);
        assert_eq!(rows[0].implied_volume, 3.0);
        assert_eq!(rows[0].implied_vwap, Some(102.0));
        assert_eq!(rows[1].special_volume, 7.0);
        assert_eq!(rows[1].close, Some(102.0));
        assert_eq!(rows[1].vwap, Some(101.2));
        assert_eq!(rows[1].twap, Some(102.0));
        assert_eq!(rows[1].book.bid_prices[0], Some(99.0));
        assert_eq!(rows[2].mid_price, Some(105.0));
    }

    #[test]
    fn starts_at_first_book_and_resets_each_segment() {
        let rows = build_minutes(
            "CBOT:YM:2026-03",
            "YMH26",
            &[(0, 120), (300, 420)],
            &[trade(305, 40_000.0, 1.0, AGGRESSOR_BUY)],
            &[],
            &BTreeMap::from([
                (60, book(39_999.0, 40_001.0)),
                (360, book(40_009.0, 40_011.0)),
            ]),
            5.0,
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
            [60, 120, 360, 420]
        );
        assert_eq!(rows[0].close, None);
        assert_eq!(rows[2].close, None);
    }

    #[test]
    fn trade_only_keeps_grid_and_has_no_synthetic_book() {
        let rows = build_trade_only_minutes(
            "CME:ES:2024-03",
            "ESH24",
            &[(0, 120)],
            &[trade(10, 100.0, 2.0, AGGRESSOR_BUY)],
            &[],
            50.0,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].volume, 2.0);
        assert_eq!(rows[0].mid_price, None);
        assert_eq!(rows[0].book.bid_prices[0], None);
        assert_eq!(rows[1].close, Some(100.0));
        assert_eq!(rows[1].mid_price, None);
    }

    #[test]
    fn twap_carries_within_a_segment_but_not_across_a_break() {
        let rows = build_trade_only_minutes(
            "CME:ES:2024-03",
            "ESH24",
            &[(0, 120), (300, 300)],
            &[
                trade(59, 100.0, 1.0, AGGRESSOR_BUY),
                trade(65, 100.0, 1.0, AGGRESSOR_BUY),
                BaselineTrade {
                    event_ns: 80_500_000_000,
                    source_order: b"80.5".to_vec(),
                    price: 101.0,
                    volume: 1.0,
                    aggressor: AGGRESSOR_BUY,
                },
                trade(110, 98.0, 1.0, AGGRESSOR_BUY),
                BaselineTrade {
                    event_ns: 110_000_000_000,
                    source_order: b"later".to_vec(),
                    price: 99.0,
                    volume: 1.0,
                    aggressor: AGGRESSOR_BUY,
                },
                trade(305, 104.0, 1.0, AGGRESSOR_BUY),
            ],
            &[],
            50.0,
        )
        .unwrap();
        assert!((rows[1].twap.unwrap() - 100.325).abs() < 1e-12);
        assert_eq!(rows[2].twap, Some(99.0));
        assert!((rows[3].twap.unwrap() - 104.0 * 55.0 / 60.0).abs() < 1e-12);
    }

    #[test]
    fn size_buckets_include_implied_only_in_total() {
        let thresholds = SizeThresholds::new(20.0, 50.0).unwrap();
        let mut buckets = SizeBuckets::default();
        buckets.add(10.0, AGGRESSOR_BUY, thresholds).unwrap();
        buckets.add(30.0, AGGRESSOR_IMPLIED, thresholds).unwrap();
        buckets.add(80.0, AGGRESSOR_SELL, thresholds).unwrap();
        assert_eq!(buckets.small_order, 10.0);
        assert_eq!(buckets.medium_order, 30.0);
        assert_eq!(buckets.large_order, 80.0);
        assert_eq!(buckets.small_buy, 10.0);
        assert_eq!(buckets.medium_buy, 0.0);
        assert_eq!(buckets.medium_sell, 0.0);
        assert_eq!(buckets.large_sell, 80.0);
        assert_eq!(buckets.total(), 120.0);
        assert_eq!(buckets.directional_total(), 90.0);
        assert_eq!(buckets.nets(), (-80.0, 0.0, 10.0));
    }

    #[test]
    fn size_percentile_matches_numpy_default_examples() {
        let values = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(linear_percentile(&values, 0.5), Some(2.5));
        assert!((linear_percentile(&values, 0.9).unwrap() - 3.7).abs() < 1e-12);
        assert_eq!(linear_percentile(&[7.0], 0.9), Some(7.0));
        assert_eq!(linear_percentile(&[], 0.5), None);
    }
}
