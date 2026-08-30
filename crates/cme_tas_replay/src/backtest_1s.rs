//! Causal dense 1s rows for the six-product TAS backtest.

use anyhow::{bail, Result};
use std::collections::BTreeMap;

use crate::{AGGRESSOR_BUY, AGGRESSOR_IMPLIED, AGGRESSOR_SELL};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Quote {
    pub sec: i64,
    pub bid: f64,
    pub bid_size: f64,
    pub ask: f64,
    pub ask_size: f64,
}

impl Quote {
    pub fn valid(self) -> bool {
        self.bid.is_finite()
            && self.bid_size.is_finite()
            && self.ask.is_finite()
            && self.ask_size.is_finite()
            && self.bid > 0.0
            && self.ask >= self.bid
            && self.bid_size >= 0.0
            && self.ask_size >= 0.0
    }

    fn midp(self) -> f64 {
        (self.bid + self.ask) / 2.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Trade {
    pub event_ns: i64,
    pub price: f64,
    pub aggressor: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Interval {
    pub start: i64,
    pub end: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BacktestRow {
    pub contract_id: String,
    pub ric: String,
    pub ts: i64,
    pub bid0p: f64,
    pub bid0v: f64,
    pub ask0p: f64,
    pub ask0v: f64,
    pub buy_high: Option<f64>,
    pub sell_low: Option<f64>,
    pub close: f64,
    pub midp: f64,
}

#[derive(Clone, Copy, Debug, Default)]
struct TradeBucket {
    buy_high: Option<f64>,
    sell_low: Option<f64>,
    close: Option<f64>,
}

impl TradeBucket {
    fn add(&mut self, trade: Trade) -> Result<()> {
        if !trade.price.is_finite() || trade.price <= 0.0 {
            bail!("trade price {} is not positive finite", trade.price);
        }
        self.close = Some(trade.price);
        match trade.aggressor {
            AGGRESSOR_BUY => {
                self.buy_high = Some(self.buy_high.map_or(trade.price, |v| v.max(trade.price)));
            }
            AGGRESSOR_SELL => {
                self.sell_low = Some(self.sell_low.map_or(trade.price, |v| v.min(trade.price)));
            }
            AGGRESSOR_IMPLIED => {
                self.buy_high = Some(self.buy_high.map_or(trade.price, |v| v.max(trade.price)));
                self.sell_low = Some(self.sell_low.map_or(trade.price, |v| v.min(trade.price)));
            }
            other => bail!("aggressor {other} is not 0/1/2"),
        }
        Ok(())
    }
}

fn trade_buckets(trades: &[Trade], interval: Interval) -> Result<BTreeMap<i64, TradeBucket>> {
    let mut sorted = trades.to_vec();
    sorted.sort_by_key(|trade| trade.event_ns);
    let mut buckets = BTreeMap::<i64, TradeBucket>::new();
    for trade in sorted {
        let sec = trade.event_ns.div_euclid(1_000_000_000);
        if sec < interval.start || sec > interval.end {
            continue;
        }
        buckets.entry(sec).or_default().add(trade)?;
    }
    Ok(buckets)
}

/// Special events are deliberately not an argument: they are consumed and
/// audited by the exporter, but have no price field in the 11-column output.
pub fn densify_interval(
    contract_id: &str,
    ric: &str,
    interval: Interval,
    quotes: &[Quote],
    trades: &[Trade],
) -> Result<Vec<BacktestRow>> {
    if interval.end <= interval.start {
        bail!("invalid interval [{}, {})", interval.start, interval.end);
    }
    let quote_by_sec = quotes
        .iter()
        .copied()
        .filter(|quote| quote.sec >= interval.start && quote.sec < interval.end && quote.valid())
        .map(|quote| (quote.sec, quote))
        .collect::<BTreeMap<_, _>>();
    let Some((&first_quote_sec, &first_quote)) = quote_by_sec.first_key_value() else {
        return Ok(Vec::new());
    };
    let lo = first_quote_sec + 1;
    if lo >= interval.end {
        return Ok(Vec::new());
    }

    let buckets = trade_buckets(trades, interval)?;
    let mut standing = first_quote;
    let mut previous_close: Option<f64> = None;
    let mut rows = Vec::with_capacity((interval.end - lo) as usize + 1);
    for sec in lo..interval.end {
        if let Some(update) = quote_by_sec.get(&(sec - 1)) {
            standing = *update;
        }
        let midp = standing.midp();
        let trade = buckets.get(&sec);
        let close = match trade.and_then(|bucket| bucket.close) {
            Some(price) => price,
            None if quote_by_sec.contains_key(&sec) => midp,
            None => previous_close.unwrap_or(midp),
        };
        rows.push(BacktestRow {
            contract_id: contract_id.to_string(),
            ric: ric.to_string(),
            ts: sec,
            bid0p: standing.bid,
            bid0v: standing.bid_size,
            ask0p: standing.ask,
            ask0v: standing.ask_size,
            buy_high: trade.and_then(|bucket| bucket.buy_high),
            sell_low: trade.and_then(|bucket| bucket.sell_low),
            close,
            midp,
        });
        previous_close = Some(close);
    }

    if let Some(closing) = buckets.get(&interval.end) {
        if let Some(update) = quote_by_sec.get(&(interval.end - 1)) {
            standing = *update;
        }
        rows.push(BacktestRow {
            contract_id: contract_id.to_string(),
            ric: ric.to_string(),
            ts: interval.end,
            bid0p: standing.bid,
            bid0v: standing.bid_size,
            ask0p: standing.ask,
            ask0v: standing.ask_size,
            buy_high: closing.buy_high,
            sell_low: closing.sell_low,
            close: closing
                .close
                .expect("a trade bucket always has a closing price"),
            midp: standing.midp(),
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quote(sec: i64, bid: f64, ask: f64) -> Quote {
        Quote {
            sec,
            bid,
            bid_size: 2.0,
            ask,
            ask_size: 3.0,
        }
    }

    #[test]
    fn dense_grid_is_causal_and_implied_updates_both_extremes() {
        let rows = densify_interval(
            "CME:ES:2024-03",
            "ESH24",
            Interval {
                start: 100,
                end: 105,
            },
            &[quote(100, 10.0, 12.0), quote(103, 11.0, 13.0)],
            &[
                Trade {
                    event_ns: 102_100_000_000,
                    price: 12.5,
                    aggressor: AGGRESSOR_BUY,
                },
                Trade {
                    event_ns: 102_200_000_000,
                    price: 10.5,
                    aggressor: AGGRESSOR_SELL,
                },
                Trade {
                    event_ns: 102_300_000_000,
                    price: 11.5,
                    aggressor: AGGRESSOR_IMPLIED,
                },
            ],
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
            vec![101, 102, 103, 104]
        );
        assert_eq!(rows[1].buy_high, Some(12.5));
        assert_eq!(rows[1].sell_low, Some(10.5));
        assert_eq!(rows[1].close, 11.5);
        assert_eq!(rows[3].bid0p, 11.0);
    }

    #[test]
    fn implied_only_trade_updates_both_extremes() {
        let rows = densify_interval(
            "NYMEX:CL:2024-02",
            "CLG24",
            Interval { start: 10, end: 13 },
            &[quote(10, 70.0, 70.1)],
            &[Trade {
                event_ns: 11_500_000_000,
                price: 70.05,
                aggressor: AGGRESSOR_IMPLIED,
            }],
        )
        .unwrap();
        assert_eq!(rows[0].buy_high, Some(70.05));
        assert_eq!(rows[0].sell_low, Some(70.05));
    }

    #[test]
    fn closing_trade_is_retained_without_future_book_backfill() {
        let rows = densify_interval(
            "COMEX:GC:2024-04",
            "GCJ24",
            Interval { start: 20, end: 24 },
            &[quote(22, 2000.0, 2000.5)],
            &[Trade {
                event_ns: 24_000_000_000,
                price: 2000.25,
                aggressor: AGGRESSOR_IMPLIED,
            }],
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
            vec![23, 24]
        );
    }
}
