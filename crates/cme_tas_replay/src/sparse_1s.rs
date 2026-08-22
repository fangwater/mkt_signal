//! Sparse 1s backtest bars and closed-bar ylabel from TAS trades + L1 quotes.
//!
//! Empty seconds are not emitted. Printable `cme_trade` only; Special is not
//! used. Bid/ask0 is the last valid uncrossed L1 strictly before second `t`.
//! Trades in `[t, t+1)` fill OHLC. Aggressor 1 (BID) updates `buy_high`,
//! 2 (ASK) updates `sell_low`; 0 still counts in OHLC / volume.

use anyhow::{anyhow, bail, Result};

use crate::{price_e9_to_f64, MISSING_PRICE, MISSING_VOLUME};

pub const YLABEL_HORIZON_MS: [i64; 5] = [5_000, 10_000, 30_000, 60_000, 300_000];
pub const BACKTEST_TABLE: &str = "backtest_cme_tas_1s";
pub const YLABEL_VENUE: &str = "cme_tas";

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopOfBook {
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
}

impl TopOfBook {
    pub fn midp(self) -> f64 {
        (self.bid_price + self.ask_price) / 2.0
    }

    pub fn is_valid(self) -> bool {
        self.bid_price.is_finite()
            && self.ask_price.is_finite()
            && self.bid_amount.is_finite()
            && self.ask_amount.is_finite()
            && self.bid_price > 0.0
            && self.ask_price >= self.bid_price
            && self.bid_amount >= 0.0
            && self.ask_amount >= 0.0
    }
}

pub fn top_of_book_from_quote(
    bid: i64,
    bid_size: u32,
    ask: i64,
    ask_size: u32,
) -> Option<TopOfBook> {
    let bid_price = price_e9_to_f64(bid)?;
    let ask_price = price_e9_to_f64(ask)?;
    if bid_size == MISSING_VOLUME || ask_size == MISSING_VOLUME {
        return None;
    }
    let book = TopOfBook {
        bid_price,
        bid_amount: f64::from(bid_size),
        ask_price,
        ask_amount: f64::from(ask_size),
    };
    book.is_valid().then_some(book)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradeMarket1sBar {
    pub ts_sec: i64,
    pub bid0p: f64,
    pub ask0p: f64,
    pub bid0v: f64,
    pub ask0v: f64,
    pub buy_high: f64,
    pub sell_low: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub turnover: f64,
}

impl TradeMarket1sBar {
    pub fn midp(self) -> f64 {
        if self.bid0p.is_finite() && self.ask0p.is_finite() {
            (self.bid0p + self.ask0p) / 2.0
        } else {
            f64::NAN
        }
    }

    pub fn clickhouse_values(self) -> [f64; 13] {
        [
            self.bid0p,
            self.bid0v,
            self.ask0p,
            self.ask0v,
            self.buy_high,
            self.sell_low,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.turnover,
            self.midp(),
        ]
    }
}

#[derive(Debug, Clone, Copy)]
struct OpenSecond {
    buy_high: f64,
    sell_low: f64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    turnover: f64,
    count: u32,
    book_at_open: Option<TopOfBook>,
}

impl OpenSecond {
    fn empty() -> Self {
        Self {
            buy_high: f64::NAN,
            sell_low: f64::NAN,
            open: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            close: f64::NAN,
            volume: 0.0,
            turnover: 0.0,
            count: 0,
            book_at_open: None,
        }
    }

    fn on_trade(&mut self, is_buy: Option<bool>, price: f64, amount: f64) {
        if self.count == 0 {
            self.open = price;
            self.high = price;
            self.low = price;
        } else {
            self.high = self.high.max(price);
            self.low = self.low.min(price);
        }
        self.close = price;
        self.volume += amount;
        self.turnover += price * amount;
        match is_buy {
            Some(true) => {
                self.buy_high = if self.buy_high.is_nan() {
                    price
                } else {
                    self.buy_high.max(price)
                };
            }
            Some(false) => {
                self.sell_low = if self.sell_low.is_nan() {
                    price
                } else {
                    self.sell_low.min(price)
                };
            }
            None => {}
        }
        self.count += 1;
    }

}

/// Sparse 1s aggregator. Idle seconds are jumped, not filled. A quote at
/// second `t` is the book strictly before second `t+1`. Unchanged books
/// without a trade do not emit a row.
#[derive(Debug)]
pub struct Sparse1sAggregator {
    last_book: Option<TopOfBook>,
    last_emitted_book: Option<TopOfBook>,
    next_sec: Option<i64>,
    current: OpenSecond,
}

impl Default for Sparse1sAggregator {
    fn default() -> Self {
        Self {
            last_book: None,
            last_emitted_book: None,
            next_sec: None,
            current: OpenSecond::empty(),
        }
    }
}

impl Sparse1sAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_quote(&mut self, timestamp_us: i64, book: TopOfBook) -> Result<Vec<TradeMarket1sBar>> {
        if !book.is_valid() {
            return Ok(Vec::new());
        }
        let quote_sec = timestamp_us.div_euclid(1_000_000) + 1;
        let closed = self.emit_until(quote_sec)?;
        self.last_book = Some(book);
        self.ensure_second(quote_sec);
        Ok(closed)
    }

    pub fn on_trade(
        &mut self,
        timestamp_us: i64,
        aggressor: u8,
        price: f64,
        amount: f64,
    ) -> Result<Vec<TradeMarket1sBar>> {
        // Invalid printable trades are skipped by the caller; keep a soft guard.
        if !price.is_finite() || price <= 0.0 || !amount.is_finite() || amount <= 0.0 {
            return Ok(Vec::new());
        }
        let is_buy = match aggressor {
            1 => Some(true),
            2 => Some(false),
            0 => None,
            _ => return Ok(Vec::new()),
        };
        let trade_sec = timestamp_us.div_euclid(1_000_000);
        let closed = self.emit_until(trade_sec)?;
        self.ensure_second(trade_sec);
        self.current.on_trade(is_buy, price, amount);
        Ok(closed)
    }

    pub fn finish(&mut self) -> Result<Vec<TradeMarket1sBar>> {
        match self.next_sec {
            Some(sec) => self.emit_until(sec + 1),
            None => Ok(Vec::new()),
        }
    }

    fn ensure_second(&mut self, sec: i64) {
        if self.next_sec.is_none() {
            self.next_sec = Some(sec);
            self.current.book_at_open = self.last_book;
        }
    }

    fn book_changed(current: &OpenSecond, last_emitted: Option<TopOfBook>) -> bool {
        match (current.book_at_open, last_emitted) {
            (Some(book), Some(prev)) => book != prev,
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    fn emit_until(&mut self, target_sec: i64) -> Result<Vec<TradeMarket1sBar>> {
        let Some(mut next) = self.next_sec else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        while next < target_sec {
            let traded = self.current.count > 0;
            let changed = Self::book_changed(&self.current, self.last_emitted_book);
            if traded || changed {
                if let Some(bar) = self.close_second()? {
                    out.push(bar);
                }
                next = self.next_sec.expect("next_sec after close");
            } else {
                self.current = OpenSecond::empty();
                self.next_sec = Some(target_sec);
                self.current.book_at_open = self.last_book;
                break;
            }
        }
        Ok(out)
    }

    fn close_second(&mut self) -> Result<Option<TradeMarket1sBar>> {
        let ts_sec = self
            .next_sec
            .ok_or_else(|| anyhow!("close_second without open second"))?;
        let current = std::mem::replace(&mut self.current, OpenSecond::empty());
        self.next_sec = Some(ts_sec + 1);
        self.current.book_at_open = self.last_book;
        let traded = current.count > 0;
        let (bid0p, bid0v, ask0p, ask0v) = match current.book_at_open {
            Some(book) => (book.bid_price, book.bid_amount, book.ask_price, book.ask_amount),
            None => (f64::NAN, f64::NAN, f64::NAN, f64::NAN),
        };
        let fill_price = if traded {
            current.close
        } else if let Some(book) = current.book_at_open {
            book.midp()
        } else {
            return Ok(None);
        };
        if let Some(book) = current.book_at_open {
            self.last_emitted_book = Some(book);
        }
        Ok(Some(TradeMarket1sBar {
            ts_sec,
            bid0p,
            bid0v,
            ask0p,
            ask0v,
            buy_high: current.buy_high,
            sell_low: current.sell_low,
            open: if traded { current.open } else { fill_price },
            high: if traded { current.high } else { fill_price },
            low: if traded { current.low } else { fill_price },
            close: fill_price,
            volume: if traded { current.volume } else { 0.0 },
            turnover: if traded { current.turnover } else { 0.0 },
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YlabelBar {
    pub ts_ms: i64,
    pub twap: f64,
    pub vwap: f64,
    pub midp: f64,
}

#[derive(Debug, Clone, Copy)]
struct OpenYlabelBucket {
    start_ms: i64,
    last_trade_us: i64,
    last_trade_price: f64,
    twap_area: f64,
    trade_notional: f64,
    trade_volume: f64,
    last_midp: Option<f64>,
    saw_trade: bool,
    saw_midp: bool,
}

impl OpenYlabelBucket {
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
            saw_trade: false,
            saw_midp: false,
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
        self.saw_trade = true;
    }

    fn on_midp(&mut self, midp: f64) {
        if midp.is_finite() && midp > 0.0 {
            self.last_midp = Some(midp);
            self.saw_midp = true;
        }
    }

    fn has_activity(&self) -> bool {
        self.saw_trade || self.saw_midp
    }

    fn close(mut self, end_ms: i64, horizon_us: i64) -> Option<YlabelBar> {
        if !self.has_activity() {
            return None;
        }
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
        Some(YlabelBar {
            ts_ms: end_ms,
            twap,
            vwap,
            midp: self.last_midp.unwrap_or(f64::NAN),
        })
    }
}

pub struct SparseYlabelAggregator {
    horizon_ms: i64,
    current: Option<OpenYlabelBucket>,
    last_trade_us: i64,
    last_trade_price: f64,
    last_midp: Option<f64>,
}

impl SparseYlabelAggregator {
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
        // Invalid printable trades are skipped by the caller; keep a soft guard.
        if !price.is_finite() || price <= 0.0 || !amount.is_finite() || amount <= 0.0 {
            return Ok(Vec::new());
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

    pub fn finish(&mut self) -> Result<Vec<YlabelBar>> {
        let Some(bucket) = self.current.take() else {
            return Ok(Vec::new());
        };
        Ok(bucket
            .close(bucket.start_ms + self.horizon_ms, self.horizon_us())
            .into_iter()
            .collect())
    }

    fn advance_to(&mut self, timestamp_us: i64) -> Result<Vec<YlabelBar>> {
        let target_start = align_ms(timestamp_us.div_euclid(1_000), self.horizon_ms);
        let mut out = Vec::new();
        while let Some(bucket) = self.current.as_ref() {
            if target_start <= bucket.start_ms {
                break;
            }
            let current = self.current.take().expect("bucket present");
            let end_ms = current.start_ms + self.horizon_ms;
            if let Some(bar) = current.close(end_ms, self.horizon_us()) {
                out.push(bar);
            }
            if end_ms < target_start {
                self.current = Some(OpenYlabelBucket::new(
                    target_start,
                    target_start.saturating_mul(1_000),
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
        self.current = Some(OpenYlabelBucket::new(
            start_ms,
            if self.last_trade_us > 0 {
                self.last_trade_us
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

pub fn ylabel_table_name(horizon_ms: i64) -> Result<String> {
    let label = match horizon_ms {
        5_000 => "5s",
        10_000 => "10s",
        30_000 => "30s",
        60_000 => "1m",
        300_000 => "5m",
        other => bail!("unsupported ylabel horizon_ms={other}"),
    };
    Ok(format!("ylabel_{YLABEL_VENUE}_{label}"))
}

pub fn market_1s_clickhouse_value_columns() -> &'static [&'static str] {
    &[
        "bid0p", "bid0v", "ask0p", "ask0v", "buy_high", "sell_low", "open", "high", "low", "close",
        "volume", "turnover", "midp",
    ]
}

pub fn market_1s_clickhouse_columns_sql() -> String {
    let mut columns = vec![
        "ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD)".to_string(),
        "symbol String".to_string(),
    ];
    columns.extend(
        market_1s_clickhouse_value_columns()
            .iter()
            .map(|name| format!("{name} Float64")),
    );
    columns.join(", ")
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

pub fn encode_market_1s_clickhouse_row(symbol: &str, bar: &TradeMarket1sBar) -> Vec<u8> {
    let mut row =
        Vec::with_capacity(16 + symbol.len() + market_1s_clickhouse_value_columns().len() * 8);
    row.extend_from_slice(&(bar.ts_sec * 1_000).to_le_bytes());
    append_var_uint(&mut row, symbol.len() as u64);
    row.extend_from_slice(symbol.as_bytes());
    for value in bar.clickhouse_values() {
        row.extend_from_slice(&value.to_le_bytes());
    }
    row
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

/// Decode a printable trade's price/amount. Returns `None` when price or volume
/// is missing or not a positive finite value (caller should log and skip).
pub fn trade_price_amount(price_e9: i64, volume: u32) -> Option<(f64, f64)> {
    if price_e9 == MISSING_PRICE || volume == MISSING_VOLUME || volume == 0 {
        return None;
    }
    let price = price_e9_to_f64(price_e9)?;
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    let amount = f64::from(volume);
    if !amount.is_finite() || amount <= 0.0 {
        return None;
    }
    Some((price, amount))
}

/// Human-readable reason when [`trade_price_amount`] returns `None`.
pub fn trade_price_amount_reject_reason(price_e9: i64, volume: u32) -> &'static str {
    if price_e9 == MISSING_PRICE {
        return "missing_price";
    }
    if volume == MISSING_VOLUME {
        return "missing_volume";
    }
    if volume == 0 {
        return "zero_volume";
    }
    match price_e9_to_f64(price_e9) {
        None => "non_finite_price",
        Some(price) if !price.is_finite() => "non_finite_price",
        Some(price) if price <= 0.0 => "non_positive_price",
        Some(_) => "invalid_amount",
    }
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
    use crate::parse_price_e9;

    fn book(bid: &str, ask: &str) -> TopOfBook {
        TopOfBook {
            bid_price: bid.parse().unwrap(),
            bid_amount: 1.0,
            ask_price: ask.parse().unwrap(),
            ask_amount: 2.0,
        }
    }

    #[test]
    fn skips_empty_seconds_and_keeps_prior_book() {
        let mut agg = Sparse1sAggregator::new();
        let quote_us = 1_000_000;
        let closed = agg.on_quote(quote_us, book("10", "11")).unwrap();
        assert!(closed.is_empty());
        let trade_us = 5_500_000;
        let price = parse_price_e9("10.5").unwrap();
        let (px, amt) = trade_price_amount(price, 3).expect("valid trade");
        let closed = agg.on_trade(trade_us, 1, px, amt).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].ts_sec, 2);
        assert_eq!(closed[0].bid0p, 10.0);
        assert_eq!(closed[0].ask0p, 11.0);
        assert_eq!(closed[0].volume, 0.0);
        assert!(closed[0].buy_high.is_nan());
        let rest = agg.finish().unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].ts_sec, 5);
        assert_eq!(rest[0].open, px);
        assert_eq!(rest[0].volume, 3.0);
        assert_eq!(rest[0].buy_high, px);
        assert!(rest[0].sell_low.is_nan());
        assert_eq!(rest[0].bid0p, 10.0);
    }

    #[test]
    fn aggressor_zero_counts_in_ohlc_not_buy_high() {
        let mut agg = Sparse1sAggregator::new();
        let (px, amt) = trade_price_amount(parse_price_e9("99").unwrap(), 1).expect("valid trade");
        let _ = agg.on_trade(1_500_000, 0, px, amt).unwrap();
        let bars = agg.finish().unwrap();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].open, px);
        assert!(bars[0].buy_high.is_nan());
        assert!(bars[0].sell_low.is_nan());
        assert!(bars[0].bid0p.is_nan());
    }

    #[test]
    fn ylabel_skips_idle_buckets() {
        let mut y = SparseYlabelAggregator::new(5_000).unwrap();
        let (px, amt) = trade_price_amount(parse_price_e9("10").unwrap(), 2).expect("valid trade");
        let closed = y.on_trade(1_000, px, amt).unwrap();
        assert!(closed.is_empty());
        let later = y.on_trade(12_000_000, px, amt).unwrap();
        assert_eq!(later.len(), 1);
        assert_eq!(later[0].ts_ms, 5_000);
        assert!(later[0].vwap.is_finite());
        let rest = y.finish().unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].ts_ms, 15_000);
    }

    #[test]
    fn invalid_trade_price_amount_is_none() {
        assert!(trade_price_amount(MISSING_PRICE, 1).is_none());
        assert!(trade_price_amount(parse_price_e9("10").unwrap(), MISSING_VOLUME).is_none());
        assert!(trade_price_amount(parse_price_e9("10").unwrap(), 0).is_none());
        assert!(trade_price_amount(parse_price_e9("0").unwrap(), 1).is_none());
        assert_eq!(
            trade_price_amount_reject_reason(MISSING_PRICE, 1),
            "missing_price"
        );
        assert_eq!(
            trade_price_amount_reject_reason(parse_price_e9("10").unwrap(), 0),
            "zero_volume"
        );
    }

    #[test]
    fn invalid_trade_does_not_emit_1s_or_ylabel() {
        let mut bars = Sparse1sAggregator::new();
        let closed = bars.on_trade(1_500_000, 1, f64::NAN, 1.0).unwrap();
        assert!(closed.is_empty());
        assert!(bars.finish().unwrap().is_empty());

        let mut y = SparseYlabelAggregator::new(5_000).unwrap();
        let closed = y.on_trade(1_000, -1.0, 1.0).unwrap();
        assert!(closed.is_empty());
        assert!(y.finish().unwrap().is_empty());
    }
}
