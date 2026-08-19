//! 1s CTA market bars from Tardis trades plus incremental L2 top-of-book.
//!
//! ClickHouse is the source of truth. The official daily HDF is only an export
//! of those rows. Bid/ask0 come from a full reconstructed L2 book, not last
//! trade and not a single-level overwrite. The tracker keeps two top-of-book
//! copies: `latest` follows every incremental update, and `valid` is the last
//! uncrossed book. They may be identical, but they are stored separately.
//! Crossed books use the same `prune_crossed_by_best_update_id` rule as the
//! 5s/60s depth replay. Trades only fill OHLC and `buy_high` / `sell_low`.
//! Row `t` uses the last valid top-of-book strictly before second `t`, and
//! trades in `[t, t+1)`.

use crate::depth_pub::orderbook::OrderBook;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use mkt_parsers::msg::mkt_msg::Level;

pub const SECONDS_PER_DAY: i64 = 86_400;
pub const MARKET_1S_SIDS: [u8; 2] = [1, 6];
pub const PRIMARY_SID: u8 = 1;
pub const HDF_KEY: &str = "df";
pub const DEFAULT_FILE_SIDS: &str = "1_6";
pub const DEFAULT_MARKET_1S_OUTPUT_DIR: &str =
    "/mnt/hdd-raid5-72t/liang_torch/crypto_data/cta_backtest_data/binanceswap/tardis_agg_1s_daily";

pub const MARKET_1S_COLUMNS: [&str; 28] = [
    "symbol",
    "ts",
    "bid0p_1",
    "bid0v_1",
    "ask0p_1",
    "ask0v_1",
    "buy_high_1",
    "sell_low_1",
    "open_1",
    "high_1",
    "low_1",
    "close_1",
    "volume_1",
    "turnover_1",
    "bid0p_6",
    "bid0v_6",
    "ask0p_6",
    "ask0v_6",
    "buy_high_6",
    "sell_low_6",
    "open_6",
    "high_6",
    "low_6",
    "close_6",
    "volume_6",
    "turnover_6",
    "midp_1",
    "midp_6",
];

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopOfBook {
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
}

impl TopOfBook {
    pub fn midp(&self) -> f64 {
        (self.bid_price + self.ask_price) / 2.0
    }

    pub fn is_valid(&self) -> bool {
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

/// Full L2 book plus two top-of-book copies: live `latest`, last uncrossed `valid`.
///
/// Incremental L2 can briefly cross. `latest` still follows the update, even
/// when crossed. `valid` is only overwritten when the current book is a
/// complete uncrossed (or locked) book. Those two copies may be the same
/// book, but they are always stored twice. The underlying map is the same
/// `OrderBook` used by 5s/60s depth replay.
#[derive(Debug, Clone, Default)]
pub struct TopOfBookTracker {
    book: OrderBook,
    next_update_id: i64,
    latest: Option<TopOfBook>,
    valid: Option<TopOfBook>,
}

impl TopOfBookTracker {
    pub fn apply_level(&mut self, is_bid: bool, price: f64, amount: f64) {
        if is_bid {
            self.apply_event(0, &[(price, amount)], &[]);
        } else {
            self.apply_event(0, &[], &[(price, amount)]);
        }
    }

    pub fn apply_book_levels(&mut self, timestamp_us: i64, bids: &[Level], asks: &[Level]) {
        let bids: Vec<(f64, f64)> = bids
            .iter()
            .map(|level| (level.price, level.amount))
            .collect();
        let asks: Vec<(f64, f64)> = asks
            .iter()
            .map(|level| (level.price, level.amount))
            .collect();
        self.apply_event(timestamp_us, &bids, &asks);
    }

    pub fn apply_event(&mut self, timestamp_us: i64, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        if self.next_update_id <= 0 {
            self.next_update_id = 1;
        }
        let update_id = self.next_update_id;
        self.next_update_id = self.next_update_id.saturating_add(1);
        self.book.apply_update(bids, asks, update_id, timestamp_us);
        self.latest = self.current_bbo();
        if self.is_crossed() {
            self.book.prune_crossed_by_best_update_id();
        }
        if let Some(book) = self.current_bbo().filter(TopOfBook::is_valid) {
            self.valid = Some(book);
        }
    }

    fn current_bbo(&self) -> Option<TopOfBook> {
        let (bid_price, bid_amount, _) = self.book.best_bid_level()?;
        let (ask_price, ask_amount, _) = self.book.best_ask_level()?;
        Some(TopOfBook {
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        })
    }

    fn is_crossed(&self) -> bool {
        match (self.book.best_bid_price(), self.book.best_ask_price()) {
            (Some(bid), Some(ask)) => bid > ask,
            _ => false,
        }
    }

    pub fn snapshot(&self) -> Option<TopOfBook> {
        self.valid
    }

    pub fn latest(&self) -> Option<TopOfBook> {
        self.latest
    }
}

#[derive(Debug, Clone, Copy)]
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
    pub fn midp(&self) -> f64 {
        (self.bid0p + self.ask0p) / 2.0
    }

    fn clickhouse_values(self) -> [f64; 26] {
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
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            self.midp(),
            f64::NAN,
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
        }
    }

    fn on_trade(&mut self, is_buy: bool, price: f64, amount: f64) {
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
        if is_buy {
            self.buy_high = if self.buy_high.is_nan() {
                price
            } else {
                self.buy_high.max(price)
            };
        } else {
            self.sell_low = if self.sell_low.is_nan() {
                price
            } else {
                self.sell_low.min(price)
            };
        }
        self.count += 1;
    }
}

#[derive(Debug)]
pub struct TradeMarket1sAggregator {
    day_start_sec: i64,
    day_end_sec: i64,
    next_sec: i64,
    last_book: Option<TopOfBook>,
    last_close: Option<f64>,
    current: OpenSecond,
}

impl TradeMarket1sAggregator {
    pub fn new(
        day: NaiveDate,
        seed_book: Option<TopOfBook>,
        seed_close: Option<f64>,
    ) -> Result<Self> {
        let day_start_sec = utc_day_start_sec(day)?;
        Ok(Self {
            day_start_sec,
            day_end_sec: day_start_sec + SECONDS_PER_DAY,
            next_sec: day_start_sec,
            last_book: seed_book.filter(TopOfBook::is_valid),
            last_close: seed_close.filter(|price| price.is_finite() && *price > 0.0),
            current: OpenSecond::empty(),
        })
    }

    pub fn on_book(&mut self, timestamp_us: i64, book: TopOfBook) -> Result<Vec<TradeMarket1sBar>> {
        if !book.is_valid() {
            bail!("1s market bar requires a valid top of book");
        }
        let quote_sec = timestamp_us.div_euclid(1_000_000) + 1;
        if quote_sec <= self.day_start_sec {
            self.last_book = Some(book);
            return Ok(Vec::new());
        }
        if timestamp_us.div_euclid(1_000_000) >= self.day_end_sec {
            return Ok(Vec::new());
        }
        let closed = self.emit_until(quote_sec.min(self.day_end_sec))?;
        self.last_book = Some(book);
        Ok(closed)
    }

    pub fn on_trade(
        &mut self,
        timestamp_us: i64,
        is_buy: bool,
        price: f64,
        amount: f64,
    ) -> Result<Vec<TradeMarket1sBar>> {
        if !price.is_finite() || price <= 0.0 || !amount.is_finite() || amount <= 0.0 {
            bail!("1s market bar requires positive finite trade price and amount");
        }
        let trade_sec = timestamp_us.div_euclid(1_000_000);
        if trade_sec < self.day_start_sec {
            self.last_close = Some(price);
            return Ok(Vec::new());
        }
        if trade_sec >= self.day_end_sec || trade_sec < self.next_sec {
            return Ok(Vec::new());
        }
        let closed = self.emit_until(trade_sec)?;
        self.current.on_trade(is_buy, price, amount);
        Ok(closed)
    }

    pub fn finish(&mut self) -> Result<Vec<TradeMarket1sBar>> {
        if self.last_book.is_none() {
            bail!(
                "no depth available to seed 1s bid/ask0 for day starting {}",
                self.day_start_sec
            );
        }
        self.emit_until(self.day_end_sec)
    }

    pub fn last_book(&self) -> Option<TopOfBook> {
        self.last_book
    }

    pub fn last_close(&self) -> Option<f64> {
        self.last_close
    }

    fn emit_until(&mut self, target_sec: i64) -> Result<Vec<TradeMarket1sBar>> {
        let limit = target_sec.min(self.day_end_sec);
        if limit <= self.next_sec {
            return Ok(Vec::new());
        }
        if self.last_book.is_none() {
            bail!("1s market bars need a top of book before emitting");
        }
        let mut out = Vec::with_capacity((limit - self.next_sec) as usize);
        while self.next_sec < limit {
            out.push(self.close_second()?);
        }
        Ok(out)
    }

    fn close_second(&mut self) -> Result<TradeMarket1sBar> {
        let book = self
            .last_book
            .context("1s market bars need a top of book before emitting")?;
        let current = std::mem::replace(&mut self.current, OpenSecond::empty());
        let traded = current.count > 0;
        let fill_price = if traded {
            current.close
        } else if let Some(close) = self.last_close {
            close
        } else {
            book.midp()
        };
        let bar = TradeMarket1sBar {
            ts_sec: self.next_sec,
            bid0p: book.bid_price,
            ask0p: book.ask_price,
            bid0v: book.bid_amount,
            ask0v: book.ask_amount,
            buy_high: current.buy_high,
            sell_low: current.sell_low,
            open: if traded { current.open } else { fill_price },
            high: if traded { current.high } else { fill_price },
            low: if traded { current.low } else { fill_price },
            close: fill_price,
            volume: if traded { current.volume } else { 0.0 },
            turnover: if traded { current.turnover } else { 0.0 },
        };
        if traded {
            self.last_close = Some(current.close);
        } else if self.last_close.is_none() {
            self.last_close = Some(fill_price);
        }
        self.next_sec += 1;
        Ok(bar)
    }
}

pub fn utc_day_start_sec(day: NaiveDate) -> Result<i64> {
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(
        day.and_hms_opt(0, 0, 0)
            .context("invalid UTC midnight for trade-only 1s day")?,
        Utc,
    )
    .timestamp())
}

pub fn market_1s_filename(symbol: &str, file_sids: &str, day: NaiveDate) -> String {
    format!(
        "tardis_1s_{symbol}_sids_{file_sids}_{}.h5",
        day.format("%Y%m%d")
    )
}

pub fn market_1s_table_name(venue_slug: &str) -> String {
    format!("baseline_{}_1s_trade", venue_slug.replace('-', "_"))
}

pub fn market_1s_clickhouse_value_columns() -> &'static [&'static str] {
    &MARKET_1S_COLUMNS[2..]
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

pub fn market_1s_clickhouse_select_sql() -> String {
    format!(
        "toUnixTimestamp(ts), {}",
        [
            "bid0p_1",
            "bid0v_1",
            "ask0p_1",
            "ask0v_1",
            "buy_high_1",
            "sell_low_1",
            "open_1",
            "high_1",
            "low_1",
            "close_1",
            "volume_1",
            "turnover_1",
        ]
        .join(", ")
    )
}

pub fn validate_market_1s_day(
    symbol: &str,
    day: NaiveDate,
    bars: &[TradeMarket1sBar],
) -> Result<()> {
    let start = utc_day_start_sec(day)?;
    if bars.len() != SECONDS_PER_DAY as usize {
        bail!(
            "{symbol} {day}: expected {} 1s rows, got {}",
            SECONDS_PER_DAY,
            bars.len()
        );
    }
    for (index, bar) in bars.iter().enumerate() {
        let expected = start + index as i64;
        if bar.ts_sec != expected {
            bail!(
                "{symbol} {day}: ts[{index}]={} expected={expected}",
                bar.ts_sec
            );
        }
        if !bar.bid0p.is_finite()
            || !bar.ask0p.is_finite()
            || bar.bid0p <= 0.0
            || bar.ask0p < bar.bid0p
        {
            bail!(
                "{symbol} {day}: invalid depth bid/ask0 at ts={}",
                bar.ts_sec
            );
        }
        if !bar.open.is_finite()
            || !bar.high.is_finite()
            || !bar.low.is_finite()
            || !bar.close.is_finite()
            || bar.high < bar.low
            || bar.close < bar.low
            || bar.close > bar.high
        {
            bail!("{symbol} {day}: invalid OHLC at ts={}", bar.ts_sec);
        }
        if bar.buy_high.is_infinite() || bar.sell_low.is_infinite() {
            bail!(
                "{symbol} {day}: infinite buy_high/sell_low at ts={}",
                bar.ts_sec
            );
        }
    }
    Ok(())
}

pub fn encode_market_1s_clickhouse_row(symbol: &str, bar: &TradeMarket1sBar) -> Vec<u8> {
    let mut row = Vec::with_capacity(16 + symbol.len() + market_1s_clickhouse_value_columns().len() * 8);
    row.extend_from_slice(&(bar.ts_sec * 1_000).to_le_bytes());
    append_var_uint(&mut row, symbol.len() as u64);
    row.extend_from_slice(symbol.as_bytes());
    for value in bar.clickhouse_values() {
        row.extend_from_slice(&value.to_le_bytes());
    }
    row
}

pub fn parse_market_1s_clickhouse_tsv(line: &str) -> Result<TradeMarket1sBar> {
    let mut parts = line.split('\t');
    let ts_sec = parts
        .next()
        .context("1s ClickHouse export missing ts")?
        .parse::<i64>()
        .context("1s ClickHouse export ts")?;
    let bid0p = parse_clickhouse_f64(parts.next(), "bid0p_1")?;
    let bid0v = parse_clickhouse_f64(parts.next(), "bid0v_1")?;
    let ask0p = parse_clickhouse_f64(parts.next(), "ask0p_1")?;
    let ask0v = parse_clickhouse_f64(parts.next(), "ask0v_1")?;
    let buy_high = parse_clickhouse_f64(parts.next(), "buy_high_1")?;
    let sell_low = parse_clickhouse_f64(parts.next(), "sell_low_1")?;
    let open = parse_clickhouse_f64(parts.next(), "open_1")?;
    let high = parse_clickhouse_f64(parts.next(), "high_1")?;
    let low = parse_clickhouse_f64(parts.next(), "low_1")?;
    let close = parse_clickhouse_f64(parts.next(), "close_1")?;
    let volume = parse_clickhouse_f64(parts.next(), "volume_1")?;
    let turnover = parse_clickhouse_f64(parts.next(), "turnover_1")?;
    if parts.next().is_some() {
        bail!("1s ClickHouse export has extra columns");
    }
    Ok(TradeMarket1sBar {
        ts_sec,
        bid0p,
        ask0p,
        bid0v,
        ask0v,
        buy_high,
        sell_low,
        open,
        high,
        low,
        close,
        volume,
        turnover,
    })
}

fn parse_clickhouse_f64(value: Option<&str>, name: &str) -> Result<f64> {
    let value = value.with_context(|| format!("1s ClickHouse export missing {name}"))?;
    if value.is_empty() || value == r"\N" || value.eq_ignore_ascii_case("nan") {
        return Ok(f64::NAN);
    }
    value
        .parse::<f64>()
        .with_context(|| format!("1s ClickHouse export {name}={value}"))
}

pub fn write_market_1s_hdf(
    path: &std::path::Path,
    symbol: &str,
    bars: &[TradeMarket1sBar],
    python: &std::path::Path,
) -> Result<()> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    if bars.len() != SECONDS_PER_DAY as usize {
        bail!(
            "1s market HDF writer expected {} rows, got {}",
            SECONDS_PER_DAY,
            bars.len()
        );
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create market 1s output dir {}", parent.display()))?;
    }

    let mut child = Command::new(python)
        .arg("-c")
        .arg(WRITE_MARKET_1S_HDF_PY)
        .arg(path.as_os_str())
        .arg(symbol)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn pandas HDF writer {}", python.display()))?;
    {
        let stdin = child
            .stdin
            .as_mut()
            .context("pandas HDF writer stdin is unavailable")?;
        writeln!(
            stdin,
            "ts,bid0p_1,bid0v_1,ask0p_1,ask0v_1,buy_high_1,sell_low_1,open_1,high_1,low_1,close_1,volume_1,turnover_1"
        )?;
        for bar in bars {
            writeln!(
                stdin,
                "{},{},{},{},{},{},{},{},{},{},{},{},{}",
                bar.ts_sec,
                bar.bid0p,
                bar.bid0v,
                bar.ask0p,
                bar.ask0v,
                csv_f64(bar.buy_high),
                csv_f64(bar.sell_low),
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.turnover,
            )?;
        }
    }
    let output = child
        .wait_with_output()
        .context("wait for pandas HDF writer")?;
    if !output.status.success() {
        bail!(
            "pandas HDF writer failed for {}: status={:?} stdout={} stderr={}",
            path.display(),
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

fn csv_f64(value: f64) -> String {
    if value.is_nan() {
        String::new()
    } else {
        value.to_string()
    }
}

fn append_var_uint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

const WRITE_MARKET_1S_HDF_PY: &str = r#"
import os
import sys

import numpy as np
import pandas as pd

output = sys.argv[1]
symbol = sys.argv[2]
columns = [
    "symbol", "ts",
    "bid0p_1", "bid0v_1", "ask0p_1", "ask0v_1",
    "buy_high_1", "sell_low_1",
    "open_1", "high_1", "low_1", "close_1", "volume_1", "turnover_1",
    "bid0p_6", "bid0v_6", "ask0p_6", "ask0v_6",
    "buy_high_6", "sell_low_6",
    "open_6", "high_6", "low_6", "close_6", "volume_6", "turnover_6",
    "midp_1", "midp_6",
]
sid6 = [
    "bid0p_6", "bid0v_6", "ask0p_6", "ask0v_6",
    "buy_high_6", "sell_low_6",
    "open_6", "high_6", "low_6", "close_6", "volume_6", "turnover_6",
    "midp_6",
]
frame = pd.read_csv(sys.stdin)
if len(frame) != 86400:
    raise SystemExit(f"expected 86400 rows, got {len(frame)}")
out = pd.DataFrame({name: np.nan for name in columns})
out["symbol"] = symbol
out["ts"] = frame["ts"].to_numpy(dtype=np.int64, copy=False)
for name in [
    "bid0p_1", "bid0v_1", "ask0p_1", "ask0v_1",
    "buy_high_1", "sell_low_1",
    "open_1", "high_1", "low_1", "close_1", "volume_1", "turnover_1",
]:
    out[name] = frame[name].to_numpy(dtype=np.float64, copy=False)
out["midp_1"] = (out["bid0p_1"] + out["ask0p_1"]) / 2.0
for name in sid6:
    out[name] = np.nan
out = out[columns]
parent = os.path.dirname(output)
if parent:
    os.makedirs(parent, exist_ok=True)
temp = output + f".{os.getpid()}.tmp"
try:
    out.to_hdf(
        temp,
        key="df",
        mode="w",
        format="table",
        index=False,
        data_columns=["symbol", "ts"],
        min_itemsize={"symbol": max(32, len(symbol.encode("utf-8")))},
        complevel=9,
        complib="blosc",
    )
    os.replace(temp, output)
finally:
    if os.path.exists(temp):
        os.unlink(temp)
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn day() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date")
    }

    fn us(offset_sec: i64, micros: i64) -> i64 {
        (utc_day_start_sec(day()).unwrap() + offset_sec) * 1_000_000 + micros
    }

    fn book(bid: f64, bid_v: f64, ask: f64, ask_v: f64) -> TopOfBook {
        TopOfBook {
            bid_price: bid,
            bid_amount: bid_v,
            ask_price: ask,
            ask_amount: ask_v,
        }
    }

    #[test]
    fn bid0_ask0_come_from_depth_not_trades() {
        let mut agg = TradeMarket1sAggregator::new(day(), None, None).unwrap();
        assert!(agg
            .on_book(us(0, 100) - 1_000_000, book(100.0, 2.0, 101.0, 3.0))
            .unwrap()
            .is_empty());
        assert!(agg
            .on_trade(us(0, 200), true, 110.0, 1.0)
            .unwrap()
            .is_empty());
        let later = agg.on_book(us(1, 0), book(102.0, 4.0, 103.0, 5.0)).unwrap();
        assert_eq!(later.len(), 2);
        assert_eq!(later[0].bid0p, 100.0);
        assert_eq!(later[0].ask0p, 101.0);
        assert_eq!(later[0].bid0v, 2.0);
        assert_eq!(later[0].ask0v, 3.0);
        assert_eq!(later[0].buy_high, 110.0);
        assert_eq!(later[0].close, 110.0);
        assert_eq!(later[1].bid0p, 100.0);
        assert_eq!(later[1].ask0p, 101.0);
        assert_eq!(later[1].volume, 0.0);
        let rest = agg.finish().unwrap();
        assert_eq!(rest[0].bid0p, 102.0);
        assert_eq!(rest[0].ask0p, 103.0);
        assert_eq!(rest[0].volume, 0.0);
        assert!(rest[0].buy_high.is_nan());
    }

    #[test]
    fn empty_seconds_keep_prior_book_and_directional_nan() {
        let seed = book(99.0, 1.0, 100.0, 1.0);
        let mut agg = TradeMarket1sAggregator::new(day(), Some(seed), Some(98.0)).unwrap();
        assert!(agg.on_trade(us(1, 100), true, 100.0, 2.0).unwrap().len() == 1);
        assert!(agg
            .on_trade(us(1, 200), false, 99.5, 1.0)
            .unwrap()
            .is_empty());
        let later = agg.on_trade(us(3, 0), true, 101.0, 3.0).unwrap();
        assert_eq!(later.len(), 2);
        assert_eq!(later[0].bid0p, 99.0);
        assert_eq!(later[0].ask0p, 100.0);
        assert_eq!(later[0].buy_high, 100.0);
        assert_eq!(later[0].sell_low, 99.5);
        assert_eq!(later[0].volume, 3.0);
        assert_eq!(later[1].bid0p, 99.0);
        assert_eq!(later[1].volume, 0.0);
        assert!(later[1].buy_high.is_nan());
    }

    #[test]
    fn finish_fills_the_rest_of_the_utc_day() {
        let seed = book(50.0, 1.0, 51.0, 1.0);
        let mut agg = TradeMarket1sAggregator::new(day(), Some(seed), Some(50.0)).unwrap();
        assert!(agg.on_trade(us(0, 1), true, 51.0, 1.0).unwrap().is_empty());
        let bars = agg.finish().unwrap();
        assert_eq!(bars.len(), 86_400);
        assert_eq!(bars[0].bid0p, 50.0);
        assert_eq!(bars[0].ask0p, 51.0);
        assert_eq!(bars[0].close, 51.0);
        assert_eq!(bars[1].bid0p, 50.0);
        assert_eq!(bars[1].close, 51.0);
        assert_eq!(
            bars.last().unwrap().ts_sec,
            utc_day_start_sec(day()).unwrap() + 86_399
        );
    }

    #[test]
    fn rejects_a_day_with_no_depth() {
        let mut agg = TradeMarket1sAggregator::new(day(), None, Some(100.0)).unwrap();
        assert!(agg.finish().unwrap_err().to_string().contains("no depth"));
    }

    #[test]
    fn top_of_book_tracker_keeps_full_book_best_bid_and_ask() {
        let mut book = TopOfBookTracker::default();
        book.apply_level(true, 100.0, 2.0);
        book.apply_level(false, 101.0, 3.0);
        book.apply_level(true, 99.0, 9.0);
        book.apply_level(false, 102.0, 8.0);
        let latest = book.latest().unwrap();
        assert_eq!(latest.bid_price, 100.0);
        assert_eq!(latest.bid_amount, 2.0);
        assert_eq!(latest.ask_price, 101.0);
        assert_eq!(latest.ask_amount, 3.0);
        assert_eq!(book.latest(), book.snapshot());
        book.apply_level(true, 100.5, 1.0);
        book.apply_level(false, 100.8, 1.5);
        let snap = book.snapshot().unwrap();
        assert_eq!(snap.bid_price, 100.5);
        assert_eq!(snap.ask_price, 100.8);
        book.apply_level(true, 100.5, 0.0);
        let latest = book.latest().unwrap();
        assert_eq!(latest.bid_price, 100.0);
        assert_eq!(latest.ask_price, 100.8);
        let snap = book.snapshot().unwrap();
        assert_eq!(snap.bid_price, 100.0);
        assert_eq!(snap.ask_price, 100.8);
    }

    #[test]
    fn crossed_latest_keeps_last_valid_copy() {
        let mut book = TopOfBookTracker::default();
        book.apply_level(true, 100.0, 2.0);
        book.apply_level(false, 101.0, 3.0);
        assert_eq!(book.latest(), book.snapshot());
        book.apply_level(true, 102.0, 1.0);
        let latest = book.latest().unwrap();
        assert_eq!(latest.bid_price, 102.0);
        assert_eq!(latest.ask_price, 101.0);
        assert!(!latest.is_valid());
        let snap = book.snapshot().unwrap();
        assert_eq!(snap.bid_price, 100.0);
        assert_eq!(snap.ask_price, 101.0);
        book.apply_level(false, 102.5, 1.5);
        let latest = book.latest().unwrap();
        let valid = book.snapshot().unwrap();
        assert_eq!(latest.bid_price, 102.0);
        assert_eq!(latest.ask_price, 102.5);
        assert_eq!(valid, latest);
        assert_eq!(book.latest(), book.snapshot());
    }

    #[test]
    fn names_match_official_daily_layout() {
        assert_eq!(
            market_1s_filename("SOLUSDT", "1_6", day()),
            "tardis_1s_SOLUSDT_sids_1_6_20260101.h5"
        );
        assert_eq!(
            market_1s_table_name("binance-futures"),
            "baseline_binance_futures_1s_trade"
        );
        assert_eq!(MARKET_1S_COLUMNS.len(), 28);
        assert_eq!(MARKET_1S_COLUMNS[2], "bid0p_1");
        assert_eq!(MARKET_1S_COLUMNS[26], "midp_1");
        assert_eq!(
            market_1s_clickhouse_value_columns(),
            &MARKET_1S_COLUMNS[2..]
        );
        assert!(market_1s_clickhouse_columns_sql().contains("bid0p_1 Float64"));
        assert!(market_1s_clickhouse_columns_sql().contains("midp_1 Float64"));
        assert!(!market_1s_clickhouse_columns_sql().contains("trade_max"));
        assert!(!market_1s_clickhouse_columns_sql().contains("trade_min"));
    }

    #[test]
    fn clickhouse_tsv_round_trips_nan_and_sizes() {
        let parsed = parse_market_1s_clickhouse_tsv(
            "1767225600\t100\t2\t101\t3\t\\N\tnan\t100.5\t100.5\t100.5\t100.5\t0\t0",
        )
        .unwrap();
        assert_eq!(parsed.ts_sec, 1_767_225_600);
        assert_eq!(parsed.bid0p, 100.0);
        assert_eq!(parsed.bid0v, 2.0);
        assert_eq!(parsed.ask0p, 101.0);
        assert_eq!(parsed.ask0v, 3.0);
        assert!(parsed.buy_high.is_nan());
        assert!(parsed.sell_low.is_nan());
        assert_eq!(parsed.close, 100.5);
        let encoded = encode_market_1s_clickhouse_row("SOLUSDT", &parsed);
        assert_eq!(
            encoded.len(),
            8 + 1 + "SOLUSDT".len() + market_1s_clickhouse_value_columns().len() * 8
        );
    }

    #[test]
    fn ignores_late_trades_after_a_second_has_closed() {
        let seed = book(100.0, 1.0, 101.0, 1.0);
        let mut agg = TradeMarket1sAggregator::new(day(), Some(seed), Some(100.0)).unwrap();
        let first = agg.on_trade(us(1, 0), true, 101.0, 1.0).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].close, 100.0);
        assert!(agg
            .on_trade(us(0, 500), false, 90.0, 1.0)
            .unwrap()
            .is_empty());
        let bars = agg.finish().unwrap();
        assert_eq!(bars[0].ts_sec, utc_day_start_sec(day()).unwrap() + 1);
        assert_eq!(bars[0].close, 101.0);
        assert!(bars[0].sell_low.is_nan());
        assert_eq!(bars[1].bid0p, 100.0);
        assert_eq!(bars[1].ask0p, 101.0);
    }
}
