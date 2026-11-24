//! Read-only valuation cache. No quote can alter order rules, risk or positions.
use anyhow::{ensure, Context, Result};
use cn_futures_state::{InstrumentKind, InstrumentMark, Text, Venue, MAX_POSITIONS};
use std::collections::HashMap;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NativeL1 {
    pub generation: u64,
    pub initial: i32,
    pub connected: i32,
    pub product_class: i32,
    pub contract_multiple: i32,
    pub trading_day: i32,
    pub native_timestamp: i32,
    pub last_trade_timestamp: i32,
    pub bid_lots: i32,
    pub ask_lots: i32,
    pub market_flags: i32,
    pub last_price: f64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub lower_limit: f64,
    pub upper_limit: f64,
    pub symbol: [u8; 64],
    pub exchange: [u8; 12],
    pub native_time: [u8; 24],
    pub last_trade_time: [u8; 24],
}
#[derive(Clone, Copy)]
pub struct ReceivedL1 {
    pub event: NativeL1,
    pub received_us: u64,
}
fn text(bytes: &[u8]) -> Result<&str> {
    let end = bytes
        .iter()
        .position(|b| *b == 0)
        .context("unterminated L1 identity")?;
    std::str::from_utf8(&bytes[..end]).context("invalid L1 identity UTF-8")
}
fn price(value: f64) -> Option<f64> {
    (value.is_finite() && value > 0.0 && value < f64::MAX).then_some(value)
}
fn nonnegative(value: i32) -> Option<i32> {
    (value >= 0).then_some(value)
}
impl ReceivedL1 {
    #[cfg(feature = "yida-sdk")]
    pub(crate) fn execution_quote<'a>(
        &'a self,
        trading_day: &'a str,
    ) -> Option<crate::ctp::exec::Quote<'a>> {
        let e = &self.event;
        if e.generation == 0
            || e.connected != 1
            || e.initial != 0
            || e.market_flags & 1 != 0
            || e.product_class != 1
            || e.native_timestamp < 0
            || trading_day.parse::<i32>().ok() != Some(e.trading_day)
        {
            return None;
        }
        Some(crate::ctp::exec::Quote {
            venue: Venue::parse(text(&e.exchange).ok()?).ok()?,
            symbol: text(&e.symbol).ok()?,
            trading_day,
            generation: e.generation,
            received_us: self.received_us,
            bid: e.bid_price,
            ask: e.ask_price,
            bid_lots: e.bid_lots,
            ask_lots: e.ask_lots,
            lower_limit: e.lower_limit,
            upper_limit: e.upper_limit,
        })
    }

    pub fn mark(&self) -> Result<InstrumentMark> {
        let e = &self.event;
        let venue = Venue::parse(text(&e.exchange)?)?;
        ensure!(
            matches!(venue, Venue::Shfe | Venue::Ine),
            "Yida L1 venue outside execution scope"
        );
        let kind = match e.product_class {
            1 => InstrumentKind::Future,
            2 => InstrumentKind::Option,
            _ => anyhow::bail!("unsupported L1 product class"),
        };
        let mark = InstrumentMark {
            venue,
            symbol: Text::new(text(&e.symbol)?)?,
            kind,
            contract_multiple: e.contract_multiple,
            last_price: price(e.last_price),
            bid_price: price(e.bid_price),
            ask_price: price(e.ask_price),
            bid_lots: nonnegative(e.bid_lots),
            ask_lots: nonnegative(e.ask_lots),
            trading_day: e.trading_day.try_into().unwrap_or(0),
            native_timestamp: nonnegative(e.native_timestamp),
            last_trade_timestamp: nonnegative(e.last_trade_timestamp),
            native_time: Text::new(text(&e.native_time)?)?,
            last_trade_time: Text::new(text(&e.last_trade_time)?)?,
            received_us: self.received_us,
            initial: e.initial != 0,
            paused: e.market_flags & 1 != 0,
        };
        mark.validate()?;
        Ok(mark)
    }
}
#[derive(Default)]
pub struct MarketBook {
    index: HashMap<(Venue, [u8; 64]), usize>,
    marks: Vec<InstrumentMark>,
    generation: u64,
}
impl MarketBook {
    pub fn apply(&mut self, received: ReceivedL1) -> Result<()> {
        if received.event.generation == 0 || received.event.generation < self.generation {
            return Ok(());
        }
        let mark = received.mark()?;
        if received.event.generation > self.generation {
            self.index.clear();
            self.marks.clear();
            self.generation = received.event.generation;
        }
        let key = (mark.venue, received.event.symbol);
        if let Some(index) = self.index.get(&key) {
            let last = &mut self.marks[*index];
            if mark.received_us < last.received_us
                || (mark.initial && !last.initial && mark.native_timestamp == last.native_timestamp)
            {
                return Ok(());
            }
            *last = mark;
        } else {
            ensure!(
                self.marks.len() < MAX_POSITIONS,
                "L1 valuation capacity exceeded"
            );
            self.index.insert(key, self.marks.len());
            self.marks.push(mark);
        }
        Ok(())
    }
    pub fn marks(&self) -> &[InstrumentMark] {
        &self.marks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn quote(generation: u64, initial: bool, timestamp: i32) -> ReceivedL1 {
        let mut symbol = [0; 64];
        symbol[..6].copy_from_slice(b"cu2612");
        let mut exchange = [0; 12];
        exchange[..4].copy_from_slice(b"SHFE");
        let mut native_time = [0; 24];
        native_time[..12].copy_from_slice(b"21:00:01.500");
        ReceivedL1 {
            received_us: 1_000_000 + timestamp as u64,
            event: NativeL1 {
                generation,
                initial: i32::from(initial),
                connected: 1,
                product_class: 1,
                contract_multiple: 5,
                trading_day: 20261113,
                native_timestamp: timestamp,
                last_trade_timestamp: timestamp,
                bid_lots: 0,
                ask_lots: 1,
                market_flags: 0,
                last_price: 80000.0,
                bid_price: f64::MAX,
                ask_price: f64::NAN,
                lower_limit: f64::NAN,
                upper_limit: f64::NAN,
                symbol,
                exchange,
                native_time,
                last_trade_time: native_time,
            },
        }
    }
    #[test]
    fn native_l1_keeps_catalog_units_and_original_time_and_normalizes_missing_prices() {
        let q = quote(1, true, 100);
        let mark = q.mark().unwrap();
        assert_eq!(mark.contract_multiple, 5);
        assert_eq!(mark.last_price, Some(80000.0));
        assert!(mark.bid_price.is_none() && mark.ask_price.is_none());
        assert_eq!(mark.bid_lots, Some(0));
        assert_eq!(mark.native_timestamp, Some(100));
        assert_eq!(mark.native_time.as_str(), "21:00:01.500");
        assert!(mark.initial);
        let mut bad = q;
        bad.event.contract_multiple = 0;
        assert!(bad.mark().is_err());
        bad = q;
        bad.event.product_class = 3;
        assert!(bad.mark().is_err());
    }
    #[test]
    fn quote_recovery_replaces_generation_and_never_relabels_initial_data_as_live() {
        let mut book = MarketBook::default();
        book.apply(quote(1, true, 100)).unwrap();
        assert!(book.marks()[0].initial);
        book.apply(quote(1, false, 100)).unwrap();
        assert!(!book.marks()[0].initial);
        let mut snapshot = quote(1, true, 100);
        snapshot.received_us += 1;
        book.apply(snapshot).unwrap();
        assert!(!book.marks()[0].initial);
        book.apply(quote(2, true, 100)).unwrap();
        assert!(book.marks()[0].initial);
        book.apply(quote(1, false, 200)).unwrap();
        assert_eq!(book.marks()[0].native_timestamp, Some(100));
        assert_eq!(book.marks().len(), 1);
    }
}

/// Venue, symbol, side and native position date of one row.
type CostKey = (Venue, Text<81>, cn_futures_state::Side, i32);
/// Lots, open cost in money and native position profit of one row.
type CostRow = (i32, f64, f64);

/// Display-only native open costs from `POSITION_COST` rows, kept per
/// position-date row and summed per venue/symbol/side when published.
/// Never feeds positions, risk or orders.
#[derive(Default)]
pub struct PositionCostBook {
    rows: HashMap<CostKey, CostRow>,
    published: cn_futures_state::PositionCosts,
    observed_us: u64,
    dirty: bool,
    invalid: bool,
}
impl PositionCostBook {
    /// A new SDK session replays every row; nothing older survives it.
    pub fn clear(&mut self) {
        self.rows.clear();
        self.published.rows.clear();
        self.published.observed_us = 0;
        self.observed_us = 0;
        self.dirty = false;
        self.invalid = false;
    }
    /// An undecodable row withholds costs until the next SDK session.
    pub fn observe(&mut self, e: &super::RawEvent, now_us: u64) {
        if let Err(error) = self.apply(e, now_us) {
            if !self.invalid {
                log::warn!("Yida position cost unavailable: {error:#}");
            }
            self.invalid = true;
        }
    }
    fn apply(&mut self, e: &super::RawEvent, now_us: u64) -> Result<()> {
        use cn_futures_state::Side;
        let venue = e.venue()?;
        let symbol: Text<81> = super::text(&e.symbol)?;
        let side = match e.position_direction {
            2 => Side::Buy,
            3 => Side::Sell,
            _ => anyhow::bail!("invalid native position direction"),
        };
        ensure!(
            e.hedge == 1 && matches!(e.date, 1 | 2) && e.lots >= 0 && e.contract_multiple > 0,
            "unsupported native position cost row"
        );
        let open_cost = e.total_open_price * f64::from(e.contract_multiple);
        ensure!(
            open_cost.is_finite() && e.position_profit.is_finite(),
            "nonfinite native position cost"
        );
        let key = (venue, symbol, side, e.date);
        if e.lots == 0 {
            self.rows.remove(&key);
        } else {
            self.rows
                .insert(key, (e.lots, open_cost, e.position_profit));
        }
        self.observed_us = now_us;
        self.dirty = true;
        Ok(())
    }
    /// Rebuilds the published sums in place after a change; reuses capacity.
    pub fn costs(&mut self) -> Option<&cn_futures_state::PositionCosts> {
        if self.invalid || self.observed_us == 0 {
            return None;
        }
        if self.dirty {
            let rows = &mut self.published.rows;
            rows.clear();
            for ((venue, symbol, side, _), (lots, open_cost, profit)) in &self.rows {
                if let Some(row) = rows
                    .iter_mut()
                    .find(|r| r.venue == *venue && r.symbol == *symbol && r.side == *side)
                {
                    row.lots += lots;
                    row.open_cost += open_cost;
                    row.position_profit += profit;
                    continue;
                }
                if rows.len() >= MAX_POSITIONS {
                    self.invalid = true;
                    return None;
                }
                rows.push(cn_futures_state::PositionCost {
                    venue: *venue,
                    symbol: *symbol,
                    side: *side,
                    lots: *lots,
                    open_cost: *open_cost,
                    position_cost: 0.0,
                    position_profit: *profit,
                });
            }
            rows.sort_by(|a, b| {
                (
                    a.venue.as_str(),
                    a.symbol.as_str(),
                    a.side == cn_futures_state::Side::Sell,
                )
                    .cmp(&(
                        b.venue.as_str(),
                        b.symbol.as_str(),
                        b.side == cn_futures_state::Side::Sell,
                    ))
            });
            self.published.observed_us = self.observed_us;
            self.dirty = false;
        }
        Some(&self.published)
    }
}
