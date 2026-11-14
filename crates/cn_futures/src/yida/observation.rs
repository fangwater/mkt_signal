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
