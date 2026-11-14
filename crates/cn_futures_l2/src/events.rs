//! Snapshot-interval event machine. Mirrors `cn_futures.events`.

use chrono::{DateTime, Timelike};
use chrono_tz::Tz;

use crate::codec::{DepthRecord, OiRecord, QueueRecord, TradeRecord, QUEUE_LEVELS};
use crate::session::Exchange;

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub trad_day: chrono::NaiveDate,
    pub instrument_id: String,
    pub product_id: String,
    pub event_time: DateTime<Tz>,
    pub seq: i64,
    pub source_row: u32,
    pub file_order: u32,
    pub volume: Option<f64>,
    pub turnover: Option<f64>,
    pub last_price: Option<f64>,
    pub open_int: Option<f64>,
    pub bid_prices: [Option<f64>; 5],
    pub bid_sizes: [Option<f64>; 5],
    pub ask_prices: [Option<f64>; 5],
    pub ask_sizes: [Option<f64>; 5],
}

impl Snapshot {
    pub fn sort_key(&self) -> (DateTime<Tz>, i64, u32, u32, &str) {
        (
            self.event_time,
            self.seq,
            self.source_row,
            self.file_order,
            self.instrument_id.as_str(),
        )
    }
}

#[derive(Clone, Debug)]
pub struct QueueSnapshot {
    pub trad_day: chrono::NaiveDate,
    pub instrument_id: String,
    pub product_id: String,
    pub event_time: DateTime<Tz>,
    pub seq: i64,
    pub source_row: u32,
    pub file_order: u32,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub bid_qty: [Option<f64>; QUEUE_LEVELS],
    pub ask_qty: [Option<f64>; QUEUE_LEVELS],
}

impl QueueSnapshot {
    pub fn sort_key(&self) -> (DateTime<Tz>, i64, u32, u32, &str) {
        (
            self.event_time,
            self.seq,
            self.source_row,
            self.file_order,
            self.instrument_id.as_str(),
        )
    }

    pub fn has_side(&self) -> bool {
        self.bid_price.is_some() || self.ask_price.is_some()
    }
}

#[derive(Clone, Copy, Debug)]
struct Book {
    bid_prices: [Option<f64>; 5],
    bid_sizes: [Option<f64>; 5],
    ask_prices: [Option<f64>; 5],
    ask_sizes: [Option<f64>; 5],
}

impl Book {
    fn bid1(self) -> Option<f64> {
        self.bid_prices[0]
    }

    fn ask1(self) -> Option<f64> {
        self.ask_prices[0]
    }

    fn two_sided(&self) -> bool {
        match (self.bid1(), self.ask1()) {
            (Some(bid), Some(ask)) => valid_px(bid) && valid_px(ask) && bid <= ask,
            _ => false,
        }
    }
}

fn valid_px(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

fn valid_cum(value: f64) -> bool {
    value.is_finite() && value >= 0.0
}

fn book_from_snapshot(snap: &Snapshot) -> Option<Book> {
    let book = Book {
        bid_prices: snap.bid_prices,
        bid_sizes: snap.bid_sizes,
        ask_prices: snap.ask_prices,
        ask_sizes: snap.ask_sizes,
    };
    book.two_sided().then_some(book)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Direction {
    Buy,
    Sell,
}

fn classify_direction(
    last_price: f64,
    prior: Option<Book>,
    prev_last: Option<f64>,
    last_nonzero: Option<Direction>,
) -> Option<Direction> {
    let prior = prior.filter(|book| book.two_sided())?;
    if last_price >= prior.ask1()? {
        return Some(Direction::Buy);
    }
    if last_price <= prior.bid1()? {
        return Some(Direction::Sell);
    }
    let prev_last = prev_last.filter(|price| valid_px(*price))?;
    if last_price > prev_last {
        Some(Direction::Buy)
    } else if last_price < prev_last {
        Some(Direction::Sell)
    } else {
        last_nonzero
    }
}

fn aggressor_code(direction: Option<Direction>) -> u8 {
    match direction {
        Some(Direction::Buy) => 1,
        Some(Direction::Sell) => 2,
        None => 0,
    }
}

pub fn event_time_utc_ns(moment: DateTime<Tz>) -> anyhow::Result<u64> {
    let nanos = moment
        .with_timezone(&chrono::Utc)
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow::anyhow!("event time {moment} overflows nanoseconds"))?;
    if nanos < 0 {
        anyhow::bail!("event time {moment} is before Unix epoch");
    }
    Ok(nanos as u64)
}

pub fn floor_local_second_utc_ns(moment: DateTime<Tz>) -> anyhow::Result<u64> {
    let floored = moment
        .with_nanosecond(0)
        .ok_or_else(|| anyhow::anyhow!("cannot floor {moment} to a second"))?;
    event_time_utc_ns(floored)
}

#[derive(Clone, Debug, Default)]
pub struct InstrumentOutput {
    pub trades: Vec<(u32, TradeRecord)>,
    pub depths: Vec<DepthRecord>,
    pub open_ints: Vec<(u32, OiRecord)>,
}

pub fn process_instrument(
    snaps: &mut [Snapshot],
    _exchange: Exchange,
) -> anyhow::Result<InstrumentOutput> {
    snaps.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
    let mut out = InstrumentOutput::default();
    let mut vol_prev: Option<f64> = None;
    let mut to_prev: Option<f64> = None;
    let mut last_px: Option<f64> = None;
    let mut last_dir: Option<Direction> = None;
    let mut book: Option<Book> = None;
    let mut pending_depth: Option<(u64, Book, String)> = None;
    let mut trade_ts = 0u64;
    let mut trade_seq = 0u32;
    let mut last_oi: Option<f64> = None;
    let mut oi_ts = 0u64;
    let mut oi_seq = 0u32;

    for snap in snaps.iter() {
        let prior_book = book;

        let mut delta_v = None;
        let mut trade_px = None;
        let mut direction = None;
        if let Some(volume) = snap.volume.filter(|value| valid_cum(*value)) {
            match vol_prev {
                None => vol_prev = Some(volume),
                Some(prev) if volume < prev => vol_prev = Some(volume),
                Some(prev) => {
                    let dv = volume - prev;
                    vol_prev = Some(volume);
                    if dv > 0.0 {
                        if let Some(price) = snap.last_price.filter(|value| valid_px(*value)) {
                            delta_v = Some(dv);
                            trade_px = Some(price);
                            direction = classify_direction(price, prior_book, last_px, last_dir);
                        }
                    }
                }
            }
        }

        let mut delta_to = None;
        if let Some(turnover) = snap.turnover.filter(|value| valid_cum(*value)) {
            match to_prev {
                None => to_prev = Some(turnover),
                Some(prev) if turnover < prev => to_prev = Some(turnover),
                Some(prev) => {
                    let dtv = turnover - prev;
                    to_prev = Some(turnover);
                    if dtv > 0.0 {
                        delta_to = Some(dtv);
                    }
                }
            }
        }

        if let Some(open_int) = snap.open_int.filter(|value| valid_cum(*value)) {
            let changed = match last_oi {
                None => true,
                Some(prev) => prev != open_int,
            };
            if changed {
                let ts = event_time_utc_ns(snap.event_time)?;
                if ts != oi_ts {
                    oi_ts = ts;
                    oi_seq = 0;
                }
                out.open_ints.push((
                    oi_seq,
                    OiRecord {
                        instrument: snap.instrument_id.clone(),
                        ts_utc_ns: ts,
                        open_int,
                        prev_open_int: last_oi,
                        delta: last_oi.map(|prev| open_int - prev),
                    },
                ));
                oi_seq = oi_seq.saturating_add(1);
                last_oi = Some(open_int);
            }
        }

        if matches!(direction, Some(Direction::Buy | Direction::Sell)) {
            last_dir = direction;
        }
        if let Some(price) = trade_px {
            last_px = Some(price);
        }
        if let Some(updated) = book_from_snapshot(snap) {
            book = Some(updated);
            let bucket = floor_local_second_utc_ns(snap.event_time)?;
            match pending_depth {
                Some((ts, _, _)) if ts != bucket => {
                    if let Some((ts, last, instrument)) = pending_depth.take() {
                        out.depths.push(depth_from_book(instrument, ts, last));
                    }
                    pending_depth = Some((bucket, updated, snap.instrument_id.clone()));
                }
                Some((_, ref mut last, _)) => *last = updated,
                None => {
                    pending_depth = Some((bucket, updated, snap.instrument_id.clone()));
                }
            }
        }
        if let (Some(dv), Some(price)) = (delta_v, trade_px) {
            let ts = event_time_utc_ns(snap.event_time)?;
            if ts != trade_ts {
                trade_ts = ts;
                trade_seq = 0;
            }
            out.trades.push((
                trade_seq,
                TradeRecord {
                    instrument: snap.instrument_id.clone(),
                    ts_utc_ns: ts,
                    price,
                    volume: dv,
                    turnover: delta_to,
                    bid: prior_book.and_then(Book::bid1),
                    bid_size: prior_book.and_then(|book| book.bid_sizes[0]),
                    ask: prior_book.and_then(Book::ask1),
                    ask_size: prior_book.and_then(|book| book.ask_sizes[0]),
                    aggressor: aggressor_code(direction),
                },
            ));
            trade_seq = trade_seq.saturating_add(1);
        }
    }
    if let Some((ts, last, instrument)) = pending_depth {
        out.depths.push(depth_from_book(instrument, ts, last));
    }
    Ok(out)
}

pub fn process_queues(snaps: &mut [QueueSnapshot]) -> anyhow::Result<Vec<(u32, QueueRecord)>> {
    snaps.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
    let mut out = Vec::new();
    let mut last_ts = 0u64;
    let mut seq = 0u32;
    for snap in snaps.iter() {
        if !snap.has_side() {
            continue;
        }
        let ts = event_time_utc_ns(snap.event_time)?;
        if ts != last_ts {
            last_ts = ts;
            seq = 0;
        }
        out.push((
            seq,
            QueueRecord {
                instrument: snap.instrument_id.clone(),
                ts_utc_ns: ts,
                bid_price: snap.bid_price,
                ask_price: snap.ask_price,
                bid_qty: snap.bid_qty,
                ask_qty: snap.ask_qty,
            },
        ));
        seq = seq.saturating_add(1);
    }
    Ok(out)
}

fn depth_from_book(instrument: String, ts_utc_ns: u64, book: Book) -> DepthRecord {
    DepthRecord {
        instrument,
        ts_utc_ns,
        bid_prices: book.bid_prices,
        bid_sizes: book.bid_sizes,
        ask_prices: book.ask_prices,
        ask_sizes: book.ask_sizes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone};
    use chrono_tz::Asia::Shanghai;

    fn snap(
        hour: u32,
        minute: u32,
        second: u32,
        nano: u32,
        volume: f64,
        last: f64,
        bid: f64,
        ask: f64,
    ) -> Snapshot {
        let date = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
        let time = chrono::NaiveTime::from_hms_nano_opt(hour, minute, second, nano).unwrap();
        Snapshot {
            trad_day: date,
            instrument_id: "rb2605".into(),
            product_id: "RB".into(),
            event_time: Shanghai
                .from_local_datetime(&date.and_time(time))
                .single()
                .unwrap(),
            seq: 1,
            source_row: 1,
            file_order: 0,
            volume: Some(volume),
            turnover: Some(volume * last),
            last_price: Some(last),
            open_int: None,
            bid_prices: [Some(bid), None, None, None, None],
            bid_sizes: [Some(1.0), None, None, None, None],
            ask_prices: [Some(ask), None, None, None, None],
            ask_sizes: [Some(1.0), None, None, None, None],
        }
    }

    fn local_hour(ts_utc_ns: u64) -> u32 {
        chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(ts_utc_ns as i64)
            .with_timezone(&Shanghai)
            .hour()
    }

    #[test]
    fn volume_up_emits_one_trade_and_last_depth() {
        let mut snaps = vec![
            snap(9, 0, 0, 500_000_000, 10.0, 3380.0, 3379.0, 3381.0),
            snap(9, 0, 1, 0, 20.0, 3382.0, 3381.0, 3383.0),
            snap(9, 0, 1, 200_000_000, 20.0, 3382.0, 3380.0, 3384.0),
        ];
        let out = process_instrument(&mut snaps, Exchange::Xsge).unwrap();
        assert_eq!(out.trades.len(), 1);
        assert_eq!(out.trades[0].1.volume, 10.0);
        assert_eq!(out.trades[0].1.price, 3382.0);
        assert_eq!(out.trades[0].1.aggressor, 1);
        assert_eq!(out.trades[0].1.bid, Some(3379.0));
        assert_eq!(out.depths.len(), 2);
        assert_eq!(out.depths[1].ask_prices[0], Some(3384.0));
    }

    #[test]
    fn auction_and_after_hours_still_write_trades_and_depth() {
        let mut snaps = vec![
            snap(8, 59, 0, 500_000_000, 2.0, 3380.0, 3379.0, 3381.0),
            snap(9, 0, 0, 500_000_000, 5.0, 3382.0, 3381.0, 3383.0),
            snap(15, 7, 0, 0, 8.0, 3383.0, 3382.0, 3384.0),
        ];
        let out = process_instrument(&mut snaps, Exchange::Xsge).unwrap();
        assert_eq!(out.trades.len(), 2);
        assert_eq!(out.trades[0].1.volume, 3.0);
        assert_eq!(out.trades[1].1.volume, 3.0);
        assert_eq!(out.depths.len(), 3);
        assert!(out
            .depths
            .iter()
            .any(|depth| local_hour(depth.ts_utc_ns) == 8));
        assert!(out
            .depths
            .iter()
            .any(|depth| local_hour(depth.ts_utc_ns) == 15));
    }

    #[test]
    fn open_int_writes_first_and_changes_including_auction() {
        let mut first = snap(8, 59, 0, 0, 2.0, 3380.0, 3379.0, 3381.0);
        first.open_int = Some(100.0);
        let mut same = snap(9, 0, 0, 0, 5.0, 3382.0, 3381.0, 3383.0);
        same.open_int = Some(100.0);
        let mut changed = snap(9, 0, 1, 0, 8.0, 3383.0, 3382.0, 3384.0);
        changed.open_int = Some(104.0);
        let mut snaps = vec![first, same, changed];
        let out = process_instrument(&mut snaps, Exchange::Xsge).unwrap();
        assert_eq!(out.open_ints.len(), 2);
        assert_eq!(out.open_ints[0].1.open_int, 100.0);
        assert_eq!(out.open_ints[0].1.prev_open_int, None);
        assert_eq!(out.open_ints[0].1.delta, None);
        assert_eq!(out.open_ints[1].1.open_int, 104.0);
        assert_eq!(out.open_ints[1].1.prev_open_int, Some(100.0));
        assert_eq!(out.open_ints[1].1.delta, Some(4.0));
    }

    #[test]
    fn queue_writes_every_source_row() {
        let date = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
        let row = |second: u32, nano: u32, bid_qty0: f64| {
            let time = chrono::NaiveTime::from_hms_nano_opt(9, 0, second, nano).unwrap();
            let mut bid_qty = [None; QUEUE_LEVELS];
            bid_qty[0] = Some(bid_qty0);
            QueueSnapshot {
                trad_day: date,
                instrument_id: "i2609".into(),
                product_id: "I".into(),
                event_time: Shanghai
                    .from_local_datetime(&date.and_time(time))
                    .single()
                    .unwrap(),
                seq: 1,
                source_row: second,
                file_order: 0,
                bid_price: Some(751.0),
                ask_price: Some(754.0),
                bid_qty,
                ask_qty: [None; QUEUE_LEVELS],
            }
        };
        let mut snaps = vec![row(0, 0, 1.0), row(0, 250_000_000, 2.0), row(1, 0, 3.0)];
        let out = process_queues(&mut snaps).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].0, 0);
        assert_eq!(out[1].0, 0);
        assert_eq!(out[1].1.bid_qty[0], Some(2.0));
        assert_ne!(out[0].1.ts_utc_ns, out[1].1.ts_utc_ns);
    }
}
