//! XNAS.ITCH only: ordinary complete MBO events -> minute source observations.
use ahash::AHashMap;
use anyhow::{bail, ensure, Context, Result};
use chrono::NaiveDate;
use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

pub const NS: u64 = 1_000_000_000;
pub const HEADER: [&str; 14] = [
    "ts_event",
    "rtype",
    "publisher_id",
    "instrument_id",
    "action",
    "side",
    "price",
    "size",
    "channel_id",
    "order_id",
    "flags",
    "ts_in_delta",
    "sequence",
    "symbol",
];

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Day {
    pub day: String,
    pub source: String,
    pub symbol: String,
    pub open_ts: i64,
    pub close_ts: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    pub ric: String,
    pub days: Vec<Day>,
}

// (availability_ns, price_e9, shares, aggressor side, physical data row)
pub type Trade = (u64, i64, u64, String, u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Minute {
    pub ts: i64,
    pub source_quality_flags: i8,
    pub trade_time_uncertain: bool,
    pub source_depth_ts_utc_ns: Option<u64>,
    pub source_row: Option<u64>,
    pub book_depth: i8,
    pub bid: Vec<(i64, u64)>,
    pub ask: Vec<(i64, u64)>,
    pub trades: Vec<Trade>,
}

impl Minute {
    pub fn new(ts: i64) -> Self {
        Self {
            ts,
            source_quality_flags: 0,
            trade_time_uncertain: false,
            source_depth_ts_utc_ns: None,
            source_row: None,
            book_depth: 0,
            bid: vec![],
            ask: vec![],
            trades: vec![],
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Extract {
    pub rows: u64,
    pub actions: BTreeMap<String, u64>,
    pub flags: BTreeMap<String, u64>,
    pub instrument_id: String,
    pub receive_backsteps: u64,
    pub source_bytes: u64,
    pub source_sha256: String,
    pub source_mtime_ns: u128,
    pub bad_book_intervals_ns: Vec<(u64, u64)>,
    pub ignored_post_session_incomplete_rows: usize,
    pub bounded_bad_timestamp_intervals_ns: Vec<(u64, u64, bool)>,
    pub minutes: Vec<Minute>,
}

struct HashReader {
    file: File,
    hash: Sha256,
    bytes: u64,
}
impl Read for HashReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.file.read(buf)?;
        self.hash.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }
}

#[derive(Clone, Copy)]
struct Order {
    side: u8,
    price: i64,
    size: u64,
}

#[derive(Default)]
struct Book {
    orders: AHashMap<(u16, u64), Order>,
    bid: BTreeMap<i64, u64>,
    ask: BTreeMap<i64, u64>,
}

impl Book {
    fn adjust(&mut self, o: Order, add: bool) -> Result<()> {
        let side = if o.side == b'B' {
            &mut self.bid
        } else {
            &mut self.ask
        };
        let old = side.get(&o.price).copied().unwrap_or(0);
        let next = if add {
            old.checked_add(o.size)
        } else {
            old.checked_sub(o.size)
        }
        .context("level quantity overflow/underflow")?;
        if next == 0 {
            side.remove(&o.price);
        } else {
            side.insert(o.price, next);
        }
        Ok(())
    }

    fn apply(&mut self, r: &Row) -> Result<()> {
        let k = (r.channel, r.order_id);
        match r.action {
            b'R' => {
                self.orders.clear();
                self.bid.clear();
                self.ask.clear();
            }
            b'A' | b'M' => {
                ensure!(
                    (r.action == b'M') == self.orders.contains_key(&k),
                    "invalid A/M identity"
                );
                ensure!(
                    matches!(r.side, b'B' | b'A') && r.price > 0 && r.size > 0,
                    "invalid order"
                );
                if let Some(old) = self.orders.remove(&k) {
                    self.adjust(old, false)?;
                }
                let o = Order {
                    side: r.side,
                    price: r.price,
                    size: r.size,
                };
                self.orders.insert(k, o);
                self.adjust(o, true)?;
            }
            b'C' => {
                let old = self.orders.remove(&k).context("cancel unknown order")?;
                ensure!(r.side == old.side, "cancel side mismatch");
                let remaining = old.size.checked_sub(r.size).context("over-cancel")?;
                self.adjust(old, false)?;
                if remaining > 0 {
                    let next = Order {
                        size: remaining,
                        ..old
                    };
                    self.orders.insert(k, next);
                    self.adjust(next, true)?;
                }
            }
            b'T' | b'F' | b'N' => (),
            _ => bail!("unknown MBO action"),
        }
        Ok(())
    }

    fn snapshot(&self, minute: &mut Minute, available: u64, row: u64) {
        minute.source_row = Some(row);
        let (Some((bp, _)), Some((ap, _))) =
            (self.bid.last_key_value(), self.ask.first_key_value())
        else {
            return;
        };
        if bp > ap {
            return;
        }
        minute.bid = self
            .bid
            .iter()
            .rev()
            .take(10)
            .map(|(&p, &s)| (p, s))
            .collect();
        minute.ask = self.ask.iter().take(10).map(|(&p, &s)| (p, s)).collect();
        minute.book_depth = minute.bid.len().min(minute.ask.len()) as i8;
        minute.source_depth_ts_utc_ns = Some(available);
    }
}

struct Row {
    action: u8,
    side: u8,
    price: i64,
    size: u64,
    channel: u16,
    order_id: u64,
    available: u64,
    source_row: u64,
    flags: u8,
}

pub fn price_e9(raw: &str) -> Result<i64> {
    if raw.is_empty() {
        return Ok(0);
    }
    let (whole, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    ensure!(
        fraction.len() <= 9 && fraction.bytes().all(|b| b.is_ascii_digit()),
        "invalid decimal"
    );
    let integral: i64 = whole.parse()?;
    ensure!(
        integral >= 0 && !whole.starts_with('-'),
        "negative equity price"
    );
    let frac = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i64>()? * 10_i64.pow(9 - fraction.len() as u32)
    };
    integral
        .checked_mul(NS as i64)
        .and_then(|x| x.checked_add(frac))
        .context("price overflow")
}

pub fn timestamp(raw: &str) -> Result<u64> {
    ensure!(
        raw.len() >= 25 && raw.ends_with("+00:00"),
        "timestamp must have UTC offset"
    );
    let day = NaiveDate::parse_from_str(&raw[..10], "%Y-%m-%d")?;
    let h: u32 = raw[11..13].parse()?;
    let m: u32 = raw[14..16].parse()?;
    let tail = &raw[17..raw.len() - 6];
    let (s, fraction) = tail.split_once('.').unwrap_or((tail, ""));
    let s: u32 = s.parse()?;
    ensure!(
        fraction.len() <= 9 && fraction.bytes().all(|b| b.is_ascii_digit()),
        "timestamp precision"
    );
    let whole = day
        .and_hms_opt(h, m, s)
        .context("invalid clock")?
        .and_utc()
        .timestamp();
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u64>()? * 10_u64.pow(9 - fraction.len() as u32)
    };
    (whole as u64)
        .checked_mul(NS)
        .and_then(|x| x.checked_add(fraction))
        .context("timestamp overflow")
}

pub fn extract(day: &Day) -> Result<Extract> {
    ensure!(
        day.open_ts > 0 && day.close_ts > day.open_ts && (day.close_ts - day.open_ts) % 60 == 0,
        "session bounds"
    );
    let path = Path::new(&day.source);
    let before = path.metadata()?;
    let hash_reader = HashReader {
        file: File::open(path)?,
        hash: Sha256::new(),
        bytes: 0,
    };
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(1 << 20, hash_reader));
    let mut reader = csv::ReaderBuilder::new().from_reader(decoder);
    ensure!(reader.headers()?.iter().eq(HEADER), "MBO header mismatch");
    let mut book = Book::default();
    let mut minutes: Vec<Minute> = (day.open_ts..day.close_ts)
        .step_by(60)
        .map(Minute::new)
        .collect();
    let mut actions = BTreeMap::<String, u64>::new();
    let mut flags_seen = BTreeMap::<String, u64>::new();
    let mut instrument = None;
    let mut pending = vec![];
    let mut last_available = 0;
    let mut previous_raw_available = 0;
    let mut backsteps = 0;
    let mut current_minute = None;
    let mut row_no = 0;
    let mut rec = csv::StringRecord::new();
    let mut source_row = 0;
    let mut invalid_since = None;
    let mut invalid_intervals = Vec::new();
    let mut group_start = 0;
    let mut group_bad_time = false;
    let mut deferred_bad_time = None;
    let mut bounded_bad_timestamp_intervals = Vec::new();
    while reader.read_record(&mut rec)? {
        row_no += 1;
        ensure!(
            rec.len() == 14 && &rec[1] == "160" && &rec[2] == "2" && rec[13] == day.symbol,
            "MBO identity/width row={row_no}"
        );
        if let Some(id) = &instrument {
            ensure!(id == &rec[3], "multiple instruments");
        } else {
            instrument = Some(rec[3].to_string());
        }
        let flags: u8 = rec[10].parse()?;
        *flags_seen.entry(flags.to_string()).or_default() += 1;
        ensure!(rec[4].len() == 1 && rec[5].len() == 1, "action/side width");
        let action = rec[4].as_bytes()[0];
        *actions.entry(rec[4].to_string()).or_default() += 1;
        let unsupported_time = flags & 32 != 0;
        let initial_clear =
            row_no == 1 && action == b'R' && flags & 8 != 0 && flags & (32 | 64 | 16 | 4) == 0;
        ensure!(
            initial_clear || (!unsupported_time && flags & (64 | 16) == 0),
            "unsupported MBO time/book quality flags={flags} row={row_no}"
        );
        let available = if flags & 8 != 0 {
            0
        } else {
            let event = timestamp(&rec[0])?;
            let recv = i128::from(event) + rec[11].parse::<i128>()?;
            ensure!(
                recv > 0 && recv <= i128::from(u64::MAX),
                "receive time overflow"
            );
            recv as u64
        };
        let row = Row {
            action,
            side: rec[5].as_bytes()[0],
            price: price_e9(&rec[6])?,
            size: rec[7].parse()?,
            channel: rec[8].parse()?,
            order_id: rec[9].parse()?,
            available,
            source_row: row_no,
            flags,
        };
        if initial_clear {
            book.apply(&row)?;
            continue;
        }
        if available > 0 && available < previous_raw_available {
            backsteps += 1;
        }
        if available > 0 {
            previous_raw_available = available;
        }
        pending.push(row);
        group_bad_time |= flags & 8 != 0;
        if flags & 128 == 0 {
            continue;
        }
        if group_bad_time {
            let has_trade = pending[group_start..]
                .iter()
                .any(|r| r.action == b'T' && r.price > 0 && r.size > 0);
            let state = deferred_bad_time.get_or_insert((last_available, false));
            state.1 |= has_trade;
            group_start = pending.len();
            group_bad_time = false;
            continue;
        }
        let available = pending
            .iter()
            .map(|r| r.available)
            .max()
            .unwrap()
            .max(last_available);
        if let Some((left, has_trade)) = deferred_bad_time.take() {
            ensure!(
                left > 0 && left / (60 * NS) == available / (60 * NS),
                "bad receive timestamp lacks trusted bounds within one minute"
            );
            bounded_bad_timestamp_intervals.push((left, available, has_trade));
        }
        let minute = (available / (60 * NS)) as i64 * 60;
        if let Some(prior) = current_minute {
            if prior != minute && prior >= day.open_ts && prior < day.close_ts {
                if invalid_since.is_none() {
                    book.snapshot(
                        &mut minutes[((prior - day.open_ts) / 60) as usize],
                        last_available,
                        source_row,
                    );
                }
            }
        }
        let has_gap = pending.iter().any(|r| r.flags & 4 != 0);
        if has_gap && invalid_since.is_none() {
            invalid_since = Some(available);
            book = Book::default();
        }
        for r in pending.drain(..) {
            // A clean clear is the only proof that the unknown resting orders
            // following a channel gap have been removed. Flag disappearance
            // alone cannot repair a book. Trades remain observed source prints.
            if !has_gap && r.action == b'R' {
                if let Some(start) = invalid_since.take() {
                    invalid_intervals.push((start, available));
                }
            }
            if invalid_since.is_none() {
                book.apply(&r)
                    .with_context(|| format!("{} row={}", day.source, r.source_row))?;
            }
            if r.action == b'T'
                && available >= day.open_ts as u64 * NS
                && available < day.close_ts as u64 * NS
            {
                ensure!(matches!(r.side, b'B' | b'A' | b'N'), "invalid trade side");
                if r.price > 0 && r.size > 0 {
                    minutes[((minute - day.open_ts) / 60) as usize]
                        .trades
                        .push((
                            available,
                            r.price,
                            r.size,
                            (r.side as char).to_string(),
                            r.source_row,
                        ));
                }
            }
        }
        current_minute = Some(minute);
        last_available = available;
        source_row = row_no;
        group_start = 0;
    }
    // An unfinished group entirely after the requested session cannot change
    // its already completed prefix. An unfinished RTH/cross-close group still
    // blocks the day. The complete gzip is always read and CRC checked above.
    let ignored_post_session_incomplete_rows = pending.len();
    ensure!(
        deferred_bad_time.is_none()
            && pending
                .iter()
                .all(|r| r.available >= day.close_ts as u64 * NS),
        "file ends inside an event within/before the session"
    );
    if let Some(prior) = current_minute {
        if prior >= day.open_ts && prior < day.close_ts {
            if invalid_since.is_none() {
                book.snapshot(
                    &mut minutes[((prior - day.open_ts) / 60) as usize],
                    last_available,
                    source_row,
                );
            }
        }
    }
    if let Some(start) = invalid_since {
        invalid_intervals.push((
            start,
            (day.close_ts as u64 * NS).max(last_available.saturating_add(1)),
        ));
    }
    for m in &mut minutes {
        let start = m.ts as u64 * NS;
        let end = start + 60 * NS;
        if invalid_intervals.iter().any(|&(a, b)| a < end && b > start) {
            m.source_quality_flags |= 4;
            m.source_depth_ts_utc_ns = None;
            m.book_depth = 0;
            m.bid.clear();
            m.ask.clear();
        }
        for &(left, right, has_trade) in &bounded_bad_timestamp_intervals {
            if left < end && right >= start {
                m.source_quality_flags |= 8;
                m.trade_time_uncertain |= has_trade;
                m.source_depth_ts_utc_ns = None;
                m.book_depth = 0;
                m.bid.clear();
                m.ask.clear();
            }
        }
    }
    let decoder = reader.into_inner();
    let hashed = decoder.into_inner().into_inner();
    ensure!(
        hashed.bytes == before.len(),
        "incomplete compressed source read"
    );
    let after = path.metadata()?;
    ensure!(
        before.len() == after.len() && before.modified()? == after.modified()?,
        "source changed during read"
    );
    Ok(Extract {
        rows: row_no,
        actions,
        flags: flags_seen,
        instrument_id: instrument.context("empty MBO source")?,
        receive_backsteps: backsteps,
        source_bytes: hashed.bytes,
        source_sha256: hex::encode(hashed.hash.finalize()),
        source_mtime_ns: before
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
        bad_book_intervals_ns: invalid_intervals,
        ignored_post_session_incomplete_rows,
        bounded_bad_timestamp_intervals_ns: bounded_bad_timestamp_intervals,
        minutes,
    })
}
