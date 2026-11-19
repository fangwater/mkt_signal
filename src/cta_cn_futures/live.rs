use super::{config::Config, history::Dominant};
use anyhow::{ensure, Context as _, Result};
use chrono::{Datelike, NaiveDate, Timelike};
use cn_futures_core::{
    baseline_1min::{synthesize_minutes, BaselineMinute, PrintTrade, SizeBuckets},
    events::{process_instrument, Snapshot},
    session::{is_auction_ts, shanghai, Exchange},
};
use cn_futures_pbs::{
    msg::{MarketDataMsg, SOURCE_RESET},
    sources::{zmq::ZmqSource, Source},
};
use cn_market_proto::{
    Book5, BookLevel, GetSizeThresholdsResponse, GetTradingSessionsResponse, MinuteBar,
    SessionWindow, SizeThreshold,
};
use std::{collections::BTreeMap, time::Duration};

#[derive(Clone)]
pub enum Event {
    Reset,
    Quote(Box<MarketDataMsg>),
}
pub fn capture<'a>(
    endpoint: &str,
    configs: impl Iterator<Item = &'a Config>,
) -> Result<Vec<crossbeam_channel::Receiver<Event>>> {
    struct Product {
        venue: String,
        prefix: String,
        month_len: usize,
        tx: crossbeam_channel::Sender<Event>,
        lost: bool,
    }

    let mut receivers = Vec::new();
    let mut products: Vec<_> = configs
        .map(|cfg| {
            let (tx, rx) = crossbeam_channel::bounded(131072);
            receivers.push(rx);
            Product {
                venue: cfg.exchange.clone(),
                prefix: cfg.native_prefix(),
                month_len: if cfg.exchange == "CZCE" { 3 } else { 4 },
                tx,
                lost: true,
            }
        })
        .collect();
    let endpoint = endpoint.to_owned();
    std::thread::Builder::new()
        .name("cn-md-ingress".into())
        .spawn(move || loop {
            let result = (|| -> Result<()> {
                let mut source = ZmqSource::new(&endpoint, 131072)?;
                for product in &mut products {
                    product.lost = true;
                }
                loop {
                    products.retain_mut(|product| {
                        if product.lost {
                            match product.tx.try_send(Event::Reset) {
                                Ok(()) => product.lost = false,
                                Err(crossbeam_channel::TrySendError::Full(_)) => {}
                                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                                    return false;
                                }
                            }
                        }
                        true
                    });
                    if products.is_empty() {
                        return Ok(());
                    }
                    if let Some(q) = source.poll()? {
                        if q.flags & SOURCE_RESET != 0 {
                            for product in &mut products {
                                product.lost = true;
                            }
                        } else if let Some(product) = products
                            .iter_mut()
                            .find(|product| {
                                q.venue() == product.venue.as_bytes()
                                    && q.symbol()
                                        .strip_prefix(product.prefix.as_bytes())
                                        .is_some_and(|month| {
                                            month.len() == product.month_len
                                                && month.iter().all(u8::is_ascii_digit)
                                        })
                            })
                            .filter(|product| !product.lost)
                        {
                            if product.tx.try_send(Event::Quote(Box::new(q))).is_err() {
                                product.lost = true;
                            }
                        }
                    } else {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            })();
            match result {
                Ok(()) => return,
                Err(e) => {
                    eprintln!("CN ingress reconnecting; cache repair required: {e:#}");
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        })?;
    Ok(receivers)
}

/// All market parameters come from the dedicated gRPC methods, once per catch-up.
#[derive(Clone)]
pub struct Context {
    exchange: Exchange,
    product: String,
    thresholds: BTreeMap<u32, SizeThreshold>,
    windows: Vec<SessionWindow>,
    pub snapshot_as_of: String,
}
impl Context {
    pub fn new(
        exchange: Exchange,
        symbol: &str,
        thresholds: GetSizeThresholdsResponse,
        sessions: GetTradingSessionsResponse,
    ) -> Result<Self> {
        let mut selected = thresholds
            .products
            .into_iter()
            .filter(|p| p.product == symbol);
        let product = selected
            .next()
            .with_context(|| format!("gRPC has no {symbol} size thresholds"))?;
        ensure!(
            selected.next().is_none(),
            "duplicate {symbol} threshold product"
        );
        let mut thresholds = BTreeMap::new();
        for row in product.thresholds {
            ensure!(
                NaiveDate::from_ymd_opt(
                    (row.applies_month / 100) as i32,
                    row.applies_month % 100,
                    1
                )
                .is_some()
                    && row.p50_turnover_cny.is_finite()
                    && row.p50_turnover_cny > 0.
                    && row.p90_turnover_cny.is_finite()
                    && row.p90_turnover_cny >= row.p50_turnover_cny,
                "invalid gRPC size threshold"
            );
            ensure!(
                thresholds.insert(row.applies_month, row).is_none(),
                "duplicate applicable threshold month"
            );
        }
        ensure!(!thresholds.is_empty(), "gRPC size thresholds are not ready");
        let mut selected = sessions
            .products
            .into_iter()
            .filter(|p| p.product == symbol);
        let product = selected
            .next()
            .with_context(|| format!("gRPC has no {symbol} sessions"))?;
        ensure!(
            selected.next().is_none() && !product.windows.is_empty(),
            "missing or duplicate {symbol} session product"
        );
        for w in &product.windows {
            ensure!(
                w.begin_sec < 86400
                    && w.end_sec <= 86400
                    && w.begin_sec % 60 == 0
                    && w.end_sec % 60 == 0,
                "invalid continuous window"
            );
        }
        NaiveDate::parse_from_str(&sessions.snapshot_as_of, "%Y-%m-%d")
            .context("gRPC session snapshot missing or invalid")?;
        Ok(Self {
            exchange,
            product: symbol.into(),
            thresholds,
            windows: product.windows,
            snapshot_as_of: sessions.snapshot_as_of,
        })
    }
    fn threshold(&self, ts_us: i64) -> Result<&SizeThreshold> {
        let day = shanghai(ts_us / 1_000_000);
        let month = day.year() as u32 * 100 + day.month();
        self.thresholds
            .get(&month)
            .with_context(|| format!("gRPC has no applicable size thresholds for {month}"))
    }
    pub(super) fn allows(&self, ts_ms: i64) -> bool {
        let sec = shanghai(ts_ms / 1000).time().num_seconds_from_midnight();
        self.windows.iter().any(|w| {
            if w.begin_sec < w.end_sec {
                w.begin_sec <= sec && sec < w.end_sec
            } else {
                sec >= w.begin_sec || sec < w.end_sec
            }
        })
    }
    pub fn ends_after(&self, minute_ms: i64) -> bool {
        let sec = shanghai((minute_ms + 60_000) / 1000)
            .time()
            .num_seconds_from_midnight();
        self.windows.iter().any(|w| w.end_sec % 86400 == sec)
    }
}
/// Exchange-clock evidence from one contract's own snapshot cadence.
#[derive(Clone, Copy, Default)]
struct Witness {
    last_ms: i64,
    min_interval_ms: i64,
    intervals: u32,
}
/// Learn a contract's cadence before trusting it; the minimum observed
/// interval is a lower bound, never an assumed 500 ms.
const WITNESS_INTERVALS: u32 = 20;
impl Witness {
    fn observe(&mut self, ts_ms: i64) {
        if self.last_ms > 0 && ts_ms > self.last_ms {
            let delta = ts_ms - self.last_ms;
            self.min_interval_ms = if self.intervals == 0 {
                delta
            } else {
                self.min_interval_ms.min(delta)
            };
            self.intervals = self.intervals.saturating_add(1);
        }
        self.last_ms = self.last_ms.max(ts_ms);
    }
    /// This contract's latest snapshot is in the final slot of `minute_ms` (or
    /// later): its next snapshot cannot be stamped inside that minute.
    fn completes(&self, minute_ms: i64) -> bool {
        self.intervals >= WITNESS_INTERVALS
            && self.last_ms >= minute_ms
            && self.last_ms + self.min_interval_ms >= minute_ms + 60_000
    }
}
pub struct Input {
    pub multiple: f64,
    pub live_after_ms: i64,
    pub context: Context,
    day: u32,
    snaps: Vec<Snapshot>,
    processed_minute_ms: i64,
    last_quote_ms: i64,
    pub last_received_us: u64,
    last_quote: Option<MarketDataMsg>,
    main_clock: Witness,
    secondary_clock: Witness,
    /// End of the latest minute sealed by the two contract clocks.
    witness_sealed_ms: i64,
}
impl Input {
    pub fn new(context: Context, multiple: f64, first_ms: i64) -> Self {
        Self {
            multiple,
            live_after_ms: chrono::Utc::now().timestamp_millis(),
            context,
            day: 0,
            snaps: Vec::new(),
            processed_minute_ms: first_ms.div_euclid(60_000) * 60_000,
            last_quote_ms: 0,
            last_received_us: 0,
            last_quote: None,
            main_clock: Witness::default(),
            secondary_clock: Witness::default(),
            witness_sealed_ms: 0,
        }
    }
    pub fn from_quotes(
        context: Context,
        multiple: f64,
        quotes: Vec<MarketDataMsg>,
        main: &Dominant,
    ) -> Result<(Self, Vec<MinuteBar>)> {
        let first = quotes
            .first()
            .context("waiting for ZMQ snapshots")?
            .exchange_ts_us
            / 1000;
        let mut input = Self::new(context, multiple, first);
        for q in quotes {
            input.ingest(&q, main)?;
        }
        // The source's next minute proves the preceding one has arrived.
        // A slow gRPC call or a stale ZMQ connection must not close a partial bar.
        let rows = input.closed(input.last_quote_ms.div_euclid(60_000) * 60_000, main)?;
        Ok((input, rows))
    }
    pub fn observe(&mut self, event: Event, main: &Dominant) -> Result<Vec<MinuteBar>> {
        let Event::Quote(q) = event else {
            anyhow::bail!("ZMQ continuity reset; gRPC catch-up required");
        };
        let old = self.last_quote_ms;
        self.witness(&q, main)?;
        self.ingest(&q, main)?;
        if old.div_euclid(60_000) == self.last_quote_ms.div_euclid(60_000) {
            return self.sealed_by_clocks(main);
        }
        let mut rows = self.closed(self.last_quote_ms.div_euclid(60_000) * 60_000, main)?;
        rows.extend(self.sealed_by_clocks(main)?);
        Ok(rows)
    }
    /// Secondary-contract snapshots only advance its exchange clock.
    fn witness(&mut self, q: &MarketDataMsg, main: &Dominant) -> Result<()> {
        let Some(secondary) = main.secondary.as_deref() else {
            return Ok(());
        };
        if q.symbol() != secondary.as_bytes()
            || q.exchange_ts_us <= 0
            || q.trading_day != main.trading_day
        {
            return Ok(());
        }
        let ts_ms = q.exchange_ts_us / 1000;
        if self.context.allows(ts_ms) {
            self.secondary_clock.observe(ts_ms);
        }
        Ok(())
    }
    /// Seal the dominant's current minute once both the dominant and the
    /// secondary contract have delivered that minute's final snapshot slot,
    /// instead of waiting for the dominant's next-minute quote.
    fn sealed_by_clocks(&mut self, main: &Dominant) -> Result<Vec<MinuteBar>> {
        if main.secondary.is_none() || self.last_quote_ms == 0 {
            return Ok(Vec::new());
        }
        let minute = self.last_quote_ms.div_euclid(60_000) * 60_000;
        if minute <= self.processed_minute_ms
            || !self.main_clock.completes(minute)
            || !self.secondary_clock.completes(minute)
        {
            return Ok(Vec::new());
        }
        let rows = self.closed(minute + 60_000, main)?;
        self.witness_sealed_ms = self.witness_sealed_ms.max(minute + 60_000);
        Ok(rows)
    }
    fn ingest(&mut self, q: &MarketDataMsg, main: &Dominant) -> Result<()> {
        if q.symbol() != main.contract.as_bytes() {
            return Ok(());
        }
        ensure!(
            q.exchange_ts_us > 0 && q.trading_day == main.trading_day,
            "native quote has another trading day or missing clock"
        );
        if let Some(last) = &self.last_quote {
            ensure!(
                q.source_generation == last.source_generation,
                "ZMQ generation changed"
            );
            let key = |q: &MarketDataMsg| (q.action_day, q.source_sequence);
            if key(q) == key(last) {
                ensure!(identical(q, last), "conflicting duplicate native snapshot");
                return Ok(());
            }
            ensure!(key(q) > key(last), "native snapshot sequence regressed");
        }
        self.context.threshold(q.exchange_ts_us)?;
        let ts_ms = q.exchange_ts_us / 1000;
        if !self.context.allows(ts_ms)
            && !is_auction_ts(ts_ms / 1000, self.context.exchange)
            && !self.context.ends_after(ts_ms - 60_000)
        {
            return Ok(());
        }
        ensure!(
            self.day == 0 || self.day == q.trading_day,
            "new trading day requires gRPC catch-up"
        );
        self.day = q.trading_day;
        ensure!(
            self.snaps.len() < 200_000,
            "bounded ZMQ snapshot buffer exhausted"
        );
        ensure!(
            ts_ms >= self.last_quote_ms,
            "out-of-order native quote; gRPC catch-up required"
        );
        ensure!(
            ts_ms >= self.witness_sealed_ms,
            "native quote arrived after its minute was sealed by contract clocks; gRPC catch-up required"
        );
        let time = shanghai(q.exchange_ts_us / 1_000_000)
            .with_nanosecond((q.exchange_ts_us.rem_euclid(1_000_000) * 1000) as u32)
            .context("invalid exchange microseconds")?;
        let float = |v: f64| v.is_finite().then_some(v);
        let qty = |v: u64| (v != u64::MAX).then_some(v as f64);
        self.snaps.push(Snapshot {
            trad_day: NaiveDate::parse_from_str(&q.trading_day.to_string(), "%Y%m%d")?,
            instrument_id: main.contract.clone(),
            product_id: self.context.product.clone(),
            event_time: time,
            seq: i64::try_from(q.source_sequence).context("invalid native sequence")?,
            source_row: self.snaps.len() as u32,
            file_order: 0,
            volume: qty(q.volume),
            turnover: float(q.turnover),
            last_price: float(q.last_price),
            open_int: float(q.open_interest),
            bid_prices: std::array::from_fn(|i| float(q.bid[i].price)),
            bid_sizes: std::array::from_fn(|i| qty(q.bid[i].quantity)),
            ask_prices: std::array::from_fn(|i| float(q.ask[i].price)),
            ask_sizes: std::array::from_fn(|i| qty(q.ask[i].quantity)),
        });
        self.last_received_us = q.received_us;
        self.last_quote_ms = ts_ms;
        self.main_clock.observe(ts_ms);
        self.last_quote = Some(*q);
        Ok(())
    }
    pub fn closed(&mut self, before_ms: i64, main: &Dominant) -> Result<Vec<MinuteBar>> {
        if self.snaps.is_empty() {
            return Ok(Vec::new());
        }
        let events = process_instrument(&mut self.snaps, self.context.exchange)?;
        let trades: Vec<_> = events
            .trades
            .iter()
            .map(|(_, r)| PrintTrade::from(r))
            .collect();
        let rows = synthesize_minutes(&main.contract, &trades, &events.depths, self.multiple)?;
        let mut seconds = Vec::new();
        for depth in &events.depths {
            let ts = (depth.ts_utc_ns / 1_000_000_000) as i64;
            if cn_futures_core::baseline_1min::Book5::from_depth(depth).is_some()
                && self.context.allows(ts * 1000)
            {
                seconds.push(ts);
            }
        }
        let segments = cn_futures_core::segments::split_depth_segments(
            &seconds,
            self.context.exchange,
            &main.contract,
        );
        let segments = cn_futures_core::baseline_1min::minute_segments_from_seconds(&segments);
        let rows = cn_futures_core::baseline_1min::fill_session_minutes_with_twap(
            &main.contract,
            rows,
            &segments,
            &trades,
        )?;
        let mut sizes = BTreeMap::<i64, SizeBuckets>::new();
        for t in &trades {
            let threshold = self.context.threshold((t.ts_utc_ns / 1000) as i64)?;
            sizes
                .entry(cn_futures_core::baseline_1min::minute_left_sec(t.ts_utc_ns))
                .or_default()
                .add(
                    t.amount,
                    t.aggressor,
                    threshold.p50_turnover_cny,
                    threshold.p90_turnover_cny,
                );
        }
        let mut out = Vec::new();
        for mut row in rows {
            let ms = row.ts * 1000;
            if ms <= self.processed_minute_ms
                || ms >= before_ms
                || is_auction_ts(row.ts, self.context.exchange)
                || !self.context.allows(ms)
            {
                continue;
            }
            ensure!(
                row.volume == 0. || sizes.contains_key(&row.ts),
                "traded minute omitted size classification"
            );
            let bucket = sizes.get(&row.ts).copied().unwrap_or_default();
            row.large_order = bucket.large_order;
            row.medium_order = bucket.medium_order;
            row.small_order = bucket.small_order;
            row.large_buy = bucket.large_buy;
            row.large_sell = bucket.large_sell;
            row.medium_buy = bucket.medium_buy;
            row.medium_sell = bucket.medium_sell;
            row.small_buy = bucket.small_buy;
            row.small_sell = bucket.small_sell;
            (row.net_buy_large, row.net_buy_medium, row.net_buy_small) = bucket.nets();
            let minute = minute(row, self.day)?;
            out.push(minute);
            self.processed_minute_ms = ms;
        }
        Ok(out)
    }
    pub fn clock(&mut self, now_ms: i64, main: &Dominant) -> Result<Vec<MinuteBar>> {
        let end = (self.last_quote_ms.div_euclid(60_000) + 1) * 60_000;
        // Session tails may have no next quote. Only close after a short grace
        // period, with a fresh final-second snapshot and an already drained queue.
        if !self.context.ends_after(end - 60_000)
            || self.last_quote_ms < end - 1000
            || now_ms < end + 2000
            || now_ms - self.last_quote_ms > 5000
            || (chrono::Utc::now().timestamp_micros() as u64).saturating_sub(self.last_received_us)
                > 5_000_000
        {
            return Ok(Vec::new());
        }
        self.closed(end, main)
    }
}
fn identical(a: &MarketDataMsg, b: &MarketDataMsg) -> bool {
    let eq = |a: f64, b: f64| a == b || (a.is_nan() && b.is_nan());
    a.exchange_ts_us == b.exchange_ts_us
        && a.volume == b.volume
        && eq(a.last_price, b.last_price)
        && eq(a.turnover, b.turnover)
        && eq(a.open_interest, b.open_interest)
        && a.bid
            .iter()
            .chain(&a.ask)
            .zip(b.bid.iter().chain(&b.ask))
            .all(|(a, b)| eq(a.price, b.price) && a.quantity == b.quantity)
}
fn minute(r: BaselineMinute, day: u32) -> Result<MinuteBar> {
    let book = r.book.context("live baseline minute lacks native book")?;
    Ok(MinuteBar {
        provisional: false,
        open_time_ms: r.ts * 1000,
        trading_day: day,
        open: r.open.unwrap_or(f64::NAN),
        high: r.high.unwrap_or(f64::NAN),
        low: r.low.unwrap_or(f64::NAN),
        close: r.close.unwrap_or(f64::NAN),
        snapshot_high: r.high.unwrap_or(f64::NAN),
        snapshot_low: r.low.unwrap_or(f64::NAN),
        volume_lots: r.volume,
        turnover_cny: r.amount,
        vwap: r.vwap.unwrap_or(f64::NAN),
        snapshot_trades: r.count as u32,
        widened: false,
        mid_price: r.mid_price,
        twap: r.twap,
        buy_volume_lots: r.buy_volume,
        sell_volume_lots: r.sell_volume,
        buy_turnover_cny: r.buy_amount,
        sell_turnover_cny: r.sell_amount,
        buy_vwap: r.buy_vwap,
        sell_vwap: r.sell_vwap,
        buy_snapshot_trades: r.buy_count,
        sell_snapshot_trades: r.sell_count,
        avg_interval_turnover_cny: r.avg_amount,
        net_buy_turnover_cny: r.net_buy_amount,
        net_buy_volume_lots: r.net_buy_volume,
        net_buy_pct: r.net_buy_pct,
        size_buckets: Some(cn_market_proto::SizeBuckets {
            large_turnover_cny: r.large_order,
            medium_turnover_cny: r.medium_order,
            small_turnover_cny: r.small_order,
            large_buy_cny: r.large_buy,
            large_sell_cny: r.large_sell,
            medium_buy_cny: r.medium_buy,
            medium_sell_cny: r.medium_sell,
            small_buy_cny: r.small_buy,
            small_sell_cny: r.small_sell,
            net_buy_large_cny: r.net_buy_large,
            net_buy_medium_cny: r.net_buy_medium,
            net_buy_small_cny: r.net_buy_small,
        }),
        book: Some(Book5 {
            bids: (0..5)
                .map(|i| BookLevel {
                    price: book.bid_prices[i],
                    volume_lots: book.bid_sizes[i],
                })
                .collect(),
            asks: (0..5)
                .map(|i| BookLevel {
                    price: book.ask_prices[i],
                    volume_lots: book.ask_sizes[i],
                })
                .collect(),
        }),
    })
}
