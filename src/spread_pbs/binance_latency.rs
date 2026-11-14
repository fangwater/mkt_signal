//! Minimal BTCUSDT Ticker / Depth / Trade benchmark: Q-E / Q-T, P50 / P99 only.
//!
//! Reuses the WS receive timestamp and production deduplication. No extra clock
//! reads, per-frame logging, raw storage, connections, or processing-stage probes.
use anyhow::{ensure, Context, Result};
use hdrhistogram::Histogram;
use serde::Serialize;

pub const WINDOW_ENV: &str = "SPREAD_PBS_BINANCE_LATENCY_WINDOW_SECS";
const SYMBOL: &str = "BTCUSDT";

#[derive(Debug)]
struct SignedHistogram {
    positive: Histogram<u64>,
    negative: Histogram<u64>,
    missing: u64,
}

impl SignedHistogram {
    fn new() -> Self {
        // Fixed initial range covers the observed multi-second tails. Resize is
        // allowed for larger factual values; never clamp them to the plot range.
        let make = || {
            let mut h = Histogram::new_with_bounds(1, 60_000_000, 3)
                .expect("constant HDR bounds and precision are valid");
            h.auto(true);
            h
        };
        Self {
            positive: make(),
            negative: make(),
            missing: 0,
        }
    }

    fn record(&mut self, recv_us: i64, origin_us: Option<i64>) {
        let delta = origin_us
            .filter(|v| *v > 0)
            .and_then(|v| recv_us.checked_sub(v));
        let Some(delta) = delta else {
            self.missing += 1;
            return;
        };
        let histogram = if delta < 0 {
            &mut self.negative
        } else {
            &mut self.positive
        };
        if histogram.record(delta.unsigned_abs()).is_err() {
            self.missing += 1;
        }
    }

    fn reset(&mut self) {
        self.positive.reset();
        self.negative.reset();
        self.missing = 0;
    }

    fn len(&self) -> u64 {
        self.positive.len() + self.negative.len()
    }

    fn quantile_ms(&self, q: f64) -> Option<f64> {
        let n = self.len();
        if n == 0 {
            return None;
        }
        let rank = ((n as f64 * q).ceil() as u64).clamp(1, n);
        let negative = self.negative.len();
        let value = if rank <= negative {
            -(self
                .negative
                .value_at_quantile(((negative - rank + 1) as f64 - 0.5) / negative as f64)
                as f64)
        } else {
            self.positive
                .value_at_quantile(((rank - negative) as f64 - 0.5) / self.positive.len() as f64)
                as f64
        };
        Some(value / 1000.0)
    }
}

#[derive(Debug)]
struct Window {
    start_us: i64,
    partial: bool,
    event: SignedHistogram,
    transaction: SignedHistogram,
}

impl Window {
    fn new(start_us: i64, partial: bool) -> Self {
        Self {
            start_us,
            partial,
            event: SignedHistogram::new(),
            transaction: SignedHistogram::new(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct FeedLatencySummary {
    pub venue: &'static str,
    pub symbol: &'static str,
    pub channel: &'static str,
    pub stream: &'static str,
    pub source: &'static str,
    pub population: &'static str,
    pub unit: &'static str,
    pub process_start_us: i64,
    pub window_start_us: i64,
    pub window_end_us: i64,
    pub window_secs: u64,
    pub partial: bool,
    pub quantile_method: &'static str,
    pub event_count: u64,
    pub tx_count: u64,
    pub event_missing: u64,
    pub tx_missing: u64,
    pub event_negative: u64,
    pub tx_negative: u64,
    pub clock_regression_samples: u64,
    pub dropped_windows: u64,
    #[serde(rename = "EventToClient_P50")]
    pub event_p50: Option<f64>,
    #[serde(rename = "EventToClient_P99")]
    pub event_p99: Option<f64>,
    #[serde(rename = "TxToClient_P50")]
    pub tx_p50: Option<f64>,
    #[serde(rename = "TxToClient_P99")]
    pub tx_p99: Option<f64>,
}

struct ChannelLatency {
    process_start_us: i64,
    window_us: i64,
    active: Window,
    pending: Option<Window>,
    spare: Option<Window>,
    channel: Channel,
    source: &'static str,
    clock_regression_samples: u64,
    dropped_windows: u64,
}

impl ChannelLatency {
    pub fn new(now_us: i64, window_secs: u64, source: &'static str) -> Result<Self> {
        ensure!(
            window_secs == 60,
            "latency window must be 60 seconds (or use env=0 to disable)"
        );
        let window_us = window_secs as i64 * 1_000_000;
        let start = now_us.div_euclid(window_us) * window_us;
        Ok(Self {
            process_start_us: now_us,
            window_us,
            active: Window::new(start, now_us != start),
            pending: None,
            source,
            spare: Some(Window::new(start, false)),
            channel: Channel::Ticker,
            clock_regression_samples: 0,
            dropped_windows: 0,
        })
    }

    fn advance(&mut self, now_us: i64) {
        let start = now_us.div_euclid(self.window_us) * self.window_us;
        if start <= self.active.start_us {
            return;
        }
        // Normally reuse a window cleared on the stats tick: no per-minute
        // histogram allocation on the receive path. A delayed tick may lose a
        // window; keep memory bounded and expose that loss.
        let mut next = self.spare.take().unwrap_or_else(|| {
            self.dropped_windows += 1;
            let mut old = self.pending.take().expect("two histogram buffers exist");
            old.event.reset();
            old.transaction.reset();
            old
        });
        self.dropped_windows += ((start - self.active.start_us) / self.window_us - 1) as u64;
        next.start_us = start;
        next.partial = false;
        self.pending = Some(std::mem::replace(&mut self.active, next));
    }

    /// Call only after successful production publication. E/T parsing is skipped
    /// for every other symbol and all duplicate/stale frames.
    pub fn record_published(&mut self, symbol: &str, recv_us: i64, raw: &[u8]) {
        if symbol != SYMBOL {
            return;
        }
        let Some((event_us, transaction_us)) =
            mkt_parsers::binance::parse_event_times_us_raw(raw, self.channel.event_type())
        else {
            return;
        };
        self.record_times(recv_us, event_us, transaction_us);
    }

    fn record_times(&mut self, recv_us: i64, event_us: Option<i64>, transaction_us: Option<i64>) {
        if recv_us < self.active.start_us {
            self.clock_regression_samples += 1;
            return;
        }
        self.advance(recv_us);
        self.active.event.record(recv_us, event_us);
        self.active.transaction.record(recv_us, transaction_us);
    }

    /// Quantile queries and output happen on the existing periodic stats tick,
    /// never on a sample-count threshold in the per-message path.
    pub fn take_completed(&mut self, now_us: i64) -> Option<FeedLatencySummary> {
        // Drain pending before advancing an idle current window.
        if self.pending.is_none() {
            self.advance(now_us);
        }
        let mut window = self.pending.take()?;
        let summary = FeedLatencySummary {
            venue: "binance-futures",
            symbol: SYMBOL,
            channel: self.channel.name(),
            stream: self.channel.stream(),
            source: self.source,
            population: "production_published_first_arrival",
            unit: "Milliseconds",
            process_start_us: self.process_start_us,
            window_start_us: window.start_us,
            window_end_us: window.start_us + self.window_us,
            window_secs: (self.window_us / 1_000_000) as u64,
            partial: window.partial,
            quantile_method: "signed_hdr_3_significant_digits_input_microseconds",
            event_count: window.event.len(),
            tx_count: window.transaction.len(),
            event_missing: window.event.missing,
            tx_missing: window.transaction.missing,
            event_negative: window.event.negative.len(),
            tx_negative: window.transaction.negative.len(),
            clock_regression_samples: std::mem::take(&mut self.clock_regression_samples),
            dropped_windows: std::mem::take(&mut self.dropped_windows),
            event_p50: window.event.quantile_ms(0.50),
            event_p99: window.event.quantile_ms(0.99),
            tx_p50: window.transaction.quantile_ms(0.50),
            tx_p99: window.transaction.quantile_ms(0.99),
        };
        window.event.reset();
        window.transaction.reset();
        self.spare = Some(window);
        Some(summary)
    }

    pub fn log_completed(&mut self, now_us: i64) {
        while let Some(summary) = self.take_completed(now_us) {
            match serde_json::to_string(&summary) {
                Ok(json) => log::info!("binance_feed_latency {}", json),
                Err(error) => log::error!("encode Binance feed latency summary: {error}"),
            }
        }
    }
}

/// Channels correspond to the existing production streams, not new connections.
#[derive(Clone, Copy, Debug)]
pub enum Channel {
    Ticker,
    Depth,
    Trade,
}

impl Channel {
    fn event_type(self) -> &'static str {
        match self {
            Self::Ticker => "bookTicker",
            Self::Depth => "depthUpdate",
            Self::Trade => "trade",
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Ticker => "Ticker",
            Self::Depth => "Depth",
            Self::Trade => "Trade",
        }
    }
    fn stream(self) -> &'static str {
        match self {
            Self::Ticker => "btcusdt@bookTicker",
            Self::Depth => "btcusdt@depth@0ms",
            Self::Trade => "btcusdt@trade",
        }
    }
}

pub struct BinanceFeedLatency {
    channels: [Option<ChannelLatency>; 3],
}

impl BinanceFeedLatency {
    pub fn from_env(now_us: i64, enabled: [bool; 3]) -> Result<Option<Self>> {
        let secs = std::env::var(WINDOW_ENV).unwrap_or_else(|_| "60".to_string());
        let secs: u64 = secs
            .parse()
            .with_context(|| format!("invalid {WINDOW_ENV}"))?;
        if secs == 0 {
            return Ok(None);
        }
        ensure!(secs == 60, "{WINDOW_ENV} must be 60, or 0 to disable");
        Ok(Some(Self::new(now_us, enabled)?))
    }

    pub fn new(now_us: i64, enabled: [bool; 3]) -> Result<Self> {
        let mut channels = [None, None, None];
        for channel in [Channel::Ticker, Channel::Depth, Channel::Trade] {
            if enabled[channel as usize] {
                let mut stats = ChannelLatency::new(now_us, 60, "production")?;
                stats.channel = channel;
                channels[channel as usize] = Some(stats);
            }
        }
        Ok(Self { channels })
    }

    pub fn record_published(&mut self, channel: Channel, symbol: &str, recv_us: i64, raw: &[u8]) {
        if symbol != SYMBOL {
            return;
        }
        if let Some(stats) = &mut self.channels[channel as usize] {
            stats.record_published(symbol, recv_us, raw);
        }
    }

    #[cfg(test)]
    pub(crate) fn take_channel(
        &mut self,
        channel: Channel,
        now_us: i64,
    ) -> Option<FeedLatencySummary> {
        self.channels[channel as usize]
            .as_mut()?
            .take_completed(now_us)
    }

    pub fn log_completed(&mut self, now_us: i64) {
        for stats in self.channels.iter_mut().flatten() {
            stats.log_completed(now_us);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn independent_origins_units_signed_values_and_missing_timestamps() {
        let mut m = ChannelLatency::new(0, 60, "test").unwrap();
        m.record_times(10_000, Some(9_000), Some(8_000));
        m.record_times(10_000, Some(11_000), None);
        let s = m.take_completed(60_000_000).unwrap();
        assert_eq!(
            (s.event_count, s.tx_count, s.tx_missing, s.event_negative),
            (2, 1, 1, 1)
        );
        assert_eq!(
            (s.event_p50, s.event_p99, s.tx_p50),
            (Some(-1.0), Some(1.0), Some(2.0))
        );
        assert!(m.take_completed(60_000_000).is_none());
    }

    #[test]
    fn utc_boundaries_empty_windows_and_startup_partial_are_explicit() {
        let mut m = ChannelLatency::new(5_000_000, 60, "test").unwrap();
        m.record_times(59_999_000, Some(59_998_000), Some(59_997_000));
        m.record_times(60_000_000, Some(59_999_000), Some(59_998_000));
        let a = m.take_completed(61_000_000).unwrap();
        assert_eq!((a.window_start_us, a.event_count, a.partial), (0, 1, true));
        let b = m.take_completed(120_000_000).unwrap();
        assert_eq!(
            (b.window_start_us, b.event_count, b.partial),
            (60_000_000, 1, false)
        );
        let empty = m.take_completed(180_000_000).unwrap();
        assert_eq!((empty.event_count, empty.event_p50), (0, None));
    }

    #[test]
    fn excludes_other_symbols_retains_second_scale_tails_and_reports_clock_steps() {
        let mut m = ChannelLatency::new(60_000_000, 60, "test").unwrap();
        m.record_published("ETHUSDT", 61_000_000, br#"{"E":59000,"T":58000}"#);
        m.record_times(59_000_000, Some(58_000_000), Some(57_000_000));
        m.record_times(62_000_000, Some(60_000_000), Some(59_000_000));
        let s = m.take_completed(120_000_000).unwrap();
        assert_eq!((s.event_count, s.clock_regression_samples), (1, 1));
        assert!((s.event_p99.unwrap() - 2000.0).abs() < 2.0);
        assert!((s.tx_p99.unwrap() - 3000.0).abs() < 3.0);
    }

    #[test]
    fn channel_filter_and_signed_nearest_rank_match_sorted_samples() {
        let mut m = ChannelLatency::new(0, 60, "test").unwrap();
        m.record_published("BTCUSDT", 10_000, br#"{"e":"depthUpdate","E":9,"T":8}"#);
        assert_eq!(m.active.event.len(), 0);
        let mut samples = Vec::new();
        for delta in -70..=120 {
            m.record_times(10_000, Some(10_000 - delta), Some(10_000 - delta));
            samples.push(delta as f64 / 1000.0);
        }
        for q in [0.01, 0.1, 0.37, 0.5, 0.99, 1.0] {
            let index = (samples.len() as f64 * q).ceil() as usize - 1;
            assert_eq!(m.active.event.quantile_ms(q), Some(samples[index]));
        }
    }

    #[test]
    #[ignore = "manual release-mode overhead microbenchmark"]
    fn benchmark_receive_path_observation_overhead() {
        use std::hint::black_box;
        use std::time::Instant;
        let raw = br#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":123,"E":1000,"T":999,"s":"BTCUSDT","b":"100","B":"1","a":"101","A":"1"}}"#;
        let mut m = ChannelLatency::new(0, 60, "test").unwrap();
        let iterations = 1_000_000;
        let start = Instant::now();
        for _ in 0..iterations {
            m.record_published(black_box("BTCUSDT"), black_box(1_002_000), black_box(raw));
        }
        let elapsed = start.elapsed();
        assert_eq!(m.active.event.len(), iterations);
        eprintln!("BTC accepted-frame E/T scan + two HDR records: {:.1} ns/sample (local synthetic, excludes flush and production load)",
            elapsed.as_nanos() as f64 / iterations as f64);
    }

    #[test]
    fn validates_window_and_reports_lost_windows() {
        assert!(ChannelLatency::new(0, 7, "test").is_err());
        let mut m = ChannelLatency::new(0, 60, "test").unwrap();
        m.record_times(60_000_000, None, None);
        m.record_times(120_000_000, None, None);
        assert_eq!(m.take_completed(120_000_000).unwrap().dropped_windows, 1);
    }
}
