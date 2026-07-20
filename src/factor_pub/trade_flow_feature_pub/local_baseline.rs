//! Exact event-time trade-flow baseline aggregation for local replays.

use crate::depth_pub::orderbook::OrderBook;
use anyhow::Result;
use bytes::Bytes;
use mkt_parsers::msg::mkt_msg::Level;
use mkt_parsers::msg::trade_flow_feature_msg::{TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM};
use rolling_common::kll_quantile::{FrozenKllSketch, StreamingKllSketch};

pub const BASELINE_BAR_MS: i64 = 5_000;
pub const RESAMPLED_BAR_MS: [i64; 2] = [10_000, 60_000];
pub const HOUR_MS: i64 = 3_600_000;
pub const BASELINE_DEPTH_LEVELS: usize = 20;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BaselineStats {
    pub bar_ms: i64,
    pub closed_bars: u64,
    pub traded_bars: u64,
    pub depth20_bars: u64,
    pub padded_depth20_bars: u64,
    pub late_trades: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BaselineDepth20 {
    pub bids: [(f64, f64); BASELINE_DEPTH_LEVELS],
    pub asks: [(f64, f64); BASELINE_DEPTH_LEVELS],
}

#[derive(Debug, Clone)]
pub struct BaselineBar {
    pub start_ms: i64,
    pub has_trade: bool,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub amount: f64,
    pub avg_amount: f64,
    pub count: u64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub buy_amount: f64,
    pub sell_amount: f64,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub vwap: f64,
    pub buy_vwap: f64,
    pub sell_vwap: f64,
    pub net_buy_amount: f64,
    pub net_buy_volume: f64,
    pub net_buy_pct: f64,
    /// Latest book at the right edge of this bar, padded with zero levels when needed.
    pub depth20: BaselineDepth20,
}

#[derive(Debug, Clone)]
pub struct HourlyNotionalKllSnapshot {
    pub hour_start_ms: i64,
    pub sketch: FrozenKllSketch,
}

/// One event-time, hour-aligned notional KLL. A new hour freezes and resets
/// the sketch; empty hours do not produce snapshots.
pub struct HourlyNotionalKll {
    current_hour_start_ms: Option<i64>,
    sketch: StreamingKllSketch,
    late_trades: u64,
}

impl Default for HourlyNotionalKll {
    fn default() -> Self {
        Self::new()
    }
}

impl HourlyNotionalKll {
    pub fn new() -> Self {
        Self {
            current_hour_start_ms: None,
            sketch: StreamingKllSketch::new(),
            late_trades: 0,
        }
    }

    pub fn on_trade(
        &mut self,
        timestamp_us: i64,
        price: f64,
        amount: f64,
    ) -> Option<HourlyNotionalKllSnapshot> {
        let notional = price * amount;
        if !notional.is_finite() || notional <= 0.0 {
            return None;
        }
        let hour_start_ms = align(timestamp_us.div_euclid(1_000), HOUR_MS);
        let snapshot = match self.current_hour_start_ms {
            None => {
                self.current_hour_start_ms = Some(hour_start_ms);
                None
            }
            Some(current) if hour_start_ms < current => {
                self.late_trades = self.late_trades.saturating_add(1);
                return None;
            }
            Some(current) if hour_start_ms > current => {
                let snapshot = self.freeze_current();
                self.current_hour_start_ms = Some(hour_start_ms);
                snapshot
            }
            Some(_) => None,
        };
        self.sketch.insert(notional);
        snapshot
    }

    pub fn flush(&mut self) -> Option<HourlyNotionalKllSnapshot> {
        self.freeze_current()
    }

    pub fn late_trades(&self) -> u64 {
        self.late_trades
    }

    fn freeze_current(&mut self) -> Option<HourlyNotionalKllSnapshot> {
        let hour_start_ms = self.current_hour_start_ms.take()?;
        if self.sketch.is_empty() {
            return None;
        }
        let snapshot = HourlyNotionalKllSnapshot {
            hour_start_ms,
            sketch: self.sketch.freeze(),
        };
        self.sketch.reset();
        Some(snapshot)
    }
}

impl BaselineBar {
    fn new(start_ms: i64) -> Self {
        Self {
            start_ms,
            has_trade: false,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            amount: 0.0,
            avg_amount: 0.0,
            count: 0,
            buy_count: 0,
            sell_count: 0,
            buy_amount: 0.0,
            sell_amount: 0.0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            vwap: 0.0,
            buy_vwap: 0.0,
            sell_vwap: 0.0,
            net_buy_amount: 0.0,
            net_buy_volume: 0.0,
            net_buy_pct: 0.0,
            depth20: BaselineDepth20 {
                bids: [(0.0, 0.0); BASELINE_DEPTH_LEVELS],
                asks: [(0.0, 0.0); BASELINE_DEPTH_LEVELS],
            },
        }
    }

    fn update_trade(&mut self, is_buy: bool, price: f64, amount: f64) {
        let notional = price * amount;
        if !price.is_finite() || !amount.is_finite() || price <= 0.0 || amount <= 0.0 {
            return;
        }
        if !self.has_trade {
            self.has_trade = true;
            self.open = price;
            self.high = price;
            self.low = price;
        } else {
            self.high = self.high.max(price);
            self.low = self.low.min(price);
        }
        self.close = price;
        self.volume += amount;
        self.amount += notional;
        self.count = self.count.saturating_add(1);
        if is_buy {
            self.buy_count = self.buy_count.saturating_add(1);
            self.buy_amount += notional;
            self.buy_volume += amount;
        } else {
            self.sell_count = self.sell_count.saturating_add(1);
            self.sell_amount += notional;
            self.sell_volume += amount;
        }
    }

    fn merge(&mut self, source: &Self) {
        if source.has_trade {
            if !self.has_trade {
                self.has_trade = true;
                self.open = source.open;
                self.high = source.high;
                self.low = source.low;
            } else {
                self.high = self.high.max(source.high);
                self.low = self.low.min(source.low);
            }
            self.close = source.close;
        }
        self.volume += source.volume;
        self.amount += source.amount;
        self.count = self.count.saturating_add(source.count);
        self.buy_count = self.buy_count.saturating_add(source.buy_count);
        self.sell_count = self.sell_count.saturating_add(source.sell_count);
        self.buy_amount += source.buy_amount;
        self.sell_amount += source.sell_amount;
        self.buy_volume += source.buy_volume;
        self.sell_volume += source.sell_volume;
    }

    fn finalize(
        mut self,
        last_close: Option<f64>,
        last_vwap: Option<f64>,
        last_buy_vwap: Option<f64>,
        last_sell_vwap: Option<f64>,
    ) -> Self {
        if !self.has_trade {
            let px = last_close.unwrap_or(0.0);
            self.open = px;
            self.high = px;
            self.low = px;
            self.close = px;
        }
        self.avg_amount = if self.count == 0 {
            0.0
        } else {
            self.amount / self.count as f64
        };
        self.vwap = if self.volume > 0.0 {
            self.amount / self.volume
        } else {
            last_vwap.unwrap_or(0.0)
        };
        self.buy_vwap = if self.buy_volume > 0.0 {
            self.buy_amount / self.buy_volume
        } else {
            last_buy_vwap.unwrap_or(0.0)
        };
        self.sell_vwap = if self.sell_volume > 0.0 {
            self.sell_amount / self.sell_volume
        } else {
            last_sell_vwap.unwrap_or(0.0)
        };
        self.net_buy_amount = self.buy_amount - self.sell_amount;
        self.net_buy_volume = self.buy_volume - self.sell_volume;
        self.net_buy_pct = self.net_buy_amount / (self.buy_amount + self.sell_amount + 1e-6);
        self
    }

    /// Encodes the same wire format as the live trade-flow publisher.
    ///
    /// Local Tardis baseline aggregation has no amount-threshold configuration,
    /// so the large/medium/small trade buckets and their net values are zero.
    /// Missing book levels are encoded as zero price and zero amount so every
    /// completed bar has the standard 20-level wire shape.
    pub fn to_trade_flow_feature_payload(&self, symbol: &str, venue: u8) -> Result<Bytes> {
        let depth = &self.depth20;
        let values = self.to_trade_flow_feature_values();
        let mut depth_values = [0.0; BASELINE_DEPTH_LEVELS * 4];
        for (index, (price, amount)) in depth.bids.iter().enumerate() {
            let offset = index * 2;
            depth_values[offset] = *price;
            depth_values[offset + 1] = *amount;
        }
        for (index, (price, amount)) in depth.asks.iter().enumerate() {
            let offset = BASELINE_DEPTH_LEVELS * 2 + index * 2;
            depth_values[offset] = *price;
            depth_values[offset + 1] = *amount;
        }
        TradeFlowFeatureMsg::encode_from_slices(
            symbol,
            venue,
            self.start_ms,
            &values,
            &depth_values,
        )
        .map_err(Into::into)
    }

    fn to_trade_flow_feature_values(&self) -> [f64; TRADE_FLOW_FEATURE_DIM] {
        [
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.amount,
            self.avg_amount,
            self.count as f64,
            self.buy_count as f64,
            self.sell_count as f64,
            self.buy_amount,
            self.sell_amount,
            self.buy_volume,
            self.sell_volume,
            0.0, // large_order
            0.0, // medium_order
            0.0, // small_order
            0.0, // large_buy
            0.0, // large_sell
            0.0, // medium_buy
            0.0, // medium_sell
            0.0, // small_buy
            0.0, // small_sell
            self.vwap,
            self.buy_vwap,
            self.sell_vwap,
            self.net_buy_amount,
            self.net_buy_volume,
            self.net_buy_pct,
            0.0, // net_buy_large
            0.0, // net_buy_medium
            0.0, // net_buy_small
        ]
    }
}

struct BarState {
    bar_ms: i64,
    current: Option<BaselineBar>,
    last_close: Option<f64>,
    last_vwap: Option<f64>,
    last_buy_vwap: Option<f64>,
    last_sell_vwap: Option<f64>,
    stats: BaselineStats,
}

impl BarState {
    fn new(bar_ms: i64) -> Self {
        Self {
            bar_ms,
            current: None,
            last_close: None,
            last_vwap: None,
            last_buy_vwap: None,
            last_sell_vwap: None,
            stats: BaselineStats {
                bar_ms,
                ..BaselineStats::default()
            },
        }
    }

    fn finalize(&mut self, bar: BaselineBar) -> BaselineBar {
        let bar = bar.finalize(
            self.last_close,
            self.last_vwap,
            self.last_buy_vwap,
            self.last_sell_vwap,
        );
        self.stats.closed_bars = self.stats.closed_bars.saturating_add(1);
        if bar.has_trade {
            self.stats.traded_bars = self.stats.traded_bars.saturating_add(1);
        }
        if bar.close > 0.0 {
            self.last_close = Some(bar.close);
        }
        if bar.vwap > 0.0 {
            self.last_vwap = Some(bar.vwap);
        }
        if bar.buy_vwap > 0.0 {
            self.last_buy_vwap = Some(bar.buy_vwap);
        }
        if bar.sell_vwap > 0.0 {
            self.last_sell_vwap = Some(bar.sell_vwap);
        }
        bar
    }

    fn close_current(&mut self) -> Option<BaselineBar> {
        self.current.take().map(|bar| self.finalize(bar))
    }
}

/// Local exact-time aggregations. 10s and 60s only consume finalized 5s bars.
pub struct LocalBaselineAggregator {
    base: BarState,
    ten_seconds: BarState,
    sixty_seconds: BarState,
    orderbook: OrderBook,
    book_initialized: bool,
    next_book_update_id: i64,
}

impl Default for LocalBaselineAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalBaselineAggregator {
    pub fn new() -> Self {
        Self {
            base: BarState::new(BASELINE_BAR_MS),
            ten_seconds: BarState::new(RESAMPLED_BAR_MS[0]),
            sixty_seconds: BarState::new(RESAMPLED_BAR_MS[1]),
            orderbook: OrderBook::new(),
            book_initialized: false,
            next_book_update_id: 1,
        }
    }

    /// Applies an L2 snapshot or delta. A book is only usable after a snapshot.
    ///
    /// The Tardis snapshots are finite-depth, so snapshots intentionally merge into
    /// the accumulated book rather than clearing prices absent from the payload.
    pub fn on_book(
        &mut self,
        timestamp_us: i64,
        is_snapshot: bool,
        bids: &[Level],
        asks: &[Level],
    ) -> Vec<BaselineBar> {
        let mut completed = Vec::new();
        if !self.advance_to(timestamp_us, &mut completed) {
            return completed;
        }
        if !self.book_initialized && !is_snapshot {
            return completed;
        }

        let bid_updates: Vec<(f64, f64)> = bids
            .iter()
            .map(|level| (level.price, level.amount))
            .collect();
        let ask_updates: Vec<(f64, f64)> = asks
            .iter()
            .map(|level| (level.price, level.amount))
            .collect();
        let update_id = self.next_book_update_id;
        self.next_book_update_id = self.next_book_update_id.saturating_add(1);
        if is_snapshot {
            self.orderbook
                .apply_snapshot(&bid_updates, &ask_updates, update_id, timestamp_us);
            self.book_initialized = true;
        } else {
            self.orderbook
                .apply_update(&bid_updates, &ask_updates, update_id, timestamp_us);
        }
        completed
    }

    /// Applies a trade and returns bars closed before this trade's time bucket.
    /// Empty intervals are intentionally skipped rather than forward-filled.
    pub fn on_trade(
        &mut self,
        timestamp_us: i64,
        is_buy: bool,
        price: f64,
        amount: f64,
    ) -> Vec<BaselineBar> {
        let mut completed = Vec::new();
        if !self.advance_to(timestamp_us, &mut completed) {
            self.base.stats.late_trades = self.base.stats.late_trades.saturating_add(1);
            return completed;
        }
        let timestamp_ms = timestamp_us.div_euclid(1_000);
        let target_start = align(timestamp_ms, BASELINE_BAR_MS);
        if self.base.current.is_none() {
            let mut bar = BaselineBar::new(target_start);
            bar.update_trade(is_buy, price, amount);
            self.base.current = Some(bar);
        } else if let Some(bar) = self.base.current.as_mut() {
            bar.update_trade(is_buy, price, amount);
        }
        completed
    }

    pub fn flush(&mut self) -> Vec<BaselineBar> {
        let mut completed = Vec::new();
        self.finish_base_current(&mut completed);
        self.ten_seconds.close_current();
        self.sixty_seconds.close_current();
        completed
    }

    pub fn stats(&self) -> [BaselineStats; 3] {
        [
            self.base.stats.clone(),
            self.ten_seconds.stats.clone(),
            self.sixty_seconds.stats.clone(),
        ]
    }

    fn advance_to(&mut self, timestamp_us: i64, completed: &mut Vec<BaselineBar>) -> bool {
        let timestamp_ms = timestamp_us.div_euclid(1_000);
        let target_start = align(timestamp_ms, BASELINE_BAR_MS);
        match self.base.current.as_ref().map(|bar| bar.start_ms) {
            Some(current_start) if target_start < current_start => false,
            Some(current_start) if target_start > current_start => {
                self.finish_base_current(completed);
                true
            }
            _ => true,
        }
    }

    fn finish_base_current(&mut self, completed: &mut Vec<BaselineBar>) {
        let Some(mut bar) = self.base.close_current() else {
            return;
        };
        let (depth20, is_complete) = self.depth20();
        bar.depth20 = depth20;
        if is_complete {
            self.base.stats.depth20_bars = self.base.stats.depth20_bars.saturating_add(1);
        } else {
            self.base.stats.padded_depth20_bars =
                self.base.stats.padded_depth20_bars.saturating_add(1);
        }
        self.consume_base_bar(&bar);
        completed.push(bar);
    }

    fn depth20(&self) -> (BaselineDepth20, bool) {
        let (bids, asks) = self.orderbook.get_depth(BASELINE_DEPTH_LEVELS);
        let mut depth20 = BaselineDepth20 {
            bids: [(0.0, 0.0); BASELINE_DEPTH_LEVELS],
            asks: [(0.0, 0.0); BASELINE_DEPTH_LEVELS],
        };
        for (index, level) in bids.iter().enumerate() {
            depth20.bids[index] = *level;
        }
        for (index, level) in asks.iter().enumerate() {
            depth20.asks[index] = *level;
        }
        let is_complete = self.book_initialized
            && self.orderbook.is_valid()
            && bids.len() == BASELINE_DEPTH_LEVELS
            && asks.len() == BASELINE_DEPTH_LEVELS;
        (depth20, is_complete)
    }

    fn consume_base_bar(&mut self, bar: &BaselineBar) {
        consume_sub_bar(&mut self.ten_seconds, bar, BASELINE_BAR_MS);
        consume_sub_bar(&mut self.sixty_seconds, bar, BASELINE_BAR_MS);
    }
}

fn consume_sub_bar(state: &mut BarState, source: &BaselineBar, source_bar_ms: i64) {
    let target_start = align(source.start_ms, state.bar_ms);
    match state.current.as_ref().map(|bar| bar.start_ms) {
        None => state.current = Some(BaselineBar::new(target_start)),
        Some(start) if target_start > start => {
            state.close_current();
        }
        Some(start) if target_start < start => return,
        Some(_) => {}
    }
    if state.current.is_none() {
        state.current = Some(BaselineBar::new(target_start));
    }
    if let Some(target) = state.current.as_mut() {
        target.merge(source);
    }
    if source.start_ms.saturating_add(source_bar_ms) >= target_start.saturating_add(state.bar_ms) {
        state.close_current();
    }
}

fn align(timestamp_ms: i64, bar_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(bar_ms)
}

#[cfg(test)]
mod tests {
    use super::{HourlyNotionalKll, LocalBaselineAggregator, HOUR_MS};
    use mkt_parsers::msg::mkt_msg::Level;
    use mkt_parsers::msg::trade_flow_feature_msg::{TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM};

    #[test]
    fn resamples_only_finalized_five_second_bars() {
        let mut agg = LocalBaselineAggregator::new();
        agg.on_trade(1_000, true, 100.0, 1.0); // 0s
        agg.on_trade(5_001_000, false, 101.0, 1.0); // 5s, seals 0s
        agg.on_trade(10_001_000, true, 102.0, 1.0); // 10s, seals 5s and then 10s
        agg.flush();

        let stats = agg.stats();
        assert_eq!(stats[0].bar_ms, 5_000);
        assert_eq!(stats[0].closed_bars, 3);
        assert_eq!(stats[1].bar_ms, 10_000);
        assert_eq!(stats[1].closed_bars, 2);
        assert_eq!(stats[2].bar_ms, 60_000);
        assert_eq!(stats[2].closed_bars, 1);
    }

    #[test]
    fn skips_missing_five_second_bars() {
        let mut agg = LocalBaselineAggregator::new();
        agg.on_trade(1_000, true, 100.0, 1.0);
        agg.on_trade(15_001_000, true, 110.0, 1.0);
        agg.flush();
        let stats = agg.stats();
        assert_eq!(stats[0].closed_bars, 2);
        assert_eq!(stats[1].closed_bars, 2);
    }

    #[test]
    fn attaches_latest_depth20_when_a_bar_closes() {
        let mut agg = LocalBaselineAggregator::new();
        let bids: Vec<Level> = (0..20)
            .map(|i| Level::from_values(100.0 - i as f64, 1.0 + i as f64))
            .collect();
        let asks: Vec<Level> = (0..20)
            .map(|i| Level::from_values(101.0 + i as f64, 2.0 + i as f64))
            .collect();
        agg.on_book(100, true, &bids, &asks);
        agg.on_trade(1_000, true, 100.5, 1.0);
        let closed = agg.on_book(5_001_000, false, &[], &[]);

        assert_eq!(closed.len(), 1);
        let depth = &closed[0].depth20;
        assert_eq!(depth.bids[0], (100.0, 1.0));
        assert_eq!(depth.bids[19], (81.0, 20.0));
        assert_eq!(depth.asks[0], (101.0, 2.0));
        assert_eq!(depth.asks[19], (120.0, 21.0));
        assert_eq!(agg.stats()[0].depth20_bars, 1);
    }

    #[test]
    fn encodes_depth20_bars_as_standard_trade_flow_messages() {
        let mut agg = LocalBaselineAggregator::new();
        let bids: Vec<Level> = (0..20)
            .map(|i| Level::from_values(100.0 - i as f64, 1.0 + i as f64))
            .collect();
        let asks: Vec<Level> = (0..20)
            .map(|i| Level::from_values(101.0 + i as f64, 2.0 + i as f64))
            .collect();
        agg.on_book(100, true, &bids, &asks);
        agg.on_trade(1_000, true, 100.5, 1.0);
        let closed = agg.on_book(5_001_000, false, &[], &[]);

        let bytes = closed[0]
            .to_trade_flow_feature_payload("BTCUSDT", 2)
            .expect("encode");
        let message = TradeFlowFeatureMsg::from_bytes(&bytes).expect("decode");
        assert_eq!(message.symbol, "BTCUSDT");
        assert_eq!(message.venue, 2);
        assert_eq!(message.ts, 0);
        assert_eq!(message.values.len(), TRADE_FLOW_FEATURE_DIM + 80);
        assert_eq!(message.values[0], 100.5);
        assert_eq!(message.values[TRADE_FLOW_FEATURE_DIM], 100.0);
        assert_eq!(message.values[TRADE_FLOW_FEATURE_DIM + 1], 1.0);
        assert_eq!(message.values[TRADE_FLOW_FEATURE_DIM + 78], 120.0);
        assert_eq!(message.values[TRADE_FLOW_FEATURE_DIM + 79], 21.0);
    }

    #[test]
    fn encodes_partial_or_missing_books_with_zero_padded_depth() {
        let mut agg = LocalBaselineAggregator::new();
        let bids: Vec<Level> = (0..2)
            .map(|i| Level::from_values(100.0 - i as f64, 1.0 + i as f64))
            .collect();
        let asks = [Level::from_values(101.0, 3.0)];
        agg.on_book(100, true, &bids, &asks);
        agg.on_trade(1_000, true, 100.5, 1.0);
        let closed = agg.on_trade(5_001_000, false, 100.6, 1.0);

        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].depth20.bids[0], (100.0, 1.0));
        assert_eq!(closed[0].depth20.bids[1], (99.0, 2.0));
        assert_eq!(closed[0].depth20.bids[2], (0.0, 0.0));
        assert_eq!(closed[0].depth20.asks[0], (101.0, 3.0));
        assert_eq!(closed[0].depth20.asks[1], (0.0, 0.0));
        assert_eq!(agg.stats()[0].padded_depth20_bars, 1);

        let message = TradeFlowFeatureMsg::from_bytes(
            &closed[0]
                .to_trade_flow_feature_payload("BTCUSDT", 2)
                .expect("encode"),
        )
        .expect("decode");
        assert_eq!(message.values[TRADE_FLOW_FEATURE_DIM + 4], 0.0);
        assert_eq!(message.values[TRADE_FLOW_FEATURE_DIM + 42], 0.0);

        let mut without_book = LocalBaselineAggregator::new();
        without_book.on_trade(1_000, true, 100.0, 1.0);
        let closed = without_book.on_trade(5_001_000, true, 101.0, 1.0);
        let message = TradeFlowFeatureMsg::from_bytes(
            &closed[0]
                .to_trade_flow_feature_payload("BTCUSDT", 2)
                .expect("encode without a snapshot"),
        )
        .expect("decode without a snapshot");
        assert_eq!(message.values[TRADE_FLOW_FEATURE_DIM], 0.0);
        assert_eq!(message.values[TRADE_FLOW_FEATURE_DIM + 79], 0.0);
    }

    #[test]
    fn freezes_kll_on_the_next_utc_hour() {
        let mut kll = HourlyNotionalKll::new();
        assert!(kll.on_trade(HOUR_MS * 1_000 - 1, 100.0, 2.0).is_none());
        let closed = kll
            .on_trade(HOUR_MS * 1_000, 100.0, 3.0)
            .expect("previous hour should freeze");
        assert_eq!(closed.hour_start_ms, 0);
        assert_eq!(closed.sketch.sample_count, 1);
        let trailing = kll.flush().expect("trailing hour should flush");
        assert_eq!(trailing.hour_start_ms, HOUR_MS);
        assert_eq!(trailing.sketch.sample_count, 1);
    }
}
