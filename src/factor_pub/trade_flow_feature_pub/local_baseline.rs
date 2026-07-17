//! Exact event-time trade-flow baseline aggregation for local replays.

use rolling_common::kll_quantile::{FrozenKllSketch, StreamingKllSketch};

pub const BASELINE_BAR_MS: i64 = 5_000;
pub const RESAMPLED_BAR_MS: [i64; 2] = [10_000, 60_000];
pub const HOUR_MS: i64 = 3_600_000;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BaselineStats {
    pub bar_ms: i64,
    pub closed_bars: u64,
    pub traded_bars: u64,
    pub ffill_bars: u64,
    pub late_trades: u64,
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
        } else {
            self.stats.ffill_bars = self.stats.ffill_bars.saturating_add(1);
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

    fn close_current<F>(&mut self, emit: &mut F)
    where
        F: FnMut(BaselineBar),
    {
        if let Some(bar) = self.current.take() {
            emit(self.finalize(bar));
        }
    }
}

/// Local exact-time aggregations. 10s and 60s only consume finalized 5s bars.
pub struct LocalBaselineAggregator {
    base: BarState,
    ten_seconds: BarState,
    sixty_seconds: BarState,
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
        }
    }

    pub fn on_trade(&mut self, timestamp_us: i64, is_buy: bool, price: f64, amount: f64) {
        let timestamp_ms = timestamp_us.div_euclid(1_000);
        let target_start = align(timestamp_ms, BASELINE_BAR_MS);
        let Some(current_start) = self.base.current.as_ref().map(|bar| bar.start_ms) else {
            let mut bar = BaselineBar::new(target_start);
            bar.update_trade(is_buy, price, amount);
            self.base.current = Some(bar);
            return;
        };
        if target_start < current_start {
            self.base.stats.late_trades = self.base.stats.late_trades.saturating_add(1);
            return;
        }
        if target_start == current_start {
            if let Some(bar) = self.base.current.as_mut() {
                bar.update_trade(is_buy, price, amount);
            }
            return;
        }

        let mut completed = Vec::new();
        self.base.close_current(&mut |bar| completed.push(bar));
        let mut empty_start = current_start.saturating_add(BASELINE_BAR_MS);
        while empty_start < target_start {
            completed.push(self.base.finalize(BaselineBar::new(empty_start)));
            empty_start = empty_start.saturating_add(BASELINE_BAR_MS);
        }
        let mut bar = BaselineBar::new(target_start);
        bar.update_trade(is_buy, price, amount);
        self.base.current = Some(bar);
        for bar in completed {
            self.consume_base_bar(bar);
        }
    }

    pub fn flush(&mut self) {
        let mut completed = Vec::new();
        self.base.close_current(&mut |bar| completed.push(bar));
        for bar in completed {
            self.consume_base_bar(bar);
        }
        self.ten_seconds.close_current(&mut |_| {});
        self.sixty_seconds.close_current(&mut |_| {});
    }

    pub fn stats(&self) -> [BaselineStats; 3] {
        [
            self.base.stats.clone(),
            self.ten_seconds.stats.clone(),
            self.sixty_seconds.stats.clone(),
        ]
    }

    fn consume_base_bar(&mut self, bar: BaselineBar) {
        consume_sub_bar(&mut self.ten_seconds, &bar, BASELINE_BAR_MS);
        consume_sub_bar(&mut self.sixty_seconds, &bar, BASELINE_BAR_MS);
    }
}

fn consume_sub_bar(state: &mut BarState, source: &BaselineBar, source_bar_ms: i64) {
    let target_start = align(source.start_ms, state.bar_ms);
    match state.current.as_ref().map(|bar| bar.start_ms) {
        None => state.current = Some(BaselineBar::new(target_start)),
        Some(start) if target_start > start => state.close_current(&mut |_| {}),
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
        state.close_current(&mut |_| {});
    }
}

fn align(timestamp_ms: i64, bar_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(bar_ms)
}

#[cfg(test)]
mod tests {
    use super::{HourlyNotionalKll, LocalBaselineAggregator, HOUR_MS};

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
    fn fills_missing_five_second_bars_before_resampling() {
        let mut agg = LocalBaselineAggregator::new();
        agg.on_trade(1_000, true, 100.0, 1.0);
        agg.on_trade(15_001_000, true, 110.0, 1.0);
        agg.flush();
        let stats = agg.stats();
        assert_eq!(stats[0].closed_bars, 4);
        assert_eq!(stats[0].ffill_bars, 2);
        assert_eq!(stats[1].closed_bars, 2);
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
