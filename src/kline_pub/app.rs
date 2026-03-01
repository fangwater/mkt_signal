//! Kline Publisher 应用主模块
//!
//! 订阅 mkt_pub 的 trade 数据，合成固定周期 K 线并发布

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::info;
use prettytable::format::{FormatBuilder, LinePosition, LineSeparator};
use prettytable::{Cell, Row, Table};
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::cfg::KlinePubConfig;
use super::publisher::KlineMsgPublisher;
use crate::common::mkt_msg::{KlineMsg, MktMsgType};

const TRADE_MAX_BYTES: usize = 128;
const SIGNAL_MAX_BYTES: usize = 64;
const IDLE_SLEEP_MICROS: u64 = 100;
const TIMER_CHECK_INTERVAL_US: u64 = 500;
const TRACKED_BASES: [&str; 3] = ["BTC", "ETH", "SOL"];

struct TradeTick {
    symbol: String,
    timestamp_ms: i64,
    price: f64,
    amount: f64,
}

#[derive(Debug, Clone)]
struct KlineBar {
    start_us: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    trade_count: u64,
    first_trade_us: i64,
    last_trade_us: i64,
}

impl KlineBar {
    fn new(start_us: i64, trade_us: i64, price: f64, amount: f64) -> Self {
        Self {
            start_us,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: amount,
            trade_count: 1,
            first_trade_us: trade_us,
            last_trade_us: trade_us,
        }
    }

    fn empty(start_us: i64, price: f64) -> Self {
        Self {
            start_us,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: 0.0,
            trade_count: 0,
            first_trade_us: start_us,
            last_trade_us: start_us,
        }
    }

    fn update(&mut self, trade_us: i64, price: f64, amount: f64) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.volume += amount;
        self.trade_count += 1;

        if trade_us < self.first_trade_us {
            self.first_trade_us = trade_us;
            self.open = price;
        }
        if trade_us >= self.last_trade_us {
            self.last_trade_us = trade_us;
            self.close = price;
        }
    }
}

struct SymbolState {
    bar: Option<KlineBar>,
    last_close: Option<f64>,
    last_bar_start_us: Option<i64>,
    last_published: Option<KlineBar>,
}

impl SymbolState {
    fn new() -> Self {
        Self {
            bar: None,
            last_close: None,
            last_bar_start_us: None,
            last_published: None,
        }
    }

    fn apply_trade(
        &mut self,
        trade_us: i64,
        price: f64,
        amount: f64,
        trade_bar_start_us: i64,
        period_us: i64,
    ) -> (Vec<KlineBar>, bool) {
        let mut closed = Vec::new();
        let mut late_trade = false;

        match self.bar.as_mut() {
            None => {
                if let Some(last_start) = self.last_bar_start_us {
                    if trade_bar_start_us <= last_start {
                        return (closed, true);
                    }
                }
                self.fill_empty_until(trade_bar_start_us, period_us, &mut closed);
                self.bar = Some(KlineBar::new(trade_bar_start_us, trade_us, price, amount));
            }
            Some(bar) => {
                if trade_bar_start_us == bar.start_us {
                    bar.update(trade_us, price, amount);
                } else if trade_bar_start_us > bar.start_us {
                    let closed_bar = self.bar.take().map(|b| self.finalize_bar(b));
                    if let Some(bar) = closed_bar {
                        closed.push(bar);
                    }
                    self.fill_empty_until(trade_bar_start_us, period_us, &mut closed);
                    self.bar = Some(KlineBar::new(trade_bar_start_us, trade_us, price, amount));
                } else {
                    // 延迟到达的旧交易（属于已封bar），直接忽略
                    late_trade = true;
                }
            }
        }

        (closed, late_trade)
    }

    fn close_due_bars(&mut self, now_us: i64, period_us: i64, delay_us: i64) -> Vec<KlineBar> {
        let mut closed = Vec::new();

        if let Some(bar) = self.bar.take() {
            let close_at = bar.start_us + period_us + delay_us;
            if now_us >= close_at {
                closed.push(self.finalize_bar(bar));
            } else {
                self.bar = Some(bar);
            }
        }

        if self.bar.is_none() {
            let target_end_us = now_us - delay_us;
            let mut next_start = match self.last_bar_start_us {
                Some(start) => start + period_us,
                None => return closed,
            };

            while next_start + period_us <= target_end_us {
                let Some(price) = self.last_close else { break };
                let empty = KlineBar::empty(next_start, price);
                closed.push(self.finalize_bar(empty));
                next_start += period_us;
            }
        }

        closed
    }

    fn finalize_bar(&mut self, bar: KlineBar) -> KlineBar {
        self.last_close = Some(bar.close);
        self.last_bar_start_us = Some(bar.start_us);
        self.last_published = Some(bar.clone());
        bar
    }

    fn fill_empty_until(&mut self, target_start_us: i64, period_us: i64, out: &mut Vec<KlineBar>) {
        let Some(last_start) = self.last_bar_start_us else {
            return;
        };
        let Some(price) = self.last_close else {
            return;
        };

        let mut next_start = last_start + period_us;
        while next_start < target_start_us {
            let empty = KlineBar::empty(next_start, price);
            out.push(self.finalize_bar(empty));
            next_start += period_us;
        }
    }

    fn snapshot(&self) -> Option<KlineSnapshot> {
        if let Some(bar) = &self.bar {
            return Some(KlineSnapshot {
                bar: bar.clone(),
                status: "open",
            });
        }
        if let Some(bar) = &self.last_published {
            return Some(KlineSnapshot {
                bar: bar.clone(),
                status: "closed",
            });
        }
        None
    }
}

struct KlineSnapshot {
    bar: KlineBar,
    status: &'static str,
}

/// Kline Publisher 应用
pub struct KlinePubApp {
    venue_slug: String,
    publisher: KlineMsgPublisher,
    subscriber: Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()>,
    signal_subscriber: Subscriber<ipc::Service, [u8; SIGNAL_MAX_BYTES], ()>,
    symbols: HashMap<String, SymbolState>,
    tracked_symbols: HashMap<String, String>,
    period_us: i64,
    delay_us: i64,
    update_count: u64,
    publish_count: u64,
    dropped_count: u64,
    late_trade_count: u64,
    signal_count: u64,
    timer_check_interval: Duration,
    last_timer_check: Instant,
    table_log_interval: Duration,
    last_table_log: Instant,
}

impl KlinePubApp {
    /// 创建应用实例
    /// venue_slug: 例如 "binance-futures", "okex-margin"
    pub fn new(config: KlinePubConfig, venue_slug: &str) -> Result<Self> {
        if config.kline_timing.period_ms == 0 {
            anyhow::bail!("kline period_ms must be > 0");
        }
        let period_us = (config.kline_timing.period_ms as i64) * 1_000;
        let delay_us = config.kline_timing.close_delay_us as i64;
        let channel_label = build_kline_channel_label(config.kline_timing.period_ms);

        let publisher = KlineMsgPublisher::new(venue_slug, &channel_label)?;
        let subscriber = Self::create_trade_subscriber(publisher.node(), venue_slug)?;
        let signal_subscriber = Self::create_signal_subscriber(publisher.node(), venue_slug)?;

        info!(
            "KlinePubApp created for {}: period={}ms, delay={}us, channel={}",
            venue_slug,
            config.kline_timing.period_ms,
            config.kline_timing.close_delay_us,
            channel_label
        );

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            publisher,
            subscriber,
            signal_subscriber,
            symbols: HashMap::new(),
            tracked_symbols: HashMap::new(),
            period_us,
            delay_us,
            update_count: 0,
            publish_count: 0,
            dropped_count: 0,
            late_trade_count: 0,
            signal_count: 0,
            timer_check_interval: Duration::from_micros(TIMER_CHECK_INTERVAL_US),
            last_timer_check: Instant::now(),
            table_log_interval: Duration::from_millis(config.kline_timing.period_ms),
            last_table_log: Instant::now(),
        })
    }

    fn create_trade_subscriber(
        node: &Node<ipc::Service>,
        venue: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()>> {
        let service_name = format!("dat_pbs/{}/trade", venue);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to trade channel: {}", service_name);
        Ok(subscriber)
    }

    fn create_signal_subscriber(
        node: &Node<ipc::Service>,
        venue: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; SIGNAL_MAX_BYTES], ()>> {
        let service_name = format!("dat_pbs/{}/signal", venue);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; SIGNAL_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to signal channel: {}", service_name);
        Ok(subscriber)
    }

    /// 主循环
    pub fn run(&mut self) -> Result<()> {
        info!("KlinePubApp[{}] starting main loop", self.venue_slug);
        let mut last_stats_time = Instant::now();
        let stats_interval = Duration::from_secs(60);

        loop {
            let mut has_message = false;
            while let Some(sample) = self.subscriber.receive()? {
                has_message = true;
                let data = sample.payload().to_vec();
                self.process_message(&data);
                self.maybe_close_due_bars();
            }

            while let Some(sample) = self.signal_subscriber.receive()? {
                has_message = true;
                let data = sample.payload().to_vec();
                self.process_signal(&data);
            }

            self.maybe_log_table();

            if !has_message {
                self.maybe_close_due_bars();
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }

            if last_stats_time.elapsed() >= stats_interval {
                self.log_stats();
                last_stats_time = Instant::now();
            }
        }
    }

    fn process_message(&mut self, data: &[u8]) {
        let Some(trade) = parse_trade(data) else {
            return;
        };

        if trade.price <= 0.0 || trade.amount <= 0.0 || trade.timestamp_ms <= 0 {
            return;
        }

        self.update_count += 1;
        self.maybe_track_symbol(&trade.symbol);

        let trade_us = trade.timestamp_ms * 1_000;
        let trade_bar_start_us = align_to_period(trade_us, self.period_us);
        let symbol = trade.symbol.clone();

        let (closed_bars, late_trade) = {
            let state = self
                .symbols
                .entry(trade.symbol)
                .or_insert_with(SymbolState::new);
            state.apply_trade(
                trade_us,
                trade.price,
                trade.amount,
                trade_bar_start_us,
                self.period_us,
            )
        };

        if late_trade {
            self.late_trade_count += 1;
        }

        for bar in closed_bars {
            self.publish_bar(&symbol, bar);
        }
    }

    fn process_signal(&mut self, data: &[u8]) {
        if parse_signal(data).is_some() {
            self.signal_count += 1;
        }
    }

    fn maybe_close_due_bars(&mut self) {
        if self.last_timer_check.elapsed() < self.timer_check_interval {
            return;
        }
        self.last_timer_check = Instant::now();
        let now_us = now_micros();
        self.close_due_bars(now_us);
    }

    fn close_due_bars(&mut self, now_us: i64) {
        let mut to_publish: Vec<(String, KlineBar)> = Vec::new();
        for (symbol, state) in self.symbols.iter_mut() {
            let bars = state.close_due_bars(now_us, self.period_us, self.delay_us);
            for bar in bars {
                to_publish.push((symbol.clone(), bar));
            }
        }

        for (symbol, bar) in to_publish {
            self.publish_bar(&symbol, bar);
        }
    }

    fn publish_bar(&mut self, symbol: &str, bar: KlineBar) {
        let timestamp_ms = bar.start_us / 1_000;
        let msg = KlineMsg::create_with_count(
            symbol.to_string(),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            timestamp_ms,
            bar.trade_count,
        );

        if self.publisher.publish(&msg) {
            self.publish_count += 1;
        } else {
            self.dropped_count += 1;
        }
    }

    fn log_stats(&mut self) {
        info!(
            "KlinePubApp[{}] stats: updates={}, published={}, dropped={}, late_trade={}, signals={}",
            self.venue_slug,
            self.update_count,
            self.publish_count,
            self.dropped_count,
            self.late_trade_count,
            self.signal_count
        );
        self.update_count = 0;
        self.publish_count = 0;
        self.dropped_count = 0;
        self.late_trade_count = 0;
        self.signal_count = 0;
        self.publisher.log_stats();
    }

    fn maybe_track_symbol(&mut self, symbol: &str) {
        let upper = symbol.to_ascii_uppercase();
        for base in TRACKED_BASES {
            if upper.starts_with(base) {
                self.tracked_symbols
                    .entry(base.to_string())
                    .or_insert_with(|| symbol.to_string());
            }
        }
    }

    fn maybe_log_table(&mut self) {
        if self.last_table_log.elapsed() < self.table_log_interval {
            return;
        }
        self.last_table_log = Instant::now();
        let ts_ms = now_micros() / 1_000;
        self.log_kline_table(ts_ms);
    }

    fn log_kline_table(&self, snapshot_ts_ms: i64) {
        let mut table = Table::new();
        let format = FormatBuilder::new()
            .padding(1, 1)
            .column_separator('|')
            .borders('|')
            .separator(LinePosition::Top, LineSeparator::new('-', '+', '+', '+'))
            .separator(LinePosition::Title, LineSeparator::new('-', '+', '+', '+'))
            .separator(LinePosition::Bottom, LineSeparator::new('-', '+', '+', '+'))
            .build();
        table.set_format(format);

        table.set_titles(Row::from(vec![
            Cell::new("Symbol").style_spec("c"),
            Cell::new("Start").style_spec("c"),
            Cell::new("Open").style_spec("c"),
            Cell::new("High").style_spec("c"),
            Cell::new("Low").style_spec("c"),
            Cell::new("Close").style_spec("c"),
            Cell::new("Volume").style_spec("c"),
            Cell::new("Count").style_spec("c"),
            Cell::new("State").style_spec("c"),
        ]));

        for base in TRACKED_BASES {
            let symbol = self
                .tracked_symbols
                .get(base)
                .cloned()
                .unwrap_or_else(|| base.to_string());

            let snapshot = self.symbols.get(&symbol).and_then(|state| state.snapshot());

            if let Some(snapshot) = snapshot {
                let bar = snapshot.bar;
                table.add_row(Row::from(vec![
                    Cell::new(&symbol),
                    Cell::new(&format!("{}", bar.start_us / 1_000)),
                    Cell::new(&format!("{:.8}", bar.open)),
                    Cell::new(&format!("{:.8}", bar.high)),
                    Cell::new(&format!("{:.8}", bar.low)),
                    Cell::new(&format!("{:.8}", bar.close)),
                    Cell::new(&format!("{:.8}", bar.volume)),
                    Cell::new(&bar.trade_count.to_string()),
                    Cell::new(snapshot.status),
                ]));
            } else {
                table.add_row(Row::from(vec![
                    Cell::new(&symbol),
                    Cell::new("-"),
                    Cell::new("-"),
                    Cell::new("-"),
                    Cell::new("-"),
                    Cell::new("-"),
                    Cell::new("-"),
                    Cell::new("-"),
                    Cell::new("-"),
                ]));
            }
        }

        info!("Kline snapshot ts_ms: {}", snapshot_ts_ms);
        let _ = table.print(&mut std::io::stderr());
    }
}

fn parse_trade(data: &[u8]) -> Option<TradeTick> {
    if data.len() < 8 {
        return None;
    }

    let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if msg_type != MktMsgType::TradeInfo as u32 {
        return None;
    }

    let symbol_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let min_len = 8 + symbol_len + 8 + 8 + 8 + 8;
    if data.len() < min_len {
        return None;
    }

    let symbol = std::str::from_utf8(&data[8..8 + symbol_len])
        .ok()?
        .to_string();

    let mut offset = 8 + symbol_len;
    let _trade_id = i64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);
    offset += 8;

    let timestamp_ms = i64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);
    offset += 8;

    // side(1) + padding(7)
    offset += 8;

    let price = f64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);
    offset += 8;

    let amount = f64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);

    Some(TradeTick {
        symbol,
        timestamp_ms,
        price,
        amount,
    })
}

fn parse_signal(data: &[u8]) -> Option<i64> {
    if data.len() < 16 {
        return None;
    }
    let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if msg_type != MktMsgType::TimeSignal as u32 {
        return None;
    }
    let timestamp = i64::from_le_bytes([
        data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
    ]);
    Some(timestamp)
}

fn align_to_period(ts_us: i64, period_us: i64) -> i64 {
    ts_us - (ts_us % period_us)
}

fn build_kline_channel_label(period_ms: u64) -> String {
    if period_ms % 1_000 == 0 {
        format!("kline{}s", period_ms / 1_000)
    } else {
        format!("kline{}ms", period_ms)
    }
}

fn now_micros() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    now.as_micros() as i64
}
