use super::{config::MarketData, state::stale};
use anyhow::{ensure, Context, Result};
use cn_futures_pbs::msg::{MarketDataMsg, SOURCE_RESET, UNKNOWN_QUANTITY};
use iceoryx2::{port::subscriber::Subscriber, prelude::*, service::ipc};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub struct Subscription {
    _node: Node<ipc::Service>,
    subscriber: Subscriber<ipc::Service, MarketDataMsg, ()>,
}
impl Subscription {
    pub fn open(root: &str, venue: &str) -> Result<Self> {
        let name = cn_futures_pbs::ipc::service_name(root, venue)?;
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        // Open only: Viz must never create a feed or impersonate a publisher.
        let service = node
            .service_builder(&ServiceName::new(&name)?)
            .publish_subscribe::<MarketDataMsg>()
            .open()
            .context("open native market IPC")?;
        let subscriber = service.subscriber_builder().buffer_size(2048).create()?;
        Ok(Self {
            _node: node,
            subscriber,
        })
    }
    pub fn drain(&self, market: &mut Market, venue: &str) -> Result<()> {
        for _ in 0..256 {
            let Some(sample) = self.subscriber.receive()? else {
                break;
            };
            market.apply(venue, *sample.payload())?;
        }
        Ok(())
    }
}
#[derive(Default)]
struct Channel {
    attached: bool,
    error: Option<String>,
    epoch: u64,
    generation: u64,
    sequence: u64,
    last_message_us: Option<u64>,
    last_quote_us: Option<u64>,
    resets: u64,
    sequence_gaps: u64,
}
pub struct Market {
    pub config: MarketData,
    channels: BTreeMap<String, Channel>,
    quotes: BTreeMap<(String, String), MarketDataMsg>,
    dropped_symbols: u64,
}
impl Market {
    pub fn new(config: MarketData) -> Self {
        let channels = config
            .venues
            .iter()
            .map(|v| (v.clone(), Channel::default()))
            .collect();
        Self {
            config,
            channels,
            quotes: BTreeMap::new(),
            dropped_symbols: 0,
        }
    }
    pub fn attached(&mut self, venue: &str) {
        if let Some(c) = self.channels.get_mut(venue) {
            c.attached = true;
            c.error = None;
        }
    }
    pub fn unavailable(&mut self, venue: &str, error: &str) {
        if let Some(c) = self.channels.get_mut(venue) {
            c.attached = false;
            c.error = Some(error.into());
            c.last_quote_us = None;
        }
        self.quotes.retain(|(v, _), _| v != venue);
    }
    pub fn apply(&mut self, venue: &str, msg: MarketDataMsg) -> Result<()> {
        ensure!(
            msg.venue() == venue.as_bytes()
                && msg.publisher_epoch > 0
                && msg.sequence > 0
                && msg.received_us > 0,
            "invalid market envelope"
        );
        let c = self
            .channels
            .get_mut(venue)
            .context("unconfigured market venue")?;
        let new_generation =
            c.epoch != msg.publisher_epoch || c.generation != msg.source_generation;
        if !new_generation {
            ensure!(
                msg.sequence >= c.sequence,
                "market sequence regressed within publisher epoch"
            );
            if msg.sequence == c.sequence {
                return Ok(());
            }
        }
        let gap = !new_generation && c.sequence > 0 && msg.sequence != c.sequence + 1;
        if gap {
            c.sequence_gaps += 1;
        }
        if new_generation || gap || msg.flags & SOURCE_RESET != 0 {
            self.quotes.retain(|(v, _), _| v != venue);
            c.last_quote_us = None;
        }
        c.epoch = msg.publisher_epoch;
        c.generation = msg.source_generation;
        c.sequence = msg.sequence;
        c.last_message_us = Some(msg.received_us);
        if msg.flags & SOURCE_RESET != 0 {
            c.resets += 1;
            return Ok(());
        }
        let symbol = std::str::from_utf8(msg.symbol()).context("invalid quote symbol")?;
        ensure!(
            !symbol.is_empty()
                && symbol.len() < 64
                && symbol.bytes().all(|b| b.is_ascii_alphanumeric()),
            "invalid native quote contract"
        );
        c.last_quote_us = Some(msg.received_us);
        if !self.config.symbols.is_empty() && !self.config.symbols.iter().any(|s| s == symbol) {
            return Ok(());
        }
        let key = (venue.to_string(), symbol.to_string());
        if self.quotes.len() >= self.config.max_symbols && !self.quotes.contains_key(&key) {
            self.dropped_symbols += 1;
            return Ok(());
        }
        self.quotes.insert(key, msg);
        Ok(())
    }
    pub fn snapshot(&self, now: u64) -> Value {
        let channels: Vec<_> = self.channels.iter().map(|(venue, c)| {
            let data_stale = stale(now, c.last_quote_us, self.config.stale_after_ms);
            json!({"venue":venue, "attached":c.attached, "error":c.error,
                "status": if !c.attached { "unavailable" } else if c.last_quote_us.is_none() { "waiting" } else if data_stale { "stale" } else { "observing" },
                "data_stale":data_stale, "last_message_us":c.last_message_us, "last_quote_us":c.last_quote_us,
                "publisher_epoch":c.epoch.to_string(), "source_generation":c.generation.to_string(),
                "sequence":c.sequence.to_string(), "reset_count":c.resets, "sequence_gap_count":c.sequence_gaps})
        }).collect();
        let quotes: Vec<_> = self.quotes.iter().map(|((venue, symbol), m)| {
            let levels = |levels: &[cn_futures_pbs::msg::Level; 5]| -> Vec<Value> {
                levels.iter().map(|l| json!({"price":finite(l.price), "lots":quantity(l.quantity)})).collect()
            };
            json!({"venue":venue, "symbol":symbol, "received_us":m.received_us,
                "exchange_ts_us":(m.exchange_ts_us != 0).then_some(m.exchange_ts_us),
                "trading_day":(m.trading_day != 0).then_some(m.trading_day), "action_day":(m.action_day != 0).then_some(m.action_day),
                "publisher_epoch":m.publisher_epoch.to_string(), "source_generation":m.source_generation.to_string(),
                "sequence":m.sequence.to_string(), "source_sequence":quantity(m.source_sequence),
                "last_price":finite(m.last_price), "volume_lots":quantity(m.volume), "turnover":finite(m.turnover),
                "open_interest":finite(m.open_interest), "bid":levels(&m.bid), "ask":levels(&m.ask),
                "data_stale":stale(now, Some(m.received_us), self.config.stale_after_ms)})
        }).collect();
        json!({"id":self.config.id, "label":self.config.label, "environment":self.config.environment,
            "service_root":self.config.service_root, "channels":channels, "quotes":quotes,
            "max_symbols":self.config.max_symbols, "dropped_symbol_messages":self.dropped_symbols})
    }
}
fn finite(x: f64) -> Option<f64> {
    x.is_finite().then_some(x)
}
fn quantity(x: u64) -> Option<String> {
    (x != UNKNOWN_QUANTITY).then(|| x.to_string())
}
