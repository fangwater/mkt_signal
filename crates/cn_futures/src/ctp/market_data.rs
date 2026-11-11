//! Continuous official CTP MdApi with explicit subscriptions and reconnect recovery.
use crate::model::{is_future_symbol, Venue};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub sdk_library: PathBuf,
    pub flow_directory: PathBuf,
    pub journal: PathBuf,
    pub fronts: Vec<String>,
    pub broker_id: String,
    pub production_mode: bool,
    pub user_id_env: String,
    pub password_env: String,
    pub event_capacity: usize,
    pub response_timeout_ms: u64,
    pub reconnect_backoff_ms: u64,
    pub symbols: Vec<Subscription>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub venue: Venue,
    pub symbol: String,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let config: Self =
            toml::from_str(&std::fs::read_to_string(path).context("read CTP MD config")?)
                .map_err(|_| anyhow::anyhow!("invalid CTP MD config schema"))?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.sdk_library.is_absolute()
                && self.flow_directory.is_absolute()
                && self.journal.is_absolute(),
            "absolute MD SDK/flow/journal paths required"
        );
        ensure!(
            !self.fronts.is_empty()
                && self.fronts.len() <= 32
                && self
                    .fronts
                    .iter()
                    .all(|f| f.starts_with("tcp://") && f.len() > 9 && !f.contains(['\0', ','])),
            "explicit MD TCP fronts required"
        );
        ensure!(
            !self.broker_id.is_empty()
                && self.broker_id.len() <= 10
                && self.broker_id.is_ascii()
                && !self.broker_id.contains('\0'),
            "invalid MD BrokerID"
        );
        ensure!(
            (64..=1_048_576).contains(&self.event_capacity)
                && self.response_timeout_ms >= 5000
                && self.reconnect_backoff_ms >= 1000,
            "invalid MD queue/timeout/backoff"
        );
        for name in [&self.user_id_env, &self.password_env] {
            ensure!(
                !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "invalid MD credential variable name"
            );
        }
        let mut symbols = HashSet::new();
        ensure!(
            !self.symbols.is_empty() && self.symbols.len() <= 1000,
            "require 1..1000 MD subscriptions"
        );
        for s in &self.symbols {
            ensure!(
                is_future_symbol(s.venue, &s.symbol) && symbols.insert(&s.symbol),
                "invalid or ambiguous MD subscription {}",
                s.symbol
            );
        }
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawEvent {
    pub kind: i32,
    pub request: i32,
    pub error: i32,
    pub last: i32,
    pub present: i32,
    pub volume: i32,
    pub millisec: i32,
    pub bid_volume: [i32; 5],
    pub ask_volume: [i32; 5],
    pub last_price: f64,
    pub turnover: f64,
    pub open_interest: f64,
    pub upper_limit: f64,
    pub lower_limit: f64,
    pub bid: [f64; 5],
    pub ask: [f64; 5],
    pub symbol: [u8; 81],
    pub exchange: [u8; 9],
    pub trading_day: [u8; 9],
    pub action_day: [u8; 9],
    pub update_time: [u8; 9],
    pub message: [u8; 256],
}
impl Default for RawEvent {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct Tick {
    pub received_us: u64,
    pub connection_generation: u64,
    pub venue: Venue,
    pub symbol: String,
    pub exchange_reported: String,
    pub trading_day: String,
    pub action_day: String,
    pub update_time: String,
    pub update_millisec: i32,
    pub last_price: Option<f64>,
    pub turnover: Option<f64>,
    pub open_interest: Option<f64>,
    pub upper_limit: Option<f64>,
    pub lower_limit: Option<f64>,
    pub volume: i32,
    pub bid: [Option<f64>; 5],
    pub ask: [Option<f64>; 5],
    pub bid_volume: [i32; 5],
    pub ask_volume: [i32; 5],
}
fn value(price: f64) -> Option<f64> {
    (price.is_finite() && price.abs() < 1e100).then_some(price)
}

pub struct Session {
    allowed: HashMap<String, Venue>,
    subscribed: HashSet<String>,
    pub connected: bool,
    pub logged_in: bool,
    pub generation: u64,
    login_request: Option<i32>,
}
impl Session {
    pub fn new(config: &Config) -> Self {
        Self {
            allowed: config
                .symbols
                .iter()
                .map(|s| (s.symbol.clone(), s.venue))
                .collect(),
            subscribed: HashSet::new(),
            connected: false,
            logged_in: false,
            generation: 0,
            login_request: None,
        }
    }
    pub fn connected(&mut self, request: i32) -> Result<()> {
        self.generation = self
            .generation
            .checked_add(1)
            .context("MD generation exhausted")?;
        self.connected = true;
        self.logged_in = false;
        self.subscribed.clear();
        self.login_request = Some(request);
        Ok(())
    }
    pub fn disconnect(&mut self) {
        self.connected = false;
        self.logged_in = false;
        self.subscribed.clear();
        self.login_request = None;
    }
    pub fn login(&mut self, event: &RawEvent) -> Result<bool> {
        if self.login_request != Some(event.request) {
            return Ok(false);
        }
        ensure!(
            event.error == 0 && event.present != 0,
            "MD login rejected: {}",
            event.error
        );
        if event.last == 0 {
            return Ok(false);
        }
        let day = super::text(&event.trading_day)?;
        ensure!(
            day.len() == 8 && day.bytes().all(|b| b.is_ascii_digit()),
            "invalid MD trading day"
        );
        self.logged_in = true;
        self.login_request = None;
        Ok(true)
    }
    pub fn subscribed(&mut self, event: &RawEvent) -> Result<()> {
        ensure!(
            self.logged_in && event.error == 0 && event.present != 0,
            "MD subscription rejected: {}",
            event.error
        );
        let symbol = super::text(&event.symbol)?;
        ensure!(
            self.allowed.contains_key(&symbol),
            "unexpected MD subscription response"
        );
        self.subscribed.insert(symbol);
        Ok(())
    }
    pub fn ready(&self) -> bool {
        self.connected && self.logged_in && self.subscribed.len() == self.allowed.len()
    }
    pub fn tick(&self, e: &RawEvent, received_us: u64) -> Result<Tick> {
        ensure!(
            self.connected && self.logged_in && e.present != 0,
            "MD tick before login"
        );
        let symbol = super::text(&e.symbol)?;
        let venue = *self
            .allowed
            .get(&symbol)
            .context("MD tick outside subscription allowlist")?;
        let exchange = super::text(&e.exchange)?;
        ensure!(
            exchange.is_empty() || exchange == venue.as_str(),
            "MD tick exchange differs from configured symbol"
        );
        ensure!(
            (0..1000).contains(&e.millisec)
                && e.volume >= 0
                && e.bid_volume.iter().chain(&e.ask_volume).all(|v| *v >= 0),
            "invalid MD timestamp or volume"
        );
        Ok(Tick {
            received_us,
            connection_generation: self.generation,
            venue,
            symbol,
            exchange_reported: exchange,
            trading_day: super::text(&e.trading_day)?,
            action_day: super::text(&e.action_day)?,
            update_time: super::text(&e.update_time)?,
            update_millisec: e.millisec,
            last_price: value(e.last_price),
            turnover: value(e.turnover),
            open_interest: value(e.open_interest),
            upper_limit: value(e.upper_limit),
            lower_limit: value(e.lower_limit),
            volume: e.volume,
            bid: e.bid.map(value),
            ask: e.ask.map(value),
            bid_volume: e.bid_volume,
            ask_volume: e.ask_volume,
        })
    }
}

#[cfg(feature = "ctp-sdk")]
mod native;
#[cfg(feature = "ctp-sdk")]
pub use native::{run, version};

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> Config {
        toml::from_str(include_str!(
            "../../../../config/ctp_market_data.example.toml"
        ))
        .unwrap()
    }
    fn login(id: i32) -> RawEvent {
        let mut e = RawEvent {
            request: id,
            present: 1,
            last: 1,
            ..RawEvent::default()
        };
        e.trading_day[..8].copy_from_slice(b"20261111");
        e
    }
    #[test]
    fn reconnect_requires_login_and_resubscription_and_discards_late_login() {
        let mut c = config();
        c.symbols.truncate(1);
        let mut s = Session::new(&c);
        s.connected(1).unwrap();
        assert!(s.login(&login(1)).unwrap());
        let mut sub = RawEvent {
            present: 1,
            ..RawEvent::default()
        };
        sub.symbol[..6].copy_from_slice(b"IF2612");
        s.subscribed(&sub).unwrap();
        assert!(s.ready());
        s.disconnect();
        assert!(!s.ready());
        s.connected(2).unwrap();
        assert!(!s.login(&login(1)).unwrap());
        assert!(!s.ready());
        s.login(&login(2)).unwrap();
        assert!(!s.ready());
        s.subscribed(&sub).unwrap();
        assert!(s.ready());
        assert_eq!(s.generation, 2);
    }
    #[test]
    fn no_invented_prices_and_wrong_venue_is_rejected() {
        let mut c = config();
        c.symbols.truncate(1);
        let mut s = Session::new(&c);
        s.connected(1).unwrap();
        s.login(&login(1)).unwrap();
        let mut e = RawEvent {
            present: 1,
            last_price: f64::MAX,
            ..RawEvent::default()
        };
        e.symbol[..6].copy_from_slice(b"IF2612");
        assert!(s.tick(&e, 1).unwrap().last_price.is_none());
        e.exchange[..3].copy_from_slice(b"DCE");
        assert!(s.tick(&e, 1).is_err());
        s.disconnect();
        assert!(s.tick(&e, 1).is_err());
    }
}
