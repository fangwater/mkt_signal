//! SDK-free native account position snapshots. One complete replaceable state
//! per publication, using Exec's length-prefixed bincode over iceoryx2.
pub mod checkpoint;
pub mod publisher;
pub mod transport;

use anyhow::{ensure, Result};
pub use persist_common::cn_futures::{Position, Side, Venue};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const MAX_POSITIONS: usize = 8192;
pub const PAYLOAD: usize = 4 * 1024 * 1024;
pub const BUFFER: usize = 2;
pub const CHANNEL: &str = "cn_futures_positions";

/// Fixed text keeps snapshot capture free of per-row heap allocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Text<const N: usize> {
    bytes: [u8; N],
    len: usize,
}
impl<const N: usize> Default for Text<N> {
    fn default() -> Self {
        Self {
            bytes: [0; N],
            len: 0,
        }
    }
}
impl<const N: usize> Text<N> {
    pub fn new(value: &str) -> Result<Self> {
        ensure!(value.len() <= N, "native state text exceeds {N} bytes");
        let mut text = Self::default();
        text.bytes[..value.len()].copy_from_slice(value.as_bytes());
        text.len = value.len();
        Ok(text)
    }
    pub fn as_str(&self) -> &str {
        // Only complete UTF-8 strings can construct Text, including decoding.
        std::str::from_utf8(&self.bytes[..self.len]).expect("validated native text")
    }
}
impl<const N: usize> Serialize for Text<N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}
impl<'de, const N: usize> Deserialize<'de> for Text<N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct Visitor<const N: usize>;
        impl<'de, const N: usize> serde::de::Visitor<'de> for Visitor<N> {
            type Value = Text<N>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "UTF-8 text up to {N} bytes")
            }
            fn visit_str<E: serde::de::Error>(
                self,
                v: &str,
            ) -> std::result::Result<Self::Value, E> {
                Text::new(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(Visitor::<N>)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Adapter {
    Ctp,
    Yida,
}
impl Adapter {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ctp => "ctp",
            Self::Yida => "yida",
        }
    }
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStatus {
    Running,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PositionRow {
    pub venue: Venue,
    pub symbol: Text<81>,
    pub side: Side,
    pub position: Position,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    Future,
    Option,
}

/// Native L1 and catalog values. Initial snapshots never become live ticks by
/// being copied into a later position heartbeat. Native timestamps stay native.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct InstrumentMark {
    pub venue: Venue,
    pub symbol: Text<81>,
    pub kind: InstrumentKind,
    pub contract_multiple: i32,
    pub last_price: Option<f64>,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub bid_lots: Option<i32>,
    pub ask_lots: Option<i32>,
    pub trading_day: u32,
    pub native_timestamp: Option<i32>,
    pub last_trade_timestamp: Option<i32>,
    pub native_time: Text<24>,
    pub last_trade_time: Text<24>,
    pub received_us: u64,
    pub initial: bool,
    pub paused: bool,
}
impl InstrumentMark {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.symbol.as_str().is_empty() && self.contract_multiple > 0 && self.received_us > 0,
            "invalid native valuation identity/multiplier/time"
        );
        ensure!(
            self.trading_day == 0 || (19000101..=29991231).contains(&self.trading_day),
            "invalid L1 trading day"
        );
        for price in [self.last_price, self.bid_price, self.ask_price]
            .into_iter()
            .flatten()
        {
            ensure!(
                price.is_finite() && price > 0.0 && price < f64::MAX,
                "invalid L1 price"
            );
        }
        ensure!(
            [
                self.bid_lots,
                self.ask_lots,
                self.native_timestamp,
                self.last_trade_timestamp
            ]
            .into_iter()
            .flatten()
            .all(|v| v >= 0),
            "invalid L1 quantity/time"
        );
        Ok(())
    }
}

/// Factual broker account values. The query time is independent of IPC heartbeats.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct AccountFunds {
    pub observed_us: u64,
    pub currency: Text<8>,
    pub balance: f64,
    pub available: f64,
    pub withdraw_quota: f64,
    pub curr_margin: f64,
    pub frozen_margin: f64,
    pub frozen_cash: f64,
    pub frozen_commission: f64,
    pub position_profit: f64,
    pub close_profit: f64,
    pub commission: f64,
}
impl AccountFunds {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.observed_us > 0, "account query time is missing");
        ensure!(
            self.currency.as_str() == "CNY",
            "unsupported account currency"
        );
        ensure!(
            [
                self.balance,
                self.available,
                self.withdraw_quota,
                self.curr_margin,
                self.frozen_margin,
                self.frozen_cash,
                self.frozen_commission,
                self.position_profit,
                self.close_profit,
                self.commission
            ]
            .iter()
            .all(|value| value.is_finite()),
            "nonfinite account funds"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Snapshot {
    pub publisher_epoch: u64,
    pub sequence: u64,
    /// Business-loop capture time, never the later serialization/send time.
    pub observed_us: u64,
    pub adapter: Adapter,
    pub account_hash: Text<64>,
    pub runtime_status: RuntimeStatus,
    pub position_ready: bool,
    pub connected: bool,
    pub ready: bool,
    pub can_trade: bool,
    pub transport_ready: bool,
    pub trading_day: Text<16>,
    pub reason: Text<512>,
    pub available_local_budget: f64,
    pub account: Option<AccountFunds>,
    pub replaced_snapshots: u64,
    pub positions: Vec<PositionRow>,
    pub marks: Vec<InstrumentMark>,
    pub l1_connected: bool,
    pub l1_dropped: u64,
}
impl Snapshot {
    pub fn empty(adapter: Adapter, account_hash: &str) -> Result<Self> {
        validate_hash(account_hash)?;
        Ok(Self {
            publisher_epoch: 0,
            sequence: 0,
            observed_us: 0,
            adapter,
            account_hash: Text::new(account_hash)?,
            runtime_status: RuntimeStatus::Running,
            position_ready: false,
            connected: false,
            ready: false,
            can_trade: false,
            transport_ready: false,
            trading_day: Text::default(),
            reason: Text::default(),
            available_local_budget: 0.0,
            account: None,
            replaced_snapshots: 0,
            positions: Vec::with_capacity(MAX_POSITIONS),
            marks: Vec::with_capacity(MAX_POSITIONS),
            l1_connected: false,
            l1_dropped: 0,
        })
    }
    pub fn validate(&self) -> Result<()> {
        validate_hash(self.account_hash.as_str())?;
        if let Some(account) = &self.account {
            account.validate()?;
            ensure!(
                account.observed_us <= self.observed_us,
                "account query is newer than capture"
            );
        }
        ensure!(
            self.publisher_epoch > 0 && self.sequence > 0 && self.observed_us > 0,
            "invalid native position envelope"
        );
        ensure!(
            self.positions.len() <= MAX_POSITIONS,
            "native position snapshot too large"
        );
        ensure!(
            self.position_ready || self.positions.is_empty(),
            "unrecovered position snapshot contains rows"
        );
        ensure!(
            !self.position_ready
                || (self.trading_day.as_str().len() == 8
                    && self
                        .trading_day
                        .as_str()
                        .bytes()
                        .all(|b| b.is_ascii_digit())),
            "recovered positions require the native trading day"
        );
        ensure!(
            !self.can_trade
                || (self.runtime_status == RuntimeStatus::Running
                    && self.position_ready
                    && self.ready
                    && self.connected
                    && self.transport_ready),
            "inconsistent native trade readiness"
        );
        ensure!(
            self.available_local_budget.is_finite() && self.available_local_budget >= 0.0,
            "invalid local budget"
        );
        let mut keys = std::collections::HashSet::new();
        for p in &self.positions {
            ensure!(
                !p.symbol.as_str().is_empty() && keys.insert((p.venue, p.symbol.as_str(), p.side)),
                "invalid/duplicate native position identity"
            );
            p.position.validate()?;
        }
        ensure!(
            self.marks.len() <= MAX_POSITIONS,
            "native mark capacity exceeded"
        );
        let mut marks = std::collections::HashSet::new();
        for mark in &self.marks {
            mark.validate()?;
            ensure!(
                marks.insert((mark.venue, mark.symbol.as_str())),
                "duplicate native valuation mark"
            );
        }
        Ok(())
    }
}
pub fn validate_hash(hash: &str) -> Result<()> {
    ensure!(
        hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid native account hash"
    );
    Ok(())
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub namespace: String,
    #[serde(default = "default_interval")]
    pub interval_ms: u64,
    pub account_snapshot_path: Option<std::path::PathBuf>,
}
fn default_interval() -> u64 {
    1000
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        service_name(&self.namespace)?;
        if let Some(path) = &self.account_snapshot_path {
            checkpoint::validate_path(path)?;
        }
        ensure!(
            (100..=60_000).contains(&self.interval_ms),
            "position IPC interval_ms must be 100..60000"
        );
        Ok(())
    }
}
pub fn service_name(namespace: &str) -> Result<String> {
    ensure!(
        !namespace.is_empty()
            && namespace.len() <= 100
            && namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')),
        "invalid native position namespace"
    );
    Ok(format!("{namespace}/viz_pubs/{CHANNEL}"))
}

#[cfg(test)]
mod tests;
