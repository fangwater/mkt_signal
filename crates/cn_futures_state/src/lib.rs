//! SDK-free native account position snapshots. One complete replaceable state
//! per publication, using Exec's length-prefixed bincode over iceoryx2.
pub mod publisher;
pub mod transport;

use anyhow::{ensure, Result};
pub use persist_common::cn_futures::{Position, Side, Venue};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const MAX_POSITIONS: usize = 8192;
pub const PAYLOAD: usize = 1024 * 1024;
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
    pub replaced_snapshots: u64,
    pub positions: Vec<PositionRow>,
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
            replaced_snapshots: 0,
            positions: Vec::with_capacity(MAX_POSITIONS),
        })
    }
    pub fn validate(&self) -> Result<()> {
        validate_hash(self.account_hash.as_str())?;
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
}
fn default_interval() -> u64 {
    1000
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        service_name(&self.namespace)?;
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
