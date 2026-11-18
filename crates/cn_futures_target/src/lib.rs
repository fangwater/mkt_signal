//! SDK-free transport for complete domestic-futures position targets.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

mod client;
mod ingress;
mod manager;
pub use client::Client;
pub use ingress::{Config, GrpcConfig, Ingress};
pub use manager::ManagerPlanConfig;

pub mod wire {
    tonic::include_proto!("cn_futures_target");
}

pub const MAX_CONTRACTS: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub source_id: String,
    pub batch_id: String,
    pub target_revision: i64,
    pub trading_day: String,
    pub expires_at_us: u64,
    pub net_targets: BTreeMap<String, i32>,
}

pub fn valid_id(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}

impl TryFrom<wire::PositionTarget> for Target {
    type Error = anyhow::Error;

    fn try_from(value: wire::PositionTarget) -> Result<Self> {
        Self::from_message(value, false)
    }
}

impl Target {
    fn from_message(value: wire::PositionTarget, observation: bool) -> Result<Self> {
        ensure!(
            valid_id(&value.source_id) && valid_id(&value.batch_id),
            "invalid source or batch id"
        );
        ensure!(
            value.target_revision > 0 && (value.expires_at_us == 0) == observation,
            "invalid revision or lease"
        );
        ensure!(
            (value.trading_day.len() == 8 && value.trading_day.bytes().all(|v| v.is_ascii_digit()))
                || (observation && value.trading_day.is_empty()),
            "invalid trading day"
        );
        ensure!(
            (1..=MAX_CONTRACTS).contains(&value.net_targets.len()),
            "invalid target contract count"
        );
        let mut net_targets = BTreeMap::new();
        for row in value.net_targets {
            let Some((venue, symbol)) = row.market.split_once(':') else {
                anyhow::bail!("expected exchange and native contract");
            };
            ensure!(
                matches!(venue, "SHFE" | "INE" | "DCE" | "CZCE" | "CFFEX" | "GFEX")
                    && (4..=63).contains(&symbol.len())
                    && symbol
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'.')
                    && symbol.bytes().any(|b| b.is_ascii_digit()),
                "invalid native contract key"
            );
            ensure!(
                net_targets.insert(row.market, row.lots).is_none(),
                "duplicate contract in target"
            );
        }
        Ok(Self {
            source_id: value.source_id,
            batch_id: value.batch_id,
            target_revision: value.target_revision,
            trading_day: value.trading_day,
            expires_at_us: value.expires_at_us,
            net_targets,
        })
    }

    pub fn into_wire(self, decision_us: u64) -> wire::PositionTarget {
        wire::PositionTarget {
            source_id: self.source_id,
            batch_id: self.batch_id,
            target_revision: self.target_revision,
            trading_day: self.trading_day,
            expires_at_us: self.expires_at_us,
            decision_us,
            net_targets: self
                .net_targets
                .into_iter()
                .map(|(market, lots)| wire::ContractTarget { market, lots })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests;
