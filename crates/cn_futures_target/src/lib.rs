//! SDK-free transport for complete domestic-futures strategy targets, and the
//! Exec-owned strategy ledger that nets and attributes them.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

mod client;
mod ingress;
pub mod io;
pub mod ledger;
mod manager;
pub use client::Client;
pub use ingress::{Config, GrpcConfig, Ingress};
pub use io::{IoLoop, Spawner};
pub use manager::ManagerPlanConfig;

pub mod wire {
    tonic::include_proto!("cn_futures_target");
}

pub const MAX_CONTRACTS: usize = 128;
pub const MAX_STRATEGIES: usize = 512;

/// Contract key to signed lots, zeros included.
pub type Vector = BTreeMap<String, i32>;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub source_id: String,
    pub batch_id: String,
    pub target_revision: i64,
    pub trading_day: String,
    pub expires_at_us: u64,
    /// Complete roster; every strategy carries the same contract vector.
    pub strategies: BTreeMap<String, Vector>,
}

pub fn valid_id(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}

fn validate_market(market: &str) -> Result<()> {
    let Some((venue, symbol)) = market.split_once(':') else {
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
    Ok(())
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
            value.target_revision > 0 && (value.expires_at_us == 0) == observation,
            "invalid revision or lease"
        );
        ensure!(
            (value.trading_day.len() == 8 && value.trading_day.bytes().all(|v| v.is_ascii_digit()))
                || (observation && value.trading_day.is_empty()),
            "invalid trading day"
        );
        let mut strategies = BTreeMap::new();
        for row in value.strategy_targets {
            let mut vector = Vector::new();
            for target in row.targets {
                ensure!(
                    vector.insert(target.market, target.lots).is_none(),
                    "duplicate contract in strategy target"
                );
            }
            ensure!(
                strategies.insert(row.strategy, vector).is_none(),
                "duplicate strategy in target"
            );
        }
        let target = Self {
            source_id: value.source_id,
            batch_id: value.batch_id,
            target_revision: value.target_revision,
            trading_day: value.trading_day,
            expires_at_us: value.expires_at_us,
            strategies,
        };
        target.validate_shape()?;
        Ok(target)
    }

    /// Identity, complete identical contract vectors and representable net lots.
    pub fn validate_shape(&self) -> Result<()> {
        ensure!(
            valid_id(&self.source_id) && valid_id(&self.batch_id),
            "invalid source or batch id"
        );
        ensure!(
            (1..=MAX_STRATEGIES).contains(&self.strategies.len()),
            "invalid target strategy count"
        );
        let first = self.strategies.values().next().context("empty roster")?;
        ensure!(
            (1..=MAX_CONTRACTS).contains(&first.len()),
            "invalid target contract count"
        );
        for market in first.keys() {
            validate_market(market)?;
        }
        for (strategy, vector) in &self.strategies {
            ensure!(
                valid_id(strategy) && strategy != ledger::UNALLOCATED,
                "invalid strategy name"
            );
            ensure!(
                vector.keys().eq(first.keys()),
                "every strategy must carry the complete contract vector, zeros included"
            );
        }
        for market in first.keys() {
            i32::try_from(self.net(market)).context("net target exceeds native lot bounds")?;
        }
        Ok(())
    }

    pub fn markets(&self) -> impl Iterator<Item = &String> {
        self.strategies
            .values()
            .next()
            .into_iter()
            .flat_map(|v| v.keys())
    }

    pub fn contains(&self, market: &str) -> bool {
        self.strategies
            .values()
            .next()
            .is_some_and(|v| v.contains_key(market))
    }

    /// Signed account target: the sum of all strategy targets for one contract.
    pub fn net(&self, market: &str) -> i64 {
        self.strategies
            .values()
            .map(|v| i64::from(v.get(market).copied().unwrap_or(0)))
            .sum()
    }

    /// Per-strategy targets for one contract, sorted by strategy name.
    /// Built at target admission, never per quote.
    pub fn strategy_targets(&self, market: &str) -> Vec<(String, i64)> {
        self.strategies
            .iter()
            .map(|(name, v)| (name.clone(), i64::from(v.get(market).copied().unwrap_or(0))))
            .collect()
    }

    pub fn into_wire(self, decision_us: u64) -> wire::PositionTarget {
        wire::PositionTarget {
            source_id: self.source_id,
            batch_id: self.batch_id,
            target_revision: self.target_revision,
            trading_day: self.trading_day,
            expires_at_us: self.expires_at_us,
            decision_us,
            strategy_targets: self
                .strategies
                .into_iter()
                .map(|(strategy, vector)| wire::StrategyTarget {
                    strategy,
                    targets: vector
                        .into_iter()
                        .map(|(market, lots)| wire::ContractTarget { market, lots })
                        .collect(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests;
