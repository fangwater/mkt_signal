use anyhow::{Context, Result};
use runtime_common::redis_client::RedisClient;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SWITCH_NAMES_KEY: &str = "exec_switch:strategy_names";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecFamily {
    BatchExec,
    ChaseExec,
}

impl ExecFamily {
    pub const fn namespace(self) -> &'static str {
        match self {
            Self::BatchExec => "batch_exec",
            Self::ChaseExec => "chase_exec",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecSwitchState {
    Requested,
    Ready,
    Activated,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecAlgorithmSwitch {
    pub from_family: ExecFamily,
    pub to_family: ExecFamily,
    pub state: ExecSwitchState,
    pub requested_at_us: i64,
    pub updated_at_us: i64,
    #[serde(default)]
    pub positions: BTreeMap<String, f64>,
}

impl ExecAlgorithmSwitch {
    pub fn validate(&self, strategy_name: &str) -> Result<()> {
        super::batch_exec_config::validate_config_strategy_name(strategy_name)?;
        if self.from_family == self.to_family {
            anyhow::bail!("Exec algorithm switch families must differ: {strategy_name}");
        }
        if self.requested_at_us <= 0 || self.updated_at_us <= 0 {
            anyhow::bail!("Exec algorithm switch timestamps must be positive: {strategy_name}");
        }
        if self.updated_at_us < self.requested_at_us {
            anyhow::bail!("Exec algorithm switch timestamp moved backwards: {strategy_name}");
        }
        if self.state == ExecSwitchState::Requested && !self.positions.is_empty() {
            anyhow::bail!(
                "requested Exec algorithm switch must not contain positions: {strategy_name}"
            );
        }
        for (symbol, qty) in &self.positions {
            if symbol.is_empty() || normalize_symbol_for_internal(symbol) != *symbol {
                anyhow::bail!(
                    "Exec algorithm switch symbol is not normalized: strategy_name={strategy_name} symbol={symbol}"
                );
            }
            if !qty.is_finite() {
                anyhow::bail!(
                    "Exec algorithm switch quantity must be finite: strategy_name={strategy_name} symbol={symbol}"
                );
            }
        }
        Ok(())
    }

    pub fn belongs_to(self: &Self, family: ExecFamily) -> bool {
        self.from_family == family || self.to_family == family
    }
}

pub fn switch_key(strategy_name: &str) -> String {
    format!("exec_switch:{strategy_name}")
}

pub fn active_names_key(family: ExecFamily) -> String {
    format!("{}:strategy_names", family.namespace())
}

pub async fn load_switches(
    client: &mut RedisClient,
) -> Result<BTreeMap<String, ExecAlgorithmSwitch>> {
    let names = client
        .get_json::<Vec<String>>(SWITCH_NAMES_KEY)
        .await
        .with_context(|| format!("load Redis key {SWITCH_NAMES_KEY}"))?
        .unwrap_or_default();
    let mut switches = BTreeMap::new();
    for strategy_name in names {
        super::batch_exec_config::validate_config_strategy_name(&strategy_name)?;
        let key = switch_key(&strategy_name);
        let switch = client
            .get_json::<ExecAlgorithmSwitch>(&key)
            .await
            .with_context(|| format!("load Redis key {key}"))?
            .ok_or_else(|| anyhow::anyhow!("indexed Exec algorithm switch is missing: {key}"))?;
        switch.validate(&strategy_name)?;
        if switches.insert(strategy_name.clone(), switch).is_some() {
            anyhow::bail!("duplicate strategy_name in {SWITCH_NAMES_KEY}: {strategy_name}");
        }
    }
    Ok(switches)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn switch_requires_distinct_families_and_normalized_positions() {
        let switch = ExecAlgorithmSwitch {
            from_family: ExecFamily::BatchExec,
            to_family: ExecFamily::ChaseExec,
            state: ExecSwitchState::Ready,
            requested_at_us: 10,
            updated_at_us: 20,
            positions: BTreeMap::from([("BTCUSDT".to_string(), 0.25)]),
        };
        switch.validate("alpha").unwrap();

        let mut invalid = switch.clone();
        invalid.to_family = ExecFamily::BatchExec;
        assert!(invalid.validate("alpha").is_err());

        let mut invalid = switch;
        invalid.positions = BTreeMap::from([("btc-usdt".to_string(), 0.25)]);
        assert!(invalid.validate("alpha").is_err());
    }

    #[test]
    fn manager_switch_json_matches_exec_contract() {
        let switch: ExecAlgorithmSwitch = serde_json::from_str(
            r#"{
                "from_family":"batch_exec",
                "to_family":"chase_exec",
                "state":"requested",
                "requested_at_us":100,
                "updated_at_us":100,
                "positions":{}
            }"#,
        )
        .unwrap();
        switch.validate("alpha").unwrap();
        assert_eq!(switch.from_family, ExecFamily::BatchExec);
        assert_eq!(switch.to_family, ExecFamily::ChaseExec);
        assert_eq!(switch.state, ExecSwitchState::Requested);
    }
}
