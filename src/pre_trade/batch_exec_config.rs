use crate::strategy::batch_exec_strategy::{BatchExecConfig, BatchExecStrategy};
use crate::strategy::StrategyManager;
use anyhow::{Context, Result};
use log::{info, warn};
use order_common::TradingVenue;
use runtime_common::redis_client::{RedisClient, RedisSettings};
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchExecRedisValue {
    #[serde(flatten)]
    pub config: BatchExecConfig,
    pub targets: BTreeMap<String, f64>,
}

impl BatchExecRedisValue {
    fn validate(&self) -> Result<()> {
        self.config.validate().map_err(anyhow::Error::msg)?;
        for (symbol, target_qty) in &self.targets {
            if normalize_symbol_for_internal(symbol).is_empty() {
                anyhow::bail!("target symbol must not be empty");
            }
            if !target_qty.is_finite() {
                anyhow::bail!("target_qty must be finite: symbol={symbol}");
            }
        }
        Ok(())
    }

    fn normalized_targets(&self) -> Result<BTreeMap<String, f64>> {
        let mut normalized = BTreeMap::new();
        for (raw_symbol, target_qty) in &self.targets {
            let symbol = normalize_symbol_for_internal(raw_symbol);
            if normalized.insert(symbol.clone(), *target_qty).is_some() {
                anyhow::bail!(
                    "duplicate normalized target symbol: raw_symbol={raw_symbol} normalized={symbol}"
                );
            }
        }
        Ok(normalized)
    }
}

pub struct BatchExecConfigReloader {
    client: RedisClient,
    venue: TradingVenue,
    snapshots: BTreeMap<String, BatchExecRedisValue>,
}

const STRATEGY_NAMES_KEY: &str = "batch_exec:strategy_names";

fn validate_strategy_name(name: &str) -> Result<()> {
    if name == "strategy_names" {
        anyhow::bail!("strategy_name is reserved: {name}");
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        anyhow::bail!("strategy_name must not be empty");
    };
    if !first.is_ascii_alphanumeric()
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        anyhow::bail!("strategy_name must match [A-Za-z0-9][A-Za-z0-9._-]*: {name}");
    }
    Ok(())
}

fn register_symbol_owners(
    owners: &mut BTreeMap<String, String>,
    strategy_name: &str,
    targets: &BTreeMap<String, f64>,
) -> Result<()> {
    for symbol in targets.keys() {
        if let Some(owner) = owners.insert(symbol.clone(), strategy_name.to_string()) {
            anyhow::bail!(
                "BatchExec symbol must have one strategy owner: symbol={symbol} owners={owner},{strategy_name}"
            );
        }
    }
    Ok(())
}

impl BatchExecConfigReloader {
    pub async fn connect(redis: RedisSettings, venue: TradingVenue) -> Result<Self> {
        let client = RedisClient::connect(redis).await?;
        Ok(Self {
            client,
            venue,
            snapshots: BTreeMap::new(),
        })
    }

    fn redis_key(strategy_name: &str) -> String {
        format!("batch_exec:{strategy_name}")
    }

    pub async fn reload(&mut self, strategy_mgr: &Rc<RefCell<StrategyManager>>) -> Result<usize> {
        let strategy_names = self
            .client
            .get_json::<Vec<String>>(STRATEGY_NAMES_KEY)
            .await
            .with_context(|| format!("load Redis key {STRATEGY_NAMES_KEY}"))?
            .unwrap_or_default();
        let mut active_names = BTreeSet::new();
        for name in strategy_names {
            validate_strategy_name(&name)?;
            if !active_names.insert(name.clone()) {
                anyhow::bail!("duplicate strategy_name in {STRATEGY_NAMES_KEY}: {name}");
            }
        }

        let mut loaded = Vec::new();
        for strategy_name in active_names.iter().cloned() {
            let key = Self::redis_key(&strategy_name);
            let payload = self
                .client
                .get_json::<BatchExecRedisValue>(&key)
                .await
                .with_context(|| format!("load Redis key {key}"))?;
            let payload = match payload {
                Some(payload) => payload,
                None => {
                    warn!("BatchExec Redis key missing: {}", key);
                    self.snapshots
                        .get(&strategy_name)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("indexed BatchExec config missing: {key}"))?
                }
            };
            payload
                .validate()
                .with_context(|| format!("invalid BatchExec config key={key}"))?;
            let normalized_targets = payload
                .normalized_targets()
                .with_context(|| format!("invalid BatchExec targets key={key}"))?;
            loaded.push((strategy_name, key, payload, normalized_targets));
        }

        let mut symbol_owners = BTreeMap::<String, String>::new();
        // A removed strategy keeps its symbols reserved until process restart. Its target is
        // reset below, but the physical position can still be flattening asynchronously.
        for (strategy_name, snapshot) in &self.snapshots {
            if !active_names.contains(strategy_name) {
                register_symbol_owners(
                    &mut symbol_owners,
                    strategy_name,
                    &snapshot.normalized_targets()?,
                )?;
            }
        }
        for (strategy_name, _, _, targets) in &loaded {
            register_symbol_owners(&mut symbol_owners, strategy_name, targets)?;
        }

        let mut applied = 0usize;
        for (strategy_name, key, payload, targets) in loaded {
            let previous = self.snapshots.get(&strategy_name);
            let config_changed = previous.is_none_or(|old| old.config != payload.config);
            let previous_targets = previous
                .map(BatchExecRedisValue::normalized_targets)
                .transpose()?
                .unwrap_or_default();
            let mut symbols = BTreeSet::new();
            symbols.extend(targets.keys().cloned());
            symbols.extend(previous_targets.keys().cloned());

            for symbol in symbols {
                let target_qty = targets.get(&symbol).copied().unwrap_or(0.0);
                let old_target = previous_targets.get(&symbol).copied();
                let target_changed = old_target != Some(target_qty);

                let strategy_id = strategy_mgr
                    .borrow_mut()
                    .ensure_batch_exec_strategy_for_normalized_symbol(
                        &strategy_name,
                        &symbol,
                        self.venue,
                        payload.config.clone(),
                    );
                if target_changed {
                    let strategy = { strategy_mgr.borrow_mut().take(strategy_id) };
                    if let Some(mut strategy) = strategy {
                        if let Some(exec) =
                            strategy.as_any_mut().downcast_mut::<BatchExecStrategy>()
                        {
                            exec.update_target(
                                target_qty,
                                get_timestamp_us(),
                                key.as_bytes().to_vec(),
                            );
                            applied += 1;
                        }
                        strategy_mgr.borrow_mut().insert(strategy);
                    }
                }
            }

            if config_changed || previous_targets != targets {
                info!(
                    "BatchExec Redis applied: strategy_name={} config_changed={} targets={}",
                    strategy_name,
                    config_changed,
                    payload.targets.len()
                );
            }
            self.snapshots.insert(strategy_name, payload);
        }

        let removed_names: Vec<String> = self
            .snapshots
            .keys()
            .filter(|name| !active_names.contains(*name))
            .cloned()
            .collect();
        for strategy_name in removed_names {
            let Some(previous) = self.snapshots.get(&strategy_name).cloned() else {
                continue;
            };
            let key = Self::redis_key(&strategy_name);
            let targets = previous.normalized_targets()?;
            let mut reset_snapshot = previous.clone();
            for target in reset_snapshot.targets.values_mut() {
                *target = 0.0;
            }
            for (symbol, old_target) in targets {
                if old_target == 0.0 {
                    continue;
                }
                let strategy_id = strategy_mgr
                    .borrow_mut()
                    .ensure_batch_exec_strategy_for_normalized_symbol(
                        &strategy_name,
                        &symbol,
                        self.venue,
                        previous.config.clone(),
                    );
                let strategy = { strategy_mgr.borrow_mut().take(strategy_id) };
                if let Some(mut strategy) = strategy {
                    if let Some(exec) = strategy.as_any_mut().downcast_mut::<BatchExecStrategy>() {
                        exec.update_target(0.0, get_timestamp_us(), key.as_bytes().to_vec());
                        applied += 1;
                    }
                    strategy_mgr.borrow_mut().insert(strategy);
                }
            }
            if previous.targets != reset_snapshot.targets {
                info!(
                    "BatchExec strategy removed from index; targets reset to zero: strategy_name={}",
                    strategy_name
                );
            }
            self.snapshots.insert(strategy_name, reset_snapshot);
        }
        Ok(applied)
    }

    pub fn spawn(mut self, strategy_mgr: Rc<RefCell<StrategyManager>>, interval: Duration) {
        tokio::task::spawn_local(async move {
            let mut timer = tokio::time::interval(interval);
            timer.tick().await;
            loop {
                timer.tick().await;
                if let Err(err) = self.reload(&strategy_mgr).await {
                    warn!("BatchExec Redis reload failed: {err:#}");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_value_contains_config_and_targets() {
        let value: BatchExecRedisValue = serde_json::from_str(
            r#"{
                "single_order_usdt": 100.0,
                "orders_per_batch": 3,
                "maker_price_anchor": "own_best",
                "tick_spacing": 2,
                "batch_interval_ms": 500,
                "maker_timeout_ms": 1000,
                "max_maker_requotes": 2,
                "target_tolerance_usdt": 10.0,
                "targets": {"BTCUSDT": 0.02}
            }"#,
        )
        .unwrap();
        value.validate().unwrap();
        assert_eq!(value.targets.get("BTCUSDT"), Some(&0.02));
        assert_eq!(value.config.orders_per_batch, 3);
    }

    #[test]
    fn rejects_symbol_owned_by_two_strategy_names() {
        let mut owners = BTreeMap::new();
        let targets = BTreeMap::from([("BTCUSDT".to_string(), 0.02)]);
        register_symbol_owners(&mut owners, "cta_a", &targets).unwrap();
        let err = register_symbol_owners(&mut owners, "cta_b", &targets).unwrap_err();
        assert!(err.to_string().contains("owners=cta_a,cta_b"));
    }

    #[test]
    fn validates_strategy_name_for_redis_keys() {
        validate_strategy_name("cta.alpha_01").unwrap();
        assert!(validate_strategy_name("").is_err());
        assert!(validate_strategy_name("cta:alpha").is_err());
    }
}
