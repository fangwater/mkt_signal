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
    position_ledger: Option<BatchExecPositionLedger>,
}

const STRATEGY_NAMES_KEY: &str = "batch_exec:strategy_names";
const POSITION_LEDGER_KEY: &str = "batch_exec_state:position_allocations";
const POSITION_LEDGER_VERSION: u32 = 1;
const POSITION_ALLOCATION_EPS: f64 = 1e-10;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchExecPositionLedger {
    version: u32,
    updated_at_us: i64,
    positions: BTreeMap<String, BTreeMap<String, f64>>,
}

impl BatchExecPositionLedger {
    fn empty() -> Self {
        Self {
            version: POSITION_LEDGER_VERSION,
            updated_at_us: 0,
            positions: BTreeMap::new(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.version != POSITION_LEDGER_VERSION {
            anyhow::bail!(
                "unsupported BatchExec position ledger version: expected={} actual={}",
                POSITION_LEDGER_VERSION,
                self.version
            );
        }
        for (strategy_name, positions) in &self.positions {
            validate_strategy_name(strategy_name)?;
            for (symbol, position_qty) in positions {
                if symbol.is_empty() || normalize_symbol_for_internal(symbol) != *symbol {
                    anyhow::bail!(
                        "BatchExec position ledger symbol is not normalized: strategy_name={strategy_name} symbol={symbol}"
                    );
                }
                if !position_qty.is_finite() {
                    anyhow::bail!(
                        "BatchExec position ledger quantity must be finite: strategy_name={strategy_name} symbol={symbol}"
                    );
                }
            }
        }
        Ok(())
    }

    fn get(&self, strategy_name: &str, symbol: &str) -> Option<f64> {
        self.positions
            .get(strategy_name)
            .and_then(|positions| positions.get(symbol))
            .copied()
    }

    fn set(&mut self, strategy_name: &str, symbol: &str, position_qty: f64) {
        self.positions
            .entry(strategy_name.to_string())
            .or_default()
            .insert(symbol.to_string(), position_qty);
    }
}

#[derive(Debug, Clone)]
struct PositionAllocationCandidate {
    strategy_id: i32,
    strategy_name: String,
    symbol: String,
    target_qty: f64,
    position_qty: f64,
    missing_position: bool,
    has_virtual_position: bool,
    allocation_ready: bool,
    execution_in_flight: bool,
    reconciliation_settled: bool,
    reconciliation_ready: bool,
}

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

fn distribute_position_difference_to_target_gaps(
    candidates: &mut [PositionAllocationCandidate],
    difference: f64,
    missing_only: bool,
) -> f64 {
    if difference.abs() <= POSITION_ALLOCATION_EPS {
        return 0.0;
    }
    let direction = difference.signum();
    let gaps: Vec<(usize, f64)> = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| !missing_only || candidate.missing_position)
        .filter_map(|(index, candidate)| {
            let gap = candidate.target_qty - candidate.position_qty;
            (gap.abs() > POSITION_ALLOCATION_EPS && gap.signum() == direction)
                .then_some((index, gap.abs()))
        })
        .collect();
    let capacity: f64 = gaps.iter().map(|(_, gap)| *gap).sum();
    if capacity <= POSITION_ALLOCATION_EPS {
        return difference;
    }

    let amount = difference.abs().min(capacity);
    let mut allocated = 0.0;
    for (offset, (index, gap)) in gaps.iter().enumerate() {
        let share = if offset + 1 == gaps.len() {
            amount - allocated
        } else {
            (amount * *gap / capacity).min(amount - allocated)
        };
        candidates[*index].position_qty += direction * share;
        allocated += share;
    }
    difference - direction * allocated
}

fn allocate_account_position(
    candidates: &mut [PositionAllocationCandidate],
    account_position_qty: f64,
) {
    if candidates.is_empty() {
        return;
    }
    let allocated_qty: f64 = candidates
        .iter()
        .map(|candidate| candidate.position_qty)
        .sum();
    let mut difference = account_position_qty - allocated_qty;
    // New allocations consume same-direction target gaps first. Persisted allocations then
    // absorb any remaining restart drift, with the first stable strategy as the final fallback.
    difference = distribute_position_difference_to_target_gaps(candidates, difference, true);
    difference = distribute_position_difference_to_target_gaps(candidates, difference, false);
    if difference.abs() > POSITION_ALLOCATION_EPS {
        candidates[0].position_qty += difference;
    }

    let corrected_total: f64 = candidates
        .iter()
        .map(|candidate| candidate.position_qty)
        .sum();
    candidates[0].position_qty += account_position_qty - corrected_total;
}

impl BatchExecConfigReloader {
    pub async fn connect(redis: RedisSettings, venue: TradingVenue) -> Result<Self> {
        let client = RedisClient::connect(redis).await?;
        Ok(Self {
            client,
            venue,
            snapshots: BTreeMap::new(),
            position_ledger: None,
        })
    }

    fn redis_key(strategy_name: &str) -> String {
        format!("batch_exec:{strategy_name}")
    }

    async fn load_position_ledger(&mut self) -> Result<()> {
        if self.position_ledger.is_some() {
            return Ok(());
        }
        let ledger = self
            .client
            .get_json::<BatchExecPositionLedger>(POSITION_LEDGER_KEY)
            .await
            .with_context(|| format!("load Redis key {POSITION_LEDGER_KEY}"))?
            .unwrap_or_else(BatchExecPositionLedger::empty);
        ledger.validate().with_context(|| {
            format!("invalid BatchExec position ledger key={POSITION_LEDGER_KEY}")
        })?;
        info!(
            "BatchExec position ledger loaded: strategies={} updated_at_us={}",
            ledger.positions.len(),
            ledger.updated_at_us
        );
        self.position_ledger = Some(ledger);
        Ok(())
    }

    fn collect_position_candidates(
        &self,
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        now_ts: i64,
    ) -> Vec<PositionAllocationCandidate> {
        let Some(ledger) = self.position_ledger.as_ref() else {
            return Vec::new();
        };
        let manager = strategy_mgr.borrow();
        let mut candidates = Vec::new();
        for strategy_id in manager.iter_ids().copied() {
            let Some(strategy) = manager.get(strategy_id) else {
                continue;
            };
            let Some(exec) = strategy.as_any().downcast_ref::<BatchExecStrategy>() else {
                continue;
            };
            if exec.exec_venue() != self.venue {
                continue;
            }
            let virtual_position = exec.virtual_position_qty();
            let persisted_position = ledger.get(exec.strategy_name(), exec.exec_symbol());
            candidates.push(PositionAllocationCandidate {
                strategy_id,
                strategy_name: exec.strategy_name().to_string(),
                symbol: exec.exec_symbol().to_string(),
                target_qty: exec.target_qty().unwrap_or(0.0),
                position_qty: virtual_position.or(persisted_position).unwrap_or(0.0),
                missing_position: virtual_position.is_none() && persisted_position.is_none(),
                has_virtual_position: virtual_position.is_some(),
                allocation_ready: exec.position_allocation_ready(),
                execution_in_flight: exec.has_execution_in_flight(),
                reconciliation_settled: exec.position_reconciliation_settled(now_ts),
                reconciliation_ready: exec.position_reconciliation_ready(now_ts),
            });
        }
        candidates.sort_by(|lhs, rhs| {
            (&lhs.symbol, &lhs.strategy_name).cmp(&(&rhs.symbol, &rhs.strategy_name))
        });
        candidates
    }

    fn suspend_position_allocations(
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        strategy_ids: &BTreeSet<i32>,
    ) -> Result<()> {
        let mut manager = strategy_mgr.borrow_mut();
        for strategy_id in strategy_ids {
            let Some(mut strategy) = manager.take(*strategy_id) else {
                anyhow::bail!(
                    "BatchExec strategy disappeared while suspending position allocation: strategy_id={strategy_id}"
                );
            };
            let result = strategy
                .as_any_mut()
                .downcast_mut::<BatchExecStrategy>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "strategy is not BatchExec while suspending position allocation: strategy_id={strategy_id}"
                    )
                })
                .and_then(|exec| {
                    exec.suspend_position_allocation()
                        .map_err(anyhow::Error::msg)
                });
            manager.insert(strategy);
            result?;
        }
        Ok(())
    }

    fn pause_position_allocations(
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        strategy_ids: &BTreeSet<i32>,
    ) -> Result<()> {
        let mut manager = strategy_mgr.borrow_mut();
        for strategy_id in strategy_ids {
            let Some(mut strategy) = manager.take(*strategy_id) else {
                anyhow::bail!(
                    "BatchExec strategy disappeared while pausing position allocation: strategy_id={strategy_id}"
                );
            };
            let result = strategy
                .as_any_mut()
                .downcast_mut::<BatchExecStrategy>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "strategy is not BatchExec while pausing position allocation: strategy_id={strategy_id}"
                    )
                })
                .map(|exec| exec.pause_position_allocation());
            manager.insert(strategy);
            result?;
        }
        Ok(())
    }

    fn apply_position_allocations(
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        plans: &[PositionAllocationCandidate],
        now_ts: i64,
    ) -> Result<usize> {
        let mut manager = strategy_mgr.borrow_mut();
        let mut applied = 0usize;
        for plan in plans {
            let Some(mut strategy) = manager.take(plan.strategy_id) else {
                anyhow::bail!(
                    "BatchExec strategy disappeared while applying position allocation: strategy_id={}",
                    plan.strategy_id
                );
            };
            let result = strategy
                .as_any_mut()
                .downcast_mut::<BatchExecStrategy>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "strategy is not BatchExec while applying position allocation: strategy_id={}",
                        plan.strategy_id
                    )
                })
                .and_then(|exec| {
                    exec.apply_position_allocation(plan.position_qty, now_ts)
                        .map_err(anyhow::Error::msg)
                });
            manager.insert(strategy);
            result?;
            applied += 1;
        }
        Ok(applied)
    }

    async fn reconcile_position_allocations(
        &mut self,
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
    ) -> Result<usize> {
        self.load_position_ledger().await?;
        if !crate::pre_trade::monitor_channel::MonitorChannel::instance()
            .exec_position_snapshot_ready()
        {
            return Ok(0);
        }

        let now_ts = get_timestamp_us();
        let candidates = self.collect_position_candidates(strategy_mgr, now_ts);
        if candidates.is_empty() {
            return Ok(0);
        }
        let mut groups = BTreeMap::<String, Vec<PositionAllocationCandidate>>::new();
        for candidate in &candidates {
            groups
                .entry(candidate.symbol.clone())
                .or_default()
                .push(candidate.clone());
        }

        let monitor = crate::pre_trade::monitor_channel::MonitorChannel::instance();
        let mut plans = Vec::new();
        for (symbol, mut group) in groups {
            let account_position_qty = monitor.get_position_qty(&symbol, self.venue);
            let allocated_qty: f64 = group.iter().map(|candidate| candidate.position_qty).sum();
            let difference = account_position_qty - allocated_qty;
            let needs_initialization = group.iter().any(|candidate| !candidate.allocation_ready);
            let can_initialize = !group.iter().any(|candidate| candidate.execution_in_flight)
                && !group.iter().any(|candidate| {
                    candidate.has_virtual_position && !candidate.reconciliation_settled
                });
            let can_reconcile = group.iter().all(|candidate| candidate.reconciliation_ready);

            if needs_initialization {
                if !can_initialize {
                    continue;
                }
            } else if difference.abs() <= POSITION_ALLOCATION_EPS || !can_reconcile {
                continue;
            }

            allocate_account_position(&mut group, account_position_qty);
            info!(
                "BatchExec aggregate position allocation planned: symbol={} account_position_qty={:.8} previous_allocated_qty={:.8} strategies={}",
                symbol,
                account_position_qty,
                allocated_qty,
                group.len()
            );
            plans.extend(group);
        }

        let mut next_ledger = self
            .position_ledger
            .clone()
            .unwrap_or_else(BatchExecPositionLedger::empty);
        for candidate in &candidates {
            next_ledger.set(
                &candidate.strategy_name,
                &candidate.symbol,
                candidate.position_qty,
            );
        }
        for plan in &plans {
            next_ledger.set(&plan.strategy_name, &plan.symbol, plan.position_qty);
        }

        if !plans.is_empty() {
            let strategy_ids = plans
                .iter()
                .map(|plan| plan.strategy_id)
                .collect::<BTreeSet<_>>();
            Self::suspend_position_allocations(strategy_mgr, &strategy_ids)?;
        }

        // A changed allocation is durable before any suspended strategy can submit again.
        let positions_changed = self
            .position_ledger
            .as_ref()
            .is_none_or(|current| current.positions != next_ledger.positions);
        if positions_changed {
            next_ledger.updated_at_us = now_ts;
            let save_result = self
                .client
                .set_json(POSITION_LEDGER_KEY, &next_ledger)
                .await;
            if let Err(err) = save_result {
                let strategy_ids = candidates
                    .iter()
                    .map(|candidate| candidate.strategy_id)
                    .collect::<BTreeSet<_>>();
                if let Err(pause_err) =
                    Self::pause_position_allocations(strategy_mgr, &strategy_ids)
                {
                    warn!(
                        "BatchExec failed to pause after position ledger save error: {pause_err:#}"
                    );
                }
                return Err(err).with_context(|| format!("save Redis key {POSITION_LEDGER_KEY}"));
            }
            self.position_ledger = Some(next_ledger);
        }

        Self::apply_position_allocations(strategy_mgr, &plans, now_ts)
    }

    pub async fn reload(&mut self, strategy_mgr: &Rc<RefCell<StrategyManager>>) -> Result<usize> {
        let mut applied = self.reconcile_position_allocations(strategy_mgr).await?;
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
        applied += self.reconcile_position_allocations(strategy_mgr).await?;
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

    fn allocation_candidate(
        strategy_id: i32,
        strategy_name: &str,
        target_qty: f64,
        position_qty: f64,
        missing_position: bool,
    ) -> PositionAllocationCandidate {
        PositionAllocationCandidate {
            strategy_id,
            strategy_name: strategy_name.to_string(),
            symbol: "BTCUSDT".to_string(),
            target_qty,
            position_qty,
            missing_position,
            has_virtual_position: !missing_position,
            allocation_ready: !missing_position,
            execution_in_flight: false,
            reconciliation_settled: true,
            reconciliation_ready: !missing_position,
        }
    }

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
    fn fresh_opposing_strategies_allocate_account_position_by_direction() {
        let mut candidates = vec![
            allocation_candidate(1, "cta_long", 1.0, 0.0, true),
            allocation_candidate(2, "cta_short", -0.4, 0.0, true),
        ];

        allocate_account_position(&mut candidates, 0.6);

        assert!((candidates[0].position_qty - 0.6).abs() < POSITION_ALLOCATION_EPS);
        assert!(candidates[1].position_qty.abs() < POSITION_ALLOCATION_EPS);
    }

    #[test]
    fn restart_difference_is_assigned_to_matching_target_gap() {
        let mut candidates = vec![
            allocation_candidate(1, "cta_long", 1.0, 0.7, false),
            allocation_candidate(2, "cta_short", -0.4, -0.4, false),
        ];

        allocate_account_position(&mut candidates, 0.6);

        assert!((candidates[0].position_qty - 1.0).abs() < POSITION_ALLOCATION_EPS);
        assert!((candidates[1].position_qty + 0.4).abs() < POSITION_ALLOCATION_EPS);
    }

    #[test]
    fn persisted_opposing_allocations_survive_zero_net_restart() {
        let mut candidates = vec![
            allocation_candidate(1, "cta_long", 1.0, 1.0, false),
            allocation_candidate(2, "cta_short", -1.0, -1.0, false),
        ];

        allocate_account_position(&mut candidates, 0.0);

        assert!((candidates[0].position_qty - 1.0).abs() < POSITION_ALLOCATION_EPS);
        assert!((candidates[1].position_qty + 1.0).abs() < POSITION_ALLOCATION_EPS);
    }

    #[test]
    fn same_direction_allocation_is_proportional_to_target_gap() {
        let mut candidates = vec![
            allocation_candidate(1, "cta_a", 1.0, 0.0, true),
            allocation_candidate(2, "cta_b", 3.0, 0.0, true),
        ];

        allocate_account_position(&mut candidates, 2.0);

        assert!((candidates[0].position_qty - 0.5).abs() < POSITION_ALLOCATION_EPS);
        assert!((candidates[1].position_qty - 1.5).abs() < POSITION_ALLOCATION_EPS);
    }

    #[test]
    fn validates_strategy_name_for_redis_keys() {
        validate_strategy_name("cta.alpha_01").unwrap();
        assert!(validate_strategy_name("").is_err());
        assert!(validate_strategy_name("cta:alpha").is_err());
    }
}
