use crate::strategy::batch_exec_strategy::{
    BatchExecConfig, BatchExecStrategy, BatchExecTarget, BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME,
};
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
    pub targets: BTreeMap<String, BatchExecTarget>,
    #[serde(default)]
    pub updated_at_us: Option<i64>,
}

impl BatchExecRedisValue {
    fn validate(&self) -> Result<()> {
        self.config.validate().map_err(anyhow::Error::msg)?;
        if self.updated_at_us.is_some_and(|timestamp| timestamp <= 0) {
            anyhow::bail!("updated_at_us must be positive when present");
        }
        for (symbol, target) in &self.targets {
            if normalize_symbol_for_internal(symbol).is_empty() {
                anyhow::bail!("target symbol must not be empty");
            }
            if !target.qty.is_finite() {
                anyhow::bail!("target_qty must be finite: symbol={symbol}");
            }
        }
        Ok(())
    }

    fn normalized_targets(&self) -> Result<BTreeMap<String, BatchExecTarget>> {
        let mut normalized = BTreeMap::new();
        for (raw_symbol, target) in &self.targets {
            let symbol = normalize_symbol_for_internal(raw_symbol);
            if normalized.insert(symbol.clone(), *target).is_some() {
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
    pending_removals: BTreeSet<String>,
    pending_ledger_removals: BTreeSet<String>,
    removal_configs: BTreeMap<String, BatchExecConfig>,
    close_configs: BTreeMap<String, BatchExecConfig>,
}

const STRATEGY_NAMES_KEY: &str = "batch_exec:strategy_names";
const REMOVED_STRATEGY_NAMES_KEY: &str = "batch_exec:removed_strategy_names";
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

    fn remove_strategy(&mut self, strategy_name: &str) {
        self.positions.remove(strategy_name);
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
    if matches!(name, "strategy_names" | "removed_strategy_names") {
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

fn validate_config_strategy_name(name: &str) -> Result<()> {
    validate_strategy_name(name)?;
    if name == BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME {
        anyhow::bail!("strategy_name is reserved: {name}");
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
        .filter(|(_, candidate)| candidate.strategy_name != BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME)
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
) -> f64 {
    if candidates.is_empty() {
        return account_position_qty;
    }
    let allocated_qty: f64 = candidates
        .iter()
        .map(|candidate| candidate.position_qty)
        .sum();
    let mut difference = account_position_qty - allocated_qty;
    // Existing strategies absorb only same-direction target gaps. Any unmatched position is
    // returned to the caller and assigned to SYSTEM_POSITION_CLOSE.
    difference = distribute_position_difference_to_target_gaps(candidates, difference, true);
    distribute_position_difference_to_target_gaps(candidates, difference, false)
}

fn assign_residual_to_position_close(
    candidates: &mut [PositionAllocationCandidate],
    residual: f64,
) -> bool {
    if residual.abs() <= POSITION_ALLOCATION_EPS {
        return true;
    }
    let Some(close) = candidates
        .iter_mut()
        .find(|candidate| candidate.strategy_name == BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME)
    else {
        return false;
    };
    close.position_qty += residual;
    true
}

fn select_requested_removals(
    removal_requests: &BTreeSet<String>,
    snapshot_names: &BTreeSet<String>,
    ledger_names: &BTreeSet<String>,
    manager_names: &BTreeSet<String>,
) -> BTreeSet<String> {
    removal_requests
        .iter()
        .filter(|name| {
            snapshot_names.contains(*name)
                || ledger_names.contains(*name)
                || manager_names.contains(*name)
        })
        .cloned()
        .collect()
}

fn nonzero_unmanaged_ledger_names(
    positions: &BTreeMap<String, BTreeMap<String, f64>>,
    active_names: &BTreeSet<String>,
    removal_requests: &BTreeSet<String>,
    manager_names: &BTreeSet<String>,
) -> BTreeSet<String> {
    positions
        .iter()
        .filter(|(strategy_name, strategy_positions)| {
            strategy_name.as_str() != BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME
                && !active_names.contains(*strategy_name)
                && !removal_requests.contains(*strategy_name)
                && !manager_names.contains(*strategy_name)
                && strategy_positions
                    .values()
                    .any(|position_qty| position_qty.abs() > POSITION_ALLOCATION_EPS)
        })
        .map(|(strategy_name, _)| strategy_name.clone())
        .collect()
}

impl BatchExecConfigReloader {
    pub async fn connect(redis: RedisSettings, venue: TradingVenue) -> Result<Self> {
        let client = RedisClient::connect(redis).await?;
        Ok(Self {
            client,
            venue,
            snapshots: BTreeMap::new(),
            position_ledger: None,
            pending_removals: BTreeSet::new(),
            pending_ledger_removals: BTreeSet::new(),
            removal_configs: BTreeMap::new(),
            close_configs: BTreeMap::new(),
        })
    }

    fn redis_key(strategy_name: &str) -> String {
        format!("batch_exec:{strategy_name}")
    }

    fn close_config_for_symbol(&self, symbol: &str) -> BatchExecConfig {
        self.close_configs
            .get(symbol)
            .cloned()
            .or_else(|| {
                self.snapshots.values().find_map(|payload| {
                    payload
                        .targets
                        .keys()
                        .any(|candidate| normalize_symbol_for_internal(candidate) == symbol)
                        .then(|| payload.config.clone())
                })
            })
            .or_else(|| {
                self.snapshots
                    .values()
                    .next()
                    .map(|payload| payload.config.clone())
            })
            .unwrap_or_default()
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

    async fn queue_removed_strategies(
        &mut self,
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        removal_requests: &BTreeSet<String>,
    ) -> Result<()> {
        let manager_names = {
            let manager = strategy_mgr.borrow();
            manager
                .iter_ids()
                .filter_map(|strategy_id| {
                    manager
                        .get(*strategy_id)?
                        .as_any()
                        .downcast_ref::<BatchExecStrategy>()
                        .filter(|exec| exec.exec_venue() == self.venue)
                        .map(|exec| exec.strategy_name().to_string())
                })
                .collect::<BTreeSet<_>>()
        };
        let snapshot_names = self.snapshots.keys().cloned().collect::<BTreeSet<_>>();
        let ledger_names = self
            .position_ledger
            .as_ref()
            .map(|ledger| ledger.positions.keys().cloned().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let removed_names = select_requested_removals(
            removal_requests,
            &snapshot_names,
            &ledger_names,
            &manager_names,
        );

        let newly_removed = removed_names
            .into_iter()
            .filter(|name| self.pending_removals.insert(name.clone()))
            .collect::<Vec<_>>();
        if !newly_removed.is_empty() {
            Self::begin_removal_reallocation(strategy_mgr, self.venue);
        }
        for strategy_name in newly_removed {
            let payload = match self.snapshots.get(&strategy_name).cloned() {
                Some(payload) => Some(payload),
                None => {
                    let key = Self::redis_key(&strategy_name);
                    self.client
                        .get_json::<BatchExecRedisValue>(&key)
                        .await
                        .with_context(|| format!("load removed BatchExec config key={key}"))?
                }
            };
            if let Some(payload) = payload {
                payload.validate().with_context(|| {
                    format!("invalid removed BatchExec config strategy_name={strategy_name}")
                })?;
                self.removal_configs
                    .insert(strategy_name.clone(), payload.config);
            }
            info!(
                "BatchExec strategy removal queued: strategy_name={}",
                strategy_name
            );
        }
        Ok(())
    }

    fn unmanaged_nonzero_ledger_names(
        &self,
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        active_names: &BTreeSet<String>,
        removal_requests: &BTreeSet<String>,
    ) -> BTreeSet<String> {
        let Some(ledger) = self.position_ledger.as_ref() else {
            return BTreeSet::new();
        };
        let manager_names = {
            let manager = strategy_mgr.borrow();
            manager
                .iter_ids()
                .filter_map(|strategy_id| {
                    manager
                        .get(*strategy_id)?
                        .as_any()
                        .downcast_ref::<BatchExecStrategy>()
                        .filter(|exec| exec.exec_venue() == self.venue)
                        .map(|exec| exec.strategy_name().to_string())
                })
                .collect::<BTreeSet<_>>()
        };
        nonzero_unmanaged_ledger_names(
            &ledger.positions,
            active_names,
            removal_requests,
            &manager_names,
        )
    }

    fn begin_removal_reallocation(
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        venue: TradingVenue,
    ) -> usize {
        let strategy_ids = {
            let manager = strategy_mgr.borrow();
            manager
                .iter_ids()
                .copied()
                .filter(|strategy_id| {
                    manager.get(*strategy_id).is_some_and(|strategy| {
                        strategy
                            .as_any()
                            .downcast_ref::<BatchExecStrategy>()
                            .is_some_and(|exec| exec.exec_venue() == venue)
                    })
                })
                .collect::<Vec<_>>()
        };
        let mut paused = 0usize;
        let mut manager = strategy_mgr.borrow_mut();
        for strategy_id in strategy_ids {
            let Some(mut strategy) = manager.take(strategy_id) else {
                continue;
            };
            if let Some(exec) = strategy.as_any_mut().downcast_mut::<BatchExecStrategy>() {
                exec.begin_position_reallocation();
                paused += 1;
            }
            manager.insert(strategy);
        }
        paused
    }

    fn all_batch_exec_reconciliation_settled(
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        venue: TradingVenue,
        now_ts: i64,
    ) -> bool {
        let manager = strategy_mgr.borrow();
        let settled = manager.iter_ids().copied().all(|strategy_id| {
            manager.get(strategy_id).is_none_or(|strategy| {
                strategy
                    .as_any()
                    .downcast_ref::<BatchExecStrategy>()
                    .is_none_or(|exec| {
                        exec.exec_venue() != venue || exec.position_reconciliation_settled(now_ts)
                    })
            })
        });
        settled
    }

    fn pending_removal_symbols(
        &mut self,
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
    ) -> BTreeSet<String> {
        let mut symbols = BTreeSet::new();
        if let Some(ledger) = self.position_ledger.as_ref() {
            for strategy_name in &self.pending_removals {
                let Some(positions) = ledger.positions.get(strategy_name) else {
                    continue;
                };
                for (symbol, position_qty) in positions {
                    if position_qty.abs() > POSITION_ALLOCATION_EPS {
                        symbols.insert(symbol.clone());
                        if let Some(config) = self.removal_configs.get(strategy_name) {
                            self.close_configs
                                .entry(symbol.clone())
                                .or_insert_with(|| config.clone());
                        }
                    }
                }
            }
        }
        let manager = strategy_mgr.borrow();
        for strategy_id in manager.iter_ids().copied() {
            let Some(strategy) = manager.get(strategy_id) else {
                continue;
            };
            let Some(exec) = strategy.as_any().downcast_ref::<BatchExecStrategy>() else {
                continue;
            };
            if self.pending_removals.contains(exec.strategy_name())
                && exec
                    .virtual_position_qty()
                    .is_some_and(|qty| qty.abs() > POSITION_ALLOCATION_EPS)
            {
                let symbol = exec.exec_symbol().to_string();
                symbols.insert(symbol.clone());
                if let Some(config) = self.removal_configs.get(exec.strategy_name()) {
                    self.close_configs
                        .entry(symbol)
                        .or_insert_with(|| config.clone());
                }
            }
        }
        symbols
    }

    fn remove_pending_strategies(&mut self, strategy_mgr: &Rc<RefCell<StrategyManager>>) -> usize {
        let strategy_ids = {
            let manager = strategy_mgr.borrow();
            manager
                .iter_ids()
                .copied()
                .filter(|strategy_id| {
                    manager.get(*strategy_id).is_some_and(|strategy| {
                        strategy
                            .as_any()
                            .downcast_ref::<BatchExecStrategy>()
                            .is_some_and(|exec| {
                                self.pending_removals.contains(exec.strategy_name())
                            })
                    })
                })
                .collect::<Vec<_>>()
        };
        let mut manager = strategy_mgr.borrow_mut();
        let mut removed = 0usize;
        for strategy_id in strategy_ids {
            if manager.remove(strategy_id).is_some() {
                removed += 1;
            }
        }
        for strategy_name in std::mem::take(&mut self.pending_removals) {
            self.pending_ledger_removals.insert(strategy_name.clone());
            self.snapshots.remove(&strategy_name);
            self.removal_configs.remove(&strategy_name);
            info!(
                "BatchExec strategy removed from memory; position pending reallocation: strategy_name={}",
                strategy_name
            );
        }
        removed
    }

    fn ensure_position_close_strategies(
        &self,
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        symbols: &BTreeSet<String>,
    ) -> usize {
        let mut applied = 0usize;
        for symbol in symbols {
            let strategy_id = strategy_mgr
                .borrow_mut()
                .ensure_batch_exec_strategy_for_normalized_symbol(
                    BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME,
                    symbol,
                    self.venue,
                    self.close_config_for_symbol(symbol),
                );
            let strategy = { strategy_mgr.borrow_mut().take(strategy_id) };
            if let Some(mut strategy) = strategy {
                if let Some(exec) = strategy.as_any_mut().downcast_mut::<BatchExecStrategy>() {
                    if exec.target_qty() != Some(0.0) {
                        exec.update_target(
                            BatchExecTarget::ZERO,
                            get_timestamp_us(),
                            b"batch_exec:system_position_close".to_vec(),
                        );
                        applied += 1;
                    }
                }
                strategy_mgr.borrow_mut().insert(strategy);
            }
        }
        applied
    }

    fn persisted_position_close_symbols(&self) -> BTreeSet<String> {
        self.position_ledger
            .as_ref()
            .and_then(|ledger| {
                ledger
                    .positions
                    .get(BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME)
            })
            .into_iter()
            .flat_map(|positions| positions.iter())
            .filter_map(|(symbol, position_qty)| {
                (position_qty.abs() > POSITION_ALLOCATION_EPS).then(|| symbol.clone())
            })
            .collect()
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
        let monitor = crate::pre_trade::monitor_channel::MonitorChannel::instance();
        let mut candidates = self.collect_position_candidates(strategy_mgr, now_ts);
        let mut groups = BTreeMap::<String, Vec<PositionAllocationCandidate>>::new();
        for candidate in &candidates {
            groups
                .entry(candidate.symbol.clone())
                .or_default()
                .push(candidate.clone());
        }

        let mut missing_close_symbols = BTreeSet::new();
        for (symbol, group) in &groups {
            if group
                .iter()
                .any(|candidate| candidate.strategy_name == BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME)
            {
                continue;
            }
            let account_position_qty = monitor.get_position_qty(symbol, self.venue);
            let allocated_qty: f64 = group.iter().map(|candidate| candidate.position_qty).sum();
            let difference = account_position_qty - allocated_qty;
            let needs_initialization = group.iter().any(|candidate| !candidate.allocation_ready);
            let can_initialize = !group.iter().any(|candidate| candidate.execution_in_flight)
                && !group.iter().any(|candidate| {
                    candidate.has_virtual_position && !candidate.reconciliation_settled
                });
            let can_reconcile = group.iter().all(|candidate| candidate.reconciliation_ready);
            if (needs_initialization && !can_initialize)
                || (!needs_initialization
                    && (difference.abs() <= POSITION_ALLOCATION_EPS || !can_reconcile))
            {
                continue;
            }
            let mut proposed = group.clone();
            let residual = allocate_account_position(&mut proposed, account_position_qty);
            if residual.abs() > POSITION_ALLOCATION_EPS {
                missing_close_symbols.insert(symbol.clone());
            }
        }
        if !missing_close_symbols.is_empty() {
            self.ensure_position_close_strategies(strategy_mgr, &missing_close_symbols);
            candidates = self.collect_position_candidates(strategy_mgr, now_ts);
            groups.clear();
            for candidate in &candidates {
                groups
                    .entry(candidate.symbol.clone())
                    .or_default()
                    .push(candidate.clone());
            }
        }

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

            let residual = allocate_account_position(&mut group, account_position_qty);
            if !assign_residual_to_position_close(&mut group, residual) {
                warn!(
                    "BatchExec position allocation deferred without close strategy: symbol={} residual_qty={:.8}",
                    symbol, residual
                );
                continue;
            }
            let corrected_total: f64 = group.iter().map(|candidate| candidate.position_qty).sum();
            if let Some(close) = group.iter_mut().find(|candidate| {
                candidate.strategy_name == BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME
            }) {
                close.position_qty += account_position_qty - corrected_total;
            }
            info!(
                "BatchExec aggregate position allocation planned: symbol={} account_position_qty={:.8} previous_allocated_qty={:.8} strategies={} close_residual_qty={:.8}",
                symbol,
                account_position_qty,
                allocated_qty,
                group.len(),
                residual
            );
            plans.extend(group);
        }

        let mut next_ledger = self
            .position_ledger
            .clone()
            .unwrap_or_else(BatchExecPositionLedger::empty);
        for strategy_name in &self.pending_ledger_removals {
            next_ledger.remove_strategy(strategy_name);
        }
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

        self.pending_ledger_removals.clear();

        Self::apply_position_allocations(strategy_mgr, &plans, now_ts)
    }

    pub async fn reload(&mut self, strategy_mgr: &Rc<RefCell<StrategyManager>>) -> Result<usize> {
        self.load_position_ledger().await?;
        let mut applied = 0usize;
        let strategy_names = self
            .client
            .get_json::<Vec<String>>(STRATEGY_NAMES_KEY)
            .await
            .with_context(|| format!("load Redis key {STRATEGY_NAMES_KEY}"))?
            .unwrap_or_default();
        let removed_strategy_names = self
            .client
            .get_json::<Vec<String>>(REMOVED_STRATEGY_NAMES_KEY)
            .await
            .with_context(|| format!("load Redis key {REMOVED_STRATEGY_NAMES_KEY}"))?
            .unwrap_or_default();
        let mut removal_requests = BTreeSet::new();
        for name in removed_strategy_names {
            validate_config_strategy_name(&name)?;
            if !removal_requests.insert(name.clone()) {
                anyhow::bail!("duplicate strategy_name in {REMOVED_STRATEGY_NAMES_KEY}: {name}");
            }
        }
        let mut active_names = BTreeSet::new();
        for name in strategy_names {
            validate_config_strategy_name(&name)?;
            if !active_names.insert(name.clone()) {
                anyhow::bail!("duplicate strategy_name in {STRATEGY_NAMES_KEY}: {name}");
            }
        }
        for strategy_name in active_names
            .intersection(&removal_requests)
            .cloned()
            .collect::<Vec<_>>()
        {
            warn!(
                "BatchExec explicit removal overrides active index: strategy_name={}",
                strategy_name
            );
            active_names.remove(&strategy_name);
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

        self.queue_removed_strategies(strategy_mgr, &removal_requests)
            .await?;

        for (strategy_name, key, payload, targets) in loaded {
            if self.pending_removals.contains(&strategy_name) {
                continue;
            }
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
                self.close_configs
                    .entry(symbol.clone())
                    .or_insert_with(|| payload.config.clone());
                let target = targets
                    .get(&symbol)
                    .copied()
                    .unwrap_or(BatchExecTarget::ZERO);
                let old_target = previous_targets.get(&symbol).copied();
                let target_changed = old_target != Some(target);

                let strategy_id = strategy_mgr
                    .borrow_mut()
                    .ensure_batch_exec_strategy_for_normalized_symbol(
                        &strategy_name,
                        &symbol,
                        self.venue,
                        payload.config.clone(),
                    );
                let strategy = { strategy_mgr.borrow_mut().take(strategy_id) };
                if let Some(mut strategy) = strategy {
                    if let Some(exec) = strategy.as_any_mut().downcast_mut::<BatchExecStrategy>() {
                        exec.set_source_updated_at_us(payload.updated_at_us.unwrap_or(0));
                        if target_changed {
                            exec.update_target(target, get_timestamp_us(), key.as_bytes().to_vec());
                            applied += 1;
                        }
                    }
                    strategy_mgr.borrow_mut().insert(strategy);
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

        let unmanaged_ledger_names =
            self.unmanaged_nonzero_ledger_names(strategy_mgr, &active_names, &removal_requests);
        if !unmanaged_ledger_names.is_empty() {
            anyhow::bail!(
                "BatchExec position ledger contains non-zero strategies without an explicit removal request: {}; use DELETE /api/strategy?name=<strategy_name>",
                unmanaged_ledger_names
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }

        if !self.pending_removals.is_empty() {
            Self::begin_removal_reallocation(strategy_mgr, self.venue);
            let now_ts = get_timestamp_us();
            if crate::pre_trade::monitor_channel::MonitorChannel::instance()
                .exec_position_snapshot_ready()
                && Self::all_batch_exec_reconciliation_settled(strategy_mgr, self.venue, now_ts)
            {
                let close_symbols = self.pending_removal_symbols(strategy_mgr);
                applied += self.remove_pending_strategies(strategy_mgr);
                applied += self.ensure_position_close_strategies(strategy_mgr, &close_symbols);
            }
            if !self.pending_removals.is_empty() {
                return Ok(applied);
            }
        }

        let persisted_close_symbols = self.persisted_position_close_symbols();
        applied += self.ensure_position_close_strategies(strategy_mgr, &persisted_close_symbols);

        applied += self.reconcile_position_allocations(strategy_mgr).await?;
        Ok(applied)
    }

    pub fn spawn(mut self, strategy_mgr: Rc<RefCell<StrategyManager>>, interval: Duration) {
        let notify = crate::pre_trade::batch_exec_reload_notify::BatchExecReloadNotify::try_open();
        tokio::task::spawn_local(async move {
            let mut timer = tokio::time::interval(interval);
            timer.tick().await;
            loop {
                if let Some(wakeup) = notify.as_ref().and_then(|channel| channel.drain()) {
                    info!(
                        "BatchExec reload notify received: strategy_name={} updated_at_us={}",
                        wakeup.strategy_name, wakeup.updated_at_us
                    );
                    if let Err(err) = self.reload(&strategy_mgr).await {
                        warn!("BatchExec Redis reload failed after notify: {err:#}");
                    }
                    continue;
                }
                if notify.is_some() {
                    tokio::select! {
                        _ = timer.tick() => {
                            if let Err(err) = self.reload(&strategy_mgr).await {
                                warn!("BatchExec Redis reload failed: {err:#}");
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                    }
                } else {
                    timer.tick().await;
                    if let Err(err) = self.reload(&strategy_mgr).await {
                        warn!("BatchExec Redis reload failed: {err:#}");
                    }
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
        assert_eq!(
            value.targets.get("BTCUSDT"),
            Some(&BatchExecTarget {
                qty: 0.02,
                signal: 0
            })
        );
        assert_eq!(value.config.orders_per_batch, 3);
        assert_eq!(value.config.max_batch, 20);
        assert_eq!(value.updated_at_us, None);
    }

    #[test]
    fn redis_value_accepts_target_objects_and_omitted_signal() {
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
                "targets": {
                    "BTCUSDT": {"qty": 0.03, "signal": -1},
                    "ETHUSDT": {"qty": -0.5}
                }
            }"#,
        )
        .unwrap();
        value.validate().unwrap();
        assert_eq!(
            value.targets.get("BTCUSDT"),
            Some(&BatchExecTarget {
                qty: 0.03,
                signal: -1
            })
        );
        assert_eq!(
            value.targets.get("ETHUSDT"),
            Some(&BatchExecTarget {
                qty: -0.5,
                signal: 0
            })
        );
    }

    #[test]
    fn redis_value_rejects_unknown_target_signal() {
        let err = serde_json::from_str::<BatchExecRedisValue>(
            r#"{
                "single_order_usdt": 100.0,
                "orders_per_batch": 3,
                "maker_price_anchor": "own_best",
                "tick_spacing": 2,
                "batch_interval_ms": 500,
                "maker_timeout_ms": 1000,
                "max_maker_requotes": 2,
                "target_tolerance_usdt": 10.0,
                "targets": {"BTCUSDT": {"qty": 0.03, "signal": 3}}
            }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("signal must be one of"));
    }

    #[test]
    fn redis_value_accepts_and_validates_source_update_time() {
        let mut value: BatchExecRedisValue = serde_json::from_str(
            r#"{
                "single_order_usdt": 100.0,
                "orders_per_batch": 3,
                "maker_price_anchor": "own_best",
                "tick_spacing": 2,
                "batch_interval_ms": 500,
                "maker_timeout_ms": 1000,
                "max_maker_requotes": 2,
                "target_tolerance_usdt": 10.0,
                "targets": {"BTCUSDT": 0.02},
                "updated_at_us": 1700000000000001
            }"#,
        )
        .unwrap();
        value.validate().unwrap();
        assert_eq!(value.updated_at_us, Some(1_700_000_000_000_001));

        value.updated_at_us = Some(0);
        assert!(value.validate().is_err());
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
    fn unmatched_position_is_returned_for_system_close() {
        let mut candidates = vec![
            allocation_candidate(1, "cta_a", -1.0, 0.0, true),
            allocation_candidate(2, "cta_b", -2.0, 0.0, true),
        ];

        let residual = allocate_account_position(&mut candidates, -5.0);

        assert!((candidates[0].position_qty + 1.0).abs() < POSITION_ALLOCATION_EPS);
        assert!((candidates[1].position_qty + 2.0).abs() < POSITION_ALLOCATION_EPS);
        assert!((residual + 2.0).abs() < POSITION_ALLOCATION_EPS);
    }

    #[test]
    fn system_close_receives_only_unmatched_residual() {
        let mut candidates = vec![
            allocation_candidate(1, "cta_a", -1.0, 0.0, true),
            allocation_candidate(2, BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME, 0.0, 0.0, true),
        ];

        let residual = allocate_account_position(&mut candidates, -3.0);
        assert!(assign_residual_to_position_close(&mut candidates, residual));

        assert!((candidates[0].position_qty + 1.0).abs() < POSITION_ALLOCATION_EPS);
        assert!((candidates[1].position_qty + 2.0).abs() < POSITION_ALLOCATION_EPS);
        assert!(
            (candidates
                .iter()
                .map(|candidate| candidate.position_qty)
                .sum::<f64>()
                + 3.0)
                .abs()
                < POSITION_ALLOCATION_EPS
        );
    }

    #[test]
    fn validates_strategy_name_for_redis_keys() {
        validate_strategy_name("cta.alpha_01").unwrap();
        assert!(validate_strategy_name("").is_err());
        assert!(validate_strategy_name("cta:alpha").is_err());
        assert!(validate_strategy_name("removed_strategy_names").is_err());
        validate_strategy_name(BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME).unwrap();
        assert!(validate_config_strategy_name(BATCH_EXEC_POSITION_CLOSE_STRATEGY_NAME).is_err());
    }

    #[test]
    fn only_explicit_removal_requests_select_runtime_state() {
        let removal_requests = BTreeSet::from(["cta_removed".to_string()]);
        let snapshot_names = BTreeSet::from([
            "cta_removed".to_string(),
            "cta_stopped_but_not_removed".to_string(),
        ]);

        assert_eq!(
            select_requested_removals(
                &removal_requests,
                &snapshot_names,
                &BTreeSet::new(),
                &BTreeSet::new(),
            ),
            removal_requests
        );
    }

    #[test]
    fn unmanaged_nonzero_ledger_requires_explicit_removal() {
        let positions = BTreeMap::from([
            (
                "cta_orphan".to_string(),
                BTreeMap::from([("BTCUSDT".to_string(), 0.5)]),
            ),
            (
                "cta_zero".to_string(),
                BTreeMap::from([("ETHUSDT".to_string(), 0.0)]),
            ),
        ]);

        assert_eq!(
            nonzero_unmanaged_ledger_names(
                &positions,
                &BTreeSet::new(),
                &BTreeSet::new(),
                &BTreeSet::new(),
            ),
            BTreeSet::from(["cta_orphan".to_string()])
        );
        assert!(nonzero_unmanaged_ledger_names(
            &positions,
            &BTreeSet::new(),
            &BTreeSet::from(["cta_orphan".to_string()]),
            &BTreeSet::new(),
        )
        .is_empty());
    }
}
