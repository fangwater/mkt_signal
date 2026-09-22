use crate::pre_trade::exec_algorithm_switch::{
    active_names_key, load_switches, switch_key, ExecAlgorithmSwitch, ExecFamily, ExecSwitchState,
};
use crate::pre_trade::PersistChannel;
use crate::strategy::batch_exec_strategy::{BatchExecStrategy, BatchExecTarget};
use crate::strategy::chase_exec::{
    validate_chase_target, ChaseExecConfig, ChaseExecConfigOverride,
    CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME,
};
use crate::strategy::chase_exec_strategy::ChaseExecStrategy;
use crate::strategy::StrategyManager;
use account_common::BinanceAccountMode;
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
use trade_signal::MktChannel;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChaseExecRedisValue {
    #[serde(flatten)]
    pub config: ChaseExecConfig,
    pub targets: BTreeMap<String, BatchExecTarget>,
    #[serde(default)]
    pub symbol_overrides: BTreeMap<String, ChaseExecConfigOverride>,
    #[serde(default)]
    pub updated_at_us: Option<i64>,
}

impl ChaseExecRedisValue {
    fn validate(&self) -> Result<()> {
        self.config.validate().map_err(anyhow::Error::msg)?;
        if self.updated_at_us.is_some_and(|timestamp| timestamp <= 0) {
            anyhow::bail!("updated_at_us must be positive when present");
        }
        for (symbol, target) in &self.targets {
            if normalize_symbol_for_internal(symbol).is_empty() {
                anyhow::bail!("target symbol must not be empty");
            }
            validate_chase_target(target)
                .map_err(anyhow::Error::msg)
                .with_context(|| format!("invalid ChaseExec target: symbol={symbol}"))?;
        }
        self.normalized_symbol_overrides()?;
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

    fn normalized_symbol_overrides(&self) -> Result<BTreeMap<String, ChaseExecConfigOverride>> {
        let mut normalized = BTreeMap::new();
        for (raw_symbol, override_config) in &self.symbol_overrides {
            let symbol = normalize_symbol_for_internal(raw_symbol);
            if symbol.is_empty() {
                anyhow::bail!("symbol override must not be empty");
            }
            if override_config.is_empty() {
                anyhow::bail!("symbol override must replace at least one parameter: {raw_symbol}");
            }
            override_config
                .validate(&self.config)
                .map_err(anyhow::Error::msg)
                .with_context(|| format!("invalid ChaseExec symbol override: {raw_symbol}"))?;
            if normalized
                .insert(symbol.clone(), override_config.clone())
                .is_some()
            {
                anyhow::bail!(
                    "duplicate normalized symbol override: raw_symbol={raw_symbol} normalized={symbol}"
                );
            }
        }
        Ok(normalized)
    }
}

pub struct ChaseExecConfigReloader {
    client: RedisClient,
    venue: TradingVenue,
    binance_account_mode: Option<BinanceAccountMode>,
    leverage_init: ChaseExecLeverageInitState,
    leverage_confirmed_symbols: BTreeSet<String>,
    leverage_blocked_symbols: BTreeSet<String>,
    snapshots: BTreeMap<String, ChaseExecRedisValue>,
    position_ledger: Option<ChaseExecPositionLedger>,
    pending_removals: BTreeSet<String>,
    pending_ledger_removals: BTreeSet<String>,
    removal_configs: BTreeMap<String, ChaseExecConfig>,
    close_configs: BTreeMap<String, ChaseExecConfig>,
    /// Symbols where a live BatchExec strategy was seen on this venue. Both
    /// exec ledgers assume exclusive ownership of the shared account
    /// position; conflicted symbols are skipped by reconcile/cross so the
    /// same physical position is never allocated twice. Kept to warn only on
    /// transitions.
    conflicted_symbols: BTreeSet<String>,
    switching_out_symbols: BTreeSet<String>,
}

const STRATEGY_NAMES_KEY: &str = "chase_exec:strategy_names";
const REMOVED_STRATEGY_NAMES_KEY: &str = "chase_exec:removed_strategy_names";

fn chase_exec_venue_supported(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures | TradingVenue::OkexFutures
    )
}
const POSITION_LEDGER_KEY: &str = "chase_exec_state:position_allocations";
const LEVERAGE_INIT_KEY: &str = "chase_exec_state:leverage_initialized";
const POSITION_LEDGER_VERSION: u32 = 1;
const LEVERAGE_INIT_VERSION: u32 = 1;
const POSITION_ALLOCATION_EPS: f64 = 1e-10;
const INTERNAL_CROSS_MAX_QUOTE_AGE_US: i64 = 5_000_000;
const LEVERAGE_INIT_REQUEST_SPACING: Duration = Duration::from_millis(75);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChaseExecLeverageInitState {
    version: u32,
    leverage: u8,
    symbols: BTreeSet<String>,
    #[serde(default)]
    scope: ChaseExecLeverageInitScope,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChaseExecLeverageInitScope {
    venue: String,
    backend: String,
    rapidx_portfolio_id: Option<String>,
}

impl ChaseExecLeverageInitState {
    fn empty() -> Self {
        Self {
            version: LEVERAGE_INIT_VERSION,
            leverage: crate::pre_trade::leverage_guard::BATCH_EXEC_DEFAULT_LEVERAGE,
            symbols: BTreeSet::new(),
            scope: ChaseExecLeverageInitScope::default(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.version != LEVERAGE_INIT_VERSION {
            anyhow::bail!(
                "unsupported ChaseExec leverage-init version: expected={} actual={}",
                LEVERAGE_INIT_VERSION,
                self.version
            );
        }
        if self.leverage != crate::pre_trade::leverage_guard::BATCH_EXEC_DEFAULT_LEVERAGE {
            anyhow::bail!(
                "ChaseExec leverage-init target mismatch: expected={} actual={}",
                crate::pre_trade::leverage_guard::BATCH_EXEC_DEFAULT_LEVERAGE,
                self.leverage
            );
        }
        for symbol in &self.symbols {
            if symbol.is_empty() || normalize_symbol_for_internal(symbol) != *symbol {
                anyhow::bail!("ChaseExec leverage-init symbol is not normalized: {symbol}");
            }
        }
        Ok(())
    }

    fn with_scope(mut self, scope: ChaseExecLeverageInitScope) -> Self {
        self.scope = scope;
        self
    }

    fn confirmed_on_startup(&self) -> BTreeSet<String> {
        if self.scope.rapidx_portfolio_id.is_some() {
            BTreeSet::new()
        } else {
            self.symbols.clone()
        }
    }
}

impl ChaseExecLeverageInitScope {
    fn current(venue: TradingVenue) -> Result<Self> {
        let exchange = match venue {
            TradingVenue::BinanceFutures | TradingVenue::BinanceCoinFutures => {
                runtime_common::exchange::Exchange::Binance
            }
            TradingVenue::OkexFutures => runtime_common::exchange::Exchange::Okex,
            other => anyhow::bail!(
                "unsupported ChaseExec futures venue for leverage-init scope: {other:?}"
            ),
        };
        let backend = runtime_common::execution_backend::ExecBackend::for_exchange(exchange)?;
        let rapidx_portfolio_id = if backend == runtime_common::execution_backend::ExecBackend::Ltp
        {
            Some(runtime_common::execution_backend::rapidx_portfolio_id()?)
        } else {
            None
        };
        Ok(Self {
            venue: venue.data_pub_slug().to_string(),
            backend: backend.as_str().to_string(),
            rapidx_portfolio_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChaseExecPositionLedger {
    version: u32,
    updated_at_us: i64,
    positions: BTreeMap<String, BTreeMap<String, f64>>,
}

impl ChaseExecPositionLedger {
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
                "unsupported ChaseExec position ledger version: expected={} actual={}",
                POSITION_LEDGER_VERSION,
                self.version
            );
        }
        for (strategy_name, positions) in &self.positions {
            validate_strategy_name(strategy_name)?;
            for (symbol, position_qty) in positions {
                if symbol.is_empty() || normalize_symbol_for_internal(symbol) != *symbol {
                    anyhow::bail!(
                        "ChaseExec position ledger symbol is not normalized: strategy_name={strategy_name} symbol={symbol}"
                    );
                }
                if !position_qty.is_finite() {
                    anyhow::bail!(
                        "ChaseExec position ledger quantity must be finite: strategy_name={strategy_name} symbol={symbol}"
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
    has_target: bool,
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
    if name == CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME {
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
        .filter(|(_, candidate)| candidate.strategy_name != CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME)
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
        .find(|candidate| candidate.strategy_name == CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME)
    else {
        return false;
    };
    close.position_qty += residual;
    true
}

fn reconcile_untradable_position(
    candidates: &mut [PositionAllocationCandidate],
    account_position_qty: f64,
) {
    if account_position_qty.abs() <= POSITION_ALLOCATION_EPS {
        for candidate in candidates {
            candidate.position_qty = 0.0;
        }
        return;
    }

    let direction = account_position_qty.signum();
    let mut weights = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| {
            candidate.strategy_name != CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME
                && candidate.position_qty.signum() == direction
                && candidate.position_qty.abs() > POSITION_ALLOCATION_EPS
        })
        .map(|(index, candidate)| (index, candidate.position_qty.abs()))
        .collect::<Vec<_>>();
    if weights.is_empty() {
        weights = candidates
            .iter()
            .enumerate()
            .filter(|(_, candidate)| {
                candidate.strategy_name != CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME
                    && candidate.target_qty.signum() == direction
                    && candidate.target_qty.abs() > POSITION_ALLOCATION_EPS
            })
            .map(|(index, candidate)| (index, candidate.target_qty.abs()))
            .collect();
    }

    for candidate in candidates.iter_mut() {
        candidate.position_qty = 0.0;
    }
    let total_weight = weights.iter().map(|(_, weight)| *weight).sum::<f64>();
    if total_weight <= POSITION_ALLOCATION_EPS {
        let fallback_index = candidates
            .iter()
            .position(|candidate| {
                candidate.strategy_name != CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME
            })
            .unwrap_or(0);
        if let Some(candidate) = candidates.get_mut(fallback_index) {
            candidate.position_qty = account_position_qty;
        }
        return;
    }

    let mut allocated = 0.0;
    for (offset, (index, weight)) in weights.iter().enumerate() {
        let qty = if offset + 1 == weights.len() {
            account_position_qty - allocated
        } else {
            account_position_qty * *weight / total_weight
        };
        candidates[*index].position_qty = qty;
        allocated += qty;
    }
}

fn proportional_cross_legs(side: &[(i32, f64)], cross_qty: f64) -> Vec<(i32, f64)> {
    let total: f64 = side.iter().map(|(_, qty)| qty).sum();
    if cross_qty <= POSITION_ALLOCATION_EPS || total <= POSITION_ALLOCATION_EPS {
        return Vec::new();
    }
    let mut legs = Vec::with_capacity(side.len());
    let mut allocated = 0.0;
    for (index, (strategy_id, qty)) in side.iter().enumerate() {
        let leg = if index + 1 == side.len() {
            cross_qty - allocated
        } else {
            *qty * cross_qty / total
        };
        if leg > POSITION_ALLOCATION_EPS {
            legs.push((*strategy_id, leg));
            allocated += leg;
        }
    }
    legs
}

/// Plans signed internal-cross legs for one symbol group: positive legs buy
/// into a long gap, negative legs sell into a short gap. Only strategies with
/// a live target and an applied ledger position participate, and the matched
/// amount is the smaller of the two opposite unexecuted totals.
fn plan_internal_cross_legs(group: &[PositionAllocationCandidate]) -> Vec<(i32, f64)> {
    let mut buy_side = Vec::new();
    let mut sell_side = Vec::new();
    for candidate in group {
        if !candidate.has_target || !candidate.allocation_ready || !candidate.has_virtual_position {
            continue;
        }
        let unexecuted = candidate.target_qty - candidate.position_qty;
        if unexecuted > POSITION_ALLOCATION_EPS {
            buy_side.push((candidate.strategy_id, unexecuted));
        } else if unexecuted < -POSITION_ALLOCATION_EPS {
            sell_side.push((candidate.strategy_id, -unexecuted));
        }
    }
    let total_buy: f64 = buy_side.iter().map(|(_, qty)| qty).sum();
    let total_sell: f64 = sell_side.iter().map(|(_, qty)| qty).sum();
    let cross_qty = total_buy.min(total_sell);
    if cross_qty <= POSITION_ALLOCATION_EPS {
        return Vec::new();
    }
    let mut legs = proportional_cross_legs(&buy_side, cross_qty);
    legs.extend(
        proportional_cross_legs(&sell_side, cross_qty)
            .into_iter()
            .map(|(strategy_id, qty)| (strategy_id, -qty)),
    );
    legs
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
            strategy_name.as_str() != CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME
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

impl ChaseExecConfigReloader {
    pub async fn connect(
        redis: RedisSettings,
        venue: TradingVenue,
        binance_account_mode: Option<BinanceAccountMode>,
    ) -> Result<Self> {
        let mut client = RedisClient::connect(redis).await?;
        let loaded_leverage_init = client
            .get_json::<ChaseExecLeverageInitState>(LEVERAGE_INIT_KEY)
            .await
            .with_context(|| format!("load Redis key {LEVERAGE_INIT_KEY}"))?
            .unwrap_or_else(ChaseExecLeverageInitState::empty);
        loaded_leverage_init
            .validate()
            .with_context(|| format!("invalid ChaseExec leverage-init key={LEVERAGE_INIT_KEY}"))?;
        let scope = ChaseExecLeverageInitScope::current(venue)?;
        let leverage_init = if loaded_leverage_init.scope == scope {
            loaded_leverage_init
        } else {
            info!(
                "ChaseExec leverage-init scope changed; clearing audit symbols: previous_venue={} previous_backend={} venue={} backend={}",
                loaded_leverage_init.scope.venue,
                loaded_leverage_init.scope.backend,
                scope.venue,
                scope.backend,
            );
            ChaseExecLeverageInitState::empty().with_scope(scope)
        };
        info!(
            "ChaseExec leverage-init audit loaded: venue={} backend={} leverage={} initialized_symbols={}; RapidX symbols require process-start confirmation",
            leverage_init.scope.venue,
            leverage_init.scope.backend,
            leverage_init.leverage,
            leverage_init.symbols.len()
        );
        let leverage_confirmed_symbols = leverage_init.confirmed_on_startup();
        Ok(Self {
            client,
            venue,
            binance_account_mode,
            leverage_init,
            leverage_confirmed_symbols,
            leverage_blocked_symbols: BTreeSet::new(),
            snapshots: BTreeMap::new(),
            position_ledger: None,
            pending_removals: BTreeSet::new(),
            pending_ledger_removals: BTreeSet::new(),
            removal_configs: BTreeMap::new(),
            close_configs: BTreeMap::new(),
            conflicted_symbols: BTreeSet::new(),
            switching_out_symbols: BTreeSet::new(),
        })
    }

    fn redis_key(strategy_name: &str) -> String {
        format!("chase_exec:{strategy_name}")
    }

    fn close_config_for_symbol(&self, symbol: &str) -> ChaseExecConfig {
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
            .get_json::<ChaseExecPositionLedger>(POSITION_LEDGER_KEY)
            .await
            .with_context(|| format!("load Redis key {POSITION_LEDGER_KEY}"))?
            .unwrap_or_else(ChaseExecPositionLedger::empty);
        ledger.validate().with_context(|| {
            format!("invalid ChaseExec position ledger key={POSITION_LEDGER_KEY}")
        })?;
        info!(
            "ChaseExec position ledger loaded: strategies={} updated_at_us={}",
            ledger.positions.len(),
            ledger.updated_at_us
        );
        self.position_ledger = Some(ledger);
        Ok(())
    }

    fn begin_requested_switches(
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        venue: TradingVenue,
        switches: &BTreeMap<String, ExecAlgorithmSwitch>,
    ) {
        let requested = switches
            .iter()
            .filter(|(_, switch)| {
                switch.from_family == ExecFamily::ChaseExec
                    && switch.state == ExecSwitchState::Requested
            })
            .map(|(name, _)| name.as_str())
            .collect::<BTreeSet<_>>();
        if requested.is_empty() {
            return;
        }
        let strategy_ids = {
            let manager = strategy_mgr.borrow();
            manager
                .iter_ids()
                .copied()
                .filter(|strategy_id| {
                    manager.get(*strategy_id).is_some_and(|strategy| {
                        strategy
                            .as_any()
                            .downcast_ref::<ChaseExecStrategy>()
                            .is_some_and(|exec| {
                                exec.exec_venue() == venue
                                    && requested.contains(exec.strategy_name())
                            })
                    })
                })
                .collect::<Vec<_>>()
        };
        let mut manager = strategy_mgr.borrow_mut();
        for strategy_id in strategy_ids {
            if let Some(mut strategy) = manager.take(strategy_id) {
                if let Some(exec) = strategy.as_any_mut().downcast_mut::<ChaseExecStrategy>() {
                    exec.begin_position_reallocation();
                }
                manager.insert(strategy);
            }
        }
    }

    fn requested_switch_symbols(
        &self,
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        switches: &BTreeMap<String, ExecAlgorithmSwitch>,
    ) -> BTreeSet<String> {
        let requested = switches
            .iter()
            .filter(|(_, switch)| {
                switch.from_family == ExecFamily::ChaseExec
                    && switch.state == ExecSwitchState::Requested
            })
            .map(|(name, _)| name.as_str())
            .collect::<BTreeSet<_>>();
        let mut symbols = BTreeSet::new();
        if let Some(ledger) = self.position_ledger.as_ref() {
            for strategy_name in &requested {
                if let Some(positions) = ledger.positions.get(*strategy_name) {
                    symbols.extend(positions.keys().cloned());
                }
            }
        }
        let manager = strategy_mgr.borrow();
        for strategy_id in manager.iter_ids().copied() {
            if let Some(exec) = manager
                .get(strategy_id)
                .and_then(|strategy| strategy.as_any().downcast_ref::<ChaseExecStrategy>())
                .filter(|exec| {
                    exec.exec_venue() == self.venue && requested.contains(exec.strategy_name())
                })
            {
                symbols.insert(exec.exec_symbol().to_string());
            }
        }
        symbols
    }

    async fn import_ready_switches(
        &mut self,
        switches: &BTreeMap<String, ExecAlgorithmSwitch>,
    ) -> Result<BTreeMap<String, BTreeMap<String, f64>>> {
        let incoming = switches
            .iter()
            .filter(|(_, switch)| {
                switch.to_family == ExecFamily::ChaseExec && switch.state == ExecSwitchState::Ready
            })
            .map(|(name, switch)| (name.clone(), switch.positions.clone()))
            .collect::<BTreeMap<_, _>>();
        if incoming.is_empty() {
            return Ok(incoming);
        }
        let ledger = self
            .position_ledger
            .as_mut()
            .context("ChaseExec position ledger was not loaded")?;
        let mut changed = false;
        for (strategy_name, positions) in &incoming {
            for (symbol, qty) in positions {
                if ledger.get(strategy_name, symbol) != Some(*qty) {
                    ledger.set(strategy_name, symbol, *qty);
                    changed = true;
                }
            }
        }
        if changed {
            ledger.updated_at_us = get_timestamp_us();
            self.client
                .set_json(POSITION_LEDGER_KEY, ledger)
                .await
                .with_context(|| {
                    format!("import Exec switch into Redis key {POSITION_LEDGER_KEY}")
                })?;
        }
        Ok(incoming)
    }

    async fn cleanup_activated_switches(
        &mut self,
        switches: &BTreeMap<String, ExecAlgorithmSwitch>,
    ) -> Result<usize> {
        let completed = switches
            .iter()
            .filter(|(_, switch)| {
                switch.from_family == ExecFamily::ChaseExec
                    && switch.state == ExecSwitchState::Activated
            })
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let mut cleaned = 0usize;
        for strategy_name in completed {
            let mut next_ledger = self
                .position_ledger
                .clone()
                .unwrap_or_else(ChaseExecPositionLedger::empty);
            next_ledger.remove_strategy(&strategy_name);
            next_ledger.updated_at_us = get_timestamp_us();
            let mut completed_switch = switches[&strategy_name].clone();
            completed_switch.state = ExecSwitchState::Completed;
            completed_switch.updated_at_us = next_ledger.updated_at_us;
            let writes = vec![
                (
                    POSITION_LEDGER_KEY.to_string(),
                    serde_json::to_string(&next_ledger)?,
                ),
                (
                    switch_key(&strategy_name),
                    serde_json::to_string(&completed_switch)?,
                ),
            ];
            self.client
                .atomic_write(&writes, &[Self::redis_key(&strategy_name)])
                .await
                .with_context(|| {
                    format!("finish ChaseExec algorithm switch: strategy_name={strategy_name}")
                })?;
            self.position_ledger = Some(next_ledger);
            self.snapshots.remove(&strategy_name);
            cleaned += 1;
            info!("ChaseExec algorithm switch cleanup complete: strategy_name={strategy_name}");
        }
        Ok(cleaned)
    }

    async fn initialize_target_leverages(
        &mut self,
        required_symbols: &BTreeSet<String>,
    ) -> BTreeSet<String> {
        let mut blocked = BTreeSet::new();
        let mut newly_initialized = BTreeSet::new();
        for (index, symbol) in required_symbols.iter().enumerate() {
            if self.leverage_confirmed_symbols.contains(symbol) {
                continue;
            }
            match crate::pre_trade::leverage_guard::set_batch_exec_default_leverage(
                self.venue,
                symbol,
                self.binance_account_mode,
            )
            .await
            {
                Ok(()) => {
                    info!(
                        "ChaseExec default leverage set: symbol={} venue={:?} leverage={}",
                        symbol, self.venue, self.leverage_init.leverage
                    );
                    newly_initialized.insert(symbol.clone());
                    self.leverage_confirmed_symbols.insert(symbol.clone());
                }
                Err(err) => {
                    warn!(
                        "ChaseExec symbol activation blocked by leverage initialization: symbol={} venue={:?} leverage={} err={:#}",
                        symbol, self.venue, self.leverage_init.leverage, err
                    );
                    blocked.insert(symbol.clone());
                }
            }
            if self.leverage_init.scope.rapidx_portfolio_id.is_some()
                && index + 1 < required_symbols.len()
            {
                tokio::time::sleep(LEVERAGE_INIT_REQUEST_SPACING).await;
            }
        }

        if !newly_initialized.is_empty() {
            self.leverage_init
                .symbols
                .extend(newly_initialized.iter().cloned());
            if let Err(err) = self
                .client
                .set_json(LEVERAGE_INIT_KEY, &self.leverage_init)
                .await
            {
                for symbol in &newly_initialized {
                    self.leverage_init.symbols.remove(symbol);
                    self.leverage_confirmed_symbols.remove(symbol);
                    blocked.insert(symbol.clone());
                }
                warn!(
                    "ChaseExec leverage-init marker save failed; affected symbols remain blocked: key={} symbols={:?} err={:#}",
                    LEVERAGE_INIT_KEY, newly_initialized, err
                );
            } else {
                info!(
                    "ChaseExec leverage-init marker saved: key={} leverage={} new_symbols={:?} total_symbols={}",
                    LEVERAGE_INIT_KEY,
                    self.leverage_init.leverage,
                    newly_initialized,
                    self.leverage_init.symbols.len()
                );
            }
        }
        blocked
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
                        .downcast_ref::<ChaseExecStrategy>()
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
                        .get_json::<ChaseExecRedisValue>(&key)
                        .await
                        .with_context(|| format!("load removed ChaseExec config key={key}"))?
                }
            };
            if let Some(payload) = payload {
                payload.validate().with_context(|| {
                    format!("invalid removed ChaseExec config strategy_name={strategy_name}")
                })?;
                self.removal_configs
                    .insert(strategy_name.clone(), payload.config);
            }
            info!(
                "ChaseExec strategy removal queued: strategy_name={}",
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
                        .downcast_ref::<ChaseExecStrategy>()
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
                            .downcast_ref::<ChaseExecStrategy>()
                            .is_some_and(|exec| exec.exec_venue() == venue)
                    })
                })
                .collect::<Vec<_>>()
        };
        let mut manager = strategy_mgr.borrow_mut();
        let mut applied = 0usize;
        for strategy_id in strategy_ids {
            let Some(mut strategy) = manager.take(strategy_id) else {
                continue;
            };
            if let Some(exec) = strategy.as_any_mut().downcast_mut::<ChaseExecStrategy>() {
                exec.begin_position_reallocation();
                applied += 1;
            }
            manager.insert(strategy);
        }
        applied
    }

    fn all_chase_exec_reconciliation_settled(
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        venue: TradingVenue,
        now_ts: i64,
    ) -> bool {
        let manager = strategy_mgr.borrow();
        let settled = manager.iter_ids().copied().all(|strategy_id| {
            manager.get(strategy_id).is_none_or(|strategy| {
                strategy
                    .as_any()
                    .downcast_ref::<ChaseExecStrategy>()
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
            let Some(exec) = strategy.as_any().downcast_ref::<ChaseExecStrategy>() else {
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
                            .downcast_ref::<ChaseExecStrategy>()
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
                "ChaseExec strategy removed from memory; position pending reallocation: strategy_name={}",
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
                .ensure_chase_exec_strategy_for_normalized_symbol(
                    CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME,
                    symbol,
                    self.venue,
                    self.close_config_for_symbol(symbol),
                );
            let strategy = { strategy_mgr.borrow_mut().take(strategy_id) };
            if let Some(mut strategy) = strategy {
                if let Some(exec) = strategy.as_any_mut().downcast_mut::<ChaseExecStrategy>() {
                    if exec.target_qty() != Some(0.0) {
                        exec.update_target(
                            BatchExecTarget::ZERO,
                            get_timestamp_us(),
                            b"chase_exec:system_position_close".to_vec(),
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
                    .get(CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME)
            })
            .into_iter()
            .flat_map(|positions| positions.iter())
            .filter(|(_, position_qty)| position_qty.abs() > POSITION_ALLOCATION_EPS)
            .map(|(symbol, _)| symbol.clone())
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
            let Some(exec) = strategy.as_any().downcast_ref::<ChaseExecStrategy>() else {
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
                has_target: exec.target_qty().is_some(),
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

    /// Symbols with a live BatchExec strategy on this venue. The BatchExec and
    /// ChaseExec position ledgers each assume exclusive ownership of the
    /// shared account position, so a symbol present in both families must be
    /// skipped by reconcile and internal cross to avoid allocating the same
    /// physical position twice.
    fn batch_exec_symbols(
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        venue: TradingVenue,
    ) -> BTreeSet<String> {
        let manager = strategy_mgr.borrow();
        manager
            .iter_ids()
            .copied()
            .filter_map(|strategy_id| {
                manager
                    .get(strategy_id)?
                    .as_any()
                    .downcast_ref::<BatchExecStrategy>()
                    .filter(|exec| exec.exec_venue() == venue)
                    .map(|exec| exec.exec_symbol().to_string())
            })
            .collect()
    }

    /// Crosses opposite unexecuted target gaps inside each symbol group. Every
    /// leg books a synthetic internal fill at the current mid so the sum of
    /// per-strategy ledger positions stays equal to the shared account
    /// position. The next per-strategy clock cancels whatever outstanding
    /// children the shrunken gap no longer needs.
    fn net_opposite_unexecuted_targets(
        &self,
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        now_ts: i64,
    ) -> usize {
        let candidates = self.collect_position_candidates(strategy_mgr, now_ts);
        let mut groups = BTreeMap::<String, Vec<PositionAllocationCandidate>>::new();
        for candidate in candidates {
            groups
                .entry(candidate.symbol.clone())
                .or_default()
                .push(candidate);
        }
        groups.retain(|symbol, _| {
            !self.conflicted_symbols.contains(symbol)
                && !self.switching_out_symbols.contains(symbol)
        });
        let monitor = crate::pre_trade::monitor_channel::MonitorChannel::instance();
        let mut applied = 0usize;
        let mut last_publish_ts_us = 0i64;
        for (symbol, group) in groups {
            let legs = plan_internal_cross_legs(&group);
            if legs.is_empty() {
                continue;
            }
            let symbol_not_tradable = monitor
                .try_venue_min_qty_table(self.venue)
                .is_some_and(|table| table.snapshot_loaded() && !table.is_tradable_symbol(&symbol));
            if symbol_not_tradable {
                continue;
            }
            let quote = MktChannel::is_initialized()
                .then(|| MktChannel::instance().get_quote(&symbol, self.venue))
                .flatten();
            let Some(quote) = quote else {
                warn!("ChaseExec internal cross skipped: symbol={symbol} no quote");
                continue;
            };
            if !quote.is_valid()
                || quote.ts <= 0
                || now_ts.saturating_sub(quote.ts) > INTERNAL_CROSS_MAX_QUOTE_AGE_US
            {
                warn!(
                    "ChaseExec internal cross skipped: symbol={symbol} invalid_or_stale_quote bid={} ask={} quote_ts={}",
                    quote.bid, quote.ask, quote.ts
                );
                continue;
            }
            info!(
                "ChaseExec internal cross planned: symbol={} legs={:?}",
                symbol, legs
            );
            for (strategy_id, signed_qty) in legs {
                let Some(mut strategy) = strategy_mgr.borrow_mut().take(strategy_id) else {
                    continue;
                };
                let result = strategy
                    .as_any_mut()
                    .downcast_mut::<ChaseExecStrategy>()
                    .ok_or_else(|| "strategy is not ChaseExec during internal cross".to_string())
                    .and_then(|exec| exec.apply_internal_cross_fill(signed_qty, &quote, now_ts));
                strategy_mgr.borrow_mut().insert(strategy);
                match result {
                    Ok(record) => {
                        // Persist keys derive from publish-time timestamps; keep
                        // consecutive records strictly ordered.
                        while get_timestamp_us() <= last_publish_ts_us {
                            std::hint::spin_loop();
                        }
                        PersistChannel::with(|channel| {
                            channel.publish_uniform_order(&record)
                        });
                        last_publish_ts_us = get_timestamp_us();
                        applied += 1;
                    }
                    Err(err) => warn!(
                        "ChaseExec internal cross leg skipped: symbol={} strategy_id={} signed_qty={:.8} err={}",
                        symbol, strategy_id, signed_qty, err
                    ),
                }
            }
        }
        applied
    }

    fn suspend_position_allocations(
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        strategy_ids: &BTreeSet<i32>,
    ) -> Result<()> {
        let mut manager = strategy_mgr.borrow_mut();
        for strategy_id in strategy_ids {
            let Some(mut strategy) = manager.take(*strategy_id) else {
                anyhow::bail!(
                    "ChaseExec strategy disappeared while suspending position allocation: strategy_id={strategy_id}"
                );
            };
            let result = strategy
                .as_any_mut()
                .downcast_mut::<ChaseExecStrategy>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "strategy is not ChaseExec while suspending position allocation: strategy_id={strategy_id}"
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
                    "ChaseExec strategy disappeared while pausing position allocation: strategy_id={strategy_id}"
                );
            };
            let result = strategy
                .as_any_mut()
                .downcast_mut::<ChaseExecStrategy>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "strategy is not ChaseExec while pausing position allocation: strategy_id={strategy_id}"
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
                    "ChaseExec strategy disappeared while applying position allocation: strategy_id={}",
                    plan.strategy_id
                );
            };
            let result = strategy
                .as_any_mut()
                .downcast_mut::<ChaseExecStrategy>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "strategy is not ChaseExec while applying position allocation: strategy_id={}",
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
        let conflicting = Self::batch_exec_symbols(strategy_mgr, self.venue);
        for symbol in conflicting.difference(&self.conflicted_symbols) {
            warn!(
                "ChaseExec position reconcile disabled: symbol={symbol} also has live BatchExec strategies; the two exec ledgers must not both claim the shared account position"
            );
        }
        for symbol in self.conflicted_symbols.difference(&conflicting) {
            info!("ChaseExec position conflict resolved: symbol={symbol}");
        }
        self.conflicted_symbols = conflicting;
        let internal_cross_legs = self.net_opposite_unexecuted_targets(strategy_mgr, now_ts);
        if internal_cross_legs > 0 {
            info!("ChaseExec internal cross applied: legs={internal_cross_legs}");
        }
        let monitor = crate::pre_trade::monitor_channel::MonitorChannel::instance();
        let mut candidates = self.collect_position_candidates(strategy_mgr, now_ts);
        let mut groups = BTreeMap::<String, Vec<PositionAllocationCandidate>>::new();
        for candidate in &candidates {
            groups
                .entry(candidate.symbol.clone())
                .or_default()
                .push(candidate.clone());
        }
        groups.retain(|symbol, _| {
            !self.conflicted_symbols.contains(symbol)
                && !self.switching_out_symbols.contains(symbol)
        });

        let mut missing_close_symbols = BTreeSet::new();
        for (symbol, group) in &groups {
            let symbol_not_tradable = monitor
                .try_venue_min_qty_table(self.venue)
                .is_some_and(|table| table.snapshot_loaded() && !table.is_tradable_symbol(symbol));
            if symbol_not_tradable {
                continue;
            }
            if group
                .iter()
                .any(|candidate| candidate.strategy_name == CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME)
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
            groups.retain(|symbol, _| {
                !self.conflicted_symbols.contains(symbol)
                    && !self.switching_out_symbols.contains(symbol)
            });
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
            let symbol_not_tradable = monitor
                .try_venue_min_qty_table(self.venue)
                .is_some_and(|table| table.snapshot_loaded() && !table.is_tradable_symbol(&symbol));
            let untradable_allocations_need_reconciliation = symbol_not_tradable
                && group
                    .iter()
                    .any(|candidate| candidate.position_qty.abs() > POSITION_ALLOCATION_EPS);

            if needs_initialization {
                if !can_initialize {
                    continue;
                }
            } else if (!untradable_allocations_need_reconciliation
                && difference.abs() <= POSITION_ALLOCATION_EPS)
                || !can_reconcile
            {
                continue;
            }

            if symbol_not_tradable {
                reconcile_untradable_position(&mut group, account_position_qty);
                info!(
                    "ChaseExec untradable-symbol position reconciliation planned: symbol={} account_position_qty={:.8} previous_allocated_qty={:.8} strategies={}",
                    symbol,
                    account_position_qty,
                    allocated_qty,
                    group.len()
                );
                plans.extend(group);
                continue;
            }

            let residual = allocate_account_position(&mut group, account_position_qty);
            if !assign_residual_to_position_close(&mut group, residual) {
                warn!(
                    "ChaseExec position allocation deferred without close strategy: symbol={} residual_qty={:.8}",
                    symbol, residual
                );
                continue;
            }
            let corrected_total: f64 = group.iter().map(|candidate| candidate.position_qty).sum();
            if let Some(close) = group.iter_mut().find(|candidate| {
                candidate.strategy_name == CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME
            }) {
                close.position_qty += account_position_qty - corrected_total;
            }
            info!(
                "ChaseExec aggregate position allocation planned: symbol={} account_position_qty={:.8} previous_allocated_qty={:.8} strategies={} close_residual_qty={:.8}",
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
            .unwrap_or_else(ChaseExecPositionLedger::empty);
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
                        "ChaseExec failed to pause after position ledger save error: {pause_err:#}"
                    );
                }
                return Err(err).with_context(|| format!("save Redis key {POSITION_LEDGER_KEY}"));
            }
            self.position_ledger = Some(next_ledger);
        }

        self.pending_ledger_removals.clear();

        let applied = Self::apply_position_allocations(strategy_mgr, &plans, now_ts)?;
        // Strategies whose allocation was just applied become cross-eligible in
        // this same pass, so a freshly activated opposite target still nets
        // before its first child can submit.
        let post_apply_cross_legs = self.net_opposite_unexecuted_targets(strategy_mgr, now_ts);
        if post_apply_cross_legs > 0 {
            info!(
                "ChaseExec internal cross applied after allocation: legs={post_apply_cross_legs}"
            );
        }
        Ok(applied)
    }

    async fn advance_requested_switches(
        &mut self,
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        switches: &BTreeMap<String, ExecAlgorithmSwitch>,
    ) -> Result<usize> {
        if !crate::pre_trade::monitor_channel::MonitorChannel::instance()
            .exec_position_snapshot_ready()
        {
            return Ok(0);
        }
        let requested = switches
            .iter()
            .filter(|(_, switch)| {
                switch.from_family == ExecFamily::ChaseExec
                    && switch.state == ExecSwitchState::Requested
            })
            .map(|(name, switch)| (name.clone(), switch.clone()))
            .collect::<Vec<_>>();
        let mut advanced = 0usize;
        for (strategy_name, mut switch) in requested {
            let mut positions = self
                .position_ledger
                .as_ref()
                .and_then(|ledger| ledger.positions.get(&strategy_name))
                .cloned()
                .unwrap_or_default();
            let strategy_ids = {
                let manager = strategy_mgr.borrow();
                manager
                    .iter_ids()
                    .copied()
                    .filter(|strategy_id| {
                        manager.get(*strategy_id).is_some_and(|strategy| {
                            strategy
                                .as_any()
                                .downcast_ref::<ChaseExecStrategy>()
                                .is_some_and(|exec| {
                                    exec.exec_venue() == self.venue
                                        && exec.strategy_name() == strategy_name
                                })
                        })
                    })
                    .collect::<Vec<_>>()
            };
            let mut runtime_symbols = BTreeSet::new();
            let mut settled = !strategy_ids.is_empty()
                || positions
                    .values()
                    .all(|qty| qty.abs() <= POSITION_ALLOCATION_EPS);
            {
                let mut manager = strategy_mgr.borrow_mut();
                for strategy_id in &strategy_ids {
                    let Some(mut strategy) = manager.take(*strategy_id) else {
                        settled = false;
                        continue;
                    };
                    if let Some(exec) = strategy.as_any_mut().downcast_mut::<ChaseExecStrategy>() {
                        exec.begin_position_reallocation();
                        runtime_symbols.insert(exec.exec_symbol().to_string());
                        match exec.virtual_position_qty() {
                            Some(qty)
                                if exec.position_reconciliation_settled(get_timestamp_us()) =>
                            {
                                positions.insert(exec.exec_symbol().to_string(), qty);
                            }
                            _ => settled = false,
                        }
                    }
                    manager.insert(strategy);
                }
            }
            if positions.iter().any(|(symbol, qty)| {
                qty.abs() > POSITION_ALLOCATION_EPS && !runtime_symbols.contains(symbol)
            }) {
                settled = false;
            }
            if settled {
                let monitor = crate::pre_trade::monitor_channel::MonitorChannel::instance();
                let ledger = self
                    .position_ledger
                    .as_ref()
                    .context("ChaseExec position ledger was not loaded")?;
                for symbol in positions.keys() {
                    let allocated = ledger
                        .positions
                        .values()
                        .filter_map(|strategy_positions| strategy_positions.get(symbol))
                        .sum::<f64>();
                    let account = monitor.get_position_qty(symbol, self.venue);
                    if (allocated - account).abs() > POSITION_ALLOCATION_EPS {
                        warn!(
                            "ChaseExec algorithm switch waits for aggregate position reconciliation: strategy_name={} symbol={} allocated_qty={:.8} account_qty={:.8}",
                            strategy_name, symbol, allocated, account
                        );
                        settled = false;
                    }
                }
            }
            if !settled {
                continue;
            }

            switch.state = ExecSwitchState::Ready;
            switch.updated_at_us = get_timestamp_us().max(switch.requested_at_us);
            switch.positions = positions;
            switch.validate(&strategy_name)?;
            let source_names_key = active_names_key(switch.from_family);
            let destination_names_key = active_names_key(switch.to_family);
            self.client
                .atomic_move_json_index_member(
                    &source_names_key,
                    &destination_names_key,
                    &strategy_name,
                    &switch_key(&strategy_name),
                    &serde_json::to_string(&switch)?,
                )
                .await
                .with_context(|| {
                    format!("publish ready Exec algorithm switch: strategy_name={strategy_name}")
                })?;
            {
                let mut manager = strategy_mgr.borrow_mut();
                for strategy_id in strategy_ids {
                    manager.remove(strategy_id);
                }
            }
            self.snapshots.remove(&strategy_name);
            advanced += 1;
            info!(
                "ChaseExec algorithm switch ready after cancel reconciliation: strategy_name={} destination={} positions={:?}",
                strategy_name,
                switch.to_family.namespace(),
                switch.positions
            );
        }
        Ok(advanced)
    }

    async fn activate_ready_switches(
        &mut self,
        strategy_mgr: &Rc<RefCell<StrategyManager>>,
        switches: &BTreeMap<String, ExecAlgorithmSwitch>,
        active_names: &BTreeSet<String>,
    ) -> Result<usize> {
        let mut activated = 0usize;
        for (strategy_name, current) in switches {
            if current.to_family != ExecFamily::ChaseExec
                || current.state != ExecSwitchState::Ready
                || !active_names.contains(strategy_name)
                || !self.snapshots.contains_key(strategy_name)
            {
                continue;
            }
            let ready = {
                let manager = strategy_mgr.borrow();
                current.positions.iter().all(|(symbol, expected)| {
                    manager.iter_ids().copied().any(|strategy_id| {
                        manager.get(strategy_id).is_some_and(|strategy| {
                            strategy
                                .as_any()
                                .downcast_ref::<ChaseExecStrategy>()
                                .is_some_and(|exec| {
                                    exec.exec_venue() == self.venue
                                        && exec.strategy_name() == strategy_name
                                        && exec.exec_symbol() == symbol
                                        && exec.position_allocation_ready()
                                        && exec.virtual_position_qty().is_some_and(|qty| {
                                            (qty - expected).abs() <= POSITION_ALLOCATION_EPS
                                        })
                                })
                        })
                    })
                })
            };
            if !ready {
                continue;
            }
            let mut next = current.clone();
            next.state = ExecSwitchState::Activated;
            next.updated_at_us = get_timestamp_us().max(next.requested_at_us);
            self.client
                .set_json(&switch_key(strategy_name), &next)
                .await
                .with_context(|| {
                    format!("activate ChaseExec algorithm switch: strategy_name={strategy_name}")
                })?;
            activated += 1;
            info!(
                "ChaseExec algorithm switch activated: strategy_name={} source={}",
                strategy_name,
                next.from_family.namespace()
            );
        }
        Ok(activated)
    }

    pub async fn reload(&mut self, strategy_mgr: &Rc<RefCell<StrategyManager>>) -> Result<usize> {
        self.load_position_ledger().await?;
        let mut applied = 0usize;
        let switches = load_switches(&mut self.client).await?;
        self.switching_out_symbols = self.requested_switch_symbols(strategy_mgr, &switches);
        Self::begin_requested_switches(strategy_mgr, self.venue, &switches);
        let incoming_positions = self.import_ready_switches(&switches).await?;
        applied += self.cleanup_activated_switches(&switches).await?;
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
                "ChaseExec explicit removal overrides active index: strategy_name={}",
                strategy_name
            );
            active_names.remove(&strategy_name);
        }
        if !active_names.is_empty() && !chase_exec_venue_supported(self.venue) {
            anyhow::bail!(
                "ChaseExec only supports binance-futures and okex-futures; venue={:?}",
                self.venue
            );
        }

        let mut loaded = Vec::new();
        for strategy_name in active_names.iter().cloned() {
            let key = Self::redis_key(&strategy_name);
            let payload = self
                .client
                .get_json::<ChaseExecRedisValue>(&key)
                .await
                .with_context(|| format!("load Redis key {key}"))?;
            let payload = match payload {
                Some(payload) => payload,
                None => {
                    warn!("ChaseExec Redis key missing: {}", key);
                    self.snapshots
                        .get(&strategy_name)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("indexed ChaseExec config missing: {key}"))?
                }
            };
            payload
                .validate()
                .with_context(|| format!("invalid ChaseExec config key={key}"))?;
            let mut normalized_targets = payload
                .normalized_targets()
                .with_context(|| format!("invalid ChaseExec targets key={key}"))?;
            if let Some(positions) = self
                .position_ledger
                .as_ref()
                .and_then(|ledger| ledger.positions.get(&strategy_name))
            {
                for symbol in positions.keys() {
                    normalized_targets
                        .entry(symbol.clone())
                        .or_insert(BatchExecTarget::ZERO);
                }
            }
            if let Some(positions) = incoming_positions.get(&strategy_name) {
                for symbol in positions.keys() {
                    normalized_targets
                        .entry(symbol.clone())
                        .or_insert(BatchExecTarget::ZERO);
                }
            }
            let normalized_symbol_overrides = payload
                .normalized_symbol_overrides()
                .with_context(|| format!("invalid ChaseExec symbol overrides key={key}"))?;
            loaded.push((
                strategy_name,
                key,
                payload,
                normalized_targets,
                normalized_symbol_overrides,
            ));
        }

        let required_leverage_symbols = loaded
            .iter()
            .flat_map(|(_, _, _, targets, _)| targets.iter())
            .filter(|(_, target)| target.qty.abs() > POSITION_ALLOCATION_EPS)
            .map(|(symbol, _)| symbol.clone())
            .collect::<BTreeSet<_>>();
        let leverage_retry_symbols = self.leverage_blocked_symbols.clone();
        self.leverage_blocked_symbols = self
            .initialize_target_leverages(&required_leverage_symbols)
            .await;

        self.queue_removed_strategies(strategy_mgr, &removal_requests)
            .await?;

        for (strategy_name, key, payload, targets, symbol_overrides) in loaded {
            if self.pending_removals.contains(&strategy_name) {
                continue;
            }
            let previous = self.snapshots.get(&strategy_name);
            let config_changed = previous.is_none_or(|old| {
                old.config != payload.config || old.symbol_overrides != payload.symbol_overrides
            });
            let previous_targets = previous
                .map(ChaseExecRedisValue::normalized_targets)
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
                let target_changed =
                    old_target != Some(target) || leverage_retry_symbols.contains(&symbol);
                let effective_config = symbol_overrides
                    .get(&symbol)
                    .map(|override_config| override_config.apply_to(&payload.config))
                    .unwrap_or_else(|| payload.config.clone());

                if target.qty.abs() > POSITION_ALLOCATION_EPS
                    && self.leverage_blocked_symbols.contains(&symbol)
                {
                    continue;
                }

                let strategy_id = strategy_mgr
                    .borrow_mut()
                    .ensure_chase_exec_strategy_for_normalized_symbol(
                        &strategy_name,
                        &symbol,
                        self.venue,
                        effective_config,
                    );
                let strategy = { strategy_mgr.borrow_mut().take(strategy_id) };
                if let Some(mut strategy) = strategy {
                    if let Some(exec) = strategy.as_any_mut().downcast_mut::<ChaseExecStrategy>() {
                        exec.set_source_updated_at_us(payload.updated_at_us.unwrap_or(0));
                        if target_changed {
                            exec.update_target(target, get_timestamp_us(), key.as_bytes().to_vec());
                            applied += 1;
                        }
                        let switching_out = switches.get(&strategy_name).is_some_and(|switch| {
                            switch.from_family == ExecFamily::ChaseExec
                                && switch.state == ExecSwitchState::Requested
                        });
                        if switching_out {
                            if exec.virtual_position_qty().is_none() {
                                if let Some(position_qty) = self
                                    .position_ledger
                                    .as_ref()
                                    .and_then(|ledger| ledger.get(&strategy_name, &symbol))
                                {
                                    exec.apply_position_allocation(
                                        position_qty,
                                        get_timestamp_us(),
                                    )
                                    .map_err(anyhow::Error::msg)?;
                                }
                            }
                            exec.begin_position_reallocation();
                        }
                    }
                    strategy_mgr.borrow_mut().insert(strategy);
                }
            }

            if config_changed || previous_targets != targets {
                info!(
                    "ChaseExec Redis applied: strategy_name={} config_changed={} targets={} symbol_overrides={}",
                    strategy_name,
                    config_changed,
                    payload.targets.len(),
                    payload.symbol_overrides.len()
                );
            }
            self.snapshots.insert(strategy_name, payload);
        }

        self.switching_out_symbols = self.requested_switch_symbols(strategy_mgr, &switches);

        let mut ledger_exemptions = removal_requests.clone();
        ledger_exemptions.extend(
            switches
                .iter()
                .filter(|(_, switch)| switch.from_family == ExecFamily::ChaseExec)
                .map(|(name, _)| name.clone()),
        );
        let unmanaged_ledger_names =
            self.unmanaged_nonzero_ledger_names(strategy_mgr, &active_names, &ledger_exemptions);
        if !unmanaged_ledger_names.is_empty() {
            anyhow::bail!(
                "ChaseExec position ledger contains non-zero strategies without an explicit removal request: {}; use DELETE /api/strategy?name=<strategy_name>",
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
                && Self::all_chase_exec_reconciliation_settled(strategy_mgr, self.venue, now_ts)
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
        applied += self
            .advance_requested_switches(strategy_mgr, &switches)
            .await?;
        applied += self
            .activate_ready_switches(strategy_mgr, &switches, &active_names)
            .await?;
        Ok(applied)
    }

    /// Reloads on the shared exec notify (`batch_exec_pubs/reload_notify`) so
    /// Manager publishes once to wake every exec reloader; each reloads only
    /// its own Redis namespace.
    pub fn spawn(mut self, strategy_mgr: Rc<RefCell<StrategyManager>>, interval: Duration) {
        let mut notify =
            crate::pre_trade::batch_exec_reload_notify::BatchExecReloadNotify::try_open();
        tokio::task::spawn_local(async move {
            let mut timer = tokio::time::interval(interval);
            timer.tick().await;
            loop {
                if let Some(wakeup) = notify.as_ref().and_then(|channel| channel.drain()) {
                    info!(
                        "ChaseExec reload notify received: strategy_name={} updated_at_us={}",
                        wakeup.strategy_name, wakeup.updated_at_us
                    );
                    if let Err(err) = self.reload(&strategy_mgr).await {
                        warn!("ChaseExec Redis reload failed after notify: {err:#}");
                    }
                    continue;
                }
                if notify.is_some() {
                    tokio::select! {
                        _ = timer.tick() => {
                            if let Err(err) = self.reload(&strategy_mgr).await {
                                warn!("ChaseExec Redis reload failed: {err:#}");
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                    }
                } else {
                    timer.tick().await;
                    if let Err(err) = self.reload(&strategy_mgr).await {
                        warn!("ChaseExec Redis reload failed: {err:#}");
                    }
                    notify =
                        crate::pre_trade::batch_exec_reload_notify::BatchExecReloadNotify::try_open(
                        );
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chase_exec_is_limited_to_linear_futures_venues() {
        assert!(chase_exec_venue_supported(TradingVenue::BinanceFutures));
        assert!(chase_exec_venue_supported(TradingVenue::OkexFutures));
        assert!(!chase_exec_venue_supported(
            TradingVenue::BinanceCoinFutures
        ));
        assert!(!chase_exec_venue_supported(TradingVenue::BinanceMargin));
    }

    #[test]
    fn redis_value_contains_config_and_targets() {
        let value: ChaseExecRedisValue = serde_json::from_str(
            r#"{
                "batch_floor_usdt": 100.0,
                "max_batch": 4,
                "max_open_batches": 2,
                "maker_recenter_trigger_bps": 3.0,
                "maker_amend_cooldown_ms": 0,
                "maker_timeout_sec": 120,
                "target_tolerance_usdt": 10.0,
                "targets": {"BTCUSDT": 0.02}
            }"#,
        )
        .unwrap();
        assert_eq!(value.config.batch_floor_usdt, 100.0);
        assert_eq!(value.config.max_batch, 4);
        assert_eq!(value.config.max_open_batches, 2);
        assert_eq!(value.config.maker_amend_cooldown_ms, 0);
        assert_eq!(value.targets["BTCUSDT"].qty, 0.02);
        value.validate().unwrap();
    }

    #[test]
    fn redis_value_accepts_target_objects_and_omitted_signal() {
        let value: ChaseExecRedisValue = serde_json::from_str(
            r#"{
                "batch_floor_usdt": 100.0,
                "max_batch": 4,
                "max_open_batches": 2,
                "maker_recenter_trigger_bps": 0.0,
                "maker_timeout_sec": 120,
                "target_tolerance_usdt": 10.0,
                "targets": {"BTCUSDT": {"qty": -0.5, "signal": 1}, "ETHUSDT": {"qty": 0.3}}
            }"#,
        )
        .unwrap();
        assert_eq!(value.config.maker_amend_cooldown_ms, 1_000);
        assert_eq!(value.targets["BTCUSDT"].qty, -0.5);
        assert_eq!(value.targets["BTCUSDT"].signal, 1);
        assert_eq!(value.targets["ETHUSDT"].signal, 0);
        value.validate().unwrap();
    }

    #[test]
    fn redis_value_rejects_unknown_target_signal() {
        let value: Result<ChaseExecRedisValue, _> = serde_json::from_str(
            r#"{
                "batch_floor_usdt": 100.0,
                "max_batch": 4,
                "max_open_batches": 2,
                "maker_recenter_trigger_bps": 0.0,
                "maker_timeout_sec": 120,
                "target_tolerance_usdt": 10.0,
                "targets": {"BTCUSDT": {"qty": 0.5, "signal": 7}}
            }"#,
        );
        assert!(value.is_err());
    }

    #[test]
    fn redis_value_rejects_removed_anchor_parameter() {
        let value: Result<ChaseExecRedisValue, _> = serde_json::from_str(
            r#"{
                "batch_floor_usdt": 100.0,
                "max_batch": 4,
                "max_open_batches": 2,
                "maker_price_anchor": "own_best",
                "maker_recenter_trigger_bps": 0.0,
                "maker_timeout_sec": 120,
                "target_tolerance_usdt": 10.0,
                "targets": {"BTCUSDT": 0.5}
            }"#,
        );
        assert!(value.is_err());
    }

    #[test]
    fn redis_value_rejects_removed_fixed_usdt_water_level() {
        let value: Result<ChaseExecRedisValue, _> = serde_json::from_str(
            r#"{
                "single_order_usdt": 100.0,
                "max_open_usdt": 200.0,
                "maker_recenter_trigger_bps": 0.0,
                "maker_timeout_sec": 120,
                "target_tolerance_usdt": 10.0,
                "targets": {"BTCUSDT": 0.5}
            }"#,
        );
        assert!(value.is_err());
    }

    #[test]
    fn redis_value_accepts_symbol_overrides() {
        let value: ChaseExecRedisValue = serde_json::from_str(
            r#"{
                "batch_floor_usdt": 100.0,
                "max_batch": 4,
                "max_open_batches": 2,
                "maker_recenter_trigger_bps": 3.0,
                "maker_timeout_sec": 120,
                "target_tolerance_usdt": 10.0,
                "targets": {"BTCUSDT": 0.02, "ETHUSDT": 0.4},
                "symbol_overrides": {"ETHUSDT": {"batch_floor_usdt": 250.0}}
            }"#,
        )
        .unwrap();
        value.validate().unwrap();
        let overrides = value.normalized_symbol_overrides().unwrap();
        let applied = overrides["ETHUSDT"].apply_to(&value.config);
        assert_eq!(applied.batch_floor_usdt, 250.0);
        assert_eq!(applied.max_open_batches, 2);
    }

    #[test]
    fn validates_strategy_name_for_redis_keys() {
        assert!(validate_strategy_name("trade01").is_ok());
        assert!(validate_strategy_name("mm.alpha-1_x").is_ok());
        assert!(validate_strategy_name("").is_err());
        assert!(validate_strategy_name("strategy_names").is_err());
        assert!(validate_strategy_name("removed_strategy_names").is_err());
        assert!(validate_config_strategy_name(CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME).is_err());
    }

    #[test]
    fn batch_exec_symbols_detects_conflicts_scoped_to_venue() {
        let manager = Rc::new(RefCell::new(StrategyManager::new()));
        manager.borrow_mut().insert(Box::new(BatchExecStrategy::new(
            1,
            "cta_alpha",
            "BTCUSDT",
            TradingVenue::BinanceFutures,
            crate::strategy::batch_exec_strategy::BatchExecConfig::default(),
        )));
        manager.borrow_mut().insert(Box::new(BatchExecStrategy::new(
            2,
            "cta_beta",
            "ETHUSDT",
            TradingVenue::OkexFutures,
            crate::strategy::batch_exec_strategy::BatchExecConfig::default(),
        )));

        let symbols =
            ChaseExecConfigReloader::batch_exec_symbols(&manager, TradingVenue::BinanceFutures);
        assert_eq!(symbols, BTreeSet::from(["BTCUSDT".to_string()]));
    }
}
