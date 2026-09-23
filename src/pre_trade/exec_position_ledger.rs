use crate::pre_trade::exec_algorithm_switch::{ExecAlgorithmSwitch, ExecFamily, ExecSwitchState};
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::PersistChannel;
use crate::strategy::batch_exec_strategy::BatchExecStrategy;
use crate::strategy::chase_exec_strategy::ChaseExecStrategy;
use crate::strategy::StrategyManager;
use anyhow::{Context, Result};
use log::{info, warn};
use order_common::TradingVenue;
use runtime_common::redis_client::RedisClient;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use trade_signal::MktChannel;

const POSITION_LEDGER_VERSION: u32 = 1;
const POSITION_EPS: f64 = 1e-10;
const CROSS_MAX_QUOTE_AGE_US: i64 = 5_000_000;

#[derive(Clone)]
struct CrossCandidate {
    strategy_id: i32,
    family: ExecFamily,
    symbol: String,
    gap: f64,
}

#[derive(Debug, PartialEq)]
struct CrossPair {
    buy_id: i32,
    sell_id: i32,
    qty: f64,
}

fn match_opposite_families(buys: &[&CrossCandidate], sells: &[&CrossCandidate]) -> Vec<CrossPair> {
    let mut pairs = Vec::new();
    let (mut buy_index, mut sell_index) = (0, 0);
    let (mut buy_remaining, mut sell_remaining) = (0.0, 0.0);
    while buy_index < buys.len() && sell_index < sells.len() {
        if buy_remaining <= POSITION_EPS {
            buy_remaining = buys[buy_index].gap;
        }
        if sell_remaining <= POSITION_EPS {
            sell_remaining = -sells[sell_index].gap;
        }
        let qty = buy_remaining.min(sell_remaining);
        if qty > POSITION_EPS {
            pairs.push(CrossPair {
                buy_id: buys[buy_index].strategy_id,
                sell_id: sells[sell_index].strategy_id,
                qty,
            });
        }
        buy_remaining -= qty;
        sell_remaining -= qty;
        if buy_remaining <= POSITION_EPS {
            buy_index += 1;
        }
        if sell_remaining <= POSITION_EPS {
            sell_index += 1;
        }
    }
    pairs
}

fn plan_cross_family_pairs(candidates: &[CrossCandidate]) -> BTreeMap<String, Vec<CrossPair>> {
    let mut grouped: BTreeMap<String, Vec<&CrossCandidate>> = BTreeMap::new();
    for candidate in candidates {
        grouped
            .entry(candidate.symbol.clone())
            .or_default()
            .push(candidate);
    }
    let mut plans = BTreeMap::new();
    for (symbol, group) in grouped {
        let select = |family, buy| {
            group
                .iter()
                .copied()
                .filter(|candidate| {
                    candidate.family == family
                        && if buy {
                            candidate.gap > POSITION_EPS
                        } else {
                            candidate.gap < -POSITION_EPS
                        }
                })
                .collect::<Vec<_>>()
        };
        let mut pairs = match_opposite_families(
            &select(ExecFamily::BatchExec, true),
            &select(ExecFamily::ChaseExec, false),
        );
        pairs.extend(match_opposite_families(
            &select(ExecFamily::ChaseExec, true),
            &select(ExecFamily::BatchExec, false),
        ));
        if !pairs.is_empty() {
            plans.insert(symbol, pairs);
        }
    }
    plans
}

pub fn cross_family_unexecuted_targets(
    strategy_mgr: &Rc<RefCell<StrategyManager>>,
    venue: TradingVenue,
) -> usize {
    if !MonitorChannel::instance().exec_position_snapshot_ready() {
        return 0;
    }
    let now_ts = get_timestamp_us();
    let candidates = {
        let manager = strategy_mgr.borrow();
        manager
            .iter_ids()
            .copied()
            .filter_map(|strategy_id| {
                let strategy = manager.get(strategy_id)?;
                let (family, symbol, target, position, ready) = if let Some(exec) =
                    strategy.as_any().downcast_ref::<BatchExecStrategy>()
                {
                    (
                        ExecFamily::BatchExec,
                        exec.exec_symbol(),
                        exec.target_qty(),
                        exec.virtual_position_qty(),
                        exec.exec_venue() == venue && exec.position_allocation_ready(),
                    )
                } else if let Some(exec) = strategy.as_any().downcast_ref::<ChaseExecStrategy>() {
                    (
                        ExecFamily::ChaseExec,
                        exec.exec_symbol(),
                        exec.target_qty(),
                        exec.virtual_position_qty(),
                        exec.exec_venue() == venue && exec.position_allocation_ready(),
                    )
                } else {
                    return None;
                };
                let (Some(target), Some(position)) = (target, position) else {
                    return None;
                };
                ready.then(|| CrossCandidate {
                    strategy_id,
                    family,
                    symbol: symbol.to_string(),
                    gap: target - position,
                })
            })
            .collect::<Vec<_>>()
    };
    let plans = plan_cross_family_pairs(&candidates);
    let mut applied = 0;
    let mut last_publish_ts_us = 0;
    for (symbol, pairs) in plans {
        let symbol_not_tradable = MonitorChannel::instance()
            .try_venue_min_qty_table(venue)
            .is_some_and(|table| table.snapshot_loaded() && !table.is_tradable_symbol(&symbol));
        if symbol_not_tradable {
            continue;
        }
        let quote = MktChannel::is_initialized()
            .then(|| MktChannel::instance().get_quote(&symbol, venue))
            .flatten();
        let Some(quote) = quote else {
            continue;
        };
        if !quote.is_valid()
            || quote.ts <= 0
            || now_ts.saturating_sub(quote.ts) > CROSS_MAX_QUOTE_AGE_US
        {
            continue;
        }
        for pair in pairs {
            let mut manager = strategy_mgr.borrow_mut();
            let Some(mut buy) = manager.take(pair.buy_id) else {
                continue;
            };
            let Some(mut sell) = manager.take(pair.sell_id) else {
                manager.insert(buy);
                continue;
            };
            let can_apply = |strategy: &Box<dyn crate::strategy::Strategy>, qty| {
                if let Some(exec) = strategy.as_any().downcast_ref::<BatchExecStrategy>() {
                    exec.can_apply_internal_cross_fill(qty, &quote)
                } else if let Some(exec) = strategy.as_any().downcast_ref::<ChaseExecStrategy>() {
                    exec.can_apply_internal_cross_fill(qty, &quote)
                } else {
                    Err("internal cross strategy changed type".to_string())
                }
            };
            if let Err(err) = can_apply(&buy, pair.qty).and_then(|_| can_apply(&sell, -pair.qty)) {
                manager.insert(buy);
                manager.insert(sell);
                warn!(
                    "cross-family internal cross preflight failed: symbol={} buy_id={} sell_id={} err={}",
                    symbol, pair.buy_id, pair.sell_id, err
                );
                continue;
            }
            let apply = |strategy: &mut Box<dyn crate::strategy::Strategy>, qty| {
                if let Some(exec) = strategy.as_any_mut().downcast_mut::<BatchExecStrategy>() {
                    exec.apply_internal_cross_fill(qty, &quote, now_ts)
                } else if let Some(exec) = strategy.as_any_mut().downcast_mut::<ChaseExecStrategy>()
                {
                    exec.apply_internal_cross_fill(qty, &quote, now_ts)
                } else {
                    Err("internal cross strategy changed type".to_string())
                }
            };
            let sell_record = apply(&mut sell, -pair.qty);
            let buy_record = if sell_record.is_ok() {
                apply(&mut buy, pair.qty)
            } else {
                Err("counterparty cross failed".to_string())
            };
            manager.insert(buy);
            manager.insert(sell);
            drop(manager);
            match (buy_record, sell_record) {
                (Ok(buy_record), Ok(sell_record)) => {
                    for record in [&buy_record, &sell_record] {
                        while get_timestamp_us() <= last_publish_ts_us {
                            std::hint::spin_loop();
                        }
                        PersistChannel::with(|channel| channel.publish_uniform_order(record));
                        last_publish_ts_us = get_timestamp_us();
                    }
                    applied += 2;
                    info!(
                        "cross-family internal cross applied: symbol={} buy_id={} sell_id={} qty={:.8}",
                        symbol, pair.buy_id, pair.sell_id, pair.qty
                    );
                }
                (buy_result, sell_result) => warn!(
                    "cross-family internal cross failed: symbol={} buy_id={} sell_id={} buy={:?} sell={:?}",
                    symbol,
                    pair.buy_id,
                    pair.sell_id,
                    buy_result.err(),
                    sell_result.err()
                ),
            }
        }
    }
    applied
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExecPositionLedger {
    version: u32,
    pub(super) updated_at_us: i64,
    pub(super) positions: BTreeMap<String, BTreeMap<String, f64>>,
}

impl ExecPositionLedger {
    pub(super) fn empty() -> Self {
        Self {
            version: POSITION_LEDGER_VERSION,
            updated_at_us: 0,
            positions: BTreeMap::new(),
        }
    }

    pub(super) fn validate(&self) -> Result<()> {
        if self.version != POSITION_LEDGER_VERSION {
            anyhow::bail!(
                "unsupported Exec position ledger version: expected={} actual={}",
                POSITION_LEDGER_VERSION,
                self.version
            );
        }
        for (strategy_name, positions) in &self.positions {
            if matches!(
                strategy_name.as_str(),
                "strategy_names" | "removed_strategy_names"
            ) {
                anyhow::bail!("strategy_name is reserved: {strategy_name}");
            }
            let mut chars = strategy_name.chars();
            if !chars.next().is_some_and(|ch| ch.is_ascii_alphanumeric())
                || !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
            {
                anyhow::bail!("invalid Exec position ledger strategy_name: {strategy_name}");
            }
            for (symbol, qty) in positions {
                if symbol.is_empty() || normalize_symbol_for_internal(symbol) != *symbol {
                    anyhow::bail!(
                        "Exec position ledger symbol is not normalized: strategy_name={strategy_name} symbol={symbol}"
                    );
                }
                if !qty.is_finite() {
                    anyhow::bail!(
                        "Exec position ledger quantity must be finite: strategy_name={strategy_name} symbol={symbol}"
                    );
                }
            }
        }
        Ok(())
    }

    pub(super) fn get(&self, strategy_name: &str, symbol: &str) -> Option<f64> {
        self.positions
            .get(strategy_name)
            .and_then(|positions| positions.get(symbol))
            .copied()
    }

    pub(super) fn set(&mut self, strategy_name: &str, symbol: &str, qty: f64) {
        self.positions
            .entry(strategy_name.to_string())
            .or_default()
            .insert(symbol.to_string(), qty);
    }

    pub(super) fn remove_strategy(&mut self, strategy_name: &str) {
        self.positions.remove(strategy_name);
    }
}

pub(super) async fn other_family_positions(
    client: &mut RedisClient,
    manager: &Rc<RefCell<StrategyManager>>,
    venue: TradingVenue,
    own_family: ExecFamily,
    switches: &BTreeMap<String, ExecAlgorithmSwitch>,
) -> Result<BTreeMap<String, f64>> {
    let other_family = match own_family {
        ExecFamily::BatchExec => ExecFamily::ChaseExec,
        ExecFamily::ChaseExec => ExecFamily::BatchExec,
    };
    let key = format!("{}_state:position_allocations", other_family.namespace());
    let ledger = client
        .get_json::<ExecPositionLedger>(&key)
        .await
        .with_context(|| format!("load Redis key {key}"))?
        .unwrap_or_else(ExecPositionLedger::empty);
    ledger
        .validate()
        .with_context(|| format!("invalid Redis key {key}"))?;
    Ok(current_family_positions(
        &ledger,
        manager,
        venue,
        other_family,
        switches,
    ))
}

pub(super) fn current_family_positions(
    ledger: &ExecPositionLedger,
    manager: &Rc<RefCell<StrategyManager>>,
    venue: TradingVenue,
    family: ExecFamily,
    switches: &BTreeMap<String, ExecAlgorithmSwitch>,
) -> BTreeMap<String, f64> {
    let mut live_positions = Vec::new();
    let manager = manager.borrow();
    for strategy_id in manager.iter_ids().copied() {
        let Some(strategy) = manager.get(strategy_id) else {
            continue;
        };
        let live = match family {
            ExecFamily::BatchExec => strategy
                .as_any()
                .downcast_ref::<BatchExecStrategy>()
                .filter(|exec| exec.exec_venue() == venue)
                .and_then(|exec| {
                    exec.virtual_position_qty()
                        .map(|qty| (exec.strategy_name(), exec.exec_symbol(), qty))
                }),
            ExecFamily::ChaseExec => strategy
                .as_any()
                .downcast_ref::<ChaseExecStrategy>()
                .filter(|exec| exec.exec_venue() == venue)
                .and_then(|exec| {
                    exec.virtual_position_qty()
                        .map(|qty| (exec.strategy_name(), exec.exec_symbol(), qty))
                }),
        };
        if let Some((name, symbol, qty)) = live {
            live_positions.push((name.to_string(), symbol.to_string(), qty));
        }
    }
    sum_family_positions(ledger.positions.clone(), live_positions, family, switches)
}

fn sum_family_positions(
    mut positions: BTreeMap<String, BTreeMap<String, f64>>,
    live_positions: Vec<(String, String, f64)>,
    family: ExecFamily,
    switches: &BTreeMap<String, ExecAlgorithmSwitch>,
) -> BTreeMap<String, f64> {
    positions.retain(|name, _| !strategy_transferred(name, family, switches));
    for (name, symbol, qty) in live_positions {
        if !strategy_transferred(&name, family, switches) {
            positions.entry(name).or_default().insert(symbol, qty);
        }
    }
    let mut totals = BTreeMap::new();
    for strategy_positions in positions.values() {
        for (symbol, qty) in strategy_positions {
            *totals.entry(symbol.clone()).or_insert(0.0) += qty;
        }
    }
    totals
}

fn strategy_transferred(
    name: &str,
    family: ExecFamily,
    switches: &BTreeMap<String, ExecAlgorithmSwitch>,
) -> bool {
    switches.get(name).is_some_and(|switch| {
        switch.from_family == family
            && matches!(
                switch.state,
                ExecSwitchState::Ready | ExecSwitchState::Activated | ExecSwitchState::Completed
            )
    })
}

pub(super) fn outgoing_switch_symbols(
    family: ExecFamily,
    ledger: Option<&ExecPositionLedger>,
    live_symbols: impl IntoIterator<Item = (String, String)>,
    switches: &BTreeMap<String, ExecAlgorithmSwitch>,
) -> BTreeSet<String> {
    let outgoing = switches
        .iter()
        .filter(|(_, switch)| {
            switch.from_family == family
                && matches!(
                    switch.state,
                    ExecSwitchState::Requested
                        | ExecSwitchState::Ready
                        | ExecSwitchState::Activated
                )
        })
        .collect::<BTreeMap<_, _>>();
    let mut symbols = BTreeSet::new();
    for (name, switch) in &outgoing {
        symbols.extend(switch.positions.keys().cloned());
        if let Some(positions) = ledger.and_then(|ledger| ledger.positions.get(name.as_str())) {
            symbols.extend(positions.keys().cloned());
        }
    }
    for (name, symbol) in live_symbols {
        if outgoing.contains_key(&name) {
            symbols.insert(symbol);
        }
    }
    symbols
}

pub(super) fn family_account_qty(
    account_qty: f64,
    other_family_positions: &BTreeMap<String, f64>,
    symbol: &str,
) -> f64 {
    account_qty - other_family_positions.get(symbol).copied().unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subtracts_only_the_other_familys_position() {
        let other = BTreeMap::from([("SOLUSDT".to_string(), -5.0)]);
        assert_eq!(family_account_qty(7.0, &other, "SOLUSDT"), 12.0);
        assert_eq!(family_account_qty(7.0, &other, "ETHUSDT"), 7.0);
    }

    #[test]
    fn reads_existing_position_ledger_format() {
        let ledger: ExecPositionLedger = serde_json::from_str(
            r#"{"version":1,"updated_at_us":42,"positions":{"CTA_TOP_V1":{"SOLUSDT":5.0}}}"#,
        )
        .unwrap();
        ledger.validate().unwrap();
        assert_eq!(ledger.get("CTA_TOP_V1", "SOLUSDT"), Some(5.0));

        let mut reserved = ledger;
        reserved
            .positions
            .insert("strategy_names".into(), BTreeMap::new());
        assert!(reserved.validate().is_err());
    }

    #[test]
    fn excludes_exported_strategy_and_prefers_live_fill_position() {
        let ledger = BTreeMap::from([
            (
                "rbf_big".to_string(),
                BTreeMap::from([("SOLUSDT".to_string(), -7.0)]),
            ),
            (
                "CTA_TOP_V1".to_string(),
                BTreeMap::from([("SOLUSDT".to_string(), 10.0)]),
            ),
        ]);
        let switches = BTreeMap::from([(
            "rbf_big".to_string(),
            ExecAlgorithmSwitch {
                from_family: ExecFamily::BatchExec,
                to_family: ExecFamily::ChaseExec,
                state: ExecSwitchState::Ready,
                requested_at_us: 10,
                updated_at_us: 20,
                positions: BTreeMap::from([("SOLUSDT".to_string(), -7.0)]),
            },
        )]);
        let totals = sum_family_positions(
            ledger,
            vec![
                ("rbf_big".into(), "SOLUSDT".into(), -7.0),
                ("CTA_TOP_V1".into(), "SOLUSDT".into(), 11.0),
            ],
            ExecFamily::BatchExec,
            &switches,
        );
        assert_eq!(totals["SOLUSDT"], 11.0);
        let ledger = ExecPositionLedger {
            version: POSITION_LEDGER_VERSION,
            updated_at_us: 20,
            positions: BTreeMap::from([
                ("rbf_big".into(), BTreeMap::from([("SOLUSDT".into(), -7.0)])),
                (
                    "CTA_TOP_V1".into(),
                    BTreeMap::from([("SOLUSDT".into(), 10.0)]),
                ),
            ]),
        };
        assert_eq!(
            outgoing_switch_symbols(
                ExecFamily::BatchExec,
                Some(&ledger),
                [("rbf_big".into(), "XRPUSDT".into())],
                &switches,
            ),
            BTreeSet::from(["SOLUSDT".into(), "XRPUSDT".into()])
        );
    }

    #[test]
    fn switch_reconciliation_uses_live_peer_position() {
        let mut manager = StrategyManager::new();
        let mut top = BatchExecStrategy::new(
            1,
            "CTA_TOP_V1",
            "SOLUSDT",
            TradingVenue::BinanceFutures,
            crate::strategy::batch_exec_strategy::BatchExecConfig::default(),
        );
        top.apply_position_allocation(11.0, 20).unwrap();
        manager.insert(Box::new(top));
        let mut rbf = BatchExecStrategy::new(
            2,
            "rbf_big",
            "SOLUSDT",
            TradingVenue::BinanceFutures,
            crate::strategy::batch_exec_strategy::BatchExecConfig::default(),
        );
        rbf.apply_position_allocation(-7.0, 20).unwrap();
        manager.insert(Box::new(rbf));
        let manager = Rc::new(RefCell::new(manager));
        let ledger = ExecPositionLedger {
            version: POSITION_LEDGER_VERSION,
            updated_at_us: 10,
            positions: BTreeMap::from([
                (
                    "CTA_TOP_V1".into(),
                    BTreeMap::from([("SOLUSDT".into(), 10.0)]),
                ),
                ("rbf_big".into(), BTreeMap::from([("SOLUSDT".into(), -7.0)])),
            ]),
        };
        let requested = BTreeMap::from([(
            "rbf_big".into(),
            ExecAlgorithmSwitch {
                from_family: ExecFamily::BatchExec,
                to_family: ExecFamily::ChaseExec,
                state: ExecSwitchState::Requested,
                requested_at_us: 10,
                updated_at_us: 10,
                positions: BTreeMap::new(),
            },
        )]);
        let current = current_family_positions(
            &ledger,
            &manager,
            TradingVenue::BinanceFutures,
            ExecFamily::BatchExec,
            &requested,
        );
        assert_eq!(current["SOLUSDT"], 4.0);

        let mut ready = requested;
        ready.get_mut("rbf_big").unwrap().state = ExecSwitchState::Ready;
        let transferred = current_family_positions(
            &ledger,
            &manager,
            TradingVenue::BinanceFutures,
            ExecFamily::BatchExec,
            &ready,
        );
        assert_eq!(transferred["SOLUSDT"], 11.0);
    }

    #[test]
    fn crosses_opposite_targets_only_between_families() {
        let candidates = vec![
            CrossCandidate {
                strategy_id: 1,
                family: ExecFamily::BatchExec,
                symbol: "SOLUSDT".into(),
                gap: 5.0,
            },
            CrossCandidate {
                strategy_id: 2,
                family: ExecFamily::BatchExec,
                symbol: "SOLUSDT".into(),
                gap: -2.0,
            },
            CrossCandidate {
                strategy_id: 3,
                family: ExecFamily::ChaseExec,
                symbol: "SOLUSDT".into(),
                gap: -3.0,
            },
        ];
        assert_eq!(
            plan_cross_family_pairs(&candidates)["SOLUSDT"],
            vec![CrossPair {
                buy_id: 1,
                sell_id: 3,
                qty: 3.0
            }]
        );
    }
}
