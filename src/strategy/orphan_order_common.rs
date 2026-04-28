use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::QueryEngHub;
use crate::strategy::manager::OrphanSourceKind;
use crate::strategy::order_query_builder::build_order_query_request;
use crate::strategy::uniform_order_helper::UniformPublishCtx;
use log::{info, warn};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub struct OrphanOrderOwner {
    pub source_strategy_id: i32,
    pub source_kind: OrphanSourceKind,
    pub uniform_ctx: Option<UniformPublishCtx>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrphanQueryState {
    query_count: u8,
    ticks_until_next_query: u32,
}

pub struct OrphanOrderTracker {
    order_ids: HashSet<i64>,
    order_owners: HashMap<i64, OrphanOrderOwner>,
    query_states: HashMap<i64, OrphanQueryState>,
    initial_query_ticks: u32,
    query_base_ticks: u32,
    query_max_ticks: u32,
}

impl OrphanOrderTracker {
    pub fn new(initial_query_ticks: u32, query_base_ticks: u32, query_max_ticks: u32) -> Self {
        Self {
            order_ids: HashSet::new(),
            order_owners: HashMap::new(),
            query_states: HashMap::new(),
            initial_query_ticks,
            query_base_ticks,
            query_max_ticks,
        }
    }

    pub fn len(&self) -> usize {
        self.order_ids.len()
    }

    pub fn contains(&self, client_order_id: i64) -> bool {
        self.order_ids.contains(&client_order_id)
    }

    pub fn tracked_order_ids(&self) -> Vec<i64> {
        self.order_ids.iter().copied().collect()
    }

    pub fn owner(&self, client_order_id: i64) -> Option<OrphanOrderOwner> {
        self.order_owners.get(&client_order_id).cloned()
    }

    pub fn uniform_ctx(&self, client_order_id: i64) -> Option<UniformPublishCtx> {
        self.order_owners
            .get(&client_order_id)
            .and_then(|owner| owner.uniform_ctx.clone())
    }

    pub fn track_order_id(&mut self, client_order_id: i64) {
        if client_order_id <= 0 {
            return;
        }
        self.order_ids.insert(client_order_id);
        self.ensure_query_state(client_order_id);
    }

    pub fn track_order_with_owner(&mut self, client_order_id: i64, owner: OrphanOrderOwner) {
        if client_order_id <= 0 {
            return;
        }
        self.track_order_id(client_order_id);
        self.order_owners.entry(client_order_id).or_insert(owner);
    }

    pub fn adopt_order_owner(&mut self, client_order_id: i64, owner: OrphanOrderOwner) {
        if client_order_id <= 0 {
            return;
        }
        self.track_order_id(client_order_id);
        self.order_owners.insert(client_order_id, owner);
    }

    pub fn forget_order_id(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
        reason: &str,
    ) -> bool {
        let removed = self.order_ids.remove(&client_order_id);
        if removed {
            self.order_owners.remove(&client_order_id);
            self.query_states.remove(&client_order_id);
            info!(
                "{}: strategy_id={} forgot order_id client_order_id={} reason={}",
                strategy_role, strategy_id, client_order_id, reason
            );
        }
        removed
    }

    pub fn query_due_now(&mut self, client_order_id: i64) -> bool {
        let query_base_ticks = self.query_base_ticks;
        let query_max_ticks = self.query_max_ticks;
        let Some(query_state) = self.query_states.get_mut(&client_order_id) else {
            return false;
        };
        if query_state.ticks_until_next_query > 0 {
            query_state.ticks_until_next_query -= 1;
            return false;
        }
        let next_query_count = query_state.query_count.saturating_add(1);
        query_state.query_count = next_query_count;
        query_state.ticks_until_next_query =
            Self::next_query_ticks(query_base_ticks, query_max_ticks, next_query_count);
        true
    }

    pub fn send_order_query(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
        forget_on_missing_local_order: bool,
    ) -> bool {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "{}: strategy_id={} send_order_query missing local order client_order_id={}",
                strategy_role, strategy_id, client_order_id
            );
            if forget_on_missing_local_order {
                self.forget_order_id(
                    strategy_role,
                    strategy_id,
                    client_order_id,
                    "query missing local order",
                );
            }
            return false;
        };
        let request_query_id = client_order_id;
        match build_order_query_request(&order, request_query_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes)
                {
                    warn!(
                        "{}: strategy_id={} publish query failed client_order_id={} request_query_id={} err={:#}",
                        strategy_role, strategy_id, client_order_id, request_query_id, err
                    );
                    return false;
                }
                info!(
                    "{}: strategy_id={} query sent client_order_id={} request_query_id={}",
                    strategy_role, strategy_id, client_order_id, request_query_id
                );
                true
            }
            Err(err) => {
                warn!(
                    "{}: strategy_id={} build query failed client_order_id={} err={}",
                    strategy_role, strategy_id, client_order_id, err
                );
                false
            }
        }
    }

    pub fn finalize_terminal_order(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
        event_time: i64,
        reason: &str,
        eps: f64,
    ) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            self.forget_order_id(strategy_role, strategy_id, client_order_id, reason);
            return;
        };
        let snapshot = {
            let mgr = order_mgr.borrow();
            mgr.get(client_order_id).map(|order| {
                (
                    order.venue,
                    order.symbol.clone(),
                    order.side,
                    order.quantity,
                    order.cumulative_filled_quantity,
                    order.price,
                )
            })
        };
        let Some((venue, symbol, side, order_qty, cumulative_qty, price)) = snapshot else {
            self.forget_order_id(strategy_role, strategy_id, client_order_id, reason);
            return;
        };

        let had_owner = if let Some(owner) = self.owner(client_order_id) {
            let order_base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, order_qty);
            let cumulative_base_qty =
                MonitorChannel::instance().qty_to_base(venue, &symbol, cumulative_qty);
            let should_record = match owner.source_kind {
                OrphanSourceKind::Open => cumulative_base_qty > eps,
                OrphanSourceKind::Hedge => order_base_qty > eps || cumulative_base_qty > eps,
            };
            if should_record {
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let mut strategy_mgr = strategy_mgr.borrow_mut();
                let normalized_symbol = normalize_symbol_for_internal(&symbol);
                let recorded = match owner.source_kind {
                    OrphanSourceKind::Open => strategy_mgr.record_open_order_terminal(
                        &normalized_symbol,
                        side,
                        order_base_qty,
                        cumulative_base_qty,
                        event_time,
                        price,
                        0,
                    ),
                    OrphanSourceKind::Hedge => strategy_mgr.record_hedge_order_terminal(
                        &normalized_symbol,
                        side,
                        order_base_qty,
                        cumulative_base_qty,
                        event_time,
                        price,
                    ),
                };
                if !recorded {
                    warn!(
                        "{}: strategy_id={} record order terminal failed client_order_id={} symbol={} source_kind={:?} cumulative_base_qty={:.8} reason={}",
                        strategy_role,
                        strategy_id,
                        client_order_id,
                        normalized_symbol,
                        owner.source_kind,
                        cumulative_base_qty,
                        reason
                    );
                }
            }
            info!(
                "{}: strategy_id={} finalized order client_order_id={} source_kind={:?} symbol={} venue={:?} side={:?} order_qty={:.8} cumulative_qty={:.8} order_base_qty={:.8} cumulative_base_qty={:.8} reason={}",
                strategy_role,
                strategy_id,
                client_order_id,
                owner.source_kind,
                symbol,
                venue,
                side,
                order_qty,
                cumulative_qty,
                order_base_qty,
                cumulative_base_qty,
                reason
            );
            true
        } else {
            warn!(
                "{}: strategy_id={} finalize terminal order missing owner client_order_id={} reason={}",
                strategy_role, strategy_id, client_order_id, reason
            );
            false
        };

        if had_owner {
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
        self.forget_order_id(strategy_role, strategy_id, client_order_id, reason);
    }

    fn ensure_query_state(&mut self, client_order_id: i64) {
        self.query_states
            .entry(client_order_id)
            .or_insert_with(|| OrphanQueryState {
                query_count: 0,
                ticks_until_next_query: self.initial_query_ticks,
            });
    }

    fn next_query_ticks(query_base_ticks: u32, query_max_ticks: u32, query_count: u8) -> u32 {
        let multiplier = 1_u32
            .checked_shl(query_count.min(31) as u32)
            .unwrap_or(u32::MAX);
        query_base_ticks
            .saturating_mul(multiplier)
            .min(query_max_ticks)
    }
}
