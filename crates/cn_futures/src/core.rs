use crate::model::*;
use anyhow::{bail, ensure, Context, Result};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// Not Send/Sync: only the business-loop thread may own or mutate this state.
pub struct TradingCore {
    limits: RiskLimits,
    day: String,
    scope: ExecutionScope,
    session: String,
    next_ref: u64,
    ready: bool,
    connected: bool,
    reason: String,
    cash: f64,
    instruments: HashMap<(Venue, String), Instrument>,
    continuous: HashMap<(Venue, String), bool>,
    positions: HashMap<(Venue, String, Side), Position>,
    orders: HashMap<OrderKey, Order>,
    system_ids: HashMap<(Venue, String), OrderKey>,
    fills: HashMap<TradeIdentity, Fill>,
    actions: VecDeque<Instant>,
    _single_owner: PhantomData<Rc<()>>,
}

#[derive(Debug, Serialize)]
pub struct CoreView<'a> {
    pub ready: bool,
    pub connected: bool,
    pub reason: &'a str,
    pub trading_day: &'a str,
    pub available_local_budget: f64,
    pub orders: Vec<&'a Order>,
    pub positions: Vec<PositionView<'a>>,
}
#[derive(Debug, Serialize)]
pub struct PositionView<'a> {
    pub venue: Venue,
    pub symbol: &'a str,
    pub side: Side,
    pub position: &'a Position,
}

impl TradingCore {
    pub fn new(limits: RiskLimits, scope: ExecutionScope) -> Result<Self> {
        limits.validate()?;
        scope.validate()?;
        Ok(Self {
            limits,
            day: String::new(),
            scope,
            session: String::new(),
            next_ref: 1,
            ready: false,
            connected: false,
            reason: "not recovered".into(),
            cash: 0.0,
            instruments: HashMap::new(),
            continuous: HashMap::new(),
            positions: HashMap::new(),
            orders: HashMap::new(),
            system_ids: HashMap::new(),
            fills: HashMap::new(),
            actions: VecDeque::new(),
            _single_owner: PhantomData,
        })
    }
    pub fn is_ready(&self) -> bool {
        self.ready && self.connected
    }
    pub fn block(&mut self, reason: impl Into<String>) {
        self.ready = false;
        self.reason = reason.into();
    }
    pub fn disconnect(&mut self) {
        self.connected = false;
        self.block("native trading disconnected; reconciliation required");
    }
    pub fn connected(&mut self) {
        self.connected = true;
        self.continuous.clear();
        self.block("session/bootstrap incomplete");
    }
    pub fn order(&self, key: &OrderKey) -> Option<&Order> {
        self.orders.get(key)
    }
    pub fn position(&self, venue: Venue, symbol: &str, side: Side) -> Position {
        self.positions
            .get(&(venue, symbol.to_owned(), side))
            .cloned()
            .unwrap_or_default()
    }
    pub fn view(&self) -> CoreView<'_> {
        CoreView {
            ready: self.is_ready(),
            connected: self.connected,
            reason: &self.reason,
            trading_day: &self.day,
            available_local_budget: self.cash,
            orders: self.orders.values().collect(),
            positions: self
                .positions
                .iter()
                .map(|((venue, symbol, side), position)| PositionView {
                    venue: *venue,
                    symbol,
                    side: *side,
                    position,
                })
                .collect(),
        }
    }
    pub fn set_trading_status(&mut self, venue: Venue, symbol_or_product: &str, continuous: bool) {
        self.continuous
            .insert((venue, symbol_or_product.to_owned()), continuous);
    }
    /// Apply only a complete new rule set; omission immediately removes trading permission.
    pub fn replace_instruments(&mut self, instruments: Vec<Instrument>) -> Result<()> {
        let mut next = HashMap::new();
        for rule in instruments {
            rule.validate()?;
            ensure!(
                self.scope.venues.contains(&rule.venue)
                    && !next.contains_key(&(rule.venue, rule.symbol.clone())),
                "duplicate instrument in rule refresh"
            );
            next.insert((rule.venue, rule.symbol.clone()), rule);
        }
        self.instruments = next;
        Ok(())
    }
    /// A cash poll may tighten the budget. Only a full reconciliation can replenish it.
    pub fn constrain_cash(&mut self, available: f64) -> Result<()> {
        ensure!(available.is_finite(), "nonfinite cash refresh");
        self.cash = self.cash.min(available);
        Ok(())
    }
    fn trading_status(&self, venue: Venue, symbol: &str) -> bool {
        self.continuous
            .get(&(venue, symbol.to_owned()))
            .or_else(|| {
                let n = symbol.len().saturating_sub(4);
                symbol
                    .get(..n)
                    .and_then(|p| self.continuous.get(&(venue, p.to_owned())))
            })
            .copied()
            .unwrap_or(false)
    }

    /// Caller must supply a complete, reconciled account snapshot, never query fragments.
    /// Validation and reconstruction are atomic with respect to the existing core.
    pub fn restore(&mut self, s: Snapshot) -> Result<()> {
        validate_snapshot(&s, &self.scope)?;
        for old in self.orders.values().filter(|o| o.remaining() > 0) {
            let recovered=s.orders.iter().find(|o| o.key==old.key || (o.venue==old.request.venue && !old.system_id.is_empty() && o.system_id==old.system_id))
                .context("snapshot omits unresolved local order; preserve reservations until a factual outcome is available")?;
            ensure!(
                recovered.venue == old.request.venue
                    && recovered.symbol == old.request.symbol
                    && recovered.side == old.request.side
                    && recovered.offset == old.request.offset
                    && recovered.price_type == old.request.price_type
                    && recovered.lots == old.request.lots
                    && recovered.traded >= old.filled,
                "snapshot conflicts with known unresolved order identity/quantity"
            );
        }
        if self.day == s.trading_day {
            for known in self.fills.values() {
                ensure!(
                    s.trades.iter().any(|f| f == known),
                    "snapshot omits or changes a known factual fill"
                );
            }
        }
        let mut next = Self::new(self.limits.clone(), self.scope.clone())?;
        next.continuous = self.continuous.clone();
        next.day = s.trading_day;
        next.session = s.session;
        next.next_ref = s
            .max_order_ref
            .checked_add(1)
            .context("order ref exhausted")?;
        if self.day == next.day && self.session == next.session {
            // Login MaxOrderRef can predate locally rejected attempts and manual reconciliation.
            next.next_ref = next.next_ref.max(self.next_ref);
        }
        ensure!(
            next.next_ref <= self.scope.reference_limit,
            "native order reference exhausted"
        );
        next.cash = s.available_cash;
        next.instruments = s
            .instruments
            .into_iter()
            .map(|r| ((r.venue, r.symbol.clone()), r))
            .collect();
        for p in s.positions {
            next.positions.insert(
                (p.venue, p.symbol, p.side),
                Position {
                    today: p.today,
                    yesterday: p.yesterday,
                    ..Position::default()
                },
            );
        }
        for f in s.trades {
            next.fills.insert(f.identity.clone(), f);
        }
        for update in s.orders {
            ensure!(
                !next.orders.contains_key(&update.key),
                "duplicate order identity in snapshot"
            );
            if update.key.session == next.session {
                if let Ok(observed) = update.key.order_ref.trim().parse::<u64>() {
                    next.next_ref = next.next_ref.max(
                        observed
                            .checked_add(1)
                            .context("observed order reference exhausted")?,
                    );
                    ensure!(
                        next.next_ref <= self.scope.reference_limit,
                        "observed order reference exceeds native capacity"
                    );
                }
            }
            let remaining = if update.status.terminal() {
                0
            } else {
                update.lots - update.traded
            };
            let mut order = Order {
                request: Intent {
                    venue: update.venue,
                    symbol: update.symbol.clone(),
                    side: update.side,
                    offset: update.offset,
                    lots: update.lots,
                    price_type: update.price_type,
                    price_ticks: 0,
                    time_in_force: TimeInForce::Day,
                    execution_route: None,
                },
                key: update.key.clone(),
                system_id: update.system_id,
                limit_price: update.limit_price,
                status: update.status,
                filled: update.traded,
                reported_filled: update.traded,
                cancelled: if update.status.terminal() {
                    update.lots - update.traded
                } else {
                    0
                },
                reserved_today: 0,
                reserved_yesterday: 0,
                local_cash_held: 0.0,
                cancel_pending: false,
                owned: false,
            };
            if remaining > 0 && update.offset != Offset::Open {
                let (today, yesterday) = next.allocate_close(
                    update.venue,
                    &update.symbol,
                    update.side.opposite(),
                    update.offset,
                    remaining,
                )?;
                order.reserved_today = today;
                order.reserved_yesterday = yesterday;
                next.adjust_frozen(&order, today, yesterday)?;
            }
            if !order.system_id.is_empty() {
                ensure!(
                    !next
                        .system_ids
                        .contains_key(&(order.request.venue, order.system_id.clone())),
                    "duplicate venue system order identity"
                );
                next.system_ids.insert(
                    (order.request.venue, order.system_id.clone()),
                    order.key.clone(),
                );
            }
            next.orders.insert(order.key.clone(), order);
        }
        next.connected = true;
        next.ready = true;
        next.reason.clear();
        *self = next;
        Ok(())
    }

    fn allocate_close(
        &self,
        venue: Venue,
        symbol: &str,
        side: Side,
        offset: Offset,
        lots: i32,
    ) -> Result<(i32, i32)> {
        let p = self.position(venue, symbol, side);
        let today = p.today - p.frozen_today;
        let yesterday = p.yesterday - p.frozen_yesterday;
        ensure!(
            i64::from(lots) <= i64::from(today) + i64::from(yesterday),
            "insufficient available close position"
        );
        if offset == Offset::CloseToday {
            ensure!(lots <= today, "insufficient today position");
            return Ok((lots, 0));
        }
        if offset == Offset::CloseYesterday {
            ensure!(lots <= yesterday, "insufficient yesterday position");
            return Ok((0, lots));
        }
        Ok(match self.limits.close_priority {
            ClosePriority::TodayFirst => {
                let t = lots.min(today);
                (t, lots - t)
            }
            ClosePriority::YesterdayFirst => {
                let y = lots.min(yesterday);
                (lots - y, y)
            }
        })
    }
    fn adjust_frozen(&mut self, order: &Order, today: i32, yesterday: i32) -> Result<()> {
        let p = self
            .positions
            .entry((
                order.request.venue,
                order.request.symbol.clone(),
                order.request.side.opposite(),
            ))
            .or_default();
        p.frozen_today += today;
        p.frozen_yesterday += yesterday;
        p.validate()
    }
    fn rate_check(&mut self, now: Instant) -> Result<()> {
        while self
            .actions
            .front()
            .is_some_and(|t| now.saturating_duration_since(*t) >= Duration::from_secs(1))
        {
            self.actions.pop_front();
        }
        ensure!(
            self.actions.len() < self.limits.max_actions_per_second,
            "account action rate limit"
        );
        self.actions.push_back(now);
        Ok(())
    }

    pub fn prepare(&mut self, intent: Intent, now: Instant) -> Result<PreparedOrder> {
        if let Some(route) = intent.execution_route {
            route.validate()?;
        }
        ensure!(self.is_ready(), "trading blocked: {}", self.reason);
        ensure!(
            self.scope.venues.contains(&intent.venue),
            "venue outside execution scope"
        );
        ensure!(
            !intent.venue.separate_close_dates() || intent.offset != Offset::Close,
            "SHFE/INE close must be planned into explicit date orders"
        );
        ensure!(
            intent.venue.separate_close_dates()
                || matches!(intent.offset, Offset::Open | Offset::Close),
            "explicit close dates not supported by this adapter"
        );
        let rule = self
            .instruments
            .get(&(intent.venue, intent.symbol.clone()))
            .context("symbol absent from complete rule snapshot")?
            .clone();
        ensure!(
            rule.active
                && (!self.scope.require_market_status
                    || self.trading_status(intent.venue, &intent.symbol)),
            "symbol inactive or outside continuous trading"
        );
        let (minimum, maximum) = match intent.price_type {
            PriceType::Limit => (rule.min_lots, rule.max_lots),
            PriceType::Market => {
                let limits = rule
                    .market_lots
                    .context("native market orders unavailable for this contract/adapter")?;
                (limits.minimum, limits.maximum)
            }
        };
        ensure!(
            intent.lots >= minimum
                && intent.lots <= maximum
                && (intent.price_type == PriceType::Limit || intent.lots % minimum == 0)
                && intent.lots <= self.limits.max_order_lots,
            "invalid order quantity"
        );
        match intent.price_type {
            PriceType::Limit => {
                ensure!(intent.price_ticks > 0, "limit price must be positive ticks")
            }
            PriceType::Market => ensure!(
                intent.price_ticks == 0,
                "native market order must use zero price_ticks"
            ),
        }
        ensure!(
            intent.time_in_force == TimeInForce::Day || self.limits.enable_fak_fok,
            "FAK/FOK not enabled for this tested venue/account"
        );
        let price = intent.price_ticks as f64 * rule.price_tick;
        ensure!(
            price.is_finite()
                && (intent.price_type == PriceType::Market || price > 0.0)
                && intent.price_ticks <= 9_007_199_254_740_991,
            "limit price overflow"
        );
        let working: i64 = self.orders.values().map(|o| i64::from(o.remaining())).sum();
        ensure!(
            working + i64::from(intent.lots) <= i64::from(self.limits.max_working_lots),
            "working lot limit"
        );
        ensure!(
            !self.orders.values().any(|o| o.request.venue == intent.venue
                && o.request.symbol == intent.symbol
                && o.request.side != intent.side
                && o.remaining() > 0
                && (intent.price_type == PriceType::Market
                    || o.request.price_type == PriceType::Market
                    || match intent.side {
                        Side::Buy => price + rule.price_tick * 1e-6 >= o.limit_price,
                        Side::Sell => price - rule.price_tick * 1e-6 <= o.limit_price,
                    })),
            "self-trade prevention"
        );
        let mut today = 0;
        let mut yesterday = 0;
        let mut cash = 0.0;
        if intent.offset == Offset::Open {
            let pending: i64 = self
                .orders
                .values()
                .filter(|o| {
                    o.request.venue == intent.venue
                        && o.request.symbol == intent.symbol
                        && o.request.side == intent.side
                        && o.request.offset == Offset::Open
                })
                .map(|o| i64::from(o.remaining()))
                .sum();
            ensure!(
                self.position(intent.venue, &intent.symbol, intent.side)
                    .total()
                    + pending
                    + i64::from(intent.lots)
                    <= i64::from(self.limits.max_position_lots_per_side),
                "position limit"
            );
            cash = rule.cash_budget_per_open_lot * f64::from(intent.lots);
            ensure!(
                cash.is_finite() && self.cash - cash >= self.limits.minimum_cash_remaining,
                "local cash budget insufficient"
            );
        } else {
            (today, yesterday) = self.allocate_close(
                intent.venue,
                &intent.symbol,
                intent.side.opposite(),
                intent.offset,
                intent.lots,
            )?;
        }
        ensure!(
            self.next_ref <= self.scope.reference_limit,
            "native order reference exhausted"
        );
        self.rate_check(now)?;
        let key = OrderKey {
            session: self.session.clone(),
            order_ref: self.next_ref.to_string(),
        };
        self.next_ref += 1;
        let order = Order {
            request: intent.clone(),
            key: key.clone(),
            system_id: String::new(),
            limit_price: price,
            status: OrderStatus::Pending,
            filled: 0,
            reported_filled: 0,
            cancelled: 0,
            reserved_today: today,
            reserved_yesterday: yesterday,
            local_cash_held: cash,
            cancel_pending: false,
            owned: true,
        };
        if intent.offset != Offset::Open {
            self.adjust_frozen(&order, today, yesterday)?;
        }
        self.cash -= cash;
        self.orders.insert(key.clone(), order);
        Ok(PreparedOrder {
            key,
            intent,
            limit_price: price,
        })
    }

    pub fn reject(&mut self, key: &OrderKey) -> Result<()> {
        let mut order = self
            .orders
            .get(key)
            .context("rejection without known order")?
            .clone();
        if order.status == OrderStatus::Rejected {
            return Ok(());
        }
        ensure!(
            order.filled == 0 && order.reported_filled == 0,
            "rejection conflicts with factual or reported fill"
        );
        let lots = order.request.lots;
        self.release_cancelled(&mut order, lots)?;
        order.status = OrderStatus::Rejected;
        order.cancel_pending = false;
        self.orders.insert(key.clone(), order);
        Ok(())
    }
    fn release_cancelled(&mut self, order: &mut Order, total_cancelled: i32) -> Result<()> {
        ensure!(
            total_cancelled >= order.cancelled
                && total_cancelled <= order.request.lots - order.filled,
            "inconsistent cancelled quantity"
        );
        let release = total_cancelled - order.cancelled;
        if order.request.offset == Offset::Open {
            let unit = self
                .instruments
                .get(&(order.request.venue, order.request.symbol.clone()))
                .map(|r| r.cash_budget_per_open_lot)
                .unwrap_or(0.0);
            let refund = (f64::from(release) * unit).min(order.local_cash_held);
            order.local_cash_held -= refund;
            self.cash += refund;
        } else {
            let (today, yesterday) = match self.limits.close_priority {
                ClosePriority::TodayFirst => {
                    let y = release.min(order.reserved_yesterday);
                    (release - y, y)
                }
                ClosePriority::YesterdayFirst => {
                    let t = release.min(order.reserved_today);
                    (t, release - t)
                }
            };
            ensure!(
                today <= order.reserved_today && yesterday <= order.reserved_yesterday,
                "close cancellation exceeds frozen inventory"
            );
            self.adjust_frozen(order, -today, -yesterday)?;
            order.reserved_today -= today;
            order.reserved_yesterday -= yesterday;
        }
        order.cancelled = total_cancelled;
        Ok(())
    }

    pub fn on_order(&mut self, update: OrderUpdate) -> Result<()> {
        ensure!(
            self.scope.venues.contains(&update.venue) && update.trading_day == self.day,
            "order outside current account day/scope"
        );
        let mut order = self
            .orders
            .get(&update.key)
            .context("untracked account order requires reconciliation")?
            .clone();
        ensure!(
            order.request.venue == update.venue
                && order.request.symbol == update.symbol
                && order.request.side == update.side
                && order.request.offset == update.offset
                && order.request.price_type == update.price_type
                && order.request.lots == update.lots,
            "order identity fields changed"
        );
        ensure!(
            update.traded >= 0 && update.traded <= update.lots,
            "invalid order cumulative fill"
        );
        if !update.system_id.is_empty() {
            ensure!(
                order.system_id.is_empty() || order.system_id == update.system_id,
                "system identity changed"
            );
            if let Some(existing) = self
                .system_ids
                .get(&(update.venue, update.system_id.clone()))
            {
                ensure!(existing == &update.key, "system identity collision");
            }
            order.system_id = update.system_id;
            self.system_ids
                .insert((update.venue, order.system_id.clone()), update.key.clone());
        }
        order.reported_filled = order.reported_filled.max(update.traded).max(order.filled);
        if update.cancel_rejected {
            order.cancel_pending = false;
        }
        if update.status == OrderStatus::Rejected {
            return self.reject(&update.key);
        }
        if update.status == OrderStatus::Cancelled {
            let cancelled = order.request.lots - order.reported_filled;
            if cancelled >= order.cancelled {
                self.release_cancelled(&mut order, cancelled)?;
            }
            if order.status != OrderStatus::Filled {
                order.status = OrderStatus::Cancelled;
            }
            order.cancel_pending = false;
        } else if update.status == OrderStatus::Filled {
            ensure!(
                update.traded == order.request.lots && order.cancelled == 0,
                "all-traded conflicts with order quantities"
            );
            order.status = OrderStatus::Filled;
            order.cancel_pending = false;
        } else if !order.status.terminal() {
            order.status = update.status;
        }
        self.orders.insert(update.key, order);
        Ok(())
    }

    pub fn on_fill(&mut self, fill: Fill) -> Result<bool> {
        validate_fill(&fill, &self.day, &self.scope)?;
        if let Some(previous) = self.fills.get(&fill.identity) {
            ensure!(previous == &fill, "conflicting duplicate trade identity");
            return Ok(false);
        }
        let key = self
            .system_ids
            .get(&(fill.identity.venue, fill.system_id.clone()))
            .cloned()
            .or_else(|| {
                if fill.order_key.is_none() && !self.scope.allow_implicit_fill_reference {
                    return None;
                }
                let key = fill.order_key.clone().unwrap_or_else(|| OrderKey {
                    session: self.session.clone(),
                    order_ref: fill.order_ref.clone(),
                });
                self.orders
                    .get(&key)
                    .filter(|o| o.system_id.is_empty() || o.system_id == fill.system_id)
                    .map(|_| key)
            })
            .context("untracked factual fill: preserve evidence and reconcile, never discard it")?;
        let mut order = self
            .orders
            .get(&key)
            .context("missing correlated order")?
            .clone();
        ensure!(
            fill.order_key.as_ref().is_none_or(|k| k == &key),
            "fill logical order identity conflicts with system identity"
        );
        ensure!(
            !order.owned || fill.order_ref.is_empty() || fill.order_ref == order.key.order_ref,
            "fill order reference conflicts with exchange order identity"
        );
        ensure!(
            order.request.venue == fill.identity.venue
                && order.request.symbol == fill.identity.symbol
                && order.request.side == fill.identity.side
                && order.request.offset == fill.offset,
            "fill conflicts with order identity"
        );
        ensure!(
            fill.lots <= order.remaining(),
            "fill exceeds unaccounted order quantity"
        );
        let side = if fill.offset == Offset::Open {
            fill.identity.side
        } else {
            fill.identity.side.opposite()
        };
        let mut position = self.position(fill.identity.venue, &fill.identity.symbol, side);
        if fill.offset == Offset::Open {
            position.today = position
                .today
                .checked_add(fill.lots)
                .context("position overflow")?;
        } else {
            let (today, yesterday) = match self.limits.close_priority {
                ClosePriority::TodayFirst => {
                    let t = fill.lots.min(order.reserved_today);
                    (t, fill.lots - t)
                }
                ClosePriority::YesterdayFirst => {
                    let y = fill.lots.min(order.reserved_yesterday);
                    (fill.lots - y, y)
                }
            };
            ensure!(
                today <= order.reserved_today && yesterday <= order.reserved_yesterday,
                "fill exceeds close reservation"
            );
            position.today -= today;
            position.yesterday -= yesterday;
            position.frozen_today -= today;
            position.frozen_yesterday -= yesterday;
            order.reserved_today -= today;
            order.reserved_yesterday -= yesterday;
        }
        position.validate()?;
        if !fill.system_id.is_empty() {
            ensure!(
                order.system_id.is_empty() || order.system_id == fill.system_id,
                "fill system identity changed"
            );
            order.system_id = fill.system_id.clone();
            self.system_ids
                .insert((fill.identity.venue, fill.system_id.clone()), key.clone());
        }
        order.filled += fill.lots;
        if order.filled == order.request.lots {
            order.status = OrderStatus::Filled;
        }
        self.positions.insert(
            (fill.identity.venue, fill.identity.symbol.clone(), side),
            position,
        );
        self.orders.insert(key, order);
        self.fills.insert(fill.identity.clone(), fill);
        Ok(true)
    }

    pub fn prepare_cancel(&mut self, key: &OrderKey, now: Instant) -> Result<PreparedCancel> {
        ensure!(self.connected, "cannot cancel while transport disconnected");
        let order = self.orders.get(key).context("unknown order to cancel")?;
        ensure!(
            !order.status.terminal() && !order.cancel_pending && order.remaining() > 0,
            "order not cancellable"
        );
        ensure!(
            !self.scope.cancel_requires_system_id || !order.system_id.is_empty(),
            "native cancel requires exchange system ID; wait for acknowledgement"
        );
        let request = PreparedCancel {
            key: key.clone(),
            symbol: order.request.symbol.clone(),
            venue: order.request.venue,
            system_id: order.system_id.clone(),
            execution_route: order.request.execution_route,
        };
        self.rate_check(now)?;
        self.orders
            .get_mut(key)
            .context("order disappeared")?
            .cancel_pending = true;
        Ok(request)
    }
    pub fn cancel_failed(&mut self, key: &OrderKey) {
        if let Some(order) = self.orders.get_mut(key) {
            order.cancel_pending = false;
        }
    }
}

/// Direct function calls on one thread: no IPC between risk, OMS and SDK submission.
pub struct SingleLoop<T: Trader> {
    pub core: TradingCore,
    pub trader: T,
    pub execute: bool,
}
impl<T: Trader> SingleLoop<T> {
    pub fn submit(&mut self, intent: Intent, now: Instant) -> Result<OrderKey> {
        self.submit_with(intent, now, |_| Ok(()))
    }
    pub fn submit_with(
        &mut self,
        intent: Intent,
        now: Instant,
        before_native: impl FnOnce(&PreparedOrder) -> Result<()>,
    ) -> Result<OrderKey> {
        ensure!(self.execute, "read-only runtime: submissions disabled");
        let reference = self.trader.allocate_reference(self.core.next_ref)?;
        ensure!(
            reference >= self.core.next_ref && reference <= self.core.scope.reference_limit,
            "invalid SDK allocated reference"
        );
        self.core.next_ref = reference;
        let request = self.core.prepare(intent, now)?;
        if let Err(error) = before_native(&request) {
            self.core.reject(&request.key)?;
            self.core.block("submission audit failed before SDK call");
            return Err(error.context("order was not submitted"));
        }
        match self.trader.insert(&request) {
            Ok(0) => Ok(request.key),
            Ok(code) => {
                self.core.reject(&request.key)?;
                bail!("native local insert rejected: {code}");
            }
            Err(error) => {
                self.core
                    .block("unknown submission outcome; reconcile before retry");
                Err(error.context("native submission outcome uncertain"))
            }
        }
    }
    pub fn cancel(&mut self, key: &OrderKey, now: Instant) -> Result<()> {
        self.cancel_with(key, now, |_| Ok(()))
    }
    pub fn cancel_with(
        &mut self,
        key: &OrderKey,
        now: Instant,
        before_native: impl FnOnce(&PreparedCancel) -> Result<()>,
    ) -> Result<()> {
        self.cancel_with_route(key, None, now, before_native)
    }
    pub fn cancel_with_route(
        &mut self,
        key: &OrderKey,
        route: Option<ExecutionRoute>,
        now: Instant,
        before_native: impl FnOnce(&PreparedCancel) -> Result<()>,
    ) -> Result<()> {
        ensure!(self.execute, "read-only runtime: cancellations disabled");
        if let Some(route) = route {
            route.validate()?;
        }
        let mut request = self.core.prepare_cancel(key, now)?;
        if route.is_some() {
            request.execution_route = route;
        }
        if let Err(error) = before_native(&request) {
            self.core.cancel_failed(key);
            self.core.block("cancellation audit failed before SDK call");
            return Err(error.context("cancellation was not submitted"));
        }
        match self.trader.cancel(&request) {
            Ok(0) => Ok(()),
            Ok(code) => {
                self.core.cancel_failed(key);
                bail!("native local cancel rejected: {code}");
            }
            Err(error) => {
                self.core.block("unknown cancellation outcome; reconcile");
                Err(error)
            }
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildOutcome {
    SubmittedLocally,
    RejectedLocally,
    Uncertain,
    NotSent,
}
#[derive(Debug, Serialize)]
pub struct ChildReceipt {
    pub parent_index: usize,
    pub key: OrderKey,
    pub offset: Offset,
    pub lots: i32,
    pub outcome: ChildOutcome,
    pub error: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct SubmitReceipt {
    pub children: Vec<ChildReceipt>,
    pub all_submitted_locally: bool,
}
impl TradingCore {
    /// Translate a generic SHFE/INE close into explicit inventory-date requests.
    /// A parent FOK cannot be implemented as independently executable children.
    pub fn plan(&self, intent: Intent) -> Result<Vec<Intent>> {
        ensure!(
            intent.lots > 0 && intent.lots <= self.limits.max_order_lots,
            "invalid parent quantity"
        );
        if !intent.venue.separate_close_dates() || intent.offset != Offset::Close {
            return Ok(vec![intent]);
        }
        let (today, yesterday) = self.allocate_close(
            intent.venue,
            &intent.symbol,
            intent.side.opposite(),
            Offset::Close,
            intent.lots,
        )?;
        ensure!(
            intent.time_in_force != TimeInForce::Fok || today == 0 || yesterday == 0,
            "cannot split FOK across today/yesterday inventory; specify one date"
        );
        let mut children = vec![];
        let allocations = match self.limits.close_priority {
            ClosePriority::TodayFirst => [
                (Offset::CloseToday, today),
                (Offset::CloseYesterday, yesterday),
            ],
            ClosePriority::YesterdayFirst => [
                (Offset::CloseYesterday, yesterday),
                (Offset::CloseToday, today),
            ],
        };
        for (offset, lots) in allocations {
            if lots > 0 {
                children.push(Intent {
                    offset,
                    lots,
                    ..intent.clone()
                });
            }
        }
        Ok(children)
    }
}
impl<T: Trader> SingleLoop<T> {
    /// Reserve and validate every child before sending any. After a send failure,
    /// return all child identities and retain every uncertain reservation.
    pub fn submit_planned_with(
        &mut self,
        intent: Intent,
        now: Instant,
        mut audit: impl FnMut(&PreparedOrder) -> Result<()>,
    ) -> Result<SubmitReceipt> {
        ensure!(self.execute, "read-only runtime: submissions disabled");
        let children = self.core.plan(intent)?;
        let mut prepared: Vec<PreparedOrder> = vec![];
        for child in children {
            let preparation = (|| {
                let reference = self.trader.allocate_reference(self.core.next_ref)?;
                ensure!(
                    reference >= self.core.next_ref && reference <= self.core.scope.reference_limit,
                    "invalid SDK allocated reference"
                );
                self.core.next_ref = reference;
                self.core.prepare(child, now)
            })();
            match preparation {
                Ok(p) => prepared.push(p),
                Err(e) => {
                    for p in &prepared {
                        self.core.reject(&p.key)?;
                    }
                    return Err(e.context("no child was submitted"));
                }
            }
        }
        let mut receipt = SubmitReceipt {
            children: vec![],
            all_submitted_locally: true,
        };
        let mut stop = false;
        for p in prepared {
            let (outcome, error) = if stop {
                self.core.reject(&p.key)?;
                (ChildOutcome::NotSent, None)
            } else if let Err(e) = audit(&p) {
                self.core.reject(&p.key)?;
                self.core.block("submission audit failed before SDK call");
                stop = true;
                (ChildOutcome::NotSent, Some(e.to_string()))
            } else {
                match self.trader.insert(&p) {
                    Ok(0) => (ChildOutcome::SubmittedLocally, None),
                    Ok(code) => {
                        self.core.reject(&p.key)?;
                        stop = true;
                        (
                            ChildOutcome::RejectedLocally,
                            Some(format!("native local rejection {code}")),
                        )
                    }
                    Err(e) => {
                        self.core
                            .block("unknown submission outcome; reconcile before retry");
                        stop = true;
                        (ChildOutcome::Uncertain, Some(e.to_string()))
                    }
                }
            };
            receipt.children.push(ChildReceipt {
                parent_index: 0,
                key: p.key,
                offset: p.intent.offset,
                lots: p.intent.lots,
                outcome,
                error,
            });
        }
        receipt.all_submitted_locally = !stop;
        Ok(receipt)
    }
}

mod batch;
#[cfg(test)]
mod tests;
