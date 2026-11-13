use crate::model::*;
use anyhow::{bail, ensure, Context, Result};
use serde::Serialize;
use smallvec::{smallvec, SmallVec};
use std::collections::{HashMap, HashSet, VecDeque};
mod book;
use book::*;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Not Send/Sync: only the business-loop thread may own or mutate this state.
pub struct TradingCore {
    limits: RiskLimits,
    day: String,
    scope: ExecutionScope,
    session: String,
    next_ref: u64,
    order_reserve: usize,
    ready: bool,
    recovery_pending: bool,
    connected: bool,
    reason: String,
    cash: f64,
    symbols: Symbols,
    instruments: Vec<Option<Arc<Instrument>>>,
    continuous: [HashMap<String, bool>; VENUE_SLOTS],
    positions: Vec<[Option<Position>; 2]>,
    orders: OrderArena,
    // Membership follows remaining(), including terminal reports awaiting factual fills.
    working_by_symbol: Vec<Vec<OrderId>>,
    pending_open: Vec<[i64; 2]>,
    working_lots: i64,
    system_ids: [HashMap<String, OrderId>; VENUE_SLOTS],
    fills: Vec<[HashMap<String, Fill>; 2]>,
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
    pub orders: Vec<OrderView<'a>>,
    pub positions: Vec<PositionView<'a>>,
    pub instruments: Vec<&'a Instrument>,
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
        let capacity = limits.max_actions_per_second.min(INITIAL_ORDER_SLOTS);
        let mut actions = VecDeque::with_capacity(capacity);
        actions.resize(capacity, Instant::now());
        actions.clear();
        Ok(Self {
            limits,
            day: String::new(),
            scope,
            session: String::new(),
            next_ref: 1,
            order_reserve: INITIAL_ORDER_SLOTS,
            ready: false,
            recovery_pending: false,
            connected: false,
            reason: "not recovered".into(),
            cash: 0.0,
            symbols: Symbols::default(),
            instruments: Vec::new(),
            continuous: Default::default(),
            positions: Vec::new(),
            orders: OrderArena::default(),
            working_by_symbol: Vec::new(),
            pending_open: Vec::new(),
            working_lots: 0,
            system_ids: Default::default(),
            fills: Vec::new(),
            actions,
            _single_owner: PhantomData,
        })
    }
    pub fn apply_reference_floors(
        &mut self,
        floors: &HashMap<(String, String), u64>,
    ) -> Result<()> {
        if let Some(floor) = floors.get(&(self.day.clone(), self.session.clone())) {
            ensure!(
                *floor <= self.scope.reference_limit,
                "durable native order reference exhausted"
            );
            self.next_ref = self.next_ref.max(*floor);
        }
        Ok(())
    }
    /// Cold-path capacity hint. Live history can grow beyond it without dropping identities.
    pub fn reserve_orders(&mut self, count: usize) -> Result<()> {
        self.order_reserve = self.order_reserve.max(count);
        self.orders
            .reserve(self.order_reserve)
            .context("reserve native order slots")?;
        if !self.session.is_empty() {
            self.orders
                .reserve_session(&self.session, self.order_reserve)
                .context("reserve native reference index")?;
        }
        self.reserve_actions(self.order_reserve)?;
        Ok(())
    }
    fn reserve_actions(&mut self, count: usize) -> Result<()> {
        let count = count.min(self.limits.max_actions_per_second);
        self.actions
            .try_reserve(count.saturating_sub(self.actions.len()))
            .context("reserve native action-rate window")?;
        if self.actions.is_empty() {
            self.actions.resize(count, Instant::now());
            self.actions.clear();
        }
        Ok(())
    }
    pub fn is_ready(&self) -> bool {
        self.ready && self.connected && !self.recovery_pending
    }
    /// Gate sends while a recovery checkpoint is being committed asynchronously.
    pub(crate) fn recovery_pending(&mut self, pending: bool) {
        self.recovery_pending = pending;
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
        for status in &mut self.continuous {
            status.clear();
        }
        self.block("session/bootstrap incomplete");
    }
    pub fn trading_day(&self) -> &str {
        &self.day
    }
    pub fn instrument(&self, venue: Venue, symbol: &str) -> Option<&Instrument> {
        self.instruments
            .get(self.symbols.id(venue, symbol)?.0)?
            .as_deref()
    }
    pub(crate) fn instrument_handle(&self, venue: Venue, symbol: &str) -> Option<Arc<Instrument>> {
        self.instruments
            .get(self.symbols.id(venue, symbol)?.0)?
            .clone()
    }
    pub fn fill_order_key<'a>(&'a self, fill: &'a Fill) -> Option<&'a OrderKey> {
        self.system_ids[venue_index(fill.identity.venue)]
            .get(fill.system_id.as_str())
            .map(|id| &self.orders.identities[id.0].key)
            .or(fill.order_key.as_ref())
    }
    pub fn order(&self, key: &OrderKey) -> Option<OrderView<'_>> {
        self.orders.get(key)
    }
    fn ensure_symbol(&mut self, venue: Venue, symbol: &str) -> SymbolId {
        let id = self.symbols.intern(venue, symbol);
        while self.positions.len() <= id.0 {
            self.positions.push([None, None]);
            self.instruments.push(None);
            self.pending_open.push([0, 0]);
            self.fills.push(Default::default());
            self.working_by_symbol.push(Vec::with_capacity(
                (self.limits.max_working_lots as usize).clamp(4, 64),
            ));
        }
        id
    }
    fn finish_order_change(&mut self, id: OrderId, previous: i32) {
        let slot = &self.orders.slots[id.0];
        let remaining = slot.remaining();
        let symbol = slot.symbol;
        self.working_lots += i64::from(remaining) - i64::from(previous);
        if slot.offset == Offset::Open {
            self.pending_open[symbol.0][side_index(slot.side)] +=
                i64::from(remaining) - i64::from(previous);
        }
        if remaining > 0 && previous == 0 {
            let index = self.working_by_symbol[symbol.0].len();
            self.working_by_symbol[symbol.0].push(id);
            self.orders.slots[id.0].working_index = std::num::NonZeroUsize::new(index + 1);
        } else if remaining == 0 && previous > 0 {
            let index = self.orders.slots[id.0]
                .working_index
                .take()
                .expect("outstanding orders have a dense working index")
                .get()
                - 1;
            self.working_by_symbol[symbol.0].swap_remove(index);
            if let Some(moved) = self.working_by_symbol[symbol.0].get(index) {
                self.orders.slots[moved.0].working_index = std::num::NonZeroUsize::new(index + 1);
            }
        }
    }
    fn commit_order(&mut self, order: Order) {
        let symbol = self.ensure_symbol(order.request.venue, &order.request.symbol);
        let (id, previous) = self.orders.insert(order, symbol);
        self.finish_order_change(id, previous);
    }
    fn position_id(&self, symbol: SymbolId, side: Side) -> Position {
        self.positions[symbol.0][side_index(side)]
            .clone()
            .unwrap_or_default()
    }
    pub fn position(&self, venue: Venue, symbol: &str, side: Side) -> Position {
        self.symbols
            .id(venue, symbol)
            .map(|id| self.position_id(id, side))
            .unwrap_or_default()
    }
    /// Copy only account state/positions into a cold-preallocated observation
    /// buffer. No order/instrument projection, JSON, I/O or mutable-core borrow
    /// crosses into the publisher thread.
    pub fn capture_positions(&self, snapshot: &mut cn_futures_state::Snapshot) -> Result<()> {
        use cn_futures_state::{PositionRow, Text, MAX_POSITIONS};
        snapshot.position_ready = !self.day.is_empty();
        snapshot.ready = self.is_ready();
        snapshot.connected = self.connected;
        snapshot.trading_day = Text::new(&self.day)?;
        snapshot.reason = Text::new(if self.recovery_pending && self.ready {
            "recovery persistence pending"
        } else {
            &self.reason
        })?;
        snapshot.available_local_budget = self.cash;
        snapshot.positions.clear();
        for (index, sides) in self.positions.iter().enumerate() {
            for (side, position) in sides.iter().enumerate() {
                let Some(position) = position else { continue };
                ensure!(
                    snapshot.positions.len() < MAX_POSITIONS,
                    "position IPC capacity exceeded; no partial snapshot published"
                );
                let (venue, symbol) = self.symbols.identity(SymbolId(index));
                snapshot.positions.push(PositionRow {
                    venue,
                    symbol: Text::new(symbol)?,
                    side: if side == 0 { Side::Buy } else { Side::Sell },
                    position: position.clone(),
                });
            }
        }
        Ok(())
    }
    pub fn view(&self) -> CoreView<'_> {
        CoreView {
            instruments: self
                .instruments
                .iter()
                .filter_map(Option::as_deref)
                .collect(),
            ready: self.is_ready(),
            connected: self.connected,
            reason: if self.recovery_pending && self.ready {
                "recovery persistence pending"
            } else {
                &self.reason
            },
            trading_day: &self.day,
            available_local_budget: self.cash,
            orders: self.orders.values().collect(),
            positions: self
                .positions
                .iter()
                .enumerate()
                .flat_map(|(index, sides)| {
                    sides
                        .iter()
                        .enumerate()
                        .filter_map(move |(side, position)| {
                            let position = position.as_ref()?;
                            let (venue, symbol) = self.symbols.identity(SymbolId(index));
                            Some(PositionView {
                                venue,
                                symbol,
                                side: if side == 0 { Side::Buy } else { Side::Sell },
                                position,
                            })
                        })
                })
                .collect(),
        }
    }
    pub fn set_trading_status(&mut self, venue: Venue, symbol_or_product: &str, continuous: bool) {
        self.continuous[venue_index(venue)].insert(symbol_or_product.to_owned(), continuous);
    }
    /// Apply only a complete new rule set; omission immediately removes trading permission.
    pub fn replace_instruments(&mut self, instruments: Vec<Instrument>) -> Result<()> {
        let mut seen = HashSet::new();
        for rule in &instruments {
            rule.validate()?;
            ensure!(
                self.scope.supports_symbol(rule.venue, &rule.symbol)
                    && seen.insert((rule.venue, rule.symbol.as_str())),
                "duplicate instrument in rule refresh"
            );
        }
        self.instruments.fill(None);
        for rule in instruments {
            let id = self.ensure_symbol(rule.venue, &rule.symbol);
            self.instruments[id.0] = Some(Arc::new(rule));
        }
        Ok(())
    }
    /// A cash poll may tighten the budget. Only a full reconciliation can replenish it.
    pub fn constrain_cash(&mut self, available: f64) -> Result<()> {
        ensure!(available.is_finite(), "nonfinite cash refresh");
        self.cash = self.cash.min(available);
        Ok(())
    }
    fn trading_status(&self, venue: Venue, symbol: &str) -> bool {
        let statuses = &self.continuous[venue_index(venue)];
        statuses
            .get(symbol)
            .or_else(|| {
                symbol
                    .find(|c: char| c.is_ascii_digit())
                    .and_then(|index| statuses.get(&symbol[..index]))
            })
            .copied()
            .unwrap_or(false)
    }

    /// Caller must supply a complete, reconciled account snapshot, never query fragments.
    /// Validation and reconstruction are atomic with respect to the existing core.
    pub fn restore(&mut self, s: Snapshot) -> Result<()> {
        validate_snapshot(&s, &self.scope)?;
        for old in self.orders.values().filter(|o| o.remaining() > 0) {
            let recovered=s.orders.iter().find(|o| &o.key==old.key || (o.venue==old.request.venue && !old.system_id.is_empty() && &o.system_id==old.system_id))
                .context("snapshot omits unresolved local order; preserve reservations until a factual outcome is available")?;
            ensure!(
                recovered.venue == old.request.venue
                    && recovered.symbol == old.request.symbol
                    && recovered.side == old.request.side
                    && recovered.offset == old.request.offset
                    && recovered.price_type == old.request.price_type
                    && recovered.time_in_force == old.request.time_in_force
                    && recovered.lots == old.request.lots
                    && recovered.traded >= old.filled,
                "snapshot conflicts with known unresolved order identity/quantity"
            );
        }
        if self.day == s.trading_day {
            for known in self
                .fills
                .iter()
                .flat_map(|sides| sides.iter().flat_map(HashMap::values))
            {
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
        next.order_reserve = self.order_reserve;
        next.reserve_actions(self.order_reserve)?;
        next.orders
            .reserve(
                s.orders
                    .len()
                    .saturating_add(INITIAL_ORDER_SLOTS)
                    .max(self.order_reserve),
            )
            .context("reserve recovered native order slots")?;
        next.orders
            .reserve_session(&next.session, self.order_reserve)
            .context("reserve recovered reference index")?;
        next.replace_instruments(s.instruments)?;
        for p in s.positions {
            let id = next.ensure_symbol(p.venue, &p.symbol);
            next.positions[id.0][side_index(p.side)] = Some(Position {
                today: p.today,
                yesterday: p.yesterday,
                ..Position::default()
            });
        }
        for f in s.trades {
            let symbol = next.ensure_symbol(f.identity.venue, &f.identity.symbol);
            next.fills[symbol.0][side_index(f.identity.side)]
                .insert(f.identity.trade_id.clone(), f);
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
                    time_in_force: update.time_in_force,
                    execution_route: None,
                },
                key: update.key.clone(),
                system_id: update.system_id,
                limit_price: update.limit_price,
                // A cancel racing the final fill can be reported as cancelled with no remainder.
                status: if update.traded == update.lots {
                    OrderStatus::Filled
                } else {
                    update.status
                },
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
                cash_reserved_per_lot: 0.0,
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
            let symbol = next.ensure_symbol(order.request.venue, &order.request.symbol);
            let id = OrderId(next.orders.slots.len());
            if !order.system_id.is_empty() {
                ensure!(
                    !next.system_ids[venue_index(order.request.venue)]
                        .contains_key(order.system_id.as_str()),
                    "duplicate venue system order identity"
                );
                next.system_ids[venue_index(order.request.venue)]
                    .insert(order.system_id.clone(), id);
            }
            let _ = symbol;
            next.commit_order(order);
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
        let symbol = self
            .symbols
            .id(order.request.venue, &order.request.symbol)
            .context("unknown position symbol")?;
        let side = order.request.side.opposite();
        let mut p = self.position_id(symbol, side);
        p.frozen_today += today;
        p.frozen_yesterday += yesterday;
        p.validate()?;
        self.positions[symbol.0][side_index(side)] = Some(p);
        Ok(())
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
        let symbol = self
            .symbols
            .id(intent.venue, &intent.symbol)
            .context("symbol absent from complete rule snapshot")?;
        let rule = self.instruments[symbol.0]
            .as_ref()
            .context("symbol absent from complete rule snapshot")?;
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
        ensure!(
            self.working_lots + i64::from(intent.lots) <= i64::from(self.limits.max_working_lots),
            "working lot limit"
        );
        ensure!(
            !self.working_by_symbol[symbol.0]
                .iter()
                .map(|id| &self.orders.slots[id.0])
                .any(|o| o.side != intent.side
                    && o.remaining() > 0
                    && (intent.price_type == PriceType::Market
                        || o.price_type == PriceType::Market
                        || match intent.side {
                            Side::Buy => price + rule.price_tick * 1e-6 >= o.limit_price,
                            Side::Sell => price - rule.price_tick * 1e-6 <= o.limit_price,
                        })),
            "self-trade prevention"
        );
        let mut today = 0;
        let mut yesterday = 0;
        let mut cash_unit = 0.0;
        if intent.offset == Offset::Open {
            let pending = self.pending_open[symbol.0][side_index(intent.side)];
            ensure!(
                self.position_id(symbol, intent.side).total() + pending + i64::from(intent.lots)
                    <= i64::from(self.limits.max_position_lots_per_side),
                "position limit"
            );
            cash_unit = rule.cash_budget_per_open_lot;
            if let (Some(margin), Some(commission)) = (rule.margin, rule.commission) {
                let margin_cash = margin.estimate(intent.side, price, rule.contract_multiple, 1)?;
                let opening_fee =
                    commission.estimate(Offset::Open, price, rule.contract_multiple, 1)?;
                ensure!(
                    (margin_cash + opening_fee).is_finite(),
                    "opening cash requirement overflow"
                );
                cash_unit = cash_unit.max(margin_cash + opening_fee);
            }
        } else {
            (today, yesterday) = self.allocate_close(
                intent.venue,
                &intent.symbol,
                intent.side.opposite(),
                intent.offset,
                intent.lots,
            )?;
        }
        if let Some(option) = &rule.option {
            if intent.side == Side::Buy {
                let premium_price = if intent.price_type == PriceType::Market {
                    ensure!(
                        option.upper_limit_price > 0.0,
                        "option market buy requires a native upper price limit"
                    );
                    option.upper_limit_price
                } else {
                    price
                };
                let premium_budget = premium_price * f64::from(option.contract_multiple)
                    + option.cash_buffer_per_buy_lot;
                ensure!(
                    premium_budget.is_finite(),
                    "option premium cash budget overflow"
                );
                cash_unit = cash_unit.max(premium_budget);
            }
        }
        let cash = cash_unit * f64::from(intent.lots);
        ensure!(
            cash.is_finite()
                && (cash == 0.0 || self.cash - cash >= self.limits.minimum_cash_remaining),
            "local cash budget insufficient"
        );
        ensure!(
            self.next_ref <= self.scope.reference_limit,
            "native order reference exhausted"
        );
        let option_market_buy_ceiling =
            if intent.side == Side::Buy && intent.price_type == PriceType::Market {
                rule.option.as_ref().map_or(0.0, |o| o.upper_limit_price)
            } else {
                0.0
            };
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
            cash_reserved_per_lot: cash_unit,
            cancel_pending: false,
            owned: true,
        };
        if intent.offset != Offset::Open {
            self.adjust_frozen(&order, today, yesterday)?;
        }
        self.cash -= cash;
        self.commit_order(order);
        Ok(PreparedOrder {
            key,
            option_market_buy_ceiling,
            intent,
            limit_price: price,
        })
    }

    pub fn reject(&mut self, key: &OrderKey) -> Result<()> {
        let id = self
            .orders
            .id(key)
            .context("rejection without known order")?;
        self.reject_id(id)
    }
    fn reject_id(&mut self, id: OrderId) -> Result<()> {
        let order = &self.orders.slots[id.0];
        if order.status == OrderStatus::Rejected {
            return Ok(());
        }
        ensure!(
            order.filled == 0 && order.reported_filled == 0,
            "rejection conflicts with factual or reported fill"
        );
        let old = order.remaining();
        let lots = order.lots;
        self.release_cancelled(id, lots)?;
        let order = &mut self.orders.slots[id.0];
        order.status = OrderStatus::Rejected;
        order.cancel_pending = false;
        self.finish_order_change(id, old);
        Ok(())
    }
    fn release_cancelled(&mut self, id: OrderId, total: i32) -> Result<()> {
        let slot = &self.orders.slots[id.0];
        let order = &self.orders.slots[id.0];
        let symbol = slot.symbol;
        ensure!(
            total >= order.cancelled && total <= order.lots - order.filled,
            "inconsistent cancelled quantity"
        );
        let release = total - order.cancelled;
        let refund = (f64::from(release) * order.cash_reserved_per_lot).min(order.local_cash_held);
        let (today, yesterday, position) = if order.offset != Offset::Open {
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
            let side = order.side.opposite();
            let mut p = self.position_id(symbol, side);
            p.frozen_today -= today;
            p.frozen_yesterday -= yesterday;
            p.validate()?;
            (today, yesterday, Some((side, p)))
        } else {
            (0, 0, None)
        };
        if let Some((side, p)) = position {
            self.positions[symbol.0][side_index(side)] = Some(p);
        }
        self.cash += refund;
        let order = &mut self.orders.slots[id.0];
        order.local_cash_held -= refund;
        order.reserved_today -= today;
        order.reserved_yesterday -= yesterday;
        order.cancelled = total;
        Ok(())
    }
    fn check_system_id(&self, id: OrderId, system_id: &str) -> Result<()> {
        if system_id.is_empty() {
            return Ok(());
        }
        let order = self.orders.view(id);
        ensure!(
            order.system_id.is_empty() || order.system_id == system_id,
            "system identity changed"
        );
        ensure!(
            self.system_ids[venue_index(order.request.venue)]
                .get(system_id)
                .is_none_or(|other| *other == id),
            "system identity collision"
        );
        Ok(())
    }
    fn bind_system_id(&mut self, id: OrderId, system_id: &str) {
        let order = &mut self.orders.identities[id.0];
        if !system_id.is_empty() && order.system_id.is_empty() {
            self.system_ids[venue_index(order.request.venue)].insert(system_id.to_owned(), id);
            order.system_id = system_id.to_owned();
        }
    }
    pub fn on_order(&mut self, update: &OrderUpdate) -> Result<()> {
        ensure!(
            self.scope.venues.contains(&update.venue) && update.trading_day == self.day,
            "order outside current account day/scope"
        );
        let id = self
            .orders
            .id(&update.key)
            .context("untracked account order requires reconciliation")?;
        let order = self.orders.view(id);
        ensure!(
            order.request.venue == update.venue
                && order.request.symbol == update.symbol
                && order.request.side == update.side
                && order.request.offset == update.offset
                && order.request.price_type == update.price_type
                && order.request.time_in_force == update.time_in_force
                && order.request.lots == update.lots,
            "order identity fields changed"
        );
        ensure!(
            update.traded >= 0 && update.traded <= update.lots,
            "invalid order cumulative fill"
        );
        self.check_system_id(id, &update.system_id)?;
        let old = order.remaining();
        let reported = order.reported_filled.max(update.traded).max(order.filled);
        if update.status == OrderStatus::Rejected {
            ensure!(
                reported == 0,
                "rejection conflicts with factual or reported fill"
            );
            self.reject_id(id)?;
            self.bind_system_id(id, &update.system_id);
            return Ok(());
        }
        if update.status == OrderStatus::Filled {
            ensure!(
                update.traded == order.request.lots && order.cancelled == 0,
                "all-traded conflicts with order quantities"
            );
        }
        if update.status == OrderStatus::Cancelled {
            let cancelled = order.request.lots - reported;
            if cancelled >= order.cancelled {
                self.release_cancelled(id, cancelled)?;
            }
        }
        self.bind_system_id(id, &update.system_id);
        let order = &mut self.orders.slots[id.0];
        order.reported_filled = reported;
        if update.cancel_rejected {
            order.cancel_pending = false;
        }
        match update.status {
            OrderStatus::Cancelled => {
                if order.status != OrderStatus::Filled {
                    order.status = OrderStatus::Cancelled;
                }
                order.cancel_pending = false;
            }
            OrderStatus::Filled => {
                order.status = OrderStatus::Filled;
                order.cancel_pending = false;
            }
            status if !order.status.terminal() => order.status = status,
            _ => {}
        }
        self.finish_order_change(id, old);
        Ok(())
    }
    pub fn on_fill(&mut self, fill: &Fill) -> Result<bool> {
        validate_fill(fill, &self.day, &self.scope)?;
        if let Some(symbol) = self.symbols.id(fill.identity.venue, &fill.identity.symbol) {
            if let Some(previous) = self.fills[symbol.0][side_index(fill.identity.side)]
                .get(fill.identity.trade_id.as_str())
            {
                ensure!(previous == fill, "conflicting duplicate trade identity");
                return Ok(false);
            }
        }
        let id = self.system_ids[venue_index(fill.identity.venue)]
            .get(fill.system_id.as_str())
            .copied()
            .or_else(|| {
                let id = if let Some(key) = &fill.order_key {
                    self.orders.id(key)
                } else if self.scope.allow_implicit_fill_reference {
                    self.orders.id_parts(&self.session, &fill.order_ref)
                } else {
                    None
                }?;
                let order = self.orders.view(id);
                (order.system_id.is_empty() || order.system_id == &fill.system_id).then_some(id)
            })
            .context("untracked factual fill: preserve evidence and reconcile, never discard it")?;
        let slot = &self.orders.slots[id.0];
        let order = self.orders.view(id);
        let symbol = slot.symbol;
        ensure!(
            fill.order_key.as_ref().is_none_or(|k| k == order.key),
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
        self.check_system_id(id, &fill.system_id)?;
        let old = order.remaining();
        let side = if fill.offset == Offset::Open {
            fill.identity.side
        } else {
            fill.identity.side.opposite()
        };
        let mut position = self.position_id(symbol, side);
        let (today, yesterday) = if fill.offset == Offset::Open {
            position.today = position
                .today
                .checked_add(fill.lots)
                .context("position overflow")?;
            (0, 0)
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
            (today, yesterday)
        };
        position.validate()?;
        self.bind_system_id(id, &fill.system_id);
        let order = &mut self.orders.slots[id.0];
        order.reserved_today -= today;
        order.reserved_yesterday -= yesterday;
        order.filled += fill.lots;
        if order.filled == order.lots {
            order.status = OrderStatus::Filled;
        }
        self.positions[symbol.0][side_index(side)] = Some(position);
        self.finish_order_change(id, old);
        self.fills[symbol.0][side_index(fill.identity.side)]
            .insert(fill.identity.trade_id.clone(), fill.clone());
        Ok(true)
    }

    pub fn prepare_cancel(&mut self, key: &OrderKey, now: Instant) -> Result<PreparedCancel> {
        ensure!(
            !self.recovery_pending,
            "recovery persistence checkpoint pending"
        );
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
        let id = self.orders.id(key).context("order disappeared")?;
        self.orders.slots[id.0].cancel_pending = true;
        Ok(request)
    }
    pub fn cancel_failed(&mut self, key: &OrderKey) {
        if let Some(id) = self.orders.id(key) {
            self.orders.slots[id.0].cancel_pending = false;
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
    pub children: SmallVec<[ChildReceipt; 2]>,
    pub all_submitted_locally: bool,
}
impl TradingCore {
    /// Translate a generic SHFE/INE close into explicit inventory-date requests.
    /// A parent FOK cannot be implemented as independently executable children.
    pub fn plan(&self, intent: Intent) -> Result<SmallVec<[Intent; 2]>> {
        ensure!(
            intent.lots > 0 && intent.lots <= self.limits.max_order_lots,
            "invalid parent quantity"
        );
        if !intent.venue.separate_close_dates() || intent.offset != Offset::Close {
            return Ok(smallvec![intent]);
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
        let mut children = SmallVec::new();
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
        let mut prepared: SmallVec<[PreparedOrder; 2]> = SmallVec::new();
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
            children: SmallVec::new(),
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

#[cfg(test)]
mod layout_tests;
